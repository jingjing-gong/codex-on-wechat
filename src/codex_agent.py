"""Codex SDK agent used by the WeChat bridge."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

from openai_codex import ApprovalMode, AsyncCodex, AsyncThread, Sandbox

logger = logging.getLogger(__name__)


def _default_workspace() -> str:
    path = Path.home() / ".codex-on-wechat" / "workspace"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


@dataclass
class AgentInfo:
    """Metadata about the running Codex SDK client."""

    name: str
    type: str
    model: str = ""
    command: str = "codex"
    pid: int = 0

    def __str__(self) -> str:
        value = f"name={self.name}, type={self.type}, model={self.model}"
        if self.pid:
            value += f", pid={self.pid}"
        return value


class CodexAgent:
    """Conversation adapter backed exclusively by the official Codex SDK."""

    def __init__(
        self,
        *,
        model: str = "",
        cwd: str | None = None,
        turn_timeout: float | None = 60,
        codex: AsyncCodex | None = None,
    ):
        self.model = model
        self.cwd = cwd or _default_workspace()
        self.turn_timeout = turn_timeout
        self._codex = codex or AsyncCodex()
        self._owns_codex = codex is None
        self._started = False
        self._threads: dict[str, AsyncThread] = {}
        self._active_turns: dict[str, Any] = {}
        self._active_messages: dict[str, str] = {}
        self._conversation_locks: dict[str, asyncio.Lock] = {}
        self._conversation_models: dict[str, str] = {}
        self._conversation_reasoning_efforts: dict[str, str] = {}

    async def start(self) -> None:
        if self._started:
            return
        await self._codex.__aenter__()
        self._started = True
        logger.info("initialized Codex SDK")

    async def stop(self) -> None:
        if self._started and self._owns_codex:
            await self._codex.__aexit__(None, None, None)
        self._started = False

    def set_cwd(self, cwd: str) -> None:
        self.cwd = cwd

    def info(self) -> AgentInfo:
        return AgentInfo(name="codex", type="codex-sdk", model=self.model)

    async def reset_session(self, conversation_id: str) -> str:
        self._threads.pop(conversation_id, None)
        thread = await self._start_thread(conversation_id)
        return thread.id

    def set_model(self, conversation_id: str, model: str) -> None:
        if model:
            self._conversation_models[conversation_id] = model
        else:
            self._conversation_models.pop(conversation_id, None)

    def get_model(self, conversation_id: str) -> str:
        return self._conversation_models.get(conversation_id, self.model)

    def set_reasoning_effort(self, conversation_id: str, effort: str) -> None:
        if effort:
            self._conversation_reasoning_efforts[conversation_id] = effort
        else:
            self._conversation_reasoning_efforts.pop(conversation_id, None)

    def get_reasoning_effort(self, conversation_id: str) -> str:
        return self._conversation_reasoning_efforts.get(conversation_id, "")

    def get_thread_id(self, conversation_id: str) -> str | None:
        thread = self._threads.get(conversation_id)
        return thread.id if thread else None

    async def interrupt(self, conversation_id: str) -> bool:
        """Interrupt the active turn for a conversation, if one exists."""
        turn = self._active_turns.get(conversation_id)
        if turn is None:
            return False
        await turn.interrupt()
        return True

    def status(self, conversation_id: str) -> str:
        """Return a short status for the conversation's current Codex turn."""
        message = self._active_messages.get(conversation_id)
        if message is None:
            return "idle"
        return f"busy: {message}"

    async def list_threads(
        self,
        *,
        search_term: str | None = None,
        archived: bool | None = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        await self.start()
        response = await self._codex.thread_list(
            search_term=search_term, archived=archived, limit=limit, cwd=self.cwd
        )
        return [self._to_dict(item) for item in response.data]

    async def resume_thread(
        self, conversation_id: str, thread_id: str
    ) -> dict[str, Any]:
        await self.start()
        thread = await self._codex.thread_resume(
            thread_id,
            approval_mode=ApprovalMode.deny_all,
            cwd=self.cwd,
            sandbox=Sandbox.full_access,
        )
        self._threads[conversation_id] = thread
        return {"id": thread.id}

    async def delete_thread(self, thread_id: str) -> None:
        await self.start()
        await self._codex.thread_archive(thread_id)
        for conversation_id, thread in list(self._threads.items()):
            if thread.id == thread_id:
                self._threads.pop(conversation_id, None)

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        await self.start()
        response = await self._codex.models(include_hidden=include_hidden)
        return [self._to_dict(model) for model in response.data]

    async def chat(self, conversation_id: str, message: str) -> str:
        parts: list[str] = []
        async for text in self.chat_stream(conversation_id, message):
            parts.append(text)
        result = "".join(parts).strip()
        if not result:
            raise RuntimeError("Codex returned an empty response")
        return result

    async def chat_stream(
        self, conversation_id: str, message: str
    ) -> AsyncIterator[str]:
        lock = self._conversation_locks.setdefault(conversation_id, asyncio.Lock())
        async with lock:
            thread = await self._get_or_create_thread(conversation_id)
            kwargs: dict[str, Any] = {
                "approval_mode": ApprovalMode.deny_all,
                "cwd": self.cwd,
                "sandbox": Sandbox.full_access,
            }
            model = self.get_model(conversation_id)
            effort = self.get_reasoning_effort(conversation_id)
            if model:
                kwargs["model"] = model
            if effort:
                kwargs["effort"] = effort

            turn = await thread.turn(message, **kwargs)
            self._active_turns[conversation_id] = turn
            self._active_messages[conversation_id] = message.strip()
            stream = turn.stream()
            emitted_text = False
            interrupted = False
            deadline = (
                time.monotonic() + self.turn_timeout
                if self.turn_timeout is not None
                else None
            )
            try:
                while True:
                    if deadline is None:
                        event = await anext(stream)
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                        event = await asyncio.wait_for(anext(stream), remaining)

                    if event.method == "item/completed":
                        item = getattr(event.payload, "item", None)
                        item = getattr(item, "root", item)
                        if getattr(item, "type", None) != "agentMessage":
                            continue
                        text = (getattr(item, "text", "") or "").strip()
                        if text:
                            emitted_text = True
                            yield text
                    elif event.method == "turn/completed":
                        completed_turn = getattr(event.payload, "turn", None)
                        status = getattr(completed_turn, "status", None)
                        status_value = getattr(status, "value", status)
                        if status_value == "failed":
                            error = getattr(completed_turn, "error", None)
                            detail = getattr(error, "message", None) or "turn failed"
                            raise RuntimeError(detail)
                        interrupted = status_value == "interrupted"
                        break
            except asyncio.TimeoutError:
                await turn.interrupt()
                raise RuntimeError("Codex turn timed out")
            finally:
                self._active_turns.pop(conversation_id, None)
                self._active_messages.pop(conversation_id, None)
                await stream.aclose()

            if not emitted_text and not interrupted:
                raise RuntimeError("Codex returned an empty response")

    async def _start_thread(self, conversation_id: str) -> AsyncThread:
        await self.start()
        kwargs: dict[str, Any] = {
            "approval_mode": ApprovalMode.deny_all,
            "cwd": self.cwd,
            "sandbox": Sandbox.full_access,
        }
        model = self.get_model(conversation_id)
        if model:
            kwargs["model"] = model
        thread = await self._codex.thread_start(**kwargs)
        self._threads[conversation_id] = thread
        return thread

    async def _get_or_create_thread(self, conversation_id: str) -> AsyncThread:
        return self._threads.get(conversation_id) or await self._start_thread(
            conversation_id
        )

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            return value.model_dump(by_alias=True, exclude_none=True)
        if hasattr(value, "__dict__"):
            return dict(value.__dict__)
        return dict(value)
