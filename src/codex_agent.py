"""Compatibility facade for the legacy interactive Codex Agent API.

The durable runtime lives in :mod:`src.agents.codex_runtime`.  Keeping this
small facade preserves the existing WeChat bot and downstream imports while
ensuring SDK execution types are translated in one module only.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, AsyncIterator

from .agents.codex_runtime import CodexRuntime
from .agents.base import AgentEvent, AgentTask


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


class CodexAgent(CodexRuntime):
    """Legacy conversation-oriented API backed by :class:`CodexRuntime`."""

    def __init__(
        self,
        *,
        model: str = "",
        cwd: str | None = None,
        turn_timeout: float | None = 60,
        codex: Any | None = None,
    ) -> None:
        super().__init__(
            model=model,
            cwd=cwd,
            turn_timeout=turn_timeout,
            codex=codex,
            # Preserve the original facade's behavior.  Durable tasks use
            # explicit application modes; only this legacy chat entry point
            # retains its historical full-access sandbox.
            mode_resolver={
                "chat": SimpleNamespace(
                    sandbox_policy="full-access",
                    approval_policy="deny_all",
                    developer_instructions="",
                )
            },
        )
        self._conversation_models: dict[str, str] = {}
        self._conversation_reasoning_efforts: dict[str, str] = {}
        self._conversation_locks: dict[str, asyncio.Lock] = {}
        self._threads: dict[str, Any] = {}

    def set_cwd(self, cwd: str) -> None:
        self._assert_loop()
        self.cwd = cwd

    def info(self) -> AgentInfo:
        return AgentInfo(name="codex", type="codex-sdk", model=self.model)

    async def stop(self) -> None:
        await super().stop()
        self._threads.clear()

    # ------------------------------------------------------------------
    # Legacy conversation/session compatibility
    def set_model(self, conversation_id: str, model: str) -> None:
        self._assert_loop()
        if model:
            self._conversation_models[str(conversation_id)] = model
        else:
            self._conversation_models.pop(str(conversation_id), None)

    def get_model(self, conversation_id: str) -> str:
        self._assert_loop()
        return self._conversation_models.get(str(conversation_id), self.model)

    def set_reasoning_effort(self, conversation_id: str, effort: str) -> None:
        self._assert_loop()
        if effort:
            self._conversation_reasoning_efforts[str(conversation_id)] = effort
        else:
            self._conversation_reasoning_efforts.pop(str(conversation_id), None)

    def get_reasoning_effort(self, conversation_id: str) -> str:
        self._assert_loop()
        return self._conversation_reasoning_efforts.get(str(conversation_id), "")

    def get_thread_id(self, conversation_id: str) -> str | None:
        self._assert_loop()
        thread = self._threads.get(str(conversation_id))
        if thread is not None:
            return str(getattr(thread, "id", thread))
        return super().get_thread_id(str(conversation_id), mode_id="chat")

    async def reset_session(self, conversation_id: str) -> str:
        conversation_id = str(conversation_id)
        self._threads.pop(conversation_id, None)
        for key in tuple(self._bindings):
            if key[0] == conversation_id:
                self._bindings.pop(key, None)
        await self.start()
        task = AgentTask(
            task_id=f"reset-{uuid.uuid4().hex}",
            conversation_id=conversation_id,
            mode_id="chat",
            model=self.get_model(conversation_id),
            reasoning_effort=self.get_reasoning_effort(conversation_id),
            inputs="",
        )
        binding = await self._binding_for(task)
        self._threads[conversation_id] = binding.thread
        return binding.thread_id

    async def resume_thread(self, conversation_id: str, thread_id: str) -> dict[str, Any]:
        await self.start()
        resume = getattr(self._codex, "thread_resume", None)
        if resume is None:
            raise RuntimeError("Codex client does not expose thread_resume()")
        kwargs = {
            "approval_mode": self._approval_mode_for_legacy(),
            "cwd": self.cwd,
            "sandbox": self._sandbox_for_legacy(),
        }
        try:
            result = resume(thread_id, **kwargs)
        except TypeError:
            result = resume(thread_id)
        thread = await result if inspect.isawaitable(result) else result
        self._threads[str(conversation_id)] = thread
        from .agents.codex_runtime import ThreadBinding

        binding = ThreadBinding(
            conversation_id=str(conversation_id),
            mode_id="chat",
            profile_version=1,
            policy_version=1,
            thread_id=str(getattr(thread, "id", thread_id)),
            thread=thread,
        )
        self._bindings[binding.key] = binding
        self._threads_by_id[binding.thread_id] = thread
        return {"id": binding.thread_id}

    async def delete_thread(self, thread_id: str) -> None:
        await self.start()
        archive = getattr(self._codex, "thread_archive", None)
        if archive is not None:
            result = archive(thread_id)
            if inspect.isawaitable(result):
                await result
        for conversation_id, thread in list(self._threads.items()):
            if str(getattr(thread, "id", thread)) == str(thread_id):
                self._threads.pop(conversation_id, None)
        for key, binding in list(self._bindings.items()):
            if binding.thread_id == str(thread_id):
                self._bindings.pop(key, None)

    async def list_threads(
        self,
        *,
        search_term: str | None = None,
        archived: bool | None = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        await self.start()
        method = getattr(self._codex, "thread_list", None)
        if method is None:
            return []
        kwargs = {"search_term": search_term, "archived": archived, "limit": limit, "cwd": self.cwd}
        try:
            response = method(**kwargs)
        except TypeError:
            response = method(search_term=search_term, archived=archived, limit=limit)
        response = await response if inspect.isawaitable(response) else response
        data = getattr(response, "data", response or [])
        return [self._to_dict(item) for item in data]

    async def list_models(self, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        # Keep the compatibility facade on the same paginated catalog path as
        # the durable adapter.  The pinned SDK exposes only the first page via
        # ``models()`` and returns subsequent pages through ``nextCursor``.
        return await super().list_models(include_hidden=include_hidden)

    async def chat(self, conversation_id: str, message: str) -> str:
        parts: list[str] = []
        async for part in self.chat_stream(conversation_id, message):
            parts.append(part)
        result = "".join(parts).strip()
        if not result:
            raise RuntimeError("Codex returned an empty response")
        return result

    async def chat_stream(self, conversation_id: str, message: Any) -> AsyncIterator[str]:
        conversation_id = str(conversation_id)
        lock = self._conversation_locks.setdefault(conversation_id, asyncio.Lock())
        async with lock:
            # Delegate to CodexRuntime's queue-backed stream so callers see
            # progress as soon as the SDK emits it.  The previous facade
            # collected every event and yielded only after the turn ended,
            # which made the legacy WeChat bridge appear unresponsive during
            # long turns.
            async for chunk in super().chat_stream(conversation_id, message):
                yield chunk
            binding = self._bindings.get((conversation_id, "chat", "1", "1"))
            if binding is not None:
                self._threads[conversation_id] = binding.thread

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            try:
                return value.model_dump(
                    mode="json", by_alias=True, exclude_none=True
                )
            except TypeError:
                return value.model_dump(by_alias=True, exclude_none=True)
        if hasattr(value, "__dict__"):
            return dict(value.__dict__)
        if isinstance(value, dict):
            return dict(value)
        fields = (
            "name",
            "path",
            "description",
            "short_description",
            "shortDescription",
            "display_name",
            "displayName",
            "version",
            "content_hash",
            "enabled",
        )
        projected = {name: getattr(value, name) for name in fields if hasattr(value, name)}
        if projected:
            return projected
        return {"value": value}

    @staticmethod
    def _approval_mode_for_legacy() -> Any:
        from .agents.codex_runtime import approval_for_policy

        return approval_for_policy("deny_all")

    @staticmethod
    def _sandbox_for_legacy() -> Any:
        from .agents.codex_runtime import sandbox_for_policy

        return sandbox_for_policy("full-access")

    async def interrupt(self, conversation_or_task_id: str) -> bool:
        """Interrupt a task ID, or the active turn for a legacy conversation."""

        self._assert_loop()
        identifier = str(conversation_or_task_id)
        if identifier in self._active_turns:
            return await super().interrupt(identifier)
        task_id = next(
            (
                task_id
                for task_id, task in self._active_tasks.items()
                if task.conversation_id == identifier
            ),
            None,
        )
        return await super().interrupt(task_id) if task_id is not None else False

    def status(self, conversation_id: str) -> str:
        """Return the legacy ``idle``/``busy: <message>`` status string."""

        self._assert_loop()
        for task_id, task in self._active_tasks.items():
            if task.conversation_id == conversation_id:
                message = self._active_messages.get(task_id, "")
                return f"busy: {message}" if message else "busy"
        return "idle"


__all__ = ["AgentInfo", "CodexAgent"]
