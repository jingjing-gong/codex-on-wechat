"""Legacy ACP protocol agent.

This speaks the original Agent Client Protocol over JSON-RPC:

    initialize -> session/new -> session/prompt (blocks; agent streams
    `session/update` notifications with `agent_message_chunk` text while
    working, and may send a `session/request_permission` request which we
    auto-allow).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Any, Optional

from .base import AgentInfo, default_workspace
from .jsonrpc import StdioJsonRpcConnection

logger = logging.getLogger(__name__)


def _extract_chunk_text(update: dict[str, Any]) -> str:
    """Extract text from a `session/update` `agent_message_chunk` payload."""
    text = update.get("text")
    if text:
        return text
    content = update.get("content")
    if isinstance(content, dict) and content.get("type") == "text":
        return content.get("text", "")
    return ""


def _extract_prompt_result_text(result: Any) -> str:
    """Some ACP agents include response content in the `session/prompt` result
    itself (alongside `stopReason`), instead of only via notifications."""
    if not isinstance(result, dict):
        return ""
    if result.get("text"):
        return result["text"]
    parts = []
    for item in result.get("content", []) or []:
        if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
            parts.append(item["text"])
    return "".join(parts)


class LegacyACPAgent:
    """ACP agent using the generic (legacy) ACP protocol."""

    def __init__(
        self,
        command: str,
        args: Optional[list[str]] = None,
        *,
        name: Optional[str] = None,
        model: str = "",
        cwd: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ):
        self.command = command
        self.args = args or []
        self.name = name or Path(command).name
        self.model = model
        self.cwd = cwd or default_workspace()
        self.env = env

        self._conn: Optional[StdioJsonRpcConnection] = None
        self._started = False
        self._sessions: dict[str, str] = {}
        self._notify_queues: dict[str, asyncio.Queue] = {}

    async def start(self) -> None:
        if self._started:
            return

        self._conn = StdioJsonRpcConnection(
            self.command, self.args, cwd=self.cwd, env=self.env
        )
        self._conn.on_notification("session/update", self._on_session_update)
        self._conn.on_request("session/request_permission", self._on_request_permission)
        await self._conn.start()

        try:
            result = await self._conn.request(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": True, "writeTextFile": True}
                    },
                },
                timeout=30.0,
            )
        except Exception as exc:
            detail = self._conn.last_stderr()
            await self._conn.stop()
            raise RuntimeError(f"agent startup failed: {detail or exc}") from exc

        self._started = True
        logger.info("initialized (pid=%s): %s", self._conn.pid, result)

    async def stop(self) -> None:
        if self._conn:
            await self._conn.stop()
        self._started = False

    def set_cwd(self, cwd: str) -> None:
        self.cwd = cwd

    def info(self) -> AgentInfo:
        return AgentInfo(
            name=self.name,
            type="acp",
            model=self.model,
            command=self.command,
            pid=self._conn.pid if self._conn else 0,
        )

    async def reset_session(self, conversation_id: str) -> str:
        self._sessions.pop(conversation_id, None)
        logger.info(
            "session reset (conversation=%s), creating new session", conversation_id
        )
        session_id, _ = await self._get_or_create_session(conversation_id)
        return session_id

    async def chat(self, conversation_id: str, message: str) -> str:
        if not self._started:
            await self.start()

        session_id, is_new = await self._get_or_create_session(conversation_id)
        pid = self._conn.pid if self._conn else 0
        if is_new:
            logger.info(
                "new session created (pid=%s, session=%s, conversation=%s)",
                pid,
                session_id,
                conversation_id,
            )
        else:
            logger.info(
                "reusing session (pid=%s, session=%s, conversation=%s)",
                pid,
                session_id,
                conversation_id,
            )

        queue: asyncio.Queue = asyncio.Queue()
        self._notify_queues[session_id] = queue
        parts: list[str] = []
        try:
            prompt_task = asyncio.create_task(
                self._conn.request(
                    "session/prompt",
                    {
                        "sessionId": session_id,
                        "prompt": [{"type": "text", "text": message}],
                    },
                    timeout=None,
                )
            )

            while True:
                get_task = asyncio.create_task(queue.get())
                done, _pending = await asyncio.wait(
                    {prompt_task, get_task}, return_when=asyncio.FIRST_COMPLETED
                )

                if get_task in done:
                    update = get_task.result()
                    if update.get("sessionUpdate") == "agent_message_chunk":
                        text = _extract_chunk_text(update)
                        if text:
                            parts.append(text)
                else:
                    get_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await get_task

                if prompt_task in done:
                    break

            # Drain any remaining queued updates that arrived alongside the response.
            while not queue.empty():
                update = queue.get_nowait()
                if update.get("sessionUpdate") == "agent_message_chunk":
                    text = _extract_chunk_text(update)
                    if text:
                        parts.append(text)

            result = prompt_task.result()
        finally:
            self._notify_queues.pop(session_id, None)

        text = "".join(parts).strip()
        if not text:
            text = _extract_prompt_result_text(result)
        if not text:
            raise RuntimeError("agent returned empty response")
        return text

    async def _get_or_create_session(self, conversation_id: str) -> tuple[str, bool]:
        session_id = self._sessions.get(conversation_id)
        if session_id:
            return session_id, False

        result = await self._conn.request(
            "session/new", {"cwd": self.cwd, "mcpServers": []}
        )
        session_id = (result or {}).get("sessionId")
        if not session_id:
            raise RuntimeError("session/new returned empty sessionId")

        self._sessions[conversation_id] = session_id
        return session_id, True

    async def _on_session_update(self, msg: dict[str, Any]) -> None:
        params = msg.get("params", {})
        session_id = params.get("sessionId")
        update = params.get("update", {})
        kind = update.get("sessionUpdate")
        if kind not in ("agent_message_chunk", "agent_thought_chunk"):
            logger.info("session/update (session=%s, type=%s)", session_id, kind)

        queue = self._notify_queues.get(session_id)
        if queue is not None:
            queue.put_nowait(update)

    async def _on_request_permission(self, msg: dict[str, Any]) -> None:
        params = msg.get("params", {})
        options = params.get("options", [])
        option_id = "allow"
        for opt in options:
            if opt.get("kind") == "allow":
                option_id = opt.get("optionId", "allow")
                break

        await self._conn.respond(
            msg["id"],
            result={"outcome": {"outcome": "selected", "optionId": option_id}},
        )
        logger.info("auto-allowed permission request")
