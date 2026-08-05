"""Codex app-server protocol agent.

Codex has its own native JSON-RPC protocol (`codex app-server --listen
stdio://`), distinct from the generic ACP protocol used by other agents
(`codex-acp` is a separate, generic-ACP wrapper — see `legacy_acp.py`).
codex-wechat-bot uses the native app-server protocol as:

    initialize -> (client sends "initialized" notification) -> thread/start
    -> turn/start (returns quickly; actual content streams via
    `item/agentMessage/delta` / `item/started` / `turn/completed`
    notifications).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from typing import Any, Optional

from .base import AgentInfo, default_workspace
from .jsonrpc import StdioJsonRpcConnection

logger = logging.getLogger(__name__)

# Matches a leading `$skill-name` mention (Codex's own "$skill-name <prompt>"
# UI shortcut for attaching a skill to a turn), e.g. "$academic-research-suite
# summarize this" -> group(1)="academic-research-suite", group(2)="summarize this".
_SKILL_MENTION_RE = re.compile(r"^\$([\w:-]+)\s*(.*)$", re.DOTALL)


class CodexAppServerAgent:
    """ACP agent for Codex's native `app-server` protocol."""

    def __init__(
        self,
        command: str,
        args: Optional[list[str]] = None,
        *,
        model: str = "",
        cwd: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ):
        self.command = command
        self.args = args or []
        self.model = model
        self.cwd = cwd or default_workspace()
        self.env = env

        self._conn: Optional[StdioJsonRpcConnection] = None
        self._started = False
        self._threads: dict[str, str] = {}
        self._turn_queues: dict[str, asyncio.Queue] = {}
        self._conversation_models: dict[str, str] = {}
        self._skills_cache: Optional[list[dict[str, Any]]] = None

    async def start(self) -> None:
        if self._started:
            return

        self._conn = StdioJsonRpcConnection(
            self.command, self.args, cwd=self.cwd, env=self.env
        )
        self._conn.on_notification("item/agentMessage/delta", self._on_item_delta)
        self._conn.on_notification("item/started", self._on_item_started)
        self._conn.on_notification("turn/completed", self._on_turn_completed)
        self._conn.on_request("turn/approval/request", self._on_approval_request)
        await self._conn.start()

        try:
            await self._conn.request(
                "initialize",
                {"clientInfo": {"name": "acp-agents", "version": "0.1.0"}},
                timeout=30.0,
            )
            await self._conn.notify("initialized")
        except Exception as exc:
            detail = self._conn.last_stderr()
            await self._conn.stop()
            raise RuntimeError(f"agent startup failed: {detail or exc}") from exc

        self._started = True
        logger.info("initialized codex app-server (pid=%s)", self._conn.pid)

    async def stop(self) -> None:
        if self._conn:
            await self._conn.stop()
        self._started = False

    def set_cwd(self, cwd: str) -> None:
        self.cwd = cwd

    def info(self) -> AgentInfo:
        return AgentInfo(
            name="codex",
            type="acp",
            model=self.model,
            command=self.command,
            pid=self._conn.pid if self._conn else 0,
        )

    async def reset_session(self, conversation_id: str) -> str:
        self._threads.pop(conversation_id, None)
        logger.info(
            "thread reset (conversation=%s), creating new thread", conversation_id
        )
        thread_id, _ = await self._get_or_create_thread(conversation_id)
        return thread_id

    def set_model(self, conversation_id: str, model: str) -> None:
        """Override the model used for `conversation_id`'s future turns.

        Takes effect on the next `chat()` call (no new thread required); pass
        an empty string to fall back to the agent's default model.
        """
        if model:
            self._conversation_models[conversation_id] = model
        else:
            self._conversation_models.pop(conversation_id, None)
        logger.info(
            "model set (conversation=%s, model=%s)",
            conversation_id,
            model or "<default>",
        )

    def get_model(self, conversation_id: str) -> str:
        """Return the effective model for `conversation_id` (override or default)."""
        return self._conversation_models.get(conversation_id, self.model)

    def get_thread_id(self, conversation_id: str) -> Optional[str]:
        """Return the Codex thread currently bound to a conversation."""
        return self._threads.get(conversation_id)

    async def list_threads(
        self,
        *,
        search_term: Optional[str] = None,
        archived: Optional[bool] = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List persisted Codex threads through the native app-server API."""
        if not self._started:
            await self.start()

        threads: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "archived": archived,
                "sortKey": "created_at",
                "sortDirection": "desc",
            }
            if search_term:
                params["searchTerm"] = search_term
            if cursor:
                params["cursor"] = cursor
            result = await self._conn.request("thread/list", params)
            threads.extend((result or {}).get("data", []))
            cursor = (result or {}).get("nextCursor")
            if not cursor:
                return threads

    async def resume_thread(
        self,
        conversation_id: str,
        thread_id: str,
        *,
        include_turns: bool = False,
    ) -> dict[str, Any]:
        """Resume a persisted Codex thread and bind it to a conversation."""
        if not self._started:
            await self.start()

        params: dict[str, Any] = {
            "threadId": thread_id,
            "cwd": self.cwd,
            "approvalPolicy": "never",
            "sandbox": {"type": "danger-full-access"},
        }
        if not include_turns:
            params["excludeTurns"] = True
        result = await self._conn.request("thread/resume", params)
        resumed = (result or {}).get("thread", {})
        resumed_id = resumed.get("id") or thread_id
        self._threads[conversation_id] = resumed_id
        return resumed

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a persisted Codex thread and clear local bindings."""
        if not self._started:
            await self.start()
        await self._conn.request("thread/delete", {"threadId": thread_id})
        for conversation_id, bound_id in list(self._threads.items()):
            if bound_id == thread_id:
                self._threads.pop(conversation_id, None)

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        """Return available models via the `model/list` RPC method.

        Each entry is the raw `Model` object from the protocol (keys include
        `id`, `model`, `displayName`, `description`, `isDefault`, `hidden`).
        Paginates through `nextCursor` automatically.
        """
        if not self._started:
            await self.start()

        models: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            params: dict[str, Any] = {}
            if include_hidden:
                params["includeHidden"] = True
            if cursor:
                params["cursor"] = cursor
            result = await self._conn.request("model/list", params)
            models.extend((result or {}).get("data", []))
            cursor = (result or {}).get("nextCursor")
            if not cursor:
                break
        return models

    async def list_skills(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        """Return available skills via the `skills/list` RPC method.

        Each entry is a raw `SkillMetadata` object (keys include `name`,
        `description`, `path`, `scope`, `enabled`, `interface`). The protocol
        groups skills by root/cwd; this flattens them into a single list.
        Cached after the first call — pass `refresh=True` to re-fetch.
        """
        if self._skills_cache is not None and not refresh:
            return self._skills_cache

        if not self._started:
            await self.start()

        result = await self._conn.request("skills/list", {})
        skills: list[dict[str, Any]] = []
        for entry in (result or {}).get("data", []):
            skills.extend(entry.get("skills", []))
        self._skills_cache = skills
        return skills

    async def _find_skill(self, name: str) -> Optional[dict[str, Any]]:
        name_lower = name.lower()
        for skill in await self.list_skills():
            if skill.get("name", "").lower() == name_lower:
                return skill
        return None

    async def _build_turn_input(self, message: str) -> list[dict[str, Any]]:
        """Build the `turn/start` input array.

        Resolves a leading `$skill-name` mention (Codex's own UI shortcut for
        attaching a skill to a turn) into a structured `SkillUserInput` item
        alongside the remaining text, mirroring what the Codex app sends when
        a skill is attached. Falls back to plain text if there's no mention,
        or the name doesn't match a known skill.
        """
        match = _SKILL_MENTION_RE.match(message.strip())
        if match:
            skill_name, rest = match.group(1), match.group(2)
            skill = await self._find_skill(skill_name)
            if skill:
                items: list[dict[str, Any]] = [
                    {"type": "skill", "name": skill["name"], "path": skill["path"]}
                ]
                if rest:
                    items.append({"type": "text", "text": rest})
                return items
        return [{"type": "text", "text": message}]

    async def chat(self, conversation_id: str, message: str) -> str:
        if not self._started:
            await self.start()

        thread_id, is_new = await self._get_or_create_thread(conversation_id)
        pid = self._conn.pid if self._conn else 0
        if is_new:
            logger.info(
                "new thread created (pid=%s, thread=%s, conversation=%s)",
                pid,
                thread_id,
                conversation_id,
            )
        else:
            logger.info(
                "reusing thread (pid=%s, thread=%s, conversation=%s)",
                pid,
                thread_id,
                conversation_id,
            )

        queue: asyncio.Queue = asyncio.Queue()
        self._turn_queues[thread_id] = queue
        parts: list[str] = []

        input_items = await self._build_turn_input(message)

        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "approvalPolicy": "never",
            "input": input_items,
            "sandboxPolicy": {"type": "dangerFullAccess"},
            "cwd": self.cwd,
        }
        model = self.get_model(conversation_id)
        if model:
            turn_params["model"] = model

        # `turn/start`'s own RPC response is just a fast "accepted" ack, not
        # the final answer — the actual content streams in via
        # `item/agentMessage/delta` / `turn/completed` notifications. Run the
        # request itself in the background and only surface request-level
        # failures into the notification queue.
        turn_task = asyncio.create_task(self._run_turn_start(turn_params, queue))

        try:
            while True:
                item = await queue.get()
                if item.get("kind") == "error":
                    raise RuntimeError(f"turn error: {item.get('text')}")
                if item.get("delta"):
                    parts.append(item["delta"])
                if item.get("text"):
                    parts.append(item["text"])
                if item.get("kind") == "completed":
                    break
        finally:
            self._turn_queues.pop(thread_id, None)
            if not turn_task.done():
                turn_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await turn_task

        text = "".join(parts).strip()
        if not text:
            raise RuntimeError("agent returned empty response")
        return text

    async def _run_turn_start(
        self, params: dict[str, Any], queue: asyncio.Queue
    ) -> None:
        try:
            await self._conn.request("turn/start", params, timeout=None)
        except Exception as exc:
            queue.put_nowait({"kind": "error", "text": str(exc)})

    async def _get_or_create_thread(self, conversation_id: str) -> tuple[str, bool]:
        thread_id = self._threads.get(conversation_id)
        if thread_id:
            return thread_id, False

        params: dict[str, Any] = {
            "approvalPolicy": "never",
            "cwd": self.cwd,
            "sandbox": "danger-full-access",
        }
        if self.model:
            params["model"] = self.model

        result = await self._conn.request("thread/start", params)
        thread_id = (result or {}).get("thread", {}).get("id")
        if not thread_id:
            raise RuntimeError("thread/start returned empty thread id")

        self._threads[conversation_id] = thread_id
        return thread_id, True

    async def _on_item_delta(self, msg: dict[str, Any]) -> None:
        params = msg.get("params", {})
        delta = params.get("delta", "")
        if delta:
            self._dispatch(params.get("threadId"), {"delta": delta})

    async def _on_item_started(self, msg: dict[str, Any]) -> None:
        params = msg.get("params", {})
        item = params.get("item", {})
        if item.get("type") != "agentMessage":
            return
        for content in item.get("content", []) or []:
            if content.get("type") == "text" and content.get("text"):
                self._dispatch(params.get("threadId"), {"text": content["text"]})

    async def _on_turn_completed(self, msg: dict[str, Any]) -> None:
        params = msg.get("params", {})
        self._dispatch(params.get("threadId"), {"kind": "completed"})

    async def _on_approval_request(self, msg: dict[str, Any]) -> None:
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
        logger.info("auto-allowed turn approval request")

    def _dispatch(self, thread_id: Optional[str], event: dict[str, Any]) -> None:
        queue = self._turn_queues.get(thread_id) if thread_id else None
        if queue is None and len(self._turn_queues) == 1:
            # Some codex protocol versions omit threadId on events; fall back
            # to the only in-flight turn.
            queue = next(iter(self._turn_queues.values()))
        if queue is not None:
            queue.put_nowait(event)
