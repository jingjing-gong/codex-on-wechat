"""Task-scoped bridge for controlled Agent messaging and cron drafts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import secrets
import socket
import stat
import time
from typing import Any, Mapping

from .store import QueueFullError


logger = logging.getLogger(__name__)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _public_peer(value: Any) -> dict[str, Any]:
    """Project only fields allowed by the public PeerDescriptor contract."""

    return {
        "agent_id": str(_field(value, "agent_id", "") or ""),
        "display_name": str(_field(value, "display_name", "") or ""),
        "summary": str(_field(value, "summary", "") or ""),
        "capabilities": sorted(
            str(item) for item in (_field(value, "capabilities", ()) or ())
        ),
        "accepted_request_types": sorted(
            str(item)
            for item in (_field(value, "accepted_request_types", ()) or ())
        ),
    }


def _public_scalar(value: Any) -> str:
    """Render one allowlisted bridge value without serializing its object."""

    if value is None:
        return ""
    candidate = getattr(value, "value", value)
    isoformat = getattr(candidate, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(candidate)


def _public_cron_draft(value: Any) -> dict[str, str]:
    """Project only user-visible fields from one natural-cron draft."""

    return {
        "draft_id": _public_scalar(
            _field(value, "draft_id", _field(value, "id", ""))
        ),
        "state": _public_scalar(
            _field(value, "state", _field(value, "status", ""))
        ),
        "schedule_kind": _public_scalar(
            _field(value, "schedule_kind", _field(value, "kind", ""))
        ),
        "schedule_expression": _public_scalar(
            _field(value, "schedule_expression", _field(value, "expression", ""))
        ),
        "timezone_name": _public_scalar(
            _field(value, "timezone_name", _field(value, "timezone", ""))
        ),
        "prompt": _public_scalar(_field(value, "prompt", "")),
        "next_fire_at": _public_scalar(_field(value, "next_fire_at", "")),
        "expires_at": _public_scalar(_field(value, "expires_at", "")),
    }


def _public_cron_job(value: Any) -> dict[str, str]:
    """Project the narrow confirmation fields from a committed cron job."""

    return {
        "job_id": _public_scalar(
            _field(value, "job_id", _field(value, "id", ""))
        ),
        "schedule_kind": _public_scalar(
            _field(value, "schedule_kind", _field(value, "kind", ""))
        ),
        "schedule_expression": _public_scalar(
            _field(value, "schedule_expression", _field(value, "expression", ""))
        ),
        "timezone_name": _public_scalar(
            _field(value, "timezone_name", _field(value, "timezone", ""))
        ),
        "next_fire_at": _public_scalar(_field(value, "next_fire_at", "")),
    }


def _cron_result_parts(value: Any) -> tuple[Any, Any]:
    """Accept direct records and manager wrappers without exposing either."""

    draft = _field(value, "draft", None)
    job = _field(value, "job", None)
    return (draft if draft is not None else value, job if job is not None else value)


def _public_pending_drafts(value: Any) -> list[dict[str, str]]:
    raw = _field(value, "drafts", value)
    if raw is None:
        return []
    if isinstance(raw, Mapping) or _field(raw, "draft_id", None) is not None:
        records = (raw,)
    elif isinstance(raw, (str, bytes, bytearray)):
        raise ValueError("pending cron drafts result is invalid")
    else:
        try:
            records = tuple(raw)
        except TypeError as exc:
            raise ValueError("pending cron drafts result is invalid") from exc
    result = [_public_cron_draft(record) for record in records]
    if any(not draft["draft_id"] for draft in result):
        raise ValueError("pending cron draft has no durable identity")
    return result


@dataclass(frozen=True, slots=True)
class AgentBridgeGrant:
    """One unguessable, execution-bound bridge authorization."""

    task_id: str
    execution_id: str
    agent_id: str
    expires_at: float


class AgentBridgeCapabilityAuthority:
    """Issue short-lived capabilities that never enter durable storage."""

    def __init__(self, *, ttl_seconds: float = 60 * 60) -> None:
        if ttl_seconds <= 0:
            raise ValueError("Agent bridge capability TTL must be positive")
        self.ttl_seconds = float(ttl_seconds)
        self._grants: dict[str, AgentBridgeGrant] = {}

    def _prune(self, now: float) -> None:
        self._grants = {
            token: grant
            for token, grant in self._grants.items()
            if grant.expires_at > now
        }

    def issue(self, task: Any) -> str:
        """Create a bearer capability bound to one task execution."""

        task_id = str(_field(task, "task_id", "") or "").strip()
        execution_id = str(_field(task, "execution_id", "") or "").strip()
        agent_id = str(_field(task, "agent_id", "") or "").strip()
        if not task_id or not execution_id or not agent_id:
            raise ValueError(
                "Agent bridge capability requires task, execution, and Agent IDs"
            )
        now = time.monotonic()
        self._prune(now)
        token = secrets.token_urlsafe(32)
        self._grants[token] = AgentBridgeGrant(
            task_id=task_id,
            execution_id=execution_id,
            agent_id=agent_id,
            expires_at=now + self.ttl_seconds,
        )
        return token

    def validate(self, token: str, *, task_id: str) -> AgentBridgeGrant:
        """Resolve a capability without revealing which binding mismatched."""

        supplied = str(token or "").strip()
        requested_task = str(task_id or "").strip()
        now = time.monotonic()
        self._prune(now)
        grant = self._grants.get(supplied)
        if grant is None or grant.task_id != requested_task:
            raise PermissionError("invalid or expired Agent bridge capability")
        return grant

    def clear(self) -> None:
        """Revoke every issued capability at the process bridge boundary."""

        self._grants.clear()


class AgentBridgeServer:
    """Serve one bounded JSON request per owner-only Unix-socket connection.

    The bridge is deliberately a thin facade.  It never writes SQLite or
    constructs mailbox/cron records itself: discovery, messaging, and cron
    drafts delegate to the manager with active-execution enforcement.
    """

    def __init__(
        self,
        manager: Any,
        socket_path: str | os.PathLike[str],
        *,
        capability_authority: AgentBridgeCapabilityAuthority | None = None,
        request_timeout: float = 10.0,
        max_request_bytes: int = 64 * 1024,
    ) -> None:
        if max_request_bytes < 256:
            raise ValueError("max_request_bytes must be at least 256")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.manager = manager
        self.socket_path = Path(socket_path).expanduser().resolve()
        self.request_timeout = float(request_timeout)
        self.max_request_bytes = int(max_request_bytes)
        self.capability_authority = (
            capability_authority or AgentBridgeCapabilityAuthority()
        )
        self._server: asyncio.AbstractServer | None = None
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._socket_identity: tuple[int, int] | None = None
        self._clients: set[asyncio.StreamWriter] = set()
        self._client_tasks: set[asyncio.Task[None]] = set()

    @property
    def started(self) -> bool:
        return self._server is not None

    def _assert_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("AgentBridgeServer must stay on its owning asyncio loop")
        return loop

    async def _prepare_socket_path(self) -> None:
        try:
            details = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(details.st_mode):
            raise RuntimeError(
                f"Agent bridge path exists and is not a socket: {self.socket_path}"
            )

        # Do not unlink another live bot's bridge.  A refused connection is the
        # normal stale-socket shape after a process crash.
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self.socket_path)), timeout=0.25
            )
        except (ConnectionRefusedError, FileNotFoundError):
            current = self.socket_path.lstat()
            if (current.st_dev, current.st_ino) != (details.st_dev, details.st_ino):
                raise RuntimeError("Agent bridge socket changed during startup")
            self.socket_path.unlink()
            return
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"Agent bridge socket is already in use: {self.socket_path}"
            ) from exc
        else:
            del reader
            writer.close()
            await writer.wait_closed()
            raise RuntimeError(
                f"Agent bridge socket is already in use: {self.socket_path}"
            )

    async def start(self) -> None:
        self._assert_loop()
        if self._server is not None:
            return
        self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        await self._prepare_socket_path()
        # Bind and chmod synchronously before listening. ``start_unix_server``
        # with a pathname may listen during its awaited setup, briefly exposing
        # the umask-derived socket mode before a later chmod.
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server: asyncio.AbstractServer | None = None
        try:
            listener.bind(str(self.socket_path))
            os.chmod(self.socket_path, 0o600)
            listener.setblocking(False)
            server = await asyncio.start_unix_server(
                self._accept_client,
                sock=listener,
                limit=self.max_request_bytes + 1,
            )
            details = self.socket_path.lstat()
            if not stat.S_ISSOCK(details.st_mode):
                raise RuntimeError("Agent bridge path is not a Unix socket")
            self._socket_identity = (details.st_dev, details.st_ino)
            self._server = server
        except BaseException:
            if server is not None:
                server.close()
                await server.wait_closed()
            else:
                listener.close()
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            raise

    async def stop(self) -> None:
        self._assert_loop()
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()
        clients = tuple(self._clients)
        for writer in clients:
            writer.close()
        tasks = tuple(self._client_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._clients.clear()
        self._client_tasks.clear()
        self.capability_authority.clear()
        identity, self._socket_identity = self._socket_identity, None
        if identity is not None:
            try:
                details = self.socket_path.lstat()
            except FileNotFoundError:
                return
            if (details.st_dev, details.st_ino) == identity:
                self.socket_path.unlink()

    async def __aenter__(self) -> "AgentBridgeServer":
        await self.start()
        return self

    async def __aexit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        await self.stop()

    @staticmethod
    def _required_text(
        request: Mapping[str, Any], name: str, *, maximum: int
    ) -> str:
        value = str(request.get(name, "") or "").strip()
        if not value:
            raise ValueError(f"{name} is required")
        if len(value) > maximum:
            raise ValueError(f"{name} is too long")
        return value

    @staticmethod
    def _optional_text(
        request: Mapping[str, Any], name: str, *, maximum: int
    ) -> str:
        value = str(request.get(name, "") or "").strip()
        if len(value) > maximum:
            raise ValueError(f"{name} is too long")
        return value

    async def _dispatch(self, request: Mapping[str, Any]) -> dict[str, Any]:
        operation = str(
            request.get("operation", request.get("op", "")) or ""
        ).strip().lower()
        task_id = self._required_text(request, "task_id", maximum=256)
        capability = self._required_text(request, "capability", maximum=512)
        grant = self.capability_authority.validate(
            capability,
            task_id=task_id,
        )
        if operation == "list":
            request_type = str(request.get("request_type", "ask") or "ask").strip()
            peers = await self.manager.list_agent_peers(
                task_id,
                request_type=request_type,
                require_active_task=True,
                required_execution_id=grant.execution_id,
            )
            return {"ok": True, "agents": [_public_peer(peer) for peer in peers]}
        if operation == "send":
            destination = self._required_text(
                request, "destination_agent_id", maximum=64
            )
            content = self._required_text(request, "content", maximum=60 * 1024)
            request_id = self._required_text(request, "request_id", maximum=256)
            request_type = str(request.get("request_type", "ask") or "ask").strip()
            result = await self.manager.send_agent_message(
                destination,
                content,
                task_id=task_id,
                request_id=request_id,
                request_type=request_type,
                actor="agent-bridge",
                require_active_task=True,
                required_execution_id=grant.execution_id,
            )
            return {
                "ok": True,
                "mailbox_id": str(_field(result, "mailbox_id", "") or ""),
                "message_id": str(_field(result, "message_id", "") or ""),
                "request_id": str(
                    _field(result, "request_id", request_id) or request_id
                ),
                "destination_agent_id": str(
                    _field(result, "destination_agent_id", destination) or destination
                ),
            }
        if operation == "cron_propose":
            schedule = self._required_text(request, "schedule", maximum=512)
            prompt = self._required_text(request, "prompt", maximum=60 * 1024)
            result = await self.manager.propose_natural_cron(
                task_id,
                schedule,
                prompt,
                required_execution_id=grant.execution_id,
            )
            draft, _job = _cron_result_parts(result)
            public = _public_cron_draft(draft)
            if not public["draft_id"]:
                raise ValueError("natural cron proposal has no durable identity")
            return {
                "ok": True,
                "operation": operation,
                # A proposal is deliberately not a cron job.  Keeping this
                # explicit prevents a language model from interpreting an
                # otherwise successful tool call as completed persistence.
                "created": False,
                **public,
            }
        if operation == "cron_confirm":
            draft_id = self._optional_text(request, "draft_id", maximum=256)
            result = await self.manager.confirm_natural_cron(
                task_id,
                draft_id=draft_id,
                required_execution_id=grant.execution_id,
            )
            draft, job = _cron_result_parts(result)
            public_draft = _public_cron_draft(draft)
            public_job = _public_cron_job(job)
            resolved_draft_id = public_draft["draft_id"] or draft_id
            if not resolved_draft_id or not public_job["job_id"]:
                raise ValueError("natural cron confirmation result is incomplete")
            return {
                "ok": True,
                "operation": operation,
                "created": True,
                **public_draft,
                **{
                    name: value or public_draft.get(name, "")
                    for name, value in public_job.items()
                },
                "draft_id": resolved_draft_id,
                "state": public_draft["state"] or "confirmed",
            }
        if operation == "cron_cancel":
            draft_id = self._optional_text(request, "draft_id", maximum=256)
            result = await self.manager.cancel_natural_cron(
                task_id,
                draft_id=draft_id,
                required_execution_id=grant.execution_id,
            )
            draft, _job = _cron_result_parts(result)
            public = _public_cron_draft(draft)
            resolved_draft_id = public["draft_id"] or draft_id
            if not resolved_draft_id:
                raise ValueError("natural cron cancellation has no durable identity")
            return {
                "ok": True,
                "operation": operation,
                "created": False,
                **public,
                "draft_id": resolved_draft_id,
                "state": public["state"] or "cancelled",
            }
        if operation == "cron_pending":
            result = await self.manager.pending_natural_cron(
                task_id,
                required_execution_id=grant.execution_id,
            )
            return {
                "ok": True,
                "operation": operation,
                "created": False,
                "drafts": _public_pending_drafts(result),
            }
        raise ValueError(
            "operation must be 'list', 'send', 'cron_propose', "
            "'cron_confirm', 'cron_cancel', or 'cron_pending'"
        )

    @staticmethod
    def _error_response(exc: BaseException) -> dict[str, Any]:
        if isinstance(exc, QueueFullError):
            code = "queue_full"
        elif isinstance(exc, PermissionError):
            code = "permission_denied"
        elif isinstance(exc, KeyError):
            code = "not_found"
        elif isinstance(exc, (TypeError, ValueError, json.JSONDecodeError)):
            code = "invalid_request"
        else:
            code = "bridge_error"
        message = str(exc) or exc.__class__.__name__
        return {"ok": False, "error": code, "message": message[:512]}

    def _accept_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.create_task(
            self._handle_client(reader, writer), name="agent-bridge-client"
        )
        self._client_tasks.add(task)

        def finished(value: asyncio.Task[None]) -> None:
            self._client_tasks.discard(value)
            try:
                value.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Agent bridge client handler failed")

        task.add_done_callback(finished)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._clients.add(writer)
        try:
            response: dict[str, Any]
            try:
                raw = await asyncio.wait_for(
                    reader.readline(), timeout=self.request_timeout
                )
                if not raw:
                    raise ValueError("request is empty")
                if len(raw.rstrip(b"\r\n")) > self.max_request_bytes:
                    raise ValueError("request exceeds size limit")
                value = json.loads(raw)
                if not isinstance(value, Mapping):
                    raise ValueError("request must be a JSON object")
                response = await self._dispatch(value)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if not isinstance(
                    exc,
                    (
                        PermissionError,
                        KeyError,
                        TypeError,
                        ValueError,
                        json.JSONDecodeError,
                    ),
                ):
                    logger.exception("Agent bridge request failed")
                response = self._error_response(exc)
            try:
                payload = json.dumps(
                    response,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8") + b"\n"
                writer.write(payload)
                await asyncio.wait_for(writer.drain(), timeout=self.request_timeout)
            except (BrokenPipeError, ConnectionError, asyncio.TimeoutError):
                pass
        finally:
            self._clients.discard(writer)
            writer.close()
            await asyncio.gather(writer.wait_closed(), return_exceptions=True)


__all__ = [
    "AgentBridgeCapabilityAuthority",
    "AgentBridgeGrant",
    "AgentBridgeServer",
]
