"""Durable bridge between one real WeChat account and process-isolated Agents.

The supervisor owns WeChat, SQLite, routing, and delivery. Its background
asyncio loop hosts only orchestration and one ``ProcessAgentRuntime`` proxy per
enabled Agent; every proxy starts a persistent child process containing that
Agent's private ``CodexRuntime``, SDK client, event loop, and thread state.
Monitor worker threads submit channel work to the supervisor loop with
``asyncio.run_coroutine_threadsafe``. Agent turns then cross the private IPC
boundary and never execute in the supervisor process.

Usage:
    python src/codex_wechat_bot.py
    python src/codex_wechat_bot.py --login   # log in and save credentials
    python src/codex_wechat_bot.py --logout  # forget saved WeChat login
"""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
import difflib
import json
import logging
import math
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import qrcode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import CodexAgent  # noqa: E402
from src.agents.codex_runtime import default_workspace  # noqa: E402
from src.runtime.agent_bridge import (  # noqa: E402
    AgentBridgeCapabilityAuthority,
    AgentBridgeServer,
)
from src.runtime.diagnostics import configure_persistent_logging  # noqa: E402
from src.runtime.manager import TaskManager  # noqa: E402
from src.runtime.process_agent import ProcessAgentRuntime  # noqa: E402
from src.runtime.media import (  # noqa: E402
    AttachmentStore,
    ManagedImageOutputPublisher,
)
from src.runtime.registry import AgentRegistry, codex_profile  # noqa: E402
from src.runtime.sqlite_store import (  # noqa: E402
    DEFAULT_MAILBOX_TTL_SECONDS,
    DEFAULT_MAX_AGENT_QUEUE,
    DEFAULT_MAX_GLOBAL_AGENT_QUEUE,
    DEFAULT_REPLY_AGGREGATION_MAX_AGE_SECONDS,
    SQLiteStore,
)
from src.runtime.supervisor import (  # noqa: E402
    ChannelAccountOwnership,
    CredentialMutationOwnership,
    SupervisorOwnership,
    SupervisorResourcesStillLive,
)
from src.runtime.shell import run_bounded_shell_process  # noqa: E402
from src.runtime.worker import AgentMailboxSupervisor, _mailbox_result_content  # noqa: E402
from src.channels.wechat import (  # noqa: E402
    WeChatDeliveryWorker,
    WeChatGateway,
    WeChatMediaDeliveryWorker,
    _bounded_public_error as _sanitize_public_error,
    _format_shell_error as _format_bounded_shell_error,
    _format_shell_markdown as _format_bounded_shell_markdown,
    send_media_delivery,
)
from src.runtime.skills import (  # noqa: E402
    SkillSyntaxError,
    find_skill,
    format_skills_markdown,
    parse_skill_invocation,
)
from wechat_ilink import (  # noqa: E402
    Client,
    Monitor,
    accounts_dir,
    delete_all_credentials,
    fetch_qrcode,
    format_message_summary,
    load_all_credentials,
    poll_qr_status,
    save_credentials,
    send_text_reply,
    send_typing_state,
)
from wechat_ilink.types import (  # noqa: E402
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
)

logger = logging.getLogger("codex_wechat_bot")

KNOWN_COMMANDS = [
    "/help",
    "/clear",
    "/reset",
    "/status",
    "/tasks",
    "/retry",
    "/cancel",
    "/recv",
    "/model",
    "/models",
    "/listmodel",
    "/listmodels",
    "/skills",
    "/listskill",
    "/listskills",
    "/session",
    "/sessions",
    "/delsession",
    "/sh",
]

HELP_TEXT = """## Commands

### Conversation
- `/help` - Show this help
- `/clear` - Clear conversation context
- `/reset` - Clear conversation context
- `/status` - Show whether Codex is busy or idle

### Tasks
- `/tasks [limit]` - List durable tasks
- `/retry <task_id>` - Explicitly retry a failed or orphaned task
- `/cancel [task_id]` - Cancel a task, or the current running task when omitted

-### Models And Sessions
- `/model [<model-id> <effort|default>|effort <effort|default>]` - Show or set the model and reasoning effort
- `/models` - List available models and reasoning levels
- `/session [id]` - Switch to or create a session
- `/sessions` - List your sessions
- `/delsession <id>` - Delete a session

### Skills
- `/skills` - List enabled skills
- `$<skill> <task description>` - Run a task with a selected skill

### Shell
- `/sh <command>` - Execute a bounded shell command in the bot workspace
"""

_MAX_SHELL_OUTPUT = 6000
_SHELL_TIMEOUT = 30
_CODEX_TASK_TIMEOUT = None
_DEFAULT_MAX_AGENT_PROCESSES = 16
# The durable bot's operating mode is administrator-selected at startup.  A
# new immutable profile version avoids conflicting with databases seeded by
# earlier releases whose Codex profile defaulted to read-only ``chat``.
_DURABLE_CODEX_PROFILE_VERSION = 3


def _format_shell_result(command: str, exit_code: int, output: str) -> str:
    """Format a completed legacy shell command as bounded safe Markdown."""
    return _format_bounded_shell_markdown(
        command,
        f"exit code: {exit_code}\n{output}",
    )


def _format_shell_error(message: Any) -> str:
    """Format a bounded shell failure without trusting exception text."""
    return _format_bounded_shell_error(message)


def _public_error(value: Any) -> str:
    """Sanitize a legacy response even though public launch uses durability."""

    return _sanitize_public_error(value, max_length=512) or "operation failed"


def run_shell_command(
    command: str,
    *,
    cwd: Path | None = None,
    timeout: int = _SHELL_TIMEOUT,
    max_output: int = _MAX_SHELL_OUTPUT,
) -> str:
    """Execute a shell command and format a bounded result for WeChat."""
    completed = run_bounded_shell_process(
        command,
        cwd=cwd or Path(__file__).resolve().parent.parent,
        timeout=timeout,
        max_output=max_output,
    )
    return _format_shell_result(command, completed.returncode, completed.output)


def _is_codex_thread_id(value: str) -> bool:
    """Return whether a value is a UUID accepted by Codex thread/resume."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return True


def _parse_model_command(argument: str) -> tuple[str, str, str]:
    """Return a model-command action, model ID, and requested reasoning effort."""
    parts = argument.split()
    if not parts:
        return "show", "", ""
    if parts[0].lower() == "effort":
        if len(parts) != 2:
            raise ValueError("usage: /model effort <effort|default>")
        return "set-effort", "", parts[1]
    if len(parts) != 2:
        raise ValueError("usage: /model <model-id> <effort>")
    return "set-model", parts[0], parts[1]


def _is_command(text: str) -> bool:
    """Return whether a message is a slash command after leading whitespace."""
    return text.lstrip().startswith("/")


def _find_model(models: list[dict[str, Any]], model_id: str) -> dict[str, Any] | None:
    return next((model for model in models if model.get("id") == model_id), None)


def _current_model(
    models: list[dict[str, Any]], configured_model: str
) -> dict[str, Any] | None:
    if configured_model:
        return _find_model(models, configured_model)
    return next((model for model in models if model.get("isDefault")), None)


def _reasoning_efforts(model: dict[str, Any] | None) -> list[str]:
    if not model:
        return []
    efforts: list[str] = []
    for option in model.get("supportedReasoningEfforts", []):
        effort = option.get("reasoningEffort") if isinstance(option, dict) else option
        if isinstance(effort, str) and effort:
            efforts.append(effort)
    return efforts


def _matching_reasoning_effort(
    model: dict[str, Any] | None, requested_effort: str
) -> str | None:
    requested_lower = requested_effort.lower()
    return next(
        (
            effort
            for effort in _reasoning_efforts(model)
            if effort.lower() == requested_lower
        ),
        None,
    )


def _format_models(
    models: list[dict[str, Any]],
    configured_model: str,
    configured_effort: str,
) -> str:
    """Format model capabilities and the active conversation's effective level."""
    current = _current_model(models, configured_model)
    lines = ["available models:"]
    for model in models:
        is_current = current is not None and model["id"] == current.get("id")
        marker = " (current)" if is_current else ""
        marker += " [default]" if model.get("isDefault") else ""
        public_model_id = _sanitize_public_error(
            model["id"], max_length=512
        )
        public_display_name = _sanitize_public_error(
            model.get("displayName", ""), max_length=512
        )
        lines.append(f"- {public_model_id}: {public_display_name}{marker}")

        efforts = ", ".join(
            _sanitize_public_error(effort, max_length=128)
            for effort in _reasoning_efforts(model)
        ) or "(not reported)"
        details: list[str] = []
        default_effort = model.get("defaultReasoningEffort")
        if default_effort:
            details.append(
                "default: "
                + _sanitize_public_error(default_effort, max_length=128)
            )
        if is_current:
            effective_effort = configured_effort or default_effort
            if effective_effort:
                source = "override" if configured_effort else "default"
                details.append(
                    "current: "
                    + _sanitize_public_error(effective_effort, max_length=128)
                    + f" ({source})"
                )
        suffix = f" [{', '.join(details)}]" if details else ""
        lines.append(f"  reasoning: {efforts}{suffix}")
    lines.append("use /model <model-id> <effort> to switch")
    return "\n".join(lines)


@dataclass
class Session:
    session_id: str
    summary: str = "未开始对话"
    thread_id: str | None = None
    reasoning_effort: str = ""
    created_at: str = ""
    updated_at: str = ""


class SessionManager:
    """Persist named sessions and their Codex thread/configuration per user."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or (Path.home() / ".codex-wechat-bot" / "sessions.json")
        self._sessions: dict[str, dict[str, Session]] = {}
        self._active: dict[str, str] = {}
        self._load()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _load(self) -> None:
        try:
            data = json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return

        for user_id, record in data.get("users", {}).items():
            session_records = record.get("sessions", {})
            self._sessions[user_id] = {
                session_id: Session(
                    session_id=session_id,
                    summary=value.get("summary", "未开始对话"),
                    thread_id=value.get("thread_id"),
                    reasoning_effort=value.get("reasoning_effort", ""),
                    created_at=value.get("created_at", ""),
                    updated_at=value.get("updated_at", ""),
                )
                for session_id, value in session_records.items()
            }
            active = record.get("active")
            if active in self._sessions[user_id]:
                self._active[user_id] = active

    def _save(self) -> None:
        payload = {"version": 1, "users": {}}
        for user_id, sessions in self._sessions.items():
            payload["users"][user_id] = {
                "active": self._active.get(user_id),
                "sessions": {
                    session_id: {
                        "summary": session.summary,
                        "thread_id": session.thread_id,
                        "reasoning_effort": session.reasoning_effort,
                        "created_at": session.created_at,
                        "updated_at": session.updated_at,
                    }
                    for session_id, session in sessions.items()
                },
            }

        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temporary_path = tempfile.mkstemp(
            prefix="sessions.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, self.path)
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)

    def _user_sessions(self, user_id: str) -> dict[str, Session]:
        return self._sessions.setdefault(user_id, {})

    def current(self, user_id: str) -> Session:
        sessions = self._user_sessions(user_id)
        session_id = self._active.get(user_id)
        if session_id and session_id in sessions:
            return sessions[session_id]
        session = self.create(user_id)
        self._active[user_id] = session.session_id
        return session

    def create(self, user_id: str, session_id: str | None = None) -> Session:
        sessions = self._user_sessions(user_id)
        requested_id = (session_id or "").strip()
        session_id = requested_id or f"session-{uuid.uuid4().hex[:8]}"
        if session_id in sessions:
            self._active[user_id] = session_id
            self._save()
            return sessions[session_id]
        timestamp = self._now()
        session = Session(
            session_id=session_id,
            created_at=timestamp,
            updated_at=timestamp,
        )
        sessions[session_id] = session
        self._active[user_id] = session_id
        self._save()
        return session

    def switch_or_create(
        self, user_id: str, session_id: str | None
    ) -> tuple[Session, bool]:
        sessions = self._user_sessions(user_id)
        requested_id = (session_id or "").strip()
        if requested_id and requested_id in sessions:
            self._active[user_id] = requested_id
            self._save()
            return sessions[requested_id], False
        return self.create(user_id, requested_id), True

    def list(self, user_id: str) -> list[Session]:
        return list(self._user_sessions(user_id).values())

    def format_list(self, user_id: str) -> str:
        """Format user-facing session aliases without exposing thread UUIDs."""
        session_list = self.list(user_id)
        if not session_list:
            return "sessions: (none)"

        active = self._active.get(user_id)
        lines = ["sessions:"]
        for session in session_list:
            marker = " (current)" if session.session_id == active else ""
            lines.append(f"- {session.session_id}{marker}: {session.summary}")
        return "\n".join(lines)

    def delete(self, user_id: str, session_id: str) -> bool:
        sessions = self._user_sessions(user_id)
        if session_id not in sessions:
            return False
        del sessions[session_id]
        if self._active.get(user_id) == session_id:
            self._active.pop(user_id, None)
        self._save()
        return True

    def set_thread_id(self, user_id: str, thread_id: str) -> None:
        session = self.current(user_id)
        session.thread_id = thread_id
        session.updated_at = self._now()
        self._save()

    def thread_id(self, user_id: str) -> str | None:
        return self.current(user_id).thread_id

    def set_reasoning_effort(self, user_id: str, effort: str) -> None:
        session = self.current(user_id)
        session.reasoning_effort = effort
        session.updated_at = self._now()
        self._save()

    def reasoning_effort(self, user_id: str) -> str:
        return self.current(user_id).reasoning_effort

    def find_by_id(self, user_id: str, session_id: str) -> Session | None:
        return self._user_sessions(user_id).get(session_id)

    def conversation_id(self, user_id: str) -> str:
        return f"{user_id}:session:{self.current(user_id).session_id}"

    def update_summary(self, user_id: str, message: str) -> None:
        session = self.current(user_id)
        if session.summary != "未开始对话":
            return
        summary = " ".join(message.split())
        session.summary = summary[:80] + ("..." if len(summary) > 80 else "")
        session.updated_at = self._now()
        self._save()


def _render_qrcode(login_url: str) -> None:
    """Print an ASCII QR code to the terminal and save a PNG for viewing."""
    qr_obj = qrcode.QRCode(border=1)
    qr_obj.add_data(login_url)
    qr_obj.make(fit=True)
    qr_obj.print_ascii(invert=True)

    img_path = Path(__file__).parent / "login_qrcode.png"
    qr_obj.make_image().save(img_path)
    print(f"QR code image saved to: {img_path}")


def login() -> Client:
    existing = load_all_credentials()
    if existing:
        logger.info("using saved credentials for %s", existing[0].ilink_user_id)
        return Client(existing[0])

    qr = fetch_qrcode()
    print("scan this QR code with WeChat to log in:")
    _render_qrcode(qr.qrcode_img_content)
    creds = poll_qr_status(qr.qrcode, on_status=lambda s: print(f"status: {s}"))
    save_credentials(creds)
    logger.info("login successful, credentials saved")
    return Client(creds)


def logout() -> None:
    count = delete_all_credentials()
    print(
        f"deleted {count} saved credential file(s); next run will require a fresh QR-code login"
    )


def _load_or_authenticate_credentials() -> tuple[Any, bool]:
    """Return exact credentials without constructing a channel client."""

    existing = load_all_credentials()
    if existing:
        logger.info("using saved credentials for %s", existing[0].ilink_user_id)
        return existing[0], False

    qr = fetch_qrcode()
    print("scan this QR code with WeChat to log in:")
    _render_qrcode(qr.qrcode_img_content)
    credentials = poll_qr_status(
        qr.qrcode,
        on_status=lambda status: print(f"status: {status}"),
    )
    return credentials, True


def _login_with_ownership() -> None:
    """Authenticate and publish credentials under mutation/account locks."""

    with CredentialMutationOwnership(accounts_dir()):
        credentials, should_save = _load_or_authenticate_credentials()
        with ChannelAccountOwnership(
            channel="wechat",
            bot_id=credentials.ilink_bot_id,
        ):
            if should_save:
                save_credentials(credentials)
                logger.info("login successful, credentials saved")


def _logout_with_ownership() -> None:
    """Delete credentials only while every discovered account is quiescent."""

    with CredentialMutationOwnership(accounts_dir()):
        account_locks: list[ChannelAccountOwnership] = []
        try:
            for bot_id in sorted(
                {
                    str(credentials.ilink_bot_id or "").strip()
                    for credentials in load_all_credentials()
                    if str(credentials.ilink_bot_id or "").strip()
                }
            ):
                lock = ChannelAccountOwnership(channel="wechat", bot_id=bot_id)
                lock.acquire()
                account_locks.append(lock)
            count = delete_all_credentials()
        finally:
            for lock in reversed(account_locks):
                lock.close()
    print(
        f"deleted {count} saved credential file(s); "
        "next run will require a fresh QR-code login"
    )


class AsyncLoopShutdownError(SupervisorResourcesStillLive):
    """The loop thread is still live, so supervisor locks must be retained."""


class AsyncLoopThread:
    """Runs a persistent asyncio event loop on a background thread.

    The codex agent's subprocess connection (asyncio.Queue/Future objects)
    must stay bound to a single event loop; this lets synchronous callers
    (wechat_ilink's Monitor worker threads) submit coroutines to it safely.
    """

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._state_lock = threading.RLock()
        self._started = False
        self._stop_requested = False
        self._closed = False
        self._ready = threading.Event()
        # Cancellation cleanup normally completes in one event-loop turn.  A
        # bounded wait keeps a synchronous monitor callback from hanging
        # forever when an SDK coroutine misbehaves, while the loop shutdown
        # path still performs a final pending-task drain.
        self._cancel_grace_seconds = 1.0

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        try:
            self.loop.run_forever()
        finally:
            self._shutdown_loop()
            with self._state_lock:
                self._closed = True
                self._stop_requested = True

    def _shutdown_loop(self) -> None:
        """Cancel and drain loop tasks before closing the event loop."""

        if self.loop.is_closed():
            return
        try:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            shutdown_executor = getattr(self.loop, "shutdown_default_executor", None)
            if shutdown_executor is not None:
                self.loop.run_until_complete(shutdown_executor())
        except BaseException:
            # Shutdown must not mask the exception that caused the monitor to
            # exit.  Pending-task diagnostics remain useful for operators.
            logger.debug("asyncio loop cleanup failed", exc_info=True)
        finally:
            self.loop.close()

    def start(self) -> None:
        with self._state_lock:
            if self._started:
                if self._thread.is_alive():
                    return
                raise RuntimeError("AsyncLoopThread has already stopped")
            if self._closed or self.loop.is_closed():
                raise RuntimeError("AsyncLoopThread cannot be restarted")
            self._started = True
            try:
                self._thread.start()
            except BaseException:
                self._started = False
                raise
        if not self._ready.wait(timeout=5):
            self.stop()
            raise RuntimeError("AsyncLoopThread failed to start")

    @staticmethod
    def _close_coro(value: Any) -> None:
        close = getattr(value, "close", None)
        if close is not None:
            try:
                close()
            except Exception:
                logger.debug("could not close unsubmitted coroutine", exc_info=True)

    def _ensure_submit_allowed(self, coro: Any) -> None:
        with self._state_lock:
            allowed = (
                self._started
                and not self._stop_requested
                and not self._closed
                and not self.loop.is_closed()
                and self.loop.is_running()
            )
        if not allowed:
            self._close_coro(coro)
            raise RuntimeError("AsyncLoopThread event loop is not running")
        if threading.current_thread() is self._thread:
            self._close_coro(coro)
            raise RuntimeError("cannot synchronously wait on the owning asyncio loop")

    def run_coro(self, coro, timeout: float | None = None) -> Any:
        self._ensure_submit_allowed(coro)
        settled = threading.Event()

        async def invoke() -> Any:
            try:
                return await coro
            finally:
                settled.set()

        try:
            future = asyncio.run_coroutine_threadsafe(invoke(), self.loop)
        except BaseException:
            self._close_coro(coro)
            raise
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            # ``Future.result(timeout=...)`` does not cancel the coroutine.
            # Cancel and give its ``finally`` blocks a bounded opportunity to
            # finish before the caller unwinds (loop shutdown drains again).
            if future.cancel():
                settled.wait(self._cancel_grace_seconds)
            raise
        except BaseException:
            # Propagate the original exception, but do not leave a submitted
            # coroutine running when the synchronous bridge gives up on it.
            if not future.done():
                future.cancel()
            if future.cancelled():
                settled.wait(self._cancel_grace_seconds)
            raise

    def run_stream(self, stream, on_item) -> None:
        async def consume() -> None:
            async for item in stream:
                on_item(item)

        self.run_coro(consume())

    def stop(self, *, timeout: float | None = 5.0) -> None:
        """Stop and definitively join the loop thread or fail closed.

        Python cannot safely kill an arbitrary in-process thread.  If loop
        cleanup ignores cancellation past the bounded join, raise the fatal
        ownership-retention marker instead of returning while runtime code is
        still able to touch SQLite or channel resources.
        """

        if timeout is not None:
            timeout = float(timeout)
            if timeout < 0:
                raise ValueError("loop shutdown timeout cannot be negative")
        with self._state_lock:
            if not self._started:
                if not self._closed and not self.loop.is_closed():
                    self._closed = True
                    self._stop_requested = True
                    self.loop.close()
                return
            request_stop = not self._stop_requested
            self._stop_requested = True
            thread = self._thread
            if request_stop and not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.stop)
        if threading.current_thread() is thread:
            raise AsyncLoopShutdownError(
                "loop thread cannot definitively join itself during shutdown"
            )
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise AsyncLoopShutdownError(
                "asyncio loop thread did not stop; supervisor ownership retained"
            )
        with self._state_lock:
            if not self._closed or not self.loop.is_closed():
                raise AsyncLoopShutdownError(
                    "asyncio loop shutdown finished without a closed-loop fence"
                )


def _legacy_main() -> None:
    args = set(sys.argv[1:])

    if "--logout" in args:
        logout()
        return

    if "--login" in args:
        wechat_client = login()
        try:
            print("login complete")
        finally:
            wechat_client.close()
        return

    wechat_client = login()
    try:
        _run_legacy(wechat_client)
    finally:
        # Monitor and Agent shutdown happen inside the runner.  Close the HTTP
        # session last so no in-flight handler can lose its transport.
        wechat_client.close()


def _run_legacy(wechat_client: Client) -> None:
    """Run the legacy bridge using an already authenticated client."""

    agent_loop = AsyncLoopThread()
    agent: CodexAgent | None = None
    loop_started = False
    try:
        agent_loop.start()
        loop_started = True
        agent = CodexAgent(turn_timeout=_CODEX_TASK_TIMEOUT)
        agent_loop.run_coro(agent.start(), timeout=30)
        logger.info("codex agent ready: %s", agent.info())
        _serve_legacy(wechat_client, agent_loop, agent)
    finally:
        if agent is not None and loop_started:
            try:
                agent_loop.run_coro(agent.stop(), timeout=10)
            except Exception:
                pass
        if loop_started:
            agent_loop.stop()


def _serve_legacy(
    wechat_client: Client,
    agent_loop: AsyncLoopThread,
    agent: CodexAgent,
) -> None:
    """Serve legacy messages after the Agent lifecycle is established."""

    sessions = SessionManager()

    def conversation_id(user_id: str) -> str:
        return sessions.conversation_id(user_id)

    def sync_reasoning_effort(user_id: str) -> str:
        """Apply the active session's persisted effort to the agent."""
        current_conversation = conversation_id(user_id)
        persisted_effort = sessions.reasoning_effort(user_id)
        if agent.get_reasoning_effort(current_conversation) != persisted_effort:
            agent.set_reasoning_effort(current_conversation, persisted_effort)
        return current_conversation

    def resume_saved_thread(current_conversation: str, saved_thread_id: str) -> None:
        """Resume the Codex thread saved for the active local session."""
        resumed = agent_loop.run_coro(
            agent.resume_thread(current_conversation, saved_thread_id),
            timeout=30,
        )
        if resumed.get("id") != saved_thread_id:
            raise RuntimeError("Codex resumed a different thread")

    def ensure_thread(user_id: str) -> str:
        """Resume the persisted Codex thread for the active bot session."""
        current_conversation = sync_reasoning_effort(user_id)
        saved_thread_id = sessions.thread_id(user_id)
        if (
            saved_thread_id
            and agent.get_thread_id(current_conversation) != saved_thread_id
        ):
            try:
                resume_saved_thread(current_conversation, saved_thread_id)
            except Exception:
                logger.warning("stale Codex thread mapping: %s", saved_thread_id)
                sessions.set_thread_id(user_id, "")
        return current_conversation

    def save_current_thread(user_id: str, current_conversation: str) -> None:
        thread_id = agent.get_thread_id(current_conversation)
        if thread_id:
            sessions.set_thread_id(user_id, thread_id)

    def handle_message(client: Client, msg) -> None:
        print(format_message_summary(msg))
        if (
            msg.message_type != MESSAGE_TYPE_USER
            or msg.message_state != MESSAGE_STATE_FINISH
        ):
            return
        for item in msg.item_list:
            if item.type == ITEM_TYPE_TEXT and item.text_item:
                text = item.text_item.text
                logger.info("from=%s text=%r", msg.from_user_id, text)

                if text.strip().lower() in ("/help", "/commands"):
                    send_text_reply(
                        client, msg.from_user_id, HELP_TEXT, msg.context_token
                    )
                    logger.info("sent help to %s", msg.from_user_id)
                    continue

                stripped = text.strip()
                lower = stripped.lower()

                if lower == "/session" or lower.startswith("/session "):
                    requested_id = stripped[len("/session") :].strip() or None
                    session, created = sessions.switch_or_create(
                        msg.from_user_id, requested_id
                    )
                    if created and requested_id and _is_codex_thread_id(requested_id):
                        sessions.set_thread_id(msg.from_user_id, requested_id)
                        session = sessions.current(msg.from_user_id)
                    elif session.thread_id and not _is_codex_thread_id(
                        session.thread_id
                    ):
                        sessions.set_thread_id(msg.from_user_id, "")
                        session = sessions.current(msg.from_user_id)
                    if session.thread_id:
                        try:
                            current_conversation = sync_reasoning_effort(
                                msg.from_user_id
                            )
                            resume_saved_thread(current_conversation, session.thread_id)
                            session = sessions.current(msg.from_user_id)
                        except Exception as exc:
                            reply = (
                                f"cannot resume session {session.session_id}: "
                                f"{_public_error(exc)}"
                            )
                            send_text_reply(
                                client, msg.from_user_id, reply, msg.context_token
                            )
                            continue
                    action = "created and switched to" if created else "switched to"
                    reply = f"{action} session: {session.session_id}\nsummary: {session.summary}"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info(
                        "session selected for %s: %s (created=%s)",
                        msg.from_user_id,
                        session.session_id,
                        created,
                    )
                    continue

                if lower == "/sessions":
                    reply = sessions.format_list(msg.from_user_id)
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    continue

                if lower == "/sh" or lower.startswith("/sh "):
                    command = stripped[len("/sh") :].strip()
                    if not command:
                        reply = "usage: /sh <command>"
                    else:
                        try:
                            reply = run_shell_command(command)
                        except subprocess.TimeoutExpired:
                            reply = _format_shell_error(
                                f"shell command timed out after {_SHELL_TIMEOUT} seconds"
                            )
                        except OSError as exc:
                            reply = _format_shell_error(
                                "shell command failed to start: "
                                f"{_public_error(exc)}"
                            )
                        except Exception as exc:
                            reply = _format_shell_error(
                                f"shell command failed: {_public_error(exc)}"
                            )
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info(
                        "executed shell command for %s: %r", msg.from_user_id, command
                    )
                    continue

                if lower.startswith("/delsession"):
                    session_id = stripped[len("/delsession") :].strip()
                    if not session_id:
                        reply = "usage: /delsession <session-id>"
                    else:
                        local_session = sessions.find_by_id(
                            msg.from_user_id, session_id
                        )
                        thread_id = (
                            local_session.thread_id if local_session else session_id
                        )
                        if local_session and not thread_id:
                            sessions.delete(msg.from_user_id, session_id)
                            active = sessions.current(msg.from_user_id)
                            reply = (
                                f"deleted local session: {session_id}\n"
                                f"current session: {active.session_id}"
                            )
                        elif not _is_codex_thread_id(thread_id):
                            reply = f"invalid Codex session id: {thread_id}"
                        else:
                            try:
                                agent_loop.run_coro(
                                    agent.delete_thread(thread_id), timeout=30
                                )
                                sessions.delete(msg.from_user_id, session_id)
                                active = sessions.current(msg.from_user_id)
                                reply = (
                                    f"deleted Codex session: {thread_id}\n"
                                    f"current session: {active.session_id}"
                                )
                            except Exception as exc:
                                reply = f"(codex error: {_public_error(exc)})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    continue

                if lower in ("/clear", "/reset"):
                    try:
                        current_conversation = ensure_thread(msg.from_user_id)
                        new_thread_id = agent_loop.run_coro(
                            agent.reset_session(current_conversation),
                            timeout=30,
                        )
                        sessions.set_thread_id(msg.from_user_id, new_thread_id)
                        reply = "context cleared, starting a new conversation"
                    except Exception as exc:
                        reply = f"(codex error: {_public_error(exc)})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info("cleared session for %s", msg.from_user_id)
                    continue

                if lower == "/cancel" or lower.startswith("/cancel "):
                    cancel_args = stripped[len("/cancel") :].split()
                    if len(cancel_args) > 1:
                        reply = "usage: /cancel [task_id]"
                        send_text_reply(
                            client, msg.from_user_id, reply, msg.context_token
                        )
                        continue
                    identifier = (
                        cancel_args[0]
                        if cancel_args
                        else conversation_id(msg.from_user_id)
                    )
                    interrupted = agent_loop.run_coro(
                        agent.interrupt(identifier), timeout=10
                    )
                    if cancel_args:
                        reply = (
                            f"cancel requested: {identifier}"
                            if interrupted
                            else f"cannot cancel task {identifier}"
                        )
                    else:
                        reply = (
                            "Codex turn interrupted"
                            if interrupted
                            else "no Codex turn is currently running"
                        )
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    continue

                if lower == "/status":
                    reply = agent.status(conversation_id(msg.from_user_id))
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    continue

                if lower == "/model" or lower.startswith("/model "):
                    arg = stripped[len("/model") :].strip()
                    current_conversation = sync_reasoning_effort(msg.from_user_id)
                    try:
                        action, requested_model, requested_effort = (
                            _parse_model_command(arg)
                        )
                        models = agent_loop.run_coro(agent.list_models(), timeout=30)
                        configured_model = agent.get_model(current_conversation)
                        selected_model = _current_model(models, configured_model)

                        if action == "show":
                            model_name = (
                                selected_model.get("id")
                                if selected_model
                                else configured_model or "(default)"
                            )
                            configured_effort = agent.get_reasoning_effort(
                                current_conversation
                            )
                            if configured_effort:
                                effort_display = f"{configured_effort} (override)"
                            else:
                                default_effort = (
                                    selected_model.get("defaultReasoningEffort")
                                    if selected_model
                                    else None
                                )
                                effort_display = (
                                    f"{default_effort} (default)"
                                    if default_effort
                                    else "(model default)"
                                )
                            efforts = _reasoning_efforts(selected_model)
                            available = ", ".join(efforts) or "(not reported)"
                            reply = (
                                f"current model: {model_name}\n"
                                f"current reasoning: {effort_display}\n"
                                f"available reasoning: {available}\n"
                                "use /model effort <level|default> to change"
                            )
                        elif action == "set-effort":
                            if requested_effort.lower() == "default":
                                agent.set_reasoning_effort(current_conversation, "")
                                sessions.set_reasoning_effort(msg.from_user_id, "")
                                default_effort = (
                                    selected_model.get("defaultReasoningEffort")
                                    if selected_model
                                    else None
                                )
                                reply = "reasoning reset to model default" + (
                                    f": {default_effort}" if default_effort else ""
                                )
                            else:
                                effort = _matching_reasoning_effort(
                                    selected_model, requested_effort
                                )
                                if not selected_model:
                                    reply = (
                                        "current model is not available; use /models"
                                    )
                                elif not effort:
                                    available = (
                                        ", ".join(_reasoning_efforts(selected_model))
                                        or "(none)"
                                    )
                                    reply = (
                                        f"unsupported reasoning level: {requested_effort}\n"
                                        f"available: {available}"
                                    )
                                else:
                                    agent.set_reasoning_effort(
                                        current_conversation, effort
                                    )
                                    sessions.set_reasoning_effort(
                                        msg.from_user_id, effort
                                    )
                                    reply = (
                                        f"reasoning level set to: {effort} "
                                        "(takes effect on your next message)"
                                    )
                        else:
                            model = _find_model(models, requested_model)
                            if not model:
                                available = ", ".join(
                                    item.get("id", "") for item in models
                                )
                                reply = (
                                    f"unknown model: {requested_model}\n"
                                    f"available: {available}"
                                )
                            else:
                                effort = ""
                                if requested_effort.lower() != "default":
                                    effort = (
                                        _matching_reasoning_effort(
                                            model, requested_effort
                                        )
                                        or ""
                                    )
                                    if not effort:
                                        available = (
                                            ", ".join(_reasoning_efforts(model))
                                            or "(none)"
                                        )
                                        reply = (
                                            f"model {requested_model} does not support "
                                            f"reasoning level: {requested_effort}\n"
                                            f"available: {available}"
                                        )
                                        send_text_reply(
                                            client,
                                            msg.from_user_id,
                                            reply,
                                            msg.context_token,
                                        )
                                        continue

                                agent.set_model(current_conversation, requested_model)
                                agent.set_reasoning_effort(current_conversation, effort)
                                sessions.set_reasoning_effort(msg.from_user_id, effort)
                                if effort:
                                    reply = (
                                        f"model set to: {requested_model}\n"
                                        f"reasoning level: {effort} "
                                        "(takes effect on your next message)"
                                    )
                                else:
                                    default_effort = model.get("defaultReasoningEffort")
                                    reply = f"model set to: {requested_model}"
                                    if default_effort:
                                        reply += f"\nreasoning level: {default_effort} (default)"
                    except ValueError as exc:
                        reply = _public_error(exc)
                    except Exception as exc:
                        reply = f"(codex error: {_public_error(exc)})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info(
                        "model command for %s: %r", msg.from_user_id, arg or "<show>"
                    )
                    continue

                if lower in ("/models", "/listmodel", "/listmodels"):
                    try:
                        models = agent_loop.run_coro(agent.list_models(), timeout=30)
                        current_conversation = sync_reasoning_effort(msg.from_user_id)
                        reply = _format_models(
                            models,
                            agent.get_model(current_conversation),
                            agent.get_reasoning_effort(current_conversation),
                        )
                    except Exception as exc:
                        reply = f"(codex error: {_public_error(exc)})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info("listed models for %s", msg.from_user_id)
                    continue

                if (
                    lower == "/skills"
                    or lower.startswith("/skills ")
                    or lower == "/listskill"
                    or lower.startswith("/listskill ")
                    or lower == "/listskills"
                    or lower.startswith("/listskills ")
                ):
                    command_name = lower.split(maxsplit=1)[0]
                    if len(stripped.split()) > 1:
                        reply = f"usage: {command_name}"
                    else:
                        try:
                            skills = agent_loop.run_coro(agent.list_skills(), timeout=30)
                            reply = format_skills_markdown(skills)
                        except Exception:
                            logger.warning("skill listing unavailable", exc_info=True)
                            reply = "skills unavailable; try again later"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info("listed skills for %s", msg.from_user_id)
                    continue

                if lower.startswith("/"):
                    first_token = lower.split()[0] if lower.split() else lower
                    suggestion = difflib.get_close_matches(
                        first_token, KNOWN_COMMANDS, n=1, cutoff=0.4
                    )
                    reply = (
                        f"unknown command: {first_token}. did you mean {suggestion[0]}?"
                        if suggestion
                        else f"unknown command: {first_token}. try /help"
                    )
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info(
                        "unknown command from %s: %r", msg.from_user_id, stripped
                    )
                    continue

                if _is_command(text):
                    logger.warning("ignoring unhandled command: %r", stripped)
                    continue

                agent_input: Any = text
                if stripped.startswith("$"):
                    try:
                        invocation = parse_skill_invocation(stripped)
                    except SkillSyntaxError as exc:
                        send_text_reply(
                            client, msg.from_user_id, str(exc), msg.context_token
                        )
                        continue
                    if invocation is not None:
                        try:
                            skills = agent_loop.run_coro(agent.list_skills(), timeout=30)
                            definition = find_skill(
                                skills,
                                invocation.name,
                                rehash_local_bundles=True,
                            )
                        except Exception as exc:
                            logger.warning("could not resolve skill: %s", exc)
                            definition = None
                        if definition is None:
                            send_text_reply(
                                client,
                                msg.from_user_id,
                                f"unknown skill: ${invocation.skill_id}. use /skills to see available skills",
                                msg.context_token,
                            )
                            continue
                        agent_input = {
                            "text": invocation.description,
                            "skill": definition.snapshot(),
                        }

                # The Codex turn can take a while to complete.  Notify the
                # user before starting it, but keep this best-effort so a
                # typing API failure never prevents the actual response.
                try:
                    send_typing_state(client, msg.from_user_id, msg.context_token)
                except Exception:
                    logger.warning(
                        "could not send typing indicator to %s",
                        msg.from_user_id,
                        exc_info=True,
                    )
                try:
                    current_conversation = ensure_thread(msg.from_user_id)
                    sessions.update_summary(msg.from_user_id, text)

                    def send_partial(partial: str) -> None:
                        send_text_reply(
                            client, msg.from_user_id, partial, msg.context_token
                        )
                        logger.info(
                            "sent partial reply to %s: %r", msg.from_user_id, partial
                        )

                    agent_loop.run_stream(
                        agent.chat_stream(current_conversation, agent_input), send_partial
                    )
                    save_current_thread(msg.from_user_id, current_conversation)
                except Exception as exc:
                    reply = f"(codex error: {_public_error(exc)})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.exception("codex chat failed for %s", msg.from_user_id)

    monitor = Monitor(wechat_client, handle_message)
    stop_event = threading.Event()
    try:
        monitor.run(stop_event)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        monitor.close()


def _durable_main() -> None:
    """Run the restart-aware SQLite task runtime and WeChat gateway.

    The low-level Monitor remains blocking and thread-based, so all runtime
    state is created and operated on ``AsyncLoopThread.loop``.  The monitor
    callback only waits for durable ingress acceptance; Codex turns and user
    outbox delivery continue as loop-owned background tasks.
    """
    args = set(sys.argv[1:])
    if "--logout" in args:
        _logout_with_ownership()
        return
    if "--login" in args:
        _login_with_ownership()
        print("login complete")
        return

    database = _durable_database()
    ownership: SupervisorOwnership | None = None
    try:
        # This is the one global lock order: credential mutation, then the
        # database/account pair.  Credential discovery and QR authentication
        # happen before Client construction; new credentials are not published
        # until their exact account and runtime database are both exclusively
        # owned.
        with CredentialMutationOwnership(accounts_dir()):
            credentials, should_save = _load_or_authenticate_credentials()
            ownership = SupervisorOwnership(
                database,
                channel="wechat",
                bot_id=credentials.ilink_bot_id,
            ).acquire()
            if should_save:
                save_credentials(credentials)
                logger.info("login successful, credentials saved")
    except BaseException:
        if ownership is not None:
            ownership.close()
        raise

    assert ownership is not None
    with ownership:
        wechat_client = Client(credentials)
        try:
            _run_owned_durable(
                wechat_client,
                database=database,
                ownership=ownership,
            )
        except SupervisorResourcesStillLive:
            # The live loop may still be using both SQLite and the HTTP client.
            # Let SupervisorOwnership retain both locks until process exit.
            raise
        except BaseException:
            _close_owned_client(wechat_client)
            raise
        else:
            _close_owned_client(wechat_client)


def _durable_database() -> Path:
    """Resolve the canonical runtime database without opening SQLite."""

    return Path(
        os.environ.get(
            "CODEX_WECHAT_DB",
            str(Path.home() / ".codex-wechat-bot" / "runtime.sqlite3"),
        )
    ).expanduser().resolve()


def _positive_environment_integer(name: str, default: int) -> int:
    """Read one security/capacity bound and reject unsafe configuration."""

    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _agent_supervisor_worker_count() -> int:
    """Return enough dispatch coroutines for independently hosted Agents.

    ``CODEX_WECHAT_WORKERS`` described the removed shared-runtime topology.
    Honouring a stale value of ``1`` would accidentally serialize otherwise
    independent Agent processes, so it is now ignored with a migration hint.
    The durable per-Agent admission slot remains the authority that prevents
    two invocations from running concurrently inside one Agent process.
    """

    if "CODEX_WECHAT_WORKERS" in os.environ:
        logger.warning(
            "CODEX_WECHAT_WORKERS is deprecated and ignored; use "
            "CODEX_WECHAT_MAX_AGENT_PROCESSES"
        )
    return _positive_environment_integer(
        "CODEX_WECHAT_MAX_AGENT_PROCESSES",
        _DEFAULT_MAX_AGENT_PROCESSES,
    )


def _nonnegative_environment_number(name: str, default: float) -> float:
    """Read one finite duration while preserving an explicit zero value."""

    raw = os.environ.get(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a non-negative finite number") from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{name} must be a non-negative finite number")
    return value


def _close_owned_client(wechat_client: Client | Any) -> None:
    """Close the HTTP boundary or retain ownership when it cannot be fenced."""

    try:
        wechat_client.close()
    except BaseException as exc:
        raise SupervisorResourcesStillLive(
            "WeChat client shutdown failed; supervisor ownership retained"
        ) from exc


def _durable_workspace() -> Path:
    """Resolve the one workspace shared by Codex tasks and `/sh`."""

    configured = os.environ.get("CODEX_WECHAT_WORKSPACE", "").strip()
    workspace = (
        Path(configured).expanduser().resolve()
        if configured
        else Path(default_workspace())
    )
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def _build_process_agent_runtime(
    *,
    workspace_path: Path,
    turn_timeout: float | None,
    managed_root: Path,
    skill_roots: tuple[Path, ...],
    image_output_publisher: Any,
    agent_socket: Path,
    agent_bridge_capability_issuer: Any,
    max_processes: int,
) -> ProcessAgentRuntime:
    """Build the supervisor proxy without constructing an SDK runtime here."""

    return ProcessAgentRuntime(
        agent_id="codex",
        max_processes=max_processes,
        cwd=str(workspace_path),
        turn_timeout=turn_timeout,
        managed_root=managed_root,
        trusted_skill_roots=skill_roots,
        image_output_publisher=image_output_publisher,
        agent_bridge_command=(
            sys.executable,
            "-m",
            "src.agent_cli",
            "--socket",
            str(agent_socket),
        ),
        agent_bridge_capability_issuer=agent_bridge_capability_issuer,
    )


async def _stop_runtime_boundaries(
    *,
    agent_bridge: AgentBridgeServer | Any | None,
    manager: TaskManager | Any | None,
    store: SQLiteStore | Any | None,
) -> None:
    """Attempt every runtime shutdown boundary before reporting one error."""

    errors: list[BaseException] = []
    # Stop task/mailbox workers and their Agent children while the bridge is
    # still accepting turn-scoped collaboration calls.  Only after every
    # child is drained/reaped may the supervisor close that Unix-socket
    # boundary.  SQLite is last and both manager/store close are idempotent.
    for label, resource in (
        ("task manager", manager),
        ("Agent bridge", agent_bridge),
        ("SQLite store", store),
    ):
        if resource is None:
            continue
        try:
            if label == "SQLite store":
                await resource.close()
            else:
                await resource.stop()
        except BaseException as exc:
            errors.append(exc)
            logger.debug("failed to stop %s", label, exc_info=True)
    if errors:
        raise errors[0]


def _raise_unproven_runtime_shutdown(
    context: str,
    errors: list[BaseException],
) -> None:
    """Fail closed after best-effort cleanup if any boundary remains unknown."""

    if not errors:
        return
    raise SupervisorResourcesStillLive(
        f"{context}; supervisor ownership retained"
    ) from errors[0]


def _run_durable(wechat_client: Client) -> None:
    """Acquire exclusive database/account ownership and run the bridge."""

    database = _durable_database()
    ownership = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id=wechat_client.bot_id,
    )
    with ownership:
        try:
            _run_owned_durable(
                wechat_client,
                database=database,
                ownership=ownership,
            )
        except SupervisorResourcesStillLive:
            raise
        except BaseException:
            _close_owned_client(wechat_client)
            raise
        else:
            _close_owned_client(wechat_client)


def _run_owned_durable(
    wechat_client: Client,
    *,
    database: Path,
    ownership: SupervisorOwnership,
) -> None:
    """Run only after both supervisor ownership locks are held."""

    if not ownership.held:
        raise RuntimeError("durable runtime requires supervisor ownership")
    agent_socket = Path(
        os.environ.get(
            "CODEX_WECHAT_AGENT_SOCKET",
            str(database.with_name(f"{database.name}.agent.sock")),
        )
    ).expanduser().resolve()
    workspace_path = _durable_workspace()
    managed_root = Path(
        os.environ.get(
            "CODEX_WECHAT_ATTACHMENTS",
            str(Path.home() / ".codex-wechat-bot" / "attachments"),
        )
    ).expanduser().resolve()
    skill_roots = tuple(
        Path(value).expanduser().resolve()
        for value in os.environ.get("CODEX_WECHAT_SKILL_ROOTS", "").split(os.pathsep)
        if value.strip()
    )
    allowed_codex_config_profiles = tuple(
        value.strip()
        for value in os.environ.get(
            "CODEX_WECHAT_ALLOWED_CONFIG_PROFILES",
            "qwen",
        )
        .replace(",", os.pathsep)
        .split(os.pathsep)
        if value.strip()
    )
    turn_timeout_text = os.environ.get("CODEX_WECHAT_TURN_TIMEOUT", "")
    try:
        turn_timeout = float(turn_timeout_text) if turn_timeout_text else None
    except ValueError:
        turn_timeout = None
    worker_count = _agent_supervisor_worker_count()
    max_agent_queue = _positive_environment_integer(
        "CODEX_WECHAT_MAX_AGENT_QUEUE", DEFAULT_MAX_AGENT_QUEUE
    )
    max_global_queue = _positive_environment_integer(
        "CODEX_WECHAT_MAX_GLOBAL_QUEUE", DEFAULT_MAX_GLOBAL_AGENT_QUEUE
    )
    mailbox_ttl_seconds = _nonnegative_environment_number(
        "CODEX_WECHAT_MAILBOX_TTL", DEFAULT_MAILBOX_TTL_SECONDS
    )
    reply_aggregation_max_age_seconds = _nonnegative_environment_number(
        "CODEX_WECHAT_REPLY_AGGREGATION_MAX_AGE",
        DEFAULT_REPLY_AGGREGATION_MAX_AGE_SECONDS,
    )
    if reply_aggregation_max_age_seconds <= 0:
        raise RuntimeError(
            "CODEX_WECHAT_REPLY_AGGREGATION_MAX_AGE must be positive"
        )
    if max_agent_queue > max_global_queue:
        raise RuntimeError(
            "CODEX_WECHAT_MAX_AGENT_QUEUE cannot exceed "
            "CODEX_WECHAT_MAX_GLOBAL_QUEUE"
        )

    # Validate/create local configuration before starting the loop.  If this
    # fails (for example, an unwritable attachment directory), there is no
    # background thread to leak.
    managed_root.mkdir(parents=True, exist_ok=True)

    agent_loop = AsyncLoopThread()
    loop_started = False
    store: SQLiteStore | None = None
    manager: TaskManager | None = None
    delivery_worker: WeChatDeliveryWorker | None = None
    media_worker: WeChatMediaDeliveryWorker | None = None
    mailbox_supervisor: AgentMailboxSupervisor | None = None
    agent_bridge: AgentBridgeServer | None = None
    delivery_future: Any | None = None
    media_future: Any | None = None
    mailbox_future: Any | None = None
    monitor: Monitor | None = None

    async def setup() -> None:
        nonlocal store, manager, delivery_worker, media_worker
        nonlocal mailbox_supervisor, agent_bridge
        # Keep SQLite attachment metadata and the managed filesystem under
        # the same canonical root.  Without this, ``register_attachment``
        # cannot enforce the configured path boundary after a restart.
        store = SQLiteStore(
            database,
            attachment_root=managed_root,
            max_agent_queue=max_agent_queue,
            max_global_queue=max_global_queue,
            mailbox_ttl_seconds=mailbox_ttl_seconds,
            reply_aggregation_max_age_seconds=(
                reply_aggregation_max_age_seconds
            ),
        )
        try:
            # Migrate without touching abandoned work.  The next transaction
            # advances the durable ownership epoch first and only then performs
            # the one strong process-boundary recovery.
            await store.initialize(recover_startup_state=False)
            epoch = await store.activate_supervisor_epoch(
                owner_instance_id=ownership.owner_instance_id,
                channel=ownership.channel,
                bot_id=ownership.bot_id,
            )
            logger.info(
                "activated supervisor epoch %s for %s/%s",
                epoch.epoch,
                ownership.channel,
                ownership.bot_id,
            )
            # SQLite owns attachment references across process restarts.  The
            # async cleanup boundary must consult that durable source before
            # removing a file; a process-local AttachmentStore ref map is only
            # a fast path and cannot protect rows restored after a crash.
            attachment_store = AttachmentStore(
                managed_root,
                reference_checker=store.attachment_referenced,
            )
            # Restore SQLite's immutable checksums before any restarted task or
            # media row can read bytes. Metadata-only compatibility rows are
            # fenced by the ordinary claim/reconcile checks.
            for attachment in await store.list_attachments(states="ready"):
                try:
                    attachment_store.remember(attachment)
                except Exception:
                    logger.warning(
                        "could not hydrate managed attachment %s",
                        attachment.attachment_id,
                        exc_info=True,
                    )
            bridge_capabilities = AgentBridgeCapabilityAuthority()
            # The supervisor owns routing, SQLite, delivery, and process
            # lifecycle only.  The proxy starts a fresh child interpreter and
            # constructs the private CodexRuntime there; ``for_agent`` gives
            # every dynamically created Agent its own persistent process.
            runtime = _build_process_agent_runtime(
                workspace_path=workspace_path,
                turn_timeout=turn_timeout,
                managed_root=managed_root,
                image_output_publisher=ManagedImageOutputPublisher(
                    attachment_store,
                    store,
                    workspace_root=workspace_path,
                ),
                skill_roots=skill_roots,
                agent_socket=agent_socket,
                agent_bridge_capability_issuer=bridge_capabilities.issue,
                max_processes=worker_count,
            )
            registry = AgentRegistry()
            # Keep the immutable v1 chat profile available for queued tasks
            # created before the trusted execute deployment. New ingress is
            # attached to collaboration-capable v3 below; retaining v1/v2 lets
            # restart recovery resolve older task snapshots without rewriting
            # history.
            registry.register_profile(
                codex_profile(profile_version=1, default_mode_id="chat")
            )
            registry.register_profile(
                codex_profile(profile_version=2, default_mode_id="execute")
            )
            registry.register(
                "codex",
                runtime,
                profile=codex_profile(
                    profile_version=_DURABLE_CODEX_PROFILE_VERSION,
                    default_mode_id="execute",
                    allow_dynamic_peers=True,
                ),
            )
            manager = TaskManager(
                store,
                registry,
                worker_count=worker_count,
                default_agent_id="codex",
                default_mode_id="execute",
                trusted_default_execute=True,
                # `/agent <name>` can create a named Codex context on demand;
                # the manager persists/restores its immutable profile while
                # the static `codex` runtime remains the transport template.
                allow_dynamic_agents=True,
                allowed_codex_config_profiles=allowed_codex_config_profiles,
                require_process_isolation=True,
                workspace_root=workspace_path,
            )
            agent_bridge = AgentBridgeServer(
                manager,
                agent_socket,
                capability_authority=bridge_capabilities,
            )
            # Listen before task workers can resume queued work. A task may use
            # the bridge as soon as its Codex turn starts.
            await agent_bridge.start()
            await manager.start()
            delivery_worker = WeChatDeliveryWorker(store, wechat_client)
            media_worker = WeChatMediaDeliveryWorker(
                store,
                wechat_client,
                attachment_store=attachment_store,
                # The worker invokes senders as ``(row, upload, client)`` so
                # the channel helper can remain directly callable in tests.
                sender=lambda row, upload, client: send_media_delivery(
                    client, row, upload
                ),
            )

            async def reply_mailbox(
                item: Any, result: Any, *, claim_token: str | None = None
            ) -> None:
                reply_to_id = getattr(item, "reply_to_id", None)
                if reply_to_id is None and isinstance(item, dict):
                    reply_to_id = item.get("reply_to_id")
                if reply_to_id:
                    return
                content = _mailbox_result_content(result)
                if not content:
                    return
                mailbox_id = getattr(item, "mailbox_id", None)
                if mailbox_id is None and isinstance(item, dict):
                    mailbox_id = item.get("mailbox_id")
                source_agent_id = getattr(item, "destination_agent_id", None)
                if source_agent_id is None and isinstance(item, dict):
                    source_agent_id = item.get("destination_agent_id")
                await manager.reply_agent_message(
                    str(mailbox_id or ""),
                    content,
                    source_agent_id=str(source_agent_id or ""),
                    original_mailbox_id=str(mailbox_id or ""),
                    original_claim_token=claim_token,
                )

            mailbox_supervisor = AgentMailboxSupervisor(
                store,
                manager.registry,
                reply_handler=reply_mailbox,
            )
        except BaseException:
            # ``TaskManager.start`` rolls back workers, but a failure during
            # store initialization or registry startup can happen before its
            # normal started flag is set.  Close both lifecycle boundaries
            # here; their stop/close methods are idempotent.
            try:
                await _stop_runtime_boundaries(
                    agent_bridge=agent_bridge,
                    manager=manager,
                    store=store,
                )
            except BaseException:
                logger.debug(
                    "failed to clean up runtime after startup error",
                    exc_info=True,
                )
            raise

    try:
        agent_loop.start()
        loop_started = True

        # Keep construction separate from Monitor.run: an exception in a
        # gateway/worker constructor needs runtime cleanup, while an ordinary
        # monitor exception should still take the normal drain path below.
        agent_loop.run_coro(setup(), timeout=60)
        assert store is not None
        assert manager is not None
        assert media_worker is not None
        gateway = WeChatGateway(
            manager,
            bot_id=wechat_client.bot_id,
            attachment_store=media_worker.attachment_store,
            shell_cwd=workspace_path,
        )
        # SQLite owns the durable channel checkpoint. Restore it before the
        # first long-poll request; the JSON monitor buffer remains a legacy
        # compatibility fallback, not a second source of truth.
        initial_cursor = agent_loop.run_coro(
            store.get_cursor(channel="wechat", bot_id=wechat_client.bot_id),
            timeout=15,
        )
        monitor = Monitor(
            wechat_client,
            # Inbound media promotion may spend up to 60 seconds in the CDN
            # downloader before SQLite can accept the message.  Keep the
            # blocking monitor bridge above that bound so it does not cancel
            # acceptance while the downloader thread is still running.
            gateway.monitor_handler(agent_loop.loop, timeout=75),
            durable_acceptance=True,
            initial_cursor=initial_cursor,
            durable_cursor_callback=lambda cursor: agent_loop.run_coro(
                store.save_cursor(
                    channel="wechat",
                    bot_id=wechat_client.bot_id,
                    cursor=cursor,
                ),
                timeout=15,
            ),
            durable_cursor_reset_callback=lambda: agent_loop.run_coro(
                store.clear_cursor(
                    channel="wechat",
                    bot_id=wechat_client.bot_id,
                ),
                timeout=15,
            ),
        )
        delivery_future = asyncio.run_coroutine_threadsafe(
            delivery_worker.run(), agent_loop.loop
        )
        media_future = asyncio.run_coroutine_threadsafe(
            media_worker.run(), agent_loop.loop
        )
        mailbox_future = asyncio.run_coroutine_threadsafe(
            mailbox_supervisor.run(), agent_loop.loop
        )
    except BaseException:
        # ``setup`` handles failures inside manager.start.  This path covers
        # loop, gateway, monitor, and delivery-task startup.
        cleanup_errors: list[BaseException] = []
        for worker in (delivery_worker, media_worker):
            if worker is not None:
                try:
                    worker.stop()
                except BaseException as exc:
                    cleanup_errors.append(exc)
                    logger.debug(
                        "failed to stop delivery worker during startup rollback",
                        exc_info=True,
                    )
        if mailbox_supervisor is not None:
            try:
                mailbox_supervisor.stop()
            except BaseException as exc:
                cleanup_errors.append(exc)
                logger.debug(
                    "failed to stop mailbox worker during startup rollback",
                    exc_info=True,
                )
        if monitor is not None:
            try:
                monitor.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
                logger.debug(
                    "failed to close monitor during startup rollback",
                    exc_info=True,
                )
        auxiliary_futures = [
            future
            for future in (delivery_future, media_future, mailbox_future)
            if future is not None
        ]
        for future in auxiliary_futures:
            try:
                future.cancel()
            except BaseException as exc:
                cleanup_errors.append(exc)
                logger.debug(
                    "failed to cancel auxiliary worker during startup rollback",
                    exc_info=True,
                )
        if auxiliary_futures and loop_started:
            async def drain_auxiliary() -> None:
                for future in auxiliary_futures:
                    try:
                        await asyncio.wait_for(asyncio.wrap_future(future), timeout=10.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        if not future.done():
                            future.cancel()
                    except Exception:
                        logger.debug("auxiliary worker exited during startup rollback", exc_info=True)
            try:
                agent_loop.run_coro(drain_auxiliary(), timeout=15)
            except BaseException as exc:
                cleanup_errors.append(exc)
                logger.debug("failed to drain auxiliary workers during startup rollback", exc_info=True)
        if loop_started:
            try:
                agent_loop.run_coro(
                    _stop_runtime_boundaries(
                        agent_bridge=agent_bridge,
                        manager=manager,
                        store=store,
                    ),
                    timeout=20,
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
                logger.debug(
                    "failed to stop runtime boundaries after startup error",
                    exc_info=True,
                )
        if loop_started:
            try:
                agent_loop.stop()
                loop_started = False
            except BaseException as exc:
                cleanup_errors.append(exc)
        _raise_unproven_runtime_shutdown(
            "runtime startup rollback could not prove cleanup",
            cleanup_errors,
        )
        raise

    stop_event = threading.Event()
    try:
        try:
            monitor.run(stop_event)
        except KeyboardInterrupt:
            pass
    finally:
        shutdown_errors: list[BaseException] = []
        stop_event.set()
        assert monitor is not None
        try:
            monitor.close()
        except BaseException as exc:
            shutdown_errors.append(exc)
            logger.exception("durable monitor shutdown failed")

        async def shutdown() -> None:
            assert manager is not None
            assert delivery_worker is not None
            assert media_worker is not None
            assert delivery_future is not None
            errors: list[BaseException] = []
            for label, worker in (
                ("delivery worker", delivery_worker),
                ("media worker", media_worker),
            ):
                try:
                    worker.stop()
                except BaseException as exc:
                    errors.append(exc)
                    logger.debug("failed to stop %s", label, exc_info=True)
            assert mailbox_supervisor is not None
            try:
                mailbox_supervisor.stop()
            except BaseException as exc:
                errors.append(exc)
                logger.debug("failed to stop mailbox worker", exc_info=True)
            # Drain every channel/Agent worker before TaskManager closes
            # SQLite.  A worker may already have claimed a row; closing the
            # store first would strand its lease transition and lose the
            # durable failure/sent state.
            auxiliary_futures = [
                future
                for future in (delivery_future, media_future, mailbox_future)
                if future is not None
            ]
            for future in auxiliary_futures:
                try:
                    await asyncio.wait_for(asyncio.wrap_future(future), timeout=10.0)
                except asyncio.TimeoutError:
                    future.cancel()
                except asyncio.CancelledError:
                    pass
                except BaseException:
                    logger.debug("auxiliary worker exited with an error", exc_info=True)
            try:
                await _stop_runtime_boundaries(
                    agent_bridge=agent_bridge,
                    manager=manager,
                    store=store,
                )
            except BaseException as exc:
                errors.append(exc)
            if errors:
                raise errors[0]

        try:
            agent_loop.run_coro(shutdown(), timeout=20)
        except BaseException as exc:
            shutdown_errors.append(exc)
            logger.exception("durable runtime shutdown failed")
        # ``shutdown`` normally closes through TaskManager.  A timeout or
        # partial worker failure must still drain SQLite before the enclosing
        # SupervisorOwnership context releases either kernel lock.
        if store is not None and loop_started:
            try:
                agent_loop.run_coro(store.close(), timeout=20)
            except BaseException as exc:
                shutdown_errors.append(exc)
                logger.exception("durable store shutdown fence failed")
        if loop_started:
            try:
                agent_loop.stop()
            except BaseException as exc:
                shutdown_errors.append(exc)
        _raise_unproven_runtime_shutdown(
            "runtime shutdown could not prove cleanup",
            shutdown_errors,
        )


def main() -> None:
    """Start the durable runtime on every public launch path."""

    if "--legacy" in set(sys.argv[1:]):
        # Keep old launch scripts working without exposing a second command
        # router that lacks durable tasks, dynamic Agents, and current command
        # semantics. The flag is now only a deprecated durable-runtime alias.
        logger.warning("--legacy is deprecated; starting the durable runtime")
    _durable_main()


if __name__ == "__main__":
    log_path = configure_persistent_logging()
    logger.info("persistent runtime log: %s", log_path)
    main()
