"""Bridge: real WeChat account <-> Codex ACP agent.

Routes incoming WeChat text messages to a persistent `codex` subprocess
(via acp_agents.create_agent("codex")) and sends the agent's reply back to
the WeChat user. Each WeChat user gets their own codex thread
(conversation_id = from_user_id), so conversational context is preserved
across messages from the same person.

wechat_ilink's Monitor dispatches messages from worker threads (sync code),
while acp_agents is async (asyncio subprocess + JSON-RPC). To bridge the two,
a single background thread runs a persistent asyncio event loop hosting the
codex agent; message handlers submit coroutines to it via
`asyncio.run_coroutine_threadsafe` and block for the result.

Usage:
    python examples/codex_wechat_bot.py
    python examples/codex_wechat_bot.py --login   # log in and save credentials
    python examples/codex_wechat_bot.py --logout  # forget saved WeChat login
"""

from __future__ import annotations

import asyncio
import difflib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import uuid
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import qrcode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from acp_agents import create_agent  # noqa: E402
from wechat_ilink import (  # noqa: E402
    Client,
    Monitor,
    delete_all_credentials,
    fetch_qrcode,
    format_message_summary,
    load_all_credentials,
    poll_qr_status,
    save_credentials,
    send_text_reply,
)
from wechat_ilink.types import (  # noqa: E402
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("codex_wechat_bot")

KNOWN_COMMANDS = [
    "/help",
    "/clear",
    "/reset",
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

HELP_TEXT = (
    "commands:\n"
    "\n/help - show this message\n"
    "\n/clear - clear conversation context\n"
    "\n/reset - clear conversation context\n"
    "\n/model - show current model\n"
    "\n/model <name> - switch model\n"
    "\n/models - list available models\n"
    "\n/skills - list available skills\n"
    "\n/session [id] - switch to or create a session\n"
    "\n/sessions - list your sessions\n"
    "\n/delsession <id> - delete a session\n"
    "\n/sh <command> - execute a shell command on the bot host\n"
    "\n"
    "skills:\n\n send $skill-name <prompt> to invoke a skill"
)

_SKILL_NAME_RE = re.compile(r"^\$([\w:-]+)")
_MAX_SHELL_OUTPUT = 6000
_SHELL_TIMEOUT = 30
_CODEX_TASK_TIMEOUT = 15 * 60


def run_shell_command(
    command: str,
    *,
    cwd: Optional[Path] = None,
    timeout: int = _SHELL_TIMEOUT,
    max_output: int = _MAX_SHELL_OUTPUT,
) -> str:
    """Execute a shell command and format a bounded result for WeChat."""
    completed = subprocess.run(
        command,
        shell=True,
        executable="/bin/sh",
        cwd=str(cwd or Path(__file__).resolve().parent.parent),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    output = completed.stdout
    if completed.stderr:
        output += f"\nstderr:\n{completed.stderr}"
    output = output.rstrip() or "(no output)"
    if len(output) > max_output:
        output = output[:max_output] + "\n... (output truncated)"
    return f"exit code: {completed.returncode}\n{output}"


def _is_codex_thread_id(value: str) -> bool:
    """Return whether a value is a UUID accepted by Codex thread/resume."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return True


@dataclass
class Session:
    session_id: str
    summary: str = "未开始对话"
    thread_id: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""


class SessionManager:
    """Persist named sessions and their Codex thread IDs per user."""

    def __init__(self, path: Optional[Path] = None) -> None:
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

    def create(self, user_id: str, session_id: Optional[str] = None) -> Session:
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
        self, user_id: str, session_id: Optional[str]
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

    def thread_id(self, user_id: str) -> Optional[str]:
        return self.current(user_id).thread_id

    def find_by_id(self, user_id: str, session_id: str) -> Optional[Session]:
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


class AsyncLoopThread:
    """Runs a persistent asyncio event loop on a background thread.

    The codex agent's subprocess connection (asyncio.Queue/Future objects)
    must stay bound to a single event loop; this lets synchronous callers
    (wechat_ilink's Monitor worker threads) submit coroutines to it safely.
    """

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def start(self) -> None:
        self._thread.start()

    def run_coro(self, coro, timeout: Optional[float] = None) -> Any:
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)


def main() -> None:
    args = set(sys.argv[1:])

    if "--logout" in args:
        logout()
        return

    if "--login" in args:
        login()
        print("login complete")
        return

    wechat_client = login()

    agent_loop = AsyncLoopThread()
    agent_loop.start()

    agent = create_agent("codex")
    agent_loop.run_coro(agent.start(), timeout=30)
    logger.info("codex agent ready: %s", agent.info())
    sessions = SessionManager()

    def conversation_id(user_id: str) -> str:
        return sessions.conversation_id(user_id)

    def ensure_thread(user_id: str) -> str:
        """Resume the persisted Codex thread for the active bot session."""
        current_conversation = conversation_id(user_id)
        saved_thread_id = sessions.thread_id(user_id)
        if (
            saved_thread_id
            and agent.get_thread_id(current_conversation) != saved_thread_id
        ):
            try:
                agent_loop.run_coro(
                    agent.resume_thread(current_conversation, saved_thread_id),
                    timeout=30,
                )
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
                            agent_loop.run_coro(
                                agent.resume_thread(
                                    conversation_id(msg.from_user_id), session.thread_id
                                ),
                                timeout=30,
                            )
                        except Exception as exc:
                            reply = f"cannot resume session {session.session_id}: {exc}"
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
                            reply = f"shell command timed out after {_SHELL_TIMEOUT} seconds"
                        except OSError as exc:
                            reply = f"shell command failed to start: {exc}"
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
                                reply = f"(codex error: {exc})"
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
                        reply = f"(codex error: {exc})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info("cleared session for %s", msg.from_user_id)
                    continue

                if lower == "/model" or lower.startswith("/model "):
                    arg = text.strip()[len("/model") :].strip()
                    if not arg:
                        current = (
                            agent.get_model(conversation_id(msg.from_user_id))
                            or "(default)"
                        )
                        reply = (
                            f"current model: {current}\nusage: /model <name> to switch"
                        )
                    else:
                        agent.set_model(conversation_id(msg.from_user_id), arg)
                        reply = (
                            f"model set to: {arg} (takes effect on your next message)"
                        )
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info(
                        "model command for %s: %r", msg.from_user_id, arg or "<show>"
                    )
                    continue

                if lower in ("/models", "/listmodel", "/listmodels"):
                    try:
                        models = agent_loop.run_coro(agent.list_models(), timeout=30)
                        current = agent.get_model(conversation_id(msg.from_user_id))
                        lines = ["available models:"]
                        for m in models:
                            marker = " (current)" if m["id"] == current else ""
                            marker += " [default]" if m.get("isDefault") else ""
                            lines.append(
                                f"- {m['id']}: {m.get('displayName', '')}{marker}"
                            )
                        lines.append("use /model <id> to switch")
                        reply = "\n".join(lines)
                    except Exception as exc:
                        reply = f"(codex error: {exc})"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    logger.info("listed models for %s", msg.from_user_id)
                    continue

                if lower in ("/skills", "/listskill", "/listskills"):
                    try:
                        skills = agent_loop.run_coro(agent.list_skills(), timeout=30)
                        lines = ["available skills (use $skill-name <prompt>):"]
                        for s in skills:
                            display = (s.get("interface") or {}).get(
                                "displayName"
                            ) or s["name"]
                            lines.append(f"- ${s['name']}: {display}")
                        reply = "\n".join(lines)
                    except Exception as exc:
                        reply = f"(codex error: {exc})"
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

                if stripped.startswith("$"):
                    name_match = _SKILL_NAME_RE.match(stripped)
                    if name_match:
                        skill_name = name_match.group(1)
                        try:
                            skills = agent_loop.run_coro(
                                agent.list_skills(), timeout=30
                            )
                        except Exception as exc:
                            skills = []
                            logger.warning("could not fetch skills list: %s", exc)
                        skill_names = [s["name"] for s in skills]
                        if not any(
                            n.lower() == skill_name.lower() for n in skill_names
                        ):
                            suggestion = difflib.get_close_matches(
                                skill_name, skill_names, n=1, cutoff=0.4
                            )
                            reply = (
                                f"unknown skill: ${skill_name}. "
                                f"did you mean ${suggestion[0]}?"
                                if suggestion
                                else (
                                    f"unknown skill: ${skill_name}. "
                                    "use /skills to see available skills"
                                )
                            )
                            send_text_reply(
                                client, msg.from_user_id, reply, msg.context_token
                            )
                            logger.info(
                                "unknown skill from %s: %r",
                                msg.from_user_id,
                                skill_name,
                            )
                            continue

                try:
                    current_conversation = ensure_thread(msg.from_user_id)
                    sessions.update_summary(msg.from_user_id, text)
                    reply = agent_loop.run_coro(
                        agent.chat(current_conversation, text),
                        timeout=_CODEX_TASK_TIMEOUT,
                    )
                    save_current_thread(msg.from_user_id, current_conversation)
                except FutureTimeoutError:
                    reply = (
                        "Codex is still working after 15 minutes. "
                        "Please wait, then send a follow-up message in this session."
                    )
                except Exception as exc:
                    reply = f"(codex error: {exc})"
                send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                logger.info("sent reply to %s: %r", msg.from_user_id, reply)

    monitor = Monitor(wechat_client, handle_message)
    stop_event = threading.Event()
    try:
        monitor.run(stop_event)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        try:
            agent_loop.run_coro(agent.stop(), timeout=10)
        except Exception:
            pass
        agent_loop.stop()


if __name__ == "__main__":
    main()
