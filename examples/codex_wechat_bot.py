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
import logging
import re
import sys
import threading
import uuid
from dataclasses import dataclass
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
    "\n"
    "skills:\n\n send $skill-name <prompt> to invoke a skill"
)

_SKILL_NAME_RE = re.compile(r"^\$([\w:-]+)")


@dataclass
class Session:
    session_id: str
    summary: str = "未开始对话"


class SessionManager:
    """Track named sessions and their Codex conversation keys per user."""

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Session]] = {}
        self._active: dict[str, str] = {}

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
            return sessions[session_id]
        session = Session(session_id=session_id)
        sessions[session_id] = session
        self._active[user_id] = session_id
        return session

    def switch_or_create(
        self, user_id: str, session_id: Optional[str]
    ) -> tuple[Session, bool]:
        sessions = self._user_sessions(user_id)
        requested_id = (session_id or "").strip()
        if requested_id and requested_id in sessions:
            self._active[user_id] = requested_id
            return sessions[requested_id], False
        return self.create(user_id, requested_id), True

    def list(self, user_id: str) -> list[Session]:
        return list(self._user_sessions(user_id).values())

    def delete(self, user_id: str, session_id: str) -> bool:
        sessions = self._user_sessions(user_id)
        if session_id not in sessions:
            return False
        del sessions[session_id]
        if self._active.get(user_id) == session_id:
            self._active.pop(user_id, None)
        return True

    def conversation_id(self, user_id: str) -> str:
        return f"{user_id}:session:{self.current(user_id).session_id}"

    def update_summary(self, user_id: str, message: str) -> None:
        session = self.current(user_id)
        if session.summary != "未开始对话":
            return
        summary = " ".join(message.split())
        session.summary = summary[:80] + ("..." if len(summary) > 80 else "")


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
                    session_list = sessions.list(msg.from_user_id)
                    active = sessions.current(msg.from_user_id).session_id
                    lines = ["sessions:"]
                    for session in session_list:
                        marker = " (current)" if session.session_id == active else ""
                        lines.append(
                            f"- {session.session_id}{marker}: {session.summary}"
                        )
                    send_text_reply(
                        client, msg.from_user_id, "\n".join(lines), msg.context_token
                    )
                    continue

                if lower.startswith("/delsession"):
                    session_id = stripped[len("/delsession") :].strip()
                    if not session_id:
                        reply = "usage: /delsession <session-id>"
                    elif sessions.delete(msg.from_user_id, session_id):
                        active = sessions.current(msg.from_user_id)
                        reply = (
                            f"deleted session: {session_id}\n"
                            f"current session: {active.session_id}"
                        )
                    else:
                        reply = f"session not found: {session_id}"
                    send_text_reply(client, msg.from_user_id, reply, msg.context_token)
                    continue

                if lower in ("/clear", "/reset"):
                    try:
                        agent_loop.run_coro(
                            agent.reset_session(
                                sessions.conversation_id(msg.from_user_id)
                            ),
                            timeout=30,
                        )
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
                            agent.get_model(sessions.conversation_id(msg.from_user_id))
                            or "(default)"
                        )
                        reply = (
                            f"current model: {current}\nusage: /model <name> to switch"
                        )
                    else:
                        agent.set_model(sessions.conversation_id(msg.from_user_id), arg)
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
                        current = agent.get_model(
                            sessions.conversation_id(msg.from_user_id)
                        )
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
                    sessions.update_summary(msg.from_user_id, text)
                    reply = agent_loop.run_coro(
                        agent.chat(sessions.conversation_id(msg.from_user_id), text),
                        timeout=120,
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
