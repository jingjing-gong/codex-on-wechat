"""Blocking update monitor for WeChat iLink used by codex-wechat-bot."""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from .auth import accounts_dir, normalize_account_id
from .client import Client
from .types import ITEM_TYPE_TEXT, WeixinMessage

logger = logging.getLogger("wechat_ilink.monitor")

MAX_CONSECUTIVE_FAILURES = 5
INITIAL_BACKOFF = 3.0  # seconds
MAX_BACKOFF = 60.0  # seconds
SESSION_EXPIRED_BACKOFF = 5.0  # seconds
ERR_CODE_SESSION_EXPIRED = -14

MessageHandler = Callable[[Client, WeixinMessage], None]


class Monitor:
    """Manages the long-poll loop for receiving messages."""

    def __init__(self, client: Client, handler: MessageHandler, max_workers: int = 8):
        self.client = client
        self.handler = handler
        self._failures = 0
        self._buf_path = (
            accounts_dir() / f"{normalize_account_id(client.bot_id)}.sync.json"
        )
        self._get_updates_buf = ""
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._handler_locks: dict[str, threading.Lock] = {}
        self._handler_locks_guard = threading.Lock()
        self._load_buf()

    def _load_buf(self) -> None:
        try:
            data = json.loads(self._buf_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return
        buf = data.get("get_updates_buf", "")
        if buf:
            self._get_updates_buf = buf
            logger.info("loaded sync buf from %s", self._buf_path)

    def _save_buf(self) -> None:
        self._buf_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._buf_path.write_text(
            json.dumps({"get_updates_buf": self._get_updates_buf})
        )

    def _calc_backoff(self) -> float:
        """Return an exponential backoff duration capped at MAX_BACKOFF."""
        delay = INITIAL_BACKOFF
        for _ in range(1, self._failures):
            delay *= 2
            if delay > MAX_BACKOFF:
                return MAX_BACKOFF
        return delay

    def run(self, stop_event: threading.Event) -> None:
        """Run the long-poll loop until `stop_event` is set. Blocks the caller."""
        logger.info("starting long-poll loop")
        while not stop_event.is_set():
            try:
                resp = self.client.get_updates(self._get_updates_buf)
            except Exception as exc:  # network/timeout errors are expected
                self._failures += 1
                backoff = self._calc_backoff()
                logger.warning(
                    "GetUpdates error (%d/%d, backoff=%.1fs): %s",
                    self._failures,
                    MAX_CONSECUTIVE_FAILURES,
                    backoff,
                    exc,
                )
                if self._failures == MAX_CONSECUTIVE_FAILURES:
                    logger.warning(
                        "%d consecutive failures. If this persists, re-authenticate "
                        "(fetch_qrcode / poll_qr_status again).",
                        MAX_CONSECUTIVE_FAILURES,
                    )
                stop_event.wait(backoff)
                continue

            # Reset failure counter on any successful response.
            self._failures = 0

            # Session expired -> reset sync buf and reconnect silently.
            if resp.errcode == ERR_CODE_SESSION_EXPIRED:
                if self._get_updates_buf:
                    logger.info("session expired, resetting sync buf")
                    self._get_updates_buf = ""
                    self._save_buf()
                else:
                    logger.warning(
                        "WeChat session expired and cannot be auto-recovered. "
                        "Re-login required."
                    )
                stop_event.wait(SESSION_EXPIRED_BACKOFF)
                continue

            # Other server errors.
            if resp.ret != 0 and resp.errcode != 0:
                logger.warning(
                    "server error: ret=%d errcode=%d errmsg=%s",
                    resp.ret,
                    resp.errcode,
                    resp.errmsg,
                )
                continue

            # Update buf for next poll.
            if resp.get_updates_buf:
                self._get_updates_buf = resp.get_updates_buf
                self._save_buf()

            # Process messages concurrently — don't block the poll loop.
            for msg in resp.msgs:
                self._executor.submit(self._safe_handle, msg)

        logger.info("shutting down")

    def _safe_handle(self, msg: WeixinMessage) -> None:
        # Keep ordinary messages ordered per contact. Commands are independent
        # control operations and must remain responsive during a long Codex turn.
        if _is_command_message(msg):
            try:
                self.handler(self.client, msg)
            except Exception:
                logger.exception("message handler raised an exception")
            return

        with self._handler_locks_guard:
            lock = self._handler_locks.setdefault(msg.from_user_id, threading.Lock())
        with lock:
            try:
                self.handler(self.client, msg)
            except Exception:
                logger.exception("message handler raised an exception")


def format_message_summary(msg: WeixinMessage) -> str:
    """Return a short description of a message, for logging."""
    text = ""
    for item in msg.item_list:
        if item.type == ITEM_TYPE_TEXT and item.text_item is not None:
            text = item.text_item.text
            break
    if len(text) > 50:
        text = text[:50] + "..."
    return (
        f"from={msg.from_user_id} type={msg.message_type} "
        f"state={msg.message_state} text={text!r}"
    )


def _is_command_message(msg: WeixinMessage) -> bool:
    """Return whether a message starts with a slash command."""
    return any(
        item.text_item is not None and item.text_item.text.lstrip().startswith("/")
        for item in msg.item_list
        if item.type == ITEM_TYPE_TEXT
    )
