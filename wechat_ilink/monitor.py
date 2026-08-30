"""Blocking update monitor for WeChat iLink used by codex-wechat-bot."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import asyncio
import inspect
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .auth import _legacy_account_stem, accounts_dir, normalize_account_id
from .client import Client
from .types import ITEM_TYPE_TEXT, WeixinMessage

logger = logging.getLogger("wechat_ilink.monitor")

MAX_CONSECUTIVE_FAILURES = 5
INITIAL_BACKOFF = 3.0  # seconds
MAX_BACKOFF = 60.0  # seconds
SESSION_EXPIRED_BACKOFF = 5.0  # seconds
ERR_CODE_SESSION_EXPIRED = -14

MessageHandler = Callable[[Client, WeixinMessage], Any]


class Monitor:
    """Manages the long-poll loop for receiving messages."""

    def __init__(
        self,
        client: Client,
        handler: MessageHandler,
        max_workers: int = 8,
        *,
        durable_acceptance: bool = False,
        durable_cursor_callback: Callable[[str], Any] | None = None,
        durable_cursor_reset_callback: Callable[[], Any] | None = None,
        initial_cursor: str | None = None,
    ):
        self.client = client
        self.handler = handler
        # The original monitor advanced the long-poll cursor as soon as a
        # response arrived.  That is useful for a synchronous, best-effort
        # bot, but can acknowledge a message before a durable task store has
        # accepted it.  Runtime gateways opt in to waiting for handlers before
        # committing the cursor; legacy callers retain the old behavior.
        self.durable_acceptance = durable_acceptance
        self.durable_cursor_callback = durable_cursor_callback
        # Session expiry invalidates the server-side cursor.  Durable callers
        # must clear their authoritative checkpoint before this monitor
        # starts polling from the beginning again; otherwise a restart would
        # restore the stale SQLite value even though the local JSON buffer is
        # empty.
        self.durable_cursor_reset_callback = durable_cursor_reset_callback
        self._failures = 0
        self._buf_path, self._legacy_buf_path = self._sync_buffer_paths(
            str(client.bot_id or "")
        )
        self._get_updates_buf = ""
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._handler_locks: dict[str, threading.Lock] = {}
        self._handler_locks_guard = threading.Lock()
        self._load_buf()
        # SQLite is authoritative for the durable runtime cursor.  The
        # optional value is applied after the filesystem compatibility cursor
        # is loaded so a restart cannot resume from an older JSON checkpoint.
        # ``None`` means no durable checkpoint was available.  An explicit
        # empty string is a durable reset and must override any legacy JSON
        # buffer loaded above.
        if initial_cursor is not None:
            self._get_updates_buf = str(initial_cursor)

    def close(self, *, wait: bool = True) -> None:
        """Release handler threads after the polling loop has stopped."""

        self._executor.shutdown(wait=wait, cancel_futures=not wait)

    @staticmethod
    def _sync_buffer_paths(bot_id: str) -> tuple[Any, Any | None]:
        """Return collision-resistant sync-buffer and optional legacy paths.

        Historically ``normalize_account_id`` replaced punctuation with ``-``.
        That made distinct bot IDs such as ``bot:a`` and ``bot-a`` share one
        cursor file.  Keep the old stem for already-safe IDs, and append a
        stable digest whenever normalization (or path sanitization) changes
        the raw identity.  A legacy path is retained for one-way migration;
        durable SQLite dedupe/cursor ownership still scopes the actual bot.
        """

        raw = str(bot_id or "")
        stem = normalize_account_id(raw)
        legacy_stem = _legacy_account_stem(raw)
        root = accounts_dir()
        new_path = root / f"{stem}.sync.json"
        legacy_path = root / f"{legacy_stem}.sync.json"
        # Do not treat an identical path as a migration source.
        return new_path, legacy_path if legacy_path != new_path else None

    def _load_buf(self) -> None:
        paths = [self._buf_path]
        # If a newly-published file is truncated/corrupt, a valid legacy
        # checkpoint is still a safer starting point than silently resetting
        # to the beginning of the channel stream.
        if self._legacy_buf_path is not None:
            paths.append(self._legacy_buf_path)
        for path in paths:
            try:
                data = json.loads(path.read_text())
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                continue
            if not isinstance(data, Mapping):
                continue
            buf = data.get("get_updates_buf", "")
            if buf:
                self._get_updates_buf = str(buf)
                logger.info("loaded sync buf from %s", path)
            return

    def _save_buf(self) -> None:
        self._buf_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        payload = json.dumps(
            {"get_updates_buf": self._get_updates_buf},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self._buf_path.name}.",
            dir=self._buf_path.parent,
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self._buf_path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

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

            # Session expired -> reset the sync buffer and reconnect silently.
            # In durable mode, clear the authoritative cursor first.  The
            # callback is deliberately synchronous from this worker thread so
            # a restart cannot observe a stale checkpoint after the local
            # buffer has been reset.
            if resp.errcode == ERR_CODE_SESSION_EXPIRED:
                # This has a dedicated recovery path and delay; do not carry a
                # preceding transient transport failure into the next healthy
                # poll after the cursor reset.
                self._failures = 0
                reset_ok = True
                reset_callback = self.durable_cursor_reset_callback
                if reset_callback is not None:
                    try:
                        reset_result = reset_callback()
                        if inspect.isawaitable(reset_result):
                            reset_result = asyncio.run(reset_result)
                        if reset_result is False:
                            raise RuntimeError("durable cursor reset callback rejected reset")
                    except BaseException:
                        reset_ok = False
                        logger.exception(
                            "durable cursor reset failed; retaining sync cursor"
                        )
                if reset_ok and (self._get_updates_buf or reset_callback is not None):
                    logger.info("session expired, resetting sync buf")
                    self._get_updates_buf = ""
                    self._save_buf()
                elif reset_ok:
                    logger.warning(
                        "WeChat session expired and cannot be auto-recovered. "
                        "Re-login required."
                    )
                stop_event.wait(SESSION_EXPIRED_BACKOFF)
                continue

            # Other server errors.
            if resp.ret != 0 or resp.errcode != 0:
                # HTTP success does not make an iLink protocol error healthy.
                # Without a delay, a persistent account/server error becomes a
                # tight request loop and can overload both the service and the
                # local process.
                self._failures += 1
                backoff = self._calc_backoff()
                logger.warning(
                    "server error (%d/%d, backoff=%.1fs): "
                    "ret=%d errcode=%d errmsg=%s",
                    self._failures,
                    MAX_CONSECUTIVE_FAILURES,
                    backoff,
                    resp.ret,
                    resp.errcode,
                    resp.errmsg,
                )
                if self._failures == MAX_CONSECUTIVE_FAILURES:
                    logger.warning(
                        "%d consecutive failures. If this persists, re-authenticate "
                        "(fetch_qrcode / poll_qr_status again).",
                        MAX_CONSECUTIVE_FAILURES,
                    )
                stop_event.wait(backoff)
                continue

            if self.durable_acceptance:
                # A durable gateway returns once its inbound row and task (or
                # command acknowledgement) are committed.  Wait for those
                # short operations before advancing the cursor.  Agent turns
                # themselves continue in the background and therefore do not
                # hold up polling.  Preserve wire order for one contact: the
                # executor may run independent contacts concurrently, but
                # submitting one future per message would let the scheduler
                # acquire the per-contact lock out of order.
                grouped: dict[str, list[WeixinMessage]] = {}
                for ordinal, msg in enumerate(resp.msgs):
                    # ``from_user_id`` is the channel identity for this
                    # monitor's single bot.  Keep malformed empty identities
                    # distinct so one bad message cannot serialize all other
                    # malformed records together.
                    contact = str(msg.from_user_id or f"<unknown:{ordinal}>")
                    grouped.setdefault(contact, []).append(msg)

                def handle_group(messages: list[WeixinMessage]) -> bool:
                    for message in messages:
                        try:
                            if not self._safe_handle(message):
                                # The cursor will replay this contact group.
                                # Do not let a later mutation overtake the
                                # first message whose durable outcome failed.
                                return False
                        except BaseException:
                            logger.exception(
                                "durable message acceptance failed in contact group"
                            )
                            return False
                    return True

                futures = [
                    self._executor.submit(handle_group, messages)
                    for messages in grouped.values()
                ]
                accepted = True
                # Consume every future even after one failure.  This keeps
                # worker exceptions observed and lets already-submitted
                # messages finish their durable dedupe attempt before the
                # cursor is retried.
                for future in futures:
                    try:
                        if not future.result():
                            accepted = False
                    except BaseException:
                        logger.exception("durable message acceptance future failed")
                        accepted = False
                if not accepted:
                    self._failures += 1
                    backoff = self._calc_backoff()
                    logger.warning(
                        "message acceptance failed (attempt %d, backoff=%.1fs); "
                        "retaining sync cursor for retry",
                        self._failures,
                        backoff,
                    )
                    # Retaining the cursor is required for at-least-once
                    # ingress, but a deterministic policy/store rejection can
                    # otherwise replay the same poison message as fast as the
                    # service answers. Apply the same bounded backoff used for
                    # transport errors without acknowledging the message.
                    stop_event.wait(backoff)
                    continue
                if resp.get_updates_buf and self.durable_cursor_callback is not None:
                    try:
                        cursor_result = self.durable_cursor_callback(resp.get_updates_buf)
                        # A callback may be an async manager/store bridge.  The
                        # Monitor itself is synchronous, so wait for that
                        # short persistence operation before acknowledging the
                        # wire cursor.
                        if hasattr(cursor_result, "__await__"):
                            import asyncio

                            cursor_result = asyncio.run(cursor_result)
                        if cursor_result is False:
                            raise RuntimeError("durable cursor callback rejected cursor")
                    except BaseException:
                        self._failures += 1
                        backoff = self._calc_backoff()
                        logger.exception(
                            "durable cursor persistence failed (attempt %d, "
                            "backoff=%.1fs); retaining sync cursor",
                            self._failures,
                            backoff,
                        )
                        stop_event.wait(backoff)
                        continue

            # Reset failure history only after transport, protocol, durable
            # acceptance, and (when configured) cursor persistence succeed.
            self._failures = 0

            # Update buf for next poll only after optional durable acceptance.
            if resp.get_updates_buf:
                self._get_updates_buf = resp.get_updates_buf
                self._save_buf()

            if not self.durable_acceptance:
                # Process messages concurrently — don't block the poll loop.
                for msg in resp.msgs:
                    self._executor.submit(self._safe_handle, msg)

        logger.info("shutting down")

    def _safe_handle(self, msg: WeixinMessage) -> bool:
        def invoke_handler() -> Any:
            result = self.handler(self.client, msg)
            if inspect.isawaitable(result):
                # Monitor callbacks execute on a worker thread.  Supporting an
                # async handler here prevents a coroutine from being treated
                # as an accepted result (and therefore prevents a cursor from
                # being acknowledged before durable ingress completes).
                return asyncio.run(result)
            return result

        def accepted_result(result: Any) -> bool:
            if result is False:
                return False
            if result is None:
                # ``None`` is the channel adapter's explicit "unsupported or
                # intentionally ignored" result (for example, a media item in
                # the text-only MVP).  In durable mode the handler has still
                # completed synchronously, so retaining the cursor forever
                # would redeliver an input the application cannot process.
                # Exceptions and explicit ``False`` continue to retain it.
                return self.durable_acceptance
            # Gateway acceptance objects expose ``accepted`` and ``duplicate``
            # separately.  A duplicate is already durably owned and therefore
            # safe to acknowledge even when ``accepted`` is false by contract.
            if isinstance(result, Mapping):
                accepted = result.get("accepted")
                duplicate = result.get("duplicate", result.get("is_duplicate", False))
            else:
                accepted = getattr(result, "accepted", None)
                duplicate = getattr(result, "duplicate", False)
            if accepted is False and not bool(duplicate):
                return False
            return True

        # Keep ordinary and mutating command messages ordered per contact.
        # Short immediate control operations remain responsive independently.
        if _is_immediate_command_message(msg):
            try:
                result = invoke_handler()
                return accepted_result(result)
            except BaseException:
                logger.exception("message handler raised an exception")
            return False

        with self._handler_locks_guard:
            lock = self._handler_locks.setdefault(msg.from_user_id, threading.Lock())
        with lock:
            try:
                result = invoke_handler()
                return accepted_result(result)
            except BaseException:
                logger.exception("message handler raised an exception")
                return False


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
    for item in msg.item_list:
        if item.type != ITEM_TYPE_TEXT or item.text_item is None:
            continue
        if item.text_item.text.strip():
            return item.text_item.text.lstrip().startswith("/")
    return False


def _is_immediate_command_message(msg: WeixinMessage) -> bool:
    """Return whether a command may bypass the per-contact ordering lock.

    Status/list/cancel/report are deliberately short control operations and
    may run while a Codex turn is active. Agent/mode/notify switches, retry, and
    legacy session/model controls share the contact lock with ordinary
    messages so their snapshots are ordered.
    Unknown commands are locked as well; this keeps a later prompt from
    overtaking a control-plane acknowledgement in the same update batch.
    """

    immediate = {
        "/status",
        "/tasks",
        "/cancel",
        "/report",
        "/agents",
        "/models",
        "/modes",
        "/skills",
        "/listskill",
        "/listskills",
    }
    for item in msg.item_list:
        if item.type != ITEM_TYPE_TEXT or item.text_item is None:
            continue
        if not item.text_item.text.strip():
            continue
        token = item.text_item.text.strip().split(maxsplit=1)
        return bool(token and token[0].lower() in immediate)
    return False
