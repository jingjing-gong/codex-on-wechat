"""Inbound WeChat typing-state acknowledgement regressions."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import pytest

from src.channels.wechat import WeChatGateway
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink.types import (
    ITEM_TYPE_IMAGE,
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_STATE_NEW,
    MESSAGE_TYPE_BOT,
    MESSAGE_TYPE_USER,
    TYPING_STATUS_TYPING,
    ImageItem,
    MediaInfo,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def _text_message(
    text: str,
    *,
    message_id: int = 41,
    message_type: int = MESSAGE_TYPE_USER,
    message_state: int = MESSAGE_STATE_FINISH,
) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=message_type,
        message_state=message_state,
        context_token="live-context",
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


def _media_message(*, message_id: int = 42) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token="media-context",
        item_list=[
            MessageItem(
                type=ITEM_TYPE_IMAGE,
                image_item=ImageItem(
                    media=MediaInfo(
                        encrypt_query_param="encrypted-reference",
                        aes_key="opaque-key",
                    ),
                    mid_size=12,
                ),
            )
        ],
    )


class _RecordingRuntime:
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events
        self.calls = 0

    async def accept_inbound(self, envelope: Any, **_kwargs: Any) -> Any:
        self.calls += 1
        self.events.append(("accept", envelope.external_message_id))
        return {"accepted": True}


class _EmptyRouter:
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events

    async def handle_command(self, command: Any, _envelope: Any) -> str:
        self.events.append(("command", command.name))
        return ""


class _TypingClient:
    bot_id = "bot"

    def __init__(
        self,
        events: list[tuple[Any, ...]],
        *,
        fail_at: str = "",
    ) -> None:
        self.events = events
        self.fail_at = fail_at

    def get_config(self, user_id: str, context_token: str = "") -> Any:
        self.events.append(("config", user_id, context_token))
        if self.fail_at == "config":
            raise RuntimeError("config unavailable")
        return SimpleNamespace(
            ret=0,
            errcode=0,
            errmsg="",
            typing_ticket="typing-ticket",
        )

    def send_typing(self, user_id: str, ticket: str, status: int) -> None:
        self.events.append(("typing", user_id, ticket, status))
        if self.fail_at == "typing":
            raise RuntimeError("typing unavailable")

    def send_message(self, _request: Any) -> Any:  # pragma: no cover - guard
        raise AssertionError("typing acknowledgement must not send a message")


@pytest.mark.parametrize(
    ("message", "expected_tail"),
    [
        (_text_message("hello"), ("accept", "41")),
        (_text_message("/status", message_id=43), ("command", "status")),
        (_media_message(), ("accept", "42")),
    ],
)
def test_monitor_types_before_supported_text_command_and_media_processing(
    message: WeixinMessage,
    expected_tail: tuple[Any, ...],
) -> None:
    async def scenario() -> None:
        events: list[tuple[Any, ...]] = []
        runtime = _RecordingRuntime(events)
        gateway = WeChatGateway(
            runtime,
            bot_id="bot",
            command_router=_EmptyRouter(events),
        )
        handler = gateway.monitor_handler(asyncio.get_running_loop())

        outcome = await asyncio.to_thread(
            handler,
            _TypingClient(events),
            message,
        )

        assert outcome is not None and outcome.accepted
        assert events[:2] == [
            ("config", "user", message.context_token),
            ("typing", "user", "typing-ticket", TYPING_STATUS_TYPING),
        ]
        assert expected_tail in events[2:]
        assert events.index(expected_tail) >= 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "message",
    [
        _text_message("unfinished", message_state=MESSAGE_STATE_NEW),
        _text_message("bot output", message_type=MESSAGE_TYPE_BOT),
        _text_message("   ", message_id=44),
    ],
)
def test_unsupported_wire_messages_do_not_emit_typing(
    message: WeixinMessage,
) -> None:
    async def scenario() -> None:
        events: list[tuple[Any, ...]] = []
        runtime = _RecordingRuntime(events)
        gateway = WeChatGateway(runtime, bot_id="bot")
        handler = gateway.monitor_handler(asyncio.get_running_loop())

        outcome = await asyncio.to_thread(
            handler,
            _TypingClient(events),
            message,
        )

        assert outcome is None
        assert events == []
        assert runtime.calls == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("fail_at", ["config", "typing"])
def test_typing_failures_do_not_reject_durable_acceptance(
    fail_at: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        events: list[tuple[Any, ...]] = []
        runtime = _RecordingRuntime(events)
        gateway = WeChatGateway(runtime, bot_id="bot")
        handler = gateway.monitor_handler(asyncio.get_running_loop())

        outcome = await asyncio.to_thread(
            handler,
            _TypingClient(events, fail_at=fail_at),
            _text_message("accepted despite typing failure"),
        )

        assert outcome is not None and outcome.accepted
        assert runtime.calls == 1
        assert any(event[0] == "accept" for event in events)

    asyncio.run(scenario())
    assert "could not send typing indicator to user" in caplog.text


def test_duplicate_delivery_refreshes_typing_without_spending_reply_quota(
    tmp_path: Any,
) -> None:
    async def scenario() -> None:
        events: list[tuple[Any, ...]] = []
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        gateway = WeChatGateway(store, bot_id="bot")
        handler = gateway.monitor_handler(asyncio.get_running_loop())
        message = _text_message("one durable task", message_id=45)
        client = _TypingClient(events)
        try:
            first = await asyncio.to_thread(handler, client, message)
            replay = await asyncio.to_thread(handler, client, message)

            assert first is not None and first.accepted and not first.duplicate
            assert replay is not None and replay.duplicate
            assert [event[0] for event in events].count("typing") == 2
            assert len(await store.list_tasks(limit=10)) == 1
            assert await store.list_outbox(limit=10) == []
            scope = await store.get_reply_scope_for_inbound(
                first.raw_result.inbound.message_id
            )
            assert scope is not None
            assert scope.used_slots == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_monitor_bridge_normalizes_system_exit_as_durable_failure(caplog) -> None:
    class ExitingRuntime:
        async def accept_inbound(self, _envelope: Any, **_kwargs: Any) -> Any:
            raise SystemExit(3)

    async def scenario() -> None:
        gateway = WeChatGateway(ExitingRuntime(), bot_id="bot")
        handler = gateway.monitor_handler(asyncio.get_running_loop())

        with caplog.at_level(logging.ERROR, logger="src.channels.wechat"):
            outcome = await asyncio.to_thread(
                handler,
                _TypingClient([]),
                _text_message("fail durably"),
            )

        assert outcome is False

    asyncio.run(scenario())
    assert "durable monitor callback failed" in caplog.text
    assert "SystemExit: 3" in caplog.text
