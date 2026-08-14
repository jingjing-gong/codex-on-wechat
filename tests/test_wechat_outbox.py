"""Focused regressions for durable WeChat command responses."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from src.channels.models import DeliveryReceipt, ReplyTarget, UserDelivery
from src.channels.wechat import (
    COMMAND_INTERRUPTED_RESPONSE,
    CommandResponse,
    WeChatDeliveryWorker,
    WeChatGateway,
    command_client_id,
    send_media_delivery,
    send_user_delivery,
    stable_contextless_client_id,
)
from src.runtime.models import InboundMessage
from src.runtime.sqlite_store import SQLiteStore, StoreError
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def _command_message(
    *,
    context_token: str = "rolling-token",
    text: str = "/help",
    message_id: int = 17,
    seq: int = 1,
) -> WeixinMessage:
    return WeixinMessage(
        seq=seq,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token=context_token,
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


class _AcceptedRuntime:
    async def accept_inbound(self, _envelope: Any, **_kwargs: Any) -> Any:
        return {"accepted": True}


def test_outbox_replay_ignores_refreshed_context_token(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            original = UserDelivery(
                delivery_id="command:one",
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    source_message_id="message",
                    context_token="first-token",
                ),
                content="response",
                client_id="client-one",
            )
            first = await store.create_user_outbox(original)
            replay = await store.create_user_outbox(
                UserDelivery(
                    delivery_id=original.delivery_id,
                    target=ReplyTarget(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        source_message_id="message",
                        context_token="refreshed-token",
                    ),
                    content=original.content,
                    client_id=original.client_id,
                )
            )

            assert replay.outbox_id == first.outbox_id
            assert replay.reply_target.context_token == "first-token"

            with pytest.raises(StoreError):
                await store.create_user_outbox(
                    UserDelivery(
                        delivery_id=original.delivery_id,
                        target=ReplyTarget(
                            channel="wechat",
                            bot_id="bot",
                            external_user_id="other-user",
                            source_message_id="message",
                            context_token="refreshed-token",
                        ),
                        content=original.content,
                        client_id=original.client_id,
                    )
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_canonical_text_send_persists_transition_before_contextless_attempt():
    requests = []
    responses = iter(
        [
            SimpleNamespace(ret=-2, errcode=0, errmsg="prepare failed"),
            SimpleNamespace(ret=0, errcode=0, errmsg=""),
        ]
    )

    def send(request: Any) -> Any:
        requests.append(request.model_copy(deep=True))
        return next(responses)

    client = SimpleNamespace(bot_id="bot-a", send_message=send)
    delivery = UserDelivery(
        delivery_id="outbox-one",
        target=ReplyTarget(
            channel="wechat",
            bot_id="bot-a",
            external_user_id="user",
            context_token="rolling-token",
        ),
        from_user_id="bot-a",
        content="reply",
        client_id="primary-id",
    )

    primary = send_user_delivery(client, delivery)

    assert primary.sent is False
    assert primary.wire_variant == "primary"
    assert primary.transition_to_wire_variant == "contextless"
    assert len(requests) == 1
    assert requests[0].msg.client_id == "primary-id"

    alternate_id = stable_contextless_client_id(delivery)
    contextless = send_user_delivery(
        client,
        replace(
            delivery,
            active_wire_variant="contextless",
            contextless_client_id=alternate_id,
        ),
    )

    assert contextless.sent is True
    assert contextless.wire_variant == "contextless"
    assert contextless.client_id == alternate_id
    assert len(requests) == 2
    assert requests[1].msg.from_user_id == "bot-a"
    assert requests[1].msg.context_token == ""
    assert requests[1].msg.client_id == alternate_id
    assert requests[1].msg.message_state == MESSAGE_STATE_FINISH


def test_canonical_delivery_rejects_wrong_selected_client_without_sending():
    requests = []
    client = SimpleNamespace(
        bot_id="bot-b",
        send_message=lambda request: requests.append(request),
    )
    delivery = UserDelivery(
        delivery_id="outbox-wrong-client",
        target=ReplyTarget(
            channel="wechat", bot_id="bot-a", external_user_id="user"
        ),
        from_user_id="bot-a",
        content="reply",
        client_id="primary-id",
    )

    receipt = send_user_delivery(client, delivery)

    assert receipt.sent is False
    assert receipt.retryable is False
    assert "does not match" in receipt.error
    assert requests == []


def test_media_send_uses_persisted_bot_stable_id_and_finish_state():
    requests = []
    client = SimpleNamespace(
        bot_id="bot-a",
        send_message=lambda request: (
            requests.append(request), SimpleNamespace(ret=0, errcode=0)
        )[1],
    )
    record = {
        "media_id": "media-one",
        "bot_id": "bot-a",
        "external_user_id": "user",
        "remote_id": "cdn-query",
        "encryption_key": "opaque-key",
        "kind": "image",
        "size": 12,
        "client_id": "media-primary",
        "context_token": "rolling-token",
    }

    assert send_media_delivery(client, record) is True
    assert len(requests) == 1
    assert requests[0].msg.from_user_id == "bot-a"
    assert requests[0].msg.client_id == "media-primary"
    assert requests[0].msg.message_state == MESSAGE_STATE_FINISH


def test_duplicate_command_uses_original_persisted_reply_target(tmp_path):
    class Router:
        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            return "stable response"

    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first_store = SQLiteStore(path)
        await first_store.initialize()
        try:
            gateway = WeChatGateway(
                first_store, bot_id="bot", command_router=Router()
            )
            first = await gateway.accept(_command_message(context_token="first"))
            assert first is not None
            assert first.envelope.context_token == "first"
        finally:
            await first_store.close()

        replay_store = SQLiteStore(path)
        await replay_store.initialize()
        try:
            replay_gateway = WeChatGateway(
                replay_store, bot_id="bot", command_router=Router()
            )
            replay = await replay_gateway.accept(
                _command_message(context_token="refreshed")
            )
            assert replay is not None
            assert replay.duplicate
            assert replay.envelope.context_token == "first"
            assert replay.envelope.source_sequence == 1
        finally:
            await replay_store.close()

    asyncio.run(scenario())


def test_gateway_recv_drains_one_fifo_batch_and_replays_without_advancing(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            origin = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="origin-message",
                    session_id="default",
                    text="create overflow",
                    context_token="origin-context",
                ),
                create_task=False,
            )
            for ordinal in range(1, 25):
                await store.project_reply_candidate(
                    target=origin.inbound.target(),
                    source_key=f"origin-item:{ordinal}",
                    content=f"reply {ordinal}",
                    source_item_id=f"origin-item-{ordinal}",
                    source_item_ordinal=ordinal,
                )

            sends: list[Any] = []
            client = SimpleNamespace(
                bot_id="bot",
                send_message=lambda request: sends.append(request),
            )
            gateway = WeChatGateway(store, bot_id="bot")
            first_message = _command_message(
                text="/recv", message_id=101, seq=101, context_token="recv-one"
            )
            first = await gateway.handle_message(client, first_message)

            assert first is not None
            assert first.accepted
            assert not first.duplicate
            assert first.command_response == ""
            # `/recv` does not add or directly send an acknowledgement when
            # its allocator already projected continued reply records.
            assert sends == []
            rows = await store.list_outbox(limit=50)
            continued = sorted(
                (
                    item
                    for item in rows
                    if item.reply_target.source_message_id == "101"
                ),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert [item.reply_ordinal for item in continued] == list(range(1, 11))
            assert [
                item.content.removesuffix("\n\nReply /recv to continue.")
                for item in continued
            ] == [f"reply {ordinal}" for ordinal in range(11, 21)]
            assert {item.from_user_id for item in continued} == {"bot"}

            replay = await gateway.handle_message(client, first_message)
            assert replay is not None
            assert replay.duplicate
            replay_rows = await store.list_outbox(limit=50)
            assert {item.outbox_id for item in replay_rows} == {
                item.outbox_id for item in rows
            }

            second = await gateway.handle_message(
                client,
                _command_message(
                    text="/recv",
                    message_id=102,
                    seq=102,
                    context_token="recv-two",
                ),
            )
            assert second is not None
            assert second.command_response == ""
            final_rows = await store.list_outbox(limit=50)
            final_batch = sorted(
                (
                    item
                    for item in final_rows
                    if item.reply_target.source_message_id == "102"
                ),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert [item.content for item in final_batch] == [
                "reply 21",
                "reply 22",
                "reply 23",
                "reply 24",
            ]
            assert [item.reply_ordinal for item in final_batch] == [1, 2, 3, 4]
            assert sends == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_direct_command_sends_only_first_durable_text_fragment(tmp_path):
    class Router:
        def __init__(self, response: str) -> None:
            self.response = response
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            return self.response

    async def scenario() -> None:
        response = "".join(str(index % 10) for index in range(6_401))
        router = Router(response)
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        requests: list[Any] = []

        def send(request: Any) -> Any:
            requests.append(request.model_copy(deep=True))
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        client = SimpleNamespace(bot_id="bot", send_message=send)
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        try:
            outcome = await gateway.handle_message(
                client,
                _command_message(text="/long", message_id=120, seq=120),
            )
            assert outcome is not None
            assert outcome.command_response == response
            assert router.calls == 1

            rows = sorted(
                await store.list_outbox(limit=20),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert len(rows) == 3
            assert [item.reply_ordinal for item in rows] == [1, 2, 3]
            assert [item.state.value for item in rows] == [
                "sent",
                "pending",
                "pending",
            ]
            assert len(requests) == 1
            assert requests[0].msg.client_id == rows[0].client_id
            assert requests[0].msg.item_list[0].text_item.text == rows[0].content
            assert rows[0].content != response

            worker = WeChatDeliveryWorker(store, client)
            assert len(await worker.run_once()) == 1
            assert len(await worker.run_once()) == 1
            assert "".join(
                request.msg.item_list[0].text_item.text for request in requests
            ) == response
            final = sorted(
                await store.list_outbox(limit=20),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert all(item.state.value == "sent" for item in final)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_quota_full_command_is_durably_deferred_without_direct_send(tmp_path):
    class Router:
        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            return "deferred command response"

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        requests: list[Any] = []
        client = SimpleNamespace(
            bot_id="bot",
            send_message=lambda request: requests.append(request),
        )
        message = _command_message(
            text="/deferred", message_id=121, seq=121
        )
        try:
            gateway = WeChatGateway(
                store, bot_id="bot", command_router=Router()
            )
            accepted = await gateway.accept(message)
            assert accepted is not None
            for ordinal in range(1, 11):
                await store.project_reply_candidate(
                    target=accepted.envelope.reply_target(),
                    source_key=f"preexisting:{ordinal}",
                    content=f"preexisting reply {ordinal}",
                )

            outcome = await gateway.handle_message(client, message)
            assert outcome is not None
            assert outcome.duplicate
            assert outcome.command_response == "deferred command response"
            assert requests == []
            assert len(await store.list_outbox(limit=20)) == 10

            replay = await store.create_user_outbox(
                UserDelivery(
                    delivery_id=outcome.response_delivery_id,
                    target=outcome.envelope.reply_target(),
                    from_user_id="bot",
                    content=outcome.command_response,
                    client_id=command_client_id(outcome.envelope),
                ),
                foreground=True,
            )
            assert replay.outbox_items == ()
            assert replay.fragments
            assert {
                fragment.state.value for fragment in replay.fragments
            } == {"deferred_quota"}
        finally:
            await store.close()

    asyncio.run(scenario())


def test_direct_command_persists_contextless_variant_before_success(tmp_path):
    class Router:
        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            return "stable response"

    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        responses = iter(
            (
                SimpleNamespace(ret=-2, errcode=0, errmsg="prepare failed"),
                SimpleNamespace(ret=0, errcode=0, errmsg=""),
            )
        )
        requests: list[Any] = []

        def send(request: Any) -> Any:
            requests.append(request.model_copy(deep=True))
            return next(responses)

        client = SimpleNamespace(bot_id="bot", send_message=send)
        gateway = WeChatGateway(store, bot_id="bot", command_router=Router())
        try:
            outcome = await gateway.handle_message(
                client,
                _command_message(text="/stable", message_id=122, seq=122),
            )
            assert outcome is not None
            row = await store.get_outbox_item(outcome.response_delivery_id)
            assert row is not None
            assert row.state.value == "sent"
            assert row.active_wire_variant == "contextless"
            assert row.contextless_client_id
            assert [request.msg.client_id for request in requests] == [
                row.client_id,
                row.contextless_client_id,
            ]
            assert [request.msg.context_token for request in requests] == [
                "rolling-token",
                "",
            ]
            reply_slot_id = row.reply_slot_id
            alternate_id = row.contextless_client_id
        finally:
            await store.close()

        with sqlite3.connect(path) as connection:
            slot = connection.execute(
                "SELECT active_wire_variant, contextless_client_id "
                "FROM reply_slots WHERE reply_slot_id=?",
                (reply_slot_id,),
            ).fetchone()
        assert slot == ("contextless", alternate_id)

    asyncio.run(scenario())


def test_contextless_command_retry_survives_store_restart(tmp_path):
    class Router:
        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            return "restart-safe response"

    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        current = [datetime(2030, 1, 1, tzinfo=timezone.utc)]
        first = SQLiteStore(path, clock=lambda: current[0])
        await first.initialize()
        initial_requests: list[Any] = []
        responses = iter(
            (
                SimpleNamespace(ret=-2, errcode=0, errmsg="prepare failed"),
                SimpleNamespace(ret=1, errcode=0, errmsg="temporary failure"),
            )
        )

        def fail_alternate(request: Any) -> Any:
            initial_requests.append(request.model_copy(deep=True))
            return next(responses)

        gateway = WeChatGateway(first, bot_id="bot", command_router=Router())
        outcome = await gateway.handle_message(
            SimpleNamespace(bot_id="bot", send_message=fail_alternate),
            _command_message(text="/restart", message_id=123, seq=123),
        )
        assert outcome is not None
        failed = await first.get_outbox_item(outcome.response_delivery_id)
        assert failed is not None
        assert failed.state.value == "retry_wait"
        assert failed.active_wire_variant == "contextless"
        assert failed.contextless_client_id
        assert [request.msg.client_id for request in initial_requests] == [
            failed.client_id,
            failed.contextless_client_id,
        ]
        await first.close()

        current[0] += timedelta(seconds=6)
        restarted = SQLiteStore(path, clock=lambda: current[0])
        await restarted.initialize()
        retry_requests: list[Any] = []

        def succeed(request: Any) -> Any:
            retry_requests.append(request.model_copy(deep=True))
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        try:
            worker = WeChatDeliveryWorker(
                restarted,
                SimpleNamespace(bot_id="bot", send_message=succeed),
            )
            receipts = await worker.run_once()
            assert len(receipts) == 1
            assert receipts[0].sent
            assert receipts[0].wire_variant == "contextless"
            assert len(retry_requests) == 1
            assert retry_requests[0].msg.client_id == failed.contextless_client_id
            assert retry_requests[0].msg.context_token == ""
            final = await restarted.get_outbox_item(outcome.response_delivery_id)
            assert final is not None
            assert final.state.value == "sent"
            assert final.active_wire_variant == "contextless"
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_command_persistence_failure_does_not_send(monkeypatch):
    class Runtime(_AcceptedRuntime):
        async def enqueue_user_outbox(self, **_kwargs: Any) -> Any:
            raise OSError("database unavailable")

    sends = 0

    async def fake_send(_client: Any, _delivery: Any) -> DeliveryReceipt:
        nonlocal sends
        sends += 1
        return DeliveryReceipt(sent=True)

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        gateway = WeChatGateway(Runtime(), bot_id="bot")
        with pytest.raises(RuntimeError, match="not durably persisted"):
            await gateway.handle_message(SimpleNamespace(bot_id="bot"), _command_message())

    asyncio.run(scenario())
    assert sends == 0


def test_command_claim_failure_does_not_send(monkeypatch):
    class Runtime(_AcceptedRuntime):
        async def enqueue_user_outbox(self, *, delivery: Any, **_kwargs: Any) -> Any:
            return delivery

        async def mark_outbox_sending(self, _outbox_id: str, **_kwargs: Any) -> bool:
            raise OSError("claim unavailable")

    sends = 0

    async def fake_send(_client: Any, _delivery: Any) -> DeliveryReceipt:
        nonlocal sends
        sends += 1
        return DeliveryReceipt(sent=True)

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        gateway = WeChatGateway(Runtime(), bot_id="bot")
        with pytest.raises(RuntimeError, match="not claimed"):
            await gateway.handle_message(SimpleNamespace(bot_id="bot"), _command_message())

    asyncio.run(scenario())
    assert sends == 0


def test_command_without_outbox_api_keeps_legacy_direct_send(monkeypatch):
    sends = 0

    async def fake_send(_client: Any, delivery: UserDelivery) -> DeliveryReceipt:
        nonlocal sends
        sends += 1
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=True,
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        gateway = WeChatGateway(_AcceptedRuntime(), bot_id="bot")
        outcome = await gateway.handle_message(
            SimpleNamespace(bot_id="bot"), _command_message()
        )
        assert outcome is not None
        assert outcome.command_response

    asyncio.run(scenario())
    assert sends == 1


def test_direct_command_marks_nonretryable_delivery_permanent(monkeypatch):
    class Runtime(_AcceptedRuntime):
        def __init__(self) -> None:
            self.failed: dict[str, Any] | None = None

        async def enqueue_user_outbox(self, *, delivery: Any, **_kwargs: Any) -> Any:
            return delivery

        async def mark_outbox_sending(self, _outbox_id: str, **_kwargs: Any) -> bool:
            return True

        async def mark_outbox_failed(self, _outbox_id: str, **kwargs: Any) -> bool:
            self.failed = kwargs
            return True

    async def rejected(_client: Any, delivery: UserDelivery) -> DeliveryReceipt:
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=False,
            error="send message failed: ret=-2 errcode=0 errmsg=prepare failed",
            retryable=False,
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", rejected)

    async def scenario() -> None:
        runtime = Runtime()
        gateway = WeChatGateway(runtime, bot_id="bot")
        outcome = await gateway.handle_message(
            SimpleNamespace(bot_id="bot"), _command_message()
        )
        assert outcome is not None
        assert runtime.failed is not None
        assert runtime.failed["retry"] is False
        assert runtime.failed["permanent"] is True

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("attempts", "expected_delay"),
    [(1, 5.0), (2, 10.0), (4, 40.0), (15, 300.0), (12111, 300.0)],
)
def test_outbox_worker_uses_capped_attempt_backoff(
    monkeypatch, attempts, expected_delay
):
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.failed: dict[str, Any] | None = None

        async def claim_outbox(self, *_args: Any, **_kwargs: Any):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "outbox_id": "outbox-retry",
                    "claim_token": "claim-token",
                    "attempts": attempts,
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "content": "retry me",
                    "client_id": "stable-client-id",
                }
            ]

        async def mark_outbox_sending(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def mark_outbox_failed(self, _outbox_id: str, **kwargs: Any) -> bool:
            self.failed = kwargs
            return True

    async def transient(_client: Any, delivery: UserDelivery) -> DeliveryReceipt:
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=False,
            error="temporary failure",
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", transient)

    async def scenario() -> None:
        store = Store()
        worker = WeChatDeliveryWorker(store, SimpleNamespace(bot_id="bot"))
        await worker.run_once()
        assert store.failed is not None
        assert store.failed["retry"] is True
        assert store.failed["permanent"] is False
        assert store.failed["delay"] == expected_delay

    asyncio.run(scenario())


def test_outbox_worker_cancels_blocked_send_after_lease_loss(monkeypatch):
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.marked = False

        async def claim_outbox(self, *_args: Any, **_kwargs: Any):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "outbox_id": "outbox-lease-loss",
                    "claim_token": "claim-token",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "content": "must not be acknowledged",
                    "client_id": "stable-client-id",
                }
            ]

        async def mark_outbox_sending(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def renew_outbox_lease(self, *_args: Any, **_kwargs: Any) -> bool:
            return False

        async def mark_outbox_sent(self, *_args: Any, **_kwargs: Any) -> bool:
            self.marked = True
            raise AssertionError("stale outbox send was acknowledged")

        async def mark_outbox_failed(self, *_args: Any, **_kwargs: Any) -> bool:
            self.marked = True
            raise AssertionError("stale outbox send was failed")

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocked_send(_client: Any, _delivery: Any) -> DeliveryReceipt:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", blocked_send)

    async def scenario() -> None:
        store = Store()
        worker = WeChatDeliveryWorker(
            store,
            SimpleNamespace(bot_id="bot"),
            lease_seconds=0.15,
        )
        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(started.wait(), timeout=1)

        assert await asyncio.wait_for(running, timeout=1) == []
        assert cancelled.is_set()
        assert store.marked is False

    asyncio.run(scenario())


def test_outbox_worker_commits_contextless_variant_before_alternate_send():
    events: list[tuple[str, str]] = []

    class Store:
        def __init__(self) -> None:
            self.claimed = False

        async def claim_outbox(self, *_args: Any, **_kwargs: Any):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "outbox_id": "outbox-fallback",
                    "claim_token": "claim-token",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot-a",
                        "external_user_id": "user",
                        "session_id": "default",
                        "context_token": "rolling-token",
                    },
                    "bot_id": "bot-a",
                    "from_user_id": "bot-a",
                    "content": "reply",
                    "client_id": "primary-id",
                    "contextless_client_id": "alternate-id",
                    "active_wire_variant": "primary",
                }
            ]

        async def mark_outbox_sending(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def activate_outbox_contextless_variant(
            self, _outbox_id: str, **kwargs: Any
        ) -> bool:
            assert kwargs["claim_token"] == "claim-token"
            assert kwargs["contextless_client_id"] == "alternate-id"
            events.append(("store", "contextless"))
            return True

        async def mark_outbox_sent(
            self, _outbox_id: str, **kwargs: Any
        ) -> bool:
            assert kwargs["claim_token"] == "claim-token"
            assert kwargs["client_id"] == "alternate-id"
            events.append(("store", "sent"))
            return True

    def send(request: Any) -> Any:
        events.append(("wire", request.msg.client_id))
        if request.msg.client_id == "primary-id":
            return SimpleNamespace(ret=-2, errcode=0, errmsg="prepare failed")
        assert request.msg.client_id == "alternate-id"
        assert request.msg.context_token == ""
        return SimpleNamespace(ret=0, errcode=0, errmsg="")

    async def scenario() -> None:
        selected = SimpleNamespace(bot_id="bot-a", send_message=send)
        worker = WeChatDeliveryWorker(
            Store(),
            SimpleNamespace(bot_id="unused"),
            client_resolver={"bot-a": selected},
        )

        receipts = await worker.run_once()

        assert len(receipts) == 1
        assert receipts[0].sent is True
        assert receipts[0].wire_variant == "contextless"

    asyncio.run(scenario())
    assert events == [
        ("wire", "primary-id"),
        ("store", "contextless"),
        ("wire", "alternate-id"),
        ("store", "sent"),
    ]


def test_outbox_worker_renews_complete_batch_and_active_completion(monkeypatch):
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.mark_started = asyncio.Event()
            self.second_renewed = asyncio.Event()
            self.active_renewed_during_mark = asyncio.Event()
            self.marked: list[str] = []

        async def claim_outbox(self, *_args: Any, **_kwargs: Any):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "outbox_id": f"outbox-{index}",
                    "claim_token": f"claim-{index}",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "content": f"message {index}",
                    "client_id": f"client-{index}",
                }
                for index in (1, 2)
            ]

        async def mark_outbox_sending(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def renew_outbox_lease(
            self, outbox_id: str, *_args: Any, **_kwargs: Any
        ) -> bool:
            if outbox_id == "outbox-2":
                self.second_renewed.set()
            if outbox_id == "outbox-1" and self.mark_started.is_set():
                self.active_renewed_during_mark.set()
            return True

        async def mark_outbox_sent(
            self, outbox_id: str, *_args: Any, **_kwargs: Any
        ) -> bool:
            if outbox_id == "outbox-1":
                self.mark_started.set()
                await release_mark.wait()
            self.marked.append(outbox_id)
            return True

    release_send = asyncio.Event()
    release_mark = asyncio.Event()

    async def fake_send(_client: Any, delivery: UserDelivery) -> DeliveryReceipt:
        if delivery.delivery_id == "outbox-1":
            await release_send.wait()
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=True,
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        store = Store()
        worker = WeChatDeliveryWorker(
            store,
            SimpleNamespace(bot_id="bot"),
            claim_limit=2,
            lease_seconds=0.15,
        )
        running = asyncio.create_task(worker.run_once())
        try:
            # Row 2 is already owned while row 1 is blocked, so its heartbeat
            # must begin before the sequential loop reaches it.
            await asyncio.wait_for(store.second_renewed.wait(), timeout=1)
            release_send.set()
            await asyncio.wait_for(store.mark_started.wait(), timeout=1)
            # The current row also remains protected until its external result
            # is committed, rather than stopping immediately after the send.
            await asyncio.wait_for(
                store.active_renewed_during_mark.wait(), timeout=1
            )
        finally:
            release_send.set()
            release_mark.set()

        receipts = await asyncio.wait_for(running, timeout=1)
        assert [receipt.delivery_id for receipt in receipts] == [
            "outbox-1",
            "outbox-2",
        ]
        assert store.marked == ["outbox-1", "outbox-2"]

    asyncio.run(scenario())


def test_command_receipt_replays_completed_response_without_rerunning_router(tmp_path):
    class Router:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> Any:
            self.calls += 1
            return CommandResponse("stable response", ("notification-1",))

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        first_router = Router()
        try:
            gateway = WeChatGateway(
                store, bot_id="bot", command_router=first_router
            )
            first = await gateway.accept(_command_message())
            assert first is not None
            assert first.command_response == "stable response"
            assert first.presentation_ids == ("notification-1",)
            assert first_router.calls == 1
            await store.close()

            replay_router = Router()
            replay_gateway = WeChatGateway(
                SQLiteStore(tmp_path / "runtime.sqlite"),
                bot_id="bot",
                command_router=replay_router,
            )
            replay = await replay_gateway.accept(_command_message())
            assert replay is not None
            assert replay.command_response == "stable response"
            assert replay.presentation_ids == ("notification-1",)
            assert replay_router.calls == 0
            await replay_gateway.runtime.close()
        finally:
            # ``store`` may already be closed after the first half; close is
            # idempotent and keeps the test robust if setup fails earlier.
            await store.close()

    asyncio.run(scenario())


def test_duplicate_long_command_reuses_fragments_and_full_receipt(
    tmp_path, monkeypatch
):
    response = "command response\n" + ("x" * 6_500)

    class Router:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            return response

    sent: list[UserDelivery] = []

    async def fake_send(
        _client: Any, delivery: UserDelivery
    ) -> DeliveryReceipt:
        sent.append(delivery)
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=True,
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        try:
            first = await gateway.handle_message(
                SimpleNamespace(bot_id="bot"), _command_message()
            )
            assert first is not None
            assert first.command_response == response
            assert router.calls == 1
            assert len(sent) == 1
            # The immediate command path owns only the canonical first
            # fragment. Remaining fragments stay durable for the worker.
            assert sent[0].content != response
            assert len(sent[0].content) == 3_000

            before = await store.list_outbox(limit=100)
            assert len(before) == 3

            replay = await gateway.handle_message(
                SimpleNamespace(bot_id="bot"), _command_message()
            )
            after = await store.list_outbox(limit=100)

            assert replay is not None and replay.duplicate
            # Acceptance reports the complete command receipt even though
            # direct wire delivery remains bound to fragment one.
            assert replay.command_response == response
            assert router.calls == 1
            assert len(sent) == 1
            assert [item.outbox_id for item in after] == [
                item.outbox_id for item in before
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_live_duplicate_defers_command_response_to_receipt_owner(tmp_path, monkeypatch):
    class Router:
        def __init__(self) -> None:
            self.calls = 0
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            self.started.set()
            await self.release.wait()
            return "owner response"

    sent: list[str] = []

    async def fake_send(
        _client: Any, delivery: UserDelivery
    ) -> DeliveryReceipt:
        sent.append(delivery.content)
        return DeliveryReceipt(
            delivery_id=delivery.delivery_id,
            client_id=delivery.client_id,
            sent=True,
        )

    monkeypatch.setattr("src.channels.wechat.send_user_delivery_async", fake_send)

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        owner = asyncio.create_task(
            gateway.handle_message(SimpleNamespace(bot_id="bot"), _command_message())
        )
        try:
            await router.started.wait()

            duplicate = await gateway.handle_message(
                SimpleNamespace(bot_id="bot"), _command_message()
            )
            assert duplicate is not None and duplicate.duplicate
            assert duplicate.command_response == ""
            assert router.calls == 1
            assert sent == []

            router.release.set()
            completed = await owner
            assert completed is not None
            assert completed.command_response == "owner response"
            assert sent == ["owner response"]

            outbox = await store.get_outbox_item(
                "command:wechat:bot:user:default:17"
            )
            assert outbox is not None
            assert outbox.content == "owner response"
            assert outbox.state.value == "sent"
        finally:
            router.release.set()
            if not owner.done():
                owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await store.close()

    asyncio.run(scenario())


def test_interrupted_command_receipt_never_reexecutes_unknown_effect(tmp_path):
    class Router:
        def __init__(self, *, fail: bool) -> None:
            self.calls = 0
            self.fail = fail

        async def handle_command(self, _command: Any, _envelope: Any) -> Any:
            self.calls += 1
            if self.fail:
                # Simulate a process dying after the mutating operation but
                # before the response receipt can be completed.
                raise RuntimeError("simulated crash")
            return "must not run"

    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        first_router = Router(fail=True)
        gateway = WeChatGateway(store, bot_id="bot", command_router=first_router)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await gateway.accept(_command_message())
        assert first_router.calls == 1
        await store.close()

        replay_router = Router(fail=False)
        replay_store = SQLiteStore(path)
        replay_gateway = WeChatGateway(
            replay_store, bot_id="bot", command_router=replay_router
        )
        replay = await replay_gateway.accept(_command_message())
        assert replay is not None
        assert replay.command_response == COMMAND_INTERRUPTED_RESPONSE
        assert replay_router.calls == 0
        await replay_store.close()

    asyncio.run(scenario())


def test_live_retry_after_router_failure_gets_interrupted_acknowledgement(tmp_path):
    class Router:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> Any:
            self.calls += 1
            raise RuntimeError("router failed after an unknown effect")

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        try:
            with pytest.raises(RuntimeError, match="router failed"):
                await gateway.accept(_command_message())

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "interrupted"

            replay = await gateway.accept(_command_message())
            assert replay is not None
            assert replay.duplicate
            assert replay.command_response == COMMAND_INTERRUPTED_RESPONSE
            assert router.calls == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_monitor_timeout_interrupts_owned_command_receipt_before_returning(
    tmp_path, monkeypatch
):
    class Router:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            return "owner response"

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        completion_started = threading.Event()

        async def blocked_completion(*_args: Any, **_kwargs: Any) -> Any:
            completion_started.set()
            await asyncio.Event().wait()
            raise AssertionError("blocked completion unexpectedly resumed")

        monkeypatch.setattr(store, "complete_command_receipt", blocked_completion)
        submit = asyncio.run_coroutine_threadsafe

        def submit_with_deterministic_timeout(coroutine, loop):
            future = submit(coroutine, loop)

            class TimedOutFuture:
                def result(self, timeout=None):
                    assert completion_started.wait(1), (
                        "command receipt completion did not start"
                    )
                    raise FutureTimeoutError()

                def cancel(self):
                    return future.cancel()

            return TimedOutFuture()

        monkeypatch.setattr(
            "src.channels.wechat.asyncio.run_coroutine_threadsafe",
            submit_with_deterministic_timeout,
        )
        monitor_handler = gateway.monitor_handler(
            asyncio.get_running_loop(), timeout=15
        )
        timed_out = asyncio.create_task(
            asyncio.to_thread(
                monitor_handler,
                SimpleNamespace(bot_id="bot"),
                _command_message(),
            )
        )
        try:
            with pytest.raises(asyncio.TimeoutError):
                await timed_out

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "interrupted"

            replay = await gateway.accept(_command_message())
            assert replay is not None
            assert replay.duplicate
            assert replay.command_response == COMMAND_INTERRUPTED_RESPONSE
            assert router.calls == 1
        finally:
            if not timed_out.done():
                timed_out.cancel()
            await asyncio.gather(timed_out, return_exceptions=True)
            await store.close()

    asyncio.run(scenario())


def test_command_completion_error_after_commit_preserves_completed_receipt(
    tmp_path, monkeypatch
):
    class Router:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            return "committed response"

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        complete = store.complete_command_receipt

        async def commit_then_fail(*args: Any, **kwargs: Any) -> Any:
            await complete(*args, **kwargs)
            raise RuntimeError("completion acknowledgement was lost")

        monkeypatch.setattr(store, "complete_command_receipt", commit_then_fail)
        try:
            with pytest.raises(
                RuntimeError, match="command response was not durably recorded"
            ):
                await gateway.accept(_command_message())

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "completed"
            assert receipt["response_text"] == "committed response"

            monkeypatch.setattr(store, "complete_command_receipt", complete)
            replay = await gateway.accept(_command_message())
            assert replay is not None
            assert replay.duplicate
            assert replay.command_response == "committed response"
            assert router.calls == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_repeated_cancellation_does_not_abort_receipt_interruption(
    tmp_path, monkeypatch
):
    class Router:
        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.started.set()
            await asyncio.Event().wait()
            raise AssertionError("blocked command unexpectedly resumed")

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        router = Router()
        gateway = WeChatGateway(store, bot_id="bot", command_router=router)
        interrupt = store.interrupt_command_receipt
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()

        async def delayed_interrupt(*args: Any, **kwargs: Any) -> Any:
            cleanup_started.set()
            await release_cleanup.wait()
            return await interrupt(*args, **kwargs)

        monkeypatch.setattr(store, "interrupt_command_receipt", delayed_interrupt)
        owner = asyncio.create_task(gateway.accept(_command_message()))
        try:
            await asyncio.wait_for(router.started.wait(), timeout=1)
            owner.cancel()
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            owner.cancel()
            await asyncio.sleep(0)
            assert not owner.done()

            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await owner

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "interrupted"
        finally:
            release_cleanup.set()
            if not owner.done():
                owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await store.close()

    asyncio.run(scenario())


def test_cancellation_after_receipt_reservation_commit_terminalizes_owner(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        gateway = WeChatGateway(store, bot_id="bot", command_router=object())
        begin = store.begin_command_receipt
        reservation_committed = asyncio.Event()
        release_reservation = asyncio.Event()

        async def commit_then_block(*args: Any, **kwargs: Any) -> Any:
            receipt = await begin(*args, **kwargs)
            reservation_committed.set()
            await release_reservation.wait()
            return receipt

        monkeypatch.setattr(store, "begin_command_receipt", commit_then_block)
        owner = asyncio.create_task(gateway.accept(_command_message()))
        try:
            await asyncio.wait_for(reservation_committed.wait(), timeout=1)
            owner.cancel()
            await asyncio.sleep(0)
            assert not owner.done()

            release_reservation.set()
            with pytest.raises(asyncio.CancelledError):
                await owner

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "interrupted"
        finally:
            release_reservation.set()
            if not owner.done():
                owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await store.close()

    asyncio.run(scenario())


def test_cancellation_after_ask_receipt_reopen_commit_terminalizes_owner(
    tmp_path, monkeypatch
):
    class FailingRouter:
        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            raise RuntimeError("interrupt initial ask receipt")

    class ReplayRouter:
        def __init__(self) -> None:
            self.calls = 0

        async def handle_command(self, _command: Any, _envelope: Any) -> str:
            self.calls += 1
            return "must not execute"

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        message = _command_message(text="/ask planner inspect this")
        with pytest.raises(RuntimeError, match="interrupt initial ask receipt"):
            await WeChatGateway(
                store, bot_id="bot", command_router=FailingRouter()
            ).accept(message)

        reopen = store.reopen_interrupted_command_receipt
        reopen_committed = asyncio.Event()
        release_reopen = asyncio.Event()

        async def commit_then_block(*args: Any, **kwargs: Any) -> Any:
            receipt = await reopen(*args, **kwargs)
            reopen_committed.set()
            await release_reopen.wait()
            return receipt

        monkeypatch.setattr(
            store, "reopen_interrupted_command_receipt", commit_then_block
        )
        router = ReplayRouter()
        owner = asyncio.create_task(
            WeChatGateway(store, bot_id="bot", command_router=router).accept(message)
        )
        try:
            await asyncio.wait_for(reopen_committed.wait(), timeout=1)
            owner.cancel()
            await asyncio.sleep(0)
            assert not owner.done()

            release_reopen.set()
            with pytest.raises(asyncio.CancelledError):
                await owner

            receipt = await store.get_command_receipt(
                "command:wechat:bot:user:default:17"
            )
            assert receipt is not None
            assert receipt["state"] == "interrupted"
            assert router.calls == 0
        finally:
            release_reopen.set()
            if not owner.done():
                owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await store.close()

    asyncio.run(scenario())


def test_second_live_store_does_not_interrupt_active_command_receipt(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        owner = SQLiteStore(path)
        observer = SQLiteStore(path)
        try:
            receipt = await owner.begin_command_receipt(
                "command:wechat:bot:user:default:17",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="17",
                command_name="mode",
                command_args=("chat",),
                command_text="/mode chat",
            )
            assert receipt["state"] == "started"

            await observer.initialize()
            still_active = await observer.get_command_receipt(receipt["command_id"])
            assert still_active is not None
            assert still_active["state"] == "started"

            completed = await owner.complete_command_receipt(
                receipt["command_id"], response_text="Mode changed to chat."
            )
            assert completed["state"] == "completed"
        finally:
            await observer.close()
            await owner.close()

    asyncio.run(scenario())


def test_command_presentation_validation_rolls_back_response_and_marks_atomically(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        try:
            notification = await store.create_user_outbox(
                UserDelivery(
                    delivery_id="notification-1",
                    target=ReplyTarget(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        session_id="default",
                    ),
                    content="background output",
                    client_id="notification-client",
                ),
                agent_id="codex",
            )
            command = UserDelivery(
                delivery_id="command:wechat:bot:user:default:17",
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                ),
                content="switched",
                client_id="command-client",
            )
            with pytest.raises(StoreError):
                await store.create_user_outbox(
                    command,
                    agent_id="codex",
                    present_outbox_ids=(notification.outbox_id, "missing"),
                )
            assert await store.get_outbox_item(command.delivery_id) is None
            unseen = await store.list_outbox(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                unseen=True,
            )
            assert [item.outbox_id for item in unseen] == [notification.outbox_id]

            await store.create_user_outbox(
                command,
                agent_id="codex",
                present_outbox_ids=(notification.outbox_id,),
            )
            presented = await store.get_outbox_item(notification.outbox_id)
            assert presented is not None
            assert presented.presentation.value == "presented"
        finally:
            await store.close()

    asyncio.run(scenario())
