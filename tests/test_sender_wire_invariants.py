"""Durable outbound sender and wire-identity invariants."""

from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from src.channels.wechat import WeChatDeliveryWorker
from src.runtime.models import InboundMessage, ReplyTarget
from src.runtime.sqlite_store import SQLiteStore, StoreError


async def _accepted(
    store: SQLiteStore,
    source_message_id: str,
    *,
    bot_id: str = "bot",
    external_user_id: str = "user",
):
    return await store.accept_inbound(
        InboundMessage(
            channel="wechat",
            bot_id=bot_id,
            external_user_id=external_user_id,
            external_message_id=source_message_id,
            session_id="default",
            text="test input",
            context_token=f"token:{source_message_id}",
        ),
        create_task=False,
    )


@pytest.mark.parametrize(
    ("second_primary", "second_contextless"),
    (
        ("wire-contextless-one", "wire-contextless-two"),
        ("wire-primary-two", "wire-primary-one"),
    ),
    ids=(
        "new-primary-matches-existing-contextless",
        "new-contextless-matches-existing-primary",
    ),
)
def test_reply_wire_ids_are_unique_across_variants_and_rollback_slot(
    tmp_path,
    second_primary: str,
    second_contextless: str,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            first = await _accepted(store, "wire-source-one")
            second = await _accepted(store, "wire-source-two")
            first_projection = await store.project_reply_candidate(
                target=first.inbound.target(),
                source_key="wire-item-one",
                content="first reply",
                client_id="wire-primary-one",
                contextless_client_id="wire-contextless-one",
            )
            assert len(first_projection.slots) == 1

            with pytest.raises(
                StoreError, match="wire client identity conflicts across variants"
            ):
                await store.project_reply_candidate(
                    target=second.inbound.target(),
                    source_key="wire-item-two",
                    content="second reply",
                    client_id=second_primary,
                    contextless_client_id=second_contextless,
                )

            second_scope = await store.get_reply_scope_for_inbound(
                second.inbound.message_id
            )
            assert second_scope is not None
            assert second_scope.used_slots == 0
            rows = await store.list_outbox(channel="wechat", bot_id="bot", limit=20)
            assert [item.content for item in rows] == ["first reply"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_unscoped_event_persists_sender_and_initialize_repairs_v19_row(
    tmp_path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        try:
            task = await store.create_task(
                {
                    "task_id": "legacy-background-task",
                    "agent_id": "codex",
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                    "reply_target": ReplyTarget(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        session_id="default",
                        source_message_id="not-a-stored-inbound",
                    ),
                    "inputs": {"text": "background work"},
                }
            )
            claim = await store.claim_task_by_id(task.task_id, "sender-test-worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            await store.complete_task(
                task.task_id,
                status="completed",
                events=[
                    {
                        "event_id": "legacy-background-event",
                        "execution_id": claim.execution_id,
                        "visibility": "user",
                        "priority": 2,
                        "content": "background reply",
                    }
                ],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            created = next(
                item
                for item in await store.list_outbox(limit=20)
                if item.event_id == "legacy-background-event"
            )
            assert created.reply_scope_id is None
            assert created.from_user_id == "bot"
            outbox_id = created.outbox_id
        finally:
            await store.close()

        # Model an already-marked latest database produced before the idempotent
        # sender repair existed.  Initialization must repair it without relying
        # on the migration body running again.
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (35,)
            connection.execute(
                "UPDATE user_outbox SET from_user_id='' WHERE outbox_id=?",
                (outbox_id,),
            )
            connection.commit()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            repaired = await restarted.get_outbox_item(outbox_id)
            assert repaired is not None
            assert repaired.bot_id == "bot"
            assert repaired.reply_target.bot_id == "bot"
            assert repaired.from_user_id == "bot"
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_contextless_activation_rejects_another_delivery_primary_id(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            target = ReplyTarget(
                channel="wechat",
                bot_id="bot",
                external_user_id="legacy-user",
                session_id="default",
            )
            reserved = await store.create_user_outbox(
                target=target,
                content="reserved primary",
                outbox_id="reserved-outbox",
                client_id="reserved-primary-client",
            )
            transitioning = await store.create_user_outbox(
                target=target,
                content="transitioning delivery",
                outbox_id="transitioning-outbox",
                client_id="transitioning-primary-client",
            )
            assert await store.mark_outbox_sending(
                transitioning.outbox_id, allow_pending=True
            )

            with pytest.raises(
                StoreError, match="wire client identity conflicts across variants"
            ):
                await store.activate_outbox_contextless_variant(
                    transitioning.outbox_id,
                    contextless_client_id=reserved.client_id,
                )

            unchanged = await store.get_outbox_item(transitioning.outbox_id)
            assert unchanged is not None
            assert unchanged.active_wire_variant == "primary"
            assert unchanged.contextless_client_id is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_delivery_worker_selects_and_validates_multiple_bot_clients(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        requests: list[tuple[str, str, str]] = []

        def client(bot_id: str):
            def send(request):
                requests.append(
                    (
                        bot_id,
                        request.msg.from_user_id,
                        request.msg.to_user_id,
                    )
                )
                return SimpleNamespace(ret=0, errcode=0, errmsg="")

            return SimpleNamespace(bot_id=bot_id, send_message=send)

        client_a = client("bot-a")
        client_b = client("bot-b")
        try:
            rows = {}
            for bot_id in ("bot-a", "bot-b", "bot-c"):
                accepted = await _accepted(
                    store,
                    f"source:{bot_id}",
                    bot_id=bot_id,
                    external_user_id=f"user:{bot_id}",
                )
                projection = await store.project_reply_candidate(
                    target=accepted.inbound.target(),
                    source_key=f"item:{bot_id}",
                    content=f"reply from {bot_id}",
                )
                rows[bot_id] = projection.outbox_items[0]

            worker = WeChatDeliveryWorker(
                store,
                SimpleNamespace(bot_id="unused"),
                client_resolver={
                    "bot-a": client_a,
                    "bot-b": client_b,
                    # Deliberately wrong: sender validation must fail before
                    # client-a can put bot-c's durable delivery on the wire.
                    "bot-c": client_a,
                },
                claim_limit=10,
            )
            receipts = await worker.run_once()

            assert len(receipts) == 3
            assert sum(receipt.sent for receipt in receipts) == 2
            assert any(
                not receipt.sent
                and not receipt.retryable
                and "does not match" in receipt.error
                for receipt in receipts
            )
            assert set(requests) == {
                ("bot-a", "bot-a", "user:bot-a"),
                ("bot-b", "bot-b", "user:bot-b"),
            }
            for bot_id in ("bot-a", "bot-b"):
                stored = await store.get_outbox_item(rows[bot_id].outbox_id)
                assert stored is not None
                assert stored.from_user_id == bot_id
                assert stored.state.value == "sent"
            rejected = await store.get_outbox_item(rows["bot-c"].outbox_id)
            assert rejected is not None
            assert rejected.from_user_id == "bot-c"
            assert rejected.state.value == "failed_permanent"
        finally:
            await store.close()

    asyncio.run(scenario())
