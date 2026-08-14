"""Foreground and notification-delivery durability regressions."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from src.channels.models import ReplyTarget, UserDelivery
from src.runtime.manager import TaskManager
from src.runtime.models import InboundMessage
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry
from src.runtime.sqlite_store import SQLiteStore, StoreError


async def _complete_with_event(
    store: SQLiteStore,
    task_id: str,
    *,
    event_id: str,
    priority: int,
) -> None:
    claim = await store.claim_task_by_id(task_id, "delivery-test-worker")
    assert claim is not None
    assert await store.mark_task_running(
        task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    await store.complete_task(
        task_id,
        status="completed",
        events=[
            {
                "event_id": event_id,
                "execution_id": claim.execution_id,
                "visibility": "user",
                "priority": priority,
                "content": event_id,
            }
        ],
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )


def test_outbox_replay_tolerates_mutable_notification_eligibility(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            delivery = UserDelivery(
                delivery_id="mutable-notify-outbox",
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                ),
                content="background notification",
                priority=2,
                client_id="mutable-notify-client",
            )
            created = await store.create_user_outbox(
                delivery,
                agent_id="codex",
            )
            assert created.notify_enabled
            assert not created.foreground

            await store.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                enabled=False,
            )

            # Delivery eligibility is mutable control state, not part of the
            # immutable outbox envelope. Replaying the same projection must
            # return its current eligibility instead of conflicting.
            replay = await store.create_user_outbox(
                delivery,
                agent_id="codex",
            )
            assert replay.outbox_id == created.outbox_id
            assert not replay.notify_enabled
            assert replay.content == created.content
            assert replay.reply_target == created.reply_target

            # Foreground/background is a projection-time fact. Unlike the
            # mutable notification preference, replay cannot reclassify it.
            with pytest.raises(StoreError, match="foreground"):
                await store.create_user_outbox(
                    delivery,
                    agent_id="codex",
                    foreground=True,
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_direct_reply_candidate_replay_keeps_delivery_snapshot_strict(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="direct-candidate-input",
                    text="reply directly",
                    ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            claim = await store.claim_task_by_id(
                accepted.task.task_id,
                "direct-candidate-worker",
            )
            assert claim is not None
            assert await store.mark_task_running(
                accepted.task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            event = await store.append_task_event(
                accepted.task.task_id,
                {
                    "event_id": "direct-candidate-event",
                    "execution_id": claim.execution_id,
                    "event_type": "agent_message",
                    "content": "direct reply",
                    "source_item_id": "direct-candidate-item",
                    "source_item_type": "agentmessage",
                    "source_item_ordinal": 0,
                },
                claim_token=claim.claim_token,
                defer_user_projection=True,
            )
            projection = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key=f"item:{event.source_item_id}",
                content=event.content,
                task_id=accepted.task.task_id,
                execution_id=claim.execution_id,
                event_id=event.event_id,
                source_item_id=event.source_item_id,
                source_item_type=event.source_item_type,
                source_item_ordinal=event.source_item_ordinal,
                notify_enabled=False,
                foreground=False,
            )
            assert not projection.candidate.notify_enabled
            assert not projection.candidate.foreground

            # The event replay exception is private to task-event projection.
            # Even a matching event ID cannot reclassify a candidate through
            # the public/direct API.
            with pytest.raises(
                StoreError,
                match="reply candidate identity conflicts: notify_enabled, foreground",
            ):
                await store.project_reply_candidate(
                    target=accepted.inbound.target(),
                    source_key=f"item:{event.source_item_id}",
                    content=event.content,
                    task_id=accepted.task.task_id,
                    execution_id=claim.execution_id,
                    event_id=event.event_id,
                    source_item_id=event.source_item_id,
                    source_item_type=event.source_item_type,
                    source_item_ordinal=event.source_item_ordinal,
                    notify_enabled=True,
                    foreground=True,
                )
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    (
        "initial_agent_id",
        "replay_agent_id",
        "expected_delivery_snapshot",
        "expected_outbox_count",
    ),
    (
        ("planner", "codex", (False, False), 0),
        ("codex", "planner", (True, True), 1),
    ),
    ids=("background-to-foreground", "foreground-to-background"),
)
def test_completed_item_replay_preserves_initial_route_delivery_snapshot(
    tmp_path,
    initial_agent_id,
    replay_agent_id,
    expected_delivery_snapshot,
    expected_outbox_count,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        try:
            await store.set_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                active_agent_id=initial_agent_id,
            )
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id=f"route-replay-{initial_agent_id}",
                    text="project once, then finish",
                ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            claim = await store.claim_task_by_id(
                accepted.task.task_id,
                "route-replay-worker",
            )
            assert claim is not None
            assert await store.mark_task_running(
                accepted.task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            event = await store.append_task_event(
                accepted.task.task_id,
                {
                    "event_id": f"route-replay-event-{initial_agent_id}",
                    "execution_id": claim.execution_id,
                    "event_type": "agent_message",
                    "visibility": "user",
                    "priority": 1,
                    "content": "stable completed item",
                    "source_item_id": f"route-replay-item-{initial_agent_id}",
                    "source_item_type": "agentmessage",
                    "source_item_ordinal": 0,
                },
                claim_token=claim.claim_token,
            )
            assert event.source_item_id == f"route-replay-item-{initial_agent_id}"
            assert event.source_item_type == "agentmessage"
            assert event.source_item_ordinal == 0

            initial_outbox = await store.list_outbox(limit=10)
            assert len(initial_outbox) == expected_outbox_count
            if initial_outbox:
                assert (
                    initial_outbox[0].notify_enabled,
                    initial_outbox[0].foreground,
                ) == expected_delivery_snapshot
            with sqlite3.connect(path) as connection:
                assert connection.execute(
                    "SELECT notify_enabled, foreground FROM reply_candidates"
                ).fetchone() == tuple(
                    int(value) for value in expected_delivery_snapshot
                )

            await store.set_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                active_agent_id=replay_agent_id,
            )
            completed = await store.complete_task(
                accepted.task.task_id,
                status="completed",
                events=[event],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert completed.state.value == "completed"

            replayed_outbox = await store.list_outbox(limit=10)
            assert len(replayed_outbox) == expected_outbox_count
            if replayed_outbox:
                assert replayed_outbox[0].outbox_id == initial_outbox[0].outbox_id
                assert (
                    replayed_outbox[0].notify_enabled,
                    replayed_outbox[0].foreground,
                ) == expected_delivery_snapshot
            with sqlite3.connect(path) as connection:
                assert connection.execute(
                    "SELECT COUNT(*) FROM reply_candidates"
                ).fetchone()[0] == 1
                assert connection.execute(
                    "SELECT COUNT(*) FROM reply_fragments"
                ).fetchone()[0] == 1
                assert connection.execute(
                    "SELECT COUNT(*) FROM user_outbox"
                ).fetchone()[0] == expected_outbox_count
                candidate_snapshot = connection.execute(
                    "SELECT notify_enabled, foreground FROM reply_candidates"
                ).fetchone()
                assert candidate_snapshot == tuple(
                    int(value) for value in expected_delivery_snapshot
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_foreground_migration_recovers_after_partial_schema_application(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="migration-input",
                    text="foreground",
                ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            await _complete_with_event(
                store,
                accepted.task.task_id,
                event_id="migration-event",
                priority=1,
            )
            command = await store.create_user_outbox(
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                ),
                content="command response",
                agent_id="codex",
                outbox_id="command:migration",
                foreground=True,
            )
            background = await store.create_user_outbox(
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                ),
                content="unrelated background output",
                agent_id="codex",
                outbox_id="background:migration",
            )
        finally:
            await store.close()

        # Model a process interruption after ALTER TABLE committed but before
        # the migration marker/backfill became durable. Initialization must
        # tolerate the existing column and finish the deterministic backfill.
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE user_outbox SET foreground=0")
            connection.execute("DELETE FROM schema_migrations WHERE version >= 12")
            connection.commit()

        recovered = SQLiteStore(path)
        await recovered.initialize()
        try:
            outbox_items = await recovered.list_outbox(limit=10)
            rows = {item.outbox_id: item for item in outbox_items}
            by_event = {item.event_id: item for item in outbox_items}
            assert rows[command.outbox_id].foreground
            assert by_event["migration-event"].foreground
            assert not rows[background.outbox_id].foreground
            with sqlite3.connect(path) as connection:
                versions = {
                    int(row[0])
                    for row in connection.execute(
                        "SELECT version FROM schema_migrations WHERE version >= 12"
                    )
                }
                assert versions == {12, 13, 14, 15, 16, 17, 18, 19}
        finally:
            await recovered.close()

    asyncio.run(scenario())


def test_notification_change_uses_projection_time_foreground_state(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            background = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="background-input",
                    text="background",
                ),
                task={"agent_id": "codex"},
            )
            assert background.task is not None
            await store.set_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                active_agent_id="planner",
            )
            await _complete_with_event(
                store,
                background.task.task_id,
                event_id="background-event",
                priority=2,
            )

            await store.set_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                active_agent_id="codex",
            )
            foreground = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="foreground-input",
                    text="foreground",
                ),
                task={"agent_id": "codex"},
            )
            assert foreground.task is not None
            await _complete_with_event(
                store,
                foreground.task.task_id,
                event_id="foreground-event",
                priority=1,
            )

            rows = {item.event_id: item for item in await store.list_outbox()}
            assert not rows["background-event"].foreground
            assert rows["background-event"].notify_enabled
            assert rows["foreground-event"].foreground
            assert rows["foreground-event"].notify_enabled

            await store.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                enabled=False,
            )
            rows = {item.event_id: item for item in await store.list_outbox()}
            assert not rows["background-event"].notify_enabled
            # Interactive responses remain eligible even under /notify off.
            assert rows["foreground-event"].notify_enabled

            claimed = await store.claim_outbox(
                "delivery-worker",
                channel="wechat",
                bot_id="bot",
                limit=10,
            )
            assert [item.event_id for item in claimed] == ["foreground-event"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_custom_default_agent_is_foreground_without_route_row(tmp_path):
    class Runtime:
        agent_id = "planner"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, _task, _emit):  # pragma: no cover - no workers
            raise AssertionError("runtime must not execute in this test")

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry(default_agent_id="planner")
        registry.register(
            "planner",
            Runtime(),
            profile=AgentProfile(agent_id="planner", display_name="Planner"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="planner",
        )
        await manager.start()
        try:
            accepted = await manager.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="planner-input",
                    text="plan this",
                )
            )
            assert accepted.task is not None
            assert accepted.task.agent_id == "planner"
            assert accepted.task.metadata["front_agent_id_at_acceptance"] == "planner"
            assert await store.list_agents_routes(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
            ) == []

            await _complete_with_event(
                store,
                accepted.task.task_id,
                event_id="planner-foreground-event",
                priority=1,
            )
            outbox = (await store.list_outbox())[0]
            assert outbox.agent_id == "planner"
            assert outbox.foreground
            assert outbox.notify_enabled

            await store.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
                enabled=False,
            )
            assert (await store.get_outbox_item(outbox.outbox_id)).notify_enabled
        finally:
            await manager.stop()

    asyncio.run(scenario())
