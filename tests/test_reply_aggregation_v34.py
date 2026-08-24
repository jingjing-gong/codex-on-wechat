"""Focused schema/store checks for durable bounded reply aggregation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from src.runtime.models import (
    AgentTask,
    InboundMessage,
    ReplyAggregateState,
    ReplyFragmentState,
)
from src.runtime.sqlite_store import SQLiteStore


async def _live_scope(store: SQLiteStore, *, source: str = "aggregate-source"):
    accepted = await store.accept_inbound(
        InboundMessage(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id=source,
            text="aggregate replies",
            context_token="context",
        ),
        create_task=False,
    )
    task = await store.create_task(
        AgentTask(
            task_id=f"task:{source}",
            agent_id="codex",
            mode_id="chat",
            profile_version=1,
            policy_version=1,
            inbound_message_id=accepted.inbound.message_id,
            reply_target=accepted.inbound.target(),
            inputs={"text": "aggregate replies"},
        )
    )
    claim = await store.claim_next_task("aggregate-test-worker")
    assert claim is not None
    return accepted.inbound.target(), task, claim.execution


def test_compatible_items_pack_replay_and_materialize_once(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            target, task, execution = await _live_scope(store)
            common = {
                "target": target,
                "task_id": task.task_id,
                "execution_id": execution.execution_id,
                "source_item_type": "agentMessage",
            }
            first = await store.append_reply_aggregate_member(
                **common,
                source_key="item:first",
                content="first",
                source_sequence=1,
                source_item_id="first",
                source_item_ordinal=1,
            )
            assert first.open_aggregate is not None
            assert first.open_aggregate.content == "first"
            second = await store.append_reply_aggregate_member(
                **common,
                source_key="item:second",
                content="second",
                source_sequence=2,
                source_item_id="second",
                source_item_ordinal=2,
            )
            assert second.open_aggregate is not None
            assert second.open_aggregate.content == "first\n\nsecond"
            replay = await store.append_reply_aggregate_member(
                **common,
                source_key="item:second",
                content="second",
                source_sequence=2,
                source_item_id="second",
                source_item_ordinal=2,
            )
            assert replay.replayed
            sealed = await store.seal_reply_aggregates(
                task_id=task.task_id,
                execution_id=execution.execution_id,
                reason="task_completed",
            )
            assert len(sealed) == 1
            projected = await store.materialize_sealed_reply_aggregate(
                sealed[0].reply_aggregate_id
            )
            assert projected.fragments[0].state is ReplyFragmentState.ALLOCATED
            assert projected.outbox_items[0].content == "first\n\nsecond"
            assert projected.outbox_items[0].reply_aggregate_id == (
                sealed[0].reply_aggregate_id
            )
            replayed_projection = await store.materialize_sealed_reply_aggregate(
                sealed[0].reply_aggregate_id
            )
            assert replayed_projection.replayed
            assert replayed_projection.outbox_items[0].outbox_id == (
                projected.outbox_items[0].outbox_id
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_prefix_split_deadline_and_due_sealing(tmp_path):
    async def scenario() -> None:
        start = datetime(2026, 8, 17, tzinfo=timezone.utc)
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            reply_aggregation_max_age_seconds=120,
        )
        await store.initialize()
        try:
            target, task, execution = await _live_scope(store, source="split")
            result = await store.append_reply_aggregate_member(
                target=target,
                source_key="item:long",
                content="x" * 3_010,
                source_sequence=1,
                task_id=task.task_id,
                execution_id=execution.execution_id,
                source_item_id="long",
                source_item_type="agentMessage",
                source_item_ordinal=1,
                sender_format="agent-prefix-v1",
                sender_prefix="codex: ",
                now=start,
            )
            assert [len(item.content) for item in result.aggregates] == [3000, 24]
            assert all(item.content.startswith("codex: ") for item in result.aggregates)
            assert result.aggregates[0].state is ReplyAggregateState.SEALED
            assert result.open_aggregate is not None
            assert result.open_aggregate.flush_due_at == start + timedelta(seconds=120)
            assert not await store.seal_due_reply_aggregates(
                now=start + timedelta(seconds=119)
            )
            due = await store.seal_due_reply_aggregates(
                now=start + timedelta(seconds=120)
            )
            assert len(due) == 1
            assert due[0].seal_reason == "max_age"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_predicted_tenth_slot_reserves_suffix_without_losing_tail(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            target, task, execution = await _live_scope(store, source="tenth")
            for ordinal in range(1, 10):
                await store.project_reply_candidate(
                    target=target,
                    source_key=f"legacy:{ordinal}",
                    content=f"legacy {ordinal}",
                )
            result = await store.append_reply_aggregate_member(
                target=target,
                source_key="item:tenth",
                content="z" * 3_000,
                source_sequence=1,
                task_id=task.task_id,
                execution_id=execution.execution_id,
                source_item_id="tenth",
                source_item_type="agentMessage",
                source_item_ordinal=1,
                seal_after_append=True,
            )
            assert sum(len(member.rendered_content) for member in result.members) == 3000
            assert len(result.sealed_aggregates[0].content) < 3000
            first = await store.materialize_sealed_reply_aggregate(
                result.sealed_aggregates[0].reply_aggregate_id
            )
            assert len(first.outbox_items[0].content) == 3000
            assert first.outbox_items[0].content.endswith("Reply /recv to continue.")
            remaining = result.sealed_aggregates[1:]
            assert remaining
            deferred = await store.materialize_sealed_reply_aggregate(
                remaining[0].reply_aggregate_id
            )
            assert deferred.fragments[0].state is ReplyFragmentState.DEFERRED_QUOTA
            assert not deferred.outbox_items
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v34_constructor_rejects_invalid_deadline():
    for value in (0, -1, float("inf"), "invalid"):
        try:
            SQLiteStore(reply_aggregation_max_age_seconds=value)
        except ValueError:
            continue
        raise AssertionError(f"deadline should be rejected: {value!r}")
