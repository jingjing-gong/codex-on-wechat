from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from src.runtime.manager import TaskManager
from src.runtime.models import (
    InboundMessage,
    TaskSteeringState,
)
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import QueueFullError


def _message(message_id: str, text: str, *, user_id: str = "owner") -> InboundMessage:
    return InboundMessage(
        channel="wechat",
        bot_id="bot",
        external_user_id=user_id,
        external_message_id=message_id,
        text=text,
    )


async def _running_task(store: SQLiteStore):
    accepted = await store.accept_inbound(_message("initial", "start"))
    assert accepted.task is not None
    claim = await store.claim_next_task("worker")
    assert claim is not None
    assert await store.mark_task_running(
        claim.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    return accepted, claim


def test_active_task_absorbs_ordered_followups_without_queue_debit(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            initial, claim = await _running_task(store)
            before = await store.get_global_agent_admission_counter()
            assert before is not None

            first = await store.accept_inbound(_message("follow-1", "one"))
            second = await store.accept_inbound(_message("follow-2", "two"))
            replay = await store.accept_inbound(_message("follow-1", "one"))

            assert first.task is not None and second.task is not None
            assert first.task.task_id == initial.task.task_id
            assert second.task.task_id == initial.task.task_id
            assert first.steered and second.steered
            assert first.steering is not None and second.steering is not None
            assert [first.steering.sequence, second.steering.sequence] == [1, 2]
            assert first.steering.inputs == {"text": "one"}
            assert second.steering.inputs == {"text": "two"}
            assert replay.duplicate
            assert replay.steering_id == first.steering_id
            assert replay.steering == first.steering

            after = await store.get_global_agent_admission_counter()
            assert after is not None
            assert after.unfinished_count == before.unfinished_count == 1
            tasks = await store.list_tasks(limit=10)
            assert [task.task_id for task in tasks] == [claim.task_id]

            steering_claim = await store.claim_next_task_steering(
                claim.task_id,
                claim.execution_id,
                task_claim_token=claim.claim_token,
                claimed_by="worker",
            )
            assert steering_claim is not None
            assert steering_claim.sequence == 1
            assert steering_claim.inputs == {"text": "one"}
            applied = await store.mark_task_steering_applied(
                steering_claim.steering_id,
                claim_token=steering_claim.claim_token,
            )
            assert applied.state is TaskSteeringState.APPLIED

            next_claim = await store.claim_next_task_steering(
                claim.task_id,
                claim.execution_id,
                task_claim_token=claim.claim_token,
                claimed_by="worker",
            )
            assert next_claim is not None
            assert next_claim.sequence == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_finish_race_promotes_one_fallback_and_retargets_ordered_tail(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            _initial, claim = await _running_task(store)
            followups = [
                await store.accept_inbound(_message(f"follow-{index}", str(index)))
                for index in range(1, 4)
            ]
            # The native provider thread is commonly learned after the steer
            # was accepted.  A race fallback must inherit that compatible
            # binding instead of starting disconnected history.
            assert await store.set_task_thread(
                claim.task_id,
                thread_id="provider-thread-after-start",
                claim_token=claim.claim_token,
            )
            await store.complete_task(
                claim.task_id,
                "done",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            records = [
                await store.get_task_steering(item.steering_id or "")
                for item in followups
            ]
            assert all(record is not None for record in records)
            first, second, third = records
            assert first is not None and second is not None and third is not None
            assert first.state is TaskSteeringState.PROMOTED
            assert first.promoted_task_id
            assert second.state is TaskSteeringState.PENDING
            assert third.state is TaskSteeringState.PENDING
            assert second.target_task_id == third.target_task_id == first.promoted_task_id
            assert second.target_execution_id == third.target_execution_id
            assert [second.sequence, third.sequence] == [2, 3]

            tasks = await store.list_tasks(limit=10, newest_first=False)
            assert len(tasks) == 2
            fallback = tasks[1]
            assert fallback.task_id == first.promoted_task_id
            assert fallback.inputs == {"text": "1"}
            assert fallback.reply_target.source_message_id == "follow-1"
            assert fallback.thread_id == "provider-thread-after-start"

            fallback_claim = await store.claim_next_task("worker")
            assert fallback_claim is not None
            assert fallback_claim.task_id == fallback.task_id
            assert await store.mark_task_running(
                fallback_claim.task_id,
                fallback_claim.claim_token,
                execution_id=fallback_claim.execution_id,
            )
            tail = []
            for _ in range(2):
                item = await store.claim_next_task_steering(
                    fallback_claim.task_id,
                    fallback_claim.execution_id,
                    task_claim_token=fallback_claim.claim_token,
                    claimed_by="worker",
                )
                assert item is not None
                tail.append(item.inputs["text"])
                await store.mark_task_steering_applied(
                    item.steering_id,
                    claim_token=item.claim_token,
                )
            assert tail == ["2", "3"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_uncertain_delivery_is_not_replayed_but_later_pending_is_preserved(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            _initial, claim = await _running_task(store)
            first = await store.accept_inbound(_message("follow-1", "uncertain"))
            second = await store.accept_inbound(_message("follow-2", "preserve"))
            steering_claim = await store.claim_next_task_steering(
                claim.task_id,
                claim.execution_id,
                task_claim_token=claim.claim_token,
                claimed_by="worker",
                lease_seconds=1,
            )
            assert steering_claim is not None

            # Losing the delivery owner after the lease is ambiguous.  The row
            # becomes terminal-unknown rather than claimable again.
            await store.recover_task_steering(
                now=steering_claim.steering.lease_expires_at
                + timedelta(seconds=1)
            )
            uncertain = await store.get_task_steering(first.steering_id or "")
            assert uncertain is not None
            assert uncertain.state is TaskSteeringState.DELIVERY_UNKNOWN

            await store.complete_task(
                claim.task_id,
                "done",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            preserved = await store.get_task_steering(second.steering_id or "")
            assert preserved is not None
            assert preserved.state is TaskSteeringState.PROMOTED
            assert preserved.promoted_task_id
            fallback = await store.get_task(preserved.promoted_task_id)
            assert fallback is not None
            assert fallback.inputs == {"text": "preserve"}
            assert len(await store.list_tasks(limit=10)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_queue_full_during_finish_keeps_pending_and_later_recovery_promotes(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            _initial, claim = await _running_task(store)
            followup = await store.accept_inbound(_message("follow", "keep me"))
            # Simulate an admission policy becoming temporarily unavailable.
            # The fallback savepoint must not roll back task completion.
            original_limit = store.max_global_queue
            store.max_global_queue = 0
            completed = await store.complete_task(
                claim.task_id,
                "done",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert completed.state.value == "completed"
            pending = await store.get_task_steering(followup.steering_id or "")
            assert pending is not None
            assert pending.state is TaskSteeringState.PENDING
            assert len(await store.list_tasks(limit=10)) == 1

            store.max_global_queue = original_limit
            assert await store.recover_task_steering() == 1
            promoted = await store.get_task_steering(followup.steering_id or "")
            assert promoted is not None
            assert promoted.state is TaskSteeringState.PROMOTED
            assert len(await store.list_tasks(limit=10)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_newer_input_cannot_overtake_queue_limited_steering_fallback(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            _initial, claim = await _running_task(store)
            older = await store.accept_inbound(_message("older", "B"))
            original_limit = store.max_global_queue
            store.max_global_queue = 0
            await store.complete_task(
                claim.task_id,
                "A",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert (
                await store.get_task_steering(older.steering_id or "")
            ).state is TaskSteeringState.PENDING

            # Capacity returned before periodic reconciliation. Accepting C
            # must transactionally publish the older B fallback first.
            store.max_global_queue = original_limit
            newer = await store.accept_inbound(_message("newer", "C"))
            assert not newer.steered
            promoted = await store.get_task_steering(older.steering_id or "")
            assert promoted is not None
            assert promoted.state is TaskSteeringState.PROMOTED

            tasks = await store.list_tasks(limit=10, newest_first=False)
            assert sorted(task.inputs["text"] for task in tasks) == ["B", "C", "start"]
            invocations = {
                item.task_id: item
                for item in await store.list_agent_invocations(limit=10)
                if item.task_id is not None
            }
            assert newer.task is not None
            assert (
                invocations[promoted.promoted_task_id].ready_sequence
                < invocations[newer.task.task_id].ready_sequence
            )
            next_claim = await store.claim_next_task("worker-2")
            assert next_claim is not None
            assert next_claim.task_id == promoted.promoted_task_id
            assert next_claim.task.inputs["text"] == "B"
            assert await store.recover_task_steering() == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_predecessor_promotion_and_newer_ingress_share_admission_transaction(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            _initial, claim = await _running_task(store)
            older = await store.accept_inbound(_message("older", "B"))
            store.max_global_queue = 0
            await store.complete_task(
                claim.task_id,
                "A",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            # Only one queue slot is available. B owns it; C must not commit
            # ahead of B, and the failed transaction must not half-promote B.
            store.max_global_queue = 1
            with pytest.raises(QueueFullError) as raised:
                await store.accept_inbound(_message("newer", "C"))
            assert raised.value.scope == "global"
            pending = await store.get_task_steering(older.steering_id or "")
            assert pending is not None
            assert pending.state is TaskSteeringState.PENDING
            assert await store.get_inbound("wechat", "bot", "newer") is None
            assert len(await store.list_tasks(limit=10)) == 1

            assert await store.recover_task_steering() == 1
            promoted = await store.get_task_steering(older.steering_id or "")
            assert promoted is not None
            assert promoted.state is TaskSteeringState.PROMOTED
            assert len(await store.list_tasks(limit=10)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_different_conversation_is_not_steered(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await _running_task(store)
            other = await store.accept_inbound(
                _message("other-message", "separate", user_id="someone-else")
            )
            assert not other.steered
            assert other.task is not None
            assert len(await store.list_tasks(limit=10)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_skill_input_can_steer_when_execution_context_matches(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            initial, _claim = await _running_task(store)
            assert initial.task is not None
            skill = {
                "skill_id": "review",
                "name": "review",
                "path": "/trusted/skills/review/SKILL.md",
                "version": "1",
                "content_hash": "sha256:trusted-snapshot",
            }
            followup = await store.accept_inbound(
                _message("skill-followup", "review this"),
                task={
                    "inputs": {"text": "review this", "skill": skill},
                    "metadata": {
                        "skill": skill,
                        "skill_id": skill["skill_id"],
                        "skill_version": skill["version"],
                        "skill_hash": skill["content_hash"],
                    },
                },
            )

            assert followup.steered
            assert followup.task is not None
            assert followup.task.task_id == initial.task.task_id
            assert followup.steering is not None
            assert followup.steering.inputs == {
                "text": "review this",
                "skill": skill,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


class _NoopRuntime:
    agent_id = "codex"

    async def start(self):
        return None

    async def stop(self):
        return None

    async def run(self, task, emit):  # pragma: no cover - no worker in this test
        raise AssertionError("runtime should not run")

    async def interrupt(self, task_id):
        return False


class _SteeringNotifier:
    def __init__(self) -> None:
        self.task_ids: list[str] = []

    def notify_steering(self, task_id: str) -> None:
        self.task_ids.append(task_id)


def test_manager_wakes_worker_and_replays_steering_without_live_route_resolution(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        manager = TaskManager(store, runtime=_NoopRuntime(), worker_count=0)
        notifier = _SteeringNotifier()
        manager._workers.append(notifier)  # focused loop-local integration fake
        try:
            initial = await manager.accept_inbound(_message("initial", "start"))
            assert initial.task is not None
            claim = await store.claim_next_task("worker")
            assert claim is not None
            assert await store.mark_task_running(
                claim.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            followup_message = _message("follow", "steer")
            followup = await manager.accept_inbound(followup_message)
            assert followup.steered
            assert notifier.task_ids == [claim.task_id]

            async def forbidden_route_lookup(*_args, **_kwargs):
                raise AssertionError("immutable replay consulted the live route")

            manager._resolve_active_agent = forbidden_route_lookup
            replay = await manager.accept_inbound(followup_message)
            assert replay.duplicate
            assert replay.steering_id == followup.steering_id
            assert notifier.task_ids == [claim.task_id, claim.task_id]
        finally:
            await store.close()

    asyncio.run(scenario())
