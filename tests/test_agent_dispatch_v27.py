"""Atomic, pre-grant child dispatch storage primitives (schema v27)."""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.models import (
    AgentProcessState,
    DispatchDecisionKind,
    DispatchSourceState,
    InvocationState,
)
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError


T0 = datetime(2026, 8, 15, 3, 0, tzinfo=timezone.utc)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _task(task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "agent_id": "codex",
        "conversation_id": "wechat:bot:user:default:codex",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": {"text": task_id},
    }


async def _owned_ready_store(
    path: Path,
) -> tuple[SQLiteStore, object, list[datetime]]:
    current = [T0]
    store = SQLiteStore(path, clock=lambda: current[0])
    await store.initialize(recover_startup_state=False)
    epoch = await store.activate_supervisor_epoch(
        owner_instance_id="dispatch-owner",
        channel="wechat",
        bot_id="bot",
        started_at=T0,
    )
    await store.begin_agent_process_generation(
        agent_id="codex",
        agent_incarnation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="dispatch-owner",
        generation_capability="dispatch-generation-secret",
        lifetime_lock_identity="dispatch-lock:codex:1",
        lifetime_lock_acquired_at=T0 + timedelta(seconds=1),
        process_lease_identity="dispatch-process-lease",
        process_lease_token="dispatch-process-token",
        lease_expires_at=T0 + timedelta(minutes=10),
        started_at=T0 + timedelta(seconds=2),
    )
    await store.commit_agent_process_handshake(
        agent_id="codex",
        agent_incarnation=1,
        worker_generation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="dispatch-owner",
        generation_capability="dispatch-generation-secret",
        process_lease_identity="dispatch-process-lease",
        process_lease_token="dispatch-process-token",
        pid=801,
        process_group_id=801,
        kernel_process_birth_id="linux-birth-801",
        hello_frame_id="dispatch-hello",
        hello_payload_hash=_digest("hello"),
        capabilities_frame_id="dispatch-capabilities",
        capabilities_payload_hash=_digest("capabilities"),
        capability_snapshot_hash=_digest("snapshot"),
        committed_at=T0 + timedelta(seconds=3),
    )
    await store.commit_agent_process_ready(
        agent_id="codex",
        agent_incarnation=1,
        worker_generation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="dispatch-owner",
        generation_capability="dispatch-generation-secret",
        process_lease_identity="dispatch-process-lease",
        process_lease_token="dispatch-process-token",
        ready_frame_id="dispatch-ready",
        ready_payload_hash=_digest("ready"),
        ready_at=T0 + timedelta(seconds=4),
    )
    current[0] = T0 + timedelta(seconds=5)
    return store, epoch, current


async def _tag_child_invocations(
    store: SQLiteStore,
    *invocation_ids: str,
) -> None:
    placeholders = ",".join("?" for _ in invocation_ids)

    def op(conn: sqlite3.Connection) -> None:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE task_executions SET dispatch_backend='child' "
                f"WHERE execution_id IN ({placeholders})",
                invocation_ids,
            )
            conn.execute(
                "UPDATE agent_invocations SET dispatch_backend='child' "
                f"WHERE invocation_id IN ({placeholders})",
                invocation_ids,
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    await store._call(op)


def _reserve_kwargs(epoch: int, suffix: str, *, now: datetime) -> dict[str, object]:
    return {
        "agent_id": "codex",
        "agent_incarnation": 1,
        "worker_generation": 1,
        "supervisor_epoch": epoch,
        "owner_instance_id": "dispatch-owner",
        "generation_capability": "dispatch-generation-secret",
        "process_lease_identity": "dispatch-process-lease",
        "process_lease_token": "dispatch-process-token",
        "dispatcher_id": "dispatch-supervisor",
        "dispatch_attempt_id": f"dispatch-attempt-{suffix}",
        "slot_id": f"dispatch-slot-{suffix}",
        "claim_token": f"dispatch-claim-{suffix}",
        "lease_identity": f"dispatch-invocation-lease-{suffix}",
        "lease_expires_at": T0 + timedelta(minutes=5),
        "invocation_job_identity": f"dispatch-job-{suffix}",
        "now": now,
    }


def _decision_kwargs(reservation, epoch: int) -> dict[str, object]:
    return {
        "agent_id": "codex",
        "agent_incarnation": 1,
        "worker_generation": 1,
        "supervisor_epoch": epoch,
        "owner_instance_id": "dispatch-owner",
        "generation_capability": "dispatch-generation-secret",
        "process_lease_identity": "dispatch-process-lease",
        "process_lease_token": "dispatch-process-token",
        "dispatch_attempt_id": reservation.attempt.dispatch_attempt_id,
        "invocation_id": reservation.invocation.invocation_id,
        "slot_id": reservation.slot.slot_id,
        "claim_token": reservation.claim_token,
        "lease_identity": reservation.attempt.lease_identity,
        "invocation_job_identity": reservation.attempt.invocation_job_identity,
    }


def _cleanup_proof(reservation, epoch: int) -> dict[str, object]:
    return {
        "proof_kind": "invocation-cleanup-v1",
        "dispatch_attempt_id": reservation.attempt.dispatch_attempt_id,
        "invocation_id": reservation.invocation.invocation_id,
        "slot_id": reservation.slot.slot_id,
        "agent_id": "codex",
        "agent_incarnation": 1,
        "worker_generation": 1,
        "supervisor_epoch": epoch,
        "process_lease_identity": "dispatch-process-lease",
        "lease_identity": reservation.attempt.lease_identity,
        "invocation_job_identity": reservation.attempt.invocation_job_identity,
        "runtime_stopped": True,
        "invocation_job_empty": True,
        "runtime_stopped_at": T0 + timedelta(seconds=6, milliseconds=100),
        "invocation_job_empty_at": T0 + timedelta(seconds=6, milliseconds=200),
        "checked_at": T0 + timedelta(seconds=6, milliseconds=300),
    }


def _process_cleanup_proof(
    epoch: int,
    *,
    checked_at: datetime,
) -> dict[str, object]:
    return {
        "proof_kind": "verified_empty_v1",
        "agent_id": "codex",
        "agent_incarnation": 1,
        "worker_generation": 1,
        "supervisor_epoch": epoch,
        "fencing_supervisor_epoch": epoch,
        "process_lease_identity": "dispatch-process-lease",
        "lifetime_lock_identity": "dispatch-lock:codex:1",
        "process_group_id": 801,
        "kernel_process_birth_id": "linux-birth-801",
        "runtime_stopped": True,
        "process_group_empty": True,
        "invocation_jobs_empty": True,
        "lifetime_lock_released": True,
        "checked_at": checked_at,
    }


def test_atomic_reservation_selects_lowest_child_candidate_and_replays(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            compatibility = await store.create_task(_task("compatibility-first"))
            child_task = await store.create_task(_task("child-task-second"))
            child_mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="child mailbox third",
                request_id="child-mailbox-third",
                message_id="child-mailbox-message-third",
            )
            assert child_mailbox.current_invocation_id is not None
            await _tag_child_invocations(
                store,
                child_task.execution_id,
                child_mailbox.current_invocation_id,
            )

            kwargs = _reserve_kwargs(
                epoch.epoch,
                "one",
                now=T0 + timedelta(seconds=5),
            )
            reservation = await store.reserve_next_agent_invocation(**kwargs)
            assert reservation is not None
            assert reservation.replayed is False
            assert reservation.task is not None
            assert reservation.task.task_id == child_task.task_id
            assert reservation.mailbox is None
            assert reservation.invocation.ready_sequence == 2
            assert reservation.invocation.state is InvocationState.DISPATCHING
            assert reservation.slot.slot_sequence == 1
            assert reservation.attempt.decision_kind is None
            assert "dispatch-claim-one" not in repr(reservation)

            replay = await store.reserve_next_agent_invocation(**kwargs)
            assert replay is not None and replay.replayed is True
            assert replay.attempt == reservation.attempt
            with pytest.raises(StoreError, match="conflicts"):
                await store.reserve_next_agent_invocation(
                    **{**kwargs, "claim_token": "wrong-dispatch-claim"}
                )
            assert await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "two",
                    now=T0 + timedelta(seconds=5, milliseconds=100),
                )
            ) is None

            snapshot = await store._call(
                lambda conn: {
                    "compatibility": tuple(
                        conn.execute(
                            "SELECT state,dispatch_backend FROM agent_invocations "
                            "WHERE invocation_id=?",
                            (compatibility.execution_id,),
                        ).fetchone()
                    ),
                    "task": tuple(
                        conn.execute(
                            "SELECT state,dispatch_backend FROM task_executions "
                            "WHERE execution_id=?",
                            (child_task.execution_id,),
                        ).fetchone()
                    ),
                    "process": conn.execute(
                        "SELECT observed_state FROM agent_processes "
                        "WHERE agent_id='codex' AND worker_generation=1"
                    ).fetchone()[0],
                    "active_slots": conn.execute(
                        "SELECT COUNT(*) FROM agent_execution_slots "
                        "WHERE state='active'"
                    ).fetchone()[0],
                    "attempts": conn.execute(
                        "SELECT COUNT(*) FROM agent_dispatch_attempts"
                    ).fetchone()[0],
                }
            )
            assert snapshot == {
                "compatibility": ("queued", "compatibility"),
                "task": ("dispatching", "child"),
                "process": "busy",
                "active_slots": 1,
                "attempts": 1,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_abort_requeue_and_cleanup_are_fenced_replay_stable(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            task = await store.create_task(_task("child-abort"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "abort",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            with pytest.raises(StoreError, match="identity conflicts"):
                await store.commit_agent_dispatch_abort(
                    **{
                        **decision,
                        "claim_token": "wrong-claim",
                        "decision_code": "lease_expired",
                        "outcome_state": "queued",
                        "source_state": "queued",
                        "decided_at": T0 + timedelta(seconds=6),
                    }
                )

            retry_at = T0 + timedelta(seconds=9)
            aborted = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="lease_expired",
                outcome_state="queued",
                source_state="queued",
                next_attempt_at=retry_at,
                decided_at=T0 + timedelta(seconds=6),
            )
            assert aborted.decision_kind is DispatchDecisionKind.ABORT
            assert aborted.decision_code == "lease_expired"
            assert aborted.decision_outcome_state is InvocationState.QUEUED
            assert aborted.decision_next_attempt_at == retry_at
            assert aborted.decision_metadata_legacy is False
            assert aborted.cleanup_proven is False

            assert aborted.decision_source_state is DispatchSourceState.QUEUED
            assert await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="lease_expired",
                outcome_state="queued",
                source_state="queued",
                next_attempt_at=retry_at,
                decided_at=T0 + timedelta(seconds=6),
            ) == aborted
            with pytest.raises(StoreError, match="next_attempt_at"):
                await store.commit_agent_dispatch_abort(
                    **decision,
                    decision_code="lease_expired",
                    outcome_state="queued",
                    source_state="queued",
                    next_attempt_at=retry_at + timedelta(seconds=1),
                    decided_at=T0 + timedelta(seconds=6),
                )
            with pytest.raises(StoreError, match="next_attempt_at"):
                await store.commit_agent_dispatch_abort(
                    **decision,
                    decision_code="lease_expired",
                    outcome_state="queued",
                    source_state="queued",
                    decided_at=T0 + timedelta(seconds=6),
                )
            with pytest.raises(StoreError, match="decision replay conflicts"):
                await store.commit_agent_dispatch_rejection(
                    **decision,
                    decision_code="invalid_snapshot",
                    outcome_state="failed",
                    source_state="failed",
                    decided_at=T0 + timedelta(seconds=6, milliseconds=50),
                )
            state = await store._call(
                lambda conn: {
                    "task": tuple(
                        conn.execute(
                            "SELECT state,claim_token,next_attempt_at "
                            "FROM tasks WHERE task_id=?",
                            (task.task_id,),
                        ).fetchone()
                    ),
                    "execution": tuple(
                        conn.execute(
                            "SELECT state,dispatch_backend FROM task_executions "
                            "WHERE execution_id=?",
                            (task.execution_id,),
                        ).fetchone()
                    ),
                    "invocation": tuple(
                        conn.execute(
                            "SELECT state,dispatch_backend,ready_sequence "
                            "FROM agent_invocations WHERE invocation_id=?",
                            (task.execution_id,),
                        ).fetchone()
                    ),
                    "counter": conn.execute(
                        "SELECT unfinished_count FROM agent_admission_counters "
                        "WHERE agent_id='codex' AND agent_incarnation=1"
                    ).fetchone()[0],
                    "slot": conn.execute(
                        "SELECT state FROM agent_execution_slots WHERE slot_id=?",
                        (reservation.slot.slot_id,),
                    ).fetchone()[0],
                    "process": conn.execute(
                        "SELECT observed_state FROM agent_processes "
                        "WHERE agent_id='codex' AND worker_generation=1"
                    ).fetchone()[0],
                }
            )
            assert state == {
                "task": (
                    "queued",
                    None,
                    retry_at.isoformat(timespec="microseconds"),
                ),
                "execution": ("queued", "child"),
                "invocation": ("queued", "child", 1),
                "counter": 1,
                "slot": "released",
                "process": "busy",
            }

            current[0] = T0 + timedelta(seconds=7)
            proof = _cleanup_proof(reservation, epoch.epoch)
            cleaned = await store.record_agent_dispatch_cleanup(
                **decision,
                cleanup_proof=proof,
                recorded_at=current[0],
            )
            assert cleaned.cleanup_proven is True
            assert len(cleaned.cleanup_proof_hash or "") == 64
            assert await store.record_agent_dispatch_cleanup(
                **decision,
                cleanup_proof=proof,
                recorded_at=current[0],
            ) == cleaned
            conflicting = dict(proof)
            conflicting["checked_at"] = T0 + timedelta(
                seconds=6, milliseconds=400
            )
            with pytest.raises(StoreError, match="cleanup proof conflicts"):
                await store.record_agent_dispatch_cleanup(
                    **decision,
                    cleanup_proof=conflicting,
                    recorded_at=current[0],
                )
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_dispatch_attempts SET lease_identity='changed' "
                        "WHERE dispatch_attempt_id=?",
                        (reservation.attempt.dispatch_attempt_id,),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                await store._call(
                    lambda conn: conn.execute(
                        "DELETE FROM agent_dispatch_attempts "
                        "WHERE dispatch_attempt_id=?",
                        (reservation.attempt.dispatch_attempt_id,),
                    )
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_permanent_mailbox_rejection_releases_admission_once(tmp_path: Path) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="reject mailbox",
                request_id="reject-mailbox",
                message_id="reject-mailbox-message",
            )
            assert mailbox.current_invocation_id is not None
            await _tag_child_invocations(store, mailbox.current_invocation_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "reject",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None and reservation.mailbox is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            rejected = await store.commit_agent_dispatch_rejection(
                **decision,
                decision_code="unsupported_capability",
                outcome_state="failed",
                source_state="rejected",
                decided_at=T0 + timedelta(seconds=6),
            )
            assert rejected.decision_kind is DispatchDecisionKind.REJECTION
            assert rejected.decision_outcome_state is InvocationState.FAILED
            assert rejected.decision_source_state is DispatchSourceState.REJECTED
            assert await store.commit_agent_dispatch_rejection(
                **decision,
                decision_code="unsupported_capability",
                outcome_state="failed",
                source_state="rejected",
                decided_at=T0 + timedelta(seconds=6),
            ) == rejected
            with pytest.raises(StoreError, match="decision replay conflicts"):
                await store.commit_agent_dispatch_rejection(
                    **decision,
                    decision_code="different_late_code",
                    outcome_state="queued",
                    source_state="pending",
                    decided_at=T0 + timedelta(seconds=7),
                )
            snapshot = await store._call(
                lambda conn: {
                    "mailbox": conn.execute(
                        "SELECT state FROM agent_mailbox WHERE mailbox_id=?",
                        (mailbox.mailbox_id,),
                    ).fetchone()[0],
                    "invocation": tuple(
                        conn.execute(
                            "SELECT state,admission_released_at "
                            "FROM agent_invocations WHERE invocation_id=?",
                            (mailbox.current_invocation_id,),
                        ).fetchone()
                    ),
                    "agent_count": conn.execute(
                        "SELECT unfinished_count FROM agent_admission_counters "
                        "WHERE agent_id='codex' AND agent_incarnation=1"
                    ).fetchone()[0],
                    "global_count": conn.execute(
                        "SELECT unfinished_count "
                        "FROM global_agent_admission_counter WHERE singleton=1"
                    ).fetchone()[0],
                    "slot": conn.execute(
                        "SELECT state FROM agent_execution_slots WHERE slot_id=?",
                        (reservation.slot.slot_id,),
                    ).fetchone()[0],
                }
            )
            assert snapshot["mailbox"] == "rejected"
            assert snapshot["invocation"][0] == "failed"
            assert snapshot["invocation"][1] is not None
            assert snapshot["agent_count"] == 0
            assert snapshot["global_count"] == 0
            assert snapshot["slot"] == "released"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cancel_and_pregrant_decision_races_are_ordered_and_replay_exact(
    tmp_path: Path,
) -> None:
    async def cancellation_wins() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "cancel-first.sqlite"
        )
        try:
            task = await store.create_task(_task("cancel-first"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "cancel-first",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            assert await store.cancel_task(
                task.task_id,
                actor="race-test",
                now=T0 + timedelta(seconds=5, milliseconds=500),
            )

            losing_decisions = (
                (
                    store.commit_agent_dispatch_abort,
                    "temporary_failure",
                    "queued",
                    "queued",
                ),
                (
                    store.commit_agent_dispatch_abort,
                    "permanent_failure",
                    "failed",
                    "failed",
                ),
                (
                    store.commit_agent_dispatch_rejection,
                    "child_rejected",
                    "failed",
                    "failed",
                ),
            )
            for commit, code, outcome, source in losing_decisions:
                with pytest.raises(
                    InvalidTransition,
                    match="requires an abort-to-cancelled decision",
                ):
                    await commit(
                        **decision,
                        decision_code=code,
                        outcome_state=outcome,
                        source_state=source,
                        decided_at=T0 + timedelta(seconds=6),
                    )
            assert (
                await store.get_agent_dispatch_attempt(
                    reservation.attempt.dispatch_attempt_id
                )
            ).decision_kind is None

            cancelled = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="cancel_requested",
                outcome_state="cancelled",
                source_state="cancelled",
                decided_at=T0 + timedelta(seconds=6),
            )
            assert cancelled.decision_kind is DispatchDecisionKind.ABORT
            assert cancelled.decision_outcome_state is InvocationState.CANCELLED
            assert cancelled.decision_source_state is DispatchSourceState.CANCELLED
            assert await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="cancel_requested",
                outcome_state="cancelled",
                source_state="cancelled",
                decided_at=T0 + timedelta(seconds=6),
            ) == cancelled
            with pytest.raises(StoreError, match="decision replay conflicts"):
                await store.commit_agent_dispatch_abort(
                    **decision,
                    decision_code="permanent_failure",
                    outcome_state="failed",
                    source_state="failed",
                    decided_at=T0 + timedelta(seconds=6),
                )
            snapshot = await store._call(
                lambda conn: {
                    "task": conn.execute(
                        "SELECT state FROM tasks WHERE task_id=?",
                        (task.task_id,),
                    ).fetchone()[0],
                    "execution": conn.execute(
                        "SELECT state FROM task_executions WHERE execution_id=?",
                        (task.execution_id,),
                    ).fetchone()[0],
                    "invocation": conn.execute(
                        "SELECT state FROM agent_invocations WHERE invocation_id=?",
                        (task.execution_id,),
                    ).fetchone()[0],
                    "slot": conn.execute(
                        "SELECT state FROM agent_execution_slots WHERE slot_id=?",
                        (reservation.slot.slot_id,),
                    ).fetchone()[0],
                    "count": conn.execute(
                        "SELECT unfinished_count FROM global_agent_admission_counter"
                    ).fetchone()[0],
                }
            )
            assert snapshot == {
                "task": "cancelled",
                "execution": "cancelled",
                "invocation": "cancelled",
                "slot": "released",
                "count": 0,
            }
        finally:
            await store.close()

    async def requeue_wins_then_cancellation_terminalizes() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "decision-first.sqlite"
        )
        try:
            task = await store.create_task(_task("decision-first"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "decision-first",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            aborted = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="retry_pregrant",
                outcome_state="queued",
                source_state="queued",
                decided_at=T0 + timedelta(seconds=6),
            )
            assert await store.cancel_task(
                task.task_id,
                actor="race-test",
                now=T0 + timedelta(seconds=7),
            )
            retained = await store.get_agent_dispatch_attempt(
                reservation.attempt.dispatch_attempt_id
            )
            assert retained == aborted
            assert retained.decision_outcome_state is InvocationState.QUEUED
            assert await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="retry_pregrant",
                outcome_state="queued",
                source_state="queued",
                decided_at=T0 + timedelta(seconds=6),
            ) == aborted
            assert (await store.get_task(task.task_id)).state.value == "cancelled"
            assert (
                await store.get_global_agent_admission_counter()
            ).unfinished_count == 0
        finally:
            await store.close()

    asyncio.run(cancellation_wins())
    asyncio.run(requeue_wins_then_cancellation_terminalizes())


def test_uncancelled_task_cannot_commit_cancelled_pregrant_outcome(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            task = await store.create_task(_task("not-cancelled"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "not-cancelled",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            with pytest.raises(
                InvalidTransition,
                match="uncancelled dispatching work cannot become cancelled",
            ):
                await store.commit_agent_dispatch_abort(
                    **_decision_kwargs(reservation, epoch.epoch),
                    decision_code="spurious_cancel",
                    outcome_state="cancelled",
                    source_state="cancelled",
                    decided_at=T0 + timedelta(seconds=6),
                )
            assert (
                await store.get_agent_dispatch_attempt(
                    reservation.attempt.dispatch_attempt_id
                )
            ).decision_kind is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_reserved_mailbox_expiry_is_atomic_typed_and_replay_exact(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            expires_at = T0 + timedelta(seconds=6)
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="expires while reserved",
                request_id="reserved-expiry-request",
                message_id="reserved-expiry-message",
                expires_at=expires_at,
                now=T0 + timedelta(seconds=5),
            )
            assert mailbox.current_invocation_id is not None
            await _tag_child_invocations(store, mailbox.current_invocation_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "mailbox-expiry",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None and reservation.mailbox is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            with pytest.raises(
                InvalidTransition,
                match="cannot expire before expires_at",
            ):
                await store.commit_agent_dispatch_abort(
                    **decision,
                    decision_code="mailbox_expired",
                    outcome_state="failed",
                    source_state="expired",
                    decided_at=T0 + timedelta(
                        seconds=5, milliseconds=999
                    ),
                )
            undecided = await store.get_agent_dispatch_attempt(
                reservation.attempt.dispatch_attempt_id
            )
            assert undecided is not None and undecided.decision_kind is None
            assert (
                await store.get_active_agent_execution_slot("codex", 1)
            ) is not None

            expired = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="mailbox_expired",
                outcome_state="failed",
                source_state="expired",
                decided_at=expires_at,
            )
            assert expired.decision_kind is DispatchDecisionKind.ABORT
            assert expired.decision_outcome_state is InvocationState.FAILED
            assert expired.decision_source_state is DispatchSourceState.EXPIRED
            assert await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="mailbox_expired",
                outcome_state="failed",
                source_state="expired",
                decided_at=expires_at,
            ) == expired
            with pytest.raises(StoreError, match="decision replay conflicts"):
                await store.commit_agent_dispatch_rejection(
                    **decision,
                    decision_code="mailbox_rejected",
                    outcome_state="failed",
                    source_state="rejected",
                    decided_at=expires_at,
                )

            snapshot = await store._call(
                lambda conn: {
                    "mailbox": tuple(
                        conn.execute(
                            "SELECT state,processed_at,expires_at "
                            "FROM agent_mailbox WHERE mailbox_id=?",
                            (mailbox.mailbox_id,),
                        ).fetchone()
                    ),
                    "invocation": tuple(
                        conn.execute(
                            "SELECT state,admission_released_at,expires_at "
                            "FROM agent_invocations WHERE invocation_id=?",
                            (mailbox.current_invocation_id,),
                        ).fetchone()
                    ),
                    "slot": conn.execute(
                        "SELECT state FROM agent_execution_slots WHERE slot_id=?",
                        (reservation.slot.slot_id,),
                    ).fetchone()[0],
                    "count": conn.execute(
                        "SELECT unfinished_count FROM global_agent_admission_counter"
                    ).fetchone()[0],
                }
            )
            expiry_text = expires_at.isoformat(timespec="microseconds")
            assert snapshot["mailbox"] == (
                "expired",
                expiry_text,
                expiry_text,
            )
            assert snapshot["invocation"][0] == "failed"
            assert snapshot["invocation"][1] is not None
            assert snapshot["invocation"][2] == expiry_text
            assert snapshot["slot"] == "released"
            assert snapshot["count"] == 0
            assert (
                await store.reconcile(now=T0 + timedelta(seconds=7))
            ).mailbox_expired == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_process_stop_refuses_active_and_cleanup_unproven_dispatch(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            task = await store.create_task(_task("stop-fence"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "stop-fence",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            stop_kwargs = {
                "agent_id": "codex",
                "agent_incarnation": 1,
                "worker_generation": 1,
                "expected_supervisor_epoch": epoch.epoch,
                "fencing_supervisor_epoch": epoch.epoch,
                "owner_instance_id": "dispatch-owner",
                "process_lease_identity": "dispatch-process-lease",
                "reason": "dispatch_test_stop",
                "exit_code": 0,
            }
            with pytest.raises(InvalidTransition, match="active execution slot"):
                await store.fence_agent_process_stopped(
                    **stop_kwargs,
                    cleanup_proof=_process_cleanup_proof(
                        epoch.epoch,
                        checked_at=T0 + timedelta(seconds=6),
                    ),
                    stopped_at=T0 + timedelta(seconds=7),
                )

            decision = _decision_kwargs(reservation, epoch.epoch)
            failed = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="dispatch_failed",
                outcome_state="failed",
                source_state="failed",
                decided_at=T0 + timedelta(seconds=6),
            )
            assert not failed.cleanup_proven
            with pytest.raises(
                InvalidTransition,
                match="before dispatch cleanup is proven",
            ):
                await store.fence_agent_process_stopped(
                    **stop_kwargs,
                    cleanup_proof=_process_cleanup_proof(
                        epoch.epoch,
                        checked_at=T0 + timedelta(seconds=7),
                    ),
                    stopped_at=T0 + timedelta(seconds=8),
                )
            assert (
                await store.get_agent_process("codex", 1, 1)
            ).observed_state is AgentProcessState.BUSY
            assert (
                await store.get_global_agent_admission_counter()
            ).unfinished_count == 0
            assert (
                await store.get_agent_execution_slot(reservation.slot.slot_id)
            ).state.value == "released"

            current[0] = T0 + timedelta(seconds=7)
            await store.record_agent_dispatch_cleanup(
                **decision,
                cleanup_proof=_cleanup_proof(reservation, epoch.epoch),
                recorded_at=current[0],
            )
            stopped = await store.fence_agent_process_stopped(
                **stop_kwargs,
                cleanup_proof=_process_cleanup_proof(
                    epoch.epoch,
                    checked_at=T0 + timedelta(seconds=8),
                ),
                stopped_at=T0 + timedelta(seconds=9),
            )
            assert stopped.observed_state is AgentProcessState.STOPPED
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v31_migrates_existing_decision_as_readable_legacy_and_fails_replay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            task = await store.create_task(_task("v30-legacy-timing"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "v30-legacy-timing",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            retry_at = T0 + timedelta(seconds=9)
            committed = await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="legacy_retry",
                outcome_state="queued",
                source_state="queued",
                next_attempt_at=retry_at,
                decided_at=T0 + timedelta(seconds=6),
            )
            assert committed.decision_next_attempt_at == retry_at
            assert not committed.decision_metadata_legacy

            def migrate_from_v30(conn: sqlite3.Connection) -> None:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for trigger in (
                        "trg_agent_dispatch_attempts_insert_valid",
                        "trg_agent_dispatch_attempts_update_valid",
                        "trg_agent_dispatch_attempts_immutable",
                        "trg_agent_dispatch_attempts_no_delete",
                    ):
                        conn.execute(f'DROP TRIGGER "{trigger}"')
                    conn.execute(
                        "ALTER TABLE agent_dispatch_attempts "
                        "DROP COLUMN decision_next_attempt_at"
                    )
                    conn.execute(
                        "ALTER TABLE agent_dispatch_attempts "
                        "DROP COLUMN decision_metadata_legacy"
                    )
                    conn.execute(
                        "DELETE FROM schema_migrations WHERE version=31"
                    )
                    store._apply_schema_v31_dispatch_decisions_tx(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version,applied_at) "
                        "VALUES (31,?)",
                        ((T0 + timedelta(seconds=7)).isoformat(
                            timespec="microseconds"
                        ),),
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise

            await store._call(migrate_from_v30)
            retained = await store.get_agent_dispatch_attempt(
                reservation.attempt.dispatch_attempt_id
            )
            assert retained is not None
            assert retained.decision_metadata_legacy
            assert retained.decision_next_attempt_at is None
            assert retained.decision_kind is DispatchDecisionKind.ABORT
            with pytest.raises(StoreError, match="cannot be replayed exactly"):
                await store.commit_agent_dispatch_abort(
                    **decision,
                    decision_code="legacy_retry",
                    outcome_state="queued",
                    source_state="queued",
                    next_attempt_at=retry_at,
                    decided_at=T0 + timedelta(seconds=6),
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v31_dispatch_triggers_reject_legacy_minting_and_invalid_decisions(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch, _current = await _owned_ready_store(
            tmp_path / "runtime.sqlite"
        )
        try:
            task = await store.create_task(_task("v31-trigger-fences"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "v31-trigger-fences",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            attempt_id = reservation.attempt.dispatch_attempt_id
            decided_text = (T0 + timedelta(seconds=6)).isoformat(
                timespec="microseconds"
            )
            retry_text = (T0 + timedelta(seconds=9)).isoformat(
                timespec="microseconds"
            )

            with pytest.raises(sqlite3.IntegrityError, match="invalid dispatch"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_dispatch_attempts SET abort_committed_at=? "
                        "WHERE dispatch_attempt_id=?",
                        (decided_text, attempt_id),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="immutable|invalid"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_dispatch_attempts "
                        "SET decision_metadata_legacy=1 "
                        "WHERE dispatch_attempt_id=?",
                        (attempt_id,),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="invalid dispatch"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_dispatch_attempts SET abort_committed_at=?,"
                        "decision_code='bad_matrix',"
                        "decision_outcome_state='failed',"
                        "decision_source_state='rejected' "
                        "WHERE dispatch_attempt_id=?",
                        (decided_text, attempt_id),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="invalid dispatch"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_dispatch_attempts SET abort_committed_at=?,"
                        "decision_code='bad_timing',"
                        "decision_outcome_state='failed',"
                        "decision_source_state='failed',"
                        "decision_next_attempt_at=? "
                        "WHERE dispatch_attempt_id=?",
                        (decided_text, retry_text, attempt_id),
                    )
                )

            await store._call(
                lambda conn: conn.execute(
                    """INSERT INTO agent_execution_slots (
                           slot_id,agent_id,agent_incarnation,slot_sequence,
                           slot_kind,state,dispatch_backend,invocation_id,
                           worker_generation,acquired_at,released_at)
                       VALUES (?,?,1,2,'invocation','released','child',?,1,?,?)""",
                    (
                        "dispatch-slot-forged-legacy",
                        "codex",
                        reservation.invocation.invocation_id,
                        decided_text,
                        decided_text,
                    ),
                )
            )
            with pytest.raises(sqlite3.IntegrityError, match="invalid dispatch"):
                await store._call(
                    lambda conn: conn.execute(
                        """INSERT INTO agent_dispatch_attempts (
                               dispatch_attempt_id,invocation_id,slot_id,agent_id,
                               agent_incarnation,worker_generation,supervisor_epoch,
                               dispatch_backend,claim_token_hash,lease_identity,
                               lease_expires_at,invocation_job_identity,created_at,
                               decision_metadata_legacy)
                           VALUES (?,?,?,'codex',1,1,?,'child',?,?,?,?,?,1)""",
                        (
                            "dispatch-attempt-forged-legacy",
                            reservation.invocation.invocation_id,
                            "dispatch-slot-forged-legacy",
                            epoch.epoch,
                            _digest("forged-claim"),
                            "forged-legacy-lease",
                            (T0 + timedelta(minutes=5)).isoformat(
                                timespec="microseconds"
                            ),
                            "forged-legacy-job",
                            decided_text,
                        ),
                    )
                )
            assert (
                await store.get_agent_dispatch_attempt(attempt_id)
            ).decision_kind is None
            with pytest.raises(ValueError, match="queued outcome"):
                await store.commit_agent_dispatch_abort(
                    **_decision_kwargs(reservation, epoch.epoch),
                    decision_code="bad_timing",
                    outcome_state="failed",
                    source_state="failed",
                    next_attempt_at=T0 + timedelta(seconds=9),
                    decided_at=T0 + timedelta(seconds=6),
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v28_to_v31_backfills_typed_mailbox_abort_rejection_source(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store, epoch, _current = await _owned_ready_store(path)
        attempt_id = ""
        try:
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="legacy typed rejection",
                request_id="v28-rejection-request",
                message_id="v28-rejection-message",
            )
            assert mailbox.current_invocation_id is not None
            await _tag_child_invocations(store, mailbox.current_invocation_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "v28-rejection",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            attempt_id = reservation.attempt.dispatch_attempt_id
            await store.commit_agent_dispatch_abort(
                **_decision_kwargs(reservation, epoch.epoch),
                decision_code="unsupported_capability",
                outcome_state="failed",
                source_state="rejected",
                decided_at=T0 + timedelta(seconds=6),
            )
        finally:
            await store.close()

        with sqlite3.connect(path) as connection:
            for trigger in (
                "trg_agent_dispatch_attempts_insert_valid",
                "trg_agent_dispatch_attempts_update_valid",
                "trg_agent_dispatch_attempts_immutable",
                "trg_agent_dispatch_attempts_no_delete",
            ):
                connection.execute(f"DROP TRIGGER {trigger}")
            connection.execute(
                "ALTER TABLE agent_dispatch_attempts "
                "DROP COLUMN decision_source_state"
            )
            connection.execute(
                "ALTER TABLE agent_dispatch_attempts "
                "DROP COLUMN decision_next_attempt_at"
            )
            connection.execute(
                "ALTER TABLE agent_dispatch_attempts "
                "DROP COLUMN decision_metadata_legacy"
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (29, 30, 31, 32, 33, 34, 35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize(recover_startup_state=False)
        try:
            retained = await migrated._call(
                lambda conn: migrated._agent_dispatch_attempt_from_row(
                    conn.execute(
                        "SELECT * FROM agent_dispatch_attempts "
                        "WHERE dispatch_attempt_id=?",
                        (attempt_id,),
                    ).fetchone()
                ),
                allow_deferred_startup=True,
            )
            assert retained is not None
            assert retained.decision_kind is DispatchDecisionKind.ABORT
            assert retained.decision_outcome_state is InvocationState.FAILED
            assert retained.decision_source_state is DispatchSourceState.REJECTED
            assert retained.decision_metadata_legacy
            assert retained.decision_next_attempt_at is None
            assert await migrated._call(
                lambda conn: conn.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                allow_deferred_startup=True,
            ) == 35
        finally:
            await migrated.close()

    asyncio.run(scenario())


def test_v26_to_v31_retains_nullable_legacy_decision_metadata(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store, epoch, current = await _owned_ready_store(path)
        reservation = None
        try:
            task = await store.create_task(_task("legacy-v26-attempt"))
            await _tag_child_invocations(store, task.execution_id)
            reservation = await store.reserve_next_agent_invocation(
                **_reserve_kwargs(
                    epoch.epoch,
                    "legacy",
                    now=T0 + timedelta(seconds=5),
                )
            )
            assert reservation is not None
            decision = _decision_kwargs(reservation, epoch.epoch)
            await store.commit_agent_dispatch_abort(
                **decision,
                decision_code="legacy_abort",
                outcome_state="queued",
                source_state="queued",
                decided_at=T0 + timedelta(seconds=6),
            )
            current[0] = T0 + timedelta(seconds=7)
            await store.record_agent_dispatch_cleanup(
                **decision,
                cleanup_proof=_cleanup_proof(reservation, epoch.epoch),
                recorded_at=current[0],
            )
        finally:
            await store.close()
        assert reservation is not None

        with sqlite3.connect(path) as connection:
            for trigger in (
                "trg_agent_dispatch_attempts_insert_valid",
                "trg_agent_dispatch_attempts_update_valid",
                "trg_agent_dispatch_attempts_immutable",
                "trg_agent_dispatch_attempts_no_delete",
            ):
                connection.execute(f"DROP TRIGGER {trigger}")
            for column in (
                "decision_metadata_legacy",
                "decision_next_attempt_at",
                "decision_source_state",
                "cleanup_proof_hash",
                "decision_outcome_state",
                "decision_code",
            ):
                connection.execute(
                    f"ALTER TABLE agent_dispatch_attempts DROP COLUMN {column}"
                )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (27, 28, 29, 30, 31, 32, 33, 34, 35)"
            )
            connection.commit()
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0] == 26

        migrated = SQLiteStore(path)
        await migrated.initialize(recover_startup_state=False)
        await migrated.close()
        with sqlite3.connect(path) as connection:
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(agent_dispatch_attempts)"
                ).fetchall()
            }
            row = connection.execute(
                "SELECT abort_committed_at,decision_code,"
                "decision_outcome_state,decision_source_state,"
                "decision_next_attempt_at,decision_metadata_legacy,"
                "runtime_stopped_at,invocation_job_empty_at,cleanup_proof_hash "
                "FROM agent_dispatch_attempts WHERE dispatch_attempt_id=?",
                (reservation.attempt.dispatch_attempt_id,),
            ).fetchone()
            triggers = {
                str(item[0])
                for item in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger' "
                    "AND name LIKE 'trg_agent_dispatch_attempts_%'"
                ).fetchall()
            }
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0] == 35
            assert {
                "decision_code",
                "decision_outcome_state",
                "decision_source_state",
                "decision_next_attempt_at",
                "decision_metadata_legacy",
                "cleanup_proof_hash",
            }.issubset(columns)
            assert row is not None
            assert row[0] is not None
            assert row[1] is None and row[2] is None and row[3] is None
            assert row[4] is None and row[5] == 1
            assert row[6] is not None and row[7] is not None
            assert row[8] is None
            assert {
                "trg_agent_dispatch_attempts_insert_valid",
                "trg_agent_dispatch_attempts_update_valid",
                "trg_agent_dispatch_attempts_immutable",
                "trg_agent_dispatch_attempts_no_delete",
            }.issubset(triggers)

    asyncio.run(scenario())
