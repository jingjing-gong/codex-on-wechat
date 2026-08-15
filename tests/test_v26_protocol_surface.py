"""Typed read surface for v26 foundations and v27 decision evidence."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.models import (
    DispatchBackend,
    ExecutionSlotKind,
    ExecutionSlotState,
)
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import DurableStore


T0 = datetime(2026, 8, 15, 2, 0, tzinfo=timezone.utc)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _task() -> dict[str, object]:
    return {
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
        "inputs": {"text": "hello"},
    }


async def _ready_owned_store(path: Path) -> tuple[SQLiteStore, int]:
    store = SQLiteStore(path)
    await store.initialize(recover_startup_state=False)
    epoch = await store.activate_supervisor_epoch(
        owner_instance_id="surface-owner",
        channel="wechat",
        bot_id="bot",
        started_at=T0,
    )
    generation_capability = "surface-generation-secret"
    process_lease_token = "surface-process-lease-secret"
    await store.begin_agent_process_generation(
        agent_id="codex",
        agent_incarnation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="surface-owner",
        generation_capability=generation_capability,
        lifetime_lock_identity="surface-lock:codex:1",
        lifetime_lock_acquired_at=T0 + timedelta(seconds=1),
        process_lease_identity="surface-process-lease",
        process_lease_token=process_lease_token,
        lease_expires_at=T0 + timedelta(minutes=10),
        started_at=T0 + timedelta(seconds=2),
    )
    await store.commit_agent_process_handshake(
        agent_id="codex",
        agent_incarnation=1,
        worker_generation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="surface-owner",
        generation_capability=generation_capability,
        process_lease_identity="surface-process-lease",
        process_lease_token=process_lease_token,
        pid=701,
        process_group_id=701,
        kernel_process_birth_id="linux-birth-701",
        hello_frame_id="surface-hello",
        hello_payload_hash=_digest("hello"),
        capabilities_frame_id="surface-capabilities",
        capabilities_payload_hash=_digest("capabilities"),
        capability_snapshot_hash=_digest("snapshot"),
        committed_at=T0 + timedelta(seconds=3),
    )
    await store.commit_agent_process_ready(
        agent_id="codex",
        agent_incarnation=1,
        worker_generation=1,
        supervisor_epoch=epoch.epoch,
        owner_instance_id="surface-owner",
        generation_capability=generation_capability,
        process_lease_identity="surface-process-lease",
        process_lease_token=process_lease_token,
        ready_frame_id="surface-ready",
        ready_payload_hash=_digest("ready"),
        ready_at=T0 + timedelta(seconds=4),
    )
    return store, epoch.epoch


def test_admission_counter_accessors_are_typed_and_protocol_visible(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            assert isinstance(store, DurableStore)
            task = await store.create_task(_task())

            counter = await store.get_agent_admission_counter("codex", 1)
            assert counter is not None
            assert counter.unfinished_count == 1
            assert counter.next_ready_sequence == 2
            assert await store.list_agent_admission_counters(
                agent_id="codex", agent_incarnation=1
            ) == [counter]

            global_counter = await store.get_global_agent_admission_counter()
            assert global_counter is not None
            assert global_counter.singleton == 1
            assert global_counter.unfinished_count == 1
            assert (await store.get_agent_invocation(task.execution_id)) is not None

            assert await store.get_agent_admission_counter("missing", 1) is None
            with pytest.raises(
                ValueError,
                match="agent_id is required with agent_incarnation",
            ):
                await store.list_agent_admission_counters(agent_incarnation=1)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_slot_and_dispatch_attempt_accessors_preserve_fences_and_cleanup(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store, epoch = await _ready_owned_store(tmp_path / "runtime.sqlite")
        try:
            task = await store.create_task(_task())
            invocation_id = task.execution_id
            lease_expires_at = T0 + timedelta(minutes=5)
            first_created_at = T0 + timedelta(seconds=5)
            cleanup_at = T0 + timedelta(seconds=6)
            second_created_at = T0 + timedelta(seconds=7)

            def seed(conn) -> None:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "UPDATE agent_processes SET observed_state='busy' "
                        "WHERE agent_id='codex' AND agent_incarnation=1 "
                        "AND worker_generation=1"
                    )
                    conn.execute(
                        "UPDATE tasks SET state='dispatching',claimed_by=?,"
                        "claim_token=?,lease_expires_at=?,attempts=1 "
                        "WHERE task_id=?",
                        (
                            "surface-dispatcher",
                            "surface-claim-2",
                            lease_expires_at.isoformat(),
                            task.task_id,
                        ),
                    )
                    conn.execute(
                        "UPDATE task_executions SET state='dispatching',"
                        "dispatch_backend='child',worker_id=?,claim_token=?,"
                        "lease_expires_at=? WHERE execution_id=?",
                        (
                            "surface-dispatcher",
                            "surface-claim-2",
                            lease_expires_at.isoformat(),
                            invocation_id,
                        ),
                    )
                    conn.execute(
                        "UPDATE agent_invocations SET state='dispatching',"
                        "dispatch_backend='child',claimed_by=?,claim_token=?,"
                        "lease_expires_at=?,updated_at=? WHERE invocation_id=?",
                        (
                            "surface-dispatcher",
                            "surface-claim-2",
                            lease_expires_at.isoformat(),
                            second_created_at.isoformat(),
                            invocation_id,
                        ),
                    )
                    conn.execute(
                        "INSERT INTO agent_execution_slots "
                        "(slot_id,agent_id,agent_incarnation,slot_sequence,"
                        "slot_kind,state,dispatch_backend,invocation_id,"
                        "worker_generation,acquired_at,released_at) "
                        "VALUES (?,?,?,?,?,'released','child',?,?,?,?)",
                        (
                            "surface-slot-1",
                            "codex",
                            1,
                            1,
                            "invocation",
                            invocation_id,
                            1,
                            first_created_at.isoformat(),
                            cleanup_at.isoformat(),
                        ),
                    )
                    conn.execute(
                        "INSERT INTO agent_dispatch_attempts "
                        "(dispatch_attempt_id,invocation_id,slot_id,agent_id,"
                        "agent_incarnation,worker_generation,supervisor_epoch,"
                        "dispatch_backend,claim_token_hash,lease_identity,"
                        "lease_expires_at,invocation_job_identity,"
                        "abort_committed_at,decision_code,"
                        "decision_outcome_state,decision_source_state,"
                        "runtime_stopped_at,"
                        "invocation_job_empty_at,cleanup_proof_hash,created_at) "
                        "VALUES (?,?,?,?,?,?,?,'child',?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            "surface-attempt-1",
                            invocation_id,
                            "surface-slot-1",
                            "codex",
                            1,
                            1,
                            epoch,
                            _digest("surface-claim-1"),
                            "surface-lease-1",
                            lease_expires_at.isoformat(),
                            "surface-job-1",
                            cleanup_at.isoformat(),
                            "test_abort",
                            "queued",
                            "queued",
                            cleanup_at.isoformat(),
                            cleanup_at.isoformat(),
                            _digest("surface-cleanup-proof"),
                            first_created_at.isoformat(),
                        ),
                    )
                    conn.execute(
                        "INSERT INTO agent_execution_slots "
                        "(slot_id,agent_id,agent_incarnation,slot_sequence,"
                        "slot_kind,state,dispatch_backend,invocation_id,"
                        "worker_generation,acquired_at) "
                        "VALUES (?,?,?,?,?,'active','child',?,?,?)",
                        (
                            "surface-slot-2",
                            "codex",
                            1,
                            2,
                            "invocation",
                            invocation_id,
                            1,
                            second_created_at.isoformat(),
                        ),
                    )
                    conn.execute(
                        "INSERT INTO agent_dispatch_attempts "
                        "(dispatch_attempt_id,invocation_id,slot_id,agent_id,"
                        "agent_incarnation,worker_generation,supervisor_epoch,"
                        "dispatch_backend,claim_token_hash,lease_identity,"
                        "lease_expires_at,invocation_job_identity,created_at) "
                        "VALUES (?,?,?,?,?,?,?,'child',?,?,?,?,?)",
                        (
                            "surface-attempt-2",
                            invocation_id,
                            "surface-slot-2",
                            "codex",
                            1,
                            1,
                            epoch,
                            _digest("surface-claim-2"),
                            "surface-lease-2",
                            lease_expires_at.isoformat(),
                            "surface-job-2",
                            second_created_at.isoformat(),
                        ),
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise

            await store._call(seed)

            active = await store.get_active_agent_execution_slot("codex", 1)
            assert active is not None
            assert active.slot_id == "surface-slot-2"
            assert active.slot_kind is ExecutionSlotKind.INVOCATION
            assert active.state is ExecutionSlotState.ACTIVE
            assert active.dispatch_backend is DispatchBackend.CHILD
            assert active.invocation_id == invocation_id
            assert active.worker_generation == 1

            released = await store.get_agent_execution_slot("surface-slot-1")
            assert released is not None
            assert released.state is ExecutionSlotState.RELEASED
            assert released.released_at == cleanup_at
            assert await store.list_agent_execution_slots(
                agent_id="codex",
                agent_incarnation=1,
                state=ExecutionSlotState.ACTIVE,
                slot_kind=ExecutionSlotKind.INVOCATION,
                worker_generation=1,
            ) == [active]

            first_attempt = await store.get_agent_dispatch_attempt(
                "surface-attempt-1"
            )
            assert first_attempt is not None
            assert first_attempt.dispatch_backend is DispatchBackend.CHILD
            assert first_attempt.runtime_stopped_at == cleanup_at
            assert first_attempt.invocation_job_empty_at == cleanup_at
            attempts = await store.list_agent_dispatch_attempts(
                invocation_id=invocation_id,
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=1,
                supervisor_epoch=epoch,
            )
            assert [attempt.dispatch_attempt_id for attempt in attempts] == [
                "surface-attempt-1",
                "surface-attempt-2",
            ]
            assert attempts[1].runtime_stopped_at is None
            assert await store.get_agent_dispatch_attempt("missing") is None
        finally:
            await store.close()

    asyncio.run(scenario())
