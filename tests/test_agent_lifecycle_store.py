"""Agent lifecycle ledger and child-generation store regressions."""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.models import AgentLifecycleState, AgentProcessState
from src.runtime.policy import AgentProfile
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError


T0 = datetime(2026, 8, 15, 1, 0, tzinfo=timezone.utc)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _owned_store(path: Path, owner: str, *, started_at: datetime = T0):
    store = SQLiteStore(path)
    await store.initialize(recover_startup_state=False)
    epoch = await store.activate_supervisor_epoch(
        owner_instance_id=owner,
        channel="wechat",
        bot_id="bot",
        started_at=started_at,
    )
    return store, epoch


def _begin_kwargs(epoch: int, owner: str, *, identity: str, offset: int = 0):
    return {
        "agent_id": "codex",
        "agent_incarnation": 1,
        "supervisor_epoch": epoch,
        "owner_instance_id": owner,
        "generation_capability": f"generation-secret-{identity}",
        "lifetime_lock_identity": "agent-lock:codex:1",
        "lifetime_lock_acquired_at": T0 + timedelta(seconds=1 + offset),
        "process_lease_identity": identity,
        "process_lease_token": f"lease-secret-{identity}",
        "lease_expires_at": T0 + timedelta(minutes=10 + offset),
        "started_at": T0 + timedelta(seconds=2 + offset),
    }


def _cleanup_proof(
    process,
    *,
    checked_at: datetime,
    fencing_epoch: int | None = None,
    never_spawned: bool = False,
):
    proof = {
        "proof_kind": "never_spawned_v1" if never_spawned else "verified_empty_v1",
        "agent_id": process.agent_id,
        "agent_incarnation": process.agent_incarnation,
        "worker_generation": process.worker_generation,
        "supervisor_epoch": process.supervisor_epoch,
        "fencing_supervisor_epoch": (
            process.supervisor_epoch if fencing_epoch is None else fencing_epoch
        ),
        "process_lease_identity": process.process_lease_identity,
        "lifetime_lock_identity": process.lifetime_lock_identity,
        "process_group_id": process.process_group_id,
        "kernel_process_birth_id": process.kernel_process_birth_id,
        "runtime_stopped": True,
        "process_group_empty": True,
        "invocation_jobs_empty": True,
        "lifetime_lock_released": True,
        "checked_at": checked_at,
    }
    if never_spawned:
        proof["spawn_attempted"] = False
    return proof


def test_migrations_24_and_25_backfill_lifecycle_and_seed_events(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.put_profile(
            AgentProfile(agent_id="disabled", profile_version=1, enabled=True)
        )
        await seeded.put_profile(
            AgentProfile(agent_id="disabled", profile_version=2, enabled=False)
        )
        await seeded.put_profile(
            AgentProfile(agent_id="deleted-but-enabled", profile_version=1)
        )
        await seeded.mark_agent_deleted("deleted-but-enabled")
        await seeded.close()

        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP TRIGGER IF EXISTS trg_agent_lifecycle_events_no_update"
            )
            connection.execute(
                "DROP TRIGGER IF EXISTS trg_agent_lifecycle_events_no_delete"
            )
            connection.execute("DROP TABLE agent_lifecycle_events")
            connection.execute("DROP TABLE agent_processes")
            connection.execute("DROP TABLE agent_lifecycle")
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (24, 25, 26, 27, 28, 29, 30, 31, 32)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            disabled = await migrated.get_agent_lifecycle("disabled")
            deleted = await migrated.get_agent_lifecycle("deleted-but-enabled")
            assert disabled is not None and deleted is not None
            assert disabled.agent_incarnation == 1
            assert disabled.profile_version == 2
            assert disabled.lifecycle_state is AgentLifecycleState.DISABLED
            assert disabled.provenance["source_profile_version"] == 2
            assert deleted.lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert deleted.tombstoned_at is not None
            assert await migrated.list_agent_processes() == []
            disabled_events = await migrated.list_agent_lifecycle_events(
                agent_id="disabled"
            )
            assert [event.event_kind for event in disabled_events] == [
                "migration_seed"
            ]
            assert disabled_events[0].created_at == disabled.updated_at
        finally:
            await migrated.close()

        # Replaying a fully landed migration marker is exact and additive.
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (24, 25, 26, 27, 28, 29, 30, 31, 32)"
            )
            connection.commit()
        replayed = SQLiteStore(path)
        await replayed.initialize()
        await replayed.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (32,)
            assert connection.execute(
                "SELECT COUNT(*) FROM agent_lifecycle WHERE agent_id='disabled'"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT COUNT(*) FROM agent_lifecycle_events "
                "WHERE agent_id='disabled'"
            ).fetchone() == (1,)
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    asyncio.run(scenario())


def test_process_begin_handshake_ready_stop_and_next_generation(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store, epoch = await _owned_store(path, "owner-1")
        try:
            begin = _begin_kwargs(epoch.epoch, "owner-1", identity="start-1")
            process = await store.begin_agent_process_generation(**begin)
            assert process.worker_generation == 1
            assert process.observed_state is AgentProcessState.STARTING
            assert await store.begin_agent_process_generation(**begin) == process

            handshake = dict(
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=1,
                supervisor_epoch=epoch.epoch,
                owner_instance_id="owner-1",
                generation_capability=begin["generation_capability"],
                process_lease_identity="start-1",
                process_lease_token=begin["process_lease_token"],
                pid=501,
                process_group_id=501,
                kernel_process_birth_id="linux-birth-501",
                hello_frame_id="hello-1",
                hello_payload_hash=_digest("hello"),
                capabilities_frame_id="capabilities-1",
                capabilities_payload_hash=_digest("capabilities"),
                capability_snapshot_hash=_digest("snapshot"),
                committed_at=T0 + timedelta(seconds=3),
            )
            handshaken = await store.commit_agent_process_handshake(**handshake)
            assert handshaken.observed_state is AgentProcessState.STARTING
            assert await store.commit_agent_process_handshake(**handshake) == handshaken

            ready_args = dict(
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=1,
                supervisor_epoch=epoch.epoch,
                owner_instance_id="owner-1",
                generation_capability=begin["generation_capability"],
                process_lease_identity="start-1",
                process_lease_token=begin["process_lease_token"],
                ready_frame_id="ready-1",
                ready_payload_hash=_digest("ready"),
                ready_at=T0 + timedelta(seconds=4),
            )
            ready = await store.commit_agent_process_ready(**ready_args)
            assert ready.observed_state is AgentProcessState.READY
            assert ready.ready_handshake_committed
            assert await store.commit_agent_process_ready(**ready_args) == ready
            with pytest.raises(StoreError, match="READY identity conflicts"):
                await store.commit_agent_process_ready(
                    **{**ready_args, "ready_payload_hash": _digest("different")}
                )
            with pytest.raises(InvalidTransition, match="non-stopped"):
                await store.begin_agent_process_generation(
                    **_begin_kwargs(epoch.epoch, "owner-1", identity="start-2")
                )

            proof = _cleanup_proof(
                ready,
                checked_at=T0 + timedelta(seconds=5),
            )
            with pytest.raises(ValueError, match="does not match cleanup_proof"):
                await store.fence_agent_process_stopped(
                    agent_id="codex",
                    agent_incarnation=1,
                    worker_generation=1,
                    expected_supervisor_epoch=epoch.epoch,
                    fencing_supervisor_epoch=epoch.epoch,
                    owner_instance_id="owner-1",
                    process_lease_identity="start-1",
                    cleanup_proof=proof,
                    cleanup_proof_hash="0" * 64,
                    reason="graceful_test_stop",
                    stopped_at=T0 + timedelta(seconds=6),
                    exit_code=0,
                )
            stopped = await store.fence_agent_process_stopped(
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=1,
                expected_supervisor_epoch=epoch.epoch,
                fencing_supervisor_epoch=epoch.epoch,
                owner_instance_id="owner-1",
                process_lease_identity="start-1",
                cleanup_proof=proof,
                reason="graceful_test_stop",
                stopped_at=T0 + timedelta(seconds=6),
                exit_code=0,
            )
            assert stopped.observed_state is AgentProcessState.STOPPED
            assert stopped.lease_ended_at == T0 + timedelta(seconds=6)
            # Lost HELLO/CAPABILITIES/READY acknowledgements remain exact
            # history after stop; replay must not revive the generation.
            assert await store.commit_agent_process_handshake(**handshake) == stopped
            assert await store.commit_agent_process_ready(**ready_args) == stopped
            # A delayed begin replay returns history and cannot allocate anew.
            assert await store.begin_agent_process_generation(**begin) == stopped

            second = await store.begin_agent_process_generation(
                **_begin_kwargs(
                    epoch.epoch,
                    "owner-1",
                    identity="start-2",
                    offset=10,
                )
            )
            assert second.worker_generation == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_replacement_epoch_can_fence_only_with_cleanup_proof(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first, epoch1 = await _owned_store(path, "owner-1")
        begin = _begin_kwargs(epoch1.epoch, "owner-1", identity="unspawned")
        process = await first.begin_agent_process_generation(**begin)
        await first.close()

        replacement, epoch2 = await _owned_store(
            path,
            "owner-2",
            started_at=T0 + timedelta(seconds=20),
        )
        try:
            incomplete = _cleanup_proof(
                process,
                checked_at=T0 + timedelta(seconds=21),
                fencing_epoch=epoch2.epoch,
                never_spawned=True,
            )
            incomplete.pop("lifetime_lock_released")
            with pytest.raises(
                ValueError, match="lifetime_lock_released must be true"
            ):
                await replacement.fence_agent_process_stopped(
                    agent_id="codex",
                    agent_incarnation=1,
                    worker_generation=1,
                    expected_supervisor_epoch=epoch1.epoch,
                    fencing_supervisor_epoch=epoch2.epoch,
                    owner_instance_id="owner-2",
                    process_lease_identity="unspawned",
                    cleanup_proof=incomplete,
                    reason="takeover",
                    stopped_at=T0 + timedelta(seconds=22),
                )
            assert (await replacement.get_agent_process("codex", 1, 1)).active

            proof = _cleanup_proof(
                process,
                checked_at=T0 + timedelta(seconds=21),
                fencing_epoch=epoch2.epoch,
                never_spawned=True,
            )
            stopped = await replacement.fence_agent_process_stopped(
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=1,
                expected_supervisor_epoch=epoch1.epoch,
                fencing_supervisor_epoch=epoch2.epoch,
                owner_instance_id="owner-2",
                process_lease_identity="unspawned",
                cleanup_proof=proof,
                reason="takeover",
                stopped_at=T0 + timedelta(seconds=22),
            )
            assert stopped.supervisor_epoch == epoch1.epoch
            assert stopped.stopped_by_supervisor_epoch == epoch2.epoch
        finally:
            await replacement.close()

        third, epoch3 = await _owned_store(
            path,
            "owner-3",
            started_at=T0 + timedelta(seconds=30),
        )
        try:
            with pytest.raises(StoreError, match="predates the fencing epoch"):
                await third.fence_agent_process_stopped(
                    agent_id="codex",
                    agent_incarnation=1,
                    worker_generation=1,
                    expected_supervisor_epoch=epoch1.epoch,
                    fencing_supervisor_epoch=epoch3.epoch,
                    owner_instance_id="owner-3",
                    process_lease_identity="unspawned",
                    cleanup_proof=proof,
                    reason="stale_takeover_proof",
                    stopped_at=T0 + timedelta(seconds=31),
                )
        finally:
            await third.close()

    asyncio.run(scenario())
