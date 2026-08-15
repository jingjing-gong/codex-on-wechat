"""Supervisor replacement recovery for persisted Agent generations."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import signal
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import src.runtime.process_lifetime as process_lifetime
from src.runtime.models import (
    AgentProcessRecord,
    AgentProcessState,
    datetime_to_text,
    json_dumps,
)
from src.runtime.process_lifetime import (
    InheritedLifetimeLock,
    RecoveredLifetimeLock,
    capture_linux_process_birth,
)
from src.runtime.process_registry import (
    AgentProcessRecoveryError,
    AgentProcessRecoveryEvidenceError,
    AgentProcessRecoveryOwner,
    recover_agent_process_generation,
)
from src.runtime.sqlite_store import SQLiteStore


NOW = datetime(2026, 8, 15, 3, 0, tzinfo=timezone.utc)


def _record(
    lock: InheritedLifetimeLock,
    process: subprocess.Popen[Any],
    *,
    start_ticks_delta: int = 0,
    ready: bool = False,
) -> AgentProcessRecord:
    birth = capture_linux_process_birth(process.pid)
    return AgentProcessRecord(
        agent_id="recovery-agent",
        agent_incarnation=2,
        worker_generation=7,
        supervisor_epoch=11,
        observed_state=(
            AgentProcessState.READY if ready else AgentProcessState.STARTING
        ),
        generation_capability_hash="1" * 64,
        lifetime_lock_identity=lock.identity,
        lifetime_lock_acquired_at=NOW,
        process_lease_identity="process-lease-7",
        process_lease_token_hash="2" * 64,
        lease_expires_at=NOW + timedelta(minutes=10),
        lease_ended_at=None,
        started_at=NOW,
        pid=process.pid,
        process_group_id=process.pid,
        kernel_process_birth_id=(
            f"linux:{birth.boot_id}:"
            f"{birth.start_time_ticks + start_ticks_delta}"
        ),
        hello_frame_id="hello-7",
        hello_payload_hash="3" * 64,
        capabilities_frame_id="capabilities-7",
        capabilities_payload_hash="4" * 64,
        capability_snapshot_hash="5" * 64,
        handshake_committed_at=NOW,
        ready_frame_id="ready-7" if ready else None,
        ready_payload_hash="6" * 64 if ready else None,
        ready_at=NOW if ready else None,
        last_heartbeat_at=NOW,
    )


def _pre_handshake_record(lock: InheritedLifetimeLock) -> AgentProcessRecord:
    return AgentProcessRecord(
        agent_id="recovery-agent",
        agent_incarnation=2,
        worker_generation=7,
        supervisor_epoch=11,
        observed_state=AgentProcessState.STARTING,
        generation_capability_hash="1" * 64,
        lifetime_lock_identity=lock.identity,
        lifetime_lock_acquired_at=NOW,
        process_lease_identity="process-lease-7",
        process_lease_token_hash="2" * 64,
        lease_expires_at=NOW + timedelta(minutes=10),
        lease_ended_at=None,
        started_at=NOW,
    )


def _stopped_with_proof(
    record: AgentProcessRecord,
    *,
    proof_kind: str,
) -> AgentProcessRecord:
    checked_at = NOW + timedelta(seconds=30)
    proof: dict[str, Any] = {
        "proof_kind": proof_kind,
        "agent_id": record.agent_id,
        "agent_incarnation": record.agent_incarnation,
        "worker_generation": record.worker_generation,
        "supervisor_epoch": record.supervisor_epoch,
        "fencing_supervisor_epoch": 12,
        "process_lease_identity": record.process_lease_identity,
        "lifetime_lock_identity": record.lifetime_lock_identity,
        "process_group_id": record.process_group_id,
        "kernel_process_birth_id": record.kernel_process_birth_id,
        "runtime_stopped": True,
        "process_group_empty": True,
        "invocation_jobs_empty": True,
        "lifetime_lock_released": True,
        "checked_at": datetime_to_text(checked_at),
    }
    if proof_kind == "never_spawned_v1":
        proof["spawn_attempted"] = False
    return replace(
        record,
        observed_state=AgentProcessState.STOPPED,
        lease_ended_at=checked_at,
        stopped_by_supervisor_epoch=12,
        cleanup_proof=proof,
        cleanup_proof_hash=hashlib.sha256(
            json_dumps(proof).encode("utf-8")
        ).hexdigest(),
        cleanup_proved_at=checked_at,
        stopped_at=checked_at,
        stop_reason="replacement_supervisor_recovery",
    )


class _RecoveryStore:
    def __init__(self, record: AgentProcessRecord, *, failures: int = 0) -> None:
        self.record = record
        self.failures = failures
        self.calls: list[dict[str, Any]] = []
        self.committed: AgentProcessRecord | None = None

    async def fence_agent_process_stopped(self, **kwargs: Any) -> AgentProcessRecord:
        self.calls.append(dict(kwargs))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected durable fence failure")
        proof = dict(kwargs["cleanup_proof"])
        proof["checked_at"] = datetime_to_text(proof["checked_at"])
        stopped_at = kwargs["stopped_at"]
        proof_hash = hashlib.sha256(
            json_dumps(proof).encode("utf-8")
        ).hexdigest()
        self.committed = replace(
            self.record,
            observed_state=AgentProcessState.STOPPED,
            lease_ended_at=stopped_at,
            stopped_by_supervisor_epoch=kwargs["fencing_supervisor_epoch"],
            cleanup_proof=proof,
            cleanup_proof_hash=proof_hash,
            cleanup_proved_at=kwargs["cleanup_proof"]["checked_at"],
            stopped_at=stopped_at,
            stop_reason=kwargs["reason"],
        )
        return self.committed


def _spawn_leader(lock: InheritedLifetimeLock, code: str) -> subprocess.Popen[Any]:
    return subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        pass_fds=(lock.child_fd,),
    )


def _kill_test_group(process_group_id: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process_group_id, signal.SIGKILL)


def test_store_failure_retains_exact_proof_for_idempotent_retry(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        lock = InheritedLifetimeLock.create((tmp_path / "locks").resolve())
        process = _spawn_leader(
            lock,
            "import signal,time;"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "time.sleep(60)",
        )
        record = _record(lock, process)
        lock.release_parent_copy()
        store = _RecoveryStore(record, failures=1)
        owner = AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=(tmp_path / "locks").resolve(),
            timeout=2,
            clock=lambda: NOW + timedelta(seconds=30),
        )
        reaper = asyncio.create_task(asyncio.to_thread(process.wait, 5))
        try:
            with pytest.raises(AgentProcessRecoveryError) as caught:
                await owner.retry()
            assert caught.value.recovery_owner is owner
            assert owner.cleanup_proven
            assert not owner.store_committed
            assert owner.lifetime_lock is not None
            assert owner.lifetime_lock.release_proven
            assert Path(owner.lifetime_lock.path).exists()
            assert await reaper is not None

            proof_before = dict(store.calls[0]["cleanup_proof"])
            stopped = await caught.value.recovery_owner.retry()
            assert stopped.observed_state is AgentProcessState.STOPPED
            assert owner.store_committed and owner.artifact_finalized
            assert not Path(owner.lifetime_lock.path).exists()
            assert store.calls[1]["cleanup_proof"] == proof_before

            # A caller replay on the same strong owner performs no new store
            # mutation and cannot recreate or re-target the old generation.
            assert await owner.retry() == stopped
            assert len(store.calls) == 2
        finally:
            _kill_test_group(process.pid)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
            owner.close()

    asyncio.run(scenario())


def test_pre_handshake_generation_without_pid_uses_lock_release_proof(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        record = _pre_handshake_record(lock)
        # This models the crash window where spawn may have been attempted but
        # no process identity was committed.  Only exact lock release is known.
        lock.release_parent_copy()
        store = _RecoveryStore(record)
        with AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=lock_root,
            timeout=0.2,
            clock=lambda: NOW + timedelta(seconds=30),
        ) as owner:
            stopped = await owner.retry()
            assert stopped.observed_state is AgentProcessState.STOPPED
            assert owner.process_group is None
            assert owner.cleanup_proven
            proof = store.calls[0]["cleanup_proof"]
            assert proof["proof_kind"] == "verified_empty_v1"
            assert "spawn_attempted" not in proof
            assert proof["pre_handshake_process_identity_absent"] is True
            assert proof["process_group_empty_by_lifetime_lock"] is True
            assert proof["process_group_id"] is None
            assert proof["kernel_process_birth_id"] is None
            assert not Path(lock.path).exists()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "proof_kind",
    ["verified_empty_v1", "never_spawned_v1"],
)
def test_stopped_replay_validates_both_cleanup_proof_kinds(
    tmp_path: Path,
    proof_kind: str,
) -> None:
    async def scenario() -> None:
        lock_root = (tmp_path / proof_kind / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        record = _stopped_with_proof(
            _pre_handshake_record(lock),
            proof_kind=proof_kind,
        )
        lock.release_parent_copy()
        store = _RecoveryStore(record)
        with AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=13,
            owner_instance_id="replacement-13",
            lifetime_lock_root=lock_root,
            timeout=0.2,
        ) as owner:
            assert await owner.retry() == record
            assert owner.artifact_finalized
            assert store.calls == []
            assert not Path(lock.path).exists()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "control_exception",
    [asyncio.CancelledError, KeyboardInterrupt, SystemExit],
)
def test_retry_preserves_control_flow_and_context_closes_evidence(
    tmp_path: Path,
    control_exception: type[BaseException],
) -> None:
    class ControlFlowStore(_RecoveryStore):
        async def fence_agent_process_stopped(
            self,
            **kwargs: Any,
        ) -> AgentProcessRecord:
            self.calls.append(dict(kwargs))
            raise control_exception()

    async def scenario() -> None:
        lock_root = (tmp_path / control_exception.__name__ / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        record = _pre_handshake_record(lock)
        lock.release_parent_copy()
        store = ControlFlowStore(record)
        owner = AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=lock_root,
            timeout=0.2,
            clock=lambda: NOW + timedelta(seconds=30),
        )
        with owner:
            with pytest.raises(control_exception):
                await owner.retry()
            assert owner.cleanup_proven
            assert owner.lifetime_lock is not None
            assert owner.lifetime_lock.release_proven
            assert Path(lock.path).exists()

        # Exiting the explicit ownership scope releases the acquired probe,
        # allowing another supervisor to reopen and finish exact-inode cleanup.
        replacement = RecoveredLifetimeLock.reopen(lock_root, lock.identity)
        try:
            assert replacement.try_prove_released()
            replacement.finalize_unlink()
        finally:
            replacement.close()

    asyncio.run(scenario())


def test_convenience_recovery_closes_owner_when_cancelled(tmp_path: Path) -> None:
    class CancelledStore(_RecoveryStore):
        async def fence_agent_process_stopped(
            self,
            **kwargs: Any,
        ) -> AgentProcessRecord:
            self.calls.append(dict(kwargs))
            raise asyncio.CancelledError

    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        record = _pre_handshake_record(lock)
        lock.release_parent_copy()
        with pytest.raises(asyncio.CancelledError):
            await recover_agent_process_generation(
                CancelledStore(record),
                record,
                fencing_supervisor_epoch=12,
                owner_instance_id="replacement-12",
                lifetime_lock_root=lock_root,
                timeout=0.2,
                clock=lambda: NOW + timedelta(seconds=30),
            )

        replacement = RecoveredLifetimeLock.reopen(lock_root, lock.identity)
        try:
            assert replacement.try_prove_released()
            replacement.finalize_unlink()
        finally:
            replacement.close()

    asyncio.run(scenario())


def test_returned_cleanup_timestamp_must_match_committed_proof(
    tmp_path: Path,
) -> None:
    class ConflictingTimestampStore(_RecoveryStore):
        async def fence_agent_process_stopped(
            self,
            **kwargs: Any,
        ) -> AgentProcessRecord:
            committed = await super().fence_agent_process_stopped(**kwargs)
            assert committed.cleanup_proved_at is not None
            return replace(
                committed,
                cleanup_proved_at=committed.cleanup_proved_at
                + timedelta(seconds=1),
            )

    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        record = _pre_handshake_record(lock)
        lock.release_parent_copy()
        owner = AgentProcessRecoveryOwner(
            ConflictingTimestampStore(record),
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=lock_root,
            timeout=0.2,
            clock=lambda: NOW + timedelta(seconds=30),
        )
        try:
            with pytest.raises(AgentProcessRecoveryError) as caught:
                await owner.retry()
            assert isinstance(
                caught.value.__cause__,
                AgentProcessRecoveryEvidenceError,
            )
            assert not owner.store_committed
            assert Path(lock.path).exists()
        finally:
            owner.close()

        replacement = RecoveredLifetimeLock.reopen(lock_root, lock.identity)
        try:
            assert replacement.try_prove_released()
            replacement.finalize_unlink()
        finally:
            replacement.close()

    asyncio.run(scenario())


def test_replacement_finishes_unlink_after_commit_ack_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        process = _spawn_leader(lock, "import time; time.sleep(60)")
        record = _record(lock, process)
        lock.release_parent_copy()
        store = _RecoveryStore(record)
        owner = AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=lock_root,
            timeout=2,
            clock=lambda: NOW + timedelta(seconds=30),
        )
        original_finalize = RecoveredLifetimeLock.finalize_unlink

        def injected_crash_window(self: RecoveredLifetimeLock) -> None:
            raise OSError("injected crash after durable stop commit")

        monkeypatch.setattr(
            RecoveredLifetimeLock,
            "finalize_unlink",
            injected_crash_window,
        )
        reaper = asyncio.create_task(asyncio.to_thread(process.wait, 5))
        try:
            with pytest.raises(AgentProcessRecoveryError) as caught:
                await owner.retry()
            assert await reaper is not None
            assert caught.value.recovery_owner.store_committed
            assert not owner.artifact_finalized
            assert store.committed is not None
            assert owner.lifetime_lock is not None
            artifact = Path(owner.lifetime_lock.path)
            assert artifact.exists()

            # Model a replacement after the first supervisor loses its
            # in-memory owner immediately after the successful SQLite fence.
            owner.close()
            monkeypatch.setattr(
                RecoveredLifetimeLock,
                "finalize_unlink",
                original_finalize,
            )
            replacement = AgentProcessRecoveryOwner(
                store,
                store.committed,
                fencing_supervisor_epoch=13,
                owner_instance_id="replacement-13",
                lifetime_lock_root=lock_root,
                timeout=2,
            )
            try:
                assert await replacement.retry() == store.committed
                assert replacement.artifact_finalized
                assert not artifact.exists()
                assert len(store.calls) == 1
                assert await replacement.retry() == store.committed
            finally:
                replacement.close()
        finally:
            _kill_test_group(process.pid)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
            owner.close()

    asyncio.run(scenario())


def test_leader_missing_nonempty_numeric_group_fails_closed_without_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        descendant_pid_path = tmp_path / "descendant.pid"
        exit_gate = tmp_path / "leader-exit"
        descendant_code = (
            "import signal,time;"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "time.sleep(60)"
        )
        leader_code = (
            "import pathlib,subprocess,sys,time;"
            f"child=subprocess.Popen([sys.executable,'-c',{descendant_code!r}],"
            f"pass_fds=({lock.child_fd},));"
            f"pathlib.Path({str(descendant_pid_path)!r}).write_text(str(child.pid));"
            f"gate=pathlib.Path({str(exit_gate)!r});"
            "\nwhile not gate.exists(): time.sleep(0.01)"
        )
        leader = _spawn_leader(lock, leader_code)
        record = _record(lock, leader)
        lock.release_parent_copy()
        signalled: list[tuple[int, int]] = []

        def record_signal(descriptor: int, sig: int) -> None:
            signalled.append((descriptor, sig))

        monkeypatch.setattr(process_lifetime, "_send_pidfd_signal", record_signal)
        try:
            for _ in range(200):
                if descendant_pid_path.exists():
                    break
                await asyncio.sleep(0.01)
            assert descendant_pid_path.exists()
            descendant_pid = int(descendant_pid_path.read_text())
            exit_gate.touch()
            assert await asyncio.to_thread(leader.wait, 3) == 0
            assert not Path(f"/proc/{leader.pid}").exists()
            assert Path(f"/proc/{descendant_pid}").exists()

            store = _RecoveryStore(record)
            owner = AgentProcessRecoveryOwner(
                store,
                record,
                fencing_supervisor_epoch=12,
                owner_instance_id="replacement-12",
                lifetime_lock_root=lock_root,
                timeout=3,
                clock=lambda: NOW + timedelta(seconds=30),
            )
            try:
                with pytest.raises(AgentProcessRecoveryError) as caught:
                    await owner.retry()
                assert "leader is missing" in str(caught.value.__cause__)
                assert Path(f"/proc/{descendant_pid}").exists()
                assert signalled == []
                assert store.calls == []
            finally:
                owner.close()
        finally:
            _kill_test_group(leader.pid)
            with contextlib.suppress(subprocess.TimeoutExpired):
                leader.wait(timeout=1)
            for _ in range(200):
                try:
                    recovered = RecoveredLifetimeLock.reopen(
                        lock_root,
                        lock.identity,
                    )
                except Exception:
                    await asyncio.sleep(0.01)
                    continue
                try:
                    if recovered.try_prove_released():
                        recovered.finalize_unlink()
                        break
                finally:
                    recovered.close()
                await asyncio.sleep(0.01)

    asyncio.run(scenario())


def test_birth_mismatch_fails_closed_without_signalling(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        lock_root = (tmp_path / "locks").resolve()
        lock = InheritedLifetimeLock.create(lock_root)
        process = _spawn_leader(lock, "import time; time.sleep(60)")
        record = _record(lock, process, start_ticks_delta=1)
        lock.release_parent_copy()
        store = _RecoveryStore(record)
        owner = AgentProcessRecoveryOwner(
            store,
            record,
            fencing_supervisor_epoch=12,
            owner_instance_id="replacement-12",
            lifetime_lock_root=lock_root,
            timeout=0.2,
        )
        try:
            with pytest.raises(AgentProcessRecoveryError) as caught:
                await owner.retry()
            assert "birth identity changed" in str(caught.value.__cause__)
            assert process.poll() is None
            assert store.calls == []
            assert caught.value.recovery_owner.lifetime_lock is not None
        finally:
            owner.close()
            _kill_test_group(process.pid)
            process.wait(timeout=3)
            recovered = RecoveredLifetimeLock.reopen(lock_root, lock.identity)
            assert recovered.try_prove_released()
            recovered.finalize_unlink()

    asyncio.run(scenario())


def test_recovery_proof_commits_through_sqlite_generation_fences(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        lock_root = (tmp_path / "locks").resolve()
        base = datetime.now(timezone.utc)
        first = SQLiteStore(database)
        await first.initialize(recover_startup_state=False)
        epoch1 = await first.activate_supervisor_epoch(
            owner_instance_id="sqlite-owner-1",
            channel="wechat",
            bot_id="bot",
            started_at=base,
        )

        lock = InheritedLifetimeLock.create(lock_root)
        process = _spawn_leader(lock, "import time; time.sleep(60)")
        birth = capture_linux_process_birth(process.pid)
        generation_capability = "sqlite-generation-secret"
        lease_token = "sqlite-process-lease-secret"
        try:
            started = await first.begin_agent_process_generation(
                agent_id="codex",
                agent_incarnation=1,
                supervisor_epoch=epoch1.epoch,
                owner_instance_id="sqlite-owner-1",
                generation_capability=generation_capability,
                lifetime_lock_identity=lock.identity,
                lifetime_lock_acquired_at=base + timedelta(seconds=1),
                process_lease_identity="sqlite-process-lease",
                process_lease_token=lease_token,
                lease_expires_at=base + timedelta(minutes=10),
                started_at=base + timedelta(seconds=2),
            )
            persisted = await first.commit_agent_process_handshake(
                agent_id="codex",
                agent_incarnation=1,
                worker_generation=started.worker_generation,
                supervisor_epoch=epoch1.epoch,
                owner_instance_id="sqlite-owner-1",
                generation_capability=generation_capability,
                process_lease_identity="sqlite-process-lease",
                process_lease_token=lease_token,
                pid=process.pid,
                process_group_id=process.pid,
                kernel_process_birth_id=birth.wire_birth_id,
                hello_frame_id="sqlite-hello",
                hello_payload_hash="1" * 64,
                capabilities_frame_id="sqlite-capabilities",
                capabilities_payload_hash="2" * 64,
                capability_snapshot_hash="3" * 64,
                committed_at=base + timedelta(seconds=3),
            )
            lock.release_parent_copy()
            await first.finish_supervisor_epoch(
                epoch1.epoch,
                owner_instance_id="sqlite-owner-1",
                reason="replacement_test",
                stopped_at=base + timedelta(seconds=10),
            )
            await first.close()

            replacement = SQLiteStore(database)
            await replacement.initialize(recover_startup_state=False)
            epoch2 = await replacement.activate_supervisor_epoch(
                owner_instance_id="sqlite-owner-2",
                channel="wechat",
                bot_id="bot",
                started_at=base + timedelta(seconds=20),
            )
            owner = AgentProcessRecoveryOwner(
                replacement,
                persisted,
                fencing_supervisor_epoch=epoch2.epoch,
                owner_instance_id="sqlite-owner-2",
                lifetime_lock_root=lock_root,
                timeout=2,
                clock=lambda: base + timedelta(seconds=30),
            )
            reaper = asyncio.create_task(asyncio.to_thread(process.wait, 5))
            try:
                stopped = await owner.retry()
                assert await reaper is not None
                assert stopped.observed_state is AgentProcessState.STOPPED
                assert stopped.stopped_by_supervisor_epoch == epoch2.epoch
                assert stopped.cleanup_proof["process_group_empty"] is True
                assert stopped.cleanup_proof["lifetime_lock_released"] is True
                assert stopped.cleanup_proof["non_schedulable_bootstrap"] is True
                assert not Path(lock.path).exists()
                assert (
                    await replacement.get_agent_process(
                        "codex",
                        1,
                        started.worker_generation,
                    )
                    == stopped
                )
            finally:
                owner.close()
                await replacement.close()
        finally:
            with contextlib.suppress(Exception):
                await first.close()
            _kill_test_group(process.pid)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
            if Path(lock.path).exists():
                with contextlib.suppress(Exception):
                    recovered = RecoveredLifetimeLock.reopen(lock_root, lock.identity)
                    if recovered.try_prove_released():
                        recovered.finalize_unlink()

    asyncio.run(scenario())


def test_pre_handshake_lock_only_proof_commits_through_sqlite(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        lock_root = (tmp_path / "locks").resolve()
        base = datetime.now(timezone.utc)
        first = SQLiteStore(database)
        await first.initialize(recover_startup_state=False)
        epoch1 = await first.activate_supervisor_epoch(
            owner_instance_id="pre-handshake-owner-1",
            channel="wechat",
            bot_id="bot",
            started_at=base,
        )
        lock = InheritedLifetimeLock.create(lock_root)
        try:
            started = await first.begin_agent_process_generation(
                agent_id="codex",
                agent_incarnation=1,
                supervisor_epoch=epoch1.epoch,
                owner_instance_id="pre-handshake-owner-1",
                generation_capability="pre-handshake-generation-secret",
                lifetime_lock_identity=lock.identity,
                lifetime_lock_acquired_at=base + timedelta(seconds=1),
                process_lease_identity="pre-handshake-process-lease",
                process_lease_token="pre-handshake-lease-secret",
                lease_expires_at=base + timedelta(minutes=10),
                started_at=base + timedelta(seconds=2),
            )
            assert started.pid is None
            lock.release_parent_copy()
            await first.finish_supervisor_epoch(
                epoch1.epoch,
                owner_instance_id="pre-handshake-owner-1",
                reason="replacement_test",
                stopped_at=base + timedelta(seconds=10),
            )
            await first.close()

            replacement = SQLiteStore(database)
            await replacement.initialize(recover_startup_state=False)
            epoch2 = await replacement.activate_supervisor_epoch(
                owner_instance_id="pre-handshake-owner-2",
                channel="wechat",
                bot_id="bot",
                started_at=base + timedelta(seconds=20),
            )
            with AgentProcessRecoveryOwner(
                replacement,
                started,
                fencing_supervisor_epoch=epoch2.epoch,
                owner_instance_id="pre-handshake-owner-2",
                lifetime_lock_root=lock_root,
                timeout=0.2,
                clock=lambda: base + timedelta(seconds=30),
            ) as owner:
                stopped = await owner.retry()
                assert stopped.observed_state is AgentProcessState.STOPPED
                assert stopped.cleanup_proof["proof_kind"] == "verified_empty_v1"
                assert "spawn_attempted" not in stopped.cleanup_proof
                assert (
                    stopped.cleanup_proof[
                        "pre_handshake_process_identity_absent"
                    ]
                    is True
                )
                assert stopped.process_group_id is None
                assert not Path(lock.path).exists()
            await replacement.close()
        finally:
            with contextlib.suppress(Exception):
                await first.close()
            if Path(lock.path).exists():
                with contextlib.suppress(Exception):
                    recovered = RecoveredLifetimeLock.reopen(
                        lock_root,
                        lock.identity,
                    )
                    if recovered.try_prove_released():
                        recovered.finalize_unlink()

    asyncio.run(scenario())
