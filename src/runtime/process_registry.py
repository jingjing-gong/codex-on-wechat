"""Supervisor-only recovery of persisted Agent process generations.

This module is deliberately disconnected from launcher and scheduling code.  A
replacement supervisor supplies one retained :class:`AgentProcessRecord`, its
currently owned supervisor epoch, and the configured lifetime-lock root.  The
recovery owner verifies and stops only that exact Linux generation, retains OS
evidence while the durable stop is fenced, and unlinks the named lock artifact
only after the store commit succeeds.

The current child foundation never becomes schedulable.  Accordingly this
primitive rejects every generation that ever committed ``READY``: proving its
invocation-job containment empty belongs to the later cgroup/job registry, not
to this process-group-only prerequisite.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from .models import (
    AgentProcessRecord,
    AgentProcessState,
    json_dumps,
    text_to_datetime,
)
from .process_lifetime import (
    ProcessLifetimeArtifactMissingError,
    ProcessLifetimeError,
    ProcessLifetimeStillHeldError,
    RecoveredLifetimeLock,
    RecoveredProcessGroup,
    terminate_recovered_process_group,
)


@runtime_checkable
class AgentProcessRecoveryStore(Protocol):
    """The one durable mutation required by process-generation recovery."""

    async def fence_agent_process_stopped(
        self,
        *,
        agent_id: str,
        agent_incarnation: int,
        worker_generation: int,
        expected_supervisor_epoch: int,
        fencing_supervisor_epoch: int,
        owner_instance_id: str,
        process_lease_identity: str,
        cleanup_proof: Mapping[str, object],
        reason: str,
        stopped_at: datetime,
        exit_code: int | None = None,
        last_error: str | None = None,
    ) -> AgentProcessRecord: ...


class AgentProcessRecoveryEvidenceError(ProcessLifetimeError):
    """Persisted metadata is insufficient for this safe recovery primitive."""


class AgentProcessRecoveryError(RuntimeError):
    """Recovery failed while a retryable owner retains the exact evidence."""

    def __init__(
        self,
        message: str,
        *,
        recovery_owner: AgentProcessRecoveryOwner,
    ) -> None:
        super().__init__(message)
        self.recovery_owner = recovery_owner


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _required_text(value: object, name: str, *, maximum: int = 512) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} is required")
    if len(normalized) > maximum:
        raise ValueError(f"{name} is too long")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in normalized):
        raise ValueError(f"{name} contains a control character")
    return normalized


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _recovery_timestamp(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if not isinstance(value, datetime):
        raise TypeError("recovery clock must return a datetime")
    if value.tzinfo is None:
        raise ValueError("recovery clock must return an aware datetime")
    return value.astimezone(timezone.utc)


class AgentProcessRecoveryOwner:
    """Strong owner of kernel/lock evidence until recovery is finalized.

    ``retry`` is serialized and idempotent.  A failure after process cleanup
    retains the acquired lock probe, so another call can replay the exact store
    fence.  A failure after the store commit retains the same probe and retries
    only the exact-inode unlink.  Closing an unfinished owner deliberately
    leaves the secure name in place for a replacement supervisor.
    """

    def __init__(
        self,
        store: AgentProcessRecoveryStore,
        record: AgentProcessRecord,
        *,
        fencing_supervisor_epoch: int,
        owner_instance_id: str,
        lifetime_lock_root: str | os.PathLike[str],
        timeout: float = 5.0,
        reason: str = "replacement_supervisor_recovery",
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(record, AgentProcessRecord):
            raise TypeError("record must be an AgentProcessRecord")
        self.store = store
        self.record = record
        self.fencing_supervisor_epoch = _positive_integer(
            fencing_supervisor_epoch,
            "fencing_supervisor_epoch",
        )
        self.owner_instance_id = _required_text(
            owner_instance_id,
            "owner_instance_id",
        )
        self.lifetime_lock_root = os.fspath(lifetime_lock_root)
        if not isinstance(self.lifetime_lock_root, str):
            raise TypeError("Agent lifetime lock root must be a text path")
        if not os.path.isabs(os.path.expanduser(self.lifetime_lock_root)):
            raise ValueError("Agent lifetime lock root must be absolute")
        if isinstance(timeout, bool):
            raise ValueError("timeout must be a non-negative finite number")
        try:
            timeout_value = float(timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("timeout must be a non-negative finite number") from exc
        if not timeout_value >= 0.0 or timeout_value == float("inf"):
            raise ValueError("timeout must be a non-negative finite number")
        self.timeout = timeout_value
        self.reason = _required_text(reason, "reason")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.clock = clock

        self._operation_lock = asyncio.Lock()
        self._lifetime_lock: RecoveredLifetimeLock | None = None
        self._process_group: RecoveredProcessGroup | None = None
        self._cleanup_proof: dict[str, object] | None = None
        self._stopped_at: datetime | None = None
        self._committed_record: AgentProcessRecord | None = None
        self._artifact_finalized = False

    @property
    def cleanup_proven(self) -> bool:
        return self._cleanup_proof is not None

    @property
    def store_committed(self) -> bool:
        return self._committed_record is not None

    @property
    def artifact_finalized(self) -> bool:
        return self._artifact_finalized

    @property
    def lifetime_lock(self) -> RecoveredLifetimeLock | None:
        return self._lifetime_lock

    @property
    def process_group(self) -> RecoveredProcessGroup | None:
        return self._process_group

    def __enter__(self) -> AgentProcessRecoveryOwner:
        """Keep descriptor ownership explicit for retryable recovery scopes."""

        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _process_identity_is_absent(self) -> bool:
        return all(
            value is None
            for value in (
                self.record.pid,
                self.record.process_group_id,
                self.record.kernel_process_birth_id,
            )
        )

    def _open_active_evidence(self) -> None:
        if self._lifetime_lock is None:
            self._lifetime_lock = RecoveredLifetimeLock.reopen(
                self.lifetime_lock_root,
                self.record.lifetime_lock_identity,
            )
        if self._process_group is not None:
            return
        if (
            self.record.observed_state is AgentProcessState.READY
            or self.record.ready_at is not None
        ):
            raise AgentProcessRecoveryEvidenceError(
                "schedulable Agent recovery requires invocation-job containment proof"
            )
        process_identity = (
            self.record.pid,
            self.record.process_group_id,
            self.record.kernel_process_birth_id,
        )
        if all(value is None for value in process_identity):
            handshake_evidence = (
                self.record.hello_frame_id,
                self.record.hello_payload_hash,
                self.record.capabilities_frame_id,
                self.record.capabilities_payload_hash,
                self.record.capability_snapshot_hash,
                self.record.handshake_committed_at,
                self.record.ready_frame_id,
                self.record.ready_payload_hash,
                self.record.ready_at,
            )
            if any(value is not None for value in handshake_evidence):
                raise AgentProcessRecoveryEvidenceError(
                    "persisted Agent handshake exists without process identity"
                )
            # A crash can occur after the durable generation row is created
            # and spawn is attempted but before process identity is committed.
            # The exact inherited lifetime lock is the only truthful evidence
            # available in this window; _recover waits for its release and
            # never claims that spawn was not attempted.
            return
        if any(value is None for value in process_identity):
            raise AgentProcessRecoveryEvidenceError(
                "persisted Agent generation has partial process birth/group evidence"
            )
        assert self.record.pid is not None
        assert self.record.process_group_id is not None
        assert self.record.kernel_process_birth_id is not None
        self._process_group = RecoveredProcessGroup.from_persisted(
            pid=self.record.pid,
            process_group_id=self.record.process_group_id,
            kernel_process_birth_id=self.record.kernel_process_birth_id,
        )

    def _validate_stopped_record(self) -> None:
        proof = self.record.cleanup_proof
        if not isinstance(proof, Mapping):
            raise AgentProcessRecoveryEvidenceError(
                "stopped Agent generation lacks cleanup proof"
            )
        proof_kind = proof.get("proof_kind")
        expected = {
            "agent_id": self.record.agent_id,
            "agent_incarnation": self.record.agent_incarnation,
            "worker_generation": self.record.worker_generation,
            "supervisor_epoch": self.record.supervisor_epoch,
            "process_lease_identity": self.record.process_lease_identity,
            "lifetime_lock_identity": self.record.lifetime_lock_identity,
            "process_group_id": self.record.process_group_id,
            "kernel_process_birth_id": self.record.kernel_process_birth_id,
            "runtime_stopped": True,
            "process_group_empty": True,
            "invocation_jobs_empty": True,
            "lifetime_lock_released": True,
        }
        if self.record.stopped_by_supervisor_epoch is not None:
            expected["fencing_supervisor_epoch"] = (
                self.record.stopped_by_supervisor_epoch
            )
        mismatches = [
            name for name, value in expected.items() if proof.get(name) != value
        ]
        if proof_kind not in {"verified_empty_v1", "never_spawned_v1"}:
            mismatches.append("proof_kind")
        if proof_kind == "never_spawned_v1" and (
            proof.get("spawn_attempted") is not False
            or not self._process_identity_is_absent()
        ):
            mismatches.append("never_spawned_evidence")
        checked_at = text_to_datetime(proof.get("checked_at"))
        computed_hash = hashlib.sha256(
            json_dumps(dict(proof)).encode("utf-8")
        ).hexdigest()
        if (
            mismatches
            or self.record.cleanup_proof_hash is None
            or not hmac.compare_digest(
                self.record.cleanup_proof_hash,
                computed_hash,
            )
            or self.record.cleanup_proved_at is None
            or self.record.stopped_at is None
            or checked_at is None
            or self.record.cleanup_proved_at != checked_at
            or self.record.stopped_at < checked_at
        ):
            raise AgentProcessRecoveryEvidenceError(
                "stopped Agent generation cleanup evidence conflicts"
            )

    async def _replay_stopped(self) -> AgentProcessRecord:
        self._validate_stopped_record()
        if self._artifact_finalized:
            return self.record
        if self._lifetime_lock is None:
            try:
                self._lifetime_lock = RecoveredLifetimeLock.reopen(
                    self.lifetime_lock_root,
                    self.record.lifetime_lock_identity,
                )
            except ProcessLifetimeArtifactMissingError:
                # The exact durable cleanup proof authorizes the prior unlink;
                # absence is replay success only for an already stopped row.
                self._artifact_finalized = True
                return self.record
        if not self._lifetime_lock.try_prove_released():
            raise ProcessLifetimeStillHeldError(
                "stopped Agent lifetime lock unexpectedly remains held"
            )
        self._lifetime_lock.finalize_unlink()
        self._artifact_finalized = True
        return self.record

    def _build_cleanup_proof(self, checked_at: datetime) -> dict[str, object]:
        proof: dict[str, object] = {
            "proof_kind": "verified_empty_v1",
            "agent_id": self.record.agent_id,
            "agent_incarnation": self.record.agent_incarnation,
            "worker_generation": self.record.worker_generation,
            "supervisor_epoch": self.record.supervisor_epoch,
            "fencing_supervisor_epoch": self.fencing_supervisor_epoch,
            "process_lease_identity": self.record.process_lease_identity,
            "lifetime_lock_identity": self.record.lifetime_lock_identity,
            "pid": self.record.pid,
            "process_group_id": self.record.process_group_id,
            "kernel_process_birth_id": self.record.kernel_process_birth_id,
            "runtime_stopped": True,
            "process_group_empty": True,
            # READY is rejected above.  The disconnected bootstrap has no
            # assignment API and therefore cannot own an invocation job.
            "invocation_jobs_empty": True,
            "lifetime_lock_released": True,
            "non_schedulable_bootstrap": True,
            "checked_at": checked_at,
        }
        if self._process_identity_is_absent():
            proof.update(
                {
                    # A launch may have been attempted.  Absence of committed
                    # PID metadata is not evidence of never spawning, so this
                    # deliberately remains verified_empty_v1 and carries no
                    # spawn_attempted=false assertion.
                    "pre_handshake_process_identity_absent": True,
                    "process_group_empty_by_lifetime_lock": True,
                }
            )
        return proof

    async def _prove_unidentified_runtime_stopped(self) -> None:
        """Wait for exact lock release when no PID identity was committed."""

        assert self._lifetime_lock is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout
        while True:
            if self._lifetime_lock.try_prove_released():
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ProcessLifetimeStillHeldError(
                    "pre-handshake Agent lifetime lock remains held without "
                    "safe process-targeting evidence"
                )
            await asyncio.sleep(min(0.02, remaining))

    @staticmethod
    def _validate_committed_record(
        expected: AgentProcessRecord,
        committed: AgentProcessRecord,
        *,
        cleanup_proof: Mapping[str, object],
        fencing_supervisor_epoch: int,
    ) -> None:
        if not isinstance(committed, AgentProcessRecord):
            raise AgentProcessRecoveryEvidenceError(
                "process recovery store returned an invalid record"
            )
        if (
            committed.agent_id,
            committed.agent_incarnation,
            committed.worker_generation,
            committed.supervisor_epoch,
            committed.process_lease_identity,
            committed.lifetime_lock_identity,
        ) != (
            expected.agent_id,
            expected.agent_incarnation,
            expected.worker_generation,
            expected.supervisor_epoch,
            expected.process_lease_identity,
            expected.lifetime_lock_identity,
        ):
            raise AgentProcessRecoveryEvidenceError(
                "process recovery store returned conflicting generation identity"
            )
        if committed.observed_state is not AgentProcessState.STOPPED:
            raise AgentProcessRecoveryEvidenceError(
                "process recovery store did not fence the generation stopped"
            )
        committed_proof = dict(committed.cleanup_proof)
        expected_proof = dict(cleanup_proof)
        committed_checked_at = text_to_datetime(
            committed_proof.pop("checked_at", None)
        )
        expected_checked_at = text_to_datetime(
            expected_proof.pop("checked_at", None)
        )
        if (
            committed_proof != expected_proof
            or committed_checked_at is None
            or expected_checked_at is None
            or committed_checked_at != expected_checked_at
            or committed.stopped_by_supervisor_epoch
            != fencing_supervisor_epoch
            or committed.cleanup_proof_hash is None
            or committed.cleanup_proved_at is None
            or committed.stopped_at is None
            or committed.cleanup_proved_at != committed_checked_at
            or committed.stopped_at != committed_checked_at
        ):
            raise AgentProcessRecoveryEvidenceError(
                "process recovery store returned conflicting cleanup evidence"
            )
        expected_hash = hashlib.sha256(
            json_dumps(dict(committed.cleanup_proof)).encode("utf-8")
        ).hexdigest()
        if not hmac.compare_digest(committed.cleanup_proof_hash, expected_hash):
            raise AgentProcessRecoveryEvidenceError(
                "process recovery store returned an invalid cleanup proof hash"
            )

    async def _recover(self) -> AgentProcessRecord:
        if self.record.observed_state is AgentProcessState.STOPPED:
            return await self._replay_stopped()
        self._open_active_evidence()
        assert self._lifetime_lock is not None

        if self._cleanup_proof is None:
            if self._process_group is None:
                await self._prove_unidentified_runtime_stopped()
            else:
                await terminate_recovered_process_group(
                    self._process_group,
                    self._lifetime_lock,
                    self.timeout,
                )
            checked_at = _recovery_timestamp(self.clock)
            self._cleanup_proof = self._build_cleanup_proof(checked_at)
            self._stopped_at = checked_at

        if self._committed_record is None:
            assert self._stopped_at is not None
            committed = await self.store.fence_agent_process_stopped(
                agent_id=self.record.agent_id,
                agent_incarnation=self.record.agent_incarnation,
                worker_generation=self.record.worker_generation,
                expected_supervisor_epoch=self.record.supervisor_epoch,
                fencing_supervisor_epoch=self.fencing_supervisor_epoch,
                owner_instance_id=self.owner_instance_id,
                process_lease_identity=self.record.process_lease_identity,
                cleanup_proof=self._cleanup_proof,
                reason=self.reason,
                stopped_at=self._stopped_at,
                exit_code=None,
                last_error=None,
            )
            self._validate_committed_record(
                self.record,
                committed,
                cleanup_proof=self._cleanup_proof,
                fencing_supervisor_epoch=self.fencing_supervisor_epoch,
            )
            self._committed_record = committed

        if not self._artifact_finalized:
            self._lifetime_lock.finalize_unlink()
            self._artifact_finalized = True
        return self._committed_record

    async def retry(self) -> AgentProcessRecord:
        """Continue the exact recovery, retaining this owner on any failure."""

        async with self._operation_lock:
            try:
                return await self._recover()
            except AgentProcessRecoveryError:
                raise
            except Exception as exc:
                raise AgentProcessRecoveryError(
                    "Agent process generation could not be safely recovered",
                    recovery_owner=self,
                ) from exc

    def close(self) -> None:
        """Release local descriptors while preserving the named retry point."""

        try:
            if self._process_group is not None:
                self._process_group.close()
        finally:
            if self._lifetime_lock is not None:
                self._lifetime_lock.close()


async def recover_agent_process_generation(
    store: AgentProcessRecoveryStore,
    record: AgentProcessRecord,
    *,
    fencing_supervisor_epoch: int,
    owner_instance_id: str,
    lifetime_lock_root: str | os.PathLike[str],
    timeout: float = 5.0,
    reason: str = "replacement_supervisor_recovery",
    clock: Callable[[], datetime] = _utc_now,
) -> AgentProcessRecord:
    """Recover one generation without spawning or enabling Agent scheduling."""

    owner = AgentProcessRecoveryOwner(
        store,
        record,
        fencing_supervisor_epoch=fencing_supervisor_epoch,
        owner_instance_id=owner_instance_id,
        lifetime_lock_root=lifetime_lock_root,
        timeout=timeout,
        reason=reason,
        clock=clock,
    )
    try:
        result = await owner.retry()
    except AgentProcessRecoveryError:
        # Retryable operational failures retain a strong owner on the error.
        raise
    except BaseException:
        # Control-flow exceptions must preserve their identity, but unlike the
        # wrapper error they cannot carry descriptor ownership to the caller.
        owner.close()
        raise
    else:
        owner.close()
        return result


__all__ = [
    "AgentProcessRecoveryError",
    "AgentProcessRecoveryEvidenceError",
    "AgentProcessRecoveryOwner",
    "AgentProcessRecoveryStore",
    "recover_agent_process_generation",
]
