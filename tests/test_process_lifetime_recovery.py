"""Recovery and filesystem-security tests for named Agent lifetime locks."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from src.runtime.agent_process import AgentProcessHandle, AgentProcessIdentity
from src.runtime.process_lifetime import (
    InheritedLifetimeLock,
    ProcessIdentityMismatchError,
    ProcessInspectionUnavailableError,
    ProcessLifetimeStillHeldError,
    RecoveredLifetimeLock,
)


IDENTITY = AgentProcessIdentity(
    supervisor_epoch=91,
    agent_id="lifetime-recovery",
    agent_incarnation=1,
    worker_generation=1,
)


def test_spawn_uses_explicit_named_root_and_unlinks_only_after_release(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"

    async def scenario() -> None:
        handle = await AgentProcessHandle.spawn(
            IDENTITY,
            lifetime_lock_root=lock_root,
            startup_timeout=3,
            exit_timeout=3,
        )
        lock_path = Path(handle.lifetime_lock_path)
        try:
            assert lock_path.parent == lock_root
            assert lock_path.exists()
            metadata = lock_path.stat()
            assert stat.S_IMODE(metadata.st_mode) == 0o600
            assert metadata.st_nlink == 1
            assert await handle.shutdown() == 0
            assert not lock_path.exists()
        finally:
            if not handle.lifetime_lock_released:
                await handle.terminate()

    asyncio.run(scenario())
    assert stat.S_IMODE(lock_root.stat().st_mode) == 0o700


def test_replacement_reopens_exact_lock_after_supervisor_crash(
    tmp_path: Path,
) -> None:
    lock_root = (tmp_path / "locks").resolve()
    project_root = Path(__file__).parents[1]
    program = '''
import asyncio, json
from src.runtime.agent_process import AgentProcessHandle, AgentProcessIdentity
async def main():
    handle = await AgentProcessHandle.spawn(
        AgentProcessIdentity(92, "replacement-crash", 1, 1),
        lifetime_lock_root={lock_root!r},
        startup_timeout=3,
        exit_timeout=2,
    )
    print(json.dumps({{
        "child_pid": handle.pid,
        "identity": handle.lifetime_lock_identity,
        "path": handle.lifetime_lock_path,
    }}), flush=True)
    await asyncio.Event().wait()
asyncio.run(main())
'''.format(lock_root=str(lock_root))
    supervisor = subprocess.Popen(
        [sys.executable, "-u", "-c", program],
        cwd=project_root,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    replacement: InheritedLifetimeLock | None = None
    child_pid: int | None = None
    lock_path: Path | None = None
    try:
        assert supervisor.stdout is not None
        line = supervisor.stdout.readline().strip()
        assert line, supervisor.stderr.read() if supervisor.stderr else ""
        evidence = json.loads(line)
        child_pid = int(evidence["child_pid"])
        identity = str(evidence["identity"])
        lock_path = Path(evidence["path"])
        assert lock_path.exists()

        before_busy_probe = len(tuple(Path("/proc/self/fd").iterdir()))
        with pytest.raises(
            ProcessLifetimeStillHeldError,
            match="prior Agent lifetime lock remains held",
        ):
            InheritedLifetimeLock.reopen(lock_root, identity)
        assert len(tuple(Path("/proc/self/fd").iterdir())) == before_busy_probe

        os.kill(supervisor.pid, signal.SIGKILL)
        supervisor.wait(timeout=3)
        for _ in range(200):
            try:
                replacement = InheritedLifetimeLock.reopen(lock_root, identity)
                break
            except ProcessLifetimeStillHeldError:
                time.sleep(0.01)
        assert replacement is not None
        assert replacement.identity == identity
        assert replacement.path == str(lock_path)
        replacement.release_parent_copy()
        assert replacement.prove_released()
        assert not lock_path.exists()
    finally:
        if supervisor.poll() is None:
            os.kill(supervisor.pid, signal.SIGKILL)
            supervisor.wait(timeout=3)
        if replacement is not None and not replacement.released:
            replacement.release_parent_copy()
            replacement.prove_released()


def test_named_identity_rejects_hardlink_alias(
    tmp_path: Path,
) -> None:
    lock_root = (tmp_path / "locks").resolve()
    lock = InheritedLifetimeLock.create(lock_root)
    alias = lock_root / "hardlink-alias"
    os.link(lock.path, alias)
    try:
        with pytest.raises(ProcessIdentityMismatchError, match="link metadata"):
            InheritedLifetimeLock.reopen(lock_root, lock.identity)
        lock.release_parent_copy()
        with pytest.raises(ProcessIdentityMismatchError, match="link metadata"):
            lock.prove_released()
    finally:
        alias.unlink(missing_ok=True)
    assert lock.prove_released()
    assert not Path(lock.path).exists()


def test_reopen_rejects_symlink_replacement_and_root_inode_drift(
    tmp_path: Path,
) -> None:
    lock_root = (tmp_path / "locks").resolve()
    lock = InheritedLifetimeLock.create(lock_root)
    identity = lock.identity
    lock_path = Path(lock.path)
    lock.release_parent_copy()
    assert lock.prove_released()

    target = lock_root / "target"
    target.write_text("not a lock", encoding="utf-8")
    target.chmod(0o600)
    lock_path.symlink_to(target.name)
    try:
        with pytest.raises(ProcessIdentityMismatchError, match="missing or unsafe"):
            InheritedLifetimeLock.reopen(lock_root, identity)
    finally:
        lock_path.unlink(missing_ok=True)
        target.unlink(missing_ok=True)

    other_root = (tmp_path / "other-locks").resolve()
    other_root.mkdir(mode=0o700)
    with pytest.raises(ProcessIdentityMismatchError, match="root identity changed"):
        InheritedLifetimeLock.reopen(other_root, identity)


def test_lock_root_rejects_symlinks_and_nonprivate_permissions(
    tmp_path: Path,
) -> None:
    real_root = tmp_path / "real-locks"
    real_root.mkdir(mode=0o700)
    symlink_root = tmp_path / "linked-locks"
    symlink_root.symlink_to(real_root, target_is_directory=True)
    with pytest.raises(
        ProcessInspectionUnavailableError,
        match="contains a symlink",
    ):
        InheritedLifetimeLock.create(symlink_root)

    unsafe_root = tmp_path / "unsafe-locks"
    unsafe_root.mkdir(mode=0o755)
    with pytest.raises(
        ProcessInspectionUnavailableError,
        match="not a private owned directory",
    ):
        InheritedLifetimeLock.create(unsafe_root)


def test_recovered_unlink_fsync_failure_is_same_owner_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_root = (tmp_path / "locks").resolve()
    lock = InheritedLifetimeLock.create(lock_root)
    artifact = Path(lock.path)
    identity = lock.identity
    lock.release_parent_copy()

    recovered = RecoveredLifetimeLock.reopen(lock_root, identity)
    assert recovered.try_prove_released()
    root_fd = recovered._root_fd
    assert root_fd is not None
    real_fsync = os.fsync
    failed = False

    def fail_first_directory_fsync(descriptor: int) -> None:
        nonlocal failed
        if descriptor == root_fd and not failed:
            failed = True
            raise OSError("injected directory fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_first_directory_fsync)
    try:
        with pytest.raises(OSError, match="injected directory fsync failure"):
            recovered.finalize_unlink()
        assert failed
        assert not artifact.exists()
        assert not recovered.finalized

        # This exact owner knows its unlink succeeded, so it retries only the
        # directory durability step and then closes both retained descriptors.
        recovered.finalize_unlink()
        assert recovered.finalized
        assert recovered._probe_fd is None
        assert recovered._root_fd is None
    finally:
        recovered.close()


def test_inherited_unlink_fsync_failure_is_same_owner_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = InheritedLifetimeLock.create((tmp_path / "locks").resolve())
    artifact = Path(lock.path)
    lock.release_parent_copy()
    root_fd = lock._root_fd
    assert root_fd is not None
    real_fsync = os.fsync
    failed = False

    def fail_first_directory_fsync(descriptor: int) -> None:
        nonlocal failed
        if descriptor == root_fd and not failed:
            failed = True
            raise OSError("injected directory fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_first_directory_fsync)
    with pytest.raises(OSError, match="injected directory fsync failure"):
        lock.prove_released()
    assert not artifact.exists()
    assert not lock.released
    assert lock.prove_released()
    assert lock.released
    assert lock._probe_fd is None
    assert lock._root_fd is None


def test_recovered_finalize_rejects_removal_before_own_unlink(
    tmp_path: Path,
) -> None:
    lock_root = (tmp_path / "locks").resolve()
    lock = InheritedLifetimeLock.create(lock_root)
    artifact = Path(lock.path)
    identity = lock.identity
    lock.release_parent_copy()

    recovered = RecoveredLifetimeLock.reopen(lock_root, identity)
    try:
        assert recovered.try_prove_released()
        artifact.unlink()
        with pytest.raises(
            ProcessIdentityMismatchError,
            match="(?:name disappeared|link metadata)",
        ):
            recovered.finalize_unlink()
        assert not recovered.finalized
        assert not recovered._name_unlinked
    finally:
        recovered.close()
