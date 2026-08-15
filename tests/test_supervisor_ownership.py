"""Process-boundary tests for exclusive supervisor ownership."""

from __future__ import annotations

import asyncio
import json
import os
import select
import stat
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from src.runtime.sqlite_store import SQLiteStore
from src.codex_wechat_bot import AsyncLoopShutdownError, AsyncLoopThread
from src.runtime.supervisor import (
    SupervisorLockSecurityError,
    SupervisorOwnership,
    SupervisorOwnershipConflict,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

_HOLDER_PROGRAM = r"""
import sys

from src.runtime.supervisor import SupervisorOwnership

ownership = SupervisorOwnership(
    sys.argv[1],
    channel=sys.argv[2],
    bot_id=sys.argv[3],
    lock_root=sys.argv[4],
    owner_instance_id="holder-child",
)
try:
    ownership.acquire()
    print("READY", flush=True)
    sys.stdin.readline()
finally:
    ownership.close()
"""

_MARKER_CONTENDER_PROGRAM = r"""
import sys
from pathlib import Path

from src.runtime.supervisor import SupervisorOwnership, SupervisorOwnershipConflict

ownership = SupervisorOwnership(
    sys.argv[1],
    channel=sys.argv[2],
    bot_id=sys.argv[3],
    lock_root=sys.argv[4],
    owner_instance_id="contender-child",
)
try:
    with ownership:
        # This represents the first database-initialization side effect.  A
        # losing supervisor must never reach it.
        Path(sys.argv[5]).write_text("database initialized", encoding="utf-8")
except SupervisorOwnershipConflict as exc:
    print(f"CONFLICT:{exc.scope}", flush=True)
else:
    print("ACQUIRED", flush=True)
    raise SystemExit(3)
"""


def _subprocess_environment() -> dict[str, str]:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not existing
        else str(PROJECT_ROOT) + os.pathsep + existing
    )
    return environment


def _cleanup_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=5)


def _start_holder(
    database: Path,
    *,
    channel: str,
    bot_id: str,
    lock_root: Path,
) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            _HOLDER_PROGRAM,
            str(database),
            channel,
            bot_id,
            str(lock_root),
        ],
        cwd=PROJECT_ROOT,
        env=_subprocess_environment(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], 5)
    if not readable:
        _cleanup_process(process)
        pytest.fail("supervisor holder child did not become ready")
    line = process.stdout.readline().strip()
    if line != "READY":
        if process.poll() is None:
            process.kill()
        _, stderr = process.communicate(timeout=5)
        pytest.fail(f"supervisor holder failed before readiness: {line!r}; {stderr}")
    return process


def _graceful_stop(process: subprocess.Popen[str]) -> None:
    assert process.stdin is not None
    process.stdin.write("close\n")
    process.stdin.flush()
    return_code = process.wait(timeout=5)
    stderr = process.stderr.read() if process.stderr is not None else ""
    assert return_code == 0, stderr


def test_second_process_conflicts_on_the_same_database(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    holder = _start_holder(
        database,
        channel="wechat",
        bot_id="holder-bot",
        lock_root=lock_root,
    )
    try:
        contender = SupervisorOwnership(
            database,
            channel="wechat",
            bot_id="different-account",
            lock_root=lock_root,
        )
        with pytest.raises(SupervisorOwnershipConflict) as raised:
            contender.acquire()
        assert raised.value.scope == "database"
        assert not contender.held
    finally:
        _cleanup_process(holder)


def test_second_process_conflicts_on_the_same_channel_account_and_rolls_back(
    tmp_path: Path,
) -> None:
    holder_database = tmp_path / "holder.sqlite"
    contender_database = tmp_path / "contender.sqlite"
    lock_root = tmp_path / "locks"
    holder = _start_holder(
        holder_database,
        channel="wechat",
        bot_id="shared-bot",
        lock_root=lock_root,
    )
    try:
        contender = SupervisorOwnership(
            contender_database,
            channel="wechat",
            bot_id="shared-bot",
            lock_root=lock_root,
        )
        with pytest.raises(SupervisorOwnershipConflict) as raised:
            contender.acquire()
        assert raised.value.scope == "account"
        assert not contender.held

        # The account conflict happens after the contender has acquired its
        # database lock.  Losing the second lock must roll the first one back.
        with SupervisorOwnership(
            contender_database,
            channel="wechat",
            bot_id="independent-bot",
            lock_root=lock_root,
        ) as probe:
            assert probe.held
    finally:
        _cleanup_process(holder)


def test_conflict_happens_before_database_initialization_marker(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    marker = tmp_path / "database-initialized"
    lock_root = tmp_path / "locks"
    holder = _start_holder(
        database,
        channel="wechat",
        bot_id="holder-bot",
        lock_root=lock_root,
    )
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-u",
                "-c",
                _MARKER_CONTENDER_PROGRAM,
                str(database),
                "wechat",
                "contender-bot",
                str(lock_root),
                str(marker),
            ],
            cwd=PROJECT_ROOT,
            env=_subprocess_environment(),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "CONFLICT:database"
        assert not marker.exists()
        assert not database.exists()
    finally:
        _cleanup_process(holder)


@pytest.mark.parametrize("forced", [False, True], ids=["graceful", "forced"])
def test_child_exit_releases_both_locks(tmp_path: Path, forced: bool) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    holder = _start_holder(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
    )
    try:
        with pytest.raises(SupervisorOwnershipConflict):
            SupervisorOwnership(
                database,
                channel="wechat",
                bot_id="bot",
                lock_root=lock_root,
            ).acquire()

        if forced:
            holder.kill()
            assert holder.wait(timeout=5) != 0
        else:
            _graceful_stop(holder)

        with SupervisorOwnership(
            database,
            channel="wechat",
            bot_id="bot",
            lock_root=lock_root,
            owner_instance_id="replacement",
        ) as replacement:
            assert replacement.held
    finally:
        _cleanup_process(holder)


def test_lock_files_are_private_stable_rendezvous_points(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    first = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
        owner_instance_id="first-owner",
    )
    first.acquire()
    paths = first.paths
    try:
        assert first._database_descriptor is not None
        assert first._account_descriptor is not None
        assert not os.get_inheritable(first._database_descriptor)
        assert not os.get_inheritable(first._account_descriptor)
        assert stat.S_IMODE(paths.root.stat().st_mode) == 0o700
        first_inodes: dict[Path, tuple[int, int]] = {}
        for path, scope in ((paths.database, "database"), (paths.account, "account")):
            details = path.stat()
            assert stat.S_ISREG(details.st_mode)
            assert stat.S_IMODE(details.st_mode) == 0o600
            if hasattr(os, "getuid"):
                assert details.st_uid == os.getuid()
            first_inodes[path] = (details.st_dev, details.st_ino)
            assert json.loads(path.read_text(encoding="utf-8")) == {
                "owner_instance_id": "first-owner",
                "pid": os.getpid(),
                "scope": scope,
            }
    finally:
        first.close()

    assert paths.database.exists()
    assert paths.account.exists()
    second = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
        owner_instance_id="second-owner",
    )
    assert second.paths == paths
    with second:
        for path, scope in ((paths.database, "database"), (paths.account, "account")):
            details = path.stat()
            assert (details.st_dev, details.st_ino) == first_inodes[path]
            assert stat.S_IMODE(details.st_mode) == 0o600
            assert json.loads(path.read_text(encoding="utf-8")) == {
                "owner_instance_id": "second-owner",
                "pid": os.getpid(),
                "scope": scope,
            }


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_forked_child_cannot_keep_parent_ownership_alive(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    ownership = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
    ).acquire()
    child_ready_read, child_ready_write = os.pipe()
    child_exit_read, child_exit_write = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:  # pragma: no cover - assertions run in the parent
        try:
            os.close(child_ready_read)
            os.close(child_exit_write)
            # The registered after-fork hook must clear the inherited object
            # state as well as close the raw duplicates, preventing a later
            # destructor from acting on a reused descriptor number.
            status = b"cleared" if not ownership.held else b"held"
            os.write(child_ready_write, status)
            os.read(child_exit_read, 1)
        finally:
            os._exit(0)

    os.close(child_ready_write)
    os.close(child_exit_read)
    try:
        assert os.read(child_ready_read, 7) == b"cleared"
        ownership.close()

        # The child is deliberately still alive.  If it retained either
        # inherited lock descriptor, this replacement would conflict.
        with SupervisorOwnership(
            database,
            channel="wechat",
            bot_id="bot",
            lock_root=lock_root,
        ) as replacement:
            assert replacement.held
    finally:
        try:
            os.write(child_exit_write, b"x")
        except OSError:
            pass
        os.close(child_ready_read)
        os.close(child_exit_write)
        waited_pid, status = os.waitpid(child_pid, 0)
        assert waited_pid == child_pid
        assert os.waitstatus_to_exitcode(status) == 0
        ownership.close()


def test_symlink_lock_root_is_rejected(tmp_path: Path) -> None:
    actual_root = tmp_path / "actual-locks"
    actual_root.mkdir(mode=0o700)
    actual_root.chmod(0o700)
    linked_root = tmp_path / "linked-locks"
    try:
        linked_root.symlink_to(actual_root, target_is_directory=True)
    except OSError as exc:  # pragma: no cover - supported on the target POSIX host
        pytest.skip(f"symlinks unavailable: {exc}")

    ownership = SupervisorOwnership(
        tmp_path / "runtime.sqlite",
        channel="wechat",
        bot_id="bot",
        lock_root=linked_root,
    )
    with pytest.raises(SupervisorLockSecurityError, match="not a directory"):
        ownership.acquire()


def test_symlink_lock_file_is_rejected(tmp_path: Path) -> None:
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    lock_root.chmod(0o700)
    ownership = SupervisorOwnership(
        tmp_path / "runtime.sqlite",
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
    )
    target = tmp_path / "attacker-controlled"
    target.write_text("do not overwrite", encoding="utf-8")
    try:
        ownership.paths.database.symlink_to(target)
    except OSError as exc:  # pragma: no cover - supported on the target POSIX host
        pytest.skip(f"symlinks unavailable: {exc}")

    with pytest.raises(SupervisorLockSecurityError, match="not a regular file"):
        ownership.acquire()
    assert target.read_text(encoding="utf-8") == "do not overwrite"


def test_non_regular_lock_file_is_rejected(tmp_path: Path) -> None:
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    lock_root.chmod(0o700)
    ownership = SupervisorOwnership(
        tmp_path / "runtime.sqlite",
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
    )
    os.mkfifo(ownership.paths.database, mode=0o600)

    with pytest.raises(SupervisorLockSecurityError, match="not a regular file"):
        ownership.acquire()


def test_owner_process_can_use_multiple_store_connections(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite"
    ownership = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=tmp_path / "locks",
    )

    async def scenario() -> None:
        first = SQLiteStore(database)
        second = SQLiteStore(database)
        try:
            await first.initialize()
            await second.initialize()
            await first.save_cursor(channel="wechat", bot_id="bot", cursor="0001")
            assert await second.get_cursor(channel="wechat", bot_id="bot") == "0001"
            await second.save_cursor(channel="wechat", bot_id="bot", cursor="0002")
            assert await first.get_cursor(channel="wechat", bot_id="bot") == "0002"
            assert (await first.health())["path"] == str(database.resolve())
            assert (await second.health())["path"] == str(database.resolve())
        finally:
            await second.close()
            await first.close()

    with ownership:
        asyncio.run(scenario())


def test_live_async_loop_retains_supervisor_ownership_until_thread_exits(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    release = threading.Event()
    started = threading.Event()
    loop_thread = AsyncLoopThread()
    ownership = SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
    )

    async def swallow_cancellation() -> None:
        started.set()
        while not release.is_set():
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                continue

    try:
        loop_thread.start()
        asyncio.run_coroutine_threadsafe(
            swallow_cancellation(), loop_thread.loop
        )
        assert started.wait(timeout=2)

        with pytest.raises(AsyncLoopShutdownError):
            with ownership:
                loop_thread.stop(timeout=0.05)

        assert loop_thread._thread.is_alive()
        assert ownership.held
        contender = SupervisorOwnership(
            database,
            channel="wechat",
            bot_id="bot",
            lock_root=lock_root,
        )
        with pytest.raises(SupervisorOwnershipConflict):
            contender.acquire()
    finally:
        release.set()
        loop_thread.stop(timeout=2)
        ownership.close()
