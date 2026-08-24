"""Durable supervisor epoch and startup-ordering regressions."""

from __future__ import annotations

import asyncio
import os
import select
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError
from src.runtime.supervisor import SupervisorOwnership


PROJECT_ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 8, 15, 1, 0, tzinfo=timezone.utc)


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
        "inputs": {"text": "recover only after the epoch"},
    }


def _subprocess_environment() -> dict[str, str]:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not existing
        else str(PROJECT_ROOT) + os.pathsep + existing
    )
    return environment


def test_owned_initialization_defers_recovery_until_epoch_activation(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        task = await seeded.create_task(_task("epoch-recovery-task"))
        claim = await seeded.claim_task_by_id(task.task_id, "old-worker")
        assert claim is not None
        await seeded.close()

        owned = SQLiteStore(path)
        await owned.initialize(recover_startup_state=False)
        try:
            with sqlite3.connect(path) as connection:
                assert connection.execute(
                    "SELECT state FROM tasks WHERE task_id=?", (task.task_id,)
                ).fetchone() == ("dispatching",)
                assert connection.execute(
                    "SELECT COUNT(*) FROM supervisor_epochs"
                ).fetchone() == (0,)

            # No store operation can cross the deferred-recovery barrier.
            with pytest.raises(StoreError, match="epoch activation is required"):
                await owned.health()

            epoch = await owned.activate_supervisor_epoch(
                owner_instance_id="owned-supervisor-1",
                channel="wechat",
                bot_id="bot",
            )
            assert epoch.epoch == 1
            assert epoch.active

            recovered = await owned.get_task(task.task_id)
            assert recovered is not None
            assert recovered.state.value == "orphaned"
            report = await owned.startup_reconcile()
            assert report.tasks_orphaned == 1

            # A same-instance retry is idempotent and cannot recover twice or
            # consume another epoch number.
            replay = await owned.activate_supervisor_epoch(
                owner_instance_id="owned-supervisor-1",
                channel="wechat",
                bot_id="bot",
            )
            assert replay == epoch
            with sqlite3.connect(path) as connection:
                assert connection.execute(
                    "SELECT COUNT(*), MAX(epoch) FROM supervisor_epochs"
                ).fetchone() == (1, 1)
        finally:
            await owned.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT stopped_at IS NOT NULL, stop_reason "
                "FROM supervisor_epochs WHERE epoch=1"
            ).fetchone() == (1, "store_closed")

        replacement = SQLiteStore(path)
        await replacement.initialize(recover_startup_state=False)
        try:
            second = await replacement.activate_supervisor_epoch(
                owner_instance_id="owned-supervisor-2",
                channel="wechat",
                bot_id="bot",
            )
            assert second.epoch == 2
            assert await replacement.finish_supervisor_epoch(
                second.epoch,
                owner_instance_id="owned-supervisor-2",
            )
            assert not await replacement.finish_supervisor_epoch(
                second.epoch,
                owner_instance_id="owned-supervisor-2",
            )
        finally:
            await replacement.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT stop_reason FROM supervisor_epochs WHERE epoch=2"
            ).fetchone() == ("graceful_shutdown",)

    asyncio.run(scenario())


def test_deferred_recovery_requires_explicit_initialization(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            with pytest.raises(
                StoreError, match="cannot be deferred after initialization"
            ):
                await store.initialize(recover_startup_state=False)
            with pytest.raises(
                StoreError, match="requires deferred startup recovery"
            ):
                await store.activate_supervisor_epoch(
                    owner_instance_id="late-owner",
                    channel="wechat",
                    bot_id="bot",
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_supervisor_epoch_timestamps_are_normalized_and_ordered(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize(recover_startup_state=False)
        try:
            with pytest.raises(ValueError, match="ISO-8601"):
                await store.activate_supervisor_epoch(
                    owner_instance_id="invalid-time-owner",
                    channel="wechat",
                    bot_id="bot",
                    started_at="not-a-timestamp",
                )

            epoch = await store.activate_supervisor_epoch(
                owner_instance_id="ordered-time-owner",
                channel="wechat",
                bot_id="bot",
                started_at=T0,
            )
            assert epoch.started_at == T0
            with pytest.raises(InvalidTransition, match="predates its start"):
                await store.finish_supervisor_epoch(
                    epoch.epoch,
                    owner_instance_id="ordered-time-owner",
                    stopped_at=T0 - timedelta(seconds=1),
                )
            assert await store.finish_supervisor_epoch(
                epoch.epoch,
                owner_instance_id="ordered-time-owner",
                stopped_at=T0 + timedelta(seconds=1),
                reason="timestamp_test",
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_deferred_recovery_gate_covers_every_process_local_store(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path)
        second = SQLiteStore(path)
        try:
            await first.initialize(recover_startup_state=False)
            # An ordinary later opener joins the existing database-wide fence;
            # it must neither recover independently nor become usable early.
            await second.initialize()
            for store in (first, second):
                with pytest.raises(
                    StoreError, match="epoch activation is required"
                ):
                    await store.health()

            entered_recovery = threading.Event()
            release_recovery = threading.Event()
            original_recovery = first._strong_startup_recovery_tx

            def paused_recovery(conn, *, now_text):
                entered_recovery.set()
                if not release_recovery.wait(timeout=5):
                    raise RuntimeError("test did not release startup recovery")
                return original_recovery(conn, now_text=now_text)

            first._strong_startup_recovery_tx = paused_recovery
            activation = asyncio.create_task(
                first.activate_supervisor_epoch(
                    owner_instance_id="shared-gate-owner",
                    channel="wechat",
                    bot_id="bot",
                )
            )
            assert await asyncio.to_thread(entered_recovery.wait, 2)

            # The second adapter waits on its own executor while the epoch and
            # strong recovery transaction is uncommitted.  It cannot observe a
            # prematurely removed process-local gate.
            second_health = asyncio.create_task(second.health())
            await asyncio.sleep(0.05)
            assert not second_health.done()

            release_recovery.set()
            epoch = await activation
            assert epoch.owner_instance_id == "shared-gate-owner"
            assert (await second_health)["path"] == str(path.resolve())
            assert (await first.health())["path"] == str(path.resolve())
        finally:
            # Never leave the deliberately paused executor behind on failure.
            release = locals().get("release_recovery")
            if release is not None:
                release.set()
            await second.close()
            await first.close()

    asyncio.run(scenario())


def test_epoch_owning_store_must_close_after_joined_adapters(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        owner = SQLiteStore(path)
        peer = SQLiteStore(path)
        await owner.initialize(recover_startup_state=False)
        epoch = await owner.activate_supervisor_epoch(
            owner_instance_id="close-order-owner",
            channel="wechat",
            bot_id="bot",
        )
        await peer.initialize()

        with pytest.raises(StoreError, match="must close after"):
            await owner.close()
        assert (await owner.health())["path"] == str(path.resolve())
        await peer.save_cursor(channel="wechat", bot_id="bot", cursor="before-close")
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT stopped_at FROM supervisor_epochs WHERE epoch=?",
                (epoch.epoch,),
            ).fetchone() == (None,)

        await peer.close()
        await owner.close()
        with sqlite3.connect(path) as connection:
            stopped, reason = connection.execute(
                "SELECT stopped_at, stop_reason FROM supervisor_epochs WHERE epoch=?",
                (epoch.epoch,),
            ).fetchone()
            assert stopped is not None
            assert reason == "store_closed"

    asyncio.run(scenario())


def test_explicit_epoch_finish_requires_local_owner_and_last_adapter(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        owner = SQLiteStore(path)
        peer = SQLiteStore(path)
        await owner.initialize(recover_startup_state=False)
        epoch = await owner.activate_supervisor_epoch(
            owner_instance_id="explicit-finish-owner",
            channel="wechat",
            bot_id="bot",
        )
        await peer.initialize()

        with pytest.raises(StoreError, match="not owned by this store"):
            await peer.finish_supervisor_epoch(
                epoch.epoch,
                owner_instance_id="explicit-finish-owner",
            )
        with pytest.raises(StoreError, match="must finish after"):
            await owner.finish_supervisor_epoch(
                epoch.epoch,
                owner_instance_id="explicit-finish-owner",
            )
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT stopped_at FROM supervisor_epochs WHERE epoch=?",
                (epoch.epoch,),
            ).fetchone() == (None,)

        await peer.close()
        assert await owner.finish_supervisor_epoch(
            epoch.epoch,
            owner_instance_id="explicit-finish-owner",
        )
        # Exact same-store replay remains idempotent after the shutdown gate is
        # installed; another adapter cannot gain this authority from DB text.
        assert not await owner.finish_supervisor_epoch(
            epoch.epoch,
            owner_instance_id="explicit-finish-owner",
        )
        await owner.close()

    asyncio.run(scenario())


def test_explicit_epoch_finish_fences_finish_close_window(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        owner = SQLiteStore(path)
        await owner.initialize(recover_startup_state=False)
        epoch = await owner.activate_supervisor_epoch(
            owner_instance_id="finish-window-owner",
            channel="wechat",
            bot_id="bot",
        )
        assert await owner.finish_supervisor_epoch(
            epoch.epoch,
            owner_instance_id="finish-window-owner",
            reason="explicit_shutdown",
        )

        # An opener which joins before final close must inherit the shutdown
        # gate.  It cannot write or activate a replacement beneath the owner.
        late = SQLiteStore(path)
        await late.initialize()
        with pytest.raises(StoreError, match="epoch activation is required"):
            await late.save_cursor(
                channel="wechat",
                bot_id="bot",
                cursor="must-not-commit",
            )
        with pytest.raises(StoreError, match="must close before"):
            await late.activate_supervisor_epoch(
                owner_instance_id="finish-window-takeover",
                channel="wechat",
                bot_id="bot",
            )

        # Retaining local epoch ownership makes the recent final-close fence
        # effective even after an explicit finish.
        with pytest.raises(StoreError, match="must close after"):
            await owner.close()
        await late.close()
        await owner.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT stop_reason FROM supervisor_epochs WHERE epoch=?",
                (epoch.epoch,),
            ).fetchone() == ("explicit_shutdown",)
            assert connection.execute(
                "SELECT COUNT(*) FROM channel_cursors"
            ).fetchone() == (0,)

        replacement = SQLiteStore(path)
        await replacement.initialize()
        try:
            assert (await replacement.health())["path"] == str(path.resolve())
        finally:
            await replacement.close()

    asyncio.run(scenario())


def test_final_epoch_close_serializes_against_a_new_opener(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        owner = SQLiteStore(path)
        await owner.initialize(recover_startup_state=False)
        await owner.activate_supervisor_epoch(
            owner_instance_id="closing-owner",
            channel="wechat",
            bot_id="bot",
        )

        entered_finish = threading.Event()
        release_finish = threading.Event()
        original_timestamp = owner._process_timestamp

        def paused_timestamp(value, name):
            if name == "stopped_at":
                entered_finish.set()
                if not release_finish.wait(timeout=5):
                    raise RuntimeError("test did not release epoch close")
            return original_timestamp(value, name)

        owner._process_timestamp = paused_timestamp
        close_task = asyncio.create_task(owner.close())
        assert await asyncio.to_thread(entered_finish.wait, 2)

        replacement = SQLiteStore(path)
        open_task = asyncio.create_task(replacement.initialize())
        await asyncio.sleep(0.05)
        assert not open_task.done()

        release_finish.set()
        await close_task
        await open_task
        try:
            assert (await replacement.health())["path"] == str(path.resolve())
        finally:
            await replacement.close()

    asyncio.run(scenario())


def test_cannot_introduce_deferred_gate_after_ordinary_live_connection(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        ordinary = SQLiteStore(path)
        late = SQLiteStore(path)
        try:
            await ordinary.initialize()
            with pytest.raises(
                StoreError, match="ordinary live connection exists"
            ):
                await late.initialize(recover_startup_state=False)
            assert (await ordinary.health())["path"] == str(path.resolve())
        finally:
            await late.close()
            await ordinary.close()

    asyncio.run(scenario())


def test_losing_contender_neither_recovers_nor_advances_epoch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"

    async def seed_claim() -> None:
        store = SQLiteStore(path)
        await store.initialize()
        task = await store.create_task(_task("contender-fence-task"))
        assert await store.claim_task_by_id(task.task_id, "old-worker") is not None
        await store.close()

    asyncio.run(seed_claim())

    contender_program = r"""
import asyncio
import sys

from src.runtime.sqlite_store import SQLiteStore
from src.runtime.supervisor import SupervisorOwnership, SupervisorOwnershipConflict

ownership = SupervisorOwnership(
    sys.argv[1],
    channel="wechat",
    bot_id="other-bot",
    lock_root=sys.argv[2],
)
try:
    ownership.acquire()
except SupervisorOwnershipConflict as exc:
    print(f"CONFLICT:{exc.scope}")
else:
    async def activate():
        store = SQLiteStore(sys.argv[1])
        await store.initialize(recover_startup_state=False)
        await store.activate_supervisor_epoch(
            owner_instance_id="losing-contender",
            channel="wechat",
            bot_id="other-bot",
        )
        await store.close()
    asyncio.run(activate())
    ownership.close()
    raise SystemExit(3)
"""

    with SupervisorOwnership(
        path,
        channel="wechat",
        bot_id="holder-bot",
        lock_root=lock_root,
    ):
        result = subprocess.run(
            [sys.executable, "-u", "-c", contender_program, str(path), str(lock_root)],
            cwd=PROJECT_ROOT,
            env=_subprocess_environment(),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "CONFLICT:database"

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT COUNT(*) FROM supervisor_epochs"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT state FROM tasks WHERE task_id='contender-fence-task'"
            ).fetchone() == ("dispatching",)


def test_crashed_owner_is_superseded_by_exactly_one_replacement_epoch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    crashed_owner_program = r"""
import asyncio
import sys

from src.runtime.sqlite_store import SQLiteStore
from src.runtime.supervisor import SupervisorOwnership

ownership = SupervisorOwnership(
    sys.argv[1],
    channel="wechat",
    bot_id="bot",
    lock_root=sys.argv[2],
    owner_instance_id="crashed-owner",
).acquire()
store = SQLiteStore(sys.argv[1])

async def start():
    await store.initialize(recover_startup_state=False)
    epoch = await store.activate_supervisor_epoch(
        owner_instance_id=ownership.owner_instance_id,
        channel=ownership.channel,
        bot_id=ownership.bot_id,
    )
    task = await store.create_task({
        "task_id": "crash-takeover-task",
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
        "inputs": {"text": "crash before close"},
    })
    assert await store.claim_task_by_id(task.task_id, "crashed-worker") is not None
    print(f"READY:{epoch.epoch}", flush=True)

asyncio.run(start())
sys.stdin.readline()
"""

    child = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            crashed_owner_program,
            str(path),
            str(lock_root),
        ],
        cwd=PROJECT_ROOT,
        env=_subprocess_environment(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None
    readable, _, _ = select.select([child.stdout], [], [], 10)
    if not readable:
        child.kill()
        _, stderr = child.communicate(timeout=5)
        pytest.fail(f"crashed owner did not become ready: {stderr}")
    assert child.stdout.readline().strip() == "READY:1"
    child.kill()
    _, child_stderr = child.communicate(timeout=5)
    assert child.returncode is not None and child.returncode != 0, child_stderr

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT epoch, stopped_at, stop_reason FROM supervisor_epochs"
        ).fetchone() == (1, None, None)
        assert connection.execute(
            "SELECT state FROM tasks WHERE task_id='crash-takeover-task'"
        ).fetchone() == ("dispatching",)

    ownership = SupervisorOwnership(
        path,
        channel="wechat",
        bot_id="bot",
        lock_root=lock_root,
        owner_instance_id="replacement-owner",
    )

    async def replace() -> None:
        store = SQLiteStore(path)
        await store.initialize(recover_startup_state=False)
        try:
            epoch = await store.activate_supervisor_epoch(
                owner_instance_id=ownership.owner_instance_id,
                channel=ownership.channel,
                bot_id=ownership.bot_id,
            )
            assert epoch.epoch == 2
            recovered = await store.get_task("crash-takeover-task")
            assert recovered is not None
            assert recovered.state.value == "orphaned"
            replay = await store.activate_supervisor_epoch(
                owner_instance_id=ownership.owner_instance_id,
                channel=ownership.channel,
                bot_id=ownership.bot_id,
            )
            assert replay.epoch == 2
        finally:
            await store.close()

    with ownership:
        asyncio.run(replace())

    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT epoch, owner_instance_id, stopped_at IS NOT NULL, stop_reason "
            "FROM supervisor_epochs ORDER BY epoch"
        ).fetchall()
        assert rows == [
            (1, "crashed-owner", 1, "superseded"),
            (2, "replacement-owner", 1, "store_closed"),
        ]


def test_migrations_23_and_24_are_idempotent_over_a_schema_22_database(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        await created.close()

        with sqlite3.connect(path) as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version>=23")
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        await migrated.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (35,)
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(supervisor_epochs)"
                ).fetchall()
            }
            assert columns == {
                "epoch",
                "owner_instance_id",
                "channel",
                "bot_id",
                "started_at",
                "stopped_at",
                "stop_reason",
            }

    asyncio.run(scenario())
