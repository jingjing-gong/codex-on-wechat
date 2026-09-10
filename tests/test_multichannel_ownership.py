"""Focused ownership tests for one database shared by several channel accounts."""

from __future__ import annotations

import json
import os
import stat
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.runtime.supervisor as supervisor
from src.runtime.supervisor import (
    ChannelAccountOwnership,
    DatabaseOwnership,
    MultiAccountOwnership,
    SupervisorAccountSetOwnership,
    SupervisorOwnershipConflict,
    SupervisorOwnershipError,
    SupervisorResourcesStillLive,
)


def test_account_set_is_canonical_sorted_deduplicated_and_private(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"
    ownership = SupervisorAccountSetOwnership(
        tmp_path / "runtime.sqlite",
        accounts=(
            ("wechat", "wechat-bot"),
            SimpleNamespace(channel=" lark ", bot_id="bot-b"),
            ("lark", "bot-a"),
            ("lark", "bot-b"),
        ),
        lock_root=lock_root,
        owner_instance_id="multi-owner",
    )

    assert ownership.accounts == (
        ("lark", "bot-a"),
        ("lark", "bot-b"),
        ("wechat", "wechat-bot"),
    )
    assert [
        (item.channel, item.bot_id) for item in ownership.account_ownerships
    ] == list(ownership.accounts)
    assert ownership.database_ownership.owner_instance_id == "multi-owner"
    assert {
        item.owner_instance_id for item in ownership.account_ownerships
    } == {"multi-owner"}

    with ownership:
        assert ownership.held
        assert stat.S_IMODE(lock_root.stat().st_mode) == 0o700
        for path, scope in (
            (ownership.paths.database, "database"),
            *((path, "account") for path in ownership.paths.accounts),
        ):
            details = path.stat()
            assert stat.S_ISREG(details.st_mode)
            assert stat.S_IMODE(details.st_mode) == 0o600
            assert json.loads(path.read_text(encoding="utf-8")) == {
                "owner_instance_id": "multi-owner",
                "pid": os.getpid(),
                "scope": scope,
            }

    assert not ownership.held
    with DatabaseOwnership(
        tmp_path / "runtime.sqlite",
        lock_root=lock_root,
    ) as replacement:
        assert replacement.held


@pytest.mark.parametrize(
    "accounts, error",
    [
        ((), "at least one"),
        (("not-a-pair",), "channel, bot_id"),
        ((("lark", ""),), "bot_id is required"),
    ],
)
def test_account_set_rejects_missing_or_malformed_accounts(
    tmp_path: Path,
    accounts,
    error: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=error):
        SupervisorAccountSetOwnership(
            tmp_path / "runtime.sqlite",
            accounts=accounts,
            lock_root=tmp_path / "locks",
        )


def test_mid_set_conflict_rolls_back_database_and_prior_accounts(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"
    with ChannelAccountOwnership(
        channel="lark",
        bot_id="bot-b",
        lock_root=lock_root,
    ):
        ownership = SupervisorAccountSetOwnership(
            tmp_path / "runtime.sqlite",
            accounts=(("lark", "bot-c"), ("lark", "bot-b"), ("lark", "bot-a")),
            lock_root=lock_root,
        )

        with pytest.raises(SupervisorOwnershipConflict) as raised:
            ownership.acquire()
        assert raised.value.scope == "account"
        assert not ownership.held
        assert not ownership.database_ownership.held
        assert not any(item.held for item in ownership.account_ownerships)

        # The database and bot-a were acquired before the bot-b conflict.  Both
        # must already be available again, while the external bot-b holder lives.
        with DatabaseOwnership(
            tmp_path / "runtime.sqlite",
            lock_root=lock_root,
        ) as database_probe:
            assert database_probe.held
        with ChannelAccountOwnership(
            channel="lark",
            bot_id="bot-a",
            lock_root=lock_root,
        ) as account_probe:
            assert account_probe.held


def test_acquisition_and_rollback_use_deterministic_reverse_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class FakeDatabaseOwnership:
        def __init__(self, _database, *, lock_root, owner_instance_id):
            self.root = Path(lock_root)
            self.path = self.root / "database.lock"
            self.owner_instance_id = owner_instance_id
            self.held = False

        def acquire(self):
            events.append("acquire:database")
            self.held = True

        def close(self):
            if self.held:
                events.append("close:database")
            self.held = False

    class FakeAccountOwnership:
        def __init__(self, *, channel, bot_id, lock_root, owner_instance_id):
            del owner_instance_id
            self.channel = channel
            self.bot_id = bot_id
            self.path = Path(lock_root) / f"{channel}-{bot_id}.lock"
            self.held = False

        def acquire(self):
            events.append(f"acquire:{self.channel}/{self.bot_id}")
            if self.bot_id == "bot-b":
                raise SupervisorOwnershipConflict("account")
            self.held = True

        def close(self):
            if self.held:
                events.append(f"close:{self.channel}/{self.bot_id}")
            self.held = False

    monkeypatch.setattr(supervisor, "DatabaseOwnership", FakeDatabaseOwnership)
    monkeypatch.setattr(supervisor, "ChannelAccountOwnership", FakeAccountOwnership)

    ownership = SupervisorAccountSetOwnership(
        tmp_path / "runtime.sqlite",
        accounts=(("wechat", "bot-z"), ("lark", "bot-b"), ("lark", "bot-a")),
        lock_root=tmp_path / "locks",
    )
    with pytest.raises(SupervisorOwnershipConflict):
        ownership.acquire()

    assert events == [
        "acquire:database",
        "acquire:lark/bot-a",
        "acquire:lark/bot-b",
        "close:lark/bot-a",
        "close:database",
    ]


def test_database_conflict_happens_before_any_account_is_acquired(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"
    database = tmp_path / "runtime.sqlite"
    with DatabaseOwnership(database, lock_root=lock_root):
        contender = SupervisorAccountSetOwnership(
            database,
            accounts=(("lark", "bot-a"), ("wechat", "bot-w")),
            lock_root=lock_root,
        )
        with pytest.raises(SupervisorOwnershipConflict) as raised:
            contender.acquire()
        assert raised.value.scope == "database"
        assert not any(item.held for item in contender.account_ownerships)

        # A database loser must not briefly claim or strand an account lock.
        with ChannelAccountOwnership(
            channel="lark",
            bot_id="bot-a",
            lock_root=lock_root,
        ) as account_probe:
            assert account_probe.held


def test_context_releases_all_locks_after_an_ordinary_exception(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"
    database = tmp_path / "runtime.sqlite"
    ownership = SupervisorAccountSetOwnership(
        database,
        accounts=(("wechat", "bot-w"), ("lark", "bot-l")),
        lock_root=lock_root,
    )

    with pytest.raises(ValueError, match="runtime failed"):
        with ownership:
            raise ValueError("runtime failed")
    assert not ownership.held

    with SupervisorAccountSetOwnership(
        database,
        accounts=(("lark", "bot-l"), ("wechat", "bot-w")),
        lock_root=lock_root,
    ) as replacement:
        assert replacement.held


def test_unproven_shutdown_retains_every_lock_until_explicit_close(
    tmp_path: Path,
) -> None:
    lock_root = tmp_path / "locks"
    database = tmp_path / "runtime.sqlite"
    ownership = SupervisorAccountSetOwnership(
        database,
        accounts=(("lark", "bot-l"), ("wechat", "bot-w")),
        lock_root=lock_root,
    )

    with pytest.raises(SupervisorResourcesStillLive, match="still running"):
        with ownership:
            raise SupervisorResourcesStillLive("account worker is still running")

    assert ownership.held
    with pytest.raises(SupervisorOwnershipConflict) as database_conflict:
        DatabaseOwnership(database, lock_root=lock_root).acquire()
    assert database_conflict.value.scope == "database"
    with pytest.raises(SupervisorOwnershipConflict) as account_conflict:
        ChannelAccountOwnership(
            channel="lark",
            bot_id="bot-l",
            lock_root=lock_root,
        ).acquire()
    assert account_conflict.value.scope == "account"

    # Tests need a deterministic cleanup escape hatch; production deliberately
    # omits this call when shutdown cannot prove its resources are fenced.
    ownership.close()
    assert not ownership.held
    with SupervisorAccountSetOwnership(
        database,
        accounts=(("wechat", "bot-w"), ("lark", "bot-l")),
        lock_root=lock_root,
    ) as replacement:
        assert replacement.held


def test_partial_state_is_rejected_instead_of_silently_completed(
    tmp_path: Path,
) -> None:
    ownership = SupervisorAccountSetOwnership(
        tmp_path / "runtime.sqlite",
        accounts=(("wechat", "bot-w"), ("lark", "bot-l")),
        lock_root=tmp_path / "locks",
    )
    ownership.database_ownership.acquire()
    try:
        with pytest.raises(SupervisorOwnershipError, match="partial state"):
            ownership.acquire()
    finally:
        ownership.close()


def test_multi_account_alias_is_the_composite_owner() -> None:
    assert MultiAccountOwnership is SupervisorAccountSetOwnership


def _incremental_owner(
    kind: str,
    *,
    database: Path,
    lock_root: Path,
):
    if kind == "account-set":
        return SupervisorAccountSetOwnership(
            database,
            accounts=(("wechat", "bot-w"),),
            lock_root=lock_root,
            owner_instance_id=f"{kind}-owner",
        )
    return supervisor.SupervisorOwnership(
        database,
        channel="wechat",
        bot_id="bot-w",
        lock_root=lock_root,
        owner_instance_id=f"{kind}-owner",
    )


@pytest.mark.parametrize("kind", ["account-set", "legacy"])
def test_incremental_account_lock_keeps_existing_ownership_and_requires_exact_handle(
    kind: str,
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    ownership = _incremental_owner(
        kind,
        database=database,
        lock_root=lock_root,
    ).acquire()
    handle = ownership.acquire_account(channel="lark", bot_id="cli_new")
    try:
        assert ownership.held
        assert handle.held
        assert handle.owner_instance_id == ownership.owner_instance_id

        # A lookalike handle and a startup-set handle are not rollback tokens.
        lookalike = ChannelAccountOwnership(
            channel="lark",
            bot_id="cli_new",
            lock_root=lock_root,
        )
        with pytest.raises(SupervisorOwnershipError, match="does not belong"):
            ownership.release_account(lookalike)
        if kind == "account-set":
            startup_handle = next(
                item
                for item in ownership.account_ownerships
                if (item.channel, item.bot_id) == ("wechat", "bot-w")
            )
            with pytest.raises(SupervisorOwnershipError, match="does not belong"):
                ownership.release_account(startup_handle)

        # Database, startup account, and new account all remain continuously
        # fenced until the exact incremental handle is released.
        with pytest.raises(SupervisorOwnershipConflict) as database_conflict:
            DatabaseOwnership(database, lock_root=lock_root).acquire()
        assert database_conflict.value.scope == "database"
        for channel, bot_id in (("wechat", "bot-w"), ("lark", "cli_new")):
            with pytest.raises(SupervisorOwnershipConflict) as account_conflict:
                ChannelAccountOwnership(
                    channel=channel,
                    bot_id=bot_id,
                    lock_root=lock_root,
                ).acquire()
            assert account_conflict.value.scope == "account"

        ownership.release_account(handle)
        assert ownership.held
        assert not handle.held
        with ChannelAccountOwnership(
            channel="lark",
            bot_id="cli_new",
            lock_root=lock_root,
        ) as replacement:
            assert replacement.held
        with pytest.raises(SupervisorOwnershipConflict):
            ChannelAccountOwnership(
                channel="wechat",
                bot_id="bot-w",
                lock_root=lock_root,
            ).acquire()
    finally:
        ownership.close()


@pytest.mark.parametrize("kind", ["account-set", "legacy"])
def test_incremental_account_conflict_leaves_live_owner_unchanged(
    kind: str,
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    ownership = _incremental_owner(
        kind,
        database=database,
        lock_root=lock_root,
    ).acquire()
    try:
        with ChannelAccountOwnership(
            channel="lark",
            bot_id="cli_busy",
            lock_root=lock_root,
        ):
            with pytest.raises(SupervisorOwnershipConflict) as raised:
                ownership.acquire_account(channel="lark", bot_id="cli_busy")
            assert raised.value.scope == "account"
            assert ownership.held
            assert not ownership._incremental_account_ownerships

        handle = ownership.acquire_account(channel="lark", bot_id="cli_busy")
        assert ownership.held
        ownership.release_account(handle)
    finally:
        ownership.close()


@pytest.mark.parametrize("kind", ["account-set", "legacy"])
def test_incremental_account_membership_survives_close_and_atomic_reacquire(
    kind: str,
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    ownership = _incremental_owner(
        kind,
        database=database,
        lock_root=lock_root,
    ).acquire()
    handle = ownership.acquire_account(channel="lark", bot_id="cli_new")

    ownership.close()
    assert not ownership.held
    assert not handle.held

    ownership.acquire()
    try:
        assert ownership.held
        assert handle.held
        ownership.release_account(handle)
    finally:
        ownership.close()


@pytest.mark.parametrize("kind", ["account-set", "legacy"])
def test_incremental_account_acquisition_is_serialized(
    kind: str,
    tmp_path: Path,
) -> None:
    ownership = _incremental_owner(
        kind,
        database=tmp_path / "runtime.sqlite",
        lock_root=tmp_path / "locks",
    ).acquire()
    barrier = threading.Barrier(3)
    results: list[ChannelAccountOwnership | BaseException] = []

    def acquire() -> None:
        barrier.wait()
        try:
            results.append(
                ownership.acquire_account(channel="lark", bot_id="cli_new")
            )
        except BaseException as exc:
            results.append(exc)

    threads = [threading.Thread(target=acquire) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2)
    try:
        handles = [item for item in results if isinstance(item, ChannelAccountOwnership)]
        errors = [item for item in results if isinstance(item, BaseException)]
        assert len(handles) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], SupervisorOwnershipError)
        assert "already owned" in str(errors[0])
        ownership.release_account(handles[0])
    finally:
        ownership.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize("kind", ["account-set", "legacy"])
def test_forked_child_does_not_retain_incremental_account_descriptor(
    kind: str,
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    lock_root = tmp_path / "locks"
    ownership = _incremental_owner(
        kind,
        database=database,
        lock_root=lock_root,
    ).acquire()
    handle = ownership.acquire_account(channel="lark", bot_id="cli_new")
    child_ready_read, child_ready_write = os.pipe()
    child_exit_read, child_exit_write = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:  # pragma: no cover - assertions run in the parent
        try:
            os.close(child_ready_read)
            os.close(child_exit_write)
            cleared = not ownership.held and not handle.held
            os.write(child_ready_write, b"cleared" if cleared else b"held")
            os.read(child_exit_read, 1)
        finally:
            os._exit(0)

    os.close(child_ready_write)
    os.close(child_exit_read)
    try:
        assert os.read(child_ready_read, 7) == b"cleared"
        ownership.close()

        # The child deliberately remains alive while a fresh owner proves it
        # did not inherit either the base descriptors or the incremental one.
        with SupervisorAccountSetOwnership(
            database,
            accounts=(("wechat", "bot-w"), ("lark", "cli_new")),
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
