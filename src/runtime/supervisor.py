"""Exclusive supervisor ownership for one runtime database and channel account.

The SQLite store intentionally supports more than one connection inside the
owning process.  Process ownership therefore lives above the store: one
``SupervisorOwnership`` instance holds two kernel advisory locks for the full
launcher lifetime, while any number of supervisor-owned store adapters may use
the database beneath it.

Lock files are stable rendezvous points only.  Their diagnostic contents and
PIDs never establish ownership; the live, non-inherited ``flock`` descriptors
do.  Files are retained after release so unlink/recreate is not part of the
normal ownership protocol.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import stat
import threading
import uuid
import weakref
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .identity import compound_id


DEFAULT_SUPERVISOR_LOCK_ROOT = Path.home() / ".codex-wechat-bot" / "locks"


# ``FD_CLOEXEC`` protects the normal fresh-interpreter subprocess path, but a
# raw fork duplicates the same open file description.  If the child retained
# that duplicate, it could keep the supervisor lock alive after the parent
# died.  Serialize fork against lock acquisition/release and close only the
# child's duplicates in the after-fork callback (calling ``LOCK_UN`` there
# would incorrectly unlock the parent's shared open-file description).
_FORK_DESCRIPTOR_LOCK = threading.RLock()
_HELD_OWNERSHIPS: weakref.WeakSet[Any] = weakref.WeakSet()
_RETAINED_OWNERSHIPS: set[Any] = set()


def _before_fork() -> None:
    _FORK_DESCRIPTOR_LOCK.acquire()


def _after_fork_parent() -> None:
    _FORK_DESCRIPTOR_LOCK.release()


def _after_fork_child() -> None:
    try:
        for ownership in tuple(_HELD_OWNERSHIPS):
            for attribute in (
                "_account_descriptor",
                "_database_descriptor",
                "_descriptor",
            ):
                descriptor = getattr(ownership, attribute, None)
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                    setattr(ownership, attribute, None)
            setattr(ownership, "_owner_pid", None)
        _HELD_OWNERSHIPS.clear()
        _RETAINED_OWNERSHIPS.clear()
    finally:
        _FORK_DESCRIPTOR_LOCK.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_parent,
        after_in_child=_after_fork_child,
    )


class SupervisorOwnershipError(RuntimeError):
    """The supervisor ownership boundary could not be established safely."""


class SupervisorOwnershipConflict(SupervisorOwnershipError):
    """Another process currently owns the requested database or account."""

    def __init__(self, scope: str) -> None:
        self.scope = str(scope)
        label = {
            "database": "runtime database",
            "account": "channel account",
            "credentials": "credential store",
        }.get(self.scope, self.scope)
        super().__init__(f"{label} is already owned by another supervisor")


class SupervisorLockSecurityError(SupervisorOwnershipError):
    """A lock root or lock file failed its ownership/type checks."""


class SupervisorResourcesStillLive(SupervisorOwnershipError):
    """Fatal shutdown fence: ownership must remain held until process exit."""


@dataclass(frozen=True, slots=True)
class SupervisorLockPaths:
    """Resolved, non-secret paths used by one supervisor ownership pair."""

    root: Path
    database: Path
    account: Path


@dataclass(frozen=True, slots=True)
class SupervisorAccountSetLockPaths:
    """Resolved lock paths for one database and a sorted account set."""

    root: Path
    database: Path
    accounts: tuple[Path, ...]


def _canonical_component(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in text):
        raise ValueError(f"{name} contains a control character")
    return text


def _canonical_database_path(value: str | os.PathLike[str]) -> Path:
    raw = os.fspath(value)
    if not str(raw).strip() or str(raw).strip() == ":memory:":
        raise ValueError("a durable database path is required")
    return Path(raw).expanduser().resolve(strict=False)


def _lock_filename(scope: str, components: tuple[str, ...]) -> str:
    framed = compound_id(f"supervisor-{scope}-lock", components)
    digest = hashlib.sha256(framed.encode("utf-8")).hexdigest()
    return f"{scope}-{digest}.lock"


def _lock_root_path(value: str | os.PathLike[str] | None) -> Path:
    configured = (
        os.environ.get("CODEX_WECHAT_LOCK_ROOT", "").strip()
        if value is None
        else os.fspath(value)
    )
    root = (
        Path(configured).expanduser()
        if str(configured).strip()
        else DEFAULT_SUPERVISOR_LOCK_ROOT
    )
    # ``absolute`` deliberately does not follow the final component.  The
    # no-follow directory open below must reject a configured symlink instead
    # of silently locking a different directory.
    return root.absolute()


def _open_private_lock_root(path: Path) -> int:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        details = path.lstat()
    except OSError as exc:
        raise SupervisorLockSecurityError("supervisor lock root is unavailable") from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise SupervisorLockSecurityError("supervisor lock root is not a directory")
    if hasattr(os, "getuid") and details.st_uid != os.getuid():
        raise SupervisorLockSecurityError("supervisor lock root has a different owner")
    if stat.S_IMODE(details.st_mode) & 0o077:
        raise SupervisorLockSecurityError("supervisor lock root must be owner-only")

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SupervisorLockSecurityError("supervisor lock root cannot be opened safely") from exc
    # PEP 446 makes Python-created descriptors non-inheritable by default, but
    # keep the ownership boundary explicit even on an alternate interpreter or
    # a platform without ``O_CLOEXEC``.  Executed Agent children must never
    # retain a supervisor ownership descriptor.
    os.set_inheritable(descriptor, False)
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or opened.st_dev != details.st_dev
        or opened.st_ino != details.st_ino
    ):
        os.close(descriptor)
        raise SupervisorLockSecurityError("supervisor lock root changed during validation")
    return descriptor


def _open_lock_file(root_descriptor: int, filename: str) -> int:
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(filename, flags, 0o600, dir_fd=root_descriptor)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise SupervisorLockSecurityError(
                "supervisor lock file is not a regular file"
            ) from exc
        raise SupervisorLockSecurityError("supervisor lock file cannot be opened safely") from exc
    os.set_inheritable(descriptor, False)

    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
            raise SupervisorLockSecurityError("supervisor lock file is not a regular file")
        if hasattr(os, "getuid") and details.st_uid != os.getuid():
            raise SupervisorLockSecurityError("supervisor lock file has a different owner")
        # A file created under an unusual umask, or retained from an older
        # release, is safely tightened only after ownership/type validation.
        if stat.S_IMODE(details.st_mode) != 0o600:
            os.fchmod(descriptor, 0o600)
        current = os.stat(filename, dir_fd=root_descriptor, follow_symlinks=False)
        if current.st_dev != details.st_dev or current.st_ino != details.st_ino:
            raise SupervisorLockSecurityError("supervisor lock file changed during validation")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _release_descriptor(descriptor: int | None) -> None:
    if descriptor is None:
        return
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _lock_nonblocking(descriptor: int, scope: str) -> None:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EAGAIN}:
            raise SupervisorOwnershipConflict(scope) from None
        raise SupervisorOwnershipError("supervisor lock acquisition failed") from exc


def _write_lock_diagnostic(
    descriptor: int,
    *,
    owner_instance_id: str,
    scope: str,
) -> None:
    payload = json.dumps(
        {
            "owner_instance_id": owner_instance_id,
            "pid": os.getpid(),
            "scope": scope,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("short supervisor lock diagnostic write")
            written += count
        os.fsync(descriptor)
    except OSError as exc:
        raise SupervisorOwnershipError(
            "supervisor lock diagnostics could not be written"
        ) from exc


class _SingleLockOwnership:
    """Shared implementation for credential-store and account-only locks."""

    def __init__(
        self,
        *,
        scope: str,
        components: tuple[str, ...],
        lock_root: str | os.PathLike[str] | None,
        owner_instance_id: str | None,
    ) -> None:
        self.scope = scope
        self.owner_instance_id = _canonical_component(
            owner_instance_id or uuid.uuid4().hex,
            "owner_instance_id",
        )
        self.root = _lock_root_path(lock_root)
        self.path = self.root / _lock_filename(scope, components)
        self._descriptor: int | None = None
        self._owner_pid: int | None = None

    @property
    def held(self) -> bool:
        return bool(
            self._owner_pid == os.getpid() and self._descriptor is not None
        )

    def acquire(self) -> "_SingleLockOwnership":
        if self.held:
            return self
        if self._descriptor is not None:
            raise SupervisorOwnershipError("supervisor ownership is in a partial state")
        if self._owner_pid is not None and self._owner_pid != os.getpid():
            raise SupervisorOwnershipError(
                "supervisor ownership cannot cross a fork boundary"
            )

        with _FORK_DESCRIPTOR_LOCK:
            root_descriptor = _open_private_lock_root(self.root)
            descriptor: int | None = None
            try:
                descriptor = _open_lock_file(root_descriptor, self.path.name)
                _lock_nonblocking(descriptor, self.scope)
                _write_lock_diagnostic(
                    descriptor,
                    owner_instance_id=self.owner_instance_id,
                    scope=self.scope,
                )
            except BaseException:
                try:
                    _release_descriptor(descriptor)
                except BaseException:
                    pass
                raise
            finally:
                os.close(root_descriptor)
            self._descriptor = descriptor
            self._owner_pid = os.getpid()
            _HELD_OWNERSHIPS.add(self)
        return self

    def close(self) -> None:
        with _FORK_DESCRIPTOR_LOCK:
            _HELD_OWNERSHIPS.discard(self)
            descriptor, self._descriptor = self._descriptor, None
            self._owner_pid = None
            try:
                _release_descriptor(descriptor)
            except BaseException as exc:
                raise SupervisorOwnershipError(
                    "supervisor ownership release failed"
                ) from exc

    release = close

    def __enter__(self) -> "_SingleLockOwnership":
        return self.acquire()

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - deterministic close is tested
        try:
            self.close()
        except Exception:
            pass


class ChannelAccountOwnership(_SingleLockOwnership):
    """Hold one canonical channel-account lock without opening SQLite."""

    def __init__(
        self,
        *,
        channel: str,
        bot_id: str,
        lock_root: str | os.PathLike[str] | None = None,
        owner_instance_id: str | None = None,
    ) -> None:
        self.channel = _canonical_component(channel, "channel")
        self.bot_id = _canonical_component(bot_id, "bot_id")
        super().__init__(
            scope="account",
            components=(self.channel, self.bot_id),
            lock_root=lock_root,
            owner_instance_id=owner_instance_id,
        )


class CredentialMutationOwnership(_SingleLockOwnership):
    """Serialize credential discovery, publication, and deletion processes."""

    def __init__(
        self,
        credential_root: str | os.PathLike[str],
        *,
        lock_root: str | os.PathLike[str] | None = None,
        owner_instance_id: str | None = None,
    ) -> None:
        raw_root = os.fspath(credential_root)
        if not str(raw_root).strip():
            raise ValueError("credential_root is required")
        self.credential_root = Path(raw_root).expanduser().resolve(strict=False)
        super().__init__(
            scope="credentials",
            components=(str(self.credential_root),),
            lock_root=lock_root,
            owner_instance_id=owner_instance_id,
        )


class DatabaseOwnership(_SingleLockOwnership):
    """Hold one canonical runtime-database lock without an account lock."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        lock_root: str | os.PathLike[str] | None = None,
        owner_instance_id: str | None = None,
    ) -> None:
        self.database_path = _canonical_database_path(database_path)
        super().__init__(
            scope="database",
            components=(str(self.database_path),),
            lock_root=lock_root,
            owner_instance_id=owner_instance_id,
        )


def _canonical_account(value: Any) -> tuple[str, str]:
    """Normalize one ``(channel, bot_id)`` account descriptor."""

    channel = getattr(value, "channel", None)
    bot_id = getattr(value, "bot_id", None)
    if channel is None and bot_id is None:
        if isinstance(value, (str, bytes, bytearray)):
            raise TypeError("an account must be a (channel, bot_id) pair")
        try:
            channel, bot_id = value
        except (TypeError, ValueError) as exc:
            raise TypeError("an account must be a (channel, bot_id) pair") from exc
    elif channel is None or bot_id is None:
        raise TypeError("an account must provide both channel and bot_id")
    return (
        _canonical_component(channel, "channel"),
        _canonical_component(bot_id, "bot_id"),
    )


class SupervisorAccountSetOwnership:
    """Atomically own one database and a deterministic set of channel accounts.

    The existing :class:`SupervisorOwnership` remains the compatibility owner
    for exactly one account.  This additive composition is the multi-channel
    boundary: it acquires the database first, then every unique account in
    canonical lexical order.  Any failure rolls all earlier acquisitions back
    in reverse order before the original error is re-raised.

    Each component is an ordinary ``_SingleLockOwnership``.  Consequently the
    existing at-fork descriptor fence applies without introducing a second raw
    descriptor collection that a child process could accidentally inherit.
    """

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        accounts: Iterable[Any],
        lock_root: str | os.PathLike[str] | None = None,
        owner_instance_id: str | None = None,
    ) -> None:
        canonical_accounts = tuple(
            sorted({_canonical_account(account) for account in accounts})
        )
        if not canonical_accounts:
            raise ValueError("at least one channel account is required")

        self._account_mutation_lock = threading.RLock()
        self.database_path = _canonical_database_path(database_path)
        self.accounts = canonical_accounts
        self.owner_instance_id = _canonical_component(
            owner_instance_id or uuid.uuid4().hex,
            "owner_instance_id",
        )
        self.database_ownership = DatabaseOwnership(
            self.database_path,
            lock_root=lock_root,
            owner_instance_id=self.owner_instance_id,
        )
        self.account_ownerships = tuple(
            ChannelAccountOwnership(
                channel=channel,
                bot_id=bot_id,
                lock_root=lock_root,
                owner_instance_id=self.owner_instance_id,
            )
            for channel, bot_id in self.accounts
        )
        # Incremental accounts are tracked by exact object identity as well as
        # canonical account key.  A caller may release only the handle returned
        # by ``acquire_account``; it can never name and accidentally release an
        # account that formed part of the startup ownership set.
        self._incremental_account_ownerships: dict[
            tuple[str, str], ChannelAccountOwnership
        ] = {}
        self.paths = SupervisorAccountSetLockPaths(
            root=self.database_ownership.root,
            database=self.database_ownership.path,
            accounts=tuple(ownership.path for ownership in self.account_ownerships),
        )

    @property
    def held(self) -> bool:
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            return self.database_ownership.held and all(
                ownership.held for ownership in self.account_ownerships
            )

    def owns_account(self, *, channel: str, bot_id: str) -> bool:
        """Return whether the canonical account is in this held lock set."""

        account = _canonical_account((channel, bot_id))
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            return self.held and account in self.accounts

    def acquire(self) -> "SupervisorAccountSetOwnership":
        """Acquire the complete lock set or leave none of it held."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            component_ownerships = (
                self.database_ownership,
                *self.account_ownerships,
            )
            held_states = tuple(ownership.held for ownership in component_ownerships)
            if all(held_states):
                return self
            if any(held_states):
                raise SupervisorOwnershipError(
                    "supervisor account-set ownership is in a partial state"
                )

            acquired: list[_SingleLockOwnership] = []
            try:
                for ownership in component_ownerships:
                    ownership.acquire()
                    acquired.append(ownership)
            except BaseException:
                # The acquisition error is authoritative, but every earlier lock is
                # still released even if one best-effort rollback close also fails.
                for ownership in reversed(acquired):
                    try:
                        ownership.close()
                    except BaseException:
                        pass
                raise
            return self

    def acquire_account(
        self,
        *,
        channel: str,
        bot_id: str,
    ) -> ChannelAccountOwnership:
        """Add one account lock without releasing the live database lock.

        Acquisition is serialized with account-set shutdown and is atomic
        from the composite owner's perspective: a conflict or security error
        leaves the existing set unchanged.  Duplicate acquisition is rejected
        rather than returning an existing handle, because returning a startup
        handle would let onboarding rollback release a lock it does not own.
        """

        account = _canonical_account((channel, bot_id))
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            if not self.held:
                raise SupervisorOwnershipError(
                    "incremental account acquisition requires held supervisor ownership"
                )
            if account in self.accounts:
                raise SupervisorOwnershipError(
                    "channel account is already owned by this supervisor"
                )
            candidate = ChannelAccountOwnership(
                channel=account[0],
                bot_id=account[1],
                lock_root=self.database_ownership.root,
                owner_instance_id=self.owner_instance_id,
            )
            candidate.acquire()
            try:
                self._incremental_account_ownerships[account] = candidate
                pairs = {
                    (ownership.channel, ownership.bot_id): ownership
                    for ownership in self.account_ownerships
                }
                pairs[account] = candidate
                self.accounts = tuple(sorted(pairs))
                self.account_ownerships = tuple(
                    pairs[key] for key in self.accounts
                )
                self.paths = SupervisorAccountSetLockPaths(
                    root=self.database_ownership.root,
                    database=self.database_ownership.path,
                    accounts=tuple(
                        ownership.path for ownership in self.account_ownerships
                    ),
                )
            except BaseException:
                self._incremental_account_ownerships.pop(account, None)
                candidate.close()
                raise
            return candidate

    def release_account(self, ownership: ChannelAccountOwnership) -> None:
        """Release exactly one handle returned by :meth:`acquire_account`.

        Runtime composition must fence and stop that account's resources
        before calling this method.  The identity check intentionally rejects
        a newly constructed lookalike and every account acquired at startup.
        """

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            account = next(
                (
                    key
                    for key, candidate in self._incremental_account_ownerships.items()
                    if candidate is ownership
                ),
                None,
            )
            if account is None:
                raise SupervisorOwnershipError(
                    "incremental account ownership does not belong to this supervisor"
                )
            ownership.close()
            del self._incremental_account_ownerships[account]
            self.accounts = tuple(
                key
                for key in self.accounts
                if key != account
            )
            self.account_ownerships = tuple(
                candidate
                for candidate in self.account_ownerships
                if candidate is not ownership
            )
            self.paths = SupervisorAccountSetLockPaths(
                root=self.database_ownership.root,
                database=self.database_ownership.path,
                accounts=tuple(
                    candidate.path for candidate in self.account_ownerships
                ),
            )

    def close(self) -> None:
        """Release every account in reverse order, then release the database."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            _RETAINED_OWNERSHIPS.discard(self)

            error: BaseException | None = None
            for ownership in reversed(
                (self.database_ownership, *self.account_ownerships)
            ):
                try:
                    ownership.close()
                except BaseException as exc:
                    error = error or exc
            if error is not None:
                raise SupervisorOwnershipError(
                    "supervisor account-set ownership release failed"
                ) from error

    release = close

    def retain_until_process_exit(self) -> None:
        """Keep the complete lock set alive after an unproven shutdown."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            if not self.held:
                raise SupervisorOwnershipError(
                    "cannot retain supervisor ownership that is not held"
                )
            # This strong reference prevents ``__del__`` from releasing any
            # component while live runtime resources might still use them.
            _RETAINED_OWNERSHIPS.add(self)

    def __enter__(self) -> "SupervisorAccountSetOwnership":
        return self.acquire()

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        if isinstance(_exc, SupervisorResourcesStillLive):
            self.retain_until_process_exit()
            return
        self.close()

    def __del__(self) -> None:  # pragma: no cover - deterministic close is tested
        try:
            self.close()
        except Exception:
            pass


# Short descriptive alias for callers that do not need the historical
# ``SupervisorOwnership`` naming convention.
MultiAccountOwnership = SupervisorAccountSetOwnership


class SupervisorOwnership:
    """Hold exclusive database and channel-account ownership until closed.

    Acquisition is synchronous and non-blocking by design.  A launcher must
    establish this boundary before constructing or initializing ``SQLiteStore``
    so a losing process cannot run migrations or startup recovery.
    """

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        channel: str,
        bot_id: str,
        lock_root: str | os.PathLike[str] | None = None,
        owner_instance_id: str | None = None,
    ) -> None:
        self.database_path = _canonical_database_path(database_path)
        self.channel = _canonical_component(channel, "channel")
        self.bot_id = _canonical_component(bot_id, "bot_id")
        self.owner_instance_id = _canonical_component(
            owner_instance_id or uuid.uuid4().hex,
            "owner_instance_id",
        )
        self._account_mutation_lock = threading.RLock()
        self._incremental_account_ownerships: dict[
            tuple[str, str], ChannelAccountOwnership
        ] = {}
        root = _lock_root_path(lock_root)
        database_name = _lock_filename("database", (str(self.database_path),))
        account_name = _lock_filename("account", (self.channel, self.bot_id))
        self.paths = SupervisorLockPaths(
            root=root,
            database=root / database_name,
            account=root / account_name,
        )
        self._database_descriptor: int | None = None
        self._account_descriptor: int | None = None
        self._owner_pid: int | None = None

    @property
    def held(self) -> bool:
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            return bool(
                self._owner_pid == os.getpid()
                and self._database_descriptor is not None
                and self._account_descriptor is not None
                and all(
                    ownership.held
                    for ownership in self._incremental_account_ownerships.values()
                )
            )

    def owns_account(self, *, channel: str, bot_id: str) -> bool:
        """Return whether the canonical account is held by this owner."""

        account = _canonical_account((channel, bot_id))
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            return self.held and (
                account == (self.channel, self.bot_id)
                or account in self._incremental_account_ownerships
            )

    def acquire(self) -> "SupervisorOwnership":
        """Acquire both locks, rolling the first back if the second conflicts."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            if self.held:
                return self
            component_states = (
                self._database_descriptor is not None,
                self._account_descriptor is not None,
                *(
                    ownership.held
                    for ownership in self._incremental_account_ownerships.values()
                ),
            )
            if any(component_states):
                raise SupervisorOwnershipError(
                    "supervisor ownership is in a partial state"
                )
            if self._owner_pid is not None and self._owner_pid != os.getpid():
                raise SupervisorOwnershipError(
                    "supervisor ownership cannot cross a fork boundary"
                )

            root_descriptor = _open_private_lock_root(self.paths.root)
            database_descriptor: int | None = None
            account_descriptor: int | None = None
            acquired_incremental: list[ChannelAccountOwnership] = []
            try:
                database_descriptor = _open_lock_file(
                    root_descriptor, self.paths.database.name
                )
                _lock_nonblocking(database_descriptor, "database")
                account_descriptor = _open_lock_file(
                    root_descriptor, self.paths.account.name
                )
                _lock_nonblocking(account_descriptor, "account")
                for ownership in self._incremental_account_ownerships.values():
                    ownership.acquire()
                    acquired_incremental.append(ownership)
                _write_lock_diagnostic(
                    database_descriptor,
                    owner_instance_id=self.owner_instance_id,
                    scope="database",
                )
                _write_lock_diagnostic(
                    account_descriptor,
                    owner_instance_id=self.owner_instance_id,
                    scope="account",
                )
            except BaseException:
                # Acquisition failures take precedence over best-effort
                # rollback.  Still attempt every close so no earlier lock
                # in the expanded ownership set can be stranded.
                for ownership in reversed(acquired_incremental):
                    try:
                        ownership.close()
                    except BaseException:
                        pass
                for descriptor in (account_descriptor, database_descriptor):
                    try:
                        _release_descriptor(descriptor)
                    except BaseException:
                        pass
                raise
            finally:
                os.close(root_descriptor)

            self._database_descriptor = database_descriptor
            self._account_descriptor = account_descriptor
            self._owner_pid = os.getpid()
            _HELD_OWNERSHIPS.add(self)
            return self

    def acquire_account(
        self,
        *,
        channel: str,
        bot_id: str,
    ) -> ChannelAccountOwnership:
        """Acquire one additional account while retaining both startup locks."""

        account = _canonical_account((channel, bot_id))
        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            if not self.held:
                raise SupervisorOwnershipError(
                    "incremental account acquisition requires held supervisor ownership"
                )
            if account == (self.channel, self.bot_id) or (
                account in self._incremental_account_ownerships
            ):
                raise SupervisorOwnershipError(
                    "channel account is already owned by this supervisor"
                )
            candidate = ChannelAccountOwnership(
                channel=account[0],
                bot_id=account[1],
                lock_root=self.paths.root,
                owner_instance_id=self.owner_instance_id,
            )
            candidate.acquire()
            try:
                self._incremental_account_ownerships[account] = candidate
            except BaseException:
                candidate.close()
                raise
            return candidate

    def release_account(self, ownership: ChannelAccountOwnership) -> None:
        """Release exactly one handle returned by :meth:`acquire_account`."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            account = next(
                (
                    key
                    for key, candidate in self._incremental_account_ownerships.items()
                    if candidate is ownership
                ),
                None,
            )
            if account is None:
                raise SupervisorOwnershipError(
                    "incremental account ownership does not belong to this supervisor"
                )
            ownership.close()
            del self._incremental_account_ownerships[account]

    def close(self) -> None:
        """Release both kernel locks.  The stable rendezvous files remain."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            _HELD_OWNERSHIPS.discard(self)
            _RETAINED_OWNERSHIPS.discard(self)
            account, self._account_descriptor = self._account_descriptor, None
            database, self._database_descriptor = self._database_descriptor, None
            self._owner_pid = None
            error: BaseException | None = None
            for ownership in reversed(
                tuple(self._incremental_account_ownerships.values())
            ):
                try:
                    ownership.close()
                except BaseException as exc:
                    error = error or exc
            for descriptor in (account, database):
                try:
                    _release_descriptor(descriptor)
                except BaseException as exc:  # close all before reporting one error
                    error = error or exc
            if error is not None:
                raise SupervisorOwnershipError(
                    "supervisor ownership release failed"
                ) from error

    release = close

    def retain_until_process_exit(self) -> None:
        """Keep fatal-shutdown ownership live even if local references unwind."""

        with _FORK_DESCRIPTOR_LOCK, self._account_mutation_lock:
            if not self.held:
                raise SupervisorOwnershipError(
                    "cannot retain supervisor ownership that is not held"
                )
            _RETAINED_OWNERSHIPS.add(self)

    def __enter__(self) -> "SupervisorOwnership":
        return self.acquire()

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        if isinstance(_exc, SupervisorResourcesStillLive):
            self.retain_until_process_exit()
            return
        self.close()

    def __del__(self) -> None:  # pragma: no cover - deterministic close is tested
        try:
            self.close()
        except Exception:
            pass


__all__ = [
    "ChannelAccountOwnership",
    "CredentialMutationOwnership",
    "DatabaseOwnership",
    "DEFAULT_SUPERVISOR_LOCK_ROOT",
    "MultiAccountOwnership",
    "SupervisorAccountSetLockPaths",
    "SupervisorAccountSetOwnership",
    "SupervisorLockPaths",
    "SupervisorLockSecurityError",
    "SupervisorOwnership",
    "SupervisorOwnershipConflict",
    "SupervisorOwnershipError",
    "SupervisorResourcesStillLive",
]
