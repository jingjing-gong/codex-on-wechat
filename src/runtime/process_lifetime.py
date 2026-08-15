"""Linux process-tree identity and inherited lifetime-lock evidence.

This module contains no scheduler or durable-store integration.  It gives the
disconnected Agent-process foundation two kernel-backed facts:

* signals are sent through pidfds only after ``/proc`` birth identity, session,
  and process-group membership match the captured generation; and
* an exclusive ``flock`` remains owned by the child open-file description and
  every fork descendant that retains it.  A distinct probe description can
  acquire that lock only after the last holder has closed it.  The inode has a
  securely created name beneath an explicit private lock root, so a replacement
  supervisor can reopen the exact root/file identity after a crash.

Linux has no pidfd operation for an entire process group.  Cleanup therefore
enumerates the verified session/group and signals each exact process through a
pidfd, repeating until the group is empty.  A descendant that deliberately
escapes the session can still keep the lifetime descriptor and make cleanup
fail closed, but pure Python cannot forcibly contain such a process without a
cgroup/job boundary.  Production assignment remains disabled until that
containment exists.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import errno
import fcntl
import os
import re
import secrets
import signal
import stat
import sys
from dataclasses import dataclass
from pathlib import Path


class ProcessLifetimeError(RuntimeError):
    """Base error for kernel process/lifetime evidence."""


class ProcessIdentityMismatchError(ProcessLifetimeError):
    """A numeric PID/group now names a different kernel process identity."""


class ProcessLifetimeArtifactMissingError(ProcessIdentityMismatchError):
    """A securely identified lifetime-lock name is conclusively absent."""


class ProcessInspectionUnavailableError(ProcessLifetimeError):
    """The host cannot provide the kernel evidence required for safe cleanup."""


class ProcessLifetimeStillHeldError(ProcessLifetimeError):
    """The exact process group or inherited lifetime lock remains live."""


def _read_boot_id() -> str:
    if not sys.platform.startswith("linux"):
        raise ProcessInspectionUnavailableError(
            "verified Agent process cleanup currently requires Linux"
        )
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except OSError as exc:
        raise ProcessInspectionUnavailableError(
            "Linux boot identity is unavailable"
        ) from exc
    if not value or any(character not in "0123456789abcdef-" for character in value):
        raise ProcessInspectionUnavailableError("Linux boot identity is invalid")
    return value


_BOOT_ID = _read_boot_id() if sys.platform.startswith("linux") else ""
DEFAULT_AGENT_LIFETIME_LOCK_ROOT = os.path.abspath(
    os.path.expanduser(
        os.path.join(
            os.environ.get(
                "CODEX_WECHAT_LOCK_ROOT",
                "~/.codex-wechat-bot/locks",
            ),
            "agent-lifetimes",
        )
    )
)
_LOCK_FILENAME_PATTERN = re.compile(r"^agent-[0-9a-f]{32}\.lock$")
_LOCK_IDENTITY_PATTERN = re.compile(
    r"^linux-flock-v2:([0-9a-f-]+):([0-9a-f]+):([0-9a-f]+):"
    r"([0-9a-f]+):([0-9a-f]+):(agent-[0-9a-f]{32}\.lock)$"
)
_PROCESS_BIRTH_ID_PATTERN = re.compile(
    r"^linux:([0-9a-f-]+):([1-9][0-9]*)$"
)


@dataclass(frozen=True, slots=True)
class LinuxProcessBirthIdentity:
    """Stable identity fields for one PID in the current boot/PID namespace."""

    pid: int
    parent_pid: int
    process_group_id: int
    session_id: int
    start_time_ticks: int
    boot_id: str

    @property
    def wire_birth_id(self) -> str:
        return f"linux:{self.boot_id}:{self.start_time_ticks}"


def _parse_linux_proc_stat(pid: int, value: str) -> LinuxProcessBirthIdentity:
    # ``comm`` is parenthesized and may itself contain spaces or ``)``.  The
    # final closing parenthesis is the only safe split point.
    closing = value.rfind(")")
    opening = value.find("(")
    if opening <= 0 or closing <= opening or closing + 2 > len(value):
        raise ProcessInspectionUnavailableError("Linux process stat is malformed")
    try:
        stat_pid = int(value[:opening].strip())
        fields = value[closing + 2 :].split()
        # fields[0] is field 3 (state); starttime is field 22.
        parent_pid = int(fields[1])
        process_group_id = int(fields[2])
        session_id = int(fields[3])
        start_time_ticks = int(fields[19])
    except (IndexError, ValueError) as exc:
        raise ProcessInspectionUnavailableError(
            "Linux process stat is malformed"
        ) from exc
    if stat_pid != pid or min(parent_pid, process_group_id, session_id) < 0:
        raise ProcessInspectionUnavailableError("Linux process stat is inconsistent")
    if start_time_ticks <= 0:
        raise ProcessInspectionUnavailableError("Linux process birth time is invalid")
    return LinuxProcessBirthIdentity(
        pid=pid,
        parent_pid=parent_pid,
        process_group_id=process_group_id,
        session_id=session_id,
        start_time_ticks=start_time_ticks,
        boot_id=_BOOT_ID,
    )


def capture_linux_process_birth(pid: int) -> LinuxProcessBirthIdentity:
    """Capture the exact current Linux identity of ``pid``."""

    if type(pid) is not int or pid <= 0:
        raise ValueError("pid must be a positive integer")
    if not sys.platform.startswith("linux") or not _BOOT_ID:
        raise ProcessInspectionUnavailableError(
            "verified Agent process cleanup currently requires Linux"
        )
    try:
        value = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except FileNotFoundError as exc:
        raise ProcessLookupError(pid) from exc
    except (PermissionError, OSError) as exc:
        raise ProcessInspectionUnavailableError(
            f"cannot inspect Linux process {pid}"
        ) from exc
    return _parse_linux_proc_stat(pid, value)


def linux_process_birth_matches(identity: LinuxProcessBirthIdentity) -> bool:
    """Return false only when the exact process is conclusively gone."""

    if not isinstance(identity, LinuxProcessBirthIdentity):
        raise TypeError("identity must be a LinuxProcessBirthIdentity")
    try:
        current = capture_linux_process_birth(identity.pid)
    except ProcessLookupError:
        return False
    if current != identity:
        raise ProcessIdentityMismatchError(
            f"PID {identity.pid} no longer matches its captured birth identity"
        )
    return True


_LIBC = ctypes.CDLL(None, use_errno=True) if sys.platform.startswith("linux") else None
_PIDFD_OPEN = getattr(_LIBC, "pidfd_open", None) if _LIBC is not None else None
_PIDFD_SEND_SIGNAL = (
    getattr(_LIBC, "pidfd_send_signal", None) if _LIBC is not None else None
)
if _PIDFD_OPEN is not None:
    _PIDFD_OPEN.argtypes = (ctypes.c_int, ctypes.c_uint)
    _PIDFD_OPEN.restype = ctypes.c_int
if _PIDFD_SEND_SIGNAL is not None:
    _PIDFD_SEND_SIGNAL.argtypes = (
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint,
    )
    _PIDFD_SEND_SIGNAL.restype = ctypes.c_int


def _open_pidfd(pid: int) -> int:
    if _PIDFD_OPEN is None or _PIDFD_SEND_SIGNAL is None:
        raise ProcessInspectionUnavailableError(
            "Linux pidfd cleanup support is unavailable"
        )
    descriptor = int(_PIDFD_OPEN(pid, 0))
    if descriptor < 0:
        error = ctypes.get_errno()
        if error == errno.ESRCH:
            raise ProcessLookupError(pid)
        raise ProcessInspectionUnavailableError(
            f"cannot open pidfd for Linux process {pid}: errno {error}"
        )
    os.set_inheritable(descriptor, False)
    return descriptor


def _send_pidfd_signal(descriptor: int, sig: int) -> None:
    if _PIDFD_SEND_SIGNAL is None:  # pragma: no cover - guarded by _open_pidfd
        raise ProcessInspectionUnavailableError(
            "Linux pidfd signaling support is unavailable"
        )
    result = int(_PIDFD_SEND_SIGNAL(descriptor, sig, None, 0))
    if result < 0:
        error = ctypes.get_errno()
        if error == errno.ESRCH:
            return
        raise ProcessLifetimeError(
            f"cannot signal verified Linux process: errno {error}"
        )


def require_linux_pidfd_support() -> None:
    """Fail before spawn if exact kernel-targeted cleanup is unavailable."""

    identity = capture_linux_process_birth(os.getpid())
    descriptor = _open_pidfd(identity.pid)
    try:
        if capture_linux_process_birth(identity.pid) != identity:
            raise ProcessIdentityMismatchError(
                "supervisor PID changed during pidfd verification"
            )
    finally:
        os.close(descriptor)


def _normalized_lock_root(lock_root: str | os.PathLike[str]) -> str:
    value = os.path.expanduser(os.fspath(lock_root))
    if not os.path.isabs(value):
        raise ValueError("Agent lifetime lock root must be absolute")
    normalized = os.path.normpath(value)
    # Reject every symlinked ancestor, not only a symlink at the final name.
    if os.path.realpath(normalized) != normalized:
        raise ProcessInspectionUnavailableError(
            "Agent lifetime lock root contains a symlink"
        )
    return normalized


def _open_secure_lock_root(
    lock_root: str | os.PathLike[str],
    *,
    create: bool,
) -> tuple[str, int, os.stat_result]:
    path = _normalized_lock_root(lock_root)
    if create:
        try:
            os.makedirs(path, mode=0o700, exist_ok=True)
        except OSError as exc:
            raise ProcessInspectionUnavailableError(
                "cannot create Agent lifetime lock root"
            ) from exc
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ProcessInspectionUnavailableError(
            "cannot securely open Agent lifetime lock root"
        ) from exc
    try:
        root_stat = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(root_stat.st_mode)
            or root_stat.st_uid != os.geteuid()
            or stat.S_IMODE(root_stat.st_mode) & 0o077
        ):
            raise ProcessInspectionUnavailableError(
                "Agent lifetime lock root is not a private owned directory"
            )
        os.set_inheritable(descriptor, False)
        return path, descriptor, root_stat
    except BaseException:
        os.close(descriptor)
        raise


def _lock_open_flags(*, create: bool = False) -> int:
    flags = os.O_RDWR
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _validate_lock_file(
    descriptor: int,
    *,
    expected_device: int | None = None,
    expected_inode: int | None = None,
) -> os.stat_result:
    file_stat = os.fstat(descriptor)
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or file_stat.st_uid != os.geteuid()
        or stat.S_IMODE(file_stat.st_mode) != 0o600
        or file_stat.st_nlink != 1
    ):
        raise ProcessIdentityMismatchError(
            "Agent lifetime lock file ownership/link metadata is unsafe"
        )
    if expected_device is not None and (
        file_stat.st_dev != expected_device or file_stat.st_ino != expected_inode
    ):
        raise ProcessIdentityMismatchError(
            "Agent lifetime lock file identity changed"
        )
    return file_stat


def _assert_holder_probe_conflict(holder_fd: int, probe_fd: int) -> None:
    fcntl.flock(holder_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    fcntl.flock(probe_fd, fcntl.LOCK_UN)
    raise ProcessInspectionUnavailableError(
        "lifetime-lock holder and probe do not conflict"
    )


@dataclass(slots=True)
class InheritedLifetimeLock:
    """A recoverable named lock plus inherited holder and private probe."""

    identity: str
    device: int
    inode: int
    root_path: str
    filename: str
    root_device: int
    root_inode: int
    _root_fd: int | None
    _holder_fd: int | None
    _probe_fd: int | None
    _name_unlinked: bool = False
    _released: bool = False

    @classmethod
    def create(
        cls,
        lock_root: str | os.PathLike[str] = DEFAULT_AGENT_LIFETIME_LOCK_ROOT,
    ) -> InheritedLifetimeLock:
        root_fd: int | None = None
        holder_fd: int | None = None
        probe_fd: int | None = None
        filename: str | None = None
        file_created = False
        try:
            root_path, root_fd, root_stat = _open_secure_lock_root(
                lock_root,
                create=True,
            )
            filename = f"agent-{secrets.token_hex(16)}.lock"
            if not _LOCK_FILENAME_PATTERN.fullmatch(filename):  # pragma: no cover
                raise ProcessLifetimeError("generated lifetime lock name is invalid")
            holder_fd = os.open(
                filename,
                _lock_open_flags(create=True),
                0o600,
                dir_fd=root_fd,
            )
            file_created = True
            os.fchmod(holder_fd, 0o600)
            holder_stat = _validate_lock_file(holder_fd)
            probe_fd = os.open(
                filename,
                _lock_open_flags(),
                dir_fd=root_fd,
            )
            _validate_lock_file(
                probe_fd,
                expected_device=holder_stat.st_dev,
                expected_inode=holder_stat.st_ino,
            )
            _assert_holder_probe_conflict(holder_fd, probe_fd)
            os.set_inheritable(holder_fd, False)
            os.set_inheritable(probe_fd, False)
            os.fsync(holder_fd)
            os.fsync(root_fd)
            identity = (
                f"linux-flock-v2:{_BOOT_ID}:{root_stat.st_dev:x}:"
                f"{root_stat.st_ino:x}:{holder_stat.st_dev:x}:"
                f"{holder_stat.st_ino:x}:{filename}"
            )
            return cls(
                identity=identity,
                device=holder_stat.st_dev,
                inode=holder_stat.st_ino,
                root_path=root_path,
                filename=filename,
                root_device=root_stat.st_dev,
                root_inode=root_stat.st_ino,
                _root_fd=root_fd,
                _holder_fd=holder_fd,
                _probe_fd=probe_fd,
            )
        except BaseException:
            if file_created and root_fd is not None and filename is not None:
                with contextlib.suppress(OSError):
                    current = os.stat(
                        filename,
                        dir_fd=root_fd,
                        follow_symlinks=False,
                    )
                    if holder_fd is not None:
                        expected = os.fstat(holder_fd)
                        if (current.st_dev, current.st_ino) == (
                            expected.st_dev,
                            expected.st_ino,
                        ):
                            os.unlink(filename, dir_fd=root_fd)
            for descriptor in (holder_fd, probe_fd, root_fd):
                if descriptor is not None:
                    with contextlib.suppress(OSError):
                        os.close(descriptor)
            raise

    @classmethod
    def reopen(
        cls,
        lock_root: str | os.PathLike[str],
        identity: str,
    ) -> InheritedLifetimeLock:
        """Acquire the exact named identity after a prior supervisor crash."""

        if type(identity) is not str:
            raise TypeError("lifetime lock identity must be a string")
        match = _LOCK_IDENTITY_PATTERN.fullmatch(identity)
        if match is None:
            raise ProcessIdentityMismatchError(
                "lifetime lock identity is not recoverable v2 metadata"
            )
        (
            boot_id,
            root_device_hex,
            root_inode_hex,
            device_hex,
            inode_hex,
            filename,
        ) = match.groups()
        if boot_id != _BOOT_ID:
            raise ProcessIdentityMismatchError(
                "lifetime lock belongs to another kernel boot"
            )
        expected_root = (int(root_device_hex, 16), int(root_inode_hex, 16))
        expected_file = (int(device_hex, 16), int(inode_hex, 16))
        root_fd: int | None = None
        holder_fd: int | None = None
        probe_fd: int | None = None
        try:
            root_path, root_fd, root_stat = _open_secure_lock_root(
                lock_root,
                create=False,
            )
            if (root_stat.st_dev, root_stat.st_ino) != expected_root:
                raise ProcessIdentityMismatchError(
                    "lifetime lock root identity changed"
                )
            try:
                holder_fd = os.open(
                    filename,
                    _lock_open_flags(),
                    dir_fd=root_fd,
                )
            except OSError as exc:
                raise ProcessIdentityMismatchError(
                    "recoverable lifetime-lock name is missing or unsafe"
                ) from exc
            _validate_lock_file(
                holder_fd,
                expected_device=expected_file[0],
                expected_inode=expected_file[1],
            )
            try:
                probe_fd = os.open(
                    filename,
                    _lock_open_flags(),
                    dir_fd=root_fd,
                )
            except OSError as exc:
                raise ProcessIdentityMismatchError(
                    "recoverable lifetime-lock probe name changed"
                ) from exc
            _validate_lock_file(
                probe_fd,
                expected_device=expected_file[0],
                expected_inode=expected_file[1],
            )
            try:
                _assert_holder_probe_conflict(holder_fd, probe_fd)
            except BlockingIOError as exc:  # pragma: no cover - helper consumes it
                raise ProcessLifetimeStillHeldError(
                    "prior Agent lifetime lock remains held"
                ) from exc
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise ProcessLifetimeStillHeldError(
                        "prior Agent lifetime lock remains held"
                    ) from exc
                raise
            # ``_assert_holder_probe_conflict`` first acquires holder.  A busy
            # prior generation therefore surfaces from its first flock call.
            _validate_lock_file(
                holder_fd,
                expected_device=expected_file[0],
                expected_inode=expected_file[1],
            )
            return cls(
                identity=identity,
                device=expected_file[0],
                inode=expected_file[1],
                root_path=root_path,
                filename=filename,
                root_device=expected_root[0],
                root_inode=expected_root[1],
                _root_fd=root_fd,
                _holder_fd=holder_fd,
                _probe_fd=probe_fd,
            )
        except BaseException:
            for descriptor in (holder_fd, probe_fd, root_fd):
                if descriptor is not None:
                    with contextlib.suppress(OSError):
                        os.close(descriptor)
            raise

    @property
    def path(self) -> str:
        return os.path.join(self.root_path, self.filename)

    @property
    def child_fd(self) -> int:
        if self._holder_fd is None:
            raise ProcessLifetimeError("lifetime-lock holder was already transferred")
        return self._holder_fd

    @property
    def released(self) -> bool:
        return self._released

    def release_parent_copy(self) -> None:
        """Close the supervisor's duplicate after transfer or failed spawn."""

        if self._holder_fd is not None:
            os.close(self._holder_fd)
            self._holder_fd = None

    def _unlink_named_identity(self) -> None:
        if self._root_fd is None:
            raise ProcessLifetimeError("lifetime-lock root descriptor is unavailable")
        if self._name_unlinked:
            # A prior call successfully removed this exact name but failed to
            # fsync the containing directory.  Retrying that durability step
            # on the same descriptor owner is safe and must not reinterpret
            # the now-absent name as an identity mismatch.
            os.fsync(self._root_fd)
            return
        root_stat = os.fstat(self._root_fd)
        if (root_stat.st_dev, root_stat.st_ino) != (
            self.root_device,
            self.root_inode,
        ):
            raise ProcessIdentityMismatchError("lifetime-lock root identity changed")
        try:
            named_stat = os.stat(
                self.filename,
                dir_fd=self._root_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError as exc:
            raise ProcessIdentityMismatchError(
                "recoverable lifetime-lock name disappeared"
            ) from exc
        if (
            not stat.S_ISREG(named_stat.st_mode)
            or named_stat.st_uid != os.geteuid()
            or stat.S_IMODE(named_stat.st_mode) != 0o600
            or named_stat.st_nlink != 1
            or (named_stat.st_dev, named_stat.st_ino) != (self.device, self.inode)
        ):
            raise ProcessIdentityMismatchError(
                "recoverable lifetime-lock name or link identity changed"
            )
        os.unlink(self.filename, dir_fd=self._root_fd)
        self._name_unlinked = True
        os.fsync(self._root_fd)

    def prove_released(self) -> bool:
        """Acquire the probe, unlink the exact name, and close all evidence."""

        if self._released:
            return True
        if self._holder_fd is not None:
            return False
        if self._probe_fd is None:
            raise ProcessLifetimeError("lifetime-lock probe is unavailable")
        if not self._name_unlinked:
            _validate_lock_file(
                self._probe_fd,
                expected_device=self.device,
                expected_inode=self.inode,
            )
            try:
                fcntl.flock(self._probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
        self._unlink_named_identity()
        self._released = True
        os.close(self._probe_fd)
        self._probe_fd = None
        if self._root_fd is not None:
            os.close(self._root_fd)
            self._root_fd = None
        return True

    def close_after_release(self) -> None:
        if not self.prove_released():
            raise ProcessLifetimeStillHeldError(
                "inherited Agent lifetime lock remains held"
            )


@dataclass(slots=True)
class RecoveredLifetimeLock:
    """Exact named lock evidence retained across durable recovery fencing.

    Unlike :meth:`InheritedLifetimeLock.reopen`, opening this object does not
    try to take the lock immediately.  A crashed supervisor must be able to
    retain the exact inode/root evidence while the old child or one of its
    descendants still owns the inherited open-file description.  Once the
    probe acquires the lock, the old generation is proven to have released it,
    but the name remains in place until the durable stop fence commits.  This
    ordering makes a replacement crash between OS cleanup and SQLite commit
    retryable.
    """

    identity: str
    device: int
    inode: int
    root_path: str
    filename: str
    root_device: int
    root_inode: int
    _root_fd: int | None
    _probe_fd: int | None
    _release_proven: bool = False
    _name_unlinked: bool = False
    _finalized: bool = False

    @classmethod
    def reopen(
        cls,
        lock_root: str | os.PathLike[str],
        identity: str,
    ) -> RecoveredLifetimeLock:
        if type(identity) is not str:
            raise TypeError("lifetime lock identity must be a string")
        match = _LOCK_IDENTITY_PATTERN.fullmatch(identity)
        if match is None:
            raise ProcessIdentityMismatchError(
                "lifetime lock identity is not recoverable v2 metadata"
            )
        (
            boot_id,
            root_device_hex,
            root_inode_hex,
            device_hex,
            inode_hex,
            filename,
        ) = match.groups()
        if boot_id != _BOOT_ID:
            raise ProcessIdentityMismatchError(
                "lifetime lock belongs to another kernel boot"
            )
        expected_root = (int(root_device_hex, 16), int(root_inode_hex, 16))
        expected_file = (int(device_hex, 16), int(inode_hex, 16))
        root_fd: int | None = None
        probe_fd: int | None = None
        try:
            root_path, root_fd, root_stat = _open_secure_lock_root(
                lock_root,
                create=False,
            )
            if (root_stat.st_dev, root_stat.st_ino) != expected_root:
                raise ProcessIdentityMismatchError(
                    "lifetime lock root identity changed"
                )
            try:
                probe_fd = os.open(
                    filename,
                    _lock_open_flags(),
                    dir_fd=root_fd,
                )
            except FileNotFoundError as exc:
                raise ProcessLifetimeArtifactMissingError(
                    "recoverable lifetime-lock name is missing"
                ) from exc
            except OSError as exc:
                raise ProcessIdentityMismatchError(
                    "recoverable lifetime-lock name is unsafe"
                ) from exc
            _validate_lock_file(
                probe_fd,
                expected_device=expected_file[0],
                expected_inode=expected_file[1],
            )
            os.set_inheritable(probe_fd, False)
            return cls(
                identity=identity,
                device=expected_file[0],
                inode=expected_file[1],
                root_path=root_path,
                filename=filename,
                root_device=expected_root[0],
                root_inode=expected_root[1],
                _root_fd=root_fd,
                _probe_fd=probe_fd,
            )
        except BaseException:
            for descriptor in (probe_fd, root_fd):
                if descriptor is not None:
                    with contextlib.suppress(OSError):
                        os.close(descriptor)
            raise

    @property
    def path(self) -> str:
        return os.path.join(self.root_path, self.filename)

    @property
    def release_proven(self) -> bool:
        return self._release_proven

    @property
    def finalized(self) -> bool:
        return self._finalized

    def _validate_named_identity(self) -> None:
        if self._root_fd is None or self._probe_fd is None:
            raise ProcessLifetimeError("recovered lifetime-lock evidence is closed")
        root_stat = os.fstat(self._root_fd)
        if (root_stat.st_dev, root_stat.st_ino) != (
            self.root_device,
            self.root_inode,
        ):
            raise ProcessIdentityMismatchError(
                "lifetime-lock root identity changed"
            )
        _validate_lock_file(
            self._probe_fd,
            expected_device=self.device,
            expected_inode=self.inode,
        )
        try:
            named_stat = os.stat(
                self.filename,
                dir_fd=self._root_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError as exc:
            raise ProcessIdentityMismatchError(
                "recoverable lifetime-lock name disappeared"
            ) from exc
        if (
            not stat.S_ISREG(named_stat.st_mode)
            or named_stat.st_uid != os.geteuid()
            or stat.S_IMODE(named_stat.st_mode) != 0o600
            or named_stat.st_nlink != 1
            or (named_stat.st_dev, named_stat.st_ino) != (self.device, self.inode)
        ):
            raise ProcessIdentityMismatchError(
                "recoverable lifetime-lock name or link identity changed"
            )

    def try_prove_released(self) -> bool:
        """Retain the exclusive probe without unlinking durable evidence."""

        if self._finalized:
            return True
        if self._release_proven:
            if not self._name_unlinked:
                self._validate_named_identity()
            return True
        self._validate_named_identity()
        assert self._probe_fd is not None
        try:
            fcntl.flock(self._probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        self._validate_named_identity()
        self._release_proven = True
        return True

    def finalize_unlink(self) -> None:
        """Unlink the exact inode only after the durable stop is committed."""

        if self._finalized:
            return
        if not self._release_proven:
            raise ProcessLifetimeError(
                "lifetime-lock release must be proven before finalization"
            )
        if not self._name_unlinked:
            # Missing-before-unlink remains an identity failure.  Only this
            # descriptor owner's successful unlink authorizes an fsync-only
            # retry after a durability error.
            self._validate_named_identity()
            assert self._root_fd is not None
            os.unlink(self.filename, dir_fd=self._root_fd)
            self._name_unlinked = True
        if self._root_fd is None:  # pragma: no cover - guarded above/close API
            raise ProcessLifetimeError("recovered lifetime-lock evidence is closed")
        os.fsync(self._root_fd)
        self._finalized = True
        self.close()

    def close(self) -> None:
        """Release local evidence without unlinking a retryable lock name."""

        for attribute in ("_probe_fd", "_root_fd"):
            descriptor = getattr(self, attribute)
            if descriptor is not None:
                with contextlib.suppress(OSError):
                    os.close(descriptor)
                setattr(self, attribute, None)


@dataclass(slots=True)
class RecoveredProcessGroup:
    """A persisted dedicated Linux group, including leader-gone recovery."""

    leader_pid: int
    process_group_id: int
    session_id: int
    leader_start_time_ticks: int
    boot_id: str
    _leader_pidfd: int | None

    @classmethod
    def from_persisted(
        cls,
        *,
        pid: int,
        process_group_id: int,
        kernel_process_birth_id: str,
    ) -> RecoveredProcessGroup:
        if type(pid) is not int or pid <= 0:
            raise ValueError("persisted Agent pid must be a positive integer")
        if type(process_group_id) is not int or process_group_id <= 0:
            raise ValueError(
                "persisted Agent process_group_id must be a positive integer"
            )
        if pid != process_group_id:
            raise ProcessIdentityMismatchError(
                "persisted Agent process group is not dedicated"
            )
        if type(kernel_process_birth_id) is not str:
            raise TypeError("kernel process birth identity must be a string")
        match = _PROCESS_BIRTH_ID_PATTERN.fullmatch(kernel_process_birth_id)
        if match is None:
            raise ProcessIdentityMismatchError(
                "kernel process birth identity is not recoverable Linux metadata"
            )
        boot_id, start_time_text = match.groups()
        if boot_id != _BOOT_ID:
            raise ProcessIdentityMismatchError(
                "Agent process generation belongs to another kernel boot"
            )
        start_time_ticks = int(start_time_text)
        leader_pidfd: int | None = None
        try:
            current = capture_linux_process_birth(pid)
        except ProcessLookupError:
            current = None
        if current is not None:
            if (
                current.process_group_id != process_group_id
                or current.session_id != process_group_id
                or current.start_time_ticks != start_time_ticks
                or current.boot_id != boot_id
            ):
                raise ProcessIdentityMismatchError(
                    "persisted Agent leader birth identity changed"
                )
            leader_pidfd = _open_pidfd(pid)
            try:
                verified = capture_linux_process_birth(pid)
                if (
                    verified.process_group_id != process_group_id
                    or verified.session_id != process_group_id
                    or verified.start_time_ticks != start_time_ticks
                    or verified.boot_id != boot_id
                ):
                    raise ProcessIdentityMismatchError(
                        "Agent leader changed during recovery pidfd capture"
                    )
            except BaseException:
                os.close(leader_pidfd)
                raise
        recovered = cls(
            leader_pid=pid,
            process_group_id=process_group_id,
            session_id=process_group_id,
            leader_start_time_ticks=start_time_ticks,
            boot_id=boot_id,
            _leader_pidfd=leader_pidfd,
        )
        try:
            recovered._members()
        except BaseException:
            recovered.close()
            raise
        return recovered

    def close(self) -> None:
        if self._leader_pidfd is not None:
            os.close(self._leader_pidfd)
            self._leader_pidfd = None

    def _members(self) -> list[LinuxProcessBirthIdentity]:
        members: list[LinuxProcessBirthIdentity] = []
        try:
            with os.scandir("/proc") as iterator:
                entries = tuple(iterator)
        except OSError as exc:
            raise ProcessInspectionUnavailableError(
                "cannot enumerate Linux process identities"
            ) from exc
        for entry in entries:
            if not entry.name.isascii() or not entry.name.isdigit():
                continue
            candidate_pid = int(entry.name)
            try:
                identity = capture_linux_process_birth(candidate_pid)
            except ProcessLookupError:
                continue
            except ProcessInspectionUnavailableError:
                raise
            if identity.process_group_id != self.process_group_id:
                continue
            if identity.session_id != self.session_id:
                raise ProcessIdentityMismatchError(
                    "Agent process-group number was reused by another session"
                )
            if identity.pid == self.leader_pid and (
                identity.start_time_ticks != self.leader_start_time_ticks
                or identity.boot_id != self.boot_id
            ):
                raise ProcessIdentityMismatchError(
                    "Agent process-group leader PID was reused"
                )
            members.append(identity)
        if self._leader_pidfd is None and members:
            # Once the persisted leader is already absent, Linux exposes no
            # kernel object tying a merely equal numeric PGID/SID to that old
            # generation.  It may have been reused, so never pidfd-signal any
            # member selected only by those numeric values.
            raise ProcessIdentityMismatchError(
                "persisted Agent leader is missing while its numeric "
                "process group is nonempty"
            )
        return members

    def is_empty(self) -> bool:
        return not self._members()

    def signal_members(self, sig: int) -> int:
        signalled = 0
        for identity in self._members():
            try:
                descriptor = _open_pidfd(identity.pid)
            except ProcessLookupError:
                continue
            try:
                try:
                    current = capture_linux_process_birth(identity.pid)
                except ProcessLookupError:
                    continue
                if current != identity:
                    raise ProcessIdentityMismatchError(
                        f"PID {identity.pid} changed during recovery targeting"
                    )
                _send_pidfd_signal(descriptor, sig)
                signalled += 1
            finally:
                os.close(descriptor)
        return signalled


@dataclass(slots=True)
class VerifiedProcessGroup:
    """A dedicated Linux session/group bound to its leader's birth identity."""

    leader: LinuxProcessBirthIdentity
    _leader_pidfd: int | None

    @classmethod
    def capture_dedicated(cls, pid: int) -> VerifiedProcessGroup:
        leader = capture_linux_process_birth(pid)
        if leader.process_group_id != pid or leader.session_id != pid:
            raise ProcessIdentityMismatchError(
                "Agent child does not own a dedicated Linux session/process group"
            )
        descriptor = _open_pidfd(pid)
        try:
            if capture_linux_process_birth(pid) != leader:
                raise ProcessIdentityMismatchError(
                    "Agent child birth identity changed during pidfd capture"
                )
        except BaseException:
            os.close(descriptor)
            raise
        return cls(leader=leader, _leader_pidfd=descriptor)

    @property
    def process_group_id(self) -> int:
        return self.leader.process_group_id

    @property
    def kernel_birth_id(self) -> str:
        return self.leader.wire_birth_id

    def close(self) -> None:
        if self._leader_pidfd is not None:
            os.close(self._leader_pidfd)
            self._leader_pidfd = None

    def _members(self) -> list[LinuxProcessBirthIdentity]:
        members: list[LinuxProcessBirthIdentity] = []
        reused: list[LinuxProcessBirthIdentity] = []
        try:
            with os.scandir("/proc") as iterator:
                entries = tuple(iterator)
        except OSError as exc:
            raise ProcessInspectionUnavailableError(
                "cannot enumerate Linux process identities"
            ) from exc
        for entry in entries:
            if not entry.name.isascii() or not entry.name.isdigit():
                continue
            pid = int(entry.name)
            try:
                identity = capture_linux_process_birth(pid)
            except ProcessLookupError:
                continue
            except ProcessInspectionUnavailableError:
                # Inability to inspect any numeric /proc entry means absence
                # cannot be established safely on hidepid-style systems.
                raise
            if identity.process_group_id != self.process_group_id:
                continue
            if identity.session_id != self.leader.session_id:
                reused.append(identity)
            else:
                members.append(identity)
        if reused:
            raise ProcessIdentityMismatchError(
                "Agent process-group number was reused by another session"
            )
        for member in members:
            if member.pid == self.leader.pid and member != self.leader:
                raise ProcessIdentityMismatchError(
                    "Agent process-group leader PID was reused"
                )
        return members

    def is_empty(self) -> bool:
        return not self._members()

    def signal_members(self, sig: int) -> int:
        """Signal one stable snapshot, binding every target through a pidfd."""

        signalled = 0
        for identity in self._members():
            try:
                descriptor = _open_pidfd(identity.pid)
            except ProcessLookupError:
                continue
            try:
                try:
                    current = capture_linux_process_birth(identity.pid)
                except ProcessLookupError:
                    continue
                if current != identity:
                    raise ProcessIdentityMismatchError(
                        f"PID {identity.pid} changed during cleanup targeting"
                    )
                _send_pidfd_signal(descriptor, sig)
                signalled += 1
            finally:
                os.close(descriptor)
        return signalled


@dataclass(slots=True)
class ExactProcessPidfd:
    """An exact direct-child kernel handle used before group verification.

    This guard exists only for the small interval after ``Popen`` and before
    bootstrap material is written.  The trusted child is blocked on the
    bootstrap pipe and therefore cannot have created descendants.  A failed
    group capture can close that pipe, wait for ordinary EOF exit, and use this
    pidfd as a final exact-child kill fence without ever targeting a bare PID.
    """

    pid: int
    _descriptor: int | None

    @classmethod
    def capture(cls, pid: int) -> ExactProcessPidfd:
        return cls(pid=pid, _descriptor=_open_pidfd(pid))

    @property
    def closed(self) -> bool:
        return self._descriptor is None

    def signal(self, sig: int) -> None:
        if self._descriptor is None:
            raise ProcessLifetimeError("exact-child pidfd is closed")
        _send_pidfd_signal(self._descriptor, sig)

    def close(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None


async def terminate_exact_prebootstrap_process(
    process: object,
    exact_process: ExactProcessPidfd,
    lifetime_lock: InheritedLifetimeLock,
    timeout: float,
) -> None:
    """Reap one trusted pre-bootstrap child and prove descriptor release."""

    lifetime_lock.release_parent_copy()
    poll = getattr(process, "poll", None)
    if poll is None:
        raise TypeError("process must provide poll()")
    total = max(0.0, float(timeout))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total
    kill_at = loop.time() + total / 2.0
    kill_sent = False
    while True:
        returncode = poll()
        if returncode is not None and lifetime_lock.prove_released():
            exact_process.close()
            return
        now = loop.time()
        if not kill_sent and now >= kill_at:
            exact_process.signal(signal.SIGKILL)
            kill_sent = True
        if now >= deadline:
            break
        await asyncio.sleep(min(0.02, deadline - now))
    raise ProcessLifetimeStillHeldError(
        "exact pre-bootstrap child could not be reaped with lock release"
    )


async def wait_for_verified_exit(
    process: object,
    group: VerifiedProcessGroup,
    lifetime_lock: InheritedLifetimeLock,
    *,
    timeout: float | None,
) -> bool:
    """Reap the leader and prove both group emptiness and lock release."""

    deadline = (
        None
        if timeout is None
        else asyncio.get_running_loop().time() + max(0.0, float(timeout))
    )
    while True:
        poll = getattr(process, "poll", None)
        if poll is None:
            raise TypeError("process must provide poll()")
        returncode = poll()
        if returncode is not None and group.is_empty() and lifetime_lock.prove_released():
            group.close()
            return True
        if deadline is not None:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.02, remaining))
        else:
            await asyncio.sleep(0.02)


async def terminate_verified_process_group(
    process: object,
    group: VerifiedProcessGroup,
    lifetime_lock: InheritedLifetimeLock,
    timeout: float,
) -> None:
    """Terminate an exact group without ever signaling a bare numeric PID."""

    lifetime_lock.release_parent_copy()
    total = max(0.0, float(timeout))
    if await wait_for_verified_exit(
        process,
        group,
        lifetime_lock,
        timeout=0,
    ):
        return

    loop = asyncio.get_running_loop()
    started = loop.time()
    grace_deadline = started + total / 2.0
    final_deadline = started + total
    phase_signal = signal.SIGTERM
    while True:
        now = loop.time()
        if now >= final_deadline:
            break
        if now >= grace_deadline:
            phase_signal = signal.SIGKILL
        group.signal_members(phase_signal)
        slice_timeout = min(0.05, max(0.0, final_deadline - now))
        if await wait_for_verified_exit(
            process,
            group,
            lifetime_lock,
            timeout=slice_timeout,
        ):
            return
    group_empty = group.is_empty()
    lock_released = lifetime_lock.prove_released()
    detail = []
    if not group_empty:
        detail.append("verified process group remains live")
    if not lock_released:
        detail.append("inherited lifetime lock remains held")
    raise ProcessLifetimeStillHeldError("; ".join(detail) or "cleanup proof failed")


async def terminate_recovered_process_group(
    group: RecoveredProcessGroup,
    lifetime_lock: RecoveredLifetimeLock,
    timeout: float,
) -> None:
    """Stop one persisted group and retain its lock proof for store fencing.

    A replacement supervisor cannot reap a former supervisor's child with
    ``waitpid``.  Instead it repeatedly enumerates the persisted dedicated
    session, targets every exact member through a pidfd, and waits until no
    member remains.  The separately opened lifetime probe must also acquire
    the exact inherited lock.  The probe stays locked and named on return so a
    caller can commit the durable stop before finalizing the filesystem name.
    """

    if not isinstance(group, RecoveredProcessGroup):
        raise TypeError("group must be a RecoveredProcessGroup")
    if not isinstance(lifetime_lock, RecoveredLifetimeLock):
        raise TypeError("lifetime_lock must be a RecoveredLifetimeLock")
    if isinstance(timeout, bool):
        raise ValueError("timeout must be a non-negative finite number")
    try:
        total = float(timeout)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout must be a non-negative finite number") from exc
    if not total >= 0.0 or total == float("inf"):
        raise ValueError("timeout must be a non-negative finite number")

    loop = asyncio.get_running_loop()
    started = loop.time()
    grace_deadline = started + total / 2.0
    final_deadline = started + total
    while True:
        group_empty = group.is_empty()
        lock_released = lifetime_lock.try_prove_released()
        if group_empty and lock_released:
            return
        now = loop.time()
        if now >= final_deadline:
            detail = []
            if not group_empty:
                detail.append("verified recovered process group remains live")
            if not lock_released:
                detail.append("recovered inherited lifetime lock remains held")
            raise ProcessLifetimeStillHeldError(
                "; ".join(detail) or "recovered cleanup proof failed"
            )
        group.signal_members(
            signal.SIGKILL if now >= grace_deadline else signal.SIGTERM
        )
        await asyncio.sleep(min(0.02, max(0.0, final_deadline - now)))


__all__ = [
    "DEFAULT_AGENT_LIFETIME_LOCK_ROOT",
    "ExactProcessPidfd",
    "InheritedLifetimeLock",
    "LinuxProcessBirthIdentity",
    "ProcessIdentityMismatchError",
    "ProcessInspectionUnavailableError",
    "ProcessLifetimeArtifactMissingError",
    "ProcessLifetimeError",
    "ProcessLifetimeStillHeldError",
    "RecoveredLifetimeLock",
    "RecoveredProcessGroup",
    "VerifiedProcessGroup",
    "capture_linux_process_birth",
    "linux_process_birth_matches",
    "require_linux_pidfd_support",
    "terminate_verified_process_group",
    "terminate_exact_prebootstrap_process",
    "terminate_recovered_process_group",
    "wait_for_verified_exit",
]
