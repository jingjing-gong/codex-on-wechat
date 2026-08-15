"""Disconnected cgroup-v2 job-containment evidence.

This module is intentionally *not* a job allocator, launcher, killer, or
scheduler integration.  It provides only the fail-closed pieces needed before
those operations can be designed safely:

* a canonical, hash-bound identity for an already-created cgroup job;
* secure descriptor-relative reopening of that exact identity;
* strict parsing of the kernel's ``cgroup.events`` evidence; and
* a non-mutating prerequisite probe for a delegated cgroup-v2 root and an
  actually runnable namespace launcher.

In particular, finding a ``bwrap`` binary -- or observing a zero exit from the
small namespace prerequisite check -- is not sufficient.  A network-preserving
launcher also needs proof against PID/IPC, session-bus, local-socket, alternate
cgroup-mount, and cooperative same-user escape.  Those proofs do not exist in
this module, so :func:`probe_cgroup_v2_containment` deliberately never returns
``available=True``.  No cgroup is created, populated, killed, or removed here.
Production child readiness and dispatch must remain disabled until those escape
gates plus allocation, launch-grant, cleanup, and durable recovery exist.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, ClassVar, Mapping, Sequence


class JobContainmentError(RuntimeError):
    """Base error for disconnected job-containment evidence."""


class CgroupJobIdentityError(ValueError, JobContainmentError):
    """A cgroup job identity is malformed or not canonically encoded."""


class CgroupJobIdentityMismatchError(JobContainmentError):
    """A reopened kernel/filesystem object does not match its identity."""


class CgroupPathSecurityError(JobContainmentError):
    """A path cannot be opened without traversal or symlink ambiguity."""


_BOOT_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_JOB_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_JOB_COMPONENT_PATTERN = re.compile(r"^job-([0-9a-f]{32})$")
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_EVENT_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_CANONICAL_UNSIGNED_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)$")
_MAX_IDENTITY_BYTES = 16 * 1024
_MAX_OWNER_PARTS = 16
_MAX_OWNER_STRING_BYTES = 256
_MAX_EVENTS_BYTES = 4096


def read_linux_boot_id() -> str:
    """Return the canonical Linux boot UUID or fail closed."""

    if not sys.platform.startswith("linux"):
        raise JobContainmentError("cgroup-v2 containment requires Linux")
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except OSError as exc:
        raise JobContainmentError("Linux boot identity is unavailable") from exc
    if _BOOT_ID_PATTERN.fullmatch(value) is None:
        raise JobContainmentError("Linux boot identity is malformed")
    return value


def _require_plain_int(value: Any, field: str, *, positive: bool) -> int:
    if type(value) is not int:
        raise CgroupJobIdentityError(f"{field} must be an integer")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "nonnegative"
        raise CgroupJobIdentityError(f"{field} must be {qualifier}")
    if value > (1 << 63) - 1:
        raise CgroupJobIdentityError(f"{field} is out of range")
    return value


def _validate_owner(owner: Any) -> tuple[str | int, ...]:
    if type(owner) is not tuple:
        raise CgroupJobIdentityError("owner must be an immutable tuple")
    if not owner or len(owner) > _MAX_OWNER_PARTS:
        raise CgroupJobIdentityError("owner tuple has an invalid size")
    if type(owner[0]) is not str:
        raise CgroupJobIdentityError("owner tuple must start with a string kind")
    validated: list[str | int] = []
    for part in owner:
        if type(part) is int:
            if part < 0 or part > (1 << 63) - 1:
                raise CgroupJobIdentityError("owner integer is out of range")
            validated.append(part)
            continue
        if type(part) is not str or not part:
            raise CgroupJobIdentityError(
                "owner parts must be nonempty strings or nonnegative integers"
            )
        if unicodedata.normalize("NFC", part) != part:
            raise CgroupJobIdentityError("owner strings must be NFC-normalized")
        try:
            encoded = part.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise CgroupJobIdentityError(
                "owner strings must contain valid Unicode scalars"
            ) from exc
        if len(encoded) > _MAX_OWNER_STRING_BYTES:
            raise CgroupJobIdentityError("owner string is too large")
        if any(unicodedata.category(character) in {"Cc", "Cs"} for character in part):
            raise CgroupJobIdentityError(
                "owner strings must not contain controls or surrogates"
            )
        validated.append(part)
    return tuple(validated)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class CgroupJobIdentityV1:
    """Immutable filesystem identity intended for an existing cgroup job.

    The identity proves only exact metadata and canonical persistence.  It is
    not evidence that the underlying filesystem is cgroup v2 or that a process
    cannot escape it; that requires a separately held, verified containment
    capability which this disconnected module does not yet issue.
    """

    boot_id: str
    mount_id: int
    root_device: int
    root_inode: int
    job_id: str
    owner: tuple[str | int, ...]
    relative_component: str
    job_device: int
    job_inode: int
    canonical_hash: str

    IDENTITY_VERSION: ClassVar[int] = 1
    BACKEND: ClassVar[str] = "cgroup-v2"
    _WIRE_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            "identity_version",
            "backend",
            "boot_id",
            "mount_id",
            "root_device",
            "root_inode",
            "job_id",
            "owner",
            "relative_component",
            "job_device",
            "job_inode",
            "canonical_hash",
        }
    )

    def __post_init__(self) -> None:
        if type(self.boot_id) is not str or _BOOT_ID_PATTERN.fullmatch(self.boot_id) is None:
            raise CgroupJobIdentityError("boot_id must be a canonical lowercase UUID")
        _require_plain_int(self.mount_id, "mount_id", positive=True)
        _require_plain_int(self.root_device, "root_device", positive=False)
        _require_plain_int(self.root_inode, "root_inode", positive=True)
        _require_plain_int(self.job_device, "job_device", positive=False)
        _require_plain_int(self.job_inode, "job_inode", positive=True)
        if type(self.job_id) is not str or _JOB_ID_PATTERN.fullmatch(self.job_id) is None:
            raise CgroupJobIdentityError("job_id must be 128 bits of lowercase hex")
        owner = _validate_owner(self.owner)
        if owner != self.owner:  # pragma: no cover - validation is identity preserving
            raise CgroupJobIdentityError("owner tuple changed during validation")
        if type(self.relative_component) is not str:
            raise CgroupJobIdentityError("relative_component must be a string")
        component_match = _JOB_COMPONENT_PATTERN.fullmatch(self.relative_component)
        if component_match is None or component_match.group(1) != self.job_id:
            raise CgroupJobIdentityError(
                "relative_component must be the safe component for job_id"
            )
        if type(self.canonical_hash) is not str or _HASH_PATTERN.fullmatch(
            self.canonical_hash
        ) is None:
            raise CgroupJobIdentityError("canonical_hash is malformed")
        expected = self._hash_payload(self._payload())
        if not hmac.compare_digest(self.canonical_hash, expected):
            raise CgroupJobIdentityError("cgroup job identity hash does not match")

    @classmethod
    def issue(
        cls,
        *,
        boot_id: str,
        mount_id: int,
        root_device: int,
        root_inode: int,
        job_id: str,
        owner: tuple[str | int, ...],
        relative_component: str,
        job_device: int,
        job_inode: int,
    ) -> CgroupJobIdentityV1:
        """Hash-bind validated metadata for an existing job.

        This method performs no allocation and does not establish that the job
        is safe to execute in.  Callers should normally use
        :func:`bind_existing_cgroup_job` with a held root descriptor.
        """

        payload: dict[str, Any] = {
            "identity_version": cls.IDENTITY_VERSION,
            "backend": cls.BACKEND,
            "boot_id": boot_id,
            "mount_id": mount_id,
            "root_device": root_device,
            "root_inode": root_inode,
            "job_id": job_id,
            "owner": list(owner) if type(owner) is tuple else owner,
            "relative_component": relative_component,
            "job_device": job_device,
            "job_inode": job_inode,
        }
        canonical_hash = cls._hash_payload(payload)
        return cls(
            boot_id=boot_id,
            mount_id=mount_id,
            root_device=root_device,
            root_inode=root_inode,
            job_id=job_id,
            owner=owner,
            relative_component=relative_component,
            job_device=job_device,
            job_inode=job_inode,
            canonical_hash=canonical_hash,
        )

    def _payload(self) -> dict[str, Any]:
        return {
            "identity_version": self.IDENTITY_VERSION,
            "backend": self.BACKEND,
            "boot_id": self.boot_id,
            "mount_id": self.mount_id,
            "root_device": self.root_device,
            "root_inode": self.root_inode,
            "job_id": self.job_id,
            "owner": list(self.owner),
            "relative_component": self.relative_component,
            "job_device": self.job_device,
            "job_inode": self.job_inode,
        }

    @staticmethod
    def _hash_payload(payload: Mapping[str, Any]) -> str:
        encoded = _canonical_json(payload).encode("ascii")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def to_wire(self) -> str:
        value = self._payload()
        value["canonical_hash"] = self.canonical_hash
        return _canonical_json(value)

    @classmethod
    def parse(cls, wire: str) -> CgroupJobIdentityV1:
        """Parse only the exact canonical wire representation."""

        if type(wire) is not str:
            raise CgroupJobIdentityError("cgroup job identity wire value must be text")
        try:
            encoded = wire.encode("ascii", errors="strict")
        except UnicodeEncodeError as exc:
            raise CgroupJobIdentityError(
                "cgroup job identity wire value must be canonical ASCII JSON"
            ) from exc
        if not encoded or len(encoded) > _MAX_IDENTITY_BYTES:
            raise CgroupJobIdentityError("cgroup job identity wire size is invalid")

        def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise CgroupJobIdentityError(
                        f"duplicate cgroup job identity field: {key}"
                    )
                result[key] = value
            return result

        def reject_constant(value: str) -> None:
            raise CgroupJobIdentityError(
                f"invalid cgroup job identity JSON constant: {value}"
            )

        try:
            decoded = json.loads(
                wire,
                object_pairs_hook=reject_duplicates,
                parse_constant=reject_constant,
            )
        except CgroupJobIdentityError:
            raise
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise CgroupJobIdentityError(
                "cgroup job identity is not valid JSON"
            ) from exc
        if type(decoded) is not dict or frozenset(decoded) != cls._WIRE_FIELDS:
            raise CgroupJobIdentityError(
                "cgroup job identity fields do not match version 1"
            )
        if type(decoded["identity_version"]) is not int or decoded[
            "identity_version"
        ] != cls.IDENTITY_VERSION:
            raise CgroupJobIdentityError("unsupported cgroup job identity version")
        if type(decoded["backend"]) is not str or decoded["backend"] != cls.BACKEND:
            raise CgroupJobIdentityError("cgroup job identity backend is invalid")
        if type(decoded["owner"]) is not list:
            raise CgroupJobIdentityError("owner wire value must be an array")
        identity = cls(
            boot_id=decoded["boot_id"],
            mount_id=decoded["mount_id"],
            root_device=decoded["root_device"],
            root_inode=decoded["root_inode"],
            job_id=decoded["job_id"],
            owner=tuple(decoded["owner"]),
            relative_component=decoded["relative_component"],
            job_device=decoded["job_device"],
            job_inode=decoded["job_inode"],
            canonical_hash=decoded["canonical_hash"],
        )
        if identity.to_wire() != wire:
            raise CgroupJobIdentityError(
                "cgroup job identity wire value is not canonical"
            )
        return identity


def new_cgroup_job_name() -> tuple[str, str]:
    """Return a random 128-bit job ID and its one-component safe name."""

    job_id = secrets.token_hex(16)
    if _JOB_ID_PATTERN.fullmatch(job_id) is None:  # pragma: no cover
        raise JobContainmentError("secure random job ID generation failed")
    return job_id, f"job-{job_id}"


def _absolute_components(path: str | os.PathLike[str]) -> tuple[str, tuple[str, ...]]:
    try:
        raw = os.fspath(path)
    except TypeError as exc:
        raise CgroupPathSecurityError("cgroup root must be a filesystem path") from exc
    if type(raw) is not str or not raw or not os.path.isabs(raw):
        raise CgroupPathSecurityError("cgroup root must be an absolute text path")
    if raw != os.path.normpath(raw):
        raise CgroupPathSecurityError(
            "cgroup root must not contain traversal or redundant components"
        )
    parts = tuple(raw.split("/")[1:])
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise CgroupPathSecurityError("cgroup root components are unsafe")
    return raw, parts


def _directory_open_flags() -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _open_absolute_directory_no_symlinks(
    path: str | os.PathLike[str],
) -> tuple[str, int, os.stat_result]:
    canonical, components = _absolute_components(path)
    descriptor: int | None = None
    try:
        descriptor = os.open("/", _directory_open_flags())
        for component in components:
            next_descriptor = os.open(
                component,
                _directory_open_flags(),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):  # pragma: no cover - O_DIRECTORY
            raise CgroupPathSecurityError("cgroup root is not a directory")
        if metadata.st_uid != os.geteuid():
            raise CgroupPathSecurityError(
                "cgroup root is not owned by the effective user"
            )
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise CgroupPathSecurityError(
                "cgroup root is writable by another principal"
            )
        os.set_inheritable(descriptor, False)
        return canonical, descriptor, metadata
    except CgroupPathSecurityError:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise CgroupPathSecurityError(
            "cgroup root contains a symlink, disappeared, or cannot be opened"
        ) from exc


def _mount_id_for_fd(descriptor: int) -> int:
    try:
        text = Path(f"/proc/self/fdinfo/{descriptor}").read_text(encoding="ascii")
    except OSError as exc:
        raise JobContainmentError("descriptor mount identity is unavailable") from exc
    values: list[int] = []
    for line in text.splitlines():
        if line.startswith("mnt_id:"):
            raw = line[len("mnt_id:") :].strip()
            if not raw.isascii() or not raw.isdecimal():
                raise JobContainmentError("descriptor mount identity is malformed")
            values.append(int(raw))
    if len(values) != 1 or values[0] <= 0:
        raise JobContainmentError("descriptor mount identity is ambiguous")
    return values[0]


@dataclass(slots=True)
class HeldCgroupRoot:
    """An exact, securely walked cgroup root descriptor."""

    path: str
    device: int
    inode: int
    mount_id: int
    _descriptor: int | None

    @property
    def descriptor(self) -> int:
        if self._descriptor is None:
            raise JobContainmentError("held cgroup root is closed")
        return self._descriptor

    def validate(self) -> os.stat_result:
        metadata = os.fstat(self.descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) & 0o022
            or (
                metadata.st_dev,
                metadata.st_ino,
                _mount_id_for_fd(self.descriptor),
            )
            != (self.device, self.inode, self.mount_id)
        ):
            raise CgroupJobIdentityMismatchError(
                "held cgroup root identity or permissions changed"
            )
        return metadata

    def close(self) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is not None:
            os.close(descriptor)

    def __enter__(self) -> HeldCgroupRoot:
        try:
            self.validate()
            return self
        except BaseException:
            with contextlib.suppress(OSError):
                self.close()
            raise

    def __exit__(self, *_exc: object) -> None:
        self.close()


def open_secure_cgroup_root(path: str | os.PathLike[str]) -> HeldCgroupRoot:
    """Walk and hold an absolute, owned root without following any symlink."""

    canonical, descriptor, metadata = _open_absolute_directory_no_symlinks(path)
    try:
        return HeldCgroupRoot(
            path=canonical,
            device=metadata.st_dev,
            inode=metadata.st_ino,
            mount_id=_mount_id_for_fd(descriptor),
            _descriptor=descriptor,
        )
    except BaseException:
        os.close(descriptor)
        raise


def _validate_job_component(relative_component: str) -> str:
    if type(relative_component) is not str or _JOB_COMPONENT_PATTERN.fullmatch(
        relative_component
    ) is None:
        raise CgroupJobIdentityError(
            "job path must be one generated relative component"
        )
    return relative_component


def _open_job_directory(
    root: HeldCgroupRoot,
    relative_component: str,
) -> tuple[int, os.stat_result, int]:
    component = _validate_job_component(relative_component)
    root.validate()
    descriptor: int | None = None
    try:
        descriptor = os.open(
            component,
            _directory_open_flags(),
            dir_fd=root.descriptor,
        )
        metadata = os.fstat(descriptor)
        named = os.stat(
            component,
            dir_fd=root.descriptor,
            follow_symlinks=False,
        )
        if not stat.S_ISDIR(metadata.st_mode):  # pragma: no cover - O_DIRECTORY
            raise CgroupJobIdentityMismatchError("cgroup job is not a directory")
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise CgroupJobIdentityMismatchError(
                "cgroup job ownership or permissions are unsafe"
            )
        if (named.st_dev, named.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise CgroupJobIdentityMismatchError(
                "cgroup job name changed while it was opened"
            )
        mount_id = _mount_id_for_fd(descriptor)
        if mount_id != root.mount_id:
            raise CgroupJobIdentityMismatchError(
                "cgroup job crossed the held root mount"
            )
        os.set_inheritable(descriptor, False)
        return descriptor, metadata, mount_id
    except JobContainmentError:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise CgroupJobIdentityMismatchError(
            "cgroup job name is missing, symlinked, or unsafe"
        ) from exc


def bind_existing_cgroup_job(
    root: HeldCgroupRoot,
    *,
    relative_component: str,
    owner: tuple[str | int, ...],
    boot_id: str | None = None,
) -> CgroupJobIdentityV1:
    """Bind an identity to an existing job beneath a held root.

    The directory must already exist.  This function never creates or mutates
    a cgroup and closes its temporary job descriptor before returning.  It is
    identity evidence only: it intentionally does not certify the filesystem
    as cgroup v2 or provide executable containment.
    """

    component = _validate_job_component(relative_component)
    job_id = component[len("job-") :]
    descriptor, metadata, _mount_id = _open_job_directory(root, component)
    try:
        return CgroupJobIdentityV1.issue(
            boot_id=read_linux_boot_id() if boot_id is None else boot_id,
            mount_id=root.mount_id,
            root_device=root.device,
            root_inode=root.inode,
            job_id=job_id,
            owner=owner,
            relative_component=component,
            job_device=metadata.st_dev,
            job_inode=metadata.st_ino,
        )
    finally:
        os.close(descriptor)


@dataclass(slots=True)
class ReopenedCgroupJob:
    """Held root/job descriptors after exact identity validation."""

    identity: CgroupJobIdentityV1
    root: HeldCgroupRoot
    _job_descriptor: int | None

    @property
    def job_descriptor(self) -> int:
        if self._job_descriptor is None:
            raise JobContainmentError("reopened cgroup job is closed")
        return self._job_descriptor

    def validate(self) -> None:
        self.root.validate()
        job = os.fstat(self.job_descriptor)
        named = os.stat(
            self.identity.relative_component,
            dir_fd=self.root.descriptor,
            follow_symlinks=False,
        )
        expected = (self.identity.job_device, self.identity.job_inode)
        if (
            not stat.S_ISDIR(job.st_mode)
            or job.st_uid != os.geteuid()
            or stat.S_IMODE(job.st_mode) & 0o022
            or (job.st_dev, job.st_ino) != expected
        ):
            raise CgroupJobIdentityMismatchError("held cgroup job identity changed")
        if (
            not stat.S_ISDIR(named.st_mode)
            or named.st_uid != os.geteuid()
            or stat.S_IMODE(named.st_mode) & 0o022
            or (named.st_dev, named.st_ino) != expected
        ):
            raise CgroupJobIdentityMismatchError("cgroup job name identity changed")
        if _mount_id_for_fd(self.job_descriptor) != self.identity.mount_id:
            raise CgroupJobIdentityMismatchError("cgroup job mount identity changed")

    def read_populated(self) -> bool:
        """Read strict emptiness evidence; this does not kill or mutate."""

        self.validate()
        descriptor = _open_control_file(
            self.job_descriptor,
            "cgroup.events",
            os.O_RDONLY,
            expected_device=self.identity.job_device,
            expected_mount_id=self.identity.mount_id,
        )
        try:
            value = os.read(descriptor, _MAX_EVENTS_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(value) > _MAX_EVENTS_BYTES:
            raise JobContainmentError("cgroup.events exceeds its evidence bound")
        try:
            text = value.decode("ascii", errors="strict")
        except UnicodeDecodeError as exc:
            raise JobContainmentError("cgroup.events is not ASCII") from exc
        return parse_cgroup_events(text)

    def close(self) -> None:
        descriptor = self._job_descriptor
        self._job_descriptor = None
        try:
            if descriptor is not None:
                os.close(descriptor)
        finally:
            self.root.close()

    def __enter__(self) -> ReopenedCgroupJob:
        try:
            self.validate()
            return self
        except BaseException:
            with contextlib.suppress(OSError):
                self.close()
            raise

    def __exit__(self, *_exc: object) -> None:
        self.close()


def reopen_cgroup_job(
    root_path: str | os.PathLike[str],
    identity: CgroupJobIdentityV1 | str,
    *,
    current_boot_id: str | None = None,
) -> ReopenedCgroupJob:
    """Reopen exact identity evidence, without certifying containment."""

    parsed = (
        CgroupJobIdentityV1.parse(identity)
        if type(identity) is str
        else identity
    )
    if not isinstance(parsed, CgroupJobIdentityV1):
        raise TypeError("identity must be CgroupJobIdentityV1 or canonical wire text")
    boot_id = read_linux_boot_id() if current_boot_id is None else current_boot_id
    if parsed.boot_id != boot_id:
        raise CgroupJobIdentityMismatchError(
            "cgroup job identity belongs to another Linux boot"
        )
    try:
        root = open_secure_cgroup_root(root_path)
    except CgroupPathSecurityError as exc:
        raise CgroupJobIdentityMismatchError(
            "cgroup root cannot be securely reopened"
        ) from exc
    descriptor: int | None = None
    try:
        if (root.mount_id, root.device, root.inode) != (
            parsed.mount_id,
            parsed.root_device,
            parsed.root_inode,
        ):
            raise CgroupJobIdentityMismatchError(
                "cgroup root mount or inode identity changed"
            )
        descriptor, metadata, mount_id = _open_job_directory(
            root,
            parsed.relative_component,
        )
        if mount_id != parsed.mount_id or (metadata.st_dev, metadata.st_ino) != (
            parsed.job_device,
            parsed.job_inode,
        ):
            raise CgroupJobIdentityMismatchError(
                "cgroup job mount or inode identity changed"
            )
        reopened = ReopenedCgroupJob(
            identity=parsed,
            root=root,
            _job_descriptor=descriptor,
        )
        reopened.validate()
        return reopened
    except BaseException:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        with contextlib.suppress(OSError):
            root.close()
        raise


def parse_cgroup_events(value: str) -> bool:
    """Return ``populated`` from strict, duplicate-free kernel event text."""

    if type(value) is not str:
        raise JobContainmentError("cgroup.events must be text")
    try:
        encoded = value.encode("ascii", errors="strict")
    except UnicodeEncodeError as exc:
        raise JobContainmentError("cgroup.events must be ASCII") from exc
    if not encoded or len(encoded) > _MAX_EVENTS_BYTES or "\r" in value:
        raise JobContainmentError("cgroup.events has invalid framing")
    lines = value.split("\n")
    if lines[-1] == "":
        lines.pop()
    if not lines or any(not line for line in lines):
        raise JobContainmentError("cgroup.events contains an empty record")
    fields: dict[str, str] = {}
    for line in lines:
        pieces = line.split(" ")
        if len(pieces) != 2:
            raise JobContainmentError("cgroup.events record is malformed")
        key, raw = pieces
        if _EVENT_KEY_PATTERN.fullmatch(key) is None or (
            _CANONICAL_UNSIGNED_PATTERN.fullmatch(raw) is None
        ):
            raise JobContainmentError("cgroup.events record is malformed")
        if key in fields:
            raise JobContainmentError(f"duplicate cgroup.events field: {key}")
        fields[key] = raw
    if "populated" not in fields or fields["populated"] not in {"0", "1"}:
        raise JobContainmentError(
            "cgroup.events must contain exactly one populated 0/1 field"
        )
    return fields["populated"] == "1"


def _open_control_file(
    directory_fd: int,
    name: str,
    flags: int,
    *,
    expected_device: int,
    expected_mount_id: int,
) -> int:
    if name not in {"cgroup.events", "cgroup.procs", "cgroup.kill"}:
        raise ValueError("unknown cgroup control file")
    open_flags = flags
    if hasattr(os, "O_CLOEXEC"):
        open_flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        open_flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        descriptor = os.open(name, open_flags, dir_fd=directory_fd)
        metadata = os.fstat(descriptor)
        named = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or metadata.st_dev != expected_device
            or _mount_id_for_fd(descriptor) != expected_mount_id
            or (metadata.st_dev, metadata.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise CgroupPathSecurityError(
                f"{name} ownership, type, or link metadata is unsafe"
            )
        os.set_inheritable(descriptor, False)
        return descriptor
    except JobContainmentError:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise CgroupPathSecurityError(f"cannot securely open {name}") from exc


def _validate_control_surface(root: HeldCgroupRoot) -> None:
    """Open, inspect, and close every required non-mutating interface."""

    root.validate()
    events_fd: int | None = None
    try:
        events_fd = _open_control_file(
            root.descriptor,
            "cgroup.events",
            os.O_RDONLY,
            expected_device=root.device,
            expected_mount_id=root.mount_id,
        )
        events_bytes = os.read(events_fd, _MAX_EVENTS_BYTES + 1)
        if len(events_bytes) > _MAX_EVENTS_BYTES:
            raise JobContainmentError("cgroup.events exceeds its evidence bound")
        parse_cgroup_events(events_bytes.decode("ascii", errors="strict"))
        for name, flags in (
            ("cgroup.procs", os.O_RDONLY),
            ("cgroup.procs", os.O_WRONLY),
            ("cgroup.kill", os.O_WRONLY),
        ):
            descriptor = _open_control_file(
                root.descriptor,
                name,
                flags,
                expected_device=root.device,
                expected_mount_id=root.mount_id,
            )
            os.close(descriptor)
    finally:
        if events_fd is not None:
            with contextlib.suppress(OSError):
                os.close(events_fd)


@dataclass(frozen=True, slots=True)
class CgroupContainmentRequirementsV1:
    """Auditable requirements; operational APIs remain disconnected."""

    backend: str = "cgroup-v2"
    identity_version: int = 1
    requires_cgroup2: bool = True
    requires_nsdelegate: bool = True
    required_control_files: tuple[str, ...] = (
        "cgroup.kill",
        "cgroup.events",
        "cgroup.procs",
    )
    requires_user_namespace: bool = True
    requires_cgroup_namespace: bool = True
    requires_pid_namespace: bool = True
    requires_ipc_namespace: bool = True
    requires_read_only_cgroup_view: bool = True
    requires_all_cgroup_views_sealed: bool = True
    requires_host_bus_isolation: bool = True
    network_namespace_policy: str = "preserve"
    network_access_required: bool = True
    launcher_proof_complete: bool = False
    allocation_api_enabled: bool = False
    kill_api_enabled: bool = False
    production_dispatch_enabled: bool = False
    foundation_state: str = "disconnected"


CGROUP_CONTAINMENT_REQUIREMENTS_V1 = CgroupContainmentRequirementsV1()


class CgroupContainmentUnavailableReason(str, Enum):
    UNSUPPORTED_PLATFORM = "unsupported_platform"
    INSECURE_ROOT = "insecure_root"
    ROOT_NOT_WRITABLE = "root_not_writable"
    MOUNTINFO_UNAVAILABLE = "mountinfo_unavailable"
    NOT_CGROUP2 = "not_cgroup2"
    NSDELEGATE_MISSING = "nsdelegate_missing"
    CONTROL_FILE_UNAVAILABLE = "control_file_unavailable"
    CONTROL_FILE_MALFORMED = "control_file_malformed"
    LAUNCHER_MISSING = "launcher_missing"
    LAUNCHER_TIMEOUT = "launcher_timeout"
    LAUNCHER_APPARMOR_DENIED = "launcher_apparmor_denied"
    LAUNCHER_USER_NAMESPACE_UNAVAILABLE = "launcher_user_namespace_unavailable"
    LAUNCHER_CGROUP_NAMESPACE_UNAVAILABLE = "launcher_cgroup_namespace_unavailable"
    LAUNCHER_NETWORK_NOT_PRESERVED = "launcher_network_not_preserved"
    LAUNCHER_CGROUP_VIEW_WRITABLE = "launcher_cgroup_view_writable"
    LAUNCHER_PROOF_INCOMPLETE = "launcher_proof_incomplete"
    LAUNCHER_PROBE_FAILED = "launcher_probe_failed"


@dataclass(frozen=True, slots=True)
class CgroupV2ContainmentUnavailable:
    reason: CgroupContainmentUnavailableReason
    detail: str
    requirements: CgroupContainmentRequirementsV1 = (
        CGROUP_CONTAINMENT_REQUIREMENTS_V1
    )
    available: bool = False


@dataclass(frozen=True, slots=True)
class CgroupV2ContainmentAvailable:
    """Reserved future result; the disconnected probe never constructs it."""

    root_path: str
    mount_id: int
    root_device: int
    root_inode: int
    launcher_path: str
    requirements: CgroupContainmentRequirementsV1 = (
        CGROUP_CONTAINMENT_REQUIREMENTS_V1
    )
    available: bool = True


CgroupV2ContainmentProbeResult = (
    CgroupV2ContainmentAvailable | CgroupV2ContainmentUnavailable
)


@dataclass(frozen=True, slots=True)
class _MountInfo:
    mount_id: int
    mount_point: str
    mount_options: frozenset[str]
    optional_fields: frozenset[str]
    filesystem_type: str
    super_options: frozenset[str]


def _decode_mountinfo_path(value: str) -> str:
    replacements = {r"\040": " ", r"\011": "\t", r"\012": "\n", r"\134": "\\"}
    index = 0
    pieces: list[str] = []
    while index < len(value):
        if value[index] != "\\":
            pieces.append(value[index])
            index += 1
            continue
        escape = value[index : index + 4]
        if escape not in replacements:
            raise JobContainmentError("mountinfo path escape is malformed")
        pieces.append(replacements[escape])
        index += 4
    return "".join(pieces)


def _mount_for_id(text: str, mount_id: int) -> _MountInfo:
    matches: list[_MountInfo] = []
    for line in text.splitlines():
        fields = line.split(" ")
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if separator < 6 or len(fields) < separator + 4:
            continue
        try:
            parsed_id = int(fields[0])
        except ValueError:
            continue
        if parsed_id != mount_id:
            continue
        matches.append(
            _MountInfo(
                mount_id=parsed_id,
                mount_point=_decode_mountinfo_path(fields[4]),
                mount_options=frozenset(fields[5].split(",")),
                optional_fields=frozenset(fields[6:separator]),
                filesystem_type=fields[separator + 1],
                super_options=frozenset(fields[separator + 3].split(",")),
            )
        )
    if len(matches) != 1:
        raise JobContainmentError("held root mount ID is absent or ambiguous")
    return matches[0]


@dataclass(frozen=True, slots=True)
class LauncherProbeExecution:
    returncode: int
    stdout: str = ""
    stderr: str = ""


LauncherProbeRunner = Callable[[Sequence[str]], LauncherProbeExecution | Any]


def _default_launcher_runner(command: Sequence[str]) -> LauncherProbeExecution:
    result = subprocess.run(
        tuple(command),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
        env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
    )
    return LauncherProbeExecution(
        returncode=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


def _launcher_probe_command(
    launcher_path: str,
    cgroup_root_path: str,
) -> tuple[str, ...]:
    parent_cgroup_namespace = os.readlink("/proc/self/ns/cgroup")
    parent_network_namespace = os.readlink("/proc/self/ns/net")
    script = (
        'test "$(readlink /proc/self/ns/cgroup)" != "$1" || exit 70; '
        'test "$(readlink /proc/self/ns/net)" = "$2" || exit 71; '
        'test "$(cat /proc/self/cgroup)" = "0::/" || exit 72; '
        'test ! -w "$3/cgroup.procs" || exit 73'
    )
    # Deliberately no --unshare-net: all Modes require network access.
    return (
        launcher_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-cgroup",
        "--ro-bind",
        "/",
        "/",
        "/bin/sh",
        "-ceu",
        script,
        "cgroup-containment-probe",
        parent_cgroup_namespace,
        parent_network_namespace,
        cgroup_root_path,
    )


def _unavailable(
    reason: CgroupContainmentUnavailableReason,
    detail: str,
) -> CgroupV2ContainmentUnavailable:
    return CgroupV2ContainmentUnavailable(reason=reason, detail=detail)


def _coerce_probe_execution(value: Any) -> LauncherProbeExecution:
    returncode = getattr(value, "returncode", None)
    stdout = getattr(value, "stdout", "")
    stderr = getattr(value, "stderr", "")
    if type(returncode) is not int or type(stdout) is not str or type(stderr) is not str:
        raise TypeError("launcher probe runner returned an invalid result")
    return LauncherProbeExecution(returncode, stdout, stderr)


def _trusted_launcher_metadata(path: str) -> os.stat_result:
    try:
        metadata = os.stat(path, follow_symlinks=True)
    except OSError as exc:
        raise CgroupPathSecurityError(
            "namespace launcher cannot be inspected"
        ) from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or not os.access(path, os.X_OK)
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise CgroupPathSecurityError(
            "namespace launcher must be a root-owned, singly linked, "
            "non-writable executable"
        )
    return metadata


def _launcher_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def probe_cgroup_v2_containment(
    root_path: str | os.PathLike[str],
    *,
    launcher: str = "bwrap",
    launcher_runner: LauncherProbeRunner | None = None,
    mountinfo_text: str | None = None,
) -> CgroupV2ContainmentProbeResult:
    """Non-mutating probe for the disconnected prerequisite set.

    Every outcome is a typed unavailable value.  Even a successful small
    launcher check leaves PID/IPC/bus/alternate-cgroup escape proof incomplete,
    and therefore cannot enable allocation, kill, child READY, or dispatch.
    """

    if not sys.platform.startswith("linux"):
        return _unavailable(
            CgroupContainmentUnavailableReason.UNSUPPORTED_PLATFORM,
            "cgroup-v2 containment requires Linux",
        )
    root: HeldCgroupRoot | None = None
    try:
        try:
            root = open_secure_cgroup_root(root_path)
        except (CgroupPathSecurityError, JobContainmentError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.INSECURE_ROOT,
                str(exc),
            )
        mode = stat.S_IMODE(root.validate().st_mode)
        if not mode & stat.S_IWUSR or not mode & stat.S_IXUSR:
            return _unavailable(
                CgroupContainmentUnavailableReason.ROOT_NOT_WRITABLE,
                "delegated cgroup root is not owner-writable/searchable",
            )
        if mountinfo_text is None:
            try:
                mountinfo_text = Path("/proc/self/mountinfo").read_text(
                    encoding="ascii"
                )
            except OSError as exc:
                return _unavailable(
                    CgroupContainmentUnavailableReason.MOUNTINFO_UNAVAILABLE,
                    f"Linux mountinfo is unavailable: {exc}",
                )
        try:
            mount = _mount_for_id(mountinfo_text, root.mount_id)
        except JobContainmentError as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.MOUNTINFO_UNAVAILABLE,
                str(exc),
            )
        if mount.filesystem_type != "cgroup2":
            return _unavailable(
                CgroupContainmentUnavailableReason.NOT_CGROUP2,
                "delegated root is not on a cgroup-v2 mount",
            )
        if "nsdelegate" not in mount.super_options:
            return _unavailable(
                CgroupContainmentUnavailableReason.NSDELEGATE_MISSING,
                "cgroup-v2 mount does not advertise nsdelegate",
            )

        try:
            _validate_control_surface(root)
        except (CgroupPathSecurityError, OSError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.CONTROL_FILE_UNAVAILABLE,
                str(exc),
            )
        except (JobContainmentError, UnicodeDecodeError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.CONTROL_FILE_MALFORMED,
                str(exc),
            )
        resolved_launcher = (
            os.path.realpath(launcher)
            if os.path.isabs(launcher)
            else shutil.which(launcher)
        )
        if not resolved_launcher:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_MISSING,
                "non-escapable namespace launcher is not installed",
            )
        try:
            launcher_stat = _trusted_launcher_metadata(resolved_launcher)
        except CgroupPathSecurityError as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_MISSING,
                str(exc),
            )
        expected_launcher_identity = _launcher_identity(launcher_stat)
        try:
            command = _launcher_probe_command(resolved_launcher, root.path)
            execution = _coerce_probe_execution(
                (launcher_runner or _default_launcher_runner)(command)
            )
        except subprocess.TimeoutExpired:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_TIMEOUT,
                "namespace launcher probe timed out",
            )
        except FileNotFoundError:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_MISSING,
                "namespace launcher disappeared before its probe",
            )
        except (OSError, TypeError, JobContainmentError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_PROBE_FAILED,
                f"namespace launcher probe could not run: {exc}",
            )
        try:
            root.validate()
            _validate_control_surface(root)
            final_launcher = _trusted_launcher_metadata(resolved_launcher)
            if _launcher_identity(final_launcher) != expected_launcher_identity:
                raise CgroupPathSecurityError(
                    "namespace launcher identity changed during its probe"
                )
        except (CgroupPathSecurityError, CgroupJobIdentityMismatchError, OSError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_PROBE_FAILED,
                f"containment prerequisite identity drifted: {exc}",
            )
        except (JobContainmentError, UnicodeDecodeError) as exc:
            return _unavailable(
                CgroupContainmentUnavailableReason.CONTROL_FILE_MALFORMED,
                f"containment control evidence drifted: {exc}",
            )
        if execution.returncode == 0:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_PROOF_INCOMPLETE,
                "namespace prerequisites ran, but PID/IPC/session-bus, local-socket, "
                "alternate-cgroup-view, and cooperative same-user escape proof is "
                "not implemented",
            )
        if execution.returncode in {70, 72}:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_CGROUP_NAMESPACE_UNAVAILABLE,
                "launcher did not establish a rooted cgroup namespace",
            )
        if execution.returncode == 71:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_NETWORK_NOT_PRESERVED,
                "launcher changed the required network namespace",
            )
        if execution.returncode == 73:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_CGROUP_VIEW_WRITABLE,
                "launcher left cgroup migration controls writable",
            )
        error_text = f"{execution.stderr}\n{execution.stdout}".lower()
        if "apparmor" in error_text:
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_APPARMOR_DENIED,
                "AppArmor denied the namespace launcher probe",
            )
        user_namespace_markers = (
            "user namespace",
            "userns",
            "uid map",
            "setting up uid map",
            "operation not permitted",
            "permission denied",
        )
        if any(marker in error_text for marker in user_namespace_markers):
            return _unavailable(
                CgroupContainmentUnavailableReason.LAUNCHER_USER_NAMESPACE_UNAVAILABLE,
                "user-namespace launcher probe was denied",
            )
        return _unavailable(
            CgroupContainmentUnavailableReason.LAUNCHER_PROBE_FAILED,
            f"namespace launcher probe exited {execution.returncode}",
        )
    finally:
        if root is not None:
            root.close()


__all__ = [
    "CGROUP_CONTAINMENT_REQUIREMENTS_V1",
    "CgroupContainmentRequirementsV1",
    "CgroupContainmentUnavailableReason",
    "CgroupJobIdentityError",
    "CgroupJobIdentityMismatchError",
    "CgroupJobIdentityV1",
    "CgroupPathSecurityError",
    "CgroupV2ContainmentAvailable",
    "CgroupV2ContainmentProbeResult",
    "CgroupV2ContainmentUnavailable",
    "HeldCgroupRoot",
    "JobContainmentError",
    "LauncherProbeExecution",
    "ReopenedCgroupJob",
    "bind_existing_cgroup_job",
    "new_cgroup_job_name",
    "open_secure_cgroup_root",
    "parse_cgroup_events",
    "probe_cgroup_v2_containment",
    "read_linux_boot_id",
    "reopen_cgroup_job",
]
