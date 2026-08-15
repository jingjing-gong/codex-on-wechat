"""Fresh-process Agent bootstrap and control-stream liveness.

This module is intentionally a narrow process foundation.  It authenticates
one exact Agent generation, establishes a connection-bound IPC session, and
implements only a non-schedulable bootstrap liveness proof,
``PING``/``PONG``, ``QUIESCE``, and ``SHUTDOWN``.  It deliberately never emits
``READY``: the target protocol reserves that signal until startup capabilities
have been durably committed.  This module does not construct an Agent runtime,
open SQLite, accept assignments, or perform channel/SDK work.

The child is executed by file path, rather than with ``python -m``.  Importing
the surrounding :mod:`src.runtime` package would run its application-facing
initializer and defeat the child's deliberately small import boundary.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import math
import os
import re
import secrets
import signal
import socket
import struct
import subprocess
import sys
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

if __package__:
    from .ipc import (
        DEFAULT_MAX_FRAME_BYTES,
        HARD_MAX_FRAME_BYTES,
        PROTOCOL_VERSION,
        BootstrapFrameCodec,
        FrameCodec,
        FrameDirection,
        FrameKind,
        FrameStream,
        IpcFrame,
        IpcProtocolError,
        IpcSequenceError,
        IpcSessionIdentity,
        IpcStreamClosedError,
        IpcTruncatedFrameError,
        ReplayDisposition,
        SequenceReplayGuard,
        canonical_json_bytes,
        read_frame,
        write_frame,
    )
    from .process_lifetime import (
        DEFAULT_AGENT_LIFETIME_LOCK_ROOT,
        ExactProcessPidfd,
        InheritedLifetimeLock,
        ProcessLifetimeError,
        VerifiedProcessGroup,
        capture_linux_process_birth,
        require_linux_pidfd_support,
        terminate_exact_prebootstrap_process,
        terminate_verified_process_group,
        wait_for_verified_exit,
    )
else:  # pragma: no cover - covered through the spawned child process
    # Direct script execution avoids importing the heavyweight ``src`` and
    # ``src.runtime`` packages.  Isolated safe-path mode deliberately omits the
    # script directory, so add this one audited absolute dependency root after
    # Python's no-site startup has completed.
    _child_module_root = os.path.realpath(os.path.dirname(__file__))
    if _child_module_root not in sys.path:
        sys.path.insert(0, _child_module_root)
    import ipc as _child_ipc_module

    if os.path.realpath(_child_ipc_module.__file__) != os.path.realpath(
        os.path.join(os.path.dirname(__file__), "ipc.py")
    ):
        raise ImportError("Agent child resolved an unexpected IPC module")
    from ipc import (  # type: ignore[no-redef]
        DEFAULT_MAX_FRAME_BYTES,
        HARD_MAX_FRAME_BYTES,
        PROTOCOL_VERSION,
        BootstrapFrameCodec,
        FrameCodec,
        FrameDirection,
        FrameKind,
        FrameStream,
        IpcFrame,
        IpcProtocolError,
        IpcSequenceError,
        IpcSessionIdentity,
        IpcStreamClosedError,
        IpcTruncatedFrameError,
        ReplayDisposition,
        SequenceReplayGuard,
        canonical_json_bytes,
        read_frame,
        write_frame,
    )
    from process_lifetime import (  # type: ignore[no-redef]
        DEFAULT_AGENT_LIFETIME_LOCK_ROOT,
        ExactProcessPidfd,
        InheritedLifetimeLock,
        ProcessLifetimeError,
        VerifiedProcessGroup,
        capture_linux_process_birth,
        require_linux_pidfd_support,
        terminate_exact_prebootstrap_process,
        terminate_verified_process_group,
        wait_for_verified_exit,
    )


AGENT_PROCESS_FEATURE = "bootstrap-liveness-v2"
GENERATION_CAPABILITY_BYTES = 32
HANDSHAKE_NONCE_BYTES = 32
DEFAULT_STARTUP_TIMEOUT = 5.0
DEFAULT_CONTROL_TIMEOUT = 5.0
DEFAULT_EXIT_TIMEOUT = 5.0
DEFAULT_REPLAY_WINDOW = 256
MAX_REPLAY_WINDOW = 4_096
MAX_PING_NONCE_BYTES = 256

_AGENT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_NONCE_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_BOOTSTRAP_MAGIC = b"COWABP01"
_BOOTSTRAP_PACKET = struct.Struct(">8s32s32s")
_ROOT_DERIVATION_DOMAIN = b"codex-wechat-agent-bootstrap-root-v1\x00"
_PROOF_KEY_DERIVATION_DOMAIN = b"codex-wechat-agent-bootstrap-proof-key-v1\x00"
_SESSION_KEY_DERIVATION_DOMAIN = b"codex-wechat-agent-session-key-v1\x00"
_CHILD_PROTOCOL_EXIT = 70
_FORBIDDEN_CHILD_MODULES = (
    "_sqlite3",
    "sqlite3",
    "src",
    "wechat_ilink",
    "openai_codex",
)


class AgentProcessError(RuntimeError):
    """Base error for the bootstrap/liveness process host."""


class AgentProcessHandshakeError(AgentProcessError):
    """A child did not complete the exact authenticated handshake."""


class AgentProcessExitedError(AgentProcessError):
    """A child exited unexpectedly during a control operation."""


class AgentProcessStateError(AgentProcessError):
    """A control operation is invalid in the handle's current state."""


class AgentProcessCleanupOwner:
    """Explicit retryable ownership of a child whose spawn cleanup failed.

    The owner deliberately retains the ``Popen`` object, pidfd/group evidence,
    and lifetime-lock probe.  Evidence may be closed only after a retry proves
    the child/tree reaped and the inherited lock released.
    """

    def __init__(
        self,
        *,
        process: subprocess.Popen[Any],
        lifetime_lock: InheritedLifetimeLock,
        default_timeout: float,
        process_group: VerifiedProcessGroup | None = None,
        exact_process: ExactProcessPidfd | None = None,
        prebootstrap_eof_only: bool = False,
    ) -> None:
        identity_count = sum(
            (
                process_group is not None,
                exact_process is not None,
                prebootstrap_eof_only,
            )
        )
        if identity_count != 1:
            raise ValueError(
                "cleanup owner requires exactly one kernel process identity"
            )
        self.process = process
        self.lifetime_lock = lifetime_lock
        self.process_group = process_group
        self.exact_process = exact_process
        self.prebootstrap_eof_only = prebootstrap_eof_only
        self.default_timeout = default_timeout
        self._closed = False
        self._retrying = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def retry(self, *, timeout: float | None = None) -> int:
        """Retry mandatory cleanup and close evidence only after proof."""

        if self._closed:
            return self.process.returncode or 0
        if self._retrying:
            raise AgentProcessStateError("Agent cleanup is already in progress")
        self._retrying = True
        try:
            bound = self.default_timeout if timeout is None else float(timeout)
            if self.process_group is not None:
                await terminate_verified_process_group(
                    self.process,
                    self.process_group,
                    self.lifetime_lock,
                    bound,
                )
            elif self.exact_process is not None:
                await terminate_exact_prebootstrap_process(
                    self.process,
                    self.exact_process,
                    self.lifetime_lock,
                    bound,
                )
            else:
                await _wait_prebootstrap_eof_cleanup(
                    self.process,
                    self.lifetime_lock,
                    bound,
                )
            returncode = self.process.poll()
            if returncode is None or not self.lifetime_lock.released:
                raise AgentProcessExitedError(
                    "Agent cleanup returned without complete release proof"
                )
            self._closed = True
            return returncode
        finally:
            self._retrying = False

    def close_after_proof(self) -> None:
        """Idempotently close evidence that is already conclusively released."""

        if self._closed:
            return
        if self.process.poll() is None or not self.lifetime_lock.prove_released():
            raise AgentProcessExitedError(
                "cannot close live Agent cleanup ownership evidence"
            )
        if self.process_group is not None:
            if not self.process_group.is_empty():
                raise AgentProcessExitedError(
                    "cannot close nonempty Agent process-group evidence"
                )
            self.process_group.close()
        if self.exact_process is not None:
            self.exact_process.close()
        self._closed = True


class AgentProcessCleanupError(AgentProcessError):
    """Spawn failed and mandatory cleanup still has an explicit owner."""

    def __init__(
        self,
        *,
        startup_error: BaseException,
        cleanup_error: BaseException,
        cleanup_owner: AgentProcessCleanupOwner,
    ) -> None:
        super().__init__(
            "Agent child startup failed and cleanup remains incomplete; "
            "retry through cleanup_owner"
        )
        self.startup_error = startup_error
        self.cleanup_error = cleanup_error
        self.cleanup_owner = cleanup_owner


class AgentProcessControlState(str, Enum):
    """The small, non-durable state vocabulary of this liveness handle."""

    BOOTSTRAPPED = "bootstrapped"
    QUIESCED = "quiesced"
    STOPPING = "stopping"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class AgentProcessIdentity:
    """Exact process-generation identity authenticated by every frame."""

    supervisor_epoch: int
    agent_id: str
    agent_incarnation: int
    worker_generation: int
    protocol_version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.protocol_version) is not int
            or self.protocol_version != PROTOCOL_VERSION
        ):
            raise ValueError("unsupported Agent process protocol version")
        if type(self.agent_id) is not str or not _AGENT_ID_PATTERN.fullmatch(
            self.agent_id
        ):
            raise ValueError("agent_id must be a canonical lowercase Agent ID")
        # Reuse the IPC contract's exact positive-integer validation.
        IpcSessionIdentity(
            supervisor_epoch=self.supervisor_epoch,
            agent_id=self.agent_id,
            agent_incarnation=self.agent_incarnation,
            worker_generation=self.worker_generation,
        )

    @property
    def ipc_identity(self) -> IpcSessionIdentity:
        return IpcSessionIdentity(
            supervisor_epoch=self.supervisor_epoch,
            agent_id=self.agent_id,
            agent_incarnation=self.agent_incarnation,
            worker_generation=self.worker_generation,
        )

    def derivation_context(self) -> bytes:
        """Return framed public identity material used in every key phase."""

        return canonical_json_bytes(
            {
                "agent_id": self.agent_id,
                "agent_incarnation": self.agent_incarnation,
                "protocol_version": self.protocol_version,
                "supervisor_epoch": self.supervisor_epoch,
                "worker_generation": self.worker_generation,
            }
        )


def _secret_bytes(
    value: bytes | bytearray | memoryview,
    *,
    field: str,
    exact_length: int,
) -> bytes | bytearray | memoryview:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(f"{field} must be bytes")
    if len(value) != exact_length:
        raise ValueError(f"{field} has an invalid length")
    return value


def _derive_bootstrap_root(
    generation_capability: bytes | bytearray | memoryview,
    identity: AgentProcessIdentity,
    launch_nonce: bytes | bytearray | memoryview,
) -> bytes:
    """Derive the per-launch root without retaining the raw capability."""

    if not isinstance(identity, AgentProcessIdentity):
        raise TypeError("identity must be an AgentProcessIdentity")
    capability = _secret_bytes(
        generation_capability,
        field="generation capability",
        exact_length=GENERATION_CAPABILITY_BYTES,
    )
    nonce = _secret_bytes(
        launch_nonce,
        field="launch nonce",
        exact_length=HANDSHAKE_NONCE_BYTES,
    )
    context = canonical_json_bytes(
        {
            "identity": {
                "agent_id": identity.agent_id,
                "agent_incarnation": identity.agent_incarnation,
                "protocol_version": identity.protocol_version,
                "supervisor_epoch": identity.supervisor_epoch,
                "worker_generation": identity.worker_generation,
            },
            "launch_nonce": bytes(nonce).hex(),
        }
    )
    copied_capability = (
        bytearray(capability) if isinstance(capability, memoryview) else None
    )
    try:
        return hmac.new(
            copied_capability if copied_capability is not None else capability,
            _ROOT_DERIVATION_DOMAIN + context,
            hashlib.sha256,
        ).digest()
    finally:
        _zero_mutable(copied_capability)


def _derive_bootstrap_proof_key(
    bootstrap_root: bytes | bytearray | memoryview,
) -> bytes:
    root = _secret_bytes(
        bootstrap_root,
        field="bootstrap root",
        exact_length=hashlib.sha256().digest_size,
    )
    copied_root = bytearray(root) if isinstance(root, memoryview) else None
    try:
        return hmac.new(
            copied_root if copied_root is not None else root,
            _PROOF_KEY_DERIVATION_DOMAIN,
            hashlib.sha256,
        ).digest()
    finally:
        _zero_mutable(copied_root)


def _derive_session_key(
    bootstrap_root: bytes | bytearray | memoryview,
    identity: AgentProcessIdentity,
    launch_nonce: bytes | bytearray | memoryview,
    child_nonce: bytes | bytearray | memoryview,
    supervisor_nonce: bytes | bytearray | memoryview,
) -> bytes:
    """Derive a session key bound to launch, connection, and exact identity."""

    root = _secret_bytes(
        bootstrap_root,
        field="bootstrap root",
        exact_length=hashlib.sha256().digest_size,
    )
    if not isinstance(identity, AgentProcessIdentity):
        raise TypeError("identity must be an AgentProcessIdentity")
    nonces = {
        name: _secret_bytes(
            value,
            field=name.replace("_", " "),
            exact_length=HANDSHAKE_NONCE_BYTES,
        )
        for name, value in (
            ("launch_nonce", launch_nonce),
            ("child_nonce", child_nonce),
            ("supervisor_nonce", supervisor_nonce),
        )
    }
    context = canonical_json_bytes(
        {
            "child_nonce": bytes(nonces["child_nonce"]).hex(),
            "identity": {
                "agent_id": identity.agent_id,
                "agent_incarnation": identity.agent_incarnation,
                "protocol_version": identity.protocol_version,
                "supervisor_epoch": identity.supervisor_epoch,
                "worker_generation": identity.worker_generation,
            },
            "launch_nonce": bytes(nonces["launch_nonce"]).hex(),
            "supervisor_nonce": bytes(nonces["supervisor_nonce"]).hex(),
        }
    )
    copied_root = bytearray(root) if isinstance(root, memoryview) else None
    try:
        return hmac.new(
            copied_root if copied_root is not None else root,
            _SESSION_KEY_DERIVATION_DOMAIN + context,
            hashlib.sha256,
        ).digest()
    finally:
        _zero_mutable(copied_root)


def _zero_mutable(value: bytearray | None) -> None:
    if value is not None:
        value[:] = b"\x00" * len(value)


def _deadline_remaining(deadline: float) -> float:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise asyncio.TimeoutError("Agent process startup deadline expired")
    return remaining


async def _write_before_deadline(
    writer: asyncio.StreamWriter,
    codec: FrameCodec,
    frame: IpcFrame,
    deadline: float,
) -> None:
    timeout = _deadline_remaining(deadline)
    await asyncio.wait_for(
        write_frame(writer, codec, frame),
        timeout=timeout,
    )


async def _write_with_timeout(
    writer: asyncio.StreamWriter,
    codec: FrameCodec,
    frame: IpcFrame,
    timeout: float,
) -> None:
    await asyncio.wait_for(write_frame(writer, codec, frame), timeout=timeout)


async def _close_stream_writer(
    writer: asyncio.StreamWriter,
    timeout: float,
) -> None:
    writer.close()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(writer.wait_closed(), timeout=timeout)


def _close_fd(fd: int | None) -> None:
    if fd is None:
        return
    with contextlib.suppress(OSError):
        os.close(fd)


def _set_cloexec(fd: int, field: str) -> None:
    if type(fd) is not int or fd < 0:
        raise AgentProcessError(f"{field} is not a valid descriptor")
    try:
        os.set_inheritable(fd, False)
    except OSError as exc:
        raise AgentProcessError(f"cannot seal {field}") from exc
    if os.get_inheritable(fd):
        raise AgentProcessError(f"cannot seal {field}")


def _new_cloexec_pipe() -> tuple[int, int]:
    if hasattr(os, "pipe2"):
        return os.pipe2(os.O_CLOEXEC)
    read_fd, write_fd = os.pipe()  # pragma: no cover - POSIX compatibility
    os.set_inheritable(read_fd, False)
    os.set_inheritable(write_fd, False)
    return read_fd, write_fd


def _write_bootstrap_material(
    fd: int,
    generation_capability: bytearray,
    launch_nonce: bytes,
) -> None:
    packet = bytearray(_BOOTSTRAP_PACKET.size)
    try:
        _BOOTSTRAP_PACKET.pack_into(
            packet,
            0,
            _BOOTSTRAP_MAGIC,
            generation_capability,
            launch_nonce,
        )
        view = memoryview(packet)
        offset = 0
        while offset < len(packet):
            written = os.write(fd, view[offset:])
            if written <= 0:  # pragma: no cover - os.write contract
                raise AgentProcessHandshakeError("bootstrap channel closed")
            offset += written
    finally:
        _zero_mutable(packet)


def _consume_bootstrap_material(
    fd: int,
    identity: AgentProcessIdentity,
) -> tuple[bytearray, bytes]:
    """Read, derive, and promptly erase a one-shot sealed bootstrap packet."""

    packet = bytearray(_BOOTSTRAP_PACKET.size)
    try:
        view = memoryview(packet)
        offset = 0
        while offset < len(packet):
            read = os.readv(fd, [view[offset:]])
            if read == 0:
                raise AgentProcessHandshakeError("bootstrap channel closed")
            offset += read
        # The parent closes its writer immediately.  Any extra byte indicates
        # a malformed bootstrap channel rather than a framed secret packet.
        if os.read(fd, 1):
            raise AgentProcessHandshakeError("invalid bootstrap material")
        magic, capability_view, launch_view = (
            packet[:8],
            memoryview(packet)[8:40],
            memoryview(packet)[40:72],
        )
        if not hmac.compare_digest(magic, _BOOTSTRAP_MAGIC):
            raise AgentProcessHandshakeError("invalid bootstrap material")
        launch_nonce = bytes(launch_view)
        root = bytearray(
            _derive_bootstrap_root(capability_view, identity, launch_nonce)
        )
        return root, launch_nonce
    finally:
        _zero_mutable(packet)
        _close_fd(fd)


def _wire_nonce(value: Any, field: str) -> bytes:
    if type(value) is not str or not _NONCE_PATTERN.fullmatch(value):
        raise IpcProtocolError(f"{field} must be a canonical handshake nonce")
    return bytes.fromhex(value)


def _ping_nonce(value: Any) -> str:
    if type(value) is not str or not value:
        raise IpcProtocolError("PING nonce must be a nonempty string")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise IpcProtocolError("PING nonce must be valid Unicode") from exc
    if len(encoded) > MAX_PING_NONCE_BYTES:
        raise IpcProtocolError("PING nonce exceeds the protocol limit")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise IpcProtocolError("PING nonce contains a control character")
    return value


def _require_payload(frame: IpcFrame, keys: set[str]) -> Mapping[str, Any]:
    if set(frame.payload) != keys:
        raise IpcProtocolError("control payload fields do not match the protocol")
    return frame.payload


def _require_exact_identity(
    frame: IpcFrame,
    identity: AgentProcessIdentity,
) -> None:
    if IpcSessionIdentity.from_frame(frame) != identity.ipc_identity:
        raise IpcSequenceError("frame identity does not match Agent generation")
    if frame.protocol_version != identity.protocol_version:
        raise IpcProtocolError("frame protocol does not match Agent generation")


def _require_frame_shape(
    frame: IpcFrame,
    identity: AgentProcessIdentity,
    *,
    kind: FrameKind,
    sequence: int | None = None,
    reply_to_frame_id: str | None | object = ...,
) -> None:
    _require_exact_identity(frame, identity)
    if frame.kind is not kind:
        raise IpcProtocolError("unexpected control frame kind")
    expected_direction = (
        FrameDirection.CHILD_TO_SUPERVISOR
        if kind in {FrameKind.HELLO, FrameKind.READY, FrameKind.PONG}
        else FrameDirection.SUPERVISOR_TO_CHILD
    )
    if frame.direction is not expected_direction:  # pragma: no cover
        raise IpcProtocolError("unexpected control frame direction")
    if frame.stream is not FrameStream.CONTROL:
        raise IpcProtocolError("unexpected control frame stream")
    if sequence is not None and frame.stream_sequence != sequence:
        raise IpcSequenceError("unexpected control frame sequence")
    if reply_to_frame_id is not ... and frame.reply_to_frame_id != reply_to_frame_id:
        raise IpcProtocolError("control response correlation does not match")


def _build_frame(
    codec: FrameCodec,
    identity: AgentProcessIdentity,
    *,
    kind: FrameKind,
    direction: FrameDirection,
    sequence: int,
    payload: Mapping[str, Any],
    reply_to_frame_id: str | None = None,
) -> IpcFrame:
    return codec.build_frame(
        kind=kind,
        direction=direction,
        stream=FrameStream.CONTROL,
        supervisor_epoch=identity.supervisor_epoch,
        agent_id=identity.agent_id,
        agent_incarnation=identity.agent_incarnation,
        worker_generation=identity.worker_generation,
        protocol_version=identity.protocol_version,
        ipc_frame_id=secrets.token_hex(16),
        reply_to_frame_id=reply_to_frame_id,
        stream_sequence=sequence,
        payload=payload,
    )


def _session_guard(
    identity: AgentProcessIdentity,
    *,
    replay_window: int,
) -> SequenceReplayGuard:
    return SequenceReplayGuard(
        identity.ipc_identity,
        initial_sequence=0,
        initial_sequences={
            (FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): 1,
            (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL): 1,
        },
        replay_window=replay_window,
    )


def _assert_child_import_boundary() -> None:
    """Fail before HELLO if application/store modules entered this process."""

    loaded = tuple(sys.modules)
    if any(
        name == prefix or name.startswith(prefix + ".")
        for name in loaded
        for prefix in _FORBIDDEN_CHILD_MODULES
    ):
        raise AgentProcessError("Agent child import boundary was violated")


class AgentChildHost:
    """Child-side authenticated bootstrap and liveness loop.

    The data descriptor is reserved and kept private for the later assignment
    protocol.  This foundation deliberately never reads or writes it.
    """

    def __init__(
        self,
        identity: AgentProcessIdentity,
        *,
        control_fd: int,
        data_fd: int,
        bootstrap_fd: int,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
        control_timeout: float = DEFAULT_CONTROL_TIMEOUT,
        replay_window: int = DEFAULT_REPLAY_WINDOW,
    ) -> None:
        if not isinstance(identity, AgentProcessIdentity):
            raise TypeError("identity must be an AgentProcessIdentity")
        if len({control_fd, data_fd, bootstrap_fd}) != 3:
            raise ValueError("child descriptors must be distinct")
        if (
            type(max_frame_bytes) is not int
            or max_frame_bytes <= 0
            or max_frame_bytes > HARD_MAX_FRAME_BYTES
        ):
            raise ValueError("max_frame_bytes exceeds the hard protocol bound")
        for value, field in (
            (startup_timeout, "startup_timeout"),
            (control_timeout, "control_timeout"),
        ):
            if (
                type(value) not in {int, float}
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(f"{field} must be positive")
        if (
            type(replay_window) is not int
            or replay_window <= 0
            or replay_window > MAX_REPLAY_WINDOW
        ):
            raise ValueError("replay_window is outside the supported bound")
        self.identity = identity
        self.control_fd = control_fd
        self.data_fd = data_fd
        self.bootstrap_fd = bootstrap_fd
        self.max_frame_bytes = max_frame_bytes
        self.startup_timeout = float(startup_timeout)
        self.control_timeout = float(control_timeout)
        self.replay_window = replay_window

    async def run(self) -> int:
        _assert_child_import_boundary()
        startup_deadline = (
            asyncio.get_running_loop().time() + self.startup_timeout
        )
        for fd, field in (
            (self.control_fd, "control descriptor"),
            (self.data_fd, "data descriptor"),
            (self.bootstrap_fd, "bootstrap descriptor"),
        ):
            _set_cloexec(fd, field)

        bootstrap_root: bytearray | None = None
        proof_key: bytearray | None = None
        session_key: bytearray | None = None
        bootstrap_codec: BootstrapFrameCodec | None = None
        session_codec: FrameCodec | None = None
        writer: asyncio.StreamWriter | None = None
        data_socket: socket.socket | None = None
        try:
            bootstrap_root, launch_nonce = _consume_bootstrap_material(
                self.bootstrap_fd,
                self.identity,
            )
            self.bootstrap_fd = -1

            control_socket = socket.socket(fileno=self.control_fd)
            self.control_fd = -1
            control_socket.setblocking(False)
            data_socket = socket.socket(fileno=self.data_fd)
            self.data_fd = -1
            data_socket.setblocking(False)
            connection_timeout = _deadline_remaining(startup_deadline)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(sock=control_socket),
                timeout=connection_timeout,
            )

            proof_key = bytearray(_derive_bootstrap_proof_key(bootstrap_root))
            bootstrap_codec = BootstrapFrameCodec(
                proof_key,
                max_frame_bytes=self.max_frame_bytes,
            )
            handshake_guard = SequenceReplayGuard(
                self.identity.ipc_identity,
                replay_window=self.replay_window,
            )
            child_nonce = os.urandom(HANDSHAKE_NONCE_BYTES)
            hello = _build_frame(
                bootstrap_codec,
                self.identity,
                kind=FrameKind.HELLO,
                direction=FrameDirection.CHILD_TO_SUPERVISOR,
                sequence=0,
                payload={
                    "child_nonce": child_nonce.hex(),
                    "child_pid": os.getpid(),
                    "feature": AGENT_PROCESS_FEATURE,
                    "launch_nonce": launch_nonce.hex(),
                    "parent_pid": os.getppid(),
                },
            )
            handshake_guard.observe(hello)
            await _write_before_deadline(
                writer,
                bootstrap_codec,
                hello,
                startup_deadline,
            )

            config_timeout = _deadline_remaining(startup_deadline)
            config = await asyncio.wait_for(
                read_frame(
                    reader,
                    bootstrap_codec,
                    expected_direction=FrameDirection.SUPERVISOR_TO_CHILD,
                    expected_stream=FrameStream.CONTROL,
                ),
                timeout=config_timeout,
            )
            handshake_guard.observe(config)
            _require_frame_shape(
                config,
                self.identity,
                kind=FrameKind.CONFIG,
                sequence=0,
                reply_to_frame_id=hello.ipc_frame_id,
            )
            payload = _require_payload(
                config,
                {
                    "child_nonce",
                    "feature",
                    "launch_nonce",
                    "max_frame_bytes",
                    "supervisor_nonce",
                    "supervisor_pid",
                },
            )
            if payload["feature"] != AGENT_PROCESS_FEATURE:
                raise IpcProtocolError("CONFIG feature negotiation failed")
            if payload["child_nonce"] != child_nonce.hex():
                raise IpcProtocolError("CONFIG child nonce does not match")
            if payload["launch_nonce"] != launch_nonce.hex():
                raise IpcProtocolError("CONFIG launch nonce does not match")
            if payload["supervisor_pid"] != os.getppid():
                raise IpcProtocolError("CONFIG parent process does not match")
            if payload["max_frame_bytes"] != self.max_frame_bytes:
                raise IpcProtocolError("CONFIG frame limit does not match")
            supervisor_nonce = _wire_nonce(
                payload["supervisor_nonce"],
                "supervisor_nonce",
            )

            session_key = bytearray(
                _derive_session_key(
                    bootstrap_root,
                    self.identity,
                    launch_nonce,
                    child_nonce,
                    supervisor_nonce,
                )
            )
            session_codec = FrameCodec(
                session_key,
                max_frame_bytes=self.max_frame_bytes,
            )
            session_guard = _session_guard(
                self.identity,
                replay_window=self.replay_window,
            )
            startup_ping_timeout = _deadline_remaining(startup_deadline)
            startup_ping = await asyncio.wait_for(
                read_frame(
                    reader,
                    session_codec,
                    expected_direction=FrameDirection.SUPERVISOR_TO_CHILD,
                    expected_stream=FrameStream.CONTROL,
                ),
                timeout=startup_ping_timeout,
            )
            session_guard.observe(startup_ping)
            _require_frame_shape(
                startup_ping,
                self.identity,
                kind=FrameKind.PING,
                sequence=1,
                reply_to_frame_id=None,
            )
            startup_payload = _require_payload(startup_ping, {"nonce"})
            startup_nonce = _ping_nonce(startup_payload["nonce"])
            startup_pong = _build_frame(
                session_codec,
                self.identity,
                kind=FrameKind.PONG,
                direction=FrameDirection.CHILD_TO_SUPERVISOR,
                sequence=session_guard.next_sequence(
                    FrameDirection.CHILD_TO_SUPERVISOR,
                    FrameStream.CONTROL,
                ),
                reply_to_frame_id=startup_ping.ipc_frame_id,
                payload={"nonce": startup_nonce},
            )
            session_guard.observe(startup_pong)
            await _write_before_deadline(
                writer,
                session_codec,
                startup_pong,
                startup_deadline,
            )

            # The raw generation capability and bootstrap-only derived key are
            # no longer needed once the session-authenticated liveness proof
            # has completed.  This is intentionally not protocol READY.
            _zero_mutable(bootstrap_root)
            bootstrap_root = None
            _zero_mutable(proof_key)
            proof_key = None
            bootstrap_codec.close()
            bootstrap_codec = None

            return await self._serve(
                reader,
                writer,
                session_codec,
                session_guard,
                initial_responses=OrderedDict(
                    [(startup_ping.ipc_frame_id, startup_pong)]
                ),
            )
        except IpcTruncatedFrameError:
            raise
        except IpcStreamClosedError:
            # EOF is a supervisor-death fence for this non-executing host.
            return 0
        finally:
            _zero_mutable(bootstrap_root)
            _zero_mutable(proof_key)
            _zero_mutable(session_key)
            if bootstrap_codec is not None:
                bootstrap_codec.close()
            if session_codec is not None:
                session_codec.close()
            _close_fd(self.bootstrap_fd if self.bootstrap_fd >= 0 else None)
            _close_fd(self.control_fd if self.control_fd >= 0 else None)
            _close_fd(self.data_fd if self.data_fd >= 0 else None)
            if writer is not None:
                await _close_stream_writer(writer, self.control_timeout)
            if data_socket is not None:
                data_socket.close()

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        codec: FrameCodec,
        guard: SequenceReplayGuard,
        *,
        initial_responses: OrderedDict[str, IpcFrame | None] | None = None,
    ) -> int:
        state = AgentProcessControlState.BOOTSTRAPPED
        responses = (
            OrderedDict()
            if initial_responses is None
            else OrderedDict(initial_responses)
        )

        while True:
            frame = await read_frame(
                reader,
                codec,
                expected_direction=FrameDirection.SUPERVISOR_TO_CHILD,
                expected_stream=FrameStream.CONTROL,
            )
            disposition = guard.observe(frame)
            if disposition is ReplayDisposition.DUPLICATE:
                if frame.ipc_frame_id not in responses:
                    raise IpcSequenceError("duplicate control response was evicted")
                previous = responses[frame.ipc_frame_id]
                if previous is not None:
                    await _write_with_timeout(
                        writer,
                        codec,
                        previous,
                        self.control_timeout,
                    )
                continue

            _require_exact_identity(frame, self.identity)
            if frame.reply_to_frame_id is not None:
                raise IpcProtocolError("control request cannot be a response")

            if frame.kind is FrameKind.PING:
                payload = _require_payload(frame, {"nonce"})
                response_kind = FrameKind.PONG
                response_payload = {
                    "nonce": _ping_nonce(payload["nonce"]),
                }
            elif frame.kind is FrameKind.QUIESCE:
                _require_payload(frame, set())
                if state is AgentProcessControlState.BOOTSTRAPPED:
                    state = AgentProcessControlState.QUIESCED
                elif state is not AgentProcessControlState.QUIESCED:
                    raise IpcProtocolError("invalid child QUIESCE state")
                # QUIESCE has no invented ACK kind.  Remember it for exact
                # duplicate handling; an ordered PING/PONG can subsequently
                # observe the latched nonschedulable state.
                responses[frame.ipc_frame_id] = None
                while len(responses) > self.replay_window:
                    responses.popitem(last=False)
                continue
            elif frame.kind is FrameKind.SHUTDOWN:
                _require_payload(frame, set())
                # SHUTDOWN likewise has no ACK in IPC v1.  A clean process
                # exit and control-stream EOF are the observable completion.
                return 0
            else:
                raise IpcProtocolError("frame kind is outside the liveness protocol")

            response = _build_frame(
                codec,
                self.identity,
                kind=response_kind,
                direction=FrameDirection.CHILD_TO_SUPERVISOR,
                sequence=guard.next_sequence(
                    FrameDirection.CHILD_TO_SUPERVISOR,
                    FrameStream.CONTROL,
                ),
                reply_to_frame_id=frame.ipc_frame_id,
                payload=response_payload,
            )
            guard.observe(response)
            responses[frame.ipc_frame_id] = response
            while len(responses) > self.replay_window:
                responses.popitem(last=False)
            await _write_with_timeout(
                writer,
                codec,
                response,
                self.control_timeout,
            )


class AgentProcessHandle:
    """Supervisor-side owner of one bootstrapped liveness child."""

    def __init__(
        self,
        *,
        identity: AgentProcessIdentity,
        process: subprocess.Popen[Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        data_socket: socket.socket,
        codec: FrameCodec,
        replay_guard: SequenceReplayGuard,
        process_group: VerifiedProcessGroup,
        lifetime_lock: InheritedLifetimeLock,
        control_timeout: float,
        exit_timeout: float,
    ) -> None:
        self.identity = identity
        self.process = process
        self._reader = reader
        self._writer = writer
        self._data_socket = data_socket
        self._codec = codec
        self._guard = replay_guard
        self._process_group = process_group
        self._lifetime_lock = lifetime_lock
        self._control_timeout = control_timeout
        self._exit_timeout = exit_timeout
        self._process_group_id = process_group.process_group_id
        self._state = AgentProcessControlState.BOOTSTRAPPED
        self._operation_lock = asyncio.Lock()

    @property
    def pid(self) -> int:
        return self.process.pid

    @property
    def returncode(self) -> int | None:
        return self.process.poll()

    @property
    def process_group_id(self) -> int:
        return self._process_group_id

    @property
    def kernel_process_birth_id(self) -> str:
        return self._process_group.kernel_birth_id

    @property
    def lifetime_lock_identity(self) -> str:
        return self._lifetime_lock.identity

    @property
    def lifetime_lock_path(self) -> str:
        return self._lifetime_lock.path

    @property
    def lifetime_lock_released(self) -> bool:
        return self._lifetime_lock.released

    @property
    def state(self) -> AgentProcessControlState:
        return self._state

    @classmethod
    async def spawn(
        cls,
        identity: AgentProcessIdentity,
        *,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
        control_timeout: float = DEFAULT_CONTROL_TIMEOUT,
        exit_timeout: float = DEFAULT_EXIT_TIMEOUT,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        replay_window: int = DEFAULT_REPLAY_WINDOW,
        generation_capability: bytes | bytearray | memoryview | None = None,
        working_directory: str | os.PathLike[str] | None = None,
        lifetime_lock_root: str | os.PathLike[str] = (
            DEFAULT_AGENT_LIFETIME_LOCK_ROOT
        ),
    ) -> AgentProcessHandle:
        """Spawn a fresh interpreter and prove non-schedulable IPC liveness."""

        if not sys.platform.startswith("linux"):
            raise AgentProcessError(
                "verified Agent child lifetime currently requires Linux"
            )
        if not isinstance(identity, AgentProcessIdentity):
            raise TypeError("identity must be an AgentProcessIdentity")
        for value, field in (
            (startup_timeout, "startup_timeout"),
            (control_timeout, "control_timeout"),
            (exit_timeout, "exit_timeout"),
        ):
            if (
                type(value) not in {int, float}
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(f"{field} must be positive")
        if (
            type(max_frame_bytes) is not int
            or max_frame_bytes <= 0
            or max_frame_bytes > HARD_MAX_FRAME_BYTES
        ):
            raise ValueError("max_frame_bytes exceeds the hard protocol bound")
        if (
            type(replay_window) is not int
            or replay_window <= 0
            or replay_window > MAX_REPLAY_WINDOW
        ):
            raise ValueError("replay_window is outside the supported bound")
        # Validate and copy all caller-owned secret input before acquiring any
        # raw-FD lifetime resources.  Validation failure must be resource-free.
        capability_buffer = bytearray(
            os.urandom(GENERATION_CAPABILITY_BYTES)
            if generation_capability is None
            else _secret_bytes(
                generation_capability,
                field="generation capability",
                exact_length=GENERATION_CAPABILITY_BYTES,
            )
        )
        try:
            require_linux_pidfd_support()
            parent_birth = capture_linux_process_birth(os.getpid())
            lifetime_lock: InheritedLifetimeLock | None = (
                InheritedLifetimeLock.create(lifetime_lock_root)
            )
        except (ProcessLifetimeError, OSError) as exc:
            _zero_mutable(capability_buffer)
            raise AgentProcessError(
                "required Agent process lifetime evidence is unavailable"
            ) from exc
        control_parent: socket.socket | None = None
        control_child: socket.socket | None = None
        data_parent: socket.socket | None = None
        data_child: socket.socket | None = None
        bootstrap_read: int | None = None
        bootstrap_write: int | None = None
        process: subprocess.Popen[Any] | None = None
        process_group: VerifiedProcessGroup | None = None
        exact_process: ExactProcessPidfd | None = None
        writer: asyncio.StreamWriter | None = None
        bootstrap_root: bytearray | None = None
        proof_key: bytearray | None = None
        session_key: bytearray | None = None
        bootstrap_codec: BootstrapFrameCodec | None = None
        session_codec: FrameCodec | None = None
        startup_deadline = (
            asyncio.get_running_loop().time() + float(startup_timeout)
        )
        try:
            control_parent, control_child = socket.socketpair(
                socket.AF_UNIX,
                socket.SOCK_STREAM,
            )
            data_parent, data_child = socket.socketpair(
                socket.AF_UNIX,
                socket.SOCK_STREAM,
            )
            for endpoint in (
                control_parent,
                control_child,
                data_parent,
                data_child,
            ):
                endpoint.set_inheritable(False)
            bootstrap_read, bootstrap_write = _new_cloexec_pipe()
            launch_nonce = os.urandom(HANDSHAKE_NONCE_BYTES)
            bootstrap_root = bytearray(
                _derive_bootstrap_root(
                    capability_buffer,
                    identity,
                    launch_nonce,
                )
            )
            proof_key = bytearray(_derive_bootstrap_proof_key(bootstrap_root))

            # Production launch always uses this supervisor's interpreter.
            # Alternate executables belong in subprocess-level tests, never
            # in the production spawn API.
            executable = os.path.realpath(sys.executable)
            child_script = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "agent_child.py")
            )
            command = [
                executable,
                "-I",
                "-S",
                child_script,
                "--child",
                "--control-fd",
                str(control_child.fileno()),
                "--data-fd",
                str(data_child.fileno()),
                "--bootstrap-fd",
                str(bootstrap_read),
                "--lifetime-fd",
                str(lifetime_lock.child_fd),
                "--supervisor-epoch",
                str(identity.supervisor_epoch),
                "--agent-id",
                identity.agent_id,
                "--agent-incarnation",
                str(identity.agent_incarnation),
                "--worker-generation",
                str(identity.worker_generation),
                "--protocol-version",
                str(identity.protocol_version),
                "--max-frame-bytes",
                str(max_frame_bytes),
                "--startup-timeout",
                repr(float(startup_timeout)),
                "--control-timeout",
                repr(float(control_timeout)),
                "--replay-window",
                str(replay_window),
                "--parent-pid",
                str(parent_birth.pid),
                "--parent-start-time",
                str(parent_birth.start_time_ticks),
                "--parent-boot-id",
                parent_birth.boot_id,
                "--lifetime-device",
                str(lifetime_lock.device),
                "--lifetime-inode",
                str(lifetime_lock.inode),
                "--lifetime-identity",
                lifetime_lock.identity,
            ]
            child_environment = {
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUNBUFFERED": "1",
                "PYTHONUTF8": "1",
            }
            process = subprocess.Popen(
                command,
                cwd=os.fspath(working_directory) if working_directory else None,
                env=child_environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=(
                    control_child.fileno(),
                    data_child.fileno(),
                    bootstrap_read,
                    lifetime_lock.child_fd,
                ),
                start_new_session=True,
            )
            try:
                exact_process = ExactProcessPidfd.capture(process.pid)
            except (ProcessLookupError, ProcessLifetimeError) as exc:
                # A process that already exited can be reaped without a
                # signal.  A live child without a pidfd is a fatal fail-closed
                # condition because no numeric-PID fallback is safe.
                if process.poll() is None:
                    raise AgentProcessHandshakeError(
                        "Agent child exact pidfd capture failed while still live"
                    ) from exc
                lifetime_lock.release_parent_copy()
                if not lifetime_lock.prove_released():
                    raise AgentProcessHandshakeError(
                        "exited Agent child retained its lifetime lock"
                    ) from exc
                raise AgentProcessHandshakeError(
                    "Agent child exited before exact pidfd capture"
                ) from exc
            try:
                process_group = VerifiedProcessGroup.capture_dedicated(process.pid)
            except (ProcessLookupError, ProcessLifetimeError) as exc:
                raise AgentProcessHandshakeError(
                    "Agent child failed kernel birth/process-group verification"
                ) from exc
            exact_process.close()
            exact_process = None
            control_child.close()
            control_child = None
            data_child.close()
            data_child = None
            _close_fd(bootstrap_read)
            bootstrap_read = None
            _write_bootstrap_material(
                bootstrap_write,
                capability_buffer,
                launch_nonce,
            )
            _close_fd(bootstrap_write)
            bootstrap_write = None
            _zero_mutable(capability_buffer)

            control_parent.setblocking(False)
            connection_timeout = _deadline_remaining(startup_deadline)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(sock=control_parent),
                timeout=connection_timeout,
            )
            control_parent = None  # transport owns the endpoint

            bootstrap_codec = BootstrapFrameCodec(
                proof_key,
                max_frame_bytes=max_frame_bytes,
            )
            handshake_guard = SequenceReplayGuard(
                identity.ipc_identity,
                replay_window=replay_window,
            )
            hello_timeout = _deadline_remaining(startup_deadline)
            hello = await asyncio.wait_for(
                read_frame(
                    reader,
                    bootstrap_codec,
                    expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
                    expected_stream=FrameStream.CONTROL,
                ),
                timeout=hello_timeout,
            )
            handshake_guard.observe(hello)
            _require_frame_shape(
                hello,
                identity,
                kind=FrameKind.HELLO,
                sequence=0,
                reply_to_frame_id=None,
            )
            hello_payload = _require_payload(
                hello,
                {
                    "child_nonce",
                    "child_pid",
                    "feature",
                    "launch_nonce",
                    "lifetime_lock_identity",
                    "parent_birth_id",
                    "parent_pid",
                },
            )
            if hello_payload["child_pid"] != process.pid:
                raise IpcProtocolError("HELLO child process does not match")
            if hello_payload["parent_pid"] != os.getpid():
                raise IpcProtocolError("HELLO parent process does not match")
            if hello_payload["parent_birth_id"] != parent_birth.wire_birth_id:
                raise IpcProtocolError("HELLO parent birth identity does not match")
            if hello_payload["lifetime_lock_identity"] != lifetime_lock.identity:
                raise IpcProtocolError("HELLO lifetime-lock identity does not match")
            if hello_payload["feature"] != AGENT_PROCESS_FEATURE:
                raise IpcProtocolError("HELLO feature negotiation failed")
            if hello_payload["launch_nonce"] != launch_nonce.hex():
                raise IpcProtocolError("HELLO launch nonce does not match")
            # From this point the authenticated child/tree is the only holder
            # of the locked description; the supervisor retains only its
            # distinct non-owning release probe.
            lifetime_lock.release_parent_copy()
            child_nonce = _wire_nonce(hello_payload["child_nonce"], "child_nonce")
            supervisor_nonce = os.urandom(HANDSHAKE_NONCE_BYTES)
            config = _build_frame(
                bootstrap_codec,
                identity,
                kind=FrameKind.CONFIG,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                sequence=0,
                reply_to_frame_id=hello.ipc_frame_id,
                payload={
                    "child_nonce": child_nonce.hex(),
                    "feature": AGENT_PROCESS_FEATURE,
                    "launch_nonce": launch_nonce.hex(),
                    "lifetime_lock_identity": lifetime_lock.identity,
                    "max_frame_bytes": max_frame_bytes,
                    "parent_birth_id": parent_birth.wire_birth_id,
                    "supervisor_nonce": supervisor_nonce.hex(),
                    "supervisor_pid": os.getpid(),
                },
            )
            handshake_guard.observe(config)
            await _write_before_deadline(
                writer,
                bootstrap_codec,
                config,
                startup_deadline,
            )

            session_key = bytearray(
                _derive_session_key(
                    bootstrap_root,
                    identity,
                    launch_nonce,
                    child_nonce,
                    supervisor_nonce,
                )
            )
            session_codec = FrameCodec(
                session_key,
                max_frame_bytes=max_frame_bytes,
            )
            session_guard = _session_guard(
                identity,
                replay_window=replay_window,
            )
            startup_nonce = secrets.token_hex(16)
            startup_ping = _build_frame(
                session_codec,
                identity,
                kind=FrameKind.PING,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                sequence=session_guard.next_sequence(
                    FrameDirection.SUPERVISOR_TO_CHILD,
                    FrameStream.CONTROL,
                ),
                payload={"nonce": startup_nonce},
            )
            session_guard.observe(startup_ping)
            await _write_before_deadline(
                writer,
                session_codec,
                startup_ping,
                startup_deadline,
            )
            startup_pong_timeout = _deadline_remaining(startup_deadline)
            startup_pong = await asyncio.wait_for(
                read_frame(
                    reader,
                    session_codec,
                    expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
                    expected_stream=FrameStream.CONTROL,
                ),
                timeout=startup_pong_timeout,
            )
            session_guard.observe(startup_pong)
            _require_frame_shape(
                startup_pong,
                identity,
                kind=FrameKind.PONG,
                sequence=1,
                reply_to_frame_id=startup_ping.ipc_frame_id,
            )
            startup_payload = _require_payload(startup_pong, {"nonce"})
            if startup_payload != {"nonce": startup_nonce}:
                raise IpcProtocolError(
                    "Agent child returned an invalid bootstrap liveness proof"
                )

            bootstrap_codec.close()
            bootstrap_codec = None

            handle = cls(
                identity=identity,
                process=process,
                reader=reader,
                writer=writer,
                data_socket=data_parent,
                codec=session_codec,
                replay_guard=session_guard,
                process_group=process_group,
                lifetime_lock=lifetime_lock,
                control_timeout=float(control_timeout),
                exit_timeout=float(exit_timeout),
            )
            process = None
            process_group = None
            lifetime_lock = None
            writer = None
            data_parent = None
            session_codec = None
            return handle
        except BaseException as exc:
            if process is not None:
                assert lifetime_lock is not None
                if process_group is None:
                    # Group capture happens before bootstrap material is
                    # written.  Close every local endpoint first so the
                    # trusted direct child observes bootstrap EOF and cannot
                    # advance to runtime/fork capability.
                    for endpoint in (
                        control_parent,
                        control_child,
                        data_parent,
                        data_child,
                    ):
                        if endpoint is not None:
                            endpoint.close()
                    control_parent = control_child = None
                    data_parent = data_child = None
                    _close_fd(bootstrap_read)
                    _close_fd(bootstrap_write)
                    bootstrap_read = bootstrap_write = None
                cleanup_owner = AgentProcessCleanupOwner(
                    process=process,
                    lifetime_lock=lifetime_lock,
                    default_timeout=float(exit_timeout),
                    process_group=process_group,
                    exact_process=exact_process,
                    prebootstrap_eof_only=(
                        process_group is None and exact_process is None
                    ),
                )
                # Transfer before awaiting: cancellation or cleanup failure
                # must leave exactly one strong owner of every raw FD/probe.
                process = None
                process_group = None
                exact_process = None
                lifetime_lock = None
                try:
                    await _await_cleanup_to_completion(cleanup_owner.retry())
                except BaseException as cleanup_exc:
                    if cleanup_owner.closed:
                        raise
                    raise AgentProcessCleanupError(
                        startup_error=exc,
                        cleanup_error=cleanup_exc,
                        cleanup_owner=cleanup_owner,
                    ) from cleanup_exc
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(exc, AgentProcessError):
                raise
            raise AgentProcessHandshakeError(
                "Agent child failed the authenticated startup handshake"
            ) from exc
        finally:
            _zero_mutable(capability_buffer)
            _zero_mutable(bootstrap_root)
            _zero_mutable(proof_key)
            _zero_mutable(session_key)
            if bootstrap_codec is not None:
                bootstrap_codec.close()
            if session_codec is not None:
                session_codec.close()
            if lifetime_lock is not None and process is None:
                lifetime_lock.release_parent_copy()
                lifetime_lock.prove_released()
            if process_group is not None and process is None:
                process_group.close()
            if exact_process is not None and process is None:
                exact_process.close()
            _close_fd(bootstrap_read)
            _close_fd(bootstrap_write)
            for endpoint in (
                control_parent,
                control_child,
                data_parent,
                data_child,
            ):
                if endpoint is not None:
                    endpoint.close()
            if writer is not None:
                with contextlib.suppress(BaseException):
                    await _await_cleanup_to_completion(
                        _close_stream_writer(writer, float(control_timeout))
                    )

    async def ping(self, nonce: str | None = None) -> str:
        """Prove control-stream liveness; this does not renew a work lease."""

        value = _ping_nonce(secrets.token_hex(16) if nonce is None else nonce)
        async with self._operation_lock:
            try:
                await self._ensure_control_available()
                request = await self._send_control_locked(
                    FrameKind.PING,
                    {"nonce": value},
                )
                response = await self._receive_control_locked(
                    request,
                    expected_kind=FrameKind.PONG,
                )
                payload = _require_payload(response, {"nonce"})
                if payload["nonce"] != value:
                    raise IpcProtocolError("Agent child returned an invalid PONG")
                return value
            except BaseException as exc:
                await self._control_failed_locked(exc)
                raise AssertionError("unreachable")  # pragma: no cover

    async def quiesce(self) -> None:
        async with self._operation_lock:
            if self._state is AgentProcessControlState.QUIESCED:
                return
            try:
                await self._ensure_control_available()
                await self._send_control_locked(FrameKind.QUIESCE, {})
                # IPC v1 defines no QUIESCE ACK.  This ordered PING is handled
                # only after QUIESCE and observes the latched state through
                # its ordinary PONG response.
                nonce = secrets.token_hex(16)
                ping = await self._send_control_locked(
                    FrameKind.PING,
                    {"nonce": nonce},
                )
                response = await self._receive_control_locked(
                    ping,
                    expected_kind=FrameKind.PONG,
                )
                payload = _require_payload(
                    response,
                    {"nonce"},
                )
                if payload != {"nonce": nonce}:
                    raise IpcProtocolError("Agent child did not latch QUIESCE")
                self._state = AgentProcessControlState.QUIESCED
            except BaseException as exc:
                await self._control_failed_locked(exc)

    async def shutdown(self) -> int:
        """Request an authenticated graceful exit and reap the exact child."""

        return await _await_cleanup_to_completion(self._shutdown_serialized())

    async def _shutdown_serialized(self) -> int:
        async with self._operation_lock:
            if self._state is AgentProcessControlState.CLOSED:
                return self.process.returncode or 0
            try:
                await self._ensure_control_available()
                await self._send_control_locked(FrameKind.SHUTDOWN, {})
                self._state = AgentProcessControlState.STOPPING
            except BaseException as exc:
                await self._control_failed_locked(exc)
                raise AssertionError("unreachable")  # pragma: no cover
            await self._close_endpoints_locked()
            try:
                returncode = await self._wait_locked(timeout=self._exit_timeout)
            except AgentProcessError:
                await self._terminate_locked()
                raise
            self._state = AgentProcessControlState.CLOSED
            if returncode != 0:
                raise AgentProcessExitedError("Agent child exited unsuccessfully")
            return returncode

    async def close_connection(self) -> int:
        """Close IPC; an idle liveness child treats supervisor EOF as clean exit."""

        return await _await_cleanup_to_completion(
            self._close_connection_serialized()
        )

    async def _close_connection_serialized(self) -> int:
        async with self._operation_lock:
            if self._state is AgentProcessControlState.CLOSED:
                return self.process.returncode or 0
            self._state = AgentProcessControlState.STOPPING
            await self._close_endpoints_locked()
            try:
                returncode = await self._wait_locked(timeout=self._exit_timeout)
            except AgentProcessError:
                await self._terminate_locked()
                raise
            self._state = AgentProcessControlState.CLOSED
            if returncode != 0:
                raise AgentProcessExitedError("Agent child rejected connection EOF")
            return returncode

    async def terminate(self) -> int:
        """Fail-closed cleanup for a broken bootstrap/liveness generation."""

        return await _await_cleanup_to_completion(self._terminate_serialized())

    async def _terminate_serialized(self) -> int:
        async with self._operation_lock:
            return await self._terminate_locked()

    async def _terminate_locked(self) -> int:
        if self._state is AgentProcessControlState.CLOSED:
            return self.process.returncode or 0
        self._state = AgentProcessControlState.STOPPING
        await self._close_endpoints_locked()
        await _terminate_process_group(
            self.process,
            self._exit_timeout,
            process_group=self._process_group,
            lifetime_lock=self._lifetime_lock,
        )
        self._state = AgentProcessControlState.CLOSED
        return self.process.returncode if self.process.returncode is not None else -1

    async def wait(self, *, timeout: float | None = None) -> int:
        """Wait for verified exit and release every process-handle endpoint."""

        async with self._operation_lock:
            if self._state is AgentProcessControlState.CLOSED:
                return self.process.returncode or 0
            return await self._wait_locked(timeout=timeout)

    async def _wait_locked(self, *, timeout: float | None = None) -> int:
        exited = await _wait_for_process_group_exit(
            self.process,
            self._process_group_id,
            timeout=timeout,
            process_group=self._process_group,
            lifetime_lock=self._lifetime_lock,
        )
        if not exited:
            raise AgentProcessExitedError("Agent child did not exit in time")
        returncode = self.process.poll()
        if returncode is None:  # pragma: no cover - group proof includes leader exit
            raise AgentProcessExitedError("Agent child exit could not be reaped")
        await self._close_endpoints_locked()
        self._state = AgentProcessControlState.CLOSED
        return returncode

    async def _ensure_control_available(self) -> None:
        if self._state not in {
            AgentProcessControlState.BOOTSTRAPPED,
            AgentProcessControlState.QUIESCED,
        }:
            raise AgentProcessStateError("Agent child is not accepting control")
        if self.process.poll() is not None:
            self._state = AgentProcessControlState.STOPPING
            raise AgentProcessExitedError("Agent child already exited")

    async def _send_control_locked(
        self,
        kind: FrameKind,
        payload: Mapping[str, Any],
    ) -> IpcFrame:
        request = _build_frame(
            self._codec,
            self.identity,
            kind=kind,
            direction=FrameDirection.SUPERVISOR_TO_CHILD,
            sequence=self._guard.next_sequence(
                FrameDirection.SUPERVISOR_TO_CHILD,
                FrameStream.CONTROL,
            ),
            payload=payload,
        )
        self._guard.observe(request)
        await asyncio.wait_for(
            write_frame(self._writer, self._codec, request),
            timeout=self._control_timeout,
        )
        return request

    async def _receive_control_locked(
        self,
        request: IpcFrame,
        *,
        expected_kind: FrameKind,
    ) -> IpcFrame:
        response = await asyncio.wait_for(
            read_frame(
                self._reader,
                self._codec,
                expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
                expected_stream=FrameStream.CONTROL,
            ),
            timeout=self._control_timeout,
        )
        if self._guard.observe(response) is not ReplayDisposition.NEW:
            raise IpcSequenceError("unexpected duplicate control response")
        _require_frame_shape(
            response,
            self.identity,
            kind=expected_kind,
            reply_to_frame_id=request.ipc_frame_id,
        )
        return response

    async def _control_failed_locked(self, exc: BaseException) -> None:
        await _await_cleanup_to_completion(self._terminate_locked())
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            raise exc
        if isinstance(exc, AgentProcessError):
            raise exc
        raise AgentProcessExitedError(
            "Agent child control exchange failed"
        ) from exc

    async def _close_endpoints_locked(self) -> None:
        if not self._writer.is_closing():
            await _close_stream_writer(self._writer, self._control_timeout)
        with contextlib.suppress(OSError):
            self._data_socket.close()
        self._codec.close()

    async def __aenter__(self) -> AgentProcessHandle:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._state is AgentProcessControlState.CLOSED:
            return
        if exc_type is None:
            await self.shutdown()
        else:
            await self.terminate()


async def _await_cleanup_to_completion(awaitable: Any) -> Any:
    """Delay caller cancellation until mandatory process cleanup has finished."""

    cleanup = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(cleanup)
            break
        except asyncio.CancelledError as exc:
            if cleanup.cancelled():
                raise
            if cancellation is None:
                cancellation = exc
            continue
    if cancellation is not None:
        raise cancellation
    return result


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _wait_for_process_group_exit(
    process: subprocess.Popen[Any],
    process_group_id: int,
    *,
    timeout: float | None,
    process_group: VerifiedProcessGroup | None = None,
    lifetime_lock: InheritedLifetimeLock | None = None,
) -> bool:
    if process_group is None or lifetime_lock is None:
        raise AgentProcessExitedError(
            "Agent child exit lacks kernel birth/lifetime-lock evidence"
        )
    if process_group.process_group_id != process_group_id:
        raise AgentProcessExitedError("Agent child process-group identity conflicts")
    try:
        return await wait_for_verified_exit(
            process,
            process_group,
            lifetime_lock,
            timeout=timeout,
        )
    except ProcessLifetimeError as exc:
        raise AgentProcessExitedError(
            "Agent child exit evidence could not be verified"
        ) from exc


async def _wait_prebootstrap_eof_cleanup(
    process: subprocess.Popen[Any],
    lifetime_lock: InheritedLifetimeLock,
    timeout: float,
) -> None:
    """Wait/reap after bootstrap EOF when even pidfd capture was unavailable.

    No numeric signal is attempted.  Timeout is an explicit fatal ownership
    condition retained by ``AgentProcessCleanupError.cleanup_owner``.
    """

    lifetime_lock.release_parent_copy()
    deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout))
    while True:
        if process.poll() is not None and lifetime_lock.prove_released():
            return
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise AgentProcessExitedError(
                "pre-bootstrap child ignored EOF without exact pidfd evidence"
            )
        await asyncio.sleep(min(0.02, remaining))


async def _terminate_process_group(
    process: subprocess.Popen[Any],
    timeout: float,
    *,
    process_group: VerifiedProcessGroup | None = None,
    lifetime_lock: InheritedLifetimeLock | None = None,
) -> None:
    """Terminate exact birth-verified members and prove lock release.

    A bare numeric process group is deliberately insufficient: Linux exposes
    no atomic birth-bound ``killpg`` operation, so cleanup uses per-member
    pidfds and refuses to act when evidence is absent or mismatched.
    """

    if process_group is None or lifetime_lock is None:
        raise AgentProcessExitedError(
            "refusing Agent cleanup without kernel birth/lifetime-lock evidence"
        )
    try:
        await terminate_verified_process_group(
            process,
            process_group,
            lifetime_lock,
            timeout,
        )
    except ProcessLifetimeError as exc:
        raise AgentProcessExitedError(
            "Agent child process tree could not be safely proven stopped"
        ) from exc


def _positive_cli_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _child_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--child", action="store_true", required=True)
    parser.add_argument("--control-fd", type=int, required=True)
    parser.add_argument("--data-fd", type=int, required=True)
    parser.add_argument("--bootstrap-fd", type=int, required=True)
    parser.add_argument("--supervisor-epoch", type=_positive_cli_integer, required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--agent-incarnation", type=_positive_cli_integer, required=True)
    parser.add_argument("--worker-generation", type=_positive_cli_integer, required=True)
    parser.add_argument("--protocol-version", type=_positive_cli_integer, required=True)
    parser.add_argument("--max-frame-bytes", type=_positive_cli_integer, required=True)
    parser.add_argument("--startup-timeout", type=float, required=True)
    parser.add_argument("--control-timeout", type=float, required=True)
    parser.add_argument("--replay-window", type=_positive_cli_integer, required=True)
    return parser


def _main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = _child_parser().parse_args(argv)
        identity = AgentProcessIdentity(
            supervisor_epoch=arguments.supervisor_epoch,
            agent_id=arguments.agent_id,
            agent_incarnation=arguments.agent_incarnation,
            worker_generation=arguments.worker_generation,
            protocol_version=arguments.protocol_version,
        )
        host = AgentChildHost(
            identity,
            control_fd=arguments.control_fd,
            data_fd=arguments.data_fd,
            bootstrap_fd=arguments.bootstrap_fd,
            max_frame_bytes=arguments.max_frame_bytes,
            startup_timeout=arguments.startup_timeout,
            control_timeout=arguments.control_timeout,
            replay_window=arguments.replay_window,
        )
        return asyncio.run(host.run())
    except (Exception, KeyboardInterrupt):
        # The supervisor owns bounded diagnostics.  In particular, never print
        # authentication failures or bootstrap material from the child.
        return _CHILD_PROTOCOL_EXIT


__all__ = [
    "AGENT_PROCESS_FEATURE",
    "AgentChildHost",
    "AgentProcessCleanupError",
    "AgentProcessCleanupOwner",
    "AgentProcessControlState",
    "AgentProcessError",
    "AgentProcessExitedError",
    "AgentProcessHandle",
    "AgentProcessHandshakeError",
    "AgentProcessIdentity",
    "AgentProcessStateError",
    "DEFAULT_CONTROL_TIMEOUT",
    "DEFAULT_EXIT_TIMEOUT",
    "DEFAULT_STARTUP_TIMEOUT",
    "DEFAULT_AGENT_LIFETIME_LOCK_ROOT",
    "GENERATION_CAPABILITY_BYTES",
    "HANDSHAKE_NONCE_BYTES",
    "MAX_REPLAY_WINDOW",
    "MAX_PING_NONCE_BYTES",
]


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(_main())
