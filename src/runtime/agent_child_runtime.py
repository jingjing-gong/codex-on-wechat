"""Minimal authenticated runtime for a non-schedulable Agent child.

The direct-exec :mod:`agent_child` launcher arms Linux parent-death handling
before importing this module.  This runtime owns only bootstrap/control
liveness.  It has no supervisor client, store, channel, SDK, capability,
readiness, or assignment dependency.
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
import socket
import struct
import sys
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
else:  # pragma: no cover - direct child execution
    import ipc as _ipc

    if os.path.realpath(_ipc.__file__) != os.path.realpath(
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


AGENT_PROCESS_FEATURE = "bootstrap-liveness-v2"
GENERATION_CAPABILITY_BYTES = 32
HANDSHAKE_NONCE_BYTES = 32
MAX_REPLAY_WINDOW = 4_096
MAX_PING_NONCE_BYTES = 256
_CHILD_PROTOCOL_EXIT = 70
_BOOTSTRAP_MAGIC = b"COWABP01"
_BOOTSTRAP_PACKET = struct.Struct(">8s32s32s")
_ROOT_DOMAIN = b"codex-wechat-agent-bootstrap-root-v1\x00"
_PROOF_DOMAIN = b"codex-wechat-agent-bootstrap-proof-key-v1\x00"
_SESSION_DOMAIN = b"codex-wechat-agent-session-key-v1\x00"
_AGENT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_NONCE_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_MODULES = (
    "_sqlite3",
    "sqlite3",
    "src",
    "wechat_ilink",
    "openai_codex",
    "agent_process",
)


@dataclass(frozen=True, slots=True)
class ChildIdentity:
    supervisor_epoch: int
    agent_id: str
    agent_incarnation: int
    worker_generation: int
    protocol_version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if self.protocol_version != PROTOCOL_VERSION:
            raise ValueError("unsupported child protocol")
        if not _AGENT_ID_PATTERN.fullmatch(self.agent_id):
            raise ValueError("invalid child Agent ID")
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


def _identity_fields(identity: ChildIdentity) -> dict[str, Any]:
    return {
        "agent_id": identity.agent_id,
        "agent_incarnation": identity.agent_incarnation,
        "protocol_version": identity.protocol_version,
        "supervisor_epoch": identity.supervisor_epoch,
        "worker_generation": identity.worker_generation,
    }


def _derive_root(
    capability: bytes | bytearray | memoryview,
    identity: ChildIdentity,
    launch_nonce: bytes,
) -> bytes:
    context = canonical_json_bytes(
        {"identity": _identity_fields(identity), "launch_nonce": launch_nonce.hex()}
    )
    copied_capability = (
        bytearray(capability) if isinstance(capability, memoryview) else None
    )
    try:
        return hmac.new(
            copied_capability if copied_capability is not None else capability,
            _ROOT_DOMAIN + context,
            hashlib.sha256,
        ).digest()
    finally:
        _zero(copied_capability)


def _derive_proof_key(root: bytes | bytearray) -> bytes:
    return hmac.new(root, _PROOF_DOMAIN, hashlib.sha256).digest()


def _derive_session_key(
    root: bytes | bytearray,
    identity: ChildIdentity,
    launch_nonce: bytes,
    child_nonce: bytes,
    supervisor_nonce: bytes,
) -> bytes:
    context = canonical_json_bytes(
        {
            "child_nonce": child_nonce.hex(),
            "identity": _identity_fields(identity),
            "launch_nonce": launch_nonce.hex(),
            "supervisor_nonce": supervisor_nonce.hex(),
        }
    )
    return hmac.new(root, _SESSION_DOMAIN + context, hashlib.sha256).digest()


def _zero(value: bytearray | None) -> None:
    if value is not None:
        value[:] = b"\x00" * len(value)


def _close_fd(fd: int | None) -> None:
    if fd is not None and fd >= 0:
        with contextlib.suppress(OSError):
            os.close(fd)


def _set_cloexec(fd: int) -> None:
    os.set_inheritable(fd, False)
    if os.get_inheritable(fd):
        raise RuntimeError("cannot seal child descriptor")


def _consume_bootstrap(fd: int, identity: ChildIdentity) -> tuple[bytearray, bytes]:
    packet = bytearray(_BOOTSTRAP_PACKET.size)
    try:
        view = memoryview(packet)
        offset = 0
        while offset < len(packet):
            count = os.readv(fd, [view[offset:]])
            if count == 0:
                raise RuntimeError("bootstrap channel closed")
            offset += count
        if os.read(fd, 1):
            raise RuntimeError("invalid bootstrap material")
        magic = packet[:8]
        if not hmac.compare_digest(magic, _BOOTSTRAP_MAGIC):
            raise RuntimeError("invalid bootstrap material")
        launch_nonce = bytes(memoryview(packet)[40:72])
        root = bytearray(
            _derive_root(memoryview(packet)[8:40], identity, launch_nonce)
        )
        return root, launch_nonce
    finally:
        _zero(packet)
        _close_fd(fd)


def _wire_nonce(value: Any, field: str) -> bytes:
    if type(value) is not str or not _NONCE_PATTERN.fullmatch(value):
        raise IpcProtocolError(f"{field} is not a canonical nonce")
    return bytes.fromhex(value)


def _ping_nonce(value: Any) -> str:
    if type(value) is not str or not value:
        raise IpcProtocolError("PING nonce must be nonempty")
    encoded = value.encode("utf-8", errors="strict")
    if len(encoded) > MAX_PING_NONCE_BYTES or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    ):
        raise IpcProtocolError("PING nonce is invalid")
    return value


def _payload(frame: IpcFrame, keys: set[str]) -> Mapping[str, Any]:
    if set(frame.payload) != keys:
        raise IpcProtocolError("control payload fields do not match")
    return frame.payload


def _require_frame(
    frame: IpcFrame,
    identity: ChildIdentity,
    *,
    kind: FrameKind,
    sequence: int | None = None,
    reply_to: str | None | object = ...,
) -> None:
    if IpcSessionIdentity.from_frame(frame) != identity.ipc_identity:
        raise IpcSequenceError("frame identity does not match child generation")
    if frame.protocol_version != identity.protocol_version or frame.kind is not kind:
        raise IpcProtocolError("unexpected control frame")
    if frame.direction is not FrameDirection.SUPERVISOR_TO_CHILD:
        raise IpcProtocolError("unexpected control direction")
    if frame.stream is not FrameStream.CONTROL:
        raise IpcProtocolError("unexpected control stream")
    if sequence is not None and frame.stream_sequence != sequence:
        raise IpcSequenceError("unexpected control sequence")
    if reply_to is not ... and frame.reply_to_frame_id != reply_to:
        raise IpcProtocolError("unexpected control correlation")


def _build_child_frame(
    codec: FrameCodec,
    identity: ChildIdentity,
    *,
    kind: FrameKind,
    sequence: int,
    payload: Mapping[str, Any],
    reply_to: str | None = None,
) -> IpcFrame:
    return codec.build_frame(
        kind=kind,
        direction=FrameDirection.CHILD_TO_SUPERVISOR,
        stream=FrameStream.CONTROL,
        supervisor_epoch=identity.supervisor_epoch,
        agent_id=identity.agent_id,
        agent_incarnation=identity.agent_incarnation,
        worker_generation=identity.worker_generation,
        protocol_version=identity.protocol_version,
        ipc_frame_id=secrets.token_hex(16),
        reply_to_frame_id=reply_to,
        stream_sequence=sequence,
        payload=payload,
    )


def _session_guard(identity: ChildIdentity, replay_window: int) -> SequenceReplayGuard:
    return SequenceReplayGuard(
        identity.ipc_identity,
        initial_sequence=0,
        initial_sequences={
            (FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): 1,
            (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL): 1,
        },
        replay_window=replay_window,
    )


def _parent_birth(parent_pid: int) -> tuple[str, int]:
    boot_id = open(
        "/proc/sys/kernel/random/boot_id", "r", encoding="ascii"
    ).read().strip()
    value = open(f"/proc/{parent_pid}/stat", "r", encoding="ascii").read()
    closing = value.rfind(")")
    return boot_id, int(value[closing + 2 :].split()[19])


class _ForkDescriptorBoundary:
    """Close IPC in fork descendants while deliberately retaining lifetime."""

    def __init__(self, control_fd: int, data_fd: int, lifetime_fd: int) -> None:
        self._leader_pid = os.getpid()
        self._descriptors = {
            fd: (os.fstat(fd).st_dev, os.fstat(fd).st_ino)
            for fd in (control_fd, data_fd)
        }
        self._lifetime_fd = lifetime_fd
        os.register_at_fork(after_in_child=self._after_fork_child)

    def _after_fork_child(self) -> None:
        if os.getpid() == self._leader_pid:  # pragma: no cover - callback contract
            return
        for fd, expected in self._descriptors.items():
            try:
                current = os.fstat(fd)
            except OSError:
                continue
            if (current.st_dev, current.st_ino) == expected:
                _close_fd(fd)
        # Direct fork descendants retain the lifetime proof.  Exec descendants
        # retain it only when their launcher does not explicitly close it;
        # production tool execution still requires cgroup-backed containment.
        with contextlib.suppress(OSError):
            os.set_inheritable(self._lifetime_fd, True)


class ChildHost:
    def __init__(
        self,
        identity: ChildIdentity,
        *,
        control_fd: int,
        data_fd: int,
        bootstrap_fd: int,
        lifetime_fd: int,
        lifetime_device: int,
        lifetime_inode: int,
        lifetime_identity: str,
        parent_pid: int,
        parent_start_time: int,
        parent_boot_id: str,
        max_frame_bytes: int,
        startup_timeout: float,
        control_timeout: float,
        replay_window: int,
    ) -> None:
        self.identity = identity
        self.control_fd = control_fd
        self.data_fd = data_fd
        self.bootstrap_fd = bootstrap_fd
        self.lifetime_fd = lifetime_fd
        self.lifetime_device = lifetime_device
        self.lifetime_inode = lifetime_inode
        self.lifetime_identity = lifetime_identity
        self.parent_pid = parent_pid
        self.parent_start_time = parent_start_time
        self.parent_boot_id = parent_boot_id
        self.max_frame_bytes = max_frame_bytes
        self.startup_timeout = startup_timeout
        self.control_timeout = control_timeout
        self.replay_window = replay_window

    def _check_parent(self) -> None:
        if os.getppid() != self.parent_pid:
            raise RuntimeError("Agent supervisor relationship was lost")
        boot_id, start_time = _parent_birth(self.parent_pid)
        if boot_id != self.parent_boot_id or start_time != self.parent_start_time:
            raise RuntimeError("Agent supervisor birth identity changed")

    async def _watch_read(
        self,
        reader: asyncio.StreamReader,
        codec: FrameCodec,
        *,
        deadline: float | None = None,
    ) -> IpcFrame:
        read = asyncio.create_task(
            read_frame(
                reader,
                codec,
                expected_direction=FrameDirection.SUPERVISOR_TO_CHILD,
                expected_stream=FrameStream.CONTROL,
            )
        )
        try:
            while True:
                timeout = min(0.25, self.control_timeout)
                if deadline is not None:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError("child startup deadline expired")
                    timeout = min(timeout, remaining)
                done, _ = await asyncio.wait({read}, timeout=timeout)
                if done:
                    return await read
                self._check_parent()
        finally:
            if not read.done():
                read.cancel()
                with contextlib.suppress(BaseException):
                    await read

    async def run(self) -> int:
        loaded = tuple(sys.modules)
        if any(
            name == prefix or name.startswith(prefix + ".")
            for name in loaded
            for prefix in _FORBIDDEN_MODULES
        ):
            raise RuntimeError("Agent child import boundary was violated")
        if len({self.control_fd, self.data_fd, self.bootstrap_fd, self.lifetime_fd}) != 4:
            raise RuntimeError("child descriptors are not distinct")
        for fd in (self.control_fd, self.data_fd, self.bootstrap_fd):
            _set_cloexec(fd)
        lifetime_stat = os.fstat(self.lifetime_fd)
        if (lifetime_stat.st_dev, lifetime_stat.st_ino) != (
            self.lifetime_device,
            self.lifetime_inode,
        ):
            raise RuntimeError("lifetime-lock descriptor identity changed")
        os.set_inheritable(self.lifetime_fd, True)
        self._check_parent()
        deadline = asyncio.get_running_loop().time() + self.startup_timeout

        root: bytearray | None = None
        proof_key: bytearray | None = None
        session_key: bytearray | None = None
        bootstrap_codec: BootstrapFrameCodec | None = None
        session_codec: FrameCodec | None = None
        writer: asyncio.StreamWriter | None = None
        data_socket: socket.socket | None = None
        try:
            root, launch_nonce = _consume_bootstrap(self.bootstrap_fd, self.identity)
            self.bootstrap_fd = -1
            control_socket = socket.socket(fileno=self.control_fd)
            self.control_fd = -1
            control_socket.setblocking(False)
            data_socket = socket.socket(fileno=self.data_fd)
            self.data_fd = -1
            data_socket.setblocking(False)
            _ForkDescriptorBoundary(
                control_socket.fileno(), data_socket.fileno(), self.lifetime_fd
            )
            reader, writer = await asyncio.open_connection(sock=control_socket)

            proof_key = bytearray(_derive_proof_key(root))
            bootstrap_codec = BootstrapFrameCodec(
                proof_key, max_frame_bytes=self.max_frame_bytes
            )
            handshake_guard = SequenceReplayGuard(
                self.identity.ipc_identity, replay_window=self.replay_window
            )
            child_nonce = os.urandom(HANDSHAKE_NONCE_BYTES)
            parent_birth_id = f"linux:{self.parent_boot_id}:{self.parent_start_time}"
            hello = _build_child_frame(
                bootstrap_codec,
                self.identity,
                kind=FrameKind.HELLO,
                sequence=0,
                payload={
                    "child_nonce": child_nonce.hex(),
                    "child_pid": os.getpid(),
                    "feature": AGENT_PROCESS_FEATURE,
                    "launch_nonce": launch_nonce.hex(),
                    "lifetime_lock_identity": self.lifetime_identity,
                    "parent_birth_id": parent_birth_id,
                    "parent_pid": self.parent_pid,
                },
            )
            handshake_guard.observe(hello)
            await asyncio.wait_for(
                write_frame(writer, bootstrap_codec, hello),
                timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
            )
            config = await self._watch_read(reader, bootstrap_codec, deadline=deadline)
            handshake_guard.observe(config)
            _require_frame(
                config,
                self.identity,
                kind=FrameKind.CONFIG,
                sequence=0,
                reply_to=hello.ipc_frame_id,
            )
            payload = _payload(
                config,
                {
                    "child_nonce",
                    "feature",
                    "launch_nonce",
                    "lifetime_lock_identity",
                    "max_frame_bytes",
                    "parent_birth_id",
                    "supervisor_nonce",
                    "supervisor_pid",
                },
            )
            expected = {
                "child_nonce": child_nonce.hex(),
                "feature": AGENT_PROCESS_FEATURE,
                "launch_nonce": launch_nonce.hex(),
                "lifetime_lock_identity": self.lifetime_identity,
                "max_frame_bytes": self.max_frame_bytes,
                "parent_birth_id": parent_birth_id,
                "supervisor_pid": self.parent_pid,
            }
            if {key: payload[key] for key in expected} != expected:
                raise IpcProtocolError("CONFIG generation evidence does not match")
            supervisor_nonce = _wire_nonce(
                payload["supervisor_nonce"], "supervisor_nonce"
            )
            session_key = bytearray(
                _derive_session_key(
                    root,
                    self.identity,
                    launch_nonce,
                    child_nonce,
                    supervisor_nonce,
                )
            )
            session_codec = FrameCodec(
                session_key, max_frame_bytes=self.max_frame_bytes
            )
            guard = _session_guard(self.identity, self.replay_window)
            ping = await self._watch_read(reader, session_codec, deadline=deadline)
            guard.observe(ping)
            _require_frame(
                ping,
                self.identity,
                kind=FrameKind.PING,
                sequence=1,
                reply_to=None,
            )
            nonce = _ping_nonce(_payload(ping, {"nonce"})["nonce"])
            pong = _build_child_frame(
                session_codec,
                self.identity,
                kind=FrameKind.PONG,
                sequence=guard.next_sequence(
                    FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL
                ),
                reply_to=ping.ipc_frame_id,
                payload={"nonce": nonce},
            )
            guard.observe(pong)
            await asyncio.wait_for(
                write_frame(writer, session_codec, pong),
                timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
            )
            _zero(root)
            root = None
            _zero(proof_key)
            proof_key = None
            bootstrap_codec.close()
            bootstrap_codec = None
            return await self._serve(
                reader,
                writer,
                session_codec,
                guard,
                OrderedDict([(ping.ipc_frame_id, pong)]),
            )
        except IpcTruncatedFrameError:
            raise
        except IpcStreamClosedError:
            return 0
        finally:
            _zero(root)
            _zero(proof_key)
            _zero(session_key)
            if bootstrap_codec is not None:
                bootstrap_codec.close()
            if session_codec is not None:
                session_codec.close()
            _close_fd(self.bootstrap_fd)
            _close_fd(self.control_fd)
            _close_fd(self.data_fd)
            _close_fd(self.lifetime_fd)
            if writer is not None:
                writer.close()
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(
                        writer.wait_closed(), timeout=self.control_timeout
                    )
            if data_socket is not None:
                data_socket.close()

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        codec: FrameCodec,
        guard: SequenceReplayGuard,
        responses: OrderedDict[str, IpcFrame | None],
    ) -> int:
        quiesced = False
        while True:
            frame = await self._watch_read(reader, codec)
            disposition = guard.observe(frame)
            if disposition is ReplayDisposition.DUPLICATE:
                if frame.ipc_frame_id not in responses:
                    raise IpcSequenceError("duplicate response was evicted")
                previous = responses[frame.ipc_frame_id]
                if previous is not None:
                    await asyncio.wait_for(
                        write_frame(writer, codec, previous),
                        timeout=self.control_timeout,
                    )
                continue
            if IpcSessionIdentity.from_frame(frame) != self.identity.ipc_identity:
                raise IpcSequenceError("frame identity does not match child")
            if frame.reply_to_frame_id is not None:
                raise IpcProtocolError("control request cannot be a response")
            if frame.kind is FrameKind.PING:
                nonce = _ping_nonce(_payload(frame, {"nonce"})["nonce"])
                response = _build_child_frame(
                    codec,
                    self.identity,
                    kind=FrameKind.PONG,
                    sequence=guard.next_sequence(
                        FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL
                    ),
                    reply_to=frame.ipc_frame_id,
                    payload={"nonce": nonce},
                )
                guard.observe(response)
                responses[frame.ipc_frame_id] = response
                await asyncio.wait_for(
                    write_frame(writer, codec, response),
                    timeout=self.control_timeout,
                )
            elif frame.kind is FrameKind.QUIESCE:
                _payload(frame, set())
                quiesced = True
                responses[frame.ipc_frame_id] = None
            elif frame.kind is FrameKind.SHUTDOWN:
                _payload(frame, set())
                return 0
            else:
                raise IpcProtocolError("frame kind is outside child liveness")
            while len(responses) > self.replay_window:
                responses.popitem(last=False)


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--child", action="store_true", required=True)
    for name in ("control-fd", "data-fd", "bootstrap-fd", "lifetime-fd"):
        parser.add_argument(f"--{name}", type=int, required=True)
    for name in (
        "supervisor-epoch",
        "agent-incarnation",
        "worker-generation",
        "protocol-version",
        "max-frame-bytes",
        "replay-window",
        "parent-pid",
        "parent-start-time",
        "lifetime-device",
        "lifetime-inode",
    ):
        parser.add_argument(f"--{name}", type=_positive, required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--parent-boot-id", required=True)
    parser.add_argument("--lifetime-identity", required=True)
    parser.add_argument("--startup-timeout", type=float, required=True)
    parser.add_argument("--control-timeout", type=float, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if (
            not math.isfinite(args.startup_timeout)
            or not math.isfinite(args.control_timeout)
            or min(args.startup_timeout, args.control_timeout) <= 0
            or args.max_frame_bytes > HARD_MAX_FRAME_BYTES
            or args.replay_window > MAX_REPLAY_WINDOW
        ):
            raise ValueError("invalid child runtime bound")
        host = ChildHost(
            ChildIdentity(
                supervisor_epoch=args.supervisor_epoch,
                agent_id=args.agent_id,
                agent_incarnation=args.agent_incarnation,
                worker_generation=args.worker_generation,
                protocol_version=args.protocol_version,
            ),
            control_fd=args.control_fd,
            data_fd=args.data_fd,
            bootstrap_fd=args.bootstrap_fd,
            lifetime_fd=args.lifetime_fd,
            lifetime_device=args.lifetime_device,
            lifetime_inode=args.lifetime_inode,
            lifetime_identity=args.lifetime_identity,
            parent_pid=args.parent_pid,
            parent_start_time=args.parent_start_time,
            parent_boot_id=args.parent_boot_id,
            max_frame_bytes=args.max_frame_bytes,
            startup_timeout=args.startup_timeout,
            control_timeout=args.control_timeout,
            replay_window=args.replay_window,
        )
        return asyncio.run(host.run())
    except (Exception, KeyboardInterrupt):
        return _CHILD_PROTOCOL_EXIT


__all__ = ["ChildHost", "ChildIdentity", "main"]
