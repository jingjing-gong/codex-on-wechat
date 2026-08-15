"""Authenticated, bounded IPC framing for Agent subprocesses.

This module is deliberately dependency-free.  It defines only the wire codec
and process-local replay guard; it does not import the SQLite store, channel
adapters, or an Agent runtime.  Durable replay receipts belong to the future
supervisor/store integration and are intentionally outside this foundation.

Frames use a four-byte big-endian body length followed by one canonical UTF-8
JSON object.  The HMAC covers every top-level field except ``auth_tag``.
Steady-state frames and HELLO/CONFIG bootstrap proofs have distinct codecs and
HMAC domains, so a session-key codec can never accept a handshake frame.  The
bootstrap codec receives only a caller-derived proof key, never the sealed
bootstrap capability itself.  Decoding authenticates the envelope before
validating or returning its payload.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import re
import struct
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any


PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = 65_536
HARD_MAX_FRAME_BYTES = 1_048_576
MIN_AUTHENTICATION_KEY_BYTES = 32
_LENGTH_BYTES = 4
_MIN_JSON_INTEGER = -(1 << 63)
_MAX_JSON_INTEGER = (1 << 63) - 1
_MAX_JSON_DEPTH = 64
_MAX_IDENTIFIER_BYTES = 512
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SESSION_HMAC_DOMAIN = b"codex-wechat-ipc-session-frame-v1\x00"
_BOOTSTRAP_HMAC_DOMAIN = b"codex-wechat-ipc-bootstrap-proof-v1\x00"


class IpcProtocolError(ValueError):
    """A frame violates the authenticated v1 IPC contract."""


class IpcAuthenticationError(IpcProtocolError):
    """A frame could not be authenticated without exposing secret material."""


class IpcFrameSizeError(IpcProtocolError):
    """A frame length is empty, invalid, or above the configured hard bound."""


class IpcStreamClosedError(IpcProtocolError, EOFError):
    """The peer closed a stream before a complete frame was available."""


class IpcTruncatedFrameError(IpcStreamClosedError):
    """The peer closed a stream after sending part of a frame."""


class IpcSequenceError(IpcProtocolError):
    """A per-direction/per-stream sequence or replay invariant was violated."""


class FrameDirection(str, Enum):
    """Authenticated physical direction of one frame."""

    SUPERVISOR_TO_CHILD = "supervisor_to_child"
    CHILD_TO_SUPERVISOR = "child_to_supervisor"


class FrameStream(str, Enum):
    """The private IPC stream on which one frame is allowed."""

    CONTROL = "control"
    DATA = "data"


class FrameAuthenticationPhase(str, Enum):
    """The non-interchangeable key phase used to authenticate a frame."""

    BOOTSTRAP_PROOF = "bootstrap_proof"
    SESSION = "session"


class FrameKind(str, Enum):
    """Closed v1 message-kind vocabulary from the process design."""

    CONFIG = "CONFIG"
    ASSIGN = "ASSIGN"
    ASSIGN_BEGIN = "ASSIGN_BEGIN"
    ASSIGN_CHUNK = "ASSIGN_CHUNK"
    ASSIGN_END = "ASSIGN_END"
    RUN_GRANTED = "RUN_GRANTED"
    ASSIGN_ABORT = "ASSIGN_ABORT"
    ASSIGN_REJECTION_COMMITTED = "ASSIGN_REJECTION_COMMITTED"
    EVENT_COMMITTED = "EVENT_COMMITTED"
    EVENT_REJECTED = "EVENT_REJECTED"
    RESULT_COMMITTED = "RESULT_COMMITTED"
    THREAD_BOUND_COMMITTED = "THREAD_BOUND_COMMITTED"
    THREAD_BOUND_REJECTED = "THREAD_BOUND_REJECTED"
    CAPABILITIES_COMMITTED = "CAPABILITIES_COMMITTED"
    CAPABILITIES_REJECTED = "CAPABILITIES_REJECTED"
    SKILL_CATALOG_COMMITTED = "SKILL_CATALOG_COMMITTED"
    SKILL_CATALOG_REJECTED = "SKILL_CATALOG_REJECTED"
    ARTIFACT_COMMITTED = "ARTIFACT_COMMITTED"
    ARTIFACT_REJECTED = "ARTIFACT_REJECTED"
    COLLABORATION_COMMITTED = "COLLABORATION_COMMITTED"
    COLLABORATION_REJECTED = "COLLABORATION_REJECTED"
    CONTROL_ABORT = "CONTROL_ABORT"
    CONTROL_COMMITTED = "CONTROL_COMMITTED"
    LEASE_RENEWED = "LEASE_RENEWED"
    INTERRUPT = "INTERRUPT"
    RESET_CONVERSATION = "RESET_CONVERSATION"
    REFRESH_CAPABILITIES = "REFRESH_CAPABILITIES"
    QUIESCE = "QUIESCE"
    SHUTDOWN = "SHUTDOWN"
    PING = "PING"

    HELLO = "HELLO"
    CAPABILITIES = "CAPABILITIES"
    SKILL_CATALOG_PROPOSED = "SKILL_CATALOG_PROPOSED"
    READY = "READY"
    READY_TO_RUN = "READY_TO_RUN"
    ASSIGN_REJECTED = "ASSIGN_REJECTED"
    ASSIGN_ABORTED = "ASSIGN_ABORTED"
    HEARTBEAT = "HEARTBEAT"
    PONG = "PONG"
    THREAD_BOUND = "THREAD_BOUND"
    EVENT = "EVENT"
    EVENT_BEGIN = "EVENT_BEGIN"
    EVENT_CHUNK = "EVENT_CHUNK"
    EVENT_END = "EVENT_END"
    RESULT = "RESULT"
    INTERRUPT_ACK = "INTERRUPT_ACK"
    IDLE = "IDLE"
    ARTIFACT_PROPOSED = "ARTIFACT_PROPOSED"
    COLLABORATION_REQUEST = "COLLABORATION_REQUEST"
    CONTROL_APPLIED = "CONTROL_APPLIED"
    CONTROL_REJECTED = "CONTROL_REJECTED"
    CONTROL_ABORTED = "CONTROL_ABORTED"
    ERROR = "ERROR"


class ReplayDisposition(str, Enum):
    """Result of observing an authenticated frame in a replay guard."""

    NEW = "new"
    DUPLICATE = "duplicate"


_BOOTSTRAP_KINDS = frozenset({FrameKind.HELLO, FrameKind.CONFIG})
_REPLAY_LANES = tuple(
    (direction, stream)
    for direction in FrameDirection
    for stream in FrameStream
)


_SUPERVISOR_CONTROL_KINDS = frozenset(
    {
        FrameKind.CONFIG,
        FrameKind.RUN_GRANTED,
        FrameKind.ASSIGN_ABORT,
        FrameKind.ASSIGN_REJECTION_COMMITTED,
        FrameKind.CONTROL_ABORT,
        FrameKind.LEASE_RENEWED,
        FrameKind.INTERRUPT,
        FrameKind.QUIESCE,
        FrameKind.SHUTDOWN,
        FrameKind.PING,
    }
)
_SUPERVISOR_DATA_KINDS = frozenset(
    {
        FrameKind.ASSIGN,
        FrameKind.ASSIGN_BEGIN,
        FrameKind.ASSIGN_CHUNK,
        FrameKind.ASSIGN_END,
        FrameKind.EVENT_COMMITTED,
        FrameKind.EVENT_REJECTED,
        FrameKind.RESULT_COMMITTED,
        FrameKind.THREAD_BOUND_COMMITTED,
        FrameKind.THREAD_BOUND_REJECTED,
        FrameKind.CAPABILITIES_COMMITTED,
        FrameKind.CAPABILITIES_REJECTED,
        FrameKind.SKILL_CATALOG_COMMITTED,
        FrameKind.SKILL_CATALOG_REJECTED,
        FrameKind.ARTIFACT_COMMITTED,
        FrameKind.ARTIFACT_REJECTED,
        FrameKind.COLLABORATION_COMMITTED,
        FrameKind.COLLABORATION_REJECTED,
        FrameKind.CONTROL_COMMITTED,
        FrameKind.RESET_CONVERSATION,
        FrameKind.REFRESH_CAPABILITIES,
    }
)
_CHILD_CONTROL_KINDS = frozenset(
    {
        FrameKind.HELLO,
        FrameKind.READY,
        FrameKind.READY_TO_RUN,
        FrameKind.ASSIGN_REJECTED,
        FrameKind.ASSIGN_ABORTED,
        FrameKind.HEARTBEAT,
        FrameKind.PONG,
        FrameKind.INTERRUPT_ACK,
        FrameKind.IDLE,
        FrameKind.ERROR,
    }
)
_CHILD_DATA_KINDS = frozenset(
    {
        FrameKind.CAPABILITIES,
        FrameKind.SKILL_CATALOG_PROPOSED,
        FrameKind.THREAD_BOUND,
        FrameKind.EVENT,
        FrameKind.EVENT_BEGIN,
        FrameKind.EVENT_CHUNK,
        FrameKind.EVENT_END,
        FrameKind.RESULT,
        FrameKind.ARTIFACT_PROPOSED,
        FrameKind.COLLABORATION_REQUEST,
        FrameKind.CONTROL_APPLIED,
        FrameKind.CONTROL_REJECTED,
        FrameKind.CONTROL_ABORTED,
    }
)

_KIND_ROUTE: dict[FrameKind, tuple[FrameDirection, FrameStream]] = {}
for _kind in _SUPERVISOR_CONTROL_KINDS:
    _KIND_ROUTE[_kind] = (FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL)
for _kind in _SUPERVISOR_DATA_KINDS:
    _KIND_ROUTE[_kind] = (FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.DATA)
for _kind in _CHILD_CONTROL_KINDS:
    _KIND_ROUTE[_kind] = (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL)
for _kind in _CHILD_DATA_KINDS:
    _KIND_ROUTE[_kind] = (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.DATA)

if set(_KIND_ROUTE) != set(FrameKind):  # pragma: no cover - import-time invariant
    raise RuntimeError("IPC frame-kind route table is incomplete")


_TOP_LEVEL_KEYS = frozenset(
    {
        "protocol_version",
        "ipc_frame_id",
        "reply_to_frame_id",
        "kind",
        "direction",
        "stream",
        "supervisor_epoch",
        "agent_id",
        "agent_incarnation",
        "worker_generation",
        "stream_sequence",
        "payload",
        "payload_hash",
        "auth_tag",
    }
)


def _normalize_json(value: Any, *, depth: int = 0) -> Any:
    """Return ordinary JSON values while rejecting ambiguous extensions."""

    if depth > _MAX_JSON_DEPTH:
        raise IpcProtocolError("JSON nesting exceeds the protocol limit")
    if value is None or type(value) in {bool, str}:  # exact types: no enum coercion
        return value
    if type(value) is int:
        if value < _MIN_JSON_INTEGER or value > _MAX_JSON_INTEGER:
            raise IpcProtocolError("JSON integer is outside the protocol range")
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise IpcProtocolError("JSON number must be finite")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise IpcProtocolError("JSON object keys must be strings")
            normalized[key] = _normalize_json(item, depth=depth + 1)
        return normalized
    if type(value) in {list, tuple}:
        return [_normalize_json(item, depth=depth + 1) for item in value]
    raise IpcProtocolError("value is not permitted in canonical JSON")


def canonical_json_bytes(value: Any) -> bytes:
    """Encode one value with the canonical JSON rules used by IPC v1."""

    normalized = _normalize_json(value)
    try:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as exc:
        raise IpcProtocolError("value cannot be encoded as canonical UTF-8 JSON") from exc


def _freeze_json(value: Any) -> Any:
    """Deep-freeze a normalized value before exposing authenticated data."""

    if type(value) is dict:
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if type(value) is list:
        return tuple(_freeze_json(item) for item in value)
    return value


class _DuplicateJsonKey(ValueError):
    pass


def _strict_json_object(body: bytes) -> dict[str, Any]:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise IpcProtocolError("frame body is not valid UTF-8") from exc

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise _DuplicateJsonKey
            result[key] = value
        return result

    def invalid_constant(_value: str) -> Any:
        raise ValueError

    try:
        value = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=invalid_constant,
        )
    except (_DuplicateJsonKey, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise IpcProtocolError("frame body is not strict JSON") from exc
    if type(value) is not dict:
        raise IpcProtocolError("frame body must be a JSON object")
    try:
        canonical = canonical_json_bytes(value)
    except IpcProtocolError:
        raise
    if not hmac.compare_digest(body, canonical):
        raise IpcProtocolError("frame body is not canonically encoded")
    return value


def _enum_value(enum_type: type[Enum], value: Any, field: str) -> Any:
    if type(value) is not str:
        raise IpcProtocolError(f"{field} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise IpcProtocolError(f"unknown {field}") from exc


def _positive_integer(value: Any, field: str) -> int:
    if type(value) is not int or not 0 < value <= _MAX_JSON_INTEGER:
        raise IpcProtocolError(f"{field} must be a positive integer")
    return value


def _sequence_integer(value: Any) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_JSON_INTEGER:
        raise IpcProtocolError("stream_sequence must be a nonnegative integer")
    return value


def _identifier(value: Any, field: str, *, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if type(value) is not str or not value or value != value.strip():
        raise IpcProtocolError(f"{field} must be a nonempty canonical string")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise IpcProtocolError(f"{field} is not valid Unicode") from exc
    if len(encoded) > _MAX_IDENTIFIER_BYTES:
        raise IpcProtocolError(f"{field} exceeds the protocol limit")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise IpcProtocolError(f"{field} contains a control character")
    return value


def _validate_route(
    kind: FrameKind,
    direction: FrameDirection,
    stream: FrameStream,
) -> None:
    if _KIND_ROUTE[kind] != (direction, stream):
        raise IpcProtocolError("frame kind is not allowed on this direction and stream")


def _payload_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _unsigned_wire(wire: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in wire.items() if key != "auth_tag"}


def _authentication_tag(
    authentication_key: bytes | bytearray | memoryview,
    wire: Mapping[str, Any],
    *,
    domain: bytes,
) -> str:
    authenticated = canonical_json_bytes(_unsigned_wire(wire))
    return hmac.new(
        authentication_key,
        domain + authenticated,
        hashlib.sha256,
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class IpcFrame:
    """One fully authenticated v1 frame, without its length prefix."""

    protocol_version: int
    ipc_frame_id: str
    reply_to_frame_id: str | None
    kind: FrameKind
    direction: FrameDirection
    stream: FrameStream
    supervisor_epoch: int
    agent_id: str
    agent_incarnation: int
    worker_generation: int
    stream_sequence: int
    payload: Mapping[str, Any]
    payload_hash: str
    auth_tag: str

    def as_dict(self) -> dict[str, Any]:
        """Return a detached application-value representation of the frame."""

        return {
            "protocol_version": self.protocol_version,
            "ipc_frame_id": self.ipc_frame_id,
            "reply_to_frame_id": self.reply_to_frame_id,
            "kind": self.kind.value,
            "direction": self.direction.value,
            "stream": self.stream.value,
            "supervisor_epoch": self.supervisor_epoch,
            "agent_id": self.agent_id,
            "agent_incarnation": self.agent_incarnation,
            "worker_generation": self.worker_generation,
            "stream_sequence": self.stream_sequence,
            "payload": _normalize_json(self.payload),
            "payload_hash": self.payload_hash,
            "auth_tag": self.auth_tag,
        }


@dataclass(frozen=True, slots=True)
class IpcSessionIdentity:
    """The exact generation identity to which replay state is pinned."""

    supervisor_epoch: int
    agent_id: str
    agent_incarnation: int
    worker_generation: int

    def __post_init__(self) -> None:
        _positive_integer(self.supervisor_epoch, "supervisor_epoch")
        _identifier(self.agent_id, "agent_id")
        _positive_integer(self.agent_incarnation, "agent_incarnation")
        _positive_integer(self.worker_generation, "worker_generation")

    @classmethod
    def from_frame(cls, frame: IpcFrame) -> IpcSessionIdentity:
        if not isinstance(frame, IpcFrame):
            raise TypeError("frame must be an authenticated IpcFrame")
        return cls(
            supervisor_epoch=frame.supervisor_epoch,
            agent_id=frame.agent_id,
            agent_incarnation=frame.agent_incarnation,
            worker_generation=frame.worker_generation,
        )


class FrameCodec:
    """Authenticate post-handshake frames with a derived session key.

    ``HELLO`` and ``CONFIG`` are deliberately rejected even if their envelope
    has a valid MAC under this codec's key.  Use :class:`BootstrapFrameCodec`
    with a separately derived bootstrap-proof key for those two kinds.
    """

    authentication_phase = FrameAuthenticationPhase.SESSION
    _hmac_domain = _SESSION_HMAC_DOMAIN

    __slots__ = (
        "__authentication_key",
        "__closed",
        "max_frame_bytes",
        "expected_direction",
        "expected_stream",
    )

    def __init__(
        self,
        authentication_key: bytes | bytearray | memoryview,
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        expected_direction: FrameDirection | str | None = None,
        expected_stream: FrameStream | str | None = None,
    ) -> None:
        if not isinstance(authentication_key, (bytes, bytearray, memoryview)):
            raise TypeError("authentication key must be bytes")
        key = bytearray(authentication_key)
        if len(key) < MIN_AUTHENTICATION_KEY_BYTES:
            raise ValueError("authentication key is too short")
        if (
            type(max_frame_bytes) is not int
            or not 0 < max_frame_bytes <= HARD_MAX_FRAME_BYTES
        ):
            raise ValueError("max_frame_bytes exceeds the hard protocol bound")
        self.__authentication_key = key
        self.__closed = False
        self.max_frame_bytes = max_frame_bytes
        self.expected_direction = (
            FrameDirection(expected_direction) if expected_direction is not None else None
        )
        self.expected_stream = (
            FrameStream(expected_stream) if expected_stream is not None else None
        )

    def close(self) -> None:
        """Erase this process-local copy of the authentication key."""

        if self.__closed:
            return
        self.__authentication_key[:] = b"\x00" * len(self.__authentication_key)
        self.__closed = True

    def _require_open(self) -> None:
        if self.__closed:
            raise IpcAuthenticationError("frame codec is closed")

    def build_frame(
        self,
        *,
        kind: FrameKind | str,
        direction: FrameDirection | str,
        stream: FrameStream | str,
        supervisor_epoch: int,
        agent_id: str,
        agent_incarnation: int,
        worker_generation: int,
        ipc_frame_id: str,
        stream_sequence: int,
        payload: Mapping[str, Any],
        reply_to_frame_id: str | None = None,
        protocol_version: int = PROTOCOL_VERSION,
    ) -> IpcFrame:
        """Create one signed frame from application values."""

        self._require_open()
        if type(payload) is not dict and not isinstance(payload, Mapping):
            raise IpcProtocolError("payload must be a JSON object")
        normalized_payload = _normalize_json(payload)
        if type(normalized_payload) is not dict:  # defensive; root is a Mapping
            raise IpcProtocolError("payload must be a JSON object")
        wire: dict[str, Any] = {
            "protocol_version": protocol_version,
            "ipc_frame_id": ipc_frame_id,
            "reply_to_frame_id": reply_to_frame_id,
            "kind": getattr(kind, "value", kind),
            "direction": getattr(direction, "value", direction),
            "stream": getattr(stream, "value", stream),
            "supervisor_epoch": supervisor_epoch,
            "agent_id": agent_id,
            "agent_incarnation": agent_incarnation,
            "worker_generation": worker_generation,
            "stream_sequence": stream_sequence,
            "payload": normalized_payload,
            "payload_hash": _payload_digest(normalized_payload),
        }
        wire["auth_tag"] = _authentication_tag(
            self.__authentication_key,
            wire,
            domain=self._hmac_domain,
        )
        frame = self._authenticated_frame(wire)
        body = canonical_json_bytes(frame.as_dict())
        self._check_body_length(len(body))
        return frame

    def encode_body(self, frame: IpcFrame) -> bytes:
        """Return the canonical authenticated body for ``frame``."""

        self._require_open()
        if not isinstance(frame, IpcFrame):
            raise TypeError("frame must be an IpcFrame")
        wire = frame.as_dict()
        expected_tag = _authentication_tag(
            self.__authentication_key,
            wire,
            domain=self._hmac_domain,
        )
        if not hmac.compare_digest(frame.auth_tag, expected_tag):
            raise IpcAuthenticationError("frame authentication failed")
        expected_payload_hash = _payload_digest(wire["payload"])
        if not hmac.compare_digest(frame.payload_hash, expected_payload_hash):
            raise IpcAuthenticationError("frame payload authentication failed")
        # Re-run all semantic checks so a manually constructed dataclass cannot
        # bypass the same contract applied to a decoded frame.
        self._authenticated_frame(wire)
        body = canonical_json_bytes(wire)
        self._check_body_length(len(body))
        return body

    def encode(self, frame: IpcFrame) -> bytes:
        """Return a length-prefixed wire frame."""

        body = self.encode_body(frame)
        return struct.pack(">I", len(body)) + body

    def decode_body(
        self,
        body: bytes | bytearray | memoryview,
        *,
        expected_direction: FrameDirection | str | None = None,
        expected_stream: FrameStream | str | None = None,
    ) -> IpcFrame:
        """Authenticate and decode one body whose prefix was already consumed."""

        self._require_open()
        if not isinstance(body, (bytes, bytearray, memoryview)):
            raise TypeError("frame body must be bytes")
        raw = bytes(body)
        self._check_body_length(len(raw))
        wire = _strict_json_object(raw)
        keys = frozenset(wire)
        if keys != _TOP_LEVEL_KEYS:
            if _TOP_LEVEL_KEYS - keys:
                raise IpcProtocolError("frame is missing required top-level fields")
            raise IpcProtocolError("frame contains unknown top-level fields")

        # Only the tag's representation is inspected before authentication.
        # Payload hashes, identities, routing, and payload contents are not
        # trusted or returned until compare_digest succeeds.
        supplied_tag = wire.get("auth_tag")
        if type(supplied_tag) is not str or not _HASH_PATTERN.fullmatch(supplied_tag):
            raise IpcAuthenticationError("frame authentication failed")
        expected_tag = _authentication_tag(
            self.__authentication_key,
            wire,
            domain=self._hmac_domain,
        )
        if not hmac.compare_digest(supplied_tag, expected_tag):
            raise IpcAuthenticationError("frame authentication failed")

        frame = self._authenticated_frame(wire)
        required_direction = (
            FrameDirection(expected_direction)
            if expected_direction is not None
            else self.expected_direction
        )
        required_stream = (
            FrameStream(expected_stream)
            if expected_stream is not None
            else self.expected_stream
        )
        if required_direction is not None and frame.direction is not required_direction:
            raise IpcProtocolError("frame arrived on the wrong direction")
        if required_stream is not None and frame.stream is not required_stream:
            raise IpcProtocolError("frame arrived on the wrong stream")
        return frame

    def decode(
        self,
        packet: bytes | bytearray | memoryview,
        *,
        expected_direction: FrameDirection | str | None = None,
        expected_stream: FrameStream | str | None = None,
    ) -> IpcFrame:
        """Decode exactly one complete length-prefixed packet."""

        if not isinstance(packet, (bytes, bytearray, memoryview)):
            raise TypeError("frame packet must be bytes")
        raw = bytes(packet)
        if len(raw) < _LENGTH_BYTES:
            raise IpcTruncatedFrameError("truncated IPC frame prefix")
        (length,) = struct.unpack(">I", raw[:_LENGTH_BYTES])
        self._check_body_length(length)
        if len(raw) != _LENGTH_BYTES + length:
            raise IpcProtocolError("packet does not contain exactly one IPC frame")
        return self.decode_body(
            raw[_LENGTH_BYTES:],
            expected_direction=expected_direction,
            expected_stream=expected_stream,
        )

    def _authenticated_frame(self, wire: Mapping[str, Any]) -> IpcFrame:
        """Validate an already-MACed mapping and construct its typed frame."""

        if frozenset(wire) != _TOP_LEVEL_KEYS:
            raise IpcProtocolError("frame top-level fields do not match IPC v1")
        if type(wire["protocol_version"]) is not int or wire["protocol_version"] != PROTOCOL_VERSION:
            raise IpcProtocolError("unsupported IPC protocol version")
        kind = _enum_value(FrameKind, wire["kind"], "frame kind")
        self._validate_authentication_phase(kind)
        direction = _enum_value(FrameDirection, wire["direction"], "frame direction")
        stream = _enum_value(FrameStream, wire["stream"], "frame stream")
        _validate_route(kind, direction, stream)
        payload = wire["payload"]
        if type(payload) is not dict:
            raise IpcProtocolError("payload must be a JSON object")
        normalized_payload = _normalize_json(payload)
        payload_hash = wire["payload_hash"]
        if type(payload_hash) is not str or not _HASH_PATTERN.fullmatch(payload_hash):
            raise IpcAuthenticationError("frame payload authentication failed")
        expected_payload_hash = _payload_digest(normalized_payload)
        if not hmac.compare_digest(payload_hash, expected_payload_hash):
            raise IpcAuthenticationError("frame payload authentication failed")
        auth_tag = wire["auth_tag"]
        if type(auth_tag) is not str or not _HASH_PATTERN.fullmatch(auth_tag):
            raise IpcAuthenticationError("frame authentication failed")
        return IpcFrame(
            protocol_version=PROTOCOL_VERSION,
            ipc_frame_id=_identifier(wire["ipc_frame_id"], "ipc_frame_id"),  # type: ignore[arg-type]
            reply_to_frame_id=_identifier(
                wire["reply_to_frame_id"],
                "reply_to_frame_id",
                nullable=True,
            ),
            kind=kind,
            direction=direction,
            stream=stream,
            supervisor_epoch=_positive_integer(
                wire["supervisor_epoch"], "supervisor_epoch"
            ),
            agent_id=_identifier(wire["agent_id"], "agent_id"),  # type: ignore[arg-type]
            agent_incarnation=_positive_integer(
                wire["agent_incarnation"], "agent_incarnation"
            ),
            worker_generation=_positive_integer(
                wire["worker_generation"], "worker_generation"
            ),
            stream_sequence=_sequence_integer(wire["stream_sequence"]),
            payload=_freeze_json(normalized_payload),
            payload_hash=payload_hash,
            auth_tag=auth_tag,
        )

    def _validate_authentication_phase(self, kind: FrameKind) -> None:
        if kind in _BOOTSTRAP_KINDS:
            raise IpcProtocolError(
                "handshake frame kind requires bootstrap-proof authentication"
            )

    def _check_body_length(self, length: int) -> None:
        if type(length) is not int or length <= 0:
            raise IpcFrameSizeError("IPC frame length must be nonzero")
        if length > self.max_frame_bytes:
            raise IpcFrameSizeError("IPC frame exceeds the configured limit")


class BootstrapFrameCodec(FrameCodec):
    """Authenticate only HELLO/CONFIG using a derived bootstrap-proof key.

    The injected key must be derived by the handshake layer from the sealed
    generation bootstrap capability.  Keeping the raw capability outside this
    codec prevents it from becoming a general steady-state MAC key.
    """

    __slots__ = ()

    authentication_phase = FrameAuthenticationPhase.BOOTSTRAP_PROOF
    _hmac_domain = _BOOTSTRAP_HMAC_DOMAIN

    def _validate_authentication_phase(self, kind: FrameKind) -> None:
        if kind not in _BOOTSTRAP_KINDS:
            raise IpcProtocolError(
                "post-handshake frame kind requires session authentication"
            )


@dataclass(slots=True)
class _ReplayLane:
    next_sequence: int
    entries: OrderedDict[int, tuple[str, str, bytes]]


class SequenceReplayGuard:
    """Bounded process-local replay detection for authenticated frames.

    Callers must first decode and authenticate a frame, then observe it here.
    Each instance is pinned to one exact supervisor epoch and Agent
    incarnation/generation; identity drift is rejected before sequence state
    can advance.  Exact duplicates inside the replay window are idempotent; a
    gap, conflicting sequence reuse, retained frame-ID reuse, or replay older
    than the window is rejected.
    """

    def __init__(
        self,
        session_identity: IpcSessionIdentity,
        *,
        initial_sequence: int = 0,
        initial_sequences: Mapping[
            tuple[FrameDirection | str, FrameStream | str], int
        ]
        | None = None,
        replay_window: int = 256,
    ) -> None:
        if not isinstance(session_identity, IpcSessionIdentity):
            raise TypeError("session_identity must be an IpcSessionIdentity")
        if (
            type(initial_sequence) is not int
            or not 0 <= initial_sequence <= _MAX_JSON_INTEGER
        ):
            raise ValueError("initial_sequence must be nonnegative")
        lane_initial_sequences = {
            lane: initial_sequence for lane in _REPLAY_LANES
        }
        if initial_sequences is not None:
            if not isinstance(initial_sequences, Mapping):
                raise TypeError("initial_sequences must be a lane mapping")
            normalized_lanes: set[tuple[FrameDirection, FrameStream]] = set()
            for raw_lane, value in initial_sequences.items():
                if type(raw_lane) is not tuple or len(raw_lane) != 2:
                    raise ValueError(
                        "initial_sequences keys must be direction/stream pairs"
                    )
                try:
                    lane = (
                        FrameDirection(raw_lane[0]),
                        FrameStream(raw_lane[1]),
                    )
                except (TypeError, ValueError):
                    raise ValueError(
                        "initial_sequences contains an unknown replay lane"
                    ) from None
                if lane in normalized_lanes:
                    raise ValueError(
                        "initial_sequences contains a duplicate replay lane"
                    )
                if (
                    type(value) is not int
                    or not 0 <= value <= _MAX_JSON_INTEGER
                ):
                    raise ValueError(
                        "initial_sequences values must be nonnegative integers"
                    )
                normalized_lanes.add(lane)
                lane_initial_sequences[lane] = value
        if type(replay_window) is not int or replay_window <= 0:
            raise ValueError("replay_window must be positive")
        self.session_identity = session_identity
        # Retain the scalar as the compatibility fallback while exposing the
        # normalized complete four-lane starting state to new callers.
        self.initial_sequence = initial_sequence
        self.initial_sequences = MappingProxyType(lane_initial_sequences)
        self.replay_window = replay_window
        self._lanes: dict[tuple[FrameDirection, FrameStream], _ReplayLane] = {}

    def observe(self, frame: IpcFrame) -> ReplayDisposition:
        if not isinstance(frame, IpcFrame):
            raise TypeError("frame must be an authenticated IpcFrame")
        if IpcSessionIdentity.from_frame(frame) != self.session_identity:
            raise IpcSequenceError("frame identity does not match replay session")
        lane_key = (frame.direction, frame.stream)
        lane = self._lanes.setdefault(
            lane_key,
            _ReplayLane(self.initial_sequences[lane_key], OrderedDict()),
        )
        sequence = frame.stream_sequence
        fingerprint = hashlib.sha256(canonical_json_bytes(frame.as_dict())).digest()
        existing = lane.entries.get(sequence)
        if sequence < lane.next_sequence:
            if existing is None:
                raise IpcSequenceError("frame replay is outside the replay window")
            frame_id, payload_hash, previous_fingerprint = existing
            if (
                frame_id == frame.ipc_frame_id
                and hmac.compare_digest(payload_hash, frame.payload_hash)
                and hmac.compare_digest(previous_fingerprint, fingerprint)
            ):
                return ReplayDisposition.DUPLICATE
            raise IpcSequenceError("frame sequence was reused with different content")
        if sequence > lane.next_sequence:
            raise IpcSequenceError("frame sequence contains a gap")
        if any(
            cached_frame_id == frame.ipc_frame_id
            for cached_frame_id, _payload_hash, _fingerprint in lane.entries.values()
        ):
            raise IpcSequenceError("ipc_frame_id was reused at a new sequence")
        lane.entries[sequence] = (
            frame.ipc_frame_id,
            frame.payload_hash,
            fingerprint,
        )
        lane.next_sequence += 1
        while len(lane.entries) > self.replay_window:
            lane.entries.popitem(last=False)
        return ReplayDisposition.NEW

    def next_sequence(
        self,
        direction: FrameDirection | str,
        stream: FrameStream | str,
    ) -> int:
        lane_key = (FrameDirection(direction), FrameStream(stream))
        lane = self._lanes.get(lane_key)
        return (
            lane.next_sequence
            if lane is not None
            else self.initial_sequences[lane_key]
        )

    @property
    def replay_size(self) -> int:
        """Return retained entries across the four fixed direction/stream lanes."""

        return sum(len(lane.entries) for lane in self._lanes.values())


async def read_frame(
    reader: asyncio.StreamReader,
    codec: FrameCodec,
    *,
    expected_direction: FrameDirection | str | None = None,
    expected_stream: FrameStream | str | None = None,
) -> IpcFrame:
    """Read and authenticate one frame without allocating above its hard cap."""

    try:
        prefix = await reader.readexactly(_LENGTH_BYTES)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise IpcStreamClosedError("IPC stream closed") from None
        raise IpcTruncatedFrameError("truncated IPC frame prefix") from None
    (length,) = struct.unpack(">I", prefix)
    codec._check_body_length(length)
    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        raise IpcTruncatedFrameError("truncated IPC frame body") from None
    return codec.decode_body(
        body,
        expected_direction=expected_direction,
        expected_stream=expected_stream,
    )


async def write_frame(writer: asyncio.StreamWriter, codec: FrameCodec, frame: IpcFrame) -> None:
    """Write one complete authenticated frame and honor stream backpressure."""

    packet = codec.encode(frame)
    writer.write(packet)
    await writer.drain()


# More explicit aliases for callers that use the plan's IPC terminology.
IpcDirection = FrameDirection
IpcStream = FrameStream
IpcFrameKind = FrameKind


__all__ = [
    "BootstrapFrameCodec",
    "DEFAULT_MAX_FRAME_BYTES",
    "FrameAuthenticationPhase",
    "FrameCodec",
    "FrameDirection",
    "FrameKind",
    "FrameStream",
    "HARD_MAX_FRAME_BYTES",
    "IpcAuthenticationError",
    "IpcDirection",
    "IpcFrame",
    "IpcFrameKind",
    "IpcFrameSizeError",
    "IpcProtocolError",
    "IpcSequenceError",
    "IpcSessionIdentity",
    "IpcStream",
    "IpcStreamClosedError",
    "IpcTruncatedFrameError",
    "MIN_AUTHENTICATION_KEY_BYTES",
    "PROTOCOL_VERSION",
    "ReplayDisposition",
    "SequenceReplayGuard",
    "canonical_json_bytes",
    "read_frame",
    "write_frame",
]
