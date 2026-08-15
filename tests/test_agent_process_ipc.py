"""Security and framing tests for the process-local Agent IPC foundation."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import hmac
import json
import struct
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import src.runtime.ipc as ipc_module
from src.runtime.ipc import (
    BootstrapFrameCodec,
    FrameCodec,
    FrameAuthenticationPhase,
    FrameDirection,
    FrameKind,
    FrameStream,
    HARD_MAX_FRAME_BYTES,
    IpcAuthenticationError,
    IpcFrame,
    IpcFrameSizeError,
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


SESSION_KEY = b"session-key-a" + (b"x" * 32)
OTHER_SESSION_KEY = b"session-key-b" + (b"y" * 32)
BOOTSTRAP_PROOF_KEY = b"bootstrap-proof-key" + (b"b" * 32)
SESSION_HMAC_DOMAIN = b"codex-wechat-ipc-session-frame-v1\x00"
BOOTSTRAP_HMAC_DOMAIN = b"codex-wechat-ipc-bootstrap-proof-v1\x00"


def _codec(**kwargs: Any) -> FrameCodec:
    return FrameCodec(SESSION_KEY, **kwargs)


def _session_identity(**overrides: Any) -> IpcSessionIdentity:
    values = {
        "supervisor_epoch": 7,
        "agent_id": "agent-alpha",
        "agent_incarnation": 3,
        "worker_generation": 11,
    }
    values.update(overrides)
    return IpcSessionIdentity(**values)


def _frame(
    codec: FrameCodec,
    *,
    kind: FrameKind = FrameKind.PING,
    direction: FrameDirection = FrameDirection.SUPERVISOR_TO_CHILD,
    stream: FrameStream = FrameStream.CONTROL,
    sequence: int = 0,
    frame_id: str = "frame-0",
    payload: dict[str, Any] | None = None,
    reply_to: str | None = None,
    supervisor_epoch: int = 7,
    agent_id: str = "agent-alpha",
    agent_incarnation: int = 3,
    worker_generation: int = 11,
) -> IpcFrame:
    return codec.build_frame(
        kind=kind,
        direction=direction,
        stream=stream,
        supervisor_epoch=supervisor_epoch,
        agent_id=agent_id,
        agent_incarnation=agent_incarnation,
        worker_generation=worker_generation,
        ipc_frame_id=frame_id,
        reply_to_frame_id=reply_to,
        stream_sequence=sequence,
        payload=(
            {
                "nonce": "snowman ☃",
                "nested": {"values": [True, None, 3]},
            }
            if payload is None
            else payload
        ),
    )


def _signed_body(
    wire: dict[str, Any],
    key: bytes = SESSION_KEY,
    *,
    domain: bytes = SESSION_HMAC_DOMAIN,
) -> bytes:
    """Sign a possibly-invalid envelope so semantic checks run after the MAC."""

    signed = dict(wire)
    signed.pop("auth_tag", None)
    authenticated = canonical_json_bytes(signed)
    signed["auth_tag"] = hmac.new(
        key,
        domain + authenticated,
        hashlib.sha256,
    ).hexdigest()
    return canonical_json_bytes(signed)


def _packet(body: bytes) -> bytes:
    return struct.pack(">I", len(body)) + body


def test_round_trip_is_canonical_authenticated_and_length_prefixed() -> None:
    codec = _codec(
        expected_direction=FrameDirection.SUPERVISOR_TO_CHILD,
        expected_stream=FrameStream.CONTROL,
    )
    source_payload = {
        "nonce": "snowman ☃",
        "nested": {"values": [True, None, 3]},
    }
    frame = _frame(codec, payload=source_payload)
    source_payload["nonce"] = "mutated after build"

    body = codec.encode_body(frame)
    packet = codec.encode(frame)

    assert body == canonical_json_bytes(frame.as_dict())
    assert body.startswith(b'{"agent_id":')
    assert "☃".encode() in body
    assert b"\\u2603" not in body
    assert struct.unpack(">I", packet[:4]) == (len(body),)
    assert packet[4:] == body
    assert frame.payload["nonce"] == "snowman ☃"
    assert codec.decode(packet) == frame
    assert codec.decode_body(body) == frame


def test_payload_hash_and_hmac_cover_the_canonical_envelope() -> None:
    codec = _codec()
    frame = _frame(codec)
    wire = frame.as_dict()
    unsigned = dict(wire)
    supplied_tag = unsigned.pop("auth_tag")

    assert wire["payload_hash"] == hashlib.sha256(
        canonical_json_bytes(wire["payload"])
    ).hexdigest()
    assert supplied_tag == hmac.new(
        SESSION_KEY,
        SESSION_HMAC_DOMAIN + canonical_json_bytes(unsigned),
        hashlib.sha256,
    ).hexdigest()


def test_bootstrap_proofs_and_session_frames_use_disjoint_codecs_and_domains() -> None:
    session_codec = _codec()
    bootstrap_codec = BootstrapFrameCodec(BOOTSTRAP_PROOF_KEY)
    assert session_codec.authentication_phase is FrameAuthenticationPhase.SESSION
    assert (
        bootstrap_codec.authentication_phase
        is FrameAuthenticationPhase.BOOTSTRAP_PROOF
    )

    hello = _frame(
        bootstrap_codec,
        kind=FrameKind.HELLO,
        direction=FrameDirection.CHILD_TO_SUPERVISOR,
        stream=FrameStream.CONTROL,
        frame_id="bootstrap-hello",
    )
    config = _frame(
        bootstrap_codec,
        kind=FrameKind.CONFIG,
        direction=FrameDirection.SUPERVISOR_TO_CHILD,
        stream=FrameStream.CONTROL,
        frame_id="bootstrap-config",
    )
    assert bootstrap_codec.decode(bootstrap_codec.encode(hello)) == hello
    assert bootstrap_codec.decode(bootstrap_codec.encode(config)) == config

    for kind, direction in (
        (FrameKind.HELLO, FrameDirection.CHILD_TO_SUPERVISOR),
        (FrameKind.CONFIG, FrameDirection.SUPERVISOR_TO_CHILD),
    ):
        with pytest.raises(IpcProtocolError, match="requires bootstrap-proof"):
            _frame(
                session_codec,
                kind=kind,
                direction=direction,
                stream=FrameStream.CONTROL,
                frame_id=f"session-{kind.value.lower()}",
            )

    with pytest.raises(IpcProtocolError, match="requires session authentication"):
        _frame(bootstrap_codec, frame_id="bootstrap-ping")

    # The domains fail closed before semantic validation on ordinary cross-use.
    with pytest.raises(IpcAuthenticationError):
        session_codec.decode(bootstrap_codec.encode(hello))
    session_ping = _frame(session_codec, frame_id="session-ping")
    with pytest.raises(IpcAuthenticationError):
        bootstrap_codec.decode(session_codec.encode(session_ping))

    # Domain separation still prevents cross-phase verification if a caller
    # accidentally supplies the same key bytes to both codec classes.
    same_key_bootstrap = BootstrapFrameCodec(SESSION_KEY)
    same_key_hello = _frame(
        same_key_bootstrap,
        kind=FrameKind.HELLO,
        direction=FrameDirection.CHILD_TO_SUPERVISOR,
        stream=FrameStream.CONTROL,
        frame_id="same-key-hello",
    )
    with pytest.raises(IpcAuthenticationError):
        session_codec.decode(same_key_bootstrap.encode(same_key_hello))
    with pytest.raises(IpcAuthenticationError):
        same_key_bootstrap.decode(session_codec.encode(session_ping))

    # Even a handshake envelope deliberately MACed with the actual session key
    # is rejected after successful MAC verification by the session codec.
    hello_with_session_mac = _signed_body(
        hello.as_dict(),
        SESSION_KEY,
        domain=SESSION_HMAC_DOMAIN,
    )
    with pytest.raises(IpcProtocolError, match="requires bootstrap-proof"):
        session_codec.decode_body(hello_with_session_mac)

    # The inverse is also forbidden: bootstrap proof framing is handshake-only.
    ping_with_bootstrap_proof = _signed_body(
        session_ping.as_dict(),
        BOOTSTRAP_PROOF_KEY,
        domain=BOOTSTRAP_HMAC_DOMAIN,
    )
    with pytest.raises(IpcProtocolError, match="requires session authentication"):
        bootstrap_codec.decode_body(ping_with_bootstrap_proof)


def test_authentication_key_is_copied_and_must_be_secret_sized_bytes() -> None:
    mutable_key = bytearray(SESSION_KEY)
    codec = FrameCodec(mutable_key)
    frame = _frame(codec)
    mutable_key[:] = b"z" * len(mutable_key)

    assert codec.decode(codec.encode(frame)) == frame
    with pytest.raises(TypeError, match="authentication key must be bytes"):
        FrameCodec("not-bytes")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="authentication key is too short"):
        FrameCodec(b"short")


def test_codec_close_erases_its_owned_key_and_fails_closed() -> None:
    codec = FrameCodec(bytearray(SESSION_KEY))
    frame = _frame(codec)
    codec.close()
    codec.close()

    assert codec._FrameCodec__authentication_key == bytearray(len(SESSION_KEY))
    with pytest.raises(IpcAuthenticationError, match="codec is closed"):
        codec.encode(frame)
    with pytest.raises(IpcAuthenticationError, match="codec is closed"):
        codec.decode_body(b"{}")


def test_wrong_key_tampering_and_validly_signed_bad_payload_hash_fail_closed() -> None:
    codec = _codec()
    frame = _frame(codec)
    packet = codec.encode(frame)

    with pytest.raises(IpcAuthenticationError, match="authentication failed"):
        FrameCodec(OTHER_SESSION_KEY).decode(packet)

    wire = frame.as_dict()
    wire["payload"]["nonce"] = "tampered"
    with pytest.raises(IpcAuthenticationError, match="authentication failed"):
        codec.decode(_packet(canonical_json_bytes(wire)))

    wire = frame.as_dict()
    wire["auth_tag"] = "0" * 64
    with pytest.raises(IpcAuthenticationError, match="authentication failed"):
        codec.decode(_packet(canonical_json_bytes(wire)))

    wire = frame.as_dict()
    wire["payload_hash"] = "f" * 64
    with pytest.raises(
        IpcAuthenticationError,
        match="payload authentication failed",
    ):
        codec.decode(_packet(_signed_body(wire)))


def test_authentication_errors_do_not_echo_key_tag_or_payload_secrets() -> None:
    codec = _codec()
    wire = _frame(codec, payload={"secret": "payload-secret-value"}).as_dict()
    supplied_tag = "a" * 64
    wire["auth_tag"] = supplied_tag

    with pytest.raises(IpcAuthenticationError) as captured:
        codec.decode(_packet(canonical_json_bytes(wire)))

    rendered = str(captured.value)
    assert SESSION_KEY.decode() not in rendered
    assert supplied_tag not in rendered
    assert "payload-secret-value" not in rendered


@pytest.mark.parametrize(
    "body",
    [
        b"\xff",
        b"{",
        b"[]",
        b'{"duplicate":1,"duplicate":1}',
        b'{"constant":NaN}',
        b'{"value":1} trailing',
    ],
    ids=[
        "invalid-utf8",
        "invalid-json",
        "non-object-root",
        "duplicate-key",
        "non-finite-number",
        "trailing-json",
    ],
)
def test_strict_json_rejects_ambiguous_or_malformed_bodies(body: bytes) -> None:
    with pytest.raises(IpcProtocolError):
        _codec().decode_body(body)


def test_noncanonical_whitespace_key_order_and_unicode_escape_are_rejected() -> None:
    codec = _codec()
    frame = _frame(codec)
    body = codec.encode_body(frame)

    whitespace = body[:-1] + b" }"
    insertion_order = json.dumps(
        frame.as_dict(),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    escaped_unicode = body.replace("☃".encode(), b"\\u2603")

    for noncanonical in (whitespace, insertion_order, escaped_unicode):
        with pytest.raises(IpcProtocolError, match="not canonically encoded"):
            codec.decode_body(noncanonical)


@pytest.mark.parametrize("remove", ["payload", "kind", "auth_tag"])
def test_missing_top_level_fields_are_rejected(remove: str) -> None:
    wire = _frame(_codec()).as_dict()
    wire.pop(remove)

    with pytest.raises(IpcProtocolError, match="missing required top-level"):
        _codec().decode_body(canonical_json_bytes(wire))


def test_unknown_top_level_fields_are_rejected() -> None:
    wire = _frame(_codec()).as_dict()
    wire["surprise"] = "not-negotiated"

    with pytest.raises(IpcProtocolError, match="unknown top-level"):
        _codec().decode_body(canonical_json_bytes(wire))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("protocol_version", 2, "unsupported IPC protocol version"),
        ("protocol_version", True, "unsupported IPC protocol version"),
        ("kind", "NOT_A_KIND", "unknown frame kind"),
        ("kind", 1, "frame kind must be a string"),
        ("direction", "sideways", "unknown frame direction"),
        ("stream", "bulk", "unknown frame stream"),
        ("supervisor_epoch", 0, "must be a positive integer"),
        ("supervisor_epoch", True, "must be a positive integer"),
        ("agent_incarnation", -1, "must be a positive integer"),
        ("worker_generation", 0, "must be a positive integer"),
        ("stream_sequence", -1, "must be a nonnegative integer"),
        ("stream_sequence", False, "must be a nonnegative integer"),
        ("ipc_frame_id", "", "must be a nonempty canonical string"),
        ("ipc_frame_id", " padded ", "must be a nonempty canonical string"),
        ("agent_id", "bad\nagent", "contains a control character"),
        ("reply_to_frame_id", 3, "must be a nonempty canonical string"),
        ("payload", [], "payload must be a JSON object"),
    ],
)
def test_authenticated_semantically_invalid_fields_are_rejected(
    field: str,
    value: Any,
    message: str,
) -> None:
    codec = _codec()
    wire = _frame(codec).as_dict()
    wire[field] = value
    if field == "payload":
        wire["payload_hash"] = hashlib.sha256(canonical_json_bytes(value)).hexdigest()

    with pytest.raises(IpcProtocolError, match=message):
        codec.decode_body(_signed_body(wire))


def test_identifier_and_json_value_bounds_are_enforced() -> None:
    codec = _codec()
    wire = _frame(codec).as_dict()
    wire["agent_id"] = "a" * 513
    with pytest.raises(IpcProtocolError, match="exceeds the protocol limit"):
        codec.decode_body(_signed_body(wire))

    for invalid_payload in (
        {1: "non-string-key"},
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": 1 << 80},
        {"value": object()},
        {"value": "\ud800"},
    ):
        with pytest.raises(IpcProtocolError):
            codec.build_frame(
                kind=FrameKind.PING,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                stream=FrameStream.CONTROL,
                supervisor_epoch=1,
                agent_id="agent",
                agent_incarnation=1,
                worker_generation=1,
                ipc_frame_id="bounded-value",
                stream_sequence=0,
                payload=invalid_payload,  # type: ignore[arg-type]
            )

    nested: dict[str, Any] = {}
    cursor = nested
    for _ in range(70):
        child: dict[str, Any] = {}
        cursor["child"] = child
        cursor = child
    with pytest.raises(IpcProtocolError, match="nesting exceeds"):
        canonical_json_bytes(nested)


def test_kind_direction_and_stream_route_is_closed_and_enforced() -> None:
    expected_kinds = {
        "CONFIG",
        "ASSIGN",
        "ASSIGN_BEGIN",
        "ASSIGN_CHUNK",
        "ASSIGN_END",
        "RUN_GRANTED",
        "ASSIGN_ABORT",
        "ASSIGN_REJECTION_COMMITTED",
        "EVENT_COMMITTED",
        "EVENT_REJECTED",
        "RESULT_COMMITTED",
        "THREAD_BOUND_COMMITTED",
        "THREAD_BOUND_REJECTED",
        "CAPABILITIES_COMMITTED",
        "CAPABILITIES_REJECTED",
        "SKILL_CATALOG_COMMITTED",
        "SKILL_CATALOG_REJECTED",
        "ARTIFACT_COMMITTED",
        "ARTIFACT_REJECTED",
        "COLLABORATION_COMMITTED",
        "COLLABORATION_REJECTED",
        "CONTROL_ABORT",
        "CONTROL_COMMITTED",
        "LEASE_RENEWED",
        "INTERRUPT",
        "RESET_CONVERSATION",
        "REFRESH_CAPABILITIES",
        "QUIESCE",
        "SHUTDOWN",
        "PING",
        "HELLO",
        "CAPABILITIES",
        "SKILL_CATALOG_PROPOSED",
        "READY",
        "READY_TO_RUN",
        "ASSIGN_REJECTED",
        "ASSIGN_ABORTED",
        "HEARTBEAT",
        "PONG",
        "THREAD_BOUND",
        "EVENT",
        "EVENT_BEGIN",
        "EVENT_CHUNK",
        "EVENT_END",
        "RESULT",
        "INTERRUPT_ACK",
        "IDLE",
        "ARTIFACT_PROPOSED",
        "COLLABORATION_REQUEST",
        "CONTROL_APPLIED",
        "CONTROL_REJECTED",
        "CONTROL_ABORTED",
        "ERROR",
    }
    assert {kind.value for kind in FrameKind} == expected_kinds

    codec = _codec()
    wire = _frame(codec).as_dict()
    wire["direction"] = FrameDirection.CHILD_TO_SUPERVISOR.value
    with pytest.raises(IpcProtocolError, match="not allowed"):
        codec.decode_body(_signed_body(wire))

    with pytest.raises(IpcProtocolError, match="wrong direction"):
        codec.decode(
            codec.encode(_frame(codec)),
            expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
        )
    with pytest.raises(IpcProtocolError, match="wrong stream"):
        codec.decode(
            codec.encode(_frame(codec)),
            expected_stream=FrameStream.DATA,
        )


def test_encode_revalidates_manually_constructed_or_mutated_frames() -> None:
    codec = _codec()
    frame = _frame(codec)

    with pytest.raises(IpcAuthenticationError):
        codec.encode_body(replace(frame, supervisor_epoch=8))

    with pytest.raises(TypeError):
        frame.payload["nonce"] = "mutated after signing"  # type: ignore[index]
    nested = frame.payload["nested"]
    assert isinstance(nested, dict) is False
    with pytest.raises(TypeError):
        nested["values"] = ()  # type: ignore[index]

    tampered = replace(frame, payload={"nonce": "mutated after signing"})
    with pytest.raises(IpcAuthenticationError):
        codec.encode_body(tampered)


def test_frame_size_prefix_and_exact_packet_bounds() -> None:
    codec = _codec()
    packet = codec.encode(_frame(codec))
    body = packet[4:]

    assert FrameCodec(SESSION_KEY, max_frame_bytes=len(body)).decode(packet)
    with pytest.raises(IpcFrameSizeError, match="exceeds"):
        FrameCodec(SESSION_KEY, max_frame_bytes=len(body) - 1).decode(packet)
    with pytest.raises(IpcFrameSizeError, match="nonzero"):
        codec.decode(struct.pack(">I", 0))
    with pytest.raises(IpcFrameSizeError, match="exceeds"):
        FrameCodec(SESSION_KEY, max_frame_bytes=16).decode(struct.pack(">I", 17))
    with pytest.raises(IpcTruncatedFrameError, match="truncated.*prefix"):
        codec.decode(packet[:3])
    with pytest.raises(IpcProtocolError, match="exactly one"):
        codec.decode(packet[:-1])
    with pytest.raises(IpcProtocolError, match="exactly one"):
        codec.decode(packet + b"trailing")

    for invalid_limit in (0, -1, HARD_MAX_FRAME_BYTES + 1, 1 << 32, True):
        with pytest.raises(ValueError, match="max_frame_bytes"):
            FrameCodec(SESSION_KEY, max_frame_bytes=invalid_limit)


def test_async_reader_accepts_partial_reads_and_writer_drains() -> None:
    class RecordingWriter:
        def __init__(self) -> None:
            self.buffer = bytearray()
            self.drain_count = 0

        def write(self, data: bytes) -> None:
            self.buffer.extend(data)

        async def drain(self) -> None:
            self.drain_count += 1

    async def scenario() -> None:
        codec = _codec()
        frame = _frame(codec)
        packet = codec.encode(frame)
        reader = asyncio.StreamReader()
        pending = asyncio.create_task(read_frame(reader, codec))

        reader.feed_data(packet[:2])
        await asyncio.sleep(0)
        assert not pending.done()
        reader.feed_data(packet[2:19])
        await asyncio.sleep(0)
        assert not pending.done()
        reader.feed_data(packet[19:])
        assert await pending == frame

        writer = RecordingWriter()
        await write_frame(writer, codec, frame)  # type: ignore[arg-type]
        assert bytes(writer.buffer) == packet
        assert writer.drain_count == 1

    asyncio.run(scenario())


def test_async_reader_rejects_eof_truncation_and_oversize_before_body_read() -> None:
    async def closed(data: bytes) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        await read_frame(reader, _codec())

    async def scenario() -> None:
        codec = _codec()
        packet = codec.encode(_frame(codec))

        with pytest.raises(IpcStreamClosedError, match="IPC stream closed"):
            await closed(b"")
        with pytest.raises(IpcTruncatedFrameError, match="truncated.*prefix"):
            await closed(packet[:2])
        with pytest.raises(IpcTruncatedFrameError, match="truncated.*body"):
            await closed(packet[:-1])

        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack(">I", 65))
        with pytest.raises(IpcFrameSizeError, match="exceeds"):
            await read_frame(reader, FrameCodec(SESSION_KEY, max_frame_bytes=64))

    asyncio.run(scenario())


def test_replay_guard_accepts_exact_duplicate_and_rejects_gap_and_conflicts() -> None:
    codec = _codec()
    guard = SequenceReplayGuard(_session_identity())
    first = _frame(codec, sequence=0, frame_id="first")

    assert guard.observe(first) is ReplayDisposition.NEW
    assert guard.observe(codec.decode(codec.encode(first))) is ReplayDisposition.DUPLICATE
    assert guard.next_sequence(first.direction, first.stream) == 1

    with pytest.raises(IpcSequenceError, match="gap"):
        guard.observe(_frame(codec, sequence=2, frame_id="gap"))
    with pytest.raises(IpcSequenceError, match="different content"):
        guard.observe(
            _frame(
                codec,
                sequence=0,
                frame_id="conflicting-sequence",
                payload={"nonce": "different"},
            )
        )
    with pytest.raises(IpcSequenceError, match="ipc_frame_id was reused"):
        guard.observe(_frame(codec, sequence=1, frame_id="first"))

    assert guard.next_sequence(first.direction, first.stream) == 1
    assert guard.replay_size == 1


@pytest.mark.parametrize(
    ("identity_field", "drifted_value"),
    [
        ("supervisor_epoch", 8),
        ("agent_id", "agent-beta"),
        ("agent_incarnation", 4),
        ("worker_generation", 12),
    ],
)
def test_replay_guard_rejects_session_identity_drift_without_advancing(
    identity_field: str,
    drifted_value: Any,
) -> None:
    codec = _codec()
    session = _session_identity()
    guard = SequenceReplayGuard(session)
    first = _frame(codec, sequence=0, frame_id="identity-first")
    assert IpcSessionIdentity.from_frame(first) == session
    assert guard.observe(first) is ReplayDisposition.NEW

    drift = {identity_field: drifted_value}
    drifted = _frame(
        codec,
        sequence=1,
        frame_id=f"drift-{identity_field}",
        **drift,
    )
    with pytest.raises(IpcSequenceError, match="identity does not match"):
        guard.observe(drifted)

    assert guard.next_sequence(first.direction, first.stream) == 1
    valid_second = _frame(codec, sequence=1, frame_id="identity-second")
    assert guard.observe(valid_second) is ReplayDisposition.NEW
    assert guard.next_sequence(first.direction, first.stream) == 2


def test_handshake_control_sequences_continue_while_data_lanes_start_at_zero() -> None:
    bootstrap_codec = BootstrapFrameCodec(BOOTSTRAP_PROOF_KEY)
    hello = _frame(
        bootstrap_codec,
        kind=FrameKind.HELLO,
        direction=FrameDirection.CHILD_TO_SUPERVISOR,
        stream=FrameStream.CONTROL,
        sequence=0,
        frame_id="transition-hello",
    )
    config = _frame(
        bootstrap_codec,
        kind=FrameKind.CONFIG,
        direction=FrameDirection.SUPERVISOR_TO_CHILD,
        stream=FrameStream.CONTROL,
        sequence=0,
        frame_id="transition-config",
    )
    assert bootstrap_codec.decode(bootstrap_codec.encode(hello)) == hello
    assert bootstrap_codec.decode(bootstrap_codec.encode(config)) == config

    starts = {
        (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL): 1,
        (FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): 1,
    }
    guard = SequenceReplayGuard(
        _session_identity(),
        initial_sequence=0,
        initial_sequences=starts,
    )
    starts[(FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL)] = 9

    for direction in FrameDirection:
        assert guard.next_sequence(direction, FrameStream.CONTROL) == 1
        assert guard.next_sequence(direction, FrameStream.DATA) == 0
    with pytest.raises(TypeError):
        guard.initial_sequences[
            (FrameDirection.CHILD_TO_SUPERVISOR, FrameStream.CONTROL)
        ] = 9  # type: ignore[index]

    session_codec = _codec()
    with pytest.raises(IpcSequenceError, match="outside the replay window"):
        guard.observe(
            _frame(
                session_codec,
                kind=FrameKind.PING,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                stream=FrameStream.CONTROL,
                sequence=0,
                frame_id="reused-handshake-sequence",
            )
        )

    first_session_frames = (
        _frame(
            session_codec,
            kind=FrameKind.PING,
            direction=FrameDirection.SUPERVISOR_TO_CHILD,
            stream=FrameStream.CONTROL,
            sequence=1,
            frame_id="session-supervisor-control",
        ),
        _frame(
            session_codec,
            kind=FrameKind.HEARTBEAT,
            direction=FrameDirection.CHILD_TO_SUPERVISOR,
            stream=FrameStream.CONTROL,
            sequence=1,
            frame_id="session-child-control",
        ),
        _frame(
            session_codec,
            kind=FrameKind.ASSIGN,
            direction=FrameDirection.SUPERVISOR_TO_CHILD,
            stream=FrameStream.DATA,
            sequence=0,
            frame_id="session-supervisor-data",
        ),
        _frame(
            session_codec,
            kind=FrameKind.CAPABILITIES,
            direction=FrameDirection.CHILD_TO_SUPERVISOR,
            stream=FrameStream.DATA,
            sequence=0,
            frame_id="session-child-data",
        ),
    )
    for frame in first_session_frames:
        assert guard.observe(frame) is ReplayDisposition.NEW
        assert guard.next_sequence(frame.direction, frame.stream) == (
            2 if frame.stream is FrameStream.CONTROL else 1
        )


def test_replay_window_evicts_old_entries_and_lanes_are_independent() -> None:
    codec = _codec()
    guard = SequenceReplayGuard(_session_identity(), replay_window=2)
    control_frames = [
        _frame(codec, sequence=sequence, frame_id=f"control-{sequence}")
        for sequence in range(3)
    ]
    for frame in control_frames:
        assert guard.observe(frame) is ReplayDisposition.NEW
    assert guard.replay_size == 2
    assert guard.observe(control_frames[1]) is ReplayDisposition.DUPLICATE
    with pytest.raises(IpcSequenceError, match="outside the replay window"):
        guard.observe(control_frames[0])

    independent = (
        _frame(
            codec,
            kind=FrameKind.ASSIGN,
            direction=FrameDirection.SUPERVISOR_TO_CHILD,
            stream=FrameStream.DATA,
            sequence=0,
            frame_id="shared-across-lanes",
        ),
        _frame(
            codec,
            kind=FrameKind.HEARTBEAT,
            direction=FrameDirection.CHILD_TO_SUPERVISOR,
            stream=FrameStream.CONTROL,
            sequence=0,
            frame_id="shared-across-lanes",
        ),
        _frame(
            codec,
            kind=FrameKind.EVENT,
            direction=FrameDirection.CHILD_TO_SUPERVISOR,
            stream=FrameStream.DATA,
            sequence=0,
            frame_id="shared-across-lanes",
        ),
    )
    for frame in independent:
        assert guard.observe(frame) is ReplayDisposition.NEW
        assert guard.next_sequence(frame.direction, frame.stream) == 1
    assert guard.next_sequence(
        FrameDirection.SUPERVISOR_TO_CHILD,
        FrameStream.CONTROL,
    ) == 3


def test_replay_guard_configuration_and_initial_sequence() -> None:
    codec = _codec()
    guard = SequenceReplayGuard(
        _session_identity(),
        initial_sequence=5,
        replay_window=1,
    )
    frame = _frame(codec, sequence=5, frame_id="starts-at-five")
    assert guard.next_sequence(frame.direction, frame.stream) == 5
    assert guard.observe(frame) is ReplayDisposition.NEW
    assert guard.next_sequence(frame.direction, frame.stream) == 6

    with pytest.raises(TypeError, match="authenticated IpcFrame"):
        guard.observe(object())  # type: ignore[arg-type]
    for invalid_initial in (-1, 1 << 80, True):
        with pytest.raises(ValueError, match="initial_sequence"):
            SequenceReplayGuard(
                _session_identity(),
                initial_sequence=invalid_initial,
            )
    for invalid_window in (0, -1, True):
        with pytest.raises(ValueError, match="replay_window"):
            SequenceReplayGuard(
                _session_identity(),
                replay_window=invalid_window,
            )
    with pytest.raises(TypeError, match="initial_sequences"):
        SequenceReplayGuard(
            _session_identity(),
            initial_sequences=[],  # type: ignore[arg-type]
        )
    for invalid_starts in (
        {("sideways", "control"): 0},
        {(FrameDirection.SUPERVISOR_TO_CHILD,): 0},
        {(FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): -1},
        {(FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): 1 << 80},
        {(FrameDirection.SUPERVISOR_TO_CHILD, FrameStream.CONTROL): True},
    ):
        with pytest.raises(ValueError, match="initial_sequences"):
            SequenceReplayGuard(
                _session_identity(),
                initial_sequences=invalid_starts,  # type: ignore[arg-type]
            )
    with pytest.raises(TypeError, match="session_identity"):
        SequenceReplayGuard(object())  # type: ignore[arg-type]
    with pytest.raises(IpcProtocolError, match="positive integer"):
        IpcSessionIdentity(
            supervisor_epoch=1 << 80,
            agent_id="agent-alpha",
            agent_incarnation=3,
            worker_generation=11,
        )


def test_ipc_module_has_no_store_channel_sdk_or_pickle_dependency() -> None:
    source = Path(ipc_module.__file__).read_text(encoding="utf-8")
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)

    forbidden_roots = {"src", "sqlite3", "wechat_ilink", "openai", "pickle"}
    assert not {name.split(".", 1)[0] for name in imported} & forbidden_roots
