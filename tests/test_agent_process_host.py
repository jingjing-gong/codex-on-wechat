"""Subprocess tests for the bootstrap-only Agent liveness host."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import os
import secrets
import signal
import struct
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import src.runtime.agent_process as agent_process_module
from src.runtime.agent_process import (
    AGENT_PROCESS_FEATURE,
    GENERATION_CAPABILITY_BYTES,
    HANDSHAKE_NONCE_BYTES,
    AgentProcessCleanupError,
    AgentProcessControlState,
    AgentProcessHandle,
    AgentProcessHandshakeError,
    AgentProcessIdentity,
)
from src.runtime.ipc import (
    BootstrapFrameCodec,
    FrameCodec,
    FrameDirection,
    FrameKind,
    FrameStream,
    HARD_MAX_FRAME_BYTES,
    IpcAuthenticationError,
    IpcProtocolError,
    write_frame,
)
from src.runtime.process_lifetime import (
    InheritedLifetimeLock,
    ProcessInspectionUnavailableError,
    ProcessLifetimeStillHeldError,
    VerifiedProcessGroup,
)


IDENTITY = AgentProcessIdentity(
    supervisor_epoch=41,
    agent_id="bootstrap-test",
    agent_incarnation=2,
    worker_generation=7,
)


async def _cleanup(handle: AgentProcessHandle | None) -> None:
    if handle is not None and handle.state is not AgentProcessControlState.CLOSED:
        await handle.terminate()


def _fd_argument(process: subprocess.Popen[Any], option: str) -> int:
    arguments = list(process.args)
    return int(arguments[arguments.index(option) + 1])


def _fd_count() -> int:
    return len(tuple(Path("/proc/self/fd").iterdir()))


def test_derivation_is_domain_separated_and_bound_to_every_generation_field() -> None:
    capability = b"c" * GENERATION_CAPABILITY_BYTES
    launch_nonce = b"l" * HANDSHAKE_NONCE_BYTES
    child_nonce = b"h" * HANDSHAKE_NONCE_BYTES
    supervisor_nonce = b"s" * HANDSHAKE_NONCE_BYTES
    root = agent_process_module._derive_bootstrap_root(
        capability,
        IDENTITY,
        launch_nonce,
    )
    proof_key = agent_process_module._derive_bootstrap_proof_key(root)
    session_key = agent_process_module._derive_session_key(
        root,
        IDENTITY,
        launch_nonce,
        child_nonce,
        supervisor_nonce,
    )

    assert len(root) == len(proof_key) == len(session_key) == 32
    assert len({root, proof_key, session_key}) == 3
    assert b'"protocol_version":1' in IDENTITY.derivation_context()

    for drifted in (
        replace(IDENTITY, supervisor_epoch=42),
        replace(IDENTITY, agent_id="other-agent"),
        replace(IDENTITY, agent_incarnation=3),
        replace(IDENTITY, worker_generation=8),
    ):
        assert (
            agent_process_module._derive_bootstrap_root(
                capability,
                drifted,
                launch_nonce,
            )
            != root
        )

    for changed_launch, changed_child, changed_supervisor in (
        (b"x" * 32, child_nonce, supervisor_nonce),
        (launch_nonce, b"x" * 32, supervisor_nonce),
        (launch_nonce, child_nonce, b"x" * 32),
    ):
        changed_root = agent_process_module._derive_bootstrap_root(
            capability,
            IDENTITY,
            changed_launch,
        )
        assert (
            agent_process_module._derive_session_key(
                changed_root,
                IDENTITY,
                changed_launch,
                changed_child,
                changed_supervisor,
            )
            != session_key
        )

    with pytest.raises(ValueError, match="protocol version"):
        replace(IDENTITY, protocol_version=2)
    with pytest.raises(ValueError, match="canonical lowercase"):
        replace(IDENTITY, agent_id="Uppercase")


def test_process_host_rejects_frame_limits_above_the_hard_ceiling() -> None:
    async def scenario() -> None:
        with pytest.raises(ValueError, match="hard protocol bound"):
            await AgentProcessHandle.spawn(
                IDENTITY,
                max_frame_bytes=HARD_MAX_FRAME_BYTES + 1,
            )

    asyncio.run(scenario())


def test_fresh_child_has_distinct_pid_exact_identity_and_liveness_states() -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        supervisor_capability = bytearray(b"g" * GENERATION_CAPABILITY_BYTES)
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                control_timeout=3,
                exit_timeout=3,
                generation_capability=supervisor_capability,
            )
            assert supervisor_capability == b"g" * GENERATION_CAPABILITY_BYTES
            assert handle.pid != os.getpid()
            assert handle.identity == IDENTITY
            assert handle.returncode is None
            assert handle.state is AgentProcessControlState.BOOTSTRAPPED
            assert handle.kernel_process_birth_id.startswith("linux:")
            assert handle.lifetime_lock_identity.startswith("linux-flock-v2:")
            assert not handle.lifetime_lock_released
            assert await handle.ping("bootstrap-ping") == "bootstrap-ping"

            await handle.quiesce()
            assert handle.state is AgentProcessControlState.QUIESCED
            assert await handle.ping("quiesced-ping") == "quiesced-ping"

            assert await handle.shutdown() == 0
            assert handle.returncode == 0
            assert handle.state is AgentProcessControlState.CLOSED
            assert handle.lifetime_lock_released
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


@pytest.mark.skipif(not Path("/proc/self/fd").exists(), reason="requires Linux procfs")
def test_secret_is_not_in_argv_or_environment_and_child_descriptors_are_sealed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_secret = "parent-only-secret-" + secrets.token_hex(16)
    monkeypatch.setenv("COW_PARENT_ONLY_SECRET", parent_secret)

    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        generation_capability = b"g" * GENERATION_CAPABILITY_BYTES
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                generation_capability=generation_capability,
            )
            arguments = [str(value) for value in handle.process.args]
            rendered_arguments = "\x00".join(arguments)
            assert parent_secret not in rendered_arguments
            assert generation_capability.decode() not in rendered_arguments
            assert "--generation-capability" not in arguments
            assert "--launch-nonce" not in arguments

            environment = Path(f"/proc/{handle.pid}/environ").read_bytes()
            assert parent_secret.encode() not in environment
            assert generation_capability not in environment
            assert b"COW_PARENT_ONLY_SECRET" not in environment

            control_fd = _fd_argument(handle.process, "--control-fd")
            data_fd = _fd_argument(handle.process, "--data-fd")
            bootstrap_fd = _fd_argument(handle.process, "--bootstrap-fd")
            for fd in (control_fd, data_fd):
                fdinfo = Path(f"/proc/{handle.pid}/fdinfo/{fd}").read_text()
                flags_line = next(
                    line for line in fdinfo.splitlines() if line.startswith("flags:")
                )
                flags = int(flags_line.split()[1], 8)
                assert flags & os.O_CLOEXEC
            # The one-shot descriptor is closed before HELLO, and therefore
            # before the supervisor accepts the bootstrap liveness proof.
            assert not Path(f"/proc/{handle.pid}/fd/{bootstrap_fd}").exists()

            assert arguments[1:3] == ["-I", "-S"]
            assert Path(arguments[3]).resolve() == Path(
                agent_process_module.__file__
            ).with_name(
                "agent_child.py"
            ).resolve()
            assert "-m" not in arguments[:4]
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_child_rejects_config_authenticated_with_the_wrong_bootstrap_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write_frame = agent_process_module.write_frame
    original_popen = agent_process_module.subprocess.Popen
    children: list[subprocess.Popen[Any]] = []

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
        child = original_popen(*args, **kwargs)
        children.append(child)
        return child

    async def corrupt_config(
        writer: asyncio.StreamWriter,
        codec: FrameCodec,
        frame: Any,
    ) -> None:
        if frame.kind is not FrameKind.CONFIG:
            await original_write_frame(writer, codec, frame)
            return
        wrong_codec = BootstrapFrameCodec(
            b"wrong-bootstrap-proof-key" + (b"!" * 32),
            max_frame_bytes=codec.max_frame_bytes,
        )
        wrong = wrong_codec.build_frame(
            kind=frame.kind,
            direction=frame.direction,
            stream=frame.stream,
            supervisor_epoch=frame.supervisor_epoch,
            agent_id=frame.agent_id,
            agent_incarnation=frame.agent_incarnation,
            worker_generation=frame.worker_generation,
            ipc_frame_id=frame.ipc_frame_id,
            reply_to_frame_id=frame.reply_to_frame_id,
            stream_sequence=frame.stream_sequence,
            payload=dict(frame.payload),
            protocol_version=frame.protocol_version,
        )
        writer.write(wrong_codec.encode(wrong))
        await writer.drain()

    monkeypatch.setattr(agent_process_module.subprocess, "Popen", recording_popen)
    monkeypatch.setattr(agent_process_module, "write_frame", corrupt_config)

    async def scenario() -> None:
        with pytest.raises(AgentProcessHandshakeError, match="startup handshake"):
            await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=2,
                exit_timeout=2,
            )

    asyncio.run(scenario())
    assert len(children) == 1
    assert children[0].poll() is not None
    assert children[0].returncode != 0


def test_startup_timeout_is_one_total_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read_frame = agent_process_module.read_frame

    async def delayed_handshake_read(*args: Any, **kwargs: Any) -> Any:
        frame = await original_read_frame(*args, **kwargs)
        if frame.kind in {FrameKind.HELLO, FrameKind.PONG}:
            await asyncio.sleep(0.45)
        return frame

    monkeypatch.setattr(
        agent_process_module,
        "read_frame",
        delayed_handshake_read,
    )

    async def scenario() -> float:
        started = asyncio.get_running_loop().time()
        with pytest.raises(AgentProcessHandshakeError, match="startup handshake"):
            await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=0.8,
                exit_timeout=1,
            )
        return asyncio.get_running_loop().time() - started

    elapsed = asyncio.run(scenario())
    assert elapsed < 1.2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("supervisor_epoch", 42),
        ("agent_id", "stale-agent"),
        ("agent_incarnation", 3),
        ("worker_generation", 8),
    ],
)
def test_child_rejects_session_frame_with_stale_generation_identity(
    field: str,
    value: Any,
) -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                exit_timeout=3,
            )
            frame_fields = {
                "supervisor_epoch": IDENTITY.supervisor_epoch,
                "agent_id": IDENTITY.agent_id,
                "agent_incarnation": IDENTITY.agent_incarnation,
                "worker_generation": IDENTITY.worker_generation,
            }
            frame_fields[field] = value
            stale = handle._codec.build_frame(
                kind=FrameKind.PING,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                stream=FrameStream.CONTROL,
                ipc_frame_id="stale-identity-frame",
                reply_to_frame_id=None,
                stream_sequence=handle._guard.next_sequence(
                    FrameDirection.SUPERVISOR_TO_CHILD,
                    FrameStream.CONTROL,
                ),
                payload={"nonce": "stale"},
                **frame_fields,
            )
            await write_frame(handle._writer, handle._codec, stale)
            assert await handle.wait(timeout=3) != 0
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_child_replays_byte_identical_pong_for_exact_duplicate_ping() -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                exit_timeout=3,
                replay_window=4,
            )
            ping = handle._codec.build_frame(
                kind=FrameKind.PING,
                direction=FrameDirection.SUPERVISOR_TO_CHILD,
                stream=FrameStream.CONTROL,
                supervisor_epoch=IDENTITY.supervisor_epoch,
                agent_id=IDENTITY.agent_id,
                agent_incarnation=IDENTITY.agent_incarnation,
                worker_generation=IDENTITY.worker_generation,
                ipc_frame_id="duplicate-ping",
                reply_to_frame_id=None,
                stream_sequence=handle._guard.next_sequence(
                    FrameDirection.SUPERVISOR_TO_CHILD,
                    FrameStream.CONTROL,
                ),
                payload={"nonce": "same-request"},
            )
            packet = handle._codec.encode(ping)
            handle._writer.write(packet + packet)
            await handle._writer.drain()

            first = await agent_process_module.read_frame(
                handle._reader,
                handle._codec,
                expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
                expected_stream=FrameStream.CONTROL,
            )
            duplicate = await agent_process_module.read_frame(
                handle._reader,
                handle._codec,
                expected_direction=FrameDirection.CHILD_TO_SUPERVISOR,
                expected_stream=FrameStream.CONTROL,
            )
            assert first == duplicate
            assert first.kind is FrameKind.PONG
            assert first.reply_to_frame_id == ping.ipc_frame_id
            assert first.payload["nonce"] == "same-request"

            assert await handle.close_connection() == 0
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_child_rejects_oversize_control_prefix_without_reading_a_body() -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                exit_timeout=3,
            )
            handle._writer.write(
                struct.pack(">I", handle._codec.max_frame_bytes + 1)
            )
            await handle._writer.drain()
            assert await handle.wait(timeout=3) != 0
            assert handle.state is AgentProcessControlState.CLOSED
            assert handle._writer.is_closing()
            assert handle._data_socket.fileno() == -1
            assert handle.lifetime_lock_released
            with pytest.raises(IpcAuthenticationError, match="codec is closed"):
                handle._codec.encode(
                    handle._codec.build_frame(
                        kind=FrameKind.PING,
                        direction=FrameDirection.SUPERVISOR_TO_CHILD,
                        stream=FrameStream.CONTROL,
                        supervisor_epoch=IDENTITY.supervisor_epoch,
                        agent_id=IDENTITY.agent_id,
                        agent_incarnation=IDENTITY.agent_incarnation,
                        worker_generation=IDENTITY.worker_generation,
                        ipc_frame_id="closed-codec",
                        reply_to_frame_id=None,
                        stream_sequence=99,
                        payload={"nonce": "closed"},
                    )
                )
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_child_control_response_write_is_bounded() -> None:
    class BlockingWriter:
        def write(self, _packet: bytes) -> None:
            return

        async def drain(self) -> None:
            await asyncio.Future()

    async def scenario() -> None:
        codec = FrameCodec(b"bounded-child-control-key" + (b"!" * 32))
        guard = agent_process_module._session_guard(IDENTITY, replay_window=4)
        ping = codec.build_frame(
            kind=FrameKind.PING,
            direction=FrameDirection.SUPERVISOR_TO_CHILD,
            stream=FrameStream.CONTROL,
            supervisor_epoch=IDENTITY.supervisor_epoch,
            agent_id=IDENTITY.agent_id,
            agent_incarnation=IDENTITY.agent_incarnation,
            worker_generation=IDENTITY.worker_generation,
            ipc_frame_id="blocked-response-ping",
            reply_to_frame_id=None,
            stream_sequence=guard.next_sequence(
                FrameDirection.SUPERVISOR_TO_CHILD,
                FrameStream.CONTROL,
            ),
            payload={"nonce": "bounded-write"},
        )
        reader = asyncio.StreamReader()
        reader.feed_data(codec.encode(ping))
        host = agent_process_module.AgentChildHost(
            IDENTITY,
            control_fd=100,
            data_fd=101,
            bootstrap_fd=102,
            control_timeout=0.05,
            replay_window=4,
        )
        try:
            with pytest.raises(asyncio.TimeoutError):
                await host._serve(
                    reader,
                    BlockingWriter(),  # type: ignore[arg-type]
                    codec,
                    guard,
                )
        finally:
            codec.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("exit_method", ["shutdown", "eof"])
def test_child_exits_cleanly_on_shutdown_or_control_eof(exit_method: str) -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(
                replace(IDENTITY, worker_generation=8),
                startup_timeout=3,
                exit_timeout=3,
            )
            if exit_method == "shutdown":
                returncode = await handle.shutdown()
            else:
                returncode = await handle.close_connection()
            assert returncode == 0
            assert handle.returncode == 0
            assert handle.state is AgentProcessControlState.CLOSED
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "partial_packet",
    [b"\x00", struct.pack(">I", 12) + b"{}"],
    ids=["partial-prefix", "partial-body"],
)
def test_child_rejects_truncated_control_frames(partial_packet: bytes) -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                exit_timeout=3,
            )
            handle._writer.write(partial_packet)
            await handle._writer.drain()
            handle._writer.close()
            await handle._writer.wait_closed()

            assert await handle.wait(timeout=3) != 0
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_bootstrap_wire_never_emits_schedulable_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read_frame = agent_process_module.read_frame
    observed: list[FrameKind] = []

    async def recording_read_frame(*args: Any, **kwargs: Any) -> Any:
        frame = await original_read_frame(*args, **kwargs)
        observed.append(frame.kind)
        return frame

    monkeypatch.setattr(agent_process_module, "read_frame", recording_read_frame)

    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(IDENTITY, startup_timeout=3)
            assert handle.state is AgentProcessControlState.BOOTSTRAPPED
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())
    assert observed == [FrameKind.HELLO, FrameKind.PONG]
    assert FrameKind.READY not in observed


def test_isolated_child_ignores_hostile_python_startup_hook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "startup-hook-ran"
    (tmp_path / "sitecustomize.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('ran', encoding='utf-8')\n",
        encoding="utf-8",
    )
    original_popen = agent_process_module.subprocess.Popen

    def injecting_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
        environment = dict(kwargs["env"])
        environment["PYTHONPATH"] = str(tmp_path)
        kwargs["env"] = environment
        return original_popen(*args, **kwargs)

    monkeypatch.setattr(agent_process_module.subprocess, "Popen", injecting_popen)

    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(IDENTITY, startup_timeout=3)
            assert await handle.ping("isolated") == "isolated"
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())
    assert not marker.exists()


def test_spawn_cancellation_reaps_the_dedicated_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_popen = agent_process_module.subprocess.Popen
    original_read_frame = agent_process_module.read_frame
    children: list[subprocess.Popen[Any]] = []
    hello_received = asyncio.Event()

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
        process = original_popen(*args, **kwargs)
        children.append(process)
        return process

    async def pause_after_hello(*args: Any, **kwargs: Any) -> Any:
        frame = await original_read_frame(*args, **kwargs)
        if frame.kind is FrameKind.HELLO:
            hello_received.set()
            await asyncio.Future()
        return frame

    monkeypatch.setattr(agent_process_module.subprocess, "Popen", recording_popen)
    monkeypatch.setattr(agent_process_module, "read_frame", pause_after_hello)

    async def scenario() -> None:
        spawn = asyncio.create_task(
            AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=3,
                exit_timeout=1,
            )
        )
        await asyncio.wait_for(hello_received.wait(), timeout=3)
        spawn.cancel()
        with pytest.raises(asyncio.CancelledError):
            await spawn

    asyncio.run(scenario())
    assert len(children) == 1
    assert children[0].poll() is not None
    assert not agent_process_module._process_group_exists(children[0].pid)


def test_terminate_finishes_group_cleanup_before_delivering_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_terminate = agent_process_module._terminate_process_group

    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()

        async def delayed_terminate(
            process: subprocess.Popen[Any],
            timeout: float,
            **evidence: Any,
        ) -> None:
            cleanup_started.set()
            await allow_cleanup.wait()
            await original_terminate(process, timeout, **evidence)

        monkeypatch.setattr(
            agent_process_module,
            "_terminate_process_group",
            delayed_terminate,
        )
        try:
            handle = await AgentProcessHandle.spawn(IDENTITY, startup_timeout=3)
            terminating = asyncio.create_task(handle.terminate())
            await asyncio.wait_for(cleanup_started.wait(), timeout=3)
            terminating.cancel()
            allow_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await terminating

            assert handle.state is AgentProcessControlState.CLOSED
            assert handle.returncode is not None
            assert handle.lifetime_lock_released
            assert not agent_process_module._process_group_exists(
                handle.process_group_id
            )
        finally:
            monkeypatch.setattr(
                agent_process_module,
                "_terminate_process_group",
                original_terminate,
            )
            await _cleanup(handle)

    asyncio.run(scenario())


def test_forced_cleanup_kills_and_reaps_the_entire_process_group(
    tmp_path: Path,
) -> None:
    child_pid_path = tmp_path / "descendant.pid"
    descendant_code = (
        "import signal,time;"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
        "time.sleep(60)"
    )
    async def scenario() -> None:
        lifetime_lock = InheritedLifetimeLock.create()
        lifetime_fd = lifetime_lock.child_fd
        leader_code = (
            "import pathlib,signal,subprocess,sys,time;"
            f"child=subprocess.Popen([sys.executable,'-c',{descendant_code!r}],"
            f"pass_fds=({lifetime_fd},));"
            f"pathlib.Path({str(child_pid_path)!r}).write_text(str(child.pid));"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "time.sleep(60)"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", leader_code],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            pass_fds=(lifetime_fd,),
        )
        process_group = VerifiedProcessGroup.capture_dedicated(process.pid)
        lifetime_lock.release_parent_copy()
        try:
            for _ in range(200):
                if child_pid_path.exists():
                    break
                await asyncio.sleep(0.01)
            assert child_pid_path.exists()
            descendant_pid = int(child_pid_path.read_text())

            await agent_process_module._terminate_process_group(
                process,
                1.0,
                process_group=process_group,
                lifetime_lock=lifetime_lock,
            )

            assert process.poll() is not None
            assert not agent_process_module._process_group_exists(process.pid)
            if Path("/proc").exists():
                assert not Path(f"/proc/{descendant_pid}").exists()
        finally:
            if agent_process_module._process_group_exists(process.pid):
                os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)

    asyncio.run(scenario())


def test_concurrent_shutdown_calls_share_one_serialized_cleanup() -> None:
    async def scenario() -> None:
        handle: AgentProcessHandle | None = None
        try:
            handle = await AgentProcessHandle.spawn(IDENTITY, startup_timeout=3)
            assert await asyncio.gather(handle.shutdown(), handle.shutdown()) == [0, 0]
            assert handle.state is AgentProcessControlState.CLOSED
            assert not agent_process_module._process_group_exists(
                handle.process_group_id
            )
        finally:
            await _cleanup(handle)

    asyncio.run(scenario())


def test_agent_child_module_has_no_store_channel_sdk_or_database_dependency() -> None:
    source_paths = (
        Path(agent_process_module.__file__).with_name("agent_child.py"),
        Path(agent_process_module.__file__).with_name("agent_child_runtime.py"),
    )
    imports: set[str] = set()
    sources = []
    for source_path in source_paths:
        source = source_path.read_text(encoding="utf-8")
        sources.append(source)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)

    forbidden = {
        name
        for name in imports
        if name == "sqlite3"
        or name.endswith("agent_process")
        or name.startswith("src.channels")
        or name.startswith("src.agents")
        or name.startswith("src.runtime.sqlite_store")
        or name.startswith("src.runtime.store")
        or name.startswith("wechat_ilink")
        or name.startswith("openai_codex")
    }
    assert forbidden == set()
    combined = "\n".join(sources)
    assert AGENT_PROCESS_FEATURE in combined
    assert "FrameKind.ASSIGN" not in combined


def test_invalid_capability_fails_before_lifetime_fds_are_acquired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _fd_count()

    def unexpected_create(cls: type[InheritedLifetimeLock]) -> InheritedLifetimeLock:
        raise AssertionError("lifetime lock must not be acquired")

    monkeypatch.setattr(
        InheritedLifetimeLock,
        "create",
        classmethod(unexpected_create),
    )

    async def scenario() -> None:
        with pytest.raises(ValueError, match="invalid length"):
            await AgentProcessHandle.spawn(
                IDENTITY,
                generation_capability=b"too-short",
            )

    asyncio.run(scenario())
    assert _fd_count() == before


def test_lifetime_lock_creation_self_tests_holder_probe_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _fd_count()

    def nonconflicting_flock(_fd: int, _operation: int) -> None:
        return None

    import src.runtime.process_lifetime as lifetime_module

    monkeypatch.setattr(lifetime_module.fcntl, "flock", nonconflicting_flock)
    with pytest.raises(
        ProcessInspectionUnavailableError,
        match="holder and probe do not conflict",
    ):
        InheritedLifetimeLock.create()
    assert _fd_count() == before


def test_group_capture_failure_reaps_child_and_closes_lifetime_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _fd_count()
    original_create = InheritedLifetimeLock.create
    original_popen = agent_process_module.subprocess.Popen
    created: list[tuple[InheritedLifetimeLock, int, int, int]] = []
    children: list[subprocess.Popen[Any]] = []
    injected = ProcessInspectionUnavailableError("injected group capture failure")

    def recording_create(
        cls: type[InheritedLifetimeLock],
        lock_root: str | os.PathLike[str],
    ) -> InheritedLifetimeLock:
        lock = original_create(lock_root)
        assert (
            lock._holder_fd is not None
            and lock._probe_fd is not None
            and lock._root_fd is not None
        )
        created.append(
            (lock, lock._holder_fd, lock._probe_fd, lock._root_fd)
        )
        return lock

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
        process = original_popen(*args, **kwargs)
        children.append(process)
        return process

    def fail_capture(_pid: int) -> VerifiedProcessGroup:
        raise injected

    monkeypatch.setattr(
        InheritedLifetimeLock,
        "create",
        classmethod(recording_create),
    )
    monkeypatch.setattr(agent_process_module.subprocess, "Popen", recording_popen)
    monkeypatch.setattr(
        VerifiedProcessGroup,
        "capture_dedicated",
        staticmethod(fail_capture),
    )

    async def scenario() -> None:
        with pytest.raises(
            AgentProcessHandshakeError,
            match="kernel birth/process-group verification",
        ) as caught:
            await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=2,
                exit_timeout=2,
            )
        assert caught.value.__cause__ is injected

    asyncio.run(scenario())
    assert len(children) == len(created) == 1
    assert children[0].poll() is not None
    lock, holder_fd, probe_fd, root_fd = created[0]
    assert lock.released
    assert lock._holder_fd is lock._probe_fd is lock._root_fd is None
    for fd in (holder_fd, probe_fd, root_fd):
        with pytest.raises(OSError):
            os.fstat(fd)
    assert _fd_count() == before


def test_failed_verified_cleanup_retains_retryable_owned_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _fd_count()
    original_read_frame = agent_process_module.read_frame
    original_terminate = agent_process_module.terminate_verified_process_group

    async def reject_startup(*_args: Any, **_kwargs: Any) -> Any:
        raise IpcProtocolError("injected authenticated startup failure")

    async def fail_cleanup(*_args: Any, **_kwargs: Any) -> None:
        raise ProcessLifetimeStillHeldError("injected verified cleanup failure")

    monkeypatch.setattr(agent_process_module, "read_frame", reject_startup)
    monkeypatch.setattr(
        agent_process_module,
        "terminate_verified_process_group",
        fail_cleanup,
    )

    async def scenario() -> None:
        with pytest.raises(AgentProcessCleanupError) as caught:
            await AgentProcessHandle.spawn(
                IDENTITY,
                startup_timeout=2,
                exit_timeout=1,
            )
        error = caught.value
        assert isinstance(error.startup_error, IpcProtocolError)
        assert isinstance(error.cleanup_error, ProcessLifetimeStillHeldError)
        owner = error.cleanup_owner
        assert not owner.closed
        assert owner.process_group is not None
        assert owner.process_group._leader_pidfd is not None
        assert owner.lifetime_lock._holder_fd is not None
        assert owner.lifetime_lock._probe_fd is not None
        assert owner.lifetime_lock._root_fd is not None
        evidence_fds = (
            owner.process_group._leader_pidfd,
            owner.lifetime_lock._holder_fd,
            owner.lifetime_lock._probe_fd,
            owner.lifetime_lock._root_fd,
        )
        for fd in evidence_fds:
            assert fd is not None
            os.fstat(fd)

        monkeypatch.setattr(agent_process_module, "read_frame", original_read_frame)
        monkeypatch.setattr(
            agent_process_module,
            "terminate_verified_process_group",
            original_terminate,
        )
        await owner.retry(timeout=2)
        owner.close_after_proof()
        assert owner.closed
        assert owner.process.poll() is not None
        assert owner.lifetime_lock.released
        for fd in evidence_fds:
            assert fd is not None
            with pytest.raises(OSError):
                os.fstat(fd)

    asyncio.run(scenario())
    assert _fd_count() == before


def test_production_spawn_has_no_injectable_python_executable() -> None:
    parameters = inspect.signature(AgentProcessHandle.spawn).parameters
    assert "python_executable" not in parameters
    assert not any("executable" in name for name in parameters)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_fork_descendant_closes_ipc_but_retains_lifetime_descriptor() -> None:
    runtime_path = Path(agent_process_module.__file__).with_name(
        "agent_child_runtime.py"
    )
    program = f'''
import os, socket, sys
sys.path.insert(0, {str(runtime_path.parent.parent.parent)!r})
from src.runtime.agent_child_runtime import _ForkDescriptorBoundary
control_parent, control_child = socket.socketpair()
data_parent, data_child = socket.socketpair()
lifetime_read, lifetime_write = os.pipe()
report_read, report_write = os.pipe()
_ForkDescriptorBoundary(control_child.fileno(), data_child.fileno(), lifetime_write)
pid = os.fork()
if pid == 0:
    os.close(report_read)
    states = []
    for fd in (control_child.fileno(), data_child.fileno(), lifetime_write):
        try:
            os.fstat(fd)
            states.append("open")
        except OSError:
            states.append("closed")
    os.write(report_write, ",".join(states).encode("ascii"))
    os._exit(0)
os.close(report_write)
result = os.read(report_read, 128)
os.waitpid(pid, 0)
print(result.decode("ascii"), flush=True)
'''
    completed = subprocess.run(
        [sys.executable, "-I", "-c", program],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert completed.stdout.strip() == "closed,closed,open"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_lifetime_lock_is_held_until_last_fork_descendant_exits() -> None:
    lifetime_lock = InheritedLifetimeLock.create()
    lifetime_fd = lifetime_lock.child_fd
    leader_code = (
        "import os,time;"
        "pid=os.fork();"
        "time.sleep(0.6) if pid==0 else None;"
        "os._exit(0)"
    )
    leader = subprocess.Popen(
        [sys.executable, "-c", leader_code],
        pass_fds=(lifetime_fd,),
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    lifetime_lock.release_parent_copy()
    leader.wait(timeout=2)
    assert not lifetime_lock.prove_released()
    for _ in range(100):
        if lifetime_lock.prove_released():
            break
        import time

        time.sleep(0.02)
    assert lifetime_lock.released


def test_cleanup_fails_closed_on_kernel_birth_identity_mismatch() -> None:
    async def scenario() -> None:
        lifetime_lock = InheritedLifetimeLock.create()
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            pass_fds=(lifetime_lock.child_fd,),
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        group = VerifiedProcessGroup.capture_dedicated(process.pid)
        correct = group.leader
        group.leader = replace(
            correct,
            start_time_ticks=correct.start_time_ticks + 1,
        )
        try:
            with pytest.raises(
                agent_process_module.AgentProcessExitedError,
                match="safely proven stopped",
            ):
                await agent_process_module._terminate_process_group(
                    process,
                    0.2,
                    process_group=group,
                    lifetime_lock=lifetime_lock,
                )
            assert process.poll() is None
        finally:
            group.leader = correct
            await agent_process_module._terminate_process_group(
                process,
                1.0,
                process_group=group,
                lifetime_lock=lifetime_lock,
            )

    asyncio.run(scenario())


def test_child_launcher_rejects_wrong_parent_before_runtime_import() -> None:
    launcher = Path(agent_process_module.__file__).with_name("agent_child.py")
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            str(launcher),
            "--parent-pid",
            "1",
            "--parent-start-time",
            "1",
            "--parent-boot-id",
            boot_id,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=3,
    )
    assert completed.returncode == 70
    assert completed.stdout == completed.stderr == b""


def test_linux_parent_death_signal_kills_bootstrapped_child(
    tmp_path: Path,
) -> None:
    project_root = Path(agent_process_module.__file__).parents[2]
    lock_root = (tmp_path / "locks").resolve()
    program = f'''
import asyncio
from src.runtime.agent_process import AgentProcessHandle, AgentProcessIdentity
async def main():
    handle = await AgentProcessHandle.spawn(
        AgentProcessIdentity(77, "parent-death", 1, 1),
        lifetime_lock_root={str(lock_root)!r},
        startup_timeout=3,
        exit_timeout=2,
    )
    print(handle.pid, handle.lifetime_lock_identity, sep="|", flush=True)
    await asyncio.Event().wait()
asyncio.run(main())
'''
    supervisor = subprocess.Popen(
        [sys.executable, "-u", "-c", program],
        cwd=project_root,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert supervisor.stdout is not None
    line = supervisor.stdout.readline().strip()
    child_text, lifetime_identity = line.split("|", 1)
    assert child_text.isdigit(), supervisor.stderr.read() if supervisor.stderr else ""
    child_pid = int(child_text)
    os.kill(supervisor.pid, signal.SIGKILL)
    supervisor.wait(timeout=3)
    for _ in range(200):
        if not Path(f"/proc/{child_pid}").exists():
            break
        import time

        time.sleep(0.01)
    assert not Path(f"/proc/{child_pid}").exists()
    replacement = InheritedLifetimeLock.reopen(lock_root, lifetime_identity)
    replacement.release_parent_copy()
    assert replacement.prove_released()
    assert not Path(replacement.path).exists()
