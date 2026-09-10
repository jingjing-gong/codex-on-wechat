from __future__ import annotations

import asyncio
import json
import os
import socket
import tempfile
from pathlib import Path

import pytest

from src.channels.lark import (
    CONFIG_ENVIRONMENT_KEY,
    LarkBotProfile,
    LarkCliProcess,
    LarkCliVersionError,
    LarkError,
    LarkPermanentDeliveryError,
    LarkProfileError,
    LarkProtocolError,
    LarkRateLimitError,
    validate_private_config_directory,
)
from src.channels.models import InboundEnvelope, ReplyTarget


FAKE_CLI = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import time

log_path = os.environ.get("FAKE_LARK_ARGS_FILE")
if log_path:
    with open(log_path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(sys.argv[1:]) + "\n")

if os.environ.get("FAKE_LARK_CREATE_DEFAULT_STATE") == "1":
    config_root = Path(os.environ["LARKSUITE_CLI_CONFIG_DIR"])
    cache_dir = config_root / "cache"
    cache_dir.mkdir(exist_ok=True)
    (cache_dir / "runtime-state.json").write_text("{}", encoding="utf-8")

# Real lark-cli 1.0.92 explicitly creates remote_meta.meta.json as 0644,
# overriding the private child umask.  Keep this fixture limited to one-shot
# verification commands: the observed artifact exists before the long-lived
# event consumer starts.
remote_meta_kind = os.environ.get("FAKE_LARK_REMOTE_META_KIND", "")
if remote_meta_kind and sys.argv[1:2] != ["event"]:
    config_root = Path(os.environ["LARKSUITE_CLI_CONFIG_DIR"])
    cache_dir = config_root / "cache"
    cache_dir.mkdir(exist_ok=True)
    remote_meta = cache_dir / "remote_meta.meta.json"
    if remote_meta_kind == "insecure-file":
        remote_meta.write_text("{}", encoding="utf-8")
        remote_meta.chmod(0o644)
    elif remote_meta_kind == "symlink" and not (
        remote_meta.exists() or remote_meta.is_symlink()
    ):
        remote_meta.symlink_to(os.environ["FAKE_LARK_REMOTE_META_TARGET"])
    elif remote_meta_kind == "hardlink" and not remote_meta.exists():
        os.link(os.environ["FAKE_LARK_REMOTE_META_TARGET"], remote_meta)

if sys.argv[1:] == ["--version"]:
    print("lark-cli " + os.environ.get("FAKE_LARK_VERSION", "1.0.92"))
    raise SystemExit(0)
if sys.argv[1:] == ["config", "show"]:
    print(json.dumps({
        "appId": "cli_processbot1", "brand": "lark", "appSecret": "****"
    }))
    raise SystemExit(0)
if sys.argv[1:] == ["auth", "status", "--verify", "--json"]:
    available = os.environ.get("FAKE_LARK_AUTH_AVAILABLE", "true")
    if available == "true":
        available = True
    elif available == "false":
        available = False
    payload = {
        "appId": "cli_processbot1",
        "brand": "lark",
        "identities": {"bot": {
            "status": "ready",
            "available": available,
            "verified": True,
            "openId": "ou_processbot1",
        }}
    }
    if os.environ.get("FAKE_LARK_AUTH_CONFLICT_APP_ALIAS"):
        payload["app_id"] = "cli_conflicting_app"
    if os.environ.get("FAKE_LARK_AUTH_RAW") == "1":
        response = payload
    elif (
        os.environ.get("FAKE_LARK_AUTH_WRAPPED") == "1"
        or "FAKE_LARK_AUTH_OK" in os.environ
    ):
        response = {
            "ok": os.environ.get("FAKE_LARK_AUTH_OK", "true") == "true",
            "identity": os.environ.get("FAKE_LARK_AUTH_IDENTITY", "bot"),
            "data": payload,
        }
    else:
        verified = os.environ.get("FAKE_LARK_AUTH_VERIFIED", "true")
        if verified == "true":
            verified = True
        elif verified == "false":
            verified = False
        response = {
            **payload,
            "identity": os.environ.get("FAKE_LARK_AUTH_IDENTITY", "bot"),
            "verified": verified,
        }
    rendered = json.dumps(response)
    duplicate = os.environ.get("FAKE_LARK_AUTH_DUPLICATE", "")
    duplicate_values = {
        "appId": ('"appId": ' + json.dumps(response.get("appId")), '"appId": "cli_forged_duplicate", '),
        "identity": ('"identity": ' + json.dumps(response.get("identity")), '"identity": "user", '),
        "openId": ('"openId": ' + json.dumps(payload["identities"]["bot"]["openId"]), '"openId": "ou_forged_duplicate", '),
        "verified": ('"verified": true', '"verified": false, '),
    }
    if duplicate in duplicate_values:
        needle, forged = duplicate_values[duplicate]
        position = rendered.rfind(needle) if duplicate == "verified" else rendered.find(needle)
        rendered = rendered[:position] + forged + rendered[position:]
    if os.environ.get("FAKE_LARK_AUTH_DIAGNOSTIC_PREFIX") == "1":
        print('diagnostic prefix {"appId":"cli_forged_prefix"}')
    if os.environ.get("FAKE_LARK_AUTH_CONFLICTING_OBJECTS") == "1":
        forged_response = dict(response)
        forged_response["appId"] = "cli_forged_conflicting_object"
        print(json.dumps(forged_response))
    print(rendered)
    raise SystemExit(0)
if sys.argv[1:] == ["whoami", "--as", "bot"]:
    print(json.dumps({
        "appId": os.environ.get("FAKE_LARK_WHOAMI_APP_ID", "cli_processbot1"),
        "available": os.environ.get("FAKE_LARK_WHOAMI_AVAILABLE", "true") == "true",
        "brand": "lark",
        "defaultAs": "auto",
        "identity": os.environ.get("FAKE_LARK_WHOAMI_IDENTITY", "bot"),
        "identitySource": os.environ.get("FAKE_LARK_WHOAMI_SOURCE", "flag"),
        "profile": "cli_processbot1",
        "tokenStatus": os.environ.get("FAKE_LARK_WHOAMI_TOKEN_STATUS", "ready"),
    }))
    raise SystemExit(0)
if sys.argv[1:] == ["event", "consume", "im.message.receive_v1", "--as", "bot"]:
    ready_line = os.environ.get(
        "FAKE_LARK_READY_LINE",
        "[event] ready event_key=im.message.receive_v1",
    )
    if ready_line:
        print(ready_line, file=sys.stderr, flush=True)
    child_path = os.environ.get("FAKE_LARK_CHILD_PID_FILE")
    if child_path:
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(child_path, "w", encoding="utf-8") as stream:
            stream.write(str(child.pid))
    if os.environ.get("FAKE_LARK_MALFORMED") == "1":
        time.sleep(0.05)
        print("not-json", flush=True)
        sys.stdin.buffer.read()
        raise SystemExit(0)
    print(json.dumps({
        "type": "im.message.receive_v1",
        "event_id": "event-1",
        "message_id": "om_1",
        "chat_id": "oc_processchat1",
        "chat_type": "p2p",
        "message_type": "text",
        "sender_id": "ou_userprocess1",
        "content": "hello"
    }), flush=True)
    sys.stdin.buffer.read()
    raise SystemExit(0)
if sys.argv[1:] == ["test-child-timeout"]:
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    with open(os.environ["FAKE_LARK_CHILD_PID_FILE"], "w", encoding="utf-8") as stream:
        stream.write(str(child.pid))
    time.sleep(60)
if os.environ.get("FAKE_LARK_ERROR") == "rate-limit":
    print(json.dumps({
        "ok": False,
        "error": {
            "type": "api", "subtype": "rate_limit", "code": 99991400,
            "message": "slow down", "retry_after_seconds": 7
        }
    }), file=sys.stderr)
    raise SystemExit(1)
if os.environ.get("FAKE_LARK_ERROR") == "missing-ack":
    print(json.dumps({"ok": True, "identity": "bot", "data": {}}))
    raise SystemExit(0)
print(json.dumps({"ok": True, "identity": "bot", "data": {"message_id": "om_sent"}}))

'''


def _executable(tmp_path: Path) -> Path:
    executable = tmp_path / "lark-cli"
    # Test setup creates the fake external dependency, not repository source.
    executable.write_text(FAKE_CLI, encoding="utf-8")
    executable.chmod(0o700)
    return executable


def _profile(tmp_path: Path) -> LarkBotProfile:
    config = tmp_path / "config"
    config.mkdir(mode=0o700)
    return LarkBotProfile(
        profile_id="process",
        app_id="cli_processbot1",
        bot_open_id="ou_processbot1",
        config_dir=config,
    )


async def _read_pid(path: Path) -> int:
    for _ in range(100):
        if path.exists() and path.read_text().strip():
            return int(path.read_text().strip())
        await asyncio.sleep(0.01)
    raise AssertionError("fake CLI child PID was not published")


async def _assert_pid_exits(pid: int) -> None:
    for _ in range(200):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"CLI descendant remained alive: {pid}")


def test_consumer_uses_isolated_config_and_yields_generation_fenced_ndjson(
    tmp_path, monkeypatch
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_VERSION", "1.0.92")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        generation = await process.start()
        try:
            assert generation == 1
            event = await anext(process.events())
            assert event.generation == generation
            assert event.payload["message_id"] == "om_1"
            assert process.profile.environment()[CONFIG_ENVIRONMENT_KEY] == str(
                process.profile.config_dir
            )
        finally:
            await process.stop()

    asyncio.run(scenario())


@pytest.mark.skipif(os.name != "posix", reason="child umask is POSIX-specific")
def test_runtime_cli_artifacts_remain_private_across_restart(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_CREATE_DEFAULT_STATE", "1")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        previous_umask = os.umask(0o022)
        try:
            await process.start()
            await process.stop()
            validate_private_config_directory(process.profile.config_dir)
            cache = process.profile.config_dir / "cache"
            assert (cache.stat().st_mode & 0o777) == 0o700
            assert (cache / "runtime-state.json").stat().st_mode & 0o777 == 0o600

            # A second startup performs the strict preflight again and must
            # accept artifacts written by the first generation.
            await process.start()
            await process.stop()
            validate_private_config_directory(process.profile.config_dir)
        finally:
            os.umask(previous_umask)
            await process.stop()

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="owner-only modes are POSIX-specific",
)
def test_runtime_normalizes_cli_remote_meta_created_as_0644(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_REMOTE_META_KIND", "insecure-file")
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=2,
        )
        previous_umask = os.umask(0o022)
        try:
            assert await process.start() == 1
            remote_meta = (
                process.profile.config_dir
                / "cache"
                / "remote_meta.meta.json"
            )
            assert remote_meta.read_text(encoding="utf-8") == "{}"
            assert remote_meta.stat().st_mode & 0o777 == 0o600
            validate_private_config_directory(
                process.profile.config_dir,
                expected_app_id=process.profile.app_id,
            )
        finally:
            os.umask(previous_umask)
            await process.stop()

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="owner-only modes are POSIX-specific",
)
def test_runtime_normalization_preserves_more_restrictive_owner_modes(
    tmp_path,
):
    async def scenario():
        profile = _profile(tmp_path)
        restricted = profile.config_dir / "restricted"
        restricted.mkdir(mode=0o700)
        state = restricted / "state.json"
        state.write_text("{}", encoding="utf-8")
        state.chmod(0o400)
        restricted.chmod(0o500)
        process = LarkCliProcess(
            profile,
            executable=str(_executable(tmp_path)),
            ready_timeout=2,
        )

        try:
            validate_private_config_directory(
                profile.config_dir,
                expected_app_id=profile.app_id,
            )
            assert await process.start() == 1
            assert restricted.stat().st_mode & 0o777 == 0o500
            assert state.stat().st_mode & 0o777 == 0o400
            await process.stop()
            assert restricted.stat().st_mode & 0o777 == 0o500
            assert state.stat().st_mode & 0o777 == 0o400
        finally:
            await process.stop()
            # Restore owner write access so pytest can remove its temporary
            # tree on platforms whose rmtree does not repair modes itself.
            restricted.chmod(0o700)
            state.chmod(0o600)

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="owner-only modes are POSIX-specific",
)
def test_runtime_secures_cli_remote_meta_when_identity_verification_fails(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_REMOTE_META_KIND", "insecure-file")
        monkeypatch.setenv("FAKE_LARK_WHOAMI_APP_ID", "cli_wrong_app")
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=2,
        )

        with pytest.raises(LarkProfileError):
            await process.start()

        remote_meta = (
            process.profile.config_dir / "cache" / "remote_meta.meta.json"
        )
        assert remote_meta.stat().st_mode & 0o777 == 0o600
        validate_private_config_directory(
            process.profile.config_dir,
            expected_app_id=process.profile.app_id,
        )
        assert not process.running

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="secure descriptor chmod is POSIX-specific",
)
@pytest.mark.parametrize("artifact_kind", ["symlink", "hardlink"])
def test_runtime_remote_meta_normalization_rejects_unsafe_reference(
    tmp_path,
    monkeypatch,
    artifact_kind,
):
    async def scenario():
        outside = tmp_path / "outside.json"
        outside.write_text("outside", encoding="utf-8")
        outside.chmod(0o644)
        monkeypatch.setenv("FAKE_LARK_REMOTE_META_KIND", artifact_kind)
        monkeypatch.setenv("FAKE_LARK_REMOTE_META_TARGET", str(outside))
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=2,
        )

        with pytest.raises(LarkProfileError):
            await process.start()

        remote_meta = (
            process.profile.config_dir / "cache" / "remote_meta.meta.json"
        )
        if artifact_kind == "symlink":
            assert remote_meta.is_symlink()
        else:
            assert remote_meta.samefile(outside)
        assert outside.read_text(encoding="utf-8") == "outside"
        assert outside.stat().st_mode & 0o777 == 0o644
        assert not process.running

    asyncio.run(scenario())


def test_profile_environment_scrubs_cli_credential_and_identity_overrides(tmp_path):
    profile = _profile(tmp_path)
    inherited = {
        "PATH": "/bin",
        "LARKSUITE_CLI_CONFIG_DIR": "/wrong",
        "LARKSUITE_CLI_APP_ID": "cli_attacker",
        "LARKSUITE_CLI_APP_SECRET": "secret-value",
        "LARKSUITE_CLI_BRAND": "feishu",
        "LARKSUITE_CLI_USER_ACCESS_TOKEN": "user-token",
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN": "tenant-token",
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN_SOURCE": "env",
        "LARKSUITE_CLI_DEFAULT_AS": "user",
        "LARKSUITE_CLI_DATA_DIR": "/wrong-key-store",
        "LARKSUITE_CLI_PROFILE": "interactive",
        "LARKSUITE_CLI_STRICT_MODE": "off",
        "LARKSUITE_CLI_AUTH_PROXY": "https://secret.invalid",
        "LARKSUITE_CLI_PROXY_KEY": "proxy-secret",
        "OPENCLAW_WORKSPACE": "/wrong-openclaw",
        "OPENCLAW_PROFILE": "foreign",
        "HERMES_HOME": "/wrong-hermes",
        "LARK_CHANNEL": "foreign-channel",
    }
    environment = profile.environment(inherited)
    assert environment["PATH"] == "/bin"
    assert environment[CONFIG_ENVIRONMENT_KEY] == str(profile.config_dir)
    assert environment["LARKSUITE_CLI_NO_UPDATE_NOTIFIER"] == "1"
    assert environment["LARKSUITE_CLI_NO_SKILLS_NOTIFIER"] == "1"
    assert not any(
        key in environment
        for key in inherited
        if key.startswith("LARKSUITE_CLI_")
        and key != CONFIG_ENVIRONMENT_KEY
    )
    assert not any(
        key in environment
        for key in inherited
        if key.startswith("OPENCLAW_") or key.startswith("HERMES_")
    )
    assert "LARK_CHANNEL" not in environment


def test_runtime_rejects_unsafe_nested_cli_config_state(tmp_path):
    config = tmp_path / "config-tree"
    config.mkdir(mode=0o700)
    nested = config / "profiles"
    nested.mkdir(mode=0o700)
    credentials = nested / "bot.json"
    credentials.write_text("{}", encoding="utf-8")
    credentials.chmod(0o644)
    with pytest.raises(LarkProfileError, match="owner-only"):
        validate_private_config_directory(config)

    credentials.chmod(0o600)
    link = nested / "current"
    link.symlink_to(credentials.name)
    with pytest.raises(LarkProfileError, match="symlink"):
        validate_private_config_directory(config)

    link.unlink()
    hardlink = nested / "credential-alias"
    os.link(credentials, hardlink)
    with pytest.raises(LarkProfileError, match="hardlinked"):
        validate_private_config_directory(config)


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
def test_runtime_accepts_exact_private_event_bus_socket_across_generations(
    tmp_path,
):
    async def scenario():
        # macOS has a short sockaddr_un limit, so keep the bound path below
        # /tmp instead of pytest's longer platform-specific temp hierarchy.
        with tempfile.TemporaryDirectory(prefix="cow-lark-", dir="/tmp") as root:
            config = Path(root)
            config.chmod(0o700)
            profile = LarkBotProfile(
                profile_id="process",
                app_id="cli_processbot1",
                bot_open_id="ou_processbot1",
                config_dir=config,
            )
            bus_dir = config / "events" / profile.app_id
            bus_dir.mkdir(parents=True, mode=0o700)
            (config / "events").chmod(0o700)
            bus_dir.chmod(0o700)
            bus_path = bus_dir / "bus.sock"
            bus = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            bus.bind(str(bus_path))
            bus_path.chmod(0o700)
            process = LarkCliProcess(
                profile,
                executable=str(_executable(tmp_path)),
                ready_timeout=2,
            )
            try:
                validate_private_config_directory(
                    config,
                    expected_app_id=profile.app_id,
                )
                assert await process.start() == 1
                await process.stop()

                # The lark-cli daemon intentionally outlives one consumer.
                # A subsequent generation must accept and reuse its socket.
                assert bus_path.exists()
                assert await process.start() == 2
            finally:
                await process.stop()
                bus.close()

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
@pytest.mark.parametrize(
    ("relative_path", "entry_kind", "entry_mode", "message"),
    [
        (("bus.sock",), "socket", 0o700, "special file"),
        (("events", "cli_otherbot1", "bus.sock"), "socket", 0o700, "special file"),
        (("events", "cli_processbot1", "other.sock"), "socket", 0o700, "special file"),
        (("events", "cli_processbot1", "bus.sock"), "regular", 0o600, "invalid type"),
        (("events", "cli_processbot1", "bus.sock"), "fifo", 0o600, "invalid type"),
        (("events", "cli_processbot1", "bus.sock"), "socket", 0o777, "owner-only"),
    ],
)
def test_runtime_rejects_event_socket_impostors(
    relative_path,
    entry_kind,
    entry_mode,
    message,
):
    with tempfile.TemporaryDirectory(prefix="cow-lark-", dir="/tmp") as root:
        config = Path(root)
        config.chmod(0o700)
        entry = config.joinpath(*relative_path)
        entry.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        for parent in (entry.parent, *entry.parents):
            if parent == config.parent:
                break
            parent.chmod(0o700)
            if parent == config:
                break

        bound_socket = None
        if entry_kind == "socket":
            bound_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            bound_socket.bind(str(entry))
        elif entry_kind == "fifo":
            os.mkfifo(entry, mode=entry_mode)
        else:
            entry.write_text("not a bus socket", encoding="utf-8")
        entry.chmod(entry_mode)
        try:
            with pytest.raises(LarkProfileError, match=message):
                validate_private_config_directory(
                    config,
                    expected_app_id="cli_processbot1",
                )
        finally:
            if bound_socket is not None:
                bound_socket.close()


def test_readiness_requires_the_exact_pinned_stderr_marker(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_READY_LINE", "consumer ready and connected")
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=0.1,
            stop_timeout=0.2,
            event_queue_size=3,
        )
        assert process._events.maxsize == 3
        with pytest.raises(LarkProtocolError, match="did not become ready"):
            await process.start()
        assert process._events.maxsize == 3
        assert not process.running

    asyncio.run(scenario())


def test_malformed_stream_terminates_live_child_and_surfaces_promptly(
    tmp_path, monkeypatch
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_MALFORMED", "1")
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=1,
            stop_timeout=0.2,
        )
        await process.start()
        with pytest.raises(LarkProtocolError, match="malformed"):
            await asyncio.wait_for(anext(process.events()), timeout=0.5)
        await asyncio.wait_for(process.stop(), timeout=0.5)
        assert not process.running

    asyncio.run(scenario())


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_consumer_stop_fences_cli_descendants(tmp_path, monkeypatch):
    async def scenario():
        pid_path = tmp_path / "consumer-child.pid"
        monkeypatch.setenv("FAKE_LARK_CHILD_PID_FILE", str(pid_path))
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=1,
            stop_timeout=0.2,
        )
        await process.start()
        child_pid = await _read_pid(pid_path)
        await process.stop()
        await _assert_pid_exits(child_pid)

    asyncio.run(scenario())


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_one_shot_timeout_fences_cli_descendants(tmp_path, monkeypatch):
    async def scenario():
        pid_path = tmp_path / "oneshot-child.pid"
        monkeypatch.setenv("FAKE_LARK_CHILD_PID_FILE", str(pid_path))
        process = LarkCliProcess(
            _profile(tmp_path),
            executable=str(_executable(tmp_path)),
            ready_timeout=1,
            stop_timeout=0.2,
        )
        operation = asyncio.create_task(
            process.run_json("test-child-timeout", timeout=0.5)
        )
        child_pid = await _read_pid(pid_path)
        with pytest.raises(LarkError, match="timed out"):
            await operation
        await _assert_pid_exits(child_pid)

    asyncio.run(scenario())


def test_unsupported_cli_version_fails_before_consumer_start(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_VERSION", "9.9.9")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkCliVersionError, match="unsupported"):
            await process.start()
        assert not process.running

    asyncio.run(scenario())


def test_runtime_accepts_wrapped_verified_bot_identity_compatibility(
    tmp_path, monkeypatch
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_AUTH_WRAPPED", "1")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        await process.start()
        try:
            assert process.running
        finally:
            await process.stop()

    asyncio.run(scenario())


def test_runtime_rejects_identityless_raw_bot_status(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_AUTH_RAW", "1")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkProfileError, match="stable bot identity"):
            await process.start()
        assert not process.running

    asyncio.run(scenario())


@pytest.mark.parametrize("duplicate", ["identity", "appId", "openId", "verified"])
def test_runtime_rejects_duplicate_bot_identity_json_keys(
    tmp_path,
    monkeypatch,
    duplicate,
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_AUTH_DUPLICATE", duplicate)
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkProfileError, match="stable bot identity"):
            await process.start()
        assert not process.running

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "environment_key",
    [
        "FAKE_LARK_AUTH_DIAGNOSTIC_PREFIX",
        "FAKE_LARK_AUTH_CONFLICTING_OBJECTS",
    ],
)
def test_runtime_auth_status_requires_one_complete_json_object(
    tmp_path,
    monkeypatch,
    environment_key,
):
    async def scenario():
        monkeypatch.setenv(environment_key, "1")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkProfileError, match="stable bot identity"):
            await process.start()
        assert not process.running

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("environment_key", "environment_value"),
    [
        ("FAKE_LARK_AUTH_AVAILABLE", "false-string"),
        ("FAKE_LARK_AUTH_CONFLICT_APP_ALIAS", "1"),
        ("FAKE_LARK_WHOAMI_IDENTITY", "user"),
        ("FAKE_LARK_WHOAMI_SOURCE", "auto"),
        ("FAKE_LARK_WHOAMI_APP_ID", "cli_conflicting_app"),
    ],
)
def test_runtime_rejects_untrusted_or_ambiguous_bot_identity_envelopes(
    tmp_path,
    monkeypatch,
    environment_key,
    environment_value,
):
    async def scenario():
        monkeypatch.setenv(environment_key, environment_value)
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkProfileError, match="stable bot identity"):
            await process.start()
        assert not process.running

    asyncio.run(scenario())


def test_runtime_accepts_user_selected_auth_status_with_explicit_bot_probe(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_AUTH_IDENTITY", "user")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        await process.start()
        assert process.running
        await process.stop()

    asyncio.run(scenario())


def test_send_and_reply_commands_match_pinned_shortcut_contract(tmp_path, monkeypatch):
    async def scenario():
        argument_log = tmp_path / "arguments.ndjson"
        monkeypatch.setenv("FAKE_LARK_ARGS_FILE", str(argument_log))
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        direct = ReplyTarget(
            channel="lark",
            bot_id="cli_processbot1",
            external_user_id="ou_target1234",
            destination_kind="open_id",
            destination_id="ou_target1234",
        )
        direct_markdown = "## Result\n\n- passed\n- [details](https://example.com)"
        await process.send_text(
            direct,
            direct_markdown,
            idempotency_key="idem-direct",
        )
        group = ReplyTarget(
            channel="lark",
            bot_id="cli_processbot1",
            external_user_id="ou_target1234",
            destination_kind="group",
            destination_id="oc_chat1234",
        )
        await process.send_text(
            group,
            "**group update**",
            idempotency_key="idem-group",
        )
        reply = ReplyTarget(
            channel="lark",
            bot_id="cli_processbot1",
            external_user_id="ou_target1234",
            source_message_id="om_source5678",
            destination_kind="chat",
            destination_id="oc_chat1234",
        )
        await process.send_text(
            reply,
            "1. first\n2. second",
            idempotency_key="idem-reply",
        )
        thread = ReplyTarget(
            channel="lark",
            bot_id="cli_processbot1",
            external_user_id="ou_target1234",
            source_message_id="om_source1234",
            destination_kind="thread",
            destination_id="oc_chat1234",
            thread_id="om_root1234",
        )
        reply_markdown = "```text\nreply\n```"
        await process.send_text(
            thread,
            reply_markdown,
            idempotency_key="idem-thread",
        )
        calls = [json.loads(line) for line in argument_log.read_text().splitlines()]
        assert calls[0] == [
            "im",
            "+messages-send",
            "--user-id",
            "ou_target1234",
            "--markdown",
            direct_markdown,
            "--as",
            "bot",
            "--idempotency-key",
            "idem-direct",
        ]
        assert calls[1] == [
            "im",
            "+messages-send",
            "--chat-id",
            "oc_chat1234",
            "--markdown",
            "**group update**",
            "--as",
            "bot",
            "--idempotency-key",
            "idem-group",
        ]
        assert calls[2] == [
            "im",
            "+messages-reply",
            "--message-id",
            "om_source5678",
            "--markdown",
            "1. first\n2. second",
            "--as",
            "bot",
            "--idempotency-key",
            "idem-reply",
        ]
        assert calls[3] == [
            "im",
            "+messages-reply",
            "--message-id",
            "om_source1234",
            "--markdown",
            reply_markdown,
            "--reply-in-thread",
            "--as",
            "bot",
            "--idempotency-key",
            "idem-thread",
        ]

    asyncio.run(scenario())


def test_structured_stderr_rate_limit_is_classified(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_ERROR", "rate-limit")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkRateLimitError) as raised:
            await process.send_text(
                ReplyTarget(
                    channel="lark",
                    bot_id="cli_processbot1",
                    external_user_id="ou_target1234",
                    destination_kind="open_id",
                    destination_id="ou_target1234",
                ),
                "hello",
                idempotency_key="idem-rate-limit",
            )
        assert raised.value.retry_after == 7

    asyncio.run(scenario())


def test_text_send_rejects_success_without_valid_message_id(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("FAKE_LARK_ERROR", "missing-ack")
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        with pytest.raises(LarkProtocolError, match="valid message ID"):
            await process.send_text(
                ReplyTarget(
                    channel="lark",
                    bot_id="cli_processbot1",
                    external_user_id="ou_target1234",
                    destination_kind="open_id",
                    destination_id="ou_target1234",
                ),
                "hello",
                idempotency_key="idem-missing-ack",
            )

    asyncio.run(scenario())


def test_media_send_rejects_success_without_valid_message_id(tmp_path):
    async def scenario():
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )

        async def missing_ack(*_args, **_kwargs):
            return {"ok": True, "data": {}}

        process.run_json = missing_ack
        with pytest.raises(LarkProtocolError, match="valid message ID"):
            await process.send_media(
                ReplyTarget(
                    channel="lark",
                    bot_id="cli_processbot1",
                    external_user_id="ou_target1234",
                    destination_kind="open_id",
                    destination_id="ou_target1234",
                ),
                "img_uploaded1234",
                kind="image",
                idempotency_key="idem-media-missing-ack",
            )

    asyncio.run(scenario())


def test_attachment_download_validates_metadata_and_size_before_read(tmp_path):
    async def scenario():
        profile = _profile(tmp_path)
        process = LarkCliProcess(
            profile, executable=str(_executable(tmp_path)), ready_timeout=2
        )

        async def downloaded(*_args, **kwargs):
            output = Path(kwargs["cwd"]) / "actual.bin"
            output.write_bytes(b"oversized")
            return {
                "ok": True,
                "data": {
                    "saved_path": "./actual.bin",
                    "size_bytes": len(b"oversized"),
                },
            }

        process.run_json = downloaded
        envelope = InboundEnvelope(
            channel="lark",
            bot_id=profile.app_id,
            external_user_id="ou_target1234",
            external_message_id="om_downloadsource1",
            text="",
        )
        with pytest.raises(LarkPermanentDeliveryError, match="file-size limit"):
            await process.download_attachment(
                profile,
                envelope,
                {
                    "kind": "file",
                    "remote_id": "file_resource1234",
                    "filename": "requested.bin",
                    "__maximum_bytes": 4,
                },
            )

    asyncio.run(scenario())


def test_upload_rejects_a_symlink_before_invoking_cli(tmp_path):
    async def scenario():
        process = LarkCliProcess(
            _profile(tmp_path), executable=str(_executable(tmp_path)), ready_timeout=2
        )
        target = tmp_path / "payload.txt"
        target.write_text("payload", encoding="utf-8")
        link = tmp_path / "payload-link.txt"
        link.symlink_to(target)
        with pytest.raises(LarkPermanentDeliveryError, match="unavailable"):
            await process.upload_media(link, kind="file")

    asyncio.run(scenario())
