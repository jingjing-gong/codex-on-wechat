"""Terminal Lark onboarding and profile-management contract tests."""

from __future__ import annotations

import asyncio
import io
import json
import os
import socket
import sqlite3
import stat
import subprocess
import tempfile
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import lark_cli as lark_cli_module
from src.lark_cli import (
    LARK_PROFILE_SCHEMA_VERSION,
    MAX_APP_SECRET_BYTES,
    OWNER_CLI_MAX_RUNTIME_SCHEMA_VERSION,
    PINNED_LARK_CLI_VERSION,
    LarkCliVersionError,
    LarkProfileAdmin,
    LarkProfileError,
    LarkProfileStoreUnavailable,
    _config_for_profile,
    _run_cli_one_shot,
    _structured_init_result,
    _subprocess_environment,
    _tighten_owned_staging_tree,
    build_parser,
    normalize_profile_name,
    run_config_init,
    verify_lark_cli_version,
)
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.supervisor import (
    ChannelAccountOwnership,
    DatabaseOwnership,
    SupervisorOwnershipConflict,
)


FAKE_LARK_CLI = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import time

args = sys.argv[1:]
helper_pid_path = os.environ.get("FAKE_LARK_HELPER_PID_FILE")
if helper_pid_path:
    helper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(helper_pid_path).write_text(str(helper.pid), encoding="utf-8")
    if os.environ.get("FAKE_LARK_HELPER_PARENT_MODE") == "hang":
        time.sleep(60)
if args == ["--version"]:
    print("lark-cli version " + os.environ.get("FAKE_LARK_VERSION", "1.0.92"))
    raise SystemExit(0)

config_dir = Path(os.environ["LARKSUITE_CLI_CONFIG_DIR"])
mode = os.environ.get("FAKE_LARK_MODE", "success")
if args[:2] == ["config", "init"]:
    argv_log = os.environ.get("FAKE_LARK_ARGV_LOG")
    if argv_log:
        Path(argv_log).write_text(json.dumps(args), encoding="utf-8")

if (
    len(args) == 7
    and args[:3] == ["config", "init", "--app-id"]
    and args[4] == "--app-secret-stdin"
    and args[5] == "--brand"
):
    requested_app_id = args[3]
    requested_brand = args[6]
    secret_stdin = sys.stdin.read()
    stdin_log = os.environ.get("FAKE_LARK_STDIN_LOG")
    if stdin_log:
        capture = Path(stdin_log)
        capture.write_text(secret_stdin, encoding="utf-8")
        capture.chmod(0o600)
    secret_value = secret_stdin.rstrip("\r\n")
    argv_leaked = bool(secret_value) and any(
        secret_value in argument for argument in args
    )
    env_leaked = bool(secret_value) and any(
        secret_value in value for value in os.environ.values()
    )
    transport_log = os.environ.get("FAKE_LARK_TRANSPORT_LOG")
    if transport_log:
        Path(transport_log).write_text(
            json.dumps({
                "argv_leaked": argv_leaked,
                "env_leaked": env_leaked,
            }),
            encoding="utf-8",
        )
    if argv_leaked or env_leaked:
        print("credential escaped stdin transport")
        raise SystemExit(12)

    init_log = os.environ.get("FAKE_LARK_INIT_LOG")
    if init_log:
        with open(init_log, "a", encoding="utf-8") as handle:
            handle.write(str(config_dir) + "\n")
    if mode == "existing-empty-after-keychain":
        # Model a CLI that mutates its process-global keychain entry before it
        # can persist config.json. The owner must not infer that an empty
        # staging directory means no external credential was touched.
        print("opaque child diagnostic: " + secret_value, flush=True)
        raise SystemExit(7)
    config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(config_dir, 0o700)
    if mode == "existing-invalid-config-after-keychain":
        invalid_config = config_dir / "config.json"
        invalid_config.write_text('{"apps": [', encoding="utf-8")
        os.chmod(invalid_config, 0o600)
        print("opaque child diagnostic: " + secret_value, flush=True)
        raise SystemExit(7)
    app_id = os.environ.get("FAKE_LARK_APP_ID", requested_app_id)
    brand = os.environ.get("FAKE_LARK_BRAND", requested_brand)
    payload = {
        "apps": [{
            "appId": app_id,
            "appSecret": {
                "source": "keychain",
                "id": "appsecret:" + app_id,
            },
            "brand": brand,
            "users": [],
            "marker": os.environ.get("FAKE_LARK_GENERATION", "one"),
        }]
    }
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(config_path, 0o600)
    # Exercise the owner's redaction boundary with the exact credential the
    # caller supplied. The fake's private capture proves transport separately.
    print("opaque child diagnostic: " + secret_value, flush=True)
    print(json.dumps({
        "appId": app_id,
        "appSecret": secret_value,
        "brand": brand,
    }), flush=True)
    if mode == "existing-fail":
        print("app_secret=" + secret_value, flush=True)
        raise SystemExit(7)
    raise SystemExit(0)

if args == ["config", "init", "--new"]:
    init_log = os.environ.get("FAKE_LARK_INIT_LOG")
    if init_log:
        with open(init_log, "a", encoding="utf-8") as handle:
            handle.write(str(config_dir) + "\n")
    print("Scan this QR:", flush=True)
    print("██  ██", flush=True)
    print("https://open.feishu.cn/page/cli?user_code=TEST-CODE", flush=True)
    if mode == "timeout":
        time.sleep(30)
    if mode == "fail":
        print("app_secret=should-never-leak", flush=True)
        raise SystemExit(7)
    config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(config_dir, 0o700)
    umask_probe = os.environ.get("FAKE_LARK_UMASK_PROBE")
    if umask_probe:
        Path(umask_probe).write_text("private child state", encoding="utf-8")
    app_id = os.environ.get("FAKE_LARK_APP_ID", "cli_test_app")
    brand = os.environ.get("FAKE_LARK_BRAND", "feishu")
    if mode == "plaintext":
        secret = "plain-secret-value"
    elif mode == "null-secret":
        secret = None
    elif mode == "relative-file":
        secret_path = config_dir / "app-secret"
        secret_path.write_text("file-secret-value", encoding="utf-8")
        os.chmod(secret_path, 0o600)
        secret = {"source": "file", "id": "app-secret"}
    elif mode == "absolute-file":
        secret_path = config_dir / "app-secret"
        secret_path.write_text("file-secret-value", encoding="utf-8")
        os.chmod(secret_path, 0o600)
        secret = {"source": "file", "id": str(secret_path.absolute())}
    elif mode == "parent-file":
        secret_path = config_dir / "app-secret"
        secret_path.write_text("file-secret-value", encoding="utf-8")
        os.chmod(secret_path, 0o600)
        secret = {
            "source": "file",
            "id": "../" + config_dir.name + "/app-secret",
        }
    else:
        secret = {"source": "keychain", "id": "appsecret:" + app_id}
    payload = {
        "apps": [{
            "appId": app_id,
            "appSecret": secret,
            "brand": brand,
            "users": [],
            "marker": os.environ.get("FAKE_LARK_GENERATION", "one"),
        }]
    }
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(config_path, 0o644 if mode == "insecure" else 0o600)
    if mode == "truncated-config":
        config_path.write_text('{"apps": [', encoding="utf-8")
    if mode == "hardlink":
        os.link(config_path, config_dir / "config-alias.json")
    if mode == "insecure":
        cache_dir = config_dir / "cache"
        cache_dir.mkdir(mode=0o755)
        os.chmod(cache_dir, 0o755)
        cache_file = cache_dir / "remote-state.json"
        cache_file.write_text("{}", encoding="utf-8")
        os.chmod(cache_file, 0o644)
    if mode == "malformed":
        print("onboarding complete but no structured result", flush=True)
    else:
        # Deliberately include a secret-shaped field. The wrapper must relay a
        # redacted value while parsing appId/brand from the unmodified capture.
        init_result = {
            "appId": app_id,
            "appSecret": "should-never-leak",
            "brand": brand,
        }
        indent = 2 if os.environ.get("FAKE_LARK_INIT_PRETTY") == "1" else None
        print(json.dumps(init_result, indent=indent), flush=True)
        if os.environ.get("FAKE_LARK_INIT_CONFLICT") == "1":
            print(json.dumps({
                "appId": "cli_conflicting_init_app",
                "appSecret": "second-secret-must-never-leak",
                "brand": brand,
            }, indent=indent), flush=True)
    raise SystemExit(0)

if args == ["auth", "status", "--verify", "--json"]:
    config_path = config_dir / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if os.environ.get("FAKE_LARK_AUTH_ADD_HARDLINK") == "1":
        os.link(config_path, config_dir / "post-auth-config-alias.json")
    app = config["apps"][0]
    available = os.environ.get("FAKE_LARK_AUTH_AVAILABLE", "true")
    if available == "true":
        available = True
    elif available == "false":
        available = False
    payload = {
        "appId": os.environ.get("FAKE_LARK_AUTH_APP_ID", app["appId"]),
        "brand": os.environ.get("FAKE_LARK_AUTH_BRAND", app["brand"]),
        "identities": {
            "bot": {
                "status": "ready",
                "available": available,
                "verified": True,
                "openId": os.environ.get("FAKE_LARK_BOT_OPEN_ID", "ou_test_bot"),
            },
            "user": {"status": "not_configured", "available": False},
        },
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
    print(rendered)
    raise SystemExit(0)

if args == ["whoami", "--as", "bot"]:
    config = json.loads((config_dir / "config.json").read_text(encoding="utf-8"))
    app = config["apps"][0]
    print(json.dumps({
        "appId": os.environ.get("FAKE_LARK_WHOAMI_APP_ID", app["appId"]),
        "available": os.environ.get("FAKE_LARK_WHOAMI_AVAILABLE", "true") == "true",
        "brand": os.environ.get("FAKE_LARK_WHOAMI_BRAND", app["brand"]),
        "defaultAs": "auto",
        "identity": os.environ.get("FAKE_LARK_WHOAMI_IDENTITY", "bot"),
        "identitySource": os.environ.get("FAKE_LARK_WHOAMI_SOURCE", "flag"),
        "profile": app["appId"],
        "tokenStatus": os.environ.get("FAKE_LARK_WHOAMI_TOKEN_STATUS", "ready"),
    }))
    raise SystemExit(0)

if args == ["config", "remove"]:
    log = os.environ.get("FAKE_LARK_REMOVE_LOG")
    if log:
        with open(log, "a", encoding="utf-8") as handle:
            handle.write(str(config_dir) + "\n")
    fail_once = os.environ.get("FAKE_LARK_REMOVE_FAIL_ONCE")
    if fail_once:
        marker = Path(fail_once)
        if not marker.exists():
            marker.write_text("failed", encoding="utf-8")
            if os.environ.get("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE"):
                config_path = config_dir / "config.json"
                if config_path.exists():
                    config_path.unlink()
            print("temporary credential backend failure")
            raise SystemExit(8)
    config_path = config_dir / "config.json"
    if config_path.exists():
        config_path.unlink()
    print("Configuration removed")
    raise SystemExit(0)

print("unexpected args: " + repr(args))
raise SystemExit(9)
'''


class MemoryProfileStore:
    def __init__(self) -> None:
        self.profiles = {}
        self.removed = set()
        self.statuses = {}
        self.initialize_calls = 0
        self.close_calls = 0

    async def initialize(self) -> None:
        self.initialize_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def create_bot_profile(self, profile):
        if profile.profile_id in self.profiles and profile.profile_id not in self.removed:
            raise LarkProfileError("duplicate profile id")
        if any(
            item.bot_id == profile.bot_id and key not in self.removed
            for key, item in self.profiles.items()
        ):
            raise LarkProfileError("duplicate live app id")
        self.profiles[profile.profile_id] = profile
        self.removed.discard(profile.profile_id)
        self.statuses[profile.profile_id] = SimpleNamespace(
            onboarding_state="registered",
            connection_state="disconnected",
        )
        return profile

    async def get_bot_profile(self, profile_id, *, include_removed=False):
        if profile_id in self.removed and not include_removed:
            return None
        return self.profiles.get(profile_id)

    async def list_bot_profiles(
        self, *, channel=None, enabled=None, include_removed=False
    ):
        values = []
        for key, profile in sorted(self.profiles.items()):
            if key in self.removed and not include_removed:
                continue
            if channel is not None and profile.channel != channel:
                continue
            if enabled is not None and profile.enabled is not enabled:
                continue
            values.append(profile)
        return values

    async def set_bot_profile_enabled(self, profile_id, enabled, *, now=None):
        del now
        profile = await self.get_bot_profile(profile_id)
        if profile is None:
            return False
        self.profiles[profile_id] = replace(
            profile,
            enabled=bool(enabled),
            updated_at=datetime.now(timezone.utc),
        )
        return True

    async def update_bot_profile_credentials(
        self,
        profile_id,
        *,
        credential_ref,
        cli_version=None,
        restart_policy=None,
        now=None,
    ):
        del now
        profile = await self.get_bot_profile(profile_id)
        if profile is None:
            raise LarkProfileError("profile missing")
        updated = replace(
            profile,
            credential_ref=credential_ref,
            cli_version=cli_version or profile.cli_version,
            restart_policy=(
                dict(restart_policy)
                if restart_policy is not None
                else profile.restart_policy
            ),
            updated_at=datetime.now(timezone.utc),
        )
        self.profiles[profile_id] = updated
        return updated

    async def remove_bot_profile(self, profile_id, *, now=None):
        profile = await self.get_bot_profile(profile_id)
        if profile is None:
            return False
        self.profiles[profile_id] = replace(
            profile,
            enabled=False,
            removed_at=now or datetime.now(timezone.utc),
        )
        self.removed.add(profile_id)
        return True

    async def get_bot_profile_status(self, profile_id):
        return self.statuses.get(profile_id)


class CommitThenCancelProfileStore(MemoryProfileStore):
    async def create_bot_profile(self, profile):
        await super().create_bot_profile(profile)
        raise asyncio.CancelledError()


def _fake_binary(tmp_path: Path) -> Path:
    binary = tmp_path / "lark-cli"
    binary.write_text(FAKE_LARK_CLI, encoding="utf-8")
    binary.chmod(0o755)
    return binary


def _admin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    store=None,
    output=None,
    input_stream=None,
) -> tuple[LarkProfileAdmin, MemoryProfileStore, io.StringIO]:
    binary = _fake_binary(tmp_path)
    store = store or MemoryProfileStore()
    output = output or io.StringIO()
    state = tmp_path / "lark-state"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=tmp_path / "runtime.sqlite",
        config_root=state,
        binary=str(binary),
        store_factory=lambda _path: store,
        output=output,
        input_stream=input_stream,
    )
    return admin, store, output


def _bind_private_event_bus_socket(profile_path: Path, app_id: str) -> socket.socket:
    bus_dir = profile_path / "events" / app_id
    bus_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    (profile_path / "events").chmod(0o700)
    bus_dir.chmod(0o700)
    bus_path = bus_dir / "bus.sock"
    bound = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    bound.bind(str(bus_path))
    bus_path.chmod(0o700)
    return bound


def test_pinned_version_is_required(tmp_path: Path, monkeypatch) -> None:
    binary = _fake_binary(tmp_path)
    assert verify_lark_cli_version(str(binary)) == PINNED_LARK_CLI_VERSION

    monkeypatch.setenv("FAKE_LARK_VERSION", "1.0.91")
    with pytest.raises(LarkCliVersionError, match="required 1.0.92"):
        verify_lark_cli_version(str(binary))


def test_app_owner_inspection_uses_isolated_bot_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "staged-profile"
    calls: list[tuple[tuple[str, ...], Path | None, float]] = []

    def run_cli(
        binary: str,
        *arguments: str,
        config_dir: Path | None = None,
        timeout: float,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del binary, cwd
        calls.append((arguments, config_dir, timeout))
        return subprocess.CompletedProcess(
            list(arguments),
            0,
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {
                                "type": 2,
                                "owner_id": "ou_test_owner",
                                "name": "ignored",
                            },
                            "app_name": "ignored",
                        }
                    },
                }
            ),
        )

    monkeypatch.setattr(lark_cli_module, "_run_cli_one_shot", run_cli)
    provisioned = lark_cli_module.ProvisionedConfig(
        app_id="cli_test_app",
        brand="feishu",
        credential_ref="keychain:appsecret:cli_test_app",
        bot_open_id="ou_test_bot",
    )

    verified = lark_cli_module.inspect_app_owner_identity(
        "lark-cli",
        config_dir,
        provisioned,
    )

    assert verified.owner_open_id == "ou_test_owner"
    assert calls == [
        (
            (
                "api",
                "GET",
                "/open-apis/application/v6/applications/me",
                "--params",
                '{"lang":"zh_cn","user_id_type":"open_id"}',
                "--as",
                "bot",
                "--format",
                "json",
            ),
            config_dir,
            20,
        )
    ]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (
            '{"ok":true,"identity":"bot","data":{"app":{'
            '"app_id":"cli_test_app","creator_id":"ou_test_owner",'
            '"status":1,"scene_type":0,"owner":{"type":2,'
            '"owner_id":"ou_test_owner","owner_id":"ou_forged_owner"}}}}',
            "ambiguous app-owner output",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "user",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {"type": 2, "owner_id": "ou_test_owner"},
                        }
                    },
                }
            ),
            "app-owner verification failed",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_other_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {"type": 2, "owner_id": "ou_test_owner"},
                        }
                    },
                }
            ),
            "conflicts with the provisioned app",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {"type": 2, "owner_id": "ou_other_owner"},
                        }
                    },
                }
            ),
            "one verified human app owner",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {"type": 1, "owner_id": "ou_test_owner"},
                        }
                    },
                }
            ),
            "verified human app owner",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_bot",
                            "status": 1,
                            "scene_type": 0,
                            "owner": {"type": 2, "owner_id": "ou_test_bot"},
                        }
                    },
                }
            ),
            "verified human app creator",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 0,
                            "scene_type": 0,
                            "owner": {"type": 2, "owner_id": "ou_test_owner"},
                        }
                    },
                }
            ),
            "enabled custom app",
        ),
        (
            json.dumps(
                {
                    "ok": True,
                    "identity": "bot",
                    "data": {
                        "app": {
                            "app_id": "cli_test_app",
                            "creator_id": "ou_test_owner",
                            "status": 1,
                            "scene_type": 0,
                        }
                    },
                }
            ),
            "verified human app owner",
        ),
    ],
    ids=[
        "duplicate-owner-id",
        "user-envelope",
        "different-app",
        "creator-owner-conflict",
        "non-human-owner",
        "creator-is-bot",
        "disabled-app",
        "feishu-owner-absent",
    ],
)
def test_app_owner_inspection_rejects_ambiguous_or_conflicting_identity(
    monkeypatch,
    tmp_path: Path,
    raw: str,
    message: str,
) -> None:
    monkeypatch.setattr(
        lark_cli_module,
        "_run_cli_one_shot",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, raw),
    )
    provisioned = lark_cli_module.ProvisionedConfig(
        app_id="cli_test_app",
        brand="feishu",
        credential_ref="keychain:appsecret:cli_test_app",
        bot_open_id="ou_test_bot",
    )

    with pytest.raises(LarkProfileError, match=message):
        lark_cli_module.inspect_app_owner_identity(
            "lark-cli",
            tmp_path,
            provisioned,
        )


def test_lark_app_owner_inspection_accepts_documented_creator_without_owner(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw = json.dumps(
        {
            "ok": True,
            "identity": "bot",
            "data": {
                "app": {
                    "app_id": "cli_test_app",
                    "creator_id": "ou_test_owner",
                    "status": 1,
                    "scene_type": 0,
                }
            },
        }
    )
    monkeypatch.setattr(
        lark_cli_module,
        "_run_cli_one_shot",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, raw),
    )
    provisioned = lark_cli_module.ProvisionedConfig(
        app_id="cli_test_app",
        brand="lark",
        credential_ref="keychain:appsecret:cli_test_app",
        bot_open_id="ou_test_bot",
    )

    verified = lark_cli_module.inspect_app_owner_identity(
        "lark-cli",
        tmp_path,
        provisioned,
    )

    assert verified.owner_open_id == "ou_test_owner"


def test_lark_app_owner_inspection_rejects_conflicting_optional_owner(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw = json.dumps(
        {
            "ok": True,
            "identity": "bot",
            "data": {
                "app": {
                    "app_id": "cli_test_app",
                    "creator_id": "ou_test_owner",
                    "status": 1,
                    "scene_type": 0,
                    "owner": {"type": 2, "owner_id": "ou_other_owner"},
                }
            },
        }
    )
    monkeypatch.setattr(
        lark_cli_module,
        "_run_cli_one_shot",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, raw),
    )
    provisioned = lark_cli_module.ProvisionedConfig(
        app_id="cli_test_app",
        brand="lark",
        credential_ref="keychain:appsecret:cli_test_app",
        bot_open_id="ou_test_bot",
    )

    with pytest.raises(LarkProfileError, match="verified human app owner"):
        lark_cli_module.inspect_app_owner_identity(
            "lark-cli",
            tmp_path,
            provisioned,
        )


def test_recover_parser_accepts_staging_name_and_optional_profile_name() -> None:
    inferred = build_parser().parse_args(
        ["recover", ".add-team-deadbeef"]
    )
    assert inferred.command == "recover"
    assert inferred.staging_name == ".add-team-deadbeef"
    assert inferred.profile_name is None

    renamed = build_parser().parse_args(
        ["recover", ".add-source-cafebabe", "destination"]
    )
    assert renamed.command == "recover"
    assert renamed.staging_name == ".add-source-cafebabe"
    assert renamed.profile_name == "destination"


def _assert_process_exits(pid: int) -> None:
    for _ in range(200):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.01)
    raise AssertionError(f"lark-cli helper remained alive: {pid}")


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_owner_one_shot_timeout_fences_cli_descendants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    config_dir = tmp_path / "timeout-config"
    config_dir.mkdir(mode=0o700)
    pid_path = tmp_path / "timeout-helper.pid"
    monkeypatch.setenv("FAKE_LARK_HELPER_PID_FILE", str(pid_path))
    monkeypatch.setenv("FAKE_LARK_HELPER_PARENT_MODE", "hang")

    with pytest.raises(subprocess.TimeoutExpired):
        _run_cli_one_shot(
            str(binary),
            "helper-test",
            config_dir=config_dir,
            timeout=0.5,
        )

    helper_pid = int(pid_path.read_text(encoding="utf-8"))
    _assert_process_exits(helper_pid)


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_owner_one_shot_success_fences_detached_cli_descendants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    config_dir = tmp_path / "completed-config"
    config_dir.mkdir(mode=0o700)
    pid_path = tmp_path / "completed-helper.pid"
    monkeypatch.setenv("FAKE_LARK_HELPER_PID_FILE", str(pid_path))

    result = _run_cli_one_shot(
        str(binary),
        "helper-test",
        config_dir=config_dir,
        timeout=1,
    )

    assert result.returncode == 9
    assert "unexpected args" in result.stdout
    helper_pid = int(pid_path.read_text(encoding="utf-8"))
    _assert_process_exits(helper_pid)


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_config_init_success_fences_detached_cli_descendants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    config_dir = tmp_path / "config-init"
    config_dir.mkdir(mode=0o700)
    pid_path = tmp_path / "config-init-helper.pid"
    monkeypatch.setenv("FAKE_LARK_HELPER_PID_FILE", str(pid_path))

    raw = run_config_init(
        str(binary),
        config_dir,
        output=io.StringIO(),
        timeout=2,
    )

    assert "cli_test_app" in raw
    helper_pid = int(pid_path.read_text(encoding="utf-8"))
    _assert_process_exits(helper_pid)


def test_config_init_callback_failure_terminates_spawned_child(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class SpawnedProcess:
        def __init__(self) -> None:
            self.stdin = None
            self.stdout = io.StringIO("")
            self.returncode = None
            self.pid = 999_998

    process = SpawnedProcess()
    terminated: list[object] = []
    monkeypatch.setattr(
        lark_cli_module.subprocess,
        "Popen",
        lambda *arguments, **options: process,
    )
    monkeypatch.setattr(
        lark_cli_module,
        "_terminate_process",
        lambda spawned: terminated.append(spawned),
    )

    def fail_spawn_marker() -> None:
        raise RuntimeError("spawn marker failed")

    with pytest.raises(RuntimeError, match="spawn marker failed"):
        run_config_init(
            "lark-cli",
            tmp_path,
            output=io.StringIO(),
            timeout=2,
            on_spawn=fail_spawn_marker,
        )

    assert terminated == [process]
    assert process.stdout.closed


def test_existing_config_init_redacts_unlabelled_exact_secret_in_capture(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    config_dir = tmp_path / "existing-config-init"
    config_dir.mkdir(mode=0o700)
    secret = "unlabelled-output-secret"

    raw = run_config_init(
        str(binary),
        config_dir,
        output=io.StringIO(),
        timeout=2,
        app_id="cli_existing_app",
        brand="feishu",
        secret_input=io.StringIO(secret + "\n"),
    )

    assert secret not in raw
    assert "opaque child diagnostic: <redacted>" in raw
    assert '"appSecret": "<redacted>"' in raw


class _TTYStringIO(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.mark.parametrize(
    ("secret_input", "message"),
    [
        (_TTYStringIO("tty-secret\n"), "requires piped standard input"),
        (io.StringIO(""), "is empty"),
        (io.StringIO("line-one\nline-two\n"), "exactly one line"),
        (
            io.StringIO("x" * (MAX_APP_SECRET_BYTES + 1)),
            "exceeded its size limit",
        ),
    ],
    ids=["tty", "empty-eof", "multiline", "oversized"],
)
def test_existing_config_init_rejects_unsafe_secret_input_before_child_start(
    tmp_path: Path,
    monkeypatch,
    secret_input: io.StringIO,
    message: str,
) -> None:
    binary = _fake_binary(tmp_path)
    config_dir = tmp_path / "invalid-existing-config-init"
    config_dir.mkdir(mode=0o700)
    argv_log = tmp_path / "argv.json"
    monkeypatch.setenv("FAKE_LARK_ARGV_LOG", str(argv_log))

    with pytest.raises(LarkProfileError, match=message):
        run_config_init(
            str(binary),
            config_dir,
            output=io.StringIO(),
            timeout=2,
            app_id="cli_existing_app",
            brand="feishu",
            secret_input=secret_input,
        )

    assert not argv_log.exists()


def test_existing_config_init_converts_stdin_close_failure_to_safe_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    secret = "pipe-close-secret"

    class ClosingChildStdin:
        def write(self, value: str) -> int:
            return len(value)

        def flush(self) -> None:
            pass

        def close(self) -> None:
            raise BrokenPipeError("child closed credential pipe")

    class ClosedPipeProcess:
        def __init__(self) -> None:
            self.stdin = ClosingChildStdin()
            self.stdout = io.StringIO("")
            self.returncode = 0
            self.pid = 999_999

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            del timeout
            return self.returncode

    process = ClosedPipeProcess()
    monkeypatch.setattr(
        lark_cli_module.subprocess,
        "Popen",
        lambda *arguments, **options: process,
    )
    monkeypatch.setattr(lark_cli_module, "_terminate_process", lambda _process: None)
    output = io.StringIO()

    with pytest.raises(LarkProfileError) as raised:
        run_config_init(
            "lark-cli",
            tmp_path,
            output=output,
            timeout=2,
            app_id="cli_pipe_close_app",
            brand="feishu",
            secret_input=io.StringIO(secret + "\n"),
        )

    assert secret not in str(raised.value)
    assert secret not in output.getvalue()


@pytest.mark.parametrize(
    "value",
    ["", ".hidden", "two words", "../escape", "slash/name", "x" * 65],
)
def test_profile_names_are_path_safe(value: str) -> None:
    with pytest.raises(LarkProfileError, match="profile name"):
        normalize_profile_name(value)


def test_add_relays_qr_redacts_secrets_and_registers_atomically(
    tmp_path: Path, monkeypatch
) -> None:
    argv_log = tmp_path / "qr-argv.json"
    monkeypatch.setenv("FAKE_LARK_ARGV_LOG", str(argv_log))
    admin, store, output = _admin(tmp_path, monkeypatch)

    created = admin.add("team")

    assert created.profile_id == "team"
    assert created.bot_id == "cli_test_app"
    assert created.brand == "feishu"
    assert created.channel == "lark"
    assert created.credential_ref == "keychain:appsecret:cli_test_app"
    assert created.restart_policy["bot_open_id"] == "ou_test_bot"
    config_dir = tmp_path / "lark-state" / "team"
    assert Path(created.config_dir) == config_dir
    assert stat.S_IMODE(config_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((config_dir / "config.json").stat().st_mode) == 0o600
    assert not list((tmp_path / "lark-state").glob(".add-*"))
    relayed = output.getvalue()
    assert "██  ██" in relayed
    assert "https://open.feishu.cn/page/cli?user_code=TEST-CODE" in relayed
    assert "should-never-leak" not in relayed
    assert "<redacted>" in relayed
    assert store.profiles["team"] == created
    assert json.loads(argv_log.read_text(encoding="utf-8")) == [
        "config",
        "init",
        "--new",
    ]


def test_add_parser_exposes_existing_app_stdin_mode_without_changing_qr_mode() -> None:
    parser = build_parser()

    qr = parser.parse_args(["add", "qr-team"])
    skipped = parser.parse_args(["add", "unmapped-team", "--without-owner"])
    existing = parser.parse_args(
        [
            "add",
            "existing-team",
            "--app-id",
            "cli_existing_app",
            "--brand",
            "lark",
            "--app-secret-stdin",
            "--owner-open-id",
            "ou_existing_owner",
        ]
    )

    assert qr.command == "add"
    assert qr.profile_name == "qr-team"
    assert qr.app_id is None
    assert qr.app_secret_stdin is False
    assert qr.owner_open_id is None
    assert qr.without_owner is False
    assert skipped.owner_open_id is None
    assert skipped.without_owner is True
    assert existing.command == "add"
    assert existing.profile_name == "existing-team"
    assert existing.app_id == "cli_existing_app"
    assert existing.brand == "lark"
    assert existing.app_secret_stdin is True
    assert existing.owner_open_id == "ou_existing_owner"
    assert existing.without_owner is False
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "add",
                "invalid-brand",
                "--app-id",
                "cli_existing_app",
                "--brand",
                "wechat",
                "--app-secret-stdin",
                "--owner-open-id",
                "ou_existing_owner",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "add",
                "conflicting-owner-choice",
                "--owner-open-id",
                "ou_existing_owner",
                "--without-owner",
            ]
        )


def test_main_forwards_existing_app_flags_and_preserves_flagless_qr_add(
    monkeypatch,
) -> None:
    calls: list[tuple[str | None, dict[str, object]]] = []

    class RecordingAdmin:
        def add(self, profile_name=None, **options):
            calls.append((profile_name, options))

    monkeypatch.setattr(lark_cli_module, "LarkProfileAdmin", RecordingAdmin)

    assert lark_cli_module.main(["add", "qr-team"]) == 0
    assert lark_cli_module.main(["add", "unmapped-team", "--without-owner"]) == 0
    assert (
        lark_cli_module.main(
            [
                "add",
                "existing-team",
                "--app-id",
                "cli_existing_app",
                "--brand",
                "lark",
                "--app-secret-stdin",
                "--owner-open-id",
                "ou_existing_owner",
            ]
        )
        == 0
    )
    assert calls == [
        (
            "qr-team",
            {
                "app_id": None,
                "brand": None,
                "app_secret_stdin": False,
                "owner_open_id": None,
                "without_owner": False,
            },
        ),
        (
            "unmapped-team",
            {
                "app_id": None,
                "brand": None,
                "app_secret_stdin": False,
                "owner_open_id": None,
                "without_owner": True,
            },
        ),
        (
            "existing-team",
            {
                "app_id": "cli_existing_app",
                "brand": "lark",
                "app_secret_stdin": True,
                "owner_open_id": "ou_existing_owner",
                "without_owner": False,
            },
        ),
    ]


def test_flagless_add_is_local_owner_managed_without_reading_an_open_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NeverRead(_TTYStringIO):
        def readline(self, *arguments, **options):
            pytest.fail("flagless QR add must not read an owner open_id")

    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=NeverRead("ou_should_not_be_read\n"),
    )

    created = admin.add("team")

    assert store.profiles["team"] == created
    assert not Path(created.config_dir, ".cow-owner-principal.json").exists()
    rendered = output.getvalue()
    assert "local owner-managed" in rendered
    assert "Owner open_id" not in rendered
    assert "OWNER UNMAPPED" not in rendered


@pytest.mark.parametrize("existing_app", [False, True], ids=["qr", "existing-app"])
def test_add_without_owner_is_a_compatible_no_op_for_human_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing_app: bool,
) -> None:
    secret_input = io.StringIO("existing-secret\n") if existing_app else io.StringIO()
    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=secret_input,
    )
    options: dict[str, object] = {"without_owner": True}
    if existing_app:
        options.update(
            {
                "app_id": "cli_existing_unmapped_app",
                "brand": "feishu",
                "app_secret_stdin": True,
            }
        )

    created = admin.add("team", **options)

    assert store.profiles["team"] == created
    assert not Path(created.config_dir, ".cow-owner-principal.json").exists()
    rendered = output.getvalue()
    assert "local owner-managed" in rendered
    assert "OWNER UNMAPPED" not in rendered
    assert "principal map" not in rendered


def test_add_rejects_conflicting_optional_human_mapping_before_any_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    monkeypatch.setattr(
        admin,
        "_verified_binary",
        lambda: pytest.fail("lark-cli must not be accessed"),
    )

    with pytest.raises(LarkProfileError, match="owner"):
        admin.add(
            "team",
            owner_open_id="ou_human_owner",
            without_owner=True,
        )

    assert store.initialize_calls == 0
    assert not (tmp_path / "lark-state").exists()


@pytest.mark.parametrize("existing_app", [False, True], ids=["qr", "existing-app"])
def test_add_can_atomically_map_explicit_app_scoped_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing_app: bool,
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    output = io.StringIO()
    owner_open_id = "ou_owner_for_this_app"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(binary),
        output=output,
        input_stream=(
            io.StringIO("existing-app-secret\n") if existing_app else None
        ),
    )
    options: dict[str, object] = {"owner_open_id": owner_open_id}
    expected_app_id = "cli_test_app"
    if existing_app:
        expected_app_id = "cli_existing_owner_app"
        options.update(
            {
                "app_id": expected_app_id,
                "brand": "feishu",
                "app_secret_stdin": True,
            }
        )

    created = admin.add("team", **options)

    async def verify() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            principal = await store.get_principal("owner")
            assert principal is not None and principal.enabled
            mapping = await store.resolve_principal_account(
                channel="lark",
                bot_id=expected_app_id,
                external_user_id=owner_open_id,
            )
            assert mapping is not None
            assert mapping.principal_id == "owner"
            assert mapping.identifier_kind == "open_id"
            assert mapping.active and mapping.principal_enabled
        finally:
            await store.close()

    asyncio.run(verify())
    assert created.bot_id == expected_app_id
    sidecar = Path(created.config_dir, ".cow-owner-principal.json")
    assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    assert json.loads(sidecar.read_text(encoding="utf-8")) == {
        "schemaVersion": 1,
        "principalId": "owner",
        "openId": owner_open_id,
    }
    assert (
        f"mapped human Lark account team/{owner_open_id} to canonical "
        "principal owner"
    ) in output.getvalue()


def test_add_maps_distinct_cross_org_app_accounts_to_one_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(binary),
        output=io.StringIO(),
        input_stream=io.StringIO("first-secret\n"),
    )
    accounts = (
        ("org-one", "cli_org_one_app", "ou_org_one_owner"),
        ("org-two", "cli_org_two_app", "ou_org_two_owner"),
    )
    for index, (profile, app_id, open_id) in enumerate(accounts):
        if index:
            admin.input_stream = io.StringIO("second-secret\n")
        admin.add(
            profile,
            app_id=app_id,
            brand="feishu",
            app_secret_stdin=True,
            owner_open_id=open_id,
        )

    async def verify() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            principal = await store.get_principal("owner")
            assert principal is not None and principal.enabled
            for _profile, app_id, open_id in accounts:
                mapping = await store.resolve_principal_account(
                    channel="lark",
                    bot_id=app_id,
                    external_user_id=open_id,
                )
                assert mapping is not None and mapping.principal_id == "owner"
            assert (
                await store.resolve_principal_account(
                    channel="lark",
                    bot_id=accounts[0][1],
                    external_user_id=accounts[1][2],
                )
                is None
            )
        finally:
            await store.close()

    asyncio.run(verify())


@pytest.mark.parametrize(
    "durable_change",
    [
        {"cli_version": "1.0.91"},
        {"enabled": False},
        {"mention_policy": "mention_required"},
        {"access_policy": "owner_only"},
        {
            "restart_policy": {
                "max_attempts": 9,
                "base_delay": 1,
                "max_delay": 60,
                "bot_open_id": "ou_test_bot",
            }
        },
    ],
    ids=[
        "cli-version",
        "enabled",
        "mention-policy",
        "access-policy",
        "full-restart-policy",
    ],
)
def test_owner_registration_result_and_commit_fence_reject_durable_profile_drift(
    tmp_path: Path,
    durable_change: dict[str, object],
) -> None:
    admin = LarkProfileAdmin(
        database=tmp_path / "runtime.sqlite",
        config_root=tmp_path / "lark-state",
        output=io.StringIO(),
    )
    expected = admin._profile_record(
        "team",
        tmp_path / "lark-state" / "team",
        lark_cli_module.ProvisionedConfig(
            app_id="cli_owner_fence_app",
            brand="feishu",
            credential_ref="keychain:appsecret:cli_owner_fence_app",
            bot_open_id="ou_test_bot",
        ),
        PINNED_LARK_CLI_VERSION,
    )
    drifted = replace(expected, **durable_change)
    owner_mapping = lark_cli_module._OwnerMappingMetadata(
        "owner",
        "ou_owner_fence_user",
    )
    exact_account = SimpleNamespace(
        principal_id="owner",
        channel="lark",
        bot_id=expected.bot_id,
        external_user_id=owner_mapping.open_id,
        identifier_kind="open_id",
        active=True,
        principal_enabled=True,
    )

    class DriftedResultStore:
        async def create_bot_profile_with_principal_account(
            self,
            profile,
            **_options,
        ):
            assert profile == expected
            return drifted, exact_account

    with pytest.raises(
        LarkProfileStoreUnavailable,
        match="returned conflicting identity",
    ):
        asyncio.run(
            admin._create_registration(
                DriftedResultStore(),
                expected,
                owner_mapping,
            )
        )

    class DriftedFenceStore:
        async def list_bot_profiles(self, **_options):
            return [drifted]

        async def get_bot_profile_status(self, _profile_id):
            return SimpleNamespace(onboarding_state="registered")

        async def resolve_principal_account(self, **_scope):
            return exact_account

    fence_state, durable = asyncio.run(
        admin._registration_fence(
            DriftedFenceStore(),
            expected,
            owner_mapping,
        )
    )
    assert fence_state == "conflict"
    assert durable is None


@pytest.mark.parametrize(
    "owner_open_id",
    ["", "owner", "on_union_id", "ou_", "ou_has space", "ou_bad/slash"],
)
def test_add_rejects_invalid_owner_open_id_before_cli_or_secret_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_open_id: str,
) -> None:
    argv_log = tmp_path / "argv.json"
    monkeypatch.setenv("FAKE_LARK_ARGV_LOG", str(argv_log))
    secret_input = io.StringIO("must-remain-unread\n")
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=secret_input,
    )

    with pytest.raises(LarkProfileError, match="--owner-open-id"):
        admin.add(
            "team",
            app_id="cli_existing_owner_app",
            brand="feishu",
            app_secret_stdin=True,
            owner_open_id=owner_open_id,
        )

    assert secret_input.tell() == 0
    assert not argv_log.exists()
    assert store.initialize_calls == 0
    assert not (tmp_path / "lark-state").exists()


def test_add_rejects_verified_bot_open_id_as_human_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="bot, not a human owner"):
        admin.add("team", owner_open_id="ou_test_bot")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "team").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


@pytest.mark.parametrize(
    "options",
    [
        {"app_id": "cli_existing_app"},
        {"app_secret_stdin": True},
        {"brand": "lark"},
    ],
    ids=["app-id-without-secret-stdin", "secret-stdin-without-app-id", "brand-in-qr-mode"],
)
def test_existing_app_add_rejects_incomplete_option_sets_before_config_init(
    tmp_path: Path,
    monkeypatch,
    options: dict[str, object],
) -> None:
    init_log = tmp_path / "init.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    secret_input = io.StringIO("must-not-be-consumed\n")
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=secret_input,
    )

    with pytest.raises(LarkProfileError):
        admin.add("invalid-options", **options)

    assert store.profiles == {}
    assert secret_input.tell() == 0
    assert not init_log.exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_existing_app_add_uses_exact_argv_and_secret_only_on_stdin(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_id = "cli_existing_app"
    secret = "existing-app-secret-value"
    secret_input = secret + "\n"
    argv_log = tmp_path / "argv.json"
    stdin_log = tmp_path / "stdin.txt"
    transport_log = tmp_path / "transport.json"
    monkeypatch.setenv("FAKE_LARK_ARGV_LOG", str(argv_log))
    monkeypatch.setenv("FAKE_LARK_STDIN_LOG", str(stdin_log))
    monkeypatch.setenv("FAKE_LARK_TRANSPORT_LOG", str(transport_log))
    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO(secret_input),
    )

    created = admin.add(
        "existing",
        app_id=app_id,
        app_secret_stdin=True,
    )

    expected_argv = [
        "config",
        "init",
        "--app-id",
        app_id,
        "--app-secret-stdin",
        "--brand",
        "feishu",
    ]
    assert json.loads(argv_log.read_text(encoding="utf-8")) == expected_argv
    assert secret not in json.dumps(expected_argv)
    assert stdin_log.read_text(encoding="utf-8") == secret_input
    assert stat.S_IMODE(stdin_log.stat().st_mode) == 0o600
    assert json.loads(transport_log.read_text(encoding="utf-8")) == {
        "argv_leaked": False,
        "env_leaked": False,
    }
    assert created.profile_id == "existing"
    assert created.bot_id == app_id
    assert created.brand == "feishu"
    assert created.credential_ref == f"keychain:appsecret:{app_id}"
    assert created.restart_policy["credential_origin"] == "existing_app"
    assert store.profiles["existing"] == created
    config_text = Path(created.config_dir, "config.json").read_text(encoding="utf-8")
    assert secret not in config_text
    assert secret not in output.getvalue()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_remove_imported_profile_preserves_external_keychain_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO("external-app-secret\n"),
    )
    created = admin.add(
        "imported",
        app_id="cli_imported_app",
        brand="feishu",
        app_secret_stdin=True,
    )

    assert created.restart_policy["credential_origin"] == "existing_app"
    assert admin.remove("imported") is True

    assert await_profile(store, "imported") is None
    assert "imported" in store.removed
    assert not Path(created.config_dir).exists()
    assert not remove_log.exists()


def test_imported_profile_rejects_qr_reauthorization_before_cli_mutation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    init_log = tmp_path / "init.log"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO("external-app-secret\n"),
    )
    created = admin.add(
        "imported",
        app_id="cli_imported_reauthorize_app",
        brand="feishu",
        app_secret_stdin=True,
    )
    config_path = Path(created.config_dir, "config.json")
    original_config = config_path.read_bytes()
    init_log.unlink()

    with pytest.raises(
        LarkProfileError,
        match="existing-app profiles cannot use QR reauthorization",
    ):
        admin.reauthorize("imported")

    assert store.profiles["imported"] == created
    assert config_path.read_bytes() == original_config
    assert not init_log.exists()
    assert not remove_log.exists()
    assert not list((tmp_path / "lark-state").glob(".reauthorize-imported-*"))


def test_archived_imported_origin_protects_later_qr_profile_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_id = "cli_external_history_app"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO("external-app-secret\n"),
    )
    admin.add(
        "imported",
        app_id=app_id,
        brand="feishu",
        app_secret_stdin=True,
    )
    assert admin.remove("imported") is True
    assert not remove_log.exists()

    monkeypatch.setenv("FAKE_LARK_APP_ID", app_id)
    monkeypatch.setenv("FAKE_LARK_BRAND", "lark")
    successor = admin.add("qr-successor")

    assert successor.brand == "lark"
    assert successor.restart_policy["credential_origin"] == "existing_app"
    assert store.profiles["qr-successor"] == successor
    assert admin.remove("qr-successor") is True
    assert not remove_log.exists()


def test_archived_imported_origin_vetoes_failed_qr_candidate_cleanup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_id = "cli_external_failure_app"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO("external-app-secret\n"),
    )
    imported = admin.add(
        "imported",
        app_id=app_id,
        brand="feishu",
        app_secret_stdin=True,
    )
    assert admin.remove("imported") is True
    assert not remove_log.exists()
    monkeypatch.setenv("FAKE_LARK_APP_ID", app_id)
    monkeypatch.setenv("FAKE_LARK_AUTH_OK", "false")

    with pytest.raises(LarkProfileError, match="verified bot open_id"):
        admin.add("failed-qr-successor")

    assert store.profiles["imported"].bot_id == imported.bot_id
    assert "failed-qr-successor" not in store.profiles
    assert not remove_log.exists()
    assert not list((tmp_path / "lark-state").glob(".add-failed-qr-successor-*"))


def test_existing_app_credential_origin_is_durable_in_sqlite(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    config_root = tmp_path / "lark-state"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(binary),
        output=io.StringIO(),
        input_stream=io.StringIO("durable-external-secret\n"),
    )

    created = admin.add(
        "durable-import",
        app_id="cli_durable_import_app",
        brand="lark",
        app_secret_stdin=True,
    )

    async def reopen() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            profile = await store.get_bot_profile("durable-import")
            assert profile is not None
            assert profile.restart_policy["credential_origin"] == "existing_app"
            assert profile.bot_id == created.bot_id == "cli_durable_import_app"
            assert profile.brand == created.brand == "lark"
        finally:
            await store.close()

    asyncio.run(reopen())


def test_existing_app_duplicate_is_rejected_before_secret_read_or_config_init(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_id = "cli_duplicate_existing_app"
    argv_log = tmp_path / "argv.json"
    stdin_log = tmp_path / "stdin.txt"
    monkeypatch.setenv("FAKE_LARK_ARGV_LOG", str(argv_log))
    monkeypatch.setenv("FAKE_LARK_STDIN_LOG", str(stdin_log))
    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO("first-secret\n"),
    )
    first = admin.add(
        "first",
        app_id=app_id,
        brand="feishu",
        app_secret_stdin=True,
    )
    argv_log.unlink()
    stdin_log.unlink()
    second_input = io.StringIO("second-secret-must-remain-unread\n")
    admin.input_stream = second_input

    with pytest.raises(LarkProfileError, match="duplicate live app"):
        admin.add(
            "second",
            app_id=app_id,
            brand="feishu",
            app_secret_stdin=True,
        )

    assert second_input.tell() == 0
    assert not argv_log.exists()
    assert not stdin_log.exists()
    assert store.profiles == {"first": first}
    assert Path(first.config_dir, "config.json").is_file()
    assert not (tmp_path / "lark-state" / "second").exists()
    assert not list((tmp_path / "lark-state").glob(".add-second-*"))


@pytest.mark.parametrize(
    ("environment_key", "actual_value"),
    [
        ("FAKE_LARK_APP_ID", "cli_unrequested_app"),
        ("FAKE_LARK_BRAND", "lark"),
    ],
    ids=["wrong-app", "wrong-brand"],
)
def test_existing_app_add_rejects_unrequested_identity_without_removing_credentials(
    tmp_path: Path,
    monkeypatch,
    environment_key: str,
    actual_value: str,
) -> None:
    secret = "wrong-identity-secret"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv(environment_key, actual_value)
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO(secret + "\n"),
    )

    with pytest.raises(LarkProfileError) as raised:
        admin.add(
            "wrong-identity",
            app_id="cli_requested_app",
            brand="feishu",
            app_secret_stdin=True,
        )

    assert secret not in str(raised.value)
    assert secret not in output.getvalue()
    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "wrong-identity").exists()
    retained = list((tmp_path / "lark-state").glob(".add-wrong-identity-*"))
    assert len(retained) == 1
    assert retained[0].joinpath("config.json").is_file()
    assert secret not in retained[0].joinpath("config.json").read_text(
        encoding="utf-8"
    )
    assert not remove_log.exists()

    with pytest.raises(LarkProfileError):
        admin.recover(retained[0].name)

    assert store.profiles == {}
    assert retained[0].is_dir()
    assert not (tmp_path / "lark-state" / "wrong-identity").exists()


def test_existing_app_cli_failure_redacts_secret_and_retains_external_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    secret = "failed-existing-app-secret"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", "existing-fail")
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO(secret + "\n"),
    )

    with pytest.raises(LarkProfileError) as raised:
        admin.add(
            "failed-existing",
            app_id="cli_failed_existing_app",
            brand="feishu",
            app_secret_stdin=True,
        )

    assert secret not in str(raised.value)
    assert secret not in output.getvalue()
    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "failed-existing").exists()
    retained = list((tmp_path / "lark-state").glob(".add-failed-existing-*"))
    assert len(retained) == 1
    assert retained[0].joinpath("config.json").is_file()
    assert secret not in retained[0].joinpath("config.json").read_text(
        encoding="utf-8"
    )
    assert not remove_log.exists()


@pytest.mark.parametrize(
    "mode",
    [
        "existing-empty-after-keychain",
        "existing-invalid-config-after-keychain",
    ],
    ids=["empty-staging", "unparseable-config"],
)
def test_existing_app_post_spawn_failure_retains_unowned_keychain_audit_state(
    tmp_path: Path,
    monkeypatch,
    mode: str,
) -> None:
    secret = "preexisting-keychain-secret"
    init_log = tmp_path / "init.log"
    stdin_log = tmp_path / "stdin.txt"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", mode)
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_STDIN_LOG", str(stdin_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(
        tmp_path,
        monkeypatch,
        input_stream=io.StringIO(secret + "\n"),
    )

    with pytest.raises(LarkProfileError, match="retained") as raised:
        admin.add(
            "post-spawn-failure",
            app_id="cli_preexisting_keychain_app",
            brand="feishu",
            app_secret_stdin=True,
        )

    assert init_log.read_text(encoding="utf-8").strip()
    assert stdin_log.read_text(encoding="utf-8") == secret + "\n"
    assert secret not in str(raised.value)
    assert secret not in output.getvalue()
    assert store.profiles == {}
    retained = list(
        (tmp_path / "lark-state").glob(".add-post-spawn-failure-*")
    )
    assert len(retained) == 1
    if mode == "existing-empty-after-keychain":
        assert not retained[0].joinpath("config.json").exists()
    else:
        assert retained[0].joinpath("config.json").read_text(encoding="utf-8") == (
            '{"apps": ['
        )
    assert not remove_log.exists()


def test_add_accepts_wrapped_verified_bot_identity_compatibility(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_AUTH_WRAPPED", "1")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    created = admin.add("wrapped-shape")

    assert created.restart_policy["bot_open_id"] == "ou_test_bot"
    assert store.profiles["wrapped-shape"] == created


def test_add_rejects_identityless_raw_bot_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_AUTH_RAW", "1")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="verified bot open_id"):
        admin.add("identityless")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "identityless").exists()


def test_add_accepts_pretty_multiline_init_output_and_redacts_secret(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_INIT_PRETTY", "1")
    admin, store, output = _admin(tmp_path, monkeypatch)

    created = admin.add("pretty")

    assert store.profiles["pretty"] == created
    assert created.bot_id == "cli_test_app"
    relayed = output.getvalue()
    assert '\n  "appId": "cli_test_app"' in relayed
    assert "should-never-leak" not in relayed
    assert '"appSecret": "<redacted>"' in relayed


def test_add_rejects_conflicting_multiline_init_identities_without_leaking(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_INIT_PRETTY", "1")
    monkeypatch.setenv("FAKE_LARK_INIT_CONFLICT", "1")
    admin, store, output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="multiple onboarding app identity"):
        admin.add("conflicting-init")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "conflicting-init").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))
    assert "should-never-leak" not in output.getvalue()
    assert "second-secret-must-never-leak" not in output.getvalue()


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (
            '[\n  {"appId":"cli_test_app","brand":"feishu"}\n]\n',
            "trailing content",
        ),
        (
            '{"appId":"cli_test_app","brand":"feishu"}\n{',
            "malformed onboarding output",
        ),
        (
            'result: {"appId":"cli_test_app","brand":"feishu"}\n',
            "malformed onboarding output",
        ),
        (
            '{"appId":"cli_test_app","brand":"feishu"}\n'
            '{"appId":"cli_test_app","brand":"feishu"}\n',
            "multiple onboarding app identity",
        ),
        (
            '{"appId":"cli_forged","appId":"cli_test_app",'
            '"brand":"feishu"}\n',
            "duplicate keys",
        ),
    ],
    ids=[
        "array",
        "truncated-tail",
        "embedded-object",
        "repeated-identity",
        "duplicate-app-id",
    ],
)
def test_structured_init_result_rejects_ambiguous_json_framing(
    raw: str,
    message: str,
) -> None:
    with pytest.raises(LarkProfileError, match=message):
        _structured_init_result(raw)


@pytest.mark.parametrize("duplicate", ["identity", "appId", "openId", "verified"])
def test_add_rejects_duplicate_bot_identity_json_keys(
    tmp_path: Path,
    monkeypatch,
    duplicate: str,
) -> None:
    monkeypatch.setenv("FAKE_LARK_AUTH_DUPLICATE", duplicate)
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="ambiguous bot identity output"):
        admin.add(f"duplicate-{duplicate.lower()}")

    assert store.profiles == {}


@pytest.mark.parametrize(
    ("environment_key", "environment_value"),
    [
        ("FAKE_LARK_AUTH_OK", "false"),
        ("FAKE_LARK_AUTH_AVAILABLE", "false-string"),
        ("FAKE_LARK_AUTH_CONFLICT_APP_ALIAS", "1"),
        ("FAKE_LARK_WHOAMI_IDENTITY", "user"),
        ("FAKE_LARK_WHOAMI_SOURCE", "auto"),
        ("FAKE_LARK_WHOAMI_APP_ID", "cli_conflicting_app"),
    ],
)
def test_add_rejects_untrusted_or_ambiguous_bot_identity_envelopes(
    tmp_path: Path,
    monkeypatch,
    environment_key: str,
    environment_value: str,
) -> None:
    monkeypatch.setenv(environment_key, environment_value)
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="verified bot open_id"):
        admin.add("bad-envelope")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "bad-envelope").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_add_accepts_user_selected_status_when_explicit_bot_probe_matches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_AUTH_IDENTITY", "user")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    created = admin.add("user-and-bot")

    assert created.restart_policy["bot_open_id"] == "ou_test_bot"
    assert store.profiles["user-and-bot"] == created


def test_add_persists_real_sqlite_profile_for_unattended_restart(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    config_root = tmp_path / "lark-state"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(binary),
        output=io.StringIO(),
    )

    created = admin.add("team")

    async def reopen() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            profile = await store.get_bot_profile("team")
            status = await store.get_bot_profile_status("team")
            assert profile is not None and status is not None
            assert profile.bot_id == created.bot_id == "cli_test_app"
            assert profile.config_dir == str(config_root / "team")
            assert profile.credential_ref == "keychain:appsecret:cli_test_app"
            assert profile.cli_version == PINNED_LARK_CLI_VERSION
            assert profile.restart_policy["bot_open_id"] == "ou_test_bot"
            assert profile.restart_policy["max_attempts"] == 8
            assert status.onboarding_state == "registered"
            assert status.connection_state == "not_started"
        finally:
            await store.close()

    __import__("asyncio").run(reopen())


def test_post_commit_output_failure_does_not_orphan_durable_profile(
    tmp_path: Path, monkeypatch
) -> None:
    class FailAfterCommit(io.StringIO):
        def write(self, value: str) -> int:
            if value.startswith("registered Lark profile"):
                raise OSError("terminal disappeared")
            return super().write(value)

    admin, store, _output = _admin(
        tmp_path,
        monkeypatch,
        output=FailAfterCommit(),
    )

    with pytest.raises(OSError, match="terminal disappeared"):
        admin.add("team")

    assert store.profiles["team"].bot_id == "cli_test_app"
    assert (tmp_path / "lark-state" / "team" / "config.json").is_file()


def test_add_store_close_failure_after_commit_preserves_profile_credentials(
    tmp_path: Path, monkeypatch
) -> None:
    class CloseAfterCreateStore(MemoryProfileStore):
        async def close(self) -> None:
            await super().close()
            if self.profiles:
                raise OSError("close failed after commit")

    store = CloseAfterCreateStore()
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)

    with pytest.raises(OSError, match="close failed after commit"):
        admin.add("team")

    assert store.profiles["team"].bot_id == "cli_test_app"
    assert (tmp_path / "lark-state" / "team" / "config.json").is_file()


def test_add_deferred_cancellation_after_commit_preserves_final_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = CommitThenCancelProfileStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)

    with pytest.raises(asyncio.CancelledError):
        admin.add("team")

    assert store.profiles["team"].bot_id == "cli_test_app"
    assert (tmp_path / "lark-state" / "team" / "config.json").is_file()
    assert not list((tmp_path / "lark-state").glob(".add-team-*"))
    assert not remove_log.exists()


def test_live_supervisor_duplicate_add_never_removes_shared_keychain_reference(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    config_root = tmp_path / "lark-state"
    remove_log = tmp_path / "removed.log"
    init_log = tmp_path / "init.log"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    output = io.StringIO()
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(binary),
        output=output,
    )
    admin.add("live")
    output.seek(0)
    output.truncate(0)
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))

    with DatabaseOwnership(database):
        with pytest.raises(SupervisorOwnershipConflict):
            admin.add("second")

    assert (config_root / "live" / "config.json").is_file()
    assert not (config_root / "second").exists()
    assert not list(config_root.glob(".add-second-*"))
    assert output.getvalue() == ""
    assert not init_log.exists()
    assert not remove_log.exists()


def test_live_v35_database_owner_blocks_add_before_qr_or_schema_access(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "runtime.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT)"
    )
    connection.execute(
        "INSERT INTO schema_migrations(version, applied_at) VALUES(35, 'legacy')"
    )
    connection.commit()
    connection.close()

    config_root = tmp_path / "lark-state"
    init_log = tmp_path / "init.log"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(_fake_binary(tmp_path)),
        output=io.StringIO(),
    )

    with DatabaseOwnership(database):
        with pytest.raises(SupervisorOwnershipConflict):
            admin.add("new-profile")

    connection = sqlite3.connect(database)
    try:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (35,)
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='bot_profiles'"
        ).fetchone() is None
    finally:
        connection.close()
    assert not list(config_root.glob(".add-*"))
    assert not init_log.exists()
    assert not remove_log.exists()


def _recoverable_staging(
    tmp_path: Path,
    *,
    encoded_profile: str = "team",
    nonce: str = "deadbeef",
    app_id: str = "cli_recovery_app",
    brand: str = "lark",
) -> Path:
    root = tmp_path / "lark-state"
    root.mkdir(mode=0o700, exist_ok=True)
    root.chmod(0o700)
    staging = root / f".add-{encoded_profile}-{nonce}"
    staging.mkdir(mode=0o700)
    config = staging / "config.json"
    config.write_text(
        json.dumps(
            {
                "apps": [
                    {
                        "appId": app_id,
                        "appSecret": {
                            "source": "keychain",
                            "id": f"appsecret:{app_id}",
                        },
                        "brand": brand,
                        "users": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config.chmod(0o600)
    return staging


@pytest.mark.parametrize(
    ("encoded_profile", "profile_name", "expected_profile"),
    [
        ("team", None, "team"),
        ("source", "destination", "destination"),
    ],
)
def test_recover_finalizes_staging_without_qr_or_credential_removal_and_is_idempotent(
    tmp_path: Path,
    monkeypatch,
    encoded_profile: str,
    profile_name: str | None,
    expected_profile: str,
) -> None:
    staging = _recoverable_staging(
        tmp_path,
        encoded_profile=encoded_profile,
    )
    init_log = tmp_path / "init.log"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)

    recovered = admin.recover(staging.name, profile_name)

    final_path = tmp_path / "lark-state" / expected_profile
    assert recovered.profile_id == expected_profile
    assert recovered.bot_id == "cli_recovery_app"
    assert Path(recovered.config_dir) == final_path
    assert store.profiles[expected_profile] == recovered
    assert final_path.joinpath("config.json").is_file()
    assert not staging.exists()
    assert not init_log.exists()
    assert not remove_log.exists()

    repeated = admin.recover(staging.name, profile_name)
    assert repeated == recovered
    assert store.profiles[expected_profile] == recovered
    assert final_path.joinpath("config.json").is_file()
    assert not init_log.exists()
    assert not remove_log.exists()


def test_recover_existing_app_staging_preserves_external_credential_provenance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_id = "cli_recovered_existing_app"
    staging = _recoverable_staging(
        tmp_path,
        encoded_profile="imported",
        app_id=app_id,
        brand="feishu",
    )
    lark_cli_module._write_existing_app_metadata(staging, app_id, "feishu")
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)

    recovered = admin.recover(staging.name)

    assert recovered.bot_id == app_id
    assert recovered.restart_policy["credential_origin"] == "existing_app"
    assert store.profiles["imported"] == recovered
    assert Path(recovered.config_dir, ".cow-onboarding.json").is_file()
    assert not remove_log.exists()


def test_recover_commits_retained_explicit_owner_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_open_id = "ou_recovered_owner"
    staging = _recoverable_staging(tmp_path)
    lark_cli_module._write_owner_mapping_metadata(staging, owner_open_id)
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    output = io.StringIO()
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(binary),
        output=output,
    )

    recovered = admin.recover(staging.name)

    async def verify() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            mapping = await store.resolve_principal_account(
                channel="lark",
                bot_id="cli_recovery_app",
                external_user_id=owner_open_id,
            )
            assert mapping is not None
            assert mapping.principal_id == "owner"
            assert mapping.active and mapping.principal_enabled
        finally:
            await store.close()

    asyncio.run(verify())
    assert Path(recovered.config_dir, ".cow-owner-principal.json").is_file()
    assert (
        f"mapped human Lark account team/{owner_open_id} to canonical "
        "principal owner"
    ) in output.getvalue()


def test_recover_deferred_cancellation_after_commit_preserves_final_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staging = _recoverable_staging(tmp_path)
    store = CommitThenCancelProfileStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=store,
    )

    with pytest.raises(asyncio.CancelledError):
        admin.recover(staging.name)

    final_path = tmp_path / "lark-state" / "team"
    assert store.profiles["team"].bot_id == "cli_recovery_app"
    assert final_path.joinpath("config.json").is_file()
    assert not staging.exists()
    assert not remove_log.exists()

    recovered = admin.recover(staging.name)
    assert recovered == store.profiles["team"]
    assert final_path.joinpath("config.json").is_file()


def test_recover_normalizes_private_root_with_public_cli_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staging = _recoverable_staging(tmp_path)
    staging.joinpath("config.json").chmod(0o644)
    cache = staging / "cache"
    cache.mkdir(mode=0o755)
    cache.chmod(0o755)
    cache_file = cache / "remote-state.json"
    cache_file.write_text("{}", encoding="utf-8")
    cache_file.chmod(0o644)
    admin, _store, _output = _admin(tmp_path, monkeypatch)

    recovered = admin.recover(staging.name)

    final_path = Path(recovered.config_dir)
    for walked_root, directories, files in os.walk(final_path):
        assert stat.S_IMODE(Path(walked_root).stat().st_mode) == 0o700
        for name in directories:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o700
        for name in files:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o600


@pytest.mark.parametrize(
    "staging_name",
    [
        "../.add-team-deadbeef",
        "/tmp/.add-team-deadbeef",
        ".add-team-short",
        ".add-team-deadbeef-extra",
        ".reauthorize-team-deadbeef",
    ],
)
def test_recover_accepts_only_canonical_add_staging_basenames(
    tmp_path: Path,
    monkeypatch,
    staging_name: str,
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="staging"):
        admin.recover(staging_name)

    assert store.profiles == {}
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_recover_duplicate_profile_retains_staging_without_secret_cleanup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    existing = admin.add("taken")
    staging = _recoverable_staging(
        tmp_path,
        encoded_profile="source",
        app_id="cli_distinct_recovery_app",
    )
    init_log = tmp_path / "recover-init.log"
    remove_log = tmp_path / "recover-remove.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))

    with pytest.raises(LarkProfileError):
        admin.recover(staging.name, "taken")

    assert store.profiles["taken"] == existing
    assert staging.joinpath("config.json").is_file()
    assert (tmp_path / "lark-state" / "taken" / "config.json").is_file()
    assert not init_log.exists()
    assert not remove_log.exists()


def test_recover_duplicate_app_retains_staging_and_live_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    existing = admin.add("live")
    staging = _recoverable_staging(
        tmp_path,
        encoded_profile="recovered",
        app_id=existing.bot_id,
        brand=existing.brand,
    )
    init_log = tmp_path / "recover-init.log"
    remove_log = tmp_path / "recover-remove.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))

    with pytest.raises(LarkProfileError, match="duplicate live app"):
        admin.recover(staging.name)

    assert store.profiles["live"] == existing
    assert staging.joinpath("config.json").is_file()
    assert not (tmp_path / "lark-state" / "recovered").exists()
    assert not init_log.exists()
    assert not remove_log.exists()


def test_recover_create_failure_rolls_publication_back_to_staging(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FailingCreateStore(MemoryProfileStore):
        async def create_bot_profile(self, profile):
            del profile
            raise LarkProfileError("injected create failure")

    store = FailingCreateStore()
    staging = _recoverable_staging(tmp_path)
    init_log = tmp_path / "recover-init.log"
    remove_log = tmp_path / "recover-remove.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=store,
    )

    with pytest.raises(LarkProfileError, match="injected create failure"):
        admin.recover(staging.name)

    assert store.profiles == {}
    assert staging.joinpath("config.json").is_file()
    assert not (tmp_path / "lark-state" / "team").exists()
    assert not init_log.exists()
    assert not remove_log.exists()


def test_failed_add_cleanup_retains_canonical_staging_for_recovery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FailFirstCreateStore(MemoryProfileStore):
        def __init__(self) -> None:
            super().__init__()
            self.fail_next_create = True

        async def create_bot_profile(self, profile):
            if self.fail_next_create:
                self.fail_next_create = False
                raise LarkProfileError("injected first create failure")
            return await super().create_bot_profile(profile)

    store = FailFirstCreateStore()
    cleanup_marker = tmp_path / "cleanup-failed"
    init_log = tmp_path / "init.log"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(cleanup_marker))
    monkeypatch.setenv("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE", "1")
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=store,
    )

    with pytest.raises(LarkProfileError, match="cleanup failed"):
        admin.add("team")

    retained = list((tmp_path / "lark-state").glob(".add-team-*"))
    assert len(retained) == 1
    assert retained[0].joinpath("config.json").is_file()
    assert _config_for_profile(retained[0]).credential_ref == (
        "keychain:appsecret:cli_test_app"
    )
    assert not list((tmp_path / "lark-state").glob(".cleanup-*"))
    assert not list((tmp_path / "lark-state").glob(".failed-cleanup-*"))
    assert not (tmp_path / "lark-state" / "team").exists()
    assert len(init_log.read_text(encoding="utf-8").splitlines()) == 1
    assert len(remove_log.read_text(encoding="utf-8").splitlines()) == 1

    recovered = admin.recover(retained[0].name)

    assert recovered.profile_id == "team"
    assert (tmp_path / "lark-state" / "team" / "config.json").is_file()
    assert not retained[0].exists()
    assert len(init_log.read_text(encoding="utf-8").splitlines()) == 1
    assert len(remove_log.read_text(encoding="utf-8").splitlines()) == 1


@pytest.mark.parametrize(
    ("mode", "expects_keychain_cleanup"),
    [("malformed", True), ("plaintext", False)],
)
def test_failed_validation_removes_partial_credentials_and_profile(
    tmp_path: Path,
    monkeypatch,
    mode: str,
    expects_keychain_cleanup: bool,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", mode)
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError):
        admin.add("broken")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "broken").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))
    assert "should-never-leak" not in output.getvalue()
    # A safely attributable keychain reference is removed; a rejected
    # plaintext value has no external keychain entry to act upon.
    assert remove_log.exists() is expects_keychain_cleanup


@pytest.mark.parametrize("mode", ["truncated-config", "null-secret"])
def test_add_retains_nonempty_unparseable_config_with_unknown_external_state(
    tmp_path: Path,
    monkeypatch,
    mode: str,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", mode)
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(
        LarkProfileError,
        match="could not determine staged credential ownership; retained",
    ) as raised:
        admin.add("truncated")

    retained = list((tmp_path / "lark-state").glob(".add-truncated-*"))
    assert len(retained) == 1
    assert str(retained[0]) in str(raised.value)
    assert retained[0].joinpath("config.json").is_file()
    assert store.profiles == {}
    assert not remove_log.exists()


@pytest.mark.skipif(os.name != "posix", reason="child umask is POSIX-specific")
def test_add_normalizes_owned_cli_artifacts_and_forces_private_child_umask(
    tmp_path: Path,
    monkeypatch,
) -> None:
    probe = tmp_path / "child-umask-probe"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", "insecure")
    monkeypatch.setenv("FAKE_LARK_UMASK_PROBE", str(probe))
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    previous_umask = os.umask(0o022)
    try:
        created = admin.add("private-state")
    finally:
        os.umask(previous_umask)

    final_path = Path(created.config_dir)
    assert store.profiles["private-state"] == created
    assert stat.S_IMODE(probe.stat().st_mode) == 0o600
    for walked_root, directories, files in os.walk(final_path):
        assert stat.S_IMODE(Path(walked_root).stat().st_mode) == 0o700
        for name in directories:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o700
        for name in files:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o600
    assert not remove_log.exists()


def test_add_retains_uninspectable_hardlinked_staging_for_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_MODE", "hardlink")
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(
        LarkProfileError,
        match="could not securely inspect staged credential state; retained",
    ) as raised:
        admin.add("hardlinked")

    retained = list((tmp_path / "lark-state").glob(".add-hardlinked-*"))
    assert len(retained) == 1
    assert str(retained[0]) in str(raised.value)
    assert retained[0].joinpath("config.json").is_file()
    assert retained[0].joinpath("config-alias.json").is_file()
    assert store.profiles == {}
    assert not remove_log.exists()


@pytest.mark.skipif(os.name != "posix", reason="descriptor chmod is POSIX-specific")
def test_staging_tightening_does_not_require_path_chmod_nofollow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir(mode=0o700)
    cache = staging / "cache"
    cache.mkdir(mode=0o755)
    cache.chmod(0o755)
    artifact = cache / "state.json"
    artifact.write_text("{}", encoding="utf-8")
    artifact.chmod(0o644)
    original_chmod = os.chmod

    def linux_style_chmod(
        path,
        mode,
        *,
        dir_fd=None,
        follow_symlinks=True,
    ):
        if follow_symlinks is False:
            raise NotImplementedError("follow_symlinks unavailable")
        return original_chmod(
            path,
            mode,
            dir_fd=dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "chmod", linux_style_chmod)

    _tighten_owned_staging_tree(staging)

    assert stat.S_IMODE(cache.stat().st_mode) == 0o700
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="O_NOFOLLOW is POSIX-specific")
def test_staging_tightening_rejects_symlink_swap_without_chmodding_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir(mode=0o700)
    artifact = staging / "state.json"
    artifact.write_text("{}", encoding="utf-8")
    artifact.chmod(0o644)
    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    outside.chmod(0o644)
    original_open = os.open
    swapped = False

    def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if not swapped and os.fspath(path) == os.fspath(artifact):
            swapped = True
            artifact.unlink()
            artifact.symlink_to(outside)
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", swap_before_open)

    with pytest.raises(LarkProfileError):
        _tighten_owned_staging_tree(staging)

    assert swapped
    assert artifact.is_symlink()
    assert stat.S_IMODE(outside.stat().st_mode) == 0o644


@pytest.mark.parametrize("mode", ["absolute-file", "parent-file"])
def test_add_rejects_file_secret_references_that_break_when_staging_moves(
    tmp_path: Path,
    monkeypatch,
    mode: str,
) -> None:
    monkeypatch.setenv("FAKE_LARK_MODE", mode)
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="relative to its config directory"):
        admin.add("relocation-unsafe")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "relocation-unsafe").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_cleanup_snapshot_restores_parent_relative_file_reference(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fail_marker = tmp_path / "remove-failed-once"
    monkeypatch.setenv("FAKE_LARK_MODE", "parent-file")
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(fail_marker))
    monkeypatch.setenv("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE", "1")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="cleanup failed"):
        admin.add("parent-reference")

    root = tmp_path / "lark-state"
    retained = list(root.glob(".add-parent-reference-*"))
    assert len(retained) == 1
    restored = _config_for_profile(retained[0])
    assert restored.credential_ref.startswith(
        f"file:../{retained[0].name}/"
    )
    assert retained[0].joinpath("app-secret").is_file()
    assert store.profiles == {}
    assert not list(root.glob(".cleanup-*"))
    assert not list(root.glob(".failed-cleanup-*"))


def test_existing_stable_profile_accepts_contained_absolute_file_secret_reference(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "stable-profile"
    profile_path.mkdir(mode=0o700)
    secret_path = profile_path / "app-secret"
    secret_path.write_text("file-secret-value", encoding="utf-8")
    secret_path.chmod(0o600)
    config_path = profile_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "apps": [
                    {
                        "appId": "cli_stable_file_app",
                        "brand": "lark",
                        "appSecret": {
                            "source": "file",
                            "id": str(secret_path.absolute()),
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path.chmod(0o600)

    provisioned = _config_for_profile(profile_path)

    assert provisioned.credential_ref == f"file:{secret_path.absolute()}"


def test_cli_failure_redacts_error_and_rolls_back(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_LARK_MODE", "fail")
    admin, store, output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError) as raised:
        admin.add("failed")

    assert "should-never-leak" not in str(raised.value)
    assert "should-never-leak" not in output.getvalue()
    assert store.profiles == {}
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_qr_failure_discards_owner_intent_only_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FAKE_LARK_MODE", "fail")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="config init failed"):
        admin.add("failed-owner", owner_open_id="ou_failed_owner")

    assert store.profiles == {}
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_add_requires_verified_bot_open_id_for_group_mention_policy(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "not-an-open-id")
    admin, store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="verified bot open_id"):
        admin.add("broken-identity")

    assert store.profiles == {}
    assert not (tmp_path / "lark-state" / "broken-identity").exists()
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_timeout_terminates_child_and_removes_staging(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_LARK_MODE", "timeout")
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "0.15")
    admin, store, _output = _admin(tmp_path, monkeypatch)
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "0.15")

    started = __import__("time").monotonic()
    with pytest.raises(LarkProfileError, match="timed out"):
        admin.add("slow")
    assert __import__("time").monotonic() - started < 5
    assert store.profiles == {}
    assert not list((tmp_path / "lark-state").glob(".add-*"))


def test_duplicate_live_app_rolls_back_only_new_profile(tmp_path: Path, monkeypatch) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    first = admin.add("first")

    with pytest.raises(LarkProfileError, match="duplicate live app"):
        admin.add("second")

    assert store.profiles["first"] == first
    assert (tmp_path / "lark-state" / "first" / "config.json").is_file()
    assert not (tmp_path / "lark-state" / "second").exists()
    # Both staged profiles point at keychain ID appsecret:cli_test_app.
    # Cleaning the rejected directory through `lark-cli config remove` would
    # erase the credential still owned by the first live profile.
    assert not remove_log.exists()


def test_duplicate_app_with_distinct_credential_reference_cleans_new_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    first = admin.add("first")
    first_path = Path(first.config_dir)
    file_secret = first_path / "existing-secret"
    file_secret.write_text("existing", encoding="utf-8")
    file_secret.chmod(0o600)
    config_path = first_path / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["apps"][0]["appSecret"] = {
        "source": "file",
        "id": file_secret.name,
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    config_path.chmod(0o600)
    store.profiles["first"] = replace(
        first,
        credential_ref=f"file:{file_secret.name}",
    )

    with pytest.raises(LarkProfileError, match="duplicate live app"):
        admin.add("second")

    assert file_secret.read_text(encoding="utf-8") == "existing"
    assert first_path.joinpath("config.json").is_file()
    assert not (tmp_path / "lark-state" / "second").exists()
    assert not list((tmp_path / "lark-state").glob(".add-second-*"))
    removed = remove_log.read_text(encoding="utf-8").splitlines()
    assert len(removed) == 1
    assert Path(removed[0]).name.startswith(".add-second-")


def test_add_does_not_remove_credential_owned_by_incompletely_archived_profile(
    tmp_path: Path, monkeypatch
) -> None:
    remove_log = tmp_path / "removed.log"
    fail_marker = tmp_path / "remove-failed-once"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(fail_marker))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("archived")

    with pytest.raises(LarkProfileError, match="cleanup failed"):
        admin.remove("archived")
    assert await_profile(store, "archived") is None
    assert (tmp_path / "lark-state" / "archived" / "config.json").is_file()
    remove_log.unlink()

    with pytest.raises(LarkProfileError, match="pending credential cleanup"):
        admin.add("replacement")

    assert not remove_log.exists()
    assert (tmp_path / "lark-state" / "archived" / "config.json").is_file()
    assert not list((tmp_path / "lark-state").glob(".add-replacement-*"))


def test_reauthorize_requires_same_app_and_swaps_config(tmp_path: Path, monkeypatch) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_MODE", "insecure")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "ou_refreshed_bot")

    updated = admin.reauthorize("team")

    config = __import__("json").loads(
        (tmp_path / "lark-state" / "team" / "config.json").read_text()
    )
    assert config["apps"][0]["marker"] == "two"
    assert updated.bot_id == "cli_test_app"
    assert updated.restart_policy["bot_open_id"] == "ou_refreshed_bot"
    assert store.profiles["team"].cli_version == PINNED_LARK_CLI_VERSION
    for walked_root, directories, files in os.walk(tmp_path / "lark-state" / "team"):
        assert stat.S_IMODE(Path(walked_root).stat().st_mode) == 0o700
        for name in directories:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o700
        for name in files:
            assert stat.S_IMODE((Path(walked_root) / name).stat().st_mode) == 0o600
    assert not list((tmp_path / "lark-state").glob(".backup-*"))
    assert not list((tmp_path / "lark-state").glob(".reauthorize-*"))


def test_reauthorize_deferred_cancellation_after_commit_preserves_new_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class CommitThenCancelUpdateStore(MemoryProfileStore):
        async def update_bot_profile_credentials(self, profile_id, **kwargs):
            await super().update_bot_profile_credentials(profile_id, **kwargs)
            raise asyncio.CancelledError()

    store = CommitThenCancelUpdateStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=store,
    )
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_MODE", "relative-file")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "ou_refreshed_bot")

    with pytest.raises(asyncio.CancelledError):
        admin.reauthorize("team")

    final_path = tmp_path / "lark-state" / "team"
    config = json.loads(final_path.joinpath("config.json").read_text(encoding="utf-8"))
    assert config["apps"][0]["marker"] == "two"
    assert config["apps"][0]["appSecret"] == {
        "source": "file",
        "id": "app-secret",
    }
    assert final_path.joinpath("app-secret").is_file()
    assert store.profiles["team"].credential_ref == "file:app-secret"
    assert store.profiles["team"].restart_policy["bot_open_id"] == "ou_refreshed_bot"
    assert not list((tmp_path / "lark-state").glob(".backup-team-*"))
    assert not list((tmp_path / "lark-state").glob(".failed-team-*"))
    assert not list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    removed_paths = remove_log.read_text(encoding="utf-8").splitlines()
    assert len(removed_paths) == 1
    assert Path(removed_paths[0]).name.startswith(".backup-team-")


def test_reauthorize_keychain_to_file_retires_old_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_MODE", "relative-file")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")

    updated = admin.reauthorize("team")

    final_path = tmp_path / "lark-state" / "team"
    assert updated.credential_ref == "file:app-secret"
    assert store.profiles["team"].credential_ref == "file:app-secret"
    assert final_path.joinpath("app-secret").is_file()
    removed_paths = remove_log.read_text(encoding="utf-8").splitlines()
    assert len(removed_paths) == 1
    assert Path(removed_paths[0]).name.startswith(".backup-team-")
    assert not list((tmp_path / "lark-state").glob(".backup-team-*"))
    assert not list((tmp_path / "lark-state").glob(".cleanup-*"))


def test_reauthorize_same_reference_does_not_remove_shared_credential(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")

    updated = admin.reauthorize("team")

    assert updated.credential_ref == "keychain:appsecret:cli_test_app"
    assert store.profiles["team"].credential_ref == updated.credential_ref
    assert not remove_log.exists()
    assert not list((tmp_path / "lark-state").glob(".backup-team-*"))


def test_reauthorize_old_cleanup_failure_restores_and_retains_backup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    fail_marker = tmp_path / "remove-failed-once"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(fail_marker))
    monkeypatch.setenv("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE", "1")
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    original_config = Path(original.config_dir, "config.json").read_bytes()
    monkeypatch.setenv("FAKE_LARK_MODE", "relative-file")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")

    with pytest.raises(
        LarkProfileError,
        match="committed but old credential cleanup is unresolved; retained",
    ):
        admin.reauthorize("team")

    root = tmp_path / "lark-state"
    final_path = root / "team"
    backups = list(root.glob(".backup-team-*"))
    assert store.profiles["team"].credential_ref == "file:app-secret"
    assert final_path.joinpath("app-secret").is_file()
    assert len(backups) == 1
    assert backups[0].joinpath("config.json").read_bytes() == original_config
    assert _config_for_profile(backups[0]).credential_ref == original.credential_ref
    assert len(remove_log.read_text(encoding="utf-8").splitlines()) == 1
    assert not list(root.glob(".cleanup-*"))
    assert not list(root.glob(".failed-cleanup-*"))


def test_reauthorize_unknown_old_disposition_retains_backup_after_commit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class OldDispositionReadFailureStore(MemoryProfileStore):
        def __init__(self) -> None:
            super().__init__()
            self.fail_listing = False

        async def list_bot_profiles(self, **kwargs):
            if self.fail_listing:
                raise OSError("old credential scan unavailable")
            return await super().list_bot_profiles(**kwargs)

        async def update_bot_profile_credentials(self, profile_id, **kwargs):
            updated = await super().update_bot_profile_credentials(
                profile_id,
                **kwargs,
            )
            self.fail_listing = True
            return updated

    store = OldDispositionReadFailureStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_MODE", "relative-file")

    with pytest.raises(
        LarkProfileError,
        match="committed but old credential cleanup is unresolved; retained",
    ):
        admin.reauthorize("team")

    root = tmp_path / "lark-state"
    assert store.profiles["team"].credential_ref == "file:app-secret"
    assert root.joinpath("team", "app-secret").is_file()
    assert len(list(root.glob(".backup-team-*"))) == 1
    assert not remove_log.exists()


def test_reauthorize_retains_both_configs_when_commit_readback_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class CommitThenUnreadableUpdateStore(MemoryProfileStore):
        def __init__(self) -> None:
            super().__init__()
            self.fail_readback = False

        async def get_bot_profile(self, profile_id, *, include_removed=False):
            if self.fail_readback:
                raise OSError("durable readback unavailable")
            return await super().get_bot_profile(
                profile_id,
                include_removed=include_removed,
            )

        async def update_bot_profile_credentials(self, profile_id, **kwargs):
            await super().update_bot_profile_credentials(profile_id, **kwargs)
            self.fail_readback = True
            raise asyncio.CancelledError()

    store = CommitThenUnreadableUpdateStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=store,
    )
    admin.add("team")
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "ou_refreshed_bot")

    with pytest.raises(
        LarkProfileError,
        match="could not prove whether Lark reauthorization committed; retained",
    ):
        admin.reauthorize("team")

    root = tmp_path / "lark-state"
    final_config = json.loads(
        root.joinpath("team", "config.json").read_text(encoding="utf-8")
    )
    backups = list(root.glob(".backup-team-*"))
    assert final_config["apps"][0]["marker"] == "two"
    assert len(backups) == 1
    backup_config = json.loads(
        backups[0].joinpath("config.json").read_text(encoding="utf-8")
    )
    assert backup_config["apps"][0]["marker"] == "one"
    assert store.profiles["team"].restart_policy["bot_open_id"] == "ou_refreshed_bot"
    assert not remove_log.exists()


def test_reauthorize_failed_update_readback_restores_old_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class RejectUpdateStore(MemoryProfileStore):
        async def update_bot_profile_credentials(self, profile_id, **kwargs):
            del profile_id, kwargs
            raise OSError("credential update rejected")

    store = RejectUpdateStore()
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)
    original = admin.add("team")
    original_bytes = Path(original.config_dir, "config.json").read_bytes()
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "ou_refreshed_bot")

    with pytest.raises(OSError, match="credential update rejected"):
        admin.reauthorize("team")

    root = tmp_path / "lark-state"
    assert Path(original.config_dir, "config.json").read_bytes() == original_bytes
    assert store.profiles["team"] == original
    assert not list(root.glob(".backup-team-*"))
    assert not list(root.glob(".failed-team-*"))
    assert not list(root.glob(".reauthorize-team-*"))


def test_reauthorize_does_not_trust_success_return_without_durable_update(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class NoOpUpdateStore(MemoryProfileStore):
        async def update_bot_profile_credentials(self, profile_id, **kwargs):
            del profile_id, kwargs
            return None

    store = NoOpUpdateStore()
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)
    original = admin.add("team")
    original_bytes = Path(original.config_dir, "config.json").read_bytes()
    monkeypatch.setenv("FAKE_LARK_GENERATION", "two")
    monkeypatch.setenv("FAKE_LARK_BOT_OPEN_ID", "ou_refreshed_bot")

    with pytest.raises(
        LarkProfileError,
        match="update did not produce its expected durable state",
    ):
        admin.reauthorize("team")

    root = tmp_path / "lark-state"
    assert Path(original.config_dir, "config.json").read_bytes() == original_bytes
    assert store.profiles["team"] == original
    assert not list(root.glob(".backup-team-*"))
    assert not list(root.glob(".failed-team-*"))


def test_reauthorize_unknown_imported_provenance_rejects_before_qr(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DispositionReadFailureStore(MemoryProfileStore):
        def __init__(self) -> None:
            super().__init__()
            self.fail_listing = False

        async def list_bot_profiles(self, **kwargs):
            if self.fail_listing:
                raise OSError("profile scan unavailable")
            return await super().list_bot_profiles(**kwargs)

    store = DispositionReadFailureStore()
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, _store, _output = _admin(tmp_path, monkeypatch, store=store)
    original = admin.add("team")
    store.fail_listing = True
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_unknown_candidate")

    with pytest.raises(
        LarkProfileError,
        match="could not verify imported credential provenance.*did not start",
    ):
        admin.reauthorize("team")

    retained = list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    assert retained == []
    assert store.profiles["team"] == original
    assert not remove_log.exists()


def test_reauthorize_preflight_rejects_durable_mismatch_before_qr(
    tmp_path: Path,
    monkeypatch,
) -> None:
    init_log = tmp_path / "init.log"
    monkeypatch.setenv("FAKE_LARK_INIT_LOG", str(init_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    init_log.unlink()
    store.profiles["team"] = replace(
        original,
        credential_ref="keychain:appsecret:cli_conflicting_app",
    )

    with pytest.raises(
        LarkProfileError,
        match="profile config identity conflicts with the durable bot profile",
    ):
        admin.reauthorize("team")

    assert not init_log.exists()
    assert not list((tmp_path / "lark-state").glob(".reauthorize-team-*"))


def test_reauthorize_failed_verification_mutation_retains_staging(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    monkeypatch.setenv("FAKE_LARK_AUTH_OK", "false")
    monkeypatch.setenv("FAKE_LARK_AUTH_ADD_HARDLINK", "1")

    with pytest.raises(
        LarkProfileError,
        match="could not securely reinspect staged reauthorization credentials; retained",
    ):
        admin.reauthorize("team")

    retained = list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    assert len(retained) == 1
    assert retained[0].joinpath("post-auth-config-alias.json").is_file()
    assert store.profiles["team"] == original
    assert not remove_log.exists()


def test_reauthorize_foreign_account_lock_conflict_retains_staging(
    tmp_path: Path,
    monkeypatch,
) -> None:
    foreign_app = "cli_locked_candidate"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    monkeypatch.setenv("FAKE_LARK_APP_ID", foreign_app)

    with ChannelAccountOwnership(channel="lark", bot_id=foreign_app):
        with pytest.raises(
            LarkProfileError,
            match="could not prove staged reauthorization credential ownership; retained",
        ):
            admin.reauthorize("team")

    retained = list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    assert len(retained) == 1
    assert retained[0].joinpath("config.json").is_file()
    assert store.profiles["team"] == original
    assert not remove_log.exists()


def test_reauthorize_retains_uninspectable_hardlinked_staging_for_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    original_bytes = Path(original.config_dir, "config.json").read_bytes()
    monkeypatch.setenv("FAKE_LARK_MODE", "hardlink")

    with pytest.raises(
        LarkProfileError,
        match="could not securely inspect staged reauthorization credentials; retained",
    ) as raised:
        admin.reauthorize("team")

    retained = list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    assert len(retained) == 1
    assert str(retained[0]) in str(raised.value)
    assert retained[0].joinpath("config.json").is_file()
    assert retained[0].joinpath("config-alias.json").is_file()
    assert store.profiles["team"] == original
    assert Path(original.config_dir, "config.json").read_bytes() == original_bytes
    assert not remove_log.exists()


def test_reauthorize_retains_tree_made_uninspectable_during_verification(
    tmp_path: Path,
    monkeypatch,
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    original_bytes = Path(original.config_dir, "config.json").read_bytes()
    monkeypatch.setenv("FAKE_LARK_AUTH_ADD_HARDLINK", "1")

    with pytest.raises(
        LarkProfileError,
        match="could not securely reinspect staged reauthorization credentials; retained",
    ) as raised:
        admin.reauthorize("team")

    retained = list((tmp_path / "lark-state").glob(".reauthorize-team-*"))
    assert len(retained) == 1
    assert str(retained[0]) in str(raised.value)
    assert retained[0].joinpath("config.json").is_file()
    assert retained[0].joinpath("post-auth-config-alias.json").is_file()
    assert store.profiles["team"] == original
    assert Path(original.config_dir, "config.json").read_bytes() == original_bytes
    assert not remove_log.exists()


def test_reauthorize_identity_mismatch_preserves_old_profile(
    tmp_path: Path, monkeypatch
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    original_bytes = (
        tmp_path / "lark-state" / "team" / "config.json"
    ).read_bytes()
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_different_app")

    with pytest.raises(LarkProfileError, match="different app identity"):
        admin.reauthorize("team")

    assert store.profiles["team"] == original
    assert (
        tmp_path / "lark-state" / "team" / "config.json"
    ).read_bytes() == original_bytes


def test_reauthorize_foreign_live_app_never_removes_peer_credential(
    tmp_path: Path, monkeypatch
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_team_app")
    original = admin.add("team")
    original_bytes = (
        tmp_path / "lark-state" / "team" / "config.json"
    ).read_bytes()
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_peer_app")
    peer = admin.add("peer")

    with pytest.raises(LarkProfileError, match="different app identity"):
        admin.reauthorize("team")

    assert store.profiles["team"] == original
    assert store.profiles["peer"] == peer
    assert (
        tmp_path / "lark-state" / "team" / "config.json"
    ).read_bytes() == original_bytes
    assert (tmp_path / "lark-state" / "peer" / "config.json").is_file()
    assert not remove_log.exists()
    assert not list((tmp_path / "lark-state").glob(".reauthorize-team-*"))


def test_reauthorize_cleans_foreign_app_owned_only_by_fully_removed_profile(
    tmp_path: Path, monkeypatch
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_team_app")
    original = admin.add("team")
    monkeypatch.setenv("FAKE_LARK_APP_ID", "cli_archived_app")
    admin.add("archived")
    assert admin.remove("archived")
    assert await_profile(store, "archived") is None
    assert not (tmp_path / "lark-state" / "archived").exists()
    remove_log.unlink()

    with pytest.raises(LarkProfileError, match="different app identity"):
        admin.reauthorize("team")

    assert store.profiles["team"] == original
    removed_paths = remove_log.read_text(encoding="utf-8").splitlines()
    assert len(removed_paths) == 1
    assert Path(removed_paths[0]).name.startswith(".reauthorize-team-")
    assert not list((tmp_path / "lark-state").glob(".reauthorize-team-*"))


def test_list_status_disable_enable_and_remove(tmp_path: Path, monkeypatch) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(tmp_path, monkeypatch)
    admin.add("team")

    admin.disable("team")
    assert store.profiles["team"].enabled is False
    admin.enable("team")
    assert store.profiles["team"].enabled is True
    assert [item.profile_id for item in admin.list()] == ["team"]
    rows = admin.status("team")
    assert rows[0][1].connection_state == "disconnected"
    assert admin.remove("team")
    assert await_profile(store, "team") is None
    assert not (tmp_path / "lark-state" / "team").exists()
    assert "team disabled" in output.getvalue()
    assert "team enabled" in output.getvalue()
    assert "team removed" in output.getvalue()


def test_existing_profile_rejects_identity_mismatched_keychain_reference(
    tmp_path: Path, monkeypatch
) -> None:
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    config_path = tmp_path / "lark-state" / "team" / "config.json"
    data = __import__("json").loads(config_path.read_text(encoding="utf-8"))
    data["apps"][0]["appSecret"]["id"] = "appsecret:cli_other_app"
    config_path.write_text(__import__("json").dumps(data), encoding="utf-8")
    config_path.chmod(0o600)

    with pytest.raises(LarkProfileError, match="conflicts with its app identity"):
        admin.disable("team")

    assert store.profiles["team"].enabled is True


def await_profile(store: MemoryProfileStore, profile_id: str):
    import asyncio

    return asyncio.run(store.get_bot_profile(profile_id))


def test_missing_store_api_has_clear_migration_error(tmp_path: Path, monkeypatch) -> None:
    class IncompleteStore:
        async def initialize(self):
            pass

        async def close(self):
            pass

    admin, _store, _output = _admin(
        tmp_path,
        monkeypatch,
        store=IncompleteStore(),
    )
    with pytest.raises(LarkProfileStoreUnavailable, match="list_bot_profiles"):
        admin.list()


def test_insecure_existing_root_is_rejected(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "lark-state"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    admin, _store, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(LarkProfileError, match="owner-only"):
        admin.list()


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
def test_stable_profile_commands_accept_only_its_private_event_bus_socket(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Keep the socket pathname below macOS's short sockaddr_un limit.
    with tempfile.TemporaryDirectory(prefix="cow-lark-", dir="/tmp") as directory:
        root = Path(directory)
        binary = _fake_binary(tmp_path)
        store = MemoryProfileStore()
        monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(root / "locks"))
        monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
        admin = LarkProfileAdmin(
            database=root / "runtime.sqlite",
            config_root=root / "lark-state",
            binary=str(binary),
            store_factory=lambda _path: store,
            output=io.StringIO(),
        )
        created = admin.add("team")
        profile_path = Path(created.config_dir)
        bus = _bind_private_event_bus_socket(profile_path, created.bot_id)
        try:
            observed = _config_for_profile(profile_path)
            assert observed.app_id == created.bot_id

            # Staging/publication inspection remains special-file-strict.
            with pytest.raises(LarkProfileError, match="event bus socket"):
                lark_cli_module._validate_private_tree(profile_path)

            admin.disable("team")
            assert store.profiles["team"].enabled is False
        finally:
            bus.close()


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
def test_stable_profile_rejects_another_apps_event_bus_socket(
    tmp_path: Path,
    monkeypatch,
) -> None:
    with tempfile.TemporaryDirectory(prefix="cow-lark-", dir="/tmp") as directory:
        root = Path(directory)
        binary = _fake_binary(tmp_path)
        store = MemoryProfileStore()
        monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(root / "locks"))
        monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
        admin = LarkProfileAdmin(
            database=root / "runtime.sqlite",
            config_root=root / "lark-state",
            binary=str(binary),
            store_factory=lambda _path: store,
            output=io.StringIO(),
        )
        created = admin.add("team")
        profile_path = Path(created.config_dir)
        bus = _bind_private_event_bus_socket(profile_path, "cli_other_admin_app")
        try:
            with pytest.raises(LarkProfileError, match="belongs to another app"):
                _config_for_profile(profile_path)
            with pytest.raises(LarkProfileError, match="belongs to another app"):
                admin.disable("team")
            assert store.profiles["team"].enabled is True
        finally:
            bus.close()


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
def test_credential_cleanup_snapshot_omits_only_the_event_bus_socket(
    tmp_path: Path,
    monkeypatch,
) -> None:
    with tempfile.TemporaryDirectory(prefix="cow-lark-", dir="/tmp") as directory:
        root = Path(directory)
        binary = _fake_binary(tmp_path)
        store = MemoryProfileStore()
        monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(root / "locks"))
        monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
        admin = LarkProfileAdmin(
            database=root / "runtime.sqlite",
            config_root=root / "lark-state",
            binary=str(binary),
            store_factory=lambda _path: store,
            output=io.StringIO(),
        )
        created = admin.add("team")
        profile_path = Path(created.config_dir)
        bus = _bind_private_event_bus_socket(profile_path, created.bot_id)
        runtime_log = profile_path / "events" / created.bot_id / "bus.log"
        runtime_log.write_text("private runtime diagnostic", encoding="utf-8")
        runtime_log.chmod(0o600)
        try:
            admin._cleanup_new_credentials(str(binary), profile_path)
        finally:
            bus.close()
        assert not profile_path.exists()
        assert not list((root / "lark-state").glob(".cleanup-*"))


def test_owner_cli_subprocess_environment_scrubs_account_overrides(
    tmp_path: Path, monkeypatch
) -> None:
    config = tmp_path / "isolated"
    for key in (
        "LARKSUITE_CLI_APP_ID",
        "LARKSUITE_CLI_APP_SECRET",
        "LARKSUITE_CLI_BRAND",
        "LARKSUITE_CLI_USER_ACCESS_TOKEN",
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN",
        "LARKSUITE_CLI_DEFAULT_AS",
        "LARKSUITE_CLI_PROFILE",
        "LARKSUITE_CLI_DATA_DIR",
        "LARKSUITE_CLI_AUTH_PROXY",
        "LARKSUITE_CLI_PROXY_KEY",
    ):
        monkeypatch.setenv(key, "attacker-controlled")
    environment = _subprocess_environment(config)
    assert environment["LARKSUITE_CLI_CONFIG_DIR"] == str(config)
    assert environment["LARKSUITE_CLI_NO_UPDATE_NOTIFIER"] == "1"
    assert environment["LARKSUITE_CLI_NO_SKILLS_NOTIFIER"] == "1"
    assert not any(
        key in environment
        for key in (
            "LARKSUITE_CLI_APP_ID",
            "LARKSUITE_CLI_APP_SECRET",
            "LARKSUITE_CLI_BRAND",
            "LARKSUITE_CLI_USER_ACCESS_TOKEN",
            "LARKSUITE_CLI_TENANT_ACCESS_TOKEN",
            "LARKSUITE_CLI_DEFAULT_AS",
            "LARKSUITE_CLI_PROFILE",
            "LARKSUITE_CLI_DATA_DIR",
            "LARKSUITE_CLI_AUTH_PROXY",
            "LARKSUITE_CLI_PROXY_KEY",
        )
    )


def test_real_list_and_status_are_read_only_while_supervisor_owns_database(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    output = io.StringIO()
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(binary),
        output=output,
    )
    admin.add("team")

    async def mark_ready() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            await store.set_bot_profile_status(
                "team",
                connection_state="ready",
                generation=4,
                last_ready_at="2026-08-31T12:00:00+00:00",
            )
        finally:
            await store.close()

    __import__("asyncio").run(mark_ready())

    async def forbidden_initialize(*_args, **_kwargs):
        raise AssertionError("read-only list/status must not initialize SQLiteStore")

    monkeypatch.setattr(SQLiteStore, "initialize", forbidden_initialize)
    with DatabaseOwnership(database):
        assert [profile.profile_id for profile in admin.list()] == ["team"]
        rows = admin.status("team")
    assert rows[0][1].connection_state == "ready"
    assert rows[0][1].generation == 4
    assert "last_ready_at=2026-08-31T12:00:00+00:00" in output.getvalue()


def test_read_only_list_fails_closed_on_missing_profile_schema(
    tmp_path: Path, monkeypatch
) -> None:
    database = tmp_path / "runtime.sqlite"
    import sqlite3

    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE unrelated(value TEXT)")
    connection.commit()
    connection.close()
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(_fake_binary(tmp_path)),
        output=io.StringIO(),
    )
    with pytest.raises(LarkProfileStoreUnavailable, match="missing the Lark"):
        admin.list()


def test_read_only_list_rejects_runtime_schema_newer_than_owner_cli(
    tmp_path: Path, monkeypatch
) -> None:
    assert LARK_PROFILE_SCHEMA_VERSION < OWNER_CLI_MAX_RUNTIME_SCHEMA_VERSION
    database = tmp_path / "runtime.sqlite"

    async def initialize() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        await store.close()

    asyncio.run(initialize())
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)",
            (OWNER_CLI_MAX_RUNTIME_SCHEMA_VERSION + 1, "future"),
        )
        connection.commit()
    finally:
        connection.close()
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(_fake_binary(tmp_path)),
        output=io.StringIO(),
    )

    with pytest.raises(LarkProfileStoreUnavailable, match="newer than this owner CLI"):
        admin.list()


def test_remove_retries_credential_cleanup_after_durable_soft_remove(
    tmp_path: Path, monkeypatch
) -> None:
    fail_marker = tmp_path / "remove-failed-once"
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(fail_marker))
    monkeypatch.setenv("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE", "1")
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    original_config = Path(original.config_dir, "config.json").read_bytes()

    with pytest.raises(LarkProfileError, match="cleanup failed"):
        admin.remove("team")
    assert await_profile(store, "team") is None
    config_path = tmp_path / "lark-state" / "team" / "config.json"
    assert config_path.read_bytes() == original_config
    assert _config_for_profile(config_path.parent).credential_ref == original.credential_ref
    assert not list((tmp_path / "lark-state").glob(".cleanup-*"))
    assert not list((tmp_path / "lark-state").glob(".failed-cleanup-*"))

    assert admin.remove("team")
    assert not (tmp_path / "lark-state" / "team").exists()


@pytest.mark.parametrize("retry_config", ["missing", "invalid"])
def test_remove_retry_retains_audit_handle_when_cleanup_outcome_is_unknown(
    tmp_path: Path,
    monkeypatch,
    retry_config: str,
) -> None:
    fail_marker = tmp_path / "remove-failed-once"
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_FAIL_ONCE", str(fail_marker))
    monkeypatch.setenv("FAKE_LARK_REMOVE_DELETE_CONFIG_BEFORE_FAILURE", "1")
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    profile_path = tmp_path / "lark-state" / "team"
    config_path = profile_path / "config.json"

    with pytest.raises(LarkProfileError, match="cleanup failed"):
        admin.remove("team")
    assert await_profile(store, "team") is None
    assert profile_path.is_dir()
    assert _config_for_profile(profile_path).credential_ref == (
        "keychain:appsecret:cli_test_app"
    )

    if retry_config == "missing":
        config_path.unlink()
    else:
        config_path.write_text("not-json", encoding="utf-8")
        config_path.chmod(0o600)

    with pytest.raises(LarkProfileError, match="cleanup outcome is unknown"):
        admin.remove("team")

    assert await_profile(store, "team") is None
    assert profile_path.is_dir()
    assert config_path.exists() is (retry_config == "invalid")
    if retry_config == "invalid":
        assert config_path.read_text(encoding="utf-8") == "not-json"
    assert len(remove_log.read_text(encoding="utf-8").splitlines()) == 1
    assert "team removed" not in output.getvalue()


def test_remove_rejects_same_app_credential_reference_substitution(
    tmp_path: Path, monkeypatch
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    admin.add("team")
    profile_path = tmp_path / "lark-state" / "team"
    config_path = profile_path / "config.json"
    substituted_secret = profile_path / "substituted-secret"
    substituted_secret.write_text("substituted", encoding="utf-8")
    substituted_secret.chmod(0o600)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["apps"][0]["appSecret"] = {
        "source": "file",
        "id": substituted_secret.name,
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    config_path.chmod(0o600)

    with pytest.raises(LarkProfileError, match="durable bot profile"):
        admin.remove("team")

    assert await_profile(store, "team") is not None
    assert profile_path.is_dir()
    assert not remove_log.exists()


def test_remove_different_reference_peer_does_not_suppress_credential_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    remove_log = tmp_path / "removed.log"
    monkeypatch.setenv("FAKE_LARK_REMOVE_LOG", str(remove_log))
    admin, store, _output = _admin(tmp_path, monkeypatch)
    original = admin.add("team")
    peer_path = tmp_path / "lark-state" / "peer"
    store.profiles["peer"] = replace(
        original,
        profile_id="peer",
        config_dir=str(peer_path),
        config_dir_identity="path-sha256:peer-test",
        credential_ref="file:peer-secret",
    )

    assert admin.remove("team")

    assert len(remove_log.read_text(encoding="utf-8").splitlines()) == 1
    assert store.profiles["peer"].removed_at is None
    assert not (tmp_path / "lark-state" / "team").exists()


def test_principal_list_native_shows_live_local_owner_managed_profiles(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"
    output = io.StringIO()

    def profile(
        profile_id: str,
        app_id: str,
        *,
        enabled: bool = True,
    ) -> dict[str, object]:
        return {
            "profile_id": profile_id,
            "channel": "lark",
            "bot_id": app_id,
            "brand": "feishu",
            "config_dir": str(tmp_path / "lark-state" / profile_id),
            "config_dir_identity": f"path-sha256:{profile_id}",
            "cli_version": PINNED_LARK_CLI_VERSION,
            "credential_ref": f"keychain:appsecret:{app_id}",
            "enabled": enabled,
        }

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            await store.create_bot_profile(profile("enabled", "cli_enabled_app"))
            await store.create_bot_profile(
                profile("disabled", "cli_disabled_app", enabled=False)
            )
            await store.create_bot_profile(profile("removed", "cli_removed_app"))
            assert await store.remove_bot_profile("removed")
        finally:
            await store.close()

    asyncio.run(seed())
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        output=output,
    )

    rows = admin.principal_list()

    # Bot-profile ownership is a separate display projection; callers keep
    # receiving the established human-principal row shape.
    assert rows == []
    rendered = output.getvalue()
    assert "OWNER-MANAGED LARK BOT PROFILES\n" in rendered
    assert "MANAGED BY\tPROFILE\tBRAND\tAPP ID\tSTATE\n" in rendered
    assert (
        "local-owner\tenabled\tfeishu\tcli_enabled_app\tenabled\n"
        in rendered
    )
    assert (
        "local-owner\tdisabled\tfeishu\tcli_disabled_app\tdisabled\n"
        in rendered
    )
    assert "cli_removed_app" not in rendered
    assert "HUMAN PRINCIPAL MAPPINGS (OPTIONAL)\n" in rendered
    assert rendered.endswith("no human Lark account mappings\n")


def test_principal_list_injected_store_shows_owner_managed_profiles_without_rows(
    tmp_path: Path,
) -> None:
    profiles = [
        SimpleNamespace(
            profile_id="work",
            channel="lark",
            bot_id="cli_work_app",
            brand="lark",
            enabled=True,
            removed_at=None,
        ),
        SimpleNamespace(
            profile_id="personal",
            channel="lark",
            bot_id="cli_personal_app",
            brand="feishu",
            enabled=False,
            removed_at=None,
        ),
    ]

    class PrincipalViewStore:
        initialized = False
        closed = False

        async def initialize(self):
            self.initialized = True

        async def close(self):
            self.closed = True

        async def list_principals(self):
            return []

        async def list_principal_accounts(self, **options):
            assert options == {"principal_id": None, "active": True}
            return []

        async def list_bot_profiles(self, **options):
            assert options == {"channel": "lark", "include_removed": False}
            return profiles

    store = PrincipalViewStore()
    output = io.StringIO()
    admin = LarkProfileAdmin(
        database=tmp_path / "runtime.sqlite",
        config_root=tmp_path / "lark-state",
        store_factory=lambda _path: store,
        output=output,
    )

    assert admin.principal_list() == []

    rendered = output.getvalue()
    assert "local-owner\twork\tlark\tcli_work_app\tenabled\n" in rendered
    assert (
        "local-owner\tpersonal\tfeishu\tcli_personal_app\tdisabled\n"
        in rendered
    )
    assert rendered.endswith("no human Lark account mappings\n")
    assert store.initialized and store.closed


def test_owner_principal_cli_maps_lists_and_unmaps_validated_profile_open_id(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    output = io.StringIO()
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=tmp_path / "lark-state",
        binary=str(binary),
        output=output,
    )
    admin.add("team")
    mapped = admin.principal_map(
        "person:alice",
        "team",
        "ou_actor_alice",
        display_name="Alice",
    )
    assert mapped.principal_id == "person:alice"
    assert mapped.bot_id == "cli_test_app"
    assert mapped.external_user_id == "ou_actor_alice"

    async def seed_non_lark_mapping() -> None:
        sqlite_store = SQLiteStore(database)
        await sqlite_store.initialize()
        try:
            await sqlite_store.map_principal_account(
                principal_id="person:alice",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wechat-user",
                identifier_kind="from_user_id",
                configured_by="test",
            )
        finally:
            await sqlite_store.close()

    asyncio.run(seed_non_lark_mapping())

    output.seek(0)
    output.truncate(0)
    with DatabaseOwnership(database):
        all_rows = admin.principal_list()
    assert len(all_rows) == 1
    rendered = output.getvalue()
    assert "OWNER-MANAGED LARK BOT PROFILES\n" in rendered
    assert "local-owner\tteam\tfeishu\tcli_test_app\tenabled\n" in rendered
    assert "HUMAN PRINCIPAL MAPPINGS (OPTIONAL)\n" in rendered
    assert (
        "person:alice\tenabled\tteam\tcli_test_app\tou_actor_alice\t1\n"
        in rendered
    )

    output.seek(0)
    output.truncate(0)
    with DatabaseOwnership(database):
        rows = admin.principal_list("person:alice")
    assert len(rows) == 1
    assert rows[0][2] == "team"
    assert rows[0][1].external_user_id == "ou_actor_alice"
    rendered = output.getvalue()
    assert "OWNER-MANAGED LARK BOT PROFILES" not in rendered
    assert rendered.startswith("HUMAN PRINCIPAL MAPPINGS (OPTIONAL)\n")
    assert (
        "person:alice\tenabled\tteam\tcli_test_app\tou_actor_alice\t1\n"
        in rendered
    )

    assert admin.principal_unmap("team", "ou_actor_alice")

    async def verify_unmapped() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            assert await store.resolve_principal_account(
                channel="lark",
                bot_id="cli_test_app",
                external_user_id="ou_actor_alice",
            ) is None
        finally:
            await store.close()

    __import__("asyncio").run(verify_unmapped())
    with pytest.raises(LarkProfileError, match="stable ou_"):
        admin.principal_map("person:bob", "team", "union-id")


def test_principal_unmap_uses_durable_identity_when_live_config_is_corrupt(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    config_root = tmp_path / "lark-state"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(binary),
        output=io.StringIO(),
    )
    admin.add("team")
    admin.principal_map("person:alice", "team", "ou_actor_alice")
    config_path = config_root / "team" / "config.json"
    config_path.write_text("not json", encoding="utf-8")
    config_path.chmod(0o600)

    assert admin.principal_unmap("team", "ou_actor_alice")

    async def verify_unmapped() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            assert await store.resolve_principal_account(
                channel="lark",
                bot_id="cli_test_app",
                external_user_id="ou_actor_alice",
            ) is None
        finally:
            await store.close()

    __import__("asyncio").run(verify_unmapped())


def test_principal_unmap_recovers_legacy_mapping_after_profile_removal(
    tmp_path: Path, monkeypatch
) -> None:
    binary = _fake_binary(tmp_path)
    database = tmp_path / "runtime.sqlite"
    config_root = tmp_path / "lark-state"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("CODEX_LARK_ONBOARD_TIMEOUT", "3")
    admin = LarkProfileAdmin(
        database=database,
        config_root=config_root,
        binary=str(binary),
        output=io.StringIO(),
    )
    admin.add("team")
    admin.principal_map("person:alice", "team", "ou_actor_alice")
    assert admin.remove("team")

    # Current removal retires mappings transactionally.  Reactivate the row
    # to model an older database or interrupted historical cleanup; revocation
    # must not require the already-deleted profile directory.
    sqlite3 = __import__("sqlite3")
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "UPDATE principal_accounts SET active=1, retired_at=NULL "
            "WHERE channel='lark' AND bot_id='cli_test_app' "
            "AND external_user_id='ou_actor_alice'"
        )
        connection.commit()
    finally:
        connection.close()

    assert not (config_root / "team").exists()
    assert admin.principal_unmap("team", "ou_actor_alice")
