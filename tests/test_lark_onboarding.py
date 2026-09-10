"""Live Lark QR staging, publication, cancellation, and rollback contracts."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import stat
import threading
import time
import tempfile
from pathlib import Path

import pytest

from src.lark_cli import LarkProfileError, cleanup_unowned_provisioned_config
from src.lark_onboarding import (
    LarkOnboardingRetainedError,
    StagedCredentialDisposition,
    public_onboarding_error,
    stage_lark_qr_onboarding,
)
from src.runtime.supervisor import CredentialMutationOwnership


VERIFICATION_URL = (
    "https://open.feishu.cn/page/cli?user_code=TEST-7QM8"
    "&lpv=1.0.92&ocv=1.0.92&from=cli"
)

FAKE_LARK_CLI = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
if args == ["--version"]:
    print("lark-cli version 1.0.92")
    raise SystemExit(0)

config_dir = Path(os.environ["LARKSUITE_CLI_CONFIG_DIR"])
mode = os.environ.get("FAKE_LIVE_ONBOARDING_MODE", "success")
if args == ["config", "init", "--new"]:
    argv_log = os.environ.get("FAKE_LIVE_ONBOARDING_ARGV_LOG")
    if argv_log:
        Path(argv_log).write_text(json.dumps(args), encoding="utf-8")
    pid_file = os.environ.get("FAKE_LIVE_ONBOARDING_PID_FILE")
    if pid_file:
        Path(pid_file).write_text(str(os.getpid()), encoding="utf-8")

    if mode == "partial":
        (config_dir / "config.json").write_text('{"apps": [', encoding="utf-8")
        os.chmod(config_dir / "config.json", 0o600)

    print("app_secret=child-output-must-never-escape", flush=True)
    if mode != "missing-url":
        print(os.environ["FAKE_LIVE_ONBOARDING_URL"], flush=True)

    release_file = os.environ.get("FAKE_LIVE_ONBOARDING_RELEASE_FILE")
    if release_file:
        deadline = time.monotonic() + 5
        while not Path(release_file).exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        if not Path(release_file).exists():
            print("callback did not release onboarding", flush=True)
            raise SystemExit(12)
    if mode in {"hang", "partial"}:
        time.sleep(60)

    cache = config_dir / "cache"
    cache.mkdir(mode=0o755)
    os.chmod(cache, 0o755)
    (cache / "remote.json").write_text("{}", encoding="utf-8")
    os.chmod(cache / "remote.json", 0o644)
    app_id = "cli_live_new_app"
    brand = os.environ.get("FAKE_LIVE_ONBOARDING_BRAND", "feishu")
    config = {
        "apps": [{
            "appId": app_id,
            "appSecret": {"source": "keychain", "id": "appsecret:" + app_id},
            "brand": brand,
            "users": [],
        }]
    }
    (config_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
    os.chmod(config_dir / "config.json", 0o644)
    print(json.dumps({
        "appId": app_id,
        "appSecret": "structured-secret-must-never-escape",
        "brand": brand,
    }), flush=True)
    raise SystemExit(0)

if args == ["auth", "status", "--verify", "--json"]:
    config = json.loads((config_dir / "config.json").read_text(encoding="utf-8"))
    app = config["apps"][0]
    print(json.dumps({
        "appId": app["appId"],
        "brand": app["brand"],
        "identity": "bot",
        "verified": True,
        "identities": {
            "bot": {
                "status": "ready",
                "available": True,
                "verified": True,
                "openId": "ou_live_new_bot",
            },
            "user": {"status": "not_configured", "available": False},
        },
    }))
    raise SystemExit(0)

if args == ["whoami", "--as", "bot"]:
    config = json.loads((config_dir / "config.json").read_text(encoding="utf-8"))
    app = config["apps"][0]
    print(json.dumps({
        "appId": app["appId"],
        "available": True,
        "brand": app["brand"],
        "defaultAs": "auto",
        "identity": "bot",
        "identitySource": "flag",
        "profile": app["appId"],
        "tokenStatus": "ready",
    }))
    raise SystemExit(0)

if args == [
    "api",
    "GET",
    "/open-apis/application/v6/applications/me",
    "--params",
    '{"lang":"zh_cn","user_id_type":"open_id"}',
    "--as",
    "bot",
    "--format",
    "json",
]:
    config = json.loads((config_dir / "config.json").read_text(encoding="utf-8"))
    app = config["apps"][0]
    creator_id = os.environ.get(
        "FAKE_LIVE_ONBOARDING_CREATOR_OPEN_ID",
        "ou_live_new_owner",
    )
    owner_id = os.environ.get(
        "FAKE_LIVE_ONBOARDING_OWNER_OPEN_ID",
        creator_id,
    )
    app_info = {
        "app_id": os.environ.get(
            "FAKE_LIVE_ONBOARDING_OWNER_APP_ID",
            app["appId"],
        ),
        "creator_id": creator_id,
        "status": int(os.environ.get(
            "FAKE_LIVE_ONBOARDING_APP_STATUS",
            "1",
        )),
        "scene_type": int(os.environ.get(
            "FAKE_LIVE_ONBOARDING_SCENE_TYPE",
            "0",
        )),
    }
    if os.environ.get("FAKE_LIVE_ONBOARDING_OMIT_OWNER") != "1":
        app_info["owner"] = {
            "type": int(os.environ.get(
                "FAKE_LIVE_ONBOARDING_OWNER_TYPE",
                "2",
            )),
            "owner_id": owner_id,
        }
    print(json.dumps({
        "ok": True,
        "identity": "bot",
        "data": {"app": app_info},
    }))
    raise SystemExit(0)

if args == ["config", "remove"]:
    remove_log = os.environ.get("FAKE_LIVE_ONBOARDING_REMOVE_LOG")
    if remove_log:
        Path(remove_log).write_text(str(config_dir), encoding="utf-8")
    print("Configuration removed")
    raise SystemExit(0)

print("unexpected arguments", json.dumps(args))
raise SystemExit(9)
'''


def _fake_binary(tmp_path: Path) -> Path:
    binary = tmp_path / "lark-cli"
    binary.write_text(FAKE_LARK_CLI, encoding="utf-8")
    binary.chmod(0o700)
    return binary


def _configure(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    binary = _fake_binary(tmp_path)
    root = tmp_path / "lark"
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_URL", VERIFICATION_URL)
    return binary, root


def _assert_process_exits(pid: int) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.02)
    pytest.fail(f"onboarding process {pid} survived cancellation")


def test_stage_forwards_exact_url_promptly_and_supports_reversible_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    argv_log = tmp_path / "argv.json"
    release = tmp_path / "release"
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_ARGV_LOG", str(argv_log))
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_RELEASE_FILE", str(release))

    async def scenario() -> None:
        received: list[str] = []

        async def url_ready(url: str) -> None:
            # The fake child cannot complete until this prompt callback runs.
            assert not release.exists()
            received.append(url)
            release.write_text("delivered", encoding="utf-8")

        staged = await stage_lark_qr_onboarding(
            config_root=root,
            profile_name="live-team",
            binary=binary,
            timeout=3,
            on_verification_url=url_ready,
        )

        assert received == [VERIFICATION_URL]
        assert staged.profile_id == "live-team"
        assert staged.app_id == "cli_live_new_app"
        assert staged.brand == "feishu"
        assert staged.bot_open_id == "ou_live_new_bot"
        assert staged.owner_open_id == "ou_live_new_owner"
        assert staged.credential_ref == "keychain:appsecret:cli_live_new_app"
        assert staged.cli_version == "1.0.92"
        assert staged.state == "staged"
        assert staged.staging_path.is_dir()
        assert not staged.final_path.exists()
        assert stat.S_IMODE((staged.staging_path / "cache").stat().st_mode) == 0o700
        assert stat.S_IMODE(
            (staged.staging_path / "cache" / "remote.json").stat().st_mode
        ) == 0o600
        assert stat.S_IMODE(
            (staged.staging_path / "config.json").stat().st_mode
        ) == 0o600

        record = staged.build_profile_record()
        assert record.profile_id == "live-team"
        assert record.channel == "lark"
        assert record.bot_id == staged.app_id
        assert record.config_dir == str(staged.final_path)
        assert record.restart_policy == {
            "max_attempts": 8,
            "base_delay": 1,
            "max_delay": 60,
            "bot_open_id": "ou_live_new_bot",
        }

        assert staged.publish() == record
        assert staged.state == "published"
        assert staged.final_path.is_dir()
        assert not staged.staging_path.exists()
        staged.restore_publication()
        assert staged.state == "staged"
        assert staged.staging_path.is_dir()
        assert not staged.final_path.exists()
        staged.publish()
        assert staged.commit() == record
        assert staged.state == "committed"

    asyncio.run(scenario())

    assert json.loads(argv_log.read_text(encoding="utf-8")) == [
        "config",
        "init",
        "--new",
    ]
    # Commit releases the credential mutation boundary deterministically.
    with CredentialMutationOwnership(root):
        pass


def test_stage_fails_closed_and_retains_credentials_when_app_owner_conflicts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv(
        "FAKE_LIVE_ONBOARDING_OWNER_OPEN_ID",
        "ou_different_owner",
    )

    async def scenario() -> Path:
        with pytest.raises(LarkOnboardingRetainedError) as raised:
            await stage_lark_qr_onboarding(
                config_root=root,
                profile_name="owner-conflict",
                binary=binary,
                timeout=3,
                on_verification_url=lambda _url: None,
            )
        return raised.value.path

    retained = asyncio.run(scenario())

    assert retained.is_dir()
    assert retained.parent == root
    assert retained.name.startswith(".add-owner-conflict-")
    assert not (root / "owner-conflict").exists()
    # Failure releases the process-local credential lock while preserving the
    # only recovery handle; it never publishes an unverified owner profile.
    with CredentialMutationOwnership(root):
        pass


def test_stage_accepts_lark_creator_when_international_owner_field_is_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_BRAND", "lark")
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_OMIT_OWNER", "1")

    async def scenario() -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            profile_name="international-owner",
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        assert staged.brand == "lark"
        assert staged.owner_open_id == "ou_live_new_owner"
        await staged.rollback(StagedCredentialDisposition.SHARED)

    asyncio.run(scenario())

    assert not (root / "international-owner").exists()
    assert not list(root.glob(".add-international-owner-*"))
    with CredentialMutationOwnership(root):
        pass


def test_owner_reverification_accepts_unchanged_staged_and_published_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)

    async def scenario() -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            profile_name="reverified-owner",
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        assert staged.reverify_owner_identity() == "ou_live_new_owner"
        staged.publish()
        assert staged.reverify_owner_identity() == "ou_live_new_owner"
        staged.restore_publication()
        await staged.rollback(StagedCredentialDisposition.SHARED)

    asyncio.run(scenario())

    assert not (root / "reverified-owner").exists()
    assert not list(root.glob(".add-reverified-owner-*"))
    with CredentialMutationOwnership(root):
        pass


def test_owner_reverification_rejects_feishu_owner_transfer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)

    async def scenario() -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            profile_name="transferred-owner",
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        staged.publish()
        monkeypatch.setenv(
            "FAKE_LIVE_ONBOARDING_OWNER_OPEN_ID",
            "ou_transferred_owner",
        )
        with pytest.raises(LarkProfileError, match="verified human app owner"):
            staged.reverify_owner_identity()
        staged.restore_publication()
        await staged.rollback(StagedCredentialDisposition.SHARED)

    asyncio.run(scenario())

    assert not (root / "transferred-owner").exists()
    with CredentialMutationOwnership(root):
        pass


def test_owner_reverification_rejects_changed_lark_creator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_BRAND", "lark")
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_OMIT_OWNER", "1")

    async def scenario() -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            profile_name="changed-creator",
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        staged.publish()
        monkeypatch.setenv(
            "FAKE_LIVE_ONBOARDING_CREATOR_OPEN_ID",
            "ou_changed_creator",
        )
        with pytest.raises(LarkProfileError, match="changed during onboarding"):
            staged.reverify_owner_identity()
        staged.restore_publication()
        await staged.rollback(StagedCredentialDisposition.SHARED)

    asyncio.run(scenario())

    assert not (root / "changed-creator").exists()
    with CredentialMutationOwnership(root):
        pass


def test_shared_credential_rollback_never_invokes_config_remove(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    remove_log = tmp_path / "remove.log"
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_REMOVE_LOG", str(remove_log))

    async def scenario() -> Path:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        path = staged.staging_path
        await staged.rollback(StagedCredentialDisposition.SHARED)
        assert staged.state == "rolled_back"
        return path

    staging_path = asyncio.run(scenario())
    assert not staging_path.exists()
    assert not remove_log.exists()
    with CredentialMutationOwnership(root):
        pass


def test_unowned_credential_rollback_reuses_validated_cli_cleanup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    remove_log = tmp_path / "remove.log"
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_REMOVE_LOG", str(remove_log))

    async def scenario() -> Path:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        path = staged.staging_path
        await staged.rollback(StagedCredentialDisposition.UNOWNED)
        assert staged.state == "rolled_back"
        return path

    staging_path = asyncio.run(scenario())
    assert remove_log.read_text(encoding="utf-8") == str(staging_path)
    assert not staging_path.exists()
    with CredentialMutationOwnership(root):
        pass


def test_unknown_credential_rollback_retains_recovery_handle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)

    async def scenario() -> Path:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        with pytest.raises(LarkOnboardingRetainedError) as raised:
            await staged.rollback(StagedCredentialDisposition.UNKNOWN)
        assert staged.state == "retained"
        assert raised.value.path == staged.staging_path
        return raised.value.path

    retained = asyncio.run(scenario())
    assert retained.is_dir()
    with CredentialMutationOwnership(root):
        pass


@pytest.mark.parametrize("mode", ["hang", "partial"])
def test_external_cancellation_cleans_empty_stage_but_retains_ambiguous_state(
    tmp_path: Path,
    monkeypatch,
    mode: str,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    cancellation = threading.Event()
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_MODE", mode)

    async def url_ready(_url: str) -> None:
        cancellation.set()

    async def scenario() -> None:
        if mode == "partial":
            with pytest.raises(LarkOnboardingRetainedError) as raised:
                await stage_lark_qr_onboarding(
                    config_root=root,
                    binary=binary,
                    timeout=3,
                    cancel_event=cancellation,
                    on_verification_url=url_ready,
                )
            assert raised.value.path.is_dir()
            assert "child-output-must-never-escape" not in str(raised.value)
        else:
            with pytest.raises(LarkProfileError, match="cancelled"):
                await stage_lark_qr_onboarding(
                    config_root=root,
                    binary=binary,
                    timeout=3,
                    cancel_event=cancellation,
                    on_verification_url=url_ready,
                )
            assert not list(root.glob(".add-*"))

    asyncio.run(scenario())
    with CredentialMutationOwnership(root):
        pass


def test_task_cancellation_terminates_child_process_group_and_cleans_stage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    pid_file = tmp_path / "child.pid"
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_MODE", "hang")
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_PID_FILE", str(pid_file))

    async def scenario() -> None:
        url_seen = asyncio.Event()

        async def url_ready(_url: str) -> None:
            url_seen.set()

        task = asyncio.create_task(
            stage_lark_qr_onboarding(
                config_root=root,
                binary=binary,
                timeout=30,
                on_verification_url=url_ready,
            )
        )
        await asyncio.wait_for(url_seen.wait(), timeout=3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    _assert_process_exits(int(pid_file.read_text(encoding="utf-8")))
    assert not list(root.glob(".add-*"))
    with CredentialMutationOwnership(root):
        pass


def test_qr_timeout_is_safe_and_does_not_leave_empty_staging(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_MODE", "hang")

    async def scenario() -> None:
        with pytest.raises(LarkProfileError, match="timed out"):
            await stage_lark_qr_onboarding(
                config_root=root,
                binary=binary,
                timeout=0.2,
                on_verification_url=lambda _url: None,
            )

    asyncio.run(scenario())
    assert not list(root.glob(".add-*"))


def test_url_callback_failure_is_redacted_and_cancels_onboarding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_MODE", "hang")

    async def fail_callback(_url: str) -> None:
        raise RuntimeError("app_secret=callback-secret-must-not-escape")

    async def scenario() -> None:
        with pytest.raises(LarkProfileError) as raised:
            await stage_lark_qr_onboarding(
                config_root=root,
                binary=binary,
                timeout=3,
                on_verification_url=fail_callback,
            )
        assert str(raised.value) == "could not deliver the Lark verification URL"

    asyncio.run(scenario())
    assert not list(root.glob(".add-*"))


def test_nonofficial_or_missing_url_is_never_forwarded_and_retains_credentials(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, root = _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_MODE", "missing-url")
    received: list[str] = []

    async def scenario() -> None:
        with pytest.raises(LarkOnboardingRetainedError) as raised:
            await stage_lark_qr_onboarding(
                config_root=root,
                binary=binary,
                timeout=3,
                on_verification_url=received.append,
            )
        assert raised.value.path.is_dir()

    asyncio.run(scenario())
    assert received == []


def test_chat_safe_error_boundary_hides_paths_urls_secrets_and_unknown_errors(
    tmp_path: Path,
) -> None:
    retained = LarkOnboardingRetainedError(
        tmp_path / ".add-private-profile-private123"
    )
    assert str(tmp_path) not in str(retained)
    assert public_onboarding_error(retained) == (
        "Lark bot onboarding did not complete safely; private staged "
        "credentials were retained for owner recovery"
    )

    profile_error = LarkProfileError(
        "retry https://open.feishu.cn/page/cli?user_code=PRIVATE at "
        f"{tmp_path}/.add-private-profile-private123; app_secret=do-not-show"
    )
    public = public_onboarding_error(profile_error)
    assert "PRIVATE" not in public
    assert str(tmp_path) not in public
    assert ".add-private" not in public
    assert "do-not-show" not in public
    assert public == "Lark bot onboarding failed"
    version_error = LarkProfileError(
        "lark-cli returned an unrecognized version response: "
        "UNLABELLED-SENTINEL-SECRET"
    )
    assert public_onboarding_error(version_error) == "Lark bot onboarding failed"
    assert "UNLABELLED" not in public_onboarding_error(version_error)
    assert public_onboarding_error(RuntimeError("unknown-secret")) == (
        "Lark bot onboarding failed"
    )
    assert len(public_onboarding_error(profile_error, maximum=17)) <= 17


def test_stage_rejects_symlink_config_root_without_starting_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary, _root = _configure(tmp_path, monkeypatch)
    real_root = tmp_path / "real-lark"
    real_root.mkdir(mode=0o700)
    linked_root = tmp_path / "linked-lark"
    linked_root.symlink_to(real_root, target_is_directory=True)
    argv_log = tmp_path / "argv.json"
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_ARGV_LOG", str(argv_log))

    async def scenario() -> None:
        with pytest.raises(LarkProfileError, match="real directory"):
            await stage_lark_qr_onboarding(
                config_root=linked_root,
                binary=binary,
                timeout=3,
                on_verification_url=lambda _url: None,
            )

    asyncio.run(scenario())
    assert not argv_log.exists()
    assert list(real_root.iterdir()) == []


def test_database_free_cleanup_facade_rejects_symlink_config_root(
    tmp_path: Path,
) -> None:
    real_root = tmp_path / "real-lark"
    real_root.mkdir(mode=0o700)
    linked_root = tmp_path / "linked-lark"
    linked_root.symlink_to(real_root, target_is_directory=True)

    with pytest.raises(LarkProfileError, match="real directory"):
        cleanup_unowned_provisioned_config(
            "lark-cli",
            linked_root,
            linked_root / ".add-team-private123",
        )


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
def test_stopped_runtime_socket_is_narrowly_removed_before_restore(
    tmp_path: Path,
    monkeypatch,
) -> None:
    binary = _fake_binary(tmp_path)
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_URL", VERIFICATION_URL)

    async def scenario(root: Path) -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        # Keep the configured spelling even where macOS exposes /tmp through
        # /private/tmp.  Durable profile identity and direct-child rollback
        # must use one consistent parent representation.
        assert staged.final_path.parent == root
        assert staged.staging_path.parent == root
        staged.publish()
        socket_parent = staged.final_path / "events" / staged.app_id
        socket_parent.mkdir(parents=True, mode=0o700)
        os.chmod(socket_parent.parent, 0o700)
        os.chmod(socket_parent, 0o700)
        bus_path = socket_parent / "bus.sock"
        bus = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        bus.bind(str(bus_path))
        os.chmod(bus_path, 0o600)
        try:
            with pytest.raises(LarkProfileError, match="unsafe staged"):
                staged.restore_publication()
            staged.remove_runtime_artifacts_after_stop()
            assert not bus_path.exists()
            staged.restore_publication()
            await staged.rollback(StagedCredentialDisposition.SHARED)
        finally:
            bus.close()

    with tempfile.TemporaryDirectory(prefix="cow-lark-live-", dir="/tmp") as value:
        asyncio.run(scenario(Path(value) / "lark"))


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "AF_UNIX"),
    reason="filesystem Unix sockets are POSIX-specific",
)
@pytest.mark.parametrize("impostor", ["regular", "wrong-app-socket"])
def test_runtime_socket_cleanup_rejects_and_preserves_impostors(
    tmp_path: Path,
    monkeypatch,
    impostor: str,
) -> None:
    binary = _fake_binary(tmp_path)
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    monkeypatch.setenv("FAKE_LIVE_ONBOARDING_URL", VERIFICATION_URL)

    async def scenario(root: Path) -> None:
        staged = await stage_lark_qr_onboarding(
            config_root=root,
            binary=binary,
            timeout=3,
            on_verification_url=lambda _url: None,
        )
        staged.publish()
        app_id = staged.app_id if impostor == "regular" else "cli_other_app"
        socket_parent = staged.final_path / "events" / app_id
        socket_parent.mkdir(parents=True, mode=0o700)
        os.chmod(socket_parent.parent, 0o700)
        os.chmod(socket_parent, 0o700)
        bus_path = socket_parent / "bus.sock"
        bus: socket.socket | None = None
        if impostor == "regular":
            bus_path.write_bytes(b"not a socket")
            os.chmod(bus_path, 0o600)
        else:
            bus = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            bus.bind(str(bus_path))
            os.chmod(bus_path, 0o600)
        try:
            with pytest.raises(LarkProfileError):
                staged.remove_runtime_artifacts_after_stop()
            assert bus_path.exists()
            retained = staged.retain()
            assert retained == staged.final_path
        finally:
            if bus is not None:
                bus.close()

    with tempfile.TemporaryDirectory(prefix="cow-lark-live-", dir="/tmp") as value:
        asyncio.run(scenario(Path(value) / "lark"))
