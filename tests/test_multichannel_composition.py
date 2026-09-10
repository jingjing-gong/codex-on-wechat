"""Production bootstrap discovery and account-set convergence tests."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.codex_wechat_bot as bot
from src.runtime.models import BotProfileRecord
from src.runtime.sqlite_store import SQLiteStore


def _profile(tmp_path: Path, profile_id: str, app_id: str) -> BotProfileRecord:
    config = (tmp_path / profile_id).resolve()
    return BotProfileRecord(
        profile_id=profile_id,
        channel="lark",
        bot_id=app_id,
        brand="lark",
        config_dir=str(config),
        config_dir_identity=f"path:{profile_id}",
        cli_version="1.0.92",
        credential_ref=f"keychain:appsecret:{app_id}",
        restart_policy={
            "max_attempts": 3,
            "base_delay": 0.1,
            "max_delay": 1.0,
            "bot_open_id": f"ou_{profile_id}_bot",
        },
    )


def test_read_only_bootstrap_discovers_every_enabled_lark_profile(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            await store.create_bot_profile(
                _profile(tmp_path, "first", "cli_first_app")
            )
            await store.create_bot_profile(
                _profile(tmp_path, "second", "cli_second_app")
            )
            await store.set_bot_profile_enabled("second", False)
        finally:
            await store.close()

    asyncio.run(seed())
    profiles = bot._configured_lark_profiles(database)
    assert [(row["profile_id"], row["app_id"]) for row in profiles] == [
        ("first", "cli_first_app")
    ]


def test_bootstrap_discovery_fails_closed_on_corrupt_v36_schema(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE bot_profile_status")
    connection.execute("DROP TABLE bot_profiles")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="schema v36 is missing"):
        bot._configured_lark_profiles(database)


def test_bootstrap_discovery_rejects_malformed_profile_json(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            await store.create_bot_profile(
                _profile(tmp_path, "first", "cli_first_app")
            )
        finally:
            await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA ignore_check_constraints=ON")
    connection.execute(
        "UPDATE bot_profiles SET restart_policy_json='not-json' "
        "WHERE profile_id='first'"
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="malformed restart policy"):
        bot._configured_lark_profiles(database)


def test_account_lock_discovery_retries_until_locked_snapshot_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = ({"profile_id": "a", "app_id": "cli_first_app"},)
    second = ({"profile_id": "b", "app_id": "cli_second_app"},)
    snapshots = iter((first, second, second))
    monkeypatch.setattr(
        bot,
        "_configured_lark_profiles",
        lambda _database: next(snapshots),
    )
    events: list[tuple[str, object]] = []

    class FakeOwnership:
        def __init__(self, _database, *, accounts=None, channel=None, bot_id=None):
            self.accounts = tuple(accounts or ((channel, bot_id),))
            self.held = False
            self.owner_instance_id = "owner"
            events.append(("construct", self.accounts))

        def acquire(self):
            self.held = True
            events.append(("acquire", self.accounts))
            return self

        def close(self):
            self.held = False
            events.append(("close", self.accounts))

    monkeypatch.setattr(bot, "SupervisorAccountSetOwnership", FakeOwnership)
    monkeypatch.setattr(bot, "SupervisorOwnership", FakeOwnership)

    ownership, profiles = bot._acquire_runtime_ownership(
        tmp_path / "runtime.sqlite",
        wechat_bot_id="wechat-bot",
    )
    assert profiles == second
    assert ownership.held
    assert ownership.accounts == (
        ("wechat", "wechat-bot"),
        ("lark", "cli_second_app"),
    )
    assert ("close", (("wechat", "wechat-bot"), ("lark", "cli_first_app"))) in events


def test_lark_upload_callback_resolves_bytes_through_shared_attachment_store() -> None:
    class ManagedAttachments:
        def __init__(self) -> None:
            self.requested: list[str] = []
            self.read: list[str] = []

        async def aget(self, attachment_id: str):
            self.requested.append(attachment_id)
            return SimpleNamespace(
                path="/managed/verified/image.png",
                mime_type="image/png",
                filename="image.png",
            )

        async def aread_bytes(self, attachment_id: str) -> bytes:
            self.read.append(attachment_id)
            return b"verified-image-bytes"

    class Client:
        def __init__(self) -> None:
            self.uploads: list[tuple[object, ...]] = []

        async def upload_media(self, path, *, kind, filename, mime_type):
            snapshot = Path(path)
            self.uploads.append(
                (
                    snapshot.read_bytes(),
                    snapshot.suffix,
                    kind,
                    filename,
                    mime_type,
                )
            )
            return {"remote_id": "img_uploaded", "kind": kind}

    async def scenario() -> None:
        managed = ManagedAttachments()
        client = Client()
        uploader, _sender, _text_sender = bot._build_lark_media_callbacks(
            managed, client
        )
        uploaded = await uploader(
            SimpleNamespace(
                attachment_id="attachment-verified",
                metadata={"kind": "image", "path": "/attacker/bypass.png"},
            )
        )
        assert uploaded["remote_id"] == "img_uploaded"
        assert managed.requested == ["attachment-verified"]
        assert managed.read == ["attachment-verified"]
        assert client.uploads == [
            (
                b"verified-image-bytes",
                ".png",
                "image",
                "image.png",
                "image/png",
            )
        ]

    asyncio.run(scenario())


def test_live_lark_composition_shares_one_service_across_account_gateways(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed: dict[str, object] = {}

    class Controller:
        def __init__(self, *args, **kwargs):
            constructed["controller_args"] = args
            constructed["controller_kwargs"] = kwargs

    class Service:
        def __init__(self, **kwargs):
            constructed["service_kwargs"] = kwargs

    monkeypatch.setattr(bot, "LarkRuntimeAccountController", Controller)
    monkeypatch.setattr(bot, "LarkChatOnboardingService", Service)
    monkeypatch.setattr(bot, "lark_config_root", lambda: tmp_path / "lark")

    store = object()
    manager = object()
    attachments = object()
    ownership = object()
    resolver = object()
    administrator = object()
    controller, service = bot._build_lark_runtime_composition(
        store=store,
        manager=manager,
        attachment_store=attachments,
        ownership=ownership,
        principal_resolver=resolver,
        administrator=administrator,
        executable="pinned-lark-cli",
        shell_cwd=tmp_path,
    )

    assert isinstance(controller, Controller)
    assert isinstance(service, Service)
    controller_args = constructed["controller_args"]
    controller_kwargs = constructed["controller_kwargs"]
    service_kwargs = constructed["service_kwargs"]
    assert controller_args == (store, manager, resolver, attachments, ownership)
    assert controller_kwargs["administrator"] is administrator
    assert controller_kwargs["executable"] == "pinned-lark-cli"
    assert controller_kwargs["media_callbacks_factory"] is bot._build_lark_media_callbacks
    assert controller_kwargs["onboarding_service_factory"](
        SimpleNamespace(profile_id="existing")
    ) is service
    assert service_kwargs["account_controller"] is controller
    assert service_kwargs["principal_resolver"] is resolver
    assert service_kwargs["ownership"] is ownership
    assert service_kwargs["config_root"] == tmp_path / "lark"


def test_startup_lark_accounts_all_preflight_before_any_activation(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, object]] = []

    class Controller:
        async def prepare(self, profile, *, acquire_ownership, preflight):
            events.append(("prepare", profile.profile_id))
            assert acquire_ownership is False
            assert preflight is True
            return f"handle:{profile.profile_id}"

        async def activate(self, handle):
            events.append(("activate", handle))
            return handle

    profiles = tuple(
        {
            "profile_id": name,
            "app_id": app_id,
            "config_dir": str(tmp_path / name),
        }
        for name, app_id in (
            ("first", "cli_first_startup"),
            ("second", "cli_second_startup"),
        )
    )
    result = asyncio.run(
        bot._start_configured_lark_accounts(Controller(), profiles)
    )
    assert result == ("handle:first", "handle:second")
    assert events[:2] == [("prepare", "first"), ("prepare", "second")]
    assert {event for event in events[2:]} == {
        ("activate", "handle:first"),
        ("activate", "handle:second"),
    }


def test_lark_shutdown_drains_onboarding_before_accounts_and_attempts_both() -> None:
    events: list[str] = []

    class Service:
        async def stop(self):
            events.append("onboarding")
            raise RuntimeError("onboarding-stop")

    class Controller:
        async def stop_all(self):
            events.append("accounts")

    with pytest.raises(RuntimeError, match="onboarding-stop"):
        asyncio.run(bot._stop_lark_runtime_composition(Service(), Controller()))
    assert events == ["onboarding", "accounts"]
