"""Owner CLI coverage for channel-neutral principal account mappings."""

from __future__ import annotations

import asyncio
import io

import pytest

from src.channels.models import InboundEnvelope
from src.principal_cli import (
    PrincipalAccountAdmin,
    PrincipalAccountAdminError,
    build_parser,
)
from src.runtime.identity import AuthenticatedActor, PrincipalResolver
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.supervisor import (
    SupervisorAccountSetOwnership,
    SupervisorOwnershipConflict,
)


def _admin(tmp_path, monkeypatch) -> tuple[PrincipalAccountAdmin, io.StringIO]:
    monkeypatch.setenv("CODEX_WECHAT_LOCK_ROOT", str(tmp_path / "locks"))
    output = io.StringIO()
    return (
        PrincipalAccountAdmin(
            database=tmp_path / "runtime.sqlite3",
            output=output,
        ),
        output,
    )


def test_owner_cli_maps_lists_and_unmaps_exact_wechat_identity_without_profile(
    tmp_path,
    monkeypatch,
) -> None:
    admin, output = _admin(tmp_path, monkeypatch)

    mapped = admin.map(
        "person:owner",
        "WeChat",
        "ilink-live-bot",
        "wx-live-user",
        identifier_kind="from_user_id",
        display_name="Owner",
    )

    assert mapped.principal_id == "person:owner"
    assert mapped.channel == "wechat"
    assert mapped.bot_id == "ilink-live-bot"
    assert mapped.external_user_id == "wx-live-user"
    assert mapped.identifier_kind == "from_user_id"
    assert mapped.mapping_revision == 1
    assert mapped.configured_by.startswith("local-owner")

    async def verify_mapping() -> None:
        store = SQLiteStore(admin.database)
        await store.initialize()
        try:
            # A generic WeChat mapping does not invent or require a bot profile.
            assert await store.list_bot_profiles(include_removed=True) == []
            resolved = await PrincipalResolver(store).resolve(
                AuthenticatedActor("wechat", "ilink-live-bot", "wx-live-user")
            )
            assert resolved.principal_id == "person:owner"
            assert resolved.principal_account_id == mapped.principal_account_id
            assert not (
                await PrincipalResolver(store).resolve(
                    channel="wechat",
                    bot_id="ilink-peer-bot",
                    external_user_id="wx-live-user",
                )
            ).resolved
            assert not (
                await PrincipalResolver(store).resolve(
                    channel="wechat",
                    bot_id="ilink-live-bot",
                    external_user_id="wx-other-user",
                )
            ).resolved
        finally:
            await store.close()

    asyncio.run(verify_mapping())

    rows = admin.list("person:owner")
    assert rows == [(rows[0][0], mapped)]
    assert "CHANNEL\tBOT ID\tEXTERNAL USER ID\tIDENTIFIER KIND" in output.getvalue()
    assert "wechat\tilink-live-bot\twx-live-user\tfrom_user_id" in output.getvalue()

    assert admin.unmap("wechat", "ilink-live-bot", "wx-live-user")

    async def verify_unmapped() -> None:
        store = SQLiteStore(admin.database)
        await store.initialize()
        try:
            assert await store.resolve_principal_account(
                channel="wechat",
                bot_id="ilink-live-bot",
                external_user_id="wx-live-user",
            ) is None
        finally:
            await store.close()

    asyncio.run(verify_unmapped())


def test_owner_cli_remaps_exact_account_with_durable_revision(
    tmp_path,
    monkeypatch,
) -> None:
    admin, _output = _admin(tmp_path, monkeypatch)
    first = admin.map(
        "person:first",
        "wechat",
        "bot-a",
        "user-a",
    )
    second = admin.map(
        "person:second",
        "wechat",
        "bot-a",
        "user-a",
    )

    assert first.mapping_revision == 1
    assert second.mapping_revision == 2
    assert second.principal_id == "person:second"

    async def verify_history() -> None:
        store = SQLiteStore(admin.database)
        await store.initialize()
        try:
            history = await store.list_principal_accounts(active=None)
            assert [(row.principal_id, row.active) for row in history] == [
                ("person:first", False),
                ("person:second", True),
            ]
        finally:
            await store.close()

    asyncio.run(verify_history())


def test_mapping_requires_exclusive_database_and_channel_account_ownership(
    tmp_path,
    monkeypatch,
) -> None:
    admin, _output = _admin(tmp_path, monkeypatch)

    with SupervisorAccountSetOwnership(
        admin.database,
        accounts=(("wechat", "live-bot"),),
    ):
        with pytest.raises(SupervisorOwnershipConflict):
            admin.map(
                "person:owner",
                "wechat",
                "live-bot",
                "live-user",
            )

    assert not admin.database.exists()


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("person:owner", "", "bot", "user"), "channel is required"),
        (("person:owner", "wechat", "bot\nspoof", "user"), "control character"),
        (("bad principal!", "wechat", "bot", "user"), "principal ID"),
    ],
)
def test_mapping_rejects_ambiguous_or_log_injectable_identity_components(
    tmp_path,
    monkeypatch,
    arguments,
    message,
) -> None:
    admin, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(PrincipalAccountAdminError, match=message):
        admin.map(*arguments)

    assert not admin.database.exists()


def test_generic_surface_preserves_lark_stable_open_id_contract(
    tmp_path,
    monkeypatch,
) -> None:
    admin, _output = _admin(tmp_path, monkeypatch)

    with pytest.raises(PrincipalAccountAdminError, match="stable ou_"):
        admin.map(
            "person:owner",
            "lark",
            "cli_live_bot",
            "union_unstable",
            identifier_kind="union_id",
        )

    assert not admin.database.exists()


def test_generic_list_shows_owner_account_created_with_lark_profile(
    tmp_path,
    monkeypatch,
) -> None:
    """The atomic add path must be visible through the generic principal CLI."""

    admin, output = _admin(tmp_path, monkeypatch)

    async def seed() -> None:
        store = SQLiteStore(admin.database)
        await store.initialize()
        try:
            await store.create_bot_profile_with_principal_account(
                {
                    "profile_id": "personal",
                    "channel": "lark",
                    "bot_id": "cli_personal_app",
                    "brand": "feishu",
                    "config_dir": str(tmp_path / "lark" / "personal"),
                    "config_dir_identity": "device:personal",
                    "cli_version": "1.0.92",
                    "credential_ref": "keychain:appsecret:cli_personal_app",
                },
                external_user_id="ou_personal_owner",
                configured_by="local-owner:test",
            )
        finally:
            await store.close()

    asyncio.run(seed())

    rows = admin.list("owner")

    assert len(rows) == 1
    principal, account = rows[0]
    assert principal.principal_id == "owner"
    assert account is not None
    assert account.channel == "lark"
    assert account.bot_id == "cli_personal_app"
    assert account.external_user_id == "ou_personal_owner"
    assert (
        "owner\tenabled\tlark\tcli_personal_app\tou_personal_owner\topen_id\t1"
        in output.getvalue()
    )


def test_generic_parser_keeps_exact_account_components_and_default_kind() -> None:
    arguments = build_parser().parse_args(
        ["map", "person:owner", "wechat", "bot-id", "user-id"]
    )

    assert arguments.command == "map"
    assert arguments.principal_id == "person:owner"
    assert arguments.channel == "wechat"
    assert arguments.bot_id == "bot-id"
    assert arguments.external_user_id == "user-id"
    assert arguments.identifier_kind == "external_user_id"


def test_owner_cli_can_adopt_existing_wechat_history_anchor(
    tmp_path,
    monkeypatch,
) -> None:
    admin, output = _admin(tmp_path, monkeypatch)

    async def seed() -> str:
        store = SQLiteStore(admin.database)
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundEnvelope(
                    channel="wechat",
                    bot_id="live-bot",
                    external_user_id="live-user",
                    external_message_id="before-mapping",
                    text="existing history",
                )
            )
            assert accepted.task is not None
            return accepted.task.conversation_id
        finally:
            await store.close()

    local_conversation = asyncio.run(seed())
    admin.map(
        "owner",
        "wechat",
        "live-bot",
        "live-user",
        identifier_kind="from_user_id",
    )
    adopted = admin.adopt(
        "owner",
        "codex",
        "wechat",
        "live-bot",
        "live-user",
    )

    assert adopted["conversation_id"] == local_conversation
    assert adopted["binding_kind"] == "adopted"
    assert "adopted wechat/live-bot/live-user history" in output.getvalue()
