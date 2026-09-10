"""Supervisor-owned automatic principal mapping from durable senders."""

from __future__ import annotations

import asyncio

from src.channels.models import InboundEnvelope
from src.runtime.sqlite_store import SQLiteStore


async def _inbound(
    store: SQLiteStore,
    *,
    channel: str,
    bot_id: str,
    external_user_id: str,
    message_id: str,
) -> None:
    await store.store_inbound(
        InboundEnvelope(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            external_message_id=message_id,
            text="hello",
        )
    )


def test_single_sender_account_is_automatically_mapped_to_owner(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        await store.initialize()
        try:
            await _inbound(
                store,
                channel="lark",
                bot_id="cli_team_bot",
                external_user_id="ou_owner1234",
                message_id="om_one",
            )

            results = await store.auto_map_owner_principal_accounts(
                accounts=(("lark", "cli_team_bot"),)
            )

            assert results[0]["outcome"] == "mapped"
            assert results[0]["identifier_kind"] == "open_id"
            mapping = await store.resolve_principal_account(
                channel="lark",
                bot_id="cli_team_bot",
                external_user_id="ou_owner1234",
            )
            assert mapping is not None
            assert mapping.principal_id == "owner"
            assert mapping.identifier_kind == "open_id"
            assert mapping.configured_by == "supervisor:auto-map"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_multi_sender_account_is_skipped(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        await store.initialize()
        try:
            await _inbound(
                store,
                channel="lark",
                bot_id="cli_shared_bot",
                external_user_id="ou_first1234",
                message_id="om_first",
            )
            await _inbound(
                store,
                channel="lark",
                bot_id="cli_shared_bot",
                external_user_id="ou_second1234",
                message_id="om_second",
            )

            results = await store.auto_map_owner_principal_accounts(
                accounts=(("lark", "cli_shared_bot"),)
            )

            assert results[0]["outcome"] == "multiple_senders"
            assert results[0]["sender_count"] == 2
            assert await store.list_principal_accounts(active=True) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_already_mapped_account_is_an_idempotent_noop(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        await store.initialize()
        try:
            await store.create_principal(principal_id="owner")
            existing = await store.map_principal_account(
                principal_id="owner",
                channel="lark",
                bot_id="cli_mapped_bot",
                external_user_id="ou_owner1234",
                identifier_kind="open_id",
                configured_by="local-owner:test",
            )
            await _inbound(
                store,
                channel="lark",
                bot_id="cli_mapped_bot",
                external_user_id="ou_owner1234",
                message_id="om_existing",
            )

            first = await store.auto_map_owner_principal_accounts(
                accounts=(("lark", "cli_mapped_bot"),)
            )
            second = await store.auto_map_owner_principal_accounts(
                accounts=(("lark", "cli_mapped_bot"),)
            )

            assert first[0]["outcome"] == "already_mapped"
            assert second[0]["outcome"] == "already_mapped"
            accounts = await store.list_principal_accounts(active=None)
            assert len(accounts) == 1
            assert accounts[0].principal_account_id == existing.principal_account_id
            assert accounts[0].mapping_revision == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_wechat_auto_mapping_uses_from_user_id_identifier_kind(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        await store.initialize()
        try:
            await _inbound(
                store,
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                message_id="wechat-message",
            )

            results = await store.auto_map_owner_principal_accounts(
                accounts=(("wechat", "wechat-bot"),)
            )
            mapping = await store.resolve_principal_account(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
            )

            assert results[0]["outcome"] == "mapped"
            assert results[0]["identifier_kind"] == "from_user_id"
            assert mapping is not None
            assert mapping.identifier_kind == "from_user_id"
        finally:
            await store.close()

    asyncio.run(scenario())
