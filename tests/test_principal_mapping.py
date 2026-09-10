"""Canonical principals authorize actors without changing transport routing."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.identity import AuthenticatedActor, PrincipalResolver
from src.runtime.sqlite_store import SQLiteStore


def test_mapping_is_exact_bot_local_and_requires_lark_open_id(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_principal(
                principal_id="person:alice", display_name="Alice"
            )
            account = await store.map_principal_account(
                principal_id="person:alice",
                channel="lark",
                bot_id="cli_bot_a",
                external_user_id="ou_same_user",
                identifier_kind="open_id",
                configured_by="local-owner",
            )
            resolver = PrincipalResolver(store)

            resolved = await resolver.resolve(
                AuthenticatedActor("lark", "cli_bot_a", "ou_same_user")
            )
            assert resolved.principal_id == "person:alice"
            assert resolved.principal_account_id == account.principal_account_id
            assert resolved.source == "configured"

            # The same-looking sender on a peer bot or channel is a distinct
            # authenticated transport account until the owner maps it.
            assert not (
                await resolver.resolve(
                    channel="lark",
                    bot_id="cli_bot_b",
                    external_user_id="ou_same_user",
                )
            ).resolved
            assert not (
                await resolver.resolve(
                    channel="wechat",
                    bot_id="cli_bot_a",
                    external_user_id="ou_same_user",
                )
            ).resolved

            with pytest.raises(ValueError, match="stable open_id"):
                await store.map_principal_account(
                    principal_id="person:alice",
                    channel="lark",
                    bot_id="cli_bot_b",
                    external_user_id="on_unstable_union_id",
                    identifier_kind="union_id",
                    configured_by="local-owner",
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_remap_is_revisioned_and_disabled_principal_does_not_authorize(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_principal(principal_id="person:old")
            await store.create_principal(principal_id="person:new")
            old = await store.map_principal_account(
                principal_id="person:old",
                channel="lark",
                bot_id="cli_bot_a",
                external_user_id="ou_actor",
                identifier_kind="open_id",
                configured_by="local-owner",
            )
            new = await store.map_principal_account(
                principal_id="person:new",
                channel="lark",
                bot_id="cli_bot_a",
                external_user_id="ou_actor",
                identifier_kind="open_id",
                configured_by="local-owner",
            )
            assert old.mapping_revision == 1
            assert new.mapping_revision == 2
            history = await store.list_principal_accounts(active=None)
            assert [(row.principal_id, row.active) for row in history] == [
                ("person:old", False),
                ("person:new", True),
            ]

            resolver = PrincipalResolver(store)
            assert (
                await resolver.resolve(
                    channel="lark",
                    bot_id="cli_bot_a",
                    external_user_id="ou_actor",
                )
            ).principal_id == "person:new"
            await store.update_principal("person:new", enabled=False)
            assert not (
                await resolver.resolve(
                    channel="lark",
                    bot_id="cli_bot_a",
                    external_user_id="ou_actor",
                )
            ).resolved
        finally:
            await store.close()

    asyncio.run(scenario())


def test_resolver_rejects_backend_account_substitution() -> None:
    class MaliciousStore:
        async def resolve_principal_account(self, **_scope):
            return {
                "principal_account_id": "mapping-1",
                "principal_id": "person:spoofed",
                "channel": "lark",
                "bot_id": "cli_other_bot",
                "external_user_id": "ou_actor",
                "identifier_kind": "open_id",
                "mapping_revision": 1,
                "active": True,
                "principal_enabled": True,
            }

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="account mismatch"):
            await PrincipalResolver(MaliciousStore()).resolve(
                channel="lark",
                bot_id="cli_expected_bot",
                external_user_id="ou_actor",
            )

    asyncio.run(scenario())
