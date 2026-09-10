from __future__ import annotations

import asyncio
import sqlite3

import pytest

from src.channels.models import InboundEnvelope
from src.runtime.models import AgentTask, ReplyTarget
from src.runtime.sqlite_store import StoreError, SQLiteStore, _transaction


_V36_CRITICAL_UNIQUE_INDEXES = (
    (
        "idx_principal_accounts_one_active",
        "principal_accounts",
        ("channel", "bot_id", "external_user_id"),
        "active = 1",
    ),
    (
        "idx_bot_profiles_live_account",
        "bot_profiles",
        ("channel", "bot_id"),
        "removed_at IS NULL",
    ),
    (
        "idx_bot_profiles_live_config_dir",
        "bot_profiles",
        ("config_dir",),
        "removed_at IS NULL",
    ),
    (
        "idx_bot_profiles_live_config_identity",
        "bot_profiles",
        ("config_dir_identity",),
        "removed_at IS NULL",
    ),
)


def _profile(profile_id: str = "team") -> dict[str, object]:
    return {
        "profile_id": profile_id,
        "channel": "lark",
        "bot_id": "cli_botaccount1",
        "brand": "lark",
        "config_dir": f"/private/lark/{profile_id}",
        "config_dir_identity": f"device:private-lark-{profile_id}",
        "cli_version": "1.2.3",
        "credential_ref": f"keychain:lark/{profile_id}",
        "mention_policy": "direct_or_mention",
        "access_policy": "all",
        "restart_policy": {"max_attempts": 8},
    }


def _chat_profile(profile_id: str = "team") -> dict[str, object]:
    profile = _profile(profile_id)
    profile["restart_policy"] = {
        **dict(profile["restart_policy"]),
        "bot_open_id": "ou_newappbot1234",
    }
    return profile


def _pending_task() -> AgentTask:
    return AgentTask(
        task_id="profile-pending-task",
        execution_id="",
        agent_id="codex",
        conversation_id="profile-pending-conversation",
        mode_id="chat",
        profile_version=1,
        policy_version=1,
        reply_target=ReplyTarget(
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id="ou_actor1234",
            destination_kind="direct",
            destination_id="oc_chat1234",
        ),
        inputs={"text": "pending"},
    )


def test_bot_profile_crud_uniqueness_generation_and_pending_removal(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            created = await store.create_bot_profile(_profile())
            assert created.profile_id == "team"
            assert created.bot_id == "cli_botaccount1"
            assert created.restart_policy == {"max_attempts": 8}
            assert await store.create_bot_profile(_profile()) == created

            status = await store.get_bot_profile_status("team")
            assert status is not None
            assert (status.onboarding_state, status.connection_state) == (
                "registered",
                "not_started",
            )
            assert status.generation == 0

            duplicate_account = _profile("duplicate-account")
            duplicate_account["config_dir"] = "/private/lark/other"
            duplicate_account["config_dir_identity"] = "device:other"
            duplicate_account["credential_ref"] = "keychain:lark/other"
            with pytest.raises(StoreError, match="already in use"):
                await store.create_bot_profile(duplicate_account)

            duplicate_config = _profile("duplicate-config")
            duplicate_config["bot_id"] = "cli_botaccount2"
            duplicate_config["config_dir"] = created.config_dir
            duplicate_config["config_dir_identity"] = created.config_dir_identity
            duplicate_config["credential_ref"] = "keychain:lark/config"
            with pytest.raises(StoreError, match="already in use"):
                await store.create_bot_profile(duplicate_config)

            ready = await store.set_bot_profile_status(
                "team",
                onboarding_state="registered",
                connection_state="ready",
                generation=3,
                last_ready_at="2026-08-31T00:00:00+00:00",
            )
            assert ready.generation == 3
            with pytest.raises(StoreError, match="cannot regress"):
                await store.set_bot_profile_status(
                    "team", connection_state="failed", generation=2
                )

            disabled = await store.disable_bot_profile("team")
            assert disabled.enabled is False
            disabled_status = await store.get_bot_profile_status("team")
            assert disabled_status is not None
            assert disabled_status.generation == 4
            assert disabled_status.connection_state == "disabled"
            enabled = await store.enable_bot_profile("team")
            assert enabled.enabled is True
            enabled_status = await store.get_bot_profile_status("team")
            assert enabled_status is not None
            assert enabled_status.generation == 5
            assert enabled_status.connection_state == "not_started"

            reauthorized = await store.update_bot_profile_credentials(
                "team",
                "keychain:lark/team-v2",
                "1.2.4",
                bot_open_id="ou_bot1234",
            )
            assert reauthorized.credential_ref == "keychain:lark/team-v2"
            assert reauthorized.cli_version == "1.2.4"
            assert reauthorized.restart_policy["bot_open_id"] == "ou_bot1234"
            reauthorized_status = await store.get_bot_profile_status("team")
            assert reauthorized_status is not None
            assert reauthorized_status.generation == 6

            pending = await store.create_task(_pending_task())
            with pytest.raises(StoreError, match="pending work"):
                await store.remove_bot_profile("team")
            assert await store.cancel_task(pending.task_id)
            assert await store.remove_bot_profile("team")
            assert await store.get_bot_profile("team") is None
            removed = await store.get_bot_profile("team", include_removed=True)
            assert removed is not None and removed.removed_at is not None
            removed_status = await store.get_bot_profile_status("team")
            assert removed_status is not None
            assert removed_status.connection_state == "disabled"
            assert removed_status.generation == 7

            # Live-only uniqueness permits a deliberate fresh profile after
            # the prior one is safely tombstoned.
            replacement = _profile("replacement")
            replacement["config_dir"] = created.config_dir
            replacement["config_dir_identity"] = created.config_dir_identity
            replacement["credential_ref"] = "keychain:lark/replacement"
            assert (
                await store.create_bot_profile(replacement)
            ).bot_id == created.bot_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_live_onboarding_profile_create_is_strict_and_rollback_is_retryable(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            created = await store.create_bot_profile_for_onboarding(_profile())
            with pytest.raises(StoreError, match="already exists"):
                await store.create_bot_profile_for_onboarding(_profile())

            # Runtime status is allowed to advance while the account is being
            # activated.  With no accepted account state, the exact newly
            # created registration can still be compensated safely.
            await store.set_bot_profile_status(
                created.profile_id,
                connection_state="ready",
                generation=1,
                last_ready_at="2026-09-07T00:00:00+00:00",
            )
            assert await store.rollback_bot_profile_registration(created)
            assert await store.get_bot_profile(created.profile_id) is None
            assert await store.get_bot_profile_status(created.profile_id) is None

            retried = await store.create_bot_profile_for_onboarding(_profile())
            assert retried.profile_id == created.profile_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_live_onboarding_profile_rollback_fails_closed_after_profile_change(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            created = await store.create_bot_profile_for_onboarding(_profile())
            await store.update_bot_profile(
                created.profile_id,
                mention_policy="allow_all",
            )
            assert not await store.rollback_bot_profile_registration(created)
            assert await store.get_bot_profile(created.profile_id) is not None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_live_onboarding_profile_rollback_fails_closed_after_account_state(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            created = await store.create_bot_profile_for_onboarding(_profile())
            await store.create_principal(principal_id="owner")
            await store.map_principal_account(
                principal_id="owner",
                channel=created.channel,
                bot_id=created.bot_id,
                external_user_id="ou_owner1234",
                identifier_kind="open_id",
                configured_by="test",
            )
            assert not await store.rollback_bot_profile_registration(created)
            assert await store.get_bot_profile(created.profile_id) == created
        finally:
            await store.close()

    asyncio.run(scenario())


async def _create_chat_onboarding_source(store: SQLiteStore):
    await store.create_principal(
        principal_id="owner",
        display_name="Local Owner",
    )
    return await store.map_principal_account(
        principal_id="owner",
        channel="lark",
        bot_id="cli_originbot1234",
        external_user_id="ou_originowner1234",
        identifier_kind="open_id",
        configured_by="test:bootstrap",
    )


async def _create_chat_onboarding_bundle(store: SQLiteStore, source):
    return await store.create_bot_profile_with_owner_for_onboarding(
        _chat_profile(),
        external_user_id="ou_newappowner1234",
        source_principal_account_id=source.principal_account_id,
        source_channel=source.channel,
        source_bot_id=source.bot_id,
        source_external_user_id=source.external_user_id,
        source_mapping_revision=source.mapping_revision,
        configured_by=f"lark-chat-onboarding:{source.principal_account_id}",
    )


def test_chat_onboarding_atomically_maps_verified_app_owner(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            profile, account = await _create_chat_onboarding_bundle(store, source)

            assert profile.profile_id == "team"
            assert account.principal_id == "owner"
            assert account.channel == "lark"
            assert account.bot_id == profile.bot_id
            assert account.external_user_id == "ou_newappowner1234"
            assert account.identifier_kind == "open_id"
            assert account.mapping_revision == 1
            assert account.active
            assert account.configured_by == (
                f"lark-chat-onboarding:{source.principal_account_id}"
            )
            assert (
                await store.resolve_principal_account(
                    channel="lark",
                    bot_id=profile.bot_id,
                    external_user_id=account.external_user_id,
                )
            ) == account
            assert (
                await store.resolve_principal_account(
                    channel=source.channel,
                    bot_id=source.bot_id,
                    external_user_id=source.external_user_id,
                )
            ) == source

            # Supervisor compatibility auto-mapping sees the atomic mapping
            # and cannot create a second revision from a later sender.
            assert await store.auto_map_owner_principal_accounts(
                accounts=[("lark", profile.bot_id)]
            ) == [
                {
                    "channel": "lark",
                    "bot_id": profile.bot_id,
                    "outcome": "already_mapped",
                    "sender_count": 0,
                    "external_user_id": "",
                    "identifier_kind": "",
                    "principal_account_id": account.principal_account_id,
                }
            ]
            assert len(await store.list_principal_accounts(active=None)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("authority_change", ("revoked", "disabled", "revision"))
def test_chat_onboarding_rechecks_exact_source_owner_inside_transaction(
    tmp_path,
    authority_change,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            revision = source.mapping_revision
            if authority_change == "revoked":
                assert await store.unmap_principal_account(
                    channel=source.channel,
                    bot_id=source.bot_id,
                    external_user_id=source.external_user_id,
                )
            elif authority_change == "disabled":
                await store.update_principal("owner", enabled=False)
            else:
                revision += 1

            with pytest.raises(StoreError, match="owner authority changed"):
                await store.create_bot_profile_with_owner_for_onboarding(
                    _chat_profile(),
                    external_user_id="ou_newappowner1234",
                    source_principal_account_id=source.principal_account_id,
                    source_channel=source.channel,
                    source_bot_id=source.bot_id,
                    source_external_user_id=source.external_user_id,
                    source_mapping_revision=revision,
                    configured_by="lark-chat-onboarding:test",
                )
            assert await store.get_bot_profile("team") is None
            target_rows = [
                row
                for row in await store.list_principal_accounts(active=None)
                if row.bot_id == "cli_botaccount1"
            ]
            assert target_rows == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_rejects_and_preserves_preexisting_target_history(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            preserved = await store.map_principal_account(
                principal_id="owner",
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_prioraccount1234",
                identifier_kind="open_id",
                configured_by="test:prior",
            )
            assert await store.unmap_principal_account(
                channel=preserved.channel,
                bot_id=preserved.bot_id,
                external_user_id=preserved.external_user_id,
            )
            preserved = (await store.list_principal_accounts(active=None))[-1]

            with pytest.raises(StoreError, match="principal-account history"):
                await _create_chat_onboarding_bundle(store, source)

            assert await store.get_bot_profile("team") is None
            assert (await store.list_principal_accounts(active=None))[-1] == preserved
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_rejects_historical_profile_for_qr_created_app(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            historical = await store.create_bot_profile(_profile("historical"))
            assert await store.remove_bot_profile(historical.profile_id)

            with pytest.raises(StoreError, match="profile history"):
                await store.create_bot_profile_with_owner_for_onboarding(
                    _chat_profile("replacement"),
                    external_user_id="ou_newappowner1234",
                    source_principal_account_id=source.principal_account_id,
                    source_channel=source.channel,
                    source_bot_id=source.bot_id,
                    source_external_user_id=source.external_user_id,
                    source_mapping_revision=source.mapping_revision,
                    configured_by="lark-chat-onboarding:test",
                )

            assert await store.get_bot_profile("replacement") is None
            assert (
                await store.get_bot_profile("historical", include_removed=True)
            ) is not None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_rejects_preexisting_target_ingress_atomically(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            await store.accept_inbound(
                InboundEnvelope(
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_unmappedactor1234",
                    external_message_id="om_preexisting_target_state",
                    text="preexisting",
                ),
                create_task=False,
            )

            with pytest.raises(StoreError, match="durable account state"):
                await _create_chat_onboarding_bundle(store, source)
            assert await store.get_bot_profile("team") is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_rejects_bot_identity_as_human_owner_before_write(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            with pytest.raises(ValueError, match="distinct verified human and bot"):
                await store.create_bot_profile_with_owner_for_onboarding(
                    _chat_profile(),
                    external_user_id="ou_newappbot1234",
                    source_principal_account_id=source.principal_account_id,
                    source_channel=source.channel,
                    source_bot_id=source.bot_id,
                    source_external_user_id=source.external_user_id,
                    source_mapping_revision=source.mapping_revision,
                    configured_by="lark-chat-onboarding:test",
                )
            assert await store.get_bot_profile("team") is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_late_mapping_collision_rolls_back_profile_bundle(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            await store.create_principal(principal_id="unrelated")
            preserved = await store.map_principal_account(
                principal_id="unrelated",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wechat-user",
                identifier_kind="from_user_id",
                configured_by="test",
                principal_account_id="forced-chat-account-collision",
            )

            with pytest.raises(StoreError, match="already in use"):
                await store.create_bot_profile_with_owner_for_onboarding(
                    _chat_profile(),
                    external_user_id="ou_newappowner1234",
                    source_principal_account_id=source.principal_account_id,
                    source_channel=source.channel,
                    source_bot_id=source.bot_id,
                    source_external_user_id=source.external_user_id,
                    source_mapping_revision=source.mapping_revision,
                    configured_by="lark-chat-onboarding:test",
                    principal_account_id="forced-chat-account-collision",
                )

            assert await store.get_bot_profile("team") is None
            assert await store.get_bot_profile_status("team") is None
            assert preserved in await store.list_principal_accounts(active=None)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_chat_onboarding_exact_bundle_rollback_preserves_source_owner(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            profile, account = await _create_chat_onboarding_bundle(store, source)
            await store.set_bot_profile_status(
                profile.profile_id,
                connection_state="ready",
                generation=1,
            )

            assert await store.rollback_bot_profile_with_owner_registration(
                profile,
                account,
            )
            assert await store.get_bot_profile(profile.profile_id) is None
            assert await store.get_bot_profile_status(profile.profile_id) is None
            assert await store.list_principal_accounts(active=None) == [source]
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("target_change", ("mapping-revoked", "ingress"))
def test_chat_onboarding_bundle_rollback_fails_closed_after_target_use(
    tmp_path,
    target_change,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            source = await _create_chat_onboarding_source(store)
            profile, account = await _create_chat_onboarding_bundle(store, source)
            if target_change == "mapping-revoked":
                assert await store.unmap_principal_account(
                    channel=account.channel,
                    bot_id=account.bot_id,
                    external_user_id=account.external_user_id,
                )
            else:
                await store.accept_inbound(
                    InboundEnvelope(
                        channel="lark",
                        bot_id=account.bot_id,
                        external_user_id=account.external_user_id,
                        external_message_id="om_new_bot_ingress",
                        text="hello",
                        principal_id="owner",
                        principal_account_id=account.principal_account_id,
                    ),
                    create_task=False,
                )

            assert not await store.rollback_bot_profile_with_owner_registration(
                profile,
                account,
            )
            assert await store.get_bot_profile(profile.profile_id) == profile
            assert any(
                row.principal_account_id == account.principal_account_id
                for row in await store.list_principal_accounts(active=None)
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_atomic_profile_owner_registration_reuses_principal_and_is_idempotent(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            existing_owner = await store.create_principal(
                principal_id="owner",
                display_name="Local Owner",
                metadata={"source": "existing"},
            )
            created, account = (
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="local-owner:test",
                )
            )

            assert created.profile_id == "team"
            assert account.principal_id == "owner"
            assert account.channel == "lark"
            assert account.bot_id == created.bot_id
            assert account.external_user_id == "ou_owner1234"
            assert account.identifier_kind == "open_id"
            assert account.mapping_revision == 1
            assert account.configured_by == "local-owner:test"
            assert await store.get_principal("owner") == existing_owner
            status = await store.get_bot_profile_status(created.profile_id)
            assert status is not None
            assert status.onboarding_state == "registered"
            assert status.connection_state == "not_started"

            replayed, replayed_account = (
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="a-different-replay-label",
                )
            )
            assert replayed == created
            assert replayed_account == account
            assert await store.list_principal_accounts(active=None) == [account]
            assert (
                await store.resolve_principal_account(
                    channel="lark",
                    bot_id=created.bot_id,
                    external_user_id="ou_owner1234",
                )
            ) == account
            assert (
                await store.resolve_principal_account(
                    channel="lark",
                    bot_id="cli_peer_bot",
                    external_user_id="ou_owner1234",
                )
                is None
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_atomic_profile_owner_registration_matches_revisioned_remap_semantics(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_principal(principal_id="prior-owner")
            prior = await store.map_principal_account(
                principal_id="prior-owner",
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_owner1234",
                identifier_kind="open_id",
                configured_by="old-local-owner",
            )

            created, current = (
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="new-local-owner",
                )
            )

            assert created.bot_id == "cli_botaccount1"
            assert current.principal_id == "owner"
            assert current.mapping_revision == 2
            history = await store.list_principal_accounts(active=None)
            assert [
                (row.principal_id, row.mapping_revision, row.active)
                for row in history
            ] == [
                (prior.principal_id, 1, False),
                ("owner", 2, True),
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_atomic_profile_owner_registration_rolls_back_every_row_on_late_failure(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_principal(principal_id="unrelated")
            preserved = await store.map_principal_account(
                principal_id="unrelated",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wechat-user",
                identifier_kind="from_user_id",
                configured_by="test",
                principal_account_id="forced-account-id-collision",
            )

            with pytest.raises(StoreError, match="already in use"):
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="local-owner:test",
                    principal_account_id="forced-account-id-collision",
                )

            assert await store.get_bot_profile("team") is None
            assert await store.get_bot_profile_status("team") is None
            assert await store.get_principal("owner") is None
            assert await store.list_principal_accounts(active=None) == [preserved]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_atomic_profile_owner_registration_rejects_disabled_owner_atomically(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            disabled_owner = await store.create_principal(
                principal_id="owner",
                enabled=False,
                metadata={"disabled-deliberately": True},
            )

            with pytest.raises(StoreError, match="owner principal is disabled"):
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="local-owner:test",
                )

            assert await store.get_bot_profile("team") is None
            assert await store.get_bot_profile_status("team") is None
            assert await store.get_principal("owner") == disabled_owner
            assert await store.list_principal_accounts(active=None) == []
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "options, message",
    [
        ({"principal_id": "person:owner"}, "principal owner"),
        ({"external_user_id": "union_owner1234"}, "stable ou_ open_id"),
        ({"identifier_kind": "union_id"}, "stable open_id"),
    ],
)
def test_atomic_profile_owner_registration_rejects_non_owner_identity_before_write(
    tmp_path,
    options,
    message,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            arguments = {
                "external_user_id": "ou_owner1234",
                "configured_by": "local-owner:test",
                **options,
            }
            with pytest.raises(ValueError, match=message):
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    **arguments,
                )
            assert await store.get_bot_profile("team") is None
            assert await store.get_principal("owner") is None
            assert await store.list_principal_accounts(active=None) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_transaction_rolls_back_a_commit_time_failure() -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        connection.execute(
            "CREATE TABLE child(id INTEGER PRIMARY KEY, "
            "parent_id INTEGER REFERENCES parent(id))"
        )
        connection.execute("PRAGMA defer_foreign_keys=ON")

        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            with _transaction(connection):
                connection.execute("INSERT INTO child VALUES(1, 999)")

        assert not connection.in_transaction
        assert connection.execute("SELECT COUNT(*) FROM child").fetchone() == (0,)
        with _transaction(connection):
            connection.execute("INSERT INTO parent VALUES(999)")
            connection.execute("INSERT INTO child VALUES(1, 999)")
        assert connection.execute("SELECT COUNT(*) FROM child").fetchone() == (1,)
    finally:
        connection.close()


def test_atomic_profile_owner_replay_rejects_a_missing_status_row(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            created = await store.create_bot_profile(_profile())
            assert await store._call(
                lambda conn: conn.execute(
                    "DELETE FROM bot_profile_status WHERE profile_id='team'"
                ).rowcount
            ) == 1

            with pytest.raises(StoreError, match="missing its status row"):
                await store.create_bot_profile_with_principal_account(
                    _profile(),
                    external_user_id="ou_owner1234",
                    configured_by="local-owner:test",
                )

            assert await store.get_bot_profile("team") == created
            assert await store.get_bot_profile_status("team") is None
            assert await store.get_principal("owner") is None
            assert await store.list_principal_accounts(active=None) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_schema_v36_reopen_rejects_a_missing_profile_status_row(tmp_path) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            await store.create_bot_profile(_profile())
        finally:
            await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "DELETE FROM bot_profile_status WHERE profile_id='team'"
        )
        connection.commit()
    finally:
        connection.close()

    async def reopen() -> None:
        store = SQLiteStore(database)
        try:
            with pytest.raises(StoreError, match="missing its status row"):
                await store.initialize()
        finally:
            await store.close()

    asyncio.run(reopen())


@pytest.mark.parametrize(
    "index_name, table_name, columns, predicate",
    _V36_CRITICAL_UNIQUE_INDEXES,
    ids=("active-principal", "live-account", "live-config", "live-config-identity"),
)
def test_schema_v36_rebuilds_a_poisoned_same_name_unique_index(
    tmp_path,
    index_name,
    table_name,
    columns,
    predicate,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(database)
    try:
        connection.execute(f"DROP INDEX {index_name}")
        connection.execute(
            f"CREATE INDEX {index_name} ON {table_name}({columns[0]})"
        )
        connection.commit()
    finally:
        connection.close()

    async def reopen() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        await store.close()

    asyncio.run(reopen())
    connection = sqlite3.connect(database)
    try:
        index_row = next(
            row
            for row in connection.execute(f"PRAGMA index_list({table_name})")
            if row[1] == index_name
        )
        assert index_row[2] == 1
        assert index_row[4] == 1
        assert tuple(
            row[2] for row in connection.execute(f"PRAGMA index_info({index_name})")
        ) == columns
        definition = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (index_name,),
        ).fetchone()[0]
        assert " ".join(definition.split()).casefold() == " ".join(
            (
                f"CREATE UNIQUE INDEX {index_name} ON {table_name}"
                f"({', '.join(columns)}) WHERE {predicate}"
            ).split()
        ).casefold()
    finally:
        connection.close()


@pytest.mark.parametrize(
    "index_name, table_name, columns, predicate",
    _V36_CRITICAL_UNIQUE_INDEXES,
    ids=("active-principal", "live-account", "live-config", "live-config-identity"),
)
def test_schema_v36_poisoned_index_with_duplicates_fails_closed_on_reopen(
    tmp_path,
    index_name,
    table_name,
    columns,
    predicate,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def seed() -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            if table_name == "principal_accounts":
                await store.create_principal(principal_id="person:first")
                await store.create_principal(principal_id="person:second")
                await store.map_principal_account(
                    principal_id="person:first",
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_first1234",
                    identifier_kind="open_id",
                    configured_by="local-owner:test",
                )
                await store.map_principal_account(
                    principal_id="person:second",
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_second1234",
                    identifier_kind="open_id",
                    configured_by="local-owner:test",
                )
            else:
                await store.create_bot_profile(_profile("first"))
                second = _profile("second")
                second["bot_id"] = "cli_botaccount2"
                await store.create_bot_profile(second)
        finally:
            await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(database)
    try:
        connection.execute(f"DROP INDEX {index_name}")
        connection.execute(
            f"CREATE INDEX {index_name} ON {table_name}"
            f"({', '.join(columns)}) WHERE {predicate}"
        )
        if index_name == "idx_principal_accounts_one_active":
            connection.execute(
                "UPDATE principal_accounts "
                "SET external_user_id='ou_first1234', mapping_revision=2 "
                "WHERE principal_id='person:second'"
            )
        elif index_name == "idx_bot_profiles_live_account":
            connection.execute(
                "UPDATE bot_profiles SET bot_id='cli_botaccount1' "
                "WHERE profile_id='second'"
            )
        elif index_name == "idx_bot_profiles_live_config_dir":
            connection.execute(
                "UPDATE bot_profiles SET config_dir='/private/lark/first' "
                "WHERE profile_id='second'"
            )
        else:
            connection.execute(
                "UPDATE bot_profiles "
                "SET config_dir_identity='device:private-lark-first' "
                "WHERE profile_id='second'"
            )
        connection.commit()
    finally:
        connection.close()

    async def reopen() -> None:
        store = SQLiteStore(database)
        try:
            with pytest.raises(
                StoreError,
                match=f"live identity uniqueness is violated: {index_name}",
            ):
                await store.initialize()
        finally:
            await store.close()

    asyncio.run(reopen())
