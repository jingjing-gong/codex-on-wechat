"""Channel commands use the store-authenticated principal mapping revision."""

from __future__ import annotations

import asyncio

from src.channels.lark import LarkBotProfile, LarkGateway
from src.channels.wechat import WeChatGateway
from src.runtime.identity import PrincipalResolver
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


class _CaptureRouter:
    def __init__(self) -> None:
        self.envelope = None

    async def handle_command(self, _command, envelope, **_kwargs):
        self.envelope = envelope
        return "captured"


def _wechat_cron_message(*, message_id: int = 1) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="wx-owner",
        to_user_id="wechat-bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token="context",
        item_list=[
            MessageItem(
                type=ITEM_TYPE_TEXT,
                text_item=TextItem(text="/cron list"),
            )
        ],
    )


def test_wechat_command_restores_durable_principal_mapping_revision(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "wechat.sqlite3")
        await store.initialize()
        try:
            await store.create_principal(principal_id="owner")
            account = await store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                identifier_kind="from_user_id",
                configured_by="test",
            )
            router = _CaptureRouter()
            gateway = WeChatGateway(
                store,
                bot_id="wechat-bot",
                command_router=router,
            )
            await gateway.accept(_wechat_cron_message())

            assert router.envelope is not None
            assert router.envelope.principal_id == "owner"
            assert (
                router.envelope.principal_account_id
                == account.principal_account_id
            )
            assert (
                router.envelope.principal_mapping_revision
                == account.mapping_revision
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_wechat_command_marks_durably_unmapped_identity(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "wechat-unmapped.sqlite3")
        await store.initialize()
        try:
            router = _CaptureRouter()
            gateway = WeChatGateway(
                store,
                bot_id="wechat-bot",
                command_router=router,
            )
            await gateway.accept(_wechat_cron_message(message_id=2))

            assert router.envelope is not None
            assert router.envelope.principal_id == ""
            assert router.envelope.principal_account_id == ""
            assert router.envelope.principal_mapping_revision == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_lark_command_uses_resolver_revision_and_ignores_raw_identity(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "lark.sqlite3")
        await store.initialize()
        config_dir = tmp_path / "lark-config"
        config_dir.mkdir(mode=0o700)
        try:
            await store.create_principal(principal_id="owner")
            account = await store.map_principal_account(
                principal_id="owner",
                channel="lark",
                bot_id="cli_revisionbot1",
                external_user_id="ou_owner1",
                identifier_kind="open_id",
                configured_by="test",
            )
            router = _CaptureRouter()
            gateway = LarkGateway(
                store,
                LarkBotProfile(
                    profile_id="revision-bot",
                    app_id="cli_revisionbot1",
                    bot_open_id="ou_revisionbot1",
                    config_dir=config_dir,
                ),
                principal_resolver=PrincipalResolver(store),
                command_router=router,
            )
            await gateway.accept_event(
                {
                    "type": "im.message.receive_v1",
                    "event_id": "evt-revision-1",
                    "message_id": "om_revision1",
                    "chat_id": "oc_revision1",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "sender_id": "ou_owner1",
                    "content": "/cron list",
                    # Event metadata is untrusted and must not become command
                    # authorization, even when it resembles internal fields.
                    "principal_id": "forged",
                    "principal_account_id": "forged-account",
                    "mapping_revision": 999,
                }
            )

            assert router.envelope is not None
            assert router.envelope.principal_id == "owner"
            assert (
                router.envelope.principal_account_id
                == account.principal_account_id
            )
            assert (
                router.envelope.principal_mapping_revision
                == account.mapping_revision
            )
        finally:
            await store.close()

    asyncio.run(scenario())
