from __future__ import annotations

import asyncio

from src.channels.models import InboundEnvelope
from src.runtime.identity import (
    group_conversation_subject,
    thread_conversation_subject,
)
from src.runtime.sqlite_store import SQLiteStore


def test_lark_ingress_never_materializes_wechat_reply_quota(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            subject = group_conversation_subject(
                "lark", "cli_botaccount1", "oc_chat1234"
            )
            accepted = await store.accept_inbound(
                InboundEnvelope(
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_actor1234",
                    external_message_id="om_lark1234",
                    text="hello",
                    conversation_subject_id=subject.conversation_subject_id,
                    conversation_subject_scope=subject.scope_key,
                    conversation_subject_kind="group",
                    destination_kind="group",
                    destination_id="oc_chat1234",
                    transport_metadata={
                        "chat_id": "oc_chat1234",
                        "chat_type": "group",
                    },
                ),
                create_task=False,
            )
            assert await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            ) is None

            wechat = await store.accept_inbound(
                InboundEnvelope(
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="wx-user",
                    external_message_id="wx-message",
                    text="hello",
                ),
                create_task=False,
            )
            assert await store.get_reply_scope_for_inbound(
                wechat.inbound.message_id
            ) is not None
            counts = await store._call(
                lambda conn: tuple(
                    conn.execute(
                        "SELECT channel,COUNT(*) FROM reply_scopes "
                        "GROUP BY channel ORDER BY channel"
                    ).fetchone()
                )
            )
            assert counts == ("wechat", 1)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_principal_remap_does_not_rewrite_group_task_identity_history(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_principal(principal_id="person:old")
            await store.create_principal(principal_id="person:new")
            old_mapping = await store.map_principal_account(
                principal_id="person:old",
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                identifier_kind="open_id",
                configured_by="local-owner",
            )
            subject = group_conversation_subject(
                "lark", "cli_botaccount1", "oc_chat1234"
            )

            def envelope(message_id: str, principal_id: str, account_id: str):
                return InboundEnvelope(
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_actor1234",
                    external_message_id=message_id,
                    text="work",
                    principal_id=principal_id,
                    principal_account_id=account_id,
                    conversation_subject_id=subject.conversation_subject_id,
                    conversation_subject_scope=subject.scope_key,
                    conversation_subject_kind="group",
                    destination_kind="group",
                    destination_id="oc_chat1234",
                    transport_metadata={"chat_id": "oc_chat1234"},
                )

            first = await store.accept_inbound(
                envelope(
                    "om_before_remap",
                    "person:old",
                    old_mapping.principal_account_id,
                )
            )
            new_mapping = await store.map_principal_account(
                principal_id="person:new",
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                identifier_kind="open_id",
                configured_by="local-owner",
            )
            second = await store.accept_inbound(
                envelope(
                    "om_after_remap",
                    "person:new",
                    new_mapping.principal_account_id,
                )
            )
            assert first.task is not None and second.task is not None
            retained = await store.get_task(first.task.task_id)
            assert retained is not None
            assert retained.principal_id == "person:old"
            assert retained.principal_account_id == (
                old_mapping.principal_account_id
            )
            assert retained.identity_snapshot["principal"][
                "mapping_revision"
            ] == 1
            assert second.task.principal_id == "person:new"
            assert second.task.identity_snapshot["principal"][
                "mapping_revision"
            ] == 2

            # Both messages share bot-local group continuity, while immutable
            # replies retain the authenticated actor and exact chat.
            assert retained.conversation_id == second.task.conversation_id
            assert retained.conversation_subject_id == (
                subject.conversation_subject_id
            )
            assert retained.actor_external_user_id == "ou_actor1234"
            assert retained.reply_target.external_user_id == "ou_actor1234"
            assert retained.reply_target.destination_id == "oc_chat1234"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_lark_thread_links_to_the_canonical_group_subject(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            group = group_conversation_subject(
                "lark", "cli_botaccount1", "oc_chat1234"
            )
            thread = thread_conversation_subject(
                "lark", "cli_botaccount1", "oc_chat1234", "omt_root1234"
            )
            await store.accept_inbound(
                InboundEnvelope(
                    channel="lark",
                    bot_id="cli_botaccount1",
                    external_user_id="ou_actor1234",
                    external_message_id="om_thread1234",
                    text="hello thread",
                    conversation_subject_id=thread.conversation_subject_id,
                    conversation_subject_scope=thread.scope_key,
                    conversation_subject_kind="thread",
                    destination_kind="thread",
                    destination_id="oc_chat1234",
                    thread_id="omt_root1234",
                    root_message_id="omt_root1234",
                    transport_metadata={
                        "chat_id": "oc_chat1234",
                        "thread_id": "omt_root1234",
                        "root_id": "omt_root1234",
                    },
                ),
                create_task=False,
            )

            records = await store._call(
                lambda conn: {
                    str(row["conversation_subject_id"]): dict(row)
                    for row in conn.execute(
                        "SELECT * FROM conversation_subjects "
                        "WHERE channel='lark' AND bot_id='cli_botaccount1'"
                    ).fetchall()
                }
            )
            assert set(records) == {
                group.conversation_subject_id,
                thread.conversation_subject_id,
            }
            assert records[group.conversation_subject_id]["scope_key"] == (
                group.scope_key
            )
            assert records[thread.conversation_subject_id]["parent_subject_id"] == (
                group.conversation_subject_id
            )
        finally:
            await store.close()

    asyncio.run(scenario())
