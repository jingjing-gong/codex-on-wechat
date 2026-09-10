from __future__ import annotations

import asyncio

from src.runtime.media import AttachmentStore
from src.runtime.models import MediaDeliveryState, OutboxState, ReplyTarget
from src.runtime.sqlite_store import SQLiteStore


async def _lark_bundle(store: SQLiteStore, root, *, outbox_id: str = "lark-bundle"):
    files = AttachmentStore(root)
    first = files.put_bytes(b"first", attachment_id=f"{outbox_id}-first")
    second = files.put_bytes(b"second", attachment_id=f"{outbox_id}-second")
    await store.register_attachment(first, kind="image")
    await store.register_attachment(second, kind="file")
    parent = await store.create_account_outbox(
        channel="lark",
        bot_id="cli_botaccount1",
        target=ReplyTarget(
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id="ou_actor1234",
            source_message_id="om_source1234",
            conversation_subject_id="thread-subject",
            conversation_subject_scope="thread-scope",
            destination_kind="thread",
            destination_id="oc_chat1234",
            thread_id="omt_root1234",
            root_message_id="omt_root1234",
        ),
        content="",
        attachments=(first.attachment_id, second.attachment_id),
        agent_id="codex",
        outbox_id=outbox_id,
        client_id=f"{outbox_id}-client",
    )
    return parent, first, second


async def _send_child(
    store: SQLiteStore,
    media_id: str,
    *,
    outbox_id: str,
    token: str,
) -> None:
    assert await store.transition_outgoing_media(
        media_id,
        MediaDeliveryState.UPLOADED,
        from_states=(MediaDeliveryState.UPLOADING,),
        outbox_id=outbox_id,
        outbox_claim_token=token,
        remote_id=f"remote-{media_id}",
    )
    assert await store.transition_outgoing_media(
        media_id,
        MediaDeliveryState.SEND_PENDING,
        from_states=(MediaDeliveryState.UPLOADED,),
        outbox_id=outbox_id,
        outbox_claim_token=token,
    )
    assert await store.transition_outgoing_media(
        media_id,
        MediaDeliveryState.SENT,
        from_states=(MediaDeliveryState.SEND_PENDING,),
        outbox_id=outbox_id,
        outbox_claim_token=token,
    )


def test_exact_account_media_claim_fences_parent_and_every_child(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _lark_bundle(store, root)
            assert parent.reply_slot_id is None
            assert await store.claim_account_outgoing_media(
                "foreign-worker",
                channel="lark",
                bot_id="cli_otheraccount",
                limit=10,
            ) == []

            claimed = await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            )
            assert len(claimed) == 2
            assert [item.metadata["bundle_ordinal"] for item in claimed] == [0, 1]
            token = claimed[0].outbox_claim_token
            assert token
            assert {item.outbox_id for item in claimed} == {parent.outbox_id}
            assert {item.claim_token for item in claimed} == {token}
            assert {item.outbox_claim_token for item in claimed} == {token}
            assert {item.state for item in claimed} == {
                MediaDeliveryState.UPLOADING
            }
            assert all(item.delivery_address is not None for item in claimed)
            assert all(
                item.delivery_address.destination_id == "oc_chat1234"
                for item in claimed
            )

            durable_parent = await store.get_outbox_item(parent.outbox_id)
            assert durable_parent is not None
            assert durable_parent.state is OutboxState.CLAIMED
            assert durable_parent.claim_token == token
            assert await store.mark_outbox_sending(
                parent.outbox_id, claim_token=token
            )

            await _send_child(
                store,
                claimed[0].media_id,
                outbox_id=parent.outbox_id,
                token=token,
            )
            siblings = await store.list_outgoing_media_for_outbox(
                outbox_id=parent.outbox_id,
                outbox_claim_token=token,
            )
            assert [item.state for item in siblings] == [
                MediaDeliveryState.SENT,
                MediaDeliveryState.UPLOADING,
            ]
            assert siblings[0].claim_token is None
            assert siblings[0].outbox_claim_token == token
            assert siblings[1].claim_token == token

            # Terminal siblings are not part of the renewal quorum.  Omitting
            # a currently unfinished sibling still fails closed.
            assert not await store.renew_canonical_media_lease(
                parent.outbox_id,
                token,
                media_ids=(siblings[0].media_id, siblings[1].media_id),
            )
            assert await store.renew_canonical_media_lease(
                parent.outbox_id,
                token,
                media_ids=(siblings[1].media_id,),
            )
            assert not await store.finish_outbox_attempt(
                parent.outbox_id,
                token,
                outcome="sent",
                client_id=parent.client_id,
            )

            await _send_child(
                store,
                siblings[1].media_id,
                outbox_id=parent.outbox_id,
                token=token,
            )
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                token,
                outcome="sent",
                client_id=parent.client_id,
                remote_delivery_id="om_bundle_sent",
            )
            terminal = await store.get_outbox_item(parent.outbox_id)
            assert terminal is not None
            assert terminal.state is OutboxState.SENT
        finally:
            await store.close()

    asyncio.run(scenario())


def test_exact_account_media_retry_keeps_sent_and_upload_checkpoints(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _lark_bundle(
                store, root, outbox_id="lark-retry-bundle"
            )
            initial = await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            )
            token = str(initial[0].outbox_claim_token)
            assert await store.mark_outbox_sending(parent.outbox_id, token)
            await _send_child(
                store,
                initial[0].media_id,
                outbox_id=parent.outbox_id,
                token=token,
            )
            # Persist the second upload but leave its external send pending.
            assert await store.transition_outgoing_media(
                initial[1].media_id,
                MediaDeliveryState.UPLOADED,
                from_states=(MediaDeliveryState.UPLOADING,),
                outbox_id=parent.outbox_id,
                outbox_claim_token=token,
                remote_id="file_uploaded_once",
            )
            assert await store.transition_outgoing_media(
                initial[1].media_id,
                MediaDeliveryState.SEND_PENDING,
                from_states=(MediaDeliveryState.UPLOADED,),
                outbox_id=parent.outbox_id,
                outbox_claim_token=token,
            )
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                token,
                outcome="retryable_failure",
                error="rate limited",
                retry_after=0,
            )
            retained = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert [item.state for item in retained] == [
                MediaDeliveryState.SENT,
                MediaDeliveryState.SEND_PENDING,
            ]
            assert all(item.claim_token is None for item in retained)

            retried = await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
                states=(
                    MediaDeliveryState.UPLOADING,
                    MediaDeliveryState.UPLOADED,
                    MediaDeliveryState.SEND_PENDING,
                ),
            )
            assert [item.state for item in retried] == [
                MediaDeliveryState.SENT,
                MediaDeliveryState.SEND_PENDING,
            ]
            retry_token = retried[0].outbox_claim_token
            assert retry_token and retry_token != token
            assert retried[0].claim_token is None
            assert retried[0].attempts == 1
            assert retried[1].claim_token == retry_token
            assert retried[1].attempts == 2
            assert retried[1].remote_id == "file_uploaded_once"

            assert await store.mark_outbox_sending(
                parent.outbox_id, retry_token
            )
            assert await store.transition_outgoing_media(
                retried[1].media_id,
                MediaDeliveryState.SENT,
                from_states=(MediaDeliveryState.SEND_PENDING,),
                outbox_id=parent.outbox_id,
                outbox_claim_token=retry_token,
            )
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                retry_token,
                outcome="sent",
                client_id=parent.client_id,
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_exact_account_all_sent_bundle_is_recoverable_after_unknown_parent(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _lark_bundle(
                store, root, outbox_id="lark-unknown-bundle"
            )
            claimed = await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            )
            token = str(claimed[0].outbox_claim_token)
            assert await store.mark_outbox_sending(parent.outbox_id, token)
            for child in claimed:
                await _send_child(
                    store,
                    child.media_id,
                    outbox_id=parent.outbox_id,
                    token=token,
                )

            # An unknown parent outcome is deliberately terminal until an
            # operator explicitly retries; automatic replay could duplicate
            # an externally visible send whose acknowledgement was lost.
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                token,
                outcome="unknown",
                error="parent acknowledgement lost",
            )
            unknown = await store.get_outbox_item(parent.outbox_id)
            assert unknown is not None
            assert unknown.state is OutboxState.DELIVERY_UNKNOWN
            assert await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            ) == []

            assert await store.retry_outbox(parent.outbox_id)
            recovered = await store.claim_account_outgoing_media(
                "lark-media-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            )
            assert len(recovered) == 2
            assert all(
                item.state is MediaDeliveryState.SENT for item in recovered
            )
            assert all(item.claim_token is None for item in recovered)
            recovery_token = recovered[0].outbox_claim_token
            assert recovery_token and recovery_token != token
            assert {
                item.outbox_claim_token for item in recovered
            } == {recovery_token}
            assert await store.mark_outbox_sending(
                parent.outbox_id, recovery_token
            )
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                recovery_token,
                outcome="sent",
                client_id=parent.client_id,
            )
        finally:
            await store.close()

    asyncio.run(scenario())
