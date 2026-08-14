"""Attachment ownership checks at task and transcription boundaries."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.media import AttachmentStore, sniff_mime
from src.runtime.models import InboundMessage
from src.runtime.sqlite_store import SQLiteStore, StoreError
from src.channels.wechat import WeChatGateway
from wechat_ilink.types import (
    ITEM_TYPE_IMAGE,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    ImageItem,
    MediaInfo,
    MessageItem,
    WeixinMessage,
)


def _task(
    *, agent_id: str, attachment_id: str | None = None
) -> dict[str, object]:
    return {
        "agent_id": agent_id,
        "conversation_id": f"wechat:bot:user:default:{agent_id}",
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": (
            {"attachment_id": attachment_id} if attachment_id is not None else {}
        ),
    }


def test_declared_native_mime_cannot_override_unknown_bytes():
    assert sniff_mime(b"not an image", declared="image/png") == (
        "application/octet-stream"
    )
    assert sniff_mime(b"%PDF-1.7\n", declared="image/png") == "application/pdf"


def test_image_filename_cannot_promote_unknown_bytes_to_native_image():
    assert sniff_mime(b"not an image", filename="photo.png") == (
        "application/octet-stream"
    )


def test_webp_signature_is_recognized_as_image():
    assert sniff_mime(b"RIFF\x00\x00\x00\x00WEBP", filename="photo.bin") == (
        "image/webp"
    )


def test_attachment_cleanup_reservation_fences_new_references(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        stored = files.put_bytes(b"cleanup-race", attachment_id="cleanup-race")
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            await store.register_attachment(stored, kind="file")
            owner = await store.create_task(_task(agent_id="codex"))
            assert await store.claim_attachment_cleanup(stored.attachment_id)
            with pytest.raises(StoreError, match="attachment is unavailable"):
                await store.add_attachment_ref(
                    "task", owner.task_id, stored.attachment_id
                )
            assert await store.finish_attachment_cleanup(
                stored.attachment_id, deleted=False
            )
            assert await store.add_attachment_ref(
                "task", owner.task_id, stored.attachment_id
            )

            # Once the durable ref is released, AttachmentStore uses the
            # SQLite reservation path and leaves the row blocked after unlink.
            assert await store.remove_attachment_ref(
                "task", owner.task_id, stored.attachment_id, agent_id="codex"
            )
            managed = AttachmentStore(
                root, reference_checker=store.attachment_referenced
            )
            assert await managed.acleanup(attachment_id=stored.attachment_id) == [
                stored.attachment_id
            ]
            assert not stored.local_path.exists()
            assert await store.get_attachment_state(stored.attachment_id) == (
                "blocked_media"
            )

            orphan = files.put_bytes(
                b"published-before-sqlite", attachment_id="cleanup-orphan"
            )
            assert await store.get_attachment(orphan.attachment_id) is None
            assert await managed.acleanup(attachment_id=orphan.attachment_id) == [
                orphan.attachment_id
            ]
            assert not orphan.local_path.exists()

            blocked = files.put_bytes(
                b"blocked-orphan", attachment_id="blocked-orphan"
            )
            await store.register_attachment(
                blocked, kind="file", state="blocked_media"
            )
            assert await managed.acleanup(attachment_id=blocked.attachment_id) == [
                blocked.attachment_id
            ]
            assert not blocked.local_path.exists()
        finally:
            await store.close()

    asyncio.run(scenario())


async def _create_planner_owned_attachment(store: SQLiteStore) -> str:
    attachment_id = "planner-owned-media"
    await store.register_attachment(
        {"attachment_id": attachment_id, "mime_type": "audio/mpeg"},
        kind="audio",
    )
    await store.create_task(
        _task(agent_id="planner", attachment_id=attachment_id)
    )
    return attachment_id


def test_add_attachment_ref_cannot_grant_foreign_task_access(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)
            codex_task = await store.create_task(_task(agent_id="codex"))
            refs_before = await store.list_attachment_refs(attachment_id)

            assert not await store.can_access_attachment(
                attachment_id,
                agent_id="codex",
                task_id=codex_task.task_id,
                channel=codex_task.reply_target.channel,
                bot_id=codex_task.reply_target.bot_id,
                external_user_id=codex_task.reply_target.external_user_id,
                session_id=codex_task.reply_target.session_id,
            )
            with pytest.raises(StoreError, match="attachment access denied"):
                await store.add_attachment_ref(
                    "task", codex_task.task_id, attachment_id
                )

            assert await store.list_attachment_refs(attachment_id) == refs_before
            assert not await store.can_access_attachment(
                attachment_id,
                agent_id="codex",
                task_id=codex_task.task_id,
                channel=codex_task.reply_target.channel,
                bot_id=codex_task.reply_target.bot_id,
                external_user_id=codex_task.reply_target.external_user_id,
                session_id=codex_task.reply_target.session_id,
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_remove_attachment_ref_requires_owner_agent(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)
            planner_task = (await store.list_tasks())[0]
            with pytest.raises(StoreError, match="attachment access denied"):
                await store.remove_attachment_ref(
                    "task",
                    planner_task.task_id,
                    attachment_id,
                    role="input",
                    agent_id="codex",
                )
            assert await store.list_attachment_refs(attachment_id)
            assert await store.remove_attachment_ref(
                "task",
                planner_task.task_id,
                attachment_id,
                role="input",
                agent_id="planner",
            )
            assert await store.list_attachment_refs(attachment_id) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_accept_inbound_cannot_self_grant_another_agents_attachment(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="malicious-inbound",
                        text="open the other Agent's file",
                    ),
                    task={
                        "agent_id": "codex",
                        "inputs": {"attachment_id": attachment_id},
                    },
                    cursor="7",
                )

            assert (
                await store.get_inbound("wechat", "bot", "malicious-inbound")
                is None
            )
            assert await store.get_cursor(channel="wechat", bot_id="bot") == ""
            tasks = await store.list_tasks()
            assert [task.agent_id for task in tasks] == ["planner"]
            refs = await store.list_attachment_refs(attachment_id)
            assert [(ref["owner_kind"], ref["owner_id"]) for ref in refs] == [
                ("task", tasks[0].task_id)
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_inbound_task_attachment_metadata_cannot_spoof_managed_path(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        claimed = files.put_bytes(b"claimed", attachment_id="claimed")
        substituted = files.put_bytes(b"substituted", attachment_id="substituted")
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            await store.register_attachment(claimed, kind="file")
            await store.register_attachment(substituted, kind="file")

            with pytest.raises(
                StoreError, match=r"attachment metadata conflicts \(path\)"
            ):
                await store.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="spoofed-path",
                        text="read this file",
                    ),
                    task={
                        "agent_id": "codex",
                        "inputs": {
                            "media": [
                                {
                                    "attachment_id": claimed.attachment_id,
                                    "path": substituted.path,
                                    "checksum": substituted.checksum,
                                    "size": substituted.size,
                                    "mime_type": substituted.mime_type,
                                    "kind": "file",
                                }
                            ]
                        },
                    },
                )

            assert await store.get_inbound("wechat", "bot", "spoofed-path") is None
            assert await store.list_tasks() == []
            assert await store.list_attachment_refs(claimed.attachment_id) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_transcription_candidate_rejects_foreign_attachment(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.create_transcription_candidate(
                    confirmation_id="foreign-candidate",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                    agent_id="codex",
                    attachment_id=attachment_id,
                    candidate_text="stolen transcript",
                )

            assert (
                await store.get_transcription_candidate("foreign-candidate")
                is None
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_confirmation_revalidates_legacy_candidate_attachment_owner(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)
            await store.create_transcription_candidate(
                confirmation_id="legacy-candidate",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                candidate_text="legacy transcript",
            )

            # Simulate a candidate written before attachment ownership was
            # enforced at creation time.
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE transcription_candidates SET attachment_id=? "
                    "WHERE confirmation_id=?",
                    (attachment_id, "legacy-candidate"),
                )
            )

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.confirm_transcription_task("legacy-candidate")

            candidate = await store.get_transcription_candidate(
                "legacy-candidate"
            )
            assert candidate is not None
            assert candidate["status"] == "pending"
            assert candidate["consumed_by_task_id"] is None
            assert (
                await store.get_task_by_dedupe("confirmation:legacy-candidate")
                is None
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_owner_agent_metadata_without_complete_route_cannot_grant_inbound_access(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "agent-only", "mime_type": "text/plain"},
                metadata={"owner_agent_id": "codex"},
            )
            with pytest.raises(StoreError, match="attachment access denied"):
                await store.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="agent-only-message",
                        text="should not adopt",
                    ),
                    task={
                        "agent_id": "codex",
                        "inputs": {"attachment_id": "agent-only"},
                    },
                )
            assert await store.list_tasks() == []
            assert await store.list_attachment_refs("agent-only") == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_store_inbound_with_unverifiable_agent_metadata_fails_closed(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "unverifiable-agent", "mime_type": "text/plain"},
                metadata={
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                    "source_message_id": "stored-message",
                    "owner_agent_id": "planner",
                },
            )
            stored = await store.store_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="stored-message",
                    payload={"media": [{"attachment_id": "unverifiable-agent"}]},
                )
            )
            assert stored.status.value == "stored"
            assert await store.list_attachment_refs("unverifiable-agent") == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_exact_agent_and_source_metadata_allows_atomic_inbound_promotion(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "exact-owner", "mime_type": "image/png"},
                kind="image",
                metadata={
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                    "source_message_id": "exact-owner-message",
                    "owner_agent_id": "codex",
                },
            )
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="exact-owner-message",
                    payload={"media": [{"attachment_id": "exact-owner"}]},
                ),
                task={
                    "agent_id": "codex",
                    "inputs": {"attachment_id": "exact-owner"},
                },
            )
            assert accepted.task is not None
            assert accepted.task.agent_id == "codex"
            refs = await store.list_attachment_refs("exact-owner")
            assert {ref["owner_kind"] for ref in refs} == {
                "inbound_message",
                "task",
            }

            replay = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="exact-owner-message",
                    payload={"media": [{"attachment_id": "exact-owner"}]},
                ),
                task={
                    # Simulate a front-Agent switch before channel redelivery.
                    "agent_id": "planner",
                    "inputs": {"attachment_id": "exact-owner"},
                },
            )
            assert replay.duplicate
            assert replay.task is not None
            assert replay.task.task_id == accepted.task.task_id
            assert replay.task.agent_id == "codex"
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("metadata_override", "message_id"),
    (
        ({"external_user_id": "other-user"}, "scoped-source"),
        ({"source_message_id": "other-message"}, "scoped-source"),
    ),
)
def test_inbound_promotion_route_or_source_mismatch_rolls_back(
    tmp_path, metadata_override, message_id
):
    async def scenario() -> None:
        metadata = {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
            "source_message_id": message_id,
        }
        metadata.update(metadata_override)
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "mismatched-owner", "mime_type": "image/png"},
                kind="image",
                metadata=metadata,
            )
            with pytest.raises(StoreError, match="attachment access denied"):
                await store.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id=message_id,
                        payload={
                            "media": [{"attachment_id": "mismatched-owner"}]
                        },
                    ),
                    task={
                        "agent_id": "codex",
                        "inputs": {"attachment_id": "mismatched-owner"},
                    },
                    cursor="9",
                )
            assert await store.get_inbound("wechat", "bot", message_id) is None
            assert await store.get_cursor(channel="wechat", bot_id="bot") == ""
            assert await store.list_attachment_refs("mismatched-owner") == []
            assert await store.list_tasks() == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_wechat_promoter_accepts_and_replays_a_managed_image(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", attachment_root=root
        )
        await store.initialize()
        downloads = 0

        def downloader(*_args, **_kwargs):
            nonlocal downloads
            downloads += 1
            return b"promoted-image"

        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=files,
            media_downloader=downloader,
        )
        message = WeixinMessage(
            message_id=321,
            from_user_id="user",
            to_user_id="bot",
            message_type=MESSAGE_TYPE_USER,
            message_state=MESSAGE_STATE_FINISH,
            item_list=[
                MessageItem(
                    type=ITEM_TYPE_IMAGE,
                    image_item=ImageItem(
                        url="remote-image",
                        media=MediaInfo(
                            encrypt_query_param="encrypted-query",
                            aes_key="encryption-key",
                        ),
                    ),
                )
            ],
        )
        try:
            first = await gateway.accept(message)
            assert first is not None
            assert first.accepted and not first.duplicate
            assert first.task_id
            assert downloads == 1

            task = await store.get_task(first.task_id)
            assert task is not None
            assert task.inputs["media"][0]["attachment_id"]
            attachment_id = task.inputs["media"][0]["attachment_id"]
            assert task.inputs["media"][0]["native_input_available"] is False
            assert await store.list_attachment_refs(attachment_id)

            replay = await gateway.accept(message)
            assert replay is not None
            assert replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            # A deterministic replay verifies the same bytes instead of
            # silently blessing a changed CDN payload.
            assert downloads == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_user_outbox_cannot_project_another_agents_attachment(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)
            refs_before = await store.list_attachment_refs(attachment_id)

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.create_user_outbox(
                    target={
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    content="exfiltrate planner media",
                    agent_id="codex",
                    outbox_id="foreign-media-outbox",
                    attachments=(attachment_id,),
                )

            assert await store.get_outbox_item("foreign-media-outbox") is None
            assert await store.list_outgoing_media(limit=10) == []
            assert await store.list_attachment_refs(attachment_id) == refs_before

            owned = await store.create_user_outbox(
                target={
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                },
                content="planner media",
                agent_id="planner",
                outbox_id="owned-media-outbox",
                attachments=(attachment_id,),
            )
            assert owned.agent_id == "planner"
            assert len(await store.list_outgoing_media(limit=10)) == 1
            with pytest.raises(StoreError, match="Agent|agent_id"):
                await store.create_outgoing_media(
                    attachment_id=attachment_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    outbox_id=owned.outbox_id,
                    idempotency_key=(
                        f"outbox:{owned.outbox_id}:attachment:0:{attachment_id}"
                    ),
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_direct_outgoing_media_requires_attachment_agent_access(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            attachment_id = await _create_planner_owned_attachment(store)
            refs_before = await store.list_attachment_refs(attachment_id)

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.create_outgoing_media(
                    attachment_id=attachment_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                    agent_id="codex",
                    media_id="foreign-direct-media",
                    idempotency_key="foreign-direct-media",
                )

            assert await store.get_outgoing_media("foreign-direct-media") is None
            assert await store.list_attachment_refs(attachment_id) == refs_before

            owned = await store.create_outgoing_media(
                attachment_id=attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
                media_id="owned-direct-media",
                idempotency_key="owned-direct-media",
                metadata={"kind": "image", "mime_type": "image/png", "size_bytes": 1},
            )
            assert owned.attachment_id == attachment_id
            assert owned.metadata["kind"] == "audio"
            assert owned.metadata["mime_type"] == "audio/mpeg"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_outgoing_projection_uses_authoritative_attachment_metadata(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        stored = files.put_bytes(b"plain-file", attachment_id="authoritative-file")
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            await store.register_attachment(stored, kind="file")
            owner = await store.create_task(
                _task(agent_id="codex", attachment_id=stored.attachment_id)
            )
            with pytest.raises(
                StoreError, match=r"attachment metadata conflicts \((kind|mime_type)\)"
            ):
                await store.create_user_outbox(
                    target=owner.reply_target,
                    content="spoofed media metadata",
                    agent_id="codex",
                    attachments=(
                        {
                            "attachment_id": stored.attachment_id,
                            "kind": "image",
                            "mime_type": "image/png",
                        },
                    ),
                )
            assert await store.list_outbox() == []
            assert await store.list_outgoing_media(limit=10) == []
        finally:
            await store.close()

    asyncio.run(scenario())
