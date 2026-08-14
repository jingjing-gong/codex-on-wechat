"""Focused durability regressions for media retention and audio confirmations."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.runtime.media import AttachmentError, AttachmentStore
from src.runtime.confirmation import AudioConfirmationManager, ConfirmationStatus
from src.runtime.manager import TaskManager
from src.runtime.models import InboundMessage
from src.runtime.sqlite_store import InvalidTransition, NotFoundError, SQLiteStore, StoreError
from src.channels.wechat import (
    external_message_id_for,
    normalize_message,
    send_media_delivery,
)
from wechat_ilink.cdn import aes_key_to_base64
from wechat_ilink.types import (
    ITEM_TYPE_IMAGE,
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def test_text_only_wechat_input_excludes_wire_diagnostics():
    envelope = normalize_message(
        WeixinMessage(
            seq=7,
            message_id=42,
            from_user_id="user",
            to_user_id="bot",
            message_type=MESSAGE_TYPE_USER,
            message_state=MESSAGE_STATE_FINISH,
            context_token="rolling-token",
            item_list=[
                MessageItem(
                    type=ITEM_TYPE_TEXT,
                    text_item=TextItem(text="hello"),
                )
            ],
        ),
        bot_id="bot",
    )

    assert envelope is not None
    assert TaskManager._inputs_from_inbound(envelope) == {"text": "hello"}


def test_store_layer_inbound_projection_drops_wire_payload_fields():
    inbound = InboundMessage(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id="wire-payload",
        text="inspect this",
        payload={
            "__media_wire_fingerprints": ["a" * 64],
            "attachments": [
                {
                    "kind": "file",
                    "remote_id": "REMOTE-SECRET",
                    "encrypted_query_param": "QUERY-SECRET",
                    "encryption_key": "AES-SECRET",
                    "metadata": {"nested": "PROMPT-SECRET"},
                }
            ],
            "custom": {"token": "UNTRUSTED"},
        },
    )

    projected = TaskManager._inputs_from_inbound(inbound)
    assert projected["text"] == "inspect this"
    assert projected["attachments"][0]["kind"] == "file"
    assert "__media_wire_fingerprints" not in projected
    assert "custom" not in projected
    assert "SECRET" not in repr(projected)


def test_sqlite_fallback_task_projection_drops_wire_payload_fields(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            accepted = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="fallback-wire-payload",
                    text="inspect this",
                    payload={
                        "attachments": [
                            {
                                "kind": "file",
                                "remote_id": "REMOTE-SECRET",
                                "encryption_key": "AES-SECRET",
                            }
                        ],
                        "custom": {"token": "PROMPT-SECRET"},
                    },
                )
            )
            assert accepted.task is not None
            assert "custom" not in accepted.task.inputs
            assert "SECRET" not in repr(accepted.task.inputs)
            assert "attachments" in accepted.task.inputs
        finally:
            await database.close()

    asyncio.run(scenario())


def test_wechat_fallback_identity_ignores_context_token_rotation():
    def message(*, context_token: str, text: str = "hello") -> WeixinMessage:
        return WeixinMessage(
            from_user_id="user",
            to_user_id="bot",
            message_type=MESSAGE_TYPE_USER,
            message_state=MESSAGE_STATE_FINISH,
            context_token=context_token,
            item_list=[
                MessageItem(
                    type=ITEM_TYPE_TEXT,
                    text_item=TextItem(text=text),
                )
            ],
        )

    original = external_message_id_for(message(context_token="token-a"))
    replay = external_message_id_for(message(context_token="token-b"))
    different_message = external_message_id_for(
        message(context_token="token-b", text="different")
    )

    assert original.startswith("fallback:")
    assert replay == original
    assert different_message != original


def test_attachment_cleanup_consults_durable_references_after_restart(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        database_path = tmp_path / "runtime.sqlite"
        database = SQLiteStore(database_path, attachment_root=root)
        await database.initialize()
        try:
            first_process = AttachmentStore(root)
            stored = first_process.put_bytes(b"durable", attachment_id="retained")
            await database.register_attachment(stored)
            owner = await database.create_task(
                {
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:user:default:codex",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "inputs": {},
                }
            )
            await database.add_attachment_ref(
                "task", owner.task_id, stored.attachment_id
            )
            await database.close()

            # A new process has neither the AttachmentStore's local reference
            # map nor the original SQLite connection.  The durable checker
            # must still protect the file.
            database = SQLiteStore(database_path, attachment_root=root)
            await database.initialize()
            restarted = AttachmentStore(
                root,
                reference_checker=database.attachment_referenced,
            )
            assert await restarted.acleanup(attachment_id=stored.attachment_id) == []
            assert stored.local_path.is_file()
            with pytest.raises(AttachmentError):
                restarted.cleanup(attachment_id=stored.attachment_id)

            await database.remove_attachment_ref(
                "task", owner.task_id, stored.attachment_id, agent_id="codex"
            )
            assert await restarted.acleanup(attachment_id=stored.attachment_id) == [
                stored.attachment_id
            ]
            assert not stored.local_path.exists()
        finally:
            await database.close()

    asyncio.run(scenario())


def test_outgoing_media_claim_honors_parent_outbox_delivery_eligibility(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        database = SQLiteStore(
            tmp_path / "runtime.sqlite", attachment_root=root
        )
        await database.initialize()
        try:
            attachment_store = AttachmentStore(root)
            stored = attachment_store.put_bytes(
                b"managed media", attachment_id="outgoing"
            )
            await database.register_attachment(stored, kind="image")
            target = {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "session_id": "default",
                "context_token": "original-context",
            }
            suppressed = await database.create_user_outbox(
                target=target,
                content="background result",
                agent_id="codex",
                outbox_id="suppressed-outbox",
                priority=2,
                notify_enabled=False,
                attachments=(stored.attachment_id,),
            )
            inbox_only = await database.create_user_outbox(
                target=target,
                content="inbox result",
                agent_id="codex",
                outbox_id="inbox-only-outbox",
                priority=2,
                delivery_mode="inbox_only",
                notify_enabled=True,
                attachments=(stored.attachment_id,),
            )
            explicit = await database.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="explicit-media",
                idempotency_key="explicit-media",
            )

            claimed = await database.claim_outgoing_media(
                "media-worker", limit=10, channel="wechat", bot_id="bot"
            )
            assert [item.media_id for item in claimed] == [explicit.media_id]

            media_by_outbox = {
                item.outbox_id: item
                for item in await database.list_outgoing_media(limit=10)
                if item.outbox_id is not None
            }
            assert media_by_outbox[suppressed.outbox_id].metadata[
                "context_token"
            ] == "original-context"
            assert media_by_outbox[inbox_only.outbox_id].metadata[
                "context_token"
            ] == "original-context"
            assert media_by_outbox[suppressed.outbox_id].state.value == "ready"
            assert media_by_outbox[suppressed.outbox_id].attempts == 0
            assert media_by_outbox[inbox_only.outbox_id].state.value == "ready"
            assert media_by_outbox[inbox_only.outbox_id].attempts == 0

            # Eligibility is derived from the durable parent at claim time.
            # Re-enabling notifications releases the push-eligible media, but
            # an inbox-only attachment remains stored for presentation.
            await database.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                enabled=True,
            )
            claimed = await database.claim_outgoing_media(
                "media-worker", limit=10, channel="wechat", bot_id="bot"
            )
            assert [item.outbox_id for item in claimed] == [suppressed.outbox_id]
            inbox_media = await database.get_outgoing_media(
                media_by_outbox[inbox_only.outbox_id].media_id
            )
            assert inbox_media is not None
            assert inbox_media.state.value == "ready"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_wechat_media_send_reuses_stable_wire_identity_and_target():
    class Client:
        bot_id = "bot"

        def __init__(self) -> None:
            self.requests = []

        def send_message(self, request):
            self.requests.append(request)
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

    aes_key = "00112233445566778899aabbccddeeff"
    record = SimpleNamespace(
        media_id="stable-media-id",
        external_user_id="user",
        remote_id="encrypted-download-param",
        encryption_key=aes_key,
        metadata={
            "kind": "image",
            "mime_type": "image/png",
            "size_bytes": 42,
            "context_token": "original-context",
        },
    )
    client = Client()
    uploaded = SimpleNamespace(cipher_size=48)

    assert send_media_delivery(client, record, uploaded)
    assert send_media_delivery(client, record, uploaded)
    first, replay = (request.msg for request in client.requests)
    assert first.client_id == replay.client_id
    assert first.to_user_id == "user"
    assert first.context_token == "original-context"
    assert len(first.item_list) == 1
    item = first.item_list[0]
    assert item.type == ITEM_TYPE_IMAGE
    assert item.image_item is not None
    assert item.image_item.mid_size == 48
    assert item.image_item.media is not None
    assert item.image_item.media.encrypt_query_param == "encrypted-download-param"
    assert item.image_item.media.aes_key == aes_key_to_base64(aes_key)


def test_wechat_media_send_rejects_nonzero_protocol_errcode():
    class Client:
        bot_id = "bot"

        def send_message(self, _request):
            return SimpleNamespace(ret=0, errcode=17, errmsg="media denied")

    record = SimpleNamespace(
        media_id="failed-media-id",
        external_user_id="user",
        remote_id="encrypted-download-param",
        encryption_key="00112233445566778899aabbccddeeff",
        metadata={"kind": "image", "mime_type": "image/png"},
    )

    with pytest.raises(RuntimeError, match=r"errcode=17.*media denied"):
        send_media_delivery(Client(), record)


def test_transcription_replay_omitted_timestamps_is_compatible(tmp_path):
    async def scenario() -> None:
        current = datetime(2030, 1, 1, tzinfo=timezone.utc)
        database = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current
        )
        await database.initialize()
        try:
            first = await database.create_transcription_candidate(
                confirmation_id="confirmation-1",
                candidate_text="hello",
            )
            replay = await database.create_transcription_candidate(
                confirmation_id="confirmation-1",
                candidate_text="hello",
            )
            assert replay["created_at"] == first["created_at"]
            assert replay["expires_at"] == (
                current + timedelta(seconds=300)
            ).isoformat(timespec="microseconds")
            assert replay["expires_at"] == first["expires_at"]
        finally:
            await database.close()

    asyncio.run(scenario())


def test_direct_candidate_default_ttl_is_durable_and_configurable(tmp_path):
    async def scenario() -> None:
        current = datetime(2031, 2, 3, 4, 5, 6, tzinfo=timezone.utc)
        database = SQLiteStore(
            tmp_path / "runtime.sqlite",
            clock=lambda: current,
            transcription_ttl_seconds=42,
        )
        await database.initialize()
        try:
            first = await database.create_transcription_candidate(
                confirmation_id="ttl-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                candidate_text="hello",
            )
            assert first["expires_at"] == (
                current + timedelta(seconds=42)
            ).isoformat(timespec="microseconds")

            # A replay computes a fresh default locally, but must return the
            # immutable first row rather than asserting that new clock value.
            later = current + timedelta(seconds=10)
            database.clock = lambda: later
            replay = await database.create_transcription_candidate(
                confirmation_id="ttl-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                candidate_text="hello",
            )
            assert replay["expires_at"] == first["expires_at"]

            # ``None`` remains an explicit opt-out; omission is what receives
            # the durable default.
            no_expiry = await database.create_transcription_candidate(
                confirmation_id="no-expiry-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                candidate_text="hello",
                expires_at=None,
            )
            assert no_expiry["expires_at"] is None
        finally:
            await database.close()

    asyncio.run(scenario())


def test_accept_inbound_assigns_default_candidate_ttl_and_replays_it(tmp_path):
    async def scenario() -> None:
        current = datetime(2032, 3, 4, 5, 6, 7, tzinfo=timezone.utc)
        database = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current
        )
        await database.initialize()
        try:
            message = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                external_message_id="voice-default-ttl",
                text="caption",
                payload={
                    "media": [
                        {
                            "kind": "audio",
                            "remote_id": "audio",
                            "candidate_text": "hello",
                        }
                    ]
                },
            )
            first = await database.accept_inbound(
                message,
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "inbound-ttl-confirmation",
                        "candidate_text": "hello",
                    },
                ),
            )
            assert first.confirmation_ids == ("inbound-ttl-confirmation",)
            candidate = await database.get_transcription_candidate(
                "inbound-ttl-confirmation"
            )
            assert candidate is not None
            expected = current + timedelta(seconds=300)
            assert candidate["expires_at"] == expected.isoformat(
                timespec="microseconds"
            )

            # Redelivery returns the durable candidate row and does not move
            # the expiry window forward.
            database.clock = lambda: current + timedelta(seconds=20)
            replay = await database.accept_inbound(
                message,
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "inbound-ttl-confirmation",
                        "candidate_text": "hello",
                    },
                ),
            )
            assert replay.duplicate
            candidate_replay = await database.get_transcription_candidate(
                "inbound-ttl-confirmation"
            )
            assert candidate_replay is not None
            assert candidate_replay["expires_at"] == candidate["expires_at"]
        finally:
            await database.close()

    asyncio.run(scenario())


def test_audio_confirmation_manager_delegates_to_durable_store(tmp_path):
    async def scenario() -> None:
        database_path = tmp_path / "runtime.sqlite"
        database = SQLiteStore(database_path)
        await database.initialize()
        try:
            manager = AudioConfirmationManager(database)
            candidate = await manager.create_candidate(
                session_key="wechat:bot:user:default",
                attachment_id="",
                candidate_text="hello from audio",
                confirmation_id="durable-confirmation",
            )
            assert candidate.status is ConfirmationStatus.PENDING
            replay = await manager.create_candidate(
                session_key="wechat:bot:user:default",
                attachment_id="",
                candidate_text="hello from audio",
                confirmation_id="durable-confirmation",
            )
            assert replay.created_at == candidate.created_at
            assert replay.expires_at == candidate.expires_at
        finally:
            await database.close()

        restarted = SQLiteStore(database_path)
        await restarted.initialize()
        try:
            manager = AudioConfirmationManager(restarted)
            assert (await manager.get("durable-confirmation")).candidate_text == "hello from audio"
            assert (await manager.confirm("durable-confirmation")).status is ConfirmationStatus.CONFIRMED
            consumed = await manager.consume(
                "durable-confirmation", task_id="audio-task"
            )
            assert consumed.status is ConfirmationStatus.CONSUMED
            assert await manager.consume(
                "durable-confirmation", task_id="audio-task"
            ) == consumed
            with pytest.raises(ValueError, match="already been consumed"):
                await manager.consume(
                    "durable-confirmation", task_id="different-task"
                )
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_transcription_replay_rejects_explicit_timestamp_changes(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="confirmation-2",
                candidate_text="hello",
                created_at="2030-01-01T00:00:00+00:00",
                expires_at="2030-01-02T00:00:00+00:00",
            )
            with pytest.raises(StoreError):
                await database.create_transcription_candidate(
                    confirmation_id="confirmation-2",
                    candidate_text="hello",
                    created_at="2030-01-03T00:00:00+00:00",
                )
            with pytest.raises(StoreError):
                await database.create_transcription_candidate(
                    confirmation_id="confirmation-2",
                    candidate_text="hello",
                    expires_at="2030-01-04T00:00:00+00:00",
                )
        finally:
            await database.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("collision_target", "collision_inputs", "conflict_field"),
    (
        (
            {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "other-user",
                "session_id": "default",
            },
            {
                "text": "confirmed text",
                "audio_confirmation_id": "owned-confirmation",
                "attachment_id": None,
                "source": "wechat",
            },
            "external_user_id",
        ),
        (
            {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "owner",
                "session_id": "default",
            },
            {"text": "unrelated task input"},
            "inputs",
        ),
    ),
)
def test_confirmation_dedupe_collision_cannot_consume_candidate(
    tmp_path, collision_target, collision_inputs, conflict_field
):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="owned-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
                agent_id="codex",
                candidate_text="confirmed text",
                source="wechat",
            )
            collision = await database.create_task(
                {
                    "dedupe_key": "confirmation:owned-confirmation",
                    "agent_id": "codex",
                    "conversation_id": (
                        "wechat:bot:"
                        + collision_target["external_user_id"]
                        + ":default:codex"
                    ),
                    "reply_target": collision_target,
                    "inputs": collision_inputs,
                }
            )

            with pytest.raises(
                StoreError,
                match=rf"confirmation task identity conflicts \({conflict_field}\)",
            ):
                await database.confirm_transcription_task("owned-confirmation")

            candidate = await database.get_transcription_candidate(
                "owned-confirmation"
            )
            assert candidate is not None
            assert candidate["status"] == "pending"
            assert candidate["consumed_by_task_id"] is None
            assert (await database.get_task(collision.task_id)).inputs == collision_inputs
        finally:
            await database.close()

    asyncio.run(scenario())


def test_existing_task_cannot_directly_consume_an_unowned_candidate(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="direct-consume-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                candidate_text="confirmed text",
            )
            unrelated = await database.create_task(
                {
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:other-user:default:codex",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "other-user",
                        "session_id": "default",
                    },
                    "inputs": {"text": "unrelated"},
                }
            )
            assert await database.resolve_transcription(
                "direct-consume-confirmation",
                status="confirmed",
                actor="owner",
            )

            with pytest.raises(
                StoreError,
                match=r"confirmation task identity conflicts \(external_user_id\)",
            ):
                await database.consume_transcription(
                    "direct-consume-confirmation", unrelated.task_id
                )
            candidate = await database.get_transcription_candidate(
                "direct-consume-confirmation"
            )
            assert candidate is not None
            assert candidate["status"] == "confirmed"
            assert candidate["consumed_by_task_id"] is None
        finally:
            await database.close()

    asyncio.run(scenario())


def test_confirmed_audio_preserves_original_caption_and_media(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            media = [
                {
                    "kind": "audio",
                    "remote_id": "encrypted-audio",
                    "candidate_text": "transcribed words",
                }
            ]
            accepted = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="owner",
                    external_message_id="voice-message",
                    text="original caption",
                    payload={"wire_diagnostic": "excluded", "media": media},
                ),
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "captioned-confirmation",
                        "candidate_text": "transcribed words",
                        "source": "wechat",
                    },
                ),
            )
            assert accepted.inbound.status.value == "awaiting_confirmation"

            task = await database.confirm_transcription_task(
                "captioned-confirmation"
            )
            assert task.inputs["text"] == "transcribed words"
            assert task.inputs["caption"] == "original caption"
            assert task.inputs["media"] == [
                {
                    "kind": "audio",
                    "mime_type": "application/octet-stream",
                    "filename": "",
                    "size": 0,
                    "checksum": "",
                    "available": False,
                    "native_input_available": False,
                    "candidate_text": "transcribed words",
                }
            ]
            assert "remote_id" not in task.inputs["media"][0]
            assert "wire_diagnostic" not in task.inputs

            replay = await database.confirm_transcription_task(
                "captioned-confirmation"
            )
            assert replay.task_id == task.task_id
            assert replay.inputs == task.inputs
        finally:
            await database.close()

    asyncio.run(scenario())


def test_expired_confirmed_candidate_is_reconciled_durably(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="expired-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                candidate_text="too late",
                created_at="2000-01-01T00:00:00+00:00",
                expires_at="2000-01-02T00:00:00+00:00",
            )
            assert await database.resolve_transcription(
                "expired-confirmation",
                status="confirmed",
                actor="owner",
                now="2000-01-01T12:00:00+00:00",
            )

            candidate = await database.get_transcription_candidate(
                "expired-confirmation"
            )
            assert candidate is not None
            assert candidate["status"] == "expired"
            assert candidate["resolved_by"] == "owner"
            with pytest.raises(
                InvalidTransition, match="transcription candidate is expired"
            ):
                await database.confirm_transcription_task("expired-confirmation")
            assert await database.get_task_by_dedupe(
                "confirmation:expired-confirmation"
            ) is None
        finally:
            await database.close()

    asyncio.run(scenario())


def test_expiring_one_candidate_keeps_sibling_confirmation_actionable(tmp_path):
    async def scenario() -> None:
        current = datetime(2033, 4, 5, 6, 7, 8, tzinfo=timezone.utc)
        database = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current
        )
        await database.initialize()
        try:
            accepted = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="owner",
                    external_message_id="multi-candidate-expiry",
                    text="voice caption",
                ),
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "early-expiry",
                        "candidate_text": "first transcript",
                        "expires_at": current + timedelta(seconds=1),
                    },
                    {
                        "confirmation_id": "later-expiry",
                        "candidate_text": "second transcript",
                        "expires_at": current + timedelta(minutes=1),
                    },
                ),
            )
            assert accepted.inbound.status.value == "awaiting_confirmation"

            current += timedelta(seconds=2)
            expired = await database.get_transcription_candidate("early-expiry")
            assert expired is not None
            assert expired["status"] == "expired"
            inbound = await database.get_inbound(accepted.inbound.message_id)
            assert inbound is not None
            assert inbound.status.value == "awaiting_confirmation"

            task = await database.confirm_transcription_task("later-expiry")
            assert task.inputs["text"] == "second transcript"
            assert len(await database.list_tasks()) == 1
            inbound = await database.get_inbound(accepted.inbound.message_id)
            assert inbound is not None
            assert inbound.status.value == "task_queued"
            assert inbound.task_id == task.task_id
        finally:
            await database.close()

    asyncio.run(scenario())


def test_confirmation_rejects_partial_supplied_reply_scope(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="scoped-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="session-a",
                candidate_text="hello",
            )
            with pytest.raises(
                StoreError,
                match=r"confirmation reply target conflicts \(bot_id\)",
            ):
                await database.confirm_transcription_task(
                    "scoped-confirmation",
                    task={"reply_target": {"channel": "wechat"}},
                )
            candidate = await database.get_transcription_candidate(
                "scoped-confirmation"
            )
            assert candidate is not None
            assert candidate["status"] == "pending"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_confirmation_manager_requires_exact_caller_scope(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            await database.create_transcription_candidate(
                confirmation_id="manager-scoped-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="session-a",
                candidate_text="hello",
            )
            manager = TaskManager(
                database,
                runtime=SimpleNamespace(agent_id="codex"),
                worker_count=0,
            )

            with pytest.raises(
                PermissionError, match="does not belong to this session"
            ):
                await manager.reject_transcription(
                    "manager-scoped-confirmation"
                )
            with pytest.raises(
                PermissionError, match="does not belong to this session"
            ):
                await manager.reject_transcription(
                    "manager-scoped-confirmation",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="other-user",
                    session_id="session-a",
                )

            assert await manager.reject_transcription(
                "manager-scoped-confirmation",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="session-a",
            )
        finally:
            await database.close()

    asyncio.run(scenario())


def test_inbound_replay_cannot_rewrite_immutable_envelope(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            original = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="message-1",
                text="original",
                payload={"media": [{"kind": "image", "remote_id": "r1"}]},
            )
            first = await database.accept_inbound(original, create_task=False)
            replay = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="message-1",
                    text="original",
                    payload={"media": [{"kind": "image", "remote_id": "r1"}]},
                ),
                create_task=False,
            )
            assert replay.duplicate
            assert replay.inbound.message_id == first.inbound.message_id

            with pytest.raises(StoreError):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="message-1",
                        text="rewritten",
                        payload={"media": [{"kind": "image", "remote_id": "r1"}]},
                    ),
                    create_task=False,
                )
            with pytest.raises(StoreError):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="message-1",
                        text="original",
                        payload={"media": [{"kind": "image", "remote_id": "r2"}]},
                    ),
                    create_task=False,
                )
        finally:
            await database.close()

    asyncio.run(scenario())


def test_inbound_replay_cannot_forge_reserved_media_fingerprint(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            message = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="fingerprint-forgery",
                payload={"media": [{"kind": "image", "remote_id": "r1"}]},
            )
            await database.accept_inbound(message, create_task=False)
            stored = await database.get_inbound(
                "wechat", "bot", "fingerprint-forgery"
            )
            assert stored is not None
            fingerprints = stored.payload["__media_wire_fingerprints"]

            with pytest.raises(StoreError, match="immutable envelope"):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="fingerprint-forgery",
                        payload={
                            "media": [{"kind": "image", "remote_id": "r2"}],
                            "__media_wire_fingerprints": fingerprints,
                        },
                    ),
                    create_task=False,
                )
        finally:
            await database.close()

    asyncio.run(scenario())


def test_candidate_route_snapshot_is_immutable_across_front_agent_switch(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            message = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="voice-route-snapshot",
                payload={"media": [{"kind": "audio"}]},
            )
            first = await database.accept_inbound(
                message,
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "route-snapshot-candidate",
                        "agent_id": "planner",
                        "mode_id": "review",
                        "profile_version": 7,
                        "policy_version": 9,
                        "candidate_text": "transcribed route",
                    },
                ),
            )
            assert first.confirmation_ids == ("route-snapshot-candidate",)
            inbound = await database.get_inbound(
                "wechat", "bot", "voice-route-snapshot"
            )
            assert inbound is not None
            assert inbound.payload["__route_snapshot"] == {
                "agent_id": "planner",
                "mode_id": "review",
                "profile_version": 7,
                "policy_version": 9,
            }

            # A redelivery after /agent switching may propose a different
            # route, but it cannot replace the first candidate projection.
            replay = await database.accept_inbound(
                message,
                create_task=False,
                transcription_candidates=(
                    {
                        "confirmation_id": "route-snapshot-candidate",
                        "agent_id": "codex",
                        "mode_id": "chat",
                        "profile_version": 1,
                        "policy_version": 1,
                        "candidate_text": "tampered route",
                    },
                ),
            )
            assert replay.duplicate
            assert replay.confirmation_ids == ("route-snapshot-candidate",)

            task = await database.confirm_transcription_task(
                "route-snapshot-candidate"
            )
            assert task.agent_id == "planner"
            assert task.mode_id == "review"
            assert task.profile_version == 7
            assert task.policy_version == 9
            assert task.inputs["text"] == "transcribed route"
        finally:
            await database.close()

    asyncio.run(scenario())


def test_command_route_snapshot_uses_command_policy_when_task_is_partial(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            accepted = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="command-route-snapshot",
                    text="/status",
                    payload={
                        "__command_snapshot": {"agent_id": "attacker"},
                        "__route_snapshot": {"agent_id": "attacker"},
                    },
                ),
                create_task=False,
                task={"agent_id": "planner"},
                command_snapshot={
                    "agent_id": "planner",
                    "mode_id": "review",
                    "profile_version": 11,
                    "policy_version": 13,
                },
            )
            assert accepted.inbound.status.value == "accepted"
            inbound = await database.get_inbound(
                "wechat", "bot", "command-route-snapshot"
            )
            assert inbound is not None
            assert inbound.payload["__route_snapshot"] == {
                "agent_id": "planner",
                "mode_id": "review",
                "profile_version": 11,
                "policy_version": 13,
            }
            assert inbound.payload["__command_snapshot"] == {
                "agent_id": "planner",
                "mode_id": "review",
                "profile_version": 11,
                "policy_version": 13,
            }
        finally:
            await database.close()

    asyncio.run(scenario())


def test_task_acceptance_repairs_a_previously_stored_inbound(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            message = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="message-stored-before-crash",
                text="resume this work",
            )
            stored = await database.store_inbound(message, cursor="12")
            assert stored.status.value == "stored"
            assert await database.get_task_by_inbound(stored.message_id) is None

            repaired = await database.accept_inbound(message, cursor="12")
            assert repaired.duplicate
            assert repaired.created
            assert repaired.task is not None
            assert repaired.inbound.status.value == "task_queued"
            assert repaired.task.inbound_message_id == stored.message_id

            replay = await database.accept_inbound(message, cursor="12")
            assert replay.duplicate
            assert not replay.created
            assert replay.task is not None
            assert replay.task.task_id == repaired.task.task_id
            assert len(await database.list_tasks()) == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def test_command_only_acceptance_cannot_be_reinterpreted_as_a_task(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            command = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="command-message",
                text="/status",
            )
            accepted = await database.accept_inbound(
                command,
                create_task=False,
                command_snapshot={"agent_id": "codex"},
            )
            assert accepted.inbound.status.value == "accepted"

            replay = await database.accept_inbound(
                command,
                create_task=True,
                task={"agent_id": "planner"},
            )
            assert replay.duplicate
            assert not replay.created
            assert replay.task is None
            assert replay.inbound.status.value == "accepted"
            assert await database.list_tasks() == []
        finally:
            await database.close()

    asyncio.run(scenario())


def test_context_token_rotation_does_not_break_generic_inbound_dedupe(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            first = await database.accept_inbound(
                {
                    "channel": "example",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    # This adapter has no stable external message ID.  The
                    # store must derive one without using the rolling token.
                    "context_token": "token-a",
                    "text": "hello",
                },
                create_task=False,
            )
            replay = await database.accept_inbound(
                {
                    "channel": "example",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "context_token": "token-b",
                    "text": "hello",
                },
                create_task=False,
            )
            assert replay.duplicate
            assert replay.inbound.message_id == first.inbound.message_id
        finally:
            await database.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("external_user_id", "agent_id", "owner_field"),
    (
        ("user-b", "codex", "external_user_id"),
        ("user-a", "planner", "agent_id"),
    ),
)
def test_inbound_dedupe_key_cannot_be_reused_across_owners(
    tmp_path, external_user_id, agent_id, owner_field
):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            first = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user-a",
                    external_message_id="message-a",
                    text="first",
                ),
                task={"agent_id": "codex", "dedupe_key": "caller-key-1"},
            )
            assert first.task is not None

            # A caller-supplied key is an ownership-scoped idempotency token,
            # not a capability to retrieve or attach another user's task.
            with pytest.raises(
                StoreError,
                match=rf"dedupe key conflicts with task ownership \({owner_field}\)",
            ):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=external_user_id,
                        external_message_id="message-b",
                        text="second",
                    ),
                    task={"agent_id": agent_id, "dedupe_key": "caller-key-1"},
                )

            assert await database.get_inbound("wechat", "bot", "message-b") is None
            owner = await database.get_task_by_dedupe("caller-key-1")
            assert owner is not None
            assert owner.task_id == first.task.task_id
            assert await database.get_task_by_inbound(first.inbound.message_id) is not None
        finally:
            await database.close()

    asyncio.run(scenario())


def test_inbound_task_integrity_failure_does_not_ack_or_advance_cursor(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            first = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="message-1",
                    text="first",
                ),
                cursor="1",
            )
            assert first.task is not None

            # The task-id uniqueness error is unrelated to inbound identity
            # deduplication. It must roll back the new envelope and cursor.
            with pytest.raises(StoreError, match="UNIQUE constraint failed"):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="message-2",
                        text="second",
                    ),
                    task={"task_id": first.task.task_id},
                    cursor="2",
                )

            assert await database.get_inbound("wechat", "bot", "message-2") is None
            assert await database.get_cursor(channel="wechat", bot_id="bot") == "1"
            assert len(await database.list_tasks()) == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def test_inbound_child_requires_owned_parent_and_updates_counter(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            parent = await database.create_task(
                {
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:user:default:codex",
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "inputs": {"text": "parent"},
                }
            )

            with pytest.raises(NotFoundError, match="parent task not found"):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="orphan-child",
                        text="orphan",
                    ),
                    task={
                        "parent_task_id": "missing-parent",
                        "child_depth": 1,
                    },
                    cursor="1",
                )
            assert await database.get_inbound("wechat", "bot", "orphan-child") is None
            assert await database.get_cursor(channel="wechat", bot_id="bot") == ""

            with pytest.raises(StoreError, match="child-task depth"):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        external_message_id="bad-depth",
                        text="bad depth",
                    ),
                    task={
                        "parent_task_id": parent.task_id,
                        "child_depth": 3,
                    },
                )

            with pytest.raises(
                StoreError,
                match=r"child task parent conflicts with ownership \(external_user_id\)",
            ):
                await database.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="another-user",
                        external_message_id="foreign-child",
                        text="foreign",
                    ),
                    task={
                        "parent_task_id": parent.task_id,
                        "child_depth": 1,
                    },
                )
            assert await database.get_inbound("wechat", "bot", "foreign-child") is None

            child = await database.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="child-1",
                    text="child",
                ),
                task={
                    "parent_task_id": parent.task_id,
                    "child_depth": 1,
                },
            )
            assert child.task is not None
            assert child.task.parent_task_id == parent.task_id
            assert await database.child_count(parent.task_id) == 1
        finally:
            await database.close()

    asyncio.run(scenario())


def test_thread_binding_is_fenced_after_terminal_task(tmp_path):
    async def scenario() -> None:
        database = SQLiteStore(tmp_path / "runtime.sqlite")
        await database.initialize()
        try:
            task = await database.create_task(
                {
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:user:default:codex",
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "inputs": {"text": "hello"},
                }
            )
            claim = await database.claim_next_task("worker")
            assert claim is not None
            assert await database.mark_task_running(
                task.task_id, claim.claim_token, execution_id=claim.execution_id
            )
            await database.complete_task(
                task.task_id,
                "done",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert not await database.set_task_thread(
                task.task_id, thread_id="late-stale-thread"
            )
            assert (await database.get_task(task.task_id)).thread_id is None
        finally:
            await database.close()

    asyncio.run(scenario())
