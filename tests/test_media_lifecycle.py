"""Outgoing media lifecycle and recovery invariants."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.channels.wechat import (
    WeChatDeliveryWorker,
    WeChatMediaDeliveryWorker,
    send_media_delivery,
)
from src.runtime.media import (
    AttachmentError,
    AttachmentStore,
    ManagedImageOutputPublisher,
)
from src.runtime.models import InboundMessage, MediaDeliveryState
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError
from wechat_ilink.types import ITEM_TYPE_IMAGE, ITEM_TYPE_TEXT


def test_attachment_stores_share_root_writer_lock_and_never_replace_bytes(tmp_path):
    root = tmp_path / "attachments"
    first = AttachmentStore(root, quota_bytes=6)
    second = AttachmentStore(root, quota_bytes=6)

    def publish(store, attachment_id, payload):
        try:
            return store.put_bytes(payload, attachment_id=attachment_id)
        except Exception as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        same_id_futures = (
            executor.submit(first.put_bytes, b"first", attachment_id="immutable"),
            executor.submit(second.put_bytes, b"other", attachment_id="immutable"),
        )
        same_id = []
        for future in same_id_futures:
            try:
                same_id.append(future.result())
            except Exception as exc:
                same_id.append(exc)

    stored = [item for item in same_id if not isinstance(item, Exception)]
    rejected = [item for item in same_id if isinstance(item, Exception)]
    assert len(stored) == 1
    assert len(rejected) == 1
    assert (root / "immutable").read_bytes() in {b"first", b"other"}
    published = Path(stored[0].path).read_bytes()
    assert hashlib.sha256(published).hexdigest() == stored[0].checksum

    # The shared root lock also makes quota accounting cover separate store
    # instances. The existing five-byte file leaves room for only one byte.
    with ThreadPoolExecutor(max_workers=2) as executor:
        quota_results = list(
            executor.map(
                lambda args: publish(*args),
                (
                    (first, "quota-a", b"x"),
                    (second, "quota-b", b"y"),
                ),
            )
        )
    assert sum(not isinstance(item, Exception) for item in quota_results) == 1


def test_process_local_attachment_reference_replay_is_idempotent(tmp_path):
    attachments = AttachmentStore(tmp_path / "attachments")
    stored = attachments.put_bytes(b"retained", attachment_id="retained")

    attachments.add_ref("task", "task-1", stored.attachment_id, "input", 0)
    attachments.add_ref("task", "task-1", stored.attachment_id, "input", 0)
    attachments.remove_ref("task", "task-1", stored.attachment_id, "input", 0)

    assert attachments.cleanup(attachment_id=stored.attachment_id) == [
        stored.attachment_id
    ]


def test_generated_image_publisher_manages_path_and_data_outputs_idempotently(
    tmp_path,
):
    async def scenario() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        managed_root = tmp_path / "attachments"
        source = workspace / "generated.png"
        png = b"\x89PNG\r\n\x1a\ngenerated-image"
        source.write_bytes(png)
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            attachment_root=managed_root,
        )
        await store.initialize()
        files = AttachmentStore(
            managed_root,
            reference_checker=store.attachment_referenced,
        )
        publisher = ManagedImageOutputPublisher(
            files,
            store,
            workspace_root=workspace,
        )
        task = SimpleNamespace(
            task_id="task-1",
            execution_id="execution-1",
            agent_id="codex",
            reply_target=SimpleNamespace(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ),
        )
        try:
            attachment_id = await publisher(
                task,
                source_item_id="image-item-1",
                source_item_ordinal=0,
                saved_path=str(source),
            )
            replay = await publisher(
                task,
                source_item_id="image-item-1",
                source_item_ordinal=0,
                saved_path=str(source),
            )
            assert replay == attachment_id
            stored = await store.get_attachment(attachment_id)
            assert stored is not None
            assert stored.mime_type == "image/png"
            assert stored.local_path.parent == managed_root.resolve()
            assert stored.local_path != source
            assert stored.local_path.read_bytes() == png
            assert await store.can_access_attachment(
                attachment_id,
                agent_id="codex",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )

            data_id = await publisher(
                task,
                source_item_id="image-item-2",
                source_item_ordinal=1,
                result=(
                    "data:image/png;base64,"
                    + base64.b64encode(png).decode("ascii")
                ),
            )
            assert data_id != attachment_id
            assert files.read_bytes(data_id) == png

            outside = tmp_path / "outside.png"
            outside.write_bytes(png)
            with pytest.raises(AttachmentError, match="outside"):
                await publisher(
                    task,
                    source_item_id="image-item-3",
                    source_item_ordinal=2,
                    saved_path=str(outside),
                )

            nested = workspace / "nested"
            nested.mkdir()
            nested_source = nested / "nested.png"
            nested_source.write_bytes(png)
            linked_parent = workspace / "linked-parent"
            linked_parent.symlink_to(nested, target_is_directory=True)
            with pytest.raises(AttachmentError, match="unavailable"):
                await publisher(
                    task,
                    source_item_id="image-item-symlink",
                    source_item_ordinal=3,
                    saved_path=str(linked_parent / "nested.png"),
                )

            fifo = workspace / "not-an-image.fifo"
            fifo.unlink(missing_ok=True)
            os.mkfifo(fifo)
            with pytest.raises(AttachmentError, match="regular file"):
                await asyncio.wait_for(
                    publisher(
                        task,
                        source_item_id="image-item-fifo",
                        source_item_ordinal=4,
                        saved_path=str(fifo),
                    ),
                    timeout=1,
                )

            source.write_bytes(b"\x89PNG\r\n\x1a\nchanged-image")
            with pytest.raises(AttachmentError, match="conflicts"):
                await publisher(
                    task,
                    source_item_id="image-item-1",
                    source_item_ordinal=0,
                    saved_path=str(source),
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_media_send_requires_send_pending_checkpoint(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=tmp_path / "attachments")
        await store.initialize()
        try:
            attachments = AttachmentStore(tmp_path / "attachments")
            stored = attachments.put_bytes(b"media", attachment_id="media-1")
            await store.register_attachment(stored, kind="image")
            item = await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="media-op-1",
                idempotency_key="media-op-1",
            )
            claim = (await store.claim_outgoing_media("worker", channel="wechat", bot_id="bot"))[0]
            assert claim.state is MediaDeliveryState.UPLOADING
            assert await store.transition_outgoing_media(
                item.media_id,
                MediaDeliveryState.UPLOADED,
                claim_token=claim.claim_token,
                remote_id="remote",
                upload_param="query",
                encryption_key="key",
            )
            with pytest.raises(InvalidTransition):
                await store.transition_outgoing_media(
                    item.media_id,
                    MediaDeliveryState.SENT,
                    from_states=(MediaDeliveryState.UPLOADED,),
                    claim_token=claim.claim_token,
                )
            assert await store.transition_outgoing_media(
                item.media_id,
                MediaDeliveryState.SEND_PENDING,
                claim_token=claim.claim_token,
                from_states=(MediaDeliveryState.UPLOADED,),
            )
            assert await store.transition_outgoing_media(
                item.media_id,
                MediaDeliveryState.SENT,
                claim_token=claim.claim_token,
                from_states=(MediaDeliveryState.SEND_PENDING,),
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_ciphertext_size_survives_store_restart_before_wechat_send(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        root = tmp_path / "attachments"
        first = SQLiteStore(database, attachment_root=root)
        await first.initialize()
        files = AttachmentStore(root)
        stored = files.put_bytes(
            b"\x89PNG\r\n\x1a\nrestart-image",
            attachment_id="restart-image",
        )
        await first.register_attachment(stored, kind="image")
        media = await first.create_outgoing_media(
            attachment_id=stored.attachment_id,
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            agent_id="codex",
            media_id="restart-media-op",
            idempotency_key="restart-media-op",
        )
        claim = (await first.claim_outgoing_media("uploader"))[0]
        assert await first.transition_outgoing_media(
            media.media_id,
            MediaDeliveryState.UPLOADED,
            claim_token=claim.claim_token,
            remote_id="encrypted-query",
            upload_param="upload-query",
            encryption_key="00" * 16,
            metadata={"cipher_size": 32},
        )
        assert await first.transition_outgoing_media(
            media.media_id,
            MediaDeliveryState.SEND_PENDING,
            claim_token=claim.claim_token,
        )
        await first.close()

        second = SQLiteStore(database, attachment_root=root)
        await second.initialize()
        try:
            restored = await second.get_outgoing_media(media.media_id)
            assert restored is not None
            assert restored.metadata["cipher_size"] == 32

            class Client:
                bot_id = "bot"

                def __init__(self) -> None:
                    self.requests = []

                def send_message(self, request):
                    self.requests.append(request)
                    return SimpleNamespace(ret=0, errcode=0, errmsg="")

            client = Client()
            assert send_media_delivery(client, restored)
            item = client.requests[0].msg.item_list[0]
            assert item.type == ITEM_TYPE_IMAGE
            assert item.image_item.mid_size == 32
        finally:
            await second.close()

    asyncio.run(scenario())


def test_expired_media_claim_cannot_be_renewed_or_advanced(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        root = tmp_path / "attachments"
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", attachment_root=root, clock=lambda: base
        )
        await store.initialize()
        try:
            files = AttachmentStore(root)
            stored = files.put_bytes(b"media", attachment_id="expired-media")
            await store.register_attachment(stored, kind="image")
            media = await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="expired-media-op",
                idempotency_key="expired-media-op",
                now=base,
            )
            claim = (
                await store.claim_outgoing_media(
                    "worker", lease_seconds=1, now=base
                )
            )[0]
            assert claim.media_id == media.media_id
            expired = base + timedelta(seconds=2)

            assert not await store.renew_outgoing_media_lease(
                media.media_id, claim.claim_token, now=expired
            )
            assert not await store.mark_media_uploaded(
                media.media_id,
                claim_token=claim.claim_token,
                remote_id="stale-remote",
                upload_param="stale-query",
                encryption_key="stale-key",
                now=expired,
            )
            assert not await store.transition_outgoing_media(
                media.media_id,
                MediaDeliveryState.FAILED,
                claim_token=claim.claim_token,
                error="stale uploader",
                now=expired,
            )
            persisted = await store.get_outgoing_media(media.media_id)
            assert persisted is not None
            assert persisted.state is MediaDeliveryState.UPLOADING
            assert persisted.remote_id is None
            assert persisted.last_error is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_failed_direct_media_retains_attachment_for_explicit_retry(tmp_path):
    """A retryable media failure must keep its source bytes alive."""

    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            files = AttachmentStore(root)
            stored = files.put_bytes(b"retryable-media", attachment_id="retryable-media")
            await store.register_attachment(stored, kind="file")
            await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="retryable-media-op",
                idempotency_key="retryable-media-op",
            )
            claim = (await store.claim_outgoing_media("worker"))[0]
            assert await store.transition_outgoing_media(
                "retryable-media-op",
                MediaDeliveryState.FAILED,
                claim_token=claim.claim_token,
                error="temporary channel failure",
            )

            managed = AttachmentStore(
                root, reference_checker=store.attachment_referenced
            )
            assert await managed.acleanup(attachment_id=stored.attachment_id) == []
            assert stored.local_path.exists()
            assert await store.get_attachment_state(stored.attachment_id) == "ready"

            assert await store.retry_outgoing_media("retryable-media-op")
            retried = await store.claim_outgoing_media("worker")
            assert len(retried) == 1
            assert retried[0].media_id == "retryable-media-op"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_outgoing_media_idempotent_replay_survives_attachment_cleanup(tmp_path):
    """Replaying a committed media operation must return its durable row."""

    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            files = AttachmentStore(root)
            stored = files.put_bytes(b"already-sent", attachment_id="already-sent")
            await store.register_attachment(stored, kind="file")
            original = await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="already-sent-op",
                idempotency_key="already-sent-op",
            )
            claim = (await store.claim_outgoing_media("worker"))[0]
            assert await store.transition_outgoing_media(
                original.media_id,
                MediaDeliveryState.UPLOADED,
                claim_token=claim.claim_token,
                remote_id="remote",
                upload_param="query",
                encryption_key="key",
            )
            assert await store.transition_outgoing_media(
                original.media_id,
                MediaDeliveryState.SEND_PENDING,
                claim_token=claim.claim_token,
            )
            assert await store.transition_outgoing_media(
                original.media_id,
                MediaDeliveryState.SENT,
                claim_token=claim.claim_token,
            )

            managed = AttachmentStore(
                root, reference_checker=store.attachment_referenced
            )
            assert await managed.acleanup(attachment_id=stored.attachment_id) == [
                stored.attachment_id
            ]
            with pytest.raises(StoreError, match="attachment|Agent|idempotency"):
                await store.create_outgoing_media(
                    attachment_id=stored.attachment_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    agent_id="planner",
                    media_id="already-sent-op",
                    idempotency_key="already-sent-op",
                )
            with pytest.raises(StoreError, match="session|access"):
                await store.create_outgoing_media(
                    attachment_id=stored.attachment_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="other-session",
                    agent_id="codex",
                    media_id="already-sent-op",
                    idempotency_key="already-sent-op",
                )
            replay = await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="already-sent-op",
                idempotency_key="already-sent-op",
            )
            assert replay.media_id == original.media_id
            assert replay.idempotency_key == original.idempotency_key
            assert replay.state is MediaDeliveryState.SENT
        finally:
            await store.close()

    asyncio.run(scenario())


def test_outgoing_media_transition_preserves_attachment_authority(tmp_path):
    """Upload callbacks may add hints, but cannot relabel managed bytes."""

    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            files = AttachmentStore(root)
            stored = files.put_bytes(b"authoritative-audio", attachment_id="authority")
            await store.register_attachment(stored, kind="audio")
            original = await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="authority-op",
                idempotency_key="authority-op",
            )
            claim = (await store.claim_outgoing_media("worker"))[0]
            assert await store.transition_outgoing_media(
                original.media_id,
                MediaDeliveryState.UPLOADED,
                claim_token=claim.claim_token,
                remote_id="remote",
                upload_param="query",
                encryption_key="key",
                metadata={
                    "kind": "image",
                    "mime_type": "image/png",
                    "size_bytes": 999,
                    "filename": "spoofed.png",
                    "vendor_hint": "preserve-me",
                },
            )
            updated = await store.get_outgoing_media(original.media_id)
            assert updated is not None
            assert updated.metadata["kind"] == original.metadata["kind"]
            assert updated.metadata["mime_type"] == original.metadata["mime_type"]
            assert updated.metadata["size_bytes"] == original.metadata["size_bytes"]
            assert "filename" not in updated.metadata
            assert updated.metadata["vendor_hint"] == "preserve-me"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_media_worker_checkpoints_restored_uploaded_rows_before_send(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            attachments = AttachmentStore(root)
            stored = attachments.put_bytes(b"media", attachment_id="media-2")
            await store.register_attachment(stored, kind="image")
            await store.create_outgoing_media(
                attachment_id=stored.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="media-op-2",
                idempotency_key="media-op-2",
                state=MediaDeliveryState.UPLOADED,
                metadata={"kind": "image"},
            )

            seen_states: list[str] = []

            async def sender(row, _uploaded, _client):
                seen_states.append(str(getattr(row.state, "value", row.state)))
                return True

            worker = WeChatMediaDeliveryWorker(
                store,
                SimpleNamespace(bot_id="bot"),
                uploader=lambda *_args: SimpleNamespace(
                    remote_id="remote", upload_param="query", encryption_key="key"
                ),
                sender=sender,
            )
            assert await worker.run_once() == 1
            assert seen_states == ["send_pending"]
            final = await store.get_outgoing_media("media-op-2")
            assert final is not None
            assert final.state is MediaDeliveryState.SENT
        finally:
            await store.close()

    asyncio.run(scenario())


def test_media_worker_cancels_blocked_upload_after_lease_loss():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.transitions: list[str] = []

        async def claim_outgoing_media(self, *_args, **_kwargs):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "media_id": "media-lease-loss",
                    "attachment_id": "attachment-1",
                    "external_user_id": "user",
                    "state": "uploading",
                    "claim_token": "claim-token",
                    "metadata": {"kind": "image"},
                }
            ]

        async def renew_outgoing_media_lease(self, *_args, **_kwargs) -> bool:
            return False

        async def transition_outgoing_media(
            self, _media_id: str, state: str, **_kwargs
        ) -> bool:
            self.transitions.append(str(state))
            raise AssertionError("stale media operation changed durable state")

    async def scenario() -> None:
        store = Store()
        started = asyncio.Event()
        cancelled = asyncio.Event()
        sends = 0

        async def uploader(_row, _client):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def sender(_row, _uploaded, _client):
            nonlocal sends
            sends += 1
            return True

        worker = WeChatMediaDeliveryWorker(
            store,
            SimpleNamespace(bot_id="bot"),
            uploader=uploader,
            sender=sender,
            lease_seconds=0.15,
        )
        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(started.wait(), timeout=1)

        assert await asyncio.wait_for(running, timeout=1) == 0
        assert cancelled.is_set()
        assert store.transitions == []
        assert sends == 0

    asyncio.run(scenario())


def test_media_worker_renews_later_batch_claim_while_first_upload_blocks():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.second_renewed = asyncio.Event()
            self.transitions: list[tuple[str, str]] = []

        async def claim_outgoing_media(self, *_args, **_kwargs):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "media_id": f"media-{index}",
                    "attachment_id": f"attachment-{index}",
                    "external_user_id": "user",
                    "state": "uploading",
                    "claim_token": f"claim-{index}",
                    "metadata": {"kind": "image"},
                }
                for index in (1, 2)
            ]

        async def renew_outgoing_media_lease(
            self, media_id: str, *_args, **_kwargs
        ) -> bool:
            if media_id == "media-2":
                self.second_renewed.set()
            return True

        async def transition_outgoing_media(
            self, media_id: str, state: str, **_kwargs
        ) -> bool:
            self.transitions.append((media_id, str(state)))
            return True

    release_first = asyncio.Event()

    async def scenario() -> None:
        store = Store()

        async def uploader(row, _client):
            if row["media_id"] == "media-1":
                await release_first.wait()
            return SimpleNamespace(
                remote_id=f"remote-{row['media_id']}",
                upload_param=f"query-{row['media_id']}",
                encryption_key="00" * 16,
            )

        async def sender(*_args):
            return True

        worker = WeChatMediaDeliveryWorker(
            store,
            SimpleNamespace(bot_id="bot"),
            uploader=uploader,
            sender=sender,
            claim_limit=2,
            lease_seconds=0.15,
        )
        running = asyncio.create_task(worker.run_once())
        try:
            # Both rows were claimed together.  The second row must be renewed
            # even though its upload has not started yet.
            await asyncio.wait_for(store.second_renewed.wait(), timeout=1)
        finally:
            release_first.set()

        assert await asyncio.wait_for(running, timeout=1) == 2
        assert store.transitions == [
            ("media-1", "uploaded"),
            ("media-1", "send_pending"),
            ("media-1", "sent"),
            ("media-2", "uploaded"),
            ("media-2", "send_pending"),
            ("media-2", "sent"),
        ]

    asyncio.run(scenario())


def test_real_workers_preserve_text_media_text_fifo_and_one_sender_identity(
    tmp_path,
):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        files = AttachmentStore(root)
        image = files.put_bytes(b"canonical-image", attachment_id="canonical-image")
        await store.register_attachment(image, kind="image")
        accepted = await store.accept_inbound(
            InboundMessage(
                channel="wechat",
                bot_id="sender-bot",
                external_user_id="user",
                external_message_id="text-media-text",
                session_id="default",
                text="build ordered replies",
                context_token="rolling-context",
            ),
            create_task=False,
        )
        target = accepted.inbound.target()
        first_text = await store.project_reply_candidate(
            target=target,
            source_key="ordered:text:1",
            content="first text",
        )
        media = await store.project_reply_candidate(
            target=target,
            source_key="ordered:media:2",
            attachments=(image.attachment_id,),
        )
        final_text = await store.project_reply_candidate(
            target=target,
            source_key="ordered:text:3",
            content="final text",
        )
        assert [
            first_text.slots[0].reply_ordinal,
            media.slots[0].reply_ordinal,
            final_text.slots[0].reply_ordinal,
        ] == [1, 2, 3]

        requests: list[object] = []

        def send(request):
            requests.append(request.model_copy(deep=True))
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        selected = SimpleNamespace(bot_id="sender-bot", send_message=send)
        uploaded_with: list[object] = []

        async def uploader(_row, client):
            uploaded_with.append(client)
            return SimpleNamespace(
                remote_id="cdn-query",
                upload_param="cdn-query",
                encryption_key="00" * 16,
                cipher_size=32,
            )

        text_worker = WeChatDeliveryWorker(
            store,
            selected,
            client_resolver={"sender-bot": selected},
        )
        media_worker = WeChatMediaDeliveryWorker(
            store,
            selected,
            client_resolver={"sender-bot": selected},
            uploader=uploader,
            sender=lambda row, upload, client: send_media_delivery(
                client, row, upload
            ),
        )
        try:
            # The media parent is ineligible until its text predecessor is
            # terminal, and the text worker cannot skip across the media row.
            assert await media_worker.run_once() == 0
            assert requests == []
            assert len(await text_worker.run_once()) == 1
            assert await text_worker.run_once() == []
            assert [request.msg.item_list[0].type for request in requests] == [
                ITEM_TYPE_TEXT
            ]

            assert await media_worker.run_once() == 1
            assert uploaded_with == [selected]
            assert len(requests) == 2
            media_request = requests[1].msg
            parent = media.outbox_items[0]
            assert media_request.client_id == parent.client_id
            assert media_request.from_user_id == parent.from_user_id == "sender-bot"
            assert media_request.to_user_id == "user"
            assert media_request.context_token == "rolling-context"
            assert [item.type for item in media_request.item_list] == [
                ITEM_TYPE_IMAGE
            ]
            assert media_request.item_list[0].image_item.mid_size == 32

            persisted_parent = await store.get_outbox_item(parent.outbox_id)
            persisted_children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert persisted_parent is not None
            assert persisted_parent.state.value == "sent"
            assert len(persisted_children) == 1
            assert persisted_children[0].state.value == "sent"

            assert len(await text_worker.run_once()) == 1
            assert [request.msg.item_list[0].type for request in requests] == [
                ITEM_TYPE_TEXT,
                ITEM_TYPE_IMAGE,
                ITEM_TYPE_TEXT,
            ]
            assert [request.msg.client_id for request in requests] == [
                first_text.outbox_items[0].client_id,
                parent.client_id,
                final_text.outbox_items[0].client_id,
            ]
            assert all(
                request.msg.from_user_id == "sender-bot" for request in requests
            )
            # A canonical child is never rediscovered by the legacy claim
            # phase after its sole parent SendMsg completes.
            assert await media_worker.run_once() == 0
            assert len(requests) == 3
        finally:
            await store.close()

    asyncio.run(scenario())


def test_real_media_worker_retries_only_durable_contextless_variant(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        current = [datetime(2030, 1, 1, tzinfo=timezone.utc)]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            attachment_root=root,
            clock=lambda: current[0],
        )
        await store.initialize()
        files = AttachmentStore(root)
        image = files.put_bytes(b"retry-image", attachment_id="retry-image")
        await store.register_attachment(image, kind="image")
        accepted = await store.accept_inbound(
            InboundMessage(
                channel="wechat",
                bot_id="sender-bot",
                external_user_id="user",
                external_message_id="media-contextless-retry",
                session_id="default",
                text="send image",
                context_token="rolling-context",
            ),
            create_task=False,
        )
        projection = await store.project_reply_candidate(
            target=accepted.inbound.target(),
            source_key="media:contextless:retry",
            attachments=(image.attachment_id,),
        )
        parent = projection.outbox_items[0]
        requests: list[object] = []
        responses = iter(
            (
                SimpleNamespace(ret=-2, errcode=0, errmsg="prepare failed"),
                SimpleNamespace(ret=1, errcode=0, errmsg="temporary failure"),
                SimpleNamespace(ret=0, errcode=0, errmsg=""),
            )
        )

        def send(request):
            requests.append(request.model_copy(deep=True))
            return next(responses)

        selected = SimpleNamespace(bot_id="sender-bot", send_message=send)
        upload_calls = 0

        async def uploader(_row, client):
            nonlocal upload_calls
            assert client is selected
            upload_calls += 1
            return SimpleNamespace(
                remote_id="cdn-query",
                upload_param="cdn-query",
                encryption_key="00" * 16,
            )

        worker = WeChatMediaDeliveryWorker(
            store,
            selected,
            client_resolver={"sender-bot": selected},
            uploader=uploader,
            sender=lambda row, upload, client: send_media_delivery(
                client, row, upload
            ),
        )
        try:
            assert await worker.run_once() == 0
            failed = await store.get_outbox_item(parent.outbox_id)
            assert failed is not None
            assert failed.state.value == "retry_wait"
            assert failed.active_wire_variant == "contextless"
            assert failed.contextless_client_id
            assert [request.msg.client_id for request in requests] == [
                parent.client_id,
                failed.contextless_client_id,
            ]
            assert [request.msg.context_token for request in requests] == [
                "rolling-context",
                "",
            ]
            assert upload_calls == 1

            current[0] += timedelta(seconds=301)
            assert await worker.run_once() == 1
            assert [request.msg.client_id for request in requests] == [
                parent.client_id,
                failed.contextless_client_id,
                failed.contextless_client_id,
            ]
            assert requests[-1].msg.context_token == ""
            assert upload_calls == 1
            completed = await store.get_outbox_item(parent.outbox_id)
            children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert completed is not None
            assert completed.state.value == "sent"
            assert completed.active_wire_variant == "contextless"
            assert len(children) == 1
            assert children[0].state.value == "sent"
        finally:
            await store.close()

    asyncio.run(scenario())
