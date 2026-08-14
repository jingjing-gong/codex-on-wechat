"""Ingress media promotion and managed attachment durability regressions."""

from __future__ import annotations

import asyncio
import threading

import pytest

from src.channels.models import InboundEnvelope
from src.channels.wechat import WeChatGateway, WeChatInboundMediaPromoter
from src.runtime.manager import TaskManager
from src.runtime.media import AttachmentError, AttachmentStore
from src.runtime.sqlite_store import SQLiteStore, StoreError
from wechat_ilink.monitor import Monitor
from wechat_ilink.types import (
    ITEM_TYPE_FILE,
    ITEM_TYPE_IMAGE,
    ITEM_TYPE_NONE,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    ImageItem,
    FileItem,
    MediaInfo,
    MessageItem,
    WeixinMessage,
)


class _Runtime:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, emit):
        return None

    async def interrupt(self, task_id: str) -> bool:
        return False


def _envelope(*, message_id: str = "message-1", media: dict[str, object]) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=message_id,
        text="look at this",
        raw={"media": [media]},
    )


def _wire_media(*, attachment_id: str = "caller-owned-id") -> dict[str, object]:
    return {
        "kind": "image",
        "remote_id": "remote-image-1",
        "encrypted_query_param": "encrypted-query-secret",
        "encryption_key": "aes-key-secret",
        "filename": "photo.png",
        "mime_type": "image/png",
        "attachment_id": attachment_id,
    }


def _message(
    *, encrypted_query: str = "encrypted-query-secret", remote_id: str = "remote-image-1"
) -> WeixinMessage:
    return WeixinMessage(
        seq=1,
        message_id=1001,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(
                type=ITEM_TYPE_IMAGE,
                image_item=ImageItem(
                    url=remote_id,
                    media=MediaInfo(
                        encrypt_query_param=encrypted_query,
                        aes_key="aes-key-secret",
                    ),
                ),
            )
        ],
    )


def _file_message(*, message_id: int = 1002, filename: str = "report.txt") -> WeixinMessage:
    return WeixinMessage(
        seq=2,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(
                type=ITEM_TYPE_FILE,
                file_item=FileItem(
                    file_name=filename,
                    len="12",
                    media=MediaInfo(
                        encrypt_query_param="encrypted-file-query",
                        aes_key="file-aes-key",
                    ),
                ),
            )
        ],
    )


def _unsupported_message(
    *, message_id: int = 1099, item_type: int = 99
) -> WeixinMessage:
    return WeixinMessage(
        seq=99,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem.model_validate(
                {
                    "type": item_type,
                    # Both known-looking and future fields are deliberately
                    # populated: neither may cross the redaction boundary.
                    "file_item": {
                        "file_name": "secret.bin",
                        "media": {
                            "encrypt_query_param": "unknown-query-secret",
                            "aes_key": "unknown-aes-secret",
                        },
                    },
                    "future_payload": {"token": "unknown-future-secret"},
                }
            )
        ],
    )


@pytest.mark.parametrize("item_type", [ITEM_TYPE_NONE, 99])
def test_unsupported_item_is_durable_redacted_context_and_replay_deduplicates(
    tmp_path, item_type,
):
    async def scenario() -> None:
        root = tmp_path / "files"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        downloads = 0

        async def downloader(**_kwargs):
            nonlocal downloads
            downloads += 1
            raise AssertionError("unsupported media must not be downloaded")

        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=AttachmentStore(root),
            media_downloader=downloader,
        )
        expected = {
            "kind": "unsupported",
            "mime_type": "application/octet-stream",
            "filename": "",
            "size": 0,
            "checksum": "",
            "available": False,
            "native_input_available": False,
            "error": f"unsupported WeChat item type: {item_type}",
        }
        try:
            first = await gateway.accept(_unsupported_message(item_type=item_type))
            assert first is not None and first.accepted and not first.duplicate
            assert first.task_id
            assert downloads == 0

            inbound = await store.get_inbound("wechat", "bot", "1099")
            assert inbound is not None
            assert inbound.payload["media"] == [expected]
            task = await store.get_task(first.task_id)
            assert task is not None
            assert task.inputs == {"text": "", "media": [expected]}
            assert len(await store.list_tasks()) == 1
            assert await store.list_attachments() == []

            persisted = repr((inbound.payload, task.inputs))
            for secret in (
                "unknown-query-secret",
                "unknown-aes-secret",
                "unknown-future-secret",
                "secret.bin",
                "future_payload",
                "file_item",
            ):
                assert secret not in persisted

            replay = await gateway.accept(_unsupported_message(item_type=item_type))
            assert replay is not None and replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            assert len(await store.list_tasks()) == 1
            assert downloads == 0

            with pytest.raises(AttachmentError, match="wire reference"):
                await gateway.accept(_unsupported_message(item_type=100))
            assert len(await store.list_tasks()) == 1
            assert downloads == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mixed_media_replay_reverifies_healthy_managed_bytes(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "files"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        payloads = [
            b"\x89PNG\r\n\x1a\noriginal-image",
            b"\x89PNG\r\n\x1a\nchanged-image",
        ]
        downloads = 0

        async def downloader(**_kwargs):
            nonlocal downloads
            payload = payloads[downloads]
            downloads += 1
            return payload

        message = _message()
        message.item_list.append(MessageItem(type=99))
        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=AttachmentStore(root),
            media_downloader=downloader,
        )
        try:
            first = await gateway.accept(message)
            assert first is not None and first.accepted and not first.duplicate
            assert downloads == 1

            with pytest.raises(AttachmentError, match="content conflicts"):
                await gateway.accept(message)
            assert downloads == 2
            assert len(await store.list_tasks()) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_uploaded_file_is_stored_and_prompts_before_agent_task_creation(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "files"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        manager = TaskManager(store, runtime=_Runtime(), worker_count=0)

        async def downloader(**_kwargs):
            return b"uploaded file bytes"

        gateway = WeChatGateway(
            manager,
            bot_id="bot",
            attachment_store=AttachmentStore(root),
            media_downloader=downloader,
        )
        try:
            accepted = await gateway.accept(_file_message())
            assert accepted is not None and accepted.accepted
            assert accepted.task_id == ""
            assert accepted.command_response == (
                "## File stored\n\n- `report.txt`\n\n"
                "What would you like me to do with it?"
            )
            assert await store.list_tasks() == []

            inbound = await store.get_inbound("wechat", "bot", "1002")
            assert inbound is not None
            assert inbound.status.value == "accepted"
            media = inbound.payload["media"]
            assert len(media) == 1
            attachment_id = media[0]["attachment_id"]
            stored = await store.get_attachment(attachment_id)
            assert stored is not None
            assert stored.local_path.read_bytes() == b"uploaded file bytes"
            refs = await store.list_attachment_refs(attachment_id)
            assert {(ref["owner_kind"], ref["owner_id"]) for ref in refs} == {
                ("inbound_message", inbound.message_id)
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_successful_promotion_persists_canonical_task_inputs_and_replays_deterministically(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=tmp_path / "files")
        await store.initialize()
        manager = TaskManager(store, runtime=_Runtime(), worker_count=0)
        downloaded: list[dict[str, object]] = []

        async def downloader(**kwargs):
            downloaded.append(kwargs)
            return b"\x89PNG\r\n\x1a\nmanaged-image"

        gateway = WeChatGateway(
            manager,
            bot_id="bot",
            attachment_store=AttachmentStore(tmp_path / "files"),
            media_downloader=downloader,
        )
        try:
            accepted = await gateway.accept(_message())
            assert accepted is not None and accepted.accepted
            task = await store.get_task(accepted.task_id)
            assert task is not None
            assert len(downloaded) == 1
            media = task.inputs["media"]
            assert len(media) == 1
            promoted = media[0]
            assert promoted["attachment_id"].startswith("wechat-in-")
            assert promoted["attachment_id"] != "caller-owned-id"
            assert promoted["path"].startswith(str((tmp_path / "files").resolve()))
            assert promoted["mime_type"] == "image/png"
            assert "encrypted-query-secret" not in repr(task.inputs)
            assert "aes-key-secret" not in repr(task.inputs)
            assert "remote-image-1" not in repr(task.inputs)
            assert "__media_wire_fingerprints" not in task.inputs
            assert "__route_snapshot" not in task.inputs
            assert "__command_snapshot" not in task.inputs

            inbound = await store.get_inbound("wechat", "bot", "1001")
            assert inbound is not None
            persisted_payload = repr(inbound.payload)
            assert "item_list" not in inbound.payload
            assert "itemList" not in inbound.payload
            assert "encrypted-query-secret" not in persisted_payload
            assert "aes-key-secret" not in persisted_payload

            envelope = gateway.normalize(_message())
            assert envelope is not None
            promoter = gateway.media_promoter
            assert promoter is not None
            first = await promoter.promote(envelope)
            replay = await promoter.promote(envelope)
            assert first.raw == replay.raw
            expected_id = promoter.attachment_id(envelope, 0, "image")
            assert first.raw["media"][0]["attachment_id"] == expected_id

            refs = await store.list_attachment_refs(expected_id)
            assert {ref["owner_kind"] for ref in refs} == {"inbound_message", "task"}
        finally:
            await store.close()

    asyncio.run(scenario())


def test_gateway_replay_rejects_changed_wire_media_reference(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=tmp_path / "files")
        await store.initialize()
        manager = TaskManager(store, runtime=_Runtime(), worker_count=0)

        async def downloader(**kwargs):
            # Both references resolve to the same bytes; the immutable wire
            # fingerprint must still reject the second envelope.
            return b"\x89PNG\r\n\x1a\nmanaged-image"

        gateway = WeChatGateway(
            manager,
            bot_id="bot",
            attachment_store=AttachmentStore(tmp_path / "files"),
            media_downloader=downloader,
        )
        try:
            first = await gateway.accept(_message())
            assert first is not None and first.accepted
            with pytest.raises(StoreError, match="immutable envelope"):
                await gateway.accept(
                    _message(
                        encrypted_query="changed-query",
                        remote_id="remote-image-2",
                    )
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_downloader_failure_retains_monitor_cursor_and_does_not_publish_file(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=tmp_path / "files")
        await store.initialize()

        async def downloader(**kwargs):
            raise OSError("cdn unavailable")

        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=AttachmentStore(tmp_path / "files"),
            media_downloader=downloader,
        )
        stop = threading.Event()

        class Client:
            bot_id = "bot"

            def __init__(self) -> None:
                self.cursors: list[str] = []

            def get_updates(self, cursor: str):
                self.cursors.append(cursor)
                if len(self.cursors) == 1:
                    return type("Response", (), {
                        "ret": 0,
                        "errcode": 0,
                        "errmsg": "",
                        "msgs": [_message()],
                        "get_updates_buf": "cursor-after-media",
                    })()
                stop.set()
                return type("Response", (), {
                    "ret": 0,
                    "errcode": 0,
                    "errmsg": "",
                    "msgs": [],
                    "get_updates_buf": "",
                })()

        monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
        client = Client()
        monitor_loop = asyncio.new_event_loop()
        monitor = Monitor(
            client,
            gateway.monitor_handler(monitor_loop),
            max_workers=1,
            durable_acceptance=True,
            initial_cursor="",
        )
        try:
            monitor.run(stop)
        finally:
            monitor._executor.shutdown(wait=True)
            monitor_loop.close()
        assert client.cursors == ["", ""]
        assert await store.get_cursor(channel="wechat", bot_id="bot") == ""
        assert await store.list_attachments() == []
        assert await store.list_tasks() == []
        assert await store.get_inbound("wechat", "bot", "1001") is None
        await store.close()

    asyncio.run(scenario())


def test_attachment_id_from_caller_is_ignored(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=tmp_path / "files")
        await store.initialize()
        try:
            attachments = AttachmentStore(tmp_path / "files")

            async def downloader(**kwargs):
                return b"payload"

            envelope = _envelope(media=_wire_media(attachment_id="attacker-selected"))
            promoter = WeChatInboundMediaPromoter(attachments, store, downloader=downloader)
            promoted = await promoter.promote(envelope)
            actual = promoted.raw["media"][0]["attachment_id"]
            assert actual == promoter.attachment_id(envelope, 0, "image")
            assert actual != "attacker-selected"
            assert (tmp_path / "files" / "attacker-selected").exists() is False
        finally:
            await store.close()

    asyncio.run(scenario())


def test_idempotent_retry_succeeds_when_existing_file_exactly_fills_quota(tmp_path):
    payload = b"exact-quota"
    first = AttachmentStore(tmp_path / "files", quota_bytes=len(payload))
    stored = first.put_bytes(payload, attachment_id="deterministic")
    restarted = AttachmentStore(tmp_path / "files", quota_bytes=len(payload))

    replay = restarted.put_bytes_idempotent(
        payload,
        filename="same.bin",
        attachment_id=stored.attachment_id,
    )
    assert replay.attachment_id == stored.attachment_id
    assert replay.checksum == stored.checksum
    assert replay.local_path.read_bytes() == payload


def test_restart_hydrates_authoritative_metadata_and_rejects_tampered_bytes(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "files"
        database_path = tmp_path / "runtime.sqlite"
        first_store = SQLiteStore(database_path, attachment_root=root)
        await first_store.initialize()
        envelope = _envelope(media=_wire_media())

        async def downloader(**kwargs):
            return b"immutable-payload"

        try:
            first_promoter = WeChatInboundMediaPromoter(
                AttachmentStore(root), first_store, downloader=downloader
            )
            promoted = await first_promoter.promote(envelope)
            attachment_id = promoted.raw["media"][0]["attachment_id"]
        finally:
            await first_store.close()

        restarted_store = SQLiteStore(database_path, attachment_root=root)
        await restarted_store.initialize()
        restarted_files = AttachmentStore(root)
        try:
            replayed = await WeChatInboundMediaPromoter(
                restarted_files, restarted_store, downloader=downloader
            ).promote(envelope)
            assert replayed.raw["media"][0]["attachment_id"] == attachment_id
            assert restarted_files.get(attachment_id).checksum == promoted.raw["media"][0]["checksum"]

            path = restarted_files.get(attachment_id).local_path
            path.write_bytes(b"tampered")
            with pytest.raises(AttachmentError):
                await WeChatInboundMediaPromoter(
                    AttachmentStore(root), restarted_store, downloader=downloader
                ).promote(envelope)
        finally:
            await restarted_store.close()

    asyncio.run(scenario())


def test_promoter_downloads_multiple_media_items_concurrently(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "files"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        started = 0
        all_started = asyncio.Event()

        async def downloader(**kwargs):
            nonlocal started
            started += 1
            if started == 2:
                all_started.set()
            await asyncio.wait_for(all_started.wait(), timeout=0.5)
            return str(kwargs["encrypted_query_param"]).encode("utf-8")

        second = {
            **_wire_media(),
            "remote_id": "remote-image-2",
            "encrypted_query_param": "encrypted-query-2",
        }
        envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="multiple-media",
            raw={"media": [_wire_media(), second]},
        )
        promoter = WeChatInboundMediaPromoter(
            AttachmentStore(root),
            store,
            downloader=downloader,
            promotion_timeout=1.0,
        )
        try:
            promoted = await asyncio.wait_for(promoter.promote(envelope), timeout=2.0)
            assert started == 2
            assert [item["attachment_id"] for item in promoted.raw["media"]] == [
                promoter.attachment_id(envelope, 0, "image"),
                promoter.attachment_id(envelope, 1, "image"),
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_promoter_enforces_one_deadline_and_drains_siblings(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "files"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        started = 0
        cancelled = 0

        async def downloader(**_kwargs):
            nonlocal started, cancelled
            started += 1
            try:
                await asyncio.Event().wait()
            finally:
                cancelled += 1

        envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="timed-out-media",
            raw={"media": [_wire_media(), _wire_media()]},
        )
        promoter = WeChatInboundMediaPromoter(
            AttachmentStore(root),
            store,
            downloader=downloader,
            promotion_timeout=0.05,
        )
        try:
            with pytest.raises(AttachmentError, match="media promotion timed out"):
                await asyncio.wait_for(promoter.promote(envelope), timeout=1.0)
            assert started == 2
            assert cancelled == 2
            assert await store.list_attachments() == []
            assert not any(root.iterdir())
        finally:
            await store.close()

    asyncio.run(scenario())


def test_duplicate_replay_is_acknowledged_after_managed_file_is_blocked(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        downloads = 0

        async def downloader(**_kwargs):
            nonlocal downloads
            downloads += 1
            return b"\x89PNG\r\n\x1a\nreplayable"

        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=AttachmentStore(root),
            media_downloader=downloader,
        )
        try:
            first = await gateway.accept(_message())
            assert first is not None and first.accepted and not first.duplicate
            task = await store.get_task(first.task_id)
            assert task is not None
            attachment = await store.get_attachment(
                task.inputs["media"][0]["attachment_id"]
            )
            assert attachment is not None
            attachment.local_path.unlink()
            report = await store.reconcile()
            assert report.missing_attachments == 1

            replay = await gateway.accept(_message())
            assert replay is not None and replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            assert downloads == 1
            assert len(await store.list_tasks()) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_duplicate_replay_is_acknowledged_while_cleanup_is_reserved(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        downloads = 0

        async def downloader(**_kwargs):
            nonlocal downloads
            downloads += 1
            return b"\x89PNG\r\n\x1a\ncleanup-replay"

        gateway = WeChatGateway(
            store,
            bot_id="bot",
            attachment_store=AttachmentStore(root),
            media_downloader=downloader,
        )
        try:
            first = await gateway.accept(_message())
            assert first is not None and first.accepted
            task = await store.get_task(first.task_id)
            assert task is not None
            attachment_id = task.inputs["media"][0]["attachment_id"]
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE attachments SET state='deleting' WHERE attachment_id=?",
                    (attachment_id,),
                )
            )

            replay = await gateway.accept(_message())
            assert replay is not None and replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            assert downloads == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_blocked_replay_rejects_changes_to_secondary_wire_aliases(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        wire = _wire_media()
        envelope = _envelope(media=wire)

        async def downloader(**_kwargs):
            return b"\x89PNG\r\n\x1a\nwire-alias"

        promoter = WeChatInboundMediaPromoter(
            AttachmentStore(root), store, downloader=downloader
        )
        try:
            promoted = await promoter.promote(envelope)
            await store.accept_inbound(
                promoted,
                task={"agent_id": "codex", "inputs": promoted.raw["media"]},
                _trusted_media_wire_fingerprints=promoted.raw[
                    "__media_wire_fingerprints"
                ],
            )
            attachment_id = promoted.raw["media"][0]["attachment_id"]
            (await store.get_attachment(attachment_id)).local_path.unlink()
            await store.reconcile()

            changed = _envelope(
                media={**wire, "media_kind": "video", "aes_key_base64": "changed"}
            )
            with pytest.raises(AttachmentError, match="wire reference"):
                await promoter.promote(changed)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_store_first_inbound_replays_after_managed_promotion_is_enabled(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            first_gateway = WeChatGateway(store, bot_id="bot")
            first = await first_gateway.accept(_message())
            assert first is not None and first.accepted and not first.duplicate

            downloads = 0

            async def downloader(**_kwargs):
                nonlocal downloads
                downloads += 1
                return b"\x89PNG\r\n\x1a\nmanaged-later"

            managed_gateway = WeChatGateway(
                store,
                bot_id="bot",
                attachment_store=AttachmentStore(root),
                media_downloader=downloader,
            )
            replay = await managed_gateway.accept(_message())
            assert replay is not None and replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            assert downloads == 0
            assert await store.list_attachments() == []

            with pytest.raises(AttachmentError, match="original wire reference"):
                await managed_gateway.accept(_message(remote_id="changed-reference"))
            assert downloads == 0
            assert await store.list_attachments() == []
        finally:
            await store.close()

    asyncio.run(scenario())
