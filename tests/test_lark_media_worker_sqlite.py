from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from src.channels.lark import (
    LarkBotProfile,
    LarkMediaDeliveryWorker,
    LarkRateLimitError,
)
from src.runtime.media import AttachmentStore
from src.runtime.models import MediaDeliveryState, OutboxState, ReplyTarget
from src.runtime.sqlite_store import SQLiteStore


def _profile(tmp_path: Path) -> LarkBotProfile:
    config = tmp_path / "lark-config"
    config.mkdir(mode=0o700, exist_ok=True)
    return LarkBotProfile(
        profile_id="bot-a",
        app_id="cli_botaccount1",
        config_dir=config,
    )


async def _create_bundle(
    store: SQLiteStore,
    files: AttachmentStore,
    *,
    outbox_id: str,
    content: str,
    actor_id: str = "ou_actor1234",
    chat_id: str = "oc_chat1234",
):
    first = files.put_bytes(
        b"first attachment",
        attachment_id=f"{outbox_id}-first",
        filename="first.png",
    )
    second = files.put_bytes(
        b"second attachment",
        attachment_id=f"{outbox_id}-second",
        filename="second.txt",
    )
    await store.register_attachment(first, kind="image")
    await store.register_attachment(second, kind="file")
    parent = await store.create_account_outbox(
        channel="lark",
        bot_id="cli_botaccount1",
        target=ReplyTarget(
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id=actor_id,
            source_message_id=f"om_source_{outbox_id}",
            conversation_subject_id="thread-subject",
            conversation_subject_scope="thread-scope",
            destination_kind="thread",
            destination_id=chat_id,
            thread_id="omt_root1234",
            root_message_id="omt_root1234",
            transport_metadata={
                "chat_id": chat_id,
                "thread_id": "omt_root1234",
                "root_id": "omt_root1234",
            },
        ),
        content=content,
        attachments=(first.attachment_id, second.attachment_id),
        agent_id="codex",
        outbox_id=outbox_id,
        client_id=f"{outbox_id}-client",
    )
    return parent, first, second


def test_mixed_bundle_sends_two_attachments_then_parent_text_once(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _create_bundle(
                store,
                files,
                outbox_id="mixed-bundle",
                content="bundle caption",
            )
            events: list[tuple[str, str, str]] = []

            async def upload(row):
                item = files.get(row.attachment_id)
                ordinal = int(row.metadata["bundle_ordinal"])
                events.append(("upload", str(ordinal), item.attachment_id))
                return {
                    "remote_id": f"remote_{ordinal}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, uploaded):
                ordinal = int(row.metadata["bundle_ordinal"])
                assert len(row.idempotency_key) <= 50
                assert row.transport_idempotency_key == row.idempotency_key
                assert len(row.text_idempotency_key) <= 50
                assert row.text_idempotency_key != row.idempotency_key
                events.append(("media", str(ordinal), row.idempotency_key))
                return {"data": {"message_id": f"om_media_{ordinal}"}}

            async def send_text(row):
                assert row.content == "bundle caption"
                assert len(row.idempotency_key) <= 50
                assert row.text_idempotency_key == row.idempotency_key
                events.append(("text", row.content, row.idempotency_key))
                return {"data": {"message_id": "om_text_bundle"}}

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
                text_sender=send_text,
            )
            assert await worker.run_once() == 1
            assert [(kind, value) for kind, value, _key in events] == [
                ("upload", "0"),
                ("media", "0"),
                ("upload", "1"),
                ("media", "1"),
                ("text", "bundle caption"),
            ]
            wire_keys = [key for kind, _value, key in events if kind != "upload"]
            assert len(wire_keys) == len(set(wire_keys)) == 3
            durable_parent = await store.get_outbox_item(parent.outbox_id)
            assert durable_parent is not None
            assert durable_parent.state is OutboxState.SENT
            assert durable_parent.remote_delivery_id == "om_text_bundle"
            children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert [child.state for child in children] == [
                MediaDeliveryState.SENT,
                MediaDeliveryState.SENT,
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_bundle_retry_preserves_sent_and_uploaded_checkpoints(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _create_bundle(
                store,
                files,
                outbox_id="checkpoint-bundle",
                content="after files",
            )
            uploads: Counter[str] = Counter()
            sends: Counter[str] = Counter()
            send_keys: dict[str, list[str]] = {}
            text_calls = 0
            text_keys: list[str] = []

            async def upload(row):
                files.get(row.attachment_id)
                uploads[row.media_id] += 1
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, _uploaded):
                sends[row.media_id] += 1
                send_keys.setdefault(row.media_id, []).append(row.idempotency_key)
                if int(row.metadata["bundle_ordinal"]) == 1 and sends[row.media_id] == 1:
                    raise LarkRateLimitError(retry_after=0.05)
                return {"data": {"message_id": f"om_{row.media_id}"}}

            async def send_text(row):
                nonlocal text_calls
                text_calls += 1
                text_keys.append(row.idempotency_key)
                return {"data": {"message_id": "om_checkpoint_text"}}

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
                text_sender=send_text,
            )
            assert await worker.run_once() == 0
            retained = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert [child.state for child in retained] == [
                MediaDeliveryState.SENT,
                MediaDeliveryState.SEND_PENDING,
            ]
            assert text_calls == 0

            await asyncio.sleep(0.06)
            assert await worker.run_once() == 1
            terminal = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert all(child.state is MediaDeliveryState.SENT for child in terminal)
            assert sorted(uploads.values()) == [1, 1]
            assert sorted(sends.values()) == [1, 2]
            assert all(len(set(keys)) == 1 for keys in send_keys.values())
            assert all(len(key) == 36 for keys in send_keys.values() for key in keys)
            assert text_calls == 1
            assert len(text_keys) == 1 and len(text_keys[0]) == 36
        finally:
            await store.close()

    asyncio.run(scenario())


def test_invalid_media_ack_makes_parent_delivery_unknown_and_releases_siblings(
    tmp_path,
):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _create_bundle(
                store,
                files,
                outbox_id="unknown-ack-bundle",
                content="must not be sent",
            )

            async def upload(row):
                files.get(row.attachment_id)
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def invalid_send(_row, _uploaded):
                return {"ok": True, "data": {}}

            async def forbidden_text(_row):
                raise AssertionError("text follows attachment acknowledgements")

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=invalid_send,
                text_sender=forbidden_text,
            )
            assert await worker.run_once() == 0
            durable_parent = await store.get_outbox_item(parent.outbox_id)
            assert durable_parent is not None
            assert durable_parent.state is OutboxState.DELIVERY_UNKNOWN
            children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            by_ordinal = sorted(
                children, key=lambda child: int(child.metadata["bundle_ordinal"])
            )
            assert [child.state for child in by_ordinal] == [
                MediaDeliveryState.SEND_PENDING,
                MediaDeliveryState.UPLOADING,
            ]
            assert all(child.claim_token is None for child in children)
            assert await store.claim_account_outgoing_media(
                "another-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            ) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_all_sent_bundle_reconciles_parent_without_resending_attachments(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _create_bundle(
                store,
                files,
                outbox_id="reconcile-bundle",
                content="",
            )
            claimed = await store.claim_account_outgoing_media(
                "crashed-worker",
                channel="lark",
                bot_id="cli_botaccount1",
                limit=1,
            )
            token = str(claimed[0].outbox_claim_token)
            assert await store.mark_outbox_sending(parent.outbox_id, token)
            for row in claimed:
                assert await store.transition_outgoing_media(
                    row.media_id,
                    MediaDeliveryState.UPLOADED,
                    from_states=(MediaDeliveryState.UPLOADING,),
                    outbox_id=parent.outbox_id,
                    outbox_claim_token=token,
                    remote_id=f"remote_{row.media_id}",
                )
                assert await store.transition_outgoing_media(
                    row.media_id,
                    MediaDeliveryState.SEND_PENDING,
                    from_states=(MediaDeliveryState.UPLOADED,),
                    outbox_id=parent.outbox_id,
                    outbox_claim_token=token,
                )
                assert await store.transition_outgoing_media(
                    row.media_id,
                    MediaDeliveryState.SENT,
                    from_states=(MediaDeliveryState.SEND_PENDING,),
                    outbox_id=parent.outbox_id,
                    outbox_claim_token=token,
                )
            assert await store.finish_outbox_attempt(
                parent.outbox_id,
                token,
                outcome="unknown",
                error="completion acknowledgement lost",
            )
            assert await store.retry_outbox(parent.outbox_id)

            async def forbidden(*_args, **_kwargs):
                raise AssertionError("all-sent reconciliation must not resend")

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=forbidden,
                sender=forbidden,
                text_sender=forbidden,
            )
            assert await worker.run_once() == 1
            durable_parent = await store.get_outbox_item(parent.outbox_id)
            assert durable_parent is not None
            assert durable_parent.state is OutboxState.SENT
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("corruption", ["wrong_account", "missing_child"])
def test_corrupt_claim_or_parent_membership_permanent_fails_without_callbacks(
    tmp_path, corruption
):
    class CorruptStoreView:
        def __init__(self, store: SQLiteStore):
            self.store = store

        def __getattr__(self, name):
            return getattr(self.store, name)

        async def claim_account_outgoing_media(self, *args, **kwargs):
            rows = await self.store.claim_account_outgoing_media(*args, **kwargs)
            if corruption == "wrong_account" and rows:
                rows[0] = replace(rows[0], bot_id="cli_foreign1234")
            return rows

        async def get_outbox_item(self, outbox_id):
            parent = await self.store.get_outbox_item(outbox_id)
            if corruption == "missing_child" and parent is not None:
                return replace(parent, attachments=parent.attachments[:1])
            return parent

    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, _first, _second = await _create_bundle(
                store,
                files,
                outbox_id=f"corrupt-{corruption}",
                content="do not send",
            )
            callback_count = 0

            async def forbidden(*_args, **_kwargs):
                nonlocal callback_count
                callback_count += 1
                raise AssertionError("corrupt bundles must fail before callbacks")

            worker = LarkMediaDeliveryWorker(
                CorruptStoreView(store),
                _profile(tmp_path),
                uploader=forbidden,
                sender=forbidden,
                text_sender=forbidden,
            )
            assert await worker.run_once() == 0
            assert callback_count == 0
            durable_parent = await store.get_outbox_item(parent.outbox_id)
            assert durable_parent is not None
            assert durable_parent.state is OutboxState.FAILED_PERMANENT
            children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert all(child.claim_token is None for child in children)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_every_claimed_bundle_heartbeat_starts_before_slow_first_refresh(tmp_path):
    class SlowFirstRefreshStore:
        def __init__(self, store: SQLiteStore, slow_outbox_id: str):
            self.store = store
            self.slow_outbox_id = slow_outbox_id
            self.events: list[tuple[str, str]] = []

        def __getattr__(self, name):
            return getattr(self.store, name)

        async def list_outgoing_media_for_outbox(self, *, outbox_id, **kwargs):
            self.events.append(("refresh", outbox_id))
            if outbox_id == self.slow_outbox_id:
                await asyncio.sleep(0.18)
            return await self.store.list_outgoing_media_for_outbox(
                outbox_id=outbox_id, **kwargs
            )

        async def renew_canonical_media_lease(
            self, outbox_id, claim_token, **kwargs
        ):
            self.events.append(("renew", outbox_id))
            return await self.store.renew_canonical_media_lease(
                outbox_id, claim_token, **kwargs
            )

    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            first, *_ = await _create_bundle(
                store,
                files,
                outbox_id="slow-first-bundle",
                content="",
            )
            second, *_ = await _create_bundle(
                store,
                files,
                outbox_id="protected-second-bundle",
                content="",
                actor_id="ou_actor5678",
                chat_id="oc_chat5678",
            )
            view = SlowFirstRefreshStore(store, first.outbox_id)

            async def upload(row):
                files.get(row.attachment_id)
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, _uploaded):
                return {"data": {"message_id": f"om_{row.media_id}"}}

            worker = LarkMediaDeliveryWorker(
                view,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
                claim_limit=2,
                lease_seconds=0.12,
            )
            assert await worker.run_once() == 2
            second_renew = view.events.index(("renew", second.outbox_id))
            second_refresh = view.events.index(("refresh", second.outbox_id))
            assert second_renew < second_refresh
            assert (await store.get_outbox_item(first.outbox_id)).state is OutboxState.SENT
            assert (await store.get_outbox_item(second.outbox_id)).state is OutboxState.SENT
        finally:
            await store.close()

    asyncio.run(scenario())


def test_rate_limit_releases_other_claimed_bundles_without_callbacks(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            first, *_ = await _create_bundle(
                store,
                files,
                outbox_id="rate-first-bundle",
                content="",
            )
            second, *_ = await _create_bundle(
                store,
                files,
                outbox_id="rate-second-bundle",
                content="",
                actor_id="ou_actor5678",
                chat_id="oc_chat5678",
            )
            upload_calls: Counter[str] = Counter()
            send_calls: Counter[str] = Counter()

            async def upload(row):
                files.get(row.attachment_id)
                upload_calls[row.outbox_id] += 1
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, _uploaded):
                send_calls[row.outbox_id] += 1
                raise LarkRateLimitError(retry_after=1)

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
                claim_limit=2,
            )
            assert await worker.run_once() == 0
            assert send_calls[first.outbox_id] == 1
            assert send_calls[second.outbox_id] == 0
            assert upload_calls[second.outbox_id] == 0
            assert (await store.get_outbox_item(first.outbox_id)).state is OutboxState.RETRY_WAIT
            assert (await store.get_outbox_item(second.outbox_id)).state is OutboxState.RETRY_WAIT
        finally:
            await store.close()

    asyncio.run(scenario())


def test_text_stage_retry_reuses_key_without_resending_attachments(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, *_ = await _create_bundle(
                store,
                files,
                outbox_id="text-retry-bundle",
                content="caption after attachments",
            )
            uploads = 0
            sends = 0
            text_keys: list[str] = []

            async def upload(row):
                nonlocal uploads
                uploads += 1
                files.get(row.attachment_id)
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, _uploaded):
                nonlocal sends
                sends += 1
                return {"data": {"message_id": f"om_{row.media_id}"}}

            async def send_text(row):
                text_keys.append(row.idempotency_key)
                if len(text_keys) == 1:
                    raise LarkRateLimitError(retry_after=0.05)
                return {"data": {"message_id": "om_text_retry_done"}}

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
                text_sender=send_text,
            )
            assert await worker.run_once() == 0
            children = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert all(child.state is MediaDeliveryState.SENT for child in children)
            await asyncio.sleep(0.06)
            assert await worker.run_once() == 1
            assert uploads == sends == 2
            assert len(text_keys) == 2
            assert text_keys[0] == text_keys[1]
            assert len(text_keys[0]) == 36
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("first_upload", ["exception", "missing_key"])
def test_upload_only_transient_retries_without_sender_uncertainty(
    tmp_path, first_upload
):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        files = AttachmentStore(root)
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            parent, *_ = await _create_bundle(
                store,
                files,
                outbox_id=f"upload-retry-{first_upload}",
                content="",
            )
            upload_attempts = 0
            send_calls = 0

            async def upload(row):
                nonlocal upload_attempts
                upload_attempts += 1
                files.get(row.attachment_id)
                if upload_attempts == 1:
                    if first_upload == "exception":
                        raise RuntimeError("temporary upload transport failure")
                    return {"kind": row.metadata["kind"]}
                return {
                    "remote_id": f"remote_{row.media_id}",
                    "kind": row.metadata["kind"],
                }

            async def send(row, _uploaded):
                nonlocal send_calls
                send_calls += 1
                return {"data": {"message_id": f"om_{row.media_id}"}}

            worker = LarkMediaDeliveryWorker(
                store,
                _profile(tmp_path),
                uploader=upload,
                sender=send,
            )
            assert await worker.run_once() == 0
            retry_parent = await store.get_outbox_item(parent.outbox_id)
            assert retry_parent is not None
            assert retry_parent.state is OutboxState.RETRY_WAIT
            assert send_calls == 0
            retained = await store.list_outgoing_media(
                outbox_id=parent.outbox_id, limit=10
            )
            assert all(child.claim_token is None for child in retained)

            # Generic upload errors use the normal bounded retry delay.
            await asyncio.sleep(1.05)
            assert await worker.run_once() == 1
            assert upload_attempts == 3
            assert send_calls == 2
        finally:
            await store.close()

    asyncio.run(scenario())
