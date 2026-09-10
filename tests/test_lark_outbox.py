from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.channels.lark import (
    LarkBotProfile,
    LarkDeliveryWorker,
    LarkMediaDeliveryWorker,
    LarkRateLimitError,
)
from src.channels.models import ReplyTarget


def _profile(tmp_path: Path) -> LarkBotProfile:
    config = tmp_path / "config"
    config.mkdir(mode=0o700)
    return LarkBotProfile(
        profile_id="bot-a",
        app_id="cli_botaccount1",
        config_dir=config,
    )


class Store:
    def __init__(self, row):
        self.row = row
        self.claims = []
        self.sent = []
        self.failed = []

    async def claim_account_outbox(self, worker_id, **kwargs):
        self.claims.append((worker_id, kwargs))
        row, self.row = self.row, None
        return [row] if row is not None else []

    async def mark_outbox_sending(self, outbox_id, **kwargs):
        return True

    async def mark_outbox_sent(self, outbox_id, **kwargs):
        self.sent.append((outbox_id, kwargs))
        return True

    async def mark_outbox_failed(self, outbox_id, **kwargs):
        self.failed.append((outbox_id, kwargs))
        return True


class Client:
    def __init__(self):
        self.calls = []

    async def send_text(self, target, content, *, idempotency_key):
        self.calls.append((target, content, idempotency_key))
        return {"data": {"message_id": "om_sent"}}


def test_worker_claims_and_sends_only_its_exact_account(tmp_path):
    async def scenario():
        target = ReplyTarget(
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id="ou_actor1234",
            source_message_id="om_source",
            conversation_subject_id="group-subject",
            destination_kind="thread",
            destination_id="oc_chat1234",
            thread_id="omt_root",
        )
        row = SimpleNamespace(
            outbox_id="outbox-1",
            channel="lark",
            bot_id="cli_botaccount1",
            content="reply",
            reply_target=target,
            client_id="stable-uuid",
            claim_token="claim-1",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        worker = LarkDeliveryWorker(store, _profile(tmp_path), client)
        receipts = await worker.run_once()
        assert len(receipts) == 1 and receipts[0].sent
        assert store.claims[0][1]["channel"] == "lark"
        assert store.claims[0][1]["bot_id"] == "cli_botaccount1"
        assert store.claims[0][1]["limit"] == 1
        assert client.calls[0][0].destination_id == "oc_chat1234"
        assert client.calls[0][0].thread_id == "omt_root"
        assert client.calls[0][2] == "stable-uuid"
        assert store.sent and not store.failed

    asyncio.run(scenario())


def test_text_worker_rejects_target_actor_conflicting_with_durable_row(tmp_path):
    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-actor-conflict",
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id="ou_durable1234",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_target1234",
                destination_kind="open_id",
                destination_id="ou_target1234",
            ),
            claim_token="claim-actor-conflict",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        receipt = (
            await LarkDeliveryWorker(store, _profile(tmp_path), client).run_once()
        )[0]
        assert not receipt.sent and not receipt.retryable
        assert receipt.error_code == "invalid_destination"
        assert client.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "transport_metadata",
    [
        {
            "chat_id": "oc_chat1234",
            "thread_id": "omt_other1234",
            "root_id": "om_root1234",
        },
        {
            "chat_id": "oc_chat1234",
            "thread_id": "omt_thread1234",
            "root_id": "om_other1234",
        },
    ],
)
def test_text_worker_rejects_thread_or_root_metadata_conflicts(
    tmp_path, transport_metadata
):
    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-thread-conflict",
            channel="lark",
            bot_id="cli_botaccount1",
            external_user_id="ou_actor1234",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                source_message_id="om_source1234",
                destination_kind="thread",
                destination_id="oc_chat1234",
                thread_id="omt_thread1234",
                root_message_id="om_root1234",
                transport_metadata=transport_metadata,
            ),
            claim_token="claim-thread-conflict",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        receipt = (
            await LarkDeliveryWorker(store, _profile(tmp_path), client).run_once()
        )[0]
        assert not receipt.sent and not receipt.retryable
        assert receipt.error_code == "invalid_destination"
        assert client.calls == []

    asyncio.run(scenario())


def test_wrong_account_row_is_permanent_and_never_sent(tmp_path):
    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-foreign",
            channel="lark",
            bot_id="cli_otheraccount",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_otheraccount",
                external_user_id="ou_actor1234",
            ),
            claim_token="claim-2",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        worker = LarkDeliveryWorker(store, _profile(tmp_path), client)
        receipts = await worker.run_once()
        assert receipts[0].sent is False
        assert receipts[0].retryable is False
        assert receipts[0].error_code == "wrong_account"
        assert client.calls == []
        assert store.failed[0][1]["permanent"] is True

    asyncio.run(scenario())


def test_worker_rejects_reply_target_that_disagrees_with_claimed_account(tmp_path):
    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-target-foreign",
            channel="lark",
            bot_id="cli_botaccount1",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_otheraccount",
                external_user_id="ou_actor1234",
                destination_kind="open_id",
                destination_id="ou_actor1234",
            ),
            claim_token="claim-target-foreign",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        receipt = (await LarkDeliveryWorker(store, _profile(tmp_path), client).run_once())[0]
        assert not receipt.sent and not receipt.retryable
        assert receipt.error_code == "invalid_destination"
        assert client.calls == []

    asyncio.run(scenario())


def test_worker_rejects_direct_destination_that_disagrees_with_actor(tmp_path):
    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-redirect",
            channel="lark",
            bot_id="cli_botaccount1",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                destination_kind="open_id",
                destination_id="ou_attacker1234",
            ),
            claim_token="claim-redirect",
            attempts=1,
        )
        store = Store(row)
        client = Client()
        receipt = (await LarkDeliveryWorker(store, _profile(tmp_path), client).run_once())[0]
        assert not receipt.sent and not receipt.retryable
        assert receipt.error_code == "invalid_destination"
        assert client.calls == []

    asyncio.run(scenario())


def test_worker_does_not_mark_send_without_valid_message_id_ack(tmp_path):
    class MissingAckClient(Client):
        async def send_text(self, target, content, *, idempotency_key):
            self.calls.append((target, content, idempotency_key))
            return {"ok": True, "data": {}}

    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-missing-ack",
            channel="lark",
            bot_id="cli_botaccount1",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                destination_kind="open_id",
                destination_id="ou_actor1234",
            ),
            claim_token="claim-missing-ack",
            attempts=1,
        )
        store = Store(row)
        receipt = (
            await LarkDeliveryWorker(
                store, _profile(tmp_path), MissingAckClient()
            ).run_once()
        )[0]
        assert not receipt.sent and receipt.retryable
        assert receipt.error_code == "invalid_acknowledgement"
        assert not store.sent and store.failed

    asyncio.run(scenario())


def test_text_transport_exception_is_redacted_without_traceback(
    tmp_path, caplog
):
    class SecretClient(Client):
        async def send_text(self, *_args, **_kwargs):
            raise RuntimeError(
                "app_secret=very-secret https://example.invalid/path?token=very-secret"
            )

    async def scenario():
        row = SimpleNamespace(
            outbox_id="outbox-secret-error",
            channel="lark",
            bot_id="cli_botaccount1",
            content="reply",
            reply_target=ReplyTarget(
                channel="lark",
                bot_id="cli_botaccount1",
                external_user_id="ou_actor1234",
                destination_kind="open_id",
                destination_id="ou_actor1234",
            ),
            claim_token="claim-secret-error",
            attempts=1,
        )
        caplog.set_level(logging.WARNING, logger="src.channels.lark")
        receipt = (
            await LarkDeliveryWorker(
                Store(row), _profile(tmp_path), SecretClient()
            ).run_once()
        )[0]
        assert not receipt.sent and receipt.retryable

    asyncio.run(scenario())
    assert "very-secret" not in caplog.text
    assert "<redacted>" in caplog.text
    assert "<redacted-url>" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_media_worker_honors_claim_state_and_durable_checkpoints(tmp_path):
    class MediaStore:
        def __init__(self):
            self.claimed = False
            self.transitions = []

        async def claim_account_outgoing_media(self, worker_id, **kwargs):
            assert kwargs["channel"] == "lark"
            assert kwargs["bot_id"] == "cli_botaccount1"
            if self.claimed:
                return []
            self.claimed = True
            return [
                SimpleNamespace(
                    media_id="media-1",
                    attachment_id="attachment-1",
                    channel="lark",
                    bot_id="cli_botaccount1",
                    state="uploading",
                    claim_token="media-claim-1",
                    idempotency_key="media-idempotency-1",
                    metadata={"kind": "image"},
                )
            ]

        async def transition_outgoing_media(self, media_id, state, **kwargs):
            self.transitions.append((media_id, state, kwargs))
            return True

        async def renew_outgoing_media_lease(self, *_args, **_kwargs):
            return True

    async def scenario():
        store = MediaStore()
        uploads = []
        sends = []

        async def upload(row):
            uploads.append(row.media_id)
            return {"remote_id": "img_uploaded1234", "kind": "image"}

        async def send(row, uploaded):
            sends.append((row.media_id, uploaded["remote_id"]))
            return {"data": {"message_id": "om_media_sent"}}

        worker = LarkMediaDeliveryWorker(
            store,
            _profile(tmp_path),
            uploader=upload,
            sender=send,
        )
        assert await worker.run_once() == 1
        assert uploads == ["media-1"]
        assert sends == [("media-1", "img_uploaded1234")]
        assert [state for _media_id, state, _kwargs in store.transitions] == [
            "uploaded",
            "send_pending",
            "sent",
        ]
        assert store.transitions[0][2]["from_states"] == ("uploading",)

    asyncio.run(scenario())


def test_media_worker_reuses_upload_checkpoint_after_rate_limit(tmp_path):
    class MediaStore:
        def __init__(self):
            self.claimed = False
            self.transitions = []
            self.retried = []

        async def claim_account_outgoing_media(self, *_args, **_kwargs):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "media_id": "media-retry",
                    "attachment_id": "attachment-retry",
                    "channel": "lark",
                    "bot_id": "cli_botaccount1",
                    "state": "uploaded",
                    "remote_id": "file_uploaded1234",
                    "claim_token": "media-claim-retry",
                    "metadata": {"kind": "file"},
                }
            ]

        async def transition_outgoing_media(self, media_id, state, **kwargs):
            self.transitions.append((media_id, state, kwargs))
            return True

        async def retry_outgoing_media(self, media_id):
            self.retried.append(media_id)
            return True

    async def scenario():
        store = MediaStore()
        uploads = []

        async def upload(_row):
            uploads.append(True)
            raise AssertionError("uploaded checkpoint must not be uploaded again")

        async def send(_row, uploaded):
            assert uploaded["remote_id"] == "file_uploaded1234"
            raise LarkRateLimitError(retry_after=3)

        worker = LarkMediaDeliveryWorker(
            store,
            _profile(tmp_path),
            uploader=upload,
            sender=send,
        )
        assert await worker.run_once() == 0
        assert uploads == []
        assert [state for _media_id, state, _kwargs in store.transitions] == [
            "send_pending",
            "failed",
        ]
        assert store.retried == ["media-retry"]
        assert await worker.run_once() == 0

    asyncio.run(scenario())
