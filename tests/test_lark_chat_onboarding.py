from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.channels.models import InboundEnvelope, ReplyTarget
from src.lark_chat_onboarding import LarkChatOnboardingService
from src.lark_onboarding import (
    LarkOnboardingRetainedError,
    StagedCredentialDisposition,
)
from src.runtime.identity import PrincipalResolution
from src.runtime.models import BotProfileRecord


_ORIGIN_APP = "cli_originbot123456"
_NEW_APP = "cli_newbot1234567890"
_OWNER_OPEN_ID = "ou_owner123456789"
_VERIFICATION_URL = (
    "https://open.feishu.cn/page/cli?user_code=ABCD-1234&lpv=1.0.92"
)


def _envelope() -> InboundEnvelope:
    return InboundEnvelope(
        channel="lark",
        bot_id=_ORIGIN_APP,
        external_user_id=_OWNER_OPEN_ID,
        external_message_id="om_add_bot_command",
        text="新增一个飞书 bot",
        session_id="default",
        agent_id="codex",
        conversation_subject_id="subject-owner",
        conversation_subject_scope="chat-owner",
        conversation_subject_kind="direct",
        principal_id="owner",
        principal_account_id="principal-account-owner",
        destination_kind="chat_id",
        destination_id="oc_owner_chat",
    )


class _Resolver:
    def __init__(self, values=None) -> None:
        self.values = list(values or ())
        self.calls = []

    async def resolve(self, actor):
        self.calls.append(actor)
        value = self.values.pop(0) if self.values else "owner"
        if isinstance(value, BaseException):
            raise value
        if value == "owner":
            return PrincipalResolution(
                actor=actor,
                principal_id="owner",
                principal_account_id="principal-account-owner",
                mapping_revision=1,
                source="configured",
            )
        return PrincipalResolution(actor=actor)


class _AttachmentStore:
    def __init__(self) -> None:
        self.values = []

    async def aput_bytes_idempotent(self, payload, **kwargs):
        stored = SimpleNamespace(
            attachment_id=kwargs["attachment_id"],
            payload=bytes(payload),
            filename=kwargs["filename"],
            mime_type=kwargs["mime_type"],
        )
        self.values.append(stored)
        return stored


class _Store:
    def __init__(self, *, auto_send: bool = True) -> None:
        self.auto_send = auto_send
        self.allowed_to_send: set[str] = set()
        self.outboxes = []
        self.outbox_by_id = {}
        self.attachments = []
        self.profiles = []
        self.owner_accounts = []
        self.create_options = None
        self.create_entered = asyncio.Event()
        self.create_release = asyncio.Event()
        self.block_profile_create = False
        self.return_without_mapping = False
        self.prove_profile_rollback = True
        self.rolled_back_profile = None

    async def register_attachment(self, stored, **kwargs):
        self.attachments.append((stored, kwargs))
        return stored

    async def create_account_outbox(self, **kwargs):
        delivery = dict(kwargs["delivery"])
        outbox_id = str(delivery["delivery_id"])
        existing = self.outbox_by_id.get(outbox_id)
        if existing is not None:
            return existing
        row = SimpleNamespace(
            outbox_id=outbox_id,
            channel=str(kwargs["channel"]),
            bot_id=str(kwargs["bot_id"]),
            external_user_id=str(kwargs["external_user_id"]),
            session_id=str(kwargs["session_id"]),
            reply_target=ReplyTarget.from_dict(delivery["target"]),
            from_user_id=str(delivery["from_user_id"]),
            sender=dict(delivery["sender_account"]),
            state="sent" if self.auto_send else "pending",
            delivery=delivery,
        )
        self.outboxes.append(row)
        self.outbox_by_id[outbox_id] = row
        return row

    async def get_outbox_item(self, outbox_id):
        row = self.outbox_by_id.get(str(outbox_id))
        if row is not None and str(outbox_id) in self.allowed_to_send:
            row.state = "sent"
        return row

    async def list_bot_profiles(self, **_kwargs):
        return list(self.profiles)

    async def resolve_principal_account(
        self,
        *,
        channel,
        bot_id,
        external_user_id,
    ):
        return next(
            (
                account
                for account in self.owner_accounts
                if account.channel == channel
                and account.bot_id == bot_id
                and account.external_user_id == external_user_id
                and account.active
            ),
            None,
        )

    async def create_bot_profile_with_owner_for_onboarding(self, record, **options):
        self.create_entered.set()
        if self.block_profile_create:
            await self.create_release.wait()
        self.create_options = dict(options)
        account = SimpleNamespace(
            principal_account_id="new-bot-owner-account",
            principal_id="owner",
            channel="lark",
            bot_id=record.bot_id,
            external_user_id=options["external_user_id"],
            identifier_kind="open_id",
            mapping_revision=1,
            active=True,
            configured_by=options["configured_by"],
        )
        self.profiles.append(record)
        if self.return_without_mapping:
            return record, SimpleNamespace(
                principal_account_id="missing-owner-account",
                principal_id="owner",
                channel="lark",
                bot_id=record.bot_id,
                external_user_id=options["external_user_id"],
                identifier_kind="open_id",
                mapping_revision=1,
                active=True,
                configured_by=options["configured_by"],
            )
        self.owner_accounts.append(account)
        return record, account

    async def rollback_bot_profile_with_owner_registration(self, record, account):
        self.rolled_back_profile = record
        if not self.prove_profile_rollback:
            return False
        if record not in self.profiles or account not in self.owner_accounts:
            return False
        self.profiles.remove(record)
        self.owner_accounts.remove(account)
        return True


class _Ownership:
    def __init__(self) -> None:
        self.acquired = []
        self.released = []

    def acquire_account(self, *, channel, bot_id):
        token = SimpleNamespace(channel=channel, bot_id=bot_id, held=True)
        self.acquired.append(token)
        return token

    def release_account(self, token):
        token.held = False
        self.released.append(token)


class _Controller:
    def __init__(self, ownership: _Ownership) -> None:
        self.ownership = ownership
        self.handles = []
        self.preflighted = []
        self.activated = []
        self.rolled_back = []
        self.released = []
        self.activation_error = None

    async def prepare(self, profile, *, ownership_handle, preflight):
        assert preflight is False
        assert ownership_handle.channel == "lark"
        assert ownership_handle.bot_id == profile.app_id
        handle = SimpleNamespace(
            profile=profile,
            ownership_handle=ownership_handle,
        )
        self.handles.append(handle)
        return handle

    async def preflight(self, handle):
        self.preflighted.append(handle)
        return handle

    async def activate(self, handle):
        self.activated.append(handle)
        if self.activation_error is not None:
            raise self.activation_error
        return handle

    async def rollback(self, handle, *, release_ownership):
        assert release_ownership is False
        self.rolled_back.append(handle)

    async def release_ownership(self, handle):
        self.released.append(handle)
        self.ownership.release_account(handle.ownership_handle)


class _Stage:
    def __init__(self, root: Path) -> None:
        self.profile_id = "new-bot"
        self.app_id = _NEW_APP
        self.owner_open_id = "ou_newappowner123456"
        self.credential_ref = "keychain:lark/new-bot"
        self.staging_path = root / ".add-new-bot-private"
        self.final_path = root / self.profile_id
        self.state = "staged"
        self.rollback_dispositions = []
        self.runtime_artifact_cleanups = 0
        self.committed = False
        self.publish_calls = 0
        self.reverify_calls = 0
        self.reverify_error = None
        self.block_reverify = False
        self.reverify_started = threading.Event()
        self.reverify_release = threading.Event()
        self.record = BotProfileRecord(
            profile_id=self.profile_id,
            channel="lark",
            bot_id=self.app_id,
            brand="feishu",
            config_dir=str(root / self.profile_id),
            config_dir_identity="100:200",
            cli_version="1.0.92",
            credential_ref=self.credential_ref,
            restart_policy={
                "max_attempts": 8,
                "base_delay": 1,
                "max_delay": 60,
                "bot_open_id": "ou_newbot1234567890",
            },
        )

    @property
    def active_path(self):
        if self.state in {"published", "committed"}:
            return self.final_path
        return self.staging_path

    def publish(self):
        self.publish_calls += 1
        self.state = "published"
        return self.record

    def commit(self):
        self.committed = True
        self.state = "committed"
        return self.record

    def reverify_owner_identity(self):
        assert self.state == "published"
        self.reverify_calls += 1
        self.reverify_started.set()
        if self.block_reverify:
            assert self.reverify_release.wait(timeout=2.0)
        if self.reverify_error is not None:
            raise self.reverify_error
        return self.owner_open_id

    def remove_runtime_artifacts_after_stop(self):
        assert self.state == "published"
        self.runtime_artifact_cleanups += 1

    async def rollback(self, disposition):
        self.rollback_dispositions.append(disposition)
        self.state = "rolled_back"

    def retain(self):
        path = self.active_path
        self.state = "retained"
        return path


def _service(
    tmp_path: Path,
    *,
    store: _Store | None = None,
    resolver: _Resolver | None = None,
    stage_factory=None,
):
    store = store or _Store()
    resolver = resolver or _Resolver()
    ownership = _Ownership()
    controller = _Controller(ownership)
    attachment_store = _AttachmentStore()
    service = LarkChatOnboardingService(
        store=store,
        manager=object(),
        attachment_store=attachment_store,
        account_controller=controller,
        ownership=ownership,
        principal_resolver=resolver,
        config_root=tmp_path,
        delivery_timeout=1.0,
        delivery_poll_interval=0.001,
        stage_factory=stage_factory or (lambda **_kwargs: None),
        qr_renderer=lambda _url: b"png",
    )
    return service, store, resolver, ownership, controller, attachment_store


async def _eventually(predicate, *, attempts: int = 1000) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("condition did not become true")


def test_verification_url_is_sent_before_qr_outbox_exists(tmp_path):
    async def scenario() -> None:
        store = _Store(auto_send=False)
        service, *_rest, attachment_store = _service(tmp_path, store=store)
        delivery = asyncio.create_task(
            service._publish_verification_bundle(
                _envelope(),
                "command-ordering",
                _VERIFICATION_URL,
            )
        )

        await _eventually(lambda: len(store.outboxes) == 1)
        url_row = store.outboxes[0]
        assert url_row.delivery["content"].endswith(_VERIFICATION_URL)
        assert url_row.delivery["attachments"] == []
        assert url_row.bot_id == _ORIGIN_APP
        assert url_row.external_user_id == _OWNER_OPEN_ID
        assert len(attachment_store.values) == 1

        # The QR delivery is not even durable until the URL is confirmed sent.
        await asyncio.sleep(0.01)
        assert len(store.outboxes) == 1
        store.allowed_to_send.add(url_row.outbox_id)

        await _eventually(lambda: len(store.outboxes) == 2)
        qr_row = store.outboxes[1]
        assert qr_row.delivery["content"] == ""
        assert qr_row.delivery["attachments"] == [
            attachment_store.values[0].attachment_id
        ]
        assert qr_row.bot_id == _ORIGIN_APP
        assert qr_row.external_user_id == _OWNER_OPEN_ID
        store.allowed_to_send.add(qr_row.outbox_id)
        await delivery

    asyncio.run(scenario())


def test_happy_path_activates_profile_and_replies_through_origin_bot(tmp_path):
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, store, resolver, ownership, controller, _attachments = _service(
            tmp_path,
            stage_factory=stage_factory,
        )
        envelope = _envelope()
        await service.start(envelope, command_id="command-happy")
        await _eventually(lambda: not service._tasks)

        assert len(resolver.calls) == 3
        assert stage.publish_calls == 1
        assert stage.reverify_calls == 1
        assert stage.committed
        assert store.profiles == [stage.record]
        assert len(store.owner_accounts) == 1
        assert store.owner_accounts[0].external_user_id == stage.owner_open_id
        assert store.create_options == {
            "external_user_id": stage.owner_open_id,
            "source_principal_account_id": envelope.principal_account_id,
            "source_channel": envelope.channel,
            "source_bot_id": envelope.bot_id,
            "source_external_user_id": envelope.external_user_id,
            "source_mapping_revision": 1,
            "configured_by": (
                "lark-chat-onboarding:" + envelope.principal_account_id
            ),
        }
        assert store.rolled_back_profile is None
        assert controller.preflighted == controller.handles
        assert controller.activated == controller.handles
        assert controller.rolled_back == []
        assert controller.released == []
        assert len(ownership.acquired) == 1
        assert ownership.released == []
        assert ownership.acquired[0].held

        success = store.outboxes[-1]
        assert "飞书 Bot 已接入 cow" in success.delivery["content"]
        assert stage.app_id in success.delivery["content"]
        assert success.bot_id == envelope.bot_id
        assert success.bot_id != stage.app_id
        assert success.external_user_id == envelope.external_user_id
        assert success.reply_target.stable_key() == envelope.reply_target.stable_key()
        assert success.from_user_id == envelope.bot_id
        assert success.sender == {"channel": "lark", "bot_id": envelope.bot_id}
        await service.stop()

    asyncio.run(scenario())


def test_revoked_owner_is_rechecked_before_any_cli_staging(tmp_path, caplog):
    secret = "appSecret=must-not-leak"
    resolver = _Resolver([RuntimeError(secret)])
    stage_calls = 0

    async def stage_factory(**_kwargs):
        nonlocal stage_calls
        stage_calls += 1
        raise AssertionError("revoked owner reached credential staging")

    async def scenario() -> None:
        service, store, *_ = _service(
            tmp_path,
            resolver=resolver,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-revoked")
        await _eventually(lambda: not service._tasks)
        assert stage_calls == 0
        assert len(store.outboxes) == 1
        assert secret not in store.outboxes[0].delivery["content"]
        await service.stop()

    asyncio.run(scenario())
    assert secret not in caplog.text


def test_owner_is_rechecked_after_scan_before_profile_publication(tmp_path):
    resolver = _Resolver(["owner", "revoked"])
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            resolver=resolver,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-owner-race")
        await _eventually(lambda: not service._tasks)

        assert stage.publish_calls == 0
        assert stage.rollback_dispositions == [
            StagedCredentialDisposition.UNOWNED
        ]
        assert store.profiles == []
        assert store.owner_accounts == []
        assert controller.handles == []
        assert len(ownership.acquired) == 1
        assert ownership.released == ownership.acquired
        assert store.outboxes[-1].delivery["content"].startswith(
            "飞书 Bot 接入失败："
        )
        await service.stop()

    asyncio.run(scenario())


def test_app_owner_is_reverified_after_preflight_before_store_commit(tmp_path):
    stage = _Stage(tmp_path)
    stage.reverify_error = RuntimeError("owner transferred")

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, store, resolver, ownership, controller, _attachments = _service(
            tmp_path,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-target-owner-race")
        await _eventually(lambda: not service._tasks)

        assert len(resolver.calls) == 2
        assert stage.publish_calls == 1
        assert stage.reverify_calls == 1
        assert store.profiles == []
        assert store.owner_accounts == []
        assert controller.preflighted == controller.handles
        assert controller.activated == []
        assert controller.rolled_back == controller.handles
        assert ownership.released == ownership.acquired
        assert stage.state == "rolled_back"
        assert store.outboxes[-1].delivery["content"].startswith(
            "飞书 Bot 接入失败："
        )
        await service.stop()

    asyncio.run(scenario())


def test_cancellation_drains_owner_reverification_before_cleanup(tmp_path):
    stage = _Stage(tmp_path)
    stage.block_reverify = True

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-cancel-reverify")
        assert await asyncio.to_thread(stage.reverify_started.wait, 1.0)

        stopping = asyncio.create_task(service.stop())
        await asyncio.sleep(0)
        assert not stopping.done()
        assert controller.rolled_back == []
        stage.reverify_release.set()
        await asyncio.wait_for(stopping, timeout=2.0)

        assert store.profiles == []
        assert store.owner_accounts == []
        assert controller.activated == []
        assert controller.rolled_back == controller.handles
        assert controller.released == controller.handles
        assert ownership.released == ownership.acquired
        assert stage.runtime_artifact_cleanups == 1
        assert stage.state == "rolled_back"

    asyncio.run(scenario())


def test_cancellation_after_profile_commit_rolls_back_exact_record(tmp_path):
    store = _Store()
    store.block_profile_create = True
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, _store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            store=store,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-cancel-create")
        await store.create_entered.wait()

        stopping = asyncio.create_task(service.stop())
        await asyncio.sleep(0)
        assert not stopping.done()
        store.create_release.set()
        await asyncio.wait_for(stopping, timeout=2.0)

        assert store.profiles == []
        assert store.rolled_back_profile is stage.record
        assert stage.runtime_artifact_cleanups == 1
        assert stage.rollback_dispositions == [
            StagedCredentialDisposition.UNOWNED
        ]
        assert not stage.committed
        assert controller.activated == []
        assert controller.rolled_back == controller.handles
        assert controller.released == controller.handles
        assert ownership.released == ownership.acquired

    asyncio.run(scenario())


def test_unproven_database_rollback_retains_credentials_and_account_lock(tmp_path):
    store = _Store()
    store.prove_profile_rollback = False
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, _store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            store=store,
            stage_factory=stage_factory,
        )
        controller.activation_error = RuntimeError("activation failed")
        await service.start(_envelope(), command_id="command-retain")
        await _eventually(lambda: not service._tasks)

        assert store.profiles == [stage.record]
        assert len(store.owner_accounts) == 1
        assert store.rolled_back_profile is stage.record
        assert stage.state == "retained"
        assert stage.rollback_dispositions == []
        assert stage.runtime_artifact_cleanups == 1
        assert controller.rolled_back == controller.handles
        assert controller.released == []
        assert len(ownership.acquired) == 1
        assert ownership.released == []
        assert ownership.acquired[0].held
        assert store.outboxes[-1].delivery["content"].startswith(
            "飞书 Bot 接入失败："
        )
        await service.stop()

    asyncio.run(scenario())


def test_no_success_or_activation_without_proven_owner_mapping(tmp_path):
    store = _Store()
    store.return_without_mapping = True
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, _store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            store=store,
            stage_factory=stage_factory,
        )
        await service.start(_envelope(), command_id="command-missing-owner-map")
        await _eventually(lambda: not service._tasks)

        assert store.profiles == [stage.record]
        assert store.owner_accounts == []
        assert controller.activated == []
        assert controller.rolled_back == controller.handles
        assert controller.released == []
        assert ownership.released == []
        assert stage.state == "retained"
        assert not any(
            "飞书 Bot 已接入 cow" in row.delivery["content"]
            for row in store.outboxes
        )
        assert store.outboxes[-1].delivery["content"].startswith(
            "飞书 Bot 接入失败："
        )
        await service.stop()

    asyncio.run(scenario())


def test_published_retention_reports_the_final_recovery_path(tmp_path):
    async def scenario() -> None:
        store = _Store()
        store.prove_profile_rollback = False
        service, _store, _resolver, ownership, _controller, _attachments = _service(
            tmp_path,
            store=store,
        )
        stage = _Stage(tmp_path)
        record = stage.publish()
        account = SimpleNamespace(principal_account_id="new-bot-owner-account")
        store.profiles.append(record)
        store.owner_accounts.append(account)
        token = ownership.acquire_account(channel="lark", bot_id=stage.app_id)

        with pytest.raises(LarkOnboardingRetainedError) as raised:
            await service._compensate(
                staged=stage,
                disposition=StagedCredentialDisposition.UNOWNED,
                account_token=token,
                runtime_handle=None,
                created_bundle=(record, account),
            )

        assert raised.value.path == stage.final_path
        assert stage.state == "retained"
        assert token.held
        assert ownership.released == []

    asyncio.run(scenario())


def test_compensation_recomputes_shared_credential_disposition(tmp_path):
    async def scenario() -> None:
        service, store, _resolver, ownership, _controller, _attachments = _service(
            tmp_path
        )
        stage = _Stage(tmp_path)
        store.profiles.append(
            SimpleNamespace(
                bot_id=stage.app_id,
                credential_ref=stage.credential_ref,
            )
        )
        token = ownership.acquire_account(channel="lark", bot_id=stage.app_id)

        await service._compensate(
            staged=stage,
            disposition=StagedCredentialDisposition.UNOWNED,
            account_token=token,
            runtime_handle=None,
            created_bundle=None,
        )

        assert stage.rollback_dispositions == [
            StagedCredentialDisposition.SHARED
        ]
        assert ownership.released == [token]

    asyncio.run(scenario())


def test_known_duplicate_account_conflict_removes_only_staging_tree(tmp_path):
    stage = _Stage(tmp_path)

    async def stage_factory(*, on_verification_url, **_kwargs):
        await on_verification_url(_VERIFICATION_URL)
        return stage

    async def scenario() -> None:
        service, store, _resolver, ownership, controller, _attachments = _service(
            tmp_path,
            stage_factory=stage_factory,
        )
        store.profiles.append(
            SimpleNamespace(
                bot_id=stage.app_id,
                credential_ref=stage.credential_ref,
            )
        )

        def reject_duplicate(**_kwargs):
            raise RuntimeError("account already owned")

        ownership.acquire_account = reject_duplicate
        await service.start(_envelope(), command_id="command-known-duplicate")
        await _eventually(lambda: not service._tasks)

        assert stage.rollback_dispositions == [
            StagedCredentialDisposition.SHARED
        ]
        assert stage.state == "rolled_back"
        assert controller.handles == []
        assert ownership.released == []
        assert store.outboxes[-1].delivery["content"].startswith(
            "飞书 Bot 接入失败："
        )
        await service.stop()

    asyncio.run(scenario())


def test_config_root_spelling_does_not_follow_symlink(tmp_path):
    real_root = tmp_path / "real"
    real_root.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(real_root, target_is_directory=True)

    service, *_ = _service(linked_root)

    assert service.config_root == linked_root.absolute()
    assert service.config_root != real_root.resolve()


def test_controller_must_share_exact_ownership_authority(tmp_path):
    store = _Store()
    controller_ownership = _Ownership()
    with pytest.raises(TypeError, match="share ownership authority"):
        LarkChatOnboardingService(
            store=store,
            manager=object(),
            attachment_store=_AttachmentStore(),
            account_controller=_Controller(controller_ownership),
            ownership=_Ownership(),
            principal_resolver=_Resolver(),
            config_root=tmp_path,
        )


@pytest.mark.parametrize("value", (0, -1, float("nan"), float("inf")))
def test_delivery_timeout_must_be_positive_and_finite(tmp_path, value):
    store = _Store()
    ownership = _Ownership()
    with pytest.raises(ValueError, match="delivery_timeout"):
        LarkChatOnboardingService(
            store=store,
            manager=object(),
            attachment_store=_AttachmentStore(),
            account_controller=_Controller(ownership),
            ownership=ownership,
            principal_resolver=_Resolver(),
            config_root=tmp_path,
            delivery_timeout=value,
        )
