"""Owner-only, chat-driven Lark bot onboarding orchestration.

The command router authenticates the initiating principal, while this service
defends that boundary again and owns the asynchronous credential/runtime/store
transaction.  Every user-facing update is a durable outbox delivery addressed
to the exact bot account and direct conversation that initiated the request.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import io
import logging
import math
import threading
import uuid
from pathlib import Path
from typing import Any, Mapping

import qrcode

from src.channels.models import InboundEnvelope, ReplyTarget, UserDelivery
from src.channels.lark import LarkBotProfile
from src.lark_onboarding import (
    LarkOnboardingRetainedError,
    StagedCredentialDisposition,
    public_onboarding_error,
    stage_lark_qr_onboarding,
)
from src.runtime.identity import AuthenticatedActor


logger = logging.getLogger(__name__)

_URL_MESSAGE = (
    "请使用要授权为 cow 全局 owner 的飞书账号扫描二维码完成新 Bot 配置；"
    "该账号将获得 owner 权限，请勿转发二维码或链接。也可打开以下官方链接：\n\n{url}"
)
_BUSY_MESSAGE = "已有一个飞书 Bot 正在配置，请先完成或等待当前配置结束。"
_SUCCESS_MESSAGE = (
    "飞书 Bot 已接入 cow，扫码账号已自动映射为 owner。\n\n"
    "Profile: `{profile_id}`\nApp ID: `{app_id}`"
)
_FAILURE_MESSAGE = "飞书 Bot 接入失败：{error}"
_FAILED_DELIVERY_STATES = frozenset({"failed_permanent", "delivery_unknown"})


class LarkChatOnboardingError(RuntimeError):
    """A chat onboarding request could not cross a safe transaction boundary."""


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _render_qr_png(url: str) -> bytes:
    code = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=8,
        border=4,
    )
    code.add_data(url)
    code.make(fit=True)
    image = code.make_image(fill_color="black", back_color="white")
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


class LarkChatOnboardingService:
    """Run at most one live QR credential transaction for all Lark gateways."""

    def __init__(
        self,
        *,
        store: Any,
        manager: Any,
        attachment_store: Any,
        account_controller: Any,
        ownership: Any,
        principal_resolver: Any,
        config_root: Path,
        binary: str = "lark-cli",
        timeout: float = 600.0,
        delivery_timeout: float = 30.0,
        delivery_poll_interval: float = 0.05,
        stage_factory: Any = stage_lark_qr_onboarding,
        qr_renderer: Any = _render_qr_png,
    ) -> None:
        self.store = store
        self.manager = manager
        self.attachment_store = attachment_store
        self.account_controller = account_controller
        if getattr(account_controller, "ownership", None) is not ownership:
            raise TypeError(
                "account_controller and onboarding must share ownership authority"
            )
        self.ownership = ownership
        resolver = getattr(principal_resolver, "resolve", principal_resolver)
        if not callable(resolver):
            raise TypeError("principal_resolver must resolve authenticated actors")
        self.principal_resolver = principal_resolver
        # The staging core must see and reject a symlink at the configured root.
        # Resolving it here would silently turn that symlink into a trusted path.
        self.config_root = Path(config_root).expanduser().absolute()
        self.binary = str(binary or "lark-cli")
        self.timeout = self._positive_timeout(timeout, "timeout")
        self.delivery_timeout = self._positive_timeout(
            delivery_timeout,
            "delivery_timeout",
        )
        self.delivery_poll_interval = self._positive_timeout(
            delivery_poll_interval,
            "delivery_poll_interval",
        )
        self.stage_factory = stage_factory
        self.qr_renderer = qr_renderer
        self._lock = asyncio.Lock()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._initial_results: dict[str, asyncio.Future[None]] = {}
        self._active_command_id = ""
        self._closed = False

    @staticmethod
    def _positive_timeout(value: Any, name: str) -> float:
        try:
            normalized = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be positive and finite") from exc
        if not math.isfinite(normalized) or normalized <= 0:
            raise ValueError(f"{name} must be positive and finite")
        return normalized

    @staticmethod
    def _validate_request(envelope: InboundEnvelope, command_id: str) -> str:
        identity = str(command_id or "").strip()
        if not identity:
            raise LarkChatOnboardingError("command identity is required")
        if (
            envelope.channel != "lark"
            or envelope.principal_id != "owner"
            or not envelope.principal_account_id
            or envelope.conversation_subject_kind != "direct"
        ):
            raise LarkChatOnboardingError("owner direct-chat authority is required")
        target = envelope.reply_target
        if (
            target.channel != "lark"
            or target.bot_id != envelope.bot_id
            or target.external_user_id != envelope.external_user_id
        ):
            raise LarkChatOnboardingError("onboarding reply target is invalid")
        return identity

    async def _require_current_owner(self, envelope: InboundEnvelope) -> Any:
        """Re-resolve the authenticated account immediately before mutation."""

        actor = AuthenticatedActor(
            channel="lark",
            bot_id=envelope.bot_id,
            external_user_id=envelope.external_user_id,
        )
        resolver = getattr(
            self.principal_resolver,
            "resolve",
            self.principal_resolver,
        )
        resolution = await _maybe_await(resolver(actor))
        resolved_actor = _field(resolution, "actor")
        if resolved_actor is not None and (
            str(_field(resolved_actor, "channel", "")) != actor.channel
            or str(_field(resolved_actor, "bot_id", "")) != actor.bot_id
            or str(_field(resolved_actor, "external_user_id", ""))
            != actor.external_user_id
        ):
            raise LarkChatOnboardingError(
                "current owner resolution belongs to another account"
            )
        if (
            str(_field(resolution, "principal_id", "") or "") != "owner"
            or str(_field(resolution, "principal_account_id", "") or "")
            != envelope.principal_account_id
        ):
            raise LarkChatOnboardingError(
                "owner authority changed before Lark onboarding"
            )
        try:
            mapping_revision = int(_field(resolution, "mapping_revision"))
        except (TypeError, ValueError) as exc:
            raise LarkChatOnboardingError(
                "current owner mapping revision is invalid"
            ) from exc
        if mapping_revision <= 0:
            raise LarkChatOnboardingError(
                "current owner mapping revision is invalid"
            )
        return resolution

    @staticmethod
    def _delivery_identity(command_id: str, kind: str) -> tuple[str, str]:
        delivery_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"codex-lark-onboarding:{command_id}:{kind}",
            )
        )
        client_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"codex-lark-onboarding-wire:{command_id}:{kind}",
            )
        )
        return delivery_id, client_id

    async def _enqueue(
        self,
        envelope: InboundEnvelope,
        command_id: str,
        kind: str,
        content: str,
        *,
        attachments: tuple[Any, ...] = (),
    ) -> Any:
        delivery_id, client_id = self._delivery_identity(command_id, kind)
        target = envelope.reply_target
        delivery = UserDelivery(
            delivery_id=delivery_id,
            target=target,
            from_user_id=envelope.bot_id,
            content=str(content),
            client_id=client_id,
            idempotency_key=client_id,
            sender_account={"channel": "lark", "bot_id": envelope.bot_id},
            transport_metadata=dict(target.transport_metadata),
            attachments=attachments,
        )
        creator = getattr(self.store, "create_account_outbox", None) or getattr(
            self.store,
            "enqueue_account_outbox",
            None,
        )
        if creator is None:
            raise LarkChatOnboardingError(
                "runtime store cannot create an account-local outbox"
            )
        return await _maybe_await(
            creator(
                delivery=delivery.to_dict(),
                channel="lark",
                bot_id=envelope.bot_id,
                external_user_id=envelope.external_user_id,
                session_id=envelope.session_id,
                agent_id=envelope.agent_id or "codex",
                foreground=True,
                bypass_channel_reply_scope=True,
            )
        )

    async def _wait_until_sent(
        self,
        envelope: InboundEnvelope,
        delivery: Any,
    ) -> None:
        """Wait for one exact account-local delivery before creating its successor."""

        outbox_id = str(
            _field(delivery, "outbox_id", "")
            or _field(delivery, "delivery_id", "")
            or ""
        )
        getter = getattr(self.store, "get_outbox_item", None) or getattr(
            self.store,
            "get_user_outbox_item",
            None,
        )
        if not outbox_id or getter is None:
            raise LarkChatOnboardingError(
                "runtime store cannot observe onboarding delivery"
            )
        deadline = asyncio.get_running_loop().time() + self.delivery_timeout
        while True:
            current = await _maybe_await(getter(outbox_id))
            current_id = str(
                _field(current, "outbox_id", "")
                or _field(current, "delivery_id", "")
                or ""
            )
            if current is None or current_id != outbox_id:
                raise LarkChatOnboardingError(
                    "onboarding delivery identity changed"
                )
            if (
                str(_field(current, "channel", "") or "") != "lark"
                or str(_field(current, "bot_id", "") or "") != envelope.bot_id
                or str(_field(current, "external_user_id", "") or "")
                != envelope.external_user_id
                or str(_field(current, "session_id", "default") or "default")
                != (envelope.session_id or "default")
            ):
                raise LarkChatOnboardingError(
                    "onboarding delivery escaped its source account"
                )
            try:
                current_target = ReplyTarget.from_dict(
                    _field(current, "reply_target")
                )
            except (TypeError, ValueError) as exc:
                raise LarkChatOnboardingError(
                    "onboarding delivery has an invalid reply target"
                ) from exc
            if current_target.stable_key() != envelope.reply_target.stable_key():
                raise LarkChatOnboardingError(
                    "onboarding delivery changed its source reply target"
                )
            sender = _field(current, "sender", {})
            if (
                str(_field(current, "from_user_id", "") or "")
                != envelope.bot_id
                or not isinstance(sender, Mapping)
                or str(sender.get("channel") or "") != "lark"
                or str(sender.get("bot_id") or "") != envelope.bot_id
            ):
                raise LarkChatOnboardingError(
                    "onboarding delivery changed its source sender account"
                )
            raw_state = _field(current, "state", "")
            state = str(getattr(raw_state, "value", raw_state) or "").lower()
            if state == "sent":
                return
            if state in _FAILED_DELIVERY_STATES:
                raise LarkChatOnboardingError(
                    "onboarding delivery reached an unsafe terminal state"
                )
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise LarkChatOnboardingError(
                    "onboarding delivery did not complete in time"
                )
            await asyncio.sleep(min(self.delivery_poll_interval, remaining))

    async def _publish_verification_bundle(
        self,
        envelope: InboundEnvelope,
        command_id: str,
        url: str,
    ) -> None:
        qr_bytes = await asyncio.to_thread(self.qr_renderer, url)
        attachment_id = "lark-onboarding-qr-" + hashlib.sha256(
            f"{command_id}\x1f{url}".encode("utf-8")
        ).hexdigest()[:40]
        stored = await self.attachment_store.aput_bytes_idempotent(
            qr_bytes,
            attachment_id=attachment_id,
            filename="lark-onboarding-qr.png",
            mime_type="image/png",
        )
        register = getattr(self.store, "register_attachment", None) or getattr(
            self.store,
            "add_attachment",
            None,
        )
        if register is None:
            raise LarkChatOnboardingError(
                "runtime store cannot register the onboarding QR code"
            )
        await _maybe_await(
            register(
                stored,
                kind="image",
                metadata={
                    "owner_agent_id": envelope.agent_id or "codex",
                    "channel": "lark",
                    "bot_id": envelope.bot_id,
                    "external_user_id": envelope.external_user_id,
                    "session_id": envelope.session_id,
                    "source_message_id": envelope.external_message_id,
                    "purpose": "lark_bot_onboarding_qr",
                },
            )
        )
        # The QR outbox does not exist until the URL has a durable SENT
        # checkpoint.  This orders the two different Lark delivery workers
        # without changing ordinary mixed-media bundle semantics.
        url_delivery = await self._enqueue(
            envelope,
            command_id,
            "verification-url",
            _URL_MESSAGE.format(url=url),
        )
        await self._wait_until_sent(envelope, url_delivery)
        qr_delivery = await self._enqueue(
            envelope,
            command_id,
            "verification-qr",
            "",
            attachments=(attachment_id,),
        )
        await self._wait_until_sent(envelope, qr_delivery)

    async def _credential_disposition(self, staged: Any) -> StagedCredentialDisposition:
        lister = getattr(self.store, "list_bot_profiles", None)
        if lister is None:
            return StagedCredentialDisposition.UNKNOWN
        try:
            profiles = await _maybe_await(
                lister(channel="lark", include_removed=True)
            )
        except BaseException:
            return StagedCredentialDisposition.UNKNOWN
        for profile in profiles or ():
            if (
                str(_field(profile, "bot_id", "")) == staged.app_id
                or str(_field(profile, "credential_ref", ""))
                == staged.credential_ref
            ):
                return StagedCredentialDisposition.SHARED
        return StagedCredentialDisposition.UNOWNED

    @staticmethod
    async def _retain_staged(staged: Any) -> Path:
        retained_path = Path(staged.active_path)
        if str(getattr(staged, "state", "")) in {
            "committed",
            "rolled_back",
            "retained",
        }:
            return retained_path
        result = staged.retain()
        return Path(result) if result is not None else retained_path

    async def _compensate(
        self,
        *,
        staged: Any,
        disposition: StagedCredentialDisposition,
        account_token: Any,
        runtime_handle: Any,
        created_bundle: Any,
    ) -> None:
        """Undo in strict resource->DB->credential->account-lock order."""

        if staged is None:
            return
        detached_handle = None
        try:
            if runtime_handle is not None:
                await self.account_controller.rollback(
                    runtime_handle,
                    release_ownership=False,
                )
                detached_handle = runtime_handle

            # An activated CLI may leave its exact app-local bus socket.  The
            # core validates/removes only that runtime artifact after the
            # controller has proven the process stopped.
            cleanup_runtime = getattr(
                staged,
                "remove_runtime_artifacts_after_stop",
                None,
            )
            if runtime_handle is not None:
                if not callable(cleanup_runtime):
                    raise LarkChatOnboardingError(
                        "staged onboarding lacks stopped-runtime cleanup"
                    )
                await _maybe_await(cleanup_runtime())

            if created_bundle is not None:
                rollback_profile = getattr(
                    self.store,
                    "rollback_bot_profile_with_owner_registration",
                    None,
                )
                if rollback_profile is None or not bool(
                    await _maybe_await(rollback_profile(*created_bundle))
                ):
                    raise LarkChatOnboardingError(
                        "durable Lark profile/owner rollback was not proven"
                    )

            # Re-read after the exact candidate row is removed.  A duplicate
            # profile/credential that appeared during the transaction must
            # conservatively prevent deletion of a shared external secret.
            current_disposition = await self._credential_disposition(staged)
            if (
                StagedCredentialDisposition.UNKNOWN
                in {disposition, current_disposition}
            ):
                disposition = StagedCredentialDisposition.UNKNOWN
            elif (
                StagedCredentialDisposition.SHARED
                in {disposition, current_disposition}
            ):
                disposition = StagedCredentialDisposition.SHARED
            else:
                disposition = StagedCredentialDisposition.UNOWNED
            await staged.rollback(disposition)
        except BaseException as exc:
            retained_path = (
                exc.path
                if isinstance(exc, LarkOnboardingRetainedError)
                else Path(staged.active_path)
            )
            if not isinstance(exc, LarkOnboardingRetainedError):
                try:
                    retained_path = await self._retain_staged(staged)
                except BaseException:
                    pass
            raise LarkOnboardingRetainedError(retained_path) from exc

        if detached_handle is not None:
            await self.account_controller.release_ownership(detached_handle)
        elif account_token is not None:
            self.ownership.release_account(account_token)

    @staticmethod
    async def _settle_critical_operation(operation: Any) -> tuple[Any, bool]:
        """Drain one fenced operation even if the parent is cancelled."""

        task = asyncio.create_task(_maybe_await(operation))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        # An operation failure takes precedence over concurrent cancellation.
        # For a store commit, this also retains the exact returned bundle for
        # compensation; for a CLI read, it fences the one-shot process group
        # before credential cleanup can begin.
        return task.result(), cancelled

    @staticmethod
    def _validate_created_bundle(
        bundle: Any,
        *,
        expected_record: Any,
        owner_open_id: str,
        configured_by: str,
    ) -> tuple[Any, Any]:
        """Validate the store's proof before exposing the new account."""

        if not isinstance(bundle, tuple) or len(bundle) != 2:
            raise LarkChatOnboardingError(
                "runtime store did not return an owner registration bundle"
            )
        profile, account = bundle
        expected_profile = LarkBotProfile.from_value(expected_record)
        observed_profile = LarkBotProfile.from_value(profile)
        if observed_profile != expected_profile:
            raise LarkChatOnboardingError(
                "runtime store returned another Lark profile"
            )
        for name in ("credential_ref", "config_dir_identity"):
            if str(_field(profile, name, "") or "") != str(
                _field(expected_record, name, "") or ""
            ):
                raise LarkChatOnboardingError(
                    "runtime store returned another Lark credential identity"
                )
        try:
            mapping_revision = int(_field(account, "mapping_revision"))
        except (TypeError, ValueError) as exc:
            raise LarkChatOnboardingError(
                "runtime store returned an invalid owner mapping revision"
            ) from exc
        if (
            not str(_field(account, "principal_account_id", "") or "")
            or str(_field(account, "principal_id", "") or "") != "owner"
            or str(_field(account, "channel", "") or "") != "lark"
            or str(_field(account, "bot_id", "") or "")
            != expected_profile.app_id
            or str(_field(account, "external_user_id", "") or "")
            != owner_open_id
            or str(_field(account, "identifier_kind", "") or "") != "open_id"
            or mapping_revision != 1
            or not bool(_field(account, "active", False))
            or not bool(_field(account, "principal_enabled", True))
            or str(_field(account, "configured_by", "") or "") != configured_by
        ):
            raise LarkChatOnboardingError(
                "runtime store returned an invalid owner registration"
            )
        return profile, account

    async def _run_transaction(
        self,
        envelope: InboundEnvelope,
        *,
        command_id: str,
        profile_id: str,
        cancel_event: threading.Event,
        initial_result: asyncio.Future[None],
    ) -> tuple[str, str]:
        staged = None
        account_token = None
        runtime_handle = None
        created_bundle = None
        disposition = StagedCredentialDisposition.UNKNOWN
        committed = False
        try:
            await self._require_current_owner(envelope)

            async def publish_verification(url: str) -> None:
                await self._publish_verification_bundle(
                    envelope,
                    command_id,
                    url,
                )
                if not initial_result.done():
                    initial_result.set_result(None)

            staged = await self.stage_factory(
                config_root=self.config_root,
                on_verification_url=publish_verification,
                profile_name=profile_id or None,
                binary=self.binary,
                timeout=self.timeout,
                cancel_event=cancel_event,
            )
            disposition = await self._credential_disposition(staged)
            try:
                account_token = self.ownership.acquire_account(
                    channel="lark",
                    bot_id=staged.app_id,
                )
            except BaseException:
                # A conflicting owner makes an otherwise-unowned external
                # credential unknowable.  A profile already observed in the
                # live store is stronger evidence: preserve SHARED so cleanup
                # removes only this isolated staging tree and never the
                # existing app's global credential.
                if disposition is not StagedCredentialDisposition.SHARED:
                    disposition = StagedCredentialDisposition.UNKNOWN
                raise

            # A mapping may be revoked while its user is scanning.  Recheck
            # under the new app's account lock before publishing credentials
            # or registering a durable profile.
            await self._require_current_owner(envelope)

            record = staged.publish()
            runtime_handle = await self.account_controller.prepare(
                LarkBotProfile.from_value(record),
                ownership_handle=account_token,
                preflight=False,
            )
            await self.account_controller.preflight(runtime_handle)

            reverify_owner = getattr(staged, "reverify_owner_identity", None)
            if not callable(reverify_owner):
                raise LarkChatOnboardingError(
                    "staged onboarding lacks app-owner reverification"
                )
            reverified_owner, cancelled_during_reverification = (
                await self._settle_critical_operation(
                    asyncio.to_thread(reverify_owner)
                )
            )
            if (
                str(reverified_owner or "") != staged.owner_open_id
                or cancelled_during_reverification
            ):
                if cancelled_during_reverification:
                    raise asyncio.CancelledError
                raise LarkChatOnboardingError(
                    "Lark app owner identity changed before registration"
                )

            # Close the remaining source-authority window as far as possible;
            # the SQLite operation below repeats this exact snapshot check in
            # the same transaction that creates the target mapping.
            source_owner = await self._require_current_owner(envelope)

            creator = getattr(
                self.store,
                "create_bot_profile_with_owner_for_onboarding",
                None,
            )
            if creator is None:
                raise LarkChatOnboardingError(
                    "runtime store lacks strict owner onboarding registration"
                )
            configured_by = (
                "lark-chat-onboarding:" + envelope.principal_account_id
            )
            created_bundle, cancelled_during_create = (
                await self._settle_critical_operation(
                    creator(
                        record,
                        external_user_id=staged.owner_open_id,
                        source_principal_account_id=str(
                            _field(source_owner, "principal_account_id", "") or ""
                        ),
                        source_channel=envelope.channel,
                        source_bot_id=envelope.bot_id,
                        source_external_user_id=envelope.external_user_id,
                        source_mapping_revision=int(
                            _field(source_owner, "mapping_revision")
                        ),
                        configured_by=configured_by,
                    )
                )
            )
            if cancelled_during_create:
                raise asyncio.CancelledError
            self._validate_created_bundle(
                created_bundle,
                expected_record=record,
                owner_open_id=staged.owner_open_id,
                configured_by=configured_by,
            )
            mapping_getter = getattr(
                self.store,
                "resolve_principal_account",
                None,
            )
            if mapping_getter is None:
                raise LarkChatOnboardingError(
                    "runtime store cannot verify the new owner mapping"
                )
            persisted_account = await _maybe_await(
                mapping_getter(
                    channel="lark",
                    bot_id=staged.app_id,
                    external_user_id=staged.owner_open_id,
                )
            )
            self._validate_created_bundle(
                (created_bundle[0], persisted_account),
                expected_record=record,
                owner_open_id=staged.owner_open_id,
                configured_by=configured_by,
            )
            if str(
                _field(persisted_account, "principal_account_id", "") or ""
            ) != str(
                _field(created_bundle[1], "principal_account_id", "") or ""
            ):
                raise LarkChatOnboardingError(
                    "runtime store owner mapping identity changed"
                )
            await self.account_controller.activate(runtime_handle)
            staged.commit()
            committed = True
            return staged.profile_id, staged.app_id
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                cancel_event.set()
            if not committed and staged is not None:
                cleanup = asyncio.create_task(
                    self._compensate(
                        staged=staged,
                        disposition=disposition,
                        account_token=account_token,
                        runtime_handle=runtime_handle,
                        created_bundle=created_bundle,
                    ),
                    name=f"lark-onboarding-compensate:{command_id}",
                )
                cancelled = isinstance(exc, asyncio.CancelledError)
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        cancelled = True
                cleanup.result()
                if cancelled:
                    raise asyncio.CancelledError from exc
            raise

    async def _run(
        self,
        envelope: InboundEnvelope,
        *,
        command_id: str,
        profile_id: str,
        cancel_event: threading.Event,
        initial_result: asyncio.Future[None],
    ) -> None:
        try:
            created_profile, app_id = await self._run_transaction(
                envelope,
                command_id=command_id,
                profile_id=profile_id,
                cancel_event=cancel_event,
                initial_result=initial_result,
            )
        except asyncio.CancelledError:
            cancel_event.set()
            if not initial_result.done():
                initial_result.cancel()
            raise
        except BaseException as exc:
            safe = public_onboarding_error(exc)
            try:
                await self._enqueue(
                    envelope,
                    command_id,
                    "failure",
                    _FAILURE_MESSAGE.format(error=safe),
                )
            except BaseException as delivery_exc:
                logger.error(
                    "could not persist Lark onboarding failure notification (%s)",
                    type(delivery_exc).__name__,
                )
                if not initial_result.done():
                    initial_result.set_exception(
                        LarkChatOnboardingError(
                            "could not persist initial onboarding response"
                        )
                    )
            else:
                if not initial_result.done():
                    initial_result.set_result(None)
            logger.error(
                "Lark chat onboarding failed (%s)",
                type(exc).__name__,
            )
            return

        try:
            await self._enqueue(
                envelope,
                command_id,
                "success",
                _SUCCESS_MESSAGE.format(
                    profile_id=created_profile,
                    app_id=app_id,
                ),
            )
        except BaseException as exc:
            # Registration/runtime activation already committed.  A transient
            # acknowledgement projection failure must never tear the bot down.
            logger.error(
                "could not persist Lark onboarding success notification (%s)",
                type(exc).__name__,
            )

    async def _run_owned(
        self,
        envelope: InboundEnvelope,
        *,
        command_id: str,
        profile_id: str,
        cancel_event: threading.Event,
        initial_result: asyncio.Future[None],
    ) -> None:
        try:
            await self._run(
                envelope,
                command_id=command_id,
                profile_id=profile_id,
                cancel_event=cancel_event,
                initial_result=initial_result,
            )
        finally:
            async with self._lock:
                self._tasks.pop(command_id, None)
                self._initial_results.pop(command_id, None)
                if self._active_command_id == command_id:
                    self._active_command_id = ""

    async def start(
        self,
        envelope: InboundEnvelope,
        *,
        command_id: str,
        profile_id: str = "",
    ) -> None:
        """Schedule onboarding after its first durable user update is proven."""

        identity = self._validate_request(envelope, command_id)
        task: asyncio.Task[None] | None = None
        initial_result: asyncio.Future[None] | None = None
        async with self._lock:
            if self._closed:
                raise LarkChatOnboardingError(
                    "Lark onboarding service is shutting down"
                )
            if identity in self._tasks:
                task = self._tasks[identity]
                initial_result = self._initial_results[identity]
            if self._active_command_id:
                if task is not None:
                    pass
                else:
                    await self._enqueue(
                        envelope,
                        identity,
                        "busy",
                        _BUSY_MESSAGE,
                    )
                    return
            if task is None:
                cancel_event = threading.Event()
                initial_result = asyncio.get_running_loop().create_future()
                task = asyncio.create_task(
                    self._run_owned(
                        envelope,
                        command_id=identity,
                        profile_id=str(profile_id or ""),
                        cancel_event=cancel_event,
                        initial_result=initial_result,
                    ),
                    name=f"lark-chat-onboarding:{identity}",
                )
                self._active_command_id = identity
                self._tasks[identity] = task
                self._initial_results[identity] = initial_result
        assert task is not None and initial_result is not None
        try:
            await asyncio.shield(initial_result)
        except asyncio.CancelledError:
            task.cancel()
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    pass
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def stop(self) -> None:
        """Cancel and drain every credential transaction before account shutdown."""

        async with self._lock:
            self._closed = True
            tasks = tuple(self._tasks.values())
            for task in tasks:
                task.cancel()
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            failures = tuple(
                result
                for result in results
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            )
            if failures:
                raise LarkChatOnboardingError(
                    "one or more Lark onboarding transactions did not stop safely"
                )


__all__ = [
    "LarkChatOnboardingError",
    "LarkChatOnboardingService",
]
