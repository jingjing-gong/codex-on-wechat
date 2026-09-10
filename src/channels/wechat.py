"""WeChat channel adapter for the durable agent runtime.

This module keeps the iLink wire models out of the domain layer.  It provides
three independently testable pieces:

* normalization of a finished WeChat text message into an immutable envelope;
* durable inbound acceptance through an injected task manager/store;
* user-outbox delivery with a stable WeChat ``client_id``.

The adapter does not own Codex state.  In production, its async methods must be
submitted to the single runtime event loop from the blocking WeChat monitor.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
import hashlib
import inspect
import json
import logging
import math
from pathlib import Path
import re
import shlex
import subprocess
import threading
import unicodedata
import uuid
from dataclasses import asdict, dataclass, is_dataclass, replace
from typing import Any, Callable, Mapping, Protocol, Sequence

from pydantic import ValidationError

from wechat_ilink.cdn import (
    CDN_TOTAL_TIMEOUT_SECONDS,
    aes_key_to_base64,
    download_file_from_cdn,
)
from wechat_ilink.sender import (
    SendMessageError,
    SenderIdentityError,
    derive_contextless_client_id,
    is_context_prepare_failure,
    resolve_sender_identity,
    send_text_reply,
    send_typing_state,
)
from wechat_ilink.markdown import markdown_to_plain_text
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    ITEM_TYPE_FILE,
    ITEM_TYPE_IMAGE,
    ITEM_TYPE_VIDEO,
    ITEM_TYPE_VOICE,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_BOT,
    MESSAGE_TYPE_USER,
    BaseInfo,
    FileItem,
    ImageItem,
    MediaInfo,
    MessageItem,
    SendMessageRequest,
    SendMsg,
    VideoItem,
    VoiceItem,
    WeixinMessage,
)

from .models import (
    ChannelCommand,
    DeliveryReceipt,
    InboundEnvelope,
    ReplyTarget,
    UserDelivery,
    parse_command,
    utc_now,
)
from .commands import (
    CommandResponse,
    MVPCommandRouter,
    _bounded_catalog_markdown,
    _bounded_markdown_fence,
    _bounded_public_error,
    _bounded_public_value,
    _command_message,
    _command_request_id_for_delivery,
    _default_model,
    _find_model,
    _format_agent,
    _format_agents_markdown,
    _format_inbox_item,
    _format_model_capability_error,
    _format_model_selection_markdown,
    _format_models_markdown,
    _format_modes_markdown,
    _format_shell_error,
    _format_shell_markdown,
    _format_task,
    _format_working_directory,
    _inbox_ids,
    _invoke_compatible,
    _is_accepted,
    _is_duplicate,
    _markdown_fence,
    _matching_effort,
    _maybe_await,
    _model_default_effort,
    _model_display_name,
    _model_efforts,
    _model_id,
    _model_is_default,
    _model_mapping,
    _model_selection,
    _normalized_public_text,
    _switch_back_fragment_specs,
    _system_role_command_response,
    _system_role_fragment_specs,
    _system_role_fragments_from_response,
    _task_belongs_to,
    _task_dedupe_key,
    _task_id,
    _task_state,
    _value,
    _working_directory_path,
    command_client_id,
    command_delivery_id,
    command_delivery_id_candidates,
    command_initial_reply,
    command_request_id,
    command_request_id_candidates,
    command_task_id,
    legacy_command_delivery_id,
    run_shell_command,
    COMMAND_HELP,
    COMMAND_POLICIES,
    COMMAND_REGISTRY,
    DEFAULT_COMMAND_POLICY,
    DEFERRED_REPLY_QUOTA_CAPABILITY,
    LARK_COMMAND_POLICY,
    MVP_COMMANDS,
    MVP_COMMAND_NAMES,
    WECHAT_COMMAND_POLICY,
    ChannelCommandPolicy,
    CommandRegistryEntry,
    CommandRegistryGroup,
    _command_registry_indexes,
    _command_usage,
    _render_command_help,
    command_help_for_channel,
    command_names_for_channel,
    command_policy_for,
    command_supported,
    filter_command_registry,
    unsupported_command_response,
)
from src.runtime.media import (
    AttachmentError,
    AttachmentStore,
    StoredAttachment,
    canonical_media_input,
    canonical_media_inputs,
    wire_media_fingerprint,
)
from src.runtime.identity import conversation_id, scoped_id
from src.runtime.models import USER_REPLY_FORMAT_AGENT_PREFIX_V1
from src.runtime.store import (
    QueueFullError,
    WORKING_DIRECTORY_RESPONSE_MAX_CHARS,
    format_working_directory_response,
)
from src.runtime.roles import (
    RoleValidationError,
    is_default_role_token,
    normalize_role_text,
    validate_role_snapshot,
)
from src.runtime.skills import (
    SkillDefinition,
    SkillInvocation,
    SkillSyntaxError,
    find_skill,
    format_skills_markdown,
    normalize_skill,
    parse_skill_invocation,
)
from src.runtime.shell import run_bounded_shell_process

logger = logging.getLogger(__name__)

CHANNEL = "wechat"
DEFAULT_AGENT_ID = "codex"
DEFAULT_SESSION_ID = "default"


ACTIVE_TASK_STATES = frozenset({"queued", "claimed", "running", "cancel_requested"})
RETRYABLE_TASK_STATES = frozenset({"failed", "orphaned", "interrupted"})
TERMINAL_TASK_STATES = frozenset({"completed", "failed", "interrupted", "cancelled", "canceled"})

_MAX_COMMAND_MARKDOWN = 6000
_MAX_SHELL_OUTPUT = 6000
_MAX_SHELL_COMMAND_MARKDOWN = 1500
_SHELL_TIMEOUT = 30
_LIST_TRUNCATION_MARKER = "_... (list truncated)_"
_CONTENT_TRUNCATION_MARKER = "... (content truncated)"
_MAX_PUBLIC_ID = 256
_MAX_PUBLIC_TEXT = WORKING_DIRECTORY_RESPONSE_MAX_CHARS
_MAX_EFFORT_NAME = 96
_MAX_EFFORT_LIST = 1024
_WECHAT_REPLY_TEXT_LIMIT = 3_000
_REPLY_CONTINUATION_SUFFIX = "\n\nReply /recv to continue."
_OUTBOX_INITIAL_RETRY_DELAY_SECONDS = 5.0
_OUTBOX_MAX_RETRY_DELAY_SECONDS = 300.0


def _outbox_retry_delay(record: Any) -> float:
    """Return a capped exponential delay for the attempt just claimed."""

    try:
        attempts = max(1, int(_value(record, "attempts", "attempt", default=1)))
    except (TypeError, ValueError):
        attempts = 1
    # Clamp the exponent before raising so corrupt/very old counters cannot
    # allocate an enormous integer. The final cap produces 5,10,...,300s.
    exponent = min(attempts - 1, 6)
    return min(
        _OUTBOX_MAX_RETRY_DELAY_SECONDS,
        _OUTBOX_INITIAL_RETRY_DELAY_SECONDS * (2**exponent),
    )


def _consume_detached_operation(task: asyncio.Task[Any]) -> None:
    """Retrieve a cancelled external operation if it finishes after lease loss."""

    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.debug("detached channel operation failed", exc_info=True)


async def _await_while_claimed(
    operation: Any,
    claim_lost: asyncio.Event,
    *,
    name: str,
) -> tuple[bool, Any]:
    """Await channel I/O until its durable claim is known to be lost."""

    operation_task = asyncio.create_task(operation, name=name)
    loss_waiter = asyncio.create_task(
        claim_lost.wait(), name=f"{name}:claim-loss"
    )
    try:
        await asyncio.wait(
            (operation_task, loss_waiter),
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Ownership loss wins a simultaneous completion. The external effect
        # may have happened, but a stale worker must neither persist nor report
        # its result under a claim that recovery can already transfer.
        if claim_lost.is_set():
            if not operation_task.done():
                operation_task.cancel()
                await asyncio.sleep(0)
            if operation_task.done():
                await asyncio.gather(operation_task, return_exceptions=True)
            else:
                operation_task.add_done_callback(_consume_detached_operation)
            return False, None
        return True, await operation_task
    finally:
        loss_waiter.cancel()
        await asyncio.gather(loss_waiter, return_exceptions=True)


async def _drain_shielded_operation(operation: Any) -> tuple[Any, bool]:
    """Finish a durable operation and report caller cancellation.

    SQLite operations deliberately settle their executor work after
    cancellation.  Shielding here also preserves the operation's return value,
    which tells the caller whether it acquired ownership that must be released
    before cancellation can escape.
    """

    task = asyncio.create_task(operation)
    cancelled_during_operation = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled_during_operation = True
            continue
    return task.result(), cancelled_during_operation


async def _invoke_external_hook(hook: Callable[..., Any], *args: Any) -> Any:
    """Invoke async hooks directly and keep synchronous channel I/O off-loop."""

    async_callable = inspect.iscoroutinefunction(hook) or inspect.iscoroutinefunction(
        getattr(hook, "__call__", None)
    )
    if async_callable:
        return await hook(*args)
    result = await asyncio.to_thread(hook, *args)
    return await result if inspect.isawaitable(result) else result




COMMAND_INTERRUPTED_RESPONSE = (
    "command outcome is unknown after interrupted processing; "
    "inspect the current state before retrying"
)

# One inbound envelope may contain more than one media item.  The channel CDN
# helper bounds each download to this same duration; promotion runs those
# independent downloads concurrently so the monitor bridge does not wait one
# full CDN timeout per item.
DEFAULT_MEDIA_PROMOTION_TIMEOUT_SECONDS = CDN_TOTAL_TIMEOUT_SECONDS


class InboundAcceptor(Protocol):
    """Minimal runtime boundary used by :class:`WeChatGateway`."""

    async def accept_inbound(self, envelope: InboundEnvelope) -> Any: ...


class CommandHandler(Protocol):
    async def handle_command(
        self,
        command: ChannelCommand,
        envelope: InboundEnvelope,
        *,
        command_id: str = "",
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class Acceptance:
    """Outcome of one normalized inbound acceptance attempt."""

    envelope: InboundEnvelope
    accepted: bool
    duplicate: bool = False
    task_id: str = ""
    command_response: str = ""
    confirmation_ids: tuple[str, ...] = ()
    raw_result: Any = None
    # Most command acknowledgements use the Agent route captured when the
    # inbound arrived.  An explicit ``/agent B`` switch is the exception: its
    # confirmation belongs to the post-switch Agent.  Keep this separate from
    # the immutable envelope so task/media ownership is never rewritten by a
    # control response.
    response_agent_id: str = ""
    # Upgrade recovery may find a response under the pre-v1 compound ID.
    # Reuse that exact projection identity so a redelivery cannot create a
    # second user-outbox row under the canonical v1 encoding.
    response_delivery_id: str = ""
    # User notifications fetched while handling /inbox are marked presented
    # only after the command acknowledgement is durably projected.
    # Keeping the IDs on the acceptance lets the gateway complete that second
    # half in one store transaction where supported.
    presentation_ids: tuple[str, ...] = ()
    # Some command renderings need transport-aware fragments.  In particular,
    # a long `/system` role must put a complete Markdown fence in every WeChat
    # message instead of letting the generic 3,000-character slicer split one
    # fence across messages.  The command receipt still owns the complete
    # response text; these are a deterministic presentation of those bytes.
    response_fragments: tuple[Mapping[str, Any], ...] = ()

    @property
    def status(self) -> str:
        if self.duplicate:
            return "duplicate"
        return "accepted" if self.accepted else "rejected"


@dataclass(frozen=True, slots=True)
class _MonitorCallbackFailure:
    exception: BaseException




def conversation_id_for(
    *,
    bot_id: str,
    external_user_id: str,
    session_id: str = DEFAULT_SESSION_ID,
    agent_id: str = DEFAULT_AGENT_ID,
) -> str:
    """Build the stable per-channel, per-Agent conversation identity."""

    return conversation_id(
        CHANNEL,
        bot_id,
        external_user_id,
        session_id or DEFAULT_SESSION_ID,
        agent_id,
    )


def extract_text(message: WeixinMessage) -> str | None:
    """Return typed text and voice transcription as one logical instruction.

    WeChat supplies speech-to-text for a voice bubble in ``VoiceItem.text``.
    Treat that transcript exactly like typed input so the message queues a
    task immediately.  Multiple text/voice items retain their wire order and
    form one task, which also prevents one channel message from bypassing the
    dedupe constraint by creating a task per item.
    """

    if (
        message.message_type != MESSAGE_TYPE_USER
        or message.message_state != MESSAGE_STATE_FINISH
    ):
        return None
    parts: list[str] = []
    for item in message.item_list:
        value = ""
        if item.type == ITEM_TYPE_TEXT and item.text_item is not None:
            value = str(item.text_item.text or "")
        elif item.type == ITEM_TYPE_VOICE and item.voice_item is not None:
            value = str(item.voice_item.text or "")
        if value.strip():
            parts.append(value)
    if not parts:
        return None
    return "\n".join(parts)


def extract_media(message: WeixinMessage) -> list[dict[str, Any]]:
    """Normalize finished WeChat media items into opaque structured refs.

    The adapter does not download or decrypt files.  It records the channel
    reference and metadata so the durable task can hand it to a managed media
    pipeline later.  Voice ``text`` is retained with that media metadata while
    :func:`extract_text` also promotes it to the task instruction.
    """
    if (
        message.message_type != MESSAGE_TYPE_USER
        or message.message_state != MESSAGE_STATE_FINISH
    ):
        return []
    values: list[dict[str, Any]] = []
    for item in message.item_list:
        # Text is normalized separately by ``extract_text``. Preserve every
        # other unrecognized item as redacted structured context so a future
        # WeChat item type cannot disappear while the durable cursor advances.
        item_type = int(item.type)
        if item_type == ITEM_TYPE_TEXT:
            continue
        kind = {
            ITEM_TYPE_IMAGE: "image",
            ITEM_TYPE_VOICE: "audio",
            ITEM_TYPE_FILE: "file",
            ITEM_TYPE_VIDEO: "video",
        }.get(item_type, "")
        if not kind:
            values.append(
                {
                    "kind": "unsupported",
                    # Retained only until the wire fingerprint is computed;
                    # canonical media redaction drops this transport field.
                    "type": item_type,
                    "mime_type": "application/octet-stream",
                    "available": False,
                    "native_input_available": False,
                    "error": f"unsupported WeChat item type: {item_type}",
                }
            )
            continue
        field_name = {
            "image": "image_item",
            "audio": "voice_item",
            "file": "file_item",
            "video": "video_item",
        }[kind]
        detail = getattr(item, field_name, None)
        if detail is None:
            detail_data: dict[str, Any] = {}
        elif hasattr(detail, "model_dump"):
            detail_data = detail.model_dump(mode="json", by_alias=True, exclude_none=True)
        elif isinstance(detail, Mapping):
            detail_data = dict(detail)
        else:
            detail_data = {
                key: getattr(detail, key)
                for key in getattr(detail, "__dataclass_fields__", {})
                if hasattr(detail, key)
            }
        media = detail_data.get("media")
        media_data = dict(media) if isinstance(media, Mapping) else {}
        values.append(
            {
                "kind": kind,
                "mime_type": {
                    "image": "image/*",
                    "audio": "audio/*",
                    "file": "application/octet-stream",
                    "video": "video/*",
                }[kind],
                "remote_id": str(
                    detail_data.get("url")
                    or detail_data.get("file_name")
                    or media_data.get("encrypt_query_param")
                    or ""
                ),
                "encrypted_query_param": str(
                    media_data.get("encrypt_query_param") or ""
                ),
                "encryption_key": str(media_data.get("aes_key") or ""),
                "filename": str(detail_data.get("file_name") or ""),
                "candidate_text": str(detail_data.get("text") or "")
                if kind == "audio"
                else "",
                "metadata": detail_data,
            }
        )
    return values


def external_message_id_for(message: WeixinMessage, *, bot_id: str = "") -> str:
    """Return a stable inbound message identity.

    iLink normally supplies ``message_id``.  For older/partial events without
    one, the fallback is a hash of the sender, recipient/bot, sequence, and
    canonical message items.  Rolling transport hints such as
    ``context_token`` are deliberately excluded, including when no sequence
    exists, so redelivery after a reconnect retains the same identity.  The
    result is explicitly namespaced so it cannot collide with a genuine iLink
    ID.
    """

    if message.message_id:
        return str(message.message_id)

    if hasattr(message, "model_dump"):
        item_payload: Any = message.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ).get("item_list", [])
    else:
        item_payload = [
            asdict(item) if is_dataclass(item) else repr(item)
            for item in message.item_list
        ]
    material = {
        "bot_id": bot_id or message.to_user_id,
        "from_user_id": message.from_user_id,
        "to_user_id": message.to_user_id,
        "seq": message.seq,
        "items": item_payload,
    }
    encoded = json.dumps(
        material, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"fallback:{hashlib.sha256(encoded).hexdigest()}"


def normalize_message(
    message: WeixinMessage,
    *,
    bot_id: str = "",
    session_id: str = DEFAULT_SESSION_ID,
    agent_id: str = DEFAULT_AGENT_ID,
) -> InboundEnvelope | None:
    """Normalize a WeChat wire message, or return ``None`` if unsupported."""

    text = extract_text(message)
    media = extract_media(message)
    if text is None and not media:
        return None
    resolved_bot_id = bot_id or message.to_user_id
    resolved_session_id = session_id or DEFAULT_SESSION_ID
    external_message_id = external_message_id_for(message, bot_id=resolved_bot_id)
    raw: Mapping[str, Any] | None = None
    if hasattr(message, "model_dump"):
        raw = message.model_dump(mode="json", by_alias=True, exclude_none=True)
    elif is_dataclass(message):
        raw = asdict(message)
    if raw is None and media:
        raw = {"media": media}
    elif media:
        raw = {**dict(raw or {}), "media": media}
    # ``context_token`` is a rolling transport hint, not part of the
    # immutable message payload.  Removing it from diagnostic metadata lets a
    # redelivered message with the same external ID but a refreshed token
    # pass replay validation while the first token remains on the durable
    # reply target.
    if raw is not None:
        raw = dict(raw)
        raw.pop("context_token", None)
        raw.pop("contextToken", None)
        # ``item_list`` is a wire-level diagnostic copy.  Its nested media
        # objects contain CDN query parameters and AES keys; the normalized
        # ``media`` projection above is the only representation that may
        # cross into durable storage.  Keep message identity computation
        # independent of this redaction (it already ran above).
        raw.pop("item_list", None)
        raw.pop("itemList", None)
    return InboundEnvelope(
        channel=CHANNEL,
        bot_id=resolved_bot_id,
        external_user_id=message.from_user_id,
        external_message_id=external_message_id,
        text=text or "",
        session_id=resolved_session_id,
        agent_id=agent_id or DEFAULT_AGENT_ID,
        conversation_id=conversation_id_for(
            bot_id=resolved_bot_id,
            external_user_id=message.from_user_id,
            session_id=resolved_session_id,
            agent_id=agent_id or DEFAULT_AGENT_ID,
        ),
        source_sequence=message.seq,
        context_token=message.context_token,
        raw=raw,
    )


def should_send_typing_state(message: WeixinMessage) -> bool:
    """Return whether one wire message warrants a transient typing state.

    Keep this predicate aligned with :func:`normalize_message` without
    invoking session/Agent resolvers from the Monitor worker thread.  A
    finished user message is supported when it contains nonblank text/voice
    transcription or any media/unsupported non-text item that normalization
    retains as structured context.
    """

    if (
        message.message_type != MESSAGE_TYPE_USER
        or message.message_state != MESSAGE_STATE_FINISH
    ):
        return False
    return extract_text(message) is not None or bool(extract_media(message))


def send_inbound_typing_state(client: Any, message: WeixinMessage) -> bool:
    """Best-effort typing acknowledgement for one supported inbound message.

    This runs on the blocking Monitor contact worker before durable acceptance
    is submitted to the runtime loop.  Typing is ephemeral and quota-free, so
    failures are logged and contained rather than entering SQLite or the
    durable outbox.
    """

    if not should_send_typing_state(message):
        return False
    try:
        send_typing_state(
            client,
            str(message.from_user_id or ""),
            str(message.context_token or ""),
        )
    except Exception:
        logger.warning(
            "could not send typing indicator to %s",
            message.from_user_id,
            exc_info=True,
        )
        return False
    return True


def stable_client_id(delivery: UserDelivery | Mapping[str, Any]) -> str:
    """Return the persisted ID or a deterministic fallback for retries."""

    item = _coerce_delivery(delivery)
    if item.client_id:
        return item.client_id
    identity = item.delivery_id or "\x1f".join(
        (item.target.stable_key(), item.task_id, item.event_id, item.content)
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-wechat:{identity}"))


def stable_contextless_client_id(
    delivery: UserDelivery | Mapping[str, Any],
) -> str:
    """Return the persisted or deterministic context-free wire identity."""

    item = _coerce_delivery(delivery)
    if item.contextless_client_id:
        if item.contextless_client_id == stable_client_id(item):
            raise ValueError(
                "contextless client_id must differ from primary client_id"
            )
        return item.contextless_client_id
    return derive_contextless_client_id(stable_client_id(item))


def _delivery_sender_id(item: UserDelivery) -> str:
    """Return the delivery's explicit sender and validate its durable target."""

    sender_id = str(item.from_user_id or "")
    target_bot_id = str(item.target.bot_id or "")
    if not sender_id:
        raise SenderIdentityError("delivery is missing outbound from_user_id")
    if not target_bot_id:
        raise SenderIdentityError("delivery target is missing bot_id")
    if sender_id != target_bot_id:
        raise SenderIdentityError(
            "delivery sender conflicts with durable reply target: "
            f"{sender_id!r} != {target_bot_id!r}"
        )
    return sender_id




















def transcription_confirmation_id(envelope: InboundEnvelope, ordinal: int) -> str:
    """Derive a stable confirmation identity for one audio item.

    The external channel identity is available before SQLite allocates its
    internal inbound row ID, and is stable across redelivery.  Including the
    item ordinal keeps multiple voice items in one envelope distinct while
    avoiding content-derived IDs that could change across protocol retries.
    """

    identity = "\x1f".join(
        (
            envelope.channel,
            envelope.bot_id,
            envelope.external_user_id,
            envelope.session_id,
            envelope.external_message_id,
            str(int(ordinal)),
        )
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-wechat:transcription:{identity}"))


def send_user_delivery(
    client: Any, delivery: UserDelivery | Mapping[str, Any]
) -> DeliveryReceipt:
    """Deliver one durable user-outbox record through WeChat.

    Only user-visible WeChat projections are accepted here.  In particular,
    Agent mailbox messages cannot accidentally pass through the channel
    sender.  The same stable ``client_id`` is used on every retry.
    """

    item = _coerce_delivery(delivery)
    primary_client_id = stable_client_id(item)
    wire_variant = item.active_wire_variant
    wire_client_id = (
        stable_contextless_client_id(item)
        if wire_variant == "contextless"
        else primary_client_id
    )
    if item.visibility != "user":
        raise ValueError("only user-visible outbox records may be sent to WeChat")
    if item.target.channel != CHANNEL:
        raise ValueError(f"cannot send {item.target.channel!r} delivery via WeChat")
    if not item.target.external_user_id:
        raise ValueError("delivery target is missing external_user_id")
    if not item.content:
        raise ValueError("delivery content is empty")

    try:
        sender_id = _delivery_sender_id(item)
        # Validate here even when a compatibility test sender does not yet
        # accept the explicit keyword.  Its ambient value is therefore known
        # to be the same durable identity and cannot redirect the send.
        resolve_sender_identity(client, sender_id)
        # Keep compatibility with simple test/dry-run senders that predate
        # the canonical keywords; the real iLink sender accepts all of them.
        try:
            parameters = inspect.signature(send_text_reply).parameters
        except (TypeError, ValueError):
            parameters = {}
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        accepts_client_id = "client_id" in parameters or accepts_kwargs
        args = (
            client,
            item.target.external_user_id,
            item.content,
            "" if wire_variant == "contextless" else item.target.context_token,
        )
        kwargs: dict[str, Any] = {}
        if accepts_client_id or not parameters:
            kwargs["client_id"] = wire_client_id
        if "message_state" in parameters or accepts_kwargs or not parameters:
            kwargs["message_state"] = MESSAGE_STATE_FINISH
        if "from_user_id" in parameters or accepts_kwargs or not parameters:
            kwargs["from_user_id"] = sender_id
        if (
            "allow_contextless_fallback" in parameters
            or accepts_kwargs
            or not parameters
        ):
            # The delivery worker must commit the one-way transition before
            # the alternate identity is allowed onto the wire.
            kwargs["allow_contextless_fallback"] = False
        send_result = send_text_reply(*args, **kwargs)
        if send_result is False:
            raise RuntimeError("channel sender reported failure")
    except Exception as exc:
        transition = (
            "contextless"
            if wire_variant == "primary"
            and bool(item.target.context_token)
            and is_context_prepare_failure(exc)
            else ""
        )
        return DeliveryReceipt(
            delivery_id=item.delivery_id,
            client_id=wire_client_id,
            sent=False,
            error=str(exc),
            retryable=(
                True if transition else bool(getattr(exc, "retryable", True))
            ),
            wire_variant=wire_variant,
            transition_to_wire_variant=transition,
        )
    return DeliveryReceipt(
        delivery_id=item.delivery_id,
        client_id=wire_client_id,
        sent=True,
        wire_variant=wire_variant,
    )


async def send_user_delivery_async(
    client: Any, delivery: UserDelivery | Mapping[str, Any]
) -> DeliveryReceipt:
    """Run the blocking iLink send outside the runtime event loop."""

    return await asyncio.to_thread(send_user_delivery, client, delivery)


def _media_metadata(record: Any) -> Mapping[str, Any]:
    value = _value(record, "metadata", default={})
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "to_dict"):
        converted = value.to_dict()
        return converted if isinstance(converted, Mapping) else {}
    return {}


def _media_aes_key(value: Any) -> str:
    """Normalize a persisted hex/base64 AES key for ``MediaInfo``."""

    key = str(value or "").strip()
    if not key:
        return ""
    # ``upload_file_to_cdn`` returns a hex key, while a row restored from the
    # wire may already contain iLink's base64(hex) representation.  Do not
    # decode arbitrary values: preserving an opaque channel value is safer
    # than corrupting it.
    try:
        if len(key) == 32:
            int(key, 16)
            return aes_key_to_base64(key)
    except (TypeError, ValueError):
        pass
    return key


def _media_upload_metadata(uploaded: Any) -> dict[str, int] | None:
    """Return retry-safe protocol metadata supplied by the CDN upload."""

    value = _value(uploaded, "cipher_size", default=None)
    if value is None:
        return None
    try:
        cipher_size = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("outgoing media upload has an invalid cipher size") from exc
    if cipher_size < 0:
        raise ValueError("outgoing media upload has an invalid cipher size")
    return {"cipher_size": cipher_size}


def _media_message_item(record: Any, uploaded: Any = None) -> MessageItem:
    """Build one WeChat media item from durable metadata and upload state."""

    metadata = _media_metadata(record)
    uploaded = uploaded if uploaded is not None else record
    remote_id = str(
        _value(record, "remote_id", "download_param", default=None)
        or _value(uploaded, "remote_id", "download_param", "upload_param", default="")
        or ""
    )
    encryption_key = _media_aes_key(
        _value(
            record,
            "encryption_key",
            "aes_key",
            "aes_key_base64",
            default=None,
        )
        or _value(
            uploaded,
            "encryption_key",
            "aes_key_hex",
            "aes_key_base64",
            default="",
        )
    )
    if not remote_id or not encryption_key:
        raise ValueError("outgoing media upload has incomplete CDN parameters")

    kind = str(
        _value(record, "kind", default=None)
        or metadata.get("kind")
        or metadata.get("media_kind")
        or "file"
    ).strip().lower()
    mime_type = str(
        _value(record, "mime_type", default=None)
        or metadata.get("mime_type")
        or ""
    ).strip().lower()
    filename = str(
        _value(record, "filename", "file_name", default=None)
        or metadata.get("filename")
        or metadata.get("file_name")
        or ""
    )
    try:
        size = int(
            _value(record, "size", "size_bytes", default=None)
            or metadata.get("size_bytes")
            or metadata.get("size")
            or 0
        )
    except (TypeError, ValueError):
        size = 0
    cipher_size_value = _value(uploaded, "cipher_size", default=None)
    if cipher_size_value is None:
        cipher_size_value = metadata.get("cipher_size")
    if cipher_size_value is None:
        cipher_size_value = size
    try:
        cipher_size = int(cipher_size_value)
    except (TypeError, ValueError):
        cipher_size = size
    media = MediaInfo(
        encrypt_query_param=remote_id,
        aes_key=encryption_key,
        encrypt_type=1,
    )
    if kind == "image" or mime_type.startswith("image/"):
        return MessageItem(
            type=ITEM_TYPE_IMAGE,
            # iLink's ``mid_size`` is the encrypted CDN object size, not the
            # plaintext attachment size. Retain it at upload checkpoint so a
            # restarted send uses the same protocol value.
            image_item=ImageItem(media=media, mid_size=max(0, cipher_size)),
        )
    if kind == "video" or mime_type.startswith("video/"):
        return MessageItem(
            type=ITEM_TYPE_VIDEO,
            video_item=VideoItem(media=media, video_size=max(0, size)),
        )
    if kind in {"audio", "voice"} or mime_type.startswith("audio/"):
        return MessageItem(
            type=ITEM_TYPE_VOICE,
            voice_item=VoiceItem(media=media, voice_size=max(0, size)),
        )
    return MessageItem(
        type=ITEM_TYPE_FILE,
        file_item=FileItem(
            media=media,
            file_name=filename,
            len=str(max(0, size)),
        ),
    )


def _media_rows(value: Any) -> tuple[Any, ...]:
    """Normalize a canonical bundle's uploaded child rows without splitting maps."""

    if value is None:
        return ()
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, Mapping)
    ):
        return tuple(value)
    return (value,)


def send_media_delivery(client: Any, record: Any, uploaded: Any = None) -> bool:
    """Send one uploaded media projection through WeChat iLink.

    A slot-linked media projection is sent from its canonical user-outbox
    parent.  That parent owns the logical delivery ID, sender, primary and
    contextless wire IDs, context token, and lease.  ``uploaded`` contains its
    read-only outgoing-media children and may contain more than one item for a
    deliberately bundled reply.  Legacy media rows without an outbox/slot keep
    their historical independent identity for compatibility.
    """

    canonical = bool(
        _value(record, "outbox_id", "delivery_id", default="")
        and _value(record, "reply_slot_id", default=None)
    )
    metadata = _media_metadata(record)
    if canonical:
        delivery = _delivery_from_outbox(record)
        if delivery is None:
            raise ValueError("canonical media outbox has no user reply target")
        delivery_id = delivery.delivery_id
        user_id = delivery.target.external_user_id
        persisted_bot_id = delivery.target.bot_id
        sender_id = _delivery_sender_id(delivery)
        context_token = delivery.target.context_token
        primary_client_id = stable_client_id(delivery)
        contextless_client_id = delivery.contextless_client_id
        wire_variant = delivery.active_wire_variant
        children = _media_rows(uploaded)
        if not children:
            raise ValueError("canonical media outbox has no uploaded media children")
        items: list[MessageItem] = []
        if delivery.content:
            items.append(
                MessageItem(
                    type=ITEM_TYPE_TEXT,
                    text_item=TextItem(
                        text=markdown_to_plain_text(delivery.content)
                    ),
                )
            )
        items.extend(_media_message_item(child) for child in children)
    else:
        media_id = str(
            _value(record, "media_id", "delivery_id", "id", default="") or ""
        )
        user_id = str(_value(record, "external_user_id", default="") or "")
        if not media_id or not user_id:
            raise ValueError(
                "outgoing media record is missing media_id or external_user_id"
            )
        delivery_id = media_id
        persisted_bot_id = str(
            _value(record, "bot_id", default=None)
            or metadata.get("bot_id")
            or ""
        )
        sender_id = str(
            _value(record, "from_user_id", "sender_bot_id", default=None)
            or metadata.get("from_user_id")
            or metadata.get("sender_bot_id")
            or persisted_bot_id
            # Legacy rows predate durable sender identity.  Their owning
            # client was historically the only source of that identity, so
            # retain that compatibility without weakening canonical outbox
            # deliveries, which require their persisted sender above.
            or getattr(client, "bot_id", "")
        )
        context_token = str(
            _value(record, "context_token", default=None)
            or metadata.get("context_token")
            or ""
        )
        primary_client_id = str(
            _value(record, "primary_client_id", default=None)
            or _value(record, "client_id", default=None)
            or uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"codex-wechat-media:{media_id}",
            )
        )
        contextless_client_id = str(
            _value(record, "contextless_client_id", default=None)
            or metadata.get("contextless_client_id")
            or ""
        )
        wire_variant = str(
            _value(record, "active_wire_variant", default=None)
            or metadata.get("active_wire_variant")
            or "primary"
        ).strip().lower()
        items = [_media_message_item(record, uploaded)]

    if not delivery_id or not user_id:
        raise ValueError("outgoing media delivery identity is incomplete")
    if not sender_id:
        raise SenderIdentityError("outgoing media is missing outbound from_user_id")
    if persisted_bot_id and sender_id != persisted_bot_id:
        raise SenderIdentityError(
            "outgoing media sender conflicts with durable bot_id: "
            f"{sender_id!r} != {persisted_bot_id!r}"
        )
    resolve_sender_identity(client, sender_id)
    if not items:
        raise ValueError("outgoing media delivery has no WeChat message items")

    wire_variant = str(
        wire_variant or "primary"
    ).strip().lower()
    if wire_variant not in {"primary", "contextless"}:
        raise ValueError(f"unsupported media wire variant: {wire_variant!r}")
    client_id = primary_client_id
    if wire_variant == "contextless":
        client_id = str(
            contextless_client_id
            or ("" if canonical else derive_contextless_client_id(primary_client_id))
        )
        if not client_id:
            raise ValueError(
                "canonical contextless media client_id was not persisted"
            )
        if client_id == primary_client_id:
            raise ValueError(
                "contextless media client_id must differ from primary client_id"
            )
        context_token = ""
    request = SendMessageRequest(
        msg=SendMsg(
            from_user_id=sender_id,
            to_user_id=user_id,
            client_id=client_id,
            message_type=MESSAGE_TYPE_BOT,
            message_state=MESSAGE_STATE_FINISH,
            item_list=items,
            context_token=context_token,
        ),
        base_info=BaseInfo(),
    )
    response = client.send_message(request)
    ret = int(getattr(response, "ret", 0) or 0)
    errcode = int(getattr(response, "errcode", 0) or 0)
    if ret != 0 or errcode != 0:
        errmsg = str(getattr(response, "errmsg", "") or "")
        prepare_failure = (
            ret == -2
            and errcode == 0
            and errmsg.strip().casefold() == "prepare failed"
        )
        raise SendMessageError(
            ret,
            errcode,
            errmsg,
            retryable=not (prepare_failure and not context_token),
        )
    return True




def _has_field(record: Any, name: str) -> bool:
    """Return whether a record carries a field, even when its value is empty.

    Mailbox projections and user-outbox projections are deliberately separate.
    Some serializers emit optional routing fields as ``None``/``""`` rather
    than omitting them; checking truthiness would let such a mailbox row reach
    a channel sender.  Keep this helper intentionally structural so it works
    for mappings, dataclasses, and lightweight test doubles.
    """

    if isinstance(record, Mapping):
        return name in record
    return hasattr(record, name)




def _capability_target(target: Any, names: Sequence[str]) -> Any | None:
    """Return the public facade/store that actually exposes a capability."""

    candidates = (target, getattr(target, "store", None))
    for candidate in candidates:
        if candidate is None:
            continue
        if any(callable(getattr(candidate, name, None)) for name in names):
            return candidate
    return None


def _coerce_delivery(value: Any) -> UserDelivery:
    """Accept channel models, mappings, and runtime outbox dataclasses."""

    if isinstance(value, UserDelivery):
        return value
    if isinstance(value, Mapping):
        # Do this check before ``UserDelivery.from_dict`` drops unknown keys;
        # otherwise a mailbox mapping carrying a perfectly valid-looking user
        # target could be mistaken for a channel delivery.
        if _has_field(value, "destination_agent_id") or _has_field(value, "mailbox_id"):
            raise ValueError("Agent mailbox records cannot be sent to WeChat")
        return UserDelivery.from_dict(value)
    converted = _delivery_from_outbox(value)
    if converted is not None:
        return converted
    raise TypeError("delivery must be a UserDelivery, mapping, or outbox record")




class WeChatInboundMediaPromoter:
    """Download WeChat media into managed files before task creation.

    The normalized inbound envelope may retain the original wire object for
    audit/retry purposes.  Its ``media`` task-facing field is replaced with
    canonical managed metadata, so encrypted query parameters and AES keys
    never reach an Agent prompt.
    """

    def __init__(
        self,
        attachment_store: AttachmentStore,
        metadata_store: Any,
        *,
        downloader: Callable[..., Any] | None = None,
        promotion_timeout: float = DEFAULT_MEDIA_PROMOTION_TIMEOUT_SECONDS,
    ) -> None:
        try:
            resolved_timeout = float(promotion_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("promotion_timeout must be a positive finite number") from exc
        if not math.isfinite(resolved_timeout) or resolved_timeout <= 0:
            raise ValueError("promotion_timeout must be a positive finite number")
        self.attachment_store = attachment_store
        self.metadata_store = metadata_store
        self.downloader = downloader or download_file_from_cdn
        self.promotion_timeout = resolved_timeout

    @staticmethod
    def attachment_id(envelope: InboundEnvelope, ordinal: int, kind: str) -> str:
        material = "\x1f".join(
            (
                envelope.channel,
                envelope.bot_id,
                envelope.external_user_id,
                envelope.external_message_id,
                str(int(ordinal)),
                str(kind or "file").lower(),
            )
        )
        return "wechat-in-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:40]

    async def promote(self, envelope: InboundEnvelope) -> InboundEnvelope:
        raw = dict(envelope.raw or {})
        media_values = raw.get("media") or ()
        if isinstance(media_values, (Mapping, str, bytes, bytearray)):
            media_values = (media_values,)
        else:
            # Normalize one-shot iterables before both fingerprinting and
            # promotion.  The raw envelope normally carries a list, but
            # compatibility adapters may expose a generator here.
            try:
                media_values = tuple(media_values)
            except TypeError:
                media_values = (media_values,)
        # Preserve a non-secret fingerprint of the original channel reference
        # for immutable replay checks after ``media`` is canonicalized.
        wire_fingerprints = [
            self._wire_fingerprint(value) for value in media_values
        ]
        raw["__media_wire_fingerprints"] = wire_fingerprints
        durable_replay = await self._durable_replay_snapshot(
            envelope,
            media_values,
            wire_fingerprints,
        )
        replay_fingerprint_override: list[str] | None = None
        if durable_replay is not None:
            replay_media, replay_fingerprints = durable_replay
            replay_fingerprint_override = replay_fingerprints
            if replay_media is not None:
                raw["media"] = replay_media
                raw["__media_wire_fingerprints"] = replay_fingerprints
                return replace(envelope, raw=raw)
        if media_values:
            tasks = [
                asyncio.create_task(self._promote_one(envelope, value, ordinal))
                for ordinal, value in enumerate(media_values)
            ]
            try:
                promoted = await asyncio.wait_for(
                    asyncio.gather(*tasks), timeout=self.promotion_timeout
                )
            except asyncio.TimeoutError as exc:
                # A timeout is a retryable ingress failure.  Explicitly cancel
                # and drain every sibling so no promotion coroutine remains
                # attached to a message whose monitor cursor was retained.
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise AttachmentError("WeChat media promotion timed out") from exc
            except BaseException:
                # ``gather`` propagates the first failure without cancelling
                # other children.  Drain them here to prevent late writes from
                # racing the monitor's retry of this immutable message.
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        else:
            promoted = []
        raw["media"] = promoted
        if replay_fingerprint_override is not None:
            # Preserve the marker format already committed for a legacy row;
            # SQLite compares this trusted out-of-band value against that
            # immutable snapshot after the healthy file is reverified.
            raw["__media_wire_fingerprints"] = replay_fingerprint_override
        return replace(envelope, raw=raw)

    async def _durable_replay_snapshot(
        self,
        envelope: InboundEnvelope,
        media_values: Sequence[Any],
        wire_fingerprints: Sequence[str],
    ) -> tuple[list[dict[str, Any]] | None, list[str]] | None:
        """Reuse immutable media metadata only when a replay cannot reverify it.

        Ready managed files still follow the ordinary download/idempotent-write
        path so changed CDN bytes are detected.  If cleanup or reconciliation
        made a file unavailable, however, SQLite already owns the message and
        its original task.  Reusing that exact durable media projection lets
        the store acknowledge the duplicate without treating it as new work.

        The same compatibility path handles messages accepted before managed
        promotion was enabled.  In both cases the raw channel reference must
        match the fingerprint stored on the first acceptance.
        """

        try:
            inbound = await _invoke_compatible(
                self.metadata_store,
                ("get_inbound", "get_inbound_message"),
                positional=(
                    envelope.channel,
                    envelope.bot_id,
                    envelope.external_message_id,
                ),
            )
        except AttributeError:
            return None
        if inbound is None:
            return None

        immutable_identity = (
            ("channel", envelope.channel, ""),
            ("bot_id", envelope.bot_id, ""),
            ("external_user_id", envelope.external_user_id, ""),
            ("external_message_id", envelope.external_message_id, ""),
            ("session_id", envelope.session_id, DEFAULT_SESSION_ID),
            ("source_sequence", envelope.source_sequence, None),
            ("text", envelope.text, ""),
        )
        for name, incoming, default in immutable_identity:
            stored = _value(inbound, name, default=default)
            if name == "session_id":
                stored = stored or DEFAULT_SESSION_ID
                incoming = incoming or DEFAULT_SESSION_ID
            if stored != incoming and str(stored) != str(incoming):
                raise AttachmentError(
                    "durable media replay conflicts with immutable inbound identity"
                )

        payload = _value(inbound, "payload", "raw", default={})
        if not isinstance(payload, Mapping):
            return None
        stored_values = payload.get("media") or ()
        if isinstance(stored_values, Mapping):
            stored_values = (stored_values,)
        else:
            try:
                stored_values = tuple(stored_values)
            except TypeError:
                return None
        if len(stored_values) != len(media_values):
            return None

        # Replays of healthy promoted files intentionally redownload and verify
        # bytes.  Snapshot hydration is reserved for unavailable or legacy
        # unpromoted records, where that verification path cannot complete.
        needs_hydration = False
        for ordinal, (stored_media, wire_media) in enumerate(
            zip(stored_values, media_values)
        ):
            kind = str(
                _value(wire_media, "kind", "type", "media_kind", default="file")
                or "file"
            ).lower()
            stored_kind = str(
                _value(stored_media, "kind", "type", "media_kind", default="file")
                or "file"
            ).lower()
            stored_id = str(
                _value(stored_media, "attachment_id", default="") or ""
            )
            if kind == "unsupported":
                if stored_kind != "unsupported" or stored_id:
                    raise AttachmentError(
                        "durable media replay conflicts with unsupported media context"
                    )
                # Opaque placeholders intentionally have no managed file. They
                # must not make a healthy sibling look unavailable, because
                # hydrating the whole stored snapshot would skip that sibling's
                # normal download and immutable-byte verification on replay.
                continue
            expected_id = self.attachment_id(envelope, ordinal, kind)
            if stored_id and stored_id != expected_id:
                raise AttachmentError(
                    "durable media replay conflicts with managed attachment identity"
                )
            if not stored_id:
                needs_hydration = True
                continue
            try:
                state = await _invoke_compatible(
                    self.metadata_store,
                    ("get_attachment_state",),
                    positional=(stored_id,),
                )
            except AttributeError:
                return None
            if str(state or "") != "ready":
                needs_hydration = True
        stored_fingerprints = payload.get("__media_wire_fingerprints")
        if not isinstance(stored_fingerprints, (list, tuple)) or len(
            stored_fingerprints
        ) != len(media_values):
            if not needs_hydration:
                return None
            raise AttachmentError(
                "durable media replay has no verifiable wire fingerprint"
            )
        verified_fingerprints: list[str] = []
        for stored, current, media in zip(
            stored_fingerprints, wire_fingerprints, media_values
        ):
            stored_value = str(stored or "").lower()
            if stored_value not in {
                str(current).lower(),
                self._legacy_store_wire_fingerprint(media),
                self._legacy_promoter_wire_fingerprint(media),
            }:
                kind = str(
                    _value(media, "kind", "type", "media_kind", default="file")
                    or "file"
                ).lower()
                if kind == "unsupported":
                    raise AttachmentError(
                        "durable media replay conflicts with the original wire reference"
                    )
                if not needs_hydration:
                    # Let the ordinary healthy replay path reach SQLite's
                    # immutable-envelope validator, preserving its precise
                    # conflict error and avoiding a pre-download policy change.
                    return None
                raise AttachmentError(
                    "durable media replay conflicts with the original wire reference"
                )
            verified_fingerprints.append(stored_value)

        if not needs_hydration:
            return (None, verified_fingerprints)
        return (
            list(canonical_media_inputs(stored_values, include_candidate=True)),
            verified_fingerprints,
        )

    async def _promote_one(
        self,
        envelope: InboundEnvelope,
        media: Any,
        ordinal: int,
    ) -> dict[str, Any]:
        kind = str(
            _value(media, "kind", "type", "media_kind", default="file") or "file"
        ).lower()
        if kind == "unsupported":
            # This is an intentionally opaque placeholder, not a channel
            # attachment reference. Never consult the store or CDN for it.
            safe = canonical_media_input(media, include_candidate=True)
            safe["available"] = False
            safe["native_input_available"] = False
            safe.setdefault("error", "unsupported WeChat media item")
            return safe
        # Derive the ID exclusively from the immutable inbound identity.  A
        # caller-supplied ID must never be able to select another file.
        attachment_id = self.attachment_id(envelope, ordinal, kind)
        existing = await self._get_attachment(attachment_id)
        if existing is not None:
            existing = self.attachment_store.remember(existing)

        encrypted_query = str(
            _value(
                media,
                "encrypted_query_param",
                "encrypt_query_param",
                "download_param",
                default="",
            )
            or ""
        )
        encryption_key = str(
            _value(
                media,
                "encryption_key",
                "aes_key",
                "aes_key_base64",
                default="",
            )
            or ""
        )
        if not encrypted_query or not encryption_key:
            if existing is not None:
                return await self._canonical_existing(existing, media, kind)
            safe = canonical_media_input(media, include_candidate=True)
            safe["available"] = False
            safe["native_input_available"] = False
            safe["error"] = "media has no downloadable managed reference"
            return safe

        try:
            payload = await self._download(media, encrypted_query, encryption_key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Download failures are retryable ingress failures.  The monitor
            # must not advance its durable cursor past media it could not own.
            raise AttachmentError("WeChat media download failed") from exc
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise AttachmentError("WeChat media downloader returned non-binary data")
        filename = str(
            _value(media, "filename", "file_name", "name", default="") or ""
        )
        declared_mime = str(
            _value(media, "mime_type", "mime", "content_type", default="") or ""
        )
        stored = await self.attachment_store.aput_bytes_idempotent(
            bytes(payload),
            filename=filename,
            mime_type=declared_mime,
            attachment_id=attachment_id,
        )
        registered = await self._register_attachment(
            stored,
            kind=kind,
            metadata={
                "filename": stored.filename,
                "channel": envelope.channel,
                "bot_id": envelope.bot_id,
                "external_user_id": envelope.external_user_id,
                "session_id": envelope.session_id or DEFAULT_SESSION_ID,
                "source_message_id": envelope.external_message_id,
                "source_ordinal": int(ordinal),
            },
        )
        return self._canonical_record(registered or stored, media, kind)

    async def _get_attachment(self, attachment_id: str) -> StoredAttachment | None:
        try:
            state = await _invoke_compatible(
                self.metadata_store,
                ("get_attachment_state",),
                positional=(attachment_id,),
            )
            if state is not None and str(state) != "ready":
                raise AttachmentError("managed attachment is unavailable")
        except AttributeError:
            pass
        try:
            return await _invoke_compatible(
                self.metadata_store,
                ("get_attachment",),
                positional=(attachment_id,),
            )
        except AttributeError:
            return None

    async def _register_attachment(
        self,
        attachment: StoredAttachment,
        *,
        kind: str,
        metadata: Mapping[str, Any],
    ) -> StoredAttachment:
        register = getattr(self.metadata_store, "register_attachment", None) or getattr(
            self.metadata_store, "add_attachment", None
        )
        if register is None:
            raise AttachmentError("runtime store cannot register managed attachments")
        result = await _invoke_compatible(
            self.metadata_store,
            ("register_attachment", "add_attachment"),
            positional=(attachment,),
            keyword={"kind": kind, "metadata": dict(metadata)},
        )
        return result if isinstance(result, StoredAttachment) else attachment

    async def _canonical_existing(
        self, existing: StoredAttachment | Mapping[str, Any] | Any, media: Any, kind: str
    ) -> dict[str, Any]:
        # Loading SQLite metadata before reading prevents a restarted file
        # store from blessing the current bytes with a newly computed checksum.
        authoritative = self.attachment_store.remember(existing)
        await self.attachment_store.aread_bytes(authoritative.attachment_id)
        return self._canonical_record(authoritative, media, kind)

    @staticmethod
    def _wire_fingerprint(media: Any) -> str:
        """Hash channel reference identity without retaining transport secrets."""
        return wire_media_fingerprint(media)

    @staticmethod
    def _legacy_promoter_wire_fingerprint(media: Any) -> str:
        """Reproduce the original promoter digest for old durable rows."""

        fields = {
            name: _value(media, *aliases, default="")
            for name, aliases in {
                "kind": ("kind", "type"),
                "remote_id": ("remote_id", "media_id", "id"),
                "encrypted_query_param": (
                    "encrypted_query_param",
                    "encrypt_query_param",
                ),
                "encryption_key": ("encryption_key", "aes_key"),
                "mime_type": ("mime_type", "mime"),
                "filename": ("filename", "file_name"),
                "size": ("size", "size_bytes"),
                "checksum": ("checksum", "sha256"),
            }.items()
        }
        encoded = json.dumps(fields, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _legacy_store_wire_fingerprint(media: Any) -> str:
        """Reproduce fingerprints written by pre-promotion store ingestion."""

        if isinstance(media, Mapping):
            fields = {
                str(name): media.get(name)
                for name in (
                    "kind",
                    "type",
                    "media_kind",
                    "remote_id",
                    "media_id",
                    "id",
                    "encrypted_query_param",
                    "encrypt_query_param",
                    "encryption_key",
                    "aes_key",
                    "mime_type",
                    "filename",
                    "file_name",
                    "size",
                    "size_bytes",
                    "checksum",
                    "sha256",
                )
            }
        else:
            fields = {"value": str(media or "")}
        encoded = json.dumps(
            fields,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _canonical_record(
        stored: StoredAttachment, media: Any, kind: str
    ) -> dict[str, Any]:
        value: dict[str, Any] = {
            "attachment_id": stored.attachment_id,
            "path": stored.path,
            "mime_type": stored.mime_type,
            "kind": kind,
            "filename": stored.filename,
            "size": stored.size,
            "checksum": stored.checksum,
            "available": True,
            # AttachmentStore derives this MIME from content signatures.  A
            # channel kind or image-looking filename alone must not promote
            # arbitrary bytes to a native SDK image input.
            "native_input_available": (
                kind == "image" and stored.mime_type.lower().startswith("image/")
            ),
        }
        if kind == "audio":
            value["candidate_text"] = str(
                _value(media, "candidate_text", default="") or ""
            )
            confidence = _value(media, "confidence", default=None)
            if confidence is not None:
                value["confidence"] = confidence
        return canonical_media_input(value, include_candidate=True)

    async def _download(
        self, media: Any, encrypted_query: str, encryption_key: str
    ) -> bytes:
        downloader = self.downloader

        def invoke() -> Any:
            try:
                signature = inspect.signature(downloader)
            except (TypeError, ValueError):
                return downloader(encrypted_query, encryption_key)
            keyword_values = {
                "media": media,
                "reference": media,
                "encrypted_query_param": encrypted_query,
                "encrypt_query_param": encrypted_query,
                "encryption_key": encryption_key,
                "aes_key": encryption_key,
                "aes_key_base64": encryption_key,
                # Bound the CDN ciphertext to the same plaintext limit that
                # the managed store will enforce after decryption.  Custom
                # downloaders that do not declare this keyword remain
                # compatible through signature filtering below.
                "max_size": int(self.attachment_store.max_file_size),
            }
            accepts_var_kw = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            kwargs = (
                keyword_values
                if accepts_var_kw
                else {
                    name: value
                    for name, value in keyword_values.items()
                    if name in signature.parameters
                }
            )
            try:
                signature.bind(**kwargs)
            except TypeError:
                return downloader(encrypted_query, encryption_key)
            return downloader(**kwargs)

        if inspect.iscoroutinefunction(downloader):
            result = invoke()
        else:
            result = await asyncio.to_thread(invoke)
        return await _maybe_await(result)




















_ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|"
    r"\x9b[0-?]*[ -/]*[@-~]|"
    r"\x1b[@-Z\\-_])"
)
_PUBLIC_ERROR_URL_PATTERN = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]*://[^\s,)\]}>\"']+"
)
_PUBLIC_ERROR_AUTH_SCHEME_PATTERN = re.compile(
    r'''(?i)(?<![\w-])(?:"(?:bearer|basic)"|'(?:bearer|basic)'|'''
    r'''(?:bearer|basic))(?:\s*[:=]\s*|\s+)'''
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_PUBLIC_ERROR_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(?<![\w-])[\"']?"
    r"((?:[a-z0-9][a-z0-9_-]{0,63})?(?:"
    r"api[_-]?key|authorization|credential|password|secret|token))"
    r"[\"']?\s*[:=]\s*"
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_PUBLIC_ERROR_HOST_PATTERN = re.compile(
    r"(?i)(?<![\w@/.-])(?:"
    r"\[(?=[0-9a-f:.%]*:)[0-9a-f:.]+(?:%[a-z0-9_.-]+)?\]|"
    r"localhost|"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?\.)+[a-z]{2,63}|"
    r"(?:\d{1,3}\.){3}\d{1,3}"
    r")(?::\d{1,5})?(?:/[^\s,)\]}>\"']*)?"
)




















_SYSTEM_ROLE_RESPONSE_PREFIX = "system role:\n"














































def _ask_task_matches(
    record: Any,
    envelope: InboundEnvelope,
    *,
    destination_agent_id: str,
    request_id: str,
) -> bool:
    """Validate that an `/ask` dedupe result belongs to this exact command."""

    if record is None or not _task_id(record) or not _task_belongs_to(record, envelope):
        return False
    target = _value(record, "reply_target", "target", default=None)
    if target is None:
        return False
    target_value = (
        target.to_dict()
        if hasattr(target, "to_dict")
        else target.as_dict()
        if hasattr(target, "as_dict")
        else target
    )
    expected_scope = (
        (("channel",), envelope.channel),
        (("bot_id",), envelope.bot_id),
        (("external_user_id", "user_id"), envelope.external_user_id),
        (("session_id",), envelope.session_id or DEFAULT_SESSION_ID),
        (
            ("source_message_id", "external_message_id", "message_id"),
            envelope.external_message_id,
        ),
    )
    for fields, expected in expected_scope:
        actual = _value(target_value, *fields, default=None)
        if actual is None or str(actual or "") != str(expected or ""):
            return False
    if _task_dedupe_key(record) != f"command-ask:{request_id}":
        return False
    if str(_value(record, "request_id", default="") or "") != request_id:
        return False
    stored_agent = str(_value(record, "agent_id", default="") or "")
    return stored_agent.casefold() == str(destination_agent_id or "").casefold()


def _command_receipt_matches(
    receipt: Any,
    command: ChannelCommand,
    envelope: InboundEnvelope,
) -> bool:
    """Validate a legacy receipt before accepting its collision-prone key."""

    if receipt is None:
        return False
    expected = (
        ("channel", envelope.channel),
        ("bot_id", envelope.bot_id),
        ("external_user_id", envelope.external_user_id),
        ("session_id", envelope.session_id or DEFAULT_SESSION_ID),
        ("external_message_id", envelope.external_message_id),
        ("command_name", command.name),
        ("command_text", envelope.text),
    )
    if any(
        str(_value(receipt, field, default="") or "") != str(value or "")
        for field, value in expected
    ):
        return False
    stored_args = tuple(
        str(value)
        for value in (_value(receipt, "command_args", default=()) or ())
    )
    return stored_args == tuple(str(value) for value in command.args)


def _command_projection_matches(
    projection: Any, envelope: InboundEnvelope
) -> bool:
    """Validate an old command outbox row against its full reply target."""

    target = _value(projection, "reply_target", "target", default=None)
    if target is None:
        return False
    if hasattr(target, "to_dict"):
        target = target.to_dict()
    elif hasattr(target, "as_dict"):
        target = target.as_dict()
    expected = (
        ("channel", envelope.channel),
        ("bot_id", envelope.bot_id),
        ("external_user_id", envelope.external_user_id),
        ("session_id", envelope.session_id or DEFAULT_SESSION_ID),
        ("source_message_id", envelope.external_message_id),
    )
    return all(
        str(_value(target, field, default="") or "") == str(value or "")
        for field, value in expected
    )


def _trusted_principal_mapping_revision(
    value: Any,
    *,
    allow_unmapped: bool = False,
) -> int | None:
    """Normalize a revision read from the store-owned identity snapshot."""

    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise RuntimeError("principal mapping revision is invalid")
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("principal mapping revision is invalid") from exc
    if revision == 0 and allow_unmapped:
        return 0
    if revision <= 0:
        raise RuntimeError("principal mapping revision is invalid")
    return revision


class WeChatGateway:
    """Normalize and durably accept WeChat messages.

    ``runtime`` should normally be a TaskManager exposing ``accept_inbound``.
    A separate ``command_router`` may be supplied for immediate control
    commands.  The gateway returns after durable acceptance; it never waits
    for an Agent turn to finish.
    """

    def __init__(
        self,
        runtime: InboundAcceptor | Any,
        *,
        bot_id: str = "",
        session_resolver: Callable[[str], str] | None = None,
        agent_resolver: Callable[[str], str] | None = None,
        command_router: CommandHandler | None = None,
        attachment_store: AttachmentStore | None = None,
        media_downloader: Callable[..., Any] | None = None,
        shell_cwd: str | Path | None = None,
        skill_resolver: Callable[..., Any] | None = None,
    ) -> None:
        self.runtime = runtime
        self.bot_id = bot_id
        self.session_resolver = session_resolver
        self.agent_resolver = agent_resolver
        self.skill_resolver = skill_resolver
        self.command_router = command_router or MVPCommandRouter(
            runtime, shell_cwd=shell_cwd
        )
        metadata_store = getattr(runtime, "store", runtime)
        self.media_promoter = (
            WeChatInboundMediaPromoter(
                attachment_store,
                metadata_store,
                downloader=media_downloader,
            )
            if attachment_store is not None
            else None
        )

    async def _resolve_skill_snapshot(
        self,
        invocation: SkillInvocation,
        envelope: InboundEnvelope,
    ) -> dict[str, Any] | None:
        """Resolve a skill through a trusted runtime/SDK catalog."""

        resolved_agent = str(envelope.agent_id or DEFAULT_AGENT_ID)
        route_reader = getattr(self.runtime, "get_active_agent", None)
        if callable(route_reader):
            try:
                selected = await _maybe_await(
                    route_reader(
                        channel=envelope.channel,
                        bot_id=envelope.bot_id,
                        external_user_id=envelope.external_user_id,
                        session_id=envelope.session_id,
                    )
                )
                selected = _value(selected, "agent_id", "id", default=selected)
                if selected:
                    resolved_agent = str(selected)
            except Exception:
                logger.debug(
                    "could not resolve Agent for skill discovery",
                    exc_info=True,
                )
        resolution_scope = {
            "agent_id": resolved_agent,
            "channel": envelope.channel,
            "bot_id": envelope.bot_id,
            "external_user_id": envelope.external_user_id,
            "session_id": envelope.session_id,
        }

        targets: list[Any] = []
        if self.skill_resolver is not None:
            targets.append(self.skill_resolver)
        router_resolver = getattr(self.command_router, "resolve_skill", None)
        if router_resolver is not None:
            targets.append(router_resolver)
        runtime_resolver = getattr(self.runtime, "resolve_skill", None)
        if runtime_resolver is not None:
            targets.append(runtime_resolver)
        runtime = getattr(self.runtime, "registry", None)
        if runtime is not None:
            targets.append(getattr(runtime, "resolve_skill", None))

        for target in targets:
            if target is None:
                continue
            try:
                result = target(
                    invocation.name,
                    **resolution_scope,
                    refresh=False,
                )
            except TypeError:
                try:
                    result = target(invocation.name)
                except TypeError:
                    continue
            result = await _maybe_await(result)
            if result is None:
                continue
            if isinstance(result, Mapping) and (
                result.get("path") or result.get("skill_id") or result.get("name")
            ):
                try:
                    definition = normalize_skill(result)
                except (TypeError, ValueError):
                    continue
                if definition.skill_id == invocation.skill_id:
                    return definition.snapshot()
                # A resolver that returns a descriptor for a different name
                # must not let that path be selected by the channel.
                continue
            if isinstance(result, Mapping) and (
                "skills" in result or "data" in result
            ):
                definition = find_skill(
                    result.get("skills", result.get("data", ())),
                    invocation.name,
                )
                if definition is not None:
                    return definition.snapshot()
                continue
            if isinstance(result, Sequence) and not isinstance(
                result, (str, bytes, bytearray)
            ):
                definition = find_skill(result, invocation.name)
                if definition is not None:
                    return definition.snapshot()
            else:
                # SDKs and registry facades may return a typed descriptor
                # directly rather than a mapping.  Normalize it through the
                # same allowlisted fields and enforce selector identity.
                try:
                    definition = normalize_skill(result)
                except (TypeError, ValueError):
                    continue
                if definition.skill_id == invocation.skill_id:
                    return definition.snapshot()

        # A resolver may expose only a catalog.  This fallback is useful for
        # narrow test doubles and compatibility facades.
        for target in (
            self.command_router,
            self.runtime,
            getattr(self.runtime, "registry", None),
        ):
            lister = getattr(target, "list_skills", None) if target is not None else None
            if lister is None:
                continue
            try:
                result = lister(**resolution_scope, refresh=False)
            except TypeError:
                try:
                    result = lister()
                except TypeError:
                    continue
            result = await _maybe_await(result)
            definition = find_skill(
                result.get("skills", result.get("data", ()))
                if isinstance(result, Mapping)
                else result,
                invocation.name,
            )
            if definition is not None:
                return definition.snapshot()
        return None

    def normalize(
        self, message: WeixinMessage, *, bot_id: str = ""
    ) -> InboundEnvelope | None:
        user_id = message.from_user_id
        session_id = (
            self.session_resolver(user_id)
            if self.session_resolver is not None
            else DEFAULT_SESSION_ID
        )
        agent_id = (
            self.agent_resolver(user_id)
            if self.agent_resolver is not None
            else DEFAULT_AGENT_ID
        )
        return normalize_message(
            message,
            bot_id=bot_id or self.bot_id,
            session_id=session_id,
            agent_id=agent_id,
        )

    # Descriptive aliases used by channel integrations and tests.
    normalize_message = normalize

    async def accept(
        self, message: WeixinMessage, *, bot_id: str = ""
    ) -> Acceptance | None:
        """Persist one message and enqueue its task, without awaiting execution."""

        envelope = self.normalize(message, bot_id=bot_id)
        if envelope is None:
            return None

        # Route lookup belongs to the TaskManager's owning asyncio loop.  The
        # blocking Monitor thread only submits this coroutine and never reads
        # loop-owned route state directly.
        if self.agent_resolver is None:
            resolver = getattr(self.runtime, "_resolve_active_agent", None)
            if resolver is not None:
                resolved = str(
                    await _maybe_await(resolver(envelope)) or envelope.agent_id
                )
                if resolved and resolved != envelope.agent_id:
                    envelope = replace(
                        envelope,
                        agent_id=resolved,
                        conversation_id=conversation_id_for(
                            bot_id=envelope.bot_id,
                            external_user_id=envelope.external_user_id,
                            session_id=envelope.session_id,
                            agent_id=resolved,
                        ),
                    )

        trusted_media_wire_fingerprints: tuple[str, ...] | None = None
        if self.media_promoter is not None:
            envelope = await self.media_promoter.promote(envelope)
            promoted_raw = envelope.raw
            if isinstance(promoted_raw, Mapping):
                marker = promoted_raw.get("__media_wire_fingerprints")
                if isinstance(marker, (list, tuple)):
                    # The promoter generated this marker from the immutable
                    # wire object before replacing it with managed metadata.
                    # Pass it out-of-band so SQLite never has to trust a
                    # caller-supplied reserved payload field.
                    trusted_media_wire_fingerprints = tuple(
                        str(value) for value in marker
                    )

        command = parse_command(envelope.text)
        skill_invocation: SkillInvocation | None = None
        skill_snapshot: dict[str, Any] | None = None
        skill_error = ""
        if command is None:
            try:
                skill_invocation = parse_skill_invocation(envelope.text)
            except SkillSyntaxError as exc:
                skill_error = str(exc)
            if skill_invocation is not None:
                try:
                    skill_snapshot = await self._resolve_skill_snapshot(
                        skill_invocation, envelope
                    )
                except Exception:
                    logger.warning(
                        "could not resolve skill %s",
                        skill_invocation.name,
                        exc_info=True,
                    )
                    skill_error = "skills unavailable; try again later"
                if skill_snapshot is None and not skill_error:
                    skill_error = (
                        f"unknown skill: ${skill_invocation.skill_id}. "
                        "use /skills to see available skills"
                    )
        if skill_error:
            # Treat an invalid dollar selector as a non-task control input.  It
            # is persisted and projected through the same durable command
            # receipt/outbox path as slash commands, so redelivery cannot
            # accidentally turn it into ordinary Agent work.
            command = ChannelCommand(
                name="__skill_error__",
                args=(skill_error,),
                raw=envelope.text,
            )
        media_values = []
        if isinstance(envelope.raw, Mapping):
            media_values = list(envelope.raw.get("media") or ())
        uploaded_files = [
            item
            for item in media_values
            if isinstance(item, Mapping)
            and str(item.get("kind", "")).strip().lower() == "file"
        ]
        if uploaded_files:
            # A file upload is a staging action, not an instruction.  Media
            # promotion has already copied the bytes into managed storage;
            # persist that inbound ownership without dispatching an Agent
            # until the user says what should be done with the file.
            command = ChannelCommand(
                name="__file_upload__",
                args=(),
                raw=envelope.text,
            )
            skill_invocation = None
            skill_snapshot = None
            skill_error = ""
        audio_items = [
            item
            for item in media_values
            if isinstance(item, Mapping)
            and str(item.get("kind", "")).strip().lower() == "audio"
        ]
        if (
            audio_items
            and not envelope.text.strip()
            and any(not str(item.get("candidate_text", "") or "").strip() for item in audio_items)
            and not uploaded_files
        ):
            # A voice bubble without WeChat's speech-to-text result is not a
            # valid task instruction.  Keep the media durable, but ask for a
            # retry instead of enqueueing an empty prompt.
            command = ChannelCommand(
                name="__audio_error__",
                args=(
                    "I couldn't transcribe that audio; please resend it or type the instruction.",
                ),
                raw=envelope.text,
            )
            skill_invocation = None
            skill_snapshot = None
            skill_error = ""
        # Control/read commands are durably recorded but must not become
        # ordinary Agent work.  A TaskManager may ignore ``create_task`` while
        # the SQLite store honors it atomically.
        # Every slash-prefixed input belongs to the control plane.  Unknown
        # commands receive a durable acknowledgement from the router instead
        # of being treated as a prompt for an Agent task.
        is_control_command = command is not None
        # Preserve normalized media references in the immutable task snapshot.
        # ``SQLiteStore.accept_inbound`` derives inputs from the inbound
        # payload when no explicit value is supplied, but TaskManager/facade
        # calls commonly pass ``inputs`` explicitly.  Supplying text alone in
        # that path would silently erase image/file/video context.
        task_inputs: dict[str, Any] = {
            "text": (
                skill_invocation.description
                if skill_invocation is not None and skill_snapshot is not None
                else envelope.text
            )
        }
        if media_values:
            task_inputs["media"] = list(canonical_media_inputs(media_values))
        acceptance_kwargs: dict[str, Any] = {
            "conversation_id": envelope.conversation_id,
            "reply_target": envelope.reply_target.to_dict(),
            "inputs": task_inputs,
            # Voice bubbles have already been transcribed by WeChat and their
            # transcript is in ``envelope.text``.  They follow the ordinary
            # task path immediately; no confirmation state machine is needed.
            "create_task": not is_control_command,
        }
        if skill_snapshot is not None:
            acceptance_kwargs["skill_snapshot"] = dict(skill_snapshot)
        if trusted_media_wire_fingerprints is not None:
            acceptance_kwargs["_trusted_media_wire_fingerprints"] = (
                trusted_media_wire_fingerprints
            )
        # TaskManager resolves the front Agent from its loop-owned route.  Do
        # not force the envelope's static ``codex`` default in that case.
        # A bare SQLiteStore (or a small fake) still needs an explicit Agent
        # snapshot for task creation.
        if not hasattr(self.runtime, "active_agent_for"):
            acceptance_kwargs["agent_id"] = envelope.agent_id
            if is_control_command:
                # SQLiteStore can persist this directly when the gateway is
                # used without TaskManager.  TaskManager computes the richer
                # policy snapshot itself and filters this compatibility field.
                acceptance_kwargs["command_snapshot"] = {
                    "agent_id": envelope.agent_id,
                    "conversation_id": envelope.conversation_id,
                }

        try:
            result = await _invoke_compatible(
                self.runtime,
                ("accept_inbound", "submit_inbound", "enqueue_inbound"),
                # Both TaskManager and SQLiteStore accept channel-neutral objects
                # by attribute (the store filters channel-specific fields at its
                # boundary).  Keeping the immutable envelope here preserves the
                # original reply target while still allowing lightweight mapping
                # fakes through the compatibility shim.
                positional=(envelope,),
                keyword=acceptance_kwargs,
            )
        except QueueFullError:
            # Capacity rejection must advance the inbound cursor and produce a
            # replay-stable reply without creating a task or consuming another
            # ready sequence.  Re-run only the inbound/control-plane half after
            # the failed all-or-nothing task transaction has rolled back.
            command = ChannelCommand(
                name="__queue_full__",
                args=(),
                raw=envelope.text,
            )
            skill_invocation = None
            skill_snapshot = None
            skill_error = ""
            rejection_kwargs = dict(acceptance_kwargs)
            rejection_kwargs["create_task"] = False
            rejection_kwargs.pop("skill_snapshot", None)
            rejection_kwargs["_synthetic_command_name"] = "__queue_full__"
            if not hasattr(self.runtime, "active_agent_for"):
                rejection_kwargs["command_snapshot"] = {
                    "agent_id": envelope.agent_id,
                    "conversation_id": envelope.conversation_id,
                    "synthetic_command_name": "__queue_full__",
                }
            result = await _invoke_compatible(
                self.runtime,
                ("accept_inbound", "submit_inbound", "enqueue_inbound"),
                positional=(envelope,),
                keyword=rejection_kwargs,
            )
        # Restore immutable ingress fields for command handling.  The inbound
        # row is the source of truth on both first delivery and duplicate
        # redelivery; a live `/agent` switch or rolling context-token refresh
        # between those attempts must not reinterpret the command's source or
        # reply target.
        inbound_record = _value(result, "inbound", default=result)
        persisted_channel = _value(inbound_record, "channel", default=None)
        persisted_bot = _value(inbound_record, "bot_id", default=None)
        persisted_user = _value(
            inbound_record, "external_user_id", "user_id", default=None
        )
        persisted_message = _value(
            inbound_record,
            "external_message_id",
            "source_message_id",
            "message_id",
            default=None,
        )
        persisted_session = _value(inbound_record, "session_id", default=None)
        persisted_sequence = _value(
            inbound_record, "source_sequence", "seq", default=None
        )
        persisted_context = _value(
            inbound_record, "context_token", "contextToken", default=None
        )
        persisted_text = _value(inbound_record, "text", default=None)
        persisted_received = _value(inbound_record, "received_at", default=None)
        persisted_payload = _value(inbound_record, "payload", default=None)
        persisted_identity = _value(
            inbound_record, "identity_snapshot", default=None
        )
        principal_snapshot = (
            persisted_identity.get("principal", {})
            if isinstance(persisted_identity, Mapping)
            else {}
        )
        if not isinstance(principal_snapshot, Mapping):
            principal_snapshot = {}
        persisted_principal = _value(
            inbound_record, "principal_id", default=None
        )
        persisted_principal_account = _value(
            inbound_record, "principal_account_id", default=None
        )
        persisted_mapping_revision = _value(
            inbound_record,
            "principal_mapping_revision",
            "mapping_revision",
            default=None,
        )
        if persisted_mapping_revision is None:
            persisted_mapping_revision = principal_snapshot.get(
                "mapping_revision"
            )
        durable_unmapped = bool(
            principal_snapshot
            and str(principal_snapshot.get("source") or "") == "unmapped"
            and not principal_snapshot.get("principal_id")
            and not principal_snapshot.get("principal_account_id")
        )
        if persisted_mapping_revision is None and durable_unmapped:
            persisted_mapping_revision = 0
        if any(
            value is not None
            for value in (
                persisted_channel,
                persisted_bot,
                persisted_user,
                persisted_message,
                persisted_session,
                persisted_sequence,
                persisted_context,
                persisted_text,
                persisted_received,
                persisted_payload,
                persisted_identity,
                persisted_principal,
                persisted_principal_account,
                persisted_mapping_revision,
            )
        ):
            raw = (
                dict(persisted_payload)
                if isinstance(persisted_payload, Mapping)
                else dict(envelope.raw or {})
            )
            source_sequence = persisted_sequence
            if source_sequence is not None:
                try:
                    source_sequence = int(source_sequence)
                except (TypeError, ValueError):
                    source_sequence = envelope.source_sequence
            envelope = replace(
                envelope,
                channel=str(persisted_channel or envelope.channel),
                bot_id=str(persisted_bot or envelope.bot_id),
                external_user_id=str(persisted_user or envelope.external_user_id),
                external_message_id=str(
                    persisted_message or envelope.external_message_id
                ),
                text=str(persisted_text if persisted_text is not None else envelope.text),
                session_id=str(persisted_session or envelope.session_id or DEFAULT_SESSION_ID),
                source_sequence=source_sequence,
                context_token=(
                    str(persisted_context)
                    if persisted_context is not None
                    else envelope.context_token
                ),
                received_at=(
                    str(persisted_received)
                    if persisted_received is not None
                    else envelope.received_at
                ),
                principal_id=str(
                    persisted_principal
                    or principal_snapshot.get("principal_id")
                    or ""
                ),
                principal_account_id=str(
                    persisted_principal_account
                    or principal_snapshot.get("principal_account_id")
                    or ""
                ),
                principal_mapping_revision=(
                    _trusted_principal_mapping_revision(
                        persisted_mapping_revision,
                        allow_unmapped=durable_unmapped,
                    )
                ),
                raw=raw,
            )
        persisted_payload = _value(inbound_record, "payload", default={})
        persisted_snapshot = (
            persisted_payload.get("__command_snapshot")
            if isinstance(persisted_payload, Mapping)
            else None
        )
        if isinstance(persisted_snapshot, Mapping):
            raw = dict(envelope.raw or {})
            raw["__command_snapshot"] = dict(persisted_snapshot)
            snapshot_agent = str(
                persisted_snapshot.get("agent_id") or envelope.agent_id or DEFAULT_AGENT_ID
            )
            snapshot_conversation = str(
                persisted_snapshot.get("conversation_id")
                or envelope.conversation_id
                or ""
            )
            envelope = replace(
                envelope,
                agent_id=snapshot_agent,
                conversation_id=snapshot_conversation,
                raw=raw,
            )
            synthetic_command_name = str(
                persisted_snapshot.get("synthetic_command_name") or ""
            )
            if command is None and synthetic_command_name == "__queue_full__":
                command = ChannelCommand(
                    name="__queue_full__",
                    args=(),
                    raw=envelope.text,
                )
        duplicate = _is_duplicate(result)
        accepted = _is_accepted(result)

        # A skill task may be redelivered after the live catalog changes (or
        # becomes temporarily unavailable).  The durable task row already
        # contains the immutable skill snapshot; recover it before turning a
        # failed live lookup into a synthetic ``__skill_error__`` command.
        # This keeps duplicate delivery task-only and prevents a misleading
        # "unknown skill" response from being projected for work that was
        # accepted successfully on the first delivery.
        if duplicate and skill_invocation is not None:
            existing_task = _value(result, "task", default=None)
            existing_inputs = _value(existing_task, "inputs", default={})
            stored_skill = _value(existing_inputs, "skill", default=None)
            if not isinstance(stored_skill, Mapping):
                existing_metadata = _value(existing_task, "metadata", default={})
                stored_skill = _value(existing_metadata, "skill", default=None)
            stored_name = _value(stored_skill, "name", "skill_id", "id", default="")
            if (
                isinstance(stored_skill, Mapping)
                and str(stored_name or "").strip().casefold()
                == skill_invocation.skill_id
            ):
                skill_snapshot = dict(stored_skill)
                skill_error = ""
                command = None

        command_response = ""
        command_id = command_delivery_id(envelope) if command is not None else ""
        presentation_ids: tuple[str, ...] = ()
        response_fragments: tuple[Mapping[str, Any], ...] = ()
        response_agent_id = envelope.agent_id
        raw_candidate_ids = _value(
            result, "confirmation_ids", "candidate_ids", default=()
        ) or ()
        if isinstance(raw_candidate_ids, str):
            raw_candidate_ids = (raw_candidate_ids,)
        candidate_ids: list[str] = [
            str(identifier) for identifier in raw_candidate_ids if identifier
        ]
        if accepted and command is not None and self.command_router is not None:
            # A monitor can advance its cursor after the inbound row commits,
            # then lose the process before the command router/outbox step.  On
            # redelivery, recover an already-projected response by its stable
            # command outbox ID.  A durable command receipt covers the earlier
            # gap between applying a control effect and projecting that
            # response: completed responses are replayed exactly, while an
            # interrupted/unknown effect is never blindly executed again.
            existing_projection = None
            if duplicate:
                for target in (self.runtime, getattr(self.runtime, "store", None)):
                    if target is None:
                        continue
                    for candidate_id in command_delivery_id_candidates(envelope):
                        try:
                            existing_projection = await _invoke_compatible(
                                target,
                                (
                                    "get_outbox_item",
                                    "get_user_outbox_item",
                                    "get_delivery",
                                ),
                                positional=(candidate_id,),
                            )
                        except AttributeError:
                            break
                        if existing_projection is not None:
                            if candidate_id == command_delivery_id(envelope) or (
                                _command_projection_matches(
                                    existing_projection, envelope
                                )
                            ):
                                command_id = candidate_id
                                break
                            existing_projection = None
                    if existing_projection is not None:
                        break
                if existing_projection is not None:
                    command_response = str(
                        _value(existing_projection, "content", "text", default="")
                        or ""
                    )
                    stored_agent = _value(
                        existing_projection, "agent_id", default=response_agent_id
                    )
                    if stored_agent:
                        response_agent_id = str(stored_agent)
                    # ``user_outbox.content`` is the immutable payload of one
                    # transport fragment, not necessarily the complete command
                    # response.  Recover the full receipt when available so a
                    # long command (notably ``/help``) is not replayed as a new,
                    # truncated candidate that conflicts with its first
                    # projection.
                    get_receipt_names = ("get_command_receipt", "get_command")
                    get_receipt_target = _capability_target(
                        self.runtime, get_receipt_names
                    )
                    if get_receipt_target is not None:
                        replay_receipt = await _invoke_compatible(
                            get_receipt_target,
                            get_receipt_names,
                            positional=(command_id,),
                        )
                        if (
                            replay_receipt is not None
                            and str(
                                _value(replay_receipt, "state", default="") or ""
                            ).lower()
                            == "completed"
                            and (
                                command_id == command_delivery_id(envelope)
                                or _command_receipt_matches(
                                    replay_receipt, command, envelope
                                )
                            )
                        ):
                            command_response = str(
                                _value(
                                    replay_receipt,
                                    "response_text",
                                    "content",
                                    default=command_response,
                                )
                                or ""
                            )
                            presentation_ids = tuple(
                                str(identifier)
                                for identifier in (
                                    _value(
                                        replay_receipt,
                                        "presentation_ids",
                                        default=(),
                                    )
                                    or ()
                                )
                                if identifier
                            )
                            response_fragments = tuple(
                                dict(fragment)
                                for fragment in (
                                    _value(
                                        replay_receipt,
                                        "response_fragments",
                                        default=(),
                                    )
                                    or ()
                                )
                                if isinstance(fragment, Mapping)
                            )
                            stored_agent = _value(
                                replay_receipt,
                                "response_agent_id",
                                "agent_id",
                                default=response_agent_id,
                            )
                            if stored_agent:
                                response_agent_id = str(stored_agent)
            if existing_projection is None:
                begin_names = (
                    "begin_command_receipt",
                    "reserve_command",
                    "begin_command",
                )
                receipt_target = _capability_target(self.runtime, begin_names)
                owns_receipt = receipt_target is None

                async def interrupt_owned_receipt() -> None:
                    if receipt_target is None:
                        return
                    interrupt_names = (
                        "interrupt_command_receipt",
                        "interrupt_command",
                    )
                    interrupt_target = _capability_target(
                        self.runtime, interrupt_names
                    )
                    if interrupt_target is None:
                        raise RuntimeError(
                            "failed command receipt cannot be terminalized"
                        )
                    try:
                        await _invoke_compatible(
                            interrupt_target,
                            interrupt_names,
                            positional=(command_id,),
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            "failed command receipt was not durably terminalized"
                        ) from exc

                async def drain_owned_receipt_interrupt() -> None:
                    # The owner is already handling cancellation, and a
                    # shutdown path may cancel it again while SQLite is
                    # terminalizing the receipt. Keep cleanup in its own task
                    # and absorb repeated caller cancellation until the durable
                    # transition has actually settled.
                    terminalization = asyncio.create_task(
                        interrupt_owned_receipt()
                    )
                    cancelled_during_cleanup = False
                    while not terminalization.done():
                        try:
                            await asyncio.shield(terminalization)
                        except asyncio.CancelledError:
                            cancelled_during_cleanup = True
                            continue
                    terminalization.result()
                    if cancelled_during_cleanup:
                        raise asyncio.CancelledError

                if receipt_target is not None:
                    get_receipt_names = ("get_command_receipt", "get_command")
                    get_receipt_target = _capability_target(
                        self.runtime, get_receipt_names
                    )
                    receipt = None
                    if duplicate and get_receipt_target is not None:
                        for candidate_id in command_delivery_id_candidates(envelope):
                            receipt = await _invoke_compatible(
                                get_receipt_target,
                                get_receipt_names,
                                positional=(candidate_id,),
                            )
                            if receipt is not None:
                                if candidate_id == command_delivery_id(envelope) or (
                                    _command_receipt_matches(
                                        receipt, command, envelope
                                    )
                                ):
                                    command_id = candidate_id
                                    break
                                receipt = None
                    try:
                        if receipt is None:
                            (
                                receipt,
                                cancelled_during_reservation,
                            ) = await _drain_shielded_operation(
                                _invoke_compatible(
                                    receipt_target,
                                    begin_names,
                                    positional=(command_id,),
                                    keyword={
                                        "channel": envelope.channel,
                                        "bot_id": envelope.bot_id,
                                        "external_user_id": envelope.external_user_id,
                                        "session_id": envelope.session_id,
                                        "external_message_id": envelope.external_message_id,
                                        "command_name": command.name,
                                        "command_args": command.args,
                                        "command_text": envelope.text,
                                    },
                                )
                            )
                        else:
                            cancelled_during_reservation = False
                    except Exception as exc:
                        raise RuntimeError(
                            "command execution was not durably reserved"
                        ) from exc
                    receipt_state = str(
                        _value(receipt, "state", default="") or ""
                    ).lower()
                    owns_receipt = bool(
                        _value(receipt, "created", "reserved", default=False)
                    )
                    if cancelled_during_reservation:
                        if owns_receipt and receipt_state == "started":
                            await drain_owned_receipt_interrupt()
                        raise asyncio.CancelledError
                    if receipt_state == "completed":
                        command_response = str(
                            _value(receipt, "response_text", "content", default="")
                            or ""
                        )
                        presentation_ids = tuple(
                            str(identifier)
                            for identifier in (
                                _value(receipt, "presentation_ids", default=()) or ()
                            )
                            if identifier
                        )
                        response_fragments = tuple(
                            dict(fragment)
                            for fragment in (
                                _value(
                                    receipt,
                                    "response_fragments",
                                    default=(),
                                )
                                or ()
                            )
                            if isinstance(fragment, Mapping)
                        )
                        stored_agent = _value(
                            receipt,
                            "response_agent_id",
                            "agent_id",
                            default=response_agent_id,
                        )
                        if stored_agent:
                            response_agent_id = str(stored_agent)
                    elif receipt_state == "interrupted":
                        if command.name == "recv" and not command.args:
                            # `/recv` is a pure idempotent projection.  Its
                            # source key is the exact inbound command scope,
                            # so reopening after a crash reuses the committed
                            # batch and can never drain the following batch.
                            reopen_names = (
                                "reopen_interrupted_command_receipt",
                                "reopen_command_receipt",
                            )
                            reopen_target = _capability_target(
                                self.runtime, reopen_names
                            )
                            if reopen_target is None:
                                command_response = COMMAND_INTERRUPTED_RESPONSE
                            else:
                                (
                                    reopened,
                                    cancelled_during_reopen,
                                ) = await _drain_shielded_operation(
                                    _invoke_compatible(
                                        reopen_target,
                                        reopen_names,
                                        positional=(command_id,),
                                        keyword={"expected_command_name": "recv"},
                                    )
                                )
                                owns_receipt = bool(
                                    _value(
                                        reopened,
                                        "reopened",
                                        "created",
                                        "reserved",
                                        default=False,
                                    )
                                )
                                if cancelled_during_reopen:
                                    if owns_receipt:
                                        await drain_owned_receipt_interrupt()
                                    raise asyncio.CancelledError
                                reopened_state = str(
                                    _value(reopened, "state", default="") or ""
                                ).lower()
                                if not owns_receipt:
                                    if reopened_state == "completed":
                                        command_response = str(
                                            _value(
                                                reopened,
                                                "response_text",
                                                "content",
                                                default="",
                                            )
                                            or ""
                                        )
                                    elif reopened_state == "interrupted":
                                        command_response = COMMAND_INTERRUPTED_RESPONSE
                        elif command.name == "ask" and command.args:
                            destination = command.args[0]
                            task = None
                            request_id = command_request_id(envelope)
                            task_target = _capability_target(
                                self.runtime,
                                ("get_task_by_dedupe",),
                            )
                            if task_target is not None:
                                for request_id in command_request_id_candidates(
                                    envelope
                                ):
                                    task = await _invoke_compatible(
                                        task_target,
                                        ("get_task_by_dedupe",),
                                        positional=(f"command-ask:{request_id}",),
                                    )
                                    if task is not None:
                                        break
                            else:
                                request_id = command_request_id(envelope)
                            task_matches = _ask_task_matches(
                                task,
                                envelope,
                                destination_agent_id=destination,
                                request_id=request_id,
                            )
                            if task_matches:
                                command_response = (
                                    f"Agent task queued: {_task_id(task)}"
                                )
                            elif task is not None or task_target is None:
                                command_response = COMMAND_INTERRUPTED_RESPONSE
                            else:
                                reopen_names = (
                                    "reopen_interrupted_command_receipt",
                                    "reopen_command_receipt",
                                )
                                reopen_target = _capability_target(
                                    self.runtime, reopen_names
                                )
                                if reopen_target is not None:
                                    (
                                        reopened,
                                        cancelled_during_reopen,
                                    ) = await _drain_shielded_operation(
                                        _invoke_compatible(
                                            reopen_target,
                                            reopen_names,
                                            positional=(command_id,),
                                        )
                                    )
                                    owns_receipt = bool(
                                        _value(
                                            reopened,
                                            "reopened",
                                            "created",
                                            "reserved",
                                            default=False,
                                        )
                                    )
                                    if cancelled_during_reopen:
                                        if owns_receipt:
                                            await drain_owned_receipt_interrupt()
                                        raise asyncio.CancelledError
                                else:
                                    command_response = COMMAND_INTERRUPTED_RESPONSE
                            if task_matches:
                                complete_names = (
                                    "complete_command_receipt",
                                    "complete_command",
                                )
                                complete_target = _capability_target(
                                    self.runtime, complete_names
                                )
                                if complete_target is None:
                                    raise RuntimeError(
                                        "recovered command response cannot be completed"
                                    )
                                completed = await _invoke_compatible(
                                    complete_target,
                                    complete_names,
                                    positional=(command_id,),
                                    keyword={
                                        "response_text": command_response,
                                        "response_agent_id": response_agent_id,
                                        "presentation_ids": (),
                                        "allow_interrupted": True,
                                    },
                                )
                                command_response = str(
                                    _value(
                                        completed,
                                        "response_text",
                                        default=command_response,
                                    )
                                    or ""
                                )
                        else:
                            # Effects other than `/ask` may have happened
                            # before the process died and are not replayable.
                            command_response = COMMAND_INTERRUPTED_RESPONSE
                    elif receipt_state == "started" and not owns_receipt:
                        # Another live invocation owns this command.  Returning
                        # no response defers its stable outbox projection to
                        # that owner; publishing the interrupted placeholder
                        # here would conflict with the owner's real response.
                        pass
                    elif not owns_receipt:
                        raise RuntimeError(
                            f"command receipt has unsupported state: {receipt_state or 'unknown'}"
                        )

                if owns_receipt:
                    try:
                        response = await _invoke_compatible(
                            self.command_router,
                            ("handle_command",),
                            positional=(command, envelope),
                            keyword={
                                "command_id": (
                                    command_id if receipt_target is not None else ""
                                )
                            },
                        )
                        if response is None and command.name not in MVP_COMMANDS:
                            token = f"/{command.name}" if command.name else "/"
                            response = f"unknown command: {token}. try /help"
                        command_response = "" if response is None else str(response)
                        presentation_ids = tuple(
                            str(identifier)
                            for identifier in getattr(
                                response, "presentation_ids", ()
                            )
                            if identifier
                        )
                        response_fragments = tuple(
                            dict(fragment)
                            for fragment in (
                                getattr(response, "response_fragments", ()) or ()
                            )
                            if isinstance(fragment, Mapping)
                        )
                        # ``/agent B [profile]`` mutates the front route before building
                        # its acknowledgement. Resolve the route again for the
                        # delivery projection without executing the command a
                        # second time.
                        if command.name == "agent" and command.args:
                            route_scope = {
                                "channel": envelope.channel,
                                "bot_id": envelope.bot_id,
                                "external_user_id": envelope.external_user_id,
                                "session_id": envelope.session_id,
                            }
                            try:
                                post_switch = await _invoke_compatible(
                                    self.runtime,
                                    ("get_active_agent", "active_agent"),
                                    keyword=route_scope,
                                )
                                post_switch = _value(
                                    post_switch,
                                    "agent_id",
                                    "id",
                                    default=post_switch,
                                )
                                if post_switch:
                                    response_agent_id = str(post_switch)
                            except Exception:
                                logger.debug(
                                    "could not resolve post-switch Agent for command response",
                                    exc_info=True,
                                )

                        if receipt_target is not None:
                            complete_names = (
                                "complete_command_receipt",
                                "complete_command",
                            )
                            complete_target = _capability_target(
                                self.runtime, complete_names
                            )
                            if complete_target is None:
                                raise RuntimeError(
                                    "command response receipt cannot be completed"
                                )
                            try:
                                completed = await _invoke_compatible(
                                    complete_target,
                                    complete_names,
                                    positional=(command_id,),
                                    keyword={
                                        "response_text": command_response,
                                        "response_agent_id": response_agent_id,
                                        "presentation_ids": presentation_ids,
                                        "response_fragments": response_fragments,
                                    },
                                )
                            except Exception as exc:
                                raise RuntimeError(
                                    "command response was not durably recorded"
                                ) from exc
                            command_response = str(
                                _value(
                                    completed,
                                    "response_text",
                                    "content",
                                    default=command_response,
                                )
                                or ""
                            )
                            presentation_ids = tuple(
                                str(identifier)
                                for identifier in (
                                    _value(
                                        completed,
                                        "presentation_ids",
                                        default=presentation_ids,
                                    )
                                    or ()
                                )
                                if identifier
                            )
                            response_fragments = tuple(
                                dict(fragment)
                                for fragment in (
                                    _value(
                                        completed,
                                        "response_fragments",
                                        default=response_fragments,
                                    )
                                    or ()
                                )
                                if isinstance(fragment, Mapping)
                            )
                    except asyncio.CancelledError:
                        # Monitor timeouts and shutdown cancel this coroutine in
                        # process. This invocation owns the receipt, so no live
                        # peer can still publish it; terminalize before exposing
                        # cancellation to prevent duplicates from waiting on a
                        # permanently ``started`` command.
                        await drain_owned_receipt_interrupt()
                        raise
                    except Exception:
                        # Completion may have committed before its await raised.
                        # Interruption is idempotent for completed receipts, so
                        # always drain the same cleanup boundary before failing.
                        await drain_owned_receipt_interrupt()
                        raise
        if (
            not response_fragments
            and command is not None
            and command.name == "system"
            and command_response
        ):
            # A completed command receipt survives process restart without
            # Python-side ``CommandResponse`` attributes.  Reconstruct the
            # deterministic role presentation from its full immutable text.
            response_fragments = _system_role_fragments_from_response(
                command_response
            )
        return Acceptance(
            envelope=envelope,
            accepted=accepted,
            duplicate=duplicate,
            task_id=_task_id(result),
            command_response=command_response,
            response_agent_id=response_agent_id,
            response_delivery_id=command_id if command is not None else "",
            presentation_ids=presentation_ids,
            response_fragments=response_fragments,
            confirmation_ids=tuple(candidate_ids),
            raw_result=result,
        )

    async def handle_message(self, client: Any, message: WeixinMessage) -> Acceptance | None:
        """Monitor-compatible async handler including immediate command replies."""

        outcome = await self.accept(message, bot_id=getattr(client, "bot_id", ""))
        if outcome and outcome.command_response:
            response_agent_id = (
                outcome.response_agent_id or outcome.envelope.agent_id
            )
            # ``outbox_id`` and ``client_id`` are global identities in the
            # durable store/channel protocol.  ``command_delivery_id``
            # namespaces the external message ID with its full destination.
            response_delivery_id = (
                outcome.response_delivery_id
                or command_delivery_id(outcome.envelope)
            )
            delivery = UserDelivery(
                delivery_id=response_delivery_id,
                target=outcome.envelope.reply_target(),
                from_user_id=outcome.envelope.bot_id,
                content=outcome.command_response,
                client_id=(
                    command_client_id(outcome.envelope)
                    if response_delivery_id
                    == command_delivery_id(outcome.envelope)
                    else str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            "codex-wechat:command:"
                            + response_delivery_id.removeprefix("command:"),
                        )
                    )
                ),
            )
            enqueue_names = (
                "enqueue_user_outbox",
                "create_user_outbox",
                "add_user_outbox",
            )
            enqueue_target = _capability_target(self.runtime, enqueue_names)
            if enqueue_target is None:
                # A small legacy facade may have no durable user-outbox API at
                # all.  Retain its historical best-effort direct response;
                # once a facade advertises the capability, however, every
                # failure below is fatal to acceptance and must not bypass the
                # durable projection.
                if len(outcome.response_fragments) > 1:
                    raise RuntimeError(
                        "command response requires a fragment-aware outbox"
                    )
                receipt = await send_user_delivery_async(client, delivery)
                if not receipt.sent:
                    logger.warning(
                        "could not send command response for inbound %s: %s",
                        outcome.envelope.external_message_id,
                        receipt.error,
                    )
                return outcome

            # The outbox insert is the acceptance boundary for a command
            # response.  Do not turn a failed/ambiguous persistence attempt
            # into an untracked external message.
            persisted = None
            if outcome.duplicate:
                # The duplicate acceptance path already validated canonical or
                # legacy ownership before selecting ``response_delivery_id``.
                # Reuse that exact fragment row instead of feeding fragment 1
                # back through candidate normalization as if it were the full
                # command response.
                lookup_names = (
                    "get_outbox_item",
                    "get_user_outbox_item",
                    "get_delivery",
                )
                for target in (self.runtime, getattr(self.runtime, "store", None)):
                    if target is None:
                        continue
                    lookup_target = _capability_target(target, lookup_names)
                    if lookup_target is None:
                        continue
                    persisted = await _invoke_compatible(
                        lookup_target,
                        lookup_names,
                        positional=(delivery.delivery_id,),
                    )
                    if persisted is not None:
                        break
            if persisted is None:
                try:
                    fragment_projector = None
                    if outcome.response_fragments:
                        for target in (
                            self.runtime,
                            getattr(self.runtime, "store", None),
                        ):
                            if target is None:
                                continue
                            fragment_projector = _capability_target(
                                target, ("project_reply_candidate",)
                            )
                            if fragment_projector is not None:
                                break
                    if fragment_projector is not None:
                        persisted = await _invoke_compatible(
                            fragment_projector,
                            ("project_reply_candidate",),
                            keyword={
                                "target": delivery.target.to_dict(),
                                "source_key": f"outbox:{delivery.delivery_id}",
                                "content": outcome.command_response,
                                "fragments": outcome.response_fragments,
                                "agent_id": response_agent_id,
                                "foreground": True,
                                "outbox_id": delivery.delivery_id,
                                "client_id": delivery.client_id,
                                "contextless_client_id": (
                                    delivery.contextless_client_id or None
                                ),
                                "from_user_id": delivery.from_user_id,
                                "present_outbox_ids": outcome.presentation_ids,
                            },
                        )
                    elif len(outcome.response_fragments) > 1:
                        raise RuntimeError(
                            "command response store cannot preserve explicit fragments"
                        )
                    else:
                        persisted = await _invoke_compatible(
                            enqueue_target,
                            enqueue_names,
                            keyword={
                                "delivery": delivery.to_dict(),
                                # Keep the command response's resolved route on the
                                # durable projection.  This is normally the inbound
                                # Agent and is the post-switch Agent for ``/agent B``.
                                "agent_id": response_agent_id,
                                # A command acknowledgement is an interactive
                                # foreground response even when notifications are off.
                                "foreground": True,
                                "present_outbox_ids": outcome.presentation_ids,
                            },
                        )
                except Exception as exc:
                    logger.warning(
                        "command response outbox persistence failed for %s",
                        delivery.delivery_id,
                        exc_info=True,
                    )
                    raise RuntimeError(
                        "command response was not durably persisted"
                    ) from exc
            if persisted is False:
                raise RuntimeError("command response outbox persistence was rejected")

            converted = _delivery_from_outbox(persisted)
            if converted is not None:
                delivery = converted
            elif _has_field(persisted, "outbox_items"):
                # Scoped stores may return the complete projection when quota
                # retained the command without allocating a wire slot.  They
                # may also expose multiple fragments for a long command.  In
                # either case the durable projection, never the unsplit local
                # response above, is the authority for what may be sent.
                projected_items = tuple(
                    _value(persisted, "outbox_items", default=()) or ()
                )
                matches = tuple(
                    item
                    for item in projected_items
                    if str(
                        _value(
                            item,
                            "outbox_id",
                            "delivery_id",
                            "id",
                            default="",
                        )
                        or ""
                    )
                    == response_delivery_id
                )
                if len(matches) == 1:
                    converted = _delivery_from_outbox(matches[0])
                    if converted is None:
                        raise RuntimeError(
                            "command response projection has no sendable outbox"
                        )
                    delivery = converted
                elif not projected_items:
                    fragments = tuple(
                        _value(persisted, "fragments", default=()) or ()
                    )
                    states = {
                        str(
                            getattr(
                                _value(fragment, "state", default=""),
                                "value",
                                _value(fragment, "state", default=""),
                            )
                            or ""
                        )
                        for fragment in fragments
                    }
                    if fragments and states == {"deferred_quota"}:
                        # Persistence succeeded and `/recv` owns the future
                        # allocation.  There is intentionally nothing to claim
                        # or put on the wire for this inbound.
                        return outcome
                    raise RuntimeError(
                        "command response projection has no allocated outbox"
                    )
                else:
                    raise RuntimeError(
                        "command response projection conflicts with its delivery identity"
                    )

            claim_names = (
                "mark_outbox_sending",
                "start_outbox_delivery",
                "mark_delivery_sending",
            )
            claim_target = _capability_target(self.runtime, claim_names)
            if claim_target is None:
                # Persisting without an ownership/lease transition is not
                # enough: a background worker could race this direct send.
                # Leave the pending row for a future worker and retain the
                # inbound cursor by failing acceptance.
                raise RuntimeError("command response outbox has no claim capability")
            try:
                claimed = await _invoke_compatible(
                    claim_target,
                    claim_names,
                    positional=(delivery.delivery_id,),
                    keyword={"allow_pending": True},
                )
            except Exception as exc:
                logger.warning(
                    "command response outbox claim failed for %s",
                    delivery.delivery_id,
                    exc_info=True,
                )
                raise RuntimeError(
                    "command response was not claimed for delivery"
                ) from exc
            if claimed is False:
                # Another worker owns (or already completed) this projection.
                # Leave delivery to that owner rather than issuing a second
                # external WeChat send.
                logger.debug(
                    "command outbox %s was claimed by another worker",
                    delivery.delivery_id,
                )
                return outcome

            # The durable row is now owned by this sender.  Any exception from
            # the external call represents a failed/unknown attempt; the row
            # remains leased and recoverable, so do not create a second row.
            receipt = await send_user_delivery_async(client, delivery)
            if receipt.transition_to_wire_variant == "contextless":
                activate_names = ("activate_outbox_contextless_variant",)
                activate_target = _capability_target(
                    self.runtime, activate_names
                )
                if activate_target is not None:
                    activated = await _invoke_compatible(
                        activate_target,
                        activate_names,
                        positional=(delivery.delivery_id,),
                        keyword={
                            "contextless_client_id": (
                                stable_contextless_client_id(delivery)
                            )
                        },
                    )
                    if activated is not False:
                        delivery = replace(
                            delivery,
                            contextless_client_id=(
                                delivery.contextless_client_id
                                or stable_contextless_client_id(delivery)
                            ),
                            active_wire_variant="contextless",
                        )
                        # The direct command path owns the durable row.  Its
                        # alternate attempt is legal only after the same
                        # persisted one-way transition used by the worker.
                        receipt = await send_user_delivery_async(
                            client, delivery
                        )
            result_names = (
                ("mark_outbox_sent", "complete_outbox", "mark_delivery_sent")
                if receipt.sent
                else ("mark_outbox_failed", "fail_outbox", "mark_delivery_failed")
            )
            result_target = _capability_target(self.runtime, result_names)
            try:
                if result_target is None:
                    raise AttributeError(
                        "runtime has no durable command delivery result capability"
                    )
                if receipt.sent:
                    await _invoke_compatible(
                        result_target,
                        result_names,
                        positional=(delivery.delivery_id,),
                        keyword={"client_id": receipt.client_id},
                    )
                else:
                    await _invoke_compatible(
                        result_target,
                        result_names,
                        positional=(delivery.delivery_id,),
                        keyword={
                            "error": receipt.error,
                            "last_error": receipt.error,
                            "retry": receipt.retryable,
                            "permanent": not receipt.retryable,
                        },
                    )
            except Exception:
                # The external attempt has already happened.  Keep the row in
                # its leased/sending state for startup reconciliation instead
                # of attempting another send from this handler.
                logger.warning(
                    "could not persist command delivery result for %s; leaving it recoverable",
                    delivery.delivery_id,
                    exc_info=True,
                )
            if not receipt.sent:
                logger.warning(
                    "could not send command response for inbound %s: %s",
                    outcome.envelope.external_message_id,
                    receipt.error,
                )
        return outcome

    accept_message = accept
    on_message = handle_message

    def monitor_handler(
        self, loop: asyncio.AbstractEventLoop, *, timeout: float | None = 15.0
    ) -> Callable[[Any, WeixinMessage], Acceptance | bool | None]:
        """Return a blocking callback suitable for ``wechat_ilink.Monitor``.

        ``Monitor`` invokes handlers from worker threads while the runtime is
        owned by one asyncio loop.  This bridge submits the coroutine safely
        and waits only for durable acceptance.  It deliberately does not read
        runtime dictionaries from the worker thread.
        """

        def handle(client: Any, message: WeixinMessage) -> Acceptance | bool | None:
            # The Monitor invokes this callback on a contact worker thread.
            # Complete the bounded best-effort typing attempt before command,
            # media-promotion, or task work reaches the runtime loop.  This is
            # deliberately outside the durable allocator and a redelivery may
            # refresh the transient state.
            send_inbound_typing_state(client, message)

            async def invoke_handler() -> Acceptance | None | _MonitorCallbackFailure:
                try:
                    return await self.handle_message(client, message)
                except BaseException as exc:
                    # asyncio tasks re-raise SystemExit and KeyboardInterrupt
                    # into their owner loop. Normalize them before they cross
                    # that task boundary so one callback cannot stop the loop.
                    return _MonitorCallbackFailure(exc)

            def normalize_result(
                result: Acceptance | None | _MonitorCallbackFailure,
            ) -> Acceptance | bool | None:
                if not isinstance(result, _MonitorCallbackFailure):
                    return result
                exc = result.exception
                logger.error(
                    "durable monitor callback failed; retaining sync cursor",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                return False

            # Useful for deterministic unit tests and single-threaded tools
            # that have not started their loop yet.
            if not loop.is_running():
                # A runtime binds itself to one asyncio loop on first use.
                # Falling back to ``asyncio.run`` after that loop has stopped
                # would silently execute against a new loop and either mutate
                # loop-owned state from the wrong owner or fail halfway
                # through ingress.  Keep the compatibility fallback only for
                # an as-yet-unbound facade.
                owner_loop = getattr(self.runtime, "_owner_loop", None)
                if owner_loop is loop:
                    raise RuntimeError(
                        "monitor handler runtime owner loop is not running"
                    )
                return normalize_result(asyncio.run(invoke_handler()))
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is loop:
                raise RuntimeError("monitor handler cannot block its owning asyncio loop")
            settled = threading.Event()
            coroutine = invoke_handler()

            async def invoke() -> Acceptance | None | _MonitorCallbackFailure:
                try:
                    return await coroutine
                finally:
                    settled.set()

            wrapped = invoke()
            try:
                future = asyncio.run_coroutine_threadsafe(wrapped, loop)
            except BaseException:
                # ``run_coroutine_threadsafe`` can lose a race with loop
                # shutdown.  Close both coroutine objects so the caller does
                # not get an un-awaited-coroutine warning.
                try:
                    wrapped.close()
                finally:
                    coroutine.close()
                raise
            try:
                return normalize_result(future.result(timeout=timeout))
            except FutureTimeoutError:
                # A monitor callback timeout must not leave the acceptance
                # coroutine running after the cursor worker has moved on.
                future.cancel()
                # Cancellation is delivered on the owner loop.  Give the
                # coroutine's ``finally`` blocks a short, bounded chance to
                # drain; loop shutdown performs a final all-task drain.
                settled.wait(0.5)
                logger.exception(
                    "durable monitor callback timed out; retaining sync cursor"
                )
                return False
            except BaseException:
                if not future.done():
                    future.cancel()
                if future.cancelled():
                    settled.wait(0.5)
                logger.exception(
                    "durable monitor callback failed; retaining sync cursor"
                )
                return False

        return handle


def make_monitor_handler(
    gateway: WeChatGateway,
    loop: asyncio.AbstractEventLoop,
    *,
    timeout: float | None = 15.0,
) -> Callable[[Any, WeixinMessage], Acceptance | bool | None]:
    """Functional form of :meth:`WeChatGateway.monitor_handler`."""

    return gateway.monitor_handler(loop, timeout=timeout)


class WeChatDeliveryWorker:
    """Bridge claimed SQLite outbox records to the blocking iLink sender.

    The worker is intentionally small and store-protocol driven.  A runtime
    may call :meth:`run_once` from its dispatcher, or use :meth:`run` as a
    dedicated asyncio task.  Claims and state transitions remain in SQLite;
    this class only performs external delivery after a claim is obtained.
    """

    def __init__(
        self,
        store: Any,
        client: Any,
        *,
        client_resolver: Mapping[str, Any] | Callable[[str], Any] | None = None,
        worker_id: str = "wechat-delivery",
        poll_interval: float = 1.0,
        claim_limit: int = 20,
        lease_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.client = client
        self.client_resolver = client_resolver
        self.worker_id = worker_id
        self.poll_interval = max(0.05, poll_interval)
        self.claim_limit = max(1, claim_limit)
        self.lease_seconds = max(0.1, float(lease_seconds))
        self._stop = asyncio.Event()
        self._owner_loop: asyncio.AbstractEventLoop | None = None

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None and self._owner_loop.is_running():
                raise RuntimeError(
                    "WeChatDeliveryWorker must be accessed from its owning asyncio loop"
                )
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                "WeChatDeliveryWorker must be accessed from its owning asyncio loop"
            )

    def stop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._owner_loop is not None and self._owner_loop is not loop:
            if self._owner_loop.is_running():
                self._owner_loop.call_soon_threadsafe(self._stop.set)
                return
        self._assert_loop()
        self._stop.set()

    async def run_once(self) -> list[DeliveryReceipt]:
        """Claim and attempt a batch of user outbox records."""

        self._assert_loop()
        claim_started = asyncio.get_running_loop().time()
        try:
            claim_kwargs = {
                "channel": CHANNEL,
                "limit": self.claim_limit,
                "lease_seconds": self.lease_seconds,
            }
            client_bot_id = str(getattr(self.client, "bot_id", "") or "")
            if client_bot_id and self.client_resolver is None:
                claim_kwargs["bot_id"] = client_bot_id
            claimed = await _invoke_compatible(
                self.store,
                (
                    # A canonical media/bundle row is owned by the media
                    # worker because upload state must settle under the same
                    # parent lease before its one SendMsg.  The dedicated
                    # store selector prevents the text worker from claiming
                    # an empty media parent and racing its subordinate upload.
                    "claim_text_outbox",
                    "claim_outbox",
                    "claim_next_outbox",
                    "claim_user_outbox",
                ),
                positional=(self.worker_id,),
                keyword=claim_kwargs,
            )
        except AttributeError:
            return []
        if claimed is None:
            return []
        if isinstance(claimed, (UserDelivery, Mapping)):
            claimed = [claimed]

        # A claim call gives this worker ownership of the complete batch at
        # once.  Protect every row immediately: starting a heartbeat only when
        # the sequential loop reaches a row lets later claims expire while an
        # earlier network send is still in progress.
        protected_claims: list[
            tuple[Any, asyncio.Event, asyncio.Task[None] | None]
        ] = []
        for record in claimed:
            outbox_id = str(
                _value(record, "outbox_id", "delivery_id", "id", default="") or ""
            )
            claim_lost = asyncio.Event()
            heartbeat = (
                self._start_lease_heartbeat(
                    outbox_id,
                    _value(record, "claim_token", default=None),
                    claim_lost=claim_lost,
                    lease_deadline=claim_started + self.lease_seconds,
                )
                if outbox_id
                else None
            )
            protected_claims.append((record, claim_lost, heartbeat))
        active_heartbeats = {
            heartbeat
            for _record, _claim_lost, heartbeat in protected_claims
            if heartbeat is not None
        }

        receipts: list[DeliveryReceipt] = []
        try:
            for record, claim_lost, heartbeat in protected_claims:
                try:
                    if claim_lost.is_set():
                        continue
                    # Runtime UserOutboxItem uses ``reply_target`` and is
                    # converted by this helper; mailbox records do not have a
                    # target and are rejected rather than accidentally sent to
                    # a user.
                    delivery = _delivery_from_outbox(record)
                    if delivery is None:
                        await self._mark_invalid(record)
                        continue
                    # Keep the durable outbox state machine explicit.
                    # Older/small store fakes may not expose this intermediate
                    # transition, in which case a claimed row can still be sent
                    # and completed.
                    try:
                        sending = await _invoke_compatible(
                            self.store,
                            (
                                "mark_outbox_sending",
                                "start_outbox_delivery",
                                "mark_delivery_sending",
                            ),
                            positional=(delivery.delivery_id,),
                            keyword={
                                "claim_token": _value(
                                    record, "claim_token", default=None
                                )
                            },
                        )
                    except AttributeError:
                        sending = None
                    if sending is False or claim_lost.is_set():
                        # The lease was lost or superseded.  Do not perform an
                        # external send without ownership; the next claimant
                        # will retry with the same persisted client ID.
                        continue
                    send_client = await self._client_for_sender(
                        delivery.from_user_id
                    )
                    owned, receipt = await _await_while_claimed(
                        send_user_delivery_async(send_client, delivery),
                        claim_lost,
                        name=f"outbox-send:{delivery.delivery_id}",
                    )
                    if not owned or claim_lost.is_set():
                        continue
                    if receipt.transition_to_wire_variant == "contextless":
                        activated = await self._activate_contextless_variant(
                            record,
                            delivery,
                        )
                        if activated is False or claim_lost.is_set():
                            # A false result is a fenced ownership loss.  The
                            # new owner decides whether/when to retry.
                            continue
                        if activated is not None:
                            delivery = replace(
                                delivery,
                                contextless_client_id=(
                                    delivery.contextless_client_id
                                    or stable_contextless_client_id(delivery)
                                ),
                                active_wire_variant="contextless",
                            )
                            # The transition is durable before this alternate
                            # identity can reach iLink.  Never retry primary.
                            owned, receipt = await _await_while_claimed(
                                send_user_delivery_async(send_client, delivery),
                                claim_lost,
                                name=(
                                    "outbox-send-contextless:"
                                    f"{delivery.delivery_id}"
                                ),
                            )
                            if not owned or claim_lost.is_set():
                                continue
                    receipts.append(receipt)
                    try:
                        # Keep renewing until the result itself is durable.  A
                        # slow store write after an external send must not let
                        # the lease expire between the effect and its fenced
                        # completion transition.
                        await self._mark(record, delivery, receipt)
                    except Exception:
                        # The external attempt has already happened.  Keep the
                        # worker alive and let the lease/recovery path reconcile
                        # the durable row rather than dropping the delivery
                        # loop.
                        logger.exception("could not persist outbox delivery result")
                finally:
                    await self._stop_lease_heartbeat(heartbeat)
                    if heartbeat is not None:
                        active_heartbeats.discard(heartbeat)
        finally:
            # Cancellation or an unexpected per-row conversion failure must
            # not leave heartbeat tasks for the rest of the claimed batch
            # attached to the loop.
            await asyncio.gather(
                *(
                    self._stop_lease_heartbeat(heartbeat)
                    for heartbeat in tuple(active_heartbeats)
                )
                ,
                return_exceptions=True,
            )
        return receipts

    async def _client_for_sender(self, from_user_id: str) -> Any:
        """Resolve the client selected for one explicit durable sender."""

        resolver = self.client_resolver
        if resolver is None:
            return self.client
        if isinstance(resolver, Mapping):
            selected = resolver.get(from_user_id)
        else:
            selected = resolver(from_user_id)
            if inspect.isawaitable(selected):
                selected = await selected
        # Let ``send_user_delivery`` turn a missing/mismatched client into a
        # permanent receipt without making an external call.
        return selected

    async def _activate_contextless_variant(
        self,
        record: Any,
        delivery: UserDelivery,
    ) -> bool | None:
        """Persist the sole allowed wire-identity transition under the lease."""

        try:
            result = await _invoke_compatible(
                self.store,
                ("activate_outbox_contextless_variant",),
                positional=(delivery.delivery_id,),
                keyword={
                    "claim_token": _value(record, "claim_token", default=None),
                    "contextless_client_id": stable_contextless_client_id(delivery),
                },
            )
        except AttributeError:
            # Compatibility stores cannot make the transition durable.  Do
            # not send an alternate wire identity; let their normal failure
            # handling retain/retry the primary projection.
            return None
        return result is not False

    async def _mark_invalid(self, record: Any) -> None:
        """Release a claimed row that cannot be mapped to a user target."""

        outbox_id = str(_value(record, "outbox_id", "delivery_id", "id", default="") or "")
        if not outbox_id:
            return
        token = _value(record, "claim_token", default=None)
        try:
            # Validation happens after the durable claim.  Preserve the same
            # explicit state machine as an external send even though no bytes
            # leave the process: claimed -> sending -> failed_permanent.
            started = await _invoke_compatible(
                self.store,
                (
                    "mark_outbox_sending",
                    "start_outbox_delivery",
                    "mark_delivery_sending",
                ),
                positional=(outbox_id,),
                keyword={"claim_token": token},
            )
            if started is False:
                return
            await _invoke_compatible(
                self.store,
                ("mark_outbox_failed", "fail_outbox", "mark_delivery_failed"),
                positional=(outbox_id,),
                keyword={
                    "claim_token": token,
                    "error": "outbox record has no user reply target",
                    "last_error": "outbox record has no user reply target",
                    "retry": False,
                    "permanent": True,
                },
            )
        except AttributeError:
            logger.debug("store has no outbox failure method", exc_info=True)
        except Exception:
            logger.exception("could not release malformed outbox record %s", outbox_id)

    async def _mark(
        self, record: Any, delivery: UserDelivery, receipt: DeliveryReceipt
    ) -> None:
        outbox_id = delivery.delivery_id or str(
            _value(record, "outbox_id", "delivery_id", "id", default="")
        )
        token = _value(record, "claim_token", default=None)
        if receipt.sent:
            names = ("mark_outbox_sent", "complete_outbox", "mark_delivery_sent")
            kwargs = {"claim_token": token, "client_id": receipt.client_id}
        else:
            names = (
                "mark_outbox_failed",
                "fail_outbox",
                "mark_delivery_failed",
            )
            kwargs = {
                "claim_token": token,
                "error": receipt.error,
                "last_error": receipt.error,
                "retry": receipt.retryable,
                "permanent": not receipt.retryable,
                "delay": _outbox_retry_delay(record),
            }
        try:
            await _invoke_compatible(
                self.store,
                names,
                positional=(outbox_id,),
                keyword=kwargs,
            )
        except AttributeError:
            logger.debug("store has no outbox completion method", exc_info=True)

    def _start_lease_heartbeat(
        self,
        outbox_id: str,
        claim_token: str | None,
        *,
        claim_lost: asyncio.Event | None = None,
        lease_deadline: float | None = None,
    ) -> asyncio.Task[None] | None:
        renew = getattr(self.store, "renew_outbox_lease", None)
        if not claim_token or renew is None:
            return None
        interval = max(0.01, self.lease_seconds / 3)
        confirmed_until = (
            float(lease_deadline)
            if lease_deadline is not None
            else asyncio.get_running_loop().time() + self.lease_seconds
        )

        async def heartbeat() -> None:
            nonlocal confirmed_until

            def lose_ownership() -> None:
                if claim_lost is not None:
                    claim_lost.set()
                logger.warning("outbox lease ownership lost for %s", outbox_id)

            try:
                while True:
                    remaining = confirmed_until - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        lose_ownership()
                        return
                    await asyncio.sleep(min(interval, remaining))
                    if asyncio.get_running_loop().time() >= confirmed_until:
                        lose_ownership()
                        return
                    renewal_started = asyncio.get_running_loop().time()
                    renewed_until = renewal_started + self.lease_seconds
                    try:
                        changed = await _invoke_compatible(
                            self.store,
                            ("renew_outbox_lease",),
                            positional=(outbox_id, claim_token),
                            keyword={"lease_seconds": self.lease_seconds},
                        )
                    except Exception:
                        logger.debug(
                            "outbox lease renewal failed for %s",
                            outbox_id,
                            exc_info=True,
                        )
                        if asyncio.get_running_loop().time() >= confirmed_until:
                            lose_ownership()
                            return
                        continue
                    if changed is False:
                        lose_ownership()
                        return
                    if asyncio.get_running_loop().time() >= renewed_until:
                        lose_ownership()
                        return
                    confirmed_until = renewed_until
            except asyncio.CancelledError:
                return

        return asyncio.create_task(
            heartbeat(), name=f"outbox-heartbeat:{outbox_id}"
        )

    @staticmethod
    async def _stop_lease_heartbeat(
        heartbeat: asyncio.Task[None] | None,
    ) -> None:
        if heartbeat is None:
            return
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)

    async def run(self) -> None:
        """Poll until :meth:`stop` is called."""

        self._assert_loop()
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("WeChat delivery iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.poll_interval)
            except asyncio.TimeoutError:
                pass

    deliver_once = run_once


class WeChatMediaDeliveryWorker:
    """Drive the durable outgoing-media lifecycle for WeChat.

    The wire protocol has channel-specific upload fields, so callers may
    inject ``uploader`` and ``sender`` functions.  The default uploader uses
    ``wechat_ilink.cdn.upload_file_to_cdn`` when available; a record is never
    marked sent until the injected sender reports success.  This worker is
    intentionally independent from text outbox delivery.
    """

    def __init__(
        self,
        store: Any,
        client: Any,
        *,
        client_resolver: Mapping[str, Any] | Callable[[str], Any] | None = None,
        attachment_store: AttachmentStore | None = None,
        uploader: Callable[..., Any] | None = None,
        sender: Callable[..., Any] | None = None,
        worker_id: str = "wechat-media-delivery",
        claim_limit: int = 5,
        poll_interval: float = 1.0,
        lease_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.client = client
        self.client_resolver = client_resolver
        self.attachment_store = attachment_store
        self.uploader = uploader
        self.sender = sender
        self.worker_id = worker_id
        self.claim_limit = max(1, int(claim_limit))
        self.poll_interval = max(0.05, float(poll_interval))
        self.lease_seconds = max(0.1, float(lease_seconds))
        self._stop = asyncio.Event()
        self._owner_loop: asyncio.AbstractEventLoop | None = None

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None and self._owner_loop.is_running():
                raise RuntimeError(
                    "WeChatMediaDeliveryWorker must be accessed from its owning asyncio loop"
                )
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                "WeChatMediaDeliveryWorker must be accessed from its owning asyncio loop"
            )

    def stop(self) -> None:
        """Request a running media-delivery loop to stop."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._owner_loop is not None and self._owner_loop is not loop:
            if self._owner_loop.is_running():
                self._owner_loop.call_soon_threadsafe(self._stop.set)
                return
        self._assert_loop()
        self._stop.set()

    async def run_once(self) -> int:
        """Process canonical slot-owned media, then legacy unscoped media."""

        self._assert_loop()
        canonical = await self._run_canonical_once()
        legacy = await self._run_legacy_once()
        return canonical + legacy

    async def _run_canonical_once(self) -> int:
        """Send media only while owning its canonical parent outbox lease."""

        claim = getattr(self.store, "claim_scoped_outgoing_media", None)
        if claim is None:
            return 0
        claim_started = asyncio.get_running_loop().time()
        claim_kwargs: dict[str, Any] = {
            "channel": CHANNEL,
            "limit": self.claim_limit,
            "lease_seconds": self.lease_seconds,
        }
        client_bot_id = str(getattr(self.client, "bot_id", "") or "")
        if client_bot_id and self.client_resolver is None:
            claim_kwargs["bot_id"] = client_bot_id
        rows = await _invoke_compatible(
            self.store,
            ("claim_scoped_outgoing_media",),
            positional=(self.worker_id,),
            keyword=claim_kwargs,
        )
        if rows is None:
            return 0
        if isinstance(rows, Mapping) or not isinstance(rows, (list, tuple, set)):
            rows = [rows]

        protected: list[
            tuple[
                Any,
                str,
                str,
                tuple[str, ...],
                asyncio.Event,
                asyncio.Task[None] | None,
            ]
        ] = []
        for media_row in rows:
            outbox_id = str(
                _value(media_row, "outbox_id", default="") or ""
            )
            token = str(_value(media_row, "claim_token", default="") or "")
            media_id = str(_value(media_row, "media_id", default="") or "")
            media_ids = (media_id,) if media_id else ()
            row = await self._fresh_outbox(outbox_id, None)
            claim_lost = asyncio.Event()
            heartbeat = (
                self._start_outbox_lease_heartbeat(
                    outbox_id,
                    token,
                    media_ids=media_ids,
                    claim_lost=claim_lost,
                    lease_deadline=claim_started + self.lease_seconds,
                )
                if row is not None and outbox_id and token and media_ids
                else None
            )
            protected.append(
                (row, outbox_id, token, media_ids, claim_lost, heartbeat)
            )
        active_heartbeats = {
            heartbeat
            for (
                _row,
                _outbox_id,
                _token,
                _media_ids,
                _claim_lost,
                heartbeat,
            ) in protected
            if heartbeat is not None
        }

        completed = 0
        try:
            for (
                row,
                outbox_id,
                token,
                _media_ids,
                claim_lost,
                heartbeat,
            ) in protected:
                try:
                    if not outbox_id or not token or claim_lost.is_set():
                        continue
                    if await self._process_canonical_outbox(
                        row,
                        outbox_id=outbox_id,
                        claim_token=token,
                        claim_lost=claim_lost,
                    ):
                        completed += 1
                except Exception as exc:
                    logger.exception(
                        "canonical outgoing WeChat media %s failed",
                        outbox_id,
                    )
                    if not claim_lost.is_set():
                        await self._fail_canonical_outbox(
                            row,
                            outbox_id=outbox_id,
                            claim_token=token,
                            error=exc,
                        )
                finally:
                    await self._stop_lease_heartbeat(heartbeat)
                    if heartbeat is not None:
                        active_heartbeats.discard(heartbeat)
        finally:
            await asyncio.gather(
                *(
                    self._stop_lease_heartbeat(heartbeat)
                    for heartbeat in tuple(active_heartbeats)
                ),
                return_exceptions=True,
            )
        return completed

    async def _run_legacy_once(self) -> int:
        """Retain the historical independent lifecycle for unscoped media."""

        self._assert_loop()
        claim = (
            getattr(self.store, "claim_legacy_outgoing_media", None)
            or getattr(self.store, "claim_outgoing_media", None)
            or getattr(self.store, "claim_media_delivery", None)
        )
        if claim is None:
            return 0
        claim_started = asyncio.get_running_loop().time()
        rows = await _invoke_compatible(
            self.store,
            (
                "claim_legacy_outgoing_media",
                "claim_outgoing_media",
                "claim_media_delivery",
            ),
            positional=(self.worker_id,),
            # ``uploaded`` is included for stores that persist a claim on a
            # row after an upload but before the channel send.  Newer stores
            # preserve ``uploaded``/``send_pending`` while claiming; older
            # stores may move every claim to ``uploading`` and are handled by
            # the metadata fallback in :meth:`_upload`.
            keyword={
                "limit": self.claim_limit,
                "states": ("ready", "upload_pending", "uploaded", "send_pending"),
                "channel": CHANNEL,
                "bot_id": str(getattr(self.client, "bot_id", "") or ""),
                "lease_seconds": self.lease_seconds,
            },
        )
        if rows is None:
            return 0
        if isinstance(rows, Mapping) or not isinstance(rows, (list, tuple, set)):
            rows = [rows]

        # All rows become claim-owned in one transaction.  Start every lease
        # heartbeat before processing the batch sequentially so a slow upload
        # or send for the first row cannot age out the claims behind it.
        protected_claims: list[
            tuple[Any, str, Any, asyncio.Event, asyncio.Task[None] | None]
        ] = []
        for row in rows:
            media_id = str(
                _value(row, "media_id", "delivery_id", "id", default="") or ""
            )
            token = _value(row, "claim_token", default=None)
            claim_lost = asyncio.Event()
            heartbeat = (
                self._start_lease_heartbeat(
                    media_id,
                    token,
                    claim_lost=claim_lost,
                    lease_deadline=claim_started + self.lease_seconds,
                )
                if media_id
                else None
            )
            protected_claims.append(
                (row, media_id, token, claim_lost, heartbeat)
            )
        active_heartbeats = {
            heartbeat
            for _row, _media_id, _token, _claim_lost, heartbeat in protected_claims
            if heartbeat is not None
        }

        done = 0
        try:
            for row, media_id, token, claim_lost, heartbeat in protected_claims:
                if not media_id:
                    continue
                try:
                    if claim_lost.is_set():
                        continue
                    owned, uploaded = await _await_while_claimed(
                        self._upload(row),
                        claim_lost,
                        name=f"media-upload:{media_id}",
                    )
                    if not owned or claim_lost.is_set():
                        continue
                    transition = getattr(
                        self.store, "transition_outgoing_media", None
                    ) or getattr(self.store, "mark_outgoing_media", None)
                    state = self._state(row)
                    # Rows returned in ``uploaded`` or ``send_pending`` already
                    # have a channel upload.  Do not upload or checkpoint them
                    # a second time on a send retry.
                    needs_upload_transition = state not in {
                        "uploaded",
                        "send_pending",
                    }
                    if needs_upload_transition:
                        if uploaded is None:
                            raise RuntimeError("media uploader returned no result")
                        if transition is not None:
                            changed = await self._transition(
                                media_id,
                                "uploaded",
                                claim_token=token,
                                remote_id=_value(
                                    uploaded,
                                    "remote_id",
                                    "download_param",
                                    default=None,
                                ),
                                upload_param=_value(
                                    uploaded,
                                    "upload_param",
                                    "download_param",
                                    default=None,
                                ),
                                encryption_key=_value(
                                    uploaded,
                                    "encryption_key",
                                    "aes_key_hex",
                                    default=None,
                                ),
                                metadata=_media_upload_metadata(uploaded),
                            )
                            if not changed:
                                # Lease ownership was lost.  Never perform an
                                # external send after a failed conditional
                                # write.
                                continue
                            state = "uploaded"

                    # Upload completion and channel send are separate durable
                    # operations.  Commit the send intent before the external
                    # call so crash recovery has an unambiguous checkpoint.
                    if transition is not None and state == "uploaded":
                        changed = await self._transition(
                            media_id,
                            "send_pending",
                            claim_token=token,
                            from_states=("uploaded",),
                        )
                        if not changed:
                            continue
                        state = "send_pending"

                    # Give the sender the freshest durable row, including the
                    # upload parameters and send-pending state.
                    send_row = await self._fresh_row(media_id, row)
                    if claim_lost.is_set():
                        continue
                    owned, sent = await _await_while_claimed(
                        self._send(send_row, uploaded),
                        claim_lost,
                        name=f"media-send:{media_id}",
                    )
                    if not owned or claim_lost.is_set():
                        continue
                    if sent is None:
                        # No sender was configured.  Upload is retained as
                        # send_pending for a channel-specific sender.
                        continue
                    if not sent:
                        if transition is not None:
                            try:
                                await self._transition(
                                    media_id,
                                    "failed",
                                    claim_token=token,
                                    error="channel media sender reported failure",
                                )
                            except Exception:
                                logger.debug(
                                    "could not persist media send failure",
                                    exc_info=True,
                                )
                        continue
                    if transition is not None:
                        changed = await self._transition(
                            media_id,
                            "sent",
                            claim_token=token,
                            from_states=("send_pending",),
                        )
                        if not changed:
                            # The send happened but the durable owner changed.
                            # Recovery can reconcile the at-least-once attempt.
                            continue
                    done += 1
                except Exception as exc:
                    logger.exception("outgoing WeChat media %s failed", media_id)
                    transition = getattr(
                        self.store, "transition_outgoing_media", None
                    ) or getattr(self.store, "mark_outgoing_media", None)
                    if transition is not None:
                        try:
                            await self._transition(
                                media_id,
                                "failed",
                                claim_token=token,
                                error=str(exc),
                            )
                        except Exception:
                            logger.debug(
                                "could not persist media failure", exc_info=True
                            )
                finally:
                    await self._stop_lease_heartbeat(heartbeat)
                    if heartbeat is not None:
                        active_heartbeats.discard(heartbeat)
        finally:
            await asyncio.gather(
                *(
                    self._stop_lease_heartbeat(heartbeat)
                    for heartbeat in tuple(active_heartbeats)
                ),
                return_exceptions=True,
            )
        return done

    async def _process_canonical_outbox(
        self,
        parent: Any,
        *,
        outbox_id: str,
        claim_token: str,
        claim_lost: asyncio.Event,
    ) -> bool:
        """Upload and send one bundle under its sole canonical outbox claim."""

        delivery = _delivery_from_outbox(parent)
        if delivery is None:
            raise ValueError("canonical media outbox has no user reply target")
        if not _value(parent, "reply_slot_id", default=None):
            raise ValueError("canonical media outbox has no reply slot")
        if not delivery.attachments:
            raise ValueError("canonical media outbox has no attachments")
        send_client = await self._client_for_sender(delivery.from_user_id)

        started = await _invoke_compatible(
            self.store,
            (
                "mark_outbox_sending",
                "start_outbox_delivery",
                "mark_delivery_sending",
            ),
            positional=(outbox_id,),
            keyword={"claim_token": claim_token},
        )
        if started is False:
            claim_lost.set()
            return False

        children = await self._canonical_children(
            parent,
            outbox_id=outbox_id,
            claim_token=claim_token,
        )
        prepared: list[Any] = []
        for child in children:
            child = await self._prepare_canonical_child(
                child,
                parent=parent,
                outbox_id=outbox_id,
                claim_token=claim_token,
                claim_lost=claim_lost,
                client=send_client,
            )
            if child is None or claim_lost.is_set():
                return False
            prepared.append(child)

        # If any child reached `sent`, the one bundled SendMsg already crossed
        # the external boundary before a prior process lost its parent result
        # write.  Never fan out another request; finish the remaining
        # subordinate checkpoints and terminalize the same canonical send.
        if any(self._state(child) == "sent" for child in prepared):
            for child in prepared:
                if self._state(child) == "sent":
                    continue
                if self._state(child) != "send_pending":
                    raise RuntimeError(
                        "partially sent canonical media bundle has invalid child state"
                    )
                changed = await self._canonical_transition(
                    str(_value(child, "media_id", default="") or ""),
                    "sent",
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    from_states=("send_pending",),
                )
                if not changed:
                    claim_lost.set()
                    return False
            return await self._complete_canonical_outbox(
                parent,
                outbox_id=outbox_id,
                claim_token=claim_token,
                claim_lost=claim_lost,
            )

        if any(self._state(child) != "send_pending" for child in prepared):
            raise RuntimeError("canonical media children are not send-pending")
        if self.sender is None:
            raise RuntimeError("canonical media sender is unavailable")
        media_ids = tuple(
            str(_value(child, "media_id", default="") or "")
            for child in prepared
        )
        if not await self._renew_canonical_claim(
            outbox_id,
            media_ids,
            claim_token,
        ):
            claim_lost.set()
            return False

        parent_for_send = parent
        try:
            owned, sent = await _await_while_claimed(
                self._send(
                    parent_for_send,
                    tuple(prepared),
                    client=send_client,
                ),
                claim_lost,
                name=f"canonical-media-send:{outbox_id}",
            )
        except Exception as exc:
            if not (
                is_context_prepare_failure(exc)
                and delivery.active_wire_variant == "primary"
                and bool(delivery.target.context_token)
            ):
                raise
            alternate_id = stable_contextless_client_id(delivery)
            activated = await _invoke_compatible(
                self.store,
                ("activate_outbox_contextless_variant",),
                positional=(outbox_id,),
                keyword={
                    "claim_token": claim_token,
                    "contextless_client_id": alternate_id,
                },
            )
            if activated is False:
                claim_lost.set()
                return False
            parent_for_send = await self._fresh_outbox(outbox_id, parent)
            transitioned = _delivery_from_outbox(parent_for_send)
            if (
                transitioned is None
                or transitioned.active_wire_variant != "contextless"
                or stable_contextless_client_id(transitioned) != alternate_id
            ):
                raise RuntimeError(
                    "canonical media contextless transition was not durable"
                )
            if not await self._renew_canonical_claim(
                outbox_id,
                media_ids,
                claim_token,
            ):
                claim_lost.set()
                return False
            owned, sent = await _await_while_claimed(
                self._send(
                    parent_for_send,
                    tuple(prepared),
                    client=send_client,
                ),
                claim_lost,
                name=f"canonical-media-send-contextless:{outbox_id}",
            )
        if not owned or claim_lost.is_set():
            return False
        if sent is not True:
            raise RuntimeError("channel media sender reported failure")

        # The children record upload/send checkpoints only.  They all refer to
        # the single request just made; terminalizing them cannot authorize a
        # second call because only the parent owns a channel claim.
        for child in prepared:
            changed = await self._canonical_transition(
                str(_value(child, "media_id", default="") or ""),
                "sent",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=("send_pending",),
            )
            if not changed:
                claim_lost.set()
                return False
        return await self._complete_canonical_outbox(
            parent_for_send,
            outbox_id=outbox_id,
            claim_token=claim_token,
            claim_lost=claim_lost,
        )

    async def _prepare_canonical_child(
        self,
        child: Any,
        *,
        parent: Any,
        outbox_id: str,
        claim_token: str,
        claim_lost: asyncio.Event,
        client: Any,
    ) -> Any | None:
        """Advance upload state using only the active parent claim as fence."""

        media_id = str(_value(child, "media_id", default="") or "")
        if not media_id:
            raise ValueError("canonical outgoing-media child has no media_id")
        state = self._state(child)
        if state == "local":
            changed = await self._canonical_transition(
                media_id,
                "ready",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=("local",),
            )
            if not changed:
                claim_lost.set()
                return None
            child = await self._fresh_canonical_row(
                media_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                fallback=child,
            )
            state = self._state(child)
        if state == "failed":
            changed = await self._canonical_transition(
                media_id,
                "upload_pending",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=("failed",),
            )
            if not changed:
                claim_lost.set()
                return None
            child = await self._fresh_canonical_row(
                media_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                fallback=child,
            )
            state = self._state(child)
        if state in {"ready", "upload_pending"}:
            changed = await self._canonical_transition(
                media_id,
                "uploading",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=(state,),
            )
            if not changed:
                claim_lost.set()
                return None
            child = await self._fresh_canonical_row(
                media_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                fallback=child,
            )
            state = self._state(child)
        if state == "uploading":
            owned, uploaded = await _await_while_claimed(
                self._upload(child, client=client),
                claim_lost,
                name=f"canonical-media-upload:{media_id}",
            )
            if not owned or claim_lost.is_set():
                return None
            if uploaded is None:
                raise RuntimeError("media uploader returned no result")
            changed = await self._canonical_transition(
                media_id,
                "uploaded",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=("uploading",),
                remote_id=_value(
                    uploaded,
                    "remote_id",
                    "download_param",
                    default=None,
                ),
                upload_param=_value(
                    uploaded,
                    "upload_param",
                    "download_param",
                    default=None,
                ),
                encryption_key=_value(
                    uploaded,
                    "encryption_key",
                    "aes_key_hex",
                    default=None,
                ),
                metadata=_media_upload_metadata(uploaded),
            )
            if not changed:
                claim_lost.set()
                return None
            child = await self._fresh_canonical_row(
                media_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                fallback=child,
            )
            state = self._state(child)
        if state == "uploaded":
            changed = await self._canonical_transition(
                media_id,
                "send_pending",
                outbox_id=outbox_id,
                claim_token=claim_token,
                from_states=("uploaded",),
            )
            if not changed:
                claim_lost.set()
                return None
            child = await self._fresh_canonical_row(
                media_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                fallback=child,
            )
            state = self._state(child)
        if state not in {"send_pending", "sent"}:
            raise RuntimeError(
                f"canonical outgoing-media child has unsupported state: {state}"
            )
        return child

    async def _canonical_children(
        self,
        parent: Any,
        *,
        outbox_id: str,
        claim_token: str,
    ) -> list[Any]:
        rows = await _invoke_compatible(
            self.store,
            ("list_outgoing_media_for_outbox", "list_outgoing_media"),
            keyword={
                "outbox_id": outbox_id,
                "outbox_claim_token": claim_token,
                "limit": 100,
            },
        )
        values = list(rows or ())
        expected = tuple(_value(parent, "attachments", default=()) or ())

        def attachment_id(value: Any) -> str:
            if isinstance(value, Mapping):
                return str(value.get("attachment_id", value.get("id", "")) or "")
            return str(
                _value(value, "attachment_id", "id", default=value)
                if not isinstance(value, (bytes, bytearray))
                else ""
            )

        buckets: dict[str, list[Any]] = {}
        for row in values:
            buckets.setdefault(attachment_id(row), []).append(row)
        ordered: list[Any] = []
        for attachment in expected:
            identifier = attachment_id(attachment)
            matches = buckets.get(identifier) or []
            if not identifier or not matches:
                raise RuntimeError(
                    "canonical media outbox is missing a durable upload child"
                )
            ordered.append(matches.pop(0))
        if any(matches for matches in buckets.values()):
            raise RuntimeError("canonical media outbox has unexpected upload children")
        return ordered

    async def _canonical_transition(
        self,
        media_id: str,
        state: str,
        *,
        outbox_id: str,
        claim_token: str,
        **kwargs: Any,
    ) -> bool:
        """Fence subordinate state by the parent; never grant send authority."""

        try:
            result = await _invoke_compatible(
                self.store,
                ("transition_outgoing_media_for_outbox",),
                positional=(media_id, state),
                keyword={
                    "outbox_id": outbox_id,
                    "outbox_claim_token": claim_token,
                    **kwargs,
                },
            )
        except AttributeError as exc:
            raise RuntimeError(
                "store cannot fence outgoing media by its canonical outbox"
            ) from exc
        return result is not False

    async def _complete_canonical_outbox(
        self,
        parent: Any,
        *,
        outbox_id: str,
        claim_token: str,
        claim_lost: asyncio.Event,
    ) -> bool:
        delivery = _delivery_from_outbox(parent)
        if delivery is None:
            raise ValueError("canonical media outbox disappeared")
        completed = await _invoke_compatible(
            self.store,
            ("mark_outbox_sent", "complete_outbox", "mark_delivery_sent"),
            positional=(outbox_id,),
            keyword={
                "claim_token": claim_token,
                "client_id": (
                    stable_contextless_client_id(delivery)
                    if delivery.active_wire_variant == "contextless"
                    else stable_client_id(delivery)
                ),
            },
        )
        if completed is False:
            claim_lost.set()
            return False
        return True

    async def _fail_canonical_outbox(
        self,
        parent: Any,
        *,
        outbox_id: str,
        claim_token: str,
        error: BaseException,
    ) -> None:
        retryable = bool(
            getattr(
                error,
                "retryable",
                not isinstance(error, (ValueError, PermissionError)),
            )
        )
        try:
            await _invoke_compatible(
                self.store,
                ("mark_outbox_failed", "fail_outbox", "mark_delivery_failed"),
                positional=(outbox_id,),
                keyword={
                    "claim_token": claim_token,
                    "error": str(error),
                    "last_error": str(error),
                    "retry": retryable,
                    "permanent": not retryable,
                    "delay": _outbox_retry_delay(parent),
                },
            )
        except Exception:
            logger.exception(
                "could not persist canonical media failure for %s",
                outbox_id,
            )

    async def _fresh_outbox(self, outbox_id: str, fallback: Any) -> Any:
        try:
            return await _invoke_compatible(
                self.store,
                ("get_outbox_item", "get_user_outbox_item", "get_delivery"),
                positional=(outbox_id,),
            ) or fallback
        except AttributeError:
            return fallback

    async def _renew_canonical_claim(
        self,
        outbox_id: str,
        media_ids: Sequence[str],
        claim_token: str,
    ) -> bool:
        try:
            result = await _invoke_compatible(
                self.store,
                ("renew_canonical_media_lease",),
                positional=(outbox_id, claim_token),
                keyword={
                    "media_ids": tuple(media_ids),
                    "lease_seconds": self.lease_seconds,
                },
            )
        except AttributeError:
            return False
        return result is not False

    async def _client_for_sender(self, from_user_id: str) -> Any:
        resolver = self.client_resolver
        if resolver is None:
            return self.client
        if isinstance(resolver, Mapping):
            selected = resolver.get(from_user_id)
        else:
            selected = resolver(from_user_id)
            if inspect.isawaitable(selected):
                selected = await selected
        return selected

    def _start_outbox_lease_heartbeat(
        self,
        outbox_id: str,
        claim_token: str,
        *,
        media_ids: Sequence[str],
        claim_lost: asyncio.Event,
        lease_deadline: float,
    ) -> asyncio.Task[None] | None:
        renew = getattr(self.store, "renew_canonical_media_lease", None)
        if renew is None:
            return None
        interval = max(0.01, self.lease_seconds / 3)
        confirmed_until = float(lease_deadline)

        async def heartbeat() -> None:
            nonlocal confirmed_until
            try:
                while True:
                    remaining = confirmed_until - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        claim_lost.set()
                        return
                    await asyncio.sleep(min(interval, remaining))
                    if asyncio.get_running_loop().time() >= confirmed_until:
                        claim_lost.set()
                        return
                    renewed_at = asyncio.get_running_loop().time()
                    renewed_until = renewed_at + self.lease_seconds
                    try:
                        changed = await _invoke_compatible(
                            self.store,
                            ("renew_canonical_media_lease",),
                            positional=(outbox_id, claim_token),
                            keyword={
                                "media_ids": tuple(media_ids),
                                "lease_seconds": self.lease_seconds,
                            },
                        )
                    except Exception:
                        logger.debug(
                            "canonical media outbox lease renewal failed for %s",
                            outbox_id,
                            exc_info=True,
                        )
                        if asyncio.get_running_loop().time() >= confirmed_until:
                            claim_lost.set()
                            return
                        continue
                    if changed is False or asyncio.get_running_loop().time() >= renewed_until:
                        claim_lost.set()
                        return
                    confirmed_until = renewed_until
            except asyncio.CancelledError:
                return

        return asyncio.create_task(
            heartbeat(),
            name=f"canonical-media-outbox-heartbeat:{outbox_id}",
        )

    async def run(self) -> None:
        """Poll durable media rows until :meth:`stop` is called."""

        self._assert_loop()
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("WeChat media delivery iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.poll_interval)
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def _state(row: Any) -> str:
        value = _value(row, "state", "status", default="")
        return str(getattr(value, "value", value) or "").strip().lower()

    async def _transition(self, media_id: str, state: str, **kwargs: Any) -> bool:
        """Apply a conditional media transition, normalizing small fakes."""

        try:
            result = await _invoke_compatible(
                self.store,
                ("transition_outgoing_media", "mark_outgoing_media"),
                positional=(media_id, state),
                keyword=kwargs,
            )
        except AttributeError:
            # A store without explicit transitions is an older compatibility
            # facade; the worker can still exercise the injected sender.
            return True
        return result is not False

    def _start_lease_heartbeat(
        self,
        media_id: str,
        claim_token: str | None,
        *,
        claim_lost: asyncio.Event | None = None,
        lease_deadline: float | None = None,
    ) -> asyncio.Task[None] | None:
        renew = getattr(self.store, "renew_outgoing_media_lease", None) or getattr(
            self.store, "renew_media_lease", None
        )
        if not claim_token or renew is None:
            return None
        interval = max(0.01, self.lease_seconds / 3)
        confirmed_until = (
            float(lease_deadline)
            if lease_deadline is not None
            else asyncio.get_running_loop().time() + self.lease_seconds
        )

        async def heartbeat() -> None:
            nonlocal confirmed_until

            def lose_ownership() -> None:
                if claim_lost is not None:
                    claim_lost.set()
                logger.warning("media lease ownership lost for %s", media_id)

            try:
                while True:
                    remaining = confirmed_until - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        lose_ownership()
                        return
                    await asyncio.sleep(min(interval, remaining))
                    if asyncio.get_running_loop().time() >= confirmed_until:
                        lose_ownership()
                        return
                    renewal_started = asyncio.get_running_loop().time()
                    renewed_until = renewal_started + self.lease_seconds
                    try:
                        changed = await _invoke_compatible(
                            self.store,
                            (
                                "renew_outgoing_media_lease",
                                "renew_media_lease",
                            ),
                            positional=(media_id, claim_token),
                            keyword={"lease_seconds": self.lease_seconds},
                        )
                    except Exception:
                        logger.debug(
                            "media lease renewal failed for %s",
                            media_id,
                            exc_info=True,
                        )
                        if asyncio.get_running_loop().time() >= confirmed_until:
                            lose_ownership()
                            return
                        continue
                    if changed is False:
                        lose_ownership()
                        return
                    if asyncio.get_running_loop().time() >= renewed_until:
                        lose_ownership()
                        return
                    confirmed_until = renewed_until
            except asyncio.CancelledError:
                return

        return asyncio.create_task(
            heartbeat(), name=f"media-heartbeat:{media_id}"
        )

    @staticmethod
    async def _stop_lease_heartbeat(
        heartbeat: asyncio.Task[None] | None,
    ) -> None:
        if heartbeat is None:
            return
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)

    async def _fresh_row(self, media_id: str, fallback: Any) -> Any:
        try:
            return await _invoke_compatible(
                self.store,
                ("get_outgoing_media", "get_media_delivery"),
                positional=(media_id,),
            ) or fallback
        except AttributeError:
            return fallback

    async def _fresh_canonical_row(
        self,
        media_id: str,
        *,
        outbox_id: str,
        claim_token: str,
        fallback: Any,
    ) -> Any:
        try:
            return await _invoke_compatible(
                self.store,
                ("get_outgoing_media_for_outbox",),
                positional=(media_id,),
                keyword={
                    "outbox_id": outbox_id,
                    "outbox_claim_token": claim_token,
                },
            ) or fallback
        except AttributeError as exc:
            raise RuntimeError(
                "store cannot reload canonical outgoing media under its parent claim"
            ) from exc

    async def _upload(self, row: Any, *, client: Any = None) -> Any:
        selected_client = self.client if client is None else client
        state = self._state(row)
        # A legacy claim implementation may expose a previously uploaded row
        # as ``uploading`` while retaining its remote parameters.  Those
        # parameters are sufficient for the send projection and prevent a
        # second CDN upload on retry.
        has_remote_upload = bool(
            _value(row, "remote_id", "download_param", default=None)
            or _value(row, "upload_param", default=None)
        )
        if state in {"uploaded", "send_pending"} or (state == "uploading" and has_remote_upload):
            return row
        attachment_id = str(_value(row, "attachment_id", default="") or "")
        if self.uploader is not None:
            return await _invoke_external_hook(self.uploader, row, selected_client)
        if self.attachment_store is None:
            raise RuntimeError("an attachment_store or uploader is required for media upload")
        # SQLite is authoritative for immutable path/size/checksum metadata.
        # Startup hydration is only an optimization and may be capped, while
        # another store instance can register a file after this worker starts.
        try:
            authoritative = await _invoke_compatible(
                self.store,
                ("get_attachment",),
                positional=(attachment_id,),
            )
        except AttributeError:
            authoritative = None
        if authoritative is not None:
            try:
                state = await _invoke_compatible(
                    self.store,
                    ("get_attachment_state",),
                    positional=(attachment_id,),
                )
            except AttributeError:
                state = "ready"
            if str(state or "ready") != "ready":
                raise AttachmentError(f"managed attachment is unavailable: {attachment_id}")
            self.attachment_store.remember(authoritative)
        data = await self.attachment_store.aread_bytes(attachment_id)
        try:
            from wechat_ilink.cdn import upload_file_to_cdn
            from wechat_ilink.types import (
                CDN_MEDIA_TYPE_FILE,
                CDN_MEDIA_TYPE_IMAGE,
                CDN_MEDIA_TYPE_VIDEO,
            )
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("WeChat CDN support is unavailable") from exc
        kind = str(
            _value(row, "kind", default=None)
            or (_value(row, "metadata", default={}) or {}).get("kind", "")
            or (_value(row, "metadata", default={}) or {}).get("media_kind", "")
        ).lower()
        mime = str(
            _value(row, "mime_type", default=None)
            or (_value(row, "metadata", default={}) or {}).get("mime_type", "")
        ).lower()
        if kind == "image" or mime.startswith("image/"):
            media_type = CDN_MEDIA_TYPE_IMAGE
        elif kind == "video" or mime.startswith("video/"):
            media_type = CDN_MEDIA_TYPE_VIDEO
        else:
            media_type = CDN_MEDIA_TYPE_FILE
        return await asyncio.to_thread(
            upload_file_to_cdn,
            selected_client,
            data,
            str(_value(row, "external_user_id", default="") or ""),
            media_type,
        )

    async def _send(
        self,
        row: Any,
        uploaded: Any,
        *,
        client: Any = None,
    ) -> bool | None:
        if self.sender is None:
            # Uploading and retaining ``send_pending`` is still a successful
            # lifecycle step; a channel-specific sender can finish it later.
            return None
        result = await _invoke_external_hook(
            self.sender,
            row,
            uploaded,
            self.client if client is None else client,
        )
        return bool(result is not False)

    process_once = run_once


def _delivery_from_outbox(record: Any) -> UserDelivery | None:
    """Convert runtime ``UserOutboxItem`` or a mapping to channel delivery."""

    # Agent mailbox rows intentionally have no reply target/channel recipient.
    # Reject based on field *presence*, not value: serializers often retain
    # optional routing columns as ``None`` or an empty string, and those rows
    # must never be interpreted as user-visible deliveries.
    if _has_field(record, "destination_agent_id") or _has_field(record, "mailbox_id"):
        return None
    visibility = _value(record, "visibility", default="user")
    visibility = str(getattr(visibility, "value", visibility) or "user").lower()
    if visibility not in {"", "user"}:
        return None
    target_value = _value(record, "reply_target", "target", default=None)
    if target_value is None:
        return None
    target = ReplyTarget.from_dict(
        target_value.to_dict()
        if hasattr(target_value, "to_dict")
        else target_value.as_dict()
        if hasattr(target_value, "as_dict")
        else target_value
    )
    # A mailbox or malformed projection may carry an empty placeholder
    # mapping rather than omitting ``reply_target`` altogether.  Treat it as
    # non-user data so it cannot reach a channel sender.
    if not target.channel or not target.external_user_id:
        return None
    outbox_id = str(_value(record, "outbox_id", "delivery_id", "id", default=""))
    content = str(_value(record, "content", "text", default="") or "")
    priority = _value(record, "priority", default=1)
    priority = int(getattr(priority, "value", priority))
    mode = _value(record, "delivery_mode", default="push_eligible")
    mode = str(getattr(mode, "value", mode))
    created = _value(record, "created_at", default=None)
    created_text = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
    return UserDelivery(
        delivery_id=outbox_id,
        target=target,
        from_user_id=str(
            _value(record, "from_user_id", "sender_bot_id", default="")
            or _value(record, "bot_id", default="")
            or target.bot_id
        ),
        content=content,
        priority=priority,
        visibility="user",
        delivery_mode=mode,
        client_id=str(
            _value(record, "primary_client_id", default="")
            or _value(record, "client_id", default="")
            or ""
        ),
        event_id=str(_value(record, "event_id", default="") or ""),
        task_id=str(_value(record, "task_id", default="") or ""),
        created_at=created_text,
        presented=str(_value(record, "presentation", default="unseen"))
        in {"presented", "acknowledged"},
        attachments=tuple(_value(record, "attachments", default=()) or ()),
        contextless_client_id=str(
            _value(record, "contextless_client_id", default="") or ""
        ),
        active_wire_variant=str(
            _value(record, "active_wire_variant", default="primary")
            or "primary"
        ),
    )


# Compatibility spelling and descriptive alias.
WechatGateway = WeChatGateway
WeChatChannelAdapter = WeChatGateway
WeChatAdapter = WeChatGateway
WechatAdapter = WeChatGateway
WeChatOutboxWorker = WeChatDeliveryWorker
WeChatMediaWorker = WeChatMediaDeliveryWorker
normalize_wechat_message = normalize_message
send_wechat_delivery = send_user_delivery


__all__ = [
    "Acceptance",
    "CHANNEL",
    "COMMAND_REGISTRY",
    "COMMAND_POLICIES",
    "CommandRegistryEntry",
    "CommandRegistryGroup",
    "CommandResponse",
    "COMMAND_HELP",
    "DEFAULT_COMMAND_POLICY",
    "DEFERRED_REPLY_QUOTA_CAPABILITY",
    "DEFAULT_AGENT_ID",
    "DEFAULT_SESSION_ID",
    "LARK_COMMAND_POLICY",
    "MVP_COMMANDS",
    "MVP_COMMAND_NAMES",
    "MVPCommandRouter",
    "WECHAT_COMMAND_POLICY",
    "ChannelCommandPolicy",
    "WeChatChannelAdapter",
    "WeChatAdapter",
    "WeChatGateway",
    "WeChatInboundMediaPromoter",
    "WeChatOutboxWorker",
    "WechatAdapter",
    "WechatGateway",
    "conversation_id_for",
    "external_message_id_for",
    "extract_text",
    "extract_media",
    "normalize_message",
    "normalize_wechat_message",
    "make_monitor_handler",
    "send_user_delivery",
    "send_user_delivery_async",
    "run_shell_command",
    "send_inbound_typing_state",
    "send_media_delivery",
    "send_wechat_delivery",
    "stable_client_id",
    "should_send_typing_state",
    "command_client_id",
    "command_delivery_id",
    "command_initial_reply",
    "command_request_id",
    "command_task_id",
    "command_help_for_channel",
    "command_names_for_channel",
    "command_policy_for",
    "command_supported",
    "filter_command_registry",
    "unsupported_command_response",
    "transcription_confirmation_id",
    "WeChatDeliveryWorker",
    "WeChatMediaDeliveryWorker",
    "WeChatMediaWorker",
]
