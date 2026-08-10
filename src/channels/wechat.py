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
import hashlib
import inspect
import json
import logging
import uuid
from dataclasses import asdict, dataclass, is_dataclass, replace
from typing import Any, Callable, Mapping, Protocol, Sequence

from wechat_ilink.sender import send_text_reply
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    WeixinMessage,
)

from .models import (
    ChannelCommand,
    DeliveryReceipt,
    InboundEnvelope,
    ReplyTarget,
    UserDelivery,
    parse_command,
)
from src.runtime.media import AttachmentStore

logger = logging.getLogger(__name__)

CHANNEL = "wechat"
DEFAULT_AGENT_ID = "codex"
DEFAULT_SESSION_ID = "default"
MVP_COMMANDS = frozenset({
    "status", "tasks", "interrupt", "retry", "cancel",
    "help", "agents", "agent", "notify", "inbox", "mode",
    "ask", "confirm", "reject",
})
MVP_COMMAND_NAMES = frozenset(f"/{name}" for name in MVP_COMMANDS)
ACTIVE_TASK_STATES = frozenset({"queued", "claimed", "running", "cancel_requested"})
RETRYABLE_TASK_STATES = frozenset({"failed", "orphaned", "interrupted"})
TERMINAL_TASK_STATES = frozenset({"completed", "failed", "interrupted", "cancelled", "canceled"})

COMMAND_HELP = (
    "commands:\n"
    "/status - show active tasks\n"
    "/tasks [limit] - list your tasks\n"
    "/interrupt [task_id] - interrupt a running task\n"
    "/retry <task_id> - explicitly retry a failed/orphaned task\n"
    "/cancel <task_id> - cancel a task\n"
    "/agents - list Agents\n"
    "/agent [id] - show or switch the front Agent\n"
    "/mode [chat|plan|review|execute] - show or set mode\n"
    "/notify [on|off] - show or set notifications\n"
    "/inbox [agent_id|all] - present unseen notifications"
    "\n/ask <agent_id> <prompt> - send a correlated Agent request"
    "\n/confirm <id> - confirm an audio transcription candidate"
    "\n/reject <id> - reject an audio transcription candidate"
)


class InboundAcceptor(Protocol):
    """Minimal runtime boundary used by :class:`WeChatGateway`."""

    async def accept_inbound(self, envelope: InboundEnvelope) -> Any: ...


class CommandHandler(Protocol):
    async def handle_command(
        self, command: ChannelCommand, envelope: InboundEnvelope
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class Acceptance:
    """Outcome of one normalized inbound acceptance attempt."""

    envelope: InboundEnvelope
    accepted: bool
    duplicate: bool = False
    task_id: str = ""
    command_response: str = ""
    raw_result: Any = None

    @property
    def status(self) -> str:
        if self.duplicate:
            return "duplicate"
        return "accepted" if self.accepted else "rejected"


def conversation_id_for(
    *,
    bot_id: str,
    external_user_id: str,
    session_id: str = DEFAULT_SESSION_ID,
    agent_id: str = DEFAULT_AGENT_ID,
) -> str:
    """Build the stable per-channel, per-Agent conversation identity."""

    return ":".join(
        (CHANNEL, bot_id, external_user_id, session_id or DEFAULT_SESSION_ID, agent_id)
    )


def extract_text(message: WeixinMessage) -> str | None:
    """Return the logical text input from a finished user message.

    A WeChat message can contain multiple text items.  They form one logical
    inbound message and therefore one task; joining them also prevents one
    channel message from bypassing the dedupe constraint by creating a task
    per item.
    """

    if (
        message.message_type != MESSAGE_TYPE_USER
        or message.message_state != MESSAGE_STATE_FINISH
    ):
        return None
    parts = [
        item.text_item.text
        for item in message.item_list
        if item.type == ITEM_TYPE_TEXT
        and item.text_item is not None
        and item.text_item.text.strip()
    ]
    if not parts:
        return None
    return "\n".join(parts)


def extract_media(message: WeixinMessage) -> list[dict[str, Any]]:
    """Normalize finished WeChat media items into opaque structured refs.

    The adapter does not download or decrypt files.  It records the channel
    reference and metadata so the durable task can hand it to a managed media
    pipeline later.  Voice ``text`` is retained as a *candidate* and is never
    promoted to ``InboundEnvelope.text`` without confirmation.
    """
    if (
        message.message_type != MESSAGE_TYPE_USER
        or message.message_state != MESSAGE_STATE_FINISH
    ):
        return []
    values: list[dict[str, Any]] = []
    for item in message.item_list:
        kind = {
            2: "image",
            3: "audio",
            4: "file",
            5: "video",
        }.get(int(item.type), "")
        if not kind:
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
    canonical message items.  The context token is used only when no sequence
    exists because it is a transport hint that may rotate across redelivery.
    The result is explicitly namespaced so it cannot collide with a genuine
    iLink ID.
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
        "context_token": message.context_token if not message.seq else "",
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


def stable_client_id(delivery: UserDelivery | Mapping[str, Any]) -> str:
    """Return the persisted ID or a deterministic fallback for retries."""

    item = _coerce_delivery(delivery)
    if item.client_id:
        return item.client_id
    identity = item.delivery_id or "\x1f".join(
        (item.target.stable_key(), item.task_id, item.event_id, item.content)
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-wechat:{identity}"))


def send_user_delivery(
    client: Any, delivery: UserDelivery | Mapping[str, Any]
) -> DeliveryReceipt:
    """Deliver one durable user-outbox record through WeChat.

    Only user-visible WeChat projections are accepted here.  In particular,
    Agent mailbox messages cannot accidentally pass through the channel
    sender.  The same stable ``client_id`` is used on every retry.
    """

    item = _coerce_delivery(delivery)
    client_id = stable_client_id(item)
    if item.visibility != "user":
        raise ValueError("only user-visible outbox records may be sent to WeChat")
    if item.target.channel != CHANNEL:
        raise ValueError(f"cannot send {item.target.channel!r} delivery via WeChat")
    if not item.target.external_user_id:
        raise ValueError("delivery target is missing external_user_id")
    if not item.content:
        raise ValueError("delivery content is empty")

    try:
        # Keep compatibility with simple test/dry-run senders that predate
        # the stable ``client_id`` parameter; the real iLink sender always
        # accepts it.
        try:
            parameters = inspect.signature(send_text_reply).parameters
        except (TypeError, ValueError):
            parameters = {}
        accepts_client_id = "client_id" in parameters or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        args = (
            client,
            item.target.external_user_id,
            item.content,
            item.target.context_token,
        )
        if accepts_client_id or not parameters:
            send_result = send_text_reply(*args, client_id=client_id)
        else:
            send_result = send_text_reply(*args)
        if send_result is False:
            raise RuntimeError("channel sender reported failure")
    except Exception as exc:
        return DeliveryReceipt(
            delivery_id=item.delivery_id,
            client_id=client_id,
            sent=False,
            error=str(exc),
        )
    return DeliveryReceipt(
        delivery_id=item.delivery_id,
        client_id=client_id,
        sent=True,
    )


async def send_user_delivery_async(
    client: Any, delivery: UserDelivery | Mapping[str, Any]
) -> DeliveryReceipt:
    """Run the blocking iLink send outside the runtime event loop."""

    return await asyncio.to_thread(send_user_delivery, client, delivery)


def _value(record: Any, *names: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        for name in names:
            if name in record:
                return record[name]
        return default
    for name in names:
        if hasattr(record, name):
            return getattr(record, name)
    return default


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


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _coerce_delivery(value: Any) -> UserDelivery:
    """Accept channel models, mappings, and runtime outbox dataclasses."""

    if isinstance(value, UserDelivery):
        return value
    if isinstance(value, Mapping):
        return UserDelivery.from_dict(value)
    converted = _delivery_from_outbox(value)
    if converted is not None:
        return converted
    raise TypeError("delivery must be a UserDelivery, mapping, or outbox record")


async def _invoke_compatible(
    target: Any,
    names: Sequence[str],
    *,
    positional: Sequence[Any] = (),
    keyword: Mapping[str, Any] | None = None,
) -> Any:
    """Call the first supported public method while tolerating narrow fakes.

    The small amount of signature adaptation keeps the channel layer usable
    with the store protocol as well as with a TaskManager facade.  It does not
    inspect or mutate runtime-owned dictionaries.
    """

    keyword = dict(keyword or {})
    for name in names:
        method = getattr(target, name, None)
        if method is None:
            continue
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return await _maybe_await(method(*positional, **keyword))
        accepts_var_kw = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if accepts_var_kw:
            filtered = keyword
        else:
            filtered = {
                key: value
                for key, value in keyword.items()
                if key in signature.parameters
            }
        try:
            signature.bind(*positional, **filtered)
        except TypeError:
            # A compatibility call may provide a value both positionally and
            # under its conventional keyword name (or the target may expose a
            # keyword-only parameter).  Retry with keyword arguments alone
            # before deciding that this method is unsupported.
            if positional:
                retry_keywords = dict(filtered)
                parameter_names = [
                    parameter.name
                    for parameter in signature.parameters.values()
                    if parameter.kind
                    in {
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    }
                ]
                for parameter_name, value in zip(parameter_names, positional):
                    retry_keywords.setdefault(parameter_name, value)
                try:
                    signature.bind(**retry_keywords)
                except TypeError:
                    continue
                return await _maybe_await(method(**retry_keywords))
            continue
        return await _maybe_await(method(*positional, **filtered))
    joined = ", ".join(names)
    raise AttributeError(f"{type(target).__name__} implements none of: {joined}")


def _task_id(result: Any) -> str:
    return str(_value(result, "task_id", "id", default="") or "")


def _is_duplicate(result: Any) -> bool:
    duplicate = _value(result, "duplicate", "is_duplicate", default=None)
    if duplicate is not None:
        return bool(duplicate)
    status = str(_value(result, "status", "inbound_status", default="")).lower()
    return status == "duplicate"


def _is_accepted(result: Any) -> bool:
    accepted = _value(result, "accepted", "created", default=None)
    if accepted is not None:
        return bool(accepted) or _is_duplicate(result)
    status = str(_value(result, "status", "inbound_status", default="")).lower()
    if status:
        return status in {
            "accepted",
            "duplicate",
            "stored",
            "task_queued",
            "queued",
        }
    return result is not False and result is not None


def _format_task(record: Any) -> str:
    task_id = str(_value(record, "task_id", "id", default="?") or "?")
    status = str(_value(record, "status", "state", default="unknown") or "unknown")
    agent_id = str(_value(record, "agent_id", default="") or "")
    attempts = _value(record, "attempts", "attempt", default=None)
    error = str(_value(record, "last_error", "error", default="") or "")
    suffix = f" agent={agent_id}" if agent_id else ""
    if attempts is not None:
        suffix += f" attempts={attempts}"
    if error:
        suffix += f" error={error}"
    return f"{task_id} {status}{suffix}"


def _task_state(record: Any) -> str:
    """Return a normalized task state for command guards and formatting."""

    value = _value(record, "status", "state", default="")
    value = getattr(value, "value", value)
    return str(value or "").strip().lower()


def _format_agent(record: Any, *, active: bool = False) -> str:
    agent_id = str(_value(record, "agent_id", "id", default="?") or "?")
    display = str(_value(record, "display_name", "name", default=agent_id) or agent_id)
    summary = str(_value(record, "summary", default="") or "")
    marker = " (current)" if active else ""
    suffix = f": {summary}" if summary else ""
    return f"{agent_id}{marker} - {display}{suffix}"


def _format_inbox_item(record: Any) -> str:
    content = str(_value(record, "content", "text", default="") or "").strip()
    task_id = str(_value(record, "task_id", default="") or "")
    priority = _value(record, "priority", default=1)
    priority = int(getattr(priority, "value", priority))
    prefix = f"[{task_id}] " if task_id else ""
    if priority >= 3:
        prefix = "[attention] " + prefix
    elif priority >= 2:
        prefix = "[notify] " + prefix
    return f"- {prefix}{content or '(empty notification)'}"


def _task_belongs_to(record: Any, envelope: InboundEnvelope) -> bool:
    """Check command ownership without exposing another user's task IDs."""

    target = _value(record, "reply_target", "target", default=None)
    if target is not None:
        target = ReplyTarget.from_dict(
            target.to_dict()
            if hasattr(target, "to_dict")
            else target.as_dict()
            if hasattr(target, "as_dict")
            else target
        )
        if target.external_user_id:
            return (
                target.channel in {"", envelope.channel}
                and target.bot_id in {"", envelope.bot_id}
                and target.external_user_id == envelope.external_user_id
                and target.session_id in {"", envelope.session_id}
            )
    # Store task rows expose these columns even when no target projection was
    # materialized yet.
    owner = _value(record, "external_user_id", "user_id", default=None)
    # A row with neither a populated reply target nor explicit owner columns
    # cannot be authorized.  Treating empty defaults as a match would let any
    # user operate on a malformed/legacy task ID.
    if owner is None and (target is None or not target.external_user_id):
        return False
    if owner is not None and str(owner) != envelope.external_user_id:
        return False
    channel = _value(record, "channel", default="")
    bot_id = _value(record, "bot_id", default="")
    session_id = _value(record, "session_id", default="")
    return (
        str(channel or "") in {"", envelope.channel}
        and str(bot_id or "") in {"", envelope.bot_id}
        and str(session_id or "") in {"", envelope.session_id}
    )


class MVPCommandRouter:
    """Handle durable-task control commands through a manager/store facade."""

    def __init__(self, manager: Any) -> None:
        self.manager = manager

    async def handle_command(
        self, command: ChannelCommand, envelope: InboundEnvelope
    ) -> str | None:
        name = command.name
        if name not in MVP_COMMANDS:
            # A slash-prefixed message is a control-plane input even when the
            # command is unsupported.  Returning a response here lets the
            # gateway durably record it without accidentally dispatching it to
            # an Agent as ordinary user text.
            token = f"/{name}" if name else "/"
            return f"unknown command: {token}. try /help"

        # Resolve the route through the manager's public API rather than
        # trusting the envelope's static/default Agent.  Gateway callers
        # normally refresh this snapshot before dispatch, but the router is
        # also a supported standalone boundary (and lightweight fakes often
        # omit the gateway's private resolver).
        route_scope = {
            "channel": envelope.channel,
            "bot_id": envelope.bot_id,
            "external_user_id": envelope.external_user_id,
            "user_id": envelope.external_user_id,
            "session_id": envelope.session_id,
            "conversation_id": envelope.conversation_id,
        }
        try:
            active_agent = await _invoke_compatible(
                self.manager,
                ("get_active_agent", "active_agent"),
                keyword=route_scope,
            )
        except Exception:
            logger.debug("active Agent lookup unavailable; using envelope route", exc_info=True)
            active_agent = envelope.agent_id
        active_agent = _value(active_agent, "agent_id", "id", default=active_agent)
        active_agent = str(active_agent or envelope.agent_id or DEFAULT_AGENT_ID)
        scope = {**route_scope, "agent_id": active_agent}
        if name == "help":
            return COMMAND_HELP if not command.args else "usage: /help"

        if name == "ask":
            if len(command.args) < 2:
                return "usage: /ask <agent_id> <prompt>"
            destination = command.args[0]
            prompt = " ".join(command.args[1:]).strip()
            if not prompt:
                return "usage: /ask <agent_id> <prompt>"
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("ask", "ask_agent", "request_agent", "send_agent_message"),
                    positional=(destination, prompt),
                    keyword={**scope, "source_agent_id": active_agent, "actor": envelope.external_user_id},
                )
            except (AttributeError, KeyError, PermissionError, ValueError) as exc:
                return f"cannot ask Agent: {exc}"
            request_id = _value(result, "request_id", default="")
            if request_id:
                return f"Agent request queued: {request_id}"
            mailbox_id = _value(result, "mailbox_id", "message_id", default="")
            return f"Agent request queued: {mailbox_id or 'accepted'}"

        if name in {"confirm", "reject"}:
            if len(command.args) != 1:
                return f"usage: /{name} <confirmation_id>"
            confirmation_id = command.args[0]
            names = (
                ("confirm_transcription", "confirm_audio", "confirm_candidate")
                if name == "confirm"
                else ("reject_transcription", "reject_audio", "reject_candidate")
            )
            try:
                result = await _invoke_compatible(
                    self.manager,
                    names,
                    positional=(confirmation_id,),
                    keyword={**scope, "actor": envelope.external_user_id},
                )
            except (AttributeError, KeyError, PermissionError, ValueError) as exc:
                return f"cannot {name} transcription: {exc}"
            if name == "reject":
                return "transcription rejected" if bool(result) else "transcription was already resolved"
            task_id = _value(result, "task_id", "id", default="")
            return f"transcription confirmed; task queued: {task_id}" if task_id else "transcription confirmed; task queued"

        if name == "agents":
            if command.args:
                return "usage: /agents"
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("list_agents", "agents", "list_profiles"),
                    keyword={},
                )
            except AttributeError:
                return "Agent registry is unavailable"
            records = list(result or [])
            if not records:
                return "agents: (none)"
            try:
                active = await _invoke_compatible(
                    self.manager,
                    ("get_active_agent", "active_agent"),
                    keyword={
                        "channel": envelope.channel,
                        "bot_id": envelope.bot_id,
                        "external_user_id": envelope.external_user_id,
                        "session_id": envelope.session_id,
                    },
                )
            except AttributeError:
                active = active_agent
            return "agents:\n" + "\n".join(
                _format_agent(item, active=str(_value(item, "agent_id", "id", default="")) == str(active))
                for item in records
            )

        if name == "agent":
            if len(command.args) > 1:
                return "usage: /agent [id]"
            if not command.args:
                try:
                    active = await _invoke_compatible(
                        self.manager,
                        ("get_active_agent", "active_agent"),
                        keyword={
                            "channel": envelope.channel,
                            "bot_id": envelope.bot_id,
                            "external_user_id": envelope.external_user_id,
                            "session_id": envelope.session_id,
                        },
                    )
                except AttributeError:
                    active = active_agent
                return f"active Agent: {active}"
            selected = command.args[0]
            try:
                active = await _invoke_compatible(
                    self.manager,
                    ("set_active_agent", "switch_agent", "set_agent"),
                    positional=(selected,),
                    keyword={
                        "channel": envelope.channel,
                        "bot_id": envelope.bot_id,
                        "external_user_id": envelope.external_user_id,
                        "session_id": envelope.session_id,
                    },
                )
            except (AttributeError, KeyError, PermissionError, ValueError) as exc:
                return f"cannot switch Agent: {exc}"
            # Switching never interrupts tasks.  Present only this Agent's
            # user-visible unseen records; mailbox rows remain untouched.
            try:
                inbox = await _invoke_compatible(
                    self.manager,
                    ("inbox", "present_inbox", "present_notifications"),
                    keyword={**scope, "agent_id": selected, "limit": 100, "present": True},
                )
            except AttributeError:
                inbox = []
            lines = [f"switched to Agent: {selected}"]
            records = list(inbox or [])
            if records:
                lines.append("notifications:")
                lines.extend(_format_inbox_item(item) for item in records)
            return "\n".join(lines)

        if name == "notify":
            if len(command.args) > 1 or (
                command.args and command.args[0].lower() not in {"on", "off"}
            ):
                return "usage: /notify [on|off]"
            if not command.args:
                try:
                    enabled = await _invoke_compatible(
                        self.manager,
                        ("get_notify", "get_notification_preference"),
                        keyword=scope,
                    )
                except AttributeError:
                    return "notification preferences are unavailable"
                return f"notifications: {'on' if bool(enabled) else 'off'}"
            enabled = command.args[0].lower() == "on"
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("set_notify", "set_notification_preference", "set_notify_preference"),
                    positional=(enabled,),
                    keyword=scope,
                )
            except (AttributeError, ValueError, PermissionError) as exc:
                return f"cannot set notifications: {exc}"
            return f"notifications: {'on' if bool(result) else 'off'}"

        if name == "inbox":
            if len(command.args) > 1:
                return "usage: /inbox [agent_id|all]"
            requested = command.args[0] if command.args else active_agent
            if requested.lower() == "all":
                try:
                    agents = await _invoke_compatible(
                        self.manager,
                        ("list_agents", "agents"),
                        keyword={},
                    )
                except AttributeError:
                    agents = []
                agent_ids = [
                    str(_value(item, "agent_id", "id", default=""))
                    for item in list(agents or [])
                ]
            else:
                agent_ids = [requested]
            records: list[Any] = []
            for agent_id in agent_ids:
                try:
                    result = await _invoke_compatible(
                        self.manager,
                        ("inbox", "present_inbox", "present_notifications"),
                        keyword={**scope, "agent_id": agent_id, "limit": 100, "present": True},
                    )
                except AttributeError:
                    result = []
                records.extend(list(result or []))
            if not records:
                return "inbox: (empty)"
            return "inbox:\n" + "\n".join(_format_inbox_item(item) for item in records)

        if name == "mode":
            if len(command.args) > 1:
                return "usage: /mode [chat|plan|review|execute]"
            if not command.args:
                try:
                    mode = await _invoke_compatible(
                        self.manager,
                        ("get_mode", "mode"),
                        keyword=scope,
                    )
                except AttributeError:
                    return "mode selection is unavailable"
                return f"mode: {mode}"
            requested_mode = command.args[0].lower()
            if requested_mode not in {"chat", "plan", "review", "execute"}:
                return "usage: /mode [chat|plan|review|execute]"
            try:
                mode = await _invoke_compatible(
                    self.manager,
                    ("set_mode", "switch_mode"),
                    positional=(requested_mode,),
                    keyword={**scope, "actor": envelope.external_user_id, "explicit": True},
                )
            except (AttributeError, KeyError, PermissionError, ValueError) as exc:
                return f"cannot set mode: {exc}"
            return f"mode: {mode}"

        if name == "status":
            if command.args:
                return "usage: /status"
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("status", "get_status", "task_status"),
                    keyword=scope,
                )
            except AttributeError:
                return "status is unavailable"
            if isinstance(result, str):
                return result
            if result is None:
                return "idle"
            if isinstance(result, Sequence) and not isinstance(
                result, (str, bytes, bytearray)
            ):
                records = list(result)
                # Facades other than TaskManager may return the full task
                # history.  `/status` is intentionally an active-work query;
                # completed/failed rows belong under `/tasks`.
                records = [
                    item
                    for item in records
                    if not _task_state(item) or _task_state(item) in ACTIVE_TASK_STATES
                ]
                if not records:
                    return "idle"
                return "active tasks:\n" + "\n".join(
                    f"- {_format_task(item)}" for item in records
                )
            if _task_state(result) in TERMINAL_TASK_STATES:
                return "idle"
            return _format_task(result)

        if name == "tasks":
            if len(command.args) > 1 or (
                command.args and not command.args[0].isdigit()
            ):
                return "usage: /tasks [limit]"
            limit = int(command.args[0]) if command.args else 20
            limit = min(max(limit, 1), 100)
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("list_tasks", "tasks", "get_tasks"),
                    keyword={**scope, "limit": limit},
                )
            except AttributeError:
                return "tasks are unavailable"
            records = list(result or [])
            if not records:
                return "tasks: (none)"
            return "tasks:\n" + "\n".join(
                f"- {_format_task(item)}" for item in records
            )

        if name == "interrupt" and not command.args:
            # Backwards-compatible shorthand: interrupt the newest active
            # task in this user's Agent conversation.  Explicit IDs remain
            # preferred when several tasks are running.
            try:
                active = await _invoke_compatible(
                    self.manager,
                    ("list_tasks", "tasks", "get_tasks"),
                    keyword={**scope, "limit": 100},
                )
            except AttributeError:
                active = []
            active_records = [
                item
                for item in list(active or [])
                if str(_value(item, "status", "state", default="")).lower()
                in {"claimed", "running", "cancel_requested"}
            ]
            if len(active_records) == 1:
                selected_id = _task_id(active_records[0])
                if not selected_id:
                    return "no active task"
                command = ChannelCommand(name="interrupt", args=(selected_id,), raw=command.raw)
            elif not active_records:
                return "no active task"
            else:
                return "usage: /interrupt <task_id>"

        if len(command.args) != 1:
            return f"usage: /{name} <task_id>"
        task_id = command.args[0]
        # Authorization is performed before invoking a control operation when
        # the facade exposes a public task lookup.  Missing/foreign tasks use
        # the same response to avoid leaking existence across users.
        getter = getattr(self.manager, "get_task", None)
        if getter is not None:
            try:
                record = await _maybe_await(getter(task_id))
            except Exception:
                record = None
            if record is None or not _task_belongs_to(record, envelope):
                return f"cannot {name} task {task_id}"
            state = _task_state(record)
            if name == "interrupt" and state and state not in {
                "claimed",
                "running",
                "cancel_requested",
            }:
                return f"cannot interrupt task {task_id}"
            if name == "retry" and state and state not in RETRYABLE_TASK_STATES:
                return f"cannot retry task {task_id}"
            if name == "cancel" and state in TERMINAL_TASK_STATES:
                return f"cannot cancel task {task_id}"
        action_names = {
            "interrupt": ("interrupt", "interrupt_task", "request_interrupt"),
            "retry": ("retry", "retry_task", "requeue_task"),
            "cancel": ("cancel", "cancel_task", "request_cancel"),
        }
        try:
            result = await _invoke_compatible(
                self.manager,
                action_names[name],
                positional=(task_id,),
                keyword=scope,
            )
        except AttributeError:
            return f"{name} is unavailable"
        if isinstance(result, str):
            return result
        state = str(_value(result, "state", "status", default="") or "").lower()
        if name == "retry" and state:
            changed = state == "queued"
        else:
            changed = bool(
                _value(result, "changed", "accepted", "requested", default=result)
            )
        verb = {
            "interrupt": "interrupt requested",
            "retry": "retry queued",
            "cancel": "cancel requested",
        }[name]
        if changed:
            return f"{verb}: {task_id}"
        return f"cannot {name} task {task_id}"


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
    ) -> None:
        self.runtime = runtime
        self.bot_id = bot_id
        self.session_resolver = session_resolver
        self.agent_resolver = agent_resolver
        self.command_router = command_router or MVPCommandRouter(runtime)

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
                try:
                    resolved = str(await _maybe_await(resolver(envelope)) or envelope.agent_id)
                except Exception:
                    logger.debug("active Agent route lookup failed", exc_info=True)
                    resolved = envelope.agent_id
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

        command = parse_command(envelope.text)
        media_values = []
        if isinstance(envelope.raw, Mapping):
            media_values = list(envelope.raw.get("media") or ())
        audio_candidates = [
            item
            for item in media_values
            if isinstance(item, Mapping)
            and str(item.get("kind", "")).lower() == "audio"
            and str(item.get("candidate_text", "") or "").strip()
        ]
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
        task_inputs: dict[str, Any] = {"text": envelope.text}
        if media_values:
            task_inputs["media"] = media_values
        acceptance_kwargs: dict[str, Any] = {
            "conversation_id": envelope.conversation_id,
            "reply_target": envelope.reply_target.to_dict(),
            "inputs": task_inputs,
            # A channel-provided voice transcription is only a candidate.  It
            # becomes task input after an explicit /confirm, never merely by
            # arriving in the message stream.
            "create_task": not is_control_command and not audio_candidates,
        }
        # TaskManager resolves the front Agent from its loop-owned route.  Do
        # not force the envelope's static ``codex`` default in that case.
        # A bare SQLiteStore (or a small fake) still needs an explicit Agent
        # snapshot for task creation.
        if not hasattr(self.runtime, "active_agent_for"):
            acceptance_kwargs["agent_id"] = envelope.agent_id

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
        duplicate = _is_duplicate(result)
        accepted = _is_accepted(result)
        command_response = ""
        if accepted and not duplicate and audio_candidates and command is None:
            creator = getattr(self.runtime, "create_transcription_candidate", None)
            if creator is None:
                creator = getattr(getattr(self.runtime, "store", None), "create_transcription_candidate", None)
            candidate_ids: list[str] = []
            if creator is not None:
                candidate_target = self.runtime if getattr(
                    self.runtime, "create_transcription_candidate", None
                ) is not None else self.runtime.store

                async def managed_attachment_id(value: Any) -> str | None:
                    """Return an attachment FK only when it is durable.

                    WeChat's ``remote_id``/encrypted CDN reference is not a
                    row in SQLite's ``attachments`` table.  Passing it as a
                    foreign key makes an otherwise valid voice message fail
                    ingestion.  A channel downloader may pre-register an
                    attachment and put its ID on the normalized media value;
                    verify that ID when the store exposes a lookup method.
                    """

                    candidate_id = str(value or "").strip()
                    if not candidate_id:
                        return None
                    getter = getattr(candidate_target, "get_attachment", None)
                    if getter is None:
                        return candidate_id
                    try:
                        found = await _maybe_await(getter(candidate_id))
                    except Exception:
                        logger.debug(
                            "attachment lookup failed for transcription candidate",
                            exc_info=True,
                        )
                        return None
                    return candidate_id if found is not None else None

                for media in audio_candidates:
                    attachment_id = await managed_attachment_id(media.get("attachment_id"))
                    candidate = await _invoke_compatible(
                        candidate_target,
                        ("create_transcription_candidate",),
                        keyword={
                            "channel": envelope.channel,
                            "bot_id": envelope.bot_id,
                            "external_user_id": envelope.external_user_id,
                            "session_id": envelope.session_id,
                            "agent_id": envelope.agent_id,
                            # ``remote_id`` is a channel reference, not a
                            # managed-attachment FK.  It remains available in
                            # the durable inbound payload for a downloader.
                            "attachment_id": attachment_id,
                            "candidate_text": str(media.get("candidate_text")),
                            "source": "wechat",
                            "confidence": media.get("confidence"),
                        },
                    )
                    identifier = _value(candidate, "confirmation_id", "id", default="")
                    if identifier:
                        candidate_ids.append(str(identifier))
            if candidate_ids:
                inbound_id = _value(result, "inbound", default=None)
                inbound_id = _value(inbound_id, "message_id", default=None)
                status_method = getattr(self.runtime, "set_inbound_status", None)
                if status_method is None:
                    status_method = getattr(getattr(self.runtime, "store", None), "set_inbound_status", None)
                if status_method is not None and inbound_id:
                    await _invoke_compatible(
                        self.runtime if getattr(self.runtime, "set_inbound_status", None) is not None else self.runtime.store,
                        ("set_inbound_status",),
                        positional=(inbound_id, "awaiting_confirmation"),
                    )
                command_response = (
                    "audio transcription candidate(s): "
                    + ", ".join(candidate_ids)
                    + ". use /confirm <id> or /reject <id>"
                )
        if (
            accepted
            and not duplicate
            and command is not None
            and self.command_router is not None
        ):
            response = await self.command_router.handle_command(command, envelope)
            if response is None and command.name not in MVP_COMMANDS:
                token = f"/{command.name}" if command.name else "/"
                response = f"unknown command: {token}. try /help"
            command_response = "" if response is None else str(response)
        return Acceptance(
            envelope=envelope,
            accepted=accepted,
            duplicate=duplicate,
            task_id=_task_id(result),
            command_response=command_response,
            raw_result=result,
        )

    async def handle_message(self, client: Any, message: WeixinMessage) -> Acceptance | None:
        """Monitor-compatible async handler including immediate command replies."""

        outcome = await self.accept(message, bot_id=getattr(client, "bot_id", ""))
        if outcome and outcome.command_response and not outcome.duplicate:
            delivery = UserDelivery(
                delivery_id=f"command:{outcome.envelope.external_message_id}",
                target=outcome.envelope.reply_target(),
                content=outcome.command_response,
                client_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"codex-wechat:command:{outcome.envelope.bot_id}:"
                        f"{outcome.envelope.external_message_id}",
                    )
                ),
            )
            persisted = None
            try:
                persisted = await _invoke_compatible(
                    self.runtime,
                    ("enqueue_user_outbox", "create_user_outbox", "add_user_outbox"),
                    keyword={"delivery": delivery.to_dict()},
                )
                converted = _delivery_from_outbox(persisted)
                if converted is not None:
                    delivery = converted
                # Claim the durable row for this direct send when supported;
                # otherwise a concurrent delivery worker could send it twice.
                try:
                    await _invoke_compatible(
                        self.runtime,
                        ("mark_outbox_sending", "start_outbox_delivery", "mark_delivery_sending"),
                        positional=(delivery.delivery_id,),
                        keyword={"allow_pending": True},
                    )
                except AttributeError:
                    pass
            except AttributeError:
                persisted = None
            except Exception:
                logger.debug("could not persist command response before send", exc_info=True)
            receipt = await send_user_delivery_async(client, delivery)
            if persisted is not None:
                try:
                    if receipt.sent:
                        await _invoke_compatible(
                            self.runtime,
                            ("mark_outbox_sent", "complete_outbox", "mark_delivery_sent"),
                            positional=(delivery.delivery_id,),
                            keyword={"client_id": receipt.client_id},
                        )
                    else:
                        await _invoke_compatible(
                            self.runtime,
                            ("mark_outbox_failed", "fail_outbox", "mark_delivery_failed"),
                            positional=(delivery.delivery_id,),
                            keyword={"error": receipt.error, "last_error": receipt.error},
                        )
                except Exception:
                    logger.debug("could not persist command delivery result", exc_info=True)
            if not receipt.sent:
                logger.warning(
                    "could not send command response for inbound %s: %s",
                    outcome.envelope.external_message_id,
                    receipt.error,
                )
                # The inbound row is already durable.  Preserve a failed
                # command response in the user outbox when the runtime/store
                # exposes an outbox projection API; delivery can then retry
                # with the same deterministic client ID.
                try:
                    if persisted is not None:
                        # The durable row was retained before the send; avoid
                        # creating a second projection for the same command.
                        return outcome
                    await _invoke_compatible(
                        self.runtime,
                        (
                            "enqueue_user_outbox",
                            "create_user_outbox",
                            "add_user_outbox",
                        ),
                        keyword={"delivery": delivery.to_dict()},
                    )
                except AttributeError:
                    logger.debug("runtime has no command outbox API", exc_info=True)
                except Exception:
                    logger.exception("could not persist failed command delivery")
        return outcome

    accept_message = accept
    on_message = handle_message

    def monitor_handler(
        self, loop: asyncio.AbstractEventLoop, *, timeout: float | None = 15.0
    ) -> Callable[[Any, WeixinMessage], Acceptance | None]:
        """Return a blocking callback suitable for ``wechat_ilink.Monitor``.

        ``Monitor`` invokes handlers from worker threads while the runtime is
        owned by one asyncio loop.  This bridge submits the coroutine safely
        and waits only for durable acceptance.  It deliberately does not read
        runtime dictionaries from the worker thread.
        """

        def handle(client: Any, message: WeixinMessage) -> Acceptance | None:
            # Useful for deterministic unit tests and single-threaded tools
            # that have not started their loop yet.
            if not loop.is_running():
                return asyncio.run(self.handle_message(client, message))
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is loop:
                raise RuntimeError("monitor handler cannot block its owning asyncio loop")
            future = asyncio.run_coroutine_threadsafe(
                self.handle_message(client, message), loop
            )
            return future.result(timeout=timeout)

        return handle


def make_monitor_handler(
    gateway: WeChatGateway,
    loop: asyncio.AbstractEventLoop,
    *,
    timeout: float | None = 15.0,
) -> Callable[[Any, WeixinMessage], Acceptance | None]:
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
        worker_id: str = "wechat-delivery",
        poll_interval: float = 1.0,
        claim_limit: int = 20,
    ) -> None:
        self.store = store
        self.client = client
        self.worker_id = worker_id
        self.poll_interval = max(0.05, poll_interval)
        self.claim_limit = max(1, claim_limit)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self) -> list[DeliveryReceipt]:
        """Claim and attempt a batch of user outbox records."""

        try:
            claim_kwargs = {"channel": CHANNEL, "limit": self.claim_limit}
            client_bot_id = str(getattr(self.client, "bot_id", "") or "")
            if client_bot_id:
                claim_kwargs["bot_id"] = client_bot_id
            claimed = await _invoke_compatible(
                self.store,
                ("claim_outbox", "claim_next_outbox", "claim_user_outbox"),
                positional=(self.worker_id,),
                keyword=claim_kwargs,
            )
        except AttributeError:
            return []
        if claimed is None:
            return []
        if isinstance(claimed, (UserDelivery, Mapping)):
            claimed = [claimed]

        receipts: list[DeliveryReceipt] = []
        for record in claimed:
            # Runtime UserOutboxItem uses ``reply_target`` and is converted by
            # this helper; mailbox records do not have a target and are
            # rejected rather than accidentally sent to a user.
            delivery = _delivery_from_outbox(record)
            if delivery is None:
                await self._mark_invalid(record)
                continue
            # Keep the durable outbox state machine explicit.  Older/small
            # store fakes may not expose this intermediate transition, in
            # which case a claimed row can still be sent and completed.
            try:
                sending = await _invoke_compatible(
                    self.store,
                    ("mark_outbox_sending", "start_outbox_delivery", "mark_delivery_sending"),
                    positional=(delivery.delivery_id,),
                    keyword={"claim_token": _value(record, "claim_token", default=None)},
                )
            except AttributeError:
                sending = None
            if sending is False:
                # The lease was lost or superseded.  Do not perform an
                # external send without ownership; the next claimant will
                # retry with the same persisted client ID.
                continue
            receipt = await send_user_delivery_async(self.client, delivery)
            receipts.append(receipt)
            try:
                await self._mark(record, delivery, receipt)
            except Exception:
                # The external attempt has already happened.  Keep the
                # worker alive and let the lease/recovery path reconcile the
                # durable row rather than dropping the delivery loop.
                logger.exception("could not persist outbox delivery result")
        return receipts

    async def _mark_invalid(self, record: Any) -> None:
        """Release a claimed row that cannot be mapped to a user target."""

        outbox_id = str(_value(record, "outbox_id", "delivery_id", "id", default="") or "")
        if not outbox_id:
            return
        token = _value(record, "claim_token", default=None)
        try:
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

    async def run(self) -> None:
        """Poll until :meth:`stop` is called."""

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
        attachment_store: AttachmentStore | None = None,
        uploader: Callable[..., Any] | None = None,
        sender: Callable[..., Any] | None = None,
        worker_id: str = "wechat-media-delivery",
        claim_limit: int = 5,
    ) -> None:
        self.store = store
        self.client = client
        self.attachment_store = attachment_store
        self.uploader = uploader
        self.sender = sender
        self.worker_id = worker_id
        self.claim_limit = max(1, int(claim_limit))

    async def run_once(self) -> int:
        claim = getattr(self.store, "claim_outgoing_media", None) or getattr(
            self.store, "claim_media_delivery", None
        )
        if claim is None:
            return 0
        rows = await _invoke_compatible(
            self.store,
            ("claim_outgoing_media", "claim_media_delivery"),
            positional=(self.worker_id,),
            keyword={"limit": self.claim_limit, "states": ("ready", "upload_pending", "send_pending")},
        )
        if rows is None:
            return 0
        if isinstance(rows, Mapping) or not isinstance(rows, (list, tuple, set)):
            rows = [rows]
        done = 0
        for row in rows:
            media_id = str(_value(row, "media_id", "delivery_id", "id", default="") or "")
            token = _value(row, "claim_token", default=None)
            if not media_id:
                continue
            try:
                uploaded = await self._upload(row)
                transition = getattr(self.store, "transition_outgoing_media", None) or getattr(
                    self.store, "mark_outgoing_media", None
                )
                if transition is not None and uploaded is not None:
                    await _invoke_compatible(
                        self.store,
                        ("transition_outgoing_media", "mark_outgoing_media"),
                        positional=(media_id, "uploaded"),
                        keyword={
                            "claim_token": token,
                            "remote_id": _value(uploaded, "remote_id", "download_param", default=None),
                            "upload_param": _value(uploaded, "upload_param", "download_param", default=None),
                            "encryption_key": _value(uploaded, "encryption_key", "aes_key_hex", default=None),
                        },
                    )
                    await _invoke_compatible(
                        self.store,
                        ("transition_outgoing_media", "mark_outgoing_media"),
                        positional=(media_id, "send_pending"),
                        keyword={"claim_token": token, "from_states": ("uploaded",)},
                    )
                sent = await self._send(row, uploaded)
                if sent:
                    if transition is not None:
                        await _invoke_compatible(
                            self.store,
                            ("transition_outgoing_media", "mark_outgoing_media"),
                            positional=(media_id, "sent"),
                            keyword={"claim_token": token, "from_states": ("send_pending", "uploaded")},
                        )
                    done += 1
            except Exception as exc:
                logger.exception("outgoing WeChat media %s failed", media_id)
                transition = getattr(self.store, "transition_outgoing_media", None)
                if transition is not None:
                    try:
                        await _invoke_compatible(
                            self.store,
                            ("transition_outgoing_media",),
                            positional=(media_id, "failed"),
                            keyword={"claim_token": token, "error": str(exc)},
                        )
                    except Exception:
                        logger.debug("could not persist media failure", exc_info=True)
        return done

    async def _upload(self, row: Any) -> Any:
        if str(_value(row, "state", default="")) in {"uploaded", "send_pending"}:
            return row
        attachment_id = str(_value(row, "attachment_id", default="") or "")
        if self.uploader is not None:
            result = self.uploader(row, self.client)
            return await result if inspect.isawaitable(result) else result
        if self.attachment_store is None:
            raise RuntimeError("an attachment_store or uploader is required for media upload")
        data = await self.attachment_store.aread_bytes(attachment_id)
        try:
            from wechat_ilink.cdn import upload_file_to_cdn
            from wechat_ilink.types import CDN_MEDIA_TYPE_FILE
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("WeChat CDN support is unavailable") from exc
        return await asyncio.to_thread(
            upload_file_to_cdn,
            self.client,
            data,
            str(_value(row, "external_user_id", default="") or ""),
            CDN_MEDIA_TYPE_FILE,
        )

    async def _send(self, row: Any, uploaded: Any) -> bool:
        if self.sender is None:
            # Uploading and retaining ``send_pending`` is still a successful
            # lifecycle step; a channel-specific sender can finish it later.
            return False
        result = self.sender(row, uploaded, self.client)
        if inspect.isawaitable(result):
            result = await result
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
        content=content,
        priority=priority,
        visibility="user",
        delivery_mode=mode,
        client_id=str(_value(record, "client_id", default="") or ""),
        event_id=str(_value(record, "event_id", default="") or ""),
        task_id=str(_value(record, "task_id", default="") or ""),
        created_at=created_text,
        presented=str(_value(record, "presentation", default="unseen"))
        in {"presented", "acknowledged"},
        attachments=tuple(_value(record, "attachments", default=()) or ()),
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
    "DEFAULT_AGENT_ID",
    "DEFAULT_SESSION_ID",
    "MVP_COMMANDS",
    "MVP_COMMAND_NAMES",
    "MVPCommandRouter",
    "WeChatChannelAdapter",
    "WeChatAdapter",
    "WeChatGateway",
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
    "send_wechat_delivery",
    "stable_client_id",
    "WeChatDeliveryWorker",
    "WeChatMediaDeliveryWorker",
    "WeChatMediaWorker",
]
