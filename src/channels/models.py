"""Channel-neutral message and delivery models.

The low-level :mod:`wechat_ilink` package intentionally exposes the wire
protocol.  The runtime, however, needs a small immutable representation that
does not know anything about WeChat JSON fields.  These dataclasses are the
boundary between those two layers.  They are deliberately dependency free so
other channel adapters can use them as well.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp suitable for persistence."""

    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    """Convert nested model values to JSON-safe values without a dependency."""

    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class ReplyTarget:
    """Durable destination for a user-visible response.

    ``context_token`` is a transport hint and may expire.  The stable
    destination identity is the channel, bot, user, session, and source
    message fields.  A target can therefore be persisted and retried after a
    process restart without retaining a live ``Client`` object.
    """

    channel: str = "wechat"
    bot_id: str = ""
    external_user_id: str = ""
    session_id: str = "default"
    source_message_id: str = ""
    source_sequence: int | None = None
    context_token: str = ""

    def __post_init__(self) -> None:
        if not self.session_id:
            object.__setattr__(self, "session_id", "default")

    @property
    def user_id(self) -> str:
        """Compatibility alias used by older channel code."""

        return self.external_user_id

    @property
    def external_message_id(self) -> str:
        """Compatibility alias for the inbound message that created the task."""

        return self.source_message_id

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __call__(self) -> "ReplyTarget":
        """Allow legacy ``envelope.reply_target()`` call sites.

        The domain model exposes a target as a value, while an early channel
        prototype exposed a factory method.  Making the value callable keeps
        both forms source-compatible without duplicating state.
        """

        return self

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | "ReplyTarget" | Any) -> "ReplyTarget":
        if isinstance(value, cls):
            return value
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            if hasattr(value, "to_dict"):
                value = value.to_dict()
            elif hasattr(value, "as_dict"):
                value = value.as_dict()
            else:
                value = {
                    name: getattr(value, name)
                    for name in cls.__dataclass_fields__
                    if hasattr(value, name)
                }
        data = dict(value)
        # Accept names used by older store schemas and by channel payloads.
        if not data.get("external_user_id"):
            data["external_user_id"] = data.pop("user_id", "")
        if not data.get("source_message_id"):
            data["source_message_id"] = data.pop("external_message_id", "")
        if "source_sequence" not in data and "seq" in data:
            data["source_sequence"] = data.pop("seq")
        allowed = {
            "channel",
            "bot_id",
            "external_user_id",
            "session_id",
            "source_message_id",
            "source_sequence",
            "context_token",
        }
        return cls(**{key: data[key] for key in allowed if key in data})

    def stable_key(self) -> str:
        """Return a deterministic key for delivery/deduplication diagnostics."""

        raw = "\x1f".join(
            (
                self.channel,
                self.bot_id,
                self.external_user_id,
                self.session_id,
                self.source_message_id,
                str(self.source_sequence if self.source_sequence is not None else ""),
            )
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class InboundEnvelope:
    """Normalized inbound channel message.

    The envelope is immutable once accepted.  ``raw`` is optional diagnostic
    metadata and should never be used as the identity or dedupe key.
    """

    channel: str = "wechat"
    bot_id: str = ""
    external_user_id: str = ""
    external_message_id: str = ""
    text: str = ""
    session_id: str = "default"
    agent_id: str = "codex"
    conversation_id: str = ""
    source_sequence: int | None = None
    context_token: str = ""
    received_at: str = field(default_factory=utc_now)
    raw: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.session_id:
            object.__setattr__(self, "session_id", "default")

    @property
    def user_id(self) -> str:
        return self.external_user_id

    @property
    def message_id(self) -> str:
        return self.external_message_id

    @property
    def payload(self) -> Mapping[str, Any]:
        """Runtime-store alias for diagnostic channel payload."""

        return self.raw or {}

    @property
    def dedupe_identity(self) -> tuple[str, str, str]:
        return self.dedupe_key()

    @property
    def source_message_id(self) -> str:
        return self.external_message_id

    def dedupe_key(self) -> tuple[str, str, str]:
        return (self.channel, self.bot_id, self.external_message_id)

    @property
    def reply_target(self) -> ReplyTarget:
        return ReplyTarget(
            channel=self.channel,
            bot_id=self.bot_id,
            external_user_id=self.external_user_id,
            session_id=self.session_id,
            source_message_id=self.external_message_id,
            source_sequence=self.source_sequence,
            context_token=self.context_token,
        )

    def target(self) -> ReplyTarget:
        """Compatibility alias used by runtime ``InboundMessage`` models."""

        return self.reply_target

    def as_store_dict(self) -> dict[str, Any]:
        """Return only fields accepted by ``SQLiteStore._coerce_inbound``."""

        # ``raw`` is diagnostic channel metadata.  Keep it useful for media
        # and protocol debugging, but do not persist the transport's rolling
        # context token as part of the immutable payload: iLink may issue a
        # fresh token when it redelivers the same message after a reconnect.
        payload = dict(self.payload)
        payload.pop("context_token", None)
        payload.pop("contextToken", None)
        channel_metadata = payload.get("channel_metadata")
        if isinstance(channel_metadata, Mapping):
            channel_metadata = dict(channel_metadata)
            channel_metadata.pop("context_token", None)
            channel_metadata.pop("contextToken", None)
            payload["channel_metadata"] = channel_metadata
        return {
            "channel": self.channel,
            "bot_id": self.bot_id,
            "external_user_id": self.external_user_id,
            "external_message_id": self.external_message_id,
            "text": self.text,
            "session_id": self.session_id,
            "source_sequence": self.source_sequence,
            "context_token": self.context_token,
            "payload": payload,
            "received_at": self.received_at,
        }

    def to_dict(self, *, include_raw: bool = True) -> dict[str, Any]:
        data = asdict(self)
        if not include_raw:
            data.pop("raw", None)
        return _json_safe(data)

    def json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


# Names used in early design notes and by downstream adapters.  Keeping these
# aliases avoids forcing callers to care which term the channel implementation
# uses for the normalized envelope.
InboundMessage = InboundEnvelope
NormalizedInbound = InboundEnvelope
ChannelMessage = InboundEnvelope


@dataclass(frozen=True, slots=True)
class UserDelivery:
    """A projected user notification/response awaiting channel delivery."""

    delivery_id: str = ""
    target: ReplyTarget = field(default_factory=ReplyTarget)
    # The outbound channel account is part of the durable delivery identity.
    # It is intentionally separate from the live channel client selected by a
    # worker: a retry must validate that client rather than silently replacing
    # the persisted sender with ``client.bot_id``.
    from_user_id: str = ""
    content: str = ""
    priority: int = 1
    visibility: str = "user"
    delivery_mode: str = "push_eligible"
    client_id: str = ""
    event_id: str = ""
    task_id: str = ""
    created_at: str = field(default_factory=utc_now)
    presented: bool = False
    attachments: tuple[Any, ...] = ()
    # ``client_id`` is the immutable primary iLink wire identity.  The only
    # permitted alternate is derived once for the context-free preparation
    # fallback and becomes active through a durable one-way transition.
    contextless_client_id: str = ""
    active_wire_variant: str = "primary"

    def __post_init__(self) -> None:
        # Older persisted projections carry the sender only as
        # ``ReplyTarget.bot_id``.  Materialize it on the channel model so new
        # serialization and every wire call are explicit while retaining
        # read compatibility with those rows.
        if not self.from_user_id and self.target.bot_id:
            object.__setattr__(self, "from_user_id", self.target.bot_id)
        variant = str(self.active_wire_variant or "primary").strip().lower()
        if variant not in {"primary", "contextless"}:
            raise ValueError(f"unsupported wire variant: {self.active_wire_variant!r}")
        object.__setattr__(self, "active_wire_variant", variant)

    @property
    def sender_bot_id(self) -> str:
        """Compatibility/domain spelling for the explicit wire sender."""

        return self.from_user_id

    @property
    def primary_client_id(self) -> str:
        """Return the immutable primary iLink client identity."""

        return self.client_id

    @property
    def text(self) -> str:
        return self.content

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["target"] = self.target.to_dict()
        return _json_safe(data)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | "UserDelivery") -> "UserDelivery":
        if isinstance(value, cls):
            return value
        data = dict(value)
        target_value = data.pop("target", data.pop("reply_target", {}))
        if hasattr(target_value, "to_dict") and not isinstance(target_value, Mapping):
            target_value = target_value.to_dict()
        elif hasattr(target_value, "as_dict") and not isinstance(target_value, Mapping):
            target_value = target_value.as_dict()
        target = ReplyTarget.from_dict(target_value)
        if "content" not in data and "text" in data:
            data["content"] = data.pop("text")
        if "delivery_id" not in data and "outbox_id" in data:
            data["delivery_id"] = data.pop("outbox_id")
        if "client_id" not in data and "primary_client_id" in data:
            data["client_id"] = data.pop("primary_client_id")
        if "from_user_id" not in data:
            data["from_user_id"] = data.pop(
                "sender_bot_id", target.bot_id
            )
        if "presented" not in data and "presentation" in data:
            presentation = data.pop("presentation")
            presentation = getattr(presentation, "value", presentation)
            data["presented"] = presentation in {"presented", "acknowledged"}
        allowed = {
            "delivery_id",
            "from_user_id",
            "content",
            "priority",
            "visibility",
            "delivery_mode",
            "client_id",
            "event_id",
            "task_id",
            "created_at",
            "presented",
            "attachments",
            "contextless_client_id",
            "active_wire_variant",
        }
        if "attachments" in data and not isinstance(data["attachments"], tuple):
            data["attachments"] = tuple(data["attachments"] or ())
        return cls(target=target, **{key: data[key] for key in allowed if key in data})


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    """Result of one channel send attempt."""

    delivery_id: str
    client_id: str
    sent: bool
    error: str = ""
    attempted_at: str = field(default_factory=utc_now)
    retryable: bool = True
    # The variant attempted by this receipt.  A definitive preparation
    # rejection on ``primary`` requests a durable transition; it does not
    # authorize the channel helper to send the alternate before persistence.
    wire_variant: str = "primary"
    transition_to_wire_variant: str = ""


@dataclass(frozen=True, slots=True)
class ChannelCommand:
    """Parsed slash command."""

    name: str
    args: tuple[str, ...] = ()
    raw: str = ""

    @property
    def argument(self) -> str:
        return " ".join(self.args)


def parse_command(text: str) -> ChannelCommand | None:
    """Parse a slash command, returning ``None`` for ordinary text.

    Parsing is intentionally conservative: a slash must be the first
    non-whitespace character and the command name is normalized to lowercase.
    Quoting is left to command-specific parsers so command syntax remains
    backwards compatible with the original bot.
    """

    raw = text or ""
    stripped = raw.strip()
    if not stripped.startswith("/"):
        return None
    tokens = stripped[1:].split()
    if not tokens or not tokens[0]:
        return ChannelCommand(name="", args=(), raw=raw)
    return ChannelCommand(name=tokens[0].lower(), args=tuple(tokens[1:]), raw=raw)


__all__ = [
    "ChannelCommand",
    "ChannelMessage",
    "DeliveryReceipt",
    "InboundEnvelope",
    "InboundMessage",
    "NormalizedInbound",
    "ReplyTarget",
    "UserDelivery",
    "parse_command",
    "utc_now",
]
