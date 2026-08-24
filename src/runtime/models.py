"""Application-level models for the durable runtime.

These models intentionally use strings and application enums rather than
Codex SDK types.  The SDK adapter is the only layer that translates runtime
policy and event values to SDK values.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum, IntEnum
from typing import Any, Iterator, Mapping


USER_REPLY_FORMAT_AGENT_PREFIX_V1 = "agent-prefix-v1"


def iter_model_descriptors(values: Any) -> Iterator[Any]:
    """Yield model leaves from SDK and compatibility catalog envelopes."""

    seen: set[int] = set()

    def is_descriptor(value: Any) -> bool:
        if isinstance(value, Mapping):
            return any(key in value for key in ("id", "model_id", "model"))
        return any(
            getattr(value, name, None) is not None
            for name in ("id", "model_id", "model")
        )

    def walk(value: Any) -> Iterator[Any]:
        if value is None or isinstance(value, (str, bytes, bytearray)):
            return
        if is_descriptor(value):
            yield value
            return
        identity = id(value)
        if identity in seen:
            return
        seen.add(identity)
        if isinstance(value, Mapping):
            for name in ("models", "data"):
                if name in value:
                    yield from walk(value.get(name))
                    return
            for nested in value.values():
                yield from walk(nested)
            return
        for name in ("models", "data"):
            nested = getattr(value, name, None)
            if nested is not None:
                yield from walk(nested)
                return
        try:
            iterator = iter(value)
        except TypeError:
            return
        for nested in iterator:
            yield from walk(nested)

    yield from walk(values)


def utcnow() -> datetime:
    """Return an aware UTC timestamp (kept in one place for test patching)."""

    return datetime.now(timezone.utc)


def datetime_to_text(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def text_to_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def json_dumps(value: Any) -> str:
    """Stable JSON representation used for snapshots and SQL parameters."""

    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def json_loads(value: str | bytes | None, default: Any = None) -> Any:
    if value in (None, "", b""):
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


try:  # ``StrEnum`` is available on Python 3.11+, while the project supports 3.10.
    from enum import StrEnum
except ImportError:  # pragma: no cover - exercised only on Python 3.10
    class StrEnum(str, Enum):
        pass


class InboundState(StrEnum):
    RECEIVED = "received"
    STORED = "stored"
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    AWAITING_CONFIRMATION = "awaiting_confirmation"
    CONFIRMED = "confirmed"
    EXPIRED = "expired"
    TASK_QUEUED = "task_queued"


class TaskState(StrEnum):
    QUEUED = "queued"
    DISPATCHING = "dispatching"
    # Public compatibility adapters historically called the pre-runtime
    # dispatch state ``claimed``.  Schema v26 stores the canonical
    # ``dispatching`` spelling and the SQLite row converter maps it back for
    # those adapters until the in-process executor is removed.
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"


class ExecutionState(StrEnum):
    QUEUED = "queued"
    DISPATCHING = "dispatching"
    CLAIMED = "claimed"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"


class EventVisibility(StrEnum):
    INTERNAL = "internal"
    USER = "user"


class EventPriority(IntEnum):
    SILENT = 0
    NORMAL = 1
    NOTIFY = 2
    ATTENTION = 3


class DeliveryMode(StrEnum):
    INBOX_ONLY = "inbox_only"
    PUSH_ELIGIBLE = "push_eligible"
    REQUIRES_ATTENTION = "requires_attention"


class OutboxState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    SENDING = "sending"
    SENT = "sent"
    RETRY_WAIT = "retry_wait"
    FAILED_PERMANENT = "failed_permanent"
    DELIVERY_UNKNOWN = "delivery_unknown"


class ReplyFragmentState(StrEnum):
    """Durable eligibility/allocation state for one channel send fragment."""

    RETAINED = "retained"
    ALLOCATED = "allocated"
    DEFERRED_QUOTA = "deferred_quota"
    INBOX_ONLY = "inbox_only"


class ReplyAggregateState(StrEnum):
    """Durable construction state for one bounded wire-text aggregate."""

    OPEN = "open"
    SEALED = "sealed"


class PresentationState(StrEnum):
    UNSEEN = "unseen"
    PRESENTED = "presented"
    ACKNOWLEDGED = "acknowledged"


class MailboxState(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    CLAIMED = "claimed"
    PROCESSING = "processing"
    PROCESSED = "processed"
    REJECTED = "rejected"
    DEAD_LETTER = "dead_letter"
    EXPIRED = "expired"
    ORPHANED_MAILBOX = "orphaned_mailbox"


class MailboxOrphanReviewAction(StrEnum):
    """Administrator action for one exact orphaned mailbox invocation."""

    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


class MailboxOrphanReviewOutcome(StrEnum):
    """Durable result of an authenticated mailbox-orphan review."""

    RETRIED = "retried"
    DEAD_LETTERED = "dead_lettered"
    REJECTED_EXPIRED = "rejected_expired"
    REJECTED_AGENT_UNAVAILABLE = "rejected_agent_unavailable"
    REJECTED_QUEUE_FULL = "rejected_queue_full"


class InvocationState(StrEnum):
    """Authoritative state shared by task and mailbox runtime work."""

    QUEUED = "queued"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"


class InvocationWorkKind(StrEnum):
    TASK = "task"
    MAILBOX = "mailbox"


class DispatchBackend(StrEnum):
    COMPATIBILITY = "compatibility"
    CHILD = "child"


class DispatchDecisionKind(StrEnum):
    GRANT = "grant"
    ABORT = "abort"
    REJECTION = "rejection"


class DispatchSourceState(StrEnum):
    """Concrete aggregate state produced by an immutable dispatch decision.

    Invocation state alone cannot distinguish a rejected mailbox from one
    whose absolute lifetime elapsed while it was reserved.  Persisting the
    actual task/mailbox projection keeps that distinction typed and replayable
    without interpreting an operator-facing ``decision_code``.
    """

    QUEUED = "queued"
    RUNNING = "running"
    CANCELLED = "cancelled"
    FAILED = "failed"
    PENDING = "pending"
    PROCESSING = "processing"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ExecutionSlotState(StrEnum):
    ACTIVE = "active"
    RELEASED = "released"


class ExecutionSlotKind(StrEnum):
    """Kind of work holding an Agent's single shared execution slot."""

    INVOCATION = "invocation"
    CONTROL = "control"


class AgentLifecycleState(StrEnum):
    """Durable registration state for one Agent incarnation."""

    ENABLED = "enabled"
    DISABLED = "disabled"
    RETIRING = "retiring"
    TOMBSTONED = "tombstoned"


class AgentDesiredProcessState(StrEnum):
    """Supervisor reconciliation target for one Agent incarnation."""

    RUNNING = "running"
    STOPPED = "stopped"


class AgentProcessState(StrEnum):
    """Durable observation of one child-process generation."""

    STARTING = "starting"
    READY = "ready"
    BUSY = "busy"
    CONTROL_BUSY = "control_busy"
    BACKOFF = "backoff"
    QUIESCING = "quiescing"
    STOPPING = "stopping"
    STOPPED = "stopped"


class MediaDeliveryState(StrEnum):
    """Durable outgoing-attachment upload/send lifecycle."""

    LOCAL = "local"
    READY = "ready"
    UPLOAD_PENDING = "upload_pending"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    SEND_PENDING = "send_pending"
    SENT = "sent"
    FAILED = "failed"


@dataclass(frozen=True)
class OutgoingMediaRecord:
    media_id: str
    attachment_id: str
    channel: str
    bot_id: str = ""
    external_user_id: str = ""
    outbox_id: str | None = None
    state: MediaDeliveryState = MediaDeliveryState.READY
    idempotency_key: str = ""
    remote_id: str | None = None
    upload_param: str | None = None
    encryption_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    claimed_by: str | None = None
    claim_token: str | None = None
    lease_expires_at: datetime | None = None
    attempts: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    uploaded_at: datetime | None = None
    sent_at: datetime | None = None
    # A scoped WeChat media operation is subordinate to the canonical
    # user-outbox SendMsg.  These values are overlaid from that parent when a
    # media worker atomically claims both rows; they are not a second delivery
    # identity stored on the upload child itself.
    reply_slot_id: str | None = None
    reply_ordinal: int | None = None
    client_id: str = ""
    contextless_client_id: str | None = None
    active_wire_variant: str = "primary"
    from_user_id: str = ""
    context_token: str | None = None
    reply_target: Mapping[str, Any] = field(default_factory=dict)

    @property
    def delivery_id(self) -> str:
        if self.reply_slot_id and self.outbox_id:
            return self.outbox_id
        return self.media_id

    @property
    def wire_client_id(self) -> str:
        if self.active_wire_variant == "contextless":
            return str(self.contextless_client_id or self.client_id)
        return self.client_id


@dataclass(frozen=True)
class ReplyTarget:
    """Durable destination for a user-facing response."""

    channel: str = ""
    bot_id: str = ""
    external_user_id: str = ""
    session_id: str = "default"
    source_message_id: str | None = None
    source_sequence: int | None = None
    context_token: str | None = None

    def __post_init__(self) -> None:
        if not self.session_id:
            object.__setattr__(self, "session_id", "default")

    def to_dict(self) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "bot_id": self.bot_id,
            "external_user_id": self.external_user_id,
            "session_id": self.session_id,
            "source_message_id": self.source_message_id,
            "source_sequence": self.source_sequence,
            "context_token": self.context_token,
        }

    @classmethod
    def from_value(cls, value: Any) -> "ReplyTarget":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            fields = {name: value.get(name) for name in cls.__dataclass_fields__}
            return cls(**fields)
        return cls()


@dataclass(frozen=True)
class InboundMessage:
    channel: str
    bot_id: str
    external_user_id: str
    external_message_id: str
    text: str = ""
    session_id: str = "default"
    source_sequence: int | None = None
    context_token: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    received_at: datetime | str | None = None
    message_id: str = ""
    status: InboundState = InboundState.RECEIVED
    reply_target: ReplyTarget | None = None
    task_id: str | None = None

    def __post_init__(self) -> None:
        # Keep provenance for replay validation without exposing transport
        # bookkeeping as part of the public envelope/schema.  A replay that
        # omitted these generated values must not assert a new UUID/clock
        # value, while explicitly supplied values remain immutable.
        message_id_supplied = bool(self.message_id)
        received_at_supplied = self.received_at is not None
        if not self.message_id:
            object.__setattr__(self, "message_id", str(uuid.uuid4()))
        if self.received_at is None:
            object.__setattr__(self, "received_at", utcnow())
        object.__setattr__(self, "_message_id_supplied", message_id_supplied)
        object.__setattr__(self, "_received_at_supplied", received_at_supplied)

    @property
    def dedupe_identity(self) -> tuple[str, str, str]:
        return (self.channel, self.bot_id, self.external_message_id)

    def target(self) -> ReplyTarget:
        return self.reply_target or ReplyTarget(
            channel=self.channel,
            bot_id=self.bot_id,
            external_user_id=self.external_user_id,
            session_id=self.session_id,
            source_message_id=self.external_message_id,
            source_sequence=self.source_sequence,
            context_token=self.context_token,
        )


@dataclass(frozen=True)
class AgentTask:
    """Immutable task snapshot handed to an AgentRuntime."""

    task_id: str = ""
    execution_id: str = ""
    agent_id: str = "codex"
    conversation_id: str = ""
    thread_id: str | None = None
    mode_id: str = "chat"
    profile_version: int = 1
    policy_version: int = 1
    model: str = ""
    reasoning_effort: str = ""
    reply_target: ReplyTarget = field(default_factory=ReplyTarget)
    inputs: Mapping[str, Any] = field(default_factory=dict)
    request_id: str | None = None
    inbound_message_id: str | None = None
    dedupe_key: str | None = None
    parent_task_id: str | None = None
    child_depth: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def with_execution(self, execution_id: str) -> "AgentTask":
        values = asdict(self)
        values["execution_id"] = execution_id
        values["reply_target"] = self.reply_target
        return AgentTask(**values)


@dataclass(frozen=True)
class AgentEvent:
    task_id: str
    sequence: int = 0
    event_type: str = "agent_message"
    visibility: EventVisibility = EventVisibility.USER
    priority: EventPriority = EventPriority.NORMAL
    content: str = ""
    attachments: tuple[str, ...] = ()
    created_at: datetime | str | None = None
    execution_id: str | None = None
    destination_agent_id: str | None = None
    request_id: str | None = None
    reply_to_id: str | None = None
    causation_id: str | None = None
    event_id: str = ""
    source_item_id: str | None = None
    source_item_type: str | None = None
    source_item_ordinal: int | None = None

    def __post_init__(self) -> None:
        if not self.event_id:
            object.__setattr__(self, "event_id", str(uuid.uuid4()))
        if self.created_at is None:
            object.__setattr__(self, "created_at", utcnow())

    @property
    def text(self) -> str:
        """Compatibility alias for channel/runtime adapters."""
        return self.content

    @classmethod
    def text_event(
        cls,
        task_id: str,
        content: str,
        *,
        sequence: int = 0,
        visibility: EventVisibility = EventVisibility.USER,
        priority: EventPriority = EventPriority.NORMAL,
        execution_id: str | None = None,
        event_type: str = "message",
        source_item_id: str | None = None,
        source_item_type: str | None = None,
        source_item_ordinal: int | None = None,
    ) -> "AgentEvent":
        """Construct a text event using the SDK-independent contract shape."""

        return cls(
            task_id=task_id,
            sequence=sequence,
            event_type=event_type,
            visibility=visibility,
            priority=priority,
            content=content,
            execution_id=execution_id,
            source_item_id=source_item_id,
            source_item_type=source_item_type,
            source_item_ordinal=source_item_ordinal,
        )

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly immutable event snapshot.

        The SQLite adapter deliberately accepts both this model and the
        contract model in ``src.agents.base``.  Keeping serialization on the
        value itself prevents ``repr(dataclass)`` from leaking into durable
        task results when callers pass a runtime-model result directly.
        """
        return {
            "event_id": self.event_id,
            "task_id": self.task_id,
            "sequence": int(self.sequence),
            "event_type": self.event_type,
            "visibility": getattr(self.visibility, "value", self.visibility),
            "priority": int(getattr(self.priority, "value", self.priority)),
            "content": self.content,
            "attachments": list(self.attachments),
            "created_at": datetime_to_text(self.created_at),
            "execution_id": self.execution_id,
            "destination_agent_id": self.destination_agent_id,
            "request_id": self.request_id,
            "reply_to_id": self.reply_to_id,
            "causation_id": self.causation_id,
            "source_item_id": self.source_item_id,
            "source_item_type": self.source_item_type,
            "source_item_ordinal": self.source_item_ordinal,
        }

    to_dict = as_dict


@dataclass(frozen=True)
class TaskEvent:
    event_id: str
    task_id: str
    sequence: int
    event_type: str
    visibility: EventVisibility
    priority: EventPriority
    content: str
    attachments: tuple[str, ...] = ()
    created_at: datetime | None = None
    execution_id: str | None = None
    destination_agent_id: str | None = None
    request_id: str | None = None
    reply_to_id: str | None = None
    causation_id: str | None = None
    source_item_id: str | None = None
    source_item_type: str | None = None
    source_item_ordinal: int | None = None


@dataclass(frozen=True)
class TaskRecord:
    task_id: str
    state: TaskState
    agent_id: str
    agent_incarnation: int
    conversation_id: str
    mode_id: str
    profile_version: int
    policy_version: int
    created_at: datetime | None
    updated_at: datetime | None
    execution_id: str | None = None
    inbound_message_id: str | None = None
    dedupe_key: str | None = None
    thread_id: str | None = None
    model: str = ""
    reasoning_effort: str = ""
    reply_target: ReplyTarget = field(default_factory=ReplyTarget)
    inputs: Mapping[str, Any] = field(default_factory=dict)
    claimed_by: str | None = None
    claim_token: str | None = field(default=None, repr=False)
    lease_expires_at: datetime | None = None
    attempts: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    result: Any = None
    parent_task_id: str | None = None
    child_depth: int = 0
    request_id: str | None = None
    terminal_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # A command-origin scope consumed atomically by the next execution claim
    # (for example explicit `/retry`).  It is cleared when snapshotted into
    # TaskExecution and never rewrites the task's immutable ReplyTarget.
    pending_delivery_reply_scope_id: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in {
            TaskState.COMPLETED,
            TaskState.FAILED,
            TaskState.INTERRUPTED,
            TaskState.CANCELLED,
        }

    @property
    def status(self) -> TaskState:
        """Compatibility spelling used by channel-facing task formatters."""
        return self.state

    @property
    def id(self) -> str:
        return self.task_id


@dataclass(frozen=True)
class TaskExecution:
    execution_id: str
    task_id: str
    attempt: int
    state: ExecutionState
    agent_id: str = "codex"
    agent_incarnation: int = 1
    dispatch_backend: DispatchBackend = DispatchBackend.COMPATIBILITY
    worker_id: str | None = None
    claim_token: str | None = None
    lease_expires_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_error: str | None = None
    external_turn_id: str | None = None
    delivery_reply_scope_id: str | None = None


@dataclass(frozen=True)
class TaskClaim:
    task: TaskRecord
    execution: TaskExecution
    claim_token: str

    @property
    def task_id(self) -> str:
        return self.task.task_id

    @property
    def execution_id(self) -> str:
        return self.execution.execution_id

    def __iter__(self):
        """Allow lightweight integrations to unpack ``(task, token)``."""
        yield self.task
        yield self.claim_token


@dataclass(frozen=True)
class UserOutboxItem:
    outbox_id: str
    task_id: str | None
    event_id: str | None
    channel: str
    bot_id: str
    external_user_id: str
    session_id: str
    agent_id: str
    reply_target: ReplyTarget
    content: str
    priority: EventPriority = EventPriority.NORMAL
    delivery_mode: DeliveryMode = DeliveryMode.PUSH_ELIGIBLE
    # Whether automatic push is currently eligible for this projection.  It
    # is intentionally distinct from ``presentation`` and ``state``.
    notify_enabled: bool = True
    # Foreground responses are interactive replies to the user input that
    # created the task.  Persist this decision at projection time so a later
    # Agent switch cannot reclassify the row while applying /notify changes.
    foreground: bool = False
    state: OutboxState = OutboxState.PENDING
    presentation: PresentationState = PresentationState.UNSEEN
    client_id: str = ""
    attempts: int = 0
    claim_token: str | None = None
    claimed_by: str | None = None
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime | None = None
    sent_at: datetime | None = None
    # Attachment IDs are projections, not binary payloads.  Keeping them on
    # the outbox record lets a channel adapter decide whether/how to upload
    # them without losing media metadata during a retry.
    attachments: tuple[Any, ...] = ()
    # Canonical WeChat reply allocation.  Legacy/direct integrations may
    # still create an intentionally unscoped outbox row, in which case these
    # values remain ``None``.
    reply_scope_id: str | None = None
    reply_slot_id: str | None = None
    reply_ordinal: int | None = None
    reply_candidate_id: str | None = None
    reply_fragment_id: str | None = None
    reply_aggregate_id: str | None = None
    # The sender identity and the only permitted wire-ID fallback are
    # delivery properties, not part of ReplyTarget identity.
    from_user_id: str = ""
    contextless_client_id: str | None = None
    active_wire_variant: str = "primary"

    @property
    def visibility(self) -> EventVisibility:
        """Outbox rows are user projections by construction."""
        return EventVisibility.USER

    @property
    def delivery_id(self) -> str:
        return self.outbox_id

    @property
    def target(self) -> ReplyTarget:
        return self.reply_target

    @property
    def wire_client_id(self) -> str:
        """Return the durable wire ID selected by the one-way variant fence."""

        if self.active_wire_variant == "contextless":
            return str(self.contextless_client_id or self.client_id)
        return self.client_id


@dataclass(frozen=True)
class ReplyScopeRecord:
    """Ten-send allowance owned by one exact stored inbound envelope."""

    reply_scope_id: str
    inbound_message_id: str
    channel: str
    bot_id: str
    external_user_id: str
    session_id: str
    source_identity: str
    capacity: int = 10
    used_slots: int = 0
    created_at: datetime | None = None

    @property
    def remaining_slots(self) -> int:
        return max(0, int(self.capacity) - int(self.used_slots))


@dataclass(frozen=True)
class ReplyCandidateRecord:
    """One stable completed item or explicit command reply candidate."""

    reply_candidate_id: str
    origin_reply_scope_id: str
    source_key: str
    channel: str
    bot_id: str
    external_user_id: str
    session_id: str
    agent_id: str = "codex"
    task_id: str | None = None
    execution_id: str | None = None
    event_id: str | None = None
    source_item_id: str | None = None
    source_item_type: str | None = None
    source_item_ordinal: int | None = None
    content: str = ""
    attachments: tuple[Any, ...] = ()
    priority: EventPriority = EventPriority.NORMAL
    delivery_mode: DeliveryMode = DeliveryMode.PUSH_ELIGIBLE
    notify_enabled: bool = True
    foreground: bool = False
    # Inbox presentation belongs to the stable completed item, not to a
    # transport outbox row: notification-suppressed items deliberately have
    # no allocated SendMsg/outbox until a later inbound presents them.
    presentation: PresentationState = PresentationState.UNSEEN
    created_at: datetime | None = None
    presented_at: datetime | None = None

    @property
    def presentation_id(self) -> str:
        """Return the durable identity accepted by command presentation."""

        return self.reply_candidate_id


@dataclass(frozen=True)
class ReplyFragmentRecord:
    """Lossless transport fragment retained independently from allocation."""

    reply_fragment_id: str
    reply_candidate_id: str
    origin_reply_scope_id: str
    fragment_ordinal: int
    fragment_kind: str
    content: str = ""
    attachments: tuple[Any, ...] = ()
    state: ReplyFragmentState = ReplyFragmentState.RETAINED
    delivery_reply_scope_id: str | None = None
    reply_slot_id: str | None = None
    deferred_sequence: int | None = None
    created_at: datetime | None = None
    allocated_at: datetime | None = None
    reply_aggregate_id: str | None = None


@dataclass(frozen=True)
class ReplySlotRecord:
    """One immutable logical WeChat SendMsg identity and reply ordinal."""

    reply_slot_id: str
    reply_scope_id: str
    reply_ordinal: int
    reply_fragment_id: str
    delivery_id: str
    client_id: str
    contextless_client_id: str | None = None
    active_wire_variant: str = "primary"
    outbox_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    reply_aggregate_id: str | None = None

    @property
    def wire_client_id(self) -> str:
        if self.active_wire_variant == "contextless":
            return str(self.contextless_client_id or self.client_id)
        return self.client_id


@dataclass(frozen=True)
class ReplyProjectionResult:
    """Idempotent result of candidate projection or a `/recv` drain batch."""

    candidate: ReplyCandidateRecord | None = None
    fragments: tuple[ReplyFragmentRecord, ...] = ()
    slots: tuple[ReplySlotRecord, ...] = ()
    outbox_items: tuple[UserOutboxItem, ...] = ()
    batch_id: str | None = None
    replayed: bool = False


@dataclass(frozen=True)
class ReplyAggregateRecord:
    """One durable, bounded wire unit before or after deterministic sealing."""

    reply_aggregate_id: str
    aggregation_key_hash: str
    aggregation_key: Mapping[str, Any]
    origin_reply_scope_id: str
    channel: str
    bot_id: str
    external_user_id: str
    session_id: str
    reply_target: ReplyTarget
    provenance_kind: str
    provenance_id: str
    agent_id: str
    sender_format: str
    sender_prefix: str
    notify_enabled: bool
    foreground: bool
    priority: EventPriority
    delivery_mode: DeliveryMode
    presentation_class: str
    renderer_version: str
    wire_kind: str
    state: ReplyAggregateState
    content: str
    content_hash: str
    payload_hash: str
    character_count: int
    first_source_sequence: int
    last_source_sequence: int
    representative_reply_candidate_id: str
    task_id: str | None = None
    execution_id: str | None = None
    command_id: str | None = None
    attachments: tuple[Any, ...] = ()
    flush_due_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    sealed_at: datetime | None = None
    seal_reason: str | None = None
    wire_reply_fragment_id: str | None = None


@dataclass(frozen=True)
class ReplyAggregateMemberRecord:
    """Ordered source-candidate slice retained inside one wire aggregate."""

    reply_aggregate_member_id: str
    reply_aggregate_id: str
    member_ordinal: int
    reply_candidate_id: str
    source_fragment_ordinal: int
    source_reply_fragment_id: str | None
    source_sequence: int
    source_character_start: int
    source_character_count: int
    prefix_before: str
    separator_before: str
    rendered_content: str
    rendered_content_hash: str
    source_rendered_content_hash: str
    source_rendered_character_count: int
    aggregate_character_start: int
    created_at: datetime | None = None


@dataclass(frozen=True)
class ReplyAggregationResult:
    """Idempotent result of appending, sealing, or deadline reconciliation."""

    aggregates: tuple[ReplyAggregateRecord, ...] = ()
    members: tuple[ReplyAggregateMemberRecord, ...] = ()
    sealed_aggregates: tuple[ReplyAggregateRecord, ...] = ()
    open_aggregate: ReplyAggregateRecord | None = None
    replayed: bool = False


@dataclass(frozen=True)
class AgentMailboxItem:
    mailbox_id: str
    message_id: str
    request_id: str
    source_agent_id: str
    destination_agent_id: str
    content: str
    state: MailboxState = MailboxState.PENDING
    claim_token: str | None = field(default=None, repr=False)
    claimed_by: str | None = None
    lease_expires_at: datetime | None = None
    attempts: int = 0
    created_at: datetime | None = None
    last_error: str | None = None
    reply_to_id: str | None = None
    causation_id: str | None = None
    task_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    # Immutable execution context selected when the mailbox envelope was
    # accepted.  This is deliberately separate from ``payload``: request
    # payloads are Agent data, while this snapshot controls runtime identity
    # and policy and must not be writable by the destination Agent.
    execution_snapshot: Mapping[str, Any] = field(default_factory=dict)
    destination_agent_incarnation: int = 1
    current_invocation_id: str | None = None
    expires_at: datetime | None = None

    @property
    def destination(self) -> str:
        return self.destination_agent_id

    @property
    def runtime_snapshot(self) -> Mapping[str, Any]:
        """Compatibility alias used by older mailbox adapters."""

        return self.execution_snapshot


@dataclass(frozen=True)
class AgentInvocationRecord:
    """One durable, generic unit of task or mailbox runtime work."""

    invocation_id: str
    work_kind: InvocationWorkKind
    work_id: str
    agent_id: str
    agent_incarnation: int
    state: InvocationState
    dispatch_backend: DispatchBackend
    ready_sequence: int
    task_id: str | None = None
    execution_id: str | None = None
    mailbox_id: str | None = None
    claimed_by: str | None = None
    claim_token: str | None = field(default=None, repr=False)
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None
    admission_released_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    terminal_at: datetime | None = None
    last_error: str | None = None
    expires_at: datetime | None = None


@dataclass(frozen=True)
class AgentInvocationEventRecord:
    """Append-only history for a generic task or mailbox invocation."""

    invocation_event_id: str
    invocation_id: str
    event_sequence: int
    event_kind: str
    previous_state: InvocationState | None
    new_state: InvocationState
    source_kind: str
    source_id: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass(frozen=True)
class MailboxOrphanReviewRecord:
    """Immutable authorization, decision, and effect for one orphan review."""

    mailbox_maintenance_id: str
    payload_hash: str
    authorization_scheme: str
    authorization_grant_digest: str
    mailbox_id: str
    mailbox_message_id: str
    expected_current_invocation_id: str
    action: MailboxOrphanReviewAction
    actor: str
    reason: str
    authorized_at: datetime
    authorization_source: str
    administrator_authorized: bool
    outcome: MailboxOrphanReviewOutcome
    replacement_invocation_id: str | None = None
    reviewed_at: datetime | None = None
    replayed: bool = False

    @property
    def accepted(self) -> bool:
        return self.outcome in {
            MailboxOrphanReviewOutcome.RETRIED,
            MailboxOrphanReviewOutcome.DEAD_LETTERED,
        }


@dataclass(frozen=True)
class AgentAdmissionCounterRecord:
    agent_id: str
    agent_incarnation: int
    unfinished_count: int
    next_ready_sequence: int
    updated_at: datetime | None = None


@dataclass(frozen=True)
class GlobalAgentAdmissionCounterRecord:
    """Account-wide unfinished Agent invocation debit."""

    singleton: int
    unfinished_count: int
    updated_at: datetime | None = None


@dataclass(frozen=True)
class AgentExecutionSlotRecord:
    slot_id: str
    agent_id: str
    agent_incarnation: int
    slot_sequence: int
    slot_kind: ExecutionSlotKind
    state: ExecutionSlotState
    dispatch_backend: DispatchBackend
    worker_generation: int
    invocation_id: str | None = None
    acquired_at: datetime | None = None
    released_at: datetime | None = None


@dataclass(frozen=True)
class AgentDispatchAttemptRecord:
    dispatch_attempt_id: str
    invocation_id: str
    slot_id: str
    agent_id: str
    agent_incarnation: int
    worker_generation: int
    supervisor_epoch: int
    dispatch_backend: DispatchBackend
    claim_token_hash: str
    lease_identity: str
    lease_expires_at: datetime | None
    invocation_job_identity: str
    grant_issued_at: datetime | None = None
    abort_committed_at: datetime | None = None
    rejection_committed_at: datetime | None = None
    decision_code: str | None = None
    decision_outcome_state: InvocationState | None = None
    decision_source_state: DispatchSourceState | None = None
    decision_next_attempt_at: datetime | None = None
    decision_metadata_legacy: bool = False
    runtime_stopped_at: datetime | None = None
    invocation_job_empty_at: datetime | None = None
    cleanup_proof_hash: str | None = None
    created_at: datetime | None = None

    @property
    def decision_kind(self) -> DispatchDecisionKind | None:
        if self.grant_issued_at is not None:
            return DispatchDecisionKind.GRANT
        if self.abort_committed_at is not None:
            return DispatchDecisionKind.ABORT
        if self.rejection_committed_at is not None:
            return DispatchDecisionKind.REJECTION
        return None

    @property
    def cleanup_proven(self) -> bool:
        return (
            self.runtime_stopped_at is not None
            and self.invocation_job_empty_at is not None
            and self.cleanup_proof_hash is not None
        )


@dataclass(frozen=True)
class AgentDispatchReservation:
    """Atomic pre-grant assignment snapshot returned to the supervisor."""

    invocation: AgentInvocationRecord
    slot: AgentExecutionSlotRecord
    attempt: AgentDispatchAttemptRecord
    claim_token: str = field(repr=False)
    task: TaskRecord | None = None
    mailbox: AgentMailboxItem | None = None
    replayed: bool = False


@dataclass(frozen=True)
class AgentResult:
    """SDK-independent terminal result accepted by the durable store.

    ``content``/``output`` are retained as aliases because early runtime
    callers used ``output`` while the Agent protocol uses ``content``.  The
    optional identifiers make this value useful when passed directly to
    ``SQLiteStore.complete_task`` rather than through ``TaskWorker``.
    """

    status: str = "completed"
    output: str = ""
    result: Any = None
    error: str | None = None
    events: tuple[AgentEvent, ...] = ()
    task_id: str = ""
    execution_id: str | None = None
    thread_id: str | None = None
    interrupted: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)
    content: str = ""

    def __post_init__(self) -> None:
        if not self.output and self.content:
            object.__setattr__(self, "output", self.content)
        elif self.output and not self.content:
            object.__setattr__(self, "content", self.output)
        if self.events is None:
            object.__setattr__(self, "events", ())
        elif not isinstance(self.events, tuple):
            object.__setattr__(self, "events", tuple(self.events))
        if isinstance(self.metadata, Mapping):
            object.__setattr__(self, "metadata", dict(self.metadata))

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "output": self.output,
            "content": self.content,
            "result": self.result,
            "error": self.error,
            "events": [
                event.as_dict() if hasattr(event, "as_dict") else event
                for event in self.events
            ],
            "execution_id": self.execution_id,
            "thread_id": self.thread_id,
            "interrupted": self.interrupted,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class AgentLifecycleRecord:
    """One retained Agent incarnation and its durable desired process state."""

    agent_id: str
    agent_incarnation: int
    profile_version: int
    lifecycle_state: AgentLifecycleState
    desired_process_state: AgentDesiredProcessState
    provenance_kind: str
    provenance_profile_version: int | None = None
    provenance: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    retiring_at: datetime | None = None
    tombstoned_at: datetime | None = None

    @property
    def enabled(self) -> bool:
        return self.lifecycle_state is AgentLifecycleState.ENABLED

    @property
    def wants_process(self) -> bool:
        return self.desired_process_state is AgentDesiredProcessState.RUNNING


@dataclass(frozen=True)
class AgentLifecycleEventRecord:
    """One append-only lifecycle projection transition or migration seed."""

    lifecycle_event_id: str
    idempotency_key: str
    agent_id: str
    agent_incarnation: int
    event_sequence: int
    event_kind: str
    previous_profile_version: int | None
    previous_lifecycle_state: AgentLifecycleState | None
    previous_desired_process_state: AgentDesiredProcessState | None
    new_profile_version: int
    new_lifecycle_state: AgentLifecycleState
    new_desired_process_state: AgentDesiredProcessState
    actor_kind: str
    actor_id: str | None
    source_kind: str
    source_id: str
    provenance: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass(frozen=True)
class AgentProcessRecord:
    """Durable evidence for one child-process generation.

    Generation rows are retained after stop.  PID and process-group values are
    diagnostics only; epoch, incarnation, generation, lease, lifetime-lock,
    handshake, and cleanup evidence are the ownership fences.
    """

    agent_id: str
    agent_incarnation: int
    worker_generation: int
    supervisor_epoch: int
    observed_state: AgentProcessState
    generation_capability_hash: str
    lifetime_lock_identity: str
    lifetime_lock_acquired_at: datetime | None
    process_lease_identity: str
    process_lease_token_hash: str
    lease_expires_at: datetime | None
    lease_ended_at: datetime | None
    started_at: datetime | None
    pid: int | None = None
    process_group_id: int | None = None
    kernel_process_birth_id: str | None = None
    hello_frame_id: str | None = None
    hello_payload_hash: str | None = None
    capabilities_frame_id: str | None = None
    capabilities_payload_hash: str | None = None
    capability_snapshot_hash: str | None = None
    handshake_committed_at: datetime | None = None
    ready_frame_id: str | None = None
    ready_payload_hash: str | None = None
    ready_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    stopped_by_supervisor_epoch: int | None = None
    cleanup_proof: Mapping[str, Any] = field(default_factory=dict)
    cleanup_proof_hash: str | None = None
    cleanup_proved_at: datetime | None = None
    stopped_at: datetime | None = None
    stop_reason: str | None = None
    last_exit_code: int | None = None
    last_error: str | None = None

    @property
    def active(self) -> bool:
        return self.observed_state is not AgentProcessState.STOPPED

    @property
    def ready_handshake_committed(self) -> bool:
        """Whether this row contains READY evidence, not scheduling authority."""

        return self.ready_at is not None


@dataclass(frozen=True)
class SupervisorEpochRecord:
    """One durable supervisor ownership generation for a runtime database."""

    epoch: int
    owner_instance_id: str
    channel: str
    bot_id: str
    started_at: datetime | None = None
    stopped_at: datetime | None = None
    stop_reason: str | None = None

    @property
    def active(self) -> bool:
        return self.stopped_at is None


@dataclass(frozen=True)
class RecoveryReport:
    tasks_orphaned: int = 0
    outbox_requeued: int = 0
    outbox_unknown: int = 0
    mailbox_requeued: int = 0
    missing_attachments: int = 0
    # Outgoing media has its own lease/state machine.  Keep its recovery
    # count separate from text outbox rows so operators can distinguish an
    # unknown text delivery from an upload that is safe to retry by its
    # idempotency key.
    media_requeued: int = 0
    # Pending mailbox expiry is terminal, unlike lease recovery which orphans
    # a possibly-started invocation.  Keep its count distinct and append this
    # field so positional compatibility with older callers is preserved.
    mailbox_expired: int = 0

    @property
    def orphaned_tasks(self) -> int:
        return self.tasks_orphaned

    @property
    def outgoing_media_requeued(self) -> int:
        """Compatibility spelling for callers that name the projection."""
        return self.media_requeued


@dataclass(frozen=True)
class InboundAcceptance:
    """Result of durable ingress and optional task creation."""

    inbound: InboundMessage
    task: TaskRecord | None = None
    created: bool = True
    duplicate: bool = False
    # Legacy audio-candidate IDs are retained for store/API compatibility.
    # Current WeChat voice ingress queues the transcript directly and leaves
    # this tuple empty.
    confirmation_ids: tuple[str, ...] = ()

    @property
    def accepted(self) -> bool:
        return not self.duplicate

    @property
    def task_id(self) -> str | None:
        return self.task.task_id if self.task else None
