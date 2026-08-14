"""SDK-independent Agent runtime contracts.

These objects are deliberately ordinary dataclasses.  They are used at the
boundary between the durable task store and an Agent runtime, so they contain
only application values and never SDK enums or model classes.  A task is a
snapshot: changing a route, profile, or mode after construction must not alter
the object that is already being executed.
"""

from __future__ import annotations

import inspect
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import IntEnum
from types import MappingProxyType

try:  # ``StrEnum`` was added in Python 3.11; the project supports 3.10.
    from enum import StrEnum
except ImportError:  # pragma: no cover - exercised only on Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        pass
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence, runtime_checkable


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp used by runtime records."""

    return datetime.now(timezone.utc)


class EventVisibility(StrEnum):
    """Where an event may be projected.

    ``internal`` events are useful for diagnostics and Agent collaboration but
    must never be sent to a channel.  ``user`` events are eligible for the
    user-outbox projection (subject to notification policy).
    """

    INTERNAL = "internal"
    USER = "user"


class EventPriority(IntEnum):
    """Canonical user notification priorities from the runtime plan."""

    SILENT = 0
    NORMAL = 1
    NOTIFY = 2
    ATTENTION = 3


@dataclass(frozen=True, slots=True)
class ReplyTarget:
    """Durable destination for a user-facing response.

    ``context_token`` is deliberately optional: it is a transport hint and is
    not used as the durable identity.  The first six fields form the stable
    channel destination described in ``plan.md``.
    """

    channel: str = ""
    bot_id: str = ""
    external_user_id: str = ""
    session_id: str = "default"
    source_message_id: str = ""
    source_sequence: int | None = None
    context_token: str | None = None

    def __post_init__(self) -> None:
        if not self.session_id:
            object.__setattr__(self, "session_id", "default")

    @property
    def user_id(self) -> str:
        """Compatibility alias used by channel adapters."""

        return self.external_user_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "bot_id": self.bot_id,
            "external_user_id": self.external_user_id,
            "session_id": self.session_id,
            "source_message_id": self.source_message_id,
            "source_sequence": self.source_sequence,
            "context_token": self.context_token,
        }

    # Store/channel adapters historically use ``to_dict``; retaining the alias
    # keeps this SDK-independent value interoperable without importing store
    # models into the Agent package.
    to_dict = as_dict


@dataclass(frozen=True, slots=True)
class AgentTask:
    """Immutable input snapshot passed to :class:`AgentRuntime.run`.

    The first fields intentionally have defaults so callers can construct a
    task from a partial persisted row while migrations are rolling forward.
    ``inputs`` may be a string, a structured mapping, or a sequence containing
    text/media values; the Codex adapter performs the final translation.
    """

    task_id: str
    execution_id: str = ""
    agent_id: str = "codex"
    conversation_id: str = ""
    thread_id: str | None = None
    mode_id: str = "chat"
    profile_version: int | str = 1
    policy_version: int | str = 1
    model: str = ""
    reasoning_effort: str = ""
    reply_target: ReplyTarget | None = None
    inputs: Any = ""
    request_id: str | None = None
    parent_task_id: str | None = None
    child_depth: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        target = self.reply_target
        if isinstance(target, Mapping):
            object.__setattr__(self, "reply_target", ReplyTarget(**dict(target)))
        elif target is not None and not isinstance(target, ReplyTarget):
            for method_name in ("as_dict", "to_dict"):
                method = getattr(target, method_name, None)
                if method is not None:
                    try:
                        converted = method()
                    except TypeError:
                        converted = None
                    if isinstance(converted, Mapping):
                        object.__setattr__(self, "reply_target", ReplyTarget(**dict(converted)))
                        break
        if isinstance(self.inputs, Mapping):
            object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))
        if isinstance(self.metadata, Mapping):
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def with_execution(self, execution_id: str) -> "AgentTask":
        """Return this snapshot with a concrete execution attempt ID."""

        return replace(self, execution_id=execution_id)

    @classmethod
    def from_record(cls, record: Any, **overrides: Any) -> "AgentTask":
        """Build a task from a mapping or an object with matching attributes.

        Stores intentionally return different row/model types across versions;
        accepting both keeps the runtime boundary stable and is useful for
        integrations that use a lightweight fake store in tests.
        """

        if isinstance(record, cls):
            values = {name: getattr(record, name) for name in cls.__dataclass_fields__}
        elif isinstance(record, Mapping):
            values = dict(record)
        else:
            values = {
                name: getattr(record, name)
                for name in cls.__dataclass_fields__
                if hasattr(record, name)
            }
        # Common persisted aliases.
        if "input" in values and "inputs" not in values:
            values["inputs"] = values.pop("input")
        if "reply_to" in values and "reply_target" not in values:
            values["reply_target"] = values.pop("reply_to")
        target = values.get("reply_target")
        if isinstance(target, Mapping):
            values["reply_target"] = ReplyTarget(**target)
        elif target is not None and not isinstance(target, ReplyTarget):
            converted = None
            for method_name in ("as_dict", "to_dict"):
                method = getattr(target, method_name, None)
                if method is not None:
                    try:
                        converted = method()
                    except TypeError:
                        converted = None
                    if isinstance(converted, Mapping):
                        values["reply_target"] = ReplyTarget(**converted)
                        break
        allowed = set(cls.__dataclass_fields__)
        values = {key: value for key, value in values.items() if key in allowed}
        values.update(overrides)
        if "task_id" not in values or not values["task_id"]:
            raise ValueError("AgentTask requires task_id")
        return cls(**values)

    def as_dict(self) -> dict[str, Any]:
        values = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }
        if isinstance(self.inputs, Mapping):
            values["inputs"] = dict(self.inputs)
        if self.reply_target is not None:
            values["reply_target"] = self.reply_target.as_dict()
        if isinstance(self.metadata, Mapping):
            values["metadata"] = dict(self.metadata)
        return values


@dataclass(frozen=True, slots=True)
class AgentEvent:
    """One ordered event emitted while a task is running."""

    task_id: str
    sequence: int = 0
    event_type: str = "message"
    visibility: EventVisibility | str = EventVisibility.USER
    priority: int = int(EventPriority.NORMAL)
    content: str = ""
    attachments: tuple[Any, ...] = ()
    created_at: datetime = field(default_factory=utc_now)
    execution_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    destination_agent_id: str | None = None
    request_id: str | None = None
    reply_to_id: str | None = None
    causation_id: str | None = None
    event_id: str = ""
    # Stable runtime-item provenance.  Codex item IDs are preferred when the
    # SDK supplies one; ``source_item_ordinal`` is the deterministic fallback
    # within one task execution.  Keeping these separate from ``sequence``
    # prevents progress/status events from changing completed-item identity.
    source_item_id: str | None = None
    source_item_type: str | None = None
    source_item_ordinal: int | None = None

    def __post_init__(self) -> None:
        # Be liberal at the boundary while keeping the canonical in-memory
        # representation predictable for stores and hidden integrations.
        visibility = self.visibility
        if not isinstance(visibility, EventVisibility):
            try:
                visibility = EventVisibility(str(visibility).lower())
            except ValueError:
                visibility = EventVisibility.INTERNAL
            object.__setattr__(self, "visibility", visibility)
        if self.attachments is None:
            object.__setattr__(self, "attachments", ())
        elif not isinstance(self.attachments, tuple):
            object.__setattr__(self, "attachments", tuple(self.attachments))
        if isinstance(self.metadata, Mapping):
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        if not self.event_id:
            identity = ":".join(
                (
                    self.task_id,
                    self.execution_id or "",
                    str(self.sequence),
                    self.event_type,
                )
            )
            object.__setattr__(self, "event_id", str(uuid.uuid5(uuid.NAMESPACE_URL, identity)))

    @property
    def text(self) -> str:
        """Alias for integrations that call event payload ``text``."""

        return self.content

    @classmethod
    def text_event(
        cls,
        task_id: str,
        content: str,
        *,
        sequence: int = 0,
        visibility: EventVisibility | str = EventVisibility.USER,
        priority: int = int(EventPriority.NORMAL),
        execution_id: str | None = None,
        event_type: str = "message",
        source_item_id: str | None = None,
        source_item_type: str | None = None,
        source_item_ordinal: int | None = None,
    ) -> "AgentEvent":
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
        return {
            "task_id": self.task_id,
            "sequence": self.sequence,
            "event_type": self.event_type,
            "visibility": self.visibility.value
            if isinstance(self.visibility, EventVisibility)
            else str(self.visibility),
            "priority": int(self.priority),
            "content": self.content,
            "attachments": list(self.attachments),
            "created_at": self.created_at,
            "execution_id": self.execution_id,
            "metadata": dict(self.metadata),
            "destination_agent_id": self.destination_agent_id,
            "request_id": self.request_id,
            "reply_to_id": self.reply_to_id,
            "causation_id": self.causation_id,
            "event_id": self.event_id,
            "source_item_id": self.source_item_id,
            "source_item_type": self.source_item_type,
            "source_item_ordinal": self.source_item_ordinal,
        }


@dataclass(frozen=True, slots=True)
class AgentResult:
    """Terminal result returned by a runtime.

    ``status`` is one of ``completed``, ``failed``, ``interrupted`` or
    ``cancelled`` by convention.  The store may persist the string verbatim.
    """

    task_id: str
    status: str = "completed"
    content: str = ""
    error: str | None = None
    events: tuple[AgentEvent, ...] = ()
    interrupted: bool = False
    execution_id: str | None = None
    thread_id: str | None = None
    usage: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # ``output`` is retained as a constructor alias for store-layer results.
    # ``__post_init__`` keeps both spellings synchronized.
    output: str = ""

    @property
    def text(self) -> str:
        return self.content

    @property
    def ok(self) -> bool:
        return self.status == "completed" and not self.error

    def __post_init__(self) -> None:
        if not self.content and self.output:
            object.__setattr__(self, "content", self.output)
        elif self.content and not self.output:
            object.__setattr__(self, "output", self.content)
        if self.events is None:
            object.__setattr__(self, "events", ())
        elif not isinstance(self.events, tuple):
            object.__setattr__(self, "events", tuple(self.events))
        if isinstance(self.usage, Mapping):
            object.__setattr__(self, "usage", MappingProxyType(dict(self.usage)))
        if isinstance(self.metadata, Mapping):
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "content": self.content,
            "output": self.output,
            "error": self.error,
            "events": [event.as_dict() for event in self.events],
            "interrupted": self.interrupted,
            "execution_id": self.execution_id,
            "thread_id": self.thread_id,
            "usage": dict(self.usage),
            "metadata": dict(self.metadata),
        }


EmitCallback = Callable[[AgentEvent], Awaitable[None]]


@runtime_checkable
class AgentRuntime(Protocol):
    """Runtime interface implemented by Codex and future Agent backends."""

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def run(self, task: AgentTask, emit: EmitCallback) -> AgentResult:
        ...

    async def interrupt(self, task_id: str) -> bool:
        ...


async def emit_if_awaitable(callback: Callable[[AgentEvent], Any], event: AgentEvent) -> None:
    """Invoke an event callback that may be sync or async.

    Runtime integrations commonly use a synchronous test collector.  Keeping
    this helper here avoids every adapter having to duplicate the check.
    """

    result = callback(event)
    if inspect.isawaitable(result):
        await result


# Compatibility aliases used by early prototypes and downstream integrations.
RuntimeTask = AgentTask
RuntimeEvent = AgentEvent
RuntimeResult = AgentResult


__all__ = [
    "AgentEvent",
    "AgentResult",
    "AgentRuntime",
    "AgentTask",
    "EmitCallback",
    "EventPriority",
    "EventVisibility",
    "ReplyTarget",
    "RuntimeEvent",
    "RuntimeResult",
    "RuntimeTask",
    "utc_now",
]
