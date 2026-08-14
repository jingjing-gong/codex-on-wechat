"""Durable-friendly audio transcription confirmation state machine.

Transcription is a suggestion, not user intent.  This module keeps the state
machine independent of a particular speech provider; a SQLite-backed caller
can persist the dataclass fields and use the same transition checks after a
restart.
"""

from __future__ import annotations

import secrets
import inspect
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        pass
from typing import Any, Awaitable, Mapping


def _as_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


class ConfirmationStatus(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CONSUMED = "consumed"


@dataclass(frozen=True, slots=True)
class TranscriptionCandidate:
    confirmation_id: str
    session_key: str
    attachment_id: str
    candidate_text: str
    source: str = "channel"
    confidence: float | None = None
    status: ConfirmationStatus = ConfirmationStatus.PENDING
    created_at: datetime = None  # type: ignore[assignment]
    expires_at: datetime | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    consumed_by_task_id: str | None = None
    route: Mapping[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        now = datetime.now(timezone.utc)
        if self.created_at is None:
            object.__setattr__(self, "created_at", now)
        if self.route is None:
            object.__setattr__(self, "route", {})


class AudioConfirmationManager:
    """Manage candidates and enforce one-time confirmation/consumption.

    The synchronous methods are the dependency-free in-memory state machine.
    When a durable store is supplied, use the ``a*`` methods (for example
    ``await acreate_candidate(...)``); they delegate every transition to the
    store and rebuild a candidate from the durable row.  Keeping the two APIs
    explicit prevents an async SQLite call from being accidentally replaced by
    a process-local mirror.
    """

    def __init__(self, store: Any | None = None, *, default_ttl: float = 300.0):
        self.store = store
        self.default_ttl = default_ttl
        self._items: dict[str, TranscriptionCandidate] = {}

    @staticmethod
    def _id() -> str:
        return secrets.token_urlsafe(12)

    def create_candidate(
        self,
        *,
        session_key: str,
        attachment_id: str,
        candidate_text: str,
        source: str = "channel",
        confidence: float | None = None,
        route: Mapping[str, Any] | None = None,
        ttl: float | None = None,
        confirmation_id: str | None = None,
    ) -> TranscriptionCandidate | Awaitable[TranscriptionCandidate]:
        if self.store is not None:
            return self.acreate_candidate(
                session_key=session_key,
                attachment_id=attachment_id,
                candidate_text=candidate_text,
                source=source,
                confidence=confidence,
                route=route,
                ttl=ttl,
                confirmation_id=confirmation_id,
            )  # type: ignore[return-value]
        if not candidate_text.strip():
            raise ValueError("candidate transcription cannot be empty")
        now = datetime.now(timezone.utc)
        item = TranscriptionCandidate(
            confirmation_id=confirmation_id or self._id(),
            session_key=session_key,
            attachment_id=attachment_id,
            candidate_text=candidate_text,
            source=source,
            confidence=confidence,
            created_at=now,
            expires_at=now + timedelta(seconds=self.default_ttl if ttl is None else ttl),
            route=dict(route or {}),
        )
        if item.confirmation_id in self._items:
            raise ValueError("confirmation id already exists")
        self._items[item.confirmation_id] = item
        return item

    async def acreate_candidate(self, **kwargs: Any) -> TranscriptionCandidate:
        """Create a candidate through the configured durable store."""

        if self.store is None:
            return self.create_candidate(**kwargs)
        method = getattr(self.store, "create_transcription_candidate", None)
        if method is None:
            raise AttributeError("store must implement create_transcription_candidate()")
        route = dict(kwargs.pop("route", {}) or {})
        session_key = kwargs.pop("session_key", None)
        if session_key is not None:
            route.setdefault("session_key", str(session_key))
        ttl = kwargs.pop("ttl", None)
        # A deterministic confirmation ID may be replayed after the first
        # process has committed it.  In that case, do not assert freshly
        # generated clock values against the immutable durable row.  Explicit
        # caller-provided timestamps remain part of the identity check.
        existing = None
        confirmation_id = kwargs.get("confirmation_id")
        if confirmation_id and (
            "created_at" not in kwargs or "expires_at" not in kwargs
        ):
            getter = getattr(self.store, "get_transcription_candidate", None)
            if getter is not None:
                existing = getter(str(confirmation_id))
                if inspect.isawaitable(existing):
                    existing = await existing
        if existing is None:
            now = datetime.now(timezone.utc)
            kwargs.setdefault("created_at", now)
            kwargs.setdefault(
                "expires_at",
                now
                + timedelta(
                    seconds=self.default_ttl if ttl is None else ttl
                ),
            )
        kwargs.setdefault("source", "channel")
        kwargs.setdefault("channel", route.get("channel", ""))
        kwargs.setdefault("bot_id", route.get("bot_id", ""))
        kwargs.setdefault("external_user_id", route.get("external_user_id", ""))
        kwargs.setdefault("session_id", route.get("session_id", "default"))
        if not kwargs.get("attachment_id"):
            kwargs["attachment_id"] = None
        metadata = dict(kwargs.pop("metadata", {}) or {})
        metadata.setdefault("route", route)
        kwargs["metadata"] = metadata
        result = method(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        item = self._from_record(result)
        if session_key and not item.session_key:
            item = replace(item, session_key=str(session_key))
        return item

    create = create_candidate

    def get(self, confirmation_id: str) -> TranscriptionCandidate | None | Awaitable[TranscriptionCandidate | None]:
        if self.store is not None:
            return self.aget(confirmation_id)  # type: ignore[return-value]
        item = self._items.get(confirmation_id)
        if item and item.status == ConfirmationStatus.PENDING and item.expires_at:
            if item.expires_at <= datetime.now(timezone.utc):
                item = replace(
                    item,
                    status=ConfirmationStatus.EXPIRED,
                    resolved_at=datetime.now(timezone.utc),
                )
                self._items[confirmation_id] = item
        return item

    async def aget(self, confirmation_id: str) -> TranscriptionCandidate | None:
        if self.store is None:
            return self.get(confirmation_id)
        method = getattr(self.store, "get_transcription_candidate", None)
        if method is None:
            raise AttributeError("store must implement get_transcription_candidate()")
        result = method(confirmation_id)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return None
        item = self._from_record(result)
        if item.status == ConfirmationStatus.PENDING and item.expires_at and item.expires_at <= datetime.now(timezone.utc):
            resolve = getattr(self.store, "resolve_transcription", None)
            if resolve is not None:
                outcome = resolve(confirmation_id, status="expired", actor="expiry")
                if inspect.isawaitable(outcome):
                    await outcome
                result = method(confirmation_id)
                if inspect.isawaitable(result):
                    result = await result
                item = self._from_record(result)
        return item

    def require(self, confirmation_id: str) -> TranscriptionCandidate | Awaitable[TranscriptionCandidate]:
        if self.store is not None:
            return self.arequire(confirmation_id)  # type: ignore[return-value]
        item = self.get(confirmation_id)
        if item is None:
            raise KeyError(f"unknown confirmation: {confirmation_id}")
        return item

    async def arequire(self, confirmation_id: str) -> TranscriptionCandidate:
        item = await self.aget(confirmation_id)
        if item is None:
            raise KeyError(f"unknown confirmation: {confirmation_id}")
        return item

    def confirm(self, confirmation_id: str, *, actor: str = "user") -> TranscriptionCandidate | Awaitable[TranscriptionCandidate]:
        if self.store is not None:
            return self.aconfirm(confirmation_id, actor=actor)  # type: ignore[return-value]
        item = self.require(confirmation_id)
        if item.status != ConfirmationStatus.PENDING:
            raise ValueError(f"confirmation is {item.status.value}")
        now = datetime.now(timezone.utc)
        item = replace(
            item,
            status=ConfirmationStatus.CONFIRMED,
            resolved_at=now,
            resolved_by=actor,
        )
        self._items[confirmation_id] = item
        return item

    async def aconfirm(self, confirmation_id: str, *, actor: str = "user") -> TranscriptionCandidate:
        if self.store is None:
            return self.confirm(confirmation_id, actor=actor)
        item = await self.arequire(confirmation_id)
        if item.status != ConfirmationStatus.PENDING:
            raise ValueError(f"confirmation is {item.status.value}")
        method = getattr(self.store, "resolve_transcription", None)
        if method is None:
            raise AttributeError("store must implement resolve_transcription()")
        result = method(confirmation_id, status="confirmed", actor=actor)
        if inspect.isawaitable(result):
            result = await result
        if not result:
            raise ValueError("confirmation is no longer pending")
        return await self.arequire(confirmation_id)

    def reject(self, confirmation_id: str, *, actor: str = "user") -> TranscriptionCandidate | Awaitable[TranscriptionCandidate]:
        if self.store is not None:
            return self.areject(confirmation_id, actor=actor)  # type: ignore[return-value]
        item = self.require(confirmation_id)
        if item.status != ConfirmationStatus.PENDING:
            raise ValueError(f"confirmation is {item.status.value}")
        item = replace(
            item,
            status=ConfirmationStatus.REJECTED,
            resolved_at=datetime.now(timezone.utc),
            resolved_by=actor,
        )
        self._items[confirmation_id] = item
        return item

    async def areject(self, confirmation_id: str, *, actor: str = "user") -> TranscriptionCandidate:
        if self.store is None:
            return self.reject(confirmation_id, actor=actor)
        item = await self.arequire(confirmation_id)
        if item.status != ConfirmationStatus.PENDING:
            raise ValueError(f"confirmation is {item.status.value}")
        method = getattr(self.store, "resolve_transcription", None)
        if method is None:
            raise AttributeError("store must implement resolve_transcription()")
        result = method(confirmation_id, status="rejected", actor=actor)
        if inspect.isawaitable(result):
            result = await result
        if not result:
            raise ValueError("confirmation is no longer pending")
        return await self.arequire(confirmation_id)

    def consume(self, confirmation_id: str, *, task_id: str) -> TranscriptionCandidate | Awaitable[TranscriptionCandidate]:
        if self.store is not None:
            return self.aconsume(confirmation_id, task_id=task_id)  # type: ignore[return-value]
        item = self.require(confirmation_id)
        if item.status == ConfirmationStatus.CONSUMED:
            if item.consumed_by_task_id == task_id:
                return item
            raise ValueError("transcription has already been consumed")
        if item.status != ConfirmationStatus.CONFIRMED:
            raise ValueError("only a confirmed transcription can be consumed")
        item = replace(
            item,
            status=ConfirmationStatus.CONSUMED,
            consumed_by_task_id=task_id,
        )
        self._items[confirmation_id] = item
        return item

    async def aconsume(self, confirmation_id: str, *, task_id: str) -> TranscriptionCandidate:
        if self.store is None:
            return self.consume(confirmation_id, task_id=task_id)
        item = await self.arequire(confirmation_id)
        if item.status == ConfirmationStatus.CONSUMED:
            if item.consumed_by_task_id == task_id:
                return item
            raise ValueError("transcription has already been consumed")
        if item.status != ConfirmationStatus.CONFIRMED:
            raise ValueError("only a confirmed transcription can be consumed")
        method = getattr(self.store, "consume_transcription", None)
        if method is None:
            raise AttributeError("store must implement consume_transcription()")
        result = method(confirmation_id, task_id)
        if inspect.isawaitable(result):
            result = await result
        if not result:
            raise ValueError("transcription has already been consumed")
        return await self.arequire(confirmation_id)

    @staticmethod
    def _from_record(record: Any) -> TranscriptionCandidate:
        def field(name: str, default: Any = None) -> Any:
            return record.get(name, default) if isinstance(record, Mapping) else getattr(record, name, default)

        metadata = field("metadata", {}) or {}
        route = dict(metadata.get("route", {})) if isinstance(metadata, Mapping) else {}
        for name in ("channel", "bot_id", "external_user_id", "session_id"):
            value = field(name, None)
            if value is not None:
                route.setdefault(name, value)
        return TranscriptionCandidate(
            confirmation_id=str(field("confirmation_id", field("id", ""))),
            session_key=str(field("session_key", route.get("session_key", route.get("session_id", ""))) or ""),
            attachment_id=str(field("attachment_id", "") or ""),
            candidate_text=str(field("candidate_text", "") or ""),
            source=str(field("source", "channel") or "channel"),
            confidence=field("confidence", None),
            status=ConfirmationStatus(str(field("status", "pending") or "pending")),
            created_at=_as_datetime(field("created_at")),
            expires_at=_as_datetime(field("expires_at")),
            resolved_at=_as_datetime(field("resolved_at")),
            resolved_by=field("resolved_by", None),
            consumed_by_task_id=field("consumed_by_task_id", None),
            route=route,
        )

    def expire(self, *, now: datetime | None = None) -> int:
        if self.store is not None:
            raise RuntimeError(
                "durable confirmations require the store's expiry operation"
            )
        current = now or datetime.now(timezone.utc)
        changed = 0
        for key, item in list(self._items.items()):
            if item.status == ConfirmationStatus.PENDING and item.expires_at and item.expires_at <= current:
                self._items[key] = replace(item, status=ConfirmationStatus.EXPIRED, resolved_at=current)
                changed += 1
        return changed

    def list_pending(self, session_key: str | None = None) -> list[TranscriptionCandidate]:
        if self.store is not None:
            raise RuntimeError(
                "durable confirmations require the store's candidate listing operation"
            )
        self.expire()
        values = [item for item in self._items.values() if item.status == ConfirmationStatus.PENDING]
        if session_key is not None:
            values = [item for item in values if item.session_key == session_key]
        return sorted(values, key=lambda item: item.created_at)


__all__ = ["AudioConfirmationManager", "ConfirmationStatus", "TranscriptionCandidate"]
