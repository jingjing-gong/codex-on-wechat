"""Process-local authorization for privileged runtime maintenance.

Maintenance capabilities deliberately live outside durable storage.  A
supervisor may inject one authority into a store and retain the only issuing
reference; callers cannot authorize a mutation by supplying audit strings or
boolean flags.  Grants are opaque bearer capabilities bound to one canonical
mailbox-orphan review and become unusable in a forked child process.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable


_AUTHORIZATION_SCHEMA = "mailbox-orphan-review-authority-v1"
_VALID_ACTIONS = frozenset({"retry", "dead_letter"})


class MaintenanceAuthorizationError(PermissionError):
    """A maintenance grant is absent, foreign, forged, or no longer valid."""


def _required_text(value: object, name: str, *, maximum: int) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    if len(text) > maximum:
        raise ValueError(f"{name} exceeds {maximum} characters")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in text):
        raise ValueError(f"{name} contains a control character")
    return text


def _canonical_time(value: datetime, name: str) -> tuple[datetime, str]:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    normalized = value.astimezone(timezone.utc)
    return normalized, normalized.isoformat(timespec="microseconds")


def canonical_mailbox_review_payload_hash(
    mailbox_message_id: object,
    expected_current_invocation_id: object,
    action: object,
) -> str:
    """Return the schema-v28 durable review identity hash.

    The maintenance row key remains separate.  Keeping this helper narrowly
    scoped preserves the durable v28 meaning while the authority envelope
    below additionally binds all human audit evidence.
    """

    message_id = _required_text(
        mailbox_message_id,
        "mailbox_message_id",
        maximum=4096,
    )
    invocation_id = _required_text(
        expected_current_invocation_id,
        "expected_current_invocation_id",
        maximum=4096,
    )
    action_text = str(getattr(action, "value", action) or "").strip().lower()
    if action_text not in _VALID_ACTIONS:
        raise ValueError("action must be retry or dead_letter")
    canonical = json.dumps(
        {
            "action": action_text,
            "expected_current_invocation_id": invocation_id,
            "mailbox_message_id": message_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_authorization_envelope(
    *,
    mailbox_message_id: object,
    expected_current_invocation_id: object,
    mailbox_maintenance_id: object,
    action: object,
    actor: object,
    reason: object,
    authorization_source: object,
    authorized_at: datetime,
) -> tuple[str, str, str]:
    message_id = _required_text(
        mailbox_message_id,
        "mailbox_message_id",
        maximum=4096,
    )
    invocation_id = _required_text(
        expected_current_invocation_id,
        "expected_current_invocation_id",
        maximum=4096,
    )
    maintenance_id = _required_text(
        mailbox_maintenance_id,
        "mailbox_maintenance_id",
        maximum=128,
    )
    action_text = str(getattr(action, "value", action) or "").strip().lower()
    if action_text not in _VALID_ACTIONS:
        raise ValueError("action must be retry or dead_letter")
    actor_text = _required_text(actor, "actor", maximum=4096)
    reason_text = _required_text(reason, "reason", maximum=4096)
    source_text = _required_text(
        authorization_source,
        "authorization_source",
        maximum=4096,
    )
    _authorized_time, authorized_text = _canonical_time(
        authorized_at,
        "authorized_at",
    )
    payload_hash = canonical_mailbox_review_payload_hash(
        message_id,
        invocation_id,
        action_text,
    )
    canonical = json.dumps(
        {
            "action": action_text,
            "actor": actor_text,
            "authorization_source": source_text,
            "authorized_at": authorized_text,
            "expected_current_invocation_id": invocation_id,
            "mailbox_maintenance_id": maintenance_id,
            "mailbox_message_id": message_id,
            "payload_hash": payload_hash,
            "reason": reason_text,
            "schema": _AUTHORIZATION_SCHEMA,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return canonical, digest, payload_hash


def canonical_mailbox_review_authorization_digest(
    *,
    mailbox_message_id: object,
    expected_current_invocation_id: object,
    mailbox_maintenance_id: object,
    action: object,
    actor: object,
    reason: object,
    authorization_source: object,
    authorized_at: datetime,
) -> str:
    """Return the digest of the complete trusted authorization envelope."""

    _canonical, digest, _payload_hash = _canonical_authorization_envelope(
        mailbox_message_id=mailbox_message_id,
        expected_current_invocation_id=expected_current_invocation_id,
        mailbox_maintenance_id=mailbox_maintenance_id,
        action=action,
        actor=actor,
        reason=reason,
        authorization_source=authorization_source,
        authorized_at=authorized_at,
    )
    return digest


@dataclass(frozen=True, slots=True)
class MailboxMaintenanceGrant:
    """Opaque authorization for one exact mailbox-orphan review."""

    grant_id: str = field(repr=False)
    payload_digest: str
    payload_hash: str
    authorization_source: str
    authorized_at: datetime


@dataclass(frozen=True, slots=True)
class _IssuedGrant:
    grant: MailboxMaintenanceGrant
    canonical_envelope: str


class MailboxMaintenanceAuthority:
    """Issue and verify supervisor-owned, process-local maintenance grants."""

    def __init__(
        self,
        *,
        authorization_source: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.authorization_source = _required_text(
            authorization_source,
            "authorization_source",
            maximum=4096,
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._owner_pid = os.getpid()
        self._grants: dict[str, _IssuedGrant] = {}
        self._lock = threading.RLock()

    def _assert_owner_process(self) -> None:
        if os.getpid() != self._owner_pid:
            raise MaintenanceAuthorizationError(
                "maintenance authority is not valid in this process"
            )

    def _now(self) -> datetime:
        value = self._clock()
        normalized, _text = _canonical_time(value, "maintenance authority clock")
        return normalized

    @staticmethod
    def _assert_grant_shape(grant: MailboxMaintenanceGrant) -> None:
        """Reject constructed/tampered capability objects before lookup.

        Dataclass annotations are not runtime validation.  In particular, an
        unhashable ``grant_id`` would otherwise escape from ``dict.get`` and a
        bytes/non-ASCII digest would make ``hmac.compare_digest`` raise
        ``TypeError``.  Treat every such object exactly like an unknown bearer
        capability instead of exposing an unexpected exception surface.
        """

        grant_id = grant.grant_id
        if (
            type(grant_id) is not str
            or not 1 <= len(grant_id) <= 256
            or any(
                character
                not in "abcdefghijklmnopqrstuvwxyz"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
                for character in grant_id
            )
        ):
            raise MaintenanceAuthorizationError(
                "invalid mailbox maintenance grant"
            )
        for digest in (grant.payload_digest, grant.payload_hash):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise MaintenanceAuthorizationError(
                    "invalid mailbox maintenance grant"
                )
        source = grant.authorization_source
        if type(source) is not str or type(grant.authorized_at) is not datetime:
            raise MaintenanceAuthorizationError(
                "invalid mailbox maintenance grant"
            )
        try:
            normalized_source = _required_text(
                source,
                "authorization_source",
                maximum=4096,
            )
            normalized_time, _time_text = _canonical_time(
                grant.authorized_at,
                "authorized_at",
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise MaintenanceAuthorizationError(
                "invalid mailbox maintenance grant"
            ) from exc
        if normalized_source != source or normalized_time != grant.authorized_at:
            raise MaintenanceAuthorizationError(
                "invalid mailbox maintenance grant"
            )

    def issue_mailbox_orphan_review(
        self,
        mailbox_message_id: object,
        expected_current_invocation_id: object,
        mailbox_maintenance_id: object,
        action: object,
        *,
        actor: object,
        reason: object,
    ) -> MailboxMaintenanceGrant:
        """Issue a bearer grant bound to one complete canonical review."""

        self._assert_owner_process()
        authorized_at = self._now()
        canonical, digest, payload_hash = _canonical_authorization_envelope(
            mailbox_message_id=mailbox_message_id,
            expected_current_invocation_id=expected_current_invocation_id,
            mailbox_maintenance_id=mailbox_maintenance_id,
            action=action,
            actor=actor,
            reason=reason,
            authorization_source=self.authorization_source,
            authorized_at=authorized_at,
        )
        with self._lock:
            while True:
                grant_id = secrets.token_urlsafe(32)
                if grant_id not in self._grants:
                    break
            grant = MailboxMaintenanceGrant(
                grant_id=grant_id,
                payload_digest=digest,
                payload_hash=payload_hash,
                authorization_source=self.authorization_source,
                authorized_at=authorized_at,
            )
            self._grants[grant_id] = _IssuedGrant(
                grant=grant,
                canonical_envelope=canonical,
            )
        return grant

    def validate_mailbox_orphan_review(
        self,
        grant: MailboxMaintenanceGrant | None,
        mailbox_message_id: object,
        expected_current_invocation_id: object,
        mailbox_maintenance_id: object,
        action: object,
        *,
        actor: object,
        reason: object,
    ) -> MailboxMaintenanceGrant:
        """Validate an exact grant without trusting any caller audit field."""

        self._assert_owner_process()
        if not isinstance(grant, MailboxMaintenanceGrant):
            raise MaintenanceAuthorizationError(
                "mailbox orphan review requires a supervisor maintenance grant"
            )
        self._assert_grant_shape(grant)
        with self._lock:
            issued = self._grants.get(grant.grant_id)
            if issued is None:
                raise MaintenanceAuthorizationError(
                    "invalid mailbox maintenance grant"
                )
            trusted = issued.grant
            if (
                not hmac.compare_digest(trusted.grant_id, grant.grant_id)
                or not hmac.compare_digest(
                    trusted.payload_digest,
                    grant.payload_digest,
                )
                or not hmac.compare_digest(trusted.payload_hash, grant.payload_hash)
                or trusted.authorization_source != grant.authorization_source
                or trusted.authorized_at != grant.authorized_at
            ):
                raise MaintenanceAuthorizationError(
                    "invalid mailbox maintenance grant"
                )
            if trusted.authorized_at > self._now():
                raise MaintenanceAuthorizationError(
                    "mailbox maintenance authorization is in the future"
                )
            canonical, digest, payload_hash = _canonical_authorization_envelope(
                mailbox_message_id=mailbox_message_id,
                expected_current_invocation_id=expected_current_invocation_id,
                mailbox_maintenance_id=mailbox_maintenance_id,
                action=action,
                actor=actor,
                reason=reason,
                authorization_source=trusted.authorization_source,
                authorized_at=trusted.authorized_at,
            )
            if (
                not hmac.compare_digest(digest, trusted.payload_digest)
                or not hmac.compare_digest(payload_hash, trusted.payload_hash)
                or not hmac.compare_digest(canonical, issued.canonical_envelope)
            ):
                raise MaintenanceAuthorizationError(
                    "mailbox maintenance grant does not match this review"
                )
            return trusted

    def revoke(self, grant: MailboxMaintenanceGrant) -> None:
        """Revoke one grant without exposing whether it was present."""

        self._assert_owner_process()
        if not isinstance(grant, MailboxMaintenanceGrant):
            return
        try:
            self._assert_grant_shape(grant)
        except MaintenanceAuthorizationError:
            return
        with self._lock:
            self._grants.pop(grant.grant_id, None)

    def clear(self) -> None:
        """Revoke every grant owned by this process-local authority."""

        self._assert_owner_process()
        with self._lock:
            self._grants.clear()


__all__ = [
    "MailboxMaintenanceAuthority",
    "MailboxMaintenanceGrant",
    "MaintenanceAuthorizationError",
    "canonical_mailbox_review_authorization_digest",
    "canonical_mailbox_review_payload_hash",
]
