"""Stable, unambiguous identifiers derived from compound runtime scope."""

from __future__ import annotations

import base64
import inspect
import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ConversationSubjectKind(str, Enum):
    """Bot-local unit used for routing and provider conversation continuity."""

    DIRECT = "direct"
    GROUP = "group"
    THREAD = "thread"


def _required_identity(value: Any, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} is required")
    return normalized


@dataclass(frozen=True, slots=True)
class AuthenticatedActor:
    """Authenticated transport identity supplied by a channel adapter.

    This value deliberately has no ``principal_id`` field.  A channel event can
    identify its authenticated sender, but only administrator-owned durable
    configuration may associate that account with a canonical principal.
    """

    channel: str
    bot_id: str
    external_user_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "channel", _required_identity(self.channel, "channel"))
        object.__setattr__(self, "bot_id", _required_identity(self.bot_id, "bot_id"))
        object.__setattr__(
            self,
            "external_user_id",
            _required_identity(self.external_user_id, "external_user_id"),
        )


@dataclass(frozen=True, slots=True)
class ConversationSubject:
    """Stable bot-local routing subject, kept separate from the acting user."""

    conversation_subject_id: str
    channel: str
    bot_id: str
    kind: ConversationSubjectKind
    scope_key: str
    external_chat_id: str = ""
    external_thread_id: str = ""
    parent_subject_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "conversation_subject_id",
            _required_identity(
                self.conversation_subject_id, "conversation_subject_id"
            ),
        )
        object.__setattr__(self, "channel", _required_identity(self.channel, "channel"))
        object.__setattr__(self, "bot_id", _required_identity(self.bot_id, "bot_id"))
        object.__setattr__(self, "scope_key", _required_identity(self.scope_key, "scope_key"))
        kind = self.kind
        if not isinstance(kind, ConversationSubjectKind):
            kind = ConversationSubjectKind(str(kind))
            object.__setattr__(self, "kind", kind)
        if kind is ConversationSubjectKind.THREAD and not self.external_thread_id:
            raise ValueError("thread conversation subject requires external_thread_id")


@dataclass(frozen=True, slots=True)
class PrincipalResolution:
    """Result of resolving one authenticated actor against owner configuration."""

    actor: AuthenticatedActor
    principal_id: str | None = None
    principal_account_id: str | None = None
    mapping_revision: int | None = None
    source: str = "unmapped"

    @property
    def resolved(self) -> bool:
        return bool(self.principal_id and self.principal_account_id)


def compound_id(namespace: str, components: Iterable[Any]) -> str:
    """Encode a tuple without delimiter ambiguity."""

    kind = str(namespace or "").strip()
    if not kind:
        raise ValueError("identity namespace is required")
    payload = json.dumps(
        [str(component or "") for component in components],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return f"{kind}:v1:{encoded}"


def scoped_id(namespace: str, components: Iterable[Any]) -> str:
    """Use a readable ID when delimiters are absent, otherwise tuple framing."""

    values = tuple(str(component or "") for component in components)
    kind = str(namespace or "").strip()
    if not kind:
        raise ValueError("identity namespace is required")
    if all(":" not in value for value in values):
        return ":".join((kind, *values))
    return compound_id(kind, values)


def conversation_subject_id(
    channel: Any,
    bot_id: Any,
    kind: ConversationSubjectKind | str,
    scope_key: Any,
) -> str:
    """Return an unambiguous durable identity for one bot-local subject."""

    subject_kind = ConversationSubjectKind(str(getattr(kind, "value", kind)))
    return compound_id(
        "conversation-subject",
        (channel, bot_id, subject_kind.value, scope_key),
    )


def direct_conversation_subject(
    channel: Any,
    bot_id: Any,
    external_user_id: Any,
) -> ConversationSubject:
    """Build the compatibility subject used by WeChat and direct Lark chats.

    Its ``scope_key`` is exactly the historical external-user value.  Therefore
    existing routes, preferences, and conversation IDs remain byte-for-byte
    stable while the new subject ID supplies explicit provenance.
    """

    channel_value = _required_identity(channel, "channel")
    bot_value = _required_identity(bot_id, "bot_id")
    user_value = _required_identity(external_user_id, "external_user_id")
    return ConversationSubject(
        conversation_subject_id=conversation_subject_id(
            channel_value,
            bot_value,
            ConversationSubjectKind.DIRECT,
            user_value,
        ),
        channel=channel_value,
        bot_id=bot_value,
        kind=ConversationSubjectKind.DIRECT,
        scope_key=user_value,
    )


def group_conversation_subject(
    channel: Any,
    bot_id: Any,
    external_chat_id: Any,
) -> ConversationSubject:
    """Build a bot-local group-root subject."""

    channel_value = _required_identity(channel, "channel")
    bot_value = _required_identity(bot_id, "bot_id")
    chat_value = _required_identity(external_chat_id, "external_chat_id")
    scope_key = compound_id("conversation-subject-scope", ("group", chat_value))
    return ConversationSubject(
        conversation_subject_id=conversation_subject_id(
            channel_value,
            bot_value,
            ConversationSubjectKind.GROUP,
            scope_key,
        ),
        channel=channel_value,
        bot_id=bot_value,
        kind=ConversationSubjectKind.GROUP,
        scope_key=scope_key,
        external_chat_id=chat_value,
    )


def thread_conversation_subject(
    channel: Any,
    bot_id: Any,
    external_chat_id: Any,
    external_thread_id: Any,
) -> ConversationSubject:
    """Build a bot-local topic/thread subject with an exact group parent."""

    parent = group_conversation_subject(channel, bot_id, external_chat_id)
    thread_value = _required_identity(external_thread_id, "external_thread_id")
    scope_key = compound_id(
        "conversation-subject-scope",
        ("thread", parent.external_chat_id, thread_value),
    )
    return ConversationSubject(
        conversation_subject_id=conversation_subject_id(
            parent.channel,
            parent.bot_id,
            ConversationSubjectKind.THREAD,
            scope_key,
        ),
        channel=parent.channel,
        bot_id=parent.bot_id,
        kind=ConversationSubjectKind.THREAD,
        scope_key=scope_key,
        external_chat_id=parent.external_chat_id,
        external_thread_id=thread_value,
        parent_subject_id=parent.conversation_subject_id,
    )


def conversation_id(
    channel: Any,
    bot_id: Any,
    external_user_id: Any,
    session_id: Any,
    agent_id: Any,
) -> str:
    """Return the canonical ID for one channel/user/session/Agent tuple."""

    values = tuple(
        str(value or "")
        for value in (
            channel,
            bot_id,
            external_user_id,
            session_id or "default",
            agent_id,
        )
    )
    if all(":" not in value for value in values):
        return ":".join(values)
    return compound_id("conversation", values)


def principal_conversation_id(
    principal_id: Any,
    session_id: Any,
    agent_id: Any,
) -> str:
    """Return the provider-history anchor for one canonical principal scope.

    Transport conversations remain channel/bot-local.  This identifier is
    used only after the durable store has proven an authenticated account's
    active principal mapping, and therefore deliberately excludes delivery
    fields such as channel, bot, chat, and thread.
    """

    return compound_id(
        "principal-conversation",
        (
            _required_identity(principal_id, "principal_id"),
            str(session_id or "default"),
            _required_identity(agent_id, "agent_id"),
        ),
    )


def legacy_conversation_id(
    channel: Any,
    bot_id: Any,
    external_user_id: Any,
    session_id: Any,
    agent_id: Any,
) -> str:
    """Return the pre-v1 representation used by persisted installations."""

    return ":".join(
        str(value or "")
        for value in (
            channel,
            bot_id,
            external_user_id,
            session_id or "default",
            agent_id,
        )
    )


def conversation_id_candidates(
    channel: Any,
    bot_id: Any,
    external_user_id: Any,
    session_id: Any,
    agent_id: Any,
) -> tuple[str, ...]:
    """Return canonical then legacy IDs, deduplicated in preference order."""

    values = (
        conversation_id(channel, bot_id, external_user_id, session_id, agent_id),
        legacy_conversation_id(
            channel, bot_id, external_user_id, session_id, agent_id
        ),
    )
    return tuple(dict.fromkeys(values))


def conversation_id_matches(
    value: Any,
    channel: Any,
    bot_id: Any,
    external_user_id: Any,
    session_id: Any,
    agent_id: Any,
) -> bool:
    """Recognize both current and persisted legacy conversation IDs."""

    return str(value or "") in conversation_id_candidates(
        channel, bot_id, external_user_id, session_id, agent_id
    )


def mailbox_conversation_id(destination_agent_id: Any, request_id: Any) -> str:
    """Return a request-scoped fallback when no user route is available."""

    return scoped_id("agent-mailbox", (destination_agent_id, request_id))


class PrincipalResolver:
    """Resolve authenticated transport accounts through the durable store.

    The resolver intentionally accepts only :class:`AuthenticatedActor` or
    three explicit identity strings.  It never consumes event payload mappings,
    metadata, commands, attachments, or a caller-supplied principal ID.
    """

    def __init__(self, store: Any) -> None:
        resolver = getattr(store, "resolve_principal_account", None)
        if not callable(resolver):
            raise TypeError("store must implement resolve_principal_account")
        self._store = store

    async def resolve(
        self,
        actor: AuthenticatedActor | None = None,
        *,
        channel: str | None = None,
        bot_id: str | None = None,
        external_user_id: str | None = None,
    ) -> PrincipalResolution:
        if actor is not None:
            if not isinstance(actor, AuthenticatedActor):
                raise TypeError("actor must be an AuthenticatedActor")
            if any(value is not None for value in (channel, bot_id, external_user_id)):
                raise TypeError("actor and explicit identity fields are mutually exclusive")
        else:
            actor = AuthenticatedActor(
                channel=_required_identity(channel, "channel"),
                bot_id=_required_identity(bot_id, "bot_id"),
                external_user_id=_required_identity(
                    external_user_id, "external_user_id"
                ),
            )

        result = self._store.resolve_principal_account(
            channel=actor.channel,
            bot_id=actor.bot_id,
            external_user_id=actor.external_user_id,
        )
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return PrincipalResolution(actor=actor)

        def value(name: str, default: Any = None) -> Any:
            if isinstance(result, dict):
                return result.get(name, default)
            return getattr(result, name, default)

        # A store lookup is scoped by the authenticated triple.  Verify its
        # returned provenance anyway so a buggy or hostile backend cannot map a
        # caller to a different transport account.
        for name, expected in (
            ("channel", actor.channel),
            ("bot_id", actor.bot_id),
            ("external_user_id", actor.external_user_id),
        ):
            if str(value(name, "") or "") != expected:
                raise RuntimeError(f"principal resolution account mismatch ({name})")
        if not bool(value("active", True)) or not bool(
            value("principal_enabled", True)
        ):
            return PrincipalResolution(actor=actor)
        principal_id = _required_identity(value("principal_id"), "principal_id")
        account_id = _required_identity(
            value("principal_account_id"), "principal_account_id"
        )
        revision = int(value("mapping_revision", 1))
        if revision <= 0:
            raise RuntimeError("principal mapping revision is invalid")
        return PrincipalResolution(
            actor=actor,
            principal_id=principal_id,
            principal_account_id=account_id,
            mapping_revision=revision,
            source="configured",
        )


__all__ = [
    "AuthenticatedActor",
    "ConversationSubject",
    "ConversationSubjectKind",
    "PrincipalResolution",
    "PrincipalResolver",
    "compound_id",
    "conversation_id",
    "conversation_id_candidates",
    "conversation_id_matches",
    "conversation_subject_id",
    "direct_conversation_subject",
    "group_conversation_subject",
    "legacy_conversation_id",
    "mailbox_conversation_id",
    "principal_conversation_id",
    "scoped_id",
    "thread_conversation_subject",
]
