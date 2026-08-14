"""Stable, unambiguous identifiers derived from compound runtime scope."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterable
from typing import Any


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
