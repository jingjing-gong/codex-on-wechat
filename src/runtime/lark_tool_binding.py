"""Supervisor-side resolution of exact Lark tool identities."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Mapping
from typing import Any

from src.agents.lark_tool_identity import (
    LARK_TOOL_IDENTITY_METADATA_KEY,
    lark_tool_identity_for_profile,
    unavailable_lark_tool_identity,
)

logger = logging.getLogger(__name__)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


async def bind_lark_tool_identity(
    store: Any,
    target: Any,
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Overwrite reserved metadata from the authenticated reply origin.

    The caller-supplied value is always discarded. Owner-authenticated Lark
    tasks either receive their exact enabled bot profile or an explicit
    unavailable snapshot that makes lark-cli fail closed. Other channels and
    non-owner Lark users are explicitly unbound; none can smuggle a reserved
    binding into an Agent child or inherit the operator's global account.
    """

    values = dict(metadata or {})
    values.pop(LARK_TOOL_IDENTITY_METADATA_KEY, None)
    channel = str(_field(target, "channel", "") or "").strip().lower()
    if channel not in {"lark", "feishu"}:
        values[LARK_TOOL_IDENTITY_METADATA_KEY] = (
            unavailable_lark_tool_identity(source="unbound")
        )
        return values
    bot_id = str(_field(target, "bot_id", "") or "").strip()
    external_user_id = str(
        _field(target, "external_user_id", "") or ""
    ).strip()
    snapshot = unavailable_lark_tool_identity(bot_id)
    lookup = getattr(store, "get_bot_profile_for_account", None)
    resolve_account = getattr(store, "resolve_principal_account", None)
    if (
        callable(lookup)
        and callable(resolve_account)
        and bot_id
        and external_user_id
    ):
        try:
            account = resolve_account(
                channel="lark",
                bot_id=bot_id,
                external_user_id=external_user_id,
            )
            if inspect.isawaitable(account):
                account = await account
            owner_mapped = bool(
                account is not None
                and str(_field(account, "principal_id", "")) == "owner"
                and bool(_field(account, "active", False))
                and bool(_field(account, "principal_enabled", False))
                and str(_field(account, "channel", "")) == "lark"
                and str(_field(account, "bot_id", "")) == bot_id
                and str(_field(account, "external_user_id", ""))
                == external_user_id
                and str(_field(account, "identifier_kind", "")) == "open_id"
            )
            if owner_mapped:
                profile = lookup("lark", bot_id, enabled=True)
                if inspect.isawaitable(profile):
                    profile = await profile
                if (
                    profile is not None
                    and str(_field(profile, "channel", "")).strip().lower()
                    == "lark"
                    and str(_field(profile, "bot_id", "")).strip() == bot_id
                ):
                    snapshot = lark_tool_identity_for_profile(profile)
        except Exception:
            # Profile state can change after task acceptance.  An unavailable
            # binding is safe and lets the Agent explain/recover without ever
            # selecting the operator's ambient lark-cli account.
            logger.warning(
                "Lark tool identity resolution failed closed for bot %s",
                bot_id,
                exc_info=True,
            )
            snapshot = unavailable_lark_tool_identity(bot_id)
    values[LARK_TOOL_IDENTITY_METADATA_KEY] = snapshot
    return values


__all__ = ["bind_lark_tool_identity"]
