"""Shared, fail-closed parsing for the pinned lark-cli identity contract."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any


_APP_ID = re.compile(r"\Acli_[A-Za-z0-9_-]{4,128}\Z")
_OPEN_ID = re.compile(r"\Aou_[A-Za-z0-9_-]{4,256}\Z")
_ENVELOPE_DISCRIMINATORS = frozenset({"ok", "data"})
_ENVELOPE_KEYS = frozenset({"ok", "identity", "data"})
_FLAT_IDENTITY_KEYS = frozenset({"identity", "verified"})
_PAYLOAD_IDENTITY_KEYS = frozenset(
    {"appId", "app_id", "brand", "verified", "identities"}
)


class LarkBotIdentityContractError(ValueError):
    """The pinned CLI returned an ambiguous or unverified bot identity."""


@dataclass(frozen=True, slots=True)
class VerifiedLarkBotIdentity:
    app_id: str
    brand: str
    open_id: str


def lark_event_bus_socket_app_id(relative_path: str | PurePath) -> str:
    """Return the app ID for the CLI's one legitimate runtime socket path.

    ``lark-cli event consume`` keeps its account-local Unix socket below the
    isolated config directory.  Credential-tree validators share this lexical
    predicate so they can narrowly distinguish that runtime artifact from
    symlinks, FIFOs, devices, and sockets at attacker-chosen locations.  File
    type, ownership, permissions, and link count remain the caller's job.
    """

    parts = PurePath(relative_path).parts
    if (
        len(parts) == 3
        and parts[0] == "events"
        and _APP_ID.fullmatch(parts[1])
        and parts[2] == "bus.sock"
    ):
        return parts[1]
    return ""


def _check_status(value: Mapping[str, Any]) -> None:
    for key in ("code", "errcode"):
        if key not in value:
            continue
        raw = value[key]
        if type(raw) is not int:
            raise LarkBotIdentityContractError("invalid identity status code")
        if raw != 0:
            raise LarkBotIdentityContractError(
                "bot identity verification reported a failure"
            )


def _required_alias(
    value: Mapping[str, Any],
    names: tuple[str, ...],
    *,
    label: str,
) -> str:
    present = [value[name] for name in names if name in value]
    if not present or any(not isinstance(item, str) for item in present):
        raise LarkBotIdentityContractError(f"missing or invalid {label}")
    normalized = [item.strip() for item in present]
    if not normalized[0] or any(item != normalized[0] for item in normalized[1:]):
        raise LarkBotIdentityContractError(f"conflicting {label} aliases")
    return normalized[0]


def _verified_bot_from_auth_status(
    value: Any,
    *,
    require_selected_bot: bool,
) -> VerifiedLarkBotIdentity:
    """Parse the verified nested bot while preserving active-identity facts."""

    if not isinstance(value, Mapping):
        raise LarkBotIdentityContractError("bot identity response is not an object")
    _check_status(value)

    # ``identity`` is not an envelope discriminator: 1.0.92 also uses it in
    # the real flat auth-status response.  ``ok`` and ``data`` unambiguously
    # select the wrapped contract, after which every envelope field is
    # required and payload fields at the outer level are forbidden.
    has_envelope = any(key in value for key in _ENVELOPE_DISCRIMINATORS)
    if has_envelope:
        selected_identity = value.get("identity")
        allowed_identities = {"bot"} if require_selected_bot else {"bot", "user"}
        if value.get("ok") is not True or selected_identity not in allowed_identities:
            raise LarkBotIdentityContractError(
                "bot identity response is not a verified bot success envelope"
            )
        data = value.get("data")
        if not isinstance(data, Mapping):
            raise LarkBotIdentityContractError(
                "bot identity success envelope has invalid data"
            )
        if any(key in value for key in _PAYLOAD_IDENTITY_KEYS):
            raise LarkBotIdentityContractError(
                "bot identity response mixes envelope and payload fields"
            )
        if any(key in data for key in _ENVELOPE_KEYS):
            raise LarkBotIdentityContractError(
                "bot identity response contains a nested envelope"
            )
        if any(key in data for key in _FLAT_IDENTITY_KEYS):
            raise LarkBotIdentityContractError(
                "bot identity response mixes wrapped and flat identity fields"
            )
        payload = data
        _check_status(payload)
    else:
        payload = value
        has_flat_identity = "identity" in payload
        has_flat_verification = "verified" in payload
        selected_identity = payload.get("identity")
        allowed_identities = {"bot"} if require_selected_bot else {"bot", "user"}
        if (
            not has_flat_identity
            or not has_flat_verification
            or selected_identity not in allowed_identities
            or payload.get("verified") is not True
        ):
            raise LarkBotIdentityContractError(
                "bot identity response has conflicting flat identity fields"
            )

    app_id = _required_alias(
        payload,
        ("appId", "app_id"),
        label="app identity",
    )
    brand = _required_alias(payload, ("brand",), label="app brand").lower()
    identities = payload.get("identities")
    if not isinstance(identities, Mapping):
        raise LarkBotIdentityContractError("missing bot identities object")
    bot = identities.get("bot")
    if not isinstance(bot, Mapping):
        raise LarkBotIdentityContractError("missing verified bot identity")
    if bot.get("status") != "ready":
        raise LarkBotIdentityContractError("bot identity is not ready")
    if bot.get("available") is not True or bot.get("verified") is not True:
        raise LarkBotIdentityContractError("bot identity is not verified and available")
    open_id = _required_alias(
        bot,
        ("openId", "open_id"),
        label="bot open_id",
    )

    if not _APP_ID.fullmatch(app_id) or brand not in {"lark", "feishu"}:
        raise LarkBotIdentityContractError("invalid verified app identity")
    if not _OPEN_ID.fullmatch(open_id):
        raise LarkBotIdentityContractError("invalid verified bot open_id")
    return VerifiedLarkBotIdentity(app_id=app_id, brand=brand, open_id=open_id)


def parse_verified_lark_bot_identity(value: Any) -> VerifiedLarkBotIdentity:
    """Parse auth-status only when its currently selected identity is the bot.

    Pinned ``lark-cli`` releases have emitted both a flat object with
    ``identity``/``verified`` at the top level and a standard
    ``{ok, identity, data}`` command envelope.  These are the only accepted
    shapes.  They may never be mixed, because choosing one of two identity
    claims would make conflicts attacker-controlled.

    ``auth status`` may legitimately select a ready user when a profile has
    both user and bot credentials.  Call
    :func:`parse_composite_verified_lark_bot_identity` when an independent
    ``whoami --as bot`` result is available to prove the explicit selection.
    """

    return _verified_bot_from_auth_status(value, require_selected_bot=True)


def _explicit_bot_selection(value: Any) -> tuple[str, str]:
    """Parse the pinned flat ``whoami --as bot`` selection contract."""

    if not isinstance(value, Mapping):
        raise LarkBotIdentityContractError("bot selection response is not an object")
    _check_status(value)
    if any(key in value for key in _ENVELOPE_DISCRIMINATORS):
        raise LarkBotIdentityContractError(
            "bot selection response has an unsupported envelope"
        )
    if (
        value.get("identity") != "bot"
        or value.get("identitySource") != "flag"
        or value.get("available") is not True
        or value.get("tokenStatus") != "ready"
    ):
        raise LarkBotIdentityContractError(
            "bot selection response does not prove an available explicit bot"
        )
    if "verified" in value and value.get("verified") is not True:
        raise LarkBotIdentityContractError(
            "bot selection response reports failed verification"
        )
    profile = value.get("profile")
    if profile is not None and (not isinstance(profile, str) or not profile.strip()):
        raise LarkBotIdentityContractError("bot selection response has invalid profile")
    app_id = _required_alias(
        value,
        ("appId", "app_id"),
        label="selected app identity",
    )
    brand = _required_alias(value, ("brand",), label="selected app brand").lower()
    if not _APP_ID.fullmatch(app_id) or brand not in {"lark", "feishu"}:
        raise LarkBotIdentityContractError("invalid explicitly selected app identity")
    return app_id, brand


def parse_composite_verified_lark_bot_identity(
    auth_status: Any,
    explicit_selection: Any,
) -> VerifiedLarkBotIdentity:
    """Verify a bot using auth status plus an explicit ``whoami --as bot``.

    ``auth status --verify`` is the only pinned diagnostic that exposes the
    verified bot ``open_id``, but its top-level identity follows ``defaultAs``
    and can therefore be ``user``.  ``whoami --as bot`` proves independently
    that the same isolated profile can select a ready bot token.  Both results
    must agree on the exact app and brand; neither claim is trusted alone.
    """

    verified = _verified_bot_from_auth_status(
        auth_status,
        require_selected_bot=False,
    )
    selected_app_id, selected_brand = _explicit_bot_selection(explicit_selection)
    if (selected_app_id, selected_brand) != (verified.app_id, verified.brand):
        raise LarkBotIdentityContractError(
            "explicit bot selection conflicts with verified bot status"
        )
    return verified


__all__ = [
    "LarkBotIdentityContractError",
    "VerifiedLarkBotIdentity",
    "lark_event_bus_socket_app_id",
    "parse_composite_verified_lark_bot_identity",
    "parse_verified_lark_bot_identity",
]
