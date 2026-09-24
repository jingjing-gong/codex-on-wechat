"""Trusted per-task Lark CLI identity passed into Agent runtimes.

The channel transport and an Agent's shell are separate process boundaries.
This module defines the narrow, non-secret snapshot that is allowed to cross
the latter boundary so a shell-launched ``lark-cli`` cannot fall back to an
operator's global configuration.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


LARK_TOOL_IDENTITY_METADATA_KEY = "_cow_lark_cli_identity"
LARK_TOOL_IDENTITY_VERSION = 1
LARK_CONFIG_ENVIRONMENT_KEY = "LARKSUITE_CLI_CONFIG_DIR"
LARK_UNAVAILABLE_CONFIG_DIR = "/dev/null"

# These variables can override the selected lark-cli profile or inject an
# identity/token.  They must not survive into a profile-bound Agent shell.
LARK_CREDENTIAL_ENV_KEYS = frozenset(
    {
        "LARKSUITE_CLI_APP_ID",
        "LARKSUITE_CLI_APP_SECRET",
        "LARKSUITE_CLI_BRAND",
        "LARKSUITE_CLI_USER_ACCESS_TOKEN",
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN",
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN_SOURCE",
        "LARKSUITE_CLI_DEFAULT_AS",
        "LARKSUITE_CLI_DATA_DIR",
        "LARKSUITE_CLI_PROFILE",
        "LARKSUITE_CLI_STRICT_MODE",
        "LARKSUITE_CLI_AUTH_PROXY",
        "LARKSUITE_CLI_PROXY_KEY",
    }
)
LARK_ENVIRONMENT_EXCLUDES = tuple(
    sorted(
        {
            *LARK_CREDENTIAL_ENV_KEYS,
            LARK_CONFIG_ENVIRONMENT_KEY,
            "LARK_CHANNEL",
            # Cover future CLI/provider selectors as well as the currently
            # reviewed names above. Codex applies explicit ``set`` values
            # after exclusions, so only the exact managed config directory
            # and notifier flags below are reintroduced.
            "LARK*",
            "FEISHU*",
            "OPENCLAW_*",
            "HERMES_*",
        }
    )
)

_PROFILE_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_APP_ID = re.compile(r"\Acli_[A-Za-z0-9_-]{4,128}\Z")
_CONFIG_IDENTITY = re.compile(r"\Apath-sha256:[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class LarkToolIdentity:
    """Validated Lark shell binding, including an explicit unavailable state."""

    available: bool
    source: str
    bot_id: str
    profile_id: str = ""
    brand: str = ""
    config_dir: str = ""
    config_dir_identity: str = ""

    @property
    def shell_config_dir(self) -> str:
        return self.config_dir if self.available else LARK_UNAVAILABLE_CONFIG_DIR

    @property
    def fingerprint(self) -> str:
        material = "\0".join(
            (
                str(LARK_TOOL_IDENTITY_VERSION),
                "bound" if self.available else "unavailable",
                self.source,
                self.bot_id,
                self.profile_id,
                self.brand,
                self.config_dir_identity,
            )
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()


def config_dir_identity(path: str | Path) -> str:
    """Return the non-secret identity format used by durable bot profiles."""

    canonical = str(Path(path).absolute()).encode("utf-8")
    return "path-sha256:" + hashlib.sha256(canonical).hexdigest()


def lark_shell_environment_set(config_dir: str) -> dict[str, str]:
    """Return overrides that neutralize base/global credential injections."""

    values = {key: "" for key in LARK_CREDENTIAL_ENV_KEYS}
    values[LARK_CONFIG_ENVIRONMENT_KEY] = str(config_dir)
    values["LARKSUITE_CLI_NO_UPDATE_NOTIFIER"] = "1"
    values["LARKSUITE_CLI_NO_SKILLS_NOTIFIER"] = "1"
    return values


def unavailable_lark_tool_identity(
    bot_id: str = "",
    *,
    source: str = "origin_bot",
) -> dict[str, Any]:
    """Return an explicit fail-closed snapshot for an unresolved Lark origin."""

    return {
        "version": LARK_TOOL_IDENTITY_VERSION,
        "state": "unavailable",
        "source": source,
        "bot_id": str(bot_id or ""),
    }


def lark_tool_identity_for_profile(profile: Any) -> dict[str, Any]:
    """Build a strict, non-secret snapshot from one enabled live profile."""

    def field(name: str, default: Any = "") -> Any:
        if isinstance(profile, Mapping):
            return profile.get(name, default)
        return getattr(profile, name, default)

    channel = str(field("channel") or "").strip().lower()
    profile_id = str(field("profile_id") or "").strip()
    bot_id = str(field("bot_id") or "").strip()
    brand = str(field("brand") or "").strip().lower()
    raw_config_dir = str(field("config_dir") or "").strip()
    identity = str(field("config_dir_identity") or "").strip()
    enabled = bool(field("enabled", False))
    removed_at = field("removed_at", None)

    if channel != "lark":
        raise ValueError("Lark tool profile channel conflicts")
    if not enabled or removed_at is not None:
        raise ValueError("Lark tool profile is not enabled")
    if not _PROFILE_ID.fullmatch(profile_id):
        raise ValueError("Lark tool profile identity is invalid")
    if not _APP_ID.fullmatch(bot_id):
        raise ValueError("Lark tool app identity is invalid")
    if brand not in {"lark", "feishu"}:
        raise ValueError("Lark tool brand is invalid")
    config_path = Path(raw_config_dir).expanduser()
    if not config_path.is_absolute():
        raise ValueError("Lark tool config directory must be absolute")
    # Match the durable profile identity contract exactly.  In particular,
    # do not resolve macOS's /tmp -> /private/tmp alias after registration.
    canonical_config_dir = str(config_path.absolute())
    if not _CONFIG_IDENTITY.fullmatch(identity):
        raise ValueError("Lark tool config identity is invalid")
    if config_dir_identity(canonical_config_dir) != identity:
        raise ValueError("Lark tool config directory identity conflicts")

    return {
        "version": LARK_TOOL_IDENTITY_VERSION,
        "state": "bound",
        "source": "origin_bot",
        "profile_id": profile_id,
        "bot_id": bot_id,
        "brand": brand,
        "config_dir": canonical_config_dir,
        "config_dir_identity": identity,
    }


def lark_tool_identity_from_metadata(
    metadata: Mapping[str, Any] | None,
) -> LarkToolIdentity:
    """Validate the reserved supervisor snapshot without accepting fallbacks."""

    if (
        not isinstance(metadata, Mapping)
        or LARK_TOOL_IDENTITY_METADATA_KEY not in metadata
    ):
        # A task without a trusted binding must never inherit the operator's
        # global lark-cli account.  This also protects legacy/compatibility
        # runtime calls that bypass the normal TaskWorker enrichment path.
        return LarkToolIdentity(
            available=False,
            source="unbound",
            bot_id="",
        )
    raw = metadata[LARK_TOOL_IDENTITY_METADATA_KEY]
    if not isinstance(raw, Mapping):
        raise ValueError("Lark tool identity snapshot is malformed")
    if raw.get("version") != LARK_TOOL_IDENTITY_VERSION:
        raise ValueError("Lark tool identity snapshot version is unsupported")
    state = str(raw.get("state") or "")
    source = str(raw.get("source") or "")
    bot_id = str(raw.get("bot_id") or "")
    if source not in {"origin_bot", "unbound"}:
        raise ValueError("Lark tool identity source is invalid")
    if source == "unbound" and bot_id:
        raise ValueError("unbound Lark tool identity contains an app")
    if bot_id and not _APP_ID.fullmatch(bot_id):
        raise ValueError("Lark tool app identity is invalid")
    if state == "unavailable":
        if set(raw) != {"version", "state", "source", "bot_id"}:
            raise ValueError("unavailable Lark tool identity snapshot is malformed")
        return LarkToolIdentity(
            available=False,
            source=source,
            bot_id=bot_id,
        )
    if state != "bound" or set(raw) != {
        "version",
        "state",
        "source",
        "profile_id",
        "bot_id",
        "brand",
        "config_dir",
        "config_dir_identity",
    }:
        raise ValueError("Lark tool identity snapshot is malformed")

    profile_id = str(raw.get("profile_id") or "")
    brand = str(raw.get("brand") or "").lower()
    config_dir = str(raw.get("config_dir") or "")
    identity = str(raw.get("config_dir_identity") or "")
    if not _PROFILE_ID.fullmatch(profile_id):
        raise ValueError("Lark tool profile identity is invalid")
    if not _APP_ID.fullmatch(bot_id):
        raise ValueError("Lark tool app identity is invalid")
    if brand not in {"lark", "feishu"}:
        raise ValueError("Lark tool brand is invalid")
    config_path = Path(config_dir).expanduser()
    if not config_path.is_absolute():
        raise ValueError("Lark tool config directory must be absolute")
    canonical_config_dir = str(config_path.absolute())
    if config_dir != canonical_config_dir:
        raise ValueError("Lark tool config directory is not canonical")
    if not _CONFIG_IDENTITY.fullmatch(identity):
        raise ValueError("Lark tool config identity is invalid")
    if config_dir_identity(config_dir) != identity:
        raise ValueError("Lark tool config directory identity conflicts")
    return LarkToolIdentity(
        available=True,
        source=source,
        bot_id=bot_id,
        profile_id=profile_id,
        brand=brand,
        config_dir=config_dir,
        config_dir_identity=identity,
    )


__all__ = [
    "LARK_CONFIG_ENVIRONMENT_KEY",
    "LARK_CREDENTIAL_ENV_KEYS",
    "LARK_ENVIRONMENT_EXCLUDES",
    "LARK_TOOL_IDENTITY_METADATA_KEY",
    "LARK_TOOL_IDENTITY_VERSION",
    "LARK_UNAVAILABLE_CONFIG_DIR",
    "LarkToolIdentity",
    "config_dir_identity",
    "lark_tool_identity_for_profile",
    "lark_tool_identity_from_metadata",
    "lark_shell_environment_set",
    "unavailable_lark_tool_identity",
]
