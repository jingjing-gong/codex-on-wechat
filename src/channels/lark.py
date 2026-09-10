"""Lark/Feishu channel adapter backed by the version-pinned ``lark-cli``.

The adapter intentionally treats every configured app as a separate channel
account.  Consumer processes, credentials, delivery claims, restart state and
rate-limit backoff are all account-local; the injected TaskManager/registry is
the only shared control plane.

No credential or principal value is accepted from message content.  The app
ID comes from the locally registered profile and the actor comes from Lark's
authenticated ``open_id`` event field.  Optional canonical principals are
resolved only after those two values have been verified.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import math
import mimetypes
import os
import re
import signal
import stat
import tempfile
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from src.lark_contract import (
    LarkBotIdentityContractError,
    lark_event_bus_socket_app_id,
    parse_composite_verified_lark_bot_identity,
)
from src.runtime.identity import (
    compound_id,
    conversation_id,
    direct_conversation_subject,
    group_conversation_subject,
    thread_conversation_subject,
)
from src.runtime.media import AttachmentStore

from .commands import MVPCommandRouter

from .models import (
    ChannelCommand,
    DeliveryReceipt,
    InboundEnvelope,
    LARK_CAPABILITIES,
    LARK_DELIVERY_POLICY,
    ReplyTarget,
    UserDelivery,
    parse_command,
)

logger = logging.getLogger(__name__)

CHANNEL = "lark"
EVENT_KEY = "im.message.receive_v1"
CONFIG_ENVIRONMENT_KEY = "LARKSUITE_CLI_CONFIG_DIR"
# Contract tests should use this constant when constructing their fake CLI.
# Deployments may pin a different reviewed build explicitly, but an arbitrary
# version reported by PATH is never accepted.
PINNED_LARK_CLI_VERSION = "1.0.92"
_PRIVATE_SUBPROCESS_OPTIONS: dict[str, Any] = (
    {"umask": 0o077} if os.name == "posix" else {}
)

_SAFE_PROFILE = re.compile(r"\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\Z")
_APP_ID = re.compile(r"\Acli_[a-zA-Z0-9_-]{4,128}\Z")
_OPEN_ID = re.compile(r"\Aou_[a-zA-Z0-9_-]{4,256}\Z")
_SEMVER = re.compile(r"(?<![0-9])v?(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)")
_SECRET_LINE = re.compile(
    r"(?i)(app[_ -]?secret|access[_ -]?token|refresh[_ -]?token|tenant[_ -]?token)"
    r"\s*[:=]\s*([^\s,;]+)"
)
_JSON_SECRET = re.compile(
    r'''(?i)(["'](?:app[_ -]?secret|access[_ -]?token|refresh[_ -]?token|'''
    r'''tenant[_ -]?token|authorization)["']\s*:\s*)'''
    r'''(?:"[^"]*"|'[^']*'|[^\s,;}\]]+)'''
)
_AUTH_SECRET = re.compile(
    r"(?i)\b(bearer|basic)\s+[^\s,;}\]]+"
)
_SECRET_ARGUMENT = re.compile(
    r"(?i)(--(?:app[-_]?secret|access[-_]?token|refresh[-_]?token|"
    r"tenant[-_]?token|authorization)(?:=|\s+))[^\s,;}\]]+"
)
_URL = re.compile(r"https?://[^\s\]\[()<>]+")
_READY_MARKER = f"[event] ready event_key={EVENT_KEY}"
_MESSAGE_ID = re.compile(r"\Aom_[a-zA-Z0-9_-]{1,256}\Z")
_IMAGE_CONTENT = re.compile(r"\A\[Image:\s*(?P<key>img_[^\]\s]+)\]\Z")
_MEDIA_CONTENT = re.compile(
    r'\A<(?P<kind>file|audio|video)\s+[^>]*?key="(?P<key>[^"]+)"'
    r'(?P<tail>[^>]*)/?>\Z'
)
_MEDIA_NAME = re.compile(r'\bname="(?P<name>[^"]*)"')
_LARK_ONBOARDING_PHRASES = frozenset(
    {
        "新增一个飞书 bot",
        "新增一个飞书bot",
        "新增一个飞书机器人",
    }
)

# The official CLI's environment credential provider takes precedence over a
# configured profile.  A long-running multi-account process must therefore not
# let an operator's interactive shell select another app, identity, or token.
_LARK_CREDENTIAL_ENV_KEYS = frozenset(
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

COMMAND_INTERRUPTED_RESPONSE = (
    "command outcome is unknown after interrupted processing; "
    "inspect the current state before retrying"
)


class LarkError(RuntimeError):
    """Base class for adapter failures safe to classify at the account edge."""


class LarkProtocolError(LarkError):
    """The pinned CLI or event stream violated its tested contract."""


class LarkProfileError(LarkError):
    """A locally configured account profile is unsafe or inconsistent."""


class LarkCliVersionError(LarkProfileError):
    """The executable does not match the configured reviewed version."""


class LarkRateLimitError(LarkError):
    def __init__(self, message: str = "Lark rate limit", *, retry_after: float = 1.0):
        super().__init__(message)
        self.retry_after = max(0.05, float(retry_after))


class LarkPermanentDeliveryError(LarkError):
    """The destination or payload can never succeed without operator action."""


class AccountState(str, Enum):
    STARTING = "starting"
    READY = "ready"
    DISCONNECTED = "disconnected"
    RESTARTING = "restarting"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class LarkBotProfile:
    """Non-secret local registration for one PersonalAgent app."""

    profile_id: str
    app_id: str
    config_dir: Path | str
    brand: str = "lark"
    display_name: str = ""
    bot_open_id: str = ""
    cli_version: str = PINNED_LARK_CLI_VERSION
    enabled: bool = True
    mention_policy: str = "direct_or_mention"
    access_policy: str = "all"
    restart_max_attempts: int = 8
    restart_base_delay: float = 1.0
    restart_max_delay: float = 60.0

    def __post_init__(self) -> None:
        profile_id = str(self.profile_id or "").strip()
        if not _SAFE_PROFILE.fullmatch(profile_id):
            raise LarkProfileError("invalid Lark profile ID")
        app_id = str(self.app_id or "").strip()
        if not _APP_ID.fullmatch(app_id):
            raise LarkProfileError("invalid Lark app ID")
        brand = str(self.brand or "").strip().lower()
        if brand not in {"lark", "feishu"}:
            raise LarkProfileError("Lark brand must be lark or feishu")
        mention_policy = str(self.mention_policy or "").strip().lower()
        if mention_policy not in {
            "direct_or_mention",
            "required",
            "mention_required",
            "all",
            "allow_all",
        }:
            raise LarkProfileError("unsupported Lark mention policy")
        access_policy = str(self.access_policy or "").strip().lower()
        if access_policy not in {
            "all",
            "allow_all",
            "mapped",
            "principals_only",
        }:
            raise LarkProfileError("unsupported Lark access policy")
        open_id = str(self.bot_open_id or "").strip()
        if open_id and not _OPEN_ID.fullmatch(open_id):
            raise LarkProfileError("invalid bot open_id")
        config_dir = Path(self.config_dir).expanduser()
        if not config_dir.is_absolute():
            raise LarkProfileError("Lark CLI config directory must be absolute")
        if int(self.restart_max_attempts) < 0:
            raise LarkProfileError("restart_max_attempts cannot be negative")
        for name, raw in (
            ("restart_base_delay", self.restart_base_delay),
            ("restart_max_delay", self.restart_max_delay),
        ):
            value = float(raw)
            if not math.isfinite(value) or value <= 0:
                raise LarkProfileError(f"{name} must be positive and finite")
        if float(self.restart_max_delay) < float(self.restart_base_delay):
            raise LarkProfileError("restart_max_delay is below restart_base_delay")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "app_id", app_id)
        object.__setattr__(self, "brand", brand)
        object.__setattr__(self, "mention_policy", mention_policy)
        object.__setattr__(self, "access_policy", access_policy)
        object.__setattr__(self, "bot_open_id", open_id)
        object.__setattr__(self, "config_dir", config_dir.resolve(strict=False))
        object.__setattr__(self, "cli_version", str(self.cli_version or "").lstrip("v"))

    @classmethod
    def from_value(cls, value: Any) -> "LarkBotProfile":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data = dict(value)
        else:
            data = {
                name: getattr(value, name)
                for name in cls.__dataclass_fields__
                if hasattr(value, name)
            }
            # Durable BotProfileRecord values use transport-neutral field
            # names.  Preserve the same aliases accepted for Mapping inputs so
            # a freshly committed live-onboarding record can be activated
            # without first round-tripping through an untyped dict.
            for alias in (
                "bot_id",
                "name",
                "restart_policy",
                "restart_policy_json",
            ):
                if alias not in data and hasattr(value, alias):
                    data[alias] = getattr(value, alias)
        if "app_id" not in data:
            data["app_id"] = data.pop("bot_id", "")
        if "profile_id" not in data:
            data["profile_id"] = data.pop("name", "")
        restart = data.pop("restart_policy", data.pop("restart_policy_json", None))
        if isinstance(restart, str):
            with contextlib.suppress(json.JSONDecodeError):
                restart = json.loads(restart)
        if isinstance(restart, Mapping):
            data.setdefault("restart_max_attempts", restart.get("max_attempts", 8))
            data.setdefault("restart_base_delay", restart.get("base_delay", 1.0))
            data.setdefault("restart_max_delay", restart.get("max_delay", 60.0))
            data.setdefault("bot_open_id", restart.get("bot_open_id", ""))
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{key: data[key] for key in allowed if key in data})

    def environment(self, base: Mapping[str, str] | None = None) -> dict[str, str]:
        environment = dict(os.environ if base is None else base)
        for key in _LARK_CREDENTIAL_ENV_KEYS:
            environment.pop(key, None)
        # Host-agent workspace integrations can redirect lark-cli discovery
        # or inject a different channel identity.  Every account subprocess
        # receives only its verified profile directory.
        for key in tuple(environment):
            if key.startswith("OPENCLAW_") or key.startswith("HERMES_"):
                environment.pop(key, None)
        environment.pop("LARK_CHANNEL", None)
        environment[CONFIG_ENVIRONMENT_KEY] = str(self.config_dir)
        environment["LARKSUITE_CLI_NO_UPDATE_NOTIFIER"] = "1"
        environment["LARKSUITE_CLI_NO_SKILLS_NOTIFIER"] = "1"
        return environment


def validate_private_config_directory(
    path: str | os.PathLike[str],
    *,
    expected_app_id: str | None = None,
) -> tuple[int, int]:
    """Validate an existing credential tree and its exact CLI bus socket.

    The event bus is a reviewed ``lark-cli`` runtime artifact, not credential
    state.  It may survive a consumer generation or supervisor restart.  Only
    the private Unix socket for the registered app is admitted; callers that
    do not provide ``expected_app_id`` retain the older special-file-strict
    behavior.
    """

    candidate = Path(path).expanduser()
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise LarkProfileError("Lark CLI config directory is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise LarkProfileError("Lark CLI config path must be a real directory")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise LarkProfileError("Lark CLI config directory has a different owner")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise LarkProfileError("Lark CLI config directory must be owner-only")

    def walk_error(error: OSError) -> None:
        raise error

    try:
        for root, directories, files in os.walk(
            candidate,
            topdown=True,
            onerror=walk_error,
            followlinks=False,
        ):
            root_path = Path(root)
            for name in (*directories, *files):
                entry = root_path / name
                details = entry.lstat()
                try:
                    relative_entry = entry.relative_to(candidate)
                except ValueError as exc:
                    raise LarkProfileError(
                        "Lark CLI config state escaped its profile directory"
                    ) from exc
                socket_app_id = lark_event_bus_socket_app_id(relative_entry)
                expected_event_socket = bool(
                    expected_app_id
                    and socket_app_id
                    and socket_app_id == expected_app_id
                )
                if stat.S_ISLNK(details.st_mode):
                    raise LarkProfileError(
                        "Lark CLI config state must not contain symlinks"
                    )
                if hasattr(os, "getuid") and details.st_uid != os.getuid():
                    raise LarkProfileError(
                        "Lark CLI config state has a different owner"
                    )
                if stat.S_IMODE(details.st_mode) & 0o077:
                    raise LarkProfileError(
                        "Lark CLI config state must be owner-only"
                    )
                if expected_event_socket:
                    # Reserve the exact path as a socket: a directory, FIFO,
                    # device, or regular-file impostor must not be accepted.
                    if (
                        name in directories
                        or not stat.S_ISSOCK(details.st_mode)
                        or details.st_nlink != 1
                    ):
                        raise LarkProfileError(
                            "Lark CLI event bus socket has an invalid type"
                        )
                    continue
                if name in directories and not stat.S_ISDIR(details.st_mode):
                    raise LarkProfileError(
                        "Lark CLI config state contains an invalid directory"
                    )
                if name in files and not stat.S_ISREG(details.st_mode):
                    raise LarkProfileError(
                        "Lark CLI config state contains a special file"
                    )
                if name in files and details.st_nlink != 1:
                    raise LarkProfileError(
                        "Lark CLI config state contains a hardlinked file"
                    )
    except LarkProfileError:
        raise
    except OSError as exc:
        raise LarkProfileError("Lark CLI config state is unavailable") from exc
    return int(info.st_dev), int(info.st_ino)


def tighten_lark_cli_config_state(
    path: str | os.PathLike[str],
    *,
    expected_app_id: str,
    expected_identity: tuple[int, int] | None = None,
) -> None:
    """Make owner-created one-shot CLI artifacts private without following links.

    The pinned CLI writes some cache files with an explicit ``0644`` mode,
    overriding the private subprocess umask. Callers use this only at a
    quiescent verification boundary after a strict preflight of the existing
    tree. Validate every entry before changing any mode, then operate on the
    exact inspected inode through ``O_NOFOLLOW`` file descriptors.
    """

    candidate = Path(path).expanduser()
    try:
        root = candidate.lstat()
    except OSError as exc:
        raise LarkProfileError("Lark CLI config directory is unavailable") from exc
    if stat.S_ISLNK(root.st_mode) or not stat.S_ISDIR(root.st_mode):
        raise LarkProfileError("Lark CLI config path must be a real directory")
    if hasattr(os, "getuid") and root.st_uid != os.getuid():
        raise LarkProfileError("Lark CLI config directory has a different owner")
    if stat.S_IMODE(root.st_mode) & 0o077:
        raise LarkProfileError("Lark CLI config directory must be owner-only")
    root_identity = (int(root.st_dev), int(root.st_ino))
    if expected_identity is not None and root_identity != expected_identity:
        raise LarkProfileError(
            "Lark CLI config directory changed during verification"
        )

    directories_to_tighten: list[tuple[Path, os.stat_result]] = []
    files_to_tighten: list[tuple[Path, os.stat_result]] = []

    def walk_error(error: OSError) -> None:
        raise error

    try:
        for walked_root, directories, files in os.walk(
            candidate,
            topdown=True,
            onerror=walk_error,
            followlinks=False,
        ):
            root_path = Path(walked_root)
            for name in (*directories, *files):
                entry = root_path / name
                details = entry.lstat()
                try:
                    relative_entry = entry.relative_to(candidate)
                except ValueError as exc:
                    raise LarkProfileError(
                        "Lark CLI config state escaped its profile directory"
                    ) from exc
                socket_app_id = lark_event_bus_socket_app_id(relative_entry)
                expected_event_socket = bool(
                    socket_app_id and socket_app_id == expected_app_id
                )
                if stat.S_ISLNK(details.st_mode):
                    raise LarkProfileError(
                        "Lark CLI config state must not contain symlinks"
                    )
                if hasattr(os, "getuid") and details.st_uid != os.getuid():
                    raise LarkProfileError(
                        "Lark CLI config state has a different owner"
                    )
                if expected_event_socket:
                    if (
                        name in directories
                        or not stat.S_ISSOCK(details.st_mode)
                        or details.st_nlink != 1
                    ):
                        raise LarkProfileError(
                            "Lark CLI event bus socket has an invalid type"
                        )
                    if stat.S_IMODE(details.st_mode) & 0o077:
                        raise LarkProfileError(
                            "Lark CLI config state must be owner-only"
                        )
                    continue
                if name in directories:
                    if not stat.S_ISDIR(details.st_mode):
                        raise LarkProfileError(
                            "Lark CLI config state contains an invalid directory"
                        )
                    if stat.S_IMODE(details.st_mode) & 0o077:
                        directories_to_tighten.append((entry, details))
                    continue
                if not stat.S_ISREG(details.st_mode):
                    raise LarkProfileError(
                        "Lark CLI config state contains a special file"
                    )
                if details.st_nlink != 1:
                    raise LarkProfileError(
                        "Lark CLI config state contains a hardlinked file"
                    )
                if stat.S_IMODE(details.st_mode) & 0o077:
                    files_to_tighten.append((entry, details))

        # Inspect the complete tree before changing anything. An unsafe entry
        # therefore cannot leave a partially normalized credential tree.
        for directory, details in directories_to_tighten:
            _chmod_exact_lark_config_entry(
                directory,
                details,
                stat.S_IMODE(details.st_mode) & 0o700,
            )
        for file_path, details in files_to_tighten:
            _chmod_exact_lark_config_entry(
                file_path,
                details,
                stat.S_IMODE(details.st_mode) & 0o700,
            )
    except LarkProfileError:
        raise
    except OSError as exc:
        raise LarkProfileError("could not secure Lark CLI config state") from exc


def _chmod_exact_lark_config_entry(
    path: Path,
    expected: os.stat_result,
    mode: int,
) -> None:
    """Apply one private mode to the exact previously inspected inode."""

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if os.name != "posix" or not nofollow or not hasattr(os, "fchmod"):
        raise LarkProfileError(
            "this platform cannot safely secure Lark CLI config state"
        )

    flags = os.O_RDONLY | nofollow
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    if stat.S_ISDIR(expected.st_mode):
        flags |= getattr(os, "O_DIRECTORY", 0)

    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        expected_kind = stat.S_IFMT(expected.st_mode)
        if (
            opened.st_dev != expected.st_dev
            or opened.st_ino != expected.st_ino
            or stat.S_IFMT(opened.st_mode) != expected_kind
            or (hasattr(os, "getuid") and opened.st_uid != os.getuid())
            or (stat.S_ISREG(opened.st_mode) and opened.st_nlink != 1)
        ):
            raise LarkProfileError(
                "Lark CLI config entry changed during permission tightening"
            )
        os.fchmod(descriptor, mode)
        secured = os.fstat(descriptor)
        if (
            secured.st_dev != opened.st_dev
            or secured.st_ino != opened.st_ino
            or stat.S_IFMT(secured.st_mode) != expected_kind
            or stat.S_IMODE(secured.st_mode) != mode
            or (stat.S_ISREG(secured.st_mode) and secured.st_nlink != 1)
        ):
            raise LarkProfileError(
                "Lark CLI config entry changed during permission tightening"
            )
    finally:
        os.close(descriptor)


def redact_lark_text(value: Any) -> str:
    """Remove known credential forms from diagnostics and onboarding errors."""

    text = str(value or "")
    text = _JSON_SECRET.sub(lambda match: f'{match.group(1)}"<redacted>"', text)
    text = _AUTH_SECRET.sub(lambda match: f"{match.group(1)} <redacted>", text)
    text = _SECRET_ARGUMENT.sub(lambda match: f"{match.group(1)}<redacted>", text)
    text = _SECRET_LINE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    return _URL.sub("<redacted-url>", text)


def parse_lark_cli_version(output: Any) -> str:
    match = _SEMVER.search(str(output or ""))
    if match is None:
        raise LarkCliVersionError("could not parse lark-cli version")
    return match.group(1)


class _DuplicateLarkJsonKey(ValueError):
    pass


def _unique_lark_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateLarkJsonKey(key)
        value[key] = item
    return value


def _last_json_object(value: Any) -> Mapping[str, Any] | None:
    """Return the last JSON object from CLI output with diagnostic prefixes."""

    text = str(value or "").strip()
    if not text:
        return None
    candidates = (text, *reversed(text.splitlines()))
    for candidate in candidates:
        try:
            decoded = json.loads(candidate.strip())
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, Mapping):
            return dict(decoded)
    return None


def _strict_json_object(value: Any) -> Mapping[str, Any]:
    """Decode exactly one complete object with no duplicate JSON keys."""

    text = str(value or "").strip()
    if not text:
        raise LarkProtocolError("lark-cli returned an empty JSON response")
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_lark_json_object,
        )
    except _DuplicateLarkJsonKey as exc:
        raise LarkProtocolError(
            "lark-cli returned JSON with duplicate keys"
        ) from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise LarkProtocolError(
            "lark-cli did not return exactly one JSON object"
        ) from exc
    if not isinstance(decoded, Mapping):
        raise LarkProtocolError("lark-cli JSON response is not an object")
    return dict(decoded)


def _error_detail(*values: Mapping[str, Any] | None) -> Mapping[str, Any]:
    for value in values:
        if not isinstance(value, Mapping):
            continue
        nested = value.get("error")
        if isinstance(nested, Mapping):
            return nested
        if any(
            key in value
            for key in ("type", "subtype", "code", "message", "retryable")
        ):
            return value
    return {}


def _positive_delay(value: Any, default: float = 1.0) -> float:
    try:
        delay = float(value)
    except (TypeError, ValueError):
        return default
    return delay if math.isfinite(delay) and delay > 0 else default


def _validate_lark_reply_target(
    target: ReplyTarget,
    *,
    bot_id: str,
    external_user_id: str | None = None,
) -> None:
    """Fail closed when a durable target disagrees with its account/destination."""

    if str(target.channel or "").strip().lower() != CHANNEL:
        raise LarkPermanentDeliveryError("Lark reply target has the wrong channel")
    if str(target.bot_id or "").strip() != str(bot_id or "").strip():
        raise LarkPermanentDeliveryError("Lark reply target has the wrong bot account")
    actor_id = str(target.external_user_id or "").strip()
    if not _OPEN_ID.fullmatch(actor_id):
        raise LarkPermanentDeliveryError("Lark reply target has an invalid actor")
    durable_actor = str(external_user_id or "").strip()
    if durable_actor and actor_id != durable_actor:
        raise LarkPermanentDeliveryError(
            "Lark reply target actor conflicts with its durable row"
        )

    destination_kind = str(target.destination_kind or "").strip().lower()
    destination_id = str(target.destination_id or "").strip()
    if destination_kind not in {"open_id", "group", "chat", "thread"}:
        raise LarkPermanentDeliveryError("Lark reply destination kind is invalid")
    if not destination_id:
        raise LarkPermanentDeliveryError("Lark reply destination is missing")
    if destination_kind == "open_id":
        if destination_id != actor_id or not _OPEN_ID.fullmatch(destination_id):
            raise LarkPermanentDeliveryError(
                "Lark direct reply destination does not match its actor"
            )
        if target.thread_id:
            raise LarkPermanentDeliveryError(
                "Lark direct reply cannot select a topic thread"
            )
    else:
        metadata = (
            target.transport_metadata
            if isinstance(target.transport_metadata, Mapping)
            else {}
        )
        stored_chat_id = str(metadata.get("chat_id") or "").strip()
        if stored_chat_id and stored_chat_id != destination_id:
            raise LarkPermanentDeliveryError(
                "Lark reply destination conflicts with its source chat"
            )
        stored_thread_id = str(metadata.get("thread_id") or "").strip()
        if stored_thread_id and stored_thread_id != str(target.thread_id or "").strip():
            raise LarkPermanentDeliveryError(
                "Lark reply thread conflicts with its source metadata"
            )
        stored_root_id = str(metadata.get("root_id") or "").strip()
        if stored_root_id and stored_root_id != str(
            target.root_message_id or ""
        ).strip():
            raise LarkPermanentDeliveryError(
                "Lark reply root conflicts with its source metadata"
            )
    if destination_kind == "thread":
        if not str(target.thread_id or "").strip() or not str(
            target.source_message_id or ""
        ).strip():
            raise LarkPermanentDeliveryError(
                "Lark thread delivery requires a thread and source message"
            )
    elif target.thread_id:
        raise LarkPermanentDeliveryError(
            "Lark topic metadata requires a thread destination"
        )


def _response_message_id(value: Any) -> str:
    data = _field(value, "data", default={})
    message_id = str(_field(value, "message_id", default="") or "").strip()
    if isinstance(data, Mapping):
        message_id = str(data.get("message_id") or message_id).strip()
    return message_id if _MESSAGE_ID.fullmatch(message_id) else ""


def _signal_process_group(
    process: asyncio.subprocess.Process, signal_value: signal.Signals
) -> None:
    """Fence a CLI generation and every descendant in its private session."""

    pid = int(getattr(process, "pid", 0) or 0)
    if os.name == "posix" and pid > 0:
        try:
            # Every subprocess below is started as a session leader, so its PID
            # is also the process-group ID.  Avoid getpgid() after parent exit,
            # when only a descendant may remain in the group.
            os.killpg(pid, signal_value)
            return
        except ProcessLookupError:
            return
        except OSError:
            # Narrow non-POSIX/embedded runtimes may reject killpg despite
            # reporting os.name=posix.  Fall back to the direct child below.
            pass
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        if signal_value == signal.SIGKILL:
            process.kill()
        else:
            process.terminate()


@dataclass(frozen=True, slots=True)
class LarkProcessEvent:
    generation: int
    payload: Mapping[str, Any]


class LarkCliProcess:
    """Generation-fenced NDJSON consumer and one-shot CLI command boundary."""

    def __init__(
        self,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        *,
        executable: str = "lark-cli",
        expected_version: str | None = None,
        ready_timeout: float = 30.0,
        stop_timeout: float = 5.0,
        event_queue_size: int = 256,
        verification_timeout: float = 30.0,
    ) -> None:
        self.profile = LarkBotProfile.from_value(profile)
        self.executable = str(executable or "lark-cli")
        self.expected_version = str(
            expected_version or self.profile.cli_version or PINNED_LARK_CLI_VERSION
        ).lstrip("v")
        self.ready_timeout = max(0.1, float(ready_timeout))
        self.stop_timeout = max(0.1, float(stop_timeout))
        self.event_queue_size = max(1, int(event_queue_size))
        self.verification_timeout = max(1.0, float(verification_timeout))
        self.generation = 0
        self._process: asyncio.subprocess.Process | None = None
        self._pump_task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._events: asyncio.Queue[LarkProcessEvent | BaseException | None] = (
            asyncio.Queue(maxsize=self.event_queue_size)
        )
        self._stderr_tail: list[str] = []

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    async def verify_version(self) -> str:
        process = await asyncio.create_subprocess_exec(
            self.executable,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self.profile.environment(),
            start_new_session=True,
            **_PRIVATE_SUBPROCESS_OPTIONS,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(), timeout=self.verification_timeout
            )
        except asyncio.TimeoutError as exc:
            _signal_process_group(process, signal.SIGKILL)
            await process.wait()
            raise LarkCliVersionError("lark-cli version check timed out") from exc
        except asyncio.CancelledError:
            _signal_process_group(process, signal.SIGKILL)
            await process.wait()
            raise
        finally:
            if process.returncode is not None:
                _signal_process_group(process, signal.SIGKILL)
        rendered = (output or b"").decode("utf-8", errors="replace")
        version = parse_lark_cli_version(rendered)
        if process.returncode != 0:
            raise LarkCliVersionError("lark-cli version check failed")
        if version != self.expected_version:
            raise LarkCliVersionError(
                f"unsupported lark-cli version {version}; expected {self.expected_version}"
            )
        return version

    @staticmethod
    def _business_data(value: Mapping[str, Any]) -> Mapping[str, Any]:
        data = value.get("data")
        return data if isinstance(data, Mapping) else value

    async def verify_profile(self) -> Mapping[str, Any]:
        """Verify the isolated config and exact bot identity before ingress."""

        config = await self.run_json("config", "show")
        config_data = self._business_data(config)
        app_id = str(config_data.get("appId", config_data.get("app_id", "")) or "")
        brand = str(config_data.get("brand") or "").strip().lower()
        if (app_id, brand) != (self.profile.app_id, self.profile.brand):
            raise LarkProfileError(
                "isolated lark-cli profile does not match the registered app"
            )

        try:
            identity_status = await self.run_json(
                "auth",
                "status",
                "--verify",
                "--json",
                strict_single_object=True,
            )
            explicit_bot = await self.run_json(
                "whoami",
                "--as",
                "bot",
                strict_single_object=True,
            )
            verified = parse_composite_verified_lark_bot_identity(
                identity_status,
                explicit_bot,
            )
        except (LarkBotIdentityContractError, LarkProtocolError) as exc:
            raise LarkProfileError(
                "lark-cli did not verify a stable bot identity"
            ) from exc
        if (verified.app_id, verified.brand) != (
            self.profile.app_id,
            self.profile.brand,
        ):
            raise LarkProfileError("verified Lark bot identity belongs to another app")
        if (
            self.profile.bot_open_id
            and verified.open_id != self.profile.bot_open_id
        ):
            raise LarkProfileError("verified Lark bot open_id changed unexpectedly")
        return {
            "app_id": verified.app_id,
            "brand": verified.brand,
            "open_id": verified.open_id,
        }

    async def start(self) -> int:
        if self.running:
            return self.generation
        config_identity = validate_private_config_directory(
            self.profile.config_dir,
            expected_app_id=self.profile.app_id,
        )
        try:
            await self.verify_version()
            await self.verify_profile()
        finally:
            # lark-cli 1.0.92 explicitly creates remote_meta.meta.json as
            # 0644, despite the child umask. Tighten only inspected,
            # owner-created ordinary artifacts and then revalidate the exact
            # root before a long-lived consumer can inherit this profile.
            tighten_lark_cli_config_state(
                self.profile.config_dir,
                expected_app_id=self.profile.app_id,
                expected_identity=config_identity,
            )
            verified_identity = validate_private_config_directory(
                self.profile.config_dir,
                expected_app_id=self.profile.app_id,
            )
            if verified_identity != config_identity:
                raise LarkProfileError(
                    "Lark CLI config directory changed during verification"
                )
        self.generation += 1
        generation = self.generation
        self._ready = asyncio.Event()
        self._events = asyncio.Queue(maxsize=self.event_queue_size)
        self._process = await asyncio.create_subprocess_exec(
            self.executable,
            "event",
            "consume",
            EVENT_KEY,
            "--as",
            "bot",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.profile.environment(),
            start_new_session=True,
            **_PRIVATE_SUBPROCESS_OPTIONS,
        )
        self._pump_task = asyncio.create_task(
            self._pump(generation),
            name=f"lark-cli:{self.profile.profile_id}:{generation}",
        )
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self.ready_timeout)
        except asyncio.TimeoutError as exc:
            await self.stop()
            raise LarkProtocolError("lark-cli consumer did not become ready") from exc
        except asyncio.CancelledError:
            await self.stop()
            raise
        if not self.running:
            error = self._stderr_tail[-1] if self._stderr_tail else "consumer exited"
            raise LarkProtocolError(redact_lark_text(error))
        return generation

    @staticmethod
    def _is_ready(_value: Mapping[str, Any] | None, line: str) -> bool:
        # Readiness is a pinned stderr wire contract, not a heuristic.  Inbound
        # output or generic words such as "connected" must never make profile
        # verification/startup appear successful for a different CLI build.
        return line.strip() == _READY_MARKER

    async def _pump(self, generation: int) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return

        async def drain_stderr() -> None:
            if process.stderr is None:
                return
            while True:
                raw = await process.stderr.readline()
                if not raw:
                    return
                line = redact_lark_text(raw.decode("utf-8", errors="replace").strip())
                if line:
                    self._stderr_tail.append(line[:500])
                    del self._stderr_tail[:-20]
                    if self._is_ready(None, line):
                        self._ready.set()

        stderr_task = asyncio.create_task(drain_stderr())
        protocol_failure = False
        try:
            while True:
                raw = await process.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="strict").strip()
                if not line:
                    continue
                value: Mapping[str, Any] | None = None
                try:
                    decoded = json.loads(line)
                    if isinstance(decoded, Mapping):
                        value = dict(decoded)
                except (UnicodeError, json.JSONDecodeError):
                    await self._events.put(
                        LarkProtocolError("malformed lark-cli NDJSON output")
                    )
                    protocol_failure = True
                    # A protocol-violating child may keep stdin open forever.
                    # Terminate it here so the supervisor receives the fenced
                    # error/end-of-generation instead of hanging in wait().
                    if process.returncode is None:
                        _signal_process_group(process, signal.SIGTERM)
                    break
                if value is not None and not self._is_control_record(value):
                    await self._events.put(LarkProcessEvent(generation, value))
            try:
                return_code = await asyncio.wait_for(
                    process.wait(), timeout=self.stop_timeout
                )
            except asyncio.TimeoutError:
                _signal_process_group(process, signal.SIGKILL)
                return_code = await process.wait()
            if generation == self.generation:
                self._ready.set()
                if return_code != 0 and not protocol_failure:
                    message = (
                        self._stderr_tail[-1]
                        if self._stderr_tail
                        else f"lark-cli consumer exited with status {return_code}"
                    )
                    await self._events.put(LarkProtocolError(message))
                await self._events.put(None)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if generation == self.generation:
                self._ready.set()
                await self._events.put(exc)
                await self._events.put(None)
        finally:
            stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)

    @staticmethod
    def _is_control_record(value: Mapping[str, Any]) -> bool:
        marker = str(value.get("type", value.get("status", "")) or "").lower()
        return marker in {"ready", "connected", "consumer_ready", "started"}

    async def events(self) -> AsyncIterator[LarkProcessEvent]:
        while True:
            value = await self._events.get()
            if value is None:
                return
            if isinstance(value, BaseException):
                raise value
            if value.generation != self.generation:
                # Generation fencing is checked both here and by the account
                # supervisor so stale pipe output can never reach ingress.
                continue
            yield value

    async def run_json(
        self,
        *arguments: str,
        input_value: Mapping[str, Any] | None = None,
        timeout: float = 30.0,
        cwd: str | os.PathLike[str] | None = None,
        strict_single_object: bool = False,
    ) -> Mapping[str, Any]:
        process = await asyncio.create_subprocess_exec(
            self.executable,
            *arguments,
            stdin=(asyncio.subprocess.PIPE if input_value is not None else None),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.profile.environment(),
            cwd=(str(cwd) if cwd is not None else None),
            start_new_session=True,
            **_PRIVATE_SUBPROCESS_OPTIONS,
        )
        payload = (
            json.dumps(input_value, ensure_ascii=False, separators=(",", ":")).encode()
            if input_value is not None
            else None
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(payload), timeout=max(0.1, float(timeout))
            )
        except asyncio.TimeoutError as exc:
            _signal_process_group(process, signal.SIGKILL)
            await process.wait()
            raise LarkError("lark-cli operation timed out") from exc
        except asyncio.CancelledError:
            _signal_process_group(process, signal.SIGKILL)
            await process.wait()
            raise
        finally:
            if process.returncode is not None:
                _signal_process_group(process, signal.SIGKILL)
        output = (stdout or b"").decode("utf-8", errors="replace").strip()
        raw_error = (stderr or b"").decode("utf-8", errors="replace").strip()
        if strict_single_object:
            stdout_value = _strict_json_object(output)
        else:
            stdout_value = _last_json_object(output)
        stderr_value = _last_json_object(raw_error)
        value = stdout_value or {}
        detail = _error_detail(
            value if value.get("ok") is False else None,
            stderr_value,
        )
        code = detail.get(
            "code",
            value.get("code", value.get("errcode", 0)),
        )
        try:
            numeric_code = int(code or 0)
        except (TypeError, ValueError):
            numeric_code = -1
        failed = (
            process.returncode != 0
            or value.get("ok") is False
            or numeric_code != 0
        )
        if failed:
            error_type = str(detail.get("type") or "").strip().lower()
            subtype = str(detail.get("subtype") or "").strip().lower()
            retry_after = detail.get(
                "retry_after_seconds",
                detail.get(
                    "retry_after",
                    value.get("retry_after", value.get("retryAfter")),
                ),
            )
            if (
                numeric_code in {429, 99991400}
                or "rate_limit" in {error_type, subtype}
                or "rate-limit" in {error_type, subtype}
            ):
                raise LarkRateLimitError(
                    "Lark rate limit",
                    retry_after=_positive_delay(retry_after),
                )
            message = redact_lark_text(
                detail.get("message")
                or detail.get("hint")
                or f"lark-cli operation failed ({error_type or 'unknown'}/{subtype or numeric_code})"
            )
            if error_type in {"validation", "permission", "authentication"} or subtype in {
                "invalid_argument",
                "invalid_client",
                "missing_scope",
                "not_found",
                "permission_denied",
            }:
                raise LarkPermanentDeliveryError(message)
            raise LarkError(message)
        if stdout_value is None:
            if output:
                raise LarkProtocolError("lark-cli returned malformed JSON")
            raise LarkProtocolError("lark-cli returned no JSON response")
        if "ok" in value and value.get("ok") is not True:
            raise LarkProtocolError("lark-cli returned an invalid success envelope")
        return dict(value)

    async def send_text(
        self,
        target: ReplyTarget | Mapping[str, Any],
        content: str,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        """Project durable Markdown content as a native Lark rich-text post.

        The cross-channel outbox deliberately stores one Markdown source.  Its
        WeChat projection remains owned by the WeChat sender; only this Lark
        adapter selects the pinned CLI's ``--markdown`` wire format, which
        converts the source to a Feishu/Lark ``post`` message with an ``md``
        element.  Plain text is also valid Markdown and keeps rendering as
        ordinary text.
        """

        target = ReplyTarget.from_dict(target)
        _validate_lark_reply_target(target, bot_id=self.profile.app_id)
        arguments = self._message_arguments(
            target,
            content_flag="--markdown",
            content=str(content),
            idempotency_key=idempotency_key,
        )
        response = await self.run_json(*arguments)
        if not _response_message_id(response):
            raise LarkProtocolError(
                "lark-cli send acknowledgement omitted a valid message ID"
            )
        return response

    @staticmethod
    def _message_arguments(
        target: ReplyTarget,
        *,
        content_flag: str,
        content: str,
        idempotency_key: str,
    ) -> tuple[str, ...]:
        key = str(idempotency_key or "").strip()
        if not key or len(key) > 50:
            raise LarkPermanentDeliveryError(
                "Lark idempotency key must contain at most 50 characters"
            )
        source_message_id = str(target.source_message_id or "").strip()
        if source_message_id:
            values = [
                "im",
                "+messages-reply",
                "--message-id",
                source_message_id,
                content_flag,
                content,
            ]
            if target.thread_id:
                values.append("--reply-in-thread")
        else:
            destination = str(target.destination_id or target.external_user_id or "")
            if not destination:
                raise LarkPermanentDeliveryError("Lark reply destination is missing")
            if target.destination_kind == "thread" or target.thread_id:
                raise LarkPermanentDeliveryError(
                    "Lark thread delivery requires its source message ID"
                )
            destination_flag = (
                "--chat-id"
                if target.destination_kind in {"chat", "group"}
                else "--user-id"
            )
            values = [
                "im",
                "+messages-send",
                destination_flag,
                destination,
                content_flag,
                content,
            ]
        values.extend(("--as", "bot", "--idempotency-key", key))
        return tuple(values)

    async def download_attachment(
        self,
        _profile: LarkBotProfile,
        envelope: InboundEnvelope,
        media: Mapping[str, Any],
    ) -> bytes:
        """Download one message-bound resource without persisting its key."""

        remote_id = str(media.get("remote_id") or "").strip()
        resource_type = "image" if str(media.get("kind") or "") == "image" else "file"
        if not remote_id:
            raise LarkProtocolError("Lark attachment key is missing")
        filename = Path(str(media.get("filename") or "attachment.bin")).name
        if not filename or filename in {".", ".."}:
            filename = "attachment.bin"
        with tempfile.TemporaryDirectory(prefix="codex-lark-download-") as directory:
            output = Path(directory) / filename
            response = await self.run_json(
                "im",
                "+messages-resources-download",
                "--message-id",
                envelope.external_message_id,
                "--file-key",
                remote_id,
                "--type",
                resource_type,
                "--output",
                f"./{filename}",
                "--as",
                "bot",
                cwd=directory,
                timeout=60.0,
            )
            try:
                data = self._business_data(response)
                saved_value = str(data.get("saved_path") or "").strip()
                reported_size = data.get("size_bytes")
                if not saved_value or type(reported_size) is not int or reported_size < 0:
                    raise LarkProtocolError(
                        "lark-cli returned invalid attachment metadata"
                    )
                saved_path = Path(saved_value)
                if not saved_path.is_absolute():
                    saved_path = Path(directory) / saved_path
                saved_path = saved_path.resolve(strict=True)
                saved_path.relative_to(Path(directory).resolve(strict=True))
                saved_info = saved_path.lstat()
                if stat.S_ISLNK(saved_info.st_mode) or not stat.S_ISREG(
                    saved_info.st_mode
                ):
                    raise LarkProtocolError(
                        "lark-cli returned an unsafe attachment path"
                    )
                if int(saved_info.st_size) != reported_size:
                    raise LarkProtocolError(
                        "lark-cli attachment size does not match its metadata"
                    )
                maximum = int(media.get("__maximum_bytes") or 0)
                if maximum and reported_size > maximum:
                    raise LarkPermanentDeliveryError(
                        "Lark attachment exceeds the managed file-size limit"
                    )
                return await asyncio.to_thread(saved_path.read_bytes)
            except (LarkError, LarkProtocolError):
                raise
            except (OSError, RuntimeError, ValueError) as exc:
                raise LarkProtocolError(
                    "lark-cli did not produce the requested attachment"
                ) from exc

    async def upload_media(
        self,
        path: str | os.PathLike[str],
        *,
        kind: str,
        filename: str = "",
        mime_type: str = "",
    ) -> Mapping[str, Any]:
        """Upload a managed local image/file and return a durable resource key."""

        supplied = Path(path).expanduser()
        try:
            supplied_info = supplied.lstat()
            if stat.S_ISLNK(supplied_info.st_mode) or not stat.S_ISREG(
                supplied_info.st_mode
            ):
                raise LarkPermanentDeliveryError(
                    "managed Lark attachment is unavailable"
                )
            candidate = supplied.resolve(strict=True)
            resolved_info = candidate.stat()
        except LarkPermanentDeliveryError:
            raise
        except (OSError, RuntimeError) as exc:
            raise LarkPermanentDeliveryError(
                "managed Lark attachment is unavailable"
            ) from exc
        if (supplied_info.st_dev, supplied_info.st_ino) != (
            resolved_info.st_dev,
            resolved_info.st_ino,
        ):
            raise LarkPermanentDeliveryError("managed Lark attachment is unavailable")
        relative = f"./{candidate.name}"
        normalized_kind = str(kind or "file").lower()
        if normalized_kind == "image":
            response = await self.run_json(
                "im",
                "images",
                "create",
                "--data",
                '{"image_type":"message"}',
                "--file",
                relative,
                "--as",
                "bot",
                cwd=candidate.parent,
                timeout=60.0,
            )
            data = self._business_data(response)
            remote_id = str(data.get("image_key") or "")
        else:
            suffix = candidate.suffix.lower()
            file_type = "stream"
            if normalized_kind == "audio" and suffix in {".opus", ".ogg"}:
                file_type = "opus"
            elif normalized_kind == "video" and suffix == ".mp4":
                file_type = "mp4"
            elif suffix == ".pdf":
                file_type = "pdf"
            response = await self.run_json(
                "im",
                "files",
                "create",
                "--data",
                json.dumps(
                    {
                        "file_type": file_type,
                        "file_name": Path(filename or candidate.name).name,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "--file",
                relative,
                "--as",
                "bot",
                cwd=candidate.parent,
                timeout=60.0,
            )
            data = self._business_data(response)
            remote_id = str(data.get("file_key") or "")
        if not remote_id:
            raise LarkProtocolError("lark-cli upload response omitted its resource key")
        return {
            "remote_id": remote_id,
            "kind": normalized_kind,
            "mime_type": str(mime_type or ""),
        }

    async def send_media(
        self,
        target: ReplyTarget | Mapping[str, Any],
        remote_id: str,
        *,
        kind: str,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        target = ReplyTarget.from_dict(target)
        _validate_lark_reply_target(target, bot_id=self.profile.app_id)
        content_flag = "--image" if str(kind or "").lower() == "image" else "--file"
        arguments = self._message_arguments(
            target,
            content_flag=content_flag,
            content=str(remote_id or ""),
            idempotency_key=idempotency_key,
        )
        if not remote_id:
            raise LarkPermanentDeliveryError("uploaded Lark resource key is missing")
        response = await self.run_json(*arguments)
        if not _response_message_id(response):
            raise LarkProtocolError(
                "lark-cli media acknowledgement omitted a valid message ID"
            )
        return response

    async def stop(self) -> None:
        process, self._process = self._process, None
        pump, self._pump_task = self._pump_task, None
        if process is not None:
            if process.returncode is None:
                if process.stdin is not None:
                    process.stdin.close()
                    with contextlib.suppress(Exception):
                        await process.stdin.wait_closed()
                try:
                    await asyncio.wait_for(process.wait(), timeout=self.stop_timeout)
                except asyncio.TimeoutError:
                    _signal_process_group(process, signal.SIGTERM)
                    try:
                        await asyncio.wait_for(
                            process.wait(), timeout=self.stop_timeout
                        )
                    except asyncio.TimeoutError:
                        _signal_process_group(process, signal.SIGKILL)
                        await process.wait()
            # The reviewed CLI may use helper processes.  Once the parent has
            # acknowledged shutdown, fence any descendant that did not exit
            # with it before allowing a new generation to start.
            _signal_process_group(process, signal.SIGKILL)
        if pump is not None:
            if not pump.done():
                pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _event_parts(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    header = _mapping(payload.get("header"))
    event = _mapping(payload.get("event"))
    if not event:
        data = _mapping(payload.get("data"))
        event = _mapping(data.get("event")) or data
    if not event and (
        payload.get("type") == EVENT_KEY
        or all(key in payload for key in ("message_id", "chat_id", "sender_id"))
    ):
        header = {
            "event_id": payload.get("event_id", ""),
            "event_type": payload.get("type", EVENT_KEY),
            "create_time": payload.get("timestamp", ""),
            "app_id": payload.get("app_id", ""),
            "tenant_key": payload.get("tenant_key", ""),
        }
        event = {
            "app_id": payload.get("app_id", ""),
            "sender": {
                "sender_type": payload.get("sender_type", ""),
                "sender_id": {"open_id": payload.get("sender_id", "")},
            },
            "message": {
                "_flattened": True,
                "message_id": payload.get("message_id", payload.get("id", "")),
                "chat_id": payload.get("chat_id", ""),
                "chat_type": payload.get("chat_type", ""),
                "message_type": payload.get("message_type", ""),
                "content": payload.get("content", ""),
                "mentions": payload.get("mentions") or (),
                "root_id": payload.get("root_id", ""),
                "thread_id": payload.get("thread_id", ""),
                "parent_id": payload.get("reply_to", ""),
                "create_time": payload.get("create_time", payload.get("timestamp", "")),
            },
        }
    return header, event


def _content_object(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {"text": value}
        return decoded if isinstance(decoded, Mapping) else {}
    return {}


def _rendered_media(message_type: str, value: Any) -> Mapping[str, Any] | None:
    """Parse the reviewed CLI's human-readable attachment markers."""

    rendered = str(value or "").strip()
    if message_type == "image":
        match = _IMAGE_CONTENT.fullmatch(rendered)
        if match is None:
            return None
        return {
            "kind": "image",
            "remote_id": match.group("key"),
            "filename": "image",
            "mime_type": "image/*",
        }
    match = _MEDIA_CONTENT.fullmatch(rendered)
    if match is None:
        return None
    marker_kind = match.group("kind")
    if message_type not in {marker_kind, "file", "audio", "video", "media"}:
        return None
    name_match = _MEDIA_NAME.search(match.group("tail") or "")
    filename = Path(name_match.group("name") if name_match else "").name
    kind = "video" if marker_kind == "video" else "audio" if marker_kind == "audio" else "file"
    guessed_mime = mimetypes.guess_type(filename)[0] if filename else None
    if not guessed_mime:
        guessed_mime = (
            "audio/ogg"
            if kind == "audio"
            else "video/mp4"
            if kind == "video"
            else "application/octet-stream"
        )
    return {
        "kind": kind,
        "remote_id": match.group("key"),
        "filename": filename,
        "mime_type": guessed_mime,
    }


def _timestamp(value: Any) -> str:
    if value in (None, ""):
        return datetime.now(timezone.utc).isoformat()
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value)
        with contextlib.suppress(ValueError):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
                timezone.utc
            ).isoformat()
        return datetime.now(timezone.utc).isoformat()
    if numeric > 10_000_000_000:
        numeric /= 1000.0
    return datetime.fromtimestamp(numeric, timezone.utc).isoformat()


def _mention_open_id(mention: Mapping[str, Any]) -> str:
    raw_identifier = mention.get("id", mention.get("user_id"))
    if isinstance(raw_identifier, str):
        return raw_identifier
    identifier = _mapping(raw_identifier)
    return str(
        identifier.get("open_id")
        or mention.get("open_id")
        or ""
    )


def _bot_mention(
    mentions: Sequence[Any], profile: LarkBotProfile
) -> Mapping[str, Any] | None:
    # Structured mention IDs are bot ``open_id`` values.  An app ID is a
    # different namespace and must never be accepted as a fallback identity.
    expected = profile.bot_open_id
    if not expected:
        return None
    for raw in mentions:
        mention = _mapping(raw)
        if _mention_open_id(mention) == expected:
            return mention
    return None


def _strip_authenticated_mention(text: str, mention: Mapping[str, Any] | None) -> str:
    if not mention:
        return text.strip()
    # The key is an authenticated structured entity such as @_user_1.  Never
    # remove the mutable display name, because ordinary user text may match it.
    key = str(mention.get("key") or "")
    if not key:
        return text.strip()
    return text.replace(key, "", 1).strip()


_RESERVED_IDENTITY_KEYS = frozenset(
    {
        "principal_id",
        "principal_account_id",
        "principal_mapping_revision",
        "mapping_revision",
        "identity_snapshot",
        "identity_snapshot_json",
        "conversation_subject_id",
    }
)


def sanitize_lark_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Keep useful non-secret provenance and reject spoofable identity fields."""

    header, event = _event_parts(payload)
    message = _mapping(event.get("message"))
    sender = _mapping(event.get("sender"))
    sender_id = _mapping(sender.get("sender_id"))
    mentions = message.get("mentions") or ()
    return {
        "event_id": str(header.get("event_id") or payload.get("event_id") or ""),
        "event_type": str(header.get("event_type") or EVENT_KEY),
        "tenant_key": str(header.get("tenant_key") or ""),
        "message_type": str(message.get("message_type") or ""),
        "chat_type": str(message.get("chat_type") or ""),
        "chat_id": str(message.get("chat_id") or ""),
        "thread_id": str(message.get("thread_id") or ""),
        "root_id": str(message.get("root_id") or ""),
        "reply_to": str(message.get("parent_id") or ""),
        "sender_id_type": str(sender_id.get("union_id") and "union_id" or "open_id"),
        "mentions": [
            {
                "key": str(_mapping(value).get("key") or ""),
                "open_id": _mention_open_id(_mapping(value)),
                "is_bot": bool(_mapping(value).get("is_bot", False)),
            }
            for value in mentions
            if isinstance(value, Mapping)
        ],
    }


def _subject_scope(
    profile: LarkBotProfile,
    *,
    actor_open_id: str,
    chat_type: str,
    chat_id: str,
    thread_id: str,
) -> tuple[str, str, str, str]:
    if chat_type in {"p2p", "direct", "private"}:
        subject = direct_conversation_subject(CHANNEL, profile.app_id, actor_open_id)
        return (
            subject.scope_key,
            subject.conversation_subject_id,
            "direct",
            "default",
        )
    if thread_id:
        subject = thread_conversation_subject(
            CHANNEL, profile.app_id, chat_id, thread_id
        )
        return (
            subject.scope_key,
            subject.conversation_subject_id,
            "thread",
            "default",
        )
    subject = group_conversation_subject(CHANNEL, profile.app_id, chat_id)
    return (
        subject.scope_key,
        subject.conversation_subject_id,
        "group",
        "default",
    )


def normalize_lark_event(
    payload: Mapping[str, Any],
    profile: LarkBotProfile | Mapping[str, Any] | Any,
    *,
    generation: int | None = None,
    active_generation: int | None = None,
    agent_id: str = "codex",
) -> InboundEnvelope | None:
    """Verify and normalize one ``im.message.receive_v1`` event."""

    profile = LarkBotProfile.from_value(profile)
    if generation is not None and active_generation is not None:
        if int(generation) != int(active_generation):
            return None
    if not isinstance(payload, Mapping):
        raise LarkProtocolError("Lark event is not an object")
    # Reserved identity values anywhere at the top level are never accepted as
    # resolver input.  They are ignored by sanitization, not trusted.
    header, event = _event_parts(payload)
    event_type = str(header.get("event_type") or payload.get("type") or EVENT_KEY)
    if event_type and event_type not in {EVENT_KEY, "event", "message"}:
        return None
    event_app_id = str(
        header.get("app_id") or event.get("app_id") or payload.get("app_id") or ""
    )
    if event_app_id and event_app_id != profile.app_id:
        raise LarkProtocolError("Lark event belongs to a different app profile")
    message = _mapping(event.get("message"))
    sender = _mapping(event.get("sender"))
    sender_id = _mapping(sender.get("sender_id"))
    actor_open_id = str(sender_id.get("open_id") or sender.get("open_id") or "")
    if not _OPEN_ID.fullmatch(actor_open_id):
        raise LarkProtocolError("Lark sender must have a tenant-stable open_id")
    sender_type = str(sender.get("sender_type") or "").strip().lower()
    if sender_type == "bot" or (
        profile.bot_open_id and actor_open_id == profile.bot_open_id
    ):
        return None
    external_message_id = str(message.get("message_id") or "")
    if not external_message_id:
        raise LarkProtocolError("Lark message ID is required")
    chat_id = str(message.get("chat_id") or "")
    if not chat_id:
        raise LarkProtocolError("Lark chat ID is required")
    chat_type = str(message.get("chat_type") or "").strip().lower()
    if chat_type not in {"p2p", "direct", "private", "group"}:
        raise LarkProtocolError("unsupported Lark chat type")
    root_id = str(message.get("root_id") or "")
    parent_id = str(message.get("parent_id") or "")
    thread_id = str(message.get("thread_id") or root_id or "")
    raw_content = message.get("content")
    content = _content_object(raw_content)
    flattened = bool(message.get("_flattened", False))
    message_type = str(message.get("message_type") or "text").lower()
    text = (
        str(raw_content or "")
        if flattened and message_type not in {"image", "file", "audio", "video", "media"}
        else str(content.get("text") or "")
        if message_type in {"text", "post", "interactive"}
        else ""
    )
    mentions = message.get("mentions") or ()
    if isinstance(mentions, Mapping):
        mentions = (mentions,)
    bot_mention = _bot_mention(tuple(mentions), profile)
    if chat_type == "group" and profile.mention_policy in {
        "required",
        "mention_required",
        "direct_or_mention",
    }:
        if bot_mention is None:
            return None
    text = _strip_authenticated_mention(text, bot_mention)

    media: list[dict[str, Any]] = []
    if message_type in {"image", "file", "audio", "video", "media"}:
        rendered_media = _rendered_media(message_type, raw_content) if flattened else None
        if rendered_media is not None:
            media.append(dict(rendered_media))
        else:
            remote_id = str(
                content.get("image_key")
                or content.get("file_key")
                or content.get("file_token")
                or content.get("key")
                or ""
            )
            if not remote_id:
                raise LarkProtocolError("Lark attachment key is missing")
            kind = "video" if message_type in {"video", "media"} else message_type
            filename = Path(
                str(content.get("file_name") or content.get("name") or "")
            ).name
            media.append(
                {
                    "kind": kind,
                    "remote_id": remote_id,
                    "filename": filename,
                    "mime_type": str(
                        content.get("mime_type")
                        or mimetypes.guess_type(filename)[0]
                        or "application/octet-stream"
                    ),
                }
            )
    if not text and not media:
        return None
    subject_scope, subject_id, subject_kind, session_id = _subject_scope(
        profile,
        actor_open_id=actor_open_id,
        chat_type=chat_type,
        chat_id=chat_id,
        thread_id=thread_id,
    )
    destination_kind = "open_id" if subject_kind == "direct" else subject_kind
    destination_id = actor_open_id if subject_kind == "direct" else chat_id
    raw: dict[str, Any] = {
        "channel_metadata": sanitize_lark_metadata(payload),
        "media": media,
    }
    if generation is not None:
        raw["generation"] = int(generation)
    return InboundEnvelope(
        channel=CHANNEL,
        bot_id=profile.app_id,
        external_user_id=actor_open_id,
        external_message_id=external_message_id,
        text=text,
        session_id=session_id,
        agent_id=str(agent_id or "codex"),
        conversation_id=conversation_id(
            CHANNEL,
            profile.app_id,
            subject_scope,
            session_id,
            str(agent_id or "codex"),
        ),
        received_at=_timestamp(message.get("create_time")),
        raw=raw,
        conversation_subject_id=subject_id,
        conversation_subject_scope=subject_scope,
        conversation_subject_kind=subject_kind,
        destination_kind=destination_kind,
        destination_id=destination_id,
        thread_id=thread_id,
        root_message_id=root_id,
        transport_metadata={
            "chat_id": chat_id,
            "chat_type": chat_type,
            "thread_id": thread_id,
            "root_id": root_id,
            "reply_to": parent_id,
        },
    )


class LarkAttachmentPromoter:
    """Download account-local Lark media into the managed attachment store."""

    def __init__(
        self,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        attachment_store: AttachmentStore,
        metadata_store: Any,
        *,
        downloader: Callable[[LarkBotProfile, InboundEnvelope, Mapping[str, Any]], Any],
        promotion_timeout: float = 60.0,
    ) -> None:
        self.profile = LarkBotProfile.from_value(profile)
        self.attachment_store = attachment_store
        self.metadata_store = metadata_store
        self.downloader = downloader
        self.promotion_timeout = max(0.1, float(promotion_timeout))

    @staticmethod
    def attachment_id(envelope: InboundEnvelope, ordinal: int) -> str:
        material = "\x1f".join(
            (
                envelope.channel,
                envelope.bot_id,
                envelope.external_message_id,
                str(int(ordinal)),
            )
        )
        return "lark-in-" + hashlib.sha256(material.encode()).hexdigest()[:40]

    async def _promote_one(
        self,
        envelope: InboundEnvelope,
        media: Mapping[str, Any],
        ordinal: int,
    ) -> dict[str, Any]:
        maximum = int(getattr(self.attachment_store, "max_file_size", 0) or 0)
        bounded_media = dict(media)
        if maximum:
            # Private adapter hint: the CLI boundary validates stat metadata and
            # rejects an oversized file before reading it into Python memory.
            bounded_media["__maximum_bytes"] = maximum
        payload = await _maybe_await(
            self.downloader(self.profile, envelope, bounded_media)
        )
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise LarkProtocolError("Lark attachment downloader returned non-bytes")
        if maximum and len(payload) > maximum:
            raise LarkPermanentDeliveryError(
                "Lark attachment exceeds the managed file-size limit"
            )
        attachment_id = self.attachment_id(envelope, ordinal)
        stored = await self.attachment_store.aput_bytes_idempotent(
            bytes(payload),
            attachment_id=attachment_id,
            filename=Path(str(media.get("filename") or "")).name,
            mime_type=str(media.get("mime_type") or "application/octet-stream"),
        )
        metadata = {
            "owner_agent_id": envelope.agent_id,
            "channel": CHANNEL,
            "bot_id": envelope.bot_id,
            "external_user_id": envelope.external_user_id,
            "session_id": envelope.session_id,
            "source_message_id": envelope.external_message_id,
            "source_ordinal": ordinal,
            # The remote key/temporary URL is intentionally absent.
        }
        await _call_first(
            self.metadata_store,
            ("register_attachment", "save_attachment"),
            stored,
            kind=str(media.get("kind") or "file").lower(),
            metadata=metadata,
        )
        kind = str(media.get("kind") or "file").lower()
        return {
            "attachment_id": stored.attachment_id,
            "path": stored.path,
            "mime_type": stored.mime_type,
            "kind": kind,
            "filename": stored.filename,
            "size": stored.size,
            "checksum": stored.checksum,
            "available": True,
            "native_input_available": kind == "image"
            and stored.mime_type.startswith("image/"),
        }

    async def promote(self, envelope: InboundEnvelope) -> InboundEnvelope:
        raw = dict(envelope.raw or {})
        values = raw.get("media") or ()
        if isinstance(values, Mapping):
            values = (values,)
        values = tuple(value for value in values if isinstance(value, Mapping))
        if not values:
            return envelope
        tasks = [
            asyncio.create_task(self._promote_one(envelope, media, ordinal))
            for ordinal, media in enumerate(values)
        ]
        try:
            promoted = await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=self.promotion_timeout
            )
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        raw["media"] = promoted
        # A non-secret digest retains replay diagnostics without persisting
        # expiring download tokens or remote object credentials.
        raw["media_fingerprints"] = [
            hashlib.sha256(
                json.dumps(
                    {
                        "kind": media.get("kind"),
                        "remote_id": media.get("remote_id"),
                        "filename": media.get("filename"),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            for media in values
        ]
        return replace(envelope, raw=raw)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _call_first(
    target: Any,
    names: Sequence[str],
    *args: Any,
    **kwargs: Any,
) -> Any:
    for candidate in (target, getattr(target, "store", None)):
        if candidate is None:
            continue
        for name in names:
            method = getattr(candidate, name, None)
            if not callable(method):
                continue
            try:
                signature = inspect.signature(method)
            except (TypeError, ValueError):
                return await _maybe_await(method(*args, **kwargs))
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            filtered = (
                kwargs
                if accepts_kwargs
                else {key: value for key, value in kwargs.items() if key in signature.parameters}
            )
            try:
                signature.bind(*args, **filtered)
            except TypeError:
                continue
            return await _maybe_await(method(*args, **filtered))
    raise AttributeError(f"no supported method: {', '.join(names)}")


def _supports_first(target: Any, names: Sequence[str]) -> bool:
    return any(
        callable(getattr(candidate, name, None))
        for candidate in (target, getattr(target, "store", None))
        if candidate is not None
        for name in names
    )


async def _drain_shielded_operation(operation: Any) -> tuple[Any, bool]:
    """Let a durable reservation settle even while its caller is cancelled."""

    task = asyncio.create_task(operation)
    cancelled_during_operation = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled_during_operation = True
    return task.result(), cancelled_during_operation


def _field(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _persisted_text(value: Any, fallback: str) -> str:
    """Render a durable timestamp/text field without changing missing values."""

    if value is None:
        return fallback
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)


def _principal_mapping_revision(
    value: Any,
    *,
    fallback: int | None = None,
    allow_unmapped: bool = False,
) -> int | None:
    """Normalize a trusted resolver/store mapping revision, failing closed."""

    if value is None or value == "":
        return fallback
    if isinstance(value, bool):
        raise LarkProtocolError("principal mapping revision is invalid")
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise LarkProtocolError("principal mapping revision is invalid") from exc
    if revision == 0 and allow_unmapped:
        return 0
    if revision <= 0:
        raise LarkProtocolError("principal mapping revision is invalid")
    return revision


def _snapshot_mapping(value: Any, key: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    nested = value.get(key, {})
    return nested if isinstance(nested, Mapping) else {}


async def _restore_persisted_command_envelope(
    acceptance: Any,
    envelope: InboundEnvelope,
    *,
    expected_bot_id: str,
) -> InboundEnvelope:
    """Restore the store-owned ingress and route snapshot for a command.

    Lark normalization necessarily starts with the default Agent route.  The
    manager selects the actual route while accepting the inbound and stores it
    under ``__command_snapshot``.  Both first delivery and redelivery must use
    that durable envelope: a later ``/agent`` switch must not reinterpret the
    command, its task scope, or its exact reply destination.
    """

    inbound = _field(acceptance, "inbound", default=acceptance)
    persisted_channel = _field(inbound, "channel", default=None)
    persisted_bot = _field(inbound, "bot_id", default=None)
    persisted_user = _field(
        inbound, "external_user_id", "user_id", default=None
    )
    persisted_message = _field(
        inbound,
        "external_message_id",
        "source_message_id",
        default=None,
    )
    persisted_payload = _field(inbound, "payload", default=None)
    has_persisted_envelope = any(
        value is not None
        for value in (
            persisted_channel,
            persisted_bot,
            persisted_user,
            persisted_message,
            persisted_payload,
        )
    )
    if not has_persisted_envelope:
        # Compatibility facades may return only an Agent ID.  They have no
        # durable envelope to restore, so retain the already verified event.
        return envelope

    channel = str(persisted_channel or envelope.channel)
    bot_id = str(persisted_bot or envelope.bot_id)
    if channel.strip().lower() != CHANNEL or bot_id != expected_bot_id:
        raise LarkProtocolError(
            "persisted Lark command belongs to a different bot account"
        )

    raw = (
        dict(persisted_payload)
        if isinstance(persisted_payload, Mapping)
        else dict(envelope.raw or {})
    )
    identity_snapshot = _field(inbound, "identity_snapshot", default={})
    subject_snapshot = _snapshot_mapping(
        identity_snapshot, "conversation_subject"
    )
    destination_snapshot = _snapshot_mapping(identity_snapshot, "destination")
    principal_snapshot = _snapshot_mapping(identity_snapshot, "principal")
    persisted_mapping_revision = _field(
        inbound,
        "principal_mapping_revision",
        "mapping_revision",
        default=None,
    )
    if persisted_mapping_revision is None:
        persisted_mapping_revision = principal_snapshot.get("mapping_revision")
    durable_unmapped = bool(
        principal_snapshot
        and str(principal_snapshot.get("source") or "") == "unmapped"
        and not principal_snapshot.get("principal_id")
        and not principal_snapshot.get("principal_account_id")
    )
    if persisted_mapping_revision is None and durable_unmapped:
        persisted_mapping_revision = 0

    source_sequence = _field(inbound, "source_sequence", "seq", default=None)
    if source_sequence is not None:
        try:
            source_sequence = int(source_sequence)
        except (TypeError, ValueError):
            source_sequence = envelope.source_sequence
    transport_metadata = _field(inbound, "transport_metadata", default=None)
    if not isinstance(transport_metadata, Mapping):
        transport_metadata = destination_snapshot.get("transport_metadata", {})
    if not isinstance(transport_metadata, Mapping):
        transport_metadata = {}

    restored = replace(
        envelope,
        channel=channel,
        bot_id=bot_id,
        external_user_id=str(persisted_user or envelope.external_user_id),
        external_message_id=str(
            persisted_message or envelope.external_message_id
        ),
        text=str(
            _field(inbound, "text", default=envelope.text)
            if _field(inbound, "text", default=None) is not None
            else envelope.text
        ),
        session_id=str(
            _field(inbound, "session_id", default=envelope.session_id)
            or "default"
        ),
        source_sequence=source_sequence,
        context_token=str(
            _field(inbound, "context_token", default=envelope.context_token)
            or ""
        ),
        received_at=_persisted_text(
            _field(inbound, "received_at", default=None), envelope.received_at
        ),
        raw=raw,
        conversation_subject_id=str(
            _field(inbound, "conversation_subject_id", default=None)
            or subject_snapshot.get("conversation_subject_id")
            or envelope.conversation_subject_id
        ),
        conversation_subject_scope=str(
            _field(inbound, "conversation_subject_scope", default=None)
            or subject_snapshot.get("scope_key")
            or envelope.conversation_subject_scope
        ),
        conversation_subject_kind=str(
            _field(inbound, "conversation_subject_kind", default=None)
            or subject_snapshot.get("kind")
            or envelope.conversation_subject_kind
        ),
        principal_id=str(
            _field(inbound, "principal_id", default=None)
            or principal_snapshot.get("principal_id")
            or ""
        ),
        principal_account_id=str(
            _field(inbound, "principal_account_id", default=None)
            or principal_snapshot.get("principal_account_id")
            or ""
        ),
        principal_mapping_revision=_principal_mapping_revision(
            persisted_mapping_revision,
            fallback=envelope.principal_mapping_revision,
            allow_unmapped=durable_unmapped,
        ),
        destination_kind=str(
            _field(inbound, "destination_kind", default=None)
            or destination_snapshot.get("kind")
            or envelope.destination_kind
        ),
        destination_id=str(
            _field(inbound, "destination_id", default=None)
            or destination_snapshot.get("id")
            or envelope.destination_id
        ),
        thread_id=str(
            _field(inbound, "thread_id", default=None)
            or destination_snapshot.get("thread_id")
            or envelope.thread_id
        ),
        root_message_id=str(
            _field(inbound, "root_message_id", default=None)
            or destination_snapshot.get("root_message_id")
            or envelope.root_message_id
        ),
        transport_metadata=dict(transport_metadata),
    )

    command_snapshot = (
        persisted_payload.get("__command_snapshot")
        if isinstance(persisted_payload, Mapping)
        else None
    )
    if isinstance(command_snapshot, Mapping):
        snapshot = dict(command_snapshot)
        raw = dict(restored.raw or {})
        raw["__command_snapshot"] = snapshot
        restored = replace(
            restored,
            agent_id=str(
                snapshot.get("agent_id") or restored.agent_id or "codex"
            ),
            conversation_id=str(
                snapshot.get("conversation_id")
                or restored.conversation_id
                or ""
            ),
            raw=raw,
        )
    return restored


def _command_receipt_matches(
    receipt: Any, command: ChannelCommand, envelope: InboundEnvelope
) -> bool:
    if receipt is None:
        return False
    expected = (
        ("channel", envelope.channel),
        ("bot_id", envelope.bot_id),
        ("external_user_id", envelope.external_user_id),
        ("session_id", envelope.session_id or "default"),
        ("external_message_id", envelope.external_message_id),
        ("command_name", command.name),
        ("command_text", envelope.text),
    )
    if any(
        str(_field(receipt, field, default="") or "") != str(value or "")
        for field, value in expected
    ):
        return False
    stored_args = tuple(
        str(value)
        for value in (_field(receipt, "command_args", default=()) or ())
    )
    return stored_args == tuple(str(value) for value in command.args)


class LarkCommandRouter:
    """Authority/capability guard around the shared command effect router."""

    def __init__(
        self,
        manager: Any,
        *,
        shared_router: Any | None = None,
        administrator: Callable[[InboundEnvelope], bool | Awaitable[bool]] | None = None,
        onboarding_service: Any | None = None,
        shell_cwd: str | Path | None = None,
    ) -> None:
        if shared_router is None:
            shared_router = MVPCommandRouter(manager, shell_cwd=shell_cwd)
        self.manager = manager
        self.shared_router = shared_router
        self.administrator = administrator
        self.onboarding_service = onboarding_service

    async def _is_admin(self, envelope: InboundEnvelope) -> bool:
        if self.administrator is None:
            return False
        return bool(await _maybe_await(self.administrator(envelope)))

    async def _enabled_agent(self, identifier: str) -> bool:
        try:
            values = await _call_first(
                self.manager,
                ("list_agents", "agents"),
                include_disabled=False,
            )
        except Exception:
            return False
        for value in values or ():
            agent_id = str(_field(value, "agent_id", "id", default="") or "")
            enabled = bool(_field(value, "enabled", default=True))
            if enabled and agent_id.casefold() == identifier.casefold():
                return True
        return False

    async def handle_command(
        self,
        command: ChannelCommand,
        envelope: InboundEnvelope,
        *,
        command_id: str = "",
    ) -> str | None:
        # Feishu users commonly use the singular spelling.  Keep this alias
        # adapter-local so the historical WeChat registry, help text, and
        # unknown-command response remain byte-for-byte unchanged.
        if command.name == "task":
            command = replace(command, name="tasks")
        if command.name == "recv":
            return "unsupported command on Lark: /recv"
        if command.name == "lark":
            if (
                not command.args
                or command.args[0].casefold() != "add"
                or len(command.args) > 2
            ):
                return "usage: /lark add [profile]"
            # This authority is intentionally narrower than the configurable
            # Lark administrator allowlist.  Only an enabled, exact account
            # mapping returned by PrincipalResolver may start credential
            # provisioning; free-form message metadata is never consulted.
            if (
                envelope.principal_id != "owner"
                or not envelope.principal_account_id
            ):
                return "owner authority is required to add a Lark bot"
            if envelope.conversation_subject_kind != "direct":
                return "Lark bot onboarding is only available in a direct chat"
            if self.onboarding_service is None:
                return "Lark bot onboarding is unavailable"
            try:
                await _call_first(
                    self.onboarding_service,
                    ("start", "start_onboarding", "start_chat_onboarding"),
                    envelope,
                    command_id=command_id or _command_id(envelope),
                    profile_id=(command.args[1] if len(command.args) == 2 else ""),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Onboarding output can contain credentials.  Do not relay an
                # arbitrary service exception (or traceback) to either chat or
                # ordinary logs; the service owns its redacted diagnostics.
                logger.error(
                    "Lark chat onboarding failed to start (%s)",
                    type(exc).__name__,
                )
                return "Lark bot onboarding could not be started; try again later"
            # The service durably publishes the exact verification URL and QR
            # bundle, then the eventual success/failure notification.  An
            # empty receipt response prevents the gateway from creating a
            # second text-only acknowledgement on replay.
            return None
        admin = await self._is_admin(envelope)
        if command.name == "delagent" and not admin:
            return "administrator authority is required to delete a global Agent"
        if command.name == "agent" and command.args and not admin:
            if len(command.args) > 1:
                return "administrator authority is required to select an Agent profile"
            if not await self._enabled_agent(command.args[0]):
                return "Agent does not exist or is disabled"
            # The shared command router must use the lifecycle-fenced
            # existing-only switch capability.  A list-then-ordinary-switch
            # sequence could otherwise recreate an Agent deleted between the
            # two operations.
            return await _call_first(
                self.shared_router,
                ("handle_command",),
                command,
                envelope,
                command_id=command_id,
                existing_agent_only=True,
            )
        if command.name == "ask" and len(command.args) >= 2 and not admin:
            # The shared router calls ``ensure_agent`` for compatibility with
            # explicitly dynamic/admin workflows.  A Lark user without global
            # authority may address an existing enabled Agent, but must never
            # create or revive one as a side effect of `/ask`.
            if not await self._enabled_agent(command.args[0]):
                return "Agent does not exist or is disabled"
            if bool(getattr(self.manager, "allow_dynamic_agents", True)):
                lifecycle_lock = getattr(self.manager, "_dynamic_agent_lock", None)
                if lifecycle_lock is None:
                    return "cannot ask Agent: existing-Agent authority is unavailable"
                # Close the list/ensure TOCTOU window against global deletion.
                # TaskManager's ensure path returns before re-taking this lock
                # for an existing live registration; unknown/deleted names are
                # rejected by the second check while deletion is fenced.
                async with lifecycle_lock:
                    if not await self._enabled_agent(command.args[0]):
                        return "Agent does not exist or is disabled"
                    return await _maybe_await(
                        self.shared_router.handle_command(
                            command,
                            envelope,
                            command_id=command_id,
                        )
                    )
        return await _maybe_await(
            self.shared_router.handle_command(
                command,
                envelope,
                command_id=command_id,
            )
        )


def _command_id(envelope: InboundEnvelope) -> str:
    return compound_id(
        "lark-command",
        (envelope.bot_id, envelope.external_message_id),
    )


def _delivery_id(envelope: InboundEnvelope) -> str:
    return compound_id(
        "lark-delivery",
        (envelope.bot_id, envelope.external_message_id, "command"),
    )


def parse_lark_command(text: str) -> ChannelCommand | None:
    """Parse slash commands plus the deliberately exact chat onboarding phrase.

    Natural-language triggering stays adapter-local so the same text remains
    an ordinary Agent prompt on WeChat and every other channel.  Only these
    complete phrases are recognized; additional prose cannot accidentally
    start a credential-mutating operation.
    """

    command = parse_command(text)
    if command is not None:
        return command
    normalized = " ".join(str(text or "").strip().split()).casefold()
    if normalized in _LARK_ONBOARDING_PHRASES:
        return ChannelCommand(name="lark", args=("add",), raw=str(text or ""))
    return None


class LarkGateway:
    """Apply Lark intake policy, principal resolution and durable acceptance."""

    def __init__(
        self,
        runtime: Any,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        *,
        principal_resolver: Any | None = None,
        command_router: LarkCommandRouter | Any | None = None,
        administrator: Callable[[InboundEnvelope], bool | Awaitable[bool]] | None = None,
        onboarding_service: Any | None = None,
        shell_cwd: str | Path | None = None,
        attachment_promoter: LarkAttachmentPromoter | None = None,
    ) -> None:
        self.runtime = runtime
        self.profile = LarkBotProfile.from_value(profile)
        self.principal_resolver = principal_resolver
        self.attachment_promoter = attachment_promoter
        self.command_router = command_router or LarkCommandRouter(
            runtime,
            administrator=administrator,
            onboarding_service=onboarding_service,
            shell_cwd=shell_cwd,
        )

    async def _resolve_principal(self, envelope: InboundEnvelope) -> InboundEnvelope:
        resolver = self.principal_resolver
        if resolver is None:
            return envelope
        # Construct the typed actor when the identity module exposes it.  No
        # raw event object or free-form principal string crosses this boundary.
        try:
            from src.runtime.identity import AuthenticatedActor

            actor: Any = AuthenticatedActor(
                channel=CHANNEL,
                bot_id=self.profile.app_id,
                external_user_id=envelope.external_user_id,
            )
        except (ImportError, TypeError):
            actor = (CHANNEL, self.profile.app_id, envelope.external_user_id)
        method = getattr(resolver, "resolve", resolver)
        resolution = await _maybe_await(method(actor))
        principal_id = str(_field(resolution, "principal_id", default="") or "")
        account_id = str(
            _field(resolution, "principal_account_id", "account_id", default="") or ""
        )
        if not principal_id:
            return envelope
        return replace(
            envelope,
            principal_id=principal_id,
            principal_account_id=account_id,
            principal_mapping_revision=_principal_mapping_revision(
                _field(resolution, "mapping_revision", default=None)
            ),
        )

    async def accept_event(
        self,
        payload: Mapping[str, Any],
        *,
        generation: int | None = None,
        active_generation: int | None = None,
    ) -> Any:
        envelope = normalize_lark_event(
            payload,
            self.profile,
            generation=generation,
            active_generation=active_generation,
        )
        if envelope is None:
            return None
        # Authorization precedes all remote-resource I/O.  In particular, an
        # unmapped/denied actor cannot make this process download and persist a
        # message-bound attachment merely by addressing the bot.
        envelope = await self._resolve_principal(envelope)
        if self.profile.access_policy in {"principals_only", "mapped"}:
            if not envelope.principal_id:
                return None
        if envelope.raw and envelope.raw.get("media") and self.attachment_promoter is None:
            # Remote Lark keys are message-bound and sometimes short-lived.
            # They must be promoted into the managed store before durable
            # acceptance, never copied into generic task metadata.
            raise LarkProtocolError("Lark attachment promoter is unavailable")
        if self.attachment_promoter is not None:
            envelope = await self.attachment_promoter.promote(envelope)
        command = parse_lark_command(envelope.text)
        inputs: dict[str, Any] = {"text": envelope.text}
        if envelope.raw and envelope.raw.get("media"):
            inputs["media"] = list(envelope.raw["media"])
        acceptance = await _call_first(
            self.runtime,
            ("accept_inbound", "submit_inbound", "enqueue_inbound"),
            envelope,
            create_task=command is None,
            inputs=inputs,
            conversation_id=envelope.conversation_id,
            reply_target=envelope.reply_target.to_dict(),
            actor=envelope.principal_id or envelope.external_user_id,
        )
        if command is None:
            return acceptance
        envelope = await _restore_persisted_command_envelope(
            acceptance,
            envelope,
            expected_bot_id=self.profile.app_id,
        )
        command_id = _command_id(envelope)
        begin_names = ("begin_command_receipt", "reserve_command", "begin_command")
        get_names = ("get_command_receipt", "get_command")
        complete_names = ("complete_command_receipt", "complete_command")
        interrupt_names = ("interrupt_command_receipt", "interrupt_command")
        durable_receipts = _supports_first(self.runtime, begin_names)
        owns_receipt = not durable_receipts
        response = ""
        response_agent_id = str(
            _field(acceptance, "agent_id", default=envelope.agent_id)
            or envelope.agent_id
        )

        async def interrupt_owned_receipt() -> None:
            if not durable_receipts:
                return
            if not _supports_first(self.runtime, interrupt_names):
                raise RuntimeError("failed command receipt cannot be terminalized")
            try:
                await _call_first(self.runtime, interrupt_names, command_id)
            except Exception as exc:
                raise RuntimeError(
                    "failed command receipt was not durably terminalized"
                ) from exc

        async def drain_owned_receipt_interrupt() -> None:
            terminalization = asyncio.create_task(interrupt_owned_receipt())
            cancelled_during_cleanup = False
            while not terminalization.done():
                try:
                    await asyncio.shield(terminalization)
                except asyncio.CancelledError:
                    cancelled_during_cleanup = True
            terminalization.result()
            if cancelled_during_cleanup:
                raise asyncio.CancelledError

        if durable_receipts:
            # Once an adapter advertises reservations, all recovery operations
            # are mandatory.  Executing an effect with only a partial receipt
            # API would leave no safe crash/cancellation boundary.
            if not all(
                _supports_first(self.runtime, names)
                for names in (get_names, complete_names, interrupt_names)
            ):
                raise RuntimeError("command receipt lifecycle is incomplete")
            try:
                receipt, cancelled_during_reservation = (
                    await _drain_shielded_operation(
                        _call_first(
                            self.runtime,
                            begin_names,
                            command_id,
                            channel=CHANNEL,
                            bot_id=self.profile.app_id,
                            external_user_id=envelope.external_user_id,
                            session_id=envelope.session_id,
                            external_message_id=envelope.external_message_id,
                            command_name=command.name,
                            command_args=command.args,
                            command_text=envelope.text,
                            principal_id=envelope.principal_id,
                            principal_account_id=envelope.principal_account_id,
                            conversation_subject_id=envelope.conversation_subject_id,
                        )
                    )
                )
            except Exception as exc:
                raise RuntimeError("command execution was not durably reserved") from exc
            if not _command_receipt_matches(receipt, command, envelope):
                raise RuntimeError("command receipt identity conflicts")
            receipt_state = str(
                _field(receipt, "state", default="") or ""
            ).lower()
            owns_receipt = bool(
                _field(receipt, "created", "reserved", default=False)
            )
            if cancelled_during_reservation:
                if owns_receipt and receipt_state == "started":
                    await drain_owned_receipt_interrupt()
                raise asyncio.CancelledError
            if receipt_state == "completed":
                response = str(
                    _field(receipt, "response_text", "content", default="") or ""
                )
                response_agent_id = str(
                    _field(
                        receipt,
                        "response_agent_id",
                        default=response_agent_id,
                    )
                    or response_agent_id
                )
            elif receipt_state == "interrupted":
                response = COMMAND_INTERRUPTED_RESPONSE
            elif receipt_state == "started" and not owns_receipt:
                # Another live invocation owns this exact command.  Its stable
                # outbox projection will be published by that owner.
                return acceptance
            elif receipt_state != "started" or not owns_receipt:
                raise RuntimeError(
                    "command receipt has unsupported state: "
                    f"{receipt_state or 'unknown'}"
                )

        if owns_receipt:
            try:
                routed_response = await self.command_router.handle_command(
                    command,
                    envelope,
                    command_id=command_id if durable_receipts else "",
                )
                response = str(routed_response or "")
                if durable_receipts:
                    # Some control effects (for example `/system` and `/cd`)
                    # atomically complete their receipt inside the manager/store
                    # transaction.  Re-read before attempting generic completion.
                    current = await _call_first(self.runtime, get_names, command_id)
                    if not _command_receipt_matches(current, command, envelope):
                        raise RuntimeError("command receipt identity conflicts")
                    current_state = str(
                        _field(current, "state", default="") or ""
                    ).lower()
                    if current_state == "completed":
                        recorded_response = str(
                            _field(
                                current,
                                "response_text",
                                "content",
                                default="",
                            )
                            or ""
                        )
                        if recorded_response != response:
                            raise RuntimeError("command response receipt conflicts")
                        response = recorded_response
                        response_agent_id = str(
                            _field(
                                current,
                                "response_agent_id",
                                default=response_agent_id,
                            )
                            or response_agent_id
                        )
                    elif current_state == "started":
                        if command.name == "agent" and command.args:
                            with contextlib.suppress(Exception):
                                active = await _call_first(
                                    self.runtime,
                                    ("get_active_agent", "active_agent"),
                                    channel=envelope.channel,
                                    bot_id=envelope.bot_id,
                                    external_user_id=(
                                        envelope.routing_subject_id
                                        or envelope.external_user_id
                                    ),
                                    session_id=envelope.session_id,
                                )
                                active = _field(
                                    active,
                                    "agent_id",
                                    "id",
                                    default=active,
                                )
                                if active:
                                    response_agent_id = str(active)
                        completed = await _call_first(
                            self.runtime,
                            complete_names,
                            command_id,
                            response_text=response,
                            response_agent_id=response_agent_id,
                        )
                        if not _command_receipt_matches(
                            completed, command, envelope
                        ) or str(
                            _field(completed, "state", default="") or ""
                        ).lower() != "completed":
                            raise RuntimeError(
                                "command response was not durably recorded"
                            )
                        response = str(
                            _field(
                                completed,
                                "response_text",
                                "content",
                                default=response,
                            )
                            or ""
                        )
                    else:
                        raise RuntimeError(
                            "command receipt lost ownership before completion"
                        )
            except asyncio.CancelledError:
                await drain_owned_receipt_interrupt()
                raise
            except Exception:
                await drain_owned_receipt_interrupt()
                raise
        if response:
            target = envelope.reply_target
            delivery_id = _delivery_id(envelope)
            idempotency_key = str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"codex-lark:{delivery_id}")
            )
            delivery = UserDelivery(
                delivery_id=delivery_id,
                target=target,
                from_user_id=self.profile.app_id,
                content=response,
                client_id=idempotency_key,
                idempotency_key=idempotency_key,
                sender_account={"channel": CHANNEL, "bot_id": self.profile.app_id},
                transport_metadata=dict(target.transport_metadata),
            )
            await _call_first(
                self.runtime,
                (
                    "create_account_outbox",
                    "enqueue_account_outbox",
                    "create_lark_outbox",
                    "create_user_outbox",
                    "enqueue_user_outbox",
                ),
                delivery=delivery.to_dict(),
                channel=CHANNEL,
                bot_id=self.profile.app_id,
                external_user_id=envelope.external_user_id,
                session_id=envelope.session_id,
                agent_id=response_agent_id,
                foreground=True,
                bypass_channel_reply_scope=True,
            )
        return acceptance

    handle_event = accept_event


def _record_target(record: Any) -> ReplyTarget:
    raw = _field(record, "reply_target", "target", default={})
    if isinstance(raw, ReplyTarget):
        return raw
    if hasattr(raw, "to_dict"):
        raw = raw.to_dict()
    return ReplyTarget.from_dict(raw)


def _lark_idempotency_key(record: Any) -> str:
    supplied = str(
        _field(
            record,
            "transport_idempotency_key",
            "idempotency_key",
            "client_id",
            default="",
        )
        or ""
    )
    if supplied:
        return supplied
    delivery_id = str(_field(record, "outbox_id", "delivery_id", default="") or "")
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-lark:{delivery_id}"))


def lark_media_idempotency_key(record: Any) -> str:
    """Return the bounded, stable wire key for one attachment message."""

    durable_key = str(
        _field(record, "idempotency_key", "media_id", default="") or ""
    ).strip()
    if not durable_key:
        durable_key = str(
            _field(record, "attachment_id", "outbox_id", default="") or ""
        ).strip()
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"codex-lark-media:{durable_key}",
        )
    )


def lark_bundle_text_idempotency_key(record: Any) -> str:
    """Return the bounded, stable wire key for a mixed bundle's text."""

    durable_key = str(
        _field(
            record,
            "transport_idempotency_key",
            "client_id",
            "outbox_id",
            default="",
        )
        or ""
    ).strip()
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"codex-lark-bundle-text:{durable_key}",
        )
    )


@dataclass(frozen=True, slots=True)
class _LarkMediaCallbackRecord:
    """Read-through media row with a transport-safe idempotency key."""

    record: Any
    idempotency_key: str
    transport_idempotency_key: str
    text_idempotency_key: str

    def __getattr__(self, name: str) -> Any:
        missing = object()
        value = _field(self.record, name, default=missing)
        if value is missing:
            raise AttributeError(name)
        return value


def _media_callback_record(record: Any, *, text: bool = False) -> Any:
    key = (
        lark_bundle_text_idempotency_key(record)
        if text
        else lark_media_idempotency_key(record)
    )
    return _LarkMediaCallbackRecord(
        record=record,
        idempotency_key=key,
        transport_idempotency_key=key,
        text_idempotency_key=lark_bundle_text_idempotency_key(record),
    )


class LarkDeliveryWorker:
    """Exact-account, lease-fenced text delivery loop."""

    def __init__(
        self,
        store: Any,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        client: LarkCliProcess | Any,
        *,
        worker_id: str | None = None,
        claim_limit: int = 1,
        poll_interval: float = 1.0,
        lease_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.profile = LarkBotProfile.from_value(profile)
        self.client = client
        suffix = hashlib.sha256(self.profile.app_id.encode()).hexdigest()[:12]
        self.worker_id = worker_id or f"lark-text:{self.profile.profile_id}:{suffix}"
        self.claim_limit = max(1, int(claim_limit))
        self.poll_interval = max(0.05, float(poll_interval))
        self.lease_seconds = max(0.1, float(lease_seconds))
        self._stop = asyncio.Event()
        self._backoff_until = 0.0

    def stop(self) -> None:
        self._stop.set()

    async def _claim(self) -> Sequence[Any]:
        kwargs = {
            "channel": CHANNEL,
            "bot_id": self.profile.app_id,
            "limit": self.claim_limit,
            "lease_seconds": self.lease_seconds,
            "has_attachments": False,
        }
        return await _call_first(
            self.store,
            ("claim_account_outbox", "claim_text_outbox", "claim_outbox"),
            self.worker_id,
            **kwargs,
        )

    async def _heartbeat(
        self, outbox_id: str, claim_token: str, lost: asyncio.Event
    ) -> None:
        interval = max(0.05, self.lease_seconds / 3.0)
        while not lost.is_set():
            await asyncio.sleep(interval)
            try:
                renewed = await _call_first(
                    self.store,
                    ("renew_outbox_lease",),
                    outbox_id,
                    claim_token,
                    lease_seconds=self.lease_seconds,
                )
            except Exception:
                lost.set()
                return
            if renewed is False:
                lost.set()
                return

    async def _send(self, record: Any) -> DeliveryReceipt:
        channel = str(_field(record, "channel", default="") or "")
        bot_id = str(_field(record, "bot_id", default="") or "")
        if (channel, bot_id) != (CHANNEL, self.profile.app_id):
            return DeliveryReceipt(
                delivery_id=str(_field(record, "outbox_id", default="") or ""),
                sent=False,
                retryable=False,
                outcome="permanent_failure",
                error="outbox account does not match Lark worker",
                error_code="wrong_account",
            )
        target = _record_target(record)
        content = str(_field(record, "content", "text", default="") or "")
        delivery_id = str(
            _field(record, "outbox_id", "delivery_id", default="") or ""
        )
        key = _lark_idempotency_key(record)
        if not delivery_id or not content:
            return DeliveryReceipt(
                delivery_id=delivery_id,
                client_id=key,
                sent=False,
                retryable=False,
                outcome="permanent_failure",
                error="Lark text delivery is incomplete",
                error_code="invalid_payload",
            )
        try:
            _validate_lark_reply_target(
                target,
                bot_id=self.profile.app_id,
                external_user_id=str(
                    _field(record, "external_user_id", default="") or ""
                ),
            )
            response = await _maybe_await(
                self.client.send_text(target, content, idempotency_key=key)
            )
        except LarkRateLimitError as exc:
            return DeliveryReceipt(
                delivery_id=delivery_id,
                client_id=key,
                sent=False,
                retryable=True,
                retry_after_seconds=exc.retry_after,
                outcome="retryable_failure",
                error="Lark rate limit",
                error_code="rate_limited",
            )
        except LarkPermanentDeliveryError as exc:
            return DeliveryReceipt(
                delivery_id=delivery_id,
                client_id=key,
                sent=False,
                retryable=False,
                outcome="permanent_failure",
                error=redact_lark_text(exc),
                error_code="invalid_destination",
            )
        except Exception as exc:
            logger.warning(
                "Lark text send failed for %s: %s",
                delivery_id,
                redact_lark_text(exc),
            )
            return DeliveryReceipt(
                delivery_id=delivery_id,
                client_id=key,
                sent=False,
                retryable=True,
                outcome="unknown",
                error=redact_lark_text(exc),
                error_code="transport_error",
            )
        remote_id = _response_message_id(response)
        if not remote_id:
            return DeliveryReceipt(
                delivery_id=delivery_id,
                client_id=key,
                sent=False,
                retryable=True,
                outcome="unknown",
                error="Lark send acknowledgement omitted a valid message ID",
                error_code="invalid_acknowledgement",
            )
        return DeliveryReceipt(
            delivery_id=delivery_id,
            client_id=key,
            sent=True,
            retryable=False,
            outcome="sent",
            remote_delivery_id=remote_id,
        )

    async def run_once(self) -> list[DeliveryReceipt]:
        now = asyncio.get_running_loop().time()
        if now < self._backoff_until:
            return []
        rows = await self._claim()
        if rows is None:
            return []
        if isinstance(rows, Mapping) or not isinstance(rows, Sequence):
            rows = (rows,)
        receipts: list[DeliveryReceipt] = []
        for record in rows:
            outbox_id = str(
                _field(record, "outbox_id", "delivery_id", default="") or ""
            )
            token = str(_field(record, "claim_token", default="") or "")
            lost = asyncio.Event()
            heartbeat = (
                asyncio.create_task(self._heartbeat(outbox_id, token, lost))
                if outbox_id and token and hasattr(self.store, "renew_outbox_lease")
                else None
            )
            try:
                started = await _call_first(
                    self.store,
                    ("mark_outbox_sending", "start_outbox_delivery"),
                    outbox_id,
                    claim_token=token or None,
                )
                if started is False or lost.is_set():
                    continue
                receipt = await self._send(record)
                if lost.is_set():
                    continue
                receipts.append(receipt)
                if receipt.sent:
                    await _call_first(
                        self.store,
                        ("finish_outbox_attempt", "mark_outbox_sent", "complete_outbox"),
                        outbox_id,
                        claim_token=token or None,
                        outcome="sent",
                        client_id=receipt.client_id,
                        remote_delivery_id=receipt.remote_delivery_id,
                        transport_receipt=receipt.transport_metadata,
                    )
                else:
                    delay = receipt.retry_after_seconds or min(
                        300.0,
                        2.0 ** max(0, int(_field(record, "attempts", default=1) or 1) - 1),
                    )
                    if receipt.error_code == "rate_limited":
                        self._backoff_until = asyncio.get_running_loop().time() + delay
                    await _call_first(
                        self.store,
                        ("finish_outbox_attempt", "mark_outbox_failed", "fail_outbox"),
                        outbox_id,
                        claim_token=token or None,
                        outcome=receipt.outcome,
                        error=receipt.error,
                        last_error=receipt.error,
                        retry=receipt.retryable,
                        permanent=not receipt.retryable,
                        retry_after=delay,
                        delay=delay,
                        transport_receipt=receipt.transport_metadata,
                    )
            finally:
                lost.set()
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
        return receipts

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Lark text delivery iteration failed: %s",
                    redact_lark_text(exc),
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass


class _LarkMediaLeaseLost(LarkError):
    pass


class _LarkMediaRetryableError(LarkError):
    pass


class _LarkMediaUnknownDelivery(LarkError):
    pass


@dataclass(slots=True)
class _LarkCanonicalBundleOwnership:
    claimed_rows: list[Any]
    outbox_id: str
    claim_token: str
    active_media_ids: set[str]
    membership_lock: asyncio.Lock
    lost: asyncio.Event
    heartbeat: asyncio.Task[None] | None = None


class LarkMediaDeliveryWorker:
    """Deliver exact-account media bundles under their canonical outbox lease.

    SQLite returns one flattened list containing every sibling for each
    claimed slotless parent.  This worker groups that list back into parent
    bundles, validates the complete immutable destination before making a
    callback, and advances each child upload/send checkpoint under the same
    parent token.  The older independent-media-row path remains supported for
    compatibility tests and stores that do not expose canonical parents.
    """

    _CLAIM_STATES = (
        "ready",
        "upload_pending",
        "uploading",
        "uploaded",
        "send_pending",
    )
    _CHILD_STATES = frozenset((*_CLAIM_STATES, "sent", "failed"))

    def __init__(
        self,
        store: Any,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        *,
        uploader: Callable[[Any], Any] | None = None,
        sender: Callable[[Any, Any], Any] | None = None,
        text_sender: Callable[[Any], Any] | None = None,
        worker_id: str | None = None,
        claim_limit: int = 3,
        poll_interval: float = 1.0,
        lease_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.profile = LarkBotProfile.from_value(profile)
        self.uploader = uploader
        self.sender = sender
        self.text_sender = text_sender
        suffix = hashlib.sha256(self.profile.app_id.encode()).hexdigest()[:12]
        self.worker_id = worker_id or f"lark-media:{suffix}"
        self.claim_limit = max(1, int(claim_limit))
        self.poll_interval = max(0.05, float(poll_interval))
        self.lease_seconds = max(0.1, float(lease_seconds))
        self._stop = asyncio.Event()
        self._backoff_until = 0.0

    def stop(self) -> None:
        self._stop.set()

    @staticmethod
    def _state(row: Any) -> str:
        raw = _field(row, "state", default="")
        return str(getattr(raw, "value", raw) or "")

    @staticmethod
    def _bundle_order(row: Any) -> tuple[int, str]:
        metadata = _field(row, "metadata", default={})
        try:
            ordinal = int(_field(metadata, "bundle_ordinal", default=1 << 30))
        except (TypeError, ValueError):
            ordinal = 1 << 30
        return ordinal, str(_field(row, "media_id", default="") or "")

    @staticmethod
    def _uploaded_checkpoint(row: Any) -> Mapping[str, Any] | None:
        remote_id = str(_field(row, "remote_id", default="") or "").strip()
        if not remote_id:
            return None
        metadata = _field(row, "metadata", default={})
        kind = str(_field(metadata, "kind", default="") or "")
        return {"remote_id": remote_id, "kind": kind}

    async def _while_owned(
        self,
        value: Any,
        lost: asyncio.Event,
        *,
        name: str,
    ) -> tuple[bool, Any]:
        operation = asyncio.create_task(_maybe_await(value), name=name)
        watcher = asyncio.create_task(lost.wait(), name=f"{name}:lease")
        done, _ = await asyncio.wait(
            (operation, watcher), return_when=asyncio.FIRST_COMPLETED
        )
        if watcher in done and lost.is_set() and not operation.done():
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            return False, None
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        return not lost.is_set(), await operation

    async def _legacy_heartbeat(
        self,
        media_id: str,
        claim_token: str,
        lost: asyncio.Event,
        *,
        initial_deadline: float,
    ) -> None:
        interval = max(0.01, self.lease_seconds / 3.0)
        confirmed_until = float(initial_deadline)
        while not lost.is_set():
            remaining = confirmed_until - asyncio.get_running_loop().time()
            if remaining <= 0:
                lost.set()
                return
            await asyncio.sleep(min(interval, remaining))
            if asyncio.get_running_loop().time() >= confirmed_until:
                lost.set()
                return
            try:
                renewed = await _call_first(
                    self.store,
                    ("renew_outgoing_media_lease", "renew_media_lease"),
                    media_id,
                    claim_token,
                    lease_seconds=self.lease_seconds,
                )
            except Exception:
                lost.set()
                return
            if renewed is False:
                lost.set()
                return
            confirmed_until = asyncio.get_running_loop().time() + self.lease_seconds

    async def _canonical_heartbeat(
        self,
        outbox_id: str,
        claim_token: str,
        active_media_ids: set[str],
        membership_lock: asyncio.Lock,
        lost: asyncio.Event,
        *,
        initial_deadline: float,
    ) -> None:
        interval = max(0.01, self.lease_seconds / 3.0)
        confirmed_until = float(initial_deadline)
        while not lost.is_set():
            remaining = confirmed_until - asyncio.get_running_loop().time()
            if remaining <= 0:
                lost.set()
                return
            await asyncio.sleep(min(interval, remaining))
            if asyncio.get_running_loop().time() >= confirmed_until:
                lost.set()
                return
            try:
                async with membership_lock:
                    if lost.is_set():
                        return
                    media_ids = tuple(sorted(active_media_ids))
                    if media_ids:
                        renewed = await _call_first(
                            self.store,
                            ("renew_canonical_media_lease",),
                            outbox_id,
                            claim_token,
                            media_ids=media_ids,
                            lease_seconds=self.lease_seconds,
                        )
                    else:
                        # Once every child has a sent checkpoint, only the
                        # parent remains live while an optional text message
                        # is delivered and the logical bundle is finalized.
                        renewed = await _call_first(
                            self.store,
                            ("renew_outbox_lease",),
                            outbox_id,
                            claim_token,
                            lease_seconds=self.lease_seconds,
                        )
            except Exception:
                lost.set()
                return
            if renewed is False:
                lost.set()
                return
            confirmed_until = asyncio.get_running_loop().time() + self.lease_seconds

    @staticmethod
    async def _stop_heartbeat(
        heartbeat: asyncio.Task[None] | None,
    ) -> None:
        if heartbeat is None:
            return
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)

    async def _legacy_transition(
        self,
        media_id: str,
        state: str,
        *,
        claim_token: str,
        **kwargs: Any,
    ) -> bool:
        changed = await _call_first(
            self.store,
            ("transition_outgoing_media",),
            media_id,
            state,
            claim_token=claim_token or None,
            **kwargs,
        )
        return changed is not False

    async def _legacy_retry_failure(
        self,
        media_id: str,
        claim_token: str,
        exc: BaseException,
        *,
        retryable: bool,
    ) -> None:
        changed = await self._legacy_transition(
            media_id,
            "failed",
            claim_token=claim_token,
            error=redact_lark_text(exc),
        )
        if changed and retryable:
            with contextlib.suppress(AttributeError):
                await _call_first(
                    self.store,
                    ("retry_outgoing_media", "retry_media_delivery"),
                    media_id,
                )

    async def _process_legacy_row(
        self,
        row: Any,
        *,
        claim_started: float,
    ) -> int:
        media_id = str(_field(row, "media_id", default="") or "")
        token = str(_field(row, "claim_token", default="") or "")
        if (
            not media_id
            or str(_field(row, "channel", default="") or "") != CHANNEL
            or str(_field(row, "bot_id", default="") or "")
            != self.profile.app_id
        ):
            return 0
        lost = asyncio.Event()
        heartbeat = (
            asyncio.create_task(
                self._legacy_heartbeat(
                    media_id,
                    token,
                    lost,
                    initial_deadline=claim_started + self.lease_seconds,
                ),
                name=f"lark-media-lease:{media_id}",
            )
            if token
            and (
                hasattr(self.store, "renew_outgoing_media_lease")
                or hasattr(self.store, "renew_media_lease")
            )
            else None
        )
        try:
            remaining_backoff = (
                self._backoff_until - asyncio.get_running_loop().time()
            )
            if remaining_backoff > 0:
                await self._legacy_retry_failure(
                    media_id,
                    token,
                    LarkRateLimitError(retry_after=remaining_backoff),
                    retryable=True,
                )
                return 0
            state = self._state(row)
            uploaded: Any = self._uploaded_checkpoint(row)
            if state == "uploading":
                if uploaded is None:
                    if self.uploader is None:
                        raise LarkPermanentDeliveryError(
                            "Lark media uploader is unavailable"
                        )
                    owned, uploaded = await self._while_owned(
                        self.uploader(row),
                        lost,
                        name=f"lark-media-upload:{media_id}",
                    )
                    if not owned:
                        return 0
                remote_id = str(
                    _field(
                        uploaded,
                        "remote_id",
                        "file_key",
                        "image_key",
                        default="",
                    )
                    or ""
                )
                if not remote_id:
                    raise LarkProtocolError(
                        "Lark media upload omitted its resource key"
                    )
                changed = await self._legacy_transition(
                    media_id,
                    "uploaded",
                    claim_token=token,
                    from_states=("uploading",),
                    remote_id=remote_id,
                    metadata={
                        "channel": CHANNEL,
                        "kind": str(_field(uploaded, "kind", default="") or ""),
                    },
                )
                if not changed:
                    lost.set()
                    return 0
                state = "uploaded"
            if state == "uploaded":
                uploaded = uploaded or self._uploaded_checkpoint(row)
                changed = await self._legacy_transition(
                    media_id,
                    "send_pending",
                    claim_token=token,
                    from_states=("uploaded",),
                )
                if not changed:
                    lost.set()
                    return 0
                state = "send_pending"
            if state != "send_pending":
                raise LarkProtocolError(
                    f"unsupported Lark media checkpoint state: {state}"
                )
            uploaded = uploaded or self._uploaded_checkpoint(row)
            if uploaded is None:
                raise LarkProtocolError(
                    "Lark send checkpoint omitted its uploaded resource"
                )
            if self.sender is None:
                raise LarkPermanentDeliveryError("Lark media sender is unavailable")
            owned, response = await self._while_owned(
                self.sender(_media_callback_record(row), uploaded),
                lost,
                name=f"lark-media-send:{media_id}",
            )
            if not owned:
                return 0
            if not _response_message_id(response):
                raise LarkProtocolError(
                    "Lark media acknowledgement omitted a valid message ID"
                )
            changed = await self._legacy_transition(
                media_id,
                "sent",
                claim_token=token,
                from_states=("send_pending",),
            )
            return 1 if changed else 0
        except LarkRateLimitError as exc:
            self._backoff_until = (
                asyncio.get_running_loop().time() + exc.retry_after
            )
            if not lost.is_set():
                with contextlib.suppress(Exception):
                    await self._legacy_retry_failure(
                        media_id, token, exc, retryable=True
                    )
        except Exception as exc:
            logger.warning(
                "Lark media operation failed for %s: %s",
                media_id,
                redact_lark_text(exc),
            )
            if not lost.is_set():
                with contextlib.suppress(Exception):
                    await self._legacy_retry_failure(
                        media_id,
                        token,
                        exc,
                        retryable=not isinstance(exc, LarkPermanentDeliveryError),
                    )
        finally:
            lost.set()
            await self._stop_heartbeat(heartbeat)
        return 0

    async def _refresh_canonical_bundle(
        self,
        rows: Sequence[Any],
        *,
        outbox_id: str,
        claim_token: str,
    ) -> tuple[list[Any], Any | None]:
        method = getattr(self.store, "list_outgoing_media_for_outbox", None)
        if method is None:
            fresh_rows = sorted(rows, key=self._bundle_order)
        else:
            fresh = await _maybe_await(
                method(
                    outbox_id=outbox_id,
                    outbox_claim_token=claim_token,
                    limit=max(1000, len(rows) + 1),
                )
            )
            fresh_rows = sorted(list(fresh or ()), key=self._bundle_order)
        if not fresh_rows:
            raise _LarkMediaLeaseLost(
                "canonical Lark media bundle is no longer owned"
            )
        get_parent = getattr(self.store, "get_outbox_item", None)
        parent = (
            await _maybe_await(get_parent(outbox_id))
            if get_parent is not None
            else None
        )
        if get_parent is not None:
            parent_state = self._state(parent) if parent is not None else ""
            if (
                parent is None
                or str(_field(parent, "claim_token", default="") or "")
                != claim_token
                or parent_state not in {"claimed", "sending"}
            ):
                raise _LarkMediaLeaseLost(
                    "canonical Lark media parent is no longer owned"
                )
        return fresh_rows, parent

    @staticmethod
    def _parent_attachment_id(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        return str(_field(value, "attachment_id", "id", default="") or "").strip()

    def _validate_canonical_bundle(
        self,
        rows: Sequence[Any],
        *,
        claimed_rows: Sequence[Any],
        parent: Any | None,
        outbox_id: str,
        claim_token: str,
    ) -> None:
        if not rows:
            raise LarkPermanentDeliveryError("Lark media bundle is empty")
        claimed_ids = [
            str(_field(row, "media_id", default="") or "")
            for row in claimed_rows
        ]
        fresh_ids = [
            str(_field(row, "media_id", default="") or "") for row in rows
        ]
        if (
            len(claimed_ids) != len(set(claimed_ids))
            or len(fresh_ids) != len(set(fresh_ids))
            or set(claimed_ids) != set(fresh_ids)
        ):
            raise LarkPermanentDeliveryError(
                "canonical Lark media bundle membership changed"
            )
        fresh_by_id = {
            str(_field(row, "media_id", default="") or ""): row for row in rows
        }
        for claimed in claimed_rows:
            media_id = str(_field(claimed, "media_id", default="") or "")
            fresh = fresh_by_id[media_id]
            if (
                str(_field(claimed, "channel", default="") or "") != CHANNEL
                or str(_field(claimed, "bot_id", default="") or "")
                != self.profile.app_id
                or str(_field(claimed, "outbox_id", default="") or "")
                != outbox_id
                or str(
                    _field(claimed, "outbox_claim_token", default="") or ""
                )
                != claim_token
                or str(_field(claimed, "attachment_id", default="") or "")
                != str(_field(fresh, "attachment_id", default="") or "")
                or str(_field(claimed, "external_user_id", default="") or "")
                != str(_field(fresh, "external_user_id", default="") or "")
                or str(_field(claimed, "content", default="") or "")
                != str(_field(fresh, "content", default="") or "")
                or _record_target(claimed).stable_key()
                != _record_target(fresh).stable_key()
            ):
                raise LarkPermanentDeliveryError(
                    "claimed Lark media child conflicts with its durable bundle"
                )
        first = rows[0]
        expected_target = _record_target(first)
        _validate_lark_reply_target(
            expected_target,
            bot_id=self.profile.app_id,
            external_user_id=str(
                _field(first, "external_user_id", default="") or ""
            ),
        )
        expected_target_key = expected_target.stable_key()
        expected_content = str(_field(first, "content", default="") or "")
        expected_client_id = str(_field(first, "client_id", default="") or "")
        if parent is None:
            raise LarkPermanentDeliveryError(
                "canonical Lark media bundle has no durable parent snapshot"
            )
        parent_state = self._state(parent)
        parent_target = _record_target(parent)
        parent_actor = str(_field(parent, "external_user_id", default="") or "")
        if (
            str(_field(parent, "outbox_id", default="") or "") != outbox_id
            or str(_field(parent, "claim_token", default="") or "") != claim_token
            or parent_state not in {"claimed", "sending"}
            or str(_field(parent, "channel", default="") or "") != CHANNEL
            or str(_field(parent, "bot_id", default="") or "")
            != self.profile.app_id
            or parent_actor
            != str(_field(first, "external_user_id", default="") or "")
            or str(_field(parent, "content", default="") or "")
            != expected_content
            or str(_field(parent, "client_id", default="") or "")
            != expected_client_id
            or parent_target.stable_key() != expected_target_key
        ):
            raise LarkPermanentDeliveryError(
                "Lark media parent conflicts with its claimed child overlay"
            )
        _validate_lark_reply_target(
            parent_target,
            bot_id=self.profile.app_id,
            external_user_id=parent_actor,
        )
        ordinals: set[int] = set()
        unfinished = False
        for row in rows:
            media_id = str(_field(row, "media_id", default="") or "")
            if not media_id or not str(
                _field(row, "attachment_id", default="") or ""
            ):
                raise LarkPermanentDeliveryError(
                    "Lark media bundle contains an incomplete child"
                )
            if (
                str(_field(row, "channel", default="") or "") != CHANNEL
                or str(_field(row, "bot_id", default="") or "")
                != self.profile.app_id
                or str(_field(row, "outbox_id", default="") or "") != outbox_id
                or str(_field(row, "outbox_claim_token", default="") or "")
                != claim_token
            ):
                raise LarkPermanentDeliveryError(
                    "Lark media bundle account or parent identity changed"
                )
            target = _record_target(row)
            _validate_lark_reply_target(
                target,
                bot_id=self.profile.app_id,
                external_user_id=str(
                    _field(row, "external_user_id", default="") or ""
                ),
            )
            if target.stable_key() != expected_target_key:
                raise LarkPermanentDeliveryError(
                    "Lark media siblings have different reply targets"
                )
            if (
                str(_field(row, "content", default="") or "") != expected_content
                or str(_field(row, "client_id", default="") or "")
                != expected_client_id
            ):
                raise LarkPermanentDeliveryError(
                    "Lark media siblings have different parent payloads"
                )
            state = self._state(row)
            if state not in self._CHILD_STATES:
                raise LarkPermanentDeliveryError(
                    f"unsupported Lark media checkpoint state: {state}"
                )
            if state == "failed":
                raise LarkPermanentDeliveryError(
                    "Lark media bundle contains a failed attachment"
                )
            unfinished = unfinished or state != "sent"
            if state in {"uploaded", "send_pending"} and not self._uploaded_checkpoint(
                row
            ):
                raise LarkPermanentDeliveryError(
                    "Lark send checkpoint omitted its uploaded resource"
                )
            metadata = _field(row, "metadata", default={})
            try:
                ordinal = int(_field(metadata, "bundle_ordinal", default=-1))
            except (TypeError, ValueError) as exc:
                raise LarkPermanentDeliveryError(
                    "Lark media bundle has an invalid attachment order"
                ) from exc
            if ordinal < 0 or ordinal in ordinals:
                raise LarkPermanentDeliveryError(
                    "Lark media bundle has an invalid attachment order"
                )
            ordinals.add(ordinal)
            if len(lark_media_idempotency_key(row)) > 50:
                raise LarkPermanentDeliveryError(
                    "Lark media idempotency key is invalid"
                )
        if ordinals != set(range(len(rows))):
            raise LarkPermanentDeliveryError(
                "Lark media bundle attachment order is incomplete"
            )
        parent_attachments = tuple(
            self._parent_attachment_id(value)
            for value in (_field(parent, "attachments", default=()) or ())
        )
        child_attachments = tuple(
            str(_field(row, "attachment_id", default="") or "") for row in rows
        )
        if (
            not parent_attachments
            or any(not value for value in parent_attachments)
            or parent_attachments != child_attachments
        ):
            raise LarkPermanentDeliveryError(
                "Lark media children do not match their parent attachments"
            )
        if unfinished and self.sender is None:
            raise LarkPermanentDeliveryError("Lark media sender is unavailable")
        if any(self._state(row) == "uploading" for row in rows) and self.uploader is None:
            raise LarkPermanentDeliveryError("Lark media uploader is unavailable")
        if expected_content and self.text_sender is None:
            raise LarkPermanentDeliveryError("Lark bundle text sender is unavailable")
        if len(lark_bundle_text_idempotency_key(first)) > 50:
            raise LarkPermanentDeliveryError(
                "Lark bundle text idempotency key is invalid"
            )

    async def _canonical_transition(
        self,
        media_id: str,
        state: str,
        *,
        outbox_id: str,
        claim_token: str,
        active_media_ids: set[str],
        membership_lock: asyncio.Lock,
        lost: asyncio.Event,
        **kwargs: Any,
    ) -> None:
        async with membership_lock:
            if lost.is_set():
                raise _LarkMediaLeaseLost("canonical Lark media lease was lost")
            changed = await _call_first(
                self.store,
                ("transition_outgoing_media",),
                media_id,
                state,
                outbox_id=outbox_id,
                outbox_claim_token=claim_token,
                **kwargs,
            )
            if changed is False:
                lost.set()
                raise _LarkMediaLeaseLost(
                    "canonical Lark media transition lost its lease"
                )
            if state in {"sent", "failed"}:
                active_media_ids.discard(media_id)

    async def _finish_canonical_bundle(
        self,
        *,
        outbox_id: str,
        claim_token: str,
        row: Any,
        outcome: str,
        lost: asyncio.Event,
        membership_lock: asyncio.Lock,
        heartbeat: asyncio.Task[None] | None,
        remote_delivery_id: str = "",
        error: str = "",
        retry_after: float | None = None,
    ) -> bool:
        kwargs: dict[str, Any] = {
            "outcome": outcome,
            "error": error,
            "last_error": error,
        }
        client_id = str(_field(row, "client_id", default="") or "")
        if client_id:
            kwargs["client_id"] = client_id
        if remote_delivery_id:
            kwargs["remote_delivery_id"] = remote_delivery_id
            kwargs["transport_receipt"] = {
                "message_id": remote_delivery_id,
            }
        if retry_after is not None:
            kwargs["retry_after"] = max(0.0, float(retry_after))
        try:
            async with membership_lock:
                if lost.is_set():
                    return False
                changed = await _call_first(
                    self.store,
                    ("finish_outbox_attempt",),
                    outbox_id,
                    claim_token=claim_token,
                    **kwargs,
                )
                if changed is False:
                    lost.set()
                    return False
                return True
        finally:
            # Keep renewal live until the fenced parent commit returns.
            await self._stop_heartbeat(heartbeat)

    def _start_canonical_ownership(
        self,
        claimed_rows: Sequence[Any],
        *,
        claim_started: float,
    ) -> _LarkCanonicalBundleOwnership | None:
        first = claimed_rows[0]
        outbox_id = str(_field(first, "outbox_id", default="") or "")
        claim_token = str(
            _field(first, "outbox_claim_token", default="") or ""
        )
        if not outbox_id or not claim_token:
            return None
        active_media_ids = {
            str(_field(row, "media_id", default="") or "")
            for row in claimed_rows
            if self._state(row) not in {"sent", "failed"}
        }
        ownership = _LarkCanonicalBundleOwnership(
            claimed_rows=list(claimed_rows),
            outbox_id=outbox_id,
            claim_token=claim_token,
            active_media_ids=active_media_ids,
            membership_lock=asyncio.Lock(),
            lost=asyncio.Event(),
        )
        if hasattr(self.store, "renew_canonical_media_lease"):
            ownership.heartbeat = asyncio.create_task(
                self._canonical_heartbeat(
                    outbox_id,
                    claim_token,
                    ownership.active_media_ids,
                    ownership.membership_lock,
                    ownership.lost,
                    initial_deadline=claim_started + self.lease_seconds,
                ),
                name=f"lark-media-bundle-lease:{outbox_id}",
            )
        return ownership

    async def _process_canonical_bundle(
        self,
        ownership: _LarkCanonicalBundleOwnership,
    ) -> int:
        claimed_rows = ownership.claimed_rows
        outbox_id = ownership.outbox_id
        claim_token = ownership.claim_token
        try:
            rows, parent = await self._refresh_canonical_bundle(
                claimed_rows,
                outbox_id=outbox_id,
                claim_token=claim_token,
            )
        except _LarkMediaLeaseLost:
            return 0
        active_media_ids = ownership.active_media_ids
        membership_lock = ownership.membership_lock
        lost = ownership.lost
        heartbeat = ownership.heartbeat
        finished = False
        remote_delivery_id = ""
        try:
            started = await _call_first(
                self.store,
                ("mark_outbox_sending",),
                outbox_id,
                claim_token=claim_token,
            )
            if started is False or lost.is_set():
                lost.set()
                return 0
            self._validate_canonical_bundle(
                rows,
                claimed_rows=claimed_rows,
                parent=parent,
                outbox_id=outbox_id,
                claim_token=claim_token,
            )
            remaining_backoff = (
                self._backoff_until - asyncio.get_running_loop().time()
            )
            if remaining_backoff > 0:
                finished = await self._finish_canonical_bundle(
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    row=rows[0],
                    outcome="retryable_failure",
                    lost=lost,
                    membership_lock=membership_lock,
                    heartbeat=heartbeat,
                    error="Lark account is rate limited",
                    retry_after=remaining_backoff,
                )
                return 0
            for row in rows:
                state = self._state(row)
                if state == "sent":
                    continue
                media_id = str(_field(row, "media_id", default="") or "")
                uploaded: Any = self._uploaded_checkpoint(row)
                if state == "uploading":
                    if uploaded is None:
                        try:
                            owned, uploaded = await self._while_owned(
                                self.uploader(row),  # type: ignore[misc]
                                lost,
                                name=f"lark-media-upload:{media_id}",
                            )
                        except (LarkPermanentDeliveryError, LarkRateLimitError):
                            raise
                        except Exception as exc:
                            raise _LarkMediaRetryableError(
                                redact_lark_text(exc)
                            ) from None
                        if not owned:
                            raise _LarkMediaLeaseLost(
                                "canonical Lark media lease was lost during upload"
                            )
                    remote_id = str(
                        _field(
                            uploaded,
                            "remote_id",
                            "file_key",
                            "image_key",
                            default="",
                        )
                        or ""
                    )
                    if not remote_id:
                        raise _LarkMediaRetryableError(
                            "Lark media upload omitted its resource key"
                        )
                    await self._canonical_transition(
                        media_id,
                        "uploaded",
                        outbox_id=outbox_id,
                        claim_token=claim_token,
                        active_media_ids=active_media_ids,
                        membership_lock=membership_lock,
                        lost=lost,
                        from_states=("uploading",),
                        remote_id=remote_id,
                        metadata={
                            "channel": CHANNEL,
                            "kind": str(
                                _field(uploaded, "kind", default="") or ""
                            ),
                        },
                    )
                    state = "uploaded"
                if state == "uploaded":
                    uploaded = uploaded or self._uploaded_checkpoint(row)
                    await self._canonical_transition(
                        media_id,
                        "send_pending",
                        outbox_id=outbox_id,
                        claim_token=claim_token,
                        active_media_ids=active_media_ids,
                        membership_lock=membership_lock,
                        lost=lost,
                        from_states=("uploaded",),
                    )
                    state = "send_pending"
                if state != "send_pending" or uploaded is None:
                    raise LarkPermanentDeliveryError(
                        f"unsupported Lark media checkpoint state: {state}"
                    )
                try:
                    owned, response = await self._while_owned(
                        self.sender(_media_callback_record(row), uploaded),  # type: ignore[misc]
                        lost,
                        name=f"lark-media-send:{media_id}",
                    )
                except (LarkPermanentDeliveryError, LarkRateLimitError):
                    raise
                except Exception as exc:
                    raise _LarkMediaUnknownDelivery(
                        redact_lark_text(exc)
                    ) from None
                if not owned:
                    raise _LarkMediaLeaseLost(
                        "canonical Lark media lease was lost during send"
                    )
                message_id = _response_message_id(response)
                if not message_id:
                    raise _LarkMediaUnknownDelivery(
                        "Lark media acknowledgement omitted a valid message ID"
                    )
                await self._canonical_transition(
                    media_id,
                    "sent",
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    active_media_ids=active_media_ids,
                    membership_lock=membership_lock,
                    lost=lost,
                    from_states=("send_pending",),
                )
                remote_delivery_id = message_id

            parent_row = rows[0]
            content = str(_field(parent_row, "content", default="") or "")
            if content:
                try:
                    owned, response = await self._while_owned(
                        self.text_sender(  # type: ignore[misc]
                            _media_callback_record(parent_row, text=True)
                        ),
                        lost,
                        name=f"lark-media-text:{outbox_id}",
                    )
                except (LarkPermanentDeliveryError, LarkRateLimitError):
                    raise
                except Exception as exc:
                    raise _LarkMediaUnknownDelivery(
                        redact_lark_text(exc)
                    ) from None
                if not owned:
                    raise _LarkMediaLeaseLost(
                        "canonical Lark media lease was lost during text send"
                    )
                message_id = _response_message_id(response)
                if not message_id:
                    raise _LarkMediaUnknownDelivery(
                        "Lark text acknowledgement omitted a valid message ID"
                    )
                remote_delivery_id = message_id

            finished = await self._finish_canonical_bundle(
                outbox_id=outbox_id,
                claim_token=claim_token,
                row=rows[0],
                outcome="sent",
                lost=lost,
                membership_lock=membership_lock,
                heartbeat=heartbeat,
                remote_delivery_id=remote_delivery_id,
            )
            return 1 if finished else 0
        except _LarkMediaLeaseLost:
            return 0
        except LarkRateLimitError as exc:
            self._backoff_until = (
                asyncio.get_running_loop().time() + exc.retry_after
            )
            if not lost.is_set():
                finished = await self._finish_canonical_bundle(
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    row=rows[0],
                    outcome="retryable_failure",
                    lost=lost,
                    membership_lock=membership_lock,
                    heartbeat=heartbeat,
                    error="Lark rate limit",
                    retry_after=exc.retry_after,
                )
        except _LarkMediaUnknownDelivery as exc:
            logger.warning(
                "Lark media bundle outcome is unknown for %s: %s",
                outbox_id,
                redact_lark_text(exc),
            )
            if not lost.is_set():
                finished = await self._finish_canonical_bundle(
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    row=rows[0],
                    outcome="unknown",
                    lost=lost,
                    membership_lock=membership_lock,
                    heartbeat=heartbeat,
                    error=redact_lark_text(exc),
                )
        except LarkPermanentDeliveryError as exc:
            logger.warning(
                "Lark media bundle permanently failed for %s: %s",
                outbox_id,
                redact_lark_text(exc),
            )
            if not lost.is_set():
                finished = await self._finish_canonical_bundle(
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    row=rows[0],
                    outcome="permanent_failure",
                    lost=lost,
                    membership_lock=membership_lock,
                    heartbeat=heartbeat,
                    error=redact_lark_text(exc),
                )
        except Exception as exc:
            logger.warning(
                "Lark media bundle failed for %s: %s",
                outbox_id,
                redact_lark_text(exc),
            )
            if not lost.is_set():
                attempts = max(
                    1,
                    int(_field(rows[0], "attempts", default=1) or 1),
                )
                finished = await self._finish_canonical_bundle(
                    outbox_id=outbox_id,
                    claim_token=claim_token,
                    row=rows[0],
                    outcome="retryable_failure",
                    lost=lost,
                    membership_lock=membership_lock,
                    heartbeat=heartbeat,
                    error=redact_lark_text(exc),
                    retry_after=min(300.0, 2.0 ** max(0, attempts - 1)),
                )
        finally:
            await self._stop_heartbeat(heartbeat)
        return 0

    async def run_once(self) -> int:
        if asyncio.get_running_loop().time() < self._backoff_until:
            return 0
        claim_started = asyncio.get_running_loop().time()
        rows = await _call_first(
            self.store,
            ("claim_account_outgoing_media", "claim_outgoing_media"),
            self.worker_id,
            channel=CHANNEL,
            bot_id=self.profile.app_id,
            limit=self.claim_limit,
            lease_seconds=self.lease_seconds,
            states=self._CLAIM_STATES,
        )
        if rows is None:
            return 0
        if isinstance(rows, Mapping) or not isinstance(rows, Sequence):
            rows = (rows,)
        canonical: dict[tuple[str, str], list[Any]] = {}
        legacy: list[Any] = []
        for row in rows:
            outbox_id = str(_field(row, "outbox_id", default="") or "")
            parent_token = str(
                _field(row, "outbox_claim_token", default="") or ""
            )
            if outbox_id or parent_token:
                if not outbox_id or not parent_token:
                    logger.warning(
                        "Ignoring malformed Lark media parent claim for %s",
                        str(_field(row, "media_id", default="") or ""),
                    )
                    continue
                canonical.setdefault((outbox_id, parent_token), []).append(row)
            else:
                legacy.append(row)
        ownerships = [
            ownership
            for bundle_rows in canonical.values()
            if (
                ownership := self._start_canonical_ownership(
                    bundle_rows,
                    claim_started=claim_started,
                )
            )
            is not None
        ]
        completed = 0
        try:
            for ownership in ownerships:
                completed += await self._process_canonical_bundle(ownership)
            for row in legacy:
                completed += await self._process_legacy_row(
                    row,
                    claim_started=claim_started,
                )
            return completed
        finally:
            await asyncio.gather(
                *(
                    self._stop_heartbeat(ownership.heartbeat)
                    for ownership in ownerships
                ),
                return_exceptions=True,
            )

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Lark media delivery iteration failed: %s",
                    redact_lark_text(exc),
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass


class LarkAccountSupervisor:
    """Restart/failure isolation for exactly one configured Lark app."""

    def __init__(
        self,
        profile: LarkBotProfile | Mapping[str, Any] | Any,
        gateway: LarkGateway,
        process: LarkCliProcess,
        *,
        delivery_worker: LarkDeliveryWorker | None = None,
        media_worker: LarkMediaDeliveryWorker | None = None,
        status_callback: Callable[[AccountState, int, str], Any] | None = None,
        stable_reset_after: float = 300.0,
        delivery_drain_timeout: float = 10.0,
    ) -> None:
        self.profile = LarkBotProfile.from_value(profile)
        self.gateway = gateway
        self.process = process
        self.delivery_worker = delivery_worker
        self.media_worker = media_worker
        self.status_callback = status_callback
        self.stable_reset_after = max(0.1, float(stable_reset_after))
        self.delivery_drain_timeout = max(0.05, float(delivery_drain_timeout))
        self.state = (
            AccountState.DISABLED
            if not self.profile.enabled
            else AccountState.DISCONNECTED
        )
        self.generation = 0
        self._stop = asyncio.Event()
        self._ready_once = asyncio.Event()
        self._readiness_terminal = asyncio.Event()
        if self.state is AccountState.DISABLED:
            self._readiness_terminal.set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._delivery_started = False

    async def _set_state(self, state: AccountState, error: str = "") -> None:
        self.state = state
        if self.status_callback is not None:
            await _maybe_await(self.status_callback(state, self.generation, error))
        if state is AccountState.READY:
            self._ready_once.set()
        elif state in {AccountState.DISABLED, AccountState.FAILED}:
            self._readiness_terminal.set()

    async def wait_until_ready(self, timeout: float | None = None) -> int:
        """Wait until this account first reaches READY and return its generation.

        FAILED, DISABLED, or an explicit stop before first readiness ends the
        wait deterministically.  A timeout uses ``asyncio.TimeoutError`` so a
        runtime profile registrar can apply its own rollback policy.
        """

        if self._ready_once.is_set():
            return self.generation
        if self._readiness_terminal.is_set():
            raise LarkError(
                f"Lark profile {self.profile.profile_id} did not become ready "
                f"({self.state.value})"
            )
        ready_waiter = asyncio.create_task(self._ready_once.wait())
        terminal_waiter = asyncio.create_task(self._readiness_terminal.wait())
        try:
            waiter = asyncio.wait(
                (ready_waiter, terminal_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if timeout is None:
                await waiter
            else:
                await asyncio.wait_for(waiter, timeout=max(0.0, float(timeout)))
        finally:
            for task in (ready_waiter, terminal_waiter):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                ready_waiter,
                terminal_waiter,
                return_exceptions=True,
            )
        if self._ready_once.is_set():
            return self.generation
        raise LarkError(
            f"Lark profile {self.profile.profile_id} did not become ready "
            f"({self.state.value})"
        )

    def stop(self) -> None:
        # ``run()`` owns the ordered shutdown: it fences ingress first, then
        # tells delivery loops to stop claiming while allowing their current
        # lease-fenced ``run_once`` calls a bounded drain window.
        self._stop.set()
        if not self._ready_once.is_set():
            self._readiness_terminal.set()

    async def _drain_delivery_workers(self) -> None:
        for worker in (self.delivery_worker, self.media_worker):
            if worker is not None:
                worker.stop()
        tasks = tuple(self._tasks)
        if not tasks:
            return
        _done, pending = await asyncio.wait(
            tasks,
            timeout=self.delivery_drain_timeout,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

    def _start_delivery_workers(self) -> None:
        if self._delivery_started:
            return
        self._delivery_started = True
        if self.delivery_worker is not None:
            self._tasks.add(asyncio.create_task(self.delivery_worker.run()))
        if self.media_worker is not None:
            self._tasks.add(asyncio.create_task(self.media_worker.run()))

    async def run(self) -> None:
        if not self.profile.enabled:
            await self._set_state(AccountState.DISABLED)
            return
        attempt = 0
        try:
            while not self._stop.is_set():
                await self._set_state(
                    AccountState.STARTING if attempt == 0 else AccountState.RESTARTING
                )
                try:
                    self.generation = await self.process.start()
                    await self._set_state(AccountState.READY)
                    self._start_delivery_workers()
                    ready_at = asyncio.get_running_loop().time()
                    events = self.process.events().__aiter__()
                    while not self._stop.is_set():
                        next_event = asyncio.create_task(anext(events))
                        stop_waiter = asyncio.create_task(self._stop.wait())
                        try:
                            done, _pending = await asyncio.wait(
                                (next_event, stop_waiter),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                        except asyncio.CancelledError:
                            next_event.cancel()
                            stop_waiter.cancel()
                            await asyncio.gather(
                                next_event, stop_waiter, return_exceptions=True
                            )
                            raise
                        if stop_waiter in done and self._stop.is_set():
                            next_event.cancel()
                            await asyncio.gather(next_event, return_exceptions=True)
                            closer = getattr(events, "aclose", None)
                            if callable(closer):
                                with contextlib.suppress(Exception):
                                    await closer()
                            # Break a consumer that has no incoming event with a
                            # deterministic ingress-first shutdown boundary.
                            await self.process.stop()
                            break
                        stop_waiter.cancel()
                        await asyncio.gather(stop_waiter, return_exceptions=True)
                        try:
                            event = next_event.result()
                        except StopAsyncIteration:
                            break
                        if event.generation != self.generation:
                            continue
                        try:
                            await self.gateway.accept_event(
                                event.payload,
                                generation=event.generation,
                                active_generation=self.generation,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            # One malformed/denied message cannot restart or
                            # poison the account stream.
                            logger.warning(
                                "Lark event rejected for profile %s: %s",
                                self.profile.profile_id,
                                redact_lark_text(exc),
                            )
                    if self._stop.is_set():
                        break
                    if (
                        asyncio.get_running_loop().time() - ready_at
                        >= self.stable_reset_after
                    ):
                        # Forgive an old failure only after an actually stable
                        # consumer lifetime.  Merely printing READY and exiting
                        # must keep consuming the bounded retry budget.
                        attempt = 0
                    raise LarkProtocolError("Lark consumer disconnected")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self.process.stop()
                    attempt += 1
                    await self._set_state(
                        AccountState.DISCONNECTED, redact_lark_text(exc)
                    )
                    if attempt > self.profile.restart_max_attempts:
                        await self._set_state(
                            AccountState.FAILED, redact_lark_text(exc)
                        )
                        return
                    delay = min(
                        self.profile.restart_max_delay,
                        self.profile.restart_base_delay * (2 ** min(attempt - 1, 10)),
                    )
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._stop.set()
            # Consumer/process fencing is the account-local ingress boundary.
            # It must complete before delivery loops are asked to stop
            # claiming additional work.
            await self.process.stop()
            await self._drain_delivery_workers()
            if self.state is not AccountState.FAILED:
                await self._set_state(AccountState.DISCONNECTED)
            if not self._ready_once.is_set():
                self._readiness_terminal.set()


# Compatibility/descriptive aliases used by tests and composition code.
LarkGatewayAdapter = LarkGateway
LarkOutboxWorker = LarkDeliveryWorker
normalize_event = normalize_lark_event


__all__ = [
    "AccountState",
    "CHANNEL",
    "CONFIG_ENVIRONMENT_KEY",
    "EVENT_KEY",
    "LARK_CAPABILITIES",
    "LARK_DELIVERY_POLICY",
    "LarkAccountSupervisor",
    "LarkAttachmentPromoter",
    "LarkBotProfile",
    "LarkCliProcess",
    "LarkCliVersionError",
    "LarkCommandRouter",
    "LarkDeliveryWorker",
    "LarkError",
    "LarkGateway",
    "LarkGatewayAdapter",
    "LarkMediaDeliveryWorker",
    "LarkOutboxWorker",
    "LarkPermanentDeliveryError",
    "LarkProcessEvent",
    "LarkProfileError",
    "LarkProtocolError",
    "LarkRateLimitError",
    "PINNED_LARK_CLI_VERSION",
    "normalize_event",
    "normalize_lark_event",
    "parse_lark_command",
    "parse_lark_cli_version",
    "redact_lark_text",
    "sanitize_lark_metadata",
    "tighten_lark_cli_config_state",
    "validate_private_config_directory",
]
