"""Owner-only terminal management for isolated Lark/Feishu bot profiles.

This module deliberately performs only local administration.  It provisions
an app with the pinned official ``lark-cli``, validates the resulting isolated
configuration, and records a non-secret credential reference through the
durable bot-profile store API.  It never runs ``lark-cli auth login``: the
runtime consumes events and sends messages as the configured bot identity.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import queue
import re
import select
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, TextIO, TypeVar

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.lark_contract import (  # noqa: E402
    LarkBotIdentityContractError,
    lark_event_bus_socket_app_id,
    parse_composite_verified_lark_bot_identity,
)
from src.runtime.models import (  # noqa: E402
    BotProfileRecord,
    BotProfileStatusRecord,
    PrincipalAccountRecord,
    PrincipalRecord,
    text_to_datetime,
)
from src.runtime.sqlite_store import SQLiteStore  # noqa: E402
from src.runtime.store import StoreError  # noqa: E402
from src.runtime.supervisor import (  # noqa: E402
    ChannelAccountOwnership,
    CredentialMutationOwnership,
    DatabaseOwnership,
    SupervisorAccountSetOwnership,
    SupervisorOwnershipConflict,
)


# This is the release whose config-init output and config.json contract are
# exercised by the fixtures in this repository.  Upgrades must change the pin
# and the fixtures together; accepting an arbitrary newer binary would make QR
# parsing and secret cleanup an unreviewed protocol change.
PINNED_LARK_CLI_VERSION = "1.0.92"
LARK_CONFIG_ENV = "LARKSUITE_CLI_CONFIG_DIR"
DEFAULT_ONBOARD_TIMEOUT_SECONDS = 600.0
MAX_INIT_OUTPUT_BYTES = 1_048_576
MAX_APP_SECRET_BYTES = 16_384
# Migration 36 introduced the tables consumed by this owner CLI.  Keep that
# capability floor separate from the compatibility ceiling: later runtime
# migrations do not move the point at which the profile schema first exists.
LARK_PROFILE_SCHEMA_VERSION = 36
OWNER_CLI_MAX_RUNTIME_SCHEMA_VERSION = 41
_ONBOARDING_METADATA_FILE = ".cow-onboarding.json"
_OWNER_MAPPING_METADATA_FILE = ".cow-owner-principal.json"
_APP_OWNER_PATH = "/open-apis/application/v6/applications/me"
_APP_OWNER_PARAMS = '{"lang":"zh_cn","user_id_type":"open_id"}'

_PROFILE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_ADD_STAGING_NAME = re.compile(
    r"^\.add-(?P<profile>[A-Za-z0-9][A-Za-z0-9_-]{0,63})-"
    r"(?P<nonce>[a-z0-9_]{8})$"
)
_PRINCIPAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")
# Keep the owner/onboarding contract byte-for-byte aligned with the runtime
# adapter.  Accepting an identity here that LarkBotProfile later rejects would
# create a durable profile that can never start.
_APP_ID = re.compile(r"^cli_[A-Za-z0-9_-]{4,128}$")
_OPEN_ID = re.compile(r"^ou_[A-Za-z0-9_-]{4,256}$")
_SEMVER = re.compile(r"(?<![0-9])v?(\d+\.\d+\.\d+)(?![0-9])")
_ANSI_ESCAPE = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|\x9b[0-?]*[ -/]*[@-~]|\x1b[@-Z\\-_])"
)
_SECRET_JSON = re.compile(
    r'(?i)("(?:app_?secret|access_?token|tenant_?access_?token|refresh_?token|password)"\s*:\s*)'
    r'("(?:\\.|[^"\\])*"|[^,}\s]+)'
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(\b(?:app_?secret|access_?token|tenant_?access_?token|refresh_?token|password)\b\s*[=:]\s*)"
    r"([^\s,;]+)"
)
_INIT_JSON_OBJECT_START = re.compile(r"(?m)^[ \t]*\{")
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
_PRIVATE_SUBPROCESS_OPTIONS: dict[str, Any] = (
    {"umask": 0o077} if os.name == "posix" else {}
)

T = TypeVar("T")


class LarkProfileError(RuntimeError):
    """A safe, operator-facing profile-management failure."""


class LarkCliVersionError(LarkProfileError):
    """The installed external CLI does not match the reviewed release."""


class LarkProfileStoreUnavailable(LarkProfileError):
    """The runtime store does not expose the required profile API."""


@dataclass(frozen=True, slots=True)
class ProvisionedConfig:
    app_id: str
    brand: str
    credential_ref: str
    bot_open_id: str = ""
    owner_open_id: str = ""


@dataclass(frozen=True, slots=True)
class _OnboardingMetadata:
    credential_origin: str
    app_id: str
    brand: str


@dataclass(frozen=True, slots=True)
class _OwnerMappingMetadata:
    principal_id: str
    open_id: str


class _StagedCredentialDisposition(Enum):
    """Whether one staged credential can be removed after a failed operation."""

    UNKNOWN = "unknown"
    SHARED = "shared"
    CLEANUP_ALLOWED = "cleanup_allowed"


def _public_text(value: Any, *, maximum: int = 1_024) -> str:
    """Bound one-line diagnostics and redact common credential spellings."""

    text = _ANSI_ESCAPE.sub("", str(value or ""))
    text = _SECRET_JSON.sub(lambda match: match.group(1) + '"<redacted>"', text)
    text = _SECRET_ASSIGNMENT.sub(
        lambda match: match.group(1) + "<redacted>", text
    )
    text = " ".join(text.split())
    if len(text) <= maximum:
        return text
    return text[: max(0, maximum - 3)].rstrip() + "..."


def _relay_text(value: str) -> str:
    """Redact secrets while preserving QR layout and verification URLs."""

    text = _SECRET_JSON.sub(lambda match: match.group(1) + '"<redacted>"', value)
    return _SECRET_ASSIGNMENT.sub(
        lambda match: match.group(1) + "<redacted>", text
    )


def _redact_exact(value: str, sensitive_values: Sequence[str]) -> str:
    """Remove exact caller-supplied secrets before retaining child output."""

    redacted = value
    for sensitive in sensitive_values:
        if sensitive:
            redacted = redacted.replace(sensitive, "<redacted>")
    return _relay_text(redacted)


def _read_app_secret(stream: TextIO, *, timeout: float) -> str:
    """Read one bounded App Secret from a non-interactive stdin stream."""

    try:
        if stream.isatty():
            raise LarkProfileError(
                "--app-secret-stdin requires piped standard input; run the "
                "existing-app command through ./cow for a hidden prompt"
            )
    except (AttributeError, OSError, ValueError):
        pass
    value: str
    try:
        descriptor = stream.fileno()
    except (AttributeError, OSError, ValueError):
        descriptor = -1
    try:
        if descriptor >= 0 and os.name == "posix":
            deadline = time.monotonic() + timeout
            payload = bytearray()
            while b"\n" not in payload and len(payload) <= MAX_APP_SECRET_BYTES + 1:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LarkProfileError(
                        "timed out while reading App Secret from standard input"
                    )
                readable, _writable, _exceptional = select.select(
                    [descriptor], [], [], remaining
                )
                if not readable:
                    raise LarkProfileError(
                        "timed out while reading App Secret from standard input"
                    )
                chunk = os.read(
                    descriptor,
                    max(1, MAX_APP_SECRET_BYTES + 2 - len(payload)),
                )
                if not chunk:
                    break
                payload.extend(chunk)
            value = bytes(payload).decode("utf-8")
        else:
            value = stream.read(MAX_APP_SECRET_BYTES + 2)
    except LarkProfileError:
        raise
    except (OSError, UnicodeError) as exc:
        raise LarkProfileError("could not read App Secret from standard input") from exc
    if not isinstance(value, str):
        raise LarkProfileError("App Secret standard input must be text")
    if len(value) > MAX_APP_SECRET_BYTES + 1:
        raise LarkProfileError("App Secret standard input exceeded its size limit")
    if value.endswith("\n"):
        value = value[:-1]
        if value.endswith("\r"):
            value = value[:-1]
    if not value:
        raise LarkProfileError("App Secret standard input is empty")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise LarkProfileError("App Secret standard input must contain exactly one line")
    if len(value.encode("utf-8")) > MAX_APP_SECRET_BYTES:
        raise LarkProfileError("App Secret standard input exceeded its size limit")
    # The reviewed CLI applies strings.TrimSpace before secure storage. Match
    # that contract so the exact redaction value is also the value it receives.
    normalized = value.strip()
    if not normalized:
        raise LarkProfileError("App Secret standard input is empty")
    return normalized


def _normalize_add_credentials(
    app_id: str | None,
    brand: str | None,
    app_secret_stdin: bool,
) -> tuple[str, str] | None:
    """Validate the optional existing-app mode without accepting a secret value."""

    if app_id is None:
        if brand is not None or app_secret_stdin:
            raise LarkProfileError(
                "--brand and --app-secret-stdin require --app-id"
            )
        return None
    normalized_app_id = str(app_id).strip()
    if not _APP_ID.fullmatch(normalized_app_id):
        raise LarkProfileError("--app-id is not a valid Lark/Feishu App ID")
    if not app_secret_stdin:
        raise LarkProfileError(
            "existing-app onboarding requires --app-secret-stdin"
        )
    normalized_brand = "feishu" if brand is None else str(brand).strip().lower()
    if normalized_brand not in {"feishu", "lark"}:
        raise LarkProfileError("--brand must be either feishu or lark")
    return normalized_app_id, normalized_brand


def normalize_profile_name(value: str | None, *, generate: bool = False) -> str:
    """Return one filesystem-safe local profile identifier."""

    name = str(value or "").strip()
    if not name and generate:
        name = f"lark-{uuid.uuid4().hex[:8]}"
    if not _PROFILE_NAME.fullmatch(name):
        raise LarkProfileError(
            "profile name must start with a letter or digit and contain only "
            "letters, digits, '_' or '-' (maximum 64 characters)"
        )
    return name


def normalize_principal_id(value: str) -> str:
    principal_id = str(value or "").strip()
    if not _PRINCIPAL_ID.fullmatch(principal_id):
        raise LarkProfileError(
            "principal ID must start with a letter or digit and contain only "
            "letters, digits, '.', '_', ':', '@', '/' or '-' (maximum 256 characters)"
        )
    return principal_id


def _normalize_owner_open_id(value: str | None) -> str | None:
    """Validate one explicit, app-scoped human identity for owner mapping."""

    if value is None:
        return None
    open_id = str(value).strip()
    if not _OPEN_ID.fullmatch(open_id):
        raise LarkProfileError(
            "--owner-open-id must be the owner's app-scoped ou_ open_id"
        )
    return open_id


def _local_owner_identity() -> str:
    return (
        f"local-owner:{os.getuid()}"
        if hasattr(os, "getuid")
        else "local-owner"
    )


def durable_database_path() -> Path:
    return Path(
        os.environ.get(
            "CODEX_WECHAT_DB",
            str(Path.home() / ".codex-wechat-bot" / "runtime.sqlite3"),
        )
    ).expanduser().resolve()


def lark_config_root() -> Path:
    configured = os.environ.get("CODEX_LARK_CONFIG_ROOT", "").strip()
    return (
        Path(configured).expanduser().absolute()
        if configured
        else (Path.home() / ".codex-wechat-bot" / "lark").absolute()
    )


def _onboard_timeout() -> float:
    raw = os.environ.get(
        "CODEX_LARK_ONBOARD_TIMEOUT", str(DEFAULT_ONBOARD_TIMEOUT_SECONDS)
    ).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise LarkProfileError(
            "CODEX_LARK_ONBOARD_TIMEOUT must be a positive finite number"
        ) from exc
    if not math.isfinite(value) or value <= 0:
        raise LarkProfileError(
            "CODEX_LARK_ONBOARD_TIMEOUT must be a positive finite number"
        )
    return value


def lark_onboarding_timeout() -> float:
    """Return the shared validated timeout for terminal and live QR flows."""

    return _onboard_timeout()


def _ensure_private_directory(path: Path, *, create: bool) -> Path:
    """Create or validate an owner-only, non-symlink directory."""

    if create and not path.exists():
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(path, 0o700)
    try:
        details = path.lstat()
    except OSError as exc:
        raise LarkProfileError(f"Lark config root is unavailable: {path}") from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise LarkProfileError("Lark config root must be a real directory")
    if hasattr(os, "getuid") and details.st_uid != os.getuid():
        raise LarkProfileError("Lark config root has a different owner")
    if stat.S_IMODE(details.st_mode) & 0o077:
        raise LarkProfileError("Lark config root must be owner-only (mode 0700)")
    return path


def _validate_private_tree(
    path: Path,
    *,
    allow_runtime_event_socket: bool = False,
) -> frozenset[str]:
    """Validate private CLI state and report any admitted event-socket apps.

    Onboarding and recovery staging retain the default special-file-strict
    policy.  Stable profiles may contain lark-cli's owner-private Unix socket
    at ``events/<app-id>/bus.sock``; callers must compare every reported app ID
    with the single app declared by the validated config document.
    """

    _ensure_private_directory(path, create=False)
    event_socket_apps: set[str] = set()

    def walk_error(error: OSError) -> None:
        raise error

    try:
        for root, directories, files in os.walk(
            path,
            topdown=True,
            onerror=walk_error,
            followlinks=False,
        ):
            root_path = Path(root)
            for name in (*directories, *files):
                candidate = root_path / name
                details = candidate.lstat()
                try:
                    relative_candidate = candidate.relative_to(path)
                except ValueError as exc:
                    raise LarkProfileError(
                        "lark-cli config state escaped its profile directory"
                    ) from exc
                event_socket_app = lark_event_bus_socket_app_id(
                    relative_candidate
                )
                if stat.S_ISLNK(details.st_mode):
                    raise LarkProfileError(
                        "lark-cli config state must not contain symlinks"
                    )
                if hasattr(os, "getuid") and details.st_uid != os.getuid():
                    raise LarkProfileError(
                        "lark-cli config state has a different owner"
                    )
                if stat.S_IMODE(details.st_mode) & 0o077:
                    raise LarkProfileError(
                        "lark-cli config state must be owner-only"
                    )
                if event_socket_app:
                    # The reviewed path is reserved for the Unix socket.  A
                    # regular file, directory, FIFO, or device at that name is
                    # an impostor and must not pass the generic file checks.
                    if (
                        not allow_runtime_event_socket
                        or name in directories
                        or not stat.S_ISSOCK(details.st_mode)
                        or details.st_nlink != 1
                    ):
                        raise LarkProfileError(
                            "lark-cli event bus socket has an invalid type"
                        )
                    event_socket_apps.add(event_socket_app)
                    continue
                if name in directories and not stat.S_ISDIR(details.st_mode):
                    raise LarkProfileError(
                        "lark-cli config state contains an invalid directory"
                    )
                if name in files and not stat.S_ISREG(details.st_mode):
                    raise LarkProfileError(
                        "lark-cli config state contains a special file"
                    )
                if name in files and details.st_nlink != 1:
                    raise LarkProfileError(
                        "lark-cli config state contains a hardlinked file"
                    )
    except LarkProfileError:
        raise
    except OSError as exc:
        raise LarkProfileError("lark-cli config state is unavailable") from exc
    return frozenset(event_socket_apps)


def _tighten_owned_staging_tree(path: Path) -> None:
    """Normalize an owner-created CLI tree to private modes.

    The wrapper creates the direct-child staging directory under an owner-only
    root before invoking the pinned CLI.  The CLI may honor a permissive shell
    umask for cache/log artifacts, so correct those mode bits while still
    rejecting links, special files, hardlinks, and foreign-owned entries.
    """

    _ensure_private_directory(path, create=False)
    directories_to_tighten: list[tuple[Path, os.stat_result]] = []
    files_to_tighten: list[tuple[Path, os.stat_result]] = []

    def walk_error(error: OSError) -> None:
        raise error

    try:
        for walked_root, directories, files in os.walk(
            path,
            topdown=True,
            onerror=walk_error,
            followlinks=False,
        ):
            root_path = Path(walked_root)
            for name in directories:
                candidate = root_path / name
                details = candidate.lstat()
                if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(
                    details.st_mode
                ):
                    raise LarkProfileError("unsafe staged credential directory")
                if hasattr(os, "getuid") and details.st_uid != os.getuid():
                    raise LarkProfileError(
                        "foreign-owned staged credential directory"
                    )
                directories_to_tighten.append((candidate, details))
            for name in files:
                candidate = root_path / name
                details = candidate.lstat()
                if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(
                    details.st_mode
                ):
                    raise LarkProfileError("unsafe staged credential file")
                if details.st_nlink != 1:
                    raise LarkProfileError("hardlinked staged credential file")
                if hasattr(os, "getuid") and details.st_uid != os.getuid():
                    raise LarkProfileError("foreign-owned staged credential file")
                files_to_tighten.append((candidate, details))
        for directory, details in directories_to_tighten:
            _chmod_exact_staging_entry(directory, details, 0o700)
        for file_path, details in files_to_tighten:
            _chmod_exact_staging_entry(file_path, details, 0o600)
    except LarkProfileError:
        raise
    except OSError as exc:
        raise LarkProfileError("could not secure staged credential state") from exc


def _chmod_exact_staging_entry(
    path: Path,
    expected: os.stat_result,
    mode: int,
) -> None:
    """Tighten one exact inode without following a replacement symlink.

    Linux commonly cannot implement ``chmod(..., follow_symlinks=False)`` and
    raises ``NotImplementedError`` even for a regular file.  Open the validated
    entry with ``O_NOFOLLOW`` instead, attest the resulting descriptor against
    the earlier ``lstat``, and apply the mode through ``fchmod``.  Descriptor
    identity also closes the path-swap interval between tree traversal and the
    permission change.
    """

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if os.name != "posix" or not nofollow or not hasattr(os, "fchmod"):
        raise LarkProfileError(
            "this platform cannot safely secure staged credential state"
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
            or (
                hasattr(os, "getuid")
                and opened.st_uid != os.getuid()
            )
            or (stat.S_ISREG(opened.st_mode) and opened.st_nlink != 1)
        ):
            raise LarkProfileError(
                "staged credential entry changed during permission tightening"
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
                "staged credential entry changed during permission tightening"
            )
    finally:
        os.close(descriptor)


def _contained_child(root: Path, name: str) -> Path:
    candidate = root / name
    if candidate.parent != root or candidate.name in {"", ".", ".."}:
        raise LarkProfileError("invalid Lark profile path")
    return candidate


def _safe_remove_tree(path: Path, root: Path) -> None:
    """Remove only one direct, non-symlink child of the validated state root."""

    if path.parent != root:
        raise LarkProfileError("refusing to remove a path outside the Lark config root")
    try:
        details = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise LarkProfileError("refusing to remove an unsafe Lark config path")
    shutil.rmtree(path)


def _event_socket_copy_ignore(
    source: Path,
    app_id: str,
) -> Callable[[str, list[str]], set[str]]:
    """Exclude only the validated, ephemeral bus socket from a tree copy."""

    if not _APP_ID.fullmatch(app_id):
        raise LarkProfileError("invalid app identity for event socket exclusion")
    socket_parent = source / "events" / app_id

    def ignore(directory: str, names: list[str]) -> set[str]:
        if Path(directory) == socket_parent and "bus.sock" in names:
            return {"bus.sock"}
        return set()

    return ignore


def _write_existing_app_metadata(path: Path, app_id: str, brand: str) -> None:
    """Persist the expected imported identity before lark-cli can mutate keys."""

    metadata_path = path / _ONBOARDING_METADATA_FILE
    payload = json.dumps(
        {
            "schemaVersion": 1,
            "credentialOrigin": "existing_app",
            "appId": app_id,
            "brand": brand,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(metadata_path, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short onboarding metadata write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, directory_flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _read_onboarding_metadata(path: Path) -> _OnboardingMetadata | None:
    """Read and strictly validate COW-owned, non-secret staging provenance."""

    metadata_path = path / _ONBOARDING_METADATA_FILE
    try:
        details = metadata_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LarkProfileError("onboarding metadata is unavailable") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or details.st_nlink != 1
        or stat.S_IMODE(details.st_mode) & 0o077
        or (hasattr(os, "getuid") and details.st_uid != os.getuid())
        or details.st_size > 4_096
    ):
        raise LarkProfileError("onboarding metadata is not a private regular file")
    try:
        value = json.loads(
            metadata_path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (_DuplicateJsonKey, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LarkProfileError("onboarding metadata is malformed") from exc
    expected_keys = {
        "schemaVersion",
        "credentialOrigin",
        "appId",
        "brand",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        raise LarkProfileError("onboarding metadata is malformed")
    app_id = value.get("appId")
    brand = value.get("brand")
    if (
        value.get("schemaVersion") != 1
        or value.get("credentialOrigin") != "existing_app"
        or not isinstance(app_id, str)
        or not _APP_ID.fullmatch(app_id)
        or not isinstance(brand, str)
        or brand not in {"feishu", "lark"}
    ):
        raise LarkProfileError("onboarding metadata is malformed")
    return _OnboardingMetadata("existing_app", app_id, brand)


def _write_owner_mapping_metadata(path: Path, open_id: str) -> None:
    """Persist explicit owner-mapping intent before credential mutation."""

    metadata_path = path / _OWNER_MAPPING_METADATA_FILE
    payload = json.dumps(
        {
            "schemaVersion": 1,
            "principalId": "owner",
            "openId": open_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(metadata_path, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short owner-mapping metadata write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, directory_flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _read_owner_mapping_metadata(path: Path) -> _OwnerMappingMetadata | None:
    """Read one private, explicit owner mapping attached to staged add state."""

    metadata_path = path / _OWNER_MAPPING_METADATA_FILE
    try:
        details = metadata_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LarkProfileError("owner-mapping metadata is unavailable") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or details.st_nlink != 1
        or stat.S_IMODE(details.st_mode) & 0o077
        or (hasattr(os, "getuid") and details.st_uid != os.getuid())
        or details.st_size > 4_096
    ):
        raise LarkProfileError(
            "owner-mapping metadata is not a private regular file"
        )
    try:
        value = json.loads(
            metadata_path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (_DuplicateJsonKey, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LarkProfileError("owner-mapping metadata is malformed") from exc
    expected_keys = {"schemaVersion", "principalId", "openId"}
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        raise LarkProfileError("owner-mapping metadata is malformed")
    open_id = value.get("openId")
    if (
        value.get("schemaVersion") != 1
        or value.get("principalId") != "owner"
        or not isinstance(open_id, str)
        or not _OPEN_ID.fullmatch(open_id)
    ):
        raise LarkProfileError("owner-mapping metadata is malformed")
    return _OwnerMappingMetadata("owner", open_id)


def _subprocess_environment(config_dir: Path | None = None) -> dict[str, str]:
    environment = dict(os.environ)
    for key in _LARK_CREDENTIAL_ENV_KEYS:
        environment.pop(key, None)
    environment.pop(LARK_CONFIG_ENV, None)
    if config_dir is not None:
        environment[LARK_CONFIG_ENV] = str(config_dir)
    # A host Agent workspace changes lark-cli's config path and may refuse
    # config init.  This owner-invoked subprocess is intentionally an isolated
    # local workspace, never an OpenClaw/Hermes/Lark-channel child.
    for key in tuple(environment):
        if key.startswith("OPENCLAW_") or key.startswith("HERMES_"):
            environment.pop(key, None)
    for key in ("LARK_CHANNEL",):
        environment.pop(key, None)
    environment["LARKSUITE_CLI_NO_UPDATE_NOTIFIER"] = "1"
    environment["LARKSUITE_CLI_NO_SKILLS_NOTIFIER"] = "1"
    return environment


def resolve_lark_cli_binary(value: str | os.PathLike[str] | None = None) -> str:
    configured = str(value or os.environ.get("CODEX_LARK_CLI", "lark-cli")).strip()
    if not configured:
        raise LarkProfileError("CODEX_LARK_CLI must name the lark-cli executable")
    found = shutil.which(configured)
    if found is None:
        raise LarkProfileError(
            "lark-cli was not found; install the official @larksuite/cli release "
            f"{PINNED_LARK_CLI_VERSION}"
        )
    return found


def verify_lark_cli_version(binary: str) -> str:
    try:
        result = _run_cli_one_shot(
            binary,
            "--version",
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LarkCliVersionError("could not execute lark-cli --version") from exc
    output = _ANSI_ESCAPE.sub("", result.stdout or "")
    versions = _SEMVER.findall(output)
    if result.returncode != 0 or len(set(versions)) != 1:
        raise LarkCliVersionError(
            "lark-cli returned an unrecognized version response: " + _public_text(output)
        )
    version = versions[0]
    if version != PINNED_LARK_CLI_VERSION:
        raise LarkCliVersionError(
            f"unsupported lark-cli version {version}; required "
            f"{PINNED_LARK_CLI_VERSION}"
        )
    return version


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if os.name == "posix" and process.pid > 0:
        # Every managed CLI invocation owns a fresh session whose process-group
        # ID is the direct child's PID.  Signal that group even when the direct
        # child has already exited: a helper may still hold stdout open (or
        # simply keep running) after its parent was reaped.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except OSError:
            if process.poll() is None:
                process.terminate()
            else:
                return

        deadline = time.monotonic() + 1.0
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                pass
        # Waiting only for the direct child is insufficient.  Give cooperative
        # descendants a short grace period, then fence the entire group.
        while time.monotonic() < deadline:
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                return
            except OSError:
                break
            time.sleep(0.02)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except OSError:
            if process.poll() is None:
                process.kill()
        if process.poll() is None:
            process.wait(timeout=5)
        return

    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _run_cli_one_shot(
    binary: str,
    *arguments: str,
    config_dir: Path | None = None,
    timeout: float,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded CLI command and fence its private process group.

    ``subprocess.run(timeout=...)`` terminates only its direct child.  The
    reviewed CLI can invoke credential/keychain helpers, so every owner-side
    diagnostic and cleanup command receives a fresh session as well.  The
    group is fenced in ``finally`` even after a successful direct-child exit:
    a detached helper must not outlive the administrative operation.
    """

    command = [binary, *arguments]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_subprocess_environment(config_dir),
        cwd=cwd,
        start_new_session=(os.name == "posix"),
        **_PRIVATE_SUBPROCESS_OPTIONS,
    )
    try:
        stdout, _stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(
            command,
            int(process.returncode or 0),
            stdout or "",
        )
    finally:
        _terminate_process(process)


def run_config_init(
    binary: str,
    config_dir: Path,
    *,
    output: TextIO,
    timeout: float,
    app_id: str | None = None,
    brand: str | None = None,
    secret_input: TextIO | None = None,
    on_spawn: Callable[[], None] | None = None,
    on_output: Callable[[str], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> str:
    """Run one pinned config-init mode and return bounded, redacted output.

    QR onboarding inherits the terminal and relays its URL/QR output. Existing-
    app onboarding reads one bounded secret from the supplied stdin stream,
    sends it only through the child's stdin pipe, and never relays child output.
    """

    deadline = time.monotonic() + timeout
    sensitive_values: tuple[str, ...] = ()
    secret_payload: str | None = None
    if app_id is None:
        if brand is not None or secret_input is not None:
            raise LarkProfileError("existing-app config init requires an App ID")
        command = [binary, "config", "init", "--new"]
        child_stdin: int | None = None
        relay_output = True
        failure_message = "lark-cli config init failed"
        timeout_message = "lark-cli QR onboarding timed out"
        cancellation_message = "lark-cli QR onboarding cancelled"
    else:
        normalized_app_id = str(app_id).strip()
        normalized_brand = str(brand or "").strip().lower()
        if not _APP_ID.fullmatch(normalized_app_id):
            raise LarkProfileError("existing-app config init has an invalid App ID")
        if normalized_brand not in {"feishu", "lark"}:
            raise LarkProfileError("existing-app config init has an invalid brand")
        if secret_input is None:
            raise LarkProfileError("existing-app config init requires App Secret stdin")
        secret = _read_app_secret(
            secret_input,
            timeout=max(0.0, deadline - time.monotonic()),
        )
        sensitive_values = (secret,)
        secret_payload = secret + "\n"
        command = [
            binary,
            "config",
            "init",
            "--app-id",
            normalized_app_id,
            "--app-secret-stdin",
            "--brand",
            normalized_brand,
        ]
        child_stdin = subprocess.PIPE
        relay_output = False
        failure_message = "lark-cli existing-app configuration failed"
        timeout_message = "lark-cli existing-app configuration timed out"
        cancellation_message = "lark-cli existing-app configuration cancelled"

    if cancel_requested is not None and cancel_requested():
        raise LarkProfileError(cancellation_message)

    try:
        process = subprocess.Popen(
            command,
            stdin=child_stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=_subprocess_environment(config_dir),
            start_new_session=(os.name == "posix"),
            **_PRIVATE_SUBPROCESS_OPTIONS,
        )
    except OSError as exc:
        raise LarkProfileError(
            "could not start lark-cli config initialization"
        ) from exc
    try:
        if on_spawn is not None:
            on_spawn()
    except BaseException:
        # The spawn marker is an internal publication fence, but keep this
        # helper safe for every caller: an unexpected callback failure must not
        # leave a credential-mutating child or its descendants running.
        _terminate_process(process)
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        assert process.stdout is not None
        try:
            process.stdout.close()
        except OSError:
            pass
        raise

    assert process.stdout is not None
    lines: queue.Queue[str | None] = queue.Queue()

    def read_output() -> None:
        try:
            for line in process.stdout:
                lines.put(line)
        finally:
            lines.put(None)

    reader = threading.Thread(
        target=read_output,
        name="lark-cli-onboarding-output",
        daemon=True,
    )
    reader.start()
    captured: list[str] = []
    size = 0
    ended = False
    stdin_error: OSError | None = None
    try:
        if secret_payload is not None:
            assert process.stdin is not None
            try:
                process.stdin.write(secret_payload)
                process.stdin.flush()
            except OSError as exc:
                stdin_error = exc
            finally:
                try:
                    process.stdin.close()
                except OSError as exc:
                    stdin_error = stdin_error or exc
            # Drop the standalone payload reference as soon as the child pipe
            # closes. The exact value remains only in the redaction tuple for
            # the lifetime of this bounded subprocess.
            secret_payload = None
        while not ended:
            if cancel_requested is not None and cancel_requested():
                raise LarkProfileError(cancellation_message)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LarkProfileError(timeout_message)
            try:
                item = lines.get(timeout=min(0.25, remaining))
            except queue.Empty:
                if process.poll() is not None and not reader.is_alive():
                    break
                continue
            if item is None:
                ended = True
                continue
            size += len(item.encode("utf-8", errors="replace"))
            if size > MAX_INIT_OUTPUT_BYTES:
                raise LarkProfileError("lark-cli onboarding output exceeded its limit")
            safe_item = _redact_exact(item, sensitive_values)
            captured.append(safe_item)
            if on_output is not None:
                on_output(safe_item)
            if relay_output:
                output.write(safe_item)
                output.flush()
        while True:
            if cancel_requested is not None and cancel_requested():
                raise LarkProfileError(cancellation_message)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LarkProfileError(timeout_message)
            try:
                return_code = process.wait(timeout=min(0.25, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
    except KeyboardInterrupt as exc:
        _terminate_process(process)
        raise LarkProfileError(cancellation_message) from exc
    except BaseException:
        _terminate_process(process)
        raise
    finally:
        # A successful direct child may have spawned a helper that closed the
        # inherited pipe but kept running.  Quiesce the complete process group
        # before inspecting or publishing its credential tree.
        _terminate_process(process)
        process.stdout.close()
        reader.join(timeout=2)

    raw = "".join(captured)
    if return_code != 0:
        if relay_output:
            raise LarkProfileError(
                failure_message + ": " + _public_text(raw or f"exit {return_code}")
            )
        raise LarkProfileError(f"{failure_message} (exit {return_code})")
    if stdin_error is not None:
        raise LarkProfileError("lark-cli did not accept App Secret from standard input")
    return raw


class _DuplicateJsonKey(ValueError):
    pass


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def _structured_init_result(raw_output: str) -> tuple[str, str]:
    """Read the one terminal app result from mixed QR/prose CLI output."""

    if len(raw_output.encode("utf-8", errors="replace")) > MAX_INIT_OUTPUT_BYTES:
        raise LarkProfileError("lark-cli onboarding output exceeded its limit")
    rendered = _ANSI_ESCAPE.sub("", raw_output)
    decoder = json.JSONDecoder(object_pairs_hook=_unique_json_object)
    identities: list[tuple[str, str, int]] = []
    for match in _INIT_JSON_OBJECT_START.finditer(rendered):
        object_start = match.end() - 1
        try:
            value, object_end = decoder.raw_decode(rendered, object_start)
        except _DuplicateJsonKey as exc:
            raise LarkProfileError(
                "lark-cli returned onboarding JSON with duplicate keys"
            ) from exc
        except (json.JSONDecodeError, RecursionError) as exc:
            raise LarkProfileError(
                "lark-cli returned malformed onboarding output"
            ) from exc
        if not isinstance(value, Mapping):
            continue
        has_app_id = "appId" in value or "app_id" in value
        has_brand = "brand" in value
        if not has_app_id and not has_brand:
            continue
        if not has_app_id or not has_brand:
            raise LarkProfileError("lark-cli returned malformed onboarding output")

        aliases = [value[name] for name in ("appId", "app_id") if name in value]
        if not aliases or any(not isinstance(item, str) for item in aliases):
            raise LarkProfileError("lark-cli returned an invalid app identity")
        normalized_aliases = [item.strip() for item in aliases]
        if (
            not normalized_aliases[0]
            or any(item != normalized_aliases[0] for item in normalized_aliases[1:])
        ):
            raise LarkProfileError(
                "lark-cli returned conflicting app identity aliases"
            )
        candidate_brand = value["brand"]
        if not isinstance(candidate_brand, str):
            raise LarkProfileError("lark-cli returned malformed onboarding output")
        app_id = normalized_aliases[0]
        brand = candidate_brand.strip().lower()
        if not _APP_ID.fullmatch(app_id) or brand not in {"lark", "feishu"}:
            raise LarkProfileError("lark-cli returned an invalid app identity")
        identities.append((app_id, brand, object_end))

    if not identities:
        raise LarkProfileError("lark-cli returned malformed onboarding output")
    if len(identities) != 1:
        raise LarkProfileError(
            "lark-cli returned multiple onboarding app identity objects"
        )
    app_id, brand, object_end = identities[0]
    if rendered[object_end:].strip():
        raise LarkProfileError(
            "lark-cli returned trailing content after its onboarding result"
        )
    return app_id, brand


def _credential_reference(
    config_dir: Path,
    app_id: str,
    secret: Any,
    *,
    context: str,
    allow_absolute_file_reference: bool,
) -> tuple[str, str]:
    """Validate the CLI's non-secret credential indirection.

    Keychain references are identity-bound by the reviewed CLI contract.  A
    file reference is accepted only when it names a private regular file
    inside this profile's isolated directory; a missing path or an escape via
    ``..`` is not a durable credential reference.
    """

    if not isinstance(secret, Mapping):
        raise LarkProfileError(
            f"{context} contains an unsupported plaintext app secret"
        )
    source = str(secret.get("source", "")).strip().lower()
    reference_id = str(secret.get("id", "")).strip()
    if source not in {"keychain", "file"} or not reference_id:
        raise LarkProfileError(f"{context} has an invalid app-secret reference")
    if source == "keychain":
        if reference_id != f"appsecret:{app_id}":
            raise LarkProfileError(
                f"{context} keychain reference conflicts with its app identity"
            )
        return source, reference_id

    reference_path = Path(reference_id).expanduser()
    if not allow_absolute_file_reference and (
        reference_path.is_absolute() or ".." in reference_path.parts
    ):
        # Onboarding and reauthorization inspect a staging directory that is
        # atomically renamed after verification.  Persisting an absolute path
        # into that staging tree would become dangling at publication.  Older
        # already-stable profiles may retain an absolute in-tree reference and
        # are validated by the compatibility path below.
        raise LarkProfileError(
            f"{context} file-secret reference must be relative to its config directory"
        )
    if not reference_path.is_absolute():
        reference_path = config_dir / reference_path
    secret_path = reference_path.resolve()
    try:
        secret_path.relative_to(config_dir.resolve())
    except ValueError as exc:
        raise LarkProfileError(
            f"{context} file secret must remain inside its isolated config directory"
        ) from exc
    try:
        details = secret_path.lstat()
    except OSError as exc:
        raise LarkProfileError(
            f"{context} file-secret reference is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or stat.S_IMODE(details.st_mode) & 0o077
    ):
        raise LarkProfileError(
            f"{context} file-secret reference must be a private regular file"
        )
    if hasattr(os, "getuid") and details.st_uid != os.getuid():
        raise LarkProfileError(
            f"{context} file-secret reference has a different owner"
        )
    return source, reference_id


def inspect_provisioned_config(
    config_dir: Path,
    raw_output: str,
    *,
    expected_identity: tuple[str, str] | None = None,
) -> ProvisionedConfig:
    """Verify output against the exact app and non-secret reference on disk."""

    _validate_private_tree(config_dir)
    config_path = config_dir / "config.json"
    try:
        details = config_path.lstat()
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LarkProfileError("lark-cli did not persist a valid config.json") from exc
    if not stat.S_ISREG(details.st_mode) or stat.S_IMODE(details.st_mode) & 0o077:
        raise LarkProfileError("lark-cli config.json must be a private regular file")
    apps = data.get("apps") if isinstance(data, Mapping) else None
    if not isinstance(apps, list) or len(apps) != 1 or not isinstance(apps[0], Mapping):
        raise LarkProfileError("lark-cli config.json must contain exactly one app")
    app = apps[0]
    app_id = str(app.get("appId", "")).strip()
    brand = str(app.get("brand", "")).strip().lower()
    if not _APP_ID.fullmatch(app_id) or brand not in {"lark", "feishu"}:
        raise LarkProfileError("lark-cli config.json has an invalid app identity")

    output_app_id, output_brand = (
        expected_identity
        if expected_identity is not None
        else _structured_init_result(raw_output)
    )
    if (app_id, brand) != (output_app_id, output_brand):
        if expected_identity is not None:
            raise LarkProfileError(
                "lark-cli persisted app identity does not match the requested identity"
            )
        raise LarkProfileError("lark-cli output does not match its persisted app identity")

    source, reference_id = _credential_reference(
        config_dir,
        app_id,
        app.get("appSecret"),
        context="lark-cli",
        allow_absolute_file_reference=False,
    )
    return ProvisionedConfig(
        app_id=app_id,
        brand=brand,
        credential_ref=f"{source}:{reference_id}",
    )


def inspect_bot_identity(
    binary: str,
    config_dir: Path,
    provisioned: ProvisionedConfig,
) -> ProvisionedConfig:
    """Verify and retain the exact bot ``open_id`` used for group mentions.

    The reviewed CLI exposes this through its read-only identity diagnostic.
    This is not user OAuth and never invokes ``auth login``.  Persisting the
    result is necessary because structured group mentions contain the bot's
    ``ou_`` identity, not its ``cli_`` app ID.
    """

    try:
        status_result = _run_cli_one_shot(
            binary,
            "auth",
            "status",
            "--verify",
            "--json",
            config_dir=config_dir,
            timeout=20,
        )
        selection_result = _run_cli_one_shot(
            binary,
            "whoami",
            "--as",
            "bot",
            config_dir=config_dir,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LarkProfileError("could not verify the provisioned Lark bot identity") from exc

    def parse_result(result: subprocess.CompletedProcess[str]) -> Mapping[str, Any]:
        raw = _ANSI_ESCAPE.sub("", result.stdout or "")
        if len(raw.encode("utf-8", errors="replace")) > MAX_INIT_OUTPUT_BYTES:
            raise LarkProfileError("lark-cli bot identity output exceeded its limit")
        try:
            value = json.loads(raw, object_pairs_hook=_unique_json_object)
        except _DuplicateJsonKey as exc:
            raise LarkProfileError(
                "lark-cli returned ambiguous bot identity output"
            ) from exc
        except (json.JSONDecodeError, RecursionError) as exc:
            raise LarkProfileError(
                "lark-cli returned malformed bot identity output"
            ) from exc
        if result.returncode != 0 or not isinstance(value, Mapping):
            raise LarkProfileError("lark-cli bot identity verification failed")
        return value

    try:
        verified = parse_composite_verified_lark_bot_identity(
            parse_result(status_result),
            parse_result(selection_result),
        )
    except LarkBotIdentityContractError as exc:
        raise LarkProfileError(
            "lark-cli did not return a verified bot open_id"
        ) from exc
    if (verified.app_id, verified.brand) != (
        provisioned.app_id,
        provisioned.brand,
    ):
        raise LarkProfileError(
            "lark-cli bot identity conflicts with the provisioned app"
        )
    return replace(provisioned, bot_open_id=verified.open_id)


def _parse_app_owner_identity(
    raw_output: str,
    *,
    expected_app_id: str,
    expected_brand: str,
    bot_open_id: str,
) -> str:
    """Parse the pinned CLI's bot-authenticated own-app response.

    ``config init --new`` intentionally does not persist the scanning human's
    identity.  The newly issued bot credential can, however, read its own app
    metadata.  For a freshly created PersonalAgent, the immutable creator and
    current enterprise-member owner identify that human in this exact app's
    ``open_id`` namespace.  Every identity-bearing field is checked together;
    no alias, first sender, or ambient user credential is an accepted fallback.
    """

    raw = _ANSI_ESCAPE.sub("", str(raw_output or ""))
    if len(raw.encode("utf-8", errors="replace")) > MAX_INIT_OUTPUT_BYTES:
        raise LarkProfileError("lark-cli app-owner output exceeded its limit")
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except _DuplicateJsonKey as exc:
        raise LarkProfileError(
            "lark-cli returned ambiguous app-owner output"
        ) from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise LarkProfileError(
            "lark-cli returned malformed app-owner output"
        ) from exc

    # Version 1.0.92's raw-API JSON contract is one standard success envelope.
    # Requiring its complete shape prevents a payload field at another nesting
    # level from competing with the bot-authenticated business response.
    if not isinstance(value, Mapping) or set(value) != {"ok", "identity", "data"}:
        raise LarkProfileError("lark-cli returned ambiguous app-owner output")
    if value.get("ok") is not True or value.get("identity") != "bot":
        raise LarkProfileError("lark-cli app-owner verification failed")
    data = value.get("data")
    if not isinstance(data, Mapping) or set(data) != {"app"}:
        raise LarkProfileError("lark-cli returned ambiguous app-owner output")
    app = data.get("app")
    if not isinstance(app, Mapping):
        raise LarkProfileError("lark-cli returned malformed app-owner output")

    # The endpoint's user_id_type=open_id request admits only its documented
    # snake_case fields.  Reject camelCase lookalikes instead of choosing one.
    if any(
        key in app
        for key in ("appId", "creatorId", "ownerId", "owner_open_id")
    ):
        raise LarkProfileError("lark-cli returned ambiguous app-owner output")
    app_id = app.get("app_id")
    creator_id = app.get("creator_id")
    status = app.get("status")
    scene_type = app.get("scene_type")
    owner_present = "owner" in app
    owner = app.get("owner")
    if (
        not isinstance(app_id, str)
        or not isinstance(creator_id, str)
        or type(status) is not int
        or type(scene_type) is not int
    ):
        raise LarkProfileError("lark-cli returned malformed app-owner output")

    if (
        expected_brand not in {"feishu", "lark"}
        or app_id != expected_app_id
        or not _APP_ID.fullmatch(app_id)
    ):
        raise LarkProfileError(
            "lark-cli app-owner identity conflicts with the provisioned app"
        )
    if status != 1 or scene_type != 0:
        raise LarkProfileError(
            "lark-cli did not return an enabled custom app"
        )
    if (
        not _OPEN_ID.fullmatch(creator_id)
        or creator_id == bot_open_id
    ):
        raise LarkProfileError(
            "lark-cli did not return one verified human app creator"
        )

    # Feishu documents and returns a human-owner object for custom apps.  The
    # international Lark schema does not promise that object, so its creator is
    # authoritative when owner is absent.  If Lark does return owner, it must
    # corroborate the creator exactly; an incomplete or conflicting object is
    # never ignored.
    if expected_brand == "feishu" and not owner_present:
        raise LarkProfileError("lark-cli did not return a verified human app owner")
    if owner_present:
        if not isinstance(owner, Mapping):
            raise LarkProfileError("lark-cli returned malformed app-owner output")
        if any(key in owner for key in ("ownerId", "open_id", "openId")):
            raise LarkProfileError("lark-cli returned ambiguous app-owner output")
        owner_type = owner.get("type")
        owner_id = owner.get("owner_id")
        if type(owner_type) is not int or not isinstance(owner_id, str):
            raise LarkProfileError("lark-cli returned malformed app-owner output")
        if (
            owner_type != 2
            or not _OPEN_ID.fullmatch(owner_id)
            or creator_id != owner_id
        ):
            raise LarkProfileError(
                "lark-cli did not return one verified human app owner"
            )
    return creator_id


def inspect_app_owner_identity(
    binary: str,
    config_dir: Path,
    provisioned: ProvisionedConfig,
) -> ProvisionedConfig:
    """Resolve the QR-created app's human owner using only its bot credential."""

    if not _OPEN_ID.fullmatch(provisioned.bot_open_id):
        raise LarkProfileError(
            "a verified bot identity is required before app-owner inspection"
        )
    try:
        result = _run_cli_one_shot(
            binary,
            "api",
            "GET",
            _APP_OWNER_PATH,
            "--params",
            _APP_OWNER_PARAMS,
            "--as",
            "bot",
            "--format",
            "json",
            config_dir=config_dir,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LarkProfileError(
            "could not verify the provisioned Lark app owner"
        ) from exc
    if result.returncode != 0:
        raise LarkProfileError("lark-cli app-owner verification failed")
    owner_open_id = _parse_app_owner_identity(
        result.stdout or "",
        expected_app_id=provisioned.app_id,
        expected_brand=provisioned.brand,
        bot_open_id=provisioned.bot_open_id,
    )
    return replace(provisioned, owner_open_id=owner_open_id)


def _config_identity(path: Path) -> str:
    canonical = str(path.absolute()).encode("utf-8")
    return "path-sha256:" + hashlib.sha256(canonical).hexdigest()


def _config_for_profile(
    path: Path,
    *,
    allow_absolute_file_reference: bool = True,
) -> ProvisionedConfig:
    """Read an existing profile without needing historic init stdout.

    Stable profiles retain compatibility with historic absolute in-tree file
    references.  A staging directory that will be renamed must opt out because
    such a reference would become dangling at publication.
    """

    event_socket_apps = _validate_private_tree(
        path,
        allow_runtime_event_socket=True,
    )
    try:
        data = json.loads((path / "config.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LarkProfileError("profile has no valid lark-cli config.json") from exc
    apps = data.get("apps") if isinstance(data, Mapping) else None
    if not isinstance(apps, list) or len(apps) != 1 or not isinstance(apps[0], Mapping):
        raise LarkProfileError("profile config must contain exactly one app")
    app = apps[0]
    app_id = str(app.get("appId", "")).strip()
    brand = str(app.get("brand", "")).strip().lower()
    if not _APP_ID.fullmatch(app_id) or brand not in {"lark", "feishu"}:
        raise LarkProfileError("profile config has an invalid app identity")
    if event_socket_apps.difference({app_id}):
        raise LarkProfileError(
            "lark-cli event bus socket belongs to another app"
        )
    source, reference_id = _credential_reference(
        path,
        app_id,
        app.get("appSecret"),
        context="profile config",
        allow_absolute_file_reference=allow_absolute_file_reference,
    )
    return ProvisionedConfig(app_id, brand, f"{source}:{reference_id}")


def secure_and_reinspect_provisioned_config(
    path: Path,
    expected: ProvisionedConfig,
) -> ProvisionedConfig:
    """Normalize CLI artifacts and fence every credential identity field.

    This helper deliberately performs no database or account-lock operation.
    Callers that use it during live onboarding must establish those ownership
    boundaries before publishing the inspected tree.
    """

    _tighten_owned_staging_tree(path)
    observed = _config_for_profile(
        path,
        allow_absolute_file_reference=False,
    )
    if (
        observed.app_id,
        observed.brand,
        observed.credential_ref,
    ) != (
        expected.app_id,
        expected.brand,
        expected.credential_ref,
    ):
        raise LarkProfileError(
            "lark-cli config identity changed during verification"
        )
    return replace(
        observed,
        bot_open_id=expected.bot_open_id,
        owner_open_id=expected.owner_open_id,
    )


def build_lark_profile_record(
    profile_id: str,
    final_path: Path,
    provisioned: ProvisionedConfig,
    version: str,
    *,
    credential_origin: str | None = None,
) -> BotProfileRecord:
    """Build the durable record shared by terminal and live onboarding."""

    normalized_profile_id = normalize_profile_name(profile_id)
    # Preserve the caller's already-vetted spelling.  In particular, macOS
    # may expose ``/tmp`` through the ``/private/tmp`` alias; rewriting it here
    # would change the durable config identity used by existing profiles.
    stable_path = Path(final_path)
    restart_policy: dict[str, Any] = {
        "max_attempts": 8,
        "base_delay": 1,
        "max_delay": 60,
        "bot_open_id": provisioned.bot_open_id,
    }
    if credential_origin == "existing_app":
        restart_policy["credential_origin"] = "existing_app"
    return BotProfileRecord(
        profile_id=normalized_profile_id,
        channel="lark",
        bot_id=provisioned.app_id,
        brand=provisioned.brand,
        config_dir=str(stable_path),
        config_dir_identity=_config_identity(stable_path),
        cli_version=version,
        credential_ref=provisioned.credential_ref,
        enabled=True,
        mention_policy="direct_or_mention",
        access_policy="all",
        restart_policy=restart_policy,
    )


def _unparseable_staging_is_discardable(path: Path) -> bool:
    """Return whether failed onboarding left no recoverable external handle.

    An empty staging directory, or one containing only COW's validated owner
    mapping intent, means ``config init`` failed before persisting CLI state.
    A structurally valid one-app document whose secret is a plaintext value is
    also local-only under the pinned CLI contract and must be removed instead
    of retained.  Every other non-empty, unparseable tree is ambiguous: it may
    be the only evidence of a partially persisted keychain/file credential and
    therefore must survive for owner audit.
    """

    try:
        entries = list(path.iterdir())
    except OSError:
        return False
    if not entries:
        return True
    if {entry.name for entry in entries} == {_OWNER_MAPPING_METADATA_FILE}:
        try:
            return _read_owner_mapping_metadata(path) is not None
        except LarkProfileError:
            return False
    config_path = path / "config.json"
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return False
    apps = data.get("apps") if isinstance(data, Mapping) else None
    if not isinstance(apps, list) or len(apps) != 1 or not isinstance(apps[0], Mapping):
        return False
    plaintext = apps[0].get("appSecret")
    return isinstance(plaintext, str) and bool(plaintext)


def _store_method(store: Any, primary: str, *aliases: str) -> Callable[..., Any]:
    for name in (primary, *aliases):
        method = getattr(store, name, None)
        if callable(method):
            return method
    raise LarkProfileStoreUnavailable(
        f"SQLiteStore does not provide {primary}(); apply the Lark bot-profile migration"
    )


async def _maybe_await(value: T | Awaitable[T]) -> T:
    if hasattr(value, "__await__"):
        return await value  # type: ignore[misc]
    return value  # type: ignore[return-value]


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _open_runtime_database_read_only(database: Path) -> sqlite3.Connection:
    """Open the profile schema without migration, recovery, or ownership locks."""

    if not database.exists():
        raise LarkProfileStoreUnavailable(
            "runtime database does not exist; start ./cow once to initialize it"
        )
    if not database.is_file():
        raise LarkProfileStoreUnavailable("runtime database is not a regular file")
    try:
        connection = sqlite3.connect(
            f"{database.as_uri()}?mode=ro",
            uri=True,
            timeout=1.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {
            "schema_migrations",
            "bot_profiles",
            "bot_profile_status",
            "principals",
            "principal_accounts",
        }
        missing = sorted(required - tables)
        if missing:
            raise LarkProfileStoreUnavailable(
                "runtime database is missing the Lark profile schema: "
                + ", ".join(missing)
            )
        versions = {
            int(row[0])
            for row in connection.execute(
                "SELECT version FROM schema_migrations"
            ).fetchall()
        }
        if LARK_PROFILE_SCHEMA_VERSION not in versions:
            raise LarkProfileStoreUnavailable(
                "runtime database has not applied the Lark profile migration"
            )
        if versions and max(versions) > OWNER_CLI_MAX_RUNTIME_SCHEMA_VERSION:
            raise LarkProfileStoreUnavailable(
                "runtime database schema is newer than this owner CLI"
            )
        expected_columns = {
            "bot_profiles": {
                "profile_id",
                "channel",
                "bot_id",
                "brand",
                "config_dir",
                "config_dir_identity",
                "cli_version",
                "credential_ref",
                "enabled",
                "mention_policy",
                "access_policy",
                "restart_policy_json",
                "created_at",
                "updated_at",
                "removed_at",
            },
            "bot_profile_status": {
                "profile_id",
                "onboarding_state",
                "connection_state",
                "generation",
                "last_ready_at",
                "retry_after",
                "last_error_code",
                "updated_at",
            },
        }
        for table, expected in expected_columns.items():
            actual = {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if not expected.issubset(actual):
                raise LarkProfileStoreUnavailable(
                    f"runtime database has an incompatible {table} schema"
                )
        return connection
    except LarkProfileStoreUnavailable:
        if "connection" in locals():
            connection.close()
        raise
    except sqlite3.Error as exc:
        if "connection" in locals():
            connection.close()
        raise LarkProfileStoreUnavailable(
            "could not read the runtime database profile schema"
        ) from exc


def _profile_from_read_only_row(row: sqlite3.Row) -> BotProfileRecord:
    try:
        restart_policy = json.loads(str(row["restart_policy_json"] or "{}"))
    except (TypeError, json.JSONDecodeError) as exc:
        raise LarkProfileStoreUnavailable(
            "bot profile contains malformed restart policy JSON"
        ) from exc
    if not isinstance(restart_policy, Mapping):
        raise LarkProfileStoreUnavailable(
            "bot profile contains an invalid restart policy"
        )
    return BotProfileRecord(
        profile_id=str(row["profile_id"]),
        channel=str(row["channel"]),
        bot_id=str(row["bot_id"]),
        brand=str(row["brand"]),
        config_dir=str(row["config_dir"]),
        config_dir_identity=str(row["config_dir_identity"]),
        cli_version=str(row["cli_version"]),
        credential_ref=str(row["credential_ref"]),
        enabled=bool(row["enabled"]),
        mention_policy=str(row["mention_policy"]),
        access_policy=str(row["access_policy"]),
        restart_policy=dict(restart_policy),
        created_at=text_to_datetime(row["created_at"]),
        updated_at=text_to_datetime(row["updated_at"]),
        removed_at=text_to_datetime(row["removed_at"]),
    )


def _status_from_read_only_row(
    row: sqlite3.Row | None,
) -> BotProfileStatusRecord | None:
    if row is None:
        return None
    return BotProfileStatusRecord(
        profile_id=str(row["profile_id"]),
        onboarding_state=str(row["onboarding_state"]),
        connection_state=str(row["connection_state"]),
        generation=int(row["generation"]),
        last_ready_at=text_to_datetime(row["last_ready_at"]),
        retry_after=text_to_datetime(row["retry_after"]),
        last_error_code=(
            str(row["last_error_code"])
            if row["last_error_code"] is not None
            else None
        ),
        updated_at=text_to_datetime(row["updated_at"]),
    )


class LarkProfileAdmin:
    """Synchronous owner CLI around async durable profile APIs."""

    def __init__(
        self,
        *,
        database: Path | None = None,
        config_root: Path | None = None,
        binary: str | None = None,
        store_factory: Callable[[Path], Any] = SQLiteStore,
        output: TextIO | None = None,
        input_stream: TextIO | None = None,
    ) -> None:
        self.database = (database or durable_database_path()).expanduser().resolve()
        self.config_root = config_root or lark_config_root()
        self._binary = binary
        self.store_factory = store_factory
        self.output = output or sys.stdout
        self.input_stream = input_stream if input_stream is not None else sys.stdin

    def _root(self) -> Path:
        return _ensure_private_directory(self.config_root, create=True)

    def _validate_read_only_root(self) -> None:
        """Validate existing credential state without creating it."""

        root = Path(self.config_root)
        if root.exists() or root.is_symlink():
            _ensure_private_directory(root, create=False)

    def _profile_path(self, profile_id: str) -> Path:
        return _contained_child(self._root(), normalize_profile_name(profile_id))

    def _verified_binary(self) -> tuple[str, str]:
        binary = resolve_lark_cli_binary(self._binary)
        return binary, verify_lark_cli_version(binary)

    async def _with_store(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        store = self.store_factory(self.database)
        try:
            initialize = getattr(store, "initialize", None)
            if not callable(initialize):
                raise LarkProfileStoreUnavailable("SQLiteStore has no initialize() method")
            await _maybe_await(initialize())
            return await operation(store)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                await _maybe_await(close())

    def _run_store(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        return asyncio.run(self._with_store(operation))

    @property
    def _native_read_only(self) -> bool:
        """Whether list/status can use the production non-mutating SQL path."""

        return self.store_factory is SQLiteStore

    def _read_profiles(
        self,
        profile_id: str | None = None,
        *,
        include_removed: bool = False,
    ) -> list[BotProfileRecord]:
        connection = _open_runtime_database_read_only(self.database)
        try:
            filters = ["channel='lark'"]
            parameters: list[Any] = []
            if not include_removed:
                filters.append("removed_at IS NULL")
            if profile_id is not None:
                filters.append("profile_id=?")
                parameters.append(profile_id)
            rows = connection.execute(
                "SELECT * FROM bot_profiles WHERE "
                + " AND ".join(filters)
                + " ORDER BY created_at, profile_id",
                parameters,
            ).fetchall()
            return [_profile_from_read_only_row(row) for row in rows]
        except LarkProfileStoreUnavailable:
            raise
        except sqlite3.Error as exc:
            raise LarkProfileStoreUnavailable(
                "could not read Lark bot profiles"
            ) from exc
        finally:
            connection.close()

    def _read_profile_statuses(
        self, profile_id: str | None = None
    ) -> list[tuple[BotProfileRecord, BotProfileStatusRecord]]:
        connection = _open_runtime_database_read_only(self.database)
        try:
            filters = ["p.channel='lark'", "p.removed_at IS NULL"]
            parameters: list[Any] = []
            if profile_id is not None:
                filters.append("p.profile_id=?")
                parameters.append(profile_id)
            rows = connection.execute(
                "SELECT p.*, s.profile_id AS status_profile_id, "
                "s.onboarding_state, s.connection_state, s.generation, "
                "s.last_ready_at, s.retry_after, s.last_error_code, "
                "s.updated_at AS status_updated_at "
                "FROM bot_profiles AS p "
                "LEFT JOIN bot_profile_status AS s ON s.profile_id=p.profile_id "
                "WHERE "
                + " AND ".join(filters)
                + " ORDER BY p.created_at, p.profile_id",
                parameters,
            ).fetchall()
            result: list[tuple[BotProfileRecord, BotProfileStatusRecord]] = []
            for row in rows:
                if row["status_profile_id"] is None:
                    raise LarkProfileStoreUnavailable(
                        "Lark bot profile is missing its durable status row"
                    )
                profile = _profile_from_read_only_row(row)
                status = BotProfileStatusRecord(
                    profile_id=str(row["status_profile_id"]),
                    onboarding_state=str(row["onboarding_state"]),
                    connection_state=str(row["connection_state"]),
                    generation=int(row["generation"]),
                    last_ready_at=text_to_datetime(row["last_ready_at"]),
                    retry_after=text_to_datetime(row["retry_after"]),
                    last_error_code=(
                        str(row["last_error_code"])
                        if row["last_error_code"] is not None
                        else None
                    ),
                    updated_at=text_to_datetime(row["status_updated_at"]),
                )
                result.append((profile, status))
            return result
        except LarkProfileStoreUnavailable:
            raise
        except sqlite3.Error as exc:
            raise LarkProfileStoreUnavailable(
                "could not read Lark bot profile status"
            ) from exc
        finally:
            connection.close()

    def _temporary_config(self, prefix: str) -> Path:
        root = self._root()
        path = Path(tempfile.mkdtemp(prefix=f".{prefix}-", dir=root))
        os.chmod(path, 0o700)
        return path

    def _profile_record(
        self,
        profile_id: str,
        final_path: Path,
        provisioned: ProvisionedConfig,
        version: str,
        *,
        credential_origin: str | None = None,
    ) -> BotProfileRecord:
        return build_lark_profile_record(
            profile_id,
            final_path,
            provisioned,
            version,
            credential_origin=credential_origin,
        )

    @staticmethod
    def _secure_and_reinspect_cli_config(
        path: Path,
        expected: ProvisionedConfig,
    ) -> ProvisionedConfig:
        """Normalize post-command artifacts and fence config identity changes."""

        return secure_and_reinspect_provisioned_config(path, expected)

    @staticmethod
    async def _all_lark_profiles(store: Any) -> list[Any]:
        method = _store_method(store, "list_bot_profiles")
        profiles = await _maybe_await(
            method(channel="lark", include_removed=True)
        )
        return list(profiles or ())

    @staticmethod
    def _has_imported_credential_provenance(
        profiles: Sequence[Any],
        *,
        app_id: str,
    ) -> bool:
        """Keep external ownership durable at App-ID scope, even after removal."""

        return any(
            str(_field(profile, "channel", "")).lower() == "lark"
            and str(_field(profile, "bot_id", "")) == app_id
            and dict(_field(profile, "restart_policy", {}) or {}).get(
                "credential_origin"
            )
            == "existing_app"
            for profile in profiles
        )

    @staticmethod
    def _is_exact_live_profile(profile: Any, expected: BotProfileRecord) -> bool:
        """Match every registration-controlled field; timestamps are store-owned."""

        restart_policy = dict(_field(profile, "restart_policy", {}) or {})
        return bool(
            _field(profile, "removed_at") is None
            and str(_field(profile, "profile_id", "")) == expected.profile_id
            and str(_field(profile, "channel", "")) == expected.channel
            and str(_field(profile, "bot_id", "")) == expected.bot_id
            and str(_field(profile, "brand", "")).lower() == expected.brand
            and str(_field(profile, "config_dir", "")) == expected.config_dir
            and str(_field(profile, "config_dir_identity", ""))
            == expected.config_dir_identity
            and str(_field(profile, "cli_version", ""))
            == expected.cli_version
            and str(_field(profile, "credential_ref", ""))
            == expected.credential_ref
            and bool(_field(profile, "enabled", False)) == expected.enabled
            and str(_field(profile, "mention_policy", ""))
            == expected.mention_policy
            and str(_field(profile, "access_policy", ""))
            == expected.access_policy
            and restart_policy == dict(expected.restart_policy)
        )

    @staticmethod
    def _is_exact_owner_mapping(
        account: Any,
        expected_profile: BotProfileRecord,
        expected_mapping: _OwnerMappingMetadata,
    ) -> bool:
        return bool(
            account is not None
            and str(_field(account, "principal_id", ""))
            == expected_mapping.principal_id
            and str(_field(account, "channel", "")).lower() == "lark"
            and str(_field(account, "bot_id", "")) == expected_profile.bot_id
            and str(_field(account, "external_user_id", ""))
            == expected_mapping.open_id
            and str(_field(account, "identifier_kind", "")) == "open_id"
            and bool(_field(account, "active", False))
            and bool(_field(account, "principal_enabled", True))
        )

    async def _registration_fence(
        self,
        store: Any,
        expected: BotProfileRecord,
        owner_mapping: _OwnerMappingMetadata | None = None,
    ) -> tuple[str, BotProfileRecord | None]:
        """Classify durable state after an ambiguous create outcome.

        SQLite operations drain their executor after cancellation, so a create
        may have committed even though its awaiting coroutine raises.  Callers
        hold database and account ownership while consuming this fence.
        """

        relevant_conflict = False
        exact: BotProfileRecord | None = None
        for profile in await self._all_lark_profiles(store):
            if self._is_exact_live_profile(profile, expected):
                exact = profile
                continue
            if (
                str(_field(profile, "profile_id", "")) == expected.profile_id
                or str(_field(profile, "bot_id", "")) == expected.bot_id
                or str(_field(profile, "config_dir", "")) == expected.config_dir
                or str(_field(profile, "config_dir_identity", ""))
                == expected.config_dir_identity
            ):
                relevant_conflict = True

        mapping_present = False
        mapping_exact = owner_mapping is None
        if owner_mapping is not None:
            resolver = _store_method(store, "resolve_principal_account")
            account = await _maybe_await(
                resolver(
                    channel="lark",
                    bot_id=expected.bot_id,
                    external_user_id=owner_mapping.open_id,
                )
            )
            mapping_present = account is not None
            mapping_exact = self._is_exact_owner_mapping(
                account,
                expected,
                owner_mapping,
            )

        if exact is not None:
            status_method = _store_method(store, "get_bot_profile_status")
            status = await _maybe_await(status_method(expected.profile_id))
            if status is None or not mapping_exact:
                return "conflict", None
            return "committed", exact
        if relevant_conflict or mapping_present:
            return "conflict", None
        return "absent", None

    async def _create_registration(
        self,
        store: Any,
        profile: BotProfileRecord,
        owner_mapping: _OwnerMappingMetadata | None,
    ) -> BotProfileRecord:
        """Commit a profile and optional owner account as one store operation."""

        if owner_mapping is None:
            method = _store_method(
                store,
                "create_bot_profile",
                "register_bot_profile",
                "add_bot_profile",
            )
            return await _maybe_await(method(profile))

        method = _store_method(
            store,
            "create_bot_profile_with_principal_account",
        )
        result = await _maybe_await(
            method(
                profile,
                principal_id=owner_mapping.principal_id,
                external_user_id=owner_mapping.open_id,
                identifier_kind="open_id",
                configured_by=_local_owner_identity(),
            )
        )
        if (
            not isinstance(result, Sequence)
            or isinstance(result, (str, bytes, bytearray))
            or len(result) != 2
        ):
            raise LarkProfileStoreUnavailable(
                "profile owner registration returned an invalid result"
            )
        created, account = result
        if not self._is_exact_live_profile(created, profile) or not (
            self._is_exact_owner_mapping(account, profile, owner_mapping)
        ):
            raise LarkProfileStoreUnavailable(
                "profile owner registration returned conflicting identity"
            )
        return created

    @staticmethod
    def _matches_reauthorization_state(
        profile: Any,
        expected: BotProfileRecord,
    ) -> bool:
        """Compare every durable field relevant to credential publication."""

        return bool(
            _field(profile, "removed_at") is None
            and str(_field(profile, "profile_id", "")) == expected.profile_id
            and str(_field(profile, "channel", "")) == expected.channel
            and str(_field(profile, "bot_id", "")) == expected.bot_id
            and str(_field(profile, "brand", "")).lower() == expected.brand
            and str(_field(profile, "config_dir", "")) == expected.config_dir
            and str(_field(profile, "config_dir_identity", ""))
            == expected.config_dir_identity
            and str(_field(profile, "cli_version", "")) == expected.cli_version
            and str(_field(profile, "credential_ref", ""))
            == expected.credential_ref
            and bool(_field(profile, "enabled", False)) == expected.enabled
            and str(_field(profile, "mention_policy", ""))
            == expected.mention_policy
            and str(_field(profile, "access_policy", ""))
            == expected.access_policy
            and dict(_field(profile, "restart_policy", {}) or {})
            == dict(expected.restart_policy)
        )

    async def _reauthorization_preflight(
        self,
        store: Any,
        *,
        profile_id: str,
        final_path: Path,
        existing: ProvisionedConfig,
    ) -> BotProfileRecord:
        """Bind the stable config to its exact live durable row before QR setup."""

        get_method = _store_method(store, "get_bot_profile")
        profile = await _maybe_await(
            get_method(profile_id, include_removed=True)
        )
        if (
            profile is None
            or _field(profile, "removed_at") is not None
            or str(_field(profile, "profile_id", "")) != profile_id
            or str(_field(profile, "channel", "")) != "lark"
            or str(_field(profile, "bot_id", "")) != existing.app_id
            or str(_field(profile, "brand", "")).lower() != existing.brand
            or str(_field(profile, "config_dir", "")) != str(final_path)
            or str(_field(profile, "config_dir_identity", ""))
            != _config_identity(final_path)
            or str(_field(profile, "credential_ref", ""))
            != existing.credential_ref
        ):
            raise LarkProfileError(
                "profile config identity conflicts with the durable bot profile"
            )
        status_method = _store_method(store, "get_bot_profile_status")
        status = await _maybe_await(status_method(profile_id))
        if status is None:
            raise LarkProfileStoreUnavailable(
                "Lark bot profile is missing its durable status row"
            )
        return profile

    async def _staged_credential_disposition(
        self,
        store: Any,
        candidate: ProvisionedConfig,
    ) -> _StagedCredentialDisposition:
        """Prove whether cleanup would affect an extant durable credential."""

        profiles = await self._all_lark_profiles(store)
        # lark-cli keychain entries are App-ID-global for the OS user. An
        # imported-origin tombstone remains a permanent ownership veto even if
        # its local config tree is gone or a later CLI flow selects a different
        # credential backend/reference.
        if self._has_imported_credential_provenance(
            profiles,
            app_id=candidate.app_id,
        ):
            return _StagedCredentialDisposition.SHARED
        for profile in profiles:
            if (
                str(_field(profile, "channel", "")).lower() != "lark"
                or str(_field(profile, "bot_id", "")) != candidate.app_id
                or str(_field(profile, "brand", "")).lower() != candidate.brand
                or str(_field(profile, "credential_ref", ""))
                != candidate.credential_ref
            ):
                continue
            archived_path = Path(str(_field(profile, "config_dir", "")))
            if (
                _field(profile, "removed_at") is None
                or archived_path.exists()
                or archived_path.is_symlink()
            ):
                try:
                    durable_config = _config_for_profile(archived_path)
                except LarkProfileError:
                    return _StagedCredentialDisposition.UNKNOWN
                if (
                    durable_config.app_id,
                    durable_config.brand,
                    durable_config.credential_ref,
                ) != (
                    candidate.app_id,
                    candidate.brand,
                    candidate.credential_ref,
                ):
                    return _StagedCredentialDisposition.UNKNOWN
                return _StagedCredentialDisposition.SHARED
        return _StagedCredentialDisposition.CLEANUP_ALLOWED

    async def _credential_has_imported_provenance(
        self,
        store: Any,
        candidate: ProvisionedConfig,
    ) -> bool:
        return self._has_imported_credential_provenance(
            await self._all_lark_profiles(store),
            app_id=candidate.app_id,
        )

    async def _reauthorization_fence(
        self,
        store: Any,
        *,
        before: BotProfileRecord,
        expected: BotProfileRecord,
    ) -> tuple[str, BotProfileRecord | None]:
        """Classify durable state after an ambiguous credential-update outcome."""

        get_method = _store_method(store, "get_bot_profile")
        current = await _maybe_await(
            get_method(expected.profile_id, include_removed=True)
        )
        if current is None:
            return "conflict", None
        status_method = _store_method(store, "get_bot_profile_status")
        status = await _maybe_await(status_method(expected.profile_id))
        if status is None:
            return "conflict", None
        # Check the desired state first.  When the logical reference and bot
        # identity were unchanged, committed and absent rows are intentionally
        # indistinguishable; either disposition is safe with the new config.
        if self._matches_reauthorization_state(current, expected):
            return "committed", current
        if self._matches_reauthorization_state(current, before):
            return "absent", current
        return "conflict", current

    def _retire_reauthorization_backup(
        self,
        binary: str,
        backup: Path,
        old_credential: ProvisionedConfig,
    ) -> None:
        """Retire an old config only after proving its credential is unshared."""

        try:
            disposition = self._run_store(
                lambda store: self._staged_credential_disposition(
                    store,
                    old_credential,
                )
            )
        except BaseException as exc:
            raise LarkProfileError(
                "Lark reauthorization committed but old credential cleanup is "
                f"unresolved; retained {backup}"
            ) from exc
        if disposition is _StagedCredentialDisposition.UNKNOWN:
            raise LarkProfileError(
                "Lark reauthorization committed but old credential cleanup is "
                f"unresolved; retained {backup}"
            )
        try:
            if disposition is _StagedCredentialDisposition.CLEANUP_ALLOWED:
                self._cleanup_new_credentials(binary, backup)
            else:
                _safe_remove_tree(backup, self._root())
        except BaseException as exc:
            # `_cleanup_new_credentials` deliberately retains the only retry
            # handle when backend cleanup fails.  Never turn that failure into
            # a plain tree deletion on the enclosing exception path.
            raise LarkProfileError(
                "Lark reauthorization committed but old credential cleanup is "
                f"unresolved; retained {backup}"
            ) from exc

    def _recovery_paths(
        self,
        staging_name: str,
        profile_name: str | None,
    ) -> tuple[str, Path, Path]:
        """Resolve one retained add directory without accepting path traversal."""

        name = str(staging_name or "").strip()
        if not name or Path(name).name != name:
            raise LarkProfileError(
                "recovery staging name must be a direct-child basename"
            )
        matched = _ADD_STAGING_NAME.fullmatch(name)
        if matched is None:
            raise LarkProfileError(
                "recovery staging name must match .add-<profile>-<8-char nonce>"
            )
        inferred_profile = normalize_profile_name(matched.group("profile"))
        profile_id = (
            normalize_profile_name(profile_name)
            if profile_name is not None
            else inferred_profile
        )
        root = self._root()
        return (
            profile_id,
            _contained_child(root, name),
            _contained_child(root, profile_id),
        )

    def _cleanup_new_credentials(self, binary: str, path: Path) -> None:
        """Remove one proven-unowned credential without risking its sole handle.

        The reviewed CLI may mutate or delete ``config.json`` before returning
        a non-zero status.  Snapshot the validated private tree first, run the
        destructive command against its stable basename (which also preserves
        legacy absolute in-tree file references), and atomically restore the
        snapshot on every command failure.  The snapshot is discarded only
        after the backend confirms success.
        """

        root = self._root()
        if path.parent != root:
            raise LarkProfileError(
                "staged credential cleanup path is outside the config root"
            )
        try:
            original = _config_for_profile(path)
            original_config = path.joinpath("config.json").read_bytes()
        except (LarkProfileError, OSError) as exc:
            raise LarkProfileError(
                "staged credential cleanup requires its private config.json; "
                f"retained {path}"
            ) from exc

        snapshot = Path(
            tempfile.mkdtemp(
                prefix=f".cleanup-{path.name[:32]}-",
                dir=root,
            )
        )
        os.chmod(snapshot, 0o700)
        try:
            # Unix sockets cannot be copied and are not credential state.  The
            # preceding exact-tree validation proved that this is the one
            # app-scoped lark-cli runtime socket; omit no other event artifact.
            shutil.copytree(
                path,
                snapshot,
                dirs_exist_ok=True,
                ignore=_event_socket_copy_ignore(path, original.app_id),
            )
            _tighten_owned_staging_tree(snapshot)
            if snapshot.joinpath("config.json").read_bytes() != original_config:
                raise LarkProfileError(
                    "credential cleanup snapshot changed config identity"
                )
            # Relative file references and keychain references remain valid in
            # the sibling snapshot and can be fully revalidated there.  A
            # legacy absolute file reference deliberately continues to name
            # the stable original basename; after restoration that reference
            # is valid again, so tree validation plus byte equality is the
            # appropriate snapshot attestation.
            source, reference_id = original.credential_ref.split(":", 1)
            reference_path = Path(reference_id).expanduser()
            relocation_sensitive = bool(
                source == "file"
                and (
                    reference_path.is_absolute()
                    or ".." in reference_path.parts
                )
            )
            if relocation_sensitive:
                original_secret = (
                    reference_path
                    if reference_path.is_absolute()
                    else path / reference_path
                ).resolve()
                relative_secret = original_secret.relative_to(path.resolve())
                if not snapshot.joinpath(relative_secret).is_file():
                    raise LarkProfileError(
                        "credential cleanup snapshot is missing its file secret"
                    )
            else:
                copied = _config_for_profile(snapshot)
                if (
                    copied.app_id,
                    copied.brand,
                    copied.credential_ref,
                ) != (
                    original.app_id,
                    original.brand,
                    original.credential_ref,
                ):
                    raise LarkProfileError(
                        "credential cleanup snapshot changed app identity"
                    )
        except BaseException as exc:
            try:
                _safe_remove_tree(snapshot, root)
            except BaseException:
                pass
            raise LarkProfileError(
                f"could not create credential cleanup snapshot; retained {path}"
            ) from exc

        def restore_snapshot() -> None:
            damaged = _contained_child(
                root,
                f".failed-cleanup-{path.name[:32]}-{uuid.uuid4().hex}",
            )
            moved_damaged = False
            try:
                if path.exists() or path.is_symlink():
                    os.replace(path, damaged)
                    moved_damaged = True
                os.replace(snapshot, path)
            except BaseException as exc:
                raise LarkProfileError(
                    "credential cleanup failed and its private snapshot could "
                    f"not be restored; retained {snapshot}"
                    + (f" and {damaged}" if moved_damaged else "")
                ) from exc
            if moved_damaged:
                try:
                    _safe_remove_tree(damaged, root)
                except BaseException as exc:
                    raise LarkProfileError(
                        "credential cleanup failed; restored the original at "
                        f"{path} and retained damaged state {damaged}"
                    ) from exc

        try:
            result = _run_cli_one_shot(
                binary,
                "config",
                "remove",
                config_dir=path,
                timeout=15,
            )
        except BaseException as exc:
            restore_snapshot()
            if isinstance(exc, (OSError, subprocess.TimeoutExpired)):
                raise LarkProfileError(
                    f"staged credential cleanup failed; restored and retained {path}"
                ) from exc
            raise
        if result.returncode != 0:
            restore_snapshot()
            raise LarkProfileError(
                "staged credential cleanup failed; restored and retained "
                f"{path}: {_public_text(result.stdout)}"
            )

        # Backend cleanup is now confirmed.  Remove the mutated source first;
        # until that succeeds, the untouched snapshot remains the recovery
        # handle.  A crash between the two deletions likewise leaves the exact
        # snapshot available for inspection or restoration.
        try:
            _safe_remove_tree(path, root)
        except BaseException as exc:
            raise LarkProfileError(
                "credential cleanup succeeded but private state cleanup failed; "
                f"retained source {path} and snapshot {snapshot}"
            ) from exc
        try:
            _safe_remove_tree(snapshot, root)
        except BaseException as exc:
            raise LarkProfileError(
                "credential cleanup succeeded but snapshot cleanup failed; "
                f"retained {snapshot}"
            ) from exc

    def _rollback_unregistered_config(
        self,
        binary: str,
        path: Path,
        *,
        database_owned: bool = False,
        account_owned_app_id: str | None = None,
        allow_credential_removal: bool = True,
    ) -> None:
        """Rollback staging without deleting an aliased app-global secret.

        ``add`` holds database ownership across the complete QR transaction.
        Its rollback therefore reuses that ownership and takes only the exact
        account lock if identity validation had not reached that point yet.
        Legacy callers may still ask this helper to acquire the complete lock
        set itself.
        """

        root = self._root()
        try:
            _tighten_owned_staging_tree(path)
        except LarkProfileError as exc:
            # A hardlink, traversal failure, foreign owner, or other unsafe
            # tree shape means we cannot safely inspect the credential
            # reference or run lark-cli cleanup against it.  Deleting the
            # directory here would erase the only recovery/audit handle while
            # potentially leaving an external keychain secret behind.
            if not allow_credential_removal:
                raise LarkProfileError(
                    "could not securely inspect imported credential state; retained "
                    f"{path} for owner audit; recover only after it contains a "
                    "valid config matching its recorded App ID and brand"
                ) from exc
            raise LarkProfileError(
                "could not securely inspect staged credential state; retained "
                f"{path}; resolve the unsafe entry and retry with "
                f"'./cow lark recover {path.name}'"
            ) from exc
        if not allow_credential_removal:
            # Existing-app initialization can mutate a process-global keychain
            # before writing config.json or validating the credentials. Even an
            # empty staging tree is therefore an audit handle, not proof that
            # external credential state is untouched.
            raise LarkProfileError(
                "could not safely remove pre-existing app credentials; retained "
                f"{path} for owner audit; recover only if it contains a valid "
                "config matching its recorded App ID and brand"
            )
        try:
            candidate = _config_for_profile(path)
        except LarkProfileError as exc:
            if _unparseable_staging_is_discardable(path):
                _safe_remove_tree(path, root)
                return
            # A non-empty truncated/unsupported config may follow a successful
            # keychain write.  With no validated identity it is unsafe both to
            # invoke ``config remove`` and to erase the only audit handle.
            raise LarkProfileError(
                "could not determine staged credential ownership; retained "
                f"{path}; inspect the private staging state before deleting it"
            ) from exc

        disposition = _StagedCredentialDisposition.UNKNOWN

        async def prove_unowned(store: Any) -> None:
            nonlocal disposition
            disposition = await self._staged_credential_disposition(
                store,
                candidate,
            )

        def finish_rollback() -> None:
            self._run_store(prove_unowned)
            if disposition is _StagedCredentialDisposition.CLEANUP_ALLOWED:
                self._cleanup_new_credentials(binary, path)
            elif disposition is _StagedCredentialDisposition.SHARED:
                _safe_remove_tree(path, root)
            else:
                raise LarkProfileError(
                    f"could not prove staged credential ownership; retained {path}"
                )

        def ownership_conflict(exc: SupervisorOwnershipConflict) -> None:
            # A read-only exact credential match proves this is an extant
            # profile's shared reference; app identity alone is insufficient
            # because the supported file and keychain backends can differ.
            if self._native_read_only:
                try:
                    profiles = self._read_profiles(include_removed=True)
                except LarkProfileStoreUnavailable:
                    profiles = []
                if any(
                    profile.channel == "lark"
                    and profile.bot_id == candidate.app_id
                    and profile.brand.lower() == candidate.brand
                    and profile.credential_ref == candidate.credential_ref
                    and (
                        profile.removed_at is None
                        or Path(profile.config_dir).exists()
                        or Path(profile.config_dir).is_symlink()
                    )
                    for profile in profiles
                ):
                    _safe_remove_tree(path, root)
                    return
            raise LarkProfileError(
                "could not prove staged credential ownership; retained "
                f"{path}; retry with './cow lark recover {path.name}'"
            ) from exc

        if database_owned:
            if account_owned_app_id is not None:
                if account_owned_app_id != candidate.app_id:
                    raise LarkProfileError(
                        f"staged app identity changed during rollback; retained {path}"
                    )
                finish_rollback()
                return
            try:
                with ChannelAccountOwnership(
                    channel="lark",
                    bot_id=candidate.app_id,
                ):
                    finish_rollback()
            except SupervisorOwnershipConflict as exc:
                ownership_conflict(exc)
            return

        try:
            with SupervisorAccountSetOwnership(
                self.database,
                accounts=(("lark", candidate.app_id),),
            ):
                finish_rollback()
        except SupervisorOwnershipConflict as exc:
            ownership_conflict(exc)

    def add(
        self,
        profile_name: str | None = None,
        *,
        app_id: str | None = None,
        brand: str | None = None,
        app_secret_stdin: bool = False,
        owner_open_id: str | None = None,
        without_owner: bool = False,
    ) -> BotProfileRecord:
        profile_id = normalize_profile_name(profile_name, generate=True)
        if owner_open_id is not None and without_owner:
            raise LarkProfileError(
                "--owner-open-id and --without-owner are mutually exclusive"
            )
        # ``without_owner`` is retained as a compatibility spelling for the
        # default: do not create an optional human-account mapping.  Bot
        # profiles themselves are always managed by the local OS owner.
        normalized_owner_open_id = _normalize_owner_open_id(owner_open_id)
        existing_identity = _normalize_add_credentials(
            app_id,
            brand,
            app_secret_stdin,
        )
        owner_mapping = (
            _OwnerMappingMetadata("owner", normalized_owner_open_id)
            if normalized_owner_open_id is not None
            else None
        )
        binary, version = self._verified_binary()
        root = self._root()
        final_path = _contained_child(root, profile_id)

        with CredentialMutationOwnership(root):
            # The existence check belongs under the same filesystem mutation
            # lock as publication; otherwise two owner CLIs can both stage the
            # same profile name and race at os.replace().
            if final_path.exists() or final_path.is_symlink():
                raise LarkProfileError(f"Lark profile already exists: {profile_id}")
            # Own SQLite before creating staging or displaying a QR code.  The
            # lock is held through publication and registration, closing the
            # old race where a live supervisor could leave a newly provisioned
            # credential stranded against a pre-migration database.
            with DatabaseOwnership(self.database):

                async def preflight(store: Any) -> None:
                    for profile in await self._all_lark_profiles(store):
                        if str(_field(profile, "profile_id", "")) == profile_id:
                            raise LarkProfileError(
                                f"Lark profile is already registered: {profile_id}"
                            )
                        if (
                            existing_identity is None
                            or str(_field(profile, "bot_id", ""))
                            != existing_identity[0]
                        ):
                            continue
                        if _field(profile, "removed_at") is None:
                            raise LarkProfileError("duplicate live app id")
                        archived_path = Path(str(_field(profile, "config_dir", "")))
                        if archived_path.exists() or archived_path.is_symlink():
                            raise LarkProfileError(
                                "an archived profile still has pending credential "
                                "cleanup for this app id"
                            )

                self._run_store(preflight)
                # A filesystem actor cannot race this check because credential
                # ownership remains held; retain it after migration as a clear
                # publication fence.
                if final_path.exists() or final_path.is_symlink():
                    raise LarkProfileError(
                        f"Lark profile already exists: {profile_id}"
                    )

                temporary = self._temporary_config(f"add-{profile_id}")
                published = False
                registered = False
                provisioned: ProvisionedConfig | None = None
                record: BotProfileRecord | None = None
                account_ownership: ChannelAccountOwnership | None = None
                config_init_started = False
                preserve_external_credential = existing_identity is not None

                def mark_config_init_started() -> None:
                    nonlocal config_init_started
                    config_init_started = True

                try:
                    if owner_mapping is not None:
                        _write_owner_mapping_metadata(
                            temporary,
                            owner_mapping.open_id,
                        )
                    if existing_identity is not None:
                        # Unlike QR onboarding, the target App ID is known. Own
                        # it before lark-cli can update its process-global
                        # keychain entry.
                        account_ownership = ChannelAccountOwnership(
                            channel="lark",
                            bot_id=existing_identity[0],
                        )
                        account_ownership.acquire()
                        _write_existing_app_metadata(
                            temporary,
                            existing_identity[0],
                            existing_identity[1],
                        )
                    raw = run_config_init(
                        binary,
                        temporary,
                        output=self.output,
                        timeout=_onboard_timeout(),
                        app_id=(
                            existing_identity[0]
                            if existing_identity is not None
                            else None
                        ),
                        brand=(
                            existing_identity[1]
                            if existing_identity is not None
                            else None
                        ),
                        secret_input=(
                            self.input_stream
                            if existing_identity is not None
                            else None
                        ),
                        on_spawn=mark_config_init_started,
                    )
                    _tighten_owned_staging_tree(temporary)
                    provisioned = inspect_provisioned_config(
                        temporary,
                        raw,
                        expected_identity=existing_identity,
                    )
                    if account_ownership is None:
                        account_ownership = ChannelAccountOwnership(
                            channel="lark",
                            bot_id=provisioned.app_id,
                        )
                        account_ownership.acquire()

                    async def reject_duplicate_app(store: Any) -> None:
                        nonlocal preserve_external_credential
                        profiles = await self._all_lark_profiles(store)
                        if self._has_imported_credential_provenance(
                            profiles,
                            app_id=provisioned.app_id,
                        ):
                            preserve_external_credential = True
                        for profile in profiles:
                            if (
                                str(_field(profile, "bot_id", ""))
                                != provisioned.app_id
                            ):
                                continue
                            if _field(profile, "removed_at") is None:
                                raise LarkProfileError("duplicate live app id")
                            archived_path = Path(
                                str(_field(profile, "config_dir", ""))
                            )
                            if archived_path.exists() or archived_path.is_symlink():
                                raise LarkProfileError(
                                    "an archived profile still has pending "
                                    "credential cleanup for this app id"
                                )
                    self._run_store(reject_duplicate_app)
                    provisioned = inspect_bot_identity(
                        binary, temporary, provisioned
                    )
                    if (
                        owner_mapping is not None
                        and owner_mapping.open_id == provisioned.bot_open_id
                    ):
                        raise LarkProfileError(
                            "--owner-open-id identifies the bot, not a human owner"
                        )
                    provisioned = self._secure_and_reinspect_cli_config(
                        temporary,
                        provisioned,
                    )
                    if existing_identity is not None:
                        expected_metadata = _OnboardingMetadata(
                            "existing_app",
                            existing_identity[0],
                            existing_identity[1],
                        )
                        if _read_onboarding_metadata(temporary) != expected_metadata:
                            raise LarkProfileError(
                                "existing-app onboarding metadata changed unexpectedly"
                            )
                    if _read_owner_mapping_metadata(temporary) != owner_mapping:
                        raise LarkProfileError(
                            "owner-mapping metadata changed unexpectedly"
                        )
                    os.replace(temporary, final_path)
                    published = True
                    record = self._profile_record(
                        profile_id,
                        final_path,
                        provisioned,
                        version,
                        credential_origin=(
                            "existing_app"
                            if preserve_external_credential
                            else None
                        ),
                    )

                    async def create(store: Any) -> BotProfileRecord:
                        nonlocal registered
                        created_record = await self._create_registration(
                            store,
                            record,
                            owner_mapping,
                        )
                        # The store method commits before returning.  Fence
                        # that fact before close/output failures can unwind.
                        registered = True
                        return created_record

                    created = self._run_store(create)
                    self.output.write(
                        f"registered Lark profile {profile_id} "
                        f"(local owner-managed; {provisioned.brand}, "
                        f"app {provisioned.app_id})\n"
                    )
                    if owner_mapping is not None:
                        self.output.write(
                            "mapped human Lark account "
                            f"{profile_id}/{owner_mapping.open_id} to canonical "
                            "principal owner\n"
                        )
                    return created
                except BaseException as operation_exc:
                    if not registered and published:
                        if record is None:
                            raise LarkProfileError(
                                "Lark profile registration outcome is unknown; "
                                f"retained {final_path}; retry with './cow lark "
                                f"recover {temporary.name}'"
                            ) from operation_exc
                        try:
                            fence_state, _durable = self._run_store(
                                lambda store: self._registration_fence(
                                    store,
                                    record,
                                    owner_mapping,
                                )
                            )
                        except BaseException:
                            raise LarkProfileError(
                                "could not prove whether Lark profile registration "
                                f"committed; retained {final_path}; retry with "
                                f"'./cow lark recover {temporary.name}'"
                            ) from operation_exc
                        if fence_state == "committed":
                            registered = True
                        elif fence_state == "conflict":
                            raise LarkProfileError(
                                "Lark profile registration has conflicting durable "
                                f"state; retained {final_path}; retry with "
                                f"'./cow lark recover {temporary.name}'"
                            ) from operation_exc
                    if not registered:
                        cleanup_path = temporary
                        if published and final_path.exists() and final_path.is_dir():
                            try:
                                if temporary.exists() or temporary.is_symlink():
                                    raise LarkProfileError(
                                        "add staging path reappeared during rollback"
                                    )
                                os.replace(final_path, temporary)
                                published = False
                            except BaseException as rollback_exc:
                                raise LarkProfileError(
                                    "Lark profile registration failed and its "
                                    "publication could not be restored; retained "
                                    f"{final_path}; retry with './cow lark recover "
                                    f"{temporary.name}'"
                                ) from rollback_exc
                        if cleanup_path.exists() and cleanup_path.is_dir():
                            if not config_init_started:
                                _safe_remove_tree(cleanup_path, root)
                            else:
                                self._rollback_unregistered_config(
                                    binary,
                                    cleanup_path,
                                    database_owned=True,
                                    account_owned_app_id=(
                                        existing_identity[0]
                                        if existing_identity is not None
                                        and account_ownership is not None
                                        and account_ownership.held
                                        else (
                                            provisioned.app_id
                                            if provisioned is not None
                                            and account_ownership is not None
                                            and account_ownership.held
                                            else None
                                        )
                                    ),
                                    allow_credential_removal=(
                                        existing_identity is None
                                    ),
                                )
                    raise
                finally:
                    if account_ownership is not None:
                        account_ownership.close()

    def recover(
        self,
        staging_name: str,
        profile_name: str | None = None,
    ) -> BotProfileRecord:
        """Finalize one retained ``add`` staging directory without another QR.

        Recovery never removes credentials.  It either publishes the exact
        private staged config and commits its durable profile, recognizes an
        already completed identical publication, or leaves the credential
        directory available for a later retry.
        """

        binary, version = self._verified_binary()
        root = self._root()
        with CredentialMutationOwnership(root):
            profile_id, staging_path, final_path = self._recovery_paths(
                staging_name,
                profile_name,
            )
            staging_exists = staging_path.exists() or staging_path.is_symlink()
            final_exists = final_path.exists() or final_path.is_symlink()
            if staging_exists and final_exists:
                raise LarkProfileError(
                    "both retained staging and final profile paths exist; "
                    "recovery made no changes"
                )
            if not staging_exists and not final_exists:
                raise LarkProfileError(
                    f"retained Lark staging directory was not found: {staging_path.name}"
                )

            candidate_path = staging_path if staging_exists else final_path
            if staging_exists:
                _tighten_owned_staging_tree(candidate_path)
            onboarding_metadata = _read_onboarding_metadata(candidate_path)
            owner_mapping = _read_owner_mapping_metadata(candidate_path)
            # This is publication input even when a previous attempt already
            # renamed it.  Reject relocation-unsafe absolute file references.
            provisioned = _config_for_profile(
                candidate_path,
                allow_absolute_file_reference=False,
            )
            if onboarding_metadata is not None and (
                provisioned.app_id,
                provisioned.brand,
            ) != (onboarding_metadata.app_id, onboarding_metadata.brand):
                raise LarkProfileError(
                    "retained existing-app config does not match its requested identity"
                )

            with DatabaseOwnership(self.database):
                with ChannelAccountOwnership(
                    channel="lark",
                    bot_id=provisioned.app_id,
                ):
                    provisioned = inspect_bot_identity(
                        binary,
                        candidate_path,
                        provisioned,
                    )
                    if (
                        owner_mapping is not None
                        and owner_mapping.open_id == provisioned.bot_open_id
                    ):
                        raise LarkProfileError(
                            "retained --owner-open-id identifies the bot, not a "
                            "human owner"
                        )
                    provisioned = self._secure_and_reinspect_cli_config(
                        candidate_path,
                        provisioned,
                    )
                    if (
                        _read_onboarding_metadata(candidate_path)
                        != onboarding_metadata
                    ):
                        raise LarkProfileError(
                            "retained onboarding metadata changed during recovery"
                        )
                    if _read_owner_mapping_metadata(candidate_path) != owner_mapping:
                        raise LarkProfileError(
                            "retained owner-mapping metadata changed during recovery"
                        )
                    preserve_external_credential = onboarding_metadata is not None
                    if not preserve_external_credential:
                        preserve_external_credential = self._run_store(
                            lambda store: self._credential_has_imported_provenance(
                                store,
                                provisioned,
                            )
                        )
                    expected = self._profile_record(
                        profile_id,
                        final_path,
                        provisioned,
                        version,
                        credential_origin=(
                            "existing_app"
                            if preserve_external_credential
                            else None
                        ),
                    )

                    async def inspect_registration(
                        store: Any,
                    ) -> BotProfileRecord | None:
                        exact: BotProfileRecord | None = None
                        for profile in await self._all_lark_profiles(store):
                            candidate_profile_id = str(
                                _field(profile, "profile_id", "")
                            )
                            if candidate_profile_id == profile_id:
                                if self._is_exact_live_profile(profile, expected):
                                    exact = profile
                                    continue
                                if _field(profile, "removed_at") is not None:
                                    raise LarkProfileError(
                                        f"Lark profile ID is archived: {profile_id}; "
                                        "choose a different recovery profile name"
                                    )
                                raise LarkProfileError(
                                    "recovery profile ID conflicts with its durable record"
                                )

                            candidate_path_text = str(
                                _field(profile, "config_dir", "")
                            )
                            candidate_identity = str(
                                _field(profile, "config_dir_identity", "")
                            )
                            if (
                                candidate_path_text == str(final_path)
                                or candidate_identity == _config_identity(final_path)
                            ):
                                raise LarkProfileError(
                                    "recovery final path conflicts with another "
                                    "durable profile"
                                )

                            if (
                                str(_field(profile, "bot_id", ""))
                                != provisioned.app_id
                            ):
                                continue
                            if _field(profile, "removed_at") is None:
                                raise LarkProfileError("duplicate live app id")
                            archived_path = Path(candidate_path_text)
                            if archived_path.exists() or archived_path.is_symlink():
                                raise LarkProfileError(
                                    "an archived profile still has pending "
                                    "credential cleanup for this app id"
                                )
                        if exact is not None:
                            status_method = _store_method(
                                store,
                                "get_bot_profile_status",
                            )
                            status = await _maybe_await(
                                status_method(expected.profile_id)
                            )
                            if status is None:
                                raise LarkProfileStoreUnavailable(
                                    "recovered Lark profile is missing its durable "
                                    "status row"
                                )
                            if owner_mapping is not None:
                                resolver = _store_method(
                                    store,
                                    "resolve_principal_account",
                                )
                                account = await _maybe_await(
                                    resolver(
                                        channel="lark",
                                        bot_id=expected.bot_id,
                                        external_user_id=owner_mapping.open_id,
                                    )
                                )
                                if not self._is_exact_owner_mapping(
                                    account,
                                    expected,
                                    owner_mapping,
                                ):
                                    raise LarkProfileError(
                                        "recovered Lark profile is missing its "
                                        "owner mapping"
                                    )
                        return exact

                    existing = self._run_store(inspect_registration)
                    if existing is not None:
                        if staging_exists:
                            if final_path.exists() or final_path.is_symlink():
                                raise LarkProfileError(
                                    "final profile path appeared during recovery"
                                )
                            os.replace(staging_path, final_path)
                        self.output.write(
                            f"Lark profile {profile_id} was already recovered "
                            f"(app {provisioned.app_id})\n"
                        )
                        return existing

                    moved = False
                    registered = False
                    try:
                        if staging_exists:
                            if final_path.exists() or final_path.is_symlink():
                                raise LarkProfileError(
                                    "final profile path appeared during recovery"
                                )
                            os.replace(staging_path, final_path)
                            moved = True

                        async def create(store: Any) -> BotProfileRecord:
                            nonlocal registered
                            created_record = await self._create_registration(
                                store,
                                expected,
                                owner_mapping,
                            )
                            registered = True
                            return created_record

                        created = self._run_store(create)
                        self.output.write(
                            f"recovered Lark profile {profile_id} "
                            f"(local owner-managed; {provisioned.brand}, "
                            f"app {provisioned.app_id})\n"
                        )
                        if owner_mapping is not None:
                            self.output.write(
                                "mapped human Lark account "
                                f"{profile_id}/{owner_mapping.open_id} to "
                                "canonical principal owner\n"
                            )
                        return created
                    except BaseException as operation_exc:
                        if not registered:
                            try:
                                fence_state, _durable = self._run_store(
                                    lambda store: self._registration_fence(
                                        store,
                                        expected,
                                        owner_mapping,
                                    )
                                )
                            except BaseException:
                                raise LarkProfileError(
                                    "could not prove whether recovery registration "
                                    f"committed; retained {final_path}"
                                ) from operation_exc
                            if fence_state == "committed":
                                registered = True
                            elif fence_state == "conflict":
                                raise LarkProfileError(
                                    "recovery registration has conflicting durable "
                                    f"state; retained {final_path}"
                                ) from operation_exc
                        if moved and not registered:
                            try:
                                if staging_path.exists() or staging_path.is_symlink():
                                    raise LarkProfileError(
                                        "staging path reappeared during recovery rollback"
                                    )
                                os.replace(final_path, staging_path)
                            except BaseException as rollback_exc:
                                raise LarkProfileError(
                                    "recovery registration failed and publication "
                                    f"could not be restored; retained {final_path}"
                                ) from rollback_exc
                        raise

    def list(self) -> list[Any]:
        self._validate_read_only_root()
        if self._native_read_only:
            profiles: list[Any] = self._read_profiles()
        else:
            async def read(store: Any) -> list[Any]:
                method = _store_method(store, "list_bot_profiles")
                values = await _maybe_await(
                    method(channel="lark", include_removed=False)
                )
                return list(values or ())

            # Injected stores are a test/development compatibility boundary.
            # Production always uses the read-only SQLite path above.
            profiles = self._run_store(read)
        if not profiles:
            self.output.write("no Lark profiles registered\n")
            return []
        self.output.write("PROFILE\tBRAND\tAPP ID\tSTATE\n")
        for profile in profiles:
            state = "enabled" if bool(_field(profile, "enabled", False)) else "disabled"
            self.output.write(
                f"{_field(profile, 'profile_id', '')}\t"
                f"{_field(profile, 'brand', '')}\t"
                f"{_field(profile, 'bot_id', '')}\t{state}\n"
            )
        return profiles

    def status(self, profile_name: str | None = None) -> list[tuple[Any, Any]]:
        profile_id = (
            normalize_profile_name(profile_name) if profile_name is not None else None
        )
        self._validate_read_only_root()
        if self._native_read_only:
            rows: list[tuple[Any, Any]] = self._read_profile_statuses(profile_id)
        else:
            async def read(store: Any) -> list[tuple[Any, Any]]:
                if profile_id is None:
                    list_method = _store_method(store, "list_bot_profiles")
                    profiles = list(
                        await _maybe_await(
                            list_method(channel="lark", include_removed=False)
                        )
                        or ()
                    )
                else:
                    get_method = _store_method(store, "get_bot_profile")
                    profile = await _maybe_await(
                        get_method(profile_id, include_removed=False)
                    )
                    profiles = [profile] if profile is not None else []
                status_method = _store_method(store, "get_bot_profile_status")
                result: list[tuple[Any, Any]] = []
                for profile in profiles:
                    status = await _maybe_await(
                        status_method(str(_field(profile, "profile_id", "")))
                    )
                    result.append((profile, status))
                return result

            rows = self._run_store(read)
        if profile_id is not None and not rows:
            raise LarkProfileError(f"unknown Lark profile: {profile_id}")
        if not rows:
            self.output.write("no Lark profiles registered\n")
            return []
        for profile, status in rows:
            enabled = "enabled" if bool(_field(profile, "enabled", False)) else "disabled"
            connection = _field(status, "connection_state", "not_started") if status else "not_started"
            onboarding = _field(status, "onboarding_state", "registered") if status else "registered"
            last_ready = _field(status, "last_ready_at", None) if status else None
            ready_text = (
                last_ready.isoformat()
                if hasattr(last_ready, "isoformat")
                else str(last_ready or "never")
            )
            retry_after = _field(status, "retry_after", None) if status else None
            retry_text = (
                retry_after.isoformat()
                if hasattr(retry_after, "isoformat")
                else str(retry_after or "none")
            )
            self.output.write(
                f"{_field(profile, 'profile_id', '')}: {enabled}, "
                f"onboarding={onboarding}, connection={connection}, "
                f"generation={int(_field(status, 'generation', 0) or 0)}, "
                f"last_ready_at={ready_text}, "
                f"retry_after={retry_text}, "
                f"last_error={_field(status, 'last_error_code', None) or 'none'}, "
                f"app={_field(profile, 'bot_id', '')}\n"
            )
        return rows

    def principal_list(
        self, principal_id: str | None = None
    ) -> list[tuple[Any, Any | None, str]]:
        """List locally managed bots and optional human Lark mappings read-only."""

        selected = normalize_principal_id(principal_id) if principal_id else None
        self._validate_read_only_root()
        rows: list[tuple[Any, Any | None, str]] = []
        profiles: list[Any] = []
        if self._native_read_only:
            connection = _open_runtime_database_read_only(self.database)
            try:
                profiles = [
                    _profile_from_read_only_row(value)
                    for value in connection.execute(
                        "SELECT * FROM bot_profiles "
                        "WHERE channel='lark' AND removed_at IS NULL "
                        "ORDER BY created_at, profile_id"
                    ).fetchall()
                ]
                filters = []
                parameters: list[Any] = []
                if selected is not None:
                    filters.append("p.principal_id=?")
                    parameters.append(selected)
                where = " WHERE " + " AND ".join(filters) if filters else ""
                values = connection.execute(
                    "SELECT p.principal_id, p.display_name, p.enabled, "
                    "p.metadata_json, p.created_at AS principal_created_at, "
                    "p.updated_at AS principal_updated_at, "
                    "pa.principal_account_id, pa.channel, pa.bot_id, "
                    "pa.external_user_id, pa.identifier_kind, "
                    "pa.mapping_revision, pa.active, pa.configured_by, "
                    "pa.created_at AS account_created_at, pa.retired_at, "
                    "bp.profile_id AS mapped_profile_id "
                    "FROM principals AS p "
                    "LEFT JOIN principal_accounts AS pa "
                    "ON pa.principal_id=p.principal_id AND pa.active=1 "
                    "AND pa.channel='lark' "
                    "LEFT JOIN bot_profiles AS bp "
                    "ON bp.channel=pa.channel AND bp.bot_id=pa.bot_id "
                    "AND bp.removed_at IS NULL"
                    + where
                    + " ORDER BY p.principal_id, pa.created_at, "
                    "pa.principal_account_id",
                    parameters,
                ).fetchall()
                for value in values:
                    try:
                        metadata = json.loads(str(value["metadata_json"] or "{}"))
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise LarkProfileStoreUnavailable(
                            "principal contains malformed metadata JSON"
                        ) from exc
                    if not isinstance(metadata, Mapping):
                        raise LarkProfileStoreUnavailable(
                            "principal contains invalid metadata"
                        )
                    principal = PrincipalRecord(
                        principal_id=str(value["principal_id"]),
                        display_name=str(value["display_name"] or ""),
                        enabled=bool(value["enabled"]),
                        metadata=dict(metadata),
                        created_at=text_to_datetime(value["principal_created_at"]),
                        updated_at=text_to_datetime(value["principal_updated_at"]),
                    )
                    account: PrincipalAccountRecord | None = None
                    if value["principal_account_id"] is not None:
                        account = PrincipalAccountRecord(
                            principal_account_id=str(
                                value["principal_account_id"]
                            ),
                            principal_id=principal.principal_id,
                            channel=str(value["channel"]),
                            bot_id=str(value["bot_id"]),
                            external_user_id=str(value["external_user_id"]),
                            identifier_kind=str(value["identifier_kind"]),
                            mapping_revision=int(value["mapping_revision"]),
                            active=bool(value["active"]),
                            configured_by=str(value["configured_by"]),
                            created_at=text_to_datetime(value["account_created_at"]),
                            retired_at=text_to_datetime(value["retired_at"]),
                            principal_enabled=principal.enabled,
                        )
                    rows.append(
                        (principal, account, str(value["mapped_profile_id"] or ""))
                    )
            except LarkProfileStoreUnavailable:
                raise
            except sqlite3.Error as exc:
                raise LarkProfileStoreUnavailable(
                    "could not read canonical principal mappings"
                ) from exc
            finally:
                connection.close()
        else:
            async def read(
                store: Any,
            ) -> tuple[list[tuple[Any, Any | None, str]], list[Any]]:
                principal_method = _store_method(store, "list_principals")
                account_method = _store_method(store, "list_principal_accounts")
                profile_method = _store_method(store, "list_bot_profiles")
                principals = list(await _maybe_await(principal_method()) or ())
                accounts = list(
                    await _maybe_await(
                        account_method(principal_id=selected, active=True)
                    )
                    or ()
                )
                profiles = list(
                    await _maybe_await(
                        profile_method(channel="lark", include_removed=False)
                    )
                    or ()
                )
                profile_by_bot = {
                    str(_field(profile, "bot_id", "")): str(
                        _field(profile, "profile_id", "")
                    )
                    for profile in profiles
                }
                result: list[tuple[Any, Any | None, str]] = []
                for principal in principals:
                    pid = str(_field(principal, "principal_id", ""))
                    if selected is not None and pid != selected:
                        continue
                    mapped = [
                        account
                        for account in accounts
                        if str(_field(account, "principal_id", "")) == pid
                        and str(_field(account, "channel", "")).lower()
                        == "lark"
                    ]
                    if not mapped:
                        result.append((principal, None, ""))
                    for account in mapped:
                        result.append(
                            (
                                principal,
                                account,
                                profile_by_bot.get(
                                    str(_field(account, "bot_id", "")), ""
                                ),
                            )
                        )
                return result, profiles

            rows, profiles = self._run_store(read)
        if selected is not None and not rows:
            raise LarkProfileError(f"unknown canonical principal: {selected}")
        if selected is None:
            self.output.write("OWNER-MANAGED LARK BOT PROFILES\n")
            if profiles:
                self.output.write("MANAGED BY\tPROFILE\tBRAND\tAPP ID\tSTATE\n")
                for profile in profiles:
                    state = (
                        "enabled"
                        if bool(_field(profile, "enabled", False))
                        else "disabled"
                    )
                    self.output.write(
                        "local-owner\t"
                        f"{_field(profile, 'profile_id', '')}\t"
                        f"{_field(profile, 'brand', '')}\t"
                        f"{_field(profile, 'bot_id', '')}\t{state}\n"
                    )
            else:
                self.output.write("no owner-managed Lark bot profiles\n")
            self.output.write("\n")

        self.output.write("HUMAN PRINCIPAL MAPPINGS (OPTIONAL)\n")
        mapped_rows = [row for row in rows if row[1] is not None]
        if not mapped_rows:
            self.output.write("no human Lark account mappings\n")
            return rows
        self.output.write("PRINCIPAL\tSTATE\tPROFILE\tAPP ID\tOPEN ID\tREVISION\n")
        for principal, account, profile_name in mapped_rows:
            self.output.write(
                f"{_field(principal, 'principal_id', '')}\t"
                f"{'enabled' if bool(_field(principal, 'enabled', False)) else 'disabled'}\t"
                f"{profile_name}\t"
                f"{_field(account, 'bot_id', '')}\t"
                f"{_field(account, 'external_user_id', '')}\t"
                f"{_field(account, 'mapping_revision', '')}\n"
            )
        return rows

    def principal_map(
        self,
        principal_id: str,
        profile_name: str,
        open_id: str,
        *,
        display_name: str = "",
    ) -> Any:
        """Create/update one owner principal and map an authenticated open_id."""

        pid = normalize_principal_id(principal_id)
        profile_id = normalize_profile_name(profile_name)
        actor_id = str(open_id or "").strip()
        if not _OPEN_ID.fullmatch(actor_id):
            raise LarkProfileError("Lark principal mapping requires a stable ou_ open_id")
        root = self._root()
        profile_path = _contained_child(root, profile_id)
        with CredentialMutationOwnership(root):
            existing = _config_for_profile(profile_path)
            with SupervisorAccountSetOwnership(
                self.database,
                accounts=(("lark", existing.app_id),),
            ):
                async def map_account(store: Any) -> Any:
                    profile_method = _store_method(store, "get_bot_profile")
                    profile = await _maybe_await(
                        profile_method(profile_id, include_removed=False)
                    )
                    if profile is None:
                        raise LarkProfileError(
                            f"unknown Lark profile: {profile_id}"
                        )
                    if (
                        str(_field(profile, "bot_id", "")) != existing.app_id
                        or Path(str(_field(profile, "config_dir", ""))).resolve()
                        != profile_path.resolve()
                    ):
                        raise LarkProfileError(
                            "profile config identity conflicts with the durable bot profile"
                        )
                    get_principal = _store_method(store, "get_principal")
                    principal = await _maybe_await(get_principal(pid))
                    if principal is None:
                        create = _store_method(store, "create_principal")
                        principal = await _maybe_await(
                            create(
                                principal_id=pid,
                                display_name=str(display_name or ""),
                                enabled=True,
                            )
                        )
                    elif display_name and str(
                        _field(principal, "display_name", "")
                    ) != str(display_name):
                        update = _store_method(store, "update_principal")
                        principal = await _maybe_await(
                            update(pid, display_name=str(display_name))
                        )
                    mapper = _store_method(store, "map_principal_account")
                    return await _maybe_await(
                        mapper(
                            principal_id=pid,
                            channel="lark",
                            bot_id=existing.app_id,
                            external_user_id=actor_id,
                            identifier_kind="open_id",
                            configured_by=_local_owner_identity(),
                        )
                    )

                result = self._run_store(map_account)
        self.output.write(f"mapped {profile_id}/{actor_id} to {pid}\n")
        return result

    def principal_unmap(self, profile_name: str, open_id: str) -> bool:
        profile_id = normalize_profile_name(profile_name)
        actor_id = str(open_id or "").strip()
        if not _OPEN_ID.fullmatch(actor_id):
            raise LarkProfileError("Lark principal mapping requires a stable ou_ open_id")
        profile_path = _contained_child(Path(self.config_root), profile_id)
        # Revocation must remain possible after credential cleanup, partial
        # directory loss, or config corruption.  The mapping is keyed by the
        # immutable durable transport account, so resolving it through
        # config.json would turn damaged credentials into an authorization
        # revocation failure (and needlessly trust attacker-modifiable bytes).
        if self._native_read_only:
            profiles = self._read_profiles(profile_id, include_removed=True)
        else:
            async def read_profile(store: Any) -> list[Any]:
                method = _store_method(store, "get_bot_profile")
                profile = await _maybe_await(
                    method(profile_id, include_removed=True)
                )
                return [] if profile is None else [profile]

            profiles = self._run_store(read_profile)
        profile = profiles[0] if len(profiles) == 1 else None
        app_id = str(_field(profile, "bot_id", ""))
        if (
            profile is None
            or str(_field(profile, "profile_id", "")) != profile_id
            or str(_field(profile, "channel", "")).lower() != "lark"
            or not _APP_ID.fullmatch(app_id)
            or str(_field(profile, "config_dir", "")) != str(profile_path)
            or str(_field(profile, "config_dir_identity", ""))
            != _config_identity(profile_path)
        ):
            raise LarkProfileError(f"unknown or conflicting Lark profile: {profile_id}")

        with SupervisorAccountSetOwnership(
            self.database,
            accounts=(("lark", app_id),),
        ):
            async def unmap_account(store: Any) -> bool:
                profile_method = _store_method(store, "get_bot_profile")
                current = await _maybe_await(
                    profile_method(profile_id, include_removed=True)
                )
                if (
                    current is None
                    or str(_field(current, "profile_id", "")) != profile_id
                    or str(_field(current, "channel", "")).lower() != "lark"
                    or str(_field(current, "bot_id", "")) != app_id
                    or str(_field(current, "config_dir", "")) != str(profile_path)
                    or str(_field(current, "config_dir_identity", ""))
                    != _config_identity(profile_path)
                ):
                    raise LarkProfileError(
                        f"unknown or conflicting Lark profile: {profile_id}"
                    )
                unmap = _store_method(store, "unmap_principal_account")
                return bool(
                    await _maybe_await(
                        unmap(
                            channel="lark",
                            bot_id=app_id,
                            external_user_id=actor_id,
                        )
                    )
                )

            removed = self._run_store(unmap_account)
        if not removed:
            raise LarkProfileError(
                f"no active principal mapping for {profile_id}/{actor_id}"
            )
        self.output.write(f"unmapped {profile_id}/{actor_id}\n")
        return True

    def _mutate_enabled(self, profile_name: str, enabled: bool) -> Any:
        profile_id = normalize_profile_name(profile_name)
        root = self._root()
        with CredentialMutationOwnership(root):
            existing = _config_for_profile(_contained_child(root, profile_id))
            with SupervisorAccountSetOwnership(
                self.database,
                accounts=(("lark", existing.app_id),),
            ):

                async def mutate(store: Any) -> Any:
                    get_method = _store_method(store, "get_bot_profile")
                    profile = await _maybe_await(
                        get_method(profile_id, include_removed=False)
                    )
                    if profile is None:
                        raise LarkProfileError(f"unknown Lark profile: {profile_id}")
                    if str(_field(profile, "bot_id", "")) != existing.app_id:
                        raise LarkProfileError(
                            "profile config identity conflicts with the durable bot profile"
                        )
                    method = _store_method(
                        store,
                        "set_bot_profile_enabled",
                        "update_bot_profile_enabled",
                    )
                    changed = await _maybe_await(method(profile_id, enabled))
                    if changed is False:
                        raise LarkProfileError(f"could not update Lark profile: {profile_id}")
                    # Stores return the updated durable record; narrow legacy
                    # stores may return only a success boolean.
                    return profile if changed is True else changed

                result = self._run_store(mutate)
        self.output.write(f"{profile_id} {'enabled' if enabled else 'disabled'}\n")
        return result

    def disable(self, profile_name: str) -> Any:
        return self._mutate_enabled(profile_name, False)

    def enable(self, profile_name: str) -> Any:
        return self._mutate_enabled(profile_name, True)

    def reauthorize(self, profile_name: str) -> Any:
        profile_id = normalize_profile_name(profile_name)
        binary, version = self._verified_binary()
        root = self._root()
        final_path = _contained_child(root, profile_id)

        with CredentialMutationOwnership(root):
            existing = _config_for_profile(final_path)
            with SupervisorAccountSetOwnership(
                self.database,
                accounts=(("lark", existing.app_id),),
            ):
                durable_before = self._run_store(
                    lambda store: self._reauthorization_preflight(
                        store,
                        profile_id=profile_id,
                        final_path=final_path,
                        existing=existing,
                    )
                )
                try:
                    imported_provenance = self._run_store(
                        lambda store: self._credential_has_imported_provenance(
                            store,
                            existing,
                        )
                    )
                except BaseException as exc:
                    raise LarkProfileError(
                        "could not verify imported credential provenance; QR "
                        "reauthorization did not start"
                    ) from exc
                if imported_provenance:
                    # `config init --new` may overwrite the OS-user-global
                    # keychain entry for this App ID before returning enough
                    # state to validate or roll back. Never mix that QR flow
                    # with a credential that COW imported but does not own.
                    raise LarkProfileError(
                        "existing-app profiles cannot use QR reauthorization; "
                        "remove this COW profile (the external credential is "
                        "retained), then add the same App ID under a new profile "
                        "with --app-secret-stdin"
                    )
                temporary = self._temporary_config(f"reauthorize-{profile_id}")
                backup = _contained_child(root, f".backup-{profile_id}-{uuid.uuid4().hex}")
                swapped = False
                durable_updated = False
                old_retirement_attempted = False
                expected_durable: BotProfileRecord | None = None
                credential_disposition = _StagedCredentialDisposition.UNKNOWN
                retain_uninspectable_temporary = False
                candidate_account_ownership: ChannelAccountOwnership | None = None
                try:
                    raw = run_config_init(
                        binary,
                        temporary,
                        output=self.output,
                        timeout=_onboard_timeout(),
                    )
                    try:
                        _tighten_owned_staging_tree(temporary)
                    except LarkProfileError as exc:
                        retain_uninspectable_temporary = True
                        raise LarkProfileError(
                            "could not securely inspect staged reauthorization "
                            f"credentials; retained {temporary}; resolve the unsafe "
                            "entry before retrying reauthorization"
                        ) from exc
                    provisioned = inspect_provisioned_config(temporary, raw)
                    if provisioned.app_id != existing.app_id:
                        # The outer owner holds the database and original app.
                        # Own a wrong-QR app as well before inspecting or
                        # deleting its process-global keychain reference.
                        candidate_account_ownership = ChannelAccountOwnership(
                            channel="lark",
                            bot_id=provisioned.app_id,
                        )
                        candidate_account_ownership.acquire()
                    credential_disposition = self._run_store(
                        lambda store: self._staged_credential_disposition(
                            store,
                            provisioned,
                        )
                    )

                    verification_error: BaseException | None = None
                    try:
                        verified = inspect_bot_identity(
                            binary,
                            temporary,
                            provisioned,
                        )
                    except BaseException as exc:
                        # A failing diagnostic may still write cache/config
                        # state.  Reinspect after its complete process group is
                        # fenced before deciding whether cleanup is safe.
                        verification_error = exc
                        verified = provisioned
                    try:
                        provisioned = self._secure_and_reinspect_cli_config(
                            temporary,
                            verified,
                        )
                    except LarkProfileError as exc:
                        # Verification is allowed to create CLI cache state,
                        # so the second normalization is a distinct security
                        # fence.  Preserve both unsafe trees and identity-
                        # changed trees for credential audit rather than
                        # deleting the only evidence/retry handle.
                        retain_uninspectable_temporary = True
                        raise LarkProfileError(
                            "could not securely reinspect staged reauthorization "
                            f"credentials; retained {temporary}; inspect the staged "
                            "state before retrying reauthorization"
                        ) from exc
                    if verification_error is not None:
                        raise verification_error
                    if (provisioned.app_id, provisioned.brand) != (
                        existing.app_id,
                        existing.brand,
                    ):
                        raise LarkProfileError(
                            "reauthorization returned a different app identity or brand"
                        )
                    expected_durable = replace(
                        durable_before,
                        credential_ref=provisioned.credential_ref,
                        cli_version=version,
                        restart_policy={
                            **dict(
                                _field(durable_before, "restart_policy", {}) or {}
                            ),
                            "bot_open_id": provisioned.bot_open_id,
                        },
                    )
                    os.replace(final_path, backup)
                    try:
                        os.replace(temporary, final_path)
                    except BaseException:
                        os.replace(backup, final_path)
                        raise
                    swapped = True

                    async def update(store: Any) -> Any:
                        get_method = _store_method(store, "get_bot_profile")
                        profile = await _maybe_await(
                            get_method(profile_id, include_removed=False)
                        )
                        if profile is None or not self._matches_reauthorization_state(
                            profile,
                            durable_before,
                        ):
                            raise LarkProfileError(
                                "durable profile state changed during reauthorization"
                            )
                        method = _store_method(
                            store,
                            "update_bot_profile_credentials",
                            "reauthorize_bot_profile",
                        )
                        updated_record = await _maybe_await(
                            method(
                                profile_id,
                                credential_ref=provisioned.credential_ref,
                                cli_version=version,
                                restart_policy=expected_durable.restart_policy,
                            )
                        )
                        return updated_record

                    self._run_store(update)
                    fence_state, durable_record = self._run_store(
                        lambda store: self._reauthorization_fence(
                            store,
                            before=durable_before,
                            expected=expected_durable,
                        )
                    )
                    if fence_state != "committed" or durable_record is None:
                        raise LarkProfileError(
                            "Lark reauthorization update did not produce its "
                            "expected durable state"
                        )
                    # Only a separate durable readback is a commit fence.  A
                    # legacy store may return False/None, and SQLite executor
                    # cancellation may raise after the transaction commits.
                    durable_updated = True
                    updated = durable_record
                    old_retirement_attempted = True
                    self._retire_reauthorization_backup(
                        binary,
                        backup,
                        existing,
                    )
                except BaseException as operation_exc:
                    if swapped:
                        if not durable_updated:
                            if expected_durable is None:
                                raise LarkProfileError(
                                    "Lark reauthorization outcome is unknown; retained "
                                    f"{final_path} and {backup}"
                                ) from operation_exc
                            try:
                                fence_state, _durable = self._run_store(
                                    lambda store: self._reauthorization_fence(
                                        store,
                                        before=durable_before,
                                        expected=expected_durable,
                                    )
                                )
                            except BaseException:
                                raise LarkProfileError(
                                    "could not prove whether Lark reauthorization "
                                    f"committed; retained {final_path} and {backup}"
                                ) from operation_exc
                            if fence_state == "committed":
                                durable_updated = True
                            elif fence_state == "conflict":
                                raise LarkProfileError(
                                    "Lark reauthorization has conflicting durable "
                                    f"state; retained {final_path} and {backup}"
                                ) from operation_exc
                        if durable_updated:
                            # SQLite and the published config both name the new
                            # credentials.  The old tree still needs an
                            # ownership-classified retirement: a backend switch
                            # may otherwise orphan its keychain secret.
                            if not old_retirement_attempted:
                                old_retirement_attempted = True
                                self._retire_reauthorization_backup(
                                    binary,
                                    backup,
                                    existing,
                                )
                        else:
                            failed_new = _contained_child(
                                root, f".failed-{profile_id}-{uuid.uuid4().hex}"
                            )
                            try:
                                os.replace(final_path, failed_new)
                                os.replace(backup, final_path)
                            except BaseException as rollback_exc:
                                raise LarkProfileError(
                                    "Lark reauthorization failed and config rollback "
                                    f"could not be completed; retained {failed_new} "
                                    f"and {backup}"
                                ) from rollback_exc
                            if (
                                credential_disposition
                                is _StagedCredentialDisposition.CLEANUP_ALLOWED
                            ):
                                self._cleanup_new_credentials(binary, failed_new)
                            elif (
                                credential_disposition
                                is _StagedCredentialDisposition.SHARED
                            ):
                                _safe_remove_tree(failed_new, root)
                            else:
                                raise LarkProfileError(
                                    "could not prove failed reauthorization credential "
                                    f"ownership; restored {final_path} and retained "
                                    f"{failed_new}"
                                ) from operation_exc
                    elif temporary.exists() and temporary.is_dir():
                        if retain_uninspectable_temporary:
                            # The exact staged tree could not be traversed
                            # safely, so neither credential cleanup nor tree
                            # deletion has a trustworthy target.
                            pass
                        elif (
                            credential_disposition
                            is _StagedCredentialDisposition.CLEANUP_ALLOWED
                        ):
                            self._cleanup_new_credentials(binary, temporary)
                        elif (
                            credential_disposition
                            is _StagedCredentialDisposition.SHARED
                        ):
                            _safe_remove_tree(temporary, root)
                        else:
                            raise LarkProfileError(
                                "could not prove staged reauthorization credential "
                                f"ownership; retained {temporary}"
                            ) from operation_exc
                    raise
                finally:
                    if candidate_account_ownership is not None:
                        candidate_account_ownership.close()
        self.output.write(f"{profile_id} reauthorized\n")
        return updated

    def remove(self, profile_name: str) -> bool:
        profile_id = normalize_profile_name(profile_name)
        binary, _version = self._verified_binary()
        root = self._root()
        final_path = _contained_child(root, profile_id)

        with CredentialMutationOwnership(root):
            config_valid = True
            try:
                existing = _config_for_profile(final_path)
            except LarkProfileError as config_error:
                # A previous `config remove` may have removed config.json and
                # then reported a backend error, or directory cleanup itself
                # may have failed.  Resolve only an already-archived row and
                # require both exact path attestations before touching its
                # directory.  A live profile still requires a valid config.
                if self._native_read_only:
                    archived_rows = self._read_profiles(
                        profile_id, include_removed=True
                    )
                else:
                    async def read_archived(store: Any) -> list[Any]:
                        method = _store_method(store, "get_bot_profile")
                        profile = await _maybe_await(
                            method(profile_id, include_removed=True)
                        )
                        return [] if profile is None else [profile]

                    archived_rows = self._run_store(read_archived)
                archived = archived_rows[0] if len(archived_rows) == 1 else None
                exact_archived_path = bool(
                    archived is not None
                    and _field(archived, "removed_at") is not None
                    and str(_field(archived, "channel", "")) == "lark"
                    and str(_field(archived, "config_dir", "")) == str(final_path)
                    and str(_field(archived, "config_dir_identity", ""))
                    == _config_identity(final_path)
                )
                archived_app_id = str(_field(archived, "bot_id", ""))
                archived_brand = str(_field(archived, "brand", "")).lower()
                if (
                    not exact_archived_path
                    or not _APP_ID.fullmatch(archived_app_id)
                    or archived_brand not in {"lark", "feishu"}
                ):
                    raise config_error
                existing = ProvisionedConfig(
                    app_id=archived_app_id,
                    brand=archived_brand,
                    credential_ref=str(_field(archived, "credential_ref", "")),
                )
                config_valid = False
            with SupervisorAccountSetOwnership(
                self.database,
                accounts=(("lark", existing.app_id),),
            ):

                async def remove_profile(store: Any) -> tuple[bool, bool, bool]:
                    get_method = _store_method(store, "get_bot_profile")
                    live_profile = await _maybe_await(
                        get_method(profile_id, include_removed=False)
                    )
                    profile = live_profile
                    if profile is None:
                        profile = await _maybe_await(
                            get_method(profile_id, include_removed=True)
                        )
                    if profile is None:
                        raise LarkProfileError(f"unknown Lark profile: {profile_id}")
                    if (
                        str(_field(profile, "channel", "")) != "lark"
                        or str(_field(profile, "bot_id", "")) != existing.app_id
                        or str(_field(profile, "brand", "")) != existing.brand
                        or str(_field(profile, "credential_ref", ""))
                        != existing.credential_ref
                    ):
                        raise LarkProfileError(
                            "profile config identity conflicts with the durable bot profile"
                        )
                    if (
                        str(_field(profile, "config_dir", "")) != str(final_path)
                        or str(_field(profile, "config_dir_identity", ""))
                        != _config_identity(final_path)
                    ):
                        raise LarkProfileError(
                            "durable profile path conflicts with the requested config directory"
                        )
                    list_method = _store_method(store, "list_bot_profiles")
                    all_profiles = list(
                        await _maybe_await(
                            list_method(channel="lark", include_removed=True)
                        )
                        or ()
                    )
                    preserve_external_credential = (
                        self._has_imported_credential_provenance(
                            all_profiles,
                            app_id=existing.app_id,
                        )
                    )
                    replacement_owns_credential = any(
                        str(_field(candidate, "profile_id", "")) != profile_id
                        and _field(candidate, "removed_at") is None
                        and str(_field(candidate, "channel", "")) == "lark"
                        and str(_field(candidate, "bot_id", "")) == existing.app_id
                        and str(_field(candidate, "brand", "")) == existing.brand
                        and str(_field(candidate, "credential_ref", ""))
                        == existing.credential_ref
                        for candidate in all_profiles
                    )
                    # The durable soft-remove intentionally commits before the
                    # external credential cleanup.  If that cleanup failed on
                    # an earlier invocation, a second `remove` must resume here
                    # instead of treating the archived row as unknown.
                    if live_profile is None:
                        return (
                            True,
                            replacement_owns_credential,
                            preserve_external_credential,
                        )
                    method = _store_method(
                        store, "remove_bot_profile", "delete_bot_profile"
                    )
                    removed = await _maybe_await(method(profile_id))
                    return (
                        bool(removed),
                        replacement_owns_credential,
                        preserve_external_credential,
                    )

                (
                    removed,
                    replacement_owns_credential,
                    preserve_external_credential,
                ) = self._run_store(remove_profile)
                if not removed:
                    raise LarkProfileError(f"could not remove Lark profile: {profile_id}")
                if not config_valid and not preserve_external_credential:
                    # Once config.json is unavailable, an earlier non-zero
                    # cleanup may nevertheless have changed an external
                    # keychain or file credential before reporting failure.
                    # The private directory and archived row are the remaining
                    # audit/recovery handle; deleting them would turn that
                    # ambiguous outcome into a false claim of completion.
                    raise LarkProfileError(
                        "profile remains disabled in SQLite but lark-cli credential "
                        "cleanup outcome is unknown because config is unavailable; "
                        f"retained {final_path}"
                    )
                # The durable soft-remove commits first.  Destructive CLI
                # cleanup uses the same restorable snapshot protocol as add
                # rollback and post-reauthorization retirement.
                if (
                    not replacement_owns_credential
                    and not preserve_external_credential
                ):
                    try:
                        self._cleanup_new_credentials(binary, final_path)
                    except LarkProfileError as exc:
                        raise LarkProfileError(
                            "profile was disabled in SQLite but lark-cli credential "
                            f"cleanup failed; retained {final_path}"
                        ) from exc
                else:
                    _safe_remove_tree(final_path, root)
        if preserve_external_credential:
            self.output.write(
                f"{profile_id} removed; existing app credential retained\n"
            )
        else:
            self.output.write(f"{profile_id} removed\n")
        return True


def cleanup_unowned_provisioned_config(
    binary: str,
    config_root: Path,
    path: Path,
) -> None:
    """Remove one caller-proven unowned staged credential safely.

    This is the database-free facade over the terminal administrator's
    snapshot-and-restore cleanup protocol.  A live caller must already hold
    the exact app's account ownership and prove from its existing store that
    no durable profile uses the credential.  This function never opens SQLite
    or acquires a database/account lock.
    """

    root = _ensure_private_directory(
        Path(config_root).expanduser().absolute(),
        create=False,
    )
    candidate = Path(path)
    if candidate.parent != root:
        raise LarkProfileError(
            "staged credential cleanup path is outside the config root"
        )
    # ``_cleanup_new_credentials`` consults only the configured root and the
    # supplied binary.  Pass an inert path explicitly so even a future default
    # database-path resolver cannot couple this live primitive to production
    # SQLite merely during construction.
    admin = LarkProfileAdmin(
        database=root / ".unused-live-onboarding.sqlite3",
        config_root=root,
        binary=binary,
    )
    admin._cleanup_new_credentials(binary, candidate)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="./cow lark",
        description="Manage isolated Lark/Feishu bot profiles.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    add = commands.add_parser(
        "add",
        help="add a bot profile by QR or existing app credentials",
    )
    add.add_argument("profile_name", nargs="?")
    add.add_argument(
        "--app-id",
        help="existing Lark/Feishu App ID (the ./cow launcher prompts for its secret)",
    )
    add.add_argument(
        "--brand",
        choices=("feishu", "lark"),
        help="existing app platform (defaults to feishu)",
    )
    add.add_argument(
        "--app-secret-stdin",
        action="store_true",
        help="automation mode: read the existing app's secret from standard input",
    )
    owner_selection = add.add_mutually_exclusive_group()
    owner_selection.add_argument(
        "--owner-open-id",
        help=(
            "optionally map this app-scoped human ou_ open_id to canonical "
            "principal 'owner'; bot ownership does not require it"
        ),
    )
    owner_selection.add_argument(
        "--without-owner",
        action="store_true",
        help=(
            "compatibility no-op: omit the optional human account mapping "
            "(the bot remains local owner-managed)"
        ),
    )
    recover = commands.add_parser(
        "recover",
        help="finalize credentials retained by an interrupted add",
    )
    recover.add_argument("staging_name")
    recover.add_argument("profile_name", nargs="?")
    commands.add_parser("list", help="list registered bot profiles")
    status = commands.add_parser("status", help="show profile connection status")
    status.add_argument("profile_name", nargs="?")
    principal = commands.add_parser(
        "principal",
        help="show local bot ownership and manage canonical human mappings",
    )
    principal_commands = principal.add_subparsers(
        dest="principal_command", required=True
    )
    principal_list = principal_commands.add_parser(
        "list", help="list owned Lark bots and optional human account mappings"
    )
    principal_list.add_argument("principal_id", nargs="?")
    principal_map = principal_commands.add_parser(
        "map", help="map one profile-local Lark open_id to a principal"
    )
    principal_map.add_argument("principal_id")
    principal_map.add_argument("profile_name")
    principal_map.add_argument("open_id")
    principal_map.add_argument("--display-name", default="")
    principal_unmap = principal_commands.add_parser(
        "unmap", help="retire one active profile-local open_id mapping"
    )
    principal_unmap.add_argument("profile_name")
    principal_unmap.add_argument("open_id")
    for name, help_text in (
        ("reauthorize", "repeat registration for a QR-provisioned profile"),
        ("disable", "stop a profile from starting"),
        ("enable", "allow a profile to start"),
        ("remove", "soft-remove a profile and delete its local credentials"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("profile_name")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    admin = LarkProfileAdmin()
    try:
        if arguments.command == "add":
            admin.add(
                arguments.profile_name,
                app_id=arguments.app_id,
                brand=arguments.brand,
                app_secret_stdin=arguments.app_secret_stdin,
                owner_open_id=arguments.owner_open_id,
                without_owner=arguments.without_owner,
            )
        elif arguments.command == "recover":
            admin.recover(arguments.staging_name, arguments.profile_name)
        elif arguments.command == "list":
            admin.list()
        elif arguments.command == "status":
            admin.status(arguments.profile_name)
        elif arguments.command == "principal":
            if arguments.principal_command == "list":
                admin.principal_list(arguments.principal_id)
            elif arguments.principal_command == "map":
                admin.principal_map(
                    arguments.principal_id,
                    arguments.profile_name,
                    arguments.open_id,
                    display_name=arguments.display_name,
                )
            elif arguments.principal_command == "unmap":
                admin.principal_unmap(
                    arguments.profile_name,
                    arguments.open_id,
                )
            else:  # pragma: no cover - argparse owns nested validation
                raise LarkProfileError(
                    f"unknown principal command: {arguments.principal_command}"
                )
        elif arguments.command == "reauthorize":
            admin.reauthorize(arguments.profile_name)
        elif arguments.command == "disable":
            admin.disable(arguments.profile_name)
        elif arguments.command == "enable":
            admin.enable(arguments.profile_name)
        elif arguments.command == "remove":
            admin.remove(arguments.profile_name)
        else:  # pragma: no cover - argparse owns command validation
            raise LarkProfileError(f"unknown command: {arguments.command}")
    except SupervisorOwnershipConflict as exc:
        print(
            f"error: {exc}; stop the running ./cow supervisor and retry",
            file=sys.stderr,
        )
        return 1
    except LarkProfileError as exc:
        print(f"error: {_public_text(exc)}", file=sys.stderr)
        return 1
    except StoreError as exc:
        print(f"error: {_public_text(exc)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_ONBOARD_TIMEOUT_SECONDS",
    "LARK_CONFIG_ENV",
    "LarkCliVersionError",
    "LarkProfileAdmin",
    "LarkProfileError",
    "LarkProfileStoreUnavailable",
    "MAX_APP_SECRET_BYTES",
    "MAX_INIT_OUTPUT_BYTES",
    "PINNED_LARK_CLI_VERSION",
    "ProvisionedConfig",
    "build_lark_profile_record",
    "build_parser",
    "cleanup_unowned_provisioned_config",
    "durable_database_path",
    "inspect_app_owner_identity",
    "inspect_bot_identity",
    "inspect_provisioned_config",
    "lark_config_root",
    "lark_onboarding_timeout",
    "main",
    "normalize_profile_name",
    "normalize_principal_id",
    "resolve_lark_cli_binary",
    "run_config_init",
    "secure_and_reinspect_provisioned_config",
    "verify_lark_cli_version",
]
