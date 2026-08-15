"""Canonical session-role normalization and immutable snapshot helpers.

The channel command owns parsing, while this module owns the bytes that may be
persisted or passed to an Agent runtime.  Keeping normalization and hashing in
one dependency-free module prevents the command, SQLite store, and SDK adapter
from accepting subtly different role values.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

import unicodedata2 as unicodedata


ROLE_NORMALIZATION_VERSION = "role-normalization-v1"
ROLE_PERSONA_COMPOSITION_VERSION = "role-persona-v1"
ROLE_MAX_SCALARS = 4_000
ROLE_MAX_UTF8_BYTES = 16 * 1024

# Unicode 15.1 White_Space property.  Do not use ``str.strip`` here: its
# behavior follows the Python runtime's bundled Unicode database rather than
# this command contract.
_WHITE_SPACE = frozenset(
    {
        "\u0009",
        "\u000a",
        "\u000b",
        "\u000c",
        "\u000d",
        "\u0020",
        "\u0085",
        "\u00a0",
        "\u1680",
        "\u2000",
        "\u2001",
        "\u2002",
        "\u2003",
        "\u2004",
        "\u2005",
        "\u2006",
        "\u2007",
        "\u2008",
        "\u2009",
        "\u200a",
        "\u2028",
        "\u2029",
        "\u202f",
        "\u205f",
        "\u3000",
    }
)


class RoleValidationError(ValueError):
    """A role cannot be represented by the versioned public contract."""


def _framed_hash(namespace: str, *values: Any) -> str:
    payload = json.dumps(
        [namespace, *(str(value) for value in values)],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _trim_unicode_whitespace(value: str) -> str:
    start = 0
    end = len(value)
    while start < end and value[start] in _WHITE_SPACE:
        start += 1
    while end > start and value[end - 1] in _WHITE_SPACE:
        end -= 1
    return value[start:end]


def normalize_role_text(value: Any) -> str:
    """Return canonical v1 role text or raise :class:`RoleValidationError`.

    CRLF and lone CR become LF; outer Unicode White_Space is removed; internal
    tabs, LF, and spacing are preserved.  Other C0/C1 controls and surrogate
    code points are rejected before persistence.
    """

    if not isinstance(value, str):
        raise RoleValidationError("role must be text")
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    normalized = unicodedata.normalize("NFC", normalized)
    normalized = _trim_unicode_whitespace(normalized)
    for character in normalized:
        codepoint = ord(character)
        if 0xD800 <= codepoint <= 0xDFFF:
            raise RoleValidationError("role contains malformed Unicode")
        if (codepoint < 0x20 or 0x7F <= codepoint <= 0x9F) and character not in {
            "\t",
            "\n",
        }:
            raise RoleValidationError("role contains unsupported control characters")
    if len(normalized) > ROLE_MAX_SCALARS:
        raise RoleValidationError(
            f"role exceeds {ROLE_MAX_SCALARS} Unicode characters"
        )
    try:
        encoded = normalized.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise RoleValidationError("role contains malformed Unicode") from exc
    if len(encoded) > ROLE_MAX_UTF8_BYTES:
        raise RoleValidationError(
            f"role exceeds {ROLE_MAX_UTF8_BYTES} UTF-8 bytes"
        )
    return normalized


def is_default_role_token(value: str) -> bool:
    """Return whether canonical text is the reserved ASCII token ``default``."""

    return value.lower() == "default"


def role_content_hash(
    *,
    normalization_version: str,
    kind: str,
    normalized_content: str,
) -> str:
    return _framed_hash(
        "session-agent-role-content",
        normalization_version,
        kind,
        normalized_content,
    )


def role_snapshot_hash(
    *,
    role_version: int,
    normalization_version: str,
    kind: str,
    normalized_content: str,
    content_hash: str,
    persona_composition_version: str,
) -> str:
    return _framed_hash(
        "session-agent-role-snapshot",
        role_version,
        normalization_version,
        kind,
        normalized_content,
        content_hash,
        persona_composition_version,
    )


def build_role_snapshot(
    *,
    role_version: int,
    kind: str,
    normalized_content: str,
    normalization_version: str = ROLE_NORMALIZATION_VERSION,
    persona_composition_version: str = ROLE_PERSONA_COMPOSITION_VERSION,
) -> dict[str, Any]:
    """Build and validate one child-safe immutable role snapshot."""

    try:
        version = int(role_version)
    except (TypeError, ValueError) as exc:
        raise RoleValidationError("role version must be a nonnegative integer") from exc
    if version < 0:
        raise RoleValidationError("role version must be a nonnegative integer")
    if normalization_version != ROLE_NORMALIZATION_VERSION:
        raise RoleValidationError("unsupported role normalization version")
    if persona_composition_version != ROLE_PERSONA_COMPOSITION_VERSION:
        raise RoleValidationError("unsupported role persona composition version")
    canonical_kind = str(kind or "").strip().lower()
    if canonical_kind not in {"default", "custom"}:
        raise RoleValidationError("role kind must be default or custom")
    canonical_content = normalize_role_text(normalized_content)
    if canonical_kind == "default":
        if canonical_content:
            raise RoleValidationError("default role content must be empty")
    elif not canonical_content:
        raise RoleValidationError("custom role content must not be empty")
    elif is_default_role_token(canonical_content):
        raise RoleValidationError("default is reserved for clearing the role")
    content_digest = role_content_hash(
        normalization_version=normalization_version,
        kind=canonical_kind,
        normalized_content=canonical_content,
    )
    snapshot_digest = role_snapshot_hash(
        role_version=version,
        normalization_version=normalization_version,
        kind=canonical_kind,
        normalized_content=canonical_content,
        content_hash=content_digest,
        persona_composition_version=persona_composition_version,
    )
    return {
        "role_version": version,
        "normalization_version": normalization_version,
        "kind": canonical_kind,
        "normalized_content": canonical_content,
        "content_hash": content_digest,
        "persona_composition_version": persona_composition_version,
        "snapshot_hash": snapshot_digest,
    }


def implicit_default_role() -> dict[str, Any]:
    return build_role_snapshot(
        role_version=0,
        kind="default",
        normalized_content="",
    )


def validate_role_snapshot(value: Any) -> dict[str, Any]:
    """Validate stored/IPC role data and return its canonical projection."""

    if not isinstance(value, Mapping):
        raise RoleValidationError("role snapshot must be a mapping")
    required = {
        "role_version",
        "normalization_version",
        "kind",
        "normalized_content",
        "content_hash",
        "persona_composition_version",
        "snapshot_hash",
    }
    if not required.issubset(value):
        raise RoleValidationError("role snapshot is incomplete")
    canonical = build_role_snapshot(
        role_version=value["role_version"],
        normalization_version=str(value["normalization_version"]),
        kind=str(value["kind"]),
        normalized_content=value["normalized_content"],
        persona_composition_version=str(value["persona_composition_version"]),
    )
    # Snapshot envelopes are already canonical data, not raw command input.
    # Accepting values that merely *normalize* to the hashed projection would
    # let different bytes claim the same immutable role identity.
    if type(value["role_version"]) is not int:  # bool is not a canonical int.
        raise RoleValidationError("role version is not canonical")
    for field in (
        "normalization_version",
        "kind",
        "normalized_content",
        "persona_composition_version",
    ):
        if not isinstance(value[field], str) or value[field] != canonical[field]:
            raise RoleValidationError(f"role {field} is not canonical")
    if (
        not isinstance(value["content_hash"], str)
        or value["content_hash"] != canonical["content_hash"]
    ):
        raise RoleValidationError("role content hash does not match")
    if (
        not isinstance(value["snapshot_hash"], str)
        or value["snapshot_hash"] != canonical["snapshot_hash"]
    ):
        raise RoleValidationError("role snapshot hash does not match")
    return canonical


def role_binding_key(value: Any) -> tuple[int, str, str]:
    snapshot = validate_role_snapshot(value)
    return (
        int(snapshot["role_version"]),
        str(snapshot["snapshot_hash"]),
        str(snapshot["persona_composition_version"]),
    )


__all__ = [
    "ROLE_MAX_SCALARS",
    "ROLE_MAX_UTF8_BYTES",
    "ROLE_NORMALIZATION_VERSION",
    "ROLE_PERSONA_COMPOSITION_VERSION",
    "RoleValidationError",
    "build_role_snapshot",
    "implicit_default_role",
    "is_default_role_token",
    "normalize_role_text",
    "role_binding_key",
    "role_content_hash",
    "role_snapshot_hash",
    "validate_role_snapshot",
]
