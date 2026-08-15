"""Trusted skill metadata and channel syntax helpers.

The Codex SDK owns skill discovery and loading.  This module only normalizes
the public metadata at the runtime/channel boundary so a task can carry an
immutable reference without accepting skill instructions from a user message.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


_SKILL_NAME_RE = re.compile(
    # Keep the selector deliberately narrower than a shell variable or path.
    # The public syntax in plan.md is ``[A-Za-z][A-Za-z0-9_-]*``; accepting
    # punctuation here would make malformed selectors look like distinct
    # registry IDs and would complicate deterministic error handling.
    r"^\$([A-Za-z][A-Za-z0-9_-]*)(?:\s+([\s\S]*))?$"
)
_MAX_SKILLS_HELP = 6000
_PATH_FRAGMENT_RE = re.compile(r"(?<![A-Za-z0-9_])/(?:[^\s`]+)")


def _as_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"false", "0", "no", "off", "disabled"}:
            return False
        if normalized in {"true", "1", "yes", "on", "enabled"}:
            return True
    return bool(value)


class SkillSyntaxError(ValueError):
    """Raised when a leading dollar expression is not a skill invocation."""


class SkillBundleError(ValueError):
    """Raised when a local skill bundle cannot be hashed safely."""


@dataclass(frozen=True, slots=True)
class SkillInvocation:
    """One parsed ``$skill task`` request."""

    name: str
    description: str

    @property
    def skill_id(self) -> str:
        return self.name.casefold()


@dataclass(frozen=True, slots=True)
class SkillDefinition:
    """Public and execution metadata for one trusted skill version."""

    name: str
    path: str
    description: str = ""
    display_name: str = ""
    version: str = ""
    content_hash: str = ""
    enabled: bool = True

    def __post_init__(self) -> None:
        name = str(self.name or "").strip()
        path = str(self.path or "").strip()
        if not name:
            raise ValueError("skill metadata is missing a name")
        if not path:
            raise ValueError(f"skill {name!r} is missing a trusted path")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "description", str(self.description or "").strip())
        object.__setattr__(self, "display_name", str(self.display_name or "").strip())
        object.__setattr__(self, "version", str(self.version or "").strip())
        object.__setattr__(self, "content_hash", str(self.content_hash or "").strip().lower())
        object.__setattr__(self, "enabled", _as_bool(self.enabled))

    @property
    def public_name(self) -> str:
        return self.display_name or self.name

    @property
    def skill_id(self) -> str:
        return self.name.casefold()

    def snapshot(self) -> dict[str, Any]:
        """Return the complete trusted reference persisted on a task."""

        return {
            "name": self.name,
            "skill_id": self.skill_id,
            "path": self.path,
            "display_name": self.display_name,
            "description": self.description,
            "version": self.version,
            "content_hash": self.content_hash,
            "enabled": bool(self.enabled),
        }

    def public_snapshot(self) -> dict[str, Any]:
        """Return fields safe for a user-visible skill listing."""

        return {
            "skill_id": self.skill_id,
            "display_name": self.public_name,
            "description": self.description,
            "summary": self.description,
            "version": self.version,
            "enabled": bool(self.enabled),
        }

    @classmethod
    def from_value(cls, value: Any) -> "SkillDefinition":
        """Normalize SDK/Pydantic/dict skill metadata.

        SDK versions use both snake_case and camelCase aliases.  Only the
        trusted metadata fields are copied; arbitrary channel payload keys are
        intentionally ignored.
        """

        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data: Mapping[str, Any] = value
        elif hasattr(value, "model_dump"):
            try:
                data = value.model_dump(by_alias=True, exclude_none=True)
            except TypeError:
                data = value.model_dump()
        elif hasattr(value, "to_dict"):
            data = value.to_dict()
        else:
            data = {
                name: getattr(value, name)
                for name in (
                    "name",
                    "skill_id",
                    "id",
                    "path",
                    "skill_path",
                    "description",
                    "summary",
                    "short_description",
                    "shortDescription",
                    "version",
                    "content_hash",
                    "hash",
                    "enabled",
                    "interface",
                )
                if hasattr(value, name)
            }
        interface = data.get("interface") if isinstance(data, Mapping) else None
        if hasattr(interface, "model_dump"):
            try:
                interface = interface.model_dump(by_alias=True, exclude_none=True)
            except TypeError:
                interface = interface.model_dump()
        if not isinstance(interface, Mapping):
            interface = {}
        name = (
            data.get("name")
            or data.get("skill_id")
            or data.get("skillId")
            or data.get("id")
            or ""
        )
        path = data.get("path") or data.get("skill_path") or ""
        description = (
            interface.get("shortDescription")
            or interface.get("short_description")
            or data.get("shortDescription")
            or data.get("short_description")
            or data.get("summary")
            or data.get("description")
            or ""
        )
        display_name = (
            data.get("display_name")
            or data.get("displayName")
            or interface.get("displayName")
            or interface.get("display_name")
            or ""
        )
        version = data.get("version") or data.get("skill_version") or ""
        content_hash = data.get("content_hash") or data.get("contentHash") or data.get("hash") or ""
        enabled = data.get("enabled", True)
        # A model enum/path object should remain an opaque trusted value, but
        # normalize it to text before it enters SQLite JSON.
        return cls(
            name=str(name or ""),
            path=str(path or ""),
            description=str(description or ""),
            display_name=str(display_name or ""),
            version=str(version or ""),
            content_hash=str(content_hash or ""),
            enabled=_as_bool(enabled),
        )


_SKILL_BUNDLE_HASH_DOMAIN = b"codex-on-wechat:skill-bundle:v1\x00"
_HASH_CHUNK_SIZE = 1024 * 1024


def _stat_identity(value: os.stat_result) -> tuple[int, int, int]:
    return (value.st_dev, value.st_ino, stat.S_IFMT(value.st_mode))


def _stat_snapshot(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    """Return fields that reveal replacement or mutation during traversal."""

    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _hash_bundle_record(
    digest: Any,
    kind: bytes,
    relative_parts: tuple[str, ...],
    mode: int,
) -> None:
    relative = b"/".join(os.fsencode(part) for part in relative_parts)
    digest.update(kind)
    digest.update(len(relative).to_bytes(8, "big"))
    digest.update(relative)
    digest.update(stat.S_IMODE(mode).to_bytes(4, "big"))


def _directory_snapshot(directory_fd: int) -> list[tuple[str, os.stat_result]]:
    try:
        with os.scandir(directory_fd) as entries:
            snapshot = [
                (entry.name, entry.stat(follow_symlinks=False)) for entry in entries
            ]
    except FileNotFoundError as exc:
        raise SkillBundleError("skill bundle changed while it was hashed") from exc
    snapshot.sort(key=lambda item: os.fsencode(item[0]))
    return snapshot


def _open_flags(*, directory: bool = False) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    return flags


def _open_bundle_entry(
    name: str,
    *,
    directory_fd: int,
    expected: os.stat_result,
    directory: bool,
) -> int:
    try:
        opened_fd = os.open(
            name,
            _open_flags(directory=directory),
            dir_fd=directory_fd,
        )
    except FileNotFoundError as exc:
        raise SkillBundleError("skill bundle changed while it was hashed") from exc
    except OSError as exc:
        # Linux reports ELOOP for an O_NOFOLLOW symlink.  Avoid importing an
        # errno-specific policy by checking the entry again without following
        # it; replacement is rejected below even on platforms without the flag.
        try:
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError:
            raise SkillBundleError("skill bundle changed while it was hashed") from exc
        if stat.S_ISLNK(current.st_mode):
            raise SkillBundleError("skill bundle must not contain symlinks") from exc
        raise
    opened = os.fstat(opened_fd)
    if _stat_identity(opened) != _stat_identity(expected):
        os.close(opened_fd)
        raise SkillBundleError("skill bundle changed while it was hashed")
    return opened_fd


def _hash_bundle_file(
    digest: Any,
    file_fd: int,
    relative_parts: tuple[str, ...],
) -> None:
    before = os.fstat(file_fd)
    if not stat.S_ISREG(before.st_mode):
        raise SkillBundleError("skill bundle contains a non-regular file")
    _hash_bundle_record(digest, b"F", relative_parts, before.st_mode)
    digest.update(before.st_size.to_bytes(8, "big"))
    total = 0
    while True:
        chunk = os.read(file_fd, _HASH_CHUNK_SIZE)
        if not chunk:
            break
        total += len(chunk)
        digest.update(chunk)
    after = os.fstat(file_fd)
    if total != before.st_size or _stat_snapshot(after) != _stat_snapshot(before):
        raise SkillBundleError("skill bundle changed while it was hashed")


def _hash_bundle_directory(
    digest: Any,
    directory_fd: int,
    relative_parts: tuple[str, ...],
    *,
    require_manifest: bool = False,
) -> None:
    before = os.fstat(directory_fd)
    entries = _directory_snapshot(directory_fd)
    if require_manifest:
        manifest = next((item for item in entries if item[0] == "SKILL.md"), None)
        if manifest is None or not stat.S_ISREG(manifest[1].st_mode):
            raise SkillBundleError("skill bundle is missing a regular SKILL.md")

    for name, entry_stat in entries:
        child_parts = relative_parts + (name,)
        if stat.S_ISLNK(entry_stat.st_mode):
            raise SkillBundleError("skill bundle must not contain symlinks")
        if stat.S_ISDIR(entry_stat.st_mode):
            _hash_bundle_record(digest, b"D", child_parts, entry_stat.st_mode)
            child_fd = _open_bundle_entry(
                name,
                directory_fd=directory_fd,
                expected=entry_stat,
                directory=True,
            )
            try:
                _hash_bundle_directory(digest, child_fd, child_parts)
            finally:
                os.close(child_fd)
            continue
        if not stat.S_ISREG(entry_stat.st_mode):
            raise SkillBundleError("skill bundle contains a non-regular file")
        child_fd = _open_bundle_entry(
            name,
            directory_fd=directory_fd,
            expected=entry_stat,
            directory=False,
        )
        try:
            _hash_bundle_file(digest, child_fd, child_parts)
        finally:
            os.close(child_fd)

    after_entries = _directory_snapshot(directory_fd)
    if [
        (name, _stat_snapshot(entry_stat)) for name, entry_stat in after_entries
    ] != [
        (name, _stat_snapshot(entry_stat)) for name, entry_stat in entries
    ] or _stat_snapshot(os.fstat(directory_fd)) != _stat_snapshot(before):
        raise SkillBundleError("skill bundle changed while it was hashed")


def hash_skill_bundle(path: str | Path) -> str:
    """Hash every SDK-visible entry in a skill path without following links.

    Directory entry names, types, and regular-file bytes are framed and sorted
    before hashing.  Empty directories are included, and any symlink or special
    file rejects the bundle because its content is not an immutable part of the
    selected tree.
    """

    bundle_path = Path(path).expanduser()
    initial = os.lstat(bundle_path)
    if stat.S_ISLNK(initial.st_mode):
        raise SkillBundleError("skill bundle path must not be a symlink")

    digest = hashlib.sha256()
    digest.update(_SKILL_BUNDLE_HASH_DOMAIN)
    if stat.S_ISDIR(initial.st_mode):
        bundle_fd = os.open(bundle_path, _open_flags(directory=True))
        try:
            opened = os.fstat(bundle_fd)
            if _stat_identity(opened) != _stat_identity(initial):
                raise SkillBundleError("skill bundle changed while it was hashed")
            _hash_bundle_record(digest, b"D", (), opened.st_mode)
            _hash_bundle_directory(
                digest,
                bundle_fd,
                (),
                require_manifest=True,
            )
            current = os.lstat(bundle_path)
            if _stat_snapshot(current) != _stat_snapshot(os.fstat(bundle_fd)):
                raise SkillBundleError("skill bundle changed while it was hashed")
        finally:
            os.close(bundle_fd)
    elif stat.S_ISREG(initial.st_mode):
        bundle_fd = os.open(bundle_path, _open_flags())
        try:
            if _stat_identity(os.fstat(bundle_fd)) != _stat_identity(initial):
                raise SkillBundleError("skill bundle changed while it was hashed")
            _hash_bundle_file(digest, bundle_fd, ())
            current = os.lstat(bundle_path)
            if _stat_snapshot(current) != _stat_snapshot(os.fstat(bundle_fd)):
                raise SkillBundleError("skill bundle changed while it was hashed")
        finally:
            os.close(bundle_fd)
    else:
        raise SkillBundleError("skill bundle path is not a regular file or directory")
    return digest.hexdigest()


def _definition_hash(definition: SkillDefinition) -> str:
    """Compute a stable hash from trusted metadata or the complete local bundle."""

    try:
        return hash_skill_bundle(definition.path)
    except SkillBundleError:
        # Existing-but-unsafe bundles must never degrade to a metadata digest.
        raise
    except (FileNotFoundError, NotADirectoryError):
        pass
    # Metadata-only descriptors are common in SDK fakes and remote catalogs.
    # They retain a deterministic fallback until a local bundle is available.
    payload: dict[str, Any] = {
        "name": definition.name,
        "path": definition.path,
        "description": definition.description,
        "display_name": definition.display_name,
        "version": definition.version,
        "enabled": bool(definition.enabled),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_skill(
    value: Any,
    *,
    rehash_local_bundle: bool = False,
) -> SkillDefinition:
    """Normalize metadata and fill missing immutable identity fields.

    Some SDK releases expose a path and content but no explicit version.  A
    content-addressed version keeps the durable registry/task snapshot
    immutable without inventing mutable discovery-order numbers.
    """

    definition = SkillDefinition.from_value(value)
    if rehash_local_bundle:
        try:
            local_hash = hash_skill_bundle(definition.path)
        except (FileNotFoundError, NotADirectoryError):
            local_hash = ""
        content_hash = local_hash or definition.content_hash or _definition_hash(definition)
    else:
        # An explicit hash may be an immutable value loaded from a task or
        # durable registry, so never replace it with current filesystem state.
        content_hash = definition.content_hash or _definition_hash(definition)
    version = definition.version or f"sha256:{content_hash}"
    if (
        definition.content_hash == content_hash
        and definition.version == version
    ):
        return definition
    return SkillDefinition(
        name=definition.name,
        path=definition.path,
        description=definition.description,
        display_name=definition.display_name,
        version=version,
        content_hash=content_hash,
        enabled=definition.enabled,
    )


def iter_skill_descriptors(values: Any) -> Iterator[Any]:
    """Yield descriptor leaves from nested SDK and compatibility catalogs.

    The pinned Codex SDK returns ``SkillsListResponse.data[].skills``.  Some
    adapters return those typed entry/response objects inside another page or
    sequence, while older adapters return mappings or flat descriptors.  Keep
    this structural traversal shared so every runtime boundary unwraps those
    shapes identically before normalizing trusted metadata.
    """

    seen: set[int] = set()

    def descriptor_mapping(value: Mapping[str, Any]) -> bool:
        return any(
            key in value for key in ("name", "skill_id", "skillId", "id")
        ) and any(key in value for key in ("path", "skill_path"))

    def walk(value: Any) -> Iterator[Any]:
        if value is None or isinstance(value, (str, bytes, bytearray)):
            return
        if isinstance(value, SkillDefinition):
            yield value
            return
        if isinstance(value, Mapping):
            if descriptor_mapping(value):
                yield value
                return
            identity = id(value)
            if identity in seen:
                return
            seen.add(identity)
            if "skills" in value or "data" in value:
                nested = value.get("skills")
                if nested is None:
                    nested = value.get("data")
                yield from walk(nested)
                return
            # Compatibility registries may expose a name-keyed mapping.
            for nested in value.values():
                yield from walk(nested)
            return

        # Generated/Pydantic response and entry objects expose these fields as
        # attributes.  Test the descriptor shape first because a future SDK
        # descriptor may itself gain unrelated envelope-like metadata.
        if any(
            getattr(value, name, None) is not None
            for name in ("name", "skill_id", "skillId", "id")
        ) and any(
            getattr(value, name, None) is not None
            for name in ("path", "skill_path")
        ):
            yield value
            return
        identity = id(value)
        if identity in seen:
            return
        seen.add(identity)
        for attribute in ("skills", "data"):
            nested = getattr(value, attribute, None)
            if nested is not None:
                yield from walk(nested)
                return

        # A compatibility typed envelope may expose only a serialization API,
        # not public ``data``/``skills`` attributes.
        for method_name in ("model_dump", "to_dict"):
            method = getattr(value, method_name, None)
            if method is None:
                continue
            try:
                converted = method(by_alias=True, exclude_none=True)
            except TypeError:
                converted = method()
            if converted is not value:
                yield from walk(converted)
            return
        try:
            iterator = iter(value)
        except TypeError:
            return
        for nested in iterator:
            yield from walk(nested)

    yield from walk(values)


def normalize_skills(
    values: Sequence[Any] | Any | None,
    *,
    rehash_local_bundles: bool = False,
) -> tuple[SkillDefinition, ...]:
    """Return deterministic, name-deduplicated trusted skill definitions."""

    selected: dict[str, SkillDefinition] = {}

    def version_key(value: str) -> tuple[int, Any]:
        text = str(value or "")
        try:
            return (1, int(text))
        except ValueError:
            return (0, text)

    for value in iter_skill_descriptors(values):
        try:
            definition = normalize_skill(
                value,
                rehash_local_bundle=rehash_local_bundles,
            )
        except (TypeError, ValueError):
            continue
        if not definition.enabled:
            continue
        key = definition.skill_id
        previous = selected.get(key)
        if previous is None:
            selected[key] = definition
            continue
        # Prefer a version with an explicit value, then use the lexical path as
        # a deterministic tie-breaker.  Never let discovery order select a
        # different skill after a process restart.
        candidate_key = (
            bool(definition.version),
            version_key(definition.version),
            definition.path,
        )
        previous_key = (
            bool(previous.version),
            version_key(previous.version),
            previous.path,
        )
        if candidate_key > previous_key:
            selected[key] = definition
    return tuple(sorted(selected.values(), key=lambda item: (item.skill_id, item.path)))


def parse_skill_invocation(text: str) -> SkillInvocation | None:
    """Parse a leading ``$skill description`` expression.

    A non-dollar message is ordinary task text.  Once a message starts with a
    single dollar, malformed or empty invocations are rejected rather than
    silently sending a possibly mistyped skill request as a normal prompt.
    """

    stripped = str(text or "").strip()
    if not stripped.startswith("$"):
        return None
    match = _SKILL_NAME_RE.fullmatch(stripped)
    if match is None:
        raise SkillSyntaxError("usage: $<skill> <task description>")
    name, description = match.groups()
    description = str(description or "").strip()
    if not description:
        raise SkillSyntaxError("usage: $<skill> <task description>")
    return SkillInvocation(name=name, description=description)


def find_skill(
    values: Sequence[Any] | Any | None,
    name: str,
    *,
    rehash_local_bundles: bool = False,
) -> SkillDefinition | None:
    """Resolve a skill case-insensitively from trusted metadata."""

    requested = str(name or "").strip().casefold()
    if not requested:
        return None
    return next(
        (
            item
            for item in normalize_skills(
                values,
                rehash_local_bundles=rehash_local_bundles,
            )
            if item.skill_id == requested
        ),
        None,
    )


def format_skills_markdown(values: Sequence[Any] | Any | None) -> str:
    """Render a bounded deterministic public `/skills` response."""

    def public_line(value: Any, *, redact_paths: bool = False) -> str:
        # A malformed SDK descriptor must not break the command projection or
        # inject additional Markdown lines into a deterministic response.
        line = " ".join(str(value or "").split())
        if redact_paths:
            line = _PATH_FRAGMENT_RE.sub("[path]", line)
        return line.replace("`", "'")

    definitions = normalize_skills(values)
    lines = [
        "## Skills",
        "Use `$<skill> <task description>` to invoke a skill.",
    ]
    if not definitions:
        lines.append("- (none enabled)")
    else:
        for definition in definitions:
            summary = public_line(
                definition.description or "No description provided.",
                redact_paths=True,
            )
            display = public_line(definition.public_name, redact_paths=True)
            name = public_line(definition.skill_id)
            lines.append(f"- `${name}` - {display}: {summary}")
    result = "\n".join(lines)
    if len(result) > _MAX_SKILLS_HELP:
        result = result[:_MAX_SKILLS_HELP].rstrip() + "\n... (list truncated)"
    return result


__all__ = [
    "SkillBundleError",
    "SkillDefinition",
    "SkillInvocation",
    "SkillSyntaxError",
    "find_skill",
    "format_skills_markdown",
    "hash_skill_bundle",
    "iter_skill_descriptors",
    "normalize_skill",
    "normalize_skills",
    "parse_skill_invocation",
]
