"""Safe loading of one named Codex configuration layer.

Only the validated filename stem crosses the parent/Agent process boundary.
The credential-bearing TOML is resolved and parsed inside the dedicated Agent
child, then the selected model/provider subset is passed to Codex over its
private app-server stdio channel.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

try:  # Python 3.11+
    import tomllib
except ImportError:  # pragma: no cover - the project supports Python 3.10.
    import tomli as tomllib


_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_MAX_CONFIG_BYTES = 4 * 1024 * 1024
_MAX_CONFIG_NESTING = 128


class CodexConfigProfileError(ValueError):
    """A named Codex configuration cannot be selected safely."""


@dataclass(frozen=True, slots=True)
class _ValidatedConfigFile:
    """Identity snapshot used to open and read one validated config exactly."""

    root: Path
    name: str
    root_device: int
    root_inode: int
    file_device: int
    file_inode: int
    file_size: int
    file_mtime_ns: int
    file_ctime_ns: int


def normalize_profile_name(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise CodexConfigProfileError(
            "Codex config profile must be a safe profile name"
        )
    normalized = value.strip()
    if normalized and not _PROFILE_NAME_RE.fullmatch(normalized):
        raise CodexConfigProfileError(
            "Codex config profile must be a safe profile name"
        )
    return normalized


def codex_home(environ: Mapping[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    configured = str(values.get("CODEX_HOME", "") or "").strip()
    return (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".codex"
    ).resolve()


def _contained_regular_file(
    root: Path,
    candidate: Path,
    *,
    label: str,
) -> _ValidatedConfigFile:
    try:
        resolved_root = root.resolve(strict=True)
        # Config filenames are direct children selected by trusted code.  Do
        # not follow even an in-root symlink: retaining the directory and file
        # identities lets `_read_toml` reject every pathname swap between
        # validation and open.
        if candidate.parent.resolve(strict=True) != resolved_root:
            raise ValueError("outside config root")
        root_metadata = resolved_root.stat()
        metadata = candidate.lstat()
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        raise CodexConfigProfileError(label) from None
    if (
        not stat.S_ISDIR(root_metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size > _MAX_CONFIG_BYTES
    ):
        raise CodexConfigProfileError(label)
    return _ValidatedConfigFile(
        root=resolved_root,
        name=candidate.name,
        root_device=int(root_metadata.st_dev),
        root_inode=int(root_metadata.st_ino),
        file_device=int(metadata.st_dev),
        file_inode=int(metadata.st_ino),
        file_size=int(metadata.st_size),
        file_mtime_ns=int(metadata.st_mtime_ns),
        file_ctime_ns=int(metadata.st_ctime_ns),
    )


def require_profile_file(
    name: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Validate a profile name and prove its expected file currently exists."""

    normalized = normalize_profile_name(name)
    if not normalized:
        return ""
    root = codex_home(environ)
    _contained_regular_file(
        root,
        root / f"{normalized}.config.toml",
        label=f"Codex config profile not found: {normalized}",
    )
    return normalized


def _read_toml(
    validated: _ValidatedConfigFile,
    *,
    label: str,
) -> tuple[dict[str, Any], bytes]:
    root_fd = -1
    file_fd = -1
    try:
        common_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        no_follow = getattr(os, "O_NOFOLLOW", 0)
        root_fd = os.open(
            validated.root,
            common_flags | no_follow | getattr(os, "O_DIRECTORY", 0),
        )
        root_metadata = os.fstat(root_fd)
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or int(root_metadata.st_dev) != validated.root_device
            or int(root_metadata.st_ino) != validated.root_inode
        ):
            raise OSError("config root identity changed")
        file_fd = os.open(
            validated.name,
            common_flags | no_follow,
            dir_fd=root_fd,
        )
        metadata = os.fstat(file_fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or int(metadata.st_dev) != validated.file_device
            or int(metadata.st_ino) != validated.file_inode
            or int(metadata.st_size) != validated.file_size
            or int(metadata.st_mtime_ns) != validated.file_mtime_ns
            or int(metadata.st_ctime_ns) != validated.file_ctime_ns
            or int(metadata.st_size) > _MAX_CONFIG_BYTES
        ):
            raise OSError("config file identity changed")
        chunks: list[bytes] = []
        remaining = _MAX_CONFIG_BYTES + 1
        while remaining > 0:
            chunk = os.read(file_fd, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > _MAX_CONFIG_BYTES or len(payload) != validated.file_size:
            raise ValueError("oversized")
        # Opening a stable inode is insufficient: another process can rewrite
        # that inode in place without changing its final size.  Recheck both
        # change timestamps after the last read so a mutation before or during
        # the read cannot cross the validated configuration boundary.
        final_metadata = os.fstat(file_fd)
        if (
            not stat.S_ISREG(final_metadata.st_mode)
            or int(final_metadata.st_dev) != validated.file_device
            or int(final_metadata.st_ino) != validated.file_inode
            or int(final_metadata.st_size) != validated.file_size
            or int(final_metadata.st_mtime_ns) != validated.file_mtime_ns
            or int(final_metadata.st_ctime_ns) != validated.file_ctime_ns
        ):
            raise OSError("config file changed while being read")
        decoded = tomllib.loads(payload.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, RecursionError, tomllib.TOMLDecodeError):
        # Parser diagnostics can quote credential-bearing source lines.  Keep
        # the public/IPC error deliberately generic.
        raise CodexConfigProfileError(label) from None
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        if root_fd >= 0:
            os.close(root_fd)
    if not isinstance(decoded, dict):
        raise CodexConfigProfileError(label)
    return decoded, payload


def _merged(base: Mapping[str, Any], layer: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = dict(base)
    for key, value in layer.items():
        previous = result.get(key)
        if isinstance(previous, Mapping) and isinstance(value, Mapping):
            result[key] = _merged(previous, value)
        else:
            result[key] = value
    return result


def _config_structure_is_bounded(value: Mapping[str, Any]) -> bool:
    """Reject parser output whose container nesting is unsafe to merge/use."""

    stack: list[tuple[Any, int]] = [(value, 0)]
    seen: set[int] = set()
    while stack:
        current, depth = stack.pop()
        if depth > _MAX_CONFIG_NESTING:
            return False
        if not isinstance(current, (Mapping, list, tuple)):
            continue
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        nested_values = current.values() if isinstance(current, Mapping) else current
        for nested in nested_values:
            if isinstance(nested, (Mapping, list, tuple)):
                stack.append((nested, depth + 1))
    return True


def _bounded_identifier(value: Any, *, maximum: int) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > maximum
        or any(ord(character) < 32 for character in normalized)
    ):
        return ""
    return normalized


@dataclass(frozen=True, slots=True)
class LoadedCodexConfigProfile:
    name: str
    model: str
    model_provider: str
    effective_config: Mapping[str, Any]
    fingerprint: str

    def selected_provider_config(self) -> dict[str, Any]:
        providers = self.effective_config.get("model_providers")
        if not isinstance(providers, Mapping) or not self.model_provider:
            return {}
        selected = providers.get(self.model_provider)
        return dict(selected) if isinstance(selected, Mapping) else {}

    def thread_config(self) -> dict[str, Any]:
        """Return the narrow provider/model settings needed by app-server."""

        values: dict[str, Any] = {}
        provider = self.selected_provider_config()
        if provider and self.model_provider:
            values["model_providers"] = {self.model_provider: provider}
        return values


def load_config_profile(
    name: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> LoadedCodexConfigProfile | None:
    """Load the base config plus one named layer from the trusted Codex home."""

    normalized = normalize_profile_name(name)
    if not normalized:
        return None
    root = codex_home(environ)
    profile_path = _contained_regular_file(
        root,
        root / f"{normalized}.config.toml",
        label=f"Codex config profile not found: {normalized}",
    )
    layer, layer_bytes = _read_toml(
        profile_path,
        label=f"Codex config profile is invalid: {normalized}",
    )
    if not _config_structure_is_bounded(layer):
        raise CodexConfigProfileError(
            f"Codex config profile is invalid: {normalized}"
        )
    base: dict[str, Any] = {}
    base_bytes = b""
    base_candidate = root / "config.toml"
    if base_candidate.exists():
        base_path = _contained_regular_file(
            root,
            base_candidate,
            label="base Codex config is unavailable",
        )
        base, base_bytes = _read_toml(
            base_path,
            label="base Codex config is invalid",
        )
        if not _config_structure_is_bounded(base):
            raise CodexConfigProfileError("base Codex config is invalid")
    effective = _merged(base, layer)
    model = _bounded_identifier(effective.get("model"), maximum=512)
    provider = _bounded_identifier(
        effective.get("model_provider"),
        maximum=256,
    )
    providers = effective.get("model_providers")
    selected_provider = (
        providers.get(provider) if isinstance(providers, Mapping) else None
    )
    if (
        not model
        or not provider
        or not isinstance(providers, Mapping)
        or not isinstance(selected_provider, Mapping)
        or not selected_provider
    ):
        # A named profile is an explicit provider boundary.  Silently falling
        # back to the base runtime when any part of that selection is absent
        # would bind the Agent name to behavior different from the requested
        # configuration.
        raise CodexConfigProfileError(
            f"Codex config profile is incomplete: {normalized}"
        )
    fingerprint = hashlib.sha256(
        normalized.encode("utf-8") + b"\0" + base_bytes + b"\0" + layer_bytes
    ).hexdigest()
    return LoadedCodexConfigProfile(
        name=normalized,
        model=model,
        model_provider=provider,
        effective_config=effective,
        fingerprint=fingerprint,
    )


__all__ = [
    "CodexConfigProfileError",
    "LoadedCodexConfigProfile",
    "codex_home",
    "load_config_profile",
    "normalize_profile_name",
    "require_profile_file",
]
