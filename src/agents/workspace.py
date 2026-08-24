"""Immutable execution-workspace snapshots for Agent tasks.

The supervisor selects a workspace, while the Agent process consumes only the
persisted snapshot.  Canonical paths plus directory identities prevent a task
from silently falling back to, escaping from, or following a replaced
workspace at execution time.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import Any, Mapping


EXECUTION_WORKSPACE_KEY = "execution_workspace"

_WORKSPACE_SNAPSHOT_VERSION = 1
_WORKSPACE_SNAPSHOT_FIELDS = frozenset(
    {
        "version",
        "root",
        "path",
        "root_st_dev",
        "root_st_ino",
        "target_st_dev",
        "target_st_ino",
    }
)


class WorkspaceError(ValueError):
    """An execution workspace is invalid, unavailable, or has changed."""


def _resolve_directory(
    value: str | os.PathLike[str],
    *,
    label: str,
    relative_to: Path | None = None,
) -> tuple[Path, os.stat_result]:
    try:
        candidate = Path(value).expanduser()
    except (TypeError, ValueError, OSError) as exc:
        raise WorkspaceError(f"{label} must be a filesystem path") from exc
    if relative_to is not None and not candidate.is_absolute():
        candidate = relative_to / candidate
    try:
        resolved = candidate.resolve(strict=True)
        details = resolved.stat()
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceError(f"{label} is unavailable") from exc
    if not stat.S_ISDIR(details.st_mode):
        raise WorkspaceError(f"{label} is not a directory")
    if not os.access(resolved, os.X_OK):
        raise WorkspaceError(f"{label} is not enterable")
    return resolved, details


def _require_contained(path: Path, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise WorkspaceError(
            "execution workspace is outside the configured root"
        ) from exc


def build_workspace_snapshot(
    root: str | os.PathLike[str],
    directory: str | os.PathLike[str],
) -> dict[str, Any]:
    """Build one canonical version-1 workspace snapshot.

    Relative directories are interpreted below ``root``.  Both paths must
    already exist and be enterable directories; this function never creates
    or changes a directory.
    """

    root_path, root_details = _resolve_directory(root, label="workspace root")
    target_path, target_details = _resolve_directory(
        directory,
        label="execution workspace",
        relative_to=root_path,
    )
    _require_contained(target_path, root_path)
    return {
        "version": _WORKSPACE_SNAPSHOT_VERSION,
        "root": str(root_path),
        "path": str(target_path),
        "root_st_dev": int(root_details.st_dev),
        "root_st_ino": int(root_details.st_ino),
        "target_st_dev": int(target_details.st_dev),
        "target_st_ino": int(target_details.st_ino),
    }


def _snapshot_identity(snapshot: Mapping[str, Any], prefix: str) -> tuple[int, int]:
    device = snapshot.get(f"{prefix}_st_dev")
    inode = snapshot.get(f"{prefix}_st_ino")
    if type(device) is not int or device < 0:
        raise WorkspaceError(f"workspace {prefix} device identity is invalid")
    if type(inode) is not int or inode < 0:
        raise WorkspaceError(f"workspace {prefix} inode identity is invalid")
    return device, inode


def _snapshot_canonical_path(
    snapshot: Mapping[str, Any],
    name: str,
) -> tuple[Path, os.stat_result]:
    raw = snapshot.get(name)
    if type(raw) is not str or not raw or not Path(raw).is_absolute():
        raise WorkspaceError(f"workspace snapshot {name} is invalid")
    resolved, details = _resolve_directory(raw, label=f"workspace snapshot {name}")
    if str(resolved) != raw:
        raise WorkspaceError(f"workspace snapshot {name} is not canonical")
    return resolved, details


def validate_workspace_snapshot(
    snapshot: Mapping[str, Any],
    configured_root: str | os.PathLike[str],
) -> str:
    """Validate a snapshot against the configured root and current filesystem.

    Validation is intentionally repeated in the Agent process immediately
    before SDK thread/turn operations.  Replacing either the configured root
    or selected directory therefore fails closed instead of changing a
    persisted task's effective working directory.
    """

    if not isinstance(snapshot, Mapping):
        raise WorkspaceError("execution workspace snapshot must be a mapping")
    if frozenset(snapshot) != _WORKSPACE_SNAPSHOT_FIELDS:
        raise WorkspaceError("execution workspace snapshot fields are invalid")
    if type(snapshot.get("version")) is not int or snapshot["version"] != 1:
        raise WorkspaceError("execution workspace snapshot version is invalid")

    configured_path, configured_details = _resolve_directory(
        configured_root,
        label="configured workspace root",
    )
    root_path, root_details = _snapshot_canonical_path(snapshot, "root")
    target_path, target_details = _snapshot_canonical_path(snapshot, "path")
    if root_path != configured_path:
        raise WorkspaceError(
            "execution workspace root does not match the configured root"
        )
    _require_contained(target_path, root_path)

    expected_root = _snapshot_identity(snapshot, "root")
    expected_target = _snapshot_identity(snapshot, "target")
    current_configured = (
        int(configured_details.st_dev),
        int(configured_details.st_ino),
    )
    current_root = (int(root_details.st_dev), int(root_details.st_ino))
    current_target = (int(target_details.st_dev), int(target_details.st_ino))
    if current_configured != expected_root or current_root != expected_root:
        raise WorkspaceError("configured workspace root identity has changed")
    if current_target != expected_target:
        raise WorkspaceError("execution workspace identity has changed")
    return str(target_path)


__all__ = [
    "EXECUTION_WORKSPACE_KEY",
    "WorkspaceError",
    "build_workspace_snapshot",
    "validate_workspace_snapshot",
]
