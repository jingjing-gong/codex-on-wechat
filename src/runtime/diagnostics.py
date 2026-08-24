"""Persistent, bounded diagnostics for the Codex-on-WeChat supervisor.

The Agent child deliberately does not print provider failures because its
stderr can contain authentication and bootstrap material.  Terminal results
cross the private IPC boundary instead, and the supervisor records a small
allow-listed diagnostic projection here.  Prompts, tool payloads, response
bodies, credentials, and request headers are never part of that projection.
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from collections.abc import Mapping, Sequence
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


DEFAULT_LOG_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
MAX_DIAGNOSTIC_TEXT = 4_096
MAX_DIAGNOSTIC_ITEMS = 32
MAX_DIAGNOSTIC_DEPTH = 8

_SECRET_KEY_PARTS = (
    "authorization",
    "apikey",
    "api_key",
    "bearer",
    "credential",
    "password",
    "secret",
    "token",
)
_ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|"
    r"\x9b[0-?]*[ -/]*[@-~]|"
    r"\x1b[@-Z\\-_])"
)
_AUTH_SCHEME_PATTERN = re.compile(
    r'''(?i)(?<![\w-])(?:"(?:bearer|basic)"|'(?:bearer|basic)'|'''
    r'''(?:bearer|basic))(?:\s*[:=]\s*|\s+)'''
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(?<![\w-])[\"']?"
    r"((?:[a-z0-9][a-z0-9_-]{0,63})?(?:"
    r"api[_-]?key|authorization|credential|password|secret|token))"
    r"[\"']?\s*[:=]\s*"
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_URL_PATTERN = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s,]+")


def runtime_log_path() -> Path:
    """Return the configured persistent supervisor log path."""

    configured = os.environ.get("CODEX_WECHAT_LOG", "").strip()
    path = (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".codex-wechat-bot" / "logs" / "codex-wechat.log"
    )
    return path.resolve()


def _positive_environment_integer(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _log_level() -> int:
    configured = os.environ.get("CODEX_WECHAT_LOG_LEVEL", "INFO").strip().upper()
    value = logging.getLevelName(configured)
    if not isinstance(value, int):
        raise RuntimeError("CODEX_WECHAT_LOG_LEVEL is invalid")
    return value


def configure_persistent_logging() -> Path:
    """Install console plus rotating owner-only file logging once per process."""

    path = runtime_log_path()
    max_bytes = _positive_environment_integer(
        "CODEX_WECHAT_LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES
    )
    backup_count = _positive_environment_integer(
        "CODEX_WECHAT_LOG_BACKUPS", DEFAULT_LOG_BACKUP_COUNT
    )
    level = _log_level()

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s pid=%(process)d process=%(processName)s "
        "thread=%(threadName)s %(name)s: %(message)s"
    )

    for existing in tuple(root.handlers):
        if not bool(getattr(existing, "_codex_wechat_persistent", False)):
            continue
        existing_path = Path(str(getattr(existing, "baseFilename", ""))).resolve()
        if existing_path == path:
            existing.setLevel(level)
            return path
        root.removeHandler(existing)
        existing.close()

    # Keep operator feedback on stderr while making it durable.  FileHandler
    # is a StreamHandler subclass, hence the explicit exclusion.
    if not any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
        for handler in root.handlers
    ):
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(formatter)
        root.addHandler(console)

    handler = RotatingFileHandler(
        path,
        mode="a",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
        delay=False,
    )
    setattr(handler, "_codex_wechat_persistent", True)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    # The log can contain user and task identifiers even though secret-bearing
    # request material is excluded.  Restrict it to the service account.
    os.chmod(path, 0o600)
    return path


def sanitize_diagnostic_text(value: Any, *, maximum: int = MAX_DIAGNOSTIC_TEXT) -> str:
    """Return one bounded line with obvious credentials and URL queries removed."""

    without_escapes = _ANSI_ESCAPE_PATTERN.sub("", str(value or ""))
    cleaned = "".join(
        " "
        if character.isspace()
        else ""
        if unicodedata.category(character).startswith("C")
        else character
        for character in without_escapes
    )
    text = " ".join(cleaned.split())
    text = _AUTH_SCHEME_PATTERN.sub("authorization=<redacted>", text)
    text = _ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}=<redacted>", text
    )

    def sanitize_url(match: re.Match[str]) -> str:
        candidate = match.group(0)
        suffix = ""
        while candidate and candidate[-1] in ".)]}":
            suffix = candidate[-1] + suffix
            candidate = candidate[:-1]
        try:
            parsed = urlsplit(candidate)
        except ValueError:
            return "<invalid-url>" + suffix
        try:
            hostname = parsed.hostname or ""
            port = parsed.port
        except ValueError:
            return "<invalid-url>" + suffix
        if not hostname:
            return "<invalid-url>" + suffix
        host = hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if port is not None:
            host = f"{host}:{port}"
        sanitized = urlunsplit((parsed.scheme, host, parsed.path, "", ""))
        return sanitized + suffix

    text = _URL_PATTERN.sub(sanitize_url, text)
    if len(text) <= maximum:
        return text
    return text[: max(0, maximum - 3)].rstrip() + "..."


def _secret_key(key: Any) -> bool:
    normalized = str(key or "").strip().lower().replace("-", "_")
    return any(part in normalized for part in _SECRET_KEY_PARTS)


def safe_diagnostic_value(value: Any, *, depth: int = 0) -> Any:
    """Project typed Codex error metadata onto bounded, secret-safe JSON."""

    if depth >= MAX_DIAGNOSTIC_DEPTH:
        return "<depth-limit>"
    if value is None or type(value) in {bool, int, float}:
        return value
    if isinstance(value, str):
        return sanitize_diagnostic_text(value)
    enum_value = getattr(value, "value", None)
    if enum_value is not None and not isinstance(value, Mapping):
        return safe_diagnostic_value(enum_value, depth=depth + 1)
    if not isinstance(value, (Mapping, Sequence, str, bytes, bytearray)):
        dumper = getattr(value, "model_dump", None)
        if callable(dumper):
            try:
                value = dumper(mode="json", by_alias=True, exclude_none=True)
            except TypeError:
                value = dumper()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for ordinal, (key, item) in enumerate(value.items()):
            if ordinal >= MAX_DIAGNOSTIC_ITEMS:
                result["_truncated"] = True
                break
            name = sanitize_diagnostic_text(key, maximum=128)
            result[name] = (
                "<redacted>"
                if _secret_key(name)
                else safe_diagnostic_value(item, depth=depth + 1)
            )
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(value[:MAX_DIAGNOSTIC_ITEMS])
        result = [safe_diagnostic_value(item, depth=depth + 1) for item in items]
        if len(value) > MAX_DIAGNOSTIC_ITEMS:
            result.append("<truncated>")
        return result
    return sanitize_diagnostic_text(value)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _status(value: Any) -> str:
    raw = _field(value, "status", "")
    return str(getattr(raw, "value", raw) or "").strip().lower()


def _task_fields(task: Any) -> dict[str, Any]:
    return {
        "task_id": sanitize_diagnostic_text(_field(task, "task_id", ""), maximum=256),
        "execution_id": sanitize_diagnostic_text(
            _field(task, "execution_id", ""), maximum=256
        ),
        "request_id": sanitize_diagnostic_text(
            _field(task, "request_id", ""), maximum=256
        ),
        "agent_id": sanitize_diagnostic_text(_field(task, "agent_id", ""), maximum=128),
        "mode_id": sanitize_diagnostic_text(_field(task, "mode_id", ""), maximum=128),
        "model": sanitize_diagnostic_text(_field(task, "model", ""), maximum=512),
        "reasoning_effort": sanitize_diagnostic_text(
            _field(task, "reasoning_effort", ""), maximum=128
        ),
    }


def _write_structured(logger: logging.Logger, level: int, event: str, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(
        safe_diagnostic_value(dict(payload)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    logger.log(level, "%s %s", event, encoded)


def log_task_started(
    logger: logging.Logger,
    task: Any,
    *,
    worker_id: str,
    attempt: Any = None,
) -> None:
    payload = _task_fields(task)
    payload.update(
        {
            "worker_id": sanitize_diagnostic_text(worker_id, maximum=256),
            "attempt": attempt,
        }
    )
    _write_structured(logger, logging.INFO, "agent_task_started", payload)


def log_task_terminal(
    logger: logging.Logger,
    task: Any,
    result: Any,
    *,
    worker_id: str,
    attempt: Any = None,
    duration_ms: int | None = None,
) -> None:
    status = _status(result) or "unknown"
    payload = _task_fields(task)
    payload.update(
        {
            "worker_id": sanitize_diagnostic_text(worker_id, maximum=256),
            "attempt": attempt,
            "status": status,
            "duration_ms": duration_ms,
            "thread_id": sanitize_diagnostic_text(
                _field(result, "thread_id", ""), maximum=256
            ),
            "error": sanitize_diagnostic_text(_field(result, "error", "")),
        }
    )
    metadata = _field(result, "metadata", {})
    diagnostics = (
        metadata.get("diagnostics") if isinstance(metadata, Mapping) else None
    )
    if isinstance(diagnostics, Mapping):
        payload["diagnostics"] = safe_diagnostic_value(diagnostics)
    level = logging.INFO if status == "completed" else logging.ERROR
    _write_structured(logger, level, "agent_task_terminal", payload)


__all__ = [
    "DEFAULT_LOG_BACKUP_COUNT",
    "DEFAULT_LOG_MAX_BYTES",
    "configure_persistent_logging",
    "log_task_started",
    "log_task_terminal",
    "runtime_log_path",
    "safe_diagnostic_value",
    "sanitize_diagnostic_text",
]
