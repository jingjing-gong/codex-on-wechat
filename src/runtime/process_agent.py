"""Process-isolated Agent runtime proxy.

``ProcessAgentRuntime`` is the application-facing ``AgentRuntime`` boundary for
the process-per-Agent topology.  Every instance owns at most one persistent
``multiprocessing`` *spawn* child.  The child constructs the SDK runtime and
owns its thread/session state; the supervisor process owns durability,
channels, bridge capabilities, and managed artifact publication.

The transport is a private duplex pipe carrying bounded JSON messages.  An
``EVENT`` is acknowledged only after the parent-side emit callback returns, so
the child cannot outrun the durable event commit.  No store, SQLite, WeChat, or
channel object is serialized into the child.

The child target is deliberately imported by the top-level module name
``process_agent``.  That keeps Python's spawn bootstrap from executing the
heavy ``src.runtime`` package initializer in the child.  Immediately before
loading ``CodexRuntime`` the child installs a narrow synthetic
``src.runtime`` package path; this permits the SDK adapter's dependency-free
``media``/``roles`` helpers without importing the store or channel graph.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import dataclasses
import datetime as _datetime
import enum
import importlib
import inspect
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import signal
import sys
import threading
import types
import unicodedata
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Callable


_PROTOCOL_VERSION = 1
_DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024
_DEFAULT_MAX_PROCESSES = 16
_BRIDGE_CAPABILITY_METADATA_KEY = "_process_agent_bridge_capability"
_AGENT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_CONFIG_PROFILE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_FACTORY_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_.]*$"
)
_MAX_IPC_ERROR_TEXT = 4_096
_ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|"
    r"\x9b[0-?]*[ -/]*[@-~]|"
    r"\x1b[@-Z\\-_])"
)
_IPC_ERROR_URL_PATTERN = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]*://[^\s,)\]}>\"']+"
)
_IPC_ERROR_AUTH_SCHEME_PATTERN = re.compile(
    r'''(?i)(?<![\w-])(?:"(?:bearer|basic)"|'(?:bearer|basic)'|'''
    r'''(?:bearer|basic))(?:\s*[:=]\s*|\s+)'''
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_IPC_ERROR_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(?<![\w-])[\"']?"
    r"((?:[a-z0-9][a-z0-9_-]{0,63})?(?:"
    r"api[_-]?key|authorization|credential|password|secret|token))"
    r"[\"']?\s*[:=]\s*"
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_IPC_ERROR_HOST_PATTERN = re.compile(
    r"(?i)(?<![\w@/.-])(?:"
    r"\[(?=[0-9a-f:.%]*:)[0-9a-f:.]+(?:%[a-z0-9_.-]+)?\]|"
    r"localhost|"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?\.)+[a-z]{2,63}|"
    r"(?:\d{1,3}\.){3}\d{1,3}"
    r")(?::\d{1,5})?(?:/[^\s,)\]}>\"']*)?"
)
_PR_SET_PDEATHSIG = 1
_SPAWN_MAIN_LOCK = threading.Lock()


def _sanitized_ipc_error(value: Any, *, fallback: str = "operation failed") -> str:
    """Bound and redact exception text before it crosses an Agent pipe."""

    without_escapes = _ANSI_ESCAPE_PATTERN.sub("", str(value or ""))
    cleaned = "".join(
        " "
        if character.isspace()
        else ""
        if unicodedata.category(character).startswith("C")
        else character
        for character in without_escapes
    )
    normalized = " ".join(cleaned.split()).replace("`", "'")
    normalized = _IPC_ERROR_URL_PATTERN.sub("<redacted-url>", normalized)
    normalized = _IPC_ERROR_AUTH_SCHEME_PATTERN.sub(
        "<redacted-authorization>", normalized
    )
    normalized = _IPC_ERROR_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}=<redacted>", normalized
    )
    normalized = _IPC_ERROR_HOST_PATTERN.sub("<redacted-host>", normalized)
    normalized = normalized or fallback
    if len(normalized) <= _MAX_IPC_ERROR_TEXT:
        return normalized
    return normalized[: _MAX_IPC_ERROR_TEXT - 3].rstrip() + "..."


class ProcessAgentError(RuntimeError):
    """Base error for the process-isolated runtime boundary."""


class ProcessAgentConfigurationError(ProcessAgentError, ValueError):
    """A process runtime cannot be configured safely."""


class ProcessAgentCapacityError(ProcessAgentError):
    """The shared Agent-process limit was reached before spawn."""


class ProcessAgentStartupError(ProcessAgentError):
    """A fresh child failed before publishing ``READY``."""


class ProcessAgentProtocolError(ProcessAgentError):
    """A private child sent a malformed or mis-correlated message."""


class ProcessAgentRemoteError(ProcessAgentError):
    """The child handled an operation but rejected or failed it."""


class ProcessAgentLostError(ProcessAgentError):
    """The exact child generation disappeared during an IPC operation.

    A process loss can occur after the SDK or one of its tools performed a
    side effect but before ``RESULT`` reached the supervisor.  Callers must
    therefore orphan/reconcile the durable invocation rather than retrying it
    as though it had never run.
    """

    execution_uncertain = True

    def __init__(
        self,
        message: str,
        *,
        agent_id: str,
        generation: int,
        pid: int | None,
    ) -> None:
        super().__init__(message)
        self.agent_id = agent_id
        self.generation = generation
        self.pid = pid
        self.execution_uncertain = True


class ProcessAgentHealth(str, enum.Enum):
    NEW = "new"
    STARTING = "starting"
    READY = "ready"
    BUSY = "busy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    LOST = "lost"


def _configured_process_limit() -> int:
    raw = os.environ.get("CODEX_WECHAT_MAX_AGENT_PROCESSES", "").strip()
    if not raw:
        return _DEFAULT_MAX_PROCESSES
    try:
        value = int(raw)
    except ValueError as exc:
        raise ProcessAgentConfigurationError(
            "CODEX_WECHAT_MAX_AGENT_PROCESSES must be a positive integer"
        ) from exc
    if value <= 0:
        raise ProcessAgentConfigurationError(
            "CODEX_WECHAT_MAX_AGENT_PROCESSES must be a positive integer"
        )
    return value


class _SharedProcessBudget:
    """Process-local admission accounting shared by Agent proxy clones."""

    def __init__(self, limit: int) -> None:
        self.limit = int(limit)
        self._lock = threading.Lock()
        self._owners: set[int] = set()

    def reserve(self, owner: object) -> None:
        token = id(owner)
        with self._lock:
            if token in self._owners:
                return
            if len(self._owners) >= self.limit:
                raise ProcessAgentCapacityError(
                    f"Agent process limit reached ({self.limit})"
                )
            self._owners.add(token)

    def release(self, owner: object) -> None:
        with self._lock:
            self._owners.discard(id(owner))

    @property
    def active(self) -> int:
        with self._lock:
            return len(self._owners)


_DEFAULT_PROCESS_BUDGET = _SharedProcessBudget(_configured_process_limit())


def _positive_timeout(value: float | int, field: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ProcessAgentConfigurationError(f"{field} must be positive")
    return result


def _factory_path(value: str | Callable[..., Any] | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        path = value.strip()
    elif callable(value):
        module_name = str(getattr(value, "__module__", "") or "")
        qualname = str(getattr(value, "__qualname__", "") or "")
        if not module_name or not qualname or "<locals>" in qualname:
            raise ProcessAgentConfigurationError(
                "backend_factory must be importable in a spawned interpreter"
            )
        path = f"{module_name}:{qualname}"
    else:
        raise ProcessAgentConfigurationError(
            "backend_factory must be an import string or callable"
        )
    if not _FACTORY_PATTERN.fullmatch(path):
        raise ProcessAgentConfigurationError(
            "backend_factory must use the import form module:qualname"
        )
    return path


def _wire_value(value: Any, *, depth: int = 0) -> Any:
    """Project one value onto the bounded JSON wire vocabulary."""

    if depth > 64:
        raise ProcessAgentProtocolError("IPC value nesting is too deep")
    if value is None or type(value) in {bool, str, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ProcessAgentProtocolError("IPC numbers must be finite")
        return value
    if isinstance(value, enum.Enum):
        return _wire_value(value.value, depth=depth + 1)
    if isinstance(value, (_datetime.datetime, _datetime.date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ProcessAgentProtocolError("IPC mapping keys must be strings")
            result[key] = _wire_value(item, depth=depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_wire_value(item, depth=depth + 1) for item in value]
    if dataclasses.is_dataclass(value):
        return _wire_value(dataclasses.asdict(value), depth=depth + 1)
    for method_name in ("as_dict", "to_dict", "model_dump"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                projected = method()
            except TypeError:
                continue
            return _wire_value(projected, depth=depth + 1)
    raise ProcessAgentProtocolError(
        f"{type(value).__name__} cannot cross the Agent process boundary"
    )


def _ipc_secret_key(value: Any) -> bool:
    normalized = re.sub(
        r"[^a-z0-9]",
        "",
        _ANSI_ESCAPE_PATTERN.sub("", str(value or "")).lower(),
    )
    return any(
        marker in normalized
        for marker in (
            "apikey",
            "authorization",
            "credential",
            "password",
            "secret",
            "token",
        )
    )


def _sanitized_ipc_diagnostic(value: Any, *, depth: int = 0) -> Any:
    """Redact nested diagnostic metadata without changing ordinary wire data."""

    if depth >= 16:
        return "<redacted-depth>"
    if isinstance(value, str):
        return _sanitized_ipc_error(value)
    if value is None or type(value) in {bool, int, float}:
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            name = str(key)
            result[name] = (
                "<redacted>"
                if _ipc_secret_key(name)
                else _sanitized_ipc_diagnostic(item, depth=depth + 1)
            )
        return result
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [
            _sanitized_ipc_diagnostic(item, depth=depth + 1) for item in value
        ]
    return _sanitized_ipc_error(value)


def _error_event_payload(value: Any) -> Any:
    if not isinstance(value, Mapping) and dataclasses.is_dataclass(value):
        value = dataclasses.asdict(value)
    if not isinstance(value, Mapping):
        for method_name in ("as_dict", "to_dict", "model_dump"):
            method = getattr(value, method_name, None)
            if not callable(method):
                continue
            try:
                projected = method()
            except TypeError:
                continue
            if isinstance(projected, Mapping):
                value = projected
                break
    if not isinstance(value, Mapping):
        return value
    event = dict(value)
    event_type = re.sub(
        r"[^a-z0-9]",
        "",
        _ANSI_ESCAPE_PATTERN.sub("", str(event.get("event_type") or "")).lower(),
    )
    if any(
        marker in event_type
        for marker in ("error", "fail", "exception", "warning", "diagnostic")
    ):
        event["content"] = _sanitized_ipc_error(event.get("content"))
    metadata = event.get("metadata")
    if isinstance(metadata, Mapping):
        event["metadata"] = _sanitized_ipc_diagnostic(metadata)
    return event


def _result_payload_for_ipc(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    error = result.get("error")
    if error not in (None, ""):
        result["error"] = _sanitized_ipc_error(error)
    status = str(result.get("status") or "").strip().lower()
    if error not in (None, "") or status in {"error", "failed", "failure"}:
        for field in ("content", "output"):
            if result.get(field):
                result[field] = _sanitized_ipc_error(result[field])
    events = result.get("events")
    if isinstance(events, Sequence) and not isinstance(
        events, (str, bytes, bytearray, memoryview)
    ):
        result["events"] = [_error_event_payload(event) for event in events]
    for field in ("metadata", "diagnostics"):
        metadata = result.get(field)
        if isinstance(metadata, Mapping):
            result[field] = _sanitized_ipc_diagnostic(metadata)
    return result


def _encode_message(message: Mapping[str, Any], max_bytes: int) -> bytes:
    try:
        encoded = json.dumps(
            _wire_value(message),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ProcessAgentProtocolError("IPC message is not canonical JSON") from exc
    if not encoded or len(encoded) > max_bytes:
        raise ProcessAgentProtocolError("IPC message exceeds its size bound")
    return encoded


def _decode_message(data: bytes, max_bytes: int) -> dict[str, Any]:
    if not data or len(data) > max_bytes:
        raise ProcessAgentProtocolError("IPC message exceeds its size bound")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProcessAgentProtocolError("IPC message is not valid UTF-8 JSON") from exc
    if type(value) is not dict:
        raise ProcessAgentProtocolError("IPC message must be an object")
    required = {"v", "type", "id", "reply_to", "agent_id", "generation", "payload"}
    if set(value) != required:
        raise ProcessAgentProtocolError("IPC envelope fields do not match")
    if value["v"] != _PROTOCOL_VERSION:
        raise ProcessAgentProtocolError("IPC protocol version does not match")
    if type(value["type"]) is not str or not value["type"]:
        raise ProcessAgentProtocolError("IPC message type is invalid")
    if type(value["id"]) is not str or not value["id"]:
        raise ProcessAgentProtocolError("IPC message identity is invalid")
    if value["reply_to"] is not None and (
        type(value["reply_to"]) is not str or not value["reply_to"]
    ):
        raise ProcessAgentProtocolError("IPC correlation identity is invalid")
    if type(value["agent_id"]) is not str:
        raise ProcessAgentProtocolError("IPC Agent identity is invalid")
    if type(value["generation"]) is not int or value["generation"] <= 0:
        raise ProcessAgentProtocolError("IPC generation is invalid")
    if type(value["payload"]) is not dict:
        raise ProcessAgentProtocolError("IPC payload must be an object")
    return value


def _message(
    message_type: str,
    *,
    agent_id: str,
    generation: int,
    payload: Mapping[str, Any] | None = None,
    reply_to: str | None = None,
    message_id: str | None = None,
) -> dict[str, Any]:
    return {
        "v": _PROTOCOL_VERSION,
        "type": str(message_type),
        "id": message_id or uuid.uuid4().hex,
        "reply_to": reply_to,
        "agent_id": agent_id,
        "generation": generation,
        "payload": dict(payload or {}),
    }


async def _recv_bytes(connection: Any) -> bytes:
    """Wait for one pipe message without parking a thread indefinitely."""

    loop = asyncio.get_running_loop()
    future: asyncio.Future[bytes] = loop.create_future()
    descriptor = connection.fileno()

    def ready() -> None:
        with contextlib.suppress(Exception):
            loop.remove_reader(descriptor)
        if future.done():
            return
        try:
            future.set_result(connection.recv_bytes())
        except BaseException as exc:
            future.set_exception(exc)

    try:
        loop.add_reader(descriptor, ready)
    except (AttributeError, NotImplementedError):  # pragma: no cover - POSIX target
        return await asyncio.to_thread(connection.recv_bytes)
    try:
        return await future
    finally:
        with contextlib.suppress(Exception):
            loop.remove_reader(descriptor)


def _parse_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return _datetime.datetime.fromisoformat(value)
    except ValueError:
        return value


def _task_payload(task: Any, *, bridge_capability: str | None = None) -> dict[str, Any]:
    if isinstance(task, Mapping):
        values = dict(task)
    else:
        method = getattr(task, "as_dict", None)
        if not callable(method):
            raise TypeError("run() requires an AgentTask-compatible value")
        values = dict(method())
    metadata = values.get("metadata")
    metadata_values = dict(metadata) if isinstance(metadata, Mapping) else {}
    if bridge_capability:
        existing = metadata_values.get(_BRIDGE_CAPABILITY_METADATA_KEY)
        if existing not in (None, "", bridge_capability):
            raise ProcessAgentProtocolError(
                "task metadata contains a conflicting bridge capability"
            )
        metadata_values[_BRIDGE_CAPABILITY_METADATA_KEY] = bridge_capability
    values["metadata"] = metadata_values
    # Reply destinations are supervisor/channel concerns and must never enter
    # the Agent child.  Runtime execution has no legitimate use for them.
    values["reply_target"] = None
    return _wire_value(values)


def _runtime_types() -> tuple[Any, Any, Any]:
    from src.agents.base import AgentEvent, AgentResult, AgentTask

    return AgentTask, AgentEvent, AgentResult


def _event_from_payload(payload: Mapping[str, Any]) -> Any:
    _AgentTask, AgentEvent, _AgentResult = _runtime_types()
    values = dict(_error_event_payload(payload))
    values["created_at"] = _parse_datetime(values.get("created_at"))
    values["attachments"] = tuple(values.get("attachments") or ())
    return AgentEvent(**values)


def _result_from_payload(payload: Mapping[str, Any]) -> Any:
    _AgentTask, _AgentEvent, AgentResult = _runtime_types()
    values = _result_payload_for_ipc(payload)
    values["events"] = tuple(
        _event_from_payload(item) for item in values.get("events") or ()
    )
    return AgentResult(**values)


def _resolve_import(path: str) -> Any:
    module_name, separator, qualname = path.partition(":")
    if not separator:
        raise ProcessAgentConfigurationError("invalid backend factory path")
    value: Any = importlib.import_module(module_name)
    for component in qualname.split("."):
        value = getattr(value, component)
    return value


def _supported_kwargs(callable_value: Callable[..., Any], values: Mapping[str, Any]) -> dict[str, Any]:
    try:
        parameters = inspect.signature(callable_value).parameters
    except (TypeError, ValueError):
        return dict(values)
    if any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ):
        return dict(values)
    return {key: value for key, value in values.items() if key in parameters}


def _linux_process_start_time(pid: int) -> int:
    value = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    closing = value.rfind(")")
    if closing < 0:
        raise RuntimeError("invalid Linux process identity")
    return int(value[closing + 2 :].split()[19])


def _arm_linux_parent_death(parent_pid: int, parent_start_time: int) -> None:
    """Make the Agent leader die if the exact supervisor process disappears."""

    if not sys.platform.startswith("linux"):
        raise RuntimeError("process-isolated Agents currently require Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = (
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    )
    prctl.restype = ctypes.c_int
    if prctl(_PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, "cannot arm Agent parent-death signal")
    # Close the documented PR_SET_PDEATHSIG race and numeric-PID reuse race.
    if os.getppid() != parent_pid:
        raise RuntimeError("Agent supervisor exited during spawn")
    if _linux_process_start_time(parent_pid) != parent_start_time:
        raise RuntimeError("Agent supervisor process identity changed")


def _watch_parent_liveness(connection: Any, process_group_id: int) -> None:
    """Kill the Agent process group when its supervisor lifetime ends.

    The parent owns the only write end of a generation-specific, one-way pipe
    and deliberately never writes to it.  EOF therefore proves that the
    supervisor no longer owns this child generation.  A dedicated thread keeps
    this proof live even while the Agent event loop is blocked in SDK or tool
    work.  Treat unexpected data or a pipe error as loss of the same contract.
    """

    try:
        connection.recv_bytes()
    except (EOFError, OSError):
        pass
    finally:
        with contextlib.suppress(Exception):
            connection.close()

    with contextlib.suppress(BaseException):
        os.killpg(process_group_id, signal.SIGKILL)
    os._exit(128 + int(signal.SIGKILL))


def _install_child_import_boundary() -> None:
    """Install only the child-safe ``src.runtime`` package search path.

    Importing ``src.runtime.media`` ordinarily executes ``src.runtime``'s
    application initializer first.  That initializer exposes SQLite, stores,
    workers, and channel-facing orchestration.  A child needs only the narrow
    helper modules imported by ``CodexRuntime``, so create the package shell
    without executing that initializer.
    """

    forbidden = (
        "_sqlite3",
        "sqlite3",
        "src.runtime.sqlite_store",
        "src.channels",
        "wechat_ilink",
    )
    loaded = tuple(sys.modules)
    violations = sorted(
        name
        for name in loaded
        if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
    )
    if violations:
        raise RuntimeError(
            "forbidden store/channel module entered Agent child: "
            + ", ".join(violations)
        )

    runtime_directory = str(Path(__file__).resolve().parent)
    repository_root = str(Path(__file__).resolve().parents[2])
    if repository_root not in sys.path:
        sys.path.insert(0, repository_root)
    package = types.ModuleType("src.runtime")
    package.__file__ = str(Path(runtime_directory) / "__init__.py")
    package.__package__ = "src.runtime"
    package.__path__ = [runtime_directory]  # type: ignore[attr-defined]
    specification = importlib.machinery.ModuleSpec(
        "src.runtime", loader=None, is_package=True
    )
    specification.submodule_search_locations = [runtime_directory]
    package.__spec__ = specification
    sys.modules["src.runtime"] = package


def _assert_child_import_boundary() -> None:
    forbidden = (
        "_sqlite3",
        "sqlite3",
        "src.runtime.sqlite_store",
        "src.runtime.store",
        "src.runtime.worker",
        "src.channels",
        "wechat_ilink",
    )
    loaded = tuple(sys.modules)
    violations = sorted(
        name
        for name in loaded
        if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
    )
    if violations:
        raise RuntimeError(
            "Agent child imported a store or channel dependency: "
            + ", ".join(violations)
        )


def _task_from_payload(payload: Mapping[str, Any]) -> Any:
    AgentTask, _AgentEvent, _AgentResult = _runtime_types()
    values = dict(payload)
    values["created_at"] = _parse_datetime(values.get("created_at"))
    return AgentTask.from_record(values)


async def _construct_child_runtime(
    config: Mapping[str, Any],
    session: "_ChildSession",
) -> Any:
    _install_child_import_boundary()
    agent_id = str(config["agent_id"])

    def bridge_capability(task: Any) -> str:
        metadata = getattr(task, "metadata", {})
        if not isinstance(metadata, Mapping):
            return ""
        return str(metadata.get(_BRIDGE_CAPABILITY_METADATA_KEY, "") or "")

    async def publish_image(task: Any, **kwargs: Any) -> Any:
        response = await session.request_parent(
            "artifact",
            {
                "run_id": session._active_run_id,
                "task": _task_payload(task),
                "proposal": _wire_value(kwargs),
            },
        )
        return response.get("value")

    constructor_values = {
        "agent_id": agent_id,
        "model": str(config.get("model", "") or ""),
        "codex_config_profile": str(
            config.get("codex_config_profile", "") or ""
        ),
        "cwd": config.get("cwd"),
        "turn_timeout": config.get("turn_timeout"),
        "managed_root": config.get("managed_root"),
        "trusted_skill_roots": tuple(config.get("trusted_skill_roots") or ()),
        "agent_bridge_command": config.get("agent_bridge_command"),
        "agent_bridge_capability_issuer": bridge_capability,
        "image_output_publisher": (
            publish_image if bool(config.get("image_publication_enabled")) else None
        ),
    }
    factory_path = config.get("backend_factory")
    if factory_path:
        factory = _resolve_import(str(factory_path))
        factory_values = _supported_kwargs(factory, constructor_values)
        if (
            constructor_values["codex_config_profile"]
            and "codex_config_profile" not in factory_values
        ):
            raise RuntimeError(
                "Agent backend does not support Codex config profiles"
            )
        created = factory(**factory_values)
    else:
        from src.agents.codex_runtime import CodexRuntime

        # CodexRuntime does not take an Agent ID constructor argument; the
        # immutable task still carries it, and assigning the descriptor keeps
        # status/introspection accurate inside this dedicated process.
        codex_values = dict(constructor_values)
        codex_values.pop("agent_id", None)
        created = CodexRuntime(**codex_values)
    runtime = await created if inspect.isawaitable(created) else created
    if runtime is None:
        raise RuntimeError("Agent backend factory returned no runtime")
    with contextlib.suppress(Exception):
        runtime.agent_id = agent_id
    start = getattr(runtime, "start", None)
    if not callable(start):
        raise RuntimeError("Agent backend does not implement start()")
    started = start()
    if inspect.isawaitable(started):
        await started
    _assert_child_import_boundary()
    return runtime


class _ChildSession:
    """One child generation's asynchronous private-pipe state machine."""

    _CONTROL_METHODS = frozenset(
        {
            "compact_session",
            "list_models",
            "list_skills",
            "resolve_skill",
            "reset_session",
        }
    )

    def __init__(self, connection: Any, config: Mapping[str, Any]) -> None:
        self.connection = connection
        self.config = dict(config)
        self.agent_id = str(config["agent_id"])
        self.generation = int(config["generation"])
        self.max_message_bytes = int(config["max_message_bytes"])
        self.ack_timeout = float(config["event_ack_timeout"])
        self.runtime: Any | None = None
        self._send_lock = asyncio.Lock()
        self._pending_parent: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._active_execution: asyncio.Task[None] | None = None
        self._active_run_id: str | None = None
        self._active_task_id: str | None = None
        self._execution_started = asyncio.Event()
        self._interrupt_latch: set[str] = set()
        self._stopping = False
        self._done = asyncio.Event()
        self._control_tasks: set[asyncio.Task[Any]] = set()

    async def send(
        self,
        message_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        reply_to: str | None = None,
        message_id: str | None = None,
    ) -> str:
        outgoing = _message(
            message_type,
            agent_id=self.agent_id,
            generation=self.generation,
            payload=payload,
            reply_to=reply_to,
            message_id=message_id,
        )
        encoded = _encode_message(outgoing, self.max_message_bytes)
        async with self._send_lock:
            await asyncio.to_thread(self.connection.send_bytes, encoded)
        return str(outgoing["id"])

    async def request_parent(
        self,
        message_type: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        request_id = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending_parent[request_id] = future
        try:
            await self.send(message_type, payload, message_id=request_id)
            response = await asyncio.wait_for(future, timeout=self.ack_timeout)
        finally:
            self._pending_parent.pop(request_id, None)
        if response.get("ok") is not True:
            raise ProcessAgentRemoteError(
                str(response.get("error") or f"parent rejected {message_type}")
            )
        return response

    def _validate(self, message: Mapping[str, Any]) -> None:
        if (
            message["agent_id"] != self.agent_id
            or message["generation"] != self.generation
        ):
            raise ProcessAgentProtocolError("IPC generation identity conflicts")

    async def run(self) -> int:
        try:
            self.runtime = await _construct_child_runtime(self.config, self)
            await self.send(
                "ready",
                {
                    "pid": os.getpid(),
                    "process_group_id": os.getpgrp(),
                    "methods": sorted(self._CONTROL_METHODS),
                },
            )
            await self._receive_loop()
            return 0
        finally:
            self._done.set()
            if self._active_execution is not None and not self._active_execution.done():
                self._active_execution.cancel()
                with contextlib.suppress(BaseException):
                    await self._active_execution
            for task in tuple(self._control_tasks):
                if not task.done():
                    task.cancel()
            if self._control_tasks:
                with contextlib.suppress(BaseException):
                    await asyncio.gather(*self._control_tasks, return_exceptions=True)
            if self.runtime is not None:
                stop = getattr(self.runtime, "stop", None)
                if callable(stop):
                    with contextlib.suppress(BaseException):
                        stopped = stop()
                        if inspect.isawaitable(stopped):
                            await stopped
            for future in self._pending_parent.values():
                if not future.done():
                    future.set_exception(EOFError("supervisor IPC closed"))
            self._pending_parent.clear()

    async def _receive_loop(self) -> None:
        while not self._done.is_set():
            receive = asyncio.create_task(_recv_bytes(self.connection))
            stopped = asyncio.create_task(self._done.wait())
            try:
                completed, _pending = await asyncio.wait(
                    {receive, stopped}, return_when=asyncio.FIRST_COMPLETED
                )
                if stopped in completed:
                    receive.cancel()
                    with contextlib.suppress(BaseException):
                        await receive
                    return
                stopped.cancel()
                with contextlib.suppress(BaseException):
                    await stopped
                raw = await receive
            except (EOFError, OSError):
                return
            incoming = _decode_message(raw, self.max_message_bytes)
            self._validate(incoming)
            reply_to = incoming["reply_to"]
            if reply_to in self._pending_parent:
                future = self._pending_parent[reply_to]
                if not future.done():
                    future.set_result(dict(incoming["payload"]))
                continue
            if reply_to is not None:
                raise ProcessAgentProtocolError("unexpected child IPC correlation")

            kind = incoming["type"]
            if kind == "run":
                await self._accept_run(incoming)
            elif kind == "interrupt":
                task = asyncio.create_task(self._handle_interrupt(incoming))
                self._track_control_task(task)
            elif kind == "call":
                task = asyncio.create_task(self._handle_call(incoming))
                self._track_control_task(task)
            elif kind == "stop":
                if not self._stopping:
                    self._stopping = True
                    task = asyncio.create_task(self._shutdown(incoming))
                    self._track_control_task(task)
            elif kind == "ping":
                await self.send(
                    "response",
                    {"ok": True, "value": incoming["payload"].get("nonce")},
                    reply_to=incoming["id"],
                )
            else:
                raise ProcessAgentProtocolError(f"unsupported parent message {kind}")

    def _track_control_task(self, task: asyncio.Task[Any]) -> None:
        self._control_tasks.add(task)
        task.add_done_callback(self._control_tasks.discard)

    async def _accept_run(self, incoming: Mapping[str, Any]) -> None:
        if self._stopping:
            await self._send_error(incoming, "Agent process is stopping")
            return
        if self._active_execution is not None and not self._active_execution.done():
            await self._send_error(incoming, "Agent process execution slot is busy")
            return
        task_payload = incoming["payload"].get("task")
        if type(task_payload) is not dict:
            await self._send_error(incoming, "run task payload is invalid")
            return
        task_id = str(task_payload.get("task_id", "") or "")
        if not task_id:
            await self._send_error(incoming, "run task identity is invalid")
            return
        # Latch the accepted identity before yielding to create_task.  An
        # immediate INTERRUPT can now match the assigned task even before the
        # backend registers its native turn.
        self._active_run_id = str(incoming["id"])
        self._active_task_id = task_id
        self._execution_started.clear()
        execution = asyncio.create_task(
            self._execute_run(str(incoming["id"]), task_payload)
        )
        self._active_execution = execution

    async def _execute_run(
        self,
        request_id: str,
        task_payload: Mapping[str, Any],
    ) -> None:
        self._execution_started.set()
        _AgentTask, _AgentEvent, AgentResult = _runtime_types()
        task: Any | None = None
        try:
            task = _task_from_payload(task_payload)
            if str(task.agent_id) != self.agent_id:
                raise ProcessAgentProtocolError(
                    "task Agent identity does not match child owner"
                )
            self._active_task_id = str(task.task_id)
            if self._active_task_id in self._interrupt_latch:
                result = AgentResult(
                    task_id=self._active_task_id,
                    execution_id=task.execution_id or None,
                    status="interrupted",
                    interrupted=True,
                )
            else:
                runtime = self.runtime
                if runtime is None:
                    raise RuntimeError("Agent runtime is not initialized")

                async def emit(event: Any) -> None:
                    event_values = (
                        event.as_dict()
                        if callable(getattr(event, "as_dict", None))
                        else event
                    )
                    await self.request_parent(
                        "event",
                        {
                            "run_id": request_id,
                            "event": _wire_value(_error_event_payload(event_values)),
                        },
                    )

                result = runtime.run(task, emit)
                if inspect.isawaitable(result):
                    result = await result
            if not callable(getattr(result, "as_dict", None)):
                if isinstance(result, Mapping):
                    result = AgentResult(**dict(result))
                else:
                    raise RuntimeError("Agent backend returned an invalid result")
        except asyncio.CancelledError:
            if task is None:
                task_id = str(task_payload.get("task_id", ""))
                execution_id = task_payload.get("execution_id")
            else:
                task_id = str(task.task_id)
                execution_id = task.execution_id or None
            result = AgentResult(
                task_id=task_id,
                execution_id=execution_id,
                status="interrupted",
                interrupted=True,
            )
        except BaseException as exc:
            task_id = (
                str(task.task_id)
                if task is not None
                else str(task_payload.get("task_id", ""))
            )
            execution_id = (
                task.execution_id or None
                if task is not None
                else task_payload.get("execution_id")
            )
            result = AgentResult(
                task_id=task_id,
                execution_id=execution_id,
                status="failed",
                error=_sanitized_ipc_error(
                    str(exc), fallback=exc.__class__.__name__
                ),
            )
        finally:
            if self._active_task_id is not None:
                self._interrupt_latch.discard(self._active_task_id)
            self._active_run_id = None
            self._active_task_id = None
        result_payload = _result_payload_for_ipc(dict(result.as_dict()))
        await self.send(
            "result",
            {"ok": True, "result": _wire_value(result_payload)},
            reply_to=request_id,
        )

    async def _handle_interrupt(self, incoming: Mapping[str, Any]) -> None:
        try:
            task_id = str(incoming["payload"].get("task_id", "") or "")
            accepted = False
            if task_id and task_id == self._active_task_id:
                self._interrupt_latch.add(task_id)
                runtime = self.runtime
                interrupt = getattr(runtime, "interrupt", None)
                if callable(interrupt):
                    outcome = interrupt(task_id)
                    accepted = bool(
                        await outcome if inspect.isawaitable(outcome) else outcome
                    )
                if (
                    not accepted
                    and self._active_execution is not None
                    and self._execution_started.is_set()
                ):
                    # Covers the short post-grant/pre-SDK-registration race.
                    self._active_execution.cancel()
                    accepted = True
                elif not accepted:
                    # The accepted task has not entered its coroutine body yet.
                    # Its latch is checked before backend.run(), so cancellation
                    # here would only suppress the required terminal RESULT.
                    accepted = True
            payload = {"ok": True, "value": accepted}
        except BaseException as exc:
            payload = {
                "ok": False,
                "error": _sanitized_ipc_error(
                    str(exc), fallback=exc.__class__.__name__
                ),
            }
        await self.send(
            "interrupt_ack",
            payload,
            reply_to=str(incoming["id"]),
        )

    async def _handle_call(self, incoming: Mapping[str, Any]) -> None:
        try:
            if self._stopping:
                raise RuntimeError("Agent process is stopping")
            if self._active_execution is not None and not self._active_execution.done():
                raise RuntimeError("Agent process execution slot is busy")
            method_name = str(incoming["payload"].get("method", "") or "")
            if method_name not in self._CONTROL_METHODS:
                raise RuntimeError(f"unsupported Agent control method {method_name}")
            runtime = self.runtime
            method = getattr(runtime, method_name, None)
            if not callable(method):
                raise RuntimeError(f"Agent backend does not implement {method_name}()")
            args = incoming["payload"].get("args") or []
            kwargs = incoming["payload"].get("kwargs") or {}
            if type(args) is not list or type(kwargs) is not dict:
                raise RuntimeError("Agent control arguments are invalid")
            value = method(*args, **kwargs)
            if inspect.isawaitable(value):
                value = await value
            payload = {"ok": True, "value": _wire_value(value)}
        except BaseException as exc:
            payload = {
                "ok": False,
                "error": _sanitized_ipc_error(
                    str(exc), fallback=exc.__class__.__name__
                ),
            }
        await self.send(
            "response", payload, reply_to=str(incoming["id"])
        )

    async def _shutdown(self, incoming: Mapping[str, Any]) -> None:
        error: str | None = None
        try:
            if self._active_task_id:
                runtime = self.runtime
                interrupt = getattr(runtime, "interrupt", None)
                if callable(interrupt):
                    outcome = interrupt(self._active_task_id)
                    if inspect.isawaitable(outcome):
                        await outcome
            if self._active_execution is not None and not self._active_execution.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._active_execution),
                        timeout=max(0.1, self.ack_timeout),
                    )
                except asyncio.TimeoutError:
                    self._active_execution.cancel()
                    with contextlib.suppress(BaseException):
                        await self._active_execution
            runtime = self.runtime
            stop = getattr(runtime, "stop", None)
            if callable(stop):
                stopped = stop()
                if inspect.isawaitable(stopped):
                    await stopped
            self.runtime = None
        except BaseException as exc:
            error = _sanitized_ipc_error(
                str(exc), fallback=exc.__class__.__name__
            )
        await self.send(
            "stopped",
            {"ok": error is None, "error": error},
            reply_to=str(incoming["id"]),
        )
        self._done.set()

    async def _send_error(self, incoming: Mapping[str, Any], error: str) -> None:
        await self.send(
            "error",
            {"ok": False, "error": _sanitized_ipc_error(error)},
            reply_to=str(incoming["id"]),
        )


async def _run_child(
    connection: Any,
    parent_liveness_connection: Any,
    config: Mapping[str, Any],
) -> int:
    parent_pid = int(config["parent_pid"])
    os.setsid()
    process_group_id = os.getpgrp()
    if process_group_id != os.getpid():
        raise RuntimeError("Agent child did not acquire a dedicated process group")
    os.set_inheritable(parent_liveness_connection.fileno(), False)

    liveness_thread = threading.Thread(
        target=_watch_parent_liveness,
        args=(parent_liveness_connection, process_group_id),
        name="cow-agent-parent-liveness",
        daemon=True,
    )
    liveness_thread.start()

    def parent_died(_signum: int, _frame: Any) -> None:
        # The Agent leader owns a dedicated session/process group.  Escalate
        # a termination request to that whole group so an SDK or tool
        # descendant cannot outlive its Agent leader.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        with contextlib.suppress(BaseException):
            os.killpg(os.getpgrp(), signal.SIGKILL)
        os._exit(128 + int(signal.SIGTERM))

    signal.signal(signal.SIGTERM, parent_died)
    if sys.platform.startswith("linux"):
        _arm_linux_parent_death(parent_pid, int(config["parent_start_time"]))
    elif os.getppid() != parent_pid:
        raise RuntimeError("Agent supervisor exited during spawn")
    session = _ChildSession(connection, config)
    try:
        return await session.run()
    except BaseException as exc:
        with contextlib.suppress(BaseException):
            await session.send(
                "fatal",
                {
                    "ok": False,
                    "error": _sanitized_ipc_error(
                        str(exc), fallback=exc.__class__.__name__
                    ),
                },
            )
        return 70
    finally:
        with contextlib.suppress(Exception):
            connection.close()


def _child_process_main(
    connection: Any,
    parent_liveness_connection: Any,
    config: Mapping[str, Any],
) -> None:
    """Spawn target.  It must remain importable as top-level ``process_agent``."""

    try:
        returncode = asyncio.run(
            _run_child(connection, parent_liveness_connection, config)
        )
    except BaseException:
        returncode = 70
    raise SystemExit(returncode)


class ProcessAgentRuntime:
    """Parent-side proxy for exactly one persistent Agent child process."""

    process_isolated = True

    def __init__(
        self,
        agent_id: str,
        *,
        model: str = "",
        codex_config_profile: str = "",
        cwd: str | os.PathLike[str] | None = None,
        turn_timeout: float | None = 60,
        managed_root: str | os.PathLike[str] | None = None,
        trusted_skill_roots: Sequence[str | os.PathLike[str]] = (),
        image_output_publisher: Callable[..., Any] | None = None,
        agent_bridge_command: Sequence[str] | str | None = None,
        agent_bridge_capability_issuer: Callable[[Any], Any] | None = None,
        backend_factory: str | Callable[..., Any] | None = None,
        start_timeout: float = 10,
        stop_timeout: float = 10,
        event_ack_timeout: float = 30,
        max_processes: int | None = None,
        max_message_bytes: int = _DEFAULT_MAX_MESSAGE_BYTES,
        mp_context: Any | None = None,
        _process_budget: _SharedProcessBudget | None = None,
    ) -> None:
        canonical_agent_id = str(agent_id or "").strip()
        if not _AGENT_ID_PATTERN.fullmatch(canonical_agent_id):
            raise ProcessAgentConfigurationError(
                "agent_id must be lowercase ASCII and start with a letter"
            )
        if codex_config_profile is not None and not isinstance(
            codex_config_profile, str
        ):
            raise ProcessAgentConfigurationError(
                "codex_config_profile must be a safe profile name"
            )
        canonical_config_profile = str(codex_config_profile or "").strip()
        if canonical_config_profile and not _CONFIG_PROFILE_PATTERN.fullmatch(
            canonical_config_profile
        ):
            raise ProcessAgentConfigurationError(
                "codex_config_profile must be a safe profile name"
            )
        if turn_timeout is not None:
            turn_timeout = _positive_timeout(turn_timeout, "turn_timeout")
        if type(max_message_bytes) is not int or not (
            1024 <= max_message_bytes <= 64 * 1024 * 1024
        ):
            raise ProcessAgentConfigurationError(
                "max_message_bytes is outside the supported bound"
            )
        if max_processes is not None and (
            type(max_processes) is not int or max_processes <= 0
        ):
            raise ProcessAgentConfigurationError("max_processes must be positive")
        if _process_budget is not None and max_processes is not None:
            if _process_budget.limit != max_processes:
                raise ProcessAgentConfigurationError(
                    "clone process limit conflicts with its shared budget"
                )

        self.agent_id = canonical_agent_id
        self.model = str(model or "")
        self.codex_config_profile = canonical_config_profile
        self.cwd = (
            str(Path(cwd).expanduser().resolve()) if cwd is not None else None
        )
        self.turn_timeout = turn_timeout
        self.managed_root = (
            str(Path(managed_root).expanduser().resolve())
            if managed_root is not None
            else None
        )
        self.trusted_skill_roots = tuple(
            str(Path(root).expanduser().resolve()) for root in trusted_skill_roots
        )
        self.image_output_publisher = image_output_publisher
        if isinstance(agent_bridge_command, str):
            self.agent_bridge_command: str | tuple[str, ...] | None = (
                agent_bridge_command.strip() or None
            )
        elif agent_bridge_command:
            self.agent_bridge_command = tuple(str(value) for value in agent_bridge_command)
        else:
            self.agent_bridge_command = None
        self.agent_bridge_capability_issuer = agent_bridge_capability_issuer
        self.backend_factory = _factory_path(backend_factory)
        self.start_timeout = _positive_timeout(start_timeout, "start_timeout")
        self.stop_timeout = _positive_timeout(stop_timeout, "stop_timeout")
        self.event_ack_timeout = _positive_timeout(
            event_ack_timeout, "event_ack_timeout"
        )
        self.max_message_bytes = max_message_bytes
        self.max_processes = (
            _process_budget.limit
            if _process_budget is not None
            else int(max_processes or _DEFAULT_PROCESS_BUDGET.limit)
        )
        self._process_budget = (
            _process_budget
            if _process_budget is not None
            else _SharedProcessBudget(max_processes)
            if max_processes is not None
            else _DEFAULT_PROCESS_BUDGET
        )
        if self._process_budget.limit != self.max_processes:
            raise ProcessAgentConfigurationError("shared process limit conflicts")
        self._mp_context = mp_context or multiprocessing.get_context("spawn")
        if getattr(self._mp_context, "get_start_method", lambda: "")() != "spawn":
            raise ProcessAgentConfigurationError(
                "ProcessAgentRuntime requires a multiprocessing spawn context"
            )

        self._health = ProcessAgentHealth.NEW
        self._generation = 0
        self._process: Any | None = None
        self._process_group_id: int | None = None
        self._connection: Any | None = None
        self._parent_liveness_connection: Any | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._ready_future: asyncio.Future[dict[str, Any]] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._send_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()
        self._slot_lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._budget_reserved = False
        self._expected_stop = False
        self._active_run_id: str | None = None
        self._active_task_id: str | None = None
        self._active_task: Any | None = None
        self._active_emit: Callable[[Any], Any] | None = None
        self._lost_error: ProcessAgentLostError | None = None

    @classmethod
    def create(cls, agent_id: str, **kwargs: Any) -> "ProcessAgentRuntime":
        """Construct an unstarted root proxy.

        Direct construction is equivalent; the named constructor makes the
        distinction from instance-level :meth:`for_agent` cloning explicit.
        """

        return cls(agent_id, **kwargs)

    def for_agent(
        self,
        agent_id: str,
        *,
        codex_config_profile: str | None = None,
    ) -> "ProcessAgentRuntime":
        """Return a new unstarted proxy with this template's exact config."""

        return type(self)(
            agent_id,
            model=self.model,
            codex_config_profile=(
                self.codex_config_profile
                if codex_config_profile is None
                else codex_config_profile
            ),
            cwd=self.cwd,
            turn_timeout=self.turn_timeout,
            managed_root=self.managed_root,
            trusted_skill_roots=self.trusted_skill_roots,
            image_output_publisher=self.image_output_publisher,
            agent_bridge_command=self.agent_bridge_command,
            agent_bridge_capability_issuer=self.agent_bridge_capability_issuer,
            backend_factory=self.backend_factory,
            start_timeout=self.start_timeout,
            stop_timeout=self.stop_timeout,
            event_ack_timeout=self.event_ack_timeout,
            max_processes=self.max_processes,
            max_message_bytes=self.max_message_bytes,
            mp_context=self._mp_context,
            _process_budget=self._process_budget,
        )

    @property
    def pid(self) -> int | None:
        process = self._process
        return int(process.pid) if process is not None and process.pid else None

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def health(self) -> str:
        process = self._process
        if (
            process is not None
            and self._health
            in {ProcessAgentHealth.STARTING, ProcessAgentHealth.READY, ProcessAgentHealth.BUSY}
            and process.exitcode is not None
        ):
            return ProcessAgentHealth.LOST.value
        return self._health.value

    @property
    def process_group_id(self) -> int | None:
        return self._process_group_id

    def _assert_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                "ProcessAgentRuntime must stay on its owning asyncio loop"
            )

    async def start(self) -> None:
        """Start or restart this Agent's exact child generation."""

        self._assert_loop()
        async with self._lifecycle_lock:
            if self._health in {ProcessAgentHealth.READY, ProcessAgentHealth.BUSY}:
                if self._process is not None and self._process.exitcode is None:
                    return
            await self._discard_previous_generation()
            self._process_budget.reserve(self)
            self._budget_reserved = True
            self._generation += 1
            generation = self._generation
            self._health = ProcessAgentHealth.STARTING
            self._expected_stop = False
            self._lost_error = None

            parent_connection = child_connection = None
            child_liveness_connection = parent_liveness_connection = None
            inserted_path = False
            try:
                parent_connection, child_connection = self._mp_context.Pipe(duplex=True)
                (
                    child_liveness_connection,
                    parent_liveness_connection,
                ) = self._mp_context.Pipe(duplex=False)
                os.set_inheritable(parent_liveness_connection.fileno(), False)
                config = self._child_config(generation)
                target, module_directory, inserted_path = self._spawn_target()
                process = self._mp_context.Process(
                    target=target,
                    args=(child_connection, child_liveness_connection, config),
                    name=f"cow-agent-{self.agent_id}-g{generation}",
                    daemon=False,
                )
                # multiprocessing's spawn bootstrap normally re-executes the
                # supervisor's ``__main__`` file before importing its target.
                # ``./cow`` imports SQLite, channels, and credentials at module
                # scope, so that default would contaminate every Agent child
                # before our boundary code can run.  During the synchronous
                # preparation snapshot, identify this dependency-free module
                # as the harmless child main.  The target remains an ordinary
                # spawn-context Process; only ``init_main_from_*`` is narrowed.
                main_module = sys.modules.get("__main__")
                bootstrap_module = sys.modules.get("process_agent")
                bootstrap_spec = getattr(bootstrap_module, "__spec__", None)
                if main_module is None or bootstrap_spec is None:
                    raise ProcessAgentConfigurationError(
                        "spawn child main module cannot be isolated"
                    )
                with _SPAWN_MAIN_LOCK:
                    original_spec = getattr(main_module, "__spec__", None)
                    try:
                        main_module.__spec__ = bootstrap_spec
                        process.start()
                    finally:
                        main_module.__spec__ = original_spec
                self._process = process
                self._parent_liveness_connection = parent_liveness_connection
                parent_liveness_connection = None
                child_connection.close()
                child_connection = None
                child_liveness_connection.close()
                child_liveness_connection = None
                self._connection = parent_connection
                parent_connection = None
                self._ready_future = asyncio.get_running_loop().create_future()
                self._reader_task = asyncio.create_task(
                    self._reader_loop(generation),
                    name=f"cow-agent-ipc-{self.agent_id}-g{generation}",
                )
                ready = await asyncio.wait_for(
                    asyncio.shield(self._ready_future), timeout=self.start_timeout
                )
                pid = ready.get("pid")
                process_group_id = ready.get("process_group_id")
                if pid != process.pid or process_group_id != process.pid:
                    raise ProcessAgentProtocolError(
                        "READY process identity does not match spawned child"
                    )
                self._process_group_id = int(process_group_id)
                self._health = ProcessAgentHealth.READY
            except BaseException as exc:
                await self._force_cleanup_current_generation()
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    raise
                if isinstance(exc, ProcessAgentCapacityError):
                    raise
                if isinstance(exc, ProcessAgentStartupError):
                    raise
                raise ProcessAgentStartupError(
                    f"Agent {self.agent_id} child failed to become ready: {exc}"
                ) from exc
            finally:
                if inserted_path:
                    with contextlib.suppress(ValueError):
                        sys.path.remove(module_directory)
                if parent_connection is not None:
                    parent_connection.close()
                if child_connection is not None:
                    child_connection.close()
                if child_liveness_connection is not None:
                    child_liveness_connection.close()
                if parent_liveness_connection is not None:
                    parent_liveness_connection.close()

    def _spawn_target(self) -> tuple[Callable[..., Any], str, bool]:
        """Load this file by a package-free name for the spawn target."""

        module_directory = str(Path(__file__).resolve().parent)
        inserted = False
        if module_directory not in sys.path:
            sys.path.insert(0, module_directory)
            inserted = True
        bootstrap = importlib.import_module("process_agent")
        if Path(bootstrap.__file__).resolve() != Path(__file__).resolve():
            raise ProcessAgentConfigurationError(
                "spawn bootstrap resolved an unexpected process_agent module"
            )
        return bootstrap._child_process_main, module_directory, inserted

    def _child_config(self, generation: int) -> dict[str, Any]:
        parent_pid = os.getpid()
        if (
            os.name != "posix"
            or not hasattr(os, "setsid")
            or not hasattr(os, "killpg")
        ):
            raise ProcessAgentConfigurationError(
                "process-isolated Agents require POSIX process-group support"
            )
        config = {
            "agent_id": self.agent_id,
            "generation": generation,
            "model": self.model,
            "codex_config_profile": self.codex_config_profile,
            "cwd": self.cwd,
            "turn_timeout": self.turn_timeout,
            "managed_root": self.managed_root,
            "trusted_skill_roots": list(self.trusted_skill_roots),
            "agent_bridge_command": self.agent_bridge_command,
            "backend_factory": self.backend_factory,
            "image_publication_enabled": self.image_output_publisher is not None,
            "event_ack_timeout": self.event_ack_timeout,
            "max_message_bytes": self.max_message_bytes,
            "parent_pid": parent_pid,
        }
        if sys.platform.startswith("linux"):
            config["parent_start_time"] = _linux_process_start_time(parent_pid)
        return config

    async def run(self, task: Any, emit: Callable[[Any], Any] | None = None) -> Any:
        """Execute one immutable task in this Agent's persistent child.

        Calls on the same proxy serialize at the parent boundary.  Calls on
        distinct Agent proxies use distinct OS processes and can overlap.
        """

        self._assert_loop()
        await self.start()
        async with self._slot_lock:
            # A queued call may wake after stop/loss; restore a child before
            # sending rather than writing to a stale generation pipe.
            await self.start()
            task_id = str(
                task.get("task_id", "")
                if isinstance(task, Mapping)
                else getattr(task, "task_id", "")
            )
            if not task_id:
                raise ValueError("Agent task_id is required")
            task_agent_id = str(
                task.get("agent_id", self.agent_id)
                if isinstance(task, Mapping)
                else getattr(task, "agent_id", self.agent_id)
            )
            if task_agent_id != self.agent_id:
                raise ValueError("task Agent identity does not match this proxy")
            capability: str | None = None
            issuer = self.agent_bridge_capability_issuer
            if issuer is not None:
                issued = issuer(task)
                capability = str(
                    await issued if inspect.isawaitable(issued) else issued or ""
                ).strip() or None
            payload = _task_payload(task, bridge_capability=capability)
            run_id = uuid.uuid4().hex
            self._active_run_id = run_id
            self._active_task_id = task_id
            self._active_task = task
            self._active_emit = emit
            self._health = ProcessAgentHealth.BUSY
            future = self._register_pending(run_id)
            try:
                await self._send("run", {"task": payload}, message_id=run_id)
                response = await asyncio.shield(future)
                response_payload = self._successful_payload(response, "result")
                result = response_payload.get("result")
                if type(result) is not dict:
                    raise ProcessAgentProtocolError("child RESULT payload is invalid")
                return _result_from_payload(result)
            except asyncio.CancelledError:
                # Never detach an executing child turn when its supervisor
                # coroutine is cancelled.  Interrupt first, then allow the
                # caller's cancellation to propagate.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self.interrupt(task_id))
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(
                        asyncio.shield(future), timeout=self.stop_timeout
                    )
                raise
            finally:
                self._pending.pop(run_id, None)
                self._active_run_id = None
                self._active_task_id = None
                self._active_task = None
                self._active_emit = None
                if self._health is ProcessAgentHealth.BUSY:
                    self._health = ProcessAgentHealth.READY

    async def interrupt(self, task_id: str) -> bool:
        """Interrupt only the active task in this exact Agent process."""

        self._assert_loop()
        if (
            not task_id
            or self._process is None
            or self._process.exitcode is not None
            or self._health not in {ProcessAgentHealth.READY, ProcessAgentHealth.BUSY}
        ):
            return False
        response = await self._request(
            "interrupt", {"task_id": str(task_id)}, expected="interrupt_ack"
        )
        return bool(response.get("value"))

    async def list_models(self, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        value = await self._control_call(
            "list_models", kwargs={"include_hidden": bool(include_hidden)}
        )
        return [dict(item) for item in value or ()]

    async def list_skills(
        self,
        *,
        refresh: bool = False,
        cwd: str | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._control_call(
            "list_skills",
            kwargs={"refresh": bool(refresh), "cwd": cwd},
        )
        return [dict(item) for item in value or ()]

    async def resolve_skill(
        self,
        name: str,
        *,
        refresh: bool = False,
        cwd: str | None = None,
    ) -> dict[str, Any] | None:
        value = await self._control_call(
            "resolve_skill",
            args=[str(name)],
            kwargs={"refresh": bool(refresh), "cwd": cwd},
        )
        return dict(value) if isinstance(value, Mapping) else None

    async def reset_session(self, conversation_id: str) -> str:
        value = await self._control_call("reset_session", args=[str(conversation_id)])
        return str(value or "")

    async def compact_session(
        self,
        conversation_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Run native context compaction inside this Agent's child process."""

        value = await self._control_call(
            "compact_session",
            args=[str(conversation_id)],
            kwargs=kwargs,
        )
        if not isinstance(value, Mapping):
            raise ProcessAgentProtocolError(
                "child context-compaction result is invalid"
            )
        return dict(value)

    compact_conversation = compact_session

    async def _control_call(
        self,
        method: str,
        *,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        self._assert_loop()
        await self.start()
        async with self._slot_lock:
            await self.start()
            response = await self._request(
                "call",
                {
                    "method": method,
                    "args": _wire_value(list(args)),
                    "kwargs": _wire_value(dict(kwargs or {})),
                },
                expected="response",
            )
            return response.get("value")

    async def ping(self, nonce: str | None = None) -> str:
        self._assert_loop()
        await self.start()
        value = str(nonce or uuid.uuid4().hex)
        response = await self._request("ping", {"nonce": value}, expected="response")
        if response.get("value") != value:
            raise ProcessAgentProtocolError("child PING response conflicts")
        return value

    async def stop(self) -> None:
        """Gracefully stop and reap this proxy's exact child process group."""

        self._assert_loop()
        async with self._lifecycle_lock:
            if self._process is None:
                self._close_parent_liveness_connection()
                self._health = ProcessAgentHealth.STOPPED
                self._release_budget()
                return
            if self._process.exitcode is not None or self._health is ProcessAgentHealth.LOST:
                await self._discard_previous_generation()
                self._health = ProcessAgentHealth.STOPPED
                return
            self._health = ProcessAgentHealth.STOPPING
            self._expected_stop = True
            try:
                response = await asyncio.wait_for(
                    self._request("stop", {}, expected="stopped"),
                    timeout=self.stop_timeout,
                )
                if response.get("error"):
                    raise ProcessAgentRemoteError(str(response["error"]))
                process = self._process
                if process is not None:
                    await self._terminate_process(process, graceful=True)
            except BaseException as exc:
                await self._force_cleanup_current_generation()
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                if isinstance(exc, ProcessAgentRemoteError):
                    raise
                raise ProcessAgentError(
                    f"Agent {self.agent_id} did not stop cleanly"
                ) from exc
            self._close_parent_liveness_connection()
            await self._close_parent_connection()
            reader = self._reader_task
            if reader is not None and reader is not asyncio.current_task():
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(asyncio.shield(reader), timeout=1)
            self._reader_task = None
            self._process = None
            self._process_group_id = None
            self._ready_future = None
            self._health = ProcessAgentHealth.STOPPED
            self._release_budget()

    async def _request(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        expected: str,
    ) -> dict[str, Any]:
        request_id = uuid.uuid4().hex
        future = self._register_pending(request_id)
        try:
            await self._send(kind, payload, message_id=request_id)
            response = await asyncio.shield(future)
            return self._successful_payload(response, expected)
        finally:
            self._pending.pop(request_id, None)

    def _register_pending(self, request_id: str) -> asyncio.Future[dict[str, Any]]:
        if request_id in self._pending:
            raise ProcessAgentProtocolError("duplicate parent request identity")
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request_id] = future
        return future

    @staticmethod
    def _successful_payload(
        response: Mapping[str, Any], expected: str
    ) -> dict[str, Any]:
        if response.get("type") != expected:
            if response.get("type") == "error":
                raise ProcessAgentRemoteError(
                    str(response["payload"].get("error") or "child rejected operation")
                )
            raise ProcessAgentProtocolError(
                f"expected child {expected}, received {response.get('type')}"
            )
        payload = dict(response["payload"])
        if payload.get("ok") is not True:
            raise ProcessAgentRemoteError(
                str(payload.get("error") or "child operation failed")
            )
        return payload

    async def _send(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        reply_to: str | None = None,
        message_id: str | None = None,
        generation: int | None = None,
    ) -> str:
        connection = self._connection
        selected_generation = self._generation if generation is None else generation
        if connection is None:
            raise self._lost_exception("Agent child IPC is not connected")
        outgoing = _message(
            kind,
            agent_id=self.agent_id,
            generation=selected_generation,
            payload=payload,
            reply_to=reply_to,
            message_id=message_id,
        )
        encoded = _encode_message(outgoing, self.max_message_bytes)
        try:
            async with self._send_lock:
                await asyncio.to_thread(connection.send_bytes, encoded)
        except (BrokenPipeError, EOFError, OSError) as exc:
            raise self._lost_exception("Agent child IPC send failed") from exc
        return str(outgoing["id"])

    async def _reader_loop(self, generation: int) -> None:
        connection = self._connection
        if connection is None:
            return
        failure: BaseException | None = None
        try:
            while generation == self._generation:
                raw = await _recv_bytes(connection)
                incoming = _decode_message(raw, self.max_message_bytes)
                if (
                    incoming["agent_id"] != self.agent_id
                    or incoming["generation"] != generation
                ):
                    raise ProcessAgentProtocolError(
                        "child IPC generation identity conflicts"
                    )
                await self._dispatch_child_message(incoming, generation)
        except asyncio.CancelledError:
            return
        except (EOFError, BrokenPipeError, OSError) as exc:
            failure = exc
        except BaseException as exc:
            failure = exc
        finally:
            if generation == self._generation:
                await self._record_connection_loss(generation, failure)

    async def _dispatch_child_message(
        self, incoming: Mapping[str, Any], generation: int
    ) -> None:
        kind = str(incoming["type"])
        reply_to = incoming["reply_to"]
        if kind == "ready":
            if reply_to is not None:
                raise ProcessAgentProtocolError("READY cannot be correlated")
            ready = self._ready_future
            if ready is None or ready.done():
                raise ProcessAgentProtocolError("duplicate or unexpected READY")
            ready.set_result(dict(incoming["payload"]))
            return
        if kind == "event":
            if reply_to is not None:
                raise ProcessAgentProtocolError("EVENT cannot be a response")
            await self._handle_child_event(incoming, generation)
            return
        if kind == "artifact":
            if reply_to is not None:
                raise ProcessAgentProtocolError("artifact proposal cannot be a response")
            await self._handle_child_artifact(incoming, generation)
            return
        if kind == "fatal":
            raise ProcessAgentRemoteError(
                str(incoming["payload"].get("error") or "Agent child failed")
            )
        if reply_to is None:
            raise ProcessAgentProtocolError(
                f"child {kind} message is missing correlation"
            )
        future = self._pending.get(str(reply_to))
        if future is None:
            # A caller can time out/cancel after asking the child to stop.  A
            # late terminal response is harmless once that exact request has
            # no waiter; generation identity still prevents cross-run reuse.
            return
        if future.done():
            raise ProcessAgentProtocolError("duplicate child response")
        future.set_result(dict(incoming))

    async def _handle_child_event(
        self, incoming: Mapping[str, Any], generation: int
    ) -> None:
        payload = incoming["payload"]
        run_id = str(payload.get("run_id", "") or "")
        event_payload = payload.get("event")
        ok = False
        error: str | None = None
        try:
            if run_id != self._active_run_id:
                raise ProcessAgentProtocolError("EVENT run correlation conflicts")
            if type(event_payload) is not dict:
                raise ProcessAgentProtocolError("EVENT payload is invalid")
            event = _event_from_payload(event_payload)
            if str(event.task_id) != self._active_task_id:
                raise ProcessAgentProtocolError("EVENT task identity conflicts")
            callback = self._active_emit
            if callback is not None:
                emitted = callback(event)
                if inspect.isawaitable(emitted):
                    await emitted
            ok = True
        except BaseException as exc:
            error = _sanitized_ipc_error(
                str(exc), fallback=exc.__class__.__name__
            )
        await self._send(
            "event_ack",
            {"ok": ok, "error": error},
            reply_to=str(incoming["id"]),
            generation=generation,
        )

    async def _handle_child_artifact(
        self, incoming: Mapping[str, Any], generation: int
    ) -> None:
        payload = incoming["payload"]
        run_id = str(payload.get("run_id", "") or "")
        proposed_task = payload.get("task")
        proposal = payload.get("proposal")
        ok = False
        error: str | None = None
        value: Any = None
        try:
            if type(proposed_task) is not dict or type(proposal) is not dict:
                raise ProcessAgentProtocolError("artifact proposal payload is invalid")
            original_task = self._active_task
            if (
                original_task is None
                or self._active_run_id is None
                or run_id != self._active_run_id
            ):
                raise ProcessAgentProtocolError("artifact has no active parent task")
            original_task_id = str(getattr(original_task, "task_id", ""))
            original_execution_id = str(
                getattr(original_task, "execution_id", "") or ""
            )
            if (
                str(proposed_task.get("task_id", "")) != original_task_id
                or str(proposed_task.get("execution_id", "") or "")
                != original_execution_id
                or str(proposed_task.get("agent_id", "")) != self.agent_id
            ):
                raise ProcessAgentProtocolError(
                    "artifact task identity conflicts with active run"
                )
            publisher = self.image_output_publisher
            if publisher is None:
                raise RuntimeError("managed image publication is unavailable")
            published = publisher(
                original_task,
                **_supported_kwargs(publisher, proposal),
            )
            if inspect.isawaitable(published):
                published = await published
            value = _wire_value(published)
            ok = True
        except BaseException as exc:
            error = _sanitized_ipc_error(
                str(exc), fallback=exc.__class__.__name__
            )
        await self._send(
            "artifact_ack",
            {"ok": ok, "error": error, "value": value},
            reply_to=str(incoming["id"]),
            generation=generation,
        )

    async def _record_connection_loss(
        self, generation: int, failure: BaseException | None
    ) -> None:
        if generation != self._generation:
            return
        expected = self._expected_stop
        process = self._process
        cleanup_error: BaseException | None = None
        if not expected and process is not None:
            # Do this before join/reap.  While the exact leader PID is still a
            # live process or zombie it cannot be reused as an unrelated
            # process-group ID; killing the group therefore also fences SDK
            # descendants left behind by an abrupt leader crash.
            try:
                await self._terminate_process(process)
            except BaseException as exc:
                # Keep the strong process handle, group identity, and budget.
                # A later stop/start recovery can retry; silently discarding
                # them would permit an untracked executor to outlive capacity.
                cleanup_error = exc
        await self._close_parent_connection()
        if expected:
            # ``stop``/``_force_cleanup_current_generation`` is the sole
            # join-and-budget owner. multiprocessing.Process.join() is not
            # thread-safe, and racing it here caused sporadic graceful-stop
            # timeouts after STOPPED had already arrived.
            return
        if cleanup_error is None:
            self._close_parent_liveness_connection()
            self._release_budget()
        error = self._lost_exception(
            "Agent child process was lost"
            + (f": {failure}" if failure else "")
            + (f"; cleanup remains owned: {cleanup_error}" if cleanup_error else "")
        )
        self._lost_error = error
        self._health = ProcessAgentHealth.LOST
        ready = self._ready_future
        if ready is not None and not ready.done():
            ready.set_exception(error)
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)

    async def _wait_process_exit(self, process: Any, timeout: float) -> bool:
        """Wait on the multiprocessing sentinel without reaping the leader."""

        if process.exitcode is not None:
            return True
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()
        descriptor = int(process.sentinel)

        def ready() -> None:
            with contextlib.suppress(Exception):
                loop.remove_reader(descriptor)
            if not future.done():
                future.set_result(True)

        loop.add_reader(descriptor, ready)
        try:
            await asyncio.wait_for(future, timeout=max(0.001, timeout))
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            with contextlib.suppress(Exception):
                loop.remove_reader(descriptor)

    @staticmethod
    def _signal_group(
        group_id: int,
        signal_value: int,
        *,
        allow_darwin_zombie_leader: bool = False,
    ) -> bool:
        try:
            os.killpg(group_id, signal_value)
            return True
        except ProcessLookupError:
            return False
        except PermissionError as exc:
            # Darwin reports EPERM for a process group whose only remaining
            # member is its unreaped zombie leader.  The caller permits this
            # result only after the multiprocessing sentinel proved that exact
            # leader exited; it then reaps the leader and proves the group
            # empty again.  Live same-UID descendants remain signalable.
            if sys.platform == "darwin" and allow_darwin_zombie_leader:
                return False
            raise ProcessAgentError(
                f"cannot signal Agent process group {group_id}"
            ) from exc
        except OSError as exc:
            raise ProcessAgentError(
                f"cannot signal Agent process group {group_id}"
            ) from exc

    async def _prove_group_empty(self, group_id: int) -> None:
        deadline = asyncio.get_running_loop().time() + self.stop_timeout
        while True:
            try:
                os.killpg(group_id, 0)
            except ProcessLookupError:
                return
            except PermissionError as exc:
                # A zombie-only group is temporarily EPERM on Darwin until its
                # final member is reaped.  Retry boundedly; an inaccessible live
                # group never becomes ESRCH and therefore still fails closed.
                if sys.platform == "darwin":
                    if asyncio.get_running_loop().time() >= deadline:
                        raise ProcessAgentError(
                            f"Agent process group {group_id} remains live"
                        ) from exc
                    await asyncio.sleep(0.02)
                    continue
                raise ProcessAgentError(
                    f"cannot prove Agent process group {group_id} empty"
                ) from exc
            except OSError as exc:
                raise ProcessAgentError(
                    f"cannot prove Agent process group {group_id} empty"
                ) from exc
            if asyncio.get_running_loop().time() >= deadline:
                raise ProcessAgentError(
                    f"Agent process group {group_id} remains live"
                )
            self._signal_group(group_id, signal.SIGKILL)
            await asyncio.sleep(0.02)

    async def _terminate_process(
        self, process: Any, *, graceful: bool = False
    ) -> None:
        """Fence the exact leader and group before releasing ownership."""

        async with self._cleanup_lock:
            await self._terminate_process_locked(process, graceful=graceful)

    async def _terminate_process_locked(
        self, process: Any, *, graceful: bool = False
    ) -> None:
        """Serialized implementation of the process/tree cleanup proof."""

        pid = int(process.pid)
        group_id = self._process_group_id
        already_reaped = process.exitcode is not None
        exited = already_reaped
        if not exited and graceful:
            exited = await self._wait_process_exit(process, self.stop_timeout)
        if not exited:
            signalled_group = False
            if group_id == pid:
                signalled_group = self._signal_group(group_id, signal.SIGTERM)
            if not signalled_group:
                with contextlib.suppress(Exception):
                    process.terminate()
            exited = await self._wait_process_exit(
                process, max(0.1, self.stop_timeout / 2)
            )
        if not exited:
            signalled_group = False
            if group_id == pid:
                signalled_group = self._signal_group(group_id, signal.SIGKILL)
            if not signalled_group:
                with contextlib.suppress(Exception):
                    process.kill()
            exited = await self._wait_process_exit(process, self.stop_timeout)
        if not exited:
            raise ProcessAgentError(
                f"Agent {self.agent_id} leader did not exit after SIGKILL"
            )

        # The leader is still unreaped here in the ordinary path, so its PID
        # cannot be reused as an unrelated process-group ID. Kill any tool/SDK
        # descendants before reaping that identity.
        if group_id == pid and not already_reaped:
            self._signal_group(
                group_id,
                signal.SIGKILL,
                allow_darwin_zombie_leader=exited,
            )
        await asyncio.to_thread(process.join, 0)
        if process.exitcode is None:
            raise ProcessAgentError(
                f"Agent {self.agent_id} leader exit could not be reaped"
            )
        if group_id == pid:
            await self._prove_group_empty(group_id)
        if graceful and process.exitcode != 0:
            raise ProcessAgentError(
                f"Agent {self.agent_id} exited with status {process.exitcode}"
            )

    async def _force_cleanup_current_generation(self) -> None:
        self._expected_stop = True
        self._health = ProcessAgentHealth.STOPPING
        await self._close_parent_connection()
        process = self._process
        if process is not None:
            await self._terminate_process(process)
        self._close_parent_liveness_connection()
        reader = self._reader_task
        if reader is not None and reader is not asyncio.current_task():
            if not reader.done():
                reader.cancel()
            with contextlib.suppress(BaseException):
                await reader
        error = self._lost_exception("Agent child generation was terminated")
        ready = self._ready_future
        if ready is not None and not ready.done():
            ready.set_exception(error)
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        self._reader_task = None
        self._process = None
        self._process_group_id = None
        self._ready_future = None
        self._health = ProcessAgentHealth.STOPPED
        self._release_budget()

    async def _discard_previous_generation(self) -> None:
        process = self._process
        await self._close_parent_connection()
        if process is not None:
            # Even a reaped leader can have left descendants in its dedicated
            # group. Do not release capacity until that group is proven empty.
            await self._terminate_process(process)
        self._close_parent_liveness_connection()
        reader = self._reader_task
        if reader is not None and reader is not asyncio.current_task():
            if not reader.done():
                reader.cancel()
            with contextlib.suppress(BaseException):
                await reader
        self._reader_task = None
        self._process = None
        self._process_group_id = None
        self._ready_future = None
        self._pending.clear()
        self._release_budget()

    async def _close_parent_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()

    def _close_parent_liveness_connection(self) -> None:
        connection = self._parent_liveness_connection
        self._parent_liveness_connection = None
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()

    def _release_budget(self) -> None:
        if self._budget_reserved:
            self._process_budget.release(self)
            self._budget_reserved = False

    def _lost_exception(self, message: str) -> ProcessAgentLostError:
        return ProcessAgentLostError(
            message,
            agent_id=self.agent_id,
            generation=self._generation,
            pid=self.pid,
        )

    async def __aenter__(self) -> "ProcessAgentRuntime":
        await self.start()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.stop()


__all__ = [
    "ProcessAgentCapacityError",
    "ProcessAgentConfigurationError",
    "ProcessAgentError",
    "ProcessAgentHealth",
    "ProcessAgentLostError",
    "ProcessAgentProtocolError",
    "ProcessAgentRemoteError",
    "ProcessAgentRuntime",
    "ProcessAgentStartupError",
]
