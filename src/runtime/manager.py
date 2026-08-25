"""Task routing and lifecycle orchestration for the MVP runtime."""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
import inspect
import logging
import math
import os
from pathlib import Path
import re
import uuid
from collections.abc import Mapping
from typing import Any, Iterable, Sequence

from src.agents.base import AgentTask, ReplyTarget, utc_now
from src.agents.config_profile import require_profile_file
from src.agents.workspace import (
    EXECUTION_WORKSPACE_KEY,
    WorkspaceError,
    build_workspace_snapshot,
    validate_workspace_snapshot,
)

from .dispatcher import SQLiteDispatcher, _call_compatible
from .identity import (
    conversation_id,
    conversation_id_matches,
    mailbox_conversation_id,
)
from .models import iter_model_descriptors
from .registry import AgentRegistry, DYNAMIC_AGENT_SUMMARY, codex_profile
from .worker import TaskWorker
from .modes import ModeRegistry
from .media import canonical_media_inputs
from .policy import (
    AgentProfile,
    EffectivePolicy,
    PolicyEngine,
    normalize_codex_config_profile,
)
from .roles import (
    RoleValidationError,
    build_role_snapshot,
    implicit_default_role,
    is_default_role_token,
    normalize_role_text,
    validate_role_snapshot,
)
from .skills import (
    find_skill,
    iter_skill_descriptors,
    normalize_skill,
    normalize_skills,
)
from .store import QueueFullError, format_working_directory_response

logger = logging.getLogger(__name__)


# ``/agent`` is a control-plane input, so keep its identity deliberately
# narrower than a free-form display label.  Apart from making routes stable,
# this prevents punctuation/whitespace from changing conversation identity or
# producing ambiguous command responses.
_AGENT_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_DYNAMIC_AGENT_SUMMARY = DYNAMIC_AGENT_SUMMARY


class _NamedAgentRuntime:
    """Logical Agent view with an independently owned runtime when supported.

    A process-backed template exposes ``for_agent(agent_id)``.  In that case
    the named Agent owns the returned delegate and therefore starts/stops a
    distinct child process.  The compatibility fallback remains intentionally
    narrow for embedders whose in-memory test/runtime objects do not provide a
    factory: those aliases retain the historical shared-delegate behavior.
    """

    def __init__(
        self,
        agent_id: str,
        delegate: Any,
        *,
        codex_config_profile: str = "",
    ) -> None:
        self.agent_id = str(agent_id)
        self.codex_config_profile = normalize_codex_config_profile(
            codex_config_profile
        )
        factory = getattr(delegate, "for_agent", None)
        if callable(factory):
            try:
                parameters = inspect.signature(factory).parameters.values()
            except (TypeError, ValueError):
                parameters = ()
            supports_profile = any(
                parameter.name == "codex_config_profile"
                or parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
            if self.codex_config_profile and not supports_profile:
                raise ValueError(
                    "Agent runtime does not support Codex config profiles"
                )
            owned_delegate = factory(
                self.agent_id,
                **(
                    {"codex_config_profile": self.codex_config_profile}
                    if supports_profile
                    else {}
                ),
            )
            if inspect.isawaitable(owned_delegate):
                raise TypeError("Agent runtime for_agent() must be synchronous")
            if owned_delegate is delegate:
                raise ValueError(
                    "Agent runtime for_agent() must return an independent runtime"
                )
            self._delegate = owned_delegate
            self._owns_delegate = True
        else:
            if self.codex_config_profile:
                raise ValueError(
                    "Agent runtime does not support Codex config profiles"
                )
            self._delegate = delegate
            self._owns_delegate = False

    @property
    def owns_delegate(self) -> bool:
        """Whether this named Agent owns an independent lifecycle boundary."""

        return self._owns_delegate

    async def start(self) -> None:
        if self._owns_delegate:
            await self._delegate.start()

    async def stop(self) -> None:
        if self._owns_delegate:
            await self._delegate.stop()

    async def run(self, task: AgentTask, emit: Any) -> Any:
        return await self._delegate.run(task, emit)

    async def interrupt(self, task_id: str) -> bool:
        return bool(await self._delegate.interrupt(task_id))

    def __getattr__(self, name: str) -> Any:
        # Preserve optional runtime helpers such as ``list_skills`` and
        # ``reset_session`` for command/runtime compatibility.
        return getattr(self._delegate, name)


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _route_agent(value: Any, default: str | None = None) -> str | None:
    """Normalize route records returned by stores and compatibility facades."""

    if isinstance(value, Mapping):
        value = value.get(
            "active_agent_id",
            value.get("agent_id", value.get("id", value.get("active_agent"))),
        )
    else:
        value = getattr(
            value,
            "active_agent_id",
            getattr(value, "agent_id", getattr(value, "id", value)),
        )
    if value is None:
        return default
    result = str(value).strip()
    return result or default


def _mode_selection(
    value: Any,
    *,
    default_mode_id: str,
    default_policy_version: int,
) -> tuple[str, int]:
    """Normalize a persisted mode row without silently selecting a newer one."""

    mode_id: Any = None
    version: Any = None
    if isinstance(value, Mapping):
        mode_id = value.get("mode_id", value.get("id", value.get("mode")))
        version = value.get("policy_version", value.get("version"))
    elif isinstance(value, (tuple, list)):
        if value:
            mode_id = value[0]
        if len(value) > 1:
            version = value[1]
    elif value is not None:
        mode_id = getattr(value, "mode_id", getattr(value, "id", None))
        version = getattr(value, "policy_version", getattr(value, "version", None))
        if mode_id is None and isinstance(value, str):
            mode_id = value
    mode = str(mode_id or default_mode_id).strip().lower() or str(default_mode_id)
    try:
        resolved_version = int(version if version is not None else default_policy_version)
    except (TypeError, ValueError):
        # An invalid persisted version must fail closed at policy snapshot
        # resolution; retaining a deterministic integer here lets that check
        # produce the useful ``mode@version`` error instead of a conversion
        # traceback in the route lookup path.
        resolved_version = -1
    return mode, resolved_version


def _execute_sandbox_for_version(policy_version: int | str) -> str:
    """Return the immutable sandbox required by each built-in execute version."""

    try:
        version = int(policy_version)
    except (TypeError, ValueError):
        return ""
    return "workspace-write" if version == 1 else "full-access"


def _model_preference(
    value: Any,
    *,
    default_model_id: str = "",
    default_reasoning_effort: str = "",
) -> tuple[str, str]:
    """Normalize a persisted model preference from store/facade shapes."""

    model_id: Any = None
    reasoning_effort: Any = None
    if isinstance(value, Mapping):
        model_id = value.get("model_id", value.get("model", value.get("id")))
        reasoning_effort = value.get(
            "reasoning_effort", value.get("effort", value.get("reasoningEffort"))
        )
    elif isinstance(value, (tuple, list)):
        if value:
            model_id = value[0]
        if len(value) > 1:
            reasoning_effort = value[1]
    elif value is not None:
        model_id = getattr(
            value, "model_id", getattr(value, "model", getattr(value, "id", None))
        )
        reasoning_effort = getattr(
            value,
            "reasoning_effort",
            getattr(value, "effort", getattr(value, "reasoningEffort", None)),
        )
    return (
        str(model_id if model_id is not None else default_model_id).strip(),
        str(
            reasoning_effort
            if reasoning_effort is not None
            else default_reasoning_effort
        ).strip(),
    )


def _model_field(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _model_identifier(value: Any) -> str:
    identifier = _model_field(value, "id", "model_id", "model", default="")
    return str(getattr(identifier, "value", identifier) or "").strip()


def _model_is_default(value: Any) -> bool:
    return bool(_model_field(value, "isDefault", "is_default", "default", default=False))


def _model_reasoning_efforts(value: Any) -> tuple[str, ...]:
    raw = _model_field(
        value,
        "supportedReasoningEfforts",
        "supported_reasoning_efforts",
        "reasoning_efforts",
        default=(),
    )
    if isinstance(raw, Mapping):
        raw = raw.values()
    efforts: list[str] = []
    for item in raw or ():
        effort = (
            item
            if isinstance(item, str)
            else _model_field(
                item,
                "reasoningEffort",
                "reasoning_effort",
                "effort",
                default="",
            )
        )
        normalized = str(getattr(effort, "value", effort) or "").strip()
        if normalized and normalized.lower() not in {value.lower() for value in efforts}:
            efforts.append(normalized)
    return tuple(efforts)


class TaskManager:
    """Own task routing and submit all runtime operations to one event loop.

    The manager never reads a runtime's private dictionaries.  Status and
    interruption are delegated through public runtime/worker methods, while
    durable state is queried from the store.
    """

    def __init__(
        self,
        store: Any,
        registry: AgentRegistry | None = None,
        *,
        runtime: Any | None = None,
        dispatcher: SQLiteDispatcher | None = None,
        workers: Iterable[TaskWorker] | None = None,
        worker_count: int = 1,
        default_agent_id: str = "codex",
        default_mode_id: str = "chat",
        profile_version: int = 1,
        policy_version: int = 1,
        model: str = "",
        reasoning_effort: str = "",
        mode_registry: ModeRegistry | None = None,
        policy_engine: PolicyEngine | None = None,
        trusted_default_execute: bool = False,
        allow_dynamic_agents: bool = False,
        allowed_codex_config_profiles: Iterable[str] | None = None,
        dynamic_agent_template_id: str | None = None,
        require_process_isolation: bool = False,
        workspace_root: str | os.PathLike[str] | None = None,
        reconcile_interval: float | None = 15.0,
    ) -> None:
        if worker_count < 0:
            raise ValueError("worker_count cannot be negative")
        if reconcile_interval is None:
            normalized_reconcile_interval = None
        else:
            try:
                normalized_reconcile_interval = float(reconcile_interval)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "reconcile_interval must be a finite positive number or None"
                ) from exc
            if (
                not math.isfinite(normalized_reconcile_interval)
                or normalized_reconcile_interval <= 0
            ):
                raise ValueError(
                    "reconcile_interval must be a finite positive number or None"
                )
        self.store = store
        # Early integrations passed a single runtime as the second positional
        # argument (``TaskManager(store, runtime)``), while the durable API
        # uses an ``AgentRegistry``.  Accept both forms without weakening the
        # registry boundary; mappings are also normalized into a registry.
        if registry is not None and not isinstance(registry, AgentRegistry):
            if isinstance(registry, Mapping):
                registry = AgentRegistry(registry)
            elif runtime is None and all(
                hasattr(registry, name)
                for name in ("start", "stop", "run", "interrupt")
            ):
                runtime = registry
                registry = None
        if registry is None:
            if runtime is None:
                raise ValueError("registry or runtime is required")
            registry = AgentRegistry()
            runtime_agent_id = str(getattr(runtime, "agent_id", "codex") or "codex")
            # The single-runtime convenience constructor still needs the
            # static MVP profile.  Without it, execute-mode tasks would have
            # no profile-level capability restriction at all.
            profile = codex_profile() if runtime_agent_id == "codex" else None
            registry.register(runtime_agent_id, runtime, profile=profile)
        self.registry = registry
        self.dispatcher = dispatcher or SQLiteDispatcher(store)
        self.default_agent_id = str(default_agent_id or "codex").strip() or "codex"
        self.default_mode_id = str(default_mode_id or "chat").strip().lower() or "chat"
        # A trusted startup mode is an administrator-owned configuration, not
        # a user command.  It is deliberately opt-in so generic TaskManager
        # users retain the fail-closed explicit-authorization requirement for
        # ``execute``.  Durable bot startup enables this only after wiring a
        # writable Codex profile and mode through its trusted configuration.
        self.trusted_default_execute = bool(trusted_default_execute)
        if self.trusted_default_execute and self.default_mode_id != "execute":
            raise ValueError(
                "trusted_default_execute requires default_mode_id='execute'"
            )
        self.profile_version = profile_version
        self.policy_version = policy_version
        self.model = model
        self.reasoning_effort = reasoning_effort
        self._workers: list[TaskWorker] = list(workers or ())
        if not self._workers:
            self._workers = [
                TaskWorker(
                    store,
                    registry,
                    self.dispatcher,
                    worker_id=f"worker-{uuid.uuid4().hex[:12]}",
                )
                for _ in range(worker_count)
            ]
        self._started = False
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._active_agent: dict[tuple[str, str, str, str], str] = {}
        self._session_modes: dict[tuple[str, str, str, str, str], tuple[str, int]] = {}
        self._session_model_preferences: dict[
            tuple[str, str, str, str, str], tuple[str, str]
        ] = {}
        self._authorized_execute_modes: set[tuple[str, str, str, str, str]] = set()
        self._control_locks: dict[tuple[str, str, str, str], asyncio.Lock] = {}
        # Named-Agent creation is an explicit deployment capability.  The
        # durable WeChat startup enables it; generic embedders remain
        # fail-closed unless they opt in.
        self.allow_dynamic_agents = bool(allow_dynamic_agents)
        configured_profiles = (
            (allowed_codex_config_profiles,)
            if isinstance(allowed_codex_config_profiles, str)
            else tuple(allowed_codex_config_profiles or ())
        )
        # Named Codex configuration files are administrator-owned authority:
        # they may contain provider credentials, MCP servers, hooks, and other
        # capabilities.  Dynamic Agent creation therefore defaults to denying
        # every non-empty selector even when a matching private file exists.
        # A literal "*" entry is a wildcard grant: any selector that still
        # passes the safe-name check is authorized, while the matching
        # private config file must still exist.  The token is recognized
        # before normalization (which rejects it) and never enters the name
        # vocabulary itself.
        self._codex_config_profile_wildcard = any(
            isinstance(value, str) and value.strip() == "*"
            for value in configured_profiles
        )
        self.allowed_codex_config_profiles = frozenset(
            normalized
            for normalized in (
                normalize_codex_config_profile(value)
                for value in configured_profiles
                if not (isinstance(value, str) and value.strip() == "*")
            )
            if normalized
        )
        # Generic embedders may still use in-memory runtimes.  The production
        # launcher opts into this fail-closed topology gate so it can never
        # silently fall back to shared/coroutine Agent execution.
        self.require_process_isolation = bool(require_process_isolation)
        self.workspace_root: str | None = None
        self._workspace_root_snapshot: dict[str, Any] | None = None
        if workspace_root is not None:
            root_snapshot = build_workspace_snapshot(workspace_root, ".")
            self.workspace_root = str(root_snapshot["root"])
            self._workspace_root_snapshot = root_snapshot
        self.dynamic_agent_template_id = str(
            dynamic_agent_template_id or self.default_agent_id
        ).strip() or self.default_agent_id
        self._dynamic_agent_lock = asyncio.Lock()
        # Dynamic aliases registered before the supervisor starts remain
        # process-local until their child runtime has actually passed startup.
        # Publishing them earlier would let a crash or failed child leave an
        # enabled Profile that a later supervisor mistakes for a proven Agent.
        self._provisional_dynamic_agent_ids: set[str] = set()
        self.mode_registry = mode_registry or ModeRegistry()
        self.policy_engine = policy_engine or PolicyEngine()
        self._active_workers: set[str] = set()
        self.reconcile_interval = normalized_reconcile_interval
        self._reconcile_stop = asyncio.Event()
        self._reconcile_task: asyncio.Task[None] | None = None

    @property
    def workers(self) -> tuple[TaskWorker, ...]:
        return tuple(self._workers)

    def _validate_process_isolation(
        self,
        *,
        require_live: bool,
        selected_agent_id: str | None = None,
    ) -> None:
        """Prove process topology globally or liveness for one selected Agent.

        Startup passes no selection and therefore requires every enabled Agent
        to be ready.  Dynamic routing passes the selected ID: unrelated lost
        peers are ignored, while the selected process still has to be ready
        and its PID must differ from every other currently live Agent proxy.
        """

        if not self.require_process_isolation:
            return
        template = self.registry.registration(self.dynamic_agent_template_id)
        if self.allow_dynamic_agents and (
            template is None
            or not callable(getattr(template.runtime, "for_agent", None))
        ):
            raise RuntimeError(
                "production dynamic Agent template lacks for_agent() isolation"
            )

        selected = (
            str(selected_agent_id or "").strip()
            if selected_agent_id is not None
            else None
        )
        selected_seen = False
        selected_pid: int | None = None
        live_pids: dict[int, str] = {}
        for descriptor in self.registry.list():
            agent_id = str(_get(descriptor, "agent_id", "") or "").strip()
            registration = self.registry.registration(agent_id)
            runtime = registration.runtime if registration is not None else None
            process_isolated = bool(
                runtime is not None
                and getattr(runtime, "process_isolated", False)
            )
            validates_this_agent = selected is None or agent_id == selected
            if not process_isolated and validates_this_agent:
                raise RuntimeError(
                    f"Agent {agent_id or '?'} is not process-isolated"
                )
            if runtime is None or not process_isolated:
                continue
            pid = getattr(runtime, "pid", None)
            generation = getattr(runtime, "generation", None)
            health = getattr(runtime, "health", "unknown")
            health = str(getattr(health, "value", health) or "").lower()
            live = not (
                type(pid) is not int
                or pid <= 0
                or pid == os.getpid()
                or type(generation) is not int
                or generation <= 0
                or health not in {"ready", "busy"}
            )
            if require_live and validates_this_agent and not live:
                raise RuntimeError(
                    f"Agent {agent_id} process did not become ready"
                )
            if agent_id == selected:
                selected_seen = True
                selected_pid = pid if live else None
            if not live:
                continue
            assert type(pid) is int
            previous = live_pids.get(pid)
            # A selected-Agent proof should not couple B to an unrelated
            # duplicate between A and C. It must still reject B sharing either
            # PID. Startup (no selection) rejects every duplicate globally.
            duplicate_in_scope = selected is None or agent_id == selected or (
                previous == selected
            )
            if previous is not None and duplicate_in_scope:
                raise RuntimeError(
                    "Agent process identity is shared: "
                    f"{previous} and {agent_id} use PID {pid}"
                )
            live_pids.setdefault(pid, agent_id)
        if selected is not None and not selected_seen:
            raise RuntimeError(f"Agent {selected} process is unavailable")
        if selected_pid is not None:
            owner = live_pids.get(selected_pid)
            if owner not in {None, selected}:
                raise RuntimeError(
                    "Agent process identity is shared: "
                    f"{owner} and {selected} use PID {selected_pid}"
                )

    async def _ensure_selected_process_ready(self, agent_id: str) -> None:
        """Start/restart and prove only the Agent about to be routed."""

        if not self.require_process_isolation or not self._started:
            return
        registration = self.registry.registration(agent_id)
        if registration is None:
            raise RuntimeError(f"Agent {agent_id} process is unavailable")
        runtime = registration.runtime
        health = getattr(runtime, "health", "unknown")
        health = str(getattr(health, "value", health) or "").lower()
        if health not in {"ready", "busy"}:
            await runtime.start()
        self._validate_process_isolation(
            require_live=True,
            selected_agent_id=agent_id,
        )

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError("TaskManager must be accessed from its owning asyncio loop")
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("TaskManager must be accessed from its owning asyncio loop")

    def _scope_lock(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> asyncio.Lock:
        key = (channel, bot_id, external_user_id, session_id or "default")
        return self._control_locks.setdefault(key, asyncio.Lock())

    def _default_mode_selection(self, agent_id: str) -> tuple[str, int]:
        """Return the Agent's configured default mode and immutable version."""

        mode_id: Any = None
        descriptor_getter = getattr(self.registry, "descriptor", None)
        if descriptor_getter is not None:
            try:
                descriptor = descriptor_getter(agent_id)
            except Exception:
                descriptor = None
            mode_id = _get(descriptor, "default_mode_id", None)
        if not mode_id:
            profile_getter = getattr(self.registry, "profile", None)
            if profile_getter is not None:
                try:
                    profile = profile_getter(agent_id)
                except Exception:
                    profile = None
                mode_id = _get(profile, "default_mode_id", None)
        mode_id = str(mode_id or self.default_mode_id).strip().lower()
        mode = self._mode_for_agent(agent_id, mode_id)
        version = _get(mode, "policy_version", self.policy_version)
        try:
            version = int(version)
        except (TypeError, ValueError):
            version = int(self.policy_version)
        return mode_id, version

    def _mode_for_agent(self, agent_id: str, mode_id: str) -> Any:
        """Resolve the latest mode compatible with an Agent's Profile grant."""

        mode_id = str(mode_id or "").strip().lower()
        mode = self.mode_registry.get(mode_id)
        if mode is None:
            raise KeyError(f"unknown Agent mode: {mode_id}")
        # Chat v3 exists solely for the explicit collaborative deployment
        # Profile. Preserve chat v2 semantics for generic/custom profiles that
        # may have exact ACLs but intentionally keep ordinary chat non-agentic.
        if mode_id == "chat" and int(_get(mode, "policy_version", 1)) >= 3:
            profile = self.registry.profile(agent_id)
            if not self._profile_enables_dynamic_collaboration(profile):
                legacy = self.mode_registry.get("chat", 2)
                if legacy is not None:
                    mode = legacy
        return mode

    @staticmethod
    def _profile_enables_dynamic_collaboration(profile: Any) -> bool:
        """Recognize only the explicit v3 wildcard/ask Profile grant."""

        try:
            version = int(_get(profile, "profile_version", 0) or 0)
        except (TypeError, ValueError):
            return False
        allowed_peers = frozenset(_get(profile, "allowed_peers", ()) or ())
        allowed_requests = frozenset(
            _get(profile, "allowed_request_types", ()) or ()
        )
        return bool(
            version >= 3
            and "*" in allowed_peers
            and "ask" in allowed_requests
        )

    def _default_profile_version(self, agent_id: str) -> int:
        """Resolve a registered Agent profile version for new task snapshots."""

        descriptor_getter = getattr(self.registry, "descriptor", None)
        descriptor = None
        if descriptor_getter is not None:
            try:
                descriptor = descriptor_getter(agent_id)
            except Exception:
                descriptor = None
        profile_getter = getattr(self.registry, "profile", None)
        profile = None
        if profile_getter is not None:
            try:
                profile = profile_getter(agent_id)
            except Exception:
                profile = None
        value = _get(
            descriptor,
            "profile_version",
            _get(profile, "profile_version", _get(profile, "version", self.profile_version)),
        )
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = int(self.profile_version)
        return value

    async def _persist_registry_definitions(
        self,
        *,
        only_agent_ids: Iterable[str] | None = None,
        exclude_agent_ids: Iterable[str] = (),
    ) -> None:
        """Publish registered Profile/Mode snapshots before workers start.

        SQLite seeds the built-in Codex definitions, but custom registrations
        must be durable before a task can reference their immutable foreign
        keys.  Stores used by integrations may not expose these optional
        persistence hooks, so absence of a hook remains a supported
        compatibility mode; errors from an implemented hook are propagated so
        an immutable-version conflict cannot be hidden by startup.

        Dynamic Agent creation scopes publication to the alias being created.
        This matters after ``/delagent``: the process-local registry retains
        immutable historical Profiles, while SQLite deliberately changes
        their lifecycle-only ``enabled`` bit to false.  Republishing an
        unrelated retired alias would otherwise turn that expected lifecycle
        difference into a same-version metadata conflict.  A global startup
        pass similarly skips tombstoned Profile history that no longer has a
        runtime registration; an explicitly registered Agent remains subject
        to the Store's strict conflict validation.
        """

        selected_agent_ids = (
            None
            if only_agent_ids is None
            else frozenset(
                str(agent_id).strip()
                for agent_id in only_agent_ids
                if str(agent_id).strip()
            )
        )
        excluded_agent_ids = frozenset(
            str(agent_id).strip()
            for agent_id in exclude_agent_ids
            if str(agent_id).strip()
        )

        profile_writer = getattr(self.store, "put_profile", None) or getattr(
            self.store, "register_profile", None
        )
        mode_writer = getattr(self.store, "put_mode", None) or getattr(
            self.store, "register_mode", None
        )
        if profile_writer is None and mode_writer is None:
            return

        profiles_by_key: dict[tuple[str, int], Any] = {}
        profile_list = getattr(self.registry, "list_profiles", None) or getattr(
            self.registry, "profiles", None
        )
        if profile_list is not None:
            values = profile_list()
            if inspect.isawaitable(values):
                values = await values
            for profile in values or ():
                agent_id = _get(profile, "agent_id", None)
                if agent_id is None:
                    continue
                agent_id = str(agent_id)
                if (
                    selected_agent_ids is not None
                    and agent_id not in selected_agent_ids
                ) or agent_id in excluded_agent_ids:
                    continue
                try:
                    version = int(
                        _get(profile, "profile_version", _get(profile, "version", 1))
                    )
                except (TypeError, ValueError):
                    version = 1
                profiles_by_key[(agent_id, version)] = profile

        # Lightweight registry implementations commonly expose descriptors
        # and only a one-version ``profile(id)`` lookup.  Include that current
        # snapshot even when they do not implement ``list_profiles``.
        descriptor_list = getattr(self.registry, "list", None)
        descriptors: Any = ()
        if descriptor_list is not None:
            try:
                descriptors = descriptor_list(include_disabled=True)
            except TypeError:
                descriptors = descriptor_list()
            if inspect.isawaitable(descriptors):
                descriptors = await descriptors
        agent_ids: set[str] = set()
        for descriptor in descriptors or ():
            agent_id = _get(descriptor, "agent_id", _get(descriptor, "id", None))
            if agent_id is None and isinstance(descriptor, str):
                agent_id = descriptor
            if agent_id is None:
                continue
            agent_id = str(agent_id)
            if (
                selected_agent_ids is not None
                and agent_id not in selected_agent_ids
            ) or agent_id in excluded_agent_ids:
                continue
            agent_ids.add(agent_id)
            if profile_list is None:
                profile_getter = getattr(self.registry, "profile", None) or getattr(
                    self.registry, "get_profile", None
                )
                if profile_getter is None:
                    continue
                version_value = _get(descriptor, "profile_version", self.profile_version)
                try:
                    version = int(version_value)
                except (TypeError, ValueError):
                    version = int(self.profile_version)
                try:
                    profile = profile_getter(agent_id, version)
                except TypeError:
                    profile = profile_getter(agent_id)
                if inspect.isawaitable(profile):
                    profile = await profile
                if profile is not None:
                    profiles_by_key[(agent_id, version)] = profile

        if selected_agent_ids is None and profiles_by_key:
            deleted_reader = getattr(self.store, "list_deleted_agents", None)
            if deleted_reader is not None:
                deleted_values = await _call_compatible(deleted_reader)
                deleted_agent_ids = {
                    str(agent_id).strip()
                    for agent_id in deleted_values or ()
                    if str(agent_id).strip()
                }
                registered_agent_ids = set(agent_ids)
                profiles_by_key = {
                    key: profile
                    for key, profile in profiles_by_key.items()
                    if not (
                        key[0] in deleted_agent_ids
                        and key[0] not in registered_agent_ids
                    )
                }

        # Profiles may be registered before a runtime/descriptor is attached;
        # include those Agent IDs when assigning mode snapshots below.
        agent_ids.update(agent_id for agent_id, _ in profiles_by_key)

        if profile_writer is not None:
            for profile in profiles_by_key.values():
                await _call_compatible(profile_writer, profile)

        if mode_writer is None:
            return

        mode_values: list[Any] = []
        mode_list = getattr(self.mode_registry, "list_all", None) or getattr(
            self.mode_registry, "definitions", None
        )
        if mode_list is not None:
            values = mode_list()
            if inspect.isawaitable(values):
                values = await values
            mode_values.extend(values or ())
        else:
            # ``list()`` is the historical latest-version API.  Pair it with
            # the public versions/get methods when available so old snapshots
            # are not dropped by startup persistence.
            latest = getattr(self.mode_registry, "list", None)
            if latest is not None:
                values = latest()
                if inspect.isawaitable(values):
                    values = await values
                mode_values.extend(values or ())
            versions = getattr(self.mode_registry, "versions", None)
            getter = getattr(self.mode_registry, "get", None)
            if versions is not None and getter is not None:
                mode_ids = {
                    str(_get(mode, "mode_id", _get(mode, "id", "")))
                    for mode in mode_values
                    if _get(mode, "mode_id", _get(mode, "id", None)) is not None
                }
                for mode_id in tuple(mode_ids):
                    found_versions = versions(mode_id)
                    if inspect.isawaitable(found_versions):
                        found_versions = await found_versions
                    for version in found_versions or ():
                        mode = getter(mode_id, version)
                        if inspect.isawaitable(mode):
                            mode = await mode
                        if mode is not None:
                            mode_values.append(mode)

        unique_modes: dict[tuple[str, int], Any] = {}
        for mode in mode_values:
            mode_id = _get(mode, "mode_id", _get(mode, "id", None))
            if mode_id is None:
                continue
            try:
                version = int(_get(mode, "policy_version", _get(mode, "version", 1)))
            except (TypeError, ValueError):
                version = 1
            unique_modes[(str(mode_id), version)] = mode

        if not agent_ids:
            agent_ids.add(str(self.default_agent_id or "codex"))
        for agent_id in sorted(agent_ids):
            for mode in unique_modes.values():
                await _call_compatible(mode_writer, mode, agent_id=agent_id)

    async def start(self) -> None:
        self._assert_loop()
        if self._started:
            return
        registry_start_attempted = False
        started_workers: list[TaskWorker] = []
        provisional_agent_ids: frozenset[str] = frozenset()
        initialize = getattr(self.store, "initialize", None)
        try:
            self._validate_trusted_default_execute()
            if initialize is not None:
                result = initialize()
                if inspect.isawaitable(result):
                    await result
            reconcile = (
                getattr(self.store, "startup_reconcile", None)
                or getattr(self.store, "reconcile", None)
                or getattr(self.store, "recover_expired_leases", None)
            )
            if reconcile is not None:
                result = reconcile()
                if inspect.isawaitable(result):
                    await result
            # Runtime registrations are process-local.  Reattach named
            # ``/agent`` aliases before publishing definitions and starting
            # workers so queued tasks can resolve their original owner after
            # a restart.
            await self._restore_dynamic_agents()
            provisional_agent_ids = frozenset(
                self._provisional_dynamic_agent_ids
            )
            self._validate_process_isolation(require_live=False)
            await self._persist_registry_definitions(
                exclude_agent_ids=provisional_agent_ids
            )
            await self._upgrade_collaboration_mode_preferences()
            # ``AgentRegistry.start`` rolls back runtimes it started itself,
            # but a failure can still occur before it returns.  Treat the
            # attempt as owned by this lifecycle so the outer rollback closes
            # every boundary consistently.
            registry_start_attempted = True
            await self.registry.start()
            self._validate_process_isolation(require_live=True)
            if provisional_agent_ids:
                # The child processes are now proven live.  Publish their
                # immutable definitions only at this point, immediately
                # before workers can create durable work that references
                # them.
                await self._persist_registry_definitions(
                    only_agent_ids=provisional_agent_ids
                )
            for worker in self._workers:
                # Record the attempt before awaiting: a worker can allocate a
                # dispatcher task and then raise, so it still needs rollback.
                started_workers.append(worker)
                await worker.start()
                self._active_workers.add(worker.worker_id)
            self._start_reconcile_loop()
            self._provisional_dynamic_agent_ids.difference_update(
                provisional_agent_ids
            )
            self._started = True
        except BaseException as startup_error:
            self._started = False
            await self._stop_reconcile_loop()
            for worker in reversed(started_workers):
                try:
                    await worker.stop()
                except Exception:
                    logger.debug("failed to roll back worker startup", exc_info=True)
            self._active_workers.clear()
            if registry_start_attempted:
                try:
                    await self.registry.stop()
                except Exception:
                    logger.debug("failed to roll back Agent registry startup", exc_info=True)
            for agent_id in sorted(provisional_agent_ids):
                try:
                    await self._rollback_uncommitted_dynamic_agent(
                        agent_id,
                        retire_durable_profile=True,
                    )
                except BaseException:
                    logger.exception(
                        "failed to roll back provisional dynamic Agent %s "
                        "after supervisor startup error",
                        agent_id,
                    )
            # Initialization/reconciliation may allocate resources before
            # raising without exposing a SQLite-style ``_conn`` attribute.
            # The manager owns the store lifecycle, so always invoke its
            # idempotent close hook after a failed start attempt.
            close = getattr(self.store, "close", None)
            if close is not None:
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.debug("failed to roll back store startup", exc_info=True)
            raise

    async def _upgrade_collaboration_mode_preferences(self) -> None:
        """Move mutable chat selections to v3 for collaboration Profiles.

        Profile and task snapshots remain immutable. ``session_modes`` is only
        a future-task preference, so an alias restored onto the explicit v3
        collaboration Profile should not remain silently pinned to chat v2's
        mode-level messaging denial.
        """

        upgrade = getattr(
            self.store, "upgrade_collaboration_chat_preferences", None
        )
        if upgrade is None:
            return
        agent_ids: list[str] = []
        for descriptor in self.registry.list():
            agent_id = str(_get(descriptor, "agent_id", "") or "").strip()
            if not agent_id:
                continue
            profile = self.registry.profile(agent_id)
            if self._profile_enables_dynamic_collaboration(profile):
                agent_ids.append(agent_id)
        if agent_ids:
            await _call_compatible(upgrade, tuple(sorted(agent_ids)))

    def _live_reconcile_method(self) -> Any | None:
        """Return only a conservative same-process lease recovery method."""

        return getattr(self.store, "recover_expired_leases", None) or getattr(
            self.store, "reconcile", None
        )

    def _start_reconcile_loop(self) -> None:
        if (
            self.reconcile_interval is None
            or self._live_reconcile_method() is None
            or (self._reconcile_task is not None and not self._reconcile_task.done())
        ):
            return
        self._reconcile_stop.clear()
        self._reconcile_task = asyncio.create_task(
            self._run_reconcile_loop(),
            name="task-manager-lease-reconcile",
        )

    async def _run_reconcile_loop(self) -> None:
        interval = self.reconcile_interval
        if interval is None:
            return
        while not self._reconcile_stop.is_set():
            try:
                await asyncio.wait_for(self._reconcile_stop.wait(), interval)
                return
            except asyncio.TimeoutError:
                pass
            method = self._live_reconcile_method()
            if method is None:
                return
            try:
                result = method()
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic lease reconciliation failed")

    async def _stop_reconcile_loop(self) -> None:
        self._reconcile_stop.set()
        task, self._reconcile_task = self._reconcile_task, None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def _validate_trusted_default_execute(self) -> None:
        """Validate the administrator-owned writable startup configuration.

        ``execute`` is intentionally not inferred from a missing session row:
        ordinary managers still require explicit mode authorization. The
        durable bot can opt into a single trusted startup mode, but only when
        the exact profile/mode snapshot grants both workspace writes and
        command execution. Any incomplete or mismatched configuration fails
        before workers are started.
        """

        if not self.trusted_default_execute:
            return
        if self.default_mode_id != "execute":
            raise PermissionError(
                "trusted execute startup requires default_mode_id='execute'"
            )
        agent_id = str(self.default_agent_id or "").strip()
        if not agent_id:
            raise PermissionError("trusted execute startup requires a default Agent")
        try:
            profile = self.registry.profile(agent_id)
        except Exception as exc:
            raise PermissionError(
                f"trusted execute profile is unavailable: {agent_id}"
            ) from exc
        if profile is None or not bool(_get(profile, "enabled", True)):
            raise PermissionError(
                f"trusted execute profile is unavailable: {agent_id}"
            )
        profile_mode = str(
            _get(profile, "default_mode_id", self.default_mode_id) or self.default_mode_id
        ).strip().lower()
        if profile_mode != "execute":
            raise PermissionError(
                "trusted execute startup requires the Agent profile default mode to be execute"
            )
        mode = self.mode_registry.get("execute")
        if mode is None:
            raise PermissionError("trusted execute mode definition is unavailable")
        try:
            effective = self.policy_engine.effective_policy(profile, mode)
        except Exception as exc:
            raise PermissionError("trusted execute policy could not be evaluated") from exc
        if (
            str(effective.sandbox_policy).strip().lower()
            != _execute_sandbox_for_version(mode.policy_version)
            or not effective.can_write_files
            or not effective.can_execute_commands
        ):
            raise PermissionError(
                "trusted execute policy must allow workspace writes and command execution"
            )

    async def stop(self) -> None:
        self._assert_loop()
        if not self._started:
            errors: list[BaseException] = []
            try:
                await self._stop_reconcile_loop()
            except BaseException as exc:
                errors.append(exc)
            # Startup rollback can leave an uncertain process registration
            # deliberately marked started.  Retry that cleanup even though
            # the manager never reached its ordinary started state.
            if self.registry.started:
                try:
                    await self.registry.stop()
                except BaseException as exc:
                    errors.append(exc)
            # Closing an explicitly opened store is still useful in tests.
            close = getattr(self.store, "close", None)
            if close is not None:
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except BaseException as exc:
                    errors.append(exc)
            if errors:
                raise errors[0]
            return
        errors: list[BaseException] = []
        try:
            await self._stop_reconcile_loop()
        except BaseException as exc:
            errors.append(exc)
        for worker in reversed(self._workers):
            try:
                await worker.stop()
            except BaseException as exc:
                errors.append(exc)
        self._active_workers.clear()
        try:
            await self.registry.stop()
        except BaseException as exc:
            errors.append(exc)
        close = getattr(self.store, "close", None)
        if close is not None:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                errors.append(exc)
        self._started = False
        if errors:
            raise errors[0]

    # ------------------------------------------------------------------ ingress and routing
    async def _accept_inbound_impl(
        self,
        inbound: Any,
        *,
        create_task: bool = True,
        agent_id: str | None = None,
        mode_id: str | None = None,
        profile_version: int | None = None,
        policy_version: int | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        dedupe_key: str | None = None,
        transcription_candidates: Any | None = None,
        audio_candidates: Any | None = None,
        cursor: str | None = None,
        channel_cursor: str | None = None,
        _trusted_media_wire_fingerprints: Sequence[str] | None = None,
        inputs: Any | None = None,
        skill_snapshot: Mapping[str, Any] | None = None,
        actor: str = "",
        explicit: bool = False,
        _synthetic_command_name: str | None = None,
    ) -> Any:
        """Persist an inbound envelope and optionally enqueue one task.

        ``create_task=False`` is used for commands: the command is durable for
        cursor/ack safety but must never be sent to Codex as user work.
        """

        self._assert_loop()
        target_for_route = self._reply_target(inbound)
        replay = await self._replay_existing_inbound_task(
            inbound,
            create_task=create_task,
            cursor=cursor,
            channel_cursor=channel_cursor,
            trusted_media_wire_fingerprints=_trusted_media_wire_fingerprints,
        )
        if replay is not None:
            return replay
        # An explicit Agent is a deliberate task-level override (used by
        # confirmed media and internal callers).  Ordinary channel ingress
        # leaves it unset, so the durable front-Agent route remains
        # authoritative across restarts.
        if agent_id is None:
            resolved_agent = await self._resolve_active_agent(inbound)
            # The in-memory cache is only a fast path; query SQLite as well so
            # a route changed by a previous process is observed before the
            # task snapshot is created.
            route_getter = getattr(self.store, "get_route", None)
            if route_getter is not None and target_for_route.external_user_id:
                persisted_agent = await _call_compatible(
                    route_getter,
                    channel=target_for_route.channel,
                    bot_id=target_for_route.bot_id,
                    external_user_id=target_for_route.external_user_id,
                    session_id=target_for_route.session_id,
                    default_agent_id=resolved_agent,
                )
                resolved_agent = _route_agent(persisted_agent, resolved_agent) or resolved_agent
                if self.allow_dynamic_agents:
                    resolved_agent = self._canonical_route_agent(resolved_agent)
        else:
            resolved_agent = str(agent_id).strip() or self.default_agent_id
            if self.allow_dynamic_agents:
                resolved_agent = self._canonical_route_agent(resolved_agent)
        # Reject a stale/disabled route before durable ingress creates a task
        # that no worker can execute.
        if self.allow_dynamic_agents:
            await self.ensure_agent(resolved_agent)
        self.registry.require(resolved_agent)
        target = target_for_route
        try:
            execution_workspace = await self._execution_workspace_for_target(
                target_for_route,
                resolved_agent,
            )
        except WorkspaceError:
            if (
                create_task
                or transcription_candidates is not None
                or audio_candidates is not None
            ):
                raise
            # A stale cwd must fail every task or cwd-consuming command, but
            # it must not poison the control plane.  Persist cwd-independent
            # commands without a workspace snapshot so `/agent`, `/help`, and
            # an absolute `/cd` can recover the session.  Relative `/cd`,
            # `/sh`, and skill discovery still resolve the invalid preference
            # later and fail closed.
            execution_workspace = None
        # Resolve the per-session mode after the Agent route is known.  A mode
        # change only affects this new snapshot; running tasks retain theirs.
        route_mode = await self._get_mode_for_target(
            target_for_route, resolved_agent
        )
        default_mode, default_mode_version = self._default_mode_selection(resolved_agent)
        resolved_mode = str(mode_id or (route_mode[0] if route_mode else default_mode)).strip().lower()
        resolved_profile_version = (
            int(profile_version)
            if profile_version is not None
            else self._default_profile_version(resolved_agent)
        )
        if policy_version is not None:
            resolved_policy_version = int(policy_version)
        elif mode_id is None and route_mode is not None:
            resolved_policy_version = int(route_mode[1])
        else:
            resolved_policy_version = int(
                _get(
                    self._mode_for_agent(resolved_agent, resolved_mode),
                    "policy_version",
                    default_mode_version,
                )
            )
        preferred_model, preferred_effort = await self._get_model_for_target(
            target_for_route, resolved_agent
        )
        resolved_model = preferred_model if model is None else str(model)
        resolved_reasoning_effort = (
            preferred_effort
            if reasoning_effort is None
            else str(reasoning_effort)
        )
        metadata = self._policy_snapshot(
            resolved_agent,
            resolved_mode,
            resolved_profile_version,
            resolved_policy_version,
            actor=actor or _get(inbound, "external_user_id", ""),
            explicit=explicit or (
                resolved_mode == "execute"
                and self._execute_authorized(
                    target_for_route,
                    resolved_agent,
                    resolved_policy_version,
                )
            ),
        )
        session_role = await self._session_role_for_target(
            target_for_route, resolved_agent
        )
        metadata = {**metadata, "session_role": session_role}
        if execution_workspace is not None:
            metadata[EXECUTION_WORKSPACE_KEY] = execution_workspace
        if agent_id is None:
            # This inbound was routed through the user's front Agent rather
            # than an explicit task-level override. Persist that fact for a
            # route-missing foreground fallback at projection time.
            metadata = {
                **metadata,
                "front_agent_id_at_acceptance": resolved_agent,
            }
        task_inputs = self._coerce_inbound_inputs(
            inputs if inputs is not None else self._inputs_from_inbound(inbound)
        )
        if skill_snapshot is not None:
            skill = await self._resolve_trusted_skill_snapshot(
                skill_snapshot,
                agent_id=resolved_agent,
                cwd=(
                    str(execution_workspace["path"])
                    if execution_workspace is not None
                    else None
                ),
            )
            task_inputs["skill"] = skill
            metadata = {
                **metadata,
                "skill": dict(skill),
                "skill_id": skill["skill_id"],
                "skill_version": skill.get("version", ""),
                "skill_hash": skill["content_hash"],
            }
        task_values = {
            "agent_id": resolved_agent,
            "mode_id": resolved_mode,
            "profile_version": resolved_profile_version,
            "policy_version": resolved_policy_version,
            "model": resolved_model,
            "reasoning_effort": resolved_reasoning_effort,
            "reply_target": target,
            "inputs": task_inputs,
            "metadata": metadata,
        }
        # Candidate ownership must be captured under the same route/mode
        # serialization as the inbound row.  A gateway may have prepared a
        # candidate spec just before a concurrent ``/agent`` switch; using
        # that stale field would redirect a later confirmation.  Explicit
        # ``agent_id`` callers still intentionally own the candidate route.
        candidate_values: Any = transcription_candidates
        if candidate_values is None:
            candidate_values = audio_candidates
        if candidate_values is not None:
            if isinstance(candidate_values, Mapping):
                candidate_values = [candidate_values]
            else:
                candidate_values = list(candidate_values)
            candidate_values = [
                {
                    **dict(spec),
                    "agent_id": resolved_agent,
                    "mode_id": resolved_mode,
                    "profile_version": resolved_profile_version,
                    "policy_version": resolved_policy_version,
                    "metadata": metadata,
                }
                for spec in candidate_values
                if isinstance(spec, Mapping)
            ]
            # Keep both compatibility spellings synchronized.  Stores use
            # ``transcription_candidates`` when present, while older facades
            # inspect ``audio_candidates``.
            transcription_candidates = tuple(candidate_values)
            audio_candidates = tuple(candidate_values)
        if dedupe_key:
            task_values["dedupe_key"] = dedupe_key
        # Channel envelopes expose ``message_id`` as an alias for the
        # *external* message identity.  The durable task foreign key must use
        # the store-generated inbound row ID instead; passing the channel ID
        # here would make the snapshot point at a non-existent row.  Only
        # adapters that explicitly expose ``durable_message_id`` (or the
        # store-layer envelope with no external ID field) may provide it.
        durable_message_id = _get(inbound, "durable_message_id", None)
        if durable_message_id is None and not hasattr(inbound, "external_message_id"):
            durable_message_id = _get(inbound, "message_id", None)
        if durable_message_id:
            task_values["inbound_message_id"] = durable_message_id
        method = getattr(self.store, "accept_inbound", None) or getattr(self.store, "ingest_inbound", None)
        if method is None:
            persisted = await self._store_inbound(inbound)
            if not create_task:
                return persisted
            return await self._submit_impl(
                task_inputs,
                target,
                agent_id=resolved_agent,
                mode_id=resolved_mode,
                profile_version=resolved_profile_version,
                policy_version=resolved_policy_version,
                model=resolved_model,
                reasoning_effort=resolved_reasoning_effort,
                inbound_message_id=_get(persisted, "message_id", _get(inbound, "message_id")),
                dedupe_key=dedupe_key,
                metadata=metadata,
                _role_snapshot_override=session_role,
                _workspace_snapshot_override=execution_workspace,
                skill_snapshot=skill_snapshot,
                actor=actor,
                explicit=explicit,
            )
        kwargs: dict[str, Any] = {
            "task": task_values,
            "create_task": create_task,
            "transcription_candidates": transcription_candidates,
            "audio_candidates": audio_candidates,
            "command_snapshot": (
                {
                    "agent_id": resolved_agent,
                    "conversation_id": self._conversation_id(target_for_route, resolved_agent),
                    "mode_id": resolved_mode,
                    "profile_version": resolved_profile_version,
                    "policy_version": resolved_policy_version,
                    "model": resolved_model,
                    "reasoning_effort": resolved_reasoning_effort,
                    **(
                        {EXECUTION_WORKSPACE_KEY: execution_workspace}
                        if execution_workspace is not None
                        else {}
                    ),
                    **(
                        {"synthetic_command_name": _synthetic_command_name}
                        if _synthetic_command_name
                        else {}
                    ),
                }
                if not create_task
                else None
            ),
            "cursor": cursor,
            "channel_cursor": channel_cursor,
            "_trusted_media_wire_fingerprints": _trusted_media_wire_fingerprints,
        }
        # Store can derive channel/user from the envelope; include explicit
        # values only when the method advertises them or accepts **kwargs.
        result = await _call_compatible(method, inbound, **kwargs)
        self.dispatcher.wake()
        return result

    async def accept_inbound(
        self,
        inbound: Any,
        *,
        create_task: bool = True,
        agent_id: str | None = None,
        mode_id: str | None = None,
        profile_version: int | None = None,
        policy_version: int | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        dedupe_key: str | None = None,
        transcription_candidates: Any | None = None,
        audio_candidates: Any | None = None,
        cursor: str | None = None,
        channel_cursor: str | None = None,
        _trusted_media_wire_fingerprints: Sequence[str] | None = None,
        inputs: Any | None = None,
        skill_snapshot: Mapping[str, Any] | None = None,
        actor: str = "",
        explicit: bool = False,
        _synthetic_command_name: str | None = None,
    ) -> Any:
        """Serialize ingress against route/mode mutations for one session."""
        self._assert_loop()
        # Channel ingress owns its route and immutable policy snapshot.  Do
        # not let a compatibility caller smuggle an older Profile/Mode row (or
        # a forged execute authorization) into a newly accepted message.  The
        # explicit snapshot replay path is intentionally kept on ``submit``
        # for confirmed candidates and is not part of this ingress API.
        if (
            mode_id is not None
            or profile_version is not None
            or policy_version is not None
            or explicit
        ):
            raise PermissionError(
                "inbound mode and policy are manager-controlled"
            )
        if _synthetic_command_name not in {None, "__queue_full__"}:
            raise PermissionError("unsupported synthetic command marker")
        target = self._reply_target(inbound)
        key = (
            target.channel,
            target.bot_id,
            target.external_user_id,
            target.session_id or "default",
        )
        lock = self._control_locks.setdefault(key, asyncio.Lock())
        async with lock:
            kwargs = {
                "agent_id": agent_id,
                "mode_id": mode_id,
                "profile_version": profile_version,
                "policy_version": policy_version,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "dedupe_key": dedupe_key,
                "transcription_candidates": transcription_candidates,
                "audio_candidates": audio_candidates,
                "cursor": cursor,
                "channel_cursor": channel_cursor,
                "_trusted_media_wire_fingerprints": (
                    _trusted_media_wire_fingerprints
                ),
                "inputs": inputs,
                "skill_snapshot": skill_snapshot,
                "actor": actor,
                "explicit": explicit,
            }
            try:
                return await self._accept_inbound_impl(
                    inbound,
                    create_task=create_task,
                    _synthetic_command_name=_synthetic_command_name,
                    **kwargs,
                )
            except QueueFullError:
                if not create_task:
                    raise
                # Preserve the exact route/mode acceptance order while
                # converting capacity exhaustion into a durable control-plane
                # receipt.  The failed task transaction consumed no task,
                # admission debit, or ready sequence.
                return await self._accept_inbound_impl(
                    inbound,
                    create_task=False,
                    _synthetic_command_name="__queue_full__",
                    **kwargs,
                )

    ingest_inbound = accept_inbound
    accept_message = accept_inbound

    async def _submit_impl(
        self,
        inputs: Any,
        reply_target: ReplyTarget | Mapping[str, Any] | None = None,
        *,
        task_id: str | None = None,
        agent_id: str | None = None,
        conversation_id: str | None = None,
        mode_id: str | None = None,
        profile_version: int | None = None,
        policy_version: int | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        inbound_message_id: str | None = None,
        dedupe_key: str | None = None,
        request_id: str | None = None,
        parent_task_id: str | None = None,
        child_depth: int = 0,
        metadata: Mapping[str, Any] | None = None,
        actor: str = "",
        explicit: bool = False,
        _policy_snapshot_override: Mapping[str, Any] | None = None,
        _role_snapshot_override: Mapping[str, Any] | None = None,
        _workspace_snapshot_override: Mapping[str, Any] | None = None,
        skill_snapshot: Mapping[str, Any] | None = None,
        initial_reply: Any = None,
        delivery_reply_scope_id: str | None = None,
        _child_parent_id: str | None = None,
        _child_max_children: int | None = None,
    ) -> Any:
        """Create a durable queued task while the caller owns any scope lock."""

        self._assert_loop()
        target = self._coerce_target(reply_target)
        if parent_task_id is not None and str(_child_parent_id or "") != str(
            parent_task_id
        ):
            raise PermissionError(
                "child tasks must be submitted through submit_child_task()"
            )
        if agent_id is None and target.external_user_id:
            resolved_agent = await self._resolve_active_agent(target)
        else:
            resolved_agent = str(agent_id).strip() if agent_id else self.default_agent_id
            if self.allow_dynamic_agents:
                resolved_agent = self._canonical_route_agent(resolved_agent)
        if self.allow_dynamic_agents:
            await self.ensure_agent(resolved_agent)
        self.registry.require(resolved_agent)
        workspace_override = _workspace_snapshot_override
        if (
            workspace_override is None
            and isinstance(_policy_snapshot_override, Mapping)
            and isinstance(
                _policy_snapshot_override.get(EXECUTION_WORKSPACE_KEY),
                Mapping,
            )
        ):
            workspace_override = _policy_snapshot_override[
                EXECUTION_WORKSPACE_KEY
            ]
        if workspace_override is None:
            execution_workspace = await self._execution_workspace_for_target(
                target,
                resolved_agent,
            )
        else:
            execution_workspace = self._validate_execution_workspace(
                workspace_override
            )
        route_mode = None
        if mode_id is None:
            route_mode = await self._get_mode_for_target(target, resolved_agent)
        default_mode, default_mode_version = self._default_mode_selection(resolved_agent)
        resolved_mode = str(mode_id or (route_mode[0] if route_mode else default_mode)).strip().lower()
        resolved_profile_version = (
            int(profile_version)
            if profile_version is not None
            else self._default_profile_version(resolved_agent)
        )
        if policy_version is not None:
            resolved_policy_version = int(policy_version)
        elif mode_id is None and route_mode is not None:
            resolved_policy_version = int(route_mode[1])
        else:
            resolved_policy_version = int(
                _get(
                    self._mode_for_agent(resolved_agent, resolved_mode),
                    "policy_version",
                    default_mode_version,
                )
            )
        preferred_model, preferred_effort = await self._get_model_for_target(
            target, resolved_agent
        )
        resolved_model = preferred_model if model is None else str(model)
        resolved_reasoning_effort = (
            preferred_effort
            if reasoning_effort is None
            else str(reasoning_effort)
        )
        if _policy_snapshot_override is None:
            policy_metadata = self._policy_snapshot(
                resolved_agent,
                resolved_mode,
                resolved_profile_version,
                resolved_policy_version,
                actor=actor or target.external_user_id,
                explicit=explicit or (
                    resolved_mode == "execute"
                    and self._execute_authorized(
                        target,
                        resolved_agent,
                        resolved_policy_version,
                    )
                ),
            )
        else:
            # Audio confirmation is an explicit continuation of a previously
            # persisted candidate.  Its policy snapshot is authoritative even
            # when the mode/profile registry has since advanced or the front
            # Agent has switched.  Do not resolve the current policy here.
            policy_metadata = {
                key: value
                for key, value in _policy_snapshot_override.items()
                if key != EXECUTION_WORKSPACE_KEY
            }
        if _role_snapshot_override is None:
            session_role = await self._session_role_for_target(
                target, resolved_agent
            )
        else:
            try:
                session_role = validate_role_snapshot(_role_snapshot_override)
            except RoleValidationError as exc:
                raise RuntimeError("trusted session role snapshot is invalid") from exc
        merged_metadata = dict(policy_metadata)
        if metadata:
            # Caller-supplied task metadata cannot replace the authoritative
            # policy snapshot; it may only add diagnostic values.
            merged_metadata.update(
                {
                    key: value
                    for key, value in metadata.items()
                    if key
                    not in {
                        "effective_policy",
                        "profile",
                        "agent_profile",
                        "mode",
                        "agent_mode",
                        "mode_authorization",
                        "session_role",
                        EXECUTION_WORKSPACE_KEY,
                    }
                }
            )
        merged_metadata["session_role"] = session_role
        if execution_workspace is not None:
            merged_metadata[EXECUTION_WORKSPACE_KEY] = execution_workspace
        if skill_snapshot is not None:
            skill = await self._resolve_trusted_skill_snapshot(
                skill_snapshot,
                agent_id=resolved_agent,
                cwd=(
                    str(execution_workspace["path"])
                    if execution_workspace is not None
                    else None
                ),
            )
            merged_metadata.update(
                {
                    "skill": dict(skill),
                    "skill_id": skill["skill_id"],
                    "skill_version": skill.get("version", ""),
                    "skill_hash": skill["content_hash"],
                }
            )
            if isinstance(inputs, Mapping):
                inputs = {**dict(inputs), "skill": skill}
            else:
                inputs = {"text": str(inputs or ""), "skill": skill}
        conversation = conversation_id or self._conversation_id(target, resolved_agent)
        task_values: dict[str, Any] = {
            "agent_id": resolved_agent,
            "conversation_id": conversation,
            "mode_id": resolved_mode,
            "profile_version": resolved_profile_version,
            "policy_version": resolved_policy_version,
            "model": resolved_model,
            "reasoning_effort": resolved_reasoning_effort,
            "reply_target": target,
            # AgentTask accepts text, structured mappings, and media-bearing
            # sequences.  Preserve non-mapping values so the SDK adapter can
            # perform native image/media translation instead of stringifying
            # them at the manager boundary.
            "inputs": inputs,
            "request_id": request_id,
            "parent_task_id": parent_task_id,
            "child_depth": child_depth,
        }
        if task_id:
            task_values["task_id"] = task_id
        task_values["metadata"] = merged_metadata
        # A store that exposes ``create_child_task`` can reserve the parent's
        # child slot and insert this snapshot in one SQLite transaction.  Keep
        # the ordinary create_task path for compatibility stores that do not
        # implement the stronger operation.
        atomic_child_method = (
            getattr(self.store, "create_child_task", None)
            if _child_parent_id is not None
            else None
        )
        method = atomic_child_method or getattr(self.store, "create_task", None) or getattr(self.store, "enqueue_task", None)
        if method is None:
            raise AttributeError("store must implement create_task()")
        kwargs = {
            "inbound_message_id": inbound_message_id,
            "dedupe_key": dedupe_key,
            "channel": target.channel,
            "bot_id": target.bot_id,
            "external_user_id": target.external_user_id,
            "session_id": target.session_id,
            "initial_reply": initial_reply,
            "delivery_reply_scope_id": delivery_reply_scope_id,
        }
        if atomic_child_method is not None:
            kwargs.update(
                {
                    "parent_task_id": _child_parent_id,
                    "max_children": _child_max_children,
                }
            )
        result = await _call_compatible(method, task_values, **kwargs)
        self.dispatcher.wake()
        return result

    async def submit(
        self,
        inputs: Any,
        reply_target: ReplyTarget | Mapping[str, Any] | None = None,
        *,
        task_id: str | None = None,
        agent_id: str | None = None,
        conversation_id: str | None = None,
        mode_id: str | None = None,
        profile_version: int | None = None,
        policy_version: int | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        inbound_message_id: str | None = None,
        dedupe_key: str | None = None,
        request_id: str | None = None,
        parent_task_id: str | None = None,
        child_depth: int = 0,
        metadata: Mapping[str, Any] | None = None,
        actor: str = "",
        explicit: bool = False,
        skill_snapshot: Mapping[str, Any] | None = None,
        initial_reply: Any = None,
        delivery_reply_scope_id: str | None = None,
        _child_parent_id: str | None = None,
        _child_max_children: int | None = None,
        _workspace_snapshot_override: Mapping[str, Any] | None = None,
    ) -> Any:
        """Create a task ordered against route/mode mutations for its session."""

        self._assert_loop()
        if _workspace_snapshot_override is not None and _child_parent_id is None:
            raise PermissionError(
                "execution workspace overrides are manager-controlled"
            )
        target = self._coerce_target(reply_target)
        kwargs = {
            "task_id": task_id,
            "agent_id": agent_id,
            "conversation_id": conversation_id,
            "mode_id": mode_id,
            "profile_version": profile_version,
            "policy_version": policy_version,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "inbound_message_id": inbound_message_id,
            "dedupe_key": dedupe_key,
            "request_id": request_id,
            "parent_task_id": parent_task_id,
            "child_depth": child_depth,
            "metadata": metadata,
            "actor": actor,
            "explicit": explicit,
            "skill_snapshot": skill_snapshot,
            "initial_reply": initial_reply,
            "delivery_reply_scope_id": delivery_reply_scope_id,
            "_child_parent_id": _child_parent_id,
            "_child_max_children": _child_max_children,
            "_workspace_snapshot_override": _workspace_snapshot_override,
        }
        if not target.external_user_id:
            return await self._submit_impl(inputs, target, **kwargs)
        async with self._scope_lock(
            channel=target.channel,
            bot_id=target.bot_id,
            external_user_id=target.external_user_id,
            session_id=target.session_id,
        ):
            return await self._submit_impl(inputs, target, **kwargs)

    enqueue = submit
    create_task = submit
    queue_task = submit

    # ------------------------------------------------------------------ task controls/status
    async def get_task(self, task_id: str) -> Any:
        self._assert_loop()
        method = getattr(self.store, "get_task", None) or getattr(self.store, "require_task", None)
        if method is None:
            return None
        return await _call_compatible(method, task_id)

    async def get_execution(self, execution_id: str) -> Any:
        """Return one durable execution attempt for retry diagnostics."""

        self._assert_loop()
        method = getattr(self.store, "get_execution", None) or getattr(
            self.store, "get_task_execution", None
        )
        if method is None:
            return None
        return await _call_compatible(method, execution_id)

    get_task_execution = get_execution

    async def list_task_executions(
        self,
        task_id: str,
        *,
        limit: int = 100,
        newest_first: bool = False,
        **filters: Any,
    ) -> list[Any]:
        """Return all durable attempts for a logical task."""

        self._assert_loop()
        method = (
            getattr(self.store, "list_task_executions", None)
            or getattr(self.store, "get_task_executions", None)
            or getattr(self.store, "list_executions", None)
        )
        if method is None:
            return []
        return list(
            await _call_compatible(
                method,
                task_id,
                limit=limit,
                newest_first=newest_first,
                **filters,
            )
            or ()
        )

    get_task_executions = list_task_executions
    list_executions = list_task_executions
    executions = list_task_executions

    async def enqueue_user_outbox(self, delivery: Any = None, **kwargs: Any) -> Any:
        """Persist an immediate user response without dispatching Agent work."""
        self._assert_loop()
        method = (
            getattr(self.store, "create_user_outbox", None)
            or getattr(self.store, "enqueue_user_outbox", None)
            or getattr(self.store, "add_user_outbox", None)
        )
        if method is None:
            raise AttributeError("store must implement create_user_outbox()")
        return await _call_compatible(method, delivery, **kwargs)

    async def get_outbox_item(self, outbox_id: str) -> Any:
        """Read one user-delivery projection by its stable ID.

        Command recovery uses this lookup to distinguish a duplicate inbound
        whose control response was already committed from a crash between
        durable ingress and command execution.  It is a read-only projection;
        it never reaches into a worker or runtime's private state.
        """

        self._assert_loop()
        method = (
            getattr(self.store, "get_outbox_item", None)
            or getattr(self.store, "get_user_outbox_item", None)
            or getattr(self.store, "get_delivery", None)
        )
        if method is None:
            return None
        return await _call_compatible(method, outbox_id)

    get_user_outbox_item = get_outbox_item
    get_delivery = get_outbox_item

    async def set_inbound_status(self, message_id: str, status: Any, **kwargs: Any) -> bool:
        self._assert_loop()
        method = getattr(self.store, "set_inbound_status", None) or getattr(
            self.store, "update_inbound_status", None
        )
        if method is None:
            return False
        return bool(await _call_compatible(method, message_id, status, **kwargs))

    async def mark_outbox_sending(self, outbox_id: str, **kwargs: Any) -> bool:
        self._assert_loop()
        method = getattr(self.store, "mark_outbox_sending", None)
        if method is None:
            # ``False`` is a meaningful conditional-write result (another
            # worker owns the row).  Do not use it to signal an unsupported
            # compatibility store: channel adapters would then suppress a
            # direct command response instead of sending it.  Raising the
            # same capability error as ``enqueue_user_outbox`` lets callers
            # fall back to an unclaimed compatibility path explicitly.
            raise AttributeError("store must implement mark_outbox_sending()")
        return bool(await _call_compatible(method, outbox_id, **kwargs))

    async def mark_outbox_sent(self, outbox_id: str, **kwargs: Any) -> bool:
        self._assert_loop()
        method = getattr(self.store, "mark_outbox_sent", None)
        if method is None:
            return False
        return bool(await _call_compatible(method, outbox_id, **kwargs))

    async def mark_outbox_failed(self, outbox_id: str, **kwargs: Any) -> bool:
        self._assert_loop()
        method = getattr(self.store, "mark_outbox_failed", None)
        if method is None:
            return False
        return bool(await _call_compatible(method, outbox_id, **kwargs))

    create_user_outbox = enqueue_user_outbox
    add_user_outbox = enqueue_user_outbox

    async def drain_deferred_replies(
        self,
        *,
        target: Any,
        source_key: str,
        limit: int = 10,
        **kwargs: Any,
    ) -> Any:
        """Allocate the next durable quota-overflow batch for ``/recv``.

        The store owns FIFO selection and replay idempotency.  Keeping this as
        a manager facade lets the command router stay on the asyncio-owned
        runtime boundary without reaching into SQLite directly.
        """

        self._assert_loop()
        method = (
            getattr(self.store, "drain_deferred_replies", None)
            or getattr(self.store, "receive_deferred_replies", None)
            or getattr(self.store, "drain_reply_overflow", None)
        )
        if method is None:
            raise AttributeError("store must implement drain_deferred_replies()")
        return await _call_compatible(
            method,
            target=target,
            source_key=source_key,
            limit=max(1, min(10, int(limit))),
            **kwargs,
        )

    receive_deferred_replies = drain_deferred_replies
    drain_reply_overflow = drain_deferred_replies

    async def activate_outbox_contextless_variant(
        self,
        outbox_id: str,
        *,
        claim_token: str | None = None,
        contextless_client_id: str | None = None,
    ) -> Any:
        """Persist the sole primary-to-contextless wire-ID transition."""

        self._assert_loop()
        method = getattr(self.store, "activate_outbox_contextless_variant", None)
        if method is None:
            raise AttributeError(
                "store must implement activate_outbox_contextless_variant()"
            )
        return await _call_compatible(
            method,
            outbox_id,
            claim_token=claim_token,
            contextless_client_id=contextless_client_id,
        )

    async def status(self, task_id: str | None = None, **filters: Any) -> Any:
        """Read durable task state; this is safe while another task runs."""

        self._assert_loop()
        if task_id is not None:
            return await self.get_task(task_id)
        # ``/status`` is an immediate control query and should report active
        # work only.  Callers can use ``list_tasks`` for full history.
        filters.setdefault(
            "states",
            ("queued", "claimed", "running", "cancel_requested"),
        )
        return await self.list_tasks(**filters)

    async def list_tasks(self, *, limit: int = 100, **filters: Any) -> list[Any]:
        self._assert_loop()
        method = getattr(self.store, "list_tasks", None)
        if method is None:
            return []
        kwargs = {"limit": limit, **filters}
        return await _call_compatible(method, **kwargs)

    tasks = list_tasks

    async def interrupt(self, task_id: str) -> bool:
        """Request durable cancellation and interrupt a live runtime turn."""

        self._assert_loop()
        task = await self.get_task(task_id)
        requested = False
        request = getattr(self.store, "request_cancel", None) or getattr(self.store, "request_interrupt", None)
        if request is not None:
            requested = bool(await _call_compatible(request, task_id))
        # A worker owns the live runtime state; ask workers through their public
        # method rather than inspecting CodexRuntime dictionaries.
        interrupted = False
        for worker in self._workers:
            interrupted = await worker.interrupt(task_id) or interrupted
        if not interrupted and task is not None:
            agent_id = _get(task, "agent_id", self.default_agent_id)
            try:
                interrupted = bool(await self.registry.interrupt(agent_id, task_id))
            except (KeyError, RuntimeError):
                interrupted = False
        return requested or interrupted

    request_interrupt = interrupt
    interrupt_task = interrupt

    async def retry(
        self,
        task_id: str,
        *,
        initial_reply: Any = None,
        delivery_reply_scope_id: str | None = None,
    ) -> Any:
        """Explicitly requeue a failed/orphaned task, preserving its history."""

        self._assert_loop()
        method = getattr(self.store, "retry_task", None) or getattr(self.store, "requeue_task", None)
        if method is not None:
            result = await _call_compatible(
                method,
                task_id,
                initial_reply=initial_reply,
                delivery_reply_scope_id=delivery_reply_scope_id,
            )
        else:
            task = await self.get_task(task_id)
            if task is None:
                return None
            transition = getattr(self.store, "transition_task", None)
            if transition is None:
                raise AttributeError("store must implement retry_task() or transition_task()")
            result = await _call_compatible(
                transition,
                task_id,
                "queued",
                from_states=("failed", "orphaned", "interrupted"),
            )
            result = await self.get_task(task_id) if result else result
        self.dispatcher.wake()
        return result

    request_retry = retry
    retry_task = retry

    async def cancel(self, task_id: str) -> bool:
        self._assert_loop()
        method = getattr(self.store, "cancel_task", None)
        if method is not None:
            result = bool(await _call_compatible(method, task_id))
            # ``cancel_task`` terminalizes queued/orphaned work directly, but
            # leaves a claimed/running execution in ``cancel_requested`` so
            # its live SDK turn can be interrupted.  Re-read after the durable
            # transition: consulting the pre-transition row would both send a
            # meaningless interrupt for work that never started and miss a
            # queued -> running race that became active during cancellation.
            task = await self.get_task(task_id)
            state = getattr(_get(task, "state", ""), "value", _get(task, "state", ""))
            if str(state or "").strip().lower() != "cancel_requested":
                return result
            interrupted = False
            for worker in self._workers:
                try:
                    interrupted = await worker.interrupt(task_id) or interrupted
                except Exception:
                    # The durable cancellation request already won the state
                    # transition.  SDK interruption is best-effort: the
                    # worker observes cancel_requested before terminalizing a
                    # late result, so a transport error must not make the
                    # command report that cancellation was rejected.
                    logger.warning(
                        "runtime interruption failed for cancelled task %s",
                        task_id,
                        exc_info=True,
                    )
            if not interrupted and task is not None:
                agent_id = str(
                    _get(task, "agent_id", self.default_agent_id)
                    or self.default_agent_id
                )
                try:
                    interrupted = bool(
                        await self.registry.interrupt(agent_id, task_id)
                    )
                except Exception:
                    interrupted = False
            return result or interrupted
        else:
            return await self.interrupt(task_id)

    cancel_task = cancel

    # ------------------------------------------------------------------ named Agent lifecycle
    @staticmethod
    def _validate_agent_id(agent_id: Any) -> str:
        value = str(agent_id or "").strip().lower()
        if not _AGENT_ID_RE.fullmatch(value):
            raise ValueError(
                "Agent ID must start with a letter and contain only letters, "
                "digits, '-' or '_' (1-64 characters)"
            )
        return value

    def _dynamic_template(self) -> tuple[Any, AgentProfile]:
        """Return the registered runtime/profile used for named Agents."""

        template_id = self.dynamic_agent_template_id
        registration = self.registry.registration(template_id)
        if registration is None:
            raise KeyError(f"dynamic Agent template is unavailable: {template_id}")
        # ``require`` also rejects a disabled template, rather than silently
        # creating an Agent that workers cannot execute.
        runtime = self.registry.require(template_id)
        profile = registration.profile or self.registry.profile(template_id)
        if not isinstance(profile, AgentProfile):
            # Compatibility registries may register a runtime without a
            # profile.  Give the named view a minimal read-only profile so it
            # remains usable for chat while execute-mode authorization still
            # fails closed.
            descriptor = self.registry.descriptor(template_id)
            profile = AgentProfile(
                agent_id=template_id,
                display_name=str(
                    _get(descriptor, "display_name", template_id) or template_id
                ),
                summary=str(_get(descriptor, "summary", "") or ""),
                capabilities=frozenset(_get(descriptor, "capabilities", ()) or ()),
                enabled=bool(_get(descriptor, "enabled", True)),
                profile_version=int(
                    _get(descriptor, "profile_version", self.profile_version) or 1
                ),
                default_mode_id="chat",
            )
        return runtime, profile

    @staticmethod
    def _named_profile(
        agent_id: str,
        template: AgentProfile,
        *,
        codex_config_profile: str = "",
    ) -> AgentProfile:
        # Named Agents start in read-only chat even when the administrator's
        # static Codex Agent defaults to trusted execute.  Execute remains an
        # explicit, per-session ``/mode execute`` decision.
        return replace(
            template,
            agent_id=agent_id,
            display_name=agent_id,
            summary=_DYNAMIC_AGENT_SUMMARY,
            default_mode_id="chat",
            codex_config_profile=codex_config_profile,
        )

    def _require_allowed_codex_config_profile(self, value: Any) -> str:
        """Authorize one selector without revealing private file existence.

        A "*" entry in the administrator allow-list is a wildcard grant: any
        selector that passes the safe-name check below is accepted.  Unsafe
        names still raise before the allow-list is consulted, and the caller
        must still prove that the matching config file exists.
        """

        normalized = normalize_codex_config_profile(value)
        if (
            normalized
            and not self._codex_config_profile_wildcard
            and normalized not in self.allowed_codex_config_profiles
        ):
            raise PermissionError("Codex config profile is not enabled")
        return normalized

    async def _rollback_uncommitted_dynamic_agent(
        self,
        agent_id: str,
        *,
        retire_durable_profile: bool,
    ) -> None:
        """Hide a failed new alias durably, then release its runtime handle.

        Profile publication consists of compatibility-store calls and can be
        ambiguous when a store commits and then raises.  A brand-new alias is
        therefore compensated with the existing retirement transaction: its
        immutable history remains auditable, while the disabled Profile plus
        tombstone cannot be restored as a runnable child.  Retained aliases
        are never retired by this helper; their earlier durable lifecycle is
        authoritative.
        """

        durable_error: BaseException | None = None
        if retire_durable_profile:
            profile_reader = getattr(self.store, "get_profile", None)
            durable_profile = (
                await _call_compatible(profile_reader, agent_id)
                if profile_reader is not None
                else None
            )
            if durable_profile is not None:
                retire = getattr(self.store, "retire_agent", None) or getattr(
                    self.store, "mark_agent_deleted", None
                )
                if retire is None:
                    durable_error = RuntimeError(
                        "store cannot retire an uncommitted Agent profile"
                    )
                else:
                    try:
                        await _call_compatible(
                            retire,
                            agent_id,
                            default_agent_id=self.default_agent_id,
                            fallback_agent_id=self.default_agent_id,
                        )
                    except BaseException as exc:
                        durable_error = exc

        current = self.registry.registration(agent_id)
        if current is not None:
            try:
                await current.runtime.stop()
            except BaseException as cleanup_error:
                # Keep the only handle that can retry an uncertain process
                # cleanup.  The caller surfaces the cleanup failure chained
                # from the operation that initiated rollback.
                if durable_error is not None:
                    raise cleanup_error from durable_error
                raise
            if durable_error is None:
                self.registry.unregister(agent_id, allow_started=True)

        if durable_error is not None:
            raise durable_error
        self._provisional_dynamic_agent_ids.discard(agent_id)

    async def _restore_dynamic_agents(self) -> None:
        """Reattach persisted ``/agent`` aliases after a process restart.

        Routes and queued tasks are durable, but runtime registrations are
        process-local.  The generated summary is an internal provenance
        marker, so only profiles created by this manager are reattached to
        the configured template runtime; arbitrary database profiles are not
        granted execution merely because they exist in SQLite.
        """

        if not self.allow_dynamic_agents:
            return
        reader = getattr(self.store, "list_profiles", None)
        if reader is None:
            return
        values = await _call_compatible(reader)
        deleted_reader = getattr(self.store, "list_deleted_agents", None)
        deleted = {
            str(value).strip().lower()
            for value in (await _call_compatible(deleted_reader) if deleted_reader is not None else ())
            if str(value).strip()
        }
        candidates: dict[str, tuple[int, AgentProfile]] = {}
        for value in values or ():
            agent_id = str(_get(value, "agent_id", "") or "").strip()
            if (
                not agent_id
                or agent_id == self.dynamic_agent_template_id
                or agent_id.lower() in deleted
                or str(_get(value, "summary", "") or "") != _DYNAMIC_AGENT_SUMMARY
            ):
                continue
            try:
                canonical_agent_id = self._validate_agent_id(agent_id)
                version = int(_get(value, "profile_version", _get(value, "version", 1)))
            except (TypeError, ValueError):
                continue
            if not isinstance(value, AgentProfile):
                continue
            if not bool(_get(value, "enabled", True)):
                continue
            if canonical_agent_id != value.agent_id:
                value = replace(value, agent_id=canonical_agent_id)
            agent_id = canonical_agent_id
            previous = candidates.get(agent_id)
            if previous is None or version > previous[0]:
                candidates[agent_id] = (version, value)
            # Retain older immutable versions for queued task snapshots.
            register_profile = getattr(self.registry, "register_profile", None)
            if register_profile is not None:
                register_profile(value)
        if not candidates:
            return
        runtime, template = self._dynamic_template()
        for agent_id, (version, profile) in sorted(candidates.items()):
            if self.registry.registration(agent_id) is not None:
                continue
            # Generated aliases track the trusted template for *future* tasks,
            # while every older immutable Profile remains registered for queued
            # and historical task snapshots.  A version bump is the only safe
            # way to add collaboration authority; never rewrite an existing
            # profile row in place.
            config_profile = normalize_codex_config_profile(
                profile.codex_config_profile
            )
            try:
                config_profile = self._require_allowed_codex_config_profile(
                    config_profile
                )
            except PermissionError:
                # Keep the immutable profile and route as durable truth, but
                # do not revive a child whose administrator grant was removed.
                # A later scoped use fails closed until startup re-allows it.
                logger.warning(
                    "dynamic Agent %s uses a disabled Codex config profile",
                    agent_id,
                )
                continue
            desired = self._named_profile(
                agent_id,
                template,
                codex_config_profile=config_profile,
            )
            desired_version = int(desired.profile_version)
            if desired_version > int(version):
                register_profile = getattr(self.registry, "register_profile", None)
                if register_profile is not None:
                    register_profile(desired)
                profile = desired
            elif desired_version == int(version) and desired != profile:
                raise ValueError(
                    "dynamic Agent profile metadata conflicts: "
                    f"{agent_id}@{desired_version}"
                )
            self.registry.register(
                agent_id,
                _NamedAgentRuntime(
                    agent_id,
                    runtime,
                    codex_config_profile=config_profile,
                ),
                profile=profile,
            )

    async def ensure_agent(
        self,
        agent_id: str,
        *,
        allow_deleted: bool = False,
        codex_config_profile: str | None = None,
    ) -> bool:
        """Ensure a named Agent exists, returning ``True`` when created.

        Existing registered Agents are never replaced.  Unknown names are
        created only when ``allow_dynamic_agents`` is enabled by trusted
        startup wiring, and inherit the template's immutable capabilities
        while receiving their own conversation/profile identity.
        """

        self._assert_loop()
        requested_config_profile: str | None = None
        if codex_config_profile is not None:
            requested_config_profile = self._require_allowed_codex_config_profile(
                codex_config_profile
            )
        raw_agent_id = str(agent_id or "").strip()
        # Preserve explicitly registered legacy IDs (some integrations used
        # mixed case or a broader identifier grammar), while all newly
        # created names use the canonical validated form.  Check the durable
        # tombstone before returning an existing registration: an interrupted
        # two-phase recreation can intentionally leave a process-local alias
        # registered while it remains durably hidden.
        registration = (
            self.registry.registration(raw_agent_id) if raw_agent_id else None
        )
        selected = (
            raw_agent_id
            if registration is not None
            else self._validate_agent_id(raw_agent_id)
        )
        deleted_reader = getattr(self.store, "is_agent_deleted", None)
        was_deleted = bool(
            deleted_reader is not None
            and await _call_compatible(deleted_reader, selected)
        )
        if was_deleted and not allow_deleted:
            raise KeyError(f"Agent was deleted: {selected}")
        if registration is not None and not (was_deleted and allow_deleted):
            bound_profile = normalize_codex_config_profile(
                _get(registration.profile, "codex_config_profile", "")
            )
            bound_profile = self._require_allowed_codex_config_profile(
                bound_profile
            )
            if (
                requested_config_profile is not None
                and requested_config_profile != bound_profile
            ):
                raise ValueError(
                    "Agent is already bound to a different Codex config profile"
                )
            self.registry.require(selected)
            await self._ensure_selected_process_ready(selected)
            return False
        if registration is None:
            try:
                self.registry.require(selected)
                return False
            except KeyError:
                if not self.allow_dynamic_agents:
                    # Preserve the registry's canonical unknown/disabled
                    # error.
                    self.registry.require(selected)
                    return False  # pragma: no cover - require always raises

        async with self._dynamic_agent_lock:
            # Deletion and recreation use the same process-local lifecycle
            # lock, but a request may have observed the tombstone before it
            # waited behind another control operation.  Refresh it here so a
            # just-retired Profile takes the explicit reactivation path.
            if deleted_reader is not None:
                deleted_now = bool(
                    await _call_compatible(deleted_reader, selected)
                )
                if deleted_now and not allow_deleted:
                    raise KeyError(f"Agent was deleted: {selected}")
                was_deleted = was_deleted or deleted_now
            # Another control request may have created this ID while we were
            # waiting for the lock.
            registration = self.registry.registration(selected)
            if registration is not None:
                bound_profile = normalize_codex_config_profile(
                    _get(registration.profile, "codex_config_profile", "")
                )
                bound_profile = self._require_allowed_codex_config_profile(
                    bound_profile
                )
                if (
                    requested_config_profile is not None
                    and requested_config_profile != bound_profile
                ):
                    raise ValueError(
                        "Agent is already bound to a different Codex config profile"
                    )
                self.registry.require(selected)
                # Another manager can retire the durable Agent while this
                # process still has its enabled runtime registration. An
                # explicit `/agent` recreation must prepare those disabled
                # rows even on this existing-registration path; otherwise the
                # guarded route commit repeatedly sees an enabled-bit conflict.
                if was_deleted and allow_deleted:
                    reactivator = getattr(self.store, "reactivate_agent", None)
                    profile = registration.profile
                    if reactivator is not None:
                        if profile is None:
                            raise RuntimeError(
                                f"Agent recreation lacks a Profile: {selected}"
                            )
                        await _call_compatible(reactivator, profile)
                        await self._persist_registry_definitions(
                            only_agent_ids=(selected,)
                        )
                await self._ensure_selected_process_ready(selected)
                return False
            runtime, template = self._dynamic_template()
            retained_profile: AgentProfile | None = None
            profile_reader = getattr(self.store, "get_profile", None)
            if profile_reader is not None:
                candidate = await _call_compatible(profile_reader, selected)
                if (
                    isinstance(candidate, AgentProfile)
                    and str(candidate.summary or "") == _DYNAMIC_AGENT_SUMMARY
                ):
                    retained_profile = candidate
            retained_config_profile = (
                self._require_allowed_codex_config_profile(
                    retained_profile.codex_config_profile
                )
                if retained_profile is not None
                else ""
            )
            brand_new_profile = retained_profile is None and not was_deleted
            if (
                requested_config_profile is not None
                and retained_profile is not None
                and requested_config_profile != retained_config_profile
            ):
                raise ValueError(
                    "Agent is already bound to a different Codex config profile"
                )
            selected_config_profile = (
                requested_config_profile
                if requested_config_profile is not None
                else retained_config_profile
            )
            if selected_config_profile:
                # Resolve only the authorized expected filename.  Parsing and
                # credential-bearing values remain inside the Agent child.
                # Existing live bindings intentionally avoid this probe so an
                # idempotent switch cannot become a private-file oracle.
                require_profile_file(selected_config_profile)
            profile = self._named_profile(
                selected,
                template,
                codex_config_profile=selected_config_profile,
            )
            self.registry.register(
                selected,
                _NamedAgentRuntime(
                    selected,
                    runtime,
                    codex_config_profile=selected_config_profile,
                ),
                profile=profile,
            )
            try:
                # `/delagent` disables retained immutable Profile versions and
                # writes a tombstone.  An explicit later `/agent <same-id>` is
                # a recreation request: atomically validate/restore those
                # lifecycle bits before ordinary idempotent publication.
                reactivator = getattr(self.store, "reactivate_agent", None)
                if was_deleted and allow_deleted and reactivator is not None:
                    await _call_compatible(reactivator, profile)
                if self._started:
                    # A brand-new alias is provisional until its isolated
                    # child has started and passed the process-readiness
                    # proof.  Starting it before publishing the immutable
                    # Profile prevents a transient child/capacity failure
                    # from leaving an enabled durable alias that
                    # `_restore_dynamic_agents()` would revive on every later
                    # supervisor restart.  Recreated aliases are already
                    # protected by their retained tombstone until the route
                    # commit below.
                    await self.registry.start()
                    await self._ensure_selected_process_ready(selected)
                if not self._started and brand_new_profile:
                    # Pre-start aliases stay process-local.  `start()` first
                    # proves all registered children, then publishes these
                    # definitions before any worker can reference them.
                    self._provisional_dynamic_agent_ids.add(selected)
                else:
                    await self._persist_registry_definitions(
                        only_agent_ids=(selected,)
                    )
            except BaseException as startup_error:
                try:
                    await self._rollback_uncommitted_dynamic_agent(
                        selected,
                        retire_durable_profile=brand_new_profile,
                    )
                except BaseException as cleanup_error:
                    raise cleanup_error from startup_error
                raise
            return True

    async def delete_agent(
        self,
        agent_id: str,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> bool:
        """Delete a dynamic Agent while preserving immutable task history."""
        self._assert_loop()
        selected = self._validate_agent_id(agent_id)
        if selected == self.dynamic_agent_template_id or selected == self.default_agent_id:
            raise ValueError("the default Agent cannot be deleted")
        registration = self.registry.registration(selected)
        if registration is None:
            reader = getattr(self.store, "is_agent_deleted", None)
            if reader is not None and await _call_compatible(reader, selected):
                raise KeyError(f"Agent not found: {selected}")
            raise KeyError(f"Agent not found: {selected}")
        profile = registration.profile or self.registry.profile(selected)
        if str(_get(profile, "summary", "") or "") != _DYNAMIC_AGENT_SUMMARY:
            raise ValueError("only dynamically created Agents can be deleted")
        async with self._dynamic_agent_lock:
            current = self.registry.registration(selected)
            if current is None:
                raise KeyError(f"Agent not found: {selected}")
            retire = getattr(self.store, "retire_agent", None)
            marker = getattr(self.store, "mark_agent_deleted", None)
            if retire is not None:
                await _call_compatible(
                    retire, selected, default_agent_id=self.default_agent_id
                )
            elif marker is not None:
                await _call_compatible(
                    marker, selected, fallback_agent_id=self.default_agent_id
                )
            else:
                raise AttributeError(
                    "store does not support durable Agent deletion"
                )
            # A process-backed named Agent owns its child lifecycle.  Stop it
            # while the registration is still reachable so a failed shutdown
            # cannot silently discard the only handle capable of reaping the
            # process.  Shared compatibility aliases have a no-op ``stop``.
            await current.runtime.stop()
            self.registry.unregister(selected, allow_started=True)
            self._active_agent = {key: value for key, value in self._active_agent.items() if value != selected}
            if channel or bot_id or external_user_id:
                route = getattr(self.store, "set_route", None)
                if route is not None:
                    await _call_compatible(
                        route,
                        channel=channel,
                        bot_id=bot_id,
                        external_user_id=external_user_id,
                        session_id=session_id or "default",
                        active_agent_id=self.default_agent_id,
                    )
            return True

    def _canonical_route_agent(self, agent_id: Any) -> str:
        """Normalize persisted dynamic routes without rewriting static IDs."""

        value = str(agent_id or "").strip()
        if not self.allow_dynamic_agents:
            return value
        if self.registry.registration(value) is not None:
            return value
        return self._validate_agent_id(value)

    # ------------------------------------------------------------------ routing/front Agent
    def active_agent_for(self, inbound_or_target: Any) -> str:
        self._assert_loop()
        target = self._coerce_target(_get(inbound_or_target, "reply_target", None))
        if not target.external_user_id:
            target = self._coerce_target(inbound_or_target)
        session_id = target.session_id or "default"
        key = (target.channel, target.bot_id, target.external_user_id, session_id)
        return self._active_agent.get(key, self.default_agent_id)

    async def _resolve_active_agent(self, inbound_or_target: Any) -> str:
        """Resolve the durable front-Agent route, falling back to local state."""

        target = self._reply_target(inbound_or_target)
        method = getattr(self.store, "get_route", None) or getattr(self.store, "active_agent", None)
        if method is not None and target.external_user_id:
            value = await _call_compatible(
                method,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=target.session_id,
                default_agent_id=self.default_agent_id,
            )
            resolved = _route_agent(value)
            if resolved:
                if self.allow_dynamic_agents:
                    resolved = self._canonical_route_agent(resolved)
                key = (
                    target.channel,
                    target.bot_id,
                    target.external_user_id,
                    target.session_id or "default",
                )
                self._active_agent[key] = resolved
                if self.allow_dynamic_agents:
                    await self.ensure_agent(resolved)
                return resolved
        resolved = self.active_agent_for(inbound_or_target)
        if self.allow_dynamic_agents:
            resolved = self._canonical_route_agent(resolved)
            await self.ensure_agent(resolved)
        return resolved

    async def _commit_active_agent_route_locked(
        self,
        agent_id: str,
        *,
        was_deleted: bool,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> Any:
        """Commit one already-ensured route while lifecycle lock is held."""

        deleted_reader = getattr(self.store, "is_agent_deleted", None)
        # `reactivate_agent` deliberately leaves the tombstone in place until
        # every immutable definition has published.  Re-read it under the
        # lifecycle lock to cover a delete that raced the initial observation
        # or ensure_agent().
        needs_reactivation_commit = was_deleted or bool(
            deleted_reader is not None
            and await _call_compatible(deleted_reader, agent_id)
        )
        registration = self.registry.registration(agent_id)
        profile = registration.profile if registration is not None else None
        commit_reactivation = getattr(
            self.store, "commit_agent_reactivation", None
        )
        key = (channel, bot_id, external_user_id, session_id)
        if commit_reactivation is not None:
            if registration is None:
                # The Agent was removed after ensure_agent() returned.  Never
                # fall through to the compatibility sequence, which could
                # clear that fresh tombstone and route to a disabled Profile.
                raise RuntimeError(f"Agent changed while switching: {agent_id}")
            if profile is not None:
                # Use the guarded transaction for every profile-backed switch,
                # not only an already-observed recreation.  This linearizes
                # its route with retirement.
                result = await _call_compatible(
                    commit_reactivation,
                    profile,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                )
                if result is False:
                    raise RuntimeError("Agent route could not be persisted")
                self._active_agent[key] = agent_id
                return result
            if needs_reactivation_commit:
                raise RuntimeError(
                    f"Agent recreation lacks a Profile: {agent_id}"
                )

        # Compatibility stores without the atomic recreation commit keep the
        # historical clear-then-route sequence, fenced by the caller's
        # lifecycle lock.
        clear_deleted = getattr(self.store, "clear_agent_deleted", None)
        if clear_deleted is not None:
            await _call_compatible(clear_deleted, agent_id)
        method = getattr(self.store, "set_active_agent", None) or getattr(
            self.store, "set_route", None
        )
        if method is not None:
            result = await _call_compatible(
                method,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                active_agent_id=agent_id,
            )
            if result is False:
                raise RuntimeError("Agent route could not be persisted")
            self._active_agent[key] = agent_id
            return result
        self._active_agent[key] = agent_id
        return agent_id

    async def set_active_agent(
        self,
        agent_id: str,
        *,
        codex_config_profile: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> Any:
        self._assert_loop()
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            agent_id = self._canonical_route_agent(agent_id)
            deleted_reader = getattr(self.store, "is_agent_deleted", None)
            was_deleted = bool(
                deleted_reader is not None
                and await _call_compatible(deleted_reader, agent_id)
            )
            ensure_keywords: dict[str, Any] = {"allow_deleted": True}
            if codex_config_profile is not None:
                ensure_keywords["codex_config_profile"] = codex_config_profile
            profile_reader = getattr(self.store, "get_profile", None)
            durable_profile_existed = bool(
                profile_reader is not None
                and await _call_compatible(profile_reader, agent_id) is not None
            )
            created = await self.ensure_agent(agent_id, **ensure_keywords)
            # Serialize the revalidation, durable route commit, and local
            # cache update with delete_agent().  Without this lifecycle lock,
            # retirement could win after a route transaction but before the
            # local cache assignment, making a failed/stale switch appear to
            # succeed in this process.
            async with self._dynamic_agent_lock:
                try:
                    return await self._commit_active_agent_route_locked(
                        agent_id,
                        was_deleted=was_deleted,
                        channel=channel,
                        bot_id=bot_id,
                        external_user_id=external_user_id,
                        session_id=session_id,
                    )
                except BaseException as switch_error:
                    if (
                        not created
                        or durable_profile_existed
                        or was_deleted
                    ):
                        raise
                    try:
                        await self._rollback_uncommitted_dynamic_agent(
                            agent_id,
                            retire_durable_profile=True,
                        )
                    except BaseException as cleanup_error:
                        raise cleanup_error from switch_error
                    raise

    switch_agent = set_active_agent
    set_agent = set_active_agent

    async def present_notifications(
        self,
        *,
        agent_id: str,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        limit: int = 100,
        present: bool = True,
    ) -> list[Any]:
        """Read unseen user events, optionally marking them presented.

        ``present=False`` is used by command projection: the response text
        must be durable before the inbox presentation state advances.
        """

        self._assert_loop()
        method = getattr(self.store, "present_unseen", None) or getattr(self.store, "present_notifications", None)
        if method is None:
            return []
        return list(
            await _call_compatible(
                method,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                limit=limit,
                present=present,
            )
            or ()
        )

    inbox = present_notifications

    async def get_active_agent(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> str:
        self._assert_loop()
        session_id = session_id or "default"
        method = getattr(self.store, "get_route", None)
        if method is not None:
            value = await _call_compatible(
                method,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                default_agent_id=self.default_agent_id,
            )
            resolved = _route_agent(value, self.default_agent_id) or self.default_agent_id
            if self.allow_dynamic_agents:
                resolved = self._canonical_route_agent(resolved)
                await self.ensure_agent(resolved)
            return resolved
        resolved = self._active_agent.get(
            (channel, bot_id, external_user_id, session_id),
            self.default_agent_id,
        )
        if self.allow_dynamic_agents:
            resolved = self._canonical_route_agent(resolved)
            await self.ensure_agent(resolved)
        return resolved

    def _default_execution_workspace(self) -> dict[str, Any] | None:
        """Return the configured root snapshot after revalidating its identity."""

        if self.workspace_root is None or self._workspace_root_snapshot is None:
            return None
        validate_workspace_snapshot(
            self._workspace_root_snapshot,
            self.workspace_root,
        )
        return dict(self._workspace_root_snapshot)

    def _validate_execution_workspace(
        self,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate and detach one trusted task/command workspace snapshot."""

        if not isinstance(snapshot, Mapping):
            raise WorkspaceError("execution workspace snapshot must be a mapping")
        pinned_root = self._default_execution_workspace()
        if self.workspace_root is None or pinned_root is None:
            raise WorkspaceError("Agent workspace root is not configured")
        if any(
            snapshot.get(field) != pinned_root[field]
            for field in ("version", "root", "root_st_dev", "root_st_ino")
        ):
            raise WorkspaceError(
                "execution workspace does not match the pinned workspace root"
            )
        validate_workspace_snapshot(snapshot, self.workspace_root)
        return dict(snapshot)

    async def _execution_workspace_for_target(
        self,
        target: ReplyTarget | Mapping[str, Any] | None,
        agent_id: str,
    ) -> dict[str, Any] | None:
        """Resolve the future-task workspace for one session and Agent.

        The preference stores a root-relative path and directory identity.
        Both are checked again before every snapshot is accepted, so a deleted
        or replaced directory cannot silently become a different task cwd.
        """

        root_snapshot = self._default_execution_workspace()
        if root_snapshot is None:
            return None
        resolved_target = self._coerce_target(target)
        if not (
            resolved_target.channel
            and resolved_target.bot_id
            and resolved_target.external_user_id
        ):
            return root_snapshot
        getter = getattr(self.store, "get_session_working_directory", None)
        if getter is None:
            return root_snapshot
        value = await _call_compatible(
            getter,
            channel=resolved_target.channel,
            bot_id=resolved_target.bot_id,
            external_user_id=resolved_target.external_user_id,
            session_id=resolved_target.session_id or "default",
            agent_id=agent_id,
        )
        if value is None:
            return root_snapshot
        relative_path = _get(value, "relative_path", None)
        expected_device = _get(value, "directory_device", None)
        expected_inode = _get(value, "directory_inode", None)
        if (
            type(relative_path) is not str
            or not relative_path
            or type(expected_device) is not int
            or expected_device < 0
            or type(expected_inode) is not int
            or expected_inode < 0
        ):
            raise WorkspaceError("stored working directory is invalid")
        relative = Path(relative_path)
        if (
            relative.is_absolute()
            or relative.as_posix() != relative_path
            or any(part == ".." for part in relative.parts)
        ):
            raise WorkspaceError("stored working directory is invalid")
        snapshot = build_workspace_snapshot(self.workspace_root, relative_path)
        if (
            snapshot["target_st_dev"] != expected_device
            or snapshot["target_st_ino"] != expected_inode
        ):
            raise WorkspaceError("stored working directory identity has changed")
        return self._validate_execution_workspace(snapshot)

    async def get_working_directory(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        workspace_snapshot: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Return the current Agent cwd or validate an accepted command cwd."""

        self._assert_loop()
        if workspace_snapshot is not None:
            snapshot = self._validate_execution_workspace(workspace_snapshot)
            return {
                "agent_id": str(agent_id or self.default_agent_id),
                "path": str(snapshot["path"]),
                EXECUTION_WORKSPACE_KEY: snapshot,
            }

        external_user_id = str(external_user_id or user_id or "")
        session_id = str(session_id or "default")
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active or self.default_agent_id).strip()
            self.registry.require(active)
            snapshot = await self._execution_workspace_for_target(
                ReplyTarget(
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                ),
                active,
            )
            if snapshot is None:
                raise WorkspaceError("Agent workspace root is not configured")
            return {
                "agent_id": active,
                "path": str(snapshot["path"]),
                EXECUTION_WORKSPACE_KEY: snapshot,
            }

    async def set_working_directory(
        self,
        path: str | os.PathLike[str],
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        command_id: str | None = None,
        workspace_snapshot: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Persist a root-confined cwd for future tasks of the current Agent."""

        self._assert_loop()
        if self.workspace_root is None:
            raise WorkspaceError("Agent workspace root is not configured")
        try:
            requested_path = os.fspath(path)
        except TypeError as exc:
            raise WorkspaceError("working directory must be a filesystem path") from exc
        if not isinstance(requested_path, str) or not requested_path or "\x00" in requested_path:
            raise WorkspaceError("working directory path is invalid")
        external_user_id = str(external_user_id or user_id or "")
        session_id = str(session_id or "default")
        receipt_id = str(command_id or "").strip()
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active or self.default_agent_id).strip()
            self.registry.require(active)
            if self._default_execution_workspace() is None:
                raise WorkspaceError("Agent workspace root is not configured")
            candidate = Path(requested_path).expanduser()
            if not candidate.is_absolute():
                if workspace_snapshot is None:
                    base_snapshot = await self._execution_workspace_for_target(
                        ReplyTarget(
                            channel=channel,
                            bot_id=bot_id,
                            external_user_id=external_user_id,
                            session_id=session_id,
                        ),
                        active,
                    )
                    if base_snapshot is None:
                        raise WorkspaceError(
                            "Agent workspace root is not configured"
                        )
                else:
                    base_snapshot = self._validate_execution_workspace(
                        workspace_snapshot
                    )
                candidate = Path(str(base_snapshot["path"])) / candidate
            snapshot = build_workspace_snapshot(self.workspace_root, candidate)
            snapshot = self._validate_execution_workspace(snapshot)
            relative_path = Path(str(snapshot["path"])).relative_to(
                Path(self.workspace_root)
            ).as_posix()
            relative_path = relative_path or "."

            setter = getattr(self.store, "set_session_working_directory", None)
            if setter is None:
                raise AttributeError(
                    "store does not support Agent working directories"
                )
            if receipt_id:
                try:
                    parameters = inspect.signature(setter).parameters
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "store atomic working-directory support cannot be verified"
                    ) from exc
                if "command_id" not in parameters and not any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters.values()
                ):
                    raise RuntimeError(
                        "store does not support atomic working-directory receipts"
                    )
            value = await _call_compatible(
                setter,
                relative_path,
                absolute_path=str(snapshot["path"]),
                directory_device=int(snapshot["target_st_dev"]),
                directory_inode=int(snapshot["target_st_ino"]),
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=active,
                updated_by=str(actor or external_user_id),
                command_id=receipt_id or None,
            )
            if not isinstance(value, Mapping):
                raise RuntimeError(
                    "store returned an invalid working-directory preference"
                )
            if (
                str(value.get("relative_path", "")) != relative_path
                or value.get("directory_device")
                != int(snapshot["target_st_dev"])
                or value.get("directory_inode")
                != int(snapshot["target_st_ino"])
            ):
                raise RuntimeError(
                    "store returned a conflicting working-directory preference"
                )
            response = format_working_directory_response(snapshot["path"])
            result = {
                **dict(value),
                "agent_id": active,
                "path": str(snapshot["path"]),
                EXECUTION_WORKSPACE_KEY: snapshot,
            }
            if result.get("command_response") != response:
                raise RuntimeError(
                    "store returned an invalid working-directory response"
                )
            if receipt_id:
                receipt = result.get("command_receipt")
                if (
                    not isinstance(receipt, Mapping)
                    or str(receipt.get("command_id", "")) != receipt_id
                    or str(receipt.get("state", "")) != "completed"
                    or str(receipt.get("response_text", "")) != response
                    or str(receipt.get("response_agent_id", "")) != active
                ):
                    raise RuntimeError(
                        "store returned an invalid atomic working-directory receipt"
                    )
            return result

    async def _session_role_for_target(
        self, target: ReplyTarget, agent_id: str
    ) -> dict[str, Any]:
        """Resolve one canonical future-task role without materializing it."""

        target = self._coerce_target(target)
        getter = getattr(self.store, "get_session_role", None) or getattr(
            self.store, "get_role", None
        )
        if getter is None or not target.external_user_id:
            return implicit_default_role()
        value = await _call_compatible(
            getter,
            channel=target.channel,
            bot_id=target.bot_id,
            external_user_id=target.external_user_id,
            session_id=target.session_id or "default",
            agent_id=agent_id,
        )
        try:
            return validate_role_snapshot(value)
        except RoleValidationError as exc:
            raise RuntimeError("stored session role is invalid") from exc

    async def _get_system_role_unlocked(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
    ) -> dict[str, Any]:
        target = ReplyTarget(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id or "default",
        )
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id or "default",
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
            await self.ensure_agent(active)
        active = str(active or "").strip()
        self.registry.require(active)
        return await self._session_role_for_target(target, active)

    async def get_system_role(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Return the front Agent's role ordered with control mutations."""

        self._assert_loop()
        external_user_id = external_user_id or user_id
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            return await self._get_system_role_unlocked(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
            )

    async def set_system_role(
        self,
        role_text: Any,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        kind: str | None = None,
        actor: str = "",
        command_id: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Persist a role selection without invoking or waiting for an Agent.

        A command-owned mutation asks the SQLite boundary to finalize the
        durable ``/system`` receipt in the same transaction.  Direct callers
        that omit ``command_id`` retain the compatibility return shape.
        """

        self._assert_loop()
        external_user_id = external_user_id or user_id
        session_id = session_id or "default"
        receipt_id = str(command_id or "").strip()
        canonical_content = normalize_role_text(role_text)
        requested_kind = str(kind or "").strip().lower()
        if not requested_kind:
            if is_default_role_token(canonical_content):
                requested_kind = "default"
                canonical_content = ""
            else:
                requested_kind = "custom" if canonical_content else "default"
        elif requested_kind == "default" and is_default_role_token(
            canonical_content
        ):
            canonical_content = ""
        # Reuse the snapshot validator for empty/custom/reserved constraints.
        # The store assigns the durable positive version.
        build_role_snapshot(
            role_version=0,
            kind=requested_kind,
            normalized_content=canonical_content,
        )
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active or "").strip()
            self.registry.require(active)
            setter = getattr(self.store, "set_session_role", None) or getattr(
                self.store, "set_role", None
            )
            if setter is None:
                raise AttributeError("store does not support session roles")
            if receipt_id:
                try:
                    setter_parameters = inspect.signature(setter).parameters
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "store atomic system-role support cannot be verified"
                    ) from exc
                if "command_id" not in setter_parameters and not any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in setter_parameters.values()
                ):
                    # `_call_compatible` intentionally drops unsupported
                    # keywords for direct legacy calls.  Doing that here could
                    # commit a role without its receipt, so reject before the
                    # legacy setter has any opportunity to mutate state.
                    raise RuntimeError(
                        "store does not support atomic system-role receipts"
                    )
            value = await _call_compatible(
                setter,
                canonical_content,
                kind=requested_kind,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=active,
                actor=actor or external_user_id,
                created_by=actor or external_user_id,
                command_id=receipt_id or None,
            )
            if not isinstance(value, Mapping):
                raise RuntimeError("store returned an invalid session role")
            try:
                snapshot = validate_role_snapshot(value)
            except RoleValidationError as exc:
                raise RuntimeError("store returned an invalid session role") from exc
            changed = bool(value.get("changed", True))
            result = {**snapshot, "changed": changed}
            if not receipt_id:
                return result

            command_response = str(value.get("command_response", "") or "")
            command_receipt = value.get("command_receipt")
            expected_response = (
                "system role: unchanged"
                if not changed
                else "system role: default"
                if snapshot["kind"] == "default"
                else "system role: updated"
            )
            if (
                command_response != expected_response
                or not isinstance(command_receipt, Mapping)
                or str(command_receipt.get("command_id", "") or "")
                != receipt_id
                or str(command_receipt.get("state", "") or "")
                != "completed"
                or str(command_receipt.get("response_text", "") or "")
                != command_response
                or str(command_receipt.get("response_agent_id", "") or "")
                != active
                or tuple(command_receipt.get("presentation_ids", ()) or ())
                or tuple(command_receipt.get("response_fragments", ()) or ())
                or str(command_receipt.get("channel", "") or "") != channel
                or str(command_receipt.get("bot_id", "") or "") != bot_id
                or str(command_receipt.get("external_user_id", "") or "")
                != external_user_id
                or str(command_receipt.get("session_id", "") or "")
                != session_id
                or str(command_receipt.get("command_name", "") or "")
                .strip()
                .lower()
                != "system"
            ):
                raise RuntimeError(
                    "store returned an invalid atomic system-role receipt"
                )
            return {
                **result,
                "command_response": command_response,
                "command_receipt": dict(command_receipt),
            }

    get_role = get_system_role
    set_role = set_system_role

    async def list_agents(self, *, include_disabled: bool = False, **_: Any) -> list[Any]:
        self._assert_loop()
        values = list(self.registry.list(include_disabled=include_disabled))
        # A two-phase recreation may leave a prepared process-local
        # registration behind when publication or route commit fails.  The
        # durable tombstone remains authoritative, so do not expose that
        # staged alias through `/agents` while it is unusable for task ingress.
        if self.allow_dynamic_agents:
            deleted_reader = getattr(self.store, "list_deleted_agents", None)
            if deleted_reader is not None:
                deleted = {
                    str(value).strip().lower()
                    for value in await _call_compatible(deleted_reader)
                    if str(value).strip()
                }
                values = [
                    value
                    for value in values
                    if str(_get(value, "agent_id", "") or "").strip().lower()
                    not in deleted
                ]

        # Process status is intentionally attached only to process-backed
        # runtimes.  Ordinary descriptors retain their historical return type
        # and shape, which keeps lightweight embedders and command tests
        # compatible while making the production isolation boundary visible.
        enriched: list[Any] = []
        for descriptor in values:
            agent_id = str(_get(descriptor, "agent_id", "") or "").strip()
            registration = self.registry.registration(agent_id)
            runtime = registration.runtime if registration is not None else None
            is_process_runtime = bool(
                runtime is not None
                and (
                    getattr(runtime, "process_isolated", False)
                    or runtime.__class__.__name__ == "ProcessAgentRuntime"
                    or (
                        isinstance(runtime, _NamedAgentRuntime)
                        and runtime.owns_delegate
                    )
                )
            )
            if not is_process_runtime:
                enriched.append(descriptor)
                continue
            serializer = getattr(descriptor, "as_dict", None)
            if serializer is not None:
                public = dict(serializer())
            elif isinstance(descriptor, Mapping):
                public = dict(descriptor)
            else:
                public = {
                    name: getattr(descriptor, name)
                    for name in (
                        "agent_id",
                        "display_name",
                        "summary",
                        "enabled",
                        "default_mode_id",
                        "profile_version",
                    )
                    if hasattr(descriptor, name)
                }
            health = getattr(runtime, "health", "unknown")
            if callable(health):
                health = health()
            if inspect.isawaitable(health):
                health = await health
            public.update(
                {
                    "pid": getattr(runtime, "pid", None),
                    "generation": getattr(runtime, "generation", None),
                    "health": getattr(health, "value", health),
                    "process_isolated": True,
                }
            )
            enriched.append(public)
        return enriched

    agents = list_agents

    async def list_modes(self, **_: Any) -> list[Any]:
        """Return latest registered mode definitions for command rendering."""

        self._assert_loop()
        return list(self.mode_registry.list())

    modes = list_modes

    async def list_models(
        self,
        *,
        agent_id: str | None = None,
        include_hidden: bool = False,
        **_: Any,
    ) -> list[Any]:
        """Return model capabilities reported by the selected Agent runtime."""

        self._assert_loop()
        active = str(agent_id or self.default_agent_id).strip() or self.default_agent_id
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
            await self.ensure_agent(active)
        runtime = self.registry.require(active)
        method = getattr(runtime, "list_models", None)
        if method is None:
            return []
        result = await _call_compatible(method, include_hidden=include_hidden)
        return list(iter_model_descriptors(result))

    models = list_models

    async def _get_model_for_target(
        self, target: ReplyTarget, agent_id: str
    ) -> tuple[str, str]:
        target = self._coerce_target(target)
        session_id = target.session_id or "default"
        key = (
            target.channel,
            target.bot_id,
            target.external_user_id,
            session_id,
            agent_id,
        )
        method = getattr(self.store, "get_session_model_preference", None) or getattr(
            self.store, "get_model_preference", None
        )
        if method is not None and target.external_user_id:
            result = await _call_compatible(
                method,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                default_model_id=self.model,
                default_reasoning_effort=self.reasoning_effort,
            )
            resolved = _model_preference(
                result,
                default_model_id=self.model,
                default_reasoning_effort=self.reasoning_effort,
            )
            self._session_model_preferences[key] = resolved
            return resolved
        return self._session_model_preferences.get(
            key, (str(self.model or ""), str(self.reasoning_effort or ""))
        )

    async def get_model_selection(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        conversation_id: str = "",
        **_: Any,
    ) -> dict[str, str]:
        """Return the durable future-task model preference for one Agent."""

        self._assert_loop()
        external_user_id = str(external_user_id or user_id or "")
        session_id = session_id or "default"
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
            await self.ensure_agent(active)
        active = str(active).strip() or self.default_agent_id
        target = ReplyTarget(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        model_id, effort = await self._get_model_for_target(target, active)
        return {
            "agent_id": active,
            "model_id": model_id,
            "reasoning_effort": effort,
            "conversation_id": conversation_id or self._conversation_id(target, active),
        }

    get_model_preference = get_model_selection

    @staticmethod
    def _matching_model(models: Sequence[Any], model_id: str) -> Any | None:
        requested = str(model_id or "").strip().lower()
        return next(
            (model for model in models if _model_identifier(model).lower() == requested),
            None,
        )

    @staticmethod
    def _matching_reasoning_effort(model: Any, effort: str) -> str | None:
        requested = str(effort or "").strip().lower()
        return next(
            (
                candidate
                for candidate in _model_reasoning_efforts(model)
                if candidate.lower() == requested
            ),
            None,
        )

    @staticmethod
    def _effort_compatible(model: Any, effort: str) -> bool:
        """Whether the model's declared efforts permit the effort.

        A catalog entry that reports no supported efforts is unknown rather
        than incapable, so it never fails this check.
        """
        if not _model_reasoning_efforts(model):
            return True
        return TaskManager._matching_reasoning_effort(model, effort) is not None

    @staticmethod
    def _resolve_reasoning_effort(model: Any, effort: str, model_label: str) -> str:
        """Normalize an explicit effort for persistence or reject it.

        Models whose catalog entry reports no supported efforts accept the
        normalized request verbatim because their capabilities are unknown.
        """
        matched = TaskManager._matching_reasoning_effort(model, effort)
        if matched is not None:
            return matched
        if _model_reasoning_efforts(model):
            raise ValueError(
                f"model {model_label} does not support reasoning effort: {effort}"
            )
        return str(effort).strip().lower()

    async def _update_runtime_model_compatibility(
        self,
        runtime: Any,
        *,
        conversation_id: str,
        agent_id: str,
        model_id: str,
        reasoning_effort: str,
    ) -> None:
        """Update optional loop-local caches used by legacy runtimes."""

        for method_name, value in (
            ("set_model", model_id),
            ("set_reasoning_effort", reasoning_effort),
        ):
            compatibility_method = getattr(runtime, method_name, None)
            if compatibility_method is None:
                continue
            try:
                await _call_compatible(
                    compatibility_method,
                    conversation_id,
                    value,
                )
            except Exception:
                logger.warning(
                    "could not update runtime compatibility %s for Agent %s",
                    method_name,
                    agent_id,
                    exc_info=True,
                )

    async def _persist_model_preference(
        self,
        *,
        target: ReplyTarget,
        agent_id: str,
        model_id: str,
        reasoning_effort: str,
        conversation_id: str = "",
        reset_thread: bool = False,
    ) -> dict[str, str]:
        resolved_conversation = conversation_id or self._conversation_id(
            target, agent_id
        )
        runtime = self.registry.require(agent_id)

        if reset_thread:
            # The pinned SDK treats an effort override as a persistent thread
            # setting and omits ``None`` from turn/start.  Consequently an
            # empty application override cannot clear a prior native
            # ``ultra`` value.  Update legacy caches first, then forget both
            # durable and live bindings so no future task can resume the
            # sticky provider thread.
            await self._reset_thread_binding_unlocked(
                resolved_conversation,
                agent_id,
                require_runtime_reset=False,
                compatibility_selection=(model_id, reasoning_effort),
            )

        method = getattr(self.store, "set_session_model_preference", None) or getattr(
            self.store, "set_model_preference", None
        )
        if method is not None and target.external_user_id:
            persisted = await _call_compatible(
                method,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=target.session_id,
                agent_id=agent_id,
                model_id=model_id,
                reasoning_effort=reasoning_effort,
            )
            if persisted is False:
                raise RuntimeError("model selection could not be persisted")
        key = (
            target.channel,
            target.bot_id,
            target.external_user_id,
            target.session_id,
            agent_id,
        )
        self._session_model_preferences[key] = (model_id, reasoning_effort)
        if not reset_thread:
            # Older interactive adapters also keep loop-local conversation
            # overrides. They are compatibility caches only: the durable
            # scoped preference above remains authoritative for new tasks.
            await self._update_runtime_model_compatibility(
                runtime,
                conversation_id=resolved_conversation,
                agent_id=agent_id,
                model_id=model_id,
                reasoning_effort=reasoning_effort,
            )
        return {
            "agent_id": agent_id,
            "model_id": model_id,
            "reasoning_effort": reasoning_effort,
            "conversation_id": resolved_conversation,
        }

    async def set_model(
        self,
        model_id: str,
        *,
        reasoning_effort: str | None = None,
        effort: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        conversation_id: str = "",
        **_: Any,
    ) -> dict[str, str]:
        """Persist a model/effort selection used only by future tasks."""

        self._assert_loop()
        external_user_id = str(external_user_id or user_id or "")
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active).strip() or self.default_agent_id
            self.registry.require(active)
            models = await self.list_models(agent_id=active)
            if not models:
                raise RuntimeError("model catalog is unavailable")
            selected = self._matching_model(models, model_id)
            if selected is None:
                raise ValueError(f"unknown model: {model_id}")
            canonical_model = _model_identifier(selected)
            if not canonical_model:
                raise ValueError("model ID is required")
            target = ReplyTarget(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            previous_model, previous_effort = await self._get_model_for_target(
                target, active
            )
            requested_effort = reasoning_effort if reasoning_effort is not None else effort
            if requested_effort is None:
                resolved_effort = previous_effort
                if selected is not None and resolved_effort and not self._effort_compatible(
                    selected, resolved_effort
                ):
                    resolved_effort = ""
            elif str(requested_effort).strip().lower() == "default":
                resolved_effort = ""
            elif selected is not None:
                resolved_effort = self._resolve_reasoning_effort(
                    selected, str(requested_effort), canonical_model
                )
            else:
                resolved_effort = str(requested_effort).strip()
            return await self._persist_model_preference(
                target=target,
                agent_id=active,
                model_id=canonical_model,
                reasoning_effort=resolved_effort,
                conversation_id=conversation_id,
                reset_thread=bool(previous_effort and not resolved_effort),
            )

    async def set_reasoning_effort(
        self,
        reasoning_effort: str,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        conversation_id: str = "",
        **_: Any,
    ) -> dict[str, str]:
        """Change only the durable effort override for the current Agent."""

        self._assert_loop()
        external_user_id = str(external_user_id or user_id or "")
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active).strip() or self.default_agent_id
            target = ReplyTarget(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            model_id, _previous_effort = await self._get_model_for_target(target, active)
            requested = str(reasoning_effort or "").strip()
            if requested.lower() == "default":
                # Clearing an override never needs model capabilities: the
                # runtime's default is valid by definition, even while live
                # model discovery is unavailable. Keep the durable model
                # selection unchanged and clear only its effort override.
                return await self._persist_model_preference(
                    target=target,
                    agent_id=active,
                    model_id=model_id,
                    reasoning_effort="",
                    conversation_id=conversation_id,
                    reset_thread=bool(_previous_effort),
                )
            models = await self.list_models(agent_id=active)
            if not models:
                raise RuntimeError("model catalog is unavailable")
            selected = (
                self._matching_model(models, model_id)
                if model_id
                else next((model for model in models if _model_is_default(model)), None)
            )
            if selected is not None:
                resolved_effort = self._resolve_reasoning_effort(
                    selected, requested, _model_identifier(selected)
                )
            else:
                # The runtime can expose a catalog without identifying which
                # entry is its opaque default.  An effort supported by every
                # candidate that declares efforts is safe regardless of that
                # hidden selection; anything narrower would risk persisting
                # an invalid pair.  Entries that report no efforts are
                # unknown, so they neither prove nor block a request.
                supported_by_all: list[str] = []
                for model in models:
                    if not _model_reasoning_efforts(model):
                        continue
                    matched = self._matching_reasoning_effort(model, requested)
                    if matched is None:
                        raise ValueError(
                            "current runtime-default model cannot be proven to "
                            f"support reasoning effort: {requested}"
                        )
                    supported_by_all.append(matched)
                if supported_by_all:
                    resolved_effort = supported_by_all[0]
                else:
                    resolved_effort = str(requested).strip().lower()
            return await self._persist_model_preference(
                target=target,
                agent_id=active,
                model_id=model_id,
                reasoning_effort=resolved_effort,
                conversation_id=conversation_id,
            )

    async def list_skills(
        self,
        *,
        agent_id: str | None = None,
        refresh: bool = False,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        workspace_snapshot: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> list[Any]:
        """Return the trusted skill catalog for the selected Agent."""

        self._assert_loop()
        active = str(agent_id or self.default_agent_id).strip() or self.default_agent_id
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
        runtime = self.registry.require(active)
        cwd: str | None = None
        if workspace_snapshot is not None:
            cwd = str(
                self._validate_execution_workspace(workspace_snapshot)["path"]
            )
        elif self.workspace_root is not None:
            target = ReplyTarget(
                channel=channel,
                bot_id=bot_id,
                external_user_id=str(external_user_id or user_id or ""),
                session_id=session_id or "default",
            )
            execution_workspace = await self._execution_workspace_for_target(
                target,
                active,
            )
            if execution_workspace is not None:
                cwd = str(execution_workspace["path"])
        method = getattr(runtime, "list_skills", None)
        if method is None:
            method = getattr(self.registry, "list_skills", None)
        if method is None:
            return []
        result = await _call_compatible(
            method,
            agent_id=active,
            refresh=refresh,
            cwd=cwd,
        )
        values = list(iter_skill_descriptors(result))
        writer = getattr(self.store, "put_skill", None) or getattr(
            self.store, "register_skill", None
        )
        if writer is not None:
            # Discovery is live and authoritative for new invocations.  The
            # durable table is append-only history used by immutable task
            # snapshots and restart diagnostics; a same-version conflict is
            # propagated instead of silently trusting changed path/content.
            for definition in normalize_skills(
                values,
                rehash_local_bundles=True,
            ):
                await _call_compatible(writer, definition, agent_id=active)
        return values

    async def resolve_skill(
        self,
        name: str,
        *,
        agent_id: str | None = None,
        refresh: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Resolve and snapshot one trusted skill for channel ingress."""

        values = await self.list_skills(
            agent_id=agent_id,
            refresh=refresh,
            **kwargs,
        )
        definition = find_skill(values, name, rehash_local_bundles=True)
        return definition.snapshot() if definition is not None else None

    async def _get_mode_for_target(
        self, target: ReplyTarget, agent_id: str
    ) -> tuple[str, int] | None:
        target = self._coerce_target(target)
        session_id = target.session_id or "default"
        key = (
            target.channel,
            target.bot_id,
            target.external_user_id,
            session_id,
            agent_id,
        )
        default_mode_id, default_policy_version = self._default_mode_selection(agent_id)
        method = getattr(self.store, "get_session_mode", None)
        if method is not None and target.external_user_id:
            result = await _call_compatible(
                method,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                default_mode_id=default_mode_id,
                default_policy_version=default_policy_version,
            )
            resolved = _mode_selection(
                result,
                default_mode_id=default_mode_id,
                default_policy_version=default_policy_version,
            )
            if resolved[0] == "execute":
                authorized = False
                auth_method = getattr(self.store, "is_mode_authorized", None) or getattr(
                    self.store, "mode_authorized", None
                )
                if auth_method is not None:
                    try:
                        authorized = bool(
                            await _call_compatible(
                                auth_method,
                                channel=target.channel,
                                bot_id=target.bot_id,
                                external_user_id=target.external_user_id,
                                session_id=session_id,
                                agent_id=agent_id,
                                mode_id="execute",
                                policy_version=resolved[1],
                            )
                        )
                    except Exception:
                        logger.debug("could not load execute authorization", exc_info=True)
                # Refresh both directions.  A revoked durable authorization
                # must not remain usable merely because this process observed
                # it as valid earlier.
                if authorized:
                    self._authorized_execute_modes.add(key)
                else:
                    self._authorized_execute_modes.discard(key)
                    if not self._trusted_default_execute_for(
                        agent_id,
                        policy_version=resolved[1],
                    ):
                        # A legacy/crash-written execute selection without its
                        # actor and timestamp is not an authorization.  Treat
                        # it as the Agent's safe non-execute default instead of
                        # passing it to `_policy_snapshot`, where it would turn
                        # every redelivery of the same inbound message into a
                        # permanent PermissionError retry loop.  This is an
                        # effective demotion only: a later explicit `/mode
                        # execute` atomically replaces the durable row with
                        # complete authorization provenance.
                        fallback = (default_mode_id, default_policy_version)
                        if fallback[0] == "execute":
                            chat = self._mode_for_agent(agent_id, "chat")
                            if chat is None:
                                raise PermissionError(
                                    "unauthorized execute mode has no safe fallback"
                                )
                            fallback = ("chat", int(chat.policy_version))
                        resolved = fallback
            else:
                self._authorized_execute_modes.discard(key)
            self._session_modes[key] = resolved
            return resolved
        return self._session_modes.get(key, (default_mode_id, default_policy_version))

    def _execute_authorized(
        self,
        target: ReplyTarget,
        agent_id: str,
        policy_version: int | None = None,
    ) -> bool:
        # A trusted startup configuration is durable authorization supplied by
        # the administrator, rather than a user/session selection.  It is
        # scoped to the configured default Agent and exact default execute
        # policy version; a different Agent or upgraded mode version still
        # requires an explicit authorization record.
        if self._trusted_default_execute_for(
            agent_id,
            policy_version=policy_version,
        ):
            return True
        key = (
            target.channel,
            target.bot_id,
            target.external_user_id,
            target.session_id,
            agent_id,
        )
        if key not in self._authorized_execute_modes:
            return False
        selected = self._session_modes.get(key)
        return bool(
            selected is not None
            and selected[0] == "execute"
            and (
                policy_version is None
                or int(selected[1]) == int(policy_version)
            )
        )

    def _trusted_default_execute_for(
        self,
        agent_id: str,
        *,
        policy_version: int | None = None,
    ) -> bool:
        """Return whether startup config authorizes this exact execute mode."""

        if not self.trusted_default_execute:
            return False
        if str(agent_id or "").strip() != str(self.default_agent_id or "").strip():
            return False
        try:
            selected_mode, selected_version = self._default_mode_selection(agent_id)
        except Exception:
            return False
        if selected_mode != "execute":
            return False
        if policy_version is not None:
            try:
                if int(selected_version) != int(policy_version):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    async def _assert_compaction_idle_unlocked(
        self,
        conversation_id: str,
        agent_id: str,
    ) -> None:
        """Fail closed when the exact conversation has active execution."""

        list_tasks = getattr(self.store, "list_tasks", None)
        if list_tasks is None:
            raise AttributeError(
                "store does not support active task lookup for context compaction"
            )
        rows = await _call_compatible(
            list_tasks,
            states=("claimed", "running", "cancel_requested"),
            conversation_id=conversation_id,
            agent_id=agent_id,
            limit=100,
        )
        active_states = {"claimed", "running", "cancel_requested"}
        for row in rows or ():
            raw_state = _get(row, "state", _get(row, "status", ""))
            state = str(getattr(raw_state, "value", raw_state) or "").lower()
            if (
                state in active_states
                and str(_get(row, "conversation_id", conversation_id))
                == conversation_id
                and str(_get(row, "agent_id", agent_id)) == agent_id
            ):
                raise RuntimeError("cannot compact while a task is running")

    async def compact_session(
        self,
        conversation_id: str | None = None,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        **_: Any,
    ) -> Any:
        """Compact the exact durable context selected for one Agent session.

        The control operation is ordered with route, mode, model, role, cwd,
        and clear mutations through the session lock.  It reconstructs the
        same immutable policy/role/workspace snapshot as a newly accepted
        turn and supplies the matching persisted provider thread to the
        selected Agent runtime.  No empty thread is created as a side effect.
        """

        self._assert_loop()
        external_user_id = str(external_user_id or user_id or "")
        session_id = str(session_id or "default")
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            target = ReplyTarget(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            active = agent_id or await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            if self.allow_dynamic_agents:
                active = self._canonical_route_agent(active)
                await self.ensure_agent(active)
            active = str(active or self.default_agent_id).strip()
            runtime = self.registry.require(active)

            if conversation_id and not conversation_id_matches(
                conversation_id,
                channel,
                bot_id,
                external_user_id,
                session_id,
                active,
            ):
                raise ValueError(
                    "conversation does not match the selected Agent session"
                )
            resolved_conversation = str(
                conversation_id or self._conversation_id(target, active)
            )
            await self._assert_compaction_idle_unlocked(
                resolved_conversation,
                active,
            )

            selected_mode = await self._get_mode_for_target(target, active)
            if selected_mode is None:
                selected_mode = self._default_mode_selection(active)
            mode_id, policy_version = selected_mode
            profile_version = self._default_profile_version(active)
            session_role = await self._session_role_for_target(target, active)
            model_id, _reasoning_effort = await self._get_model_for_target(
                target,
                active,
            )
            metadata = self._policy_snapshot(
                active,
                mode_id,
                profile_version,
                policy_version,
                actor=actor or external_user_id,
                explicit=(
                    mode_id == "execute"
                    and self._execute_authorized(
                        target,
                        active,
                        policy_version,
                    )
                ),
            )
            metadata["session_role"] = session_role
            execution_workspace = await self._execution_workspace_for_target(
                target,
                active,
            )
            if execution_workspace is not None:
                metadata[EXECUTION_WORKSPACE_KEY] = execution_workspace

            get_binding = getattr(
                self.store, "get_thread_binding", None
            ) or getattr(self.store, "thread_binding", None)
            if get_binding is None:
                raise AttributeError(
                    "store does not support durable context bindings"
                )
            binding = await _call_compatible(
                get_binding,
                resolved_conversation,
                mode_id=mode_id,
                profile_version=profile_version,
                policy_version=policy_version,
                session_role=session_role,
            )
            thread_id = str(_get(binding, "thread_id", binding) or "").strip()
            if not thread_id:
                raise RuntimeError(
                    "cannot compact session: no Codex thread is bound"
                )

            compact = getattr(runtime, "compact_session", None) or getattr(
                runtime, "compact_conversation", None
            )
            if compact is None:
                raise AttributeError(
                    "Agent runtime does not support context compaction"
                )
            result = compact(
                resolved_conversation,
                mode_id=mode_id,
                profile_version=profile_version,
                policy_version=policy_version,
                session_role=session_role,
                thread_id=thread_id,
                agent_id=active,
                model=model_id,
                metadata=metadata,
            )
            if inspect.isawaitable(result):
                result = await result
            return result

    compact_conversation = compact_session

    async def _reset_thread_binding_unlocked(
        self,
        conversation_id: str,
        agent_id: str,
        *,
        require_runtime_reset: bool = True,
        compatibility_selection: tuple[str, str] | None = None,
    ) -> str:
        """Forget one durable/live provider binding under its scope lock."""

        resolved_conversation = str(conversation_id)
        active = str(agent_id).strip()
        self.registry.require(active)

        # A reset must not mutate a task whose execution may already have
        # produced external side effects. Queued/terminal snapshots stay
        # immutable; only an active execution prevents the control action.
        list_tasks = getattr(self.store, "list_tasks", None)
        if list_tasks is not None:
            rows = await _call_compatible(
                list_tasks,
                states=("claimed", "running", "cancel_requested"),
                conversation_id=resolved_conversation,
                agent_id=active,
                limit=100,
            )
            active_states = {"claimed", "running", "cancel_requested"}
            active_rows = [
                row
                for row in list(rows or ())
                if str(
                    getattr(
                        _get(row, "state", _get(row, "status", "")),
                        "value",
                        _get(row, "state", _get(row, "status", "")),
                    )
                    or ""
                ).lower()
                in active_states
                and str(_get(row, "conversation_id", resolved_conversation))
                == resolved_conversation
                and str(_get(row, "agent_id", active)) == active
            ]
            if active_rows:
                raise RuntimeError("cannot clear while a task is running")

        runtime = self.registry.require(active)
        if compatibility_selection is not None:
            model_id, reasoning_effort = compatibility_selection
            await self._update_runtime_model_compatibility(
                runtime,
                conversation_id=resolved_conversation,
                agent_id=active,
                model_id=model_id,
                reasoning_effort=reasoning_effort,
            )

        clear_binding = getattr(
            self.store, "clear_thread_bindings", None
        ) or getattr(self.store, "clear_conversation_threads", None)
        if clear_binding is not None:
            await _call_compatible(clear_binding, resolved_conversation)

        reset = getattr(runtime, "reset_session", None) or getattr(
            runtime, "clear_session", None
        )
        if reset is None:
            if require_runtime_reset:
                raise AttributeError(
                    "Agent runtime does not support conversation reset"
                )
            return ""
        result = await _call_compatible(reset, resolved_conversation)
        return str(result or "")

    async def clear_session(
        self,
        conversation_id: str | None = None,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        **_: Any,
    ) -> str:
        """Clear one user's Codex conversation and start a fresh thread.

        The command is serialized with route/mode mutations.  Existing active
        executions are left untouched and therefore block a reset rather than
        having their immutable thread snapshot replaced.  SQLite's binding
        lookup is cleared before the runtime reset so a later task cannot
        resurrect the old thread after a process restart.
        """

        self._assert_loop()
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            target = ReplyTarget(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            active = str(
                agent_id
                or await self.get_active_agent(
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                )
                or self.default_agent_id
            ).strip()
            self.registry.require(active)
            resolved_conversation = str(
                conversation_id or self._conversation_id(target, active)
            )
            return await self._reset_thread_binding_unlocked(
                resolved_conversation,
                active,
            )

    reset_session = clear_session
    clear_conversation = clear_session
    reset_conversation = clear_session

    async def get_mode(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        **_: Any,
    ) -> str:
        self._assert_loop()
        session_id = session_id or "default"
        target = ReplyTarget(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
        mode = await self._get_mode_for_target(target, active)
        return mode[0] if mode else self.default_mode_id

    async def _set_mode_unlocked(
        self,
        mode_id: str,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        explicit: bool = False,
        **_: Any,
    ) -> str:
        """Persist a future-task mode; running snapshots remain unchanged."""
        self._assert_loop()
        session_id = session_id or "default"
        mode_id = str(mode_id).strip().lower()
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
        active = str(active).strip()
        self.registry.require(active)
        mode = self._mode_for_agent(active, mode_id)
        decision = self.policy_engine.authorize_mode(
            mode,
            actor=actor or external_user_id,
            explicit=explicit,
        )
        decision.require()
        profile_getter = getattr(self.registry, "profile", None)
        profile = profile_getter(active) if profile_getter is not None else None
        if mode.mode_id == "execute" and profile is None:
            raise PermissionError(
                "execute mode requires an Agent profile with write and command permissions"
            )
        if profile is not None:
            effective = self.policy_engine.effective_policy(profile, mode)
            if mode.mode_id == "execute":
                # A writable sandbox alone is not an execute authorization.
                # Execute is the explicitly authorized unrestricted mode, so
                # reject profiles that narrow either file writes or command
                # execution (or alter the required sandbox)
                # before persisting the session selection.
                if (
                    effective.sandbox_policy
                    != _execute_sandbox_for_version(mode.policy_version)
                    or not effective.can_write_files
                    or not effective.can_execute_commands
                ):
                    raise PermissionError(
                        "execute mode requires workspace writes and command execution"
                    )
        key = (channel, bot_id, external_user_id, session_id, active)
        value = (mode.mode_id, int(mode.policy_version))
        method = getattr(self.store, "set_session_mode", None)
        if method is not None:
            persisted = await _call_compatible(
                method,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=active,
                mode_id=mode.mode_id,
                policy_version=mode.policy_version,
                authorized_by=(actor or external_user_id) if mode.mode_id == "execute" else None,
                authorized_at=utc_now() if mode.mode_id == "execute" else None,
            )
            if persisted is False:
                raise RuntimeError("mode selection could not be persisted")
        self._session_modes[key] = value
        if mode.mode_id == "execute":
            self._authorized_execute_modes.add(key)
        else:
            self._authorized_execute_modes.discard(key)
        return mode.mode_id

    async def set_mode(
        self,
        mode_id: str,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        explicit: bool = False,
        **kwargs: Any,
    ) -> str:
        self._assert_loop()
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            return await self._set_mode_unlocked(
                mode_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                actor=actor,
                explicit=explicit,
                **kwargs,
            )

    switch_mode = set_mode

    async def _set_notify_unlocked(
        self,
        enabled: bool,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        **_: Any,
    ) -> bool:
        self._assert_loop()
        session_id = session_id or "default"
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
        method = getattr(self.store, "set_notification_preference", None)
        if method is None:
            return bool(enabled)
        await _call_compatible(
            method,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
            agent_id=active,
            enabled=enabled,
        )
        return bool(enabled)

    async def set_notify(
        self,
        enabled: bool,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        **kwargs: Any,
    ) -> bool:
        self._assert_loop()
        session_id = session_id or "default"
        async with self._scope_lock(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        ):
            return await self._set_notify_unlocked(
                enabled,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
                **kwargs,
            )

    set_notify_preference = set_notify

    async def get_notify(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        **_: Any,
    ) -> bool:
        self._assert_loop()
        session_id = session_id or "default"
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        if self.allow_dynamic_agents:
            active = self._canonical_route_agent(active)
        method = getattr(self.store, "get_notification_preference", None)
        if method is None:
            return True
        return bool(await _call_compatible(
            method,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
            agent_id=active,
        ))

    async def inbox(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        limit: int = 100,
        present: bool = True,
        switch_only: bool = False,
        **_: Any,
    ) -> list[Any]:
        self._assert_loop()
        active = agent_id or await self.get_active_agent(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        # Item candidates, rather than transport outboxes, own replies that
        # were retained because their Agent was in the background.  A
        # switch-back reads only that class; generic `/inbox` additionally
        # retains the legacy/outbox presentation surface below.
        candidate_method = getattr(
            self.store, "present_inbox_candidates", None
        )
        candidates: list[Any] = []
        if candidate_method is not None:
            candidates = list(
                await _call_compatible(
                    candidate_method,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                    agent_id=active,
                    limit=limit,
                    present=present,
                    switch_only=switch_only,
                )
                or ()
            )
        if switch_only:
            return candidates
        remaining = max(0, int(limit) - len(candidates))
        if remaining <= 0:
            return candidates
        if present:
            method = getattr(self.store, "present_unseen", None)
            if method is not None:
                return candidates + list(
                    await _call_compatible(
                        method,
                        channel=channel,
                        bot_id=bot_id,
                        external_user_id=external_user_id,
                        session_id=session_id,
                        agent_id=active,
                        limit=remaining,
                    )
                    or ()
                )
        method = getattr(self.store, "list_outbox", None)
        if method is None:
            return candidates
        return candidates + list(
            await _call_compatible(
                method,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=active,
                unseen=True,
                limit=remaining,
            )
            or ()
        )

    async def switch_back_inbox(
        self,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str,
        limit: int = 100,
        present: bool = False,
        **_: Any,
    ) -> list[Any]:
        """Return only unread items completed while ``agent_id`` was away."""

        return await self.inbox(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
            agent_id=agent_id,
            limit=limit,
            present=present,
            switch_only=True,
        )

    # ------------------------------------------------------------------ Agent collaboration
    async def _collaboration_policy(
        self,
        source_agent_id: str,
        *,
        task_id: str | None = None,
        require_active_task: bool = False,
        required_execution_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> Any:
        if task_id:
            task = await self.get_task(str(task_id))
            if task is None:
                raise PermissionError(f"task unavailable: {task_id}")
            if str(_get(task, "agent_id", "") or "") != str(source_agent_id):
                raise PermissionError("source Agent does not own the task")
            if required_execution_id is not None and str(
                _get(task, "execution_id", "") or ""
            ) != str(required_execution_id):
                raise PermissionError(
                    "Agent bridge capability does not match the task execution"
                )
            if require_active_task:
                state = getattr(
                    _get(task, "state", _get(task, "status", "")),
                    "value",
                    _get(task, "state", _get(task, "status", "")),
                )
                if str(state or "").strip().lower() != "running":
                    raise PermissionError("Agent bridge requires a running task")

            task_target = self._coerce_target(_get(task, "reply_target", None))
            supplied_scope = (channel, bot_id, external_user_id)
            expected_scope = (
                task_target.channel,
                task_target.bot_id,
                task_target.external_user_id,
            )
            for supplied, expected in zip(supplied_scope, expected_scope):
                if supplied and str(supplied) != str(expected or ""):
                    raise PermissionError("task does not belong to this session")
            if (
                (any(supplied_scope) or (session_id and session_id != "default"))
                and task_target.session_id
                and str(session_id or "default") != str(task_target.session_id)
            ):
                raise PermissionError("task does not belong to this session")

            metadata = _get(task, "metadata", {}) or {}
            policy_data = (
                metadata.get("effective_policy")
                if isinstance(metadata, Mapping)
                else None
            )
            if policy_data is not None:
                try:
                    policy = (
                        policy_data
                        if isinstance(policy_data, EffectivePolicy)
                        else EffectivePolicy(**dict(policy_data))
                    )
                except (TypeError, ValueError) as exc:
                    raise PermissionError("task policy snapshot is invalid") from exc
                expected = {
                    "profile_id": source_agent_id,
                    "profile_version": _get(task, "profile_version", None),
                    "mode_id": _get(task, "mode_id", None),
                    "mode_policy_version": _get(task, "policy_version", None),
                }
                if any(
                    str(_get(policy, name, "")) != str(value)
                    for name, value in expected.items()
                ):
                    raise PermissionError("task policy snapshot does not match task")
                return policy

            # Compatibility tasks may predate embedded effective-policy JSON.
            # Resolve only their exact immutable profile/mode versions; never
            # fall back to the current session selection.
            profile = self.registry.profile(
                source_agent_id, int(_get(task, "profile_version", 1))
            )
            mode = self.mode_registry.get(
                str(_get(task, "mode_id", "chat")),
                int(_get(task, "policy_version", 1)),
            )
            if profile is None or mode is None:
                raise PermissionError("task policy snapshot is unavailable")
            return self.policy_engine.effective_policy(profile, mode)

        if required_execution_id is not None:
            raise PermissionError(
                "Agent bridge capability requires a durable task"
            )
        profile = self.registry.profile(source_agent_id)
        if profile is None or not profile.enabled:
            raise PermissionError(f"Agent profile unavailable: {source_agent_id}")
        target = ReplyTarget(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id or "default",
        )
        selected = await self._get_mode_for_target(target, source_agent_id)
        mode_id, version = selected or (profile.default_mode_id, self.policy_version)
        mode = self.mode_registry.get(mode_id, version)
        if mode is None:
            raise PermissionError(f"Agent mode unavailable: {mode_id}@{version}")
        return self.policy_engine.effective_policy(profile, mode)

    async def list_agent_peers(
        self,
        task_id: str,
        *,
        request_type: str = "ask",
        require_active_task: bool = True,
        required_execution_id: str | None = None,
    ) -> list[Any]:
        """Return public peers authorized by one immutable task snapshot.

        This is the discovery half of the local Agent bridge.  It exposes only
        public descriptors, never Profile prompts or private configuration, and
        applies the same mode/Profile/request-type gates as message enqueueing.
        """

        self._assert_loop()
        task = await self.get_task(str(task_id))
        if task is None:
            raise PermissionError(f"task unavailable: {task_id}")
        source = str(_get(task, "agent_id", "") or "").strip()
        if not source:
            raise PermissionError("task Agent is unavailable")
        request_kind = str(request_type or "ask").strip()
        policy = await self._collaboration_policy(
            source,
            task_id=str(task_id),
            require_active_task=require_active_task,
            required_execution_id=required_execution_id,
        )
        self.policy_engine.check_request_type(policy, request_kind).require()

        peers: list[Any] = []
        for descriptor in self.registry.list():
            destination = str(_get(descriptor, "agent_id", "") or "").strip()
            if not destination or destination == source:
                continue
            if not self.policy_engine.check_peer(policy, destination).allowed:
                continue
            accepted = frozenset(
                _get(descriptor, "accepted_request_types", ()) or ()
            )
            if request_kind not in accepted:
                continue
            public = getattr(descriptor, "peer_descriptor", None)
            peers.append(public() if public is not None else descriptor)
        return peers

    agent_peers = list_agent_peers

    async def _audit_collaboration(
        self,
        *,
        allowed: bool,
        reason: str,
        actor: str,
        source_agent_id: str,
        destination_agent_id: str,
        request_type: str,
        task_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        action: str = "agent_message",
    ) -> None:
        recorder = getattr(self.store, "record_audit_event", None) or getattr(
            self.store, "append_audit_event", None
        )
        if recorder is None:
            return
        try:
            await _call_compatible(
                recorder,
                action=action,
                allowed=allowed,
                reason=reason,
                actor=actor,
                source_agent_id=source_agent_id,
                destination_agent_id=destination_agent_id,
                request_type=request_type,
                task_id=task_id,
                payload=dict(payload or {}),
            )
        except Exception:
            logger.debug("could not persist collaboration audit event", exc_info=True)

    async def _validate_agent_message_attachments(
        self,
        payload: Any,
        *,
        source_agent_id: str = "",
        task_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> None:
        """Require every managed attachment to exist and belong to the source."""

        attachment_getter = getattr(self.store, "get_attachment", None)
        if attachment_getter is None:
            return
        access_checker = (
            getattr(self.store, "can_access_attachment", None)
            or getattr(self.store, "attachment_accessible", None)
            or getattr(self.store, "check_attachment_access", None)
        )
        attachment_values = self._attachment_values(payload)
        for raw_attachment in attachment_values:
            if isinstance(raw_attachment, Mapping):
                attachment_id = raw_attachment.get(
                    "attachment_id", raw_attachment.get("id", "")
                )
            else:
                attachment_id = getattr(
                    raw_attachment, "attachment_id", raw_attachment
                )
            attachment_id = str(attachment_id or "").strip()
            if not attachment_id:
                continue
            found = await _call_compatible(attachment_getter, attachment_id)
            if found is None:
                raise PermissionError(f"attachment is unavailable: {attachment_id}")
            if access_checker is not None:
                accessible = await _call_compatible(
                    access_checker,
                    attachment_id,
                    agent_id=source_agent_id,
                    source_agent_id=source_agent_id,
                    task_id=task_id,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id or "default",
                )
                if not accessible:
                    raise PermissionError(
                        f"attachment access denied: {attachment_id}"
                    )

    @staticmethod
    def _attachment_values(value: Any) -> tuple[Any, ...]:
        """Extract every explicit managed-attachment field from a payload."""

        if isinstance(value, Mapping):
            result: list[Any] = []
            for key in ("attachments", "attachment_ids", "media"):
                raw = value.get(key)
                if raw is None:
                    continue
                if isinstance(raw, (Mapping, str, bytes, bytearray)):
                    result.append(raw)
                else:
                    try:
                        result.extend(raw)
                    except TypeError:
                        result.append(raw)
            if value.get("attachment_id") is not None:
                result.append(value["attachment_id"])
            return tuple(result)
        if isinstance(value, (str, bytes, bytearray)):
            return ()
        try:
            candidates = tuple(value)
        except TypeError:
            candidates = (value,)
        # The SQLite task serializer treats a non-mapping input sequence as
        # attachment-shaped values. Validate the identical set here so a list
        # containing a managed ID cannot acquire it by creating the child ref.
        return candidates

    async def _mailbox_execution_snapshot(
        self,
        destination_agent_id: str,
        *,
        task_id: str | None = None,
        request_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> dict[str, Any]:
        """Freeze the destination Agent task context before mailbox enqueue.

        A mailbox request is executed outside the source task worker, so it
        cannot inherit the source task's user conversation or thread.  Every
        direction in one request chain receives a deterministic mailbox-only
        conversation keyed by its destination and logical request ID. Resolve
        the destination's own route/mode/profile and persist the resulting
        policy metadata with the mailbox projection.
        """

        destination = str(destination_agent_id or "").strip()
        if not destination:
            raise ValueError("destination Agent is required")
        logical_request_id = str(request_id or "").strip()
        if not logical_request_id:
            raise ValueError("mailbox request ID is required")
        conversation = mailbox_conversation_id(destination, logical_request_id)
        source_task = await self.get_task(str(task_id)) if task_id else None
        if source_task is not None:
            target = self._coerce_target(_get(source_task, "reply_target", None))
            model = str(_get(source_task, "model", "") or self.model)
            reasoning_effort = str(
                _get(source_task, "reasoning_effort", "") or self.reasoning_effort
            )
            # A correlated response returns to the Agent that owns the
            # originating task. Reuse that exact immutable task binding rather
            # than resolving the Agent's now-current session mode.
            if str(_get(source_task, "agent_id", "") or "") == destination:
                metadata = _get(source_task, "metadata", {}) or {}
                metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                metadata.pop("session_role", None)
                # Correlated replies retain the originating task's immutable
                # policy/thread binding, but cwd is destination-scoped mutable
                # state.  Resolve it when this new mailbox turn is accepted.
                metadata.pop(EXECUTION_WORKSPACE_KEY, None)
                response_workspace = await self._execution_workspace_for_target(
                    target,
                    destination,
                )
                if response_workspace is not None:
                    metadata[EXECUTION_WORKSPACE_KEY] = response_workspace
                return {
                    "agent_id": destination,
                    "conversation_id": conversation,
                    "reply_target": target.to_dict(),
                    "mode_id": str(_get(source_task, "mode_id", "chat") or "chat"),
                    "profile_version": int(
                        _get(source_task, "profile_version", 1) or 1
                    ),
                    "policy_version": int(
                        _get(source_task, "policy_version", 1) or 1
                    ),
                    "model": model,
                    "reasoning_effort": reasoning_effort,
                    "metadata": metadata,
                }
        else:
            target = ReplyTarget(
                channel=str(channel or ""),
                bot_id=str(bot_id or ""),
                external_user_id=str(external_user_id or ""),
                session_id=str(session_id or "default") or "default",
            )
            model = str(self.model or "")
            reasoning_effort = str(self.reasoning_effort or "")
        has_route = bool(target.channel and target.bot_id and target.external_user_id)
        default_mode, default_version = self._default_mode_selection(destination)
        route_mode = (
            await self._get_mode_for_target(target, destination)
            if has_route
            else None
        )
        mode_id = str(
            route_mode[0] if route_mode is not None else default_mode
        ).strip().lower()
        policy_version = int(
            route_mode[1] if route_mode is not None else default_version
        )
        profile_version = self._default_profile_version(destination)
        metadata = self._policy_snapshot(
            destination,
            mode_id,
            profile_version,
            policy_version,
            actor=target.external_user_id,
            explicit=(
                mode_id != "execute"
                or self._execute_authorized(target, destination, policy_version)
            ),
        )
        metadata.pop("session_role", None)
        execution_workspace = await self._execution_workspace_for_target(
            target,
            destination,
        )
        if execution_workspace is not None:
            metadata[EXECUTION_WORKSPACE_KEY] = execution_workspace
        return {
            "agent_id": destination,
            "conversation_id": conversation,
            "reply_target": target.to_dict(),
            "mode_id": mode_id,
            "profile_version": int(profile_version),
            "policy_version": int(policy_version),
            "model": model,
            "reasoning_effort": reasoning_effort,
            "metadata": metadata,
        }

    async def send_agent_message(
        self,
        destination_agent_id: str,
        content: str,
        *,
        source_agent_id: str | None = None,
        request_type: str = "ask",
        request_id: str | None = None,
        reply_to_id: str | None = None,
        causation_id: str | None = None,
        task_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str | None = None,
        actor: str = "",
        require_active_task: bool = False,
        required_execution_id: str | None = None,
        **_: Any,
    ) -> Any:
        """Authorize and enqueue an Agent-directed message.

        This method never creates a user outbox projection.  The destination
        is validated against the source profile/mode and the destination's
        public accepted request types before the store is called.
        """
        self._assert_loop()
        destination = str(destination_agent_id).strip()
        message = str(content or "").strip()
        if not destination or not message:
            raise ValueError("destination Agent and content are required")
        source = source_agent_id or agent_id
        if source is None and task_id:
            source_task = await self.get_task(str(task_id))
            if source_task is not None:
                source = str(_get(source_task, "agent_id", "") or "")
        if not source:
            source = await self.get_active_agent(
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
        source = str(source).strip()
        request_kind = str(request_type or "ask").strip()
        # Allocate the logical request identity once, before deriving the
        # mailbox-only conversation or asking the store to persist the row.
        # A store-generated fallback would make the runtime snapshot and the
        # durable correlation use different request IDs.
        request_id = str(request_id or "").strip() or uuid.uuid4().hex
        envelope_payload = dict(payload or {})
        reason = ""
        try:
            if source == destination:
                raise PermissionError("an Agent cannot send a mailbox request to itself")
            destination_descriptor = self.registry.descriptor(destination)
            if destination_descriptor is None or not destination_descriptor.enabled:
                raise PermissionError(f"unknown or disabled Agent: {destination}")
            policy = await self._collaboration_policy(
                source,
                task_id=task_id,
                require_active_task=require_active_task,
                required_execution_id=required_execution_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
            self.policy_engine.check_peer(policy, destination).require()
            self.policy_engine.check_request_type(policy, request_kind).require()
            accepted = destination_descriptor.accepted_request_types
            if not accepted or request_kind not in accepted:
                raise PermissionError(
                    f"destination Agent does not accept request type: {request_kind}"
                )
            # Explicit managed attachment IDs must be resolvable before an
            # Agent mailbox message is committed.  Channel-only references
            # (for example encrypted CDN metadata without an attachment_id)
            # remain structured payload and are handled by the destination's
            # media policy instead of being mistaken for local files.
            await self._validate_agent_message_attachments(
                envelope_payload,
                source_agent_id=source,
                task_id=task_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            )
        except (KeyError, PermissionError) as exc:
            reason = str(exc)
            await self._audit_collaboration(
                allowed=False,
                reason=reason,
                actor=actor or external_user_id,
                source_agent_id=source,
                destination_agent_id=destination,
                request_type=request_kind,
                task_id=task_id,
            )
            raise PermissionError(reason) from None
        method = getattr(self.store, "create_agent_message", None) or getattr(
            self.store, "send_agent_message", None
        )
        if method is None:
            raise AttributeError("store must implement create_agent_message()")
        envelope_payload.setdefault("request_type", request_kind)
        execution_snapshot = await self._mailbox_execution_snapshot(
            destination,
            task_id=task_id,
            request_id=request_id,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        result = await _call_compatible(
            method,
            source_agent_id=source,
            destination_agent_id=destination,
            content=message,
            request_id=request_id,
            reply_to_id=reply_to_id,
            causation_id=causation_id,
            task_id=task_id,
            payload=envelope_payload,
            execution_snapshot=execution_snapshot,
            require_active_task=require_active_task,
            required_execution_id=required_execution_id,
        )
        await self._audit_collaboration(
            allowed=True,
            reason="",
            actor=actor or external_user_id,
            source_agent_id=source,
            destination_agent_id=destination,
            request_type=request_kind,
            task_id=task_id,
            payload={"request_id": _get(result, "request_id", request_id)},
        )
        return result

    async def process_mailbox_once(
        self,
        destination_agent_id: str,
        *,
        worker_id: str = "agent-mailbox",
        handler: Any | None = None,
    ) -> int:
        """Process one durable mailbox batch on the manager's loop."""
        from .worker import AgentMailboxWorker, _mailbox_result_content

        async def reply_mailbox(
            item: Any, result: Any, *, claim_token: str | None = None
        ) -> None:
            # Responses are delivered to the requesting Agent as mailbox input,
            # but they are not themselves requests for another automatic reply.
            # This also respects the one-row-per-destination/request invariant.
            if _get(item, "reply_to_id", None):
                return
            content = _mailbox_result_content(result)
            if not content:
                return
            mailbox_id = _get(item, "mailbox_id", _get(item, "message_id", ""))
            source_agent_id = _get(item, "destination_agent_id", destination_agent_id)
            await self.reply_agent_message(
                str(mailbox_id),
                content,
                source_agent_id=str(source_agent_id),
                original_mailbox_id=str(mailbox_id),
                original_claim_token=claim_token,
            )

        worker = AgentMailboxWorker(
            self.store,
            self.registry,
            destination_agent_id,
            worker_id=worker_id,
            handler=handler,
            reply_handler=reply_mailbox,
        )
        return await worker.run_once()

    process_agent_mailbox = process_mailbox_once

    async def ask(
        self,
        destination_agent_id: str,
        prompt: str,
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("request_type", "ask")
        return await self.send_agent_message(destination_agent_id, prompt, **kwargs)

    ask_agent = ask
    request_agent = ask

    async def reply_agent_message(
        self,
        mailbox_id: str,
        content: str,
        *,
        source_agent_id: str | None = None,
        actor: str = "",
        payload: Mapping[str, Any] | None = None,
        **scope: Any,
    ) -> Any:
        """Create a correlated response using the durable request envelope.

        A response is authority already granted by the original A-to-B
        request, not a new B-to-A request.  Consequently it does not consult
        B's ordinary outbound ACL or A's accepted-request list.  The durable
        original remains authoritative for every correlation and routing ID.
        """

        self._assert_loop()
        getter = getattr(self.store, "get_mailbox_item", None) or getattr(
            self.store, "get_agent_mailbox_item", None
        )
        if getter is None:
            raise AttributeError("store must implement get_mailbox_item()")
        original = await _call_compatible(getter, mailbox_id)
        if original is None:
            raise KeyError(f"mailbox message not found: {mailbox_id}")
        expected_responder = str(_get(original, "destination_agent_id", "") or "").strip()
        responder = str(source_agent_id or expected_responder).strip()
        destination = str(_get(original, "source_agent_id", "") or "").strip()
        request_id = str(_get(original, "request_id", "") or "").strip()
        reply_to_id = str(_get(original, "message_id", "") or "").strip()
        task_id = _get(original, "task_id", None)
        message = str(content or "").strip()
        original_payload = _get(original, "payload", {}) or {}
        request_type = str(
            original_payload.get("request_type", "ask")
            if isinstance(original_payload, Mapping)
            else "ask"
        )
        response_payload = dict(payload or {})
        response_payload.setdefault("request_type", request_type)
        audit_actor = actor or str(scope.get("external_user_id", "") or responder)

        reason = ""
        try:
            if not message:
                raise ValueError("reply content is required")
            if not expected_responder or responder != expected_responder:
                raise PermissionError("only the destination Agent may reply")
            if not destination or not request_id or not reply_to_id:
                raise PermissionError("original mailbox correlation is incomplete")
            await self._validate_agent_message_attachments(
                response_payload,
                source_agent_id=responder,
                task_id=task_id,
                channel=str(scope.get("channel", "") or ""),
                bot_id=str(scope.get("bot_id", "") or ""),
                external_user_id=str(scope.get("external_user_id", "") or ""),
                session_id=str(scope.get("session_id", "default") or "default"),
            )
        except (KeyError, PermissionError, ValueError) as exc:
            reason = str(exc)
            await self._audit_collaboration(
                allowed=False,
                reason=reason,
                actor=audit_actor,
                source_agent_id=responder,
                destination_agent_id=destination,
                request_type=request_type,
                task_id=task_id,
            )
            if isinstance(exc, ValueError):
                raise
            raise PermissionError(reason) from None

        create = getattr(self.store, "create_agent_message", None)
        if create is None:
            raise AttributeError("store must implement create_agent_message()")
        execution_snapshot = await self._mailbox_execution_snapshot(
            destination,
            task_id=task_id,
            request_id=request_id,
            channel=str(scope.get("channel", "") or ""),
            bot_id=str(scope.get("bot_id", "") or ""),
            external_user_id=str(scope.get("external_user_id", "") or ""),
            session_id=str(scope.get("session_id", "default") or "default"),
        )
        result = await _call_compatible(
            create,
            source_agent_id=responder,
            destination_agent_id=destination,
            content=message,
            request_id=request_id,
            reply_to_id=reply_to_id,
            causation_id=reply_to_id,
            task_id=task_id,
            payload=response_payload,
            message_id=scope.get("message_id"),
            execution_snapshot=execution_snapshot,
            original_mailbox_id=scope.get("original_mailbox_id"),
            original_claim_token=scope.get("original_claim_token"),
        )
        await self._audit_collaboration(
            allowed=True,
            reason="",
            actor=audit_actor,
            source_agent_id=responder,
            destination_agent_id=destination,
            request_type=request_type,
            task_id=task_id,
            payload={
                "request_id": request_id,
                "reply_to_id": reply_to_id,
                "mailbox_id": _get(result, "mailbox_id", None),
            },
        )
        return result

    async def submit_child_task(
        self,
        parent_task_id: str,
        inputs: Any,
        *,
        agent_id: str | None = None,
        request_type: str = "child_task",
        reply_target: ReplyTarget | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Authorize and atomically reserve a child slot before enqueueing."""
        self._assert_loop()
        parent = await self.get_task(parent_task_id)
        if parent is None:
            raise KeyError(f"parent task not found: {parent_task_id}")
        source = str(_get(parent, "agent_id", self.default_agent_id))
        destination = str(agent_id or source).strip()
        target = self._coerce_target(
            reply_target or _get(parent, "reply_target", None)
        )
        policy = await self._collaboration_policy(
            source,
            task_id=parent_task_id,
            channel=target.channel,
            bot_id=target.bot_id,
            external_user_id=target.external_user_id,
            session_id=target.session_id,
        )
        depth = int(_get(parent, "child_depth", 0)) + 1
        existing = 0
        counter = getattr(self.store, "child_count", None)
        if counter is not None:
            existing = int(await _call_compatible(counter, parent_task_id))
        try:
            await self._validate_agent_message_attachments(
                inputs,
                source_agent_id=source,
                task_id=parent_task_id,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=target.session_id,
            )
            self.policy_engine.check_child_task(
                policy,
                child_depth=depth,
                existing_children=existing,
            ).require()
            if destination != source:
                destination_descriptor = self.registry.descriptor(destination)
                if (
                    destination_descriptor is None
                    or not destination_descriptor.enabled
                ):
                    raise PermissionError(
                        f"unknown or disabled Agent: {destination}"
                    )
                self.policy_engine.authorize_collaboration(
                    policy,
                    peer_id=destination,
                    request_type=request_type,
                ).require()
                accepted = destination_descriptor.accepted_request_types
                if not accepted or request_type not in accepted:
                    raise PermissionError(
                        "destination Agent does not accept request type: "
                        f"{request_type}"
                    )
        except (KeyError, PermissionError, ValueError) as exc:
            await self._audit_collaboration(
                allowed=False,
                reason=str(exc),
                actor=source,
                source_agent_id=source,
                destination_agent_id=destination,
                request_type=request_type,
                task_id=parent_task_id,
                action="child_task",
            )
            if isinstance(exc, PermissionError):
                raise
            raise PermissionError(str(exc)) from None
        # Prefer the store-level atomic reserve+insert operation.  A separate
        # reservation is retained only for older stores; those callers still
        # receive compensating release on an enqueue failure.
        atomic_child = getattr(self.store, "create_child_task", None)
        reserve = None if atomic_child is not None else getattr(self.store, "reserve_child_task", None)
        if reserve is not None:
            accepted = await _call_compatible(
                reserve,
                parent_task_id,
                max_children=policy.max_children_per_task,
            )
            if not accepted:
                raise PermissionError("maximum child-task count exceeded")
        child_metadata = kwargs.pop("metadata", None)
        if child_metadata:
            child_metadata = {**dict(child_metadata), "request_type": request_type}
        else:
            child_metadata = {"request_type": request_type}
        child_workspace: Mapping[str, Any] | None = None
        if destination == source:
            parent_metadata = _get(parent, "metadata", {}) or {}
            if isinstance(parent_metadata, Mapping) and isinstance(
                parent_metadata.get(EXECUTION_WORKSPACE_KEY),
                Mapping,
            ):
                child_workspace = parent_metadata[EXECUTION_WORKSPACE_KEY]
        try:
            return await self.submit(
                inputs,
                reply_target or _get(parent, "reply_target", None),
                agent_id=destination,
                parent_task_id=parent_task_id,
                child_depth=depth,
                request_id=kwargs.pop("request_id", None),
                metadata=child_metadata,
                _child_parent_id=parent_task_id,
                _child_max_children=policy.max_children_per_task,
                _workspace_snapshot_override=child_workspace,
                **kwargs,
            )
        except Exception:
            release = getattr(self.store, "release_child_task", None)
            if reserve is not None and release is not None:
                try:
                    await _call_compatible(release, parent_task_id)
                except Exception:
                    logger.debug("could not release reserved child slot", exc_info=True)
            raise

    create_child_task = submit_child_task

    # ------------------------------------------------------------------ audio confirmation
    async def create_transcription_candidate(self, **kwargs: Any) -> Any:
        self._assert_loop()
        method = getattr(self.store, "create_transcription_candidate", None)
        if method is None:
            raise AttributeError("store must implement create_transcription_candidate()")
        return await _call_compatible(method, **kwargs)

    create_audio_candidate = create_transcription_candidate

    async def _candidate_for_scope(
        self,
        confirmation_id: str,
        *,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> Any:
        getter = getattr(self.store, "get_transcription_candidate", None)
        if getter is None:
            raise AttributeError("store must implement get_transcription_candidate()")
        candidate = await _call_compatible(getter, confirmation_id)
        if candidate is None:
            raise KeyError(f"unknown confirmation: {confirmation_id}")
        def field(name: str, default: Any = "") -> Any:
            return candidate.get(name, default) if isinstance(candidate, Mapping) else getattr(candidate, name, default)
        # A confirmation is owned by the original channel/session identity;
        # switching the front Agent or presenting another inbox cannot redirect
        # it to a different user.
        expected = (field("channel"), field("bot_id"), field("external_user_id"), field("session_id", "default"))
        actual = (channel, bot_id, external_user_id, session_id or "default")
        # Route arguments are an authorization boundary, not optional display
        # hints.  A command routed through WeChat always supplies all three
        # channel/user fields; accepting an omitted value here would let a
        # caller probe or consume another user's confirmation by ID alone.
        if not all(str(expected[index] or "") for index in range(3)):
            raise PermissionError("confirmation has incomplete ownership scope")
        if any(
            not str(actual[index] or "")
            or str(actual[index]) != str(expected[index])
            for index in range(3)
        ) or str(actual[3] or "default") != str(expected[3] or "default"):
            raise PermissionError("confirmation does not belong to this session")
        return candidate

    async def confirm_transcription(
        self,
        confirmation_id: str,
        *,
        actor: str = "",
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        **_: Any,
    ) -> Any:
        self._assert_loop()
        session_id = session_id or "default"
        candidate = await self._candidate_for_scope(
            confirmation_id,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        def field(name: str, default: Any = "") -> Any:
            return candidate.get(name, default) if isinstance(candidate, Mapping) else getattr(candidate, name, default)
        agent = str(field("agent_id", self.default_agent_id) or self.default_agent_id)
        candidate_inbound_id = field("inbound_message_id", None)
        original_inbound = None
        inbound_getter = (
            getattr(self.store, "get_inbound", None)
            or getattr(self.store, "get_inbound_message", None)
        )
        if candidate_inbound_id and inbound_getter is not None:
            original_inbound = await _call_compatible(
                inbound_getter, str(candidate_inbound_id)
            )
        target = ReplyTarget(
            channel=str(field("channel", channel)),
            bot_id=str(field("bot_id", bot_id)),
            external_user_id=str(field("external_user_id", external_user_id)),
            session_id=str(field("session_id", session_id) or "default"),
            source_message_id=str(
                _get(original_inbound, "external_message_id", "") or ""
            ),
            source_sequence=_get(original_inbound, "source_sequence", None),
            context_token=_get(original_inbound, "context_token", None),
        )
        default_mode, default_version = self._default_mode_selection(agent)
        mode_id = str(field("mode_id", default_mode) or default_mode)
        try:
            version = int(field("policy_version", default_version) or default_version)
        except (TypeError, ValueError):
            version = int(default_version)
        default_profile_version = self._default_profile_version(agent)
        try:
            profile_version = int(
                field("profile_version", default_profile_version)
                or default_profile_version
            )
        except (TypeError, ValueError):
            profile_version = int(default_profile_version)
        stored_metadata = field("metadata", None)
        if isinstance(stored_metadata, Mapping):
            metadata = dict(stored_metadata)
        else:
            # Compatibility for pre-snapshot candidates. New candidates always
            # carry the immutable policy selected under the ingress route lock.
            metadata = self._policy_snapshot(
                agent,
                mode_id,
                profile_version,
                version,
                actor=actor or external_user_id,
                explicit=(
                    mode_id != "execute"
                    or self._execute_authorized(target, agent, version)
                ),
            )
        task_values = {
            "agent_id": agent,
            "mode_id": mode_id,
            "profile_version": profile_version,
            "policy_version": version,
            "reply_target": target,
            "metadata": metadata,
        }
        creator = getattr(self.store, "confirm_transcription_task", None) or getattr(
            self.store, "confirm_candidate_task", None
        )
        if creator is not None:
            task = await _call_compatible(
                creator,
                confirmation_id,
                task=task_values,
                actor=actor or external_user_id,
            )
            self.dispatcher.wake()
            return task
        resolver = getattr(self.store, "resolve_transcription", None)
        consumer = getattr(self.store, "consume_transcription", None)
        if resolver is None or consumer is None:
            raise AttributeError("store lacks transcription confirmation operations")
        candidate_status = str(field("status", "pending") or "pending")
        if candidate_status == "pending":
            if not await _call_compatible(
                resolver,
                confirmation_id,
                status="confirmed",
                actor=actor or external_user_id,
            ):
                raise ValueError("confirmation is no longer pending")
        elif candidate_status != "confirmed":
            raise ValueError(f"confirmation is {candidate_status}")
        # Compatibility stores do not have the atomic confirmation projector,
        # but they still must receive the same immutable input snapshot.  The
        # candidate owns the transcript/attachment; the linked inbound owns
        # the original caption and normalized media context.
        confirmed_inputs: dict[str, Any] = {
            "text": field("candidate_text", ""),
            "audio_confirmation_id": confirmation_id,
            "attachment_id": field("attachment_id", None),
            "source": field("source", ""),
        }
        inbound_id = candidate_inbound_id
        if inbound_id:
            if inbound_getter is None:
                raise AttributeError(
                    "store cannot restore confirmed transcription input"
                )
            if original_inbound is None:
                original_inbound = await _call_compatible(
                    inbound_getter, str(inbound_id)
                )
            if original_inbound is None:
                raise ValueError(
                    "confirmed transcription inbound message is unavailable"
                )
            caption = str(_get(original_inbound, "text", "") or "")
            if caption:
                confirmed_inputs["caption"] = caption
            payload = _get(original_inbound, "payload", {}) or {}
            if isinstance(payload, Mapping) and payload.get("media"):
                confirmed_inputs["media"] = list(
                    canonical_media_inputs(
                        payload["media"], include_candidate=True
                    )
                )
        # Compatibility stores lack the atomic confirmation projector. Reuse
        # the persisted continuation snapshot only through the private submit
        # path while holding the same scope lock as ordinary task creation.
        # The public ``submit()`` API must never accept a caller-provided policy
        # snapshot because that would bypass execute-mode authorization.  A
        # legacy candidate with no role field predates session roles and is
        # therefore pinned to the canonical implicit default; absence here
        # must not mean "resolve the user's current role".
        continuation_role = (
            metadata["session_role"]
            if isinstance(metadata, Mapping) and "session_role" in metadata
            else implicit_default_role()
        )
        async with self._scope_lock(
            channel=target.channel,
            bot_id=target.bot_id,
            external_user_id=target.external_user_id,
            session_id=target.session_id,
        ):
            task = await self._submit_impl(
                confirmed_inputs,
                target,
                agent_id=agent,
                mode_id=mode_id,
                profile_version=profile_version,
                policy_version=version,
                metadata=metadata,
                _policy_snapshot_override=(
                    metadata if isinstance(metadata, Mapping) else None
                ),
                _role_snapshot_override=continuation_role,
                inbound_message_id=(str(inbound_id) if inbound_id else None),
                dedupe_key=f"confirmation:{confirmation_id}",
            )
        if not await _call_compatible(consumer, confirmation_id, _get(task, "task_id", "")):
            raise ValueError("confirmation could not be consumed")
        self.dispatcher.wake()
        return task

    confirm_audio = confirm_transcription
    confirm_candidate = confirm_transcription

    async def reject_transcription(
        self,
        confirmation_id: str,
        *,
        actor: str = "",
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        **_: Any,
    ) -> bool:
        self._assert_loop()
        await self._candidate_for_scope(
            confirmation_id,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        resolver = getattr(self.store, "resolve_transcription", None)
        if resolver is None:
            raise AttributeError("store must implement resolve_transcription()")
        return bool(
            await _call_compatible(
                resolver,
                confirmation_id,
                status="rejected",
                actor=actor or external_user_id,
            )
        )

    reject_audio = reject_transcription
    reject_candidate = reject_transcription

    # ------------------------------------------------------------------ helpers
    def _policy_snapshot(
        self,
        agent_id: str,
        mode_id: str,
        profile_version: int,
        policy_version: int,
        *,
        actor: str = "",
        explicit: bool = False,
    ) -> dict[str, Any]:
        """Resolve and freeze policy values on a newly-created task.

        Profiles and modes are immutable versions.  The snapshot is stored as
        JSON metadata so a restart cannot silently apply a newer policy to a
        queued task.  A mode selected through the persisted session route is
        already explicitly authorized; direct promotion to ``execute`` still
        requires ``explicit=True``.
        """

        mode_id = str(mode_id).strip().lower()
        mode = self.mode_registry.get(mode_id, policy_version)
        if mode is None:
            # A task carries a concrete policy version.  Falling back to the
            # latest mode here would silently change permissions for queued
            # work after a policy upgrade, defeating the immutable snapshot
            # invariant.
            raise KeyError(f"unknown Agent mode policy: {mode_id}@{policy_version}")
        if mode.mode_id == "execute":
            trusted_default = self._trusted_default_execute_for(
                agent_id,
                policy_version=policy_version,
            )
            decision = self.policy_engine.authorize_mode(
                mode,
                # Startup authorization has no WeChat actor.  Use a stable
                # administrator marker only for the exact trusted default
                # snapshot; direct/user promotions still require their real
                # actor and explicit flag.
                actor=actor or ("trusted-startup" if trusted_default else ""),
                explicit=explicit or trusted_default,
            )
            decision.require()
        profile = None
        getter = getattr(self.registry, "profile", None)
        if getter is not None:
            try:
                profile = getter(agent_id, profile_version)
            except TypeError:
                # Lightweight registry fakes may only expose ``profile(id)``;
                # still verify the returned version instead of silently
                # applying a newer profile to an older task snapshot.
                profile = getter(agent_id)
            if profile is None:
                # A registered Agent with no profile is a valid compatibility
                # case and receives an empty policy snapshot.  If a profile
                # exists but the requested immutable version does not, fail
                # closed rather than falling back to the latest version.
                try:
                    current = getter(agent_id)
                except TypeError:
                    current = None
                if current is not None:
                    raise KeyError(
                        f"unknown Agent profile: {agent_id}@{profile_version}"
                    )
            else:
                actual_version = _get(
                    profile,
                    "profile_version",
                    _get(profile, "version", None),
                )
                if actual_version is not None and str(actual_version) != str(profile_version):
                    raise KeyError(
                        f"unknown Agent profile: {agent_id}@{profile_version}"
                    )
        mode_snapshot = mode.as_dict() if hasattr(mode, "as_dict") else {
            "mode_id": _get(mode, "mode_id", mode_id),
            "developer_instructions": _get(mode, "developer_instructions", ""),
            "sandbox_policy": _get(mode, "sandbox_policy", "read-only"),
            "approval_policy": _get(mode, "approval_policy", "deny_all"),
            "allowed_tools": sorted(_get(mode, "allowed_tools", ()) or ()),
            "denied_tools": sorted(_get(mode, "denied_tools", ()) or ()),
            "can_write_files": bool(_get(mode, "can_write_files", False)),
            "can_execute_commands": bool(_get(mode, "can_execute_commands", False)),
            "can_create_child_tasks": bool(_get(mode, "can_create_child_tasks", False)),
            "can_send_agent_messages": bool(_get(mode, "can_send_agent_messages", False)),
            "policy_version": int(_get(mode, "policy_version", policy_version)),
        }
        if profile is None:
            if mode.mode_id == "execute":
                raise PermissionError(
                    "execute mode requires an Agent profile with write and command permissions"
                )
            return {"mode": mode_snapshot}
        effective = self.policy_engine.effective_policy(profile, mode)
        if mode.mode_id == "execute" and (
            effective.sandbox_policy
            != _execute_sandbox_for_version(mode.policy_version)
            or not effective.can_write_files
            or not effective.can_execute_commands
        ):
            # Keep every ingress path fail-closed, including internal task
            # creation and child-task submission that can carry an explicit
            # mode.  CodexRuntime enforces the sandbox string, so allowing a
            # writable mode when the profile denies command execution would
            # otherwise turn a descriptive denial into a live SDK capability.
            raise PermissionError(
                "execute mode requires workspace writes and command execution"
            )
        snapshot = {
            "effective_policy": effective.as_dict(),
            "profile": profile.as_dict(),
            # Keep the raw mode definition as well as the narrowed effective
            # policy.  The latter intentionally omits some descriptive mode
            # fields (and may have profile restrictions applied), while a
            # queued task must retain the exact developer instructions and
            # SDK mapping after a process restart.
            "mode": mode_snapshot,
        }
        if self._trusted_default_execute_for(
            agent_id,
            policy_version=policy_version,
        ) and mode.mode_id == "execute":
            # Keep non-user authorization provenance beside the immutable
            # policy. This is audit metadata; EffectivePolicy remains the
            # enforcement source.
            snapshot["mode_authorization"] = {
                "source": "trusted_startup",
                "actor": "trusted-startup",
            }
        return snapshot

    async def _store_inbound(self, inbound: Any) -> Any:
        method = getattr(self.store, "store_inbound", None) or getattr(self.store, "record_inbound", None)
        if method is None:
            raise AttributeError("store must implement accept_inbound() or store_inbound()")
        return await _call_compatible(method, inbound)

    async def _replay_existing_inbound_task(
        self,
        inbound: Any,
        *,
        create_task: bool,
        cursor: str | None,
        channel_cursor: str | None,
        trusted_media_wire_fingerprints: Sequence[str] | None,
    ) -> Any | None:
        """Return an immutable duplicate before live route/skill resolution.

        A redelivery must retain the first task's skill and policy snapshot even
        when the current Agent catalog has rolled forward or disappeared.  The
        lookup is deliberately keyed by the complete durable channel identity
        and verifies the owning user/session and linked task before replaying
        through SQLite's idempotent ingress operation.  Messages without an
        existing task continue through normal first-delivery validation.
        """

        getter = getattr(self.store, "get_inbound", None) or getattr(
            self.store, "get_inbound_message", None
        )
        acceptor = getattr(self.store, "accept_inbound", None) or getattr(
            self.store, "ingest_inbound", None
        )
        if getter is None or acceptor is None:
            return None
        target = self._reply_target(inbound)
        external_message_id = str(
            _get(inbound, "external_message_id", "") or ""
        ).strip()
        if not external_message_id or not target.channel or not target.bot_id:
            return None
        try:
            existing = await _call_compatible(
                getter,
                target.channel,
                target.bot_id,
                external_message_id,
            )
        except (AttributeError, TypeError, KeyError, ValueError):
            # Narrow compatibility stores may expose only an internal-ID
            # lookup.  Falling through preserves their original ingress path.
            return None
        if existing is None:
            return None

        def same_identity(name: str, incoming: Any, default: Any = "") -> bool:
            stored = _get(existing, name, default)
            if name == "session_id":
                stored = stored or "default"
                incoming = incoming or "default"
            return str(stored or default) == str(incoming or default)

        if not same_identity("channel", target.channel):
            return None
        if not same_identity("bot_id", target.bot_id):
            return None
        if not same_identity("external_message_id", external_message_id):
            return None
        if not same_identity("external_user_id", target.external_user_id):
            return None
        if not same_identity("session_id", target.session_id or "default", "default"):
            return None

        task_id = str(_get(existing, "task_id", "") or "").strip()
        if not task_id:
            return None
        task_getter = getattr(self.store, "get_task", None)
        existing_task = None
        if task_getter is not None:
            try:
                existing_task = await _call_compatible(task_getter, task_id)
            except (AttributeError, TypeError, KeyError, ValueError):
                return None
            if existing_task is None:
                return None
            linked_inbound = str(
                _get(existing_task, "inbound_message_id", "") or ""
            ).strip()
            stored_message_id = str(_get(existing, "message_id", "") or "").strip()
            if linked_inbound and stored_message_id and linked_inbound != stored_message_id:
                return None
            task_target = self._coerce_target(_get(existing_task, "reply_target", None))
            if task_target.external_user_id and task_target.external_user_id != target.external_user_id:
                return None
            if task_target.channel and task_target.channel != target.channel:
                return None
            if task_target.bot_id and task_target.bot_id != target.bot_id:
                return None
            if (task_target.session_id or "default") != (target.session_id or "default"):
                return None

        # Do not pass a live task/skill snapshot: SQLite's duplicate branch
        # returns the already persisted row, while its replay validator still
        # checks the immutable inbound envelope and media fingerprints.
        try:
            result = await _call_compatible(
                acceptor,
                inbound,
                task=None,
                create_task=create_task,
                cursor=cursor,
                channel_cursor=channel_cursor,
                _trusted_media_wire_fingerprints=trusted_media_wire_fingerprints,
            )
        except (AttributeError, TypeError, KeyError, ValueError):
            return None
        replay_task = _get(result, "task", None)
        replay_task_id = str(_get(replay_task, "task_id", "") or "").strip()
        if replay_task is not None and replay_task_id and replay_task_id != task_id:
            raise PermissionError("inbound replay returned a foreign task")
        replay_inbound = _get(result, "inbound", result)
        if replay_inbound is not None:
            replay_user = str(_get(replay_inbound, "external_user_id", "") or "")
            replay_session = str(_get(replay_inbound, "session_id", "") or "default")
            if replay_user and replay_user != target.external_user_id:
                raise PermissionError("inbound replay returned a foreign user")
            if replay_session != (target.session_id or "default"):
                raise PermissionError("inbound replay returned a foreign session")
        self.dispatcher.wake()
        return result

    @staticmethod
    def _text_from_inbound(inbound: Any) -> str:
        return str(_get(inbound, "text", _get(inbound, "content", _get(inbound, "body", ""))) or "")

    @staticmethod
    def _inputs_from_inbound(inbound: Any) -> Mapping[str, Any]:
        payload = _get(inbound, "payload", {}) or {}
        # ``InboundEnvelope.payload`` may contain the full wire diagnostic
        # object for audit purposes.  Do not hand sender IDs, encrypted CDN
        # fields, or unrelated protocol data to the Agent prompt; retain only
        # normalized media references.  Apply the same allowlist to
        # store-layer ``InboundMessage`` values: their payload can come from a
        # channel adapter or replay and is not itself an Agent-input contract.
        raw = _get(inbound, "raw", None)
        source = raw if isinstance(raw, Mapping) else payload
        projected: dict[str, Any] = {}
        if isinstance(source, Mapping):
            for key in ("media", "attachments", "images"):
                if key in source:
                    projected[key] = list(
                        canonical_media_inputs(source.get(key))
                    )
        result = {"text": TaskManager._text_from_inbound(inbound)}
        result.update(projected)
        return result

    @staticmethod
    def _coerce_inbound_inputs(value: Any) -> dict[str, Any]:
        """Copy channel-projected inputs without accepting mutable aliases."""

        if isinstance(value, Mapping):
            return dict(value)
        if value is None:
            return {"text": ""}
        if isinstance(value, str):
            return {"text": value}
        return {"text": str(value)}

    @staticmethod
    def _trusted_skill_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
        """Validate and freeze the trusted skill reference used by a task.

        Skill instructions are deliberately absent from this structure.  The
        SDK resolves the registered path when it translates the immutable
        ``SkillInput``; a channel caller can provide a name/description but
        cannot inject a developer prompt or capability grant.
        """

        if not isinstance(value, Mapping):
            raise PermissionError("skill snapshot is invalid")
        data = dict(value)
        name = str(
            data.get("name")
            or data.get("skill_id")
            or data.get("skillId")
            or data.get("id")
            or ""
        ).strip()
        path = str(data.get("path") or data.get("skill_path") or "").strip()
        if not name or not path:
            raise PermissionError("skill snapshot is incomplete")
        try:
            definition = normalize_skill(
                {
                    "name": name,
                    "path": path,
                    "description": data.get("description", ""),
                    "display_name": data.get("display_name", data.get("displayName", "")),
                    "version": data.get("version", data.get("skill_version", "")),
                    "content_hash": (
                        data.get("content_hash")
                        or data.get("contentHash")
                        or data.get("skill_hash")
                        or data.get("hash")
                        or ""
                    ),
                    "enabled": data.get("enabled", True),
                }
            )
        except (TypeError, ValueError) as exc:
            raise PermissionError("skill snapshot is invalid") from exc
        if not definition.enabled:
            raise PermissionError("skill is disabled")
        snapshot = definition.snapshot()
        supplied_id = str(data.get("skill_id") or "").strip().casefold()
        if supplied_id and supplied_id != snapshot["skill_id"]:
            raise PermissionError("skill snapshot identity does not match its name")
        return snapshot

    async def _resolve_trusted_skill_snapshot(
        self,
        value: Mapping[str, Any],
        *,
        agent_id: str,
        cwd: str | None = None,
    ) -> dict[str, Any]:
        """Validate a skill reference against the registered Agent catalog.

        The channel gateway resolves a selector before ingress, but the
        manager is also a public task boundary used by confirmations and
        integrations.  Re-resolving by canonical name here prevents those
        callers from replacing the trusted path, version, or content hash in
        an otherwise well-shaped mapping.  A runtime without a catalog fails
        closed rather than treating a caller-provided filesystem path as a
        skill definition.
        """

        snapshot = self._trusted_skill_snapshot(value)
        supplied_hash = ""
        if isinstance(value, Mapping):
            supplied_hash = str(
                value.get("content_hash")
                or value.get("contentHash")
                or value.get("skill_hash")
                or value.get("hash")
                or ""
            ).strip()
        runtime = self.registry.require(agent_id)
        candidates: Any = None

        # Prefer the Agent runtime, but allow a registry-owned catalog for
        # deployments that centralize skill discovery separately from SDK
        # execution.
        for target in (runtime, self.registry):
            resolver = getattr(target, "resolve_skill", None)
            if resolver is None:
                continue
            candidates = await _call_compatible(
                resolver,
                snapshot["name"],
                agent_id=agent_id,
                refresh=False,
                cwd=cwd,
            )
            if candidates is not None:
                break

        if candidates is None:
            for target in (runtime, self.registry):
                lister = getattr(target, "list_skills", None)
                if lister is None:
                    continue
                candidates = await _call_compatible(
                    lister,
                    agent_id=agent_id,
                    refresh=False,
                    cwd=cwd,
                )
                if candidates is not None:
                    break

        if isinstance(candidates, Mapping):
            # Runtime adapters commonly return either one descriptor or an
            # envelope containing ``skills``/``data``.
            if candidates.get("path") or candidates.get("name") or candidates.get("skill_id"):
                catalog_values: Any = (candidates,)
            else:
                catalog_values = candidates.get(
                    "skills",
                    candidates.get("data", ()),
                )
        else:
            catalog_values = candidates

        definition = find_skill(
            catalog_values,
            snapshot["name"],
            rehash_local_bundles=True,
        )
        if definition is None:
            raise PermissionError(f"skill is not registered: {snapshot['name']}")
        trusted = definition.snapshot()
        for field in ("skill_id", "path", "version", "content_hash", "enabled"):
            if field == "content_hash" and not supplied_hash:
                # The catalog is authoritative when a compatibility caller
                # omitted the hash; do not reject a valid name/path solely
                # because the manager had to fill this immutable field.
                continue
            supplied = snapshot.get(field)
            expected = trusted.get(field)
            if field == "enabled":
                if bool(supplied) is not bool(expected):
                    raise PermissionError("skill snapshot enabled state does not match")
            elif str(supplied or "") != str(expected or ""):
                raise PermissionError(f"skill snapshot {field} does not match registry")
        return trusted

    @staticmethod
    def _coerce_target(value: Any) -> ReplyTarget:
        if isinstance(value, ReplyTarget):
            return value
        if value is None:
            return ReplyTarget()
        if isinstance(value, Mapping):
            # Channel envelopes commonly expose ``external_message_id`` or
            # ``message_id`` instead of the canonical reply-target spelling.
            # Preserve that identity so a task can always be delivered after
            # a restart, even when the caller supplied a plain mapping.
            fields = {name: value.get(name) for name in ReplyTarget.__dataclass_fields__}
            if not fields.get("source_message_id"):
                fields["source_message_id"] = value.get(
                    "external_message_id", value.get("message_id", "")
                )
            return ReplyTarget(**fields)
        if hasattr(value, "target"):
            target = value.target()
            if target is not value:
                return TaskManager._coerce_target(target)
        return ReplyTarget(
            channel=str(_get(value, "channel", "") or ""),
            bot_id=str(_get(value, "bot_id", "") or ""),
            external_user_id=str(_get(value, "external_user_id", _get(value, "user_id", "")) or ""),
            session_id=str(_get(value, "session_id", "default") or "default"),
            source_message_id=_get(value, "source_message_id", _get(value, "message_id", None)),
            source_sequence=_get(value, "source_sequence", None),
            context_token=_get(value, "context_token", None),
        )

    @staticmethod
    def _reply_target(inbound: Any) -> ReplyTarget:
        target = _get(inbound, "reply_target", None)
        if target is not None:
            return TaskManager._coerce_target(target)
        return TaskManager._coerce_target(inbound)

    @staticmethod
    def _conversation_id(target: ReplyTarget, agent_id: str) -> str:
        return conversation_id(
            target.channel,
            target.bot_id,
            target.external_user_id,
            target.session_id,
            agent_id,
        )

    def submit_threadsafe(self, coroutine: Any, *, timeout: float | None = None) -> Any:
        """Submit a manager coroutine from a gateway thread.

        The caller must have started the manager; no runtime dictionaries are
        exposed to the calling thread.
        """

        if self._owner_loop is None or not self._owner_loop.is_running():
            # A coroutine object is otherwise left un-awaited when the loop
            # has already stopped, producing a warning and potentially
            # retaining captured runtime state.
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            raise RuntimeError("TaskManager event loop is not running")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._owner_loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError:
            future.cancel()
            raise
        except BaseException:
            if not future.done():
                future.cancel()
            raise

    @property
    def started(self) -> bool:
        return self._started


Manager = TaskManager

__all__ = ["Manager", "TaskManager"]
