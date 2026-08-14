"""Codex SDK implementation of the application ``AgentRuntime`` contract.

No other domain module imports ``openai_codex``.  This adapter translates the
string policies and immutable task snapshots into SDK values, owns SDK thread
bindings, and turns the SDK notification stream into ordered ``AgentEvent``
records.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import shlex
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Mapping, Sequence

from .base import (
    AgentEvent,
    AgentResult,
    AgentTask,
    EmitCallback,
    EventPriority,
    EventVisibility,
    emit_if_awaitable,
)
from src.runtime.media import canonical_media_input, sniff_mime

try:  # Keep importing the domain contracts possible without the optional SDK.
    from openai_codex import (
        ApprovalMode,
        AsyncCodex,
        AsyncThread,
        LocalImageInput,
        SkillInput,
        Sandbox,
        TextInput,
    )
except ImportError:  # pragma: no cover - only used in minimal installations
    ApprovalMode = AsyncCodex = AsyncThread = LocalImageInput = SkillInput = Sandbox = TextInput = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


def default_workspace() -> str:
    """Return and create the canonical durable Codex workspace."""

    path = Path.home() / ".codex-on-wechat" / "workspace"
    path.mkdir(parents=True, exist_ok=True)
    return str(path.resolve())


_default_workspace = default_workspace


@dataclass(frozen=True, slots=True)
class ThreadBinding:
    """A policy-specific SDK thread binding."""

    conversation_id: str
    mode_id: str
    profile_version: int | str
    policy_version: int | str
    thread_id: str
    thread: Any

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self.conversation_id,
            self.mode_id,
            str(self.profile_version),
            str(self.policy_version),
        )


def sandbox_for_policy(policy: str | Any) -> Any:
    """Translate an application sandbox string to the SDK enum.

    Unknown values fail closed to read-only.  Returning the input when the SDK
    is unavailable keeps this helper useful with a fake Codex client.
    """

    value = getattr(policy, "value", policy)
    value = str(value or "read-only").lower().replace("_", "-")
    aliases = {
        "read-only": "read-only",
        "readonly": "read-only",
        "workspace-write": "workspace-write",
        "workspacewrite": "workspace-write",
        "full-access": "full-access",
        "fullaccess": "full-access",
    }
    # Unknown application values must never become a permissive SDK setting.
    # Keep the canonical string when the optional SDK is unavailable so fake
    # clients observe the same fail-closed behavior.
    value = aliases.get(value, "read-only")
    if Sandbox is None:
        return value
    mapping = {
        "read-only": getattr(Sandbox, "read_only", None),
        "workspace-write": getattr(Sandbox, "workspace_write", None),
        "full-access": getattr(Sandbox, "full_access", None),
    }
    return mapping.get(value) or mapping.get("read-only") or "read-only"


def approval_for_policy(policy: str | Any) -> Any:
    """Translate an application approval policy to the SDK enum (fail closed)."""

    value = getattr(policy, "value", policy)
    value = str(value or "deny_all").lower().replace("-", "_")
    aliases = {
        "deny_all": "deny_all",
        "auto_review": "auto_review",
    }
    value = aliases.get(value, "deny_all")
    if ApprovalMode is None:
        return value
    mapping = {
        "deny_all": getattr(ApprovalMode, "deny_all", None),
        "auto_review": getattr(ApprovalMode, "auto_review", None),
    }
    return mapping.get(value) or mapping.get("deny_all") or "deny_all"


# Descriptive aliases used by compatibility tests and adapters.
translate_sandbox = sandbox_for_policy
translate_approval = approval_for_policy


class CodexRuntime:
    """Run immutable :class:`AgentTask` snapshots on Codex SDK threads."""

    agent_id = "codex"

    def __init__(
        self,
        *,
        model: str = "",
        cwd: str | None = None,
        turn_timeout: float | None = 60,
        codex: Any | None = None,
        codex_factory: Callable[[], Any] | None = None,
        mode_resolver: Callable[[str], Any] | Mapping[str, Any] | None = None,
        profile_prompt: str = "",
        managed_root: str | Path | None = None,
        profile_resolver: Callable[[str, int | str], Any] | Mapping[Any, Any] | None = None,
        skill_root: str | Path | None = None,
        trusted_skill_root: str | Path | None = None,
        trusted_skill_roots: Sequence[str | Path] | None = None,
        image_output_publisher: Callable[..., Any] | None = None,
        agent_bridge_command: Sequence[str] | str | None = None,
        agent_bridge_capability_issuer: Callable[[AgentTask], str] | None = None,
    ) -> None:
        self.model = model
        self.cwd = cwd or default_workspace()
        self.turn_timeout = turn_timeout
        if codex is not None and codex_factory is not None:
            raise ValueError("pass codex or codex_factory, not both")
        self._codex = codex
        self._codex_factory = codex_factory or (lambda: AsyncCodex()) if AsyncCodex is not None else codex_factory
        self._owns_codex = codex is None
        self._codex_context: Any | None = None
        self._codex_entered = False
        self._started = False
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._bindings: dict[tuple[str, str, str, str], ThreadBinding] = {}
        self._threads_by_id: dict[str, Any] = {}
        self._active_turns: dict[str, Any] = {}
        self._active_tasks: dict[str, AgentTask] = {}
        self._active_messages: dict[str, str] = {}
        self._interrupt_requested: set[str] = set()
        # Interactive ``chat_stream`` consumers opt into delta callbacks. A
        # normal durable task still receives only stable message events.
        self._streaming_task_ids: set[str] = set()
        # Thread bindings are policy-specific, but execution serialization is
        # scoped to the Agent conversation.  Different modes therefore use
        # different SDK threads without running those threads concurrently for
        # the same logical conversation.
        self._run_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._conversation_models: dict[str, str] = {}
        self._conversation_reasoning_efforts: dict[str, str] = {}
        self._skills_cache: list[dict[str, Any]] | None = None
        if mode_resolver is None:
            # Import application modes lazily to avoid making the Agent
            # contract depend on the runtime package during module loading.
            try:
                from src.runtime.modes import ModeRegistry

                mode_resolver = ModeRegistry()
            except Exception:
                mode_resolver = None
        self._mode_resolver = mode_resolver
        self.profile_prompt = profile_prompt
        self.managed_root = Path(managed_root).expanduser().resolve() if managed_root else None
        roots = list(trusted_skill_roots or ())
        if skill_root is not None:
            roots.append(skill_root)
        if trusted_skill_root is not None:
            roots.append(trusted_skill_root)
        self.trusted_skill_roots = tuple(
            Path(root).expanduser().resolve() for root in roots if str(root).strip()
        )
        self._profile_resolver = profile_resolver
        # The SDK supplies generated-image bytes/path metadata, but durable
        # publication belongs to the embedding because it owns the attachment
        # store and SQLite ACL.  The callback returns one managed attachment ID.
        self._image_output_publisher = image_output_publisher
        if isinstance(agent_bridge_command, str):
            self._agent_bridge_command = agent_bridge_command.strip()
        elif agent_bridge_command:
            self._agent_bridge_command = shlex.join(
                str(value) for value in agent_bridge_command
            )
        else:
            self._agent_bridge_command = ""
        self._agent_bridge_capability_issuer = agent_bridge_capability_issuer

    # ------------------------------------------------------------------
    # Lifecycle and state ownership
    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError("CodexRuntime state must be accessed from its owning asyncio loop")
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("CodexRuntime state must be accessed from its owning asyncio loop")

    async def start(self) -> None:
        self._assert_loop()
        async with self._lifecycle_lock:
            if self._started:
                return
            context: Any | None = None
            try:
                if self._codex is None:
                    if self._codex_factory is None:
                        raise RuntimeError("openai-codex is not installed")
                    created = self._codex_factory()
                    self._codex = await created if inspect.isawaitable(created) else created
                    if self._codex is None:
                        raise RuntimeError("Codex client factory returned no client")
                context = self._codex
                enter = getattr(context, "__aenter__", None)
                if enter is not None and self._owns_codex:
                    result = enter()
                    entered = await result if inspect.isawaitable(result) else result
                    self._codex_context = context
                    self._codex_entered = True
                    if entered is not None:
                        self._codex = entered
                self._started = True
            except BaseException as exc:
                # A context manager can allocate a transport before __aenter__
                # raises.  Best-effort exit here keeps registry/manager startup
                # rollback from leaking that transport; cleanup errors must not
                # mask the original startup failure.
                leave = (
                    getattr(context, "__aexit__", None)
                    if context is not None and self._owns_codex
                    else None
                )
                if leave is not None:
                    try:
                        result = leave(type(exc), exc, exc.__traceback__)
                        if inspect.isawaitable(result):
                            await result
                    except BaseException:
                        logger.debug(
                            "failed to close Codex context after startup error",
                            exc_info=True,
                        )
                self._codex_context = None
                self._codex_entered = False
                self._started = False
                if self._owns_codex:
                    self._codex = None
                raise
            logger.info("initialized Codex SDK runtime")

    async def stop(self) -> None:
        self._assert_loop()
        async with self._lifecycle_lock:
            if not self._started and not self._codex_entered:
                return
            errors: list[BaseException] = []
            try:
                # Interrupt active turns before closing the SDK transport.
                for task_id in tuple(self._active_turns):
                    try:
                        await self.interrupt(task_id)
                    except BaseException as exc:
                        errors.append(exc)
                        logger.debug(
                            "failed to interrupt task during shutdown", exc_info=True
                        )
                if self._codex_entered and self._codex_context is not None:
                    leave = getattr(self._codex_context, "__aexit__", None)
                    if leave is not None:
                        try:
                            result = leave(None, None, None)
                            if inspect.isawaitable(result):
                                await result
                        except BaseException as exc:
                            errors.append(exc)
                elif self._owns_codex and self._codex is not None:
                    close = getattr(self._codex, "close", None)
                    if close is not None:
                        try:
                            result = close()
                            if inspect.isawaitable(result):
                                await result
                        except BaseException as exc:
                            errors.append(exc)
            finally:
                # Lifecycle state is cleared even when interruption, context
                # shutdown, or cancellation fails partway through stopping.
                self._codex_context = None
                self._codex_entered = False
                self._started = False
                self._bindings.clear()
                self._threads_by_id.clear()
                self._active_turns.clear()
                self._active_tasks.clear()
                self._active_messages.clear()
                self._interrupt_requested.clear()
                self._streaming_task_ids.clear()
                self._run_locks.clear()
                self._skills_cache = None
                if self._owns_codex:
                    self._codex = None
            if errors:
                raise errors[0]

    # ------------------------------------------------------------------
    # Public status/control helpers
    async def interrupt(self, task_id: str) -> bool:
        self._assert_loop()
        turn = self._active_turns.get(str(task_id))
        if turn is None:
            return False
        interrupt = getattr(turn, "interrupt", None)
        if interrupt is None:
            return False
        self._interrupt_requested.add(str(task_id))
        result = interrupt()
        try:
            if inspect.isawaitable(result):
                await result
        except BaseException:
            self._interrupt_requested.discard(str(task_id))
            raise
        return True

    def status(self, task_id: str | None = None, conversation_id: str | None = None) -> str:
        """Return a short, loop-local status without exposing mutable maps."""

        self._assert_loop()
        if task_id is not None:
            task = self._active_tasks.get(str(task_id))
            return "busy" if task is not None else "idle"
        if conversation_id is not None:
            return "busy" if any(
                task.conversation_id == conversation_id for task in self._active_tasks.values()
            ) else "idle"
        return "busy" if self._active_tasks else "idle"

    def active_tasks(self) -> tuple[str, ...]:
        self._assert_loop()
        return tuple(self._active_tasks)

    # Compatibility helpers for the pre-MVP interactive adapter.  They all
    # operate on the same policy-bound thread map used by ``run``.
    def set_model(self, conversation_id: str, model: str) -> None:
        self._assert_loop()
        if model:
            self._conversation_models[str(conversation_id)] = str(model)
        else:
            self._conversation_models.pop(str(conversation_id), None)

    def get_model(self, conversation_id: str) -> str:
        self._assert_loop()
        return self._conversation_models.get(str(conversation_id), self.model)

    def set_reasoning_effort(self, conversation_id: str, effort: str) -> None:
        self._assert_loop()
        if effort:
            self._conversation_reasoning_efforts[str(conversation_id)] = str(effort)
        else:
            self._conversation_reasoning_efforts.pop(str(conversation_id), None)

    def get_reasoning_effort(self, conversation_id: str) -> str:
        self._assert_loop()
        return self._conversation_reasoning_efforts.get(str(conversation_id), "")

    def info(self) -> dict[str, Any]:
        self._assert_loop()
        return {"name": "codex", "type": "codex-sdk", "model": self.model}

    async def reset_session(self, conversation_id: str) -> str:
        """Drop all mode-specific bindings for a conversation and start chat."""

        self._assert_loop()
        conversation_id = str(conversation_id)
        for key, binding in tuple(self._bindings.items()):
            if key[0] == conversation_id:
                self._bindings.pop(key, None)
                self._threads_by_id.pop(binding.thread_id, None)
        task = AgentTask(task_id=f"reset-{conversation_id}", conversation_id=conversation_id)
        binding = await self._binding_for(task)
        return binding.thread_id

    async def chat(self, conversation_id: str, message: Any) -> str:
        chunks: list[str] = []
        async for chunk in self.chat_stream(conversation_id, message):
            chunks.append(chunk)
        value = "".join(chunks).strip()
        if not value:
            raise RuntimeError("Codex returned an empty response")
        return value

    async def chat_stream(self, conversation_id: str, message: Any) -> AsyncIterator[str]:
        task = AgentTask(
            task_id=f"chat-{uuid.uuid4().hex}",
            agent_id=self.agent_id,
            conversation_id=str(conversation_id),
            mode_id="chat",
            model=self.get_model(str(conversation_id)),
            reasoning_effort=self.get_reasoning_effort(str(conversation_id)),
            inputs=message,
        )
        queue: asyncio.Queue[AgentEvent | None] = asyncio.Queue()

        async def emit(event: AgentEvent) -> None:
            await queue.put(event)

        async def run_and_signal() -> AgentResult:
            try:
                return await self.run(task, emit)
            finally:
                # Wake the consumer even when the runtime returns without
                # emitting an event (a common lightweight test double).
                queue.put_nowait(None)

        self._streaming_task_ids.add(task.task_id)
        runner = asyncio.create_task(run_and_signal())
        yielded_content = False
        try:
            while True:
                if runner.done() and queue.empty():
                    result = await runner
                    if result.error:
                        raise RuntimeError(result.error)
                    # Lightweight runtimes and older SDK shims may return a
                    # terminal result without invoking the event callback.
                    # The interactive stream still has to expose that answer.
                    if result.content and not yielded_content:
                        yielded_content = True
                        yield result.content
                    return
                event = await queue.get()
                if event is None:
                    result = await runner
                    if result.error:
                        raise RuntimeError(result.error)
                    if result.content and not yielded_content:
                        yielded_content = True
                        yield result.content
                    return
                if event.content:
                    yielded_content = True
                    yield event.content
        finally:
            if not runner.done():
                runner.cancel()
                try:
                    await runner
                except asyncio.CancelledError:
                    pass
            self._streaming_task_ids.discard(task.task_id)

    async def list_threads(self, *, search_term: str | None = None, archived: bool | None = False, limit: int = 100) -> list[dict[str, Any]]:
        await self.start()
        method = getattr(self._codex, "thread_list", None)
        if method is None:
            return []
        result = await self._call_async(method, search_term=search_term, archived=archived, limit=limit, cwd=self.cwd)
        data = getattr(result, "data", result if isinstance(result, (list, tuple)) else [])
        return [self._to_dict(item) for item in data]

    async def resume_thread(self, conversation_id: str, thread_id: str, *, mode_id: str = "chat", profile_version: int | str = 1, policy_version: int | str = 1) -> dict[str, Any]:
        await self.start()
        task = AgentTask(task_id=f"resume-{thread_id}", conversation_id=str(conversation_id), thread_id=thread_id, mode_id=mode_id, profile_version=profile_version, policy_version=policy_version)
        binding = await self._binding_for(task)
        return {"id": binding.thread_id}

    async def delete_thread(self, thread_id: str) -> None:
        await self.start()
        method = getattr(self._codex, "thread_archive", None)
        if method is not None:
            await self._call_async(method, thread_id)
        for key, binding in tuple(self._bindings.items()):
            if binding.thread_id == thread_id:
                self._bindings.pop(key, None)
        self._threads_by_id.pop(thread_id, None)

    async def list_models(self, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        await self.start()
        method = getattr(self._codex, "models", None)
        if method is None:
            return []
        result = await self._call_async(method, include_hidden=include_hidden)
        pages: list[Any] = [result]
        cursor = self._catalog_cursor(result)
        seen_cursors: set[str] = set()
        client = getattr(self._codex, "_client", None)
        request = getattr(client, "request", None)
        while cursor and cursor not in seen_cursors and request is not None:
            seen_cursors.add(cursor)
            try:
                from openai_codex.generated.v2_all import (
                    ModelListParams,
                    ModelListResponse,
                )
            except ImportError:
                break
            try:
                params = ModelListParams(
                    cursor=cursor,
                    includeHidden=include_hidden,
                ).model_dump(mode="json", by_alias=True, exclude_none=True)
            except (AttributeError, TypeError, ValueError):
                # Older SDKs may expose a cursor without the generated
                # pagination request types used by the pinned adapter. Keep
                # compatibility detection separate from the request itself:
                # request failures must not be mistaken for an old SDK and
                # silently turn a complete catalog into a partial one.
                break
            result = await self._call_async(
                request,
                "model/list",
                params,
                response_model=ModelListResponse,
            )
            pages.append(result)
            cursor = self._catalog_cursor(result)

        from src.runtime.models import iter_model_descriptors

        return [self._to_dict(item) for item in iter_model_descriptors(pages)]

    @staticmethod
    def _catalog_cursor(value: Any) -> str:
        if isinstance(value, Mapping):
            cursor = value.get("nextCursor", value.get("next_cursor"))
        else:
            cursor = getattr(value, "next_cursor", getattr(value, "nextCursor", None))
        return str(cursor or "").strip()

    async def list_skills(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        """Return the Codex skill catalog through the supported SDK boundary.

        ``openai-codex`` exposes ``SkillInput`` publicly but does not expose a
        flat ``skills()`` method in every pinned release.  Newer releases keep
        the typed ``skills/list`` RPC on the async client's public ``request``
        method, while test doubles and older adapters may provide
        ``list_skills`` directly.  Support both without leaking SDK objects to
        the channel layer.
        """

        if self._skills_cache is not None and not refresh:
            return [dict(item) for item in self._skills_cache]
        await self.start()
        method = getattr(self._codex, "list_skills", None)
        result: Any = None
        if method is not None:
            result = await self._call_async(method, refresh=refresh)
        else:
            ensure_initialized = getattr(self._codex, "_ensure_initialized", None)
            if ensure_initialized is not None:
                await self._call_async(ensure_initialized)
            client = getattr(self._codex, "_client", None)
            request = getattr(client, "request", None)
            if request is None:
                self._skills_cache = []
                return []
            try:
                from openai_codex.generated.v2_all import (
                    SkillsListParams,
                    SkillsListResponse,
                )

                params = SkillsListParams(
                    cwds=[self.cwd],
                    forceReload=bool(refresh),
                ).model_dump(by_alias=True, exclude_none=True)
                result = await self._call_async(
                    request,
                    "skills/list",
                    params,
                    response_model=SkillsListResponse,
                )
            except (ImportError, AttributeError, TypeError):
                # An older SDK may not have the generated response type.  A
                # narrow raw request fallback keeps compatibility with a
                # client that accepts an untyped response model.
                try:
                    result = await self._call_async(
                        request,
                        "skills/list",
                        {"cwds": [self.cwd], "forceReload": bool(refresh)},
                        response_model=dict,
                    )
                except Exception:
                    result = None
        from src.runtime.skills import iter_skill_descriptors

        skills = [
            self._to_dict(skill)
            for skill in iter_skill_descriptors(result)
        ]
        self._skills_cache = [dict(item) for item in skills]
        return [dict(item) for item in skills]

    async def resolve_skill(self, name: str, *, refresh: bool = False) -> dict[str, Any] | None:
        """Resolve one enabled skill by case-insensitive canonical name."""

        from src.runtime.skills import find_skill

        definition = find_skill(
            await self.list_skills(refresh=refresh),
            name,
            rehash_local_bundles=True,
        )
        return definition.snapshot() if definition is not None else None

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if hasattr(value, "model_dump"):
            try:
                return value.model_dump(
                    mode="json", by_alias=True, exclude_none=True
                )
            except TypeError:
                try:
                    return value.model_dump(by_alias=True, exclude_none=True)
                except TypeError:
                    return value.model_dump()
        if hasattr(value, "__dict__"):
            return dict(value.__dict__)
        # Slots-based dataclasses (including ``SkillDefinition``) do not have
        # ``__dict__`` but still expose stable public descriptor attributes.
        fields = (
            "name",
            "path",
            "description",
            "short_description",
            "shortDescription",
            "display_name",
            "displayName",
            "version",
            "content_hash",
            "enabled",
        )
        projected = {name: getattr(value, name) for name in fields if hasattr(value, name)}
        if projected:
            return projected
        return {"value": value}

    def get_thread_id(
        self,
        conversation_id: str,
        *,
        mode_id: str = "chat",
        profile_version: int | str = 1,
        policy_version: int | str = 1,
    ) -> str | None:
        self._assert_loop()
        binding = self._bindings.get((str(conversation_id), mode_id, str(profile_version), str(policy_version)))
        return binding.thread_id if binding else None

    # ------------------------------------------------------------------
    # Main runtime contract
    async def run(self, task: AgentTask, emit: EmitCallback | None = None) -> AgentResult:
        self._assert_loop()
        if emit is None:
            async def emit(_event: AgentEvent) -> None:
                return None
        if not isinstance(task, AgentTask):
            task = AgentTask.from_record(task)
        if not task.execution_id:
            task = task.with_execution(f"exec-{uuid.uuid4().hex}")
        lock = self._run_locks.setdefault(self._run_lock_key(task), asyncio.Lock())
        async with lock:
            try:
                return await self._run_locked(task, emit)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return AgentResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id or None,
                    status="failed",
                    error=str(exc) or exc.__class__.__name__,
                )

    async def _run_locked(self, task: AgentTask, emit: EmitCallback) -> AgentResult:
        self._assert_loop()
        if not task.conversation_id:
            raise ValueError("AgentTask.conversation_id is required for Codex execution")
        self._require_execute_policy_snapshot(task)
        # The timeout covers client initialization, thread binding/resume, and
        # turn startup as well as event streaming.  A hung RPC before the
        # first notification must not leave a worker lease occupied forever.
        deadline = time.monotonic() + self.turn_timeout if self.turn_timeout is not None else None
        try:
            await self._await_with_deadline(self.start(), deadline)
            binding = await self._await_with_deadline(self._binding_for(task), deadline)
        except asyncio.TimeoutError:
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id or None,
                status="failed",
                error="Codex turn timed out",
            )
        kwargs: dict[str, Any] = {
            "approval_mode": approval_for_policy(self._policy_value(task, "approval_policy", "deny_all")),
            "sandbox": sandbox_for_policy(self._policy_value(task, "sandbox_policy", "read-only")),
            "cwd": self.cwd,
        }
        model = task.model or self.model
        if model:
            kwargs["model"] = model
        if task.reasoning_effort:
            kwargs["effort"] = task.reasoning_effort

        try:
            # This is the final synchronous operation before the native turn
            # call.  Revalidating here minimizes the mutation window between
            # hashing the complete bundle and handing its path to the SDK.
            input_value = self._translate_input(task.inputs)
            input_value = self._with_agent_bridge_context(task, input_value)
            turn = await self._await_with_deadline(
                self._start_turn(binding.thread, input_value, kwargs), deadline
            )
        except asyncio.TimeoutError:
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id or None,
                status="failed",
                error="Codex turn timed out",
                thread_id=binding.thread_id,
            )
        task_id = str(task.task_id)
        self._active_turns[task_id] = turn
        self._active_tasks[task_id] = task
        self._active_messages[task_id] = self._input_preview(task.inputs)
        emitted: list[AgentEvent] = []
        sequence = 0
        # Count every completed SDK item, including ineligible tool/reasoning
        # items.  This gives text items a stable fallback identity and order
        # even when the SDK omits an item ID.
        completed_item_ordinal = 0
        # SDK reconnect/replay can expose the same completed item more than
        # once.  An SDK item ID identifies one immutable item within this
        # execution, so a byte-for-byte replay is ignored and conflicting
        # reuse fails the turn before it can append a second reply candidate.
        completed_items_by_id: dict[str, tuple[str, str, tuple[Any, ...]]] = {}
        # Image publication copies bytes and registers durable metadata, so it
        # must not run twice before the ordinary completed-event deduplicator
        # sees a replay. Cache the first publication by immutable SDK item ID.
        completed_images_by_id: dict[
            str, tuple[tuple[str, str], tuple[AgentEvent, ...]]
        ] = {}
        text_parts: list[str] = []
        delta_parts: list[str] = []
        stream_deltas = task.task_id in self._streaming_task_ids
        interrupted = False
        status = "completed"
        error_text: str | None = None
        stream = None
        interrupt_requested = False
        try:
            stream = self._stream(turn)
            async for notification in self._iterate_with_deadline(stream, deadline):
                method = str(
                    notification.get("method", "")
                    if isinstance(notification, Mapping)
                    else getattr(notification, "method", "")
                ).lower()
                is_completed_item = method in {"item/completed", "item_completed"}
                extracted = await self._events_for_notification(
                    task,
                    notification,
                    sequence,
                    source_item_ordinal=(
                        completed_item_ordinal if is_completed_item else None
                    ),
                    completed_images_by_id=completed_images_by_id,
                )
                if is_completed_item:
                    completed_item_ordinal += 1
                for event in extracted:
                    if event.event_type == "message_delta":
                        if event.content:
                            delta_parts.append(event.content)
                            if stream_deltas:
                                # Deltas are interactive-only progress. Keep
                                # them out of ``result.events`` so durable
                                # workers project one stable final message.
                                delta_event = AgentEvent.text_event(
                                    task.task_id,
                                    event.content,
                                    sequence=sequence,
                                    visibility=EventVisibility.INTERNAL,
                                    priority=int(EventPriority.SILENT),
                                    event_type="message_delta",
                                    execution_id=task.execution_id or None,
                                )
                                sequence += 1
                                await emit_if_awaitable(emit, delta_event)
                        continue
                    if event.source_item_id:
                        source_item_id = str(event.source_item_id)
                        item_snapshot = (
                            str(event.source_item_type or ""),
                            str(event.content or ""),
                            tuple(event.attachments or ()),
                        )
                        previous = completed_items_by_id.get(source_item_id)
                        if previous is not None:
                            if previous != item_snapshot:
                                raise RuntimeError(
                                    "Codex completed item identity conflicts: "
                                    f"{source_item_id}"
                                )
                            delta_parts.clear()
                            continue
                        completed_items_by_id[source_item_id] = item_snapshot
                    sequence = max(sequence, event.sequence + 1)
                    emitted.append(event)
                    if event.content:
                        text_parts.append(event.content)
                    # A completed item often repeats the text already sent as
                    # deltas. Interactive callers should not receive it twice;
                    # durable callers still get the stable event as usual.
                    if not (stream_deltas and delta_parts):
                        await emit_if_awaitable(emit, event)
                    delta_parts.clear()
                terminal = self._terminal_status(notification)
                if terminal is not None:
                    if terminal not in {"failed", "error"} and not text_parts and delta_parts:
                        final_event = AgentEvent.text_event(
                            task.task_id,
                            "".join(delta_parts).strip(),
                            sequence=sequence,
                            execution_id=task.execution_id or None,
                        )
                        if final_event.content:
                            emitted.append(final_event)
                            text_parts.append(final_event.content)
                            if not (stream_deltas and delta_parts):
                                await emit_if_awaitable(emit, final_event)
                    if terminal in {"failed", "error"}:
                        status = "failed"
                        error_text = self._terminal_error(notification)
                    elif terminal in {"interrupted", "cancelled", "canceled"}:
                        status = "interrupted"
                        interrupted = True
                    break
        except asyncio.TimeoutError:
            status = "failed"
            error_text = "Codex turn timed out"
            try:
                await self.interrupt(task_id)
            except Exception:
                logger.debug("failed to interrupt timed-out turn", exc_info=True)
        except asyncio.CancelledError:
            # Cancellation is an interruption from the worker's perspective;
            # propagate cancellation after asking the SDK to stop the turn.
            try:
                await self.interrupt(task_id)
            finally:
                raise
        except Exception as exc:
            # A notification transport or durable emit callback can fail while
            # the Codex turn is still executing.  Closing the local iterator
            # only unregisters notifications in the pinned SDK; it does not
            # stop the remote turn, which is especially unsafe for writable
            # execute-mode work.  Best-effort interruption keeps the terminal
            # failure from leaving detached side effects running.
            if task_id not in self._interrupt_requested:
                try:
                    await self.interrupt(task_id)
                except Exception:
                    logger.debug(
                        "failed to interrupt Codex turn after stream failure",
                        exc_info=True,
                    )
            status = "failed"
            error_text = str(exc) or exc.__class__.__name__
        finally:
            interrupt_requested = task_id in self._interrupt_requested
            self._interrupt_requested.discard(task_id)
            self._active_turns.pop(task_id, None)
            self._active_tasks.pop(task_id, None)
            self._active_messages.pop(task_id, None)
            if stream is not None:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    try:
                        result = close()
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        logger.debug("failed to close Codex event stream", exc_info=True)

        if status == "completed" and interrupt_requested:
            status = "interrupted"
            interrupted = True
        if status == "completed" and not text_parts and delta_parts:
            final_event = AgentEvent.text_event(
                task.task_id,
                "".join(delta_parts).strip(),
                sequence=sequence,
                execution_id=task.execution_id or None,
            )
            if final_event.content:
                emitted.append(final_event)
                text_parts.append(final_event.content)
                if not (stream_deltas and delta_parts):
                    await emit_if_awaitable(emit, final_event)
        has_media_output = any(
            tuple(event.attachments or ()) for event in emitted
        )
        if (
            status == "completed"
            and not text_parts
            and not has_media_output
            and not interrupted
        ):
            status = "failed"
            error_text = error_text or "Codex returned an empty response"
        return AgentResult(
            task_id=task.task_id,
            execution_id=task.execution_id or None,
            status=status,
            content="".join(text_parts).strip(),
            error=error_text,
            events=tuple(emitted),
            interrupted=interrupted,
            thread_id=binding.thread_id,
        )

    @staticmethod
    async def _await_with_deadline(awaitable: Any, deadline: float | None) -> Any:
        """Await one startup operation without leaking an unawaited coroutine."""

        if deadline is None:
            return await awaitable
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            close = getattr(awaitable, "close", None)
            if close is not None:
                close()
            raise asyncio.TimeoutError
        return await asyncio.wait_for(awaitable, remaining)

    async def _start_turn(self, thread: Any, input_value: Any, kwargs: dict[str, Any]) -> Any:
        method = getattr(thread, "turn", None)
        if method is None:
            raise RuntimeError("Codex thread does not expose turn()")
        # Small fakes and older SDKs may not accept every optional kwarg.
        result = method(input_value, **self._supported_kwargs(method, kwargs))
        if inspect.isawaitable(result):
            return await result
        return result

    @staticmethod
    def _supported_kwargs(method: Callable[..., Any], kwargs: Mapping[str, Any]) -> dict[str, Any]:
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            return dict(kwargs)
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
            return dict(kwargs)
        return {name: value for name, value in kwargs.items() if name in parameters}

    async def _iterate_with_deadline(self, stream: Any, deadline: float | None) -> AsyncIterator[Any]:
        """Iterate an async stream while enforcing the per-turn timeout."""

        if inspect.isawaitable(stream):
            stream = await self._await_with_deadline(stream, deadline)
        # Compatibility fakes and a few SDK shims expose a finite synchronous
        # iterable rather than an async iterator.  Consume it without routing
        # through the event loop's blocking executor; these values are already
        # materialized and therefore cannot block on network I/O.
        if not hasattr(stream, "__aiter__"):
            for item in stream:
                if deadline is not None and time.monotonic() >= deadline:
                    raise asyncio.TimeoutError
                yield item
            return
        iterator = stream.__aiter__()
        while True:
            if deadline is None:
                try:
                    item = await anext(iterator)
                except StopAsyncIteration:
                    return
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                try:
                    item = await asyncio.wait_for(anext(iterator), remaining)
                except StopAsyncIteration:
                    return
            yield item

    def _stream(self, turn: Any) -> Any:
        stream = getattr(turn, "stream", None)
        if stream is None:
            # A few test doubles expose an async iterator directly.
            return turn
        result = stream() if callable(stream) else stream
        return result

    # ------------------------------------------------------------------
    # Thread and policy translation
    def _thread_key(self, task: AgentTask) -> tuple[str, str, str, str]:
        return (
            task.conversation_id,
            task.mode_id or "chat",
            str(task.profile_version),
            str(task.policy_version),
        )

    def _run_lock_key(self, task: AgentTask) -> tuple[str, str]:
        """Return the serialization identity for direct runtime execution."""

        return (task.conversation_id, task.agent_id or self.agent_id)

    def _policy_value(self, task: AgentTask, name: str, default: Any) -> Any:
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        policy = self._effective_policy_for_task(task)
        # If a caller supplied an explicit but mismatched policy snapshot,
        # fail closed instead of falling back to a permissive raw mode value.
        if "effective_policy" in metadata and policy is None:
            return default
        if isinstance(policy, Mapping) and name in policy:
            return policy[name]
        if policy is not None and hasattr(policy, name):
            return getattr(policy, name)
        mode = self._mode_for_task(task)
        if isinstance(mode, Mapping) and name in mode:
            return mode[name]
        if mode is not None and hasattr(mode, name):
            return getattr(mode, name)
        return default

    @staticmethod
    def _snapshot_value(value: Any, *names: str) -> Any:
        if isinstance(value, Mapping):
            for name in names:
                if name in value:
                    return value[name]
            return None
        for name in names:
            if hasattr(value, name):
                return getattr(value, name)
        return None

    @classmethod
    def _snapshot_matches(
        cls,
        value: Any,
        *,
        agent_id: str | None = None,
        profile_version: int | str | None = None,
        mode_id: str | None = None,
        policy_version: int | str | None = None,
    ) -> bool:
        """Validate fields that a supplied immutable snapshot explicitly carries.

        Older integrations returned unversioned lightweight objects.  Those
        remain usable as compatibility values; whenever an identity/version is
        present, however, a mismatch is rejected instead of silently applying
        policy from another task.
        """

        if agent_id is not None:
            actual_agent = cls._snapshot_value(value, "agent_id", "profile_id", "id")
            if actual_agent is not None and str(actual_agent) != str(agent_id):
                return False
        if profile_version is not None:
            actual_profile = cls._snapshot_value(value, "profile_version", "profileVersion")
            if actual_profile is not None and str(actual_profile) != str(profile_version):
                return False
        if mode_id is not None:
            actual_mode = cls._snapshot_value(value, "mode_id", "mode", "id")
            if actual_mode is not None and str(actual_mode) != str(mode_id):
                return False
        if policy_version is not None:
            actual_policy = cls._snapshot_value(
                value,
                "mode_policy_version",
                "policy_version",
                "version",
            )
            if actual_policy is not None and str(actual_policy) != str(policy_version):
                return False
        return True

    @classmethod
    def _effective_policy_for_task(cls, task: AgentTask) -> Any | None:
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        policy = metadata.get("effective_policy")
        if policy is None:
            return None
        if not cls._snapshot_matches(
            policy,
            agent_id=task.agent_id,
            profile_version=task.profile_version,
            mode_id=task.mode_id or "chat",
            policy_version=task.policy_version,
        ):
            return None
        return policy

    @classmethod
    def _require_execute_policy_snapshot(cls, task: AgentTask) -> None:
        """Fail closed before an execute task can reach a writable SDK thread."""

        if str(task.mode_id or "").strip().lower() != "execute":
            return
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        policy = metadata.get("effective_policy")
        required_identity = {
            "profile_id": task.agent_id,
            "profile_version": task.profile_version,
            "mode_id": task.mode_id,
            "mode_policy_version": task.policy_version,
        }
        for field, expected in required_identity.items():
            actual = cls._snapshot_value(policy, field)
            if actual is None:
                raise PermissionError(
                    "execute mode requires a complete effective policy snapshot"
                )
            if str(actual) != str(expected):
                raise PermissionError(
                    "execute mode effective policy snapshot does not match the task"
                )

        sandbox = cls._snapshot_value(policy, "sandbox_policy")
        sandbox = str(getattr(sandbox, "value", sandbox) or "").strip().lower()
        sandbox = sandbox.replace("_", "-")
        can_write = cls._snapshot_value(policy, "can_write_files")
        can_execute = cls._snapshot_value(policy, "can_execute_commands")
        try:
            policy_version = int(task.policy_version)
        except (TypeError, ValueError):
            policy_version = -1
        required_sandbox = "workspace-write" if policy_version == 1 else "full-access"
        if sandbox != required_sandbox or can_write is not True or can_execute is not True:
            raise PermissionError(
                "execute mode requires its versioned writable sandbox, file-write, "
                "and command-execution policy"
            )

    @staticmethod
    def _mode_policy_version(value: Any) -> Any:
        if isinstance(value, Mapping):
            return value.get("policy_version", value.get("version"))
        return getattr(value, "policy_version", getattr(value, "version", None))

    def _mode_for_task(self, task: AgentTask) -> Any | None:
        """Resolve the exact immutable mode snapshot carried by a task."""

        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        candidate = metadata.get("mode") or metadata.get("agent_mode")
        if candidate is not None:
            if isinstance(candidate, Mapping):
                candidate_id = candidate.get("mode_id", candidate.get("id"))
            else:
                candidate_id = getattr(
                    candidate, "mode_id", getattr(candidate, "id", None)
                )
            candidate_version = self._mode_policy_version(candidate)
            id_matches = candidate_id is None or str(candidate_id) == str(task.mode_id)
            version_matches = candidate_version is None or str(candidate_version) == str(task.policy_version)
            if id_matches and version_matches:
                return candidate
        return self._resolve_mode(task.mode_id, task.policy_version)

    def _resolve_mode(self, mode_id: str, version: int | str | None = None) -> Any | None:
        resolver = self._mode_resolver
        if resolver is None:
            return None
        if isinstance(resolver, Mapping):
            if version is None:
                return resolver.get(mode_id)
            # Versioned mappings commonly use either a tuple key or a
            # ``mode@version`` key.  Never fall back to the unversioned/latest
            # entry when a task carries an immutable policy version.
            candidate = resolver.get((mode_id, version))
            if candidate is None:
                candidate = resolver.get(f"{mode_id}@{version}")
            if candidate is None:
                # A simple mapping may be keyed only by mode ID.  It is safe
                # to use that entry only when its embedded version exactly
                # matches the task snapshot (builtin modes are versioned).
                candidate = resolver.get(mode_id)
            if candidate is None:
                return None
            actual = self._mode_policy_version(candidate)
            return candidate if actual is None or str(actual) == str(version) else None
        getter = getattr(resolver, "get", None)
        if getter is not None:
            try:
                if version is not None:
                    candidate = getter(mode_id, version)
                else:
                    candidate = getter(mode_id)
                if candidate is None:
                    return None
                actual = self._mode_policy_version(candidate)
                return candidate if version is None or actual is None or str(actual) == str(version) else None
            except TypeError:
                if version is not None:
                    try:
                        candidate = getter(mode_id)
                    except TypeError:
                        candidate = None
                    actual = self._mode_policy_version(candidate) if candidate is not None else None
                    return candidate if candidate is not None and (actual is None or str(actual) == str(version)) else None
                return None
        try:
            if version is not None:
                try:
                    candidate = resolver(mode_id, version)
                except TypeError:
                    candidate = resolver(mode_id)
                actual = self._mode_policy_version(candidate) if candidate is not None else None
                return candidate if candidate is not None and (actual is None or str(actual) == str(version)) else None
            return resolver(mode_id)
        except TypeError:
            return None

    def _developer_instructions(self, task: AgentTask) -> str:
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        policy = self._effective_policy_for_task(task)
        mode = self._mode_for_task(task)
        pieces: list[str] = []
        if self.profile_prompt:
            pieces.append(self.profile_prompt)
        profile = metadata.get("profile") or metadata.get("agent_profile")
        if profile is not None and not self._snapshot_matches(
            profile,
            agent_id=task.agent_id,
            profile_version=task.profile_version,
        ):
            profile = None
        if profile is None and self._profile_resolver is not None:
            try:
                if isinstance(self._profile_resolver, Mapping):
                    profile = self._profile_resolver.get(
                        (task.agent_id, task.profile_version)
                    )
                    if profile is None:
                        profile = self._profile_resolver.get(
                            f"{task.agent_id}@{task.profile_version}"
                        )
                    if profile is None:
                        profile = self._profile_resolver.get(task.agent_id)
                    if profile is not None:
                        if not self._snapshot_matches(
                            profile,
                            agent_id=task.agent_id,
                            profile_version=task.profile_version,
                        ):
                            profile = None
                else:
                    try:
                        profile = self._profile_resolver(
                            task.agent_id, task.profile_version
                        )
                    except TypeError:
                        # Compatibility resolvers from the interactive
                        # adapter often accept only the Agent ID.  Validate
                        # the returned snapshot before using that fallback.
                        profile = self._profile_resolver(task.agent_id)
                    if profile is not None and not self._snapshot_matches(
                        profile,
                        agent_id=task.agent_id,
                        profile_version=task.profile_version,
                    ):
                        profile = None
            except Exception:
                profile = None
        profile_prompt = (
            profile.get("system_prompt", "") if isinstance(profile, Mapping)
            else getattr(profile, "system_prompt", "") if profile is not None else ""
        )
        if profile_prompt:
            pieces.append(str(profile_prompt))
        for value in (
            getattr(policy, "developer_instructions", None)
            if policy is not None and not isinstance(policy, Mapping)
            else (policy or {}).get("developer_instructions") if isinstance(policy, Mapping) else None,
            mode.get("developer_instructions")
            if isinstance(mode, Mapping)
            else getattr(mode, "developer_instructions", None),
        ):
            if value and value not in pieces:
                pieces.append(str(value))
        return "\n\n".join(pieces)

    async def _binding_for(self, task: AgentTask) -> ThreadBinding:
        key = self._thread_key(task)
        existing = self._bindings.get(key)
        if existing is not None:
            return existing
        thread = None
        # A persisted thread is valid only for this exact policy key.  The task
        # snapshot carries the key, so resuming it is safe.
        if task.thread_id:
            resume = getattr(self._codex, "thread_resume", None)
            if resume is not None:
                kwargs = {
                    "approval_mode": approval_for_policy(self._policy_value(task, "approval_policy", "deny_all")),
                    "sandbox": sandbox_for_policy(self._policy_value(task, "sandbox_policy", "read-only")),
                    "cwd": self.cwd,
                }
                instructions = self._developer_instructions(task)
                if instructions:
                    kwargs["developer_instructions"] = instructions
                model = task.model or self.model
                if model:
                    kwargs["model"] = model
                try:
                    thread = await self._call_async(resume, task.thread_id, **kwargs)
                except Exception:
                    logger.info("could not resume persisted Codex thread %s; starting a new one", task.thread_id)
        if thread is None:
            start = getattr(self._codex, "thread_start", None)
            if start is None:
                raise RuntimeError("Codex client does not expose thread_start()")
            kwargs = {
                "approval_mode": approval_for_policy(self._policy_value(task, "approval_policy", "deny_all")),
                "sandbox": sandbox_for_policy(self._policy_value(task, "sandbox_policy", "read-only")),
                "cwd": self.cwd,
            }
            instructions = self._developer_instructions(task)
            if instructions:
                kwargs["developer_instructions"] = instructions
            model = task.model or self.model
            if model:
                kwargs["model"] = model
            thread = await self._call_async(start, **kwargs)
        thread_id_value = (
            thread.get("id")
            if isinstance(thread, Mapping)
            else getattr(thread, "id", None)
        )
        thread_id = str(thread_id_value or task.thread_id or f"thread-{task.task_id}")
        binding = ThreadBinding(
            conversation_id=task.conversation_id,
            mode_id=task.mode_id or "chat",
            profile_version=task.profile_version,
            policy_version=task.policy_version,
            thread_id=thread_id,
            thread=thread,
        )
        self._bindings[key] = binding
        self._threads_by_id[thread_id] = thread
        return binding

    async def _call_async(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        result = method(*args, **self._supported_kwargs(method, kwargs))
        if inspect.isawaitable(result):
            return await result
        return result

    def _translate_input(self, value: Any) -> Any:
        """Translate structured text/image input without hiding unsupported media."""

        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            # Persisted task rows commonly use ``{"text": ..., "images": [...]}``.
            items: list[Any] = []
            unsupported_media: list[Any] = []
            skill_value = value.get("skill")
            if skill_value is not None:
                skill_name = self._media_field(skill_value, "name", "skill_id", "id")
                skill_path = self._media_field(skill_value, "path")
                if skill_name and skill_path:
                    # Codex expects the skill attachment before the user's
                    # description, matching the native `$skill prompt` wire
                    # representation.
                    items.append(self._skill_input(skill_value))
                elif skill_name or skill_path:
                    raise ValueError("skill snapshot is incomplete")
            text = value.get("text", value.get("content", ""))
            if text:
                items.append(TextInput(str(text)) if TextInput is not None else str(text))
            # A single attachment record is also a valid persisted input.  It
            # must remain structured when the SDK cannot consume it; turning
            # the Python mapping into prompt text would erase the media kind
            # and could cause an Agent to treat an untrusted file as prose.
            single_kind = str(value.get("kind", value.get("type", "")) or "").lower()
            if single_kind and single_kind not in {"text", "prompt", "message"}:
                if single_kind in {"image", "local_image", "localimage"}:
                    translated = self._image_input(value)
                    if translated is not None:
                        items.append(translated)
                    else:
                        unsupported_media.append(value)
                else:
                    unsupported_media.append(value)
            elif not single_kind and self._is_image_media(value):
                translated = self._image_input(value)
                if translated is not None:
                    items.append(translated)
                else:
                    unsupported_media.append(value)
            for image in self._media_values(value.get("images", ())):
                translated = self._image_input(image)
                if translated is not None:
                    items.append(translated)
                else:
                    unsupported_media.append(image)
            for media in self._media_values(value.get("attachments", ())):
                if self._is_image_media(media):
                    translated = self._image_input(media)
                    if translated is not None:
                        items.append(translated)
                    else:
                        unsupported_media.append(media)
                else:
                    unsupported_media.append(media)
            for media in self._media_values(value.get("media", ())):
                if self._is_image_media(media):
                    translated = self._image_input(media)
                    if translated is not None:
                        items.append(translated)
                    else:
                        unsupported_media.append(media)
                else:
                    unsupported_media.append(media)
            if unsupported_media:
                # Keep unsupported media explicit and structured.  This is not
                # presented as user prose, so the Agent can distinguish it
                # from the prompt and decide which controlled media tool is
                # required.
                context = {
                    "media_context": [canonical_media_input(item) for item in unsupported_media],
                    "native_input_unavailable": True,
                }
                import json

                rendered = "Structured media context: " + json.dumps(context, ensure_ascii=False, sort_keys=True)
                items.append(TextInput(rendered) if TextInput is not None else rendered)
            if items:
                return items if len(items) > 1 else items[0]
            # Preserve arbitrary structured records as a media context too;
            # this covers future channel attachment kinds without silently
            # stringifying binary metadata.
            if any(key in value for key in ("attachment_id", "mime_type", "path", "remote_id", "media")):
                return self._media_context_input([value])
            return str(value)
        if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
            items: list[Any] = []
            unsupported: list[Any] = []
            for item in value:
                if isinstance(item, str):
                    items.append(TextInput(item) if TextInput is not None else item)
                elif (
                    (TextInput is not None and isinstance(item, TextInput))
                    or (LocalImageInput is not None and isinstance(item, LocalImageInput))
                    or (SkillInput is not None and isinstance(item, SkillInput))
                ):
                    items.append(item)
                elif isinstance(item, Mapping) and (
                    str(item.get("type", "")).lower() == "skill"
                    or (item.get("path") and (item.get("skill_id") or item.get("name")))
                ):
                    skill_name = item.get("name", item.get("skill_id"))
                    skill_path = item.get("path")
                    if not skill_name or not skill_path:
                        raise ValueError("skill snapshot is incomplete")
                    items.append(self._skill_input(item))
                elif (
                    self._media_field(item, "path")
                    and self._media_field(item, "skill_id", "name", "id")
                ):
                    items.append(self._skill_input(item))
                elif self._is_image_media(item):
                    translated = self._image_input(item)
                    if translated is None:
                        unsupported.append(item)
                    else:
                        items.append(translated)
                else:
                    # Keep non-image media explicit rather than passing an
                    # arbitrary object through to the SDK input normalizer.
                    if self._is_media_record(item):
                        unsupported.append(item)
                    else:
                        items.append(item)
            if unsupported:
                items.append(self._media_context_input(unsupported))
            return items
        return value

    def _with_agent_bridge_context(self, task: AgentTask, value: Any) -> Any:
        """Prepend a task-scoped collaboration capability when policy allows.

        Thread developer instructions are policy-bound and may be reused by
        several tasks, so they cannot safely carry a task ID.  This context is
        attached to the individual turn instead.  The local bridge revalidates
        the running task and its immutable policy before every list/send call;
        the text here is discovery, not authorization.
        """

        command = self._agent_bridge_command
        issuer = self._agent_bridge_capability_issuer
        policy = self._effective_policy_for_task(task)
        can_send = (
            policy.get("can_send_agent_messages")
            if isinstance(policy, Mapping)
            else getattr(policy, "can_send_agent_messages", None)
            if policy is not None
            else None
        )
        # Discovery is exposed only by an explicit immutable policy grant.
        # Falling back to a raw mode would advertise the bridge to legacy
        # v1/v2 tasks whose Profile ACL intentionally denies collaboration.
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        # Mailbox workers create claim-scoped synthetic tasks rather than rows
        # in the durable task table. Do not advertise a task bridge that would
        # necessarily fail active-task validation for those internal turns.
        if (
            not command
            or issuer is None
            or can_send is not True
            or bool(metadata.get("internal_mailbox"))
        ):
            return value
        task_id = str(task.task_id or "").strip()
        execution_id = str(task.execution_id or "").strip()
        if not task_id or not execution_id:
            return value
        capability = str(issuer(task) or "").strip()
        if not capability:
            raise RuntimeError("Agent bridge capability issuer returned no token")
        quoted_task_id = shlex.quote(task_id)
        quoted_capability = shlex.quote(capability)
        context_text = (
            "Codex-on-WeChat collaboration capability for this turn:\n"
            "You may communicate with Agents created by /agent through the "
            "durable local mailbox. Discover authorized peers with:\n"
            f"  {command} --task-id {quoted_task_id} "
            f"--capability {quoted_capability} list\n"
            "Send a request with:\n"
            f"  {command} --task-id {quoted_task_id} "
            f"--capability {quoted_capability} send <agent-id> "
            "'<message>'\n"
            "Use only these commands for named-Agent communication. The bridge "
            "enforces this task's immutable ACL and returns a durable request ID."
        )
        context = TextInput(context_text) if TextInput is not None else context_text
        if isinstance(value, list):
            return [context, *value]
        if isinstance(value, tuple):
            return [context, *value]
        if value in (None, ""):
            return context
        return [context, value]

    def _media_context_input(self, media: Sequence[Any]) -> Any:
        """Build a textual SDK input that explicitly labels unsupported media."""
        import json

        context = {
            "media_context": [canonical_media_input(item) for item in media],
            "native_input_unavailable": True,
        }
        rendered = "Structured media context: " + json.dumps(
            context, ensure_ascii=False, sort_keys=True, default=str
        )
        return TextInput(rendered) if TextInput is not None else rendered

    @staticmethod
    def _media_values(value: Any) -> tuple[Any, ...]:
        """Normalize singleton media records without iterating mapping keys."""

        if value is None:
            return ()
        if isinstance(value, (Mapping, str, bytes, bytearray, Path)):
            return (value,)
        if isinstance(value, Sequence):
            return tuple(value)
        return (value,)

    @staticmethod
    def _media_field(value: Any, *names: str) -> Any:
        if isinstance(value, Mapping):
            for name in names:
                if name in value:
                    return value[name]
            return None
        for name in names:
            if hasattr(value, name):
                return getattr(value, name)
        return None

    @classmethod
    def _is_image_media(cls, value: Any) -> bool:
        kind = str(cls._media_field(value, "kind", "type", "media_type") or "").lower()
        mime = str(cls._media_field(value, "mime_type", "mime", "content_type") or "").lower()
        return kind in {"image", "local_image", "localimage"} or mime.startswith("image/")

    @classmethod
    def _is_media_record(cls, value: Any) -> bool:
        return any(
            cls._media_field(value, name) is not None
            for name in (
                "kind",
                "type",
                "attachment_id",
                "mime_type",
                "path",
                "local_path",
                "file_path",
                "remote_id",
            )
        )

    def _skill_input(self, value: Any) -> Any:
        """Translate a persisted skill reference only after trust checks.

        The manager normally validates a snapshot against the SDK catalog.
        This second check protects restart paths and direct adapter callers:
        configured skill roots, a loaded catalog entry, or a recorded content
        hash must agree before a native ``SkillInput`` is emitted.  A legacy
        descriptor with no hash and no configured/catalog root is retained for
        compatibility with older SDK fakes; it still cannot carry arbitrary
        instructions or capabilities.
        """

        name = self._media_field(value, "name", "skill_id", "id")
        path = self._media_field(value, "path")
        if not name or not path:
            raise ValueError("skill snapshot is incomplete")
        name = str(name).strip()
        path_obj = Path(str(path)).expanduser()
        if path_obj.is_symlink():
            raise ValueError("skill path is not trusted")
        try:
            resolved = path_obj.resolve(strict=False)
        except OSError as exc:
            raise ValueError("skill path is not trusted") from exc

        # Explicit roots are an administrator-provided boundary.  Resolve the
        # candidate before checking containment so ``..`` and symlink escapes
        # cannot pass a lexical prefix test.
        if self.trusted_skill_roots:
            if not any(
                resolved == root or root in resolved.parents
                for root in self.trusted_skill_roots
            ):
                raise ValueError("skill path is outside the trusted root")

        expected_hash = str(
            self._media_field(value, "content_hash", "skill_hash", "hash") or ""
        ).strip().lower()

        def verify_local_content(
            *,
            required: bool = True,
        ) -> None:
            """Verify the persisted hash against the complete local bundle."""

            from src.runtime.skills import SkillBundleError, hash_skill_bundle

            try:
                bundle_hash = hash_skill_bundle(resolved)
            except FileNotFoundError:
                if required:
                    raise ValueError("skill content is unavailable")
                return
            except OSError as exc:
                raise ValueError("skill content is unavailable") from exc
            except SkillBundleError as exc:
                raise ValueError("skill content is not trusted") from exc
            if expected_hash != bundle_hash:
                raise ValueError("skill content hash does not match")

        catalog = self._skills_cache
        if catalog is not None:
            from src.runtime.skills import find_skill, normalize_skill

            definition = find_skill(catalog, name)
            if definition is None:
                raise ValueError("skill is not present in the trusted catalog")
            trusted = normalize_skill(definition, rehash_local_bundle=True)
            try:
                trusted_path = Path(trusted.path).expanduser().resolve(strict=False)
            except OSError as exc:
                raise ValueError("skill path is not trusted") from exc
            if trusted_path != resolved:
                # A queued task may legitimately reference an older immutable
                # version after the live catalog advances the same name to a
                # new path.  The persisted hash is the proof for that historical
                # version, while the configured root remains the administrator's
                # path boundary.  A caller-supplied hash alone cannot override
                # the current catalog selection.
                if not self.trusted_skill_roots or not expected_hash:
                    raise ValueError("skill path does not match the trusted catalog")
                verify_local_content()
            else:
                if expected_hash and expected_hash != trusted.content_hash:
                    raise ValueError("skill hash does not match the trusted catalog")
                if not expected_hash:
                    expected_hash = trusted.content_hash

                # Re-read the selected skill bytes even when the catalog supplied
                # a hash.  A file can change after discovery; trusting the cached
                # descriptor alone would then violate the immutable snapshot.
                verify_local_content(
                    required=bool(self.trusted_skill_roots),
                )
        elif expected_hash:
            # A restarted runtime may not have populated its SDK cache yet.
            # The durable snapshot still binds execution to the complete
            # local bundle even without a live catalog entry.
            verify_local_content()
        elif self.trusted_skill_roots:
            # A configured root without a catalog/hash still requires a real
            # skill payload rather than allowing an arbitrary new path.
            from src.runtime.skills import SkillBundleError, hash_skill_bundle

            try:
                hash_skill_bundle(resolved)
            except FileNotFoundError as exc:
                raise ValueError("skill content is unavailable") from exc
            except OSError as exc:
                raise ValueError("skill content is unavailable") from exc
            except SkillBundleError as exc:
                raise ValueError("skill content is not trusted") from exc

        if SkillInput is None:
            return {"type": "skill", "name": name, "path": str(resolved)}
        return SkillInput(name=name, path=str(resolved))

    def _image_input(self, value: Any) -> Any | None:
        if LocalImageInput is None:
            return None
        # Canonical managed media records explicitly mark whether content
        # passed MIME/signature validation.  Never let a channel ``kind`` or
        # filename override a durable negative decision.  Inputs from older
        # callers without this field retain the legacy path-only behavior.
        native_flag = self._media_field(value, "native_input_available")
        if native_flag is False:
            return None
        if isinstance(value, (str, Path)):
            path = value
        else:
            path = self._media_field(value, "path", "local_path", "file_path")
        if path:
            path_obj = Path(str(path)).expanduser()
            if path_obj.is_symlink() or not path_obj.is_file():
                return None
            resolved = path_obj.resolve()
            if self.managed_root is not None:
                try:
                    resolved.relative_to(self.managed_root)
                except ValueError:
                    return None
            expected_size = self._media_field(value, "size", "size_bytes")
            if expected_size not in (None, ""):
                try:
                    numeric_size = int(expected_size)
                except (TypeError, ValueError):
                    return None
                if numeric_size > 0 and resolved.stat().st_size != numeric_size:
                    return None
        if not path:
            return None
        expected_checksum = str(
            self._media_field(value, "checksum", "sha256") or ""
        ).strip().lower()
        # Managed/canonical records carry either an attachment identity,
        # checksum, availability marker, MIME, or an explicit native
        # decision.  For those records verify the bytes before constructing
        # ``LocalImageInput``; a kind/filename claim must not be enough.
        declared_mime = str(
            self._media_field(value, "mime_type", "mime", "content_type") or ""
        ).strip().lower()
        explicit_available = self._media_field(value, "available")
        if explicit_available is False:
            return None
        managed_record = bool(
            self._media_field(value, "attachment_id")
            or expected_checksum
            or explicit_available is not None
            or declared_mime
            or native_flag is not None
        )
        payload: bytes | None = None
        if managed_record:
            try:
                payload = resolved.read_bytes()
            except OSError:
                return None
            actual_mime = sniff_mime(payload, str(path_obj.name), declared_mime)
            if not actual_mime.lower().startswith("image/"):
                return None
        if expected_checksum:
            try:
                if payload is None:
                    payload = resolved.read_bytes()
                digest = hashlib.sha256(payload).hexdigest()
            except OSError:
                return None
            if digest != expected_checksum:
                return None
        return LocalImageInput(str(resolved))

    @staticmethod
    def _input_preview(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()[:200]
        if isinstance(value, Mapping):
            return str(value.get("text", value.get("content", ""))).strip()[:200]
        return str(value).strip()[:200]

    # ------------------------------------------------------------------
    # SDK event normalization
    @staticmethod
    def _payload(notification: Any) -> Any:
        if isinstance(notification, Mapping):
            return notification.get("payload", notification)
        return getattr(notification, "payload", notification)

    @classmethod
    def _terminal_status(cls, notification: Any) -> str | None:
        method = str(
            notification.get("method", "") if isinstance(notification, Mapping)
            else getattr(notification, "method", "")
        ).lower()
        if "turn/completed" not in method and method not in {"turn_completed", "completed"}:
            return None
        payload = cls._payload(notification)
        turn = payload.get("turn", payload) if isinstance(payload, Mapping) else getattr(payload, "turn", payload)
        status = (
            turn.get("status") if isinstance(turn, Mapping)
            else getattr(turn, "status", turn if isinstance(turn, str) else None)
        )
        return str(getattr(status, "value", status) or "completed").lower()

    @classmethod
    def _terminal_error(cls, notification: Any) -> str:
        payload = cls._payload(notification)
        turn = payload.get("turn", payload) if isinstance(payload, Mapping) else getattr(payload, "turn", payload)
        error = turn.get("error") if isinstance(turn, Mapping) else getattr(turn, "error", None)
        if isinstance(error, Mapping):
            error = error.get("message", error)
        return str(getattr(error, "message", None) or error or "Codex turn failed")

    @classmethod
    def _completed_image_output(cls, notification: Any) -> dict[str, Any] | None:
        """Extract one successful SDK image-generation completion.

        ``ImageGenerationThreadItem`` is a tool item, but unlike command and
        reasoning diagnostics it has an explicit user-visible image output.
        Keep this parser narrow so a generic tool path/result cannot become a
        channel attachment.
        """

        method = str(
            notification.get("method", "")
            if isinstance(notification, Mapping)
            else getattr(notification, "method", "")
        ).lower()
        if method not in {"item/completed", "item_completed"}:
            return None
        payload = cls._payload(notification)
        envelope = (
            payload.get("item", payload)
            if isinstance(payload, Mapping)
            else getattr(payload, "item", payload)
        )
        item = (
            envelope.get("root", envelope)
            if isinstance(envelope, Mapping)
            else getattr(envelope, "root", envelope)
        )

        def field(value: Any, *names: str) -> Any:
            if isinstance(value, Mapping):
                for name in names:
                    if name in value:
                        return value[name]
                return None
            for name in names:
                if hasattr(value, name):
                    return getattr(value, name)
            return None

        raw_kind = str(field(item, "type") or "")
        kind = "".join(
            character for character in raw_kind.lower() if character.isalnum()
        )
        if kind != "imagegeneration":
            return None
        status = str(field(item, "status") or "").strip().lower()
        if status in {"failed", "error", "cancelled", "canceled"}:
            return {}
        item_id = field(item, "id", "item_id", "itemId")
        if not item_id:
            item_id = field(envelope, "id", "item_id", "itemId")

        def scalar_text(value: Any) -> str:
            # The pinned SDK represents absolute paths as a Pydantic RootModel;
            # ``str(AbsolutePathBuf)`` renders ``root='...'`` rather than the
            # filesystem path. Mapping-shaped protocol shims use the same root
            # envelope, so unwrap either form before crossing the SDK boundary.
            for _ in range(3):
                if isinstance(value, Mapping) and set(value) == {"root"}:
                    value = value["root"]
                    continue
                if not isinstance(value, (str, bytes, bytearray)) and hasattr(
                    value, "root"
                ):
                    value = getattr(value, "root")
                    continue
                break
            return str(value or "").strip()

        return {
            "source_item_id": str(item_id or "").strip(),
            "source_item_type": kind,
            "saved_path": scalar_text(field(item, "saved_path", "savedPath")),
            "result": scalar_text(field(item, "result")),
        }

    async def _events_for_notification(
        self,
        task: AgentTask,
        notification: Any,
        sequence: int,
        *,
        source_item_ordinal: int | None = None,
        completed_images_by_id: dict[
            str, tuple[tuple[str, str], tuple[AgentEvent, ...]]
        ]
        | None = None,
    ) -> list[AgentEvent]:
        image = self._completed_image_output(notification)
        if image is None:
            return self._events_from_notification(
                task,
                notification,
                sequence,
                source_item_ordinal=source_item_ordinal,
            )
        if not image:
            return []
        source_item_id = str(image["source_item_id"] or "").strip()
        raw_snapshot = (str(image["saved_path"]), str(image["result"]))
        if source_item_id and completed_images_by_id is not None:
            previous = completed_images_by_id.get(source_item_id)
            if previous is not None:
                previous_snapshot, previous_events = previous
                if previous_snapshot != raw_snapshot:
                    raise RuntimeError(
                        "Codex completed item identity conflicts: "
                        f"{source_item_id}"
                    )
                return list(previous_events)
        publisher = self._image_output_publisher
        if publisher is None:
            logger.warning(
                "Codex produced image item %s without a managed image publisher",
                image["source_item_id"] or source_item_ordinal,
            )
            return []
        kwargs = {
            "source_item_id": image["source_item_id"],
            "source_item_ordinal": source_item_ordinal,
            "saved_path": image["saved_path"],
            "result": image["result"],
        }
        published = publisher(
            task,
            **self._supported_kwargs(publisher, kwargs),
        )
        if inspect.isawaitable(published):
            published = await published
        if isinstance(published, Mapping):
            attachment_id = published.get(
                "attachment_id", published.get("id", "")
            )
        else:
            attachment_id = getattr(published, "attachment_id", published)
        attachment_id = str(attachment_id or "").strip()
        if not attachment_id:
            raise RuntimeError("managed image publisher returned no attachment ID")
        events = [
            AgentEvent(
                task_id=task.task_id,
                sequence=sequence,
                event_type="image_generation",
                visibility=EventVisibility.USER,
                priority=int(EventPriority.NORMAL),
                content="",
                attachments=(attachment_id,),
                execution_id=task.execution_id or None,
                source_item_id=image["source_item_id"] or None,
                source_item_type=image["source_item_type"],
                source_item_ordinal=(
                    sequence
                    if source_item_ordinal is None
                    else int(source_item_ordinal)
                ),
            )
        ]
        if source_item_id and completed_images_by_id is not None:
            completed_images_by_id[source_item_id] = (
                raw_snapshot,
                tuple(events),
            )
        return events

    @classmethod
    def _events_from_notification(
        cls,
        task: AgentTask,
        notification: Any,
        sequence: int,
        *,
        source_item_ordinal: int | None = None,
    ) -> list[AgentEvent]:
        method = str(
            notification.get("method", "") if isinstance(notification, Mapping)
            else getattr(notification, "method", "")
        ).lower()
        payload = cls._payload(notification)
        # Completed agent-message items are stable and avoid duplicate deltas.
        if method in {"item/completed", "item_completed"}:
            item_envelope = (
                payload.get("item", payload)
                if isinstance(payload, Mapping)
                else getattr(payload, "item", payload)
            )
            item = (
                item_envelope.get("root", item_envelope)
                if isinstance(item_envelope, Mapping)
                else getattr(item_envelope, "root", item_envelope)
            )
            if isinstance(item, Mapping):
                raw_kind = str(item.get("type", "") or "")
                text = str(item.get("text", item.get("content", "")) or "").strip()
                item_id = item.get("id", item.get("item_id", item.get("itemId")))
            else:
                raw_kind = str(getattr(item, "type", "") or "")
                text = str(getattr(item, "text", "") or "").strip()
                item_id = getattr(
                    item,
                    "id",
                    getattr(item, "item_id", getattr(item, "itemId", None)),
                )
            if not item_id:
                if isinstance(item_envelope, Mapping):
                    item_id = item_envelope.get(
                        "id",
                        item_envelope.get("item_id", item_envelope.get("itemId")),
                    )
                else:
                    item_id = getattr(
                        item_envelope,
                        "id",
                        getattr(
                            item_envelope,
                            "item_id",
                            getattr(item_envelope, "itemId", None),
                        ),
                    )
            # Accept only the SDK's agentMessage item kind.  Removing benign
            # spelling separators supports typed/dict wrappers without
            # broadening eligibility to generic ``message`` or diagnostic
            # item types that happen to carry text.
            kind = "".join(character for character in raw_kind.lower() if character.isalnum())
            if kind != "agentmessage":
                return []
            if not text:
                return []
            source_item_id = str(item_id).strip() if item_id is not None else ""
            return [
                AgentEvent.text_event(
                    task.task_id,
                    text,
                    sequence=sequence,
                    execution_id=task.execution_id or None,
                    event_type="agent_message",
                    source_item_id=source_item_id or None,
                    source_item_type=kind,
                    source_item_ordinal=(
                        sequence
                        if source_item_ordinal is None
                        else int(source_item_ordinal)
                    ),
                )
            ]
        # Newer SDK versions expose agent-message delta notifications.  Emit
        # them as events when no completed item is provided by the fake/client.
        if "agentmessage" in method and ("delta" in method or "text" in method):
            if isinstance(payload, Mapping):
                text = payload.get("delta") or payload.get("text") or payload.get("content")
            else:
                text = getattr(payload, "delta", None) or getattr(payload, "text", None) or getattr(payload, "content", None)
            if text:
                return [
                    AgentEvent.text_event(
                        task.task_id,
                        str(text),
                        sequence=sequence,
                        event_type="message_delta",
                        execution_id=task.execution_id or None,
                    )
                ]
        # A simple test double may yield strings or dictionaries directly.
        if isinstance(notification, str) and notification.strip():
            return [
                AgentEvent.text_event(
                    task.task_id,
                    notification.strip(),
                    sequence=sequence,
                    execution_id=task.execution_id or None,
                )
            ]
        if isinstance(notification, Mapping):
            text = notification.get("text") or notification.get("content")
            if text:
                return [
                    AgentEvent.text_event(
                        task.task_id,
                        str(text),
                        sequence=sequence,
                        execution_id=task.execution_id or None,
                    )
                ]
        return []


__all__ = [
    "CodexRuntime",
    "ThreadBinding",
    "approval_for_policy",
    "sandbox_for_policy",
    "translate_approval",
    "translate_sandbox",
]
