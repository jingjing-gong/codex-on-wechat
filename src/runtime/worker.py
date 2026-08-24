"""Background task worker for durable Agent execution."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from src.agents.base import (
    AgentEvent,
    AgentResult,
    AgentTask,
    EventPriority,
    EventVisibility,
    ReplyTarget,
    emit_if_awaitable,
)

from .dispatcher import SQLiteDispatcher, _call_compatible
from .diagnostics import log_task_started, log_task_terminal
from .identity import mailbox_conversation_id

logger = logging.getLogger(__name__)


def _consume_background_task(task: asyncio.Task[Any]) -> None:
    """Retrieve a detached shutdown task's result without blocking teardown."""

    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.debug("detached task worker exited after shutdown", exc_info=True)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _mailbox_result_content(result: Any) -> str:
    """Extract stable textual output from an Agent mailbox invocation."""

    if isinstance(result, str):
        return result.strip()
    content = _field(result, "content", None)
    if content is None:
        content = _field(result, "output", None)
    if content is None:
        content = _field(result, "result", None)
    if content:
        return str(content).strip()
    events = _field(result, "events", ()) or ()
    parts: list[str] = []
    for event in events:
        event_type = str(_field(event, "event_type", "") or "").lower()
        if event_type == "message_delta":
            continue
        value = _field(event, "content", "")
        if value:
            parts.append(str(value))
    return "".join(parts).strip()


class TaskWorker:
    """Claim tasks from SQLite and run them outside store transactions."""

    def __init__(
        self,
        store: Any,
        registry: Any | None = None,
        dispatcher: SQLiteDispatcher | None = None,
        *,
        runtime: Any | None = None,
        worker_id: str | None = None,
        poll_interval: float | None = None,
        lease_seconds: float | None = None,
        stop_timeout: float = 5.0,
    ) -> None:
        self.store = store
        if registry is None and runtime is None:
            raise ValueError("registry or runtime is required")
        self.registry = registry if registry is not None else {getattr(runtime, "agent_id", "codex"): runtime}
        self.dispatcher = dispatcher or SQLiteDispatcher(
            store,
            poll_interval=poll_interval or 0.5,
            claim_lease_seconds=lease_seconds or 60.0,
        )
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self.poll_interval = poll_interval or self.dispatcher.poll_interval
        self.lease_seconds = lease_seconds or self.dispatcher.claim_lease_seconds
        self.stop_timeout = max(0.0, float(stop_timeout))
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._active: dict[str, tuple[Any, AgentTask]] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        self._runtime_active: set[str] = set()
        self._lost_task_claims: set[str] = set()
        self.completed_count = 0
        self.failed_count = 0

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError("TaskWorker must be accessed from its owning asyncio loop")
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("TaskWorker must be accessed from its owning asyncio loop")

    async def start(self) -> None:
        self._assert_loop()
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        await self.dispatcher.start()
        self._task = asyncio.create_task(self.run_forever(), name=f"task-worker:{self.worker_id}")

    async def stop(self) -> None:
        self._assert_loop()
        self._stop_event.set()
        await self.dispatcher.stop()
        # Ask active SDK turns to stop before waiting for the worker loop.
        # Retain the exact execution fences in case a runtime ignores that
        # request and exhausts the graceful-shutdown timeout.
        shutdown_claims = tuple(
            (task_id, raw_task, task)
            for task_id, (raw_task, task) in self._active.items()
        )
        for task_id in tuple(self._active):
            try:
                await self.interrupt(task_id)
            except Exception:
                logger.debug("failed to interrupt task during worker shutdown", exc_info=True)
        task, self._task = self._task, None
        if task is not None:
            done: set[asyncio.Task[None]] = set()
            if self.stop_timeout:
                done, _pending = await asyncio.wait(
                    (task,), timeout=self.stop_timeout
                )
            if task not in done and not task.done():
                # The Agent may already have caused external side effects, so
                # never turn this into queued work. Fence the exact attempt as
                # orphaned before cancelling only the local orchestration
                # coroutine; retry remains an explicit user decision.
                await self._orphan_shutdown_claims(shutdown_claims)
                self._lost_task_claims.update(
                    task_id for task_id, _raw_task, _task in shutdown_claims
                )
                task.cancel()

                # Cancellation is cooperative. A compatibility runtime can
                # swallow CancelledError and remain blocked, so give cleanup a
                # second bounded window rather than awaiting it forever during
                # process shutdown.
                if self.stop_timeout:
                    done, _pending = await asyncio.wait(
                        (task,), timeout=self.stop_timeout
                    )
                if task not in done and not task.done():
                    logger.warning(
                        "task worker %s did not stop after cancellation",
                        self.worker_id,
                    )
                    task.add_done_callback(_consume_background_task)
            if task.done():
                _consume_background_task(task)
        heartbeats = tuple(self._heartbeat_tasks.values())
        for heartbeat in heartbeats:
            heartbeat.cancel()
        if heartbeats:
            await asyncio.gather(*heartbeats, return_exceptions=True)
        self._heartbeat_tasks.clear()

    async def run_forever(self) -> None:
        self._assert_loop()
        while not self._stop_event.is_set() and not self.dispatcher.stopping:
            try:
                did_work = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("task worker iteration failed")
                did_work = False
            if not did_work:
                await self.dispatcher.wait(self.poll_interval)

    run = run_forever

    async def run_once(self) -> bool:
        """Claim and execute at most one task; return whether work was found."""

        self._assert_loop()
        claim_started = time.monotonic()
        claimed = await self.dispatcher.claim(self.worker_id)
        if claimed is None:
            return False
        raw_task, claim_token = self._extract_claim(claimed)
        task = self._task_from_claim(raw_task, claim_token)
        task_id = str(task.task_id)
        lock = self.dispatcher.lock_for(task)
        async with lock:
            execution_id = task.execution_id or f"exec-{uuid.uuid4().hex}"
            task = task.with_execution(execution_id)
            self._active[task_id] = (raw_task, task)
            heartbeat = self._start_heartbeat(
                task_id,
                claim_token,
                lease_deadline=claim_started + self.lease_seconds,
            )
            try:
                running_changed = await self._mark_running(task, claim_token)
                if running_changed is False:
                    # A claim may have expired, been cancelled, or been
                    # superseded before the worker reached this transition.
                    # Never execute after the ownership/state transition was
                    # rejected: doing so would run work another worker owns.
                    current = await self._current_task(task_id)
                    current_state = _field(current, "state", "") if current is not None else ""
                    current_state = getattr(current_state, "value", current_state)
                    if str(current_state).lower() in {"cancel_requested", "cancelled", "canceled"}:
                        # A queued claim can race with a cancellation request.
                        # Only finalize it when the durable row still carries
                        # this worker's token; otherwise the cancellation (or
                        # terminal transition) has already been handled by a
                        # different owner.
                        current_token = _field(current, "claim_token", None) if current is not None else None
                        if current_token == claim_token and str(current_state).lower() == "cancel_requested":
                            try:
                                await self._finish(
                                    task,
                                    AgentResult(
                                        task_id=task_id,
                                        execution_id=execution_id,
                                        status="cancelled",
                                        interrupted=True,
                                    ),
                                    claim_token,
                                )
                            except Exception:
                                logger.debug("could not finalize raced cancellation for %s", task_id, exc_info=True)
                        return True
                    return True
                attempt = _field(raw_task, "attempts", _field(raw_task, "attempt", None))
                runtime_started_at = time.monotonic()
                log_task_started(
                    logger,
                    task,
                    worker_id=self.worker_id,
                    attempt=attempt,
                )
                try:
                    runtime = self._runtime_for(task.agent_id)
                except Exception as exc:
                    # A persisted task can outlive a disabled/unregistered
                    # Agent after restart.  No external turn was started, so
                    # fail it durably under the current claim instead of
                    # leaving a running row until lease expiry.
                    result = AgentResult(
                        task_id=task.task_id,
                        execution_id=execution_id,
                        status="failed",
                        error=str(exc) or exc.__class__.__name__,
                    )
                    result = self._with_failure_notice(task, result)
                    await self._finish(task, result, claim_token)
                    log_task_terminal(
                        logger,
                        task,
                        result,
                        worker_id=self.worker_id,
                        attempt=attempt,
                        duration_ms=max(
                            0, int((time.monotonic() - runtime_started_at) * 1000)
                        ),
                    )
                    self.failed_count += 1
                    return True
                events: list[AgentEvent] = []
                event_offset = await self._event_offset(task.task_id)
                emitted_sequences: set[int] = set()
                next_event_sequence = event_offset

                async def emit(event: AgentEvent) -> None:
                    nonlocal next_event_sequence
                    # Runtime-local sequences usually start at zero for each
                    # execution.  Persisted task events are task-global, so
                    # offset retries and repair accidental duplicate or
                    # out-of-order numbers.
                    event = self._coerce_event(task, event)
                    try:
                        requested_sequence = event_offset + max(0, int(event.sequence))
                    except (TypeError, ValueError):
                        requested_sequence = next_event_sequence
                    sequence = max(requested_sequence, next_event_sequence)
                    while sequence in emitted_sequences:
                        sequence += 1
                    emitted_sequences.add(sequence)
                    next_event_sequence = sequence + 1
                    event = replace(
                        event,
                        sequence=sequence,
                        execution_id=event.execution_id or task.execution_id,
                        # Preserve the SDK-independent event identity.  The
                        # store uses it as the immutable envelope key, so a
                        # callback replay (and the same event repeated in a
                        # terminal result) resolves to one row instead of
                        # creating a second projection.
                        event_id=event.event_id,
                    )
                    event = replace(event, event_id=self._stable_event_id(event))
                    events.append(event)
                    await self._persist_event(event, claim_token=claim_token)

                # A failed renewal before the runtime starts means durable
                # ownership has already moved.  Do not begin external work
                # under a stale claim.
                if task_id in self._lost_task_claims:
                    return True

                # Cancellation can commit after ``mark_task_running`` while
                # event history is being loaded. Re-read at the last durable
                # boundary before entering the Agent runtime so a turn that
                # has never started cannot begin after cancel_requested.
                current = await self._current_task(task_id)
                # The lease heartbeat runs independently while the durable
                # state read above is in flight.  It can discover an expired or
                # transferred claim after the earlier check but before this
                # coroutine resumes.  Recheck at the actual runtime boundary so
                # work is never started under ownership already known to be
                # lost.
                if task_id in self._lost_task_claims:
                    return True
                current_state = self._status_value(
                    _field(current, "state", "")
                ) if current is not None else ""
                if current_state in {"cancel_requested", "cancelled", "canceled"}:
                    if current_state == "cancel_requested":
                        try:
                            await self._finish(
                                task,
                                AgentResult(
                                    task_id=task_id,
                                    execution_id=execution_id,
                                    status="cancelled",
                                    interrupted=True,
                                ),
                                claim_token,
                            )
                        except Exception:
                            # The ownership/lease may have changed with the
                            # cancellation. Store fencing is authoritative.
                            logger.debug(
                                "could not finalize pre-start cancellation for %s",
                                task_id,
                                exc_info=True,
                            )
                    return True
                self._runtime_active.add(task_id)
                try:
                    try:
                        result = await runtime.run(task, emit)
                        if not isinstance(result, AgentResult):
                            result = self._coerce_result(task, result, events)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.exception("Agent task %s failed", task_id)
                        # Losing a child process after an assignment crossed
                        # the runtime boundary is not an ordinary model
                        # failure. The external turn may already have caused
                        # effects or emitted an unacknowledged result, so the
                        # durable attempt must require explicit review/retry
                        # instead of being presented as a safely failed turn.
                        uncertain = bool(
                            getattr(exc, "execution_uncertain", False)
                        )
                        result = AgentResult(
                            task_id=task_id,
                            execution_id=execution_id,
                            status="orphaned" if uncertain else "failed",
                            error=str(exc) or exc.__class__.__name__,
                            events=tuple(events),
                        )
                finally:
                    self._runtime_active.discard(task_id)
                # A token can remain on the row briefly after its lease has
                # expired.  Once ownership is lost or can no longer be
                # confirmed, do not terminalize an externally uncertain turn
                # merely because the runtime returned after interruption.
                if task_id in self._lost_task_claims:
                    return True
                result_identity_valid = True
                try:
                    result = self._normalize_result(task, result)
                except (TypeError, ValueError) as exc:
                    logger.error("Agent task %s returned an invalid result: %s", task_id, exc)
                    result_identity_valid = False
                    result = AgentResult(
                        task_id=task_id,
                        execution_id=execution_id,
                        status="failed",
                        error=str(exc),
                        events=tuple(events),
                    )
                # A cancellation request is durable and wins a race with a
                # turn that happened to finish just as ``/cancel`` arrived.
                current = await self._current_task(task_id)
                current_state = _field(current, "state", "") if current is not None else ""
                current_state = getattr(current_state, "value", current_state)
                if (
                    current is not None
                    and self._status_value(current_state) == "cancel_requested"
                    and self._status_value(result.status) not in {"interrupted", "cancelled", "canceled"}
                ):
                    result = AgentResult(
                        task_id=task_id,
                        execution_id=result.execution_id or execution_id,
                        status="interrupted",
                        content=result.content,
                        error=result.error,
                        events=result.events,
                        interrupted=True,
                        thread_id=result.thread_id,
                        usage=getattr(result, "usage", {}),
                        metadata=getattr(result, "metadata", {}),
                    )
                merged_events = self._merge_events(
                    task,
                    events,
                    result.events,
                    event_offset=event_offset,
                )
                if merged_events != tuple(result.events):
                    result = AgentResult(
                        task_id=task_id,
                        execution_id=result.execution_id or execution_id,
                        status=result.status,
                        content=result.content,
                        error=result.error,
                        events=merged_events,
                        interrupted=result.interrupted,
                        thread_id=result.thread_id,
                        usage=getattr(result, "usage", {}),
                        metadata=getattr(result, "metadata", {}),
                    )
                # A control command may have set cancel_requested while the
                # SDK turn was winding down.  The durable state wins over a
                # late successful-looking SDK result; do not report work as
                # completed after an explicit interruption request.
                getter = getattr(self.store, "get_task", None)
                if getter is not None:
                    current = await _call_compatible(getter, task_id)
                    if (
                        self._status_value(_field(current, "state", "")) == "cancel_requested"
                        and self._status_value(result.status) not in {"interrupted", "cancelled", "canceled"}
                    ):
                        result = AgentResult(
                            task_id=task_id,
                            execution_id=result.execution_id or execution_id,
                            status="interrupted",
                            content=result.content,
                            error=result.error,
                            events=result.events,
                            interrupted=True,
                            thread_id=result.thread_id,
                            usage=getattr(result, "usage", {}),
                            metadata=getattr(result, "metadata", {}),
                        )
                if result_identity_valid:
                    result = self._with_failure_notice(task, result)
                await self._finish(task, result, claim_token)
                log_task_terminal(
                    logger,
                    task,
                    result,
                    worker_id=self.worker_id,
                    attempt=attempt,
                    duration_ms=max(
                        0, int((time.monotonic() - runtime_started_at) * 1000)
                    ),
                )
                if self._status_value(result.status) == "completed":
                    self.completed_count += 1
                else:
                    self.failed_count += 1
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    self._heartbeat_tasks.pop(task_id, None)
                    try:
                        await heartbeat
                    except asyncio.CancelledError:
                        pass
                self._active.pop(task_id, None)
                self._runtime_active.discard(task_id)
                self._lost_task_claims.discard(task_id)
        return True

    claim_and_run = run_once

    async def interrupt(self, task_id: str) -> bool:
        """Interrupt a task currently owned by this worker."""

        self._assert_loop()
        active = self._active.get(str(task_id))
        if active is None:
            return False
        _, task = active
        runtime = self._runtime_for(task.agent_id)
        return bool(await runtime.interrupt(str(task_id)))

    def active_tasks(self) -> tuple[str, ...]:
        self._assert_loop()
        return tuple(self._active)

    def _runtime_for(self, agent_id: str) -> Any:
        require = getattr(self.registry, "require", None) or getattr(self.registry, "runtime_for", None)
        if require is not None:
            return require(agent_id)
        if isinstance(self.registry, Mapping):
            return self.registry[agent_id]
        get = getattr(self.registry, "get", None)
        if get is not None:
            runtime = get(agent_id)
            if runtime is not None:
                return runtime
        raise KeyError(f"unknown Agent runtime: {agent_id}")

    # ------------------------------------------------------------------ store integration
    def _extract_claim(self, claimed: Any) -> tuple[Any, str | None]:
        if isinstance(claimed, tuple) and len(claimed) >= 2:
            first, second = claimed[0], claimed[-1]
            # A tuple of (task, token) is the common claim result.  Avoid
            # treating a tuple-valued task itself as a claim envelope.
            if not isinstance(first, (str, bytes, int, float)):
                return first, str(second) if second is not None else None
        task = _field(claimed, "task", None)
        if task is not None:
            return task, _field(claimed, "claim_token", None) or _field(claimed, "token", None)
        return claimed, _field(claimed, "claim_token", None) or _field(claimed, "token", None)

    def _task_from_claim(self, raw_task: Any, claim_token: str | None) -> AgentTask:
        if isinstance(raw_task, AgentTask):
            task = raw_task
        else:
            values: dict[str, Any] = {}
            names = set(AgentTask.__dataclass_fields__)
            if isinstance(raw_task, Mapping):
                values = {name: raw_task[name] for name in names if name in raw_task}
            else:
                values = {
                    name: getattr(raw_task, name)
                    for name in names
                    if hasattr(raw_task, name)
                }
            if "task_id" not in values:
                alias_task_id = _field(raw_task, "id", None)
                if alias_task_id is not None:
                    values["task_id"] = alias_task_id
            # SQLite rows often use ``input_text`` or ``payload``.
            if "inputs" not in values:
                for alias in ("input", "input_text", "payload", "prompt"):
                    candidate = _field(raw_task, alias, None)
                    if candidate is not None:
                        values["inputs"] = candidate
                        break
            task = AgentTask.from_record(values)
        # Normalize store/channel ReplyTarget variants to the contract type.
        target = getattr(task, "reply_target", None)
        if target is not None and not hasattr(target, "as_dict"):
            target_data = {
                name: getattr(target, name, None)
                for name in ("channel", "bot_id", "external_user_id", "session_id", "source_message_id", "source_sequence", "context_token")
            }
            from src.agents.base import ReplyTarget

            task = AgentTask.from_record(
                task,
                reply_target=ReplyTarget(**target_data),
            )
        if claim_token and isinstance(task.metadata, Mapping):
            metadata = dict(task.metadata)
            metadata.setdefault("claim_token", claim_token)
            # Preserve immutable task snapshot while making the token available
            # to completion projections; token is never sent to an Agent.
            task = AgentTask.from_record(task, metadata=metadata)
        return task

    async def _mark_running(self, task: AgentTask, claim_token: str | None) -> Any:
        method = getattr(self.store, "mark_task_running", None) or getattr(self.store, "start_task", None)
        if method is None:
            return None
        return await _call_compatible(
            method,
            task.task_id,
            execution_id=task.execution_id,
            claim_token=claim_token,
            worker_id=self.worker_id,
        )

    async def _persist_event(self, event: AgentEvent, *, claim_token: str | None) -> Any:
        method = getattr(self.store, "append_task_event", None) or getattr(self.store, "append_event", None)
        if method is None:
            return None
        event_id = self._stable_event_id(event)
        return await _call_compatible(
            method,
            event.task_id,
            event=event,
            event_id=event_id,
            idempotency_key=event_id,
            sequence=event.sequence,
            event_type=event.event_type,
            visibility=getattr(event.visibility, "value", event.visibility),
            priority=event.priority,
            content=event.content,
            attachments=getattr(event, "attachments", ()),
            destination_agent_id=getattr(event, "destination_agent_id", None),
            request_id=getattr(event, "request_id", None),
            reply_to_id=getattr(event, "reply_to_id", None),
            causation_id=getattr(event, "causation_id", None),
            source_item_id=getattr(event, "source_item_id", None),
            source_item_type=getattr(event, "source_item_type", None),
            source_item_ordinal=getattr(event, "source_item_ordinal", None),
            execution_id=event.execution_id,
            claim_token=claim_token,
            # A completed Agent-message item is itself the durable user reply
            # boundary.  Project it in the same transaction as its event;
            # progress/status events remain deferred or internal, and task
            # completion only synthesizes compatibility text when no item
            # already represents the terminal result.
            defer_user_projection=not self._is_completed_user_item(event),
        )

    @staticmethod
    def _is_completed_user_item(event: AgentEvent) -> bool:
        """Return whether ``event`` is a stable completed user item.

        Merely carrying source-item metadata is insufficient: command,
        reasoning, plan, and status items can contain diagnostic text.  Only
        the normalized Codex ``agentMessage`` boundary or an explicitly
        promoted ``imageGeneration`` attachment may be projected while a turn
        is still running.  A stable item identity is also required so callback
        replay cannot allocate another logical reply.
        """

        source_type = "".join(
            character
            for character in str(getattr(event, "source_item_type", "") or "").lower()
            if character.isalnum()
        )
        event_type = "".join(
            character
            for character in str(getattr(event, "event_type", "") or "").lower()
            if character.isalnum()
        )
        visibility = str(
            getattr(
                getattr(event, "visibility", EventVisibility.USER),
                "value",
                getattr(event, "visibility", EventVisibility.USER),
            )
        ).lower()
        source_item_id = str(getattr(event, "source_item_id", "") or "").strip()
        source_item_ordinal = getattr(event, "source_item_ordinal", None)
        content = str(getattr(event, "content", "") or "").strip()
        attachments = tuple(getattr(event, "attachments", ()) or ())
        stable_text = bool(
            source_type == "agentmessage"
            and event_type in {"agentmessage", "message"}
            and (content or attachments)
        )
        # Keep this deliberately specific. Generic tool/command paths must not
        # become channel files; only the adapter's managed image event is an
        # eligible attachment-only completed item.
        stable_image = bool(
            source_type == "imagegeneration"
            and event_type == "imagegeneration"
            and attachments
            and not content
        )
        return bool(
            visibility == EventVisibility.USER.value
            and not getattr(event, "destination_agent_id", None)
            and (source_item_id or source_item_ordinal is not None)
            and (stable_text or stable_image)
        )

    async def _finish(self, task: AgentTask, result: AgentResult, claim_token: str | None) -> Any:
        if result.thread_id:
            setter = getattr(self.store, "set_task_thread", None) or getattr(self.store, "set_thread_binding", None)
            if setter is not None:
                await _call_compatible(
                    setter,
                    task.task_id,
                    thread_id=result.thread_id,
                    claim_token=claim_token,
                )
        method = getattr(self.store, "finish_task", None) or getattr(self.store, "complete_task", None)
        if method is not None:
            return await _call_compatible(
                method,
                task.task_id,
                status=result.status,
                result=result.content,
                output=result.content,
                error=result.error,
                events=result.events,
                execution_id=result.execution_id or task.execution_id,
                thread_id=result.thread_id,
                claim_token=claim_token,
                worker_id=self.worker_id,
            )
        transition = getattr(self.store, "transition_task", None)
        if transition is not None:
            return await _call_compatible(
                transition,
                task.task_id,
                result.status,
                claim_token=claim_token,
                error=result.error,
                last_error=result.error,
                result=result.content,
            )
        raise AttributeError("store must implement finish_task() or transition_task()")

    async def _renew(self, task_id: str, claim_token: str | None) -> Any:
        method = getattr(self.store, "renew_task_lease", None) or getattr(self.store, "renew_task_claim", None)
        if method is None:
            return None
        return await _call_compatible(
            method,
            task_id,
            claim_token=claim_token,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )

    async def _current_task(self, task_id: str) -> Any | None:
        method = getattr(self.store, "get_task", None)
        if method is None:
            return None
        # This read fences the final boundary before entering the Agent runtime.
        # A store failure is not evidence that the task is absent: cancellation
        # or ownership may have changed while the row was unavailable.  Let the
        # iteration fail so recovery can reconcile the claimed attempt without
        # starting externally visible work under an unverified state.
        return await _call_compatible(method, task_id)

    async def _orphan_shutdown_claims(
        self,
        claims: tuple[tuple[str, Any, AgentTask], ...],
    ) -> None:
        """Fence timed-out executions as unknown without requeueing them."""

        transition = getattr(self.store, "transition_task", None)
        if transition is None:
            return
        for task_id, raw_task, task in claims:
            metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
            claim_token = metadata.get("claim_token") or _field(
                raw_task, "claim_token", None
            )
            if not claim_token:
                continue
            try:
                await _call_compatible(
                    transition,
                    task_id,
                    "orphaned",
                    from_states=("claimed", "running", "cancel_requested"),
                    claim_token=claim_token,
                    execution_id=task.execution_id or None,
                    last_error="worker shutdown timed out after interrupt request",
                )
            except Exception:
                # A concurrent terminal write may have won.  Ownership-fenced
                # transitions fail closed, so logging is sufficient here.
                logger.warning(
                    "could not orphan timed-out task %s during worker shutdown",
                    task_id,
                    exc_info=True,
                )

    async def _event_offset(self, task_id: str) -> int:
        method = getattr(self.store, "list_task_events", None) or getattr(self.store, "get_task_events", None)
        if method is None:
            next_method = getattr(self.store, "next_event_sequence", None)
            if next_method is not None:
                try:
                    return int(await _call_compatible(next_method, task_id))
                except Exception:
                    pass
            return 0
        try:
            values = await _call_compatible(method, task_id, limit=100000)
            sequences = [int(_field(item, "sequence", -1)) for item in (values or ())]
            return max(sequences, default=-1) + 1
        except Exception:
            return 0

    def _start_heartbeat(
        self,
        task_id: str,
        claim_token: str | None,
        *,
        lease_deadline: float | None = None,
    ) -> asyncio.Task[None] | None:
        if not claim_token:
            return None
        # Short leases used by tests and embedded deployments still need at
        # least one renewal opportunity before their confirmed deadline.
        interval = max(0.01, self.lease_seconds / 3)
        confirmed_until = (
            float(lease_deadline)
            if lease_deadline is not None
            else time.monotonic() + self.lease_seconds
        )

        async def heartbeat() -> None:
            nonlocal confirmed_until

            async def lose_ownership() -> None:
                self._lost_task_claims.add(task_id)
                logger.warning("task lease ownership lost for %s", task_id)
                if task_id in self._runtime_active:
                    try:
                        await self.interrupt(task_id)
                    except Exception:
                        logger.debug(
                            "failed to interrupt task after lease loss for %s",
                            task_id,
                            exc_info=True,
                        )

            try:
                while task_id in self._active:
                    remaining = confirmed_until - time.monotonic()
                    if remaining <= 0:
                        await lose_ownership()
                        return
                    await asyncio.sleep(min(interval, remaining))
                    if task_id in self._active:
                        # An expired claim cannot be revived. Check the last
                        # confirmed deadline before asking the store to renew;
                        # otherwise a delayed event loop could renew after
                        # another worker has become eligible to recover it.
                        if time.monotonic() >= confirmed_until:
                            await lose_ownership()
                            return
                        renewal_started = time.monotonic()
                        renewed_until = renewal_started + self.lease_seconds
                        try:
                            renewed = await self._renew(task_id, claim_token)
                        except Exception:
                            logger.debug(
                                "task lease renewal failed for %s",
                                task_id,
                                exc_info=True,
                            )
                            # Retry a transient failure while the last confirmed
                            # lease is still live.  Past that deadline, another
                            # worker may recover the row even if this process
                            # never observed an explicit token mismatch.
                            if time.monotonic() >= confirmed_until:
                                await lose_ownership()
                                return
                            continue
                        if renewed is False:
                            # The durable token is no longer ours. Record that
                            # fence even when the turn has not started yet.
                            # Only interrupt while ``runtime.run`` is actually
                            # in flight: a terminal commit clears the lease
                            # before this coroutine is cancelled and must not
                            # look like a reason to interrupt a finished turn.
                            await lose_ownership()
                            return
                        # SQLite computes the renewed deadline when the call
                        # starts.  If the call itself outlived that interval,
                        # the returned success no longer proves ownership.
                        if time.monotonic() >= renewed_until:
                            await lose_ownership()
                            return
                        confirmed_until = renewed_until
            except asyncio.CancelledError:
                return

        task = asyncio.create_task(heartbeat(), name=f"task-heartbeat:{task_id}")
        self._heartbeat_tasks[task_id] = task
        return task

    @staticmethod
    def _status_value(value: Any) -> str:
        value = getattr(value, "value", value)
        normalized = str(value or "completed").strip().lower()
        return {
            "success": "completed",
            "error": "failed",
            "canceled": "cancelled",
        }.get(normalized, normalized)

    @classmethod
    def _normalize_result(cls, task: AgentTask, result: AgentResult) -> AgentResult:
        """Bind a runtime result to the current immutable execution."""

        if result.task_id and str(result.task_id) != str(task.task_id):
            raise ValueError("Agent result task_id does not match the running task")
        if (
            result.execution_id
            and task.execution_id
            and str(result.execution_id) != str(task.execution_id)
        ):
            raise ValueError(
                "Agent result execution_id does not match the running execution"
            )
        status = cls._status_value(result.status)
        return AgentResult(
            task_id=str(task.task_id),
            execution_id=task.execution_id or result.execution_id,
            status=status,
            content=str(result.content or ""),
            error=result.error,
            events=tuple(result.events or ()),
            interrupted=bool(
                result.interrupted or status in {"interrupted", "cancelled"}
            ),
            thread_id=result.thread_id,
            usage=getattr(result, "usage", {}),
            metadata=getattr(result, "metadata", {}),
        )

    @classmethod
    def _with_failure_notice(
        cls,
        task: AgentTask,
        result: AgentResult,
    ) -> AgentResult:
        """Add one explicit safe terminal reply to an owned failed attempt.

        Raw runtime/provider errors remain internal because they may contain
        endpoints, credentials, or other implementation details.  A worker
        calls this helper only after the result identity has been validated;
        fabricated cross-task results therefore remain delivery-silent.
        Nothing is retried or cleared automatically because a disconnected
        turn may already have produced external side effects.  Completed
        Agent-message items can be useful progress, but they do not prove that
        the user was told the turn ultimately failed.
        """

        if cls._status_value(result.status) != "failed":
            return result
        events = tuple(result.events or ())
        task_id = str(task.task_id)
        # A process-isolated runtime can fail before returning its binding even
        # though the immutable task was already pinned to a provider thread.
        # Either identity means retry will resume the same context.
        if result.thread_id or task.thread_id:
            notice = (
                f"task failed: {task_id}\n"
                "check /tasks before retrying. /retry reuses the same context; "
                "/clear starts fresh."
            )
        else:
            notice = (
                f"task failed: {task_id}\n"
                "check /tasks before retrying or sending a new prompt."
            )
        # A provider/runtime event is untrusted even when it labels itself an
        # error.  Recognize only this worker protocol's exact fixed text; all
        # arbitrary diagnostics remain internal.
        for event in events:
            visibility = getattr(
                _field(event, "visibility", EventVisibility.USER),
                "value",
                _field(event, "visibility", EventVisibility.USER),
            )
            event_type = str(_field(event, "event_type", "") or "")
            if (
                str(visibility) == EventVisibility.USER.value
                and not _field(event, "destination_agent_id", None)
                and event_type == "failure_notice"
                and str(_field(event, "content", "") or "") == notice
                and not tuple(_field(event, "attachments", ()) or ())
            ):
                return result
        next_sequence = 0
        for event in events:
            try:
                next_sequence = max(
                    next_sequence,
                    int(_field(event, "sequence", -1)) + 1,
                )
            except (TypeError, ValueError):
                continue
        notice_event = AgentEvent.text_event(
            task_id,
            notice,
            sequence=next_sequence,
            visibility=EventVisibility.USER,
            priority=int(EventPriority.NORMAL),
            execution_id=result.execution_id or task.execution_id or None,
            event_type="failure_notice",
        )
        return replace(
            result,
            content=(result.content if str(result.content or "").strip() else notice),
            events=(*events, notice_event),
        )

    @staticmethod
    def _coerce_event(task: AgentTask, event: Any) -> AgentEvent:
        """Normalize event-shaped runtime values to the public contract."""

        if isinstance(event, AgentEvent):
            normalized = event
        else:
            if isinstance(event, Mapping):
                source = dict(event)
            else:
                source = {
                    name: getattr(event, name)
                    for name in AgentEvent.__dataclass_fields__
                    if hasattr(event, name)
                }
                if not source and event is not None:
                    source["content"] = str(event)
            if "content" not in source and "text" in source:
                source["content"] = source["text"]
            attachments = source.get("attachments", ())
            if attachments is None:
                attachments = ()
            elif isinstance(attachments, (str, bytes, bytearray)):
                attachments = (attachments,)
            try:
                sequence = int(source.get("sequence", 0) or 0)
            except (TypeError, ValueError):
                sequence = 0
            priority = getattr(source.get("priority", 1), "value", source.get("priority", 1))
            try:
                priority = int(priority)
            except (TypeError, ValueError):
                priority = 1
            visibility = getattr(source.get("visibility", "user"), "value", source.get("visibility", "user"))
            event_type = getattr(
                source.get("event_type", source.get("type", "message")),
                "value",
                source.get("event_type", source.get("type", "message")),
            )
            normalized = AgentEvent(
                task_id=str(source.get("task_id") or task.task_id),
                sequence=sequence,
                event_type=str(event_type or "message"),
                visibility=visibility,
                priority=priority,
                content=str(source.get("content", "") or ""),
                attachments=tuple(attachments),
                created_at=source.get("created_at") or task.created_at,
                execution_id=source.get("execution_id"),
                metadata=source.get("metadata", {}) or {},
                destination_agent_id=source.get("destination_agent_id"),
                request_id=source.get("request_id"),
                reply_to_id=source.get("reply_to_id"),
                causation_id=source.get("causation_id"),
                event_id=str(source.get("event_id", source.get("message_id", "")) or ""),
                source_item_id=source.get("source_item_id"),
                source_item_type=source.get("source_item_type"),
                source_item_ordinal=source.get("source_item_ordinal"),
            )
        if normalized.task_id and str(normalized.task_id) != str(task.task_id):
            raise ValueError("Agent event task_id does not match the running task")
        if (
            normalized.execution_id
            and task.execution_id
            and str(normalized.execution_id) != str(task.execution_id)
        ):
            raise ValueError("Agent event execution_id does not match the running execution")
        try:
            normalized_sequence = int(normalized.sequence)
        except (TypeError, ValueError):
            normalized_sequence = 0
        return replace(
            normalized,
            task_id=str(task.task_id),
            sequence=normalized_sequence,
            execution_id=normalized.execution_id or task.execution_id,
        )

    @staticmethod
    def _event_semantic_key(event: AgentEvent) -> tuple[Any, ...]:
        visibility = getattr(event.visibility, "value", event.visibility)
        priority = getattr(event.priority, "value", event.priority)
        return (
            str(event.event_type),
            str(visibility),
            int(priority),
            str(event.content),
            tuple(repr(item) for item in event.attachments),
            event.destination_agent_id,
            event.request_id,
            event.reply_to_id,
            event.causation_id,
            event.source_item_id,
            event.source_item_type,
            event.source_item_ordinal,
        )

    def _merge_events(
        self,
        task: AgentTask,
        emitted: list[AgentEvent],
        returned: Any,
        *,
        event_offset: int,
    ) -> tuple[AgentEvent, ...]:
        """Merge callback and result-only events without duplicating replays."""

        merged = list(emitted)
        callback_remaining = Counter(self._event_semantic_key(item) for item in emitted)
        identities = {item.event_id for item in emitted if item.event_id}
        used_sequences = {int(item.sequence) for item in emitted}
        next_sequence = max(used_sequences, default=event_offset - 1) + 1
        had_callbacks = bool(emitted)

        for raw_event in returned or ():
            event = self._coerce_event(task, raw_event)
            if event.event_id and event.event_id in identities:
                continue
            semantic = self._event_semantic_key(event)
            if callback_remaining[semantic] > 0:
                callback_remaining[semantic] -= 1
                continue

            if had_callbacks:
                sequence = next_sequence
            else:
                try:
                    local_sequence = max(0, int(event.sequence))
                except (TypeError, ValueError):
                    local_sequence = 0
                sequence = event_offset + local_sequence
                sequence = max(sequence, next_sequence)
            while sequence in used_sequences:
                sequence += 1
            used_sequences.add(sequence)
            next_sequence = sequence + 1
            event = replace(
                event,
                sequence=sequence,
                execution_id=event.execution_id or task.execution_id,
                event_id=event.event_id,
            )
            event = replace(event, event_id=self._stable_event_id(event))
            merged.append(event)
            if event.event_id:
                identities.add(event.event_id)
        return tuple(merged)

    @classmethod
    def _stable_event_id(cls, event: AgentEvent) -> str:
        """Return an immutable identity for one task/execution event.

        Runtime-provided IDs are intentionally ignored by the worker because
        SDK turns commonly regenerate them on replay.  The semantic payload is
        included alongside the task-global sequence so a conflicting replay
        cannot silently overwrite the original envelope.
        """

        task_id = str(getattr(event, "task_id", "") or "")
        execution_id = str(getattr(event, "execution_id", "") or "")
        try:
            sequence = int(getattr(event, "sequence", 0) or 0)
        except (TypeError, ValueError):
            sequence = 0
        semantic = repr(cls._event_semantic_key(event))
        identity = "\x1f".join((task_id, execution_id, str(sequence), semantic))
        return str(uuid.uuid5(uuid.NAMESPACE_URL, "codex-task-event:" + identity))

    @staticmethod
    def _coerce_result(task: AgentTask, result: Any, events: list[AgentEvent]) -> AgentResult:
        if result is None:
            return AgentResult(task_id=task.task_id, execution_id=task.execution_id, events=tuple(events))
        if isinstance(result, str):
            return AgentResult(task_id=task.task_id, execution_id=task.execution_id, content=result, events=tuple(events))
        if isinstance(result, Mapping):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status=str(result.get("status", "completed")),
                content=str(result.get("content", result.get("output", "")) or ""),
                error=result.get("error"),
                events=tuple(result.get("events", events) or ()),
                interrupted=bool(result.get("interrupted", False)),
                thread_id=result.get("thread_id"),
            )
        # Accept the store-layer AgentResult shape (``output``/``result``)
        # without importing that model into the worker.
        if hasattr(result, "status") or hasattr(result, "output") or hasattr(result, "error"):
            output = getattr(result, "content", None)
            if output is None:
                output = getattr(result, "output", getattr(result, "result", ""))
            raw_events = getattr(result, "events", None) or tuple(events)
            return AgentResult(
                task_id=task.task_id,
                execution_id=getattr(result, "execution_id", None) or task.execution_id,
                status=str(getattr(result, "status", "completed")),
                content=str(output or ""),
                error=getattr(result, "error", None),
                events=tuple(raw_events),
                interrupted=bool(getattr(result, "interrupted", False)),
                thread_id=getattr(result, "thread_id", None),
            )
        return AgentResult(task_id=task.task_id, execution_id=task.execution_id, content=str(result or ""), events=tuple(events))


class AgentMailboxWorker:
    """Process durable Agent-to-Agent mailbox records.

    Mailbox processing is intentionally separate from :class:`TaskWorker` and
    the WeChat delivery worker.  A claimed mailbox item is handed to a public
    runtime hook (``receive_agent_message``/``handle_mailbox``) when one is
    available; otherwise the normal Agent ``run`` contract receives an
    internal task snapshot.  No user outbox projection is created here.
    """

    def __init__(
        self,
        store: Any,
        registry: Any,
        destination_agent_id: str,
        *,
        worker_id: str = "agent-mailbox",
        handler: Any | None = None,
        reply_handler: Any | None = None,
        poll_interval: float = 0.5,
        lease_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.registry = registry
        self.destination_agent_id = str(destination_agent_id)
        self.worker_id = worker_id
        self.poll_interval = max(0.05, float(poll_interval))
        self.lease_seconds = max(0.1, float(lease_seconds))
        self.handler = handler
        # Called as ``reply_handler(mailbox_item, runtime_result)`` after the
        # destination Agent has produced a result.  The callback owns the
        # durable correlation details; this worker never sends to a channel.
        self.reply_handler = reply_handler
        self._stop = asyncio.Event()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._lease_heartbeats: dict[str, asyncio.Task[None]] = {}
        self._lost_mailbox_claims: set[str] = set()
        self._mailbox_claim_loss_events: dict[str, asyncio.Event] = {}

    def _assert_loop(self) -> None:
        """Keep mailbox state and its asyncio.Event on one event loop."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError(
                    "AgentMailboxWorker must be accessed from its owning asyncio loop"
                )
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                "AgentMailboxWorker must be accessed from its owning asyncio loop"
            )

    def stop(self) -> None:
        """Request the mailbox loop to stop from any calling thread."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._owner_loop is not None and self._owner_loop is not loop:
            if self._owner_loop.is_running():
                self._owner_loop.call_soon_threadsafe(self._stop.set)
                return
            # The owner loop may already have stopped during process
            # teardown.  Setting an asyncio.Event is still harmless and keeps
            # shutdown idempotent instead of raising from ``_assert_loop``.
            self._stop.set()
            return
        self._assert_loop()
        self._stop.set()

    def _runtime(self) -> Any:
        require = getattr(self.registry, "require", None)
        if require is not None:
            return require(self.destination_agent_id)
        if isinstance(self.registry, Mapping):
            return self.registry[self.destination_agent_id]
        getter = getattr(self.registry, "get", None)
        if getter is not None:
            runtime = getter(self.destination_agent_id)
            if runtime is not None:
                return runtime
        raise KeyError(f"unknown Agent runtime: {self.destination_agent_id}")

    async def run_once(self) -> int:
        self._assert_loop()
        claim_method = getattr(self.store, "claim_mailbox", None) or getattr(
            self.store, "claim_next_mailbox", None
        )
        if claim_method is None:
            return 0
        claim_started = time.monotonic()
        claimed = await _call_compatible(
            claim_method,
            self.destination_agent_id,
            self.worker_id,
            limit=1,
            lease_seconds=self.lease_seconds,
        )
        if claimed is None:
            return 0
        envelope_token: str | None = None
        # Some lightweight stores mirror TaskWorker and return ``(item,
        # claim_token)`` instead of a one-item list.  Distinguish that shape
        # from a tuple containing two mailbox rows before iterating.
        if (
            isinstance(claimed, tuple)
            and len(claimed) == 2
            and (
                claimed[1] is None
                or isinstance(claimed[1], (str, bytes, int, float))
            )
            and (
                _field(claimed[0], "mailbox_id", None)
                or _field(claimed[0], "id", None)
                or _field(claimed[0], "message_id", None)
            )
        ):
            claimed, envelope_token = claimed[0], str(claimed[1]) if claimed[1] is not None else None
        if isinstance(claimed, Mapping) or not isinstance(claimed, (list, tuple, set)):
            claimed = [claimed]
        processed = 0
        for item in claimed:
            mailbox_id = str(
                _field(item, "mailbox_id", None)
                or _field(item, "id", None)
                or _field(item, "message_id", "")
                or ""
            )
            token = (
                _field(item, "claim_token", None)
                or _field(item, "token", None)
                or envelope_token
            )
            if not mailbox_id:
                continue
            mark_processing = getattr(self.store, "mark_mailbox_processing", None)
            if mark_processing is not None:
                changed = await _call_compatible(mark_processing, mailbox_id, claim_token=token)
                if changed is False:
                    continue
            claim_lost = asyncio.Event()
            self._mailbox_claim_loss_events[mailbox_id] = claim_lost
            heartbeat = self._start_lease_heartbeat(
                mailbox_id,
                token,
                lease_deadline=claim_started + self.lease_seconds,
            )
            try:
                processed += await self._process_claimed_item(
                    item,
                    mailbox_id=mailbox_id,
                    claim_token=token,
                    claim_lost=claim_lost,
                )
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    if self._lease_heartbeats.get(mailbox_id) is heartbeat:
                        self._lease_heartbeats.pop(mailbox_id, None)
                    try:
                        await heartbeat
                    except asyncio.CancelledError:
                        pass
                if self._mailbox_claim_loss_events.get(mailbox_id) is claim_lost:
                    self._mailbox_claim_loss_events.pop(mailbox_id, None)
                self._lost_mailbox_claims.discard(mailbox_id)
        return processed

    async def _process_claimed_item(
        self,
        item: Any,
        *,
        mailbox_id: str,
        claim_token: str | None,
        claim_lost: asyncio.Event | None = None,
    ) -> int:
        """Dispatch one claimed item while the caller maintains its lease."""

        # A response may have committed just before a process died, while the
        # original request was still marked processing.  On recovery, finalize
        # that request from durable correlation instead of invoking the
        # destination Agent a second time.
        find_response = getattr(
            self.store, "get_correlated_mailbox_response", None
        ) or getattr(self.store, "get_mailbox_response", None)
        if (
            find_response is not None
            and not _field(item, "reply_to_id", None)
            and await _call_compatible(find_response, mailbox_id) is not None
        ):
            complete = getattr(
                self.store, "mark_mailbox_processed", None
            ) or getattr(self.store, "complete_mailbox", None)
            changed = True
            if complete is not None:
                changed = await _call_compatible(
                    complete, mailbox_id, claim_token=claim_token
                )
            return int(changed is not False)

        async def dispatch_and_reply() -> None:
            await self._validate_mailbox_attachments(item)
            result = await self._dispatch(item)
            # Cooperative cancellation is best-effort: a compatibility handler
            # can swallow CancelledError and return.  Recheck the durable claim
            # signal at the publication boundary so that cannot produce a stale
            # correlated response.
            if claim_lost is not None and claim_lost.is_set():
                return
            if self.reply_handler is not None:
                try:
                    parameters = inspect.signature(self.reply_handler).parameters
                except (TypeError, ValueError):
                    parameters = {}
                accepts_claim_token = not parameters or any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters.values()
                ) or "claim_token" in parameters
                callback_result = (
                    self.reply_handler(item, result, claim_token=claim_token)
                    if accepts_claim_token
                    else self.reply_handler(item, result)
                )
                if inspect.isawaitable(callback_result):
                    await callback_result

        try:
            if claim_lost is None:
                await dispatch_and_reply()
            else:
                operation = asyncio.create_task(
                    dispatch_and_reply(), name=f"mailbox-dispatch:{mailbox_id}"
                )
                lost_waiter = asyncio.create_task(
                    claim_lost.wait(), name=f"mailbox-claim-loss:{mailbox_id}"
                )
                try:
                    done, _pending = await asyncio.wait(
                        (operation, lost_waiter),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if lost_waiter in done:
                        # Once another owner can claim this durable envelope,
                        # the old Agent turn must stop as well as suppress its
                        # reply.  CodexRuntime translates coroutine cancellation
                        # into an SDK turn interrupt.
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)
                        return 0
                    await operation
                finally:
                    lost_waiter.cancel()
                    await asyncio.gather(lost_waiter, return_exceptions=True)
                    if not operation.done():
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)

            # The heartbeat can lose ownership immediately after the guarded
            # work completes.  Keep the final durable write token-fenced and do
            # not attempt it when that loss is already visible.
            if mailbox_id in self._lost_mailbox_claims:
                return 0
        except PermissionError as exc:
            reject = getattr(self.store, "reject_mailbox", None) or getattr(
                self.store, "mark_mailbox_rejected", None
            )
            if reject is not None:
                await _call_compatible(
                    reject, mailbox_id, claim_token=claim_token, error=str(exc)
                )
            return 0
        except Exception as exc:
            fail = getattr(self.store, "mark_mailbox_failed", None)
            if fail is not None:
                await _call_compatible(
                    fail, mailbox_id, claim_token=claim_token, error=str(exc)
                )
            logger.exception("Agent mailbox item %s failed", mailbox_id)
            return 0

        complete = getattr(self.store, "mark_mailbox_processed", None) or getattr(
            self.store, "complete_mailbox", None
        )
        changed = True
        if complete is not None:
            changed = await _call_compatible(
                complete, mailbox_id, claim_token=claim_token
            )
        return int(changed is not False)

    def _start_lease_heartbeat(
        self,
        mailbox_id: str,
        claim_token: str | None,
        *,
        lease_deadline: float | None = None,
    ) -> asyncio.Task[None] | None:
        """Renew one mailbox claim until its dispatch path is finalized."""

        renew = (
            getattr(self.store, "renew_mailbox_lease", None)
            or getattr(self.store, "renew_agent_mailbox_lease", None)
            or getattr(self.store, "extend_mailbox_lease", None)
        )
        if renew is None or not claim_token:
            return None
        interval = max(0.02, self.lease_seconds / 3)
        confirmed_until = (
            float(lease_deadline)
            if lease_deadline is not None
            else time.monotonic() + self.lease_seconds
        )

        async def heartbeat() -> None:
            nonlocal confirmed_until

            def lose_ownership() -> None:
                self._lost_mailbox_claims.add(mailbox_id)
                claim_lost = self._mailbox_claim_loss_events.get(mailbox_id)
                if claim_lost is not None:
                    claim_lost.set()
                logger.warning("mailbox lease ownership lost for %s", mailbox_id)

            try:
                while True:
                    remaining = confirmed_until - time.monotonic()
                    if remaining <= 0:
                        lose_ownership()
                        return
                    await asyncio.sleep(min(interval, remaining))
                    # Never revive an already expired mailbox claim after the
                    # loop was delayed beyond its last confirmed deadline.
                    if time.monotonic() >= confirmed_until:
                        lose_ownership()
                        return
                    renewal_started = time.monotonic()
                    renewed_until = renewal_started + self.lease_seconds
                    try:
                        renewed = await _call_compatible(
                            renew,
                            mailbox_id,
                            claim_token,
                            lease_seconds=self.lease_seconds,
                        )
                    except Exception:
                        logger.debug(
                            "mailbox lease renewal failed for %s",
                            mailbox_id,
                            exc_info=True,
                        )
                        # A transient failure is harmless only inside the last
                        # confirmed lease.  At expiry, recovery may transfer the
                        # envelope without this worker seeing a ``False``.
                        if time.monotonic() >= confirmed_until:
                            lose_ownership()
                            return
                        continue
                    if renewed is False:
                        lose_ownership()
                        return
                    if time.monotonic() >= renewed_until:
                        lose_ownership()
                        return
                    confirmed_until = renewed_until
            except asyncio.CancelledError:
                return

        heartbeat_task = asyncio.create_task(
            heartbeat(), name=f"mailbox-heartbeat:{mailbox_id}"
        )
        self._lease_heartbeats[mailbox_id] = heartbeat_task
        return heartbeat_task

    @staticmethod
    def _attachment_values(payload: Any) -> tuple[Any, ...]:
        """Extract every explicit attachment field from a mailbox payload."""

        if isinstance(payload, Mapping):
            values: list[Any] = []
            for key in ("attachments", "attachment_ids", "media"):
                raw = payload.get(key)
                if raw is None:
                    continue
                if isinstance(raw, (Mapping, str, bytes, bytearray)):
                    values.append(raw)
                else:
                    try:
                        values.extend(raw)
                    except TypeError:
                        values.append(raw)
            if payload.get("attachment_id") is not None:
                values.append(payload["attachment_id"])
            return tuple(values)
        return ()

    async def _validate_mailbox_attachments(self, item: Any) -> None:
        """Fence managed media immediately before handing a request to an Agent."""

        payload = _field(item, "payload", {}) or {}
        values = self._attachment_values(payload)
        if not values:
            return
        getter = getattr(self.store, "get_attachment", None)
        checker = (
            getattr(self.store, "can_access_attachment", None)
            or getattr(self.store, "attachment_accessible", None)
            or getattr(self.store, "check_attachment_access", None)
        )
        if getter is None and checker is None:
            return
        target = _field(item, "reply_target", None)
        task_id = _field(item, "task_id", None)
        scope = {
            "channel": str(_field(target, "channel", "") or ""),
            "bot_id": str(_field(target, "bot_id", "") or ""),
            "external_user_id": str(
                _field(target, "external_user_id", "") or ""
            ),
            "session_id": str(
                _field(target, "session_id", "default") or "default"
            ),
        }
        for raw in values:
            if isinstance(raw, Mapping):
                attachment_id = raw.get("attachment_id", raw.get("id", ""))
            else:
                attachment_id = getattr(
                    raw, "attachment_id", raw if isinstance(raw, str) else ""
                )
            attachment_id = str(attachment_id or "").strip()
            if not attachment_id:
                continue
            if getter is not None:
                found = await _call_compatible(getter, attachment_id)
                if found is None:
                    raise PermissionError(
                        f"attachment is unavailable: {attachment_id}"
                    )
            if checker is not None:
                accessible = await _call_compatible(
                    checker,
                    attachment_id,
                    agent_id=self.destination_agent_id,
                    source_agent_id=self.destination_agent_id,
                    task_id=task_id,
                    **scope,
                )
                if not accessible:
                    raise PermissionError(
                        f"attachment access denied: {attachment_id}"
                    )

    @staticmethod
    def _mailbox_target(value: Any) -> ReplyTarget:
        if isinstance(value, ReplyTarget):
            return value
        if isinstance(value, Mapping):
            fields = {
                name: value.get(name)
                for name in ReplyTarget.__dataclass_fields__
                if name in value
            }
            return ReplyTarget(**fields)
        if value is not None:
            converter = getattr(value, "to_dict", None) or getattr(
                value, "as_dict", None
            )
            if converter is not None:
                converted = converter()
                if isinstance(converted, Mapping):
                    return AgentMailboxWorker._mailbox_target(converted)
        return ReplyTarget()

    async def _mailbox_execution_snapshot(self, item: Any) -> dict[str, Any]:
        """Load and validate the immutable context for a fallback Agent run."""

        raw_snapshot = _field(item, "execution_snapshot", None) or _field(
            item, "runtime_snapshot", None
        )
        payload = _field(item, "payload", {}) or {}
        if not raw_snapshot and isinstance(payload, Mapping):
            raw_snapshot = payload.get("_execution_snapshot") or payload.get(
                "_runtime_snapshot"
            )
        snapshot = dict(raw_snapshot) if isinstance(raw_snapshot, Mapping) else {}
        task_record = None
        task_id = _field(item, "task_id", None)
        getter = getattr(self.store, "get_task", None)
        if task_id and getter is not None:
            task_record = await _call_compatible(getter, task_id)

        target = self._mailbox_target(snapshot.get("reply_target"))
        if not any((target.channel, target.bot_id, target.external_user_id)):
            target = self._mailbox_target(_field(task_record, "reply_target", None))
        destination = self.destination_agent_id
        request_id = str(
            _field(item, "request_id", None)
            or _field(item, "mailbox_id", None)
            or _field(item, "message_id", "")
            or uuid.uuid4().hex
        )
        conversation_id = str(snapshot.get("conversation_id", "") or "")
        expected_conversation_id = mailbox_conversation_id(destination, request_id)
        if conversation_id and conversation_id != expected_conversation_id:
            raise PermissionError(
                "mailbox conversation identity conflicts with its request"
            )
        # Mailbox turns are request/destination scoped even when their reply
        # target contains a complete user route.  The route controls where a
        # later foreground response may be presented; it is never a provider
        # thread identity for internal Agent work.
        conversation_id = expected_conversation_id

        snapshot_agent = str(snapshot.get("agent_id", destination) or destination)
        if snapshot_agent != destination:
            raise PermissionError("mailbox execution snapshot Agent mismatch")

        # A legacy row may predate immutable mailbox policy snapshots. Resolve
        # the destination's durable default conservatively, but never inherit
        # the source task's mode/profile tuple (that would cross an Agent
        # policy boundary).
        profile = None
        profile_getter = getattr(self.store, "get_profile", None) or getattr(
            self.store, "load_profile", None
        )
        if profile_getter is not None and "profile_version" not in snapshot:
            profile = await _call_compatible(profile_getter, destination)
        default_mode_id = str(
            _field(profile, "default_mode_id", "chat") or "chat"
        )
        try:
            profile_version = int(
                snapshot.get(
                    "profile_version",
                    _field(profile, "profile_version", 1),
                )
                or 1
            )
        except (TypeError, ValueError):
            raise PermissionError("mailbox profile version is invalid") from None
        mode_id = str(snapshot.get("mode_id", default_mode_id) or default_mode_id)
        mode = None
        mode_getter = getattr(self.store, "get_mode", None) or getattr(
            self.store, "load_mode", None
        )
        if mode_getter is not None and "policy_version" not in snapshot:
            mode = await _call_compatible(mode_getter, destination, mode_id)
        try:
            policy_version = int(
                snapshot.get(
                    "policy_version",
                    _field(mode, "policy_version", 1),
                )
                or 1
            )
        except (TypeError, ValueError):
            raise PermissionError("mailbox policy version is invalid") from None
        if profile_version <= 0 or policy_version <= 0:
            raise PermissionError("mailbox policy snapshot is invalid")

        metadata = snapshot.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
        metadata.pop("session_role", None)
        if profile is not None and "profile" not in metadata:
            converter = getattr(profile, "as_dict", None) or getattr(
                profile, "to_dict", None
            )
            if converter is not None:
                value = converter()
                if isinstance(value, Mapping):
                    metadata["profile"] = dict(value)
        if mode is not None and "mode" not in metadata:
            converter = getattr(mode, "as_dict", None) or getattr(mode, "to_dict", None)
            if converter is not None:
                value = converter()
                if isinstance(value, Mapping):
                    metadata["mode"] = dict(value)
        metadata["internal_mailbox"] = True
        metadata["mailbox_id"] = str(_field(item, "mailbox_id", "") or "")
        return {
            "conversation_id": conversation_id,
            "reply_target": target,
            "mode_id": mode_id,
            "profile_version": profile_version,
            "policy_version": policy_version,
            "model": str(snapshot.get("model", "") or ""),
            "reasoning_effort": str(snapshot.get("reasoning_effort", "") or ""),
            "metadata": metadata,
        }

    async def _dispatch(self, item: Any) -> Any:
        self._assert_loop()
        if self.handler is not None:
            result = self.handler(item)
            return await result if inspect.isawaitable(result) else result
        runtime = self._runtime()
        for name in ("receive_agent_message", "handle_mailbox", "handle_agent_message"):
            method = getattr(runtime, name, None)
            if method is not None:
                result = method(item)
                return await result if inspect.isawaitable(result) else result
        run = getattr(runtime, "run", None)
        if run is None:
            raise AttributeError("Agent runtime has no mailbox handler or run()")
        execution_snapshot = await self._mailbox_execution_snapshot(item)
        task = AgentTask(
            task_id=f"mailbox-{_field(item, 'mailbox_id', None) or _field(item, 'message_id', '')}",
            execution_id=f"mailbox-exec-{uuid.uuid4().hex}",
            agent_id=self.destination_agent_id,
            conversation_id=execution_snapshot["conversation_id"],
            reply_target=execution_snapshot["reply_target"],
            mode_id=execution_snapshot["mode_id"],
            profile_version=execution_snapshot["profile_version"],
            policy_version=execution_snapshot["policy_version"],
            model=execution_snapshot["model"],
            reasoning_effort=execution_snapshot["reasoning_effort"],
            inputs={
                "agent_message": {
                    "message_id": _field(item, "message_id", ""),
                    "request_id": _field(item, "request_id", ""),
                    "source_agent_id": _field(item, "source_agent_id", ""),
                    "destination_agent_id": self.destination_agent_id,
                    "content": _field(item, "content", ""),
                    "payload": _field(item, "payload", {}) or {},
                }
            },
            request_id=str(_field(item, "request_id", "") or "") or None,
            metadata=execution_snapshot["metadata"],
        )

        async def emit(_event: Any) -> None:
            # Mailbox responses are correlated Agent messages, not channel
            # deliveries.  A runtime may explicitly call TaskManager's reply
            # facade; silently dropping unsolicited user events here prevents
            # accidental WeChat sends.
            return None

        return await run(task, emit)

    async def run(self) -> None:
        self._assert_loop()
        while not self._stop.is_set():
            try:
                worked = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Agent mailbox iteration failed")
                worked = 0
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), self.poll_interval)
                except asyncio.TimeoutError:
                    pass

    process_once = run_once


class AgentMailboxSupervisor:
    """Keep one loop-owned mailbox worker running per enabled Agent.

    Agent registrations can change after process startup through ``/agent`` and
    ``/ask``.  The supervisor follows the registry's public descriptor list so
    those Agents receive the same durable mailbox processing as Agents restored
    during startup.  Child workers are always stopped and drained before the
    supervisor returns.
    """

    def __init__(
        self,
        store: Any,
        registry: Any,
        *,
        worker_id_prefix: str = "agent-mailbox",
        handler: Any | None = None,
        reply_handler: Any | None = None,
        poll_interval: float = 0.5,
        lease_seconds: float = 60.0,
        stop_timeout: float = 5.0,
    ) -> None:
        self.store = store
        self.registry = registry
        self.worker_id_prefix = str(worker_id_prefix or "agent-mailbox")
        self.handler = handler
        self.reply_handler = reply_handler
        self.poll_interval = max(0.05, float(poll_interval))
        self.lease_seconds = max(0.1, float(lease_seconds))
        self.stop_timeout = max(0.0, float(stop_timeout))
        self._stop = asyncio.Event()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._workers: dict[str, AgentMailboxWorker] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._owner_loop is None:
            if loop is not None:
                self._owner_loop = loop
            return
        if loop is not None and self._owner_loop is not loop:
            raise RuntimeError(
                "AgentMailboxSupervisor must be accessed from its owning asyncio loop"
            )

    @property
    def destination_agent_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._workers))

    def stop(self) -> None:
        """Request shutdown from either the owning loop or another thread."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._owner_loop is not None and self._owner_loop is not loop:
            if self._owner_loop.is_running():
                self._owner_loop.call_soon_threadsafe(self._stop.set)
                return
        self._assert_loop()
        self._stop.set()

    async def _agent_ids(self) -> set[str]:
        listing = (
            getattr(self.registry, "list", None)
            or getattr(self.registry, "list_agents", None)
            or getattr(self.registry, "descriptors", None)
        )
        if listing is None:
            raise AttributeError("Agent registry must expose a public descriptor list")
        descriptors = listing()
        if inspect.isawaitable(descriptors):
            descriptors = await descriptors
        result: set[str] = set()
        for descriptor in descriptors or ():
            enabled = bool(_field(descriptor, "enabled", True))
            agent_id = str(
                _field(descriptor, "agent_id", _field(descriptor, "id", "")) or ""
            ).strip()
            if enabled and agent_id:
                result.add(agent_id)
        return result

    async def _stop_agents(self, agent_ids: set[str]) -> None:
        tasks: list[asyncio.Task[None]] = []
        for agent_id in sorted(agent_ids):
            worker = self._workers.pop(agent_id, None)
            task = self._tasks.pop(agent_id, None)
            if worker is not None:
                worker.stop()
            if task is not None:
                tasks.append(task)
        if tasks:
            pending = set(tasks)
            try:
                if self.stop_timeout:
                    _done, pending = await asyncio.wait(
                        tasks, timeout=self.stop_timeout
                    )
            finally:
                # A mailbox runtime may ignore the cooperative stop event.  Do
                # not let it retain the supervisor (and therefore SQLite) past
                # the bounded process-shutdown window.  Runtime adapters such
                # as Codex translate coroutine cancellation into an SDK turn
                # interrupt; the durable mailbox lease remains recoverable.
                for task in pending:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _sync_workers(self) -> None:
        desired = await self._agent_ids()
        await self._stop_agents(set(self._workers) - desired)
        for agent_id in sorted(desired):
            task = self._tasks.get(agent_id)
            if task is not None and not task.done():
                continue
            if task is not None:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception(
                        "mailbox worker for Agent %s exited unexpectedly", agent_id
                    )
            worker = AgentMailboxWorker(
                self.store,
                self.registry,
                agent_id,
                worker_id=f"{self.worker_id_prefix}:{agent_id}",
                handler=self.handler,
                reply_handler=self.reply_handler,
                poll_interval=self.poll_interval,
                lease_seconds=self.lease_seconds,
            )
            self._workers[agent_id] = worker
            self._tasks[agent_id] = asyncio.create_task(
                worker.run(), name=f"agent-mailbox:{agent_id}"
            )

    async def run(self) -> None:
        self._assert_loop()
        try:
            while not self._stop.is_set():
                try:
                    await self._sync_workers()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("could not synchronize Agent mailbox workers")
                try:
                    await asyncio.wait_for(self._stop.wait(), self.poll_interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self._stop_agents(set(self._workers))


MailboxWorker = AgentMailboxWorker
AgentMessageWorker = AgentMailboxWorker


Worker = TaskWorker
TaskExecutionWorker = TaskWorker

__all__ = [
    "AgentMailboxSupervisor",
    "AgentMailboxWorker",
    "AgentMessageWorker",
    "MailboxWorker",
    "TaskExecutionWorker",
    "TaskWorker",
    "Worker",
]
