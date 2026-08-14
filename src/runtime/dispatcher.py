"""SQLite-backed dispatch wake-up and local serialization coordination."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from typing import Any, Callable


def task_serialization_key(task: Any) -> tuple[str, str, str, str, str]:
    """Return the default per-Agent conversation serialization key."""

    def value(name: str, default: str = "") -> Any:
        if isinstance(task, Mapping):
            return task.get(name, default)
        return getattr(task, name, default)

    reply = value("reply_target", None)

    def reply_value(name: str) -> str:
        if reply is None:
            return ""
        if isinstance(reply, Mapping):
            return str(reply.get(name, "") or "")
        return str(getattr(reply, name, "") or "")

    explicit = value("serialization_key", None)
    if explicit:
        if isinstance(explicit, (tuple, list)):
            padded = tuple(str(part) for part in explicit[:5]) + ("",) * max(0, 5 - len(explicit))
            return padded[:5]  # type: ignore[return-value]
        return (str(explicit), "", "", "", "")
    return (
        reply_value("channel"),
        reply_value("bot_id"),
        reply_value("external_user_id"),
        reply_value("session_id") or str(value("conversation_id", "")),
        str(value("agent_id", "codex") or "codex"),
    )


class SQLiteDispatcher:
    """Wake workers when durable tasks may be available.

    The event is deliberately lossy.  Workers always poll the SQLite store on
    startup and after each timeout, so a crash or missed notification cannot
    lose queued work.
    """

    def __init__(
        self,
        store: Any,
        *,
        poll_interval: float = 0.5,
        claim_lease_seconds: float = 60.0,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.store = store
        self.poll_interval = float(poll_interval)
        self.claim_lease_seconds = float(claim_lease_seconds)
        self._wake_event = asyncio.Event()
        self._stopping = False
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._locks: dict[tuple[str, str, str, str, str], asyncio.Lock] = {}

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError("SQLiteDispatcher must be accessed from its owning asyncio loop")
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("SQLiteDispatcher must be accessed from its owning asyncio loop")

    async def start(self) -> None:
        self._assert_loop()
        self._stopping = False
        self._wake_event.set()  # Always inspect durable work at startup.

    async def stop(self) -> None:
        self._assert_loop()
        self._stopping = True
        self._wake_event.set()

    def wake(self) -> None:
        self._assert_loop()
        self._wake_event.set()

    notify = wake

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait for an explicit wake-up; return False on polling timeout."""

        self._assert_loop()
        if self._stopping:
            return False
        timeout = self.poll_interval if timeout is None else timeout
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        self._wake_event.clear()
        return True

    async def claim(self, worker_id: str) -> Any | None:
        """Atomically claim the next queued task through the store."""

        self._assert_loop()
        method = getattr(self.store, "claim_next_task", None) or getattr(self.store, "claim_task", None)
        if method is None:
            raise AttributeError("store must implement claim_next_task() or claim_task()")
        kwargs = {
            "worker_id": worker_id,
            "claimed_by": worker_id,
            "lease_seconds": self.claim_lease_seconds,
            "lease_duration": self.claim_lease_seconds,
        }
        try:
            return await _call_compatible(method, **kwargs)
        except TypeError:
            # Some lightweight stores expose ``claim_task(task_id, worker_id)``
            # rather than a next-task claim.  The durable store remains the
            # source of truth; this fallback only adapts that shape.
            listing = getattr(self.store, "list_tasks", None)
            if listing is None:
                raise
            queued = await _call_compatible(listing, state="queued", limit=1)
            if not queued:
                return None
            task_id = getattr(queued[0], "task_id", None)
            if isinstance(queued[0], Mapping):
                task_id = queued[0].get("task_id")
            if not task_id:
                return None
            return await _call_compatible(
                method,
                task_id,
                worker_id=worker_id,
                lease_seconds=self.claim_lease_seconds,
            )

    dispatch = claim
    claim_next = claim

    def lock_for(self, task: Any) -> asyncio.Lock:
        """Return the loop-local lock for the task's serialization key."""

        self._assert_loop()
        key = task_serialization_key(task)
        return self._locks.setdefault(key, asyncio.Lock())

    def prune_locks(self) -> None:
        """Drop unused conversation locks after a burst of work."""

        self._assert_loop()
        self._locks = {key: lock for key, lock in self._locks.items() if lock.locked()}

    @property
    def stopping(self) -> bool:
        return self._stopping


async def _call_compatible(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a store method with the subset of keyword arguments it accepts."""

    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        parameters = {}
    if parameters and not any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    ):
        kwargs = {name: value for name, value in kwargs.items() if name in parameters}
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


# General alias retained for non-SQLite deployments in later phases.
TaskDispatcher = SQLiteDispatcher
Dispatcher = SQLiteDispatcher


__all__ = ["Dispatcher", "SQLiteDispatcher", "TaskDispatcher", "task_serialization_key"]
