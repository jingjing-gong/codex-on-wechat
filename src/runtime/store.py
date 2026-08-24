"""Backend-neutral durable store contract.

The runtime deliberately depends on this small protocol rather than on
SQLite-specific details.  :class:`~src.runtime.sqlite_store.SQLiteStore` is
the first implementation, but keeping the contract here makes a later
PostgreSQL or broker-backed store a replacement instead of a rewrite.

Methods are intentionally typed in terms of ``Any`` at the edges.  Channel
adapters and Agent runtimes have their own immutable envelope classes and the
SQLite adapter already performs the normalization; importing those concrete
types here would re-introduce the coupling this module is meant to prevent.
Implementations should document whether additional methods participate in a
caller-owned transaction.  The public methods below commit their own short
transaction in the SQLite implementation.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


WORKING_DIRECTORY_RESPONSE_PREFIX = "working directory: "
WORKING_DIRECTORY_RESPONSE_MAX_CHARS = 512


def format_working_directory_response(path: Any) -> str:
    """Return the canonical public acknowledgement for one resolved cwd.

    Filesystem paths may legally contain newlines, other control whitespace,
    and Markdown delimiters.  A command acknowledgement must not let those
    characters create extra lines or Markdown structure, and it must retain a
    fixed public size even when the canonical path is very long.
    """

    public_path = " ".join(str(path or "").split()).replace("`", "'")
    if not public_path:
        raise ValueError("working directory response has no path")
    path_budget = (
        WORKING_DIRECTORY_RESPONSE_MAX_CHARS
        - len(WORKING_DIRECTORY_RESPONSE_PREFIX)
    )
    if len(public_path) > path_budget:
        public_path = public_path[: path_budget - 3].rstrip() + "..."
    return WORKING_DIRECTORY_RESPONSE_PREFIX + public_path


class StoreError(RuntimeError):
    """Base exception exposed by every durable-store backend."""


class NotFoundError(StoreError):
    """A required durable object does not exist."""


class InvalidTransition(StoreError):
    """A requested durable state transition is not currently valid."""


class QueueFullError(StoreError):
    """The durable Agent invocation admission limit was reached."""

    def __init__(self, scope: str, limit: int) -> None:
        normalized = str(scope or "global").strip().lower()
        if normalized not in {"agent", "global"}:
            normalized = "global"
        self.scope = normalized
        self.limit = int(limit)
        super().__init__(f"queue_full: {normalized} limit {self.limit}")


@runtime_checkable
class DurableStore(Protocol):
    """Asynchronous persistence boundary used by the task manager/worker."""

    async def initialize(
        self, *, recover_startup_state: bool | None = None
    ) -> None: ...

    async def close(self) -> None: ...

    async def activate_supervisor_epoch(self, **kwargs: Any) -> Any: ...

    async def finish_supervisor_epoch(
        self, epoch: int, **kwargs: Any
    ) -> bool: ...

    async def get_agent_lifecycle(
        self, agent_id: str, agent_incarnation: int | None = None
    ) -> Any | None: ...

    async def list_agent_lifecycles(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_lifecycle_event(
        self, lifecycle_event_id: str
    ) -> Any | None: ...

    async def list_agent_lifecycle_events(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_process(
        self,
        agent_id: str,
        agent_incarnation: int,
        worker_generation: int | None = None,
    ) -> Any | None: ...

    async def list_agent_processes(self, **kwargs: Any) -> list[Any]: ...

    async def begin_agent_process_generation(self, **kwargs: Any) -> Any: ...

    async def commit_agent_process_handshake(self, **kwargs: Any) -> Any: ...

    async def commit_agent_process_ready(self, **kwargs: Any) -> Any: ...

    async def fence_agent_process_stopped(self, **kwargs: Any) -> Any: ...

    async def accept_inbound(
        self, inbound: Any, *, task: Any | None = None, create_task: bool = True, **kwargs: Any
    ) -> Any: ...

    async def create_task(self, task: Any | None = None, **kwargs: Any) -> Any: ...

    async def get_task(self, task_id: str) -> Any | None: ...

    async def get_execution(self, execution_id: str) -> Any | None: ...

    async def get_agent_invocation(self, invocation_id: str) -> Any | None: ...

    async def list_agent_invocations(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_admission_counter(
        self, agent_id: str, agent_incarnation: int
    ) -> Any | None: ...

    async def list_agent_admission_counters(self, **kwargs: Any) -> list[Any]: ...

    async def get_global_agent_admission_counter(self) -> Any | None: ...

    async def get_agent_execution_slot(self, slot_id: str) -> Any | None: ...

    async def get_active_agent_execution_slot(
        self, agent_id: str, agent_incarnation: int
    ) -> Any | None: ...

    async def list_agent_execution_slots(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_dispatch_attempt(
        self, dispatch_attempt_id: str
    ) -> Any | None: ...

    async def list_agent_dispatch_attempts(self, **kwargs: Any) -> list[Any]: ...

    async def reserve_next_agent_invocation(self, **kwargs: Any) -> Any | None: ...

    async def commit_agent_dispatch_abort(self, **kwargs: Any) -> Any: ...

    async def commit_agent_dispatch_rejection(self, **kwargs: Any) -> Any: ...

    async def record_agent_dispatch_cleanup(self, **kwargs: Any) -> Any: ...

    async def list_task_executions(
        self, task_id: str, *, limit: int = 100, newest_first: bool = False
    ) -> list[Any]: ...

    async def list_tasks(self, *, limit: int = 100, **filters: Any) -> list[Any]: ...

    async def claim_next_task(
        self, worker_id: str, *, lease_seconds: float = 60.0, **kwargs: Any
    ) -> Any | None: ...

    async def mark_task_running(
        self, task_id: str, claim_token: str, **kwargs: Any
    ) -> bool: ...

    async def append_task_event(self, task_id: str, event: Any = None, **kwargs: Any) -> Any: ...

    async def complete_task(self, task_id: str, result: Any = None, **kwargs: Any) -> Any: ...

    async def request_cancel(self, task_id: str, **kwargs: Any) -> bool: ...

    async def retry_task(self, task_id: str, **kwargs: Any) -> Any: ...

    async def begin_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def get_command_receipt(self, command_id: str) -> Any | None: ...

    async def reopen_interrupted_command_receipt(
        self, command_id: str, **kwargs: Any
    ) -> Any: ...

    async def complete_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def interrupt_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def claim_outbox(self, worker_id: str, **kwargs: Any) -> list[Any]: ...

    async def create_user_outbox(self, delivery: Any = None, **kwargs: Any) -> Any: ...

    async def get_outbox_item(self, outbox_id: str) -> Any | None: ...

    async def get_reply_scope(self, reply_scope_id: str) -> Any | None: ...

    async def get_reply_scope_for_inbound(
        self, inbound_message_id: str
    ) -> Any | None: ...

    async def project_reply_candidate(self, **kwargs: Any) -> Any: ...

    async def append_reply_aggregate_member(self, **kwargs: Any) -> Any: ...

    async def get_reply_aggregate(self, reply_aggregate_id: str) -> Any | None: ...

    async def list_reply_aggregates(self, **kwargs: Any) -> list[Any]: ...

    async def list_reply_aggregate_members(
        self, reply_aggregate_id: str
    ) -> list[Any]: ...

    async def seal_reply_aggregates(self, **kwargs: Any) -> Any: ...

    async def seal_due_reply_aggregates(self, **kwargs: Any) -> Any: ...

    async def materialize_sealed_reply_aggregate(
        self, reply_aggregate_id: str, **kwargs: Any
    ) -> Any: ...

    async def drain_deferred_replies(self, **kwargs: Any) -> Any: ...

    async def activate_outbox_contextless_variant(
        self, outbox_id: str, claim_token: str | None = None, **kwargs: Any
    ) -> bool: ...

    async def mark_outbox_sent(self, outbox_id: str, **kwargs: Any) -> bool: ...

    async def mark_outbox_failed(self, outbox_id: str, **kwargs: Any) -> bool: ...

    async def create_outgoing_media(self, **kwargs: Any) -> Any: ...

    async def claim_outgoing_media(self, worker_id: str, **kwargs: Any) -> list[Any]: ...

    async def transition_outgoing_media(self, media_id: str, to_state: Any, **kwargs: Any) -> bool: ...

    async def create_transcription_candidate(self, **kwargs: Any) -> Any: ...

    async def get_transcription_candidate(self, confirmation_id: str) -> Any | None: ...

    async def resolve_transcription(self, confirmation_id: str, *, status: str, **kwargs: Any) -> bool: ...

    async def consume_transcription(self, confirmation_id: str, task_id: str) -> bool: ...

    async def retry_outgoing_media(self, media_id: str, **kwargs: Any) -> bool: ...

    async def present_unseen(self, **kwargs: Any) -> list[Any]: ...

    async def present_inbox_candidates(self, **kwargs: Any) -> list[Any]: ...

    async def claim_mailbox(self, destination_agent_id: str, worker_id: str, **kwargs: Any) -> list[Any]: ...

    async def renew_mailbox_lease(
        self, mailbox_id: str, claim_token: str, **kwargs: Any
    ) -> bool: ...

    async def create_agent_message(self, **kwargs: Any) -> Any: ...

    async def review_mailbox_orphan(
        self,
        mailbox_message_id: str,
        expected_current_invocation_id: str,
        mailbox_maintenance_id: str,
        action: Any,
        **kwargs: Any,
    ) -> Any: ...

    async def get_mailbox_orphan_review(
        self, mailbox_maintenance_id: str
    ) -> Any | None: ...

    async def list_mailbox_orphan_reviews(self, **kwargs: Any) -> list[Any]: ...

    async def list_agent_invocation_events(
        self, invocation_id: str, **kwargs: Any
    ) -> list[Any]: ...

    async def get_route(self, **kwargs: Any) -> str: ...

    async def set_route(self, **kwargs: Any) -> bool: ...

    async def get_session_role(self, **kwargs: Any) -> Any: ...

    async def set_session_role(self, role_text: str, **kwargs: Any) -> Any: ...

    async def get_session_working_directory(self, **kwargs: Any) -> Any | None: ...

    async def set_session_working_directory(
        self, relative_path: str, **kwargs: Any
    ) -> Any: ...

    async def mark_agent_deleted(self, agent_id: str, **kwargs: Any) -> bool: ...

    async def reactivate_agent(self, profile: Any) -> bool: ...

    async def commit_agent_reactivation(self, profile: Any, **kwargs: Any) -> bool: ...

    async def clear_agent_deleted(self, agent_id: str) -> bool: ...

    async def is_agent_deleted(self, agent_id: str) -> bool: ...

    async def put_skill(self, skill: Any, *, agent_id: str = "codex") -> bool: ...

    async def get_skill(
        self, agent_id: str, skill_id: str, version: str | None = None
    ) -> Any | None: ...

    async def list_skills(
        self, *, agent_id: str = "codex", enabled: bool | None = None
    ) -> list[Any]: ...

    async def save_cursor(self, *, channel: str, bot_id: str, cursor: str, **kwargs: Any) -> bool: ...

    async def clear_cursor(self, *, channel: str, bot_id: str, **kwargs: Any) -> bool: ...

    async def get_cursor(self, *, channel: str, bot_id: str) -> str: ...

    async def reconcile(self, **kwargs: Any) -> Any: ...

    async def startup_reconcile(self, **kwargs: Any) -> Any: ...

    async def claim_attachment_cleanup(self, attachment_id: str, **kwargs: Any) -> bool: ...

    async def finish_attachment_cleanup(
        self, attachment_id: str, *, deleted: bool, **kwargs: Any
    ) -> bool: ...


# Names used by integrations written against earlier plan revisions.  They
# intentionally point at the same structural protocol.
Store = DurableStore
StoreProtocol = DurableStore
RuntimeStore = DurableStore


__all__ = [
    "DurableStore",
    "InvalidTransition",
    "NotFoundError",
    "QueueFullError",
    "RuntimeStore",
    "Store",
    "StoreError",
    "StoreProtocol",
]
