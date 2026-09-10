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
MAILBOX_OPERATOR_CANCEL_REASON = "cancelled by operator /cancel"


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
        if normalized not in {"account", "account_agent", "agent", "global"}:
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

    async def get_task_steering(self, steering_id: str) -> Any | None: ...

    async def get_task_steering_by_inbound(
        self, inbound_message_id: str
    ) -> Any | None: ...

    async def list_pending_task_steering(self, **kwargs: Any) -> list[Any]: ...

    async def claim_next_task_steering(
        self,
        task_id: str,
        execution_id: str,
        **kwargs: Any,
    ) -> Any | None: ...

    async def mark_task_steering_applied(
        self, steering_id: str, **kwargs: Any
    ) -> Any: ...

    async def release_task_steering(
        self, steering_id: str, **kwargs: Any
    ) -> Any: ...

    async def promote_task_steering(
        self, steering_id: str, **kwargs: Any
    ) -> Any: ...

    async def recover_task_steering(self, **kwargs: Any) -> int: ...

    async def get_agent_invocation(self, invocation_id: str) -> Any | None: ...

    async def list_agent_invocations(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_admission_counter(
        self, agent_id: str, agent_incarnation: int
    ) -> Any | None: ...

    async def list_agent_admission_counters(self, **kwargs: Any) -> list[Any]: ...

    async def get_account_admission_counter(
        self, channel: str, bot_id: str
    ) -> Any | None: ...

    async def list_account_admission_counters(self, **kwargs: Any) -> list[Any]: ...

    async def get_agent_account_admission_counter(
        self,
        agent_id: str,
        agent_incarnation: int,
        channel: str,
        bot_id: str,
    ) -> Any | None: ...

    async def list_agent_account_admission_counters(
        self, **kwargs: Any
    ) -> list[Any]: ...

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

    async def create_cron_job(
        self, job: Any | None = None, **kwargs: Any
    ) -> Any: ...

    async def get_cron_job(self, job_id: str) -> Any | None: ...

    async def list_cron_jobs(self, **kwargs: Any) -> list[Any]: ...

    async def disable_cron_job(self, job_id: str, **kwargs: Any) -> Any: ...

    async def reconcile_cron_jobs(self, **kwargs: Any) -> list[Any]: ...

    async def fire_next_due_cron_job(self, **kwargs: Any) -> Any | None: ...

    async def list_cron_firings(
        self, job_id: str, **kwargs: Any
    ) -> list[Any]: ...

    async def create_natural_cron_draft(
        self, draft: Any | None = None, **kwargs: Any
    ) -> Any: ...

    async def get_natural_cron_draft(self, draft_id: str) -> Any | None: ...

    async def list_natural_cron_drafts(self, **kwargs: Any) -> list[Any]: ...

    async def confirm_natural_cron_draft(
        self, draft_id: str, **kwargs: Any
    ) -> Any: ...

    async def cancel_natural_cron_draft(
        self, draft_id: str, **kwargs: Any
    ) -> Any: ...

    async def begin_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def get_command_receipt(self, command_id: str) -> Any | None: ...

    async def reopen_interrupted_command_receipt(
        self, command_id: str, **kwargs: Any
    ) -> Any: ...

    async def complete_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def interrupt_command_receipt(self, command_id: str, **kwargs: Any) -> Any: ...

    async def create_principal(self, principal: Any = None, **kwargs: Any) -> Any: ...

    async def get_principal(self, principal_id: str) -> Any | None: ...

    async def list_principals(self, **kwargs: Any) -> list[Any]: ...

    async def update_principal(self, principal_id: str, **kwargs: Any) -> Any: ...

    async def delete_principal(self, principal_id: str) -> bool: ...

    async def map_principal_account(self, **kwargs: Any) -> Any: ...

    async def resolve_principal_account(self, **kwargs: Any) -> Any | None: ...

    async def list_principal_accounts(self, **kwargs: Any) -> list[Any]: ...

    async def auto_map_owner_principal_accounts(
        self, *, accounts: Any, **kwargs: Any
    ) -> list[Any]: ...

    async def unmap_principal_account(self, **kwargs: Any) -> bool: ...

    async def get_principal_conversation_binding(self, **kwargs: Any) -> Any | None: ...

    async def get_transport_conversation_id(self, **kwargs: Any) -> str | None: ...

    async def bind_principal_conversation(self, **kwargs: Any) -> Any: ...

    async def resolve_principal_conversation(
        self, conversation_id: str, **kwargs: Any
    ) -> str: ...

    async def put_conversation_subject(self, subject: Any, **kwargs: Any) -> Any: ...

    async def get_conversation_subject(self, conversation_subject_id: str) -> Any | None: ...

    async def create_bot_profile(self, profile: Any = None, **kwargs: Any) -> Any: ...

    async def create_bot_profile_for_onboarding(
        self, profile: Any = None, **kwargs: Any
    ) -> Any: ...

    async def create_bot_profile_with_owner_for_onboarding(
        self,
        profile: Any = None,
        *,
        external_user_id: str,
        source_principal_account_id: str,
        source_channel: str,
        source_bot_id: str,
        source_external_user_id: str,
        source_mapping_revision: int,
        configured_by: str,
        principal_account_id: str | None = None,
        now: Any | None = None,
        **kwargs: Any,
    ) -> tuple[Any, Any]: ...

    async def rollback_bot_profile_registration(
        self, profile: Any, **kwargs: Any
    ) -> bool: ...

    async def rollback_bot_profile_with_owner_registration(
        self,
        profile: Any,
        principal_account: Any,
        **kwargs: Any,
    ) -> bool: ...

    async def create_bot_profile_with_principal_account(
        self,
        profile: Any = None,
        *,
        principal_id: str = "owner",
        external_user_id: str,
        identifier_kind: str = "open_id",
        configured_by: str,
        principal_account_id: str | None = None,
        now: Any | None = None,
        **kwargs: Any,
    ) -> tuple[Any, Any]: ...

    async def get_bot_profile(self, profile_id: str, **kwargs: Any) -> Any | None: ...

    async def list_bot_profiles(self, **kwargs: Any) -> list[Any]: ...

    async def update_bot_profile(self, profile_id: str, **kwargs: Any) -> Any: ...

    async def set_bot_profile_enabled(self, profile_id: str, enabled: bool, **kwargs: Any) -> Any: ...

    async def get_bot_profile_status(self, profile_id: str) -> Any | None: ...

    async def set_bot_profile_status(self, profile_id: str, **kwargs: Any) -> Any: ...

    async def update_bot_profile_credentials(self, profile_id: str, credential_ref: str, **kwargs: Any) -> Any: ...

    async def remove_bot_profile(self, profile_id: str, **kwargs: Any) -> bool: ...

    async def claim_outbox(self, worker_id: str, **kwargs: Any) -> list[Any]: ...

    async def claim_account_outbox(
        self, worker_id: str, *, channel: str, bot_id: str, **kwargs: Any
    ) -> list[Any]: ...

    async def create_user_outbox(self, delivery: Any = None, **kwargs: Any) -> Any: ...

    async def create_account_outbox(
        self, delivery: Any = None, *, channel: str, bot_id: str, **kwargs: Any
    ) -> Any: ...

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

    async def finish_outbox_attempt(self, outbox_id: str, **kwargs: Any) -> bool: ...

    async def create_outgoing_media(self, **kwargs: Any) -> Any: ...

    async def claim_outgoing_media(self, worker_id: str, **kwargs: Any) -> list[Any]: ...

    async def claim_account_outgoing_media(
        self, worker_id: str, *, channel: str, bot_id: str, **kwargs: Any
    ) -> list[Any]: ...

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

    async def cancel_active_mailbox_invocation(
        self, agent_id: str, **kwargs: Any
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
    "MAILBOX_OPERATOR_CANCEL_REASON",
    "NotFoundError",
    "QueueFullError",
    "RuntimeStore",
    "Store",
    "StoreError",
    "StoreProtocol",
]
