"""SQLite implementation of the durable runtime store.

The public API is asynchronous.  SQLite work is executed on a dedicated
single-thread executor so a slow database operation never blocks the owning
asyncio loop.  Transactions are short and all lease claims are performed with
``BEGIN IMMEDIATE``; this is what makes the store safe when more than one
dispatcher is active.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .models import (
    AgentEvent,
    AgentMailboxItem,
    AgentResult,
    AgentTask,
    DeliveryMode,
    EventPriority,
    EventVisibility,
    ExecutionState,
    InboundAcceptance,
    InboundMessage,
    InboundState,
    MailboxState,
    MediaDeliveryState,
    OutgoingMediaRecord,
    OutboxState,
    PresentationState,
    RecoveryReport,
    ReplyCandidateRecord,
    ReplyFragmentRecord,
    ReplyFragmentState,
    ReplyProjectionResult,
    ReplyScopeRecord,
    ReplySlotRecord,
    ReplyTarget,
    TaskClaim,
    TaskEvent,
    TaskExecution,
    TaskRecord,
    TaskState,
    UserOutboxItem,
    USER_REPLY_FORMAT_AGENT_PREFIX_V1,
    datetime_to_text,
    json_dumps,
    json_loads,
    text_to_datetime,
    utcnow,
)
from .media import StoredAttachment, canonical_media_inputs, wire_media_fingerprint
from .identity import (
    compound_id,
    conversation_id as canonical_conversation_id,
    conversation_id_candidates,
    conversation_id_matches,
    mailbox_conversation_id,
)

logger = logging.getLogger(__name__)


# Startup state is recovered when the first connection to a durable database
# opens in this process.  Additional stores may legitimately share that
# database (for example, channel and maintenance facades); treating each
# connection as a process restart would interrupt work still owned by another
# live connection.
_LIVE_DATABASES_LOCK = threading.RLock()
_LIVE_DATABASES: dict[tuple[int, str], int] = {}


_TASK_STATES = tuple(state.value for state in TaskState)
_EXECUTION_STATES = tuple(state.value for state in ExecutionState)
_INBOUND_STATES = tuple(state.value for state in InboundState)
_OUTBOX_STATES = tuple(state.value for state in OutboxState)
_MAILBOX_STATES = tuple(state.value for state in MailboxState)

# Removing a live Agent registration would strand work that has not reached a
# final state.  Orphaned work is included because it remains eligible for an
# explicit retry.  Failed/interrupted/cancelled/completed rows are immutable
# history and therefore do not prevent retirement.
_AGENT_RETIREMENT_BLOCKING_TASK_STATES = (
    TaskState.QUEUED.value,
    TaskState.CLAIMED.value,
    TaskState.RUNNING.value,
    TaskState.CANCEL_REQUESTED.value,
    TaskState.ORPHANED.value,
)

# Sentinel used by confirmation creation to distinguish an omitted timestamp
# (which must not assert a newly generated value during replay) from an
# explicitly supplied immutable value.
_UNSET = object()

# Audio transcription is a pending user decision, not an unbounded inbox.
# Keep the default aligned with ``AudioConfirmationManager`` while allowing a
# deployment to choose a shorter/longer policy at the durable store boundary.
DEFAULT_TRANSCRIPTION_TTL_SECONDS = 300.0

# Keep each finished WeChat plain-text reply within 3,000 Unicode characters.
# Ordinal ten always reserves this deterministic prompt before allocation so
# a later candidate cannot race the decision to advertise continuation.
REPLY_SCOPE_CAPACITY = 10
REPLY_TEXT_MAX_CHARS = 3000
REPLY_CONTINUATION_SUFFIX = "\n\nReply /recv to continue."

# Attachment identity and storage metadata are authoritative in SQLite.  A
# channel/Agent attachment mapping may carry transport hints, but these fields
# must never be overlaid onto an outgoing-media projection by the caller.
_ATTACHMENT_AUTHORITY_KEYS = frozenset(
    {
        "attachment_id",
        "id",
        "path",
        "local_path",
        "file_path",
        "checksum",
        "sha256",
        "size",
        "size_bytes",
        "mime_type",
        "mime",
        "content_type",
        "kind",
        "type",
        "media_kind",
        "filename",
        "state",
        "owner_agent_id",
        "agent_id",
        "channel",
        "bot_id",
        "external_user_id",
        "user_id",
        "session_id",
        "source_message_id",
        "source_ordinal",
        "created_at",
        # Transport credentials belong in the channel upload checkpoint, not
        # in caller-controlled attachment metadata.
        "remote_id",
        "download_param",
        "encrypted_query_param",
        "encrypt_query_param",
        "upload_param",
        "encryption_key",
        "aes_key",
        "aes_key_hex",
        "aes_key_base64",
    }
)

# Inbound rows are an immutable channel envelope plus a small durable state
# machine.  Keep the transition table explicit so a late gateway callback
# cannot move a task-owned message back to ``stored`` (or confirm a rejected
# audio candidate) by accident.
_ALLOWED_INBOUND_TRANSITIONS: Mapping[InboundState, frozenset[InboundState]] = {
    InboundState.RECEIVED: frozenset({InboundState.STORED}),
    InboundState.STORED: frozenset(
        {
            InboundState.ACCEPTED,
            InboundState.DUPLICATE,
            InboundState.REJECTED,
            InboundState.AWAITING_CONFIRMATION,
            InboundState.TASK_QUEUED,
        }
    ),
    InboundState.ACCEPTED: frozenset({InboundState.TASK_QUEUED}),
    InboundState.AWAITING_CONFIRMATION: frozenset(
        {InboundState.CONFIRMED, InboundState.REJECTED, InboundState.EXPIRED}
    ),
    InboundState.CONFIRMED: frozenset({InboundState.TASK_QUEUED}),
    # These states are terminal projections.  Replaying the same update is
    # idempotent, but a different state is never silently accepted.
    InboundState.DUPLICATE: frozenset(),
    InboundState.REJECTED: frozenset(),
    InboundState.EXPIRED: frozenset(),
    InboundState.TASK_QUEUED: frozenset(),
}


# Keep this migration as one explicit SQL unit.  Future schema changes should
# append a migration instead of mutating an already-applied migration.
_MIGRATION_1 = f"""
CREATE TABLE IF NOT EXISTS agent_profiles (
    agent_id TEXT NOT NULL,
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    display_name TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    system_prompt TEXT NOT NULL DEFAULT '',
    responsibilities_json TEXT NOT NULL DEFAULT '{{}}',
    constraints_json TEXT NOT NULL DEFAULT '{{}}',
    capabilities_json TEXT NOT NULL DEFAULT '{{}}',
    allowed_peers_json TEXT NOT NULL DEFAULT '[]',
    denied_peers_json TEXT NOT NULL DEFAULT '[]',
    allowed_request_types_json TEXT NOT NULL DEFAULT '[]',
    denied_request_types_json TEXT NOT NULL DEFAULT '[]',
    max_child_depth INTEGER NOT NULL DEFAULT 0 CHECK (max_child_depth >= 0),
    max_children_per_task INTEGER NOT NULL DEFAULT 0 CHECK (max_children_per_task >= 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    default_mode_id TEXT NOT NULL DEFAULT 'chat',
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, profile_version)
);

CREATE TABLE IF NOT EXISTS agent_modes (
    agent_id TEXT NOT NULL,
    mode_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    developer_instructions TEXT NOT NULL DEFAULT '',
    sandbox_policy TEXT NOT NULL DEFAULT 'read_only',
    approval_policy TEXT NOT NULL DEFAULT 'deny_all',
    allowed_tools_json TEXT NOT NULL DEFAULT '[]',
    denied_tools_json TEXT NOT NULL DEFAULT '[]',
    can_write_files INTEGER NOT NULL DEFAULT 0 CHECK (can_write_files IN (0, 1)),
    can_execute_commands INTEGER NOT NULL DEFAULT 0 CHECK (can_execute_commands IN (0, 1)),
    can_create_child_tasks INTEGER NOT NULL DEFAULT 0 CHECK (can_create_child_tasks IN (0, 1)),
    can_send_agent_messages INTEGER NOT NULL DEFAULT 0 CHECK (can_send_agent_messages IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, mode_id, policy_version)
);

CREATE TABLE IF NOT EXISTS routes (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    active_agent_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id)
);

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    mode_id TEXT NOT NULL,
    profile_version INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    thread_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (agent_id, profile_version)
      REFERENCES agent_profiles(agent_id, profile_version),
    FOREIGN KEY (agent_id, mode_id, policy_version)
      REFERENCES agent_modes(agent_id, mode_id, policy_version)
);

CREATE TABLE IF NOT EXISTS inbound_messages (
    message_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    source_sequence INTEGER,
    context_token TEXT,
    text TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    status TEXT NOT NULL DEFAULT 'received' CHECK (status IN ({','.join(repr(x) for x in _INBOUND_STATES)})),
    received_at TEXT NOT NULL,
    stored_at TEXT NOT NULL,
    task_id TEXT,
    UNIQUE (channel, bot_id, external_message_id)
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    dedupe_key TEXT UNIQUE,
    inbound_message_id TEXT,
    channel TEXT NOT NULL DEFAULT '',
    bot_id TEXT NOT NULL DEFAULT '',
    external_user_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT 'default',
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    thread_id TEXT,
    mode_id TEXT NOT NULL,
    profile_version INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    reasoning_effort TEXT NOT NULL DEFAULT '',
    reply_target_json TEXT NOT NULL DEFAULT '{{}}',
    inputs_json TEXT NOT NULL DEFAULT '{{}}',
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ({','.join(repr(x) for x in _TASK_STATES)})),
    claimed_by TEXT,
    claim_token TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    last_error TEXT,
    result_json TEXT,
    parent_task_id TEXT,
    child_depth INTEGER NOT NULL DEFAULT 0 CHECK (child_depth >= 0),
    request_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    terminal_at TEXT,
    cancel_requested_at TEXT,
    UNIQUE (inbound_message_id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
    ,FOREIGN KEY (agent_id, profile_version)
      REFERENCES agent_profiles(agent_id, profile_version)
    ,FOREIGN KEY (agent_id, mode_id, policy_version)
      REFERENCES agent_modes(agent_id, mode_id, policy_version)
);

CREATE TABLE IF NOT EXISTS task_executions (
    execution_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    state TEXT NOT NULL CHECK (state IN ({','.join(repr(x) for x in _EXECUTION_STATES)})),
    worker_id TEXT,
    claim_token TEXT,
    lease_expires_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    last_error TEXT,
    external_turn_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (task_id, attempt),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    execution_id TEXT,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK (visibility IN ('internal', 'user')),
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 3),
    content TEXT NOT NULL DEFAULT '',
    attachments_json TEXT NOT NULL DEFAULT '[]',
    destination_agent_id TEXT,
    request_id TEXT,
    reply_to_id TEXT,
    causation_id TEXT,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (task_id, sequence),
    UNIQUE (task_id, idempotency_key),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (execution_id) REFERENCES task_executions(execution_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    request_id TEXT,
    reply_to_id TEXT,
    causation_id TEXT,
    task_id TEXT,
    source_agent_id TEXT,
    destination_agent_id TEXT,
    visibility TEXT NOT NULL DEFAULT 'internal' CHECK (visibility IN ('internal', 'user')),
    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 3),
    content TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS attachments (
    attachment_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT,
    mime_type TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    checksum TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'ready'
);

CREATE TABLE IF NOT EXISTS attachment_refs (
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT '',
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (owner_kind, owner_id, attachment_id, role, ordinal),
    FOREIGN KEY (attachment_id) REFERENCES attachments(attachment_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transcription_candidates (
    confirmation_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    attachment_id TEXT,
    candidate_text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'rejected', 'expired', 'consumed')),
    created_at TEXT NOT NULL,
    expires_at TEXT,
    resolved_at TEXT,
    resolved_by TEXT,
    consumed_by_task_id TEXT,
    FOREIGN KEY (attachment_id) REFERENCES attachments(attachment_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS user_outbox (
    outbox_id TEXT PRIMARY KEY,
    event_id TEXT,
    task_id TEXT,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_message_id TEXT,
    source_sequence INTEGER,
    context_token TEXT,
    reply_target_json TEXT NOT NULL DEFAULT '{{}}',
    content TEXT NOT NULL DEFAULT '',
    attachments_json TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 3),
    delivery_mode TEXT NOT NULL DEFAULT 'push_eligible' CHECK (delivery_mode IN ('inbox_only', 'push_eligible', 'requires_attention')),
    notify_enabled INTEGER NOT NULL DEFAULT 1 CHECK (notify_enabled IN (0, 1)),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ({','.join(repr(x) for x in _OUTBOX_STATES)})),
    presentation TEXT NOT NULL DEFAULT 'unseen' CHECK (presentation IN ('unseen', 'presented', 'acknowledged')),
    client_id TEXT NOT NULL UNIQUE,
    claimed_by TEXT,
    claim_token TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    presented_at TEXT,
    acknowledged_at TEXT,
    UNIQUE (event_id, channel, bot_id, external_user_id, session_id),
    FOREIGN KEY (event_id) REFERENCES messages(message_id) ON DELETE SET NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_mailbox (
    mailbox_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL UNIQUE,
    request_id TEXT NOT NULL,
    reply_to_id TEXT,
    causation_id TEXT,
    source_agent_id TEXT NOT NULL,
    destination_agent_id TEXT NOT NULL,
    task_id TEXT,
    content TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    execution_snapshot_json TEXT NOT NULL DEFAULT '{{}}',
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ({','.join(repr(x) for x in _MAILBOX_STATES)})),
    claimed_by TEXT,
    claim_token TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT,
    FOREIGN KEY (message_id) REFERENCES messages(message_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS notification_preferences (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    notify_enabled INTEGER NOT NULL DEFAULT 1 CHECK (notify_enabled IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_inbound_task ON inbound_messages(task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(state, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_conversation ON tasks(channel, bot_id, external_user_id, session_id, agent_id, state);
CREATE INDEX IF NOT EXISTS idx_tasks_request ON tasks(request_id);
CREATE INDEX IF NOT EXISTS idx_executions_task ON task_executions(task_id, attempt);
CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, sequence);
CREATE INDEX IF NOT EXISTS idx_task_events_request ON task_events(request_id);
CREATE INDEX IF NOT EXISTS idx_outbox_recipient ON user_outbox(channel, bot_id, external_user_id, session_id, agent_id, state, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_queue ON user_outbox(state, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_presentation ON user_outbox(agent_id, presentation, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_mailbox_destination ON agent_mailbox(destination_agent_id, state, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_mailbox_request ON agent_mailbox(request_id);
"""


_MIGRATION_2 = """
CREATE TABLE IF NOT EXISTS session_modes (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    mode_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    authorized_by TEXT,
    authorized_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id, agent_id)
);

CREATE TABLE IF NOT EXISTS channel_cursors (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    cursor TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id)
);

CREATE TABLE IF NOT EXISTS child_task_counters (
    parent_task_id TEXT PRIMARY KEY,
    child_count INTEGER NOT NULL DEFAULT 0 CHECK (child_count >= 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (parent_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mailbox_destination_request
    ON agent_mailbox(destination_agent_id, request_id);
CREATE INDEX IF NOT EXISTS idx_session_modes_user
    ON session_modes(channel, bot_id, external_user_id, session_id, agent_id);
"""


_MIGRATION_3 = """
CREATE INDEX IF NOT EXISTS idx_attachments_state
    ON attachments(state, created_at);
"""


_MIGRATION_4 = """
CREATE INDEX IF NOT EXISTS idx_profiles_enabled
    ON agent_profiles(agent_id, enabled, profile_version);
"""


_MIGRATION_5 = """
CREATE TABLE IF NOT EXISTS thread_bindings (
    conversation_id TEXT NOT NULL,
    mode_id TEXT NOT NULL,
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    thread_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (conversation_id, mode_id, profile_version, policy_version),
    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_thread_bindings_thread ON thread_bindings(thread_id);
"""


_MIGRATION_6 = """
CREATE INDEX IF NOT EXISTS idx_messages_request ON messages(request_id);
CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to_id);
CREATE INDEX IF NOT EXISTS idx_attachment_refs_attachment ON attachment_refs(attachment_id);
"""


# ``ordinal`` is part of an attachment reference's identity.  Migration 1
# predates ordered multi-part inputs and used a key that omitted it; rebuild
# that table for databases created by an earlier version.  Keeping this as a
# separate migration preserves the append-only migration contract.
_MIGRATION_7 = """
DROP TABLE IF EXISTS attachment_refs_new;
CREATE TABLE attachment_refs_new (
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT '',
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (owner_kind, owner_id, attachment_id, role, ordinal),
    FOREIGN KEY (attachment_id) REFERENCES attachments(attachment_id) ON DELETE CASCADE
);
INSERT OR IGNORE INTO attachment_refs_new
    (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
SELECT owner_kind, owner_id, attachment_id, role, ordinal, created_at
FROM attachment_refs;
DROP TABLE attachment_refs;
ALTER TABLE attachment_refs_new RENAME TO attachment_refs;
CREATE INDEX IF NOT EXISTS idx_attachment_refs_attachment ON attachment_refs(attachment_id);
"""

_MIGRATION_8 = """
CREATE TABLE IF NOT EXISTS audit_events (
    audit_id TEXT PRIMARY KEY,
    actor TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    allowed INTEGER NOT NULL DEFAULT 0 CHECK (allowed IN (0, 1)),
    reason TEXT NOT NULL DEFAULT '',
    source_agent_id TEXT,
    destination_agent_id TEXT,
    request_type TEXT,
    task_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_task ON audit_events(task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_agents ON audit_events(source_agent_id, destination_agent_id, created_at);

CREATE TABLE IF NOT EXISTS outgoing_media (
    media_id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL,
    outbox_id TEXT,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL DEFAULT '',
    external_user_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'ready'
        CHECK (state IN ('local','ready','upload_pending','uploading','uploaded','send_pending','sent','failed')),
    idempotency_key TEXT NOT NULL UNIQUE,
    remote_id TEXT,
    upload_param TEXT,
    encryption_key TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    claimed_by TEXT,
    claim_token TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    uploaded_at TEXT,
    sent_at TEXT,
    FOREIGN KEY (attachment_id) REFERENCES attachments(attachment_id) ON DELETE RESTRICT,
    FOREIGN KEY (outbox_id) REFERENCES user_outbox(outbox_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_outgoing_media_queue ON outgoing_media(state, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outgoing_media_outbox ON outgoing_media(outbox_id);
"""

# Legacy transcription-candidate rows are linked to the immutable inbound
# envelope for migration and audit. Current channel ingress does not create
# them; the compatibility state machine remains available to older callers.
_MIGRATION_9 = """
CREATE INDEX IF NOT EXISTS idx_transcription_inbound
    ON transcription_candidates(inbound_message_id);
"""

# Confirmation ownership includes the exact route policy selected when audio
# was accepted.  These columns are deliberately nullable for rows created by
# pre-v10 stores: a deterministic replay can repair the missing snapshot, while
# new rows always persist concrete values.
_MIGRATION_10 = """
CREATE INDEX IF NOT EXISTS idx_transcription_route
    ON transcription_candidates(agent_id, mode_id, profile_version, policy_version);
"""

# Command ingress, command effects, and the user-outbox projection cannot all
# share one transaction because some control operations also interrupt a live
# runtime.  This receipt closes the replay gap conservatively: a completed
# command can restore its exact response after a crash, while a command whose
# outcome was in flight is never blindly executed a second time.
_MIGRATION_11 = """
CREATE TABLE IF NOT EXISTS command_receipts (
    command_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL,
    command_name TEXT NOT NULL,
    command_args_json TEXT NOT NULL DEFAULT '[]',
    command_text TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'started'
        CHECK (state IN ('started', 'completed', 'interrupted')),
    response_text TEXT,
    response_agent_id TEXT,
    presentation_ids_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    interrupted_at TEXT,
    UNIQUE (channel, bot_id, external_user_id, session_id, external_message_id)
);
CREATE INDEX IF NOT EXISTS idx_command_receipts_state
    ON command_receipts(state, created_at);
"""


# Foreground/background delivery semantics are selected when a user event is
# projected.  They cannot be reconstructed from the live front-Agent route
# later because the user may have switched Agents in the meantime.
_MIGRATION_12 = """
ALTER TABLE user_outbox ADD COLUMN foreground INTEGER NOT NULL DEFAULT 0
    CHECK (foreground IN (0, 1));
"""

# Mailbox execution is a real Agent task boundary.  Persist the immutable
# destination route/policy snapshot with the projection so a restart or a
# later Profile/Mode change cannot make an old request share a different
# thread or silently acquire new capabilities.
_MIGRATION_13 = """
ALTER TABLE agent_mailbox ADD COLUMN execution_snapshot_json TEXT NOT NULL DEFAULT '{}';
"""


# Model selection is user/session configuration, but it is also Agent-local:
# two named Agents may intentionally use different models for the same WeChat
# conversation.  Keep the mutable preference separate from immutable task
# snapshots; new tasks copy these values when they are accepted.
_MIGRATION_14 = """
CREATE TABLE IF NOT EXISTS session_model_preferences (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    model_id TEXT NOT NULL DEFAULT '',
    reasoning_effort TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_session_model_preferences_user
    ON session_model_preferences(
        channel, bot_id, external_user_id, session_id, agent_id
);
"""


# Skill discovery may come from the SDK, but tasks refer to a deployment-owned
# immutable version.  Keep every version Agent-scoped so aliases and future
# runtimes cannot accidentally resolve each other's trusted paths.
_MIGRATION_15 = """
CREATE TABLE IF NOT EXISTS skills (
    agent_id TEXT NOT NULL CHECK (length(trim(agent_id)) > 0),
    skill_id TEXT NOT NULL CHECK (length(trim(skill_id)) > 0),
    version TEXT NOT NULL CHECK (length(trim(version)) > 0),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    path TEXT NOT NULL CHECK (length(trim(path)) > 0),
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    content_hash TEXT NOT NULL CHECK (length(trim(content_hash)) > 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, skill_id, version)
);

CREATE INDEX IF NOT EXISTS idx_skills_agent_enabled
    ON skills(agent_id, enabled, skill_id, version);
"""

_MIGRATION_16 = """
CREATE TABLE IF NOT EXISTS deleted_agents (
    agent_id TEXT PRIMARY KEY,
    deleted_at TEXT NOT NULL
);
"""


# Built-in mode v2 grants command and network access in every selectable mode.
# Session rows are mutable future-task preferences, so move them to v2 while
# leaving tasks, conversations, and thread bindings pinned to their immutable
# v1 snapshots. A populated execute authorization is the durable evidence of
# the user's explicit `/mode execute` choice, so migrate that evidence with
# the mutable session selection instead of leaving execute selected but
# unusable. Missing authorization remains missing and fails closed.
_MIGRATION_17 = """
UPDATE session_modes
SET policy_version=2,
    updated_at=CURRENT_TIMESTAMP
WHERE policy_version=1
  AND agent_id='codex'
  AND mode_id IN ('chat','plan','review','execute');
"""


# Existing `/agent` aliases are generated from the same built-in Mode registry
# as Codex, but schema 17 migrated only the static `codex` session rows. Move
# the mutable future-task selections for proven generated aliases to v2 as
# well. The migration seeds their immutable v2 definitions transactionally
# before applying this update; arbitrary custom Agent profiles remain pinned.
_MIGRATION_18 = """
UPDATE session_modes
SET policy_version=2,
    updated_at=CURRENT_TIMESTAMP
WHERE policy_version=1
  AND mode_id IN ('chat','plan','review','execute')
  AND EXISTS (
      SELECT 1
      FROM agent_profiles
      WHERE agent_profiles.agent_id=session_modes.agent_id
        AND agent_profiles.summary='Named Codex Agent created through /agent.'
  );
"""


# One exact inbound envelope owns one immutable ten-SendMsg reply allowance.
# Candidates and fragments retain completed-item boundaries even when the
# allowance is exhausted; `/recv` moves only those retained fragments into a
# fresh inbound scope.  `reply_slots` is the single authority for logical
# sends across text and media projections.
_MIGRATION_19 = """
CREATE TABLE IF NOT EXISTS reply_scopes (
    reply_scope_id TEXT PRIMARY KEY,
    inbound_message_id TEXT NOT NULL UNIQUE,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    source_identity TEXT NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 10 CHECK (capacity = 10),
    used_slots INTEGER NOT NULL DEFAULT 0
        CHECK (used_slots BETWEEN 0 AND capacity),
    created_at TEXT NOT NULL,
    FOREIGN KEY (inbound_message_id) REFERENCES inbound_messages(message_id)
        ON DELETE CASCADE,
    UNIQUE (channel, bot_id, external_user_id, session_id, source_identity)
);

CREATE TABLE IF NOT EXISTS reply_candidates (
    reply_candidate_id TEXT PRIMARY KEY,
    origin_reply_scope_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    agent_id TEXT NOT NULL DEFAULT 'codex',
    task_id TEXT,
    execution_id TEXT,
    event_id TEXT,
    source_item_id TEXT,
    source_item_type TEXT,
    source_item_ordinal INTEGER CHECK (
        source_item_ordinal IS NULL OR source_item_ordinal >= 0
    ),
    content TEXT NOT NULL DEFAULT '',
    attachments_json TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 3),
    delivery_mode TEXT NOT NULL DEFAULT 'push_eligible'
        CHECK (delivery_mode IN ('inbox_only','push_eligible','requires_attention')),
    notify_enabled INTEGER NOT NULL DEFAULT 1 CHECK (notify_enabled IN (0, 1)),
    foreground INTEGER NOT NULL DEFAULT 0 CHECK (foreground IN (0, 1)),
    created_at TEXT NOT NULL,
    FOREIGN KEY (origin_reply_scope_id) REFERENCES reply_scopes(reply_scope_id)
        ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL,
    FOREIGN KEY (execution_id) REFERENCES task_executions(execution_id)
        ON DELETE SET NULL,
    FOREIGN KEY (event_id) REFERENCES task_events(event_id) ON DELETE SET NULL,
    UNIQUE (origin_reply_scope_id, source_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reply_candidate_source_item
    ON reply_candidates(task_id, execution_id, source_item_id)
    WHERE task_id IS NOT NULL AND execution_id IS NOT NULL
      AND source_item_id IS NOT NULL AND length(trim(source_item_id)) > 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_reply_candidate_source_ordinal
    ON reply_candidates(task_id, execution_id, source_item_ordinal)
    WHERE task_id IS NOT NULL AND execution_id IS NOT NULL
      AND (source_item_id IS NULL OR length(trim(source_item_id)) = 0)
      AND source_item_ordinal IS NOT NULL;

CREATE TABLE IF NOT EXISTS reply_deferred_counters (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    PRIMARY KEY (channel, bot_id, external_user_id, session_id)
);

CREATE TABLE IF NOT EXISTS reply_fragments (
    reply_fragment_id TEXT PRIMARY KEY,
    reply_candidate_id TEXT NOT NULL,
    origin_reply_scope_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    fragment_ordinal INTEGER NOT NULL CHECK (fragment_ordinal > 0),
    fragment_kind TEXT NOT NULL
        CHECK (fragment_kind IN ('text','media','bundle')),
    content TEXT NOT NULL DEFAULT '',
    attachments_json TEXT NOT NULL DEFAULT '[]',
    state TEXT NOT NULL DEFAULT 'retained'
        CHECK (state IN ('retained','allocated','deferred_quota','inbox_only')),
    delivery_reply_scope_id TEXT,
    reply_slot_id TEXT,
    deferred_sequence INTEGER CHECK (
        deferred_sequence IS NULL OR deferred_sequence > 0
    ),
    created_at TEXT NOT NULL,
    allocated_at TEXT,
    FOREIGN KEY (reply_candidate_id)
        REFERENCES reply_candidates(reply_candidate_id) ON DELETE CASCADE,
    FOREIGN KEY (origin_reply_scope_id)
        REFERENCES reply_scopes(reply_scope_id) ON DELETE CASCADE,
    FOREIGN KEY (delivery_reply_scope_id)
        REFERENCES reply_scopes(reply_scope_id) ON DELETE SET NULL,
    UNIQUE (reply_candidate_id, fragment_ordinal),
    UNIQUE (reply_slot_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reply_fragment_deferred_sequence
    ON reply_fragments(
        channel, bot_id, external_user_id, session_id, deferred_sequence
    ) WHERE deferred_sequence IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_reply_fragment_deferred_fifo
    ON reply_fragments(
        channel, bot_id, external_user_id, session_id, state,
        deferred_sequence, reply_candidate_id, fragment_ordinal
    );

CREATE TABLE IF NOT EXISTS reply_slots (
    reply_slot_id TEXT PRIMARY KEY,
    reply_scope_id TEXT NOT NULL,
    reply_ordinal INTEGER NOT NULL CHECK (reply_ordinal BETWEEN 1 AND 10),
    reply_fragment_id TEXT NOT NULL UNIQUE,
    delivery_id TEXT NOT NULL UNIQUE,
    client_id TEXT NOT NULL UNIQUE,
    contextless_client_id TEXT UNIQUE,
    active_wire_variant TEXT NOT NULL DEFAULT 'primary'
        CHECK (active_wire_variant IN ('primary','contextless')),
    outbox_id TEXT UNIQUE,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (reply_scope_id) REFERENCES reply_scopes(reply_scope_id)
        ON DELETE CASCADE,
    FOREIGN KEY (reply_fragment_id)
        REFERENCES reply_fragments(reply_fragment_id) ON DELETE CASCADE,
    UNIQUE (reply_scope_id, reply_ordinal),
    CHECK (
        contextless_client_id IS NULL OR contextless_client_id <> client_id
    )
);

CREATE TABLE IF NOT EXISTS reply_drain_batches (
    reply_batch_id TEXT PRIMARY KEY,
    destination_reply_scope_id TEXT NOT NULL UNIQUE,
    source_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT 'default',
    created_at TEXT NOT NULL,
    FOREIGN KEY (destination_reply_scope_id)
        REFERENCES reply_scopes(reply_scope_id) ON DELETE CASCADE,
    UNIQUE (destination_reply_scope_id, source_key)
);

CREATE TABLE IF NOT EXISTS reply_drain_batch_items (
    reply_batch_id TEXT NOT NULL,
    batch_ordinal INTEGER NOT NULL CHECK (batch_ordinal > 0),
    reply_fragment_id TEXT NOT NULL UNIQUE,
    reply_slot_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (reply_batch_id, batch_ordinal),
    FOREIGN KEY (reply_batch_id) REFERENCES reply_drain_batches(reply_batch_id)
        ON DELETE CASCADE,
    FOREIGN KEY (reply_fragment_id)
        REFERENCES reply_fragments(reply_fragment_id) ON DELETE RESTRICT,
    FOREIGN KEY (reply_slot_id) REFERENCES reply_slots(reply_slot_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_reply_scope_recipient
    ON reply_scopes(channel, bot_id, external_user_id, session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_reply_slot_scope
    ON reply_slots(reply_scope_id, reply_ordinal);
"""

_LATEST_SCHEMA_VERSION = 19



class StoreError(RuntimeError):
    """Base exception for durable store errors."""


class NotFoundError(StoreError):
    pass


class InvalidTransition(StoreError):
    pass


@contextmanager
def _transaction(conn: sqlite3.Connection):
    """Run a short immediate transaction and always roll back on failure."""

    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def _executescript_atomic(conn: sqlite3.Connection, script: str) -> None:
    """Execute a schema script without ``sqlite3.executescript``.

    Python's ``executescript`` commits any open transaction before running the
    script.  That behavior is unsafe for migrations because a later failing
    statement can leave tables behind while ``schema_migrations`` still points
    at the previous version.  The migration scripts in this module contain
    ordinary DDL/DML statements, so execute each complete statement through the
    already-open transaction instead.  ``sqlite3.complete_statement`` handles
    quoted semicolons and multiline definitions for us.
    """

    buffer: list[str] = []
    for line in script.splitlines(keepends=True):
        buffer.append(line)
        candidate = "".join(buffer)
        if not sqlite3.complete_statement(candidate):
            continue
        statement = candidate.strip()
        buffer.clear()
        if statement:
            conn.execute(statement)
    trailing = "".join(buffer).strip()
    if trailing:
        conn.execute(trailing)


def _utc_text(value: datetime | str | None = None) -> str:
    return datetime_to_text(value or utcnow()) or datetime_to_text(utcnow())  # type: ignore[return-value]


def _normalized_authorization_evidence(
    actor: Any,
    authorized_at: Any,
) -> tuple[str, str] | None:
    """Return canonical execute authorization provenance or fail closed.

    Session rows can outlive the runtime that wrote them and can also arrive
    through a database import, so truthy strings are not sufficient evidence.
    Require a real, nonblank actor and an ISO-8601 timestamp that the runtime
    can parse, then persist/compare the timestamp in canonical UTC form.
    """

    if not isinstance(actor, str):
        return None
    actor_text = actor.strip()
    if not actor_text:
        return None
    if isinstance(authorized_at, str):
        authorized_at = authorized_at.strip()
        if not authorized_at:
            return None
    elif not isinstance(authorized_at, datetime):
        return None
    parsed = text_to_datetime(authorized_at)
    if parsed is None:
        return None
    timestamp_text = datetime_to_text(parsed)
    if not timestamp_text:
        return None
    return actor_text, timestamp_text


def _enum_value(value: Any, default: Any = None) -> Any:
    if value is None:
        value = default
    return getattr(value, "value", value)


def _uuid() -> str:
    return str(uuid.uuid4())


def _skill_version_key(value: str) -> tuple[tuple[int, Any], ...]:
    """Return a deterministic numeric-aware ordering for opaque versions."""

    return tuple(
        (1, int(part)) if part.isdigit() else (0, part.casefold())
        for part in re.split(r"(\d+)", str(value or ""))
    )


def _cursor_should_advance(existing: str | None, candidate: str | None) -> bool:
    """Return whether a durable channel cursor may be replaced.

    iLink cursors are normally opaque strings, so lexical ordering is not a
    valid general rule.  Numeric cursors are common in test doubles and some
    channel implementations, however; for those values a stale replay must
    never move the checkpoint backwards.  For opaque values we preserve the
    protocol's ordering by accepting the newest callback supplied by the
    single monitor and only suppressing an exact duplicate.
    """

    if candidate is None:
        return False
    candidate_text = str(candidate)
    if existing is None:
        return True
    existing_text = str(existing)
    if candidate_text == existing_text:
        return False
    try:
        existing_number = int(existing_text.strip())
        candidate_number = int(candidate_text.strip())
    except (TypeError, ValueError):
        return True
    return candidate_number >= existing_number


def _snapshot_value(value: Any, *, text_key: str = "text") -> Any:
    """Return a JSON-safe task snapshot without assuming Mapping inputs.

    The SDK-independent Agent contract permits a string or a sequence of
    text/media values, while the SQLite row historically used a mapping.  Do
    not call ``dict(value)`` on those valid forms (which raises for strings or
    mangles sequences); preserve mappings and wrap scalar values explicitly.
    """

    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (str, bytes, bytearray)):
        return {text_key: value.decode() if isinstance(value, bytes) else str(value)}
    return value


class SQLiteStore:
    """Durable asynchronous SQLite store.

    Every public method commits its own transaction unless its name ends in
    ``_in_transaction`` (those helpers are intentionally private).  Callers
    should treat returned models as snapshots; mutating them never mutates the
    database.  ``:memory:`` is supported for tests and keeps one connection for
    the store lifetime.
    """

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], datetime] | None = None,
        attachment_root: str | Path | None = None,
        transcription_ttl_seconds: float = DEFAULT_TRANSCRIPTION_TTL_SECONDS,
    ) -> None:
        raw_path = str(path)
        # SQLite does not expand shell syntax.  Canonicalize file-backed paths
        # once so ``~/...`` and relative aliases point at the directory whose
        # permissions/startup ownership we actually validate.  Keep the special
        # in-memory name untouched.
        self.path = (
            raw_path
            if raw_path == ":memory:"
            else str(Path(raw_path).expanduser().resolve())
        )
        self.busy_timeout_ms = max(0, int(busy_timeout_ms))
        self.clock = clock or utcnow
        try:
            transcription_ttl = float(transcription_ttl_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("transcription_ttl_seconds must be a finite number") from exc
        if not math.isfinite(transcription_ttl) or transcription_ttl < 0:
            raise ValueError(
                "transcription_ttl_seconds must be a finite nonnegative number"
            )
        self.transcription_ttl_seconds = transcription_ttl
        self.attachment_root = (
            Path(attachment_root).expanduser().resolve()
            if attachment_root is not None
            else None
        )
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="runtime-sqlite")
        self._conn: sqlite3.Connection | None = None
        self._live_database_key: tuple[int, str] | None = None
        # The first durable opener performs strong process-boundary recovery
        # synchronously before publishing itself in ``_LIVE_DATABASES``.  Keep
        # that report until the lifecycle owner calls ``startup_reconcile`` so
        # callers retain the existing diagnostics without repeating recovery.
        self._startup_recovery_report: RecoveryReport | None = None
        self._initialized = False
        self._closed = False
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "SQLiteStore":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def initialize(self) -> None:
        """Open the connection and apply idempotent migrations."""

        async with self._lock:
            if self._initialized:
                return
            if self._closed:
                raise StoreError("store is closed")
            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(self._executor, self._open_sync)
            try:
                _result, cancellation = await self._drain_executor_future(future)
            finally:
                self._initialized = self._conn is not None
            if cancellation is not None:
                raise cancellation

    # Common lifecycle aliases used by callers.
    start = initialize
    open = initialize

    async def close(self) -> None:
        cancellation: asyncio.CancelledError | None = None
        async with self._lock:
            if self._closed:
                return
            if self._conn is not None:
                loop = asyncio.get_running_loop()
                future = loop.run_in_executor(self._executor, self._close_sync)
                _result, cancellation = await self._drain_executor_future(future)
            self._closed = True
            self._initialized = False
        self._executor.shutdown(wait=True, cancel_futures=False)
        if cancellation is not None:
            raise cancellation

    async def dispose(self) -> None:
        await self.close()

    shutdown = close

    @staticmethod
    async def _drain_executor_future(
        future: asyncio.Future[Any],
    ) -> tuple[Any, asyncio.CancelledError | None]:
        """Wait for executor work without letting repeated cancellation un-fence it."""

        cancellation: asyncio.CancelledError | None = None
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError as exc:
                # Keep the caller inside its lifecycle/store lock until the
                # executor finishes. A task may be cancelled more than once;
                # every request is deferred until the synchronous operation is
                # no longer able to touch the connection.
                cancellation = exc
        # A synchronous database/open/close failure takes precedence over a
        # cancellation that arrived while it was running.
        return future.result(), cancellation

    def _open_sync(self) -> None:
        """Open/migrate a connection and close it if initialization fails."""

        try:
            if self.path == ":memory:":
                self._open_sync_impl(recover_startup_state=True)
                return

            resolved_path = Path(self.path).expanduser().resolve()
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            database_key = (os.getpid(), str(resolved_path))
            # Serialize process-local ownership transfer with close().  The
            # first successful opener performs crash recovery before it is
            # published as live; a concurrent opener can neither skip that
            # recovery after a failed initialization nor create a new receipt
            # that the first opener then marks interrupted.
            with _LIVE_DATABASES_LOCK:
                live_count = _LIVE_DATABASES.get(database_key, 0)
                self._open_sync_impl(
                    recover_startup_state=live_count == 0
                )
                _LIVE_DATABASES[database_key] = live_count + 1
                self._live_database_key = database_key
        except BaseException:
            # Migrations are transactional, but the connection itself must
            # also be released when a migration or pragma fails.  Otherwise a
            # failed initialize can retain a file lock until process exit.
            self._startup_recovery_report = None
            self._close_sync()
            raise

    def _open_sync_impl(self, *, recover_startup_state: bool) -> None:
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(
                parents=True, exist_ok=True
            )
        conn = sqlite3.connect(
            self.path,
            timeout=max(0.001, self.busy_timeout_ms / 1000),
            check_same_thread=False,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        # Publish the connection before migrations so the wrapper can close it
        # if any schema statement fails.
        self._conn = conn
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        conn.execute("PRAGMA synchronous = NORMAL")
        # WAL is unavailable for an in-memory database; SQLite simply reports
        # ``memory`` there, which is fine for tests.
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:
            logger.debug("WAL unavailable for %s", self.path, exc_info=True)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied_versions = tuple(
            int(row[0])
            for row in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        )
        current = applied_versions[-1] if applied_versions else 0
        if current > _LATEST_SCHEMA_VERSION:
            raise StoreError(
                "database schema is newer than this runtime: "
                f"{current} > {_LATEST_SCHEMA_VERSION}"
            )
        if applied_versions != tuple(range(1, current + 1)):
            raise StoreError("database schema migration history is incomplete")
        if current < 1:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_1)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (1, _utc_text()),
                )
        if current < 2:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_2)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (2, _utc_text()),
                )
        if current < 3:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
                }
                if "metadata_json" not in columns:
                    conn.execute(
                        "ALTER TABLE tasks ADD COLUMN metadata_json "
                        "TEXT NOT NULL DEFAULT '{}'"
                    )
                _executescript_atomic(conn, _MIGRATION_3)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (3, _utc_text()),
                )
        if current < 4:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute("PRAGMA table_info(agent_profiles)").fetchall()
                }
                if "denied_request_types_json" not in columns:
                    conn.execute(
                        "ALTER TABLE agent_profiles ADD COLUMN "
                        "denied_request_types_json TEXT NOT NULL DEFAULT '[]'"
                    )
                if "default_mode_id" not in columns:
                    conn.execute(
                        "ALTER TABLE agent_profiles ADD COLUMN "
                        "default_mode_id TEXT NOT NULL DEFAULT 'chat'"
                    )
                _executescript_atomic(conn, _MIGRATION_4)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (4, _utc_text()),
                )
        if current < 5:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_5)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (5, _utc_text()),
                )
        if current < 6:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_6)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (6, _utc_text()),
                )
        if current < 7:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_7)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (7, _utc_text()),
                )
        if current < 8:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_8)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (8, _utc_text()),
                )
        if current < 9:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(transcription_candidates)"
                    ).fetchall()
                }
                if "inbound_message_id" not in columns:
                    conn.execute(
                        "ALTER TABLE transcription_candidates ADD COLUMN "
                        "inbound_message_id TEXT REFERENCES inbound_messages(message_id) "
                        "ON DELETE SET NULL"
                    )
                _executescript_atomic(conn, _MIGRATION_9)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (9, _utc_text()),
                )
        if current < 10:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(transcription_candidates)"
                    ).fetchall()
                }
                for name, definition in (
                    ("mode_id", "TEXT"),
                    ("profile_version", "INTEGER"),
                    ("policy_version", "INTEGER"),
                    ("metadata_json", "TEXT"),
                ):
                    if name not in columns:
                        conn.execute(
                            f"ALTER TABLE transcription_candidates ADD COLUMN {name} {definition}"
                        )
                # Legacy candidates had no policy snapshot.  Give them an
                # explicit conservative snapshot rather than allowing a later
                # confirmation to recompute the current session mode.
                conn.execute(
                    "UPDATE transcription_candidates SET mode_id=COALESCE(mode_id,'chat'), "
                    "profile_version=COALESCE(profile_version,1), "
                    "policy_version=COALESCE(policy_version,1), "
                    "metadata_json=COALESCE(metadata_json,'{}')"
                )
                _executescript_atomic(conn, _MIGRATION_10)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (10, _utc_text()),
                )
        if current < 11:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_11)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (11, _utc_text()),
                )
        if current < 12:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(user_outbox)"
                    ).fetchall()
                }
                if "foreground" not in columns:
                    _executescript_atomic(conn, _MIGRATION_12)
                # Preserve the foreground rows that can be identified from
                # pre-v12 data without consulting a mutable current route.
                conn.execute(
                    """UPDATE user_outbox SET foreground=1
                       WHERE outbox_id LIKE 'command:%'
                          OR (
                              priority=1 AND notify_enabled=1
                              AND task_id IN (
                                  SELECT task_id FROM tasks
                                  WHERE inbound_message_id IS NOT NULL
                              )
                          )"""
                )
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (12, _utc_text()),
                )
        if current < 13:
            with _transaction(conn):
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(agent_mailbox)"
                    ).fetchall()
                }
                if "execution_snapshot_json" not in columns:
                    _executescript_atomic(conn, _MIGRATION_13)
                # Legacy rows have no trustworthy destination context.  Keep
                # an explicit empty marker so workers can apply the
                # request-scoped compatibility isolation path instead of
                # assuming a shared Agent conversation.
                conn.execute(
                    "UPDATE agent_mailbox SET execution_snapshot_json=COALESCE(execution_snapshot_json,'{}')"
                )
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (13, _utc_text()),
                )
        if current < 14:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_14)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (14, _utc_text()),
                )
        if current < 15:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_15)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (15, _utc_text()),
                )
        if current < 16:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_16)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (16, _utc_text()),
                )
        if current < 17:
            with _transaction(conn):
                # The migration references mode v2. Seed both immutable
                # generations in the same transaction before switching any
                # mutable session preference to the new version.
                self._seed_defaults(conn)
                _executescript_atomic(conn, _MIGRATION_17)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (17, _utc_text()),
                )
        if current < 18:
            with _transaction(conn):
                # Generated aliases own Agent-scoped immutable mode rows. Seed
                # their v2 targets before moving mutable session selections.
                self._seed_dynamic_mode_v2(conn)
                _executescript_atomic(conn, _MIGRATION_18)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (18, _utc_text()),
                )
        if current < 19:
            with _transaction(conn):
                _executescript_atomic(conn, _MIGRATION_19)
                outbox_columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(user_outbox)"
                    ).fetchall()
                }
                for name, definition in (
                    (
                        "reply_scope_id",
                        "TEXT REFERENCES reply_scopes(reply_scope_id) ON DELETE RESTRICT",
                    ),
                    (
                        "reply_slot_id",
                        "TEXT REFERENCES reply_slots(reply_slot_id) ON DELETE RESTRICT",
                    ),
                    (
                        "reply_ordinal",
                        "INTEGER CHECK (reply_ordinal IS NULL OR reply_ordinal BETWEEN 1 AND 10)",
                    ),
                    (
                        "reply_candidate_id",
                        "TEXT REFERENCES reply_candidates(reply_candidate_id) ON DELETE RESTRICT",
                    ),
                    (
                        "reply_fragment_id",
                        "TEXT REFERENCES reply_fragments(reply_fragment_id) ON DELETE RESTRICT",
                    ),
                    ("from_user_id", "TEXT NOT NULL DEFAULT ''"),
                    ("contextless_client_id", "TEXT"),
                    (
                        "active_wire_variant",
                        "TEXT NOT NULL DEFAULT 'primary' CHECK "
                        "(active_wire_variant IN ('primary','contextless'))",
                    ),
                ):
                    if name not in outbox_columns:
                        conn.execute(
                            f"ALTER TABLE user_outbox ADD COLUMN {name} {definition}"
                        )
                event_columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(task_events)"
                    ).fetchall()
                }
                for name, definition in (
                    ("source_item_id", "TEXT"),
                    ("source_item_type", "TEXT"),
                    (
                        "source_item_ordinal",
                        "INTEGER CHECK (source_item_ordinal IS NULL OR source_item_ordinal >= 0)",
                    ),
                ):
                    if name not in event_columns:
                        conn.execute(
                            f"ALTER TABLE task_events ADD COLUMN {name} {definition}"
                        )
                execution_columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(task_executions)"
                    ).fetchall()
                }
                if "delivery_reply_scope_id" not in execution_columns:
                    conn.execute(
                        "ALTER TABLE task_executions ADD COLUMN "
                        "delivery_reply_scope_id TEXT REFERENCES "
                        "reply_scopes(reply_scope_id) ON DELETE SET NULL"
                    )
                task_columns = {
                    str(row[1])
                    for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
                }
                if "pending_delivery_reply_scope_id" not in task_columns:
                    conn.execute(
                        "ALTER TABLE tasks ADD COLUMN "
                        "pending_delivery_reply_scope_id TEXT REFERENCES "
                        "reply_scopes(reply_scope_id) ON DELETE SET NULL"
                    )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_reply_slot "
                    "ON user_outbox(reply_slot_id) WHERE reply_slot_id IS NOT NULL"
                )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_contextless_client "
                    "ON user_outbox(contextless_client_id) "
                    "WHERE contextless_client_id IS NOT NULL"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_task_events_source_item "
                    "ON task_events(task_id, execution_id, source_item_id, "
                    "source_item_ordinal)"
                )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "idx_task_events_source_item_unique ON task_events"
                    "(task_id, execution_id, source_item_id) "
                    "WHERE execution_id IS NOT NULL AND source_item_id IS NOT NULL "
                    "AND length(trim(source_item_id)) > 0"
                )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "idx_task_events_source_ordinal_unique ON task_events"
                    "(task_id, execution_id, source_item_ordinal) "
                    "WHERE execution_id IS NOT NULL "
                    "AND (source_item_id IS NULL OR length(trim(source_item_id))=0) "
                    "AND source_item_ordinal IS NOT NULL"
                )
                inbound_rows = conn.execute(
                    "SELECT * FROM inbound_messages ORDER BY stored_at, message_id"
                ).fetchall()
                for inbound_row in inbound_rows:
                    self._ensure_reply_scope_for_inbound_row_tx(conn, inbound_row)
                conn.execute(
                    """UPDATE task_executions
                       SET delivery_reply_scope_id=(
                           SELECT rs.reply_scope_id
                           FROM tasks t
                           JOIN reply_scopes rs
                             ON rs.inbound_message_id=t.inbound_message_id
                           WHERE t.task_id=task_executions.task_id
                       )
                       WHERE delivery_reply_scope_id IS NULL"""
                )
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (19, _utc_text()),
                )
        # v19 originally added ``from_user_id`` with an empty compatibility
        # default.  Repair both freshly migrated rows and databases that were
        # already marked v19 before the sender backfill was introduced.  The
        # channel conversion layer can fall back to ``bot_id``, but keeping the
        # durable row explicit is required for deterministic multi-client
        # resolver selection after restart.
        outbox_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(user_outbox)").fetchall()
        }
        if "from_user_id" in outbox_columns:
            with _transaction(conn):
                conn.execute(
                    "UPDATE user_outbox SET from_user_id=bot_id "
                    "WHERE length(trim(COALESCE(from_user_id, '')))=0"
                )
        if current >= 18:
            # Publish all built-in immutable definitions atomically.  A
            # conflict must not leave a partially seeded catalog behind.
            with _transaction(conn):
                self._seed_defaults(conn)
        # The first connection represents this process taking ownership of
        # the durable database. Receipts and leased work it finds active have
        # uncertain effects from the previous process, so recover them before
        # this connection is published as live. A second live connection is
        # not a restart and must leave current owners alone.
        if recover_startup_state:
            startup_report: RecoveryReport
            with _transaction(conn):
                now_text = self._now()
                conn.execute(
                    "UPDATE command_receipts SET state='interrupted', "
                    "interrupted_at=COALESCE(interrupted_at, ?) "
                    "WHERE state='started'",
                    (now_text,),
                )
                startup_report = self._recover_startup_state_tx(
                    conn,
                    now_text=now_text,
                )
            # Publish diagnostics only after the recovery transaction commits.
            self._startup_recovery_report = startup_report
        self._conn = conn

    def _close_sync(self) -> None:
        database_key = self._live_database_key
        if database_key is None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
            return

        # Serialize the final connection close with process-local opens.  An
        # opener must never observe this store as live after its SQLite
        # connection has already closed, or it could skip abandoned-receipt
        # recovery and leave a command permanently in ``started``.
        with _LIVE_DATABASES_LOCK:
            try:
                if self._conn is not None:
                    self._conn.close()
                    self._conn = None
            finally:
                if self._live_database_key is not None:
                    live_count = _LIVE_DATABASES.get(database_key, 0)
                    if live_count <= 1:
                        _LIVE_DATABASES.pop(database_key, None)
                    else:
                        _LIVE_DATABASES[database_key] = live_count - 1
                    self._live_database_key = None

    def _recover_startup_state_tx(
        self,
        conn: sqlite3.Connection,
        *,
        now_text: str,
    ) -> RecoveryReport:
        """Recover ownership left by a previous process inside an open TX.

        This runs while ``_LIVE_DATABASES_LOCK`` still serializes the first
        process-local opener.  Keeping the full recovery here prevents another
        connection from publishing new claims between detecting a process
        boundary and reclaiming the old owners.
        """

        recovery_reason = "process restarted before claim completion"
        # Repair counters left by the pre-atomic reserve/create path. Durable
        # child rows are the source of truth after restart.
        conn.execute(
            """UPDATE child_task_counters
               SET child_count=(
                       SELECT COUNT(*) FROM tasks child
                       WHERE child.parent_task_id =
                             child_task_counters.parent_task_id
                   ),
                   updated_at=?""",
            (now_text,),
        )
        conn.execute(
            """INSERT INTO child_task_counters
               (parent_task_id, child_count, updated_at)
               SELECT child.parent_task_id, COUNT(*), ?
               FROM tasks child
               JOIN tasks parent ON parent.task_id=child.parent_task_id
               WHERE child.parent_task_id IS NOT NULL
               GROUP BY child.parent_task_id
               ON CONFLICT(parent_task_id) DO UPDATE SET
                   child_count=excluded.child_count,
                   updated_at=excluded.updated_at""",
            (now_text,),
        )
        task_rows = conn.execute(
            "SELECT task_id FROM tasks "
            "WHERE state IN ('claimed','running','cancel_requested')"
        ).fetchall()
        for row in task_rows:
            current_task = self._fetch_task_tx(conn, row["task_id"])
            if current_task is not None:
                self._append_event_tx(
                    conn,
                    row["task_id"],
                    event_type="orphaned",
                    visibility=EventVisibility.INTERNAL,
                    priority=EventPriority.SILENT,
                    content=recovery_reason,
                    execution_id=current_task.execution_id,
                    idempotency_key=(
                        "recovery:orphaned:"
                        + str(
                            current_task.execution_id
                            or current_task.claim_token
                            or current_task.task_id
                        )
                    ),
                    created_at=now_text,
                )
            conn.execute(
                "UPDATE tasks SET state='orphaned', claimed_by=NULL, "
                "claim_token=NULL, lease_expires_at=NULL, updated_at=?, "
                "last_error=COALESCE(last_error,?) WHERE task_id=?",
                (now_text, recovery_reason, row["task_id"]),
            )
            conn.execute(
                "UPDATE task_executions SET state='orphaned', finished_at=?, "
                "lease_expires_at=NULL, last_error=COALESCE(last_error,?) "
                "WHERE task_id=? AND finished_at IS NULL",
                (now_text, recovery_reason, row["task_id"]),
            )

        outbox_rows = conn.execute(
            "SELECT outbox_id, state FROM user_outbox "
            "WHERE state IN ('claimed','sending')"
        ).fetchall()
        outbox_requeued = 0
        outbox_unknown = 0
        for row in outbox_rows:
            recovered_state = (
                "delivery_unknown" if row["state"] == "sending" else "pending"
            )
            conn.execute(
                "UPDATE user_outbox SET state=?, claimed_by=NULL, "
                "claim_token=NULL, lease_expires_at=NULL, next_attempt_at=?, "
                "last_error=COALESCE(last_error,?) WHERE outbox_id=?",
                (recovered_state, now_text, recovery_reason, row["outbox_id"]),
            )
            if recovered_state == "pending":
                outbox_requeued += 1
            else:
                outbox_unknown += 1

        mailbox_rows = conn.execute(
            "SELECT mailbox_id FROM agent_mailbox "
            "WHERE state IN ('claimed','processing')"
        ).fetchall()
        for row in mailbox_rows:
            conn.execute(
                "UPDATE agent_mailbox SET state='pending', claimed_by=NULL, "
                "claim_token=NULL, lease_expires_at=NULL, next_attempt_at=?, "
                "last_error=COALESCE(last_error,?) WHERE mailbox_id=?",
                (now_text, recovery_reason, row["mailbox_id"]),
            )

        media_rows = conn.execute(
            "SELECT media_id, state FROM outgoing_media "
            "WHERE state IN ('uploading','send_pending','uploaded') "
            "AND (claimed_by IS NOT NULL OR claim_token IS NOT NULL)"
        ).fetchall()
        media_requeued = 0
        for row in media_rows:
            recovered_state = (
                "upload_pending" if row["state"] == "uploading" else row["state"]
            )
            changed = conn.execute(
                "UPDATE outgoing_media SET state=?, claimed_by=NULL, "
                "claim_token=NULL, lease_expires_at=NULL, next_attempt_at=?, "
                "updated_at=?, last_error=COALESCE(last_error, ?) "
                "WHERE media_id=?",
                (
                    recovered_state,
                    now_text,
                    now_text,
                    recovery_reason
                    + "; external media operation is retryable by stable identity",
                    row["media_id"],
                ),
            ).rowcount
            media_requeued += int(changed == 1)

        missing = 0
        attachment_rows = conn.execute(
            "SELECT attachment_id, path, checksum, size_bytes, state "
            "FROM attachments WHERE state <> 'blocked_media'"
        ).fetchall()
        for row in attachment_rows:
            raw_path = row["path"]
            invalid = not bool(raw_path)
            path: Path | None = None
            if raw_path:
                path = Path(str(raw_path))
                invalid = not path.is_file() or path.is_symlink()
                if not invalid:
                    resolved = path.resolve(strict=False)
                    if self.attachment_root is None:
                        invalid = True
                    else:
                        try:
                            resolved.relative_to(self.attachment_root)
                        except ValueError:
                            invalid = True
                        else:
                            invalid = resolved != path
            if not invalid and path is not None:
                try:
                    invalid = path.stat().st_size != int(row["size_bytes"] or 0)
                except OSError:
                    invalid = True
            if not invalid and path is not None and row["checksum"]:
                try:
                    invalid = (
                        hashlib.sha256(path.read_bytes()).hexdigest()
                        != row["checksum"]
                    )
                except OSError:
                    invalid = True
            if invalid:
                conn.execute(
                    "UPDATE attachments SET state='blocked_media' "
                    "WHERE attachment_id=?",
                    (row["attachment_id"],),
                )
                missing += 1
            elif str(row["state"] or "ready") == "deleting":
                conn.execute(
                    "UPDATE attachments SET state='ready' "
                    "WHERE attachment_id=? AND state='deleting'",
                    (row["attachment_id"],),
                )

        return RecoveryReport(
            tasks_orphaned=len(task_rows),
            outbox_requeued=outbox_requeued,
            outbox_unknown=outbox_unknown,
            mailbox_requeued=len(mailbox_rows),
            missing_attachments=missing,
            media_requeued=media_requeued,
        )

    def _seed_defaults(self, conn: sqlite3.Connection) -> None:
        now = _utc_text()
        # Keep the durable MVP definitions in lockstep with the public
        # registry.  Older databases contain deliberately sparse placeholders;
        # _ensure_* recognizes those rows and upgrades only that known seed,
        # while any real immutable version conflict remains fail-closed.
        from .modes import (
            builtin_modes,
            collaborative_builtin_modes,
            legacy_builtin_modes,
        )
        from .registry import codex_profile

        profile = codex_profile()
        modes = [
            *legacy_builtin_modes().values(),
            *builtin_modes().values(),
            *collaborative_builtin_modes().values(),
        ]
        self._ensure_profile_tx(
            conn,
            agent_id=profile.agent_id,
            profile_version=int(profile.profile_version),
            snapshot=profile,
            now=now,
            allow_legacy_seed_upgrade=True,
        )
        for mode in modes:
            self._ensure_mode_tx(
                conn,
                agent_id=profile.agent_id,
                mode_id=mode.mode_id,
                policy_version=int(mode.policy_version),
                snapshot=mode,
                now=now,
                allow_legacy_seed_upgrade=True,
            )

    def _seed_dynamic_mode_v2(self, conn: sqlite3.Connection) -> None:
        """Seed current Modes for proven ``/agent`` aliases."""

        from .modes import builtin_modes, collaborative_builtin_modes
        from .registry import DYNAMIC_AGENT_SUMMARY

        now = _utc_text()
        agent_ids = tuple(
            str(row["agent_id"])
            for row in conn.execute(
                "SELECT DISTINCT agent_id FROM agent_profiles "
                "WHERE summary=? ORDER BY agent_id",
                (DYNAMIC_AGENT_SUMMARY,),
            ).fetchall()
        )
        for agent_id in agent_ids:
            for mode in (
                *builtin_modes().values(),
                *collaborative_builtin_modes().values(),
            ):
                self._ensure_mode_tx(
                    conn,
                    agent_id=agent_id,
                    mode_id=mode.mode_id,
                    policy_version=int(mode.policy_version),
                    snapshot=mode,
                    now=now,
                )

    async def _call(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        """Execute ``fn`` on the dedicated SQLite thread.

        Cancellation waits for the underlying operation before releasing the
        store lock, preventing a canceled caller from leaving a transaction in
        flight while the next operation starts.
        """

        async with self._lock:
            if self._closed:
                raise StoreError("store is closed")
            if not self._initialized:
                loop = asyncio.get_running_loop()
                open_future = loop.run_in_executor(self._executor, self._open_sync)
                try:
                    _result, cancellation = await self._drain_executor_future(
                        open_future
                    )
                finally:
                    self._initialized = self._conn is not None
                if cancellation is not None:
                    raise cancellation
            conn = self._conn
            if conn is None:
                raise StoreError("store connection is unavailable")
            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(self._executor, functools.partial(fn, conn))
            result, cancellation = await self._drain_executor_future(future)
            if cancellation is not None:
                raise cancellation
            return result

    def _now(self, value: datetime | str | None = None) -> str:
        if value is not None:
            return _utc_text(value)
        now = self.clock()
        return _utc_text(now)

    @staticmethod
    def _reply_source_identity_from_row(row: sqlite3.Row) -> str:
        """Return the immutable source component for one stored inbound."""

        external_message_id = str(row["external_message_id"] or "").strip()
        if external_message_id:
            return external_message_id
        source_sequence = row["source_sequence"]
        if source_sequence not in (None, "", 0, "0"):
            return compound_id("source-sequence", (source_sequence,))
        return str(row["message_id"])

    @classmethod
    def _reply_scope_identity_from_row(cls, row: sqlite3.Row) -> tuple[str, str]:
        source_identity = cls._reply_source_identity_from_row(row)
        reply_scope_id = compound_id(
            "reply-scope",
            (
                row["channel"],
                row["bot_id"],
                row["external_user_id"],
                row["session_id"] or "default",
                source_identity,
            ),
        )
        return reply_scope_id, source_identity

    @classmethod
    def _ensure_reply_scope_for_inbound_row_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> sqlite3.Row:
        """Insert/reuse the sole reply scope for a persisted inbound row."""

        reply_scope_id, source_identity = cls._reply_scope_identity_from_row(row)
        conn.execute(
            """INSERT OR IGNORE INTO reply_scopes
               (reply_scope_id, inbound_message_id, channel, bot_id,
                external_user_id, session_id, source_identity, capacity,
                used_slots, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 10, 0, ?)""",
            (
                reply_scope_id,
                row["message_id"],
                row["channel"],
                row["bot_id"],
                row["external_user_id"],
                row["session_id"] or "default",
                source_identity,
                row["stored_at"],
            ),
        )
        stored = conn.execute(
            "SELECT * FROM reply_scopes WHERE inbound_message_id=?",
            (row["message_id"],),
        ).fetchone()
        if stored is None:
            raise StoreError("failed to persist inbound reply scope")
        expected = {
            "reply_scope_id": reply_scope_id,
            "channel": str(row["channel"] or ""),
            "bot_id": str(row["bot_id"] or ""),
            "external_user_id": str(row["external_user_id"] or ""),
            "session_id": str(row["session_id"] or "default"),
            "source_identity": source_identity,
            "capacity": REPLY_SCOPE_CAPACITY,
        }
        for column, value in expected.items():
            actual = stored[column]
            equal = (
                int(actual) == int(value)
                if column == "capacity"
                else str(actual or "") == str(value or "")
            )
            if not equal:
                raise StoreError(
                    "inbound reply scope conflicts with its immutable envelope"
                    f" ({column})"
                )
        return stored

    @classmethod
    def _reply_scope_for_target_tx(
        cls,
        conn: sqlite3.Connection,
        target: ReplyTarget,
    ) -> sqlite3.Row | None:
        """Resolve a target only against its exact persisted inbound owner."""

        params = (
            str(target.channel),
            str(target.bot_id),
            str(target.external_user_id),
            str(target.session_id or "default"),
        )
        inbound = None
        if target.source_message_id:
            inbound = conn.execute(
                """SELECT * FROM inbound_messages
                   WHERE channel=? AND bot_id=? AND external_user_id=?
                     AND session_id=?
                     AND (external_message_id=? OR message_id=?)
                   ORDER BY CASE WHEN external_message_id=? THEN 0 ELSE 1 END
                   LIMIT 1""",
                (
                    *params,
                    str(target.source_message_id),
                    str(target.source_message_id),
                    str(target.source_message_id),
                ),
            ).fetchone()
        elif target.source_sequence not in (None, 0):
            inbound = conn.execute(
                """SELECT * FROM inbound_messages
                   WHERE channel=? AND bot_id=? AND external_user_id=?
                     AND session_id=? AND source_sequence=?
                   ORDER BY stored_at, message_id LIMIT 1""",
                (*params, int(target.source_sequence)),
            ).fetchone()
        if inbound is None:
            return None
        return cls._ensure_reply_scope_for_inbound_row_tx(conn, inbound)

    @staticmethod
    def _reply_target_for_scope_tx(
        conn: sqlite3.Connection,
        scope: sqlite3.Row,
    ) -> ReplyTarget:
        inbound = conn.execute(
            """SELECT external_message_id, source_sequence, context_token
               FROM inbound_messages WHERE message_id=?""",
            (scope["inbound_message_id"],),
        ).fetchone()
        if inbound is None:
            raise StoreError("reply scope has no stored inbound envelope")
        return ReplyTarget(
            channel=str(scope["channel"]),
            bot_id=str(scope["bot_id"]),
            external_user_id=str(scope["external_user_id"]),
            session_id=str(scope["session_id"] or "default"),
            source_message_id=str(inbound["external_message_id"] or "") or None,
            source_sequence=(
                int(inbound["source_sequence"])
                if inbound["source_sequence"] is not None
                else None
            ),
            context_token=inbound["context_token"],
        )

    @staticmethod
    def _reply_scope_from_row(row: sqlite3.Row | None) -> ReplyScopeRecord | None:
        if row is None:
            return None
        return ReplyScopeRecord(
            reply_scope_id=str(row["reply_scope_id"]),
            inbound_message_id=str(row["inbound_message_id"]),
            channel=str(row["channel"]),
            bot_id=str(row["bot_id"]),
            external_user_id=str(row["external_user_id"]),
            session_id=str(row["session_id"] or "default"),
            source_identity=str(row["source_identity"]),
            capacity=int(row["capacity"]),
            used_slots=int(row["used_slots"]),
            created_at=text_to_datetime(row["created_at"]),
        )

    @staticmethod
    def _reply_candidate_from_row(
        row: sqlite3.Row | None,
    ) -> ReplyCandidateRecord | None:
        if row is None:
            return None
        return ReplyCandidateRecord(
            reply_candidate_id=str(row["reply_candidate_id"]),
            origin_reply_scope_id=str(row["origin_reply_scope_id"]),
            source_key=str(row["source_key"]),
            channel=str(row["channel"]),
            bot_id=str(row["bot_id"]),
            external_user_id=str(row["external_user_id"]),
            session_id=str(row["session_id"] or "default"),
            agent_id=str(row["agent_id"] or "codex"),
            task_id=row["task_id"],
            execution_id=row["execution_id"],
            event_id=row["event_id"],
            source_item_id=row["source_item_id"],
            source_item_type=row["source_item_type"],
            source_item_ordinal=(
                int(row["source_item_ordinal"])
                if row["source_item_ordinal"] is not None
                else None
            ),
            content=str(row["content"] or ""),
            attachments=tuple(json_loads(row["attachments_json"], []) or []),
            priority=EventPriority(int(row["priority"])),
            delivery_mode=DeliveryMode(str(row["delivery_mode"])),
            notify_enabled=bool(row["notify_enabled"]),
            foreground=bool(row["foreground"]),
            created_at=text_to_datetime(row["created_at"]),
        )

    @staticmethod
    def _reply_fragment_from_row(
        row: sqlite3.Row | None,
    ) -> ReplyFragmentRecord | None:
        if row is None:
            return None
        return ReplyFragmentRecord(
            reply_fragment_id=str(row["reply_fragment_id"]),
            reply_candidate_id=str(row["reply_candidate_id"]),
            origin_reply_scope_id=str(row["origin_reply_scope_id"]),
            fragment_ordinal=int(row["fragment_ordinal"]),
            fragment_kind=str(row["fragment_kind"]),
            content=str(row["content"] or ""),
            attachments=tuple(json_loads(row["attachments_json"], []) or []),
            state=ReplyFragmentState(str(row["state"])),
            delivery_reply_scope_id=row["delivery_reply_scope_id"],
            reply_slot_id=row["reply_slot_id"],
            deferred_sequence=(
                int(row["deferred_sequence"])
                if row["deferred_sequence"] is not None
                else None
            ),
            created_at=text_to_datetime(row["created_at"]),
            allocated_at=text_to_datetime(row["allocated_at"]),
        )

    @staticmethod
    def _reply_slot_from_row(row: sqlite3.Row | None) -> ReplySlotRecord | None:
        if row is None:
            return None
        return ReplySlotRecord(
            reply_slot_id=str(row["reply_slot_id"]),
            reply_scope_id=str(row["reply_scope_id"]),
            reply_ordinal=int(row["reply_ordinal"]),
            reply_fragment_id=str(row["reply_fragment_id"]),
            delivery_id=str(row["delivery_id"]),
            client_id=str(row["client_id"]),
            contextless_client_id=row["contextless_client_id"],
            active_wire_variant=str(row["active_wire_variant"] or "primary"),
            outbox_id=row["outbox_id"],
            payload=json_loads(row["payload_json"], {}) or {},
            created_at=text_to_datetime(row["created_at"]),
        )

    def _default_transcription_expiry(self, created_at: datetime | str) -> str:
        """Return the durable expiry derived from a candidate creation time."""

        created_value = text_to_datetime(_utc_text(created_at))
        if created_value is None:
            raise ValueError("candidate created_at must be an ISO-8601 timestamp")
        return _utc_text(
            created_value + timedelta(seconds=self.transcription_ttl_seconds)
        )

    # ------------------------------------------------------------------
    # Row conversion helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _task_from_row(row: sqlite3.Row | None) -> TaskRecord | None:
        if row is None:
            return None
        return TaskRecord(
            task_id=row["task_id"],
            state=TaskState(row["state"]),
            agent_id=row["agent_id"],
            conversation_id=row["conversation_id"],
            mode_id=row["mode_id"],
            profile_version=int(row["profile_version"]),
            policy_version=int(row["policy_version"]),
            created_at=text_to_datetime(row["created_at"]),
            updated_at=text_to_datetime(row["updated_at"]),
            execution_id=row["execution_id"] if "execution_id" in row.keys() else None,
            inbound_message_id=row["inbound_message_id"],
            dedupe_key=row["dedupe_key"],
            thread_id=row["thread_id"],
            model=row["model"],
            reasoning_effort=row["reasoning_effort"],
            reply_target=ReplyTarget.from_value(json_loads(row["reply_target_json"], {})),
            inputs=json_loads(row["inputs_json"], {}),
            claimed_by=row["claimed_by"],
            claim_token=row["claim_token"],
            lease_expires_at=text_to_datetime(row["lease_expires_at"]),
            attempts=int(row["attempts"]),
            next_attempt_at=text_to_datetime(row["next_attempt_at"]),
            last_error=row["last_error"],
            result=json_loads(row["result_json"], None),
            parent_task_id=row["parent_task_id"],
            child_depth=int(row["child_depth"]),
            request_id=row["request_id"],
            terminal_at=text_to_datetime(row["terminal_at"]),
            metadata=json_loads(row["metadata_json"], {}) or {},
            pending_delivery_reply_scope_id=(
                row["pending_delivery_reply_scope_id"]
                if "pending_delivery_reply_scope_id" in row.keys()
                else None
            ),
        )

    @staticmethod
    def _execution_from_row(row: sqlite3.Row | None) -> TaskExecution | None:
        if row is None:
            return None
        return TaskExecution(
            execution_id=row["execution_id"],
            task_id=row["task_id"],
            attempt=int(row["attempt"]),
            state=ExecutionState(row["state"]),
            worker_id=row["worker_id"],
            claim_token=row["claim_token"],
            lease_expires_at=text_to_datetime(row["lease_expires_at"]),
            started_at=text_to_datetime(row["started_at"]),
            finished_at=text_to_datetime(row["finished_at"]),
            last_error=row["last_error"],
            external_turn_id=row["external_turn_id"],
            delivery_reply_scope_id=(
                row["delivery_reply_scope_id"]
                if "delivery_reply_scope_id" in row.keys()
                else None
            ),
        )

    @staticmethod
    def _task_event_from_row(row: sqlite3.Row) -> TaskEvent:
        return TaskEvent(
            event_id=row["event_id"],
            task_id=row["task_id"],
            sequence=int(row["sequence"]),
            event_type=row["event_type"],
            visibility=EventVisibility(row["visibility"]),
            priority=EventPriority(int(row["priority"])),
            content=row["content"],
            attachments=tuple(json_loads(row["attachments_json"], []) or []),
            created_at=text_to_datetime(row["created_at"]),
            execution_id=row["execution_id"],
            destination_agent_id=row["destination_agent_id"],
            request_id=row["request_id"],
            reply_to_id=row["reply_to_id"],
            causation_id=row["causation_id"],
            source_item_id=(
                row["source_item_id"] if "source_item_id" in row.keys() else None
            ),
            source_item_type=(
                row["source_item_type"]
                if "source_item_type" in row.keys()
                else None
            ),
            source_item_ordinal=(
                int(row["source_item_ordinal"])
                if "source_item_ordinal" in row.keys()
                and row["source_item_ordinal"] is not None
                else None
            ),
        )

    @staticmethod
    def _outbox_from_row(row: sqlite3.Row | None) -> UserOutboxItem | None:
        if row is None:
            return None
        return UserOutboxItem(
            outbox_id=row["outbox_id"],
            task_id=row["task_id"],
            event_id=row["event_id"],
            channel=row["channel"],
            bot_id=row["bot_id"],
            external_user_id=row["external_user_id"],
            session_id=row["session_id"],
            agent_id=row["agent_id"],
            reply_target=ReplyTarget.from_value(json_loads(row["reply_target_json"], {})),
            content=row["content"],
            priority=EventPriority(int(row["priority"])),
            delivery_mode=DeliveryMode(row["delivery_mode"]),
            notify_enabled=bool(row["notify_enabled"]) if "notify_enabled" in row.keys() else True,
            foreground=bool(row["foreground"]) if "foreground" in row.keys() else False,
            state=OutboxState(row["state"]),
            presentation=PresentationState(row["presentation"]),
            client_id=row["client_id"],
            attempts=int(row["attempts"]),
            claim_token=row["claim_token"],
            claimed_by=row["claimed_by"],
            lease_expires_at=text_to_datetime(row["lease_expires_at"]),
            next_attempt_at=text_to_datetime(row["next_attempt_at"]),
            last_error=row["last_error"],
            created_at=text_to_datetime(row["created_at"]),
            sent_at=text_to_datetime(row["sent_at"]),
            attachments=tuple(json_loads(row["attachments_json"], []) or []),
            reply_scope_id=(
                row["reply_scope_id"] if "reply_scope_id" in row.keys() else None
            ),
            reply_slot_id=(
                row["reply_slot_id"] if "reply_slot_id" in row.keys() else None
            ),
            reply_ordinal=(
                int(row["reply_ordinal"])
                if "reply_ordinal" in row.keys() and row["reply_ordinal"] is not None
                else None
            ),
            reply_candidate_id=(
                row["reply_candidate_id"]
                if "reply_candidate_id" in row.keys()
                else None
            ),
            reply_fragment_id=(
                row["reply_fragment_id"]
                if "reply_fragment_id" in row.keys()
                else None
            ),
            from_user_id=(
                str(row["from_user_id"] or "")
                if "from_user_id" in row.keys()
                else str(row["bot_id"] or "")
            ),
            contextless_client_id=(
                row["contextless_client_id"]
                if "contextless_client_id" in row.keys()
                else None
            ),
            active_wire_variant=(
                str(row["active_wire_variant"] or "primary")
                if "active_wire_variant" in row.keys()
                else "primary"
            ),
        )

    @staticmethod
    def _command_receipt_from_row(
        row: sqlite3.Row | None, *, created: bool = False
    ) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "command_id": str(row["command_id"]),
            "channel": str(row["channel"]),
            "bot_id": str(row["bot_id"]),
            "external_user_id": str(row["external_user_id"]),
            "session_id": str(row["session_id"]),
            "external_message_id": str(row["external_message_id"]),
            "command_name": str(row["command_name"]),
            "command_args": tuple(
                str(value)
                for value in (json_loads(row["command_args_json"], []) or [])
            ),
            "command_text": str(row["command_text"] or ""),
            "state": str(row["state"]),
            "response_text": str(row["response_text"] or ""),
            "response_agent_id": str(row["response_agent_id"] or ""),
            "presentation_ids": tuple(
                str(value)
                for value in (
                    json_loads(row["presentation_ids_json"], []) or []
                )
            ),
            "created": bool(created),
        }

    @staticmethod
    def _inbound_from_row(row: sqlite3.Row) -> InboundMessage:
        return InboundMessage(
            channel=row["channel"],
            bot_id=row["bot_id"],
            external_user_id=row["external_user_id"],
            external_message_id=row["external_message_id"],
            text=row["text"],
            session_id=row["session_id"],
            source_sequence=row["source_sequence"],
            context_token=row["context_token"],
            payload=json_loads(row["payload_json"], {}) or {},
            received_at=text_to_datetime(row["received_at"]),
            message_id=row["message_id"],
            status=InboundState(row["status"]),
            task_id=row["task_id"],
        )

    @classmethod
    def _coerce_inbound(
        cls,
        value: InboundMessage | Mapping[str, Any] | None,
        kwargs: Mapping[str, Any],
    ) -> InboundMessage:
        data: dict[str, Any] = {}
        # ``message_id`` and ``received_at`` are generated by the domain
        # model when a channel omits them.  Remember whether the caller
        # actually asserted either value so an idempotent replay does not
        # conflict merely because coercion allocated a fresh UUID/timestamp.
        message_id_supplied = False
        received_at_supplied = False
        if isinstance(value, InboundMessage):
            # Preserve the immutable domain ID when a channel does not expose
            # a stable external ID, but still run through fallback-key
            # derivation below.  Returning early would collapse every ID-less
            # message into the same empty unique key.
            if value.external_message_id:
                if not kwargs:
                    object.__setattr__(
                        value,
                        "_message_id_supplied",
                        bool(getattr(value, "_message_id_supplied", False)),
                    )
                    object.__setattr__(
                        value,
                        "_received_at_supplied",
                        bool(getattr(value, "_received_at_supplied", False)),
                    )
                    return value
            data.update(
                {
                    name: getattr(value, name)
                    for name in InboundMessage.__dataclass_fields__
                    if name != "task_id"
                }
            )
            message_id_supplied = bool(
                getattr(value, "_message_id_supplied", False)
            )
            received_at_supplied = bool(
                getattr(value, "_received_at_supplied", False)
            )
        elif isinstance(value, Mapping):
            data.update(value)
            # ``message_id`` is also a compatibility alias for an external
            # message ID.  It names the internal immutable envelope only when
            # the mapping separately supplies ``external_message_id``.
            message_id_supplied = bool(
                value.get("external_message_id") and value.get("message_id")
            )
            received_at_supplied = (
                "received_at" in value and value.get("received_at") is not None
            )
        elif value is not None:
            # Channel adapters intentionally use their own immutable envelope
            # type.  Accept matching attributes (and ``to_dict``/dataclass
            # values) at this boundary without importing channel modules into
            # the domain store.
            # Prefer an adapter's store-specific representation.  Generic
            # ``to_dict`` values can contain routing fields such as the
            # currently selected Agent; those are not part of the immutable
            # channel envelope and may legitimately differ on redelivery.
            if hasattr(value, "as_store_dict"):
                try:
                    converted = value.as_store_dict()
                    if isinstance(converted, Mapping):
                        data.update(converted)
                except TypeError:
                    pass
            if not data and hasattr(value, "to_dict"):
                try:
                    converted = value.to_dict()
                    if isinstance(converted, Mapping):
                        data.update(converted)
                except TypeError:
                    pass
            if not data and hasattr(value, "as_dict"):
                converted = value.as_dict()
                if isinstance(converted, Mapping):
                    data.update(converted)
            if not data:
                for name in (
                    "channel", "bot_id", "external_user_id", "external_message_id",
                    "text", "session_id", "source_sequence", "context_token",
                    "received_at", "payload", "message_id",
                ):
                    if hasattr(value, name):
                        data[name] = getattr(value, name)
        supplied_kwargs = {
            key: val for key, val in kwargs.items() if val is not None
        }
        if supplied_kwargs.get("message_id") and (
            supplied_kwargs.get("external_message_id")
            or data.get("external_message_id")
        ):
            message_id_supplied = True
        if "received_at" in supplied_kwargs:
            received_at_supplied = True
        data.update(supplied_kwargs)
        # A few channel adapters call the text field ``content`` or ``body``.
        if "text" not in data:
            data["text"] = data.pop("content", data.pop("body", ""))
        if "external_message_id" not in data:
            data["external_message_id"] = data.pop("message_id", "")
        if not data.get("external_message_id"):
            # Stable fallback for channels without a message id.  Persisting
            # this value makes retries deduplicate instead of creating work.
            # Some gateways do not expose a stable message id and may omit
            # both sequence and text for media-only updates.  Include the
            # canonical envelope payload so distinct media updates from the
            # same sender do not collapse into one row.  ``context_token`` is
            # deliberately excluded: it is a rolling transport hint and may
            # change when the channel redelivers the *same* message after a
            # reconnect.  Using it here would turn a harmless replay into a
            # second logical task.
            canonical = json_dumps(
                {
                    key: data.get(key)
                    for key in (
                        "channel",
                        "bot_id",
                        "external_user_id",
                        "session_id",
                        "source_sequence",
                        "text",
                    )
                }
                | {
                    "payload": cls._inbound_payload_snapshot(
                        data.get("payload") or {}
                    )
                }
            )
            data["external_message_id"] = "fallback-" + hashlib.sha256(canonical.encode()).hexdigest()
        data.setdefault("channel", "")
        data.setdefault("bot_id", "")
        data.setdefault("external_user_id", "")
        data.setdefault("session_id", "default")
        if not data.get("session_id"):
            data["session_id"] = "default"
        data.setdefault("payload", {})
        allowed = set(InboundMessage.__dataclass_fields__)
        # Preserve channel-specific diagnostics under payload rather than
        # allowing unknown envelope fields to break durable acceptance.
        extras = {key: data[key] for key in tuple(data) if key not in allowed}
        if extras:
            payload = dict(data.get("payload") or {})
            payload.setdefault("channel_metadata", extras)
            data["payload"] = payload
        result = InboundMessage(
            **{key: data[key] for key in allowed if key in data}
        )
        object.__setattr__(result, "_message_id_supplied", message_id_supplied)
        object.__setattr__(result, "_received_at_supplied", received_at_supplied)
        return result

    @classmethod
    def _validate_inbound_replay_tx(
        cls,
        row: sqlite3.Row,
        message: InboundMessage,
        *,
        trusted_media_wire_fingerprints: Sequence[str] | None = None,
    ) -> None:
        """Fail closed when a dedupe identity is reused for new content.

        The channel identity is the lookup key, not permission to rewrite the
        stored envelope.  ``context_token`` is intentionally a transport hint
        and is therefore ignored on replay; the first committed value remains
        on the durable reply target.  UUIDs and receive timestamps are the two
        other exceptions:
        channel adapters commonly omit them and coercion then generates fresh
        values on every replay, so they are compared only when the caller
        explicitly supplied them.
        """

        expected: dict[str, Any] = {
            "channel": message.channel,
            "bot_id": message.bot_id,
            "external_user_id": message.external_user_id,
            "external_message_id": message.external_message_id,
            "session_id": message.session_id or "default",
            "source_sequence": message.source_sequence,
            "text": message.text,
        }
        if bool(getattr(message, "_message_id_supplied", False)):
            expected["message_id"] = message.message_id
        if bool(getattr(message, "_received_at_supplied", False)):
            expected["received_at"] = _utc_text(message.received_at)

        for column, incoming in expected.items():
            stored = row[column]
            if column == "session_id":
                stored = stored or "default"
            equal = (stored is None and incoming is None) or str(stored) == str(
                incoming
            )
            if not equal:
                raise StoreError(
                    "inbound identity conflicts with immutable envelope "
                    f"({column})"
                )

        # The stored row is authoritative and may carry the adapter's
        # hash-only fingerprint after media was promoted.  A replay payload is
        # untrusted: any fingerprint it supplies must be recomputed from its
        # wire reference rather than accepted as an assertion.
        stored_payload = cls._inbound_payload_snapshot(
            json_loads(row["payload_json"], {}) or {},
            trust_media_fingerprints=True,
        )
        incoming_payload = cls._inbound_payload_snapshot(
            message.payload or {},
            trusted_media_wire_fingerprints=trusted_media_wire_fingerprints,
        )
        if stored_payload != incoming_payload:
            raise StoreError(
                "inbound identity conflicts with immutable envelope (payload)"
            )

    @classmethod
    def _inbound_payload_snapshot(
        cls,
        payload: Any,
        *,
        trust_media_fingerprints: bool = False,
        trusted_media_wire_fingerprints: Sequence[str] | None = None,
    ) -> Any:
        """Canonicalize diagnostic payloads for immutable replay checks.

        Channel adapters may include a rolling transport context token in
        either the payload itself or a nested ``channel_metadata`` object.
        That hint is deliberately excluded from the replay identity.  Other
        payload fields, including media references and protocol sequence data,
        remain strict immutable envelope content.
        """

        snapshot = cls._json_snapshot(
            cls._sanitize_inbound_payload(
                payload,
                trust_media_fingerprints=trust_media_fingerprints,
                trusted_media_wire_fingerprints=trusted_media_wire_fingerprints,
            )
        )
        if not isinstance(snapshot, Mapping):
            return snapshot
        snapshot = dict(snapshot)
        snapshot.pop("context_token", None)
        snapshot.pop("contextToken", None)
        # A command's resolved route is an immutable replay hint persisted by
        # the runtime, not part of the channel envelope identity.  Redelivery
        # may carry a newly resolved route; never reject it as changed wire
        # content, and never let the incoming value overwrite the stored one.
        snapshot.pop("__command_snapshot", None)
        snapshot.pop("__route_snapshot", None)
        if "media" in snapshot:
            snapshot["media"] = list(
                canonical_media_inputs(snapshot.get("media"), include_candidate=True)
            )
        if "attachments" in snapshot:
            snapshot["attachments"] = list(
                canonical_media_inputs(
                    snapshot.get("attachments"), include_candidate=True
                )
            )
        if "images" in snapshot:
            snapshot["images"] = list(
                canonical_media_inputs(
                    snapshot.get("images"), include_candidate=True
                )
            )
        metadata = snapshot.get("channel_metadata")
        if isinstance(metadata, Mapping):
            metadata = dict(metadata)
            metadata.pop("context_token", None)
            metadata.pop("contextToken", None)
            snapshot["channel_metadata"] = metadata
        return snapshot

    @classmethod
    def _sanitize_inbound_payload(
        cls,
        payload: Any,
        *,
        trust_media_fingerprints: bool = False,
        trusted_media_wire_fingerprints: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Drop untrusted control snapshots and redact wire media secrets."""

        sanitized = dict(payload) if isinstance(payload, Mapping) else {}
        supplied_wire_fingerprints = sanitized.pop(
            "__media_wire_fingerprints", None
        )
        if trusted_media_wire_fingerprints is not None:
            try:
                trusted_values = list(trusted_media_wire_fingerprints)
            except TypeError as exc:
                raise ValueError("invalid trusted media fingerprints") from exc
            for fingerprint in trusted_values:
                value = str(fingerprint or "").lower()
                if len(value) != 64:
                    raise ValueError("invalid trusted media fingerprint")
                try:
                    int(value, 16)
                except ValueError as exc:
                    raise ValueError("invalid trusted media fingerprint") from exc
            trusted_values = [str(value).lower() for value in trusted_values]
        else:
            trusted_values = None
        sanitized.pop("__command_snapshot", None)
        sanitized.pop("__route_snapshot", None)
        # WeChat's raw model dump duplicates every media object under
        # ``item_list``, including encrypted CDN query parameters and AES
        # keys.  The channel adapter normally removes it after extracting the
        # canonical ``media`` projection; enforce the same boundary here for
        # direct store callers and older adapters.
        sanitized.pop("item_list", None)
        sanitized.pop("itemList", None)
        for key in ("media", "attachments", "images"):
            if key in sanitized:
                raw_values = sanitized.get(key)
                if key == "media":
                    if isinstance(raw_values, Mapping) or isinstance(
                        raw_values, (str, bytes, bytearray)
                    ):
                        raw_values = (raw_values,)
                    try:
                        raw_values = tuple(raw_values or ())
                    except TypeError:
                        raw_values = (raw_values,)

                    # A promoted envelope contains only managed metadata and
                    # may legitimately carry the fingerprint computed by the
                    # trusted channel adapter.  Raw wire media, however, must
                    # always be fingerprinted afresh; otherwise a caller can
                    # pair a changed CDN reference with a copied digest.
                    wire_fields = {
                        "remote_id",
                        "media_id",
                        "id",
                        "encrypted_query_param",
                        "encrypt_query_param",
                        "download_param",
                        "encryption_key",
                        "aes_key",
                        "aes_key_base64",
                        "upload_param",
                    }
                    has_wire_reference = any(
                        not isinstance(value, Mapping)
                        or bool(wire_fields.intersection(value.keys()))
                        for value in raw_values
                    )
                    supplied_list = None
                    if trusted_values is not None:
                        if len(trusted_values) != len(raw_values):
                            raise ValueError(
                                "trusted media fingerprint count conflicts with media"
                            )
                        supplied_list = trusted_values
                    elif trust_media_fingerprints:
                        if isinstance(supplied_wire_fingerprints, (list, tuple)):
                            supplied_list = [
                                str(value).lower()
                                for value in supplied_wire_fingerprints
                            ]
                            if len(supplied_list) != len(raw_values):
                                raise ValueError(
                                    "stored media fingerprint count conflicts with media"
                                )
                        if has_wire_reference:
                            supplied_list = None
                    if supplied_list is None:
                        fingerprints: list[str] = []
                        for value in raw_values:
                            # Keep only a deterministic non-secret digest in
                            # SQLite.  Raw CDN query/key material never crosses
                            # the durable envelope boundary.
                            fingerprints.append(wire_media_fingerprint(value))
                        if fingerprints:
                            sanitized["__media_wire_fingerprints"] = fingerprints
                    else:
                        sanitized["__media_wire_fingerprints"] = supplied_list
                sanitized[key] = list(
                    canonical_media_inputs(
                        raw_values, include_candidate=True
                    )
                )
        return sanitized

    @staticmethod
    def _coerce_reply_target(value: Any, *, fallback: ReplyTarget | None = None) -> ReplyTarget:
        if value is not None and not isinstance(value, (ReplyTarget, Mapping)):
            if hasattr(value, "to_dict"):
                try:
                    value = value.to_dict()
                except TypeError:
                    pass
            elif hasattr(value, "as_dict"):
                value = value.as_dict()
            elif all(hasattr(value, name) for name in ("channel", "bot_id", "external_user_id")):
                value = {
                    name: getattr(value, name, None)
                    for name in ReplyTarget.__dataclass_fields__
                }
        target = ReplyTarget.from_value(value)
        if fallback is not None:
            empty_scope = not any(
                (target.channel, target.bot_id, target.external_user_id)
            )
            target = ReplyTarget(
                channel=target.channel or fallback.channel,
                bot_id=target.bot_id or fallback.bot_id,
                external_user_id=target.external_user_id or fallback.external_user_id,
                session_id=(
                    fallback.session_id
                    if empty_scope and target.session_id in ("", "default")
                    else target.session_id or fallback.session_id or "default"
                ),
                source_message_id=target.source_message_id or fallback.source_message_id,
                source_sequence=(
                    target.source_sequence
                    if target.source_sequence is not None
                    else fallback.source_sequence
                ),
                context_token=target.context_token or fallback.context_token,
            )
        return target

    @staticmethod
    def _validate_reply_target_scope(
        target: ReplyTarget,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> None:
        expected = {
            "channel": channel,
            "bot_id": bot_id,
            "external_user_id": external_user_id,
            "session_id": session_id or "default",
        }
        target_has_scope = bool(
            target.channel or target.bot_id or target.external_user_id
        )
        for field, owner in expected.items():
            supplied = getattr(target, field)
            # ReplyTarget() uses ``default`` as its dataclass default.  When
            # all other scope fields are empty that value means "unspecified",
            # not an attempted override of a non-default inbound session.
            if field == "session_id" and supplied == "default" and not target_has_scope:
                continue
            if supplied and owner and str(supplied) != str(owner):
                raise StoreError(f"reply target conflicts with inbound ownership ({field})")

    @staticmethod
    def _coerce_task(value: AgentTask | Mapping[str, Any] | None, kwargs: Mapping[str, Any]) -> AgentTask:
        if isinstance(value, AgentTask):
            if not kwargs:
                return value
            data: dict[str, Any] = {
                name: getattr(value, name) for name in value.__dataclass_fields__
            }
        elif value is not None and not isinstance(value, Mapping):
            data = {}
            for converter in ("as_dict", "to_dict"):
                method = getattr(value, converter, None)
                if method is not None:
                    try:
                        converted = method()
                    except TypeError:
                        converted = None
                    if isinstance(converted, Mapping):
                        data.update(converted)
                        break
            if not data:
                for name in (
                    "task_id", "execution_id", "agent_id", "conversation_id", "thread_id",
                    "mode_id", "profile_version", "policy_version", "model", "reasoning_effort",
                    "reply_target", "inputs", "request_id", "inbound_message_id", "dedupe_key",
                    "parent_task_id", "child_depth", "metadata",
                ):
                    if hasattr(value, name):
                        data[name] = getattr(value, name)
        else:
            data = dict(value or {})
        data.update({key: val for key, val in kwargs.items() if val is not None})
        # ``input`` and ``payload`` are common aliases for the immutable task
        # input snapshot.
        if "inputs" not in data:
            data["inputs"] = data.pop("input", data.pop("payload", {}))
        data.setdefault("task_id", "")
        data.setdefault("agent_id", "codex")
        data.setdefault("mode_id", "chat")
        data.setdefault("profile_version", 1)
        data.setdefault("policy_version", 1)
        data.setdefault("model", "")
        data.setdefault("reasoning_effort", "")
        data.setdefault("reply_target", ReplyTarget())
        data.setdefault("inputs", {})
        data.setdefault("conversation_id", "")
        data.setdefault("thread_id", None)
        data.setdefault("execution_id", "")
        data.setdefault("request_id", None)
        data.setdefault("inbound_message_id", None)
        data.setdefault("dedupe_key", None)
        data.setdefault("parent_task_id", None)
        data.setdefault("child_depth", 0)
        data.setdefault("metadata", {})
        # Channel/runtime adapters may provide their own immutable
        # ReplyTarget dataclass.  Normalize by attributes/as_dict before the
        # domain model conversion instead of silently dropping the snapshot.
        data["reply_target"] = SQLiteStore._coerce_reply_target(data["reply_target"])
        # Ignore extra fields from channel-specific dictionaries while keeping
        # the core snapshot strict and predictable.
        allowed = set(AgentTask.__dataclass_fields__)
        return AgentTask(**{key: data[key] for key in allowed})

    @staticmethod
    def _task_select_sql() -> str:
        # Include the latest execution id in the row model without making the
        # task table mutable whenever a new attempt is created.
        return """SELECT t.*, (
                    SELECT e.execution_id FROM task_executions e
                    WHERE e.task_id = t.task_id ORDER BY e.attempt DESC LIMIT 1
                ) AS execution_id
                FROM tasks t"""

    @classmethod
    def _fetch_task_tx(cls, conn: sqlite3.Connection, task_id: str) -> TaskRecord | None:
        row = conn.execute(f"{cls._task_select_sql()} WHERE t.task_id = ?", (task_id,)).fetchone()
        return cls._task_from_row(row)

    @classmethod
    def _fetch_task_by_dedupe_tx(cls, conn: sqlite3.Connection, dedupe_key: str | None) -> TaskRecord | None:
        if not dedupe_key:
            return None
        row = conn.execute(f"{cls._task_select_sql()} WHERE t.dedupe_key = ?", (dedupe_key,)).fetchone()
        return cls._task_from_row(row)

    @classmethod
    def _confirmation_snapshot_tx(
        cls,
        conn: sqlite3.Connection,
        candidate: sqlite3.Row,
        confirmation_id: str,
    ) -> dict[str, Any]:
        """Build the immutable task projection owned by one candidate.

        A transcription candidate is created from an inbound envelope, while
        the later ``/confirm`` command is a new message and may carry a
        completely different (or absent) input payload.  Reconstructing the
        snapshot here keeps confirmation idempotent across restarts and
        prevents a caller from replacing the original caption/media with
        command-time data.
        """

        route = {
            "channel": str(candidate["channel"] or ""),
            "bot_id": str(candidate["bot_id"] or ""),
            "external_user_id": str(candidate["external_user_id"] or ""),
            "session_id": str(candidate["session_id"] or "default") or "default",
        }
        inbound_id = (
            str(candidate["inbound_message_id"] or "")
            if "inbound_message_id" in candidate.keys()
            else ""
        )
        inbound = None
        if inbound_id:
            inbound = conn.execute(
                "SELECT external_message_id, source_sequence, context_token, text, payload_json, task_id "
                "FROM inbound_messages WHERE message_id=?",
                (inbound_id,),
            ).fetchone()

        original_target = ReplyTarget(
            **route,
            source_message_id=(
                inbound["external_message_id"] if inbound is not None else None
            ),
            source_sequence=(
                inbound["source_sequence"] if inbound is not None else None
            ),
            context_token=(
                inbound["context_token"] if inbound is not None else None
            ),
        )

        # Keep only normalized media references.  The inbound payload also
        # contains wire diagnostics and sender metadata which must not become
        # executable Agent input.
        inputs: dict[str, Any] = {
            "text": str(candidate["candidate_text"] or ""),
            "audio_confirmation_id": confirmation_id,
            "attachment_id": candidate["attachment_id"],
            "source": str(candidate["source"] or ""),
        }
        if inbound is not None:
            caption = str(inbound["text"] or "")
            if caption:
                inputs["caption"] = caption
            payload = json_loads(inbound["payload_json"], {}) or {}
            if isinstance(payload, Mapping):
                media = payload.get("media")
                if media:
                    inputs["media"] = list(
                        canonical_media_inputs(media, include_candidate=True)
                    )

        metadata = json_loads(
            candidate["metadata_json"] if "metadata_json" in candidate.keys() else None,
            {},
        ) or {}
        if not isinstance(metadata, Mapping):
            metadata = {}

        agent_id = str(candidate["agent_id"] or "codex")
        mode_id = str(candidate["mode_id"] or "chat")
        profile_version = int(
            candidate["profile_version"]
            if candidate["profile_version"] is not None
            else 1
        )
        policy_version = int(
            candidate["policy_version"]
            if candidate["policy_version"] is not None
            else 1
        )
        return {
            "route": route,
            "inbound_id": inbound_id or None,
            "inbound_task_id": (
                str(inbound["task_id"] or "") if inbound is not None else ""
            ),
            # A candidate can be one of several audio items on the same
            # inbound envelope.  Only the first confirmed item may claim the
            # envelope's one-task FK; later candidates intentionally leave it
            # NULL.
            "expected_inbound_id": (
                inbound_id
                if inbound is not None and not str(inbound["task_id"] or "")
                else None
            ),
            "target": original_target,
            "agent_id": agent_id,
            "mode_id": mode_id,
            "profile_version": profile_version,
            "policy_version": policy_version,
            "metadata": cls._json_snapshot(dict(metadata)),
            "inputs": cls._json_snapshot(inputs),
            "conversation_id": canonical_conversation_id(
                route["channel"],
                route["bot_id"],
                route["external_user_id"],
                route["session_id"],
                agent_id,
            ),
        }

    @classmethod
    def _validate_confirmation_task_tx(
        cls,
        conn: sqlite3.Connection,
        existing: TaskRecord,
        snapshot: Mapping[str, Any],
        confirmation_id: str,
        *,
        consumed: bool = False,
    ) -> None:
        """Fail closed when a confirmation dedupe winner is not its task.

        ``confirmation:<id>`` is an exactly-once identity, not a capability
        to retrieve an arbitrary task.  Every durable field that the
        candidate owns is compared before the candidate can be consumed.
        """

        route = snapshot["route"]
        raw = conn.execute(
            "SELECT channel, bot_id, external_user_id, session_id, inbound_message_id "
            "FROM tasks WHERE task_id=?",
            (existing.task_id,),
        ).fetchone()
        if raw is None:
            raise StoreError("confirmation task disappeared")
        for field in ("channel", "bot_id", "external_user_id", "session_id"):
            expected = str(route.get(field) or ("default" if field == "session_id" else ""))
            actual = str(getattr(existing.reply_target, field, "") or "")
            if actual != expected:
                raise StoreError(
                    f"confirmation task identity conflicts ({field})"
                )
            stored = str(raw[field] or ("default" if field == "session_id" else ""))
            if stored != expected:
                raise StoreError(
                    f"confirmation task identity conflicts ({field})"
                )

        if str(existing.agent_id or "") != str(snapshot["agent_id"]):
            raise StoreError("confirmation task identity conflicts (agent_id)")
        if str(existing.mode_id or "") != str(snapshot["mode_id"]):
            raise StoreError("confirmation task identity conflicts (mode_id)")
        if int(existing.profile_version) != int(snapshot["profile_version"]):
            raise StoreError("confirmation task identity conflicts (profile_version)")
        if int(existing.policy_version) != int(snapshot["policy_version"]):
            raise StoreError("confirmation task identity conflicts (policy_version)")
        if not conversation_id_matches(
            existing.conversation_id,
            route["channel"],
            route["bot_id"],
            route["external_user_id"],
            route["session_id"],
            snapshot["agent_id"],
        ):
            raise StoreError("confirmation task identity conflicts (conversation_id)")
        if str(existing.dedupe_key or "") != f"confirmation:{confirmation_id}":
            raise StoreError("confirmation task identity conflicts (dedupe_key)")

        expected_target = cls._reply_target_snapshot(snapshot["target"].to_dict())
        actual_target = cls._reply_target_snapshot(existing.reply_target.to_dict())
        # ``context_token`` is a rolling transport hint.  It is deliberately
        # excluded from the identity comparison, just as it is for inbound
        # replay validation.
        if isinstance(expected_target, Mapping):
            expected_target = dict(expected_target)
            expected_target.pop("context_token", None)
        if isinstance(actual_target, Mapping):
            actual_target = dict(actual_target)
            actual_target.pop("context_token", None)
        if cls._json_snapshot(actual_target) != cls._json_snapshot(expected_target):
            raise StoreError("confirmation task identity conflicts (reply_target)")

        if cls._json_snapshot(existing.inputs) != cls._json_snapshot(snapshot["inputs"]):
            raise StoreError("confirmation task identity conflicts (inputs)")
        if cls._json_snapshot(existing.metadata) != cls._json_snapshot(snapshot["metadata"]):
            raise StoreError("confirmation task identity conflicts (metadata)")

        # A confirmed task is never a child task.  For a multi-candidate
        # inbound, only the first candidate may claim the inbound FK; a replay
        # may therefore legitimately have either no link or this envelope's
        # link, but never a different envelope.
        if existing.parent_task_id is not None or int(existing.child_depth or 0) != 0:
            raise StoreError("confirmation task identity conflicts (parent)")
        if consumed and snapshot.get("inbound_id"):
            if raw["inbound_message_id"] not in (None, snapshot["inbound_id"]):
                raise StoreError("confirmation task identity conflicts (inbound)")
        elif not consumed and raw["inbound_message_id"] != snapshot.get(
            "expected_inbound_id"
        ):
            raise StoreError("confirmation task identity conflicts (inbound)")

    @staticmethod
    def _validate_dedupe_owner(
        existing: TaskRecord,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
        parent_task_id: str | None,
        conversation_id: str | None = None,
    ) -> None:
        """Reject reuse of a logical dedupe key across ownership scopes.

        A unique key prevents duplicate execution, but by itself it does not
        prevent an untrusted caller from asking for another user's existing
        task.  Mode/policy are intentionally excluded: a replay after a
        control-plane change must return the original immutable task snapshot.
        """

        target = existing.reply_target
        expected = {
            "channel": channel,
            "bot_id": bot_id,
            "external_user_id": external_user_id,
            "session_id": session_id or "default",
        }
        for field, value in expected.items():
            actual = getattr(target, field)
            if str(actual or "") != str(value or ""):
                raise StoreError(f"dedupe key conflicts with task ownership ({field})")
        if str(existing.agent_id or "") != str(agent_id or ""):
            raise StoreError("dedupe key conflicts with task ownership (agent_id)")
        if conversation_id and str(existing.conversation_id) != str(conversation_id):
            if not conversation_id_matches(
                existing.conversation_id,
                channel,
                bot_id,
                external_user_id,
                session_id,
                agent_id,
            ) or not conversation_id_matches(
                conversation_id,
                channel,
                bot_id,
                external_user_id,
                session_id,
                agent_id,
            ):
                raise StoreError("dedupe key conflicts with task conversation")
        # A child request is owned by its exact parent.  Route identity alone
        # is insufficient because two tasks for the same Agent conversation
        # could otherwise reuse one dedupe key and charge the child to the
        # wrong parent's admission counter.
        if str(existing.parent_task_id or "") != str(parent_task_id or ""):
            raise StoreError("dedupe key conflicts with child-task parent")

    @classmethod
    def _validate_child_parent_tx(
        cls,
        conn: sqlite3.Connection,
        task: AgentTask,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> str | None:
        """Validate a child snapshot against its durable parent.

        ``tasks.parent_task_id`` is intentionally a logical relationship (the
        schema cannot use a foreign key without making task history deletion
        destructive).  Every insertion path therefore performs the same
        existence, route-ownership, and depth checks before writing the child.
        The child Agent may differ from the parent; the manager's policy layer
        authorizes that cross-Agent delegation before reaching the store.
        """

        parent_id = str(task.parent_task_id or "")
        if not parent_id:
            return None
        parent = conn.execute(
            "SELECT task_id, channel, bot_id, external_user_id, session_id, "
            "child_depth FROM tasks WHERE task_id=?",
            (parent_id,),
        ).fetchone()
        if parent is None:
            raise NotFoundError(f"parent task not found: {parent_id}")
        expected_scope = {
            "channel": str(channel or ""),
            "bot_id": str(bot_id or ""),
            "external_user_id": str(external_user_id or ""),
            "session_id": str(session_id or "default"),
        }
        for field, expected in expected_scope.items():
            actual = str(parent[field] or ("default" if field == "session_id" else ""))
            if actual != expected:
                raise StoreError(
                    f"child task parent conflicts with ownership ({field})"
                )
        try:
            child_depth = int(task.child_depth)
        except (TypeError, ValueError) as exc:
            raise StoreError("invalid child-task depth") from exc
        expected_depth = int(parent["child_depth"] or 0) + 1
        if child_depth != expected_depth:
            raise StoreError(
                "child-task depth does not follow parent depth "
                f"({expected_depth})"
            )
        return parent_id

    @classmethod
    def _update_child_counter_tx(
        cls,
        conn: sqlite3.Connection,
        parent_task_id: str,
        *,
        now: str,
    ) -> None:
        """Persist a counter that never undercounts durable child rows."""

        durable_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE parent_task_id=?",
                (parent_task_id,),
            ).fetchone()[0]
        )
        counter = conn.execute(
            "SELECT child_count FROM child_task_counters WHERE parent_task_id=?",
            (parent_task_id,),
        ).fetchone()
        counter_count = int(counter["child_count"]) if counter else 0
        conn.execute(
            """INSERT INTO child_task_counters
               (parent_task_id, child_count, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(parent_task_id)
               DO UPDATE SET child_count=excluded.child_count,
                             updated_at=excluded.updated_at""",
            (parent_task_id, max(counter_count, durable_count), now),
        )

    @staticmethod
    def _mapping_snapshot(value: Any) -> dict[str, Any]:
        """Normalize a profile/mode/policy object without importing registries."""
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return dict(value)
        for name in ("as_dict", "to_dict"):
            method = getattr(value, name, None)
            if method is None:
                continue
            try:
                converted = method()
            except TypeError:
                converted = None
            if isinstance(converted, Mapping):
                return dict(converted)
        fields = getattr(value, "__dataclass_fields__", {})
        if fields:
            return {name: getattr(value, name) for name in fields if hasattr(value, name)}
        return {}

    @classmethod
    def _json_snapshot(cls, value: Any) -> Any:
        """Make set/tuple-heavy registry values deterministic JSON values."""
        if isinstance(value, Mapping):
            return {str(key): cls._json_snapshot(item) for key, item in value.items()}
        if isinstance(value, (set, frozenset)):
            return sorted(cls._json_snapshot(item) for item in value)
        if isinstance(value, (tuple, list)):
            return [cls._json_snapshot(item) for item in value]
        return value

    @classmethod
    def _reply_target_snapshot(cls, value: Any) -> Any:
        """Canonicalize a reply target for durable identity checks.

        A channel context token is a rolling transport hint.  It can change
        when the same inbound message is replayed after reconnecting, so it
        must not make an otherwise identical outbox projection conflict.
        Keep the helper local to the store boundary and accept both naming
        conventions used by channel payloads and older schemas.
        """

        snapshot = cls._json_snapshot(value)
        if not isinstance(snapshot, Mapping):
            return snapshot
        snapshot = dict(snapshot)
        snapshot.pop("context_token", None)
        snapshot.pop("contextToken", None)
        return snapshot

    @classmethod
    def _profile_snapshot_values(
        cls,
        *,
        agent_id: str,
        profile_version: int,
        snapshot: Any,
    ) -> tuple[dict[str, Any], bool]:
        data = cls._mapping_snapshot(snapshot)
        if data:
            snapshot_agent = str(data.get("agent_id", agent_id) or agent_id)
            snapshot_version = int(data.get("profile_version", data.get("version", profile_version)))
            if snapshot_agent != str(agent_id) or snapshot_version != int(profile_version):
                raise StoreError("profile snapshot does not match task ownership")
        def j(name: str, default: Any = ()) -> str:
            return json_dumps(cls._json_snapshot(data.get(name, default)))
        return (
            {
                "display_name": str(data.get("display_name", agent_id) or ""),
                "summary": str(data.get("summary", "") or ""),
                "system_prompt": str(data.get("system_prompt", "") or ""),
                "responsibilities_json": j("responsibilities"),
                "constraints_json": j("constraints"),
                "capabilities_json": j("capabilities"),
                "allowed_peers_json": j("allowed_peers"),
                "denied_peers_json": j("denied_peers"),
                "allowed_request_types_json": j("allowed_request_types"),
                "denied_request_types_json": j("denied_request_types"),
                "max_child_depth": int(data.get("max_child_depth", 0) or 0),
                "max_children_per_task": int(data.get("max_children_per_task", 0) or 0),
                "enabled": int(bool(data.get("enabled", True))),
                "default_mode_id": str(data.get("default_mode_id", "chat") or "chat"),
            },
            bool(data),
        )

    @classmethod
    def _mode_snapshot_values(
        cls,
        *,
        agent_id: str,
        mode_id: str,
        policy_version: int,
        snapshot: Any,
    ) -> tuple[dict[str, Any], bool]:
        data = cls._mapping_snapshot(snapshot)
        if data:
            snapshot_mode = str(data.get("mode_id", mode_id) or mode_id)
            snapshot_version = int(
                data.get("policy_version", data.get("mode_policy_version", policy_version))
            )
            if snapshot_mode != str(mode_id) or snapshot_version != int(policy_version):
                raise StoreError("mode snapshot does not match task ownership")
        sandbox = str(data.get("sandbox_policy", "read-only") or "read-only").replace("-", "_")
        return (
            {
                "developer_instructions": str(data.get("developer_instructions", "") or ""),
                "sandbox_policy": sandbox,
                "approval_policy": str(data.get("approval_policy", "deny_all") or "deny_all"),
                "allowed_tools_json": json_dumps(cls._json_snapshot(data.get("allowed_tools", ()))),
                "denied_tools_json": json_dumps(cls._json_snapshot(data.get("denied_tools", ()))),
                "can_write_files": int(bool(data.get("can_write_files", False))),
                "can_execute_commands": int(bool(data.get("can_execute_commands", False))),
                "can_create_child_tasks": int(bool(data.get("can_create_child_tasks", False))),
                "can_send_agent_messages": int(bool(data.get("can_send_agent_messages", False))),
            },
            bool(data),
        )

    @staticmethod
    def _legacy_profile_placeholder(row: sqlite3.Row, *, agent_id: str, profile_version: int) -> bool:
        """Recognize rows written by the pre-snapshot Codex seed only."""
        if str(agent_id) != "codex" or int(profile_version) != 1:
            return False
        return (
            str(row["display_name"] or "") in {"", "codex", "Codex"}
            and str(row["summary"] or "") in {"", "Codex runtime"}
            and not str(row["system_prompt"] or "")
            and all(
                json_loads(row[name], []) in ([], {})
                for name in (
                    "responsibilities_json",
                    "constraints_json",
                    "capabilities_json",
                    "allowed_peers_json",
                    "denied_peers_json",
                    "allowed_request_types_json",
                    "denied_request_types_json",
                )
            )
            and int(row["max_child_depth"] or 0) == 0
            and int(row["max_children_per_task"] or 0) == 0
            and int(row["enabled"] or 0) == 1
            and str(row["default_mode_id"] or "") == "chat"
        )

    @staticmethod
    def _legacy_mode_placeholder(
        row: sqlite3.Row, *, agent_id: str, mode_id: str, policy_version: int
    ) -> bool:
        """Recognize the exact pre-snapshot rows for built-in Codex modes."""

        if (
            str(agent_id) != "codex"
            or str(mode_id) not in {"chat", "plan", "review", "execute"}
            or int(policy_version) != 1
        ):
            return False
        return (
            not str(row["developer_instructions"] or "")
            and str(row["sandbox_policy"] or "") == "read_only"
            and str(row["approval_policy"] or "") == "deny_all"
            and json_loads(row["allowed_tools_json"], []) == []
            and json_loads(row["denied_tools_json"], []) == []
            and not any(
                bool(row[column])
                for column in (
                    "can_write_files",
                    "can_execute_commands",
                    "can_create_child_tasks",
                    "can_send_agent_messages",
                )
            )
        )

    @classmethod
    def _interim_execute_v1_snapshot(cls, row: sqlite3.Row) -> bool:
        """Recognize the short-lived full-access rewrite of execute@1.

        That release changed an already-published immutable row and then
        failed on its next startup. Repair only the exact known built-in
        snapshot; arbitrary conflicts remain fail-closed.
        """

        from .modes import legacy_builtin_modes

        canonical = legacy_builtin_modes()["execute"]
        values, _ = cls._mode_snapshot_values(
            agent_id="codex",
            mode_id="execute",
            policy_version=1,
            snapshot=canonical,
        )

        def tool_set(value: Any) -> frozenset[str] | None:
            try:
                decoded = json.loads(value)
            except (TypeError, ValueError):
                return None
            if not isinstance(decoded, list) or not all(
                isinstance(item, str) for item in decoded
            ):
                return None
            return frozenset(decoded)

        for column, expected in values.items():
            actual = row[column]
            if column == "sandbox_policy":
                if str(actual or "") != "full_access":
                    return False
            elif column in {"allowed_tools_json", "denied_tools_json"}:
                actual_tools = tool_set(actual)
                expected_tools = tool_set(expected)
                if actual_tools is None or actual_tools != expected_tools:
                    return False
            elif str(actual) != str(expected):
                return False
        return True

    @classmethod
    def _ensure_profile_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        agent_id: str,
        profile_version: int,
        snapshot: Any = None,
        now: str,
        allow_legacy_seed_upgrade: bool = False,
    ) -> None:
        values, supplied = cls._profile_snapshot_values(
            agent_id=agent_id, profile_version=int(profile_version), snapshot=snapshot
        )
        row = conn.execute(
            "SELECT * FROM agent_profiles WHERE agent_id=? AND profile_version=?",
            (agent_id, int(profile_version)),
        ).fetchone()
        if row is not None:
            if supplied:
                mismatches = []
                for column, expected in values.items():
                    actual = row[column]
                    if column.endswith("_json"):
                        if cls._json_snapshot(json_loads(actual, [])) != cls._json_snapshot(
                            json_loads(expected, [])
                        ):
                            mismatches.append(column)
                    elif str(actual) != str(expected):
                        mismatches.append(column)
                if mismatches:
                    if (
                        allow_legacy_seed_upgrade
                        and cls._legacy_profile_placeholder(
                            row,
                            agent_id=agent_id,
                            profile_version=int(profile_version),
                        )
                    ):
                        assignments = ", ".join(f"{column}=?" for column in values)
                        conn.execute(
                            f"UPDATE agent_profiles SET {assignments} WHERE agent_id=? AND profile_version=?",
                            [*values.values(), agent_id, int(profile_version)],
                        )
                    else:
                        raise StoreError(
                            f"profile version metadata conflicts: {agent_id}@{profile_version}"
                        )
            return
        conn.execute(
            """INSERT INTO agent_profiles
               (agent_id, profile_version, display_name, summary, system_prompt,
                responsibilities_json, constraints_json, capabilities_json,
                allowed_peers_json, denied_peers_json, allowed_request_types_json,
                denied_request_types_json, max_child_depth, max_children_per_task,
                enabled, default_mode_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                agent_id,
                int(profile_version),
                values["display_name"],
                values["summary"],
                values["system_prompt"],
                values["responsibilities_json"],
                values["constraints_json"],
                values["capabilities_json"],
                values["allowed_peers_json"],
                values["denied_peers_json"],
                values["allowed_request_types_json"],
                values["denied_request_types_json"],
                values["max_child_depth"],
                values["max_children_per_task"],
                values["enabled"],
                values["default_mode_id"],
                now,
            ),
        )

    @classmethod
    def _ensure_mode_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        agent_id: str,
        mode_id: str,
        policy_version: int,
        snapshot: Any = None,
        now: str,
        allow_legacy_seed_upgrade: bool = False,
    ) -> None:
        values, supplied = cls._mode_snapshot_values(
            agent_id=agent_id,
            mode_id=mode_id,
            policy_version=int(policy_version),
            snapshot=snapshot,
        )
        row = conn.execute(
            "SELECT * FROM agent_modes WHERE agent_id=? AND mode_id=? AND policy_version=?",
            (agent_id, mode_id, int(policy_version)),
        ).fetchone()
        if row is not None:
            if supplied:
                for column, expected in values.items():
                    actual = row[column]
                    if column.endswith("_json"):
                        equal = cls._json_snapshot(json_loads(actual, [])) == cls._json_snapshot(
                            json_loads(expected, [])
                        )
                    else:
                        equal = str(actual) == str(expected)
                    if not equal:
                        if (
                            allow_legacy_seed_upgrade
                            and (
                                cls._legacy_mode_placeholder(
                                    row,
                                    agent_id=agent_id,
                                    mode_id=mode_id,
                                    policy_version=int(policy_version),
                                )
                                or (
                                    str(agent_id) == "codex"
                                    and str(mode_id) == "execute"
                                    and int(policy_version) == 1
                                    and cls._interim_execute_v1_snapshot(row)
                                )
                            )
                        ):
                            assignments = ", ".join(f"{column}=?" for column in values)
                            conn.execute(
                                f"UPDATE agent_modes SET {assignments} WHERE agent_id=? AND mode_id=? AND policy_version=?",
                                [*values.values(), agent_id, mode_id, int(policy_version)],
                            )
                            break
                        raise StoreError(
                            f"mode version metadata conflicts: {agent_id}/{mode_id}@{policy_version}"
                        )
            return
        conn.execute(
            """INSERT INTO agent_modes
               (agent_id, mode_id, policy_version, developer_instructions,
                sandbox_policy, approval_policy, allowed_tools_json, denied_tools_json,
                can_write_files, can_execute_commands, can_create_child_tasks,
                can_send_agent_messages, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                agent_id,
                mode_id,
                int(policy_version),
                values["developer_instructions"],
                values["sandbox_policy"],
                values["approval_policy"],
                values["allowed_tools_json"],
                values["denied_tools_json"],
                values["can_write_files"],
                values["can_execute_commands"],
                values["can_create_child_tasks"],
                values["can_send_agent_messages"],
                now,
            ),
        )

    @classmethod
    def _ensure_profile_mode_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        agent_id: str,
        profile_version: int,
        mode_id: str,
        policy_version: int,
        now: str,
        metadata: Any = None,
        profile_snapshot: Any = None,
        mode_snapshot: Any = None,
    ) -> None:
        metadata_map = cls._mapping_snapshot(metadata)
        profile_snapshot = profile_snapshot or metadata_map.get("profile") or metadata_map.get("agent_profile")
        mode_snapshot = mode_snapshot or metadata_map.get("mode")
        if mode_snapshot is None:
            effective = cls._mapping_snapshot(metadata_map.get("effective_policy"))
            if effective:
                mode_snapshot = {
                    "mode_id": mode_id,
                    "policy_version": effective.get("mode_policy_version", policy_version),
                    **{
                        key: effective[key]
                        for key in (
                            "developer_instructions",
                            "sandbox_policy",
                            "approval_policy",
                            "allowed_tools",
                            "denied_tools",
                            "can_write_files",
                            "can_execute_commands",
                            "can_create_child_tasks",
                            "can_send_agent_messages",
                        )
                        if key in effective
                    },
                }
        cls._ensure_profile_tx(
            conn,
            agent_id=agent_id,
            profile_version=int(profile_version),
            snapshot=profile_snapshot,
            now=now,
        )
        cls._ensure_mode_tx(
            conn,
            agent_id=agent_id,
            mode_id=mode_id,
            policy_version=int(policy_version),
            snapshot=mode_snapshot,
            now=now,
        )

    @classmethod
    def _ensure_conversation_tx(
        cls,
        conn: sqlite3.Connection,
        task: AgentTask,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        now: str,
    ) -> tuple[str, bool]:
        session_id = session_id or "default"
        conversation_id = task.conversation_id
        if not conversation_id:
            conversation_id = canonical_conversation_id(
                channel, bot_id, external_user_id, session_id, task.agent_id
            )
        cls._ensure_profile_mode_tx(
            conn,
            agent_id=task.agent_id,
            profile_version=task.profile_version,
            mode_id=task.mode_id,
            policy_version=task.policy_version,
            now=now,
            metadata=task.metadata,
        )
        existing = conn.execute(
            "SELECT channel, bot_id, external_user_id, session_id, agent_id, mode_id, "
            "profile_version, policy_version FROM conversations WHERE conversation_id=?",
            (conversation_id,),
        ).fetchone()
        if existing is not None:
            immutable = {
                "channel": channel,
                "bot_id": bot_id,
                "external_user_id": external_user_id,
                "session_id": session_id,
                "agent_id": task.agent_id,
            }
            for column, expected in immutable.items():
                if str(existing[column]) != str(expected):
                    raise StoreError(
                        f"conversation identity conflicts: {conversation_id} ({column})"
                    )
            return conversation_id
        candidate_ids = conversation_id_candidates(
            channel, bot_id, external_user_id, session_id, task.agent_id
        )
        canonical_id = candidate_ids[0]
        legacy_id = candidate_ids[-1]
        if conversation_id == canonical_id and legacy_id != canonical_id:
            legacy = conn.execute(
                "SELECT channel, bot_id, external_user_id, session_id, agent_id, "
                "mode_id, profile_version, policy_version "
                "FROM conversations WHERE conversation_id=?",
                (legacy_id,),
            ).fetchone()
            if legacy is not None:
                immutable = {
                    "channel": channel,
                    "bot_id": bot_id,
                    "external_user_id": external_user_id,
                    "session_id": session_id,
                    "agent_id": task.agent_id,
                }
                if all(
                    str(legacy[column]) == str(expected)
                    for column, expected in immutable.items()
                ):
                    return legacy_id
        conn.execute(
            """INSERT OR IGNORE INTO conversations
               (conversation_id, channel, bot_id, external_user_id, session_id,
                agent_id, mode_id, profile_version, policy_version, thread_id,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                conversation_id,
                channel,
                bot_id,
                external_user_id,
                session_id,
                task.agent_id,
                task.mode_id,
                int(task.profile_version),
                int(task.policy_version),
                task.thread_id,
                now,
                now,
            ),
        )
        # ``INSERT OR IGNORE`` can only lose a race after the validation above;
        # re-read and fail closed if another writer inserted a different scope.
        created = conn.execute(
            "SELECT channel, bot_id, external_user_id, session_id, agent_id, mode_id, "
            "profile_version, policy_version FROM conversations WHERE conversation_id=?",
            (conversation_id,),
        ).fetchone()
        if created is None:
            raise StoreError("conversation insert failed")
        for column, expected in {
            "channel": channel,
            "bot_id": bot_id,
            "external_user_id": external_user_id,
            "session_id": session_id,
            "agent_id": task.agent_id,
        }.items():
            if str(created[column]) != str(expected):
                raise StoreError(f"conversation identity conflicts: {conversation_id} ({column})")
        return conversation_id

    @staticmethod
    def _thread_binding_tx(
        conn: sqlite3.Connection,
        *,
        conversation_id: str,
        mode_id: str,
        profile_version: int,
        policy_version: int,
    ) -> str | None:
        row = conn.execute(
            """SELECT thread_id FROM thread_bindings
               WHERE conversation_id=? AND mode_id=? AND profile_version=?
                 AND policy_version=?""",
            (conversation_id, mode_id, int(profile_version), int(policy_version)),
        ).fetchone()
        return str(row["thread_id"]) if row is not None else None

    @staticmethod
    def _transition_inbound_tx(
        conn: sqlite3.Connection,
        message_id: str,
        target: InboundState,
        *,
        now: str,
        task_id: str | None = None,
    ) -> bool:
        """Apply one inbound transition inside an existing transaction."""

        row = conn.execute(
            "SELECT status FROM inbound_messages WHERE message_id=?",
            (message_id,),
        ).fetchone()
        if row is None:
            return False
        current = InboundState(str(row["status"]))
        if current == target:
            return False
        if target not in _ALLOWED_INBOUND_TRANSITIONS[current]:
            raise InvalidTransition(
                f"invalid inbound transition {current.value!r} -> {target.value!r}"
            )
        assignments = ["status=?", "stored_at=?"]
        values: list[Any] = [target.value, now]
        if task_id is not None:
            assignments.append("task_id=?")
            values.append(task_id)
        return conn.execute(
            "UPDATE inbound_messages SET " + ", ".join(assignments)
            + " WHERE message_id=? AND status=?",
            [*values, message_id, current.value],
        ).rowcount == 1

    @classmethod
    def _advance_confirmation_inbound_tx(
        cls,
        conn: sqlite3.Connection,
        message_id: str | None,
        target: InboundState,
        *,
        now: str,
        task_id: str | None = None,
    ) -> bool:
        """Advance a linked audio ingress row without regressing siblings.

        Older candidate rows may point at an inbound that is still ``stored``;
        newer creation moves it to ``awaiting_confirmation`` atomically.  This
        helper bridges that legacy gap, while treating a later aggregate state
        (for example one sibling already consumed) as authoritative.
        """

        if not message_id:
            return False
        row = conn.execute(
            "SELECT status FROM inbound_messages WHERE message_id=?",
            (message_id,),
        ).fetchone()
        if row is None:
            return False
        current = InboundState(str(row["status"]))
        changed = False
        if current == InboundState.STORED and target != InboundState.AWAITING_CONFIRMATION:
            changed = cls._transition_inbound_tx(
                conn,
                message_id,
                InboundState.AWAITING_CONFIRMATION,
                now=now,
            ) or changed
            current = InboundState.AWAITING_CONFIRMATION
        if target == InboundState.TASK_QUEUED and current == InboundState.AWAITING_CONFIRMATION:
            changed = cls._transition_inbound_tx(
                conn,
                message_id,
                InboundState.CONFIRMED,
                now=now,
            ) or changed
            current = InboundState.CONFIRMED
        if current == target:
            return changed
        if target not in _ALLOWED_INBOUND_TRANSITIONS[current]:
            # A sibling candidate may already have advanced this aggregate
            # ingress row.  Never regress confirmed/task-owned input because a
            # different candidate was rejected later.
            return changed
        return cls._transition_inbound_tx(
            conn,
            message_id,
            target,
            now=now,
            task_id=task_id,
        ) or changed

    @classmethod
    def _advance_terminal_confirmation_inbound_tx(
        cls,
        conn: sqlite3.Connection,
        message_id: str | None,
        confirmation_id: str,
        target: InboundState,
        *,
        now: str,
    ) -> bool:
        """Project a terminal candidate only after all siblings are terminal."""

        if target not in {InboundState.REJECTED, InboundState.EXPIRED}:
            raise ValueError("terminal confirmation target must be rejected or expired")
        if message_id:
            sibling = conn.execute(
                "SELECT 1 FROM transcription_candidates "
                "WHERE inbound_message_id=? AND confirmation_id<>? "
                "AND status IN ('pending','confirmed') LIMIT 1",
                (message_id, confirmation_id),
            ).fetchone()
            if sibling is not None:
                return False
        return cls._advance_confirmation_inbound_tx(
            conn,
            message_id,
            target,
            now=now,
        )

    # ------------------------------------------------------------------
    # Ingress and task creation
    # ------------------------------------------------------------------
    async def store_inbound(
        self,
        inbound: InboundMessage | Mapping[str, Any] | None = None,
        *,
        cursor: str | None = None,
        channel_cursor: str | None = None,
        cursor_channel: str | None = None,
        cursor_bot_id: str | None = None,
        _trusted_media_wire_fingerprints: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> InboundMessage:
        """Persist one normalized inbound envelope, idempotently.

        The insert is committed before this method returns.  A duplicate
        identity returns the original immutable row rather than overwriting
        channel data.
        """

        inbound_keys = {
            "channel", "bot_id", "external_user_id", "external_message_id",
            "text", "content", "body", "session_id", "source_sequence",
            "context_token", "payload", "received_at", "message_id",
        }
        message = self._coerce_inbound(
            inbound,
            {key: value for key, value in kwargs.items() if key in inbound_keys},
        )
        stored_payload = self._sanitize_inbound_payload(
            message.payload,
            trusted_media_wire_fingerprints=(
                _trusted_media_wire_fingerprints
            ),
        )
        cursor_value = cursor if cursor is not None else channel_cursor
        now = self._now()

        def op(conn: sqlite3.Connection) -> InboundMessage:
            with _transaction(conn):
                insert_cursor = conn.execute(
                    """INSERT OR IGNORE INTO inbound_messages
                       (message_id, channel, bot_id, external_user_id,
                        external_message_id, session_id, source_sequence,
                        context_token, text, payload_json, status,
                        received_at, stored_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        message.message_id,
                        message.channel,
                        message.bot_id,
                        message.external_user_id,
                        message.external_message_id,
                        message.session_id,
                        message.source_sequence,
                        message.context_token,
                        message.text,
                        json_dumps(stored_payload),
                        # ``store_inbound`` is the durable ``received ->
                        # stored`` transition.  A freshly-normalized
                        # envelope carries ``received`` by default, but it
                        # must not remain in that transient state after the
                        # commit.  Preserve an explicit later state for
                        # callers replaying a persisted row.
                        (
                            InboundState.STORED.value
                            if _enum_value(message.status) == InboundState.RECEIVED.value
                            else _enum_value(message.status, InboundState.STORED.value)
                        ),
                        _utc_text(message.received_at),
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM inbound_messages WHERE channel = ? AND bot_id = ? AND external_message_id = ?",
                    message.dedupe_identity,
                ).fetchone()
                if row is None:
                    raise StoreError("failed to persist inbound message")
                if insert_cursor.rowcount == 0:
                    self._validate_inbound_replay_tx(
                        row,
                        message,
                        trusted_media_wire_fingerprints=(
                            _trusted_media_wire_fingerprints
                        ),
                    )
                # The channel identity key deliberately follows the protocol
                # `(channel, bot, external_message_id)`, but a malformed or
                # sender-scoped ID must never let one user's envelope alias
                # another user's task.  Treat a scope mismatch as a hard
                # ownership conflict rather than a harmless duplicate.
                if (
                    str(row["external_user_id"] or "")
                    != str(message.external_user_id or "")
                    or str(row["session_id"] or "default")
                    != str(message.session_id or "default")
                ):
                    raise StoreError(
                        "inbound identity conflicts with an existing user/session"
                    )
                self._ensure_reply_scope_for_inbound_row_tx(conn, row)
                self._retain_inbound_attachments_tx(
                    conn,
                    message_id=str(row["message_id"]),
                    attachments=self._input_attachment_values(
                        json_loads(row["payload_json"], {}) or {}
                    ),
                    channel=str(row["channel"] or ""),
                    bot_id=str(row["bot_id"] or ""),
                    external_user_id=str(row["external_user_id"] or ""),
                    session_id=str(row["session_id"] or "default"),
                    source_message_id=str(row["external_message_id"] or ""),
                    agent_id=None,
                    created_at=row["stored_at"],
                )
                cursor_scope = (
                    cursor_channel or message.channel,
                    cursor_bot_id or message.bot_id,
                )
                existing_cursor = conn.execute(
                    "SELECT cursor FROM channel_cursors WHERE channel=? AND bot_id=?",
                    cursor_scope,
                ).fetchone()
                # The message row makes a duplicate cursor safe to repair,
                # while numeric checkpoints are never allowed to regress.
                current_cursor = (
                    str(existing_cursor["cursor"])
                    if existing_cursor is not None
                    else None
                )
                if _cursor_should_advance(current_cursor, cursor_value):
                    conn.execute(
                        """INSERT INTO channel_cursors(channel, bot_id, cursor, updated_at)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(channel, bot_id)
                           DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at""",
                        (
                            cursor_scope[0],
                            cursor_scope[1],
                            str(cursor_value),
                            now,
                        ),
                    )
                return self._inbound_from_row(row)

        return await self._call(op)

    # Aliases used by channel adapters.
    record_inbound = store_inbound
    persist_inbound = store_inbound

    @classmethod
    def _delivery_reply_scope_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        target: ReplyTarget,
        reply_scope_id: str | None,
    ) -> sqlite3.Row | None:
        scope = (
            conn.execute(
                "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                (str(reply_scope_id),),
            ).fetchone()
            if reply_scope_id
            else cls._reply_scope_for_target_tx(conn, target)
        )
        if scope is None:
            return None
        if (
            str(scope["channel"]),
            str(scope["bot_id"]),
            str(scope["external_user_id"]),
            str(scope["session_id"] or "default"),
        ) != (
            str(target.channel),
            str(target.bot_id),
            str(target.external_user_id),
            str(target.session_id or "default"),
        ):
            raise StoreError("delivery reply scope conflicts with task recipient")
        return scope

    @classmethod
    def _project_initial_task_reply_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        initial_reply: Any,
        task: TaskRecord,
        fallback_target: ReplyTarget,
        delivery_reply_scope_id: str | None,
        now: str,
    ) -> tuple[str, bool]:
        """Reserve a command acknowledgement before queued work is visible."""

        data: dict[str, Any] = {}
        if isinstance(initial_reply, Mapping):
            data.update(initial_reply)
        elif initial_reply is not None:
            for name in (
                "target",
                "reply_target",
                "source_key",
                "idempotency_key",
                "content",
                "text",
                "attachments",
                "priority",
                "delivery_mode",
                "outbox_id",
                "delivery_id",
                "client_id",
                "contextless_client_id",
                "from_user_id",
                "reply_scope_id",
                "agent_id",
            ):
                if hasattr(initial_reply, name):
                    data[name] = getattr(initial_reply, name)
            converter = getattr(initial_reply, "to_dict", None) or getattr(
                initial_reply, "as_dict", None
            )
            if converter is not None:
                try:
                    converted = converter()
                except TypeError:
                    converted = None
                if isinstance(converted, Mapping):
                    data.update(converted)
        target = cls._coerce_reply_target(
            data.get("target", data.get("reply_target")), fallback=fallback_target
        )
        source_key = str(
            data.get("source_key")
            or data.get("idempotency_key")
            or data.get("outbox_id")
            or data.get("delivery_id")
            or ""
        ).strip()
        if not source_key:
            raise ValueError("initial task reply source_key is required")
        raw_attachments = data.get("attachments", ()) or ()
        if isinstance(raw_attachments, (str, bytes, bytearray, Mapping)):
            attachments = (raw_attachments,)
        else:
            attachments = tuple(raw_attachments)
        scope_value = str(
            delivery_reply_scope_id or data.get("reply_scope_id") or ""
        ) or None
        scope = cls._delivery_reply_scope_tx(
            conn, target=target, reply_scope_id=scope_value
        )
        if scope is None:
            raise NotFoundError(
                "initial task reply does not identify a stored inbound scope"
            )
        projection = cls._project_reply_candidate_tx(
            conn,
            target=target,
            source_key=source_key,
            content=str(data.get("content", data.get("text", "")) or ""),
            attachments=attachments,
            reply_scope_id=str(scope["reply_scope_id"]),
            # This is an independent command response, not task output.  Its
            # identity must match the Gateway's immediate delivery replay.
            agent_id=str(data.get("agent_id") or task.agent_id),
            task_id=None,
            priority=data.get("priority", EventPriority.NORMAL),
            delivery_mode=data.get(
                "delivery_mode", DeliveryMode.PUSH_ELIGIBLE
            ),
            notify_enabled=True,
            foreground=True,
            preferred_outbox_id=(
                str(data.get("outbox_id") or data.get("delivery_id") or "")
                or None
            ),
            preferred_client_id=str(data.get("client_id") or "") or None,
            preferred_contextless_client_id=(
                str(data.get("contextless_client_id") or "") or None
            ),
            from_user_id=str(data.get("from_user_id") or target.bot_id or ""),
            now=now,
        )
        if not projection.slots or not projection.outbox_items:
            raise StoreError(
                "initial task reply could not reserve a canonical send slot"
            )
        return str(scope["reply_scope_id"]), bool(projection.replayed)

    async def create_task(
        self,
        task: AgentTask | Mapping[str, Any] | None = None,
        *,
        inbound_message_id: str | None = None,
        dedupe_key: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str | None = None,
        initial_reply: Any = None,
        delivery_reply_scope_id: str | None = None,
        _reserve_child_parent_id: str | None = None,
        _reserve_child_max: int | None = None,
        **kwargs: Any,
    ) -> TaskRecord:
        """Create a queued task, returning the existing row for duplicates."""

        values = dict(kwargs)
        if inbound_message_id is not None:
            values["inbound_message_id"] = inbound_message_id
        if dedupe_key is not None:
            values["dedupe_key"] = dedupe_key
        # Explicit keyword arguments take precedence, but omitted/empty
        # convenience defaults must not erase channel identity carried by a
        # task mapping or ReplyTarget.
        if channel:
            values["channel"] = channel
        if bot_id:
            values["bot_id"] = bot_id
        if external_user_id:
            values["external_user_id"] = external_user_id
        if session_id:
            values["session_id"] = session_id
        task_snapshot = self._coerce_task(task, values)
        # Route identity is not part of AgentTask's dataclass fields.  Capture
        # top-level mapping keys before coercion, then derive omitted scope
        # from the immutable ReplyTarget so direct mapping callers cannot
        # accidentally create an empty/shared conversation.
        task_route: Mapping[str, Any] = task if isinstance(task, Mapping) else {}
        target_route = task_snapshot.reply_target
        mapped_channel = str(task_route.get("channel") or "")
        mapped_bot_id = str(task_route.get("bot_id") or "")
        mapped_user_id = str(task_route.get("external_user_id") or "")
        mapped_session_id = str(task_route.get("session_id") or "")
        # Use the normalized snapshot for race recovery too.  Callers often
        # place these identities inside the task mapping rather than in the
        # convenience keyword arguments.
        effective_dedupe_key = task_snapshot.dedupe_key or dedupe_key
        effective_inbound_id = task_snapshot.inbound_message_id or inbound_message_id
        now = self._now()

        def prepare_initial_delivery_tx(
            conn: sqlite3.Connection, record: TaskRecord
        ) -> TaskRecord:
            scope_id: str | None = None
            replayed = True
            if initial_reply is not None:
                scope_id, replayed = self._project_initial_task_reply_tx(
                    conn,
                    initial_reply=initial_reply,
                    task=record,
                    fallback_target=record.reply_target,
                    delivery_reply_scope_id=delivery_reply_scope_id,
                    now=now,
                )
            elif delivery_reply_scope_id:
                scope = self._delivery_reply_scope_tx(
                    conn,
                    target=record.reply_target,
                    reply_scope_id=delivery_reply_scope_id,
                )
                if scope is None:
                    raise NotFoundError(
                        "task delivery scope does not identify a stored inbound"
                    )
                scope_id = str(scope["reply_scope_id"])
            if scope_id is None:
                return record
            if record.state == TaskState.QUEUED:
                stored_pending = str(
                    record.pending_delivery_reply_scope_id or ""
                )
                if stored_pending and stored_pending != scope_id:
                    raise StoreError(
                        "queued task delivery reply scope conflicts"
                    )
                conn.execute(
                    "UPDATE tasks SET pending_delivery_reply_scope_id=? "
                    "WHERE task_id=? AND state='queued'",
                    (scope_id, record.task_id),
                )
                refreshed = self._fetch_task_tx(conn, record.task_id)
                if refreshed is None:
                    raise StoreError("task disappeared while preparing delivery")
                return refreshed
            if not replayed:
                # A task already visible to a dispatcher cannot acquire its
                # first acknowledgement after the claim.  Roll the new
                # projection back and force the caller to reconcile.
                raise StoreError(
                    "task was claimed before its initial reply reservation"
                )
            execution = conn.execute(
                "SELECT delivery_reply_scope_id FROM task_executions "
                "WHERE task_id=? ORDER BY attempt DESC LIMIT 1",
                (record.task_id,),
            ).fetchone()
            if (
                execution is not None
                and execution["delivery_reply_scope_id"] is not None
                and str(execution["delivery_reply_scope_id"]) != scope_id
            ):
                raise StoreError("task execution delivery reply scope conflicts")
            return record

        def op(conn: sqlite3.Connection) -> TaskRecord | None:
            with _transaction(conn):
                inbound_id = task_snapshot.inbound_message_id
                inbound_row: sqlite3.Row | None = None
                if inbound_id:
                    inbound_row = conn.execute(
                        "SELECT channel, bot_id, external_user_id, session_id, message_id, "
                        "external_message_id, source_sequence, context_token "
                        "FROM inbound_messages WHERE message_id = ?",
                        (inbound_id,),
                    ).fetchone()
                    if inbound_row is None:
                        raise NotFoundError(
                            f"inbound message not found: {inbound_id}"
                        )
                    if inbound_row is not None:
                        # A linked inbound envelope owns its channel scope.
                        # Explicit convenience fields or a ReplyTarget may
                        # not redirect that durable message, even when the
                        # dedupe lookup would otherwise return an existing
                        # task before insertion.
                        for supplied, persisted, field in (
                            (channel or mapped_channel, inbound_row["channel"], "channel"),
                            (bot_id or mapped_bot_id, inbound_row["bot_id"], "bot_id"),
                            (
                                external_user_id or mapped_user_id,
                                inbound_row["external_user_id"],
                                "external_user_id",
                            ),
                            (
                                session_id or mapped_session_id,
                                inbound_row["session_id"] or "default",
                                "session_id",
                            ),
                        ):
                            if supplied and str(supplied) != str(persisted):
                                raise StoreError(
                                    f"task scope conflicts with inbound ownership ({field})"
                                )
                        self._validate_reply_target_scope(
                            self._coerce_reply_target(task_snapshot.reply_target),
                            channel=inbound_row["channel"],
                            bot_id=inbound_row["bot_id"],
                            external_user_id=inbound_row["external_user_id"],
                            session_id=inbound_row["session_id"] or "default",
                        )
                        channel_value = (
                            channel or mapped_channel or inbound_row["channel"] or target_route.channel
                        )
                        bot_value = (
                            bot_id or mapped_bot_id or inbound_row["bot_id"] or target_route.bot_id
                        )
                        user_value = (
                            external_user_id
                            or mapped_user_id
                            or inbound_row["external_user_id"]
                            or target_route.external_user_id
                        )
                        session_value = (
                            session_id
                            or mapped_session_id
                            or inbound_row["session_id"]
                            or target_route.session_id
                            or "default"
                        )
                    else:
                        channel_value, bot_value, user_value, session_value = (
                            channel or mapped_channel or target_route.channel,
                            bot_id or mapped_bot_id or target_route.bot_id,
                            external_user_id or mapped_user_id or target_route.external_user_id,
                            session_id or mapped_session_id or target_route.session_id or "default",
                        )
                else:
                    channel_value, bot_value, user_value, session_value = (
                        channel or mapped_channel or target_route.channel,
                        bot_id or mapped_bot_id or target_route.bot_id,
                        external_user_id or mapped_user_id or target_route.external_user_id,
                        session_id or mapped_session_id or target_route.session_id or "default",
                    )
                if not task_snapshot.dedupe_key and inbound_id:
                    dedupe = f"inbound:{inbound_id}"
                else:
                    dedupe = task_snapshot.dedupe_key
                conversation_id = self._ensure_conversation_tx(
                    conn,
                    task_snapshot,
                    channel=channel_value,
                    bot_id=bot_value,
                    external_user_id=user_value,
                    session_id=session_value,
                    now=now,
                )
                thread_id = task_snapshot.thread_id or self._thread_binding_tx(
                    conn,
                    conversation_id=conversation_id,
                    mode_id=task_snapshot.mode_id,
                    profile_version=task_snapshot.profile_version,
                    policy_version=task_snapshot.policy_version,
                )
                existing = self._fetch_task_by_dedupe_tx(conn, dedupe)
                if existing is not None:
                    self._validate_dedupe_owner(
                        existing,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                        agent_id=task_snapshot.agent_id,
                        parent_task_id=task_snapshot.parent_task_id,
                        conversation_id=conversation_id,
                    )
                    self._retain_task_input_attachments_tx(
                        conn,
                        task_id=existing.task_id,
                        inputs=existing.inputs,
                        created_at=existing.created_at,
                    )
                    return prepare_initial_delivery_tx(conn, existing)
                # A task may be deduplicated by inbound id even when callers
                # supplied a different dedupe key (the schema enforces this).
                if inbound_id:
                    row = conn.execute(
                        f"{self._task_select_sql()} WHERE t.inbound_message_id = ?",
                        (inbound_id,),
                    ).fetchone()
                    existing = self._task_from_row(row)
                    if existing is not None:
                        self._validate_dedupe_owner(
                            existing,
                            channel=channel_value,
                            bot_id=bot_value,
                            external_user_id=user_value,
                            session_id=session_value,
                            agent_id=task_snapshot.agent_id,
                            parent_task_id=task_snapshot.parent_task_id,
                            conversation_id=conversation_id,
                        )
                        return prepare_initial_delivery_tx(conn, existing)
                task_id = task_snapshot.task_id or _uuid()
                # Child admission and task insertion must share one durable
                # transaction.  A process crash between a standalone counter
                # reservation and INSERT would otherwise leak a child slot
                # forever.  The duplicate checks above intentionally precede
                # this block so replaying an already-created child does not
                # increment the counter a second time.
                child_parent_id = (
                    str(task_snapshot.parent_task_id)
                    if task_snapshot.parent_task_id
                    else None
                )
                if _reserve_child_parent_id is not None:
                    parent_id = str(_reserve_child_parent_id)
                    if str(task_snapshot.parent_task_id or "") != parent_id:
                        raise StoreError(
                            "child reservation parent does not match task snapshot"
                        )
                    child_parent_id = parent_id
                if child_parent_id is not None:
                    # Validate the logical parent relationship on every task
                    # insertion path, including compatibility callers that do
                    # not use ``create_child_task``.
                    self._validate_child_parent_tx(
                        conn,
                        task_snapshot,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                    )

                current_count: int | None = None
                if child_parent_id is not None:
                    counter = conn.execute(
                        "SELECT child_count FROM child_task_counters "
                        "WHERE parent_task_id=?", (child_parent_id,)
                    ).fetchone()
                    durable_count = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM tasks WHERE parent_task_id=?",
                            (child_parent_id,),
                        ).fetchone()[0]
                    )
                    # Legacy standalone reservations and direct task inserts
                    # can leave either side ahead.  Admission must account for
                    # both until startup reconciliation repairs the counter.
                    current_count = max(
                        int(counter["child_count"]) if counter else 0,
                        durable_count,
                    )
                    if _reserve_child_parent_id is not None:
                        max_children = (
                            int(_reserve_child_max)
                            if _reserve_child_max is not None
                            else -1
                        )
                        if max_children >= 0 and current_count >= max_children:
                            raise PermissionError(
                                "maximum child-task count exceeded"
                            )
                reply_target = self._coerce_reply_target(
                    task_snapshot.reply_target,
                    fallback=ReplyTarget(
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                        source_message_id=(
                            inbound_row["external_message_id"]
                            if inbound_id and inbound_row is not None
                            else inbound_id
                        ),
                        source_sequence=(
                            inbound_row["source_sequence"]
                            if inbound_id and inbound_row is not None
                            else None
                        ),
                        context_token=(
                            inbound_row["context_token"]
                            if inbound_id and inbound_row is not None
                            else None
                        ),
                    ),
                )
                self._validate_reply_target_scope(
                    reply_target,
                    channel=channel_value,
                    bot_id=bot_value,
                    external_user_id=user_value,
                    session_id=session_value,
                )
                conn.execute(
                    """INSERT INTO tasks
                       (task_id, dedupe_key, inbound_message_id, channel, bot_id,
                        external_user_id, session_id, agent_id, conversation_id,
                        thread_id, mode_id, profile_version, policy_version,
                        model, reasoning_effort, reply_target_json, inputs_json,
                        metadata_json,
                        state, attempts, next_attempt_at, parent_task_id,
                        child_depth, request_id, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               'queued', 0, ?, ?, ?, ?, ?, ?)""",
                    (
                        task_id,
                        dedupe,
                        inbound_id,
                        channel_value,
                        bot_value,
                        user_value,
                        session_value,
                        task_snapshot.agent_id,
                        conversation_id,
                        thread_id,
                        task_snapshot.mode_id,
                        int(task_snapshot.profile_version),
                        int(task_snapshot.policy_version),
                        task_snapshot.model,
                        task_snapshot.reasoning_effort,
                        json_dumps(reply_target.to_dict()),
                        json_dumps(_snapshot_value(task_snapshot.inputs)),
                        json_dumps(_snapshot_value(task_snapshot.metadata, text_key="value")),
                        None,
                        task_snapshot.parent_task_id,
                        int(task_snapshot.child_depth),
                        task_snapshot.request_id,
                        now,
                        now,
                    ),
                )
                if inbound_id:
                    # Publish the task owner inside this transaction before
                    # validating inbound media refs.  Any validation failure
                    # rolls this provisional link back with the task insert.
                    conn.execute(
                        "UPDATE inbound_messages SET task_id = ?, status = 'task_queued' WHERE message_id = ?",
                        (task_id, inbound_id),
                    )
                self._validate_task_input_attachment_access_tx(
                    conn,
                    task_id=task_id,
                    inbound_message_id=inbound_id,
                    inputs=task_snapshot.inputs,
                    agent_id=task_snapshot.agent_id,
                    channel=channel_value,
                    bot_id=bot_value,
                    external_user_id=user_value,
                    session_id=session_value,
                )
                if child_parent_id is not None:
                    # Keep the counter an exact lower bound for durable child
                    # rows.  If a compatibility caller reserved a slot before
                    # this insert, the pre-existing count already includes
                    # that slot; taking ``max`` avoids double charging it.
                    self._update_child_counter_tx(
                        conn, child_parent_id, now=now
                    )
                self._retain_task_input_attachments_tx(
                    conn,
                    task_id=task_id,
                    inputs=task_snapshot.inputs,
                    created_at=now,
                )
                created = self._fetch_task_tx(conn, task_id)
                if created is None:
                    raise StoreError("failed to create task")
                return prepare_initial_delivery_tx(conn, created)

        requested_scope = (
            channel or mapped_channel or target_route.channel,
            bot_id or mapped_bot_id or target_route.bot_id,
            external_user_id or mapped_user_id or target_route.external_user_id,
            session_id or mapped_session_id or target_route.session_id or "default",
        )
        requested_conversation = task_snapshot.conversation_id or canonical_conversation_id(
            *requested_scope, task_snapshot.agent_id
        )
        try:
            return await self._call(op)
        except sqlite3.IntegrityError as exc:
            # A concurrent writer may win the dedupe race between our lookup
            # and INSERT.  Return that winner whenever possible.
            if effective_dedupe_key:
                existing = await self.get_task_by_dedupe(effective_dedupe_key)
                if existing is not None:
                    self._validate_dedupe_owner(
                        existing,
                        channel=requested_scope[0],
                        bot_id=requested_scope[1],
                        external_user_id=requested_scope[2],
                        session_id=requested_scope[3],
                        agent_id=task_snapshot.agent_id,
                        parent_task_id=task_snapshot.parent_task_id,
                        conversation_id=requested_conversation,
                    )
                    return existing
            if effective_inbound_id:
                existing = await self.get_task_by_inbound(effective_inbound_id)
                if existing is not None:
                    self._validate_dedupe_owner(
                        existing,
                        channel=requested_scope[0],
                        bot_id=requested_scope[1],
                        external_user_id=requested_scope[2],
                        session_id=requested_scope[3],
                        agent_id=task_snapshot.agent_id,
                        parent_task_id=task_snapshot.parent_task_id,
                        conversation_id=requested_conversation,
                    )
                    return existing
            raise StoreError(str(exc)) from exc

    enqueue_task = create_task

    async def create_child_task(
        self,
        task: AgentTask | Mapping[str, Any] | None = None,
        *,
        parent_task_id: str,
        max_children: int,
        **kwargs: Any,
    ) -> TaskRecord:
        """Insert a child task while reserving its parent slot atomically.

        This is deliberately a store operation instead of a manager-level
        ``reserve`` followed by ``create_task``.  Both the counter increment
        and task row are committed (or rolled back) together, so a crash can
        never leave an admission slot without a corresponding durable task.
        """

        kwargs.setdefault("parent_task_id", str(parent_task_id))
        return await self.create_task(
            task,
            _reserve_child_parent_id=str(parent_task_id),
            _reserve_child_max=int(max_children),
            **kwargs,
        )

    async def accept_inbound(
        self,
        inbound: InboundMessage | Mapping[str, Any] | None = None,
        *,
        task: AgentTask | Mapping[str, Any] | None = None,
        create_task: bool = True,
        transcription_candidates: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
        audio_candidates: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
        command_snapshot: Mapping[str, Any] | None = None,
        cursor: str | None = None,
        channel_cursor: str | None = None,
        cursor_channel: str | None = None,
        cursor_bot_id: str | None = None,
        _trusted_media_wire_fingerprints: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> InboundAcceptance:
        """Durably store ingress and create at most one task in one transaction."""

        inbound_keys = {
            "channel", "bot_id", "external_user_id", "external_message_id",
            "text", "content", "body", "session_id", "source_sequence",
            "context_token", "payload", "received_at", "message_id",
        }
        message = self._coerce_inbound(
            inbound,
            {key: value for key, value in kwargs.items() if key in inbound_keys},
        )
        cursor_value = cursor if cursor is not None else channel_cursor
        task_values: dict[str, Any] = {}
        if isinstance(task, Mapping):
            task_values.update(task)
        elif isinstance(task, AgentTask):
            task_values = {name: getattr(task, name) for name in task.__dataclass_fields__}
        # Explicit task fields can be passed alongside the inbound envelope.
        for key in (
            "agent_id", "conversation_id", "thread_id", "mode_id", "profile_version",
            "policy_version", "model", "reasoning_effort", "reply_target", "inputs",
            "request_id", "dedupe_key",
        ):
            if key in kwargs:
                task_values[key] = kwargs[key]
        candidate_input = (
            transcription_candidates
            if transcription_candidates is not None
            else audio_candidates
        )
        if isinstance(candidate_input, Mapping):
            candidate_specs = [dict(candidate_input)]
        else:
            candidate_specs = [dict(item) for item in (candidate_input or ())]
        # A direct store caller may provide only a candidate/command route
        # snapshot instead of the fully materialized task mapping that the
        # TaskManager normally supplies.  Resolve each immutable route field
        # from the same trusted sources, in precedence order, before writing
        # the internal snapshot.  This keeps later media ACL checks and
        # confirmation replay tied to the original mode/policy as well as the
        # original Agent.
        command_values: Mapping[str, Any] = (
            command_snapshot if isinstance(command_snapshot, Mapping) else {}
        )
        candidate_route_values: Mapping[str, Any] = (
            candidate_specs[0] if candidate_specs else {}
        )

        def route_value(name: str, default: Any = None) -> Any:
            for values in (task_values, command_values, candidate_route_values):
                value = values.get(name)
                if value not in (None, ""):
                    return value
            return default

        def route_int(name: str, default: int = 1) -> int:
            value = route_value(name, default)
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

        # Explicit legacy candidate specs still require the compatibility
        # confirmation path. Current voice ingress supplies ordinary task text
        # and leaves this collection empty.
        if candidate_specs:
            create_task = False
        now = self._now()
        # Commands are not tasks, but their route/mode still needs the same
        # immutable replay semantics.  Store the snapshot alongside the
        # inbound payload; the replay canonicalizer deliberately ignores this
        # reserved diagnostic key so a newer in-memory route cannot turn a
        # duplicate into a conflicting envelope.
        inbound_payload = self._sanitize_inbound_payload(
            message.payload,
            trusted_media_wire_fingerprints=(
                _trusted_media_wire_fingerprints
            ),
        )
        # Reserved snapshots are written only from the trusted acceptance
        # arguments below.  A channel payload cannot inject an Agent route or
        # command authorization hint by using an internal-looking key.
        inbound_payload.pop("__command_snapshot", None)
        inbound_payload.pop("__route_snapshot", None)
        if command_snapshot is not None and not create_task:
            inbound_payload["__command_snapshot"] = dict(command_values)
        # Persist the route selected for this immutable ingress identity.  It
        # is an internal authorization hint (excluded from replay comparison
        # and Agent inputs), and lets a pending audio candidate prove which
        # Agent owns an inbound attachment before a task exists.
        route_agent = str(
            route_value(
                "agent_id",
                "codex" if (create_task or candidate_specs) else "",
            )
            or ""
        ).strip()
        if route_agent:
            inbound_payload["__route_snapshot"] = {
                "agent_id": route_agent,
                "mode_id": str(route_value("mode_id", "chat") or "chat"),
                "profile_version": route_int("profile_version"),
                "policy_version": route_int("policy_version"),
            }

        def op(conn: sqlite3.Connection) -> InboundAcceptance:
            with _transaction(conn):
                insert_cursor = conn.execute(
                    """INSERT OR IGNORE INTO inbound_messages
                       (message_id, channel, bot_id, external_user_id,
                        external_message_id, session_id, source_sequence,
                        context_token, text, payload_json, status,
                        received_at, stored_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'stored', ?, ?)""",
                    (
                        message.message_id,
                        message.channel,
                        message.bot_id,
                        message.external_user_id,
                        message.external_message_id,
                        message.session_id,
                        message.source_sequence,
                        message.context_token,
                        message.text,
                        json_dumps(inbound_payload),
                        _utc_text(message.received_at),
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM inbound_messages WHERE channel = ? AND bot_id = ? AND external_message_id = ?",
                    message.dedupe_identity,
                ).fetchone()
                if row is None:
                    raise StoreError("failed to persist inbound message")
                if insert_cursor.rowcount == 0:
                    self._validate_inbound_replay_tx(
                        row,
                        message,
                        trusted_media_wire_fingerprints=(
                            _trusted_media_wire_fingerprints
                        ),
                    )
                if (
                    str(row["external_user_id"] or "")
                    != str(message.external_user_id or "")
                    or str(row["session_id"] or "default")
                    != str(message.session_id or "default")
                ):
                    raise StoreError(
                        "inbound identity conflicts with an existing user/session"
                    )
                # The reply allowance is part of ingress durability.  Create
                # it before routing, validation, command handling, or task
                # publication so every later user response has one allocator.
                self._ensure_reply_scope_for_inbound_row_tx(conn, row)
                persisted = self._inbound_from_row(row)
                duplicate = insert_cursor.rowcount == 0
                existing_task = None
                if persisted.task_id:
                    existing_task = self._fetch_task_tx(conn, persisted.task_id)
                retain_agent_id: str | None = (
                    str(
                        route_value("agent_id", "")
                        or ("codex" if (create_task or candidate_specs) else "")
                    )
                    or None
                )
                if duplicate:
                    # Replays must use the first accepted route when checking
                    # owner-agent metadata.  A later /agent switch is not an
                    # authorization change for the immutable inbound row.
                    stored_payload = json_loads(row["payload_json"], {}) or {}
                    stored_route = (
                        stored_payload.get("__route_snapshot", {})
                        if isinstance(stored_payload, Mapping)
                        else {}
                    )
                    stored_agent = (
                        stored_route.get("agent_id")
                        if isinstance(stored_route, Mapping)
                        else None
                    )
                    if not stored_agent and existing_task is not None:
                        stored_agent = existing_task.agent_id
                    if not stored_agent:
                        candidate_owner = conn.execute(
                            "SELECT agent_id FROM transcription_candidates "
                            "WHERE inbound_message_id=? ORDER BY created_at ASC "
                            "LIMIT 1",
                            (persisted.message_id,),
                        ).fetchone()
                        if candidate_owner is not None:
                            stored_agent = candidate_owner["agent_id"]
                    retain_agent_id = str(stored_agent or "") or None
                self._retain_inbound_attachments_tx(
                    conn,
                    message_id=persisted.message_id,
                    attachments=self._input_attachment_values(
                        json_loads(row["payload_json"], {}) or {}
                    ),
                    channel=str(row["channel"] or ""),
                    bot_id=str(row["bot_id"] or ""),
                    external_user_id=str(row["external_user_id"] or ""),
                    session_id=str(row["session_id"] or "default"),
                    source_message_id=str(row["external_message_id"] or ""),
                    agent_id=retain_agent_id,
                    created_at=row["stored_at"],
                )
                # The channel cursor and inbound ownership move in the same
                # transaction.  A crash can therefore never acknowledge a
                # cursor whose message row was rolled back.
                cursor_scope = (
                    cursor_channel or persisted.channel,
                    cursor_bot_id or persisted.bot_id,
                )
                existing_cursor = conn.execute(
                    "SELECT cursor FROM channel_cursors WHERE channel=? AND bot_id=?",
                    cursor_scope,
                ).fetchone()
                current_cursor = (
                    str(existing_cursor["cursor"])
                    if existing_cursor is not None
                    else None
                )
                if _cursor_should_advance(current_cursor, cursor_value):
                    conn.execute(
                        """INSERT INTO channel_cursors(channel, bot_id, cursor, updated_at)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(channel, bot_id)
                           DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at""",
                        (
                            cursor_scope[0],
                            cursor_scope[1],
                            str(cursor_value),
                            now,
                        ),
                    )
                if duplicate:
                    # The first committed acceptance owns both task meaning
                    # and audio-candidate policy.  A plain ``stored`` row with
                    # no task is the one exception: it may be the durable half
                    # of a store-then-create flow interrupted by a crash, so a
                    # task-creating replay is allowed to repair it below.
                    # Commands are advanced to ``accepted`` and audio ingress
                    # to ``awaiting_confirmation`` on their first acceptance,
                    # preventing either from being reinterpreted as Agent work.
                    prior_candidates = conn.execute(
                        "SELECT confirmation_id FROM transcription_candidates "
                        "WHERE inbound_message_id=? "
                        "ORDER BY created_at ASC, confirmation_id ASC",
                        (persisted.message_id,),
                    ).fetchall()
                    if (
                        not create_task
                        or existing_task is not None
                        or persisted.status != InboundState.STORED
                        or prior_candidates
                    ):
                        return InboundAcceptance(
                            persisted,
                            existing_task,
                            created=False,
                            duplicate=True,
                            confirmation_ids=tuple(
                                str(item["confirmation_id"])
                                for item in prior_candidates
                            ),
                        )
                confirmation_ids: list[str] = []
                for ordinal, spec in enumerate(candidate_specs):
                    candidate_text = str(
                        spec.get(
                            "candidate_text",
                            spec.get("text", spec.get("transcript", "")),
                        )
                        or ""
                    ).strip()
                    if not candidate_text:
                        raise ValueError("candidate_text cannot be empty")
                    confirmation_id = str(
                        spec.get("confirmation_id")
                        or uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            "codex-transcription:"
                            + persisted.message_id
                            + ":"
                            + str(spec.get("ordinal", ordinal)),
                        )
                    )
                    expires_at_supplied = "expires_at" in spec
                    expires_value = spec.get("expires_at")
                    candidate_expiry = (
                        _utc_text(expires_value)
                        if expires_at_supplied and expires_value is not None
                        else (
                            None
                            if expires_at_supplied
                            else self._default_transcription_expiry(now)
                        )
                    )
                    self._insert_transcription_candidate_tx(
                        conn,
                        confirmation_id=confirmation_id,
                        channel=persisted.channel,
                        bot_id=persisted.bot_id,
                        external_user_id=persisted.external_user_id,
                        session_id=persisted.session_id,
                        agent_id=str(
                            spec.get("agent_id")
                            or task_values.get("agent_id")
                            or command_values.get("agent_id")
                            or "codex"
                        ),
                        mode_id=str(
                            spec.get("mode_id")
                            or task_values.get("mode_id")
                            or command_values.get("mode_id")
                            or "chat"
                        ),
                        profile_version=int(
                            spec.get("profile_version")
                            or task_values.get("profile_version")
                            or command_values.get("profile_version")
                            or 1
                        ),
                        policy_version=int(
                            spec.get("policy_version")
                            or task_values.get("policy_version")
                            or command_values.get("policy_version")
                            or 1
                        ),
                        metadata=(
                            spec.get("metadata")
                            if spec.get("metadata") is not None
                            else task_values.get("metadata", {})
                        ),
                        attachment_id=(
                            str(spec["attachment_id"])
                            if spec.get("attachment_id")
                            else None
                        ),
                        inbound_message_id=persisted.message_id,
                        candidate_text=candidate_text,
                        source=str(spec.get("source") or "channel"),
                        confidence=(
                            float(spec["confidence"])
                            if spec.get("confidence") is not None
                            else None
                        ),
                        created_at=now,
                        expires_at=candidate_expiry,
                        expires_at_supplied=expires_at_supplied,
                    )
                    confirmation_ids.append(confirmation_id)
                if not create_task:
                    if not confirmation_ids:
                        self._transition_inbound_tx(
                            conn,
                            persisted.message_id,
                            InboundState.ACCEPTED,
                            now=now,
                        )
                    # Candidate insertion above may have advanced the inbound
                    # state; return the refreshed envelope in the same commit.
                    refreshed_inbound = self._inbound_from_row(
                        conn.execute(
                            "SELECT * FROM inbound_messages WHERE message_id=?",
                            (persisted.message_id,),
                        ).fetchone()
                    )
                    return InboundAcceptance(
                        refreshed_inbound,
                        existing_task,
                        created=not duplicate,
                        duplicate=duplicate,
                        confirmation_ids=tuple(confirmation_ids),
                    )
                # Build a task snapshot from the inbound envelope.  The task
                # carries the original reply target even if the active route
                # changes before execution.
                task_values.setdefault("inbound_message_id", persisted.message_id)
                task_values.setdefault("dedupe_key", f"inbound:{persisted.message_id}")
                task_values.setdefault("channel", persisted.channel)
                task_values.setdefault("bot_id", persisted.bot_id)
                task_values.setdefault("external_user_id", persisted.external_user_id)
                task_values.setdefault("session_id", persisted.session_id)
                if "inputs" not in task_values:
                    fallback_inputs: dict[str, Any] = {"text": persisted.text}
                    for key in ("media", "attachments", "images"):
                        value = persisted.payload.get(key)
                        if value is not None:
                            fallback_inputs[key] = list(
                                canonical_media_inputs(value)
                            )
                    task_values["inputs"] = fallback_inputs
                task_values.setdefault("reply_target", persisted.target())
                snapshot = self._coerce_task(task, task_values)
                # Inline task insertion avoids a nested async call and keeps
                # ingress, task ownership, and inbound status atomic.
                channel_value = persisted.channel
                bot_value = persisted.bot_id
                user_value = persisted.external_user_id
                session_value = persisted.session_id
                conversation_id = self._ensure_conversation_tx(
                    conn,
                    snapshot,
                    channel=channel_value,
                    bot_id=bot_value,
                    external_user_id=user_value,
                    session_id=session_value,
                    now=now,
                )
                thread_id = snapshot.thread_id or self._thread_binding_tx(
                    conn,
                    conversation_id=conversation_id,
                    mode_id=snapshot.mode_id,
                    profile_version=snapshot.profile_version,
                    policy_version=snapshot.policy_version,
                )
                dedupe = snapshot.dedupe_key or f"inbound:{persisted.message_id}"
                existing = self._fetch_task_by_dedupe_tx(conn, dedupe)
                if existing is not None:
                    self._validate_dedupe_owner(
                        existing,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                        agent_id=snapshot.agent_id,
                        parent_task_id=snapshot.parent_task_id,
                        conversation_id=conversation_id,
                    )
                child_parent_id = None
                if existing is None and snapshot.parent_task_id:
                    child_parent_id = self._validate_child_parent_tx(
                        conn,
                        snapshot,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                    )
                if existing is None:
                    task_id = snapshot.task_id or _uuid()
                    target = self._coerce_reply_target(snapshot.reply_target, fallback=persisted.target())
                    self._validate_reply_target_scope(
                        target,
                        channel=persisted.channel,
                        bot_id=persisted.bot_id,
                        external_user_id=persisted.external_user_id,
                        session_id=persisted.session_id,
                    )
                    conn.execute(
                        """INSERT INTO tasks
                           (task_id, dedupe_key, inbound_message_id, channel, bot_id,
                            external_user_id, session_id, agent_id, conversation_id,
                            thread_id, mode_id, profile_version, policy_version,
                            model, reasoning_effort, reply_target_json, inputs_json,
                            metadata_json,
                            state, attempts, next_attempt_at, parent_task_id,
                            child_depth, request_id, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                   'queued', 0, ?, ?, ?, ?, ?, ?)""",
                        (
                            task_id, dedupe, persisted.message_id, channel_value,
                            bot_value, user_value, session_value, snapshot.agent_id,
                            conversation_id, thread_id, snapshot.mode_id,
                            int(snapshot.profile_version), int(snapshot.policy_version),
                            snapshot.model, snapshot.reasoning_effort,
                            json_dumps(target.to_dict()), json_dumps(_snapshot_value(snapshot.inputs)),
                            json_dumps(_snapshot_value(snapshot.metadata, text_key="value")),
                            None, snapshot.parent_task_id, int(snapshot.child_depth),
                            snapshot.request_id, now, now,
                        ),
                    )
                    # Make the inbound ref resolve to this Agent while the
                    # attachment validator runs.  The surrounding transaction
                    # guarantees a denial cannot leak this provisional link.
                    conn.execute(
                        "UPDATE inbound_messages SET task_id = ?, status = 'task_queued' WHERE message_id = ?",
                        (task_id, persisted.message_id),
                    )
                    self._validate_task_input_attachment_access_tx(
                        conn,
                        task_id=task_id,
                        inbound_message_id=persisted.message_id,
                        inputs=snapshot.inputs,
                        agent_id=snapshot.agent_id,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                    )
                    self._retain_task_input_attachments_tx(
                        conn,
                        task_id=task_id,
                        inputs=snapshot.inputs,
                        created_at=now,
                    )
                    if child_parent_id is not None:
                        self._update_child_counter_tx(
                            conn, child_parent_id, now=now
                        )
                    existing = self._fetch_task_tx(conn, task_id)
                    return InboundAcceptance(
                        self._inbound_from_row(
                            conn.execute("SELECT * FROM inbound_messages WHERE message_id = ?", (persisted.message_id,)).fetchone()
                        ),
                        existing,
                        created=True,
                        duplicate=duplicate,
                    )
                conn.execute(
                    "UPDATE inbound_messages SET task_id = ?, status = 'task_queued' WHERE message_id = ?",
                    (existing.task_id, persisted.message_id),
                )
                self._retain_task_input_attachments_tx(
                    conn,
                    task_id=existing.task_id,
                    inputs=existing.inputs,
                    created_at=existing.created_at,
                )
                refreshed = self._inbound_from_row(
                    conn.execute("SELECT * FROM inbound_messages WHERE message_id = ?", (persisted.message_id,)).fetchone()
                )
                return InboundAcceptance(refreshed, existing, created=False, duplicate=duplicate)

        try:
            return await self._call(op)
        except sqlite3.IntegrityError as exc:
            # The entire transaction rolled back.  A concurrent transaction
            # may have already accepted this identity.  Only repair through
            # the replay path when that winner is demonstrably durable.  If
            # the identity is absent, the integrity error came from task or
            # projection insertion and acknowledging a newly stored inbound
            # would lose user work and advance the channel cursor.
            winner = await self.get_inbound(
                message.channel,
                message.bot_id,
                message.external_message_id,
            )
            if winner is None:
                raise StoreError(str(exc)) from exc
            existing = await self.store_inbound(
                message,
                cursor=cursor_value,
                cursor_channel=cursor_channel,
                cursor_bot_id=cursor_bot_id,
            )
            if existing is not None:
                existing_task = await self.get_task_by_inbound(existing.message_id)
                # A durable inbound row is not proof that the requested task
                # projection committed.  In the store-then-create recovery
                # path, an unrelated task constraint (for example a reused
                # task_id) can fail after the existing inbound was found.  Do
                # not acknowledge that failure as a harmless duplicate and
                # strand the message permanently in ``stored``.
                if create_task and existing_task is None:
                    raise StoreError(str(exc)) from exc
                confirmation_ids = await self._call(
                    lambda conn: tuple(
                        str(row["confirmation_id"])
                        for row in conn.execute(
                            "SELECT confirmation_id FROM transcription_candidates "
                            "WHERE inbound_message_id=? "
                            "ORDER BY created_at ASC, confirmation_id ASC",
                            (existing.message_id,),
                        ).fetchall()
                    )
                )
                return InboundAcceptance(
                    existing,
                    existing_task,
                    created=False,
                    duplicate=True,
                    confirmation_ids=confirmation_ids,
                )
            raise StoreError(str(exc)) from exc

    # Compatibility names for gateways and tests.
    ingest_inbound = accept_inbound
    accept_message = accept_inbound

    async def get_inbound(
        self,
        channel_or_message_id: str,
        bot_id: str | None = None,
        external_message_id: str | None = None,
    ) -> InboundMessage | None:
        """Look up ingress by internal id or durable channel identity."""

        def op(conn: sqlite3.Connection) -> InboundMessage | None:
            if bot_id is None and external_message_id is None:
                row = conn.execute(
                    "SELECT * FROM inbound_messages WHERE message_id = ?",
                    (channel_or_message_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM inbound_messages WHERE channel = ? AND bot_id = ? AND external_message_id = ?",
                    (channel_or_message_id, bot_id or "", external_message_id or ""),
                ).fetchone()
            return self._inbound_from_row(row) if row is not None else None

        return await self._call(op)

    get_inbound_message = get_inbound

    async def get_reply_scope(
        self, reply_scope_id: str
    ) -> ReplyScopeRecord | None:
        """Return one immutable inbound reply allowance by its stable ID."""

        return await self._call(
            lambda conn: self._reply_scope_from_row(
                conn.execute(
                    "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                    (str(reply_scope_id),),
                ).fetchone()
            )
        )

    async def get_reply_scope_for_inbound(
        self, inbound_message_id: str
    ) -> ReplyScopeRecord | None:
        """Return the sole reply scope owned by a stored inbound row."""

        return await self._call(
            lambda conn: self._reply_scope_from_row(
                conn.execute(
                    "SELECT * FROM reply_scopes WHERE inbound_message_id=?",
                    (str(inbound_message_id),),
                ).fetchone()
            )
        )

    async def get_reply_scope_for_target(
        self, target: ReplyTarget | Mapping[str, Any]
    ) -> ReplyScopeRecord | None:
        """Resolve an exact persisted target without using its context token."""

        reply_target = self._coerce_reply_target(target)

        def op(conn: sqlite3.Connection) -> ReplyScopeRecord | None:
            return self._reply_scope_from_row(
                self._reply_scope_for_target_tx(conn, reply_target)
            )

        return await self._call(op)

    async def set_inbound_status(
        self,
        message_id: str,
        status: InboundState | str,
        *,
        task_id: str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        """Apply an explicit durable inbound state transition."""
        try:
            target = InboundState(str(_enum_value(status)))
        except ValueError as exc:
            raise InvalidTransition(f"unknown inbound state: {status!r}") from exc
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                return self._transition_inbound_tx(
                    conn,
                    message_id,
                    target,
                    now=now_text,
                    task_id=task_id,
                )

        return await self._call(op)

    update_inbound_status = set_inbound_status

    async def list_inbound(self, *, limit: int = 100) -> list[InboundMessage]:
        def op(conn: sqlite3.Connection) -> list[InboundMessage]:
            rows = conn.execute(
                "SELECT * FROM inbound_messages ORDER BY stored_at DESC, message_id DESC LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
            return [self._inbound_from_row(row) for row in rows]

        return await self._call(op)

    async def get_task(self, task_id: str) -> TaskRecord | None:
        return await self._call(lambda conn: self._fetch_task_tx(conn, task_id))

    async def require_task(self, task_id: str) -> TaskRecord:
        task = await self.get_task(task_id)
        if task is None:
            raise NotFoundError(f"task not found: {task_id}")
        return task

    async def get_execution(self, execution_id: str) -> TaskExecution | None:
        """Load one concrete execution attempt by its durable ID."""

        return await self._call(
            lambda conn: self._execution_from_row(
                conn.execute(
                    "SELECT * FROM task_executions WHERE execution_id=?",
                    (execution_id,),
                ).fetchone()
            )
        )

    get_task_execution = get_execution

    async def list_task_executions(
        self,
        task_id: str,
        *,
        limit: int = 100,
        newest_first: bool = False,
    ) -> list[TaskExecution]:
        """List all attempts for a task, preserving execution history."""

        order = "DESC" if newest_first else "ASC"

        def op(conn: sqlite3.Connection) -> list[TaskExecution]:
            rows = conn.execute(
                "SELECT * FROM task_executions WHERE task_id=? "
                f"ORDER BY attempt {order}, execution_id {order} LIMIT ?",
                (task_id, max(0, int(limit))),
            ).fetchall()
            return [
                execution
                for row in rows
                if (execution := self._execution_from_row(row)) is not None
            ]

        return await self._call(op)

    get_task_executions = list_task_executions
    list_executions = list_task_executions

    async def get_task_by_dedupe(self, dedupe_key: str) -> TaskRecord | None:
        return await self._call(lambda conn: self._fetch_task_by_dedupe_tx(conn, dedupe_key))

    async def get_task_by_inbound(self, inbound_message_id: str) -> TaskRecord | None:
        def op(conn: sqlite3.Connection) -> TaskRecord | None:
            row = conn.execute(
                f"{self._task_select_sql()} WHERE t.inbound_message_id = ?",
                (inbound_message_id,),
            ).fetchone()
            return self._task_from_row(row)

        return await self._call(op)

    async def list_tasks(
        self,
        *,
        states: Iterable[TaskState | str] | None = None,
        state: TaskState | str | None = None,
        agent_id: str | None = None,
        conversation_id: str | None = None,
        external_user_id: str | None = None,
        channel: str | None = None,
        bot_id: str | None = None,
        session_id: str | None = None,
        limit: int = 100,
        newest_first: bool = True,
    ) -> list[TaskRecord]:
        filters: list[str] = []
        params: list[Any] = []
        if state is not None:
            states = [state]
        if states is not None:
            state_values = [_enum_value(value) for value in states]
            if not state_values:
                return []
            filters.append("t.state IN (" + ",".join("?" for _ in state_values) + ")")
            params.extend(state_values)
        for column, value in (
            ("t.agent_id", agent_id),
            ("t.conversation_id", conversation_id),
            ("t.external_user_id", external_user_id),
            ("t.channel", channel),
            ("t.bot_id", bot_id),
            ("t.session_id", session_id),
        ):
            if value is not None:
                filters.append(f"{column} = ?")
                params.append(value)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        order = "DESC" if newest_first else "ASC"
        params.append(max(0, int(limit)))

        def op(conn: sqlite3.Connection) -> list[TaskRecord]:
            rows = conn.execute(
                f"{self._task_select_sql()}{where} ORDER BY t.created_at {order}, t.task_id {order} LIMIT ?",
                params,
            ).fetchall()
            return [record for row in rows if (record := self._task_from_row(row)) is not None]

        return await self._call(op)

    async def count_queued_tasks(self) -> int:
        return int(
            await self._call(
                lambda conn: conn.execute("SELECT COUNT(*) FROM tasks WHERE state = 'queued'").fetchone()[0]
            )
        )

    pending_task_count = count_queued_tasks

    # ------------------------------------------------------------------
    # Task claims and state transitions
    # ------------------------------------------------------------------
    @staticmethod
    def _lease_deadline(now: str, lease_seconds: float) -> str:
        parsed = text_to_datetime(now) or utcnow()
        return _utc_text(parsed + timedelta(seconds=max(0.001, float(lease_seconds))))

    @staticmethod
    def _lease_is_active(expires_at: datetime | str | None, now: datetime | str) -> bool:
        """Return whether a durable claim still owns its mutation window."""

        expiry = text_to_datetime(expires_at)
        current = text_to_datetime(now)
        return expiry is not None and current is not None and expiry > current

    @classmethod
    def _claim_selected_task_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        worker_id: str,
        now: str,
        lease_expires_at: str,
    ) -> TaskClaim | None:
        task_id = row["task_id"]
        token = _uuid()
        execution_id = _uuid()
        attempt = int(row["attempts"]) + 1
        scope_row = conn.execute(
            """SELECT COALESCE(
                       t.pending_delivery_reply_scope_id,
                       rs.reply_scope_id
                   ) AS reply_scope_id
               FROM tasks t
               LEFT JOIN reply_scopes rs
                 ON rs.inbound_message_id=t.inbound_message_id
               WHERE t.task_id=?""",
            (task_id,),
        ).fetchone()
        delivery_reply_scope_id = (
            scope_row["reply_scope_id"] if scope_row is not None else None
        )
        changed = conn.execute(
            """UPDATE tasks
               SET state = 'claimed', claimed_by = ?, claim_token = ?,
                   lease_expires_at = ?, attempts = ?, updated_at = ?,
                   last_error = NULL, pending_delivery_reply_scope_id = NULL
               WHERE task_id = ? AND state = 'queued'
                 AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                 AND NOT EXISTS (
                     SELECT 1
                     FROM attachment_refs AS ar
                     LEFT JOIN attachments AS a
                       ON a.attachment_id = ar.attachment_id
                     WHERE ar.owner_kind = 'task'
                       AND ar.owner_id = tasks.task_id
                       AND (a.attachment_id IS NULL OR a.state <> 'ready')
                 )""",
            (worker_id, token, lease_expires_at, attempt, now, task_id, now),
        ).rowcount
        if changed != 1:
            return None
        conn.execute(
            """INSERT INTO task_executions
               (execution_id, task_id, attempt, state, worker_id, claim_token,
                lease_expires_at, delivery_reply_scope_id, created_at)
               VALUES (?, ?, ?, 'claimed', ?, ?, ?, ?, ?)""",
            (
                execution_id,
                task_id,
                attempt,
                worker_id,
                token,
                lease_expires_at,
                delivery_reply_scope_id,
                now,
            ),
        )
        task = cls._fetch_task_tx(conn, task_id)
        execution = cls._execution_from_row(
            conn.execute("SELECT * FROM task_executions WHERE execution_id = ?", (execution_id,)).fetchone()
        )
        if task is None or execution is None:
            raise StoreError("claim transaction did not produce task and execution")
        return TaskClaim(task=task, execution=execution, claim_token=token)

    async def claim_next_task(
        self,
        worker_id: str,
        *,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
        agent_id: str | None = None,
        enforce_serialization: bool = True,
    ) -> TaskClaim | None:
        """Atomically claim the oldest runnable task.

        The default serialization key is channel/bot/user/session/Agent.  A
        task is skipped while another task for that key is claimed, running,
        or waiting for an interrupt to finish.
        """

        def op(conn: sqlite3.Connection) -> TaskClaim | None:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                filters = ["t.state = 'queued'", "(t.next_attempt_at IS NULL OR t.next_attempt_at <= ?)"]
                params: list[Any] = [now_text]
                if agent_id is not None:
                    filters.append("t.agent_id = ?")
                    params.append(agent_id)
                if enforce_serialization:
                    filters.append(
                        """NOT EXISTS (
                            SELECT 1 FROM tasks active
                            WHERE active.task_id <> t.task_id
                              AND active.channel = t.channel
                              AND active.bot_id = t.bot_id
                              AND active.external_user_id = t.external_user_id
                              AND active.session_id = t.session_id
                              AND active.agent_id = t.agent_id
                              AND active.state IN ('claimed', 'running', 'cancel_requested')
                        )"""
                    )
                filters.append(
                    """NOT EXISTS (
                        SELECT 1
                        FROM attachment_refs AS ar
                        LEFT JOIN attachments AS a
                          ON a.attachment_id = ar.attachment_id
                        WHERE ar.owner_kind = 'task'
                          AND ar.owner_id = t.task_id
                          AND (a.attachment_id IS NULL OR a.state <> 'ready')
                    )"""
                )
                row = conn.execute(
                    "SELECT t.* FROM tasks t WHERE " + " AND ".join(filters) +
                    " ORDER BY t.created_at ASC, t.task_id ASC LIMIT 1",
                    params,
                ).fetchone()
                if row is None:
                    return None
                return self._claim_selected_task_tx(
                    conn, row, worker_id=worker_id, now=now_text, lease_expires_at=lease
                )

        return await self._call(op)

    # Dispatcher terminology aliases.  ``claim_task`` historically had two
    # call shapes in downstream integrations: ``claim_task(worker_id)`` for
    # the next row and ``claim_task(task_id, worker_id)`` for an explicit row.
    # Keep both without weakening the atomic SQL paths above.
    async def claim_task(
        self,
        task_or_worker_id: str,
        worker_id: str | None = None,
        **kwargs: Any,
    ) -> TaskClaim | None:
        if worker_id is None:
            return await self.claim_next_task(task_or_worker_id, **kwargs)
        return await self.claim_task_by_id(task_or_worker_id, worker_id, **kwargs)

    claim_queued_task = claim_next_task

    async def claim_task_by_id(
        self,
        task_id: str,
        worker_id: str,
        *,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
        enforce_serialization: bool = True,
    ) -> TaskClaim | None:
        """Atomically claim a specific queued task.

        Keep the explicit-ID path behaviorally symmetric with
        :meth:`claim_next_task`: callers that intentionally coordinate
        serialization themselves can opt out of the per-conversation active
        task guard, while the default remains serialized.  The conditional
        predicate is part of the same ``BEGIN IMMEDIATE`` transaction as the
        claim update, so opting out cannot weaken ownership/lease checks.
        """
        def op(conn: sqlite3.Connection) -> TaskClaim | None:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                filters = [
                    "t.task_id = ?",
                    "t.state = 'queued'",
                    "(t.next_attempt_at IS NULL OR t.next_attempt_at <= ?)",
                ]
                params: list[Any] = [task_id, now_text]
                if enforce_serialization:
                    filters.append(
                        """NOT EXISTS (
                            SELECT 1 FROM tasks active
                            WHERE active.task_id <> t.task_id
                              AND active.channel = t.channel
                              AND active.bot_id = t.bot_id
                              AND active.external_user_id = t.external_user_id
                              AND active.session_id = t.session_id
                              AND active.agent_id = t.agent_id
                              AND active.state IN ('claimed', 'running', 'cancel_requested')
                        )"""
                    )
                filters.append(
                    """NOT EXISTS (
                        SELECT 1
                        FROM attachment_refs AS ar
                        LEFT JOIN attachments AS a
                          ON a.attachment_id = ar.attachment_id
                        WHERE ar.owner_kind = 'task'
                          AND ar.owner_id = t.task_id
                          AND (a.attachment_id IS NULL OR a.state <> 'ready')
                    )"""
                )
                row = conn.execute(
                    "SELECT * FROM tasks t WHERE " + " AND ".join(filters),
                    params,
                ).fetchone()
                if row is None:
                    return None
                return self._claim_selected_task_tx(
                    conn, row, worker_id=worker_id, now=now_text, lease_expires_at=lease
                )

        return await self._call(op)

    async def renew_task_lease(
        self,
        task_id: str,
        claim_token: str,
        *,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                changed = conn.execute(
                    """UPDATE tasks SET lease_expires_at = ?, updated_at = ?
                       WHERE task_id = ? AND claim_token = ?
                         AND state IN ('claimed', 'running', 'cancel_requested')
                         AND lease_expires_at IS NOT NULL
                         AND lease_expires_at > ?""",
                    (lease, now_text, task_id, claim_token, now_text),
                ).rowcount
                if changed:
                    execution_changed = conn.execute(
                        """UPDATE task_executions SET lease_expires_at = ?
                           WHERE task_id = ? AND claim_token = ? AND finished_at IS NULL
                             AND lease_expires_at IS NOT NULL
                             AND lease_expires_at > ?""",
                        (lease, task_id, claim_token, now_text),
                    ).rowcount
                    if execution_changed != 1:
                        raise StoreError("active task execution lease could not be renewed")
                return changed == 1

        return await self._call(op)

    extend_task_lease = renew_task_lease

    async def mark_task_running(
        self,
        task_id: str,
        claim_token: str,
        *,
        execution_id: str | None = None,
        external_turn_id: str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                changed = conn.execute(
                    """UPDATE tasks SET state = 'running', updated_at = ?
                       WHERE task_id = ? AND claim_token = ? AND state = 'claimed'
                         AND lease_expires_at IS NOT NULL
                         AND lease_expires_at > ?""",
                    (now_text, task_id, claim_token, now_text),
                ).rowcount
                if changed:
                    execution_changed = conn.execute(
                        """UPDATE task_executions
                           SET state = 'running', started_at = COALESCE(started_at, ?),
                               external_turn_id = COALESCE(?, external_turn_id)
                           WHERE task_id = ? AND claim_token = ? AND state = 'claimed'
                             AND lease_expires_at IS NOT NULL
                             AND lease_expires_at > ?
                             AND (? IS NULL OR execution_id = ?)""",
                        (
                            now_text,
                            external_turn_id,
                            task_id,
                            claim_token,
                            now_text,
                            execution_id,
                            execution_id,
                        ),
                    ).rowcount
                    if execution_changed != 1:
                        raise StoreError("task execution could not be marked running")
                return changed == 1

        return await self._call(op)

    start_task = mark_task_running

    async def set_task_thread(
        self,
        task_id: str,
        *,
        thread_id: str,
        claim_token: str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        """Persist the SDK thread for the task's exact policy binding."""
        if not thread_id:
            return False
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                row = conn.execute(
                    "SELECT conversation_id, mode_id, profile_version, policy_version, "
                    "thread_id, claim_token, state, lease_expires_at "
                    "FROM tasks WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                if row is None:
                    return False
                # Updating a thread binding while an execution is active is a
                # worker-owned mutation.  Require the lease fence rather than
                # allowing a stale callback (or an arbitrary task-id caller)
                # to replace the SDK thread.  Queued tasks may still be
                # prepared by administrative code before their first claim,
                # but that prebinding is write-once.  Terminal/recovery rows
                # are immutable: a late callback must never replace the
                # thread snapshot that a future retry will resume.
                active_states = {"claimed", "running", "cancel_requested"}
                terminal_states = {
                    "completed",
                    "failed",
                    "interrupted",
                    "cancelled",
                    "orphaned",
                }
                if row["state"] in terminal_states:
                    return False
                if row["state"] in active_states and (
                    not claim_token or row["claim_token"] != claim_token
                ):
                    return False
                if row["state"] in active_states and not self._lease_is_active(
                    row["lease_expires_at"], now_text
                ):
                    return False
                if row["state"] in active_states:
                    execution = conn.execute(
                        "SELECT claim_token, lease_expires_at, finished_at "
                        "FROM task_executions WHERE task_id=? "
                        "ORDER BY attempt DESC LIMIT 1",
                        (task_id,),
                    ).fetchone()
                    if (
                        execution is None
                        or execution["finished_at"] is not None
                        or execution["claim_token"] != claim_token
                        or not self._lease_is_active(
                            execution["lease_expires_at"], now_text
                        )
                    ):
                        return False
                if claim_token is not None and row["claim_token"] != claim_token:
                    return False
                if row["state"] == "queued":
                    existing_thread = row["thread_id"]
                    if existing_thread and str(existing_thread) != str(thread_id):
                        return False
                changed = conn.execute(
                    "UPDATE tasks SET thread_id=?, updated_at=? WHERE task_id=?",
                    (str(thread_id), now_text, task_id),
                ).rowcount
                conn.execute(
                    """INSERT INTO thread_bindings
                       (conversation_id, mode_id, profile_version, policy_version,
                        thread_id, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(conversation_id, mode_id, profile_version, policy_version)
                       DO UPDATE SET thread_id=excluded.thread_id, updated_at=excluded.updated_at""",
                    (
                        row["conversation_id"], row["mode_id"],
                        int(row["profile_version"]), int(row["policy_version"]),
                        str(thread_id), now_text,
                    ),
                )
                return changed == 1

        return await self._call(op)

    set_thread_binding = set_task_thread

    async def get_thread_binding(
        self,
        conversation_id: str,
        *,
        mode_id: str,
        profile_version: int = 1,
        policy_version: int = 1,
    ) -> str | None:
        def op(conn: sqlite3.Connection) -> str | None:
            return self._thread_binding_tx(
                conn,
                conversation_id=conversation_id,
                mode_id=mode_id,
                profile_version=profile_version,
                policy_version=policy_version,
            )

        return await self._call(op)

    thread_binding = get_thread_binding

    async def clear_thread_bindings(self, conversation_id: str) -> int:
        """Forget future thread bindings for one durable conversation.

        A clear/reset command starts a fresh Codex conversation.  Historical
        task rows retain their immutable thread snapshots; only the lookup
        used by subsequently-created tasks is removed.  Returning the number
        of deleted bindings makes the operation easy to audit while keeping
        the method idempotent for command redelivery.
        """

        conversation_id = str(conversation_id or "")
        if not conversation_id:
            return 0

        def op(conn: sqlite3.Connection) -> int:
            with _transaction(conn):
                result = conn.execute(
                    "DELETE FROM thread_bindings WHERE conversation_id = ?",
                    (conversation_id,),
                )
                return int(result.rowcount)

        return await self._call(op)

    clear_conversation_threads = clear_thread_bindings

    _ALLOWED_TRANSITIONS: Mapping[TaskState, frozenset[TaskState]] = {
        TaskState.QUEUED: frozenset({TaskState.CLAIMED, TaskState.CANCELLED}),
        TaskState.CLAIMED: frozenset({TaskState.RUNNING, TaskState.ORPHANED, TaskState.CANCEL_REQUESTED}),
        TaskState.RUNNING: frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCEL_REQUESTED, TaskState.ORPHANED}),
        TaskState.CANCEL_REQUESTED: frozenset({TaskState.INTERRUPTED, TaskState.CANCELLED, TaskState.ORPHANED}),
        TaskState.ORPHANED: frozenset({TaskState.QUEUED, TaskState.FAILED, TaskState.CANCELLED}),
        TaskState.FAILED: frozenset({TaskState.QUEUED}),
        TaskState.COMPLETED: frozenset(),
        # An interrupted attempt is terminal, but the user may explicitly
        # retry it as a new execution just like a failed/orphaned task.
        TaskState.INTERRUPTED: frozenset({TaskState.QUEUED}),
        TaskState.CANCELLED: frozenset(),
    }

    async def transition_task(
        self,
        task_id: str,
        to_state: TaskState | str,
        *,
        from_states: Iterable[TaskState | str] | TaskState | str | None = None,
        claim_token: str | None = None,
        execution_id: str | None = None,
        last_error: str | None = None,
        result: Any = None,
        now: datetime | str | None = None,
    ) -> bool:
        """Conditionally transition a task and return whether it changed."""

        target = TaskState(_enum_value(to_state))
        if from_states is None:
            sources = [state for state, allowed in self._ALLOWED_TRANSITIONS.items() if target in allowed]
        elif isinstance(from_states, (str, TaskState)):
            sources = [TaskState(_enum_value(from_states))]
        else:
            sources = [TaskState(_enum_value(value)) for value in from_states]
        if any(target not in self._ALLOWED_TRANSITIONS[source] for source in sources):
            raise InvalidTransition(f"invalid task transition {sources!r} -> {target.value}")
        terminal = target in {TaskState.COMPLETED, TaskState.FAILED, TaskState.INTERRUPTED, TaskState.CANCELLED}
        release_claim = terminal or target == TaskState.ORPHANED
        execution_state_for = {
            TaskState.CLAIMED: ExecutionState.CLAIMED,
            TaskState.RUNNING: ExecutionState.RUNNING,
            TaskState.COMPLETED: ExecutionState.COMPLETED,
            TaskState.FAILED: ExecutionState.FAILED,
            TaskState.INTERRUPTED: ExecutionState.INTERRUPTED,
            TaskState.CANCELLED: ExecutionState.CANCELLED,
            TaskState.ORPHANED: ExecutionState.ORPHANED,
        }.get(target)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                current = self._fetch_task_tx(conn, task_id)
                if current is None or current.state not in sources:
                    return False
                # Active state transitions are execution-owned.  Keep the
                # boolean/conditional API for callers that probe a transition,
                # but fail closed when the token is omitted or stale.  Recovery
                # and explicit retry transitions from queued/orphaned/failed
                # rows remain administrative operations and do not require a
                # lease token.
                active_states = {
                    TaskState.CLAIMED,
                    TaskState.RUNNING,
                    TaskState.CANCEL_REQUESTED,
                }
                if current.state in active_states and (
                    not claim_token or current.claim_token != claim_token
                ):
                    return False
                if claim_token is not None and current.claim_token != claim_token:
                    return False
                if current.state in active_states and not self._lease_is_active(
                    current.lease_expires_at, now_text
                ):
                    return False
                # A task row is only a projection of the latest execution
                # attempt.  Active transitions must fence against that exact
                # attempt before appending any event; otherwise a corrupted
                # or stale task can be terminalized without finishing its
                # owner (or can finish several unfinished attempts at once).
                active_execution_id: str | None = None
                active_execution_row: sqlite3.Row | None = None
                if current.state in active_states:
                    active_execution_id = str(execution_id or current.execution_id or "")
                    if not active_execution_id:
                        raise StoreError("active task has no execution record")
                    if current.execution_id and str(current.execution_id) != active_execution_id:
                        raise StoreError(
                            "execution is not the task's active execution"
                        )
                    active_execution_row = conn.execute(
                        "SELECT execution_id, task_id, claim_token, lease_expires_at, finished_at "
                        "FROM task_executions WHERE execution_id = ?",
                        (active_execution_id,),
                    ).fetchone()
                    if active_execution_row is None:
                        raise StoreError(f"execution not found: {active_execution_id}")
                    if str(active_execution_row["task_id"]) != str(task_id):
                        raise StoreError("execution does not belong to its task")
                    if active_execution_row["claim_token"] != claim_token:
                        raise StoreError("execution claim token does not match")
                    if active_execution_row["finished_at"] is not None:
                        raise InvalidTransition("execution is already terminal")
                    if not self._lease_is_active(
                        active_execution_row["lease_expires_at"], now_text
                    ):
                        return False
                    # The update below is intentionally fenced by execution
                    # ID as well as token.  Count every unfinished row now so
                    # a corrupt duplicate with either the same or a stale
                    # token cannot survive terminalization unnoticed.
                    active_count = conn.execute(
                        "SELECT COUNT(*) FROM task_executions "
                        "WHERE task_id = ? AND finished_at IS NULL",
                        (task_id,),
                    ).fetchone()[0]
                    if int(active_count) != 1:
                        raise StoreError(
                            "task has an ambiguous active execution"
                        )
                elif terminal:
                    # queued/orphaned administrative transitions do not own an
                    # execution.  Refuse to commit over a dangling unfinished
                    # attempt rather than accidentally finalizing it later.
                    unfinished = conn.execute(
                        "SELECT COUNT(*) FROM task_executions "
                        "WHERE task_id = ? AND finished_at IS NULL",
                        (task_id,),
                    ).fetchone()[0]
                    if int(unfinished) != 0:
                        raise StoreError(
                            "administrative transition has unfinished execution"
                        )
                    if execution_id is not None:
                        requested = conn.execute(
                            "SELECT task_id, finished_at FROM task_executions "
                            "WHERE execution_id = ?",
                            (str(execution_id),),
                        ).fetchone()
                        if requested is None:
                            raise StoreError(f"execution not found: {execution_id}")
                        if str(requested["task_id"]) != str(task_id):
                            raise StoreError("execution does not belong to its task")
                        if current.execution_id and str(current.execution_id) != str(execution_id):
                            raise StoreError(
                                "execution is not the task's active execution"
                            )
                # Terminal transitions carry one durable audit/final event.
                # Append it before the state update in this same transaction;
                # projection code receives the pre-transition task snapshot,
                # which contains the immutable reply target.
                if terminal:
                    completed_output = (
                        str(result.get("output", result.get("content", "")))
                        if isinstance(result, Mapping)
                        else str(result or "")
                    )
                    user_visible = target == TaskState.COMPLETED and bool(completed_output)
                    terminal_event = self._append_event_tx(
                        conn,
                        task_id,
                        event_type="terminal",
                        visibility=EventVisibility.USER if user_visible else EventVisibility.INTERNAL,
                        priority=EventPriority.NORMAL if user_visible else EventPriority.SILENT,
                        content=completed_output or last_error or target.value,
                        execution_id=active_execution_id or current.execution_id,
                        created_at=now_text,
                    )
                    self._project_event_tx(
                        conn,
                        terminal_event,
                        task=current,
                        allow_compatibility_final=user_visible,
                    )
                placeholders = ",".join("?" for _ in sources)
                filters = ["task_id = ?", f"state IN ({placeholders})"]
                params: list[Any] = [task_id, *[state.value for state in sources]]
                if claim_token is not None:
                    filters.append("claim_token = ?")
                    params.append(claim_token)
                if current.state in active_states:
                    filters.extend(
                        ["lease_expires_at IS NOT NULL", "lease_expires_at > ?"]
                    )
                    params.append(now_text)
                changed = conn.execute(
                    f"""UPDATE tasks SET state = ?, updated_at = ?, last_error = ?,
                          result_json = ?, terminal_at = ?,
                          claimed_by = CASE WHEN ? THEN NULL ELSE claimed_by END,
                          claim_token = CASE WHEN ? THEN NULL ELSE claim_token END,
                          lease_expires_at = CASE WHEN ? THEN NULL ELSE lease_expires_at END
                          WHERE {' AND '.join(filters)}""",
                    (
                        target.value, now_text, last_error,
                        json_dumps(result) if result is not None else None,
                        now_text if terminal else None,
                        int(release_claim), int(release_claim), int(release_claim), *params,
                    ),
                ).rowcount
                if changed != 1:
                    # A terminal event/projection was appended above.  Never
                    # commit it when the ownership-fenced state update loses
                    # its row (for example, a stale callback or a trigger).
                    if terminal:
                        raise InvalidTransition(f"cannot transition task {task_id}")
                    return False
                if execution_state_for is not None and active_execution_id is not None:
                    # Active execution validation above guarantees that this
                    # exact row is the sole owner.  Keep the same fence on the
                    # write and require exactly one affected row so a partial
                    # task/execution transition always rolls back atomically.
                    execution_changed = conn.execute(
                        "UPDATE task_executions SET state=?, "
                        "finished_at=CASE WHEN ? THEN ? ELSE finished_at END, "
                        "lease_expires_at=CASE WHEN ? THEN NULL ELSE lease_expires_at END, "
                        "last_error=COALESCE(?, last_error) "
                        "WHERE execution_id = ? AND task_id = ? "
                        "AND finished_at IS NULL AND claim_token = ? "
                        "AND lease_expires_at IS NOT NULL "
                        "AND lease_expires_at > ?",
                        (
                            execution_state_for.value,
                            int(release_claim),
                            now_text,
                            int(release_claim),
                            last_error,
                            active_execution_id,
                            task_id,
                            claim_token,
                            now_text,
                        ),
                    ).rowcount
                    if execution_changed != 1:
                        raise InvalidTransition(
                            "active execution could not be finalized"
                        )
                return True

        return await self._call(op)

    async def request_cancel(self, task_id: str, *, now: datetime | str | None = None) -> bool:
        """Request interruption without losing ownership of an active task."""

        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute("SELECT state FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
                if row is None:
                    return False
                state = TaskState(row["state"])
                if state == TaskState.QUEUED:
                    changed = conn.execute(
                        """UPDATE tasks SET state = 'cancelled', updated_at = ?,
                           terminal_at = ?, cancel_requested_at = ? WHERE task_id = ? AND state = 'queued'""",
                        (now_text, now_text, now_text, task_id),
                    ).rowcount == 1
                    if changed:
                        # Keep cancellation auditable even when no worker ever
                        # claimed the queued task.  This is part of the same
                        # transaction as the terminal state transition.
                        self._append_event_tx(
                            conn,
                            task_id,
                            event_type="cancelled",
                            visibility=EventVisibility.INTERNAL,
                            priority=EventPriority.SILENT,
                            content="cancel requested",
                            created_at=now_text,
                        )
                    return changed
                if state in {TaskState.CLAIMED, TaskState.RUNNING}:
                    return conn.execute(
                        """UPDATE tasks SET state = 'cancel_requested', updated_at = ?,
                           cancel_requested_at = ? WHERE task_id = ? AND state IN ('claimed', 'running')""",
                        (now_text, now_text, task_id),
                    ).rowcount == 1
                return state == TaskState.CANCEL_REQUESTED

        return await self._call(op)

    request_interrupt = request_cancel

    # ------------------------------------------------------------------
    # Events, messages, and terminal task projections
    # ------------------------------------------------------------------
    @staticmethod
    def _mailbox_from_row(row: sqlite3.Row | None) -> AgentMailboxItem | None:
        if row is None:
            return None
        snapshot = {}
        if "execution_snapshot_json" in row.keys():
            raw_snapshot = json_loads(row["execution_snapshot_json"], {})
            if isinstance(raw_snapshot, Mapping):
                snapshot = dict(raw_snapshot)
        return AgentMailboxItem(
            mailbox_id=row["mailbox_id"],
            message_id=row["message_id"],
            request_id=row["request_id"],
            source_agent_id=row["source_agent_id"],
            destination_agent_id=row["destination_agent_id"],
            content=row["content"],
            state=MailboxState(row["state"]),
            claim_token=row["claim_token"],
            claimed_by=row["claimed_by"],
            lease_expires_at=text_to_datetime(row["lease_expires_at"]),
            attempts=int(row["attempts"]),
            created_at=text_to_datetime(row["created_at"]),
            last_error=row["last_error"],
            reply_to_id=row["reply_to_id"],
            causation_id=row["causation_id"],
            task_id=row["task_id"],
            payload=json_loads(row["payload_json"], {}) or {},
            execution_snapshot=snapshot,
        )

    @classmethod
    def _mailbox_execution_snapshot_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        destination_agent_id: str,
        request_id: str,
        task_scope: Any = None,
        supplied: Mapping[str, Any] | None = None,
        reply_target: Any = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> dict[str, Any]:
        """Build the immutable runtime context for one mailbox projection.

        Mailbox rows are not ordinary user tasks, but a Codex fallback still
        needs the same identity/policy tuple as an ``AgentTask``.  Resolve it
        while the envelope transaction is open.  A caller-provided snapshot
        (normally produced by :class:`TaskManager`) is authoritative; the
        durable Profile/Mode tables fill compatibility gaps for low-level
        store callers and event projections.
        """

        destination = str(destination_agent_id or "").strip()
        if not destination:
            raise StoreError("mailbox destination Agent is required")

        def value(source: Any, name: str, default: Any = None) -> Any:
            if source is None:
                return default
            if isinstance(source, Mapping):
                return source.get(name, default)
            try:
                return source[name]
            except (KeyError, TypeError, IndexError):
                return getattr(source, name, default)

        owner_channel = str(value(task_scope, "channel", channel) or channel or "")
        owner_bot = str(value(task_scope, "bot_id", bot_id) or bot_id or "")
        owner_user = str(
            value(task_scope, "external_user_id", external_user_id)
            or external_user_id
            or ""
        )
        owner_session = str(
            value(task_scope, "session_id", session_id) or session_id or "default"
        ) or "default"
        raw_owner_target = value(task_scope, "reply_target_json", None)
        if isinstance(raw_owner_target, str):
            raw_owner_target = json_loads(raw_owner_target, {})
        owner_target = cls._coerce_reply_target(
            raw_owner_target,
            fallback=ReplyTarget(
                channel=owner_channel,
                bot_id=owner_bot,
                external_user_id=owner_user,
                session_id=owner_session,
            ),
        )
        supplied_map = dict(supplied or {})
        if (
            not supplied_map
            and str(value(task_scope, "agent_id", "") or "") == destination
            and value(task_scope, "conversation_id", None)
        ):
            raw_metadata = value(task_scope, "metadata_json", {})
            if isinstance(raw_metadata, str):
                raw_metadata = json_loads(raw_metadata, {})
            supplied_map = {
                "agent_id": destination,
                "conversation_id": str(value(task_scope, "conversation_id", "")),
                "reply_target": owner_target.to_dict(),
                "mode_id": str(value(task_scope, "mode_id", "chat") or "chat"),
                "profile_version": int(
                    value(task_scope, "profile_version", 1) or 1
                ),
                "policy_version": int(
                    value(task_scope, "policy_version", 1) or 1
                ),
                "model": str(value(task_scope, "model", "") or ""),
                "reasoning_effort": str(
                    value(task_scope, "reasoning_effort", "") or ""
                ),
                "metadata": (
                    dict(raw_metadata) if isinstance(raw_metadata, Mapping) else {}
                ),
            }
        target = cls._coerce_reply_target(
            supplied_map.get("reply_target", reply_target),
            fallback=owner_target,
        )
        # Explicit scope arguments are only a fallback for taskless requests;
        # a task-owned envelope cannot redirect its durable user identity.
        if task_scope is not None:
            cls._validate_reply_target_scope(
                target,
                channel=owner_channel,
                bot_id=owner_bot,
                external_user_id=owner_user,
                session_id=owner_session,
            )
        elif any((channel, bot_id, external_user_id)):
            cls._validate_reply_target_scope(
                target,
                channel=str(channel or ""),
                bot_id=str(bot_id or ""),
                external_user_id=str(external_user_id or ""),
                session_id=str(session_id or "default"),
            )
        target = cls._coerce_reply_target(
            target,
            fallback=ReplyTarget(
                channel=owner_channel,
                bot_id=owner_bot,
                external_user_id=owner_user,
                session_id=owner_session,
            ),
        )
        has_complete_route = bool(
            target.channel and target.bot_id and target.external_user_id
        )
        canonical_conversation = (
            canonical_conversation_id(
                target.channel,
                target.bot_id,
                target.external_user_id,
                target.session_id,
                destination,
            )
            if has_complete_route
            else ""
        )
        supplied_conversation = str(
            supplied_map.get("conversation_id", "") or ""
        ).strip()
        conversation_id = (
            cls._resolve_scoped_conversation_tx(
                conn,
                supplied_conversation,
                channel=target.channel,
                bot_id=target.bot_id,
                external_user_id=target.external_user_id,
                session_id=target.session_id,
                agent_id=destination,
            )
            if canonical_conversation
            else supplied_conversation
            or mailbox_conversation_id(destination, request_id)
        )

        # Resolve destination mode/profile versions from durable definitions
        # only when the manager did not provide a complete immutable snapshot.
        profile_row = conn.execute(
            "SELECT * FROM agent_profiles WHERE agent_id=? "
            "ORDER BY profile_version DESC LIMIT 1",
            (destination,),
        ).fetchone()
        try:
            profile_version = int(
                supplied_map.get(
                    "profile_version",
                    value(profile_row, "profile_version", 1),
                )
                or 1
            )
        except (TypeError, ValueError):
            raise StoreError("mailbox profile version is invalid") from None
        if profile_row is not None and int(profile_row["profile_version"]) != profile_version:
            profile_row = conn.execute(
                "SELECT * FROM agent_profiles WHERE agent_id=? AND profile_version=?",
                (destination, profile_version),
            ).fetchone()
        default_mode = str(
            value(profile_row, "default_mode_id", "chat") or "chat"
        )
        selected_mode = None
        selected_policy_version = None
        if has_complete_route:
            selected_mode = conn.execute(
                "SELECT mode_id, policy_version, authorized_by, authorized_at "
                "FROM session_modes "
                "WHERE channel=? AND bot_id=? AND external_user_id=? "
                "AND session_id=? AND agent_id=?",
                (
                    target.channel,
                    target.bot_id,
                    target.external_user_id,
                    target.session_id or "default",
                    destination,
                ),
            ).fetchone()
        mode_id = str(
            supplied_map.get(
                "mode_id",
                value(selected_mode, "mode_id", default_mode),
            )
            or default_mode
        ).strip().lower()
        if mode_id == "execute" and not (
            selected_mode is not None
            and value(selected_mode, "authorized_by", None)
            and value(selected_mode, "authorized_at", None)
        ):
            # A selected row without durable actor/timestamp evidence cannot
            # promote an internal mailbox turn to a writable execute mode.
            mode_id = "chat"
            selected_mode = None
        try:
            policy_version = int(
                supplied_map.get(
                    "policy_version",
                    value(selected_mode, "policy_version", None),
                )
                or 0
            )
        except (TypeError, ValueError):
            raise StoreError("mailbox policy version is invalid") from None
        mode_row = None
        if policy_version > 0:
            mode_row = conn.execute(
                "SELECT * FROM agent_modes WHERE agent_id=? AND mode_id=? "
                "AND policy_version=?",
                (destination, mode_id, policy_version),
            ).fetchone()
        if mode_row is None:
            mode_row = conn.execute(
                "SELECT * FROM agent_modes WHERE agent_id=? AND mode_id=? "
                "ORDER BY policy_version DESC LIMIT 1",
                (destination, mode_id),
            ).fetchone()
        if policy_version <= 0:
            policy_version = int(value(mode_row, "policy_version", 1) or 1)
        if policy_version <= 0:
            raise StoreError("mailbox policy version is invalid")

        metadata = supplied_map.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, Mapping) else {}

        def profile_snapshot(row: Any) -> dict[str, Any] | None:
            if row is None:
                return None
            return {
                "agent_id": str(row["agent_id"]),
                "display_name": str(row["display_name"] or ""),
                "summary": str(row["summary"] or ""),
                "system_prompt": str(row["system_prompt"] or ""),
                "responsibilities": json_loads(row["responsibilities_json"], []) or [],
                "constraints": json_loads(row["constraints_json"], []) or [],
                "capabilities": json_loads(row["capabilities_json"], []) or [],
                "allowed_peers": json_loads(row["allowed_peers_json"], []) or [],
                "denied_peers": json_loads(row["denied_peers_json"], []) or [],
                "allowed_request_types": json_loads(row["allowed_request_types_json"], []) or [],
                "denied_request_types": json_loads(row["denied_request_types_json"], []) or [],
                "max_child_depth": int(row["max_child_depth"] or 0),
                "max_children_per_task": int(row["max_children_per_task"] or 0),
                "enabled": bool(row["enabled"]),
                "profile_version": int(row["profile_version"]),
                "default_mode_id": str(row["default_mode_id"] or "chat"),
            }

        def mode_snapshot(row: Any) -> dict[str, Any] | None:
            if row is None:
                return None
            return {
                "mode_id": str(row["mode_id"]),
                "developer_instructions": str(row["developer_instructions"] or ""),
                "sandbox_policy": str(row["sandbox_policy"] or "read-only").replace("_", "-"),
                "approval_policy": str(row["approval_policy"] or "deny_all"),
                "allowed_tools": json_loads(row["allowed_tools_json"], []) or [],
                "denied_tools": json_loads(row["denied_tools_json"], []) or [],
                "can_write_files": bool(row["can_write_files"]),
                "can_execute_commands": bool(row["can_execute_commands"]),
                "can_create_child_tasks": bool(row["can_create_child_tasks"]),
                "can_send_agent_messages": bool(row["can_send_agent_messages"]),
                "policy_version": int(row["policy_version"]),
            }

        metadata.setdefault("profile", profile_snapshot(profile_row))
        metadata.setdefault("mode", mode_snapshot(mode_row))
        metadata.setdefault("internal_mailbox", True)
        snapshot: dict[str, Any] = {
            "agent_id": destination,
            "conversation_id": conversation_id,
            "reply_target": target.to_dict(),
            "mode_id": mode_id,
            "profile_version": profile_version,
            "policy_version": policy_version,
            "model": str(
                supplied_map.get(
                    "model",
                    value(task_scope, "model", "") or "",
                )
                or ""
            ),
            "reasoning_effort": str(
                supplied_map.get(
                    "reasoning_effort",
                    value(task_scope, "reasoning_effort", "") or "",
                )
                or ""
            ),
            "metadata": metadata,
        }
        # Preserve additional adapter-owned immutable fields while excluding
        # arbitrary mailbox payload keys from the runtime control plane.
        for key in ("thread_id", "request_type"):
            if key in supplied_map:
                snapshot[key] = supplied_map[key]
        return snapshot

    @staticmethod
    def _resolve_scoped_conversation_tx(
        conn: sqlite3.Connection,
        conversation_id: str,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
    ) -> str:
        """Resolve a canonical conversation, preserving only proven legacy rows."""

        candidates = conversation_id_candidates(
            channel,
            bot_id,
            external_user_id,
            session_id,
            agent_id,
        )
        canonical_id = candidates[0]
        requested = str(conversation_id or "").strip()
        if not requested or requested == canonical_id:
            return canonical_id
        legacy_id = candidates[-1]
        if requested != legacy_id:
            raise StoreError("mailbox conversation identity conflicts with reply target")
        if legacy_id == canonical_id:
            return canonical_id

        legacy = conn.execute(
            "SELECT channel, bot_id, external_user_id, session_id, agent_id "
            "FROM conversations WHERE conversation_id=?",
            (legacy_id,),
        ).fetchone()
        expected = {
            "channel": channel,
            "bot_id": bot_id,
            "external_user_id": external_user_id,
            "session_id": session_id or "default",
            "agent_id": agent_id,
        }
        if legacy is not None and all(
            str(legacy[column] or "") == str(value or "")
            for column, value in expected.items()
        ):
            return legacy_id
        # An unproven delimiter-joined ID is ambiguous. The framed ID keeps the
        # mailbox turn isolated without rejecting an otherwise valid envelope.
        return canonical_id

    async def resolve_scoped_conversation(
        self,
        conversation_id: str,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str,
    ) -> str:
        """Resolve a mailbox snapshot ID against persisted conversation scope."""

        return await self._call(
            lambda conn: self._resolve_scoped_conversation_tx(
                conn,
                conversation_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
                agent_id=agent_id,
            )
        )

    @staticmethod
    def _outgoing_media_from_row(
        row: sqlite3.Row | None,
        parent: sqlite3.Row | None = None,
    ) -> OutgoingMediaRecord | None:
        if row is None:
            return None
        return OutgoingMediaRecord(
            media_id=row["media_id"],
            attachment_id=row["attachment_id"],
            channel=row["channel"],
            bot_id=row["bot_id"],
            external_user_id=row["external_user_id"],
            outbox_id=row["outbox_id"],
            state=MediaDeliveryState(row["state"]),
            idempotency_key=row["idempotency_key"],
            remote_id=row["remote_id"],
            upload_param=row["upload_param"],
            encryption_key=row["encryption_key"],
            metadata=json_loads(row["metadata_json"], {}) or {},
            claimed_by=row["claimed_by"],
            claim_token=row["claim_token"],
            lease_expires_at=text_to_datetime(row["lease_expires_at"]),
            attempts=int(row["attempts"] or 0),
            next_attempt_at=text_to_datetime(row["next_attempt_at"]),
            last_error=row["last_error"],
            created_at=text_to_datetime(row["created_at"]),
            updated_at=text_to_datetime(row["updated_at"]),
            uploaded_at=text_to_datetime(row["uploaded_at"]),
            sent_at=text_to_datetime(row["sent_at"]),
            reply_slot_id=(parent["reply_slot_id"] if parent is not None else None),
            reply_ordinal=(
                int(parent["reply_ordinal"])
                if parent is not None and parent["reply_ordinal"] is not None
                else None
            ),
            client_id=(str(parent["client_id"] or "") if parent is not None else ""),
            contextless_client_id=(
                parent["contextless_client_id"] if parent is not None else None
            ),
            active_wire_variant=(
                str(parent["active_wire_variant"] or "primary")
                if parent is not None
                else "primary"
            ),
            from_user_id=(
                str(parent["from_user_id"] or parent["bot_id"] or "")
                if parent is not None
                else ""
            ),
            context_token=(parent["context_token"] if parent is not None else None),
            reply_target=(
                json_loads(parent["reply_target_json"], {}) or {}
                if parent is not None
                else {}
            ),
        )

    @classmethod
    def _authorize_outgoing_media_replay_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        agent_id: str | None,
        outbox_id: str | None,
        session_id: str | None,
        channel: str,
        bot_id: str,
        external_user_id: str,
    ) -> None:
        """Authenticate an idempotent media replay without reading the file.

        Sent rows may intentionally outlive their local attachment, so replay
        authorization cannot depend on the attachment being ``ready``.  New
        direct rows persist their Agent snapshot in metadata; outbox-backed
        rows derive it from the durable outbox owner.  Legacy direct rows fall
        back to the live attachment ACL while bytes still exist.
        """

        supplied_agent = str(agent_id or "").strip()
        supplied_outbox = str(outbox_id or "").strip()
        stored_outbox = str(row["outbox_id"] or "").strip()
        if supplied_outbox != stored_outbox:
            raise StoreError("outgoing media idempotency key is immutable")

        stored_metadata = json_loads(row["metadata_json"], {}) or {}
        stored_agent = (
            str(
                stored_metadata.get(
                    "owner_agent_id", stored_metadata.get("agent_id", "")
                )
                or ""
            ).strip()
            if isinstance(stored_metadata, Mapping)
            else ""
        )
        stored_session = (
            str(stored_metadata.get("session_id") or "default")
            if isinstance(stored_metadata, Mapping)
            else "default"
        )
        if stored_outbox:
            outbox = conn.execute(
                "SELECT agent_id, session_id FROM user_outbox WHERE outbox_id=?",
                (stored_outbox,),
            ).fetchone()
            outbox_agent = str(outbox["agent_id"] or "").strip() if outbox else ""
            operation_session = (
                str(outbox["session_id"] or "default") if outbox else stored_session
            )
            if session_id is not None and str(session_id or "default") != operation_session:
                raise StoreError("outgoing media session conflicts with its operation")
            if outbox_agent:
                if not supplied_agent or supplied_agent != outbox_agent:
                    raise StoreError("outgoing media Agent conflicts with its outbox")
                return
            if stored_agent:
                if not supplied_agent or supplied_agent != stored_agent:
                    raise StoreError("attachment access denied")
                return
            raise StoreError("attachment access denied")

        if session_id is not None and stored_session and str(
            session_id or "default"
        ) != stored_session:
            raise StoreError("outgoing media session conflicts with its operation")

        if not supplied_agent:
            raise StoreError("attachment access denied")
        if stored_agent:
            if supplied_agent != stored_agent:
                raise StoreError("attachment access denied")
            return

        # Legacy direct rows did not persist an operation owner.  They can be
        # replayed only while SQLite can still prove the supplied Agent owns
        # the attachment; once cleanup blocks the row, fail closed.
        attachment = conn.execute(
            "SELECT metadata_json, state FROM attachments WHERE attachment_id=?",
            (row["attachment_id"],),
        ).fetchone()
        if attachment is None or str(attachment["state"] or "ready") != "ready":
            raise StoreError("attachment access denied")
        if not cls._attachment_accessible_tx(
            conn,
            str(row["attachment_id"]),
            agent_id=supplied_agent,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id or "default",
        ):
            raise StoreError("attachment access denied")

    @staticmethod
    def _event_values(event: Any = None, **kwargs: Any) -> dict[str, Any]:
        """Normalize AgentEvent/TaskEvent/mapping values without SDK imports."""
        data: dict[str, Any] = {}
        if event is not None:
            if isinstance(event, Mapping):
                data.update(event)
            elif hasattr(event, "as_dict"):
                converted = event.as_dict()
                if isinstance(converted, Mapping):
                    data.update(converted)
            elif hasattr(event, "to_dict"):
                converted = event.to_dict()
                if isinstance(converted, Mapping):
                    data.update(converted)
            else:
                for name in (
                    "event_id", "message_id", "task_id", "execution_id", "sequence",
                    "event_type", "visibility", "priority", "content", "text",
                    "attachments", "created_at", "destination_agent_id", "request_id",
                    "reply_to_id", "causation_id", "idempotency_key",
                    "source_item_id", "source_item_type", "source_item_ordinal",
                ):
                    if hasattr(event, name):
                        data[name] = getattr(event, name)
        data.update({key: value for key, value in kwargs.items() if value is not None})
        created_at_explicit = "created_at" in data and data["created_at"] is not None
        if "content" not in data:
            data["content"] = data.get("text", "")
        if "event_id" not in data or not data["event_id"]:
            data["event_id"] = data.get("message_id") or _uuid()
        data.setdefault("event_type", "message")
        data.setdefault("visibility", EventVisibility.USER.value)
        data.setdefault("priority", EventPriority.NORMAL.value)
        data.setdefault("attachments", ())
        data.setdefault("created_at", utcnow())
        # A compatibility caller may replay only an event ID and content.  In
        # that case the generated timestamp is not part of the caller-owned
        # envelope.  Runtime AgentEvent values do carry an explicit timestamp,
        # and reusing their identity with a different timestamp must fail.
        data["_created_at_explicit"] = created_at_explicit
        data["visibility"] = _enum_value(data["visibility"], EventVisibility.USER.value)
        data["priority"] = int(_enum_value(data["priority"], EventPriority.NORMAL.value))
        return data

    @staticmethod
    def _event_from_row(row: sqlite3.Row | None) -> TaskEvent | None:
        return SQLiteStore._task_event_from_row(row) if row is not None else None

    @staticmethod
    def _validate_event_execution_tx(
        conn: sqlite3.Connection,
        *,
        task_id: str,
        execution_id: str | None,
    ) -> None:
        """Ensure an event execution belongs to its task.

        ``task_events.execution_id`` has a foreign key to
        ``task_executions.execution_id`` but SQLite cannot express the
        required composite ``(task_id, execution_id)`` relationship with the
        current schema.  Validate that relationship at the write boundary so
        a stale/malicious callback cannot attach another task's execution to
        this task's event.
        """

        if not execution_id:
            return
        row = conn.execute(
            "SELECT task_id FROM task_executions WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"execution not found: {execution_id}")
        if str(row["task_id"]) != str(task_id):
            raise StoreError("event execution does not belong to its task")

    @classmethod
    def _existing_event_replay_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        event_id: str,
        idempotency_key: str,
        idempotency_explicit: bool,
        values: Mapping[str, Any],
        explicit_sequence: bool,
    ) -> sqlite3.Row | None:
        """Return an exact durable replay and reject identity reuse.

        ``event_id`` is global while an idempotency key is scoped to a task.
        A runtime may reconstruct a fresh event UUID while retaining the same
        explicit idempotency key, but neither identity may be used to rewrite
        the immutable event envelope.
        """

        by_event_id = conn.execute(
            "SELECT * FROM task_events WHERE event_id=?",
            (event_id,),
        ).fetchone()
        by_idempotency = None
        if idempotency_explicit:
            by_idempotency = conn.execute(
                "SELECT * FROM task_events WHERE task_id=? AND idempotency_key=?",
                (task_id, idempotency_key),
            ).fetchone()
        by_source_item = None
        execution_id = values.get("execution_id")
        source_item_id = str(values.get("source_item_id") or "").strip()
        source_item_ordinal = values.get("source_item_ordinal")
        if execution_id and source_item_id:
            by_source_item = conn.execute(
                "SELECT * FROM task_events WHERE task_id=? AND execution_id=? "
                "AND source_item_id=? LIMIT 1",
                (task_id, execution_id, source_item_id),
            ).fetchone()
        elif execution_id and source_item_ordinal is not None:
            by_source_item = conn.execute(
                "SELECT * FROM task_events WHERE task_id=? AND execution_id=? "
                "AND (source_item_id IS NULL OR length(trim(source_item_id))=0) "
                "AND source_item_ordinal=? LIMIT 1",
                (task_id, execution_id, int(source_item_ordinal)),
            ).fetchone()
        if (
            by_event_id is not None
            and by_idempotency is not None
            and str(by_event_id["event_id"]) != str(by_idempotency["event_id"])
        ):
            raise StoreError("event_id and idempotency key identify different events")
        identified = [
            row
            for row in (by_event_id, by_idempotency, by_source_item)
            if row is not None
        ]
        if len({str(row["event_id"]) for row in identified}) > 1:
            raise StoreError("event identities identify different events")
        existing = by_event_id or by_idempotency or by_source_item
        if existing is None:
            return None
        if str(existing["task_id"]) != str(task_id):
            raise StoreError("event identity is already attached to another task")

        mismatches: list[str] = []
        incoming = {
            "execution_id": values.get("execution_id"),
            "event_type": str(_enum_value(values.get("event_type"), "message")),
            "visibility": str(
                _enum_value(values.get("visibility"), EventVisibility.USER.value)
            ),
            "priority": int(
                values.get("priority", EventPriority.NORMAL.value)
            ),
            "content": str(values.get("content") or ""),
            "destination_agent_id": values.get("destination_agent_id"),
            "request_id": values.get("request_id"),
            "reply_to_id": values.get("reply_to_id"),
            "causation_id": values.get("causation_id"),
            "source_item_id": values.get("source_item_id"),
            "source_item_type": values.get("source_item_type"),
            "source_item_ordinal": values.get("source_item_ordinal"),
        }
        if values.get("_created_at_explicit"):
            incoming["created_at"] = _utc_text(values.get("created_at"))
        if idempotency_explicit:
            incoming["idempotency_key"] = idempotency_key
        for column, expected in incoming.items():
            actual = existing[column]
            if column in {"priority", "source_item_ordinal"} and expected is not None:
                equal = int(actual) == int(expected)
            else:
                equal = str(actual or "") == str(expected or "")
            if not equal:
                mismatches.append(column)
        if explicit_sequence and int(existing["sequence"]) != int(values["sequence"]):
            mismatches.append("sequence")
        stored_attachments = json_loads(existing["attachments_json"], []) or []
        if cls._json_snapshot(stored_attachments) != cls._json_snapshot(
            list(values.get("attachments") or ())
        ):
            mismatches.append("attachments")
        if mismatches:
            raise StoreError(
                "event identity conflicts with immutable envelope: "
                + ", ".join(mismatches)
            )
        return existing

    @classmethod
    def _retain_event_attachments_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        message_id: str,
        attachments: Iterable[Any],
        created_at: datetime | str | None,
    ) -> None:
        """Create normalized retention refs for registered event media."""

        cls._retain_attachment_refs_tx(
            conn,
            owner_kind="task",
            owner_id=task_id,
            attachments=attachments,
            role="event",
            created_at=created_at,
        )
        cls._retain_attachment_refs_tx(
            conn,
            owner_kind="message",
            owner_id=message_id,
            attachments=attachments,
            role="event",
            created_at=created_at,
        )

    @classmethod
    def _retain_attachment_refs_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        owner_kind: str,
        owner_id: str,
        attachments: Iterable[Any],
        role: str,
        created_at: datetime | str | None,
    ) -> None:
        """Retain registered managed attachments for one durable owner.

        Channel-only media references intentionally remain in the immutable
        JSON snapshot but cannot form a foreign-key-backed retention lease.
        This keeps ingress tolerant of remote media while ensuring every
        managed file referenced by a task/message is protected from cleanup.
        """

        created = _utc_text(created_at)
        for ordinal, raw_attachment in enumerate(attachments):
            attachment_id, _metadata = cls._attachment_value(raw_attachment)
            if not attachment_id:
                continue
            attachment_row = conn.execute(
                "SELECT state FROM attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if attachment_row is None:
                # Preserve unknown/channel-only references in the immutable
                # event payload without manufacturing an invalid managed ref.
                continue
            if str(attachment_row["state"] or "ready") != "ready":
                raise StoreError(f"attachment is unavailable: {attachment_id}")
            conn.execute(
                """INSERT OR IGNORE INTO attachment_refs
                   (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (owner_kind, owner_id, attachment_id, role, ordinal, created),
            )

    @classmethod
    def _input_attachment_values(cls, inputs: Any) -> tuple[Any, ...]:
        """Extract attachment-shaped values from a task input snapshot."""

        if isinstance(inputs, Mapping):
            values: list[Any] = []
            for key in ("attachments", "attachment_ids", "media"):
                raw = inputs.get(key)
                if raw is None:
                    continue
                if isinstance(raw, (Mapping, str, bytes, bytearray)):
                    values.append(raw)
                else:
                    try:
                        values.extend(raw)
                    except TypeError:
                        values.append(raw)
            if inputs.get("attachment_id") is not None:
                values.append(inputs["attachment_id"])
            return tuple(values)
        if isinstance(inputs, (str, bytes, bytearray)):
            return ()
        if isinstance(inputs, Iterable):
            return tuple(inputs)
        return ()

    @classmethod
    def _retain_task_input_attachments_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        inputs: Any,
        created_at: datetime | str | None,
    ) -> None:
        cls._retain_attachment_refs_tx(
            conn,
            owner_kind="task",
            owner_id=task_id,
            attachments=cls._input_attachment_values(inputs),
            role="input",
            created_at=created_at,
        )

    @classmethod
    def _retain_inbound_attachments_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        message_id: str,
        attachments: Iterable[Any],
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        source_message_id: str,
        agent_id: str | None,
        created_at: datetime | str | None,
    ) -> None:
        """Retain only attachments explicitly owned by this channel scope.

        An inbound payload is untrusted input.  Merely naming a known local
        attachment must never create the first durable reference that grants
        access to another user's file.  The media promotion path registers a
        matching immutable owner scope before ingress; legacy/opaque IDs are
        left in the inbound JSON but do not receive a retention lease.
        """

        allowed: list[Any] = []
        expected = {
            "channel": str(channel or ""),
            "bot_id": str(bot_id or ""),
            "external_user_id": str(external_user_id or ""),
            "session_id": str(session_id or "default"),
        }
        source_value = str(source_message_id or "")
        for raw_attachment in attachments or ():
            attachment_id, _metadata = cls._attachment_value(raw_attachment)
            if not attachment_id:
                continue
            row = conn.execute(
                "SELECT metadata_json, state FROM attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if row is None:
                continue
            attachment_state = str(row["state"] or "ready")
            if attachment_state == "blocked_media":
                # Keep blocked media visible in the immutable envelope, but
                # never create a retention lease for a file that cannot be
                # safely read.
                continue
            if attachment_state == "deleting":
                # Cleanup may reserve a file after the inbound/task refs were
                # committed.  A duplicate of that already-owned message must
                # remain acknowledgeable, but a new/unowned ingress must not
                # adopt a row whose filesystem phase belongs to cleanup.
                retained = conn.execute(
                    "SELECT 1 FROM attachment_refs r "
                    "LEFT JOIN tasks t ON r.owner_kind='task' "
                    "AND r.owner_id=t.task_id "
                    "WHERE r.attachment_id=? AND ("
                    "(r.owner_kind='inbound_message' AND r.owner_id=?) OR "
                    "(r.owner_kind='task' AND t.inbound_message_id=?)"
                    ") LIMIT 1",
                    (attachment_id, message_id, message_id),
                ).fetchone()
                if retained is not None:
                    continue
            if attachment_state != "ready":
                raise StoreError(f"attachment is unavailable: {attachment_id}")
            metadata = json_loads(row["metadata_json"], {}) or {}
            if not isinstance(metadata, Mapping):
                continue
            owner_scope = cls._attachment_metadata_scope(metadata)
            source_declared = metadata.get("source_message_id")
            # Inbound adoption requires a complete route and immutable source
            # message binding.  Partial metadata (including owner-agent-only
            # rows) is deliberately ignored rather than upgraded into a
            # capability by an untrusted envelope.
            if owner_scope is None or source_declared in (None, ""):
                continue
            if not cls._attachment_complete_scope_matches(
                metadata,
                channel=expected["channel"],
                bot_id=expected["bot_id"],
                external_user_id=expected["external_user_id"],
                session_id=expected["session_id"],
            ) or str(source_declared) != source_value:
                raise StoreError(f"attachment access denied: {attachment_id}")
            declared_agent = str(
                metadata.get("owner_agent_id", metadata.get("agent_id", "")) or ""
            )
            if declared_agent and (
                not agent_id or declared_agent != str(agent_id)
            ):
                # A store-then-create caller has no resolved Agent yet; fail
                # closed by withholding the inbound ref.  Atomic acceptance
                # supplies the snapshot Agent and rejects a mismatch.
                if not agent_id:
                    continue
                raise StoreError(f"attachment access denied: {attachment_id}")
            allowed.append(raw_attachment)
        cls._retain_attachment_refs_tx(
            conn,
            owner_kind="inbound_message",
            owner_id=message_id,
            attachments=allowed,
            role="media",
            created_at=created_at,
        )

    @classmethod
    def _validate_task_input_attachment_access_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        task_id: str | None,
        inbound_message_id: str | None = None,
        inputs: Any,
        agent_id: str,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        require_registered: bool = False,
    ) -> None:
        """Reject foreign known IDs before a task can self-grant a ref.

        For compatibility, an attachment with no owner metadata and no prior
        references may be adopted by the first explicit task.  Once an owner
        exists, access is evaluated against that immutable Agent/user scope.
        Caller-supplied local metadata is only an assertion: SQLite remains
        authoritative for the path, checksum, size, MIME type, and kind.
        """

        for raw_attachment in cls._input_attachment_values(inputs):
            attachment_id, supplied_metadata = cls._attachment_value(raw_attachment)
            if not isinstance(supplied_metadata, Mapping):
                supplied_metadata = {}
            if not isinstance(raw_attachment, Mapping):
                supplied_metadata = {
                    **dict(supplied_metadata),
                    **{
                        field: getattr(raw_attachment, field)
                        for field in (
                            "path",
                            "local_path",
                            "file_path",
                            "checksum",
                            "sha256",
                            "size",
                            "size_bytes",
                            "mime_type",
                            "mime",
                            "content_type",
                            "kind",
                            "type",
                            "media_kind",
                        )
                        if hasattr(raw_attachment, field)
                    },
                }
            supplied_path = next(
                (
                    supplied_metadata.get(field)
                    for field in ("path", "local_path", "file_path")
                    if supplied_metadata.get(field) not in (None, "")
                ),
                None,
            )
            if not attachment_id:
                if supplied_path is not None:
                    raise StoreError(
                        "managed attachment path requires an attachment_id"
                    )
                continue
            row = conn.execute(
                "SELECT kind, path, mime_type, size_bytes, checksum, "
                "metadata_json, state FROM attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if row is None:
                if require_registered or supplied_path is not None:
                    raise StoreError(f"attachment is unavailable: {attachment_id}")
                # Channel-only references are retained in task JSON but are
                # not managed files and therefore cannot be stolen by ID.
                continue
            if str(row["state"] or "ready") != "ready":
                raise StoreError(f"attachment is unavailable: {attachment_id}")
            metadata = json_loads(row["metadata_json"], {}) or {}
            refs = conn.execute(
                "SELECT owner_kind, owner_id FROM attachment_refs WHERE attachment_id=?",
                (attachment_id,),
            ).fetchall()
            has_owner_metadata = isinstance(metadata, Mapping) and bool(
                metadata.get("owner_agent_id")
                or metadata.get("agent_id")
                or metadata.get("channel")
                or metadata.get("bot_id")
                or metadata.get("external_user_id")
                or metadata.get("user_id")
            )
            # Rows without owner metadata or prior refs are retained for
            # backwards-compatible first-use adoption.  They still go
            # through the authoritative metadata checks below; otherwise a
            # caller could pair an unowned ID with an arbitrary managed path.
            if refs or has_owner_metadata:
                if not cls._attachment_accessible_tx(
                    conn,
                    attachment_id,
                    agent_id=str(agent_id or ""),
                    task_id=task_id,
                    inbound_message_id=inbound_message_id,
                    channel=str(channel or ""),
                    bot_id=str(bot_id or ""),
                    external_user_id=str(external_user_id or ""),
                    session_id=str(session_id or "default"),
                ):
                    raise StoreError(f"attachment access denied: {attachment_id}")

            if supplied_path is not None:
                authoritative_path = str(row["path"] or "")
                if not authoritative_path:
                    raise StoreError(
                        f"attachment metadata conflicts (path): {attachment_id}"
                    )
                try:
                    supplied_resolved = Path(str(supplied_path)).expanduser().resolve(
                        strict=False
                    )
                    authoritative_resolved = Path(authoritative_path).expanduser().resolve(
                        strict=False
                    )
                except (OSError, RuntimeError, ValueError) as exc:
                    raise StoreError(
                        f"attachment metadata conflicts (path): {attachment_id}"
                    ) from exc
                if supplied_resolved != authoritative_resolved:
                    raise StoreError(
                        f"attachment metadata conflicts (path): {attachment_id}"
                    )

            supplied_checksum = next(
                (
                    supplied_metadata.get(field)
                    for field in ("checksum", "sha256")
                    if supplied_metadata.get(field) not in (None, "")
                ),
                None,
            )
            if supplied_checksum is not None and str(supplied_checksum).lower() != str(
                row["checksum"] or ""
            ).lower():
                raise StoreError(
                    f"attachment metadata conflicts (checksum): {attachment_id}"
                )

            supplied_size = next(
                (
                    supplied_metadata.get(field)
                    for field in ("size", "size_bytes")
                    if supplied_metadata.get(field) is not None
                ),
                None,
            )
            if supplied_size is not None:
                try:
                    size_matches = int(supplied_size) == int(row["size_bytes"] or 0)
                except (TypeError, ValueError):
                    size_matches = False
                if not size_matches:
                    raise StoreError(
                        f"attachment metadata conflicts (size): {attachment_id}"
                    )

            supplied_mime = str(
                next(
                    (
                        supplied_metadata.get(field)
                        for field in ("mime_type", "mime", "content_type")
                        if supplied_metadata.get(field) not in (None, "")
                    ),
                    "",
                )
                or ""
            ).split(";", 1)[0].strip().lower()
            authoritative_mime = str(row["mime_type"] or "").strip().lower()
            if supplied_mime:
                if not authoritative_mime:
                    raise StoreError(
                        f"attachment metadata conflicts (mime_type): {attachment_id}"
                    )
                mime_matches = supplied_mime == authoritative_mime
                if supplied_mime.endswith("/*"):
                    mime_matches = authoritative_mime.startswith(
                        supplied_mime[:-1]
                    )
                if not mime_matches:
                    raise StoreError(
                        f"attachment metadata conflicts (mime_type): {attachment_id}"
                    )

            supplied_kind = str(
                next(
                    (
                        supplied_metadata.get(field)
                        for field in ("kind", "type", "media_kind")
                        if supplied_metadata.get(field) not in (None, "")
                    ),
                    "",
                )
                or ""
            ).strip().lower()
            authoritative_kind = str(row["kind"] or "").strip().lower()
            kind_aliases = {
                "local_image": "image",
                "localimage": "image",
                "voice": "audio",
            }
            supplied_kind = kind_aliases.get(supplied_kind, supplied_kind)
            authoritative_kind = kind_aliases.get(
                authoritative_kind, authoritative_kind
            )
            if (
                supplied_kind
                and (
                    not authoritative_kind
                    or supplied_kind != authoritative_kind
                )
            ):
                raise StoreError(
                    f"attachment metadata conflicts (kind): {attachment_id}"
                )

    @classmethod
    def _validate_event_attachment_access_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        attachments: Iterable[Any],
    ) -> None:
        """Fence registered event media to the task/Agent that owns it.

        Unknown IDs are channel-only references and remain in the immutable
        event payload for adapters that understand them.  A registered local
        attachment, however, must already be reachable through the task,
        source/destination message, or an explicit owner metadata scope.  Do
        this check before creating the event's own retention refs; otherwise
        merely naming an ID in an event would grant the sender access to it.
        """

        owner = conn.execute(
            "SELECT agent_id, channel, bot_id, external_user_id, session_id "
            "FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if owner is None:
            raise NotFoundError(f"task not found: {task_id}")
        attachment_values = tuple(attachments or ())
        # Validate caller-supplied attachment assertions (path, checksum,
        # size, MIME, and kind) against SQLite before the event can create its
        # own retention/projection refs.  The ACL pass below remains as an
        # explicit event-boundary check for readability and legacy rows.
        cls._validate_task_input_attachment_access_tx(
            conn,
            task_id=str(task_id),
            inputs={"attachments": attachment_values},
            agent_id=str(owner["agent_id"] or ""),
            channel=str(owner["channel"] or ""),
            bot_id=str(owner["bot_id"] or ""),
            external_user_id=str(owner["external_user_id"] or ""),
            session_id=str(owner["session_id"] or "default"),
        )
        for raw_attachment in attachment_values:
            attachment_id, _metadata = cls._attachment_value(raw_attachment)
            if not attachment_id:
                continue
            row = conn.execute(
                "SELECT attachment_id FROM attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if row is None:
                continue
            if not cls._attachment_accessible_tx(
                conn,
                attachment_id,
                agent_id=str(owner["agent_id"] or ""),
                task_id=str(task_id),
                channel=str(owner["channel"] or ""),
                bot_id=str(owner["bot_id"] or ""),
                external_user_id=str(owner["external_user_id"] or ""),
                session_id=str(owner["session_id"] or "default"),
            ):
                raise StoreError(f"attachment access denied: {attachment_id}")

    @classmethod
    def _append_event_tx(
        cls,
        conn: sqlite3.Connection,
        task_id: str,
        event: Any = None,
        *,
        event_id: str | None = None,
        execution_id: str | None = None,
        sequence: int | None = None,
        event_type: str | None = None,
        visibility: EventVisibility | str | None = None,
        priority: EventPriority | int | None = None,
        content: str | None = None,
        attachments: Iterable[Any] | None = None,
        destination_agent_id: str | None = None,
        request_id: str | None = None,
        reply_to_id: str | None = None,
        causation_id: str | None = None,
        source_item_id: str | None = None,
        source_item_type: str | None = None,
        source_item_ordinal: int | None = None,
        idempotency_key: str | None = None,
        created_at: datetime | str | None = None,
    ) -> TaskEvent:
        # The positional task owner is authoritative, but a serialized event
        # carrying a different task ID is an identity conflict rather than a
        # value that may be silently rewritten by the normalization kwargs.
        raw_task_id: Any = None
        if isinstance(event, Mapping):
            raw_task_id = event.get("task_id")
        elif event is not None:
            raw_task_id = getattr(event, "task_id", None)
        if raw_task_id and str(raw_task_id) != str(task_id):
            raise StoreError("event task_id does not match destination task")
        values = cls._event_values(
            event,
            task_id=task_id,
            event_id=event_id,
            execution_id=execution_id,
            sequence=sequence,
            event_type=event_type,
            visibility=visibility,
            priority=priority,
            content=content,
            attachments=attachments,
            destination_agent_id=destination_agent_id,
            request_id=request_id,
            reply_to_id=reply_to_id,
            causation_id=causation_id,
            source_item_id=source_item_id,
            source_item_type=source_item_type,
            source_item_ordinal=source_item_ordinal,
            created_at=created_at,
        )
        if values.get("task_id") and str(values["task_id"]) != str(task_id):
            raise StoreError("event task_id does not match destination task")
        event_id = str(values["event_id"])
        idempotency_explicit = bool(
            idempotency_key or values.get("idempotency_key")
        )
        effective_idempotency = str(
            idempotency_key or values.get("idempotency_key") or event_id
        )
        explicit_sequence = values.get("sequence") is not None
        # Resolve a replay by its immutable identity before looking at the
        # sequence.  A runtime retry may reconstruct an event with a fresh
        # UUID while retaining the same explicit idempotency key.
        existing_identity = cls._existing_event_replay_tx(
            conn,
            task_id=task_id,
            event_id=event_id,
            idempotency_key=effective_idempotency,
            idempotency_explicit=idempotency_explicit,
            values=values,
            explicit_sequence=explicit_sequence,
        )
        if existing_identity is not None:
            cls._retain_event_attachments_tx(
                conn,
                task_id=task_id,
                message_id=str(existing_identity["event_id"]),
                attachments=json_loads(existing_identity["attachments_json"], []) or [],
                created_at=existing_identity["created_at"],
            )
            return cls._task_event_from_row(existing_identity)
        cls._validate_event_attachment_access_tx(
            conn,
            task_id=task_id,
            attachments=values.get("attachments") or (),
        )
        cls._validate_event_execution_tx(
            conn,
            task_id=task_id,
            execution_id=(
                str(values["execution_id"])
                if values.get("execution_id")
                else None
            ),
        )
        # ``None`` means append at the next sequence.  Explicit sequences are
        # preferred, but a distinct event is never silently discarded merely
        # because a previous execution used the same local sequence number.
        if not explicit_sequence:
            row = conn.execute(
                "SELECT COALESCE(MAX(sequence), -1) + 1 AS sequence FROM task_events WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            values["sequence"] = int(row["sequence"])
        seq = int(values["sequence"])
        attachments_json = json_dumps(list(values.get("attachments") or ()))
        created = _utc_text(values.get("created_at"))
        insert_values = (
            event_id,
            task_id,
            values.get("execution_id"),
            seq,
            str(_enum_value(values.get("event_type"), "message")),
            str(_enum_value(values.get("visibility"), EventVisibility.USER.value)),
            int(values.get("priority", EventPriority.NORMAL.value)),
            str(values.get("content") or ""),
            attachments_json,
            values.get("destination_agent_id"),
            values.get("request_id"),
            values.get("reply_to_id"),
            values.get("causation_id"),
            values.get("source_item_id"),
            values.get("source_item_type"),
            (
                int(values["source_item_ordinal"])
                if values.get("source_item_ordinal") is not None
                else None
            ),
            effective_idempotency,
            created,
        )
        # Sequence numbers are task-global.  If a caller supplies a local
        # sequence that is already occupied by a *different* event, append at
        # the next free number rather than returning the old event and losing
        # terminal/output content.  This is especially important when a
        # runtime emitted progress at sequence 0 and then returned a final
        # result without emitting a final event.
        for _ in range(1000):
            try:
                conn.execute(
                    """INSERT INTO task_events
                       (event_id, task_id, execution_id, sequence, event_type,
                        visibility, priority, content, attachments_json,
                        destination_agent_id, request_id, reply_to_id, causation_id,
                        source_item_id, source_item_type, source_item_ordinal,
                        idempotency_key, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    insert_values,
                )
                break
            except sqlite3.IntegrityError:
                # A concurrent/replayed insert may have won.  Prefer an exact
                # identity match; otherwise advance only the sequence.
                row = cls._existing_event_replay_tx(
                    conn,
                    task_id=task_id,
                    event_id=event_id,
                    idempotency_key=effective_idempotency,
                    idempotency_explicit=idempotency_explicit,
                    values=values,
                    explicit_sequence=explicit_sequence,
                )
                if row is not None:
                    cls._retain_event_attachments_tx(
                        conn,
                        task_id=task_id,
                        message_id=str(row["event_id"]),
                        attachments=json_loads(row["attachments_json"], []) or [],
                        created_at=row["created_at"],
                    )
                    return cls._task_event_from_row(row)
                occupied = conn.execute(
                    "SELECT * FROM task_events WHERE task_id = ? AND sequence = ?",
                    (task_id, seq),
                ).fetchone()
                if occupied is None:
                    raise
                # If the semantic event is an exact replay despite a changed
                # UUID, treat it idempotently; otherwise allocate a new slot.
                same = (
                    str(occupied["event_type"]) == str(_enum_value(values.get("event_type"), "message"))
                    and str(occupied["execution_id"] or "") == str(values.get("execution_id") or "")
                    and str(occupied["visibility"]) == str(_enum_value(values.get("visibility"), EventVisibility.USER.value))
                    and int(occupied["priority"]) == int(values.get("priority", EventPriority.NORMAL.value))
                    and str(occupied["content"] or "") == str(values.get("content") or "")
                    and str(occupied["attachments_json"] or "[]") == attachments_json
                    and occupied["destination_agent_id"] == values.get("destination_agent_id")
                    and occupied["request_id"] == values.get("request_id")
                    and occupied["reply_to_id"] == values.get("reply_to_id")
                    and occupied["causation_id"] == values.get("causation_id")
                    and (
                        not values.get("_created_at_explicit")
                        or str(occupied["created_at"] or "")
                        == str(_utc_text(values.get("created_at")) or "")
                    )
                )
                if same:
                    cls._retain_event_attachments_tx(
                        conn,
                        task_id=task_id,
                        message_id=str(occupied["event_id"]),
                        attachments=json_loads(occupied["attachments_json"], []) or [],
                        created_at=occupied["created_at"],
                    )
                    return cls._task_event_from_row(occupied)
                seq = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(sequence), -1) + 1 FROM task_events WHERE task_id = ?",
                        (task_id,),
                    ).fetchone()[0]
                )
                insert_values = (*insert_values[:3], seq, *insert_values[4:])
        else:  # pragma: no cover - defensive guard against a corrupt table
            raise StoreError("could not allocate task event sequence")

        # ``messages`` is the immutable domain envelope.  It is separate from
        # delivery projections, and using the event ID as its message ID gives
        # retries a stable correlation without duplicating content.  Message
        # IDs are global across task events and Agent mailbox messages; never
        # let ``INSERT OR IGNORE`` silently bind this event to another
        # envelope.
        if conn.execute(
            "SELECT 1 FROM messages WHERE message_id=?", (event_id,)
        ).fetchone() is not None:
            raise StoreError(
                f"message ID is already attached to a different envelope: {event_id}"
            )
        conn.execute(
            """INSERT INTO messages
               (message_id, request_id, reply_to_id, causation_id, task_id,
                source_agent_id, destination_agent_id, visibility, priority,
                content, payload_json, created_at)
               SELECT ?, ?, ?, ?, t.task_id, t.agent_id, ?, ?, ?, ?, ?, ?
               FROM tasks t WHERE t.task_id = ?""",
            (
                event_id,
                values.get("request_id"),
                values.get("reply_to_id"),
                values.get("causation_id"),
                values.get("destination_agent_id"),
                str(_enum_value(values.get("visibility"), EventVisibility.USER.value)),
                int(values.get("priority", EventPriority.NORMAL.value)),
                str(values.get("content") or ""),
                json_dumps({"attachments": list(values.get("attachments") or ())}),
                created,
                task_id,
            ),
        )
        cls._retain_event_attachments_tx(
            conn,
            task_id=task_id,
            message_id=event_id,
            attachments=values.get("attachments") or (),
            created_at=created,
        )
        row = conn.execute("SELECT * FROM task_events WHERE event_id = ?", (event_id,)).fetchone()
        result = cls._task_event_from_row(row)
        if result is None:
            raise StoreError("event insert did not produce a row")
        return result

    @staticmethod
    def _reply_event_field(event: Any, name: str, default: Any = None) -> Any:
        if isinstance(event, Mapping):
            return event.get(name, default)
        return getattr(event, name, default)

    @staticmethod
    def _normalized_reply_event_token(value: Any) -> str:
        return "".join(
            character
            for character in str(value or "").lower()
            if character.isalnum()
        )

    @classmethod
    def _is_stable_completed_reply_event(cls, event: Any) -> bool:
        """Recognize the completed Agent-item boundary used by the worker."""

        visibility = str(
            _enum_value(
                cls._reply_event_field(
                    event, "visibility", EventVisibility.USER.value
                ),
                EventVisibility.USER.value,
            )
        )
        source_type = cls._normalized_reply_event_token(
            cls._reply_event_field(event, "source_item_type", "")
        )
        event_type = cls._normalized_reply_event_token(
            cls._reply_event_field(event, "event_type", "")
        )
        source_item_id = str(
            cls._reply_event_field(event, "source_item_id", "") or ""
        ).strip()
        source_item_ordinal = cls._reply_event_field(
            event, "source_item_ordinal", None
        )
        content = str(
            cls._reply_event_field(event, "content", "") or ""
        ).strip()
        attachments = tuple(
            cls._reply_event_field(event, "attachments", ()) or ()
        )
        stable_text = bool(
            source_type == "agentmessage"
            and event_type in {"agentmessage", "message"}
            and (content or attachments)
        )
        # Image-generation output reaches this boundary only after its bytes
        # have been promoted into a registered managed attachment.  Do not
        # broaden this to arbitrary tool items or path-bearing diagnostics.
        stable_image = bool(
            source_type == "imagegeneration"
            and event_type == "imagegeneration"
            and attachments
            and not content
        )
        return bool(
            visibility == EventVisibility.USER.value
            and not cls._reply_event_field(event, "destination_agent_id", None)
            and (source_item_id or source_item_ordinal is not None)
            and (stable_text or stable_image)
        )

    @classmethod
    def _is_compatibility_final_reply_event(cls, event: Any) -> bool:
        """Accept one identity-less final while excluding deltas/items."""

        visibility = str(
            _enum_value(
                cls._reply_event_field(
                    event, "visibility", EventVisibility.USER.value
                ),
                EventVisibility.USER.value,
            )
        )
        if (
            visibility != EventVisibility.USER.value
            or cls._reply_event_field(event, "destination_agent_id", None)
        ):
            return False
        event_type = cls._normalized_reply_event_token(
            cls._reply_event_field(event, "event_type", "")
        )
        # Compatibility runtimes historically used a source-less ``message``
        # or explicit final/terminal event.  Status, reasoning, tool, and
        # other progress events are diagnostics even when they contain text.
        if event_type not in {"message", "agentmessage", "final", "terminal"}:
            return False
        # Once an event asserts completed-item provenance it must satisfy the
        # strict Agent-message predicate above.  Tool/reasoning items and
        # malformed item identities cannot fall back to the legacy final path.
        if (
            cls._reply_event_field(event, "source_item_id", None)
            or cls._reply_event_field(event, "source_item_ordinal", None)
            is not None
            or cls._normalized_reply_event_token(
                cls._reply_event_field(event, "source_item_type", "")
            )
        ):
            return False
        return bool(
            str(cls._reply_event_field(event, "content", "") or "").strip()
            or tuple(cls._reply_event_field(event, "attachments", ()) or ())
        )

    @classmethod
    def _project_event_tx(
        cls,
        conn: sqlite3.Connection,
        event: TaskEvent,
        *,
        task: TaskRecord,
        notify_enabled: bool | None = None,
        delivery_mode: DeliveryMode | str = DeliveryMode.PUSH_ELIGIBLE,
        execution_snapshot: Mapping[str, Any] | None = None,
        allow_compatibility_final: bool = False,
    ) -> None:
        """Create user/mailbox projections for an already-appended event."""
        visibility = _enum_value(event.visibility, EventVisibility.INTERNAL.value)
        if str(event.task_id) != str(task.task_id):
            raise StoreError("event/task projection ownership mismatch")
        owner = conn.execute(
            "SELECT channel, bot_id, external_user_id, session_id, agent_id "
            "FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        if owner is None:
            raise NotFoundError(f"task not found: {task.task_id}")
        target = cls._coerce_reply_target(
            task.reply_target,
            fallback=ReplyTarget(
                channel=owner["channel"],
                bot_id=owner["bot_id"],
                external_user_id=owner["external_user_id"],
                session_id=owner["session_id"] or "default",
            ),
        )
        cls._validate_reply_target_scope(
            target,
            channel=owner["channel"],
            bot_id=owner["bot_id"],
            external_user_id=owner["external_user_id"],
            session_id=owner["session_id"] or "default",
        )
        if str(owner["agent_id"]) != str(task.agent_id):
            raise StoreError("task Agent projection ownership mismatch")
        if event.destination_agent_id:
            # Agent-directed events are never projected to user_outbox.
            request_id = event.request_id or event.event_id
            if event.reply_to_id:
                original = conn.execute(
                    "SELECT request_id, source_agent_id, destination_agent_id, task_id "
                    "FROM messages WHERE message_id=?",
                    (event.reply_to_id,),
                ).fetchone()
                if original is None:
                    raise StoreError("reply target message does not exist")
                if original["request_id"] and event.request_id and str(original["request_id"]) != str(event.request_id):
                    raise StoreError("reply request_id does not match original request")
                if original["source_agent_id"] and str(original["source_agent_id"]) != str(event.destination_agent_id):
                    raise StoreError("response destination must be the original source Agent")
                if original["destination_agent_id"] and str(original["destination_agent_id"]) != str(task.agent_id):
                    raise StoreError("response source Agent does not own the original request")
                if original["task_id"] is not None and str(original["task_id"]) != str(task.task_id):
                    raise StoreError("response task does not match the original request")
                request_id = event.request_id or original["request_id"] or event.event_id
            mailbox_payload = json_dumps({"attachments": list(event.attachments)})
            task_scope = conn.execute(
                "SELECT agent_id, channel, bot_id, external_user_id, session_id, "
                "conversation_id, reply_target_json, mode_id, profile_version, "
                "policy_version, model, reasoning_effort, metadata_json "
                "FROM tasks WHERE task_id=?",
                (task.task_id,),
            ).fetchone()
            mailbox_snapshot = cls._mailbox_execution_snapshot_tx(
                conn,
                destination_agent_id=str(event.destination_agent_id),
                request_id=str(request_id),
                task_scope=task_scope,
                supplied=execution_snapshot,
            )
            existing_mailbox = conn.execute(
                "SELECT * FROM agent_mailbox "
                "WHERE destination_agent_id=? AND request_id=?",
                (event.destination_agent_id, request_id),
            ).fetchone()
            if existing_mailbox is not None:
                existing_snapshot = json_loads(
                    existing_mailbox["execution_snapshot_json"]
                    if "execution_snapshot_json" in existing_mailbox.keys()
                    else "{}",
                    {},
                ) or {}
                if execution_snapshot is None:
                    # A replay must use the snapshot accepted with the first
                    # projection. Re-resolving the now-current session mode
                    # would turn an idempotent event replay into a policy
                    # mutation or a false conflict.
                    mailbox_snapshot = existing_snapshot
                # A logical request is idempotent, but a reused request ID
                # cannot redirect or rewrite an existing envelope.
                immutable = (
                    str(existing_mailbox["source_agent_id"] or "")
                    == str(task.agent_id)
                    and str(existing_mailbox["destination_agent_id"] or "")
                    == str(event.destination_agent_id)
                    and str(existing_mailbox["reply_to_id"] or "")
                    == str(event.reply_to_id or "")
                    and str(existing_mailbox["causation_id"] or "")
                    == str(event.causation_id or "")
                    and str(existing_mailbox["task_id"] or "")
                    == str(task.task_id)
                    and str(existing_mailbox["content"] or "")
                    == str(event.content or "")
                    and str(existing_mailbox["payload_json"] or "{}")
                    == mailbox_payload
                    and cls._json_snapshot(existing_snapshot)
                    == cls._json_snapshot(mailbox_snapshot)
                )
                if not immutable:
                    raise StoreError(
                        "mailbox request envelope conflicts with an existing request"
                    )
                return
            mailbox_id = _uuid()
            conn.execute(
                """INSERT OR IGNORE INTO agent_mailbox
                   (mailbox_id, message_id, request_id, reply_to_id, causation_id,
                    source_agent_id, destination_agent_id, task_id, content,
                    payload_json, execution_snapshot_json, state, attempts, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?)""",
                (
                    mailbox_id,
                    event.event_id,
                    request_id,
                    event.reply_to_id,
                    event.causation_id,
                    task.agent_id,
                    event.destination_agent_id,
                    task.task_id,
                    event.content,
                    mailbox_payload,
                    json_dumps(mailbox_snapshot),
                    _utc_text(event.created_at),
                ),
            )
            # Keep managed attachments alive for the mailbox lifetime.  The
            # event payload still retains unknown/channel-only references, but
            # only registered attachments can form an FK-backed retention ref.
            for ordinal, raw_attachment in enumerate(event.attachments):
                attachment_id, _metadata = cls._attachment_value(raw_attachment)
                if not attachment_id:
                    continue
                if conn.execute(
                    "SELECT 1 FROM attachments WHERE attachment_id=?",
                    (attachment_id,),
                ).fetchone() is None:
                    continue
                conn.execute(
                    """INSERT OR IGNORE INTO attachment_refs
                       (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                       VALUES ('mailbox', ?, ?, 'agent_message', ?, ?)""",
                    (mailbox_id, attachment_id, ordinal, _utc_text(event.created_at)),
                )
            return
        if visibility != EventVisibility.USER.value:
            return
        if not cls._is_stable_completed_reply_event(event) and not (
            allow_compatibility_final
            and cls._is_compatibility_final_reply_event(event)
        ):
            return
        route = conn.execute(
            """SELECT active_agent_id FROM routes WHERE channel = ? AND bot_id = ?
               AND external_user_id = ? AND session_id = ?""",
            (target.channel, target.bot_id, target.external_user_id, target.session_id),
        ).fetchone()
        metadata = task.metadata if isinstance(task.metadata, Mapping) else {}
        projected_content = str(event.content or "")
        if (
            metadata.get("user_reply_format")
            == USER_REPLY_FORMAT_AGENT_PREFIX_V1
            and projected_content.strip()
        ):
            projected_content = f"{task.agent_id}: {projected_content.strip()}"
        accepted_front_agent = str(
            metadata.get("front_agent_id_at_acceptance", "") or ""
        )
        foreground = bool(
            metadata.get("direct_user_request") is True
            or (
                task.inbound_message_id
                and (
                    (route is not None and route["active_agent_id"] == task.agent_id)
                    or (
                        route is None
                        and (
                            accepted_front_agent == task.agent_id
                            # Preserve the pre-snapshot Codex behavior for tasks
                            # created before this metadata field existed.
                            or (
                                not accepted_front_agent
                                and task.agent_id == "codex"
                            )
                        )
                    )
                )
            )
        )
        # Find the current per-session/per-Agent preference.  Absence means on.
        if foreground:
            # Direct responses to the active front Agent are interactive and
            # are never suppressed by /notify off.  ``silent`` remains an
            # audit/inbox event, however, and must not become an automatic
            # channel send merely because it came from the foreground.
            notify_enabled = event.priority > EventPriority.SILENT
        elif notify_enabled is None:
            pref = conn.execute(
                """SELECT notify_enabled FROM notification_preferences
                   WHERE channel = ? AND bot_id = ? AND external_user_id = ?
                     AND session_id = ? AND agent_id = ?""",
                (
                    target.channel,
                    target.bot_id,
                    target.external_user_id,
                    target.session_id,
                    task.agent_id,
                ),
            ).fetchone()
            notify_enabled = (
                event.priority >= EventPriority.NOTIFY
                and (True if pref is None else bool(pref["notify_enabled"]))
            )
        else:
            notify_enabled = bool(notify_enabled) and event.priority >= EventPriority.NOTIFY
        execution_scope = None
        if event.execution_id:
            execution_scope_row = conn.execute(
                "SELECT delivery_reply_scope_id FROM task_executions "
                "WHERE execution_id=? AND task_id=?",
                (event.execution_id, task.task_id),
            ).fetchone()
            if execution_scope_row is not None:
                execution_scope = execution_scope_row["delivery_reply_scope_id"]
        resolved_scope = (
            conn.execute(
                "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                (execution_scope,),
            ).fetchone()
            if execution_scope
            else cls._reply_scope_for_target_tx(conn, target)
        )
        if resolved_scope is not None:
            delivery_target = (
                cls._reply_target_for_scope_tx(conn, resolved_scope)
                if execution_scope
                else target
            )
            if not projected_content.strip() and not event.attachments:
                return
            source_key = (
                f"item:{event.source_item_id}"
                if event.source_item_id
                else (
                    f"item-ordinal:{event.source_item_ordinal}"
                    if event.source_item_ordinal is not None
                    else f"event:{event.event_id}"
                )
            )
            cls._project_reply_candidate_tx(
                conn,
                target=delivery_target,
                source_key=source_key,
                content=projected_content,
                attachments=event.attachments,
                reply_scope_id=str(resolved_scope["reply_scope_id"]),
                agent_id=task.agent_id,
                task_id=task.task_id,
                execution_id=event.execution_id,
                event_id=event.event_id,
                source_item_id=event.source_item_id,
                source_item_type=event.source_item_type,
                source_item_ordinal=event.source_item_ordinal,
                priority=event.priority,
                delivery_mode=delivery_mode,
                notify_enabled=bool(notify_enabled),
                foreground=foreground,
                from_user_id=delivery_target.bot_id,
                now=_utc_text(event.created_at),
                preserve_existing_delivery_snapshot=True,
            )
            return
        outbox_id = _uuid()
        client_id = str(uuid.uuid4())
        conn.execute(
            """INSERT OR IGNORE INTO user_outbox
               (outbox_id, event_id, task_id, channel, bot_id,
                external_user_id, session_id, agent_id, source_message_id,
                source_sequence, context_token, reply_target_json, content,
                attachments_json, priority, delivery_mode, notify_enabled, foreground,
                state, presentation, client_id, attempts, created_at,
                from_user_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'pending', 'unseen', ?, 0, ?, ?)""",
            (
                outbox_id,
                event.event_id,
                task.task_id,
                target.channel,
                target.bot_id,
                target.external_user_id,
                target.session_id,
                task.agent_id,
                target.source_message_id,
                target.source_sequence,
                target.context_token,
                json_dumps(target.to_dict()),
                projected_content,
                json_dumps(list(event.attachments)),
                int(event.priority),
                str(_enum_value(delivery_mode, DeliveryMode.PUSH_ELIGIBLE.value)),
                int(bool(notify_enabled)),
                int(foreground),
                client_id,
                _utc_text(event.created_at),
                target.bot_id,
            ),
        )
        # Attachment IDs remain on the immutable event/outbox snapshots for
        # compatibility, but managed attachments also receive normalized
        # retention and upload/send projections.  Unknown IDs are deliberately
        # left as structured outbox metadata: unsupported media must remain
        # visible to the Agent/channel rather than aborting an otherwise valid
        # terminal task transaction.
        durable_outbox = conn.execute(
            """SELECT outbox_id FROM user_outbox
               WHERE event_id = ? AND channel = ? AND bot_id = ?
                 AND external_user_id = ? AND session_id = ?""",
            (
                event.event_id,
                target.channel,
                target.bot_id,
                target.external_user_id,
                target.session_id,
            ),
        ).fetchone()
        if durable_outbox is not None:
            cls._project_outgoing_media_tx(
                conn,
                event=event,
                task=task,
                outbox_id=str(durable_outbox["outbox_id"]),
            )

    @staticmethod
    def _attachment_value(value: Any) -> tuple[str, Mapping[str, Any]]:
        """Extract a managed attachment ID and optional channel metadata."""

        if isinstance(value, Mapping):
            aid = value.get("attachment_id", value.get("id", ""))
            return str(aid or "").strip(), value
        aid = getattr(value, "attachment_id", value if isinstance(value, str) else "")
        metadata = getattr(value, "metadata", {})
        return str(aid or "").strip(), metadata if isinstance(metadata, Mapping) else {}

    @classmethod
    def _project_outgoing_media_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        event: TaskEvent,
        task: TaskRecord,
        outbox_id: str,
    ) -> None:
        """Project managed event attachments into durable media operations.

        The projection is idempotent by a deterministic key derived from the
        immutable event, attachment ID, and ordinal.  It intentionally only
        materializes attachments registered in SQLite; channel-only references
        remain in ``attachments_json`` and can be handled by an adapter that
        understands them without weakening the managed-file foreign key.
        """

        cls._project_outgoing_media_values_tx(
            conn,
            target=task.reply_target,
            attachments=event.attachments,
            outbox_id=outbox_id,
            created_at=event.created_at,
        )

    @classmethod
    def _project_outgoing_media_values_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        target: ReplyTarget,
        attachments: Iterable[Any],
        outbox_id: str,
        created_at: datetime | str | None,
    ) -> None:
        """Shared attachment projection for event and direct outbox paths."""

        outbox_owner = conn.execute(
            "SELECT agent_id, session_id FROM user_outbox WHERE outbox_id=?",
            (outbox_id,),
        ).fetchone()
        if outbox_owner is None:
            raise NotFoundError(f"outbox not found: {outbox_id}")
        for ordinal, raw_attachment in enumerate(attachments):
            attachment_id, supplied_metadata = cls._attachment_value(raw_attachment)
            if not attachment_id:
                continue
            attachment_row = conn.execute(
                "SELECT kind, mime_type, size_bytes, metadata_json, state "
                "FROM attachments WHERE attachment_id = ?",
                (attachment_id,),
            ).fetchone()
            if attachment_row is None:
                continue
            # A blocked file is retained as structured context but must not be
            # queued for an upload worker that cannot read it.
            if str(attachment_row["state"] or "ready") == "blocked_media":
                continue
            if str(attachment_row["state"] or "ready") != "ready":
                raise StoreError(f"attachment is unavailable: {attachment_id}")
            metadata = json_loads(attachment_row["metadata_json"], {}) or {}
            if isinstance(metadata, Mapping):
                metadata = dict(metadata)
            else:
                metadata = {}
            metadata.update(
                {
                    "kind": attachment_row["kind"],
                    "mime_type": attachment_row["mime_type"],
                    "size_bytes": int(attachment_row["size_bytes"] or 0),
                }
            )
            # Preserve non-authoritative adapter hints without copying binary
            # payloads or caller-supplied storage/credential fields.
            metadata.update(
                {
                    str(key): value
                    for key, value in supplied_metadata.items()
                    if str(key).lower() not in _ATTACHMENT_AUTHORITY_KEYS
                }
            )
            # Retain the exact transport hint selected for the durable outbox
            # target so a later media retry is routed like its text projection.
            metadata["owner_agent_id"] = str(outbox_owner["agent_id"] or "")
            metadata["session_id"] = str(
                outbox_owner["session_id"] or "default"
            )
            metadata["context_token"] = target.context_token
            # This is ordering metadata only; attachment bytes and authority
            # remain in the managed attachment row.  It lets a canonical
            # multi-item SendMsg reconstruct the candidate's defined order
            # without sorting opaque media IDs.
            metadata["bundle_ordinal"] = ordinal
            idem = f"outbox:{outbox_id}:attachment:{ordinal}:{attachment_id}"
            media_id = str(
                uuid.uuid5(uuid.NAMESPACE_URL, "codex-outgoing-media:" + idem)
            )
            conn.execute(
                """INSERT OR IGNORE INTO outgoing_media
                   (media_id, attachment_id, outbox_id, channel, bot_id,
                    external_user_id, state, idempotency_key, metadata_json,
                    attempts, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'ready', ?, ?, 0, ?, ?)""",
                (
                    media_id,
                    attachment_id,
                    outbox_id,
                    target.channel,
                    target.bot_id,
                    target.external_user_id,
                    idem,
                    json_dumps(metadata),
                    _utc_text(created_at),
                    _utc_text(created_at),
                ),
            )
            # The outbox projection is the retention owner for a media send;
            # duplicate event projection attempts are harmless due to the
            # composite reference key.
            conn.execute(
                """INSERT OR IGNORE INTO attachment_refs
                   (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                   VALUES ('outbox', ?, ?, 'outgoing_media', ?, ?)""",
                (
                    outbox_id,
                    attachment_id,
                    ordinal,
                    _utc_text(created_at),
                ),
            )

    async def append_task_event(
        self,
        task_id: str,
        event: Any = None,
        **kwargs: Any,
    ) -> TaskEvent:
        """Append one ordered immutable event and its eligible projection.

        Workers set ``defer_user_projection`` while a runtime turn is active.
        This keeps progress durable without exposing a final-looking response
        before the terminal task transaction commits. Agent mailbox
        projections are not deferred because collaboration may be needed to
        finish the task.
        """
        claim_token = kwargs.pop("claim_token", None)
        defer_user_projection = bool(kwargs.pop("defer_user_projection", False))
        # Worker/runtime integration may include ownership metadata which is
        # validated here but is not part of the immutable event envelope.
        kwargs.pop("worker_id", None)
        def op(conn: sqlite3.Connection) -> TaskEvent:
            with _transaction(conn):
                now_text = self._now()
                task = self._fetch_task_tx(conn, task_id)
                if task is None:
                    raise NotFoundError(f"task not found: {task_id}")
                if task.is_terminal:
                    # Replays of an already durable event are harmless, but a
                    # new event after terminalization would violate the
                    # completion transaction's immutable history.  Resolve
                    # the explicit identity without creating another
                    # projection; retrying a terminal callback must not append
                    # a second user delivery.
                    identity_kwargs = dict(kwargs)
                    identity_kwargs["task_id"] = task_id
                    values = self._event_values(event, **identity_kwargs)
                    if claim_token is not None:
                        replay_execution_id = values.get("execution_id") or task.execution_id
                        if replay_execution_id is None:
                            raise StoreError("terminal replay has no execution record")
                        replay_execution = conn.execute(
                            "SELECT task_id, claim_token FROM task_executions "
                            "WHERE execution_id = ?",
                            (replay_execution_id,),
                        ).fetchone()
                        if replay_execution is None:
                            raise StoreError(
                                f"execution not found: {replay_execution_id}"
                            )
                        if str(replay_execution["task_id"]) != str(task_id):
                            raise StoreError(
                                "event execution does not belong to its task"
                            )
                        if replay_execution["claim_token"] != claim_token:
                            raise StoreError("execution claim token does not match")
                    event_id = str(values.get("event_id") or "")
                    idem = str(
                        kwargs.get("idempotency_key")
                        or values.get("idempotency_key")
                        or event_id
                    )
                    existing = self._existing_event_replay_tx(
                        conn,
                        task_id=task_id,
                        event_id=event_id,
                        idempotency_key=idem,
                        idempotency_explicit=bool(
                            kwargs.get("idempotency_key")
                            or values.get("idempotency_key")
                        ),
                        values=values,
                        explicit_sequence=values.get("sequence") is not None,
                    )
                    if existing is not None:
                        self._retain_event_attachments_tx(
                            conn,
                            task_id=task_id,
                            message_id=str(existing["event_id"]),
                            attachments=json_loads(
                                existing["attachments_json"], []
                            ) or [],
                            created_at=existing["created_at"],
                        )
                        return self._task_event_from_row(existing)
                    raise InvalidTransition(
                        f"cannot append event to terminal task {task_id}"
                    )
                # Events are execution-owned writes.  A queued/orphaned task
                # has no active worker that may append history, and an active
                # task must be fenced by the exact lease token issued by the
                # claim transaction.  Treat an omitted token the same as a
                # stale token; otherwise any caller with a task id could
                # smuggle a user-visible event into another worker's task.
                if task.state not in {
                    TaskState.CLAIMED,
                    TaskState.RUNNING,
                    TaskState.CANCEL_REQUESTED,
                }:
                    raise InvalidTransition(
                        f"cannot append event to task {task_id} from {task.state.value}"
                    )
                if not claim_token or task.claim_token != claim_token:
                    raise StoreError("task claim token does not match")
                if not self._lease_is_active(task.lease_expires_at, now_text):
                    raise StoreError("task claim lease has expired")
                # The task-row token fences the worker, but the event also
                # names one concrete execution attempt.  Without checking that
                # second identity, a retry worker holding the current token
                # could accidentally attach a callback to an older, finished
                # execution of the same logical task.  Bind omitted execution
                # IDs to the active attempt and reject every stale/cross-attempt
                # value before an event or projection is written.
                event_values = self._event_values(event, **kwargs)
                event_execution_id = (
                    event_values.get("execution_id") or task.execution_id
                )
                if event_execution_id is None:
                    raise StoreError("active task has no execution record")
                execution_row = conn.execute(
                    "SELECT task_id, claim_token, lease_expires_at, finished_at "
                    "FROM task_executions WHERE execution_id = ?",
                    (str(event_execution_id),),
                ).fetchone()
                if execution_row is None:
                    raise StoreError(f"execution not found: {event_execution_id}")
                if str(execution_row["task_id"]) != str(task_id):
                    raise StoreError("event execution does not belong to its task")
                if (
                    task.execution_id is not None
                    and str(event_execution_id) != str(task.execution_id)
                ):
                    raise StoreError("event execution is not the task's active execution")
                if execution_row["finished_at"] is not None:
                    raise InvalidTransition("event execution is already terminal")
                if execution_row["claim_token"] != claim_token:
                    raise StoreError("execution claim token does not match")
                if not self._lease_is_active(
                    execution_row["lease_expires_at"], now_text
                ):
                    raise StoreError("task execution lease has expired")
                event_kwargs = dict(kwargs)
                event_kwargs["execution_id"] = str(event_execution_id)
                result = self._append_event_tx(
                    conn, task_id, event, **event_kwargs
                )
                if not (
                    defer_user_projection
                    and result.visibility == EventVisibility.USER
                    and not result.destination_agent_id
                ):
                    self._project_event_tx(conn, result, task=task)
                return result

        return await self._call(op)

    append_event = append_task_event
    record_task_event = append_task_event

    async def list_task_events(
        self,
        task_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 1000,
    ) -> list[TaskEvent]:
        def op(conn: sqlite3.Connection) -> list[TaskEvent]:
            params: list[Any] = [task_id]
            where = "task_id = ?"
            if after_sequence is not None:
                where += " AND sequence > ?"
                params.append(int(after_sequence))
            params.append(max(0, int(limit)))
            rows = conn.execute(
                f"SELECT * FROM task_events WHERE {where} ORDER BY sequence ASC LIMIT ?", params
            ).fetchall()
            return [self._task_event_from_row(row) for row in rows]

        return await self._call(op)

    get_task_events = list_task_events

    async def complete_task(
        self,
        task_id: str,
        result: AgentResult | Mapping[str, Any] | str | None = None,
        *,
        status: str | None = None,
        events: Iterable[Any] | None = None,
        error: str | None = None,
        claim_token: str | None = None,
        execution_id: str | None = None,
        now: datetime | str | None = None,
        delivery_mode: DeliveryMode | str = DeliveryMode.PUSH_ELIGIBLE,
    ) -> TaskRecord:
        """Commit terminal task state, final event, and projections atomically."""
        completion_data_supplied = (
            result is not None
            or status is not None
            or error is not None
            or events is not None
            or execution_id is not None
        )
        supplied_events = list(events or ())
        result_data: Any = result
        output = ""
        terminal_status = _enum_value(status) if status is not None else None
        if isinstance(result, AgentResult):
            terminal_status = terminal_status or _enum_value(result.status)
            output = result.output
            result_data = result.as_dict()
            if error is None:
                error = result.error
            if not supplied_events:
                supplied_events = list(result.events)
            execution_id = execution_id or result.execution_id
        elif result is not None and hasattr(result, "status"):
            # Accept the SDK-independent contract object from ``src.agents``
            # without importing that module into the store's type boundary.
            terminal_status = terminal_status or _enum_value(getattr(result, "status", "completed"))
            output = str(
                getattr(result, "content", getattr(result, "output", getattr(result, "text", "")))
                or ""
            )
            result_data = result.as_dict() if hasattr(result, "as_dict") else result
            if error is None:
                error = getattr(result, "error", None)
            if not supplied_events:
                supplied_events = list(getattr(result, "events", ()) or ())
            execution_id = execution_id or getattr(result, "execution_id", None)
        elif isinstance(result, Mapping):
            terminal_status = terminal_status or _enum_value(result.get("status", "completed"))
            output = str(result.get("output", result.get("content", result.get("text", ""))) or "")
            if error is None:
                error = result.get("error")
            if not supplied_events:
                supplied_events = list(result.get("events", ()) or ())
        elif isinstance(result, str):
            terminal_status = terminal_status or "completed"
            output = result
        terminal_status = str(_enum_value(terminal_status, "completed") or "completed").lower()
        state_map = {
            "completed": TaskState.COMPLETED,
            "success": TaskState.COMPLETED,
            "failed": TaskState.FAILED,
            "error": TaskState.FAILED,
            "interrupted": TaskState.INTERRUPTED,
            "cancelled": TaskState.CANCELLED,
            "canceled": TaskState.CANCELLED,
        }
        target_state = state_map.get(terminal_status, TaskState.FAILED)
        if error and target_state == TaskState.COMPLETED:
            target_state = TaskState.FAILED
        # If the runtime returned text without emitting a corresponding
        # user-visible event, synthesize one.  A runtime may emit progress
        # events and still return a distinct terminal string; the presence of
        # those progress rows must not suppress the final projection.  When
        # the returned text is merely the concatenation of emitted user text,
        # avoid duplicating it.
        if output:
            compatibility_event_contents: list[str] = []
            has_stable_completed_reply = False
            for item in supplied_events:
                values = self._event_values(item)
                if self._is_stable_completed_reply_event(values):
                    has_stable_completed_reply = True
                elif self._is_compatibility_final_reply_event(values):
                    compatibility_event_contents.append(
                        str(values.get("content") or "")
                    )
            normalized_output = output.strip()
            # AgentResult.content is an aggregate convenience once stable
            # completed items exist.  Formatting differences (for example a
            # newline inserted between two items) must never manufacture a
            # second aggregate reply.  An identity-less compatibility event is
            # accepted only when exactly one such final represents the result;
            # multiple progress-like messages become one synthesized final.
            represented = has_stable_completed_reply or (
                len(compatibility_event_contents) == 1
                and compatibility_event_contents[0].strip() == normalized_output
            )
            if not represented:
                # Use ``sequence=None`` so it is allocated after any progress
                # events already persisted for this task; a local sequence 0
                # must never hide terminal output.
                supplied_events.append(
                    {
                        "task_id": task_id,
                        "sequence": None,
                        "event_type": "final",
                        "visibility": EventVisibility.USER,
                        "priority": EventPriority.NORMAL,
                        "content": output,
                        "execution_id": execution_id,
                        "_store_synthesized_final": True,
                    }
                )

        def op(conn: sqlite3.Connection) -> TaskRecord:
            nonlocal target_state, error
            with _transaction(conn):
                now_text = self._now(now)
                task = self._fetch_task_tx(conn, task_id)
                if task is None:
                    raise NotFoundError(f"task not found: {task_id}")
                if task.is_terminal:
                    # A completion callback may be retried after its commit.
                    # Return the terminal task only when every caller-supplied
                    # event identity still names the same immutable envelope;
                    # do not let a stale callback rewrite or smuggle a new
                    # event merely because task state is already terminal.
                    if claim_token is not None:
                        replay_execution_id = execution_id or task.execution_id
                        if replay_execution_id is None:
                            raise StoreError(
                                "terminal replay has no execution record"
                            )
                        replay_execution = conn.execute(
                            "SELECT task_id, claim_token FROM task_executions "
                            "WHERE execution_id = ?",
                            (replay_execution_id,),
                        ).fetchone()
                        if replay_execution is None:
                            raise StoreError(
                                f"execution not found: {replay_execution_id}"
                            )
                        if str(replay_execution["task_id"]) != str(task_id):
                            raise StoreError(
                                "completion execution does not belong to its task"
                            )
                        if replay_execution["claim_token"] != claim_token:
                            raise StoreError("execution claim token does not match")
                    if completion_data_supplied:
                        if target_state != task.state:
                            raise StoreError(
                                "terminal replay status conflicts with persisted task state"
                            )
                        if execution_id is not None and str(execution_id) != str(
                            task.execution_id or ""
                        ):
                            raise StoreError(
                                "terminal replay execution does not match persisted execution"
                            )
                        persisted_output = ""
                        persisted_error = task.last_error
                        persisted_result = task.result
                        if isinstance(persisted_result, Mapping):
                            persisted_output = str(
                                persisted_result.get(
                                    "output",
                                    persisted_result.get(
                                        "content", persisted_result.get("text", "")
                                    ),
                                )
                                or ""
                            )
                            persisted_error = persisted_result.get(
                                "error", persisted_error
                            )
                        elif persisted_result is not None:
                            persisted_output = str(persisted_result)
                        if output and output != persisted_output:
                            raise StoreError(
                                "terminal replay output conflicts with persisted result"
                            )
                        if error is not None and str(error or "") != str(
                            persisted_error or ""
                        ):
                            raise StoreError(
                                "terminal replay error conflicts with persisted result"
                            )
                    for item in supplied_events:
                        values = self._event_values(item)
                        if values.pop("_store_synthesized_final", False):
                            # Store-generated final text has no caller-owned
                            # replay identity. The durable terminal/result row
                            # already proves that projection was committed.
                            continue
                        raw_task_id = values.get("task_id")
                        if raw_task_id and str(raw_task_id) != str(task_id):
                            raise StoreError(
                                "event task_id does not match destination task"
                            )
                        values["task_id"] = task_id
                        if not values.get("execution_id"):
                            values["execution_id"] = execution_id or task.execution_id
                        event_id_value = str(values.get("event_id") or "")
                        idem = str(
                            values.get("idempotency_key") or event_id_value
                        )
                        existing = self._existing_event_replay_tx(
                            conn,
                            task_id=task_id,
                            event_id=event_id_value,
                            idempotency_key=idem,
                            idempotency_explicit=bool(values.get("idempotency_key")),
                            values=values,
                            explicit_sequence=values.get("sequence") is not None,
                        )
                        if existing is None:
                            raise InvalidTransition(
                                f"cannot append event to terminal task {task_id}"
                            )
                        self._retain_event_attachments_tx(
                            conn,
                            task_id=task_id,
                            message_id=str(existing["event_id"]),
                            attachments=json_loads(
                                existing["attachments_json"], []
                            ) or [],
                            created_at=existing["created_at"],
                        )
                    return task
                # Completion is an execution-owned terminal write.  Do this
                # check before appending any supplied events so an unowned or
                # tokenless callback cannot leave durable history behind even
                # if the later state update would fail.
                if not claim_token or task.claim_token != claim_token:
                    raise StoreError("task claim token does not match")
                if not self._lease_is_active(task.lease_expires_at, now_text):
                    raise StoreError("task claim lease has expired")
                # Completion belongs to an execution that entered ``running``.
                # A claimed row has not started Agent work yet, so allowing it
                # to complete would leave its execution without ``started_at``.
                # ``cancel_requested`` remains valid because cancellation may
                # race between claim and the worker's running transition.
                if task.state not in {
                    TaskState.RUNNING,
                    TaskState.CANCEL_REQUESTED,
                }:
                    raise InvalidTransition(
                        f"cannot complete task {task_id} from {task.state.value}"
                    )
                # A terminal callback must belong to the currently claimed
                # execution.  The task row exposes the latest execution ID as
                # a projection; reject an explicitly supplied stale or
                # cross-task ID before appending events so the execution
                # history cannot be corrupted.
                effective_execution_id = execution_id or task.execution_id
                if effective_execution_id is None:
                    raise StoreError("active task has no execution record")
                execution_row = conn.execute(
                    "SELECT task_id, claim_token, lease_expires_at, finished_at "
                    "FROM task_executions "
                    "WHERE execution_id = ?",
                    (effective_execution_id,),
                ).fetchone()
                if execution_row is None:
                    raise StoreError(f"execution not found: {effective_execution_id}")
                if str(execution_row["task_id"]) != str(task_id):
                    raise StoreError("completion execution does not belong to its task")
                if execution_row["finished_at"] is not None:
                    raise InvalidTransition("execution is already terminal")
                if execution_row["claim_token"] != claim_token:
                    raise StoreError("execution claim token does not match")
                if not self._lease_is_active(
                    execution_row["lease_expires_at"], now_text
                ):
                    raise StoreError("task execution lease has expired")
                unfinished_count = conn.execute(
                    "SELECT COUNT(*) FROM task_executions "
                    "WHERE task_id=? AND finished_at IS NULL",
                    (task_id,),
                ).fetchone()[0]
                if int(unfinished_count) != 1:
                    raise StoreError("task has an ambiguous active execution")
                if task.execution_id and execution_id and str(task.execution_id) != str(execution_id):
                    raise StoreError("completion execution is not the task's active execution")
                # An explicit cancellation request wins a race with a late
                # runtime result.  Never turn ``cancel_requested`` back into
                # ``completed``/``failed`` merely because the SDK returned.
                if task.state == TaskState.CANCEL_REQUESTED:
                    if target_state not in {TaskState.INTERRUPTED, TaskState.CANCELLED}:
                        target_state = (
                            TaskState.INTERRUPTED
                            if target_state == TaskState.COMPLETED
                            else TaskState.CANCELLED
                        )
                    if target_state == TaskState.INTERRUPTED:
                        error = error or "task interrupted"
                # Append all events before transitioning.  Existing sequence or
                # idempotency rows are returned, so a retried completion is safe.
                appended: list[TaskEvent] = []
                synthesized_reply_event_ids: set[str] = set()
                for item in supplied_events:
                    values = self._event_values(item)
                    synthesized_final = bool(
                        values.pop("_store_synthesized_final", False)
                    )
                    if synthesized_final and task.state == TaskState.CANCEL_REQUESTED:
                        # A cancellation request wins over a late successful
                        # result.  Retain the returned text for internal audit,
                        # but never project it as a successful user response.
                        values["event_type"] = target_state.value
                        values["visibility"] = EventVisibility.INTERNAL
                        values["priority"] = EventPriority.SILENT
                    if not values.get("task_id"):
                        values["task_id"] = task_id
                    if not values.get("execution_id"):
                        values["execution_id"] = effective_execution_id
                    appended_event = self._append_event_tx(conn, task_id, values)
                    appended.append(appended_event)
                    if synthesized_final:
                        synthesized_reply_event_ids.add(appended_event.event_id)
                # Every terminal transition carries a dedicated ``terminal``
                # domain event.  A user-facing ``final`` event is a result
                # projection, not the state-machine marker itself.  Check the
                # current execution's existing history as well so a retry
                # that already appended the marker remains idempotent without
                # treating a marker from an older execution as this attempt's
                # terminal proof.
                has_terminal_event = any(
                    str(item.event_type).lower() == "terminal"
                    and str(item.execution_id or "")
                    == str(effective_execution_id or "")
                    for item in appended
                )
                if not has_terminal_event:
                    existing_terminal = conn.execute(
                        "SELECT 1 FROM task_events "
                        "WHERE task_id=? AND event_type='terminal' "
                        "AND ((execution_id IS NULL AND ? IS NULL) "
                        "OR execution_id=?) LIMIT 1",
                        (task_id, effective_execution_id, effective_execution_id),
                    ).fetchone()
                    has_terminal_event = existing_terminal is not None
                if not has_terminal_event and target_state in {
                    TaskState.COMPLETED, TaskState.FAILED, TaskState.INTERRUPTED, TaskState.CANCELLED
                }:
                    # Keep a terminal audit event even when the runtime has no
                    # user-visible text.  The deterministic idempotency key
                    # makes this safe for compatibility callers that replay
                    # the same completion before observing the terminal row.
                    appended.append(
                        self._append_event_tx(
                            conn,
                            task_id,
                            event_type="terminal",
                            visibility=EventVisibility.INTERNAL,
                            priority=EventPriority.SILENT,
                            content=error or target_state.value,
                            execution_id=effective_execution_id,
                            idempotency_key=(
                                f"terminal:{effective_execution_id}:{target_state.value}"
                            ),
                        )
                    )
                stable_reply_event_ids = {
                    item.event_id
                    for item in appended
                    if self._is_stable_completed_reply_event(item)
                }
                if stable_reply_event_ids:
                    terminal_reply_event_ids = stable_reply_event_ids
                elif synthesized_reply_event_ids:
                    terminal_reply_event_ids = synthesized_reply_event_ids
                elif output:
                    exact_compatibility_events = [
                        item
                        for item in appended
                        if self._is_compatibility_final_reply_event(item)
                        and str(item.content or "").strip() == output.strip()
                    ]
                    terminal_reply_event_ids = (
                        {exact_compatibility_events[0].event_id}
                        if len(exact_compatibility_events) == 1
                        else set()
                    )
                else:
                    # Preserve event-only compatibility runtimes while still
                    # requiring one unambiguous terminal selection. Multiple
                    # source-less messages are progress-like without a final
                    # aggregate and therefore are not independently sendable.
                    compatibility_events = [
                        item
                        for item in appended
                        if self._is_compatibility_final_reply_event(item)
                    ]
                    terminal_reply_event_ids = (
                        {compatibility_events[0].event_id}
                        if len(compatibility_events) == 1
                        else set()
                    )
                for item in appended:
                    if (
                        item.visibility == EventVisibility.USER
                        and not item.destination_agent_id
                        and item.event_id not in terminal_reply_event_ids
                    ):
                        continue
                    self._project_event_tx(
                        conn,
                        item,
                        task=task,
                        delivery_mode=delivery_mode,
                        allow_compatibility_final=(
                            item.event_id in terminal_reply_event_ids
                        ),
                    )
                changed = conn.execute(
                    """UPDATE tasks SET state = ?, updated_at = ?, terminal_at = ?,
                           last_error = ?, result_json = ?, claimed_by = NULL,
                           claim_token = NULL, lease_expires_at = NULL
                       WHERE task_id = ? AND state NOT IN ('completed','failed','interrupted','cancelled')
                         AND (? IS NULL OR claim_token = ?)
                         AND lease_expires_at IS NOT NULL
                         AND lease_expires_at > ?""",
                    (
                        target_state.value,
                        now_text,
                        now_text,
                        error,
                        json_dumps(result_data) if result_data is not None else None,
                        task_id,
                        claim_token,
                        claim_token,
                        now_text,
                    ),
                ).rowcount
                if changed != 1:
                    # Another idempotent completion may have won.  Return its
                    # durable result rather than duplicating projections.
                    current = self._fetch_task_tx(conn, task_id)
                    if current is not None and current.is_terminal:
                        return current
                    raise InvalidTransition(f"cannot complete task {task_id}")
                execution_id_value = effective_execution_id
                if execution_id_value:
                    execution_changed = conn.execute(
                        """UPDATE task_executions SET state = ?, finished_at = ?,
                               last_error = ?, lease_expires_at = NULL
                           WHERE execution_id = ? AND task_id = ? AND finished_at IS NULL
                             AND (? IS NULL OR claim_token = ?)
                             AND lease_expires_at IS NOT NULL
                             AND lease_expires_at > ?""",
                        (
                            target_state.value,
                            now_text,
                            error,
                            execution_id_value,
                            task_id,
                            claim_token,
                            claim_token,
                            now_text,
                        ),
                    ).rowcount
                    if execution_changed != 1:
                        raise InvalidTransition("active execution could not be finalized")
                refreshed = self._fetch_task_tx(conn, task_id)
                if refreshed is None:
                    raise StoreError("completed task disappeared")
                return refreshed

        return await self._call(op)

    finish_task = complete_task
    complete = complete_task

    async def fail_task(self, task_id: str, error: str, **kwargs: Any) -> TaskRecord:
        return await self.complete_task(task_id, status="failed", error=error, **kwargs)

    async def mark_task_completed(self, task_id: str, result: Any = None, **kwargs: Any) -> TaskRecord:
        return await self.complete_task(task_id, result=result, status="completed", **kwargs)

    async def retry_task(
        self,
        task_id: str,
        *,
        actor: str = "",
        delay: float = 0.0,
        initial_reply: Any = None,
        delivery_reply_scope_id: str | None = None,
        now: datetime | str | None = None,
    ) -> TaskRecord | None:
        """Explicitly requeue a failed/orphaned task as a new execution attempt."""
        now_text = self._now(now)
        next_at = self._lease_deadline(now_text, delay) if delay > 0 else now_text

        def op(conn: sqlite3.Connection) -> TaskRecord | None:
            with _transaction(conn):
                row = self._fetch_task_tx(conn, task_id)
                if row is None:
                    return None
                pending_scope_id: str | None = None
                initial_replayed = True
                if initial_reply is not None:
                    pending_scope_id, initial_replayed = (
                        self._project_initial_task_reply_tx(
                            conn,
                            initial_reply=initial_reply,
                            task=row,
                            fallback_target=row.reply_target,
                            delivery_reply_scope_id=delivery_reply_scope_id,
                            now=now_text,
                        )
                    )
                elif delivery_reply_scope_id:
                    scope = self._delivery_reply_scope_tx(
                        conn,
                        target=row.reply_target,
                        reply_scope_id=delivery_reply_scope_id,
                    )
                    if scope is None:
                        raise NotFoundError(
                            "retry delivery scope does not identify a stored inbound"
                        )
                    pending_scope_id = str(scope["reply_scope_id"])
                if row.state not in {TaskState.ORPHANED, TaskState.FAILED, TaskState.INTERRUPTED}:
                    if pending_scope_id and row.state == TaskState.QUEUED:
                        existing_pending = str(
                            row.pending_delivery_reply_scope_id or ""
                        )
                        if existing_pending and existing_pending != pending_scope_id:
                            raise StoreError("retry delivery reply scope conflicts")
                        conn.execute(
                            "UPDATE tasks SET pending_delivery_reply_scope_id=? "
                            "WHERE task_id=? AND state='queued'",
                            (pending_scope_id, task_id),
                        )
                        return self._fetch_task_tx(conn, task_id)
                    if pending_scope_id and not initial_replayed:
                        raise StoreError(
                            "task was claimed before its retry reply reservation"
                        )
                    return row
                conn.execute(
                    """UPDATE tasks SET state = 'queued', claimed_by = NULL,
                           claim_token = NULL, lease_expires_at = NULL,
                           next_attempt_at = ?, last_error = NULL, updated_at = ?,
                           result_json = NULL, terminal_at = NULL,
                           cancel_requested_at = NULL,
                           pending_delivery_reply_scope_id=? WHERE task_id = ?""",
                    (next_at, now_text, pending_scope_id, task_id),
                )
                # Preserve history and make the explicit retry auditable.
                self._append_event_tx(
                    conn,
                    task_id,
                    event_type="retry_requested",
                    visibility=EventVisibility.INTERNAL,
                    priority=EventPriority.SILENT,
                    content=f"retry requested by {actor}" if actor else "retry requested",
                    created_at=now_text,
                )
                return self._fetch_task_tx(conn, task_id)

        return await self._call(op)

    requeue_task = retry_task

    async def cancel_task(self, task_id: str, *, actor: str = "", now: datetime | str | None = None) -> bool:
        """Cancel a queued task or request cancellation for an active one."""
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute("SELECT state FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
                if row is None:
                    return False
                state = TaskState(row["state"])
                if state in {TaskState.QUEUED, TaskState.ORPHANED}:
                    changed = conn.execute(
                        "UPDATE tasks SET state='cancelled', updated_at=?, terminal_at=? WHERE task_id=? AND state=?",
                        (now_text, now_text, task_id, state.value),
                    ).rowcount == 1
                    if changed:
                        self._append_event_tx(conn, task_id, event_type="cancelled", visibility=EventVisibility.INTERNAL, priority=EventPriority.SILENT, content=actor or "cancelled", created_at=now_text)
                    return changed
                if state in {TaskState.CLAIMED, TaskState.RUNNING}:
                    return conn.execute(
                        "UPDATE tasks SET state='cancel_requested', cancel_requested_at=?, updated_at=? WHERE task_id=? AND state IN ('claimed','running')",
                        (now_text, now_text, task_id),
                    ).rowcount == 1
                return state in {TaskState.CANCEL_REQUESTED, TaskState.CANCELLED}
        return await self._call(op)

    # ------------------------------------------------------------------ command receipts
    async def begin_command_receipt(
        self,
        command_id: str,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        external_message_id: str,
        command_name: str,
        command_args: Iterable[Any] = (),
        command_text: str = "",
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Reserve one control command for at-most-once effect execution.

        The reservation is intentionally separate from the command effect.
        If the process dies after the effect but before its response is
        published, startup marks the row ``interrupted`` and a redelivery
        receives a recovery response instead of rerunning an unknown effect.
        Immutable command fields are checked on every replay so a malformed
        channel retry cannot borrow another command's receipt.
        """

        command_id = str(command_id or "").strip()
        if not command_id:
            raise ValueError("command_id is required")
        values = (
            str(channel or ""),
            str(bot_id or ""),
            str(external_user_id or ""),
            str(session_id or "default"),
            str(external_message_id or ""),
            str(command_name or ""),
            tuple(str(value) for value in (command_args or ())),
            str(command_text or ""),
        )
        if not values[0] or not values[1] or not values[2] or not values[4]:
            raise ValueError("command receipt scope is incomplete")
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            with _transaction(conn):
                inserted = conn.execute(
                    """INSERT OR IGNORE INTO command_receipts
                       (command_id, channel, bot_id, external_user_id,
                        session_id, external_message_id, command_name,
                        command_args_json, command_text, state, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?)""",
                    (
                        command_id,
                        values[0],
                        values[1],
                        values[2],
                        values[3],
                        values[4],
                        values[5],
                        json_dumps(list(values[6])),
                        values[7],
                        now_text,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                if row is None:
                    raise StoreError("command receipt reservation failed")
                immutable = {
                    "channel": values[0],
                    "bot_id": values[1],
                    "external_user_id": values[2],
                    "session_id": values[3],
                    "external_message_id": values[4],
                    "command_name": values[5],
                    "command_text": values[7],
                }
                for column, expected in immutable.items():
                    if str(row[column] or "") != str(expected or ""):
                        raise StoreError(
                            f"command receipt identity conflicts: {command_id} ({column})"
                        )
                stored_args = tuple(
                    str(value)
                    for value in (json_loads(row["command_args_json"], []) or [])
                )
                if stored_args != values[6]:
                    raise StoreError(
                        f"command receipt identity conflicts: {command_id} (command_args)"
                    )
                return self._command_receipt_from_row(
                    row, created=inserted.rowcount == 1
                ) or {}

        return await self._call(op)

    # Short aliases make the capability discoverable to channel adapters and
    # preserve a readable protocol name for compatibility facades.
    reserve_command = begin_command_receipt
    begin_command = begin_command_receipt

    async def get_command_receipt(self, command_id: str) -> dict[str, Any] | None:
        command_id = str(command_id or "").strip()
        if not command_id:
            return None

        def op(conn: sqlite3.Connection) -> dict[str, Any] | None:
            row = conn.execute(
                "SELECT * FROM command_receipts WHERE command_id=?", (command_id,)
            ).fetchone()
            return self._command_receipt_from_row(row)

        return await self._call(op)

    get_command = get_command_receipt

    async def reopen_interrupted_command_receipt(
        self,
        command_id: str,
        *,
        expected_command_name: str = "ask",
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Atomically reacquire an interrupted idempotent command receipt.

        `/ask` task creation and `/recv` FIFO allocation are both protected by
        deterministic durable identities; the channel revalidates ownership
        before requesting this transition.  Other command effects retain
        their unknown-outcome terminal state.
        """

        command_id = str(command_id or "").strip()
        if not command_id:
            raise ValueError("command_id is required")
        expected = str(expected_command_name or "ask").strip().lower()
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"command receipt not found: {command_id}")
                if str(row["command_name"] or "").strip().lower() != expected:
                    raise StoreError(
                        f"command receipt cannot be reopened: {command_id}"
                    )
                changed = conn.execute(
                    "UPDATE command_receipts SET state='started', "
                    "interrupted_at=NULL, completed_at=NULL, response_text=NULL, "
                    "response_agent_id=NULL, presentation_ids_json='[]' "
                    "WHERE command_id=? AND state='interrupted'",
                    (command_id,),
                ).rowcount
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                result = self._command_receipt_from_row(row) or {}
                result["reopened"] = changed == 1
                result["reopened_at"] = now_text if changed == 1 else ""
                return result

        return await self._call(op)

    reopen_command_receipt = reopen_interrupted_command_receipt

    async def complete_command_receipt(
        self,
        command_id: str,
        *,
        response_text: str,
        response_agent_id: str = "",
        presentation_ids: Iterable[str] = (),
        allow_interrupted: bool = False,
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Publish an immutable command response after its effect commits."""

        command_id = str(command_id or "").strip()
        if not command_id:
            raise ValueError("command_id is required")
        response_text = str(response_text or "")
        response_agent_id = str(response_agent_id or "")
        presentation_values = tuple(
            dict.fromkeys(str(value) for value in (presentation_ids or ()) if value)
        )
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"command receipt not found: {command_id}")
                existing_state = str(row["state"])
                if existing_state == "completed":
                    existing = self._command_receipt_from_row(row) or {}
                    if (
                        str(existing.get("response_text", "")) != response_text
                        or str(existing.get("response_agent_id", ""))
                        != response_agent_id
                        or tuple(existing.get("presentation_ids", ()))
                        != presentation_values
                    ):
                        raise StoreError(
                            f"command receipt completion conflicts: {command_id}"
                        )
                    return existing
                if existing_state == "interrupted" and allow_interrupted:
                    if str(row["command_name"] or "").strip().lower() not in {
                        "ask",
                        "recv",
                    }:
                        raise StoreError(
                            f"command receipt cannot recover completion: {command_id}"
                        )
                elif existing_state != "started":
                    # An interrupted receipt is deliberately terminal.  A
                    # stale pre-crash caller must not publish a late response
                    # after a recovery redelivery has taken ownership.
                    raise StoreError(
                        f"command receipt is not active: {command_id} ({existing_state})"
                    )
                changed = conn.execute(
                    """UPDATE command_receipts
                       SET state='completed', response_text=?,
                           response_agent_id=?, presentation_ids_json=?,
                           completed_at=?
                       WHERE command_id=? AND state=?""",
                    (
                        response_text,
                        response_agent_id,
                        json_dumps(list(presentation_values)),
                        now_text,
                        command_id,
                        existing_state,
                    ),
                ).rowcount
                if changed != 1:
                    raise StoreError("command receipt completion lost its reservation")
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                return self._command_receipt_from_row(row) or {}

        return await self._call(op)

    complete_command = complete_command_receipt

    async def interrupt_command_receipt(
        self,
        command_id: str,
        *,
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Terminalize a live receipt whose in-process command raised.

        The command effect may have committed before the exception escaped, so
        the safe response is the same unknown-outcome state used after process
        recovery. Replays can then project that acknowledgement immediately
        without rerunning the command.
        """

        command_id = str(command_id or "").strip()
        if not command_id:
            raise ValueError("command_id is required")
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"command receipt not found: {command_id}")
                state = str(row["state"])
                if state == "started":
                    changed = conn.execute(
                        "UPDATE command_receipts SET state='interrupted', "
                        "interrupted_at=COALESCE(interrupted_at, ?) "
                        "WHERE command_id=? AND state='started'",
                        (now_text, command_id),
                    ).rowcount
                    if changed != 1:
                        raise StoreError(
                            "command receipt interruption lost its reservation"
                        )
                elif state not in {"interrupted", "completed"}:
                    raise StoreError(
                        f"command receipt has unsupported state: {command_id} ({state})"
                    )
                row = conn.execute(
                    "SELECT * FROM command_receipts WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                return self._command_receipt_from_row(row) or {}

        return await self._call(op)

    interrupt_command = interrupt_command_receipt

    # ------------------------------------------------------------------
    # Outbox and presentation leases
    # ------------------------------------------------------------------
    @staticmethod
    def _reply_wire_ids(
        reply_fragment_id: str,
        *,
        client_id: str | None = None,
        contextless_client_id: str | None = None,
    ) -> tuple[str, str]:
        primary = str(
            client_id
            or uuid.uuid5(
                uuid.NAMESPACE_URL,
                "codex-reply-primary:" + str(reply_fragment_id),
            )
        )
        alternate = str(
            contextless_client_id
            or uuid.uuid5(
                uuid.NAMESPACE_URL,
                "codex-reply-contextless:" + str(reply_fragment_id),
            )
        )
        if primary == alternate:
            raise ValueError("contextless client ID must differ from primary client ID")
        return primary, alternate

    @staticmethod
    def _assert_reply_wire_ids_available_tx(
        conn: sqlite3.Connection,
        *,
        primary_client_id: str,
        contextless_client_id: str | None,
        owner_outbox_id: str | None = None,
        owner_reply_slot_id: str | None = None,
    ) -> None:
        """Reject reuse of a wire ID in either variant column.

        SQLite uniqueness constraints apply to one column at a time, so they
        cannot prevent one delivery's primary ID from equalling another
        delivery's contextless ID.  All allocator writes run under the same
        immediate transaction; checking both canonical slots and legacy
        outbox rows here therefore closes that cross-column race as well as
        producing a deterministic store error for caller-supplied IDs.
        """

        primary = str(primary_client_id or "")
        alternate = str(contextless_client_id or "")
        if not primary:
            raise ValueError("primary client ID is required")
        if alternate and alternate == primary:
            raise ValueError(
                "contextless client ID must differ from primary client ID"
            )
        wire_ids = (primary, alternate) if alternate else (primary,)
        placeholders = ",".join("?" for _ in wire_ids)

        slot_filters = [
            f"(client_id IN ({placeholders}) OR "
            f"contextless_client_id IN ({placeholders}))"
        ]
        slot_params: list[Any] = [*wire_ids, *wire_ids]
        if owner_reply_slot_id:
            slot_filters.append("reply_slot_id<>?")
            slot_params.append(str(owner_reply_slot_id))
        slot_collision = conn.execute(
            "SELECT reply_slot_id FROM reply_slots WHERE "
            + " AND ".join(slot_filters)
            + " LIMIT 1",
            slot_params,
        ).fetchone()
        if slot_collision is not None:
            raise StoreError("wire client identity conflicts across variants")

        outbox_filters = [
            f"(client_id IN ({placeholders}) OR "
            f"contextless_client_id IN ({placeholders}))"
        ]
        outbox_params: list[Any] = [*wire_ids, *wire_ids]
        if owner_outbox_id:
            outbox_filters.append("outbox_id<>?")
            outbox_params.append(str(owner_outbox_id))
        outbox_collision = conn.execute(
            "SELECT outbox_id FROM user_outbox WHERE "
            + " AND ".join(outbox_filters)
            + " LIMIT 1",
            outbox_params,
        ).fetchone()
        if outbox_collision is not None:
            raise StoreError("wire client identity conflicts across variants")

    @classmethod
    def _next_deferred_sequence_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> int:
        scope = (channel, bot_id, external_user_id, session_id or "default")
        conn.execute(
            """INSERT OR IGNORE INTO reply_deferred_counters
               (channel, bot_id, external_user_id, session_id, next_sequence)
               VALUES (?, ?, ?, ?, 1)""",
            scope,
        )
        row = conn.execute(
            """SELECT next_sequence FROM reply_deferred_counters
               WHERE channel=? AND bot_id=? AND external_user_id=?
                 AND session_id=?""",
            scope,
        ).fetchone()
        if row is None:
            raise StoreError("failed to reserve deferred reply sequence")
        sequence = int(row["next_sequence"])
        changed = conn.execute(
            """UPDATE reply_deferred_counters SET next_sequence=?
               WHERE channel=? AND bot_id=? AND external_user_id=?
                 AND session_id=? AND next_sequence=?""",
            (sequence + 1, *scope, sequence),
        ).rowcount
        if changed != 1:
            raise StoreError("deferred reply sequence reservation was lost")
        return sequence

    @classmethod
    def _prepare_reply_fragment_specs(
        cls,
        *,
        content: str,
        attachments: Sequence[Any],
        fragments: Sequence[Any] | None,
        first_reply_ordinal: int,
    ) -> tuple[dict[str, Any], ...]:
        """Normalize one candidate into lossless, item-local fragments."""

        prepared: list[dict[str, Any]] = []
        if fragments is not None:
            for raw in fragments:
                if isinstance(raw, Mapping):
                    kind = str(
                        raw.get("fragment_kind", raw.get("kind", "text"))
                        or "text"
                    ).strip().lower()
                    fragment_content = str(raw.get("content", raw.get("text", "")) or "")
                    raw_attachments = raw.get("attachments", ()) or ()
                    if isinstance(raw_attachments, (str, bytes, bytearray, Mapping)):
                        fragment_attachments = (raw_attachments,)
                    else:
                        fragment_attachments = tuple(raw_attachments)
                else:
                    kind = "text"
                    fragment_content = str(raw or "")
                    fragment_attachments = ()
                if kind not in {"text", "media", "bundle"}:
                    raise ValueError(f"unsupported reply fragment kind: {kind}")
                if not fragment_content.strip() and not fragment_attachments:
                    raise ValueError("reply fragment is empty")
                # One normalized media fragment maps to one canonical
                # SendMsg/upload child.  An explicit adapter bundle may carry
                # several attachments, but preserving quota and predecessor
                # ordering requires splitting them before slot allocation.
                normalized_specs: list[tuple[str, str, tuple[Any, ...]]]
                if fragment_attachments:
                    normalized_specs = [
                        (
                            "bundle" if fragment_content else "media",
                            fragment_content,
                            (fragment_attachments[0],),
                        )
                    ]
                    normalized_specs.extend(
                        ("media", "", (attachment,))
                        for attachment in fragment_attachments[1:]
                    )
                else:
                    normalized_specs = [(kind, fragment_content, ())]
                for normalized_kind, normalized_text, normalized_attachments in normalized_specs:
                    wire_ordinal = first_reply_ordinal + len(prepared)
                    text_limit = (
                        REPLY_TEXT_MAX_CHARS - len(REPLY_CONTINUATION_SUFFIX)
                        if wire_ordinal >= REPLY_SCOPE_CAPACITY
                        else REPLY_TEXT_MAX_CHARS
                    )
                    if (
                        normalized_kind in {"text", "bundle"}
                        and len(normalized_text) > text_limit
                    ):
                        raise ValueError(
                            "explicit reply fragment exceeds its deterministic text limit"
                        )
                    prepared.append(
                        {
                            "kind": normalized_kind,
                            "content": normalized_text,
                            "attachments": normalized_attachments,
                        }
                    )
            return tuple(prepared)

        normalized_text = str(content or "").strip()
        cursor = 0
        while cursor < len(normalized_text):
            wire_ordinal = first_reply_ordinal + len(prepared)
            # Deferred fragments use the safe ordinal-ten size.  That makes a
            # later `/recv` allocation lossless without having to mutate or
            # reorder an already-persisted FIFO fragment.
            text_limit = (
                REPLY_TEXT_MAX_CHARS
                if wire_ordinal < REPLY_SCOPE_CAPACITY
                else REPLY_TEXT_MAX_CHARS - len(REPLY_CONTINUATION_SUFFIX)
            )
            piece = normalized_text[cursor : cursor + text_limit]
            prepared.append({"kind": "text", "content": piece, "attachments": ()})
            cursor += len(piece)
        for attachment in attachments:
            prepared.append(
                {"kind": "media", "content": "", "attachments": (attachment,)}
            )
        if not prepared:
            raise ValueError("reply candidate is empty")
        return tuple(prepared)

    @classmethod
    def _reply_outbox_for_fragment_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        candidate: sqlite3.Row,
        fragment: sqlite3.Row,
        target: ReplyTarget,
        slot: sqlite3.Row | None,
        now: str,
        preferred_outbox_id: str | None = None,
        preferred_client_id: str | None = None,
        preferred_contextless_client_id: str | None = None,
        from_user_id: str | None = None,
    ) -> sqlite3.Row:
        """Create/update the sole subordinate outbox row for a fragment."""

        fragment_id = str(fragment["reply_fragment_id"])
        fragment_ordinal = int(fragment["fragment_ordinal"])
        outbox_id = str(
            preferred_outbox_id
            if preferred_outbox_id and fragment_ordinal == 1
            else compound_id("reply-outbox", (fragment_id,))
        )
        client_value, contextless_value = cls._reply_wire_ids(
            fragment_id,
            client_id=(
                preferred_client_id
                if preferred_client_id and fragment_ordinal == 1
                else None
            ),
            contextless_client_id=(
                preferred_contextless_client_id
                if preferred_contextless_client_id and fragment_ordinal == 1
                else None
            ),
        )
        if slot is not None:
            client_value = str(slot["client_id"])
            contextless_value = str(slot["contextless_client_id"] or "") or None
            payload = json_loads(slot["payload_json"], {}) or {}
            content_value = str(payload.get("content", fragment["content"] or ""))
            attachments_value = tuple(
                payload.get(
                    "attachments",
                    json_loads(fragment["attachments_json"], []) or [],
                )
                or ()
            )
            reply_scope_id = str(slot["reply_scope_id"])
            reply_slot_id = str(slot["reply_slot_id"])
            reply_ordinal = int(slot["reply_ordinal"])
        else:
            content_value = str(fragment["content"] or "")
            attachments_value = tuple(
                json_loads(fragment["attachments_json"], []) or []
            )
            reply_scope_id = str(fragment["origin_reply_scope_id"])
            reply_slot_id = None
            reply_ordinal = None
        sender_value = str(from_user_id or target.bot_id or "")
        if sender_value != str(target.bot_id or ""):
            raise StoreError("outbox sender identity conflicts with reply target bot")
        event_id = candidate["event_id"] if fragment_ordinal == 1 else None
        values = (
            outbox_id,
            event_id,
            candidate["task_id"],
            target.channel,
            target.bot_id,
            target.external_user_id,
            target.session_id or "default",
            candidate["agent_id"],
            target.source_message_id,
            target.source_sequence,
            target.context_token,
            json_dumps(target.to_dict()),
            content_value,
            json_dumps(list(attachments_value)),
            int(candidate["priority"]),
            str(candidate["delivery_mode"]),
            int(bool(candidate["notify_enabled"])),
            int(bool(candidate["foreground"])),
            OutboxState.PENDING.value,
            PresentationState.UNSEEN.value,
            client_value,
            0,
            now,
            reply_scope_id,
            reply_slot_id,
            reply_ordinal,
            candidate["reply_candidate_id"],
            fragment_id,
            sender_value,
            contextless_value,
            "primary",
        )
        conn.execute(
            """INSERT OR IGNORE INTO user_outbox
               (outbox_id, event_id, task_id, channel, bot_id,
                external_user_id, session_id, agent_id, source_message_id,
                source_sequence, context_token, reply_target_json, content,
                attachments_json, priority, delivery_mode, notify_enabled,
                foreground, state, presentation, client_id, attempts, created_at,
                reply_scope_id, reply_slot_id, reply_ordinal,
                reply_candidate_id, reply_fragment_id, from_user_id,
                contextless_client_id, active_wire_variant)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            values,
        )
        row = conn.execute(
            "SELECT * FROM user_outbox WHERE outbox_id=?", (outbox_id,)
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT * FROM user_outbox WHERE client_id=?", (client_value,)
            ).fetchone()
        if row is None:
            raise StoreError("reply outbox insert failed")
        if (
            str(row["reply_candidate_id"] or "")
            != str(candidate["reply_candidate_id"])
            or str(row["reply_fragment_id"] or "") != fragment_id
        ):
            raise StoreError("reply outbox identity conflicts with its fragment")
        if slot is not None:
            existing_slot = str(row["reply_slot_id"] or "")
            if existing_slot and existing_slot != str(slot["reply_slot_id"]):
                raise StoreError("reply outbox is already owned by another slot")
            if str(row["client_id"]) != client_value:
                raise StoreError("reply outbox primary wire identity conflicts")
            if existing_slot:
                stored_target = json_loads(row["reply_target_json"], {}) or {}
                if cls._reply_target_snapshot(stored_target) != cls._reply_target_snapshot(
                    target.to_dict()
                ):
                    raise StoreError("canonical reply target identity conflicts")
                if (
                    str(row["reply_scope_id"] or "") != reply_scope_id
                    or int(row["reply_ordinal"] or 0) != int(reply_ordinal or 0)
                    or str(row["content"] or "") != content_value
                    or cls._json_snapshot(
                        json_loads(row["attachments_json"], []) or []
                    )
                    != cls._json_snapshot(list(attachments_value))
                    or str(row["from_user_id"] or "") != sender_value
                ):
                    raise StoreError("canonical reply send envelope conflicts")
                # Context tokens roll, but the first slot allocation owns the
                # exact wire payload.  A replay retains that snapshot rather
                # than mutating a potentially already-attempted send.
                return row
            conn.execute(
                """UPDATE user_outbox
                   SET channel=?, bot_id=?, external_user_id=?, session_id=?,
                       source_message_id=?, source_sequence=?, context_token=?,
                       reply_target_json=?, content=?, attachments_json=?,
                       reply_scope_id=?, reply_slot_id=?, reply_ordinal=?,
                       from_user_id=?, contextless_client_id=?
                   WHERE outbox_id=? AND (reply_slot_id IS NULL OR reply_slot_id=?)""",
                (
                    target.channel,
                    target.bot_id,
                    target.external_user_id,
                    target.session_id or "default",
                    target.source_message_id,
                    target.source_sequence,
                    target.context_token,
                    json_dumps(target.to_dict()),
                    content_value,
                    json_dumps(list(attachments_value)),
                    reply_scope_id,
                    reply_slot_id,
                    reply_ordinal,
                    sender_value,
                    contextless_value,
                    outbox_id,
                    reply_slot_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM user_outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
        if row is None:
            raise StoreError("reply outbox disappeared")
        return row

    @classmethod
    def _defer_reply_fragment_tx(
        cls,
        conn: sqlite3.Connection,
        fragment: sqlite3.Row,
    ) -> sqlite3.Row:
        if str(fragment["state"]) == ReplyFragmentState.DEFERRED_QUOTA.value:
            return fragment
        sequence = cls._next_deferred_sequence_tx(
            conn,
            channel=str(fragment["channel"]),
            bot_id=str(fragment["bot_id"]),
            external_user_id=str(fragment["external_user_id"]),
            session_id=str(fragment["session_id"] or "default"),
        )
        changed = conn.execute(
            """UPDATE reply_fragments
               SET state='deferred_quota', deferred_sequence=?
               WHERE reply_fragment_id=? AND state IN ('retained','inbox_only')
                 AND reply_slot_id IS NULL""",
            (sequence, fragment["reply_fragment_id"]),
        ).rowcount
        if changed != 1:
            raise StoreError("reply fragment defer reservation was lost")
        stored = conn.execute(
            "SELECT * FROM reply_fragments WHERE reply_fragment_id=?",
            (fragment["reply_fragment_id"],),
        ).fetchone()
        if stored is None:
            raise StoreError("deferred reply fragment disappeared")
        return stored

    @classmethod
    def _allocate_reply_fragment_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        fragment: sqlite3.Row,
        scope: sqlite3.Row,
        target: ReplyTarget,
        candidate: sqlite3.Row,
        now: str,
        preferred_outbox_id: str | None = None,
        preferred_client_id: str | None = None,
        preferred_contextless_client_id: str | None = None,
        from_user_id: str | None = None,
    ) -> tuple[sqlite3.Row | None, sqlite3.Row, sqlite3.Row | None]:
        """Reserve one never-recycled scope ordinal or durably defer it."""

        if fragment["reply_slot_id"] is not None:
            slot = conn.execute(
                "SELECT * FROM reply_slots WHERE reply_slot_id=?",
                (fragment["reply_slot_id"],),
            ).fetchone()
            if slot is None:
                raise StoreError("allocated reply fragment has no canonical slot")
            outbox = cls._reply_outbox_for_fragment_tx(
                conn,
                candidate=candidate,
                fragment=fragment,
                target=target,
                slot=slot,
                now=now,
                preferred_outbox_id=preferred_outbox_id,
                preferred_client_id=preferred_client_id,
                preferred_contextless_client_id=preferred_contextless_client_id,
                from_user_id=from_user_id,
            )
            return slot, fragment, outbox
        used_slots = int(scope["used_slots"])
        capacity = int(scope["capacity"])
        if used_slots >= capacity:
            deferred = cls._defer_reply_fragment_tx(conn, fragment)
            # A quota-deferred fragment is retained content, not a canonical
            # send.  Creating a pending outbox here would let an ordinary
            # worker bypass the ten-slot allocator before `/recv` assigns a
            # fresh scope.
            return None, deferred, None
        reply_ordinal = used_slots + 1
        reply_scope_id = str(scope["reply_scope_id"])
        fragment_id = str(fragment["reply_fragment_id"])
        reply_slot_id = compound_id(
            "reply-slot", (reply_scope_id, reply_ordinal)
        )
        delivery_id = str(
            preferred_outbox_id
            if preferred_outbox_id and int(fragment["fragment_ordinal"]) == 1
            else compound_id("reply-outbox", (fragment_id,))
        )
        client_value, contextless_value = cls._reply_wire_ids(
            fragment_id,
            client_id=(
                preferred_client_id
                if preferred_client_id and int(fragment["fragment_ordinal"]) == 1
                else None
            ),
            contextless_client_id=(
                preferred_contextless_client_id
                if preferred_contextless_client_id
                and int(fragment["fragment_ordinal"]) == 1
                else None
            ),
        )
        cls._assert_reply_wire_ids_available_tx(
            conn,
            primary_client_id=client_value,
            contextless_client_id=contextless_value,
            # A compatibility row with this logical delivery ID may be
            # attached to its first canonical slot below.  Its immutable wire
            # values are validated again by the attachment path.
            owner_outbox_id=delivery_id,
        )
        payload_content = str(fragment["content"] or "")
        if (
            reply_ordinal == REPLY_SCOPE_CAPACITY
            and str(fragment["fragment_kind"]) in {"text", "bundle"}
            and payload_content
        ):
            if len(payload_content) + len(REPLY_CONTINUATION_SUFFIX) > REPLY_TEXT_MAX_CHARS:
                raise StoreError("ordinal-ten reply fragment did not reserve suffix capacity")
            payload_content += REPLY_CONTINUATION_SUFFIX
        payload = {
            "kind": str(fragment["fragment_kind"]),
            "content": payload_content,
            "attachments": json_loads(fragment["attachments_json"], []) or [],
            "target": target.to_dict(),
            "from_user_id": str(from_user_id or target.bot_id or ""),
        }
        changed = conn.execute(
            """UPDATE reply_scopes SET used_slots=?
               WHERE reply_scope_id=? AND used_slots=? AND used_slots < capacity""",
            (reply_ordinal, reply_scope_id, used_slots),
        ).rowcount
        if changed != 1:
            raise StoreError("reply ordinal reservation was lost")
        conn.execute(
            """INSERT INTO reply_slots
               (reply_slot_id, reply_scope_id, reply_ordinal,
                reply_fragment_id, delivery_id, client_id,
                contextless_client_id, active_wire_variant, payload_json,
                created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'primary', ?, ?)""",
            (
                reply_slot_id,
                reply_scope_id,
                reply_ordinal,
                fragment_id,
                delivery_id,
                client_value,
                contextless_value,
                json_dumps(payload),
                now,
            ),
        )
        conn.execute(
            """UPDATE reply_fragments
               SET state='allocated', delivery_reply_scope_id=?,
                   reply_slot_id=?, deferred_sequence=NULL, allocated_at=?
               WHERE reply_fragment_id=? AND reply_slot_id IS NULL""",
            (reply_scope_id, reply_slot_id, now, fragment_id),
        )
        stored_fragment = conn.execute(
            "SELECT * FROM reply_fragments WHERE reply_fragment_id=?",
            (fragment_id,),
        ).fetchone()
        slot = conn.execute(
            "SELECT * FROM reply_slots WHERE reply_slot_id=?", (reply_slot_id,)
        ).fetchone()
        if stored_fragment is None or slot is None:
            raise StoreError("reply slot transaction did not produce durable rows")
        outbox = cls._reply_outbox_for_fragment_tx(
            conn,
            candidate=candidate,
            fragment=stored_fragment,
            target=target,
            slot=slot,
            now=now,
            preferred_outbox_id=preferred_outbox_id,
            preferred_client_id=preferred_client_id,
            preferred_contextless_client_id=preferred_contextless_client_id,
            from_user_id=from_user_id,
        )
        conn.execute(
            "UPDATE reply_slots SET outbox_id=? WHERE reply_slot_id=?",
            (outbox["outbox_id"], reply_slot_id),
        )
        slot = conn.execute(
            "SELECT * FROM reply_slots WHERE reply_slot_id=?", (reply_slot_id,)
        ).fetchone()
        return slot, stored_fragment, outbox

    @classmethod
    def _reply_projection_for_candidate_tx(
        cls,
        conn: sqlite3.Connection,
        reply_candidate_id: str,
        *,
        replayed: bool,
    ) -> ReplyProjectionResult:
        candidate_row = conn.execute(
            "SELECT * FROM reply_candidates WHERE reply_candidate_id=?",
            (reply_candidate_id,),
        ).fetchone()
        if candidate_row is None:
            raise StoreError("reply candidate disappeared")
        fragment_rows = conn.execute(
            """SELECT * FROM reply_fragments
               WHERE reply_candidate_id=? ORDER BY fragment_ordinal""",
            (reply_candidate_id,),
        ).fetchall()
        slot_rows = conn.execute(
            """SELECT s.* FROM reply_slots s
               JOIN reply_fragments f
                 ON f.reply_fragment_id=s.reply_fragment_id
               WHERE f.reply_candidate_id=?
               ORDER BY f.fragment_ordinal""",
            (reply_candidate_id,),
        ).fetchall()
        outbox_rows = conn.execute(
            """SELECT o.* FROM user_outbox o
               JOIN reply_fragments f
                 ON f.reply_fragment_id=o.reply_fragment_id
               WHERE f.reply_candidate_id=?
               ORDER BY f.fragment_ordinal""",
            (reply_candidate_id,),
        ).fetchall()
        return ReplyProjectionResult(
            candidate=cls._reply_candidate_from_row(candidate_row),
            fragments=tuple(
                item
                for row in fragment_rows
                if (item := cls._reply_fragment_from_row(row)) is not None
            ),
            slots=tuple(
                item
                for row in slot_rows
                if (item := cls._reply_slot_from_row(row)) is not None
            ),
            outbox_items=tuple(
                item
                for row in outbox_rows
                if (item := cls._outbox_from_row(row)) is not None
            ),
            replayed=replayed,
        )

    @classmethod
    def _project_reply_candidate_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        target: ReplyTarget,
        source_key: str,
        content: str = "",
        attachments: Sequence[Any] = (),
        fragments: Sequence[Any] | None = None,
        reply_scope_id: str | None = None,
        agent_id: str = "codex",
        task_id: str | None = None,
        execution_id: str | None = None,
        event_id: str | None = None,
        source_item_id: str | None = None,
        source_item_type: str | None = None,
        source_item_ordinal: int | None = None,
        priority: EventPriority | int = EventPriority.NORMAL,
        delivery_mode: DeliveryMode | str = DeliveryMode.PUSH_ELIGIBLE,
        notify_enabled: bool = True,
        foreground: bool = False,
        preserve_existing_delivery_snapshot: bool = False,
        preferred_outbox_id: str | None = None,
        preferred_client_id: str | None = None,
        preferred_contextless_client_id: str | None = None,
        from_user_id: str | None = None,
        now: str,
    ) -> ReplyProjectionResult:
        source_value = str(source_key or "").strip()
        if not source_value:
            raise ValueError("reply candidate source_key is required")
        if reply_scope_id:
            scope = conn.execute(
                "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                (str(reply_scope_id),),
            ).fetchone()
        else:
            scope = cls._reply_scope_for_target_tx(conn, target)
        if scope is None:
            raise NotFoundError("reply target does not identify a stored inbound scope")
        expected_route = (
            str(scope["channel"]),
            str(scope["bot_id"]),
            str(scope["external_user_id"]),
            str(scope["session_id"] or "default"),
        )
        actual_route = (
            str(target.channel),
            str(target.bot_id),
            str(target.external_user_id),
            str(target.session_id or "default"),
        )
        if actual_route != expected_route:
            raise StoreError("reply scope conflicts with candidate recipient")
        canonical_target = cls._reply_target_for_scope_tx(conn, scope)
        if cls._reply_target_snapshot(target.to_dict()) != cls._reply_target_snapshot(
            canonical_target.to_dict()
        ):
            raise StoreError("reply target conflicts with its stored inbound scope")
        sender_value = str(from_user_id or target.bot_id or "")
        if sender_value != str(target.bot_id or ""):
            raise StoreError("outbox sender identity conflicts with reply target bot")
        normalized_content = str(content or "").strip()
        attachment_values = tuple(attachments or ())
        if not normalized_content and not attachment_values and not fragments:
            raise ValueError("reply candidate is empty")
        stable_source = (
            ("item", str(task_id), str(execution_id), str(source_item_id))
            if task_id and execution_id and str(source_item_id or "").strip()
            else (
                ("ordinal", str(task_id), str(execution_id), int(source_item_ordinal))
                if task_id and execution_id and source_item_ordinal is not None
                else ("scope", str(scope["reply_scope_id"]), source_value)
            )
        )
        reply_candidate_id = compound_id("reply-candidate", stable_source)
        existing = conn.execute(
            "SELECT * FROM reply_candidates WHERE reply_candidate_id=?",
            (reply_candidate_id,),
        ).fetchone()
        if existing is None:
            conn.execute(
                """INSERT OR IGNORE INTO reply_candidates
                   (reply_candidate_id, origin_reply_scope_id, source_key,
                    channel, bot_id, external_user_id, session_id, agent_id,
                    task_id, execution_id, event_id, source_item_id,
                    source_item_type, source_item_ordinal, content,
                    attachments_json, priority, delivery_mode, notify_enabled,
                    foreground, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    reply_candidate_id,
                    scope["reply_scope_id"],
                    source_value,
                    *actual_route,
                    str(agent_id or "codex"),
                    task_id,
                    execution_id,
                    event_id,
                    str(source_item_id) if source_item_id else None,
                    str(source_item_type) if source_item_type else None,
                    int(source_item_ordinal)
                    if source_item_ordinal is not None
                    else None,
                    normalized_content,
                    json_dumps(list(attachment_values)),
                    int(_enum_value(priority, EventPriority.NORMAL.value)),
                    str(_enum_value(delivery_mode, DeliveryMode.PUSH_ELIGIBLE.value)),
                    int(bool(notify_enabled)),
                    int(bool(foreground)),
                    now,
                ),
            )
            existing = conn.execute(
                "SELECT * FROM reply_candidates WHERE reply_candidate_id=?",
                (reply_candidate_id,),
            ).fetchone()
            if existing is None:
                # A stable source unique index may have resolved a replay
                # whose caller reconstructed a different candidate ID.
                if task_id and execution_id and source_item_id:
                    existing = conn.execute(
                        """SELECT * FROM reply_candidates
                           WHERE task_id=? AND execution_id=? AND source_item_id=?""",
                        (task_id, execution_id, str(source_item_id)),
                    ).fetchone()
                elif task_id and execution_id and source_item_ordinal is not None:
                    existing = conn.execute(
                        """SELECT * FROM reply_candidates
                           WHERE task_id=? AND execution_id=?
                             AND (source_item_id IS NULL OR length(trim(source_item_id))=0)
                             AND source_item_ordinal=?""",
                        (task_id, execution_id, int(source_item_ordinal)),
                    ).fetchone()
            created = existing is not None and not conn.execute(
                "SELECT 1 FROM reply_fragments WHERE reply_candidate_id=? LIMIT 1",
                (existing["reply_candidate_id"],),
            ).fetchone()
        else:
            created = False
        if existing is None:
            raise StoreError("reply candidate insert failed")
        if (
            preserve_existing_delivery_snapshot
            and event_id
            and str(existing["event_id"] or "") == str(event_id)
        ):
            # Stable completed items are projected as soon as they arrive and
            # replayed when the task commits its terminal state. Agent routes
            # and notification preferences may change between those two
            # transactions; the first candidate owns that delivery snapshot.
            notify_enabled = bool(existing["notify_enabled"])
            foreground = bool(existing["foreground"])
        immutable = {
            "origin_reply_scope_id": str(scope["reply_scope_id"]),
            "source_key": source_value,
            "channel": actual_route[0],
            "bot_id": actual_route[1],
            "external_user_id": actual_route[2],
            "session_id": actual_route[3],
            "agent_id": str(agent_id or "codex"),
            "task_id": task_id,
            "execution_id": execution_id,
            "event_id": event_id,
            "source_item_id": source_item_id,
            "source_item_type": source_item_type,
            "source_item_ordinal": source_item_ordinal,
            "content": normalized_content,
            "attachments_json": json_dumps(list(attachment_values)),
            "priority": int(_enum_value(priority, EventPriority.NORMAL.value)),
            "delivery_mode": str(
                _enum_value(delivery_mode, DeliveryMode.PUSH_ELIGIBLE.value)
            ),
            "notify_enabled": int(bool(notify_enabled)),
            "foreground": int(bool(foreground)),
        }
        conflicts: list[str] = []
        for column, expected in immutable.items():
            actual = existing[column]
            if column == "attachments_json":
                equal = cls._json_snapshot(json_loads(actual, [])) == cls._json_snapshot(
                    json_loads(expected, [])
                )
            elif column in {"priority", "notify_enabled", "foreground", "source_item_ordinal"} and expected is not None:
                equal = int(actual) == int(expected)
            else:
                equal = str(actual or "") == str(expected or "")
            if not equal:
                conflicts.append(column)
        if conflicts:
            raise StoreError(
                "reply candidate identity conflicts: " + ", ".join(conflicts)
            )
        reply_candidate_id = str(existing["reply_candidate_id"])
        existing_fragments = conn.execute(
            "SELECT 1 FROM reply_fragments WHERE reply_candidate_id=? LIMIT 1",
            (reply_candidate_id,),
        ).fetchone()
        if existing_fragments is not None:
            projection = cls._reply_projection_for_candidate_tx(
                conn, reply_candidate_id, replayed=True
            )
            if preferred_outbox_id and projection.outbox_items:
                matched = next(
                    (
                        item
                        for item in projection.outbox_items
                        if item.outbox_id == str(preferred_outbox_id)
                    ),
                    None,
                )
                if matched is None:
                    raise StoreError("reply outbox identity conflicts on replay")
                if preferred_client_id and matched.client_id != str(
                    preferred_client_id
                ):
                    raise StoreError("reply primary wire identity conflicts on replay")
                if (
                    preferred_contextless_client_id
                    and matched.contextless_client_id
                    != str(preferred_contextless_client_id)
                ):
                    raise StoreError(
                        "reply contextless wire identity conflicts on replay"
                    )
                if matched.from_user_id != sender_value:
                    raise StoreError("reply sender identity conflicts on replay")
            return projection
        specs = cls._prepare_reply_fragment_specs(
            content=normalized_content,
            attachments=attachment_values,
            fragments=fragments,
            first_reply_ordinal=int(scope["used_slots"]) + 1,
        )
        mode_value = str(existing["delivery_mode"])
        push_eligible = (
            bool(existing["notify_enabled"])
            and mode_value != DeliveryMode.INBOX_ONLY.value
        )
        for fragment_ordinal, spec in enumerate(specs, start=1):
            fragment_id = compound_id(
                "reply-fragment", (reply_candidate_id, fragment_ordinal)
            )
            conn.execute(
                """INSERT INTO reply_fragments
                   (reply_fragment_id, reply_candidate_id,
                    origin_reply_scope_id, channel, bot_id, external_user_id,
                    session_id, fragment_ordinal, fragment_kind, content,
                    attachments_json, state, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'retained', ?)""",
                (
                    fragment_id,
                    reply_candidate_id,
                    scope["reply_scope_id"],
                    *actual_route,
                    fragment_ordinal,
                    spec["kind"],
                    spec["content"],
                    json_dumps(list(spec["attachments"])),
                    now,
                ),
            )
            fragment_row = conn.execute(
                "SELECT * FROM reply_fragments WHERE reply_fragment_id=?",
                (fragment_id,),
            ).fetchone()
            if fragment_row is None:
                raise StoreError("reply fragment insert failed")
            scope = conn.execute(
                "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                (scope["reply_scope_id"],),
            ).fetchone()
            if scope is None:
                raise StoreError("reply scope disappeared during allocation")
            if push_eligible:
                _slot, stored_fragment, outbox = cls._allocate_reply_fragment_tx(
                    conn,
                    fragment=fragment_row,
                    scope=scope,
                    target=target,
                    candidate=existing,
                    now=now,
                    preferred_outbox_id=preferred_outbox_id,
                    preferred_client_id=preferred_client_id,
                    preferred_contextless_client_id=preferred_contextless_client_id,
                    from_user_id=from_user_id,
                )
            else:
                conn.execute(
                    "UPDATE reply_fragments SET state='inbox_only' "
                    "WHERE reply_fragment_id=?",
                    (fragment_id,),
                )
                stored_fragment = conn.execute(
                    "SELECT * FROM reply_fragments WHERE reply_fragment_id=?",
                    (fragment_id,),
                ).fetchone()
                outbox = None
            attachment_fragment_values = tuple(
                json_loads(stored_fragment["attachments_json"], []) or []
            )
            if attachment_fragment_values and outbox is not None:
                cls._project_outgoing_media_values_tx(
                    conn,
                    target=target,
                    attachments=attachment_fragment_values,
                    outbox_id=str(outbox["outbox_id"]),
                    created_at=now,
                )
        return cls._reply_projection_for_candidate_tx(
            conn, reply_candidate_id, replayed=False
        )

    async def project_reply_candidate(
        self,
        *,
        target: ReplyTarget | Mapping[str, Any],
        source_key: str,
        content: str = "",
        attachments: Iterable[Any] = (),
        fragments: Iterable[Any] | None = None,
        reply_scope_id: str | None = None,
        agent_id: str = "codex",
        task_id: str | None = None,
        execution_id: str | None = None,
        event_id: str | None = None,
        source_item_id: str | None = None,
        source_item_type: str | None = None,
        source_item_ordinal: int | None = None,
        priority: EventPriority | int = EventPriority.NORMAL,
        delivery_mode: DeliveryMode | str = DeliveryMode.PUSH_ELIGIBLE,
        notify_enabled: bool = True,
        foreground: bool = False,
        outbox_id: str | None = None,
        client_id: str | None = None,
        contextless_client_id: str | None = None,
        from_user_id: str | None = None,
        now: datetime | str | None = None,
    ) -> ReplyProjectionResult:
        """Idempotently retain and allocate one completed reply item."""

        reply_target = self._coerce_reply_target(target)
        attachment_values = tuple(attachments or ())
        fragment_values = tuple(fragments) if fragments is not None else None

        def op(conn: sqlite3.Connection) -> ReplyProjectionResult:
            with _transaction(conn):
                return self._project_reply_candidate_tx(
                    conn,
                    target=reply_target,
                    source_key=source_key,
                    content=content,
                    attachments=attachment_values,
                    fragments=fragment_values,
                    reply_scope_id=reply_scope_id,
                    agent_id=agent_id,
                    task_id=task_id,
                    execution_id=execution_id,
                    event_id=event_id,
                    source_item_id=source_item_id,
                    source_item_type=source_item_type,
                    source_item_ordinal=source_item_ordinal,
                    priority=priority,
                    delivery_mode=delivery_mode,
                    notify_enabled=notify_enabled,
                    foreground=foreground,
                    preferred_outbox_id=outbox_id,
                    preferred_client_id=client_id,
                    preferred_contextless_client_id=contextless_client_id,
                    from_user_id=from_user_id,
                    now=self._now(now),
                )

        return await self._call(op)

    @classmethod
    def _reply_projection_for_batch_tx(
        cls,
        conn: sqlite3.Connection,
        reply_batch_id: str,
        *,
        replayed: bool,
    ) -> ReplyProjectionResult:
        batch = conn.execute(
            "SELECT * FROM reply_drain_batches WHERE reply_batch_id=?",
            (reply_batch_id,),
        ).fetchone()
        if batch is None:
            raise StoreError("reply drain batch disappeared")
        fragment_rows = conn.execute(
            """SELECT f.* FROM reply_drain_batch_items i
               JOIN reply_fragments f
                 ON f.reply_fragment_id=i.reply_fragment_id
               WHERE i.reply_batch_id=? ORDER BY i.batch_ordinal""",
            (reply_batch_id,),
        ).fetchall()
        slot_rows = conn.execute(
            """SELECT s.* FROM reply_drain_batch_items i
               JOIN reply_slots s ON s.reply_slot_id=i.reply_slot_id
               WHERE i.reply_batch_id=? ORDER BY i.batch_ordinal""",
            (reply_batch_id,),
        ).fetchall()
        outbox_rows = conn.execute(
            """SELECT o.* FROM reply_drain_batch_items i
               JOIN user_outbox o ON o.reply_slot_id=i.reply_slot_id
               WHERE i.reply_batch_id=? ORDER BY i.batch_ordinal""",
            (reply_batch_id,),
        ).fetchall()
        return ReplyProjectionResult(
            fragments=tuple(
                item
                for row in fragment_rows
                if (item := cls._reply_fragment_from_row(row)) is not None
            ),
            slots=tuple(
                item
                for row in slot_rows
                if (item := cls._reply_slot_from_row(row)) is not None
            ),
            outbox_items=tuple(
                item
                for row in outbox_rows
                if (item := cls._outbox_from_row(row)) is not None
            ),
            batch_id=str(batch["reply_batch_id"]),
            replayed=replayed,
        )

    async def drain_deferred_replies(
        self,
        *,
        target: ReplyTarget | Mapping[str, Any],
        source_key: str,
        limit: int = REPLY_SCOPE_CAPACITY,
        reply_scope_id: str | None = None,
        from_user_id: str | None = None,
        now: datetime | str | None = None,
    ) -> ReplyProjectionResult:
        """Allocate the oldest recipient FIFO batch under a `/recv` scope.

        The batch row is inserted even when the queue is empty.  Replaying the
        same inbound/source key therefore returns the first result and can
        never drain a later batch.
        """

        reply_target = self._coerce_reply_target(target)
        source_value = str(source_key or "").strip()
        if not source_value:
            raise ValueError("reply drain source_key is required")
        drain_limit = max(0, min(REPLY_SCOPE_CAPACITY, int(limit)))

        def op(conn: sqlite3.Connection) -> ReplyProjectionResult:
            with _transaction(conn):
                if reply_scope_id:
                    scope = conn.execute(
                        "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                        (str(reply_scope_id),),
                    ).fetchone()
                else:
                    scope = self._reply_scope_for_target_tx(conn, reply_target)
                if scope is None:
                    raise NotFoundError(
                        "reply drain target does not identify a stored inbound scope"
                    )
                route = (
                    str(scope["channel"]),
                    str(scope["bot_id"]),
                    str(scope["external_user_id"]),
                    str(scope["session_id"] or "default"),
                )
                if route != (
                    str(reply_target.channel),
                    str(reply_target.bot_id),
                    str(reply_target.external_user_id),
                    str(reply_target.session_id or "default"),
                ):
                    raise StoreError("reply drain scope conflicts with recipient")
                canonical_target = self._reply_target_for_scope_tx(conn, scope)
                if self._reply_target_snapshot(
                    reply_target.to_dict()
                ) != self._reply_target_snapshot(canonical_target.to_dict()):
                    raise StoreError(
                        "reply drain target conflicts with its stored inbound scope"
                    )
                existing = conn.execute(
                    """SELECT * FROM reply_drain_batches
                       WHERE destination_reply_scope_id=?""",
                    (scope["reply_scope_id"],),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["source_key"]) != source_value
                        or tuple(str(existing[name] or "") for name in (
                            "channel", "bot_id", "external_user_id", "session_id"
                        )) != route
                    ):
                        raise StoreError(
                            "reply drain identity conflicts with its first batch"
                        )
                    return self._reply_projection_for_batch_tx(
                        conn,
                        str(existing["reply_batch_id"]),
                        replayed=True,
                    )
                batch_id = compound_id(
                    "reply-drain", (scope["reply_scope_id"], source_value)
                )
                now_text = self._now(now)
                conn.execute(
                    """INSERT INTO reply_drain_batches
                       (reply_batch_id, destination_reply_scope_id, source_key,
                        channel, bot_id, external_user_id, session_id, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        batch_id,
                        scope["reply_scope_id"],
                        source_value,
                        *route,
                        now_text,
                    ),
                )
                remaining = min(
                    drain_limit,
                    int(scope["capacity"]) - int(scope["used_slots"]),
                )
                rows = conn.execute(
                    """SELECT * FROM reply_fragments
                       WHERE channel=? AND bot_id=? AND external_user_id=?
                         AND session_id=? AND state='deferred_quota'
                         AND reply_slot_id IS NULL
                       ORDER BY deferred_sequence, reply_candidate_id,
                                fragment_ordinal
                       LIMIT ?""",
                    (*route, max(0, remaining)),
                ).fetchall()
                for batch_ordinal, fragment in enumerate(rows, start=1):
                    candidate = conn.execute(
                        "SELECT * FROM reply_candidates WHERE reply_candidate_id=?",
                        (fragment["reply_candidate_id"],),
                    ).fetchone()
                    if candidate is None:
                        raise StoreError("deferred fragment has no reply candidate")
                    scope = conn.execute(
                        "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                        (scope["reply_scope_id"],),
                    ).fetchone()
                    slot, _stored_fragment, _outbox = self._allocate_reply_fragment_tx(
                        conn,
                        fragment=fragment,
                        scope=scope,
                        # The destination scope owns the exact inbound reply
                        # target, including the first durable context token.
                        # Never let a same-recipient caller substitute another
                        # message's source identity or rolling wire context.
                        target=canonical_target,
                        candidate=candidate,
                        now=now_text,
                        from_user_id=from_user_id,
                    )
                    if slot is None:
                        raise StoreError("reply drain selected more fragments than capacity")
                    conn.execute(
                        """INSERT INTO reply_drain_batch_items
                           (reply_batch_id, batch_ordinal, reply_fragment_id,
                            reply_slot_id)
                           VALUES (?, ?, ?, ?)""",
                        (
                            batch_id,
                            batch_ordinal,
                            fragment["reply_fragment_id"],
                            slot["reply_slot_id"],
                        ),
                    )
                return self._reply_projection_for_batch_tx(
                    conn, batch_id, replayed=False
                )

        return await self._call(op)

    async def create_user_outbox(
        self,
        delivery: Any = None,
        *,
        target: Any = None,
        content: str | None = None,
        channel: str | None = None,
        bot_id: str | None = None,
        external_user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        task_id: str | None = None,
        event_id: str | None = None,
        priority: EventPriority | int | None = None,
        delivery_mode: DeliveryMode | str | None = None,
        client_id: str | None = None,
        contextless_client_id: str | None = None,
        from_user_id: str | None = None,
        reply_scope_id: str | None = None,
        source_key: str | None = None,
        outbox_id: str | None = None,
        attachments: Iterable[Any] | None = None,
        present_outbox_ids: Iterable[str] | None = None,
        notify_enabled: bool | None = None,
        foreground: bool | None = None,
        now: datetime | str | None = None,
    ) -> UserOutboxItem | ReplyProjectionResult:
        """Persist an explicit user delivery projection.

        Normal Agent events are projected by ``complete_task``/``append_event``;
        this entry point is for immediate command responses and channel
        adapters that need to preserve a failed send after the inbound row has
        already been committed.  It never accepts an Agent mailbox item as a
        destination.
        """
        data: dict[str, Any] = {}
        if isinstance(delivery, Mapping):
            data.update(delivery)
        elif delivery is not None:
            for name in (
                "target", "reply_target", "content", "text", "channel", "bot_id",
                "external_user_id", "session_id", "agent_id", "task_id", "event_id",
                "priority", "delivery_mode", "client_id", "delivery_id", "outbox_id",
                "contextless_client_id", "from_user_id", "reply_scope_id", "source_key",
                "attachments", "notify_enabled",
                "foreground",
            ):
                if hasattr(delivery, name):
                    data[name] = getattr(delivery, name)
            converter = getattr(delivery, "to_dict", None) or getattr(delivery, "as_dict", None)
            if converter is not None:
                try:
                    converted = converter()
                except TypeError:
                    converted = None
                if isinstance(converted, Mapping):
                    data.update(converted)
        if target is None:
            target = data.get("target", data.get("reply_target"))
        target = self._coerce_reply_target(target)
        # Explicit keyword values override a mapping, while preserving values
        # carried by a channel target when omitted.
        channel_value = channel if channel is not None else (data.get("channel") or target.channel)
        bot_value = bot_id if bot_id is not None else (data.get("bot_id") or target.bot_id)
        user_value = external_user_id if external_user_id is not None else (data.get("external_user_id") or target.external_user_id)
        session_value = session_id if session_id is not None else (data.get("session_id") or target.session_id or "default")
        if not channel_value or not bot_value or not user_value:
            raise ValueError("user outbox target is incomplete")
        target = ReplyTarget(
            channel=str(channel_value), bot_id=str(bot_value),
            external_user_id=str(user_value), session_id=str(session_value),
            source_message_id=target.source_message_id,
            source_sequence=target.source_sequence,
            context_token=target.context_token,
        )
        # Reject a caller-provided target that disagrees with explicit scope
        # fields before any foreign-key projection is written.
        self._validate_reply_target_scope(
            target,
            channel=str(channel_value),
            bot_id=str(bot_value),
            external_user_id=str(user_value),
            session_id=str(session_value or "default"),
        )
        content_value = content if content is not None else data.get("content", data.get("text", ""))
        if not str(content_value or ""):
            raise ValueError("user outbox content is empty")
        event_value = event_id if event_id is not None else data.get("event_id")
        task_value = task_id if task_id is not None else data.get("task_id")
        # Channel-facing delivery values commonly serialize optional foreign
        # keys as empty strings.  SQLite foreign keys require NULL for an
        # absent relation; persisting ``''`` would reject immediate command
        # responses before they can be retried.
        event_value = event_value or None
        task_value = task_value or None
        agent_value = str(
            agent_id if agent_id is not None else data.get("agent_id") or "codex"
        )
        priority_value = int(
            _enum_value(
                priority if priority is not None else data.get("priority"),
                EventPriority.NORMAL.value,
            )
        )
        mode_value = str(
            _enum_value(
                delivery_mode if delivery_mode is not None else data.get("delivery_mode"),
                DeliveryMode.PUSH_ELIGIBLE.value,
            )
        )
        notify_value = bool(
            notify_enabled
            if notify_enabled is not None
            else data.get("notify_enabled", True)
        )
        foreground_value = bool(
            foreground
            if foreground is not None
            else data.get("foreground", False)
        )
        outbox_value = str(outbox_id or data.get("outbox_id") or data.get("delivery_id") or _uuid())
        identity = "\x1f".join((str(channel_value), str(bot_value), str(user_value), str(session_value), outbox_value, str(event_value or ""), str(content_value)))
        client_value = str(client_id or data.get("client_id") or uuid.uuid5(uuid.NAMESPACE_URL, "codex-outbox:" + identity))
        contextless_value = (
            contextless_client_id
            if contextless_client_id is not None
            else data.get("contextless_client_id")
        )
        # Dataclass serializers commonly represent an absent optional ID as
        # an empty string.  Treat that as NULL: the v19 partial unique index
        # otherwise makes every legacy outbox row contend for the same ""
        # alternate wire identity.
        contextless_value = str(contextless_value or "") or None
        from_user_value = str(
            from_user_id
            if from_user_id is not None
            else data.get("from_user_id") or target.bot_id
        )
        if from_user_value != str(target.bot_id or ""):
            raise StoreError("outbox sender identity conflicts with reply target bot")
        scope_value = str(
            reply_scope_id
            if reply_scope_id is not None
            else data.get("reply_scope_id") or ""
        ) or None
        source_key_value = str(
            source_key
            if source_key is not None
            else data.get("source_key") or f"outbox:{outbox_value}"
        )
        attachment_values = tuple(attachments if attachments is not None else data.get("attachments", ()) or ())
        presentation_values = tuple(
            dict.fromkeys(str(item) for item in (present_outbox_ids or ()) if item)
        )
        now_text = self._now(now)

        def op(
            conn: sqlite3.Connection,
        ) -> UserOutboxItem | ReplyProjectionResult:
            with _transaction(conn):
                effective_task_value = task_value
                owner_task = None
                if effective_task_value is not None:
                    owner_task = conn.execute(
                        "SELECT task_id, channel, bot_id, external_user_id, session_id, agent_id "
                        "FROM tasks WHERE task_id=?",
                        (effective_task_value,),
                    ).fetchone()
                    if owner_task is None:
                        raise NotFoundError(f"task not found: {effective_task_value}")
                if event_value is not None:
                    owner_event = conn.execute(
                        "SELECT m.task_id, m.visibility, m.destination_agent_id, "
                        "t.channel, t.bot_id, t.external_user_id, "
                        "t.session_id, t.agent_id FROM messages m "
                        "LEFT JOIN tasks t ON t.task_id=m.task_id WHERE m.message_id=?",
                        (event_value,),
                    ).fetchone()
                    if owner_event is None:
                        raise NotFoundError(f"event not found: {event_value}")
                    if (
                        str(owner_event["visibility"] or "internal") != EventVisibility.USER.value
                        or owner_event["destination_agent_id"] is not None
                    ):
                        raise StoreError(
                            "agent-directed/internal event cannot be projected to user outbox"
                        )
                    if owner_event["task_id"] is not None:
                        if effective_task_value is not None and str(effective_task_value) != str(owner_event["task_id"]):
                            raise StoreError("outbox event and task ownership conflict")
                        effective_task_value = str(owner_event["task_id"])
                        owner_task = owner_event
                owner = owner_task
                if owner is not None:
                    for field in ("channel", "bot_id", "external_user_id", "session_id"):
                        supplied = getattr(target, field)
                        expected = owner[field]
                        if supplied and expected and str(supplied) != str(expected):
                            raise StoreError(f"outbox target conflicts with task ownership ({field})")
                    if str(agent_value) != str(owner["agent_id"]):
                        raise StoreError("outbox Agent conflicts with task ownership")
                # An explicit outbox is itself a capability-bearing media
                # owner.  Fence every registered attachment before inserting
                # that owner row, otherwise naming another Agent's attachment
                # here would manufacture both an upload operation and a new
                # durable reference.  The task-input validator also enforces
                # SQLite-authoritative path/checksum/size/MIME/kind metadata
                # while retaining compatibility with unknown channel IDs and
                # safe first use of an unowned legacy attachment.
                self._validate_task_input_attachment_access_tx(
                    conn,
                    task_id=(
                        str(effective_task_value)
                        if effective_task_value is not None
                        else None
                    ),
                    inputs={"attachments": attachment_values},
                    agent_id=agent_value,
                    channel=target.channel,
                    bot_id=target.bot_id,
                    external_user_id=target.external_user_id,
                    session_id=target.session_id,
                )
                resolved_scope = (
                    conn.execute(
                        "SELECT * FROM reply_scopes WHERE reply_scope_id=?",
                        (scope_value,),
                    ).fetchone()
                    if scope_value
                    else self._reply_scope_for_target_tx(conn, target)
                )
                if scope_value or resolved_scope is not None:
                    event_snapshot = (
                        conn.execute(
                            "SELECT execution_id, source_item_id, source_item_type, "
                            "source_item_ordinal FROM task_events WHERE event_id=?",
                            (event_value,),
                        ).fetchone()
                        if event_value is not None
                        else None
                    )
                    projection = self._project_reply_candidate_tx(
                        conn,
                        target=target,
                        source_key=source_key_value,
                        content=str(content_value),
                        attachments=attachment_values,
                        reply_scope_id=(
                            str(resolved_scope["reply_scope_id"])
                            if resolved_scope is not None
                            else scope_value
                        ),
                        agent_id=agent_value,
                        task_id=(
                            str(effective_task_value)
                            if effective_task_value is not None
                            else None
                        ),
                        execution_id=(
                            event_snapshot["execution_id"]
                            if event_snapshot is not None
                            else None
                        ),
                        event_id=(
                            str(event_value)
                            if event_snapshot is not None
                            else None
                        ),
                        source_item_id=(
                            event_snapshot["source_item_id"]
                            if event_snapshot is not None
                            else None
                        ),
                        source_item_type=(
                            event_snapshot["source_item_type"]
                            if event_snapshot is not None
                            else None
                        ),
                        source_item_ordinal=(
                            event_snapshot["source_item_ordinal"]
                            if event_snapshot is not None
                            else None
                        ),
                        priority=priority_value,
                        delivery_mode=mode_value,
                        notify_enabled=notify_value,
                        foreground=foreground_value,
                        preferred_outbox_id=outbox_value,
                        preferred_client_id=client_value,
                        preferred_contextless_client_id=(
                            str(contextless_value)
                            if contextless_value is not None
                            else None
                        ),
                        from_user_id=from_user_value,
                        now=now_text,
                    )
                    if presentation_values:
                        placeholders = ",".join("?" for _ in presentation_values)
                        presentation_rows = conn.execute(
                            f"SELECT outbox_id, channel, bot_id, external_user_id, "
                            f"session_id FROM user_outbox WHERE outbox_id IN ({placeholders})",
                            presentation_values,
                        ).fetchall()
                        if len(presentation_rows) != len(presentation_values):
                            raise StoreError(
                                "command presentation references an unavailable outbox item"
                            )
                        expected_scope = (
                            str(target.channel),
                            str(target.bot_id),
                            str(target.external_user_id),
                            str(target.session_id or "default"),
                        )
                        if any(
                            tuple(
                                str(row[name] or ("default" if name == "session_id" else ""))
                                for name in (
                                    "channel", "bot_id", "external_user_id", "session_id"
                                )
                            )
                            != expected_scope
                            for row in presentation_rows
                        ):
                            raise StoreError(
                                "command presentation conflicts with user/session ownership"
                            )
                        conn.execute(
                            f"UPDATE user_outbox SET presentation='presented', "
                            f"presented_at=COALESCE(presented_at, ?) "
                            f"WHERE outbox_id IN ({placeholders}) "
                            f"AND presentation='unseen'",
                            (now_text, *presentation_values),
                        )
                    return projection
                self._assert_reply_wire_ids_available_tx(
                    conn,
                    primary_client_id=client_value,
                    contextless_client_id=contextless_value,
                    owner_outbox_id=outbox_value,
                )
                conn.execute(
                    """INSERT OR IGNORE INTO user_outbox
                       (outbox_id, event_id, task_id, channel, bot_id,
                        external_user_id, session_id, agent_id, source_message_id,
                        source_sequence, context_token, reply_target_json, content,
                        attachments_json, priority, delivery_mode, notify_enabled,
                        foreground, state, presentation, client_id, attempts, created_at,
                        from_user_id, contextless_client_id, active_wire_variant)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               'pending', 'unseen', ?, 0, ?, ?, ?, 'primary')""",
                    (
                        outbox_value, event_value, effective_task_value, target.channel,
                        target.bot_id, target.external_user_id, target.session_id,
                        agent_value, target.source_message_id, target.source_sequence,
                        target.context_token, json_dumps(target.to_dict()), str(content_value),
                        json_dumps(list(attachment_values)), priority_value, mode_value,
                        int(notify_value), int(foreground_value), client_value, now_text,
                        from_user_value,
                        (str(contextless_value) if contextless_value is not None else None),
                    ),
                )
                row = conn.execute("SELECT * FROM user_outbox WHERE outbox_id = ?", (outbox_value,)).fetchone()
                if row is None:
                    # A caller may have supplied a client ID that won a race;
                    # return that durable row rather than creating a second
                    # external delivery identity.
                    row = conn.execute("SELECT * FROM user_outbox WHERE client_id = ?", (client_value,)).fetchone()
                if row is None:
                    raise StoreError("user outbox insert failed")
                immutable_outbox = {
                    "outbox_id": outbox_value,
                    "event_id": event_value,
                    "task_id": effective_task_value,
                    "channel": target.channel,
                    "bot_id": target.bot_id,
                    "external_user_id": target.external_user_id,
                    "session_id": target.session_id,
                    "agent_id": agent_value,
                    "content": str(content_value),
                    "client_id": client_value,
                    "source_message_id": target.source_message_id,
                    "source_sequence": target.source_sequence,
                    "context_token": target.context_token,
                    "priority": priority_value,
                    "delivery_mode": mode_value,
                    "foreground": foreground_value,
                }
                for column, expected in immutable_outbox.items():
                    # ``context_token`` is supplied by the transport and may
                    # be refreshed on replay.  The durable destination and
                    # command identity are all of the other target fields;
                    # retain the first committed token as the send hint.
                    if column == "context_token":
                        continue
                    if column == "priority":
                        equal = int(row[column]) == int(expected)
                    elif column == "foreground":
                        equal = int(row[column]) == int(expected)
                    elif column == "source_sequence":
                        equal = (
                            row[column] is None
                            and expected is None
                        ) or (
                            row[column] is not None
                            and expected is not None
                            and int(row[column]) == int(expected)
                        )
                    else:
                        equal = str(row[column] or "") == str(expected or "")
                    if not equal:
                        raise StoreError(f"outbox identity conflicts: {outbox_value} ({column})")
                stored_target = json_loads(row["reply_target_json"], {}) or {}
                if self._reply_target_snapshot(stored_target) != self._reply_target_snapshot(
                    target.to_dict()
                ):
                    raise StoreError(
                        f"outbox identity conflicts: {outbox_value} (reply_target)"
                    )
                stored_attachments = json_loads(row["attachments_json"], []) or []
                if self._json_snapshot(stored_attachments) != self._json_snapshot(
                    list(attachment_values)
                ):
                    raise StoreError(f"outbox identity conflicts: {outbox_value} (attachments)")
                if presentation_values:
                    placeholders = ",".join("?" for _ in presentation_values)
                    presentation_rows = conn.execute(
                        f"SELECT outbox_id, channel, bot_id, external_user_id, "
                        f"session_id FROM user_outbox WHERE outbox_id IN ({placeholders})",
                        presentation_values,
                    ).fetchall()
                    if len(presentation_rows) != len(presentation_values):
                        raise StoreError(
                            "command presentation references an unavailable outbox item"
                        )
                    expected_scope = (
                        str(target.channel),
                        str(target.bot_id),
                        str(target.external_user_id),
                        str(target.session_id or "default"),
                    )
                    for presentation_row in presentation_rows:
                        actual_scope = (
                            str(presentation_row["channel"] or ""),
                            str(presentation_row["bot_id"] or ""),
                            str(presentation_row["external_user_id"] or ""),
                            str(presentation_row["session_id"] or "default"),
                        )
                        if actual_scope != expected_scope:
                            raise StoreError(
                                "command presentation conflicts with user/session ownership"
                            )
                    conn.execute(
                        f"UPDATE user_outbox SET presentation='presented', "
                        f"presented_at=COALESCE(presented_at, ?) "
                        f"WHERE outbox_id IN ({placeholders}) "
                        f"AND presentation='unseen'",
                        (now_text, *presentation_values),
                    )
                durable = self._outbox_from_row(row)
                if durable is None:
                    raise StoreError("user outbox row is malformed")
                # Immediate command responses and channel adapters can carry
                # managed attachment IDs without passing through a task event.
                # Materialize those IDs into the same durable media lifecycle
                # used by terminal task projections.  Unknown/channel-only
                # references remain in ``attachments_json`` and are handled by
                # the channel adapter without weakening SQLite FKs.
                for ordinal, raw_attachment in enumerate(durable.attachments):
                    attachment_id, supplied_metadata = self._attachment_value(raw_attachment)
                    if not attachment_id:
                        continue
                    attachment_row = conn.execute(
                        "SELECT kind, mime_type, size_bytes, metadata_json, state "
                        "FROM attachments WHERE attachment_id=?",
                        (attachment_id,),
                    ).fetchone()
                    if attachment_row is None or str(attachment_row["state"] or "ready") == "blocked_media":
                        continue
                    if str(attachment_row["state"] or "ready") != "ready":
                        raise StoreError(f"attachment is unavailable: {attachment_id}")
                    metadata = json_loads(attachment_row["metadata_json"], {}) or {}
                    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                    metadata.update(
                        {
                            "kind": attachment_row["kind"],
                            "mime_type": attachment_row["mime_type"],
                            "size_bytes": int(attachment_row["size_bytes"] or 0),
                        }
                    )
                    if isinstance(supplied_metadata, Mapping):
                        metadata.update(
                            {
                                str(key): value
                                for key, value in supplied_metadata.items()
                                if str(key).lower() not in _ATTACHMENT_AUTHORITY_KEYS
                            }
                        )
                    metadata["owner_agent_id"] = durable.agent_id
                    metadata["session_id"] = durable.session_id
                    metadata["context_token"] = durable.reply_target.context_token
                    idem = f"outbox:{durable.outbox_id}:attachment:{ordinal}:{attachment_id}"
                    media_id = str(
                        uuid.uuid5(uuid.NAMESPACE_URL, "codex-outgoing-media:" + idem)
                    )
                    conn.execute(
                        """INSERT OR IGNORE INTO outgoing_media
                           (media_id, attachment_id, outbox_id, channel, bot_id,
                            external_user_id, state, idempotency_key, metadata_json,
                            attempts, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, 'ready', ?, ?, 0, ?, ?)""",
                        (
                            media_id,
                            attachment_id,
                            durable.outbox_id,
                            durable.channel,
                            durable.bot_id,
                            durable.external_user_id,
                            idem,
                            json_dumps(metadata),
                            _utc_text(durable.created_at),
                            _utc_text(durable.created_at),
                        ),
                    )
                    conn.execute(
                        """INSERT OR IGNORE INTO attachment_refs
                           (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                           VALUES ('outbox', ?, ?, 'outgoing_media', ?, ?)""",
                        (
                            durable.outbox_id,
                            attachment_id,
                            ordinal,
                            _utc_text(durable.created_at),
                        ),
                    )
                return durable

        durable = await self._call(op)
        if isinstance(durable, ReplyProjectionResult):
            if not durable.outbox_items:
                # The candidate and FIFO fragment committed successfully, but
                # the source scope has no remaining wire slot.  Return that
                # typed, durable outcome so a channel adapter can acknowledge
                # the inbound without manufacturing an eleventh send or
                # treating successful deferral as a persistence failure.
                return durable
            selected = next(
                (
                    item
                    for item in durable.outbox_items
                    if item.outbox_id == outbox_value
                ),
                durable.outbox_items[0],
            )
            if selected.outbox_id != outbox_value:
                raise StoreError(
                    f"outbox identity conflicts: {outbox_value} (outbox_id)"
                )
            if selected.client_id != client_value:
                raise StoreError(
                    f"outbox identity conflicts: {outbox_value} (client_id)"
                )
            return selected
        return durable

    enqueue_user_outbox = create_user_outbox
    add_user_outbox = create_user_outbox
    create_outbox = create_user_outbox

    async def list_outbox(
        self,
        *,
        channel: str | None = None,
        bot_id: str | None = None,
        external_user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        states: Iterable[OutboxState | str] | None = None,
        unseen: bool | None = None,
        limit: int = 100,
        include_silent: bool = True,
    ) -> list[UserOutboxItem]:
        filters: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("channel", channel), ("bot_id", bot_id), ("external_user_id", external_user_id),
            ("session_id", session_id), ("agent_id", agent_id),
        ):
            if value is not None:
                filters.append(f"{column} = ?")
                params.append(value)
        if states is not None:
            values = [_enum_value(item) for item in states]
            if not values:
                return []
            filters.append("state IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        if unseen is True:
            filters.append("presentation = 'unseen'")
        elif unseen is False:
            filters.append("presentation <> 'unseen'")
        if not include_silent:
            filters.append("priority > 0")
        where = " WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(0, int(limit)))
        def op(conn: sqlite3.Connection) -> list[UserOutboxItem]:
            rows = conn.execute(
                f"SELECT * FROM user_outbox{where} ORDER BY priority DESC, created_at ASC, outbox_id ASC LIMIT ?",
                params,
            ).fetchall()
            return [self._outbox_from_row(row) for row in rows]
        return await self._call(op)

    get_outbox = list_outbox
    list_user_outbox = list_outbox

    async def get_outbox_item(self, outbox_id: str) -> UserOutboxItem | None:
        """Return one durable user projection by its stable delivery ID."""

        def op(conn: sqlite3.Connection) -> UserOutboxItem | None:
            row = conn.execute(
                "SELECT * FROM user_outbox WHERE outbox_id=?", (str(outbox_id),)
            ).fetchone()
            return self._outbox_from_row(row)

        return await self._call(op)

    get_user_outbox_item = get_outbox_item
    get_delivery = get_outbox_item

    async def claim_outbox(
        self,
        worker_id: str,
        *,
        channel: str | None = None,
        bot_id: str | None = None,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
        agent_id: str | None = None,
        include_silent: bool = True,
        automatic: bool = True,
        has_attachments: bool | None = None,
        canonical_only: bool = False,
    ) -> list[UserOutboxItem]:
        if int(limit) <= 0:
            return []
        def op(conn: sqlite3.Connection) -> list[UserOutboxItem]:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                filters = [
                    "state IN ('pending','retry_wait')",
                    "(next_attempt_at IS NULL OR next_attempt_at <= ?)",
                    # Legacy unscoped rows remain compatible.  Every v19
                    # candidate row, however, becomes claimable only after a
                    # canonical reply slot exists.
                    "(reply_candidate_id IS NULL OR reply_slot_id IS NOT NULL)",
                    """(reply_scope_id IS NULL OR NOT EXISTS (
                           SELECT 1 FROM user_outbox AS predecessor
                           WHERE predecessor.reply_scope_id=user_outbox.reply_scope_id
                             AND predecessor.reply_ordinal < user_outbox.reply_ordinal
                             AND predecessor.state NOT IN (
                                 'sent','failed_permanent','delivery_unknown'
                             )
                       ))""",
                ]
                params: list[Any] = [now_text]
                if channel is not None:
                    filters.append("channel = ?"); params.append(channel)
                if bot_id is not None:
                    filters.append("bot_id = ?"); params.append(bot_id)
                if agent_id is not None:
                    filters.append("agent_id = ?"); params.append(agent_id)
                kind_guards: list[str] = []
                if has_attachments is True:
                    kind_guards.append(
                        "COALESCE(attachments_json, '[]') <> '[]'"
                    )
                elif has_attachments is False:
                    kind_guards.append(
                        "COALESCE(attachments_json, '[]') = '[]'"
                    )
                if canonical_only:
                    kind_guards.append("reply_slot_id IS NOT NULL")
                filters.extend(kind_guards)
                if not include_silent:
                    filters.append("priority > 0")
                if automatic:
                    filters.append("notify_enabled = 1")
                    filters.append("delivery_mode <> 'inbox_only'")
                    # Presentation is an inbox/read projection, independent
                    # from transport delivery. Explicit inbox presentation
                    # must not make a pending external send disappear.
                rows = conn.execute(
                    "SELECT * FROM user_outbox WHERE " + " AND ".join(filters) +
                    " ORDER BY priority DESC, created_at ASC, "
                    "COALESCE(reply_ordinal, 0) ASC, outbox_id ASC LIMIT ?",
                    [*params, max(1, int(limit))],
                ).fetchall()
                claimed: list[UserOutboxItem] = []
                for row in rows:
                    token = _uuid()
                    kind_guard_sql = (
                        " AND " + " AND ".join(kind_guards)
                        if kind_guards
                        else ""
                    )
                    changed = conn.execute(
                        f"""UPDATE user_outbox SET state='claimed', claimed_by=?,
                               claim_token=?, lease_expires_at=?, attempts=attempts+1
                           WHERE outbox_id=? AND state IN ('pending','retry_wait')
                             AND (reply_candidate_id IS NULL OR reply_slot_id IS NOT NULL)
                             AND (reply_scope_id IS NULL OR NOT EXISTS (
                                 SELECT 1 FROM user_outbox AS predecessor
                                 WHERE predecessor.reply_scope_id=user_outbox.reply_scope_id
                                   AND predecessor.reply_ordinal < user_outbox.reply_ordinal
                                   AND predecessor.state NOT IN (
                                       'sent','failed_permanent','delivery_unknown'
                                   )
                             )){kind_guard_sql}""",
                        (worker_id, token, lease, row["outbox_id"]),
                    ).rowcount
                    if not changed:
                        continue
                    fresh = conn.execute("SELECT * FROM user_outbox WHERE outbox_id=?", (row["outbox_id"],)).fetchone()
                    if fresh is not None:
                        claimed.append(self._outbox_from_row(fresh))
                return claimed
        return await self._call(op)

    async def claim_text_outbox(self, worker_id: str, **kwargs: Any) -> list[UserOutboxItem]:
        """Claim canonical/legacy text rows while leaving media bundles alone."""

        return await self.claim_outbox(
            worker_id,
            has_attachments=False,
            **kwargs,
        )

    async def claim_media_outbox(
        self,
        worker_id: str,
        **kwargs: Any,
    ) -> list[UserOutboxItem]:
        """Claim slot-owned media bundles through their sole SendMsg owner."""

        return await self.claim_outbox(
            worker_id,
            has_attachments=True,
            canonical_only=True,
            **kwargs,
        )

    claim_next_outbox = claim_outbox
    claim_user_outbox = claim_outbox

    async def mark_outbox_sending(
        self,
        outbox_id: str,
        claim_token: str | None = None,
        *,
        allow_pending: bool = False,
        worker_id: str | None = None,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
    ) -> bool:
        """Move a claimed outbox row to ``sending`` with a recovery lease.

        Direct command responses are sometimes persisted and sent before the
        normal delivery worker gets a chance to claim them.  Those callers use
        ``allow_pending=True``; giving that path the same claim token/lease as
        a worker prevents a crash between the external send and its durable
        acknowledgement from leaving an un-recoverable ``sending`` row.
        """
        generated_token = str(claim_token or _uuid())
        owner = str(worker_id or "direct-send")

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                # A caller without a token may atomically claim only a
                # pending direct-send row.  Never let it hijack a row already
                # owned by a delivery worker; worker paths must provide their
                # persisted claim token.
                if allow_pending and claim_token is None:
                    states = "'pending','retry_wait'"
                else:
                    states = "'claimed'"
                filters = f"outbox_id = ? AND state IN ({states})"
                params: list[Any] = [outbox_id]
                if claim_token is not None:
                    filters += " AND claim_token = ?"; params.append(claim_token)
                    filters += " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                    params.append(now_text)
                elif not allow_pending:
                    # Claimed rows always have an owner token.  Refuse a
                    # tokenless transition rather than letting a stale
                    # callback take over a worker's send.
                    filters += " AND 0"
                # Preserve an existing worker claim.  A pending direct send
                # receives a fresh ownership token and expiry; this is a
                # single conditional UPDATE so no second sender can slip in.
                changed = conn.execute(
                    "UPDATE user_outbox SET state='sending', "
                    "claimed_by=CASE WHEN state IN ('pending','retry_wait') THEN ? ELSE claimed_by END, "
                    "claim_token=CASE WHEN state IN ('pending','retry_wait') THEN ? ELSE claim_token END, "
                    "lease_expires_at=CASE WHEN state IN ('pending','retry_wait') THEN ? ELSE lease_expires_at END, "
                    "attempts=CASE WHEN state IN ('pending','retry_wait') THEN attempts+1 ELSE attempts END "
                    "WHERE " + filters,
                    [owner, generated_token, lease, *params],
                ).rowcount
                return changed == 1
        return await self._call(op)

    async def activate_outbox_contextless_variant(
        self,
        outbox_id: str,
        claim_token: str | None = None,
        *,
        contextless_client_id: str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        """Fence the sole permitted primary-to-contextless wire transition.

        The active claim and lease are retained.  Replaying the exact same
        transition is idempotent; a stale owner, expired lease, different
        alternate ID, or any attempt to rewrite the primary identity fails.
        """

        token = str(claim_token or "").strip()

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                row = conn.execute(
                    "SELECT * FROM user_outbox WHERE outbox_id=?",
                    (str(outbox_id),),
                ).fetchone()
                if row is None:
                    return False
                token_owner = bool(token) and (
                    str(row["state"])
                    in {OutboxState.CLAIMED.value, OutboxState.SENDING.value}
                    and str(row["claim_token"] or "") == token
                )
                direct_owner = (
                    not token
                    and str(row["state"]) == OutboxState.SENDING.value
                    and str(row["claimed_by"] or "") == "direct-send"
                )
                if (
                    not (token_owner or direct_owner)
                    or not self._lease_is_active(row["lease_expires_at"], now_text)
                ):
                    return False
                existing_alternate = str(row["contextless_client_id"] or "")
                desired = str(contextless_client_id or existing_alternate or "")
                if not desired:
                    identity = str(
                        row["reply_fragment_id"] or row["reply_slot_id"] or outbox_id
                    )
                    _primary, desired = self._reply_wire_ids(identity)
                if desired == str(row["client_id"]):
                    raise StoreError(
                        "contextless client ID must differ from primary client ID"
                    )
                if existing_alternate and existing_alternate != desired:
                    raise StoreError(
                        "contextless wire identity conflicts with its first value"
                    )
                if str(row["active_wire_variant"] or "primary") == "contextless":
                    return True
                self._assert_reply_wire_ids_available_tx(
                    conn,
                    primary_client_id=str(row["client_id"]),
                    contextless_client_id=desired,
                    owner_outbox_id=str(outbox_id),
                    owner_reply_slot_id=(
                        str(row["reply_slot_id"])
                        if row["reply_slot_id"] is not None
                        else None
                    ),
                )
                slot = None
                if row["reply_slot_id"] is not None:
                    slot = conn.execute(
                        "SELECT * FROM reply_slots WHERE reply_slot_id=?",
                        (row["reply_slot_id"],),
                    ).fetchone()
                    if slot is None:
                        raise StoreError("canonical reply outbox has no slot")
                    if str(slot["client_id"]) != str(row["client_id"]):
                        raise StoreError("reply slot primary wire identity conflicts")
                    slot_alternate = str(slot["contextless_client_id"] or "")
                    if slot_alternate and slot_alternate != desired:
                        raise StoreError(
                            "reply slot contextless wire identity conflicts"
                        )
                    if str(slot["active_wire_variant"] or "primary") != "primary":
                        raise StoreError("reply slot wire variant conflicts with outbox")
                owner_filter = (
                    "claim_token=? AND state IN ('claimed','sending')"
                    if token
                    else "claimed_by='direct-send' AND state='sending'"
                )
                owner_params: tuple[Any, ...] = (token,) if token else ()
                changed = conn.execute(
                    "UPDATE user_outbox SET contextless_client_id=?, "
                    "active_wire_variant='contextless' WHERE outbox_id=? AND "
                    + owner_filter
                    + " AND lease_expires_at IS NOT NULL AND lease_expires_at>? "
                    "AND active_wire_variant='primary' "
                    "AND (contextless_client_id IS NULL OR contextless_client_id=?)",
                    (desired, outbox_id, *owner_params, now_text, desired),
                ).rowcount
                if changed != 1:
                    return False
                if slot is not None:
                    slot_changed = conn.execute(
                        """UPDATE reply_slots
                           SET contextless_client_id=?,
                               active_wire_variant='contextless'
                           WHERE reply_slot_id=? AND active_wire_variant='primary'
                             AND (contextless_client_id IS NULL
                                  OR contextless_client_id=?)""",
                        (desired, slot["reply_slot_id"], desired),
                    ).rowcount
                    if slot_changed != 1:
                        raise StoreError(
                            "reply slot contextless transition was lost"
                        )
                return True

        try:
            return await self._call(op)
        except sqlite3.IntegrityError as exc:
            raise StoreError(str(exc)) from exc

    activate_delivery_contextless_variant = activate_outbox_contextless_variant

    async def mark_outbox_sent(
        self,
        outbox_id: str,
        claim_token: str | None = None,
        *,
        client_id: str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = "outbox_id = ? AND state = 'sending'"
                params: list[Any] = [outbox_id]
                if claim_token is not None:
                    filters += " AND claim_token = ?"; params.append(claim_token)
                else:
                    # The only tokenless sender is the synchronous command
                    # path, which is marked ``direct-send`` above.  A stale
                    # callback must not finalize a row currently owned by a
                    # background delivery worker.
                    filters += " AND claimed_by = 'direct-send'"
                filters += " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                params.append(now_text)
                if client_id is not None:
                    filters += (
                        " AND ((active_wire_variant='primary' AND client_id=?) "
                        "OR (active_wire_variant='contextless' "
                        "AND contextless_client_id=?))"
                    )
                    params.extend((client_id, client_id))
                changed = conn.execute(
                    "UPDATE user_outbox SET state='sent', sent_at=?, lease_expires_at=NULL, claim_token=NULL, claimed_by=NULL WHERE " + filters,
                    [now_text, *params],
                ).rowcount == 1
                if changed and claim_token is not None:
                    conn.execute(
                        """UPDATE outgoing_media
                           SET lease_expires_at=NULL, claim_token=NULL,
                               claimed_by=NULL, updated_at=?
                           WHERE outbox_id=? AND claim_token=?""",
                        (now_text, outbox_id, claim_token),
                    )
                return changed
        return await self._call(op)

    complete_outbox = mark_outbox_sent
    mark_delivery_sent = mark_outbox_sent

    async def mark_outbox_failed(
        self,
        outbox_id: str,
        claim_token: str | None = None,
        *,
        error: str = "",
        last_error: str | None = None,
        retry: bool = True,
        permanent: bool = False,
        delay: float = 5.0,
        now: datetime | str | None = None,
    ) -> bool:
        state = OutboxState.FAILED_PERMANENT if permanent or not retry else OutboxState.RETRY_WAIT
        message = last_error if last_error is not None else error
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                next_at = (
                    now_text
                    if state == OutboxState.RETRY_WAIT and delay <= 0
                    else self._lease_deadline(now_text, delay)
                    if state == OutboxState.RETRY_WAIT
                    else None
                )
                filters = "outbox_id = ? AND state = 'sending'"
                params: list[Any] = [outbox_id]
                if claim_token is not None:
                    filters += " AND claim_token = ?"; params.append(claim_token)
                else:
                    filters += " AND claimed_by = 'direct-send'"
                filters += " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                params.append(now_text)
                changed = conn.execute(
                    "UPDATE user_outbox SET state=?, next_attempt_at=?, last_error=?, lease_expires_at=NULL, claim_token=NULL, claimed_by=NULL WHERE " + filters,
                    [state.value, next_at, message, *params],
                ).rowcount == 1
                if changed and claim_token is not None:
                    # Preserve uploaded/send-pending state for the retry while
                    # releasing the subordinate lease in the same parent
                    # failure transaction.  It cannot be reclaimed unless a
                    # future canonical parent claim succeeds first.
                    conn.execute(
                        """UPDATE outgoing_media
                           SET lease_expires_at=NULL, claim_token=NULL,
                               claimed_by=NULL, updated_at=?
                           WHERE outbox_id=? AND claim_token=?""",
                        (now_text, outbox_id, claim_token),
                    )
                return changed
        return await self._call(op)

    fail_outbox = mark_outbox_failed
    mark_delivery_failed = mark_outbox_failed

    async def retry_outbox(
        self, outbox_id: str, *, now: datetime | str | None = None
    ) -> bool:
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                return conn.execute(
                    """UPDATE user_outbox SET state='pending', next_attempt_at=?,
                           last_error=NULL WHERE outbox_id=? AND state IN ('delivery_unknown','failed_permanent')""",
                    (now_text, outbox_id),
                ).rowcount == 1
        return await self._call(op)

    # ------------------------------------------------------------------
    # Outgoing media upload/send lifecycle
    async def create_outgoing_media(
        self,
        *,
        attachment_id: str,
        channel: str,
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str | None = None,
        agent_id: str | None = None,
        outbox_id: str | None = None,
        media_id: str | None = None,
        idempotency_key: str | None = None,
        state: MediaDeliveryState | str = MediaDeliveryState.READY,
        metadata: Mapping[str, Any] | None = None,
        now: datetime | str | None = None,
    ) -> OutgoingMediaRecord:
        """Create one durable outgoing-media operation.

        Binary data remains in ``attachments``; this row contains only the
        upload/send projection and stable retry identity.  Repeating a call
        with the same idempotency key returns the original row.
        """
        aid = str(attachment_id).strip()
        if not aid:
            raise ValueError("attachment_id is required")
        mid = str(media_id or _uuid())
        idem = str(idempotency_key or f"media:{channel}:{bot_id}:{external_user_id}:{outbox_id or ''}:{aid}")
        state_value = str(_enum_value(state, MediaDeliveryState.READY.value))
        if state_value not in {item.value for item in MediaDeliveryState}:
            raise ValueError(f"invalid outgoing media state: {state_value}")
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> OutgoingMediaRecord:
            with _transaction(conn):
                # Resolve an immutable replay before consulting the current
                # attachment lifecycle.  A successfully sent operation may
                # outlive cleanup of its local bytes, but its idempotency key
                # must continue to return the original durable result.
                existing = conn.execute(
                    "SELECT * FROM outgoing_media WHERE idempotency_key=?", (idem,)
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["attachment_id"]) != aid
                        or str(existing["channel"]) != str(channel)
                        or str(existing["bot_id"] or "") != str(bot_id)
                        or str(existing["external_user_id"] or "")
                        != str(external_user_id)
                        or str(existing["outbox_id"] or "")
                        != str(outbox_id or "")
                    ):
                        raise StoreError(
                            f"outgoing media idempotency key is immutable: {idem}"
                        )
                    self._authorize_outgoing_media_replay_tx(
                        conn,
                        existing,
                        agent_id=agent_id,
                        outbox_id=outbox_id,
                        session_id=session_id,
                        channel=str(channel),
                        bot_id=str(bot_id),
                        external_user_id=str(external_user_id),
                    )
                    item = self._outgoing_media_from_row(existing)
                    if item is None:
                        raise StoreError("outgoing media row is malformed")
                    return item

                attachment_row = conn.execute(
                    "SELECT kind, mime_type, size_bytes, metadata_json, state "
                    "FROM attachments WHERE attachment_id=?",
                    (aid,),
                ).fetchone()
                if attachment_row is None:
                    raise NotFoundError(f"attachment not found: {aid}")
                if str(attachment_row["state"] or "ready") != "ready":
                    raise StoreError(f"attachment access denied: {aid}")
                resolved_agent = str(agent_id or "").strip()
                if outbox_id and not resolved_agent:
                    raise StoreError(
                        "agent_id is required for outbox media operations"
                    )
                outbox_task_id: str | None = None
                if outbox_id:
                    outbox_row = conn.execute(
                        "SELECT channel, bot_id, external_user_id, session_id, agent_id, task_id "
                        "FROM user_outbox "
                        "WHERE outbox_id=?",
                        (outbox_id,),
                    ).fetchone()
                    if outbox_row is None:
                        raise NotFoundError(f"outbox not found: {outbox_id}")
                    if (
                        str(outbox_row["channel"]) != str(channel)
                        or str(outbox_row["bot_id"]) != str(bot_id)
                        or str(outbox_row["external_user_id"]) != str(external_user_id)
                        or (
                            session_id is not None
                            and str(outbox_row["session_id"] or "default")
                            != str(session_id or "default")
                        )
                    ):
                        raise StoreError("outgoing media target does not match its outbox")
                    outbox_agent = str(outbox_row["agent_id"] or "").strip()
                    if resolved_agent and outbox_agent and resolved_agent != outbox_agent:
                        raise StoreError("outgoing media Agent conflicts with its outbox")
                    resolved_agent = resolved_agent or outbox_agent
                    outbox_task_id = str(outbox_row["task_id"] or "") or None

                # Validate the durable attachment before an outgoing-media
                # row or retention ref can be created.  Unowned legacy rows
                # remain adoptable on first use; rows with metadata or prior
                # refs require an explicit Agent ACL (or the owning outbox).
                attachment_metadata = (
                    json_loads(attachment_row["metadata_json"], {}) or {}
                )
                has_owner_metadata = isinstance(attachment_metadata, Mapping) and bool(
                    attachment_metadata.get("owner_agent_id")
                    or attachment_metadata.get("agent_id")
                    or attachment_metadata.get("channel")
                    or attachment_metadata.get("bot_id")
                    or attachment_metadata.get("external_user_id")
                    or attachment_metadata.get("user_id")
                )
                has_refs = conn.execute(
                    "SELECT 1 FROM attachment_refs WHERE attachment_id=? LIMIT 1",
                    (aid,),
                ).fetchone() is not None
                if not resolved_agent and (has_owner_metadata or has_refs):
                    # Ownership is not authentication.  Even when a durable
                    # ref identifies one Agent, a caller that omits its own
                    # Agent identity cannot prove it is allowed to extend or
                    # replay that lease.  Only an explicit ``agent_id`` or a
                    # verified owning ``outbox_id`` above may authorize it.
                    raise StoreError(f"attachment access denied: {aid}")
                self._validate_task_input_attachment_access_tx(
                    conn,
                    task_id=outbox_task_id,
                    inputs={"attachment_id": aid},
                    agent_id=resolved_agent,
                    channel=str(channel or ""),
                    bot_id=str(bot_id or ""),
                    external_user_id=str(external_user_id or ""),
                    session_id=str(session_id or "default"),
                    require_registered=True,
                )
                media_metadata = (
                    dict(attachment_metadata)
                    if isinstance(attachment_metadata, Mapping)
                    else {}
                )
                media_metadata.update(
                    {
                        "kind": attachment_row["kind"],
                        "mime_type": attachment_row["mime_type"],
                        "size_bytes": int(attachment_row["size_bytes"] or 0),
                    }
                )
                if resolved_agent:
                    # Persist the immutable operation principal separately
                    # from caller-provided transport hints so a later replay
                    # can be authorized even after local cleanup.
                    media_metadata["owner_agent_id"] = resolved_agent
                media_metadata["session_id"] = str(
                    session_id or "default"
                )
                media_metadata.update(
                    {
                        str(key): value
                        for key, value in (metadata or {}).items()
                        if str(key).lower() not in _ATTACHMENT_AUTHORITY_KEYS
                    }
                )
                # A caller-supplied media ID is also an immutable operation
                # identity.  Do not let it silently alias a different
                # idempotency key.
                same_media_id = conn.execute(
                    "SELECT idempotency_key FROM outgoing_media WHERE media_id=?",
                    (mid,),
                ).fetchone()
                if same_media_id is not None and str(same_media_id["idempotency_key"]) != idem:
                    raise StoreError(f"outgoing media ID is immutable: {mid}")
                try:
                    conn.execute(
                        """INSERT INTO outgoing_media
                           (media_id, attachment_id, outbox_id, channel, bot_id,
                            external_user_id, state, idempotency_key, metadata_json,
                            attempts, created_at, updated_at, uploaded_at, sent_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?,
                                   CASE WHEN ? IN ('uploaded','send_pending','sent') THEN ? ELSE NULL END,
                                   CASE WHEN ? = 'sent' THEN ? ELSE NULL END)""",
                        (
                            mid,
                            aid,
                            outbox_id,
                            channel,
                            bot_id,
                            external_user_id,
                            state_value,
                            idem,
                            json_dumps(media_metadata),
                            now_text,
                            now_text,
                            state_value,
                            now_text,
                            state_value,
                            now_text,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # Another writer may have won the idempotency race.  Read
                    # its durable row and return it when the identity matches;
                    # normalize unrelated uniqueness violations to StoreError.
                    winner = conn.execute(
                        "SELECT * FROM outgoing_media WHERE idempotency_key=?",
                        (idem,),
                    ).fetchone()
                    if winner is not None:
                        if (
                            str(winner["attachment_id"]) != aid
                            or str(winner["channel"]) != str(channel)
                            or str(winner["bot_id"] or "") != str(bot_id)
                            or str(winner["external_user_id"] or "") != str(external_user_id)
                            or str(winner["outbox_id"] or "") != str(outbox_id or "")
                        ):
                            raise StoreError(f"outgoing media idempotency key is immutable: {idem}") from exc
                        item = self._outgoing_media_from_row(winner)
                        if item is not None:
                            return item
                    raise StoreError(str(exc)) from exc
                item = self._outgoing_media_from_row(
                    conn.execute("SELECT * FROM outgoing_media WHERE media_id=?", (mid,)).fetchone()
                )
                if item is None:
                    raise StoreError("outgoing media insert failed")
                return item

        return await self._call(op)

    enqueue_outgoing_media = create_outgoing_media
    create_media_delivery = create_outgoing_media

    async def get_outgoing_media(self, media_id: str) -> OutgoingMediaRecord | None:
        return await self._call(
            lambda conn: self._outgoing_media_from_row(
                conn.execute("SELECT * FROM outgoing_media WHERE media_id=?", (media_id,)).fetchone()
            )
        )

    get_media_delivery = get_outgoing_media

    async def list_outgoing_media(
        self,
        *,
        states: Iterable[MediaDeliveryState | str] | None = None,
        outbox_id: str | None = None,
        limit: int = 100,
    ) -> list[OutgoingMediaRecord]:
        filters: list[str] = []
        params: list[Any] = []
        if states is not None:
            values = [str(_enum_value(item)) for item in states]
            if not values:
                return []
            filters.append("state IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        if outbox_id is not None:
            filters.append("outbox_id=?")
            params.append(outbox_id)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(0, int(limit)))

        def op(conn: sqlite3.Connection) -> list[OutgoingMediaRecord]:
            rows = conn.execute(
                f"SELECT * FROM outgoing_media{where} ORDER BY created_at ASC, media_id ASC LIMIT ?",
                params,
            ).fetchall()
            return [item for row in rows if (item := self._outgoing_media_from_row(row)) is not None]

        return await self._call(op)

    list_media_deliveries = list_outgoing_media

    async def list_outgoing_media_for_outbox(
        self,
        *,
        outbox_id: str,
        outbox_claim_token: str,
        limit: int = 100,
        now: datetime | str | None = None,
    ) -> list[OutgoingMediaRecord]:
        """Read subordinate upload rows only for an active canonical claim."""

        parent_id = str(outbox_id or "").strip()
        token = str(outbox_claim_token or "").strip()
        if not parent_id or not token:
            raise ValueError("outbox_id and outbox_claim_token are required")

        def op(conn: sqlite3.Connection) -> list[OutgoingMediaRecord]:
            now_text = self._now(now)
            parent = conn.execute(
                """SELECT * FROM user_outbox
                   WHERE outbox_id=? AND claim_token=?
                     AND reply_slot_id IS NOT NULL
                     AND state IN ('claimed','sending')
                     AND lease_expires_at IS NOT NULL
                     AND lease_expires_at>?""",
                (parent_id, token, now_text),
            ).fetchone()
            if parent is None:
                return []
            rows = conn.execute(
                """SELECT * FROM outgoing_media
                   WHERE outbox_id=? AND claim_token=?
                     AND lease_expires_at IS NOT NULL
                     AND lease_expires_at>?
                   ORDER BY created_at ASC, media_id ASC LIMIT ?""",
                (parent_id, token, now_text, max(0, int(limit))),
            ).fetchall()
            values = [
                item
                for row in rows
                if (item := self._outgoing_media_from_row(row, parent)) is not None
            ]

            def bundle_order(item: OutgoingMediaRecord) -> tuple[int, str]:
                try:
                    ordinal = int(item.metadata.get("bundle_ordinal", 1 << 30))
                except (TypeError, ValueError):
                    ordinal = 1 << 30
                return ordinal, item.media_id

            return sorted(values, key=bundle_order)

        return await self._call(op)

    async def get_outgoing_media_for_outbox(
        self,
        media_id: str,
        *,
        outbox_id: str,
        outbox_claim_token: str,
        now: datetime | str | None = None,
    ) -> OutgoingMediaRecord | None:
        """Return one child overlaid with its actively claimed parent."""

        parent_id = str(outbox_id or "").strip()
        token = str(outbox_claim_token or "").strip()
        if not parent_id or not token:
            raise ValueError("outbox_id and outbox_claim_token are required")

        def op(conn: sqlite3.Connection) -> OutgoingMediaRecord | None:
            now_text = self._now(now)
            parent = conn.execute(
                """SELECT * FROM user_outbox
                   WHERE outbox_id=? AND claim_token=?
                     AND reply_slot_id IS NOT NULL
                     AND state IN ('claimed','sending')
                     AND lease_expires_at IS NOT NULL
                     AND lease_expires_at>?""",
                (parent_id, token, now_text),
            ).fetchone()
            if parent is None:
                return None
            child = conn.execute(
                """SELECT * FROM outgoing_media
                   WHERE media_id=? AND outbox_id=? AND claim_token=?
                     AND lease_expires_at IS NOT NULL
                     AND lease_expires_at>?""",
                (str(media_id), parent_id, token, now_text),
            ).fetchone()
            return self._outgoing_media_from_row(child, parent)

        return await self._call(op)

    async def claim_outgoing_media(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
        states: Iterable[MediaDeliveryState | str] | None = None,
        channel: str | None = None,
        bot_id: str | None = None,
        scoped: bool | None = False,
    ) -> list[OutgoingMediaRecord]:
        """Claim media eligible for automatic channel delivery.

        For slot-linked media, this transaction acquires both the upload child
        and its canonical user-outbox parent with the same token and lease.
        The returned record overlays the parent's delivery/client/sender
        identity, so the child cannot authorize an independent SendMsg.
        Legacy unscoped media retains its historical independent claim.
        """
        if int(limit) <= 0:
            return []
        requested_states = (
            (
                MediaDeliveryState.READY,
                MediaDeliveryState.UPLOAD_PENDING,
                MediaDeliveryState.UPLOADING,
                MediaDeliveryState.SEND_PENDING,
                # A process can crash after a successful upload but before
                # persisting ``send_pending``.  Uploaded rows therefore stay
                # claimable and can proceed directly to channel send.
                MediaDeliveryState.UPLOADED,
            )
            if states is None
            else states
        )
        state_values = [
            MediaDeliveryState(str(_enum_value(item))).value
            for item in requested_states
        ]
        if not state_values:
            return []

        def op(conn: sqlite3.Connection) -> list[OutgoingMediaRecord]:
            scope_filters: list[str] = []
            scope_params: list[Any] = []
            if channel is not None:
                scope_filters.append("m.channel=?")
                scope_params.append(str(channel))
            if bot_id is not None:
                scope_filters.append("m.bot_id=?")
                scope_params.append(str(bot_id))
            if scoped is True:
                scope_filters.append("o.reply_slot_id IS NOT NULL")
            elif scoped is False:
                scope_filters.append(
                    "(m.outbox_id IS NULL OR o.reply_slot_id IS NULL)"
                )
            scope_sql = (" AND " + " AND ".join(scope_filters)) if scope_filters else ""
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                placeholders = ",".join("?" for _ in state_values)
                rows = conn.execute(
                    f"""SELECT m.* FROM outgoing_media AS m
                        LEFT JOIN user_outbox AS o ON o.outbox_id=m.outbox_id
                        WHERE m.state IN ({placeholders}){scope_sql}
                          AND (m.next_attempt_at IS NULL OR m.next_attempt_at <= ?)
                          AND (m.claim_token IS NULL OR m.lease_expires_at IS NULL
                               OR m.lease_expires_at <= ?)
                          AND EXISTS (
                              SELECT 1 FROM attachments AS a
                              WHERE a.attachment_id = m.attachment_id
                                AND a.state = 'ready'
                          )
                          AND (
                              (
                                  o.reply_slot_id IS NOT NULL
                                  AND o.state IN ('pending','retry_wait')
                                  AND (o.next_attempt_at IS NULL
                                       OR o.next_attempt_at <= ?)
                                  AND o.notify_enabled=1
                                  AND o.delivery_mode <> 'inbox_only'
                                  AND NOT EXISTS (
                                      SELECT 1 FROM user_outbox AS predecessor
                                      WHERE predecessor.reply_scope_id=o.reply_scope_id
                                        AND predecessor.reply_ordinal < o.reply_ordinal
                                        AND predecessor.state NOT IN (
                                            'sent','failed_permanent','delivery_unknown'
                                        )
                                  )
                              )
                              OR (
                                  (m.outbox_id IS NULL OR o.reply_slot_id IS NULL)
                                  AND (
                                      m.outbox_id IS NULL
                                      OR (
                                          o.notify_enabled=1
                                          AND o.delivery_mode <> 'inbox_only'
                                      )
                                  )
                              )
                          )
                        ORDER BY m.created_at ASC, m.media_id ASC LIMIT ?""",
                    [
                        *state_values,
                        *scope_params,
                        now_text,
                        now_text,
                        now_text,
                        max(1, int(limit)),
                    ],
                ).fetchall()
                result: list[OutgoingMediaRecord] = []
                for row in rows:
                    token = _uuid()
                    parent = None
                    if row["outbox_id"] is not None:
                        parent = conn.execute(
                            "SELECT * FROM user_outbox WHERE outbox_id=?",
                            (row["outbox_id"],),
                        ).fetchone()
                    canonical = bool(
                        parent is not None and parent["reply_slot_id"] is not None
                    )
                    if canonical:
                        parent_changed = conn.execute(
                            """UPDATE user_outbox
                               SET state='claimed', claimed_by=?, claim_token=?,
                                   lease_expires_at=?, attempts=attempts+1
                               WHERE outbox_id=?
                                 AND state IN ('pending','retry_wait')
                                 AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                                 AND reply_slot_id IS NOT NULL
                                 AND notify_enabled=1
                                 AND delivery_mode <> 'inbox_only'
                                 AND NOT EXISTS (
                                     SELECT 1 FROM user_outbox AS predecessor
                                     WHERE predecessor.reply_scope_id=user_outbox.reply_scope_id
                                       AND predecessor.reply_ordinal < user_outbox.reply_ordinal
                                       AND predecessor.state NOT IN (
                                           'sent','failed_permanent','delivery_unknown'
                                       )
                                 )""",
                            (
                                worker_id,
                                token,
                                lease,
                                row["outbox_id"],
                                now_text,
                            ),
                        ).rowcount
                        if parent_changed != 1:
                            continue
                    changed = conn.execute(
                        f"""UPDATE outgoing_media
                            SET state=CASE WHEN state IN ('ready','upload_pending')
                                           THEN 'uploading' ELSE state END,
                                claimed_by=?, claim_token=?,
                                lease_expires_at=?, attempts=attempts+1, updated_at=?
                            WHERE media_id=? AND state IN ({placeholders})
                              AND (claim_token IS NULL OR lease_expires_at IS NULL
                                   OR lease_expires_at <= ?)
                              AND EXISTS (
                                  SELECT 1 FROM attachments AS a
                                  WHERE a.attachment_id = outgoing_media.attachment_id
                                    AND a.state = 'ready'
                              )
                              AND (
                                  ?=0 OR (
                                      outbox_id=? AND EXISTS (
                                          SELECT 1 FROM user_outbox AS parent
                                          WHERE parent.outbox_id=outgoing_media.outbox_id
                                            AND parent.reply_slot_id IS NOT NULL
                                            AND parent.claim_token=?
                                            AND parent.state='claimed'
                                            AND parent.lease_expires_at>?
                                      )
                                  )
                              )""",
                        [
                            worker_id,
                            token,
                            lease,
                            now_text,
                            row["media_id"],
                            *state_values,
                            now_text,
                            int(canonical),
                            row["outbox_id"],
                            token,
                            now_text,
                        ],
                    ).rowcount
                    if changed != 1:
                        if canonical:
                            raise StoreError(
                                "canonical media claim lost its parent transaction"
                            )
                        continue
                    fresh = conn.execute(
                        "SELECT * FROM outgoing_media WHERE media_id=?",
                        (row["media_id"],),
                    ).fetchone()
                    fresh_parent = (
                        conn.execute(
                            "SELECT * FROM user_outbox WHERE outbox_id=?",
                            (row["outbox_id"],),
                        ).fetchone()
                        if canonical
                        else None
                    )
                    item = self._outgoing_media_from_row(fresh, fresh_parent)
                    if item is not None:
                        result.append(item)
                return result

        return await self._call(op)

    claim_media_delivery = claim_outgoing_media

    async def claim_scoped_outgoing_media(
        self,
        worker_id: str,
        **kwargs: Any,
    ) -> list[OutgoingMediaRecord]:
        kwargs.pop("scoped", None)
        return await self.claim_outgoing_media(worker_id, scoped=True, **kwargs)

    async def claim_legacy_outgoing_media(
        self,
        worker_id: str,
        **kwargs: Any,
    ) -> list[OutgoingMediaRecord]:
        kwargs.pop("scoped", None)
        return await self.claim_outgoing_media(worker_id, scoped=False, **kwargs)

    async def transition_outgoing_media(
        self,
        media_id: str,
        to_state: MediaDeliveryState | str,
        *,
        from_states: Iterable[MediaDeliveryState | str] | None = None,
        claim_token: str | None = None,
        outbox_id: str | None = None,
        outbox_claim_token: str | None = None,
        remote_id: str | None = None,
        upload_param: str | None = None,
        encryption_key: str | None = None,
        error: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        parent_id = str(outbox_id or "").strip()
        parent_token = str(outbox_claim_token or "").strip()
        if bool(parent_id) != bool(parent_token):
            raise ValueError(
                "outbox_id and outbox_claim_token must be supplied together"
            )
        if parent_id and claim_token is not None:
            raise ValueError(
                "canonical media transition cannot use two claim authorities"
            )
        target = MediaDeliveryState(str(_enum_value(to_state)))
        transition_map: dict[MediaDeliveryState, frozenset[MediaDeliveryState]] = {
            MediaDeliveryState.LOCAL: frozenset(),
            MediaDeliveryState.READY: frozenset({MediaDeliveryState.LOCAL, MediaDeliveryState.FAILED}),
            MediaDeliveryState.UPLOAD_PENDING: frozenset({MediaDeliveryState.LOCAL, MediaDeliveryState.READY, MediaDeliveryState.FAILED}),
            MediaDeliveryState.UPLOADING: frozenset(
                {MediaDeliveryState.READY, MediaDeliveryState.UPLOAD_PENDING}
            ),
            MediaDeliveryState.UPLOADED: frozenset({MediaDeliveryState.UPLOADING}),
            MediaDeliveryState.SEND_PENDING: frozenset({MediaDeliveryState.UPLOADED}),
            # An upload acknowledgement is not a channel delivery.  Keep the
            # explicit send_pending checkpoint so a crash/restart can
            # distinguish "uploaded but not sent" from a successfully sent
            # item and so callers cannot terminalize an upload without a
            # durable send intent.
            MediaDeliveryState.SENT: frozenset({MediaDeliveryState.SEND_PENDING}),
            MediaDeliveryState.FAILED: frozenset({
                MediaDeliveryState.UPLOADING,
                MediaDeliveryState.UPLOADED,
                MediaDeliveryState.SEND_PENDING,
            }),
        }
        if from_states is None:
            allowed_sources = tuple(item.value for item in transition_map[target])
        else:
            allowed_sources = tuple(str(_enum_value(item)) for item in from_states)
            if any(
                MediaDeliveryState(item) not in transition_map[target]
                for item in allowed_sources
            ):
                raise InvalidTransition(
                    f"invalid outgoing media transition {allowed_sources!r} -> {target.value}"
                )
        if not allowed_sources:
            raise InvalidTransition(f"no outgoing media source state permits {target.value}")
        terminal = target in {MediaDeliveryState.SENT, MediaDeliveryState.FAILED}

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = [
                    "media_id=?",
                    "state IN (" + ",".join("?" for _ in allowed_sources) + ")",
                ]
                params: list[Any] = [media_id, *allowed_sources]
                if parent_id:
                    # The upload child and canonical outbox were acquired in
                    # one transaction with the same token/lease.  Validate
                    # both immediately before every mutation; neither row can
                    # independently authorize a channel send.
                    filters.extend(
                        [
                            "outbox_id=?",
                            "claim_token=?",
                            "lease_expires_at IS NOT NULL",
                            "lease_expires_at > ?",
                            "EXISTS (SELECT 1 FROM user_outbox AS parent "
                            "WHERE parent.outbox_id=outgoing_media.outbox_id "
                            "AND parent.reply_slot_id IS NOT NULL "
                            "AND parent.claim_token=? "
                            "AND parent.state IN ('claimed','sending') "
                            "AND parent.lease_expires_at IS NOT NULL "
                            "AND parent.lease_expires_at>?)",
                        ]
                    )
                    params.extend(
                        (
                            parent_id,
                            parent_token,
                            now_text,
                            parent_token,
                            now_text,
                        )
                    )
                elif claim_token is not None:
                    filters.append("claim_token=?")
                    params.append(claim_token)
                    filters.extend(
                        ["lease_expires_at IS NOT NULL", "lease_expires_at > ?"]
                    )
                    params.append(now_text)
                else:
                    # Tokenless transitions are permitted only for an
                    # unclaimed/admin-created row.  Once a worker owns the
                    # media lease, every callback must present that exact
                    # token so a stale uploader/sender cannot overwrite the
                    # current owner's state.
                    filters.append("claim_token IS NULL")
                assignments = [
                    "state=?",
                    "updated_at=?",
                    "last_error=?",
                    "next_attempt_at=NULL",
                ]
                values: list[Any] = [target.value, now_text, error]
                if target == MediaDeliveryState.UPLOADED:
                    assignments.extend(["remote_id=COALESCE(?, remote_id)", "upload_param=COALESCE(?, upload_param)", "encryption_key=COALESCE(?, encryption_key)", "uploaded_at=?"])
                    values.extend([remote_id, upload_param, encryption_key, now_text])
                elif remote_id is not None or upload_param is not None or encryption_key is not None:
                    assignments.extend(["remote_id=COALESCE(?, remote_id)", "upload_param=COALESCE(?, upload_param)", "encryption_key=COALESCE(?, encryption_key)"])
                    values.extend([remote_id, upload_param, encryption_key])
                if metadata is not None:
                    metadata_rows = conn.execute(
                        "SELECT m.metadata_json AS media_metadata_json, "
                        "a.kind, a.mime_type, a.size_bytes, "
                        "a.metadata_json AS attachment_metadata_json "
                        "FROM outgoing_media m JOIN attachments a "
                        "ON a.attachment_id=m.attachment_id WHERE m.media_id=?",
                        (media_id,),
                    ).fetchone()
                    if metadata_rows is None:
                        return False
                    current_metadata = (
                        json_loads(metadata_rows["media_metadata_json"], {}) or {}
                    )
                    attachment_metadata = (
                        json_loads(
                            metadata_rows["attachment_metadata_json"], {}
                        )
                        or {}
                    )
                    merged_metadata: dict[str, Any] = {}
                    for source in (current_metadata, attachment_metadata, metadata):
                        if not isinstance(source, Mapping):
                            continue
                        merged_metadata.update(
                            {
                                str(key): value
                                for key, value in source.items()
                                if str(key).lower()
                                not in _ATTACHMENT_AUTHORITY_KEYS
                            }
                        )
                    if isinstance(attachment_metadata, Mapping):
                        # Ownership/source fields are not transport hints.
                        # Copy them only from the authoritative attachment.
                        for key in (
                            "owner_agent_id",
                            "agent_id",
                            "channel",
                            "bot_id",
                            "external_user_id",
                            "user_id",
                            "session_id",
                            "source_message_id",
                            "source_ordinal",
                            "created_at",
                            "filename",
                        ):
                            if key in attachment_metadata:
                                merged_metadata[key] = attachment_metadata[key]
                    merged_metadata.update(
                        {
                            "kind": metadata_rows["kind"],
                            "mime_type": metadata_rows["mime_type"],
                            "size_bytes": int(metadata_rows["size_bytes"] or 0),
                        }
                    )
                    assignments.append("metadata_json=?")
                    values.append(json_dumps(merged_metadata))
                if terminal:
                    assignments.extend(["lease_expires_at=NULL", "claim_token=NULL", "claimed_by=NULL"])
                    if target == MediaDeliveryState.SENT:
                        assignments.append("sent_at=COALESCE(sent_at, ?)")
                        values.append(now_text)
                changed = conn.execute(
                    "UPDATE outgoing_media SET " + ", ".join(assignments)
                    + " WHERE " + " AND ".join(filters),
                    [*values, *params],
                ).rowcount
                return changed == 1

        return await self._call(op)

    async def transition_outgoing_media_for_outbox(
        self,
        media_id: str,
        to_state: MediaDeliveryState | str,
        *,
        outbox_id: str,
        outbox_claim_token: str,
        **kwargs: Any,
    ) -> bool:
        """Advance a subordinate upload using only its parent send claim."""

        return await self.transition_outgoing_media(
            media_id,
            to_state,
            outbox_id=outbox_id,
            outbox_claim_token=outbox_claim_token,
            **kwargs,
        )

    async def mark_media_uploaded(
        self,
        media_id: str,
        *,
        claim_token: str | None = None,
        remote_id: str | None = None,
        upload_param: str | None = None,
        encryption_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        """Compatibility helper for the common upload acknowledgement."""

        return await self.transition_outgoing_media(
            media_id,
            MediaDeliveryState.UPLOADED,
            claim_token=claim_token,
            remote_id=remote_id,
            upload_param=upload_param,
            encryption_key=encryption_key,
            metadata=metadata,
            now=now,
        )

    mark_outgoing_media = transition_outgoing_media
    transition_media_delivery = transition_outgoing_media

    async def retry_outgoing_media(
        self,
        media_id: str,
        *,
        now: datetime | str | None = None,
    ) -> bool:
        """Explicitly return a failed media operation to the upload queue.

        Persisted remote upload parameters are retained.  The WeChat media
        worker recognizes them after claiming ``upload_pending`` and can retry
        the send without performing a second CDN upload.
        """

        return await self.transition_outgoing_media(
            media_id,
            MediaDeliveryState.UPLOAD_PENDING,
            from_states=(MediaDeliveryState.FAILED,),
            now=now,
        )

    retry_media_delivery = retry_outgoing_media

    async def renew_outgoing_media_lease(
        self, media_id: str, claim_token: str, *, lease_seconds: float = 60.0, now: datetime | str | None = None
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                return conn.execute(
                    "UPDATE outgoing_media SET lease_expires_at=?, updated_at=? "
                    "WHERE media_id=? AND claim_token=? "
                    "AND state IN ('uploading','uploaded','send_pending') "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
                    (lease, now_text, media_id, claim_token, now_text),
                ).rowcount == 1

        return await self._call(op)

    renew_media_lease = renew_outgoing_media_lease

    async def renew_canonical_media_lease(
        self,
        outbox_id: str,
        claim_token: str,
        *,
        media_ids: Iterable[str],
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
    ) -> bool:
        """Atomically renew one canonical send and all upload subordinates."""

        parent_id = str(outbox_id or "").strip()
        token = str(claim_token or "").strip()
        children = tuple(dict.fromkeys(str(value or "").strip() for value in media_ids))
        if not parent_id or not token or not children or any(not value for value in children):
            return False

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                parent = conn.execute(
                    """SELECT 1 FROM user_outbox
                       WHERE outbox_id=? AND claim_token=?
                         AND reply_slot_id IS NOT NULL
                         AND state IN ('claimed','sending')
                         AND lease_expires_at IS NOT NULL
                         AND lease_expires_at>?""",
                    (parent_id, token, now_text),
                ).fetchone()
                placeholders = ",".join("?" for _ in children)
                child_count = conn.execute(
                    f"""SELECT COUNT(*) FROM outgoing_media
                        WHERE outbox_id=? AND claim_token=?
                          AND media_id IN ({placeholders})
                          AND state IN ('uploading','uploaded','send_pending')
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at>?""",
                    (parent_id, token, *children, now_text),
                ).fetchone()[0]
                if parent is None or int(child_count) != len(children):
                    return False
                parent_changed = conn.execute(
                    """UPDATE user_outbox SET lease_expires_at=?
                       WHERE outbox_id=? AND claim_token=?
                         AND state IN ('claimed','sending')
                         AND lease_expires_at>?""",
                    (lease, parent_id, token, now_text),
                ).rowcount
                children_changed = conn.execute(
                    f"""UPDATE outgoing_media
                        SET lease_expires_at=?, updated_at=?
                        WHERE outbox_id=? AND claim_token=?
                          AND media_id IN ({placeholders})
                          AND state IN ('uploading','uploaded','send_pending')
                          AND lease_expires_at>?""",
                    (lease, now_text, parent_id, token, *children, now_text),
                ).rowcount
                if parent_changed != 1 or children_changed != len(children):
                    raise StoreError("canonical media lease renewal was split")
                return True

        return await self._call(op)

    async def renew_outbox_lease(
        self, outbox_id: str, claim_token: str, *, lease_seconds: float = 60.0, now: datetime | str | None = None
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                return conn.execute(
                    "UPDATE user_outbox SET lease_expires_at=? WHERE outbox_id=? "
                    "AND claim_token=? AND state IN ('claimed','sending') "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
                    (lease, outbox_id, claim_token, now_text),
                ).rowcount == 1
        return await self._call(op)

    async def present_unseen(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
        limit: int = 100,
        present: bool = True,
    ) -> list[UserOutboxItem]:
        """Return unseen records, optionally marking them presented.

        Command handlers use ``present=False`` to read the inbox before the
        acknowledgement outbox row is committed.  The gateway then passes the
        IDs to :meth:`create_user_outbox`, which marks them in that same
        transaction.  Existing callers retain the historical marking
        behavior with the default ``present=True``.
        """
        def op(conn: sqlite3.Connection) -> list[UserOutboxItem]:
            with _transaction(conn):
                rows = conn.execute(
                    """SELECT * FROM user_outbox WHERE channel=? AND bot_id=?
                       AND external_user_id=? AND session_id=? AND agent_id=?
                       AND presentation='unseen'
                       ORDER BY priority DESC, created_at ASC, outbox_id ASC LIMIT ?""",
                    (channel, bot_id, external_user_id, session_id, agent_id, max(0, int(limit))),
                ).fetchall()
                ids = [row["outbox_id"] for row in rows]
                if ids and present:
                    placeholders = ",".join("?" for _ in ids)
                    conn.execute(
                        f"UPDATE user_outbox SET presentation='presented', presented_at=? WHERE outbox_id IN ({placeholders}) AND presentation='unseen'",
                        [self._now(), *ids],
                    )
                if ids and present:
                    rows = conn.execute(
                        f"SELECT * FROM user_outbox WHERE outbox_id IN ({placeholders}) ORDER BY priority DESC, created_at ASC, outbox_id ASC",
                        ids,
                    ).fetchall()
                return [self._outbox_from_row(row) for row in rows]
        return await self._call(op)

    present_notifications = present_unseen

    async def acknowledge_outbox(self, outbox_id: str, *, now: datetime | str | None = None) -> bool:
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                return conn.execute(
                    "UPDATE user_outbox SET presentation='acknowledged', acknowledged_at=? WHERE outbox_id=? AND presentation <> 'acknowledged'",
                    (now_text, outbox_id),
                ).rowcount == 1
        return await self._call(op)

    ack_outbox = acknowledge_outbox

    async def set_notification_preference(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
        enabled: bool,
        now: datetime | str | None = None,
    ) -> bool:
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                conn.execute(
                    """INSERT INTO notification_preferences
                       (channel, bot_id, external_user_id, session_id, agent_id, notify_enabled, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel, bot_id, external_user_id, session_id, agent_id)
                       DO UPDATE SET notify_enabled=excluded.notify_enabled, updated_at=excluded.updated_at""",
                    (channel, bot_id, external_user_id, session_id, agent_id, int(bool(enabled)), now_text),
                )
                # Apply the current preference to already-pending background
                # rows. Foreground is a projection-time fact; consulting the
                # live route here would reclassify old background rows after
                # the user switched Agents.
                conn.execute(
                    """UPDATE user_outbox AS o SET notify_enabled=(
                           CASE WHEN ? <> 0 AND o.priority >= 2 THEN 1 ELSE 0 END)
                       WHERE o.channel=? AND o.bot_id=? AND o.external_user_id=?
                         AND o.session_id=? AND o.agent_id=?
                         AND o.state IN ('pending','retry_wait')
                         AND o.foreground=0""",
                    (
                    int(bool(enabled)), channel, bot_id, external_user_id,
                        session_id, agent_id,
                    ),
                )
                return True
        return await self._call(op)

    set_notify_preference = set_notification_preference
    set_notification = set_notification_preference

    async def get_notification_preference(
        self, *, channel: str, bot_id: str, external_user_id: str, session_id: str, agent_id: str
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                "SELECT notify_enabled FROM notification_preferences WHERE channel=? AND bot_id=? AND external_user_id=? AND session_id=? AND agent_id=?",
                (channel, bot_id, external_user_id, session_id, agent_id),
            ).fetchone()
            return True if row is None else bool(row["notify_enabled"])
        return await self._call(op)

    # ------------------------------------------------------------------
    # Agent mailbox and correlated messages
    # ------------------------------------------------------------------
    async def list_mailbox(
        self,
        destination_agent_id: str,
        *,
        states: Iterable[MailboxState | str] | None = None,
        limit: int = 100,
    ) -> list[AgentMailboxItem]:
        filters = ["destination_agent_id = ?"]; params: list[Any] = [destination_agent_id]
        if states is not None:
            values = [_enum_value(item) for item in states]
            if not values:
                return []
            filters.append("state IN (" + ",".join("?" for _ in values) + ")"); params.extend(values)
        params.append(max(0, int(limit)))
        def op(conn: sqlite3.Connection) -> list[AgentMailboxItem]:
            rows = conn.execute("SELECT * FROM agent_mailbox WHERE " + " AND ".join(filters) + " ORDER BY created_at ASC LIMIT ?", params).fetchall()
            return [self._mailbox_from_row(row) for row in rows if self._mailbox_from_row(row) is not None]
        return await self._call(op)

    get_mailbox = list_mailbox
    list_agent_mailbox = list_mailbox

    async def get_mailbox_item(self, mailbox_id: str) -> AgentMailboxItem | None:
        def op(conn: sqlite3.Connection) -> AgentMailboxItem | None:
            return self._mailbox_from_row(
                conn.execute("SELECT * FROM agent_mailbox WHERE mailbox_id=?", (mailbox_id,)).fetchone()
            )

        return await self._call(op)

    get_agent_mailbox_item = get_mailbox_item

    async def get_correlated_mailbox_response(
        self, mailbox_id: str
    ) -> AgentMailboxItem | None:
        """Return the durable response to one request mailbox envelope.

        This lookup closes the recovery window between committing a response
        and marking its request processed.  It deliberately derives every
        correlation field from the immutable original rather than trusting a
        worker-provided request or destination ID.
        """

        def op(conn: sqlite3.Connection) -> AgentMailboxItem | None:
            row = conn.execute(
                """SELECT response.*
                     FROM agent_mailbox AS original
                     JOIN agent_mailbox AS response
                       ON response.request_id = original.request_id
                      AND response.reply_to_id = original.message_id
                      AND response.source_agent_id = original.destination_agent_id
                      AND response.destination_agent_id = original.source_agent_id
                    WHERE original.mailbox_id=?
                    ORDER BY response.created_at ASC
                    LIMIT 1""",
                (mailbox_id,),
            ).fetchone()
            return self._mailbox_from_row(row)

        return await self._call(op)

    get_mailbox_response = get_correlated_mailbox_response
    find_mailbox_response = get_correlated_mailbox_response

    async def claim_mailbox(
        self,
        destination_agent_id: str,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
    ) -> list[AgentMailboxItem]:
        if int(limit) <= 0:
            return []
        def op(conn: sqlite3.Connection) -> list[AgentMailboxItem]:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                rows = conn.execute(
                    """SELECT * FROM agent_mailbox WHERE destination_agent_id=?
                       AND state IN ('pending') AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       ORDER BY created_at ASC LIMIT ?""",
                    (destination_agent_id, now_text, max(1, int(limit))),
                ).fetchall()
                result: list[AgentMailboxItem] = []
                for row in rows:
                    token = _uuid()
                    if conn.execute(
                        "UPDATE agent_mailbox SET state='claimed', claimed_by=?, claim_token=?, lease_expires_at=?, attempts=attempts+1 WHERE mailbox_id=? AND state='pending'",
                        (worker_id, token, lease, row["mailbox_id"]),
                    ).rowcount:
                        fresh = conn.execute("SELECT * FROM agent_mailbox WHERE mailbox_id=?", (row["mailbox_id"],)).fetchone()
                        item = self._mailbox_from_row(fresh)
                        if item is not None:
                            result.append(item)
                return result
        return await self._call(op)

    claim_next_mailbox = claim_mailbox
    claim_agent_mailbox = claim_mailbox

    async def mark_mailbox_processing(
        self,
        mailbox_id: str,
        claim_token: str | None = None,
        *,
        now: datetime | str | None = None,
    ) -> bool:
        if not claim_token:
            return False
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = (
                    "mailbox_id=? AND state='claimed' AND claim_token=? "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                )
                params: list[Any] = [mailbox_id, claim_token, now_text]
                return conn.execute("UPDATE agent_mailbox SET state='processing' WHERE " + filters, params).rowcount == 1
        return await self._call(op)

    async def renew_mailbox_lease(
        self,
        mailbox_id: str,
        claim_token: str,
        *,
        lease_seconds: float = 60.0,
        now: datetime | str | None = None,
    ) -> bool:
        """Extend a claimed/processing mailbox lease under its token fence.

        Mailbox handlers may perform a long Agent turn.  Renewing the lease
        from the owning worker prevents startup reconciliation (or a second
        mailbox worker) from re-queuing that turn while it is still running.
        The conditional update is intentionally short and does not inspect or
        mutate the message payload.
        """
        if not claim_token:
            return False

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                lease = self._lease_deadline(now_text, lease_seconds)
                return conn.execute(
                    """UPDATE agent_mailbox SET lease_expires_at=?
                       WHERE mailbox_id=? AND claim_token=?
                         AND state IN ('claimed','processing')
                         AND lease_expires_at IS NOT NULL
                         AND lease_expires_at > ?""",
                    (lease, mailbox_id, claim_token, now_text),
                ).rowcount == 1

        return await self._call(op)

    renew_agent_mailbox_lease = renew_mailbox_lease
    extend_mailbox_lease = renew_mailbox_lease

    async def mark_mailbox_processed(self, mailbox_id: str, claim_token: str | None = None, *, now: datetime | str | None = None) -> bool:
        if not claim_token:
            return False
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = (
                    "mailbox_id=? AND state='processing' AND claim_token=? "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                )
                params: list[Any] = [mailbox_id, claim_token, now_text]
                return conn.execute("UPDATE agent_mailbox SET state='processed', processed_at=?, lease_expires_at=NULL, claim_token=NULL, claimed_by=NULL WHERE " + filters, [now_text, *params]).rowcount == 1
        return await self._call(op)

    async def mark_mailbox_failed(
        self, mailbox_id: str, claim_token: str | None = None, *, error: str = "", dead_letter: bool = False, now: datetime | str | None = None
    ) -> bool:
        if not claim_token:
            return False
        state = MailboxState.DEAD_LETTER if dead_letter else MailboxState.PENDING
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = (
                    "mailbox_id=? AND state='processing' AND claim_token=? "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                )
                params: list[Any] = [mailbox_id, claim_token, now_text]
                return conn.execute("UPDATE agent_mailbox SET state=?, last_error=?, next_attempt_at=?, lease_expires_at=NULL, claim_token=NULL, claimed_by=NULL WHERE " + filters, [state.value, error, now_text, *params]).rowcount == 1
        return await self._call(op)

    async def reject_mailbox(
        self,
        mailbox_id: str,
        claim_token: str | None = None,
        *,
        error: str = "",
        now: datetime | str | None = None,
    ) -> bool:
        """Permanently reject a mailbox request after policy validation."""
        if not claim_token:
            return False

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                now_text = self._now(now)
                filters = (
                    "mailbox_id=? AND state='processing' AND claim_token=? "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at > ?"
                )
                params: list[Any] = [mailbox_id, claim_token, now_text]
                return conn.execute(
                    "UPDATE agent_mailbox SET state='rejected', last_error=?, "
                    "processed_at=?, next_attempt_at=NULL, lease_expires_at=NULL, "
                    "claim_token=NULL, claimed_by=NULL WHERE " + filters,
                    [error, now_text, *params],
                ).rowcount == 1

        return await self._call(op)

    mark_mailbox_rejected = reject_mailbox

    complete_mailbox = mark_mailbox_processed

    async def create_agent_message(
        self,
        *,
        source_agent_id: str,
        destination_agent_id: str,
        content: str,
        request_id: str | None = None,
        reply_to_id: str | None = None,
        causation_id: str | None = None,
        task_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        message_id: str | None = None,
        execution_snapshot: Mapping[str, Any] | None = None,
        runtime_snapshot: Mapping[str, Any] | None = None,
        reply_target: Any = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        original_mailbox_id: str | None = None,
        original_claim_token: str | None = None,
        require_active_task: bool = False,
        required_execution_id: str | None = None,
        now: datetime | str | None = None,
    ) -> AgentMailboxItem:
        """Create a correlated Agent mailbox message, never a user delivery."""
        mid = message_id or _uuid()
        rid = request_id or _uuid()
        if (original_mailbox_id is None) != (original_claim_token is None):
            raise ValueError(
                "original mailbox id and claim token must be supplied together"
            )
        payload_snapshot = dict(payload or {})
        supplied_execution_snapshot = execution_snapshot or runtime_snapshot
        if supplied_execution_snapshot is None:
            # Internal event/projector adapters may carry the snapshot in a
            # reserved payload key while older callers only know ``payload``.
            candidate_snapshot = payload_snapshot.get("_execution_snapshot")
            if isinstance(candidate_snapshot, Mapping):
                supplied_execution_snapshot = candidate_snapshot
        def op(conn: sqlite3.Connection) -> AgentMailboxItem:
            with _transaction(conn):
                now_text = self._now(now)
                if original_mailbox_id is not None:
                    original_claim = conn.execute(
                        """SELECT mailbox_id, message_id, request_id,
                                  source_agent_id, destination_agent_id, task_id
                           FROM agent_mailbox
                           WHERE mailbox_id=? AND state='processing'
                             AND claim_token=?
                             AND lease_expires_at IS NOT NULL
                             AND lease_expires_at > ?""",
                        (
                            str(original_mailbox_id),
                            str(original_claim_token),
                            now_text,
                        ),
                    ).fetchone()
                    if original_claim is None:
                        raise StoreError("mailbox response claim is not active")
                    expected = {
                        "request_id": original_claim["request_id"],
                        "reply_to_id": original_claim["message_id"],
                        "task_id": original_claim["task_id"],
                        "source_agent_id": original_claim["destination_agent_id"],
                        "destination_agent_id": original_claim["source_agent_id"],
                    }
                    supplied = {
                        "request_id": rid,
                        "reply_to_id": reply_to_id,
                        "task_id": task_id,
                        "source_agent_id": source_agent_id,
                        "destination_agent_id": destination_agent_id,
                    }
                    for field, expected_value in expected.items():
                        if str(supplied[field] or "") != str(expected_value or ""):
                            raise StoreError(
                                f"mailbox response claim conflicts ({field})"
                            )
                if require_active_task or required_execution_id is not None:
                    if task_id is None:
                        raise StoreError("Agent bridge requires a task ID")
                    active_task = conn.execute(
                        """SELECT t.state, (
                                   SELECT e.execution_id
                                   FROM task_executions AS e
                                   WHERE e.task_id=t.task_id
                                   ORDER BY e.attempt DESC LIMIT 1
                               ) AS execution_id
                           FROM tasks AS t WHERE t.task_id=?""",
                        (task_id,),
                    ).fetchone()
                    if active_task is None:
                        raise NotFoundError(f"task not found: {task_id}")
                    if require_active_task and str(
                        active_task["state"] or ""
                    ) != "running":
                        raise StoreError("Agent bridge requires a running task")
                    if required_execution_id is not None and str(
                        active_task["execution_id"] or ""
                    ) != str(required_execution_id):
                        raise StoreError(
                            "Agent bridge capability does not match the task execution"
                        )
                attachment_values = self._input_attachment_values(payload_snapshot)
                # A request ID is logical across retries; return an existing
                # destination row rather than enqueueing duplicate work.  Do
                # this immutable replay check before consulting current media
                # availability: once the envelope is durable, a later cleanup
                # or blocked file cannot turn the same request into a second
                # logical operation or an apparent ingress failure.
                existing = conn.execute("SELECT * FROM agent_mailbox WHERE destination_agent_id=? AND request_id=?", (destination_agent_id, rid)).fetchone()
                if existing is not None:
                    existing_payload = json_loads(existing["payload_json"], {}) or {}
                    incoming_payload = _snapshot_value(payload_snapshot) or {}
                    immutable = {
                        "source_agent_id": source_agent_id,
                        "destination_agent_id": destination_agent_id,
                        "reply_to_id": reply_to_id,
                        "causation_id": causation_id,
                        "task_id": task_id,
                        "content": content,
                    }
                    for column, expected in immutable.items():
                        if str(existing[column] or "") != str(expected or ""):
                            raise StoreError(
                                f"mailbox request envelope conflicts: {destination_agent_id}/{rid}"
                            )
                    if _snapshot_value(existing_payload) != _snapshot_value(incoming_payload):
                        raise StoreError(
                            f"mailbox request payload conflicts: {destination_agent_id}/{rid}"
                        )
                    if message_id is not None and str(existing["message_id"]) != str(mid):
                        raise StoreError("mailbox request message_id conflicts")
                    if supplied_execution_snapshot is not None:
                        existing_snapshot = json_loads(
                            existing["execution_snapshot_json"]
                            if "execution_snapshot_json" in existing.keys()
                            else "{}",
                            {},
                        ) or {}
                        incoming_snapshot = self._mailbox_execution_snapshot_tx(
                            conn,
                            destination_agent_id=destination_agent_id,
                            request_id=rid,
                            supplied=supplied_execution_snapshot,
                            reply_target=reply_target,
                            channel=channel,
                            bot_id=bot_id,
                            external_user_id=external_user_id,
                            session_id=session_id,
                        )
                        if self._json_snapshot(existing_snapshot) != self._json_snapshot(
                            incoming_snapshot
                        ):
                            raise StoreError(
                                "mailbox execution snapshot conflicts with existing request"
                            )
                    item = self._mailbox_from_row(existing)
                    if item is not None:
                        self._retain_attachment_refs_tx(
                            conn,
                            owner_kind="mailbox",
                            owner_id=str(existing["mailbox_id"]),
                            attachments=attachment_values,
                            role="agent_message",
                            created_at=existing["created_at"],
                        )
                        return item
                task_scope = None
                if task_id is not None:
                    task_scope = conn.execute(
                        "SELECT agent_id, channel, bot_id, external_user_id, "
                        "session_id, conversation_id, reply_target_json, mode_id, state, "
                        "profile_version, policy_version, model, reasoning_effort, "
                        "metadata_json, ("
                        "    SELECT e.execution_id FROM task_executions AS e "
                        "    WHERE e.task_id=tasks.task_id "
                        "    ORDER BY e.attempt DESC LIMIT 1"
                        ") AS execution_id "
                        "FROM tasks WHERE task_id=?",
                        (task_id,),
                    ).fetchone()
                    if task_scope is None:
                        raise NotFoundError(f"task not found: {task_id}")
                    if require_active_task and str(task_scope["state"] or "") != "running":
                        raise StoreError("Agent bridge requires a running task")
                    if required_execution_id is not None and str(
                        task_scope["execution_id"] or ""
                    ) != str(required_execution_id):
                        raise StoreError(
                            "Agent bridge capability does not match the task execution"
                        )
                elif require_active_task or required_execution_id is not None:
                    raise StoreError("Agent bridge requires a task ID")
                resolved_execution_snapshot = self._mailbox_execution_snapshot_tx(
                    conn,
                    destination_agent_id=destination_agent_id,
                    request_id=rid,
                    task_scope=task_scope,
                    supplied=supplied_execution_snapshot,
                    reply_target=reply_target,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                )
                for raw_attachment in attachment_values:
                    attachment_id, _metadata = self._attachment_value(raw_attachment)
                    if not attachment_id:
                        continue
                    if conn.execute(
                        "SELECT 1 FROM attachments WHERE attachment_id=?",
                        (attachment_id,),
                    ).fetchone() is None:
                        raise StoreError(f"attachment is unavailable: {attachment_id}")
                    if not self._attachment_accessible_tx(
                        conn,
                        attachment_id,
                        agent_id=str(source_agent_id),
                        task_id=task_id,
                        channel=(str(task_scope["channel"] or "") if task_scope else ""),
                        bot_id=(str(task_scope["bot_id"] or "") if task_scope else ""),
                        external_user_id=(
                            str(task_scope["external_user_id"] or "")
                            if task_scope else ""
                        ),
                        session_id=(
                            str(task_scope["session_id"] or "default")
                            if task_scope else "default"
                        ),
                    ):
                        raise StoreError(f"attachment access denied: {attachment_id}")
                if task_id is not None and reply_to_id is None:
                    if str(task_scope["agent_id"]) != str(source_agent_id):
                        raise StoreError("mailbox source Agent does not own its task")
                if reply_to_id:
                    original = conn.execute(
                        "SELECT message_id, request_id, source_agent_id, destination_agent_id, task_id FROM messages WHERE message_id=?",
                        (reply_to_id,),
                    ).fetchone()
                    if original is None:
                        raise StoreError("reply target message does not exist")
                    if original["request_id"] and original["request_id"] != rid:
                        raise StoreError("reply request_id does not match original request")
                    if original["source_agent_id"] and original["source_agent_id"] != destination_agent_id:
                        raise StoreError("response destination must be the original source Agent")
                    if original["destination_agent_id"] and original["destination_agent_id"] != source_agent_id:
                        raise StoreError("response source Agent does not own the original request")
                    if original["task_id"] is not None and str(original["task_id"]) != str(task_id or ""):
                        raise StoreError("response task does not match the original request")
                    if task_id is not None and conn.execute(
                        "SELECT 1 FROM tasks WHERE task_id=?", (task_id,)
                    ).fetchone() is None:
                        raise NotFoundError(f"task not found: {task_id}")
                message_existing = conn.execute(
                    "SELECT request_id, reply_to_id, causation_id, task_id, source_agent_id, "
                    "destination_agent_id, content, payload_json FROM messages WHERE message_id=?",
                    (mid,),
                ).fetchone()
                if message_existing is not None:
                    same_message = all(
                        str(message_existing[column] or "") == str(expected or "")
                        for column, expected in {
                            "request_id": rid,
                            "reply_to_id": reply_to_id,
                            "causation_id": causation_id,
                            "task_id": task_id,
                            "source_agent_id": source_agent_id,
                            "destination_agent_id": destination_agent_id,
                            "content": content,
                        }.items()
                    ) and self._json_snapshot(
                        json_loads(message_existing["payload_json"], {}) or {}
                    ) == self._json_snapshot(payload_snapshot)
                    if not same_message:
                        raise StoreError(f"message_id already belongs to another envelope: {mid}")
                    existing_mailbox = conn.execute(
                        "SELECT * FROM agent_mailbox WHERE message_id=?", (mid,)
                    ).fetchone()
                    if existing_mailbox is not None:
                        item = self._mailbox_from_row(existing_mailbox)
                        if item is not None:
                            return item
                conn.execute(
                    """INSERT INTO messages (message_id, request_id, reply_to_id, causation_id,
                       task_id, source_agent_id, destination_agent_id, visibility, priority,
                       content, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'internal', 0, ?, ?, ?)""",
                    (mid, rid, reply_to_id, causation_id, task_id, source_agent_id, destination_agent_id, content, json_dumps(payload_snapshot), now_text),
                )
                mailbox_id = _uuid()
                conn.execute(
                    """INSERT INTO agent_mailbox
                       (mailbox_id, message_id, request_id, reply_to_id, causation_id,
                        source_agent_id, destination_agent_id, task_id, content, payload_json,
                        execution_snapshot_json, state, attempts, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?)""",
                    (
                        mailbox_id,
                        mid,
                        rid,
                        reply_to_id,
                        causation_id,
                        source_agent_id,
                        destination_agent_id,
                        task_id,
                        content,
                        json_dumps(payload_snapshot),
                        json_dumps(resolved_execution_snapshot),
                        now_text,
                    ),
                )
                # Retain managed attachments for the mailbox lifetime.  A
                # channel-only/unknown reference stays in the structured
                # payload but cannot create a foreign-key reference.
                for ordinal, raw_attachment in enumerate(attachment_values):
                    attachment_id, _metadata = self._attachment_value(raw_attachment)
                    if not attachment_id:
                        continue
                    if conn.execute(
                        "SELECT 1 FROM attachments WHERE attachment_id=?",
                        (attachment_id,),
                    ).fetchone() is None:
                        continue
                    conn.execute(
                        """INSERT OR IGNORE INTO attachment_refs
                           (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                           VALUES ('mailbox', ?, ?, 'agent_message', ?, ?)""",
                        (mailbox_id, attachment_id, ordinal, now_text),
                    )
                item = self._mailbox_from_row(conn.execute("SELECT * FROM agent_mailbox WHERE message_id=?", (mid,)).fetchone())
                if item is None: raise StoreError("mailbox insert failed")
                return item
        return await self._call(op)

    send_agent_message = create_agent_message
    enqueue_mailbox = create_agent_message

    # ------------------------------------------------------------------ audit trail
    async def record_audit_event(
        self,
        *,
        action: str,
        allowed: bool,
        reason: str = "",
        actor: str = "",
        source_agent_id: str | None = None,
        destination_agent_id: str | None = None,
        request_type: str | None = None,
        task_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        audit_id: str | None = None,
        now: datetime | str | None = None,
    ) -> str:
        aid = str(audit_id or _uuid())
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> str:
            with _transaction(conn):
                conn.execute(
                    """INSERT OR IGNORE INTO audit_events
                       (audit_id, actor, action, allowed, reason,
                        source_agent_id, destination_agent_id, request_type,
                        task_id, payload_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (aid, actor, action, int(bool(allowed)), reason,
                     source_agent_id, destination_agent_id, request_type,
                     task_id, json_dumps(dict(payload or {})), now_text),
                )
                return aid

        return await self._call(op)

    append_audit_event = record_audit_event

    async def list_audit_events(self, *, task_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: list[Any] = []
        if task_id is not None:
            filters.append("task_id=?")
            params.append(task_id)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(0, int(limit)))

        def op(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                f"SELECT * FROM audit_events{where} ORDER BY created_at DESC, audit_id DESC LIMIT ?",
                params,
            ).fetchall()
            values: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["allowed"] = bool(item["allowed"])
                item["payload"] = json_loads(item.pop("payload_json", "{}"), {}) or {}
                values.append(item)
            return values

        return await self._call(op)

    # ------------------------------------------------------------------
    # Routes, profiles, modes, and conversations
    # ------------------------------------------------------------------
    async def set_route(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        active_agent_id: str,
        now: datetime | str | None = None,
    ) -> bool:
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                conn.execute(
                    """INSERT INTO routes(channel, bot_id, external_user_id, session_id, active_agent_id, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel, bot_id, external_user_id, session_id)
                       DO UPDATE SET active_agent_id=excluded.active_agent_id, updated_at=excluded.updated_at""",
                    (channel, bot_id, external_user_id, session_id, active_agent_id, now_text),
                )
                return True
        return await self._call(op)

    set_active_agent = set_route
    set_agent = set_route

    async def get_route(self, *, channel: str, bot_id: str, external_user_id: str, session_id: str = "default", default_agent_id: str = "codex") -> str:
        def op(conn: sqlite3.Connection) -> str:
            row = conn.execute("SELECT active_agent_id FROM routes WHERE channel=? AND bot_id=? AND external_user_id=? AND session_id=?", (channel, bot_id, external_user_id, session_id)).fetchone()
            return str(row["active_agent_id"] if row else default_agent_id)
        return await self._call(op)

    active_agent = get_route

    async def retire_agent(
        self, agent_id: str, *, default_agent_id: str = "codex"
    ) -> int:
        """Disable an idle Agent while retaining immutable profile history.

        The unfinished-task check, tombstone, profile disablement, and route
        fallback share one transaction.  This prevents a concurrent task
        insert from being stranded between a preflight query and retirement.
        """

        agent_id = str(agent_id or "").strip()
        default_agent_id = str(default_agent_id or "codex").strip() or "codex"
        now_text = self._now()

        def op(conn: sqlite3.Connection) -> int:
            with _transaction(conn):
                self._assert_agent_retirable_tx(conn, agent_id)
                cursor = conn.execute(
                    "UPDATE agent_profiles SET enabled=0 WHERE agent_id=?",
                    (agent_id,),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO deleted_agents(agent_id, deleted_at) VALUES (?, ?)",
                    (agent_id, now_text),
                )
                conn.execute(
                    "UPDATE routes SET active_agent_id=?, updated_at=? "
                    "WHERE active_agent_id=?",
                    (default_agent_id, now_text, agent_id),
                )
                return int(cursor.rowcount)

        return await self._call(op)

    async def reactivate_agent(self, profile: Any) -> bool:
        """Prepare an explicitly recreated dynamic Agent for publication.

        Retirement changes only the lifecycle ``enabled`` bit of otherwise
        immutable Profile rows.  Reusing `/agent <id>` is an explicit request
        to recreate that alias, so restore those bits while deliberately
        retaining its tombstone.  The requested current Profile must match its
        retired immutable metadata exactly; only ``enabled: false -> true`` is
        repairable here.  Registry publication can then fail or be interrupted
        without making the half-restored alias visible after restart.  A
        genuine same-version conflict remains fail-closed.
        """

        data = self._mapping_snapshot(profile)
        if not data:
            raise ValueError("profile must be a mapping or profile value")
        agent_id = str(data.get("agent_id") or "").strip()
        if not agent_id:
            raise ValueError("profile.agent_id is required")
        profile_version = int(data.get("profile_version", data.get("version", 1)))
        values, supplied = self._profile_snapshot_values(
            agent_id=agent_id,
            profile_version=profile_version,
            snapshot=data,
        )
        if not supplied or int(values["enabled"]) != 1:
            raise ValueError("reactivated profile must be enabled")
        now = self._now()

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                tombstone = conn.execute(
                    "SELECT 1 FROM deleted_agents WHERE agent_id=?",
                    (agent_id,),
                ).fetchone()
                if tombstone is None:
                    # Another process may already have completed the same
                    # recreation.  Normal immutable Profile persistence will
                    # validate that state after this idempotent no-op.
                    return False

                row = conn.execute(
                    "SELECT * FROM agent_profiles "
                    "WHERE agent_id=? AND profile_version=?",
                    (agent_id, profile_version),
                ).fetchone()
                if row is None:
                    self._ensure_profile_tx(
                        conn,
                        agent_id=agent_id,
                        profile_version=profile_version,
                        snapshot=data,
                        now=now,
                    )
                else:
                    mismatches = []
                    for column, expected in values.items():
                        if column == "enabled":
                            continue
                        actual = row[column]
                        if column.endswith("_json"):
                            if self._json_snapshot(
                                json_loads(actual, [])
                            ) != self._json_snapshot(json_loads(expected, [])):
                                mismatches.append(column)
                        elif str(actual) != str(expected):
                            mismatches.append(column)
                    if mismatches:
                        raise StoreError(
                            "profile version metadata conflicts: "
                            f"{agent_id}@{profile_version}"
                        )

                # Retirement disables every retained version.  Re-enable the
                # complete immutable history so registry publication remains
                # idempotent in the same process and after a restart.
                conn.execute(
                    "UPDATE agent_profiles SET enabled=1 WHERE agent_id=?",
                    (agent_id,),
                )
                return True

        return await self._call(op)

    async def commit_agent_reactivation(
        self,
        profile: Any,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        now: datetime | str | None = None,
    ) -> bool:
        """Publish a prepared Agent and its selected route atomically.

        This is the commit half of :meth:`reactivate_agent`.  It validates the
        requested current immutable Profile again, requires the complete
        retained Profile history to be enabled when a tombstone is present,
        then clears that tombstone and writes the route in one SQLite
        transaction.  The same guarded transaction is also safe for ordinary
        profile-backed switches, which linearizes them with concurrent Agent
        retirement.  A concurrent recreation that already cleared the
        tombstone is idempotent and may still add its own scope route.
        """

        data = self._mapping_snapshot(profile)
        if not data:
            raise ValueError("profile must be a mapping or profile value")
        agent_id = str(data.get("agent_id") or "").strip()
        if not agent_id:
            raise ValueError("profile.agent_id is required")
        profile_version = int(data.get("profile_version", data.get("version", 1)))
        values, supplied = self._profile_snapshot_values(
            agent_id=agent_id,
            profile_version=profile_version,
            snapshot=data,
        )
        if not supplied or int(values["enabled"]) != 1:
            raise ValueError("reactivated profile must be enabled")
        session_id = str(session_id or "default")
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT 1 FROM agent_profiles "
                    "WHERE agent_id=? AND profile_version=?",
                    (agent_id, profile_version),
                ).fetchone()
                if row is None:
                    raise StoreError(
                        f"cannot reactivate missing Agent profile: "
                        f"{agent_id}@{profile_version}"
                    )
                # Keep immutable conflict detection centralized.  The row is
                # known to exist, so this call can validate but cannot insert.
                self._ensure_profile_tx(
                    conn,
                    agent_id=agent_id,
                    profile_version=profile_version,
                    snapshot=data,
                    now=now_text,
                )
                tombstone = conn.execute(
                    "SELECT 1 FROM deleted_agents WHERE agent_id=?",
                    (agent_id,),
                ).fetchone()
                if tombstone is not None:
                    disabled = conn.execute(
                        "SELECT 1 FROM agent_profiles "
                        "WHERE agent_id=? AND enabled=0 LIMIT 1",
                        (agent_id,),
                    ).fetchone()
                    if disabled is not None:
                        raise StoreError(
                            f"Agent reactivation is incomplete: {agent_id}"
                        )
                conn.execute(
                    "DELETE FROM deleted_agents WHERE agent_id=?",
                    (agent_id,),
                )
                conn.execute(
                    """INSERT INTO routes(
                           channel, bot_id, external_user_id, session_id,
                           active_agent_id, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel, bot_id, external_user_id, session_id)
                       DO UPDATE SET
                           active_agent_id=excluded.active_agent_id,
                           updated_at=excluded.updated_at""",
                    (
                        channel,
                        bot_id,
                        external_user_id,
                        session_id,
                        agent_id,
                        now_text,
                    ),
                )
                return True

        return await self._call(op)

    @staticmethod
    def _assert_agent_retirable_tx(
        conn: sqlite3.Connection, agent_id: str
    ) -> None:
        placeholders = ",".join(
            "?" for _ in _AGENT_RETIREMENT_BLOCKING_TASK_STATES
        )
        blocking = conn.execute(
            "SELECT 1 FROM tasks WHERE agent_id=? "
            f"AND state IN ({placeholders}) LIMIT 1",
            (agent_id, *_AGENT_RETIREMENT_BLOCKING_TASK_STATES),
        ).fetchone()
        if blocking is not None:
            raise InvalidTransition(
                f"cannot delete Agent {agent_id}: unfinished tasks remain"
            )

    async def list_agents_routes(self, *, channel: str, bot_id: str, external_user_id: str, session_id: str = "default") -> list[dict[str, Any]]:
        def op(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute("SELECT * FROM routes WHERE channel=? AND bot_id=? AND external_user_id=? AND session_id=?", (channel, bot_id, external_user_id, session_id)).fetchall()
            return [dict(row) for row in rows]
        return await self._call(op)

    async def put_profile(self, profile: Any) -> bool:
        data = self._mapping_snapshot(profile)
        if not data:
            raise ValueError("profile must be a mapping or profile value")
        now = self._now()
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                self._ensure_profile_tx(
                    conn,
                    agent_id=str(data.get("agent_id") or "").strip(),
                    profile_version=int(data.get("profile_version", data.get("version", 1))),
                    snapshot=data,
                    now=now,
                )
                return True
        return await self._call(op)

    register_profile = put_profile

    async def get_profile(
        self, agent_id: str, profile_version: int | None = None
    ) -> Any | None:
        """Load an immutable public Agent profile snapshot."""
        def op(conn: sqlite3.Connection) -> Any | None:
            if profile_version is None:
                row = conn.execute(
                    "SELECT * FROM agent_profiles WHERE agent_id=? ORDER BY profile_version DESC LIMIT 1",
                    (agent_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM agent_profiles WHERE agent_id=? AND profile_version=?",
                    (agent_id, int(profile_version)),
                ).fetchone()
            if row is None:
                return None
            from .policy import AgentProfile

            return AgentProfile(
                agent_id=row["agent_id"],
                display_name=row["display_name"],
                summary=row["summary"],
                system_prompt=row["system_prompt"],
                responsibilities=tuple(json_loads(row["responsibilities_json"], []) or []),
                constraints=tuple(json_loads(row["constraints_json"], []) or []),
                capabilities=frozenset(json_loads(row["capabilities_json"], []) or []),
                allowed_peers=frozenset(json_loads(row["allowed_peers_json"], []) or []),
                denied_peers=frozenset(json_loads(row["denied_peers_json"], []) or []),
                allowed_request_types=frozenset(json_loads(row["allowed_request_types_json"], []) or []),
                denied_request_types=frozenset(
                    json_loads(row["denied_request_types_json"], []) or []
                )
                if "denied_request_types_json" in row.keys()
                else frozenset(),
                max_child_depth=int(row["max_child_depth"]),
                max_children_per_task=int(row["max_children_per_task"]),
                enabled=bool(row["enabled"]),
                profile_version=int(row["profile_version"]),
                default_mode_id=(row["default_mode_id"] if "default_mode_id" in row.keys() else "chat"),
            )

        return await self._call(op)

    load_profile = get_profile

    async def list_profiles(self, *, agent_id: str | None = None) -> list[Any]:
        values = []
        if agent_id is not None:
            values.append(agent_id)
        def op(conn: sqlite3.Connection) -> list[Any]:
            rows = conn.execute(
                "SELECT agent_id, profile_version FROM agent_profiles "
                + ("WHERE agent_id=? " if agent_id is not None else "")
                + "ORDER BY agent_id, profile_version",
                values,
            ).fetchall()
            return [
                # Conversion is kept in the async method's public helper so
                # callers receive the same immutable value type as get_profile.
                (row["agent_id"], int(row["profile_version"]))
                for row in rows
            ]
        keys = await self._call(op)
        result: list[Any] = []
        for aid, version in keys:
            profile = await self.get_profile(aid, version)
            if profile is not None:
                result.append(profile)
        return result

    async def mark_agent_deleted(
        self, agent_id: str, *, fallback_agent_id: str = "codex", now: datetime | str | None = None
    ) -> bool:
        """Tombstone a dynamic Agent and move routes back to a fallback."""
        agent_id = str(agent_id or "").strip()
        fallback_agent_id = str(fallback_agent_id or "codex").strip()
        if not agent_id:
            raise ValueError("agent_id is required")
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                self._assert_agent_retirable_tx(conn, agent_id)
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO deleted_agents(agent_id, deleted_at) VALUES (?, ?)",
                    (agent_id, now_text),
                )
                conn.execute(
                    "UPDATE routes SET active_agent_id=?, updated_at=? WHERE active_agent_id=?",
                    (fallback_agent_id, now_text, agent_id),
                )
                return bool(cursor.rowcount)

        return await self._call(op)

    async def clear_agent_deleted(self, agent_id: str) -> bool:
        agent_id = str(agent_id or "").strip()
        return bool(
            await self._call(
                lambda conn: conn.execute(
                    "DELETE FROM deleted_agents WHERE agent_id=?", (agent_id,)
                ).rowcount
            )
        )

    async def is_agent_deleted(self, agent_id: str) -> bool:
        agent_id = str(agent_id or "").strip()
        return bool(
            await self._call(
                lambda conn: conn.execute(
                    "SELECT 1 FROM deleted_agents WHERE agent_id=?", (agent_id,)
                ).fetchone()
            )
        )

    async def list_deleted_agents(self) -> list[str]:
        return await self._call(
            lambda conn: [
                str(row["agent_id"])
                for row in conn.execute(
                    "SELECT agent_id FROM deleted_agents ORDER BY agent_id"
                ).fetchall()
            ]
        )

    async def put_mode(self, mode: Any, *, agent_id: str = "codex") -> bool:
        data = self._mapping_snapshot(mode)
        if not data:
            raise ValueError("mode must be a mapping or mode value")
        aid = str(data.get("agent_id", agent_id) or agent_id); now = self._now()
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                self._ensure_mode_tx(
                    conn,
                    agent_id=aid,
                    mode_id=str(data.get("mode_id") or "").strip(),
                    policy_version=int(data.get("policy_version", data.get("version", 1))),
                    snapshot=data,
                    now=now,
                )
                return True
        return await self._call(op)

    register_mode = put_mode

    async def get_mode(
        self, agent_id: str, mode_id: str, policy_version: int | None = None
    ) -> Any | None:
        def op(conn: sqlite3.Connection) -> Any | None:
            if policy_version is None:
                row = conn.execute(
                    "SELECT * FROM agent_modes WHERE agent_id=? AND mode_id=? "
                    "ORDER BY policy_version DESC LIMIT 1",
                    (agent_id, mode_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM agent_modes WHERE agent_id=? AND mode_id=? AND policy_version=?",
                    (agent_id, mode_id, int(policy_version)),
                ).fetchone()
            if row is None:
                return None
            from .modes import AgentMode

            return AgentMode(
                mode_id=row["mode_id"],
                developer_instructions=row["developer_instructions"],
                sandbox_policy=str(row["sandbox_policy"]).replace("_", "-"),
                approval_policy=row["approval_policy"],
                allowed_tools=frozenset(json_loads(row["allowed_tools_json"], []) or []),
                denied_tools=frozenset(json_loads(row["denied_tools_json"], []) or []),
                can_write_files=bool(row["can_write_files"]),
                can_execute_commands=bool(row["can_execute_commands"]),
                can_create_child_tasks=bool(row["can_create_child_tasks"]),
                can_send_agent_messages=bool(row["can_send_agent_messages"]),
                policy_version=int(row["policy_version"]),
            )

        return await self._call(op)

    load_mode = get_mode

    async def list_modes(self, *, agent_id: str = "codex") -> list[Any]:
        def op(conn: sqlite3.Connection) -> list[tuple[str, int]]:
            rows = conn.execute(
                "SELECT mode_id, policy_version FROM agent_modes WHERE agent_id=? "
                "ORDER BY mode_id, policy_version",
                (agent_id,),
            ).fetchall()
            return [(str(row["mode_id"]), int(row["policy_version"])) for row in rows]

        keys = await self._call(op)
        result: list[Any] = []
        for mode_id, version in keys:
            mode = await self.get_mode(agent_id, mode_id, version)
            if mode is not None:
                result.append(mode)
        return result

    async def put_skill(self, skill: Any, *, agent_id: str = "codex") -> bool:
        """Register one immutable, Agent-scoped skill version."""

        from .skills import SkillDefinition, normalize_skill

        supplied = SkillDefinition.from_value(skill)
        if not supplied.version:
            raise ValueError("skill version must not be empty")
        definition = normalize_skill(skill)
        aid = str(agent_id or "").strip()
        version = str(definition.version or "").strip()
        if not aid:
            raise ValueError("agent_id must not be empty")
        if not definition.content_hash:
            raise ValueError("skill content_hash must not be empty")

        values = {
            "name": definition.name,
            "path": definition.path,
            "display_name": definition.display_name,
            "description": definition.description,
            "content_hash": definition.content_hash,
            "enabled": int(bool(definition.enabled)),
        }
        now = self._now()

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT * FROM skills WHERE agent_id=? AND skill_id=? AND version=?",
                    (aid, definition.skill_id, version),
                ).fetchone()
                if row is not None:
                    mismatches = [
                        column
                        for column, expected in values.items()
                        if str(row[column]) != str(expected)
                    ]
                    if mismatches:
                        raise StoreError(
                            "skill version metadata conflicts: "
                            f"{aid}/{definition.skill_id}@{version} "
                            f"({', '.join(mismatches)})"
                        )
                    return True
                conn.execute(
                    """INSERT INTO skills
                       (agent_id, skill_id, version, name, path, display_name,
                        description, content_hash, enabled, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        aid,
                        definition.skill_id,
                        version,
                        values["name"],
                        values["path"],
                        values["display_name"],
                        values["description"],
                        values["content_hash"],
                        values["enabled"],
                        now,
                    ),
                )
                return True

        return await self._call(op)

    register_skill = put_skill

    @staticmethod
    def _skill_from_row(row: sqlite3.Row) -> Any:
        from .skills import SkillDefinition

        return SkillDefinition(
            name=str(row["name"]),
            path=str(row["path"]),
            description=str(row["description"]),
            display_name=str(row["display_name"]),
            version=str(row["version"]),
            content_hash=str(row["content_hash"]),
            enabled=bool(row["enabled"]),
        )

    async def get_skill(
        self,
        agent_id: str,
        skill_id: str,
        version: str | None = None,
    ) -> Any | None:
        """Load an exact skill version, or the naturally latest version."""

        aid = str(agent_id or "").strip()
        canonical_id = str(skill_id or "").strip().casefold()
        if not aid or not canonical_id:
            return None
        requested_version = None if version is None else str(version).strip()
        if version is not None and not requested_version:
            raise ValueError("skill version must not be empty")

        def op(conn: sqlite3.Connection) -> Any | None:
            if requested_version is not None:
                row = conn.execute(
                    "SELECT * FROM skills WHERE agent_id=? AND skill_id=? AND version=?",
                    (aid, canonical_id, requested_version),
                ).fetchone()
            else:
                rows = conn.execute(
                    "SELECT * FROM skills WHERE agent_id=? AND skill_id=?",
                    (aid, canonical_id),
                ).fetchall()
                row = max(
                    rows,
                    key=lambda item: (
                        _skill_version_key(str(item["version"])),
                        str(item["version"]),
                    ),
                    default=None,
                )
            return None if row is None else self._skill_from_row(row)

        return await self._call(op)

    load_skill = get_skill

    async def list_skills(
        self,
        *,
        agent_id: str = "codex",
        enabled: bool | None = None,
    ) -> list[Any]:
        """List immutable skill versions in deterministic registry order."""

        aid = str(agent_id or "").strip()
        if not aid:
            return []

        def op(conn: sqlite3.Connection) -> list[Any]:
            parameters: list[Any] = [aid]
            predicate = "agent_id=?"
            if enabled is not None:
                predicate += " AND enabled=?"
                parameters.append(int(bool(enabled)))
            rows = conn.execute(
                f"SELECT * FROM skills WHERE {predicate}",
                parameters,
            ).fetchall()
            rows = sorted(
                rows,
                key=lambda row: (
                    str(row["skill_id"]),
                    _skill_version_key(str(row["version"])),
                    str(row["version"]),
                ),
            )
            return [self._skill_from_row(row) for row in rows]

        return await self._call(op)

    # ------------------------------------------------------------------
    # Recovery and diagnostics
    # ------------------------------------------------------------------
    async def reconcile(
        self,
        *,
        now: datetime | str | None = None,
        startup: bool = False,
    ) -> RecoveryReport:
        """Reconcile abandoned work without automatically retrying tasks.

        A live-process sweep only owns expired leases.  At a confirmed process
        boundary, however, every active claim belongs to the previous process
        even when its wall-clock lease has time remaining.  ``startup=True``
        applies that stronger recovery rule while retaining conservative task
        and delivery state transitions.
        """
        if startup:
            # Strong recovery is guarded by the process-local first-opener
            # boundary. Route this compatibility flag through the guarded API
            # so another live connection cannot reclaim healthy owners.
            return await self.startup_reconcile(now=now)
        now_text = self._now(now)
        recovery_reason = "lease expired"
        def op(conn: sqlite3.Connection) -> RecoveryReport:
            with _transaction(conn):
                # Repair counters left by the pre-atomic reserve/create path.
                # Durable child rows are the source of truth after restart;
                # an abandoned reservation must not consume capacity forever,
                # and a legacy direct insert must not bypass the limit.
                conn.execute(
                    """UPDATE child_task_counters
                       SET child_count=(
                               SELECT COUNT(*) FROM tasks child
                               WHERE child.parent_task_id =
                                     child_task_counters.parent_task_id
                           ),
                           updated_at=?""",
                    (now_text,),
                )
                conn.execute(
                    """INSERT INTO child_task_counters
                       (parent_task_id, child_count, updated_at)
                       SELECT child.parent_task_id, COUNT(*), ?
                       FROM tasks child
                       JOIN tasks parent ON parent.task_id=child.parent_task_id
                       WHERE child.parent_task_id IS NOT NULL
                       GROUP BY child.parent_task_id
                       ON CONFLICT(parent_task_id) DO UPDATE SET
                           child_count=excluded.child_count,
                           updated_at=excluded.updated_at""",
                    (now_text,),
                )
                task_sql = (
                    "SELECT task_id FROM tasks "
                    "WHERE state IN ('claimed','running','cancel_requested')"
                )
                task_params: tuple[Any, ...] = ()
                if not startup:
                    task_sql += " AND (lease_expires_at IS NULL OR lease_expires_at <= ?)"
                    task_params = (now_text,)
                task_rows = conn.execute(task_sql, task_params).fetchall()
                for row in task_rows:
                    current_task = self._fetch_task_tx(conn, row["task_id"])
                    if current_task is not None:
                        # Recovery is a real state transition, not just a
                        # lease-field cleanup.  Preserve an internal history
                        # event so operators can distinguish a restart/lease
                        # loss from an explicit failure or cancellation.
                        self._append_event_tx(
                            conn,
                            row["task_id"],
                            event_type="orphaned",
                            visibility=EventVisibility.INTERNAL,
                            priority=EventPriority.SILENT,
                            content=recovery_reason,
                            execution_id=current_task.execution_id,
                            idempotency_key=(
                                "recovery:orphaned:"
                                + str(
                                    current_task.execution_id
                                    or current_task.claim_token
                                    or current_task.task_id
                                )
                            ),
                            created_at=now_text,
                        )
                    conn.execute("UPDATE tasks SET state='orphaned', claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL, updated_at=?, last_error=COALESCE(last_error,?) WHERE task_id=?", (now_text, recovery_reason, row["task_id"]))
                    conn.execute("UPDATE task_executions SET state='orphaned', finished_at=?, lease_expires_at=NULL, last_error=COALESCE(last_error,?) WHERE task_id=? AND finished_at IS NULL", (now_text, recovery_reason, row["task_id"]))
                # A legacy/direct sender may have written ``sending`` before
                # lease support was enabled, leaving a NULL expiry.  Treat
                # that row as recoverable as well; modern rows always carry a
                # concrete lease from ``claim_outbox`` or
                # ``mark_outbox_sending(..., allow_pending=True)``.
                outbox_sql = (
                    "SELECT outbox_id, state FROM user_outbox "
                    "WHERE state IN ('claimed','sending')"
                )
                outbox_params: tuple[Any, ...] = ()
                if not startup:
                    outbox_sql += " AND (lease_expires_at IS NULL OR lease_expires_at <= ?)"
                    outbox_params = (now_text,)
                outbox_rows = conn.execute(outbox_sql, outbox_params).fetchall()
                outbox_requeued = 0
                outbox_unknown = 0
                for row in outbox_rows:
                    recovered_state = "delivery_unknown" if row["state"] == "sending" else "pending"
                    conn.execute("UPDATE user_outbox SET state=?, claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL, next_attempt_at=?, last_error=COALESCE(last_error,?) WHERE outbox_id=?", (recovered_state, now_text, recovery_reason, row["outbox_id"]))
                    if recovered_state == "pending": outbox_requeued += 1
                    else: outbox_unknown += 1
                mailbox_sql = (
                    "SELECT mailbox_id FROM agent_mailbox "
                    "WHERE state IN ('claimed','processing')"
                )
                mailbox_params: tuple[Any, ...] = ()
                if not startup:
                    mailbox_sql += " AND (lease_expires_at IS NULL OR lease_expires_at <= ?)"
                    mailbox_params = (now_text,)
                mailbox_rows = conn.execute(mailbox_sql, mailbox_params).fetchall()
                for row in mailbox_rows:
                    conn.execute("UPDATE agent_mailbox SET state='pending', claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL, next_attempt_at=?, last_error=COALESCE(last_error,?) WHERE mailbox_id=?", (now_text, recovery_reason, row["mailbox_id"]))
                # Upload/send operations have an independent lease. Uploading
                # is returned to the upload queue under the row's stable
                # idempotency key. Uploaded has not crossed the send checkpoint.
                # Send-pending is externally ambiguous, but the channel worker
                # reuses the stable media ID as its wire identity, so retaining
                # that checkpoint provides safe at-least-once recovery.
                media_sql = (
                    "SELECT media_id, state FROM outgoing_media "
                    "WHERE state IN ('uploading','send_pending','uploaded') "
                    "AND (claimed_by IS NOT NULL OR claim_token IS NOT NULL)"
                )
                media_params: tuple[Any, ...] = ()
                if not startup:
                    media_sql += " AND (lease_expires_at IS NULL OR lease_expires_at <= ?)"
                    media_params = (now_text,)
                media_rows = conn.execute(media_sql, media_params).fetchall()
                media_requeued = 0
                for row in media_rows:
                    recovered_state = (
                        "upload_pending"
                        if row["state"] == "uploading"
                        else row["state"]
                    )
                    changed = conn.execute(
                        "UPDATE outgoing_media SET state=?, claimed_by=NULL, "
                        "claim_token=NULL, lease_expires_at=NULL, "
                        "next_attempt_at=?, updated_at=?, "
                        "last_error=COALESCE(last_error, ?) WHERE media_id=?",
                        (
                            recovered_state,
                            now_text,
                            now_text,
                            recovery_reason
                            + "; external media operation is retryable by stable identity",
                            row["media_id"],
                        ),
                    ).rowcount
                    media_requeued += int(changed == 1)

                missing = 0
                attachment_sql = (
                    "SELECT attachment_id, path, checksum, size_bytes, state "
                    "FROM attachments WHERE state <> 'blocked_media'"
                )
                if not startup:
                    # A live cleanup reservation owns the file-system phase;
                    # the normal reconciliation sweep must not race it or
                    # steal its state transition. Startup includes these rows
                    # to repair a reservation left by a crashed process.
                    attachment_sql += " AND state <> 'deleting'"
                for row in conn.execute(attachment_sql).fetchall():
                    raw_path = row["path"]
                    invalid = not bool(raw_path)
                    path: Path | None = None
                    if raw_path:
                        path = Path(str(raw_path))
                        invalid = not path.is_file() or path.is_symlink()
                        if not invalid:
                            resolved = path.resolve(strict=False)
                            if self.attachment_root is None:
                                invalid = True
                            else:
                                try:
                                    resolved.relative_to(self.attachment_root)
                                except ValueError:
                                    invalid = True
                                else:
                                    invalid = resolved != path
                    if not invalid and path is not None:
                        try:
                            invalid = path.stat().st_size != int(row["size_bytes"] or 0)
                        except OSError:
                            invalid = True
                    if not invalid and path is not None and row["checksum"]:
                        try:
                            digest = hashlib.sha256(path.read_bytes()).hexdigest()
                            invalid = digest != row["checksum"]
                        except OSError:
                            invalid = True
                    if invalid:
                        conn.execute(
                            "UPDATE attachments SET state='blocked_media' WHERE attachment_id=?",
                            (row["attachment_id"],),
                        )
                        missing += 1
                    elif startup and str(row["state"] or "ready") == "deleting":
                        # A process may have crashed after reserving cleanup
                        # but before unlinking the file.  A valid file is
                        # recoverable; return it to the normal ready state.
                        conn.execute(
                            "UPDATE attachments SET state='ready' "
                            "WHERE attachment_id=? AND state='deleting'",
                            (row["attachment_id"],),
                        )
                return RecoveryReport(
                    len(task_rows),
                    outbox_requeued,
                    outbox_unknown,
                    len(mailbox_rows),
                    missing,
                    media_requeued,
                )
        return await self._call(op)

    recover = reconcile

    async def startup_reconcile(
        self, *, now: datetime | str | None = None
    ) -> RecoveryReport:
        """Reclaim every active lease left by the previous process.

        This is intentionally distinct from :meth:`reconcile`: callers that
        run a periodic expiry sweep must not invalidate a healthy worker whose
        lease has not elapsed, while a process startup has an explicit
        ownership boundary and can recover all prior claims.
        """
        # Initialization performs process-boundary recovery before the first
        # connection is published to other stores. Consume that report once so
        # the lifecycle owner retains its diagnostics without repeating the
        # state transition. A second live connection (or a repeated call on the
        # first) owns only expired leases.
        await self.initialize()
        async with self._lock:
            report = self._startup_recovery_report
            self._startup_recovery_report = None
        live_report = await self.reconcile(now=now, startup=False)
        if report is None:
            return live_report
        return RecoveryReport(
            tasks_orphaned=report.tasks_orphaned + live_report.tasks_orphaned,
            outbox_requeued=report.outbox_requeued + live_report.outbox_requeued,
            outbox_unknown=report.outbox_unknown + live_report.outbox_unknown,
            mailbox_requeued=report.mailbox_requeued + live_report.mailbox_requeued,
            missing_attachments=(
                report.missing_attachments + live_report.missing_attachments
            ),
            media_requeued=report.media_requeued + live_report.media_requeued,
        )

    async def recover_expired_leases(self, *, now: datetime | str | None = None) -> RecoveryReport:
        return await self.reconcile(now=now)

    async def health(self) -> dict[str, Any]:
        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            return {
                "path": self.path,
                "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
                "foreign_keys": bool(conn.execute("PRAGMA foreign_keys").fetchone()[0]),
                "queued_tasks": conn.execute("SELECT COUNT(*) FROM tasks WHERE state='queued'").fetchone()[0],
                "pending_outbox": conn.execute("SELECT COUNT(*) FROM user_outbox WHERE state IN ('pending','retry_wait')").fetchone()[0],
            }
        return await self._call(op)

    async def set_session_mode(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str = "codex",
        mode_id: str,
        policy_version: int = 1,
        authorized_by: str | None = None,
        authorized_at: datetime | str | None = None,
        now: datetime | str | None = None,
    ) -> bool:
        now_text = self._now(now)
        normalized_mode = str(mode_id or "").strip().lower()
        normalized_actor: str | None = None
        auth_text: str | None = None
        if normalized_mode == "execute":
            if authorized_by is not None or authorized_at is not None:
                evidence = _normalized_authorization_evidence(
                    authorized_by, authorized_at
                )
                if evidence is None:
                    raise ValueError(
                        "execute authorization requires a nonblank actor and "
                        "a valid timestamp"
                    )
                normalized_actor, auth_text = evidence

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                conn.execute(
                    """INSERT INTO session_modes
                       (channel, bot_id, external_user_id, session_id, agent_id,
                        mode_id, policy_version, authorized_by, authorized_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel, bot_id, external_user_id, session_id, agent_id)
                       DO UPDATE SET mode_id=excluded.mode_id,
                           policy_version=excluded.policy_version,
                           authorized_by=excluded.authorized_by,
                           authorized_at=excluded.authorized_at,
                           updated_at=excluded.updated_at""",
                    (channel, bot_id, external_user_id, session_id, agent_id,
                     mode_id, int(policy_version), normalized_actor, auth_text, now_text),
                )
                return True
        return await self._call(op)

    async def get_session_mode(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str = "codex",
        default_mode_id: str = "chat",
        default_policy_version: int = 1,
    ) -> tuple[str, int]:
        def op(conn: sqlite3.Connection) -> tuple[str, int]:
            row = conn.execute(
                """SELECT mode_id, policy_version FROM session_modes
                   WHERE channel=? AND bot_id=? AND external_user_id=?
                     AND session_id=? AND agent_id=?""",
                (channel, bot_id, external_user_id, session_id, agent_id),
            ).fetchone()
            return (
                (str(row["mode_id"]), int(row["policy_version"]))
                if row is not None
                else (default_mode_id, int(default_policy_version))
            )
        return await self._call(op)

    async def upgrade_collaboration_chat_preferences(
        self,
        agent_ids: Iterable[str],
        *,
        now: datetime | str | None = None,
    ) -> int:
        """Move mutable chat-v2 selections to an available chat-v3 mode.

        Profile, task, and conversation snapshots are immutable policy
        history.  ``session_modes`` instead selects the mode for a future
        task, so restored collaborative aliases may safely adopt the explicit
        v3 chat definition after that definition has been persisted.
        """

        normalized_ids = tuple(
            sorted(
                {
                    str(agent_id or "").strip()
                    for agent_id in agent_ids or ()
                    if str(agent_id or "").strip()
                }
            )
        )
        if not normalized_ids:
            return 0
        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> int:
            updated = 0
            with _transaction(conn):
                for agent_id in normalized_ids:
                    cursor = conn.execute(
                        """UPDATE session_modes
                           SET policy_version=3, updated_at=?
                           WHERE agent_id=?
                             AND mode_id='chat'
                             AND policy_version=2
                             AND EXISTS (
                                 SELECT 1
                                 FROM agent_modes
                                 WHERE agent_modes.agent_id=session_modes.agent_id
                                   AND agent_modes.mode_id='chat'
                                   AND agent_modes.policy_version=3
                             )""",
                        (now_text, agent_id),
                    )
                    updated += max(int(cursor.rowcount), 0)
            return updated

        return await self._call(op)

    async def set_session_model_preference(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str = "codex",
        model_id: str = "",
        reasoning_effort: str = "",
        now: datetime | str | None = None,
    ) -> bool:
        """Persist the model used by future tasks in one Agent session."""

        now_text = self._now(now)

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                conn.execute(
                    """INSERT INTO session_model_preferences
                       (channel, bot_id, external_user_id, session_id, agent_id,
                        model_id, reasoning_effort, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel, bot_id, external_user_id, session_id, agent_id)
                       DO UPDATE SET model_id=excluded.model_id,
                           reasoning_effort=excluded.reasoning_effort,
                           updated_at=excluded.updated_at""",
                    (
                        str(channel or ""),
                        str(bot_id or ""),
                        str(external_user_id or ""),
                        str(session_id or "default"),
                        str(agent_id or "codex"),
                        str(model_id or ""),
                        str(reasoning_effort or ""),
                        now_text,
                    ),
                )
                return True

        return await self._call(op)

    async def get_session_model_preference(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str = "codex",
        default_model_id: str = "",
        default_reasoning_effort: str = "",
    ) -> tuple[str, str]:
        """Return the durable future-task model selection for one Agent."""

        def op(conn: sqlite3.Connection) -> tuple[str, str]:
            row = conn.execute(
                """SELECT model_id, reasoning_effort
                   FROM session_model_preferences
                   WHERE channel=? AND bot_id=? AND external_user_id=?
                     AND session_id=? AND agent_id=?""",
                (
                    str(channel or ""),
                    str(bot_id or ""),
                    str(external_user_id or ""),
                    str(session_id or "default"),
                    str(agent_id or "codex"),
                ),
            ).fetchone()
            if row is None:
                return (
                    str(default_model_id or ""),
                    str(default_reasoning_effort or ""),
                )
            return (
                str(row["model_id"] or ""),
                str(row["reasoning_effort"] or ""),
            )

        return await self._call(op)

    set_model_preference = set_session_model_preference
    get_model_preference = get_session_model_preference

    async def is_mode_authorized(
        self,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str = "default",
        agent_id: str = "codex",
        mode_id: str = "execute",
        policy_version: int | None = None,
    ) -> bool:
        """Return whether a mode was explicitly authorized for this scope.

        Execute authorization is deliberately separate from the selected
        mode.  A restart must not infer write permission merely because a
        session row says ``mode_id=execute``; both the actor and timestamp
        are required durable evidence of an explicit authorization.
        """
        normalized = str(mode_id).strip().lower()

        def op(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                """SELECT mode_id, policy_version, authorized_by, authorized_at
                   FROM session_modes
                   WHERE channel=? AND bot_id=? AND external_user_id=?
                     AND session_id=? AND agent_id=?""",
                (channel, bot_id, external_user_id, session_id or "default", agent_id),
            ).fetchone()
            return bool(
                row is not None
                and str(row["mode_id"] or "").strip().lower() == normalized
                and (
                    policy_version is None
                    or int(row["policy_version"]) == int(policy_version)
                )
                and _normalized_authorization_evidence(
                    row["authorized_by"], row["authorized_at"]
                )
                is not None
            )

        return await self._call(op)

    mode_authorized = is_mode_authorized

    async def save_cursor(
        self, *, channel: str, bot_id: str, cursor: str, now: datetime | str | None = None
    ) -> bool:
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                existing = conn.execute(
                    "SELECT cursor FROM channel_cursors WHERE channel=? AND bot_id=?",
                    (channel, bot_id),
                ).fetchone()
                current_cursor = (
                    str(existing["cursor"])
                    if existing is not None
                    else None
                )
                if _cursor_should_advance(current_cursor, cursor):
                    conn.execute(
                        """INSERT INTO channel_cursors(channel, bot_id, cursor, updated_at)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(channel, bot_id)
                           DO UPDATE SET cursor=excluded.cursor,
                                         updated_at=excluded.updated_at""",
                        (channel, bot_id, cursor, now_text),
                    )
                return True
        return await self._call(op)

    async def clear_cursor(
        self,
        *,
        channel: str,
        bot_id: str,
        now: datetime | str | None = None,
    ) -> bool:
        """Clear the authoritative channel checkpoint after session expiry.

        A missing row is already the desired state, so this operation is
        idempotent.  Keeping reset separate from ``save_cursor`` prevents an
        opaque/lexically ordered cursor policy from accidentally retaining a
        stale checkpoint when a channel invalidates its session.
        """

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                conn.execute(
                    "DELETE FROM channel_cursors WHERE channel=? AND bot_id=?",
                    (channel, bot_id),
                )
                return True

        return await self._call(op)

    # Compatibility aliases used by channel adapters and older integrations.
    reset_cursor = clear_cursor
    delete_cursor = clear_cursor

    async def get_cursor(self, *, channel: str, bot_id: str) -> str:
        def op(conn: sqlite3.Connection) -> str:
            row = conn.execute("SELECT cursor FROM channel_cursors WHERE channel=? AND bot_id=?", (channel, bot_id)).fetchone()
            return str(row["cursor"] if row else "")
        return await self._call(op)

    async def reserve_child_task(
        self,
        parent_task_id: str,
        *,
        max_children: int,
        now: datetime | str | None = None,
    ) -> bool:
        """Atomically enforce a parent's child-task count limit."""
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                parent = conn.execute("SELECT task_id FROM tasks WHERE task_id=?", (parent_task_id,)).fetchone()
                if parent is None:
                    return False
                row = conn.execute("SELECT child_count FROM child_task_counters WHERE parent_task_id=?", (parent_task_id,)).fetchone()
                durable_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM tasks WHERE parent_task_id=?",
                        (parent_task_id,),
                    ).fetchone()[0]
                )
                count = max(int(row["child_count"]) if row else 0, durable_count)
                if max_children >= 0 and count >= max_children:
                    return False
                conn.execute(
                    """INSERT INTO child_task_counters(parent_task_id, child_count, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(parent_task_id) DO UPDATE SET
                           child_count=excluded.child_count,
                           updated_at=excluded.updated_at""",
                    (parent_task_id, count + 1, now_text),
                )
                return True
        return await self._call(op)

    async def child_count(self, parent_task_id: str) -> int:
        def op(conn: sqlite3.Connection) -> int:
            row = conn.execute("SELECT child_count FROM child_task_counters WHERE parent_task_id=?", (parent_task_id,)).fetchone()
            durable_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE parent_task_id=?",
                    (parent_task_id,),
                ).fetchone()[0]
            )
            return max(int(row["child_count"]) if row else 0, durable_count)
        return await self._call(op)

    async def release_child_task(self, parent_task_id: str, *, count: int = 1) -> bool:
        """Release a previously reserved child slot after enqueue failure."""
        amount = max(1, int(count))
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT child_count FROM child_task_counters WHERE parent_task_id=?",
                    (parent_task_id,),
                ).fetchone()
                if row is None:
                    return False
                current = int(row["child_count"])
                durable_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM tasks WHERE parent_task_id=?",
                        (parent_task_id,),
                    ).fetchone()[0]
                )
                # A compensating release may remove only uncommitted
                # reservations.  Never undercount children that already have
                # durable task rows.
                new_count = max(durable_count, current - amount)
                return conn.execute(
                    "UPDATE child_task_counters SET child_count=?, updated_at=? WHERE parent_task_id=?",
                    (new_count, self._now(), parent_task_id),
                ).rowcount == 1
        return await self._call(op)

    # ------------------------------------------------------------------
    # Attachment metadata and transcription candidates
    # ------------------------------------------------------------------
    @staticmethod
    def _attachment_from_row(row: sqlite3.Row | None) -> StoredAttachment | None:
        if row is None:
            return None
        return StoredAttachment(
            attachment_id=row["attachment_id"],
            path=row["path"] or "",
            mime_type=row["mime_type"] or "application/octet-stream",
            size=int(row["size_bytes"] or 0),
            checksum=row["checksum"] or "",
            filename=(json_loads(row["metadata_json"], {}) or {}).get("filename", ""),
            created_at=row["created_at"] or "",
        )

    async def register_attachment(
        self,
        attachment: StoredAttachment | Mapping[str, Any] | Any,
        *,
        kind: str = "file",
        metadata: Mapping[str, Any] | None = None,
        state: str = "ready",
    ) -> StoredAttachment:
        if isinstance(attachment, StoredAttachment):
            data: dict[str, Any] = {
                "attachment_id": attachment.attachment_id,
                "path": attachment.path,
                "mime_type": attachment.mime_type,
                "size": attachment.size,
                "checksum": attachment.checksum,
                "filename": attachment.filename,
                "created_at": attachment.created_at,
            }
        elif isinstance(attachment, Mapping):
            data = dict(attachment)
        else:
            data = {name: getattr(attachment, name) for name in (
                "attachment_id", "path", "mime_type", "size", "size_bytes", "checksum", "filename", "created_at"
            ) if hasattr(attachment, name)}
        aid = str(data.get("attachment_id") or _uuid())
        kind_value = str(data.get("kind") or kind or "file")
        now = data.get("created_at") or self._now()
        meta = dict(metadata or {})
        if data.get("filename"):
            meta.setdefault("filename", data["filename"])
        path_value = data.get("path")
        if path_value:
            if self.attachment_root is None:
                raise StoreError(
                    "attachment_root is required for managed attachment paths"
                )
            path_obj = Path(str(path_value)).expanduser()
            # Metadata may be registered before a download completes, so a
            # missing path is allowed and will be marked blocked_media during
            # reconciliation.  Symlinks are rejected up front to prevent a
            # later task from escaping the managed attachment root.
            if path_obj.is_symlink():
                raise StoreError("attachment path may not be a symlink")
            resolved_path = path_obj.resolve(strict=False)
            try:
                resolved_path.relative_to(self.attachment_root)
            except ValueError as exc:
                raise StoreError(
                    "attachment path escapes the managed attachment root"
                ) from exc
            # Persist the canonical path so a later restart cannot make a
            # relative path resolve somewhere different.
            path_obj = resolved_path
            data["path"] = str(path_obj)
            if path_obj.is_file() and data.get("checksum"):
                try:
                    digest = hashlib.sha256(path_obj.read_bytes()).hexdigest()
                except OSError as exc:
                    raise StoreError(f"cannot verify attachment: {aid}") from exc
                if digest != str(data["checksum"]):
                    raise StoreError(f"attachment checksum mismatch: {aid}")
            if path_obj.is_file() and data.get("size", data.get("size_bytes")) is not None:
                declared_size = int(data.get("size", data.get("size_bytes", 0)) or 0)
                actual_size = path_obj.stat().st_size
                if declared_size != actual_size:
                    raise StoreError(f"attachment size mismatch: {aid}")
        metadata_supplied = metadata is not None or bool(data.get("filename"))
        created_at_supplied = data.get("created_at") is not None

        def op(conn: sqlite3.Connection) -> StoredAttachment:
            with _transaction(conn):
                existing = conn.execute(
                    "SELECT * FROM attachments WHERE attachment_id = ?", (aid,)
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO attachments
                           (attachment_id, kind, path, mime_type, size_bytes, checksum,
                            metadata_json, created_at, state)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            aid,
                            kind_value,
                            data.get("path"),
                            data.get("mime_type", "application/octet-stream"),
                            int(data.get("size", data.get("size_bytes", 0)) or 0),
                            data.get("checksum", ""),
                            json_dumps(meta),
                            _utc_text(now),
                            state,
                        ),
                    )
                else:
                    if str(existing["state"] or "ready") == "deleting":
                        # A cleanup worker owns the filesystem phase for this
                        # ID.  Do not let a late publisher/register retry
                        # adopt or rewrite the row between the reservation and
                        # unlink; startup reconciliation will resolve it.
                        raise StoreError(f"attachment is unavailable: {aid}")
                    # Attachment metadata is immutable.  Re-registering the
                    # same ID is idempotent only when its identity is exactly
                    # the same; silently replacing a path/checksum could make
                    # a queued task read a different file after restart.
                    incoming = (
                        str(kind_value),
                        data.get("path"),
                        data.get("mime_type", "application/octet-stream"),
                        int(data.get("size", data.get("size_bytes", 0)) or 0),
                        data.get("checksum", ""),
                    )
                    stored = (
                        str(existing["kind"] or "file"),
                        existing["path"],
                        existing["mime_type"],
                        int(existing["size_bytes"] or 0),
                        existing["checksum"] or "",
                    )
                    if incoming != stored:
                        raise StoreError(f"attachment metadata is immutable: {aid}")
                    if metadata_supplied and self._json_snapshot(
                        json_loads(existing["metadata_json"], {}) or {}
                    ) != self._json_snapshot(meta):
                        raise StoreError(f"attachment metadata is immutable: {aid}")
                    if created_at_supplied and str(existing["created_at"] or "") != str(
                        _utc_text(now) or ""
                    ):
                        raise StoreError(f"attachment metadata is immutable: {aid}")
                record = self._attachment_from_row(conn.execute("SELECT * FROM attachments WHERE attachment_id=?", (aid,)).fetchone())
                if record is None: raise StoreError("attachment metadata insert failed")
                return record
        return await self._call(op)

    add_attachment = register_attachment

    async def get_attachment(self, attachment_id: str) -> StoredAttachment | None:
        return await self._call(lambda conn: self._attachment_from_row(conn.execute("SELECT * FROM attachments WHERE attachment_id=?", (attachment_id,)).fetchone()))

    async def get_attachment_state(self, attachment_id: str) -> str | None:
        """Return the authoritative lifecycle state for one attachment."""

        def op(conn: sqlite3.Connection) -> str | None:
            row = conn.execute(
                "SELECT state FROM attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            return str(row["state"]) if row is not None else None

        return await self._call(op)

    async def list_attachments(
        self,
        *,
        states: Iterable[str] | str | None = None,
        limit: int = 10_000,
    ) -> list[StoredAttachment]:
        """List immutable metadata for hydrating a managed file store."""

        if isinstance(states, str):
            state_values = [states]
        elif states is None:
            state_values = []
        else:
            state_values = [str(value) for value in states]

        def op(conn: sqlite3.Connection) -> list[StoredAttachment]:
            where = ""
            params: list[Any] = []
            if state_values:
                where = " WHERE state IN (" + ",".join("?" for _ in state_values) + ")"
                params.extend(state_values)
            params.append(max(0, int(limit)))
            rows = conn.execute(
                "SELECT * FROM attachments" + where
                + " ORDER BY created_at ASC, attachment_id ASC LIMIT ?",
                params,
            ).fetchall()
            return [
                item
                for row in rows
                if (item := self._attachment_from_row(row)) is not None
            ]

        return await self._call(op)

    @staticmethod
    def _attachment_metadata_scope(
        metadata: Mapping[str, Any] | Any,
    ) -> dict[str, Any] | None:
        """Return a complete immutable channel scope, if one is declared."""

        if not isinstance(metadata, Mapping):
            return None
        user_key = (
            "external_user_id"
            if "external_user_id" in metadata
            else "user_id"
            if "user_id" in metadata
            else None
        )
        required = ("channel", "bot_id", "session_id")
        if user_key is None or metadata.get(user_key) is None or any(
            key not in metadata or metadata.get(key) is None for key in required
        ):
            return None
        return {
            "channel": metadata.get("channel"),
            "bot_id": metadata.get("bot_id"),
            "external_user_id": metadata.get(user_key),
            "session_id": metadata.get("session_id") or "default",
        }

    @classmethod
    def _attachment_complete_scope_matches(
        cls,
        metadata: Mapping[str, Any] | Any,
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> bool:
        scope = cls._attachment_metadata_scope(metadata)
        if scope is None:
            return False
        expected = {
            "channel": str(channel or ""),
            "bot_id": str(bot_id or ""),
            "external_user_id": str(external_user_id or ""),
            "session_id": str(session_id or "default") or "default",
        }
        return all(
            str(scope[field]) == expected[field] for field in expected
        )

    @staticmethod
    def _attachment_scope_matches(
        row: Mapping[str, Any],
        *,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
    ) -> bool:
        expected = {
            "channel": channel,
            "bot_id": bot_id,
            "external_user_id": external_user_id,
            "session_id": session_id or "default",
        }
        for field, supplied in expected.items():
            if not supplied:
                continue
            actual = row[field] if field in row.keys() else None
            if field == "session_id":
                actual = actual or "default"
            if actual is not None and str(actual) != str(supplied):
                return False
        return True

    @classmethod
    def _attachment_accessible_tx(
        cls,
        conn: sqlite3.Connection,
        attachment_id: str,
        *,
        agent_id: str = "",
        task_id: str | None = None,
        inbound_message_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> bool:
        """Return whether a managed attachment is in the caller's durable ACL."""

        attachment = conn.execute(
            "SELECT metadata_json, state FROM attachments WHERE attachment_id=?",
            (attachment_id,),
        ).fetchone()
        if attachment is None or str(attachment["state"] or "ready") != "ready":
            return False

        metadata = json_loads(attachment["metadata_json"], {}) or {}
        inbound_context_id = str(inbound_message_id or "")
        if not inbound_context_id and task_id:
            task_context = conn.execute(
                "SELECT inbound_message_id FROM tasks WHERE task_id=?",
                (str(task_id),),
            ).fetchone()
            if task_context is not None:
                inbound_context_id = str(task_context["inbound_message_id"] or "")
        inbound_ref_for_context = bool(
            inbound_context_id
            and conn.execute(
                "SELECT 1 FROM attachment_refs WHERE attachment_id=? "
                "AND owner_kind='inbound_message' AND owner_id=? LIMIT 1",
                (attachment_id, inbound_context_id),
            ).fetchone()
        )
        inbound_metadata_matches = True
        if inbound_ref_for_context:
            inbound_owner = conn.execute(
                "SELECT channel, bot_id, external_user_id, session_id, "
                "external_message_id, payload_json FROM inbound_messages "
                "WHERE message_id=?",
                (inbound_context_id,),
            ).fetchone()
            inbound_route_agent = ""
            if inbound_owner is not None:
                inbound_payload = json_loads(inbound_owner["payload_json"], {}) or {}
                inbound_route = (
                    inbound_payload.get("__route_snapshot", {})
                    if isinstance(inbound_payload, Mapping)
                    else {}
                )
                if isinstance(inbound_route, Mapping):
                    inbound_route_agent = str(
                        inbound_route.get("agent_id") or ""
                    )
            inbound_metadata_matches = bool(
                inbound_owner is not None
                and isinstance(metadata, Mapping)
                and cls._attachment_complete_scope_matches(
                    metadata,
                    channel=str(inbound_owner["channel"] or ""),
                    bot_id=str(inbound_owner["bot_id"] or ""),
                    external_user_id=str(inbound_owner["external_user_id"] or ""),
                    session_id=str(inbound_owner["session_id"] or "default"),
                )
                and str(metadata.get("source_message_id") or "")
                == str(inbound_owner["external_message_id"] or "")
                and inbound_route_agent
                and inbound_route_agent == str(agent_id or "")
            )
        if isinstance(metadata, Mapping):
            owner_agent = str(
                metadata.get("owner_agent_id", metadata.get("agent_id", "")) or ""
            )
            metadata_scope = cls._attachment_metadata_scope(metadata)
            if (
                owner_agent
                and agent_id
                and owner_agent == str(agent_id)
                and metadata_scope is not None
                and (not inbound_ref_for_context or inbound_metadata_matches)
            ):
                if cls._attachment_complete_scope_matches(
                    metadata,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                ):
                    return True

        refs = conn.execute(
            "SELECT owner_kind, owner_id FROM attachment_refs WHERE attachment_id=?",
            (attachment_id,),
        ).fetchall()
        for ref in refs:
            kind = str(ref["owner_kind"] or "")
            owner_id = str(ref["owner_id"] or "")
            owner: sqlite3.Row | None = None
            owner_agents: set[str] = set()
            if kind == "task":
                owner = conn.execute(
                    "SELECT task_id, agent_id, channel, bot_id, external_user_id, "
                    "session_id FROM tasks WHERE task_id=?",
                    (owner_id,),
                ).fetchone()
                if owner is not None:
                    owner_agents.add(str(owner["agent_id"] or ""))
            elif kind == "inbound_message":
                owner = conn.execute(
                    "SELECT i.message_id, i.channel, i.bot_id, i.external_user_id, "
                    "i.session_id, i.external_message_id, i.payload_json, "
                    "i.task_id AS inbound_task_id, "
                    "t.agent_id FROM inbound_messages i "
                    "LEFT JOIN tasks t ON t.task_id=i.task_id WHERE i.message_id=?",
                    (owner_id,),
                ).fetchone()
                if owner is not None and owner["agent_id"]:
                    owner_agents.add(str(owner["agent_id"]))
                if owner is not None:
                    # A task row is inserted before its inbound projection is
                    # linked.  Treat that exact task->inbound identity as the
                    # owner during the enclosing transaction, without letting
                    # a same-user task borrow another envelope's media.
                    if task_id:
                        task_owner = conn.execute(
                            "SELECT agent_id, inbound_message_id FROM tasks "
                            "WHERE task_id=?",
                            (str(task_id),),
                        ).fetchone()
                        if (
                            task_owner is not None
                            and str(task_owner["inbound_message_id"] or "")
                            == owner_id
                        ):
                            owner_agents.add(str(task_owner["agent_id"] or ""))
                    if inbound_message_id and str(inbound_message_id) == owner_id:
                        inbound_payload = json_loads(owner["payload_json"], {}) or {}
                        route_snapshot = (
                            inbound_payload.get("__route_snapshot", {})
                            if isinstance(inbound_payload, Mapping)
                            else {}
                        )
                        route_agent = str(
                            route_snapshot.get("agent_id", "")
                            if isinstance(route_snapshot, Mapping)
                            else ""
                        )
                        if route_agent and route_agent == str(agent_id or ""):
                            owner_agents.add(route_agent)
                    # Inbound refs are valid only when the attachment was
                    # promoted for this exact route and source message.  This
                    # also fences legacy refs created from partial metadata.
                    if not isinstance(metadata, Mapping):
                        continue
                    declared_agent = str(
                        metadata.get(
                            "owner_agent_id", metadata.get("agent_id", "")
                        )
                        or ""
                    )
                    if declared_agent and (
                        not agent_id or declared_agent != str(agent_id)
                    ):
                        continue
                    if (
                        metadata_scope := cls._attachment_metadata_scope(metadata)
                    ) is None or metadata.get("source_message_id") in (None, ""):
                        continue
                    if not cls._attachment_scope_matches(
                        metadata_scope,
                        channel=str(owner["channel"] or ""),
                        bot_id=str(owner["bot_id"] or ""),
                        external_user_id=str(owner["external_user_id"] or ""),
                        session_id=str(owner["session_id"] or "default"),
                    ) or str(metadata.get("source_message_id")) != str(
                        owner["external_message_id"] or ""
                    ):
                        continue
            elif kind == "message":
                owner = conn.execute(
                    "SELECT m.source_agent_id, m.destination_agent_id, "
                    "t.channel, t.bot_id, t.external_user_id, t.session_id "
                    "FROM messages m LEFT JOIN tasks t ON t.task_id=m.task_id "
                    "WHERE m.message_id=?",
                    (owner_id,),
                ).fetchone()
                if owner is not None:
                    owner_agents.update(
                        str(value)
                        for value in (
                            owner["source_agent_id"],
                            owner["destination_agent_id"],
                        )
                        if value
                    )
            elif kind == "mailbox":
                owner = conn.execute(
                    "SELECT m.source_agent_id, m.destination_agent_id, "
                    "t.channel, t.bot_id, t.external_user_id, t.session_id "
                    "FROM agent_mailbox m LEFT JOIN tasks t ON t.task_id=m.task_id "
                    "WHERE m.mailbox_id=?",
                    (owner_id,),
                ).fetchone()
                if owner is not None:
                    owner_agents.update(
                        str(value)
                        for value in (
                            owner["source_agent_id"],
                            owner["destination_agent_id"],
                        )
                        if value
                    )
            elif kind == "outbox":
                owner = conn.execute(
                    "SELECT agent_id, channel, bot_id, external_user_id, "
                    "session_id FROM user_outbox WHERE outbox_id=?",
                    (owner_id,),
                ).fetchone()
                if owner is not None:
                    owner_agents.add(str(owner["agent_id"] or ""))
            if owner is None:
                continue
            if agent_id and str(agent_id) not in owner_agents:
                continue
            if not cls._attachment_scope_matches(
                owner,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id,
            ):
                continue
            # A concrete task context cannot borrow another Agent's task or
            # inbound attachment merely because the user/session happens to
            # be the same.  The responding Agent may still reuse an
            # attachment it owns from a different task (for example, a
            # mailbox reply is correlated to the original task's ID), so only
            # cross-Agent refs are fenced here.
            if task_id and kind in {"task", "inbound_message"} and owner_id != str(task_id):
                if not agent_id or not any(
                    str(value or "") == str(agent_id)
                    for value in owner_agents
                ):
                    continue
            return True
        return False

    async def can_access_attachment(
        self,
        attachment_id: str,
        *,
        agent_id: str = "",
        source_agent_id: str = "",
        task_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
    ) -> bool:
        resolved_agent = str(agent_id or source_agent_id or "")
        return await self._call(
            lambda conn: self._attachment_accessible_tx(
                conn,
                str(attachment_id),
                agent_id=resolved_agent,
                task_id=task_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id or "default",
            )
        )

    attachment_accessible = can_access_attachment
    check_attachment_access = can_access_attachment

    @classmethod
    def _attachment_ref_owner_context_tx(
        cls,
        conn: sqlite3.Connection,
        owner_kind: str,
        owner_id: str,
    ) -> dict[str, Any]:
        """Resolve a durable attachment-ref owner into its ACL context.

        ``attachment_refs`` is intentionally polymorphic, but accepting an
        arbitrary owner ID here would let a caller manufacture a new ACL
        edge.  Keep the owner lookup in the same transaction as the ref
        insert and derive the Agent/scope from the immutable owner row.
        """

        kind = str(owner_kind or "").strip()
        oid = str(owner_id or "").strip()
        if not oid:
            raise ValueError("attachment owner_id is required")
        supported = {"task", "message", "mailbox", "outbox", "inbound_message"}
        if kind not in supported:
            raise StoreError(f"unsupported attachment owner kind: {kind}")

        row: sqlite3.Row | None
        context: dict[str, Any] = {
            "agent_id": "",
            "owner_agents": set(),
            "task_id": None,
            "inbound_message_id": None,
            "channel": "",
            "bot_id": "",
            "external_user_id": "",
            "session_id": "default",
            "external_message_id": "",
        }
        if kind == "task":
            row = conn.execute(
                "SELECT task_id, agent_id, channel, bot_id, "
                "external_user_id, session_id FROM tasks WHERE task_id=?",
                (oid,),
            ).fetchone()
            if row is not None:
                context.update(
                    {
                        "agent_id": str(row["agent_id"] or ""),
                        "owner_agents": {str(row["agent_id"] or "")},
                        "task_id": oid,
                        "channel": str(row["channel"] or ""),
                        "bot_id": str(row["bot_id"] or ""),
                        "external_user_id": str(row["external_user_id"] or ""),
                        "session_id": str(row["session_id"] or "default"),
                    }
                )
        elif kind == "inbound_message":
            row = conn.execute(
                "SELECT i.message_id, i.channel, i.bot_id, i.external_user_id, "
                "i.session_id, i.external_message_id, i.payload_json, i.task_id, "
                "t.agent_id AS task_agent_id "
                "FROM inbound_messages i LEFT JOIN tasks t ON t.task_id=i.task_id "
                "WHERE i.message_id=?",
                (oid,),
            ).fetchone()
            if row is not None:
                payload = json_loads(row["payload_json"], {}) or {}
                route = (
                    payload.get("__route_snapshot", {})
                    if isinstance(payload, Mapping)
                    else {}
                )
                route_agent = (
                    str(route.get("agent_id") or "")
                    if isinstance(route, Mapping)
                    else ""
                )
                context.update(
                    {
                        "agent_id": str(row["task_agent_id"] or route_agent),
                        "owner_agents": {
                            value
                            for value in (
                                str(row["task_agent_id"] or ""),
                                route_agent,
                            )
                            if value
                        },
                        "task_id": str(row["task_id"] or "") or None,
                        "inbound_message_id": oid,
                        "channel": str(row["channel"] or ""),
                        "bot_id": str(row["bot_id"] or ""),
                        "external_user_id": str(row["external_user_id"] or ""),
                        "session_id": str(row["session_id"] or "default"),
                        "external_message_id": str(row["external_message_id"] or ""),
                    }
                )
        elif kind == "message":
            row = conn.execute(
                "SELECT m.message_id, m.source_agent_id, m.destination_agent_id, "
                "m.task_id, t.agent_id AS task_agent_id, t.channel, t.bot_id, "
                "t.external_user_id, t.session_id FROM messages m "
                "LEFT JOIN tasks t ON t.task_id=m.task_id WHERE m.message_id=?",
                (oid,),
            ).fetchone()
            if row is not None:
                context.update(
                    {
                        # The source Agent is the principal that can attach
                        # media to an Agent message; a response may carry a
                        # task owned by the other side of the exchange.
                        "agent_id": str(
                            row["source_agent_id"] or row["task_agent_id"] or ""
                        ),
                        "owner_agents": {
                            value
                            for value in (
                                str(row["source_agent_id"] or ""),
                                str(row["destination_agent_id"] or ""),
                                str(row["task_agent_id"] or ""),
                            )
                            if value
                        },
                        "task_id": str(row["task_id"] or "") or None,
                        "channel": str(row["channel"] or ""),
                        "bot_id": str(row["bot_id"] or ""),
                        "external_user_id": str(row["external_user_id"] or ""),
                        "session_id": str(row["session_id"] or "default"),
                    }
                )
        elif kind == "mailbox":
            row = conn.execute(
                "SELECT m.mailbox_id, m.source_agent_id, m.destination_agent_id, "
                "m.task_id, t.channel, t.bot_id, t.external_user_id, t.session_id "
                "FROM agent_mailbox m LEFT JOIN tasks t ON t.task_id=m.task_id "
                "WHERE m.mailbox_id=?",
                (oid,),
            ).fetchone()
            if row is not None:
                context.update(
                    {
                        "agent_id": str(row["source_agent_id"] or ""),
                        "owner_agents": {
                            value
                            for value in (
                                str(row["source_agent_id"] or ""),
                                str(row["destination_agent_id"] or ""),
                            )
                            if value
                        },
                        "task_id": str(row["task_id"] or "") or None,
                        "channel": str(row["channel"] or ""),
                        "bot_id": str(row["bot_id"] or ""),
                        "external_user_id": str(row["external_user_id"] or ""),
                        "session_id": str(row["session_id"] or "default"),
                    }
                )
        else:  # outbox
            row = conn.execute(
                "SELECT outbox_id, agent_id, task_id, channel, bot_id, "
                "external_user_id, session_id FROM user_outbox WHERE outbox_id=?",
                (oid,),
            ).fetchone()
            if row is not None:
                context.update(
                    {
                        "agent_id": str(row["agent_id"] or ""),
                        "owner_agents": {str(row["agent_id"] or "")},
                        "task_id": str(row["task_id"] or "") or None,
                        "channel": str(row["channel"] or ""),
                        "bot_id": str(row["bot_id"] or ""),
                        "external_user_id": str(row["external_user_id"] or ""),
                        "session_id": str(row["session_id"] or "default"),
                    }
                )

        if row is None:
            raise NotFoundError(f"{kind} attachment owner not found: {oid}")
        if not context["agent_id"]:
            raise StoreError(f"{kind} attachment owner has no Agent context: {oid}")
        return context

    async def add_attachment_ref(
        self,
        owner_kind: str,
        owner_id: str,
        attachment_id: str,
        *,
        role: str = "",
        ordinal: int = 0,
    ) -> bool:
        now = self._now()
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                kind = str(owner_kind or "").strip()
                oid = str(owner_id or "").strip()
                aid = str(attachment_id or "").strip()
                if not aid:
                    raise ValueError("attachment_id is required")
                attachment = conn.execute(
                    "SELECT metadata_json, state FROM attachments WHERE attachment_id=?",
                    (aid,),
                ).fetchone()
                if attachment is None:
                    raise NotFoundError(f"attachment not found: {aid}")
                if str(attachment["state"] or "ready") != "ready":
                    raise StoreError(f"attachment is unavailable: {aid}")

                owner = self._attachment_ref_owner_context_tx(conn, kind, oid)
                metadata = json_loads(attachment["metadata_json"], {}) or {}
                refs_exist = conn.execute(
                    "SELECT 1 FROM attachment_refs WHERE attachment_id=? LIMIT 1",
                    (aid,),
                ).fetchone() is not None
                declared_agent = (
                    str(
                        metadata.get("owner_agent_id", metadata.get("agent_id", ""))
                        or ""
                    )
                    if isinstance(metadata, Mapping)
                    else ""
                )

                if kind == "inbound_message":
                    # Inbound media is bound to the exact channel envelope and
                    # source message.  A same-user/same-Agent row is not a
                    # substitute for that immutable source binding.
                    exact_source = bool(
                        isinstance(metadata, Mapping)
                        and self._attachment_complete_scope_matches(
                            metadata,
                            channel=owner["channel"],
                            bot_id=owner["bot_id"],
                            external_user_id=owner["external_user_id"],
                            session_id=owner["session_id"],
                        )
                        and str(metadata.get("source_message_id") or "")
                        == str(owner["external_message_id"] or "")
                    )
                    if not exact_source or (
                        declared_agent and declared_agent != str(owner["agent_id"])
                    ):
                        raise StoreError(f"attachment access denied: {aid}")
                    # A fully promoted row with no prior ref can be adopted by
                    # its exact inbound owner even when older metadata omitted
                    # owner_agent_id.  Existing refs must pass the normal ACL.
                    if refs_exist and not self._attachment_accessible_tx(
                        conn,
                        aid,
                        agent_id=str(owner["agent_id"]),
                        task_id=owner["task_id"],
                        inbound_message_id=owner["inbound_message_id"],
                        channel=str(owner["channel"] or ""),
                        bot_id=str(owner["bot_id"] or ""),
                        external_user_id=str(owner["external_user_id"] or ""),
                        session_id=str(owner["session_id"] or "default"),
                    ):
                        raise StoreError(f"attachment access denied: {aid}")
                elif refs_exist or (
                    isinstance(metadata, Mapping)
                    and bool(
                        metadata.get("owner_agent_id")
                        or metadata.get("agent_id")
                        or metadata.get("channel")
                        or metadata.get("bot_id")
                        or metadata.get("external_user_id")
                        or metadata.get("user_id")
                    )
                ):
                    if not self._attachment_accessible_tx(
                        conn,
                        aid,
                        agent_id=str(owner["agent_id"]),
                        task_id=owner["task_id"],
                        channel=str(owner["channel"] or ""),
                        bot_id=str(owner["bot_id"] or ""),
                        external_user_id=str(owner["external_user_id"] or ""),
                        session_id=str(owner["session_id"] or "default"),
                    ):
                        raise StoreError(f"attachment access denied: {aid}")
                conn.execute(
                    """INSERT OR IGNORE INTO attachment_refs
                       (owner_kind, owner_id, attachment_id, role, ordinal, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (kind, oid, aid, role, int(ordinal), now),
                )
                return True
        return await self._call(op)

    async def remove_attachment_ref(
        self,
        owner_kind: str,
        owner_id: str,
        attachment_id: str,
        *,
        role: str = "",
        ordinal: int | None = None,
        agent_id: str | None = None,
        source_agent_id: str | None = None,
    ) -> bool:
        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                caller_agent = str(agent_id or source_agent_id or "").strip()
                if not caller_agent:
                    raise StoreError("attachment ref removal requires an Agent")
                kind = str(owner_kind or "").strip()
                oid = str(owner_id or "").strip()
                aid = str(attachment_id or "").strip()
                filters = "owner_kind=? AND owner_id=? AND attachment_id=? AND role=?"
                params: list[Any] = [kind, oid, aid, role]
                if ordinal is not None:
                    filters += " AND ordinal=?"
                    params.append(int(ordinal))
                ref = conn.execute(
                    "SELECT 1 FROM attachment_refs WHERE " + filters + " LIMIT 1",
                    params,
                ).fetchone()
                if ref is None:
                    return False
                owner = self._attachment_ref_owner_context_tx(conn, kind, oid)
                attachment = conn.execute(
                    "SELECT metadata_json, state FROM attachments WHERE attachment_id=?",
                    (aid,),
                ).fetchone()
                if attachment is None:
                    return False
                if caller_agent not in owner["owner_agents"]:
                    raise StoreError(f"attachment access denied: {aid}")
                metadata = json_loads(attachment["metadata_json"], {}) or {}
                if kind == "inbound_message":
                    if not (
                        isinstance(metadata, Mapping)
                        and self._attachment_complete_scope_matches(
                            metadata,
                            channel=owner["channel"],
                            bot_id=owner["bot_id"],
                            external_user_id=owner["external_user_id"],
                            session_id=owner["session_id"],
                        )
                        and str(metadata.get("source_message_id") or "")
                        == str(owner["external_message_id"] or "")
                        and (
                            not metadata.get("owner_agent_id", metadata.get("agent_id"))
                            or str(
                                metadata.get(
                                    "owner_agent_id", metadata.get("agent_id", "")
                                )
                            )
                            == str(owner["agent_id"])
                        )
                    ):
                        raise StoreError(f"attachment access denied: {aid}")
                elif not self._attachment_accessible_tx(
                    conn,
                    aid,
                    agent_id=caller_agent,
                    task_id=owner["task_id"],
                    inbound_message_id=owner["inbound_message_id"],
                    channel=str(owner["channel"] or ""),
                    bot_id=str(owner["bot_id"] or ""),
                    external_user_id=str(owner["external_user_id"] or ""),
                    session_id=str(owner["session_id"] or "default"),
                ):
                    raise StoreError(f"attachment access denied: {aid}")
                return conn.execute(
                    "DELETE FROM attachment_refs WHERE " + filters,
                    params,
                ).rowcount > 0
        return await self._call(op)

    @staticmethod
    def _attachment_live_referenced_tx(
        conn: sqlite3.Connection, attachment_id: str
    ) -> bool:
        """Check all durable live owners while the caller holds its TX lock."""

        # Polymorphic references cover task/message/outbox ownership; media
        # and transcription projections have dedicated foreign-key columns and
        # therefore participate in retention checks as well.  Sent/failed
        # delivery rows are historical once their explicit outbox ref is gone.
        return conn.execute(
            """SELECT 1
                 FROM attachment_refs r
                 LEFT JOIN user_outbox o
                   ON r.owner_kind='outbox' AND r.owner_id=o.outbox_id
                 WHERE r.attachment_id=?
                   AND (
                       r.owner_kind <> 'outbox'
                       OR o.outbox_id IS NULL
                       OR o.state NOT IN ('sent','failed_permanent')
                   )
               UNION ALL
               SELECT 1 FROM outgoing_media
                WHERE attachment_id=? AND state <> 'sent'
               UNION ALL
               SELECT 1 FROM transcription_candidates
                WHERE attachment_id=? AND status IN ('pending','confirmed')
               LIMIT 1""",
            (attachment_id, attachment_id, attachment_id),
        ).fetchone() is not None

    async def attachment_referenced(self, attachment_id: str) -> bool:
        return await self._call(
            lambda conn: self._attachment_live_referenced_tx(
                conn, str(attachment_id)
            )
        )

    async def claim_attachment_cleanup(
        self, attachment_id: str, *, allow_orphan: bool = False
    ) -> bool:
        """Atomically reserve an unreferenced attachment for file deletion.

        ``allow_orphan`` is used only by filesystem cleanup for a file left by
        a crash before SQLite registration.  Durable projection rows are still
        checked before such a file is released.
        """
        aid = str(attachment_id or "").strip()
        if not aid:
            return False

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT state FROM attachments WHERE attachment_id=?", (aid,)
                ).fetchone()
                if row is None:
                    if not allow_orphan or self._attachment_live_referenced_tx(
                        conn, aid
                    ):
                        return False
                    # A filesystem-only publication can exist after a crash
                    # between the atomic file replace and SQLite registration.
                    # Insert a durable tombstone before returning so a
                    # concurrent publisher cannot register the same ID while
                    # the unlink is in progress.  The tombstone is retained
                    # as ``blocked_media`` after deletion and reconciled like
                    # any other missing attachment.
                    try:
                        changed = conn.execute(
                            """INSERT INTO attachments
                               (attachment_id, kind, path, mime_type, size_bytes,
                                checksum, metadata_json, created_at, state)
                               VALUES (?, 'file', NULL, 'application/octet-stream',
                                       0, '', ?, ?, 'deleting')""",
                            (
                                aid,
                                json_dumps({"cleanup_orphan": True}),
                                self._now(),
                            ),
                        ).rowcount
                    except sqlite3.IntegrityError:
                        return False
                    return changed == 1
                row_state = str(row["state"] or "ready")
                if row_state not in {"ready", "blocked_media"}:
                    return False
                if self._attachment_live_referenced_tx(conn, aid):
                    return False
                return (
                    conn.execute(
                        "UPDATE attachments SET state='deleting' "
                        "WHERE attachment_id=? AND state=?",
                        (aid, row_state),
                    ).rowcount
                    == 1
                )

        return await self._call(op)

    async def finish_attachment_cleanup(
        self, attachment_id: str, *, deleted: bool
    ) -> bool:
        """Release a cleanup reservation after filesystem deletion/retry."""
        aid = str(attachment_id or "").strip()
        if not aid:
            return False

        def op(conn: sqlite3.Connection) -> bool:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT metadata_json FROM attachments WHERE attachment_id=?",
                    (aid,),
                ).fetchone()
                if row is None:
                    return True
                metadata = json_loads(row["metadata_json"], {}) or {}
                orphan_reservation = bool(
                    isinstance(metadata, Mapping)
                    and metadata.get("cleanup_orphan") is True
                )
                # A failed/unobserved unlink of an orphan tombstone must not
                # expose a metadata-free ready attachment.  Leave it blocked
                # so a later cleanup can retry without allowing adoption.
                target_state = (
                    "blocked_media" if deleted or orphan_reservation else "ready"
                )
                return (
                    conn.execute(
                        "UPDATE attachments SET state=? "
                        "WHERE attachment_id=? AND state='deleting'",
                        (target_state, aid),
                    ).rowcount
                    == 1
                )

        return await self._call(op)

    async def list_attachment_refs(
        self,
        attachment_id: str | None = None,
        *,
        owner_kind: str | None = None,
        owner_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List durable attachment references for retention/audit tooling."""
        filters: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("attachment_id", attachment_id),
            ("owner_kind", owner_kind),
            ("owner_id", owner_id),
        ):
            if value is not None:
                filters.append(f"{column}=?")
                params.append(value)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        def op(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT * FROM attachment_refs" + where +
                " ORDER BY owner_kind, owner_id, ordinal, role",
                params,
            ).fetchall()
            return [dict(row) for row in rows]
        return await self._call(op)

    @classmethod
    def _validate_transcription_inbound_owner_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        inbound_message_id: str | None,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
        mode_id: str,
        profile_version: int,
        policy_version: int,
        metadata: Mapping[str, Any],
        existing_candidate_status: str | None,
    ) -> None:
        """Prove that a candidate owns its linked inbound envelope.

        ``inbound_message_id`` is a relationship, not a caller-provided hint.
        Validate it in the same transaction as candidate insertion so a
        foreign candidate cannot change another route's confirmation state.
        """

        inbound_id = str(inbound_message_id or "").strip()
        if not inbound_id:
            return
        inbound = conn.execute(
            """SELECT i.message_id, i.channel, i.bot_id, i.external_user_id,
                      i.external_message_id, i.session_id, i.payload_json,
                      i.status, i.task_id AS inbound_task_id,
                      t.task_id AS linked_task_id,
                      t.inbound_message_id AS linked_task_inbound_id,
                      t.channel AS linked_task_channel,
                      t.bot_id AS linked_task_bot_id,
                      t.external_user_id AS linked_task_external_user_id,
                      t.session_id AS linked_task_session_id,
                      t.agent_id AS linked_task_agent_id,
                      t.mode_id AS linked_task_mode_id,
                      t.profile_version AS linked_task_profile_version,
                      t.policy_version AS linked_task_policy_version
               FROM inbound_messages i
               LEFT JOIN tasks t ON t.task_id=i.task_id
               WHERE i.message_id=?""",
            (inbound_id,),
        ).fetchone()
        if inbound is None:
            raise StoreError(
                f"transcription candidate inbound message not found: {inbound_id}"
            )

        expected_route = {
            "channel": str(channel or ""),
            "bot_id": str(bot_id or ""),
            "external_user_id": str(external_user_id or ""),
            "session_id": str(session_id or "default") or "default",
        }
        for field, expected in expected_route.items():
            default = "default" if field == "session_id" else ""
            persisted = str(inbound[field] or default)
            if persisted != expected:
                raise StoreError(
                    f"transcription candidate inbound route conflicts ({field})"
                )

        # Provenance assertions in candidate metadata must identify this same
        # durable envelope.  They are optional for compatibility, but never
        # act as wildcards when supplied.
        route_metadata = metadata.get("route")
        metadata_sources = [metadata]
        if isinstance(route_metadata, Mapping):
            metadata_sources.append(route_metadata)
        for source_metadata in metadata_sources:
            declared_inbound = source_metadata.get("inbound_message_id")
            if declared_inbound not in (None, "") and str(
                declared_inbound
            ) != inbound_id:
                raise StoreError(
                    "transcription candidate inbound source conflicts "
                    "(inbound_message_id)"
                )
            for field in ("source_message_id", "external_message_id"):
                declared_source = source_metadata.get(field)
                if declared_source not in (None, "") and str(
                    declared_source
                ) != str(inbound["external_message_id"] or ""):
                    raise StoreError(
                        f"transcription candidate inbound source conflicts ({field})"
                    )

        inbound_task_id = str(inbound["inbound_task_id"] or "")
        linked_task_id = str(inbound["linked_task_id"] or "")
        if inbound_task_id and linked_task_id != inbound_task_id:
            raise StoreError(
                "transcription candidate inbound ownership has no linked task"
            )
        if linked_task_id:
            if str(inbound["linked_task_inbound_id"] or "") != inbound_id:
                raise StoreError(
                    "transcription candidate inbound ownership has a foreign task"
                )
            for field, expected in expected_route.items():
                task_field = f"linked_task_{field}"
                default = "default" if field == "session_id" else ""
                if str(inbound[task_field] or default) != expected:
                    raise StoreError(
                        "transcription candidate inbound task route conflicts "
                        f"({field})"
                    )
        payload = json_loads(inbound["payload_json"], {}) or {}
        route_snapshot = (
            payload.get("__route_snapshot", {})
            if isinstance(payload, Mapping)
            else {}
        )
        if not isinstance(route_snapshot, Mapping):
            route_snapshot = {}

        def version_value(value: Any, field: str) -> int | None:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except (TypeError, ValueError) as exc:
                raise StoreError(
                    f"transcription candidate inbound ownership has invalid {field}"
                ) from exc

        task_owner: dict[str, Any] = {}
        if linked_task_id:
            task_owner = {
                "agent_id": str(inbound["linked_task_agent_id"] or ""),
                "mode_id": str(inbound["linked_task_mode_id"] or ""),
                "profile_version": version_value(
                    inbound["linked_task_profile_version"], "profile_version"
                ),
                "policy_version": version_value(
                    inbound["linked_task_policy_version"], "policy_version"
                ),
            }
        snapshot_owner = {
            "agent_id": str(route_snapshot.get("agent_id") or ""),
            "mode_id": str(route_snapshot.get("mode_id") or ""),
            "profile_version": version_value(
                route_snapshot.get("profile_version"), "profile_version"
            ),
            "policy_version": version_value(
                route_snapshot.get("policy_version"), "policy_version"
            ),
        }
        candidate_owner = {
            "agent_id": str(agent_id or ""),
            "mode_id": str(mode_id or ""),
            "profile_version": int(profile_version),
            "policy_version": int(policy_version),
        }
        for field, candidate_value in candidate_owner.items():
            task_value = task_owner.get(field)
            snapshot_value = snapshot_owner.get(field)
            if task_value not in (None, "") and snapshot_value not in (
                None,
                "",
            ) and task_value != snapshot_value:
                raise StoreError(
                    "transcription candidate inbound ownership is inconsistent "
                    f"({field})"
                )
            owner_value = task_value or snapshot_value
            if owner_value not in (None, "") and owner_value != candidate_value:
                raise StoreError(
                    "transcription candidate inbound ownership conflicts "
                    f"({field})"
                )

        if not (task_owner.get("agent_id") or snapshot_owner.get("agent_id")):
            raise StoreError(
                "transcription candidate inbound ownership is unavailable"
            )

        current = InboundState(str(inbound["status"]))
        if existing_candidate_status is None:
            allowed_states = {
                InboundState.STORED,
                InboundState.AWAITING_CONFIRMATION,
            }
        else:
            # Replays may observe an aggregate state advanced by a sibling
            # candidate.  Permit only states reachable for the stored
            # candidate status; accepted/duplicate ingress is never a voice
            # confirmation owner.
            allowed_replay_states = {
                "pending": {
                    InboundState.STORED,
                    InboundState.AWAITING_CONFIRMATION,
                    InboundState.CONFIRMED,
                    InboundState.TASK_QUEUED,
                },
                "confirmed": {
                    InboundState.STORED,
                    InboundState.AWAITING_CONFIRMATION,
                    InboundState.CONFIRMED,
                    InboundState.TASK_QUEUED,
                },
                "rejected": {
                    InboundState.STORED,
                    InboundState.AWAITING_CONFIRMATION,
                    InboundState.CONFIRMED,
                    InboundState.REJECTED,
                    InboundState.EXPIRED,
                    InboundState.TASK_QUEUED,
                },
                "expired": {
                    InboundState.STORED,
                    InboundState.AWAITING_CONFIRMATION,
                    InboundState.CONFIRMED,
                    InboundState.REJECTED,
                    InboundState.EXPIRED,
                    InboundState.TASK_QUEUED,
                },
                "consumed": {
                    InboundState.STORED,
                    InboundState.AWAITING_CONFIRMATION,
                    InboundState.CONFIRMED,
                    InboundState.TASK_QUEUED,
                },
            }
            allowed_states = allowed_replay_states.get(
                str(existing_candidate_status), set()
            )
        if current not in allowed_states:
            raise StoreError(
                "transcription candidate inbound state conflicts "
                f"({current.value})"
            )

    @classmethod
    def _insert_transcription_candidate_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        confirmation_id: str,
        channel: str,
        bot_id: str,
        external_user_id: str,
        session_id: str,
        agent_id: str,
        mode_id: str,
        profile_version: int,
        policy_version: int,
        metadata: Any,
        attachment_id: str | None,
        inbound_message_id: str | None,
        candidate_text: str,
        source: str,
        confidence: float | None,
        created_at: str,
        expires_at: str | None,
        created_at_supplied: bool = True,
        expires_at_supplied: bool = True,
    ) -> dict[str, Any]:
        """Insert or validate one immutable confirmation inside a transaction."""

        metadata_snapshot = cls._mapping_snapshot(metadata)
        route_metadata = metadata_snapshot.get("route")
        if isinstance(route_metadata, Mapping):
            for field, expected in {
                "channel": channel,
                "bot_id": bot_id,
                "external_user_id": external_user_id,
                "session_id": session_id or "default",
            }.items():
                supplied = route_metadata.get(field)
                if supplied not in (None, "") and str(supplied) != str(expected):
                    raise StoreError(
                        f"transcription candidate route conflicts ({field})"
                    )

        existing = conn.execute(
            "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
            (confirmation_id,),
        ).fetchone()
        cls._validate_transcription_inbound_owner_tx(
            conn,
            inbound_message_id=inbound_message_id,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id=session_id or "default",
            agent_id=agent_id,
            mode_id=mode_id,
            profile_version=int(profile_version),
            policy_version=int(policy_version),
            metadata=metadata_snapshot,
            existing_candidate_status=(
                str(existing["status"] or "") if existing is not None else None
            ),
        )
        if existing is not None:
            expected: dict[str, Any] = {
                "channel": channel,
                "bot_id": bot_id,
                "external_user_id": external_user_id,
                "session_id": session_id or "default",
                "agent_id": agent_id,
                "mode_id": mode_id,
                "candidate_text": candidate_text,
                "source": source,
                "confidence": confidence,
                "created_at": created_at,
                "expires_at": expires_at,
                "attachment_id": attachment_id,
                "inbound_message_id": inbound_message_id,
                "profile_version": int(profile_version),
                "policy_version": int(policy_version),
            }
            for column, value in expected.items():
                # Callers that omitted generated timestamps are replaying the
                # same logical candidate but do not possess the original clock
                # value.  They must not accidentally assert a fresh timestamp;
                # explicit values remain immutable and are checked below.
                if column == "created_at" and not created_at_supplied:
                    continue
                if column == "expires_at" and not expires_at_supplied:
                    continue
                stored = existing[column]
                if column == "confidence" and stored is not None and value is not None:
                    equal = float(stored) == float(value)
                else:
                    equal = (
                        stored is None and value is None
                    ) or str(stored) == str(value)
                if not equal:
                    raise StoreError(
                        f"transcription candidate identity is immutable: {confirmation_id}"
                    )
            incoming_metadata = cls._json_snapshot(metadata_snapshot)
            stored_metadata = cls._json_snapshot(
                json_loads(existing["metadata_json"], {}) or {}
            )
            if stored_metadata != incoming_metadata:
                raise StoreError(
                    f"transcription candidate identity is immutable: {confirmation_id}"
                )
            refreshed = conn.execute(
                "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
                (confirmation_id,),
            ).fetchone()
            if refreshed is None:  # pragma: no cover - transaction corruption guard
                raise StoreError("transcription candidate disappeared")
            linked_inbound = refreshed["inbound_message_id"]
            status_target = {
                "pending": InboundState.AWAITING_CONFIRMATION,
                "confirmed": InboundState.CONFIRMED,
                "rejected": InboundState.REJECTED,
                "expired": InboundState.EXPIRED,
                "consumed": InboundState.TASK_QUEUED,
            }.get(str(refreshed["status"] or ""))
            if status_target is not None:
                if status_target in {InboundState.REJECTED, InboundState.EXPIRED}:
                    cls._advance_terminal_confirmation_inbound_tx(
                        conn,
                        linked_inbound,
                        confirmation_id,
                        status_target,
                        now=created_at,
                    )
                else:
                    cls._advance_confirmation_inbound_tx(
                        conn,
                        linked_inbound,
                        status_target,
                        now=created_at,
                        task_id=(
                            refreshed["consumed_by_task_id"]
                            if status_target == InboundState.TASK_QUEUED
                            else None
                        ),
                    )
            return dict(refreshed)

        if attachment_id:
            cls._validate_task_input_attachment_access_tx(
                conn,
                task_id=None,
                inbound_message_id=inbound_message_id,
                inputs={"attachment_id": attachment_id},
                agent_id=agent_id,
                channel=channel,
                bot_id=bot_id,
                external_user_id=external_user_id,
                session_id=session_id or "default",
                require_registered=True,
            )

        conn.execute(
            """INSERT INTO transcription_candidates
               (confirmation_id, channel, bot_id, external_user_id, session_id,
                agent_id, mode_id, profile_version, policy_version, metadata_json,
                attachment_id, inbound_message_id, candidate_text, source,
                confidence, status, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
            (
                confirmation_id,
                channel,
                bot_id,
                external_user_id,
                session_id or "default",
                agent_id,
                mode_id,
                int(profile_version),
                int(policy_version),
                json_dumps(cls._json_snapshot(metadata_snapshot)),
                attachment_id,
                inbound_message_id,
                candidate_text,
                source,
                confidence,
                created_at,
                expires_at,
            ),
        )
        cls._advance_confirmation_inbound_tx(
            conn,
            inbound_message_id,
            InboundState.AWAITING_CONFIRMATION,
            now=created_at,
        )
        created = conn.execute(
            "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
            (confirmation_id,),
        ).fetchone()
        if created is None:
            raise StoreError("transcription candidate insert failed")
        return dict(created)

    async def create_transcription_candidate(
        self,
        *,
        confirmation_id: str | None = None,
        channel: str = "",
        bot_id: str = "",
        external_user_id: str = "",
        session_id: str = "default",
        agent_id: str = "codex",
        mode_id: str = "chat",
        profile_version: int = 1,
        policy_version: int = 1,
        metadata: Mapping[str, Any] | None = None,
        attachment_id: str | None = None,
        inbound_message_id: str | None = None,
        candidate_text: str,
        source: str = "",
        confidence: float | None = None,
        expires_at: datetime | str | None | object = _UNSET,
        created_at: datetime | str | None | object = _UNSET,
    ) -> dict[str, Any]:
        if not candidate_text.strip():
            raise ValueError("candidate_text cannot be empty")
        cid = confirmation_id or _uuid()
        created_at_supplied = created_at is not _UNSET and created_at is not None
        expires_at_supplied = expires_at is not _UNSET
        created = _utc_text(created_at if created_at_supplied else self._now())
        expiry = (
            _utc_text(expires_at)
            if expires_at is not _UNSET and expires_at is not None
            else (
                None
                if expires_at is None
                else self._default_transcription_expiry(created)
            )
        )
        def op(conn: sqlite3.Connection) -> dict[str, Any]:
            with _transaction(conn):
                return self._insert_transcription_candidate_tx(
                    conn,
                    confirmation_id=cid,
                    channel=channel,
                    bot_id=bot_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                    agent_id=agent_id,
                    mode_id=mode_id,
                    profile_version=int(profile_version),
                    policy_version=int(policy_version),
                    metadata=metadata or {},
                    attachment_id=attachment_id,
                    inbound_message_id=inbound_message_id,
                    candidate_text=candidate_text,
                    source=source,
                    confidence=confidence,
                    created_at=created,
                    expires_at=expiry,
                    created_at_supplied=created_at_supplied,
                    expires_at_supplied=expires_at_supplied,
                )
        value = await self._call(op)
        value["metadata"] = json_loads(value.get("metadata_json"), {}) or {}
        return value

    async def get_transcription_candidate(self, confirmation_id: str) -> dict[str, Any] | None:
        def op(conn: sqlite3.Connection) -> dict[str, Any] | None:
            # Reads are also a reconciliation point.  A process may restart
            # without a timer firing, so a pending/confirmed row past its
            # deadline must not remain apparently actionable in the durable
            # state machine.
            now_text = self._now()
            now_value = text_to_datetime(now_text) or utcnow()
            row = conn.execute("SELECT * FROM transcription_candidates WHERE confirmation_id=?", (confirmation_id,)).fetchone()
            if row is None:
                return None
            status = str(row["status"] or "pending")
            expires = text_to_datetime(row["expires_at"])
            expiry_supplied = row["expires_at"] not in (None, "")
            if status in {"pending", "confirmed"} and (
                (expiry_supplied and expires is None)
                or (expires is not None and expires <= now_value)
            ):
                with _transaction(conn):
                    conn.execute(
                        "UPDATE transcription_candidates SET status='expired', "
                        "resolved_at=COALESCE(resolved_at, ?), "
                        "resolved_by=COALESCE(resolved_by, 'expiry') "
                        "WHERE confirmation_id=? AND status=?",
                        (now_text, confirmation_id, status),
                    )
                    self._advance_terminal_confirmation_inbound_tx(
                        conn,
                        row["inbound_message_id"]
                        if "inbound_message_id" in row.keys()
                        else None,
                        confirmation_id,
                        InboundState.EXPIRED,
                        now=now_text,
                    )
                    row = conn.execute(
                        "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
                        (confirmation_id,),
                    ).fetchone()
            value = dict(row)
            value["metadata"] = json_loads(value.get("metadata_json"), {}) or {}
            return value
        return await self._call(op)

    async def resolve_transcription(
        self, confirmation_id: str, *, status: str, actor: str = "", now: datetime | str | None = None
    ) -> bool:
        if status not in {"confirmed", "rejected", "expired"}:
            raise ValueError("invalid transcription resolution status")
        now_text = self._now(now)
        def op(conn: sqlite3.Connection) -> bool | str:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT status, expires_at, inbound_message_id FROM transcription_candidates "
                    "WHERE confirmation_id=?",
                    (confirmation_id,),
                ).fetchone()
                if row is None:
                    return False
                current = str(row["status"] or "pending")
                if current != "pending":
                    return False
                expires = text_to_datetime(row["expires_at"])
                now_value = text_to_datetime(now_text) or utcnow()
                # Expiry is authoritative over a late human confirmation.
                # Persist the expiration in this same transaction so retries
                # observe a stable terminal state.
                if (
                    (row["expires_at"] not in (None, "") and expires is None)
                    or (expires is not None and expires <= now_value)
                ):
                    changed = conn.execute(
                        "UPDATE transcription_candidates SET status='expired', "
                        "resolved_at=?, resolved_by='expiry' WHERE confirmation_id=? "
                        "AND status='pending'",
                        (now_text, confirmation_id),
                    ).rowcount == 1
                    if status == "expired":
                        self._advance_terminal_confirmation_inbound_tx(
                            conn,
                            row["inbound_message_id"],
                            confirmation_id,
                            InboundState.EXPIRED,
                            now=now_text,
                        )
                        return changed
                    self._advance_terminal_confirmation_inbound_tx(
                        conn,
                        row["inbound_message_id"],
                        confirmation_id,
                        InboundState.EXPIRED,
                        now=now_text,
                    )
                    # Return a sentinel so the transaction commits the
                    # expiration before the public method reports the invalid
                    # late confirmation.
                    return "expired"
                changed = conn.execute(
                    """UPDATE transcription_candidates SET status=?, resolved_at=?, resolved_by=?
                       WHERE confirmation_id=? AND status='pending'""",
                    (status, now_text, actor, confirmation_id),
                ).rowcount == 1
                if changed:
                    inbound_target = {
                        "confirmed": InboundState.CONFIRMED,
                        "rejected": InboundState.REJECTED,
                        "expired": InboundState.EXPIRED,
                    }[status]
                    if status in {"rejected", "expired"}:
                        self._advance_terminal_confirmation_inbound_tx(
                            conn,
                            row["inbound_message_id"],
                            confirmation_id,
                            inbound_target,
                            now=now_text,
                        )
                    else:
                        self._advance_confirmation_inbound_tx(
                            conn,
                            row["inbound_message_id"],
                            inbound_target,
                            now=now_text,
                        )
                return changed
        outcome = await self._call(op)
        if outcome == "expired":
            raise InvalidTransition("transcription candidate has expired")
        return bool(outcome)

    async def consume_transcription(self, confirmation_id: str, task_id: str) -> bool:
        def op(conn: sqlite3.Connection) -> bool | str:
            with _transaction(conn):
                row = conn.execute(
                    "SELECT status, expires_at, inbound_message_id FROM transcription_candidates "
                    "WHERE confirmation_id=?",
                    (confirmation_id,),
                ).fetchone()
                if row is None:
                    return False
                if str(row["status"] or "") != "confirmed":
                    return False
                expires = text_to_datetime(row["expires_at"])
                if (
                    (row["expires_at"] not in (None, "") and expires is None)
                    or (
                        expires is not None
                        and expires <= (text_to_datetime(self._now()) or utcnow())
                    )
                ):
                    conn.execute(
                        "UPDATE transcription_candidates SET status='expired', "
                        "resolved_at=COALESCE(resolved_at, ?), "
                        "resolved_by=COALESCE(resolved_by, 'expiry') "
                        "WHERE confirmation_id=? AND status='confirmed'",
                        (self._now(), confirmation_id),
                    )
                    self._advance_terminal_confirmation_inbound_tx(
                        conn,
                        row["inbound_message_id"],
                        confirmation_id,
                        InboundState.EXPIRED,
                        now=self._now(),
                    )
                    return "expired"
                # When a concrete task row exists, consumption is fenced to
                # the candidate-owned projection.  Keep the historical
                # compatibility behavior for callers that pass an external
                # task identifier without a durable task row (the standalone
                # in-memory confirmation manager uses that form), but never
                # let an existing cross-user/different-input task claim this
                # candidate.
                task_row = self._fetch_task_tx(conn, task_id) if task_id else None
                if task_row is not None:
                    candidate_full = conn.execute(
                        "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
                        (confirmation_id,),
                    ).fetchone()
                    if candidate_full is None:  # pragma: no cover
                        return False
                    self._validate_confirmation_task_tx(
                        conn,
                        task_row,
                        self._confirmation_snapshot_tx(
                            conn, candidate_full, confirmation_id
                        ),
                        confirmation_id,
                    )
                changed = conn.execute(
                    """UPDATE transcription_candidates SET status='consumed', consumed_by_task_id=?
                       WHERE confirmation_id=? AND status='confirmed'
                         AND (consumed_by_task_id IS NULL OR consumed_by_task_id=?)""",
                    (task_id, confirmation_id, task_id),
                ).rowcount == 1
                if changed:
                    self._advance_confirmation_inbound_tx(
                        conn,
                        row["inbound_message_id"],
                        InboundState.TASK_QUEUED,
                        now=self._now(),
                        task_id=task_id,
                    )
                return changed
        outcome = await self._call(op)
        if outcome == "expired":
            raise InvalidTransition("transcription candidate has expired")
        return bool(outcome)

    async def confirm_transcription_task(
        self,
        confirmation_id: str,
        *,
        task: AgentTask | Mapping[str, Any] | None = None,
        actor: str = "",
        now: datetime | str | None = None,
    ) -> TaskRecord:
        """Confirm and consume one audio candidate while creating one task.

        Candidate resolution, task deduplication, and consumption happen in a
        single SQLite transaction.  A retried ``/confirm`` therefore returns
        the original task instead of creating a second execution input.
        """
        now_text = self._now(now)
        task_values: dict[str, Any] = {}
        if isinstance(task, Mapping):
            task_values.update(task)
        elif task is not None:
            task_values.update(
                {
                    name: getattr(task, name)
                    for name in getattr(task, "__dataclass_fields__", {})
                    if hasattr(task, name)
                }
            )

        def op(conn: sqlite3.Connection) -> TaskRecord:
            with _transaction(conn):
                candidate = conn.execute(
                    "SELECT * FROM transcription_candidates WHERE confirmation_id=?",
                    (confirmation_id,),
                ).fetchone()
                if candidate is None:
                    raise NotFoundError(f"transcription candidate not found: {confirmation_id}")
                status = str(candidate["status"] or "pending")
                confirmation_snapshot = self._confirmation_snapshot_tx(
                    conn, candidate, confirmation_id
                )
                expires = text_to_datetime(candidate["expires_at"])
                # Expiry is authoritative for both an unconfirmed row and a
                # row confirmed by an older compatibility path but not yet
                # consumed.  Persist the terminal state before returning the
                # error; raising inside the transaction would roll it back.
                if status in {"pending", "confirmed"}:
                    if (
                        (candidate["expires_at"] not in (None, "") and expires is None)
                        or (
                            expires is not None
                            and expires <= (text_to_datetime(now_text) or utcnow())
                        )
                    ):
                        conn.execute(
                            "UPDATE transcription_candidates SET status='expired', "
                            "resolved_at=COALESCE(resolved_at, ?), "
                            "resolved_by=COALESCE(resolved_by, 'expiry') "
                            "WHERE confirmation_id=? AND status=?",
                            (now_text, confirmation_id, status),
                        )
                        self._advance_terminal_confirmation_inbound_tx(
                            conn,
                            confirmation_snapshot["inbound_id"],
                            confirmation_id,
                            InboundState.EXPIRED,
                            now=now_text,
                        )
                        # Commit the durable expiry before reporting the late
                        # confirmation to the caller.
                        return None
                    if status == "pending":
                        conn.execute(
                            "UPDATE transcription_candidates SET status='confirmed', resolved_at=?, resolved_by=? "
                            "WHERE confirmation_id=? AND status='pending'",
                            (now_text, actor, confirmation_id),
                        )
                        status = "confirmed"
                        self._advance_confirmation_inbound_tx(
                            conn, confirmation_snapshot["inbound_id"],
                            InboundState.CONFIRMED,
                            now=now_text,
                        )
                if status == "consumed":
                    prior_id = candidate["consumed_by_task_id"]
                    prior = self._fetch_task_tx(conn, prior_id) if prior_id else None
                    if prior is not None:
                        self._validate_confirmation_task_tx(
                            conn,
                            prior,
                            confirmation_snapshot,
                            confirmation_id,
                            consumed=True,
                        )
                        self._advance_confirmation_inbound_tx(
                            conn, confirmation_snapshot["inbound_id"],
                            InboundState.TASK_QUEUED,
                            now=now_text,
                            task_id=prior.task_id,
                        )
                        return prior
                    raise InvalidTransition("transcription candidate was already consumed")
                if status != "confirmed":
                    raise InvalidTransition(f"transcription candidate is {status}")

                route = confirmation_snapshot["route"]
                original_target = confirmation_snapshot["target"]
                supplied_target = task_values.get("reply_target")
                if supplied_target is not None:
                    supplied_target = self._coerce_reply_target(supplied_target)
                    # A caller-provided target is an assertion about the
                    # caller's ownership.  Missing scope fields are not
                    # treated as wildcards when the candidate has a concrete
                    # owner; otherwise a partial/cross-user replay could be
                    # mistaken for the original confirmation.
                    for field in ("channel", "bot_id", "external_user_id"):
                        expected = str(route[field] or "")
                        supplied = str(getattr(supplied_target, field) or "")
                        if expected and supplied != expected:
                            raise StoreError(
                                f"confirmation reply target conflicts ({field})"
                            )
                    supplied_session = str(supplied_target.session_id or "default")
                    if supplied_session != route["session_id"]:
                        raise StoreError(
                            "confirmation reply target conflicts (session_id)"
                        )
                    for field in ("source_message_id", "source_sequence"):
                        supplied_value = getattr(supplied_target, field)
                        expected_value = getattr(original_target, field)
                        if supplied_value is not None and supplied_value != expected_value:
                            raise StoreError(
                                f"confirmation reply target conflicts ({field})"
                            )
                # The candidate is the immutable owner of route and policy.
                # Never recompute mode/profile from the current session route
                # during a later /confirm after the user has switched Agents.
                task_values["agent_id"] = str(candidate["agent_id"] or "codex")
                task_values["mode_id"] = str(
                    candidate["mode_id"] or task_values.get("mode_id") or "chat"
                )
                task_values["profile_version"] = int(
                    candidate["profile_version"]
                    if candidate["profile_version"] is not None
                    else task_values.get("profile_version", 1)
                )
                task_values["policy_version"] = int(
                    candidate["policy_version"]
                    if candidate["policy_version"] is not None
                    else task_values.get("policy_version", 1)
                )
                stored_metadata = json_loads(
                    candidate["metadata_json"] if "metadata_json" in candidate.keys() else None,
                    None,
                )
                if stored_metadata is not None:
                    task_values["metadata"] = stored_metadata or {}
                task_values["reply_target"] = original_target
                canonical_inputs = dict(confirmation_snapshot["inputs"])
                supplied_inputs = self._mapping_snapshot(task_values.get("inputs"))
                # Confirmation input is candidate-owned.  Permit callers to
                # repeat the canonical fields for compatibility, but reject
                # any extra or conflicting values so a replay cannot create a
                # task with a different prompt/media snapshot.
                for key, value in supplied_inputs.items():
                    if key not in canonical_inputs:
                        raise StoreError(
                            f"confirmation task input conflicts ({key})"
                        )
                    if self._json_snapshot(value) != self._json_snapshot(
                        canonical_inputs[key]
                    ):
                        raise StoreError(
                            f"confirmation task input conflicts ({key})"
                        )
                task_values["inputs"] = canonical_inputs
                # Confirmation identity is the logical task identity.  A
                # caller-supplied task snapshot may contribute model/input
                # fields, but it cannot opt out of exactly-once consumption.
                task_values["dedupe_key"] = f"confirmation:{confirmation_id}"
                expected_conversation = str(confirmation_snapshot["conversation_id"])
                supplied_conversation = str(task_values.get("conversation_id") or "")
                if supplied_conversation and not conversation_id_matches(
                    supplied_conversation,
                    route["channel"],
                    route["bot_id"],
                    route["external_user_id"],
                    route["session_id"],
                    task_values["agent_id"],
                ):
                    raise StoreError(
                        "confirmation task identity conflicts (conversation_id)"
                    )
                task_values["conversation_id"] = (
                    supplied_conversation or expected_conversation
                )
                snapshot = self._coerce_task(task, task_values)
                existing = self._fetch_task_by_dedupe_tx(conn, snapshot.dedupe_key)
                if existing is not None:
                    # Validate before touching the candidate or repairing
                    # inbound projections.  An unrelated task that happens
                    # to occupy the reserved key must fail closed.
                    self._validate_confirmation_task_tx(
                        conn,
                        existing,
                        confirmation_snapshot,
                        confirmation_id,
                    )
                    self._validate_task_input_attachment_access_tx(
                        conn,
                        task_id=existing.task_id,
                        inbound_message_id=confirmation_snapshot["inbound_id"],
                        inputs=existing.inputs,
                        agent_id=existing.agent_id,
                        channel=route["channel"],
                        bot_id=route["bot_id"],
                        external_user_id=route["external_user_id"],
                        session_id=route["session_id"],
                    )
                    self._retain_task_input_attachments_tx(
                        conn,
                        task_id=existing.task_id,
                        inputs=existing.inputs,
                        created_at=existing.created_at,
                    )
                    changed = conn.execute(
                        "UPDATE transcription_candidates SET status='consumed', consumed_by_task_id=? "
                        "WHERE confirmation_id=? AND status='confirmed'",
                        (existing.task_id, confirmation_id),
                    ).rowcount
                    if changed != 1:
                        # A concurrent retry can only reach this branch after
                        # the transaction lock is held.  Re-read the row and
                        # use the consumed path above on its next attempt.
                        raise InvalidTransition(
                            "transcription candidate was already consumed"
                        )
                    self._advance_confirmation_inbound_tx(
                        conn,
                        confirmation_snapshot["inbound_id"],
                        InboundState.TASK_QUEUED,
                        now=now_text,
                        task_id=existing.task_id,
                    )
                    return existing

                conversation_id = self._ensure_conversation_tx(
                    conn,
                    snapshot,
                    channel=route["channel"],
                    bot_id=route["bot_id"],
                    external_user_id=route["external_user_id"],
                    session_id=route["session_id"],
                    now=now_text,
                )
                thread_id = snapshot.thread_id or self._thread_binding_tx(
                    conn,
                    conversation_id=conversation_id,
                    mode_id=snapshot.mode_id,
                    profile_version=snapshot.profile_version,
                    policy_version=snapshot.policy_version,
                )
                if existing is None:
                    task_id = snapshot.task_id or _uuid()
                    # Preserve the original inbound ownership when this is
                    # the first confirmed candidate for that envelope.  An
                    # inbound row has a one-task uniqueness guard; if a
                    # multi-candidate message already owns another task,
                    # retain this candidate's route/input without violating
                    # that guard.
                    linked_inbound_id = confirmation_snapshot[
                        "expected_inbound_id"
                    ]
                    target = self._coerce_reply_target(
                        snapshot.reply_target,
                        fallback=original_target,
                    )
                    conn.execute(
                        """INSERT INTO tasks
                           (task_id, dedupe_key, inbound_message_id, channel, bot_id,
                            external_user_id, session_id, agent_id, conversation_id,
                            thread_id, mode_id, profile_version, policy_version,
                            model, reasoning_effort, reply_target_json, inputs_json,
                            metadata_json, state, attempts, next_attempt_at,
                            parent_task_id, child_depth, request_id, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                   'queued', 0, ?, ?, ?, ?, ?, ?)""",
                        (
                            task_id, snapshot.dedupe_key, linked_inbound_id,
                            route["channel"], route["bot_id"],
                            route["external_user_id"], route["session_id"], snapshot.agent_id,
                            conversation_id, thread_id, snapshot.mode_id,
                            int(snapshot.profile_version), int(snapshot.policy_version),
                            snapshot.model, snapshot.reasoning_effort,
                            json_dumps(target.to_dict()), json_dumps(_snapshot_value(snapshot.inputs)),
                            json_dumps(_snapshot_value(snapshot.metadata, text_key="value")),
                            None, snapshot.parent_task_id, int(snapshot.child_depth),
                            snapshot.request_id, now_text, now_text,
                        ),
                    )
                    if linked_inbound_id:
                        # Bind the candidate's inbound envelope before media
                        # ACL validation.  This is provisional and rolls back
                        # atomically if the candidate attachment is foreign.
                        conn.execute(
                            "UPDATE inbound_messages SET task_id = ? "
                            "WHERE message_id = ? AND task_id IS NULL",
                            (task_id, linked_inbound_id),
                        )
                    self._validate_task_input_attachment_access_tx(
                        conn,
                        task_id=task_id,
                        inbound_message_id=confirmation_snapshot["inbound_id"],
                        inputs=snapshot.inputs,
                        agent_id=snapshot.agent_id,
                        channel=route["channel"],
                        bot_id=route["bot_id"],
                        external_user_id=route["external_user_id"],
                        session_id=route["session_id"],
                    )
                    self._retain_task_input_attachments_tx(
                        conn,
                        task_id=task_id,
                        inputs=snapshot.inputs,
                        created_at=now_text,
                    )
                    existing = self._fetch_task_tx(conn, task_id)
                if existing is None:
                    raise StoreError("confirmed transcription task insert failed")
                self._retain_task_input_attachments_tx(
                    conn,
                    task_id=existing.task_id,
                    inputs=existing.inputs,
                    created_at=existing.created_at,
                )
                conn.execute(
                    "UPDATE transcription_candidates SET status='consumed', consumed_by_task_id=? WHERE confirmation_id=? AND status='confirmed'",
                    (existing.task_id, confirmation_id),
                )
                self._advance_confirmation_inbound_tx(
                    conn,
                    candidate["inbound_message_id"]
                    if "inbound_message_id" in candidate.keys()
                    else None,
                    InboundState.TASK_QUEUED,
                    now=now_text,
                    task_id=existing.task_id,
                )
                return existing

        outcome = await self._call(op)
        if outcome is None:
            raise InvalidTransition("transcription candidate has expired")
        return outcome

    confirm_candidate_task = confirm_transcription_task


    # Compatibility alias for immediate control commands.
    interrupt_task = request_cancel


__all__ = ["InvalidTransition", "NotFoundError", "SQLiteStore", "StoreError"]
