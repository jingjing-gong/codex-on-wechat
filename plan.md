# Multi-Agent Runtime Plan

## 1. Objective

Build a multi-agent system controlled through IM channels, starting with WeChat. Users can switch the front Agent while other Agents continue in the background. Agents can collaborate under explicit policy. User notifications, Agent messages, tasks, and media are durable in SQLite.

The first deployment is deliberately small:

```text
one process
one asyncio event loop
one WeChat gateway
one SQLite database
one Codex SDK runtime
```

The architecture must remain replaceable so PostgreSQL, RabbitMQ, Redis, NATS, Feishu, and multiple workers can be added later without rewriting the domain layer.

## 2. Scope and Delivery Strategy

The complete product includes multiple runtimes, collaboration, notification policy, Agent modes, and media. It must not be implemented as one large change.

### MVP

The MVP proves durable background execution:

- Durable inbound-message acceptance and deduplication
- SQLite-backed task queue with atomic task ownership
- One registered `CodexRuntime`
- Background task execution
- Front-Agent routing model, even if only `codex` exists initially
- `/status`, `/tasks`, `/interrupt`, and explicit retry visibility
- Durable user outbox using the original channel destination
- Text input only
- One verified Codex mode

### Later increments

After the MVP invariants pass tests:

1. Multiple Agent profiles and `/agent` switching
2. Agent modes and effective policy
3. Notification priority, `/notify`, and switch-time inbox presentation
4. Agent-to-Agent mailbox and ACLs
5. Images
6. Audio transcription confirmation
7. Video tools
8. Additional IM channels

This ordering is mandatory. Media and autonomous collaboration must not delay the durable task core.

## 3. System Invariants

These rules are more important than individual classes or tables.

1. A received channel message is durably stored before it is considered accepted by the application.
2. The same inbound message creates at most one logical task.
3. A queued task has one execution owner at a time.
4. `asyncio.Queue` is only a wake-up optimization; SQLite is the source of truth.
5. A task snapshots its Agent, conversation, mode, policy, model, and reply target at creation. Switching the front Agent never changes an existing task.
6. A user-visible event and an Agent-directed message use separate delivery projections.
7. Agent mailbox messages never call a channel sender.
8. `/notify off` suppresses automatic user delivery but never deletes an event.
9. Agent Profile and Mode prompts are not security boundaries; `PolicyEngine` enforces permissions.
10. A denial always wins when policies are combined.
11. Runtime state is owned by the asyncio loop. Gateway threads do not read or mutate runtime dictionaries directly.
12. External delivery is at-least-once unless the channel provides proven idempotency. Stable delivery IDs are reused on retry.
13. Binary media is stored in managed files, not SQLite blobs.
14. Restart recovery never blindly repeats a task whose external side effects are unknown.
15. Task terminal state, final domain event, and resulting delivery projections are committed in one SQLite transaction.
16. Notification preference is scoped to one user session and one Agent, not globally to the user.
17. Foreground responses and background notifications have different delivery semantics.

## 4. Architecture

```text
Channel Gateway
    -> Inbound Store
    -> Command Router
    -> Task Manager
    -> SQLite Dispatcher
    -> Agent Registry
        -> Codex Runtime
        -> future runtimes
    -> Domain Events
        -> User Outbox
        -> Agent Mailbox
    -> Delivery Workers
```

Responsibilities:

- `wechat_ilink`: WeChat HTTP protocol, authentication, long polling, CDN, raw channel models, and channel sending.
- Channel adapter: normalizes WeChat events into domain envelopes and maps user deliveries back to WeChat.
- SQLite store: transactions, migrations, task claims, message records, leases, retries, and recovery.
- Task manager: routing, task creation, status, interruption, retry, Agent switching, and collaboration.
- Agent registry: runtime lookup and public Agent descriptors.
- Policy engine: profile, mode, tool, collaboration, media, and attachment authorization.
- Codex runtime: the only layer that imports and translates Codex SDK execution types.

## 5. Identity and Ownership

Keep these IDs distinct:

```text
channel              wechat / feishu / future
bot_id               specific channel bot
external_user_id     channel user identity
external_message_id  channel message identity
session_id           user-visible logical session
agent_id              codex / planner / researcher
conversation_id      channel + bot + user + session + Agent
task_id               one logical execution
execution_id          one concrete execution attempt
message_id            one immutable domain message
request_id            one logical Agent request across retries
```

A task is permanently owned by the resolved `agent_id` and `conversation_id` selected when it is created.

A Codex thread binding is also policy-specific:

```text
(conversation_id, mode_id, profile_version, policy_version)
```

Do not reuse an execute-mode thread for plan/review work, or a thread created under an old Profile/Mode policy. This prevents instructions, sandbox assumptions, and writable capabilities from leaking across modes.

A durable reply target contains:

```text
channel
bot_id
external_user_id
session_id
source_message_id
source_sequence
context_token
```

`context_token` is a transport hint, not the durable destination identity.

## 6. State Machines

### 6.1 Inbound messages

```text
received -> stored -> accepted | duplicate | rejected
stored -> awaiting_confirmation | task_queued
awaiting_confirmation -> confirmed | rejected | expired
confirmed -> task_queued
```

The gateway stores normalized inbound messages and dedupe identity before task creation. Cursor advancement and inbound storage must be coordinated so a crash cannot acknowledge an unowned message.

### 6.2 Tasks

```text
queued -> claimed -> running
running -> completed | failed | cancel_requested
cancel_requested -> interrupted | cancelled
claimed/running -> orphaned on restart or expired lease
orphaned -> queued by explicit retry
orphaned -> failed by explicit abandonment
```

Do not use automatic `running -> queued` recovery. A Codex turn may have completed external side effects before the process crashed.

### 6.3 User outbox

```text
pending -> claimed -> sending -> sent
sending -> retry_wait -> pending
sending -> failed_permanent
claimed/sending -> pending after expired lease or recovery review
```

Presentation is separate from delivery:

```text
unseen -> presented -> acknowledged
```

Do not call a successfully sent WeChat message `read` unless the channel supplies a read receipt.

### 6.4 Agent mailbox

```text
pending -> claimed -> processing -> processed
processing -> rejected | dead_letter
claimed/processing -> pending after lease expiry
```

Mailbox processing must be idempotent by destination Agent and logical `request_id`.

## 7. Durable Dispatch and Concurrency

### 7.1 Inbound deduplication

Use a unique key:

```text
(channel, bot_id, external_message_id)
```

When the channel lacks a stable message ID, derive and persist a documented fallback key from available sequence and sender fields.

Task creation uses `INSERT ... ON CONFLICT` semantics and returns the existing task for duplicates.

### 7.2 Task claiming

Workers claim tasks transactionally:

```text
queued
    -> claimed_by
    -> claim_token
    -> lease_expires_at
    -> claimed
```

Required fields:

```text
claimed_by
claim_token
lease_expires_at
attempts
next_attempt_at
last_error
```

SQLite claims must use a short transaction, then execute the Agent outside the transaction. `asyncio.Queue` may wake a dispatcher but cannot be the only record of pending work.

### 7.3 Serialization

Default serialization key:

```text
(channel, bot_id, external_user_id, session_id, agent_id)
```

Tasks for the same Agent conversation run sequentially. Different Agents may run concurrently for the same user.

Commands are classified:

- Immediate read/control: `/status`, `/tasks`, `/interrupt`; may bypass normal-message locking.
- Serialized mutation: `/agent`, `/mode`, `/session`, `/clear`, `/model`, deletion; must be submitted to the loop and ordered against conversation control state.

A running task keeps its immutable thread and policy snapshot. A command cannot replace that task's thread or policy.

All TaskManager, Runtime, Profile, Mode, status, and interruption operations are submitted to the owning asyncio loop. Gateway worker threads may wait for a short command acknowledgement, but they never inspect Runtime dictionaries directly.

## 8. Agent Profiles and Collaboration Policy

An immutable Agent Profile version contains:

```text
agent_id
display_name
summary
system_prompt
responsibilities
constraints
capabilities
allowed_peers
denied_peers
allowed_request_types
max_child_depth
max_children_per_task
enabled
profile_version
```

Only public `PeerDescriptor` records are exposed to collaborators:

```text
agent_id
display_name
summary
capabilities
accepted_request_types
```

Other Agents' system prompts and private configuration are never shared.

Policy precedence narrows permissions only:

```text
hard platform policy
-> administrator policy
-> Agent Profile restrictions
-> Agent Mode restrictions
-> task-specific restrictions
```

Rules:

- Deny overrides allow.
- Missing peer permission means deny.
- Agents cannot modify Profiles, Modes, or ACLs.
- Collaboration requires an allowed peer and request type.
- Attachment access is checked before mailbox delivery.
- Rejected operations create audit events.
- Child-task depth and count are updated atomically.

Profile definitions are immutable versions. Tasks reference a concrete Profile version or persist a canonical profile snapshot and hash.

## 9. Agent Modes and Codex SDK

Agent Profile defines who an Agent is. Agent Mode defines what it may do for a specific task.

Initial modes:

| Mode      | Purpose                   | SDK sandbox     | Collaboration       |
| --------- | ------------------------- | --------------- | ------------------- |
| `chat`    | conversation and analysis | read-only       | disabled by default |
| `plan`    | planning and delegation   | read-only       | allowed by ACL      |
| `review`  | review code and diffs     | read-only       | limited             |
| `execute` | implement approved work   | workspace-write | allowed by ACL      |

Domain models must not import SDK enums. Define application enums and translate them inside `CodexRuntime`.

```python
@dataclass(frozen=True)
class AgentMode:
    mode_id: str
    developer_instructions: str
    sandbox_policy: str
    approval_policy: str
    allowed_tools: frozenset[str]
    denied_tools: frozenset[str]
    can_write_files: bool
    can_execute_commands: bool
    can_create_child_tasks: bool
    can_send_agent_messages: bool
    policy_version: int
```

Effective policy precedence:

```text
task.mode_id > session.mode_id > agent.default_mode_id
```

An Agent cannot promote itself to `execute`. The user or administrator must explicitly authorize the mode. Persist the actor and timestamp for execute authorization.

### Codex SDK adapter contract

Pin and test the supported `openai-codex` version. Only `src/agents/codex_runtime.py` imports SDK execution types.

The adapter translates:

```text
application sandbox policy -> SDK Sandbox
application approval policy -> SDK ApprovalMode
mode instructions -> thread developer_instructions
image attachment -> LocalImageInput when supported
turn stream -> AgentEvent sequence
interrupt -> AsyncTurnHandle.interrupt
```

`developer_instructions` are applied when starting or resuming a thread. If changing mode cannot safely update an existing thread's instructions and sandbox, use a separate thread per `(conversation, mode)` or explicitly restart/resume under the new policy.

The selected design is a separate SDK thread binding per `(conversation_id, mode_id, profile_version, policy_version)`. Mode or policy changes affect future tasks only and never mutate an already-running turn.

Sandbox alone does not guarantee every application policy. Git push, deployment, profile changes, ACL changes, unauthorized paths, and tool restrictions require verified interception through SDK configuration or an application tool broker.

## 10. Runtime Interfaces

```python
class AgentRuntime(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def run(
        self,
        task: AgentTask,
        emit: Callable[[AgentEvent], Awaitable[None]],
    ) -> AgentResult: ...
    async def interrupt(self, task_id: str) -> bool: ...
```

`AgentTask` carries immutable snapshots:

```text
task_id
execution_id
agent_id
conversation_id
thread_id
mode_id
profile_version
policy_version
model
reasoning_effort
reply_target
inputs
```

`AgentEvent` carries:

```text
task_id
sequence
event_type
visibility
priority
content
attachments
created_at
```

Event sequence is unique per task so duplicate execution cannot append the same logical event twice.

## 11. Message Model and Delivery Separation

`messages` is the immutable domain event/envelope record. Delivery tables contain only projection and delivery state.

### Agent requests

Use separate identifiers:

```text
message_id      immutable envelope
request_id      logical request across retries
reply_to_id     exact request message being answered
causation_id    event that caused this message
task_id         associated task
```

A response must satisfy:

```text
response.request_id == request.request_id
response.reply_to_id == request.message_id
response.destination_agent_id == request.source_agent_id
```

The store derives the response target from the original request; the responding Agent cannot redirect it arbitrarily.

### User delivery eligibility

Keep these dimensions separate:

```text
visibility       internal | user
priority         silent | normal | notify | attention
delivery_mode    inbox_only | push_eligible | requires_attention
notify_policy    user/Agent preference controlling automatic push
```

Notification preference is stored per:

```text
(channel, bot_id, external_user_id, session_id, agent_id)
```

This allows Agent A to use `/notify on` while Agent B remains silent.

Default priority values:

```text
0 silent
1 normal
2 notify
3 attention
```

`/notify off` suppresses automatic user delivery but does not delete or hide notifications. Agent mailbox delivery is never affected by `/notify`.

The default safety decision is that even `attention` does not bypass `/notify off`; it remains visible on switch or `/inbox`. A future administrator policy may define a small set of safety-critical events that can bypass this rule.

Foreground and background behavior:

- A direct response from the current front Agent to the user message that created the task is delivered as an interactive response. It is not suppressed by `/notify off`.
- If the user switches away before that task emits output, remaining user-visible output is stored as that Agent's pending inbox content.
- A background Agent with `/notify on` may push short `notify`/`attention` reminders such as `codex task completed` or `planner requires user input`.
- A background Agent with `/notify off` remains silent; its full user-visible output is stored until the user switches to that Agent or opens `/inbox`.
- Agent-to-Agent messages never become foreground responses or user reminders unless explicitly promoted to a user-visible message.

### Front Agent switching

`/agent B`:

1. Updates `routes.active_agent_id`.
2. Does not alter running tasks.
3. Does not consume B's Agent mailbox.
4. Presents B's unseen user notifications ordered by priority descending and creation time ascending.
5. Marks them `presented`, not necessarily channel-read or acknowledged.

Switch-time presentation drains only B's user-visible unseen/pending records. It does not process B's Agent mailbox and does not alter B's tasks.

## 12. SQLite Schema Requirements

The implementation may split tables further, but the following logical entities are required:

```text
schema_migrations
inbound_messages
agent_profiles
agent_modes
routes
conversations
tasks
task_executions
task_events
attachments
attachment_refs
transcription_candidates
messages
user_outbox
agent_mailbox
```

### Required constraints and indexes

- Unique inbound identity: `(channel, bot_id, external_message_id)`.
- Unique task dedupe key for user-originated work.
- Immutable Agent Profile and Mode versions.
- Foreign keys from tasks and conversations to immutable Profile/Mode versions.
- Foreign keys enabled with `PRAGMA foreign_keys = ON`.
- WAL mode and a configured busy timeout.
- `CHECK` constraints for task states, delivery states, priorities, and nonnegative depth/attempts.
- Indexes for queued tasks, recipient outbox, destination mailbox, task events, and request IDs.
- Stable `client_id` stored per user outbox item and reused on delivery retry.
- Lease owner, claim token, expiry, attempts, next retry, and last error for tasks, user outbox, and Agent mailbox.

SQLite access must not block the Codex event loop. Use `aiosqlite` or a dedicated store executor; the chosen adapter must be tested for cancellation, connection lifecycle, busy timeout, and transaction rollback. All store methods must document whether they commit internally or participate in a caller-owned transaction.

The durable task completion transaction is:

```text
task running -> task terminal state
append terminal task event
create eligible user_outbox and/or agent_mailbox projections
commit
```

The external WeChat send happens after commit. If it fails or its result is unknown, the outbox row remains retryable and the task result is not lost.

## 13. Media Model

Separate channel references from managed attachments:

```text
InboundMediaRef
    channel metadata and encrypted remote references

StoredAttachment
    immutable managed local file and metadata

RuntimeMediaInput
    native SDK input or tool reference
```

Supported attachment kinds include:

```text
image
audio
video
file
```

Managed files require:

- Atomic writes
- Canonical paths under a managed root
- MIME sniffing
- Size and quota limits
- Checksums
- No symlink traversal
- Reference-based retention leases
- Cleanup only when no live task/message/outbox references remain

Use `attachment_refs` rather than duplicating attachment JSON in every table:

```text
owner_kind: inbound_message | task | message | outbox
owner_id
attachment_id
role
ordinal
```

### Images

If the runtime and model support native image input, translate a stored image to Codex SDK `LocalImageInput`. Otherwise expose controlled OCR/vision tools and media context. Never silently convert an unsupported image into ordinary text.

### Audio confirmation

Use a durable confirmation record:

```text
confirmation_id
session identity
attachment_id
candidate_text
source
confidence
status: pending | confirmed | rejected | expired | consumed
created_at
expires_at
resolved_at
resolved_by
consumed_by_task_id
```

Use `/confirm <id>` and `/reject <id>` when multiple candidates exist. Confirmation creates one immutable task input exactly once.

The confirmation stores the original channel/session/Agent route. Confirming audio after the user switches the front Agent sends the confirmed transcript to the Agent that owns the pending media task, not automatically to the newly selected Agent.

### Video

Store metadata and expose tools such as frame extraction, audio extraction, transcription, and thumbnails. Apply resource limits and timeouts. A model that cannot consume video still receives media context and available-tool information.

### Outgoing media

Define a separate upload/send lifecycle:

```text
local -> ready -> upload_pending -> uploading -> uploaded -> send_pending -> sent
```

Channel adapters translate canonical attachments into channel-specific media items. Upload and send operations require stable idempotency keys where possible.

## 14. Restart and Recovery

Startup reconciliation is idempotent:

```text
expired task claims -> orphaned
expired outbox claims -> pending or delivery_unknown
expired mailbox claims -> pending
queued tasks -> dispatcher-visible
missing attachment files -> blocked_media
```

Do not automatically retry orphaned tasks. Provide:

```text
/retry <task_id>
/cancel <task_id>
```

A retry creates a new execution attempt and preserves the original task history. If the runtime exposes a durable turn ID that can be reconciled safely, the adapter may inspect it before deciding the recovery state.

## 15. Commands

MVP commands:

```text
/status
/tasks
/interrupt <task_id>
/retry <task_id>
/cancel <task_id>
```

Later commands:

```text
/agents
/agent [id]
/mode [chat|plan|review|execute]
/notify [on|off]
/inbox [agent_id|all]
/ask <agent_id> <prompt>
/confirm <id>
/reject <id>
```

Read-only/immediate commands may run while a task is active. Mutating commands are serialized through the TaskManager and asyncio loop.

## 16. Module Layout

```text
src/runtime/models.py
src/runtime/store.py
src/runtime/sqlite_store.py
src/runtime/dispatcher.py
src/runtime/registry.py
src/runtime/manager.py
src/runtime/worker.py
src/runtime/policy.py
src/runtime/modes.py
src/runtime/media.py
src/agents/base.py
src/agents/codex_runtime.py
src/channels/models.py
src/channels/wechat.py
```

Existing boundaries:

- `wechat_ilink/` remains the low-level WeChat implementation.
- `src/codex_agent.py` is gradually reduced or absorbed into `CodexRuntime`.
- `src/codex_wechat_bot.py` becomes startup wiring plus the WeChat gateway/command adapter.
- JSON `SessionManager` is removed only after SQLite route/conversation tests pass.

## 17. Implementation Phases and Exit Criteria

### Phase 1: Durable ingress and store

Deliver:

- Migrations and SQLite configuration
- Inbound dedupe
- Task, execution, event, and outbox models
- Atomic state transitions and claims
- Restart reconciliation

Exit criteria:

- Duplicate inbound messages create one task.
- Two workers cannot claim the same task.
- Queued tasks survive a crash before in-memory wake-up.
- Existing tests remain green.

### Phase 2: Codex runtime MVP

Deliver:

- One static Codex Profile
- One verified Mode
- Codex SDK adapter contract and compatibility smoke tests
- Background task execution
- Task status, interruption, and original reply target

Exit criteria:

- WeChat handler returns after task acceptance.
- Codex task continues in the background.
- `/interrupt <task_id>` interrupts the intended task.
- Runtime state is accessed only through the asyncio loop.

### Phase 3: Reliable user delivery

Deliver:

- User outbox claims and leases
- Stable WeChat `client_id`
- Retry and failure visibility
- Presentation state

Exit criteria:

- A crash does not lose a completed task result.
- Delivery retries reuse the same client ID.
- Agent mailbox records cannot be sent to WeChat.

### Phase 4: Multiple Agents and switching

Deliver:

- Agent Registry
- Multiple immutable Profile versions
- `/agents` and `/agent`
- Per-Agent conversations
- Switching-time notification presentation

Exit criteria:

- Switching does not change or interrupt running tasks.
- Different Agents can run concurrently for one user.
- Same Agent conversation remains serialized.

### Phase 5: Modes and policy

Deliver:

- Agent Modes and EffectivePolicy
- `/mode`
- Explicit execute authorization
- Verified Codex SDK sandbox mapping
- Tool/peer restrictions

Exit criteria:

- Read-only modes cannot write through verified execution paths.
- Deny overrides allow.
- Mode changes do not mutate running-task policy snapshots.

### Phase 6: Agent collaboration

Deliver:

- Agent mailbox claims and leases
- Request/reply/causation IDs
- ACL enforcement
- Child-task limits

Exit criteria:

- A -> B request and B -> A response route correctly.
- Duplicate mailbox processing is idempotent.
- Agent responses do not enter user outbox unless promoted.

### Phase 7: Media

Deliver in order:

1. Managed attachments and files
2. Images with native Codex input when supported
3. Audio candidate transcription and confirmation
4. Video metadata and tools
5. Outgoing media lifecycle

Exit criteria:

- Unsupported media remains visible to the Agent as structured context.
- Audio is never submitted as confirmed text without confirmation.
- Missing or expired files become explicit task states.

### Phase 8: Hardening and additional channels

- Retention and quotas
- Audit tooling
- Operational metrics
- Feishu adapter
- Evaluate PostgreSQL or a broker only when multiple processes are required

## 18. Verification Matrix

Required tests include:

- Inbound/cursor durability before acknowledgement
- Inbound deduplication
- Atomic task claiming
- Conditional state transitions
- Per-conversation serialization and cross-Agent concurrency
- Command ordering and runtime loop ownership
- Orphan recovery and explicit retry
- Profile/Mode immutable snapshots
- Codex SDK version compatibility
- Sandbox and approval translation
- Stable outbox delivery IDs and retry leases
- User outbox versus Agent mailbox separation
- Agent request/response correlation and ACL rejection
- Notification suppression and switch-time presentation
- Attachment path, quota, checksum, retention, and permission checks
- Image native input path
- Audio confirmation state machine
- Video resource limits and tool results
- Existing repository tests

Run:

```bash
uv sync --extra test
uv run pytest
```

## 19. Explicit Non-Goals for MVP

- RabbitMQ, Redis, NATS, or PostgreSQL
- Multiple processes or machines
- Feishu
- Autonomous Agent collaboration
- Audio/video processing
- Editable Profile/Mode administration UI
- Automatic retry of orphaned tasks
- Exactly-once external message delivery

The MVP is successful when the current WeChat-to-Codex flow becomes a durable, restart-aware background task system without losing messages, duplicating tasks, or blocking command handling.
