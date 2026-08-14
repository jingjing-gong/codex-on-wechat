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

Backward compatibility is part of the WeChat contract.  The durable command
router must preserve the original `/sh` and `/clear` behavior while it adds
multi-Agent controls.  These commands must not become unknown commands merely
because routing moved to SQLite.

Every public launcher path uses the durable command router. The deprecated
`--legacy` process flag may remain as an invocation alias, but it must start
the same durable runtime and cannot expose the pre-runtime router.

## 2. Scope and Delivery Strategy

The complete product includes multiple runtimes, collaboration, notification policy, Agent modes, and media. It must not be implemented as one large change.

### MVP

The MVP proves durable background execution:

- Durable inbound-message acceptance and deduplication
- SQLite-backed task queue with atomic task ownership
- One registered `CodexRuntime`
- Background task execution
- Front-Agent routing model, even if only `codex` exists initially
- `/status`, `/tasks`, `/cancel [task-id]`, and explicit retry visibility
- Durable user outbox using the original channel destination
- Text input plus WeChat voice transcription and durable file staging
- One verified Codex mode registry (`chat`, `plan`, `review`, `execute`)
- User-facing `/mode` switching with immutable task policy snapshots
- Durable `/agent` switch-or-create routing with per-Agent conversations
- Dynamic-only `/delagent` deletion with durable tombstones and route fallback

### Later increments

After the MVP invariants pass tests:

1. Notification priority, `/notify`, and explicit `/inbox` presentation
2. Agent-to-Agent mailbox and ACLs
3. Images
4. Video tools
5. Additional IM channels

This ordering is mandatory. Media and autonomous collaboration must not delay the durable task core.

## 3. System Invariants

These rules are more important than individual classes or tables.

1. A received channel message is durably stored before it is considered accepted by the application.
2. The same inbound message creates at most one logical task.
3. A queued task has one execution owner at a time.
4. `asyncio.Queue` is only a wake-up optimization; SQLite is the source of truth.
5. A task snapshots its Agent, conversation, mode, policy, model, reasoning effort, and reply target at creation. Switching the front Agent or changing a model preference never changes an existing task.
6. A user-visible event and an Agent-directed message use separate delivery projections.
7. Agent mailbox messages never call a channel sender.
8. `/notify off` suppresses unsolicited background delivery but never deletes
   an event or suppresses the requested item replies from an explicit
   user-originated `/ask` task.
9. Agent Profile and Mode prompts are not security boundaries; `PolicyEngine` enforces permissions.
10. A denial always wins when policies are combined.
11. Runtime state is owned by the asyncio loop. Gateway threads do not read or mutate runtime dictionaries directly.
12. External delivery is at-least-once unless the channel provides proven idempotency. Stable delivery IDs are reused on retry.
13. Binary media is stored in managed files, not SQLite blobs.
14. Restart recovery never blindly repeats a task whose external side effects are unknown.
15. Each stable runtime item event, its reply candidate, and its quota/allocation
    decision are committed in one SQLite transaction. Task terminal state, the
    terminal domain event, and any terminal-only projections are likewise
    committed atomically; terminal aggregate text never duplicates already
    projected items.
16. Notification preference is scoped to one user session and one Agent, not globally to the user.
17. Foreground responses and background notifications have different delivery semantics.
18. Every claim-owned mutation is fenced by active state, matching claim
    token, and an unexpired task/execution, outbox, media, or mailbox lease.
    An expired claim cannot be renewed, completed, failed, or otherwise
    revived by its former owner.
19. Compound identifiers derived from tuples use a canonical,
    collision-resistant encoding; delimiter-joined legacy IDs are accepted
    only after their persisted component columns match the requested scope.
20. The durable launcher resolves one workspace path for both Codex and
    `/sh`. `CODEX_WECHAT_WORKSPACE` overrides the runtime default, and the
    resolved directory is created before either execution path starts.
21. User replies are item-based. A stable completed user-visible text output
    item is a text-reply candidate only when its normalized text is non-empty;
    deltas, reasoning, tool/status items, and aggregate result text cannot
    create a second copy. Independently authorized media/attachment output is a
    media-reply candidate even when it has no text, and uses the same allocator;
    text plus attachments from one completed item remains one defined candidate
    bundle rather than two unrelated replies.
22. One exact finished inbound WeChat user message authorizes at most ten
    distinct logical `SendMsg` reply identities across commands, Agent output,
    media, delivery retries, replay, and restart. Slot allocation is durable
    and transactional, and overflow is retained rather than assigned an
    eleventh identity or silently discarded. Because external delivery remains
    at-least-once, this is a logical identity guarantee rather than a claim that
    an uncooperative channel can never display a duplicate after an ambiguous
    network outcome.
23. The first successful projection of a stable reply candidate snapshots its
    rendered user content, foreground classification, and effective
    notification eligibility. Exact event or terminal replay reuses that
    snapshot even if `/agent` or `/notify` changed mutable route state in the
    meantime; replay cannot reinterpret the same candidate under the new
    route.

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
        -> Reply Candidate Projector
            -> Reply-Scope Allocator
                -> Reply Slot + Canonical SendMsg
                    -> User Outbox / Outgoing Media State
        -> Agent Mailbox
    -> Delivery Workers
```

Responsibilities:

- `wechat_ilink`: WeChat HTTP protocol, authentication, long polling, CDN, raw channel models, and channel sending.
- Channel adapter: normalizes WeChat events into domain envelopes and maps user deliveries back to WeChat.
- SQLite store: transactions, migrations, task claims, message records, leases, retries, and recovery.
- Task manager: routing, task creation, status, cancellation, retry, Agent switching, and collaboration.
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

Conversation and command-delivery IDs are generated from framed component
tuples rather than ambiguous delimiter concatenation. The encoding is
versioned and deterministic. During upgrade, a matching legacy conversation
may be reused to preserve its Codex thread, and a legacy command receipt or
outbox row may be replayed only after its stored channel, bot, user, session,
message, and command fields match the incoming envelope.

Mailbox execution snapshots follow the same upgrade rule. A delimiter-joined
legacy conversation ID is retained only when the `conversations` row carrying
that ID has the exact channel, bot, user, session, and destination-Agent
columns. An absent or conflicting row causes the snapshot to use the canonical
framed ID. Workers repeat this validation when loading old queued snapshots so
pre-upgrade rows cannot make two colon-bearing scopes share an Agent thread.

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

Every durably stored finished inbound WeChat user message also owns one reply
scope, including a message that is later rejected by command, media, or task
validation:

```text
source_identity = source_message_id
                  or framed-v1("source-sequence", source_sequence)
                  or stored_inbound_id
reply_scope_id = framed-v1(
    channel,
    bot_id,
    external_user_id,
    session_id,
    source_identity,
)
capacity = 10 outbound WeChat SendMsg slots
```

The canonical framed tuple is generated from the stored inbound envelope and
is reused on redelivery. A non-empty/nonzero `source_sequence` is an identity
fallback only when the channel supplies no stable source message ID; otherwise
the inbound row's generated ID is used. Sequence is not appended as a second
independently varying component. `context_token`, task ID, execution ID,
current front Agent, and process lifetime are not part of the scope identity.
A delivery slot is identified by `(reply_scope_id, reply_ordinal)` where the
ordinal is in `1..10`; the ordinal, logical delivery ID, and primary/optional
contextless wire IDs belong to the delivery projection, not to `ReplyTarget` or
the Agent task. Clearing or rotating a context token, including the narrow
contextless preparation fallback, never creates a new scope or another slot.

The inbound row and reply-scope row are inserted atomically before routing or
validation. A duplicate stored envelope reuses both. If this transaction does
not commit, the bot does not emit an untracked validation error; after commit,
acceptance, rejection, command, and task responses all use the same allocator.

The initial execution uses its task's origin reply scope. An explicit
`/retry <task_id>` preserves the task's immutable origin `ReplyTarget` and
history but snapshots the `/retry` inbound message's scope as that new
execution's delivery scope. The retry acknowledgement and its new output items
therefore share the fresh ten slots without rewriting the original task. This
execution retry is distinct from retrying one failed outbox send, which always
reuses the already reserved slot.

## 6. State Machines

### 6.1 Inbound messages

```text
received -> stored -> accepted | duplicate | rejected
stored -> task_queued
```

The gateway stores normalized inbound messages and dedupe identity before task
creation. A file-only upload takes the `accepted` branch after managed
attachment promotion and waits for a user instruction; it never creates an
Agent task on upload. A voice bubble takes the `task_queued` branch only when
the channel supplies non-empty speech-to-text. Cursor advancement and inbound
storage must be coordinated so a crash cannot acknowledge an unowned message.
If durable acceptance or cursor persistence fails, the monitor retains the
cursor and retries with bounded exponential backoff; a deterministic policy or
store error must never become a hot replay loop.

Older `awaiting_confirmation`/transcription-candidate rows remain readable for
database migration and audit, but they are not created by current WeChat
ingress and `/confirm`/`/reject` are no longer supported commands.

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

Reply-candidate eligibility precedes the outbox state machine:

```text
retained -> allocated | deferred_quota | inbox_only
allocated -> user_outbox.pending
deferred_quota -> allocated under a later /recv reply scope | presented in inbox
inbox_only -> presentation_selected
presentation_selected -> allocated under /inbox scope | deferred_quota
allocated presentation -> presented after successful delivery -> acknowledged
```

Only `allocated` candidates are claimable by a delivery worker. Allocation and
canonical-send insertion are one transaction; the sender never implements the
cap by counting process-local sends. A delivery worker claims only that one
canonical `SendMsg` record for a slot. Candidate, user-outbox presentation, and
outgoing-media rows are read-only subordinates during send and cannot trigger
additional channel calls. A quota-deferred or notification-suppressed candidate
remains durable and visible, but cannot sit forever as an ordinary pending row
that workers repeatedly claim. Once reserved, an ordinal is never recycled
after a timeout, unknown outcome, permanent failure, lease expiry, or restart
because WeChat may already have accepted that reply.

`/inbox` presentation runs through the same allocator. Only records selected
and allocated (or selected for durable quota continuation) leave `inbox_only`;
anything outside the selection remains unseen. Selected records beyond the
command scope's remaining slots become `deferred_quota`, not `presented`, and
`/recv` can deliver them later. Presentation changes to `presented` only after
the corresponding canonical send succeeds; command replay reuses the
selection and never marks unsent excess as presented.

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

Every heartbeat and terminal write revalidates the active row state, claim
token, and transaction-time lease deadline. Task writes also validate the
corresponding execution claim. Expired claims are ownership loss, not a grace
period: they cannot be renewed or finalized even if periodic reconciliation
has not yet changed the row state. Workers stop or cooperatively cancel any
in-flight Agent turn, WeChat send, media upload/send, or synchronous hook when
ownership is lost, and suppress every subsequent stale store write.

The running process performs periodic expiry reconciliation in addition to
startup recovery, so dead owners are reclaimed without requiring a restart.
Heartbeats stop before their confirmed lease deadline and worker shutdown is
bounded even when an external coroutine swallows cancellation.

When an outbox or media worker claims a batch, every returned row is
heartbeat-protected immediately rather than only when the sequential delivery
loop reaches it. The active row remains protected through its final fenced
SQLite transition, and cancellation drains all remaining batch heartbeats.
This prevents a slow earlier network operation from expiring later locally
queued claims or opening a second-owner race inside one claimed batch.

Immediately before starting external Agent work, a task worker rechecks claim
loss after its final awaited durable task read. Losing the lease during that
read prevents `AgentRuntime.run` from starting, even when the earlier
pre-start ownership check succeeded.

### 7.3 Serialization

Default serialization key:

```text
(channel, bot_id, external_user_id, session_id, agent_id)
```

Tasks for the same Agent conversation run sequentially. Different Agents may run concurrently for the same user.

Commands are classified:

- Immediate read/control: `/status`, `/tasks`, `/agents`, `/models`, `/modes`,
  `/skills`, and `/cancel [task-id]`; these may bypass normal-message locking.
- Ordered control: `/retry`, `/agent`, `/delagent`, `/mode`, `/clear` (and its
  `/reset` alias), `/model`, `/notify`, `/inbox`, `/recv`, and `/sh`; these
  retain per-contact ordering and submit runtime/store effects to the owning
  loop where applicable.
- User task selection: `/ask <agent_id> <prompt>` and a leading
  `$<skill> <task description>` are validated and then submitted as ordinary
  durable work with immutable route, model, mode, policy, and optional skill
  snapshots. `/ask` selects an explicit destination Agent without changing the
  front-Agent route.
- Legacy compatibility commands: `/sh` and `/clear` retain their original
  semantics and reply target.  `/sh` is executed only through the existing
  bounded shell-command helper; it is never dispatched as Agent text.  The
  durable adapter may record the command and project its response, but must
  not replace it with an `unknown command` response.

A running task keeps its immutable thread and policy snapshot. A command cannot replace that task's thread or policy.

All TaskManager, Runtime, Profile, Mode, model-selection, status, and
cancellation operations are submitted to the owning asyncio loop. Gateway
worker threads may wait for a short command acknowledgement, but they never
inspect Runtime dictionaries directly. Active-task cancellation may use the
runtime's internal interruption API, but that API is not exposed as a public
`/interrupt` command.

The only public cancellation command is `/cancel [task-id]`. With a task ID,
it targets that user-owned task and applies the valid cancellation transition
for its current state. With no task ID, it targets only the currently running
task on the active Agent for the current `(channel, bot, user, session)` route;
it never selects work owned by another Agent. If no such task is running, it
returns a deterministic no-running-task response without changing task state.
For a running task, the TaskManager durably records `cancel_requested` before
calling `AgentRuntime.interrupt(task_id)` and then records the resulting
terminal state. `/interrupt` is neither registered nor advertised as a channel
command; `AgentRuntime.interrupt` and SDK turn interruption remain internal
runtime APIs.

### 7.4 WeChat transport and local credential safety

The iLink transport has two independent success signals. Every protocol
response is successful only when both `ret == 0` and `errcode == 0`; an HTTP
2xx response alone is not success. Session-expiry handling may reset a cursor
only through its explicit recovery path. All other protocol errors use bounded
backoff and must never advance either the compatibility cursor or the durable
SQLite cursor.

Every HTTP response object is closed after its body is consumed, including on
validation and protocol failures. Long-lived client sessions are closed during
normal shutdown, login-only operation, startup rollback, and exceptional
termination.

A `requests.Session` is never used concurrently. The iLink client owns a pool
of thread-exclusive sessions: sequential calls may reuse one, concurrent
poll/send/upload calls acquire different sessions, and request admission is
atomic with shutdown. `close()` rejects new calls, waits for all admitted
calls through response cleanup, closes every owned session exactly once, and
coordinates concurrent close callers idempotently.

CDN downloads are streamed rather than buffered without limit. They enforce a
maximum encrypted and plaintext size plus a total deadline, reject partial or
oversized payloads, accept only valid AES key lengths, and validate every byte
of PKCS#7 padding before publishing decrypted data. Failed downloads never
publish a managed attachment.

Credential persistence requires a non-empty bot token and bot identity. File
names are derived with collision-resistant account normalization, publication
uses a flushed atomic replacement, and existing credentials for a different
raw identity are never overwritten. The account directory and credential
files use owner-only permissions (`0700` and `0600` respectively); incomplete,
invalid, or symlinked credential records are not loaded.

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

Task policy metadata is reserved runtime state. Public submission callers may
add diagnostics but cannot replace the effective policy, Agent Profile, Mode,
authorization evidence, or their aliases through arbitrary metadata. The
only policy-snapshot override is a private continuation path for a previously
persisted transcription candidate; it reuses that exact stored snapshot and
is not exposed by the public `submit()` API.

## 9. Agent Modes and Codex SDK

Agent Profile defines who an Agent is. Agent Mode defines what it may do for a specific task.

Current built-in modes (policy version 2):

| Mode      | Purpose                   | SDK sandbox   | Commands/network | File-write intent | Collaboration       |
| --------- | ------------------------- | ------------- | ---------------- | ----------------- | ------------------- |
| `chat`    | conversation and analysis | full-access   | allowed          | do not write      | disabled by default |
| `plan`    | planning and delegation   | full-access   | allowed          | do not write      | allowed by ACL      |
| `review`  | review code and diffs     | full-access   | allowed          | do not write      | limited             |
| `execute` | implement approved work   | full-access   | allowed          | allowed           | allowed by ACL      |

Every selectable v2 mode maps to Codex `full-access`, which provides shell and
network access. `chat`, `plan`, and `review` retain `can_write_files=false` and
explicit no-edit developer instructions; because the SDK sandbox itself is
unrestricted, this is behavioral intent rather than a filesystem security
boundary. A future hard no-write guarantee would require a brokered tool layer
or a sandbox that independently permits network while denying writes.

Domain models must not import SDK enums. Define application enums and translate them inside `CodexRuntime`.

All four modes are selectable through `/mode`; the selected mode is scoped to
the current `(channel, bot, user, session, Agent)` route and affects future
tasks only.  A deployment may configure `execute` as its trusted default, but
must still retain the explicit immutable mode snapshot on every task.

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

`/mode` with no argument reports the effective mode for the active Agent and
session. `/mode <chat|plan|review|execute>` validates and persists the mode ID
and immutable policy version; an execute selection also persists its
trimmed, nonblank authorization actor and normalized, parseable timestamp;
malformed durable provenance fails closed. It is serialized through the owning
asyncio loop. A running task keeps its original mode and policy snapshot. A
trusted deployment may set `default_mode_id=execute` at startup, but a valid
durable user-selected mode row takes precedence on restart. Selecting
`execute` requires explicit authorization (or the trusted startup grant), and
must fail closed when the Agent Profile or effective policy does not permit
workspace writes and command execution. A legacy or partially written execute
selection without complete authorization provenance never grants execute
permission: its effective mode falls back to that Agent's non-execute default
(or current `chat`) until a new explicit `/mode execute` replaces the row. The
execute snapshot maps to Codex `full-access` and the configured command/tool
permissions.

Built-in Mode definitions are append-only. Version 1 retains the exact
published policies (`chat`, `plan`, and `review` use `read-only`; `execute`
uses `workspace-write`). Version 2 carries the current command/network policy.
Startup seeds both generations and never rewrites a populated v1 definition;
only the recognized historical sparse seed and the exact short-lived
full-access `execute@1` compatibility row may be repaired to canonical v1.
Schema migration 17 moves mutable static-Codex built-in session selections
from v1 to v2 and carries forward existing execute authorization provenance
atomically; custom Agent mode catalogs are untouched.
Queued/running tasks, conversations, task snapshots, and thread bindings stay
pinned to their original version. Reselecting `/mode <id>` always selects the
latest version for future tasks.

`execute` is a mode value, not a second command or execution protocol.  Do
not expose `/execute`; `/mode execute` is the only interactive execute-mode
selector.

`/modes` is a read-only command that renders the available modes as bounded,
deterministic Markdown. It uses a heading, puts each mode ID in inline code,
and appends the literal Markdown marker `**(current)**` to the effective mode
for the active Agent and session. Exactly one available mode carries that
marker. The listing must not mutate the route, create a task, or imply that
`/execute` is a separate command.

### Model and reasoning selection

The selected Codex model and reasoning-effort override are durable preferences
scoped to:

```text
(channel, bot_id, external_user_id, session_id, agent_id)
```

Each Agent therefore restores its own selection after a process restart or
after the user switches away and back. The runtime or model default applies
when no durable override exists. A task copies the effective `model` and
`reasoning_effort` into its immutable snapshot when the task is accepted;
changing either preference affects future tasks only, including future
`/ask` tasks addressed to that Agent. Queued, claimed, and running tasks retain
the values already captured in their snapshots.

The mutation forms are:

```text
/model [<model-id> <effort|default>|effort <effort|default>]
```

The first form selects a model and its reasoning effort for the active Agent.
The second changes only the active Agent's reasoning effort while preserving
its selected model. Model IDs and effort values are matched
case-insensitively against capabilities reported by that Agent's runtime and
are persisted using the runtime's canonical spelling. An unsupported model or
effort returns a deterministic usage/capability error and changes neither
preference. The effort token `default` clears the durable effort override;
with the first form it still selects the requested model, and with the second
form it leaves the selected model unchanged. Runtime conversation setters may
be updated for compatibility, but the durable scoped preference is the source
of truth.

The effort vocabulary is intentionally open-ended. The application, durable
schema, and task snapshot do not impose a fixed enum or infer support from a
model name. Values such as `max`, `ultra`, and future SDK additions round-trip
only when the selected live model descriptor advertises them. Consequently,
`ultra` is enabled for a model that reports it and rejected atomically for a
model that does not; `/models` remains the account- and runtime-specific source
of truth.

`/models` is a read-only command that obtains the model registry from the
active Agent's runtime and renders bounded, deterministic Markdown. It uses a
heading and one entry per model, with model IDs and effort names in inline
code. Every model entry lists all reasoning efforts supported by that model
and identifies its default effort. The effective model entry appends the
literal Markdown marker `**(current)**`, and its effective effort is identified
as either a durable override or the model default. The runtime default model is
also identified without confusing it with the current marker. Exactly one
model carries `**(current)**`. The command creates no task and changes no
preference.

A durable model selection can outlive the corresponding live catalog entry.
In that case `/models` appends that stored model as an `**(unavailable)**`
entry carrying the sole `**(current)**` marker and preserves its stored effort
display, including when the live catalog is empty. It does not relabel the
runtime default as current or mutate the preference from this read-only
command. A persisted mode missing from the live mode registry is represented
the same way by `/modes`, so a read-only listing never hides the active durable
selection.

`/model` with no argument is also read-only and reports the effective model
and reasoning effort for the active Agent and session. It creates no task and
changes no preference.

The model adapter consumes every page exposed by the pinned Codex SDK
`ModelListResponse.nextCursor` contract. Typed response envelopes and
compatibility `data`/`models` mappings are normalized to descriptor records
before capability matching or Markdown rendering, so `/models` and `/model`
operate on the complete catalog rather than only the first page.

Compatibility detection is confined to generated pagination type setup. Once
a page request starts, any request or decoding failure aborts the catalog read
instead of being mistaken for an older SDK and returning a partial catalog.
Model command boundaries convert ordinary SDK transport/RPC failures into a
bounded, deterministic service-unavailable response and complete the command
receipt. Runtime response-validation failures follow the same path and never
expose provider payload details. Expected capability errors remain actionable
but are normalized to one Markdown-safe bounded line. Cancellation still
propagates for interruption terminalization.

If a nonempty catalog reports neither a durable selection nor any runtime
default marker, `/models` appends an unavailable `runtime default` entry as
the sole `**(current)**` item. It does not guess that the first catalog model
is current. `/model effort <effort>` may still report that opaque runtime
default together with the validated effort override only when every reported
model supports that effort; otherwise the mutation fails without changing
the durable preference. Clearing the override with `default` is always safe.

### Skills

Skills are trusted, deployment-owned instruction/capability bundles.  A public
skill descriptor contains only:

```text
skill_id
display_name
summary
version
enabled
```

The durable skill registry is Agent-scoped and append-only.  A
`(agent_id, skill_id, version)` identifies one immutable definition; replaying
the same definition is idempotent, while attempting to change its path,
metadata, enabled state, or content hash is rejected.  When discovery supplies
no version, use the content-addressed version `sha256:<content_hash>` so a
changed skill creates a new version instead of mutating one already referenced
by a task.

`/skills` lists enabled public descriptors as deterministic Markdown and never
exposes private instructions or filesystem locations.  It is an immediate
read command and does not create a task.

Skill discovery accepts the pinned Codex SDK
`SkillsListResponse.data[].skills` shape and compatibility catalogs expressed
as mappings, typed envelopes, or sequences containing those envelopes.
Envelope unwrapping is recursive and produces only descriptor records before
normalization, so SDK page/response objects never appear in channel output or
the durable registry.

A message matching `$<skill> <task description>` creates ordinary durable
Agent work with an explicit skill selection.  Recognize a skill invocation
only when `$` is the first non-whitespace character, the skill name matches
`[A-Za-z][A-Za-z0-9_-]*`, and a nonempty task description follows.  Canonical
skill IDs are lowercase.  Unknown or disabled skills return
`unknown skill: $<id>. use /skills` and do not create a task; malformed
invocations return a usage response.  The skill selector is not a shell
variable and is never executed by `/sh`.

The created task snapshots `skill_id`, immutable skill version, content hash,
and the public descriptor.  Trusted skill instructions are resolved from the
registered version and applied as instructions, never accepted from the
channel payload or concatenated into user-controlled metadata.  Skill
capabilities are narrowed by the Agent Profile, selected Mode, administrator
policy, and task policy; a skill cannot promote a task to `execute` or grant a
tool denied by the effective policy.  Duplicate delivery must resolve to the
same task and skill snapshot even if the registry changes afterward.

When the Codex SDK supports native skill inputs, the adapter translates the
validated snapshot to `SkillInput(name, path)` after checking that the path is
inside the trusted skill root and matches the recorded hash.  If native skill
inputs are unavailable, the adapter uses the approved instruction translation
for that skill version; it must never pass an unvalidated channel path.

The recorded hash covers the complete skill bundle, not only `SKILL.md`:
every regular file beneath the skill root participates using a deterministic
relative-path, type, and content framing. Discovery and execution reject
symlinks, non-regular entries, path escapes, and files changed while they are
being inspected. The runtime revalidates the whole bundle immediately before
use so auxiliary-file edits and TOCTOU replacement cannot retain an old
trusted snapshot.

### Codex SDK adapter contract

Pin and test the supported `openai-codex` version. Only `src/agents/codex_runtime.py` imports SDK execution types.

The adapter translates:

```text
application sandbox policy -> SDK Sandbox
application approval policy -> SDK ApprovalMode
mode instructions -> thread developer_instructions
validated skill snapshot -> SDK SkillInput when supported
image attachment -> LocalImageInput when supported
turn stream -> AgentEvent sequence
interrupt -> AsyncTurnHandle.interrupt
```

`developer_instructions` are applied when starting or resuming a thread. If a
future deployment changes mode and cannot safely update an existing thread's
instructions and sandbox, use a separate thread per `(conversation, mode)` or
explicitly restart/resume under the new policy.

The selected design is a separate SDK thread binding per
`(conversation_id, mode_id, profile_version, policy_version)`.  `/mode` or a
policy change affects future tasks only and never mutates an already-running
turn; each mode gets its own SDK binding so sandbox and instructions cannot
leak across modes.

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
skill_id
skill_version
skill_hash
model
reasoning_effort
reply_target
delivery_reply_scope_id
inputs
```

`AgentEvent` carries:

```text
task_id
execution_id
sequence
event_type
visibility
priority
content
attachments
source_item_id
source_item_type
source_item_ordinal
created_at
```

Event sequence is unique per execution. For Codex, the adapter uses the SDK
item ID when available, scoped by task and execution; otherwise
`(task_id, execution_id, source_item_ordinal)` is the stable fallback for one
execution. Duplicate observation of the same completed item reuses the event
and candidate rather than appending another reply. Execution attempt number
orders retry events without requiring a task-global event sequence. The adapter
assigns the fallback ordinal to every `item/completed` notification in observed
SDK order before filtering empty/internal items; store arrival timing never
defines it.

Codex reply eligibility follows the stable completed-item boundary used by the
SDK stream:

- `item/completed` whose normalized root type is `agentMessage` becomes one
  ordered reply candidate if and only if `item.text.strip()` is non-empty.
- Completed command/tool, reasoning, plan/status, and other internal items do
  not become user replies merely because their diagnostic payload contains
  text.
- Message deltas are optional internal progress only. They are never durable
  reply candidates and never consume WeChat quota.
- `turn/completed` records terminal status but does not create aggregate reply
  text. `AgentResult.content` is a convenience aggregate and cannot project a
  second copy of completed items.
- A compatibility runtime that exposes only one final string may synthesize
  exactly one completed message item, but only when it emitted no stable
  completed user-visible text item representing that string for the execution.
- A completed, policy-authorized user-visible attachment/media output creates
  an ordered media candidate independently of text eligibility. Empty text does
  not suppress that media candidate, and one source attachment is not also
  projected as an empty text reply. Text and attachments carried by the same
  completed item share one candidate bundle; channel-required fragments/sends
  from that bundle still consume separate slots.

The runtime awaits the durable `emit` transaction for each stable completed
item. It never treats a process-local text buffer as the reply source of truth
or calls WeChat from the SDK stream callback. If durable projection fails, the
worker interrupts/fails the turn according to its lease rules; an uncommitted
item is never sent from memory.

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

### Item-based WeChat replies and reply quota

The domain boundary is the completed output item, while the transport quota is
the actual outbound WeChat message. Preserve completed-item order and
boundaries: do not concatenate text from different Agent items merely because
they arrived close together. Normalize and prepare each candidate separately;
each finished plain-text WeChat reply is limited to 3,000 Unicode characters.
If one item exceeds that limit, split only that item into ordered fragments
linked by `(source_item_id, fragment_ordinal)`.

Reply presentation is rendered once per completed item before transport
fragmentation. A newly accepted `/ask` task snapshots the metadata format
marker `user_reply_format=agent-prefix-v1`; for each eligible nonblank text
item, that format renders `<canonical-agent-id>: <message>` using the task's
immutable canonical `agent_id`. The prefix is added exactly once to each
completed item, and its characters count toward the same 3,000-character
fragment limit. Raw task events and aggregate results retain the Agent's
unprefixed content. An attachment/media-only item remains media-only and must
not gain an empty or sender-only text fragment; a text-plus-media item keeps
the normal single candidate-bundle semantics. Tasks without the marker,
including old or already in-flight `/ask` tasks, retain the legacy unprefixed
format so rollout and replay cannot change a persisted candidate's identity.

The first successful durable candidate projection also snapshots the rendered
content and the delivery classification used for that candidate: whether it
was foreground and whether notification policy made it automatically
deliverable. Re-observing the exact stable event, including while committing
the terminal task result, reuses those stored values instead of recalculating
them from the current front-Agent route or `/notify` preference. This replay
exception requires the same nonempty stable event identity. A direct/public
attempt to reuse a candidate identity with different content or delivery
classification remains a conflict rather than silently adopting either
version.

The selected WeChat transport model is a sequence of distinct
`MESSAGE_STATE_FINISH` replies. Each text fragment or media send owns one
stable logical `delivery_id`, one canonical claimable `SendMsg` record, and one
reply ordinal. That record snapshots an explicit immutable outbound sender/bot
identity; delivery selects and validates the matching channel client instead
of inheriting ambient client state. It also has an immutable primary
`client_id`; the sole exact
context-preparation fallback may use a separately stored deterministic
contextless wire ID while remaining the same delivery and slot. Its persisted
`active_wire_variant` begins as `primary` and may move only once to
`contextless` after a definitive primary preparation rejection. Distinct
replies are counted by slot/delivery identity, not by retry attempt or raw wire
client ID. Item delivery does not alternate between cumulative
`MESSAGE_STATE_GENERATING` revisions and finished chunks: mixing those
identities makes quota accounting and restart replay ambiguous. A normal retry
resends the exact stored payload with the currently active wire ID and ordinal.

All causal user replies use one allocator, including:

- completed Agent text/media candidate bundles and their transport fragments;
- command acknowledgements, command results, validation errors, and runtime
  errors;
- the immediate `/ask` acknowledgement and all later `/ask` output items;
- text and media `SendMsg` operations projected from the same inbound message.

Typing indicators, configuration requests, and media upload calls are not
`SendMsg` replies and do not consume a slot. There are no direct command,
error, foreground, media, or notification sender paths that can bypass the
durable allocator.

For every supported finished inbound WeChat user message, the gateway
immediately schedules a best-effort `typing` state before media promotion,
command handling, or Agent task dispatch. This applies uniformly to text,
voice, media, and slash-command messages that normalize into an inbound
envelope; non-user, non-finished, or otherwise unsupported wire messages do
not trigger typing. The typing request uses the exact inbound user and rolling
`context_token` with the receiving bot client, fetches the required typing
ticket through `getconfig`, and runs on the Monitor's contact worker before
the acceptance coroutine is submitted. This guarantees that the typing
attempt precedes command/task processing without blocking the runtime event
loop or consuming its bounded durable-acceptance timeout; the channel
client's own request timeouts bound the worker-side attempt.

Typing is ephemeral channel state rather than a durable user reply. It is not
inserted into SQLite, does not reserve a reply slot, and is not retried by the
outbox. A duplicate channel delivery may therefore refresh the typing state
without creating another logical `SendMsg`. Configuration or typing failures
are logged and contained: they never reject the inbound message, cancel its
task, alter its reply scope, or replace its eventual durable response.

When a command both acknowledges and creates work, its acknowledgement
candidate and slot reservation commit before the new task/execution becomes
dispatcher-visible. The enqueue/wake-up boundary cannot let a fast worker
allocate an output ordinal ahead of the acknowledgement. This is required for
`/ask` and explicit `/retry`, whose acknowledgement and later output share one
scope in causal order.

For each candidate that is push-eligible, one SQLite transaction:

1. inserts or reuses the stable source event and reply candidate;
2. locks/updates the exact reply-scope row and determines its remaining
   ordinals before final wire chunking;
3. prepares deterministic channel fragments for those exact ordinals, without
   crossing item boundaries and with reduced capacity for text ordinal 10;
4. reserves successive unused ordinals for as many ordered fragments as
   capacity permits and creates one canonical send per allocated fragment;
5. records every remaining source span/fragment as `deferred_quota` in FIFO
   order without creating a sendable row.

Allocation is serialized by the database using the reply-scope row and the
unique `(reply_scope_id, reply_ordinal)` constraint. It must not be implemented
as an unlocked `COUNT(*)` followed by insertion. Duplicate SDK notifications,
inbound replay, command replay, concurrent projectors, and restart all resolve
to the existing candidate/fragment and slot. Delivery failure never causes an
eleventh allocation or releases a possibly consumed ordinal.

Whenever ordinal 10 is text, its immutable payload always reserves room for a
deterministic continuation suffix telling the user to send `/recv` if more
output follows, even when no later item has arrived yet. The decision therefore
does not race item 11. Suffix capacity is removed before channel chunking; any
displaced text becomes the first FIFO overflow fragment, so the notice cannot
truncate or lose content. If ordinal 10 is a media reply, it remains media and
no eleventh notice is sent; `/recv` is also documented in `/help`.

`/recv` is an ordered, idempotent continuation command. Its inbound message
creates a new reply scope and context token, then transactionally reprojects
the oldest quota-deferred fragments into that new scope, preserving each
fragment's origin scope, source item, and order. It drains at most the new
scope's ten slots; if more remains, its tenth text reply carries the same safe
continuation suffix. With no deferred output, `/recv` creates one deterministic
empty-queue response. Redelivery of the same `/recv` message reuses its prior
allocations and never drains the next batch.

The continuation queue is scoped to the exact
`(channel, bot_id, external_user_id, session_id)` recipient and may contain
eligible results from multiple Agents/tasks in their committed global FIFO
order. It never crosses a bot, user, or session and never drains `inbox_only`
notification content that the user has not selected. It may drain content that
an explicit `/inbox` command already changed to
`presentation_selected`/`deferred_quota`. A transactionally assigned
`deferred_sequence`, followed by candidate and fragment ordinals as tie-breakers,
makes this ordering stable under concurrent Agent completion.

An unrelated new user message creates its own scope but does not silently
adopt old overflow or replace an old task's reply target. `/recv` is the only
automatic quota-continuation path; `/inbox` presents notification-suppressed
content through the new command message's own scope. Agent switching never
selects or presents stored output.
FIFO order is durable across tasks, processes, failures, and repeated `/recv`
commands; prepending newly deferred content ahead of older content is forbidden.

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

The default safety decision is that even `attention` does not bypass `/notify off`; it remains visible through `/inbox`. A future administrator policy may define a small set of safety-critical events that can bypass this rule.

Foreground and background behavior:

- A direct response from the current front Agent to the user message that
  created the task is delivered item by item as an interactive response. It is
  not suppressed by `/notify off`, but every fragment still consumes that
  source message's shared reply scope.
- A response to an explicit user `/ask <agent_id> <prompt>` task is delivered
  to that command's durable reply target even when the destination Agent is
  not the current front Agent. It is not suppressed by `/notify off`, because
  it is a requested result rather than an unsolicited background notification.
  Its immediate acknowledgement already occupies one slot, so its completed
  output items share only the remaining slots.
- For an ordinary task accepted from the then-current front Agent (not an
  explicit `/ask`), switching to another Agent before an output candidate's
  first projection makes that newly projected output background/inbox content;
  it does not reserve slots in the old scope. An explicit `/inbox` command
  presents it through that command message's new reply scope. Candidates
  already projected before the switch keep their original delivery snapshot
  during exact replay.
- A background Agent with `/notify on` may push at most one bounded
  `notify`/`attention` reminder such as `codex task completed` or `planner
  requires user input` for a completion. It consumes a slot only when it has a
  valid, unexhausted causal reply scope; it cannot fan out one reminder per SDK
  item.
- Except for an explicit `/ask` result, a background Agent with `/notify off`
  remains silent; its full user-visible output is stored until the user opens
  `/inbox`.
- Agent-to-Agent messages never become foreground responses or user reminders unless explicitly promoted to a user-visible message.

A notification with no valid inbound reply scope, or whose original scope is
exhausted, remains inbox-only. It cannot borrow the most recent unrelated
user message, overwrite that message's `context_token`, or reset another
scope's counter. Foreground status does not exempt any response from the
ten-message transport cap.

### Front Agent switching

`/agents` is a read-only command that renders the enabled registered Agents and
persisted dynamic aliases as bounded, deterministic Markdown. It uses a
heading, puts each public Agent ID in inline code, includes only public
descriptor fields, and appends the literal Markdown marker `**(current)**` to
the active front Agent. Exactly one listed Agent carries that marker. It never
exposes system prompts or private configuration, changes the route, or creates
a task.

`/agent B`:

1. Validates `B` as a canonical Agent ID matching `[A-Za-z][A-Za-z0-9_-]{0,63}` (stored in lowercase).
2. Switches to an existing registered Agent, or creates a named alias over the configured Codex runtime when it does not exist.
3. Persists the immutable named Profile and route before acknowledging the command; named aliases default to `chat` and do not inherit trusted `execute` authorization.
4. Does not alter running tasks.
5. Does not consume B's Agent mailbox.
6. Returns only the switch confirmation; it neither selects nor presents B's prior output or unseen notifications.

`/delagent <agent_id>` deletes one dynamically created named Agent. It accepts
exactly one validated Agent ID, never deletes the static/template Agent, and
rejects explicitly registered non-dynamic Agents. Deletion removes the live
alias, routes currently pointing at it back to the configured default Agent, and
persists a tombstone so restart restoration cannot recreate the alias from its
immutable profile snapshot. Existing tasks and conversations retain their
immutable Agent/Profile snapshots for audit and recovery; deletion does not
silently mutate or delete historical work. The durable retirement transaction
rejects deletion while the Agent owns queued, claimed, running,
cancel-requested, or orphaned work; the user must first let that work finish or
cancel it. This check is committed atomically with profile disablement, route
fallback, and the tombstone so a concurrent submission cannot be stranded.
An explicit later `/agent <same-id>` recreates the retired alias. Reactivation
validates the requested current Profile against the retained same-version
metadata, permits only the lifecycle transition from disabled to enabled,
and re-enables every retained version in a preparation transaction while the
tombstone remains authoritative. Only after Profile/Mode publication and
runtime registration succeed does a second transaction atomically remove the
tombstone and write the requesting scope's route. An interrupted preparation,
a publication failure, or a route failure therefore remains hidden across
restart and is safe to retry. Concurrent recreations may commit distinct scope
routes idempotently. Profile-backed route commits are guarded even for ordinary
switches, and switch revalidation, durable commit, and local route-cache update
share the Agent lifecycle lock with deletion. A concurrent switch/delete thus
linearizes cleanly: whichever commits last determines both durable and local
state, and no switch can clear a new tombstone or route to a disabled Profile.
Any non-lifecycle metadata difference remains an immutable-version conflict;
the route and tombstone stay unchanged. Tombstoned staged registrations remain
hidden from `/agents` until a retry commits successfully.

Stored output and unseen notifications remain unchanged during a switch and are
presented only through an explicit `/inbox` command.

Named aliases retain independent Agent IDs, conversation IDs, Profile snapshots,
and route bindings even when they share the underlying Codex transport. Profiles
created by `/agent` are restored as usable aliases after restart. Invalid names
are rejected without creating a Profile or route.

### Direct user requests to an Agent

`/ask <agent_id> <prompt>` submits the prompt as an ordinary user-originated
task owned by the explicitly named Agent. This command is distinct from the
internal Agent-to-Agent mailbox request API: it creates a normal task with the
requesting user's durable `ReplyTarget`, immutable task snapshots, and inbound
command deduplication.

The destination Agent ID is canonicalized and validated using the same rules
as `/agent`. An existing Agent is reused; a valid unknown name creates and
persists the same kind of dynamic Codex alias that `/agent <name>` would
create. Invalid names and empty prompts fail without creating an Agent or
task. Creating or selecting the destination for `/ask` does not switch the
front-Agent route and does not alter any running task.

Acceptance returns an immediate deterministic acknowledgement containing the
task ID. The destination Agent's eligible completed output items are then
projected in order through the normal WeChat outbox to the requester's original
reply target; the user receives the Agent's item replies in WeChat, not merely
a completion notice or one duplicated aggregate result. Because `/ask` is an
explicit user request, those item replies remain push eligible even when the
dynamically created or existing destination is not the front Agent or its
background notification preference is off. The acknowledgement and item
fragments share the command message's ten slots. Replaying the same inbound
command resolves to the same logical task and must not enqueue duplicate work,
reply candidates, or delivery projections.

New `/ask` tasks snapshot `user_reply_format=agent-prefix-v1`. Their nonblank
text output is presented as `<canonical-agent-id>: <message>` so the sender is
explicit even when the requested Agent is not front. Rendering uses the
immutable task `agent_id`, happens once per completed item before chunking, and
therefore counts the prefix inside the 3,000-character bound. It does not
rewrite raw Agent events or create text for media-only items. Existing and
in-flight `/ask` tasks without that marker remain unprefixed, and replay of an
already projected item reuses its stored rendered content rather than adding a
prefix again.

`/ask` is the only mutating command whose interrupted receipt can be recovered
automatically. Its deterministic `command-ask:<request_id>` task dedupe key
makes both crash windows idempotent: if a matching, user-owned task already
exists, recovery completes the receipt with its original task ID; if no task
exists, recovery atomically reopens the interrupted receipt and reruns the
command. A foreign or conflicting dedupe winner fails closed. `/sh` and all
other commands keep the unknown-outcome response and are never blindly
repeated after an interrupted receipt.

Recovery accepts an existing `/ask` task only when its durable reply target
matches the exact channel, bot, user, session, and source message of the
command receipt. User-level task ownership alone is insufficient because old
delimiter-joined request IDs can collide when session or message components
contain colons.

An invocation that owns or reopens a command receipt must cover its complete
effect-and-completion lifecycle. If monitor timeout, shutdown, or another
in-process cancellation ends that invocation, it durably transitions a still
`started` receipt to `interrupted` before cancellation escapes. Reservation and
reopen operations are drained across cancellation so a committed ownership
transition cannot be stranded without its terminalization. A duplicate that
does not own the receipt never performs this transition.

## 12. SQLite Schema Requirements

The implementation may split tables further, but the following logical entities are required:

```text
schema_migrations
inbound_messages
agent_profiles
agent_modes
session_modes
session_model_preferences
skills
routes
conversations
tasks
task_executions
task_events
reply_scopes
reply_candidates
reply_fragments
reply_slots
outbound_sends
attachments
attachment_refs
transcription_candidates
messages
user_outbox
outgoing_media
agent_mailbox
```

### Required constraints and indexes

- Unique inbound identity: `(channel, bot_id, external_message_id)`.
- Unique task dedupe key for user-originated work.
- Immutable Agent Profile and Mode versions.
- One durable mode row and one durable model-preference row per
  `(channel, bot_id, external_user_id, session_id, agent_id)` scope.
- Immutable enabled Skill versions and content hashes.
- Foreign keys from tasks and conversations to immutable Profile/Mode versions.
- Foreign keys enabled with `PRAGMA foreign_keys = ON`.
- Startup rejects a schema migration history with gaps or a version newer
  than the runtime understands; it must never infer compatibility from only
  the maximum recorded version.
- WAL mode and a configured busy timeout.
- `CHECK` constraints for task states, delivery states, priorities, and nonnegative depth/attempts.
- Indexes for queued tasks, recipient outbox, destination mailbox, task events, and request IDs.
- One reply scope per exact inbound envelope, enforced by
  `UNIQUE (inbound_message_id)`, with an immutable WeChat capacity of ten and
  foreign keys back to the inbound message and durable reply target.
- Stable completed-item identity and order on reply candidates. Codex item ID is
  preferred but is scoped by `(task_id, execution_id)`;
  `(task_id, execution_id, source_item_ordinal)` is the required fallback.
- The first reply-candidate projection stores immutable rendered content,
  foreground classification, and effective notification eligibility. Exact
  replay with the same nonempty event identity reuses that snapshot; route or
  notification changes cannot mutate it, while non-event/direct identity
  conflicts still fail closed.
- Unique event keys `(task_id, execution_id, sequence)` and unique completed
  source-item keys within that execution. Execution attempt number plus event
  sequence defines retry-history order.
- Unique fragment identity `(reply_candidate_id, fragment_ordinal)`, a
  transactionally assigned per-recipient `deferred_sequence`, a partial unique
  key on `(channel, bot_id, external_user_id, session_id, deferred_sequence)`,
  and a FIFO index over quota-deferred fragments.
- Every task execution snapshots its delivery reply scope. The initial
  execution references the task's origin scope; an explicit `/retry` execution
  references the command message's scope without mutating the task target.
- `CHECK (reply_ordinal BETWEEN 1 AND 10)` and unique
  `(reply_scope_id, reply_ordinal)` on shared reply slots. Text, command, error,
  notification, and outgoing-media projections all reference this same slot
  entity, so separate tables cannot each allocate ten replies.
- Exactly one canonical claimable `outbound_sends` record per reserved slot,
  enforced by `UNIQUE (reply_slot_id)`. It owns the immutable channel payload,
  logical `delivery_id`, primary wire client ID, optional deterministic
  contextless wire ID, one-way `active_wire_variant`, send state, and lease.
  `user_outbox`, candidate-item, and outgoing-media rows are subordinate records
  and cannot independently call the channel sender. If `user_outbox` physically
  serves as `outbound_sends`, it must carry the same one-to-one slot constraint.
- Unique source-event/candidate projection identity so duplicate item
  completion, command replay, or `/recv` replay returns the existing fragment
  and slot.
- Stable logical delivery ID and primary/contextless wire IDs stored per
  canonical send. Retry reuses the primary ID before the one-way branch and the
  alternate ID after it; the alternate is permitted only for the exact
  contextless fallback. Delivery and populated wire IDs are globally unique,
  the alternate differs from the primary, and `active_wire_variant` has a
  `primary -> contextless` transition only. A `/recv` allocation also retains
  the origin scope and source item while using the `/recv` message's new
  delivery scope.
- Lease owner, claim token, expiry, attempts, next retry, and last error for
  tasks, canonical outbound sends/user outbox, outgoing media, and Agent
  mailbox.

SQLite access must not block the Codex event loop. Use `aiosqlite` or a
dedicated store executor; the chosen adapter must be tested for cancellation,
connection lifecycle, busy timeout, and transaction rollback. Executor-backed
initialization, ordinary operations, and close remain fenced by the store
lifecycle lock until their synchronous work finishes, even when the awaiting
task is cancelled repeatedly. Cancellation is re-raised only after that work
is drained, so no queued operation can touch a connection after the lock is
released or while the executor and connection are being closed. All store
methods must document whether they commit internally or participate in a
caller-owned transaction.

The durable completed-item projection transaction is:

```text
insert-or-reuse stable completed task event
render presentation once and insert-or-reuse ordered reply candidate with its
    rendered content, foreground classification, and effective notification
    eligibility snapshot
if user-push eligible:
    lock reply scope and determine remaining ordinals
    prepare ordinal-aware deterministic fragments
    for each fragment in order:
        reserve next reply slot 1..10
        create one canonical outbound send and subordinate user_outbox projection
        or mark this and all remaining fragments deferred_quota when full
if Agent-directed:
    create agent_mailbox projection
commit
```

Task completion then atomically records the terminal state and terminal event.
It projects a compatibility final-text candidate only when no completed
user-visible text item represents that content. One explicitly synthesized
terminal failure or interruption reply may be created with the idempotency key
`(execution_id, terminal-system-reply)`; ordinary status events remain internal.
The synthetic reply uses the same allocator and never a direct sender.

The external WeChat send happens after commit. If it fails or its result is
unknown, the outbox row remains retryable, its ordinal remains spent, and the
item content is not lost.
Text delivery uses capped exponential retry backoff. If iLink definitively
returns the exact context-preparation failure (`ret=-2`, `errcode=0`, `prepare
failed`), the sender durably changes that canonical send to its contextless
wire variant and retries without the rolling `context_token` using the stored
deterministic alternate client ID. It never reserves a second ordinal and never
switches back to the primary ID after this one-way branch. A definitive repeated
contextless preparation failure is permanent; a later retryable or ambiguous
failure reuses the active wire variant. The allocator guarantees no more than
ten logical slots; the at-least-once delivery caveat still applies if WeChat
fails to deduplicate an ambiguous retry of the same wire ID.

All task, execution, outbox, outgoing-media, attachment-cleanup, and mailbox
conditional mutations require an unexpired lease as observed inside their
SQLite transaction. Mailbox response publication and source-request
completion validate the original claim atomically in the same transaction.

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
- Process-local serialization of quota checks and publication across every
  `AttachmentStore` instance that shares the same managed root
- No-clobber publication: an existing immutable attachment ID is never
  overwritten, including by concurrent writers
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

### Audio voice bubbles

WeChat's `VoiceItem.text` is the channel speech-to-text result. When it is
non-empty, normalize it into the immutable inbound `text` field and submit it
as the user instruction in the same way as typed text. Keep the promoted audio
attachment in the task's structured media context so the Agent can inspect the
source when supported. One inbound message produces one task, and replay uses
the normal inbound dedupe key.

If the channel cannot provide a transcript, retain/promote the media when
possible, create no empty task, and return a deterministic retry message asking
the user to resend the audio or type the instruction. Current ingress has no
human confirmation state machine: `/confirm` and `/reject` are intentionally
unknown and are not advertised. Legacy transcription-candidate tables and
manager/store methods may remain during migration, but new channel messages do
not create candidate rows.

### Uploaded files

Promote file bytes into the managed attachment store and retain the inbound
attachment reference with its checksum, MIME type, and ownership metadata. A
file upload (with or without a caption) is a staging event, not Agent work:
create no task and reply with deterministic Markdown naming the stored file and
asking what the user wants done with it. The upload remains durable for a later
explicit file workflow; it must never be interpreted as a shell command or an
implicit instruction merely because a caption was present.

### Unsupported channel media

A finished user message containing an unsupported or non-content WeChat item
type, including the protocol's `ITEM_TYPE_NONE` sentinel, is still durably
accepted and creates at most one ordinary task. Normalize each such item to
redacted structured media context containing only a generic MIME,
unavailable/native-input flags, and a bounded diagnostic identifying its
numeric type. The numeric transport discriminator participates in the
non-secret replay fingerprint but is not copied as a generic `type` field into
the durable media schema. Never copy opaque item fields, CDN credentials, or
future protocol payloads into the inbound row or task snapshot, and never
attempt to download the placeholder. Redelivery uses the normal immutable
inbound identity and returns the original task rather than creating duplicate
work.

### Video

Store metadata and expose tools such as frame extraction, audio extraction, transcription, and thumbnails. Apply resource limits and timeouts. A model that cannot consume video still receives media context and available-tool information.

### Outgoing media

Define a separate upload/send lifecycle:

```text
local -> ready -> upload_pending -> uploading -> uploaded -> send_pending -> sent
```

Channel adapters translate canonical attachments into channel-specific media
items. Upload operations use stable idempotency keys where the CDN supports
them. Every WeChat media `SendMsg` must use the mandatory canonical reply-slot
delivery identity, primary wire ID, and one-way contextless fallback rules; send
idempotency is not optional merely because upload idempotency is unavailable.

For WeChat, the protocol quota unit is one distinct logical `SendMsg` identity,
not one domain event, attachment, SDK item, or HTTP attempt. Every text fragment
and every media `SendMsg` causally tied to an inbound message references one of
that message's shared reply slots. A `SendMsg` containing multiple channel items
still consumes one slot only when those channel items are the defined bundle of
one reply candidate. It must never coalesce two completed Agent items, two
command replies, or otherwise independent candidates to evade item boundaries
or quota. Separate media sends consume separate slots. CDN upload,
configuration, and typing calls consume none. Media deferred by quota keeps its
attachment references and FIFO position for `/recv` or inbox presentation.

## 14. Restart and Recovery

Startup reconciliation is idempotent:

```text
expired task claims -> orphaned
expired outbox claims -> pending or delivery_unknown
expired mailbox claims -> pending
queued tasks -> dispatcher-visible
missing attachment files -> blocked_media
```

Reply scopes, candidates, fragments, reservations, and overflow are recovered
as ordinary durable state. Recovery may requeue a slot's delivery lease, but it
never frees or renumbers the slot, resets a scope to ten, changes its source
message/context, or turns quota-deferred content into an unscoped send. A new
inbound message creates a different reply scope; only an idempotent `/recv`
projection moves deferred content into that new scope.

For one durable SQLite path, only the first process-local opener performs the
strong process-boundary recovery that reclaims unexpired ownership left by a
previous process.  Additional connections opened while that database remains
live perform lease-expiry reconciliation and attachment validation only; they
must not orphan or reclaim healthy work owned by another live connection.
Closing the final live connection establishes the next startup boundary, so
the next opener performs strong recovery again.  Failed initialization never
retains startup ownership, and independent `:memory:` stores do not share it.

Do not automatically retry orphaned tasks. Provide:

```text
/retry <task_id>
/cancel [task-id]
```

A retry of failed, orphaned, or interrupted work creates a new execution
attempt and preserves the original task history. Its immutable delivery scope
is the `/retry` command's inbound scope; redelivery of a completed command
resolves to that same execution and scope, while an interrupted receipt retains
the existing unknown-outcome rule. Cancelling an orphaned task requires its
explicit task ID; the
no-argument `/cancel` form only targets the current running task on the active
Agent. If the runtime exposes a durable turn ID that can be reconciled safely,
the adapter may inspect it before deciding the recovery state.

## 15. Commands

The help response is a short Markdown document, not one long unformatted line.
Use a heading and grouped bullet lists (or equivalent line-separated Markdown)
so WeChat clients render each command on its own line.  Keep command names and
arguments in inline code, and keep the response bounded and deterministic so a
redelivery produces byte-for-byte equivalent help text.  The help includes
`/skills`, `/mode`, `/model`, `/models`, `/modes`, `/agents`, `/delagent`,
`/cancel [task-id]`, `/recv`, and the `$<skill> <task description>` syntax, but
never advertises `/execute` or `/interrupt`.

Help contains exactly one canonical mutation entry,
`/model [<model-id> <effort|default>|effort <effort|default>]`; `/models`
remains its separate read-only catalog command. Do not split model and effort
mutation into multiple `/model` help lines.

Required compatibility commands:

```text
/help
/clear
/reset
/sh <command>
/skills
```

`/clear` continues to clear the active Codex conversation and start a fresh
thread; `/reset` is its public compatibility alias. `/sh <command>` continues
to run the bounded shell helper in the bot workspace and return its result as
bounded Markdown with a heading, exit status, and fence-safe `text` code block
for output. Empty output and command failures also use deterministic Markdown
responses. The helper bounds stdout and stderr while the child is running,
drains both pipes concurrently, starts a separate POSIX process group, and on
timeout or decoding failure terminates and reaps the entire process tree;
descendants cannot retain pipes to escape the deadline. `/skills` lists
enabled public skill descriptors and does not create a task. These commands
are not sent to an Agent as ordinary prompts.

Durable runtime and delivery commands:

```text
/status
/tasks [limit]
/retry <task_id>
/cancel [task-id]
/agents
/agent [name]
/delagent <agent_id>
/mode [chat|plan|review|execute]
/modes
/model [<model-id> <effort|default>|effort <effort|default>]
/models
/notify [on|off]
/inbox [agent_id|all]
/recv
/ask <agent_id> <prompt>
```

`/recv` never creates Agent work. It durably drains the oldest
`deferred_quota` reply fragments into the `/recv` inbound message's own
ten-slot reply scope. It returns a deterministic no-pending-output response
when the queue is empty and is safe under inbound redelivery, cancellation,
and restart.

Read-only/immediate commands may run while a task is active. Mutating commands are serialized through the TaskManager and asyncio loop.

`/cancel` is the sole public cancellation command. `/cancel <task_id>` targets
the specified user-owned task; `/cancel` with no argument interrupts the
current running task on the active Agent through the internal runtime API. It
returns a deterministic response when there is no current running task.
`/interrupt` remains an internal runtime operation and is not a channel
command.

`/mode` is the sole interactive mode selector; `/execute` is intentionally not
exposed.  A leading `$<skill> <task description>` is task ingress rather than
a slash command and receives the current Agent, session, mode, policy, and
skill snapshots.  Both command effects and skill selection are durable and
idempotent on redelivery.

`/agent` with no argument reports the active Agent. `/agent <name>` switches to
the existing Agent or creates a durable named Codex alias when absent, then
persists the route. A malformed name returns an error and has no side effects.
File-only uploads are acknowledged as stored media and receive a question about
the desired action; they are not listed as task commands because they do not
create a task until a future explicit file workflow is selected.

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
src/runtime/identity.py
src/runtime/shell.py
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
- `/cancel <task_id>` cancels the intended user-owned task, and `/cancel`
  without an ID interrupts the current running task on the active Agent.
- No public `/interrupt` command is registered or advertised; cancellation of
  a running task reaches the runtime's internal interrupt API.
- Runtime state is accessed only through the asyncio loop.

### Phase 3: Reliable user delivery

Deliver:

- User outbox claims and leases
- Completed-item reply projector with source-item deduplication
- Durable inbound reply scopes and shared ten-slot allocator
- Exactly one canonical claimable `SendMsg` per reply slot
- Stable WeChat reply ordinals, logical delivery IDs, and one-way primary or
  contextless wire identity
- FIFO quota overflow plus idempotent `/recv` continuation
- Retry and failure visibility
- Presentation state

Exit criteria:

- A crash does not lose a completed task result.
- Every non-empty completed Agent message item is retained and projected in SDK
  order without aggregate-result duplication.
- No source inbound message can allocate more than ten distinct logical
  text/media `SendMsg` replies, including command acknowledgements and runtime
  errors, and no slot can fan out through both text and media workers.
- Delivery retries reuse the same ordinal and client ID; restart does not reset
  the allowance or reverse overflow.
- `/recv` drains the next FIFO batch under its own new scope without rerunning
  the Agent or losing a truncated tail.
- Agent mailbox records cannot be sent to WeChat.

### Phase 4: Multiple Agents and switching

Deliver:

- Agent Registry
- Multiple immutable Profile versions
- Deterministic Markdown `/agents` listing with exactly one current marker,
  plus `/agent` reporting and switching
- Switch-or-create named aliases with durable Profile/route restoration
- Dynamic-only `/delagent` deletion with static/default-Agent protection,
  fallback of every affected route, and a restart-persistent tombstone
- Per-Agent conversations
- Explicit `/inbox` notification presentation

Exit criteria:

- Switching does not change or interrupt running tasks.
- Different Agents can run concurrently for one user.
- Same Agent conversation remains serialized.
- `/agents` lists public descriptors for registered Agents and dynamic aliases
  as Markdown and marks the active front Agent.
- A valid unknown `/agent <name>` creates a usable chat-mode alias and survives restart; invalid names create neither a Profile nor a route.
- `/delagent <name>` cannot remove the default/static Agent, preserves
  immutable task and conversation history, redirects affected routes to the
  default Agent, does not restore the deleted alias after restart, and rejects
  retirement while unfinished or orphaned work still belongs to the Agent. A
  later explicit `/agent <same-name>` safely recreates an exact retained
  dynamic Profile but rejects genuine same-version metadata conflicts.

### Phase 5: Modes, skills, and policy

Deliver:

- Immutable Agent Modes and EffectivePolicy
- `/mode` reporting and session/Agent-scoped switching
- Markdown `/modes` listing with exactly one current marker
- Runtime-backed model registry, durable model/effort preferences, both
  `/model` mutation forms, and Markdown `/models` listing
- Explicit execute authorization plus an optional trusted execute default
- Trusted Skill registry, `/skills`, and `$<skill> <task description>` ingress
- Verified Codex SDK sandbox mapping
- Tool/peer restrictions

Exit criteria:

- `/mode` reports and persists the selected mode; the next accepted task uses
  that immutable snapshot, while running tasks remain unchanged.
- `/modes` and `/models` render deterministic Markdown, enumerate their
  respective mode/model capabilities, and mark exactly one effective current
  entry; `/models` lists every supported effort for each model, including
  open-ended values such as `max` or `ultra` only where advertised.
- `/model [<model-id> <effort|default>|effort <effort|default>]` changes the
  model and/or effort; the `effort` form preserves the selected model.
- Every latest `/mode` selection maps to `full-access` with command and network
  access; chat, plan, and review retain no-edit behavioral intent while
  execute permits file writes.
- `/mode execute` creates an authorized execute selection, and the Codex
  adapter maps its v2 tasks to `full-access`.
- Invalid execute definitions fail closed. A persisted execute selection with
  missing authorization is treated as the Agent's safe non-execute default
  until an explicit `/mode execute` records complete authorization; it never
  grants execute permission or poisons inbound replay. `/execute` remains
  unknown.
- `/skills` lists only public enabled descriptors; `$<skill> <description>`
  produces one durable task with an immutable trusted skill snapshot.
- Deny overrides allow.
- Mode, skill, or policy changes do not mutate running-task snapshots.

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
3. Direct voice transcription ingress with no confirmation commands
4. Video metadata and tools
5. Outgoing media lifecycle

Exit criteria:

- Unsupported media remains visible to the Agent as structured context.
- A non-empty channel voice transcript becomes exactly one task instruction;
  an unavailable transcript never creates an empty task.
- File-only uploads are durably stored and prompt for the user's next action
  without creating an Agent task.
- Missing or expired files become explicit task states.

### Phase 8: Hardening and additional channels

- Retention and storage quotas (the WeChat reply quota is Phase 3 protocol work)
- Audit tooling
- Operational metrics
- Feishu adapter
- Evaluate PostgreSQL or a broker only when multiple processes are required

## 18. Verification Matrix

Required tests include:

- Inbound/cursor durability before acknowledgement
- iLink success validation across both `ret` and `errcode`, protocol-error and
  durable-acceptance backoff without cursor advancement, and HTTP
  response/session cleanup
- Inbound deduplication
- Atomic task claiming
- Conditional state transitions, including stale-token and expired-lease
  rejection for every claim-owned mutation
- Periodic same-process expiry reconciliation and cooperative cancellation of
  external work after ownership loss
- Explicit and lazy SQLite initialization, normal store operations, and close
  drain executor work before releasing the lifecycle lock under repeated
  cancellation
- Per-conversation serialization and cross-Agent concurrency
- Command ordering and runtime loop ownership
- Markdown help rendering, `/recv` advertisement, and deterministic redelivery
- Legacy `/sh` and `/clear` behavior after durable routing, including bounded
  Markdown `/sh` results, bounded in-flight stdout/stderr memory, and
  process-tree termination on timeout
- `/cancel [task-id]` target selection, no-task behavior, durable cancellation
  transitions, and internal runtime interruption; public `/interrupt` remains
  unknown and absent from help
- `/mode` reporting, switching, execute authorization, and restart persistence
- Exact v1 built-in preservation plus v2 seeding without metadata conflicts;
  schema-17 static-Codex and schema-18 generated-Agent session
  selection/authorization migration to v2 while existing task and thread
  snapshots remain pinned to v1 and arbitrary custom Agents remain unchanged
- Deterministic Markdown `/modes`, `/models`, and `/agents` listings with
  exactly one current marker; per-model effort enumeration; and an explicit
  unavailable runtime-default current entry when the catalog marks no default
- Both branches of `/model [<model-id> <effort|default>|effort <effort|default>]`,
  including invalid capability rejection and
  immutable task snapshots, complete pagination, page-failure propagation,
  bounded capability errors, and deterministic non-disclosing SDK
  service-unavailable command responses; forward-compatible `max`/`ultra`
  values round-trip only for catalog entries that advertise them
- Exactly one consolidated `/model [...]` mutation entry in `/help`, with
  `/models` retained as the separate catalog command
- `/execute` is not advertised and cannot create Agent work
- Deterministic `/skills` listing and `$<skill> <description>` parsing
- Unknown/disabled skill rejection, immutable skill version/hash snapshots,
  whole-bundle symlink-safe hashing and TOCTOU revalidation, and skill/mode
  policy intersection
- Orphan recovery and explicit retry
- Explicit `/retry` execution output and acknowledgement sharing the retry
  message's scope while the task's origin `ReplyTarget` remains immutable
- Profile/Mode immutable snapshots
- Codex SDK version compatibility
- Sandbox and approval translation
- End-to-end execute task snapshot and writable Codex execution; fail-closed
  mode authorization when its policy is missing
- Completed-item normalization for zero, blank, one, ten, and eleven-or-more
  Codex items: only non-empty completed `agentMessage` text is reply-worthy;
  deltas, tool/reasoning/status items, and terminal aggregate content never
  duplicate it, while a final-string-only compatibility runtime synthesizes
  exactly one item
- Policy-authorized media-only completed output creates one media candidate and
  no empty text candidate; text plus attachments in one completed item remains
  one candidate bundle while any channel-required sends consume their own slots
- Scope creation in the durable inbound transaction before acceptance or
  rejection, so command/validation errors share the same ten-slot enforcement
- Immediate best-effort WeChat typing state for every supported finished user
  message, including commands and media, before slow ingress/task work;
  unsupported wire states emit none, duplicate delivery may refresh it, and
  getconfig/sendtyping failures never reject durable acceptance or consume
  its runtime-loop timeout
- Item-order preservation and deterministic within-item chunking at the WeChat
  text bound, with distinct `MESSAGE_STATE_FINISH` replies and no cross-item
  coalescing or cumulative `GENERATING`/`FINISH` identity mixing
- Metadata-versioned `/ask` sender rendering: each eligible text item is
  exactly `<canonical-agent-id>: <message>`, the prefix occurs once before
  3,000-character chunking and counts toward that limit, multiple items each
  receive one prefix, media-only items receive no sender-only text, and legacy
  or in-flight tasks without the marker remain unprefixed. As a boundary case,
  a `planner` reply containing 5,992 copies of `界` renders to 6,001 characters,
  fragments as `[3000, 3000, 1]`, and concatenates to exactly one
  `planner: ` prefix followed by the raw item text
- Exact completed-event and terminal replay before and after `/agent` route
  switches and `/notify` changes: one candidate and fragment set survives in
  both front-to-background and background-to-front route changes with its first
  rendered content, foreground, and notification snapshot, without identity
  conflicts, duplicate output, or a doubled `/ask` sender prefix; direct
  conflicting candidate reuse still fails closed
- SDK item-ID scoping and fallback uniqueness by task/execution plus execution
  attempt ordering, including explicit retry output that neither conflicts with
  nor reorders an earlier attempt's events
- Durable per-item emit acknowledgement and crash/failure boundaries, with no
  process-local buffered text sent before its event transaction commits
- Exact inbound reply-scope isolation and atomic ordinals `1..10` under
  duplicate item notifications, duplicate inbound delivery, concurrent command
  and task projection, multiple workers, lease expiry, ambiguous send outcome,
  and restart; no path creates ordinal 11 or recycles an ordinal
- Shared allowance across command acknowledgements/results/errors, Agent item
  fragments, notifications, text, and outgoing media; typing/config/upload
  calls consume no slot, and an `/ask` acknowledgement reduces the slots
  available to its later item replies
- Atomic acknowledgement-before-dispatch ordering for `/ask` and `/retry`, so
  a fast task worker cannot reserve an earlier ordinal than its command reply
- One canonical claimable send per reply slot, with subordinate text/media rows
  unable to fan one slot out into multiple sends or combine distinct completed
  items in one multi-item `SendMsg`
- Scope-locked, ordinal-aware chunking and an unconditional tenth-text
  continuation suffix, with no item-11 race, lost tail, or eleventh notice when
  the tenth reply is media
- FIFO quota overflow and `/recv` behavior for empty, partial, full, repeated,
  concurrently extended, crash-recovered, and multi-batch queues; replaying one
  `/recv` inbound message reuses its batch instead of draining another
- Stable outbox delivery IDs and retry leases, including primary-client-ID
  reuse, a definitive one-way context-preparation branch whose ambiguous
  retries stay on the contextless wire ID, the same logical slot across both
  variants, and immutable old-task context when a newer inbound message arrives
- User outbox versus Agent mailbox separation
- Agent request/response correlation and ACL rejection
- Notification suppression and explicit `/inbox` presentation under the new
  command scope, including more than ten selected items, marking only delivered
  items presented, durable `/recv` continuation for selected excess, no slot
  reservation for unselected inbox-only items, no history or notification
  replay during `/agent` switching, and no borrowing an unrelated inbound scope
  for background notifications
- Attachment path, quota, checksum, retention, and permission checks
- CDN download size/deadline bounds, strict AES keys, strict PKCS#7 padding,
  and no publication after a failed download
- Complete, collision-resistant, atomically published credential records with
  private directory/file permissions
- Image native input path
- Voice transcript-to-task ingress, blank-transcript retry, and audio replay dedupe
- File-only staging response, managed bytes, attachment references, and zero-task invariant
- `/agent` existing switch, dynamic creation, durable route/Profile restore, and invalid-name rejection
- `/delagent <agent_id>` dynamic-Agent deletion, default-Agent protection,
  atomic unfinished/orphaned-work rejection, route fallback, restart
  tombstones, immutable historical task preservation, and exact-metadata
  reactivation through a later explicit `/agent <same-id>`, including
  multi-version restoration, interrupted prepare/commit recovery, atomic
  tombstone-and-route publication, and concurrent scope switches
- `/ask` reuse or dynamic Agent creation, immediate task acknowledgement, and
  sender-labeled ordered completed-item delivery to the original WeChat reply
  target even when that Agent is not front or has `/notify off`, including
  crash-before-task and crash-after-task receipt recovery, exact-scope legacy
  dedupe validation, and monitor-cancellation receipt terminalization
- Collision-resistant conversation and command tuple IDs plus scoped legacy
  persisted-ID compatibility, including mailbox snapshot revalidation
- Shared durable Codex and `/sh` workspace resolution for source and installed
  launches
- Concurrent iLink poll/send/upload isolation and close/request race handling
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
- Local audio/video processing (voice uses the channel-provided transcript only)
- Editable Profile/Mode administration UI
- Skill installation, editing, or registry mutation from a channel command
- A separate `/execute` command (execute is selected only through `/mode`)
- Automatic retry of orphaned tasks
- Exactly-once external message delivery

The MVP is successful when the current WeChat-to-Codex flow becomes a durable, restart-aware background task system without losing messages, duplicating tasks, or blocking command handling.
