# Codex-on-WeChat Runtime Plan

This is the normative plan for the implemented durable WeChat bot. It begins
with the complete command surface, then defines the features and process
boundaries behind those commands. The production target is one supervisor OS
process plus one persistent, fresh-interpreter Agent process for every enabled
Agent. Process separation is required for lifecycle, concurrency, and failure
isolation; it is not presented as a hostile-code security sandbox.

The current durable schema is version 33. Version 32 adds a presentation cursor
to item-based reply candidates and persists command-receipt
`response_fragments_json`. Its migration marks every older candidate presented,
so enabling switch-back presentation can never reinterpret a pre-v32 transcript
as unread output. Version 33 adds `session_agent_working_directories` for
durable user/session/Agent working-directory selections.

The bounded WeChat wire-aggregation design in section 8.3 is the next planned
schema revision, v34; it is not implemented by the current v33 runtime. Version
34 adds durable aggregation groups and ordered source-item membership while
migrating every older fragment/outbox as a sealed singleton. It never
reinterprets already sent, quota-deferred, or inbox-only history as a new
multi-item aggregate.

## 1. Command contract

### 1.1 Common command rules

- A command starts when `/` is the first non-whitespace character. Command
  names are ASCII case-insensitive and normalized to lowercase.
- Slash commands remain in the supervisor control plane. They are never sent
  to an Agent as ordinary prompt text.
- Missing or extra arguments produce a deterministic `usage:` reply without a
  partial side effect. Unknown slash commands produce an error and no task.
- Every supported inbound command is durably accepted before its effect or
  acknowledgement. Redelivery reuses the same receipt and presentation.
- Mutations for one `(channel, bot, user, session)` are serialized so route,
  role, mode, model, working directory, and task snapshots cannot observe
  half-applied changes.
- Command replies use the same ten-send reply scope, explicit destination, and
  3,000-character wire limit as Agent replies. Planned v34 applies terminal
  wire aggregation before allocating those sends.
- `/clear` and `/reset` are public aliases. `/listskill` and `/listskills` are
  supported help-hidden compatibility aliases of `/skills`.
- `/execute`, `/interrupt`, `/commands`, `/listmodel`, `/listmodels`, and the
  old in-memory session commands are intentionally not public commands.

### 1.2 Authoritative command registry

`src/channels/wechat.py` owns one immutable registry. `/help` is generated from
it, so each public syntax appears once. In particular, model selection is one
combined `/model` line rather than several overlapping commands.

#### Conversation

| Syntax | Behavior |
| --- | --- |
| `/help` | Render the generated command registry. |
| `/clear` | Start a fresh conversation for the current session and front Agent. |
| `/reset` | Public alias of `/clear`. |
| `/compact` | Compact the exact current Agent/session provider context in place. |
| `/cd [path]` | Show or change the current Agent's working directory. |
| `/sh <command>` | Run one bounded supervisor-owned shell command in the front Agent's accepted working directory. |
| `/skills` | List enabled skills for the front Agent. |
| `$<skill> <task description>` | Queue a task with an immutable skill snapshot. |

#### Tasks

| Syntax | Behavior |
| --- | --- |
| `/status` | Show the caller's active tasks and relevant Agent health. |
| `/tasks [limit]` | List the caller's durable task history. |
| `/retry <task-id>` | Explicitly retry eligible failed, interrupted, or orphaned work. |
| `/cancel [task-id]` | Cancel the named task, or the caller's current front-Agent task. |

#### Agents

| Syntax | Behavior |
| --- | --- |
| `/agents` | List enabled Agents, current route, PID, generation, and health. |
| `/agent [agent-id]` | Show the current Agent, or create/ensure and switch; a switch may include eligible unseen background items from the destination Agent. |
| `/delagent <agent-id>` | Retire a dynamic Agent and reap its dedicated process. |
| `/ask <agent-id> <prompt>` | Queue explicit work for an Agent without changing the route. |

#### Agent configuration

| Syntax | Behavior |
| --- | --- |
| `/system [default|<role>]` | Show, set, or clear the current Agent's session role. |
| `/mode [chat|plan|review|execute]` | Show or select the mode for future tasks. |
| `/modes` | List modes and mark the effective selection. |
| `/model [<model-id> <effort|default>|effort <effort|default>]` | Show or select model and reasoning effort. |
| `/models` | List advertised models and supported efforts. |
| `/notify [on|off]` | Show or set background notification preference. |

#### Delivery

| Syntax | Behavior |
| --- | --- |
| `/inbox [agent-id|all]` | Present selected unseen background output. |
| `/recv` | Deliver the next FIFO batch deferred by the ten-send quota. |

### 1.3 Important command semantics

- `/agent B` changes future routing and never sends B's provider transcript,
  task/event history, allocated or sent output, or quota-deferred fragments.
  When B was not the front Agent, the switch acknowledgement may also present
  only B's post-v32 unseen completed-item candidates that were durably captured
  as background and whose fragments all remain `inbox_only`. This is delivery
  of an unread presentation candidate, not replay of conversation history.
- A valid unknown `/agent <id>` creates a dynamic Agent, starts its process,
  waits for readiness, and only then switches the route.
- `/delagent` never deletes immutable Profiles, tasks, events, or conversation
  history. It blocks new work, stops and reaps the exact child, unregisters the
  runtime only after cleanup, and preserves records for audit/retry decisions.
- Recreating a deleted ID creates a fresh lifecycle identity and child process;
  it does not adopt the old process or provider-thread cache.
- `/cd` with no path shows the front Agent's selected working directory.
  `/cd <path>` changes it only for the exact channel, bot, user, session, and
  Agent. A quoted path may contain spaces, and a relative path resolves from
  that Agent's current selection.
- `/cd` accepts only an existing, enterable directory canonically contained by
  `CODEX_WECHAT_WORKSPACE`. A symlink or canonical-path escape fails closed.
  The selection is durable across restart and switching away and back; it does
  not call process-global `os.chdir()` or restart an Agent child.
- `/ask` is requested output even when the destination is not the front Agent.
  Each nonblank completed text item is rendered as `sender: message`, where
  `sender` is the canonical Agent that produced the item. Its task uses the
  destination Agent's selected working directory for the originating session,
  not the front/source Agent's directory.
- `/system` affects future tasks for the exact user/session/Agent. A changed
  role rotates the conversation binding. It cannot grant permissions and is
  never an edit of the administrator-owned Agent Profile.
- `/compact` resolves the front Agent and exact current conversation, mode,
  Profile/policy versions, role, model, and workspace under the per-session
  control lock. It rejects an active conversation or missing durable thread
  binding before provider work. Native compaction preserves the provider thread
  and never deletes durable task/event history; `/clear` remains the command
  for starting a fresh conversation.
- `/model` accepts `ultra`, `max`, and future effort names only when the chosen
  model advertises them. `default` clears the override without guessing.
- `/cancel` commits the durable cancellation request before asking the owning
  Agent process to interrupt its local turn. Another Agent process is never
  interrupted by that request.

## 2. Implemented process topology

```text
./cow supervisor process
├── WeChat monitor/gateway, typing, commands, CDN, and SendMsg
├── TaskManager, durable task/mailbox dispatch, and reply delivery
├── SQLiteStore and its dedicated executor thread
├── Agent bridge server and managed attachment publisher
├── process proxy: codex
│   └── dedicated codex Agent process
│       └── one asyncio loop + one CodexRuntime + one SDK client
├── process proxy: planner
│   └── dedicated planner Agent process
│       └── one asyncio loop + one CodexRuntime + one SDK client
└── process proxy: any other enabled Agent
    └── its own persistent Agent process and runtime state
```

The supervisor contains no live production `CodexRuntime`. It stores only a
`ProcessAgentRuntime` proxy for each Agent. The default Agent owns one child;
`for_agent(agent_id)` creates a configuration-preserving but independent proxy
for every named Agent.

Each Agent child owns its own:

- OS PID and process generation;
- asyncio event loop and single execution slot;
- `CodexRuntime` instance and Codex SDK/app-server client;
- provider thread bindings, model/effort compatibility caches, active turns,
  interrupt state, and skill cache.

No live object above is shared between Agents. Agent children share the
configured confinement root and Unix user, so simultaneous execute-mode tasks
can still conflict when their selected directories overlap.

The supervisor alone constructs and accesses SQLite, the WeChat client,
delivery workers, reply quotas, and durable routes. Children are not given a
store or channel object and cannot send directly to WeChat.

## 3. Process startup, lifecycle, and health

### 3.1 Fresh-interpreter bootstrap

Every Agent is started through a fresh Python interpreter, never by forking a
supervisor that already owns SQLite, HTTP clients, threads, or an event loop.
The bootstrap imports the child module directly rather than re-executing the
public `codex_wechat_bot.py` launcher. The child constructs its runtime only
after the private IPC endpoint and configuration are established.

The child import boundary excludes SQLite/store, channel, and WeChat modules.
The production backend imports only SDK-independent Agent contracts, the
required mode/media/role helpers, and `CodexRuntime`.

### 3.2 Lifecycle states

The public proxy exposes these health values:

```text
new -> starting -> ready -> busy -> ready
starting | ready | busy -> lost
ready | busy -> stopping -> stopped
lost -> starting                         on restart
```

- `pid` is the current child leader PID or absent.
- `generation` increases on every restart of one proxy.
- A dynamic delete reaps the child before its only supervisor handle is
  discarded.
- Cleanup failures retain the process handle, registry ownership marker, and
  capacity reservation so shutdown can be retried safely.
- Normal shutdown stops task/mailbox workers and Agent children while the
  collaboration bridge remains available, then closes the bridge and SQLite.
- Every child is joined. Graceful-stop timeout escalates to termination and
  kill of the Agent process group, and cleanup succeeds only after exit is
  proven.

### 3.3 Failure behavior

If a child disappears while idle, only that Agent becomes `lost`; peer Agents
and WeChat remain alive. Its next explicit `start()` or invocation creates a
new process generation.

If a child disappears after a task crossed the runtime boundary, side effects
may already have occurred. The proxy raises an exception marked
`execution_uncertain`; `TaskWorker` records the attempt as `orphaned`. It is
never silently retried. The user may inspect it and issue `/retry` explicitly.

Supervisor parent-death signaling kills each Agent leader if the supervisor is
hard-killed. Complete non-escapable cleanup of arbitrary daemonized tool
descendants requires optional cgroup-v2/container hardening. That hardening is
not required to claim the implemented independent Agent processes, and process
separation alone is not a hostile-code sandbox.

## 4. Scheduling and concurrency

SQLite remains the durable source of truth. The active scheduling path is:

```text
queued durable invocation
  -> supervisor compatibility claim and lease
  -> ProcessAgentRuntime proxy
  -> owning Agent child RUN
  -> acknowledged events and terminal result
  -> durable completion and reply projection
```

The word `compatibility` here names the existing durable claim backend; it no
longer means in-process Agent execution.

Scheduling rules:

- Task and mailbox invocations share one durable FIFO and one active slot per
  Agent incarnation.
- The same Agent executes at most one runtime invocation at a time. The proxy
  and child both enforce this even for lightweight/custom stores.
- Different Agents have different processes and can execute concurrently.
- Filesystem-barrier tests must prove overlap; coroutine creation alone is not
  accepted as evidence of parallel Agent execution.
- The supervisor uses enough dispatch coroutines to feed independent Agents.
  `CODEX_WECHAT_MAX_AGENT_PROCESSES` controls both the default process capacity
  and production dispatcher breadth; its default is 16.
- `CODEX_WECHAT_WORKERS` is deprecated and ignored so an old value of `1`
  cannot accidentally serialize the new topology.
- A saturated process limit creates no extra child. Capacity is released only
  after a child has stopped and been reaped.

## 5. Private Agent IPC

Each proxy and child communicate over one private, inherited, full-duplex IPC
endpoint using bounded canonical JSON messages. Every envelope carries protocol
version, Agent ID, process generation, message ID, correlation ID, kind, and a
JSON payload. Agent tasks, events, and results cross the boundary only through
SDK-independent value objects.

Required protocol behavior:

- The child sends `ready` only after its runtime and SDK client start.
- A parent `run` carries one immutable task snapshot for the exact child Agent,
  including its accepted `execution_workspace`.
- The child may continue reading priority `interrupt` and `stop` controls while
  a run is active.
- One child accepts only one run at a time.
- Every emitted event is sent to the supervisor and waits for an acknowledgement.
  The supervisor acknowledges only after its emit callback returns, which is
  the durable event-commit boundary in production.
- Terminal results are correlated to the exact run and generation.
- Unknown, duplicate-conflicting, cross-Agent, or stale-generation messages
  fail the child generation closed.
- IPC and callback waits are bounded; payload nesting, numbers, mapping keys,
  and maximum encoded bytes are validated.
- Session compaction is a correlated control call sent only to the selected
  Agent child. Provider configuration, credentials, and context-cache entries
  never cross IPC.

Two supervisor-owned capabilities are relayed without moving their owners into
the child:

- Generated-image publication calls back to the supervisor. The parent
  validates task/execution/Agent correlation and invokes the publisher with
  the original task, including its reply target; only the attachment ID returns.
- Agent collaboration capabilities are issued in the supervisor for the exact
  task execution. The child receives the task-scoped bearer token only for its
  turn and uses the owner-only bridge socket; the bridge revalidates active
  task policy before list/send operations.

## 6. Agent identity, routing, and history

A front route is scoped by `(channel, bot_id, external_user_id, session_id)`.
Conversation identity also includes the Agent:

```text
(channel, bot, user, session, agent) -> conversation_id
```

Provider thread bindings are narrower again and include conversation, mode,
Profile version, policy version, role version/hash, and persona-composition
version. Consequently:

- switching Agents does not merge threads or resend provider/task history;
- changing role/policy creates a new binding rather than resuming under changed
  instructions;
- queued/running tasks retain the route, conversation, role, mode, model,
  effort, skill, media, working-directory snapshot, and reply target accepted
  with that task; and
- a child restart can resume a durable provider thread ID but never adopts a
  different Agent's cache.

Dynamic Agents have independent durable Profiles, lifecycle identity, routes,
preferences, task queue, mailbox, conversation IDs, child PID, runtime, and
SDK client. Immutable historical rows survive deletion and recreation.

### 6.1 Durable switch-back presentation

Schema v32 adds `reply_candidates.presentation` and `presented_at`. The v32
migration marks every pre-v32 candidate `presented`; no row created before the
unread cursor existed can later be guessed to be a missed reply.

After `/agent B` commits B as the front route, it may call
`present_inbox_candidates(..., agent_id=B, switch_only=True, present=False)`.
The exact channel/bot/user/session-scoped candidate must satisfy all of these
conditions:

- `presentation='unseen'`;
- immutable background classification (`foreground=0`);
- a completed item-based candidate with nonblank text;
- at least one reply fragment; and
- every fragment still has state `inbox_only`.

This selector never reads provider transcripts, task/event history, or
conversation history. It excludes all pre-v32 history, foreground candidates,
already presented candidates, attachment-only or fragmentless candidates, and
any candidate with allocated, sent, failed/ambiguous, mixed-state, or
`deferred_quota` output. In particular, `/recv` exclusively owns
quota-deferred fragments. An attachment-only candidate remains unseen for a
surface that can represent it; `/agent` never renders it as empty text.

### 6.2 Working-directory selection and execution snapshots

`CODEX_WECHAT_WORKSPACE` is the canonical confinement root for every Agent
working directory, not merely a default cwd. Schema v33 stores one root-relative
preference and target device/inode identity for each
`(channel, bot, user, session, Agent)`. `/cd` query and mutation use that exact
scope, and a mutation plus its command-receipt completion commit atomically.

At task acceptance, the supervisor resolves the destination Agent's preference
and stores an immutable `execution_workspace` containing canonical root/path
values and root/target device and inode identities. Queued and running work
keeps that accepted snapshot; a later `/cd` affects only future work. The
selected root and target must already exist, be enterable, and remain
canonically inside the configured root.

The Agent child validates the snapshot before SDK/client or network work and
again immediately before the native turn. A missing, moved, replaced, or
identity-changed root or target fails closed instead of silently changing cwd.
Selection is task-local: `/cd` never calls process-global `os.chdir()` and does
not restart a child process.

A stale selected directory does not poison the control plane: cwd-dependent
tasks, `/sh`, skill discovery, `/cd` queries, and relative `/cd` fail closed, while
cwd-independent commands and an absolute `/cd` to a valid in-root directory
remain available for recovery.

For backward compatibility, a legacy durable task without an
`execution_workspace` continues in that Agent's configured default working
directory. The device/inode-pinned fail-closed guarantee therefore applies to
snapshotted and newly accepted work.

Validation is repeated immediately before an SDK/native turn and before the
supervisor launches `/sh`. Those pathname-based APIs do not provide an open
directory-descriptor (`dirfd`) contract, so a residual TOCTOU window remains
between final validation and path consumption.

`/sh` remains a supervisor command and runs with the front Agent snapshot
captured when that command was accepted. Explicit `/ask` work uses the
destination Agent's selection for the originating user/session scope. Durable
Agent-to-Agent mailbox work uses the same destination-scoped rule rather than
inheriting the sending Agent's directory.

## 7. Profiles, modes, system roles, models, and skills

### 7.1 Versioned metadata

Profiles and Modes are immutable by `(id, version)`. Re-registering the exact
same canonical metadata is idempotent. A genuinely different payload under the
same key is corruption and fails closed. Legacy metadata is canonicalized at
the boundary so benign representation differences do not produce errors such
as `mode version metadata conflicts: codex/execute@1` or
`profile version metadata conflicts: bb@2`.

Set-valued Profile and Mode fields compare by canonical membership rather than
JSON array order. Ordered responsibilities/constraints, lifecycle state, and
all permission-bearing scalar values remain exact and conflict-fatal.

New versions are appended; live database rows are never edited in place merely
to match current code. Startup publishes every retained Profile/Mode snapshot
needed by queued tasks.

### 7.2 Mode permissions

The built-in modes are `chat`, `plan`, `review`, and `execute`.

| Mode | Network | Commands | File writes | Intended use |
| --- | --- | --- | --- | --- |
| `chat` | yes | yes | no by policy/instruction | Conversation and analysis |
| `plan` | yes | yes | no by policy/instruction | Inspection and planning |
| `review` | yes | yes | no by policy/instruction | Review and diagnosis |
| `execute` | yes | yes | yes | Implementation and verification |

`/mode execute` records explicit trusted authorization for future tasks. The
task's effective policy snapshot is validated again inside `CodexRuntime`.
`/plan` is not a command; plan mode is selected with `/mode plan`.

These process/mode boundaries are cooperative policy, not protection against a
malicious same-UID Agent. A stronger sandbox must use OS isolation.

### 7.3 System roles

`/system` stores a normalized, versioned user role for one
user/session/Agent. The normalization version is pinned; malformed controls,
NUL, invalid Unicode, and oversized values are rejected. Repeating the current
role is idempotent. A changed role and conversation rotation commit together.

The role is instruction only. It cannot grant network, command, filesystem,
collaboration, peer, or delivery permissions. Mailbox work does not inherit a
user session role.

### 7.4 Models and efforts

Model discovery happens in the selected Agent's own child process. The complete
paginated catalog is normalized in the supervisor. Selections are stored per
session and copied into future immutable tasks; they do not require a mutable
supervisor runtime cache.

Efforts are capability-driven, not hard-coded. `ultra` is enabled exactly for
models that advertise it. Unsupported model/effort pairs change nothing.

### 7.5 Context windows and compaction

Context discovery and native compaction belong to the selected Agent child.
The child reads Codex's effective configuration, then queries the configured
provider's same-origin model detail/list endpoints for an exact, case-sensitive
model ID. Only validated context-window extensions are accepted; ordinary
OpenAI-compatible catalogs are allowed to omit them.

For a resolved window, thread start and resume receive model-specific native
Codex settings. The total-token auto-compaction threshold is
`floor(context_window * 0.80)`, or a lower validated provider/configured
threshold. If provider metadata is absent, `model_context_window` is a fallback
only when the effective configuration selects that exact model. Otherwise no
window is invented and native Codex configuration/catalog behavior remains
authoritative.

Resolution uses a child-local TTL cache keyed by provider, base URL, exact
model, and effective fallback. Agent processes never share the cache. Provider
credentials are read and used only inside the child, are never logged, and do
not cross IPC.

Manual `/compact` uses the same exact mode/Profile/policy/role binding as the
current session. The supervisor rejects active or unbound contexts, then the
owning child invokes native SDK compaction in place. The provider thread ID and
durable history are preserved; another Agent process cannot be targeted.

### 7.6 Skills

The selected Agent process discovers skills, while the supervisor persists
canonical descriptors and immutable bundle hashes. `$<skill>` acceptance
snapshots the exact skill version/path/hash. Changed or missing bytes fail
closed rather than silently substituting another skill.

## 8. WeChat ingress, typing, item identity, and bounded aggregation

### 8.1 Ingress and typing

Only finished user messages are accepted. Supported text, voice transcript,
and retained media items are normalized in wire order into one inbound
instruction and one deduplicated task. The channel cursor advances only after
the required durable acceptance/command boundary succeeds.

As soon as a supported message is received, the monitor sends a best-effort
typing state using the exact bot/user/context. Typing is presence metadata, not
a reply message, so it consumes none of the ten reply slots. Typing failure
does not reject an otherwise valid durable message.

### 8.2 Stable item boundary

Reply eligibility, identity, ordering, and deduplication are item-based. A
stable completed runtime item is reply-eligible when it is an Agent message
containing nonblank text. Deltas, reasoning, tool calls, status updates, and
aggregate terminal text do not create extra candidates. One durable source
item is not necessarily one WeChat `SendMsg`: compatible text candidates may
be combined only by the wire-aggregation stage defined below.

Identity prefers the SDK item ID and falls back to a deterministic ordinal
within the task execution. Replaying the same identity with the same canonical
content is idempotent. Reusing it with different content, foreground class, or
notification snapshot fails closed. This prevents the former
`reply candidate identity conflicts: notify_enabled, foreground` failure from
being caused by mutable route/notification state during replay.

Aggregation never erases the source boundary. Every rendered byte remains
traceable to an ordered candidate/member record, so replay, sender attribution,
inbox presentation, notification policy, and audit continue to operate on
stable completed items even when the wire uses fewer messages.

### 8.3 Durable bounded wire aggregation (planned schema v34)

The newer `origin/main` implementation provides the behavioral reference: it
buffers completed Agent-message text, inserts `\n\n` between items, and flushes
on size, elapsed time, or turn completion before sending terminal messages.
This design adopts that buffer-before-send ordering, but not its process-local
per-user buffer, 2,500-character transport limit, event-driven pseudo-timer, or
tenth-message truncation. Aggregation here is SQLite-backed, exact-scope, and
uses the existing 3,000-character wire limit.

An open text aggregate is keyed by all immutable properties that could change
meaning or delivery:

- origin reply scope and full reply target, including channel, bot, user,
  session, source identity, and context token;
- aggregation provenance: the exact task and execution for live output, or the
  exact command receipt for newly rendered command output;
- producing Agent and sender-prefix format/version;
- foreground/background class, notification snapshot, priority, delivery
  mode, presentation class, and renderer version; and
- text-only wire kind.

Different users, sessions, Agents, commands, notification classes, or
renderers never share an aggregate. Live output never crosses its source task
or execution. A `/agent` or `/inbox` command may pack eligible candidates from
several historical source tasks only inside that exact immutable command
receipt; the resulting presentation aggregate still preserves every candidate
identity and source boundary. `/recv` never repacks or combines its already
sealed aggregates. Media and adapter-defined bundles are hard barriers.
Notification-suppressed `inbox_only` candidates remain individually unseen
until an explicit presentation command selects them.

The deterministic packer applies sender rendering first, orders members by
their stable source ordinal, and inserts exactly two newline characters
between different source items. Continuations of one long item are lossless
and receive no artificial item separator. Packing measures the final rendered
WeChat text in Python characters. Appending content that would exceed 3,000
seals the largest complete prefix and continues in another aggregate; no text
is truncated or silently dropped.

An open aggregate is sealed and made eligible for quota allocation when any of
these conditions occurs:

- it reaches the 3,000-character hard boundary;
- 120 seconds have elapsed since its first unflushed member;
- its task becomes completed, failed, interrupted, cancelled, or orphaned;
- media, a bundle, or any aggregation-key change must preserve source order;
  or
- an explicit command, `/agent` switch-back, `/inbox`, or `/recv` response is
  ready. Command and drain responses never wait for the timer.

The max-age deadline is durable and serviced by a real scheduled/reconciliation
path; it is not checked only when another item arrives. Event/candidate insert
and aggregate membership commit together. Terminal task projection and sealing
of its remaining text commit together. Concurrent deadline and terminal
flushes must converge on one immutable aggregate ID, member list, rendered
content, and payload hash. Only sealed aggregates can obtain a reply slot or
outbox row. Once sealed, membership and content never change; retries reuse the
same `client_id` and exact terminal `MESSAGE_STATE_FINISH` payload.

Schema v34 adds durable aggregate and ordered membership records containing the
exact grouping key, state (`open` or `sealed`), renderer version, content hash,
first/last source sequence, `flush_due_at`, and seal timestamps. Reply-slot and
outbox allocation points to a sealed aggregate, while membership maps every
source candidate/fragment to that aggregate in order. Startup restores due open
groups; replay cannot append a member twice. The migration wraps each pre-v34
wire fragment in a sealed one-member aggregate while preserving its slot,
client ID, outbox, delivery state, and deferred FIFO position.

### 8.4 Atomic switch-back acknowledgement

`/agent` selects eligible destination-Agent candidates without marking them.
It renders them under `unseen messages:` and stores their candidate IDs,
complete response text, item-local boundaries, and the deterministic sealed
aggregate specification in the durable command receipt. Schema v34 persists
this as `response_aggregates_json` while retaining the older fragment field for
receipt compatibility. Selection alone therefore cannot lose a reply if
command projection fails.

Projecting every sealed command aggregate and changing all of its selected
member candidates to `presentation='presented'` happen in one SQLite
transaction. A validation or projection failure leaves both effects
uncommitted. Crash/redelivery reuses the immutable command receipt, aggregate
membership, presentation IDs, content, and outbox; it does not rerun the
selector against newer state or mark a different set.

The acknowledgement header and each unseen text item retain separate logical
membership boundaries, but adjacent rendered text may share one wire aggregate
when the complete result stays within 3,000 characters. The resulting command
response uses the new `/agent` inbound message's normal ten-slot scope.
Overflow from that newly rendered command response may itself become a sealed
`deferred_quota` aggregate and later belongs to `/recv`. This does not make an
excluded source candidate eligible: a source candidate that already contains
allocated, sent, mixed-state, or `deferred_quota` material is never selected in
the first place.

### 8.5 Ten-send quota and 3,000-character aggregates

Each accepted user message opens one durable reply scope containing at most ten
logical `SendMsg` identities. Command acknowledgements, sealed Agent text
aggregates, media, and retries of an already allocated send share those ten
slots. Aggregation and sealing happen before quota allocation, so several
compatible source items can spend one slot while retaining all member IDs.

Each sealed text aggregate contains at most 3,000 final rendered Python
characters. At most ten text aggregates/media sends are allocated to the
current scope; overflow is stored FIFO as `deferred_quota`. `/recv` moves those
already sealed aggregates into a later scope without reordering, crossing task
boundaries, or recombining them. A failed or ambiguous send never recycles its
ordinal.

If the final available slot includes a `/recv` continuation notice, the notice
and its separators count toward the same 3,000-character bound. Any body text
displaced by that suffix is sealed at the head of the deferred FIFO; it is
never truncated or dropped.

### 8.6 Sender and destination

Every channel send specifies the destination user explicitly rather than
relying on ambient contact state. Durable rows retain the full reply target,
including channel, bot, external user, session, source message/sequence, and
optional context token.

Explicit `/ask` results and Agent-to-Agent correlated answers include the
producing Agent prefix in text: `sender: message`. Prefix rendering happens
before packing and counts toward the 3,000-character bound. A live aggregate
never crosses producing Agents, so several compatible items from one `/ask`
render as `sender: first\n\nsecond`, not repeated ambiguous sender changes.
Multi-Agent command views keep an explicit prefix at each producer boundary.
General foreground replies retain their normal item text while still carrying
the durable producing Agent identity.

### 8.7 Notifications and delivery

Foreground replies and explicit `/ask` output are push-eligible regardless of
`/notify`. Background output is either pushed when enabled or retained for
`/inbox`. Aggregation never lets notification-suppressed/background content
piggyback on a foreground or push-eligible aggregate. `/inbox` selects unseen
item candidates; `/recv` drains only sealed quota-deferred aggregates. Text and
media sends use durable outboxes, stable wire IDs, bounded retry, and explicit
unknown-outcome states.

## 9. Agent-to-Agent collaboration

Agent collaboration uses a durable mailbox owned by the supervisor. A source
turn receives a short-lived capability bound to its task and execution. The
bridge lists only authorized peers and persists sends only after revalidating
the active immutable policy.

Mailbox scheduling rules:

- the destination Agent's child executes the mailbox turn;
- the invocation uses that destination Agent's working directory for the
  originating user/session scope, never the source Agent's directory;
- task and mailbox work share the same one-invocation Agent slot and FIFO;
- mailbox conversations are destination/request-scoped and never reuse a user
  conversation or session role;
- correlated replies are persisted as Agent messages, not sent directly by the
  child; and
- claim loss cancels the local run and suppresses stale publication.

Different destination Agents can process mailbox work concurrently in their
different processes.

## 10. Persistence, recovery, and deletion

Current v33 SQLite owns inbound deduplication, commands, Profiles/Modes,
routes, roles, session/Agent working-directory preferences, tasks and their
immutable execution-workspace snapshots, executions, events, mailboxes,
attachments, reply candidates/fragments, reply slots, and delivery outboxes.
Planned v34 additionally owns open/sealed wire aggregates, ordered membership,
and durable flush deadlines. IPC and process-local aggregation buffers are
never the durable source of truth.

One supervisor epoch and filesystem ownership locks prevent concurrent owners
of the database/account. Startup recovery runs only after the new epoch is
active. It restores dynamic Agent registrations, starts one child per enabled
Agent, republishes immutable definitions, and then resumes dispatch/delivery.

Claims and leases fence task/mailbox writes. A worker that loses ownership
must not begin a turn or terminalize a late result. Child loss after run starts
is orphaned; ordinary model/runtime failures are failed; explicit cancellation
is interrupted/cancelled according to the durable state.

Deletion is retirement, not erasure. `/delagent` redirects routes, rejects new
work for the retired incarnation, reaps the exact child, and retains historical
rows. It clears only that Agent's mutable working-directory preferences;
immutable task/history rows and their accepted snapshots remain. Process-local
historical Profile snapshots may remain cached for audit, but creating another
Agent publishes definitions only for that selected Agent; it must never
republish an unrelated retired Profile's stale `enabled` value. Global startup
likewise omits detached tombstoned history while retaining strict validation
for explicitly registered Agents. Recreating the same string ID starts a new
process/lifecycle context.

## 11. Media and generated images

Binary media lives in a managed filesystem; SQLite stores immutable metadata,
checksums, ownership references, and delivery state. Agent tasks receive only
authorized managed inputs, never raw remote channel credentials.

Generated-image completion is proposed by the child and published by the
supervisor. The supervisor securely reads a regular workspace file or bounded
data image, verifies type/size/identity, stores it idempotently, registers
metadata, and returns only a managed attachment ID to the child. Media delivery
uses the same reply scope and explicit destination rules as text.

## 12. Security and trust boundaries

The process design provides failure and lifecycle isolation:

- one Agent crash does not directly kill peer runtimes or WeChat;
- one Agent cannot mutate another Agent's in-memory SDK/thread cache;
- the supervisor can interrupt, restart, or delete one Agent independently;
  and
- process PIDs/generations make the boundary observable and testable.

It does not provide same-UID confidentiality. Agent processes share the host,
user account, and configured confinement root. Full hostile isolation would
require containers/namespaces, separate credentials, brokered tools, and a
delegated cgroup-v2 or equivalent job boundary. The existing cgroup/job modules
remain optional hardening foundations; production process-per-Agent execution
does not fail closed merely because a delegated cgroup is unavailable.

## 13. Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `CODEX_WECHAT_DB` | user runtime database | Canonical SQLite path; supervisor only. |
| `CODEX_WECHAT_WORKSPACE` | durable user workspace | Canonical confinement root for every Agent and `/sh` working directory. |
| `CODEX_WECHAT_ATTACHMENTS` | managed attachment root | Durable binary media directory. |
| `CODEX_WECHAT_SKILL_ROOTS` | empty | Trusted skill bundle roots. |
| `CODEX_WECHAT_TURN_TIMEOUT` | runtime default | Per-turn bound passed to each child runtime. |
| `CODEX_WECHAT_MAX_AGENT_PROCESSES` | `16` | Maximum live Agent children and dispatcher breadth. |
| `CODEX_WECHAT_MAX_AGENT_QUEUE` | store default | Per-Agent unfinished invocation limit. |
| `CODEX_WECHAT_MAX_GLOBAL_QUEUE` | store default | Global unfinished invocation limit. |
| `CODEX_WECHAT_MAILBOX_TTL` | store default | Mailbox expiry interval. |
| `CODEX_WECHAT_REPLY_AGGREGATION_MAX_AGE` | `120` | Planned v34 maximum seconds before nonempty compatible text is sealed for delivery. |

`CODEX_WECHAT_WORKERS` is deprecated, logs a warning, and has no scheduling or
process-count effect.

## 14. Verification and acceptance gates

The process cutover is accepted only when tests prove all of the following:

- default and dynamic Agents have unique child PIDs distinct from supervisor;
- two Agent children cross a filesystem barrier before either is released;
- two calls to one Agent never enter its backend together;
- task and mailbox invocations share that same Agent slot;
- interrupting Agent A does not interrupt Agent B;
- SIGKILL of A makes its active execution uncertain/orphaned while B continues;
- restarting A uses a new PID and higher process generation;
- dynamic creation starts a child, `/delagent` reaps it, and recreation is fresh;
- event acknowledgement follows supervisor callback completion;
- generated-image relay uses the original parent task/reply target;
- child imports contain no SQLite/store/channel/WeChat modules;
- process capacity is retained until reaping and released afterward;
- graceful, timeout, startup-failure, cancellation, and repeated-stop paths do
  not leak a child, process handle, registry marker, or capacity reservation;
- `/agent` never replays provider/task/conversation history and presents only
  exact-scope post-v32 unseen background text candidates whose fragments are
  all still `inbox_only`;
- foreground, pre-v32, presented, allocated/sent/mixed-state, fragmentless,
  attachment-only, and source-`deferred_quota` candidates stay off the switch
  response; command receipt/outbox replay preserves the original presentation
  IDs, aggregate membership, and item-local boundaries atomically;
- stable completed items remain independent source/deduplication records, while
  two compatible items `first` and `second` seal as one wire message containing
  exactly `first\n\nsecond` when the final rendering fits;
- live aggregation never crosses reply scope, full target, task/execution,
  producing Agent, sender format, notification/foreground class, delivery
  mode, media, or renderer version; command presentation may cross historical
  source tasks only within one immutable command receipt;
- an open aggregate is durable before any outbox exists; 3,000-character,
  120-second, media-barrier, and every terminal-task flush are lossless and
  idempotent across restart and a concurrent deadline/terminal race;
- source-item replay never adds duplicate membership, reply slots, or outbox
  rows, and existing v33 sends migrate as unchanged sealed singletons;
- `/ask` uses one unambiguous `sender: message` prefix per single-producer
  aggregate, final text aggregates are at most 3,000 characters, and one inbound
  scope allocates no more than ten sends;
- the tenth-slot continuation suffix is included in the length calculation;
  displaced body text and all later aggregates remain byte-for-byte recoverable
  in `/recv` FIFO order;
- media/bundle barriers preserve source order, concurrent Agents replying to
  one user never share an aggregate, and switch-back/inbox presentation marks
  every selected aggregate member atomically;
- `/cd` query/set is isolated by channel, bot, user, session, and Agent;
  quoted paths work, relative paths start at the current selection, and the
  selection persists across restart and switch-back;
- nonexistent, non-enterable, out-of-root, and symlink-escape directories are
  rejected, and accepted snapshots fail closed after root/target replacement,
  movement, or identity change;
- queued/running work retains its immutable canonical path and root/target
  device/inode snapshot while a later `/cd` changes only future work;
- `/sh` uses the front Agent's accepted snapshot, while `/ask` and mailbox work
  use the destination Agent's selection for the originating session;
- `/cd` neither changes process-global cwd nor restarts an Agent, and retirement
  clears only that Agent's mutable directory preferences;
- every public launcher uses the same durable multi-process topology; and
- focused suites, the complete pytest suite, `git diff --check`, and a safe
  `./cow --help` smoke test pass.

The v26-v31 direct child-dispatch tables, authenticated IPC foundation, pidfd
helpers, and job-containment probes remain available for future hardening.
They are not the active scheduler and must not be described as prerequisites
for the implemented process-proxy topology.
