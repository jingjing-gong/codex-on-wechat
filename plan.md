# Codex-on-WeChat Runtime Plan

This is the normative plan for the implemented durable WeChat bot. It begins
with the complete command surface, then defines the features and process
boundaries behind those commands. The production target is one supervisor OS
process plus one persistent, fresh-interpreter Agent process for every enabled
Agent. Process separation is required for lifecycle, concurrency, and failure
isolation; it is not presented as a hostile-code security sandbox.

The current durable schema is version 32. Version 32 adds a presentation cursor
to item-based reply candidates and persists command-receipt
`response_fragments_json`. Its migration marks every older candidate presented,
so enabling switch-back presentation can never reinterpret a pre-v32 transcript
as unread output.

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
  role, mode, model, and task snapshots cannot observe half-applied changes.
- Command replies use the same ten-send reply scope, explicit destination, and
  3,000-character fragmentation rules as Agent replies.
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
| `/sh <command>` | Run one bounded supervisor-owned shell command in the workspace. |
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
- `/ask` is requested output even when the destination is not the front Agent.
  Each nonblank completed text item is rendered as `sender: message`, where
  `sender` is the canonical Agent that produced the item.
- `/system` affects future tasks for the exact user/session/Agent. A changed
  role rotates the conversation binding. It cannot grant permissions and is
  never an edit of the administrator-owned Agent Profile.
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

No live object above is shared between Agents. Agent children may share the
configured workspace and Unix user, so simultaneous execute-mode tasks can
still conflict at the filesystem level.

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
- A parent `run` carries one immutable task snapshot for the exact child Agent.
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
  effort, skill, media, and reply target accepted with that task; and
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

### 7.5 Skills

The selected Agent process discovers skills, while the supervisor persists
canonical descriptors and immutable bundle hashes. `$<skill>` acceptance
snapshots the exact skill version/path/hash. Changed or missing bytes fail
closed rather than silently substituting another skill.

## 8. WeChat ingress, typing, and item-based replies

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

Replies are item-based. A stable completed runtime item is reply-eligible when
it is an Agent message containing nonblank text. Deltas, reasoning, tool calls,
status updates, and aggregate terminal text do not create extra replies.

Identity prefers the SDK item ID and falls back to a deterministic ordinal
within the task execution. Replaying the same identity with the same canonical
content is idempotent. Reusing it with different content, foreground class, or
notification snapshot fails closed. This prevents the former
`reply candidate identity conflicts: notify_enabled, foreground` failure from
being caused by mutable route/notification state during replay.

### 8.3 Atomic switch-back acknowledgement

`/agent` selects eligible destination-Agent candidates without marking them.
It renders them under `unseen messages:` and stores their candidate IDs,
complete response text, and item-local `response_fragments_json` specification
in the durable command receipt. Selection alone therefore cannot lose a reply
if command projection fails.

Projecting the command response outbox and changing the selected candidates to
`presentation='presented'` happen in one SQLite transaction. A validation or
projection failure leaves both effects uncommitted. Crash/redelivery reuses the
immutable command receipt, response fragments, presentation IDs, and outbox;
it does not rerun the selector against newer state or mark a different set.

The acknowledgement header and each unseen text item retain separate logical
fragment boundaries. Each boundary is split at 3,000 Python characters. The
resulting command response uses the new `/agent` inbound message's normal
ten-slot scope. Overflow from that newly rendered command response may itself
become `deferred_quota` and later belongs to `/recv`. This does not make an
excluded source candidate eligible: a source candidate that already contains
a `deferred_quota` fragment is never selected in the first place.

### 8.4 Ten-send quota and 3,000-character chunks

Each accepted user message opens one durable reply scope containing at most ten
logical `SendMsg` identities. Command acknowledgements, Agent text items,
media, and retries of an already allocated send share those ten slots.

Each text item is split into fragments of at most 3,000 Python characters.
Fragments retain source item identity and ordinal. At most ten are allocated to
the current scope; overflow is stored FIFO as `deferred_quota` and is delivered
through later `/recv` scopes. A failed or ambiguous send never recycles its
ordinal.

### 8.5 Sender and destination

Every channel send specifies the destination user explicitly rather than
relying on ambient contact state. Durable rows retain the full reply target,
including channel, bot, external user, session, source message/sequence, and
optional context token.

Explicit `/ask` results and Agent-to-Agent correlated answers include the
producing Agent prefix in text: `sender: message`. General foreground replies
retain their normal item text while still carrying the durable producing
Agent identity.

### 8.6 Notifications and delivery

Foreground replies and explicit `/ask` output are push-eligible regardless of
`/notify`. Background output is either pushed when enabled or retained for
`/inbox`. `/inbox` selects unseen items; `/recv` drains only quota-deferred
fragments. Text and media sends use durable outboxes, stable wire IDs, bounded
retry, and explicit unknown-outcome states.

## 9. Agent-to-Agent collaboration

Agent collaboration uses a durable mailbox owned by the supervisor. A source
turn receives a short-lived capability bound to its task and execution. The
bridge lists only authorized peers and persists sends only after revalidating
the active immutable policy.

Mailbox scheduling rules:

- the destination Agent's child executes the mailbox turn;
- task and mailbox work share the same one-invocation Agent slot and FIFO;
- mailbox conversations are destination/request-scoped and never reuse a user
  conversation or session role;
- correlated replies are persisted as Agent messages, not sent directly by the
  child; and
- claim loss cancels the local run and suppresses stale publication.

Different destination Agents can process mailbox work concurrently in their
different processes.

## 10. Persistence, recovery, and deletion

SQLite owns inbound deduplication, commands, Profiles/Modes, routes, roles,
tasks, executions, events, mailboxes, attachments, reply candidates/fragments,
reply slots, and delivery outboxes. IPC is never the durable source of truth.

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
rows. Process-local historical Profile snapshots may remain cached for audit,
but creating another Agent publishes definitions only for that selected Agent;
it must never republish an unrelated retired Profile's stale `enabled` value.
Global startup likewise omits detached tombstoned history while retaining
strict validation for explicitly registered Agents. Recreating the same string
ID starts a new process/lifecycle context.

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
user account, and normally the workspace. Full hostile isolation would require
containers/namespaces, separate credentials, brokered tools, and a delegated
cgroup-v2 or equivalent job boundary. The existing cgroup/job modules remain
optional hardening foundations; production process-per-Agent execution does
not fail closed merely because a delegated cgroup is unavailable.

## 13. Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `CODEX_WECHAT_DB` | user runtime database | Canonical SQLite path; supervisor only. |
| `CODEX_WECHAT_WORKSPACE` | durable user workspace | Workspace shared by Agent children and `/sh`. |
| `CODEX_WECHAT_ATTACHMENTS` | managed attachment root | Durable binary media directory. |
| `CODEX_WECHAT_SKILL_ROOTS` | empty | Trusted skill bundle roots. |
| `CODEX_WECHAT_TURN_TIMEOUT` | runtime default | Per-turn bound passed to each child runtime. |
| `CODEX_WECHAT_MAX_AGENT_PROCESSES` | `16` | Maximum live Agent children and dispatcher breadth. |
| `CODEX_WECHAT_MAX_AGENT_QUEUE` | store default | Per-Agent unfinished invocation limit. |
| `CODEX_WECHAT_MAX_GLOBAL_QUEUE` | store default | Global unfinished invocation limit. |
| `CODEX_WECHAT_MAILBOX_TTL` | store default | Mailbox expiry interval. |

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
  IDs and item-local fragments atomically;
- `/ask` uses `sender: message`, text chunks are at most 3,000 characters, and
  one inbound scope allocates no more than ten sends;
- every public launcher uses the same durable multi-process topology; and
- focused suites, the complete pytest suite, `git diff --check`, and a safe
  `./cow --help` smoke test pass.

The v26-v31 direct child-dispatch tables, authenticated IPC foundation, pidfd
helpers, and job-containment probes remain available for future hardening.
They are not the active scheduler and must not be described as prerequisites
for the implemented process-proxy topology.
