# Implemented Architecture

This document describes the code that `./cow` actually runs as of 2026-08-17.
It is an implemented one-process-per-Agent system: the supervisor owns WeChat,
SQLite, routing, and delivery, while every enabled Agent owns a persistent,
independent OS child process containing its own `CodexRuntime` and SDK client.
An Agent is not a supervisor coroutine or thread: it has a distinct PID,
interpreter, address space, event loop, runtime, and SDK client.

The latest SQLite schema is version 33. The process cutover uses the existing
durable task/mailbox claim path; v32 adds reply-candidate presentation state
and command-receipt `response_fragments_json`, while v33 adds scoped
`session_agent_working_directories`. Neither changes which process owns durable
scheduling.

## 1. Architecture at a glance

| Area | Active implementation |
| --- | --- |
| Public runtime | One `./cow` supervisor process per database and WeChat account |
| Agent execution | One persistent fresh-interpreter child process per enabled Agent |
| Agent state | One child-local asyncio loop, `CodexRuntime`, SDK client, thread cache, and execution slot per Agent |
| Cross-Agent concurrency | Different Agent children execute concurrently |
| Same-Agent concurrency | Task and mailbox work share one serialized Agent slot |
| Durability | Supervisor-owned SQLite v33 with claims, leases, events, workspace preferences, reply presentation/projection, and outboxes |
| Working directories | Per-session/Agent selection inside one configured confinement root; immutable per accepted task |
| Context management | Exact-model provider discovery plus native manual/automatic compaction inside each Agent child |
| Replies | Stable completed text items, ten sends per inbound scope, 3,000-character fragments |
| Collaboration | Durable mailbox plus a supervisor-owned capability bridge |
| Failure isolation | One child can be interrupted, killed, restarted, or deleted without replacing peer children |
| Diagnostics | Owner-only rotating supervisor log with sanitized structured task lifecycle and typed provider failures |
| Process security | Lifecycle/failure isolation, not a hostile same-UID sandbox |

The production launcher contains no live `CodexRuntime`. It creates
`ProcessAgentRuntime` proxies and enables a fail-closed topology gate: startup
fails if an enabled Agent is in-process, shares a child PID, reports an invalid
generation, or does not become ready.

## 2. Active process topology

```mermaid
flowchart LR
    WX[WeChat iLink service]
    DB[(SQLite v33)]
    FS[(Managed attachments<br/>and workspace confinement root)]

    subgraph SUP[./cow supervisor OS process]
        MON[Monitor main thread<br/>+ contact worker pool]

        subgraph LOOP[AsyncLoopThread: one supervisor asyncio loop]
            GW[WeChatGateway<br/>normalize + commands]
            TM[TaskManager<br/>routes + immutable snapshots]
            TW[TaskWorker coroutines]
            MB[Mailbox supervisor<br/>one coroutine per Agent]
            REG[AgentRegistry]
            PA[ProcessAgentRuntime proxies]
            BR[AgentBridgeServer]
            OD[Text/media delivery workers]
        end

        SQL[SQLiteStore executor thread]
        PUB[Managed image publisher]
    end

    subgraph A[Agent process: codex]
        ALOOP[private asyncio loop]
        AR[private CodexRuntime]
        ASDK[private SDK/app-server client]
        ALOOP --> AR --> ASDK
    end

    subgraph B[Agent process: planner]
        BLOOP[private asyncio loop]
        BRUN[private CodexRuntime]
        BSDK[private SDK/app-server client]
        BLOOP --> BRUN --> BSDK
    end

    subgraph N[Agent process: each other enabled Agent]
        NLOOP[private asyncio loop]
        NRUN[private CodexRuntime + SDK client]
        NLOOP --> NRUN
    end

    WX <-->|poll, typing, SendMsg, CDN| MON
    MON --> GW --> TM
    TM --> TW
    TM <--> SQL <--> DB
    TW <--> SQL
    MB <--> SQL
    OD <--> SQL
    REG --> PA
    TW --> PA
    MB --> PA
    PA <-->|private bounded JSON IPC| A
    PA <-->|private bounded JSON IPC| B
    PA <-->|private bounded JSON IPC| N
    A -. task capability .-> BR
    B -. task capability .-> BR
    N -. task capability .-> BR
    BR --> TM
    A -. artifact proposal .-> PUB
    B -. artifact proposal .-> PUB
    N -. artifact proposal .-> PUB
    PUB <--> FS
    PUB <--> SQL
    OD <--> FS
```

An Agent process is a dedicated leader process. The Codex SDK may create an
app-server or tool subprocess beneath that leader. Those descendants belong to
the Agent's process group during normal operation, but the architectural unit
exposed through `/agents` is the dedicated leader PID and generation.

## 3. Ownership boundaries

| Resource or responsibility | Supervisor | One Agent child |
| --- | --- | --- |
| WeChat credentials/client, polling, typing, CDN, `SendMsg` | owns | absent |
| SQLite connection, migrations, claims, leases, reply quotas | owns | absent |
| Routes, Profiles, Modes, roles, model preferences, skill snapshots | resolves/persists | consumes immutable task snapshot |
| Working-directory preferences and execution snapshots | resolves, persists, captures | validates and consumes task-local snapshot |
| Task/mailbox scheduling | owns | executes only assigned work for its Agent |
| `CodexRuntime` and SDK client | no live production instance | owns exactly one |
| Provider thread bindings and active turns | no | owns privately |
| Provider credentials and context resolver/cache | absent | owns privately |
| Event persistence and user reply projection | owns | proposes events and waits for ACK |
| Generated-image publication | validates and persists | proposes task-correlated artifact |
| Agent collaboration policy/mailbox | validates and persists | calls bridge with task capability |
| Workspace confinement root | configures and validates | accesses the accepted directory according to task policy |

The children receive no store, database connection, WeChat client, sender, or
reply quota allocator. The fresh interpreter and a strict import check keep
SQLite/store/channel/WeChat modules out of the child runtime graph.

## 4. Threads, loops, and processes

The supervisor uses several execution contexts, none of which substitutes for
an Agent child:

| Context | Purpose |
| --- | --- |
| Supervisor main thread | Blocking iLink long poll |
| Monitor contact pool | Concurrent contacts with per-contact ordering |
| `AsyncLoopThread` | Gateway, manager, registry, process proxies, task/mailbox and delivery coroutines |
| SQLite executor thread | Serializes one SQLite connection without blocking the loop |
| Bounded helper threads/processes | Blocking channel sends, media work, `/sh`, and child pipe writes |
| Agent child process | One Agent's runtime/SDK state and one execution slot |

Monitor threads enter the supervisor loop only through thread-safe coroutine
submission. Every process proxy is bound to that one supervisor loop. Each
child creates a separate loop with `asyncio.run`; no loop, lock, SDK client, or
SQLite handle is inherited from the supervisor.

## 5. Startup and topology proof

The public launcher is `cow`. `./cow` and `./cow run` synchronize dependencies
and execute `src/codex_wechat_bot.py`; the deprecated Python `--legacy` flag
reaches the same durable topology.

Startup proceeds in this order:

1. Resolve credentials, database, account, workspace, attachment, bridge, skill,
   and capacity configuration.
2. Acquire exclusive advisory locks for the database and `(channel, bot_id)`.
   Another live supervisor fails before SQLite opens.
3. Start the supervisor asyncio loop, open SQLite, apply consecutive migrations
   through v33, activate a new supervisor epoch, and reconcile durable state.
4. Hydrate ready attachment metadata and create the parent-owned image publisher.
5. Construct one unstarted `ProcessAgentRuntime` for `codex`; no SDK runtime is
   constructed in the supervisor.
6. Register immutable Codex Profile versions, build `TaskManager` with
   `require_process_isolation=True`, and start the owner-only Agent bridge.
7. Manager startup restores durable dynamic-Agent registrations. Each named
   registration calls the template's `for_agent(id)`, producing an independent
   proxy with shared configuration and process-capacity accounting.
8. `AgentRegistry.start()` starts every enabled proxy. Each child must publish
   a positive PID, positive generation, and `ready` health.
9. The production gate verifies every PID is distinct from the supervisor and
   every other enabled Agent before task/mailbox workers start.
10. Start delivery/media/mailbox workers, restore the channel cursor, and begin
    polling WeChat.

### Fresh child bootstrap

`multiprocessing` uses the `spawn` start method, never `fork`. Normal Python
spawn re-executes the supervisor's `__main__` module before unpickling its
target; doing that to `codex_wechat_bot.py` would import SQLite and WeChat in
every child. The proxy therefore serializes spawn preparation under a lock and
identifies the dependency-free top-level `process_agent` module as the child
main. The target then starts from that clean module graph.

Before importing `CodexRuntime`, the child installs a narrow synthetic
`src.runtime` package path so the adapter can load `media`, `roles`, `modes`,
and `policy` helpers without executing the heavyweight runtime initializer.
Startup asserts that SQLite/store/worker/channel/WeChat modules are absent.

## 6. Durable ingress and execution sequence

```mermaid
sequenceDiagram
    participant W as WeChat
    participant G as Supervisor gateway
    participant S as SQLiteStore
    participant K as TaskWorker
    participant P as Agent process proxy
    participant A as Owning Agent child
    participant C as Child CodexRuntime/SDK
    participant O as Delivery worker

    W->>G: finished supported message
    G->>W: typing state (best effort, no reply slot)
    G->>S: atomic inbound + reply scope + task/command receipt
    S-->>G: accepted or idempotent replay

    K->>S: claim next runnable invocation + lease
    S-->>K: immutable AgentTask + execution_workspace snapshot
    K->>P: run(task, durable emit callback)
    P->>A: RUN over private IPC
    A->>A: validate workspace snapshot
    A->>C: initialize SDK and validate again before native turn

    loop stable completed runtime items
        C-->>A: AgentEvent
        A->>P: EVENT(run, task, item identity)
        P->>K: emit(event)
        K->>S: append event + project candidate/fragments/slots
        S-->>K: commit
        K-->>P: callback returned
        P-->>A: EVENT_ACK
    end

    C-->>A: terminal AgentResult
    A-->>P: RESULT
    P-->>K: AgentResult value
    K->>S: fenced terminal completion
    O->>S: claim allocated outbox row
    S-->>O: stable wire identity + explicit sender target
    O->>W: SendMsg
    O->>S: sent, retry wait, permanent failure, or unknown outcome
```

Commands remain in the supervisor and never enter this RUN path. `/sh` is a
bounded supervisor-owned subprocess, not Agent work, but it runs in the front
Agent's working-directory snapshot captured when the command was accepted.

### Working-directory selection and execution snapshots

`CODEX_WECHAT_WORKSPACE` is a canonical confinement root, not merely the
default cwd. Schema v33 stores a root-relative selection plus target
device/inode identity for each `(channel, bot, user, session, Agent)`. `/cd`
with no path reads that selection; `/cd <path>` changes only that scope.
Quoted paths support spaces, and relative paths resolve from the current
selection. The directory must already exist, be enterable, and resolve inside
the root; canonical and symlink escapes fail closed. The mutation and command
receipt complete atomically and persist across restart and switch-back.

Acceptance freezes `execution_workspace` with canonical root/path values and
root/target device and inode identities. Queued and running tasks retain it,
so later `/cd` changes only future work. The destination Agent child validates
the snapshot before SDK or network work and again before the native turn. A
missing, moved, replaced, or identity-changed root/target fails closed. The
runtime passes task-local cwd values and never calls process-global
`os.chdir()` or restarts an Agent for `/cd`.

If the selected target becomes stale, task and cwd-consuming command paths
fail closed. The supervisor can still persist cwd-independent controls without
a workspace snapshot, allowing an absolute in-root `/cd` to repair the scope.

For backward compatibility, a legacy durable task without an
`execution_workspace` continues in that Agent's configured default working
directory. The device/inode-pinned fail-closed guarantee therefore applies to
snapshotted and newly accepted work.

Validation is repeated immediately before an SDK/native turn and before the
supervisor launches `/sh`. Those pathname-based APIs do not provide an open
directory-descriptor (`dirfd`) contract, so a residual TOCTOU window remains
between final validation and path consumption.

Explicit `/ask` tasks resolve the destination Agent's selection for the
originating user/session, not the front/source Agent's selection. Agent-to-Agent
mailbox turns use the same destination-scoped rule. Dynamic Agent retirement
clears only that Agent's mutable directory preferences; immutable task/history
rows and their accepted snapshots remain.

## 7. Scheduling and real cross-Agent concurrency

The active SQLite rows use `dispatch_backend=compatibility`. That name now
means the retained durable claim/lease adapter; execution behind the claimed
row is process-isolated and never falls back to a supervisor `CodexRuntime`.

SQLite allocates a monotonically increasing `ready_sequence` for each Agent
incarnation. Task and mailbox claim paths share the same SQL guard:

- at most one `dispatching`, `running`, or `cancel_requested` invocation exists
  for an Agent incarnation;
- runnable task and mailbox work observe one per-Agent FIFO;
- delayed retries and not-yet-ready attachments do not block later runnable
  work; and
- different Agents are independently eligible.

The proxy and child each add a second one-slot guard. Thus task work and mailbox
work for one Agent cannot overlap even with a lightweight custom store, while
different proxies send to different child PIDs and execute simultaneously.

`CODEX_WECHAT_MAX_AGENT_PROCESSES` defaults to 16 and supplies both the shared
child-process capacity and production task-dispatch breadth. The old
`CODEX_WECHAT_WORKERS` value is ignored with a warning so a stale value of `1`
cannot globally serialize the process topology.

The concurrency tests use filesystem entry/release barriers in separate child
PIDs. Both Agents must enter before either release file exists, proving real
overlap rather than two queued supervisor coroutines.

## 8. Agent lifecycle, routing, and history

A route is scoped by `(channel, bot_id, external_user_id, session_id)`. The
canonical conversation adds `agent_id`, and provider bindings additionally pin
mode, Profile version, policy version, and role version/hash.

### Create and switch

`/agent <id> [profile]` canonicalizes the ID, validates an optional Codex
config filename stem, restores or creates a dynamic Profile,
constructs a new process proxy, persists definitions, starts the child, and
proves its live unique PID before committing the new route. A failure leaves
the old route unchanged.

The immutable Profile stores only `codex_config_profile`, never TOML contents
or provider credentials. The child resolves the base configuration plus
`$CODEX_HOME/<profile>.config.toml`, passes the selected model/provider subset
over its private app-server stdio channel on both thread start and resume, and
merges the provider's bounded `/models` IDs with Codex `model/list`. Unknown
provider models receive no invented effort or modality capabilities. A
one-argument switch preserves an existing binding; an explicit conflicting
binding is rejected before process or route mutation.

Switching changes future ingress and never reads or sends provider transcripts,
task/event history, conversation history, mailbox contents, or thread caches.
Work that was already queued/running retains its original Agent and
conversation.

After the route commit, `/agent` may select destination-Agent candidates as
unseen messages. The selector is scoped to the exact channel, bot, user,
session, and destination Agent and requires `presentation='unseen'`, immutable
background classification (`foreground=0`), nonblank completed-item text, at
least one fragment, and every fragment still in `inbox_only`.

This is unread-item presentation, not history replay. The selector excludes
all pre-v32 candidates, foreground or already presented items, attachment-only
or fragmentless items, and anything with allocated, sent, failed/ambiguous,
mixed-state, or `deferred_quota` fragments. `/recv` remains the exclusive owner
of quota-deferred fragments. Attachment-only candidates remain unseen rather
than being rendered as empty text by `/agent`.

### Delete and recreate

`/delagent` is durable retirement. The store prevents new work and redirects
affected routes. The manager then stops and reaps the exact dynamic child while
its proxy is still registered. Only proven cleanup permits unregistering the
last handle. Immutable Profiles, tasks, events, reply rows, and conversations
remain. Only that Agent's mutable working-directory preferences are cleared;
accepted task snapshots remain immutable history.

The process-local registry may retain those historical Profile values, including
their pre-retirement `enabled` snapshot. Dynamic definition publication is
therefore scoped to the Agent being created or reactivated; creating one Agent
cannot republish an unrelated tombstoned Agent and conflict with SQLite's
disabled lifecycle state. Startup omits only detached tombstoned history and
still validates explicitly registered Profiles strictly.

Explicit recreation of the same string ID builds a fresh proxy and child PID.
Its process generation starts in the new process context; it never adopts the
old runtime/client/thread cache.

### Listing

`/agents` enriches process-backed descriptors with `pid`, `generation`,
`health`, and `process_isolated=true`. The rendered command marks the current
Agent and shows this process evidence. Compatibility embedders retain their old
descriptor shape, but the production topology gate rejects them.

## 9. Private IPC and child controls

The active process boundary is `src/runtime/process_agent.py`. It uses one
private duplex pipe per Agent and bounded canonical JSON envelopes containing:

```text
protocol version
message kind and ID
optional reply correlation ID
Agent ID and process generation
JSON payload
```

Supported flows include ready, run, event/ack, result, interrupt/ack, stop,
ping, model/skill discovery, skill resolution, session reset/compaction,
generated-image publication, and bridge-capability use.

Wire conversion accepts only JSON-safe scalars, finite numbers, string-keyed
mappings, bounded sequences, dates, paths, enums, and explicit domain
serializers. It validates nesting and a maximum encoded message size. Stale
generation, cross-Agent, mis-correlated, duplicate-conflicting, malformed, or
oversized traffic kills the generation rather than guessing.

The child receive loop remains active during a run, so priority interrupt and
stop controls reach the owning runtime. An early interrupt is latched even
before the backend registers its native turn. Interrupting one proxy cannot
touch a peer process.

Model and effort setters are intentionally not child controls. The supervisor
persists those preferences and places them in every future immutable task.
Model/skill listing and conversation reset do require child RPC because they
consult or mutate that Agent's private SDK/runtime state.

### Context discovery and compaction

`/compact` remains a supervisor command. Under the session control lock,
`TaskManager` resolves the captured/current Agent plus its exact conversation,
mode, Profile/policy versions, role, model, and workspace. It rejects active
work, a mismatched conversation, or a missing durable thread binding before
calling the selected process proxy. The correlated control reaches only that
Agent child, which invokes the native SDK compaction on the same provider
thread. The binding and durable task/event history remain unchanged.

Each child also owns a `ProviderModelContextResolver`. It reads the effective
Codex configuration and queries the configured provider's exact-model detail
and list endpoints. Valid provider context extensions are authoritative. If a
catalog omits them—as the OpenAI-compatible catalog contract permits—the
resolver uses `model_context_window` only when effective configuration selects
that exact model. It never derives or invents a limit from the model name.

When a window is resolved, thread start/resume receive native context and
automatic-compaction settings with a total-token threshold at 80% of the
window, capped by any lower valid advertised/configured threshold. The TTL
cache is isolated per child and keyed by provider/base URL/exact model and
fallback. Provider credentials are consumed only in that child; neither
credentials nor secret-bearing provider responses cross IPC or enter logs.

## 10. Parent-owned callbacks

### Event acknowledgement

The child sends each event and waits. The proxy validates run, task, Agent, and
generation identity, reconstructs `AgentEvent`, and awaits the supervisor emit
callback. In production that callback persists and projects the event. Only
then does the proxy return `EVENT_ACK`. A rejected callback returns an error to
the child and cannot be mistaken for a durable commit.

### Generated images

The child cannot own SQLite/attachment publication. It sends an artifact
proposal containing bounded output metadata. The proxy correlates task ID,
execution ID, and Agent ID to the original parent-side `AgentTask`, then calls
`ManagedImageOutputPublisher` with that exact object and its complete
`ReplyTarget`. Only the managed attachment ID returns to the child.

### Agent bridge

The supervisor issues an unguessable capability bound to task, execution, and
source Agent. The child prepends bridge discovery only when immutable policy
allows Agent messaging and the task is not an internal mailbox turn. The local
CLI reaches the owner-only Unix socket; the bridge revalidates the live task
and policy before listing peers or persisting a send.

## 11. Item-based WeChat replies

The stable reply boundary is a completed runtime Agent-message item containing
nonblank text. Deltas, reasoning, tools, status items, and aggregate terminal
content do not create duplicate replies. Identity uses the SDK item ID when
available and otherwise a deterministic ordinal within the task execution.

Each inbound user message creates one durable reply scope with at most ten
logical `SendMsg` identities. Text is split into fragments of at most 3,000
characters. Excess fragments are retained FIFO as `deferred_quota`; `/recv`
allocates the next batch under a later inbound scope. Allocated ordinals and
wire IDs are never recycled after failure or ambiguity.

Candidate projection snapshots rendered content, foreground/background class,
notification eligibility, sender Agent, and destination. Exact event replay
reuses that snapshot even if `/agent` or `/notify` later changes. Conflicting
reuse fails closed, preventing mutable route state from causing the former
`reply candidate identity conflicts: notify_enabled, foreground` error.

Schema v32 adds `reply_candidates.presentation`, `presented_at`, and durable
command-receipt `response_fragments_json`. Migration marks every pre-v32
candidate presented, because older databases have no sound way to distinguish
unread output from history. New eligible background candidates begin unseen.

Switch-back delivery uses this durable sequence:

```text
/agent route commit
  -> select exact-scope candidates with switch_only=True, present=False
  -> complete immutable command receipt with candidate IDs and fragments
  -> atomically project command outbox and mark those candidates presented
  -> SendMsg from the durable outbox
```

Selection does not mark a candidate. If command projection fails, the candidate
remains unseen and no partial command outbox survives. Crash/redelivery reuses
the receipt's original response, presentation IDs, item-local fragments, and
outbox instead of selecting again. The acknowledgement header and every unseen
text item keep separate logical fragment boundaries across replay.

The switch acknowledgement uses the `/agent` inbound message's ordinary
ten-slot scope and 3,000-character fragmentation. Its own overflow may become
`deferred_quota` for `/recv`; that newly deferred command output is distinct
from source candidates already containing `deferred_quota`, which are never
eligible for switch-back selection.

Every actual send uses the explicit stored destination user. `/ask` and
correlated Agent answers render completed items as `sender: message`, with the
canonical producing Agent as sender. Foreground ordinary replies retain their
item text while preserving sender identity in durable rows.

Typing is sent best-effort immediately after a supported message arrives. It
is not a `SendMsg`, consumes no quota slot, and does not affect durable
acceptance if it fails.

## 12. Profiles, roles, modes, models, and skills

Profiles and Modes are immutable `(id, version)` snapshots. Re-registering
canonical-identical metadata is idempotent; a real payload conflict fails
closed. Compatibility normalization prevents harmless legacy representation
differences from raising `mode version metadata conflicts` or
`profile version metadata conflicts` during route/process restoration.

All built-in modes allow network and command execution. `chat`, `plan`, and
`review` retain no-edit policy/instructions; `execute` additionally permits
workspace writes and requires an authorization snapshot. `/plan` is not a
command—`/mode plan` selects that mode.

`/system` stores a user/session/Agent-scoped role snapshot. A changed role
rotates the durable conversation binding; queued/running work remains pinned.
Role text changes instructions only and cannot grant permissions. Mailbox turns
never inherit a user role.

Model discovery runs in the selected Agent child and follows SDK pagination.
Model/effort preferences are durable future-task fields. Effort names are
capability-driven, so `ultra` appears and is accepted only for models that
advertise it.

Context-window discovery is separate because catalog context fields are
provider extensions. Exact-model validated metadata, or an exact effective
configuration fallback, drives native 80% automatic compaction. `/compact`
uses the current binding in the owning child and preserves its thread/history.

Skill discovery also runs in the selected child. The supervisor normalizes and
persists descriptors; task ingress pins the exact trusted bundle path/version/
hash and fails if those bytes later conflict.

## 13. Persistence and recovery

SQLite owns all correctness-critical state: inbound deduplication, command
receipts, Profiles/Modes, Agent lifecycle, routes, roles, session/Agent
working-directory preferences, tasks/executions and their immutable workspace
snapshots, events, mailbox invocations, thread bindings, attachments, reply
candidates/fragments/scopes/slots, and delivery outboxes. IPC wakeups and child
memory are never durable truth.

Task claims carry worker and claim-token leases. Event append, thread binding,
terminal completion, admission release, and mailbox transitions are conditional
on that ownership. Claim loss prevents a new external turn and suppresses a
late result.

If a child exits after RUN was sent, `ProcessAgentLostError` carries
`execution_uncertain=true`. `TaskWorker` maps it to `orphaned`, never `failed`,
because the SDK or a tool may already have caused an external effect. `/retry`
is the explicit recovery action.

An idle lost proxy restarts with a new PID and higher generation when started
again or when the next invocation reaches it. Peer proxies keep their original
PIDs/generations and continue running.

The repository also contains v26-v31 direct-child-dispatch tables and APIs,
authenticated IPC/lifetime foundations, and cgroup job probes. They are not the
active production scheduler. The active, tested cutover is the process proxy
behind the existing durable claim path.

## 14. Shutdown and cleanup ownership

Normal shutdown order is:

1. stop Monitor ingress;
2. stop/drain delivery and mailbox supervisor coroutines;
3. stop TaskManager workers and every Agent process while the bridge is live;
4. close the Agent bridge;
5. close SQLite and its executor;
6. close the supervisor event loop and WeChat client; and
7. release ownership locks.

Child stop asks the local runtime to interrupt active work, waits boundedly,
closes the SDK client, sends `stopped`, exits, and is reaped. Timeout escalates
through process-group `SIGTERM` and `SIGKILL`. A cleanup serialization lock
ensures only one coroutine joins/reaps a given process handle.

The proxy does not clear its child handle, process-group identity, registry
marker, or shared capacity reservation until leader exit, reaping, and current
process-group emptiness checks succeed. Cleanup failure remains retryable; the
manager retains supervisor ownership rather than pretending shutdown completed.

## 15. Security and remaining hardening boundary

The process architecture materially isolates runtime lifecycle and failure:

- one Agent cannot share or corrupt another Agent's in-memory thread/cache;
- one stuck client can be interrupted/killed/restarted independently;
- cross-Agent work runs in different PIDs; and
- WeChat/SQLite survive a single child crash.

It is not a hostile-code confidentiality boundary. Children normally share a
Unix UID and configured workspace root, and current modes allow network/commands.
Every selected cwd is canonically confined to that root, but same-UID processes
can still reach overlapping files. The leader
arms Linux parent-death `SIGKILL`, and normal/lost-child cleanup kills its
process group. Arbitrary descendants that deliberately escape that group are
not guaranteed to die if the supervisor itself receives `SIGKILL`.

Non-escapable descendant cleanup requires a real delegated cgroup-v2,
container, namespace, or brokered-tool boundary. `job_containment.py` currently
provides identity/probe foundations only. This optional hardening gap does not
change the implemented one-dedicated-process-per-Agent topology and is not a
startup prerequisite.

## 16. Active configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_WECHAT_DB` | user runtime DB | Supervisor SQLite path |
| `CODEX_WECHAT_WORKSPACE` | durable workspace | Canonical confinement root for every Agent and `/sh` working directory |
| `CODEX_WECHAT_ATTACHMENTS` | managed attachment directory | Binary media root |
| `CODEX_WECHAT_AGENT_SOCKET` | beside DB | Owner-only collaboration socket |
| `CODEX_WECHAT_SKILL_ROOTS` | empty | Trusted skill roots |
| `CODEX_WECHAT_TURN_TIMEOUT` | runtime default | Child Codex turn timeout |
| `CODEX_WECHAT_MAX_AGENT_PROCESSES` | `16` | Child capacity and dispatcher breadth |
| `CODEX_WECHAT_MAX_AGENT_QUEUE` | store default | Per-Agent unfinished work bound |
| `CODEX_WECHAT_MAX_GLOBAL_QUEUE` | store default | Global unfinished work bound |
| `CODEX_WECHAT_MAILBOX_TTL` | store default | Mailbox expiry |

`CODEX_WECHAT_WORKERS` is deprecated and ignored.

## 17. Implementation map

| File | Responsibility |
| --- | --- |
| `cow` | Public launcher |
| `src/codex_wechat_bot.py` | Supervisor construction, topology gate, lifecycle order |
| `src/runtime/process_agent.py` | Active process proxy, clean spawn bootstrap, child loop, IPC, cleanup |
| `src/runtime/manager.py` | Routing, dynamic lifecycle, workspace resolution, immutable task snapshots, production isolation validation |
| `src/runtime/registry.py` | Runtime ownership and retryable start/stop bookkeeping |
| `src/runtime/worker.py` | Task/mailbox execution, event commit callbacks, uncertainty mapping |
| `src/runtime/dispatcher.py` | Durable claim wakeups and compatibility serialization |
| `src/runtime/sqlite_store.py` | SQLite v35 schema, immutable Agent config-profile bindings, scoped working-directory preferences, claims, events, reply presentation/projection, recovery |
| `src/channels/wechat.py` | Command registry, gateway, item projection, 3,000-character replies |
| `src/runtime/agent_bridge.py` | Task capability and local collaboration server |
| `src/runtime/media.py` | Managed attachments and parent-owned image publication |
| `src/agents/workspace.py` | Canonical workspace snapshots, confinement, and device/inode revalidation |
| `src/agents/model_context.py` | Child-local exact-provider/model context discovery and auto-compaction settings |
| `src/agents/config_profile.py` | Safe named Codex config loading and base-layer merge inside Agent children |
| `src/agents/codex_runtime.py` | Child-local Codex SDK adapter |
| `src/runtime/agent_process.py` | Disconnected authenticated-process foundation, not active executor |
| `src/runtime/agent_child_runtime.py` | Disconnected direct-dispatch foundation, not active executor |
| `src/runtime/job_containment.py` | Optional containment identity/probe foundation |

## 18. Executable architecture evidence

The process tests do not rely only on mocks. They prove:

- supervisor PID, Agent A PID, and Agent B PID are all different;
- two Agent children enter a filesystem barrier before either is released;
- one Agent serializes two runs;
- interrupts are scoped to the owning child;
- SIGKILL of one busy child yields an uncertain/orphanable error while its peer
  continues and the killed Agent restarts with a fresh PID/generation;
- TaskManager routes a dynamic Agent task to that Agent's child;
- `/delagent` reaps the exact PID and recreation gets a fresh process;
- the production launcher import graph still creates a clean child without
  importing SQLite/channel modules;
- generated-image relay uses the original supervisor task and ReplyTarget;
- event acknowledgements follow callback completion;
- `/compact` targets only the exact selected Agent binding, rejects active or
  unbound contexts, and executes in that Agent child;
- exact provider/model context resolution, 80% native thresholds, fallback,
  cache isolation, and secret redaction are covered without live provider calls;
- immediate interrupt races terminate instead of hanging;
- capacity transfers only after child cleanup;
- failed cleanup retains process/group/budget ownership; and
- `/cd` scope isolation and persistence, path quoting/relative resolution,
  root containment, symlink rejection, and atomic command receipt are tested;
- accepted workspace snapshots survive later `/cd`, `/sh` uses its captured
  front-Agent snapshot, and `/ask`/mailbox work selects the destination Agent's
  directory;
- Agent-side double validation rejects missing, moved, or replaced roots and
  targets without a process-global cwd change; and
- production rejects in-process runtimes and shared Agent PIDs.

The final release gate is the complete pytest suite, whitespace/diff checks,
and a safe `./cow --help` launcher smoke test.
