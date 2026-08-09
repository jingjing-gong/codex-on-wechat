# Multi-Agent SQLite + Asyncio Plan

## 1. Goal

Build a multi-agent system that can be managed through an IM channel such as WeChat.
The first implementation will use one WeChat bot, SQLite, and one asyncio event loop.
It will not introduce RabbitMQ, Redis, PostgreSQL, or Feishu yet.

The first phase must support:

- Multiple Agent runtimes
- Switching the front Agent with `/agent <id>`
- Background execution after switching to another Agent
- Agent status and interruption
- Agent-to-Agent requests and responses
- Separate user notifications and Agent mailboxes
- Agent-specific system prompts, capabilities, responsibilities, and collaboration ACLs
- Image, video, and audio inputs
- Voice transcription confirmation before an Agent uses the transcript
- SQLite persistence and restart state recovery

The current WeChat transport remains responsible for WeChat protocol details. The task and Agent layers must not depend directly on WeChat APIs.

## 2. Architecture

```text
WeChat Gateway
    -> Unified Message Model
    -> Command Router
    -> Task Manager
    -> Agent Registry
        -> Codex Runtime
        -> Future Agent Runtimes
    -> SQLite Store
        -> User Outbox
        -> Agent Mailbox
    -> WeChat Delivery Worker
```

Core principles:

- SQLite stores facts and durable state.
- asyncio provides real-time task execution.
- AgentRegistry routes tasks to runtimes.
- user_outbox contains only user-facing delivery records.
- agent_mailbox contains only Agent-to-Agent delivery records.
- System prompts describe Agent behavior, while PolicyEngine enforces permissions.

## 3. Identity Model

The system must keep these identities separate:

```text
channel              wechat / feishu / future channels
bot_id               the specific channel bot
external_user_id     the user's ID in that channel
session_id           the user's logical session alias
agent_id              codex / planner / researcher / ...
conversation_id      user + agent + session context
task_id              one concrete execution
```

Even in the first WeChat-only version, use a channel-aware key:

```python
@dataclass(frozen=True)
class ConversationKey:
    channel: str
    bot_id: str
    external_user_id: str
    session_id: str
```

The first implementation uses:

```text
channel = "wechat"
bot_id = the current WeChat bot ID
external_user_id = msg.from_user_id
```

This keeps the storage model ready for future Feishu support.

## 4. Agent Profiles

Every Agent is defined by a persistent profile:

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

Example profile:

```python
AgentProfile(
    agent_id="codex",
    display_name="Codex",
    summary="代码实现、测试和本地工程操作",
    system_prompt="...",
    responsibilities=("implementation", "testing", "local execution"),
    constraints=("do not deploy production changes",),
    capabilities=frozenset({"text", "image_input", "local_file_access"}),
    allowed_peers=frozenset({"planner", "researcher", "reviewer"}),
    denied_peers=frozenset({"deployment"}),
    allowed_request_types=frozenset({"implementation", "bug_fix", "code_review"}),
)
```

The system prompt is not a security boundary. Before an Agent message or child task is created, `PolicyEngine` must enforce:

- Sender and recipient are enabled
- Recipient is allowed by `allowed_peers`
- Recipient is not in `denied_peers`
- The request type is allowed
- Task depth and child count are within limits
- Attachments are accessible to the recipient

Default policy is deny: an Agent not explicitly listed in `allowed_peers` cannot be contacted.
Agents cannot modify their own prompt, profile, capabilities, or ACLs.

Prompt construction order:

```text
Platform safety rules
-> Agent system prompt
-> Responsibilities and constraints
-> Available tools and media capabilities
-> Public descriptors of allowed peer Agents
-> Current task
-> Media context and attachments
-> Confirmed transcripts and tool results
```

Only public peer descriptors are exposed to an Agent. Other Agents' complete system prompts are never shared.

## 5. Agent Runtime Boundary

Add a runtime interface independent of WeChat:

```python
class AgentRuntime(Protocol):
    agent_id: str
    display_name: str

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def run(
        self,
        task: AgentTask,
        emit: Callable[[AgentEvent], Awaitable[None]],
    ) -> None: ...
    async def interrupt(self, task_id: str) -> bool: ...
```

Keep the current `CodexAgent` as the SDK adapter. Add a `CodexRuntime` wrapper that converts `AgentTask` inputs into Codex SDK inputs and converts SDK output into `AgentEvent` objects.

The first registry contains only `CodexRuntime`, but the interface must allow future planner, researcher, reviewer, and other runtimes.

## 6. SQLite Schema

Database path:

```text
~/.codex-on-wechat/runtime.sqlite3
```

### agents

Stores Agent profiles and versions:

```sql
CREATE TABLE agents (
    agent_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    summary TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    responsibilities_json TEXT NOT NULL,
    constraints_json TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    allowed_peers_json TEXT NOT NULL,
    denied_peers_json TEXT NOT NULL,
    allowed_request_types_json TEXT NOT NULL,
    max_child_depth INTEGER NOT NULL DEFAULT 3,
    max_children_per_task INTEGER NOT NULL DEFAULT 20,
    enabled INTEGER NOT NULL DEFAULT 1,
    profile_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

### routes

Stores the current front Agent for a user session:

```sql
CREATE TABLE routes (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    active_agent_id TEXT NOT NULL,
    notify_level INTEGER NOT NULL DEFAULT 2,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id)
);
```

Switching Agent updates only this table. It does not stop existing tasks or consume Agent mailboxes.

### conversations

Stores one context per user, Agent, and session:

```sql
CREATE TABLE conversations (
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_thread_id TEXT,
    summary TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    reasoning_effort TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, bot_id, external_user_id, session_id, agent_id)
);
```

### tasks

```sql
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    conversation_key TEXT NOT NULL,
    prompt TEXT NOT NULL,
    status TEXT NOT NULL,
    parent_task_id TEXT,
    root_task_id TEXT,
    depth INTEGER NOT NULL DEFAULT 0,
    agent_profile_version INTEGER NOT NULL,
    reply_target_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error TEXT
);
```

Statuses:

```text
queued
running
completed
failed
interrupted
cancelled
failed_needs_retry
```

### attachments and task_inputs

Do not store media bytes in SQLite. Store references and metadata:

```sql
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    task_id TEXT,
    kind TEXT NOT NULL,
    local_path TEXT NOT NULL,
    mime_type TEXT,
    size INTEGER,
    duration_ms INTEGER,
    width INTEGER,
    height INTEGER,
    checksum TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE task_inputs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attachment_id TEXT,
    text TEXT,
    confirmed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
```

### messages, user_outbox, and agent_mailbox

All messages are recorded in `messages`, but delivery targets are separated:

```sql
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    source_agent_id TEXT,
    destination_type TEXT NOT NULL,
    destination_agent_id TEXT,
    task_id TEXT,
    parent_task_id TEXT,
    correlation_id TEXT,
    in_reply_to TEXT,
    request_type TEXT,
    event_type TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    user_visible INTEGER NOT NULL DEFAULT 0,
    content TEXT NOT NULL,
    attachments_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);

CREATE TABLE user_outbox (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    task_id TEXT,
    source_agent_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    context_token TEXT,
    priority INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    text TEXT NOT NULL,
    attachments_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    read_at TEXT
);

CREATE TABLE agent_mailbox (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    task_id TEXT,
    source_agent_id TEXT NOT NULL,
    destination_agent_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    in_reply_to TEXT,
    request_type TEXT NOT NULL,
    content TEXT NOT NULL,
    attachments_json TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    processed_at TEXT
);
```

`agent_mailbox` never calls a channel sender. Agent responses must use `correlation_id` and `in_reply_to` to route back to the original requesting Agent.

## 7. Priorities and Notification Policy

Use four priority levels:

```text
0 silent
1 normal
2 notify
3 attention
```

Rules:

- `silent`: internal event; not shown by default.
- `normal`: shown when switching Agent or using `/inbox`.
- `notify`: actively delivered only when the Agent's notify policy allows it.
- `attention`: user intervention, approval, authentication, or repeated failure; saved even when silent.

`/notify` affects only user delivery:

```text
/notify off
    No automatic user delivery.
    Agent-to-Agent messages continue normally.
    User notifications remain in user_outbox.

/notify on
    notify and attention events may be automatically delivered.
```

An Agent message does not become a user notification merely because it has high priority. It must be explicitly `user_visible` or be a defined user-facing event.

## 8. Media Inputs

Use a channel-independent attachment model:

```python
@dataclass
class MediaAttachment:
    kind: Literal["image", "video", "audio"]
    local_path: str | None
    mime_type: str | None
    size: int | None
    duration_ms: int | None
    width: int | None
    height: int | None
    transcript: str | None
    metadata: dict[str, Any]
```

All media must produce structured media context even if the current model cannot process it natively.

### Images

- Download and decrypt through the WeChat CDN layer.
- If the runtime supports vision input, pass `LocalImageInput` to Codex.
- Otherwise preserve the attachment and expose controlled image tools such as metadata, OCR, or a vision adapter.
- Never pretend an image is ordinary text.

### Audio

- Prefer `VoiceItem.text` when it is a reliable candidate transcript.
- Otherwise invoke an `AudioTranscriber` tool.
- Store the original audio attachment.
- Set status to `awaiting_confirmation`.
- Send the candidate transcript to the user.
- Only `/confirm` sends the transcript to the Agent.
- `/reject` discards the candidate transcript but keeps the original attachment.

### Video

- Save the original video and metadata.
- Tell the Agent that a video exists even when no native video input is available.
- Provide a `VideoTool` interface for metadata, frame extraction, audio extraction, transcription, and thumbnails.
- Do not pass an MP4 as an image input.
- If no video tool exists, continue processing other text while clearly reporting the limitation.

Agent input is composed of:

```text
text
media_context
attachments
confirmed_transcripts
tool_results
```

## 9. Task Execution

User input flow:

```text
WeChat message
    -> normalize text and media
    -> read routes.active_agent_id
    -> save original channel reply target
    -> create queued task in SQLite
    -> enqueue task on asyncio.Queue
    -> send task-started acknowledgement
    -> Agent Worker executes in background
    -> write task events
    -> create user_outbox or agent_mailbox records
```

The WeChat handler must not synchronously wait for the full Codex turn. The worker owns the task and continues after the front Agent changes.

One conversation should serialize its normal tasks unless the Agent explicitly supports concurrency. Different Agents may run concurrently for the same user.

On process startup:

```text
running -> failed_needs_retry
```

Do not automatically repeat a task in the first phase. Keep pending user outbox and Agent mailbox records for later delivery/processing.

## 10. Agent Collaboration

Agent requests must include:

```text
source_agent_id
destination_agent_id
request_type
task_id
parent_task_id
correlation_id
in_reply_to
content
attachments
```

An Agent response routes to the original requesting Agent's mailbox. It does not go to WeChat unless explicitly promoted to a user-visible event.

Limit collaboration with:

```text
max_child_depth = 3
max_children_per_task = 20
```

Record rejected requests as audit events.

## 11. Commands

First phase commands:

```text
/agents
/agent
/agent <id>
/status
/status all
/interrupt
/interrupt <task_id|agent_id>
/notify on
/notify off
/inbox
/inbox <agent_id|all>
/tasks
/ask <agent_id> <prompt>
/confirm
/reject
```

`/agent <id>` only changes routing and then displays that Agent's pending user outbox records ordered by priority and time. It does not wait for tasks and does not consume agent_mailbox records.

## 12. Modules and Migration

Add:

```text
src/runtime/models.py
src/runtime/store.py
src/runtime/sqlite_store.py
src/runtime/registry.py
src/runtime/manager.py
src/runtime/worker.py
src/runtime/policy.py
src/runtime/media.py
src/agents/base.py
src/agents/codex.py
```

Keep:

```text
wechat_ilink/
```

as the WeChat protocol, authentication, polling, CDN, and sender layer.

Gradually simplify:

```text
src/codex_agent.py
    Codex SDK adapter

src/codex_wechat_bot.py
    WeChat gateway, command parsing, and channel delivery adapter
```

The JSON `SessionManager` should be replaced by SQLite route/conversation records after the new store is proven by tests.

## 13. Implementation Phases

### Phase 1: Persistence and models

- Add SQLite connection and schema migrations.
- Add Store interfaces and CRUD tests.
- Add AgentProfile, Task, Message, Outbox, Mailbox, and attachment models.
- Add startup recovery marking `running` tasks as `failed_needs_retry`.

### Phase 2: Agent registry and Codex runtime

- Add AgentRegistry and PolicyEngine.
- Wrap the existing CodexAgent as CodexRuntime.
- Persist profile version and per-Agent conversation records.

### Phase 3: Async task execution

- Add TaskManager and asyncio workers.
- Change normal WeChat text handling from synchronous `run_stream` to task submission.
- Keep `/interrupt` and `/status` responsive through the command bypass.

### Phase 4: Agent switching and notifications

- Implement `/agents` and `/agent <id>`.
- Implement `/notify on|off`.
- Add user notification worker and switch-time pending outbox display.
- Preserve the task's original WeChat reply target.

### Phase 5: Agent collaboration

- Add Agent mailbox worker.
- Add request/response correlation.
- Enforce peer ACLs and request types.
- Add child-task depth and count limits.

### Phase 6: Media

- Normalize WeChat text/image/video/audio messages.
- Download and decrypt media to a managed runtime directory.
- Add image native input support.
- Add audio transcription and `/confirm`/`/reject`.
- Add video metadata and VideoTool interfaces.
- Keep binary data out of SQLite.

### Phase 7: Hardening

- Add outbox retries and idempotent delivery.
- Add task and mailbox audit events.
- Add cleanup/retention for media files.
- Run the full test suite and perform manual WeChat validation.

## 14. Verification

Tests must cover:

- SQLite schema, migrations, CRUD, and transactions
- No duplicate task claims
- Agent switching without affecting running tasks
- `/notify off` suppressing automatic user delivery
- `/notify on` delivering eligible notifications
- Switch-time display of the selected Agent's user outbox
- Agent mailbox messages never being sent to WeChat
- Agent A -> Agent B request and B -> A response
- Correlation ID routing
- Policy ACL rejection
- Agent system prompt and profile version persistence
- Image routing and Codex image input
- Video media awareness and tool metadata
- Audio transcription confirmation and rejection
- Original WeChat reply target preservation
- Restart recovery behavior
- Existing tests remaining green

Run from the repository root:

```bash
uv sync --extra test
uv run pytest
```

## 15. Explicit Non-Goals for Phase 1

- RabbitMQ
- Redis
- PostgreSQL
- Feishu adapter
- Multiple worker processes or machines
- Distributed locks
- Unlimited autonomous Agent collaboration
- Web administration UI
- Automatic task retries
- Complex authorization management

The intended first implementation is:

```text
SQLite + asyncio + one WeChat gateway + Codex Runtime
```

The storage, message, and runtime interfaces must remain replaceable so RabbitMQ, PostgreSQL, Redis, NATS, and additional channels can be added later without rewriting the domain layer.
