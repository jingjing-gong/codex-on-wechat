# codex-on-wechat

`codex-on-wechat` connects a WeChat account to a durable, multi-Agent Codex
runtime. Messages are accepted into SQLite before execution, Agents can keep
working in the background, and replies are delivered through a durable outbox.

The WeChat integration uses the unofficial iLink protocol and is intended for
personal or educational use.

## Quick start

```bash
./cow
```

The launcher installs `uv` when necessary, synchronizes dependencies, and
starts the bot. On first run, scan the terminal QR code with WeChat. Login
credentials are stored locally and reused on later runs.

Login or remove all saved logins without starting the bot:

```bash
./cow login
./cow logout
```

Requirements:

- Python 3.10 or newer
- A WeChat account
- Codex authentication usable by `openai-codex`

## Commands

Send `/help` in WeChat for the authoritative command summary.

### Conversation

- `/clear` or `/reset` clears the active Agent conversation.
- `/mode [chat|plan|review|execute]` shows or changes the current mode.
- `/modes` lists all modes in Markdown and marks the current mode.
- `/model [<model-id> <effort|default>|effort <effort|default>]` shows or changes the model and reasoning effort.
- `/models` lists model effort choices in Markdown and keeps a persisted,
  catalog-missing selection visible as current and unavailable.
- `/skills` lists enabled skills.
- `$<skill> <task description>` submits work with an explicit skill snapshot.

All current modes allow command execution and network access. `chat`, `plan`,
and `review` instruct the Agent not to modify files; `execute` also permits
workspace changes.

Reasoning efforts are capability-gated by the live Codex model catalog rather
than a fixed application list. Forward-compatible values such as `max` and
`ultra` are listed and accepted only for models that advertise them; use
`/models` to see the choices available to the current account and runtime.

### Tasks

- `/status` shows active work.
- `/tasks [limit]` lists durable tasks.
- `/retry <task-id>` creates a new attempt for failed, orphaned, or interrupted work.
- `/cancel <task-id>` cancels the specified task owned by the current user.
- `/cancel` interrupts the running task on the current Agent and session.

`/cancel` is the only public cancellation command. There is no `/interrupt`
command; runtime interruption is an internal operation performed only after
the cancellation request is stored.

### Agents and notifications

- `/agents` lists registered Agents in Markdown and marks the front Agent.
- `/agent` shows the front Agent.
- `/agent <name>` switches to an Agent or creates a named Codex Agent context;
  it returns only a switch confirmation and never replays prior history.
- `/delagent <agent-id>` deletes a dynamically created Agent and routes affected
  sessions back to the default Agent. A later `/agent <same-id>` explicitly
  recreates it after validating its retained immutable Profile metadata.
- `/ask <agent-id> <prompt>` sends durable work to any valid Agent without changing the front Agent.
- `/notify [on|off]` shows or changes background notifications for the current Agent.
- `/inbox [agent-id|all]` presents unseen Agent notifications.
- `/recv` receives the next FIFO batch when a prior user message produced more
  than WeChat's ten-reply allowance.

An explicit `/ask` receives an immediate task acknowledgement. Its final reply
is returned to the originating WeChat conversation even when background
notifications for the destination Agent are disabled. Each non-empty text item
identifies its source as `<agent-id>: <message>` before 3,000-character reply
chunking.

New tasks on the durable v3 Agent profile can also contact other Agents created
with `/agent`. Codex receives a task-scoped local `list`/`send` capability; the
command carries a short-lived, unguessable token bound to the current task
execution. The bridge rechecks that execution, the running task's immutable
mode, peer ACL, and request type before writing to the durable Agent mailbox.
Repeated identical CLI sends use a stable request ID. Agent mailbox traffic is
internal and never bypasses the user outbox to send directly to WeChat. Replies
return to the requesting Agent's conversation without creating an automatic
reply loop.

### Shell access

`/sh <command>` executes a shell through `/bin/sh` in the configured bot
workspace. The response is bounded and formatted as Markdown with the command,
exit status, and fence-safe output. It has a 30-second timeout and can execute
arbitrary host commands, so expose this bot only to trusted WeChat users.

## Runtime behavior

The default launcher uses the SQLite-backed durable runtime. Each accepted
task snapshots its Agent, conversation, mode, policy, model, effort, skill,
attachments, and reply target. Switching settings affects future tasks only.
Work for one Agent conversation is serialized; different Agents can run
concurrently.

For every supported finished user message, the bot sends a best-effort WeChat
typing state before command, media, or task processing. Typing failures do not
reject the message and typing does not consume one of the ten reply slots.

Text and voice messages with a non-empty channel transcript create tasks.
Uploaded files are staged in managed storage and acknowledged without
implicitly executing their caption.

Completed Codex image-generation items are sent as native WeChat images. The
runtime accepts the SDK's workspace-local `savedPath` or a bounded
`data:image/...;base64` result, verifies PNG/JPEG/GIF/WebP bytes, copies them to
managed attachment storage, and then uses the durable CDN/media worker. Paths
outside `CODEX_WECHAT_WORKSPACE`, symlinks, spoofed image bytes, and oversized
outputs fail closed. Delivery retries reuse the same attachment and WeChat
reply identity.
Task results and user deliveries survive restart. Work interrupted by an
uncertain process exit becomes orphaned and requires explicit `/retry`.

Older launch scripts may still pass `--legacy`, but it is now a deprecated
alias for the same durable runtime. It no longer selects a separate command
router:

```bash
uv run src/codex_wechat_bot.py --legacy
```

## Configuration

The durable runtime recognizes these environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_WECHAT_DB` | `~/.codex-wechat-bot/runtime.sqlite3` | SQLite runtime database |
| `CODEX_WECHAT_ATTACHMENTS` | `~/.codex-wechat-bot/attachments` | Managed attachment root |
| `CODEX_WECHAT_WORKSPACE` | `~/.codex-on-wechat/workspace` | Shared Codex and `/sh` working directory |
| `CODEX_WECHAT_AGENT_SOCKET` | `<database>.agent.sock` | Owner-only local Agent mailbox bridge |
| `CODEX_WECHAT_WORKERS` | `1` | Concurrent task workers |
| `CODEX_WECHAT_TURN_TIMEOUT` | unset | Optional Codex turn timeout in seconds |
| `CODEX_WECHAT_SKILL_ROOTS` | unset | Path-separated trusted skill roots |

Credentials and compatibility cursor files are stored under
`~/.codex-wechat-bot/accounts/`. Credential publication is atomic and uses
owner-only filesystem permissions. The durable WeChat cursor is also stored in
SQLite.

## Development

Install dependencies and run the test suite:

```bash
uv sync --extra test
uv run pytest -q
```

The complete architecture, invariants, state machines, and verification matrix
are documented in [`plan.md`](plan.md).
