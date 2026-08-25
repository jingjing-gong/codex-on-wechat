# codex-on-wechat

`codex-on-wechat` connects a WeChat account to Codex. Each Agent runs in its
own OS process, tasks survive restarts in SQLite, and different Agents can
work concurrently.

The WeChat integration uses the unofficial iLink protocol. Use it only for
personal or educational purposes and only with trusted WeChat users.

## Quick start

You need Linux or macOS, Python 3.10 or newer, internet access, a WeChat
account, and a working Codex login.

From the repository root:

```bash
# Log in to WeChat by scanning the terminal QR code.
./cow login

# Log in to Codex separately and verify it.
uv run codex login
uv run codex login status

# Start the bot and leave this process running.
./cow
```

The launcher installs `uv` with `curl` when necessary and synchronizes the
Python dependencies. On later runs, `./cow` reuses the saved WeChat login.
Stop the bot with `Ctrl-C`.

`./cow login` is only for WeChat. `uv run codex login` is for Codex. To remove
all locally saved WeChat credentials, run `./cow logout`.

## First use

Send an ordinary WeChat message to work with the current Agent. A useful
multi-Agent workflow is:

```text
/agent reviewer
/system You review technical writing for accuracy.
/agent writer
/system You are a concise technical writer.
Draft a release note for this project.
/ask reviewer Review this draft: <paste the draft here>
```

To bind a new Agent to a named Codex configuration, pass its filename stem:

```text
/agent qwen-agent qwen
/models
```

This selects `$CODEX_HOME/qwen.config.toml`; `/models` then uses that Agent's
provider catalog and configured default. The binding is durable and immutable:
repeating the same selection is safe, while reusing the Agent ID with another
profile is rejected. `/agent <id>` preserves an existing binding and uses the
base Codex configuration for a new Agent.

`/agent <id> [profile]` switches to an existing Agent or creates it. Agent IDs are
lowercase, start with a letter, and may contain letters, digits, `-`, and `_`.
Profile names are validated filename stems; paths and command fragments are
rejected. Provider configuration and credentials are never included in bot
responses or process arguments.
Use `/system default` to clear a custom role.

Agents keep separate histories, so include the necessary context in `/ask`.

Switching Agents never replays conversation history. It may include unseen
background replies produced while that Agent was not selected.

## Commands

Send `/help` in WeChat for the authoritative list.

| Command | Purpose |
| --- | --- |
| `/clear`, `/reset` | Start a fresh conversation for the current Agent. |
| `/compact` | Compact the current Agent's active context without clearing its conversation. |
| `/skills`, `$<skill> <task>` | List skills or run a task with one. |
| `/cd [path]` | Show or change the current Agent's working directory. |
| `/sh <command>` | Run a shell command in the current Agent's working directory, with a 30-second limit. |
| `/status`, `/tasks [limit]` | Show active work or task history. |
| `/cancel [task-id]` | Cancel a task, or the current running task when omitted. |
| `/retry <task-id>` | Retry failed, orphaned, or interrupted work. |
| `/agents`, `/agent [agent-id] [profile]` | List Agents or show/switch/create the current Agent, optionally selecting a named Codex config. |
| `/delagent <agent-id>` | Retire a dynamically created Agent; immutable task and history records remain. |
| `/ask <agent-id> <prompt>` | Send work to an Agent without switching to it. |
| `/system [default\|<role>]` | Show, clear, or set the current Agent's role. |
| `/mode [chat\|plan\|review\|execute]`, `/modes` | Show, change, or list modes. |
| `/model`, `/models` | Show the current model or list available models and efforts. |
| `/model <model-id> <effort\|default>` | Set the model and supported reasoning effort. |
| `/model effort <effort\|default>` | Change or clear only the effort override. |
| `/notify [on\|off]` | Control background notifications for the current Agent. |
| `/inbox [agent-id\|all]` | Show unseen background replies. |
| `/recv` | Receive replies deferred by WeChat's ten-message quota. |

Run `/models` before choosing efforts such as `max` or `ultra`; supported
efforts depend on the selected model.

## Modes and Agent behavior

All current modes allow network access and command execution:

- `chat`: conversation and analysis without file changes.
- `plan`: inspection and planning without file changes.
- `review`: code and diff review without file changes.
- `execute`: implementation with workspace changes allowed.

The default production Agent starts in `execute` mode. Use `/mode` to inspect
the current selection.

Settings apply to future tasks. Already queued or running work keeps the
Agent, role, mode, model, effort, and working-directory snapshot captured when
it was accepted.

Each Agent owns one persistent OS process and handles one task at a time.
Different Agents run in parallel. An Agent may send a supervised request to
another created Agent; the durable mailbox allows one correlated response and
does not automatically reply to replies.

## Context compaction

`/compact` compacts only the current Agent's exact session and provider thread.
It preserves the thread and durable task/event history; unlike `/clear`, it
does not start a new conversation. The command is rejected if that conversation
is executing or has no bound provider thread.

Automatic compaction is model-specific. Inside its own process, each Agent
queries the configured provider for the exact model's context window and asks
Codex to compact at 80% of a validated window. Provider catalogs may omit this
non-standard metadata. In that case the bot uses a context window only when the
effective Codex configuration supplies one for the exact selected model; it
never invents a limit. Provider credentials and the context cache remain inside
that Agent process and never cross supervisor IPC.

For a provider that omits the extension, set `model` and
`model_context_window` in the effective Codex configuration to the provider's
documented values; an optional lower `model_auto_compact_token_limit` is
honored. The fallback is ignored when `model` does not exactly match.

## Working directories

Use `/cd` to inspect or change the current Agent's working directory:

```text
/cd
/cd projects/api
/cd "projects/My App"
```

With no path, `/cd` shows the current directory. A path changes it only for the
current channel, bot, user, session, and Agent. Relative paths start from that
Agent's currently selected directory. The selection is stored in SQLite, so it
survives restarts and switching away and back. `/cd` does not change the
supervisor's process-wide directory or restart an Agent process.

Every selected path must resolve to an existing, enterable directory inside
`CODEX_WECHAT_WORKSPACE`. This setting is a confinement root, not merely a
default working directory. Canonical-path checks reject symlink escapes outside
the root.

When the bot accepts a task, it freezes an immutable `execution_workspace`
snapshot with the canonical root and path plus their device/inode identities.
Queued and running work keeps that snapshot, so a later `/cd` affects only
future work. The Agent validates it before SDK or network work and again before
the native turn; a missing, moved, or replaced root or directory fails closed.
If a selected directory disappears, cwd-dependent work and relative `/cd`
fail, while `/help`, `/agent`, and an absolute `/cd` to a valid in-root
directory remain available for recovery.

For backward compatibility, a legacy durable task without an
`execution_workspace` continues in that Agent's configured default working
directory. The device/inode-pinned fail-closed guarantee therefore applies to
snapshotted and newly accepted work.

Validation is repeated immediately before an SDK/native turn and before the
supervisor launches `/sh`. Those pathname-based APIs do not provide an open
directory-descriptor (`dirfd`) contract, so a residual TOCTOU window remains
between final validation and path consumption.

`/sh` remains supervisor-owned but uses the front Agent's workspace snapshot
captured with that command. `/ask <agent-id>` and Agent-to-Agent mailbox work
instead use the destination Agent's selected directory for the originating
user and session; they do not inherit the front or sending Agent's directory.
Retiring a dynamic Agent clears only that Agent's mutable directory selections;
immutable tasks and history remain.

## Replies and background work

- The bot sends a best-effort typing state when it receives a supported message.
- Each WeChat text reply is at most 3,000 characters.
- One received message permits at most ten reply sends. Use `/recv` for overflow.
- `/inbox` shows unseen background items; it does not drain `/recv` overflow.
- `/ask` results use `sender: message` so the producing Agent is clear.
- Tasks and pending deliveries survive a normal restart.

## Configuration

Export configuration before starting `./cow`; this repository does not load a
`.env` file.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_WECHAT_DB` | `~/.codex-wechat-bot/runtime.sqlite3` | Durable SQLite database. |
| `CODEX_WECHAT_WORKSPACE` | `~/.codex-on-wechat/workspace` | Confinement root for all Agent working directories. |
| `CODEX_WECHAT_ATTACHMENTS` | `~/.codex-wechat-bot/attachments` | Managed uploaded/generated files. |
| `CODEX_WECHAT_LOG` | `~/.codex-wechat-bot/logs/codex-wechat.log` | Owner-only rotating supervisor/task diagnostic log. |
| `CODEX_WECHAT_LOG_LEVEL` | `INFO` | Persistent and console logging level. |
| `CODEX_WECHAT_LOG_MAX_BYTES` | `20971520` | Maximum size of the active log before rotation. |
| `CODEX_WECHAT_LOG_BACKUPS` | `5` | Number of rotated log files retained. |
| `CODEX_WECHAT_ALLOWED_CONFIG_PROFILES` | `qwen` | Comma-separated Codex config profile names that `/agent` may select. `*` enables every safe profile name. |
| `CODEX_WECHAT_MAX_AGENT_PROCESSES` | `16` | Maximum independent Agent processes. |
| `CODEX_WECHAT_TURN_TIMEOUT` | unset | Optional Codex turn timeout in seconds. |
| `CODEX_WECHAT_SKILL_ROOTS` | unset | Colon-separated trusted skill directories. |

Example:

```bash
CODEX_WECHAT_WORKSPACE=/absolute/path/to/workspace ./cow
```

`CODEX_WECHAT_ALLOWED_CONFIG_PROFILES` names the Codex config layers that
`/agent <id> <profile>` may select. Each named profile is a standalone file
at `$CODEX_HOME/<profile>.config.toml` (for example `~/.codex/spark.config.toml`);
`[profiles.*]` sections in the base `config.toml` are not read. Selecting a
profile authorizes the name only: the file must still exist, be valid TOML,
and contain a `model`, `model_provider`, and matching `model_providers`
entry. Setting the value to `*` accepts any profile name that passes the
safe-name check (one `A-Za-z0-9` character start, then letters, digits, `_`,
or `-`; at most 64 characters); unsafe names are still rejected and the file
existence check still applies.

`CODEX_WECHAT_WORKERS` is deprecated and ignored.

## Safety and troubleshooting

- `/sh` can execute arbitrary host commands. All modes also have command and
  network access, while `execute` can modify the workspace. Use trusted users.
- Agent processes isolate lifecycle and concurrency; they are not hostile-user
  sandboxes. They run as the same OS account, and working-directory confinement
  is not a general filesystem-access sandbox.
- If Codex work cannot start, run `uv run codex login status`.
- To inspect recent task/provider failures, run `./cow logs 300`. Structured
  `agent_task_started` and `agent_task_terminal` records include task, Agent,
  model, effort, duration, and typed Codex error status, but exclude prompts,
  response bodies, request headers, and credentials.
- To force a fresh WeChat QR login, run `./cow logout`, then `./cow login`.
- For missing output, check `/status`, `/tasks`, `/inbox all`, and `/recv`.
- Only one `./cow` supervisor may own a database and WeChat account at a time.

## Development and design

```bash
uv sync --extra test
uv run pytest -q
```

On macOS, pytest skips the disconnected Linux pidfd/cgroup hardening
foundations; the active process-per-Agent runtime and launcher suites run on
both supported platforms.

See [architecture.md](architecture.md) for the implemented process and data
flow, and [plan.md](plan.md) for the complete behavior and invariants.
