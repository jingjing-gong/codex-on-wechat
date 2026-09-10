# codex-on-wechat

`codex-on-wechat` connects WeChat plus one or more Lark/Feishu bot accounts to
one shared Codex Agent runtime. Each Agent runs in its own OS process, tasks
survive restarts in SQLite, and different Agents can work concurrently.

The WeChat integration uses the unofficial iLink protocol. Use it only for
personal or educational purposes and only with trusted WeChat users.

## Quick start

You need Linux or macOS, Python 3.10 or newer, internet access, and a WeChat
account. The launcher installs Codex if missing; a working Codex login is
still required.

From the repository root:

```bash
# Log in to WeChat by scanning the terminal QR code.
./cow wechat login

# Log in to Codex separately and verify it.
export PATH="$HOME/.local/bin:$PATH"
codex login
codex login status

# Start the bot and leave this process running.
./cow

# From another terminal, check every registered bot in one view.
./cow status
```

The launcher installs `uv` with `curl` when necessary and synchronizes the
Python dependencies. Before starting the bot or logging in to WeChat, it
reuses Codex from `PATH` or `~/.local/bin/codex`. If neither exists, it runs:

```bash
sh -c 'curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh'
```

The selected executable is passed to all Agent processes. An explicit
`CODEX_WECHAT_CODEX_BIN` takes priority; an invalid override fails rather than
installing or selecting a different binary. Administrative commands such as
`status`, `logs`, `lark`, and `logout` do not install Codex. On later runs,
`./cow` reuses the saved WeChat login. Stop the bot with `Ctrl-C`.

`./cow wechat login` is only for WeChat. `codex login` is for Codex. To
remove all locally saved WeChat credentials, run `./cow wechat logout`. The
older `./cow login` and `./cow logout` spellings remain compatibility aliases.

### Check bot health

`./cow status` prints one compact `CHANNEL  BOT  STATE  DETAIL` table for every
registered Lark profile and saved WeChat account. Lark rows use the same
profile, App ID, enabled state, connection state, generation,
`last_ready_at`, and `last_error` data as `./cow lark status`. WeChat is ready
only when its saved credential is valid, a matching supervisor epoch is
active, and the exact account's long-poll checkpoint has been updated
recently; it does not infer health from a process name.

The command exits zero only when every row is `READY`, and exits non-zero when
any registered bot is down (or no bot is registered), so it can be used by
monitoring scripts. The WeChat activity window defaults to 120 seconds and can
be changed with `CODEX_WECHAT_STATUS_STALE_SECONDS`.

### Add Lark/Feishu bots

Install the reviewed `lark-cli` release (the runtime rejects every other
version), then add each PersonalAgent app through its own terminal QR flow:

```bash
npx @larksuite/cli@1.0.92 install
lark-cli --version          # must report 1.0.92
./cow lark add work
./cow lark add personal
./cow lark list
./cow                 # starts WeChat and every enabled Lark profile
```

When the gateway is already running, an actively mapped canonical `owner` can
add another app without using the Mac terminal. In a direct conversation with
any connected Lark bot, send either `/lark add [profile]` or the exact phrase
`新增一个飞书 bot`. COW rechecks the sender's current app-scoped owner mapping,
then returns the official configuration link unchanged together with a QR
image. After the scan completes, the running supervisor validates and
registers the isolated profile through its existing SQLite connection and
starts only that new account. Existing Lark accounts and WeChat keep their
credentials, routes, conversations, delivery workers, and account locks.

Chat onboarding is deliberately unavailable in groups, to unmapped or revoked
senders, and on WeChat. Only one credential-provisioning attempt runs at a
time. Its progress and final result are durably addressed back through the
Lark bot and direct chat that initiated it. Scan only with the account that
should receive cow's global `owner` authority, and do not forward the one-time
QR or link. After bot identity verification, COW uses only the new bot's
isolated credentials to read its own official application metadata. It accepts
the app-scoped human `creator_id` only when the returned App ID, app kind,
enabled state, identity mode, and documented owner fields agree; the Bot Open
ID is explicitly rejected as a human identity. The exact owner mapping,
profile, and status then commit in one SQLite transaction after the initiating
owner mapping is revalidated inside that transaction. The new bot can therefore
use owner-only commands immediately. Any missing, malformed, stale, or
conflicting identity leaves no partially registered bot or mapping.

The terminal `./cow lark add` path remains the offline administration path. It
acquires exclusive database ownership and therefore must not be run against a
live gateway. Use chat onboarding for a live add, or stop the gateway normally
before using the terminal command; no permission-mode change is part of either
flow.

Every profile added through the local owner CLI is implicitly managed by the
local OS owner; no human Open ID is required or prompted for. `./cow lark list`
is the concise bot-profile inventory. Unfiltered `./cow lark principal list`
shows the same owner-managed bots first, followed by their optional human
principal mappings. The generic `./cow principal list` remains a human-account
mapping view and does not list bots.

To add a bot from an existing Feishu custom app instead, pass its App ID. The
launcher prompts for the App Secret with terminal echo disabled; the value is
not placed in the command line, environment, or shell history:

```bash
./cow lark add work --app-id cli_xxxxxxxxx --brand feishu
# App Secret: <hidden input>
```

If the local owner already knows their human account's app-scoped Open ID,
`--owner-open-id` can additionally map that sender identity to the canonical
principal named `owner` during registration:

```bash
./cow lark add work --owner-open-id ou_xxxxxxxxx
./cow lark add work --app-id cli_xxxxxxxxx --brand feishu \
  --owner-open-id ou_xxxxxxxxx
# App Secret: <hidden input>
```

`--owner-open-id` is optional and must be the human sender account's `open_id`
under the exact app being added. After bot identity verification, COW publishes
the private config as a separately recoverable filesystem step, then commits
the durable profile, status, and, when requested, human mapping to canonical
principal `owner` in one SQLite transaction. A requested mapping therefore
cannot commit without its profile registration. Lark/Feishu Open IDs are
app-scoped, so
the same person normally has a different `ou_...` value for every app, and
accounts in different organizations necessarily require their own value. Do
not reuse an Open ID obtained through another bot. Bot authentication cannot
infer this human ID. Omitting the option simply creates the owner-managed bot
without a human mapping at registration time. `--without-owner` remains
accepted as a compatibility spelling for that default and is mutually
exclusive with `--owner-open-id`.

For non-interactive automation, the original explicit stdin contract remains
available. Redirected input is consumed only when `--app-secret-stdin` is
present:

```bash
# Replace this command with your secret manager's stdout command.
secret-manager read lark/app-secret | \
  ./cow lark add work --app-id cli_xxxxxxxxx --brand feishu \
    --app-secret-stdin
```

Automation may add `--owner-open-id ou_xxxxxxxxx` when it intentionally creates
the optional human mapping; no owner-decision flag is otherwise necessary.

Before using existing credentials, enable the app's bot capability, grant and
approve its required IM permissions, subscribe to `im.message.receive_v1`
through a long connection, publish an app version, and make the app available
to the intended users. Never put an App Secret directly in an argument,
environment variable, source file, or chat message.

App ID/App Secret authentication is bot application authentication. Terminal
QR and existing-app onboarding do not by themselves establish a human sender
mapping: despite its compatibility name, `--owner-open-id` is an explicit
mapping claim for one human account, not proof of bot ownership. The live chat
QR flow is narrower: because it creates a new app, COW can verify that app's
app-scoped human creator/owner through the bot-authenticated own-application
API and atomically map that identity. None of these paths provides user OAuth
or runs `lark-cli auth login`. App credentials and human Open IDs are bound to
their specific app and organization. To serve multiple organizations, scan the
live QR with the intended account in each organization (or create/authorize an
app there and use the terminal flow), giving every app a distinct COW profile.

After onboarding, the supervisor can infer the owner from authenticated,
durable inbound history. At each startup, while it owns the database and the
complete enabled account set, it checks every enabled Lark profile and the
active WeChat credential. An account with no active principal mapping and
exactly one distinct `inbound_messages.external_user_id` is mapped to canonical
principal `owner`; Lark records the identifier as `open_id`, and WeChat as
`from_user_id`. Existing mappings are never overwritten. Accounts with zero or
two or more distinct senders are logged and skipped, so group/shared accounts
cannot be guessed. The operation is idempotent and never uses message text.

This inference runs at startup: if a brand-new account receives its first
message during the current run, it becomes eligible on the next normal
supervisor start. The explicit principal mapping commands remain available
when the account is shared or an owner mapping is needed immediately.

Each profile has an isolated owner-only CLI configuration directory and the
stable app ID is its durable `bot_id`. The flagless QR flow uses
`lark-cli config init --new`; the existing-app flow initializes the same kind
of isolated profile with `lark-cli config init --app-id ...
--app-secret-stdin --brand ...`. Neither flow is user OAuth or runs
`lark-cli auth login`. Every managed CLI child uses an owner-only `077` umask;
retained staging artifacts are safely normalized before publication while
links, special files, hardlinks, and foreign ownership remain rejected.

The reviewed CLI's keychain entry is global to the local OS user and App ID;
changing `LARKSUITE_CLI_CONFIG_DIR` does not isolate that keychain entry. COW
therefore rejects duplicate live App IDs and serializes its own credential
mutations. Do not run standalone `lark-cli config` operations concurrently for
the same App ID. Removing a profile imported with App ID/App Secret deletes
only COW's local profile state and intentionally retains the external/global
credential. `./cow lark reauthorize` is consequently QR-only and rejects an
imported profile before starting `lark-cli`. To rotate an imported secret,
remove that COW profile, then add the same App ID under a new profile name with
the hidden App Secret prompt (or `--app-secret-stdin` for automation).

Management commands are:

```text
./cow lark list
./cow lark status [profile]
./cow lark recover <retained-.add-name> [profile]
./cow lark reauthorize <profile>
./cow lark disable <profile>
./cow lark enable <profile>
./cow lark remove <profile>
./cow lark principal list [principal-id]
./cow lark principal map <principal-id> <profile> <ou_open_id> [--display-name NAME]
./cow lark principal unmap <profile> <ou_open_id>
```

Canonical principals can also be mapped to any exact authenticated channel
account without requiring a Lark bot profile:

```text
./cow principal list [principal-id]
./cow principal map <principal-id> <channel> <bot-id> <external-user-id> [--identifier-kind KIND] [--display-name NAME]
./cow principal adopt <principal-id> <agent-id> <channel> <bot-id> <external-user-id> [--session-id SESSION]
./cow principal unmap <channel> <bot-id> <external-user-id>
```

These views deliberately separate operational bot ownership from sender
identity. `./cow lark list` shows registered bot profiles. Unfiltered `./cow
lark principal list` prints `OWNER-MANAGED LARK BOT PROFILES` for every live
profile, then `HUMAN PRINCIPAL MAPPINGS (OPTIONAL)` (or reports that none
exist). Filtering that command by principal ID shows only the human section.
The generic principal list spans channels but remains human-mapping-only. A
profile created earlier without a human mapping is still owner-managed. It is
automatically mapped on a later startup when its durable history has exactly
one sender, or it can be mapped explicitly after its app-scoped human Open ID
is known:

```bash
./cow lark principal map owner <profile> ou_xxxxxxxxx
./cow principal list owner
```

For the live WeChat account, use `channel=wechat`, the credential's exact
`ilink_bot_id`, and the inbound message's exact `from_user_id`. The identifier
kind defaults to `external_user_id`; `--identifier-kind from_user_id` records
the more specific WeChat namespace for audit. Mapping and unmapping acquire
exclusive ownership of the runtime database and that channel account, so stop
a running `./cow` supervisor before changing authorization records.

Profile creation acquires exclusive database ownership before starting either
onboarding flow. If a supervisor is already running, stop it and retry; no new
Lark credential is provisioned. A failed existing-app import may retain a
private `.add-*` directory because COW cannot safely assume ownership of a
possibly pre-existing global credential. If that directory contains a valid
config matching its recorded App ID and brand, stop the supervisor and finalize
it without another scan:

```bash
./cow lark recover .add-work-ab12cd34
```

Pass only the retained directory's basename. Recovery revalidates the exact
bot identity, publishes it atomically, and never deletes its credential.
Metadata-only, malformed, or identity-mismatched retained directories are audit
artifacts and cannot be recovered automatically; keep them private for manual
inspection.

Direct messages are accepted normally. Group messages require a structured
mention of that exact bot; matching its display name as plain text is not
enough. Replies return to the exact originating app, chat and topic/thread.
Every bot sees the same enabled Agent catalog, but its route and conversation
selection stay local to that bot and direct/chat/thread subject.

Lark text replies are projected as native rich-text `post` messages through
the pinned CLI's Markdown converter, so headings, lists, links and fenced code
render in Feishu/Lark. This projection is Lark-only: WeChat keeps its existing
plain-text Markdown projection and wire behavior.

On Lark, ordinary users may switch only to an already-enabled Agent. Creating
an Agent with `/agent <new-id> [profile]`, selecting a configuration profile,
or using `/delagent` changes the global catalog and therefore requires a
canonical principal listed in `CODEX_LARK_ADMIN_PRINCIPALS`. WeChat retains
its existing command behavior.

Principal mappings are local human identity records for
authorization, audit, and shared history; they are not bot-ownership records.
Mapped direct accounts share provider conversation history for the same
canonical principal, Agent, and session. Agent selection and other mutable
route state remain channel-local, and every task still replies through the
exact bot, chat/thread, and channel that initiated it. Group chats and Lark
threads never join principal history. Existing provider histories are not
mergeable; use `principal adopt` before starting the supervisor when one
established direct conversation (for example the longer-running WeChat thread)
must remain the canonical history anchor. The
legacy `./cow lark principal map` command continues to validate the durable bot
profile and a tenant-stable `ou_` Lark `open_id`; the generic
`./cow principal map` command records the exact channel/bot/user triple supplied
by the local owner. Event text can never supply a principal. Add the mapped principal ID to
`CODEX_LARK_ADMIN_PRINCIPALS` before granting global Agent-catalog authority.

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

Send `/help` in either channel for its authoritative list. Lark omits `/recv`,
which is specifically WeChat's ten-send continuation command.

| Command | Purpose |
| --- | --- |
| `/clear`, `/reset` | Start a fresh conversation for the current Agent. |
| `/compact` | Compact the current Agent's active context without clearing its conversation. |
| `/skills`, `$<skill> <task>` | List skills or run a task with one. |
| `/cd [path]` | Show or change the current Agent's working directory. |
| `/sh <command>` | Run a shell command in the current Agent's working directory, with a 30-second limit. |
| `/status`, `/tasks [limit]` | Show active work or task history. |
| Natural-language scheduling, `/cron add <schedule> -- <prompt>`, `/cron list`, `/cron delete <job-id>`, `/cron help` | Propose/confirm, create, inspect, or disable durable scheduled Agent work. |
| `/cancel [task-id]` | Cancel a task, or the current running task when omitted. |
| `/report <message>` | Record an operator report in the persistent bot log without starting Agent work. |
| `/retry <task-id>` | Retry failed, orphaned, or interrupted work. |
| `/agents`, `/agent [agent-id] [profile]` | List Agents or show/switch/create the current Agent, optionally selecting a named Codex config. |
| `/delagent <agent-id> [force]` | Retire a dynamically created Agent. The safe default refuses unfinished work; `force` interrupts/cancels it and reports affected task IDs. Immutable task and history records remain. |
| `/ask <agent-id> <prompt>` | Send work to an Agent without switching to it. |
| `/system [default\|<role>]` | Show, clear, or set the current Agent's role. |
| `/mode [chat\|plan\|review\|execute]`, `/modes` | Show, change, or list modes. |
| `/model`, `/models` | Show the current model or list available models and efforts. |
| `/model <model-id> [<effort\|default>]` | Set the model; omitted effort clears any override and uses the model default. |
| `/model effort <effort\|default>` | Change or clear only the effort override. |
| `/notify [on\|off]` | Control background notifications for the current Agent. |
| `/inbox [agent-id\|all]` | Show unseen background replies, excluding command results. |
| `/recv` | Receive replies deferred by WeChat's ten-message quota. |

### Scheduled prompts

You may schedule work in an ordinary WeChat or Lark conversation without
writing cron syntax. For example, send `每天工作日上午9点帮我总结待办`. The Agent
normalizes the request and returns a draft containing the schedule, timezone,
work prompt, next firing time, draft ID, and expiry. This first turn does not
create a job. Send a later message that is exactly `确认` or `confirm` to create
it, or `取消` / `cancel` to discard it. A draft expires after 15 minutes, and a
new proposal in the same bot/chat/Agent conversation supersedes the older
pending draft. Ambiguous requests are clarified before a draft is saved.

The explicit `/cron` commands remain available and `/cron add` creates a job
immediately. Both paths schedule work for the Agent that is current when the
job is created.
When an occurrence fires, cow atomically queues that prompt for the Agent but
does not echo the prompt back to the user. The Agent's eventual result—or a
safe failure notice—is proactively delivered through the exact channel, bot,
chat/topic, and account that created the job, even when `/notify` is off. Its
exact Agent incarnation, mode, model, reasoning effort, role, working
directory, Profile, and policy are frozen when the job is created; later
session changes affect new work but do not silently reinterpret an existing
schedule.

```text
/cron add at 2026-09-09T09:30 -- Prepare today's briefing
/cron add every 2h -- Check the deployment and report any failures
/cron add cron 0 9 * * 1-5 -- Summarize today's priorities
/cron add cron 30 8 * * * --tz Europe/London -- Send the London handoff
/cron list
/cron delete cron-0123456789abcdef0123456789abcdef
```

Natural-language and explicit schedules use `Asia/Shanghai` unless another
IANA timezone is requested (or `--tz <IANA timezone>` is present). `at`
accepts an ISO date/time, `every` accepts compact durations such as `30m`,
`2h`, or `1h30m`, and `cron` accepts exactly five fields (minute, hour,
day-of-month, month, day-of-week). Jobs and their next occurrence survive
gateway restarts. If the gateway was down across multiple occurrences, a job
fires once at most on recovery and advances to the first occurrence after the
current time; one-shot jobs disable after their single firing. Deleting an
Agent also disables its schedules, so recreating the same Agent ID cannot
inherit old jobs. Removing or disabling the originating Lark bot, revoking the
owner principal mapping, or disabling the target Agent/Profile also disables
the affected schedule instead of rerouting it through another identity.

Operator reports are emitted as one warning line whose message starts with
`COW_OP_REPORT ` followed by compact JSON. Watch recent logs with
`./cow logs 500 | grep 'COW_OP_REPORT'`.

Run `/models` before choosing efforts such as `max` or `ultra`; supported
efforts depend on the selected model. `/model gpt-6-astra` uses the model's
default effort, even if the previous selection had an effort override.
Use `/model gpt-6-astra xhigh` to set an explicit effort.

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
- `/inbox` shows unseen background items, excluding command results; it does not drain `/recv` overflow.
- `/ask` results use `sender: message` so the producing Agent is clear.
- Tasks and pending deliveries survive a normal restart.

## Configuration

Export configuration before starting `./cow`; this repository does not load a
`.env` file.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_WECHAT_CODEX_BIN` | `codex` on `PATH` | External Codex executable path or command name; never falls back to the SDK-bundled runtime. |
| `CODEX_WECHAT_DB` | `~/.codex-wechat-bot/runtime.sqlite3` | Durable SQLite database. |
| `CODEX_WECHAT_WORKSPACE` | `~/.codex-on-wechat/workspace` | Confinement root for all Agent working directories. |
| `CODEX_WECHAT_ATTACHMENTS` | `~/.codex-wechat-bot/attachments` | Managed uploaded/generated files. |
| `CODEX_WECHAT_LOG` | `~/.codex-wechat-bot/logs/codex-wechat.log` | Owner-only rotating supervisor/task diagnostic log. |
| `CODEX_WECHAT_LOG_LEVEL` | `INFO` | Persistent and console logging level. |
| `CODEX_WECHAT_LOG_MAX_BYTES` | `20971520` | Maximum size of the active log before rotation. |
| `CODEX_WECHAT_LOG_BACKUPS` | `5` | Number of rotated log files retained. |
| `CODEX_WECHAT_STATUS_STALE_SECONDS` | `120` | Maximum age of WeChat long-poll activity accepted by `./cow status`. |
| `CODEX_WECHAT_ALLOWED_CONFIG_PROFILES` | `qwen` | Comma-separated Codex config profile names that `/agent` may select. `*` enables every safe profile name. |
| `CODEX_WECHAT_MAX_AGENT_PROCESSES` | `32` | Maximum independent Agent processes. |
| `CODEX_WECHAT_MAX_AGENT_QUEUE` | `256` | Maximum unfinished invocations for one Agent. |
| `CODEX_WECHAT_MAX_GLOBAL_QUEUE` | `2048` | Maximum unfinished invocations across the runtime. |
| `CODEX_WECHAT_MAX_ACCOUNT_QUEUE` | global limit | Maximum unfinished invocations accepted from one channel account (`channel`, `bot_id`). |
| `CODEX_WECHAT_MAX_ACCOUNT_AGENT_QUEUE` | Agent limit | Maximum unfinished invocations from one channel account to one Agent. |
| `CODEX_WECHAT_TURN_TIMEOUT` | unset | Optional Codex turn timeout in seconds. |
| `CODEX_WECHAT_SKILL_ROOTS` | unset | Colon-separated trusted skill directories. |
| `CODEX_LARK_CLI` | `lark-cli` | Reviewed Lark CLI executable. |
| `CODEX_LARK_CONFIG_ROOT` | `~/.codex-wechat-bot/lark` | Owner-private root for isolated Lark bot profiles and live chat onboarding stages. |
| `CODEX_LARK_ONBOARD_TIMEOUT` | `600` | Maximum seconds for either terminal or in-chat QR bot onboarding. |
| `CODEX_LARK_ADMIN_PRINCIPALS` | unset | Canonical principal IDs allowed to create/delete global Agents or select config profiles through Lark. |

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

The Python SDK remains a dependency, but its PyPI-bundled Codex executable
is not used. Upgrade your external Codex installation separately, then restart
the bot so its Agent processes launch the new binary. Restarting the terminal's
shared Codex daemon does not restart the bot's separate app-server processes.
To select a specific installation:

```bash
CODEX_WECHAT_CODEX_BIN="$HOME/.local/bin/codex" ./cow
```

`CODEX_WECHAT_WORKERS` is deprecated and ignored.

## Safety and troubleshooting

- `/sh` can execute arbitrary host commands. All modes also have command and
  network access, while `execute` can modify the workspace. Use trusted users.
- Agent processes isolate lifecycle and concurrency; they are not hostile-user
  sandboxes. They run as the same OS account, and working-directory confinement
  is not a general filesystem-access sandbox.
- If Codex work cannot start, run `codex login status` (use the executable selected by `CODEX_WECHAT_CODEX_BIN` if set).
- To inspect recent task/provider failures, run `./cow logs 300`. Structured
  `agent_task_started` and `agent_task_terminal` records include task, Agent,
  model, effort, duration, and typed Codex error status, but exclude prompts,
  response bodies, request headers, and credentials.
- To force a fresh WeChat QR login, run `./cow wechat logout`, then
  `./cow wechat login`.
- For missing output, check `/status`, `/tasks`, `/inbox all`, and `/recv`.
- Only one `./cow` supervisor may own a database or any configured channel
  account at a time; database and sorted account locks are acquired atomically.

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
