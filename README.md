# acp-agents

Bridges a real WeChat account to the Codex AI coding agent, so you can chat
with Codex from WeChat.

## Quick start

```bash
./cow
```

This installs `uv` if needed, syncs dependencies, and starts the bot. On
first run, scan the printed QR code with WeChat to log in (credentials are
saved and reused automatically after that).

Login and logout directly:

```bash
./cow login
./cow logout
```

Each WeChat contact gets their own Codex conversation. Send `/help` in a chat
to see available commands (`/clear`, `/model`, `/models`, `/skills`), or
`$skill-name <prompt>` to invoke a specific skill.

Session commands are available per WeChat user:

```text
/session <thread-id>        resume a Codex session or create a mapping
/session                     create and switch to a new local session
/sessions                    list Codex sessions from thread/list
/delsession <thread-id>     delete the Codex session
```

## Requirements

- Python >= 3.9
- A `codex` CLI on `PATH` (supporting `codex app-server`, or the `codex-acp`
  wrapper binary)
- A WeChat account

