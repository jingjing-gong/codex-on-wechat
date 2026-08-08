# codex-on-wechat

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
to see available commands (`/clear`, `/model`, and `/models`).

Session commands are available per WeChat user:

```text
/session <session-id>       switch to or create a short session alias
/session                    create and switch to a new local session
/sessions                   list short session IDs and summaries
/delsession <session-id>    delete the mapped Codex session
```

The `/sh <command>` command executes a shell command in the bot project's root
directory and returns its output. It has a 30-second timeout and can execute
arbitrary commands on the host, so use it only with trusted WeChat users.

## Requirements

- Python >= 3.10
- The `openai-codex` package, installed automatically by `uv`
- A WeChat account

