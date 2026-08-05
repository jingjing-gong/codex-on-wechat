# acp-agents

Bridges a real WeChat account to the Codex AI coding agent, so you can chat
with Codex from WeChat.

## Quick start

```bash
./run.sh
```

This installs `uv` if needed, syncs dependencies, and starts the bot. On
first run, scan the printed QR code with WeChat to log in (credentials are
saved and reused automatically after that).

Each WeChat contact gets their own Codex conversation. Send `/help` in a chat
to see available commands (`/clear`, `/model`, `/models`, `/skills`), or
`$skill-name <prompt>` to invoke a specific skill.

## Requirements

- Python >= 3.9
- A `codex` CLI on `PATH` (supporting `codex app-server`, or the `codex-acp`
  wrapper binary)
- A WeChat account

