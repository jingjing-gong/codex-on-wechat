"""Interactive chat demo for the codex ACP agent.

Usage:
    python examples/acp_chat.py codex

Requires a codex CLI on PATH: `codex-acp` (preferred) or `codex` (native
`app-server` mode).

Commands while chatting:
  /new    reset the conversation (start a fresh session/thread)
  /quit   exit
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from acp_agents import SUPPORTED_AGENTS, create_agent  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


async def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in SUPPORTED_AGENTS:
        print(f"usage: python examples/acp_chat.py <{'|'.join(SUPPORTED_AGENTS)}>")
        raise SystemExit(1)

    name = sys.argv[1]
    agent = create_agent(name)
    await agent.start()
    print(f"connected: {agent.info()}")
    print("type a message, or /new to reset the session, /quit to exit")

    conversation_id = "cli"
    try:
        while True:
            try:
                message = await asyncio.to_thread(input, "> ")
            except EOFError:
                break
            message = message.strip()
            if not message:
                continue
            if message == "/new":
                await agent.reset_session(conversation_id)
                print("(session reset)")
                continue
            if message in ("/quit", "/exit"):
                break

            try:
                reply = await agent.chat(conversation_id, message)
            except Exception as exc:
                print(f"error: {exc}")
                continue
            print(reply)
    finally:
        await agent.stop()


if __name__ == "__main__":
    asyncio.run(main())
