"""acp_agents: Python ACP (Agent Client Protocol) client for codex-wechat-bot.

Bridges WeChat to long-running Codex subprocesses over JSON-RPC/stdio via
either supported protocol variant:

- Codex app-server protocol (`initialize` / `thread/start` / `turn/start` /
  item events) — used by the native `codex app-server` subcommand.
- Legacy ACP (`initialize` / `session/new` / `session/prompt` /
  `session/update`) — used by the `codex-acp` wrapper binary, if present.

Quick start::

    import asyncio
    from acp_agents import create_agent

    async def main():
        agent = create_agent("codex")
        await agent.start()
        reply = await agent.chat("conversation-1", "hello!")
        print(reply)
        await agent.stop()

    asyncio.run(main())
"""

from .base import Agent, AgentInfo, default_workspace
from .codex_agent import CodexAppServerAgent
from .detect import CANDIDATES, AgentCandidate, DetectedAgent, detect, detect_all, which
from .factory import SUPPORTED_AGENTS, create_agent
from .jsonrpc import JsonRpcError, StdioJsonRpcConnection
from .legacy_acp import LegacyACPAgent

__all__ = [
    "Agent",
    "AgentInfo",
    "default_workspace",
    "CodexAppServerAgent",
    "LegacyACPAgent",
    "StdioJsonRpcConnection",
    "JsonRpcError",
    "AgentCandidate",
    "CANDIDATES",
    "DetectedAgent",
    "detect",
    "detect_all",
    "which",
    "create_agent",
    "SUPPORTED_AGENTS",
]
