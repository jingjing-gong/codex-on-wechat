"""Codex SDK integration for codex-wechat-bot."""

from .codex_agent import AgentInfo, CodexAgent

# New durable runtime contracts are exported alongside the legacy interactive
# adapter so existing integrations continue to import ``src.CodexAgent`` while
# newer wiring can use ``src.agents.CodexRuntime``.
from .agents.codex_runtime import CodexRuntime
from .agents.base import AgentEvent, AgentResult, AgentRuntime, AgentTask, ReplyTarget

__all__ = [
    "AgentInfo",
    "CodexAgent",
    "CodexRuntime",
    "AgentEvent",
    "AgentResult",
    "AgentRuntime",
    "AgentTask",
    "ReplyTarget",
]
