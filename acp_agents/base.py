"""Shared types for ACP agent implementations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Protocol, runtime_checkable


def default_workspace() -> str:
    """Return `~/.acp_agents/workspace`, creating it if needed."""
    path = Path.home() / ".acp_agents" / "workspace"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


@dataclass
class AgentInfo:
    """Metadata about a running agent, for logging/debugging."""

    name: str
    type: str
    model: str = ""
    command: str = ""
    pid: int = 0

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        s = f"name={self.name}, type={self.type}, model={self.model}, command={self.command}"
        if self.pid:
            s += f", pid={self.pid}"
        return s


@runtime_checkable
class Agent(Protocol):
    """Interface implemented by every codex-wechat-bot ACP agent."""

    async def start(self) -> None:
        """Launch the underlying subprocess and complete the ACP handshake."""
        ...

    async def stop(self) -> None:
        """Terminate the underlying subprocess."""
        ...

    def chat(self, conversation_id: str, message: str) -> Awaitable[str]:
        """Send a message to the agent and return its reply.

        `conversation_id` is used to maintain a persistent session/thread per
        caller, so follow-up messages retain context.
        """
        ...

    def reset_session(self, conversation_id: str) -> Awaitable[str]:
        """Clear the session/thread for `conversation_id` and start a new one.

        Returns the new session/thread id.
        """
        ...

    def info(self) -> AgentInfo:
        """Return metadata about this agent."""
        ...

    def set_cwd(self, cwd: str) -> None:
        """Change the working directory used for subsequent sessions."""
        ...
