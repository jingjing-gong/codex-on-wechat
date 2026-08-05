"""Factory for creating ACP agent instances (currently: codex only)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import Agent
from .codex_agent import CodexAppServerAgent
from .detect import detect as detect_agent
from .legacy_acp import LegacyACPAgent

SUPPORTED_AGENTS = ("codex",)


def _uses_codex_app_server(command: str, args: list[str]) -> bool:
    """Return whether a command uses the native Codex app-server protocol.

    Only the native `codex` binary invoked with `app-server` uses Codex's own
    protocol. `codex-acp` is a standard ACP wrapper and uses the generic
    protocol instead.
    """
    base = Path(command).name.lower()
    return base in ("codex", "codex.exe") and "app-server" in args


def create_agent(
    name: str,
    *,
    command: Optional[str] = None,
    args: Optional[list[str]] = None,
    model: str = "",
    cwd: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> Agent:
    """Create an ACP-based agent for `codex`.

    If `command` is not given, auto-detects the binary on PATH (see
    `acp_agents.detect`). Raises `RuntimeError` if no suitable binary can be
    found.
    """
    if command is None:
        detected = detect_agent(name)
        if detected is None:
            raise RuntimeError(
                f"could not find an ACP-compatible binary for agent '{name}' on PATH. "
                f"Supported agents: {', '.join(SUPPORTED_AGENTS)}"
            )
        command = detected.command
        args = args if args is not None else detected.args
        model = model or detected.model

    args = args or []

    if _uses_codex_app_server(command, args):
        return CodexAppServerAgent(
            command=command, args=args, model=model, cwd=cwd, env=env
        )

    return LegacyACPAgent(
        command=command, args=args, name=name, model=model, cwd=cwd, env=env
    )
