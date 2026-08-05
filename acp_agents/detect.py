"""Detect installed ACP-compatible agents for codex-wechat-bot."""

from __future__ import annotations

import platform
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AgentCandidate:
    """One way to run an agent. Multiple candidates can map to the same
    agent name; the first detected one (in list order) wins."""

    name: str
    binary: str
    args: list[str] = field(default_factory=list)
    check_args: Optional[list[str]] = None
    type: str = "acp"
    model: str = ""


# Ordered by priority: for each agent name, earlier entries are preferred
# (e.g. the dedicated `codex-acp` binary is tried before native
# `codex app-server`).
CANDIDATES: list[AgentCandidate] = [
    AgentCandidate(name="codex", binary="codex-acp", type="acp"),
    AgentCandidate(
        name="codex",
        binary="codex",
        args=["app-server", "--listen", "stdio://"],
        check_args=["app-server", "--help"],
        type="acp",
    ),
]


@dataclass
class DetectedAgent:
    name: str
    command: str
    args: list[str]
    type: str
    model: str


def which(binary: str) -> Optional[str]:
    """Find a binary by name. Tries `shutil.which` first (fast, uses the
    current PATH); falls back to resolving via a login shell, which sources
    the user's shell profile (`~/.zshrc`, `~/.bashrc`) and picks up binaries
    installed through version managers (nvm, mise, etc.) that only modify
    PATH in interactive shells."""
    path = shutil.which(binary)
    if path:
        return path

    shell = "zsh" if platform.system() == "Darwin" else "bash"
    try:
        result = subprocess.run(
            [shell, "-lic", f"which {binary}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None

    resolved = result.stdout.strip()
    if not resolved or "not found" in resolved:
        return None
    return resolved


def _probe(binary: str, args: list[str]) -> bool:
    try:
        result = subprocess.run([binary, *args], capture_output=True, timeout=3)
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def detect(name: str) -> Optional[DetectedAgent]:
    """Detect the highest-priority installed candidate for `name`."""
    for candidate in CANDIDATES:
        if candidate.name != name:
            continue
        path = which(candidate.binary)
        if not path:
            continue
        if candidate.check_args and not _probe(path, candidate.check_args):
            continue
        return DetectedAgent(
            name=name,
            command=path,
            args=list(candidate.args),
            type=candidate.type,
            model=candidate.model,
        )
    return None


def detect_all() -> dict[str, DetectedAgent]:
    """Detect every supported agent installed on this machine."""
    found: dict[str, DetectedAgent] = {}
    for candidate in CANDIDATES:
        if candidate.name in found:
            continue
        detected = detect(candidate.name)
        if detected:
            found[candidate.name] = detected
    return found
