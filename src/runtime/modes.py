"""Application Agent modes.

Modes are domain values.  They intentionally use string sandbox and approval
policies; translation to ``openai_codex`` enums belongs solely to the Codex
adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class AgentMode:
    mode_id: str
    developer_instructions: str = ""
    sandbox_policy: str = "read-only"
    approval_policy: str = "deny_all"
    allowed_tools: frozenset[str] = frozenset()
    denied_tools: frozenset[str] = frozenset()
    can_write_files: bool = False
    can_execute_commands: bool = False
    can_create_child_tasks: bool = False
    can_send_agent_messages: bool = False
    policy_version: int = 1

    def __post_init__(self) -> None:
        # Normalize collection inputs while retaining an immutable snapshot.
        allowed = (self.allowed_tools,) if isinstance(self.allowed_tools, str) else self.allowed_tools
        denied = (self.denied_tools,) if isinstance(self.denied_tools, str) else self.denied_tools
        object.__setattr__(self, "allowed_tools", frozenset(allowed or ()))
        object.__setattr__(self, "denied_tools", frozenset(denied or ()))

    @property
    def version(self) -> int:
        return self.policy_version

    def allows_tool(self, tool: str) -> bool:
        """Return whether this mode permits ``tool`` before profile policy."""

        name = str(tool).strip()
        if not name or name in self.denied_tools:
            return False
        # An empty allow-list means "no explicitly constrained tools" in the
        # initial modes.  A non-empty list is an allow-list.
        return not self.allowed_tools or name in self.allowed_tools

    def with_version(self, version: int) -> "AgentMode":
        return replace(self, policy_version=version)

    def as_dict(self) -> dict[str, object]:
        return {
            "mode_id": self.mode_id,
            "developer_instructions": self.developer_instructions,
            "sandbox_policy": self.sandbox_policy,
            "approval_policy": self.approval_policy,
            "allowed_tools": sorted(self.allowed_tools),
            "denied_tools": sorted(self.denied_tools),
            "can_write_files": self.can_write_files,
            "can_execute_commands": self.can_execute_commands,
            "can_create_child_tasks": self.can_create_child_tasks,
            "can_send_agent_messages": self.can_send_agent_messages,
            "policy_version": self.policy_version,
        }


def builtin_modes() -> dict[str, AgentMode]:
    """Return fresh immutable definitions for the four initial modes."""

    read_tools = frozenset({"read", "search", "list", "status"})
    return {
        "chat": AgentMode(
            mode_id="chat",
            developer_instructions=(
                "Hold a conversation and provide analysis. Commands and network "
                "access are available when needed, but do not modify files."
            ),
            sandbox_policy="full-access",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"execute"},
            can_execute_commands=True,
            policy_version=2,
        ),
        "plan": AgentMode(
            mode_id="plan",
            developer_instructions=(
                "Inspect context and produce an implementation plan. Commands "
                "and network access are available, but do not modify files."
            ),
            sandbox_policy="full-access",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"delegate", "execute"},
            can_execute_commands=True,
            can_create_child_tasks=True,
            can_send_agent_messages=True,
            policy_version=2,
        ),
        "review": AgentMode(
            mode_id="review",
            developer_instructions=(
                "Review code and diffs. Commands and network access are available; "
                "report concrete defects and missing tests without modifying files."
            ),
            sandbox_policy="full-access",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"diff", "execute"},
            can_execute_commands=True,
            can_send_agent_messages=True,
            policy_version=2,
        ),
        "execute": AgentMode(
            mode_id="execute",
            developer_instructions=(
                "Implement the approved work in the workspace and report the "
                "result succinctly."
            ),
            # Execute is the explicitly authorized unrestricted mode.  The
            # SDK's workspace-write sandbox blocks network access, which makes
            # package installation and other normal execution tasks fail.
            sandbox_policy="full-access",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"diff", "write", "edit", "execute"},
            can_write_files=True,
            can_execute_commands=True,
            can_create_child_tasks=True,
            can_send_agent_messages=True,
            policy_version=2,
        ),
    }


def collaborative_builtin_modes() -> dict[str, AgentMode]:
    """Return newer built-ins introduced for explicit Agent collaboration.

    Keep the complete v2 catalog in :func:`builtin_modes` so tasks already
    snapshotted at that version remain resolvable.  Chat v3 only opens the
    mode-level messaging gate; a Profile still needs an explicit peer and
    request-type grant before the effective policy permits a message.
    """

    return {
        "chat": replace(
            builtin_modes()["chat"],
            can_send_agent_messages=True,
            policy_version=3,
        )
    }


def legacy_builtin_modes() -> dict[str, AgentMode]:
    """Version-1 built-ins retained for persisted tasks and session routes."""

    read_tools = frozenset({"read", "search", "list", "status"})
    return {
        "chat": AgentMode(
            mode_id="chat",
            developer_instructions=(
                "Hold a conversation and provide analysis. Do not modify files "
                "or execute commands."
            ),
            sandbox_policy="read-only",
            approval_policy="deny_all",
            allowed_tools=read_tools,
        ),
        "plan": AgentMode(
            mode_id="plan",
            developer_instructions=(
                "Inspect context and produce an implementation plan. Keep the "
                "workspace read-only."
            ),
            sandbox_policy="read-only",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"delegate"},
            can_create_child_tasks=True,
            can_send_agent_messages=True,
        ),
        "review": AgentMode(
            mode_id="review",
            developer_instructions=(
                "Review code and diffs. Report concrete defects and missing tests; "
                "do not modify the workspace."
            ),
            sandbox_policy="read-only",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"diff"},
            can_send_agent_messages=True,
        ),
        "execute": AgentMode(
            mode_id="execute",
            developer_instructions=(
                "Implement the approved work in the workspace and report the "
                "result succinctly."
            ),
            sandbox_policy="workspace-write",
            approval_policy="deny_all",
            allowed_tools=read_tools | {"diff", "write", "edit", "execute"},
            can_write_files=True,
            can_execute_commands=True,
            can_create_child_tasks=True,
            can_send_agent_messages=True,
        ),
    }


class ModeRegistry:
    """In-memory registry of immutable mode versions.

    The durable store may persist definitions separately; this registry is a
    process-local lookup used by task routing and runtime adapters.  Replacing
    a mode with a different value at an existing version is rejected to avoid
    changing policy for already-created tasks.
    """

    def __init__(
        self,
        modes: Iterable[AgentMode] | Mapping[str, AgentMode] | None = None,
        *,
        include_builtins: bool = True,
    ) -> None:
        self._modes: dict[tuple[str, int], AgentMode] = {}
        self._latest: dict[str, int] = {}
        if include_builtins:
            for mode in legacy_builtin_modes().values():
                self.register(mode)
            for mode in builtin_modes().values():
                self.register(mode)
            for mode in collaborative_builtin_modes().values():
                self.register(mode)
        if modes:
            values = modes.values() if isinstance(modes, Mapping) else modes
            for mode in values:
                self.register(mode)

    def register(self, mode: AgentMode | Mapping[str, object], *, replace_latest: bool = False) -> AgentMode:
        if isinstance(mode, Mapping):
            mode = AgentMode(**dict(mode))
        if not isinstance(mode, AgentMode):
            raise TypeError("mode must be an AgentMode")
        key = (mode.mode_id, int(mode.policy_version))
        previous = self._modes.get(key)
        if previous is not None and previous != mode:
            raise ValueError(f"mode version already registered: {mode.mode_id}@{mode.policy_version}")
        self._modes[key] = mode
        latest = self._latest.get(mode.mode_id)
        if latest is None or replace_latest or mode.policy_version > latest:
            self._latest[mode.mode_id] = int(mode.policy_version)
        return mode

    add = register

    def get(self, mode_id: str, version: int | None = None) -> AgentMode | None:
        mode_id = str(mode_id)
        if version is None:
            version = self._latest.get(mode_id)
        if version is None:
            return None
        return self._modes.get((mode_id, int(version)))

    def require(self, mode_id: str, version: int | None = None) -> AgentMode:
        mode = self.get(mode_id, version)
        if mode is None:
            suffix = "" if version is None else f"@{version}"
            raise KeyError(f"unknown Agent mode: {mode_id}{suffix}")
        return mode

    def versions(self, mode_id: str) -> tuple[int, ...]:
        return tuple(sorted(version for ident, version in self._modes if ident == mode_id))

    def list_all(self) -> tuple[AgentMode, ...]:
        """Return every immutable mode version, not only each mode's latest.

        ``list()`` is kept as the routing-oriented latest-version view.  Store
        initialization uses this complete view so an older policy referenced
        by a queued task remains available after a restart.
        """

        return tuple(
            self._modes[key]
            for key in sorted(self._modes, key=lambda value: (value[0], value[1]))
        )

    definitions = list_all

    def list(self) -> tuple[AgentMode, ...]:
        return tuple(
            self._modes[(mode_id, version)]
            for mode_id, version in sorted(self._latest.items())
        )

    def __contains__(self, mode_id: object) -> bool:
        return isinstance(mode_id, str) and mode_id in self._latest

    def __iter__(self):
        return iter(self.list())


Mode = AgentMode


__all__ = ["AgentMode", "Mode", "ModeRegistry", "builtin_modes", "legacy_builtin_modes"]
