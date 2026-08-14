"""Agent Profile and permission policy evaluation.

Profiles and modes are descriptive configuration, not security boundaries.
``PolicyEngine`` performs the final intersection and makes denial precedence
explicit so a permissive mode cannot elevate a restrictive profile.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .modes import AgentMode


def _frozen(values: Iterable[str] | None) -> frozenset[str]:
    if isinstance(values, str):
        values = (values,)
    return frozenset(str(value).strip() for value in (values or ()) if str(value).strip())


@dataclass(frozen=True, slots=True)
class PeerDescriptor:
    """Public, non-sensitive description exposed to another Agent."""

    agent_id: str
    display_name: str
    summary: str = ""
    capabilities: frozenset[str] = frozenset()
    accepted_request_types: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "capabilities", _frozen(self.capabilities))
        object.__setattr__(self, "accepted_request_types", _frozen(self.accepted_request_types))


@dataclass(frozen=True, slots=True)
class AgentProfile:
    agent_id: str
    display_name: str = ""
    summary: str = ""
    system_prompt: str = ""
    responsibilities: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    capabilities: frozenset[str] = frozenset()
    allowed_peers: frozenset[str] = frozenset()
    denied_peers: frozenset[str] = frozenset()
    allowed_request_types: frozenset[str] = frozenset()
    denied_request_types: frozenset[str] = frozenset()
    max_child_depth: int = 0
    max_children_per_task: int = 0
    enabled: bool = True
    profile_version: int = 1
    default_mode_id: str = "chat"

    def __post_init__(self) -> None:
        object.__setattr__(self, "responsibilities", tuple(self.responsibilities or ()))
        object.__setattr__(self, "constraints", tuple(self.constraints or ()))
        object.__setattr__(self, "capabilities", _frozen(self.capabilities))
        object.__setattr__(self, "allowed_peers", _frozen(self.allowed_peers))
        object.__setattr__(self, "denied_peers", _frozen(self.denied_peers))
        object.__setattr__(self, "allowed_request_types", _frozen(self.allowed_request_types))
        object.__setattr__(self, "denied_request_types", _frozen(self.denied_request_types))
        if self.max_child_depth < 0 or self.max_children_per_task < 0:
            raise ValueError("child limits cannot be negative")

    @property
    def version(self) -> int:
        return self.profile_version

    def descriptor(self) -> PeerDescriptor:
        return PeerDescriptor(
            agent_id=self.agent_id,
            display_name=self.display_name,
            summary=self.summary,
            capabilities=self.capabilities,
            accepted_request_types=(
                self.allowed_request_types - self.denied_request_types
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "summary": self.summary,
            "system_prompt": self.system_prompt,
            "responsibilities": list(self.responsibilities),
            "constraints": list(self.constraints),
            "capabilities": sorted(self.capabilities),
            "allowed_peers": sorted(self.allowed_peers),
            "denied_peers": sorted(self.denied_peers),
            "allowed_request_types": sorted(self.allowed_request_types),
            "denied_request_types": sorted(self.denied_request_types),
            "max_child_depth": self.max_child_depth,
            "max_children_per_task": self.max_children_per_task,
            "enabled": self.enabled,
            "profile_version": self.profile_version,
            "default_mode_id": self.default_mode_id,
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str = ""
    code: str = ""

    def __bool__(self) -> bool:
        return self.allowed

    def require(self) -> None:
        if not self.allowed:
            raise PermissionError(self.reason or "operation denied")


@dataclass(frozen=True, slots=True)
class EffectivePolicy:
    """The concrete, task-snapshotted permissions."""

    profile_id: str
    profile_version: int
    mode_id: str
    mode_policy_version: int
    policy_version: int
    sandbox_policy: str
    approval_policy: str
    developer_instructions: str
    allowed_tools: frozenset[str] = frozenset()
    denied_tools: frozenset[str] = frozenset()
    allowed_peers: frozenset[str] = frozenset()
    denied_peers: frozenset[str] = frozenset()
    allowed_request_types: frozenset[str] = frozenset()
    denied_request_types: frozenset[str] = frozenset()
    can_write_files: bool = False
    can_execute_commands: bool = False
    can_create_child_tasks: bool = False
    can_send_agent_messages: bool = False
    max_child_depth: int = 0
    max_children_per_task: int = 0

    def __post_init__(self) -> None:
        # SQLite JSON snapshots deserialize these fields as lists. Normalize
        # them back to immutable sets before the policy is consulted so a
        # reconstructed task policy cannot be mutated after authorization.
        for name in (
            "allowed_tools",
            "denied_tools",
            "allowed_peers",
            "denied_peers",
            "allowed_request_types",
            "denied_request_types",
        ):
            object.__setattr__(self, name, _frozen(getattr(self, name)))

    def allows_tool(self, tool: str) -> bool:
        name = str(tool).strip()
        if not name or name in self.denied_tools:
            return False
        return name in self.allowed_tools

    def can_peer(self, peer_id: str) -> bool:
        peer_id = str(peer_id).strip()
        # Missing peer permission is deny-by-default (plan invariant 8).
        return bool(peer_id and peer_id in self.allowed_peers and peer_id not in self.denied_peers)

    def can_request(self, request_type: str) -> bool:
        request_type = str(request_type).strip()
        return bool(
            request_type
            and request_type in self.allowed_request_types
            and request_type not in self.denied_request_types
        )

    def as_dict(self) -> dict[str, Any]:
        result = {
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "mode_id": self.mode_id,
            "mode_policy_version": self.mode_policy_version,
            "policy_version": self.policy_version,
            "sandbox_policy": self.sandbox_policy,
            "approval_policy": self.approval_policy,
            "developer_instructions": self.developer_instructions,
            "allowed_tools": sorted(self.allowed_tools),
            "denied_tools": sorted(self.denied_tools),
            "allowed_peers": sorted(self.allowed_peers),
            "denied_peers": sorted(self.denied_peers),
            "allowed_request_types": sorted(self.allowed_request_types),
            "denied_request_types": sorted(self.denied_request_types),
            "can_write_files": self.can_write_files,
            "can_execute_commands": self.can_execute_commands,
            "can_create_child_tasks": self.can_create_child_tasks,
            "can_send_agent_messages": self.can_send_agent_messages,
            "max_child_depth": self.max_child_depth,
            "max_children_per_task": self.max_children_per_task,
        }
        return result


@dataclass(frozen=True, slots=True)
class _Restriction:
    """Internal normalized administrator/task restriction."""

    allowed_tools: frozenset[str] | None = None
    denied_tools: frozenset[str] = frozenset()
    allowed_peers: frozenset[str] | None = None
    denied_peers: frozenset[str] = frozenset()
    allowed_request_types: frozenset[str] | None = None
    denied_request_types: frozenset[str] = frozenset()
    can_write_files: bool | None = None
    can_execute_commands: bool | None = None
    can_create_child_tasks: bool | None = None
    can_send_agent_messages: bool | None = None


def _restriction(value: Any) -> _Restriction:
    if value is None:
        return _Restriction()
    if isinstance(value, _Restriction):
        return value
    if isinstance(value, EffectivePolicy):
        return _Restriction(
            allowed_tools=value.allowed_tools,
            denied_tools=value.denied_tools,
            allowed_peers=value.allowed_peers,
            denied_peers=value.denied_peers,
            allowed_request_types=value.allowed_request_types,
            denied_request_types=value.denied_request_types,
            can_write_files=value.can_write_files,
            can_execute_commands=value.can_execute_commands,
            can_create_child_tasks=value.can_create_child_tasks,
            can_send_agent_messages=value.can_send_agent_messages,
        )
    if isinstance(value, Mapping):
        get = value.get
    else:
        get = lambda key, default=None: getattr(value, key, default)

    def allowed(name: str) -> frozenset[str] | None:
        result = get(name)
        return None if result is None else _frozen(result)

    return _Restriction(
        allowed_tools=allowed("allowed_tools"),
        denied_tools=_frozen(get("denied_tools")),
        allowed_peers=allowed("allowed_peers"),
        denied_peers=_frozen(get("denied_peers")),
        allowed_request_types=allowed("allowed_request_types"),
        denied_request_types=_frozen(get("denied_request_types")),
        can_write_files=get("can_write_files"),
        can_execute_commands=get("can_execute_commands"),
        can_create_child_tasks=get("can_create_child_tasks"),
        can_send_agent_messages=get("can_send_agent_messages"),
    )


class PolicyEngine:
    """Combine hard, administrator, profile, mode, and task restrictions.

    Every layer can narrow a permission.  For set-valued permissions an
    allow-list is intersected with the existing allow-list; deny-lists are
    unioned.  A denial therefore always wins, including when a value appears in
    both an allow and deny list.
    """

    def __init__(self, *, hard_policy: Any | None = None, policy_version: int = 1) -> None:
        self.hard_policy = _restriction(hard_policy)
        self.policy_version = int(policy_version)

    @staticmethod
    def _intersect_allow(current: frozenset[str] | None, incoming: frozenset[str] | None) -> frozenset[str] | None:
        if incoming is None:
            return current
        if current is None:
            return incoming
        return current & incoming

    def effective_policy(
        self,
        profile: AgentProfile | Mapping[str, Any],
        mode: AgentMode | Mapping[str, Any],
        *,
        administrator_policy: Any | None = None,
        task_policy: Any | None = None,
    ) -> EffectivePolicy:
        if not isinstance(profile, AgentProfile):
            profile = AgentProfile(**dict(profile))
        if not isinstance(mode, AgentMode):
            mode = AgentMode(**dict(mode))
        if not profile.enabled:
            raise PermissionError(f"Agent profile disabled: {profile.agent_id}")

        admin = _restriction(administrator_policy)
        task = _restriction(task_policy)

        # Profile/mode defaults are themselves restrictions.  An empty profile
        # allow-list means no explicit restriction, while mode's non-empty list
        # acts as an allow-list.
        allowed_tools: frozenset[str] | None = None
        # Peer and request ACLs are deny-by-default. Unlike a tool capability
        # allow-list, an empty profile ACL is an explicit absence of authority
        # and a broader administrator/task layer must not be able to grant it.
        allowed_peers: frozenset[str] | None = profile.allowed_peers
        allowed_requests: frozenset[str] | None = profile.allowed_request_types
        denied_tools: set[str] = set()
        denied_tools.update(mode.denied_tools)
        denied_peers = set(profile.denied_peers)
        denied_requests: set[str] = set(profile.denied_request_types)

        for restriction in (self.hard_policy, admin, task):
            allowed_tools = self._intersect_allow(allowed_tools, restriction.allowed_tools)
            denied_tools.update(restriction.denied_tools)
            allowed_peers = self._intersect_allow(allowed_peers, restriction.allowed_peers)
            denied_peers.update(restriction.denied_peers)
            allowed_requests = self._intersect_allow(allowed_requests, restriction.allowed_request_types)
            denied_requests.update(restriction.denied_request_types)

        # Non-empty mode allow-lists are narrowed by intersection.  The final
        # effective policy remains deny-by-default when every layer supplies an
        # empty allow-list.
        allowed_tools = self._intersect_allow(allowed_tools, mode.allowed_tools or None)
        # A profile capability list, when supplied, is an allow-list for tools.
        if profile.capabilities:
            allowed_tools = self._intersect_allow(allowed_tools, profile.capabilities)

        # Boolean capabilities narrow monotonically.  Hard/admin/task False
        # values override profile/mode True values; a True value never elevates.
        can_write = bool(mode.can_write_files)
        can_execute = bool(mode.can_execute_commands)
        can_children = bool(mode.can_create_child_tasks)
        can_messages = bool(mode.can_send_agent_messages)

        # Profile capabilities and limits are an additional upper bound.  The
        # mode describes what a task would like to do, while the profile says
        # what this Agent is allowed to do at all.  Empty capability sets are
        # retained as the historical "no explicit tool restriction" value;
        # once a profile supplies capabilities, writable/command execution is
        # granted only by the corresponding capability.  Collaboration and
        # child-task permissions are deny-by-default from their ACL/limit
        # fields, independent of the descriptive capability list.
        if profile.capabilities:
            capabilities = {str(value).strip().lower() for value in profile.capabilities}
            can_write = can_write and bool(capabilities & {"write", "edit", "filesystem"})
            can_execute = can_execute and bool(
                capabilities & {"execute", "command", "commands", "shell"}
            )
        can_children = can_children and profile.max_child_depth > 0 and profile.max_children_per_task > 0
        can_messages = can_messages and bool(
            profile.allowed_peers and profile.allowed_request_types
        )
        for restriction in (self.hard_policy, admin, task):
            if restriction.can_write_files is not None and not bool(restriction.can_write_files):
                can_write = False
            if restriction.can_execute_commands is not None and not bool(restriction.can_execute_commands):
                can_execute = False
            if restriction.can_create_child_tasks is not None and not bool(restriction.can_create_child_tasks):
                can_children = False
            if restriction.can_send_agent_messages is not None and not bool(restriction.can_send_agent_messages):
                can_messages = False

        # A read-only (or unknown/malformed) sandbox is authoritative even if a
        # mode claims it can write or execute. Unknown values fail closed rather
        # than accidentally inheriting command execution from a permissive
        # boolean flag.
        sandbox = str(mode.sandbox_policy).lower().replace("_", "-")
        if sandbox not in {"read-only", "workspace-write", "full-access"}:
            can_write = False
            can_execute = False
            sandbox = "read-only"
        if sandbox == "read-only":
            can_write = False
            can_execute = False

        # Remove explicit denials from all allow-lists at the final boundary.
        allowed_tools_final = frozenset(allowed_tools or ()) - frozenset(denied_tools)
        allowed_peers_final = frozenset(allowed_peers or ()) - frozenset(denied_peers)
        allowed_requests_final = frozenset(allowed_requests or ()) - frozenset(denied_requests)
        return EffectivePolicy(
            profile_id=profile.agent_id,
            profile_version=int(profile.profile_version),
            mode_id=mode.mode_id,
            mode_policy_version=int(mode.policy_version),
            policy_version=self.policy_version,
            sandbox_policy=sandbox,
            approval_policy=mode.approval_policy,
            developer_instructions=mode.developer_instructions,
            allowed_tools=allowed_tools_final,
            denied_tools=frozenset(denied_tools),
            allowed_peers=allowed_peers_final,
            denied_peers=frozenset(denied_peers),
            allowed_request_types=allowed_requests_final,
            denied_request_types=frozenset(denied_requests),
            can_write_files=can_write,
            can_execute_commands=can_execute,
            can_create_child_tasks=can_children,
            can_send_agent_messages=can_messages,
            max_child_depth=profile.max_child_depth,
            max_children_per_task=profile.max_children_per_task,
        )

    build_effective_policy = effective_policy
    effective = effective_policy

    def check_tool(self, policy: EffectivePolicy, tool: str) -> PolicyDecision:
        if policy.allows_tool(tool):
            return PolicyDecision(True)
        return PolicyDecision(False, f"tool not permitted: {tool}", "tool_denied")

    def check_peer(self, policy: EffectivePolicy, peer_id: str) -> PolicyDecision:
        if not policy.can_send_agent_messages:
            return PolicyDecision(False, "Agent messages are disabled in this mode", "collaboration_denied")
        if policy.can_peer(peer_id):
            return PolicyDecision(True)
        return PolicyDecision(False, f"peer not permitted: {peer_id}", "peer_denied")

    def check_request_type(self, policy: EffectivePolicy, request_type: str) -> PolicyDecision:
        if policy.can_request(request_type):
            return PolicyDecision(True)
        return PolicyDecision(False, f"request type not permitted: {request_type}", "request_denied")

    def check_child_task(
        self,
        policy: EffectivePolicy,
        *,
        child_depth: int,
        existing_children: int = 0,
    ) -> PolicyDecision:
        if not policy.can_create_child_tasks:
            return PolicyDecision(False, "child tasks are disabled", "child_tasks_denied")
        if int(child_depth) > int(policy.max_child_depth):
            return PolicyDecision(False, "maximum child-task depth exceeded", "child_depth_exceeded")
        if int(existing_children) >= int(policy.max_children_per_task):
            return PolicyDecision(False, "maximum child-task count exceeded", "child_count_exceeded")
        return PolicyDecision(True)

    def authorize_mode(self, mode: AgentMode, *, actor: str, explicit: bool = False) -> PolicyDecision:
        """Require explicit authorization for the writable ``execute`` mode."""

        mode_id = str(mode.mode_id).strip().lower()
        if mode_id == "execute" and not explicit:
            return PolicyDecision(False, "execute mode requires explicit authorization", "mode_authorization_required")
        if mode_id == "execute" and not actor:
            return PolicyDecision(False, "an authorization actor is required", "actor_required")
        return PolicyDecision(True)

    def authorize_collaboration(
        self,
        policy: EffectivePolicy,
        *,
        peer_id: str,
        request_type: str,
    ) -> PolicyDecision:
        """Check the mode capability, peer ACL, and request type together."""
        peer_decision = self.check_peer(policy, peer_id)
        if not peer_decision.allowed:
            return peer_decision
        request_decision = self.check_request_type(policy, request_type)
        if not request_decision.allowed:
            return request_decision
        return PolicyDecision(True)

    check_agent_message = authorize_collaboration

    # Concise aliases used by command routers.
    can_use_tool = check_tool
    can_collaborate = check_peer
    check_collaboration = check_peer


__all__ = [
    "AgentProfile",
    "EffectivePolicy",
    "PeerDescriptor",
    "PermissionPolicy",
    "Profile",
    "PolicyDecision",
    "PolicyEngine",
]

Profile = AgentProfile
PermissionPolicy = PolicyEngine
