"""Process-local registry for Agent runtimes and public descriptors."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Iterable, Mapping

from src.agents.base import AgentRuntime

from .policy import AgentProfile, PeerDescriptor

logger = logging.getLogger(__name__)

# Durable provenance marker shared by named-Agent restoration and migrations.
DYNAMIC_AGENT_SUMMARY = "Named Codex Agent created through /agent."


@dataclass(frozen=True, slots=True)
class AgentDescriptor:
    agent_id: str
    display_name: str
    summary: str = ""
    capabilities: frozenset[str] = frozenset()
    accepted_request_types: frozenset[str] = frozenset()
    enabled: bool = True
    default_mode_id: str = "chat"
    profile_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "capabilities", frozenset(self.capabilities or ()))
        object.__setattr__(self, "accepted_request_types", frozenset(self.accepted_request_types or ()))

    @classmethod
    def from_profile(cls, profile: AgentProfile) -> "AgentDescriptor":
        return cls(
            agent_id=profile.agent_id,
            display_name=profile.display_name or profile.agent_id,
            summary=profile.summary,
            capabilities=profile.capabilities,
            accepted_request_types=(
                profile.allowed_request_types - profile.denied_request_types
            ),
            enabled=profile.enabled,
            default_mode_id=profile.default_mode_id,
            profile_version=profile.profile_version,
        )

    def peer_descriptor(self) -> PeerDescriptor:
        return PeerDescriptor(
            agent_id=self.agent_id,
            display_name=self.display_name,
            summary=self.summary,
            capabilities=self.capabilities,
            accepted_request_types=self.accepted_request_types,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "summary": self.summary,
            "capabilities": sorted(self.capabilities),
            "accepted_request_types": sorted(self.accepted_request_types),
            "enabled": self.enabled,
            "default_mode_id": self.default_mode_id,
            "profile_version": self.profile_version,
        }


@dataclass(frozen=True, slots=True)
class RegisteredAgent:
    runtime: AgentRuntime
    descriptor: AgentDescriptor
    profile: AgentProfile | None = None

    @property
    def agent_id(self) -> str:
        return self.descriptor.agent_id


def codex_profile(
    *,
    profile_version: int = 1,
    default_mode_id: str = "chat",
    allow_dynamic_peers: bool = False,
) -> AgentProfile:
    """Return the static MVP Codex profile.

    Collaboration remains disabled unless trusted deployment wiring explicitly
    enables ``allow_dynamic_peers``.  That option is intended for the v3
    durable profile: it grants the narrow ``ask`` request type and uses the
    policy engine's explicit wildcard peer selector.  Generic embedders and
    historical v1/v2 snapshots therefore remain deny-by-default.
    """

    if allow_dynamic_peers and int(profile_version) < 3:
        raise ValueError(
            "dynamic peer collaboration requires profile_version >= 3"
        )
    allowed_peers = frozenset({"*"}) if allow_dynamic_peers else frozenset()
    allowed_request_types = (
        frozenset({"ask"}) if allow_dynamic_peers else frozenset()
    )

    return AgentProfile(
        agent_id="codex",
        display_name="Codex",
        summary="Software engineering Agent backed by the Codex SDK.",
        responsibilities=("answer questions", "inspect and modify the configured workspace"),
        constraints=("follow the selected mode and effective policy",),
        capabilities=frozenset({"read", "search", "list", "status", "diff", "write", "edit", "execute"}),
        allowed_peers=allowed_peers,
        denied_peers=frozenset(),
        allowed_request_types=allowed_request_types,
        max_child_depth=0,
        max_children_per_task=0,
        enabled=True,
        profile_version=profile_version,
        default_mode_id=default_mode_id,
    )


class AgentRegistry:
    """Runtime lookup owned by one asyncio event loop."""

    def __init__(
        self,
        agents: Mapping[str, AgentRuntime] | Iterable[RegisteredAgent] | None = None,
        *,
        default_agent_id: str | None = None,
    ) -> None:
        self._agents: dict[str, RegisteredAgent] = {}
        self._profiles: dict[tuple[str, int], AgentProfile] = {}
        self._started: set[str] = set()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self.default_agent_id = default_agent_id
        if agents:
            if isinstance(agents, Mapping):
                for agent_id, runtime in agents.items():
                    self.register(agent_id, runtime)
            else:
                for registration in agents:
                    self.register(
                        registration.agent_id,
                        registration.runtime,
                        profile=registration.profile,
                        descriptor=registration.descriptor,
                    )

    def _assert_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._owner_loop is not None:
                raise RuntimeError("AgentRegistry must be accessed from its owning asyncio loop")
            return
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("AgentRegistry must be accessed from its owning asyncio loop")

    def register(
        self,
        agent_id: str,
        runtime: AgentRuntime,
        *,
        profile: AgentProfile | None = None,
        descriptor: AgentDescriptor | None = None,
        replace: bool = False,
    ) -> RegisteredAgent:
        self._assert_loop()
        agent_id = str(agent_id).strip()
        if not agent_id:
            raise ValueError("agent_id is required")
        if agent_id in self._agents:
            if replace and agent_id in self._started:
                raise RuntimeError("cannot replace a started Agent runtime")
            if not replace:
                previous = self._agents[agent_id]
                if previous.runtime is runtime and (profile is None or profile == previous.profile):
                    return previous
                raise ValueError(f"Agent already registered: {agent_id}")
        if profile is not None and profile.agent_id != agent_id:
            raise ValueError("profile.agent_id must match registry agent_id")
        if profile is not None:
            profile_key = (agent_id, int(profile.profile_version))
            previous_profile = self._profiles.get(profile_key)
            if previous_profile is not None and previous_profile != profile:
                raise ValueError(
                    f"Agent profile version already registered: {agent_id}@{profile.profile_version}"
                )
            self._profiles[profile_key] = profile
        if descriptor is None:
            if profile is not None:
                descriptor = AgentDescriptor.from_profile(profile)
            else:
                descriptor = AgentDescriptor(agent_id=agent_id, display_name=agent_id)
        elif descriptor.agent_id != agent_id:
            raise ValueError("descriptor.agent_id must match registry agent_id")
        registration = RegisteredAgent(runtime=runtime, descriptor=descriptor, profile=profile)
        self._agents[agent_id] = registration
        if self.default_agent_id is None:
            self.default_agent_id = agent_id
        return registration

    add = register

    def register_profile(self, profile: AgentProfile) -> AgentProfile:
        if not isinstance(profile, AgentProfile):
            raise TypeError("profile must be an AgentProfile")
        key = (profile.agent_id, int(profile.profile_version))
        previous = self._profiles.get(key)
        if previous is not None and previous != profile:
            raise ValueError(f"Agent profile version already registered: {profile.agent_id}@{profile.profile_version}")
        self._profiles[key] = profile
        registration = self._agents.get(profile.agent_id)
        if registration is not None and registration.profile is None:
            self._agents[profile.agent_id] = RegisteredAgent(
                runtime=registration.runtime,
                descriptor=AgentDescriptor.from_profile(profile),
                profile=profile,
            )
        return profile

    def register_agent(
        self,
        runtime: AgentRuntime,
        *,
        agent_id: str | None = None,
        profile: AgentProfile | None = None,
        descriptor: AgentDescriptor | None = None,
        replace: bool = False,
    ) -> RegisteredAgent:
        """Convenience form accepting the runtime as the first argument."""

        resolved = agent_id or getattr(runtime, "agent_id", None) or (
            profile.agent_id if profile is not None else None
        )
        if not resolved:
            raise ValueError("agent_id is required when runtime has no agent_id")
        return self.register(
            str(resolved),
            runtime,
            profile=profile,
            descriptor=descriptor,
            replace=replace,
        )

    def unregister(
        self, agent_id: str, *, allow_started: bool = False
    ) -> RegisteredAgent | None:
        self._assert_loop()
        if agent_id in self._started and not allow_started:
            raise RuntimeError("cannot unregister a started Agent runtime")
        self._started.discard(agent_id)
        return self._agents.pop(agent_id, None)

    def is_started(self, agent_id: str) -> bool:
        self._assert_loop()
        return agent_id in self._started

    def get(self, agent_id: str | None = None) -> AgentRuntime | None:
        self._assert_loop()
        registration = self._agents.get(agent_id or self.default_agent_id or "")
        if registration is None or not registration.descriptor.enabled:
            return None
        return registration.runtime

    def require(self, agent_id: str | None = None) -> AgentRuntime:
        resolved = agent_id or self.default_agent_id
        runtime = self.get(resolved)
        if runtime is None:
            raise KeyError(f"unknown or disabled Agent: {resolved}")
        return runtime

    runtime_for = require

    def registration(self, agent_id: str | None = None) -> RegisteredAgent | None:
        self._assert_loop()
        return self._agents.get(agent_id or self.default_agent_id or "")

    def profile(self, agent_id: str | None = None, version: int | None = None) -> AgentProfile | None:
        registration = self.registration(agent_id)
        if version is not None:
            return self._profiles.get((agent_id or "", int(version)))
        return registration.profile if registration else self._profiles.get((agent_id or "", 1))

    def list_profiles(self, *, agent_id: str | None = None) -> tuple[AgentProfile, ...]:
        """Return immutable profiles known to the registry.

        ``profile()`` intentionally resolves one version for task routing.  A
        lifecycle owner needs the complete set when publishing definitions to
        a durable store, including older versions retained for queued tasks.
        Keep the result sorted and detached from the registry's mutable index.
        """

        self._assert_loop()
        if agent_id is None:
            values = self._profiles.values()
        else:
            values = (
                profile
                for (profile_agent_id, _), profile in self._profiles.items()
                if profile_agent_id == str(agent_id)
            )
        return tuple(
            sorted(values, key=lambda profile: (profile.agent_id, int(profile.profile_version)))
        )

    profiles = list_profiles

    def descriptor(self, agent_id: str | None = None) -> AgentDescriptor | None:
        registration = self.registration(agent_id)
        return registration.descriptor if registration else None

    get_descriptor = descriptor
    get_profile = profile

    def list(self, *, include_disabled: bool = False) -> tuple[AgentDescriptor, ...]:
        self._assert_loop()
        descriptors = (
            registration.descriptor
            for registration in self._agents.values()
            if include_disabled or registration.descriptor.enabled
        )
        return tuple(sorted(descriptors, key=lambda value: value.agent_id))

    list_agents = list
    descriptors = list

    async def start(self) -> None:
        self._assert_loop()
        started_here: list[str] = []
        attempted: str | None = None
        try:
            for agent_id, registration in self._agents.items():
                if agent_id in self._started or not registration.descriptor.enabled:
                    continue
                attempted = agent_id
                await registration.runtime.start()
                self._started.add(agent_id)
                started_here.append(agent_id)
                attempted = None
        except BaseException:
            # ``start`` itself may allocate a subprocess/transport and then
            # raise before the registry records it as started.  Stop that
            # attempted runtime as well as earlier successful registrations.
            cleanup = ([attempted] if attempted is not None else []) + list(
                reversed(started_here)
            )
            seen: set[str] = set()
            for agent_id in cleanup:
                if agent_id in seen:
                    continue
                seen.add(agent_id)
                try:
                    await self._agents[agent_id].runtime.stop()
                except BaseException:
                    # A failed process stop is an ownership uncertainty, not
                    # evidence that the runtime is gone.  Retain the started
                    # marker so an outer lifecycle owner can retry cleanup;
                    # otherwise ``stop()`` would skip the only remaining
                    # handle capable of reaping the child.
                    self._started.add(agent_id)
                    logger.debug(
                        "failed to roll back Agent runtime startup for %s",
                        agent_id,
                        exc_info=True,
                    )
                else:
                    self._started.discard(agent_id)
            raise

    async def stop(self) -> None:
        self._assert_loop()
        error: BaseException | None = None
        for agent_id in tuple(reversed(tuple(self._agents))):
            if agent_id not in self._started:
                continue
            try:
                await self._agents[agent_id].runtime.stop()
            except BaseException as exc:  # stop every runtime before propagating
                error = error or exc
            else:
                self._started.discard(agent_id)
        if error is not None:
            raise error

    async def interrupt(self, agent_id: str, task_id: str) -> bool:
        self._assert_loop()
        return await self.require(agent_id).interrupt(task_id)

    @property
    def started(self) -> bool:
        return bool(self._started)

    def __len__(self) -> int:
        return len(self._agents)

    def __contains__(self, agent_id: object) -> bool:
        return isinstance(agent_id, str) and agent_id in self._agents


__all__ = [
    "AgentDescriptor",
    "AgentRegistry",
    "RegisteredAgent",
    "Registry",
    "RuntimeRegistry",
    "codex_profile",
]

RuntimeRegistry = AgentRegistry
Registry = AgentRegistry
