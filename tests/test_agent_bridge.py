"""Focused coverage for task-scoped dynamic-Agent collaboration."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace

import pytest

from src.agents.base import AgentResult, ReplyTarget
from src.runtime.agent_bridge import AgentBridgeServer
from src.runtime.manager import TaskManager
from src.runtime.modes import AgentMode
from src.runtime.policy import AgentProfile, PolicyEngine
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


class _Runtime:
    agent_id = "codex"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit):
        return AgentResult(task_id=task.task_id, content="handled")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _target() -> ReplyTarget:
    return ReplyTarget(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        session_id="default",
    )


def _manager(
    database: Path,
    *,
    profile_version: int = 3,
    collaborative: bool = True,
    extra_profiles: tuple[AgentProfile, ...] = (),
) -> TaskManager:
    registry = AgentRegistry()
    runtime = _Runtime()
    registry.register(
        "codex",
        runtime,
        profile=codex_profile(
            profile_version=profile_version,
            default_mode_id="chat",
            allow_dynamic_peers=collaborative,
        ),
    )
    for profile in extra_profiles:
        registry.register(profile.agent_id, _Runtime(), profile=profile)
    return TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="chat",
        allow_dynamic_agents=True,
    )


async def _running_dynamic_task(manager: TaskManager, source: str = "planner"):
    await manager.ensure_agent(source)
    await manager.ensure_agent("reviewer")
    task = await manager.submit(
        "coordinate with another Agent",
        _target(),
        agent_id=source,
        mode_id="chat",
    )
    claim = await manager.store.claim_task_by_id(task.task_id, "bridge-test")
    assert claim is not None
    assert await manager.store.mark_task_running(
        task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    return await manager.get_task(task.task_id)


def test_wildcard_peer_policy_intersects_narrowly_and_denials_win():
    profile = codex_profile(
        profile_version=3,
        allow_dynamic_peers=True,
    )
    mode = AgentMode(mode_id="chat", can_send_agent_messages=True, policy_version=3)

    wildcard = PolicyEngine().effective_policy(profile, mode)
    assert wildcard.can_peer("planner")
    assert wildcard.can_request("ask")

    narrowed = PolicyEngine(
        hard_policy={"allowed_peers": {"planner"}}
    ).effective_policy(profile, mode)
    assert narrowed.can_peer("planner")
    assert not narrowed.can_peer("reviewer")

    exact_denial = PolicyEngine(
        hard_policy={"denied_peers": {"planner"}}
    ).effective_policy(profile, mode)
    assert not exact_denial.can_peer("planner")
    assert exact_denial.can_peer("reviewer")

    wildcard_denial = PolicyEngine(
        hard_policy={"denied_peers": {"*"}}
    ).effective_policy(profile, mode)
    assert not wildcard_denial.can_peer("planner")

    with pytest.raises(ValueError, match="profile_version >= 3"):
        codex_profile(profile_version=2, allow_dynamic_peers=True)


def test_two_dynamic_aliases_use_manager_mailbox_without_user_outbox(tmp_path):
    async def scenario() -> None:
        closed = AgentProfile(
            agent_id="closed",
            display_name="Closed",
            allowed_request_types=frozenset(),
        )
        manager = _manager(
            tmp_path / "runtime.sqlite", extra_profiles=(closed,)
        )
        await manager.start()
        try:
            task = await _running_dynamic_task(manager)
            assert task is not None
            assert task.agent_id == "planner"
            assert task.profile_version == 3
            assert task.policy_version == 3

            peers = await manager.list_agent_peers(task.task_id)
            peer_ids = {peer.agent_id for peer in peers}
            assert "reviewer" in peer_ids
            assert "closed" not in peer_ids
            assert "planner" not in peer_ids

            message = await manager.send_agent_message(
                "reviewer",
                "Please inspect the proposed change.",
                task_id=task.task_id,
                request_id="dynamic-agent-request",
                require_active_task=True,
            )
            assert message.source_agent_id == "planner"
            assert message.destination_agent_id == "reviewer"
            mailbox = await manager.store.list_mailbox("reviewer")
            assert [item.request_id for item in mailbox] == [
                "dynamic-agent-request"
            ]
            assert await manager.store.list_outbox() == []

            with pytest.raises(PermissionError, match="itself"):
                await manager.send_agent_message(
                    "planner",
                    "self request",
                    task_id=task.task_id,
                    request_id="self-request",
                    require_active_task=True,
                )
            with pytest.raises(PermissionError, match="unknown or disabled"):
                await manager.send_agent_message(
                    "missing",
                    "unknown request",
                    task_id=task.task_id,
                    request_id="missing-request",
                    require_active_task=True,
                )
            with pytest.raises(PermissionError, match="does not accept"):
                await manager.send_agent_message(
                    "closed",
                    "closed request",
                    task_id=task.task_id,
                    request_id="closed-request",
                    require_active_task=True,
                )

            queued = await manager.submit(
                "not running",
                _target(),
                agent_id="planner",
                mode_id="chat",
            )
            with pytest.raises(PermissionError, match="running task"):
                await manager.list_agent_peers(queued.task_id)
            with pytest.raises(PermissionError, match="running task"):
                await manager.send_agent_message(
                    "reviewer",
                    "must be rejected",
                    task_id=queued.task_id,
                    request_id="inactive-request",
                    require_active_task=True,
                )
            assert await manager.store.list_mailbox("reviewer") == mailbox
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_restored_dynamic_alias_upgrades_profile_without_rewriting_history(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = _manager(database, profile_version=2, collaborative=False)
        await first.start()
        try:
            assert await first.ensure_agent("planner")
            old_profile = await first.store.get_profile("planner", 2)
            assert old_profile is not None
            assert old_profile.allowed_peers == frozenset()
            assert old_profile.allowed_request_types == frozenset()
            await first.store.set_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
                mode_id="chat",
                policy_version=2,
            )

            old_task = await first.submit(
                "retain the old immutable snapshots",
                _target(),
                agent_id="planner",
                mode_id="chat",
            )
            assert old_task.profile_version == 2
            assert old_task.policy_version == 2
        finally:
            await first.stop()

        second = _manager(database, profile_version=3, collaborative=True)
        await second.start()
        try:
            retained = await second.store.get_profile("planner", 2)
            upgraded = await second.store.get_profile("planner", 3)
            assert retained == old_profile
            assert upgraded is not None
            assert upgraded.allowed_peers == frozenset({"*"})
            assert upgraded.allowed_request_types == frozenset({"ask"})
            registration = second.registry.registration("planner")
            assert registration is not None
            assert registration.profile == upgraded
            assert {
                profile.profile_version
                for profile in second.registry.list_profiles(agent_id="planner")
            } == {2, 3}
            assert await second.store.get_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
            ) == ("chat", 3)

            retained_task = await second.get_task(old_task.task_id)
            assert retained_task is not None
            assert retained_task.profile_version == 2
            assert retained_task.policy_version == 2
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_legacy_wildcard_profile_is_not_silently_promoted_to_chat_v3(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(database)
        await seeded.initialize()
        await seeded.set_session_mode(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            agent_id="legacy",
            mode_id="chat",
            policy_version=2,
        )
        await seeded.close()

        legacy = AgentProfile(
            agent_id="legacy",
            display_name="Legacy wildcard",
            allowed_peers=frozenset({"*"}),
            allowed_request_types=frozenset({"ask"}),
            profile_version=2,
            default_mode_id="chat",
        )
        manager = _manager(database, extra_profiles=(legacy,))
        await manager.start()
        try:
            assert await manager.store.get_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="legacy",
            ) == ("chat", 2)
            task = await manager.submit(
                "legacy policy remains immutable",
                _target(),
                agent_id="legacy",
                mode_id="chat",
            )
            assert task.profile_version == 2
            assert task.policy_version == 2
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_owner_only_socket_cli_lists_and_sends_for_running_task(tmp_path):
    async def invoke(*arguments: str, environment: dict[str, str] | None = None):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "src.agent_cli",
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        stdout, stderr = await process.communicate()
        return process.returncode, stdout.decode(), stderr.decode()

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite")
        await manager.start()
        bridge = AgentBridgeServer(manager, tmp_path / "bridge" / "agent.sock")
        try:
            task = await _running_dynamic_task(manager)
            assert task is not None
            await bridge.start()
            details = bridge.socket_path.stat()
            assert stat.S_IMODE(details.st_mode) == 0o600

            environment = os.environ.copy()
            environment["CODEX_WECHAT_AGENT_SOCKET"] = str(bridge.socket_path)
            environment["CODEX_WECHAT_TASK_ID"] = task.task_id
            environment["CODEX_WECHAT_AGENT_CAPABILITY"] = (
                bridge.capability_authority.issue(task)
            )

            code, stdout, stderr = await invoke("list", environment=environment)
            assert (code, stderr) == (0, "")
            listing = json.loads(stdout)
            assert "reviewer" in {
                value["agent_id"] for value in listing["agents"]
            }

            code, stdout, stderr = await invoke(
                "send",
                "reviewer",
                "Review this from the CLI",
                environment=environment,
            )
            assert (code, stderr) == (0, "")
            response = json.loads(stdout)
            assert response["ok"] is True
            assert response["destination_agent_id"] == "reviewer"
            request_id = response["request_id"]

            # A retry after a lost response derives the same logical ID and
            # resolves the already-committed mailbox row.
            code, replay_stdout, stderr = await invoke(
                "send",
                "reviewer",
                "Review this from the CLI",
                environment=environment,
            )
            assert (code, stderr) == (0, "")
            assert json.loads(replay_stdout)["request_id"] == request_id
            mailbox = await manager.store.list_mailbox("reviewer")
            assert [item.content for item in mailbox] == [
                "Review this from the CLI"
            ]
            assert await manager.store.list_outbox() == []

            forged_environment = dict(environment)
            forged_environment["CODEX_WECHAT_AGENT_CAPABILITY"] = "forged"
            code, _stdout, stderr = await invoke(
                "send",
                "reviewer",
                "must not be delivered",
                environment=forged_environment,
            )
            assert code == 1
            assert "permission_denied" in stderr

            cross_bound_environment = dict(environment)
            cross_bound_environment["CODEX_WECHAT_AGENT_CAPABILITY"] = (
                bridge.capability_authority.issue(
                    SimpleNamespace(
                        task_id="other-task",
                        execution_id="other-execution",
                        agent_id="intruder",
                    )
                )
            )
            code, _stdout, stderr = await invoke(
                "send",
                "reviewer",
                "cross-task impersonation",
                environment=cross_bound_environment,
            )
            assert code == 1
            assert "permission_denied" in stderr
            assert [item.content for item in await manager.store.list_mailbox(
                "reviewer"
            )] == ["Review this from the CLI"]
        finally:
            await bridge.stop()
            assert not bridge.socket_path.exists()
            await manager.stop()

    asyncio.run(scenario())
