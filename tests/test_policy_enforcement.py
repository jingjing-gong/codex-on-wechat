"""Focused policy, ACL, and immutable-task authorization regressions."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.manager import TaskManager
from src.runtime.modes import AgentMode
from src.runtime.models import ReplyTarget
from src.runtime.policy import AgentProfile, EffectivePolicy, PolicyEngine
from src.runtime.registry import AgentRegistry
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.worker import AgentMailboxWorker


class _Runtime:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, emit):
        return None

    async def interrupt(self, task_id: str) -> bool:
        return False


def _profile(
    agent_id: str,
    *,
    allowed_peers: set[str] | frozenset[str] = frozenset(),
    allowed_request_types: set[str] | frozenset[str] = frozenset(),
    denied_request_types: set[str] | frozenset[str] = frozenset(),
    max_children: int = 0,
) -> AgentProfile:
    return AgentProfile(
        agent_id=agent_id,
        capabilities=frozenset({"read", "delegate"}),
        allowed_peers=frozenset(allowed_peers),
        allowed_request_types=frozenset(allowed_request_types),
        denied_request_types=frozenset(denied_request_types),
        max_child_depth=2 if max_children else 0,
        max_children_per_task=max_children,
        default_mode_id="plan" if max_children else "chat",
    )


def _registry(*profiles: AgentProfile) -> AgentRegistry:
    registry = AgentRegistry()
    for profile in profiles:
        registry.register(profile.agent_id, _Runtime(), profile=profile)
    return registry


def _target() -> ReplyTarget:
    return ReplyTarget(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        session_id="default",
    )


def test_effective_policy_is_immutable_and_missing_acl_cannot_be_granted():
    policy = EffectivePolicy(
        profile_id="source",
        profile_version=1,
        mode_id="plan",
        mode_policy_version=1,
        policy_version=1,
        sandbox_policy="read-only",
        approval_policy="deny_all",
        developer_instructions="",
        allowed_tools=["read"],
        denied_tools=["write"],
        allowed_peers=["destination"],
    )
    assert policy.allowed_tools == frozenset({"read"})
    assert policy.allowed_peers == frozenset({"destination"})

    engine = PolicyEngine(
        hard_policy={
            "allowed_peers": {"destination"},
            "allowed_request_types": {"ask"},
            "can_send_agent_messages": True,
        }
    )
    effective = engine.effective_policy(
        AgentProfile(agent_id="source"),
        AgentMode(
            mode_id="plan",
            can_send_agent_messages=True,
        ),
    )
    assert not effective.can_peer("destination")
    assert not effective.can_request("ask")
    assert not engine.authorize_collaboration(
        effective, peer_id="destination", request_type="ask"
    )


def test_agent_messages_use_the_originating_task_policy_snapshot(tmp_path):
    async def scenario() -> None:
        source = _profile(
            "source",
            allowed_peers={"destination"},
            allowed_request_types={"ask"},
        )
        destination = _profile(
            "destination",
            allowed_request_types={"ask"},
        )
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = TaskManager(
            store,
            _registry(source, destination),
            worker_count=0,
            default_agent_id="source",
        )
        await manager.start()
        try:
            chat_task = await manager.submit(
                "chat task", _target(), agent_id="source", mode_id="chat"
            )
            await manager.set_mode(
                "plan",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="source",
            )
            with pytest.raises(PermissionError, match="disabled"):
                await manager.send_agent_message(
                    "destination",
                    "must remain denied",
                    task_id=chat_task.task_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                )

            plan_task = await manager.submit(
                "plan task", _target(), agent_id="source", mode_id="plan"
            )
            await manager.set_mode(
                "chat",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="source",
            )
            message = await manager.send_agent_message(
                "destination",
                "snapshot remains authorized",
                task_id=plan_task.task_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
            )
            assert message.source_agent_id == "source"
            assert message.destination_agent_id == "destination"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_cross_agent_children_require_acl_and_generic_submit_cannot_bypass_limits(
    tmp_path,
):
    async def scenario() -> None:
        source = _profile(
            "source",
            allowed_peers={"allowed", "denied-by-profile"},
            allowed_request_types={"child_task"},
            max_children=1,
        )
        allowed = _profile(
            "allowed",
            allowed_request_types={"child_task"},
        )
        denied_peer = _profile(
            "denied-peer",
            allowed_request_types={"child_task"},
        )
        denied_by_profile = _profile(
            "denied-by-profile",
            allowed_request_types={"child_task"},
            denied_request_types={"child_task"},
        )
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = TaskManager(
            store,
            _registry(source, allowed, denied_peer, denied_by_profile),
            worker_count=0,
            default_agent_id="source",
        )
        await manager.start()
        try:
            await store.register_attachment(
                {"attachment_id": "foreign-file", "mime_type": "text/plain"}
            )
            await manager.submit(
                {"attachments": ["foreign-file"]},
                _target(),
                agent_id="denied-peer",
                mode_id="chat",
            )
            parent = await manager.submit(
                "parent", _target(), agent_id="source", mode_id="plan"
            )

            with pytest.raises(PermissionError, match="peer not permitted"):
                await manager.submit_child_task(
                    parent.task_id,
                    "unauthorized peer",
                    agent_id="denied-peer",
                    reply_target=_target(),
                )
            with pytest.raises(PermissionError, match="request type"):
                await manager.submit_child_task(
                    parent.task_id,
                    "unauthorized request",
                    agent_id="allowed",
                    request_type="deploy",
                    reply_target=_target(),
                )
            with pytest.raises(PermissionError, match="does not accept"):
                await manager.submit_child_task(
                    parent.task_id,
                    "destination deny wins",
                    agent_id="denied-by-profile",
                    reply_target=_target(),
                )
            with pytest.raises(PermissionError, match="attachment access denied"):
                await manager.submit_child_task(
                    parent.task_id,
                    {
                        "attachments": [],
                        "attachment_ids": ["foreign-file"],
                    },
                    agent_id="allowed",
                    reply_target=_target(),
                )
            with pytest.raises(PermissionError, match="attachment access denied"):
                await manager.submit_child_task(
                    parent.task_id,
                    ["foreign-file"],
                    agent_id="allowed",
                    reply_target=_target(),
                )

            child = await manager.submit_child_task(
                parent.task_id,
                "authorized child",
                agent_id="allowed",
                reply_target=_target(),
            )
            assert child.parent_task_id == parent.task_id
            assert await store.child_count(parent.task_id) == 1

            with pytest.raises(PermissionError, match="maximum child-task count"):
                await manager.submit_child_task(
                    parent.task_id,
                    "over limit",
                    agent_id="allowed",
                    reply_target=_target(),
                )
            with pytest.raises(PermissionError, match="submit_child_task"):
                await manager.submit(
                    "direct bypass",
                    _target(),
                    agent_id="source",
                    parent_task_id=parent.task_id,
                    child_depth=1,
                )
            assert await store.child_count(parent.task_id) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_execute_authorization_is_bound_to_the_mode_policy_version(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.set_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                mode_id="execute",
                policy_version=1,
                authorized_by="user",
                authorized_at="2026-08-10T00:00:00+00:00",
            )
            scope = {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "agent_id": "codex",
                "mode_id": "execute",
            }
            assert await store.is_mode_authorized(**scope, policy_version=1)
            assert not await store.is_mode_authorized(**scope, policy_version=2)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_execute_authorization_requires_complete_normalized_provenance(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        scope = {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "agent_id": "codex",
            "mode_id": "execute",
            "policy_version": 2,
        }
        try:
            for authorized_by, authorized_at in (
                ("user", None),
                (None, "2026-08-10T00:00:00+00:00"),
                ("   ", "2026-08-10T00:00:00+00:00"),
                ("user", "not-a-timestamp"),
            ):
                with pytest.raises(ValueError, match="authorization requires"):
                    await store.set_session_mode(
                        **scope,
                        authorized_by=authorized_by,
                        authorized_at=authorized_at,
                    )

            await store.set_session_mode(
                **scope,
                authorized_by="  user  ",
                authorized_at="2026-08-10T08:00:00+08:00",
            )
            row = await store._call(
                lambda conn: conn.execute(
                    "SELECT authorized_by, authorized_at FROM session_modes"
                ).fetchone()
            )
            assert tuple(row) == (
                "user",
                "2026-08-10T00:00:00.000000+00:00",
            )
            assert await store.is_mode_authorized(**scope)

            # Direct SQL/imports can bypass the setter. Authorization checks
            # must still reject truthy but malformed durable provenance.
            for column, value in (
                ("authorized_by", "   "),
                ("authorized_at", "not-a-timestamp"),
            ):
                await store._call(
                    lambda conn, column=column, value=value: conn.execute(
                        f"UPDATE session_modes SET {column}=?", (value,)
                    ).rowcount
                )
                assert not await store.is_mode_authorized(**scope)
                await store.set_session_mode(
                    **scope,
                    authorized_by="user",
                    authorized_at="2026-08-10T00:00:00+00:00",
                )

            # Provenance has no meaning for non-execute selections and must
            # not leak forward when a caller switches modes.
            await store.set_session_mode(
                **{**scope, "mode_id": "chat"},
                authorized_by="ignored",
                authorized_at="not-a-timestamp",
            )
            row = await store._call(
                lambda conn: conn.execute(
                    "SELECT authorized_by, authorized_at FROM session_modes"
                ).fetchone()
            )
            assert tuple(row) == (None, None)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_delivery_rejects_attachment_blocked_after_enqueue(tmp_path):
    class _MailboxRuntime(_Runtime):
        def __init__(self) -> None:
            self.calls = 0

        async def run(self, task, emit):
            self.calls += 1
            return "should not run"

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        runtime = _MailboxRuntime()
        registry = AgentRegistry()
        registry.register("planner", runtime)
        try:
            await store.register_attachment(
                {"attachment_id": "mailbox-file", "mime_type": "text/plain"}
            )
            source_task = await store.create_task(
                {
                    "task_id": "source-task",
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:user:default:codex",
                    "reply_target": _target(),
                    "inputs": {"attachments": ["mailbox-file"]},
                }
            )
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="inspect",
                task_id=source_task.task_id,
                request_id="blocked-media-request",
                payload={"attachments": ["mailbox-file"]},
            )
            # Reconciliation marks a now-missing managed file blocked before
            # the mailbox worker claims the already-queued request.
            await store.reconcile()
            worker = AgentMailboxWorker(store, registry, "planner")
            assert await worker.run_once() == 0
            assert runtime.calls == 0
            item = await store.get_mailbox_item(request.mailbox_id)
            assert item is not None
            assert item.state.value == "rejected"
            assert "attachment access denied" in (item.last_error or "")
        finally:
            await store.close()

    asyncio.run(scenario())
