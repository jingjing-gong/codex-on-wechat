"""Regression coverage for named Agents selected through ``/agent``."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from src.agents.base import AgentResult
from src.channels.lark import LarkCommandRouter
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.runtime.manager import TaskManager
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore, StoreError


class _Runtime:
    agent_id = "codex"

    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def run(self, _task, _emit) -> AgentResult:
        return AgentResult(content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _envelope(text: str, *, message_id: str = "message-1") -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=message_id,
        text=text,
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
    )


def _manager(store: SQLiteStore, runtime: _Runtime) -> TaskManager:
    registry = AgentRegistry()
    registry.register("codex", runtime, profile=codex_profile(default_mode_id="chat"))
    return TaskManager(
        store,
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="chat",
        allow_dynamic_agents=True,
    )


def test_agent_command_switches_existing_registered_agent_without_replacing_it(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        codex = _Runtime()
        planner = _Runtime()
        registry = AgentRegistry()
        registry.register("codex", codex, profile=codex_profile(default_mode_id="chat"))
        registry.register(
            "planner",
            planner,
            profile=AgentProfile(
                agent_id="planner",
                display_name="Planner",
                capabilities=frozenset({"read"}),
            ),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent planner"), _envelope("/agent planner")
            )
            assert str(response) == "switched to Agent: planner"
            assert manager.registry.require("planner") is planner
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_command_creates_named_agent_and_routes_new_work(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            response = await router.handle_command(
                parse_command("/agent planner"), _envelope("/agent planner")
            )
            assert str(response) == "switched to Agent: planner"
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "planner"
            assert "planner" in {
                descriptor.agent_id for descriptor in await manager.list_agents()
            }

            accepted = await manager.accept_inbound(
                _envelope("work", message_id="message-2"),
                create_task=True,
            )
            assert accepted.task.agent_id == "planner"
            assert accepted.task.conversation_id.endswith(":planner")
            assert await store.get_profile("planner", 1) is not None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_existing_only_agent_switch_never_recreates_a_deleted_agent(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )
            assert await manager.delete_agent("planner")

            with pytest.raises(KeyError, match="Agent was deleted: planner"):
                await manager.set_existing_active_agent(
                    "planner",
                    channel="lark",
                    bot_id="cli_restricted",
                    external_user_id="ou_restricted",
                    session_id="default",
                )

            assert await store.is_agent_deleted("planner")
            assert manager.registry.registration("planner") is None
            assert await store.get_route(
                channel="lark",
                bot_id="cli_restricted",
                external_user_id="ou_restricted",
                session_id="default",
                default_agent_id="codex",
            ) == "codex"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_lark_existing_only_switch_loses_delete_race_without_recreating_agent(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )

            passed_lark_admission = asyncio.Event()
            continue_switch = asyncio.Event()
            shared = MVPCommandRouter(manager)

            class BlockingSharedRouter:
                async def handle_command(
                    self,
                    command,
                    envelope,
                    *,
                    command_id="",
                    existing_agent_only=False,
                ):
                    assert command.name == "agent"
                    assert existing_agent_only is True
                    passed_lark_admission.set()
                    await continue_switch.wait()
                    return await shared.handle_command(
                        command,
                        envelope,
                        command_id=command_id,
                        existing_agent_only=existing_agent_only,
                    )

            user_envelope = InboundEnvelope(
                channel="lark",
                bot_id="cli_restricted",
                external_user_id="ou_restricted",
                external_message_id="om_switch_race",
                text="/agent planner",
            )
            user_router = LarkCommandRouter(
                manager,
                shared_router=BlockingSharedRouter(),
            )
            switching = asyncio.create_task(
                user_router.handle_command(
                    parse_command("/agent planner"), user_envelope
                )
            )
            await passed_lark_admission.wait()

            admin_envelope = InboundEnvelope(
                channel="lark",
                bot_id="cli_admin",
                external_user_id="ou_admin",
                external_message_id="om_delete_race",
                text="/delagent planner",
            )
            admin_router = LarkCommandRouter(
                manager,
                shared_router=shared,
                administrator=lambda _envelope: True,
            )
            assert await admin_router.handle_command(
                parse_command("/delagent planner"), admin_envelope
            ) == "Agent deleted: planner"

            continue_switch.set()
            response = await switching
            assert str(response).startswith("cannot switch Agent:")
            assert "Agent was deleted: planner" in str(response)
            assert await store.is_agent_deleted("planner")
            assert manager.registry.registration("planner") is None
            assert await store.get_route(
                channel="lark",
                bot_id="cli_restricted",
                external_user_id="ou_restricted",
                session_id="default",
                default_agent_id="codex",
            ) == "codex"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_named_agent_route_and_profile_restore_after_restart(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = _manager(SQLiteStore(database), _Runtime())
        await first.start()
        await first.set_active_agent(
            "researcher",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        # Simulate a route written by an older release before Agent IDs were
        # canonicalized to lowercase.
        await first.store.set_route(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            active_agent_id="Researcher",
        )
        await first.stop()

        second = _manager(SQLiteStore(database), _Runtime())
        await second.start()
        try:
            assert await second.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "researcher"
            assert second.registry.require("researcher") is not None
            assert "researcher" in {
                descriptor.agent_id for descriptor in await second.list_agents()
            }
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_dynamic_creation_preserves_existing_mixed_case_registration(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        codex = _Runtime()
        legacy = _Runtime()
        registry = AgentRegistry()
        registry.register("codex", codex, profile=codex_profile(default_mode_id="chat"))
        registry.register(
            "Planner",
            legacy,
            profile=AgentProfile(agent_id="Planner", display_name="Planner"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "Planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "Planner"
            assert manager.registry.require("Planner") is legacy
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_command_rejects_unsafe_named_agent_id(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent ../escape"),
                _envelope("/agent ../escape"),
            )
            assert str(response).startswith("cannot switch Agent: Agent ID must start")
            assert "../escape" not in manager.registry
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_named_agent_falls_back_to_read_only_profile_for_legacy_runtime(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _Runtime()
        registry = AgentRegistry()
        # Older integrations registered a runtime without an AgentProfile.
        registry.register("codex", runtime)
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            assert await manager.ensure_agent("legacy")
            profile = manager.registry.profile("legacy")
            assert profile is not None
            assert profile.default_mode_id == "chat"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delagent_removes_dynamic_alias_falls_back_route_and_survives_restart(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = _manager(SQLiteStore(database), _Runtime())
        await first.start()
        try:
            await first.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            task = await first.submit(
                "preserve this snapshot",
                _envelope("work").reply_target,
                agent_id="planner",
            )
            # Retirement is allowed only after the Agent has no unfinished
            # work.  Cancelling a queued task preserves its immutable history
            # while making the live alias safe to remove.
            assert await first.cancel(task.task_id)
            assert await first.delete_agent("planner")
            assert first.registry.registration("planner") is None
            assert await first.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"
            assert await first.store.is_agent_deleted("planner")
            stored = await first.store.get_task(task.task_id)
            assert stored is not None and stored.agent_id == "planner"
            assert stored.state.value == "cancelled"
        finally:
            await first.stop()

        second = _manager(SQLiteStore(database), _Runtime())
        await second.start()
        try:
            assert second.registry.registration("planner") is None
            assert await second.store.is_agent_deleted("planner")
        finally:
            await second.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("restart_before_create", (False, True))
def test_creating_new_agent_does_not_republish_retired_profile_history(
    tmp_path, restart_before_create
):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"

        def manager_for(registry: AgentRegistry | None = None) -> TaskManager:
            if registry is None:
                registry = AgentRegistry()
                registry.register(
                    "codex",
                    _Runtime(),
                    profile=codex_profile(
                        profile_version=2,
                        default_mode_id="execute",
                    ),
                )
            return TaskManager(
                SQLiteStore(database),
                registry,
                worker_count=0,
                default_agent_id="codex",
                default_mode_id="execute",
                allow_dynamic_agents=True,
            )

        manager = manager_for()
        await manager.start()
        try:
            await manager.set_active_agent(
                "writer",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.delete_agent("writer")
            retired = await manager.store.get_profile("writer", 2)
            assert retired is not None and not retired.enabled

            # AgentRegistry deliberately retains immutable Profile history
            # after unregistering a runtime. Creating another alias must not
            # republish that stale enabled=True snapshot over the lifecycle-
            # disabled durable row.
            assert manager.registry.registration("writer") is None
            retained = manager.registry.profile("writer", 2)
            assert retained is not None and retained.enabled

            if restart_before_create:
                retained_registry = manager.registry
                await manager.stop()
                manager = manager_for(retained_registry)
                await manager.start()
                assert manager.registry.registration("writer") is None
                retained = manager.registry.profile("writer", 2)
                assert retained is not None and retained.enabled

            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent misc"), _envelope("/agent misc")
            )

            assert response == "switched to Agent: misc"
            assert manager.registry.registration("misc") is not None
            assert await manager.store.is_agent_deleted("writer")
            unchanged = await manager.store.get_profile("writer", 2)
            assert unchanged is not None and not unchanged.enabled
            assert await manager.store.get_mode("misc", "chat", 2) == (
                manager.mode_registry.get("chat", 2)
            )
            assert await manager.store.get_mode("misc", "execute", 2) == (
                manager.mode_registry.get("execute", 2)
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_startup_does_not_hide_registered_retired_profile_conflict(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(
                profile_version=2,
                default_mode_id="execute",
            ),
        )
        first = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        await first.start()
        await first.set_active_agent(
            "writer",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        assert await first.delete_agent("writer")
        retained = registry.profile("writer", 2)
        assert retained is not None and retained.enabled
        await first.stop()

        # Only detached historical Profiles are omitted from global startup
        # publication. An explicit runtime registration is authoritative and
        # must still expose its enabled-bit conflict with the tombstoned row.
        registry.register("writer", _Runtime(), profile=retained)
        restarted = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        try:
            with pytest.raises(
                StoreError,
                match="profile version metadata conflicts: writer@2",
            ):
                await restarted.start()
        finally:
            await restarted.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("restart_before_recreate", (False, True))
def test_agent_command_recreates_retired_alias_without_rewriting_profile(
    tmp_path, restart_before_recreate
):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"

        def manager_for() -> TaskManager:
            registry = AgentRegistry()
            registry.register(
                "codex",
                _Runtime(),
                profile=codex_profile(
                    profile_version=2,
                    default_mode_id="execute",
                ),
            )
            return TaskManager(
                SQLiteStore(database),
                registry,
                worker_count=0,
                default_agent_id="codex",
                default_mode_id="execute",
                allow_dynamic_agents=True,
            )

        manager = manager_for()
        await manager.start()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            original = await manager.store.get_profile("bb", 2)
            assert original is not None and original.enabled
            task = await manager.submit(
                "preserve recreation history",
                _envelope("work", message_id="bb-history").reply_target,
                agent_id="bb",
            )
            assert await manager.cancel(task.task_id)
            assert await manager.delete_agent("bb")
            retired = await manager.store.get_profile("bb", 2)
            assert retired is not None and not retired.enabled
            assert await manager.store.is_agent_deleted("bb")
        finally:
            if restart_before_recreate:
                await manager.stop()

        if restart_before_recreate:
            manager = manager_for()
            await manager.start()

        try:
            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent bb"), _envelope("/agent bb")
            )

            assert response == "switched to Agent: bb"
            restored = await manager.store.get_profile("bb", 2)
            assert restored is not None and restored.enabled
            assert restored == original
            assert not await manager.store.is_agent_deleted("bb")
            assert manager.registry.require("bb") is not None
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "bb"
            retained = await manager.store.get_task(task.task_id)
            assert retained is not None
            assert retained.agent_id == "bb"
            assert retained.profile_version == 2
            assert retained.conversation_id == task.conversation_id
            assert retained.state.value == "cancelled"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_recreation_rejects_genuine_retired_profile_conflict(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(profile_version=2, default_mode_id="execute"),
        )
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.delete_agent("bb")
            await manager.store._call(
                lambda conn: conn.execute(
                    "UPDATE agent_profiles SET capabilities_json='[\"tampered\"]' "
                    "WHERE agent_id='bb' AND profile_version=2"
                ).rowcount
            )

            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent bb"), _envelope("/agent bb")
            )

            assert response == (
                "cannot switch Agent: profile version metadata conflicts: bb@2"
            )
            assert manager.registry.registration("bb") is None
            assert await manager.store.is_agent_deleted("bb")
            persisted = await manager.store.get_profile("bb", 2)
            assert persisted is not None and not persisted.enabled
            assert persisted.capabilities == frozenset({"tampered"})
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_recreation_accepts_legacy_profile_set_order(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(profile_version=2, default_mode_id="execute"),
        )
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "writer",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            original = await manager.store.get_profile("writer", 2)
            assert original is not None
            assert await manager.delete_agent("writer")
            await manager.store._call(
                lambda conn: conn.execute(
                    """UPDATE agent_profiles
                       SET capabilities_json=?
                       WHERE agent_id='writer' AND profile_version=2""",
                    (
                        '["write","status","search","read","list",'
                        '"execute","edit","diff"]',
                    ),
                ).rowcount
            )

            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent writer"), _envelope("/agent writer")
            )

            assert response == "switched to Agent: writer"
            restored = await manager.store.get_profile("writer", 2)
            assert restored == original
            assert restored is not None and restored.enabled
            assert not await manager.store.is_agent_deleted("writer")
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_recreation_restores_every_retained_profile_version(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(profile_version=2, default_mode_id="execute"),
        )
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            current = await manager.store.get_profile("bb", 2)
            assert current is not None
            older = replace(current, profile_version=1)
            manager.registry.register_profile(older)
            await manager.store.put_profile(older)

            assert await manager.delete_agent("bb")
            for version in (1, 2):
                retired = await manager.store.get_profile("bb", version)
                assert retired is not None and not retired.enabled

            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent bb"), _envelope("/agent bb")
            )

            assert response == "switched to Agent: bb"
            assert not await manager.store.is_agent_deleted("bb")
            for version in (1, 2):
                restored = await manager.store.get_profile("bb", version)
                assert restored is not None and restored.enabled
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_second_manager_can_recreate_agent_retired_by_first_manager(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = _manager(SQLiteStore(database), _Runtime())
        await first.start()
        second: TaskManager | None = None
        try:
            await first.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )

            second = _manager(SQLiteStore(database), _Runtime())
            await second.start()
            assert second.registry.registration("bb") is not None

            assert await first.delete_agent("bb")
            # Runtime registries are process-local, so the other manager still
            # holds its stale registration until durable revalidation.
            assert second.registry.registration("bb") is not None
            assert await second.store.is_agent_deleted("bb")

            response = await MVPCommandRouter(second).handle_command(
                parse_command("/agent bb"), _envelope("/agent bb")
            )

            assert response == "switched to Agent: bb"
            assert not await second.store.is_agent_deleted("bb")
            restored = await second.store.get_profile("bb", 1)
            assert restored is not None and restored.enabled
            assert await second.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "bb"
        finally:
            if second is not None:
                await second.stop()
            await first.stop()

    asyncio.run(scenario())


def test_interrupted_agent_recreation_stays_hidden_and_is_retryable(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"

        def manager_for() -> TaskManager:
            registry = AgentRegistry()
            registry.register(
                "codex", _Runtime(), profile=codex_profile(default_mode_id="chat")
            )
            return TaskManager(
                SQLiteStore(database),
                registry,
                worker_count=0,
                allow_dynamic_agents=True,
            )

        first = manager_for()
        await first.start()
        await first.set_active_agent(
            "bb",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        assert await first.delete_agent("bb")

        # Simulate interruption after Profile preparation but before the
        # tombstone/route commit performed by set_active_agent().
        assert await first.ensure_agent("bb", allow_deleted=True)
        prepared = await first.store.get_profile("bb", 1)
        assert prepared is not None and prepared.enabled
        assert await first.store.is_agent_deleted("bb")
        with pytest.raises(KeyError, match="Agent was deleted: bb"):
            await first.ensure_agent("bb")
        await first.stop()

        second = manager_for()
        await second.start()
        try:
            assert second.registry.registration("bb") is None
            assert await second.store.is_agent_deleted("bb")

            response = await MVPCommandRouter(second).handle_command(
                parse_command("/agent bb"), _envelope("/agent bb")
            )

            assert response == "switched to Agent: bb"
            assert second.registry.require("bb") is not None
            assert not await second.store.is_agent_deleted("bb")
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_agent_recreation_publication_failure_keeps_tombstone_across_restart(
    tmp_path,
):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first_store = SQLiteStore(database)
        first = _manager(first_store, _Runtime())
        await first.start()
        await first.set_active_agent(
            "bb",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        assert await first.delete_agent("bb")

        original_put_mode = first_store.put_mode

        async def fail_bb_mode(mode, *, agent_id="codex"):
            if agent_id == "bb":
                raise RuntimeError("mode publication failed")
            return await original_put_mode(mode, agent_id=agent_id)

        first_store.put_mode = fail_bb_mode  # type: ignore[method-assign]
        response = await MVPCommandRouter(first).handle_command(
            parse_command("/agent bb"), _envelope("/agent bb")
        )

        assert response == "cannot switch Agent: mode publication failed"
        assert first.registry.registration("bb") is None
        assert await first_store.is_agent_deleted("bb")
        prepared = await first_store.get_profile("bb", 1)
        assert prepared is not None and prepared.enabled
        assert await first.get_active_agent(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        ) == "codex"
        await first.stop()

        second = _manager(SQLiteStore(database), _Runtime())
        await second.start()
        try:
            assert second.registry.registration("bb") is None
            assert await second.store.is_agent_deleted("bb")
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_older_profile_conflict_after_prepare_remains_tombstoned(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(profile_version=2, default_mode_id="execute"),
        )
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        await manager.start()
        await manager.set_active_agent(
            "bb",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        current = await manager.store.get_profile("bb", 2)
        assert current is not None
        older = replace(current, profile_version=1)
        manager.registry.register_profile(older)
        await manager.store.put_profile(older)
        assert await manager.delete_agent("bb")
        await manager.store._call(
            lambda conn: conn.execute(
                "UPDATE agent_profiles SET capabilities_json='[\"tampered\"]' "
                "WHERE agent_id='bb' AND profile_version=1"
            ).rowcount
        )

        response = await MVPCommandRouter(manager).handle_command(
            parse_command("/agent bb"), _envelope("/agent bb")
        )

        assert response == (
            "cannot switch Agent: profile version metadata conflicts: bb@1"
        )
        assert manager.registry.registration("bb") is None
        assert await manager.store.is_agent_deleted("bb")
        await manager.stop()

        restarted_registry = AgentRegistry()
        restarted_registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(profile_version=2, default_mode_id="execute"),
        )
        restarted = TaskManager(
            SQLiteStore(database),
            restarted_registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="execute",
            allow_dynamic_agents=True,
        )
        # Supply the same current static template snapshot used by the first
        # process; startup must still hide the prepared alias by tombstone.
        await restarted.start()
        try:
            assert restarted.registry.registration("bb") is None
            assert await restarted.store.is_agent_deleted("bb")
        finally:
            await restarted.stop()

    asyncio.run(scenario())


def test_agent_recreation_commit_rolls_back_tombstone_clear_with_route_failure(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.delete_agent("bb")
            await store._call(
                lambda conn: conn.execute(
                    """CREATE TRIGGER reject_bb_route
                       BEFORE INSERT ON routes
                       WHEN NEW.active_agent_id='bb'
                       BEGIN
                           SELECT RAISE(ABORT, 'route rejected');
                       END"""
                )
            )

            with pytest.raises(sqlite3.IntegrityError, match="route rejected"):
                await manager.set_active_agent(
                    "bb",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                )
            assert await store.is_agent_deleted("bb")
            assert manager.registry.registration("bb") is not None
            assert "bb" not in {
                descriptor.agent_id for descriptor in await manager.list_agents()
            }
            with pytest.raises(KeyError, match="Agent was deleted: bb"):
                await manager.submit(
                    "must stay rejected",
                    _envelope("submit tombstone").reply_target,
                    agent_id="bb",
                )
            with pytest.raises(KeyError, match="Agent was deleted: bb"):
                await manager.accept_inbound(
                    _envelope(
                        "must stay rejected",
                        message_id="accept-tombstoned-bb",
                    ),
                    create_task=True,
                    agent_id="bb",
                )
            assert await store.list_tasks(limit=10) == []
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"

            await store._call(
                lambda conn: conn.execute("DROP TRIGGER reject_bb_route")
            )
            retried = await MVPCommandRouter(manager).handle_command(
                parse_command("/agent bb"),
                _envelope("/agent bb", message_id="retry-bb"),
            )
            assert retried == "switched to Agent: bb"
            assert not await store.is_agent_deleted("bb")
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_concurrent_agent_recreation_commits_every_scope_route(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )
            assert await manager.delete_agent("bb")

            original_ensure = manager.ensure_agent
            both_observed_tombstone = asyncio.Event()
            release_recreations = asyncio.Event()
            arrivals = 0
            creation_results: list[bool] = []

            async def synchronized_ensure(
                agent_id: str, *, allow_deleted: bool = False
            ) -> bool:
                nonlocal arrivals
                if agent_id == "bb" and allow_deleted:
                    arrivals += 1
                    if arrivals == 2:
                        both_observed_tombstone.set()
                    await release_recreations.wait()
                result = await original_ensure(
                    agent_id, allow_deleted=allow_deleted
                )
                if agent_id == "bb" and allow_deleted:
                    creation_results.append(result)
                return result

            manager.ensure_agent = synchronized_ensure  # type: ignore[method-assign]

            first_switch = asyncio.create_task(
                manager.set_active_agent(
                    "bb",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user-1",
                    session_id="default",
                )
            )
            second_switch = asyncio.create_task(
                manager.set_active_agent(
                    "bb",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user-2",
                    session_id="default",
                )
            )
            await asyncio.wait_for(both_observed_tombstone.wait(), timeout=1)
            release_recreations.set()
            await asyncio.gather(first_switch, second_switch)

            assert not await store.is_agent_deleted("bb")
            assert sorted(creation_results) == [False, True]
            for user_id in ("user-1", "user-2"):
                assert await manager.get_active_agent(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user_id,
                    session_id="default",
                ) == "bb"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delete_racing_switch_cannot_route_to_a_disabled_agent(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        release_switch = asyncio.Event()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )

            original_ensure = manager.ensure_agent
            switch_resolved = asyncio.Event()

            async def pause_after_ensure(
                agent_id: str, *, allow_deleted: bool = False
            ) -> bool:
                result = await original_ensure(
                    agent_id, allow_deleted=allow_deleted
                )
                if agent_id == "bb" and allow_deleted:
                    switch_resolved.set()
                    await release_switch.wait()
                return result

            manager.ensure_agent = pause_after_ensure  # type: ignore[method-assign]
            switch = asyncio.create_task(
                manager.set_active_agent(
                    "bb",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="racer",
                    session_id="default",
                )
            )
            await asyncio.wait_for(switch_resolved.wait(), timeout=1)
            assert await manager.delete_agent("bb")
            release_switch.set()

            with pytest.raises(
                RuntimeError, match="Agent changed while switching: bb"
            ):
                await switch

            assert manager.registry.registration("bb") is None
            assert await store.is_agent_deleted("bb")
            profile = await store.get_profile("bb", 1)
            assert profile is not None and not profile.enabled
            for user_id in ("owner", "racer"):
                assert await store.get_route(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user_id,
                    session_id="default",
                    default_agent_id="codex",
                ) == "codex"
        finally:
            release_switch.set()
            await manager.stop()

    asyncio.run(scenario())


def test_delete_after_route_commit_wins_over_switch_cache_update(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        release_commit = asyncio.Event()
        try:
            await manager.set_active_agent(
                "bb",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )

            original_commit = store.commit_agent_reactivation
            route_committed = asyncio.Event()

            async def pause_after_commit(profile, **kwargs):
                result = await original_commit(profile, **kwargs)
                if kwargs.get("external_user_id") == "racer":
                    route_committed.set()
                    await release_commit.wait()
                return result

            store.commit_agent_reactivation = (  # type: ignore[method-assign]
                pause_after_commit
            )
            switch = asyncio.create_task(
                manager.set_active_agent(
                    "bb",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="racer",
                    session_id="default",
                )
            )
            await asyncio.wait_for(route_committed.wait(), timeout=1)
            deletion = asyncio.create_task(manager.delete_agent("bb"))
            await asyncio.sleep(0)
            assert not deletion.done()

            release_commit.set()
            assert await switch
            assert await deletion

            assert manager.registry.registration("bb") is None
            assert await store.is_agent_deleted("bb")
            profile = await store.get_profile("bb", 1)
            assert profile is not None and not profile.enabled
            assert manager.active_agent_for(
                replace(
                    _envelope("route check"), external_user_id="racer"
                ).reply_target
            ) == "codex"
            assert await store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="racer",
                session_id="default",
                default_agent_id="codex",
            ) == "codex"
        finally:
            release_commit.set()
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "task_state",
    ("queued", "claimed", "running", "cancel_requested", "orphaned"),
)
def test_delagent_rejects_agent_with_unfinished_or_resumable_work(
    tmp_path, task_state
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / f"{task_state}.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            task = await manager.submit(
                f"work in {task_state}",
                _envelope("work").reply_target,
                agent_id="planner",
            )

            claimed_at = datetime(2026, 8, 14, tzinfo=timezone.utc)
            claim = None
            if task_state != "queued":
                claim = await store.claim_task_by_id(
                    task.task_id,
                    "worker",
                    lease_seconds=1,
                    now=claimed_at,
                )
                assert claim is not None
            if task_state in {"running", "cancel_requested", "orphaned"}:
                assert claim is not None
                assert await store.mark_task_running(
                    task.task_id,
                    claim.claim_token,
                    execution_id=claim.execution_id,
                    now=claimed_at,
                )
            if task_state == "cancel_requested":
                assert await store.cancel_task(task.task_id, actor="user")
            elif task_state == "orphaned":
                await store.reconcile(now=claimed_at + timedelta(seconds=2))

            current = await store.get_task(task.task_id)
            assert current is not None and current.state.value == task_state
            with pytest.raises(
                RuntimeError, match="unfinished tasks remain"
            ):
                await manager.delete_agent("planner")

            # A rejected retirement has no partial tombstone, profile, route,
            # or process-local registry effects.
            assert manager.registry.registration("planner") is not None
            assert not await store.is_agent_deleted("planner")
            profile = await store.get_profile("planner", 1)
            assert profile is not None and profile.enabled
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "planner"
        finally:
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("task_state", "terminal_state", "runtime_interrupt"),
    (
        ("queued", "cancelled", False),
        ("claimed", "interrupted", True),
        ("running", "interrupted", True),
        ("cancel_requested", "interrupted", True),
        ("orphaned", "cancelled", False),
    ),
)
def test_delagent_force_terminalizes_unfinished_work_atomically(
    tmp_path, task_state, terminal_state, runtime_interrupt
):
    class OwnedChild(_Runtime):
        def __init__(self, agent_id: str) -> None:
            super().__init__()
            self.agent_id = agent_id
            self.interrupted: list[str] = []

        async def interrupt(self, task_id: str) -> bool:
            self.interrupted.append(task_id)
            return True

    class FactoryRuntime(_Runtime):
        def __init__(self) -> None:
            super().__init__()
            self.children: dict[str, OwnedChild] = {}

        def for_agent(self, agent_id: str) -> OwnedChild:
            child = OwnedChild(agent_id)
            self.children[agent_id] = child
            return child

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / f"force-{task_state}.sqlite")
        runtime = FactoryRuntime()
        manager = _manager(store, runtime)
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            child = runtime.children["planner"]
            task = await manager.submit(
                f"force {task_state}",
                _envelope("work").reply_target,
                agent_id="planner",
            )
            execution_id = str(task.execution_id)

            claimed_at = datetime.now(timezone.utc)
            claim = None
            if task_state != "queued":
                claim = await store.claim_task_by_id(
                    task.task_id,
                    "worker",
                    lease_seconds=60,
                    now=claimed_at,
                )
                assert claim is not None
            if task_state in {"running", "cancel_requested", "orphaned"}:
                assert claim is not None
                assert await store.mark_task_running(
                    task.task_id,
                    claim.claim_token,
                    execution_id=claim.execution_id,
                    now=claimed_at,
                )
            if task_state == "cancel_requested":
                assert await store.cancel_task(task.task_id, actor="owner")
            elif task_state == "orphaned":
                await store.reconcile(
                    now=claimed_at + timedelta(seconds=61)
                )

            result = await manager.delete_agent("planner", force=True)
            assert result["cancelled_task_ids"] == (task.task_id,)
            assert result["active_task_ids"] == (
                (task.task_id,) if runtime_interrupt else ()
            )
            assert result["disabled_profile_count"] == 1

            stored = await store.get_task(task.task_id)
            assert stored is not None and stored.state.value == terminal_state
            execution = await store.get_execution(execution_id)
            assert execution is not None
            assert execution.state.value == (
                "orphaned" if task_state == "orphaned" else terminal_state
            )
            invocation = await store.get_agent_invocation(execution_id)
            assert invocation is not None
            assert invocation.admission_released_at is not None
            assert child.interrupted == (
                [task.task_id] if runtime_interrupt else []
            )
            assert child.stopped == 1
            assert manager.registry.registration("planner") is None
            assert await store.is_agent_deleted("planner")
            profile = await store.get_profile("planner", 1)
            assert profile is not None and not profile.enabled
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delagent_force_rejects_mailbox_queue_and_reply_survives_self_delete(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "force-mailbox.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            task = await manager.submit(
                "queued work",
                _envelope("work").reply_target,
                agent_id="planner",
            )
            mailbox = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="queued collaboration",
                request_id="force-delete-request",
                message_id="force-delete-message",
                payload={"request_type": "ask"},
            )

            response = await MVPCommandRouter(manager).handle_command(
                parse_command("/delagent planner force"),
                _envelope("/delagent planner force", message_id="delete-message"),
            )
            assert response == (
                "Agent force-deleted: planner; force-cancelled 1 unfinished "
                f"task(s): {task.task_id}; rejected 1 queued Agent message(s)"
            )
            stored_mailbox = await store.get_mailbox_item(mailbox.mailbox_id)
            assert stored_mailbox is not None
            assert stored_mailbox.state.value == "rejected"
            invocation = await store.get_agent_invocation(
                str(mailbox.current_invocation_id)
            )
            assert invocation is not None
            assert invocation.state.value == "cancelled"
            assert invocation.admission_released_at is not None
            # The command is handled outside the deleted Agent process; route
            # fallback and process teardown cannot crash its acknowledgement.
            assert manager.registry.registration("planner") is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delagent_force_retries_runtime_cleanup_after_durable_retirement(tmp_path):
    class FlakyOwnedChild(_Runtime):
        def __init__(self, agent_id: str) -> None:
            super().__init__()
            self.agent_id = agent_id
            self.stop_attempts = 0

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("shutdown is not yet proven")
            await super().stop()

    class FactoryRuntime(_Runtime):
        def __init__(self) -> None:
            super().__init__()
            self.children: dict[str, FlakyOwnedChild] = {}

        def for_agent(self, agent_id: str) -> FlakyOwnedChild:
            child = FlakyOwnedChild(agent_id)
            self.children[agent_id] = child
            return child

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "force-stop-retry.sqlite")
        runtime = FactoryRuntime()
        manager = _manager(store, runtime)
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            child = runtime.children["planner"]
            task = await manager.submit(
                "cancel me during force-delete",
                _envelope("work").reply_target,
                agent_id="planner",
            )

            with pytest.raises(RuntimeError, match="shutdown is not yet proven"):
                await manager.delete_agent("planner", force=True)

            assert await store.is_agent_deleted("planner")
            assert manager.registry.registration("planner") is not None
            assert child.stop_attempts == 1

            result = await manager.delete_agent("planner", force=True)

            assert result["cancelled_task_ids"] == (task.task_id,)
            assert result["active_task_ids"] == ()
            assert result["rejected_mailbox_ids"] == ()
            assert result["disabled_profile_count"] == 1
            assert child.stop_attempts == 2
            assert child.stopped == 1
            assert manager.registry.registration("planner") is None
            assert await store.is_agent_deleted("planner")
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delagent_force_distinguishes_missing_and_tombstoned_agents(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "force-missing.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            with pytest.raises(KeyError, match="Agent not found: missing"):
                await manager.delete_agent("missing", force=True)

            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.delete_agent("planner")
            with pytest.raises(
                RuntimeError, match="Agent was already deleted: planner"
            ):
                await manager.delete_agent("planner", force=True)
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_agent_reactivation_discards_stale_force_delete_report(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "force-report-reactivation.sqlite")
        manager = _manager(store, _Runtime())
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )
            assert await manager.delete_agent("planner")
            manager._pending_force_delete_reports["planner"] = {
                "cancelled_task_ids": ("old-task",)
            }

            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            )

            assert "planner" not in manager._pending_force_delete_reports
            assert not await store.is_agent_deleted("planner")
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delagent_rejects_static_and_explicit_agents(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _Runtime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        registry.register(
            "reviewer", runtime, profile=AgentProfile(agent_id="reviewer")
        )
        manager = TaskManager(
            store, registry, worker_count=0, allow_dynamic_agents=True
        )
        await manager.start()
        try:
            for agent_id, expected in (
                ("codex", "default Agent"),
                ("reviewer", "only dynamically created Agents"),
            ):
                try:
                    await manager.delete_agent(agent_id)
                except ValueError as exc:
                    assert expected in str(exc)
                else:
                    raise AssertionError("deletion unexpectedly succeeded")
        finally:
            await manager.stop()

    asyncio.run(scenario())
