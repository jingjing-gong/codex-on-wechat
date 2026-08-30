"""Lifecycle coverage for mailbox workers that follow dynamic Agents."""

from __future__ import annotations

import asyncio

from src.agents.base import AgentResult
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.worker import AgentMailboxSupervisor


class _Runtime:
    agent_id = "codex"

    def __init__(self) -> None:
        self.tasks = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit):
        self.tasks.append(task)
        return AgentResult(task_id=task.task_id, content="handled")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def test_mailbox_supervisor_adds_worker_for_agent_created_after_startup(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _Runtime()
        registry = AgentRegistry()
        registry.register(
            "codex", runtime, profile=codex_profile(default_mode_id="chat")
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        supervisor = AgentMailboxSupervisor(store, registry, poll_interval=0.01)
        await manager.start()
        running = asyncio.create_task(supervisor.run())
        try:
            assert await manager.ensure_agent("planner")
            item = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="review this",
                request_id="dynamic-mailbox-request",
            )

            async def processed() -> bool:
                for _ in range(100):
                    current = await store.get_mailbox_item(item.mailbox_id)
                    if current is not None and current.state.value == "processed":
                        return True
                    await asyncio.sleep(0.01)
                return False

            assert await processed()
            assert supervisor.destination_agent_ids == ("codex", "planner")
            assert [task.agent_id for task in runtime.tasks] == ["planner"]
        finally:
            supervisor.stop()
            await asyncio.wait_for(running, timeout=1)
            assert supervisor.destination_agent_ids == ()
            await manager.stop()

    asyncio.run(scenario())


def test_mailbox_supervisor_bounds_shutdown_of_stuck_runtime(tmp_path):
    class BlockingRuntime(_Runtime):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()

        async def run(self, task, _emit):
            self.tasks.append(task)
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = BlockingRuntime()
        registry = AgentRegistry()
        registry.register(
            "codex", runtime, profile=codex_profile(default_mode_id="chat")
        )
        manager = TaskManager(store, registry, worker_count=0)
        supervisor = AgentMailboxSupervisor(
            store,
            registry,
            poll_interval=0.01,
            stop_timeout=0.02,
        )
        await manager.start()
        running = asyncio.create_task(supervisor.run())
        try:
            await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="never finishes",
                request_id="stuck-mailbox-request",
            )
            await asyncio.wait_for(runtime.started.wait(), timeout=1)

            supervisor.stop()
            await asyncio.wait_for(running, timeout=0.5)

            assert runtime.cancelled.is_set()
            assert supervisor.destination_agent_ids == ()
        finally:
            if not running.done():
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            await manager.stop()

    asyncio.run(scenario())


def test_mailbox_supervisor_schedules_cancel_on_its_owner_loop():
    class Store:
        def __init__(self) -> None:
            self.cancel_loops = []

        async def cancel_active_mailbox_invocation(
            self, agent_id: str, **_kwargs
        ) -> bool:
            assert agent_id == "codex"
            self.cancel_loops.append(asyncio.get_running_loop())
            return True

    class Registry:
        def list(self):
            return []

    async def scenario() -> None:
        store = Store()
        supervisor = AgentMailboxSupervisor(
            store, Registry(), poll_interval=0.01
        )
        running = asyncio.create_task(supervisor.run())
        try:
            for _ in range(100):
                if supervisor._owner_loop is not None:
                    break
                await asyncio.sleep(0.01)
            owner_loop = asyncio.get_running_loop()
            assert supervisor._owner_loop is owner_loop

            changed = await asyncio.to_thread(
                lambda: asyncio.run(supervisor.request_cancel("codex"))
            )

            assert changed
            assert store.cancel_loops == [owner_loop]
        finally:
            supervisor.stop()
            await asyncio.wait_for(running, timeout=1)

    asyncio.run(scenario())
