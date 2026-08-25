"""End-to-end contract for one persistent runtime process per Agent.

These tests deliberately use filesystem barriers instead of coroutine events.
The backend runs in a fresh interpreter, so a marker proves that the matching
child process has entered ``AgentRuntime.run``.  Requiring two markers before
creating either release file distinguishes real cross-process overlap from two
queued proxy coroutines in the supervisor.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import time
from typing import Any

import pytest

from src.agents.base import AgentEvent, AgentTask
from src.runtime.manager import TaskManager
from src.runtime.process_agent import ProcessAgentLostError, ProcessAgentRuntime
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


_BACKEND_FACTORY = (
    "tests.process_probe_backend:barrier_probe_backend_factory"
)


def _task(root: Path, agent_id: str, task_id: str) -> AgentTask:
    return AgentTask(
        task_id=task_id,
        execution_id=f"execution-{task_id}",
        agent_id=agent_id,
        conversation_id=f"conversation-{agent_id}",
        inputs=task_id,
        metadata={"probe_root": str(root)},
    )


async def _wait_for_path(path: Path, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for child marker: {path.name}")
        await asyncio.sleep(0.01)


async def _wait_for_process_exit(pid: int, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            pass
        if time.monotonic() >= deadline:
            raise AssertionError(f"Agent child PID {pid} was not reaped")
        await asyncio.sleep(0.01)


async def _wait_for_task_state(
    store: SQLiteStore,
    task_id: str,
    expected: str,
    *,
    timeout: float = 5.0,
) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        task = await store.get_task(task_id)
        raw_state = getattr(task, "state", "")
        state = getattr(raw_state, "value", raw_state)
        if str(state) == expected:
            return task
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"task {task_id} did not reach {expected}; current state is {state}"
            )
        await asyncio.sleep(0.01)


def _entry(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _runtime(agent_id: str, tmp_path: Path) -> ProcessAgentRuntime:
    return ProcessAgentRuntime.create(
        agent_id,
        cwd=str(tmp_path),
        backend_factory=_BACKEND_FACTORY,
        start_timeout=5,
        stop_timeout=5,
        event_ack_timeout=5,
    )


def _kill_runtime_generation(
    runtime: ProcessAgentRuntime,
    *,
    pid: int,
    generation: int,
) -> None:
    process = runtime._process
    if (
        process is None
        or process.exitcode is not None
        or runtime.pid != pid
        or runtime.generation != generation
        or runtime.process_group_id != pid
    ):
        raise AssertionError("runtime process generation changed before signal")
    process.kill()


def test_distinct_agents_execute_in_distinct_child_pids_and_overlap(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "barriers"
        alpha = _runtime("alpha", tmp_path)
        beta = _runtime("beta", tmp_path)
        alpha_pid: int | None = None
        beta_pid: int | None = None
        alpha_events: list[AgentEvent] = []
        beta_events: list[AgentEvent] = []

        async def capture_alpha(event: AgentEvent) -> None:
            alpha_events.append(event)

        async def capture_beta(event: AgentEvent) -> None:
            beta_events.append(event)

        try:
            await asyncio.gather(alpha.start(), beta.start())
            alpha_pid, beta_pid = alpha.pid, beta.pid
            assert isinstance(alpha_pid, int) and alpha_pid > 0
            assert isinstance(beta_pid, int) and beta_pid > 0
            assert len({os.getpid(), alpha_pid, beta_pid}) == 3

            alpha_run = asyncio.create_task(
                alpha.run(_task(root, "alpha", "alpha-one"), capture_alpha)
            )
            beta_run = asyncio.create_task(
                beta.run(_task(root, "beta", "beta-one"), capture_beta)
            )
            alpha_marker = root / "alpha-one.entered.json"
            beta_marker = root / "beta-one.entered.json"

            # Neither child is released until both runtimes have really
            # entered.  A global one-at-a-time worker cannot pass this barrier.
            await asyncio.gather(
                _wait_for_path(alpha_marker),
                _wait_for_path(beta_marker),
            )
            assert _entry(alpha_marker) == {
                "agent_id": "alpha",
                "backend_agent_id": "alpha",
                "pid": alpha_pid,
                "task_id": "alpha-one",
            }
            assert _entry(beta_marker) == {
                "agent_id": "beta",
                "backend_agent_id": "beta",
                "pid": beta_pid,
                "task_id": "beta-one",
            }

            (root / "alpha-one.release").touch()
            (root / "beta-one.release").touch()
            alpha_result, beta_result = await asyncio.gather(alpha_run, beta_run)
            assert alpha_result.status == beta_result.status == "completed"
            assert alpha_result.content == f"alpha:{alpha_pid}"
            assert beta_result.content == f"beta:{beta_pid}"
            assert [event.content for event in alpha_events] == [f"alpha:{alpha_pid}"]
            assert [event.content for event in beta_events] == [f"beta:{beta_pid}"]
        finally:
            await asyncio.gather(alpha.stop(), beta.stop(), return_exceptions=True)
        assert alpha_pid is not None and beta_pid is not None
        await asyncio.gather(
            _wait_for_process_exit(alpha_pid),
            _wait_for_process_exit(beta_pid),
        )

    asyncio.run(scenario())


def test_parent_lifetime_pipe_loss_kills_only_its_agent_group(tmp_path: Path) -> None:
    async def scenario() -> None:
        alpha = _runtime("alpha", tmp_path)
        beta = _runtime("beta", tmp_path)
        alpha_pid: int | None = None
        beta_pid: int | None = None
        try:
            await asyncio.gather(alpha.start(), beta.start())
            alpha_pid = alpha.pid
            beta_pid = beta.pid
            assert isinstance(alpha_pid, int) and alpha_pid > 0
            assert isinstance(beta_pid, int) and beta_pid > 0

            # The parent deliberately never writes to this generation-specific
            # endpoint. Closing it simulates abrupt supervisor loss. If the
            # child inherited another write end during spawn, EOF would never
            # arrive and this test would time out.
            lifetime = alpha._parent_liveness_connection
            assert lifetime is not None
            assert os.get_inheritable(lifetime.fileno()) is False
            lifetime.close()

            deadline = time.monotonic() + 5
            while alpha.health != "lost":
                if time.monotonic() >= deadline:
                    raise AssertionError("Agent did not detect parent lifetime loss")
                await asyncio.sleep(0.01)

            await _wait_for_process_exit(alpha_pid)
            assert alpha._parent_liveness_connection is None
            assert beta.pid == beta_pid
            assert beta.health == "ready"
        finally:
            await asyncio.gather(alpha.stop(), beta.stop(), return_exceptions=True)
        assert alpha_pid is not None and beta_pid is not None
        await asyncio.gather(
            _wait_for_process_exit(alpha_pid),
            _wait_for_process_exit(beta_pid),
        )

    asyncio.run(scenario())


def test_one_agent_serializes_runtime_invocations(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = tmp_path / "barriers"
        runtime = _runtime("alpha", tmp_path)

        async def discard(_event: AgentEvent) -> None:
            return None

        try:
            await runtime.start()
            first = asyncio.create_task(
                runtime.run(_task(root, "alpha", "first"), discard)
            )
            await _wait_for_path(root / "first.entered.json")
            second = asyncio.create_task(
                runtime.run(_task(root, "alpha", "second"), discard)
            )

            # Give the proxy ample opportunity to forward an unsafe second
            # invocation.  The child owns exactly one Agent execution slot.
            await asyncio.sleep(0.2)
            assert not (root / "second.entered.json").exists()

            (root / "first.release").touch()
            assert (await first).status == "completed"
            await _wait_for_path(root / "second.entered.json")
            (root / "second.release").touch()
            assert (await second).status == "completed"
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_task_manager_dynamic_agent_owns_process_and_recreation_is_fresh(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "manager-barriers"
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        template = _runtime("codex", tmp_path)
        registry = AgentRegistry()
        registry.register(
            "codex",
            template,
            profile=codex_profile(
                profile_version=3,
                default_mode_id="execute",
                allow_dynamic_peers=True,
            ),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=2,
            default_agent_id="codex",
            default_mode_id="execute",
            trusted_default_execute=True,
            allow_dynamic_agents=True,
            require_process_isolation=True,
            reconcile_interval=None,
        )
        default_pid: int | None = None
        old_planner_pid: int | None = None
        recreated_pid: int | None = None
        try:
            await manager.start()
            default_pid = template.pid
            assert isinstance(default_pid, int) and default_pid > 0

            assert await manager.ensure_agent("planner") is True
            planner = registry.require("planner")
            old_planner_pid = planner.pid
            old_planner_generation = planner.generation
            assert isinstance(old_planner_pid, int) and old_planner_pid > 0
            assert old_planner_pid != default_pid

            process_records = {
                record["agent_id"]: record
                for record in await manager.list_agents()
            }
            assert {
                key: process_records["codex"][key]
                for key in ("pid", "generation", "health", "process_isolated")
            } == {
                "pid": default_pid,
                "generation": template.generation,
                "health": "ready",
                "process_isolated": True,
            }
            assert {
                key: process_records["planner"][key]
                for key in ("pid", "generation", "health", "process_isolated")
            } == {
                "pid": old_planner_pid,
                "generation": old_planner_generation,
                "health": "ready",
                "process_isolated": True,
            }

            # Submit through the real durable manager/worker path.  The child
            # marker proves routing reached planner's runtime process rather
            # than the template runtime in the supervisor or default child.
            task_id = "manager-planner-task"
            await manager.submit(
                "probe planner",
                task_id=task_id,
                agent_id="planner",
                mode_id="chat",
                metadata={"probe_root": str(root)},
            )
            marker = root / f"{task_id}.entered.json"
            await _wait_for_path(marker)
            assert _entry(marker)["pid"] == old_planner_pid
            assert _entry(marker)["agent_id"] == "planner"
            (root / f"{task_id}.release").touch()
            await _wait_for_task_state(store, task_id, "completed")

            assert await manager.delete_agent("planner") is True
            assert registry.registration("planner") is None
            await _wait_for_process_exit(old_planner_pid)

            assert (
                await manager.ensure_agent("planner", allow_deleted=True)
                is True
            )
            recreated = registry.require("planner")
            recreated_pid = recreated.pid
            assert isinstance(recreated_pid, int) and recreated_pid > 0
            assert recreated_pid not in {default_pid, old_planner_pid}
            assert (recreated_pid, recreated.generation) != (
                old_planner_pid,
                old_planner_generation,
            )
        finally:
            await manager.stop()
        if default_pid is not None:
            await _wait_for_process_exit(default_pid)
        if old_planner_pid is not None:
            await _wait_for_process_exit(old_planner_pid)
        if recreated_pid is not None:
            await _wait_for_process_exit(recreated_pid)

    asyncio.run(scenario())


def test_dynamic_agent_restore_spawns_fresh_independent_process(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "restore.sqlite3"

        def build_manager():
            template = _runtime("codex", tmp_path)
            registry = AgentRegistry()
            registry.register(
                "codex",
                template,
                profile=codex_profile(
                    profile_version=3,
                    default_mode_id="execute",
                    allow_dynamic_peers=True,
                ),
            )
            manager = TaskManager(
                SQLiteStore(database),
                registry,
                worker_count=2,
                default_agent_id="codex",
                default_mode_id="execute",
                trusted_default_execute=True,
                allow_dynamic_agents=True,
                require_process_isolation=True,
                reconcile_interval=None,
            )
            return manager, registry, template

        first, first_registry, first_template = build_manager()
        first_default_pid: int | None = None
        first_planner_pid: int | None = None
        await first.start()
        try:
            await first.ensure_agent("planner")
            first_default_pid = first_template.pid
            first_planner_pid = first_registry.require("planner").pid
            assert isinstance(first_default_pid, int)
            assert isinstance(first_planner_pid, int)
            assert first_default_pid != first_planner_pid
        finally:
            await first.stop()
        await asyncio.gather(
            _wait_for_process_exit(first_default_pid),
            _wait_for_process_exit(first_planner_pid),
        )

        second, second_registry, second_template = build_manager()
        second_default_pid: int | None = None
        second_planner_pid: int | None = None
        await second.start()
        try:
            restored = second_registry.registration("planner")
            assert restored is not None
            second_default_pid = second_template.pid
            second_planner_pid = restored.runtime.pid
            assert isinstance(second_default_pid, int)
            assert isinstance(second_planner_pid, int)
            assert second_default_pid != second_planner_pid
            assert second_default_pid not in {
                first_default_pid,
                first_planner_pid,
            }
            assert second_planner_pid not in {
                first_default_pid,
                first_planner_pid,
            }
        finally:
            await second.stop()
        await asyncio.gather(
            _wait_for_process_exit(second_default_pid),
            _wait_for_process_exit(second_planner_pid),
        )

    asyncio.run(scenario())


def test_interrupt_is_scoped_to_the_owning_agent_process(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = tmp_path / "barriers"
        alpha = _runtime("alpha", tmp_path)
        beta = _runtime("beta", tmp_path)

        async def discard(_event: AgentEvent) -> None:
            return None

        try:
            await asyncio.gather(alpha.start(), beta.start())
            alpha_run = asyncio.create_task(
                alpha.run(_task(root, "alpha", "alpha-cancel"), discard)
            )
            beta_run = asyncio.create_task(
                beta.run(_task(root, "beta", "beta-keep"), discard)
            )
            await asyncio.gather(
                _wait_for_path(root / "alpha-cancel.entered.json"),
                _wait_for_path(root / "beta-keep.entered.json"),
            )

            assert await alpha.interrupt("alpha-cancel") is True
            alpha_result = await asyncio.wait_for(alpha_run, timeout=5)
            assert alpha_result.status == "interrupted"
            assert alpha_result.interrupted is True
            assert (root / "alpha-cancel.interrupted").exists()

            await asyncio.sleep(0.1)
            assert not beta_run.done()
            assert not (root / "beta-keep.interrupted").exists()
            assert await beta.interrupt("not-beta-keep") is False
            (root / "beta-keep.release").touch()
            assert (await beta_run).status == "completed"
        finally:
            await asyncio.gather(alpha.stop(), beta.stop(), return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires SIGKILL")
def test_child_crash_is_uncertain_and_restart_uses_new_pid_and_generation(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "barriers"
        runtime = _runtime("alpha", tmp_path)
        peer = _runtime("beta", tmp_path)
        old_pid: int | None = None
        peer_pid: int | None = None
        replacement_pid: int | None = None

        async def discard(_event: AgentEvent) -> None:
            return None

        try:
            await asyncio.gather(runtime.start(), peer.start())
            old_pid = runtime.pid
            peer_pid = peer.pid
            old_generation = runtime.generation
            peer_generation = peer.generation
            assert isinstance(old_pid, int) and old_pid > 0
            assert isinstance(peer_pid, int) and peer_pid > 0

            running = asyncio.create_task(
                runtime.run(_task(root, "alpha", "crash"), discard)
            )
            peer_running = asyncio.create_task(
                peer.run(_task(root, "beta", "peer-survives"), discard)
            )
            await asyncio.gather(
                _wait_for_path(root / "crash.entered.json"),
                _wait_for_path(root / "peer-survives.entered.json"),
            )
            _kill_runtime_generation(
                runtime,
                pid=old_pid,
                generation=old_generation,
            )

            with pytest.raises(ProcessAgentLostError) as raised:
                await asyncio.wait_for(running, timeout=5)
            assert raised.value.execution_uncertain is True
            await _wait_for_process_exit(old_pid)
            assert not peer_running.done()
            assert peer.pid == peer_pid
            assert peer.generation == peer_generation

            # Starting the same logical proxy after loss creates a fresh
            # process generation; it never adopts or overlaps the old child.
            await runtime.start()
            assert isinstance(runtime.pid, int) and runtime.pid > 0
            assert runtime.pid != old_pid
            assert runtime.generation > old_generation
            replacement_pid = runtime.pid

            followup = asyncio.create_task(
                runtime.run(_task(root, "alpha", "after-restart"), discard)
            )
            await _wait_for_path(root / "after-restart.entered.json")
            assert _entry(root / "after-restart.entered.json")["pid"] == runtime.pid
            (root / "after-restart.release").touch()
            assert (await followup).status == "completed"
            (root / "peer-survives.release").touch()
            assert (await peer_running).status == "completed"
            assert peer.pid == peer_pid
        finally:
            await asyncio.gather(runtime.stop(), peer.stop(), return_exceptions=True)
        if old_pid is not None:
            await _wait_for_process_exit(old_pid)
        if peer_pid is not None:
            await _wait_for_process_exit(peer_pid)
        if replacement_pid is not None:
            await _wait_for_process_exit(replacement_pid)

    asyncio.run(scenario())
