"""Focused safety contracts for the process-isolated Agent proxy."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
from typing import Any

import pytest

from src.agents.base import AgentTask, ReplyTarget
from src.agents.workspace import (
    EXECUTION_WORKSPACE_KEY,
    build_workspace_snapshot,
)
from src.runtime.process_agent import (
    ProcessAgentCapacityError,
    ProcessAgentError,
    ProcessAgentRuntime,
    ProcessAgentStartupError,
)


_SAFE_FACTORY = "tests.process_probe_backend:barrier_probe_backend_factory"
_CONTAMINATED_FACTORY = (
    "tests.process_probe_backend:contaminated_backend_factory"
)


async def _wait_for_path(path: Path, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {path.name}")
        await asyncio.sleep(0.01)


def _runtime(
    agent_id: str,
    tmp_path: Path,
    **overrides: Any,
) -> ProcessAgentRuntime:
    values: dict[str, Any] = {
        "cwd": str(tmp_path),
        "backend_factory": _SAFE_FACTORY,
        "start_timeout": 5,
        "stop_timeout": 5,
        "event_ack_timeout": 5,
    }
    values.update(overrides)
    return ProcessAgentRuntime.create(agent_id, **values)


def test_interrupt_before_backend_registration_always_returns_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("alpha", tmp_path)
        marker = tmp_path / "pre-registration.entered"
        task = AgentTask(
            task_id="immediate-interrupt",
            execution_id="execution-immediate-interrupt",
            agent_id="alpha",
            conversation_id="conversation-alpha",
            inputs="wait",
            metadata={
                "probe_root": str(tmp_path / "barriers"),
                "pre_register_marker": str(marker),
                "pre_register_delay": 30,
            },
        )
        try:
            running = asyncio.create_task(runtime.run(task))
            await _wait_for_path(marker)
            assert await runtime.interrupt(task.task_id) is True
            result = await asyncio.wait_for(running, timeout=5)
            assert result.status == "interrupted"
            assert result.interrupted is True
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_failed_force_cleanup_retains_process_and_capacity_ownership(
    tmp_path: Path,
) -> None:
    class StubbornProcess:
        def __init__(self) -> None:
            self.pid = 987_654_321
            self.exitcode = None
            self.read_fd, self.write_fd = os.pipe()
            self.sentinel = self.read_fd

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        def join(self, _timeout: float | None = None) -> None:
            return None

        def close(self) -> None:
            os.close(self.read_fd)
            os.close(self.write_fd)

    async def scenario() -> None:
        runtime = _runtime(
            "alpha", tmp_path, max_processes=1, stop_timeout=0.02
        )
        process = StubbornProcess()
        runtime._process_budget.reserve(runtime)
        runtime._budget_reserved = True
        runtime._process = process
        try:
            with pytest.raises(ProcessAgentError, match="did not exit"):
                await runtime._force_cleanup_current_generation()
            assert runtime._process is process
            assert runtime._budget_reserved is True
            assert runtime._process_budget.active == 1
            assert runtime.health == "stopping"
        finally:
            runtime._release_budget()
            process.close()

    asyncio.run(scenario())


def test_repeated_stop_and_capacity_handoff_has_one_join_owner(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        alpha = _runtime("alpha", tmp_path, max_processes=1)
        beta = alpha.for_agent("beta")
        try:
            for _ in range(6):
                await alpha.start()
                with pytest.raises(ProcessAgentCapacityError):
                    await beta.start()
                await alpha.stop()
                assert alpha._process_budget.active == 0

                await beta.start()
                with pytest.raises(ProcessAgentCapacityError):
                    await alpha.start()
                await beta.stop()
                assert alpha._process_budget.active == 0
        finally:
            await asyncio.gather(alpha.stop(), beta.stop(), return_exceptions=True)

    asyncio.run(scenario())


def test_artifact_relay_uses_original_parent_task_and_reply_target(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        observed: list[tuple[Any, dict[str, Any]]] = []

        async def publish(task: Any, **proposal: Any) -> dict[str, str]:
            observed.append((task, dict(proposal)))
            return {"attachment_id": "managed-image-1"}

        runtime = _runtime(
            "alpha", tmp_path, image_output_publisher=publish
        )
        target = ReplyTarget(
            channel="wechat",
            bot_id="bot-1",
            external_user_id="user-1",
            session_id="session-1",
            source_message_id="message-1",
        )
        task = AgentTask(
            task_id="artifact-task",
            execution_id="execution-artifact-task",
            agent_id="alpha",
            conversation_id="conversation-alpha",
            reply_target=target,
            inputs="make an image",
            metadata={
                "probe_artifact": True,
                "saved_path": str(tmp_path / "generated.png"),
            },
        )
        try:
            result = await runtime.run(task)
            assert result.status == "completed"
            assert result.content == "managed-image-1"
            assert result.metadata["attachment_id"] == "managed-image-1"
            assert len(observed) == 1
            published_task, proposal = observed[0]
            assert published_task is task
            assert published_task.reply_target is target
            assert proposal == {
                "result": "",
                "saved_path": str(tmp_path / "generated.png"),
                "source_item_id": "image-item-1",
                "source_item_ordinal": 1,
            }
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_workspace_snapshots_cross_one_process_without_changing_process_cwd(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    first_directory = workspace / "first"
    second_directory = workspace / "second"
    first_directory.mkdir(parents=True)
    second_directory.mkdir()
    first_snapshot = build_workspace_snapshot(workspace, first_directory)
    second_snapshot = build_workspace_snapshot(workspace, second_directory)

    async def scenario() -> None:
        runtime = _runtime("alpha", workspace)
        try:
            first = await runtime.run(
                AgentTask(
                    task_id="workspace-first",
                    execution_id="execution-workspace-first",
                    agent_id="alpha",
                    conversation_id="conversation-alpha",
                    inputs="inspect first",
                    metadata={
                        "probe_workspace": True,
                        EXECUTION_WORKSPACE_KEY: first_snapshot,
                    },
                )
            )
            first_pid = runtime.pid
            second = await runtime.run(
                AgentTask(
                    task_id="workspace-second",
                    execution_id="execution-workspace-second",
                    agent_id="alpha",
                    conversation_id="conversation-alpha",
                    inputs="inspect second",
                    metadata={
                        "probe_workspace": True,
                        EXECUTION_WORKSPACE_KEY: second_snapshot,
                    },
                )
            )

            assert first.status == second.status == "completed"
            assert isinstance(first_pid, int) and first_pid > 0
            assert runtime.pid == first_pid
            assert first.metadata["pid"] == second.metadata["pid"] == first_pid
            assert first.metadata["configured_cwd"] == str(workspace.resolve())
            assert second.metadata["configured_cwd"] == str(workspace.resolve())
            assert first.metadata["process_cwd"] == second.metadata["process_cwd"]
            assert first.metadata["process_cwd"] not in {
                str(first_directory.resolve()),
                str(second_directory.resolve()),
            }
            assert first.metadata["execution_workspace"] == first_snapshot
            assert second.metadata["execution_workspace"] == second_snapshot
            assert (
                first.metadata["execution_workspace"]["path"]
                != second.metadata["execution_workspace"]["path"]
            )
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_compaction_control_runs_in_the_selected_agent_process(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("alpha", tmp_path)
        try:
            await runtime.start()
            child_pid = runtime.pid
            result = await runtime.compact_session(
                "conversation-alpha",
                thread_id="thread-alpha",
                mode_id="execute",
                profile_version=3,
                policy_version=2,
                session_role={"kind": "test"},
            )
            assert result == {
                "thread_id": "thread-alpha",
                "compaction": {
                    "agent_id": "alpha",
                    "conversation_id": "conversation-alpha",
                    "pid": child_pid,
                },
            }
            assert runtime.pid == child_pid
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_shared_process_capacity_releases_only_after_child_stop(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        first = _runtime("alpha", tmp_path, max_processes=1)
        second = first.for_agent("beta")
        first_pid: int | None = None
        try:
            await first.start()
            first_pid = first.pid
            assert isinstance(first_pid, int) and first_pid > 0
            with pytest.raises(ProcessAgentCapacityError):
                await second.start()
            assert second.pid is None

            await first.stop()
            await second.start()
            assert isinstance(second.pid, int) and second.pid > 0
            assert second.pid != first_pid
        finally:
            await asyncio.gather(first.stop(), second.stop(), return_exceptions=True)

    asyncio.run(scenario())


def test_child_import_boundary_is_clean_and_rejects_sqlite_backend(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clean = _runtime("alpha", tmp_path)
        contaminated = _runtime(
            "beta", tmp_path, backend_factory=_CONTAMINATED_FACTORY
        )
        try:
            result = await clean.run(
                AgentTask(
                    task_id="boundary-probe",
                    execution_id="execution-boundary-probe",
                    agent_id="alpha",
                    conversation_id="conversation-alpha",
                    inputs="probe",
                    metadata={"probe_import_boundary": True},
                )
            )
            assert result.status == "completed"
            assert result.metadata["forbidden_modules"] == []

            with pytest.raises(ProcessAgentStartupError, match="SQLite|sqlite"):
                await contaminated.start()
            assert contaminated.pid is None
        finally:
            await asyncio.gather(
                clean.stop(), contaminated.stop(), return_exceptions=True
            )

    asyncio.run(scenario())
