"""Focused TaskManager coverage for durable context compaction."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from src.agents.base import AgentResult, ReplyTarget
from src.agents.workspace import EXECUTION_WORKSPACE_KEY
from src.runtime.identity import conversation_id
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.roles import validate_role_snapshot
from src.runtime.sqlite_store import SQLiteStore


class _CompactionRuntime:
    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id
        self.compact_calls: list[tuple[str, dict[str, Any]]] = []
        self.compact_entered = asyncio.Event()
        self.compact_release: asyncio.Event | None = None
        self.active_compactions = 0
        self.max_active_compactions = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="done")

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        assert include_hidden is False
        return [
            {
                "id": "provider-model",
                "displayName": "Provider model",
                "isDefault": True,
                "supportedReasoningEfforts": [],
            }
        ]

    async def compact_session(
        self,
        compact_conversation_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.compact_calls.append(
            (str(compact_conversation_id), dict(kwargs))
        )
        self.active_compactions += 1
        self.max_active_compactions = max(
            self.max_active_compactions,
            self.active_compactions,
        )
        self.compact_entered.set()
        try:
            if self.compact_release is not None:
                await self.compact_release.wait()
            return {
                "thread_id": str(kwargs["thread_id"]),
                "compacted": True,
            }
        finally:
            self.active_compactions -= 1


def _scope(*, session_id: str = "session-a") -> dict[str, str]:
    return {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": "user",
        "session_id": session_id,
    }


def _manager(
    database: Path,
    workspace: Path,
) -> tuple[TaskManager, _CompactionRuntime, _CompactionRuntime]:
    codex_runtime = _CompactionRuntime("codex")
    writer_runtime = _CompactionRuntime("writer")
    writer_profile = replace(
        codex_profile(profile_version=4),
        agent_id="writer",
        display_name="Writer",
    )
    registry = AgentRegistry()
    registry.register("codex", codex_runtime, profile=codex_profile())
    registry.register("writer", writer_runtime, profile=writer_profile)
    manager = TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="codex",
        workspace_root=workspace,
        reconcile_interval=None,
    )
    return manager, codex_runtime, writer_runtime


async def _seed_writer_context(
    manager: TaskManager,
    workspace: Path,
    *,
    thread_id: str = "thread-writer",
) -> tuple[Any, dict[str, Any], dict[str, str]]:
    scope = _scope()
    await manager.set_active_agent("writer", **scope)
    await manager.set_mode("plan", agent_id="writer", **scope)
    await manager.set_model(
        "provider-model",
        agent_id="writer",
        **scope,
    )
    role_result = await manager.set_system_role(
        "Write concise release notes.",
        kind="custom",
        agent_id="writer",
        actor="user",
        **scope,
    )
    session_role = validate_role_snapshot(role_result)
    selected_directory = workspace / "writer-directory"
    selected_directory.mkdir()
    await manager.set_working_directory(
        "writer-directory",
        agent_id="writer",
        **scope,
    )
    task = await manager.submit(
        {"text": "seed context"},
        ReplyTarget(**scope),
    )
    assert await manager.store.set_task_thread(
        task.task_id,
        thread_id=thread_id,
    )
    return task, session_role, scope


def test_compact_resolves_and_forwards_the_exact_current_agent_binding(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        manager, codex_runtime, writer_runtime = _manager(
            tmp_path / "runtime.sqlite",
            workspace,
        )
        await manager.start()
        try:
            task, session_role, scope = await _seed_writer_context(
                manager,
                workspace,
            )
            result = await manager.compact_session(actor="user", **scope)

            expected_conversation = conversation_id(
                "wechat", "bot", "user", "session-a", "writer"
            )
            assert result == {
                "thread_id": "thread-writer",
                "compacted": True,
            }
            assert not codex_runtime.compact_calls
            assert len(writer_runtime.compact_calls) == 1
            called_conversation, kwargs = writer_runtime.compact_calls[0]
            assert called_conversation == expected_conversation
            assert kwargs["thread_id"] == "thread-writer"
            assert kwargs["agent_id"] == "writer"
            assert kwargs["mode_id"] == task.mode_id == "plan"
            assert kwargs["profile_version"] == task.profile_version == 4
            assert kwargs["policy_version"] == task.policy_version == 2
            assert kwargs["model"] == task.model == "provider-model"
            assert kwargs["session_role"] == session_role

            metadata = kwargs["metadata"]
            assert metadata["session_role"] == session_role
            assert metadata["profile"]["agent_id"] == "writer"
            assert metadata["profile"]["profile_version"] == 4
            assert metadata["mode"]["mode_id"] == "plan"
            assert metadata["mode"]["policy_version"] == 2
            assert metadata["effective_policy"]["mode_id"] == "plan"
            assert metadata[EXECUTION_WORKSPACE_KEY] == (
                task.metadata[EXECUTION_WORKSPACE_KEY]
            )
            assert metadata[EXECUTION_WORKSPACE_KEY]["path"] == str(
                (workspace / "writer-directory").resolve()
            )

            with pytest.raises(
                ValueError,
                match="conversation does not match",
            ):
                await manager.compact_session(
                    conversation_id=conversation_id(
                        "wechat", "bot", "user", "session-a", "codex"
                    ),
                    actor="user",
                    **scope,
                )
            assert len(writer_runtime.compact_calls) == 1

            with pytest.raises(
                RuntimeError,
                match="no Codex thread is bound",
            ):
                await manager.compact_session(
                    actor="user",
                    **_scope(session_id="empty-session"),
                )
            assert len(writer_runtime.compact_calls) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_compact_shares_the_per_session_control_lock(tmp_path: Path) -> None:
    async def scenario() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        manager, _codex_runtime, writer_runtime = _manager(
            tmp_path / "runtime.sqlite",
            workspace,
        )
        await manager.start()
        try:
            _task, _role, scope = await _seed_writer_context(
                manager,
                workspace,
            )
            writer_runtime.compact_release = asyncio.Event()
            first = asyncio.create_task(manager.compact_session(**scope))
            await asyncio.wait_for(
                writer_runtime.compact_entered.wait(),
                timeout=1,
            )
            second = asyncio.create_task(manager.compact_session(**scope))
            await asyncio.sleep(0)
            assert len(writer_runtime.compact_calls) == 1
            assert not first.done()
            assert not second.done()

            writer_runtime.compact_release.set()
            first_result, second_result = await asyncio.gather(first, second)
            assert first_result["thread_id"] == "thread-writer"
            assert second_result["thread_id"] == "thread-writer"
            assert len(writer_runtime.compact_calls) == 2
            assert writer_runtime.max_active_compactions == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_compact_rejects_an_active_task_before_entering_the_runtime(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        manager, _codex_runtime, writer_runtime = _manager(
            tmp_path / "runtime.sqlite",
            workspace,
        )
        await manager.start()
        try:
            task, _role, scope = await _seed_writer_context(
                manager,
                workspace,
            )
            claim = await manager.store.claim_task_by_id(
                task.task_id,
                "test-worker",
            )
            assert claim is not None

            with pytest.raises(
                RuntimeError,
                match="cannot compact while a task is running",
            ):
                await manager.compact_session(**scope)
            assert not writer_runtime.compact_calls
        finally:
            await manager.stop()

    asyncio.run(scenario())
