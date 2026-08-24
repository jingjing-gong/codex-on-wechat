"""Focused execution-workspace boundary tests for ``CodexRuntime``."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.agents.base import AgentTask
from src.agents.codex_runtime import CodexRuntime
from src.agents.workspace import (
    EXECUTION_WORKSPACE_KEY,
    WorkspaceError,
    build_workspace_snapshot,
    validate_workspace_snapshot,
)


class _Turn:
    def stream(self):
        async def values():
            yield "workspace inspected"

        return values()


class _Thread:
    id = "workspace-thread"

    def __init__(self) -> None:
        self.turn_cwds: list[str | None] = []

    async def turn(self, _input: Any, *, cwd: str | None = None, **_: Any) -> _Turn:
        self.turn_cwds.append(cwd)
        return _Turn()


class _Codex:
    def __init__(self) -> None:
        self.thread = _Thread()
        self.start_cwds: list[str | None] = []
        self.resume_cwds: list[tuple[str, str | None]] = []

    async def thread_start(self, *, cwd: str | None = None, **_: Any) -> _Thread:
        self.start_cwds.append(cwd)
        return self.thread

    async def thread_resume(
        self,
        thread_id: str,
        *,
        cwd: str | None = None,
        **_: Any,
    ) -> _Thread:
        self.resume_cwds.append((thread_id, cwd))
        return self.thread


def _task(
    task_id: str,
    snapshot: dict[str, Any] | None = None,
    *,
    conversation_id: str = "workspace-conversation",
    thread_id: str | None = None,
) -> AgentTask:
    metadata = (
        {EXECUTION_WORKSPACE_KEY: snapshot} if snapshot is not None else {}
    )
    return AgentTask(
        task_id=task_id,
        agent_id="codex",
        conversation_id=conversation_id,
        thread_id=thread_id,
        mode_id="chat",
        profile_version=1,
        policy_version=1,
        inputs="inspect the workspace",
        metadata=metadata,
    )


def test_tasks_select_turn_cwd_without_splitting_the_thread(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    first = root / "first"
    second = root / "second"
    first.mkdir(parents=True)
    second.mkdir()
    first_snapshot = build_workspace_snapshot(root, first)
    second_snapshot = build_workspace_snapshot(root, second)

    async def scenario() -> None:
        codex = _Codex()
        runtime = CodexRuntime(codex=codex, cwd=str(root))

        first_result = await runtime.run(_task("task-first", first_snapshot))
        second_result = await runtime.run(_task("task-second", second_snapshot))

        assert first_result.status == "completed", first_result.error
        assert second_result.status == "completed", second_result.error
        assert codex.start_cwds == [str(first.resolve())]
        assert codex.resume_cwds == []
        assert codex.thread.turn_cwds == [
            str(first.resolve()),
            str(second.resolve()),
        ]

    asyncio.run(scenario())


def test_thread_start_resume_and_legacy_tasks_receive_the_expected_cwd(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    selected = root / "selected"
    selected.mkdir(parents=True)
    snapshot = build_workspace_snapshot(root, selected)

    async def scenario() -> None:
        started = _Codex()
        start_runtime = CodexRuntime(codex=started, cwd=str(root))
        started_result = await start_runtime.run(_task("start", snapshot))
        assert started_result.status == "completed", started_result.error
        assert started.start_cwds == [str(selected.resolve())]

        resumed = _Codex()
        resume_runtime = CodexRuntime(codex=resumed, cwd=str(root))
        result = await resume_runtime.run(
            _task("resume", snapshot, thread_id="workspace-thread")
        )
        assert result.status == "completed", result.error
        assert resumed.resume_cwds == [
            ("workspace-thread", str(selected.resolve()))
        ]

        legacy = _Codex()
        legacy_runtime = CodexRuntime(codex=legacy, cwd=str(root))
        legacy_result = await legacy_runtime.run(_task("legacy"))
        assert legacy_result.status == "completed", legacy_result.error
        assert legacy.start_cwds == [str(root)]
        assert legacy.thread.turn_cwds == [str(root)]

    asyncio.run(scenario())


def test_invalid_snapshot_fails_before_any_sdk_operation(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    selected = root / "selected"
    selected.mkdir(parents=True)
    snapshot = build_workspace_snapshot(root, selected)
    snapshot["target_st_ino"] += 1

    async def scenario() -> None:
        codex = _Codex()
        runtime = CodexRuntime(codex=codex, cwd=str(root))
        result = await runtime.run(_task("tampered", snapshot))

        assert result.status == "failed"
        assert "identity has changed" in str(result.error)
        assert codex.start_cwds == []
        assert codex.resume_cwds == []
        assert codex.thread.turn_cwds == []
        assert runtime._started is False

    asyncio.run(scenario())


def test_workspace_rejects_escape_and_replaced_directory(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    selected = root / "selected"
    outside = tmp_path / "outside"
    selected.mkdir(parents=True)
    outside.mkdir()

    with pytest.raises(WorkspaceError, match="outside"):
        build_workspace_snapshot(root, outside)

    snapshot = build_workspace_snapshot(root, selected)
    old_selected = root / "selected-old"
    selected.rename(old_selected)
    selected.mkdir()
    with pytest.raises(WorkspaceError, match="identity has changed"):
        validate_workspace_snapshot(snapshot, root)


def test_workspace_is_revalidated_after_thread_creation(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    selected = root / "selected"
    selected.mkdir(parents=True)
    snapshot = build_workspace_snapshot(root, selected)

    class ReplacingCodex(_Codex):
        async def thread_start(
            self,
            *,
            cwd: str | None = None,
            **kwargs: Any,
        ) -> _Thread:
            thread = await super().thread_start(cwd=cwd, **kwargs)
            selected.rename(root / "selected-old")
            selected.mkdir()
            return thread

    async def scenario() -> None:
        codex = ReplacingCodex()
        runtime = CodexRuntime(codex=codex, cwd=str(root))
        result = await runtime.run(_task("replace-at-boundary", snapshot))

        assert result.status == "failed"
        assert "identity has changed" in str(result.error)
        assert codex.start_cwds == [str(selected.resolve())]
        assert codex.thread.turn_cwds == []

    asyncio.run(scenario())


def test_skill_catalog_cache_is_scoped_to_requested_cwd(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    class SkillCodex(_Codex):
        def __init__(self) -> None:
            super().__init__()
            self.skill_calls: list[tuple[bool, str | None]] = []

        async def list_skills(
            self,
            *,
            refresh: bool = False,
            cwd: str | None = None,
        ) -> list[dict[str, Any]]:
            self.skill_calls.append((refresh, cwd))
            assert cwd is not None
            return [
                {
                    "name": Path(cwd).name,
                    "path": str(Path(cwd) / "skills" / Path(cwd).name),
                    "enabled": True,
                }
            ]

    async def scenario() -> None:
        codex = SkillCodex()
        runtime = CodexRuntime(codex=codex, cwd=str(tmp_path))

        first_catalog = await runtime.list_skills(cwd=str(first))
        assert await runtime.list_skills(cwd=str(first)) == first_catalog
        second_catalog = await runtime.list_skills(cwd=str(second))

        assert first_catalog[0]["name"] == "first"
        assert second_catalog[0]["name"] == "second"
        assert codex.skill_calls == [
            (False, str(first)),
            (False, str(second)),
        ]

    asyncio.run(scenario())
