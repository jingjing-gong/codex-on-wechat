from __future__ import annotations

import asyncio
import functools
import json
from pathlib import Path
import subprocess
import sys

import pytest

import src.codex_wechat_bot as bot
from src.agents.base import AgentResult
from src.channels.wechat import _format_agent
from src.codex_wechat_bot import (
    _DEFAULT_MAX_AGENT_PROCESSES,
    _agent_supervisor_worker_count,
)
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


def _async_test(function):
    """Run one coroutine test without adding a pytest async plugin."""

    @functools.wraps(function)
    def run(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return run


class _ProcessLikeRuntime:
    process_isolated = True
    _next_pid = 41_000

    def __init__(self, agent_id: str, *, children=None) -> None:
        self.agent_id = agent_id
        self.children = [] if children is None else children
        type(self)._next_pid += 1
        self.pid = type(self)._next_pid
        self.generation = 1
        self.health = "new"
        self.started = 0
        self.stopped = 0
        self.fail_next_stop = False

    def for_agent(self, agent_id: str):
        child = type(self)(agent_id, children=self.children)
        self.children.append(child)
        return child

    async def start(self) -> None:
        self.started += 1
        self.health = "ready"

    async def stop(self) -> None:
        if self.fail_next_stop:
            self.fail_next_stop = False
            raise RuntimeError("child shutdown is not yet proven")
        self.stopped += 1
        self.health = "stopped"

    async def run(self, task, emit):
        return AgentResult(task_id=task.task_id)

    async def interrupt(self, task_id: str) -> bool:
        return False


def test_supervisor_worker_count_ignores_legacy_single_worker(monkeypatch, caplog):
    monkeypatch.setenv("CODEX_WECHAT_WORKERS", "1")
    monkeypatch.delenv("CODEX_WECHAT_MAX_AGENT_PROCESSES", raising=False)

    assert _agent_supervisor_worker_count() == _DEFAULT_MAX_AGENT_PROCESSES
    assert "CODEX_WECHAT_WORKERS is deprecated and ignored" in caplog.text


def test_production_builder_constructs_only_process_proxy(monkeypatch, tmp_path: Path):
    captured = {}

    class Proxy:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(bot, "ProcessAgentRuntime", Proxy)
    publisher = object()
    issuer = object()
    workspace = tmp_path / "workspace"
    managed = tmp_path / "attachments"
    socket = tmp_path / "agent.sock"
    skills = (tmp_path / "skills",)

    runtime = bot._build_process_agent_runtime(
        workspace_path=workspace,
        turn_timeout=90,
        managed_root=managed,
        skill_roots=skills,
        image_output_publisher=publisher,
        agent_socket=socket,
        agent_bridge_capability_issuer=issuer,
        max_processes=8,
    )

    assert isinstance(runtime, Proxy)
    assert captured == {
        "agent_id": "codex",
        "max_processes": 8,
        "cwd": str(workspace),
        "turn_timeout": 90,
        "managed_root": managed,
        "trusted_skill_roots": skills,
        "image_output_publisher": publisher,
        "agent_bridge_command": (
            bot.sys.executable,
            "-m",
            "src.agent_cli",
            "--socket",
            str(socket),
        ),
        "agent_bridge_capability_issuer": issuer,
    }
    assert not hasattr(bot, "CodexRuntime")


def test_production_import_graph_spawns_clean_agent_child(tmp_path: Path):
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("process_launcher_probe.py")),
            str(tmp_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    status = json.loads(completed.stdout.strip().splitlines()[-1])
    assert status["child_pid"] > 0
    assert status["generation"] == 1
    assert status["health"] == "ready"


def test_process_agent_format_exposes_pid_generation_and_health():
    rendered = _format_agent(
        {
            "agent_id": "planner",
            "display_name": "planner",
            "process_isolated": True,
            "pid": 43210,
            "generation": 3,
            "health": "ready",
        },
        active=True,
    )

    assert "**(current)**" in rendered
    assert "pid `43210`" in rendered
    assert "generation `3`" in rendered
    assert "health `ready`" in rendered


@_async_test
async def test_production_gate_rejects_in_process_runtime(tmp_path: Path):
    class SharedRuntime:
        agent_id = "codex"
        started = 0

        async def start(self):
            self.started += 1

        async def stop(self):
            return None

        async def run(self, task, emit):
            return AgentResult(task_id=task.task_id)

        async def interrupt(self, task_id):
            return False

    runtime = SharedRuntime()
    registry = AgentRegistry()
    registry.register(
        "codex",
        runtime,
        profile=codex_profile(
            profile_version=3,
            default_mode_id="execute",
            allow_dynamic_peers=True,
        ),
    )
    manager = TaskManager(
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        require_process_isolation=True,
    )

    with pytest.raises(RuntimeError, match="not process-isolated"):
        await manager.start()
    assert runtime.started == 0


@_async_test
async def test_production_gate_rejects_shared_agent_pid(tmp_path: Path):
    first = _ProcessLikeRuntime("codex")
    second = _ProcessLikeRuntime("reviewer")
    second.pid = first.pid
    registry = AgentRegistry()
    registry.register(
        "codex",
        first,
        profile=codex_profile(
            profile_version=3,
            default_mode_id="execute",
            allow_dynamic_peers=True,
        ),
    )
    registry.register("reviewer", second)
    manager = TaskManager(
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        require_process_isolation=True,
    )

    with pytest.raises(RuntimeError, match="process identity is shared"):
        await manager.start()
    assert first.stopped == 1
    assert second.stopped == 1


@_async_test
async def test_registry_retains_uncertain_process_for_stop_retry():
    runtime = _ProcessLikeRuntime("codex")
    registry = AgentRegistry({"codex": runtime})
    await registry.start()
    runtime.fail_next_stop = True

    with pytest.raises(RuntimeError, match="shutdown is not yet proven"):
        await registry.stop()

    assert registry.started is True
    assert registry.is_started("codex") is True
    await registry.stop()
    assert registry.started is False
    assert runtime.stopped == 1


@_async_test
async def test_dynamic_agent_owns_and_stops_independent_runtime(tmp_path: Path):
    template = _ProcessLikeRuntime("codex")
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
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        allow_dynamic_agents=True,
    )

    await manager.start()
    try:
        assert await manager.ensure_agent("planner")
        assert len(template.children) == 1
        planner = template.children[0]
        assert planner.agent_id == "planner"
        assert planner.pid != template.pid
        assert planner.started == 1

        records = {
            record["agent_id"]: record
            for record in await manager.list_agents()
        }
        assert records["codex"]["pid"] == template.pid
        assert records["planner"]["pid"] == planner.pid
        assert records["planner"]["health"] == "ready"

        assert await manager.delete_agent("planner")
        assert planner.stopped == 1
        assert registry.registration("planner") is None
    finally:
        await manager.stop()


@_async_test
async def test_lost_peer_does_not_block_distinct_agent_ensure_or_switch(
    tmp_path: Path,
):
    template = _ProcessLikeRuntime("codex")
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
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        allow_dynamic_agents=True,
        require_process_isolation=True,
    )

    await manager.start()
    try:
        assert await manager.ensure_agent("alpha")
        alpha = template.children[0]
        alpha.health = "lost"

        assert await manager.ensure_agent("beta")
        beta = template.children[1]
        assert beta.health == "ready"
        assert alpha.health == "lost"

        # `/agent beta` calls this path. The unrelated lost alpha process must
        # not couple or veto beta's healthy route commit.
        await manager.set_active_agent(
            "beta",
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
        )
        assert (
            await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
            )
            == "beta"
        )

        # Selecting alpha itself still requires liveness; its process proxy is
        # restarted and proven rather than routing to a LOST generation.
        await manager.ensure_agent("alpha")
        assert alpha.started == 2
        assert alpha.health == "ready"
    finally:
        await manager.stop()


@_async_test
async def test_new_selected_agent_must_be_ready(tmp_path: Path):
    class UnreadyChild(_ProcessLikeRuntime):
        async def start(self) -> None:
            self.started += 1
            self.health = "lost"

    class Template(_ProcessLikeRuntime):
        def for_agent(self, agent_id: str):
            child = UnreadyChild(agent_id, children=self.children)
            self.children.append(child)
            return child

    template = Template("codex")
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
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        allow_dynamic_agents=True,
        require_process_isolation=True,
    )

    await manager.start()
    try:
        with pytest.raises(RuntimeError, match="process did not become ready"):
            await manager.ensure_agent("broken")
        assert registry.registration("broken") is None
        assert template.children[0].stopped == 1
    finally:
        await manager.stop()


@_async_test
async def test_failed_child_stop_keeps_registered_handle_for_retry(tmp_path: Path):
    template = _ProcessLikeRuntime("codex")
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
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        allow_dynamic_agents=True,
    )

    await manager.start()
    try:
        await manager.ensure_agent("planner")
        planner = template.children[0]
        planner.fail_next_stop = True

        with pytest.raises(RuntimeError, match="shutdown is not yet proven"):
            await manager.delete_agent("planner")

        # Never discard the only proxy that can still reap an uncertain child.
        registration = registry.registration("planner")
        assert registration is not None
        assert registration.runtime.pid == planner.pid

        # Retirement and shutdown are retry-safe.  The second attempt proves
        # process exit before the registry handle is removed.
        assert await manager.delete_agent("planner")
        assert planner.stopped == 1
        assert registry.registration("planner") is None
    finally:
        await manager.stop()


@_async_test
async def test_dynamic_start_rollback_clears_retained_started_marker(tmp_path: Path):
    class FailingChild(_ProcessLikeRuntime):
        def __init__(self, agent_id: str, *, children=None) -> None:
            super().__init__(agent_id, children=children)
            self.stop_attempts = 0

        async def start(self) -> None:
            raise RuntimeError("child did not become ready")

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("first reap proof failed")
            await super().stop()

    class Template(_ProcessLikeRuntime):
        def for_agent(self, agent_id: str):
            child = FailingChild(agent_id, children=self.children)
            self.children.append(child)
            return child

    template = Template("codex")
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
        SQLiteStore(tmp_path / "runtime.sqlite3"),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        allow_dynamic_agents=True,
    )

    await manager.start()
    try:
        with pytest.raises(RuntimeError, match="did not become ready"):
            await manager.ensure_agent("planner")
        failed = template.children[0]
        assert failed.stop_attempts == 2
        assert registry.registration("planner") is None
        assert registry.is_started("planner") is False
    finally:
        await manager.stop()
