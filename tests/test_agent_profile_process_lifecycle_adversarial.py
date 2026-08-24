"""Adversarial real-process lifecycle coverage for ``/agent NAME PROFILE``."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.runtime.manager import TaskManager
from src.runtime.process_agent import (
    ProcessAgentCapacityError,
    ProcessAgentRuntime,
    ProcessAgentStartupError,
)
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


_PROBE_FACTORY = (
    "tests.process_probe_backend:profile_lifecycle_probe_backend_factory"
)


def _write_profile_files(root: Path) -> None:
    root.mkdir()
    for profile, model in (("qwen", "qwen-model"), ("other", "other-model")):
        (root / f"{profile}.config.toml").write_text(
            f'model = "{model}"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )


def _manager(
    database: Path,
    process_root: Path,
    *,
    max_processes: int,
) -> TaskManager:
    template = ProcessAgentRuntime.create(
        "codex",
        cwd=str(process_root),
        backend_factory=_PROBE_FACTORY,
        start_timeout=5,
        stop_timeout=5,
        event_ack_timeout=5,
        max_processes=max_processes,
    )
    registry = AgentRegistry()
    registry.register(
        "codex",
        template,
        profile=codex_profile(default_mode_id="chat"),
    )
    return TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="chat",
        allow_dynamic_agents=True,
        allowed_codex_config_profiles=("qwen", "other"),
        require_process_isolation=True,
        reconcile_interval=None,
    )


def _process_runtime(manager: TaskManager, agent_id: str) -> ProcessAgentRuntime:
    registration = manager.registry.registration(agent_id)
    assert registration is not None
    runtime = getattr(registration.runtime, "_delegate", registration.runtime)
    assert isinstance(runtime, ProcessAgentRuntime)
    return runtime


def _process_budget(manager: TaskManager) -> Any:
    runtime = manager.registry.require("codex")
    assert isinstance(runtime, ProcessAgentRuntime)
    return runtime._process_budget


def test_real_start_and_capacity_failures_are_atomic_and_reusable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_profile_files(config_home)
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        manager = _manager(
            tmp_path / "capacity.sqlite",
            tmp_path,
            max_processes=2,
        )
        await manager.start()
        budget = _process_budget(manager)
        try:
            assert budget.active == 1

            with pytest.raises(
                ProcessAgentStartupError,
                match="synthetic named child startup failure",
            ):
                await manager.set_active_agent(
                    "fail-start",
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="startup-failure",
                    session_id="default",
                )
            assert budget.active == 1
            assert manager.registry.registration("fail-start") is None
            assert await manager.store.get_profile("fail-start", 1) is None
            assert await manager.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="startup-failure",
                session_id="default",
            ) == "codex"

            await manager.set_active_agent(
                "survivor",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="survivor",
                session_id="default",
            )
            assert budget.active == 2
            assert _process_runtime(manager, "survivor").health == "ready"

            with pytest.raises(ProcessAgentCapacityError):
                await manager.set_active_agent(
                    "saturated",
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="capacity-failure",
                    session_id="default",
                )
            assert budget.active == 2
            assert manager.registry.registration("saturated") is None
            assert await manager.store.get_profile("saturated", 1) is None
            assert await manager.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="capacity-failure",
                session_id="default",
            ) == "codex"

            assert await manager.delete_agent("survivor")
            assert budget.active == 1
            await manager.set_active_agent(
                "replacement",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="replacement",
                session_id="default",
            )
            assert budget.active == 2
            assert _process_runtime(manager, "replacement").health == "ready"
        finally:
            await manager.stop()
        assert budget.active == 0

    asyncio.run(scenario())


def test_killed_profile_child_and_supervisor_restart_keep_exact_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_profile_files(config_home)
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "restart.sqlite"

        first = _manager(database, tmp_path, max_processes=3)
        await first.start()
        first_runtime: ProcessAgentRuntime | None = None
        killed_process: Any | None = None
        try:
            await first.set_active_agent(
                "researcher",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            )
            first_runtime = _process_runtime(first, "researcher")
            first_generation = first_runtime.generation
            killed_process = first_runtime._process
            assert killed_process is not None
            killed_process.kill()
            for _ in range(200):
                if first_runtime.health == "lost":
                    break
                await asyncio.sleep(0.01)
            assert first_runtime.health == "lost"

            await asyncio.gather(
                *(
                    first.set_active_agent(
                        "researcher",
                        codex_config_profile="qwen",
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=f"after-kill-{index}",
                        session_id="default",
                    )
                    for index in range(12)
                )
            )
            assert first_runtime.generation == first_generation + 1
            assert isinstance(first_runtime.pid, int) and first_runtime.pid > 0
            assert first_runtime.health == "ready"
            assert _process_budget(first).active == 2
            for index in range(12):
                assert await first.store.get_route(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=f"after-kill-{index}",
                    session_id="default",
                ) == "researcher"
            assert [
                model["id"] for model in await first.list_models(
                    agent_id="researcher"
                )
            ] == ["qwen-model"]
        finally:
            await first.stop()
        assert killed_process is not None and killed_process.exitcode is not None

        second = _manager(database, tmp_path, max_processes=3)
        await second.start()
        second_budget = _process_budget(second)
        try:
            restored = _process_runtime(second, "researcher")
            assert restored.health == "ready"
            assert isinstance(restored.pid, int) and restored.pid > 0
            assert second_budget.active == 2
            profile = await second.store.get_profile("researcher", 1)
            assert profile is not None
            assert profile.enabled
            assert profile.codex_config_profile == "qwen"
            assert await second.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="owner",
                session_id="default",
            ) == "researcher"
            assert [
                model["id"] for model in await second.list_models(
                    agent_id="researcher"
                )
            ] == ["qwen-model"]

            with pytest.raises(
                ValueError,
                match="already bound to a different Codex config profile",
            ):
                await second.set_active_agent(
                    "researcher",
                    codex_config_profile="other",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="wrong-rebind",
                    session_id="default",
                )
            assert restored.health == "ready"
            assert restored.codex_config_profile == "qwen"
            assert await second.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="wrong-rebind",
                session_id="default",
            ) == "codex"
        finally:
            await second.stop()
        assert second_budget.active == 0

    asyncio.run(scenario())


def test_restart_capacity_failure_reaps_partial_children_and_is_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_profile_files(config_home)
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "restart-capacity.sqlite"

        seeded = _manager(database, tmp_path, max_processes=3)
        await seeded.start()
        seeded_budget = _process_budget(seeded)
        try:
            for agent_id in ("alpha", "beta"):
                await seeded.set_active_agent(
                    agent_id,
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=agent_id,
                    session_id="default",
                )
            assert seeded_budget.active == 3
        finally:
            await seeded.stop()
        assert seeded_budget.active == 0

        constrained = _manager(database, tmp_path, max_processes=2)
        constrained_budget = _process_budget(constrained)
        try:
            with pytest.raises(ProcessAgentCapacityError):
                await constrained.start()
            assert constrained_budget.active == 0
        finally:
            await constrained.stop()
        assert constrained_budget.active == 0

        recovered = _manager(database, tmp_path, max_processes=3)
        await recovered.start()
        recovered_budget = _process_budget(recovered)
        try:
            assert recovered_budget.active == 3
            for agent_id in ("alpha", "beta"):
                runtime = _process_runtime(recovered, agent_id)
                profile = await recovered.store.get_profile(agent_id, 1)
                assert runtime.health == "ready"
                assert runtime.codex_config_profile == "qwen"
                assert profile is not None and profile.enabled
                assert profile.codex_config_profile == "qwen"
                assert await recovered.get_active_agent(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=agent_id,
                    session_id="default",
                ) == agent_id
        finally:
            await recovered.stop()
        assert recovered_budget.active == 0

    asyncio.run(scenario())


def test_real_child_delete_switch_recreate_churn_stays_coherent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_profile_files(config_home)
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "churn.sqlite"
        first = _manager(database, tmp_path, max_processes=3)
        await first.start()
        budget = _process_budget(first)
        start_gate = asyncio.Event()
        expected_switch_conflicts: list[str] = []
        rejected_rebindings = 0

        async def switcher(user_id: str) -> None:
            await start_gate.wait()
            for iteration in range(8):
                try:
                    await first.set_active_agent(
                        "racer",
                        codex_config_profile="qwen",
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=user_id,
                        session_id=f"s{iteration % 2}",
                    )
                except RuntimeError as exc:
                    assert str(exc) == "Agent changed while switching: racer"
                    expected_switch_conflicts.append(str(exc))
                await asyncio.sleep(0)

        async def deleter() -> None:
            await start_gate.wait()
            for iteration in range(6):
                try:
                    await first.delete_agent("racer")
                except KeyError:
                    pass
                await asyncio.sleep(0)
                try:
                    await first.set_active_agent(
                        "racer",
                        codex_config_profile="qwen",
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="churn-owner",
                        session_id=f"r{iteration}",
                    )
                except RuntimeError as exc:
                    assert str(exc) == "Agent changed while switching: racer"
                    expected_switch_conflicts.append(str(exc))

        async def wrong_rebinder() -> None:
            nonlocal rejected_rebindings
            await start_gate.wait()
            for iteration in range(8):
                with pytest.raises(
                    ValueError,
                    match="already bound to a different Codex config profile",
                ):
                    await first.set_active_agent(
                        "racer",
                        codex_config_profile="other",
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="wrong-rebinder",
                        session_id=f"w{iteration}",
                    )
                rejected_rebindings += 1
                await asyncio.sleep(0)

        try:
            await first.set_active_agent(
                "racer",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="initial-owner",
                session_id="default",
            )
            operations = [
                asyncio.create_task(switcher(f"user-{index}"))
                for index in range(4)
            ]
            operations.extend(
                (asyncio.create_task(deleter()), asyncio.create_task(wrong_rebinder()))
            )
            start_gate.set()
            await asyncio.wait_for(asyncio.gather(*operations), timeout=30)

            await first.set_active_agent(
                "racer",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="final-owner",
                session_id="default",
            )
            runtime = _process_runtime(first, "racer")
            profile = await first.store.get_profile("racer", 1)
            assert runtime.health == "ready"
            assert runtime.codex_config_profile == "qwen"
            assert profile is not None and profile.enabled
            assert profile.codex_config_profile == "qwen"
            assert not await first.store.is_agent_deleted("racer")
            assert budget.active == 2
            assert rejected_rebindings == 8
            assert await first.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="final-owner",
                session_id="default",
            ) == "racer"

            assert await first.delete_agent("racer")
            assert first.registry.registration("racer") is None
            assert await first.store.is_agent_deleted("racer")
            retired = await first.store.get_profile("racer", 1)
            assert retired is not None and not retired.enabled
            assert retired.codex_config_profile == "qwen"
            assert budget.active == 1
        finally:
            start_gate.set()
            await first.stop()
        assert budget.active == 0

        second = _manager(database, tmp_path, max_processes=3)
        await second.start()
        second_budget = _process_budget(second)
        try:
            assert second.registry.registration("racer") is None
            assert await second.store.is_agent_deleted("racer")
            assert second_budget.active == 1

            await second.set_active_agent(
                "racer",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="post-restart",
                session_id="default",
            )
            restored = _process_runtime(second, "racer")
            assert restored.health == "ready"
            assert restored.codex_config_profile == "qwen"
            assert second_budget.active == 2
            assert not await second.store.is_agent_deleted("racer")
        finally:
            await second.stop()
        assert second_budget.active == 0

    asyncio.run(scenario())
