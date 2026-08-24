"""Process-loss recovery for a named Agent's immutable config binding."""

from __future__ import annotations

import asyncio
from pathlib import Path

from src.runtime.manager import TaskManager
from src.runtime.process_agent import ProcessAgentRuntime
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


def test_profile_child_process_loss_restarts_with_the_same_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        config_home.mkdir()
        (config_home / "qwen.config.toml").write_text(
            'model = "qwen3.8-27b"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        template = ProcessAgentRuntime.create(
            "codex",
            cwd=str(tmp_path),
            backend_factory=(
                "tests.test_agent_codex_config_profile:"
                "profile_probe_backend_factory"
            ),
            start_timeout=5,
            stop_timeout=5,
            event_ack_timeout=5,
            max_processes=4,
        )
        registry = AgentRegistry()
        registry.register(
            "codex",
            template,
            profile=codex_profile(default_mode_id="chat"),
        )
        manager = TaskManager(
            SQLiteStore(tmp_path / "runtime.sqlite"),
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
            allow_dynamic_agents=True,
            allowed_codex_config_profiles=("qwen",),
            require_process_isolation=True,
            reconcile_interval=None,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "researcher",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="first-user",
                session_id="default",
            )
            registration = manager.registry.registration("researcher")
            assert registration is not None
            runtime = registration.runtime
            first_pid = runtime.pid
            first_generation = runtime.generation
            assert isinstance(first_pid, int) and first_pid > 0
            process = runtime._delegate._process
            assert process is not None and process.pid == first_pid
            process.kill()

            for _ in range(100):
                if str(getattr(runtime.health, "value", runtime.health)) == "lost":
                    break
                await asyncio.sleep(0.02)
            assert str(getattr(runtime.health, "value", runtime.health)) == "lost"

            await manager.set_active_agent(
                "researcher",
                codex_config_profile="qwen",
                channel="wechat",
                bot_id="bot",
                external_user_id="second-user",
                session_id="default",
            )
            assert runtime.generation > first_generation
            assert isinstance(runtime.pid, int) and runtime.pid > 0
            assert str(getattr(runtime.health, "value", runtime.health)) == "ready"
            assert [
                model["id"]
                for model in await manager.list_models(agent_id="researcher")
            ] == ["qwen3.8-27b"]
            profile = await manager.store.get_profile("researcher", 1)
            assert profile is not None
            assert profile.codex_config_profile == "qwen"
        finally:
            await manager.stop()

    asyncio.run(scenario())
