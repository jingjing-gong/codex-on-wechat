"""Failure-atomicity regression for dynamic Codex config-profile Agents."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from src.agents.base import AgentResult
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


class _FailingChildRuntime:
    """Template whose named children deterministically fail during startup."""

    agent_id = "codex"

    def __init__(self, *, agent_id: str = "codex") -> None:
        self.agent_id = agent_id

    def for_agent(
        self,
        agent_id: str,
        *,
        codex_config_profile: str | None = None,
    ) -> "_FailingChildRuntime":
        del codex_config_profile
        return _FailingChildRuntime(agent_id=agent_id)

    async def start(self) -> None:
        if self.agent_id != "codex":
            raise RuntimeError("synthetic child startup failure")

    async def stop(self) -> None:
        return None

    async def run(self, _task: Any, _emit: Any = None) -> AgentResult:
        return AgentResult(content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False


class _ReadyChildRuntime(_FailingChildRuntime):
    """Template whose named children become ready and record that fact."""

    def __init__(
        self,
        *,
        agent_id: str = "codex",
        starts: list[str] | None = None,
    ) -> None:
        super().__init__(agent_id=agent_id)
        self.starts = starts if starts is not None else []

    def for_agent(
        self,
        agent_id: str,
        *,
        codex_config_profile: str | None = None,
    ) -> "_ReadyChildRuntime":
        del codex_config_profile
        return _ReadyChildRuntime(agent_id=agent_id, starts=self.starts)

    async def start(self) -> None:
        self.starts.append(self.agent_id)


class _RouteCommitFailStore(SQLiteStore):
    async def commit_agent_reactivation(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("synthetic route persistence failure")


class _PostCommitProfileFailStore(SQLiteStore):
    async def put_profile(self, profile: Any) -> Any:
        result = await super().put_profile(profile)
        if getattr(profile, "agent_id", "") == "ready-then-store-error":
            raise RuntimeError("synthetic post-commit profile failure")
        return result


def _manager(
    database: Path,
    *,
    runtime: _FailingChildRuntime | None = None,
    store: SQLiteStore | None = None,
) -> TaskManager:
    registry = AgentRegistry()
    registry.register(
        "codex",
        runtime or _FailingChildRuntime(),
        profile=codex_profile(default_mode_id="chat"),
    )
    return TaskManager(
        store or SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="chat",
        allow_dynamic_agents=True,
        allowed_codex_config_profiles=("qwen",),
        reconcile_interval=None,
    )


def test_failed_profile_child_start_cannot_poison_the_next_restart(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        config_home.mkdir()
        (config_home / "qwen.config.toml").write_text(
            'model = "qwen3.8-27b"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "runtime.sqlite"

        first = _manager(database)
        await first.start()
        try:
            try:
                await first.set_active_agent(
                    "poisoned",
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                )
            except RuntimeError as exc:
                assert str(exc) == "synthetic child startup failure"
            else:  # pragma: no cover - the fake must fail deterministically.
                raise AssertionError("named child unexpectedly started")

            assert first.registry.registration("poisoned") is None
            assert await first.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"
            durable_profile_remained = (
                await first.store.get_profile("poisoned", 1) is not None
            )
        finally:
            await first.stop()

        second = _manager(database)
        restart_error: BaseException | None = None
        try:
            try:
                await second.start()
            except BaseException as exc:
                restart_error = exc
        finally:
            await second.stop()

        assert not durable_profile_remained and restart_error is None, (
            "failed spawn was not atomic: "
            f"durable_profile_remained={durable_profile_remained}, "
            f"restart_error={type(restart_error).__name__}"
        )

    asyncio.run(scenario())


def test_pre_start_ensure_failure_cannot_leave_a_restorable_profile(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        config_home.mkdir()
        (config_home / "qwen.config.toml").write_text(
            'model = "qwen3.8-27b"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "pre-start.sqlite"

        first = _manager(database)
        assert await first.ensure_agent(
            "prestart",
            codex_config_profile="qwen",
        )
        try:
            try:
                await first.start()
            except RuntimeError as exc:
                assert str(exc) == "synthetic child startup failure"
            else:  # pragma: no cover - the fake must fail deterministically.
                raise AssertionError("named child unexpectedly started")
        finally:
            await first.stop()

        inspection = SQLiteStore(database)
        await inspection.initialize()
        try:
            profile = await inspection.get_profile("prestart", 1)
            safely_staged = bool(
                profile is None
                or not profile.enabled
                or await inspection.is_agent_deleted("prestart")
            )
        finally:
            await inspection.close()

        second = _manager(database)
        restart_error: BaseException | None = None
        try:
            try:
                await second.start()
            except BaseException as exc:
                restart_error = exc
        finally:
            await second.stop()

        assert safely_staged and restart_error is None, (
            "pre-start ensure poisoned durable restoration: "
            f"safely_staged={safely_staged}, "
            f"restart_error={type(restart_error).__name__}"
        )

    asyncio.run(scenario())


def test_route_persistence_failure_after_child_ready_rolls_back_new_agent(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        config_home.mkdir()
        (config_home / "qwen.config.toml").write_text(
            'model = "qwen3.8-27b"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "route-failure.sqlite"
        runtime = _ReadyChildRuntime()
        first = _manager(
            database,
            runtime=runtime,
            store=_RouteCommitFailStore(database),
        )
        await first.start()
        try:
            try:
                await first.set_active_agent(
                    "ready-then-failed",
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                )
            except RuntimeError as exc:
                assert str(exc) == "synthetic route persistence failure"
            else:  # pragma: no cover - the fake store must fail.
                raise AssertionError("route persistence unexpectedly succeeded")

            assert "ready-then-failed" in runtime.starts
            assert await first.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"
            profile = await first.store.get_profile("ready-then-failed", 1)
            safely_staged = bool(
                profile is None
                or not profile.enabled
                or await first.store.is_agent_deleted("ready-then-failed")
            )
            registration_remained = (
                first.registry.registration("ready-then-failed") is not None
            )
        finally:
            await first.stop()

        assert not registration_remained and safely_staged, (
            "route commit failure published a live/durable Agent: "
            f"registration_remained={registration_remained}, "
            f"Safely_staged={safely_staged}"
        )

    asyncio.run(scenario())


def test_profile_persistence_error_after_child_ready_cannot_poison_restart(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        config_home.mkdir()
        (config_home / "qwen.config.toml").write_text(
            'model = "qwen3.8-27b"\nmodel_provider = "synthetic"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        database = tmp_path / "post-commit-profile-failure.sqlite"
        runtime = _ReadyChildRuntime()
        first = _manager(
            database,
            runtime=runtime,
            store=_PostCommitProfileFailStore(database),
        )
        await first.start()
        try:
            try:
                await first.set_active_agent(
                    "ready-then-store-error",
                    codex_config_profile="qwen",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                )
            except RuntimeError as exc:
                assert str(exc) == "synthetic post-commit profile failure"
            else:  # pragma: no cover - the fake store must fail.
                raise AssertionError("profile persistence unexpectedly succeeded")

            assert "ready-then-store-error" in runtime.starts
            assert first.registry.registration("ready-then-store-error") is None
            profile = await first.store.get_profile("ready-then-store-error", 1)
            safely_staged = bool(
                profile is None
                or not profile.enabled
                or await first.store.is_agent_deleted("ready-then-store-error")
            )
        finally:
            await first.stop()

        second = _manager(database)
        restart_error: BaseException | None = None
        try:
            try:
                await second.start()
            except BaseException as exc:
                restart_error = exc
        finally:
            await second.stop()

        assert safely_staged and restart_error is None, (
            "ambiguous profile commit poisoned restoration: "
            f"safely_staged={safely_staged}, "
            f"restart_error={type(restart_error).__name__}"
        )

    asyncio.run(scenario())
