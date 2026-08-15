"""Durable startup workspace resolution regressions."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.codex_wechat_bot as bot


def test_default_durable_workspace_uses_codex_default(monkeypatch, tmp_path):
    expected = tmp_path / "codex-workspace"
    monkeypatch.delenv("CODEX_WECHAT_WORKSPACE", raising=False)
    monkeypatch.setattr(bot, "default_workspace", lambda: str(expected))

    assert bot._durable_workspace() == expected
    assert expected.is_dir()


def test_configured_durable_workspace_is_expanded_and_created(
    monkeypatch, tmp_path
):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CODEX_WECHAT_WORKSPACE", "~/shared-workspace")

    expected = (home / "shared-workspace").resolve()
    assert bot._durable_workspace() == expected
    assert expected.is_dir()


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "-inf", "not-a-number"])
def test_mailbox_ttl_environment_rejects_unsafe_values(monkeypatch, value):
    monkeypatch.setenv("CODEX_WECHAT_MAILBOX_TTL", value)

    with pytest.raises(RuntimeError, match="non-negative finite number"):
        bot._nonnegative_environment_number("CODEX_WECHAT_MAILBOX_TTL", 86_400)


def test_mailbox_ttl_environment_preserves_zero(monkeypatch):
    monkeypatch.setenv("CODEX_WECHAT_MAILBOX_TTL", "0")

    assert bot._nonnegative_environment_number(
        "CODEX_WECHAT_MAILBOX_TTL", 86_400
    ) == 0


def test_durable_runner_holds_both_locks_before_owned_startup(
    monkeypatch, tmp_path
):
    database = (tmp_path / "runtime.sqlite").resolve()
    events: list[str] = []

    class Ownership:
        def __init__(self, path, *, channel, bot_id):
            assert Path(path) == database
            assert channel == "wechat"
            assert bot_id == "bot"
            self.channel = channel
            self.bot_id = bot_id
            self.owner_instance_id = "test-owner"
            self.held = False
            events.append("constructed")

        def __enter__(self):
            self.held = True
            events.append("acquired")
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            events.append("released")
            self.held = False

    def run_owned(_client, *, database, ownership):
        assert database == (tmp_path / "runtime.sqlite").resolve()
        assert ownership.held
        events.append("owned-runtime")

    monkeypatch.setenv("CODEX_WECHAT_DB", str(database))
    monkeypatch.setattr(bot, "SupervisorOwnership", Ownership)
    monkeypatch.setattr(bot, "_run_owned_durable", run_owned)

    client = SimpleNamespace(
        bot_id="bot",
        close=lambda: events.append("client-closed"),
    )
    bot._run_durable(client)

    assert events == [
        "constructed",
        "acquired",
        "owned-runtime",
        "client-closed",
        "released",
    ]


def test_runtime_boundary_cleanup_stops_agents_before_bridge_and_store():
    events: list[str] = []
    bridge_error = RuntimeError("bridge stop failed")

    class Bridge:
        async def stop(self):
            events.append("bridge")
            raise bridge_error

    class Manager:
        async def stop(self):
            events.append("manager")

    class Store:
        async def close(self):
            events.append("store")

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="bridge stop failed") as raised:
            await bot._stop_runtime_boundaries(
                agent_bridge=Bridge(),
                manager=Manager(),
                store=Store(),
            )
        assert raised.value is bridge_error

    asyncio.run(scenario())
    assert events == ["manager", "bridge", "store"]


def test_owned_startup_cleanup_failure_requires_ownership_retention(
    monkeypatch, tmp_path: Path
) -> None:
    startup_error = RuntimeError("database initialization failed")
    cleanup_error = RuntimeError("database close failed")

    store_options = {}

    class FailingStore:
        def __init__(self, *_args, **kwargs):
            store_options.update(kwargs)

        async def initialize(self, **_kwargs):
            raise startup_error

        async def close(self):
            raise cleanup_error

    class SynchronousLoop:
        loop = None

        def start(self):
            pass

        def run_coro(self, coroutine, timeout=None):
            del timeout
            return asyncio.run(coroutine)

        def stop(self):
            pass

    monkeypatch.setattr(bot, "SQLiteStore", FailingStore)
    monkeypatch.setattr(bot, "AsyncLoopThread", SynchronousLoop)
    monkeypatch.setenv("CODEX_WECHAT_WORKSPACE", str(tmp_path / "workspace"))
    monkeypatch.setenv("CODEX_WECHAT_ATTACHMENTS", str(tmp_path / "attachments"))
    monkeypatch.setenv("CODEX_WECHAT_AGENT_SOCKET", str(tmp_path / "agent.sock"))
    monkeypatch.setenv("CODEX_WECHAT_MAILBOX_TTL", "12.5")

    ownership = SimpleNamespace(
        held=True,
        owner_instance_id="owner",
        channel="wechat",
        bot_id="bot",
    )
    with pytest.raises(
        bot.SupervisorResourcesStillLive,
        match="startup rollback could not prove cleanup",
    ) as raised:
        bot._run_owned_durable(
            SimpleNamespace(bot_id="bot"),
            database=tmp_path / "runtime.sqlite",
            ownership=ownership,
        )
    assert raised.value.__cause__ is cleanup_error
    assert store_options["mailbox_ttl_seconds"] == 12.5
