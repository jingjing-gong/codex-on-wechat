"""Adversarial command-level checks for Codex config-profile authority.

These tests deliberately use only synthetic profile files.  They exercise the
same ``/agent`` surface as a WeChat user while proving that a syntactically
valid file in ``CODEX_HOME`` is not, by itself, authority to select it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


_QWEN_MODEL = "qwen3.8-27b"
_SENSITIVE_MARKER = "profile-secret-must-not-escape"


def _profile_file(config_home: Path, name: str, model: str) -> None:
    config_home.mkdir(parents=True, exist_ok=True)
    (config_home / f"{name}.config.toml").write_text(
        (
            f'model = "{model}"\n'
            'model_provider = "synthetic"\n'
            f'experimental_bearer_token = "{_SENSITIVE_MARKER}"\n'
            '[model_providers.synthetic]\n'
            'name = "Synthetic"\n'
            'base_url = "http://127.0.0.1:9/v1"\n'
            'wire_api = "responses"\n'
        ),
        encoding="utf-8",
    )


class _ProfileRuntime:
    agent_id = "codex"

    def __init__(
        self,
        *,
        agent_id: str = "codex",
        codex_config_profile: str = "",
        factory_calls: list[tuple[str, str]] | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.codex_config_profile = codex_config_profile
        self.factory_calls = factory_calls if factory_calls is not None else []

    def for_agent(
        self,
        agent_id: str,
        *,
        codex_config_profile: str | None = None,
    ) -> "_ProfileRuntime":
        selected = str(codex_config_profile or "")
        self.factory_calls.append((agent_id, selected))
        return _ProfileRuntime(
            agent_id=agent_id,
            codex_config_profile=selected,
            factory_calls=self.factory_calls,
        )

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, _task: Any, _emit: Any = None) -> AgentResult:
        return AgentResult(content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        del include_hidden
        model = _QWEN_MODEL if self.codex_config_profile == "qwen" else "base-model"
        return [{"id": model, "isDefault": True}]


def _manager(
    database: Path,
    runtime: _ProfileRuntime,
    *,
    allowed: tuple[str, ...] = (),
) -> TaskManager:
    registry = AgentRegistry()
    registry.register(
        "codex",
        runtime,
        profile=codex_profile(default_mode_id="chat"),
    )
    return TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="chat",
        allow_dynamic_agents=True,
        allowed_codex_config_profiles=allowed,
        reconcile_interval=None,
    )


def _envelope(text: str, *, message_id: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=message_id,
        text=text,
        session_id="default",
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
    )


async def _route(router: MVPCommandRouter, text: str, *, message_id: str) -> str:
    command = parse_command(text)
    assert command is not None
    return str(
        await router.handle_command(command, _envelope(text, message_id=message_id))
    )


def test_profile_selection_is_default_deny_before_runtime_or_store_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _profile_file(config_home, "qwen", _QWEN_MODEL)
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        runtime = _ProfileRuntime()
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            response = await _route(
                MVPCommandRouter(manager),
                "/agent researcher qwen",
                message_id="denied-profile",
            )
            assert response.startswith("cannot switch Agent:")
            assert str(config_home) not in response
            assert _SENSITIVE_MARKER not in response
            assert manager.registry.registration("researcher") is None
            assert await manager.store.get_profile("researcher", 1) is None
            assert runtime.factory_calls == []
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_allowlisted_profile_succeeds_but_other_existing_file_is_denied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _profile_file(config_home, "qwen", _QWEN_MODEL)
        _profile_file(config_home, "other", "other-model")
        monkeypatch.setenv("CODEX_HOME", str(config_home))
        runtime = _ProfileRuntime()
        manager = _manager(
            tmp_path / "runtime.sqlite",
            runtime,
            allowed=("qwen",),
        )
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            assert await _route(
                router,
                "/agent researcher qwen",
                message_id="allowed-profile",
            ) == "switched to Agent: researcher"
            models = await _route(router, "/models", message_id="qwen-models")
            assert f"**`{_QWEN_MODEL}`**" in models
            assert _SENSITIVE_MARKER not in models

            denied = await _route(
                router,
                "/agent untrusted other",
                message_id="unlisted-profile",
            )
            assert denied.startswith("cannot switch Agent:")
            assert str(config_home) not in denied
            assert _SENSITIVE_MARKER not in denied
            assert manager.registry.registration("untrusted") is None
            assert await manager.store.get_profile("untrusted", 1) is None
            assert runtime.factory_calls == [("researcher", "qwen")]
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "researcher"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_legacy_one_argument_agent_creation_does_not_require_profile_authority(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _ProfileRuntime()
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            response = await _route(
                MVPCommandRouter(manager),
                "/agent legacy",
                message_id="legacy-agent",
            )
            assert response == "switched to Agent: legacy"
            assert runtime.factory_calls == [("legacy", "")]
            stored = await manager.store.get_profile("legacy", 1)
            assert stored is not None
            assert stored.codex_config_profile == ""
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_restart_rejects_persisted_profile_removed_from_allowlist_without_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        database = tmp_path / "runtime.sqlite"
        _profile_file(config_home, "qwen", _QWEN_MODEL)
        monkeypatch.setenv("CODEX_HOME", str(config_home))

        first_runtime = _ProfileRuntime()
        first = _manager(database, first_runtime, allowed=("qwen",))
        await first.start()
        try:
            assert await _route(
                MVPCommandRouter(first),
                "/agent researcher qwen",
                message_id="persist-profile",
            ) == "switched to Agent: researcher"
        finally:
            await first.stop()

        second_runtime = _ProfileRuntime()
        second = _manager(database, second_runtime)
        await second.start()
        try:
            # The supervisor may keep serving unaffected/default Agents, but
            # it must not revive or route into the persisted child after the
            # administrator removes that selector from the allowlist.
            assert second.registry.registration("researcher") is None
            assert second_runtime.factory_calls == []
            with pytest.raises(PermissionError) as caught:
                await second.get_active_agent(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                )
            public_error = str(caught.value)
            assert str(config_home) not in public_error
            assert _SENSITIVE_MARKER not in public_error
            assert await second.store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "researcher"
        finally:
            await second.stop()

    asyncio.run(scenario())
