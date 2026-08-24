"""User-facing regressions for named Agents bound to Codex config profiles.

The catalog runtime in this module is deliberately hermetic.  It reads the
``model`` field from temporary ``*.config.toml`` files, but it never starts a
Codex process, reads a real user profile, or contacts the configured provider.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

try:
    import tomllib
except ImportError:  # pragma: no cover - exercised on the Python 3.10 CI lane
    import tomli as tomllib

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter, WeChatGateway
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


_QWEN_MODEL = "qwen3.8-27b"


def _envelope(
    text: str,
    *,
    user: str = "user",
    message_id: str = "message-1",
) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id=user,
        external_message_id=message_id,
        text=text,
        agent_id="codex",
        conversation_id=f"wechat:bot:{user}:default:codex",
    )


def _message(text: str, *, sequence: int, user: str = "user") -> WeixinMessage:
    return WeixinMessage(
        seq=sequence,
        message_id=sequence,
        from_user_id=user,
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token=f"context-{sequence}",
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


def _write_fake_profile(config_home: Path, name: str, model: str) -> None:
    config_home.mkdir(parents=True, exist_ok=True)
    (config_home / f"{name}.config.toml").write_text(
        (
            f'model = "{model}"\n'
            'model_provider = "hermetic-test"\n'
            "\n"
            "[model_providers.hermetic-test]\n"
            'name = "Hermetic test provider"\n'
            'base_url = "http://127.0.0.1:9/v1"\n'
            'wire_api = "responses"\n'
            'env_key = "HERMETIC_TEST_API_KEY"\n'
        ),
        encoding="utf-8",
    )


class _CatalogRuntime:
    """Small runtime factory whose model catalog is selected by profile."""

    agent_id = "codex"

    def __init__(
        self,
        config_home: Path,
        *,
        agent_id: str = "codex",
        codex_config_profile: str = "",
        factory_calls: list[tuple[str, str]] | None = None,
    ) -> None:
        self.config_home = config_home
        self.agent_id = agent_id
        self.codex_config_profile = str(codex_config_profile or "")
        self.factory_calls = factory_calls if factory_calls is not None else []
        self.started = 0
        self.stopped = 0
        self._models = self._load_models()

    def _load_models(self) -> list[dict[str, Any]]:
        if not self.codex_config_profile:
            return [
                {
                    "id": "default-test-model",
                    "displayName": "Default test model",
                    "isDefault": True,
                    "supportedReasoningEfforts": ["medium"],
                    "defaultReasoningEffort": "medium",
                }
            ]
        profile_path = self.config_home / (
            f"{self.codex_config_profile}.config.toml"
        )
        with profile_path.open("rb") as stream:
            config = tomllib.load(stream)
        model = str(config.get("model") or "").strip()
        if not model:
            raise ValueError("hermetic profile has no model")
        return [
            {
                "id": model,
                "displayName": f"{self.codex_config_profile} test model",
                "isDefault": True,
                "supportedReasoningEfforts": ["low", "medium", "high"],
                "defaultReasoningEffort": "medium",
            }
        ]

    def for_agent(
        self,
        agent_id: str,
        codex_config_profile: str | None = None,
    ) -> "_CatalogRuntime":
        selected = str(codex_config_profile or "")
        self.factory_calls.append((agent_id, selected))
        return _CatalogRuntime(
            self.config_home,
            agent_id=agent_id,
            codex_config_profile=selected,
            factory_calls=self.factory_calls,
        )

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def run(self, _task: Any, _emit: Any) -> AgentResult:
        return AgentResult(content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        del include_hidden
        return [dict(model) for model in self._models]


def _manager(database: Path, runtime: _CatalogRuntime) -> TaskManager:
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
        # Every profile used below is synthetic and explicitly trusted by the
        # test administrator.  Production defaults to denying non-empty
        # selectors unless they appear in its configured allowlist.
        allowed_codex_config_profiles=("qwen", "alternate", "local"),
        reconcile_interval=None,
    )


async def _route(
    router: MVPCommandRouter,
    text: str,
    *,
    user: str = "user",
    message_id: str = "message-1",
) -> str:
    command = parse_command(text)
    assert command is not None
    result = await router.handle_command(
        command,
        _envelope(text, user=user, message_id=message_id),
    )
    return str(result)


def test_agent_profile_argument_is_forwarded_and_one_argument_stays_compatible():
    class Manager:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def get_active_agent(self, **_scope: Any) -> str:
            return "codex"

        async def set_active_agent(
            self,
            agent_id: str,
            *,
            codex_config_profile: str | None = None,
            **_scope: Any,
        ) -> str:
            self.calls.append((agent_id, codex_config_profile))
            return agent_id

    async def scenario() -> None:
        manager = Manager()
        router = MVPCommandRouter(manager)

        assert await _route(router, "/agent qwen-agent qwen") == (
            "switched to Agent: qwen-agent"
        )
        assert await _route(router, "/agent legacy-agent") == (
            "switched to Agent: legacy-agent"
        )
        assert await _route(router, "/agent too many arguments") == (
            "usage: /agent [agent-id] [profile]"
        )
        assert manager.calls == [
            ("qwen-agent", "qwen"),
            ("legacy-agent", None),
        ]

    asyncio.run(scenario())


def test_gateway_switches_to_profile_agent_and_models_uses_its_fake_catalog(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        runtime = _CatalogRuntime(config_home)
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            gateway = WeChatGateway(
                manager,
                bot_id="bot",
                command_router=MVPCommandRouter(manager),
            )

            switched = await gateway.accept(
                _message("/agent qwen-agent qwen", sequence=1)
            )
            assert switched is not None
            assert switched.command_response == "switched to Agent: qwen-agent"
            assert switched.response_agent_id == "qwen-agent"

            models = await gateway.accept(_message("/models", sequence=2))
            assert models is not None
            assert models.response_agent_id == "qwen-agent"
            assert "**Current Agent:** `qwen-agent`" in models.command_response
            assert f"**`{_QWEN_MODEL}`**" in models.command_response
            assert runtime.factory_calls == [("qwen-agent", "qwen")]

            stored = await manager.store.get_profile("qwen-agent", 1)
            assert stored is not None
            assert stored.codex_config_profile == "qwen"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_profile_validation_rejects_traversal_and_missing_files_without_creation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        runtime = _CatalogRuntime(config_home)
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            cases = (
                ("escape-agent", "../qwen"),
                ("suffix-agent", "qwen.config.toml"),
                ("missing-agent", "not-installed"),
            )
            for ordinal, (agent_id, config_profile) in enumerate(cases, start=1):
                response = await _route(
                    router,
                    f"/agent {agent_id} {config_profile}",
                    message_id=f"validation-{ordinal}",
                )
                assert response.startswith("cannot switch Agent:")
                assert os.fspath(config_home) not in response
                assert manager.registry.registration(agent_id) is None
                assert await manager.store.get_profile(agent_id, 1) is None
            assert runtime.factory_calls == []
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_same_profile_is_idempotent_and_a_different_binding_cannot_replace_it(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        _write_fake_profile(config_home, "alternate", "alternate-model")
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        runtime = _CatalogRuntime(config_home)
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            assert await _route(router, "/agent analyst qwen") == (
                "switched to Agent: analyst"
            )
            assert await _route(
                router, "/agent analyst qwen", message_id="same-binding"
            ) == "switched to Agent: analyst"
            assert runtime.factory_calls == [("analyst", "qwen")]

            # Move the user's front route away first.  A rejected rebinding
            # must not make the old Agent current as a partial side effect.
            assert await _route(
                router, "/agent scratch", message_id="switch-away"
            ) == "switched to Agent: scratch"
            conflict = await _route(
                router,
                "/agent analyst alternate",
                message_id="conflicting-binding",
            )
            assert conflict.startswith("cannot switch Agent:")
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "scratch"
            stored = await manager.store.get_profile("analyst", 1)
            assert stored is not None
            assert stored.codex_config_profile == "qwen"
            assert runtime.factory_calls.count(("analyst", "qwen")) == 1
            assert ("analyst", "alternate") not in runtime.factory_calls
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_profile_binding_survives_restart_delete_and_implicit_recreation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        database = tmp_path / "runtime.sqlite"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))

        first_runtime = _CatalogRuntime(config_home)
        first = _manager(database, first_runtime)
        await first.start()
        try:
            response = await _route(
                MVPCommandRouter(first), "/agent researcher qwen"
            )
            assert response == "switched to Agent: researcher"
            assert first_runtime.factory_calls == [("researcher", "qwen")]
        finally:
            await first.stop()

        second_runtime = _CatalogRuntime(config_home)
        second = _manager(database, second_runtime)
        await second.start()
        try:
            # Startup restoration must construct the child with its durable
            # binding before any user command reaches it.
            assert second_runtime.factory_calls == [("researcher", "qwen")]
            models = await _route(
                MVPCommandRouter(second), "/models", message_id="after-restart"
            )
            assert f"**`{_QWEN_MODEL}`**" in models
            deleted = await _route(
                MVPCommandRouter(second),
                "/delagent researcher",
                message_id="delete",
            )
            assert deleted == "Agent deleted: researcher"
        finally:
            await second.stop()

        third_runtime = _CatalogRuntime(config_home)
        third = _manager(database, third_runtime)
        await third.start()
        try:
            assert third.registry.registration("researcher") is None
            # Omitting the profile during an explicit recreation means
            # preserve the prior immutable binding, not silently use default.
            recreated = await _route(
                MVPCommandRouter(third),
                "/agent researcher",
                message_id="recreate",
            )
            assert recreated == "switched to Agent: researcher"
            assert third_runtime.factory_calls == [("researcher", "qwen")]
            stored = await third.store.get_profile("researcher", 1)
            assert stored is not None
            assert stored.enabled
            assert stored.codex_config_profile == "qwen"
            models = await _route(
                MVPCommandRouter(third),
                "/models",
                message_id="after-recreate",
            )
            assert f"**`{_QWEN_MODEL}`**" in models
        finally:
            await third.stop()

    asyncio.run(scenario())


def test_two_agents_keep_isolated_profile_catalogs(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        _write_fake_profile(config_home, "local", "local-test-model")
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        runtime = _CatalogRuntime(config_home)
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            assert await _route(
                router, "/agent qwen-agent qwen", user="qwen-user"
            ) == "switched to Agent: qwen-agent"
            assert await _route(
                router,
                "/agent local-agent local",
                user="local-user",
                message_id="local-switch",
            ) == "switched to Agent: local-agent"

            qwen_models = await _route(
                router,
                "/models",
                user="qwen-user",
                message_id="qwen-models",
            )
            local_models = await _route(
                router,
                "/models",
                user="local-user",
                message_id="local-models",
            )
            assert f"**`{_QWEN_MODEL}`**" in qwen_models
            assert "local-test-model" not in qwen_models
            assert "**`local-test-model`**" in local_models
            assert _QWEN_MODEL not in local_models
            assert sorted(runtime.factory_calls) == [
                ("local-agent", "local"),
                ("qwen-agent", "qwen"),
            ]
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_concurrent_conflicting_profile_creations_publish_exactly_one_binding(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_fake_profile(config_home, "qwen", _QWEN_MODEL)
        _write_fake_profile(config_home, "alternate", "alternate-model")
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        runtime = _CatalogRuntime(config_home)
        manager = _manager(tmp_path / "runtime.sqlite", runtime)
        await manager.start()
        try:
            async def select(user: str, profile: str) -> Any:
                return await manager.set_active_agent(
                    "shared-agent",
                    codex_config_profile=profile,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id="default",
                )

            results = await asyncio.gather(
                select("qwen-user", "qwen"),
                select("alternate-user", "alternate"),
                return_exceptions=True,
            )
            failures = [result for result in results if isinstance(result, Exception)]
            assert len(failures) == 1
            assert isinstance(failures[0], ValueError)
            assert "different Codex config profile" in str(failures[0])
            assert len(runtime.factory_calls) == 1
            winning_profile = runtime.factory_calls[0][1]
            assert winning_profile in {"qwen", "alternate"}

            stored = await manager.store.get_profile("shared-agent", 1)
            assert stored is not None
            assert stored.codex_config_profile == winning_profile
            routes = {
                user: await manager.get_active_agent(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id="default",
                )
                for user in ("qwen-user", "alternate-user")
            }
            winner = (
                "qwen-user" if winning_profile == "qwen" else "alternate-user"
            )
            loser = (
                "alternate-user" if winner == "qwen-user" else "qwen-user"
            )
            assert routes[winner] == "shared-agent"
            assert routes[loser] == "codex"
        finally:
            await manager.stop()

    asyncio.run(scenario())
