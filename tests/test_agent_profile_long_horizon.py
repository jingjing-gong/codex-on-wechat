"""Long-horizon state-machine regression for profile-bound dynamic Agents.

All provider profiles, routes, and durable state in this module are synthetic
and live under ``tmp_path``.  No Codex process or provider connection is used.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

from tests.test_agent_config_profiles import (
    _CatalogRuntime,
    _manager,
    _write_fake_profile,
)


def test_profile_bindings_survive_thousands_of_scoped_transitions(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        database = tmp_path / "runtime.sqlite"
        profile_models = {
            "qwen": "synthetic-qwen-model",
            "alternate": "synthetic-alternate-model",
            "local": "synthetic-local-model",
        }
        for profile_name, model_id in profile_models.items():
            _write_fake_profile(config_home, profile_name, model_id)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))

        agent_ids = tuple(f"agent-{ordinal:03d}" for ordinal in range(120))
        bindings = {
            agent_id: tuple(profile_models)[ordinal % len(profile_models)]
            for ordinal, agent_id in enumerate(agent_ids)
        }
        scopes = tuple(
            (f"user-{ordinal // 2:02d}", f"session-{ordinal % 2}")
            for ordinal in range(40)
        )
        expected_routes = {scope: "codex" for scope in scopes}
        expected_models: dict[tuple[str, str, str], tuple[str, str]] = {}

        manager = _manager(
            database,
            _CatalogRuntime(config_home),
        )
        await manager.start()
        try:
            # Publish every immutable binding before the transition stream.
            for ordinal, agent_id in enumerate(agent_ids):
                user, session = scopes[ordinal % len(scopes)]
                await manager.set_active_agent(
                    agent_id,
                    codex_config_profile=bindings[agent_id],
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id=session,
                )
                expected_routes[(user, session)] = agent_id

            rejected_rebindings = 0
            deletion_recreations = 0
            model_changes = 0
            supervisor_restarts = 0

            for ordinal in range(4_096):
                if ordinal and ordinal % 800 == 0:
                    await manager.stop()
                    manager = _manager(
                        database,
                        _CatalogRuntime(config_home),
                    )
                    await manager.start()
                    supervisor_restarts += 1

                    # A fresh supervisor must restore both bindings and routes
                    # before it processes the next simulated user action.
                    for agent_id in agent_ids:
                        stored = await manager.store.get_profile(agent_id, 1)
                        assert stored is not None
                        assert stored.enabled
                        assert stored.codex_config_profile == bindings[agent_id]
                        assert manager.registry.registration(agent_id) is not None
                    for (user, session), expected_agent in expected_routes.items():
                        assert await manager.get_active_agent(
                            channel="wechat",
                            bot_id="bot",
                            external_user_id=user,
                            session_id=session,
                        ) == expected_agent

                scope = scopes[(ordinal * 17 + 3) % len(scopes)]
                user, session = scope
                agent_id = agent_ids[(ordinal * 37 + 11) % len(agent_ids)]
                await manager.set_active_agent(
                    agent_id,
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id=session,
                )
                expected_routes[scope] = agent_id

                if ordinal % 53 == 0:
                    wrong_profile = next(
                        name
                        for name in profile_models
                        if name != bindings[agent_id]
                    )
                    with pytest.raises(
                        ValueError,
                        match="different Codex config profile",
                    ):
                        await manager.set_active_agent(
                            agent_id,
                            codex_config_profile=wrong_profile,
                            channel="wechat",
                            bot_id="bot",
                            external_user_id=user,
                            session_id=session,
                        )
                    assert await manager.get_active_agent(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=user,
                        session_id=session,
                    ) == expected_routes[scope]
                    rejected_rebindings += 1

                if ordinal % 131 == 0:
                    selected_model = profile_models[bindings[agent_id]]
                    await manager.set_model(
                        selected_model,
                        reasoning_effort="high",
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=user,
                        session_id=session,
                        agent_id=agent_id,
                    )
                    expected_models[(user, session, agent_id)] = (
                        selected_model,
                        "high",
                    )
                    model_changes += 1

                if ordinal and ordinal % 157 == 0:
                    victim = agent_ids[(ordinal * 19 + 7) % len(agent_ids)]
                    await manager.delete_agent(victim)
                    for routed_scope, routed_agent in tuple(expected_routes.items()):
                        if routed_agent == victim:
                            expected_routes[routed_scope] = "codex"
                    expected_models = {
                        key: value
                        for key, value in expected_models.items()
                        if key[2] != victim
                    }

                    recreate_scope = scopes[(ordinal * 23 + 5) % len(scopes)]
                    recreate_user, recreate_session = recreate_scope
                    await manager.set_active_agent(
                        victim,
                        channel="wechat",
                        bot_id="bot",
                        external_user_id=recreate_user,
                        session_id=recreate_session,
                    )
                    expected_routes[recreate_scope] = victim
                    restored = await manager.store.get_profile(victim, 1)
                    assert restored is not None
                    assert restored.enabled
                    assert restored.codex_config_profile == bindings[victim]
                    deletion_recreations += 1

            assert rejected_rebindings == 78
            assert deletion_recreations == 26
            assert model_changes == 32
            assert supervisor_restarts == 5

            for agent_id in agent_ids:
                stored = await manager.store.get_profile(agent_id, 1)
                assert stored is not None
                assert stored.enabled
                assert stored.codex_config_profile == bindings[agent_id]
                assert manager.registry.registration(agent_id) is not None
                assert not await manager.store.is_agent_deleted(agent_id)

            for (user, session), expected_agent in expected_routes.items():
                assert await manager.get_active_agent(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id=session,
                ) == expected_agent

            for (user, session, agent_id), expected in expected_models.items():
                selection = await manager.get_model_selection(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id=user,
                    session_id=session,
                    agent_id=agent_id,
                )
                assert (
                    selection["model_id"],
                    selection["reasoning_effort"],
                ) == expected
        finally:
            await manager.stop()

    asyncio.run(scenario())
