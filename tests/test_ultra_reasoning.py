"""Model-scoped ``ultra`` reasoning regressions for durable WeChat commands."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


_ULTRA_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "id": "gpt-5.6-sol",
        "displayName": "GPT-5.6 Sol",
        "isDefault": True,
        "defaultReasoningEffort": "low",
        "supportedReasoningEfforts": [
            {"reasoningEffort": effort}
            for effort in ("low", "medium", "high", "xhigh", "max", "ultra")
        ],
    },
    {
        "id": "gpt-5.6-terra",
        "displayName": "GPT-5.6 Terra",
        "isDefault": False,
        "defaultReasoningEffort": "medium",
        "supportedReasoningEfforts": [
            {"reasoningEffort": effort}
            for effort in ("low", "medium", "high", "xhigh", "max", "ultra")
        ],
    },
    {
        "id": "gpt-5.6-luna",
        "displayName": "GPT-5.6 Luna",
        "isDefault": False,
        "defaultReasoningEffort": "medium",
        "supportedReasoningEfforts": [
            {"reasoningEffort": effort}
            for effort in ("low", "medium", "high", "xhigh", "max")
        ],
    },
)


class _ModelRuntime:
    agent_id = "codex"

    def __init__(self, catalog: Iterable[Mapping[str, Any]]) -> None:
        self.catalog = tuple(deepcopy(dict(model)) for model in catalog)
        self.compatibility_calls: list[tuple[str, str, str]] = []
        self.current_model = ""
        self.current_effort = ""
        self.reset_calls: list[tuple[str, str, str]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        assert include_hidden is False
        return [deepcopy(model) for model in self.catalog]

    def set_model(self, conversation_id: str, model_id: str) -> None:
        self.compatibility_calls.append(("model", conversation_id, model_id))
        self.current_model = model_id

    def set_reasoning_effort(
        self, conversation_id: str, reasoning_effort: str
    ) -> None:
        self.compatibility_calls.append(
            ("effort", conversation_id, reasoning_effort)
        )
        self.current_effort = reasoning_effort

    async def reset_session(self, conversation_id: str) -> str:
        self.reset_calls.append(
            (conversation_id, self.current_model, self.current_effort)
        )
        return ""


def _manager_for(
    database: Path,
    catalog: Iterable[Mapping[str, Any]] = _ULTRA_CATALOG,
) -> tuple[TaskManager, _ModelRuntime]:
    runtime = _ModelRuntime(catalog)
    registry = AgentRegistry()
    registry.register("codex", runtime, profile=codex_profile())
    return TaskManager(SQLiteStore(database), registry, worker_count=0), runtime


def _envelope(text: str, *, message_id: str = "ultra-command") -> InboundEnvelope:
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


async def _route(router: MVPCommandRouter, text: str) -> str:
    command = parse_command(text)
    assert command is not None
    result = await router.handle_command(command, _envelope(text))
    assert result is not None
    return str(result)


async def _selection(manager: TaskManager) -> dict[str, str]:
    return await manager.get_model_selection(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        session_id="default",
        agent_id="codex",
    )


def _catalog_line(rendered: str, model_id: str) -> str:
    marker = f"**`{model_id}`**"
    return next(line for line in rendered.splitlines() if marker in line)


def test_help_keeps_effort_choices_model_scoped() -> None:
    async def scenario() -> None:
        manager, _runtime = _manager_for(Path(":memory:"))
        await manager.start()
        try:
            rendered = await _route(MVPCommandRouter(manager), "/help")
            assert (
                rendered.count(
                    "`/model [<model-id> <effort|default>|effort <effort|default>]`"
                )
                == 1
            )
            assert (
                "`/models` - List models and their supported reasoning efforts"
                in rendered
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_models_catalog_renders_ultra_only_for_supported_models(tmp_path) -> None:
    async def scenario() -> None:
        manager, _runtime = _manager_for(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            rendered = await _route(MVPCommandRouter(manager), "/models")

            sol = _catalog_line(rendered, "gpt-5.6-sol")
            terra = _catalog_line(rendered, "gpt-5.6-terra")
            luna = _catalog_line(rendered, "gpt-5.6-luna")
            assert "`ultra`" in sol
            assert "`ultra`" in terra
            assert "`ultra`" not in luna
            assert "`max`" in sol and "`max`" in terra and "`max`" in luna
            assert rendered.count("`ultra`") == 2
            assert rendered.count("**(current)**") == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_model_and_effort_only_commands_canonicalize_supported_ultra(
    tmp_path,
) -> None:
    async def scenario() -> None:
        manager, runtime = _manager_for(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            model_result = await _route(
                router, "/model GPT-5.6-SOL ULTRA"
            )
            assert "- **Model:** `gpt-5.6-sol`" in model_result
            assert "- **Reasoning effort:** `ultra` (override)" in model_result
            assert (await _selection(manager))["reasoning_effort"] == "ultra"

            await _route(router, "/model gpt-5.6-sol high")
            effort_result = await _route(router, "/model effort UlTrA")
            assert "- **Model:** `gpt-5.6-sol`" in effort_result
            assert "- **Reasoning effort:** `ultra` (override)" in effort_result
            assert await _selection(manager) == {
                "agent_id": "codex",
                "model_id": "gpt-5.6-sol",
                "reasoning_effort": "ultra",
                "conversation_id": "wechat:bot:user:default:codex",
            }
            assert runtime.compatibility_calls[-1] == (
                "effort",
                "wechat:bot:user:default:codex",
                "ultra",
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_luna_rejects_ultra_without_mutating_model_or_effort(tmp_path) -> None:
    async def scenario() -> None:
        manager, _runtime = _manager_for(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            await _route(router, "/model gpt-5.6-sol ultra")
            before_model_rejection = await _selection(manager)

            model_rejection = await _route(
                router, "/model gpt-5.6-luna ultra"
            )
            assert model_rejection == (
                "cannot set model: model gpt-5.6-luna does not support "
                "reasoning effort: ultra"
            )
            assert await _selection(manager) == before_model_rejection

            await _route(router, "/model gpt-5.6-luna max")
            before_effort_rejection = await _selection(manager)
            effort_rejection = await _route(router, "/model effort ultra")
            assert effort_rejection == (
                "cannot set model: model gpt-5.6-luna does not support "
                "reasoning effort: ultra"
            )
            assert await _selection(manager) == before_effort_rejection
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_default_effort_forgets_the_sticky_ultra_thread(tmp_path) -> None:
    async def scenario() -> None:
        manager, runtime = _manager_for(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            await _route(router, "/model gpt-5.6-sol ultra")
            accepted = await manager.accept_inbound(
                _envelope("bind ultra", message_id="bind-ultra"),
                create_task=True,
            )
            assert accepted.task is not None
            conversation_id = accepted.task.conversation_id
            assert await manager.store.set_task_thread(
                accepted.task.task_id,
                thread_id="native-ultra-thread",
            )
            assert await manager.store.get_thread_binding(
                conversation_id,
                mode_id=accepted.task.mode_id,
                profile_version=accepted.task.profile_version,
                policy_version=accepted.task.policy_version,
            ) == "native-ultra-thread"

            rendered = await _route(router, "/model effort default")

            assert "- **Reasoning effort:** `model default` (default)" in rendered
            assert (await _selection(manager))["reasoning_effort"] == ""
            assert runtime.reset_calls == [
                (conversation_id, "gpt-5.6-sol", "")
            ]
            assert await manager.store.get_thread_binding(
                conversation_id,
                mode_id=accepted.task.mode_id,
                profile_version=accepted.task.profile_version,
                policy_version=accepted.task.policy_version,
            ) is None
            future = await manager.accept_inbound(
                _envelope("after default", message_id="after-default"),
                create_task=True,
            )
            assert future.task is not None
            assert future.task.thread_id is None
            assert future.task.reasoning_effort == ""
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_incompatible_model_switch_forgets_the_sticky_ultra_thread(
    tmp_path,
) -> None:
    async def scenario() -> None:
        manager, runtime = _manager_for(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            scope = {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "session_id": "default",
                "agent_id": "codex",
            }
            await manager.set_model(
                "gpt-5.6-sol",
                reasoning_effort="ultra",
                **scope,
            )
            accepted = await manager.accept_inbound(
                _envelope("bind ultra", message_id="bind-before-luna"),
                create_task=True,
            )
            assert accepted.task is not None
            assert await manager.store.set_task_thread(
                accepted.task.task_id,
                thread_id="native-ultra-thread",
            )

            selected = await manager.set_model("gpt-5.6-luna", **scope)

            assert selected["model_id"] == "gpt-5.6-luna"
            assert selected["reasoning_effort"] == ""
            assert runtime.reset_calls == [
                (
                    accepted.task.conversation_id,
                    "gpt-5.6-luna",
                    "",
                )
            ]
            assert await manager.store.get_thread_binding(
                accepted.task.conversation_id,
                mode_id=accepted.task.mode_id,
                profile_version=accepted.task.profile_version,
                policy_version=accepted.task.policy_version,
            ) is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ultra_preference_survives_restart_and_only_future_tasks_inherit_it(
    tmp_path,
) -> None:
    database = tmp_path / "runtime.sqlite"

    async def scenario() -> None:
        first, _first_runtime = _manager_for(database)
        await first.start()
        try:
            before = await first.accept_inbound(
                _envelope("before selection", message_id="before-ultra"),
                create_task=True,
            )
            assert before.task is not None
            assert (before.task.model, before.task.reasoning_effort) == ("", "")

            await _route(
                MVPCommandRouter(first), "/model GPT-5.6-SOL ULTRA"
            )
            selected = await first.accept_inbound(
                _envelope("selected", message_id="selected-ultra"),
                create_task=True,
            )
            assert selected.task is not None
            assert (selected.task.model, selected.task.reasoning_effort) == (
                "gpt-5.6-sol",
                "ultra",
            )
            before_task_id = before.task.task_id
            selected_task_id = selected.task.task_id
        finally:
            await first.stop()

        second, _second_runtime = _manager_for(database)
        await second.start()
        try:
            assert await _selection(second) == {
                "agent_id": "codex",
                "model_id": "gpt-5.6-sol",
                "reasoning_effort": "ultra",
                "conversation_id": "wechat:bot:user:default:codex",
            }
            future = await second.accept_inbound(
                _envelope("future", message_id="future-ultra"),
                create_task=True,
            )
            assert future.task is not None
            assert (future.task.model, future.task.reasoning_effort) == (
                "gpt-5.6-sol",
                "ultra",
            )

            persisted_before = await second.get_task(before_task_id)
            persisted_selected = await second.get_task(selected_task_id)
            assert persisted_before is not None
            assert persisted_selected is not None
            assert (
                persisted_before.model,
                persisted_before.reasoning_effort,
            ) == ("", "")
            assert (
                persisted_selected.model,
                persisted_selected.reasoning_effort,
            ) == ("gpt-5.6-sol", "ultra")
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_opaque_runtime_default_requires_every_model_to_support_ultra(
    tmp_path,
) -> None:
    mixed_catalog = (
        {
            "id": "model-a",
            "supportedReasoningEfforts": ["low", "ultra"],
        },
        {
            "id": "model-b",
            "supportedReasoningEfforts": ["low", "max"],
        },
    )
    all_ultra_catalog = (
        {
            "id": "model-a",
            "supportedReasoningEfforts": ["low", "ultra"],
        },
        {
            "id": "model-b",
            "supportedReasoningEfforts": ["medium", "ultra"],
        },
    )

    async def scenario() -> None:
        mixed, _mixed_runtime = _manager_for(
            tmp_path / "mixed.sqlite", mixed_catalog
        )
        await mixed.start()
        try:
            rejected = await _route(
                MVPCommandRouter(mixed), "/model effort ultra"
            )
            assert rejected == (
                "cannot set model: current runtime-default model cannot be "
                "proven to support reasoning effort: ultra"
            )
            assert (await _selection(mixed))["model_id"] == ""
            assert (await _selection(mixed))["reasoning_effort"] == ""
        finally:
            await mixed.stop()

        all_ultra, _all_runtime = _manager_for(
            tmp_path / "all.sqlite", all_ultra_catalog
        )
        await all_ultra.start()
        try:
            accepted = await _route(
                MVPCommandRouter(all_ultra), "/model effort ULTRA"
            )
            assert "- **Model:** `runtime default`" in accepted
            assert "- **Reasoning effort:** `ultra` (override)" in accepted
            assert (await _selection(all_ultra))["model_id"] == ""
            assert (await _selection(all_ultra))["reasoning_effort"] == "ultra"
        finally:
            await all_ultra.stop()

    asyncio.run(scenario())


def test_unreported_efforts_model_accepts_explicit_effort(tmp_path) -> None:
    async def scenario() -> None:
        catalog = (
            {
                "id": "qwen3.8-27b",
                "displayName": "Qwen 3.8 27B",
                "isDefault": True,
                "supportedReasoningEfforts": [],
            },
            {
                "id": "gpt-5.6-luna",
                "displayName": "GPT-5.6 Luna",
                "isDefault": False,
                "defaultReasoningEffort": "medium",
                "supportedReasoningEfforts": [
                    {"reasoningEffort": effort}
                    for effort in ("low", "medium", "high", "xhigh", "max")
                ],
            },
        )
        manager, runtime = _manager_for(tmp_path / "runtime.sqlite", catalog)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            result = await _route(router, "/model qwen3.8-27b xhigh")
            assert "- **Model:** `qwen3.8-27b`" in result
            assert "- **Reasoning effort:** `xhigh` (override)" in result
            assert (await _selection(manager))["reasoning_effort"] == "xhigh"
            assert runtime.compatibility_calls[-1] == (
                "effort",
                "wechat:bot:user:default:codex",
                "xhigh",
            )

            effort_result = await _route(router, "/model effort ultra")
            assert "- **Reasoning effort:** `ultra` (override)" in effort_result
            assert (await _selection(manager))["reasoning_effort"] == "ultra"

            rejection = await _route(router, "/model gpt-5.6-luna ultra")
            assert rejection == (
                "cannot set model: model gpt-5.6-luna does not support "
                "reasoning effort: ultra"
            )
            assert (await _selection(manager))["reasoning_effort"] == "ultra"

            await manager.set_model(
                "gpt-5.6-luna",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
            )
            assert (await _selection(manager))["reasoning_effort"] == ""
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_unreported_efforts_model_keeps_previous_override(tmp_path) -> None:
    async def scenario() -> None:
        catalog = (
            {
                "id": "gpt-5.6-sol",
                "displayName": "GPT-5.6 Sol",
                "isDefault": True,
                "supportedReasoningEfforts": [
                    {"reasoningEffort": effort}
                    for effort in ("low", "medium", "high", "xhigh", "max", "ultra")
                ],
            },
            {
                "id": "qwen3.8-27b",
                "displayName": "Qwen 3.8 27B",
                "isDefault": False,
                "supportedReasoningEfforts": [],
            },
        )
        manager, _runtime = _manager_for(tmp_path / "runtime.sqlite", catalog)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            await _route(router, "/model gpt-5.6-sol ultra")
            await manager.set_model(
                "qwen3.8-27b",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
            )
            assert await _selection(manager) == {
                "agent_id": "codex",
                "model_id": "qwen3.8-27b",
                "reasoning_effort": "ultra",
                "conversation_id": "wechat:bot:user:default:codex",
            }
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_opaque_default_accepts_effort_when_no_entry_reports_efforts(
    tmp_path,
) -> None:
    async def scenario() -> None:
        catalog = (
            {
                "id": "provider-a",
                "displayName": "Provider A",
                "isDefault": False,
                "supportedReasoningEfforts": [],
            },
            {
                "id": "provider-b",
                "displayName": "Provider B",
                "isDefault": False,
                "supportedReasoningEfforts": [],
            },
        )
        manager, _runtime = _manager_for(tmp_path / "runtime.sqlite", catalog)
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            result = await _route(router, "/model effort xhigh")
            assert "- **Reasoning effort:** `xhigh` (override)" in result
            assert (await _selection(manager))["reasoning_effort"] == "xhigh"

            mixed = (
                {
                    "id": "gpt-a",
                    "displayName": "A",
                    "isDefault": False,
                    "supportedReasoningEfforts": ["low", "high"],
                },
                {
                    "id": "provider-b",
                    "displayName": "B",
                    "isDefault": False,
                    "supportedReasoningEfforts": [],
                },
            )
            database = tmp_path / "mixed.sqlite"
            second, _second_runtime = _manager_for(database, mixed)
            await second.start()
            try:
                second_router = MVPCommandRouter(second)
                rejection = await _route(second_router, "/model effort ultra")
                assert rejection == (
                    "cannot set model: current runtime-default model cannot "
                    "be proven to support reasoning effort: ultra"
                )
                await _route(second_router, "/model effort high")
                assert (
                    await second.get_model_selection(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        session_id="default",
                        agent_id="codex",
                    )
                )["reasoning_effort"] == "high"
            finally:
                await second.stop()
        finally:
            await manager.stop()

    asyncio.run(scenario())
