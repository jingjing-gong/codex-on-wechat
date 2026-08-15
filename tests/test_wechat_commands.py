"""Compatibility and formatting tests for the durable WeChat commands."""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest
from openai_codex import CodexRpcError, TransportClosedError
from pydantic import TypeAdapter

from src.agents.base import AgentResult, AgentTask, ReplyTarget
from src.channels.models import ChannelCommand, InboundEnvelope, parse_command
from src.channels.wechat import (
    COMMAND_HELP,
    COMMAND_REGISTRY,
    MVPCommandRouter,
    MVP_COMMANDS,
    MVP_COMMAND_NAMES,
    _MAX_COMMAND_MARKDOWN,
    command_delivery_id,
)
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


def _envelope(
    text: str,
    *,
    message_id: str = "message-1",
    session_id: str = "default",
) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=message_id,
        text=text,
        session_id=session_id,
        agent_id="codex",
        conversation_id=f"wechat:bot:user:{session_id}:codex",
    )


def test_help_is_deterministic_markdown_with_one_command_per_line():
    assert COMMAND_HELP.startswith("## Commands\n")
    assert [group.title for group in COMMAND_REGISTRY] == [
        "Conversation",
        "Tasks",
        "Agents",
        "Agent Configuration",
        "Delivery",
    ]
    public_entries = [
        entry for group in COMMAND_REGISTRY for entry in group.entries
    ]
    assert len({entry.syntax for entry in public_entries}) == len(public_entries)
    for entry in public_entries:
        assert COMMAND_HELP.count(f"- `{entry.syntax}` - ") == 1

    expected_names = {
        "agent",
        "agents",
        "ask",
        "cancel",
        "clear",
        "delagent",
        "help",
        "inbox",
        "listskill",
        "listskills",
        "mode",
        "model",
        "models",
        "modes",
        "notify",
        "recv",
        "reset",
        "retry",
        "sh",
        "skills",
        "status",
        "system",
        "tasks",
    }
    assert MVP_COMMANDS == expected_names
    assert MVP_COMMAND_NAMES == {f"/{name}" for name in expected_names}
    assert "`/listskill`" not in COMMAND_HELP
    assert "`/listskills`" not in COMMAND_HELP
    assert COMMAND_HELP.count("`$<skill> <task description>`") == 1
    assert "`/model [<model-id> <effort|default>|effort <effort|default>]`" in COMMAND_HELP
    assert "`/retry <task-id>`" in COMMAND_HELP
    assert "`/cancel [task-id]`" in COMMAND_HELP
    assert "`/agent [agent-id]`" in COMMAND_HELP
    assert "`/delagent <agent-id>`" in COMMAND_HELP
    assert "`/ask <agent-id> <prompt>`" in COMMAND_HELP
    assert "`/inbox [agent-id|all]`" in COMMAND_HELP
    assert "`/execute`" not in COMMAND_HELP
    assert "`/interrupt" not in COMMAND_HELP
    assert sum(1 for line in COMMAND_HELP.splitlines() if line.startswith("- `/model ")) == 1
    assert "`/recv`" in COMMAND_HELP
    assert not COMMAND_HELP.endswith("\n\n")


def test_system_command_rejects_missing_or_mismatched_raw_compatibility_input():
    class Manager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

        async def get_system_role(self, **_kwargs):
            raise AssertionError("malformed commands must not read role state")

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        for command in (
            ChannelCommand(name="system", args=("planner",), raw=""),
            ChannelCommand(name="system", args=(), raw="/mode"),
        ):
            assert await router.handle_command(
                command, _envelope(command.raw or "/system planner")
            ) == "invalid system command"

    asyncio.run(scenario())


def test_system_command_bounds_and_sanitizes_manager_failures():
    unsafe_detail = ("private\n`value` " * 100) + "tail"

    class ReadManager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

        async def get_system_role(self, **_kwargs):
            raise RuntimeError(unsafe_detail)

    class SetManager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

        async def set_system_role(self, _role_text, **_kwargs):
            raise RuntimeError(unsafe_detail)

    async def scenario() -> None:
        read = await MVPCommandRouter(ReadManager()).handle_command(
            parse_command("/system"), _envelope("/system")
        )
        write = await MVPCommandRouter(SetManager()).handle_command(
            parse_command("/system planner"), _envelope("/system planner")
        )
        assert read.startswith("cannot get system role: private 'value'")
        assert write.startswith("cannot set system role: private 'value'")
        for response in (read, write):
            assert "\n" not in response
            assert "`" not in response
            assert response.endswith("...")
            assert len(response) < 550

    asyncio.run(scenario())


def test_recv_delegates_one_idempotent_batch_without_extra_acknowledgement():
    calls = []

    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def drain_deferred_replies(self, **kwargs):
            calls.append(kwargs)
            return {"outbox_items": [{"outbox_id": "continued-1"}]}

    async def scenario() -> None:
        envelope = _envelope("/recv", message_id="recv-message")
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/recv"), envelope
        )

        assert response == ""
        assert calls == [
            {
                "target": envelope.reply_target.to_dict(),
                "source_key": command_delivery_id(envelope),
                "limit": 10,
            }
        ]

    asyncio.run(scenario())


def test_recv_has_deterministic_empty_and_usage_responses():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def drain_deferred_replies(self, **_kwargs):
            return {"outbox_items": []}

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        assert await router.handle_command(
            parse_command("/recv"), _envelope("/recv")
        ) == "no deferred replies"
        assert await router.handle_command(
            parse_command("/recv now"), _envelope("/recv now")
        ) == "usage: /recv"

    asyncio.run(scenario())


def test_delagent_command_validates_arity_and_delegates():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def delete_agent(self, agent_id: str, **_kwargs) -> bool:
            assert agent_id == "planner"
            return True

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        envelope = _envelope("/delagent planner")
        assert await router.handle_command(
            parse_command("/delagent planner"), envelope
        ) == "Agent deleted: planner"
        assert await router.handle_command(
            parse_command("/delagent"), _envelope("/delagent")
        ) == "usage: /delagent <agent-id>"
        assert await router.handle_command(
            parse_command("/delagent a b"), _envelope("/delagent a b")
        ) == "usage: /delagent <agent-id>"

    asyncio.run(scenario())


def test_durable_router_keeps_bounded_shell_command():
    calls: list[tuple[str, Path | None]] = []

    def runner(command: str, *, cwd: Path | None = None) -> str:
        calls.append((command, cwd))
        return "exit code: 0\nhello"

    async def scenario() -> None:
        router = MVPCommandRouter(object(), shell_cwd=Path("/workspace"), shell_runner=runner)
        result = await router.handle_command(parse_command("/sh printf hello"), _envelope("/sh printf hello"))
        assert result == (
            "## Shell Result\n\n"
            "- **Exit code:** `0`\n\n"
            "### Command\n\n"
            "```sh\nprintf hello\n```\n\n"
            "### Output\n\n"
            "```text\nhello\n```"
        )

        usage = await router.handle_command(parse_command("/sh"), _envelope("/sh"))
        assert usage == "usage: /sh <command>"

    asyncio.run(scenario())
    assert calls == [("printf hello", Path("/workspace"))]


def test_durable_router_reports_shell_timeout_without_retrying():
    calls = 0

    def runner(_command: str, *, cwd: Path | None = None) -> str:
        nonlocal calls
        calls += 1
        raise subprocess.TimeoutExpired("/bin/sh", 30)

    async def scenario() -> None:
        router = MVPCommandRouter(object(), shell_runner=runner)
        result = await router.handle_command(parse_command("/sh sleep 31"), _envelope("/sh sleep 31"))
        assert result == (
            "## Shell Error\n\n"
            "```text\nshell command timed out after 30 seconds\n```"
        )

    asyncio.run(scenario())
    assert calls == 1


def test_agents_are_markdown_and_mark_the_current_agent():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "planner"

        async def list_agents(self):
            return [
                {
                    "agent_id": "codex",
                    "display_name": "Codex",
                    "summary": "Engineering Agent",
                },
                {
                    "agent_id": "planner",
                    "display_name": "Planner",
                    "summary": "Plans the work",
                },
            ]

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/agents"), _envelope("/agents")
        )
        assert result == (
            "## Agents\n\n"
            "- **`codex`** - Codex: Engineering Agent\n"
            "- **`planner`** **(current)** - Planner: Plans the work"
        )

    asyncio.run(scenario())


def test_agents_keep_missing_active_agent_visible_as_current():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "retired-agent"

        async def list_agents(self):
            return [{"agent_id": "codex", "display_name": "Codex"}]

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/agents"), _envelope("/agents")
        )

        assert result.count("**(current)**") == 1
        assert "- **`codex`** - Codex" in result
        assert (
            "- **`retired-agent`** **(current)** **(unavailable)**" in result
        )

    asyncio.run(scenario())


def test_agent_switch_does_not_replay_inbox_history():
    switched: list[tuple[str, dict[str, object]]] = []
    inbox_calls: list[dict[str, object]] = []

    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def set_active_agent(self, agent_id: str, **kwargs) -> str:
            switched.append((agent_id, kwargs))
            return agent_id

        async def inbox(self, **kwargs):
            inbox_calls.append(kwargs)
            return [
                {
                    "outbox_id": "historical-reply",
                    "content": "old Agent history",
                }
            ]

    async def scenario() -> None:
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/agent planner"), _envelope("/agent planner")
        )

        assert response == "switched to Agent: planner"
        assert getattr(response, "presentation_ids", ()) == ()

    asyncio.run(scenario())
    assert switched and switched[0][0] == "planner"
    assert inbox_calls == []


def test_modes_are_markdown_and_mark_the_current_mode():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def get_mode(self, **_kwargs) -> str:
            return "review"

        async def list_modes(self, **_kwargs):
            return [
                {"mode_id": "chat", "sandbox_policy": "read-only"},
                {
                    "mode_id": "review",
                    "sandbox_policy": "workspace-write",
                    "can_write_files": True,
                    "can_execute_commands": True,
                },
            ]

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/modes"), _envelope("/modes")
        )
        assert result == (
            "## Modes\n\n"
            "- **`chat`** - sandbox `read-only`\n"
            "- **`review`** **(current)** - sandbox `workspace-write`, "
            "file writes, commands"
        )

    asyncio.run(scenario())


def test_modes_keep_stale_durable_selection_visible_as_current():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def get_mode(self, **_kwargs) -> str:
            return "retired-mode"

        async def list_modes(self, **_kwargs):
            return [{"mode_id": "chat", "sandbox_policy": "read-only"}]

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/modes"), _envelope("/modes")
        )

        assert result.count("**(current)**") == 1
        assert "- **`chat`** - sandbox `read-only`" in result
        assert (
            "- **`retired-mode`** **(current)** **(unavailable)**" in result
        )

    asyncio.run(scenario())


_MODEL_CATALOG = (
    {
        "id": "gpt-fast",
        "displayName": "Fast",
        "isDefault": True,
        "defaultReasoningEffort": "medium",
        "supportedReasoningEfforts": (
            {"reasoningEffort": "low"},
            {"reasoningEffort": "medium"},
        ),
    },
    {
        "id": "gpt-deep",
        "displayName": "Deep",
        "isDefault": False,
        "defaultReasoningEffort": "high",
        "supportedReasoningEfforts": (
            {"reasoningEffort": "medium"},
            {"reasoningEffort": "high"},
            {"reasoningEffort": "xhigh"},
        ),
    },
)


class _ModelCommandManager:
    def __init__(self) -> None:
        self.model_id = "gpt-deep"
        self.effort = "xhigh"
        self.calls: list[tuple[str, ...]] = []

    async def get_active_agent(self, **_kwargs) -> str:
        return "planner"

    async def list_models(
        self, *, agent_id: str, include_hidden: bool = False
    ) -> list[dict[str, object]]:
        self.calls.append(("list_models", agent_id, str(include_hidden)))
        return [dict(model) for model in _MODEL_CATALOG]

    async def get_model_selection(self, **_kwargs) -> dict[str, str]:
        self.calls.append(("get_model_selection",))
        return {"model_id": self.model_id, "reasoning_effort": self.effort}

    async def set_model(
        self,
        model_id: str,
        *,
        reasoning_effort: str,
        agent_id: str,
        conversation_id: str,
    ) -> dict[str, str]:
        self.calls.append(
            (
                "set_model",
                model_id,
                reasoning_effort,
                agent_id,
                conversation_id,
            )
        )
        self.model_id = model_id
        self.effort = "" if reasoning_effort.lower() == "default" else reasoning_effort
        return {"model_id": self.model_id, "reasoning_effort": self.effort}

    async def set_reasoning_effort(
        self,
        reasoning_effort: str,
        *,
        agent_id: str,
        conversation_id: str,
    ) -> dict[str, str]:
        self.calls.append(
            (
                "set_reasoning_effort",
                reasoning_effort,
                agent_id,
                conversation_id,
            )
        )
        self.effort = "" if reasoning_effort.lower() == "default" else reasoning_effort
        return {"model_id": self.model_id, "reasoning_effort": self.effort}


def test_models_markdown_lists_each_effort_and_effective_current_selection():
    async def scenario() -> None:
        manager = _ModelCommandManager()
        result = await MVPCommandRouter(manager).handle_command(
            parse_command("/models"), _envelope("/models")
        )
        assert result == (
            "## Models\n\n"
            "**Current Agent:** `planner`\n\n"
            "- **`gpt-fast`** **(default)** - Fast. Efforts: `low`, `medium`; "
            "default effort: `medium`.\n"
            "- **`gpt-deep`** **(current)** - Deep. Efforts: `medium`, `high`, "
            "`xhigh`; default effort: `high`; current effort: `xhigh` (override)."
        )
        assert manager.calls == [
            ("list_models", "planner", "False"),
            ("get_model_selection",),
        ]

    asyncio.run(scenario())


def test_models_markdown_keeps_current_marker_distinct_for_runtime_default():
    class Manager(_ModelCommandManager):
        def __init__(self) -> None:
            super().__init__()
            self.model_id = ""
            self.effort = ""

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/models"), _envelope("/models")
        )

        assert result.count("**(current)**") == 1
        assert "- **`gpt-fast`** **(current)** **(default)** - Fast." in result
        assert "**(current, default)**" not in result

    asyncio.run(scenario())


def test_models_marks_opaque_runtime_default_when_catalog_has_no_default():
    class Manager(_ModelCommandManager):
        def __init__(self) -> None:
            super().__init__()
            self.model_id = ""
            self.effort = ""

        async def list_models(self, **_kwargs):
            return [
                {
                    "id": "gpt-a",
                    "displayName": "A",
                    "supportedReasoningEfforts": ["low", "high"],
                },
                {
                    "id": "gpt-b",
                    "displayName": "B",
                    "supportedReasoningEfforts": ["medium"],
                },
            ]

    async def scenario() -> None:
        manager = Manager()
        router = MVPCommandRouter(manager)

        result = await router.handle_command(
            parse_command("/models"), _envelope("/models")
        )
        assert result.count("**(current)**") == 1
        assert (
            "- **`runtime default`** **(current)** **(unavailable)**. "
            "Efforts: not reported; current effort: `model default` (default)."
        ) in result
        assert "- **`gpt-a`** - A." in result

        selection = await router.handle_command(
            parse_command("/model effort high"),
            _envelope("/model effort high"),
        )
        assert "- **Model:** `runtime default`" in selection
        assert "- **Reasoning effort:** `high` (override)" in selection

    asyncio.run(scenario())


def test_models_markdown_keeps_stale_durable_selection_visible_as_current():
    class Manager(_ModelCommandManager):
        def __init__(self) -> None:
            super().__init__()
            self.model_id = "retired-model"
            self.effort = "high"

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/models"), _envelope("/models")
        )

        assert result.count("**(current)**") == 1
        assert "- **`gpt-fast`** **(default)** - Fast." in result
        assert (
            "- **`retired-model`** **(current)** **(unavailable)**. "
            "Efforts: not reported; current effort: `high` (override)."
        ) in result

    asyncio.run(scenario())


def test_models_keep_stale_selection_visible_when_catalog_is_empty():
    class Manager(_ModelCommandManager):
        async def list_models(self, **_kwargs):
            return []

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/models"), _envelope("/models")
        )

        assert result.count("**(current)**") == 1
        assert (
            "- **`gpt-deep`** **(current)** **(unavailable)**. "
            "Efforts: not reported; current effort: `xhigh` (override)."
        ) in result

    asyncio.run(scenario())


def test_model_exact_syntax_dispatches_model_and_effort_only_calls():
    async def scenario() -> None:
        manager = _ModelCommandManager()
        router = MVPCommandRouter(manager)

        selected = await router.handle_command(
            parse_command("/model gpt-deep xhigh"),
            _envelope("/model gpt-deep xhigh"),
        )
        assert selected == (
            "## Current Model\n\n"
            "- **Agent:** `planner`\n"
            "- **Model:** `gpt-deep`\n"
            "- **Reasoning effort:** `xhigh` (override)\n"
            "- **Applies to:** future tasks"
        )

        effort_only = await router.handle_command(
            parse_command("/model effort default"),
            _envelope("/model effort default"),
        )
        assert effort_only == (
            "## Current Model\n\n"
            "- **Agent:** `planner`\n"
            "- **Model:** `gpt-deep`\n"
            "- **Reasoning effort:** `model default` (default)\n"
            "- **Applies to:** future tasks"
        )
        assert manager.calls == [
            ("list_models", "planner", "False"),
            (
                "set_model",
                "gpt-deep",
                "xhigh",
                "planner",
                "wechat:bot:user:default:codex",
            ),
            (
                "set_reasoning_effort",
                "default",
                "planner",
                "wechat:bot:user:default:codex",
            ),
        ]

    asyncio.run(scenario())


def test_model_effort_default_does_not_require_live_catalog():
    class Manager(_ModelCommandManager):
        async def list_models(self, **_kwargs):
            raise AssertionError("effort reset must not query the live catalog")

    async def scenario() -> None:
        manager = Manager()
        result = await MVPCommandRouter(manager).handle_command(
            parse_command("/model effort default"),
            _envelope("/model effort default"),
        )

        assert "- **Model:** `gpt-deep`" in result
        assert "- **Reasoning effort:** `model default` (default)" in result
        assert manager.model_id == "gpt-deep"
        assert manager.effort == ""
        assert manager.calls == [
            (
                "set_reasoning_effort",
                "default",
                "planner",
                "wechat:bot:user:default:codex",
            )
        ]

    asyncio.run(scenario())


def test_model_commands_terminalize_sdk_service_failures_deterministically():
    class CatalogFailure(_ModelCommandManager):
        async def list_models(self, **_kwargs):
            raise TransportClosedError("connection detail must not leak")

    class SelectionFailure(_ModelCommandManager):
        async def set_model(self, *_args, **_kwargs):
            raise CodexRpcError(
                -32000,
                "provider detail must not leak",
            )

        async def set_reasoning_effort(self, *_args, **_kwargs):
            raise TransportClosedError("connection detail must not leak")

    class DecodeFailure(_ModelCommandManager):
        async def list_models(self, **_kwargs):
            TypeAdapter(int).validate_python({"provider_secret": "x" * 10_000})

        async def set_reasoning_effort(self, *_args, **_kwargs):
            TypeAdapter(int).validate_python({"provider_secret": "x" * 10_000})

    async def scenario() -> None:
        catalog_router = MVPCommandRouter(CatalogFailure())
        for command in ("/model", "/models"):
            assert await catalog_router.handle_command(
                parse_command(command), _envelope(command)
            ) == "cannot list model: model service is unavailable"

        assert await MVPCommandRouter(SelectionFailure()).handle_command(
            parse_command("/model gpt-deep high"),
            _envelope("/model gpt-deep high"),
        ) == "cannot set model: model service is unavailable"
        assert await MVPCommandRouter(SelectionFailure()).handle_command(
            parse_command("/model effort default"),
            _envelope("/model effort default"),
        ) == "cannot set model: model service is unavailable"

        decode_router = MVPCommandRouter(DecodeFailure())
        assert await decode_router.handle_command(
            parse_command("/models"), _envelope("/models")
        ) == "cannot list model: model service is unavailable"
        assert await decode_router.handle_command(
            parse_command("/model effort default"),
            _envelope("/model effort default"),
        ) == "cannot set model: model service is unavailable"

    asyncio.run(scenario())


def test_model_capability_errors_are_bounded_and_markdown_safe():
    hostile = "unknown model: `bad`\n## leaked " + ("x" * 10_000)

    class Manager(_ModelCommandManager):
        async def set_model(self, *_args, **_kwargs):
            raise ValueError(hostile)

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/model missing high"),
            _envelope("/model missing high"),
        )

        assert result.startswith("cannot set model: unknown model: 'bad' ## leaked ")
        assert len(result) <= 550
        assert "`" not in result
        assert "\n" not in result
        assert hostile not in result
        assert result.endswith("...")

    asyncio.run(scenario())


def test_model_read_response_bounds_and_sanitizes_live_catalog_fields():
    hostile = ("value`\n## injected " * 1000).strip()

    class Manager(_ModelCommandManager):
        async def get_active_agent(self, **_kwargs) -> str:
            return hostile

        async def list_models(self, **_kwargs):
            return [
                {
                    "id": hostile,
                    "isDefault": True,
                    "defaultReasoningEffort": hostile,
                }
            ]

        async def get_model_selection(self, **_kwargs) -> dict[str, str]:
            return {"model_id": "", "reasoning_effort": ""}

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/model"), _envelope("/model")
        )

        assert len(result) <= _MAX_COMMAND_MARKDOWN
        assert result.count("`") == 6
        assert "\n## injected" not in result
        assert "'" in result
        assert "..." in result

    asyncio.run(scenario())


def test_model_read_response_bounds_stale_persisted_selection():
    hostile = "retired`model" * 2000

    class Manager(_ModelCommandManager):
        async def list_models(self, **_kwargs):
            return []

        async def get_model_selection(self, **_kwargs) -> dict[str, str]:
            return {"model_id": hostile, "reasoning_effort": hostile}

    async def scenario() -> None:
        result = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/model"), _envelope("/model")
        )

        assert len(result) <= _MAX_COMMAND_MARKDOWN
        assert result.count("`") == 6
        assert hostile not in result
        assert "retired'model" in result
        assert "..." in result

    asyncio.run(scenario())


def test_command_arities_are_rejected_before_dispatch():
    expected = {
        "/agents extra": "usage: /agents",
        "/modes extra": "usage: /modes",
        "/models extra": "usage: /models",
        "/model gpt-deep": (
            "usage: /model [<model-id> <effort|default>|effort "
            "<effort|default>]"
        ),
        "/model gpt-deep high extra": (
            "usage: /model [<model-id> <effort|default>|effort "
            "<effort|default>]"
        ),
        "/model effort": (
            "usage: /model [<model-id> <effort|default>|effort "
            "<effort|default>]"
        ),
        "/cancel task-1 extra": "usage: /cancel [task-id]",
    }

    async def scenario() -> None:
        router = MVPCommandRouter(object())
        for text, usage in expected.items():
            assert await router.handle_command(
                parse_command(text), _envelope(text)
            ) == usage

    asyncio.run(scenario())


def test_cancel_accepts_explicit_id_and_selects_only_current_running_task():
    target = {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": "user",
        "session_id": "default",
    }

    class Manager:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []
            self.tasks = {
                "explicit-task": {
                    "task_id": "explicit-task",
                    "agent_id": "codex",
                    "status": "running",
                    "reply_target": target,
                },
                "current-task": {
                    "task_id": "current-task",
                    "agent_id": "codex",
                    "status": "running",
                    "reply_target": target,
                },
            }

        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def list_tasks(self, **kwargs):
            self.calls.append(("list_tasks", kwargs))
            return [self.tasks["current-task"]]

        async def get_task(self, task_id: str):
            self.calls.append(("get_task", task_id))
            return self.tasks.get(task_id)

        async def cancel(self, task_id: str, **_kwargs) -> bool:
            self.calls.append(("cancel", task_id))
            return True

    async def scenario() -> None:
        manager = Manager()
        router = MVPCommandRouter(manager)
        assert await router.handle_command(
            parse_command("/cancel explicit-task"),
            _envelope("/cancel explicit-task"),
        ) == "cancel requested: explicit-task"
        assert await router.handle_command(
            parse_command("/cancel"), _envelope("/cancel")
        ) == "cancel requested: current-task"

        assert manager.calls[0:2] == [
            ("get_task", "explicit-task"),
            ("cancel", "explicit-task"),
        ]
        assert manager.calls[2] == (
            "list_tasks",
            {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "user_id": "user",
                "session_id": "default",
                "conversation_id": "wechat:bot:user:default:codex",
                "agent_id": "codex",
                "states": ("running",),
                "limit": 2,
                "newest_first": True,
            },
        )
        assert manager.calls[3:] == [
            ("get_task", "current-task"),
            ("cancel", "current-task"),
        ]

    asyncio.run(scenario())


def test_explicit_cancel_accepts_user_owned_task_from_another_session():
    class Manager:
        def __init__(self) -> None:
            self.cancelled: list[str] = []

        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def get_task(self, task_id: str):
            return {
                "task_id": task_id,
                "agent_id": "planner",
                "status": "running",
                "reply_target": {
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "older-session",
                },
            }

        async def cancel(self, task_id: str, **_kwargs) -> bool:
            self.cancelled.append(task_id)
            return True

    async def scenario() -> None:
        manager = Manager()
        response = await MVPCommandRouter(manager).handle_command(
            parse_command("/cancel older-task"),
            _envelope("/cancel older-task", session_id="current-session"),
        )

        assert response == "cancel requested: older-task"
        assert manager.cancelled == ["older-task"]

    asyncio.run(scenario())


@pytest.mark.parametrize("command", ("cancel", "retry"))
@pytest.mark.parametrize(
    "target",
    (
        {"bot_id": "bot", "external_user_id": "user"},
        {"channel": "wechat", "external_user_id": "user"},
        {"channel": "", "bot_id": "bot", "external_user_id": "user"},
        {"channel": "wechat", "bot_id": "", "external_user_id": "user"},
        {"channel": "other", "bot_id": "bot", "external_user_id": "user"},
        {"channel": "wechat", "bot_id": "other", "external_user_id": "user"},
    ),
)
def test_explicit_task_controls_reject_incomplete_or_foreign_channel_scope(
    command,
    target,
):
    class Manager:
        def __init__(self) -> None:
            self.controlled: list[str] = []

        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def get_task(self, task_id: str):
            return {
                "task_id": task_id,
                "status": "failed" if command == "retry" else "running",
                "reply_target": target,
            }

        async def cancel(self, task_id: str, **_kwargs) -> bool:
            self.controlled.append(task_id)
            return True

        async def retry(self, task_id: str, **_kwargs) -> bool:
            self.controlled.append(task_id)
            return True

    async def scenario() -> None:
        manager = Manager()
        response = await MVPCommandRouter(manager).handle_command(
            parse_command(f"/{command} task-1"),
            _envelope(f"/{command} task-1"),
        )

        assert response == f"cannot {command} task task-1"
        assert manager.controlled == []

    asyncio.run(scenario())


def test_interrupt_is_an_unknown_command():
    async def scenario() -> None:
        router = MVPCommandRouter(object())
        for text in ("/interrupt", "/interrupt task-1"):
            assert await router.handle_command(
                parse_command(text), _envelope(text)
            ) == "unknown command: /interrupt. try /help"

    asyncio.run(scenario())


def test_mode_is_the_only_user_facing_execute_control():
    class Manager:
        def __init__(self) -> None:
            self.mode = "chat"
            self.set_calls: list[tuple[str, str, bool, str]] = []

        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def get_mode(self, **_kwargs) -> str:
            return self.mode

        async def set_mode(
            self,
            mode_id: str,
            *,
            actor: str,
            explicit: bool,
            agent_id: str,
            **_kwargs,
        ) -> str:
            self.set_calls.append((mode_id, actor, explicit, agent_id))
            self.mode = mode_id
            return mode_id

    async def scenario() -> None:
        manager = Manager()
        router = MVPCommandRouter(manager)

        assert await router.handle_command(
            parse_command("/mode"), _envelope("/mode")
        ) == "mode: chat"
        assert await router.handle_command(
            parse_command("/mode REVIEW"), _envelope("/mode REVIEW")
        ) == "mode: review"
        assert manager.set_calls == [("review", "user", True, "codex")]

        for text in ("/mode invalid", "/mode chat extra"):
            assert await router.handle_command(
                parse_command(text), _envelope(text)
            ) == "usage: /mode [chat|plan|review|execute]"

        assert await router.handle_command(
            parse_command("/execute"), _envelope("/execute")
        ) == "unknown command: /execute. try /help"

        class BrokenModeManager(Manager):
            async def get_mode(self, **_kwargs) -> str:
                raise RuntimeError("persisted mode is unavailable")

        assert await MVPCommandRouter(BrokenModeManager()).handle_command(
            parse_command("/mode"), _envelope("/mode")
        ) == "cannot get mode: persisted mode is unavailable"

    asyncio.run(scenario())


class _ResetRuntime:
    def __init__(self) -> None:
        self.reset_calls: list[str] = []

    async def reset_session(self, conversation_id: str) -> str:
        self.reset_calls.append(conversation_id)
        return "new-thread"


def test_clear_resets_runtime_and_forgets_durable_thread_binding(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        runtime = _ResetRuntime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
        )
        try:
            conversation = "wechat:bot:user:default:codex"
            task = await store.create_task(
                AgentTask(
                    task_id="queued-before-clear",
                    agent_id="codex",
                    conversation_id=conversation,
                    mode_id="chat",
                    profile_version=1,
                    policy_version=1,
                    reply_target=ReplyTarget(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="user",
                        session_id="default",
                    ),
                    inputs={"text": "old"},
                )
            )
            assert await store.set_task_thread(task.task_id, thread_id="old-thread")
            assert await store.get_thread_binding(
                conversation,
                mode_id="chat",
                profile_version=1,
                policy_version=1,
            ) == "old-thread"

            router = MVPCommandRouter(manager)
            result = await router.handle_command(parse_command("/clear"), _envelope("/clear"))
            assert result == "context cleared, starting a new conversation"
            assert runtime.reset_calls == [conversation]
            assert await store.get_thread_binding(
                conversation,
                mode_id="chat",
                profile_version=1,
                policy_version=1,
            ) is None
        finally:
            await store.close()

    asyncio.run(scenario())


class _ModelRuntime:
    agent_id = "codex"

    def __init__(self) -> None:
        self.model_calls: list[tuple[str, str, str]] = []

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
    ) -> list[dict[str, object]]:
        assert include_hidden is False
        return [dict(model) for model in _MODEL_CATALOG]

    async def set_model(self, conversation_id: str, model_id: str) -> None:
        self.model_calls.append(("model", conversation_id, model_id))

    async def set_reasoning_effort(
        self, conversation_id: str, reasoning_effort: str
    ) -> None:
        self.model_calls.append(("effort", conversation_id, reasoning_effort))


def test_model_selection_is_durable_per_dynamic_agent_session_and_snapshotted(
    tmp_path,
):
    database = tmp_path / "runtime.sqlite"
    scope = {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": "user",
        "agent_id": "planner",
    }

    def manager_for(runtime: _ModelRuntime) -> TaskManager:
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        return TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )

    async def scenario() -> None:
        first = manager_for(_ModelRuntime())
        await first.start()
        try:
            for session_id in ("default", "focused"):
                await first.set_active_agent(
                    "planner",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id=session_id,
                )

            before = await first.accept_inbound(
                _envelope("before selection", message_id="model-before"),
                create_task=True,
            )
            assert before.task is not None
            assert (before.task.agent_id, before.task.model, before.task.reasoning_effort) == (
                "planner",
                "",
                "",
            )

            assert await first.set_model(
                "gpt-deep",
                reasoning_effort="xhigh",
                session_id="default",
                **scope,
            ) == {
                "agent_id": "planner",
                "model_id": "gpt-deep",
                "reasoning_effort": "xhigh",
                "conversation_id": "wechat:bot:user:default:planner",
            }
            await first.set_model(
                "gpt-fast",
                reasoning_effort="low",
                session_id="focused",
                **scope,
            )

            selected = await first.accept_inbound(
                _envelope("selected", message_id="model-selected"),
                create_task=True,
            )
            focused = await first.accept_inbound(
                _envelope(
                    "focused",
                    message_id="model-focused",
                    session_id="focused",
                ),
                create_task=True,
            )
            assert selected.task is not None
            assert focused.task is not None
            assert (selected.task.agent_id, selected.task.model, selected.task.reasoning_effort) == (
                "planner",
                "gpt-deep",
                "xhigh",
            )
            assert (focused.task.agent_id, focused.task.model, focused.task.reasoning_effort) == (
                "planner",
                "gpt-fast",
                "low",
            )
            selected_task_id = selected.task.task_id
        finally:
            await first.stop()

        second = manager_for(_ModelRuntime())
        await second.start()
        try:
            default_selection = await second.get_model_selection(
                session_id="default", **scope
            )
            focused_selection = await second.get_model_selection(
                session_id="focused", **scope
            )
            assert (
                default_selection["agent_id"],
                default_selection["model_id"],
                default_selection["reasoning_effort"],
            ) == ("planner", "gpt-deep", "xhigh")
            assert (
                focused_selection["agent_id"],
                focused_selection["model_id"],
                focused_selection["reasoning_effort"],
            ) == ("planner", "gpt-fast", "low")

            with pytest.raises(ValueError, match="unknown model: missing-model"):
                await second.set_model(
                    "missing-model",
                    reasoning_effort="high",
                    session_id="default",
                    **scope,
                )
            assert await second.get_model_selection(
                session_id="default", **scope
            ) == default_selection

            with pytest.raises(
                ValueError,
                match="gpt-fast does not support reasoning effort: xhigh",
            ):
                await second.set_model(
                    "gpt-fast",
                    reasoning_effort="xhigh",
                    session_id="default",
                    **scope,
                )
            assert await second.get_model_selection(
                session_id="default", **scope
            ) == default_selection

            future = await second.accept_inbound(
                _envelope("future", message_id="model-future"),
                create_task=True,
            )
            assert future.task is not None
            assert (future.task.agent_id, future.task.model, future.task.reasoning_effort) == (
                "planner",
                "gpt-deep",
                "xhigh",
            )

            original = await second.get_task(selected_task_id)
            assert original is not None
            assert (original.model, original.reasoning_effort) == ("gpt-deep", "xhigh")
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_manager_effort_change_handles_catalog_without_default_marker(tmp_path):
    class Runtime(_ModelRuntime):
        async def list_models(self, *, include_hidden: bool = False):
            assert include_hidden is False
            return [
                {
                    "id": "gpt-a",
                    "supportedReasoningEfforts": ["low", "high"],
                },
                {
                    "id": "gpt-b",
                    "supportedReasoningEfforts": ["low", "medium"],
                },
            ]

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = Runtime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        scope = {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
            "agent_id": "codex",
        }
        try:
            selected = await manager.set_reasoning_effort("low", **scope)
            assert selected["model_id"] == ""
            assert selected["reasoning_effort"] == "low"
            assert await manager.get_model_selection(**scope) == selected

            with pytest.raises(ValueError, match="cannot be proven"):
                await manager.set_reasoning_effort("high", **scope)
            assert (await manager.get_model_selection(**scope))["reasoning_effort"] == "low"

            cleared = await manager.set_reasoning_effort("default", **scope)
            assert cleared["model_id"] == ""
            assert cleared["reasoning_effort"] == ""
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_manager_clears_effort_when_model_catalog_is_empty(tmp_path):
    class Runtime(_ModelRuntime):
        async def list_models(self, *, include_hidden: bool = False):
            raise AssertionError("clearing an effort must not query the catalog")

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = Runtime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        scope = {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
            "agent_id": "codex",
        }
        try:
            await store.set_session_model_preference(
                **scope,
                model_id="retired-model",
                reasoning_effort="xhigh",
            )

            cleared = await manager.set_reasoning_effort("default", **scope)

            assert cleared["model_id"] == "retired-model"
            assert cleared["reasoning_effort"] == ""
            assert await manager.get_model_selection(**scope) == cleared
        finally:
            await manager.stop()

    asyncio.run(scenario())
