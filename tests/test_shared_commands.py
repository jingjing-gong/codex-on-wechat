"""Channel-neutral command registry and capability-policy coverage."""

from __future__ import annotations

import asyncio
import hashlib

import src.channels.wechat as wechat
from src.channels import commands
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter


RECV_HELP_LINE = (
    "- `/recv` - Receive the next replies deferred by WeChat's ten-message quota"
)


def _envelope(text: str, *, channel: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel=channel,
        bot_id="bot",
        external_user_id="user",
        external_message_id="message-1",
        text=text,
        session_id="default",
        agent_id="codex",
        conversation_id=f"{channel}:bot:user:default:codex",
    )


def test_wechat_keeps_historical_registry_exports_and_help() -> None:
    assert commands.COMMAND_HELP == wechat.COMMAND_HELP
    assert commands.COMMAND_REGISTRY is wechat.COMMAND_REGISTRY
    assert commands.CommandRegistryEntry is wechat.CommandRegistryEntry
    assert commands.CommandRegistryGroup is wechat.CommandRegistryGroup
    assert commands.MVP_COMMANDS is wechat.MVP_COMMANDS
    assert commands.MVP_COMMAND_NAMES is wechat.MVP_COMMAND_NAMES

    assert RECV_HELP_LINE in commands.COMMAND_HELP.splitlines()
    assert hashlib.sha256(commands.COMMAND_HELP.encode("utf-8")).hexdigest() == (
        "ed5077d23488a0854aebbb81a79fc2bd795016cfe0f656b49021aef83745bdee"
    )
    assert commands.command_help_for_channel() == commands.COMMAND_HELP
    assert commands.command_help_for_channel("wechat") == commands.COMMAND_HELP


def test_runtime_router_is_defined_once_and_reexported_by_wechat() -> None:
    assert commands.MVPCommandRouter is wechat.MVPCommandRouter
    assert commands.MVPCommandRouter.__module__ == "src.channels.commands"


def test_lark_and_feishu_compose_adapter_commands_without_changing_wechat() -> None:
    for channel in ("lark", "feishu"):
        help_text = commands.command_help_for_channel(channel)
        registry = commands.filter_command_registry(channel=channel)
        names = commands.command_names_for_channel(channel)

        assert RECV_HELP_LINE not in help_text.splitlines()
        assert "`/inbox [agent-id|all]`" in help_text
        assert "### Delivery" in help_text
        assert "recv" not in names
        assert "inbox" in names
        assert "cron" in names
        assert "lark" in names
        assert "### Lark" in help_text
        assert "`/lark add [profile]`" in help_text
        assert all(
            entry.name != "recv"
            for group in registry
            for entry in group.entries
        )


def test_channel_capability_policy_rejects_recv_outside_wechat() -> None:
    assert commands.command_supported("recv", channel="wechat")
    assert commands.command_supported("/recv", channel="wechat")
    assert not commands.command_supported("recv", channel="lark")
    assert not commands.command_supported("recv", channel="feishu")
    assert commands.command_supported("inbox", channel="lark")
    assert commands.command_supported("lark", channel="lark")
    assert commands.command_supported("lark", channel="feishu")
    assert not commands.command_supported("lark", channel="wechat")


def test_lark_router_help_uses_filtered_registry() -> None:
    class Manager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

    async def scenario() -> None:
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/help"),
            _envelope("/help", channel="lark"),
        )

        assert response == commands.command_help_for_channel("lark")
        assert response is not None
        assert RECV_HELP_LINE not in response.splitlines()
        assert "`/inbox [agent-id|all]`" in response
        assert "`/lark add [profile]`" in response

    asyncio.run(scenario())


def test_lark_router_rejects_recv_without_touching_wechat_quota_api() -> None:
    class Manager:
        def __getattr__(self, name: str):
            raise AssertionError(f"unsupported Lark command called manager.{name}")

    async def scenario() -> None:
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command("/recv"),
            _envelope("/recv", channel="lark"),
        )
        assert response == "unknown command: /recv. try /help"

    asyncio.run(scenario())


def test_wechat_task_singular_stays_unknown_and_tasks_output_is_unchanged() -> None:
    class Manager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

        async def list_tasks(self, **_kwargs):
            return []

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        singular = await router.handle_command(
            parse_command("/task"),
            _envelope("/task", channel="wechat"),
        )
        plural = await router.handle_command(
            parse_command("/tasks"),
            _envelope("/tasks", channel="wechat"),
        )

        assert singular == "unknown command: /task. try /help"
        assert plural == "tasks: (none)"

    asyncio.run(scenario())


def test_wechat_does_not_parse_or_expose_lark_onboarding_controls() -> None:
    class Manager:
        pass

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        slash = await router.handle_command(
            parse_command("/lark add"),
            _envelope("/lark add", channel="wechat"),
        )

        assert slash == "unknown command: /lark. try /help"
        assert parse_command("新增一个飞书 bot") is None
        assert "`/lark add [profile]`" not in commands.COMMAND_HELP

    asyncio.run(scenario())
