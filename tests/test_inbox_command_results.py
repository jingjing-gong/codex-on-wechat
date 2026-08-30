"""Regressions for keeping command responses out of ``/inbox``."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.agents.base import AgentResult
from src.channels.wechat import COMMAND_HELP, WeChatGateway
from src.runtime.manager import TaskManager
from src.runtime.models import InboundMessage, ReplyFragmentState
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


class _Runtime:
    agent_id = "codex"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="unused")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _message(text: str, message_id: int) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token=f"context-{message_id}",
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


def test_production_inbox_excludes_command_results_but_keeps_agent_messages(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            reconcile_interval=None,
        )
        await manager.start()
        sent = []

        def send(request):
            sent.append(request)
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        client = SimpleNamespace(bot_id="bot", send_message=send)
        gateway = WeChatGateway(manager, bot_id="bot")
        try:
            help_result = await gateway.handle_message(
                client, _message("/help", 1)
            )
            switch_result = await gateway.handle_message(
                client, _message("/agent codex", 2)
            )
            assert help_result is not None
            assert help_result.command_response == COMMAND_HELP
            assert switch_result is not None
            assert switch_result.command_response == "switched to Agent: codex"

            unseen_ids = {
                item.outbox_id
                for item in await store.list_outbox(unseen=True, limit=20)
            }
            assert help_result.response_delivery_id in unseen_ids
            assert switch_result.response_delivery_id in unseen_ids

            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="normal-origin",
                    text="normal user message",
                ),
                create_task=False,
            )
            normal = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="normal-agent-reply",
                content="normal Agent message",
                agent_id="codex",
                notify_enabled=False,
            )
            assert normal.candidate is not None
            assert {fragment.state for fragment in normal.fragments} == {
                ReplyFragmentState.INBOX_ONLY
            }

            inbox_result = await gateway.handle_message(
                client, _message("/inbox", 3)
            )
            assert inbox_result is not None
            assert inbox_result.command_response == (
                "inbox:\n- normal Agent message"
            )
            assert "## Commands" not in inbox_result.command_response
            assert "switched to Agent" not in inbox_result.command_response
            assert len(sent) == 3
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_inbox_outbox_filter_excludes_every_command_fragment(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            command_scope = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="command-origin",
                    text="/system long-role",
                ),
                create_task=False,
            )
            command = await store.project_reply_candidate(
                target=command_scope.inbound.target(),
                source_key="outbox:command:wechat:test",
                content="command result part one command result part two",
                fragments=(
                    {"kind": "text", "content": "command result part one"},
                    {"kind": "text", "content": "command result part two"},
                ),
                agent_id="codex",
                foreground=True,
                outbox_id="command:wechat:test",
                client_id="command-client",
            )
            assert len(command.outbox_items) == 2
            assert any(
                not item.outbox_id.startswith("command:")
                for item in command.outbox_items
            )

            normal_scope = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="agent-origin",
                    text="normal user message",
                ),
                create_task=False,
            )
            normal = await store.project_reply_candidate(
                target=normal_scope.inbound.target(),
                source_key="normal-agent-output",
                content="normal allocated Agent message",
                agent_id="codex",
            )
            assert len(normal.outbox_items) == 1

            visible = await store.list_outbox(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                unseen=True,
                include_command_responses=False,
                limit=20,
            )
            assert [item.content for item in visible] == [
                "normal allocated Agent message"
            ]
        finally:
            await store.close()

    asyncio.run(scenario())
