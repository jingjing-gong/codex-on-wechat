"""Conversation-subject routing and exact-target coexistence coverage."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope
from src.channels.models import ReplyTarget as ChannelReplyTarget
from src.runtime.identity import group_conversation_subject
from src.runtime.manager import TaskManager
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


class _Runtime:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def run(self, _task, _emit) -> AgentResult:
        return AgentResult(content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _manager(path) -> TaskManager:
    registry = AgentRegistry()
    registry.register(
        "codex",
        _Runtime(),
        profile=codex_profile(default_mode_id="chat"),
    )
    for agent_id in ("planner", "reviewer"):
        registry.register(
            agent_id,
            _Runtime(),
            profile=AgentProfile(agent_id=agent_id, display_name=agent_id.title()),
        )
    return TaskManager(
        SQLiteStore(path),
        registry,
        worker_count=0,
        default_mode_id="chat",
    )


def _group_envelope(
    *,
    bot_id: str,
    actor_id: str,
    message_id: str,
    chat_id: str = "oc_shared_chat",
) -> InboundEnvelope:
    subject = group_conversation_subject("lark", bot_id, chat_id)
    return InboundEnvelope(
        channel="lark",
        bot_id=bot_id,
        external_user_id=actor_id,
        external_message_id=message_id,
        text="work",
        conversation_subject_id=subject.conversation_subject_id,
        conversation_subject_scope=subject.scope_key,
        conversation_subject_kind="group",
        destination_kind="group",
        destination_id=chat_id,
        transport_metadata={"chat_id": chat_id, "chat_type": "group"},
    )


def test_group_routes_use_subject_but_replies_retain_actor_and_chat(tmp_path) -> None:
    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            bot_a_subject = group_conversation_subject(
                "lark", "cli_bot_a", "oc_shared_chat"
            )
            bot_b_subject = group_conversation_subject(
                "lark", "cli_bot_b", "oc_shared_chat"
            )
            await manager.set_active_agent(
                "planner",
                channel="lark",
                bot_id="cli_bot_a",
                external_user_id=bot_a_subject.scope_key,
                session_id="default",
            )
            assert await manager.get_active_agent(
                channel="lark",
                bot_id="cli_bot_a",
                external_user_id=bot_a_subject.scope_key,
                session_id="default",
            ) == "planner"
            await manager.set_active_agent(
                "reviewer",
                channel="lark",
                bot_id="cli_bot_b",
                external_user_id=bot_b_subject.scope_key,
                session_id="default",
            )
            assert await manager.get_active_agent(
                channel="lark",
                bot_id="cli_bot_b",
                external_user_id=bot_b_subject.scope_key,
                session_id="default",
            ) == "reviewer"

            first = await manager.accept_inbound(
                _group_envelope(
                    bot_id="cli_bot_a",
                    actor_id="ou_alice",
                    message_id="om_a1",
                )
            )
            same_group = await manager.accept_inbound(
                _group_envelope(
                    bot_id="cli_bot_a",
                    actor_id="ou_bob",
                    message_id="om_a2",
                )
            )
            peer_bot = await manager.accept_inbound(
                _group_envelope(
                    bot_id="cli_bot_b",
                    actor_id="ou_alice",
                    message_id="om_b1",
                )
            )
            wechat = await manager.accept_inbound(
                InboundEnvelope(
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="ou_alice",
                    external_message_id="wx-1",
                    text="work",
                )
            )

            assert first.task.agent_id == same_group.task.agent_id == "planner"
            assert first.task.conversation_id == same_group.task.conversation_id
            assert peer_bot.task.agent_id == "reviewer"
            assert peer_bot.task.conversation_id != first.task.conversation_id
            assert wechat.task.agent_id == "codex"

            # State is group-scoped, but delivery remains tied to the exact
            # originating actor/bot/chat instead of the opaque route subject.
            assert first.task.conversation_subject_id == (
                bot_a_subject.conversation_subject_id
            )
            assert first.task.identity_snapshot["conversation_subject"]["scope_key"] == (
                bot_a_subject.scope_key
            )
            assert first.task.actor_external_user_id == "ou_alice"
            assert first.task.reply_target.external_user_id == "ou_alice"
            assert first.task.reply_target.destination_id == "oc_shared_chat"
            assert first.task.reply_target.destination_kind == "group"
            assert peer_bot.task.reply_target.bot_id == "cli_bot_b"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_wechat_stable_delivery_key_ignores_additive_subject_provenance() -> None:
    legacy = ChannelReplyTarget(
        channel="wechat",
        bot_id="wechat-bot",
        external_user_id="wx-user",
        session_id="default",
        source_message_id="wx-message",
        source_sequence=7,
    )
    enriched = replace(
        legacy,
        conversation_subject_id="conversation-subject:v1:backfill",
        conversation_subject_scope="wx-user",
        destination_kind="direct",
        destination_id="wx-user",
    )
    assert enriched.stable_key() == legacy.stable_key()

    # The same additive fields are delivery identity on Lark and therefore do
    # distinguish exact chat/thread destinations there.
    lark = replace(legacy, channel="lark", bot_id="cli_bot")
    lark_group = replace(
        lark,
        conversation_subject_id="conversation-subject:v1:group",
        conversation_subject_scope="conversation-subject-scope:v1:group",
        destination_kind="group",
        destination_id="oc_group",
    )
    assert lark_group.stable_key() != lark.stable_key()
