"""Compound identity collision and legacy compatibility coverage."""

from __future__ import annotations

import asyncio
import sqlite3

from src.channels.wechat import command_delivery_id, conversation_id_for
from src.channels.models import InboundEnvelope
from src.runtime.identity import conversation_id
from src.runtime.sqlite_store import SQLiteStore


def _envelope(*, bot_id: str, user_id: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id=bot_id,
        external_user_id=user_id,
        external_message_id="message",
        text="/status",
    )


def test_compound_ids_do_not_collide_when_components_contain_colons():
    assert conversation_id_for(
        bot_id="bot:a", external_user_id="user"
    ) != conversation_id_for(bot_id="bot", external_user_id="a:user")
    assert command_delivery_id(
        _envelope(bot_id="bot:a", user_id="user")
    ) != command_delivery_id(_envelope(bot_id="bot", user_id="a:user"))


def test_store_reuses_matching_legacy_conversation(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        try:
            target = {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "session_id": "default",
            }
            first = await store.create_task(
                {
                    "task_id": "legacy",
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:user:default:codex",
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "reply_target": target,
                    "inputs": {"text": "first"},
                }
            )
            second = await store.create_task(
                {
                    "task_id": "canonical",
                    "agent_id": "codex",
                    "conversation_id": conversation_id(
                        "wechat", "bot", "user", "default", "codex"
                    ),
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "reply_target": target,
                    "inputs": {"text": "second"},
                }
            )
            assert second.conversation_id == first.conversation_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_store_keeps_colon_bearing_scopes_distinct(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        try:
            tasks = []
            for task_id, bot_id, user_id in (
                ("first", "bot:a", "user"),
                ("second", "bot", "a:user"),
            ):
                tasks.append(
                    await store.create_task(
                        {
                            "task_id": task_id,
                            "agent_id": "codex",
                            "mode_id": "chat",
                            "profile_version": 1,
                            "policy_version": 1,
                            "reply_target": {
                                "channel": "wechat",
                                "bot_id": bot_id,
                                "external_user_id": user_id,
                                "session_id": "default",
                            },
                            "inputs": {"text": task_id},
                        }
                    )
                )
            assert tasks[0].conversation_id != tasks[1].conversation_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_nonmatching_legacy_collision_does_not_block_canonical_scope(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        try:
            first = await store.create_task(
                {
                    "task_id": "legacy-owner",
                    "agent_id": "codex",
                    "conversation_id": "wechat:bot:a:user:default:codex",
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot:a",
                        "external_user_id": "user",
                        "session_id": "default",
                    },
                    "inputs": {"text": "legacy"},
                }
            )
            second = await store.create_task(
                {
                    "task_id": "canonical-owner",
                    "agent_id": "codex",
                    "mode_id": "chat",
                    "profile_version": 1,
                    "policy_version": 1,
                    "reply_target": {
                        "channel": "wechat",
                        "bot_id": "bot",
                        "external_user_id": "a:user",
                        "session_id": "default",
                    },
                    "inputs": {"text": "canonical"},
                }
            )
            assert first.conversation_id != second.conversation_id
        finally:
            await store.close()

        connection = sqlite3.connect(path)
        try:
            assert connection.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone()[0] == 2
        finally:
            connection.close()

    asyncio.run(scenario())
