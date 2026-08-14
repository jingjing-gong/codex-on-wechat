"""Ownership checks for durable transcription candidates."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.models import InboundMessage
from src.runtime.sqlite_store import SQLiteStore, StoreError


def test_candidate_cannot_rebind_foreign_inbound_route(tmp_path):
    """A candidate route must match the inbound envelope it projects."""

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="alice",
                    external_message_id="alice-message",
                    session_id="default",
                    text="alice-private-caption",
                ),
                create_task=False,
            )
            with pytest.raises(StoreError, match="inbound|route|ownership|access"):
                await store.create_transcription_candidate(
                    confirmation_id="cross-user-candidate",
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="bob",
                    session_id="default",
                    agent_id="codex",
                    inbound_message_id=accepted.inbound.message_id,
                    candidate_text="forged transcript",
                )

            assert await store.get_transcription_candidate("cross-user-candidate") is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_inbound_candidate_snapshots_default_agent_owner(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="alice",
                    external_message_id="voice-message",
                ),
                create_task=False,
                transcription_candidates={
                    "confirmation_id": "default-agent-candidate",
                    "candidate_text": "hello",
                },
            )

            assert accepted.inbound.payload["__route_snapshot"]["agent_id"] == "codex"
            candidate = await store.get_transcription_candidate(
                "default-agent-candidate"
            )
            assert candidate is not None
            assert candidate["agent_id"] == "codex"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_candidate_cannot_override_selected_agent_route(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            with pytest.raises(StoreError, match="ownership|route"):
                await store.accept_inbound(
                    InboundMessage(
                        channel="wechat",
                        bot_id="bot",
                        external_user_id="alice",
                        external_message_id="agent-route-conflict",
                    ),
                    task={
                        "agent_id": "codex",
                        "mode_id": "chat",
                        "profile_version": 1,
                        "policy_version": 1,
                    },
                    transcription_candidates={
                        "confirmation_id": "foreign-agent-candidate",
                        "agent_id": "planner",
                        "candidate_text": "forged transcript",
                    },
                )

            assert await store.get_inbound(
                "wechat", "bot", "agent-route-conflict"
            ) is None
            assert await store.get_transcription_candidate(
                "foreign-agent-candidate"
            ) is None
        finally:
            await store.close()

    asyncio.run(scenario())
