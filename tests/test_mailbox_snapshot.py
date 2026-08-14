"""Mailbox execution identity and immutable policy snapshot regressions."""

from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from src.agents.base import AgentResult
from src.runtime.models import ReplyTarget
from src.runtime.identity import conversation_id
from src.runtime.modes import AgentMode
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry
from src.runtime.sqlite_store import SQLiteStore, StoreError
from src.runtime.worker import AgentMailboxWorker


def _task(task_id: str, user_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "agent_id": "codex",
        "conversation_id": f"wechat:bot:{user_id}:default:codex",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": user_id,
            "session_id": "default",
        },
        "inputs": {"text": task_id},
    }


class _Runtime:
    def __init__(self) -> None:
        self.tasks = []

    async def run(self, task, emit):
        self.tasks.append(task)
        return AgentResult(task_id=task.task_id, content="ok")


async def _store(path) -> SQLiteStore:
    store = SQLiteStore(path)
    await store.initialize()
    # Persist a destination definition that is deliberately different from
    # the source task's chat@1 tuple.
    await store.put_profile(
        AgentProfile(
            agent_id="planner",
            profile_version=3,
            default_mode_id="review",
            capabilities=frozenset({"read"}),
        )
    )
    await store.put_mode(
        AgentMode(
            mode_id="review",
            policy_version=7,
            developer_instructions="review only",
            allowed_tools=frozenset({"read"}),
        ),
        agent_id="planner",
    )
    return store


def test_mailbox_fallback_is_per_user_and_uses_destination_snapshot(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        runtime = _Runtime()
        try:
            first = await store.create_task(_task("source-1", "user-a"))
            second = await store.create_task(_task("source-2", "user-b"))
            first_message = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="one",
                request_id="request-a",
                task_id=first.task_id,
            )
            second_message = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="two",
                request_id="request-b",
                task_id=second.task_id,
            )

            worker = AgentMailboxWorker(store, {"planner": runtime}, "planner")
            assert await worker.run_once() == 1
            assert await worker.run_once() == 1
            assert [task.conversation_id for task in runtime.tasks] == [
                "wechat:bot:user-a:default:planner",
                "wechat:bot:user-b:default:planner",
            ]
            assert [(task.mode_id, task.profile_version, task.policy_version) for task in runtime.tasks] == [
                ("review", 3, 7),
                ("review", 3, 7),
            ]
            for mailbox_id in (first_message.mailbox_id, second_message.mailbox_id):
                item = await store.get_mailbox_item(mailbox_id)
                assert item is not None
                assert item.execution_snapshot["agent_id"] == "planner"
                assert item.execution_snapshot["conversation_id"].endswith(":planner")
        finally:
            await store.close()

    asyncio.run(scenario())


def test_taskless_mailbox_requests_get_request_scoped_conversations(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        runtime = _Runtime()
        try:
            first = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="one",
                request_id="request-a",
            )
            second = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="two",
                request_id="request-b",
            )
            assert first.execution_snapshot["conversation_id"] != second.execution_snapshot[
                "conversation_id"
            ]
            worker = AgentMailboxWorker(store, {"planner": runtime}, "planner")
            assert await worker.run_once() == 1
            assert await worker.run_once() == 1
            assert runtime.tasks[0].conversation_id != runtime.tasks[1].conversation_id
            assert runtime.tasks[0].conversation_id.endswith(":request-a")
            assert runtime.tasks[1].conversation_id.endswith(":request-b")
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_snapshot_replay_does_not_follow_new_destination_definition(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        try:
            source = await store.create_task(_task("source", "user"))
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="frozen",
                request_id="request-frozen",
                task_id=source.task_id,
            )
            original = request.execution_snapshot
            # A later immutable definition must not rewrite an already queued
            # mailbox projection.
            await store.put_profile(
                AgentProfile(
                    agent_id="planner",
                    profile_version=4,
                    default_mode_id="execute",
                    capabilities=frozenset({"read", "write"}),
                )
            )
            await store.put_mode(
                AgentMode(
                    mode_id="execute",
                    policy_version=9,
                    can_write_files=True,
                    can_execute_commands=True,
                ),
                agent_id="planner",
            )
            replay = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="frozen",
                request_id="request-frozen",
                task_id=source.task_id,
            )
            assert replay.mailbox_id == request.mailbox_id
            assert replay.execution_snapshot == original
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_snapshot_cannot_redirect_task_route(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        try:
            source = await store.create_task(_task("source", "user-a"))
            with pytest.raises(StoreError, match="conversation identity"):
                await store.create_agent_message(
                    source_agent_id="codex",
                    destination_agent_id="planner",
                    content="redirect",
                    request_id="request-redirect",
                    task_id=source.task_id,
                    execution_snapshot={
                        "conversation_id": "wechat:bot:user-b:default:planner",
                        "mode_id": "review",
                        "profile_version": 3,
                        "policy_version": 7,
                    },
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_event_projected_mailbox_gets_destination_route_snapshot(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        try:
            source = await store.create_task(_task("source", "user-a"))
            claim = await store.claim_task_by_id(source.task_id, "event-worker")
            assert claim is not None
            assert await store.mark_task_running(
                source.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            await store.append_task_event(
                source.task_id,
                {
                    "event_id": "mailbox-event",
                    "event_type": "agent_message",
                    "visibility": "internal",
                    "content": "projected",
                    "destination_agent_id": "planner",
                    "request_id": "event-request",
                    "execution_id": claim.execution_id,
                },
                claim_token=claim.claim_token,
            )
            rows = await store.list_mailbox("planner")
            assert len(rows) == 1
            assert rows[0].execution_snapshot["conversation_id"] == (
                "wechat:bot:user-a:default:planner"
            )
            assert rows[0].execution_snapshot["mode_id"] == "review"
            assert rows[0].execution_snapshot["profile_version"] == 3
            assert rows[0].execution_snapshot["policy_version"] == 7
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_legacy_conversation_requires_matching_persisted_scope(tmp_path):
    async def scenario() -> None:
        store = await _store(tmp_path / "runtime.sqlite")
        legacy_id = "wechat:bot:a:user:default:planner"
        first_target = {
            "channel": "wechat",
            "bot_id": "bot:a",
            "external_user_id": "user",
            "session_id": "default",
        }
        second_target = {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "a:user",
            "session_id": "default",
        }
        try:
            # Establish exact durable ownership of the legacy ID for only the
            # first of two scopes whose old delimiter-joined IDs collide.
            await store.create_task(
                {
                    "task_id": "legacy-owner",
                    "agent_id": "planner",
                    "conversation_id": legacy_id,
                    "mode_id": "review",
                    "profile_version": 3,
                    "policy_version": 7,
                    "reply_target": first_target,
                    "inputs": {"text": "owner"},
                }
            )
            first = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="first",
                request_id="legacy-request-a",
                reply_target=first_target,
                execution_snapshot={
                    "agent_id": "planner",
                    "conversation_id": legacy_id,
                    "reply_target": first_target,
                    "mode_id": "review",
                    "profile_version": 3,
                    "policy_version": 7,
                },
            )
            second = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="second",
                request_id="legacy-request-b",
                reply_target=second_target,
                execution_snapshot={
                    "agent_id": "planner",
                    "conversation_id": legacy_id,
                    "reply_target": second_target,
                    "mode_id": "review",
                    "profile_version": 3,
                    "policy_version": 7,
                },
            )

            assert first.execution_snapshot["conversation_id"] == legacy_id
            assert second.execution_snapshot["conversation_id"] == conversation_id(
                "wechat", "bot", "a:user", "default", "planner"
            )
            assert first.execution_snapshot["conversation_id"] != second.execution_snapshot[
                "conversation_id"
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_canonicalizes_unproven_legacy_mailbox_snapshots(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = await _store(path)
        runtime = _Runtime()
        routes = (
            ("old-a", "bot:a", "user"),
            ("old-b", "bot", "a:user"),
        )
        try:
            rows = []
            for request_id, bot_id, user_id in routes:
                target = {
                    "channel": "wechat",
                    "bot_id": bot_id,
                    "external_user_id": user_id,
                    "session_id": "default",
                }
                rows.append(
                    await store.create_agent_message(
                        source_agent_id="codex",
                        destination_agent_id="planner",
                        content=request_id,
                        request_id=request_id,
                        reply_target=target,
                        execution_snapshot={
                            "agent_id": "planner",
                            "reply_target": target,
                            "mode_id": "review",
                            "profile_version": 3,
                            "policy_version": 7,
                        },
                    )
                )
            await store.close()

            # Simulate two pre-framing mailbox snapshots that carried the same
            # ambiguous ID. Neither has a persisted matching conversation row.
            connection = sqlite3.connect(path)
            try:
                for row in rows:
                    snapshot = dict(row.execution_snapshot)
                    snapshot["conversation_id"] = (
                        "wechat:bot:a:user:default:planner"
                    )
                    connection.execute(
                        "UPDATE agent_mailbox SET execution_snapshot_json=? "
                        "WHERE mailbox_id=?",
                        (json.dumps(snapshot), row.mailbox_id),
                    )
                connection.commit()
            finally:
                connection.close()

            store = SQLiteStore(path)
            await store.initialize()
            worker = AgentMailboxWorker(store, {"planner": runtime}, "planner")
            assert await worker.run_once() == 1
            assert await worker.run_once() == 1
            assert [task.conversation_id for task in runtime.tasks] == [
                conversation_id("wechat", "bot:a", "user", "default", "planner"),
                conversation_id("wechat", "bot", "a:user", "default", "planner"),
            ]
            assert runtime.tasks[0].conversation_id != runtime.tasks[1].conversation_id
        finally:
            await store.close()

    asyncio.run(scenario())
