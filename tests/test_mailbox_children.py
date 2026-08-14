"""Focused regressions for collaboration ownership and child admission."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.sqlite_store import SQLiteStore, StoreError
from src.agents.base import AgentResult
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry


def _task(
    task_id: str,
    *,
    agent_id: str = "codex",
    inputs: object | None = None,
    dedupe_key: str | None = None,
    parent_task_id: str | None = None,
    child_depth: int = 0,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "dedupe_key": dedupe_key,
        "agent_id": agent_id,
        "conversation_id": f"wechat:bot:user:default:{agent_id}",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": inputs or {"text": task_id},
        "parent_task_id": parent_task_id,
        "child_depth": child_depth,
    }


def test_child_replay_cannot_move_between_parents(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            first_parent = await store.create_task(_task("parent-1"))
            second_parent = await store.create_task(_task("parent-2"))
            child = await store.create_child_task(
                _task(
                    "child-1",
                    dedupe_key="child-request-1",
                    parent_task_id=first_parent.task_id,
                    child_depth=1,
                ),
                parent_task_id=first_parent.task_id,
                max_children=2,
            )
            assert child.parent_task_id == first_parent.task_id

            with pytest.raises(StoreError, match="child-task parent"):
                await store.create_child_task(
                    _task(
                        "child-2",
                        dedupe_key="child-request-1",
                        parent_task_id=second_parent.task_id,
                        child_depth=1,
                    ),
                    parent_task_id=second_parent.task_id,
                    max_children=2,
                )

            assert await store.child_count(first_parent.task_id) == 1
            assert await store.child_count(second_parent.task_id) == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_reconcile_releases_abandoned_child_reservation(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            parent = await store.create_task(_task("parent"))
            assert await store.reserve_child_task(parent.task_id, max_children=1)
            assert await store.child_count(parent.task_id) == 1

            await store.reconcile()
            assert await store.child_count(parent.task_id) == 0

            child = await store.create_child_task(
                _task(
                    "child",
                    dedupe_key="child-after-recovery",
                    parent_task_id=parent.task_id,
                    child_depth=1,
                ),
                parent_task_id=parent.task_id,
                max_children=1,
            )
            assert child.parent_task_id == parent.task_id
            assert await store.child_count(parent.task_id) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_attachments_follow_agent_and_task_ownership(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "codex-file", "mime_type": "text/plain"}
            )
            await store.register_attachment(
                {"attachment_id": "planner-file", "mime_type": "text/plain"}
            )
            codex_task = await store.create_task(
                _task(
                    "codex-task",
                    inputs={"attachment_ids": ["codex-file"]},
                )
            )
            await store.create_task(
                _task(
                    "planner-task",
                    agent_id="planner",
                    inputs={"attachments": ["planner-file"]},
                )
            )

            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="inspect this",
                request_id="request-1",
                task_id=codex_task.task_id,
                payload={"attachment_ids": ["codex-file"]},
            )
            replay = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="inspect this",
                request_id="request-1",
                task_id=codex_task.task_id,
                payload={"attachment_ids": ["codex-file"]},
            )
            assert replay.mailbox_id == request.mailbox_id

            # A reply remains correlated to the request's task, but the
            # responding Agent may attach output owned by its own task in the
            # same durable user/session scope.
            response = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="result",
                request_id=request.request_id,
                reply_to_id=request.message_id,
                task_id=codex_task.task_id,
                payload={"attachments": ["planner-file"]},
            )
            assert response.reply_to_id == request.message_id

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.create_agent_message(
                    source_agent_id="reviewer",
                    destination_agent_id="planner",
                    content="steal",
                    request_id="request-2",
                    payload={"attachment_ids": ["codex-file"]},
                )
            with pytest.raises(StoreError, match="attachment is unavailable"):
                await store.create_agent_message(
                    source_agent_id="codex",
                    destination_agent_id="planner",
                    content="missing",
                    request_id="request-3",
                    payload={"attachment_ids": ["does-not-exist"]},
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_worker_replies_through_task_manager(tmp_path):
    class Runtime:
        def __init__(self, agent_id: str, content: str = "answer") -> None:
            self.agent_id = agent_id
            self.content = content
            self.calls = 0

        async def run(self, task, emit):
            self.calls += 1
            return AgentResult(task_id=task.task_id, content=self.content)

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_task(_task("source-task"))
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="question",
                request_id="request-reply",
                task_id="source-task",
            )
            planner = Runtime("planner")
            codex = Runtime("codex", "do not reply again")
            registry = AgentRegistry()
            registry.register("planner", planner)
            registry.register("codex", codex)
            manager = TaskManager(store, registry)

            assert await manager.process_mailbox_once("planner") == 1
            assert planner.calls == 1
            original = await store.get_mailbox_item(request.mailbox_id)
            assert original is not None
            assert original.state.value == "processed"
            responses = await store.list_mailbox("codex")
            assert [(item.content, item.reply_to_id) for item in responses] == [
                ("answer", request.message_id)
            ]
            assert await manager.process_mailbox_once("codex") == 1
            assert codex.calls == 1
            assert (await store.list_mailbox("planner")) == [original]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_recovery_does_not_repeat_agent_after_response_commit(tmp_path):
    class Runtime:
        agent_id = "planner"

        def __init__(self) -> None:
            self.calls = 0

        async def run(self, task, emit):
            self.calls += 1
            return AgentResult(task_id=task.task_id, content="new answer")

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.create_task(_task("source-task"))
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="question",
                request_id="request-recovery",
                task_id="source-task",
            )
            claimed = await store.claim_mailbox("planner", "crashed-worker")
            assert len(claimed) == 1
            token = claimed[0].claim_token
            assert token
            assert await store.mark_mailbox_processing(
                request.mailbox_id, claim_token=token
            )
            response = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="durable answer",
                request_id=request.request_id,
                reply_to_id=request.message_id,
                causation_id=request.message_id,
                task_id=request.task_id,
                payload={"request_type": "ask"},
            )
            # Simulate recovery of the request after its response committed but
            # before the original worker recorded successful processing.
            assert await store.mark_mailbox_failed(
                request.mailbox_id,
                claim_token=token,
                error="process stopped after response commit",
            )

            runtime = Runtime()
            registry = AgentRegistry()
            registry.register("planner", runtime)
            manager = TaskManager(store, registry)

            assert await manager.process_mailbox_once("planner") == 1
            assert runtime.calls == 0
            recovered = await store.get_mailbox_item(request.mailbox_id)
            assert recovered is not None
            assert recovered.state.value == "processed"
            assert await store.get_correlated_mailbox_response(
                request.mailbox_id
            ) == response
        finally:
            await store.close()

    asyncio.run(scenario())


def test_task_event_cannot_self_grant_foreign_attachment(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.register_attachment(
                {"attachment_id": "planner-only", "mime_type": "text/plain"}
            )
            codex_task = await store.create_task(_task("codex-task"))
            await store.create_task(
                _task(
                    "planner-task",
                    agent_id="planner",
                    inputs={"attachments": ["planner-only"]},
                )
            )
            claim = await store.claim_task_by_id(codex_task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                codex_task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )

            with pytest.raises(StoreError, match="attachment access denied"):
                await store.append_task_event(
                    codex_task.task_id,
                    {
                        "event_id": "foreign-media-event",
                        "sequence": 0,
                        "content": "not mine",
                        "attachments": ["planner-only"],
                        "destination_agent_id": "planner",
                    },
                    claim_token=claim.claim_token,
                )
        finally:
            await store.close()

    asyncio.run(scenario())
