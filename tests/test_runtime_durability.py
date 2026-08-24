"""High-risk transaction and worker durability regressions."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from src.agents.base import AgentEvent, AgentResult, AgentTask
from src.runtime.media import AttachmentStore
from src.runtime.models import InboundMessage, ReplyFragmentState
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.sqlite_store import StoreError
from src.runtime.worker import AgentMailboxWorker, TaskWorker


def _task() -> dict[str, object]:
    return {
        "agent_id": "codex",
        "conversation_id": "wechat:bot:user:default:codex",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": {"text": "hello"},
    }


def test_worker_does_not_terminalize_after_thread_binding_persistence_error():
    class Store:
        def __init__(self) -> None:
            self.completed = False

        async def set_task_thread(self, *_args, **_kwargs):
            raise StoreError("thread binding persistence failed")

        async def complete_task(self, *_args, **_kwargs):
            self.completed = True

    async def scenario() -> None:
        store = Store()
        worker = TaskWorker(
            store,
            runtime=type("Runtime", (), {"agent_id": "codex"})(),
            worker_id="binding-failure-worker",
        )
        task = AgentTask(
            task_id="binding-failure-task",
            conversation_id="binding-failure-conversation",
        )
        result = AgentResult(
            task_id=task.task_id,
            status="completed",
            thread_id="provider-thread",
        )
        with pytest.raises(StoreError, match="thread binding persistence failed"):
            await worker._finish(task, result, "claim-token")
        assert store.completed is False

    asyncio.run(scenario())


def test_worker_projects_completed_items_before_terminal_without_duplication(tmp_path):
    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.emitted = asyncio.Event()
            self.release = asyncio.Event()

        async def run(self, task, emit):
            event = AgentEvent.text_event(
                task.task_id,
                "durable final response",
                execution_id=task.execution_id,
                event_type="agent_message",
                source_item_id="item-1",
                source_item_type="agentmessage",
                source_item_ordinal=0,
            )
            await emit(event)
            self.emitted.set()
            await self.release.wait()
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                content=event.content,
                events=(event,),
            )

        async def interrupt(self, _task_id: str) -> bool:
            self.release.set()
            return True

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = Runtime()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=runtime, worker_id="worker")
            running = asyncio.create_task(worker.run_once())
            await runtime.emitted.wait()

            # A completed output item is the reply boundary.  Its event and
            # outbox projection commit together even while the turn continues.
            assert len(await store.list_task_events(task.task_id)) == 1
            running_outbox = [
                item for item in await store.list_outbox() if item.task_id == task.task_id
            ]
            assert [item.content for item in running_outbox] == [
                "durable final response"
            ]
            assert (await store.get_task(task.task_id)).state.value == "running"

            runtime.release.set()
            assert await running is True
            assert (await store.get_task(task.task_id)).state.value == "completed"
            outbox = [
                item for item in await store.list_outbox() if item.task_id == task.task_id
            ]
            # The aggregate AgentResult repeats the completed item and must
            # resolve to the same projection rather than creating a terminal
            # duplicate.
            assert [item.content for item in outbox] == ["durable final response"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_projects_completed_generated_image_without_empty_text(tmp_path):
    class Runtime:
        agent_id = "codex"

        async def run(self, task, emit):
            event = AgentEvent(
                task_id=task.task_id,
                execution_id=task.execution_id,
                event_type="image_generation",
                content="",
                attachments=("generated-image",),
                source_item_id="image-item-1",
                source_item_type="imagegeneration",
                source_item_ordinal=0,
            )
            await emit(event)
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="completed",
                events=(event,),
            )

        async def interrupt(self, _task_id: str) -> bool:
            return True

    async def scenario() -> None:
        attachment_root = tmp_path / "attachments"
        files = AttachmentStore(attachment_root)
        stored = files.put_bytes(
            b"\x89PNG\r\n\x1a\ngenerated",
            filename="generated.png",
            attachment_id="generated-image",
        )
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            attachment_root=attachment_root,
        )
        try:
            await store.register_attachment(
                stored,
                kind="image",
                metadata={
                    "filename": stored.filename,
                    "owner_agent_id": "codex",
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                },
            )
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")

            assert await worker.run_once() is True
            assert (await store.get_task(task.task_id)).state.value == "completed"
            events = [
                event
                for event in await store.list_task_events(task.task_id)
                if event.event_type == "image_generation"
            ]
            assert len(events) == 1
            assert events[0].attachments == (stored.attachment_id,)
            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(outbox) == 1
            assert outbox[0].content == ""
            assert outbox[0].attachments == (stored.attachment_id,)
            media = await store.list_outgoing_media(limit=10)
            assert len(media) == 1
            assert media[0].outbox_id == outbox[0].outbox_id
            assert media[0].attachment_id == stored.attachment_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_only_projects_stable_completed_agent_items_immediately():
    class Store:
        def __init__(self) -> None:
            self.defer_values: list[bool] = []

        async def append_task_event(self, _task_id, **kwargs):
            self.defer_values.append(bool(kwargs["defer_user_projection"]))

    class Runtime:
        agent_id = "codex"

    async def scenario() -> None:
        store = Store()
        worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")
        events = (
            AgentEvent.text_event(
                "task-1",
                "completed answer",
                event_type="message",
                source_item_id="item-1",
                source_item_type="agentMessage",
                source_item_ordinal=0,
            ),
            AgentEvent.text_event(
                "task-1",
                "tool diagnostics",
                event_type="message",
                source_item_id="tool-1",
                source_item_type="commandExecution",
                source_item_ordinal=1,
            ),
            AgentEvent.text_event(
                "task-1",
                "partial delta",
                event_type="message_delta",
                source_item_id="item-2",
                source_item_type="agentMessage",
                source_item_ordinal=2,
            ),
            AgentEvent.text_event(
                "task-1",
                "   ",
                event_type="agent_message",
                source_item_id="item-3",
                source_item_type="agentMessage",
                source_item_ordinal=3,
            ),
            AgentEvent.text_event(
                "task-1",
                "identity missing",
                event_type="agent_message",
                source_item_type="agentMessage",
            ),
        )
        for event in events:
            await worker._persist_event(event, claim_token="claim-1")

        # False means immediate projection. Every other event remains deferred
        # until the terminal compatibility projection/filtering boundary.
        assert store.defer_values == [False, True, True, True, True]

    asyncio.run(scenario())


def test_terminal_completion_replaces_message_delta_with_one_final_reply(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            delta = AgentEvent.text_event(
                task.task_id,
                "unfinished partial",
                execution_id=claim.execution_id,
                event_type="message_delta",
                source_item_id="message-1",
                source_item_type="agentMessage",
                source_item_ordinal=0,
            )

            await store.complete_task(
                task.task_id,
                result=AgentResult(
                    task_id=task.task_id,
                    execution_id=claim.execution_id,
                    content="complete aggregate",
                    events=(delta,),
                ),
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.content for item in outbox] == ["complete aggregate"]
            assert all(item.event_id != delta.event_id for item in outbox)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_terminal_completion_prefers_stable_items_over_aggregate_text(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            items = tuple(
                AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=ordinal,
                    execution_id=claim.execution_id,
                    event_type="agent_message",
                    source_item_id=f"message-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                )
                for ordinal, content in enumerate(("first", "second"))
            )

            await store.complete_task(
                task.task_id,
                result=AgentResult(
                    task_id=task.task_id,
                    execution_id=claim.execution_id,
                    # This aggregate is deliberately formatted differently
                    # from either completed item and their plain concatenation.
                    content="first\n\nsecond",
                    events=items,
                ),
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.content for item in outbox] == ["first", "second"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_terminal_completion_collapses_source_less_messages_into_aggregate(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            progress = tuple(
                AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=ordinal,
                    execution_id=claim.execution_id,
                    event_type="message",
                )
                for ordinal, content in enumerate(("working", "still working"))
            )

            await store.complete_task(
                task.task_id,
                result=AgentResult(
                    task_id=task.task_id,
                    execution_id=claim.execution_id,
                    content="one terminal aggregate",
                    events=progress,
                ),
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.content for item in outbox] == [
                "one terminal aggregate"
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_source_less_reasoning_and_status_events_are_not_replies(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            diagnostics = tuple(
                AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=ordinal,
                    execution_id=claim.execution_id,
                    event_type=event_type,
                )
                for ordinal, (event_type, content) in enumerate(
                    (
                        ("reasoning", "private reasoning text"),
                        ("status", "progress status text"),
                    )
                )
            )
            for event in diagnostics:
                await store.append_task_event(
                    task.task_id,
                    event,
                    claim_token=claim.claim_token,
                )
            assert await store.list_outbox() == []

            await store.complete_task(
                task.task_id,
                result=AgentResult(
                    task_id=task.task_id,
                    execution_id=claim.execution_id,
                    content="actual final answer",
                    events=diagnostics,
                ),
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.content for item in outbox] == ["actual final answer"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_terminal_replays_require_the_original_execution_claim(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            event = {
                "event_id": "terminal-replay-event",
                "idempotency_key": "terminal-replay-event",
                "execution_id": claim.execution_id,
                "event_type": "final",
                "content": "done",
            }
            await store.complete_task(
                task.task_id,
                status="completed",
                events=[event],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            with pytest.raises(StoreError, match="execution claim token"):
                await store.complete_task(
                    task.task_id,
                    status="completed",
                    events=[event],
                    claim_token="stale-claim-token",
                    execution_id=claim.execution_id,
                )
            with pytest.raises(StoreError, match="execution claim token"):
                await store.append_task_event(
                    task.task_id,
                    event,
                    claim_token="stale-claim-token",
                )

            # The retained execution token is sufficient for an exact retry,
            # even though terminalization cleared the task-row claim token.
            replay = await store.complete_task(
                task.task_id,
                status="completed",
                events=[event],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert replay.state.value == "completed"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_does_not_interrupt_after_runtime_returned_during_terminal_commit():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.terminal = False
            self.renewed_after_terminal = asyncio.Event()

        async def claim_next_task(self, **_kwargs):
            if self.claimed:
                return None
            self.claimed = True
            return (
                {
                    **_task(),
                    "task_id": "task-1",
                },
                "claim-token",
            )

        async def mark_task_running(self, *_args, **_kwargs) -> bool:
            return True

        async def list_task_events(self, *_args, **_kwargs):
            return []

        async def get_task(self, _task_id: str):
            return {"state": "completed" if self.terminal else "running"}

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            if not self.terminal:
                return True
            self.renewed_after_terminal.set()
            return False

        async def complete_task(self, *_args, **_kwargs) -> bool:
            # Simulate SQLite having committed the terminal row while its
            # executor callback has not resumed the worker coroutine yet.
            self.terminal = True
            await asyncio.wait_for(self.renewed_after_terminal.wait(), timeout=1)
            return True

    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.interrupt_calls = 0

        async def run(self, task, _emit):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
            )

        async def interrupt(self, _task_id: str) -> bool:
            self.interrupt_calls += 1
            return True

    async def scenario() -> None:
        store = Store()
        runtime = Runtime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="worker",
            lease_seconds=0.05,
        )

        assert await asyncio.wait_for(worker.run_once(), timeout=1) is True
        assert runtime.interrupt_calls == 0
        assert worker._lost_task_claims == set()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("result_task_id", "result_execution_id", "message"),
    (
        ("another-task", None, "task_id"),
        (None, "another-execution", "execution_id"),
    ),
)
def test_worker_fails_a_runtime_result_with_mismatched_identity(
    tmp_path,
    result_task_id: str | None,
    result_execution_id: str | None,
    message: str,
):
    class Runtime:
        agent_id = "codex"

        async def run(self, task, _emit):
            return AgentResult(
                task_id=result_task_id or task.task_id,
                execution_id=result_execution_id or task.execution_id,
                content="must not be delivered",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / f"{message}.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")

            assert await worker.run_once() is True
            failed = await store.get_task(task.task_id)
            assert failed is not None
            assert failed.state.value == "failed"
            assert message in (failed.last_error or "")
            assert "must not be delivered" not in str(failed.result or "")
            assert [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_projects_one_sanitized_notice_for_runtime_failure(tmp_path):
    raw_error = (
        "stream disconnected before completion: error sending request for url "
        "(http://provider-secret.example/v1/responses)"
    )

    class Runtime:
        agent_id = "codex"

        async def run(self, task, _emit):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                error=raw_error,
                thread_id="provider-thread",
                metadata={"diagnostics": {"phase": "terminal"}},
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime-failure-notice.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")

            assert await worker.run_once() is True
            failed = await store.get_task(task.task_id)
            assert failed is not None
            assert failed.state.value == "failed"
            assert failed.last_error == raw_error

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(outbox) == 1
            notice = outbox[0].content
            assert task.task_id in notice
            assert "/tasks" in notice
            assert "/retry reuses the same context" in notice
            assert "/clear starts fresh" in notice
            assert "provider-secret" not in notice
            assert "/v1/responses" not in notice

            # A second worker pass observes the terminal row and cannot create
            # another event, aggregate, or outbox delivery.
            assert await worker.run_once() is False
            replayed = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.outbox_id for item in replayed] == [outbox[0].outbox_id]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_stream_failure_keeps_completed_item_and_adds_one_terminal_notice(
    tmp_path,
):
    raw_error = (
        "stream disconnected after completion from "
        "provider-secret.example:8317/v1/responses?token=TOPSECRET"
    )

    class Runtime:
        agent_id = "codex"

        async def run(self, task, emit):
            event = AgentEvent.text_event(
                task.task_id,
                "stable answer before disconnect",
                execution_id=task.execution_id,
                event_type="agent_message",
                source_item_type="agentmessage",
                source_item_ordinal=0,
            )
            await emit(event)
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content=event.content,
                error=raw_error,
                events=(event,),
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "stable-before-disconnect.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")

            assert await worker.run_once() is True
            failed = await store.get_task(task.task_id)
            assert failed is not None
            assert failed.state.value == "failed"
            assert failed.last_error == raw_error

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert outbox[0].content == "stable answer before disconnect"
            notices = [
                item for item in outbox if item.content.startswith("task failed:")
            ]
            assert len(notices) == 1
            assert all("provider-secret" not in item.content for item in outbox)
            assert all("TOPSECRET" not in item.content for item in outbox)

            events = await store.list_task_events(task.task_id)
            assert [event.event_type for event in events] == [
                "agent_message",
                "failure_notice",
                "terminal",
            ]
            assert await worker.run_once() is False
            assert [
                item.outbox_id
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == [item.outbox_id for item in outbox]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_timeout_after_many_progress_items_adds_one_terminal_failure_notice(
    tmp_path,
):
    progress = tuple(
        f"progress checkpoint {ordinal}"
        for ordinal in (2, 18, 38, 82, 93, 106, 118)
    )

    class Runtime:
        agent_id = "codex"

        async def run(self, task, emit):
            events = []
            for sequence, (ordinal, content) in enumerate(
                zip((2, 18, 38, 82, 93, 106, 118), progress)
            ):
                event = AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=sequence,
                    execution_id=task.execution_id,
                    event_type="agent_message",
                    source_item_id=f"progress-item-{ordinal}",
                    source_item_type="agentmessage",
                    source_item_ordinal=ordinal,
                )
                await emit(event)
                events.append(event)
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content="".join(progress),
                error="Codex turn timed out at https://provider-secret.example/v1",
                events=tuple(events),
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "progress-timeout.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")
            assert await worker.run_once() is True

            failed = await store.get_task(task.task_id)
            assert failed is not None
            assert failed.state.value == "failed"
            events = await store.list_task_events(task.task_id)
            assert [item.event_type for item in events] == [
                *("agent_message" for _ in progress),
                "failure_notice",
                "terminal",
            ]

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            wire_text = "\n".join(item.content for item in outbox)
            for content in progress:
                assert wire_text.count(content) == 1
            assert wire_text.count(f"task failed: {task.task_id}") == 1
            assert wire_text.count("/retry reuses the same context") == 1
            assert "provider-secret" not in wire_text
            assert "/v1" not in wire_text

            assert await worker.run_once() is False
            assert [
                item.outbox_id
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == [item.outbox_id for item in outbox]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_failure_notice_bypasses_saturated_progress_quota_and_remains_last(
    tmp_path,
):
    recv_suffix = "\n\nReply /recv to continue."
    progress_values = [
        "progress 1:" + "A" * (6_000 - len("progress 1:")),
        *(
            f"progress {ordinal}:"
            + marker * (3_000 - len(f"progress {ordinal}:"))
            for ordinal, marker in zip(range(2, 9), "BCDEFGH")
        ),
    ]
    aggregate_ten_source_size = 3_000
    ordinal_ten_body_limit = aggregate_ten_source_size - len(recv_suffix)
    ninth_size = aggregate_ten_source_size // 2
    progress_values.extend(
        (
            "progress 9:" + "I" * (ninth_size - len("progress 9:")),
            "progress 10:"
            + "J"
            * (
                aggregate_ten_source_size
                - ninth_size
                - 2
                - len("progress 10:")
            ),
        )
    )
    progress = tuple(progress_values)

    class Runtime:
        agent_id = "codex"

        async def run(self, task, emit):
            events = []
            for ordinal, content in enumerate(progress):
                event = AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=ordinal,
                    execution_id=task.execution_id,
                    event_type="agent_message",
                    source_item_id=f"quota-progress-{ordinal}",
                    source_item_type="agentmessage",
                    source_item_ordinal=ordinal,
                )
                await emit(event)
                events.append(event)
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content="".join(progress),
                error="Codex turn timed out at https://provider-secret.example/v1",
                events=tuple(events),
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        path = tmp_path / "saturated-failure-notice.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            accepted = await first.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="saturated-failure-source",
                    session_id="default",
                    text="run a long task",
                    context_token="saturated-context",
                ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            task = accepted.task
            assert await TaskWorker(
                first,
                runtime=Runtime(),
                worker_id="saturated-worker",
            ).run_once() is True

            scope = await first.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            assert scope.used_slots == scope.capacity == 10
            immediate = sorted(
                (
                    item
                    for item in await first.list_outbox(limit=100)
                    if item.task_id == task.task_id
                ),
                key=lambda item: (item.created_at, item.outbox_id),
            )
            assert len(immediate) == 11
            assert [item.reply_ordinal for item in immediate[:10]] == list(
                range(1, 11)
            )
            assert immediate[0].content + immediate[1].content == progress[0]
            assert [item.content for item in immediate[2:9]] == list(
                progress[1:8]
            )
            assert immediate[9].content.endswith(recv_suffix)
            assert immediate[9].content[: -len(recv_suffix)] == (
                progress[8] + "\n\n" + progress[9]
            )[:ordinal_ten_body_limit]
            progress_spillover = (progress[8] + "\n\n" + progress[9])[
                ordinal_ten_body_limit:
            ]
            assert len(progress_spillover) == len(recv_suffix)
            notice = immediate[-1]
            assert notice.content.startswith(f"task failed: {task.task_id}\n")
            assert notice.content.count(f"task failed: {task.task_id}") == 1
            assert notice.active_wire_variant == "contextless"

            assert notice.reply_scope_id == scope.reply_scope_id
            assert notice.reply_slot_id is None
            assert notice.reply_ordinal == 10
            assert notice.reply_candidate_id is not None
            assert notice.reply_fragment_id is not None
            assert notice.reply_aggregate_id is not None
            assert notice.contextless_client_id
            assert notice.contextless_client_id != notice.client_id
            assert notice.wire_client_id == notice.contextless_client_id
            assert notice.channel == "wechat"
            assert notice.bot_id == "bot"
            assert notice.external_user_id == "user"
            assert notice.session_id == "default"
            assert notice.agent_id == "codex"
            assert notice.foreground is True
            assert notice.notify_enabled is True
            assert notice.from_user_id == "bot"
            assert notice.reply_target == accepted.inbound.target()

            aggregate = await first.get_reply_aggregate(
                notice.reply_aggregate_id
            )
            assert aggregate is not None
            assert aggregate.state.value == "sealed"
            assert aggregate.wire_reply_fragment_id == notice.reply_fragment_id
            assert (
                aggregate.representative_reply_candidate_id
                == notice.reply_candidate_id
            )

            hostile = await first.project_reply_candidate(
                target=accepted.inbound.target(),
                reply_scope_id=scope.reply_scope_id,
                source_key="hostile:failure-notice",
                content="hostile failure_notice token=TOPSECRET",
                source_item_id="hostile-failure-notice",
                source_item_type="agentMessage",
                source_item_ordinal=999,
                foreground=True,
            )
            assert len(hostile.fragments) == 1
            assert hostile.fragments[0].state is ReplyFragmentState.DEFERRED_QUOTA
            assert hostile.outbox_items == ()

            with sqlite3.connect(path) as connection:
                with pytest.raises(
                    sqlite3.IntegrityError,
                    match="terminal failure safety outbox is immutable",
                ):
                    connection.execute(
                        "UPDATE user_outbox SET content='mutated' "
                        "WHERE outbox_id=?",
                        (notice.outbox_id,),
                    )
        finally:
            await first.close()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            await restarted.startup_reconcile()
            durable = sorted(
                (
                    item
                    for item in await restarted.list_outbox(limit=100)
                    if item.task_id == task.task_id
                ),
                key=lambda item: (item.created_at, item.outbox_id),
            )
            assert [item.outbox_id for item in durable] == [
                item.outbox_id for item in immediate
            ]

            delivered = []
            while claimed := await restarted.claim_outbox(
                "ordered-delivery-worker",
                limit=20,
            ):
                assert len(claimed) == 1
                item = claimed[0]
                delivered.append(item)
                assert await restarted.mark_outbox_sending(
                    item.outbox_id,
                    item.claim_token,
                )
                assert await restarted.mark_outbox_sent(
                    item.outbox_id,
                    item.claim_token,
                    client_id=item.wire_client_id,
                )
            assert [item.outbox_id for item in delivered] == [
                item.outbox_id for item in durable
            ]
            assert delivered[-1].content == notice.content

            recv = await restarted.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="saturated-failure-recv",
                    session_id="default",
                    text="/recv",
                    context_token="recv-context",
                ),
                create_task=False,
            )
            drained = await restarted.drain_deferred_replies(
                target=recv.inbound.target(),
                source_key="command:/recv",
            )
            assert [item.content for item in drained.outbox_items] == [
                progress_spillover,
                "hostile failure_notice token=TOPSECRET",
            ]
            assert all(
                item.reply_fragment_id != notice.reply_fragment_id
                for item in drained.outbox_items
            )
        finally:
            await restarted.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("boundary", "recovered_state", "claimable_after_restart"),
    (
        ("pending", "pending", True),
        ("sending", "delivery_unknown", False),
        ("sent", "sent", False),
    ),
)
def test_saturated_failure_notice_restart_boundaries_are_exactly_once(
    tmp_path,
    boundary: str,
    recovered_state: str,
    claimable_after_restart: bool,
):
    class Runtime:
        agent_id = "codex"

        async def run(self, task, _emit):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                error="private provider failure",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        path = tmp_path / f"saturated-failure-{boundary}.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            accepted = await first.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id=f"saturated-boundary-{boundary}",
                    session_id="default",
                    text="run",
                    context_token=f"context-{boundary}",
                ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            task = accepted.task
            scope = await first.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            for ordinal in range(1, 11):
                projection = await first.project_reply_candidate(
                    target=accepted.inbound.target(),
                    reply_scope_id=scope.reply_scope_id,
                    source_key=f"boundary-progress:{ordinal}",
                    content=f"boundary progress {ordinal}",
                    source_item_id=f"boundary-progress-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                    foreground=True,
                )
                assert projection.slots[0].reply_ordinal == ordinal

            assert await TaskWorker(
                first,
                runtime=Runtime(),
                worker_id=f"boundary-worker-{boundary}",
            ).run_once() is True
            task_outbox = [
                item
                for item in await first.list_outbox(limit=100)
                if item.task_id == task.task_id
            ]
            assert len(task_outbox) == 1
            notice = task_outbox[0]
            assert notice.active_wire_variant == "contextless"
            original_identity = (
                notice.outbox_id,
                notice.client_id,
                notice.contextless_client_id,
                notice.reply_candidate_id,
                notice.reply_fragment_id,
                notice.reply_aggregate_id,
            )

            for ordinal in range(1, 11):
                claimed = await first.claim_outbox(
                    f"progress-delivery-{boundary}",
                    limit=20,
                )
                assert len(claimed) == 1
                assert claimed[0].outbox_id != notice.outbox_id
                assert claimed[0].reply_ordinal == ordinal
                assert await first.mark_outbox_sending(
                    claimed[0].outbox_id,
                    claimed[0].claim_token,
                )
                assert await first.mark_outbox_sent(
                    claimed[0].outbox_id,
                    claimed[0].claim_token,
                    client_id=claimed[0].wire_client_id,
                )

            if boundary in {"sending", "sent"}:
                notice_claim = await first.claim_outbox(
                    f"notice-delivery-{boundary}",
                    limit=20,
                )
                assert [item.outbox_id for item in notice_claim] == [
                    notice.outbox_id
                ]
                assert await first.mark_outbox_sending(
                    notice.outbox_id,
                    notice_claim[0].claim_token,
                )
                if boundary == "sent":
                    assert await first.mark_outbox_sent(
                        notice.outbox_id,
                        notice_claim[0].claim_token,
                        client_id=notice.wire_client_id,
                    )
        finally:
            await first.close()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            await restarted.startup_reconcile()
            recovered = await restarted.get_outbox_item(notice.outbox_id)
            assert recovered is not None
            assert recovered.state.value == recovered_state
            assert (
                recovered.outbox_id,
                recovered.client_id,
                recovered.contextless_client_id,
                recovered.reply_candidate_id,
                recovered.reply_fragment_id,
                recovered.reply_aggregate_id,
            ) == original_identity
            assert recovered.active_wire_variant == "contextless"

            claims = await restarted.claim_outbox(
                f"restart-delivery-{boundary}",
                limit=20,
            )
            if claimable_after_restart:
                assert [item.outbox_id for item in claims] == [notice.outbox_id]
                assert await restarted.mark_outbox_sending(
                    notice.outbox_id,
                    claims[0].claim_token,
                )
                assert await restarted.mark_outbox_sent(
                    notice.outbox_id,
                    claims[0].claim_token,
                    client_id=recovered.wire_client_id,
                )
                assert await restarted.claim_outbox(
                    f"second-restart-delivery-{boundary}",
                    limit=20,
                ) == []
            else:
                assert claims == []
        finally:
            await restarted.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("notice_as_event", (False, True))
def test_intended_public_failure_notice_is_delivered_once(
    tmp_path,
    notice_as_event: bool,
):
    class Runtime:
        agent_id = "codex"

        async def run(self, task, _emit):
            notice = (
                f"task failed: {task.task_id}\n"
                "check /tasks before retrying. /retry reuses the same context; "
                "/clear starts fresh."
            )
            events = (
                (
                    AgentEvent.text_event(
                        task.task_id,
                        notice,
                        execution_id=task.execution_id,
                        event_type="failure_notice",
                    ),
                )
                if notice_as_event
                else ()
            )
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content=notice,
                error="private provider diagnostic",
                events=events,
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / f"intended-notice-{int(notice_as_event)}.sqlite"
        )
        await store.initialize()
        try:
            task = await store.create_task(_task())
            assert await TaskWorker(
                store,
                runtime=Runtime(),
                worker_id="worker",
            ).run_once() is True

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(outbox) == 1
            assert outbox[0].content.count(f"task failed: {task.task_id}") == 1
            events = await store.list_task_events(task.task_id)
            assert [item.event_type for item in events].count("failure_notice") == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_hostile_runtime_failure_events_cannot_replace_sanitized_notice(
    tmp_path,
):
    hostile_events = (
        ("error", "request failed at https://provider-secret.example/v1"),
        ("failed", "Authorization: Bearer private-token"),
        ("failure_notice", "token=TOPSECRET host=provider-secret.example"),
    )

    class Runtime:
        agent_id = "codex"

        async def run(self, task, emit):
            events = []
            for sequence, (event_type, content) in enumerate(hostile_events):
                event = AgentEvent.text_event(
                    task.task_id,
                    content,
                    sequence=sequence,
                    execution_id=task.execution_id,
                    event_type=event_type,
                )
                await emit(event)
                events.append(event)
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content="".join(content for _kind, content in hostile_events),
                error="provider transport failed",
                events=tuple(events),
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "hostile-failure-events.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            assert await TaskWorker(
                store,
                runtime=Runtime(),
                worker_id="worker",
            ).run_once() is True

            outbox = [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(outbox) == 1
            assert outbox[0].content.startswith(f"task failed: {task.task_id}\n")
            assert "provider-secret" not in outbox[0].content
            assert "private-token" not in outbox[0].content
            assert "TOPSECRET" not in outbox[0].content
            events = await store.list_task_events(task.task_id)
            assert [item.event_type for item in events] == [
                "error",
                "failed",
                "failure_notice",
                "failure_notice",
                "terminal",
            ]
            assert sum(
                item.content.startswith(f"task failed: {task.task_id}\n")
                for item in events
            ) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_failure_notice_event_type_lookalike_cannot_break_saturated_delivery(
    tmp_path,
):
    class Runtime:
        agent_id = "codex"

        async def run(self, task, _emit):
            notice = (
                f"task failed: {task.task_id}\n"
                "check /tasks before retrying or sending a new prompt."
            )
            lookalike = AgentEvent.text_event(
                task.task_id,
                notice,
                execution_id=task.execution_id,
                event_type="failure-notice",
            )
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                content=notice,
                error="private provider diagnostic",
                events=(lookalike,),
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "failure-notice-type-lookalike.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                InboundMessage(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    external_message_id="failure-notice-type-lookalike",
                    session_id="default",
                    text="run",
                    context_token="lookalike-context",
                ),
                task={"agent_id": "codex"},
            )
            assert accepted.task is not None
            task = accepted.task
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            for ordinal in range(1, 11):
                projection = await store.project_reply_candidate(
                    target=accepted.inbound.target(),
                    reply_scope_id=scope.reply_scope_id,
                    source_key=f"lookalike-prefill:{ordinal}",
                    content=f"prefill {ordinal}",
                    source_item_id=f"lookalike-prefill-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                    foreground=True,
                )
                assert projection.slots[0].reply_ordinal == ordinal

            assert await TaskWorker(
                store,
                runtime=Runtime(),
                worker_id="lookalike-worker",
            ).run_once() is True

            failed = await store.get_task(task.task_id)
            assert failed is not None
            assert failed.state.value == "failed"
            task_outbox = [
                item
                for item in await store.list_outbox(limit=100)
                if item.task_id == task.task_id
            ]
            assert len(task_outbox) == 1
            assert task_outbox[0].active_wire_variant == "contextless"
            assert task_outbox[0].content == (
                f"task failed: {task.task_id}\n"
                "check /tasks before retrying or sending a new prompt."
            )
            events = await store.list_task_events(task.task_id)
            assert [item.event_type for item in events].count("failure_notice") == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_failure_notice_uses_durable_task_thread_when_result_omits_it():
    task = AgentTask(
        task_id="thread-bound-failure",
        thread_id="durable-provider-thread",
    )
    result = AgentResult(
        task_id=task.task_id,
        status="failed",
        error="sanitized internally",
    )

    noticed = TaskWorker._with_failure_notice(task, result)

    assert "/retry reuses the same context" in noticed.content
    assert "/clear starts fresh" in noticed.content


def test_failure_notice_restart_recovery_never_resends_a_sent_row(tmp_path):
    class FailingRuntime:
        agent_id = "codex"

        async def run(self, task, _emit):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                error="provider-secret.example:8317/v1 token=TOPSECRET",
                thread_id="provider-thread",
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    class NeverRuntime:
        agent_id = "codex"

        async def run(self, _task, _emit):
            raise AssertionError("terminal task must not execute after restart")

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        path = tmp_path / "failure-notice-restart.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            task = await first.create_task(_task())
            worker = TaskWorker(
                first,
                runtime=FailingRuntime(),
                worker_id="first-worker",
            )
            assert await worker.run_once() is True
            initial = [
                item
                for item in await first.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(initial) == 1
            assert initial[0].state.value == "pending"
            notice_id = initial[0].outbox_id
        finally:
            await first.close()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            await restarted.startup_reconcile()
            worker = TaskWorker(
                restarted,
                runtime=NeverRuntime(),
                worker_id="restart-worker",
            )
            assert await worker.run_once() is False
            pending = [
                item
                for item in await restarted.list_outbox()
                if item.task_id == task.task_id
            ]
            assert [item.outbox_id for item in pending] == [notice_id]
            claims = await restarted.claim_outbox(
                "delivery-worker",
                automatic=False,
            )
            claim = next(item for item in claims if item.outbox_id == notice_id)
            assert await restarted.mark_outbox_sending(
                notice_id,
                claim_token=claim.claim_token,
            )
            assert await restarted.mark_outbox_sent(
                notice_id,
                claim_token=claim.claim_token,
            )
        finally:
            await restarted.close()

        after_send = SQLiteStore(path)
        await after_send.initialize()
        try:
            await after_send.startup_reconcile()
            persisted = [
                item
                for item in await after_send.list_outbox()
                if item.task_id == task.task_id
            ]
            assert len(persisted) == 1
            assert persisted[0].outbox_id == notice_id
            assert persisted[0].state.value == "sent"
            assert await after_send.claim_outbox(
                "another-delivery-worker",
                automatic=False,
            ) == []
            assert await TaskWorker(
                after_send,
                runtime=NeverRuntime(),
                worker_id="final-worker",
            ).run_once() is False
        finally:
            await after_send.close()

    asyncio.run(scenario())


def test_execution_uncertain_runtime_loss_is_orphaned_without_delivery(tmp_path):
    class ExecutionUncertainError(RuntimeError):
        execution_uncertain = True

    class Runtime:
        agent_id = "codex"

        async def run(self, _task, _emit):
            raise ExecutionUncertainError(
                "child lost after assignment http://provider-secret.example/v1"
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "uncertain-runtime-loss.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker")

            assert await worker.run_once() is True
            orphaned = await store.get_task(task.task_id)
            assert orphaned is not None
            assert orphaned.state.value == "orphaned"

            executions = await store.list_task_executions(task.task_id)
            assert len(executions) == 1
            assert executions[0].state.value == "orphaned"
            assert [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == []
            events = await store.list_task_events(task.task_id)
            assert [(item.event_type, item.visibility.value) for item in events] == [
                ("terminal", "internal")
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_shutdown_orphans_a_turn_that_ignores_interrupt(tmp_path):
    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()
            self.interrupt_calls = 0

        async def run(self, _task, _emit):
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

        async def interrupt(self, _task_id: str) -> bool:
            self.interrupt_calls += 1
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "shutdown.sqlite")
        await store.initialize()
        runtime = Runtime()
        try:
            created = await store.create_task(_task())
            worker = TaskWorker(
                store,
                runtime=runtime,
                worker_id="worker",
                stop_timeout=0.02,
            )
            await worker.start()
            await asyncio.wait_for(runtime.started.wait(), timeout=1)

            await asyncio.wait_for(worker.stop(), timeout=1)

            stopped = await store.get_task(created.task_id)
            assert stopped is not None
            assert stopped.state.value == "orphaned"
            assert "shutdown timed out" in (stopped.last_error or "")
            execution = await store.get_execution(stopped.execution_id or "")
            assert execution is not None
            assert execution.state.value == "orphaned"
            assert execution.finished_at is not None
            assert runtime.interrupt_calls >= 1
            assert runtime.cancelled.is_set()
            assert await store.claim_next_task("another-worker") is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_worker_shutdown_is_bounded_when_runtime_swallows_cancellation(tmp_path):
    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()
            self.release = asyncio.Event()

        async def run(self, task, _emit):
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                # Compatibility runtimes are not guaranteed to propagate
                # cancellation. Keep the coroutine alive until test cleanup.
                await self.release.wait()
                return AgentResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    content="stale result",
                )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "swallowed-cancel.sqlite")
        await store.initialize()
        runtime = Runtime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="worker",
            stop_timeout=0.02,
        )
        try:
            created = await store.create_task(_task())
            await worker.start()
            await asyncio.wait_for(runtime.started.wait(), timeout=1)

            await asyncio.wait_for(worker.stop(), timeout=0.25)
            assert runtime.cancelled.is_set()
            stopped = await store.get_task(created.task_id)
            assert stopped is not None
            assert stopped.state.value == "orphaned"

            runtime.release.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            stopped = await store.get_task(created.task_id)
            assert stopped is not None
            assert stopped.state.value == "orphaned"
        finally:
            runtime.release.set()
            await store.close()

    asyncio.run(scenario())


def test_worker_never_starts_runtime_after_durable_prestart_cancel():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.state = "claimed"
            self.finished_status = ""

        async def claim_next_task(self, **_kwargs):
            if self.claimed:
                return None
            self.claimed = True
            return ({**_task(), "task_id": "task-prestart-cancel"}, "claim-token")

        async def mark_task_running(self, *_args, **_kwargs) -> bool:
            self.state = "running"
            return True

        async def list_task_events(self, *_args, **_kwargs):
            # This is the scheduling point at which /cancel races with the
            # worker after the running transition but before runtime.run.
            self.state = "cancel_requested"
            return []

        async def get_task(self, _task_id: str):
            return {"state": self.state, "claim_token": "claim-token"}

        async def complete_task(self, *_args, status: str, **_kwargs) -> bool:
            self.finished_status = status
            self.state = status
            return True

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            return True

    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.run_calls = 0

        async def run(self, *_args, **_kwargs):
            self.run_calls += 1
            raise AssertionError("runtime started after durable cancellation")

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = Store()
        runtime = Runtime()
        worker = TaskWorker(store, runtime=runtime, worker_id="worker")

        assert await worker.run_once() is True
        assert runtime.run_calls == 0
        assert store.finished_status == "cancelled"

    asyncio.run(scenario())


def test_worker_never_starts_runtime_when_lease_is_lost_during_final_state_read():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.final_read_started = asyncio.Event()
            self.release_final_read = asyncio.Event()
            self.finished = False

        async def claim_next_task(self, **_kwargs):
            if self.claimed:
                return None
            self.claimed = True
            return ({**_task(), "task_id": "task-final-boundary"}, "claim-token")

        async def mark_task_running(self, *_args, **_kwargs) -> bool:
            return True

        async def list_task_events(self, *_args, **_kwargs):
            return []

        async def get_task(self, _task_id: str):
            self.final_read_started.set()
            await self.release_final_read.wait()
            return {"state": "running", "claim_token": "claim-token"}

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            await self.final_read_started.wait()
            return False

        async def complete_task(self, *_args, **_kwargs) -> bool:
            self.finished = True
            return True

    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.run_calls = 0
            self.interrupt_calls = 0

        async def run(self, task, _emit):
            self.run_calls += 1
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                content="must not run",
            )

        async def interrupt(self, _task_id: str) -> bool:
            self.interrupt_calls += 1
            return True

    async def scenario() -> None:
        store = Store()
        runtime = Runtime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="worker",
            lease_seconds=0.1,
        )

        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(store.final_read_started.wait(), timeout=1)
        for _ in range(100):
            if "task-final-boundary" in worker._lost_task_claims:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("heartbeat did not report ownership loss")
        store.release_final_read.set()

        assert await asyncio.wait_for(running, timeout=1) is True
        assert runtime.run_calls == 0
        assert runtime.interrupt_calls == 0
        assert store.finished is False

    asyncio.run(scenario())


def test_worker_never_starts_runtime_when_final_state_read_fails():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.finished = False

        async def claim_next_task(self, **_kwargs):
            if self.claimed:
                return None
            self.claimed = True
            return ({**_task(), "task_id": "task-final-read-error"}, "claim-token")

        async def mark_task_running(self, *_args, **_kwargs) -> bool:
            return True

        async def list_task_events(self, *_args, **_kwargs):
            return []

        async def get_task(self, _task_id: str):
            raise OSError("database unavailable")

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            return True

        async def complete_task(self, *_args, **_kwargs) -> bool:
            self.finished = True
            raise AssertionError("an unverified task was terminalized")

    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.run_calls = 0

        async def run(self, *_args, **_kwargs):
            self.run_calls += 1
            raise AssertionError("runtime started without a durable state read")

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = Store()
        runtime = Runtime()
        worker = TaskWorker(store, runtime=runtime, worker_id="worker")

        with pytest.raises(OSError, match="database unavailable"):
            await worker.run_once()
        assert runtime.run_calls == 0
        assert store.finished is False

    asyncio.run(scenario())


def test_task_heartbeat_does_not_attempt_to_revive_expired_claim(monkeypatch):
    class Store:
        renew_calls = 0

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            self.renew_calls += 1
            return True

    class Runtime:
        agent_id = "codex"

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = Store()
        worker = TaskWorker(store, runtime=Runtime(), lease_seconds=0.1)
        worker._active["expired"] = ({}, object())  # type: ignore[assignment]
        clock = [0.0]

        async def delayed_sleep(_delay: float) -> None:
            clock[0] = 2.0

        monkeypatch.setattr("src.runtime.worker.time.monotonic", lambda: clock[0])
        monkeypatch.setattr("src.runtime.worker.asyncio.sleep", delayed_sleep)
        heartbeat = worker._start_heartbeat(
            "expired", "claim-token", lease_deadline=1.0
        )
        assert heartbeat is not None
        await heartbeat

        assert store.renew_calls == 0
        assert "expired" in worker._lost_task_claims

    asyncio.run(scenario())


def test_mailbox_heartbeat_does_not_attempt_to_revive_expired_claim(monkeypatch):
    class Store:
        renew_calls = 0

        async def renew_mailbox_lease(self, *_args, **_kwargs) -> bool:
            self.renew_calls += 1
            return True

    async def scenario() -> None:
        store = Store()
        worker = AgentMailboxWorker(
            store,
            {},
            "planner",
            lease_seconds=0.1,
        )
        claim_lost = asyncio.Event()
        worker._mailbox_claim_loss_events["expired"] = claim_lost
        clock = [0.0]

        async def delayed_sleep(_delay: float) -> None:
            clock[0] = 2.0

        monkeypatch.setattr("src.runtime.worker.time.monotonic", lambda: clock[0])
        monkeypatch.setattr("src.runtime.worker.asyncio.sleep", delayed_sleep)
        heartbeat = worker._start_lease_heartbeat(
            "expired", "claim-token", lease_deadline=1.0
        )
        assert heartbeat is not None
        await heartbeat

        assert store.renew_calls == 0
        assert claim_lost.is_set()
        assert "expired" in worker._lost_mailbox_claims

    asyncio.run(scenario())


def test_task_worker_does_not_commit_after_renewal_errors_outlive_lease():
    class Store:
        def __init__(self) -> None:
            self.claimed = False
            self.finished = False

        async def claim_next_task(self, **_kwargs):
            if self.claimed:
                return None
            self.claimed = True
            return ({**_task(), "task_id": "task-lease-error"}, "claim-token")

        async def mark_task_running(self, *_args, **_kwargs) -> bool:
            return True

        async def list_task_events(self, *_args, **_kwargs):
            return []

        async def get_task(self, _task_id: str):
            return {"state": "running"}

        async def renew_task_lease(self, *_args, **_kwargs) -> bool:
            raise OSError("database unavailable")

        async def complete_task(self, *_args, **_kwargs) -> None:
            self.finished = True
            raise AssertionError("an unconfirmed task claim was terminalized")

    class Runtime:
        agent_id = "codex"

        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.interrupted = asyncio.Event()

        async def run(self, task, _emit):
            self.started.set()
            await self.release.wait()
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                content="stale result",
            )

        async def interrupt(self, _task_id: str) -> bool:
            self.interrupted.set()
            self.release.set()
            return True

    async def scenario() -> None:
        store = Store()
        runtime = Runtime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="worker",
            lease_seconds=0.1,
        )
        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(runtime.started.wait(), timeout=1)
        await asyncio.wait_for(runtime.interrupted.wait(), timeout=1)

        assert await asyncio.wait_for(running, timeout=1) is True
        assert store.finished is False
        assert worker._lost_task_claims == set()

    asyncio.run(scenario())
