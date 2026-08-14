"""High-risk transaction and worker durability regressions."""

from __future__ import annotations

import asyncio

import pytest

from src.agents.base import AgentEvent, AgentResult
from src.runtime.media import AttachmentStore
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
