from __future__ import annotations

import asyncio

import pytest

from src.runtime.models import AgentTask, ReplyTarget
from src.runtime.sqlite_store import QueueFullError, SQLiteStore


def _task(task_id: str, *, bot_id: str) -> AgentTask:
    return AgentTask(
        task_id=task_id,
        execution_id="",
        agent_id="codex",
        conversation_id=f"conversation-{task_id}",
        mode_id="chat",
        profile_version=1,
        policy_version=1,
        reply_target=ReplyTarget(
            channel="lark",
            bot_id=bot_id,
            external_user_id=f"ou_{bot_id}",
            session_id="default",
            destination_kind="direct",
            destination_id=f"oc_{bot_id}",
        ),
        inputs={"text": task_id},
    )


async def _complete_next(store: SQLiteStore) -> str:
    claim = await store.claim_next_task("fairness-worker")
    assert claim is not None
    assert await store.mark_task_running(
        claim.task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    completed = await store.complete_task(
        claim.task.task_id,
        result={"status": "completed"},
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )
    assert completed.state.value == "completed"
    return claim.task.task_id


def test_account_limits_are_atomic_and_report_the_exact_scope(tmp_path):
    async def account_limit() -> None:
        store = SQLiteStore(
            tmp_path / "account.sqlite",
            max_agent_queue=5,
            max_global_queue=10,
            max_account_queue=1,
            max_account_agent_queue=5,
        )
        await store.initialize()
        try:
            first = await store.create_task(_task("a-first", bot_id="bot-a"))
            with pytest.raises(QueueFullError) as rejected:
                await store.create_task(_task("a-rejected", bot_id="bot-a"))
            assert rejected.value.scope == "account"
            assert rejected.value.limit == 1
            assert await store.get_task("a-rejected") is None
            counter = await store.get_account_admission_counter("lark", "bot-a")
            assert counter is not None and counter.unfinished_count == 1
            assert await store.cancel_task(first.task_id)
            counter = await store.get_account_admission_counter("lark", "bot-a")
            assert counter is not None and counter.unfinished_count == 0
        finally:
            await store.close()

    async def stream_limit() -> None:
        store = SQLiteStore(
            tmp_path / "stream.sqlite",
            max_agent_queue=5,
            max_global_queue=10,
            max_account_queue=5,
            max_account_agent_queue=1,
        )
        await store.initialize()
        try:
            first = await store.create_task(_task("stream-first", bot_id="bot-a"))
            with pytest.raises(QueueFullError) as rejected:
                await store.create_task(
                    _task("stream-rejected", bot_id="bot-a")
                )
            assert rejected.value.scope == "account_agent"
            assert rejected.value.limit == 1
            invocation = await store.get_agent_invocation(first.execution_id)
            assert invocation is not None
            counter = await store.get_agent_account_admission_counter(
                invocation.agent_id,
                invocation.agent_incarnation,
                "lark",
                "bot-a",
            )
            assert counter is not None and counter.unfinished_count == 1
        finally:
            await store.close()

    asyncio.run(account_limit())
    asyncio.run(stream_limit())


def test_round_robin_is_durable_and_fifo_within_each_account(tmp_path):
    async def seed_and_serve_first(path) -> None:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            # A noisy account arrives first and has a deeper queue.  The first
            # turn remains the legacy oldest-ready choice; subsequent turns
            # rotate bot accounts without violating either stream's FIFO.
            for task_id in ("a-1", "a-2", "a-3"):
                await store.create_task(_task(task_id, bot_id="bot-a"))
            for task_id in ("b-1", "b-2"):
                await store.create_task(_task(task_id, bot_id="bot-b"))
            assert await _complete_next(store) == "a-1"
            counters = await store.list_agent_account_admission_counters(
                agent_id="codex"
            )
            by_bot = {counter.bot_id: counter for counter in counters}
            assert by_bot["bot-a"].last_served_ordinal > (
                by_bot["bot-b"].last_served_ordinal
            )
        finally:
            await store.close()

    async def finish_after_restart(path) -> None:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            order = [await _complete_next(store) for _ in range(4)]
            assert order == ["b-1", "a-2", "b-2", "a-3"]
            assert await store.claim_next_task("fairness-worker") is None
            accounts = await store.list_account_admission_counters(
                channel="lark"
            )
            assert {(item.bot_id, item.unfinished_count) for item in accounts} == {
                ("bot-a", 0),
                ("bot-b", 0),
            }
        finally:
            await store.close()

    path = tmp_path / "runtime.sqlite"
    asyncio.run(seed_and_serve_first(path))
    asyncio.run(finish_after_restart(path))
