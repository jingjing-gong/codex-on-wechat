from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from src.runtime.models import AgentResult, AgentTask, ReplyTarget
from src.runtime.agent_bridge import AgentBridgeServer
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import QueueFullError, SQLiteStore
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter, WeChatGateway
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def _task(task_id: str) -> AgentTask:
    return AgentTask(
        task_id=task_id,
        execution_id="",
        agent_id="codex",
        conversation_id=f"conversation-{task_id}",
        mode_id="chat",
        profile_version=1,
        policy_version=1,
        reply_target=ReplyTarget(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        ),
        inputs={"text": task_id},
    )


def _message(text: str, *, message_id: int = 901) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token="queue-context",
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


async def _counts(store: SQLiteStore) -> tuple[int, int, int, int]:
    return await store._call(
        lambda conn: (
            int(
                conn.execute(
                    "SELECT unfinished_count FROM agent_admission_counters "
                    "WHERE agent_id='codex'"
                ).fetchone()[0]
            ),
            int(
                conn.execute(
                    "SELECT next_ready_sequence FROM agent_admission_counters "
                    "WHERE agent_id='codex'"
                ).fetchone()[0]
            ),
            int(
                conn.execute(
                    "SELECT unfinished_count FROM global_agent_admission_counter "
                    "WHERE singleton=1"
                ).fetchone()[0]
            ),
            int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]),
        )
    )


def test_agent_admission_rejection_is_atomic_and_does_not_consume_sequence(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            max_agent_queue=1,
            max_global_queue=2,
        )
        await store.initialize()
        try:
            first = await store.create_task(_task("first"))
            assert await _counts(store) == (1, 2, 1, 1)

            with pytest.raises(QueueFullError) as rejected:
                await store.create_task(_task("rejected"))
            assert rejected.value.scope == "agent"
            assert rejected.value.limit == 1
            assert await _counts(store) == (1, 2, 1, 1)
            assert await store.get_task("rejected") is None

            assert await store.cancel_task(first.task_id)
            accepted = await store.create_task(_task("after-release"))
            assert accepted.task_id == "after-release"
            assert await _counts(store) == (1, 3, 1, 2)
            invocation = await store.get_agent_invocation(accepted.execution_id)
            assert invocation is not None
            assert invocation.ready_sequence == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_global_admission_rejects_task_and_mailbox_without_partial_rows(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            max_agent_queue=2,
            max_global_queue=2,
        )
        await store.initialize()
        try:
            await store.create_task(_task("occupies-global"))
            await store.create_task(_task("also-occupies-global"))
            with pytest.raises(QueueFullError) as task_rejected:
                await store.create_task(_task("global-task-rejected"))
            assert task_rejected.value.scope == "global"

            with pytest.raises(QueueFullError) as mailbox_rejected:
                await store.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="codex",
                    content="must roll back",
                    request_id="full-mailbox-request",
                    message_id="full-mailbox-message",
                )
            assert mailbox_rejected.value.scope == "global"
            snapshot = await store._call(
                lambda conn: (
                    int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]),
                    int(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]),
                    int(
                        conn.execute("SELECT COUNT(*) FROM agent_mailbox").fetchone()[0]
                    ),
                    int(
                        conn.execute(
                            "SELECT COUNT(*) FROM agent_invocations"
                        ).fetchone()[0]
                    ),
                )
            )
            assert snapshot == (2, 0, 0, 2)
            assert await _counts(store) == (2, 3, 2, 2)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_retry_queue_full_preserves_terminal_attempt_and_admission(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            max_agent_queue=1,
            max_global_queue=2,
        )
        await store.initialize()
        try:
            retryable = await store.create_task(_task("retryable"))
            retained_execution = retryable.execution_id
            claim = await store.claim_task_by_id(retryable.task_id, "worker")
            assert claim is not None
            assert await store.mark_task_running(
                retryable.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            failed = await store.complete_task(
                retryable.task_id,
                result={"status": "failed", "error": "expected"},
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert failed.state.value == "failed"
            await store.create_task(_task("occupier"))

            with pytest.raises(QueueFullError):
                await store.retry_task(retryable.task_id)

            unchanged = await store.get_task(retryable.task_id)
            assert unchanged is not None
            assert unchanged.state.value == "failed"
            assert unchanged.execution_id == retained_execution
            attempts = await store.list_task_executions(retryable.task_id)
            assert len(attempts) == 1
            assert await _counts(store) == (1, 3, 1, 2)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_queue_limit_configuration_is_strict(tmp_path):
    with pytest.raises(ValueError):
        SQLiteStore(tmp_path / "zero.sqlite", max_agent_queue=0)
    with pytest.raises(ValueError):
        SQLiteStore(
            tmp_path / "inverted.sqlite",
            max_agent_queue=3,
            max_global_queue=2,
        )
    assert AgentBridgeServer._error_response(QueueFullError("agent", 1)) == {
        "ok": False,
        "error": "queue_full",
        "message": "queue_full: agent limit 1",
    }


def test_task_and_mailbox_claims_share_ready_order_and_active_fence(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="first",
                request_id="mailbox-first",
                message_id="mailbox-first-message",
            )
            task = await store.create_task(_task("task-second"))

            # A task-specific worker cannot jump an earlier runnable mailbox
            # invocation for the same Agent.
            assert await store.claim_task_by_id(task.task_id, "task-worker") is None
            mailbox_claims = await store.claim_mailbox(
                "codex", "mailbox-worker", limit=20
            )
            assert [item.mailbox_id for item in mailbox_claims] == [
                mailbox.mailbox_id
            ]
            claim = mailbox_claims[0]
            assert await store.mark_mailbox_processing(
                mailbox.mailbox_id, claim.claim_token
            )

            # The active mailbox invocation fences the otherwise-ready task.
            assert await store.claim_next_task("task-worker") is None
            assert await store.mark_mailbox_processed(
                mailbox.mailbox_id, claim.claim_token
            )

            task_claim = await store.claim_task_by_id(task.task_id, "task-worker")
            assert task_claim is not None
            assert task_claim.task.task_id == task.task_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_startup_reconstructs_missing_or_stale_admission_projection(tmp_path):
    async def seed(path) -> None:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            await store.create_task(_task("one"))
            await store.create_task(_task("two"))
        finally:
            await store.close()

    path = tmp_path / "runtime.sqlite"
    asyncio.run(seed(path))

    connection = sqlite3.connect(path)
    try:
        connection.execute("DELETE FROM agent_admission_counters")
        connection.execute(
            "UPDATE global_agent_admission_counter SET unfinished_count=99"
        )
        connection.commit()
    finally:
        connection.close()

    async def verify() -> None:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            assert await _counts(store) == (2, 3, 2, 2)
            assert await store.cancel_task("one")
            assert await _counts(store) == (1, 3, 1, 2)
        finally:
            await store.close()

    asyncio.run(verify())


def test_gateway_queue_full_is_durable_replayable_and_creates_no_task(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            max_agent_queue=1,
            max_global_queue=2,
        )
        await store.initialize()
        try:
            await store.create_task(_task("occupier"))
            gateway = WeChatGateway(
                store,
                bot_id="bot",
                command_router=MVPCommandRouter(store),
            )
            message = _message("please enqueue this")
            client = SimpleNamespace(bot_id="bot")
            accepted = await gateway.handle_message(client, message)
            assert accepted is not None
            assert accepted.command_response == (
                "Agent queue is full; try again later."
            )

            replay = await gateway.handle_message(client, message)
            assert replay is not None
            assert replay.command_response == accepted.command_response
            assert await _counts(store) == (1, 2, 1, 1)
            assert await store._call(
                lambda conn: int(
                    conn.execute("SELECT COUNT(*) FROM inbound_messages").fetchone()[0]
                )
            ) == 1
            outbox = await store.list_outbox()
            assert len(outbox) == 1
            assert "queue is full" in outbox[0].content.lower()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_ask_command_renders_stable_queue_full_response():
    class Manager:
        async def get_active_agent(self, **_kwargs):
            return "codex"

        async def ensure_agent(self, *_args, **_kwargs):
            return True

        async def submit(self, *_args, **_kwargs):
            raise QueueFullError("global", 2)

    async def scenario() -> None:
        envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="ask-full",
            text="/ask planner review this",
            session_id="default",
            agent_id="codex",
            conversation_id="conversation",
        )
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command(envelope.text),
            envelope,
        )
        assert response == "cannot ask Agent: queue is full"

    asyncio.run(scenario())


def test_manager_keeps_queue_rejection_ordered_with_route_snapshot(tmp_path):
    class Runtime:
        agent_id = "codex"

        async def start(self):
            return None

        async def stop(self):
            return None

        async def run(self, task, _emit):
            return AgentResult(task_id=task.task_id, content="unused")

        async def interrupt(self, _task_id):
            return False

    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            max_agent_queue=1,
            max_global_queue=2,
        )
        registry = AgentRegistry()
        registry.register("codex", Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
            reconcile_interval=None,
        )
        await manager.start()
        try:
            await manager.submit(
                "occupy",
                ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                ),
            )
            gateway = WeChatGateway(
                manager,
                bot_id="bot",
                command_router=MVPCommandRouter(manager),
            )
            client = SimpleNamespace(bot_id="bot")
            first = await gateway.handle_message(
                client,
                _message("queued after capacity", message_id=902),
            )
            assert first is not None
            assert first.command_response == (
                "Agent queue is full; try again later."
            )
            assert first.response_agent_id == "codex"
            assert await _counts(store) == (1, 2, 1, 1)
        finally:
            await manager.stop()

    asyncio.run(scenario())
