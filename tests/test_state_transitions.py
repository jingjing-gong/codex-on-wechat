"""Public state-transition APIs must not skip execution stages."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.models import InboundMessage
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError


class _Runtime:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.interrupted: list[str] = []

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def run(self, *_args: object) -> None:
        return None

    async def interrupt(self, _task_id: str) -> bool:
        self.interrupted.append(_task_id)
        return True


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


def _target() -> dict[str, str]:
    return {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": "user",
        "session_id": "default",
    }


def test_started_agent_registration_cannot_be_replaced() -> None:
    async def scenario() -> None:
        original = _Runtime()
        replacement = _Runtime()
        registry = AgentRegistry({"codex": original})
        await registry.start()
        try:
            with pytest.raises(RuntimeError, match="cannot replace a started Agent"):
                registry.register("codex", replacement, replace=True)

            assert registry.require("codex") is original
            assert original.started == 1
            assert original.stopped == 0
            assert replacement.started == 0
            assert replacement.stopped == 0
        finally:
            await registry.stop()

        assert original.stopped == 1
        assert replacement.stopped == 0

    asyncio.run(scenario())


def test_durable_cancel_succeeds_when_runtime_interrupt_raises() -> None:
    class Store:
        async def get_task(self, task_id: str):
            return {"task_id": task_id, "agent_id": "codex", "state": "running"}

        async def cancel_task(self, _task_id: str) -> bool:
            return True

    class Runtime(_Runtime):
        async def interrupt(self, _task_id: str) -> bool:
            raise OSError("SDK connection closed")

    async def scenario() -> None:
        registry = AgentRegistry({"codex": Runtime()})
        manager = TaskManager(Store(), registry, worker_count=0)

        assert await manager.cancel("task-1") is True

    asyncio.run(scenario())


@pytest.mark.parametrize("initial_state", ("queued", "orphaned"))
def test_administrative_cancel_does_not_interrupt_a_nonrunning_task(
    initial_state: str,
) -> None:
    class Store:
        state = initial_state

        async def get_task(self, task_id: str):
            return {"task_id": task_id, "agent_id": "codex", "state": self.state}

        async def cancel_task(self, _task_id: str) -> bool:
            self.state = "cancelled"
            return True

    async def scenario() -> None:
        runtime = _Runtime()
        registry = AgentRegistry({"codex": runtime})
        manager = TaskManager(Store(), registry, worker_count=0)

        assert await manager.cancel("task-1") is True
        assert runtime.interrupted == []

    asyncio.run(scenario())


def test_cancel_interrupts_a_task_that_becomes_active_during_transition() -> None:
    class Store:
        calls = 0

        async def get_task(self, task_id: str):
            self.calls += 1
            return {
                "task_id": task_id,
                "agent_id": "codex",
                "state": "cancel_requested",
            }

        async def cancel_task(self, _task_id: str) -> bool:
            return True

    async def scenario() -> None:
        runtime = _Runtime()
        registry = AgentRegistry({"codex": runtime})
        manager = TaskManager(Store(), registry, worker_count=0)

        assert await manager.cancel("task-1") is True
        assert runtime.interrupted == ["task-1"]

    asyncio.run(scenario())


def test_successful_terminal_apis_require_intermediate_states(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "task-worker")
            assert claim is not None

            with pytest.raises(InvalidTransition, match="from claimed"):
                await store.complete_task(
                    task.task_id,
                    status="completed",
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                )

            claimed_task = await store.get_task(task.task_id)
            execution = await store.get_execution(claim.execution_id)
            assert claimed_task is not None
            assert claimed_task.state.value == "claimed"
            assert execution is not None
            assert execution.state.value == "claimed"
            assert execution.started_at is None

            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            completed = await store.complete_task(
                task.task_id,
                status="completed",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            assert completed.state.value == "completed"

            outbox = await store.create_user_outbox(
                target=_target(),
                content="deliver me",
                outbox_id="state-machine-outbox",
            )
            outbox_claims = await store.claim_outbox(
                "outbox-worker", automatic=False
            )
            claimed_outbox = next(
                item for item in outbox_claims if item.outbox_id == outbox.outbox_id
            )
            assert not await store.mark_outbox_sent(
                outbox.outbox_id, claimed_outbox.claim_token
            )
            assert not await store.mark_outbox_failed(
                outbox.outbox_id,
                claimed_outbox.claim_token,
                permanent=True,
            )
            assert (await store.get_outbox_item(outbox.outbox_id)).state.value == "claimed"
            assert await store.mark_outbox_sending(
                outbox.outbox_id, claimed_outbox.claim_token
            )
            assert await store.mark_outbox_sent(
                outbox.outbox_id, claimed_outbox.claim_token
            )
            assert (await store.get_outbox_item(outbox.outbox_id)).state.value == "sent"

            mailbox = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="process me",
            )
            mailbox_claims = await store.claim_mailbox(
                "planner", "mailbox-worker"
            )
            claimed_mailbox = next(
                item
                for item in mailbox_claims
                if item.mailbox_id == mailbox.mailbox_id
            )
            assert not await store.mark_mailbox_processed(
                mailbox.mailbox_id, claimed_mailbox.claim_token
            )
            assert not await store.mark_mailbox_failed(
                mailbox.mailbox_id, claimed_mailbox.claim_token
            )
            assert not await store.reject_mailbox(
                mailbox.mailbox_id, claimed_mailbox.claim_token
            )
            assert (await store.get_mailbox_item(mailbox.mailbox_id)).state.value == "claimed"
            assert await store.mark_mailbox_processing(
                mailbox.mailbox_id, claimed_mailbox.claim_token
            )
            assert await store.mark_mailbox_processed(
                mailbox.mailbox_id, claimed_mailbox.claim_token
            )
            assert (await store.get_mailbox_item(mailbox.mailbox_id)).state.value == "processed"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_expired_outbox_claim_cannot_be_renewed_or_finalized(tmp_path) -> None:
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store = SQLiteStore(tmp_path / "runtime.sqlite", clock=lambda: base)
        await store.initialize()
        try:
            outbox = await store.create_user_outbox(
                target=_target(),
                content="deliver me",
                outbox_id="expired-outbox",
                now=base,
            )
            claim = (
                await store.claim_outbox(
                    "outbox-worker",
                    automatic=False,
                    lease_seconds=1,
                    now=base,
                )
            )[0]
            assert claim.outbox_id == outbox.outbox_id
            assert await store.mark_outbox_sending(
                outbox.outbox_id, claim.claim_token, now=base
            )
            expired = base + timedelta(seconds=2)

            assert not await store.renew_outbox_lease(
                outbox.outbox_id, claim.claim_token, now=expired
            )
            assert not await store.mark_outbox_sent(
                outbox.outbox_id, claim.claim_token, now=expired
            )
            assert not await store.mark_outbox_failed(
                outbox.outbox_id,
                claim.claim_token,
                error="stale sender",
                now=expired,
            )
            persisted = await store.get_outbox_item(outbox.outbox_id)
            assert persisted is not None
            assert persisted.state.value == "sending"
            assert persisted.last_error is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_outbox_retry_deadline_uses_transaction_time(tmp_path) -> None:
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        current_time = [base]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current_time[0]
        )
        await store.initialize()
        try:
            outbox = await store.create_user_outbox(
                target=_target(), content="retry me", now=base
            )
            claim = (
                await store.claim_outbox(
                    "outbox-worker",
                    automatic=False,
                    lease_seconds=60,
                    now=base,
                )
            )[0]
            assert await store.mark_outbox_sending(
                outbox.outbox_id, claim.claim_token, now=base
            )

            current_time[0] = base + timedelta(seconds=10)
            assert await store.mark_outbox_failed(
                outbox.outbox_id,
                claim.claim_token,
                error="retry later",
                delay=5,
            )
            persisted = await store.get_outbox_item(outbox.outbox_id)
            assert persisted is not None
            assert persisted.next_attempt_at == base + timedelta(seconds=15)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_delivery_unknown_retry_reports_accepted_and_rejected_transitions(
    tmp_path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "delivery-unknown-retry.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            outbox = await first.create_user_outbox(
                target=_target(),
                content="ambiguous send",
                outbox_id="delivery-unknown-retry",
            )
            claim = (
                await first.claim_outbox(
                    "outbox-worker",
                    automatic=False,
                )
            )[0]
            assert claim.outbox_id == outbox.outbox_id
            assert await first.mark_outbox_sending(
                outbox.outbox_id,
                claim.claim_token,
            )
        finally:
            await first.close()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            recovered = await restarted.get_outbox_item(outbox.outbox_id)
            assert recovered is not None
            assert recovered.state.value == "delivery_unknown"

            assert await restarted.retry_outbox(outbox.outbox_id) is True
            pending = await restarted.get_outbox_item(outbox.outbox_id)
            assert pending is not None
            assert pending.state.value == "pending"
            assert await restarted.retry_outbox(outbox.outbox_id) is False
            assert await restarted.retry_outbox("missing-outbox") is False
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_store_first_task_failure_is_not_acknowledged_as_duplicate(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            owner = await store.create_task({**_task(), "task_id": "occupied-task"})
            inbound = InboundMessage(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="stored-before-task-failure",
                text="must remain retryable",
            )
            stored = await store.store_inbound(inbound)

            with pytest.raises(StoreError, match="UNIQUE constraint failed"):
                await store.accept_inbound(
                    inbound,
                    task={**_task(), "task_id": owner.task_id},
                )

            unchanged = await store.get_inbound(stored.message_id)
            assert unchanged is not None
            assert unchanged.status.value == "stored"
            assert unchanged.task_id is None
            assert await store.get_task_by_inbound(stored.message_id) is None
            assert [task.task_id for task in await store.list_tasks()] == [
                owner.task_id
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("versions", [(1, 3), tuple(range(1, 16)) + (99,)])
def test_store_rejects_incompatible_migration_history(tmp_path, versions) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()

        with sqlite3.connect(path) as connection:
            connection.execute("DELETE FROM schema_migrations")
            connection.executemany(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                [(version, "2026-08-13T00:00:00+00:00") for version in versions],
            )
            connection.commit()

        reopened = SQLiteStore(path)
        try:
            with pytest.raises(StoreError, match="database schema"):
                await reopened.initialize()
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_command_receipt_identity_is_scoped_and_terminal(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            values = {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user-a",
                "session_id": "default",
                "external_message_id": "command-message",
                "command_name": "mode",
                "command_args": ("review",),
                "command_text": "/mode review",
            }
            receipt = await store.begin_command_receipt("command-receipt", **values)
            assert receipt["state"] == "started"

            with pytest.raises(StoreError, match="external_user_id"):
                await store.begin_command_receipt(
                    "command-receipt",
                    **{**values, "external_user_id": "user-b"},
                )

            completed = await store.complete_command_receipt(
                "command-receipt",
                response_text="mode: review",
                response_agent_id="codex",
            )
            assert completed["state"] == "completed"
            with pytest.raises(StoreError, match="completion conflicts"):
                await store.complete_command_receipt(
                    "command-receipt",
                    response_text="mode: chat",
                    response_agent_id="codex",
                )

            unchanged = await store.interrupt_command_receipt("command-receipt")
            assert unchanged["state"] == "completed"
            assert unchanged["response_text"] == "mode: review"
        finally:
            await store.close()

    asyncio.run(scenario())
