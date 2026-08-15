"""Mailbox lease renewal regressions for long Agent dispatches."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.sqlite_store import SQLiteStore, StoreError
from src.runtime.worker import AgentMailboxWorker


def test_mailbox_worker_renews_lease_during_long_dispatch(tmp_path):
    async def scenario() -> None:
        current_time = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current_time[0]
        )
        await store.initialize()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def handler(_item):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return "done"

        try:
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="long request",
                request_id="long-mailbox-request",
            )
            worker = AgentMailboxWorker(
                store,
                {},
                "planner",
                worker_id="mailbox-a",
                handler=handler,
                lease_seconds=0.12,
            )
            running = asyncio.create_task(worker.run_once())
            await asyncio.wait_for(started.wait(), timeout=1)

            claimed = await store.get_mailbox_item(request.mailbox_id)
            assert claimed is not None
            assert claimed.state.value == "processing"
            assert claimed.lease_expires_at is not None
            original_expiry = claimed.lease_expires_at

            # Advance the store clock near the original deadline and wait for
            # the worker's heartbeat to publish a later lease.
            current_time[0] += timedelta(seconds=0.08)
            for _ in range(100):
                renewed = await store.get_mailbox_item(request.mailbox_id)
                if (
                    renewed is not None
                    and renewed.lease_expires_at is not None
                    and renewed.lease_expires_at > original_expiry
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("mailbox lease was not renewed")

            assert not await store.renew_mailbox_lease(
                request.mailbox_id,
                "stale-claim-token",
                lease_seconds=0.12,
            )

            # This instant is after the original claim but before the renewed
            # deadline. Reconciliation must leave the active request owned, and
            # a second worker must not invoke the Agent again.
            current_time[0] += timedelta(seconds=0.08)
            report = await store.reconcile()
            assert report.mailbox_requeued == 0
            duplicate_worker = AgentMailboxWorker(
                store,
                {},
                "planner",
                worker_id="mailbox-b",
                handler=handler,
                lease_seconds=0.12,
            )
            assert await duplicate_worker.run_once() == 0
            assert calls == 1

            release.set()
            assert await asyncio.wait_for(running, timeout=1) == 1
            completed = await store.get_mailbox_item(request.mailbox_id)
            assert completed is not None
            assert completed.state.value == "processed"
            assert completed.claim_token is None
            assert completed.lease_expires_at is None
            assert worker._lease_heartbeats == {}
        finally:
            release.set()
            await store.close()

    asyncio.run(scenario())


def test_expired_mailbox_claim_cannot_renew_finalize_or_publish_reply(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        current_time = [base]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current_time[0]
        )
        await store.initialize()
        try:
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="question",
                request_id="expired-request",
            )
            claim = (
                await store.claim_mailbox(
                    "planner", "worker", lease_seconds=1, now=base
                )
            )[0]
            assert await store.mark_mailbox_processing(
                request.mailbox_id, claim.claim_token, now=base
            )
            expired = base + timedelta(seconds=2)
            current_time[0] = expired

            assert not await store.renew_mailbox_lease(
                request.mailbox_id, claim.claim_token, now=expired
            )
            assert not await store.mark_mailbox_processed(
                request.mailbox_id, claim.claim_token, now=expired
            )
            assert not await store.mark_mailbox_failed(
                request.mailbox_id, claim.claim_token, now=expired
            )
            assert not await store.reject_mailbox(
                request.mailbox_id, claim.claim_token, now=expired
            )
            with pytest.raises(StoreError, match="response claim is not active"):
                await store.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="codex",
                    content="stale answer",
                    request_id=request.request_id,
                    reply_to_id=request.message_id,
                    original_mailbox_id=request.mailbox_id,
                    original_claim_token=claim.claim_token,
                )
            assert await store.get_correlated_mailbox_response(
                request.mailbox_id
            ) is None
            assert await store._call(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE request_id=? "
                    "AND reply_to_id=?",
                    (request.request_id, request.message_id),
                ).fetchone()[0]
            ) == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_response_uses_transaction_time_after_waiting_for_store(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        current_time = [base]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current_time[0]
        )
        await store.initialize()
        release = threading.Event()
        started = threading.Event()
        try:
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="question",
                request_id="queued-response-request",
            )
            claim = (
                await store.claim_mailbox(
                    "planner", "worker", lease_seconds=1, now=base
                )
            )[0]
            assert await store.mark_mailbox_processing(
                request.mailbox_id, claim.claim_token, now=base
            )

            def hold_store(_conn) -> None:
                started.set()
                assert release.wait(timeout=2)

            blocker = asyncio.create_task(store._call(hold_store))
            assert await asyncio.to_thread(started.wait, 1)
            pending = asyncio.create_task(
                store.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="codex",
                    content="late answer",
                    request_id=request.request_id,
                    reply_to_id=request.message_id,
                    original_mailbox_id=request.mailbox_id,
                    original_claim_token=claim.claim_token,
                )
            )
            await asyncio.sleep(0)
            current_time[0] = base + timedelta(seconds=2)
            release.set()
            await blocker

            with pytest.raises(StoreError, match="response claim is not active"):
                await pending
            assert await store.get_correlated_mailbox_response(
                request.mailbox_id
            ) is None
        finally:
            release.set()
            await store.close()

    asyncio.run(scenario())


def test_mailbox_worker_does_not_publish_reply_after_lease_loss(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        started = asyncio.Event()
        release = asyncio.Event()
        cancelled = asyncio.Event()
        replies: list[str] = []

        async def handler(_item):
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return "stale answer"

        async def reply_handler(_item, result):
            replies.append(str(result))

        try:
            request = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="long request",
                request_id="lost-mailbox-request",
            )
            worker = AgentMailboxWorker(
                store,
                {},
                "planner",
                worker_id="mailbox-a",
                handler=handler,
                reply_handler=reply_handler,
                lease_seconds=0.1,
            )
            running = asyncio.create_task(worker.run_once())
            await asyncio.wait_for(started.wait(), timeout=1)

            claimed = await store.get_mailbox_item(request.mailbox_id)
            assert claimed is not None
            assert claimed.claim_token
            assert await store.mark_mailbox_failed(
                request.mailbox_id,
                claim_token=claimed.claim_token,
                error="ownership transferred",
            )
            await asyncio.wait_for(cancelled.wait(), timeout=1)
            assert await asyncio.wait_for(running, timeout=1) == 0
            assert replies == []
            orphaned = await store.get_mailbox_item(request.mailbox_id)
            assert orphaned is not None
            assert orphaned.state.value == "orphaned_mailbox"
            assert worker._lost_mailbox_claims == set()
            assert worker._mailbox_claim_loss_events == {}
        finally:
            release.set()
            await store.close()

    asyncio.run(scenario())


def test_mailbox_worker_cancels_dispatch_after_renewal_errors_outlive_lease():
    class Store:
        def __init__(self) -> None:
            self.claimed = False

        async def claim_mailbox(self, *_args, **_kwargs):
            if self.claimed:
                return []
            self.claimed = True
            return [
                {
                    "mailbox_id": "mailbox-1",
                    "message_id": "message-1",
                    "claim_token": "claim-1",
                }
            ]

        async def mark_mailbox_processing(self, *_args, **_kwargs) -> bool:
            return True

        async def renew_mailbox_lease(self, *_args, **_kwargs) -> bool:
            raise OSError("database unavailable")

        async def mark_mailbox_processed(self, *_args, **_kwargs) -> bool:
            raise AssertionError("an unconfirmed mailbox claim was terminalized")

    async def scenario() -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()
        replies: list[str] = []

        async def handler(_item):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def reply_handler(_item, result):
            replies.append(str(result))

        worker = AgentMailboxWorker(
            Store(),
            {},
            "planner",
            handler=handler,
            reply_handler=reply_handler,
            lease_seconds=0.1,
        )
        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(cancelled.wait(), timeout=1)

        assert await asyncio.wait_for(running, timeout=1) == 0
        assert replies == []
        assert worker._mailbox_claim_loss_events == {}

    asyncio.run(scenario())
