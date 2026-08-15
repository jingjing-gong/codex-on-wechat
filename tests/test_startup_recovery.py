"""Process-boundary recovery and managed-media claim fences."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.media import AttachmentStore
from src.runtime.models import MediaDeliveryState
from src.runtime.manager import TaskManager
from src.runtime.sqlite_store import SQLiteStore, StoreError


def _task(task_id: str = "task-restart", *, attachment_id: str | None = None) -> dict[str, object]:
    inputs: dict[str, object] = {"text": "restart me"}
    if attachment_id:
        inputs["attachments"] = [attachment_id]
    return {
        "task_id": task_id,
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
        "inputs": inputs,
    }


def _target() -> dict[str, str]:
    return {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": "user",
        "session_id": "default",
    }


def test_store_expands_file_backed_database_path(monkeypatch, tmp_path):
    async def scenario() -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        store = SQLiteStore("~/.runtime/state.sqlite")
        assert Path(store.path) == (tmp_path / ".runtime" / "state.sqlite").resolve()
        await store.initialize()
        try:
            assert (tmp_path / ".runtime" / "state.sqlite").is_file()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cancelled_store_close_drains_connection_shutdown(tmp_path, monkeypatch):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        close_started = threading.Event()
        allow_close = threading.Event()
        original_close = store._close_sync

        def delayed_close() -> None:
            close_started.set()
            assert allow_close.wait(timeout=2)
            original_close()

        monkeypatch.setattr(store, "_close_sync", delayed_close)
        close_task = asyncio.create_task(store.close())
        assert await asyncio.to_thread(close_started.wait, 2)

        close_task.cancel()
        await asyncio.sleep(0)
        assert not close_task.done()
        close_task.cancel()
        await asyncio.sleep(0)
        assert not close_task.done()

        health_task = asyncio.create_task(store.health())
        await asyncio.sleep(0)
        assert not health_task.done()

        allow_close.set()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        with pytest.raises(StoreError, match="store is closed"):
            await health_task
        assert store._closed
        assert not store._initialized
        assert store._conn is None

    asyncio.run(scenario())


def test_repeated_cancellation_cannot_release_store_during_initialize(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        open_started = threading.Event()
        allow_open = threading.Event()
        original_open = store._open_sync

        def delayed_open() -> None:
            open_started.set()
            assert allow_open.wait(timeout=2)
            original_open()

        monkeypatch.setattr(store, "_open_sync", delayed_open)
        initialize_task = asyncio.create_task(store.initialize())
        assert await asyncio.to_thread(open_started.wait, 2)

        initialize_task.cancel()
        await asyncio.sleep(0)
        assert not initialize_task.done()
        initialize_task.cancel()
        await asyncio.sleep(0)
        assert not initialize_task.done()

        health_task = asyncio.create_task(store.health())
        await asyncio.sleep(0)
        assert not health_task.done()

        allow_open.set()
        with pytest.raises(asyncio.CancelledError):
            await initialize_task
        await health_task
        assert store._initialized
        assert store._conn is not None
        await store.close()

    asyncio.run(scenario())


def test_repeated_cancellation_cannot_release_store_during_lazy_initialize(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        open_started = threading.Event()
        allow_open = threading.Event()
        original_open = store._open_sync

        def delayed_open() -> None:
            open_started.set()
            assert allow_open.wait(timeout=2)
            original_open()

        monkeypatch.setattr(store, "_open_sync", delayed_open)
        first_health = asyncio.create_task(store.health())
        assert await asyncio.to_thread(open_started.wait, 2)

        first_health.cancel()
        await asyncio.sleep(0)
        assert not first_health.done()
        first_health.cancel()
        await asyncio.sleep(0)
        assert not first_health.done()

        competing_health = asyncio.create_task(store.health())
        await asyncio.sleep(0)
        assert not competing_health.done()

        allow_open.set()
        with pytest.raises(asyncio.CancelledError):
            await first_health
        await competing_health
        assert store._initialized
        assert store._conn is not None
        await store.close()

    asyncio.run(scenario())


def test_repeated_cancellation_drains_store_operation_before_unlock(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        operation_started = threading.Event()
        allow_operation = threading.Event()

        def delayed_operation(conn) -> None:
            operation_started.set()
            assert allow_operation.wait(timeout=2)
            conn.execute("SELECT 1").fetchone()

        operation_task = asyncio.create_task(store._call(delayed_operation))
        assert await asyncio.to_thread(operation_started.wait, 2)

        operation_task.cancel()
        await asyncio.sleep(0)
        assert not operation_task.done()
        operation_task.cancel()
        await asyncio.sleep(0)
        assert not operation_task.done()

        health_task = asyncio.create_task(store.health())
        await asyncio.sleep(0)
        assert not health_task.done()

        allow_operation.set()
        with pytest.raises(asyncio.CancelledError):
            await operation_task
        await health_task
        await store.close()

    asyncio.run(scenario())


def test_task_manager_uses_process_boundary_reconciliation() -> None:
    class Runtime:
        agent_id = "codex"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def interrupt(self, _task_id: str) -> bool:
            return True

    class Store:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def initialize(self) -> None:
            self.calls.append("initialize")

        async def startup_reconcile(self) -> None:
            self.calls.append("startup_reconcile")

        async def reconcile(self) -> None:
            self.calls.append("reconcile")

        async def close(self) -> None:
            self.calls.append("close")

    async def scenario() -> None:
        store = Store()
        manager = TaskManager(store, runtime=Runtime(), worker_count=0)
        await manager.start()
        try:
            assert store.calls[:2] == ["initialize", "startup_reconcile"]
            assert "reconcile" not in store.calls
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_task_manager_periodically_recovers_expired_leases_without_restart(
    tmp_path,
):
    class Runtime:
        agent_id = "codex"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        current_time = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            clock=lambda: current_time[0],
        )
        manager = TaskManager(
            store,
            runtime=Runtime(),
            worker_count=0,
            reconcile_interval=0.01,
        )
        await manager.start()
        try:
            task = await store.create_task(_task("live-expired-task"))
            task_claim = await store.claim_task_by_id(
                task.task_id,
                "task-worker",
                lease_seconds=1,
                now=current_time[0],
            )
            assert task_claim is not None
            assert await store.mark_task_running(
                task.task_id,
                task_claim.claim_token,
                execution_id=task_claim.execution_id,
                now=current_time[0],
            )

            blocked_mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="wait behind the task",
                request_id="live-blocked-mailbox",
            )
            assert await store.claim_mailbox(
                "codex",
                "mailbox-worker",
                lease_seconds=1,
                now=current_time[0],
            ) == []

            # A different Agent owns an independent active-invocation fence,
            # so task and mailbox recovery can still be exercised together.
            mailbox = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="recover me",
                request_id="live-expired-mailbox",
            )
            mailbox_claims = await store.claim_mailbox(
                "planner",
                "mailbox-worker",
                lease_seconds=1,
                now=current_time[0],
            )
            assert len(mailbox_claims) == 1
            mailbox_claim = mailbox_claims[0]
            assert await store.mark_mailbox_processing(
                mailbox.mailbox_id,
                mailbox_claim.claim_token,
            )

            claimed_outbox = await store.create_user_outbox(
                target=_target(),
                content="claimed",
                outbox_id="live-expired-outbox-claimed",
                now=current_time[0],
            )
            sending_outbox = await store.create_user_outbox(
                target=_target(),
                content="sending",
                outbox_id="live-expired-outbox-sending",
                now=current_time[0],
            )
            outbox_claims = await store.claim_outbox(
                "outbox-worker",
                limit=2,
                lease_seconds=1,
                now=current_time[0],
                automatic=False,
            )
            by_id = {item.outbox_id: item for item in outbox_claims}
            assert set(by_id) == {
                claimed_outbox.outbox_id,
                sending_outbox.outbox_id,
            }
            assert await store.mark_outbox_sending(
                sending_outbox.outbox_id,
                claim_token=by_id[sending_outbox.outbox_id].claim_token,
                now=current_time[0],
            )

            current_time[0] += timedelta(seconds=2)

            async def recovered() -> bool:
                current_task = await store.get_task(task.task_id)
                current_mailbox = await store.get_mailbox_item(mailbox.mailbox_id)
                current_claimed = await store.get_outbox_item(
                    claimed_outbox.outbox_id
                )
                current_sending = await store.get_outbox_item(
                    sending_outbox.outbox_id
                )
                return bool(
                    current_task is not None
                    and current_task.state.value == "orphaned"
                    and current_mailbox is not None
                    and current_mailbox.state.value == "orphaned_mailbox"
                    and current_claimed is not None
                    and current_claimed.state.value == "pending"
                    and current_sending is not None
                    and current_sending.state.value == "delivery_unknown"
                )

            for _ in range(100):
                if await recovered():
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("periodic lease reconciliation did not run")

            execution = await store.get_execution(task_claim.execution_id)
            assert execution is not None
            assert execution.state.value == "orphaned"
            still_blocked = await store.get_mailbox_item(
                blocked_mailbox.mailbox_id
            )
            assert still_blocked is not None
            assert still_blocked.state.value == "pending"
            assert manager._reconcile_task is not None
            assert not manager._reconcile_task.done()
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_task_manager_drains_periodic_reconcile_before_store_close() -> None:
    class Runtime:
        agent_id = "codex"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def interrupt(self, _task_id: str) -> bool:
            return False

    class Store:
        def __init__(self) -> None:
            self.sweep_started = asyncio.Event()
            self.sweep_active = False
            self.closed = False

        async def initialize(self) -> None:
            return None

        async def startup_reconcile(self) -> None:
            return None

        async def recover_expired_leases(self) -> None:
            self.sweep_active = True
            self.sweep_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.sweep_active = False

        async def close(self) -> None:
            assert not self.sweep_active
            self.closed = True

    async def scenario() -> None:
        store = Store()
        manager = TaskManager(
            store,
            runtime=Runtime(),
            worker_count=0,
            reconcile_interval=0.01,
        )
        await manager.start()
        await asyncio.wait_for(store.sweep_started.wait(), timeout=1)
        await asyncio.wait_for(manager.stop(), timeout=1)

        assert store.closed
        assert manager._reconcile_task is None
        assert not store.sweep_active

    asyncio.run(scenario())


def test_startup_reconcile_reclaims_future_leases_conservatively(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path, attachment_root=tmp_path / "attachments")
        await first.initialize()
        base = datetime(2030, 1, 1, tzinfo=timezone.utc)
        try:
            task = await first.create_task(_task())
            task_claim = await first.claim_task_by_id(
                task.task_id, "old-task-worker", lease_seconds=3600, now=base
            )
            assert task_claim is not None
            assert await first.mark_task_running(
                task.task_id,
                task_claim.claim_token,
                execution_id=task_claim.execution_id,
                now=base,
            )

            claimed_outbox = await first.create_user_outbox(
                target=_target(),
                content="claimed delivery",
                outbox_id="restart-outbox-claimed",
                now=base,
            )
            sending_outbox = await first.create_user_outbox(
                target=_target(),
                content="sending delivery",
                outbox_id="restart-outbox-sending",
                now=base,
            )
            outbox_claims = await first.claim_outbox(
                "old-outbox-worker",
                limit=2,
                lease_seconds=3600,
                now=base,
                automatic=False,
            )
            assert {item.outbox_id for item in outbox_claims} == {
                claimed_outbox.outbox_id,
                sending_outbox.outbox_id,
            }
            by_id = {item.outbox_id: item for item in outbox_claims}
            assert await first.mark_outbox_sending(
                sending_outbox.outbox_id,
                by_id[sending_outbox.outbox_id].claim_token,
                now=base,
            )

            blocked_mailbox = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="codex",
                content="blocked by running task",
                request_id="restart-mailbox-blocked",
                now=base,
            )
            assert await first.claim_mailbox(
                "codex",
                "old-mailbox-worker",
                limit=2,
                lease_seconds=3600,
                now=base,
            ) == []

            mailbox_claimed = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="claimed mailbox",
                request_id="restart-mailbox-claimed",
                now=base,
            )
            mailbox_waiting = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="second planner mailbox",
                request_id="restart-mailbox-waiting",
                now=base,
            )
            mailbox_processing = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="reviewer",
                content="processing mailbox",
                request_id="restart-mailbox-processing",
                now=base,
            )
            planner_claims = await first.claim_mailbox(
                "planner",
                "old-mailbox-worker",
                limit=2,
                lease_seconds=3600,
                now=base,
            )
            # limit is an API compatibility ceiling; an Agent receives at
            # most one active invocation, leaving its second row queued.
            assert [item.mailbox_id for item in planner_claims] == [
                mailbox_claimed.mailbox_id
            ]
            assert (
                await first.get_mailbox_item(mailbox_waiting.mailbox_id)
            ).state.value == "pending"
            reviewer_claims = await first.claim_mailbox(
                "reviewer",
                "old-mailbox-worker",
                limit=2,
                lease_seconds=3600,
                now=base,
            )
            assert [item.mailbox_id for item in reviewer_claims] == [
                mailbox_processing.mailbox_id
            ]
            assert await first.mark_mailbox_processing(
                mailbox_processing.mailbox_id,
                reviewer_claims[0].claim_token,
            )

            attachment_store = AttachmentStore(tmp_path / "attachments")
            attachment = attachment_store.put_bytes(b"media", attachment_id="restart-media-file")
            await first.register_attachment(attachment, kind="image")
            media_upload = await first.create_outgoing_media(
                attachment_id=attachment.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="restart-media-upload",
                idempotency_key="restart-media-upload",
                now=base,
            )
            media_uploaded = await first.create_outgoing_media(
                attachment_id=attachment.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="restart-media-uploaded",
                idempotency_key="restart-media-uploaded",
                state=MediaDeliveryState.UPLOADED,
                now=base,
            )
            media_send = await first.create_outgoing_media(
                attachment_id=attachment.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="restart-media-send",
                idempotency_key="restart-media-send",
                state=MediaDeliveryState.SEND_PENDING,
                now=base,
            )
            media_claims = await first.claim_outgoing_media(
                "old-media-worker", limit=10, lease_seconds=3600, now=base
            )
            assert {item.media_id for item in media_claims} == {
                media_upload.media_id,
                media_uploaded.media_id,
                media_send.media_id,
            }

            # A normal expiry sweep must not steal healthy leases.
            live_report = await first.reconcile(now=base + timedelta(seconds=1))
            assert live_report == type(live_report)()
            assert (await first.get_task(task.task_id)).state.value == "running"
        finally:
            await first.close()

        restarted = SQLiteStore(path, attachment_root=tmp_path / "attachments")
        await restarted.initialize()
        try:
            report = await restarted.startup_reconcile(now=base + timedelta(seconds=2))
            assert report.tasks_orphaned == 1
            assert report.outbox_requeued == 1
            assert report.outbox_unknown == 1
            assert report.mailbox_requeued == 2
            assert report.media_requeued == 3

            recovered_task = await restarted.get_task(task.task_id)
            assert recovered_task is not None
            assert recovered_task.state.value == "orphaned"
            execution = await restarted.get_execution(task_claim.execution_id)
            assert execution is not None
            assert execution.state.value == "orphaned"

            recovered_outbox = {
                item.outbox_id: item
                for item in await restarted.list_outbox(limit=10)
            }
            assert recovered_outbox[claimed_outbox.outbox_id].state.value == "pending"
            assert recovered_outbox[sending_outbox.outbox_id].state.value == "delivery_unknown"
            assert all(item.claim_token is None for item in recovered_outbox.values())

            recovered_mailbox = {
                mailbox_id: await restarted.get_mailbox_item(mailbox_id)
                for mailbox_id in (
                    blocked_mailbox.mailbox_id,
                    mailbox_claimed.mailbox_id,
                    mailbox_waiting.mailbox_id,
                    mailbox_processing.mailbox_id,
                )
            }
            assert (
                recovered_mailbox[mailbox_claimed.mailbox_id].state.value
                == "orphaned_mailbox"
            )
            assert (
                recovered_mailbox[mailbox_processing.mailbox_id].state.value
                == "orphaned_mailbox"
            )
            assert (
                recovered_mailbox[blocked_mailbox.mailbox_id].state.value
                == "pending"
            )
            assert (
                recovered_mailbox[mailbox_waiting.mailbox_id].state.value
                == "pending"
            )
            assert all(
                item is not None and item.claim_token is None
                for item in recovered_mailbox.values()
            )

            recovered_media = {
                item.media_id: item
                for item in await restarted.list_outgoing_media(limit=10)
            }
            assert recovered_media[media_upload.media_id].state is MediaDeliveryState.UPLOAD_PENDING
            assert recovered_media[media_uploaded.media_id].state is MediaDeliveryState.UPLOADED
            assert recovered_media[media_send.media_id].state is MediaDeliveryState.SEND_PENDING
            assert all(item.claim_token is None for item in recovered_media.values())

            # Startup recovery is idempotent after the ownership fields clear.
            assert await restarted.startup_reconcile(now=base + timedelta(seconds=3)) == type(report)()
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_second_live_store_startup_reconcile_does_not_steal_healthy_leases(tmp_path):
    class Runtime:
        agent_id = "codex"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, *_args, **_kwargs):
            return None

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        root = tmp_path / "attachments"
        first = SQLiteStore(path, attachment_root=root)
        await first.initialize()
        base = datetime(2030, 1, 1, tzinfo=timezone.utc)
        second = None
        try:
            task = await first.create_task(_task("live-task"))
            task_claim = await first.claim_task_by_id(
                task.task_id, "live-task-worker", lease_seconds=3600, now=base
            )
            assert task_claim is not None
            assert await first.mark_task_running(
                task.task_id,
                task_claim.claim_token,
                execution_id=task_claim.execution_id,
                now=base,
            )

            outbox = await first.create_user_outbox(
                target=_target(), content="live delivery", now=base
            )
            outbox_claim = next(
                item
                for item in await first.claim_outbox(
                    "live-outbox-worker",
                    lease_seconds=3600,
                    automatic=False,
                    now=base,
                )
                if item.outbox_id == outbox.outbox_id
            )
            assert await first.mark_outbox_sending(
                outbox.outbox_id, outbox_claim.claim_token, now=base
            )

            blocked_mailbox = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="codex",
                content="blocked live mailbox",
                request_id="live-mailbox-blocked",
                now=base,
            )
            assert await first.claim_mailbox(
                "codex",
                "live-mailbox-worker",
                lease_seconds=3600,
                now=base,
            ) == []

            mailbox = await first.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="live mailbox",
                request_id="live-mailbox",
                now=base,
            )
            mailbox_claim = (
                await first.claim_mailbox(
                    "planner",
                    "live-mailbox-worker",
                    lease_seconds=3600,
                    now=base,
                )
            )[0]
            assert mailbox_claim.mailbox_id == mailbox.mailbox_id
            assert await first.mark_mailbox_processing(
                mailbox.mailbox_id, mailbox_claim.claim_token
            )

            attachment_store = AttachmentStore(root)
            attachment = attachment_store.put_bytes(
                b"live-media", attachment_id="live-media-file"
            )
            await first.register_attachment(attachment, kind="image")
            media = await first.create_outgoing_media(
                attachment_id=attachment.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="live-media",
                idempotency_key="live-media",
                now=base,
            )
            media_claim = (
                await first.claim_outgoing_media(
                    "live-media-worker", lease_seconds=3600, now=base
                )
            )[0]
            assert media_claim.media_id == media.media_id

            second = SQLiteStore(path, attachment_root=root)
            await second.initialize()
            report = await second.startup_reconcile(
                now=base + timedelta(seconds=1)
            )
            assert report == type(report)()
            assert (await first.get_task(task.task_id)).state.value == "running"
            assert (await first.get_outbox_item(outbox.outbox_id)).state.value == "sending"
            assert (await first.get_mailbox_item(mailbox.mailbox_id)).state.value == "processing"
            assert (
                await first.get_mailbox_item(blocked_mailbox.mailbox_id)
            ).state.value == "pending"
            assert (await first.get_outgoing_media(media.media_id)).state.value == "uploading"

            # The first owner has already consumed its empty open-time report.
            # Calling its startup facade again must also preserve live claims.
            assert await first.startup_reconcile(
                now=base + timedelta(seconds=2)
            ) == type(report)()
            assert (await first.get_task(task.task_id)).state.value == "running"

            # A second TaskManager start uses the same facade. It must not
            # reinterpret the observer connection as a process boundary.
            manager = TaskManager(second, runtime=Runtime(), worker_count=0)
            await manager.start()
            try:
                assert (await first.get_task(task.task_id)).state.value == "running"
                assert (await first.get_outbox_item(outbox.outbox_id)).state.value == "sending"
                assert (await first.get_mailbox_item(mailbox.mailbox_id)).state.value == "processing"
                assert (
                    await first.get_mailbox_item(blocked_mailbox.mailbox_id)
                ).state.value == "pending"
                assert (await first.get_outgoing_media(media.media_id)).state.value == "uploading"
            finally:
                await manager.stop()
                second = None
        finally:
            if second is not None:
                await second.close()
            await first.close()

    asyncio.run(scenario())


def test_reopen_after_last_close_establishes_a_new_startup_boundary(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        base = datetime(2030, 1, 1, tzinfo=timezone.utc)
        first = SQLiteStore(path)
        await first.initialize()
        task = await first.create_task(_task("reopen-task"))
        claim = await first.claim_task_by_id(
            task.task_id, "stopped-process", lease_seconds=3600, now=base
        )
        assert claim is not None
        assert await first.mark_task_running(
            task.task_id,
            claim.claim_token,
            execution_id=claim.execution_id,
            now=base,
        )
        await first.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            # Strong recovery happened before initialize published the reopened
            # connection, and startup_reconcile returns that cached report.
            assert (await reopened.get_task(task.task_id)).state.value == "orphaned"
            report = await reopened.startup_reconcile(
                now=base + timedelta(seconds=1)
            )
            assert report.tasks_orphaned == 1
            assert await reopened.startup_reconcile(
                now=base + timedelta(seconds=2)
            ) == type(report)()
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_in_memory_store_startup_report_is_consumed_once() -> None:
    async def scenario() -> None:
        store = SQLiteStore(":memory:")
        await store.initialize()
        try:
            first = await store.startup_reconcile()
            second = await store.startup_reconcile()
            assert first == type(first)()
            assert second == type(second)()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_failed_first_open_does_not_suppress_next_startup_recovery(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        base = datetime(2030, 1, 1, tzinfo=timezone.utc)
        creator = SQLiteStore(path)
        await creator.initialize()
        task = await creator.create_task(_task("failed-open-task"))
        claim = await creator.claim_task_by_id(
            task.task_id, "old-worker", lease_seconds=3600, now=base
        )
        assert claim is not None
        assert await creator.mark_task_running(
            task.task_id,
            claim.claim_token,
            execution_id=claim.execution_id,
            now=base,
        )
        await creator.close()

        original = SQLiteStore._recover_startup_state_tx

        def fail_recovery(self, conn, *, now_text):
            original(self, conn, now_text=now_text)
            raise RuntimeError("startup recovery failed")

        monkeypatch.setattr(SQLiteStore, "_recover_startup_state_tx", fail_recovery)
        failed = SQLiteStore(path)
        with pytest.raises(RuntimeError, match="startup recovery failed"):
            await failed.initialize()
        await failed.close()

        monkeypatch.setattr(SQLiteStore, "_recover_startup_state_tx", original)
        recovered = SQLiteStore(path)
        await recovered.initialize()
        try:
            report = await recovered.startup_reconcile(
                now=base + timedelta(seconds=1)
            )
            assert report.tasks_orphaned == 1
            assert (await recovered.get_task(task.task_id)).state.value == "orphaned"
        finally:
            await recovered.close()

    asyncio.run(scenario())


def test_reconcile_blocks_missing_managed_media_from_task_and_media_claims(tmp_path):
    async def scenario() -> None:
        root = tmp_path / "attachments"
        store = SQLiteStore(tmp_path / "runtime.sqlite", attachment_root=root)
        await store.initialize()
        try:
            attachment_store = AttachmentStore(root)
            task_attachment = attachment_store.put_bytes(b"task", attachment_id="missing-task-file")
            media_attachment = attachment_store.put_bytes(b"media", attachment_id="missing-media-file")
            await store.register_attachment(task_attachment, kind="file")
            await store.register_attachment(media_attachment, kind="image")
            task = await store.create_task(
                _task("task-with-missing-media", attachment_id=task_attachment.attachment_id)
            )
            media = await store.create_outgoing_media(
                attachment_id=media_attachment.attachment_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                agent_id="codex",
                media_id="media-with-missing-file",
                idempotency_key="media-with-missing-file",
            )
            task_attachment.local_path.unlink()
            media_attachment.local_path.unlink()

            report = await store.startup_reconcile()
            assert report.missing_attachments == 2
            assert not await store.can_access_attachment(
                task_attachment.attachment_id, agent_id="codex", task_id=task.task_id
            )
            assert await store.claim_task_by_id(task.task_id, "worker") is None
            assert await store.claim_outgoing_media("media-worker", limit=10) == []
            retained_media = await store.get_outgoing_media(media.media_id)
            assert retained_media is not None
            assert retained_media.state.value == "ready"
        finally:
            await store.close()

    asyncio.run(scenario())
