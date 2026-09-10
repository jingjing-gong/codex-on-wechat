"""Durable natural-language cron proposal and confirmation invariants."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.agents.base import AgentResult
from src.runtime.identity import thread_conversation_subject
from src.runtime.manager import TaskManager
from src.runtime.models import InboundMessage, ReplyTarget, TaskSteeringState
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import InvalidTransition, NotFoundError, StoreError


UTC = timezone.utc
START = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)


class _Runtime:
    agent_id = "codex"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(
            task_id=task.task_id,
            execution_id=task.execution_id,
            content="done",
        )

    async def interrupt(self, _task_id: str) -> bool:
        return False


class _Clock:
    def __init__(self, value: datetime = START) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


async def _manager(path: Path, clock: _Clock) -> TaskManager:
    store = SQLiteStore(path, clock=clock)
    registry = AgentRegistry()
    registry.register(
        "codex",
        _Runtime(),
        profile=codex_profile(
            profile_version=2,
            default_mode_id="execute",
        ),
    )
    manager = TaskManager(
        store,
        registry,
        worker_count=0,
        default_agent_id="codex",
        default_mode_id="execute",
        trusted_default_execute=True,
        cron_tick_interval=None,
    )
    await manager.start()
    return manager


def _wechat(message_id: str, text: str, *, bot_id: str = "wechat-bot") -> InboundMessage:
    return InboundMessage(
        channel="wechat",
        bot_id=bot_id,
        external_user_id="owner-user",
        external_message_id=message_id,
        text=text,
        session_id="default",
    )


def _lark_thread(
    message_id: str,
    text: str,
    *,
    bot_id: str = "cli_lark_bot",
    reply_to: str = "",
) -> InboundMessage:
    chat_id = "oc_natural_cron_chat"
    thread_id = "omt_natural_cron_thread"
    subject = thread_conversation_subject(
        "lark", bot_id, chat_id, thread_id
    )
    return InboundMessage(
        channel="lark",
        bot_id=bot_id,
        external_user_id="ou_owner_user",
        external_message_id=message_id,
        text=text,
        session_id="default",
        conversation_subject_id=subject.conversation_subject_id,
        conversation_subject_scope=subject.scope_key,
        conversation_subject_kind="thread",
        destination_kind="thread",
        destination_id=chat_id,
        thread_id=thread_id,
        root_message_id=thread_id,
        transport_metadata={
            "chat_id": chat_id,
            "chat_type": "group",
            "thread_id": thread_id,
            "root_id": thread_id,
            "reply_to": reply_to,
        },
    )


async def _start_task(
    manager: TaskManager,
    inbound: InboundMessage,
    *,
    worker_id: str,
) -> tuple[Any, Any, Any]:
    accepted = await manager.accept_inbound(inbound)
    assert accepted.task is not None
    claim = await manager.store.claim_task_by_id(
        accepted.task.task_id, worker_id
    )
    assert claim is not None
    assert await manager.store.mark_task_running(
        accepted.task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    task = await manager.get_task(accepted.task.task_id)
    assert task is not None
    return accepted, claim, task


async def _finish_task(
    manager: TaskManager,
    task: Any,
    claim: Any,
    *,
    content: str = "finished",
) -> None:
    await manager.store.complete_task(
        task.task_id,
        status="completed",
        events=[
            {
                "event_id": f"event-{task.task_id}",
                "execution_id": claim.execution_id,
                "visibility": "user",
                "priority": 1,
                "content": content,
            }
        ],
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )


async def _apply_steering(
    manager: TaskManager,
    accepted: Any,
    claim: Any,
    *,
    worker_id: str,
) -> None:
    assert accepted.steering is not None
    delivery = await manager.store.claim_next_task_steering(
        accepted.task.task_id,
        claim.execution_id,
        task_claim_token=claim.claim_token,
        claimed_by=worker_id,
        lease_seconds=60,
    )
    assert delivery is not None
    assert delivery.steering_id == accepted.steering.steering_id
    applied = await manager.store.mark_task_steering_applied(
        delivery.steering_id,
        claim_token=delivery.claim_token,
    )
    assert applied.state is TaskSteeringState.APPLIED


def test_proposal_requires_later_input_then_executes_and_routes_only_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "runtime.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("request", "每天检查部署状态并告诉我"),
                worker_id="source-worker",
            )

            def inject_stale_bridge_capability(conn):
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE tasks SET metadata_json=json_set("
                    "metadata_json,'$._process_agent_bridge_capability','stale'"
                    ") WHERE task_id=?",
                    (source.task_id,),
                )
                conn.commit()

            await manager.store._call(inject_stale_bridge_capability)
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1m",
                "check deployment health and summarize it",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            assert draft.state == "pending"
            assert draft.expires_at == clock.value + timedelta(minutes=15)
            assert await manager.store.get_cron_job(draft.job_id) is None
            assert "direct_user_request" not in draft.task_template["metadata"]
            assert "_process_agent_bridge_capability" not in (
                draft.task_template["metadata"]
            )

            with pytest.raises(PermissionError, match="later user input"):
                await manager.confirm_natural_cron(
                    source.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=source_claim.execution_id,
                    now=clock.value,
                )
            assert await manager.store.get_cron_job(draft.job_id) is None

            clock.value += timedelta(seconds=1)
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            _confirmation, confirm_claim, confirm_task = await _start_task(
                manager,
                _wechat("confirmation", "确认"),
                worker_id="confirm-worker",
            )
            clock.value += timedelta(seconds=1)
            result = await manager.confirm_natural_cron(
                confirm_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=confirm_claim.execution_id,
                now=clock.value,
            )
            assert result["draft"].state == "confirmed"
            job = result["job"]
            assert job.job_id == draft.job_id
            assert job.expires_at is None
            assert job.prompt == "check deployment health and summarize it"
            assert "direct_user_request" not in job.task_template["metadata"]
            assert "_process_agent_bridge_capability" not in (
                job.task_template["metadata"]
            )
            assert await manager.store.list_outbox(limit=100) == [
                item
                for item in await manager.store.list_outbox(limit=100)
                if item.task_id in {source.task_id, confirm_task.task_id}
            ]
            await _finish_task(manager, confirm_task, confirm_claim)

            clock.value = draft.next_fire_at
            fired = await manager.store.fire_next_due_cron_job(now=clock.value)
            assert fired is not None
            assert fired.task.inputs == {
                "text": "check deployment health and summarize it"
            }
            assert fired.reminder is None
            assert fired.firing.outbox_id is None
            assert not [
                item
                for item in await manager.store.list_outbox(limit=100)
                if item.task_id == fired.task.task_id
            ]

            fire_claim = await manager.store.claim_task_by_id(
                fired.task.task_id, "cron-worker"
            )
            assert fire_claim is not None
            assert await manager.store.mark_task_running(
                fired.task.task_id,
                fire_claim.claim_token,
                execution_id=fire_claim.execution_id,
            )
            await manager.store.complete_task(
                fired.task.task_id,
                status="completed",
                events=[
                    {
                        "event_id": "natural-cron-result",
                        "execution_id": fire_claim.execution_id,
                        "visibility": "user",
                        "priority": 1,
                        "content": "deployment is healthy",
                    }
                ],
                claim_token=fire_claim.claim_token,
                execution_id=fire_claim.execution_id,
            )
            result_rows = [
                item
                for item in await manager.store.list_outbox(limit=100)
                if item.task_id == fired.task.task_id
            ]
            assert len(result_rows) == 1
            assert result_rows[0].content == "deployment is healthy"
            assert result_rows[0].bot_id == "wechat-bot"
            assert result_rows[0].external_user_id == "owner-user"
            assert not result_rows[0].foreground
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_applied_steering_can_confirm_but_pending_steering_cannot(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "runtime.sqlite", clock)
        try:
            _accepted, claim, task = await _start_task(
                manager,
                _wechat("request", "一分钟后提醒我"),
                worker_id="steering-worker",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                task.task_id,
                "every 1m",
                "perform the scheduled task",
                required_execution_id=claim.execution_id,
                now=clock.value,
            )
            clock.value += timedelta(seconds=1)
            confirmation = await manager.accept_inbound(
                _wechat("confirmation", "确认")
            )
            assert confirmation.steering is not None
            with pytest.raises(PermissionError, match="later user input"):
                await manager.confirm_natural_cron(
                    task.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=claim.execution_id,
                    now=clock.value,
                )
            await _apply_steering(
                manager,
                confirmation,
                claim,
                worker_id="steering-worker",
            )
            confirmed = await manager.confirm_natural_cron(
                task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=claim.execution_id,
                now=clock.value,
            )
            assert confirmed["draft"].confirmation_steering_id == (
                confirmation.steering.steering_id
            )
            assert confirmed["draft"].state == "confirmed"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_lark_confirmation_ignores_per_message_reply_parent_but_freezes_thread(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "runtime.sqlite", clock)
        try:
            await manager.store.create_bot_profile(
                {
                    "profile_id": "natural-lark",
                    "channel": "lark",
                    "bot_id": "cli_lark_bot",
                    "brand": "feishu",
                    "config_dir": str(tmp_path / "lark-config"),
                    "config_dir_identity": "device:natural-lark",
                    "cli_version": "1.0.0",
                    "credential_ref": "keychain:natural-lark",
                    "mention_policy": "direct_or_mention",
                    "access_policy": "all",
                    "restart_policy": {},
                }
            )
            _accepted, source_claim, source = await _start_task(
                manager,
                _lark_thread(
                    "om_request",
                    "每天汇总这个话题",
                    reply_to="om_old_parent",
                ),
                worker_id="lark-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "summarize this topic",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            assert "reply_to" not in draft.origin_reply_target.transport_metadata
            clock.value += timedelta(seconds=1)
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            _accepted, confirm_claim, confirm_task = await _start_task(
                manager,
                _lark_thread(
                    "om_confirm",
                    "confirm",
                    reply_to="om_proposal_reply",
                ),
                worker_id="lark-confirm",
            )
            confirmed = await manager.confirm_natural_cron(
                confirm_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=confirm_claim.execution_id,
                now=clock.value,
            )
            target: ReplyTarget = confirmed["job"].origin_reply_target
            assert target.bot_id == "cli_lark_bot"
            assert target.destination_id == "oc_natural_cron_chat"
            assert target.thread_id == "omt_natural_cron_thread"
            assert target.root_message_id == "omt_natural_cron_thread"
            assert target.source_message_id == "om_request"
            assert "reply_to" not in target.transport_metadata
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_confirmation_rolls_back_job_insert_and_can_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "atomic.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("atomic-request", "每分钟检查一次"),
                worker_id="atomic-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1m",
                "perform the atomic check",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            _accepted, confirm_claim, confirm_task = await _start_task(
                manager,
                _wechat("atomic-confirm", "确认"),
                worker_id="atomic-confirm",
            )

            original = SQLiteStore._insert_natural_cron_job_tx.__func__

            def insert_then_fail(cls, conn, *, draft, now_text):
                original(cls, conn, draft=draft, now_text=now_text)
                raise RuntimeError("injected response construction failure")

            monkeypatch.setattr(
                SQLiteStore,
                "_insert_natural_cron_job_tx",
                classmethod(insert_then_fail),
            )
            with pytest.raises(RuntimeError, match="injected"):
                await manager.confirm_natural_cron(
                    confirm_task.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=confirm_claim.execution_id,
                    now=clock.value,
                )
            rolled_back = await manager.store.get_natural_cron_draft(
                draft.draft_id
            )
            assert rolled_back is not None and rolled_back.state == "pending"
            assert await manager.store.get_cron_job(draft.job_id) is None

            monkeypatch.setattr(
                SQLiteStore,
                "_insert_natural_cron_job_tx",
                classmethod(original),
            )
            confirmed = await manager.confirm_natural_cron(
                confirm_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=confirm_claim.execution_id,
                now=clock.value,
            )
            assert confirmed["draft"].state == "confirmed"
            assert confirmed["job"].job_id == draft.job_id
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_confirmation_is_idempotent_after_reopen_and_response_loss(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "reopen.sqlite"
        clock = _Clock()
        manager = await _manager(path, clock)
        _accepted, source_claim, source = await _start_task(
            manager,
            _wechat("reopen-request", "每小时运行健康检查"),
            worker_id="reopen-source",
        )
        clock.value += timedelta(seconds=1)
        draft = await manager.propose_natural_cron(
            source.task_id,
            "every 1h",
            "run the durable health check",
            required_execution_id=source_claim.execution_id,
            now=clock.value,
        )
        await _finish_task(manager, source, source_claim)
        clock.value += timedelta(seconds=1)
        _accepted, confirm_claim, confirm_task = await _start_task(
            manager,
            _wechat("reopen-confirm", "确认"),
            worker_id="reopen-confirm",
        )
        first = await manager.confirm_natural_cron(
            confirm_task.task_id,
            draft_id=draft.draft_id,
            required_execution_id=confirm_claim.execution_id,
            now=clock.value,
        )
        assert first["job"].job_id == draft.job_id
        await _finish_task(manager, confirm_task, confirm_claim)
        await manager.stop()

        reopened = await _manager(path, clock)
        try:
            clock.value += timedelta(seconds=1)
            _accepted, retry_claim, retry_task = await _start_task(
                reopened,
                _wechat(
                    "reopen-confirm-retry",
                    f"确认 {draft.draft_id}",
                ),
                worker_id="reopen-retry",
            )
            replay = await reopened.confirm_natural_cron(
                retry_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=retry_claim.execution_id,
                now=clock.value,
            )
            assert replay["job"].job_id == draft.job_id
            count = await reopened.store._call(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) FROM cron_jobs WHERE job_id=?",
                    (draft.job_id,),
                ).fetchone()[0]
            )
            assert count == 1
        finally:
            await reopened.stop()

    asyncio.run(scenario())


def test_confirmation_uses_frozen_template_after_session_preferences_change(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "frozen.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("frozen-request", "每天生成报告"),
                worker_id="frozen-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1d",
                "generate the frozen report",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            frozen = dict(draft.task_template)
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            _accepted, confirm_claim, confirm_task = await _start_task(
                manager,
                _wechat("frozen-confirm", "确认"),
                worker_id="frozen-confirm",
            )

            await manager.store.set_session_mode(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                session_id="default",
                agent_id="codex",
                mode_id="chat",
                policy_version=1,
                now=clock.value,
            )
            await manager.store.set_session_model_preference(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                session_id="default",
                agent_id="codex",
                model_id="future-model",
                reasoning_effort="low",
                now=clock.value,
            )
            stat = tmp_path.stat()
            await manager.store.set_session_working_directory(
                ".",
                absolute_path=str(tmp_path),
                directory_device=stat.st_dev,
                directory_inode=stat.st_ino,
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                session_id="default",
                agent_id="codex",
                updated_by="test",
                now=clock.value,
            )
            confirmed = await manager.confirm_natural_cron(
                confirm_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=confirm_claim.execution_id,
                now=clock.value,
            )
            assert confirmed["job"].task_template == frozen
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_recurring_job_survives_draft_ttl_and_schema_reopen(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "recurring.sqlite"
        clock = _Clock()
        manager = await _manager(path, clock)
        _accepted, source_claim, source = await _start_task(
            manager,
            _wechat("recurring-request", "每分钟执行检查"),
            worker_id="recurring-source",
        )
        clock.value += timedelta(seconds=1)
        draft = await manager.propose_natural_cron(
            source.task_id,
            "every 1m",
            "execute the recurring check",
            required_execution_id=source_claim.execution_id,
            now=clock.value,
        )
        await _finish_task(manager, source, source_claim)
        clock.value += timedelta(seconds=1)
        _accepted, claim, task = await _start_task(
            manager,
            _wechat("recurring-confirm", "确认"),
            worker_id="recurring-confirm",
        )
        confirmed = await manager.confirm_natural_cron(
            task.task_id,
            draft_id=draft.draft_id,
            required_execution_id=claim.execution_id,
            now=clock.value,
        )
        assert confirmed["job"].expires_at is None
        await _finish_task(manager, task, claim)
        await manager.stop()

        clock.value = draft.expires_at + timedelta(minutes=1)
        reopened = await _manager(path, clock)
        try:
            job = await reopened.store.get_cron_job(draft.job_id)
            assert job is not None and job.enabled
            assert job.expires_at is None
            fired = await reopened.store.fire_next_due_cron_job(now=clock.value)
            assert fired is not None and fired.firing.job_id == draft.job_id
            updated = await reopened.store.get_cron_job(draft.job_id)
            assert updated is not None and updated.enabled
            assert updated.next_fire_at is not None
            assert updated.next_fire_at > clock.value
        finally:
            await reopened.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["confirm", "cancel"])
def test_rejected_resolution_durably_expires_overdue_draft(
    tmp_path: Path,
    action: str,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / f"expired-{action}.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat(f"expired-{action}-request", "稍后执行"),
                worker_id=f"expired-{action}-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "this proposal must expire",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            await _finish_task(manager, source, source_claim)
            clock.value = draft.expires_at + timedelta(seconds=1)
            text = "确认" if action == "confirm" else "取消"
            _accepted, claim, task = await _start_task(
                manager,
                _wechat(f"expired-{action}-resolution", text),
                worker_id=f"expired-{action}-resolution",
            )
            method = (
                manager.confirm_natural_cron
                if action == "confirm"
                else manager.cancel_natural_cron
            )
            with pytest.raises(InvalidTransition, match="expired"):
                await method(
                    task.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=claim.execution_id,
                    now=clock.value,
                )
            persisted = await manager.store.get_natural_cron_draft(
                draft.draft_id
            )
            assert persisted is not None and persisted.state == "expired"
            assert persisted.resolved_at == clock.value
            assert await manager.store.get_cron_job(draft.job_id) is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_proposal_retry_after_resolution_is_not_reported_as_a_new_draft(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "resolved-retry.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("resolved-retry-request", "每小时执行"),
                worker_id="resolved-retry-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "resolved proposal",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            clock.value += timedelta(seconds=1)
            confirmation = await manager.accept_inbound(
                _wechat("resolved-retry-confirm", "确认")
            )
            await _apply_steering(
                manager,
                confirmation,
                source_claim,
                worker_id="resolved-retry-source",
            )
            await manager.confirm_natural_cron(
                source.task_id,
                draft_id=draft.draft_id,
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            with pytest.raises(InvalidTransition, match="already confirmed"):
                await manager.store.create_natural_cron_draft(
                    draft_id=draft.draft_id,
                    job_id=draft.job_id,
                    source_task_id=draft.source_task_id,
                    source_execution_id=draft.source_execution_id,
                    schedule_kind=draft.schedule_kind,
                    schedule_expression=draft.schedule_expression,
                    timezone_name=draft.timezone_name,
                    prompt=draft.prompt,
                    next_fire_at=draft.next_fire_at,
                    created_at=draft.created_at,
                    expires_at=draft.expires_at,
                    now=clock.value,
                )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_successful_cancellation_never_materializes_a_job(tmp_path: Path) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "cancel.sqlite", clock)
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("cancel-request", "每天执行"),
                worker_id="cancel-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1d",
                "cancelled work",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            _accepted, claim, task = await _start_task(
                manager,
                _wechat("cancel-resolution", f"取消 {draft.draft_id}"),
                worker_id="cancel-resolution",
            )
            cancelled = await manager.cancel_natural_cron(
                task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=claim.execution_id,
                now=clock.value,
            )
            assert cancelled.state == "cancelled"
            assert cancelled.confirmation_inbound_message_id == (
                task.inbound_message_id
            )
            assert await manager.store.get_cron_job(draft.job_id) is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_applied_confirmation_steering_survives_source_execution_retry(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(tmp_path / "execution-retry.sqlite", clock)
        try:
            _accepted, first_claim, source = await _start_task(
                manager,
                _wechat("execution-retry-request", "每小时检查"),
                worker_id="first-worker",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "check after execution retry",
                required_execution_id=first_claim.execution_id,
                now=clock.value,
            )
            await manager.store.complete_task(
                source.task_id,
                status="failed",
                error="injected retry",
                claim_token=first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            clock.value += timedelta(seconds=1)
            retried = await manager.store.retry_task(
                source.task_id,
                actor="test",
                now=clock.value,
            )
            assert retried is not None
            second_claim = await manager.store.claim_task_by_id(
                source.task_id, "second-worker"
            )
            assert second_claim is not None
            assert second_claim.execution_id != first_claim.execution_id
            assert await manager.store.mark_task_running(
                source.task_id,
                second_claim.claim_token,
                execution_id=second_claim.execution_id,
            )
            clock.value += timedelta(seconds=1)
            confirmation = await manager.accept_inbound(
                _wechat("execution-retry-confirm", f"确认 {draft.draft_id}")
            )
            await _apply_steering(
                manager,
                confirmation,
                second_claim,
                worker_id="second-worker",
            )
            confirmed = await manager.confirm_natural_cron(
                source.task_id,
                draft_id=draft.draft_id,
                required_execution_id=second_claim.execution_id,
                now=clock.value,
            )
            assert confirmed["draft"].confirmation_execution_id == (
                second_claim.execution_id
            )
            assert confirmed["draft"].confirmation_steering_id == (
                confirmation.steering.steering_id
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("other_channel", ["wechat", "lark"])
def test_same_principal_cannot_confirm_from_another_bot_or_channel(
    tmp_path: Path,
    other_channel: str,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(
            tmp_path / f"cross-{other_channel}.sqlite", clock
        )
        try:
            await manager.store.create_principal(principal_id="owner")
            await manager.store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                identifier_kind="from_user_id",
                configured_by="test",
            )
            if other_channel == "lark":
                await manager.store.create_bot_profile(
                    {
                        "profile_id": "cross-lark",
                        "channel": "lark",
                        "bot_id": "cli_lark_bot",
                        "brand": "feishu",
                        "config_dir": str(tmp_path / "cross-lark-config"),
                        "config_dir_identity": "device:cross-lark",
                        "cli_version": "1.0.0",
                        "credential_ref": "keychain:cross-lark",
                        "mention_policy": "direct_or_mention",
                        "access_policy": "all",
                        "restart_policy": {},
                    }
                )
                await manager.store.map_principal_account(
                    principal_id="owner",
                    channel="lark",
                    bot_id="cli_lark_bot",
                    external_user_id="ou_owner_user",
                    identifier_kind="open_id",
                    configured_by="test",
                )
                attacker = _lark_thread(
                    "cross-confirm", "confirm placeholder"
                )
            else:
                await manager.store.map_principal_account(
                    principal_id="owner",
                    channel="wechat",
                    bot_id="wechat-other",
                    external_user_id="owner-user",
                    identifier_kind="from_user_id",
                    configured_by="test",
                )
                attacker = _wechat(
                    "cross-confirm", "confirm placeholder", bot_id="wechat-other"
                )
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("cross-request", "每小时检查"),
                worker_id="cross-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "cross-scope work",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            await _finish_task(manager, source, source_claim)
            attacker = replace(attacker, text=f"确认 {draft.draft_id}")
            clock.value += timedelta(seconds=1)
            _accepted, claim, task = await _start_task(
                manager, attacker, worker_id="cross-attacker"
            )
            with pytest.raises(NotFoundError, match="not found"):
                await manager.confirm_natural_cron(
                    task.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=claim.execution_id,
                    now=clock.value,
                )
            assert await manager.store.get_cron_job(draft.job_id) is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_mapping_revision_change_rejects_confirmation_but_reopens_cleanly(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "mapping-change.sqlite"
        clock = _Clock()
        manager = await _manager(path, clock)
        await manager.store.create_principal(principal_id="owner-old")
        await manager.store.create_principal(principal_id="owner-new")
        await manager.store.map_principal_account(
            principal_id="owner-old",
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="owner-user",
            identifier_kind="from_user_id",
            configured_by="test",
        )
        _accepted, source_claim, source = await _start_task(
            manager,
            _wechat("mapping-request", "每小时检查"),
            worker_id="mapping-source",
        )
        clock.value += timedelta(seconds=1)
        draft = await manager.propose_natural_cron(
            source.task_id,
            "every 1h",
            "mapping-sensitive work",
            required_execution_id=source_claim.execution_id,
            now=clock.value,
        )
        await _finish_task(manager, source, source_claim)
        replacement = await manager.store.map_principal_account(
            principal_id="owner-new",
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="owner-user",
            identifier_kind="from_user_id",
            configured_by="test",
        )
        assert replacement.mapping_revision == 2
        clock.value += timedelta(seconds=1)
        _accepted, claim, task = await _start_task(
            manager,
            _wechat("mapping-confirm", f"确认 {draft.draft_id}"),
            worker_id="mapping-confirm",
        )
        with pytest.raises((NotFoundError, PermissionError)):
            await manager.confirm_natural_cron(
                task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=claim.execution_id,
                now=clock.value,
            )
        await manager.stop()

        reopened = SQLiteStore(path, clock=clock)
        await reopened.initialize()
        try:
            persisted = await reopened.get_natural_cron_draft(draft.draft_id)
            assert persisted is not None and persisted.state == "pending"
            assert await reopened.get_cron_job(draft.job_id) is None
        finally:
            await reopened.close()

        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE natural_cron_drafts "
                "SET principal_mapping_revision=99 WHERE draft_id=?",
                (draft.draft_id,),
            )
        tampered = SQLiteStore(path, clock=clock)
        with pytest.raises(StoreError, match="principal_mapping_revision"):
            await tampered.initialize()

    asyncio.run(scenario())


@pytest.mark.parametrize("first_action", ["confirm", "cancel"])
def test_concurrent_confirm_and_cancel_obeys_inbound_order(
    tmp_path: Path,
    first_action: str,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(
            tmp_path / f"resolution-race-{first_action}.sqlite", clock
        )
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat("race-request", "每小时检查"),
                worker_id="race-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "race-safe work",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            clock.value += timedelta(seconds=1)
            action_order = (
                ("confirm", "cancel")
                if first_action == "confirm"
                else ("cancel", "confirm")
            )
            accepted = {}
            for resolution_action in action_order:
                text = "确认" if resolution_action == "confirm" else "取消"
                accepted[resolution_action] = await manager.accept_inbound(
                    _wechat(
                        f"race-{first_action}-{resolution_action}",
                        f"{text} {draft.draft_id}",
                    )
                )
            for resolution_action in action_order:
                await _apply_steering(
                    manager,
                    accepted[resolution_action],
                    source_claim,
                    worker_id="race-source",
                )

            results = await asyncio.gather(
                manager.confirm_natural_cron(
                    source.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=source_claim.execution_id,
                    now=clock.value,
                ),
                manager.cancel_natural_cron(
                    source.task_id,
                    draft_id=draft.draft_id,
                    required_execution_id=source_claim.execution_id,
                    now=clock.value,
                ),
                return_exceptions=True,
            )
            assert sum(not isinstance(result, BaseException) for result in results) == 1
            resolved = await manager.store.get_natural_cron_draft(draft.draft_id)
            assert resolved is not None
            job = await manager.store.get_cron_job(draft.job_id)
            expected_state = (
                "confirmed" if first_action == "confirm" else "cancelled"
            )
            assert resolved.state == expected_state
            assert (job is not None) is (first_action == "confirm")
        finally:
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("first_action", ["confirm", "cancel"])
def test_distinct_resolution_tasks_obey_durable_inbound_order(
    tmp_path: Path,
    first_action: str,
) -> None:
    async def scenario() -> None:
        clock = _Clock()
        manager = await _manager(
            tmp_path / f"direct-order-{first_action}.sqlite", clock
        )
        try:
            _accepted, source_claim, source = await _start_task(
                manager,
                _wechat(f"direct-{first_action}-request", "每小时检查"),
                worker_id="direct-order-source",
            )
            clock.value += timedelta(seconds=1)
            draft = await manager.propose_natural_cron(
                source.task_id,
                "every 1h",
                "direct order work",
                required_execution_id=source_claim.execution_id,
                now=clock.value,
            )
            await _finish_task(manager, source, source_claim)
            clock.value += timedelta(seconds=1)
            action_order = (
                ("confirm", "cancel")
                if first_action == "confirm"
                else ("cancel", "confirm")
            )
            accepted = {}
            for resolution_action in action_order:
                text = "确认" if resolution_action == "confirm" else "取消"
                accepted[resolution_action] = await manager.accept_inbound(
                    _wechat(
                        f"direct-{first_action}-{resolution_action}",
                        f"{text} {draft.draft_id}",
                    )
                )

            later_action = action_order[1]
            later_task = accepted[later_action].task
            assert later_task is not None
            later_claim = await manager.store.claim_task_by_id(
                later_task.task_id, "direct-order-later"
            )
            assert later_claim is None

            earlier_action = action_order[0]
            earlier_task = accepted[earlier_action].task
            assert earlier_task is not None
            earlier_claim = await manager.store.claim_task_by_id(
                earlier_task.task_id, "direct-order-earlier"
            )
            assert earlier_claim is not None
            assert await manager.store.mark_task_running(
                earlier_task.task_id,
                earlier_claim.claim_token,
                execution_id=earlier_claim.execution_id,
            )
            earlier_method = (
                manager.confirm_natural_cron
                if earlier_action == "confirm"
                else manager.cancel_natural_cron
            )
            await earlier_method(
                earlier_task.task_id,
                draft_id=draft.draft_id,
                required_execution_id=earlier_claim.execution_id,
                now=clock.value,
            )
            resolved = await manager.store.get_natural_cron_draft(
                draft.draft_id
            )
            assert resolved is not None
            expected_state = (
                "confirmed" if first_action == "confirm" else "cancelled"
            )
            assert resolved.state == expected_state
            job = await manager.store.get_cron_job(draft.job_id)
            assert (job is not None) is (first_action == "confirm")
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_schema_reopen_rejects_confirmed_job_snapshot_drift(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "drift.sqlite"
        clock = _Clock()
        manager = await _manager(path, clock)
        _accepted, source_claim, source = await _start_task(
            manager,
            _wechat("drift-request", "每小时检查"),
            worker_id="drift-source",
        )
        clock.value += timedelta(seconds=1)
        draft = await manager.propose_natural_cron(
            source.task_id,
            "every 1h",
            "original immutable prompt",
            required_execution_id=source_claim.execution_id,
            now=clock.value,
        )
        await _finish_task(manager, source, source_claim)
        clock.value += timedelta(seconds=1)
        _accepted, claim, task = await _start_task(
            manager,
            _wechat("drift-confirm", "确认"),
            worker_id="drift-confirm",
        )
        await manager.confirm_natural_cron(
            task.task_id,
            draft_id=draft.draft_id,
            required_execution_id=claim.execution_id,
            now=clock.value,
        )
        await manager.stop()

        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE cron_jobs SET prompt='tampered' WHERE job_id=?",
                (draft.job_id,),
            )
        reopened = SQLiteStore(path, clock=clock)
        with pytest.raises(StoreError, match="conflicts with its draft"):
            await reopened.initialize()

    asyncio.run(scenario())
