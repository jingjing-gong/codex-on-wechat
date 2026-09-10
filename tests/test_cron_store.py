"""Durable cron persistence, ownership, and atomic firing invariants."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.agents.base import AgentResult
from src.runtime.models import AgentTask, ReplyTarget, TaskState
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import InvalidTransition, NotFoundError, StoreError
from src.runtime.worker import TaskWorker


UTC = timezone.utc
START = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)


async def _add_job(
    store: SQLiteStore,
    *,
    job_id: str = "cron-job-1",
    now: datetime = START,
    schedule_kind: str = "interval",
    schedule_expression: str = "1m",
    target: ReplyTarget | None = None,
    principal_id: str | None = None,
    principal_account_id: str | None = None,
    principal_mapping_revision: int | None = None,
    agent_incarnation: int | None = None,
    expires_at: datetime | None = None,
):
    target = target or ReplyTarget(
        channel="wechat",
        bot_id="wechat-bot",
        external_user_id="owner-user",
        session_id="default",
        source_message_id="stale-message",
        context_token="stale-context",
        conversation_subject_scope="owner-user",
        destination_kind="direct",
        destination_id="owner-user",
    )
    return await store.create_cron_job(
        job_id=job_id,
        principal_id=principal_id,
        principal_account_id=principal_account_id,
        principal_mapping_revision=principal_mapping_revision,
        origin_channel=target.channel,
        origin_bot_id=target.bot_id,
        origin_external_user_id=target.external_user_id,
        origin_conversation_subject_scope=(
            target.conversation_subject_scope or target.external_user_id
        ),
        origin_session_id=target.session_id,
        origin_reply_target=target,
        agent_id="codex",
        agent_incarnation=agent_incarnation,
        schedule_kind=schedule_kind,
        schedule_expression=schedule_expression,
        prompt="perform the scheduled work",
        expires_at=expires_at,
        created_at=now,
    )


def test_schema_v40_and_create_is_idempotent_but_conflicts_fail(tmp_path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite3"
        store = SQLiteStore(path, clock=lambda: START)
        await store.initialize()
        try:
            first = await _add_job(store)
            replay = await _add_job(store)
            assert replay == first
            assert first.next_fire_at == START + timedelta(minutes=1)
            assert first.task_template["inputs"] == {
                "text": "perform the scheduled work"
            }
            with pytest.raises(StoreError, match="identity conflicts"):
                await store.create_cron_job(
                    job_id=first.job_id,
                    origin_channel="wechat",
                    origin_bot_id="wechat-bot",
                    origin_external_user_id="owner-user",
                    origin_conversation_subject_scope="owner-user",
                    origin_reply_target=first.origin_reply_target,
                    agent_id="codex",
                    schedule_kind="interval",
                    schedule_expression="2m",
                    prompt=first.prompt,
                    created_at=START,
                )
        finally:
            await store.close()
        with sqlite3.connect(path) as conn:
            assert conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0] == 41
            assert {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('cron_jobs','cron_firings')"
                )
            } == {"cron_jobs", "cron_firings"}

    asyncio.run(scenario())


def test_cron_create_fences_the_captured_agent_incarnation(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            lifecycle = await store.get_agent_lifecycle("codex")
            assert lifecycle is not None
            await store.retire_agent("codex")
            with pytest.raises(InvalidTransition, match="not enabled"):
                await _add_job(
                    store,
                    job_id="stale-incarnation",
                    agent_incarnation=lifecycle.agent_incarnation,
                )
            assert await store.get_cron_job("stale-incarnation") is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cron_list_order_is_deterministic_and_validated(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            for job_id in ("cron-b", "cron-a", "cron-c"):
                await _add_job(store, job_id=job_id)

            ascending = await store.list_cron_jobs(order="asc")
            descending = await store.list_cron_jobs(order="desc")
            assert [job.job_id for job in ascending] == [
                "cron-a",
                "cron-b",
                "cron-c",
            ]
            assert [job.job_id for job in descending] == [
                "cron-c",
                "cron-b",
                "cron-a",
            ]
            with pytest.raises(ValueError, match="asc or desc"):
                await store.list_cron_jobs(order="newest")
        finally:
            await store.close()

    asyncio.run(scenario())


def test_due_fire_atomically_creates_only_task_and_advance(tmp_path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite3"
        store = SQLiteStore(path, clock=lambda: START)
        await store.initialize()
        job_id = ""
        try:
            job = await _add_job(store)
            job_id = job.job_id
            result = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            )
            assert result is not None
            assert result.job.job_id == job.job_id
            assert result.task.state is TaskState.QUEUED
            assert result.task.inputs == {"text": job.prompt}
            assert result.task.reply_target.source_message_id is None
            assert result.task.reply_target.context_token is None
            assert result.reminder is None
            assert result.firing.outbox_id is None
            assert await store.list_outbox(limit=100) == []
            assert result.job.next_fire_at == START + timedelta(minutes=2)
            assert len(await store.list_cron_firings(job.job_id)) == 1
            assert await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            ) is None
        finally:
            await store.close()

        # A second v40 open must use the nullable-firing validator rather than
        # reapplying v39's required-reminder assumptions.
        reopened = SQLiteStore(path, clock=lambda: START)
        await reopened.initialize()
        try:
            firings = await reopened.list_cron_firings(job_id)
            assert len(firings) == 1
            assert firings[0].outbox_id is None
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_v39_upgrade_suppresses_prompt_echoes_and_repairs_unseen_results(
    tmp_path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite3"
        store = SQLiteStore(path, clock=lambda: START)
        await store.initialize()
        prompt_rows: dict[str, str] = {}
        firings: dict[str, object] = {}
        for suffix in ("pending", "unknown", "sent"):
            await _add_job(store, job_id=f"cron-{suffix}")
        for suffix in ("pending", "unknown", "sent"):
            fired = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            )
            assert fired is not None
            firings[fired.job.job_id] = fired
            prompt = await store.create_user_outbox(
                outbox_id=f"legacy-prompt-{suffix}",
                target=fired.task.reply_target,
                content=fired.job.prompt,
                agent_id=fired.job.agent_id,
                priority=2,
                delivery_mode="push_eligible",
                notify_enabled=True,
                foreground=False,
            )
            prompt_rows[suffix] = prompt.outbox_id

        pending_firing = firings["cron-pending"]
        result = await store.create_user_outbox(
            outbox_id="legacy-cron-result",
            target=pending_firing.task.reply_target,
            content="computed scheduled result",
            task_id=pending_firing.task.task_id,
            agent_id=pending_firing.job.agent_id,
            priority=1,
            delivery_mode="push_eligible",
            notify_enabled=False,
            foreground=False,
        )
        for fired in firings.values():
            assert await store.request_cancel(fired.task.task_id)
        await store.close()

        # Recreate the exact v39 NOT NULL firing boundary and attach its eager
        # prompt reminders.  The surrounding DB was produced by the real
        # migration chain, so every referenced task/job/outbox is genuine.
        with sqlite3.connect(path) as conn:
            for suffix in ("pending", "unknown", "sent"):
                fired = firings[f"cron-{suffix}"]
                conn.execute(
                    "UPDATE cron_firings SET outbox_id=? WHERE firing_id=?",
                    (prompt_rows[suffix], fired.firing.firing_id),
                )
            conn.execute(
                "UPDATE user_outbox SET state='delivery_unknown' "
                "WHERE outbox_id=?",
                (prompt_rows["unknown"],),
            )
            conn.execute(
                "UPDATE user_outbox SET state='sent',sent_at=? "
                "WHERE outbox_id=?",
                (
                    "2026-09-08T01:01:00.000000+00:00",
                    prompt_rows["sent"],
                ),
            )
            conn.execute("DROP INDEX idx_cron_firings_job")
            conn.execute("ALTER TABLE cron_firings RENAME TO cron_firings_v40")
            conn.execute(
                """CREATE TABLE cron_firings (
                       firing_id TEXT PRIMARY KEY,
                       job_id TEXT NOT NULL,
                       scheduled_for TEXT NOT NULL,
                       task_id TEXT NOT NULL UNIQUE,
                       outbox_id TEXT NOT NULL UNIQUE,
                       fired_at TEXT NOT NULL,
                       FOREIGN KEY (job_id) REFERENCES cron_jobs(job_id)
                           ON DELETE RESTRICT,
                       FOREIGN KEY (task_id) REFERENCES tasks(task_id)
                           ON DELETE RESTRICT,
                       FOREIGN KEY (outbox_id) REFERENCES user_outbox(outbox_id)
                           ON DELETE RESTRICT,
                       UNIQUE (job_id, scheduled_for)
                   )"""
            )
            conn.execute(
                """INSERT INTO cron_firings
                       (firing_id,job_id,scheduled_for,task_id,outbox_id,fired_at)
                   SELECT firing_id,job_id,scheduled_for,task_id,outbox_id,fired_at
                     FROM cron_firings_v40"""
            )
            conn.execute("DROP TABLE cron_firings_v40")
            conn.execute(
                "CREATE INDEX idx_cron_firings_job "
                "ON cron_firings(job_id,scheduled_for,firing_id)"
            )
            conn.execute(
                "DELETE FROM schema_migrations WHERE version IN (40,41)"
            )
            conn.commit()

        upgraded = SQLiteStore(path, clock=lambda: START)
        await upgraded.initialize()
        try:
            assert await upgraded._call(
                lambda conn: conn.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0]
            ) == 41
            outbox_not_null = await upgraded._call(
                lambda conn: next(
                    row[3]
                    for row in conn.execute("PRAGMA table_info(cron_firings)")
                    if row[1] == "outbox_id"
                )
            )
            assert outbox_not_null == 0

            pending = await upgraded.get_outbox_item(prompt_rows["pending"])
            assert pending is not None
            assert pending.state.value == "failed_permanent"
            assert pending.delivery_mode.value == "inbox_only"
            assert not pending.notify_enabled
            assert pending.presentation.value == "acknowledged"
            assert not await upgraded.retry_outbox(pending.outbox_id)

            unknown = await upgraded.get_outbox_item(prompt_rows["unknown"])
            assert unknown is not None
            assert unknown.state.value == "delivery_unknown"
            assert unknown.delivery_mode.value == "inbox_only"
            assert not unknown.notify_enabled
            assert unknown.presentation.value == "acknowledged"
            assert not await upgraded.retry_outbox(unknown.outbox_id)

            sent = await upgraded.get_outbox_item(prompt_rows["sent"])
            assert sent is not None
            assert sent.state.value == "sent"
            assert sent.delivery_mode.value == "push_eligible"
            assert sent.notify_enabled

            repaired = await upgraded.get_outbox_item(result.outbox_id)
            assert repaired is not None
            assert repaired.state.value == "pending"
            assert repaired.presentation.value == "unseen"
            assert repaired.notify_enabled
            claimed = await upgraded.claim_account_outbox(
                "v40-result-worker",
                channel="wechat",
                bot_id="wechat-bot",
                limit=10,
            )
            assert [item.outbox_id for item in claimed] == [result.outbox_id]

            # A recurring job whose immutable template originated in v39 can
            # fire again under v40: it creates no prompt outbox and its Agent
            # result is proactively claimable despite /notify off.
            await upgraded.set_notification_preference(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                session_id="default",
                agent_id="codex",
                enabled=False,
            )
            new_firing = await upgraded.fire_next_due_cron_job(
                now=START + timedelta(minutes=2)
            )
            assert new_firing is not None
            assert new_firing.firing.outbox_id is None
            class ResultRuntime:
                agent_id = "codex"

                async def run(self, task, _emit):
                    return AgentResult(
                        task_id=task.task_id,
                        execution_id=task.execution_id,
                        status="completed",
                        content="post-upgrade result",
                    )

                async def interrupt(self, _task_id):
                    return False

            assert await TaskWorker(
                upgraded,
                runtime=ResultRuntime(),
                worker_id="post-v39-cron-worker",
            ).run_once()
            new_results = [
                item
                for item in await upgraded.list_outbox(limit=100)
                if item.task_id == new_firing.task.task_id
            ]
            assert len(new_results) == 1
            assert new_results[0].content == "post-upgrade result"
            assert new_results[0].notify_enabled
            assert not new_results[0].foreground
            assert await upgraded._call(
                lambda conn: conn.execute("PRAGMA foreign_key_check").fetchall()
            ) == []
        finally:
            await upgraded.close()

    asyncio.run(scenario())


def test_global_queue_full_leaves_entire_occurrence_due(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite3",
            clock=lambda: START,
            max_agent_queue=1,
            max_global_queue=1,
            max_account_queue=1,
            max_account_agent_queue=1,
        )
        await store.initialize()
        try:
            target = ReplyTarget(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
            )
            await store.create_task(
                AgentTask(
                    agent_id="codex",
                    reply_target=target,
                    inputs={"text": "occupy admission"},
                )
            )
            job = await _add_job(store)
            assert await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            ) is None
            current = await store.get_cron_job(job.job_id)
            assert current is not None
            assert current.next_fire_at == job.next_fire_at
            assert await store.list_cron_firings(job.job_id) == []
            assert await store.get_task(
                store._cron_occurrence_id(
                    "task", job.job_id, "2026-09-08T01:01:00.000000+00:00"
                )
            ) is None
            assert await store.get_outbox_item(
                store._cron_occurrence_id(
                    "outbox", job.job_id, "2026-09-08T01:01:00.000000+00:00"
                )
            ) is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_full_account_does_not_block_another_due_account(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite3",
            clock=lambda: START,
            max_agent_queue=10,
            max_global_queue=10,
            max_account_queue=1,
            max_account_agent_queue=10,
        )
        await store.initialize()
        try:
            account_a = ReplyTarget(
                channel="wechat",
                bot_id="account-a",
                external_user_id="owner-a",
                conversation_subject_scope="owner-a",
            )
            account_b = ReplyTarget(
                channel="wechat",
                bot_id="account-b",
                external_user_id="owner-b",
                conversation_subject_scope="owner-b",
            )
            occupying = await store.create_task(
                AgentTask(
                    agent_id="codex",
                    reply_target=account_a,
                    inputs={"text": "occupy only account A"},
                )
            )
            blocked = await _add_job(
                store,
                job_id="cron-a-blocked",
                target=account_a,
            )
            eligible = await _add_job(
                store,
                job_id="cron-b-eligible",
                target=account_b,
            )

            fired = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            )
            assert fired is not None
            assert fired.job.job_id == eligible.job_id
            still_due = await store.get_cron_job(blocked.job_id)
            assert still_due is not None
            assert still_due.next_fire_at == START + timedelta(minutes=1)
            assert await store.list_cron_firings(blocked.job_id) == []

            assert await store.request_cancel(occupying.task_id)
            later = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            )
            assert later is not None and later.job.job_id == blocked.job_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_restart_reconcile_repairs_projection_and_catches_up_once(tmp_path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite3"
        first = SQLiteStore(path, clock=lambda: START)
        await first.initialize()
        job = await _add_job(first)
        await first.close()

        # Simulate a damaged future projection.  Startup reconciliation
        # derives the first occurrence from the immutable creation anchor.
        with sqlite3.connect(path) as conn:
            conn.execute(
                "UPDATE cron_jobs SET next_fire_at=? WHERE job_id=?",
                (
                    "2026-09-08T09:00:00.000000+00:00",
                    job.job_id,
                ),
            )
            conn.commit()

        restart_time = START + timedelta(minutes=5)
        second = SQLiteStore(path, clock=lambda: restart_time)
        await second.initialize()
        try:
            changed = await second.reconcile_cron_jobs(now=restart_time)
            assert [item.job_id for item in changed] == [job.job_id]
            repaired = await second.get_cron_job(job.job_id)
            assert repaired is not None
            assert repaired.next_fire_at == START + timedelta(minutes=1)
            firing = await second.fire_next_due_cron_job(now=restart_time)
            assert firing is not None
            assert firing.firing.scheduled_for == START + timedelta(minutes=1)
            # Four additional missed minutes are skipped, not replayed.
            assert firing.job.next_fire_at == START + timedelta(minutes=6)
        finally:
            await second.close()

        third = SQLiteStore(path, clock=lambda: restart_time)
        await third.initialize()
        try:
            assert await third.reconcile_cron_jobs(now=restart_time) == []
            stable = await third.get_cron_job(job.job_id)
            assert stable is not None
            assert stable.next_fire_at == START + timedelta(minutes=6)
            assert await third.fire_next_due_cron_job(now=restart_time) is None
            assert len(await third.list_cron_firings(job.job_id)) == 1
        finally:
            await third.close()

    asyncio.run(scenario())


def test_two_store_instances_cannot_double_fire(tmp_path) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite3"
        first = SQLiteStore(path, clock=lambda: START)
        second = SQLiteStore(path, clock=lambda: START)
        await first.initialize()
        await second.initialize()
        try:
            job = await _add_job(first)
            results = await asyncio.gather(
                first.fire_next_due_cron_job(
                    now=START + timedelta(minutes=1)
                ),
                second.fire_next_due_cron_job(
                    now=START + timedelta(minutes=1)
                ),
            )
            assert sum(result is not None for result in results) == 1
            assert len(await first.list_cron_firings(job.job_id)) == 1
        finally:
            await second.close()
            await first.close()

    asyncio.run(scenario())


def test_revoked_mapping_disables_job_instead_of_firing(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            await store.create_principal(principal_id="owner")
            account = await store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="owner-user",
                identifier_kind="user_id",
                configured_by="test",
            )
            job = await _add_job(
                store,
                principal_id="owner",
                principal_account_id=account.principal_account_id,
                principal_mapping_revision=account.mapping_revision,
            )
            await store.update_principal("owner", enabled=False)
            with pytest.raises(PermissionError, match="mapping"):
                await _add_job(
                    store,
                    principal_id="owner",
                    principal_account_id=account.principal_account_id,
                    principal_mapping_revision=account.mapping_revision,
                )
            assert await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            ) is None
            disabled = await store.get_cron_job(job.job_id)
            assert disabled is not None and not disabled.enabled
            assert "mapping" in str(disabled.disabled_reason)
            assert await store.list_cron_firings(job.job_id) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_owner_scoped_cron_access_fences_mapping_and_legacy_origin(
    tmp_path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            target_a = ReplyTarget(
                channel="wechat",
                bot_id="bot-a",
                external_user_id="user-a",
                conversation_subject_scope="user-a",
            )
            target_b = ReplyTarget(
                channel="lark",
                bot_id="bot-b",
                external_user_id="user-b",
                conversation_subject_scope="user-b",
            )
            # Lark creation is fenced by an enabled bot profile even before a
            # principal is mapped.
            await store.create_bot_profile(
                profile_id="profile-b",
                channel="lark",
                bot_id="bot-b",
                brand="feishu",
                config_dir=str(tmp_path / "profile-b"),
                config_dir_identity="identity:profile-b",
                cli_version="test",
                credential_ref="file:test-b",
            )
            foreign = ReplyTarget(
                channel="wechat",
                bot_id="bot-foreign",
                external_user_id="user-foreign",
                conversation_subject_scope="user-foreign",
            )
            legacy_a = await _add_job(
                store,
                job_id="legacy-a",
                target=target_a,
                now=START,
            )
            legacy_foreign = await _add_job(
                store,
                job_id="legacy-foreign",
                target=foreign,
                now=START + timedelta(seconds=1),
            )

            unmapped = await store.list_cron_jobs_for_owner(
                origin_channel="wechat",
                origin_bot_id="bot-a",
                origin_external_user_id="user-a",
            )
            assert [job.job_id for job in unmapped] == [legacy_a.job_id]
            with pytest.raises(NotFoundError, match="not found"):
                await store.get_cron_job_for_owner(
                    legacy_foreign.job_id,
                    origin_channel="wechat",
                    origin_bot_id="bot-a",
                    origin_external_user_id="user-a",
                )

            await store.create_principal(principal_id="owner")
            await store.create_principal(principal_id="other")
            account_a = await store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="bot-a",
                external_user_id="user-a",
                identifier_kind="user_id",
                configured_by="test",
            )
            account_b = await store.map_principal_account(
                principal_id="owner",
                channel="lark",
                bot_id="bot-b",
                external_user_id="user-b",
                identifier_kind="open_id",
                configured_by="test",
            )
            foreign_account = await store.map_principal_account(
                principal_id="other",
                channel="wechat",
                bot_id="bot-foreign",
                external_user_id="user-foreign",
                identifier_kind="user_id",
                configured_by="test",
            )
            mapped_a = await _add_job(
                store,
                job_id="mapped-a",
                target=target_a,
                principal_id="owner",
                principal_account_id=account_a.principal_account_id,
                principal_mapping_revision=account_a.mapping_revision,
                now=START + timedelta(seconds=2),
            )
            mapped_b = await _add_job(
                store,
                job_id="mapped-b",
                target=target_b,
                principal_id="owner",
                principal_account_id=account_b.principal_account_id,
                principal_mapping_revision=account_b.mapping_revision,
                now=START + timedelta(seconds=3),
            )
            mapped_foreign = await _add_job(
                store,
                job_id="mapped-foreign",
                target=foreign,
                principal_id="other",
                principal_account_id=foreign_account.principal_account_id,
                principal_mapping_revision=foreign_account.mapping_revision,
                now=START + timedelta(seconds=4),
            )
            owner_scope = {
                "principal_id": "owner",
                "principal_account_id": account_a.principal_account_id,
                "principal_mapping_revision": account_a.mapping_revision,
                "origin_channel": "wechat",
                "origin_bot_id": "bot-a",
                "origin_external_user_id": "user-a",
            }
            visible = await store.list_cron_jobs_for_owner(**owner_scope)
            assert [job.job_id for job in visible] == [
                mapped_b.job_id,
                mapped_a.job_id,
                legacy_a.job_id,
            ]
            newest = await store.list_cron_jobs_for_owner(
                **owner_scope,
                limit=2,
            )
            assert [job.job_id for job in newest] == [
                mapped_b.job_id,
                mapped_a.job_id,
            ]
            assert (
                await store.get_cron_job_for_owner(
                    mapped_b.job_id, **owner_scope
                )
            ).job_id == mapped_b.job_id
            for hidden in (legacy_foreign.job_id, mapped_foreign.job_id):
                with pytest.raises(NotFoundError, match="not found"):
                    await store.get_cron_job_for_owner(hidden, **owner_scope)

            disabled = await store.disable_cron_job_for_owner(
                legacy_a.job_id,
                **owner_scope,
            )
            assert not disabled.enabled

            replacement = await store.map_principal_account(
                principal_id="other",
                channel="wechat",
                bot_id="bot-a",
                external_user_id="user-a",
                identifier_kind="user_id",
                configured_by="remap-test",
            )
            assert replacement.mapping_revision > account_a.mapping_revision
            with pytest.raises(PermissionError, match="mapping"):
                await store.list_cron_jobs_for_owner(**owner_scope)
            with pytest.raises(NotFoundError, match="not found"):
                await store.get_cron_job_for_owner(
                    mapped_a.job_id, **owner_scope
                )
            with pytest.raises(NotFoundError, match="not found"):
                await store.disable_cron_job_for_owner(
                    mapped_a.job_id, **owner_scope
                )
            with pytest.raises(PermissionError, match="mapping"):
                await store.list_cron_jobs_for_owner(
                    origin_channel="wechat",
                    origin_bot_id="bot-a",
                    origin_external_user_id="user-a",
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_lark_thread_fires_through_exact_bot_chat_and_bot_revocation_stops_it(
    tmp_path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            await store.create_bot_profile(
                profile_id="lark-profile",
                channel="lark",
                bot_id="cli_test_bot",
                brand="feishu",
                config_dir=str(tmp_path / "lark-profile"),
                config_dir_identity="identity:lark-profile",
                cli_version="test",
                credential_ref="file:test-credential",
            )
            target = ReplyTarget(
                channel="lark",
                bot_id="cli_test_bot",
                external_user_id="ou_owner",
                source_message_id="om_thread_parent",
                context_token="stale-context",
                conversation_subject_scope="oc_chat",
                destination_kind="thread",
                destination_id="oc_chat",
                thread_id="omt_thread",
                root_message_id="om_root",
            )
            job = await _add_job(store, target=target)
            fired = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=1)
            )
            assert fired is not None
            assert fired.reminder is None
            assert fired.firing.outbox_id is None
            assert fired.task.reply_target.bot_id == "cli_test_bot"
            assert fired.task.reply_target.destination_id == "oc_chat"
            assert (
                fired.task.reply_target.source_message_id
                == "om_thread_parent"
            )
            assert fired.task.reply_target.context_token is None
            assert await store.list_outbox(limit=100) == []
            route = await store._call(
                lambda conn: tuple(
                    conn.execute(
                        "SELECT channel,bot_id,external_user_id FROM tasks "
                        "WHERE task_id=?",
                        (fired.task.task_id,),
                    ).fetchone()
                )
            )
            assert route == ("lark", "cli_test_bot", "oc_chat")

            await store.set_bot_profile_enabled("lark-profile", False)
            assert await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=2)
            ) is None
            disabled = await store.get_cron_job(job.job_id)
            assert disabled is not None and not disabled.enabled
            assert "Lark bot" in str(disabled.disabled_reason)
            assert len(await store.list_cron_firings(job.job_id)) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_disable_expiry_one_shot_and_agent_retirement(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3", clock=lambda: START)
        await store.initialize()
        try:
            manual = await _add_job(store, job_id="manual")
            disabled = await store.disable_cron_job(
                manual.job_id,
                origin_channel="wechat",
                origin_bot_id="wechat-bot",
                origin_external_user_id="owner-user",
            )
            assert not disabled.enabled
            assert await store.disable_cron_job(manual.job_id) == disabled

            with pytest.raises(ValueError, match="expiry precedes"):
                await _add_job(
                    store,
                    job_id="bad-expiry",
                    expires_at=START + timedelta(minutes=1),
                )

            one_shot = await _add_job(
                store,
                job_id="one-shot",
                schedule_kind="at",
                schedule_expression="2026-09-08T09:02:00+08:00",
            )
            result = await store.fire_next_due_cron_job(
                now=START + timedelta(minutes=2)
            )
            assert result is not None and result.job.job_id == one_shot.job_id
            assert not result.job.enabled
            assert result.job.next_fire_at is None
            assert await store.request_cancel(result.task.task_id)

            retirement = await _add_job(
                store, job_id="disabled-with-agent"
            )
            await store.retire_agent("codex")
            retired = await store.get_cron_job(retirement.job_id)
            assert retired is not None and not retired.enabled
            assert retired.disabled_reason == "target Agent was deleted"
        finally:
            await store.close()

    asyncio.run(scenario())
