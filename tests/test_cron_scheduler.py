"""TaskManager cron lifecycle, frozen templates, and ownership fencing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.agents.base import AgentResult, ReplyTarget
from src.runtime.manager import TaskManager
from src.runtime.identity import thread_conversation_subject
from src.runtime.models import InboundMessage
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import QueueFullError, StoreError
from src.runtime.worker import TaskWorker


class _Runtime:
    agent_id = "codex"

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []

    async def start(self) -> None:
        self.events.append("runtime-start")

    async def stop(self) -> None:
        self.events.append("runtime-stop")

    async def run(self, _task: object) -> None:
        raise AssertionError("scheduler lifecycle test must not execute a task")

    async def interrupt(self, _task_id: str) -> bool:
        return False


class _ResultRuntime(_Runtime):
    def __init__(
        self,
        *,
        status: str = "completed",
        content: str = "scheduled work completed",
        error: str | None = None,
    ) -> None:
        super().__init__()
        self.status = status
        self.content = content
        self.error = error
        self.seen_prompts: list[str] = []

    async def run(self, task: object, _emit: object) -> AgentResult:
        inputs = getattr(task, "inputs", {})
        self.seen_prompts.append(str(inputs.get("text") or ""))
        return AgentResult(
            task_id=str(getattr(task, "task_id")),
            execution_id=str(getattr(task, "execution_id")),
            status=self.status,
            content=self.content,
            error=self.error,
        )


class _SchedulerStore:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.fire_results: list[object] = [SimpleNamespace(job_id="cron-a"), None]
        self.fired = asyncio.Event()
        self.closed = False

    async def initialize(self) -> None:
        self.events.append("store-initialize")

    async def reconcile_cron_jobs(self, **_kwargs: object) -> list[object]:
        assert "runtime-start" in self.events
        self.events.append("cron-reconcile")
        return []

    async def fire_next_due_cron_job(self, **_kwargs: object) -> object | None:
        assert not self.closed
        self.events.append("cron-fire")
        result = self.fire_results.pop(0) if self.fire_results else None
        if result is not None:
            self.fired.set()
        return result

    async def close(self) -> None:
        self.closed = True
        self.events.append("store-close")


def test_cron_scheduler_starts_after_runtime_and_stops_before_store() -> None:
    async def scenario() -> list[str]:
        store = _SchedulerStore()
        runtime = _Runtime(store.events)
        manager = TaskManager(
            store,
            runtime,
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=0.01,
        )
        await manager.start()
        await asyncio.wait_for(store.fired.wait(), timeout=1)
        await manager.stop()
        fire_count = store.events.count("cron-fire")
        await asyncio.sleep(0.03)
        assert store.events.count("cron-fire") == fire_count
        return store.events

    events = asyncio.run(scenario())
    assert events.index("runtime-start") < events.index("cron-reconcile")
    assert events.index("cron-reconcile") < events.index("cron-fire")
    assert events.index("runtime-stop") < events.index("store-close")


def test_cron_queue_full_is_retried_without_killing_scheduler() -> None:
    class FullOnceStore(_SchedulerStore):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0
            self.fire_results = []

        async def fire_next_due_cron_job(self, **_kwargs: object) -> object | None:
            self.attempts += 1
            if self.attempts == 1:
                raise QueueFullError("agent", 1)
            self.fired.set()
            return None

    async def scenario() -> int:
        store = FullOnceStore()
        runtime = _Runtime(store.events)
        manager = TaskManager(
            store,
            runtime,
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=0.01,
        )
        await manager.start()
        await asyncio.wait_for(store.fired.wait(), timeout=1)
        await manager.stop()
        return store.attempts

    assert asyncio.run(scenario()) >= 2


class _CronStore:
    def __init__(self, *, mapped: bool = True) -> None:
        self.mapped = mapped
        self.created: dict[str, object] | None = None
        self.jobs: list[SimpleNamespace] = []
        self.disabled: list[str] = []

    async def resolve_principal_account(self, **kwargs: object) -> object | None:
        if not self.mapped:
            return None
        assert kwargs == {
            "channel": "lark",
            "bot_id": "cli-a",
            "external_user_id": "ou-owner",
        }
        return SimpleNamespace(
            principal_id="owner",
            principal_account_id="account-a",
            mapping_revision=7,
        )

    async def create_cron_job(self, **kwargs: object) -> object:
        self.created = dict(kwargs)
        return SimpleNamespace(**kwargs)

    async def list_cron_jobs(self, **kwargs: object) -> list[object]:
        values = list(self.jobs)
        principal_id = kwargs.get("principal_id")
        if principal_id is not None:
            values = [job for job in values if job.principal_id == principal_id]
        origin_channel = kwargs.get("origin_channel")
        if origin_channel is not None:
            values = [
                job
                for job in values
                if (
                    job.origin_channel,
                    job.origin_bot_id,
                    job.origin_external_user_id,
                )
                == (
                    origin_channel,
                    kwargs.get("origin_bot_id"),
                    kwargs.get("origin_external_user_id"),
                )
            ]
        return values

    async def get_cron_job(self, job_id: str) -> object | None:
        return next((job for job in self.jobs if job.job_id == job_id), None)

    async def disable_cron_job(self, job_id: str, **_kwargs: object) -> object:
        self.disabled.append(job_id)
        return await self.get_cron_job(job_id)


def test_add_cron_freezes_complete_lark_task_and_origin() -> None:
    async def scenario() -> dict[str, object]:
        store = _CronStore()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
            model="gpt-test",
            reasoning_effort="high",
        )
        target = ReplyTarget(
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
            session_id="session-a",
            source_message_id="om-source",
            conversation_subject_id="subject-a",
            conversation_subject_scope="chat-a:thread-a",
            destination_kind="chat",
            destination_id="chat-a",
            thread_id="thread-a",
            root_message_id="root-a",
        )
        job = await manager.add_cron_job(
            "cron 0 9 * * 1-5 --tz Europe/London",
            "prepare briefing",
            job_id="cron-a",
            reply_target=target,
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
            session_id="session-a",
            conversation_subject_id="subject-a",
            conversation_subject_scope="chat-a:thread-a",
            conversation_subject_kind="thread",
            principal_id="owner",
            principal_account_id="account-a",
            agent_id="codex",
            now=datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc),
        )
        assert job.job_id == "cron-a"
        assert store.created is not None
        return store.created

    created = asyncio.run(scenario())
    assert created["principal_id"] == "owner"
    assert created["principal_account_id"] == "account-a"
    assert created["origin_channel"] == "lark"
    assert created["origin_bot_id"] == "cli-a"
    assert created["origin_external_user_id"] == "ou-owner"
    assert created["origin_conversation_subject_scope"] == "chat-a:thread-a"
    assert created["timezone_name"] == "Europe/London"
    assert created["next_fire_at"] == datetime(
        2026, 9, 8, 8, 0, tzinfo=timezone.utc
    )
    template = created["task_template"]
    assert template["agent_id"] == "codex"
    assert template["mode_id"] == "chat"
    assert template["profile_version"] == 1
    assert template["policy_version"] >= 1
    assert template["model"] == "gpt-test"
    assert template["reasoning_effort"] == "high"
    assert template["inputs"] == {"text": "prepare briefing"}
    assert template["reply_target"].destination_id == "chat-a"
    assert template["metadata"]["cron_job_id"] == "cron-a"
    assert template["metadata"]["session_role"]
    identity = template["identity_snapshot"]
    assert identity["actor"]["external_user_id"] == "ou-owner"
    assert identity["principal"]["mapping_revision"] == 7
    assert identity["conversation_subject"]["scope_key"] == "chat-a:thread-a"


def test_cron_ownership_shares_by_principal_but_isolates_unmapped_accounts() -> None:
    async def scenario() -> tuple[list[str], list[str], list[str], list[str]]:
        store = _CronStore(mapped=True)
        store.jobs = [
            SimpleNamespace(
                job_id="mapped-other-bot",
                principal_id="owner",
                origin_channel="wechat",
                origin_bot_id="bot-a",
                origin_external_user_id="wx-owner",
                created_at="2026-09-08T01:00:00+00:00",
            ),
            SimpleNamespace(
                job_id="unmapped-own",
                principal_id="",
                origin_channel="lark",
                origin_bot_id="cli-a",
                origin_external_user_id="ou-owner",
                created_at="2026-09-08T02:00:00+00:00",
            ),
            SimpleNamespace(
                job_id="unmapped-foreign",
                principal_id="",
                origin_channel="lark",
                origin_bot_id="cli-a",
                origin_external_user_id="ou-other",
                created_at="2026-09-08T03:00:00+00:00",
            ),
        ]
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        mapped = await manager.list_cron_jobs(
            principal_id="owner",
            principal_account_id="account-a",
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
        )
        await manager.delete_cron_job(
            "mapped-other-bot",
            principal_id="owner",
            principal_account_id="account-a",
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
        )
        # A job created by this exact account before it was mapped remains
        # manageable after mapping; the durable disable fence uses the job's
        # stored (empty) principal rather than the caller's new principal.
        await manager.delete_cron_job(
            "unmapped-own",
            principal_id="owner",
            principal_account_id="account-a",
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
        )

        unmapped_store = _CronStore(mapped=False)
        unmapped_store.jobs = list(store.jobs)
        unmapped_manager = TaskManager(
            unmapped_store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        unmapped = await unmapped_manager.list_cron_jobs(
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
        )
        with pytest.raises(KeyError, match="does not exist"):
            await unmapped_manager.delete_cron_job(
                "unmapped-foreign",
                channel="lark",
                bot_id="cli-a",
                external_user_id="ou-owner",
            )
        return (
            [job.job_id for job in mapped],
            [job.job_id for job in unmapped],
            [*store.disabled, *unmapped_store.disabled],
            [job.job_id for job in store.jobs],
        )

    mapped, unmapped, disabled, all_jobs = asyncio.run(scenario())
    assert mapped == ["unmapped-own", "mapped-other-bot"]
    assert unmapped == ["unmapped-own"]
    assert disabled == ["mapped-other-bot", "unmapped-own"]
    assert "unmapped-foreign" in all_jobs


def test_add_cron_rejects_past_one_shot_and_changed_principal_mapping() -> None:
    async def scenario() -> None:
        store = _CronStore()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        target = ReplyTarget(
            channel="lark",
            bot_id="cli-a",
            external_user_id="ou-owner",
        )
        with pytest.raises(ValueError, match="no future occurrence"):
            await manager.add_cron_job(
                "at 2026-09-07T09:00Z",
                "too late",
                reply_target=target,
                channel="lark",
                bot_id="cli-a",
                external_user_id="ou-owner",
                agent_id="codex",
                now=datetime(2026, 9, 8, tzinfo=timezone.utc),
            )
        with pytest.raises(PermissionError, match="mapping changed"):
            await manager.add_cron_job(
                "every 1h",
                "check",
                reply_target=target,
                channel="lark",
                bot_id="cli-a",
                external_user_id="ou-owner",
                principal_id="different",
                principal_account_id="different-account",
                agent_id="codex",
                now=datetime(2026, 9, 8, tzinfo=timezone.utc),
            )
        assert store.created is None

    asyncio.run(scenario())


def test_sqlite_cron_survives_restart_catches_up_once_and_routes_wechat(
    tmp_path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite3"
        created_at = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
        store = SQLiteStore(database)
        await store.initialize()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        await manager.add_cron_job(
            "every 1h",
            "check the deployment",
            job_id="cron-restart",
            reply_target=ReplyTarget(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                source_message_id="stale-source",
                source_sequence=9,
                context_token="stale-context",
            ),
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            agent_id="codex",
            now=created_at,
        )
        await store.close()

        restarted = SQLiteStore(database)
        await restarted.initialize()
        await restarted.reconcile_cron_jobs(
            now=datetime(2026, 9, 8, 3, 30, tzinfo=timezone.utc)
        )
        fired = await restarted.fire_next_due_cron_job(
            now=datetime(2026, 9, 8, 3, 30, tzinfo=timezone.utc)
        )
        assert fired is not None
        assert fired.firing.scheduled_for == datetime(
            2026, 9, 8, 1, 0, tzinfo=timezone.utc
        )
        assert fired.job.next_fire_at == datetime(
            2026, 9, 8, 4, 0, tzinfo=timezone.utc
        )
        assert fired.task.inputs == {"text": "check the deployment"}
        assert fired.reminder is None
        assert fired.firing.outbox_id is None
        target = fired.task.reply_target
        assert target.channel == "wechat"
        assert target.bot_id == "wechat-bot"
        assert target.external_user_id == "wx-owner"
        assert target.source_message_id in (None, "")
        assert target.source_sequence is None
        assert target.context_token in (None, "")
        assert await restarted.list_outbox(limit=100) == []
        await restarted.set_notification_preference(
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            session_id="default",
            agent_id="codex",
            enabled=False,
        )
        runtime = _ResultRuntime(content="deployment is healthy")
        assert await TaskWorker(
            restarted,
            runtime=runtime,
            worker_id="cron-wechat-worker",
        ).run_once()
        assert runtime.seen_prompts == ["check the deployment"]
        results = [
            item
            for item in await restarted.list_outbox(limit=100)
            if item.task_id == fired.task.task_id
        ]
        assert len(results) == 1
        assert results[0].content == "deployment is healthy"
        assert results[0].notify_enabled
        assert not results[0].foreground
        assert await restarted.fire_next_due_cron_job(
            now=datetime(2026, 9, 8, 3, 30, tzinfo=timezone.utc)
        ) is None
        firings = await restarted.list_cron_firings("cron-restart")
        assert len(firings) == 1
        await restarted.close()

    asyncio.run(scenario())


def test_sqlite_cron_executes_and_routes_result_to_exact_lark_bot_and_chat(
    tmp_path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite3")
        await store.initialize()
        await store.create_bot_profile(
            {
                "profile_id": "lark-a",
                "channel": "lark",
                "bot_id": "cli_lark_a",
                "brand": "feishu",
                "config_dir": str(tmp_path / "lark-a"),
                "config_dir_identity": "device:lark-a",
                "cli_version": "1.0.0",
                "credential_ref": "keychain:lark/lark-a",
                "mention_policy": "direct_or_mention",
                "access_policy": "all",
                "restart_policy": {},
            }
        )
        inbound = await store.store_inbound(
            InboundMessage(
                channel="lark",
                bot_id="cli_lark_a",
                external_user_id="ou_actor_a",
                external_message_id="om_source_a",
                session_id="default",
                text="/cron add every 1h -- check",
                destination_kind="chat",
                destination_id="oc_chat_a",
            )
        )
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        await manager.add_cron_job(
            "every 1h",
            "check",
            job_id="cron-lark-route",
            reply_target=inbound.target(),
            channel="lark",
            bot_id="cli_lark_a",
            external_user_id="ou_actor_a",
            session_id="default",
            conversation_subject_id=str(inbound.conversation_subject_id or ""),
            conversation_subject_scope=inbound.conversation_subject_scope,
            conversation_subject_kind=inbound.conversation_subject_kind,
            agent_id="codex",
            now=datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc),
        )
        fired = await store.fire_next_due_cron_job(
            now=datetime(2026, 9, 8, 1, 0, tzinfo=timezone.utc)
        )
        assert fired is not None
        assert fired.reminder is None
        assert fired.firing.outbox_id is None
        target = fired.task.reply_target
        assert target.channel == "lark"
        assert target.bot_id == "cli_lark_a"
        assert target.external_user_id == "ou_actor_a"
        assert target.destination_kind == "chat"
        assert target.destination_id == "oc_chat_a"
        assert target.conversation_subject_id == inbound.conversation_subject_id
        assert target.source_message_id in (None, "")
        assert await store.list_outbox(limit=100) == []
        await store.set_notification_preference(
            channel="lark",
            bot_id="cli_lark_a",
            external_user_id="ou_actor_a",
            session_id="default",
            agent_id="codex",
            enabled=False,
        )
        runtime = _ResultRuntime(content="lark scheduled result")
        assert await TaskWorker(
            store,
            runtime=runtime,
            worker_id="cron-lark-worker",
        ).run_once()
        assert runtime.seen_prompts == ["check"]
        results = [
            item
            for item in await store.list_outbox(limit=100)
            if item.task_id == fired.task.task_id
        ]
        assert len(results) == 1
        assert results[0].content == "lark scheduled result"
        assert results[0].notify_enabled
        assert not results[0].foreground
        # A different app cannot claim the result even if it knows its ID.
        assert await store.claim_account_outbox(
            "wrong-bot-worker",
            channel="lark",
            bot_id="cli_lark_b",
            limit=10,
        ) == []
        claimed = await store.claim_account_outbox(
            "right-bot-worker",
            channel="lark",
            bot_id="cli_lark_a",
            limit=10,
        )
        assert [item.outbox_id for item in claimed] == [results[0].outbox_id]
        await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["wechat", "lark"])
def test_cron_failure_is_returned_through_exact_origin_even_with_notify_off(
    tmp_path,
    channel: str,
) -> None:
    async def scenario() -> None:
        bot_id = "wechat-bot" if channel == "wechat" else "cli_failure_bot"
        external_user_id = "wx-owner" if channel == "wechat" else "ou-owner"
        destination_id = external_user_id if channel == "wechat" else "oc_failure"
        store = SQLiteStore(tmp_path / f"{channel}.sqlite3")
        await store.initialize()
        if channel == "lark":
            await store.create_bot_profile(
                profile_id="lark-failure-profile",
                channel="lark",
                bot_id=bot_id,
                brand="feishu",
                config_dir=str(tmp_path / "lark-failure-profile"),
                config_dir_identity="identity:lark-failure-profile",
                cli_version="test",
                credential_ref="file:test-failure-credential",
            )
        target = ReplyTarget(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            conversation_subject_scope=destination_id,
            destination_kind="direct" if channel == "wechat" else "chat",
            destination_id=destination_id,
        )
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        await manager.add_cron_job(
            "every 1h",
            "perform a scheduled failure check",
            job_id=f"cron-failure-{channel}",
            reply_target=target,
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            conversation_subject_scope=destination_id,
            agent_id="codex",
            now=datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc),
        )
        await store.set_notification_preference(
            channel=channel,
            bot_id=bot_id,
            external_user_id=external_user_id,
            session_id="default",
            agent_id="codex",
            enabled=False,
        )
        fired = await store.fire_next_due_cron_job(
            now=datetime(2026, 9, 8, 1, 0, tzinfo=timezone.utc)
        )
        assert fired is not None
        assert await store.list_outbox(limit=100) == []
        runtime = _ResultRuntime(
            status="failed",
            content="",
            error="private provider diagnostic",
        )
        assert await TaskWorker(
            store,
            runtime=runtime,
            worker_id=f"cron-failure-worker-{channel}",
        ).run_once()
        assert runtime.seen_prompts == ["perform a scheduled failure check"]
        failures = [
            item
            for item in await store.list_outbox(limit=100)
            if item.task_id == fired.task.task_id
        ]
        assert len(failures) == 1
        failure = failures[0]
        assert failure.content.startswith(f"task failed: {fired.task.task_id}")
        assert "private provider diagnostic" not in failure.content
        assert failure.notify_enabled
        assert not failure.foreground
        assert failure.channel == channel
        assert failure.bot_id == bot_id
        assert failure.external_user_id == external_user_id
        assert failure.reply_target.destination_id == destination_id
        if channel == "lark":
            assert await store.claim_account_outbox(
                "wrong-lark-bot",
                channel="lark",
                bot_id="cli_other_bot",
                limit=10,
            ) == []
        claimed = await store.claim_account_outbox(
            f"right-{channel}-bot",
            channel=channel,
            bot_id=bot_id,
            limit=10,
        )
        assert [item.outbox_id for item in claimed] == [failure.outbox_id]
        await store.close()

    asyncio.run(scenario())


def test_cron_add_replay_after_one_shot_fire_skips_mutable_template_state(
    tmp_path,
) -> None:
    async def scenario() -> None:
        created_at = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
        fire_at = created_at + timedelta(minutes=1)
        store = SQLiteStore(tmp_path / "replay.sqlite3")
        await store.initialize()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        target = ReplyTarget(
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            conversation_subject_scope="wx-owner",
        )
        try:
            original = await manager.add_cron_job(
                f"at {fire_at.isoformat()}",
                "one shot",
                job_id="cron-replay-fired",
                reply_target=target,
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                principal_mapping_revision=0,
                agent_id="codex",
                now=created_at,
            )
            fired = await store.fire_next_due_cron_job(now=fire_at)
            assert fired is not None

            async def mutable_state_must_not_be_read(**_kwargs: object) -> object:
                raise AssertionError("cron replay rebuilt its frozen task template")

            manager._build_cron_task_template = mutable_state_must_not_be_read  # type: ignore[method-assign]
            replay = await manager.add_cron_job(
                f"at {fire_at.isoformat()}",
                "one shot",
                job_id="cron-replay-fired",
                reply_target=target,
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                principal_mapping_revision=0,
                agent_id="codex",
                now=created_at,
            )

            assert replay.job_id == original.job_id
            assert replay.next_fire_at is None
            assert replay.task_template == original.task_template
            assert len(await store.list_cron_firings(original.job_id)) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cron_template_preserves_lark_thread_parent_subject(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "lark-thread.sqlite3")
        await store.initialize()
        await store.create_bot_profile(
            {
                "profile_id": "lark-thread",
                "channel": "lark",
                "bot_id": "cli_thread_bot",
                "brand": "feishu",
                "config_dir": str(tmp_path / "lark-thread"),
                "config_dir_identity": "device:lark-thread",
                "cli_version": "1.0.0",
                "credential_ref": "keychain:lark/thread",
                "mention_policy": "direct_or_mention",
                "access_policy": "all",
                "restart_policy": {},
            }
        )
        subject = thread_conversation_subject(
            "lark", "cli_thread_bot", "oc_thread_chat", "omt_thread"
        )
        inbound = await store.store_inbound(
            InboundMessage(
                channel="lark",
                bot_id="cli_thread_bot",
                external_user_id="ou_owner",
                external_message_id="om_thread_source",
                text="/cron add every 1h -- check thread",
                conversation_subject_id=subject.conversation_subject_id,
                conversation_subject_scope=subject.scope_key,
                conversation_subject_kind="thread",
                destination_kind="thread",
                destination_id="oc_thread_chat",
                thread_id="omt_thread",
                root_message_id="omt_thread",
                transport_metadata={
                    "chat_id": "oc_thread_chat",
                    "thread_id": "omt_thread",
                    "root_id": "omt_thread",
                },
            )
        )
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        try:
            job = await manager.add_cron_job(
                "every 1h",
                "check thread",
                job_id="cron-lark-thread-parent",
                reply_target=inbound.target(),
                channel="lark",
                bot_id="cli_thread_bot",
                external_user_id="ou_owner",
                conversation_subject_id=subject.conversation_subject_id,
                conversation_subject_scope=subject.scope_key,
                conversation_subject_kind="thread",
                principal_mapping_revision=0,
                agent_id="codex",
                now=datetime(2026, 9, 8, tzinfo=timezone.utc),
            )
            snapshot = job.task_template["identity_snapshot"]
            assert snapshot["conversation_subject"] == {
                "conversation_subject_id": subject.conversation_subject_id,
                "kind": "thread",
                "scope_key": subject.scope_key,
                "parent_subject_id": subject.parent_subject_id,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cron_add_fences_concurrent_agent_delete_and_recreate(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "incarnation-race.sqlite3")
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                session_id="default",
            )
            registration = manager.registry.registration("planner")
            assert registration is not None and registration.profile is not None
            profile = registration.profile
            first_lifecycle = await store.get_agent_lifecycle("planner")
            assert first_lifecycle is not None

            entered = asyncio.Event()
            resume = asyncio.Event()
            original_create = store.create_cron_job

            async def blocked_create(*args: object, **kwargs: object) -> object:
                entered.set()
                await resume.wait()
                return await original_create(*args, **kwargs)

            store.create_cron_job = blocked_create  # type: ignore[method-assign]
            adding = asyncio.create_task(
                manager.add_cron_job(
                    "every 1h",
                    "do not retarget me",
                    job_id="cron-incarnation-race",
                    reply_target=ReplyTarget(
                        channel="wechat",
                        bot_id="wechat-bot",
                        external_user_id="wx-owner",
                        conversation_subject_scope="wx-owner",
                    ),
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="wx-owner",
                    principal_mapping_revision=0,
                    agent_id="planner",
                    now=datetime(2026, 9, 8, tzinfo=timezone.utc),
                )
            )
            await asyncio.wait_for(entered.wait(), timeout=1)
            await store.force_retire_agent("planner")
            await store.reactivate_agent(profile)
            await store.commit_agent_reactivation(
                profile,
                channel="wechat",
                bot_id="other-bot",
                external_user_id="other-user",
                session_id="default",
            )
            second_lifecycle = await store.get_agent_lifecycle("planner")
            assert second_lifecycle is not None
            assert (
                second_lifecycle.agent_incarnation
                > first_lifecycle.agent_incarnation
            )
            resume.set()

            with pytest.raises(StoreError, match="target Agent"):
                await adding
            assert await store.get_cron_job("cron-incarnation-race") is None
        finally:
            resume.set()
            await manager.stop()

    asyncio.run(scenario())


def test_cron_manager_lists_newest_union_with_deterministic_limit(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "list-order.sqlite3")
        await store.initialize()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        target = ReplyTarget(
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            conversation_subject_scope="wx-owner",
        )
        created_at = datetime(2026, 9, 8, tzinfo=timezone.utc)
        try:
            identifiers = [f"zlegacy-{index:03d}" for index in range(3)]
            for job_id in identifiers:
                await manager.add_cron_job(
                    "every 1h",
                    "legacy",
                    job_id=job_id,
                    reply_target=target,
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="wx-owner",
                    principal_mapping_revision=0,
                    agent_id="codex",
                    now=created_at,
                )
            await store.create_principal(principal_id="owner")
            account = await store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                identifier_kind="user_id",
                configured_by="test",
            )
            mapped_ids = [f"mapped-{index:03d}" for index in range(98)]
            identifiers.extend(mapped_ids)
            for job_id in mapped_ids:
                await manager.add_cron_job(
                    "every 1h",
                    "mapped",
                    job_id=job_id,
                    reply_target=target,
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="wx-owner",
                    principal_id="owner",
                    principal_account_id=account.principal_account_id,
                    principal_mapping_revision=account.mapping_revision,
                    agent_id="codex",
                    now=created_at,
                )
            ownership = {
                "principal_id": "owner",
                "principal_account_id": account.principal_account_id,
                "principal_mapping_revision": account.mapping_revision,
                "channel": "wechat",
                "bot_id": "wechat-bot",
                "external_user_id": "wx-owner",
            }
            jobs = await manager.list_cron_jobs(**ownership, limit=100)
            assert [job.job_id for job in jobs] == sorted(
                identifiers, reverse=True
            )[:100]
            assert await manager.list_cron_jobs(**ownership, limit=0) == []
            with pytest.raises(ValueError, match="non-negative integer"):
                await manager.list_cron_jobs(**ownership, limit=-1)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cron_manager_owner_operations_recheck_mapping_in_store_transaction(
    tmp_path,
) -> None:
    class RevokingStore(SQLiteStore):
        revoke_on = ""

        async def _revoke_if_requested(self, operation: str) -> None:
            if self.revoke_on != operation:
                return
            self.revoke_on = ""
            assert await self.unmap_principal_account(
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
            )

        async def list_cron_jobs_for_owner(self, **kwargs: object):
            await self._revoke_if_requested("list")
            return await super().list_cron_jobs_for_owner(**kwargs)

        async def disable_cron_job_for_owner(
            self, job_id: str, **kwargs: object
        ):
            await self._revoke_if_requested("delete")
            return await super().disable_cron_job_for_owner(job_id, **kwargs)

    async def scenario() -> None:
        store = RevokingStore(tmp_path / "owner-race.sqlite3")
        await store.initialize()
        manager = TaskManager(
            store,
            _Runtime(),
            worker_count=0,
            reconcile_interval=None,
            cron_tick_interval=None,
        )
        target = ReplyTarget(
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            conversation_subject_scope="wx-owner",
        )
        await store.create_principal(principal_id="owner")
        first_account = await store.map_principal_account(
            principal_id="owner",
            channel="wechat",
            bot_id="wechat-bot",
            external_user_id="wx-owner",
            identifier_kind="user_id",
            configured_by="test",
        )
        try:
            job = await manager.add_cron_job(
                "every 1h",
                "protected",
                job_id="cron-owner-race",
                reply_target=target,
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                principal_id="owner",
                principal_account_id=first_account.principal_account_id,
                principal_mapping_revision=first_account.mapping_revision,
                agent_id="codex",
                now=datetime(2026, 9, 8, tzinfo=timezone.utc),
            )
            first_scope = {
                "principal_id": "owner",
                "principal_account_id": first_account.principal_account_id,
                "principal_mapping_revision": first_account.mapping_revision,
                "channel": "wechat",
                "bot_id": "wechat-bot",
                "external_user_id": "wx-owner",
            }
            store.revoke_on = "list"
            with pytest.raises(PermissionError, match="mapping"):
                await manager.list_cron_jobs(**first_scope)

            second_account = await store.map_principal_account(
                principal_id="owner",
                channel="wechat",
                bot_id="wechat-bot",
                external_user_id="wx-owner",
                identifier_kind="user_id",
                configured_by="test-remap",
            )
            second_scope = {
                "principal_id": "owner",
                "principal_account_id": second_account.principal_account_id,
                "principal_mapping_revision": second_account.mapping_revision,
                "channel": "wechat",
                "bot_id": "wechat-bot",
                "external_user_id": "wx-owner",
            }
            store.revoke_on = "delete"
            with pytest.raises(KeyError, match="does not exist"):
                await manager.delete_cron_job(job.job_id, **second_scope)
            retained = await store.get_cron_job(job.job_id)
            assert retained is not None and retained.enabled
        finally:
            await store.close()

    asyncio.run(scenario())
