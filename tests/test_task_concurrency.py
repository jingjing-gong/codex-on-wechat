"""Task ownership, serialization, and recovery regressions."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.modes import AgentMode, builtin_modes, legacy_builtin_modes
from src.runtime.policy import AgentProfile
from src.runtime.registry import DYNAMIC_AGENT_SUMMARY, codex_profile
from src.runtime.sqlite_store import SQLiteStore, StoreError


def _task(
    *,
    agent_id: str = "codex",
    conversation_id: str | None = None,
    text: str = "hello",
) -> dict[str, object]:
    return {
        "agent_id": agent_id,
        "conversation_id": conversation_id
        or f"wechat:bot:user:default:{agent_id}",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": {"text": text},
    }


def test_claims_are_atomic_serialized_per_agent_and_concurrent_across_agents(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path)
        second = SQLiteStore(path)
        await first.initialize()
        await second.initialize()
        try:
            task = await first.create_task(_task(text="first"))
            claims = await asyncio.gather(
                first.claim_task_by_id(task.task_id, "worker-a"),
                second.claim_task_by_id(task.task_id, "worker-b"),
            )
            winners = [claim for claim in claims if claim is not None]
            assert len(winners) == 1

            same_conversation = await first.create_task(_task(text="second"))
            assert (
                await second.claim_task_by_id(
                    same_conversation.task_id, "worker-c"
                )
                is None
            )

            await first.put_profile(
                AgentProfile(agent_id="planner", display_name="Planner")
            )
            await first.put_mode(AgentMode(mode_id="chat"), agent_id="planner")
            other_agent = await first.create_task(
                _task(agent_id="planner", text="parallel")
            )
            cross_agent_claim = await second.claim_task_by_id(
                other_agent.task_id, "worker-d"
            )
            assert cross_agent_claim is not None
            assert cross_agent_claim.task.agent_id == "planner"
        finally:
            await first.close()
            await second.close()

    asyncio.run(scenario())


def test_minimal_custom_codex_mode_version_cannot_be_rewritten(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            original = AgentMode(mode_id="custom")
            await store.put_mode(original)

            with pytest.raises(StoreError, match="mode version metadata conflicts"):
                await store.put_mode(
                    AgentMode(mode_id="custom", can_execute_commands=True)
                )

            persisted = await store.get_mode("codex", "custom", 1)
            assert persisted == original
            assert not persisted.can_execute_commands
        finally:
            await store.close()

    asyncio.run(scenario())


def test_minimal_codex_profile_version_cannot_be_rewritten_publicly(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            # Recreate the exact sparse row written by the historical seed.
            await store._call(
                lambda conn: conn.execute(
                    """UPDATE agent_profiles
                       SET display_name='Codex', summary='', system_prompt='',
                           responsibilities_json='[]', constraints_json='[]',
                           capabilities_json='[]', allowed_peers_json='[]',
                           denied_peers_json='[]', allowed_request_types_json='[]',
                           denied_request_types_json='[]', max_child_depth=0,
                           max_children_per_task=0, enabled=1,
                           default_mode_id='chat'
                       WHERE agent_id='codex' AND profile_version=1"""
                ).rowcount
            )

            with pytest.raises(
                StoreError, match="profile version metadata conflicts"
            ):
                await store.put_profile(codex_profile())

            persisted = await store.get_profile("codex", 1)
            assert persisted is not None
            assert persisted.capabilities == frozenset()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_startup_upgrades_only_the_known_legacy_profile_seed(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        try:
            await seeded._call(
                lambda conn: conn.execute(
                    """UPDATE agent_profiles
                       SET display_name='Codex', summary='', system_prompt='',
                           responsibilities_json='[]', constraints_json='[]',
                           capabilities_json='[]', allowed_peers_json='[]',
                           denied_peers_json='[]', allowed_request_types_json='[]',
                           denied_request_types_json='[]', max_child_depth=0,
                           max_children_per_task=0, enabled=1,
                           default_mode_id='chat'
                       WHERE agent_id='codex' AND profile_version=1"""
                ).rowcount
            )
        finally:
            await seeded.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            upgraded = await reopened.get_profile("codex", 1)
            assert upgraded == codex_profile()
            assert "execute" in upgraded.capabilities
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_startup_upgrades_only_the_known_legacy_builtin_mode_seed(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        try:
            await seeded._call(
                lambda conn: conn.execute(
                    """UPDATE agent_modes
                       SET developer_instructions='', sandbox_policy='read_only',
                           approval_policy='deny_all', allowed_tools_json='[]',
                           denied_tools_json='[]', can_write_files=0,
                           can_execute_commands=0, can_create_child_tasks=0,
                           can_send_agent_messages=0
                       WHERE agent_id='codex' AND mode_id='execute'
                         AND policy_version=1"""
                ).rowcount
            )
        finally:
            await seeded.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            upgraded = await reopened.get_mode("codex", "execute", 1)
            assert upgraded is not None
            assert upgraded.developer_instructions
            assert upgraded == legacy_builtin_modes()["execute"]
            assert upgraded.sandbox_policy == "workspace-write"
            assert upgraded.can_write_files
            assert upgraded.can_execute_commands
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_startup_preserves_published_mode_v1_and_migrates_session_to_v2(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            legacy_execute = await first.get_mode("codex", "execute", 1)
            assert legacy_execute == legacy_builtin_modes()["execute"]
            await first.set_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                mode_id="execute",
                policy_version=1,
                authorized_by="user",
                authorized_at=datetime.now(timezone.utc),
            )
            # Recreate a database last opened by the schema-16 runtime. The
            # v1 catalog and session row remain real published data, while v2
            # definitions and the v17 marker did not exist yet. This proves
            # the upgrade seeds its referenced immutable mode before moving
            # the mutable session selection to that version.
            def restore_schema_16(conn):
                conn.execute(
                    "DELETE FROM agent_modes WHERE agent_id='codex' "
                    "AND policy_version=2"
                )
                return conn.execute(
                    "DELETE FROM schema_migrations WHERE version>=17"
                ).rowcount

            await first._call(restore_schema_16)
        finally:
            await first.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            assert await reopened.get_mode("codex", "execute", 1) == (
                legacy_builtin_modes()["execute"]
            )
            assert await reopened.get_mode("codex", "execute", 2) == (
                builtin_modes()["execute"]
            )
            assert await reopened.get_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
            ) == ("execute", 2)
            assert await reopened.is_mode_authorized(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
                mode_id="execute",
                policy_version=2,
            )
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_startup_repairs_reordered_known_interim_full_access_execute_v1(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        await store._call(
            lambda conn: conn.execute(
                """UPDATE agent_modes
                   SET sandbox_policy='full_access',
                       allowed_tools_json='["write","status","search","read","list","execute","edit","diff"]'
                   WHERE agent_id='codex' AND mode_id='execute'
                     AND policy_version=1"""
            ).rowcount
        )
        await store.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            assert await reopened.get_mode("codex", "execute", 1) == (
                legacy_builtin_modes()["execute"]
            )
            assert await reopened.get_mode("codex", "execute", 2) == (
                builtin_modes()["execute"]
            )
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_startup_rejects_near_match_interim_full_access_execute_v1(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        await store._call(
            lambda conn: conn.execute(
                """UPDATE agent_modes
                   SET sandbox_policy='full_access',
                       allowed_tools_json='["write","status","search","read","list","execute","edit"]'
                   WHERE agent_id='codex' AND mode_id='execute'
                     AND policy_version=1"""
            ).rowcount
        )
        await store.close()

        reopened = SQLiteStore(path)
        with pytest.raises(StoreError, match="mode version metadata conflicts"):
            await reopened.initialize()
        await reopened.close()

    asyncio.run(scenario())


def test_mode_v2_migration_updates_generated_agent_sessions_only(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        generated = AgentProfile(
            agent_id="named",
            display_name="named",
            summary=DYNAMIC_AGENT_SUMMARY,
        )
        await store.put_profile(generated)
        for mode in legacy_builtin_modes().values():
            await store.put_mode(mode, agent_id="named")
        await store.set_session_mode(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            agent_id="named",
            mode_id="execute",
            policy_version=1,
            authorized_by="user",
            authorized_at="2026-08-13T00:00:00+00:00",
        )
        historical = await store.create_task(
            _task(agent_id="named", text="keep v1 snapshot")
        )
        await store._call(
            lambda conn: conn.execute(
                "DELETE FROM schema_migrations WHERE version>=18"
            ).rowcount
        )
        await store.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            assert await reopened.get_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="named",
            ) == ("execute", 2)
            assert await reopened.is_mode_authorized(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="named",
                mode_id="execute",
                policy_version=2,
            )
            for mode_id, expected in builtin_modes().items():
                assert await reopened.get_mode(
                    "named", mode_id, 2
                ) == expected
            retained = await reopened.get_task(historical.task_id)
            assert retained is not None
            assert retained.agent_id == "named"
            assert retained.policy_version == 1
            assert await reopened.get_mode("named", "chat", 1) == (
                legacy_builtin_modes()["chat"]
            )
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_mode_v2_migration_does_not_retarget_custom_agent_sessions(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        await store.put_profile(AgentProfile(agent_id="planner"))
        await store.put_mode(AgentMode(mode_id="chat"), agent_id="planner")
        await store.set_session_mode(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            agent_id="planner",
            mode_id="chat",
            policy_version=1,
        )
        await store._call(
            lambda conn: conn.execute(
                "DELETE FROM schema_migrations WHERE version>=17"
            ).rowcount
        )
        await store.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            assert await reopened.get_session_mode(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
            ) == ("chat", 1)
            assert await reopened.get_mode("planner", "chat", 1) == AgentMode(
                mode_id="chat"
            )
            assert await reopened.get_mode("planner", "chat", 2) is None
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_expired_claim_becomes_orphaned_and_requires_explicit_retry(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claimed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
            first_claim = await store.claim_task_by_id(
                task.task_id,
                "worker-a",
                lease_seconds=1,
                now=claimed_at,
            )
            assert first_claim is not None
            assert await store.mark_task_running(
                task.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
                now=claimed_at,
            )

            report = await store.reconcile(now=claimed_at + timedelta(seconds=2))
            assert report.tasks_orphaned == 1
            orphaned = await store.get_task(task.task_id)
            assert orphaned is not None
            assert orphaned.state.value == "orphaned"
            assert await store.claim_next_task("worker-b") is None
            assert not await store.renew_task_lease(
                task.task_id, first_claim.claim_token
            )

            retried = await store.retry_task(
                task.task_id,
                actor="user",
                now=claimed_at + timedelta(seconds=3),
            )
            assert retried is not None
            assert retried.state.value == "queued"
            second_claim = await store.claim_task_by_id(
                task.task_id,
                "worker-b",
                now=claimed_at + timedelta(seconds=3),
            )
            assert second_claim is not None
            assert second_claim.execution_id != first_claim.execution_id
            assert second_claim.task.attempts == 2
            executions = await store.list_task_executions(task.task_id)
            assert [item.state.value for item in executions] == [
                "orphaned",
                "claimed",
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_expired_task_claim_cannot_be_renewed_or_publish_worker_results(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        current_time = [base]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", clock=lambda: current_time[0]
        )
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(
                task.task_id, "worker", lease_seconds=1, now=base
            )
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
                now=base,
            )
            expired = base + timedelta(seconds=2)
            current_time[0] = expired

            assert not await store.renew_task_lease(
                task.task_id, claim.claim_token, now=expired
            )
            assert not await store.set_task_thread(
                task.task_id,
                thread_id="stale-thread",
                claim_token=claim.claim_token,
                now=expired,
            )
            assert not await store.transition_task(
                task.task_id,
                "completed",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
                result="stale",
                now=expired,
            )
            with pytest.raises(StoreError, match="lease has expired"):
                await store.append_task_event(
                    task.task_id,
                    {"content": "stale", "execution_id": claim.execution_id},
                    claim_token=claim.claim_token,
                )
            with pytest.raises(StoreError, match="lease has expired"):
                await store.complete_task(
                    task.task_id,
                    "stale",
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                    now=expired,
                )
            assert (await store.get_task(task.task_id)).state.value == "running"
            assert await store.list_outbox() == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_expired_execution_lease_fences_live_task_projection(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store = SQLiteStore(tmp_path / "runtime.sqlite", clock=lambda: base)
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(
                task.task_id, "worker", lease_seconds=60, now=base
            )
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
                now=base,
            )
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE task_executions SET lease_expires_at=? "
                    "WHERE execution_id=?",
                    (
                        (base - timedelta(seconds=1)).isoformat(
                            timespec="microseconds"
                        ),
                        claim.execution_id,
                    ),
                ).rowcount
            )

            with pytest.raises(
                StoreError, match="execution lease could not be renewed"
            ):
                await store.renew_task_lease(
                    task.task_id, claim.claim_token, now=base
                )
            assert not await store.set_task_thread(
                task.task_id,
                thread_id="stale-thread",
                claim_token=claim.claim_token,
                now=base,
            )
            assert not await store.transition_task(
                task.task_id,
                "completed",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
                result="stale",
                now=base,
            )
            with pytest.raises(StoreError, match="execution lease has expired"):
                await store.append_task_event(
                    task.task_id,
                    {"content": "stale", "execution_id": claim.execution_id},
                    claim_token=claim.claim_token,
                )
            with pytest.raises(StoreError, match="execution lease has expired"):
                await store.complete_task(
                    task.task_id,
                    "stale",
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                    now=base,
                )
            assert await store.list_task_events(task.task_id) == []
            assert await store.list_outbox() == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_task_event_uses_transaction_time_after_waiting_for_store(tmp_path):
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
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(
                task.task_id, "worker", lease_seconds=1, now=base
            )
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
                now=base,
            )

            def hold_store(_conn) -> None:
                started.set()
                assert release.wait(timeout=2)

            blocker = asyncio.create_task(store._call(hold_store))
            assert await asyncio.to_thread(started.wait, 1)
            pending = asyncio.create_task(
                store.append_task_event(
                    task.task_id,
                    {"content": "late", "execution_id": claim.execution_id},
                    claim_token=claim.claim_token,
                )
            )
            await asyncio.sleep(0)
            current_time[0] = base + timedelta(seconds=2)
            release.set()
            await blocker

            with pytest.raises(StoreError, match="lease has expired"):
                await pending
            assert await store.list_task_events(task.task_id) == []
            assert await store.list_outbox() == []
        finally:
            release.set()
            await store.close()

    asyncio.run(scenario())


def test_explicit_cancel_terminalizes_orphaned_task(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claimed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
            claim = await store.claim_task_by_id(
                task.task_id,
                "worker-a",
                lease_seconds=1,
                now=claimed_at,
            )
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
                now=claimed_at,
            )
            assert (
                await store.reconcile(now=claimed_at + timedelta(seconds=2))
            ).tasks_orphaned == 1

            cancelled_at = claimed_at + timedelta(seconds=3)
            assert await store.cancel_task(
                task.task_id,
                actor="user",
                now=cancelled_at,
            )
            cancelled = await store.get_task(task.task_id)
            assert cancelled is not None
            assert cancelled.state.value == "cancelled"
            assert cancelled.terminal_at == cancelled_at

            execution = await store.get_execution(claim.execution_id)
            assert execution is not None
            assert execution.state.value == "orphaned"
            events = await store.list_task_events(task.task_id)
            assert [event.event_type for event in events] == [
                "orphaned",
                "cancelled",
            ]
            assert events[-1].content == "user"

            # Command redelivery acknowledges the durable terminal state
            # without appending a second cancellation event.
            assert await store.cancel_task(task.task_id, actor="user")
            assert await store.list_task_events(task.task_id) == events
        finally:
            await store.close()

    asyncio.run(scenario())


def test_retry_clears_result_and_events_are_fenced_to_active_execution(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            first_claim = await store.claim_task_by_id(task.task_id, "worker-a")
            assert first_claim is not None
            assert await store.mark_task_running(
                task.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            await store.complete_task(
                task.task_id,
                result={"status": "failed", "diagnostic": "first attempt"},
                claim_token=first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )

            retried = await store.retry_task(task.task_id, actor="user")
            assert retried is not None
            assert retried.state.value == "queued"
            assert retried.result is None
            second_claim = await store.claim_task_by_id(task.task_id, "worker-b")
            assert second_claim is not None
            assert await store.mark_task_running(
                task.task_id,
                second_claim.claim_token,
                execution_id=second_claim.execution_id,
            )

            with pytest.raises(StoreError, match="active execution"):
                await store.append_task_event(
                    task.task_id,
                    {
                        "event_id": "stale-execution-event",
                        "execution_id": first_claim.execution_id,
                        "content": "stale",
                    },
                    claim_token=second_claim.claim_token,
                )

            current_event = await store.append_task_event(
                task.task_id,
                {"event_id": "current-execution-event", "content": "current"},
                claim_token=second_claim.claim_token,
                defer_user_projection=True,
            )
            assert current_event.execution_id == second_claim.execution_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_completion_rolls_back_events_when_projection_fails(tmp_path):
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

            with pytest.raises(StoreError, match="reply target message"):
                await store.complete_task(
                    task.task_id,
                    status="completed",
                    events=[
                        {
                            "event_id": "invalid-agent-reply",
                            "execution_id": claim.execution_id,
                            "event_type": "agent_response",
                            "destination_agent_id": "planner",
                            "reply_to_id": "missing-message",
                            "content": "cannot project",
                        }
                    ],
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                )

            current = await store.get_task(task.task_id)
            assert current is not None
            assert current.state.value == "running"
            assert await store.list_task_events(task.task_id) == []
            execution = await store.get_execution(claim.execution_id)
            assert execution is not None
            assert execution.state.value == "running"
            assert execution.finished_at is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_transition_task_rejects_a_missing_active_execution(tmp_path):
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

            # Simulate a corrupt/partially migrated database where the task
            # still advertises active ownership but its execution disappeared.
            await store._call(
                lambda conn: conn.execute(
                    "DELETE FROM task_executions WHERE execution_id=?",
                    (claim.execution_id,),
                ).rowcount
            )

            with pytest.raises(StoreError, match="execution"):
                await store.transition_task(
                    task.task_id,
                    "completed",
                    from_states=("running",),
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                    result={"output": "must not be delivered"},
                )

            current = await store.get_task(task.task_id)
            assert current is not None
            assert current.state.value == "running"
            assert await store.list_task_events(task.task_id) == []
            assert [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_complete_task_fails_closed_when_unfinished_executions_are_ambiguous(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            claim = await store.claim_task_by_id(task.task_id, "worker-a")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )

            # Simulate a crash/repair that left a second unfinished attempt
            # behind while the task projection still points at the first one.
            await store._call(
                lambda conn: conn.execute(
                    """INSERT INTO task_executions
                       (execution_id, task_id, attempt, state, worker_id,
                        claim_token, lease_expires_at, created_at)
                       VALUES (?, ?, ?, 'running', ?, ?, ?, ?)""",
                    (
                        "ambiguous-execution",
                        task.task_id,
                        99,
                        "worker-b",
                        "ambiguous-token",
                        "2099-01-01T00:00:00+00:00",
                        "2026-01-01T00:00:00+00:00",
                    ),
                ).rowcount
            )

            with pytest.raises(StoreError, match="ambiguous active execution"):
                await store.complete_task(
                    task.task_id,
                    status="completed",
                    result={"output": "must not commit"},
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                )

            current = await store.get_task(task.task_id)
            assert current is not None
            assert current.state.value == "running"
            assert await store.list_task_events(task.task_id) == []
            executions = await store.list_task_executions(task.task_id)
            assert len(executions) == 2
            assert all(item.finished_at is None for item in executions)
            assert [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_transition_task_rolls_back_terminal_projection_when_task_update_loses_row(
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
            await store._call(
                lambda conn: conn.execute(
                    """CREATE TEMP TRIGGER ignore_terminal_task_update
                       BEFORE UPDATE OF state ON tasks
                       WHEN NEW.state = 'completed'
                       BEGIN SELECT RAISE(IGNORE); END"""
                )
            )

            with pytest.raises(StoreError, match="cannot transition"):
                await store.transition_task(
                    task.task_id,
                    "completed",
                    from_states=("running",),
                    claim_token=claim.claim_token,
                    execution_id=claim.execution_id,
                    result={"output": "must roll back"},
                )

            current = await store.get_task(task.task_id)
            execution = await store.get_execution(claim.execution_id)
            assert current is not None
            assert current.state.value == "running"
            assert execution is not None
            assert execution.state.value == "running"
            assert execution.finished_at is None
            assert await store.list_task_events(task.task_id) == []
            assert [
                item
                for item in await store.list_outbox()
                if item.task_id == task.task_id
            ] == []
        finally:
            await store.close()

    asyncio.run(scenario())
