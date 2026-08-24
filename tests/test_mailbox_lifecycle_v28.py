"""Immutable mailbox expiry and reviewed orphan maintenance (schema v28)."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.maintenance_authority import MailboxMaintenanceAuthority
from src.runtime.models import MailboxOrphanReviewOutcome
from src.runtime.registry import codex_profile
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError


BASE = datetime(2026, 8, 15, tzinfo=timezone.utc)
REVIEW_ACTOR = "administrator"
REVIEW_REASON = "operator reviewed the orphan"


def _maintenance_store(path, **kwargs) -> SQLiteStore:
    authority = MailboxMaintenanceAuthority(
        authorization_source="test-admin-policy",
        clock=lambda: BASE,
    )
    return SQLiteStore(
        path,
        mailbox_maintenance_authority=authority,
        **kwargs,
    )


def _task(task_id: str) -> dict[str, object]:
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
        "inputs": {"text": task_id},
    }


async def _orphan_mailbox(
    store: SQLiteStore,
    suffix: str,
    *,
    expires_at: datetime | None = None,
):
    await store.put_profile(
        codex_profile(profile_version=3, allow_dynamic_peers=True),
    )
    item = await store.create_agent_message(
        source_agent_id="planner",
        destination_agent_id="codex",
        content=f"work {suffix}",
        request_id=f"request-{suffix}",
        message_id=f"message-{suffix}",
        payload={"request_type": "ask"},
        expires_at=expires_at,
        now=BASE,
    )
    claimed = (
        await store.claim_mailbox(
            "codex",
            f"worker-{suffix}",
            lease_seconds=100,
            now=BASE + timedelta(seconds=1),
        )
    )[0]
    assert claimed.claim_token
    assert await store.mark_mailbox_processing(
        claimed.mailbox_id,
        claimed.claim_token,
        now=BASE + timedelta(seconds=2),
    )
    assert await store.mark_mailbox_failed(
        claimed.mailbox_id,
        claimed.claim_token,
        error=f"orphan {suffix}",
        now=BASE + timedelta(seconds=3),
    )
    return item


def _review_kwargs(
    store: SQLiteStore,
    mailbox_message_id: str,
    expected_invocation_id: str,
    maintenance_id: str,
    action: str,
    *,
    actor: str = REVIEW_ACTOR,
    reason: str = REVIEW_REASON,
) -> dict[str, object]:
    authority = store._mailbox_maintenance_authority
    assert authority is not None
    return {
        "actor": actor,
        "reason": reason,
        "maintenance_grant": authority.issue_mailbox_orphan_review(
            mailbox_message_id,
            expected_invocation_id,
            maintenance_id,
            action,
            actor=actor,
            reason=reason,
        ),
    }


def test_v28_migrates_expiry_and_seeds_append_only_invocation_history(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        task = await store.create_task(_task("migration-task"), now=BASE)
        mailbox = await store.create_agent_message(
            source_agent_id="planner",
            destination_agent_id="codex",
            content="migration mailbox",
            request_id="migration-request",
            message_id="migration-message",
            now=BASE,
        )
        await store.close()

        # Model a v27 database while retaining the additive columns that a
        # process could have committed before its v28 marker.
        with sqlite3.connect(path) as connection:
            trigger_names = [
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger' "
                    "AND (name LIKE 'trg_agent_invocation%event%' "
                    "OR name LIKE 'trg_agent_invocation_expiry_%' "
                    "OR name LIKE 'trg_agent_mailbox_expiry_%' "
                    "OR name LIKE 'trg_mailbox_orphan_reviews_%')"
                ).fetchall()
            ]
            for name in trigger_names:
                connection.execute(f'DROP TRIGGER "{name}"')
            connection.execute("DROP TABLE mailbox_orphan_reviews")
            connection.execute("DROP TABLE agent_invocation_events")
            connection.execute("UPDATE agent_mailbox SET expires_at=NULL")
            connection.execute(
                "UPDATE agent_invocations SET expires_at=NULL "
                "WHERE work_kind='mailbox'"
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (28, 29, 30, 31, 32, 33, 34, 35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        # This regression verifies migration output, not age-dependent startup
        # recovery of the deliberately old queued fixture.
        await migrated.initialize(recover_startup_state=False)
        try:
            facts = await migrated._call(
                lambda conn: {
                    "version": conn.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0],
                    "mailbox_expiry": conn.execute(
                        "SELECT expires_at FROM agent_mailbox WHERE mailbox_id=?",
                        (mailbox.mailbox_id,),
                    ).fetchone()[0],
                    "invocation_expiry": conn.execute(
                        "SELECT expires_at FROM agent_invocations "
                        "WHERE invocation_id=?",
                        (mailbox.current_invocation_id,),
                    ).fetchone()[0],
                    "task_expiry": conn.execute(
                        "SELECT expires_at FROM agent_invocations "
                        "WHERE invocation_id=?",
                        (task.execution_id,),
                    ).fetchone()[0],
                    "events": [
                        tuple(row)
                        for row in conn.execute(
                            "SELECT invocation_id,event_sequence,event_kind "
                            "FROM agent_invocation_events ORDER BY invocation_id"
                        ).fetchall()
                    ],
                    "foreign_keys": conn.execute(
                        "PRAGMA foreign_key_check"
                    ).fetchall(),
                },
                allow_deferred_startup=True,
            )
            assert facts["version"] == 35
            assert facts["mailbox_expiry"] == facts["invocation_expiry"]
            assert facts["mailbox_expiry"] == (
                BASE + timedelta(days=1)
            ).isoformat(timespec="microseconds")
            assert facts["task_expiry"] is None
            assert {row[2] for row in facts["events"]} == {
                "migration_snapshot"
            }
            assert facts["foreign_keys"] == []

            with pytest.raises(sqlite3.IntegrityError, match="expiry is immutable"):
                await migrated._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_mailbox SET expires_at=? WHERE mailbox_id=?",
                        ((BASE + timedelta(days=2)).isoformat(), mailbox.mailbox_id),
                    ),
                    allow_deferred_startup=True,
                )
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                await migrated._call(
                    lambda conn: conn.execute(
                        "DELETE FROM agent_invocation_events WHERE invocation_id=?",
                        (mailbox.current_invocation_id,),
                    ),
                    allow_deferred_startup=True,
                )
        finally:
            await migrated.close()

    asyncio.run(scenario())


def test_pending_expiry_refuses_claim_and_releases_admission_once(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite", mailbox_ttl_seconds=2)
        await store.initialize()
        try:
            item = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="short lived",
                request_id="expiry-request",
                message_id="expiry-message",
                now=BASE,
            )
            replay = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="short lived",
                request_id="expiry-request",
                message_id="expiry-message",
                now=BASE + timedelta(hours=1),
            )
            assert replay.expires_at == item.expires_at
            with pytest.raises(StoreError, match="expiry conflicts"):
                await store.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="codex",
                    content="short lived",
                    request_id="expiry-request",
                    message_id="expiry-message",
                    expires_at=BASE + timedelta(seconds=3),
                    now=BASE,
                )

            assert await store.claim_mailbox(
                "codex", "late-worker", now=BASE + timedelta(seconds=2)
            ) == []
            report = await store.reconcile(now=BASE + timedelta(seconds=2))
            assert report.mailbox_expired == 1
            assert (await store.reconcile(
                now=BASE + timedelta(seconds=3)
            )).mailbox_expired == 0
            retained = await store.get_mailbox_item(item.mailbox_id)
            invocation = await store.get_agent_invocation(
                str(item.current_invocation_id)
            )
            assert retained is not None and retained.state.value == "expired"
            assert invocation is not None and invocation.state.value == "failed"
            assert invocation.admission_released_at is not None
            assert (await store.get_global_agent_admission_counter()).unfinished_count == 0
            assert [
                event.new_state.value
                for event in await store.list_agent_invocation_events(
                    str(item.current_invocation_id)
                )
            ] == ["queued", "failed"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_reviewed_retry_is_fresh_fenced_and_replay_stable(tmp_path):
    async def scenario() -> None:
        store = _maintenance_store(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, "retry")
            old_id = str(mailbox.current_invocation_id)
            before = await store.get_agent_admission_counter("codex", 1)
            review_kwargs = _review_kwargs(
                store,
                mailbox.message_id,
                old_id,
                "maintenance-retry",
                "retry",
            )
            result = await store.review_mailbox_orphan(
                mailbox.message_id,
                old_id,
                "maintenance-retry",
                "retry",
                now=BASE + timedelta(seconds=4),
                **review_kwargs,
            )
            assert result.outcome is MailboxOrphanReviewOutcome.RETRIED
            assert result.replacement_invocation_id not in {None, old_id}
            replay = await store.review_mailbox_orphan(
                mailbox.message_id,
                old_id,
                "maintenance-retry",
                "retry",
                now=BASE + timedelta(days=1),
                **review_kwargs,
            )
            assert replay.replayed and replay == result.__class__(
                **{**result.__dict__, "replayed": True}
            )
            with pytest.raises(StoreError, match="maintenance identity conflicts"):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    old_id,
                    "maintenance-retry",
                    "retry",
                    **_review_kwargs(
                        store,
                        mailbox.message_id,
                        old_id,
                        "maintenance-retry",
                        "retry",
                        actor="different-admin",
                    ),
                )
            with pytest.raises(InvalidTransition, match="current invocation"):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    old_id,
                    "maintenance-stale",
                    "retry",
                    now=BASE + timedelta(seconds=5),
                    **_review_kwargs(
                        store,
                        mailbox.message_id,
                        old_id,
                        "maintenance-stale",
                        "retry",
                    ),
                )

            aggregate = await store.get_mailbox_item(mailbox.mailbox_id)
            old = await store.get_agent_invocation(old_id)
            replacement = await store.get_agent_invocation(
                str(result.replacement_invocation_id)
            )
            after = await store.get_agent_admission_counter("codex", 1)
            assert aggregate is not None and aggregate.state.value == "pending"
            assert aggregate.current_invocation_id == result.replacement_invocation_id
            assert old is not None and old.state.value == "orphaned"
            assert replacement is not None and replacement.state.value == "queued"
            assert replacement.work_id == mailbox.message_id
            assert replacement.mailbox_id == mailbox.mailbox_id
            assert replacement.expires_at == mailbox.expires_at
            assert before is not None and after is not None
            assert replacement.ready_sequence == before.next_ready_sequence
            assert after.unfinished_count == before.unfinished_count + 1
            assert after.next_ready_sequence == before.next_ready_sequence + 1
            assert (
                await store.list_agent_invocation_events(old_id)
            )[-1].event_kind == "orphan_review_resolved"

            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE mailbox_orphan_reviews SET reason='changed' "
                        "WHERE mailbox_maintenance_id='maintenance-retry'"
                    )
                )
            with pytest.raises(StoreError, match="caller-supplied"):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    str(result.replacement_invocation_id),
                    "maintenance-unauthorized",
                    "dead_letter",
                    actor=REVIEW_ACTOR,
                    reason=REVIEW_REASON,
                    administrator_authorized=False,
                )
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "profile_change",
    ["request_acl_removed", "request_acl_denied", "disabled"],
)
def test_reviewed_retry_requires_current_destination_profile_eligibility(
    tmp_path,
    profile_change,
):
    async def scenario() -> None:
        store = _maintenance_store(tmp_path / f"runtime-{profile_change}.sqlite")
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, profile_change)
            old_id = str(mailbox.current_invocation_id)
            if profile_change == "request_acl_removed":
                replacement_profile = codex_profile(
                    profile_version=4,
                    allow_dynamic_peers=False,
                )
            elif profile_change == "request_acl_denied":
                replacement_profile = replace(
                    codex_profile(
                        profile_version=4,
                        allow_dynamic_peers=True,
                    ),
                    denied_request_types=frozenset({"ask"}),
                )
            else:
                replacement_profile = replace(
                    codex_profile(
                        profile_version=4,
                        allow_dynamic_peers=True,
                    ),
                    enabled=False,
                )
            await store.put_profile(replacement_profile)

            lifecycle = await store.get_agent_lifecycle("codex", 1)
            assert lifecycle is not None and lifecycle.profile_version == 4
            if profile_change != "disabled":
                assert lifecycle.lifecycle_state.value == "enabled"
            else:
                assert lifecycle.lifecycle_state.value == "disabled"

            rejected = await store.review_mailbox_orphan(
                mailbox.message_id,
                old_id,
                f"maintenance-{profile_change}",
                "retry",
                now=BASE + timedelta(seconds=5),
                **_review_kwargs(
                    store,
                    mailbox.message_id,
                    old_id,
                    f"maintenance-{profile_change}",
                    "retry",
                ),
            )
            assert rejected.outcome is (
                MailboxOrphanReviewOutcome.REJECTED_AGENT_UNAVAILABLE
            )
            assert rejected.replacement_invocation_id is None
            retained = await store.get_mailbox_item(mailbox.mailbox_id)
            assert retained is not None
            assert retained.state.value == "orphaned_mailbox"
            assert retained.current_invocation_id == old_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_reviewed_retry_of_response_keeps_original_request_authority(tmp_path):
    async def scenario() -> None:
        store = _maintenance_store(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.put_profile(
                codex_profile(profile_version=3, allow_dynamic_peers=True),
            )
            planner_profile = replace(
                codex_profile(profile_version=3, allow_dynamic_peers=False),
                agent_id="planner",
                display_name="Planner",
            )
            await store.put_profile(planner_profile)
            original = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="request",
                request_id="response-retry-request",
                message_id="response-retry-original",
                payload={"request_type": "ask"},
                now=BASE,
            )
            response = await store.create_agent_message(
                source_agent_id="codex",
                destination_agent_id="planner",
                content="response",
                request_id=original.request_id,
                message_id="response-retry-response",
                reply_to_id=original.message_id,
                causation_id=original.message_id,
                payload={"request_type": "ask"},
                now=BASE,
            )
            claim = (
                await store.claim_mailbox(
                    "planner",
                    "response-worker",
                    lease_seconds=100,
                    now=BASE + timedelta(seconds=1),
                )
            )[0]
            assert claim.mailbox_id == response.mailbox_id and claim.claim_token
            assert await store.mark_mailbox_processing(
                claim.mailbox_id,
                claim.claim_token,
                now=BASE + timedelta(seconds=2),
            )
            assert await store.mark_mailbox_failed(
                claim.mailbox_id,
                claim.claim_token,
                error="orphaned response",
                now=BASE + timedelta(seconds=3),
            )

            old_id = str(response.current_invocation_id)
            retried = await store.review_mailbox_orphan(
                response.message_id,
                old_id,
                "maintenance-response-retry",
                "retry",
                now=BASE + timedelta(seconds=4),
                **_review_kwargs(
                    store,
                    response.message_id,
                    old_id,
                    "maintenance-response-retry",
                    "retry",
                ),
            )
            assert retried.outcome is MailboxOrphanReviewOutcome.RETRIED
            assert retried.replacement_invocation_id not in {None, old_id}
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("lifecycle_state", ["disabled", "retiring"])
def test_retry_rejects_unavailable_agent_but_dead_letter_remains_allowed(
    tmp_path, lifecycle_state
):
    async def scenario() -> None:
        store = _maintenance_store(
            tmp_path / f"runtime-{lifecycle_state}.sqlite"
        )
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, lifecycle_state)
            old_id = str(mailbox.current_invocation_id)
            retiring_at = (
                (BASE + timedelta(seconds=4)).isoformat()
                if lifecycle_state == "retiring"
                else None
            )
            await store._call(
                lambda conn: conn.execute(
                    """UPDATE agent_lifecycle
                          SET lifecycle_state=?,desired_process_state='stopped',
                              retiring_at=?,updated_at=?
                        WHERE agent_id='codex' AND agent_incarnation=1""",
                    (
                        lifecycle_state,
                        retiring_at,
                        (BASE + timedelta(seconds=4)).isoformat(),
                    ),
                )
            )
            rejected = await store.review_mailbox_orphan(
                mailbox.message_id,
                old_id,
                f"maintenance-{lifecycle_state}-retry",
                "retry",
                now=BASE + timedelta(seconds=5),
                **_review_kwargs(
                    store,
                    mailbox.message_id,
                    old_id,
                    f"maintenance-{lifecycle_state}-retry",
                    "retry",
                ),
            )
            assert rejected.outcome is (
                MailboxOrphanReviewOutcome.REJECTED_AGENT_UNAVAILABLE
            )
            assert (await store.get_mailbox_item(
                mailbox.mailbox_id
            )).state.value == "orphaned_mailbox"
            dead = await store.review_mailbox_orphan(
                mailbox.message_id,
                old_id,
                f"maintenance-{lifecycle_state}-dead",
                "dead_letter",
                now=BASE + timedelta(seconds=6),
                **_review_kwargs(
                    store,
                    mailbox.message_id,
                    old_id,
                    f"maintenance-{lifecycle_state}-dead",
                    "dead_letter",
                ),
            )
            assert dead.outcome is MailboxOrphanReviewOutcome.DEAD_LETTERED
            assert (await store.get_mailbox_item(
                mailbox.mailbox_id
            )).state.value == "dead_letter"
            assert (await store.get_global_agent_admission_counter()).unfinished_count == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_expired_and_queue_full_retry_rejections_are_durable_without_gaps(tmp_path):
    async def scenario() -> None:
        expired_store = _maintenance_store(tmp_path / "expired.sqlite")
        await expired_store.initialize()
        try:
            expired = await _orphan_mailbox(
                expired_store,
                "expired",
                expires_at=BASE + timedelta(seconds=2),
            )
            old_id = str(expired.current_invocation_id)
            rejected = await expired_store.review_mailbox_orphan(
                expired.message_id,
                old_id,
                "maintenance-expired-retry",
                "retry",
                now=BASE + timedelta(seconds=4),
                **_review_kwargs(
                    expired_store,
                    expired.message_id,
                    old_id,
                    "maintenance-expired-retry",
                    "retry",
                ),
            )
            assert rejected.outcome is MailboxOrphanReviewOutcome.REJECTED_EXPIRED
            dead = await expired_store.review_mailbox_orphan(
                expired.message_id,
                old_id,
                "maintenance-expired-dead",
                "dead_letter",
                now=BASE + timedelta(seconds=5),
                **_review_kwargs(
                    expired_store,
                    expired.message_id,
                    old_id,
                    "maintenance-expired-dead",
                    "dead_letter",
                ),
            )
            assert dead.outcome is MailboxOrphanReviewOutcome.DEAD_LETTERED
        finally:
            await expired_store.close()

        store = _maintenance_store(
            tmp_path / "queue.sqlite",
            max_agent_queue=1,
            max_global_queue=1,
        )
        await store.initialize()
        try:
            orphan = await _orphan_mailbox(store, "queue-orphan")
            old_id = str(orphan.current_invocation_id)
            blocker = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="capacity blocker",
                request_id="queue-blocker-request",
                message_id="queue-blocker-message",
                now=BASE + timedelta(seconds=4),
            )
            before = await store.get_agent_admission_counter("codex", 1)
            queue_full_kwargs = _review_kwargs(
                store,
                orphan.message_id,
                old_id,
                "maintenance-queue-full",
                "retry",
            )
            rejected = await store.review_mailbox_orphan(
                orphan.message_id,
                old_id,
                "maintenance-queue-full",
                "retry",
                now=BASE + timedelta(seconds=5),
                **queue_full_kwargs,
            )
            after_rejection = await store.get_agent_admission_counter("codex", 1)
            assert rejected.outcome is (
                MailboxOrphanReviewOutcome.REJECTED_QUEUE_FULL
            )
            assert after_rejection == before
            assert (await store.get_mailbox_item(
                orphan.mailbox_id
            )).current_invocation_id == old_id

            claimed = (
                await store.claim_mailbox(
                    "codex", "capacity-worker", lease_seconds=100,
                    now=BASE + timedelta(seconds=6)
                )
            )[0]
            assert claimed.mailbox_id == blocker.mailbox_id and claimed.claim_token
            assert await store.mark_mailbox_processing(
                claimed.mailbox_id,
                claimed.claim_token,
                now=BASE + timedelta(seconds=7),
            )
            assert await store.mark_mailbox_failed(
                claimed.mailbox_id,
                claimed.claim_token,
                error="finished blocker",
                dead_letter=True,
                now=BASE + timedelta(seconds=8),
            )
            replay = await store.review_mailbox_orphan(
                orphan.message_id,
                old_id,
                "maintenance-queue-full",
                "retry",
                now=BASE + timedelta(seconds=9),
                **queue_full_kwargs,
            )
            assert replay.replayed
            assert replay.outcome is MailboxOrphanReviewOutcome.REJECTED_QUEUE_FULL
            retried = await store.review_mailbox_orphan(
                orphan.message_id,
                old_id,
                "maintenance-queue-retry-new-id",
                "retry",
                now=BASE + timedelta(seconds=10),
                **_review_kwargs(
                    store,
                    orphan.message_id,
                    old_id,
                    "maintenance-queue-retry-new-id",
                    "retry",
                ),
            )
            assert retried.outcome is MailboxOrphanReviewOutcome.RETRIED
            replacement = await store.get_agent_invocation(
                str(retried.replacement_invocation_id)
            )
            assert before is not None and replacement is not None
            assert replacement.ready_sequence == before.next_ready_sequence
        finally:
            await store.close()

    asyncio.run(scenario())
