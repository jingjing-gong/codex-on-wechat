"""Adversarial schema-v30 mailbox maintenance hardening tests."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import src.runtime.maintenance_authority as maintenance_module
from src.runtime.maintenance_authority import (
    MailboxMaintenanceAuthority,
    MaintenanceAuthorizationError,
    canonical_mailbox_review_authorization_digest,
    canonical_mailbox_review_payload_hash,
)
from src.runtime.registry import codex_profile
from src.runtime.sqlite_store import SQLiteStore, StoreError


BASE = datetime(2026, 8, 15, tzinfo=timezone.utc)


def _authority(clock=lambda: BASE) -> MailboxMaintenanceAuthority:
    return MailboxMaintenanceAuthority(
        authorization_source="test-supervisor",
        clock=clock,
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


async def _orphan_mailbox(store: SQLiteStore, suffix: str):
    await store.put_profile(
        codex_profile(profile_version=3, allow_dynamic_peers=True),
    )
    mailbox = await store.create_agent_message(
        source_agent_id="planner",
        destination_agent_id="codex",
        content=f"work {suffix}",
        request_id=f"request-{suffix}",
        message_id=f"message-{suffix}",
        payload={"request_type": "ask"},
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
        error="orphaned",
        now=BASE + timedelta(seconds=3),
    )
    return mailbox


def test_process_local_grants_are_exact_unforgeable_and_time_fenced(monkeypatch):
    current = [BASE]
    authority = _authority(lambda: current[0])
    grant = authority.issue_mailbox_orphan_review(
        "message-1",
        "invocation-1",
        "maintenance-1",
        "retry",
        actor="administrator",
        reason="reviewed",
    )
    assert authority.validate_mailbox_orphan_review(
        grant,
        "message-1",
        "invocation-1",
        "maintenance-1",
        "retry",
        actor="administrator",
        reason="reviewed",
    ) is grant

    with pytest.raises(MaintenanceAuthorizationError, match="does not match"):
        authority.validate_mailbox_orphan_review(
            grant,
            "message-1",
            "invocation-1",
            "maintenance-1",
            "retry",
            actor="different",
            reason="reviewed",
        )
    with pytest.raises(MaintenanceAuthorizationError, match="invalid"):
        authority.validate_mailbox_orphan_review(
            replace(grant, payload_digest="0" * 64),
            "message-1",
            "invocation-1",
            "maintenance-1",
            "retry",
            actor="administrator",
            reason="reviewed",
        )
    foreign = _authority().issue_mailbox_orphan_review(
        "message-1",
        "invocation-1",
        "maintenance-1",
        "retry",
        actor="administrator",
        reason="reviewed",
    )
    with pytest.raises(MaintenanceAuthorizationError, match="invalid"):
        authority.validate_mailbox_orphan_review(
            foreign,
            "message-1",
            "invocation-1",
            "maintenance-1",
            "retry",
            actor="administrator",
            reason="reviewed",
        )

    current[0] = BASE + timedelta(seconds=1)
    future = authority.issue_mailbox_orphan_review(
        "message-2",
        "invocation-2",
        "maintenance-2",
        "dead_letter",
        actor="administrator",
        reason="reviewed",
    )
    current[0] = BASE
    with pytest.raises(MaintenanceAuthorizationError, match="future"):
        authority.validate_mailbox_orphan_review(
            future,
            "message-2",
            "invocation-2",
            "maintenance-2",
            "dead_letter",
            actor="administrator",
            reason="reviewed",
        )

    current[0] = datetime(2026, 8, 15)
    with pytest.raises(ValueError, match="timezone"):
        authority.validate_mailbox_orphan_review(
            grant,
            "message-1",
            "invocation-1",
            "maintenance-1",
            "retry",
            actor="administrator",
            reason="reviewed",
        )
    current[0] = BASE

    revoked = authority.issue_mailbox_orphan_review(
        "message-revoked",
        "invocation-revoked",
        "maintenance-revoked",
        "retry",
        actor="administrator",
        reason="reviewed",
    )
    authority.revoke(revoked)
    with pytest.raises(MaintenanceAuthorizationError, match="invalid"):
        authority.validate_mailbox_orphan_review(
            revoked,
            "message-revoked",
            "invocation-revoked",
            "maintenance-revoked",
            "retry",
            actor="administrator",
            reason="reviewed",
        )

    naive = _authority(lambda: datetime(2026, 8, 15))
    with pytest.raises(ValueError, match="timezone"):
        naive.issue_mailbox_orphan_review(
            "message-3",
            "invocation-3",
            "maintenance-3",
            "retry",
            actor="administrator",
            reason="reviewed",
        )

    owner_pid = maintenance_module.os.getpid()
    monkeypatch.setattr(maintenance_module.os, "getpid", lambda: owner_pid + 1)
    with pytest.raises(MaintenanceAuthorizationError, match="process"):
        authority.validate_mailbox_orphan_review(
            grant,
            "message-1",
            "invocation-1",
            "maintenance-1",
            "retry",
            actor="administrator",
            reason="reviewed",
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"grant_id": []},
        {"grant_id": b"not-text"},
        {"grant_id": "not-ascii-界"},
        {"payload_digest": b"0" * 64},
        {"payload_hash": None},
        {"authorization_source": object()},
        {"authorized_at": "2026-08-15T00:00:00+00:00"},
        {"authorized_at": datetime(2026, 8, 15)},
    ],
)
def test_malformed_constructed_grants_fail_closed(changes):
    authority = _authority()
    grant = authority.issue_mailbox_orphan_review(
        "message-malformed",
        "invocation-malformed",
        "maintenance-malformed",
        "retry",
        actor="administrator",
        reason="reviewed",
    )
    malformed = replace(grant, **changes)

    with pytest.raises(MaintenanceAuthorizationError, match="invalid"):
        authority.validate_mailbox_orphan_review(
            malformed,
            "message-malformed",
            "invocation-malformed",
            "maintenance-malformed",
            "retry",
            actor="administrator",
            reason="reviewed",
        )
    authority.revoke(malformed)


def test_mailbox_review_is_disabled_without_injected_authority(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, "disabled")
            with pytest.raises(StoreError, match="maintenance is disabled"):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    str(mailbox.current_invocation_id),
                    "maintenance-disabled",
                    "dead_letter",
                    actor="caller",
                    reason="caller says yes",
                    authorized_at=BASE,
                    authorization_source="caller",
                    administrator_authorized=True,
                    now=BASE + timedelta(seconds=4),
                )
            assert await store.list_mailbox_orphan_reviews() == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_store_maps_malformed_constructed_grant_to_authorization_failure(tmp_path):
    async def scenario() -> None:
        authority = _authority()
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            mailbox_maintenance_authority=authority,
        )
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, "malformed-grant")
            expected_id = str(mailbox.current_invocation_id)
            grant = authority.issue_mailbox_orphan_review(
                mailbox.message_id,
                expected_id,
                "maintenance-malformed-grant",
                "retry",
                actor="administrator",
                reason="reviewed",
            )
            malformed = replace(grant, grant_id=[])
            with pytest.raises(StoreError, match="invalid"):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    expected_id,
                    "maintenance-malformed-grant",
                    "retry",
                    actor="administrator",
                    reason="reviewed",
                    maintenance_grant=malformed,
                    now=BASE + timedelta(seconds=4),
                )
            assert await store.list_mailbox_orphan_reviews() == []
            retained = await store.get_mailbox_item(mailbox.mailbox_id)
            assert retained is not None
            assert retained.state.value == "orphaned_mailbox"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_lost_response_uses_read_api_and_new_timestamp_cannot_reauthorize(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first_authority = _authority(lambda: BASE)
        first = SQLiteStore(
            path,
            mailbox_maintenance_authority=first_authority,
        )
        await first.initialize()
        mailbox = await _orphan_mailbox(first, "lost-response")
        expected_id = str(mailbox.current_invocation_id)
        grant = first_authority.issue_mailbox_orphan_review(
            mailbox.message_id,
            expected_id,
            "maintenance-lost-response",
            "dead_letter",
            actor="administrator",
            reason="reviewed",
        )
        kwargs = {
            "actor": "administrator",
            "reason": "reviewed",
            "maintenance_grant": grant,
        }
        committed = await first.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            "maintenance-lost-response",
            "dead_letter",
            now=BASE + timedelta(seconds=4),
            **kwargs,
        )
        replay = await first.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            "maintenance-lost-response",
            "dead_letter",
            now=BASE + timedelta(seconds=5),
            **kwargs,
        )
        assert replay.replayed
        assert replay.authorization_grant_digest == (
            committed.authorization_grant_digest
        )
        assert replay.authorization_scheme == "process_grant_v1"
        await first.close()

        # Process-local capabilities intentionally do not survive a
        # supervisor restart.  Lost-response recovery is the immutable read by
        # maintenance ID, not mutation reauthorization.
        reader = SQLiteStore(path)
        await reader.initialize()
        retained = await reader.get_mailbox_orphan_review(
            "maintenance-lost-response"
        )
        assert retained is not None and retained == committed
        await reader.close()

        second_authority = _authority(lambda: BASE + timedelta(seconds=6))
        second = SQLiteStore(
            path,
            mailbox_maintenance_authority=second_authority,
        )
        await second.initialize()
        try:
            replacement_grant = second_authority.issue_mailbox_orphan_review(
                mailbox.message_id,
                expected_id,
                "maintenance-lost-response",
                "dead_letter",
                actor="administrator",
                reason="reviewed",
            )
            with pytest.raises(
                StoreError,
                match="maintenance identity conflicts",
            ):
                await second.review_mailbox_orphan(
                    mailbox.message_id,
                    expected_id,
                    "maintenance-lost-response",
                    "dead_letter",
                    actor="administrator",
                    reason="reviewed",
                    maintenance_grant=replacement_grant,
                    now=BASE + timedelta(seconds=7),
                )
        finally:
            await second.close()

    asyncio.run(scenario())


def test_v28_migration_canonicalizes_semantically_equal_expiry(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        mailbox = await created.create_agent_message(
            source_agent_id="planner",
            destination_agent_id="codex",
            content="canonicalize",
            request_id="canonical-request",
            message_id="canonical-message",
            now=BASE,
        )
        await created.close()

        noncanonical = (BASE + timedelta(days=1, hours=8)).isoformat(
            timespec="seconds"
        ).replace("+00:00", "+08:00")
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
            connection.execute(
                "UPDATE agent_mailbox SET expires_at=? WHERE mailbox_id=?",
                (noncanonical, mailbox.mailbox_id),
            )
            connection.execute(
                "UPDATE agent_invocations SET expires_at=? "
                "WHERE invocation_id=?",
                (noncanonical, mailbox.current_invocation_id),
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (28,29,30,31,32,33,34,35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            expected = (BASE + timedelta(days=1)).isoformat(
                timespec="microseconds"
            )
            retained = await migrated._call(
                lambda conn: tuple(
                    conn.execute(
                        "SELECT mailbox.expires_at,invocation.expires_at "
                        "FROM agent_mailbox AS mailbox "
                        "JOIN agent_invocations AS invocation "
                        "ON invocation.invocation_id=mailbox.current_invocation_id "
                        "WHERE mailbox.mailbox_id=?",
                        (mailbox.mailbox_id,),
                    ).fetchone()
                )
            )
            assert retained == (expected, expected)
        finally:
            await migrated.close()

    asyncio.run(scenario())


def test_v29_to_v30_expiry_normalizes_atomically_and_conflicts_roll_back(
    tmp_path,
):
    async def seed(path, suffix: str, invocation_expiry: str):
        created = SQLiteStore(path)
        await created.initialize()
        mailbox = await created.create_agent_message(
            source_agent_id="planner",
            destination_agent_id="codex",
            content=suffix,
            request_id=f"request-{suffix}",
            message_id=f"message-{suffix}",
            now=BASE,
        )
        await created.close()
        mailbox_expiry = "2026-08-16T08:00:00+08:00"
        with sqlite3.connect(path) as connection:
            trigger_sql = [
                str(row[0])
                for row in connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' "
                    "AND name IN ('trg_agent_mailbox_expiry_immutable',"
                    "'trg_agent_invocation_expiry_update_valid') "
                    "ORDER BY name"
                ).fetchall()
            ]
            connection.execute(
                "DROP TRIGGER trg_agent_mailbox_expiry_immutable"
            )
            connection.execute(
                "DROP TRIGGER trg_agent_invocation_expiry_update_valid"
            )
            connection.execute(
                "UPDATE agent_mailbox SET expires_at=? WHERE mailbox_id=?",
                (mailbox_expiry, mailbox.mailbox_id),
            )
            connection.execute(
                "UPDATE agent_invocations SET expires_at=? "
                "WHERE invocation_id=?",
                (invocation_expiry, mailbox.current_invocation_id),
            )
            for statement in trigger_sql:
                connection.execute(statement)
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35)"
            )
            connection.commit()
        return mailbox, mailbox_expiry

    async def scenario() -> None:
        valid_path = tmp_path / "valid.sqlite"
        valid, _raw = await seed(
            valid_path,
            "valid",
            "2026-08-16T08:00:00+08:00",
        )
        migrated = SQLiteStore(valid_path)
        await migrated.initialize()
        try:
            expected = "2026-08-16T00:00:00.000000+00:00"
            assert await migrated._call(
                lambda conn: tuple(
                    conn.execute(
                        "SELECT mailbox.expires_at,invocation.expires_at "
                        "FROM agent_mailbox AS mailbox "
                        "JOIN agent_invocations AS invocation "
                        "ON invocation.invocation_id=mailbox.current_invocation_id "
                        "WHERE mailbox.mailbox_id=?",
                        (valid.mailbox_id,),
                    ).fetchone()
                )
            ) == (expected, expected)
        finally:
            await migrated.close()

        invalid_path = tmp_path / "invalid.sqlite"
        invalid, raw_mailbox = await seed(
            invalid_path,
            "invalid",
            "2026-08-16T08:00:01+08:00",
        )
        rejected = SQLiteStore(invalid_path)
        with pytest.raises(StoreError, match="invocation expiry conflicts"):
            await rejected.initialize()
        await rejected.close()
        with sqlite3.connect(invalid_path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (29,)
            assert tuple(
                connection.execute(
                    "SELECT mailbox.expires_at,invocation.expires_at "
                    "FROM agent_mailbox AS mailbox "
                    "JOIN agent_invocations AS invocation "
                    "ON invocation.invocation_id=mailbox.current_invocation_id "
                    "WHERE mailbox.mailbox_id=?",
                    (invalid.mailbox_id,),
                ).fetchone()
            ) == (raw_mailbox, "2026-08-16T08:00:01+08:00")
            triggers = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger' "
                    "AND name IN ('trg_agent_mailbox_expiry_immutable',"
                    "'trg_agent_invocation_expiry_update_valid')"
                ).fetchall()
            }
            assert triggers == {
                "trg_agent_mailbox_expiry_immutable",
                "trg_agent_invocation_expiry_update_valid",
            }

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("invocation_event_id", "forged-event-id"),
        ("event_kind", "migration_snapshot"),
        ("new_state", "failed"),
        ("source_kind", "forged-source"),
        ("source_id", "forged-source-id"),
        ("metadata_json", '{"forged":true}'),
        (
            "created_at",
            (BASE - timedelta(days=1)).isoformat(timespec="microseconds"),
        ),
    ],
)
def test_v28_marker_replay_rejects_each_noncanonical_sequence_one_field(
    tmp_path,
    column,
    value,
):
    async def scenario() -> None:
        path = tmp_path / f"runtime-{column}.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        task = await created.create_task(_task(f"seed-{column}"), now=BASE)
        await created.close()

        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP TRIGGER trg_agent_invocation_events_no_update"
            )
            connection.execute(
                f"UPDATE agent_invocation_events SET {column}=? "
                "WHERE invocation_id=? AND event_sequence=1",
                (value, task.execution_id),
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (28,29,30,31,32,33,34,35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="sequence-one identity conflicts"):
            await migrated.initialize()
        await migrated.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (27,)

    asyncio.run(scenario())


def test_v30_rejects_lone_migration_snapshot_timestamp_corruption(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        task = await created.create_task(_task("migration-time-corrupt"), now=BASE)
        await created.close()

        corrupted_at = (BASE - timedelta(days=1)).isoformat(
            timespec="microseconds"
        )
        with sqlite3.connect(path) as connection:
            trigger_sql = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' "
                    "AND name='trg_agent_invocation_events_no_update'"
                ).fetchone()[0]
            )
            connection.execute(
                "DROP TRIGGER trg_agent_invocation_events_no_update"
            )
            connection.execute(
                """UPDATE agent_invocation_events
                      SET invocation_event_id=?,event_kind='migration_snapshot',
                          source_kind='migration',source_id='schema-v28',
                          metadata_json='{"schema_version":28}',created_at=?
                    WHERE invocation_id=? AND event_sequence=1""",
                (
                    "invocation-event:migration-v28:" + task.execution_id,
                    corrupted_at,
                    task.execution_id,
                ),
            )
            connection.execute(trigger_sql)
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="sequence-one identity conflicts"):
            await migrated.initialize()
        await migrated.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (29,)
            assert connection.execute(
                "SELECT created_at FROM agent_invocation_events "
                "WHERE invocation_id=? AND event_sequence=1",
                (task.execution_id,),
            ).fetchone() == (corrupted_at,)

    asyncio.run(scenario())


def _review_row(
    *,
    mailbox,
    expected_invocation_id: str,
    maintenance_id: str,
    action: str,
    outcome: str,
    replacement_invocation_id: str | None = None,
    scheme: str = "process_grant_v1",
) -> tuple[object, ...]:
    actor = "administrator"
    reason = "reviewed"
    source = "test-supervisor"
    payload_hash = canonical_mailbox_review_payload_hash(
        mailbox.message_id,
        expected_invocation_id,
        action,
    )
    digest = canonical_mailbox_review_authorization_digest(
        mailbox_message_id=mailbox.message_id,
        expected_current_invocation_id=expected_invocation_id,
        mailbox_maintenance_id=maintenance_id,
        action=action,
        actor=actor,
        reason=reason,
        authorization_source=source,
        authorized_at=BASE,
    )
    return (
        maintenance_id,
        payload_hash,
        digest,
        scheme,
        mailbox.mailbox_id,
        mailbox.message_id,
        expected_invocation_id,
        action,
        actor,
        reason,
        BASE.isoformat(timespec="microseconds"),
        source,
        1,
        outcome,
        replacement_invocation_id,
        (BASE + timedelta(seconds=4)).isoformat(timespec="microseconds"),
    )


def test_v30_compound_fks_reject_expected_and_replacement_recombination(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            first = await _orphan_mailbox(store, "first")
            second = await _orphan_mailbox(store, "second")
            replacement = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="replacement from another mailbox",
                request_id="request-replacement",
                message_id="message-replacement",
                now=BASE + timedelta(seconds=4),
            )
            insert_sql = """INSERT INTO mailbox_orphan_reviews (
                mailbox_maintenance_id,payload_hash,
                authorization_grant_digest,authorization_scheme,mailbox_id,
                mailbox_message_id,expected_current_invocation_id,action,
                actor,reason,authorized_at,authorization_source,
                administrator_authorized,outcome,replacement_invocation_id,
                reviewed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

            def drop_review_effect_triggers(conn):
                for name in (
                    "trg_mailbox_orphan_reviews_insert_valid",
                    "trg_mailbox_orphan_reviews_resolution_event",
                ):
                    conn.execute(f'DROP TRIGGER "{name}"')

            await store._call(drop_review_effect_triggers)
            with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                await store._call(
                    lambda conn: conn.execute(
                        insert_sql,
                        _review_row(
                            mailbox=second,
                            expected_invocation_id=str(
                                first.current_invocation_id
                            ),
                            maintenance_id="cross-expected",
                            action="dead_letter",
                            outcome="dead_lettered",
                        ),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                await store._call(
                    lambda conn: conn.execute(
                        insert_sql,
                        _review_row(
                            mailbox=second,
                            expected_invocation_id=str(
                                second.current_invocation_id
                            ),
                            maintenance_id="cross-replacement",
                            action="retry",
                            outcome="retried",
                            replacement_invocation_id=str(
                                replacement.current_invocation_id
                            ),
                        ),
                    )
                )
            assert await store.list_mailbox_orphan_reviews() == []
            assert await store._call(
                lambda conn: conn.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
            ) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v30_migration_rejects_preexisting_cross_mailbox_review_atomically(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        first = await _orphan_mailbox(created, "migration-first")
        second = await _orphan_mailbox(created, "migration-second")
        await created.close()

        values = _review_row(
            mailbox=second,
            expected_invocation_id=str(first.current_invocation_id),
            maintenance_id="migration-cross-link",
            action="dead_letter",
            outcome="dead_lettered",
        )
        legacy_values = (*values[:2], *values[4:])
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            insert_trigger = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' "
                    "AND name='trg_mailbox_orphan_reviews_insert_valid'"
                ).fetchone()[0]
            )
            connection.execute(
                "DROP TRIGGER trg_mailbox_orphan_reviews_insert_valid"
            )
            connection.execute(
                "ALTER TABLE mailbox_orphan_reviews "
                "DROP COLUMN authorization_scheme"
            )
            connection.execute(
                "ALTER TABLE mailbox_orphan_reviews "
                "DROP COLUMN authorization_grant_digest"
            )
            connection.execute(
                """INSERT INTO mailbox_orphan_reviews (
                    mailbox_maintenance_id,payload_hash,mailbox_id,
                    mailbox_message_id,expected_current_invocation_id,
                    action,actor,reason,authorized_at,
                    authorization_source,administrator_authorized,outcome,
                    replacement_invocation_id,reviewed_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                legacy_values,
            )
            connection.execute(insert_trigger)
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="invocation identity conflicts"):
            await migrated.initialize()
        await migrated.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (29,)
            assert connection.execute(
                "SELECT COUNT(*) FROM mailbox_orphan_reviews "
                "WHERE mailbox_maintenance_id='migration-cross-link'"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' "
                "AND name='trg_mailbox_orphan_reviews_insert_valid'"
            ).fetchone() == (1,)

    asyncio.run(scenario())


def test_v29_review_history_migrates_losslessly_as_legacy_audit(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        mailbox = await _orphan_mailbox(created, "legacy-review")
        await created.close()
        expected_id = str(mailbox.current_invocation_id)
        v30_values = _review_row(
            mailbox=mailbox,
            expected_invocation_id=expected_id,
            maintenance_id="legacy-maintenance",
            action="dead_letter",
            outcome="dead_lettered",
        )
        legacy_values = (*v30_values[:2], *v30_values[4:])
        with sqlite3.connect(path) as connection:
            connection.execute(
                "ALTER TABLE mailbox_orphan_reviews "
                "DROP COLUMN authorization_scheme"
            )
            connection.execute(
                "ALTER TABLE mailbox_orphan_reviews "
                "DROP COLUMN authorization_grant_digest"
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35)"
            )
            connection.execute(
                """INSERT INTO mailbox_orphan_reviews (
                    mailbox_maintenance_id,payload_hash,mailbox_id,
                    mailbox_message_id,expected_current_invocation_id,
                    action,actor,reason,authorized_at,
                    authorization_source,administrator_authorized,outcome,
                    replacement_invocation_id,reviewed_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                legacy_values,
            )
            connection.execute(
                "UPDATE agent_mailbox SET state='dead_letter',processed_at=? "
                "WHERE mailbox_id=?",
                (
                    (BASE + timedelta(seconds=4)).isoformat(
                        timespec="microseconds"
                    ),
                    mailbox.mailbox_id,
                ),
            )
            before = {
                "review": tuple(
                    connection.execute(
                        "SELECT mailbox_maintenance_id,payload_hash,"
                        "mailbox_id,mailbox_message_id,"
                        "expected_current_invocation_id,action,actor,reason,"
                        "authorized_at,authorization_source,"
                        "administrator_authorized,outcome,"
                        "replacement_invocation_id,reviewed_at "
                        "FROM mailbox_orphan_reviews"
                    ).fetchone()
                ),
                "events": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM agent_invocation_events "
                        "WHERE source_kind='mailbox_orphan_review' "
                        "AND source_id='legacy-maintenance'"
                    ).fetchone()[0]
                ),
                "version": int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
            }
            connection.commit()
        assert before["version"] == 29 and before["events"] == 1

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            retained = await migrated.get_mailbox_orphan_review(
                "legacy-maintenance"
            )
            assert retained is not None
            assert retained.authorization_scheme == "legacy_v28_audit"
            assert retained.authorization_grant_digest == (
                canonical_mailbox_review_authorization_digest(
                    mailbox_message_id=retained.mailbox_message_id,
                    expected_current_invocation_id=(
                        retained.expected_current_invocation_id
                    ),
                    mailbox_maintenance_id=retained.mailbox_maintenance_id,
                    action=retained.action,
                    actor=retained.actor,
                    reason=retained.reason,
                    authorization_source=retained.authorization_source,
                    authorized_at=retained.authorized_at,
                )
            )
            after = await migrated._call(
                lambda conn: {
                    "review": tuple(
                        conn.execute(
                            "SELECT mailbox_maintenance_id,payload_hash,"
                            "mailbox_id,mailbox_message_id,"
                            "expected_current_invocation_id,action,actor,reason,"
                            "authorized_at,authorization_source,"
                            "administrator_authorized,outcome,"
                            "replacement_invocation_id,reviewed_at "
                            "FROM mailbox_orphan_reviews"
                        ).fetchone()
                    ),
                    "events": int(
                        conn.execute(
                            "SELECT COUNT(*) FROM agent_invocation_events "
                            "WHERE source_kind='mailbox_orphan_review' "
                            "AND source_id='legacy-maintenance'"
                        ).fetchone()[0]
                    ),
                    "version": int(
                        conn.execute(
                            "SELECT MAX(version) FROM schema_migrations"
                        ).fetchone()[0]
                    ),
                    "foreign_keys": conn.execute(
                        "PRAGMA foreign_key_check"
                    ).fetchall(),
                }
            )
            assert after["review"] == before["review"]
            assert after["events"] == 1
            assert after["version"] == 35
            assert after["foreign_keys"] == []
        finally:
            await migrated.close()

    asyncio.run(scenario())


def test_v30_replay_rejects_malformed_reviewed_at_without_losing_history(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        authority = _authority()
        created = SQLiteStore(
            path,
            mailbox_maintenance_authority=authority,
        )
        await created.initialize()
        mailbox = await _orphan_mailbox(created, "malformed-reviewed-at")
        expected_id = str(mailbox.current_invocation_id)
        grant = authority.issue_mailbox_orphan_review(
            mailbox.message_id,
            expected_id,
            "maintenance-malformed-time",
            "dead_letter",
            actor="administrator",
            reason="reviewed",
        )
        await created.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            "maintenance-malformed-time",
            "dead_letter",
            actor="administrator",
            reason="reviewed",
            maintenance_grant=grant,
            now=BASE + timedelta(seconds=4),
        )
        await created.close()

        with sqlite3.connect(path) as connection:
            trigger_sql = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' "
                    "AND name='trg_mailbox_orphan_reviews_no_update'"
                ).fetchone()[0]
            )
            connection.execute(
                "DROP TRIGGER trg_mailbox_orphan_reviews_no_update"
            )
            connection.execute(
                "UPDATE mailbox_orphan_reviews SET reviewed_at='not-a-time' "
                "WHERE mailbox_maintenance_id='maintenance-malformed-time'"
            )
            connection.execute(trigger_sql)
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="decision time conflicts"):
            await migrated.initialize()
        await migrated.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (29,)
            assert connection.execute(
                "SELECT reviewed_at FROM mailbox_orphan_reviews "
                "WHERE mailbox_maintenance_id='maintenance-malformed-time'"
            ).fetchone() == ("not-a-time",)
            assert connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' "
                "AND name='trg_mailbox_orphan_reviews_no_update'"
            ).fetchone() == (1,)

    asyncio.run(scenario())
