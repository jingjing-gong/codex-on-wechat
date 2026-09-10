"""Schema-v30/v31 invocation-history and timestamp-boundary regressions."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.maintenance_authority import MailboxMaintenanceAuthority
from src.runtime.media import StoredAttachment
from src.runtime.models import text_to_datetime
from src.runtime.registry import codex_profile
from src.runtime.sqlite_store import SQLiteStore, StoreError, _utc_text


BASE = datetime(2026, 8, 15, tzinfo=timezone.utc)


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


def _text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


def _offset_text(value: str) -> str:
    parsed = text_to_datetime(value)
    assert parsed is not None
    return parsed.astimezone(
        timezone(timedelta(hours=8))
    ).isoformat(timespec="seconds")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2030-01-01T08:00:00+08:00", "2030-01-01T00:00:00.000000+00:00"),
        ("2030-01-01T00:00:00Z", "2030-01-01T00:00:00.000000+00:00"),
        ("2030-01-01T00:00:00", "2030-01-01T00:00:00.000000+00:00"),
    ],
)
def test_utc_text_canonicalizes_every_accepted_string(value, expected):
    assert _utc_text(value) == expected


@pytest.mark.parametrize("value", ["", "   ", "not-a-time", 7])
def test_utc_text_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="ISO-8601"):
        _utc_text(value)  # type: ignore[arg-type]


def test_public_mailbox_api_canonicalizes_offset_now_and_rejects_invalid(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.put_profile(
                codex_profile(profile_version=3, allow_dynamic_peers=True),
            )
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="canonical timestamp",
                request_id="timestamp-valid",
                message_id="timestamp-valid-message",
                now="2026-08-15T08:00:00+08:00",
            )
            retained = await store._call(
                lambda conn: tuple(
                    conn.execute(
                        """SELECT mailbox.created_at,invocation.created_at,
                                  invocation.updated_at,event.created_at
                             FROM agent_mailbox AS mailbox
                             JOIN agent_invocations AS invocation
                               ON invocation.invocation_id=
                                  mailbox.current_invocation_id
                             JOIN agent_invocation_events AS event
                               ON event.invocation_id=invocation.invocation_id
                              AND event.event_sequence=1
                            WHERE mailbox.mailbox_id=?""",
                        (mailbox.mailbox_id,),
                    ).fetchone()
                )
            )
            canonical = "2026-08-15T00:00:00.000000+00:00"
            assert retained == (canonical, canonical, canonical, canonical)

            for suffix, invalid in enumerate(("", "   ", "not-a-time"), 1):
                with pytest.raises(ValueError, match="ISO-8601"):
                    await store.create_agent_message(
                        source_agent_id="planner",
                        destination_agent_id="codex",
                        content="invalid timestamp",
                        request_id=f"timestamp-invalid-{suffix}",
                        message_id=f"timestamp-invalid-message-{suffix}",
                        now=invalid,
                    )
            assert await store._call(
                lambda conn: int(
                    conn.execute(
                        "SELECT COUNT(*) FROM agent_mailbox "
                        "WHERE request_id LIKE 'timestamp-invalid-%'"
                    ).fetchone()[0]
                )
            ) == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_attachment_empty_created_at_remains_an_omission_on_replay(tmp_path):
    async def scenario() -> None:
        current = [BASE]
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            clock=lambda: current[0],
        )
        await store.initialize()
        try:
            attachment = StoredAttachment(
                attachment_id="attachment-empty-created-at",
                path="",
                mime_type="application/octet-stream",
                size=0,
                checksum="",
            )
            first = await store.register_attachment(attachment)
            current[0] = BASE + timedelta(days=1)
            replay = await store.register_attachment(attachment)
            assert first.created_at == replay.created_at == _text(BASE)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_task_event_adapter_rejects_malformed_required_timestamp(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task("bad-event-row-time"))
            claim = await store.claim_next_task("timestamp-test-worker")
            assert claim is not None
            assert await store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            event = await store.append_task_event(
                task.task_id,
                {
                    "event_id": "bad-event-row-time",
                    "event_type": "diagnostic",
                    "visibility": "internal",
                    "content": "retained",
                },
                claim_token=claim.claim_token,
            )
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE task_events SET created_at='not-a-time' "
                    "WHERE event_id=?",
                    (event.event_id,),
                )
            )
            with pytest.raises(
                StoreError,
                match="task event creation timestamp is invalid",
            ):
                await store.list_task_events(task.task_id)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_outbox_adapter_rejects_malformed_required_timestamp(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            outbox = await store.create_user_outbox(
                target={
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                },
                content="retained",
                agent_id="codex",
                outbox_id="bad-outbox-row-time",
                now=BASE,
            )
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE user_outbox SET created_at='not-a-time' "
                    "WHERE outbox_id=?",
                    (outbox.outbox_id,),
                )
            )
            with pytest.raises(
                StoreError,
                match="user outbox creation timestamp is invalid",
            ):
                await store.get_outbox_item(outbox.outbox_id)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_inbound_adapter_rejects_malformed_required_timestamp(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            inbound = await store.store_inbound(
                {
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "external_message_id": "bad-inbound-row-time",
                    "text": "retained",
                    "received_at": BASE,
                }
            )
            await store._call(
                lambda conn: conn.execute(
                    "UPDATE inbound_messages SET received_at='not-a-time' "
                    "WHERE message_id=?",
                    (inbound.message_id,),
                )
            )
            with pytest.raises(
                StoreError,
                match="inbound receipt timestamp is invalid",
            ):
                await store.get_inbound(inbound.message_id)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_pre_v28_offset_invocation_history_migrates_to_canonical_utc(
    tmp_path,
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        created = SQLiteStore(path)
        await created.initialize()
        task = await created.create_task(_task("pre-v28-offset"))
        await created.close()
        invocation_id = str(task.execution_id)

        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP TRIGGER trg_agent_invocations_event_after_insert"
            )
            connection.execute(
                "DROP TRIGGER trg_agent_invocations_event_after_state_update"
            )
            connection.execute("DROP TABLE mailbox_orphan_reviews")
            connection.execute("DROP TABLE agent_invocation_events")
            connection.execute(
                """UPDATE agent_invocations
                      SET created_at='2026-08-15T08:00:00+08:00',
                          updated_at='2026-08-15T08:00:03+08:00'
                    WHERE invocation_id=?""",
                (invocation_id,),
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (28,29,30,31,32,33,34,35,36,37,38,39,40,41)"
            )
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            retained = await migrated._call(
                lambda conn: tuple(
                    conn.execute(
                        """SELECT invocation.created_at,invocation.updated_at,
                                  event.created_at,
                                  (SELECT MAX(version) FROM schema_migrations)
                             FROM agent_invocations AS invocation
                             JOIN agent_invocation_events AS event
                               ON event.invocation_id=invocation.invocation_id
                              AND event.event_sequence=1
                            WHERE invocation.invocation_id=?""",
                        (invocation_id,),
                    ).fetchone()
                )
            )
            assert retained == (
                "2026-08-15T00:00:00.000000+00:00",
                "2026-08-15T00:00:03.000000+00:00",
                "2026-08-15T00:00:03.000000+00:00",
                41,
            )
        finally:
            await migrated.close()

    asyncio.run(scenario())


def _prepare_v29_ordering_seed(path, corruption: str) -> str:
    async def seed() -> str:
        created = SQLiteStore(path)
        await created.initialize()
        task = await created.create_task(_task("ordering-" + corruption))
        await created.close()
        return str(task.execution_id)

    invocation_id = asyncio.run(seed())
    event1 = _text(BASE + timedelta(seconds=1))
    event2 = _text(BASE + timedelta(seconds=2))
    updated = _text(BASE + timedelta(seconds=3))
    with sqlite3.connect(path) as connection:
        trigger_sql = str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' "
                "AND name='trg_agent_invocation_events_no_update'"
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_update")
        connection.execute(
            "UPDATE agent_invocations SET created_at=?,updated_at=? "
            "WHERE invocation_id=?",
            (_text(BASE), updated, invocation_id),
        )
        connection.execute(
            """UPDATE agent_invocation_events
                  SET invocation_event_id=?,event_kind='migration_snapshot',
                      previous_state=NULL,new_state='queued',
                      source_kind='migration',source_id='schema-v28',
                      metadata_json='{"schema_version":28}',created_at=?
                WHERE invocation_id=? AND event_sequence=1""",
            (
                "invocation-event:migration-v28:" + invocation_id,
                event1,
                invocation_id,
            ),
        )
        connection.execute(
            """INSERT INTO agent_invocation_events (
                   invocation_event_id,invocation_id,event_sequence,event_kind,
                   previous_state,new_state,source_kind,source_id,
                   metadata_json,created_at)
               VALUES (?, ?, 2, 'state_transition', 'queued', 'dispatching',
                       'store', ?, '{}', ?)""",
            (
                "invocation-event:" + invocation_id + ":00000000000000000002",
                invocation_id,
                invocation_id,
                event2,
            ),
        )
        if corruption == "created_after_event1":
            connection.execute(
                "UPDATE agent_invocations SET created_at=? "
                "WHERE invocation_id=?",
                (_text(BASE + timedelta(seconds=1, microseconds=1)), invocation_id),
            )
        elif corruption == "event1_after_event2":
            connection.execute(
                "UPDATE agent_invocation_events SET created_at=? "
                "WHERE invocation_id=? AND event_sequence=1",
                (_text(BASE + timedelta(seconds=2, microseconds=1)), invocation_id),
            )
        elif corruption == "event2_after_updated":
            connection.execute(
                "UPDATE agent_invocation_events SET created_at=? "
                "WHERE invocation_id=? AND event_sequence=2",
                (_text(BASE + timedelta(seconds=3, microseconds=1)), invocation_id),
            )
        elif corruption == "created_after_updated":
            connection.execute(
                "UPDATE agent_invocations SET created_at=? "
                "WHERE invocation_id=?",
                (_text(BASE + timedelta(seconds=4)), invocation_id),
            )
        else:  # pragma: no cover - test parameter is closed.
            raise AssertionError(corruption)
        connection.execute(trigger_sql)
        connection.execute(
            "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35,36,37,38,39,40,41)"
        )
        connection.commit()
    return invocation_id


@pytest.mark.parametrize(
    "corruption",
    [
        "created_after_event1",
        "event1_after_event2",
        "event2_after_updated",
        "created_after_updated",
    ],
)
def test_v30_rejects_invocation_event_timestamp_ordering_atomically(
    tmp_path,
    corruption,
):
    path = tmp_path / f"{corruption}.sqlite"
    invocation_id = _prepare_v29_ordering_seed(path, corruption)

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="timestamp|sequence-one"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE invocation_id=?",
            (invocation_id,),
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' "
            "AND name='trg_agent_invocation_events_no_update'"
        ).fetchone() == (1,)


async def _orphan_mailbox(store: SQLiteStore, suffix: str):
    await store.put_profile(
        codex_profile(profile_version=3, allow_dynamic_peers=True),
    )
    mailbox = await store.create_agent_message(
        source_agent_id="planner",
        destination_agent_id="codex",
        content="review " + suffix,
        request_id="request-" + suffix,
        message_id="message-" + suffix,
        payload={"request_type": "ask"},
        now=BASE,
    )
    claimed = (
        await store.claim_mailbox(
            "codex",
            "worker-" + suffix,
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


def _prepare_review(
    path,
    suffix: str,
    *,
    legacy_shape: bool = True,
    maintenance_id_from_invocation: bool = False,
):
    async def seed():
        authority = MailboxMaintenanceAuthority(
            authorization_source="test-supervisor",
            clock=lambda: BASE,
        )
        store = SQLiteStore(path, mailbox_maintenance_authority=authority)
        await store.initialize()
        mailbox = await _orphan_mailbox(store, suffix)
        expected_id = str(mailbox.current_invocation_id)
        maintenance_id = (
            expected_id
            if maintenance_id_from_invocation
            else "maintenance-" + suffix
        )
        grant = authority.issue_mailbox_orphan_review(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "dead_letter",
            actor="administrator",
            reason="reviewed",
        )
        review = await store.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "dead_letter",
            actor="administrator",
            reason="reviewed",
            maintenance_grant=grant,
            now=BASE + timedelta(seconds=4),
        )
        await store.close()
        return expected_id, maintenance_id, review.reviewed_at

    expected_id, maintenance_id, reviewed_at = asyncio.run(seed())
    if legacy_shape:
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
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35,36,37,38,39,40,41)"
            )
            connection.commit()
    return expected_id, maintenance_id, reviewed_at


def _prepare_rejected_review(path, suffix: str, *, legacy_shape: bool = True):
    async def seed():
        authority = MailboxMaintenanceAuthority(
            authorization_source="test-supervisor",
            clock=lambda: BASE,
        )
        store = SQLiteStore(
            path,
            mailbox_ttl_seconds=2,
            mailbox_maintenance_authority=authority,
        )
        await store.initialize()
        mailbox = await _orphan_mailbox(store, suffix)
        expected_id = str(mailbox.current_invocation_id)
        maintenance_id = "maintenance-" + suffix
        grant = authority.issue_mailbox_orphan_review(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "retry",
            actor="administrator",
            reason="reviewed",
        )
        review = await store.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "retry",
            actor="administrator",
            reason="reviewed",
            maintenance_grant=grant,
            now=BASE + timedelta(seconds=4),
        )
        assert review.outcome.value == "rejected_expired"
        await store.close()
        return expected_id, maintenance_id, review.reviewed_at

    expected_id, maintenance_id, reviewed_at = asyncio.run(seed())
    if legacy_shape:
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
                "DELETE FROM schema_migrations WHERE version IN (30,31,32,33,34,35,36,37,38,39,40,41)"
            )
            connection.commit()
    return expected_id, maintenance_id, reviewed_at


def _event_trigger_sql(connection: sqlite3.Connection, name: str) -> str:
    return str(
        connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (name,),
        ).fetchone()[0]
    )


def test_v30_rejects_missing_accepted_review_event_atomically(tmp_path):
    path = tmp_path / "missing.sqlite"
    _expected_id, maintenance_id, _reviewed_at = _prepare_review(
        path, "missing"
    )
    with sqlite3.connect(path) as connection:
        trigger_sql = _event_trigger_sql(
            connection, "trg_agent_invocation_events_no_delete"
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_delete")
        connection.execute(
            "DELETE FROM agent_invocation_events "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            (maintenance_id,),
        )
        connection.execute(trigger_sql)
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="review event history conflicts"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            (maintenance_id,),
        ).fetchone() == (0,)


def test_v30_rejects_duplicate_accepted_review_event_atomically(tmp_path):
    path = tmp_path / "duplicate.sqlite"
    expected_id, maintenance_id, reviewed_at = _prepare_review(path, "duplicate")
    with sqlite3.connect(path) as connection:
        sequence = int(
            connection.execute(
                "SELECT MAX(event_sequence) FROM agent_invocation_events "
                "WHERE invocation_id=?",
                (expected_id,),
            ).fetchone()[0]
        ) + 1
        connection.execute(
            """INSERT INTO agent_invocation_events (
                   invocation_event_id,invocation_id,event_sequence,event_kind,
                   previous_state,new_state,source_kind,source_id,
                   metadata_json,created_at)
               VALUES (?, ?, ?, 'orphan_review_resolved', 'orphaned',
                       'orphaned', 'mailbox_orphan_review', ?, ?, ?)""",
            (
                "duplicate-review-event:" + maintenance_id,
                expected_id,
                sequence,
                maintenance_id,
                json.dumps(
                    {
                        "action": "dead_letter",
                        "outcome": "dead_lettered",
                        "replacement_invocation_id": None,
                    }
                ),
                _utc_text(reviewed_at),
            ),
        )
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="review event history conflicts"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            (maintenance_id,),
        ).fetchone() == (2,)


@pytest.mark.parametrize("corruption", ["event-id", "sequence-99"])
def test_v30_rejects_noncanonical_review_event_identity_and_sequence(
    tmp_path,
    corruption,
):
    path = tmp_path / f"review-{corruption}.sqlite"
    expected_id, maintenance_id, _reviewed_at = _prepare_review(
        path,
        "review-" + corruption,
    )
    with sqlite3.connect(path) as connection:
        trigger_sql = _event_trigger_sql(
            connection, "trg_agent_invocation_events_no_update"
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_update")
        if corruption == "event-id":
            connection.execute(
                "UPDATE agent_invocation_events "
                "SET invocation_event_id='forged-review-event-id' "
                "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
                (maintenance_id,),
            )
        else:
            connection.execute(
                "UPDATE agent_invocation_events "
                "SET event_sequence=99,invocation_event_id=? "
                "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
                (
                    "invocation-event:"
                    + expected_id
                    + ":00000000000000000099",
                    maintenance_id,
                ),
            )
        connection.execute(trigger_sql)
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="review event history conflicts"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        retained = connection.execute(
            "SELECT invocation_event_id,event_sequence "
            "FROM agent_invocation_events "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            (maintenance_id,),
        ).fetchone()
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        if corruption == "event-id":
            assert retained[0] == "forged-review-event-id"
        else:
            assert retained[1] == 99


def test_v30_rejects_fully_forged_resolution_event_for_rejected_review(
    tmp_path,
):
    path = tmp_path / "rejected-resolution.sqlite"
    expected_id, maintenance_id, _reviewed_at = _prepare_rejected_review(
        path,
        "rejected-resolution",
    )
    with sqlite3.connect(path) as connection:
        sequence = int(
            connection.execute(
                "SELECT MAX(event_sequence) FROM agent_invocation_events "
                "WHERE invocation_id=?",
                (expected_id,),
            ).fetchone()[0]
        ) + 1
        connection.execute(
            """INSERT INTO agent_invocation_events (
                   invocation_event_id,invocation_id,event_sequence,event_kind,
                   previous_state,new_state,source_kind,source_id,
                   metadata_json,created_at)
               VALUES (?, ?, ?, 'orphan_review_resolved', 'orphaned',
                       'orphaned', 'forged-review-source',
                       'forged-review-source-id', ?, ?)""",
            (
                "invocation-event:"
                + expected_id
                + ":"
                + f"{sequence:020d}",
                expected_id,
                sequence,
                '{"forged":true}',
                _text(BASE + timedelta(seconds=5)),
            ),
        )
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(
            StoreError,
            match="resolution history",
        ):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE invocation_id=? AND event_kind='orphan_review_resolved'",
            (expected_id,),
        ).fetchone() == (1,)


def test_v30_rejects_review_authorized_after_decision_atomically(tmp_path):
    path = tmp_path / "future-authorization.sqlite"
    _expected_id, maintenance_id, _reviewed_at = _prepare_review(
        path,
        "future-authorization",
    )
    future_authorization = _text(BASE + timedelta(seconds=5))
    with sqlite3.connect(path) as connection:
        trigger_sql = _event_trigger_sql(
            connection, "trg_mailbox_orphan_reviews_no_update"
        )
        connection.execute("DROP TRIGGER trg_mailbox_orphan_reviews_no_update")
        connection.execute(
            "UPDATE mailbox_orphan_reviews SET authorized_at=? "
            "WHERE mailbox_maintenance_id=?",
            (future_authorization, maintenance_id),
        )
        connection.execute(trigger_sql)
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="authorization follows its decision"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT authorized_at FROM mailbox_orphan_reviews "
            "WHERE mailbox_maintenance_id=?",
            (maintenance_id,),
        ).fetchone() == (future_authorization,)
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' "
            "AND name='trg_mailbox_orphan_reviews_no_update'"
        ).fetchone() == (1,)


def test_v30_rejects_review_before_orphan_terminalization_atomically(tmp_path):
    path = tmp_path / "review-before-orphan.sqlite"
    _expected_id, maintenance_id, _reviewed_at = _prepare_review(
        path,
        "review-before-orphan",
    )
    premature_review = _text(BASE + timedelta(seconds=2))
    with sqlite3.connect(path) as connection:
        event_trigger = _event_trigger_sql(
            connection, "trg_agent_invocation_events_no_update"
        )
        review_trigger = _event_trigger_sql(
            connection, "trg_mailbox_orphan_reviews_no_update"
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_update")
        connection.execute("DROP TRIGGER trg_mailbox_orphan_reviews_no_update")
        connection.execute(
            "UPDATE mailbox_orphan_reviews SET reviewed_at=? "
            "WHERE mailbox_maintenance_id=?",
            (premature_review, maintenance_id),
        )
        connection.execute(
            "UPDATE agent_invocation_events SET created_at=? "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            (premature_review, maintenance_id),
        )
        connection.execute(event_trigger)
        connection.execute(review_trigger)
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="predates orphan terminalization"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT reviewed_at FROM mailbox_orphan_reviews "
            "WHERE mailbox_maintenance_id=?",
            (maintenance_id,),
        ).fetchone() == (premature_review,)
        triggers = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND name IN ('trg_agent_invocation_events_no_update',"
                "'trg_mailbox_orphan_reviews_no_update')"
            ).fetchall()
        }
        assert triggers == {
            "trg_agent_invocation_events_no_update",
            "trg_mailbox_orphan_reviews_no_update",
        }


def test_review_api_rejects_decision_before_orphan_terminalization(tmp_path):
    async def scenario() -> None:
        authority = MailboxMaintenanceAuthority(
            authorization_source="test-supervisor",
            clock=lambda: BASE,
        )
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            mailbox_maintenance_authority=authority,
        )
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(store, "api-before-orphan")
            expected_id = str(mailbox.current_invocation_id)
            maintenance_id = "maintenance-api-before-orphan"
            grant = authority.issue_mailbox_orphan_review(
                mailbox.message_id,
                expected_id,
                maintenance_id,
                "dead_letter",
                actor="administrator",
                reason="reviewed",
            )
            with pytest.raises(
                StoreError,
                match="predates orphan terminalization",
            ):
                await store.review_mailbox_orphan(
                    mailbox.message_id,
                    expected_id,
                    maintenance_id,
                    "dead_letter",
                    actor="administrator",
                    reason="reviewed",
                    maintenance_grant=grant,
                    now=BASE + timedelta(seconds=2),
                )
            assert await store.list_mailbox_orphan_reviews() == []
            assert await store._call(
                lambda conn: int(
                    conn.execute(
                        "SELECT COUNT(*) FROM agent_invocation_events "
                        "WHERE event_kind='orphan_review_resolved'"
                    ).fetchone()[0]
                )
            ) == 0
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("name", "assignment", "value"),
    [
        ("event-kind", "event_kind='state_transition',new_state='failed'", None),
        ("source-kind", "source_kind='corrupt-review-source'", None),
        ("source-id", "source_id='corrupt-review-id'", None),
        (
            "metadata",
            "metadata_json=?",
            '{"action":"retry","outcome":"dead_lettered",'
            '"replacement_invocation_id":null}',
        ),
        (
            "duplicate-metadata-key",
            "metadata_json=?",
            '{"action":"retry","action":"dead_letter",'
            '"outcome":"dead_lettered","replacement_invocation_id":null}',
        ),
        (
            "review-time",
            "created_at=?",
            _text(BASE + timedelta(seconds=5)),
        ),
    ],
)
def test_v30_rejects_mismatched_accepted_review_event_atomically(
    tmp_path,
    name,
    assignment,
    value,
):
    path = tmp_path / f"mismatch-{name}.sqlite"
    _expected_id, maintenance_id, _reviewed_at = _prepare_review(path, name)
    with sqlite3.connect(path) as connection:
        trigger_sql = _event_trigger_sql(
            connection, "trg_agent_invocation_events_no_update"
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_update")
        params = (() if value is None else (value,)) + (maintenance_id,)
        connection.execute(
            "UPDATE agent_invocation_events SET "
            + assignment
            + " WHERE source_kind='mailbox_orphan_review' AND source_id=?",
            params,
        )
        connection.execute(trigger_sql)
        connection.commit()

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        with pytest.raises(StoreError, match="review event history conflicts"):
            await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (29,)
        assert connection.execute(
            "SELECT COUNT(*) FROM mailbox_orphan_reviews "
            "WHERE mailbox_maintenance_id=?",
            (maintenance_id,),
        ).fetchone() == (1,)


def test_v29_review_id_may_equal_invocation_id_through_v31(tmp_path):
    path = tmp_path / "review-invocation-id-collision.sqlite"
    expected_id, maintenance_id, _reviewed_at = _prepare_review(
        path,
        "review-invocation-id-collision",
        maintenance_id_from_invocation=True,
    )
    assert maintenance_id == expected_id

    async def scenario() -> None:
        migrated = SQLiteStore(path)
        await migrated.initialize()
        await migrated.close()

    asyncio.run(scenario())
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone() == (41,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE source_kind='mailbox_orphan_review' AND source_id=? "
            "AND event_kind='orphan_review_resolved'",
            (maintenance_id,),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_invocation_events "
            "WHERE source_kind='store' AND source_id=?",
            (expected_id,),
        ).fetchone()[0] >= 1
        assert connection.execute(
            "SELECT outcome FROM mailbox_orphan_reviews "
            "WHERE mailbox_maintenance_id=?",
            (maintenance_id,),
        ).fetchone() == ("dead_lettered",)


def test_v30_to_v31_canonicalizes_review_and_event_together(tmp_path):
    path = tmp_path / "v30-to-v31.sqlite"
    expected_id, maintenance_id, reviewed_at = _prepare_review(
        path,
        "v30-to-v31",
        legacy_shape=False,
    )
    with sqlite3.connect(path) as connection:
        original_invocation = tuple(
            connection.execute(
                "SELECT created_at,updated_at,terminal_at "
                "FROM agent_invocations "
                "WHERE invocation_id=?",
                (expected_id,),
            ).fetchone()
        )
        original_events = {
            int(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT event_sequence,created_at FROM agent_invocation_events "
                "WHERE invocation_id=?",
                (expected_id,),
            ).fetchall()
        }
        event_trigger = _event_trigger_sql(
            connection, "trg_agent_invocation_events_no_update"
        )
        review_trigger = _event_trigger_sql(
            connection, "trg_mailbox_orphan_reviews_no_update"
        )
        connection.execute("DROP TRIGGER trg_agent_invocation_events_no_update")
        connection.execute("DROP TRIGGER trg_mailbox_orphan_reviews_no_update")
        connection.execute(
            "UPDATE agent_invocations "
            "SET created_at=?,updated_at=?,terminal_at=? "
            "WHERE invocation_id=?",
            (
                _offset_text(str(original_invocation[0])),
                _offset_text(str(original_invocation[1])),
                _offset_text(str(original_invocation[2])),
                expected_id,
            ),
        )
        for sequence, created_at in original_events.items():
            connection.execute(
                "UPDATE agent_invocation_events SET created_at=? "
                "WHERE invocation_id=? AND event_sequence=?",
                (_offset_text(created_at), expected_id, sequence),
            )
        connection.execute(
            "UPDATE mailbox_orphan_reviews SET reviewed_at=? "
            "WHERE mailbox_maintenance_id=?",
            (_offset_text(_utc_text(reviewed_at)), maintenance_id),
        )
        connection.execute(event_trigger)
        connection.execute(review_trigger)
        connection.execute(
            "DELETE FROM schema_migrations WHERE version IN (31,32,33,34,35,36,37,38,39,40,41)"
        )
        connection.commit()

    migrated = SQLiteStore(path)

    async def scenario() -> tuple[object, ...]:
        await migrated.initialize()
        try:
            return await migrated._call(
                lambda conn: tuple(
                    conn.execute(
                        """SELECT invocation.created_at,invocation.updated_at,
                                  invocation.terminal_at,review.reviewed_at,
                                  event.created_at,
                                  (SELECT MAX(version) FROM schema_migrations)
                             FROM agent_invocations AS invocation
                             JOIN mailbox_orphan_reviews AS review
                               ON review.expected_current_invocation_id=
                                  invocation.invocation_id
                             JOIN agent_invocation_events AS event
                               ON event.invocation_id=invocation.invocation_id
                              AND event.source_kind='mailbox_orphan_review'
                              AND event.source_id=review.mailbox_maintenance_id
                            WHERE invocation.invocation_id=?""",
                        (expected_id,),
                    ).fetchone()
                )
            )
        finally:
            await migrated.close()

    retained = asyncio.run(scenario())
    assert retained[0] == _utc_text(str(original_invocation[0]))
    assert retained[1] == _utc_text(str(original_invocation[1]))
    assert retained[2] == _utc_text(str(original_invocation[2]))
    assert retained[3] == retained[4] == _utc_text(reviewed_at)
    assert text_to_datetime(str(retained[3])) == text_to_datetime(
        _offset_text(_utc_text(reviewed_at))
    )
    assert retained[5] == 41


def test_v30_to_v31_canonicalizes_rejected_review_timestamp(tmp_path):
    async def seed(path):
        authority = MailboxMaintenanceAuthority(
            authorization_source="test-supervisor",
            clock=lambda: BASE,
        )
        store = SQLiteStore(
            path,
            mailbox_ttl_seconds=2,
            mailbox_maintenance_authority=authority,
        )
        await store.initialize()
        mailbox = await _orphan_mailbox(store, "v30-rejected-review")
        expected_id = str(mailbox.current_invocation_id)
        maintenance_id = "maintenance-v30-rejected-review"
        grant = authority.issue_mailbox_orphan_review(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "retry",
            actor="administrator",
            reason="reviewed",
        )
        review = await store.review_mailbox_orphan(
            mailbox.message_id,
            expected_id,
            maintenance_id,
            "retry",
            actor="administrator",
            reason="reviewed",
            maintenance_grant=grant,
            now=BASE + timedelta(seconds=4),
        )
        assert review.outcome.value == "rejected_expired"
        await store.close()
        return maintenance_id, review.reviewed_at

    path = tmp_path / "v30-rejected-review.sqlite"
    maintenance_id, reviewed_at = asyncio.run(seed(path))
    with sqlite3.connect(path) as connection:
        trigger_sql = _event_trigger_sql(
            connection, "trg_mailbox_orphan_reviews_no_update"
        )
        connection.execute("DROP TRIGGER trg_mailbox_orphan_reviews_no_update")
        connection.execute(
            "UPDATE mailbox_orphan_reviews SET reviewed_at=? "
            "WHERE mailbox_maintenance_id=?",
            (_offset_text(_utc_text(reviewed_at)), maintenance_id),
        )
        connection.execute(trigger_sql)
        connection.execute(
            "DELETE FROM schema_migrations WHERE version IN (31,32,33,34,35,36,37,38,39,40,41)"
        )
        connection.commit()

    async def migrate() -> tuple[str, int, int]:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            return await store._call(
                lambda conn: (
                    str(
                        conn.execute(
                            "SELECT reviewed_at FROM mailbox_orphan_reviews "
                            "WHERE mailbox_maintenance_id=?",
                            (maintenance_id,),
                        ).fetchone()[0]
                    ),
                    int(
                        conn.execute(
                            "SELECT COUNT(*) FROM agent_invocation_events "
                            "WHERE source_kind='mailbox_orphan_review' "
                            "AND source_id=?",
                            (maintenance_id,),
                        ).fetchone()[0]
                    ),
                    int(
                        conn.execute(
                            "SELECT MAX(version) FROM schema_migrations"
                        ).fetchone()[0]
                    ),
                )
            )
        finally:
            await store.close()

    retained_at, event_count, version = asyncio.run(migrate())
    assert retained_at == _utc_text(reviewed_at)
    assert text_to_datetime(retained_at) == text_to_datetime(
        _offset_text(_utc_text(reviewed_at))
    )
    assert event_count == 0
    assert version == 41
