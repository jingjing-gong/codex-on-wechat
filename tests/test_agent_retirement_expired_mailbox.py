"""Agent retirement fences expired mailbox orphans without erasing evidence."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.maintenance_authority import MailboxMaintenanceAuthority
from src.runtime.models import MailboxOrphanReviewOutcome
from src.runtime.registry import codex_profile
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore, StoreError


BASE = datetime(2026, 8, 16, tzinfo=timezone.utc)
RETIRE_AT = BASE + timedelta(seconds=10)
REVIEW_AT = RETIRE_AT + timedelta(seconds=1)
WRITER_PROFILE = replace(
    codex_profile(profile_version=3, allow_dynamic_peers=True),
    agent_id="writer",
    display_name="Writer",
)


class _MutableClock:
    def __init__(self, current: datetime = BASE) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def _maintenance_store(path: Path, clock: _MutableClock) -> SQLiteStore:
    authority = MailboxMaintenanceAuthority(
        authorization_source="synthetic-test-policy",
        clock=clock,
    )
    return SQLiteStore(
        path,
        clock=clock,
        mailbox_maintenance_authority=authority,
    )


async def _orphan_mailbox(
    store: SQLiteStore,
    suffix: str,
    *,
    expires_at: datetime,
):
    await store.put_profile(WRITER_PROFILE)
    created = await store.create_agent_message(
        source_agent_id="planner",
        destination_agent_id="writer",
        content=f"synthetic work {suffix}",
        request_id=f"request-{suffix}",
        message_id=f"message-{suffix}",
        payload={"request_type": "ask"},
        expires_at=expires_at,
        now=BASE,
    )
    claimed_items = await store.claim_mailbox(
        "writer",
        f"worker-{suffix}",
        lease_seconds=100,
        now=BASE + timedelta(seconds=1),
    )
    assert len(claimed_items) == 1
    claimed = claimed_items[0]
    assert claimed.mailbox_id == created.mailbox_id
    assert claimed.claim_token
    assert await store.mark_mailbox_processing(
        claimed.mailbox_id,
        claimed.claim_token,
        now=BASE + timedelta(seconds=2),
    )
    assert await store.mark_mailbox_failed(
        claimed.mailbox_id,
        claimed.claim_token,
        error=f"synthetic orphan {suffix}",
        now=BASE + timedelta(seconds=3),
    )

    mailbox = await store.get_mailbox_item(created.mailbox_id)
    assert mailbox is not None
    assert mailbox.state.value == "orphaned_mailbox"
    assert mailbox.claim_token is None
    assert mailbox.claimed_by is None
    assert mailbox.lease_expires_at is None
    assert mailbox.current_invocation_id
    invocation = await store.get_agent_invocation(mailbox.current_invocation_id)
    assert invocation is not None
    assert invocation.state.value == "orphaned"
    assert invocation.admission_released_at == BASE + timedelta(seconds=3)
    assert invocation.terminal_at == BASE + timedelta(seconds=3)
    assert invocation.claim_token is None
    assert invocation.claimed_by is None
    assert invocation.lease_expires_at is None
    events = await store.list_agent_invocation_events(invocation.invocation_id)
    assert [event.new_state.value for event in events] == [
        "queued",
        "dispatching",
        "running",
        "orphaned",
    ]
    return mailbox


async def _wait_for(event: threading.Event) -> None:
    assert await asyncio.wait_for(asyncio.to_thread(event.wait), timeout=3)


def test_future_orphan_still_blocks_agent_retirement(tmp_path: Path) -> None:
    async def scenario() -> None:
        clock = _MutableClock()
        store = _maintenance_store(tmp_path / "future.sqlite", clock)
        await store.initialize()
        try:
            mailbox = await _orphan_mailbox(
                store,
                "future",
                expires_at=RETIRE_AT + timedelta(seconds=1),
            )
            invocation_id = str(mailbox.current_invocation_id)
            profile_before = await store.get_profile("writer", 3)
            invocation_before = await store.get_agent_invocation(invocation_id)
            events_before = await store.list_agent_invocation_events(invocation_id)
            lifecycle_before = await store.list_agent_lifecycle_events(
                agent_id="writer"
            )

            clock.current = RETIRE_AT
            with pytest.raises(
                InvalidTransition,
                match="unresolved mailbox work remains",
            ):
                await store.retire_agent("writer")

            assert await store.get_profile("writer", 3) == profile_before
            assert profile_before is not None and profile_before.enabled
            assert not await store.is_agent_deleted("writer")
            assert await store.get_mailbox_item(mailbox.mailbox_id) == mailbox
            assert await store.get_agent_invocation(invocation_id) == invocation_before
            assert (
                await store.list_agent_invocation_events(invocation_id)
                == events_before
            )
            assert (
                await store.list_agent_lifecycle_events(agent_id="writer")
                == lifecycle_before
            )
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("expiry_offset", "suffix"),
    [(timedelta(0), "exact"), (-timedelta(seconds=1), "past")],
)
def test_expired_orphan_permits_retirement_and_remains_reviewable(
    tmp_path: Path,
    expiry_offset: timedelta,
    suffix: str,
) -> None:
    async def scenario() -> None:
        clock = _MutableClock()
        store = _maintenance_store(tmp_path / f"{suffix}.sqlite", clock)
        await store.initialize()
        try:
            mailbox_before = await _orphan_mailbox(
                store,
                suffix,
                expires_at=RETIRE_AT + expiry_offset,
            )
            invocation_id = str(mailbox_before.current_invocation_id)
            invocation_before = await store.get_agent_invocation(invocation_id)
            events_before = await store.list_agent_invocation_events(invocation_id)

            clock.current = RETIRE_AT
            assert await store.retire_agent("writer") == 1

            profile = await store.get_profile("writer", 3)
            assert profile is not None and not profile.enabled
            assert await store.is_agent_deleted("writer")
            lifecycle = await store.get_agent_lifecycle("writer")
            assert lifecycle is not None
            assert lifecycle.lifecycle_state.value == "tombstoned"
            assert lifecycle.desired_process_state.value == "stopped"
            assert await store.get_mailbox_item(
                mailbox_before.mailbox_id
            ) == mailbox_before
            assert await store.get_agent_invocation(invocation_id) == invocation_before
            assert (
                await store.list_agent_invocation_events(invocation_id)
                == events_before
            )

            clock.current = REVIEW_AT
            authority = store._mailbox_maintenance_authority
            assert authority is not None
            maintenance_id = f"maintenance-{suffix}-dead-letter"
            actor = "synthetic-administrator"
            reason = "synthetic expired-orphan review"
            grant = authority.issue_mailbox_orphan_review(
                mailbox_before.message_id,
                invocation_id,
                maintenance_id,
                "dead_letter",
                actor=actor,
                reason=reason,
            )
            review = await store.review_mailbox_orphan(
                mailbox_before.message_id,
                invocation_id,
                maintenance_id,
                "dead_letter",
                actor=actor,
                reason=reason,
                maintenance_grant=grant,
                now=REVIEW_AT,
            )
            assert review.outcome is MailboxOrphanReviewOutcome.DEAD_LETTERED
            assert review.replacement_invocation_id is None
            assert review.authorization_source == "synthetic-test-policy"
            assert review.administrator_authorized
            assert await store.get_mailbox_orphan_review(maintenance_id) == review
            assert await store.list_mailbox_orphan_reviews(
                mailbox_message_id=mailbox_before.message_id
            ) == [review]

            reviewed_mailbox = await store.get_mailbox_item(
                mailbox_before.mailbox_id
            )
            assert reviewed_mailbox is not None
            assert reviewed_mailbox.mailbox_id == mailbox_before.mailbox_id
            assert reviewed_mailbox.current_invocation_id == invocation_id
            assert reviewed_mailbox.state.value == "dead_letter"
            assert await store.get_agent_invocation(invocation_id) == invocation_before
            events_after_review = await store.list_agent_invocation_events(
                invocation_id
            )
            assert events_after_review[:-1] == events_before
            assert events_after_review[-1].event_kind == "orphan_review_resolved"
            assert events_after_review[-1].previous_state.value == "orphaned"
            assert events_after_review[-1].new_state.value == "orphaned"
            assert events_after_review[-1].source_kind == "mailbox_orphan_review"
            assert events_after_review[-1].source_id == maintenance_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_retirement_winner_rejects_concurrent_new_mailbox_work(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        retirement_checked = threading.Event()
        release_retirement = threading.Event()
        admission_started = threading.Event()

        class PausingRetirementStore(SQLiteStore):
            @staticmethod
            def _assert_agent_retirable_tx(conn, agent_id, *, now_text):
                SQLiteStore._assert_agent_retirable_tx(
                    conn,
                    agent_id,
                    now_text=now_text,
                )
                retirement_checked.set()
                if not release_retirement.wait(timeout=4):
                    raise AssertionError("retirement test barrier timed out")

        clock = _MutableClock()
        path = tmp_path / "retirement-wins.sqlite"
        retiring = PausingRetirementStore(path, clock=clock)
        admitting = SQLiteStore(path, clock=clock)
        retirement_task = None
        admission_task = None
        await retiring.initialize()
        await admitting.initialize()
        try:
            expired = await _orphan_mailbox(
                retiring,
                "retirement-wins-expired",
                expires_at=RETIRE_AT - timedelta(seconds=1),
            )
            clock.current = RETIRE_AT
            retirement_task = asyncio.create_task(retiring.retire_agent("writer"))
            await _wait_for(retirement_checked)
            await admitting._call(
                lambda conn: conn.set_trace_callback(
                    lambda statement: (
                        admission_started.set()
                        if statement.strip().upper().startswith("BEGIN IMMEDIATE")
                        else None
                    )
                )
            )
            admission_task = asyncio.create_task(
                admitting.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="writer",
                    content="synthetic concurrent work after retirement started",
                    request_id="request-retirement-wins-new",
                    message_id="message-retirement-wins-new",
                    payload={"request_type": "ask"},
                    expires_at=RETIRE_AT + timedelta(minutes=1),
                    now=RETIRE_AT,
                )
            )
            await _wait_for(admission_started)
            await asyncio.sleep(0.02)
            assert not admission_task.done()

            release_retirement.set()
            assert await retirement_task == 1
            with pytest.raises(
                StoreError,
                match="Agent incarnation is not uniquely resolvable: writer",
            ):
                await admission_task

            retained_mailbox = await retiring.list_mailbox("writer")
            retained_invocations = await retiring.list_agent_invocations(
                agent_id="writer"
            )
            assert [item.mailbox_id for item in retained_mailbox] == [
                expired.mailbox_id
            ]
            assert [item.invocation_id for item in retained_invocations] == [
                expired.current_invocation_id
            ]
            assert await retiring.is_agent_deleted("writer")
        finally:
            release_retirement.set()
            for task in (retirement_task, admission_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (retirement_task, admission_task) if task),
                return_exceptions=True,
            )
            await asyncio.gather(retiring.close(), admitting.close())

    asyncio.run(scenario())


def test_concurrent_new_mailbox_winner_blocks_retirement(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        admission_created = threading.Event()
        release_admission = threading.Event()
        retirement_started = threading.Event()

        class PausingAdmissionStore(SQLiteStore):
            @classmethod
            def _create_queued_mailbox_invocation_tx(cls, conn, **kwargs):
                invocation_id = SQLiteStore._create_queued_mailbox_invocation_tx(
                    conn,
                    **kwargs,
                )
                admission_created.set()
                if not release_admission.wait(timeout=4):
                    raise AssertionError("admission test barrier timed out")
                return invocation_id

        clock = _MutableClock()
        path = tmp_path / "admission-wins.sqlite"
        admitting = PausingAdmissionStore(path, clock=clock)
        retiring = SQLiteStore(path, clock=clock)
        admission_task = None
        retirement_task = None
        await admitting.initialize()
        await retiring.initialize()
        try:
            expired = await _orphan_mailbox(
                retiring,
                "admission-wins-expired",
                expires_at=RETIRE_AT - timedelta(seconds=1),
            )
            clock.current = RETIRE_AT
            admission_task = asyncio.create_task(
                admitting.create_agent_message(
                    source_agent_id="planner",
                    destination_agent_id="writer",
                    content="synthetic work that wins the write lock",
                    request_id="request-admission-wins-new",
                    message_id="message-admission-wins-new",
                    payload={"request_type": "ask"},
                    expires_at=RETIRE_AT + timedelta(minutes=1),
                    now=RETIRE_AT,
                )
            )
            await _wait_for(admission_created)
            await retiring._call(
                lambda conn: conn.set_trace_callback(
                    lambda statement: (
                        retirement_started.set()
                        if statement.strip().upper().startswith("BEGIN IMMEDIATE")
                        else None
                    )
                )
            )
            retirement_task = asyncio.create_task(retiring.retire_agent("writer"))
            await _wait_for(retirement_started)
            await asyncio.sleep(0.02)
            assert not retirement_task.done()

            release_admission.set()
            admitted = await admission_task
            with pytest.raises(
                InvalidTransition,
                match="unfinished Agent work remains",
            ):
                await retirement_task

            profile = await retiring.get_profile("writer", 3)
            assert profile is not None and profile.enabled
            assert not await retiring.is_agent_deleted("writer")
            mailboxes = await retiring.list_mailbox("writer")
            assert {item.mailbox_id for item in mailboxes} == {
                expired.mailbox_id,
                admitted.mailbox_id,
            }
            invocation = await retiring.get_agent_invocation(
                str(admitted.current_invocation_id)
            )
            assert invocation is not None
            assert invocation.state.value == "queued"
            assert invocation.admission_released_at is None
        finally:
            release_admission.set()
            for task in (admission_task, retirement_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (admission_task, retirement_task) if task),
                return_exceptions=True,
            )
            await asyncio.gather(admitting.close(), retiring.close())

    asyncio.run(scenario())
