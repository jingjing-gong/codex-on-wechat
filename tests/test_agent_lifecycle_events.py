"""Schema-v25 Agent lifecycle projection and event-ledger regressions."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.runtime.models import (
    AgentDesiredProcessState,
    AgentLifecycleState,
)
from src.runtime.policy import AgentProfile
from src.runtime.sqlite_store import InvalidTransition, SQLiteStore


T0 = datetime(2026, 8, 15, 2, 0, tzinfo=timezone.utc)


def _profile(
    agent_id: str,
    profile_version: int,
    *,
    enabled: bool = True,
) -> AgentProfile:
    """Return distinct, replayable immutable metadata for one Profile."""

    return AgentProfile(
        agent_id=agent_id,
        profile_version=profile_version,
        display_name=f"{agent_id} v{profile_version}",
        summary=f"Profile version {profile_version}",
        system_prompt=f"You are {agent_id} version {profile_version}.",
        capabilities=frozenset({"read", f"profile-v{profile_version}"}),
        enabled=enabled,
    )


def test_profile_publication_is_monotonic_idempotent_and_restart_safe(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        profiles = [_profile("monotonic", version) for version in (1, 2, 3)]

        first = SQLiteStore(path)
        await first.initialize()
        try:
            for profile in profiles:
                assert await first.put_profile(profile)

            # Concurrent/reordered startup publication of immutable history
            # must neither duplicate events nor move the projection backward.
            await asyncio.gather(
                first.put_profile(profiles[2]),
                first.put_profile(profiles[0]),
                first.put_profile(profiles[1]),
                first.put_profile(profiles[2]),
            )

            lifecycle = await first.get_agent_lifecycle("monotonic")
            assert lifecycle is not None
            assert lifecycle.agent_incarnation == 1
            assert lifecycle.profile_version == 3
            assert lifecycle.lifecycle_state is AgentLifecycleState.ENABLED
            assert (
                lifecycle.desired_process_state
                is AgentDesiredProcessState.RUNNING
            )

            events = await first.list_agent_lifecycle_events(
                agent_id="monotonic"
            )
            assert [event.event_kind for event in events] == [
                "profile_published",
                "profile_published",
                "profile_published",
            ]
            assert [event.event_sequence for event in events] == [1, 2, 3]
            assert [event.new_profile_version for event in events] == [1, 2, 3]
            assert [event.previous_profile_version for event in events] == [
                None,
                1,
                2,
            ]
            assert all(
                event.actor_kind == "compatibility_unattributed"
                and event.actor_id is None
                for event in events
            )
            event_ids = [event.lifecycle_event_id for event in events]
        finally:
            await first.close()

        restarted = SQLiteStore(path)
        await restarted.initialize()
        try:
            # Registry startup may publish retained versions in any order.
            for profile in reversed(profiles):
                assert await restarted.put_profile(profile)
            for profile in profiles:
                assert await restarted.put_profile(profile)

            lifecycle = await restarted.get_agent_lifecycle("monotonic")
            assert lifecycle is not None and lifecycle.profile_version == 3
            replayed = await restarted.list_agent_lifecycle_events(
                agent_id="monotonic"
            )
            assert [event.lifecycle_event_id for event in replayed] == event_ids
            assert [event.event_sequence for event in replayed] == [1, 2, 3]
        finally:
            await restarted.close()

    asyncio.run(scenario())


def test_post_v25_creation_disabled_publication_and_delete_marker_wins(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            assert await store.put_profile(_profile("post-v25", 1))
            created = await store.get_agent_lifecycle("post-v25")
            assert created is not None
            assert created.agent_incarnation == 1
            assert created.lifecycle_state is AgentLifecycleState.ENABLED

            assert await store.put_profile(
                _profile("post-v25", 2, enabled=False)
            )
            disabled = await store.get_agent_lifecycle("post-v25")
            assert disabled is not None
            assert disabled.profile_version == 2
            assert disabled.lifecycle_state is AgentLifecycleState.DISABLED
            assert (
                disabled.desired_process_state
                is AgentDesiredProcessState.STOPPED
            )

            assert await store.mark_agent_deleted(
                "post-v25", now=T0 + timedelta(seconds=1)
            )
            assert await store.put_profile(_profile("post-v25", 3))
            assert await store.put_profile(_profile("post-v25", 1))

            tombstone = await store.get_agent_lifecycle("post-v25")
            assert tombstone is not None
            assert tombstone.agent_incarnation == 1
            assert tombstone.profile_version == 3
            assert tombstone.lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert (
                tombstone.desired_process_state
                is AgentDesiredProcessState.STOPPED
            )
            assert await store.is_agent_deleted("post-v25")
            latest_profile = await store.get_profile("post-v25", 3)
            assert latest_profile is not None and latest_profile.enabled

            events = await store.list_agent_lifecycle_events(
                agent_id="post-v25"
            )
            assert [event.event_kind for event in events] == [
                "profile_published",
                "profile_published",
                "deleted",
                "profile_published",
            ]
            assert [event.new_profile_version for event in events] == [
                1,
                2,
                2,
                3,
            ]
            assert events[-1].new_lifecycle_state is (
                AgentLifecycleState.TOMBSTONED
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_delete_prepare_commit_and_direct_clear_create_fresh_incarnations(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            version_1 = _profile("recreated", 1)
            version_2 = _profile("recreated", 2)
            await store.put_profile(version_1)
            await store.put_profile(version_2)

            assert await store.retire_agent("recreated") == 2
            retired = await store.get_agent_lifecycle("recreated")
            assert retired is not None
            assert retired.agent_incarnation == 1
            assert retired.lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert all(
                not profile.enabled
                for profile in await store.list_profiles(agent_id="recreated")
            )

            assert await store.reactivate_agent(version_2)
            prepared = await store.get_agent_lifecycle("recreated")
            assert prepared is not None
            assert prepared.agent_incarnation == 1
            assert prepared.lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert await store.is_agent_deleted("recreated")
            assert all(
                profile.enabled
                for profile in await store.list_profiles(agent_id="recreated")
            )

            assert await store.commit_agent_reactivation(
                version_2,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                now=T0 + timedelta(seconds=2),
            )
            current = await store.get_agent_lifecycle("recreated")
            assert current is not None
            assert current.agent_incarnation == 2
            assert current.profile_version == 2
            assert current.lifecycle_state is AgentLifecycleState.ENABLED
            assert not await store.is_agent_deleted("recreated")

            retained = await store.list_agent_lifecycles(agent_id="recreated")
            assert [item.agent_incarnation for item in retained] == [1, 2]
            assert retained[0].lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert retained[1].lifecycle_state is AgentLifecycleState.ENABLED
            first_events = await store.list_agent_lifecycle_events(
                agent_id="recreated",
                agent_incarnation=1,
            )
            second_events = await store.list_agent_lifecycle_events(
                agent_id="recreated",
                agent_incarnation=2,
            )
            assert [event.event_kind for event in first_events] == [
                "profile_published",
                "profile_published",
                "retired",
                "profile_prepared",
            ]
            assert [event.event_kind for event in second_events] == [
                "reactivated"
            ]
            event_count = len(first_events) + len(second_events)

            # A normal route switch after recreation is not a new Agent
            # incarnation and does not manufacture another lifecycle event.
            assert await store.commit_agent_reactivation(
                version_2,
                channel="wechat",
                bot_id="bot",
                external_user_id="other-user",
                now=T0 + timedelta(seconds=3),
            )
            assert len(
                await store.list_agent_lifecycles(agent_id="recreated")
            ) == 2
            assert len(
                await store.list_agent_lifecycle_events(agent_id="recreated")
            ) == event_count

            await store.put_profile(_profile("direct-enabled", 1))
            assert await store.mark_agent_deleted("direct-enabled")
            assert await store.clear_agent_deleted("direct-enabled")
            direct_enabled = await store.list_agent_lifecycles(
                agent_id="direct-enabled"
            )
            assert [item.agent_incarnation for item in direct_enabled] == [1, 2]
            assert direct_enabled[0].lifecycle_state is (
                AgentLifecycleState.TOMBSTONED
            )
            assert direct_enabled[1].lifecycle_state is AgentLifecycleState.ENABLED
            assert [
                event.event_kind
                for event in await store.list_agent_lifecycle_events(
                    agent_id="direct-enabled", agent_incarnation=2
                )
            ] == ["deletion_cleared"]

            await store.put_profile(
                _profile("direct-disabled", 1, enabled=False)
            )
            assert await store.mark_agent_deleted("direct-disabled")
            assert await store.clear_agent_deleted("direct-disabled")
            direct_disabled = await store.get_agent_lifecycle("direct-disabled")
            assert direct_disabled is not None
            assert direct_disabled.agent_incarnation == 2
            assert direct_disabled.lifecycle_state is (
                AgentLifecycleState.DISABLED
            )
            assert direct_disabled.desired_process_state is (
                AgentDesiredProcessState.STOPPED
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_event_api_filters_and_append_only_triggers(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.put_profile(_profile("ledger", 1))
            await store.put_profile(_profile("ledger", 2))
            await store.put_profile(_profile("other-ledger", 1))

            events = await store.list_agent_lifecycle_events(agent_id="ledger")
            assert len(events) == 2
            assert await store.get_agent_lifecycle_event(
                events[0].lifecycle_event_id
            ) == events[0]
            assert await store.get_agent_lifecycle_event("missing-event") is None
            assert await store.list_agent_lifecycle_events(
                agent_id="ledger", limit=1
            ) == [events[0]]
            assert await store.list_agent_lifecycle_events(
                agent_id="ledger",
                agent_incarnation=1,
                after_sequence=1,
            ) == [events[1]]
            assert await store.list_agent_lifecycle_events(
                agent_id="ledger",
                agent_incarnation=1,
                after_sequence=2,
            ) == []
            assert await store.list_agent_lifecycle_events(limit=0) == []
            with pytest.raises(ValueError, match="agent_id is required"):
                await store.list_agent_lifecycle_events(agent_incarnation=1)
            with pytest.raises(
                ValueError,
                match="agent_id and agent_incarnation are required",
            ):
                await store.list_agent_lifecycle_events(
                    agent_id="ledger", after_sequence=0
                )

            event_id = events[0].lifecycle_event_id
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                await store._call(
                    lambda conn: conn.execute(
                        "UPDATE agent_lifecycle_events "
                        "SET source_kind=source_kind "
                        "WHERE lifecycle_event_id=?",
                        (event_id,),
                    )
                )
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                await store._call(
                    lambda conn: conn.execute(
                        "DELETE FROM agent_lifecycle_events "
                        "WHERE lifecycle_event_id=?",
                        (event_id,),
                    )
                )
            assert await store.get_agent_lifecycle_event(event_id) == events[0]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v25_migration_reconciles_compatibility_drift_and_replays_exactly(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.put_profile(_profile("profile-drift", 1))
        await seeded.put_profile(
            _profile("profile-drift", 2, enabled=False)
        )
        await seeded.put_profile(_profile("deleted-drift", 1))
        await seeded.put_profile(_profile("cleared-drift", 1))
        await seeded.mark_agent_deleted(
            "cleared-drift", now=T0 + timedelta(seconds=1)
        )
        await seeded.put_profile(_profile("deleted-no-lifecycle", 1))
        await seeded.close()

        deleted_at = (T0 + timedelta(seconds=2)).isoformat(timespec="microseconds")
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP TRIGGER IF EXISTS trg_agent_lifecycle_events_no_update"
            )
            connection.execute(
                "DROP TRIGGER IF EXISTS trg_agent_lifecycle_events_no_delete"
            )
            connection.execute("DROP TABLE agent_lifecycle_events")
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (25, 26, 27, 28, 29, 30, 31, 32)"
            )
            # Simulate Profile publication after v24 that never updated its
            # lifecycle projection.
            connection.execute(
                """UPDATE agent_lifecycle
                   SET profile_version=1, lifecycle_state='enabled',
                       desired_process_state='running',
                       retiring_at=NULL, tombstoned_at=NULL
                   WHERE agent_id='profile-drift' AND agent_incarnation=1"""
            )
            # A deletion compatibility marker must override a still-enabled
            # projection when v25 lands.
            connection.execute(
                "INSERT INTO deleted_agents(agent_id, deleted_at) VALUES (?, ?)",
                ("deleted-drift", deleted_at),
            )
            # A compatibility clear performed after v24 left a tombstone but
            # removed its marker; v25 must allocate a fresh incarnation.
            connection.execute(
                "DELETE FROM deleted_agents WHERE agent_id='cleared-drift'"
            )
            # A Profile and deletion marker may have been created after v24
            # without any lifecycle row at all.
            connection.execute(
                "DELETE FROM agent_lifecycle "
                "WHERE agent_id='deleted-no-lifecycle'"
            )
            connection.execute(
                "INSERT INTO deleted_agents(agent_id, deleted_at) VALUES (?, ?)",
                ("deleted-no-lifecycle", deleted_at),
            )
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            profile_drift = await migrated.get_agent_lifecycle("profile-drift")
            assert profile_drift is not None
            assert profile_drift.agent_incarnation == 1
            assert profile_drift.profile_version == 2
            assert profile_drift.lifecycle_state is AgentLifecycleState.DISABLED
            assert [
                event.event_kind
                for event in await migrated.list_agent_lifecycle_events(
                    agent_id="profile-drift"
                )
            ] == ["migration_seed", "compatibility_reconciled"]

            deleted_drift = await migrated.get_agent_lifecycle("deleted-drift")
            assert deleted_drift is not None
            assert deleted_drift.agent_incarnation == 1
            assert deleted_drift.lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert [
                event.event_kind
                for event in await migrated.list_agent_lifecycle_events(
                    agent_id="deleted-drift"
                )
            ] == ["migration_seed", "compatibility_reconciled"]

            cleared = await migrated.list_agent_lifecycles(
                agent_id="cleared-drift"
            )
            assert [item.agent_incarnation for item in cleared] == [1, 2]
            assert cleared[0].lifecycle_state is AgentLifecycleState.TOMBSTONED
            assert cleared[1].lifecycle_state is AgentLifecycleState.ENABLED
            assert [
                event.event_kind
                for event in await migrated.list_agent_lifecycle_events(
                    agent_id="cleared-drift"
                )
            ] == ["migration_seed", "compatibility_reconciled"]

            no_lifecycle = await migrated.get_agent_lifecycle(
                "deleted-no-lifecycle"
            )
            assert no_lifecycle is not None
            assert no_lifecycle.agent_incarnation == 1
            assert no_lifecycle.lifecycle_state is (
                AgentLifecycleState.TOMBSTONED
            )
            no_lifecycle_events = await migrated.list_agent_lifecycle_events(
                agent_id="deleted-no-lifecycle"
            )
            assert [event.event_kind for event in no_lifecycle_events] == [
                "compatibility_reconciled"
            ]
            assert no_lifecycle_events[0].previous_profile_version is None

            audited_agents = (
                "profile-drift",
                "deleted-drift",
                "cleared-drift",
                "deleted-no-lifecycle",
            )
            before_replay = {
                agent_id: [
                    event.lifecycle_event_id
                    for event in await migrated.list_agent_lifecycle_events(
                        agent_id=agent_id
                    )
                ]
                for agent_id in audited_agents
            }
            audited_events = []
            for agent_id in audited_agents:
                audited_events.extend(
                    await migrated.list_agent_lifecycle_events(
                        agent_id=agent_id
                    )
                )
            assert all(
                event.actor_kind == "migration" and event.actor_id is not None
                for event in audited_events
            )
        finally:
            await migrated.close()

        # A crash after the schema/table work but before retaining the marker
        # must replay without relabeling v25-created incarnations or events.
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (25, 26, 27, 28, 29, 30, 31, 32)"
            )
            connection.commit()
        replayed = SQLiteStore(path)
        await replayed.initialize()
        try:
            for agent_id, expected_ids in before_replay.items():
                events = await replayed.list_agent_lifecycle_events(
                    agent_id=agent_id
                )
                assert [event.lifecycle_event_id for event in events] == (
                    expected_ids
                )
            assert len(
                await replayed.list_agent_lifecycles(agent_id="cleared-drift")
            ) == 2
        finally:
            await replayed.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (32,)
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    asyncio.run(scenario())


def test_live_process_tombstone_rejection_rolls_back_compatibility_writes(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize(recover_startup_state=False)
        try:
            epoch = await store.activate_supervisor_epoch(
                owner_instance_id="owner-live",
                channel="wechat",
                bot_id="bot",
                started_at=T0,
            )
            await store.put_profile(_profile("live-agent", 1))
            await store.set_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                active_agent_id="live-agent",
            )
            process = await store.begin_agent_process_generation(
                agent_id="live-agent",
                agent_incarnation=1,
                supervisor_epoch=epoch.epoch,
                owner_instance_id="owner-live",
                generation_capability="generation-secret-live",
                lifetime_lock_identity="agent-lock:live-agent:1",
                lifetime_lock_acquired_at=T0 + timedelta(seconds=1),
                process_lease_identity="lease-live-agent-1",
                process_lease_token="lease-secret-live-agent",
                lease_expires_at=T0 + timedelta(minutes=10),
                started_at=T0 + timedelta(seconds=2),
            )
            assert process.active

            with pytest.raises(
                InvalidTransition,
                match="cannot tombstone Agent with a live process generation",
            ):
                await store.retire_agent("live-agent")
            profile = await store.get_profile("live-agent", 1)
            assert profile is not None and profile.enabled
            assert not await store.is_agent_deleted("live-agent")

            with pytest.raises(
                InvalidTransition,
                match="cannot tombstone Agent with a live process generation",
            ):
                await store.mark_agent_deleted("live-agent")
            lifecycle = await store.get_agent_lifecycle("live-agent")
            assert lifecycle is not None
            assert lifecycle.lifecycle_state is AgentLifecycleState.ENABLED
            assert not await store.is_agent_deleted("live-agent")
            assert [
                event.event_kind
                for event in await store.list_agent_lifecycle_events(
                    agent_id="live-agent"
                )
            ] == ["profile_published"]
            assert await store.get_route(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
            ) == "live-agent"
            retained_process = await store.get_agent_process(
                "live-agent", 1, process.worker_generation
            )
            assert retained_process is not None and retained_process.active
        finally:
            await store.close()

    asyncio.run(scenario())
