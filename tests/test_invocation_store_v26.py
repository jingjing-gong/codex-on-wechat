"""Schema-v26 invocation truth plus additive v27 dispatch metadata."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.modes import AgentMode
from src.runtime.models import TaskState
from src.runtime.policy import AgentProfile
from src.runtime.sqlite_store import SQLiteStore


def _task(task_id: str, *, text: str | None = None) -> dict[str, object]:
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
        "inputs": {"text": text or task_id},
    }


async def _admission_snapshot(store: SQLiteStore) -> dict[str, object]:
    def op(conn):
        per_agent = conn.execute(
            "SELECT unfinished_count,next_ready_sequence "
            "FROM agent_admission_counters "
            "WHERE agent_id='codex' AND agent_incarnation=1"
        ).fetchone()
        global_row = conn.execute(
            "SELECT unfinished_count FROM global_agent_admission_counter "
            "WHERE singleton=1"
        ).fetchone()
        return {
            "per_agent": (
                None
                if per_agent is None
                else (int(per_agent[0]), int(per_agent[1]))
            ),
            "global": None if global_row is None else int(global_row[0]),
            "slots": int(
                conn.execute("SELECT COUNT(*) FROM agent_execution_slots").fetchone()[0]
            ),
            "dispatch_attempts": int(
                conn.execute("SELECT COUNT(*) FROM agent_dispatch_attempts").fetchone()[0]
            ),
        }

    return await store._call(op)


async def _projected_states(
    store: SQLiteStore, *, task_id: str, mailbox_id: str
) -> dict[str, str]:
    def op(conn):
        task = conn.execute(
            "SELECT state,current_execution_id FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        execution = conn.execute(
            "SELECT state FROM task_executions WHERE execution_id=?",
            (task["current_execution_id"],),
        ).fetchone()
        task_invocation = conn.execute(
            "SELECT state FROM agent_invocations WHERE invocation_id=?",
            (task["current_execution_id"],),
        ).fetchone()
        mailbox = conn.execute(
            "SELECT state,current_invocation_id FROM agent_mailbox WHERE mailbox_id=?",
            (mailbox_id,),
        ).fetchone()
        mailbox_invocation = conn.execute(
            "SELECT state FROM agent_invocations WHERE invocation_id=?",
            (mailbox["current_invocation_id"],),
        ).fetchone()
        return {
            "task": str(task["state"]),
            "execution": str(execution["state"]),
            "task_invocation": str(task_invocation["state"]),
            "mailbox": str(mailbox["state"]),
            "mailbox_invocation": str(mailbox_invocation["state"]),
        }

    return await store._call(op)


async def _execute_transaction(
    store: SQLiteStore,
    *statements: tuple[str, tuple[object, ...]],
) -> None:
    """Execute raw test mutations while rolling back deferred-FK failures."""

    def op(conn):
        conn.execute("BEGIN IMMEDIATE")
        try:
            for sql, parameters in statements:
                conn.execute(sql, parameters)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    await store._call(op)


def test_fresh_v27_schema_has_canonical_foreign_keys(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            def inspect(conn):
                tables = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                sql = "\n".join(
                    str(row[0] or "")
                    for row in conn.execute(
                        "SELECT sql FROM sqlite_master "
                        "WHERE type IN ('table','index')"
                    ).fetchall()
                )
                foreign_keys = {
                    table: {
                        (str(row[2]), str(row[3]), str(row[4]))
                        for row in conn.execute(
                            f"PRAGMA foreign_key_list({table})"
                        ).fetchall()
                    }
                    for table in (
                        "tasks",
                        "task_executions",
                        "agent_mailbox",
                        "agent_invocations",
                        "agent_execution_slots",
                        "agent_dispatch_attempts",
                    )
                }
                return {
                    "version": int(
                        conn.execute(
                            "SELECT MAX(version) FROM schema_migrations"
                        ).fetchone()[0]
                    ),
                    "tables": tables,
                    "sql": sql,
                    "foreign_keys": foreign_keys,
                    "violations": [
                        tuple(row)
                        for row in conn.execute("PRAGMA foreign_key_check").fetchall()
                    ],
                }

            schema = await store._call(inspect)
            assert schema["version"] == 32
            assert {
                "agent_invocations",
                "agent_admission_counters",
                "global_agent_admission_counter",
                "agent_execution_slots",
                "agent_dispatch_attempts",
            }.issubset(schema["tables"])
            assert schema["violations"] == []
            assert "tasks_v26" not in schema["sql"]
            assert "task_executions_v26" not in schema["sql"]
            assert "agent_mailbox_v26" not in schema["sql"]

            foreign_keys = schema["foreign_keys"]
            assert (
                "task_executions",
                "current_execution_id",
                "execution_id",
            ) in foreign_keys["tasks"]
            assert (
                "agent_invocations",
                "execution_id",
                "invocation_id",
            ) in foreign_keys["task_executions"]
            assert (
                "agent_invocations",
                "current_invocation_id",
                "invocation_id",
            ) in foreign_keys["agent_mailbox"]
            assert (
                "tasks",
                "task_id",
                "task_id",
            ) in foreign_keys["agent_invocations"]
            assert (
                "task_executions",
                "execution_id",
                "execution_id",
            ) in foreign_keys["agent_invocations"]
            assert (
                "agent_mailbox",
                "mailbox_id",
                "mailbox_id",
            ) in foreign_keys["agent_invocations"]
            assert (
                "agent_invocations",
                "invocation_id",
                "invocation_id",
            ) in foreign_keys["agent_execution_slots"]
            assert (
                "agent_execution_slots",
                "slot_id",
                "slot_id",
            ) in foreign_keys["agent_dispatch_attempts"]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_acceptance_creates_unified_invocations_and_counters_without_fake_slots(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task("task-first"))
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="mailbox second",
                request_id="mailbox-second",
                message_id="mailbox-message-second",
            )

            invocations = await store.list_agent_invocations(agent_id="codex")
            assert [item.work_kind.value for item in invocations] == [
                "task",
                "mailbox",
            ]
            assert [item.ready_sequence for item in invocations] == [1, 2]
            assert [item.state.value for item in invocations] == ["queued", "queued"]
            assert all(
                item.dispatch_backend.value == "compatibility"
                for item in invocations
            )

            task_invocation, mailbox_invocation = invocations
            assert task.execution_id == task_invocation.invocation_id
            assert task_invocation.execution_id == task_invocation.invocation_id
            assert task_invocation.work_id == task_invocation.invocation_id
            assert task_invocation.task_id == task.task_id
            assert task_invocation.mailbox_id is None
            assert mailbox.current_invocation_id == mailbox_invocation.invocation_id
            assert mailbox_invocation.work_id == mailbox.message_id
            assert mailbox_invocation.mailbox_id == mailbox.mailbox_id
            assert mailbox_invocation.task_id is None
            assert mailbox_invocation.execution_id is None

            assert await _admission_snapshot(store) == {
                "per_agent": (2, 3),
                "global": 2,
                "slots": 0,
                "dispatch_attempts": 0,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_task_and_mailbox_cross_row_identity_corruption_is_rejected(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            first_task = await store.create_task(_task("owned-task-one"))
            second_task = await store.create_task(_task("owned-task-two"))
            first_mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="owned mailbox one",
                request_id="owned-mailbox-one",
                message_id="owned-mailbox-message-one",
            )
            second_mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="owned mailbox two",
                request_id="owned-mailbox-two",
                message_id="owned-mailbox-message-two",
            )

            # Every value below exists independently and passed the v25
            # single-column FKs.  Schema v26 must reject recombining those
            # valid identities into a cross-owned projection.
            corruptions = (
                (
                    "UPDATE tasks SET current_execution_id=? WHERE task_id=?",
                    (second_task.execution_id, first_task.task_id),
                ),
                (
                    "UPDATE agent_invocations SET task_id=? WHERE invocation_id=?",
                    (second_task.task_id, first_task.execution_id),
                ),
                (
                    "UPDATE task_executions SET dispatch_backend='child' "
                    "WHERE execution_id=?",
                    (first_task.execution_id,),
                ),
                (
                    "UPDATE agent_mailbox SET current_invocation_id=? "
                    "WHERE mailbox_id=?",
                    (
                        second_mailbox.current_invocation_id,
                        first_mailbox.mailbox_id,
                    ),
                ),
                (
                    "UPDATE agent_invocations SET work_id='wrong-message' "
                    "WHERE invocation_id=?",
                    (first_mailbox.current_invocation_id,),
                ),
            )
            for sql, parameters in corruptions:
                with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                    await _execute_transaction(store, (sql, parameters))

            retained = await store._call(
                lambda conn: {
                    "task_pointer": conn.execute(
                        "SELECT current_execution_id FROM tasks WHERE task_id=?",
                        (first_task.task_id,),
                    ).fetchone()[0],
                    "task_invocation": tuple(
                        conn.execute(
                            "SELECT task_id,work_id,dispatch_backend "
                            "FROM agent_invocations WHERE invocation_id=?",
                            (first_task.execution_id,),
                        ).fetchone()
                    ),
                    "execution_backend": conn.execute(
                        "SELECT dispatch_backend FROM task_executions "
                        "WHERE execution_id=?",
                        (first_task.execution_id,),
                    ).fetchone()[0],
                    "mailbox_pointer": conn.execute(
                        "SELECT current_invocation_id FROM agent_mailbox "
                        "WHERE mailbox_id=?",
                        (first_mailbox.mailbox_id,),
                    ).fetchone()[0],
                    "mailbox_work_id": conn.execute(
                        "SELECT work_id FROM agent_invocations "
                        "WHERE invocation_id=?",
                        (first_mailbox.current_invocation_id,),
                    ).fetchone()[0],
                    "foreign_keys": [
                        tuple(row)
                        for row in conn.execute("PRAGMA foreign_key_check").fetchall()
                    ],
                }
            )
            assert retained == {
                "task_pointer": first_task.execution_id,
                "task_invocation": (
                    first_task.task_id,
                    first_task.execution_id,
                    "compatibility",
                ),
                "execution_backend": "compatibility",
                "mailbox_pointer": first_mailbox.current_invocation_id,
                "mailbox_work_id": first_mailbox.message_id,
                "foreign_keys": [],
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_slot_and_dispatch_attempt_cross_owner_corruption_is_rejected(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await store.put_profile(
                AgentProfile(agent_id="planner", display_name="Planner")
            )
            await store.put_mode(AgentMode(mode_id="chat"), agent_id="planner")
            first_task = await store.create_task(_task("child-invocation-one"))
            second_task = await store.create_task(_task("child-invocation-two"))

            epoch = await store._call(
                lambda conn: int(
                    conn.execute(
                        """INSERT INTO supervisor_epochs (
                               owner_instance_id,channel,bot_id,started_at)
                           VALUES ('ownership-test-supervisor','wechat','bot',
                                   '2026-01-01T00:00:00+00:00')"""
                    ).lastrowid
                )
            )
            process_insert = (
                """INSERT INTO agent_processes (
                       agent_id,agent_incarnation,worker_generation,
                       supervisor_epoch,observed_state,
                       generation_capability_hash,lifetime_lock_identity,
                       lifetime_lock_acquired_at,process_lease_identity,
                       process_lease_token_hash,lease_expires_at,started_at)
                   VALUES (?,?,1,?,'starting',?,?,?,?,?,?,?)"""
            )
            now = "2026-01-01T00:00:00+00:00"
            lease = "2026-01-01T01:00:00+00:00"
            await _execute_transaction(
                store,
                (
                    "UPDATE task_executions SET dispatch_backend='child' "
                    "WHERE execution_id IN (?,?)",
                    (first_task.execution_id, second_task.execution_id),
                ),
                (
                    "UPDATE agent_invocations SET dispatch_backend='child' "
                    "WHERE invocation_id IN (?,?)",
                    (first_task.execution_id, second_task.execution_id),
                ),
                (
                    process_insert,
                    (
                        "codex",
                        1,
                        epoch,
                        "a" * 64,
                        "codex-lifetime-lock",
                        now,
                        "codex-process-lease",
                        "b" * 64,
                        lease,
                        now,
                    ),
                ),
                (
                    process_insert,
                    (
                        "planner",
                        1,
                        epoch,
                        "c" * 64,
                        "planner-lifetime-lock",
                        now,
                        "planner-process-lease",
                        "d" * 64,
                        lease,
                        now,
                    ),
                ),
                (
                    """INSERT INTO agent_execution_slots (
                           slot_id,agent_id,agent_incarnation,slot_sequence,
                           slot_kind,state,dispatch_backend,invocation_id,
                           worker_generation,acquired_at)
                       VALUES ('owned-slot','codex',1,1,'invocation','active',
                               'child',?,1,?)""",
                    (first_task.execution_id, now),
                ),
            )

            # Planner has a real lifecycle and process generation, while the
            # invocation belongs to codex.  All independent FKs resolve; only
            # the compound slot/invocation ownership key rejects this row.
            with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                await _execute_transaction(
                    store,
                    (
                        """INSERT INTO agent_execution_slots (
                               slot_id,agent_id,agent_incarnation,slot_sequence,
                               slot_kind,state,dispatch_backend,invocation_id,
                               worker_generation,acquired_at)
                           VALUES ('cross-agent-slot','planner',1,1,
                                   'invocation','active','child',?,1,?)""",
                        (first_task.execution_id, now),
                    ),
                )

            attempt_insert = (
                """INSERT INTO agent_dispatch_attempts (
                       dispatch_attempt_id,invocation_id,slot_id,agent_id,
                       agent_incarnation,worker_generation,supervisor_epoch,
                       dispatch_backend,claim_token_hash,lease_identity,
                       lease_expires_at,invocation_job_identity,created_at)
                   VALUES (?,?,?,'codex',1,1,?,'child',?,?,?,?,?)"""
            )
            # The second invocation is a valid child invocation owned by the
            # same Agent/process, but it was never assigned owned-slot.
            with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                await _execute_transaction(
                    store,
                    (
                        attempt_insert,
                        (
                            "cross-invocation-attempt",
                            second_task.execution_id,
                            "owned-slot",
                            epoch,
                            "e" * 64,
                            "cross-attempt-lease",
                            lease,
                            "cross-attempt-job",
                            now,
                        ),
                    ),
                )

            await _execute_transaction(
                store,
                (
                    attempt_insert,
                    (
                        "owned-attempt",
                        first_task.execution_id,
                        "owned-slot",
                        epoch,
                        "f" * 64,
                        "owned-attempt-lease",
                        lease,
                        "owned-attempt-job",
                        now,
                    ),
                ),
            )
            assert await store._call(
                lambda conn: [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT invocation_id,slot_id,agent_id,agent_incarnation "
                        "FROM agent_dispatch_attempts"
                    ).fetchall()
                ]
            ) == [(first_task.execution_id, "owned-slot", "codex", 1)]
            assert await store._call(
                lambda conn: [
                    tuple(row)
                    for row in conn.execute("PRAGMA foreign_key_check").fetchall()
                ]
            ) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_compatibility_dual_write_and_terminal_release_are_exactly_once(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task("compat-task"))
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="compat mailbox",
                request_id="compat-mailbox",
                message_id="compat-mailbox-message",
            )

            task_claim = await store.claim_task_by_id(task.task_id, "task-worker")
            assert task_claim is not None
            assert await _projected_states(
                store, task_id=task.task_id, mailbox_id=mailbox.mailbox_id
            ) == {
                "task": "dispatching",
                "execution": "dispatching",
                "task_invocation": "dispatching",
                "mailbox": "pending",
                "mailbox_invocation": "queued",
            }
            assert await store.mark_task_running(
                task.task_id,
                task_claim.claim_token,
                execution_id=task_claim.execution_id,
            )
            assert await store.claim_mailbox(
                "codex", "mailbox-worker", limit=1
            ) == []

            completed = await store.complete_task(
                task.task_id,
                result={"status": "completed"},
                claim_token=task_claim.claim_token,
                execution_id=task_claim.execution_id,
            )
            assert completed.state.value == "completed"
            assert (await _admission_snapshot(store))["per_agent"] == (1, 3)
            assert await _projected_states(
                store, task_id=task.task_id, mailbox_id=mailbox.mailbox_id
            ) == {
                "task": "completed",
                "execution": "completed",
                "task_invocation": "completed",
                "mailbox": "pending",
                "mailbox_invocation": "queued",
            }

            mailbox_claims = await store.claim_mailbox(
                "codex", "mailbox-worker", limit=1
            )
            assert [item.mailbox_id for item in mailbox_claims] == [mailbox.mailbox_id]
            mailbox_claim = mailbox_claims[0]
            assert await store.mark_mailbox_processing(
                mailbox.mailbox_id, mailbox_claim.claim_token
            )
            assert await _projected_states(
                store, task_id=task.task_id, mailbox_id=mailbox.mailbox_id
            ) == {
                "task": "completed",
                "execution": "completed",
                "task_invocation": "completed",
                "mailbox": "processing",
                "mailbox_invocation": "running",
            }

            assert await store.mark_mailbox_processed(
                mailbox.mailbox_id, mailbox_claim.claim_token
            )
            assert await _projected_states(
                store, task_id=task.task_id, mailbox_id=mailbox.mailbox_id
            ) == {
                "task": "completed",
                "execution": "completed",
                "task_invocation": "completed",
                "mailbox": "processed",
                "mailbox_invocation": "completed",
            }

            released = await store._call(
                lambda conn: [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT state,admission_released_at,terminal_at "
                        "FROM agent_invocations ORDER BY ready_sequence"
                    ).fetchall()
                ]
            )
            assert [row[0] for row in released] == ["completed", "completed"]
            assert all(row[1] is not None and row[2] is not None for row in released)
            assert await _admission_snapshot(store) == {
                "per_agent": (0, 3),
                "global": 0,
                "slots": 0,
                "dispatch_attempts": 0,
            }

            replay = await store.complete_task(
                task.task_id,
                result={"status": "completed"},
                claim_token=task_claim.claim_token,
                execution_id=task_claim.execution_id,
            )
            assert replay.state.value == "completed"
            assert not await store.mark_mailbox_processed(
                mailbox.mailbox_id, mailbox_claim.claim_token
            )
            assert await _admission_snapshot(store) == {
                "per_agent": (0, 3),
                "global": 0,
                "slots": 0,
                "dispatch_attempts": 0,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_claimed_compatibility_filter_maps_to_canonical_dispatching(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task("claimed-filter"))
            claim = await store.claim_task_by_id(task.task_id, "worker")
            assert claim is not None
            assert claim.task.state is TaskState.CLAIMED
            assert claim.execution.state.value == "claimed"

            for state_filter in (TaskState.CLAIMED, "claimed", "dispatching"):
                listed = await store.list_tasks(state=state_filter)
                assert [item.task_id for item in listed] == [task.task_id]
                assert listed[0].state is TaskState.CLAIMED

            stored = await store._call(
                lambda conn: (
                    conn.execute(
                        "SELECT state FROM tasks WHERE task_id=?", (task.task_id,)
                    ).fetchone()[0],
                    conn.execute(
                        "SELECT state FROM task_executions WHERE execution_id=?",
                        (claim.execution_id,),
                    ).fetchone()[0],
                    conn.execute(
                        "SELECT state FROM agent_invocations WHERE invocation_id=?",
                        (claim.execution_id,),
                    ).fetchone()[0],
                )
            )
            assert stored == ("dispatching", "dispatching", "dispatching")
        finally:
            await store.close()

    asyncio.run(scenario())


def test_retry_allocates_a_fresh_invocation_identity_and_ready_sequence(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task("retry-task"))
            first_id = task.execution_id
            first_claim = await store.claim_task_by_id(task.task_id, "worker-a")
            assert first_claim is not None
            assert first_claim.execution_id == first_id
            assert await store.mark_task_running(
                task.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            failed = await store.complete_task(
                task.task_id,
                result={"status": "failed", "error": "first failure"},
                claim_token=first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            assert failed.state.value == "failed"

            retried = await store.retry_task(task.task_id, actor="user")
            assert retried is not None
            assert retried.state.value == "queued"
            assert retried.execution_id is not None
            assert retried.execution_id != first_id

            executions = await store.list_task_executions(task.task_id)
            assert [item.attempt for item in executions] == [1, 2]
            assert [item.execution_id for item in executions] == [
                first_id,
                retried.execution_id,
            ]
            assert [item.state.value for item in executions] == ["failed", "queued"]

            invocations = await store.list_agent_invocations(agent_id="codex")
            assert [item.invocation_id for item in invocations] == [
                first_id,
                retried.execution_id,
            ]
            assert [item.ready_sequence for item in invocations] == [1, 2]
            assert [item.state.value for item in invocations] == ["failed", "queued"]
            assert invocations[0].admission_released_at is not None
            assert invocations[1].admission_released_at is None
            assert await _admission_snapshot(store) == {
                "per_agent": (1, 3),
                "global": 1,
                "slots": 0,
                "dispatch_attempts": 0,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_mailbox_recovery_orphans_without_requeue_and_releases_once(tmp_path):
    async def scenario() -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store = SQLiteStore(tmp_path / "runtime.sqlite", clock=lambda: base)
        await store.initialize()
        try:
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="do not replay",
                request_id="orphan-mailbox",
                message_id="orphan-mailbox-message",
                now=base,
            )
            claim = (
                await store.claim_mailbox(
                    "codex",
                    "mailbox-worker",
                    lease_seconds=1,
                    now=base,
                )
            )[0]
            assert await store.mark_mailbox_processing(
                mailbox.mailbox_id, claim.claim_token, now=base
            )

            report = await store.reconcile(now=base + timedelta(seconds=2))
            assert report.mailbox_requeued == 1
            recovered = await store.get_mailbox_item(mailbox.mailbox_id)
            assert recovered is not None
            assert recovered.state.value == "orphaned_mailbox"
            assert recovered.claim_token is None
            assert recovered.claimed_by is None
            assert await store._call(
                lambda conn: conn.execute(
                    "SELECT next_attempt_at FROM agent_mailbox WHERE mailbox_id=?",
                    (mailbox.mailbox_id,),
                ).fetchone()[0]
            ) is None
            invocation = await store.get_agent_invocation(
                recovered.current_invocation_id
            )
            assert invocation is not None
            assert invocation.state.value == "orphaned"
            assert invocation.admission_released_at is not None
            assert invocation.terminal_at is not None
            assert await store.claim_mailbox(
                "codex", "replacement-worker", now=base + timedelta(seconds=3)
            ) == []
            assert await _admission_snapshot(store) == {
                "per_agent": (0, 2),
                "global": 0,
                "slots": 0,
                "dispatch_attempts": 0,
            }

            repeated = await store.reconcile(now=base + timedelta(seconds=4))
            assert repeated.mailbox_requeued == 0
            assert await _admission_snapshot(store) == {
                "per_agent": (0, 2),
                "global": 0,
                "slots": 0,
                "dispatch_attempts": 0,
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v26_migration_dead_letters_partial_legacy_mailbox_snapshot(
    tmp_path, monkeypatch
):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        # Build an exact schema-v25 database by stopping just before the v26
        # rebuild, then insert the kind of incomplete snapshot retained by an
        # older compatibility sender.
        with monkeypatch.context() as migration_pause:
            migration_pause.setattr(
                SQLiteStore,
                "_apply_schema_v26_rebuild",
                lambda self, conn: None,
            )
            legacy = SQLiteStore(path)
            await legacy.initialize()
            try:
                def seed_partial_snapshot(conn):
                    mailbox_time = "2026-01-01T00:00:00+00:00"
                    task_time = "2026-01-01T00:00:01+00:00"
                    conn.execute(
                        """INSERT INTO messages (
                               message_id,request_id,source_agent_id,
                               destination_agent_id,visibility,priority,content,
                               payload_json,created_at)
                           VALUES (?,?,?,?,'internal',0,?,'{}',?)""",
                        (
                            "legacy-message",
                            "legacy-request",
                            "planner",
                            "codex",
                            "legacy partial snapshot",
                            mailbox_time,
                        ),
                    )
                    conn.execute(
                        """INSERT INTO agent_mailbox (
                               mailbox_id,message_id,request_id,source_agent_id,
                               destination_agent_id,content,payload_json,
                               execution_snapshot_json,state,attempts,created_at)
                           VALUES (?,?,?,?,?,?,?,?,'pending',0,?)""",
                        (
                            "legacy-mailbox",
                            "legacy-message",
                            "legacy-request",
                            "planner",
                            "codex",
                            "legacy partial snapshot",
                            "{}",
                            json.dumps({"profile_version": 1}),
                            mailbox_time,
                        ),
                    )
                    conn.execute(
                        """INSERT INTO conversations (
                               conversation_id,channel,bot_id,external_user_id,
                               session_id,agent_id,mode_id,profile_version,
                               policy_version,created_at,updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            "legacy-conversation",
                            "wechat",
                            "bot",
                            "user",
                            "default",
                            "codex",
                            "chat",
                            1,
                            1,
                            task_time,
                            task_time,
                        ),
                    )
                    conn.execute(
                        """INSERT INTO tasks (
                               task_id,channel,bot_id,external_user_id,session_id,
                               agent_id,conversation_id,mode_id,profile_version,
                               policy_version,reply_target_json,inputs_json,state,
                               attempts,child_depth,created_at,updated_at,
                               metadata_json)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,
                                   'queued',0,0,?,?,?)""",
                        (
                            "legacy-task",
                            "wechat",
                            "bot",
                            "user",
                            "default",
                            "codex",
                            "legacy-conversation",
                            "chat",
                            1,
                            1,
                            "{}",
                            json.dumps({"text": "newer task"}),
                            task_time,
                            task_time,
                            "{}",
                        ),
                    )

                await legacy._call(seed_partial_snapshot)
                assert await legacy._call(
                    lambda conn: conn.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ) == 25
            finally:
                await legacy.close()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            mailbox = await migrated.get_mailbox_item("legacy-mailbox")
            assert mailbox is not None
            assert mailbox.state.value == "dead_letter"
            assert mailbox.current_invocation_id is not None
            invocation = await migrated.get_agent_invocation(
                mailbox.current_invocation_id
            )
            assert invocation is not None
            assert invocation.state.value == "failed"
            assert invocation.admission_released_at is not None
            assert invocation.terminal_at is not None
            assert await migrated.claim_mailbox("codex", "worker") == []
            migrated_order = await migrated._call(
                lambda conn: [
                    (str(row[0]), row[1], int(row[2]))
                    for row in conn.execute(
                        "SELECT work_kind,mailbox_id,ready_sequence "
                        "FROM agent_invocations "
                        "WHERE agent_id='codex' AND agent_incarnation=1 "
                        "ORDER BY ready_sequence"
                    ).fetchall()
                ]
            )
            # Migration merges both work kinds by durable acceptance time;
            # table-by-table replay must not put every task ahead of mailboxes.
            assert migrated_order == [
                ("mailbox", "legacy-mailbox", 1),
                ("task", None, 2),
            ]
            assert await _admission_snapshot(migrated) == {
                "per_agent": (1, 3),
                "global": 1,
                "slots": 0,
                "dispatch_attempts": 0,
            }
            assert await migrated._call(
                lambda conn: [
                    tuple(row)
                    for row in conn.execute("PRAGMA foreign_key_check").fetchall()
                ]
            ) == []
        finally:
            await migrated.close()

    asyncio.run(scenario())
