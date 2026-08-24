"""Schema-v33 session/Agent working-directory store regressions."""

from __future__ import annotations

import asyncio
import json
import shlex
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.runtime.policy import AgentProfile
from src.runtime.sqlite_store import SQLiteStore, StoreError
from src.runtime.store import (
    WORKING_DIRECTORY_RESPONSE_MAX_CHARS,
    format_working_directory_response,
)


SCOPE = {
    "channel": "wechat",
    "bot_id": "bot",
    "external_user_id": "user",
    "session_id": "default",
    "agent_id": "codex",
}


def test_schema_v33_migrates_a_v32_database(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "v32-to-v33.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()

        # Recreate the exact additive boundary: all v32 schema is retained,
        # while the v33 marker and table are absent.
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP INDEX idx_session_agent_working_directories_agent"
            )
            connection.execute("DROP TABLE session_agent_working_directories")
            connection.execute("DELETE FROM schema_migrations WHERE version>=33")
            connection.commit()

        migrated = SQLiteStore(path)
        await migrated.initialize()
        await migrated.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall() == [(version,) for version in range(1, 36)]
            columns = {
                row[1]: row
                for row in connection.execute(
                    "PRAGMA table_info(session_agent_working_directories)"
                )
            }
            assert set(columns) == {
                "channel",
                "bot_id",
                "external_user_id",
                "session_id",
                "agent_id",
                "relative_path",
                "directory_device",
                "directory_inode",
                "updated_by",
                "updated_at",
            }
            assert "absolute_path" not in columns
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    asyncio.run(scenario())


def test_schema_v33_finishes_a_landed_table_without_its_marker(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "partial-v33.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()

        # Model a crash after the table DDL landed but before its marker and
        # derivable lookup index became durable.
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP INDEX idx_session_agent_working_directories_agent"
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version>=33"
            )
            connection.commit()

        recovered = SQLiteStore(path)
        await recovered.initialize()
        await recovered.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (35,)
            assert connection.execute(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' "
                "AND name='idx_session_agent_working_directories_agent'"
            ).fetchone() == ("session_agent_working_directories",)

    asyncio.run(scenario())


def test_schema_v33_rejects_a_malformed_partial_table_without_marking_it(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "malformed-v33.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP INDEX idx_session_agent_working_directories_agent"
            )
            connection.execute("DROP TABLE session_agent_working_directories")
            connection.execute(
                "CREATE TABLE session_agent_working_directories "
                "(channel TEXT PRIMARY KEY)"
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version>=33"
            )
            connection.commit()

        malformed = SQLiteStore(path)
        with pytest.raises(StoreError, match="schema v33.*incomplete"):
            await malformed.initialize()
        await malformed.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (32,)
            assert [
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(session_agent_working_directories)"
                )
            ] == ["channel"]

    asyncio.run(scenario())


def test_schema_v33_marker_without_its_table_fails_closed(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "missing-v33.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP INDEX idx_session_agent_working_directories_agent"
            )
            connection.execute("DROP TABLE session_agent_working_directories")
            connection.commit()

        missing = SQLiteStore(path)
        with pytest.raises(StoreError, match="marker exists without"):
            await missing.initialize()
        await missing.close()
        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (35,)
            assert connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='session_agent_working_directories'"
            ).fetchone() is None

    asyncio.run(scenario())


def test_schema_v33_rejects_a_scope_collapsing_unique_lookup_index(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "unique-index-v33.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()
        with sqlite3.connect(path) as connection:
            connection.execute(
                "DROP INDEX idx_session_agent_working_directories_agent"
            )
            connection.execute(
                "CREATE UNIQUE INDEX "
                "idx_session_agent_working_directories_agent "
                "ON session_agent_working_directories(agent_id)"
            )
            connection.commit()

        malformed = SQLiteStore(path)
        with pytest.raises(StoreError, match="index conflicts"):
            await malformed.initialize()
        await malformed.close()

    asyncio.run(scenario())


def test_working_directories_are_scope_isolated_and_persistent(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "working-directories.sqlite"
        store = SQLiteStore(path)
        try:
            assert await store.get_session_working_directory(**SCOPE) is None
            first = await store.set_session_working_directory(
                "projects/alpha",
                absolute_path="/workspace/projects/alpha",
                directory_device=11,
                directory_inode=101,
                updated_by="user",
                now=datetime(2026, 8, 16, 1, tzinfo=timezone.utc),
                **SCOPE,
            )
            assert first["changed"] is True
            assert first["relative_path"] == "projects/alpha"
            assert first["command_response"] == (
                "working directory: /workspace/projects/alpha"
            )
            unchanged = await store.set_session_working_directory(
                "projects/alpha/.",
                absolute_path="/workspace/projects/alpha",
                directory_device=11,
                directory_inode=101,
                updated_by="someone-else",
                now=datetime(2026, 8, 16, 2, tzinfo=timezone.utc),
                **SCOPE,
            )
            assert unchanged["changed"] is False
            assert unchanged["updated_by"] == "user"

            planner_scope = {**SCOPE, "agent_id": "planner"}
            other_session = {**SCOPE, "session_id": "other"}
            await store.set_session_working_directory(
                "planner",
                absolute_path="/workspace/planner",
                directory_device=11,
                directory_inode=102,
                updated_by="user",
                **planner_scope,
            )
            await store.set_session_working_directory(
                ".",
                absolute_path="/workspace",
                directory_device=11,
                directory_inode=100,
                updated_by="user",
                **other_session,
            )
            assert (
                await store.get_session_working_directory(**planner_scope)
            )["relative_path"] == "planner"
            assert (
                await store.get_session_working_directory(**other_session)
            )["relative_path"] == "."
        finally:
            await store.close()

        reopened = SQLiteStore(path)
        try:
            selected = await reopened.get_session_working_directory(**SCOPE)
            assert selected is not None
            assert selected["relative_path"] == "projects/alpha"
            assert selected["directory_device"] == 11
            assert selected["directory_inode"] == 101
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_cd_receipt_and_preference_commit_atomically_and_replay_original(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "atomic-cd.sqlite"
        store = SQLiteStore(path)
        try:
            await store.begin_command_receipt(
                "cd-alpha",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="message-cd-alpha",
                command_name="cd",
                command_args=('"projects', 'alpha"'),
                command_text='/cd "projects alpha"',
            )
            result = await store.set_session_working_directory(
                "projects alpha",
                absolute_path="/workspace/projects alpha",
                directory_device=21,
                directory_inode=201,
                updated_by="user",
                command_id="cd-alpha",
                now=datetime(2026, 8, 16, 3, tzinfo=timezone.utc),
                **SCOPE,
            )
            receipt = result["command_receipt"]
            assert receipt["state"] == "completed"
            assert receipt["response_text"] == (
                "working directory: /workspace/projects alpha"
            )
            assert receipt["response_agent_id"] == "codex"
            assert receipt["outcome"]["type"] == "working_directory"
            assert receipt["outcome"]["version"] == 2
            assert receipt["outcome"]["absolute_path"] == (
                "/workspace/projects alpha"
            )
            assert "absolute_path" not in receipt["outcome"]["working_directory"]

            # The ordinary gateway completion path may observe an already
            # completed atomic receipt and must replay it exactly.
            assert await store.complete_command_receipt(
                "cd-alpha",
                response_text="working directory: /workspace/projects alpha",
                response_agent_id="codex",
            ) == receipt

            # A later preference change must not rewrite the old command's
            # answer. Replaying the completed mutation returns its outcome.
            await store.set_session_working_directory(
                "projects/beta",
                absolute_path="/workspace/projects/beta",
                directory_device=21,
                directory_inode=202,
                updated_by="user",
                **SCOPE,
            )
            replay = await store.set_session_working_directory(
                "projects alpha",
                absolute_path="/workspace/projects alpha",
                directory_device=21,
                directory_inode=201,
                updated_by="user",
                command_id="cd-alpha",
                **SCOPE,
            )
            assert replay["relative_path"] == "projects alpha"
            assert replay["command_receipt"] == receipt
            assert (
                await store.get_session_working_directory(**SCOPE)
            )["relative_path"] == "projects/beta"

            with pytest.raises(StoreError, match="mutation conflicts"):
                await store.set_session_working_directory(
                    "projects/gamma",
                    absolute_path="/workspace/projects/gamma",
                    directory_device=21,
                    directory_inode=203,
                    updated_by="user",
                    command_id="cd-alpha",
                    **SCOPE,
                )

            corrupted_outcome = dict(receipt["outcome"])
            corrupted_snapshot = dict(corrupted_outcome["working_directory"])
            corrupted_snapshot.pop("updated_at")
            corrupted_outcome["working_directory"] = corrupted_snapshot
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE command_receipts SET outcome_json=? "
                    "WHERE command_id='cd-alpha'",
                    (json.dumps(corrupted_outcome, sort_keys=True),),
                )
                connection.commit()
            with pytest.raises(StoreError, match="completion conflicts"):
                await store.set_session_working_directory(
                    "projects alpha",
                    absolute_path="/workspace/projects alpha",
                    directory_device=21,
                    directory_inode=201,
                    updated_by="user",
                    command_id="cd-alpha",
                    **SCOPE,
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cd_atomic_receipt_uses_one_safe_bounded_response(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "safe-cd-response.sqlite"
        store = SQLiteStore(path)
        relative = "line\n\t`tick`/" + ("segment" * 90)
        absolute = f"/workspace/{relative}"
        command_text = f"/cd {shlex.quote(absolute)}"
        command_args = tuple(command_text.strip()[1:].split()[1:])
        expected = format_working_directory_response(absolute)
        try:
            await store.begin_command_receipt(
                "cd-safe-response",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="message-cd-safe-response",
                command_name="cd",
                command_args=command_args,
                command_text=command_text,
            )
            result = await store.set_session_working_directory(
                relative,
                absolute_path=absolute,
                directory_device=22,
                directory_inode=202,
                updated_by="user",
                command_id="cd-safe-response",
                **SCOPE,
            )
            receipt = result["command_receipt"]
            assert result["command_response"] == expected
            assert receipt["response_text"] == expected
            assert receipt["outcome"]["command_response"] == expected
            assert receipt["outcome"]["absolute_path"] == absolute
            assert len(expected) == WORKING_DIRECTORY_RESPONSE_MAX_CHARS
            assert expected.endswith("...")
            assert "line 'tick'" in expected
            assert "`" not in expected
            assert not any(character in expected for character in "\n\r\t\v\f")

            assert await store.complete_command_receipt(
                "cd-safe-response",
                response_text=expected,
                response_agent_id="codex",
            ) == receipt
            replay = await store.set_session_working_directory(
                relative,
                absolute_path=absolute,
                directory_device=22,
                directory_inode=202,
                updated_by="user",
                command_id="cd-safe-response",
                **SCOPE,
            )
            assert replay["command_response"] == expected
            assert replay["command_receipt"] == receipt
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cd_receipt_requires_one_path_and_binds_absolute_target(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "cd-receipt-binding.sqlite")

        async def reserve(command_id: str, command_text: str) -> None:
            await store.begin_command_receipt(
                command_id,
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id=f"message-{command_id}",
                command_name="cd",
                command_args=tuple(command_text.strip()[1:].split()[1:]),
                command_text=command_text,
            )

        try:
            for command_id, command_text in (
                ("cd-too-many", "/cd /workspace/alpha extra"),
                ("cd-unclosed", '/cd "/workspace/alpha'),
            ):
                await reserve(command_id, command_text)
                with pytest.raises(StoreError, match="identity conflicts"):
                    await store.set_session_working_directory(
                        "alpha",
                        absolute_path="/workspace/alpha",
                        directory_device=23,
                        directory_inode=203,
                        updated_by="user",
                        command_id=command_id,
                        **SCOPE,
                    )

            await reserve("cd-wrong-target", "/cd /workspace/alpha")
            with pytest.raises(StoreError, match="mutation conflicts"):
                await store.set_session_working_directory(
                    "beta",
                    absolute_path="/workspace/beta",
                    directory_device=23,
                    directory_inode=204,
                    updated_by="user",
                    command_id="cd-wrong-target",
                    **SCOPE,
                )
            assert await store.get_session_working_directory(**SCOPE) is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_cd_preference_rolls_back_when_receipt_completion_fails(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "rollback-cd.sqlite"
        store = SQLiteStore(path)
        try:
            await store.begin_command_receipt(
                "cd-rollback",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="message-cd-rollback",
                command_name="cd",
                command_args=("alpha",),
                command_text="/cd alpha",
            )
            with sqlite3.connect(path) as connection:
                connection.execute(
                    """CREATE TRIGGER fail_cd_receipt_completion
                       BEFORE UPDATE OF state ON command_receipts
                       WHEN OLD.command_id='cd-rollback'
                            AND NEW.state='completed'
                       BEGIN
                           SELECT RAISE(ABORT, 'forced cd receipt failure');
                       END"""
                )
                connection.commit()

            with pytest.raises(
                sqlite3.DatabaseError, match="forced cd receipt failure"
            ):
                await store.set_session_working_directory(
                    "alpha",
                    absolute_path="/workspace/alpha",
                    directory_device=31,
                    directory_inode=301,
                    updated_by="user",
                    command_id="cd-rollback",
                    **SCOPE,
                )
            assert await store.get_session_working_directory(**SCOPE) is None
            assert (await store.get_command_receipt("cd-rollback"))["state"] == (
                "started"
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_retiring_agent_deletes_only_its_working_directory_preferences(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "retire-cwd.sqlite")
        planner_scope = {**SCOPE, "agent_id": "planner"}
        try:
            await store.put_profile(AgentProfile(agent_id="planner"))
            await store.set_session_working_directory(
                "planner",
                absolute_path="/workspace/planner",
                directory_device=41,
                directory_inode=401,
                updated_by="user",
                **planner_scope,
            )
            await store.set_session_working_directory(
                "codex",
                absolute_path="/workspace/codex",
                directory_device=41,
                directory_inode=402,
                updated_by="user",
                **SCOPE,
            )

            assert await store.retire_agent("planner") == 1
            assert (
                await store.get_session_working_directory(**planner_scope)
                is None
            )
            assert (
                await store.get_session_working_directory(**SCOPE)
            )["relative_path"] == "codex"
        finally:
            await store.close()

    asyncio.run(scenario())
