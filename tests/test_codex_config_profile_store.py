"""Durability and validation regressions for per-Agent Codex config profiles."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from src.runtime.policy import AgentProfile
from src.runtime.sqlite_store import SQLiteStore, StoreError


def _remove_v35_profile_column(path: Path, *, retain_marker: bool) -> None:
    """Turn a freshly-created database back into the additive v34 boundary."""

    with sqlite3.connect(path) as connection:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(agent_profiles)")
        }
        assert "codex_config_profile" in columns
        connection.execute(
            "ALTER TABLE agent_profiles DROP COLUMN codex_config_profile"
        )
        if not retain_marker:
            connection.execute("DELETE FROM schema_migrations WHERE version>=35")
        connection.commit()


def test_agent_profile_normalizes_and_serializes_codex_config_profile() -> None:
    profile = AgentProfile(
        agent_id="qwen-agent",
        profile_version=3,
        codex_config_profile="  qwen_3-8  ",
    )

    assert profile.codex_config_profile == "qwen_3-8"
    assert profile.as_dict()["codex_config_profile"] == "qwen_3-8"
    assert AgentProfile(**profile.as_dict()) == profile
    # The provider/config selection is private runtime metadata, not part of
    # the descriptor shared with users or peer Agents.
    assert not hasattr(profile.descriptor(), "codex_config_profile")


@pytest.mark.parametrize(
    "value",
    (
        ".",
        "..",
        "../admin",
        "nested/profile",
        r"nested\profile",
        "--profile",
        "white space",
        "line\nbreak",
        "nul\x00byte",
    ),
)
def test_agent_profile_rejects_unsafe_codex_config_profile_names(value: str) -> None:
    with pytest.raises(ValueError, match=r"(?i)codex config profile"):
        AgentProfile(agent_id="safe-agent", codex_config_profile=value)


def test_sqlite_codex_config_profile_round_trip_and_immutable_conflict(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path)
        await first.initialize()
        try:
            version_3 = AgentProfile(
                agent_id="qwen-agent",
                display_name="Qwen Agent",
                profile_version=3,
                codex_config_profile="qwen_3-8",
            )
            assert await first.put_profile(version_3)
            assert await first.put_profile(version_3)
            assert await first.get_profile("qwen-agent", 3) == version_3

            with pytest.raises(
                StoreError,
                match="profile version metadata conflicts: qwen-agent@3",
            ):
                await first.put_profile(
                    replace(version_3, codex_config_profile="qwen-canary")
                )

            version_4 = replace(
                version_3,
                profile_version=4,
                codex_config_profile="qwen-canary",
            )
            assert await first.put_profile(version_4)
            assert [
                (profile.profile_version, profile.codex_config_profile)
                for profile in await first.list_profiles(agent_id="qwen-agent")
            ] == [(3, "qwen_3-8"), (4, "qwen-canary")]
        finally:
            await first.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            restored_3 = await reopened.get_profile("qwen-agent", 3)
            restored_4 = await reopened.get_profile("qwen-agent", 4)
            assert restored_3 is not None
            assert restored_4 is not None
            assert restored_3.codex_config_profile == "qwen_3-8"
            assert restored_4.codex_config_profile == "qwen-canary"
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_sqlite_mapping_input_cannot_bypass_profile_name_validation(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            with pytest.raises(ValueError, match=r"(?i)codex config profile"):
                await store.put_profile(
                    {
                        "agent_id": "unsafe-agent",
                        "profile_version": 1,
                        "codex_config_profile": "../admin",
                    }
                )
            assert await store.get_profile("unsafe-agent", 1) is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_schema_v35_migrates_populated_v34_profiles_to_default_config(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "v34-to-v35.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        try:
            # Empty is the compatibility value: pre-v35 Agents used the base
            # Codex configuration and must not acquire a same-named profile.
            await seeded.put_profile(
                AgentProfile(
                    agent_id="legacy-agent",
                    profile_version=2,
                    codex_config_profile="",
                )
            )
        finally:
            await seeded.close()

        _remove_v35_profile_column(path, retain_marker=False)

        migrated = SQLiteStore(path)
        await migrated.initialize()
        try:
            builtin = await migrated.get_profile("codex", 1)
            legacy = await migrated.get_profile("legacy-agent", 2)
            assert builtin is not None and builtin.codex_config_profile == ""
            assert legacy is not None and legacy.codex_config_profile == ""
        finally:
            await migrated.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall() == [(version,) for version in range(1, 42)]
            column = next(
                row
                for row in connection.execute("PRAGMA table_info(agent_profiles)")
                if row[1] == "codex_config_profile"
            )
            assert str(column[2]).upper() == "TEXT"
            assert int(column[3]) == 1
            assert str(column[4]) == "''"
            assert connection.execute(
                "SELECT agent_id, codex_config_profile FROM agent_profiles "
                "WHERE agent_id IN ('codex','legacy-agent') ORDER BY agent_id"
            ).fetchall() == [("codex", ""), ("legacy-agent", "")]
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    asyncio.run(scenario())


def test_schema_v35_marker_without_profile_column_fails_closed(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "missing-v35-column.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        await seeded.close()
        with sqlite3.connect(path) as connection:
            # Retain the deliberately corrupt v35 marker while removing the
            # later additive migration that would otherwise fail first.
            connection.execute("DELETE FROM schema_migrations WHERE version>35")
            connection.commit()
        _remove_v35_profile_column(path, retain_marker=True)

        reopened = SQLiteStore(path)
        try:
            with pytest.raises(StoreError, match="schema v35.*profile"):
                await reopened.initialize()
        finally:
            await reopened.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (35,)
            assert "codex_config_profile" not in {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(agent_profiles)")
            }

    asyncio.run(scenario())


def test_legacy_codex_seed_upgrade_does_not_erase_named_config_profile(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "legacy-seed-conflict.sqlite"
        seeded = SQLiteStore(path)
        await seeded.initialize()
        try:
            changed = await seeded._call(
                lambda connection: connection.execute(
                    """UPDATE agent_profiles
                       SET display_name='Codex', summary='', system_prompt='',
                           responsibilities_json='[]', constraints_json='[]',
                           capabilities_json='[]', allowed_peers_json='[]',
                           denied_peers_json='[]', allowed_request_types_json='[]',
                           denied_request_types_json='[]', max_child_depth=0,
                           max_children_per_task=0, enabled=1,
                           default_mode_id='chat',
                           codex_config_profile='named-safe'
                       WHERE agent_id='codex' AND profile_version=1"""
                ).rowcount
            )
            assert changed == 1
        finally:
            await seeded.close()

        reopened = SQLiteStore(path)
        try:
            with pytest.raises(
                StoreError,
                match="profile version metadata conflicts: codex@1",
            ):
                await reopened.initialize()
        finally:
            await reopened.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT codex_config_profile FROM agent_profiles "
                "WHERE agent_id='codex' AND profile_version=1"
            ).fetchone() == ("named-safe",)

    asyncio.run(scenario())
