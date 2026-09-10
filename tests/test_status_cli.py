"""Read-only aggregate channel health reporting."""

from __future__ import annotations

import asyncio
import io
import json
import os
import sqlite3
from datetime import datetime, timezone

import src.status_cli as status_cli
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink import normalize_account_id


def _seed_database(path) -> None:
    async def seed() -> None:
        store = SQLiteStore(path)
        await store.initialize()
        try:
            await store.create_bot_profile(
                {
                    "profile_id": "team",
                    "channel": "lark",
                    "bot_id": "cli_team_bot",
                    "brand": "feishu",
                    "config_dir": "/private/lark/team",
                    "config_dir_identity": "device:team",
                    "cli_version": "1.0.92",
                    "credential_ref": "keychain:lark/team",
                }
            )
            await store.set_bot_profile_status(
                "team",
                connection_state="ready",
                generation=4,
                last_ready_at="2026-09-05T11:59:30+00:00",
            )
        finally:
            await store.close()

    asyncio.run(seed())
    connection = sqlite3.connect(path)
    connection.execute(
        """INSERT INTO supervisor_epochs
               (owner_instance_id, channel, bot_id, started_at)
           VALUES ('live-owner', 'wechat', 'wechat-bot',
                   '2026-09-05T11:59:00+00:00')"""
    )
    connection.commit()
    connection.close()


def _seed_wechat_account(root, *, activity_timestamp: float) -> None:
    root.mkdir()
    (root / "wechat-bot.json").write_text(
        json.dumps(
            {
                "bot_token": "test-token",
                "ilink_bot_id": "wechat-bot",
                "baseurl": "https://example.invalid",
                "ilink_user_id": "wechat-user",
            }
        ),
        encoding="utf-8",
    )
    sync = root / f"{normalize_account_id('wechat-bot')}.sync.json"
    sync.write_text(json.dumps({"get_updates_buf": "cursor"}), encoding="utf-8")
    os.utime(sync, (activity_timestamp, activity_timestamp))


def test_status_lists_ready_lark_and_wechat_bots_in_one_table(tmp_path) -> None:
    database = tmp_path / "runtime.sqlite3"
    accounts = tmp_path / "accounts"
    _seed_database(database)
    current = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
    _seed_wechat_account(accounts, activity_timestamp=current.timestamp())
    output = io.StringIO()

    ready = status_cli.RuntimeStatusAdmin(
        database=database,
        account_root=accounts,
        stale_seconds=120,
        now=current,
        output=output,
    ).status()

    rendered = output.getvalue()
    assert ready
    assert rendered.splitlines()[0].split() == [
        "CHANNEL",
        "BOT",
        "STATE",
        "DETAIL",
    ]
    assert "lark" in rendered and "team" in rendered and "READY" in rendered
    assert "profile=team" in rendered
    assert "app_id=cli_team_bot" in rendered
    assert "enabled=yes connection=ready generation=4" in rendered
    assert "last_ready_at=2026-09-05T11:59:30+00:00" in rendered
    assert "last_error=none" in rendered
    assert "wechat" in rendered and "wechat-bot" in rendered
    assert "credentials=active supervisor_epoch=1 long_poll=active" in rendered


def test_status_is_down_when_wechat_long_poll_activity_is_stale(tmp_path) -> None:
    database = tmp_path / "runtime.sqlite3"
    accounts = tmp_path / "accounts"
    _seed_database(database)
    current = datetime(2026, 9, 5, 12, 10, tzinfo=timezone.utc)
    _seed_wechat_account(
        accounts,
        activity_timestamp=datetime(
            2026, 9, 5, 12, 0, tzinfo=timezone.utc
        ).timestamp(),
    )
    output = io.StringIO()

    ready = status_cli.RuntimeStatusAdmin(
        database=database,
        account_root=accounts,
        stale_seconds=120,
        now=current,
        output=output,
    ).status()

    assert not ready
    assert "wechat-bot  DOWN" in output.getvalue()
    assert "long_poll=inactive" in output.getvalue()


def test_status_main_returns_nonzero_when_any_bot_is_down(monkeypatch) -> None:
    class DownStatus:
        def status(self) -> bool:
            return False

    monkeypatch.setattr(status_cli, "RuntimeStatusAdmin", DownStatus)

    assert status_cli.main([]) == 1
