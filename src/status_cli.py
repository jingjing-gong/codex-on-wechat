"""Read-only aggregate health for every registered channel bot."""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import stat
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from wechat_ilink import accounts_dir, normalize_account_id  # noqa: E402
from wechat_ilink.client import LONG_POLL_TIMEOUT  # noqa: E402
from wechat_ilink.types import Credentials  # noqa: E402


DEFAULT_LONG_POLL_STALE_SECONDS = max(120.0, (LONG_POLL_TIMEOUT + 5.0) * 3.0)


class RuntimeStatusError(RuntimeError):
    """A safe, operator-facing aggregate status failure."""


@dataclass(frozen=True, slots=True)
class BotStatus:
    channel: str
    bot: str
    ready: bool
    detail: str

    @property
    def state(self) -> str:
        return "READY" if self.ready else "DOWN"


def durable_database_path() -> Path:
    return Path(
        os.environ.get(
            "CODEX_WECHAT_DB",
            str(Path.home() / ".codex-wechat-bot" / "runtime.sqlite3"),
        )
    ).expanduser().resolve()


def _timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _display_timestamp(value: Any) -> str:
    parsed = _timestamp(value)
    return parsed.isoformat() if parsed is not None else "never"


class RuntimeStatusAdmin:
    """Build the monitoring view without taking runtime ownership."""

    def __init__(
        self,
        *,
        database: Path | None = None,
        account_root: Path | None = None,
        stale_seconds: float | None = None,
        output: TextIO | None = None,
        now: datetime | None = None,
    ) -> None:
        self.database = (database or durable_database_path()).expanduser().resolve()
        self.account_root = (account_root or accounts_dir()).expanduser().resolve()
        configured_stale = (
            os.environ.get("CODEX_WECHAT_STATUS_STALE_SECONDS", "").strip()
            if stale_seconds is None
            else str(stale_seconds)
        )
        try:
            self.stale_seconds = (
                float(configured_stale)
                if configured_stale
                else DEFAULT_LONG_POLL_STALE_SECONDS
            )
        except ValueError as exc:
            raise RuntimeStatusError(
                "CODEX_WECHAT_STATUS_STALE_SECONDS must be a positive number"
            ) from exc
        if not math.isfinite(self.stale_seconds) or self.stale_seconds <= 0:
            raise RuntimeStatusError(
                "CODEX_WECHAT_STATUS_STALE_SECONDS must be a positive number"
            )
        current = now or datetime.now(timezone.utc)
        self.now = (
            current.replace(tzinfo=timezone.utc)
            if current.tzinfo is None
            else current.astimezone(timezone.utc)
        )
        self.output = output or sys.stdout

    def _read_database(
        self,
    ) -> tuple[list[sqlite3.Row], dict[tuple[str, str], sqlite3.Row]]:
        if not self.database.exists():
            return [], {}
        if not self.database.is_file():
            raise RuntimeStatusError("runtime database is not a regular file")
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                f"{self.database.as_uri()}?mode=ro",
                uri=True,
                timeout=1.0,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required = {
                "supervisor_epochs",
                "bot_profiles",
                "bot_profile_status",
            }
            missing = sorted(required - tables)
            if missing:
                raise RuntimeStatusError(
                    "runtime database is missing bot status schema: "
                    + ", ".join(missing)
                )
            profiles = connection.execute(
                """SELECT p.profile_id, p.bot_id, p.enabled,
                          s.connection_state, s.generation, s.last_ready_at,
                          s.last_error_code
                     FROM bot_profiles AS p
                     LEFT JOIN bot_profile_status AS s
                       ON s.profile_id=p.profile_id
                    WHERE p.channel='lark' AND p.removed_at IS NULL
                    ORDER BY p.profile_id"""
            ).fetchall()
            epochs = {
                (str(row["channel"]), str(row["bot_id"])): row
                for row in connection.execute(
                    """SELECT epoch, owner_instance_id, channel, bot_id, started_at
                         FROM supervisor_epochs
                        WHERE stopped_at IS NULL
                        ORDER BY epoch"""
                ).fetchall()
            }
            return profiles, epochs
        except RuntimeStatusError:
            raise
        except sqlite3.Error as exc:
            raise RuntimeStatusError("could not read runtime bot status") from exc
        finally:
            if connection is not None:
                connection.close()

    def _wechat_credentials(self) -> list[Credentials]:
        root = self.account_root
        if not root.exists():
            return []
        if not root.is_dir():
            raise RuntimeStatusError("WeChat account store is not a directory")
        credentials: dict[str, Credentials] = {}
        try:
            entries = sorted(root.iterdir())
        except OSError as exc:
            raise RuntimeStatusError("could not read WeChat account store") from exc
        for entry in entries:
            try:
                details = entry.lstat()
            except OSError:
                continue
            if entry.suffix != ".json" or not stat.S_ISREG(details.st_mode):
                continue
            try:
                value = json.loads(entry.read_text(encoding="utf-8"))
                credential = Credentials.model_validate(value)
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
                continue
            bot_id = str(credential.ilink_bot_id or "").strip()
            if bot_id and str(credential.bot_token or "").strip():
                credentials[bot_id] = credential
        return [credentials[key] for key in sorted(credentials)]

    def _long_poll_activity(self, bot_id: str) -> datetime | None:
        path = self.account_root / f"{normalize_account_id(bot_id)}.sync.json"
        try:
            details = path.lstat()
            if not stat.S_ISREG(details.st_mode):
                return None
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or not isinstance(
                value.get("get_updates_buf"), str
            ):
                return None
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        return datetime.fromtimestamp(details.st_mtime, tz=timezone.utc)

    def rows(self) -> list[BotStatus]:
        profiles, active_epochs = self._read_database()
        rows: list[BotStatus] = []
        for profile in profiles:
            enabled = bool(profile["enabled"])
            connection = str(profile["connection_state"] or "not_started")
            generation = int(profile["generation"] or 0)
            ready = enabled and connection == "ready"
            rows.append(
                BotStatus(
                    channel="lark",
                    bot=str(profile["profile_id"]),
                    ready=ready,
                    detail=(
                        f"profile={profile['profile_id']} app_id={profile['bot_id']} "
                        f"enabled={'yes' if enabled else 'no'} "
                        f"connection={connection} generation={generation} "
                        f"last_ready_at={_display_timestamp(profile['last_ready_at'])} "
                        f"last_error={profile['last_error_code'] or 'none'}"
                    ),
                )
            )

        for credential in self._wechat_credentials():
            bot_id = str(credential.ilink_bot_id)
            epoch = active_epochs.get(("wechat", bot_id))
            epoch_started = (
                _timestamp(epoch["started_at"]) if epoch is not None else None
            )
            activity = self._long_poll_activity(bot_id)
            activity_age = (
                max(0.0, (self.now - activity).total_seconds())
                if activity is not None
                else None
            )
            long_poll_active = bool(
                epoch_started is not None
                and activity is not None
                and activity >= epoch_started
                and activity_age is not None
                and activity_age <= self.stale_seconds
            )
            rows.append(
                BotStatus(
                    channel="wechat",
                    bot=bot_id,
                    ready=epoch is not None and long_poll_active,
                    detail=(
                        "credentials=active "
                        f"supervisor_epoch={epoch['epoch'] if epoch is not None else 'none'} "
                        f"long_poll={'active' if long_poll_active else 'inactive'} "
                        f"last_activity_at={activity.isoformat() if activity else 'never'}"
                    ),
                )
            )
        return rows

    def status(self) -> bool:
        rows = self.rows()
        channel_width = max([len("CHANNEL"), *(len(row.channel) for row in rows)])
        bot_width = max([len("BOT"), *(len(row.bot) for row in rows)])
        state_width = max([len("STATE"), *(len(row.state) for row in rows)])
        self.output.write(
            f"{'CHANNEL':<{channel_width}}  {'BOT':<{bot_width}}  "
            f"{'STATE':<{state_width}}  DETAIL\n"
        )
        for row in rows:
            self.output.write(
                f"{row.channel:<{channel_width}}  {row.bot:<{bot_width}}  "
                f"{row.state:<{state_width}}  {row.detail}\n"
            )
        if not rows:
            self.output.write("no registered bots\n")
            return False
        return all(row.ready for row in rows)


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="./cow status",
        description="Show readiness for all registered Lark and WeChat bots.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    build_parser().parse_args(argv)
    try:
        return 0 if RuntimeStatusAdmin().status() else 1
    except RuntimeStatusError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover - exercised through launcher tests
    raise SystemExit(main())


__all__ = [
    "BotStatus",
    "RuntimeStatusAdmin",
    "RuntimeStatusError",
    "build_parser",
    "durable_database_path",
    "main",
]
