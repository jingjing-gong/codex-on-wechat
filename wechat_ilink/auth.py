"""QR-code login flow and credential storage for codex-wechat-bot."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Callable, List, Optional

import requests

from .types import Credentials, QRCodeResponse, QRStatusResponse

QRCODE_URL = "https://ilinkai.weixin.qq.com/ilink/bot/get_bot_qrcode?bot_type=3"
QRSTATUS_URL = "https://ilinkai.weixin.qq.com/ilink/bot/get_qrcode_status?qrcode="

STATUS_WAIT = "wait"
STATUS_SCANNED = "scaned"  # matches the upstream API's spelling
STATUS_CONFIRMED = "confirmed"
STATUS_EXPIRED = "expired"


def fetch_qrcode() -> QRCodeResponse:
    """Fetch a new QR code to start the login flow."""
    resp = requests.get(QRCODE_URL, timeout=40)
    resp.raise_for_status()
    return QRCodeResponse.model_validate(resp.json())


def poll_qr_status(
    qrcode: str,
    on_status: Optional[Callable[[str], None]] = None,
    stop_check: Optional[Callable[[], bool]] = None,
) -> Credentials:
    """Poll for QR scan/confirmation until login succeeds or the code expires.

    `on_status` is called on each status change ("wait", "scaned", "confirmed",
    "expired") so the caller can display progress. `stop_check`, if provided,
    is polled between requests to allow cooperative cancellation.
    """
    url = QRSTATUS_URL + qrcode

    while True:
        if stop_check is not None and stop_check():
            raise RuntimeError("polling cancelled")

        try:
            resp = requests.get(url, timeout=40)
            resp.raise_for_status()
            data = QRStatusResponse.model_validate(resp.json())
        except requests.exceptions.RequestException:
            # Timeout is normal for long-poll, retry.
            continue

        if on_status is not None:
            on_status(data.status)

        if data.status == STATUS_CONFIRMED:
            return Credentials(
                bot_token=data.bot_token,
                ilink_bot_id=data.ilink_bot_id,
                baseurl=data.baseurl,
                ilink_user_id=data.ilink_user_id,
            )
        if data.status == STATUS_EXPIRED:
            raise RuntimeError("QR code expired")
        # STATUS_WAIT / STATUS_SCANNED / unknown -> keep polling


def accounts_dir() -> Path:
    """Directory where account credentials are stored."""
    return Path.home() / ".codex-wechat-bot" / "accounts"


def normalize_account_id(raw: str) -> str:
    """Convert a raw bot ID into a filesystem-safe name."""
    return re.sub(r"[@.:]", "-", raw)


def save_credentials(creds: Credentials) -> None:
    """Persist credentials to the local codex-wechat-bot account store."""
    directory = accounts_dir()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    account_id = normalize_account_id(creds.ilink_bot_id)
    path = directory / f"{account_id}.json"
    path.write_text(json.dumps(creds.model_dump(), indent=2, ensure_ascii=False))
    os.chmod(path, 0o600)


def load_all_credentials() -> List[Credentials]:
    """Load all saved account credentials."""
    directory = accounts_dir()
    if not directory.exists():
        return []

    result: List[Credentials] = []
    for entry in sorted(directory.iterdir()):
        if entry.suffix != ".json":
            continue
        try:
            data = json.loads(entry.read_text())
            creds = Credentials.model_validate(data)
        except (json.JSONDecodeError, ValueError):
            continue
        if creds.bot_token:
            result.append(creds)
    return result


def credentials_path() -> Path:
    """Return the accounts directory path, for display purposes."""
    return accounts_dir()


def delete_all_credentials() -> int:
    """Delete all saved account credential files (a local "logout": there is
    no server-side session-revocation endpoint in this unofficial protocol,
    so this just forces a fresh QR-code login on next run). Returns the
    number of files deleted."""
    directory = accounts_dir()
    if not directory.exists():
        return 0

    count = 0
    for entry in sorted(directory.iterdir()):
        if entry.suffix != ".json":
            continue
        entry.unlink()
        count += 1
    return count
