"""QR-code login flow and credential storage for codex-wechat-bot."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import quote

import requests

from .types import Credentials, QRCodeResponse, QRStatusResponse

QRCODE_URL = "https://ilinkai.weixin.qq.com/ilink/bot/get_bot_qrcode?bot_type=3"
QRSTATUS_URL = "https://ilinkai.weixin.qq.com/ilink/bot/get_qrcode_status?qrcode="

STATUS_WAIT = "wait"
STATUS_SCANNED = "scaned"  # matches the upstream API's spelling
STATUS_CONFIRMED = "confirmed"
STATUS_EXPIRED = "expired"
QR_RETRY_INITIAL_SECONDS = 0.5
QR_RETRY_MAX_SECONDS = 5.0


def _ensure_api_success(operation: str, response: object) -> None:
    ret = int(getattr(response, "ret", 0) or 0)
    errcode = int(getattr(response, "errcode", 0) or 0)
    if ret != 0 or errcode != 0:
        errmsg = str(getattr(response, "errmsg", "") or "")
        raise RuntimeError(
            f"{operation} failed: ret={ret} errcode={errcode} errmsg={errmsg}"
        )


def fetch_qrcode() -> QRCodeResponse:
    """Fetch a new QR code to start the login flow."""
    resp = requests.get(QRCODE_URL, timeout=40)
    try:
        resp.raise_for_status()
        response = QRCodeResponse.model_validate(resp.json())
    finally:
        close = getattr(resp, "close", None)
        if callable(close):
            close()
    _ensure_api_success("get_bot_qrcode", response)
    return response


def poll_qr_status(
    qrcode: str,
    on_status: Callable[[str], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> Credentials:
    """Poll for QR scan/confirmation until login succeeds or the code expires.

    `on_status` is called on each status change ("wait", "scaned", "confirmed",
    "expired") so the caller can display progress. `stop_check`, if provided,
    is polled between requests to allow cooperative cancellation.
    """
    token = str(qrcode or "")
    if not token:
        raise ValueError("qrcode must not be empty")
    # QR tokens are opaque and may contain base64/query delimiter characters.
    # Concatenating them verbatim can change the server-side token value.
    url = QRSTATUS_URL + quote(token, safe="")
    previous_status: str | None = None
    retry_delay = QR_RETRY_INITIAL_SECONDS

    while True:
        if stop_check is not None and stop_check():
            raise RuntimeError("polling cancelled")

        try:
            resp = requests.get(url, timeout=40)
            try:
                resp.raise_for_status()
                data = QRStatusResponse.model_validate(resp.json())
            finally:
                close = getattr(resp, "close", None)
                if callable(close):
                    close()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            # Timeouts are normal for long-polling, while DNS/connection
            # failures can return immediately.  Bound their retry rate so an
            # outage cannot hot-loop a CPU core or the resolver.
            if stop_check is not None and stop_check():
                raise RuntimeError("polling cancelled")
            time.sleep(retry_delay)
            retry_delay = min(QR_RETRY_MAX_SECONDS, retry_delay * 2)
            continue

        _ensure_api_success("get_qrcode_status", data)
        retry_delay = QR_RETRY_INITIAL_SECONDS

        if on_status is not None and data.status != previous_status:
            on_status(data.status)
        previous_status = data.status

        if data.status == STATUS_CONFIRMED:
            credentials = Credentials(
                bot_token=data.bot_token,
                ilink_bot_id=data.ilink_bot_id,
                baseurl=data.baseurl,
                ilink_user_id=data.ilink_user_id,
            )
            if not credentials.bot_token.strip() or not credentials.ilink_bot_id.strip():
                raise RuntimeError("QR confirmation returned incomplete credentials")
            return credentials
        if data.status == STATUS_EXPIRED:
            raise RuntimeError("QR code expired")
        # STATUS_WAIT / STATUS_SCANNED / unknown -> keep polling


def accounts_dir() -> Path:
    """Directory where account credentials are stored."""
    return Path.home() / ".codex-wechat-bot" / "accounts"


_ACCOUNT_STEM_LIMIT = 96


def _legacy_account_stem(raw: str) -> str:
    """Return the sanitized stem used by older account files.

    This helper is intentionally private.  New storage must use
    :func:`normalize_account_id`; the legacy spelling exists only so saved
    credentials and sync cursors can be discovered during migration.
    """

    value = str(raw or "")
    # ASCII names are intentional here: account IDs are identifiers, not
    # display text, and restricting the alphabet prevents platform-specific
    # path surprises (including both slash variants on Windows).
    normalized = re.sub(r"[^A-Za-z0-9_-]", "-", value)
    if not normalized:
        normalized = "account"
    return normalized[:_ACCOUNT_STEM_LIMIT]


def normalize_account_id(raw: str) -> str:
    """Convert a raw bot ID into a collision-resistant path component.

    Already-safe IDs retain their historical spelling.  Any normalization or
    truncation adds a digest of the complete raw value, preventing distinct
    IDs such as ``bot:a`` and ``bot-a`` from sharing account state.
    """

    value = str(raw or "")
    stem = _legacy_account_stem(value)
    if value == stem:
        return stem
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    prefix_limit = _ACCOUNT_STEM_LIMIT - len(digest) - 1
    prefix = stem[:prefix_limit] or "account"
    return f"{prefix}-{digest}"


_ACCOUNT_WRITE_LOCK = threading.RLock()


def _existing_account_owner(path: Path) -> str | None:
    """Read the bot identity from an existing credential file, if valid."""

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(data, dict):
        return None
    owner = data.get("ilink_bot_id")
    return str(owner) if owner is not None else None


def _credential_path(raw_bot_id: str, directory: Path) -> Path:
    """Choose a collision-resistant path while preserving legacy names.

    Older releases stored transformed IDs directly (for example ``bot:a`` at
    ``bot-a.json``).  If that path is absent, retain it for the first writer;
    when it is occupied by another raw ID, route the new account to a stable
    digest-suffixed name instead of overwriting credentials.
    """

    raw = str(raw_bot_id or "")
    normalized = normalize_account_id(raw)
    primary = directory / f"{normalized}.json"
    if primary.exists() and _existing_account_owner(primary) == raw:
        return primary

    # An existing pre-migration file is still authoritative for the same raw
    # account identity.  Never adopt it merely because its sanitized stem
    # matches; that is the collision this migration fixes.
    legacy = directory / f"{_legacy_account_stem(raw)}.json"
    if (
        legacy != primary
        and legacy.exists()
        and _existing_account_owner(legacy) == raw
    ):
        return legacy
    if not primary.exists():
        return primary

    # A safe raw ID can share its primary spelling with another ID's legacy
    # file (``bot-a`` versus old ``bot:a``).  Leave that file untouched and
    # use a stable fallback for the safe ID.
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    prefix_limit = _ACCOUNT_STEM_LIMIT - len(digest) - 1
    candidate = directory / f"{normalized[:prefix_limit]}-{digest}.json"
    if candidate.exists():
        candidate_owner = _existing_account_owner(candidate)
        if candidate_owner not in (None, raw):
            # A digest collision is extraordinarily unlikely, but refusing to
            # overwrite is the only safe behavior if a manually-created file
            # has claimed the path.
            raise RuntimeError(f"credential path collision: {candidate.name}")
    return candidate


def save_credentials(creds: Credentials) -> None:
    """Persist credentials to the local codex-wechat-bot account store."""
    if not creds.bot_token.strip() or not creds.ilink_bot_id.strip():
        raise ValueError("credentials require a bot token and bot identity")

    directory = accounts_dir()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    # ``mkdir(mode=...)`` does not tighten an existing directory.  Credential
    # files are private too, but the account directory also reveals bot IDs and
    # contains cursor state, so preserve the intended owner-only boundary.
    os.chmod(directory, 0o700)

    # Resolve the path and publish it atomically under a process-local lock so
    # two simultaneous QR-login completions cannot clobber one another.
    with _ACCOUNT_WRITE_LOCK:
        path = _credential_path(str(creds.ilink_bot_id or ""), directory)
        payload = json.dumps(
            creds.model_dump(), indent=2, ensure_ascii=False, sort_keys=True
        ).encode("utf-8")
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def load_all_credentials() -> list[Credentials]:
    """Load all saved account credentials."""
    directory = accounts_dir()
    if not directory.exists():
        return []

    result: list[Credentials] = []
    for entry in sorted(directory.iterdir()):
        if entry.suffix != ".json" or entry.is_symlink() or not entry.is_file():
            continue
        try:
            data = json.loads(entry.read_text(encoding="utf-8"))
            creds = Credentials.model_validate(data)
        except (json.JSONDecodeError, ValueError, OSError, UnicodeError):
            continue
        # A token without its bot identity cannot be scoped safely.  In
        # particular, loading it would make durable channel ownership use an
        # empty bot_id and collide with every other incomplete account.
        if creds.bot_token.strip() and creds.ilink_bot_id.strip():
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
    with _ACCOUNT_WRITE_LOCK:
        for entry in sorted(directory.iterdir()):
            if entry.suffix != ".json" or (entry.is_dir() and not entry.is_symlink()):
                continue
            try:
                entry.unlink()
            except FileNotFoundError:
                # A concurrent external cleanup already achieved the desired
                # result.  Keep logout best-effort across all account files.
                continue
            count += 1
    return count
