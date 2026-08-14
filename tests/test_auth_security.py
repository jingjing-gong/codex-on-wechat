"""Authentication identity and credential-storage regressions."""

from __future__ import annotations

import json
import stat

import pytest

from wechat_ilink import auth
from wechat_ilink.types import Credentials


class _Response:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


def test_poll_qr_status_quotes_opaque_token(monkeypatch):
    requested: list[str] = []

    def fake_get(url: str, **_kwargs):
        requested.append(url)
        return _Response({"status": auth.STATUS_EXPIRED})

    monkeypatch.setattr(auth.requests, "get", fake_get)

    with pytest.raises(RuntimeError, match="expired"):
        auth.poll_qr_status("token+/=&part")

    assert requested == [auth.QRSTATUS_URL + "token%2B%2F%3D%26part"]


def test_confirmed_qr_requires_token_and_bot_identity(monkeypatch):
    monkeypatch.setattr(
        auth.requests,
        "get",
        lambda *_args, **_kwargs: _Response(
            {"status": auth.STATUS_CONFIRMED, "bot_token": "token"}
        ),
    )

    with pytest.raises(RuntimeError, match="incomplete credentials"):
        auth.poll_qr_status("qr-token")


def test_poll_qr_status_only_reports_state_changes(monkeypatch):
    responses = iter(
        [
            _Response({"status": auth.STATUS_WAIT}),
            _Response({"status": auth.STATUS_WAIT}),
            _Response({"status": auth.STATUS_SCANNED}),
            _Response({"status": auth.STATUS_EXPIRED}),
        ]
    )
    monkeypatch.setattr(auth.requests, "get", lambda *_args, **_kwargs: next(responses))
    statuses: list[str] = []

    with pytest.raises(RuntimeError, match="expired"):
        auth.poll_qr_status("qr-token", on_status=statuses.append)

    assert statuses == [auth.STATUS_WAIT, auth.STATUS_SCANNED, auth.STATUS_EXPIRED]


def test_poll_qr_status_does_not_retry_permanent_http_errors(monkeypatch):
    calls = 0

    class FailedResponse:
        closed = False

        def raise_for_status(self):
            raise auth.requests.exceptions.HTTPError("bad request")

        def close(self):
            self.closed = True

    response = FailedResponse()

    def fake_get(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return response

    monkeypatch.setattr(auth.requests, "get", fake_get)

    with pytest.raises(auth.requests.exceptions.HTTPError, match="bad request"):
        auth.poll_qr_status("qr-token")

    assert calls == 1
    assert response.closed is True


def test_poll_qr_status_backs_off_connection_failures(monkeypatch):
    attempts = 0
    sleeps: list[float] = []

    def fake_get(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise auth.requests.exceptions.ConnectionError("offline")
        return _Response({"status": auth.STATUS_EXPIRED})

    monkeypatch.setattr(auth.requests, "get", fake_get)
    monkeypatch.setattr(auth.time, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="expired"):
        auth.poll_qr_status("qr-token")

    assert sleeps == [0.5, 1.0, 2.0, 4.0]


def test_poll_qr_backoff_honors_cancellation_before_sleep(monkeypatch):
    monkeypatch.setattr(
        auth.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            auth.requests.exceptions.ConnectionError("offline")
        ),
    )
    sleeps: list[float] = []
    monkeypatch.setattr(auth.time, "sleep", sleeps.append)
    checks = iter((False, True))

    with pytest.raises(RuntimeError, match="cancelled"):
        auth.poll_qr_status("qr-token", stop_check=lambda: next(checks))

    assert sleeps == []


def test_credential_store_rejects_and_skips_identityless_tokens(
    monkeypatch, tmp_path
):
    directory = tmp_path / "accounts"
    monkeypatch.setattr(auth, "accounts_dir", lambda: directory)

    with pytest.raises(ValueError, match="bot token and bot identity"):
        auth.save_credentials(Credentials(bot_token="secret"))
    assert not directory.exists()

    directory.mkdir()
    (directory / "invalid.json").write_text(
        json.dumps({"bot_token": "secret", "ilink_bot_id": ""}),
        encoding="utf-8",
    )
    (directory / "valid.json").write_text(
        json.dumps({"bot_token": "secret-2", "ilink_bot_id": "bot-2"}),
        encoding="utf-8",
    )

    loaded = auth.load_all_credentials()
    assert [(item.ilink_bot_id, item.bot_token) for item in loaded] == [
        ("bot-2", "secret-2")
    ]


def test_save_tightens_account_directory_permissions(monkeypatch, tmp_path):
    directory = tmp_path / "accounts"
    directory.mkdir(mode=0o755)
    monkeypatch.setattr(auth, "accounts_dir", lambda: directory)

    auth.save_credentials(Credentials(bot_token="secret", ilink_bot_id="bot"))

    assert stat.S_IMODE(directory.stat().st_mode) == 0o700


def test_logout_ignores_json_directories_and_removes_credentials(monkeypatch, tmp_path):
    directory = tmp_path / "accounts"
    directory.mkdir()
    monkeypatch.setattr(auth, "accounts_dir", lambda: directory)
    (directory / "not-a-credential.json").mkdir()
    credential = directory / "bot.json"
    credential.write_text("{}", encoding="utf-8")

    assert auth.delete_all_credentials() == 1
    assert not credential.exists()
    assert (directory / "not-a-credential.json").is_dir()
