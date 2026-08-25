"""Process entrypoint diagnostic regressions."""

from __future__ import annotations

import logging

import pytest

import src.codex_wechat_bot as bot


def test_main_logs_system_exit_before_reraising(monkeypatch, caplog) -> None:
    def exit_runtime() -> None:
        raise SystemExit(17)

    monkeypatch.setattr(bot, "_durable_main", exit_runtime)

    with caplog.at_level(logging.ERROR, logger="codex_wechat_bot"):
        with pytest.raises(SystemExit) as raised:
            bot.main()

    assert raised.value.code == 17
    assert "fatal durable runtime failure: type=SystemExit code=17" in caplog.text
    assert "SystemExit: 17" in caplog.text


def test_main_logs_uncaught_exception_before_reraising(monkeypatch, caplog) -> None:
    def fail_runtime() -> None:
        raise RuntimeError("entrypoint exploded")

    monkeypatch.setattr(bot, "_durable_main", fail_runtime)

    with caplog.at_level(logging.ERROR, logger="codex_wechat_bot"):
        with pytest.raises(RuntimeError, match="entrypoint exploded"):
            bot.main()

    assert "fatal durable runtime failure: type=RuntimeError code=None" in caplog.text
    assert "RuntimeError: entrypoint exploded" in caplog.text
