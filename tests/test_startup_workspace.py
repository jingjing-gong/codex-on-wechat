"""Durable startup workspace resolution regressions."""

from __future__ import annotations

from pathlib import Path

import src.codex_wechat_bot as bot


def test_default_durable_workspace_uses_codex_default(monkeypatch, tmp_path):
    expected = tmp_path / "codex-workspace"
    monkeypatch.delenv("CODEX_WECHAT_WORKSPACE", raising=False)
    monkeypatch.setattr(bot, "default_workspace", lambda: str(expected))

    assert bot._durable_workspace() == expected
    assert expected.is_dir()


def test_configured_durable_workspace_is_expanded_and_created(
    monkeypatch, tmp_path
):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CODEX_WECHAT_WORKSPACE", "~/shared-workspace")

    expected = (home / "shared-workspace").resolve()
    assert bot._durable_workspace() == expected
    assert expected.is_dir()
