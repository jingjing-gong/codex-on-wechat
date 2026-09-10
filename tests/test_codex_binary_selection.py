"""External Codex executable selection at runtime startup."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

pytest.importorskip("openai_codex")

from src.agents import codex_runtime


def _executable(directory: Path, name: str = "codex") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    executable = directory / name
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    return executable


@pytest.mark.parametrize("override", [None, "", "  "])
def test_default_uses_codex_on_path(monkeypatch, tmp_path, override):
    executable = _executable(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    if override is None:
        monkeypatch.delenv("CODEX_WECHAT_CODEX_BIN", raising=False)
    else:
        monkeypatch.setenv("CODEX_WECHAT_CODEX_BIN", override)
    configs = []
    client = object()

    def create_client(*, config):
        configs.append(config)
        return client

    monkeypatch.setattr(codex_runtime, "AsyncCodex", create_client)

    async def scenario():
        runtime = codex_runtime.CodexRuntime(cwd=str(tmp_path))
        await runtime.start()
        assert runtime._codex is client
        assert configs[0].codex_bin == str(executable)
        await runtime.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("override_kind", ["absolute", "home", "command", "relative"])
def test_override_selects_external_executable(monkeypatch, tmp_path, override_kind):
    _executable(tmp_path)
    selected = _executable(tmp_path / "custom bin", "selected-codex")
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    overrides = {
        "absolute": str(selected),
        "home": "~/custom bin/selected-codex",
        "command": "selected-codex",
        "relative": "./custom bin/selected-codex",
    }
    if override_kind == "command":
        monkeypatch.setenv("PATH", str(selected.parent))
    monkeypatch.setenv("CODEX_WECHAT_CODEX_BIN", overrides[override_kind])
    monkeypatch.setattr(codex_runtime, "AsyncCodex", lambda *, config: config)

    config = codex_runtime._create_codex_client()

    assert config.codex_bin == str(selected)


@pytest.mark.parametrize("override_kind", ["unset", "missing", "not_executable"])
def test_missing_executable_never_falls_back(monkeypatch, tmp_path, override_kind):
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.delenv("CODEX_WECHAT_CODEX_BIN", raising=False)
    if override_kind != "unset":
        _executable(tmp_path)
        selected = tmp_path / "selected-codex"
        if override_kind == "not_executable":
            selected.write_text("not executable")
        monkeypatch.setenv("CODEX_WECHAT_CODEX_BIN", str(selected))

    def unexpected_client(**kwargs):
        pytest.fail("SDK must not be created without an external executable")

    monkeypatch.setattr(codex_runtime, "AsyncCodex", unexpected_client)

    async def scenario():
        runtime = codex_runtime.CodexRuntime(cwd=str(tmp_path))
        with pytest.raises(FileNotFoundError, match="CODEX_WECHAT_CODEX_BIN"):
            await runtime.start()

    asyncio.run(scenario())


@pytest.mark.parametrize("injection", ["client", "factory"])
def test_injected_client_does_not_require_an_executable(monkeypatch, tmp_path, injection):
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("CODEX_WECHAT_CODEX_BIN", str(tmp_path / "missing"))
    client = object()
    kwargs = {"codex": client} if injection == "client" else {"codex_factory": lambda: client}

    async def scenario():
        runtime = codex_runtime.CodexRuntime(cwd=str(tmp_path), **kwargs)
        await runtime.start()
        assert runtime._codex is client
        await runtime.stop()

    asyncio.run(scenario())
