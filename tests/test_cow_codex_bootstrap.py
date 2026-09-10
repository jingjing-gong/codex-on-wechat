"""Hermetic launcher checks for standalone Codex installation."""

from __future__ import annotations

import shlex
import shutil
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INSTALL_URL = "https://chatgpt.com/codex/install.sh"


def _executable(path: Path, script: str = "exit 0\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + script, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture
def launcher(tmp_path):
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    for command in ("sh", "dirname", "mkdir", "chmod", "sed"):
        (binary_dir / command).symlink_to(shutil.which(command))
    log = tmp_path / "launch.log"
    _executable(
        binary_dir / "uv",
        'printf "uv:%s:%s\\n" "$*" "${CODEX_WECHAT_CODEX_BIN:-}" >> "$LAUNCH_LOG"\n',
    )
    installer = (
        'printf "install:%s\\n" "${CODEX_NON_INTERACTIVE:-}" >> "$LAUNCH_LOG"\n'
        'if [ "${FAIL_INSTALL:-}" = 1 ]; then exit 1; fi\n'
        'if [ "${SKIP_BINARY:-}" = 1 ]; then exit 0; fi\n'
        'mkdir -p "$HOME/.local/bin"\n'
        'printf "#!/bin/sh\\nexit 0\\n" > "$HOME/.local/bin/codex"\n'
        'chmod +x "$HOME/.local/bin/codex"\n'
    )
    _executable(
        binary_dir / "curl",
        'printf "curl:%s\\n" "$*" >> "$LAUNCH_LOG"\n'
        'if [ "${FAIL_DOWNLOAD:-}" = 1 ]; then exit 22; fi\n'
        f"printf '%s' {shlex.quote(installer)}\n",
    )
    environment = {
        "PATH": str(binary_dir),
        "HOME": str(home),
        "LAUNCH_LOG": str(log),
    }
    bash = shutil.which("bash")

    def run(*arguments, **overrides):
        result = subprocess.run(
            [bash, str(PROJECT_ROOT / "cow"), *arguments],
            env={**environment, **overrides},
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        lines = log.read_text().splitlines() if log.exists() else []
        return result, lines

    return run, binary_dir, home


@pytest.mark.parametrize("arguments", [(), ("run",), ("login",), ("wechat", "login")])
def test_installs_missing_codex_non_interactively(launcher, arguments):
    run, _, home = launcher

    result, lines = run(*arguments)

    assert result.returncode == 0, result.stderr
    assert f"curl:-fsSL {INSTALL_URL}" in lines
    assert "install:1" in lines
    assert (home / ".local/bin/codex").is_file()
    assert lines[-1].endswith(f":{home}/.local/bin/codex")


def test_does_not_reinstall_on_next_run(launcher):
    run, _, _ = launcher

    assert run()[0].returncode == 0
    result, lines = run()

    assert result.returncode == 0, result.stderr
    assert lines.count(f"curl:-fsSL {INSTALL_URL}") == 1
    assert lines.count("install:1") == 1


@pytest.mark.parametrize("location", ["path", "home"])
def test_reuses_existing_codex_without_download(launcher, location):
    run, binary_dir, home = launcher
    selected = _executable(
        binary_dir / "codex" if location == "path" else home / ".local/bin/codex"
    )

    result, lines = run()

    assert result.returncode == 0, result.stderr
    assert not any(line.startswith(("curl:", "install:")) for line in lines)
    assert lines[-1].endswith(f":{selected}")


def test_prefers_path_over_default_install_location(launcher):
    run, binary_dir, home = launcher
    selected = _executable(binary_dir / "codex")
    _executable(home / ".local/bin/codex")

    result, lines = run()

    assert result.returncode == 0, result.stderr
    assert lines[-1].endswith(f":{selected}")


@pytest.mark.parametrize("override_kind", ["absolute", "home", "command"])
def test_explicit_override_takes_priority(launcher, override_kind):
    run, binary_dir, home = launcher
    _executable(binary_dir / "codex")
    selected = _executable(home / "custom bin/codex")
    overrides = {"absolute": str(selected), "home": "~/custom bin/codex"}
    if override_kind == "command":
        selected = _executable(binary_dir / "selected-codex")
        overrides["command"] = "selected-codex"

    result, lines = run(CODEX_WECHAT_CODEX_BIN=overrides[override_kind])

    assert result.returncode == 0, result.stderr
    assert not any(line.startswith(("curl:", "install:")) for line in lines)
    assert lines[-1].endswith(f":{selected}")


@pytest.mark.parametrize("exists", [True, False])
def test_invalid_override_fails_without_install_or_fallback(launcher, exists):
    run, binary_dir, home = launcher
    _executable(binary_dir / "codex")
    selected = home / "invalid-codex"
    if exists:
        selected.write_text("not executable")

    result, lines = run(CODEX_WECHAT_CODEX_BIN=str(selected))

    assert result.returncode != 0
    assert "CODEX_WECHAT_CODEX_BIN" in result.stderr
    assert not lines


@pytest.mark.parametrize("failure", ["FAIL_INSTALL", "FAIL_DOWNLOAD", "SKIP_BINARY"])
def test_failed_install_never_launches_bot(launcher, failure):
    run, _, _ = launcher

    result, lines = run(**{failure: "1"})

    assert result.returncode != 0
    assert "error: Codex" in result.stderr
    assert not any(line.startswith("uv:") for line in lines)


@pytest.mark.parametrize(
    "arguments",
    [("status",), ("lark", "list"), ("logout",), ("wechat", "logout"), ("--help",)],
)
def test_admin_commands_do_not_install_codex(launcher, arguments):
    run, _, _ = launcher

    result, lines = run(*arguments)

    assert result.returncode == 0, result.stderr
    assert not any(line.startswith(("curl:", "install:")) for line in lines)
