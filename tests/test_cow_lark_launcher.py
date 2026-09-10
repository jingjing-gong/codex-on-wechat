"""Shell-launcher compatibility tests for the additive Lark command family."""

from __future__ import annotations

import os
import pty
import select
import subprocess
import termios
import time
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _fake_uv(tmp_path: Path) -> tuple[Path, Path]:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    log = tmp_path / "uv.log"
    uv = binary_dir / "uv"
    uv.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_UV_LOG\"\n"
        "if [[ -n \"${FAKE_UV_STDIN_LOG:-}\" ]]; then\n"
        "  fake_stdin=''\n"
        "  IFS= read -r fake_stdin || true\n"
        "  printf '%s\\t%s\\n' \"${1:-}\" \"$fake_stdin\" >> \"$FAKE_UV_STDIN_LOG\"\n"
        "fi\n"
        "if [[ -n \"${FAKE_UV_ENV_PROBE_LOG:-}\" ]]; then\n"
        "  printf '%s\\t%s\\t%s\\n' \"${1:-}\" \"${lark_app_secret+set}\" \"${lark_app_secret-}\" >> \"$FAKE_UV_ENV_PROBE_LOG\"\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    uv.chmod(0o755)
    codex = binary_dir / "codex"
    codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    codex.chmod(0o755)
    return binary_dir, log


def _run_cow(tmp_path: Path, *arguments: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    binary_dir, log = _fake_uv(tmp_path)
    environment = dict(os.environ)
    environment["PATH"] = str(binary_dir) + os.pathsep + environment.get("PATH", "")
    environment["FAKE_UV_LOG"] = str(log)
    result = subprocess.run(
        ["bash", str(PROJECT_ROOT / "cow"), *arguments],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    lines = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return result, lines


@pytest.mark.parametrize(
    "arguments, expected_run",
    [
        ((), "run src/codex_wechat_bot.py"),
        (("run", "--legacy"), "run src/codex_wechat_bot.py --legacy"),
        (("wechat", "login"), "run src/codex_wechat_bot.py --login"),
        (("wechat", "logout"), "run src/codex_wechat_bot.py --logout"),
        (("login",), "run src/codex_wechat_bot.py --login"),
        (("logout",), "run src/codex_wechat_bot.py --logout"),
        (("status",), "run src/status_cli.py"),
        (("lark", "list"), "run src/lark_cli.py list"),
        (("lark", "status", "team"), "run src/lark_cli.py status team"),
        (("lark", "add", "team"), "run src/lark_cli.py add team"),
        (
            ("lark", "add", "team", "--owner-open-id", "ou_team_owner"),
            "run src/lark_cli.py add team --owner-open-id ou_team_owner",
        ),
        (
            ("lark", "recover", ".add-team-deadbeef"),
            "run src/lark_cli.py recover .add-team-deadbeef",
        ),
        (
            ("lark", "recover", ".add-source-cafebabe", "destination"),
            "run src/lark_cli.py recover .add-source-cafebabe destination",
        ),
        (
            (
                "principal",
                "map",
                "person:owner",
                "wechat",
                "live-bot",
                "live-user",
            ),
            "run src/principal_cli.py map person:owner wechat live-bot live-user",
        ),
    ],
)
def test_launcher_preserves_existing_routes_and_adds_lark(
    tmp_path: Path,
    arguments: tuple[str, ...],
    expected_run: str,
) -> None:
    result, lines = _run_cow(tmp_path, *arguments)

    assert result.returncode == 0, result.stderr
    assert lines == ["sync --extra test", expected_run]
    assert "Owner Open ID" not in result.stderr
    assert "App Secret:" not in result.stderr


def test_launcher_help_mentions_lark_without_changing_existing_help(tmp_path: Path) -> None:
    result, lines = _run_cow(tmp_path, "--help")

    assert result.returncode == 0
    assert lines == []
    assert "./cow          # run the bot" in result.stdout
    assert "./cow wechat login" in result.stdout
    assert "./cow wechat logout" in result.stdout
    assert "./cow login" in result.stdout
    assert "./cow status" in result.stdout
    assert "./cow lark ..." in result.stdout
    assert "./cow principal ..." in result.stdout


def test_wechat_launcher_help_is_scoped_and_does_not_run_uv(tmp_path: Path) -> None:
    result, lines = _run_cow(tmp_path, "wechat", "--help")

    assert result.returncode == 0
    assert lines == []
    assert "usage: ./cow wechat <login|logout>" in result.stdout
    assert "login   log in to WeChat" in result.stdout
    assert "logout  delete saved WeChat credentials" in result.stdout


@pytest.mark.parametrize("arguments", [("wechat",), ("wechat", "unknown")])
def test_wechat_launcher_rejects_missing_or_unknown_action(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    result, lines = _run_cow(tmp_path, *arguments)

    assert result.returncode == 2
    assert lines == []
    assert "usage: ./cow wechat <login|logout>" in result.stderr
    if len(arguments) == 2:
        assert "unknown WeChat command: unknown" in result.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        ("wechat", "login", "--logout"),
        ("wechat", "logout", "--login"),
        ("wechat", "login", "unexpected"),
    ],
)
def test_wechat_launcher_rejects_trailing_arguments(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    result, lines = _run_cow(tmp_path, *arguments)

    assert result.returncode == 2
    assert lines == []
    assert f"unexpected arguments for './cow wechat {arguments[1]}'" in result.stderr
    assert "usage: ./cow wechat <login|logout>" in result.stderr


def test_lark_launcher_reserves_secret_stdin_for_the_owner_cli(tmp_path: Path) -> None:
    binary_dir, log = _fake_uv(tmp_path)
    stdin_log = tmp_path / "uv-stdin.log"
    environment = dict(os.environ)
    environment["PATH"] = str(binary_dir) + os.pathsep + environment.get("PATH", "")
    environment["FAKE_UV_LOG"] = str(log)
    environment["FAKE_UV_STDIN_LOG"] = str(stdin_log)

    result = subprocess.run(
        [
            "bash",
            str(PROJECT_ROOT / "cow"),
            "lark",
            "add",
            "existing",
            "--app-id",
            "cli_existing_app",
            "--app-secret-stdin",
            "--owner-open-id",
            "ou_existing_owner",
        ],
        cwd=tmp_path,
        env=environment,
        input="synthetic-secret\n",
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert log.read_text(encoding="utf-8").splitlines() == [
        "sync --extra test",
        "run src/lark_cli.py add existing --app-id cli_existing_app "
        "--app-secret-stdin --owner-open-id ou_existing_owner",
    ]
    assert stdin_log.read_text(encoding="utf-8").splitlines() == [
        "sync\t",
        "run\tsynthetic-secret",
    ]


@pytest.mark.skipif(os.name != "posix", reason="hidden prompt requires a POSIX tty")
@pytest.mark.parametrize(
    ("explicit_stdin_flag", "xtrace", "owner_arguments"),
    [
        (False, False, ()),
        (False, False, ("--owner-open-id", "ou_existing_owner")),
        (True, True, ("--owner-open-id", "ou_existing_owner")),
        (False, False, ("--without-owner",)),
    ],
    ids=[
        "default-owner-managed-bot",
        "implicit-stdin",
        "explicit-stdin-with-inherited-xtrace",
        "explicit-without-owner",
    ],
)
def test_lark_launcher_prompts_without_echo_and_keeps_secret_off_argv_and_env(
    tmp_path: Path,
    explicit_stdin_flag: bool,
    xtrace: bool,
    owner_arguments: tuple[str, ...],
) -> None:
    binary_dir, log = _fake_uv(tmp_path)
    stdin_log = tmp_path / "uv-stdin.log"
    environment_log = tmp_path / "uv-environment.log"
    environment = dict(os.environ)
    environment["PATH"] = str(binary_dir) + os.pathsep + environment.get("PATH", "")
    environment["FAKE_UV_LOG"] = str(log)
    environment["FAKE_UV_STDIN_LOG"] = str(stdin_log)
    environment["FAKE_UV_ENV_PROBE_LOG"] = str(environment_log)
    arguments = ["bash"]
    if xtrace:
        arguments.append("-x")
    arguments.extend(
        [
            str(PROJECT_ROOT / "cow"),
            "lark",
            "add",
            "existing",
            "--app-id",
            "cli_existing_app",
            "--brand",
            "feishu",
            *owner_arguments,
        ]
    )
    if explicit_stdin_flag:
        arguments.append("--app-secret-stdin")
    secret = r"-interactive secret $() \\ * ; end-"
    master_fd, slave_fd = pty.openpty()
    process = subprocess.Popen(
        arguments,
        cwd=tmp_path,
        env=environment,
        stdin=slave_fd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        close_fds=True,
    )
    prompt = bytearray()
    tty_output = bytearray()
    try:
        assert process.stderr is not None
        deadline = time.monotonic() + 10
        while b"App Secret: " not in prompt:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "launcher did not display the App Secret prompt"
            readable, _writable, _exceptional = select.select(
                [process.stderr], [], [], remaining
            )
            assert readable, "launcher did not display the App Secret prompt"
            chunk = os.read(process.stderr.fileno(), 4_096)
            assert chunk, "launcher exited before reading the App Secret"
            prompt.extend(chunk)

        while termios.tcgetattr(slave_fd)[3] & termios.ECHO:
            assert time.monotonic() < deadline, "terminal echo was not disabled"
            time.sleep(0.01)
        os.write(master_fd, secret.encode("utf-8") + b"\n")
        os.close(slave_fd)
        slave_fd = -1
        stdout, stderr_tail = process.communicate(timeout=10)

        while True:
            readable, _writable, _exceptional = select.select(
                [master_fd], [], [], 0
            )
            if not readable:
                break
            try:
                chunk = os.read(master_fd, 4_096)
            except OSError:
                break
            if not chunk:
                break
            tty_output.extend(chunk)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if slave_fd >= 0:
            os.close(slave_fd)
        os.close(master_fd)

    assert process.returncode == 0
    owner_arguments_text = (
        " " + " ".join(owner_arguments) if owner_arguments else ""
    )
    expected_arguments = (
        "run src/lark_cli.py add existing --app-id cli_existing_app --brand feishu "
        f"{owner_arguments_text.lstrip()} --app-secret-stdin"
        if explicit_stdin_flag
        else "run src/lark_cli.py add --app-secret-stdin existing --app-id "
        f"cli_existing_app --brand feishu{owner_arguments_text}"
    )
    assert log.read_text(encoding="utf-8").splitlines() == [
        "sync --extra test",
        expected_arguments,
    ]
    assert stdin_log.read_text(encoding="utf-8").splitlines() == [
        "sync\t",
        f"run\t{secret}",
    ]
    assert environment_log.read_text(encoding="utf-8").splitlines() == [
        "sync\t\t",
        "run\t\t",
    ]
    public_output = stdout + bytes(prompt) + stderr_tail + bytes(tty_output)
    assert b"App Secret: " in public_output
    assert b"Owner Open ID" not in public_output
    assert secret.encode("utf-8") not in public_output
    assert secret not in "\n".join(log.read_text(encoding="utf-8").splitlines())
    assert secret not in environment_log.read_text(encoding="utf-8")


@pytest.mark.skipif(os.name != "posix", reason="prompt routing requires a POSIX tty")
@pytest.mark.parametrize(
    "arguments",
    [
        ("add", "qr-team"),
        ("add", "invalid", "--app-id", "not-an-app-id"),
        ("add", "--help", "--app-id", "cli_valid_app"),
        ("add", "--", "--app-id", "cli_valid_app"),
        ("status", "team", "--app-id", "cli_valid_app"),
    ],
    ids=["qr", "invalid-app-id", "help", "option-terminator", "other-command"],
)
def test_lark_launcher_does_not_prompt_for_non_existing_app_paths(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    binary_dir, log = _fake_uv(tmp_path)
    environment = dict(os.environ)
    environment["PATH"] = str(binary_dir) + os.pathsep + environment.get("PATH", "")
    environment["FAKE_UV_LOG"] = str(log)
    master_fd, slave_fd = pty.openpty()
    try:
        result = subprocess.run(
            ["bash", str(PROJECT_ROOT / "cow"), "lark", *arguments],
            cwd=tmp_path,
            env=environment,
            stdin=slave_fd,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    finally:
        os.close(slave_fd)
        os.close(master_fd)

    assert result.returncode == 0, result.stderr
    assert "App Secret:" not in result.stderr
    assert "Owner Open ID" not in result.stderr
    assert log.read_text(encoding="utf-8").splitlines() == [
        "sync --extra test",
        "run src/lark_cli.py " + " ".join(arguments),
    ]


def test_unknown_launcher_command_reports_lark_usage(tmp_path: Path) -> None:
    result, lines = _run_cow(tmp_path, "unknown")

    assert result.returncode == 2
    assert lines == []
    assert "unknown command: unknown" in result.stderr
    assert "wechat <login|logout>" in result.stderr
    assert "status" in result.stderr
    assert "lark <command>" in result.stderr
    assert "principal <command>" in result.stderr
