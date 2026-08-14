"""Process-safety regressions for durable and legacy ``/sh`` execution."""

from __future__ import annotations

import shlex
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Callable

import pytest

from src.channels.wechat import run_shell_command as run_durable_shell_command
from src.codex_wechat_bot import run_shell_command as run_legacy_shell_command


ShellRunner = Callable[..., str]


@pytest.mark.parametrize(
    "runner",
    [run_durable_shell_command, run_legacy_shell_command],
    ids=["durable", "legacy"],
)
def test_shell_empty_output_respects_the_configured_bound(
    runner: ShellRunner, tmp_path: Path
) -> None:
    result = runner("true", cwd=tmp_path, timeout=1, max_output=0)

    assert "(no output)" not in result
    assert "... (output truncated)" in result


@pytest.mark.parametrize(
    "runner",
    [run_durable_shell_command, run_legacy_shell_command],
    ids=["durable", "legacy"],
)
def test_shell_capture_stays_bounded_while_draining_large_output(
    runner: ShellRunner, tmp_path: Path
) -> None:
    script = (
        "import os\n"
        "chunk = b'x' * 65536\n"
        "for _ in range(128):\n"
        "    os.write(1, chunk)\n"
        "for _ in range(128):\n"
        "    os.write(2, chunk)\n"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"

    tracemalloc.start()
    try:
        result = runner(command, cwd=tmp_path, timeout=10, max_output=256)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert "exit code" in result.lower()
    assert "... (output truncated)" in result
    assert peak < 2 * 1024 * 1024


@pytest.mark.parametrize(
    "runner",
    [run_durable_shell_command, run_legacy_shell_command],
    ids=["durable", "legacy"],
)
def test_shell_timeout_kills_descendants_that_retain_output_pipes(
    runner: ShellRunner, tmp_path: Path
) -> None:
    ready = tmp_path / "ready"
    survived = tmp_path / "survived"
    command = (
        "trap '' TERM; "
        f"printf ready > {shlex.quote(str(ready))}; "
        f"(sleep 1; printf survived > {shlex.quote(str(survived))}; sleep 2) &"
    )

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        runner(command, cwd=tmp_path, timeout=0.2, max_output=256)
    elapsed = time.monotonic() - started

    assert ready.read_text() == "ready"
    assert elapsed < 1.0
    time.sleep(0.9)
    assert not survived.exists()


def test_shell_timeout_after_command_closes_its_output_pipes(tmp_path: Path) -> None:
    command = "exec >/dev/null 2>&1; trap '' TERM; sleep 2"

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        run_durable_shell_command(
            command,
            cwd=tmp_path,
            timeout=0.1,
            max_output=256,
        )

    assert time.monotonic() - started < 1.0


def test_shell_decode_failure_still_kills_the_process_group(tmp_path: Path) -> None:
    survived = tmp_path / "survived"
    script = (
        "import os, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "os.write(1, b'\\xff')\n"
        "time.sleep(0.6)\n"
        f"open({str(survived)!r}, 'w').write('survived')\n"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"

    with pytest.raises(UnicodeDecodeError):
        run_durable_shell_command(
            command,
            cwd=tmp_path,
            timeout=2,
            max_output=256,
        )

    time.sleep(0.7)
    assert not survived.exists()
