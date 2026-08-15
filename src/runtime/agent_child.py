"""Minimal direct-exec launcher for the disconnected Agent child.

Parent-death fencing is armed before importing asyncio, the IPC codec, or the
child runtime.  This file intentionally never imports the supervisor client.
"""

from __future__ import annotations

import ctypes
import os
import signal
import sys


_CHILD_PROTOCOL_EXIT = 70
_PR_SET_PDEATHSIG = 1


def _argument(argv: list[str], name: str) -> str:
    try:
        index = argv.index(name)
        return argv[index + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError(f"missing child argument {name}") from exc


def _current_parent_start_time(parent_pid: int) -> int:
    value = open(f"/proc/{parent_pid}/stat", "r", encoding="ascii").read()
    closing = value.rfind(")")
    fields = value[closing + 2 :].split()
    return int(fields[19])


def _arm_parent_death(argv: list[str]) -> None:
    expected_pid = int(_argument(argv, "--parent-pid"))
    expected_start = int(_argument(argv, "--parent-start-time"))
    expected_boot = _argument(argv, "--parent-boot-id")
    if min(expected_pid, expected_start) <= 0 or not sys.platform.startswith("linux"):
        raise RuntimeError("Linux parent-death fencing is required")

    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = (
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    )
    prctl.restype = ctypes.c_int
    if prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "cannot arm Linux parent-death signal")

    # PR_SET_PDEATHSIG has a documented setup race.  Rechecking both the
    # parent relationship and kernel birth identity closes it without using a
    # thread-unsafe Popen(preexec_fn=...).
    if os.getppid() != expected_pid:
        raise RuntimeError("Agent supervisor exited during child launch")
    boot_id = open(
        "/proc/sys/kernel/random/boot_id", "r", encoding="ascii"
    ).read().strip()
    if boot_id != expected_boot or _current_parent_start_time(expected_pid) != expected_start:
        raise RuntimeError("Agent supervisor birth identity changed")


def _main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    try:
        _arm_parent_death(values)
        module_root = os.path.realpath(os.path.dirname(__file__))
        if module_root not in sys.path:
            sys.path.insert(0, module_root)
        import agent_child_runtime

        expected = os.path.realpath(os.path.join(module_root, "agent_child_runtime.py"))
        if os.path.realpath(agent_child_runtime.__file__) != expected:
            raise ImportError("Agent child resolved an unexpected runtime module")
        return int(agent_child_runtime.main(values))
    except BaseException:
        # Never print bootstrap/authentication material from the child.
        return _CHILD_PROTOCOL_EXIT


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(_main())
