"""Bounded POSIX shell-process execution used by WeChat commands."""

from __future__ import annotations

import codecs
from dataclasses import dataclass
import io
import locale
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time
from typing import BinaryIO


_READ_CHUNK_BYTES = 64 * 1024
_TERMINATE_GRACE_SECONDS = 0.25
_KILL_GRACE_SECONDS = 0.5
_STDERR_HEADING = "\nstderr:\n"
_TRUNCATION_MARKER = "\n... (output truncated)"


class _BoundedTextCapture:
    """Decode a stream continuously while retaining only a fixed prefix."""

    def __init__(self, limit: int, encoding: str) -> None:
        self._limit = limit
        self._decoder = io.IncrementalNewlineDecoder(
            codecs.getincrementaldecoder(encoding)(),
            translate=True,
        )
        self._parts: list[str] = []
        self._stored_length = 0
        self.total_length = 0
        self.last_non_whitespace = -1
        self._finished = False

    def feed(self, value: bytes, *, final: bool = False) -> None:
        if self._finished:
            return
        decoded = self._decoder.decode(value, final=final)
        if decoded:
            without_trailing_whitespace = decoded.rstrip()
            if without_trailing_whitespace:
                self.last_non_whitespace = (
                    self.total_length + len(without_trailing_whitespace) - 1
                )
            self.total_length += len(decoded)
            remaining = self._limit - self._stored_length
            if remaining > 0:
                prefix = decoded[:remaining]
                self._parts.append(prefix)
                self._stored_length += len(prefix)
        if final:
            self._finished = True

    def finish(self) -> None:
        self.feed(b"", final=True)

    @property
    def prefix(self) -> str:
        return "".join(self._parts)


@dataclass(frozen=True, slots=True)
class BoundedShellResult:
    """A completed shell result whose user-visible output is already bounded."""

    returncode: int
    output: str


def _read_ready_streams(
    selector: selectors.BaseSelector,
    captures: dict[int, _BoundedTextCapture],
    timeout: float,
) -> bool:
    """Drain currently readable pipes and return whether select found work."""

    events = selector.select(max(0.0, timeout))
    for key, _mask in events:
        file_descriptor = key.fd
        capture = captures[file_descriptor]
        try:
            chunk = os.read(file_descriptor, _READ_CHUNK_BYTES)
        except BlockingIOError:
            continue
        if chunk:
            capture.feed(chunk)
            continue
        capture.finish()
        selector.unregister(key.fileobj)
        key.fileobj.close()
    return bool(events)


def _signal_process_group(process: subprocess.Popen[bytes], signum: int) -> None:
    """Signal the private session created for one shell command."""

    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


def _drain_for(
    selector: selectors.BaseSelector,
    captures: dict[int, _BoundedTextCapture],
    duration: float,
) -> None:
    deadline = time.monotonic() + duration
    while selector.get_map():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        _read_ready_streams(selector, captures, remaining)


def _terminate_process_group(
    process: subprocess.Popen[bytes],
    selector: selectors.BaseSelector,
    captures: dict[int, _BoundedTextCapture],
) -> None:
    """Boundedly terminate the shell and every process in its session group."""

    try:
        _signal_process_group(process, signal.SIGTERM)
        _drain_for(selector, captures, _TERMINATE_GRACE_SECONDS)
    except BaseException:
        # Teardown must continue even when decoding malformed output or
        # another cleanup-only error fails while the pipes are being drained.
        pass

    # The group may outlive its shell leader, including when a background
    # descendant ignores SIGTERM while retaining either output pipe.
    try:
        _signal_process_group(process, signal.SIGKILL)
    except BaseException:
        pass
    try:
        _drain_for(selector, captures, _KILL_GRACE_SECONDS)
    except BaseException:
        pass
    try:
        process.wait(timeout=_KILL_GRACE_SECONDS)
    except BaseException:
        # SIGKILL has already been attempted. Do not let an uninterruptible
        # process or cleanup-only error make a WeChat command hang forever.
        pass


def _close_pipes(
    selector: selectors.BaseSelector,
    streams: tuple[BinaryIO, BinaryIO],
) -> None:
    selector.close()
    for stream in streams:
        if not stream.closed:
            stream.close()


def _combined_output(
    stdout: _BoundedTextCapture,
    stderr: _BoundedTextCapture,
    max_output: int,
) -> str:
    has_stderr = stderr.total_length > 0
    if has_stderr:
        captured = stdout.prefix + _STDERR_HEADING + stderr.prefix
        if stderr.last_non_whitespace >= 0:
            stripped_length = (
                stdout.total_length
                + len(_STDERR_HEADING)
                + stderr.last_non_whitespace
                + 1
            )
        else:
            stripped_length = stdout.total_length + len(_STDERR_HEADING.rstrip())
    else:
        captured = stdout.prefix
        stripped_length = stdout.last_non_whitespace + 1

    if stripped_length == 0:
        captured = "(no output)"
        stripped_length = len(captured)
    output = captured[: min(stripped_length, max_output)]
    if stripped_length > max_output:
        output += _TRUNCATION_MARKER
    return output


def run_bounded_shell_process(
    command: str,
    *,
    cwd: str | Path,
    timeout: float,
    max_output: int,
) -> BoundedShellResult:
    """Run ``command`` with bounded capture and whole-process-group timeout.

    stdout and stderr are drained concurrently even after their retained
    prefixes fill.  A command is complete only after the shell exits and both
    pipes reach EOF, so a background descendant retaining a pipe remains
    covered by the same timeout and process-group cleanup.
    """

    if max_output < 0:
        raise ValueError("max_output must be non-negative")
    if timeout < 0:
        raise ValueError("timeout must be non-negative")

    encoding = locale.getpreferredencoding(False)
    stdout_capture = _BoundedTextCapture(max_output + 1, encoding)
    stderr_capture = _BoundedTextCapture(max_output + 1, encoding)
    selector = selectors.DefaultSelector()
    try:
        process = subprocess.Popen(
            command,
            shell=True,
            executable="/bin/sh",
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except BaseException:
        selector.close()
        raise
    assert process.stdout is not None
    assert process.stderr is not None
    streams = (process.stdout, process.stderr)
    capture_by_fd = {
        process.stdout.fileno(): stdout_capture,
        process.stderr.fileno(): stderr_capture,
    }
    deadline = time.monotonic() + timeout

    try:
        selector.register(process.stdout, selectors.EVENT_READ)
        selector.register(process.stderr, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not _read_ready_streams(
                selector, capture_by_fd, remaining
            ):
                raise subprocess.TimeoutExpired(command, timeout)

        if process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout)
            process.wait(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_group(process, selector, capture_by_fd)
        exc.output = stdout_capture.prefix
        exc.stderr = stderr_capture.prefix
        raise
    except BaseException:
        _terminate_process_group(process, selector, capture_by_fd)
        raise
    finally:
        _close_pipes(selector, streams)

    return BoundedShellResult(
        returncode=process.returncode,
        output=_combined_output(stdout_capture, stderr_capture, max_output),
    )


__all__ = ["BoundedShellResult", "run_bounded_shell_process"]
