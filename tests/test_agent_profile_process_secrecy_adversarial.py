"""Adversarial secrecy probes for error-bearing process IPC messages."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest

from src.agents.base import AgentTask, ReplyTarget
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.runtime.manager import TaskManager
from src.runtime.process_agent import (
    ProcessAgentRemoteError,
    ProcessAgentRuntime,
    ProcessAgentStartupError,
)
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


_FACTORY = "tests.process_probe_backend:ipc_secrecy_probe_backend_factory"
_SECRET = "SYNTHETIC-IPC-URI-CREDENTIAL-7E19"
_ESC_SECRET = "SYNTHETIC-IPC-ESC-CREDENTIAL-91A4"
_BEARER_SECRET = "SYNTHETIC-IPC-QUOTED-BEARER-4C2D"
_BASIC_SECRET = "SYNTHETIC-IPC-QUOTED-BASIC-8F31"
_CAMEL_SECRET = "SYNTHETIC-IPC-CAMEL-SECRET-63B7"
_HOST = "provider-ipc-secret.invalid"
_URI_USER = "fake-user"


def _runtime(
    agent_id: str,
    tmp_path: Path,
    *,
    image_output_publisher: Any = None,
) -> ProcessAgentRuntime:
    return ProcessAgentRuntime.create(
        agent_id,
        cwd=str(tmp_path),
        backend_factory=_FACTORY,
        image_output_publisher=image_output_publisher,
        start_timeout=5,
        stop_timeout=5,
        event_ack_timeout=5,
    )


def _task(operation: str) -> AgentTask:
    return AgentTask(
        task_id=f"ipc-{operation}",
        execution_id=f"execution-ipc-{operation}",
        agent_id="ipc-probe",
        conversation_id="conversation-ipc-probe",
        inputs=operation,
    )


def _assert_no_sensitive_diagnostic(value: Any) -> None:
    rendered = str(value or "")
    assert _SECRET not in rendered
    assert _ESC_SECRET not in rendered
    assert _HOST not in rendered
    assert _URI_USER not in rendered
    assert "acme+tls://" not in rendered
    assert "\x1b" not in rendered
    assert "\x00" not in rendered
    assert "<redacted" in rendered


def _assert_exception_chain_is_sanitized(exc: BaseException) -> None:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        _assert_no_sensitive_diagnostic(current)
        current = current.__cause__ or current.__context__


async def _wait_for_terminal_task(
    manager: TaskManager,
    task_id: str,
    *,
    timeout: float = 5,
) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        task = await manager.get_task(task_id)
        raw_state = getattr(task, "state", "")
        state = str(getattr(raw_state, "value", raw_state) or "")
        if state in {"completed", "failed", "interrupted", "cancelled"}:
            return task
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(
                f"task {task_id} did not terminalize; current state={state}"
            )
        await asyncio.sleep(0.01)


def test_startup_fatal_redacts_custom_scheme_uri_userinfo(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("fatal-uri", tmp_path)
        try:
            with pytest.raises(ProcessAgentStartupError) as captured:
                await runtime.start()
            _assert_exception_chain_is_sanitized(captured.value)
            assert runtime.pid is None
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_startup_fatal_redacts_ansi_escape_obfuscated_assignment(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("fatal-c0", tmp_path)
        try:
            with pytest.raises(ProcessAgentStartupError) as captured:
                await runtime.start()
            _assert_exception_chain_is_sanitized(captured.value)
            assert runtime.pid is None
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_control_error_redacts_malformed_provider_diagnostics(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            with pytest.raises(ProcessAgentRemoteError) as captured:
                await runtime.list_models(include_hidden=False)
            _assert_exception_chain_is_sanitized(captured.value)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_raised_task_uri_error_is_sanitized_before_result_crosses_ipc(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            result = await runtime.run(_task("raise-uri-error"))
            assert result.status == "failed"
            _assert_no_sensitive_diagnostic(result.error)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_raised_task_c0_error_is_sanitized_before_result_crosses_ipc(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            result = await runtime.run(_task("raise-c0-error"))
            assert result.status == "failed"
            _assert_no_sensitive_diagnostic(result.error)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_raised_task_error_redacts_quoted_auth_schemes_and_tokens(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            result = await runtime.run(_task("raise-quoted-auth-error"))
            assert result.status == "failed"
            rendered = str(result.error or "")
            leaked = {
                marker
                for marker in (_BEARER_SECRET, _BASIC_SECRET, _CAMEL_SECRET)
                if marker in rendered
            }
            assert not leaked, f"leaked auth markers: {sorted(leaked)}"
            assert "<redacted" in rendered
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_returned_task_error_is_sanitized_before_result_crosses_ipc(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            result = await runtime.run(_task("return-error"))
            assert result.status == "failed"
            _assert_no_sensitive_diagnostic(result.error)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_provider_error_event_is_sanitized_before_parent_callback(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        observed = []
        try:
            result = await runtime.run(_task("emit-error"), observed.append)
            assert result.status == "completed"
            assert len(observed) == 1
            assert observed[0].event_type == "provider_error"
            _assert_no_sensitive_diagnostic(observed[0].content)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_returned_provider_error_event_is_sanitized_in_terminal_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        try:
            result = await runtime.run(_task("return-error-event"))
            assert result.status == "failed"
            assert len(result.events) == 1
            assert result.events[0].event_type == "provider_error"
            _assert_no_sensitive_diagnostic(result.events[0].content)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_parent_event_callback_error_is_sanitized_in_ack_and_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)

        async def reject_event(_event: Any) -> None:
            from tests.process_probe_backend import _ipc_sensitive_uri_text

            raise RuntimeError(_ipc_sensitive_uri_text())

        try:
            result = await runtime.run(_task("callback-error"), reject_event)
            assert result.status == "failed"
            _assert_no_sensitive_diagnostic(result.error)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_parent_artifact_callback_error_is_sanitized_in_ack_and_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from tests.process_probe_backend import _ipc_sensitive_failure_text

        async def reject_artifact(_task: Any, **_proposal: Any) -> None:
            raise RuntimeError(_ipc_sensitive_failure_text())

        runtime = _runtime(
            "ipc-probe",
            tmp_path,
            image_output_publisher=reject_artifact,
        )
        try:
            result = await runtime.run(_task("artifact-callback-error"))
            assert result.status == "failed"
            _assert_no_sensitive_diagnostic(result.error)
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_shutdown_error_is_sanitized_before_parent_receives_it(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        runtime = _runtime("stop-error", tmp_path)
        await runtime.start()
        try:
            with pytest.raises(ProcessAgentRemoteError) as captured:
                await runtime.stop()
            _assert_exception_chain_is_sanitized(captured.value)
            assert runtime.pid is None
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_interrupt_error_returns_sanitized_ack_without_child_stderr_leak(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> None:
        runtime = _runtime("ipc-probe", tmp_path)
        entered = asyncio.Event()

        async def observe_entered(_event: Any) -> None:
            entered.set()

        running = asyncio.create_task(
            runtime.run(_task("interrupt-error"), observe_entered)
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        try:
            with pytest.raises(ProcessAgentRemoteError) as captured:
                await asyncio.wait_for(
                    runtime.interrupt("ipc-interrupt-error"),
                    timeout=2,
                )
            _assert_exception_chain_is_sanitized(captured.value)
        finally:
            # The synthetic backend fails only its first interrupt, so cleanup
            # remains deterministic even when this regression fails.
            assert await asyncio.wait_for(
                runtime.interrupt("ipc-interrupt-error"),
                timeout=2,
            )
            result = await asyncio.wait_for(running, timeout=5)
            assert result.status == "interrupted"
            await runtime.stop()

    asyncio.run(scenario())
    child_output = capfd.readouterr()
    for rendered in (child_output.out, child_output.err):
        assert _SECRET not in rendered
        assert _ESC_SECRET not in rendered
        assert _HOST not in rendered
        assert "acme+tls://" not in rendered


def test_task_manager_durable_and_public_failure_chain_contains_no_ipc_secrets(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "ipc-secrecy.sqlite"
        runtime = _runtime("codex", tmp_path)
        registry = AgentRegistry()
        registry.register(
            "codex",
            runtime,
            profile=codex_profile(default_mode_id="chat"),
        )
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=1,
            default_agent_id="codex",
            default_mode_id="chat",
            require_process_isolation=True,
            reconcile_interval=None,
        )
        target = ReplyTarget(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
        )
        caplog.set_level(logging.INFO)
        await manager.start()
        try:
            await manager.submit(
                "return-error",
                target,
                task_id="durable-returned-error",
                agent_id="codex",
                mode_id="chat",
            )
            returned_error = await _wait_for_terminal_task(
                manager, "durable-returned-error"
            )
            assert str(getattr(returned_error.state, "value", returned_error.state)) == (
                "failed"
            )

            await manager.submit(
                "return-error-event",
                target,
                task_id="durable-returned-event",
                agent_id="codex",
                mode_id="chat",
            )
            returned_event = await _wait_for_terminal_task(
                manager, "durable-returned-event"
            )
            assert str(getattr(returned_event.state, "value", returned_event.state)) == (
                "failed"
            )

            task_events = {
                task_id: await manager.store.list_task_events(task_id)
                for task_id in (
                    "durable-returned-error",
                    "durable-returned-event",
                )
            }
            outbox = await manager.store.list_outbox()
            envelope = InboundEnvelope(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                external_message_id="ipc-secrecy-tasks",
                text="/tasks 20",
                session_id="default",
                agent_id="codex",
                conversation_id="wechat:bot:user:default:codex",
            )
            public_tasks = await MVPCommandRouter(manager).handle_command(
                parse_command(envelope.text), envelope
            )

            inspected = repr(
                {
                    "tasks": (returned_error, returned_event),
                    "events": task_events,
                    "outbox": outbox,
                    "public_tasks": str(public_tasks),
                    "logs": caplog.text,
                }
            )
            for forbidden in (
                _SECRET,
                _ESC_SECRET,
                _BEARER_SECRET,
                _BASIC_SECRET,
                _CAMEL_SECRET,
                _HOST,
                _URI_USER,
                "acme+tls://",
                "\x1b",
                "\x00",
            ):
                assert forbidden not in inspected
            assert "<redacted" in inspected
            assert "task failed:" in inspected
        finally:
            await manager.stop()

    asyncio.run(scenario())
