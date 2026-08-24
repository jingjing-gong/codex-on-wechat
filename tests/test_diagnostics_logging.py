"""Persistent and structured runtime diagnostic regressions."""

from __future__ import annotations

import asyncio
import json
import logging
import stat

from src.agents.base import AgentResult
from src.agents.codex_runtime import CodexRuntime
from src.runtime.diagnostics import (
    configure_persistent_logging,
    sanitize_diagnostic_text,
)
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.worker import TaskWorker


def _task() -> dict[str, object]:
    return {
        "agent_id": "investigator",
        "conversation_id": "wechat:bot:user:default:investigator",
        "mode_id": "execute",
        "profile_version": 1,
        "policy_version": 1,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "ultra",
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": {"text": "private prompt must not be logged"},
    }


def test_persistent_logging_writes_owner_only_rotating_file(tmp_path, monkeypatch):
    log_path = tmp_path / "diagnostics" / "runtime.log"
    monkeypatch.setenv("CODEX_WECHAT_LOG", str(log_path))
    monkeypatch.setenv("CODEX_WECHAT_LOG_MAX_BYTES", "4096")
    monkeypatch.setenv("CODEX_WECHAT_LOG_BACKUPS", "2")

    root = logging.getLogger()
    previous_level = root.level
    try:
        assert configure_persistent_logging() == log_path.resolve()
        logging.getLogger("diagnostic-test").error("persistent marker")
        for handler in root.handlers:
            handler.flush()

        assert "persistent marker" in log_path.read_text(encoding="utf-8")
        assert stat.S_IMODE(log_path.stat().st_mode) == 0o600
    finally:
        for handler in tuple(root.handlers):
            if bool(getattr(handler, "_codex_wechat_persistent", False)):
                root.removeHandler(handler)
                handler.close()
        root.setLevel(previous_level)


def test_diagnostic_text_redacts_credentials_and_url_queries():
    rendered = sanitize_diagnostic_text(
        "Bearer abc.def token=top-secret "
        "url=https://provider.example/v1/responses?api_key=secret#fragment"
    )

    assert "abc.def" not in rendered
    assert "top-secret" not in rendered
    assert "api_key=secret" not in rendered
    assert "https://provider.example/v1/responses" in rendered
    assert "<redacted>" in rendered


def test_diagnostic_text_redacts_wrapped_and_structured_credentials():
    marker = "SYNTHETIC-DIAGNOSTIC-CREDENTIAL-6F31"
    cases = (
        f'Bearer "{marker}"',
        f'"Bearer" "{marker}"',
        f"Basic [{marker}]",
        f"Bearer: {marker}",
        f"bearer_token={marker}",
        f"bearerToken={marker}",
        f'{{"api_key": "{marker}"}}',
        f"access_token={marker}",
        f"refreshToken={marker}",
        f"client_secret={marker}",
        f"clientSecret={marker}",
    )

    for value in cases:
        rendered = sanitize_diagnostic_text(value)
        assert marker not in rendered
        assert "<redacted>" in rendered


def test_terminal_diagnostics_keep_typed_502_without_turn_items():
    details = CodexRuntime._terminal_diagnostics(
        {
            "method": "turn/completed",
            "payload": {
                "turn": {
                    "id": "turn-502",
                    "durationMs": 29_123,
                    "items": [{"type": "agentMessage", "text": "private output"}],
                    "error": {
                        "message": "unexpected status 502 Bad Gateway",
                        "additionalDetails": "upstream request failed",
                        "codexErrorInfo": {
                            "responseTooManyFailedAttempts": {
                                "httpStatusCode": 502
                            }
                        },
                    },
                }
            },
        }
    )

    assert details == {
        "phase": "terminal",
        "provider_turn_id": "turn-502",
        "provider_duration_ms": 29_123,
        "codex_error": {
            "message": "unexpected status 502 Bad Gateway",
            "additionalDetails": "upstream request failed",
            "codexErrorInfo": {
                "responseTooManyFailedAttempts": {"httpStatusCode": 502}
            },
        },
    }
    assert "private output" not in json.dumps(details)


def test_worker_logs_failed_result_without_prompt(tmp_path, caplog):
    class Runtime:
        agent_id = "investigator"

        async def run(self, task, _emit):
            return AgentResult(
                task_id=task.task_id,
                execution_id=task.execution_id,
                status="failed",
                error=(
                    "unexpected status 502 Bad Gateway: Unknown error, "
                    "url: http://106.15.49.184:8317/v1/responses"
                ),
                metadata={
                    "diagnostics": {
                        "phase": "terminal",
                        "provider_id": "usgw",
                        "model_id": "gpt-5.6-sol",
                        "reasoning_effort": "ultra",
                        "codex_error": {
                            "codexErrorInfo": {
                                "responseTooManyFailedAttempts": {
                                    "httpStatusCode": 502
                                }
                            }
                        },
                    }
                },
            )

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            task = await store.create_task(_task())
            worker = TaskWorker(store, runtime=Runtime(), worker_id="worker-test")

            with caplog.at_level(logging.INFO, logger="src.runtime.worker"):
                assert await worker.run_once() is True

            records = [
                record.getMessage()
                for record in caplog.records
                if record.name == "src.runtime.worker"
            ]
            started = next(value for value in records if value.startswith("agent_task_started "))
            terminal = next(value for value in records if value.startswith("agent_task_terminal "))
            payload = json.loads(terminal.removeprefix("agent_task_terminal "))

            assert task.task_id in started
            assert payload["agent_id"] == "investigator"
            assert payload["reasoning_effort"] == "ultra"
            assert payload["diagnostics"]["provider_id"] == "usgw"
            assert payload["diagnostics"]["codex_error"]["codexErrorInfo"][
                "responseTooManyFailedAttempts"
            ]["httpStatusCode"] == 502
            assert "private prompt must not be logged" not in "\n".join(records)
        finally:
            await store.close()

    asyncio.run(scenario())
