"""Credential redaction at the dedicated Agent process boundary."""

from __future__ import annotations

import asyncio

import pytest

from src.runtime.process_agent import (
    ProcessAgentRemoteError,
    ProcessAgentRuntime,
)


_SYNTHETIC_MARKER = "SYNTHETIC-CHILD-CREDENTIAL-MARKER"
_SYNTHETIC_URL = (
    "https://synthetic-child-provider.invalid/v1/models?"
    f"token={_SYNTHETIC_MARKER}"
)


class _SensitiveFailureBackend:
    def __init__(self, **_kwargs) -> None:
        return None

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def run(self, _task, _emit=None):
        return {"status": "completed"}

    async def list_models(self, *, include_hidden: bool = False):
        del include_hidden
        raise RuntimeError(
            f"provider failed at {_SYNTHETIC_URL}; "
            f"Authorization: Bearer {_SYNTHETIC_MARKER}"
        )


def sensitive_failure_backend_factory(**kwargs):
    return _SensitiveFailureBackend(**kwargs)


def test_child_control_error_cannot_export_provider_credentials_or_url(
    tmp_path,
) -> None:
    async def scenario() -> None:
        runtime = ProcessAgentRuntime.create(
            "researcher",
            cwd=str(tmp_path),
            codex_config_profile="qwen",
            backend_factory=(
                "tests.test_agent_profile_process_secrecy:"
                "sensitive_failure_backend_factory"
            ),
            start_timeout=5,
            stop_timeout=5,
            event_ack_timeout=5,
        )
        try:
            with pytest.raises(ProcessAgentRemoteError) as captured:
                await runtime.list_models(include_hidden=False)
            public_parent_error = str(captured.value)
            assert _SYNTHETIC_MARKER not in public_parent_error
            assert "synthetic-child-provider.invalid" not in public_parent_error
            assert "https://" not in public_parent_error
            assert "<redacted" in public_parent_error
        finally:
            await runtime.stop()

    asyncio.run(scenario())
