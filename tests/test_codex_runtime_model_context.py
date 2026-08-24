"""CodexRuntime integration tests for provider-derived context settings."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.agents import model_context
from src.agents.base import AgentTask
from src.agents.codex_runtime import CodexRuntime
from src.agents.model_context import (
    ModelContextResolutionError,
    ModelContextSettings,
)
from src.runtime.roles import implicit_default_role


class _Thread:
    def __init__(self, thread_id: str) -> None:
        self.id = thread_id


class _Codex:
    def __init__(self) -> None:
        self.thread_start_calls: list[dict[str, Any]] = []
        self.thread_resume_calls: list[tuple[str, dict[str, Any]]] = []
        self._next_thread = 1

    async def thread_start(self, **kwargs: Any) -> _Thread:
        self.thread_start_calls.append(dict(kwargs))
        thread = _Thread(f"thread-{self._next_thread}")
        self._next_thread += 1
        return thread

    async def thread_resume(self, thread_id: str, **kwargs: Any) -> _Thread:
        self.thread_resume_calls.append((thread_id, dict(kwargs)))
        return _Thread(thread_id)


class _Resolver:
    def __init__(self, settings: ModelContextSettings | Exception) -> None:
        self.settings = settings
        self.calls: list[str | None] = []

    async def resolve(self, model_id: str | None = None) -> ModelContextSettings:
        self.calls.append(model_id)
        if isinstance(self.settings, Exception):
            raise self.settings
        return self.settings


def _settings(
    *,
    provider: str = "provider-a",
    model: str = "model-a",
    window: int = 128_000,
    threshold: int = 102_400,
) -> ModelContextSettings:
    return ModelContextSettings(
        provider_id=provider,
        model_id=model,
        context_window=window,
        model_auto_compact_token_limit=threshold,
        source="provider:model-detail",
    )


def _task(**overrides: Any) -> AgentTask:
    values: dict[str, Any] = {
        "task_id": "task-a",
        "agent_id": "codex",
        "conversation_id": "conversation-a",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "model": "model-a",
        "metadata": {"session_role": implicit_default_role()},
    }
    values.update(overrides)
    return AgentTask(**values)


def _expected_config(window: int, threshold: int) -> dict[str, Any]:
    return {
        "model_context_window": window,
        "model_auto_compact_token_limit": threshold,
        "model_auto_compact_token_limit_scope": "total",
        "features": {"unified_exec": False},
        "tool_output_token_limit": 500,
    }


def test_new_and_persisted_threads_receive_identical_provider_context_config():
    async def scenario() -> None:
        resolver = _Resolver(_settings())

        started_codex = _Codex()
        started_runtime = CodexRuntime(
            codex=started_codex,
            cwd="/workspace",
            model_context_resolver=resolver,
        )
        await started_runtime.start()
        started = await started_runtime._binding_for(_task())

        resumed_codex = _Codex()
        resumed_runtime = CodexRuntime(
            codex=resumed_codex,
            cwd="/workspace",
            model_context_resolver=resolver,
        )
        await resumed_runtime.start()
        resumed = await resumed_runtime._binding_for(
            _task(task_id="task-resumed", thread_id="thread-persisted")
        )

        assert started.thread_id == "thread-1"
        assert resumed.thread_id == "thread-persisted"
        assert len(started_codex.thread_start_calls) == 1
        assert len(resumed_codex.thread_resume_calls) == 1
        start_kwargs = started_codex.thread_start_calls[0]
        resumed_id, resume_kwargs = resumed_codex.thread_resume_calls[0]
        assert resumed_id == "thread-persisted"
        for kwargs in (start_kwargs, resume_kwargs):
            assert kwargs["model"] == "model-a"
            assert kwargs["model_provider"] == "provider-a"
            assert kwargs["config"] == _expected_config(128_000, 102_400)
        assert started.context_config_fingerprint
        assert (
            started.context_config_fingerprint
            == resumed.context_config_fingerprint
        )

    asyncio.run(scenario())


def test_idle_live_binding_resumes_same_thread_when_context_config_changes():
    async def scenario() -> None:
        resolver = _Resolver(_settings())
        codex = _Codex()
        runtime = CodexRuntime(
            codex=codex,
            cwd="/workspace",
            model_context_resolver=resolver,
        )
        await runtime.start()
        first = await runtime._binding_for(_task())

        resolver.settings = _settings(window=64_000, threshold=51_200)
        refreshed = await runtime._binding_for(_task(task_id="task-b"))
        unchanged = await runtime._binding_for(_task(task_id="task-c"))

        assert first.thread_id == refreshed.thread_id == unchanged.thread_id
        assert first.context_config_fingerprint != refreshed.context_config_fingerprint
        assert refreshed is unchanged
        assert len(codex.thread_start_calls) == 1
        assert len(codex.thread_resume_calls) == 1
        assert codex.thread_resume_calls[0][0] == "thread-1"
        assert codex.thread_resume_calls[0][1]["config"] == _expected_config(
            64_000,
            51_200,
        )

    asyncio.run(scenario())


def test_model_change_refreshes_same_provider_and_provider_change_fails_closed():
    async def scenario() -> None:
        resolver = _Resolver(_settings())
        codex = _Codex()
        runtime = CodexRuntime(
            codex=codex,
            cwd="/workspace",
            model_context_resolver=resolver,
        )
        await runtime.start()
        first = await runtime._binding_for(_task())

        resolver.settings = _settings(
            model="model-b",
            window=96_000,
            threshold=76_800,
        )
        changed_model = await runtime._binding_for(
            _task(task_id="task-b", model="model-b")
        )
        assert changed_model.thread_id == first.thread_id
        assert changed_model.model_id == "model-b"
        assert codex.thread_resume_calls[-1][1]["model"] == "model-b"

        resolver.settings = _settings(
            provider="provider-b",
            model="model-b",
            window=96_000,
            threshold=76_800,
        )
        with pytest.raises(RuntimeError, match="model provider conflicts"):
            await runtime._binding_for(
                _task(task_id="task-provider-b", model="model-b")
            )
        assert len(codex.thread_resume_calls) == 1

    asyncio.run(scenario())


def test_missing_new_model_metadata_cannot_reuse_old_model_context_override():
    async def scenario() -> None:
        resolver = _Resolver(_settings())
        runtime = CodexRuntime(
            codex=_Codex(),
            cwd="/workspace",
            model_context_resolver=resolver,
        )
        await runtime.start()
        await runtime._binding_for(_task())

        resolver.settings = ModelContextResolutionError("generic unavailable")
        with pytest.raises(RuntimeError, match="unavailable for the new model"):
            await runtime._binding_for(
                _task(task_id="task-b", model="model-b")
            )

    asyncio.run(scenario())


def test_public_config_reader_is_scoped_to_the_task_workspace():
    class ConfigCodex(_Codex):
        def __init__(self) -> None:
            super().__init__()
            self.config_calls: list[dict[str, Any]] = []

        async def config_read(self, **kwargs: Any) -> dict[str, Any]:
            self.config_calls.append(dict(kwargs))
            return {"config": {"model": "model-a"}}

    async def scenario() -> None:
        codex = ConfigCodex()
        runtime = CodexRuntime(codex=codex, cwd="/default")
        await runtime.start()
        result = await runtime._read_effective_config("/selected/project")
        assert result == {"config": {"model": "model-a"}}
        assert codex.config_calls == [
            {"cwd": "/selected/project", "include_layers": False}
        ]

    asyncio.run(scenario())


def test_default_child_local_resolver_queries_configured_exact_model(
    monkeypatch: pytest.MonkeyPatch,
):
    class ConfigCodex(_Codex):
        async def config_read(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "config": {
                    "model": "model-a",
                    "model_provider": "provider-a",
                    "model_providers": {
                        "provider-a": {
                            "base_url": "https://provider.example/v1",
                            "experimental_bearer_token": "in-child-secret",
                        }
                    },
                }
            }

    class Response:
        def raise_for_status(self) -> None:
            return None

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield (
                b'{"id":"model-a","metadata":'
                b'{"context_window":100000}}'
            )

        def close(self) -> None:
            return None

    requests: list[tuple[str, dict[str, Any]]] = []

    def get(url: str, **kwargs: Any) -> Response:
        requests.append((url, kwargs))
        return Response()

    monkeypatch.setattr(model_context.requests, "get", get)

    async def scenario() -> None:
        codex = ConfigCodex()
        runtime = CodexRuntime(codex=codex, cwd="/workspace")
        await runtime.start()
        binding = await runtime._binding_for(_task())

        assert binding.provider_id == "provider-a"
        assert binding.model_id == "model-a"
        assert codex.thread_start_calls[0]["config"] == _expected_config(
            100_000,
            80_000,
        )
        assert codex.thread_start_calls[0]["model_provider"] == "provider-a"

    asyncio.run(scenario())

    assert len(requests) == 1
    url, kwargs = requests[0]
    assert url == "https://provider.example/v1/models/model-a"
    assert kwargs["headers"]["Authorization"] == "Bearer in-child-secret"
    assert kwargs["allow_redirects"] is False
    assert kwargs["stream"] is True
