"""Hermetic config-layer and CodexRuntime tests for named Codex profiles."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

import src.agents.config_profile as config_profile_module
from src.agents.base import AgentTask
from src.agents.codex_runtime import CodexRuntime
from src.agents.config_profile import (
    CodexConfigProfileError,
    load_config_profile,
    require_profile_file,
)
from src.agents.model_context import ModelContextResolutionError
from src.runtime.roles import implicit_default_role


_QWEN_MODEL = "qwen3.8-27b"


def _write_layered_profiles(config_home: Path) -> None:
    config_home.mkdir(parents=True, exist_ok=True)
    (config_home / "config.toml").write_text(
        """
model = "base-model"
model_provider = "base-provider"
base_only = "retained"
tool_output_token_limit = 999999

[features]
unified_exec = true
base_feature = true

[model_providers.base-provider]
name = "Base provider"
base_url = "https://base.invalid/v1"
wire_api = "responses"

[model_providers.qwen]
name = "Qwen inherited name"
base_url = "https://old-qwen.invalid/v1"
wire_api = "responses"
env_key = "OLD_QWEN_TEST_KEY"

[model_providers.unselected]
name = "Must not cross the thread boundary"
base_url = "https://unselected.invalid/v1"
wire_api = "responses"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_home / "qwen.config.toml").write_text(
        f"""
model = "{_QWEN_MODEL}"
model_provider = "qwen"
named_only = "retained"

[features]
named_feature = true

[model_providers.qwen]
base_url = "https://qwen.invalid/v1"
env_key = "QWEN_HERMETIC_TEST_KEY"

[model_providers.named-extra]
name = "Also excluded from thread config"
base_url = "https://named-extra.invalid/v1"
wire_api = "responses"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_catalog_profile(config_home: Path) -> None:
    config_home.mkdir(parents=True, exist_ok=True)
    (config_home / "qwen.config.toml").write_text(
        f"""
model = "{_QWEN_MODEL}"
model_provider = "qwen"

[model_providers.qwen]
name = "Hermetic Qwen provider"
base_url = "https://provider.example/v1/"
wire_api = "responses"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _task(*, thread_id: str | None = None) -> AgentTask:
    return AgentTask(
        task_id="profile-task",
        agent_id="codex",
        conversation_id="profile-conversation",
        thread_id=thread_id,
        mode_id="chat",
        profile_version=1,
        policy_version=1,
        model="",
        metadata={"session_role": implicit_default_role()},
    )


class _UnavailableContextResolver:
    def __init__(self) -> None:
        self.calls: list[str | None] = []

    async def resolve(self, model_id: str | None = None) -> Any:
        self.calls.append(model_id)
        raise ModelContextResolutionError("no hermetic context metadata")


class _Thread:
    def __init__(self, thread_id: str) -> None:
        self.id = thread_id


class _RecordingCodex:
    def __init__(self) -> None:
        self.starts: list[dict[str, Any]] = []
        self.resumes: list[tuple[str, dict[str, Any]]] = []

    async def thread_start(self, **kwargs: Any) -> _Thread:
        self.starts.append(dict(kwargs))
        return _Thread("thread-created")

    async def thread_resume(
        self, thread_id: str, **kwargs: Any
    ) -> _Thread:
        self.resumes.append((thread_id, dict(kwargs)))
        return _Thread(thread_id)


class _CatalogCodex:
    def __init__(self, models: list[dict[str, Any]] | None = None) -> None:
        self.catalog = list(models or ())
        self.include_hidden_calls: list[bool] = []

    async def models(
        self, *, include_hidden: bool = False
    ) -> list[dict[str, Any]]:
        self.include_hidden_calls.append(include_hidden)
        return [dict(model) for model in self.catalog]


class _Response:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self.payload

    def close(self) -> None:
        self.closed = True


def test_loader_deep_merges_base_and_named_layer_and_fingerprints_both(
    tmp_path: Path,
) -> None:
    config_home = tmp_path / "codex-home"
    _write_layered_profiles(config_home)
    environ = {"CODEX_HOME": os.fspath(config_home)}

    loaded = load_config_profile("qwen", environ=environ)
    replay = load_config_profile("qwen", environ=environ)
    assert loaded is not None and replay is not None
    assert loaded.name == "qwen"
    assert loaded.model == _QWEN_MODEL
    assert loaded.model_provider == "qwen"
    assert loaded.fingerprint == replay.fingerprint
    assert loaded.effective_config["base_only"] == "retained"
    assert loaded.effective_config["named_only"] == "retained"
    assert loaded.effective_config["features"] == {
        "unified_exec": True,
        "base_feature": True,
        "named_feature": True,
    }
    assert set(loaded.effective_config["model_providers"]) == {
        "base-provider",
        "qwen",
        "unselected",
        "named-extra",
    }
    assert loaded.selected_provider_config() == {
        "name": "Qwen inherited name",
        "base_url": "https://qwen.invalid/v1",
        "wire_api": "responses",
        "env_key": "QWEN_HERMETIC_TEST_KEY",
    }
    assert loaded.thread_config() == {
        "model_providers": {"qwen": loaded.selected_provider_config()}
    }

    # The fingerprint covers source bytes, not merely the selected fields, so
    # a process restart cannot resume a binding after either layer changes.
    base_path = config_home / "config.toml"
    base_path.write_text(
        base_path.read_text(encoding="utf-8") + "# changed source bytes\n",
        encoding="utf-8",
    )
    changed = load_config_profile("qwen", environ=environ)
    assert changed is not None
    assert changed.effective_config == loaded.effective_config
    assert changed.fingerprint != loaded.fingerprint


@pytest.mark.parametrize(
    "name",
    (
        ".",
        "..",
        "../outside",
        "nested/profile",
        r"nested\profile",
        "/absolute",
        "qwen.config.toml",
        "white space",
    ),
)
def test_loader_rejects_path_shaped_profile_names(
    tmp_path: Path, name: str
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    environ = {"CODEX_HOME": os.fspath(config_home)}

    with pytest.raises(
        CodexConfigProfileError,
        match="Codex config profile must be a safe profile name",
    ):
        require_profile_file(name, environ=environ)
    with pytest.raises(CodexConfigProfileError):
        load_config_profile(name, environ=environ)


def test_loader_rejects_out_of_root_symlinks_directories_and_sensitive_toml_errors(
    tmp_path: Path,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    outside = tmp_path / "outside.config.toml"
    outside.write_text('model = "outside"\n', encoding="utf-8")
    (config_home / "escape.config.toml").symlink_to(outside)
    (config_home / "directory.config.toml").mkdir()
    sentinel = "HERMETIC-SENTINEL-MUST-NOT-LEAK"
    (config_home / "malformed.config.toml").write_text(
        f'api_key = "{sentinel}" trailing-invalid-text\n',
        encoding="utf-8",
    )
    environ = {"CODEX_HOME": os.fspath(config_home)}

    for name in ("escape", "directory"):
        with pytest.raises(CodexConfigProfileError) as captured:
            require_profile_file(name, environ=environ)
        assert os.fspath(tmp_path) not in str(captured.value)
        assert "outside" not in str(captured.value)

    # Existence validation is intentionally separate from child-local TOML
    # parsing.  The parser failure must never quote the sensitive source line.
    assert require_profile_file("malformed", environ=environ) == "malformed"
    with pytest.raises(CodexConfigProfileError) as captured:
        load_config_profile("malformed", environ=environ)
    assert str(captured.value) == "Codex config profile is invalid: malformed"
    assert sentinel not in str(captured.value)


@pytest.mark.parametrize(
    "contents",
    (
        'model_provider = "custom"\n[model_providers.custom]\n',
        'model = "model-a"\n[model_providers.custom]\n',
        'model = "model-a"\nmodel_provider = "custom"\n',
    ),
)
def test_loader_rejects_incomplete_named_provider_selection(
    tmp_path: Path,
    contents: str,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    (config_home / "qwen.config.toml").write_text(
        contents,
        encoding="utf-8",
    )

    with pytest.raises(CodexConfigProfileError) as captured:
        load_config_profile(
            "qwen",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )

    assert str(captured.value) == "Codex config profile is incomplete: qwen"


def test_loader_cannot_follow_out_of_root_symlink_swapped_after_validation(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    profile_path = config_home / "qwen.config.toml"
    profile_path.write_text(
        'model = "safe-model"\n'
        'model_provider = "safe-provider"\n'
        '[model_providers.safe-provider]\n'
        'base_url = "https://safe.invalid/v1"\n',
        encoding="utf-8",
    )
    outside = tmp_path / "outside.config.toml"
    outside.write_text(
        'model = "outside-model"\n'
        'model_provider = "outside-provider"\n'
        'synthetic_marker = "OUTSIDE-SYNTHETIC-MARKER"\n'
        '[model_providers.outside-provider]\n'
        'base_url = "https://outside.invalid/v1"\n',
        encoding="utf-8",
    )
    environ = {"CODEX_HOME": os.fspath(config_home)}
    original_check = config_profile_module._contained_regular_file
    swapped = False

    def validate_then_swap(
        root: Path,
        candidate: Path,
        *,
        label: str,
    ) -> Path:
        nonlocal swapped
        resolved = original_check(root, candidate, label=label)
        if not swapped and candidate.name == "qwen.config.toml":
            swapped = True
            candidate.unlink()
            candidate.symlink_to(outside)
        return resolved

    monkeypatch.setattr(
        config_profile_module,
        "_contained_regular_file",
        validate_then_swap,
    )

    try:
        loaded = load_config_profile("qwen", environ=environ)
    except CodexConfigProfileError:
        # Rejecting a path whose identity changed is the safest outcome.
        return

    # Reading from the already-validated descriptor is also safe: the
    # attacker-controlled replacement must never become the loaded layer.
    assert loaded is not None
    assert loaded.model == "safe-model"
    assert loaded.model_provider == "safe-provider"
    assert "synthetic_marker" not in loaded.effective_config


def test_thread_start_and_resume_receive_selected_provider_and_safety_config(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_layered_profiles(config_home)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        expected_provider = {
            "name": "Qwen inherited name",
            "base_url": "https://qwen.invalid/v1",
            "wire_api": "responses",
            "env_key": "QWEN_HERMETIC_TEST_KEY",
        }
        expected_config = {
            "model_providers": {"qwen": expected_provider},
            "features": {"unified_exec": False},
            "tool_output_token_limit": 500,
        }

        start_codex = _RecordingCodex()
        start_resolver = _UnavailableContextResolver()
        started_runtime = CodexRuntime(
            codex=start_codex,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
            model_context_resolver=start_resolver,
        )
        started = await started_runtime._binding_for(_task())

        resume_codex = _RecordingCodex()
        resume_resolver = _UnavailableContextResolver()
        resumed_runtime = CodexRuntime(
            codex=resume_codex,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
            model_context_resolver=resume_resolver,
        )
        resumed = await resumed_runtime._binding_for(
            _task(thread_id="thread-persisted")
        )

        assert started.thread_id == "thread-created"
        assert resumed.thread_id == "thread-persisted"
        assert start_resolver.calls == [_QWEN_MODEL]
        assert resume_resolver.calls == [_QWEN_MODEL]
        assert len(start_codex.starts) == 1
        assert len(resume_codex.resumes) == 1
        resumed_id, resume_kwargs = resume_codex.resumes[0]
        assert resumed_id == "thread-persisted"
        for kwargs in (start_codex.starts[0], resume_kwargs):
            assert kwargs["model"] == _QWEN_MODEL
            assert kwargs["model_provider"] == "qwen"
            assert kwargs["config"] == expected_config
            assert set(kwargs["config"]["model_providers"]) == {"qwen"}
            serialized = repr(kwargs["config"])
            assert "base-provider" not in serialized
            assert "unselected" not in serialized
            assert "named-extra" not in serialized
            assert "named_only" not in serialized
            assert "base_only" not in serialized
        assert started.model_id == _QWEN_MODEL
        assert resumed.model_id == _QWEN_MODEL
        assert started.provider_id == "qwen"
        assert resumed.provider_id == "qwen"
        assert started.context_config_fingerprint
        assert (
            started.context_config_fingerprint
            == resumed.context_config_fingerprint
        )

    asyncio.run(scenario())


def test_list_models_merges_and_casefold_deduplicates_provider_catalog_without_capabilities(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_catalog_profile(config_home)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
        responses: list[_Response] = []
        requests: list[tuple[str, dict[str, Any]]] = []

        def fake_get(url: str, **kwargs: Any) -> _Response:
            requests.append((url, kwargs))
            response = _Response(
                {
                    "data": [
                        {
                            "id": _QWEN_MODEL,
                            "supportedReasoningEfforts": ["ultra"],
                            "context_window": 999_999,
                            "input_modalities": ["text", "image"],
                        },
                        {
                            "id": _QWEN_MODEL.upper(),
                            "reasoning_efforts": ["high"],
                        },
                        {
                            "id": "provider-only",
                            "reasoning": {"efforts": ["max"]},
                            "context_length": 123_456,
                        },
                    ]
                }
            )
            responses.append(response)
            return response

        monkeypatch.setattr("src.agents.model_context.requests.get", fake_get)
        sdk = _CatalogCodex(
            [
                {
                    "id": "sdk-only",
                    "displayName": "SDK only",
                    # The SDK's base-provider default must not compete with
                    # the named profile's actual thread-start model.
                    "isDefault": True,
                    "supportedReasoningEfforts": ["high"],
                }
            ]
        )
        runtime = CodexRuntime(
            codex=sdk,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )
        models = await runtime.list_models(include_hidden=False)

        assert sdk.include_hidden_calls == [False]
        assert [model["id"] for model in models] == [
            "sdk-only",
            _QWEN_MODEL,
            "provider-only",
        ]
        assert len(requests) == 1
        url, request = requests[0]
        assert url == "https://provider.example/v1/models"
        assert request["allow_redirects"] is False
        assert request["stream"] is True
        assert request["headers"] == {"Accept": "application/json"}
        assert responses[0].closed

        sdk_model, qwen_model, provider_only = models
        assert sdk_model["supportedReasoningEfforts"] == ["high"]
        assert sdk_model["isDefault"] is False
        for provider_model in (qwen_model, provider_only):
            assert provider_model["supportedReasoningEfforts"] == []
            assert set(provider_model) == {
                "id",
                "displayName",
                "isDefault",
                "supportedReasoningEfforts",
            }
        assert qwen_model["isDefault"] is True
        assert provider_only["isDefault"] is False

    asyncio.run(scenario())


def test_list_models_retains_profile_default_when_provider_omits_it(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_catalog_profile(config_home)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))

        def fake_get(_url: str, **_kwargs: Any) -> _Response:
            return _Response(
                {
                    "data": [
                        {
                            "id": "provider-only",
                            "supportedReasoningEfforts": ["ultra"],
                        }
                    ]
                }
            )

        monkeypatch.setattr("src.agents.model_context.requests.get", fake_get)
        sdk = _CatalogCodex(
            [
                {
                    "id": "PROVIDER-ONLY",
                    "displayName": "SDK provider-only",
                    "isDefault": False,
                    "supportedReasoningEfforts": ["medium"],
                }
            ]
        )
        runtime = CodexRuntime(
            codex=sdk,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )
        models = await runtime.list_models()

        # The SDK row wins the case-insensitive duplicate, while the explicit
        # profile model is added conservatively when discovery omitted it.
        assert [model["id"] for model in models] == [
            "PROVIDER-ONLY",
            _QWEN_MODEL,
        ]
        assert models[0]["supportedReasoningEfforts"] == ["medium"]
        assert models[1] == {
            "id": _QWEN_MODEL,
            "displayName": _QWEN_MODEL,
            "isDefault": True,
            "supportedReasoningEfforts": [],
        }

    asyncio.run(scenario())


def test_profile_model_is_the_only_default_when_sdk_has_unrelated_default(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_catalog_profile(config_home)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))

        def fake_get(_url: str, **_kwargs: Any) -> _Response:
            return _Response({"data": [{"id": _QWEN_MODEL}]})

        monkeypatch.setattr("src.agents.model_context.requests.get", fake_get)
        runtime = CodexRuntime(
            codex=_CatalogCodex(
                [
                    {
                        "id": "sdk-default",
                        "displayName": "SDK default",
                        "isDefault": True,
                        "supportedReasoningEfforts": ["medium"],
                    }
                ]
            ),
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )

        models = await runtime.list_models(include_hidden=False)

        assert [model["id"] for model in models] == [
            "sdk-default",
            _QWEN_MODEL,
        ]
        assert [model["id"] for model in models if model["isDefault"]] == [
            _QWEN_MODEL
        ]

    asyncio.run(scenario())


def test_provider_catalog_honors_include_hidden_for_merged_models(
    tmp_path: Path, monkeypatch: Any
) -> None:
    async def scenario() -> None:
        config_home = tmp_path / "codex-home"
        _write_catalog_profile(config_home)
        monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))

        def fake_get(_url: str, **_kwargs: Any) -> _Response:
            return _Response(
                {
                    "data": [
                        {"id": _QWEN_MODEL, "hidden": False},
                        {"id": "provider-visible", "hidden": False},
                        {"id": "provider-hidden", "hidden": True},
                    ]
                }
            )

        monkeypatch.setattr("src.agents.model_context.requests.get", fake_get)
        sdk = _CatalogCodex()
        runtime = CodexRuntime(
            codex=sdk,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )

        visible = await runtime.list_models(include_hidden=False)
        including_hidden = await runtime.list_models(include_hidden=True)

        assert sdk.include_hidden_calls == [False, True]
        assert [model["id"] for model in visible] == [
            _QWEN_MODEL,
            "provider-visible",
        ]
        assert [model["id"] for model in including_hidden] == [
            _QWEN_MODEL,
            "provider-visible",
            "provider-hidden",
        ]

    asyncio.run(scenario())
