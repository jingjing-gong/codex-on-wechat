"""Adversarial public-catalog regressions for named config profiles."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.agents.codex_runtime import CodexRuntime


def _envelope(text: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id="catalog-security",
        text=text,
        session_id="default",
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
    )


_SENSITIVE_MODEL = (
    "https://synthetic-profile.invalid/v1/models?"
    "api_key=SYNTHETIC-URL-CREDENTIAL"
)
_SENSITIVE_EFFORT = "Bearer SYNTHETIC-BEARER-CREDENTIAL"
_SENSITIVE_ASSIGNMENT = "password=SYNTHETIC-ASSIGNMENT-CREDENTIAL"
_SENSITIVE_HOST = (
    "synthetic-catalog.invalid:9443/private/SYNTHETIC-HOST-PATH"
)
_HOSTILE_MARKDOWN = "visible`\n## SYNTHETIC-INJECTED-HEADING"


@pytest.mark.parametrize(
    ("records", "configured_model", "configured_effort", "unavailable"),
    (
        # Empty provider/SDK catalog with a durable explicit selection.
        ((), _SENSITIVE_MODEL, _SENSITIVE_EFFORT, True),
        # Nonempty catalog with a durable selection that is no longer listed.
        (
            ({"id": "safe-model", "isDefault": True},),
            _SENSITIVE_MODEL,
            _SENSITIVE_EFFORT,
            True,
        ),
        # Nonempty catalog without a default: the runtime default is opaque.
        (
            ({"id": "safe-model", "isDefault": False},),
            "",
            _SENSITIVE_ASSIGNMENT,
            True,
        ),
        # Exact live selection. Raw identity matching must precede redaction.
        (
            (
                {
                    "id": _SENSITIVE_MODEL,
                    "displayName": _SENSITIVE_ASSIGNMENT,
                    "isDefault": True,
                    "supportedReasoningEfforts": (
                        _SENSITIVE_EFFORT,
                        _SENSITIVE_HOST,
                        _HOSTILE_MARKDOWN,
                    ),
                    "defaultReasoningEffort": _SENSITIVE_HOST,
                },
            ),
            _SENSITIVE_MODEL,
            _SENSITIVE_EFFORT,
            False,
        ),
        # Runtime-selected default with no durable model override.
        (
            (
                {
                    "id": _SENSITIVE_MODEL,
                    "displayName": _SENSITIVE_ASSIGNMENT,
                    "isDefault": True,
                    "supportedReasoningEfforts": (_SENSITIVE_EFFORT,),
                    "defaultReasoningEffort": _SENSITIVE_HOST,
                },
            ),
            "",
            "",
            False,
        ),
    ),
)
def test_models_all_selection_branches_redact_catalog_credentials(
    records: tuple[dict[str, Any], ...],
    configured_model: str,
    configured_effort: str,
    unavailable: bool,
) -> None:
    class Manager:
        async def get_active_agent(self, **_kwargs: Any) -> str:
            return "planner"

        async def list_models(self, **_kwargs: Any) -> tuple[dict[str, Any], ...]:
            return records

        async def get_model_selection(self, **_kwargs: Any) -> dict[str, str]:
            return {
                "model_id": configured_model,
                "reasoning_effort": configured_effort,
            }

    async def scenario() -> None:
        rendered = str(
            await MVPCommandRouter(Manager()).handle_command(
                parse_command("/models"), _envelope("/models")
            )
        )

        assert rendered.count("**(current)**") == 1
        assert ("**(unavailable)**" in rendered) is unavailable
        assert "https://" not in rendered
        assert "synthetic-profile.invalid" not in rendered
        assert "SYNTHETIC-URL-CREDENTIAL" not in rendered
        assert "SYNTHETIC-BEARER-CREDENTIAL" not in rendered
        assert "SYNTHETIC-ASSIGNMENT-CREDENTIAL" not in rendered
        assert "synthetic-catalog.invalid" not in rendered
        assert "SYNTHETIC-HOST-PATH" not in rendered
        assert "\n## SYNTHETIC-INJECTED-HEADING" not in rendered
        assert "<redacted" in rendered

    asyncio.run(scenario())


def _write_qwen_profile(config_home: Path) -> None:
    config_home.mkdir()
    (config_home / "qwen.config.toml").write_text(
        'model = "qwen3.8-27b"\n'
        'model_provider = "qwen"\n'
        '[model_providers.qwen]\n'
        'base_url = "https://synthetic-provider.invalid/v1"\n',
        encoding="utf-8",
    )


class _CatalogSdk:
    def __init__(self, records: tuple[dict[str, Any], ...] = ()) -> None:
        self.records = records
        self.include_hidden_calls: list[bool] = []

    async def models(self, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        self.include_hidden_calls.append(include_hidden)
        return [dict(record) for record in self.records]


class _JsonResponse:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self.payload

    def close(self) -> None:
        self.closed = True


def test_provider_catalog_failure_is_best_effort_and_profile_default_wins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_home = tmp_path / "codex-home"
    _write_qwen_profile(config_home)
    monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
    secret = "SYNTHETIC-PROVIDER-FAILURE-CREDENTIAL"

    def failed_get(_url: str, **_kwargs: Any) -> Any:
        raise RuntimeError(f"Authorization: Bearer {secret}")

    monkeypatch.setattr("src.agents.model_context.requests.get", failed_get)
    sdk = _CatalogSdk(
        (
            {
                "id": "sdk-default",
                "isDefault": True,
                "supportedReasoningEfforts": ["high"],
            },
        )
    )

    async def scenario() -> None:
        runtime = CodexRuntime(
            codex=sdk,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )
        records = await runtime.list_models(include_hidden=False)

        assert sdk.include_hidden_calls == [False]
        assert [record["id"] for record in records] == [
            "sdk-default",
            "qwen3.8-27b",
        ]
        assert [record["isDefault"] for record in records] == [False, True]

    asyncio.run(scenario())
    assert secret not in caplog.text


def test_provider_hidden_aliases_honor_flag_and_preserve_one_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_home = tmp_path / "codex-home"
    _write_qwen_profile(config_home)
    monkeypatch.setenv("CODEX_HOME", os.fspath(config_home))
    responses: list[_JsonResponse] = []

    def fake_get(_url: str, **_kwargs: Any) -> _JsonResponse:
        response = _JsonResponse(
            {
                "data": [
                    {"id": "qwen3.8-27b", "is_hidden": "false"},
                    {"id": "provider-visible", "hidden": 0},
                    {"id": "provider-hidden-a", "isHidden": True},
                    {"id": "provider-hidden-b", "is_hidden": "yes"},
                    {"id": "provider-hidden-c", "hidden": 1},
                ]
            }
        )
        responses.append(response)
        return response

    monkeypatch.setattr("src.agents.model_context.requests.get", fake_get)
    sdk = _CatalogSdk(
        (
            {"id": "sdk-only", "isDefault": True},
            # Case-insensitive deduplication must retain the SDK row while
            # still marking the profile's exact effective identity default.
            {"id": "QWEN3.8-27B", "isDefault": False},
        )
    )

    async def scenario() -> None:
        runtime = CodexRuntime(
            codex=sdk,
            cwd=os.fspath(tmp_path),
            codex_config_profile="qwen",
        )
        visible = await runtime.list_models(include_hidden=False)
        including_hidden = await runtime.list_models(include_hidden=True)

        assert [record["id"] for record in visible] == [
            "sdk-only",
            "QWEN3.8-27B",
            "provider-visible",
        ]
        assert [record["id"] for record in including_hidden] == [
            "sdk-only",
            "QWEN3.8-27B",
            "provider-visible",
            "provider-hidden-a",
            "provider-hidden-b",
            "provider-hidden-c",
        ]
        for records in (visible, including_hidden):
            defaults = [record for record in records if record["isDefault"]]
            assert [record["id"] for record in defaults] == ["QWEN3.8-27B"]
        assert sdk.include_hidden_calls == [False, True]
        assert all(response.closed for response in responses)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "sensitive_value",
    (
        # Runtime/SDK rows are not constrained to HTTP provider URLs. URI
        # user-info must not survive merely because the scheme is unfamiliar.
        (
            "custom-provider://synthetic-user:"
            "SYNTHETIC-URI-USERINFO-CREDENTIAL@catalog.invalid/v1"
        ),
        # C0 controls that are not whitespace can split a credential label
        # without being removed by ``str.split`` normalization.
        "api_\x1bkey=SYNTHETIC-CONTROL-OBFUSCATED-CREDENTIAL",
        "Bearer [SYNTHETIC-WRAPPED-AUTH-CREDENTIAL]",
        "Basic: SYNTHETIC-COLON-AUTH-CREDENTIAL",
        "refreshToken=SYNTHETIC-CAMEL-TOKEN-CREDENTIAL",
        "clientSecret=SYNTHETIC-CAMEL-SECRET-CREDENTIAL",
    ),
)
def test_models_malformed_catalog_values_cannot_bypass_credential_redaction(
    sensitive_value: str,
) -> None:
    class Manager:
        async def get_active_agent(self, **_kwargs: Any) -> str:
            return "planner"

        async def list_models(self, **_kwargs: Any) -> tuple[dict[str, Any], ...]:
            return (
                {
                    "id": sensitive_value,
                    "displayName": sensitive_value,
                    "isDefault": True,
                    "supportedReasoningEfforts": (sensitive_value,),
                },
            )

        async def get_model_selection(self, **_kwargs: Any) -> dict[str, str]:
            return {"model_id": sensitive_value, "reasoning_effort": ""}

    async def scenario() -> None:
        rendered = str(
            await MVPCommandRouter(Manager()).handle_command(
                parse_command("/models"), _envelope("/models")
            )
        )

        assert rendered.count("**(current)**") == 1
        assert "SYNTHETIC-URI-USERINFO-CREDENTIAL" not in rendered
        assert "SYNTHETIC-CONTROL-OBFUSCATED-CREDENTIAL" not in rendered
        assert "SYNTHETIC-WRAPPED-AUTH-CREDENTIAL" not in rendered
        assert "SYNTHETIC-COLON-AUTH-CREDENTIAL" not in rendered
        assert "SYNTHETIC-CAMEL-TOKEN-CREDENTIAL" not in rendered
        assert "SYNTHETIC-CAMEL-SECRET-CREDENTIAL" not in rendered
        assert "\x1b" not in rendered
        assert "<redacted" in rendered

    asyncio.run(scenario())
