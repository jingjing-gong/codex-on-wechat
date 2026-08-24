"""Provider model-context discovery and fallback security tests."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
import threading
import time

import pytest

from src.agents.model_context import (
    MAX_CONTEXT_WINDOW,
    MAX_MODEL_COLLECTION_ITEMS,
    MIN_CONTEXT_WINDOW,
    ModelContextResolutionError,
    ProviderModelContextResolver,
)


class _Response:
    def __init__(self, payload, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error

    def raise_for_status(self) -> None:
        if self.error is not None:
            raise self.error

    def json(self):
        return self.payload


class _StreamingResponse:
    def __init__(self, chunks: list[bytes], *, content_length: int | None = None) -> None:
        self.chunks = chunks
        self.headers = (
            {"Content-Length": str(content_length)}
            if content_length is not None
            else {}
        )
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int):
        assert chunk_size == 64 * 1024
        yield from self.chunks

    def json(self):
        raise AssertionError("streaming responses must be decoded from bounded bytes")

    def close(self) -> None:
        self.closed = True


def test_detail_metadata_uses_effective_provider_auth_and_thread_transport():
    main_thread = threading.get_ident()
    reads = 0
    requests = []

    async def read_config():
        nonlocal reads
        reads += 1
        return {
            "config": {
                "model": "acme/model v1",
                "model_provider": "custom",
                "model_providers": {
                    "custom": {
                        "base_url": "https://provider.example/v1/",
                        "env_key": "ACME_API_KEY",
                        "http_headers": {"X-Fixed": "fixed"},
                        "env_http_headers": {"X-Organization": "ACME_ORG"},
                        "query_params": {
                            "api-version": "2026-08-17",
                            "access": "query-secret",
                        },
                    }
                },
            }
        }

    def get(url, **kwargs):
        requests.append((url, kwargs, threading.get_ident()))
        return _Response(
            {
                "id": "acme/model v1",
                "model_info": {"contextWindow": 262_144},
                "limits": {"autoCompactTokenLimit": 180_000},
            }
        )

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={"ACME_API_KEY": "bearer-secret", "ACME_ORG": "org-a"},
        )
        settings = await resolver.resolve()

        assert settings.provider_id == "custom"
        assert settings.model_id == "acme/model v1"
        assert settings.context_window == 262_144
        assert settings.model_auto_compact_token_limit == 180_000
        assert settings.source == "provider:model-detail"
        assert settings.as_config_overrides() == {
            "model_context_window": 262_144,
            "model_auto_compact_token_limit": 180_000,
            "model_auto_compact_token_limit_scope": "total",
        }
        with pytest.raises(FrozenInstanceError):
            settings.context_window = 1  # type: ignore[misc]

    asyncio.run(scenario())

    assert reads == 1
    assert len(requests) == 1
    url, kwargs, request_thread = requests[0]
    assert url == "https://provider.example/v1/models/acme%2Fmodel%20v1"
    assert request_thread != main_thread
    assert kwargs == {
        "headers": {
            "Accept": "application/json",
            "X-Fixed": "fixed",
            "X-Organization": "org-a",
            "Authorization": "Bearer bearer-secret",
        },
        "params": {
            "api-version": "2026-08-17",
            "access": "query-secret",
        },
        "timeout": 10.0,
        "allow_redirects": False,
        "stream": True,
    }


@pytest.mark.parametrize(
    ("requires_openai_auth", "expected_authorization"),
    (
        (True, "Bearer openai-secret"),
        (False, None),
        ("true", None),
        ("false", None),
        (1, None),
    ),
)
def test_openai_key_requires_an_explicit_boolean_authorization_grant(
    requires_openai_auth,
    expected_authorization,
):
    requests = []

    async def read_config():
        return {
            "model": "model-a",
            "model_provider": "custom",
            "model_providers": {
                "custom": {
                    "base_url": "https://provider.example/v1",
                    "requires_openai_auth": requires_openai_auth,
                }
            },
        }

    def get(_url, **kwargs):
        requests.append(kwargs)
        return _Response({"id": "model-a", "context_window": 128_000})

    async def scenario() -> None:
        settings = await ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={"OPENAI_API_KEY": "openai-secret"},
        ).resolve()
        assert settings.context_window == 128_000

    asyncio.run(scenario())

    assert requests
    assert requests[0]["headers"].get("Authorization") == expected_authorization


def test_detail_without_metadata_falls_back_to_exact_collection_model():
    calls = []

    async def read_config():
        return {
            "model": "model-2",
            "modelProvider": "provider-a",
            "modelProviders": {
                "provider-a": {"baseUrl": "http://localhost:8317/v1"}
            },
        }

    def get(url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith("/models/model-2"):
            return _Response({"id": "model-2", "object": "model"})
        return _Response(
            {
                "object": "list",
                "data": [
                    {"id": "MODEL-2", "context_length": 999_999},
                    {
                        "id": "model-2",
                        "capabilities": {"max_model_len": 128_000},
                    },
                ],
            }
        )

    async def scenario() -> None:
        settings = await ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={},
        ).resolve()
        assert settings.context_window == 128_000
        assert settings.model_auto_compact_token_limit == 102_400
        assert settings.source == "provider:model-list"

    asyncio.run(scenario())
    assert [call[0] for call in calls] == [
        "http://localhost:8317/v1/models/model-2",
        "http://localhost:8317/v1/models",
    ]


def test_effective_experimental_bearer_token_is_supported_without_exposure():
    observed_headers = []
    responses = []

    async def read_config():
        return {
            "config": {
                "model": "model-a",
                "model_provider": "provider-a",
                "experimental_bearer_token": "experimental-secret",
                "model_providers": {
                    "provider-a": {"base_url": "https://provider.example/v1"}
                },
            }
        }

    def get(_url, **kwargs):
        observed_headers.append(kwargs["headers"])
        response = _StreamingResponse(
            [b'{"id":"model-a","context_window":100000}'],
            content_length=40,
        )
        responses.append(response)
        return response

    async def scenario() -> None:
        settings = await ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={},
        ).resolve()
        assert settings.model_auto_compact_token_limit == 80_000
        assert "experimental-secret" not in repr(settings)

    asyncio.run(scenario())
    assert observed_headers == [
        {
            "Accept": "application/json",
            "Authorization": "Bearer experimental-secret",
        }
    ]
    assert responses[0].closed


def test_effective_config_fallback_is_model_scoped_and_cache_fingerprinted():
    config = {
        "model": "configured-model",
        "model_provider": "provider-a",
        "model_context_window": 100_000,
        "model_auto_compact_token_limit": 70_000,
        "model_providers": {
            "provider-a": {"base_url": "https://provider.example/v1"}
        },
    }
    calls = []

    async def read_config():
        return dict(config)

    def get(url, **_kwargs):
        calls.append(url)
        if "/models/" in url:
            return _Response({"id": url.rsplit("/", 1)[-1], "object": "model"})
        return _Response({"object": "list", "data": []})

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            default_fallback=50_000,
            http_get=get,
            environ={},
        )
        first = await resolver.resolve()
        assert (
            first.context_window,
            first.model_auto_compact_token_limit,
            first.source,
        ) == (100_000, 70_000, "fallback:effective-config")
        assert await resolver.resolve() is first
        assert len(calls) == 2

        # A validated config fallback change is part of the cache key.
        config["model_context_window"] = 200_000
        config.pop("model_auto_compact_token_limit")
        changed = await resolver.resolve()
        assert (
            changed.context_window,
            changed.model_auto_compact_token_limit,
            changed.source,
        ) == (200_000, 160_000, "fallback:effective-config")
        assert len(calls) == 4

        # The selected model's config window must not leak to another model.
        other = await resolver.resolve("other-model")
        assert (
            other.context_window,
            other.model_auto_compact_token_limit,
            other.source,
        ) == (50_000, 40_000, "fallback:default")

    asyncio.run(scenario())


def test_cache_isolated_by_canonical_provider_base_url():
    config = {
        "model": "same-model",
        "model_provider": "same-provider",
        "model_providers": {
            "same-provider": {"base_url": "HTTPS://ONE.example:443/v1/"}
        },
    }
    calls = []

    async def read_config():
        return config

    def get(url, **_kwargs):
        calls.append(url)
        window = 100_000 if "one.example" in url else 200_000
        return _Response({"id": "same-model", "context_window": window})

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={},
        )
        first = await resolver.resolve()
        assert first.context_window == 100_000

        # A trailing slash difference canonicalizes to the same cache key.
        config["model_providers"]["same-provider"]["base_url"] = (
            "https://one.example/v1"
        )
        assert await resolver.resolve() is first
        assert len(calls) == 1

        config["model_providers"]["same-provider"]["base_url"] = (
            "https://two.example/v1"
        )
        second = await resolver.resolve()
        assert second.context_window == 200_000
        assert second is not first
        assert len(calls) == 2

    asyncio.run(scenario())


def test_cache_key_separates_provider_model_and_base_url_dimensions():
    config = {
        "model": "model-a",
        "model_provider": "provider-a",
        "model_providers": {
            "provider-a": {"base_url": "https://shared.example/v1"},
            "provider-b": {"base_url": "https://shared.example/v1"},
        },
    }
    calls = []

    async def read_config():
        return config

    def get(url, **_kwargs):
        calls.append(url)
        model = url.rsplit("/", 1)[-1]
        return _Response({"id": model, "context_window": 100_000 + len(calls)})

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={},
        )
        results = [await resolver.resolve()]

        config["model_provider"] = "provider-b"
        results.append(await resolver.resolve())

        config["model"] = "model-b"
        results.append(await resolver.resolve())

        config["model_providers"]["provider-b"]["base_url"] = (
            "https://other.example/v1"
        )
        results.append(await resolver.resolve())

        assert len({id(result) for result in results}) == 4
        assert [result.context_window for result in results] == [
            100_001,
            100_002,
            100_003,
            100_004,
        ]

    asyncio.run(scenario())
    assert len(calls) == 4


def test_missing_or_invalid_metadata_uses_only_explicit_fallbacks():
    calls = []

    async def read_config():
        return {
            "model_provider": "provider-a",
            "model_providers": {
                "provider-a": {"base_url": "https://provider.example/v1"}
            },
        }

    def get(url, **_kwargs):
        calls.append(url)
        if "/models/" in url:
            # max_output_tokens is deliberately not a context-window field.
            return _Response(
                {"id": url.rsplit("/", 1)[-1], "max_output_tokens": 1_000_000}
            )
        model_id = "known" if len(calls) == 2 else "unknown"
        return _Response(
            {
                "data": [
                    {
                        "id": model_id,
                        # Strings are not accepted as provider integer metadata.
                        "context_window": "262144",
                    }
                ]
            }
        )

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            model_fallbacks={
                    "known": {
                        "context_window": 131_072,
                        "model_auto_compact_token_limit": 100_000,
                }
            },
            default_fallback=64_000,
            http_get=get,
            environ={},
        )

        known = await resolver.resolve("known")
        assert (
            known.context_window,
            known.model_auto_compact_token_limit,
            known.source,
        ) == (131_072, 100_000, "fallback:model")

        unknown = await resolver.resolve("unknown")
        assert (
            unknown.context_window,
            unknown.model_auto_compact_token_limit,
            unknown.source,
        ) == (64_000, 51_200, "fallback:default")

    asyncio.run(scenario())
    assert len(calls) == 4


def test_conflicting_or_out_of_bounds_provider_values_never_escape_validation():
    async def read_config():
        return {
            "model": "conflict",
            "model_provider": "provider-a",
            "model_providers": {
                "provider-a": {"base_url": "https://provider.example/v1"}
            },
        }

    def get(url, **_kwargs):
        if "/models/" in url:
            return _Response(
                {
                    "id": "conflict",
                    "context_window": MIN_CONTEXT_WINDOW - 1,
                }
            )
        return _Response(
            {
                "data": [
                    {
                        "id": "conflict",
                        "context_window": 128_000,
                        "limits": {"context_length": MAX_CONTEXT_WINDOW},
                    }
                ]
            }
        )

    async def scenario() -> None:
        settings = await ProviderModelContextResolver(
            read_config,
            default_fallback={
                "context_window": 96_000,
                "compact_threshold": 80_000,
            },
            http_get=get,
            environ={},
        ).resolve()
        assert settings.context_window == 96_000
        assert settings.model_auto_compact_token_limit == 76_800
        assert settings.source == "fallback:default"

    asyncio.run(scenario())


def test_provider_failures_are_sanitized_when_no_fallback_exists():
    secret = "provider-secret-in-exception"
    calls = 0
    clock_value = [100.0]

    async def read_config():
        return {
            "model": "model-a",
            "model_provider": "provider-a",
            "model_providers": {
                "provider-a": {
                    "base_url": "https://provider.example/v1",
                    "query_params": {"api-key": "query-secret"},
                    "env_key": "PROVIDER_KEY",
                }
            },
        }

    def get(_url, **_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError(secret)

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            ttl_seconds=1_000,
            http_get=get,
            environ={"PROVIDER_KEY": "header-secret"},
            clock=lambda: clock_value[0],
        )
        with pytest.raises(ModelContextResolutionError) as captured:
            await resolver.resolve()
        rendered = str(captured.value)
        assert secret not in rendered
        assert "query-secret" not in rendered
        assert "header-secret" not in rendered
        assert captured.value.__cause__ is None

        # A sanitized miss is cached without preserving the secret exception.
        with pytest.raises(ModelContextResolutionError) as cached:
            await resolver.resolve()
        assert str(cached.value) == rendered
        assert calls == 2

        clock_value[0] = 401.0
        with pytest.raises(ModelContextResolutionError):
            await resolver.resolve()
        assert calls == 4

    asyncio.run(scenario())


def test_cache_is_async_safe_and_expires_by_provider_and_model():
    reads = 0
    calls = 0
    clock_value = [100.0]

    async def read_config():
        nonlocal reads
        reads += 1
        return {
            "model": "cached-model",
            "model_provider": "provider-a",
            "model_providers": {
                "provider-a": {"base_url": "https://provider.example/v1"}
            },
        }

    def get(_url, **_kwargs):
        nonlocal calls
        calls += 1
        time.sleep(0.02)
        return _Response({"id": "cached-model", "context_window": 128_000})

    async def scenario() -> None:
        resolver = ProviderModelContextResolver(
            read_config,
            ttl_seconds=10,
            http_get=get,
            environ={},
            clock=lambda: clock_value[0],
        )
        first, concurrent = await asyncio.gather(resolver.resolve(), resolver.resolve())
        assert first is concurrent
        assert calls == 1

        cached = await resolver.resolve()
        assert cached is first
        assert calls == 1

        clock_value[0] = 111.0
        refreshed = await resolver.resolve()
        assert refreshed is not first
        assert calls == 2

        resolver.clear_cache(provider_id="provider-a", model_id="cached-model")
        await resolver.resolve()
        assert calls == 3

    asyncio.run(scenario())
    assert reads == 5


def test_response_bytes_and_model_catalog_traversal_are_bounded():
    config = {
        "model": "late-model",
        "model_provider": "provider-a",
        "model_providers": {
            "provider-a": {"base_url": "https://provider.example/v1"}
        },
    }

    async def read_config():
        return config

    oversized_responses = []

    def oversized_get(_url, **_kwargs):
        response = _StreamingResponse([b"x" * 1_025])
        oversized_responses.append(response)
        return response

    async def scenario() -> None:
        byte_bounded = ProviderModelContextResolver(
            read_config,
            default_fallback=50_000,
            max_response_bytes=1_024,
            http_get=oversized_get,
            environ={},
        )
        settings = await byte_bounded.resolve()
        assert settings.source == "fallback:default"
        assert len(oversized_responses) == 2
        assert all(response.closed for response in oversized_responses)

        calls = 0

        def catalog_get(url, **_kwargs):
            nonlocal calls
            calls += 1
            if "/models/" in url:
                return _Response({"id": "late-model", "object": "model"})
            data = [
                {"id": f"unrelated-{ordinal}", "context_window": 100_000}
                for ordinal in range(MAX_MODEL_COLLECTION_ITEMS)
            ]
            data.append({"id": "late-model", "context_window": 999_999})
            return _Response({"data": data})

        traversal_bounded = ProviderModelContextResolver(
            read_config,
            default_fallback=60_000,
            http_get=catalog_get,
            environ={},
        )
        bounded = await traversal_bounded.resolve()
        assert bounded.context_window == 60_000
        assert bounded.source == "fallback:default"
        assert calls == 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kwargs",
    (
        {"default_fallback": MIN_CONTEXT_WINDOW - 1},
        {"default_fallback": MAX_CONTEXT_WINDOW + 1},
        {"default_fallback": True},
        {
            "default_fallback": {
                "context_window": 128_000,
                "compact_threshold": 128_000,
            }
        },
        {"ttl_seconds": 0},
        {"request_timeout": float("inf")},
        {"max_response_bytes": 1_023},
    ),
)
def test_constructor_rejects_invalid_bounds(kwargs):
    async def read_config():
        return {}

    with pytest.raises(ValueError):
        ProviderModelContextResolver(read_config, **kwargs)
