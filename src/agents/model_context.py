"""Resolve provider-advertised, model-specific Codex context settings.

OpenAI-compatible model catalogs do not have a standard context-window field.
This module therefore accepts only an explicit allowlist of common provider
extensions, requires an exact model match for collection responses, and falls
back only to explicit effective configuration or caller-supplied settings.  It
never derives a context window from a model name or an output-token field.

The resolver is intentionally independent of :class:`CodexRuntime`.  A child
runtime can inject its effective ``config/read`` callable and use the returned
immutable settings as thread configuration overrides.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import json
import math
import os
import re
import time
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import quote, urlsplit, urlunsplit

import requests


MIN_CONTEXT_WINDOW = 4_096
MAX_CONTEXT_WINDOW = 10_000_000
DEFAULT_CACHE_TTL_SECONDS = 3_600.0
MAX_NEGATIVE_CACHE_TTL_SECONDS = 300.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MIN_RESPONSE_BYTES = 1_024
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_MODEL_TRAVERSAL_NODES = 10_000
MAX_MODEL_COLLECTION_ITEMS = 5_000
MAX_METADATA_FIELDS = 512
AUTO_COMPACT_WINDOW_RATIO = 0.80


class ModelContextResolutionError(RuntimeError):
    """Raised when neither provider metadata nor an explicit fallback works."""


@dataclass(frozen=True, slots=True)
class ModelContextFallback:
    """One explicit model/default fallback supplied by the embedding."""

    context_window: int
    model_auto_compact_token_limit: int | None = None


@dataclass(frozen=True, slots=True)
class ModelContextSettings:
    """Validated immutable settings ready for Codex thread injection."""

    provider_id: str
    model_id: str
    context_window: int
    model_auto_compact_token_limit: int
    source: str

    def as_config_overrides(self) -> dict[str, int | str]:
        """Return a fresh Codex-compatible thread configuration mapping."""

        return {
            "model_context_window": self.context_window,
            "model_auto_compact_token_limit": self.model_auto_compact_token_limit,
            "model_auto_compact_token_limit_scope": "total",
        }


ConfigReader = Callable[[], Awaitable[Any]]
HttpGet = Callable[..., Any]
FallbackValue = ModelContextFallback | ModelContextSettings | Mapping[str, Any] | int
CacheKey = tuple[str, str, str, tuple[int, int] | None]


_CONTEXT_FIELD_KEYS = frozenset(
    {
        "contextwindow",
        "contextlength",
        "maxcontextlength",
        "maxmodellength",
        "maxmodellen",
        "maxinputtokens",
        "inputtokenlimit",
        "modelcontextwindow",
    }
)
_COMPACT_FIELD_KEYS = frozenset(
    {
        "modelautocompacttokenlimit",
        "autocompacttokenlimit",
        "compactthreshold",
        "compactionthreshold",
        "autocompactthreshold",
    }
)
_NESTED_METADATA_KEYS = frozenset(
    {
        "modelinfo",
        "limits",
        "capabilities",
        "metadata",
        "context",
        "tokenlimits",
        "architecture",
    }
)
_COLLECTION_KEYS = frozenset({"data", "models", "items", "result", "model"})
_MODEL_ID_KEYS = ("id", "model_id", "modelId", "model", "name")
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _canonical_key(value: Any) -> str:
    return "".join(character for character in str(value) if character.isalnum()).lower()


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    dumper = getattr(value, "model_dump", None)
    if callable(dumper):
        for kwargs in (
            {"mode": "python", "by_alias": False},
            {"mode": "python"},
            {},
        ):
            try:
                dumped = dumper(**kwargs)
            except (TypeError, ValueError):
                continue
            if isinstance(dumped, Mapping):
                return dumped
    attributes = getattr(value, "__dict__", None)
    return attributes if isinstance(attributes, Mapping) else None


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    return None


def _strict_int(value: Any) -> int | None:
    # Provider JSON must advertise a real integer.  Reject bool, integral
    # floats, and numeric strings instead of accepting ambiguous coercions.
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _valid_window(value: Any) -> int | None:
    resolved = _strict_int(value)
    if resolved is None or not MIN_CONTEXT_WINDOW <= resolved <= MAX_CONTEXT_WINDOW:
        return None
    return resolved


def _valid_threshold(value: Any, *, context_window: int) -> int | None:
    resolved = _strict_int(value)
    if resolved is None or not 0 < resolved < context_window:
        return None
    return resolved


def _native_auto_compact_threshold(
    context_window: int,
    advertised: Any = None,
) -> int:
    """Return Codex's explicit total-token threshold for a known window."""

    baseline = math.floor(context_window * AUTO_COMPACT_WINDOW_RATIO)
    advertised_threshold = _valid_threshold(
        advertised,
        context_window=context_window,
    )
    if advertised_threshold is None:
        return baseline
    return min(baseline, advertised_threshold)


def _normalize_fallback(value: FallbackValue, *, label: str) -> ModelContextFallback:
    if isinstance(value, ModelContextSettings):
        window = value.context_window
        threshold = value.model_auto_compact_token_limit
    elif isinstance(value, ModelContextFallback):
        window = value.context_window
        threshold = value.model_auto_compact_token_limit
    elif isinstance(value, Mapping):
        window = _first(value, "context_window", "model_context_window")
        threshold = _first(
            value,
            "model_auto_compact_token_limit",
            "auto_compact_token_limit",
            "compact_threshold",
        )
    else:
        window = value
        threshold = None

    resolved_window = _valid_window(window)
    if resolved_window is None:
        raise ValueError(
            f"{label} context_window must be an integer between "
            f"{MIN_CONTEXT_WINDOW} and {MAX_CONTEXT_WINDOW}"
        )
    if threshold is not None:
        if _valid_threshold(threshold, context_window=resolved_window) is None:
            raise ValueError(
                f"{label} compact threshold must be a positive integer below "
                "context_window"
            )
    resolved_threshold = _native_auto_compact_threshold(
        resolved_window,
        threshold,
    )
    return ModelContextFallback(resolved_window, resolved_threshold)


def _effective_config_fallback(
    config: Mapping[str, Any],
    *,
    model_id: str,
) -> ModelContextFallback | None:
    """Read an explicit fallback only for the config's selected exact model."""

    configured_model = _first(config, "model", "model_id", "modelId")
    if not isinstance(configured_model, str) or configured_model != model_id:
        return None
    window = _valid_window(
        _first(config, "model_context_window", "modelContextWindow")
    )
    if window is None:
        return None
    advertised_threshold = _first(
        config,
        "model_auto_compact_token_limit",
        "modelAutoCompactTokenLimit",
    )
    return ModelContextFallback(
        window,
        _native_auto_compact_threshold(window, advertised_threshold),
    )


def _model_identity(candidate: Mapping[str, Any]) -> str | None:
    for key in _MODEL_ID_KEYS:
        value = candidate.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _find_exact_model(payload: Any, model_id: str) -> Mapping[str, Any] | None:
    """Find an exact catalog item without borrowing neighboring metadata."""

    root = _as_mapping(payload)
    if root is None:
        return None
    queue: deque[Any] = deque([root])
    visited: set[int] = set()
    traversed = 0
    collection_items = 0
    while queue and traversed < MAX_MODEL_TRAVERSAL_NODES:
        value = queue.popleft()
        traversed += 1
        mapping = _as_mapping(value)
        if mapping is not None:
            marker = id(mapping)
            if marker in visited:
                continue
            visited.add(marker)
            if _model_identity(mapping) == model_id:
                return mapping
            keyed = mapping.get(model_id)
            keyed_mapping = _as_mapping(keyed)
            if keyed_mapping is not None:
                return keyed_mapping
            for ordinal, (key, nested) in enumerate(mapping.items()):
                if ordinal >= MAX_MODEL_COLLECTION_ITEMS:
                    break
                if _canonical_key(key) in _COLLECTION_KEYS:
                    queue.append(nested)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                if collection_items >= MAX_MODEL_COLLECTION_ITEMS:
                    break
                queue.append(nested)
                collection_items += 1
    return None


def _metadata_values(
    candidate: Mapping[str, Any],
) -> tuple[list[Any], list[Any], bool]:
    windows: list[Any] = []
    thresholds: list[Any] = []
    queue: deque[tuple[Mapping[str, Any], int]] = deque([(candidate, 0)])
    visited: set[int] = set()
    field_count = 0
    truncated = False
    while queue:
        mapping, depth = queue.popleft()
        marker = id(mapping)
        if marker in visited:
            continue
        visited.add(marker)
        for key, value in mapping.items():
            if field_count >= MAX_METADATA_FIELDS:
                truncated = True
                break
            field_count += 1
            canonical = _canonical_key(key)
            if canonical in _CONTEXT_FIELD_KEYS:
                windows.append(value)
            elif canonical in _COMPACT_FIELD_KEYS:
                thresholds.append(value)
            elif canonical in _NESTED_METADATA_KEYS and depth < 4:
                nested = _as_mapping(value)
                if nested is not None:
                    queue.append((nested, depth + 1))
        if truncated:
            break
    return windows, thresholds, truncated


def _extract_metadata(
    candidate: Mapping[str, Any],
) -> tuple[int, int] | None:
    raw_windows, raw_thresholds, truncated = _metadata_values(candidate)
    if truncated or not raw_windows:
        return None
    windows = [_valid_window(value) for value in raw_windows]
    # One invalid/conflicting advertised value invalidates this provider row;
    # an explicit fallback is safer than arbitrarily choosing among fields.
    if any(value is None for value in windows) or len(set(windows)) != 1:
        return None
    context_window = windows[0]
    assert context_window is not None

    thresholds = [
        _valid_threshold(value, context_window=context_window)
        for value in raw_thresholds
    ]
    valid_thresholds = [value for value in thresholds if value is not None]
    advertised_threshold = min(valid_thresholds) if valid_thresholds else None
    return context_window, _native_auto_compact_threshold(
        context_window,
        advertised_threshold,
    )


def _provider_metadata(
    payload: Any,
    *,
    model_id: str,
    collection: bool,
) -> tuple[int, int] | None:
    mapping = _as_mapping(payload)
    if mapping is None:
        return None
    exact = _find_exact_model(mapping, model_id)
    if exact is not None:
        return _extract_metadata(exact)
    if collection:
        return None
    # A detail endpoint may omit ``id``.  Accept its top-level/nested explicit
    # metadata only when it did not positively identify a different model.
    if _model_identity(mapping) is None:
        return _extract_metadata(mapping)
    return None


def _validated_id(value: Any, *, label: str, maximum: int) -> str:
    resolved = str(value or "").strip()
    if not resolved or len(resolved) > maximum or any(
        ord(character) < 32 for character in resolved
    ):
        raise ModelContextResolutionError(f"effective {label} is unavailable")
    return resolved


def _provider_base_url(provider: Mapping[str, Any]) -> str | None:
    value = _first(provider, "base_url", "baseUrl", "api_base", "apiBase")
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    hostname = parsed.hostname
    if hostname is None:
        return None
    canonical_host = hostname.casefold()
    if ":" in canonical_host:
        canonical_host = f"[{canonical_host}]"
    default_port = 80 if parsed.scheme == "http" else 443
    netloc = (
        f"{canonical_host}:{port}"
        if port is not None and port != default_port
        else canonical_host
    )
    return urlunsplit(
        (
            parsed.scheme,
            netloc,
            parsed.path.rstrip("/"),
            "",
            "",
        )
    )


def _safe_header(name: Any, value: Any) -> tuple[str, str] | None:
    if not isinstance(name, str) or not _HEADER_NAME.fullmatch(name):
        return None
    if not isinstance(value, str) or "\r" in value or "\n" in value:
        return None
    return name, value


def _set_header(headers: dict[str, str], name: str, value: str) -> None:
    for existing in tuple(headers):
        if existing.casefold() == name.casefold():
            del headers[existing]
    headers[name] = value


def _request_options(
    provider: Mapping[str, Any],
    *,
    effective_config: Mapping[str, Any],
    environ: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, Any]] | None:
    headers = {"Accept": "application/json"}
    configured_headers = _first(
        provider,
        "http_headers",
        "httpHeaders",
        "headers",
    )
    if configured_headers is not None:
        mapping = _as_mapping(configured_headers)
        if mapping is None:
            return None
        for raw_name, raw_value in mapping.items():
            header = _safe_header(raw_name, raw_value)
            if header is None:
                return None
            _set_header(headers, *header)

    environment_headers = _first(
        provider,
        "env_http_headers",
        "envHttpHeaders",
    )
    if environment_headers is not None:
        mapping = _as_mapping(environment_headers)
        if mapping is None:
            return None
        for raw_name, raw_environment_name in mapping.items():
            if not isinstance(raw_environment_name, str):
                return None
            environment_value = environ.get(raw_environment_name)
            if environment_value is None:
                continue
            header = _safe_header(raw_name, environment_value)
            if header is None:
                return None
            _set_header(headers, *header)

    auth = _as_mapping(_first(provider, "auth", "authentication")) or provider
    token = _first(
        auth,
        "bearer_token",
        "bearerToken",
        "api_key",
        "apiKey",
        "experimental_bearer_token",
        "experimentalBearerToken",
    )
    if token is not None and not isinstance(token, str):
        return None
    environment_key = _first(auth, "env_key", "envKey", "api_key_env", "apiKeyEnv")
    if environment_key is not None:
        if not isinstance(environment_key, str):
            return None
        token = environ.get(environment_key)
    if not token:
        experimental_token = _first(
            provider,
            "experimental_bearer_token",
            "experimentalBearerToken",
        )
        if experimental_token is None:
            experimental_token = _first(
                effective_config,
                "experimental_bearer_token",
                "experimentalBearerToken",
            )
        if experimental_token is not None and not isinstance(
            experimental_token,
            str,
        ):
            return None
        token = experimental_token
    requires_openai_auth = _first(
        provider,
        "requires_openai_auth",
        "requiresOpenAIAuth",
    )
    if not token and requires_openai_auth is True:
        token = environ.get("OPENAI_API_KEY")
    if token and not any(name.casefold() == "authorization" for name in headers):
        header = _safe_header("Authorization", f"Bearer {token}")
        if header is None:
            return None
        _set_header(headers, *header)

    raw_query = _first(provider, "query_params", "queryParams", "params")
    query: dict[str, Any] = {}
    if raw_query is not None:
        mapping = _as_mapping(raw_query)
        if mapping is None:
            return None
        for raw_name, raw_value in mapping.items():
            if not isinstance(raw_name, str) or not raw_name:
                return None
            if isinstance(raw_value, (str, int, float, bool)) or raw_value is None:
                query[raw_name] = raw_value
            elif isinstance(raw_value, (list, tuple)) and all(
                isinstance(item, (str, int, float, bool)) or item is None
                for item in raw_value
            ):
                query[raw_name] = tuple(raw_value)
            else:
                return None
    return headers, query


class ProviderModelContextResolver:
    """Resolve and TTL-cache settings for one effective provider/model pair."""

    def __init__(
        self,
        config_reader: ConfigReader,
        *,
        model_fallbacks: Mapping[str, FallbackValue] | None = None,
        default_fallback: FallbackValue | None = None,
        ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        http_get: HttpGet | None = None,
        environ: Mapping[str, str] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(config_reader):
            raise TypeError("config_reader must be callable")
        if http_get is not None and not callable(http_get):
            raise TypeError("http_get must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        try:
            resolved_ttl = float(ttl_seconds)
            resolved_timeout = float(request_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "TTL and request timeout must be finite positive values"
            ) from exc
        if (
            not math.isfinite(resolved_ttl)
            or resolved_ttl <= 0
            or not math.isfinite(resolved_timeout)
            or resolved_timeout <= 0
        ):
            raise ValueError("TTL and request timeout must be finite positive values")
        if (
            isinstance(max_response_bytes, bool)
            or not isinstance(max_response_bytes, int)
            or not MIN_RESPONSE_BYTES
            <= max_response_bytes
            <= MAX_RESPONSE_BYTES
        ):
            raise ValueError(
                f"max_response_bytes must be between {MIN_RESPONSE_BYTES} "
                f"and {MAX_RESPONSE_BYTES}"
            )

        normalized_fallbacks = {
            str(model_id): _normalize_fallback(
                value,
                label=f"fallback for model {model_id!r}",
            )
            for model_id, value in (model_fallbacks or {}).items()
        }
        self._config_reader = config_reader
        self._model_fallbacks = MappingProxyType(normalized_fallbacks)
        self._default_fallback = (
            _normalize_fallback(default_fallback, label="default fallback")
            if default_fallback is not None
            else None
        )
        self._ttl_seconds = resolved_ttl
        self._request_timeout = resolved_timeout
        self._max_response_bytes = max_response_bytes
        self._http_get = requests.get if http_get is None else http_get
        self._environ = environ if environ is not None else os.environ
        self._clock = clock
        self._cache: dict[
            CacheKey,
            tuple[float, ModelContextSettings | None],
        ] = {}
        self._locks: dict[CacheKey, asyncio.Lock] = {}

    async def __call__(
        self,
        model_id: str | None = None,
        *,
        provider_id: str | None = None,
    ) -> ModelContextSettings:
        return await self.resolve(model_id, provider_id=provider_id)

    async def resolve(
        self,
        model_id: str | None = None,
        *,
        provider_id: str | None = None,
    ) -> ModelContextSettings:
        """Resolve one exact model from effective config and provider metadata."""

        config: Mapping[str, Any] = {}
        try:
            raw_config = await self._config_reader()
            outer = _as_mapping(raw_config)
            if outer is not None:
                nested = _as_mapping(_first(outer, "config", "effective_config"))
                config = nested or outer
        except Exception:
            # Config/provider exceptions can include request headers or URLs.
            # Never propagate their potentially secret-bearing messages.
            config = {}

        selected_model = _validated_id(
            model_id or _first(config, "model", "model_id", "modelId"),
            label="model",
            maximum=512,
        )
        selected_provider = _validated_id(
            provider_id
            or _first(
                config,
                "model_provider",
                "modelProvider",
                "provider_id",
                "providerId",
            ),
            label="model provider",
            maximum=256,
        )
        providers = _as_mapping(
            _first(config, "model_providers", "modelProviders", "providers")
        )
        provider = _as_mapping(providers.get(selected_provider)) if providers else None
        base_url = _provider_base_url(provider) if provider is not None else None
        effective_fallback = _effective_config_fallback(
            config,
            model_id=selected_model,
        )
        fallback_fingerprint = (
            (
                effective_fallback.context_window,
                effective_fallback.model_auto_compact_token_limit,
            )
            if effective_fallback is not None
            else None
        )
        key: CacheKey = (
            selected_provider,
            selected_model,
            base_url or "",
            fallback_fingerprint,
        )
        now = self._clock()
        cached = self._cache.get(key)
        if cached is not None and cached[0] > now:
            return self._cached_settings(cached[1])

        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = self._clock()
            cached = self._cache.get(key)
            if cached is not None and cached[0] > now:
                return self._cached_settings(cached[1])
            try:
                settings = await self._resolve_uncached(
                    config,
                    provider_id=selected_provider,
                    model_id=selected_model,
                    provider=provider,
                    base_url=base_url,
                    effective_fallback=effective_fallback,
                )
            except ModelContextResolutionError:
                negative_ttl = min(
                    self._ttl_seconds,
                    MAX_NEGATIVE_CACHE_TTL_SECONDS,
                )
                self._cache[key] = (now + negative_ttl, None)
                raise ModelContextResolutionError(
                    "model context metadata is unavailable for the selected "
                    "provider/model"
                ) from None
            self._cache[key] = (now + self._ttl_seconds, settings)
            return settings

    @staticmethod
    def _cached_settings(
        settings: ModelContextSettings | None,
    ) -> ModelContextSettings:
        if settings is None:
            raise ModelContextResolutionError(
                "model context metadata is unavailable for the selected "
                "provider/model"
            ) from None
        return settings

    async def _resolve_uncached(
        self,
        config: Mapping[str, Any],
        *,
        provider_id: str,
        model_id: str,
        provider: Mapping[str, Any] | None,
        base_url: str | None,
        effective_fallback: ModelContextFallback | None,
    ) -> ModelContextSettings:
        options = (
            _request_options(
                provider,
                effective_config=config,
                environ=self._environ,
            )
            if provider is not None
            else None
        )
        if base_url is not None and options is not None:
            headers, query = options
            detail_url = f"{base_url}/models/{quote(model_id, safe='')}"
            detail = await self._fetch_json(detail_url, headers=headers, query=query)
            metadata = _provider_metadata(
                detail,
                model_id=model_id,
                collection=False,
            )
            if metadata is not None:
                return self._settings(
                    provider_id,
                    model_id,
                    metadata,
                    source="provider:model-detail",
                )

            collection = await self._fetch_json(
                f"{base_url}/models",
                headers=headers,
                query=query,
            )
            metadata = _provider_metadata(
                collection,
                model_id=model_id,
                collection=True,
            )
            if metadata is not None:
                return self._settings(
                    provider_id,
                    model_id,
                    metadata,
                    source="provider:model-list",
                )

        if effective_fallback is not None:
            return ModelContextSettings(
                provider_id=provider_id,
                model_id=model_id,
                context_window=effective_fallback.context_window,
                model_auto_compact_token_limit=(
                    effective_fallback.model_auto_compact_token_limit
                ),
                source="fallback:effective-config",
            )

        fallback = self._model_fallbacks.get(model_id)
        source = "fallback:model"
        if fallback is None:
            fallback = self._default_fallback
            source = "fallback:default"
        if fallback is None:
            raise ModelContextResolutionError(
                "model context metadata is unavailable for the selected provider/model"
            ) from None
        return ModelContextSettings(
            provider_id=provider_id,
            model_id=model_id,
            context_window=fallback.context_window,
            model_auto_compact_token_limit=fallback.model_auto_compact_token_limit,
            source=source,
        )

    async def _fetch_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        query: Mapping[str, Any],
    ) -> Any | None:
        def request() -> Any:
            response = self._http_get(
                url,
                headers=dict(headers),
                params=dict(query),
                timeout=self._request_timeout,
                allow_redirects=False,
                stream=True,
            )
            try:
                response.raise_for_status()
                response_headers = _as_mapping(getattr(response, "headers", None))
                content_length = (
                    _first(response_headers, "Content-Length", "content-length")
                    if response_headers is not None
                    else None
                )
                if content_length is not None:
                    try:
                        advertised_size = int(content_length)
                    except (TypeError, ValueError):
                        raise ValueError("invalid provider response size") from None
                    if (
                        advertised_size < 0
                        or advertised_size > self._max_response_bytes
                    ):
                        raise ValueError("provider response exceeds size limit")

                iterator = getattr(response, "iter_content", None)
                if callable(iterator):
                    chunks: list[bytes] = []
                    total = 0
                    for chunk in iterator(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        if not isinstance(chunk, bytes):
                            raise ValueError("invalid provider response body")
                        total += len(chunk)
                        if total > self._max_response_bytes:
                            raise ValueError("provider response exceeds size limit")
                        chunks.append(chunk)
                    return json.loads(b"".join(chunks))

                content = getattr(response, "content", None)
                if isinstance(content, (bytes, bytearray)):
                    if len(content) > self._max_response_bytes:
                        raise ValueError("provider response exceeds size limit")
                    return json.loads(bytes(content))
                # Narrow requests-like test transports may expose only json().
                # Model traversal below remains bounded even in that case.
                return response.json()
            finally:
                close = getattr(response, "close", None)
                if callable(close):
                    close()

        try:
            return await asyncio.to_thread(request)
        except Exception:
            # Do not log or surface provider exceptions: requests includes the
            # URL (and therefore configured query secrets) in many messages.
            return None

    @staticmethod
    def _settings(
        provider_id: str,
        model_id: str,
        metadata: tuple[int, int],
        *,
        source: str,
    ) -> ModelContextSettings:
        context_window, compact_threshold = metadata
        return ModelContextSettings(
            provider_id=provider_id,
            model_id=model_id,
            context_window=context_window,
            model_auto_compact_token_limit=compact_threshold,
            source=source,
        )

    def clear_cache(
        self,
        *,
        provider_id: str | None = None,
        model_id: str | None = None,
    ) -> None:
        """Invalidate all cached settings or a selected provider/model subset."""

        if provider_id is None and model_id is None:
            self._cache.clear()
            return
        for key in tuple(self._cache):
            if provider_id is not None and key[0] != provider_id:
                continue
            if model_id is not None and key[1] != model_id:
                continue
            del self._cache[key]


async def list_provider_model_descriptors(
    effective_config: Mapping[str, Any],
    *,
    include_hidden: bool = False,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    http_get: HttpGet | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Return a conservative OpenAI-compatible provider catalog.

    Custom providers are not necessarily represented by Codex ``model/list``.
    Only bounded model IDs are projected here; the provider's unstandardized
    capability fields are deliberately not interpreted or exposed.
    """

    provider_id = _first(
        effective_config,
        "model_provider",
        "modelProvider",
        "provider_id",
        "providerId",
    )
    if not isinstance(provider_id, str) or not provider_id.strip():
        return []
    provider_id = provider_id.strip()
    providers = _as_mapping(
        _first(
            effective_config,
            "model_providers",
            "modelProviders",
            "providers",
        )
    )
    provider = _as_mapping(providers.get(provider_id)) if providers else None
    base_url = _provider_base_url(provider) if provider is not None else None
    options = (
        _request_options(
            provider,
            effective_config=effective_config,
            environ=os.environ if environ is None else environ,
        )
        if provider is not None
        else None
    )
    if base_url is None or options is None:
        return []
    # Reuse the resolver's bounded, redirect-free, redacted transport.  The
    # config reader is never called for this direct catalog operation.
    async def read_config() -> Mapping[str, Any]:
        return effective_config

    resolver = ProviderModelContextResolver(
        read_config,
        request_timeout=request_timeout,
        max_response_bytes=max_response_bytes,
        http_get=http_get,
        environ=environ,
    )
    headers, query = options
    payload = await resolver._fetch_json(
        f"{base_url}/models",
        headers=headers,
        query=query,
    )
    root = _as_mapping(payload)
    if root is not None:
        candidates = _first(root, "data", "models", "items")
        if candidates is None and _model_identity(root) is not None:
            candidates = (root,)
    elif isinstance(payload, (list, tuple)):
        candidates = payload
    else:
        candidates = ()
    if isinstance(candidates, Mapping):
        candidates = candidates.values()
    if isinstance(candidates, (str, bytes, bytearray)):
        return []
    try:
        iterator = iter(candidates)
    except TypeError:
        return []

    selected_model = _first(effective_config, "model", "model_id", "modelId")
    selected_model = selected_model if isinstance(selected_model, str) else ""
    descriptors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ordinal, candidate in enumerate(iterator):
        if ordinal >= MAX_MODEL_COLLECTION_ITEMS:
            break
        mapping = _as_mapping(candidate)
        model_id = _model_identity(mapping) if mapping is not None else None
        hidden = False
        if mapping is not None:
            for key, value in mapping.items():
                if _canonical_key(key) != "ishidden" and _canonical_key(key) != "hidden":
                    continue
                hidden = value is True or (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and value != 0
                ) or (
                    isinstance(value, str)
                    and value.strip().casefold() in {"1", "true", "yes", "hidden"}
                )
                if hidden:
                    break
        if (
            not isinstance(model_id, str)
            or not model_id
            or len(model_id) > 512
            or any(ord(character) < 32 for character in model_id)
            or (hidden and not include_hidden)
            or model_id in seen
        ):
            continue
        seen.add(model_id)
        descriptors.append(
            {
                "id": model_id,
                "displayName": model_id,
                "isDefault": model_id == selected_model,
                "supportedReasoningEfforts": [],
            }
        )
    return descriptors


__all__ = [
    "DEFAULT_CACHE_TTL_SECONDS",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "MAX_CONTEXT_WINDOW",
    "MAX_NEGATIVE_CACHE_TTL_SECONDS",
    "MIN_CONTEXT_WINDOW",
    "ModelContextFallback",
    "ModelContextResolutionError",
    "ModelContextSettings",
    "ProviderModelContextResolver",
    "list_provider_model_descriptors",
]
