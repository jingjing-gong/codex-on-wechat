"""Temp-only race and auth-type regressions for named config profiles."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

import src.agents.config_profile as config_profile_module
from src.agents.config_profile import CodexConfigProfileError, load_config_profile
from src.agents.model_context import ProviderModelContextResolver


_SYNTHETIC_OPENAI_KEY = "SYNTHETIC-OPENAI-KEY-MUST-NOT-BE-USED"


def _complete_profile(prefix: str, *, large: bool = False) -> bytes:
    payload = (
        f'model = "{prefix}-model"\n'
        f'model_provider = "{prefix}-provider"\n'
        f'[model_providers.{prefix}-provider]\n'
        f'base_url = "https://{prefix}.invalid/v1"\n'
    ).encode("utf-8")
    if large:
        # Keep a meaningful field beyond the loader's 64 KiB read boundary.
        # An undetected in-place rewrite would otherwise yield a valid hybrid
        # document containing the old header and the replacement's late field.
        payload += b"# " + (b"padding" * 16_384) + b"\n"
        payload += f'late_marker = "{prefix}"\n'.encode("utf-8")
    return payload


def _large_base(prefix: str) -> bytes:
    return (
        f'early_base_marker = "{prefix}"\n'.encode("utf-8")
        + b"# "
        + (b"padding" * 16_384)
        + b"\n"
        + f'late_base_marker = "{prefix}"\n'.encode("utf-8")
    )


@pytest.mark.parametrize(
    ("target_kind", "expected_error"),
    (
        ("profile", "Codex config profile is invalid: qwen"),
        ("base", "base Codex config is invalid"),
    ),
)
def test_loader_rejects_same_inode_same_size_mutation_between_reads(
    tmp_path: Path,
    monkeypatch: Any,
    target_kind: str,
    expected_error: str,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    profile_path = config_home / "qwen.config.toml"
    base_path = config_home / "config.toml"
    if target_kind == "profile":
        target = profile_path
        original_payload = _complete_profile("safe", large=True)
        replacement_payload = _complete_profile("evil", large=True)
    else:
        profile_path.write_bytes(_complete_profile("safe"))
        target = base_path
        original_payload = _large_base("safe")
        replacement_payload = _large_base("evil")
    assert len(original_payload) == len(replacement_payload)
    target.write_bytes(original_payload)
    original_metadata = target.stat()
    original_read = config_profile_module.os.read
    mutated = False

    def mutate_after_first_chunk(file_descriptor: int, count: int) -> bytes:
        nonlocal mutated
        chunk = original_read(file_descriptor, count)
        metadata = os.fstat(file_descriptor)
        if (
            not mutated
            and chunk
            and int(metadata.st_dev) == int(original_metadata.st_dev)
            and int(metadata.st_ino) == int(original_metadata.st_ino)
        ):
            mutated = True
            # Preserve pathname, inode, byte count, and the validated mtime.
            # Only ctime remains available to prove this in-place mutation.
            target.write_bytes(replacement_payload)
            os.utime(
                target,
                ns=(original_metadata.st_atime_ns, original_metadata.st_mtime_ns),
            )
            changed = target.stat()
            assert changed.st_ino == original_metadata.st_ino
            assert changed.st_size == original_metadata.st_size
            assert changed.st_mtime_ns == original_metadata.st_mtime_ns
            assert changed.st_ctime_ns != original_metadata.st_ctime_ns
        return chunk

    monkeypatch.setattr(config_profile_module.os, "read", mutate_after_first_chunk)

    with pytest.raises(CodexConfigProfileError) as captured:
        load_config_profile(
            "qwen",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )

    assert mutated
    assert str(captured.value) == expected_error
    assert "evil" not in str(captured.value)
    assert os.fspath(tmp_path) not in str(captured.value)


@pytest.mark.parametrize("target_kind", ("profile", "base"))
def test_loader_never_consumes_path_replacement_mid_read(
    tmp_path: Path,
    monkeypatch: Any,
    target_kind: str,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    profile_path = config_home / "qwen.config.toml"
    base_path = config_home / "config.toml"
    replacement_path = config_home / "replacement.toml"
    if target_kind == "profile":
        target = profile_path
        target.write_bytes(_complete_profile("safe", large=True))
        replacement_path.write_bytes(_complete_profile("evil", large=True))
    else:
        profile_path.write_bytes(_complete_profile("safe"))
        target = base_path
        target.write_bytes(_large_base("safe"))
        replacement_path.write_bytes(_large_base("evil"))
    original_metadata = target.stat()
    original_read = config_profile_module.os.read
    swapped = False

    def swap_after_first_chunk(file_descriptor: int, count: int) -> bytes:
        nonlocal swapped
        chunk = original_read(file_descriptor, count)
        metadata = os.fstat(file_descriptor)
        if (
            not swapped
            and chunk
            and int(metadata.st_dev) == int(original_metadata.st_dev)
            and int(metadata.st_ino) == int(original_metadata.st_ino)
        ):
            swapped = True
            os.replace(replacement_path, target)
            assert target.stat().st_ino != original_metadata.st_ino
            # The already-open descriptor must remain pinned to the validated
            # regular file even though the directory entry now points elsewhere.
            assert os.fstat(file_descriptor).st_ino == original_metadata.st_ino
        return chunk

    monkeypatch.setattr(config_profile_module.os, "read", swap_after_first_chunk)

    try:
        loaded = load_config_profile(
            "qwen",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )
    except CodexConfigProfileError as exc:
        # Unlinking the opened inode can update ctime, so rejection is the
        # strongest valid result.  Its public diagnostic must remain generic.
        assert swapped
        assert "evil" not in str(exc)
        assert os.fspath(tmp_path) not in str(exc)
        return

    # Platforms/filesystems that preserve the opened inode metadata may use
    # the complete validated descriptor snapshot, but never the new pathname.
    assert swapped
    assert loaded is not None
    assert loaded.model == "safe-model"
    assert loaded.model_provider == "safe-provider"
    serialized = repr(loaded.effective_config)
    assert "evil" not in serialized
    if target_kind == "base":
        assert loaded.effective_config["early_base_marker"] == "safe"
        assert loaded.effective_config["late_base_marker"] == "safe"


@pytest.mark.parametrize(
    "malformed_flag",
    (
        "true",
        "false",
        1,
        0,
        1.0,
        [True],
        {"value": True},
    ),
)
@pytest.mark.parametrize(
    "flag_name",
    ("requires_openai_auth", "requiresOpenAIAuth"),
)
def test_loaded_profile_malformed_auth_flag_never_grants_openai_key(
    tmp_path: Path,
    malformed_flag: Any,
    flag_name: str,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    # Use a fully synthetic mapping for the malformed value, while retaining
    # the same effective shape produced by a temp-only named profile load.
    (config_home / "qwen.config.toml").write_text(
        'model = "safe-model"\n'
        'model_provider = "safe-provider"\n'
        '[model_providers.safe-provider]\n'
        'base_url = "https://provider.invalid/v1"\n',
        encoding="utf-8",
    )
    loaded = load_config_profile(
        "qwen",
        environ={"CODEX_HOME": os.fspath(config_home)},
    )
    assert loaded is not None
    effective = dict(loaded.effective_config)
    providers = dict(effective["model_providers"])
    provider = dict(providers["safe-provider"])
    provider[flag_name] = malformed_flag
    providers["safe-provider"] = provider
    effective["model_providers"] = providers
    requests: list[dict[str, Any]] = []

    async def read_config() -> dict[str, Any]:
        return effective

    def get(_url: str, **kwargs: Any) -> Any:
        requests.append(dict(kwargs))

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, Any]:
                return {"id": "safe-model", "context_window": 128_000}

        return Response()

    async def scenario() -> None:
        settings = await ProviderModelContextResolver(
            read_config,
            http_get=get,
            environ={"OPENAI_API_KEY": _SYNTHETIC_OPENAI_KEY},
        ).resolve()
        assert settings.model_id == "safe-model"

    asyncio.run(scenario())

    assert requests
    headers = requests[0]["headers"]
    assert "Authorization" not in headers
    assert _SYNTHETIC_OPENAI_KEY not in repr(requests)
