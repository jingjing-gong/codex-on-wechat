"""Adversarial filesystem and input-bound regressions for config profiles."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

import src.agents.config_profile as config_profile_module
from src.agents.config_profile import (
    CodexConfigProfileError,
    load_config_profile,
    require_profile_file,
)


def _complete_profile(prefix: str = "safe") -> bytes:
    return (
        f'model = "{prefix}-model"\n'
        f'model_provider = "{prefix}-provider"\n'
        f'[model_providers.{prefix}-provider]\n'
        f'base_url = "https://{prefix}.invalid/v1"\n'
    ).encode("utf-8")


@pytest.mark.parametrize("payload_kind", ("oversize", "invalid_utf8", "malformed"))
def test_profile_input_bounds_fail_closed_without_source_disclosure(
    tmp_path: Path,
    payload_kind: str,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    marker = b"SYNTHETIC-CONFIG-SOURCE-MARKER"
    if payload_kind == "oversize":
        payload = b"#" + marker + b"\n" + b"x" * (4 * 1024 * 1024)
    elif payload_kind == "invalid_utf8":
        payload = marker + b"\n\xff\xfe"
    else:
        payload = b'api_key = "' + marker + b'" trailing-garbage\n'
    (config_home / "hostile.config.toml").write_bytes(payload)
    environ = {"CODEX_HOME": os.fspath(config_home)}

    if payload_kind == "oversize":
        with pytest.raises(CodexConfigProfileError) as captured:
            require_profile_file("hostile", environ=environ)
    else:
        assert require_profile_file("hostile", environ=environ) == "hostile"
        with pytest.raises(CodexConfigProfileError) as captured:
            load_config_profile("hostile", environ=environ)

    public_error = str(captured.value)
    assert "SYNTHETIC-CONFIG-SOURCE-MARKER" not in public_error
    assert os.fspath(tmp_path) not in public_error
    assert len(public_error) < 100


def test_loader_rejects_same_inode_profile_mutation_after_validation(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    candidate = config_home / "qwen.config.toml"
    original_payload = _complete_profile("safe")
    replacement_payload = _complete_profile("evil")
    assert len(original_payload) == len(replacement_payload)
    candidate.write_bytes(original_payload)
    original_check = config_profile_module._contained_regular_file
    replaced = False

    def validate_then_mutate(
        root: Path,
        path: Path,
        *,
        label: str,
    ) -> Any:
        nonlocal replaced
        validated = original_check(root, path, label=label)
        if not replaced and path.name == "qwen.config.toml":
            replaced = True
            # Truncating and rewriting a regular file preserves its inode and
            # final size, defeating identity checks that omit timestamps.
            path.write_bytes(replacement_payload)
        return validated

    monkeypatch.setattr(
        config_profile_module,
        "_contained_regular_file",
        validate_then_mutate,
    )

    try:
        loaded = load_config_profile(
            "qwen",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )
    except CodexConfigProfileError:
        return

    assert loaded is not None
    assert loaded.model == "safe-model"
    assert loaded.model_provider == "safe-provider"


def test_loader_normalizes_pathological_toml_nesting_to_profile_error(
    tmp_path: Path,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    nested_table = ".".join("a" for _ in range(990))
    marker = "SYNTHETIC-DEEP-TOML-MARKER"
    (config_home / "config.toml").write_text(
        f"[{nested_table}]\nbase_value = 1\n",
        encoding="utf-8",
    )
    (config_home / "deep.config.toml").write_text(
        'model = "safe-model"\n'
        'model_provider = "safe-provider"\n'
        '[model_providers.safe-provider]\n'
        'base_url = "https://safe.invalid/v1"\n'
        f"[{nested_table}]\nlayer_value = \"{marker}\"\n",
        encoding="utf-8",
    )

    with pytest.raises(CodexConfigProfileError) as captured:
        load_config_profile(
            "deep",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )

    assert marker not in str(captured.value)
    assert os.fspath(tmp_path) not in str(captured.value)


def test_loader_rejects_empty_selected_provider_table(
    tmp_path: Path,
) -> None:
    config_home = tmp_path / "codex-home"
    config_home.mkdir()
    (config_home / "empty.config.toml").write_text(
        'model = "safe-model"\n'
        'model_provider = "empty-provider"\n'
        '[model_providers.empty-provider]\n',
        encoding="utf-8",
    )

    with pytest.raises(
        CodexConfigProfileError,
        match="Codex config profile is incomplete: empty",
    ):
        load_config_profile(
            "empty",
            environ={"CODEX_HOME": os.fspath(config_home)},
        )
