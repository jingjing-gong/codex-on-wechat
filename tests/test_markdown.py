"""Markdown conversion regressions for user-visible WeChat text."""

from src.channels.wechat import (
    _MAX_COMMAND_MARKDOWN,
    _format_agents_markdown,
    _format_models_markdown,
    _format_modes_markdown,
    _format_shell_error,
    _format_shell_markdown,
)
from wechat_ilink.markdown import markdown_to_plain_text


def test_fenced_code_removes_language_labels_but_preserves_literal_markup():
    markdown = (
        "## Result\n\n"
        "````text\n"
        "literal ``` ticks, `inline`, and *asterisks*\n"
        "````"
    )

    assert markdown_to_plain_text(markdown) == (
        "Result\n\n"
        "literal ``` ticks, `inline`, and *asterisks*"
    )


def test_shell_markdown_survives_plain_text_delivery_with_long_fences():
    rendered = _format_shell_markdown(
        "printf '```'",
        "exit code: 0\nliteral ``` output",
    )

    plain = markdown_to_plain_text(rendered)
    assert "sh\n" not in plain
    assert "text\n" not in plain
    assert "printf '```'" in plain
    assert "literal ``` output" in plain


def test_text_after_fenced_code_stays_on_its_own_line():
    assert markdown_to_plain_text("pre\n```text\na\n```\npost") == "pre\na\npost"


def test_shell_markdown_bounds_command_output_and_closes_dynamic_fences():
    rendered = _format_shell_markdown(
        "printf '" + ("`" * 7000) + "'",
        "exit code: 0\n" + ("output\n" * 2000),
    )

    assert len(rendered) <= _MAX_COMMAND_MARKDOWN
    assert rendered.startswith("## Shell Result\n\n- **Exit code:** `0`")
    assert rendered.count("... (content truncated)") == 2
    plain = markdown_to_plain_text(rendered)
    assert "Shell Result" in plain
    assert "Output" in plain
    assert plain.count("... (content truncated)") == 2


def test_shell_error_is_bounded_and_fence_safe():
    rendered = _format_shell_error(("`" * 7000) + (" failure" * 1000))

    assert len(rendered) <= _MAX_COMMAND_MARKDOWN
    assert rendered.startswith("## Shell Error\n\n")
    assert markdown_to_plain_text(rendered).endswith("... (content truncated)")


def test_shell_markdown_rejects_unbounded_fabricated_exit_status():
    rendered = _format_shell_markdown(
        "command",
        "exit code: " + ("9" * 7000) + "\noutput",
    )

    assert len(rendered) <= _MAX_COMMAND_MARKDOWN
    assert "- **Exit code:** `unknown`" in rendered
    assert markdown_to_plain_text(rendered).endswith("... (content truncated)")


def test_legacy_shell_formatters_share_the_bounded_markdown_contract():
    from src import codex_wechat_bot as legacy_bot

    rendered = legacy_bot._format_shell_result(
        "printf '" + ("`" * 7000) + "'",
        0,
        "output\n" * 2000,
    )
    error = legacy_bot._format_shell_error(("`" * 7000) + (" failure" * 1000))

    assert len(rendered) <= _MAX_COMMAND_MARKDOWN
    assert len(error) <= _MAX_COMMAND_MARKDOWN
    assert rendered.count("... (content truncated)") == 2
    assert markdown_to_plain_text(rendered).count("... (content truncated)") == 2
    assert markdown_to_plain_text(error).endswith("... (content truncated)")


def test_catalog_markdown_is_bounded_and_retains_late_current_entries():
    agents = [
        {
            "agent_id": f"agent-{index}",
            "display_name": "Agent " + ("name " * 120),
            "summary": "summary " * 120,
        }
        for index in range(100)
    ]
    models = [
        {
            "id": f"model-{index}",
            "displayName": "Model " + ("name " * 120),
            "supportedReasoningEfforts": [f"effort-{item}" for item in range(100)],
        }
        for index in range(100)
    ]
    modes = [
        {
            "mode_id": f"mode-{index}",
            "sandbox_policy": "sandbox-" + ("policy " * 120),
        }
        for index in range(100)
    ]

    rendered_values = (
        _format_agents_markdown(agents, active_agent="agent-99"),
        _format_models_markdown(
            models,
            agent_id="agent-99",
            configured_model="model-99",
            configured_effort="effort-99",
        ),
        _format_modes_markdown(modes, current_mode="mode-99"),
    )

    for rendered in rendered_values:
        assert len(rendered) <= _MAX_COMMAND_MARKDOWN
        assert rendered.count("**(current)**") == 1
        assert "_... (list truncated)_" in rendered
        assert "- **`" in rendered
    assert "**`agent-99`** **(current)**" in rendered_values[0]
    assert "**`model-99`** **(current)**" in rendered_values[1]
    assert "**`mode-99`** **(current)**" in rendered_values[2]


def test_empty_mode_catalog_keeps_stored_selection_visible():
    assert _format_modes_markdown([], current_mode="retired-mode") == (
        "## Modes\n\n"
        "- **`retired-mode`** **(current)** **(unavailable)**"
    )
