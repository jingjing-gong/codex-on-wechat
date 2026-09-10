"""Narrow request construction for the task-scoped natural-cron CLI."""

from __future__ import annotations

import json

import pytest

from src import agent_cli


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (
            (
                "cron",
                "propose",
                "--schedule",
                "cron 0 9 * * 1-5 --tz Asia/Shanghai",
                "--prompt",
                "总结今天的待办",
            ),
            {
                "operation": "cron_propose",
                "schedule": "cron 0 9 * * 1-5 --tz Asia/Shanghai",
                "prompt": "总结今天的待办",
            },
        ),
        (
            ("cron", "confirm", "cron-draft-a"),
            {"operation": "cron_confirm", "draft_id": "cron-draft-a"},
        ),
        (
            ("cron", "confirm"),
            {"operation": "cron_confirm", "draft_id": ""},
        ),
        (
            ("cron", "cancel", "cron-draft-a"),
            {"operation": "cron_cancel", "draft_id": "cron-draft-a"},
        ),
        (("cron", "pending"), {"operation": "cron_pending"}),
    ],
)
def test_cron_cli_sends_only_task_scoped_arguments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    arguments: tuple[str, ...],
    expected: dict[str, str],
) -> None:
    captured: list[tuple[str, dict[str, object], float]] = []

    def request(
        socket_path: str,
        payload: dict[str, object],
        *,
        timeout: float,
    ) -> dict[str, object]:
        captured.append((socket_path, dict(payload), timeout))
        return {"ok": True, "operation": payload["operation"]}

    monkeypatch.setattr(agent_cli, "_request", request)
    result = agent_cli.main(
        (
            "--socket",
            "/tmp/cow-agent.sock",
            "--task-id",
            "task-a",
            "--capability",
            "cap-a",
            *arguments,
        )
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert len(captured) == 1
    socket_path, payload, timeout = captured[0]
    assert socket_path == "/tmp/cow-agent.sock"
    assert timeout == 10.0
    assert payload == {
        "task_id": "task-a",
        "capability": "cap-a",
        **expected,
    }
    assert {
        "principal_id",
        "principal_account_id",
        "channel",
        "bot_id",
        "external_user_id",
        "reply_target",
        "agent_id",
    }.isdisjoint(payload)

