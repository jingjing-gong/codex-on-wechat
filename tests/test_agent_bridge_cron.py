"""Task capability and public-output fences for natural cron operations."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.runtime.agent_bridge import AgentBridgeServer


UTC = timezone.utc


def _draft(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "draft_id": "cron-draft-a",
        "state": "pending",
        "schedule_kind": "cron",
        "schedule_expression": "0 9 * * 1-5",
        "timezone_name": "Asia/Shanghai",
        "prompt": "总结今天的待办",
        "next_fire_at": datetime(2026, 9, 9, 1, 0, tzinfo=UTC),
        "expires_at": datetime(2026, 9, 8, 4, 15, tzinfo=UTC),
        "job_id": "",
        "private_identity_snapshot": {"secret": "must not cross bridge"},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Manager:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    async def propose_natural_cron(
        self,
        task_id: str,
        schedule: str,
        prompt: str,
        *,
        required_execution_id: str,
    ) -> object:
        self.calls.append(
            ("propose", task_id, schedule, prompt, required_execution_id)
        )
        return _draft()

    async def confirm_natural_cron(
        self,
        task_id: str,
        *,
        draft_id: str,
        required_execution_id: str,
    ) -> object:
        self.calls.append(("confirm", task_id, draft_id, required_execution_id))
        return SimpleNamespace(
            draft=_draft(state="confirmed", job_id="cron-job-a"),
            job=SimpleNamespace(
                job_id="cron-job-a",
                schedule_kind="cron",
                schedule_expression="0 9 * * 1-5",
                timezone_name="Asia/Shanghai",
                next_fire_at=datetime(2026, 9, 9, 1, 0, tzinfo=UTC),
                private_task_template={"secret": "must not cross bridge"},
            ),
        )

    async def cancel_natural_cron(
        self,
        task_id: str,
        *,
        draft_id: str,
        required_execution_id: str,
    ) -> object:
        self.calls.append(("cancel", task_id, draft_id, required_execution_id))
        return _draft(state="cancelled", draft_id=draft_id or "cron-draft-a")

    async def pending_natural_cron(
        self,
        task_id: str,
        *,
        required_execution_id: str,
    ) -> object:
        self.calls.append(("pending", task_id, required_execution_id))
        return [_draft(), _draft(draft_id="cron-draft-b", prompt="check service")]


def test_cron_bridge_delegates_with_execution_capability_and_public_fields(
    tmp_path,
) -> None:
    async def scenario() -> None:
        manager = _Manager()
        bridge = AgentBridgeServer(manager, tmp_path / "agent.sock")
        task = SimpleNamespace(
            task_id="task-a",
            execution_id="exec-a",
            agent_id="codex",
        )
        capability = bridge.capability_authority.issue(task)
        common: dict[str, object] = {
            "task_id": "task-a",
            "capability": capability,
            # Caller-controlled identity fields must never be forwarded.
            "principal_id": "forged-owner",
            "bot_id": "forged-bot",
            "reply_target": {"external_user_id": "victim"},
        }

        proposed = await bridge._dispatch(
            {
                **common,
                "operation": "cron_propose",
                "schedule": "cron 0 9 * * 1-5 --tz Asia/Shanghai",
                "prompt": "总结今天的待办",
            }
        )
        assert proposed == {
            "ok": True,
            "operation": "cron_propose",
            "created": False,
            "draft_id": "cron-draft-a",
            "state": "pending",
            "schedule_kind": "cron",
            "schedule_expression": "0 9 * * 1-5",
            "timezone_name": "Asia/Shanghai",
            "prompt": "总结今天的待办",
            "next_fire_at": "2026-09-09T01:00:00+00:00",
            "expires_at": "2026-09-08T04:15:00+00:00",
        }
        assert "private_identity_snapshot" not in proposed

        confirmed = await bridge._dispatch(
            {**common, "operation": "cron_confirm", "draft_id": "cron-draft-a"}
        )
        assert confirmed["created"] is True
        assert confirmed["state"] == "confirmed"
        assert confirmed["draft_id"] == "cron-draft-a"
        assert confirmed["job_id"] == "cron-job-a"
        assert "private_task_template" not in confirmed

        cancelled = await bridge._dispatch(
            {**common, "operation": "cron_cancel", "draft_id": "cron-draft-a"}
        )
        assert cancelled["created"] is False
        assert cancelled["state"] == "cancelled"

        pending = await bridge._dispatch(
            {**common, "operation": "cron_pending"}
        )
        assert pending["created"] is False
        assert [item["draft_id"] for item in pending["drafts"]] == [
            "cron-draft-a",
            "cron-draft-b",
        ]
        assert all("private_identity_snapshot" not in item for item in pending["drafts"])

        assert manager.calls == [
            (
                "propose",
                "task-a",
                "cron 0 9 * * 1-5 --tz Asia/Shanghai",
                "总结今天的待办",
                "exec-a",
            ),
            ("confirm", "task-a", "cron-draft-a", "exec-a"),
            ("cancel", "task-a", "cron-draft-a", "exec-a"),
            ("pending", "task-a", "exec-a"),
        ]

    asyncio.run(scenario())


def test_cron_bridge_rejects_forged_capability_before_manager_call(tmp_path) -> None:
    async def scenario() -> None:
        manager = _Manager()
        bridge = AgentBridgeServer(manager, tmp_path / "agent.sock")

        with pytest.raises(PermissionError, match="invalid or expired"):
            await bridge._dispatch(
                {
                    "operation": "cron_propose",
                    "task_id": "task-a",
                    "capability": "forged",
                    "schedule": "every 2h",
                    "prompt": "check service",
                }
            )
        assert manager.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("operation", "result", "message"),
    [
        ("cron_propose", SimpleNamespace(state="pending"), "durable identity"),
        (
            "cron_confirm",
            SimpleNamespace(draft=_draft(), job=SimpleNamespace()),
            "incomplete",
        ),
        ("cron_pending", [SimpleNamespace(state="pending")], "durable identity"),
    ],
)
def test_cron_bridge_fails_closed_on_malformed_manager_success(
    tmp_path,
    operation: str,
    result: object,
    message: str,
) -> None:
    class Manager(_Manager):
        async def propose_natural_cron(self, *_args, **_kwargs):
            return result

        async def confirm_natural_cron(self, *_args, **_kwargs):
            return result

        async def pending_natural_cron(self, *_args, **_kwargs):
            return result

    async def scenario() -> None:
        manager = Manager()
        bridge = AgentBridgeServer(manager, tmp_path / "agent.sock")
        capability = bridge.capability_authority.issue(
            SimpleNamespace(
                task_id="task-a",
                execution_id="exec-a",
                agent_id="codex",
            )
        )
        request: dict[str, object] = {
            "operation": operation,
            "task_id": "task-a",
            "capability": capability,
        }
        if operation == "cron_propose":
            request.update(schedule="every 2h", prompt="check service")
        elif operation == "cron_confirm":
            request["draft_id"] = "cron-draft-a"
        with pytest.raises(ValueError, match=message):
            await bridge._dispatch(request)

    asyncio.run(scenario())
