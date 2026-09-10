"""Shared `/cron` command grammar, routing, and ownership coverage."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from src.channels.commands import MVPCommandRouter, command_help_for_channel
from src.channels.models import InboundEnvelope, parse_command


def _envelope(
    text: str,
    *,
    channel: str = "wechat",
    bot_id: str = "bot-a",
    actor: str = "user-a",
    principal_id: str = "",
    principal_account_id: str = "",
    principal_mapping_revision: int | None = None,
) -> InboundEnvelope:
    lark = channel == "lark"
    return InboundEnvelope(
        channel=channel,
        bot_id=bot_id,
        external_user_id=actor,
        external_message_id="message-1",
        text=text,
        session_id="session-a",
        agent_id="codex",
        conversation_id=f"{channel}:{bot_id}:{actor}:session-a:codex",
        principal_id=principal_id,
        principal_account_id=principal_account_id,
        principal_mapping_revision=principal_mapping_revision,
        conversation_subject_id="subject-thread" if lark else "",
        conversation_subject_scope="chat-a:thread-a" if lark else actor,
        conversation_subject_kind="thread" if lark else "direct",
        destination_kind="chat" if lark else "",
        destination_id="chat-a" if lark else "",
        thread_id="thread-a" if lark else "",
        root_message_id="root-a" if lark else "",
    )


@dataclass
class _Job:
    job_id: str
    agent_id: str = "codex"
    schedule_kind: str = "cron"
    schedule_expression: str = "0 9 * * 1-5"
    timezone_name: str = "Asia/Shanghai"
    prompt: str = "prepare briefing"
    next_fire_at: str = "2026-09-09T01:00:00+00:00"


class _Manager:
    def __init__(self) -> None:
        self.add_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.list_calls: list[dict[str, object]] = []
        self.delete_calls: list[tuple[str, dict[str, object]]] = []

    async def get_active_agent(self, **_kwargs: object) -> str:
        return "qwen"

    async def add_cron_job(self, *args: object, **kwargs: object) -> _Job:
        self.add_calls.append((args, dict(kwargs)))
        return _Job(
            job_id=str(kwargs["job_id"]),
            agent_id=str(kwargs["agent_id"]),
            timezone_name=(
                "Europe/London"
                if "Europe/London" in str(args[0] if args else "")
                else "Asia/Shanghai"
            ),
        )

    async def list_cron_jobs(self, **kwargs: object) -> list[_Job]:
        self.list_calls.append(dict(kwargs))
        return [_Job("cron-one")]

    async def delete_cron_job(self, job_id: str, **kwargs: object) -> None:
        self.delete_calls.append((job_id, dict(kwargs)))


def test_cron_help_is_shared_by_wechat_and_lark() -> None:
    for channel in ("wechat", "lark"):
        help_text = command_help_for_channel(channel)
        assert "`/cron <add|list|delete|help> ...`" in help_text

        async def scenario() -> str | None:
            envelope = _envelope("/cron help", channel=channel)
            return await MVPCommandRouter(_Manager()).handle_command(
                parse_command(envelope.text), envelope
            )

        response = asyncio.run(scenario())
        assert response is not None
        assert "Asia/Shanghai" in response
        assert "catch" in response


def test_cron_add_preserves_schedule_prompt_origin_and_replay_id() -> None:
    manager = _Manager()
    text = "/cron add cron 0 9 * * 1-5 --tz Europe/London -- prepare briefing -- concise"
    envelope = _envelope(
        text,
        channel="lark",
        bot_id="cli_a",
        actor="ou_owner",
        principal_id="owner",
        principal_account_id="account-a",
        principal_mapping_revision=7,
    )

    async def scenario() -> tuple[str | None, str | None]:
        router = MVPCommandRouter(manager)
        command = parse_command(text)
        return (
            await router.handle_command(command, envelope, command_id="receipt-a"),
            await router.handle_command(command, envelope, command_id="receipt-a"),
        )

    first, replay = asyncio.run(scenario())
    assert first == replay
    assert first is not None and first.startswith("cron job added: cron-")
    assert first.endswith("; timezone: Europe/London")
    assert "next fire" not in first
    assert len(manager.add_calls) == 2
    first_args, first_kwargs = manager.add_calls[0]
    replay_args, replay_kwargs = manager.add_calls[1]
    assert first_args == (
        "cron 0 9 * * 1-5 --tz Europe/London",
        "prepare briefing -- concise",
    )
    assert first_kwargs["job_id"] == replay_kwargs["job_id"]
    assert first_kwargs["now"] == envelope.received_at
    assert replay_kwargs["now"] == envelope.received_at
    assert first_kwargs["agent_id"] == "qwen"
    assert first_kwargs["principal_id"] == "owner"
    assert first_kwargs["principal_account_id"] == "account-a"
    assert first_kwargs["principal_mapping_revision"] == 7
    assert first_kwargs["channel"] == "lark"
    assert first_kwargs["bot_id"] == "cli_a"
    assert first_kwargs["external_user_id"] == "ou_owner"
    assert first_kwargs["conversation_subject_scope"] == "chat-a:thread-a"
    target = first_kwargs["reply_target"]
    assert target.destination_id == "chat-a"
    assert target.thread_id == "thread-a"


def test_cron_list_and_delete_use_principal_plus_authenticated_origin() -> None:
    manager = _Manager()
    envelope = _envelope(
        "/cron list",
        channel="lark",
        principal_id="owner",
        principal_account_id="account-a",
        principal_mapping_revision=7,
    )

    async def scenario() -> tuple[str | None, str | None]:
        router = MVPCommandRouter(manager)
        listed = await router.handle_command(parse_command(envelope.text), envelope)
        deleted_envelope = _envelope(
            "/cron delete cron-one",
            channel="lark",
            principal_id="owner",
            principal_account_id="account-a",
            principal_mapping_revision=7,
        )
        deleted = await router.handle_command(
            parse_command(deleted_envelope.text), deleted_envelope
        )
        return listed, deleted

    listed, deleted = asyncio.run(scenario())
    assert listed is not None and "cron-one" in listed
    assert deleted == "cron job deleted: cron-one"
    assert manager.list_calls == [
        {
            "principal_id": "owner",
            "principal_account_id": "account-a",
            "principal_mapping_revision": 7,
            "channel": "lark",
            "bot_id": "bot-a",
            "external_user_id": "user-a",
            "enabled": True,
            "limit": 100,
        }
    ]
    assert manager.delete_calls == [
        (
            "cron-one",
            {
                "principal_id": "owner",
                "principal_account_id": "account-a",
                "principal_mapping_revision": 7,
                "channel": "lark",
                "bot_id": "bot-a",
                "external_user_id": "user-a",
            },
        )
    ]


def test_cron_add_rejects_missing_delimiter_without_mutation() -> None:
    manager = _Manager()
    envelope = _envelope("/cron add every 2h run a check")

    async def scenario() -> str | None:
        return await MVPCommandRouter(manager).handle_command(
            parse_command(envelope.text), envelope
        )

    response = asyncio.run(scenario())
    assert response is not None and "/cron add every" in response
    assert manager.add_calls == []


def test_cron_validation_error_is_clear_and_bounded() -> None:
    class InvalidManager(_Manager):
        async def add_cron_job(self, *args: object, **kwargs: object) -> _Job:
            raise ValueError("cron schedule requires exactly five fields")

    envelope = _envelope("/cron add cron 0 9 * -- do work")

    async def scenario() -> str | None:
        return await MVPCommandRouter(InvalidManager()).handle_command(
            parse_command(envelope.text), envelope
        )

    assert asyncio.run(scenario()) == (
        "cannot add cron job: cron schedule requires exactly five fields"
    )
