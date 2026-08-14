"""Direct WeChat voice-transcription ingress regressions."""

from __future__ import annotations

import asyncio

from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import (
    COMMAND_HELP,
    MVPCommandRouter,
    WeChatGateway,
    normalize_message,
)
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    ITEM_TYPE_VOICE,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    VoiceItem,
    WeixinMessage,
)


def _voice_message(*, message_id: int = 41) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(
                type=ITEM_TYPE_VOICE,
                voice_item=VoiceItem(text="inspect the current changes"),
            )
        ],
    )


def test_voice_transcription_is_normalized_as_instruction_in_wire_order():
    message = _voice_message()
    message.item_list.insert(
        0,
        MessageItem(
            type=ITEM_TYPE_TEXT,
            text_item=TextItem(text="first instruction"),
        ),
    )

    envelope = normalize_message(message, bot_id="bot")

    assert envelope is not None
    assert envelope.text == "first instruction\ninspect the current changes"
    assert envelope.raw is not None
    assert envelope.raw["media"][0]["candidate_text"] == (
        "inspect the current changes"
    )


def test_voice_transcription_queues_task_without_confirmation(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            gateway = WeChatGateway(store, bot_id="bot")
            first = await gateway.accept(_voice_message())

            assert first is not None
            assert first.accepted and not first.duplicate
            assert first.task_id
            assert first.command_response == ""
            assert first.confirmation_ids == ()

            task = await store.get_task(first.task_id)
            assert task is not None
            assert task.inputs["text"] == "inspect the current changes"
            assert task.inputs["media"][0]["kind"] == "audio"
            assert "candidate_text" not in task.inputs["media"][0]

            inbound = await store.get_inbound("wechat", "bot", "41")
            assert inbound is not None
            assert inbound.status.value == "task_queued"

            replay = await gateway.accept(_voice_message())
            assert replay is not None
            assert replay.accepted and replay.duplicate
            assert replay.task_id == first.task_id
            assert len(await store.list_tasks()) == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_untranscribed_voice_does_not_enqueue_empty_task(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            message = _voice_message(message_id=42)
            message.item_list[0].voice_item.text = ""
            gateway = WeChatGateway(store, bot_id="bot")

            accepted = await gateway.accept(message)

            assert accepted is not None
            assert accepted.accepted
            assert not accepted.task_id
            assert "couldn't transcribe" in accepted.command_response
            assert await store.list_tasks() == []
            inbound = await store.get_inbound("wechat", "bot", "42")
            assert inbound is not None
            assert inbound.status.value == "accepted"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_audio_confirmation_commands_are_not_advertised():
    assert "/confirm" not in COMMAND_HELP
    assert "/reject" not in COMMAND_HELP

    async def scenario() -> None:
        envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="command",
            text="/confirm old-id",
        )
        router = MVPCommandRouter(object())
        assert await router.handle_command(
            parse_command("/confirm old-id"), envelope
        ) == "unknown command: /confirm. try /help"
        assert await router.handle_command(
            parse_command("/reject old-id"), envelope
        ) == "unknown command: /reject. try /help"

    asyncio.run(scenario())
