"""User-facing dynamic Agent task coverage for ``/ask``."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.channels.wechat import (
    COMMAND_INTERRUPTED_RESPONSE,
    WeChatGateway,
    command_delivery_id,
    command_request_id,
    command_request_id_candidates,
    legacy_command_delivery_id,
    normalize_message,
)
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


class _Runtime:
    agent_id = "codex"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="target Agent answer")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _envelope(text: str, *, message_id: str = "ask-message") -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=message_id,
        text=text,
        session_id="default",
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
        source_sequence=17,
        context_token="reply-token",
    )


def test_ask_creates_deduplicated_dynamic_agent_task_and_projects_answer(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register(
            "codex",
            _Runtime(),
            profile=codex_profile(default_mode_id="chat"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            router = MVPCommandRouter(manager)
            envelope = _envelope("/ask Planner inspect the failure")
            await store.accept_inbound(envelope, create_task=False)
            command = parse_command(envelope.text)
            assert command is not None

            response = await router.handle_command(command, envelope)
            assert str(response).startswith("Agent task queued: ")
            task_id = str(response).removeprefix("Agent task queued: ")

            task = await store.get_task(task_id)
            assert task is not None
            assert task.agent_id == "planner"
            assert task.inputs == {"text": "inspect the failure"}
            assert task.reply_target.source_message_id == "ask-message"
            assert task.reply_target.source_sequence == 17
            assert task.reply_target.context_token == "reply-token"
            assert task.metadata["direct_user_request"] is True
            assert task.metadata["requesting_agent_id"] == "codex"
            assert task.metadata["user_reply_format"] == "agent-prefix-v1"
            assert await manager.get_active_agent(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
            ) == "codex"

            replay = await router.handle_command(command, envelope)
            assert replay == response
            tasks = await store.list_tasks(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                limit=10,
            )
            assert [item.task_id for item in tasks] == [task_id]

            await store.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
                enabled=False,
            )
            claim = await store.claim_task_by_id(task_id, "ask-test-worker")
            assert claim is not None
            assert await store.mark_task_running(
                task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            await store.complete_task(
                task_id,
                result="target Agent answer",
                status="completed",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            outbox = sorted(
                await store.list_outbox(limit=10),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert len(outbox) == 2
            acknowledgement, answer = outbox
            assert acknowledgement.content == f"Agent task queued: {task_id}"
            assert acknowledgement.reply_ordinal == 1
            assert acknowledgement.from_user_id == "bot"
            assert answer.task_id == task_id
            assert answer.agent_id == "planner"
            assert answer.content == "planner: target Agent answer"
            assert answer.reply_ordinal == 2
            assert answer.foreground
            assert answer.notify_enabled
            assert answer.reply_target.source_message_id == "ask-message"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ask_prefixes_one_stable_item_before_chunking_and_replay(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            envelope = _envelope(
                "/ask Planner produce a long answer",
                message_id="ask-long-item",
            )
            await store.accept_inbound(envelope, create_task=False)
            response = await MVPCommandRouter(manager).handle_command(
                parse_command(envelope.text),
                envelope,
            )
            task_id = str(response).removeprefix("Agent task queued: ")

            task = await store.get_task(task_id)
            assert task is not None
            assert task.metadata["user_reply_format"] == "agent-prefix-v1"

            claim = await store.claim_task_by_id(task_id, "ask-long-worker")
            assert claim is not None
            assert await store.mark_task_running(
                task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )

            raw = "界" * 5992
            event = await store.append_task_event(
                task_id,
                {
                    "event_id": "ask-long-stable-item",
                    "execution_id": claim.execution_id,
                    "event_type": "agent_message",
                    "content": raw,
                    "source_item_id": "ask-long-source-item",
                    "source_item_type": "agentmessage",
                    "source_item_ordinal": 1,
                },
                claim_token=claim.claim_token,
            )
            assert event.content == raw

            before_completion = sorted(
                await store.list_outbox(limit=10),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert len(before_completion) == 4
            acknowledgement = before_completion[0]
            assert acknowledgement.reply_ordinal == 1
            assert acknowledgement.content == f"Agent task queued: {task_id}"
            assert not acknowledgement.content.startswith("planner: ")

            fragments = [
                item for item in before_completion if item.task_id == task_id
            ]
            assert fragments[0].event_id == event.event_id
            rendered = "planner: " + raw
            assert [len(item.content) for item in fragments] == [3000, 3000, 1]
            assert "".join(item.content for item in fragments) == rendered
            assert "".join(item.content for item in fragments).count("planner: ") == 1

            await store.complete_task(
                task_id,
                status="completed",
                events=[event],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            # A retried terminal callback is also harmless and must not add a
            # second prefix or another set of fragments.
            await store.complete_task(
                task_id,
                status="completed",
                events=[event],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            after_replay = sorted(
                await store.list_outbox(limit=10),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert len(after_replay) == 4
            replayed_fragments = [
                item for item in after_replay if item.task_id == task_id
            ]
            assert [len(item.content) for item in replayed_fragments] == [
                3000,
                3000,
                1,
            ]
            replayed = "".join(item.content for item in replayed_fragments)
            assert replayed == rendered
            assert replayed.count("planner: ") == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ask_rejects_invalid_dynamic_agent_name_without_creating_a_task(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            envelope = _envelope("/ask ../planner inspect the failure")
            response = await MVPCommandRouter(manager).handle_command(
                parse_command(envelope.text),
                envelope,
            )
            assert str(response).startswith("cannot ask Agent: Agent ID must start")
            assert await store.list_tasks(limit=10) == []
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ask_preserves_prompt_whitespace_and_newlines(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            text = "/ask Planner first line\n  second   line"
            envelope = _envelope(text, message_id="ask-whitespace")
            await store.accept_inbound(envelope, create_task=False)
            command = parse_command(text)
            assert command is not None

            response = await MVPCommandRouter(manager).handle_command(
                command,
                envelope,
            )
            task_id = str(response).removeprefix("Agent task queued: ")
            task = await store.get_task(task_id)
            assert task is not None
            assert task.inputs == {"text": "first line\n  second   line"}
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_retry_acknowledgement_precedes_output_on_the_retry_message_scope(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        try:
            origin = _envelope("original work", message_id="origin-message")
            accepted = await store.accept_inbound(origin, create_task=False)
            task = await manager.submit(
                "original work",
                origin.reply_target,
                inbound_message_id=accepted.inbound.message_id,
            )
            first_claim = await store.claim_task_by_id(
                task.task_id, "first-attempt"
            )
            assert first_claim is not None
            assert await store.mark_task_running(
                task.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            await store.complete_task(
                task.task_id,
                status="failed",
                error="first attempt failed",
                claim_token=first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )

            retry_envelope = _envelope(
                f"/retry {task.task_id}", message_id="retry-message"
            )
            await store.accept_inbound(retry_envelope, create_task=False)
            response = await MVPCommandRouter(manager).handle_command(
                parse_command(retry_envelope.text),
                retry_envelope,
            )
            assert response == f"retry queued: {task.task_id}"

            retry_scope = await store.get_reply_scope_for_target(
                retry_envelope.reply_target
            )
            queued = await store.get_task(task.task_id)
            assert retry_scope is not None and queued is not None
            assert (
                queued.pending_delivery_reply_scope_id
                == retry_scope.reply_scope_id
            )
            retry_rows = [
                item
                for item in await store.list_outbox(limit=20)
                if item.reply_scope_id == retry_scope.reply_scope_id
            ]
            assert len(retry_rows) == 1
            assert retry_rows[0].content == f"retry queued: {task.task_id}"
            assert retry_rows[0].reply_ordinal == 1

            second_claim = await store.claim_task_by_id(
                task.task_id, "second-attempt"
            )
            assert second_claim is not None
            assert (
                second_claim.execution.delivery_reply_scope_id
                == retry_scope.reply_scope_id
            )
            assert await store.mark_task_running(
                task.task_id,
                second_claim.claim_token,
                execution_id=second_claim.execution_id,
            )
            await store.complete_task(
                task.task_id,
                status="completed",
                result="retry answer",
                claim_token=second_claim.claim_token,
                execution_id=second_claim.execution_id,
            )

            retry_rows = sorted(
                (
                    item
                    for item in await store.list_outbox(limit=20)
                    if item.reply_scope_id == retry_scope.reply_scope_id
                ),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert [item.content for item in retry_rows] == [
                f"retry queued: {task.task_id}",
                "retry answer",
            ]
            assert [item.reply_ordinal for item in retry_rows] == [1, 2]
            assert all(
                item.reply_target.source_message_id == "retry-message"
                for item in retry_rows
            )
            original = await store.get_task(task.task_id)
            assert original is not None
            assert original.reply_target.source_message_id == "origin-message"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def _ask_message() -> WeixinMessage:
    return WeixinMessage(
        seq=17,
        message_id=170,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token="reply-token",
        item_list=[
            MessageItem(
                type=ITEM_TYPE_TEXT,
                text_item=TextItem(text="/ask Planner recover this"),
            )
        ],
    )


def test_ask_recovery_reruns_when_crash_precedes_task_commit(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        envelope = normalize_message(_ask_message(), bot_id="bot")
        assert envelope is not None
        await store.begin_command_receipt(
            command_delivery_id(envelope),
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            external_message_id="170",
            command_name="ask",
            command_args=("Planner", "recover", "this"),
            command_text=envelope.text,
        )
        await store.store_inbound(envelope)
        await manager.stop()

        reopened_store = SQLiteStore(path)
        reopened_registry = AgentRegistry()
        reopened_registry.register("codex", _Runtime(), profile=codex_profile())
        reopened_manager = TaskManager(
            reopened_store,
            reopened_registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await reopened_manager.start()
        try:
            outcome = await WeChatGateway(
                reopened_manager,
                bot_id="bot",
                command_router=MVPCommandRouter(reopened_manager),
            ).accept(_ask_message())
            assert outcome is not None
            assert outcome.command_response.startswith("Agent task queued: ")
            tasks = await reopened_store.list_tasks(limit=10)
            assert len(tasks) == 1
            assert tasks[0].dedupe_key == (
                f"command-ask:{command_request_id(envelope)}"
            )
        finally:
            await reopened_manager.stop()

    asyncio.run(scenario())


def test_gateway_sends_the_atomically_reserved_ask_acknowledgement_once(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        try:
            requests = []

            def send(request):
                requests.append(request)
                return SimpleNamespace(ret=0, errcode=0, errmsg="")

            client = SimpleNamespace(bot_id="bot", send_message=send)
            gateway = WeChatGateway(
                manager,
                bot_id="bot",
                command_router=MVPCommandRouter(manager),
            )
            outcome = await gateway.handle_message(client, _ask_message())

            assert outcome is not None
            assert outcome.command_response.startswith("Agent task queued: ")
            assert len(requests) == 1
            assert requests[0].msg.from_user_id == "bot"
            assert requests[0].msg.to_user_id == "user"
            rows = await store.list_outbox(limit=10)
            assert len(rows) == 1
            assert rows[0].outbox_id == command_delivery_id(outcome.envelope)
            assert rows[0].reply_ordinal == 1
            assert rows[0].state.value == "sent"

            # The command receipt and initial-reply candidate are both
            # idempotent, so a channel redelivery cannot issue another wire
            # identity or enqueue another task.
            replay = await gateway.handle_message(client, _ask_message())
            assert replay is not None and replay.duplicate
            assert len(requests) == 1
            assert len(await store.list_tasks(limit=10)) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ask_recovery_returns_task_when_crash_follows_task_commit(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        envelope = normalize_message(_ask_message(), bot_id="bot")
        assert envelope is not None
        command_id = command_delivery_id(envelope)
        request_id = command_request_id(envelope)
        await store.begin_command_receipt(
            command_id,
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            session_id="default",
            external_message_id="170",
            command_name="ask",
            command_args=("Planner", "recover", "this"),
            command_text=envelope.text,
        )
        await store.store_inbound(envelope)
        await manager.ensure_agent("Planner")
        task = await manager.submit(
            "recover this",
            envelope.reply_target,
            agent_id="Planner",
            actor="user",
            explicit=True,
            request_id=request_id,
            dedupe_key=f"command-ask:{request_id}",
            metadata={
                "direct_user_request": True,
                "requesting_agent_id": "codex",
            },
        )
        await manager.stop()

        reopened_store = SQLiteStore(path)
        reopened_registry = AgentRegistry()
        reopened_registry.register("codex", _Runtime(), profile=codex_profile())
        reopened_manager = TaskManager(
            reopened_store,
            reopened_registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await reopened_manager.start()
        try:
            outcome = await WeChatGateway(
                reopened_manager,
                bot_id="bot",
                command_router=MVPCommandRouter(reopened_manager),
            ).accept(_ask_message())
            assert outcome is not None
            assert outcome.command_response == f"Agent task queued: {task.task_id}"
            assert len(await reopened_store.list_tasks(limit=10)) == 1
            receipt = await reopened_store.get_command_receipt(command_id)
            assert receipt is not None and receipt["state"] == "completed"
        finally:
            await reopened_manager.stop()

    asyncio.run(scenario())


def test_ask_recovery_rejects_colon_shifted_legacy_task_scope(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        replay_envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="c",
            text="/ask Planner recover this",
            session_id="a:b",
            agent_id="codex",
        )
        foreign_envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="b:c",
            text=replay_envelope.text,
            session_id="a",
            agent_id="codex",
        )
        assert legacy_command_delivery_id(replay_envelope) == legacy_command_delivery_id(
            foreign_envelope
        )
        legacy_request_id = command_request_id_candidates(replay_envelope)[-1]
        assert legacy_request_id == command_request_id_candidates(foreign_envelope)[-1]

        store = SQLiteStore(path)
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await manager.start()
        await manager.ensure_agent("Planner")
        foreign_task = await manager.submit(
            "recover this",
            foreign_envelope.reply_target,
            agent_id="Planner",
            actor="user",
            explicit=True,
            request_id=legacy_request_id,
            dedupe_key=f"command-ask:{legacy_request_id}",
            metadata={"direct_user_request": True},
        )
        command_id = command_delivery_id(replay_envelope)
        await store.begin_command_receipt(
            command_id,
            channel=replay_envelope.channel,
            bot_id=replay_envelope.bot_id,
            external_user_id=replay_envelope.external_user_id,
            session_id=replay_envelope.session_id,
            external_message_id=replay_envelope.external_message_id,
            command_name="ask",
            command_args=("Planner", "recover", "this"),
            command_text=replay_envelope.text,
        )
        await store.store_inbound(replay_envelope)
        await manager.stop()

        reopened_store = SQLiteStore(path)
        reopened_registry = AgentRegistry()
        reopened_registry.register("codex", _Runtime(), profile=codex_profile())
        reopened_manager = TaskManager(
            reopened_store,
            reopened_registry,
            worker_count=0,
            allow_dynamic_agents=True,
        )
        await reopened_manager.start()
        try:
            gateway = WeChatGateway(
                reopened_manager,
                bot_id="bot",
                command_router=MVPCommandRouter(reopened_manager),
            )
            gateway.normalize = lambda _message, *, bot_id="": replay_envelope
            outcome = await gateway.accept(_ask_message())

            assert outcome is not None
            assert outcome.command_response == COMMAND_INTERRUPTED_RESPONSE
            assert foreign_task.task_id not in outcome.command_response
            receipt = await reopened_store.get_command_receipt(command_id)
            assert receipt is not None and receipt["state"] == "interrupted"
            assert len(await reopened_store.list_tasks(limit=10)) == 1
        finally:
            await reopened_manager.stop()

    asyncio.run(scenario())
