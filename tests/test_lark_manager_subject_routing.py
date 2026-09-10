"""TaskManager regressions for Lark subject state and exact delivery."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from src.agents.base import AgentResult
from src.channels.lark import (
    LarkBotProfile,
    LarkCommandRouter,
    LarkDeliveryWorker,
    LarkGateway,
)
from src.channels.models import InboundEnvelope, parse_command
from src.runtime.identity import (
    conversation_id,
    group_conversation_subject,
    principal_conversation_id,
    thread_conversation_subject,
)
from src.runtime.manager import TaskManager
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


class _Runtime:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="ok")

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(self, *, include_hidden: bool = False):
        assert include_hidden is False
        return [
            {
                "id": "gpt-subject",
                "isDefault": True,
                "supportedReasoningEfforts": [
                    {"reasoningEffort": "low"},
                    {"reasoningEffort": "high"},
                ],
            }
        ]

    def set_model(self, _conversation_id: str, _model_id: str) -> None:
        return None

    def set_reasoning_effort(
        self, _conversation_id: str, _reasoning_effort: str
    ) -> None:
        return None


def _manager(database: Path, *, workspace_root: Path | None = None) -> TaskManager:
    registry = AgentRegistry()
    registry.register("codex", _Runtime(), profile=codex_profile())
    registry.register(
        "planner",
        _Runtime(),
        profile=AgentProfile(agent_id="planner", display_name="Planner"),
    )
    return TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        workspace_root=workspace_root,
    )


def _group_envelope(
    text: str,
    *,
    actor: str,
    message_id: str,
    bot_id: str = "cli_bot_subject",
    chat_id: str = "oc_subject_chat",
) -> InboundEnvelope:
    subject = group_conversation_subject("lark", bot_id, chat_id)
    return InboundEnvelope(
        channel="lark",
        bot_id=bot_id,
        external_user_id=actor,
        external_message_id=message_id,
        text=text,
        conversation_id=conversation_id(
            "lark", bot_id, subject.scope_key, "default", "codex"
        ),
        conversation_subject_id=subject.conversation_subject_id,
        conversation_subject_scope=subject.scope_key,
        conversation_subject_kind="group",
        destination_kind="group",
        destination_id=chat_id,
        transport_metadata={"chat_id": chat_id, "chat_type": "group"},
    )


async def _command(router: LarkCommandRouter, envelope: InboundEnvelope) -> str:
    command = parse_command(envelope.text)
    assert command is not None
    response = await router.handle_command(command, envelope)
    assert response is not None
    return str(response)


def test_group_commands_share_subject_state_without_actor_or_wechat_leakage(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        workspace = tmp_path / "workspace"
        shared_cwd = workspace / "shared"
        shared_cwd.mkdir(parents=True)
        manager = _manager(
            tmp_path / "runtime.sqlite", workspace_root=workspace
        )
        await manager.start()
        try:
            router = LarkCommandRouter(manager)
            alice = _group_envelope(
                "/agent planner", actor="ou_alice", message_id="om-agent"
            )
            subject_scope = alice.conversation_subject_scope
            assert await _command(router, alice) == "switched to Agent: planner"
            assert await manager.get_active_agent(
                channel="lark",
                bot_id=alice.bot_id,
                external_user_id=subject_scope,
            ) == "planner"
            # The authenticated actor never becomes an alternate route key.
            assert await manager.get_active_agent(
                channel="lark",
                bot_id=alice.bot_id,
                external_user_id=alice.external_user_id,
            ) == "codex"

            assert await _command(
                router,
                _group_envelope(
                    "/mode review", actor="ou_bob", message_id="om-mode"
                ),
            ) == "mode: review"
            assert await _command(
                router,
                _group_envelope(
                    "/system Shared reviewer",
                    actor="ou_alice",
                    message_id="om-role",
                ),
            ) == "system role: updated"
            model_response = await _command(
                router,
                _group_envelope(
                    "/model gpt-subject high",
                    actor="ou_bob",
                    message_id="om-model",
                ),
            )
            assert "gpt-subject" in model_response and "high" in model_response
            assert str(
                await _command(
                    router,
                    _group_envelope(
                        "/cd shared", actor="ou_alice", message_id="om-cwd"
                    ),
                )
            ).endswith("/shared")

            accepted = await manager.accept_inbound(
                _group_envelope(
                    "new turn", actor="ou_charlie", message_id="om-prompt"
                )
            )
            task = accepted.task
            assert task.agent_id == "planner"
            assert task.mode_id == "review"
            assert task.model == "gpt-subject"
            assert task.reasoning_effort == "high"
            assert task.metadata["session_role"]["normalized_content"] == (
                "Shared reviewer"
            )
            assert task.metadata["execution_workspace"]["path"] == str(shared_cwd)
            assert task.actor_external_user_id == "ou_charlie"
            assert task.reply_target.external_user_id == "ou_charlie"
            assert task.reply_target.destination_id == "oc_subject_chat"

            # Same external ID on WeChat retains its legacy independent key.
            wechat = await manager.accept_inbound(
                InboundEnvelope(
                    channel="wechat",
                    bot_id="wechat-bot",
                    external_user_id="ou_charlie",
                    external_message_id="wx-prompt",
                    text="wechat turn",
                )
            )
            assert wechat.task.agent_id == "codex"
            assert wechat.task.mode_id == "chat"
            assert wechat.task.model == ""
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_non_admin_agent_switch_losing_delete_race_never_recreates_agent(
    tmp_path: Path,
) -> None:
    class BlockingSharedRouter:
        def __init__(self, delegate) -> None:
            self.delegate = delegate
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def handle_command(self, *args, **kwargs):
            # Reaching this boundary proves Lark's initial enabled-Agent check
            # succeeded.  Hold the shared effect router until administrator
            # deletion wins the list-then-switch race deterministically.
            self.entered.set()
            await self.release.wait()
            return await self.delegate.handle_command(*args, **kwargs)

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
        switch_task = None
        blocking = None
        try:
            await manager.set_active_agent(
                "planner",
                channel="wechat",
                bot_id="seed-bot",
                external_user_id="seed-owner",
            )

            ordinary = LarkCommandRouter(manager)
            blocking = BlockingSharedRouter(ordinary.shared_router)
            restricted = LarkCommandRouter(manager, shared_router=blocking)
            envelope = _group_envelope(
                "/agent planner",
                actor="ou_restricted",
                message_id="om-agent-delete-race",
            )
            switch_task = asyncio.create_task(_command(restricted, envelope))
            await asyncio.wait_for(blocking.entered.wait(), timeout=1)

            administrator = LarkCommandRouter(
                manager,
                administrator=lambda _envelope: True,
            )
            deleted = await _command(
                administrator,
                _group_envelope(
                    "/delagent planner",
                    actor="ou_administrator",
                    message_id="om-delete-agent-race",
                ),
            )
            assert deleted == "Agent deleted: planner"

            blocking.release.set()
            response = await asyncio.wait_for(switch_task, timeout=1)
            assert response == (
                "cannot switch Agent: 'Agent was deleted: planner'"
            )
            assert await store.is_agent_deleted("planner")
            assert manager.registry.registration("planner") is None
            assert await store.get_route(
                channel="lark",
                bot_id=envelope.bot_id,
                external_user_id=envelope.conversation_subject_scope,
                session_id=envelope.session_id,
                default_agent_id="codex",
            ) == "codex"
        finally:
            if blocking is not None:
                blocking.release.set()
            if switch_task is not None and not switch_task.done():
                switch_task.cancel()
                await asyncio.gather(switch_task, return_exceptions=True)
            await manager.stop()

    asyncio.run(scenario())


def test_lark_ask_existing_agent_queues_without_wechat_reply_scope(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            envelope = _group_envelope(
                "/ask planner inspect the failure",
                actor="ou_requester",
                message_id="om-ask-existing",
            )
            response = await _command(LarkCommandRouter(manager), envelope)
            assert response.startswith("Agent task queued: ")
            task_id = response.removeprefix("Agent task queued: ")

            task = await manager.store.get_task(task_id)
            assert task is not None
            assert task.agent_id == "planner"
            assert task.inputs == {"text": "inspect the failure"}
            assert task.actor_external_user_id == "ou_requester"
            assert task.reply_target.destination_id == "oc_subject_chat"
            assert task.pending_delivery_reply_scope_id is None
            # The Lark gateway publishes the completed command receipt through
            # its slotless exact-account outbox. The shared router must not
            # create a WeChat reply-slot projection while queuing the task.
            assert await manager.store.list_outbox(
                channel="lark", bot_id=envelope.bot_id
            ) == []
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_lark_gateway_ask_publishes_one_slotless_account_acknowledgement(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite")
        await manager.start()
        try:
            config_dir = tmp_path / "lark-config"
            config_dir.mkdir(mode=0o700)
            profile = LarkBotProfile(
                profile_id="ask-gateway",
                app_id="cli_bot_askgateway",
                bot_open_id="ou_bot_askgateway",
                config_dir=config_dir,
            )
            gateway = LarkGateway(manager, profile)
            event = {
                "type": "im.message.receive_v1",
                "event_id": "evt-ask-gateway",
                "message_id": "om_ask_gateway",
                "chat_id": "oc_ask_gateway",
                "chat_type": "p2p",
                "message_type": "text",
                "sender_id": "ou_ask_requester",
                "content": "/ask planner inspect the failure",
            }

            accepted = await gateway.accept_event(event)
            assert accepted is not None
            tasks = await manager.store.list_tasks(
                channel="lark",
                bot_id=profile.app_id,
                external_user_id="ou_ask_requester",
                limit=10,
            )
            assert len(tasks) == 1
            task = tasks[0]
            assert task.agent_id == "planner"
            assert task.pending_delivery_reply_scope_id is None

            rows = await manager.store.list_outbox(
                channel="lark", bot_id=profile.app_id
            )
            assert len(rows) == 1
            acknowledgement = rows[0]
            assert acknowledgement.content == f"Agent task queued: {task.task_id}"
            assert acknowledgement.reply_slot_id is None
            assert acknowledgement.reply_scope_id is None
            assert acknowledgement.bot_id == profile.app_id

            replay = await gateway.accept_event(event)
            assert replay is not None
            assert len(await manager.store.list_tasks(limit=10)) == 1
            assert len(
                await manager.store.list_outbox(
                    channel="lark", bot_id=profile.app_id
                )
            ) == 1

            claim = await manager.store.claim_task_by_id(
                task.task_id, "lark-ask-first-attempt"
            )
            assert claim is not None
            assert await manager.store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            await manager.store.complete_task(
                task.task_id,
                status="failed",
                error="retry me",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )
            retry_event = {
                **event,
                "event_id": "evt-retry-gateway",
                "message_id": "om_retry_gateway",
                "content": f"/retry {task.task_id}",
            }
            retried = await gateway.accept_event(retry_event)
            assert retried is not None
            queued = await manager.store.get_task(task.task_id)
            assert queued is not None
            assert queued.state.value == "queued"
            assert queued.pending_delivery_reply_scope_id is None
            rows = await manager.store.list_outbox(
                channel="lark", bot_id=profile.app_id
            )
            assert {row.content for row in rows} == {
                f"Agent task queued: {task.task_id}",
                f"retry queued: {task.task_id}",
            }
            assert all(
                row.reply_slot_id is None and row.reply_scope_id is None
                for row in rows
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_mapped_lark_task_commands_use_persisted_qwen_principal_scope(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        registry = AgentRegistry()
        registry.register("codex", _Runtime(), profile=codex_profile())
        registry.register(
            "qwen",
            _Runtime(),
            profile=AgentProfile(agent_id="qwen", display_name="Qwen"),
        )
        manager = TaskManager(
            SQLiteStore(tmp_path / "runtime.sqlite"),
            registry,
            worker_count=0,
        )
        await manager.start()
        try:
            config_dir = tmp_path / "lark-task-scope"
            config_dir.mkdir(mode=0o700)
            profile = LarkBotProfile(
                profile_id="task-scope",
                app_id="cli_bot_taskscope",
                bot_open_id="ou_bot_taskscope",
                config_dir=config_dir,
            )
            actor = "ou_task_scope_user"
            await manager.store.create_principal(principal_id="task-owner")
            await manager.store.map_principal_account(
                principal_id="task-owner",
                channel="lark",
                bot_id=profile.app_id,
                external_user_id=actor,
                identifier_kind="open_id",
                configured_by="test-owner",
            )
            await manager.set_active_agent(
                "qwen",
                channel="lark",
                bot_id=profile.app_id,
                external_user_id=actor,
            )
            gateway = LarkGateway(manager, profile)

            def event(text: str, message_id: str) -> dict[str, object]:
                return {
                    "type": "im.message.receive_v1",
                    "event_id": f"evt-{message_id}",
                    "message_id": message_id,
                    "chat_id": "oc_task_scope_chat",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "sender_id": actor,
                    "content": text,
                }

            work = await gateway.accept_event(
                event("remember this qwen turn", "om_task_scope_work")
            )
            assert work is not None and work.task is not None
            task = work.task
            assert task.agent_id == "qwen"
            assert task.conversation_id == principal_conversation_id(
                "task-owner", "default", "qwen"
            )
            local_qwen_conversation = conversation_id(
                "lark", profile.app_id, actor, "default", "qwen"
            )
            # The manager facade is the authoritative alias resolver even for
            # callers that do not enter through LarkGateway.
            listed_directly = await manager.list_tasks(
                channel="lark",
                bot_id=profile.app_id,
                external_user_id=actor,
                session_id="default",
                agent_id="qwen",
                conversation_id=local_qwen_conversation,
                limit=10,
            )
            assert [record.task_id for record in listed_directly] == [task.task_id]

            tasks_event = event("/tasks", "om_task_scope_tasks")
            tasks_acceptance = await gateway.accept_event(tasks_event)
            assert tasks_acceptance is not None
            snapshot = tasks_acceptance.inbound.payload["__command_snapshot"]
            assert snapshot["agent_id"] == "qwen"
            # The immutable command snapshot is intentionally transport-local;
            # TaskManager.list_tasks must resolve it to the principal anchor.
            assert snapshot["conversation_id"] == local_qwen_conversation
            assert snapshot["conversation_id"] != task.conversation_id

            rows = await manager.store.list_outbox(
                channel="lark", bot_id=profile.app_id
            )
            tasks_rows = [
                row
                for row in rows
                if row.reply_target.source_message_id == "om_task_scope_tasks"
            ]
            assert len(tasks_rows) == 1
            tasks_response = tasks_rows[0].content
            assert tasks_response.startswith("tasks:\n")
            assert task.task_id in tasks_response
            assert "agent=qwen" in tasks_response
            assert "tasks: (none)" not in tasks_response

            # A live route change after acceptance cannot reinterpret a
            # redelivered command. Its completed receipt and exact reply stay
            # byte-for-byte stable and are not published twice.
            await manager.set_active_agent(
                "codex",
                channel="lark",
                bot_id=profile.app_id,
                external_user_id=actor,
            )
            replay = await gateway.accept_event(tasks_event)
            assert replay is not None and replay.duplicate
            rows = await manager.store.list_outbox(
                channel="lark", bot_id=profile.app_id
            )
            replay_rows = [
                row
                for row in rows
                if row.reply_target.source_message_id == "om_task_scope_tasks"
            ]
            assert [row.content for row in replay_rows] == [tasks_response]

            await manager.set_active_agent(
                "qwen",
                channel="lark",
                bot_id=profile.app_id,
                external_user_id=actor,
            )
            await gateway.accept_event(
                event("/task", "om_task_scope_task_alias")
            )
            await gateway.accept_event(
                event("/status", "om_task_scope_status")
            )
            rows = await manager.store.list_outbox(
                channel="lark", bot_id=profile.app_id
            )
            by_source = {
                row.reply_target.source_message_id: row.content for row in rows
            }
            assert by_source["om_task_scope_task_alias"] == tasks_response
            status_response = by_source["om_task_scope_status"]
            assert status_response.startswith("active tasks:\n")
            assert task.task_id in status_response
            assert "agent=qwen" in status_response
            assert status_response != "idle"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_lark_agent_result_uses_slotless_exact_account_outbox(tmp_path: Path) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        # Deliberately exceed both WeChat/iLink's 3,000-character boundary and
        # the unsubstantiated 4,000-character shortcut previously associated
        # with Lark.  A peer-channel result must remain intact and never enter
        # WeChat aggregation, quota, or `/recv` projections.
        long_reply = "lark-long-reply:" + ("0123456789" * 650)
        manager = _manager(database)
        await manager.start()
        try:
            subject = thread_conversation_subject(
                "lark", "cli_bot_thread", "oc_thread_chat", "omt_root"
            )
            envelope = InboundEnvelope(
                channel="lark",
                bot_id="cli_bot_thread",
                external_user_id="ou_thread_actor",
                external_message_id="om_thread_prompt",
                text="work in topic",
                conversation_subject_id=subject.conversation_subject_id,
                conversation_subject_scope=subject.scope_key,
                conversation_subject_kind="thread",
                destination_kind="thread",
                destination_id="oc_thread_chat",
                thread_id="omt_root",
                root_message_id="omt_root",
                transport_metadata={
                    "chat_id": "oc_thread_chat",
                    "thread_id": "omt_root",
                    "root_id": "omt_root",
                },
            )
            accepted = await manager.accept_inbound(envelope)
            task = accepted.task
            claim = await manager.store.claim_task_by_id(
                task.task_id, "lark-result-worker"
            )
            assert claim is not None
            assert await manager.store.mark_task_running(
                task.task_id,
                claim.claim_token,
                execution_id=claim.execution_id,
            )
            await manager.store.complete_task(
                task.task_id,
                status="completed",
                events=[
                    {
                        "event_id": "lark-result-event",
                        "execution_id": claim.execution_id,
                        "event_type": "agent_message",
                        "visibility": "user",
                        "priority": 1,
                        "content": long_reply,
                        "source_item_id": "lark-result-item",
                        "source_item_type": "agentmessage",
                        "source_item_ordinal": 0,
                    }
                ],
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            rows = await manager.store.list_outbox(
                channel="lark", bot_id="cli_bot_thread"
            )
            assert len(rows) == 1
            delivery = rows[0]
            assert delivery.content == long_reply
            assert len(delivery.content) > 6_000
            assert delivery.external_user_id == "ou_thread_actor"
            assert delivery.reply_slot_id is None
            assert delivery.reply_scope_id is None
            assert delivery.reply_target.external_user_id == "ou_thread_actor"
            assert delivery.reply_target.destination_id == "oc_thread_chat"
            assert delivery.reply_target.thread_id == "omt_root"
            assert delivery.reply_target.root_message_id == "omt_root"
            assert delivery.delivery_address is not None
            assert delivery.delivery_address.channel == "lark"
            assert delivery.delivery_address.bot_id == "cli_bot_thread"
            assert delivery.delivery_address.destination_id == "oc_thread_chat"
            assert delivery.delivery_address.thread_id == "omt_root"
            assert delivery.delivery_address.root_message_id == "omt_root"

            sends: list[tuple[object, str, str]] = []

            class _Client:
                async def send_text(
                    self, target, content: str, *, idempotency_key: str
                ) -> dict[str, object]:
                    sends.append((target, content, idempotency_key))
                    return {"data": {"message_id": "om_long_reply_sent"}}

            worker = LarkDeliveryWorker(
                manager.store,
                LarkBotProfile(
                    profile_id="thread-bot",
                    app_id="cli_bot_thread",
                    config_dir=tmp_path / "lark-cli-thread",
                ),
                _Client(),
                worker_id="lark-exact-worker",
            )
            receipts = await worker.run_once()
            assert len(receipts) == 1 and receipts[0].sent
            assert sends[0][1] == long_reply
            assert sends[0][0].destination_id == "oc_thread_chat"
            assert sends[0][0].thread_id == "omt_root"
            assert sends[0][0].root_message_id == "omt_root"
            assert await manager.store.claim_account_outbox(
                "lark-other-worker",
                channel="lark",
                bot_id="cli_other_bot",
                limit=10,
            ) == []
        finally:
            await manager.stop()

        with sqlite3.connect(database) as connection:
            # Lark never consumes the WeChat ten-send allowance or creates
            # 3,000-character aggregation, quota-fragment, or `/recv` state.
            assert connection.execute(
                "SELECT COALESCE(SUM(used_slots),0) FROM reply_scopes "
                "WHERE channel=? AND bot_id=?",
                ("lark", "cli_bot_thread"),
            ).fetchone() == (0,)
            for table in (
                "reply_candidates",
                "reply_fragments",
                "reply_deferred_counters",
                "reply_aggregates",
            ):
                assert connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE channel=? AND bot_id=?",
                    ("lark", "cli_bot_thread"),
                ).fetchone() == (0,)
            assert connection.execute(
                "SELECT COUNT(*) FROM reply_candidates WHERE task_id=?",
                (task.task_id,),
            ).fetchone() == (0,)
            assert connection.execute(
                """SELECT COUNT(*) FROM reply_slots AS slot
                   JOIN reply_fragments AS fragment
                     ON fragment.reply_fragment_id=slot.reply_fragment_id
                   JOIN reply_candidates AS candidate
                     ON candidate.reply_candidate_id=fragment.reply_candidate_id
                   WHERE candidate.task_id=?""",
                (task.task_id,),
            ).fetchone() == (0,)

    asyncio.run(scenario())
