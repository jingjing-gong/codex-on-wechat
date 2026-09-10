"""Cross-channel provider history with immutable origin delivery targets."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from src.agents.base import AgentResult
from src.channels.commands import MVPCommandRouter
from src.channels.models import InboundEnvelope, parse_command
from src.runtime.identity import (
    conversation_id,
    direct_conversation_subject,
    group_conversation_subject,
    principal_conversation_id,
    thread_conversation_subject,
)
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore


async def _map(
    store: SQLiteStore,
    *,
    principal_id: str,
    channel: str,
    bot_id: str,
    user_id: str,
) -> None:
    if await store.get_principal(principal_id) is None:
        await store.create_principal(principal_id=principal_id)
    await store.map_principal_account(
        principal_id=principal_id,
        channel=channel,
        bot_id=bot_id,
        external_user_id=user_id,
        identifier_kind=("open_id" if channel == "lark" else "from_user_id"),
        configured_by="test-owner",
    )


def _direct(
    *,
    channel: str,
    bot_id: str,
    user_id: str,
    message_id: str,
    chat_id: str = "",
) -> InboundEnvelope:
    if channel == "lark":
        subject = direct_conversation_subject(channel, bot_id, user_id)
        return InboundEnvelope(
            channel=channel,
            bot_id=bot_id,
            external_user_id=user_id,
            external_message_id=message_id,
            text=f"prompt:{message_id}",
            conversation_subject_id=subject.conversation_subject_id,
            conversation_subject_scope=subject.scope_key,
            conversation_subject_kind="direct",
            destination_kind="direct",
            destination_id=chat_id,
            transport_metadata={"chat_id": chat_id, "chat_type": "p2p"},
        )
    return InboundEnvelope(
        channel=channel,
        bot_id=bot_id,
        external_user_id=user_id,
        external_message_id=message_id,
        text=f"prompt:{message_id}",
    )


async def _complete(store: SQLiteStore, task_id: str, event_id: str) -> None:
    claim = await store.claim_task_by_id(task_id, f"worker:{event_id}")
    assert claim is not None
    assert await store.mark_task_running(
        task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    await store.complete_task(
        task_id,
        status="completed",
        events=[
            {
                "event_id": event_id,
                "execution_id": claim.execution_id,
                "event_type": "agent_message",
                "visibility": "user",
                "priority": 1,
                "content": f"reply:{event_id}",
                "source_item_id": f"item:{event_id}",
                "source_item_type": "agentmessage",
                "source_item_ordinal": 0,
            }
        ],
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )


def test_mapped_direct_accounts_share_history_but_not_reply_targets(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accounts = (
                ("wechat", "wx-bot", "wx-owner"),
                ("lark", "cli_bot_a", "ou_owner_a"),
                ("lark", "cli_bot_b", "ou_owner_b"),
            )
            for channel, bot_id, user_id in accounts:
                await _map(
                    store,
                    principal_id="owner",
                    channel=channel,
                    bot_id=bot_id,
                    user_id=user_id,
                )

            wechat = await store.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="wx-bot",
                    user_id="wx-owner",
                    message_id="wx-message",
                )
            )
            lark_a = await store.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot_a",
                    user_id="ou_owner_a",
                    message_id="om-a",
                    chat_id="oc-a",
                )
            )
            lark_b = await store.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot_b",
                    user_id="ou_owner_b",
                    message_id="om-b",
                    chat_id="oc-b",
                )
            )
            tasks = (wechat.task, lark_a.task, lark_b.task)
            assert all(task is not None for task in tasks)
            expected = principal_conversation_id("owner", "default", "codex")
            assert {task.conversation_id for task in tasks if task} == {expected}

            assert wechat.task.reply_target.channel == "wechat"
            assert wechat.task.reply_target.source_message_id == "wx-message"
            assert lark_a.task.reply_target.bot_id == "cli_bot_a"
            assert lark_a.task.reply_target.destination_id == "oc-a"
            assert lark_b.task.reply_target.bot_id == "cli_bot_b"
            assert lark_b.task.reply_target.destination_id == "oc-b"

            assert await store.set_task_thread(
                wechat.task.task_id,
                thread_id="provider-owner-thread",
            )
            later = await store.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot_a",
                    user_id="ou_owner_a",
                    message_id="om-a-later",
                    chat_id="oc-a",
                )
            )
            assert later.task is not None
            assert later.task.conversation_id == expected
            assert later.task.thread_id == "provider-owner-thread"

            for accepted, event_id in (
                (wechat, "reply-wechat"),
                (lark_a, "reply-lark-a"),
                (lark_b, "reply-lark-b"),
            ):
                await _complete(store, accepted.task.task_id, event_id)

            wx_rows = await store.list_outbox(channel="wechat", bot_id="wx-bot")
            a_rows = await store.list_outbox(channel="lark", bot_id="cli_bot_a")
            b_rows = await store.list_outbox(channel="lark", bot_id="cli_bot_b")
            assert len(wx_rows) == len(a_rows) == len(b_rows) == 1
            assert wx_rows[0].reply_target.source_message_id == "wx-message"
            assert wx_rows[0].reply_target.external_user_id == "wx-owner"
            assert a_rows[0].reply_target.source_message_id == "om-a"
            assert a_rows[0].reply_target.destination_id == "oc-a"
            assert b_rows[0].reply_target.source_message_id == "om-b"
            assert b_rows[0].reply_target.destination_id == "oc-b"
            assert wx_rows[0].reply_slot_id is not None
            assert a_rows[0].reply_slot_id is None
            assert b_rows[0].reply_slot_id is None
        finally:
            await store.close()

    asyncio.run(scenario())


def test_groups_threads_unmapped_and_remapped_accounts_stay_isolated(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            await _map(
                store,
                principal_id="owner-a",
                channel="lark",
                bot_id="cli_bot",
                user_id="ou_actor",
            )
            direct = await store.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot",
                    user_id="ou_actor",
                    message_id="om-direct",
                    chat_id="oc-direct",
                )
            )
            group = group_conversation_subject("lark", "cli_bot", "oc-group")
            grouped = await store.accept_inbound(
                InboundEnvelope(
                    channel="lark",
                    bot_id="cli_bot",
                    external_user_id="ou_actor",
                    external_message_id="om-group",
                    text="group prompt",
                    conversation_subject_id=group.conversation_subject_id,
                    conversation_subject_scope=group.scope_key,
                    conversation_subject_kind="group",
                    destination_kind="group",
                    destination_id="oc-group",
                    transport_metadata={"chat_id": "oc-group"},
                )
            )
            thread = thread_conversation_subject(
                "lark", "cli_bot", "oc-group", "omt-root"
            )
            threaded = await store.accept_inbound(
                InboundEnvelope(
                    channel="lark",
                    bot_id="cli_bot",
                    external_user_id="ou_actor",
                    external_message_id="om-thread",
                    text="thread prompt",
                    conversation_subject_id=thread.conversation_subject_id,
                    conversation_subject_scope=thread.scope_key,
                    conversation_subject_kind="thread",
                    destination_kind="thread",
                    destination_id="oc-group",
                    thread_id="omt-root",
                    root_message_id="omt-root",
                    transport_metadata={
                        "chat_id": "oc-group",
                        "thread_id": "omt-root",
                    },
                )
            )
            assert len(
                {
                    direct.task.conversation_id,
                    grouped.task.conversation_id,
                    threaded.task.conversation_id,
                }
            ) == 3

            unmapped = await store.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="legacy-bot",
                    user_id="legacy-user",
                    message_id="legacy-message",
                )
            )
            assert unmapped.task.conversation_id == conversation_id(
                "wechat", "legacy-bot", "legacy-user", "default", "codex"
            )

            await store.create_principal(principal_id="owner-b")
            await store.map_principal_account(
                principal_id="owner-b",
                channel="lark",
                bot_id="cli_bot",
                external_user_id="ou_actor",
                identifier_kind="open_id",
                configured_by="test-owner",
            )
            remapped = await store.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot",
                    user_id="ou_actor",
                    message_id="om-remapped",
                    chat_id="oc-direct",
                )
            )
            assert remapped.task.conversation_id == principal_conversation_id(
                "owner-b", "default", "codex"
            )
            assert remapped.task.conversation_id != direct.task.conversation_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_explicit_wechat_anchor_preserves_existing_provider_thread(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        store = SQLiteStore(database)
        await store.initialize()
        try:
            historical = await store.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="wx-bot",
                    user_id="wx-owner",
                    message_id="wx-before-mapping",
                )
            )
            assert historical.task is not None
            local_anchor = historical.task.conversation_id
            assert await store.set_task_thread(
                historical.task.task_id,
                thread_id="existing-wechat-thread",
            )
            await _map(
                store,
                principal_id="owner",
                channel="wechat",
                bot_id="wx-bot",
                user_id="wx-owner",
            )
            await _map(
                store,
                principal_id="owner",
                channel="lark",
                bot_id="cli_bot",
                user_id="ou_owner",
            )
            binding = await store.bind_principal_conversation(
                principal_id="owner",
                agent_id="codex",
                session_id="default",
                conversation_id=local_anchor,
                configured_by="test-owner",
            )
            assert binding["conversation_id"] == local_anchor
        finally:
            await store.close()

        reopened = SQLiteStore(database)
        await reopened.initialize()
        try:
            lark = await reopened.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot",
                    user_id="ou_owner",
                    message_id="om-after-mapping",
                    chat_id="oc-owner",
                )
            )
            assert lark.task is not None
            assert lark.task.conversation_id == local_anchor
            assert lark.task.thread_id == "existing-wechat-thread"
            assert lark.task.reply_target.channel == "lark"
            assert lark.task.reply_target.destination_id == "oc-owner"
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_lark_clear_resolves_shared_anchor_without_changing_reply_route(tmp_path) -> None:
    class Runtime:
        def __init__(self) -> None:
            self.reset_calls: list[str] = []

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, task, _emit) -> AgentResult:
            return AgentResult(task_id=task.task_id, content="ok")

        async def interrupt(self, _task_id: str) -> bool:
            return False

        async def reset_session(self, conversation_id: str) -> str:
            self.reset_calls.append(conversation_id)
            return "reset"

    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(database)
        await seeded.initialize()
        try:
            historical = await seeded.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="wx-bot",
                    user_id="wx-owner",
                    message_id="wx-history",
                )
            )
            anchor = historical.task.conversation_id
            assert await seeded.set_task_thread(
                historical.task.task_id,
                thread_id="shared-thread-before-clear",
            )
            await _map(
                seeded,
                principal_id="owner",
                channel="wechat",
                bot_id="wx-bot",
                user_id="wx-owner",
            )
            await _map(
                seeded,
                principal_id="owner",
                channel="lark",
                bot_id="cli_bot",
                user_id="ou_owner",
            )
            await seeded.bind_principal_conversation(
                principal_id="owner",
                agent_id="codex",
                conversation_id=anchor,
                configured_by="test-owner",
            )
        finally:
            await seeded.close()

        runtime = Runtime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        manager = TaskManager(
            SQLiteStore(database),
            registry,
            worker_count=0,
        )
        await manager.start()
        try:
            command = _direct(
                channel="lark",
                bot_id="cli_bot",
                user_id="ou_owner",
                message_id="om-clear",
                chat_id="oc-owner",
            )
            await manager.accept_inbound(command, create_task=False)
            await manager.clear_session(
                conversation_id=command.conversation_id,
                channel="lark",
                bot_id="cli_bot",
                external_user_id="ou_owner",
                agent_id="codex",
            )
            assert runtime.reset_calls == [anchor]
            assert await manager.store.get_thread_binding(
                anchor,
                mode_id="chat",
                profile_version=1,
                policy_version=1,
            ) is None
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_adopted_anchor_keeps_exact_account_legacy_task_history_visible(
    tmp_path,
) -> None:
    class Runtime:
        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, task, _emit) -> AgentResult:
            return AgentResult(task_id=task.task_id, content="ok")

        async def interrupt(self, _task_id: str) -> bool:
            return False

    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        seeded = SQLiteStore(database)
        await seeded.initialize()
        try:
            lark_legacy = await seeded.accept_inbound(
                _direct(
                    channel="lark",
                    bot_id="cli_bot",
                    user_id="ou_owner",
                    message_id="om-before-adoption",
                    chat_id="oc-owner",
                )
            )
            wechat_anchor = await seeded.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="wx-bot",
                    user_id="wx-owner",
                    message_id="wx-before-adoption",
                )
            )
            unmapped_wechat = await seeded.accept_inbound(
                _direct(
                    channel="wechat",
                    bot_id="legacy-bot",
                    user_id="legacy-user",
                    message_id="wx-unmapped-task",
                )
            )
            assert lark_legacy.task is not None
            assert wechat_anchor.task is not None
            assert unmapped_wechat.task is not None

            await _map(
                seeded,
                principal_id="owner",
                channel="wechat",
                bot_id="wx-bot",
                user_id="wx-owner",
            )
            await _map(
                seeded,
                principal_id="owner",
                channel="lark",
                bot_id="cli_bot",
                user_id="ou_owner",
            )
            await seeded.bind_principal_conversation(
                principal_id="owner",
                agent_id="codex",
                conversation_id=wechat_anchor.task.conversation_id,
                configured_by="test-owner",
            )
        finally:
            await seeded.close()

        registry = AgentRegistry()
        registry.register("codex", Runtime(), profile=codex_profile())
        manager = TaskManager(SQLiteStore(database), registry, worker_count=0)
        await manager.start()
        try:
            async def command_output(envelope: InboundEnvelope) -> str:
                accepted = await manager.accept_inbound(
                    envelope,
                    create_task=False,
                )
                snapshot = accepted.inbound.payload["__command_snapshot"]
                restored = replace(
                    envelope,
                    agent_id=str(snapshot["agent_id"]),
                    conversation_id=str(snapshot["conversation_id"]),
                    raw={
                        **dict(envelope.raw or {}),
                        "__command_snapshot": snapshot,
                    },
                )
                result = await MVPCommandRouter(manager).handle_command(
                    parse_command(restored.text),
                    restored,
                )
                assert isinstance(result, str)
                return result

            lark_command = replace(
                _direct(
                    channel="lark",
                    bot_id="cli_bot",
                    user_id="ou_owner",
                    message_id="om-tasks-after-adoption",
                    chat_id="oc-owner",
                ),
                text="/tasks",
            )
            lark_output = await command_output(lark_command)
            assert lark_legacy.task.task_id in lark_output
            assert wechat_anchor.task.task_id not in lark_output
            assert unmapped_wechat.task.task_id not in lark_output

            wechat_command = replace(
                _direct(
                    channel="wechat",
                    bot_id="legacy-bot",
                    user_id="legacy-user",
                    message_id="wx-unmapped-tasks",
                ),
                text="/tasks",
            )
            wechat_output = await command_output(wechat_command)
            assert wechat_output == (
                "tasks:\n"
                f"- {unmapped_wechat.task.task_id} TaskState.QUEUED "
                "agent=codex attempts=0"
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())
