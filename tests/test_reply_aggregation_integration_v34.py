"""High-level pipeline regressions for durable v34 WeChat aggregation."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

import pytest

from src.runtime.media import AttachmentStore
from src.runtime.models import InboundMessage, ReplyTarget
from src.runtime.sqlite_store import SQLiteStore


_RECV_SUFFIX = "\n\nReply /recv to continue."


def _inbound(source_message_id: str) -> InboundMessage:
    return InboundMessage(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id=source_message_id,
        session_id="default",
        text="aggregate these replies",
        context_token=f"context:{source_message_id}",
    )


async def _accept_running_task(
    store: SQLiteStore,
    source_message_id: str,
    *,
    agent_id: str = "codex",
    metadata: Mapping[str, Any] | None = None,
):
    accepted = await store.accept_inbound(
        _inbound(source_message_id),
        task={
            "agent_id": agent_id,
            "metadata": dict(metadata or {}),
        },
    )
    assert accepted.task is not None
    claim = await store.claim_task_by_id(
        accepted.task.task_id,
        f"aggregation-worker:{source_message_id}",
    )
    assert claim is not None
    assert await store.mark_task_running(
        accepted.task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    return accepted, claim


async def _create_task(
    store: SQLiteStore,
    *,
    task_id: str,
    target: ReplyTarget,
    agent_id: str,
):
    task = await store.create_task(
        {
            "task_id": task_id,
            "agent_id": agent_id,
            "conversation_id": (
                f"wechat:bot:user:default:{agent_id}:{task_id}"
            ),
            "mode_id": "chat",
            "profile_version": 1,
            "policy_version": 1,
            "reply_target": target,
            "inputs": {"text": task_id},
            "metadata": {"direct_user_request": True},
        }
    )
    return task


async def _claim_running_task(store: SQLiteStore, task: Any):
    claim = await store.claim_task_by_id(
        task.task_id,
        f"aggregation-worker:{task.task_id}",
    )
    assert claim is not None
    assert await store.mark_task_running(
        task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    return claim


def _completed_item(
    task_id: str,
    execution_id: str,
    *,
    item_id: str,
    ordinal: int,
    content: str,
) -> dict[str, Any]:
    return {
        "event_id": f"event:{task_id}:{item_id}",
        "task_id": task_id,
        "execution_id": execution_id,
        "event_type": "agent_message",
        "visibility": "user",
        "priority": 1,
        "content": content,
        "source_item_id": item_id,
        "source_item_type": "agentMessage",
        "source_item_ordinal": ordinal,
    }


async def _complete(
    store: SQLiteStore,
    task_id: str,
    claim: Any,
    events: list[dict[str, Any]],
):
    return await store.complete_task(
        task_id,
        status="completed",
        events=events,
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )


async def _task_outbox(store: SQLiteStore, task_id: str):
    return sorted(
        (
            item
            for item in await store.list_outbox(limit=100)
            if item.task_id == task_id
        ),
        key=lambda item: (item.reply_ordinal or 0, item.outbox_id),
    )


def test_completed_items_share_one_terminal_wire_aggregate(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(store, "two-items")
            task = accepted.task
            assert task is not None
            events = [
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="item-first",
                    ordinal=0,
                    content="first",
                ),
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="item-second",
                    ordinal=1,
                    content="second",
                ),
            ]

            await _complete(store, task.task_id, claim, events)

            outbox = await _task_outbox(store, task.task_id)
            assert [item.content for item in outbox] == ["first\n\nsecond"]
            assert outbox[0].reply_aggregate_id
            assert outbox[0].reply_ordinal == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_ask_prefix_is_rendered_once_for_a_multi_item_aggregate(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(
                store,
                "ask-items",
                agent_id="planner",
                metadata={
                    "direct_user_request": True,
                    "requesting_agent_id": "codex",
                    "user_reply_format": "agent-prefix-v1",
                },
            )
            task = accepted.task
            assert task is not None
            events = [
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="planner-first",
                    ordinal=0,
                    content="first",
                ),
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="planner-second",
                    ordinal=1,
                    content="second",
                ),
            ]

            await _complete(store, task.task_id, claim, events)

            outbox = await _task_outbox(store, task.task_id)
            assert [item.content for item in outbox] == [
                "planner: first\n\nsecond"
            ]
            assert outbox[0].content.count("planner: ") == 1
            assert len(outbox[0].content) <= 3_000
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("character_count", "expected_fragment_lengths"),
    ((3_000, [3_000]), (3_001, [3_000, 1])),
)
def test_text_boundary_is_character_counted_and_lossless(
    tmp_path,
    character_count: int,
    expected_fragment_lengths: list[int],
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / f"runtime-{character_count}.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(
                store,
                f"boundary-{character_count}",
            )
            task = accepted.task
            assert task is not None
            source = "界" * character_count
            event = _completed_item(
                task.task_id,
                claim.execution_id,
                item_id=f"long-item-{character_count}",
                ordinal=0,
                content=source,
            )

            await _complete(store, task.task_id, claim, [event])

            outbox = await _task_outbox(store, task.task_id)
            assert [len(item.content) for item in outbox] == (
                expected_fragment_lengths
            )
            assert "".join(item.content for item in outbox) == source
            assert all(len(item.content) <= 3_000 for item in outbox)
            assert all(item.reply_aggregate_id for item in outbox)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_terminal_replay_reuses_the_same_aggregate_and_outbox(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(store, "replay")
            task = accepted.task
            assert task is not None
            events = [
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="replay-first",
                    ordinal=0,
                    content="first",
                ),
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="replay-second",
                    ordinal=1,
                    content="second",
                ),
            ]

            await _complete(store, task.task_id, claim, events)
            first_projection = await _task_outbox(store, task.task_id)
            first_identity = [
                (
                    item.outbox_id,
                    item.client_id,
                    item.reply_slot_id,
                    item.reply_aggregate_id,
                    item.content,
                )
                for item in first_projection
            ]

            await _complete(store, task.task_id, claim, events)

            replay_projection = await _task_outbox(store, task.task_id)
            assert [
                (
                    item.outbox_id,
                    item.client_id,
                    item.reply_slot_id,
                    item.reply_aggregate_id,
                    item.content,
                )
                for item in replay_projection
            ] == first_identity
            assert len(replay_projection) == 1
            assert replay_projection[0].content == "first\n\nsecond"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_live_aggregation_never_crosses_task_or_agent_identity(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted, first_claim = await _accept_running_task(
                store,
                "shared-scope",
            )
            first = accepted.task
            assert first is not None
            target = accepted.inbound.target()
            second = await _create_task(
                store,
                task_id="second-codex-task",
                target=target,
                agent_id="codex",
            )
            third = await _create_task(
                store,
                task_id="third-planner-task",
                target=target,
                agent_id="planner",
            )

            for task, existing_claim, item_id, content in (
                (first, first_claim, "first-task-item", "first task"),
                (second, None, "second-task-item", "second task"),
                (third, None, "planner-task-item", "planner task"),
            ):
                claim = existing_claim or await _claim_running_task(store, task)
                await _complete(
                    store,
                    task.task_id,
                    claim,
                    [
                        _completed_item(
                            task.task_id,
                            claim.execution_id,
                            item_id=item_id,
                            ordinal=0,
                            content=content,
                        )
                    ],
                )

            outbox = sorted(
                await store.list_outbox(limit=100),
                key=lambda item: (item.reply_ordinal or 0, item.outbox_id),
            )
            scoped = [
                item
                for item in outbox
                if item.reply_target.source_message_id == "shared-scope"
            ]
            assert [item.content for item in scoped] == [
                "first task",
                "second task",
                "planner task",
            ]
            assert len({item.reply_scope_id for item in scoped}) == 1
            assert len({item.reply_aggregate_id for item in scoped}) == 3
            assert [item.agent_id for item in scoped] == [
                "codex",
                "codex",
                "planner",
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_saturated_failure_notice_waiting_on_another_task_stays_sendable(
    tmp_path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                _inbound("cross-task-failure-order"),
                create_task=False,
            )
            target = accepted.inbound.target()
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            for ordinal in range(1, 11):
                projection = await store.project_reply_candidate(
                    target=target,
                    reply_scope_id=scope.reply_scope_id,
                    source_key=f"cross-task-prefill:{ordinal}",
                    content=f"prefill {ordinal}",
                    source_item_id=f"cross-task-prefill-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                    foreground=True,
                )
                assert projection.slots[0].reply_ordinal == ordinal

            older = await _create_task(
                store,
                task_id="older-planner-task",
                target=target,
                agent_id="planner",
            )
            failed = await _create_task(
                store,
                task_id="later-failed-task",
                target=target,
                agent_id="codex",
            )
            older_claim = await _claim_running_task(store, older)
            failed_claim = await _claim_running_task(store, failed)
            older_event = await store.append_task_event(
                older.task_id,
                _completed_item(
                    older.task_id,
                    older_claim.execution_id,
                    item_id="older-open-item",
                    ordinal=0,
                    content="older open result",
                ),
                claim_token=older_claim.claim_token,
            )

            notice = (
                f"task failed: {failed.task_id}\n"
                "check /tasks before retrying or sending a new prompt."
            )
            await store.complete_task(
                failed.task_id,
                result=notice,
                status="failed",
                error="private provider diagnostic",
                events=[
                    {
                        "event_id": "cross-task-failure-notice",
                        "task_id": failed.task_id,
                        "execution_id": failed_claim.execution_id,
                        "event_type": "failure_notice",
                        "visibility": "user",
                        "priority": 1,
                        "content": notice,
                    }
                ],
                claim_token=failed_claim.claim_token,
                execution_id=failed_claim.execution_id,
            )
            # The later notice cannot consume a wire position while the older
            # task's aggregate is still open.
            assert await _task_outbox(store, failed.task_id) == []

            await store.complete_task(
                older.task_id,
                result="older open result",
                status="completed",
                events=[older_event],
                claim_token=older_claim.claim_token,
                execution_id=older_claim.execution_id,
            )

            failure_outbox = await _task_outbox(store, failed.task_id)
            assert len(failure_outbox) == 1
            assert failure_outbox[0].content == notice
            assert failure_outbox[0].active_wire_variant == "contextless"
            assert failure_outbox[0].reply_slot_id is None
            assert failure_outbox[0].reply_ordinal == 10
            # The older ordinary result remains quota-deferred; durable failed
            # state must not become a general slotless-send permission.
            assert await _task_outbox(store, older.task_id) == []
        finally:
            await store.close()

    asyncio.run(scenario())


def test_text_media_text_barrier_preserves_wire_order(tmp_path) -> None:
    async def scenario() -> None:
        attachment_root = tmp_path / "attachments"
        files = AttachmentStore(attachment_root)
        stored = files.put_bytes(
            b"\x89PNG\r\n\x1a\naggregate-barrier",
            filename="barrier.png",
            attachment_id="barrier-image",
        )
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            attachment_root=attachment_root,
        )
        await store.initialize()
        try:
            await store.register_attachment(
                stored,
                kind="image",
                metadata={
                    "filename": stored.filename,
                    "owner_agent_id": "codex",
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                },
            )
            accepted, claim = await _accept_running_task(store, "media-barrier")
            task = accepted.task
            assert task is not None
            events = [
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="text-before-media",
                    ordinal=0,
                    content="before",
                ),
                {
                    "event_id": f"event:{task.task_id}:image",
                    "task_id": task.task_id,
                    "execution_id": claim.execution_id,
                    "event_type": "image_generation",
                    "visibility": "user",
                    "priority": 1,
                    "content": "",
                    "attachments": (stored.attachment_id,),
                    "source_item_id": "generated-image",
                    "source_item_type": "imageGeneration",
                    "source_item_ordinal": 1,
                },
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="text-after-media",
                    ordinal=2,
                    content="after",
                ),
            ]

            await _complete(store, task.task_id, claim, events)

            outbox = await _task_outbox(store, task.task_id)
            assert [(item.content, item.attachments) for item in outbox] == [
                ("before", ()),
                ("", (stored.attachment_id,)),
                ("after", ()),
            ]
            assert [item.reply_ordinal for item in outbox] == [1, 2, 3]
            assert len({item.reply_aggregate_id for item in outbox}) == 3
        finally:
            await store.close()

    asyncio.run(scenario())


def test_claim_outbox_seals_due_open_aggregate_once(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(
            tmp_path / "runtime.sqlite",
            reply_aggregation_max_age_seconds=1,
        )
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(store, "deadline")
            task = accepted.task
            assert task is not None
            event = await store.append_task_event(
                task.task_id,
                _completed_item(
                    task.task_id,
                    claim.execution_id,
                    item_id="deadline-item",
                    ordinal=0,
                    content="flush me",
                ),
                claim_token=claim.claim_token,
                defer_user_projection=True,
            )
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            aggregate = await store.append_reply_aggregate_member(
                target=accepted.inbound.target(),
                reply_scope_id=scope.reply_scope_id,
                source_key="item:deadline-item",
                content=event.content,
                source_sequence=event.source_item_ordinal or 0,
                agent_id=task.agent_id,
                task_id=task.task_id,
                execution_id=claim.execution_id,
                event_id=event.event_id,
                source_item_id=event.source_item_id,
                source_item_type=event.source_item_type,
                source_item_ordinal=event.source_item_ordinal,
                foreground=True,
                now="2030-01-01T00:00:00+00:00",
            )
            assert aggregate.open_aggregate is not None
            assert await _task_outbox(store, task.task_id) == []

            claimed = await store.claim_outbox(
                "deadline-delivery-worker",
                now="2030-01-01T00:00:02+00:00",
                limit=10,
            )

            assert [item.content for item in claimed] == ["flush me"]
            assert claimed[0].reply_aggregate_id == (
                aggregate.open_aggregate.reply_aggregate_id
            )
            first_identity = (
                claimed[0].outbox_id,
                claimed[0].client_id,
                claimed[0].reply_aggregate_id,
            )
            assert await store.claim_outbox(
                "second-deadline-worker",
                now="2030-01-01T00:00:03+00:00",
                limit=10,
            ) == []
            durable = await _task_outbox(store, task.task_id)
            assert len(durable) == 1
            assert (
                durable[0].outbox_id,
                durable[0].client_id,
                durable[0].reply_aggregate_id,
            ) == first_identity
        finally:
            await store.close()

    asyncio.run(scenario())


def test_tenth_slot_suffix_and_recv_fifo_are_lossless(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await store.accept_inbound(
                _inbound("quota-source"),
                create_task=False,
            )
            target = accepted.inbound.target()
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            for ordinal in range(1, 10):
                projection = await store.project_reply_candidate(
                    target=target,
                    reply_scope_id=scope.reply_scope_id,
                    source_key=f"prefill:{ordinal}",
                    content=f"prefill {ordinal}",
                    source_item_id=f"prefill-item-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                )
                assert projection.slots[0].reply_ordinal == ordinal

            task = await _create_task(
                store,
                task_id="quota-long-task",
                target=target,
                agent_id="codex",
            )
            claim = await _claim_running_task(store, task)
            source = "A" * 3_200 + "B" * 3_200 + "C" * 500
            event = _completed_item(
                task.task_id,
                claim.execution_id,
                item_id="quota-long-item",
                ordinal=0,
                content=source,
            )

            await _complete(store, task.task_id, claim, [event])

            source_scope_outbox = sorted(
                (
                    item
                    for item in await store.list_outbox(limit=100)
                    if item.reply_scope_id == scope.reply_scope_id
                ),
                key=lambda item: item.reply_ordinal or 0,
            )
            assert len(source_scope_outbox) == 10
            tenth = source_scope_outbox[-1]
            assert tenth.reply_ordinal == 10
            assert tenth.content.endswith(_RECV_SUFFIX)
            assert len(tenth.content) <= 3_000
            first_body = tenth.content[: -len(_RECV_SUFFIX)]

            recv = await store.accept_inbound(
                _inbound("recv-source"),
                create_task=False,
            )
            drained = await store.drain_deferred_replies(
                target=recv.inbound.target(),
                source_key="command:/recv",
            )
            assert drained.outbox_items
            assert first_body + "".join(
                item.content for item in drained.outbox_items
            ) == source
            replay = await store.drain_deferred_replies(
                target=recv.inbound.target(),
                source_key="command:/recv",
            )
            assert replay.replayed
            assert [item.outbox_id for item in replay.outbox_items] == [
                item.outbox_id for item in drained.outbox_items
            ]
            assert [item.reply_aggregate_id for item in replay.outbox_items] == [
                item.reply_aggregate_id for item in drained.outbox_items
            ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_notification_snapshot_survives_event_and_terminal_replay(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(
                store,
                "notify-snapshot",
                agent_id="planner",
            )
            task = accepted.task
            assert task is not None
            event_values = _completed_item(
                task.task_id,
                claim.execution_id,
                item_id="notify-item",
                ordinal=0,
                content="background result",
            )
            event_values["priority"] = 2
            event = await store.append_task_event(
                task.task_id,
                event_values,
                claim_token=claim.claim_token,
            )
            await store.set_notification_preference(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="planner",
                enabled=False,
            )

            await _complete(store, task.task_id, claim, [event])
            first = await _task_outbox(store, task.task_id)
            assert len(first) == 1
            assert first[0].notify_enabled
            assert not first[0].foreground
            identity = (
                first[0].outbox_id,
                first[0].reply_aggregate_id,
                first[0].client_id,
            )

            await _complete(store, task.task_id, claim, [event])
            replay = await _task_outbox(store, task.task_id)
            assert len(replay) == 1
            assert (
                replay[0].outbox_id,
                replay[0].reply_aggregate_id,
                replay[0].client_id,
            ) == identity
            assert replay[0].notify_enabled
            assert not replay[0].foreground
        finally:
            await store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("terminal_status", ("failed", "interrupted", "cancelled"))
def test_non_success_terminal_states_seal_pending_text(
    tmp_path,
    terminal_status: str,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / f"runtime-{terminal_status}.sqlite")
        await store.initialize()
        try:
            accepted, claim = await _accept_running_task(
                store,
                f"terminal-{terminal_status}",
            )
            task = accepted.task
            assert task is not None
            content = f"last text before {terminal_status}"
            event = _completed_item(
                task.task_id,
                claim.execution_id,
                item_id=f"{terminal_status}-item",
                ordinal=0,
                content=content,
            )

            terminal = await store.complete_task(
                task.task_id,
                status=terminal_status,
                events=[event],
                error=f"terminal {terminal_status}",
                claim_token=claim.claim_token,
                execution_id=claim.execution_id,
            )

            assert terminal.state.value == terminal_status
            outbox = await _task_outbox(store, task.task_id)
            assert [item.content for item in outbox] == [content]
            assert outbox[0].reply_aggregate_id
        finally:
            await store.close()

    asyncio.run(scenario())
