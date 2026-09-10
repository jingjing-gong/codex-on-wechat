"""Adversarial coverage for durable input steering into active tasks."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from src.agents.base import AgentResult
from src.runtime.identity import (
    conversation_id,
    direct_conversation_subject,
    group_conversation_subject,
    principal_conversation_id,
)
from src.runtime.models import InboundMessage, TaskSteeringState
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.worker import TaskWorker


def _inbound(
    message_id: str,
    text: str,
    *,
    channel: str = "wechat",
    bot_id: str = "bot",
    user_id: str = "user",
    session_id: str = "default",
    group_id: str = "",
) -> InboundMessage:
    if group_id:
        subject = group_conversation_subject(channel, bot_id, group_id)
        return InboundMessage(
            channel=channel,
            bot_id=bot_id,
            external_user_id=user_id,
            external_message_id=message_id,
            text=text,
            session_id=session_id,
            conversation_subject_id=subject.conversation_subject_id,
            conversation_subject_scope=subject.scope_key,
            conversation_subject_kind="group",
            destination_kind="group",
            destination_id=group_id,
            transport_metadata={"chat_id": group_id, "chat_type": "group"},
        )
    if channel == "lark":
        subject = direct_conversation_subject(channel, bot_id, user_id)
        chat_id = f"chat:{user_id}"
        return InboundMessage(
            channel=channel,
            bot_id=bot_id,
            external_user_id=user_id,
            external_message_id=message_id,
            text=text,
            session_id=session_id,
            conversation_subject_id=subject.conversation_subject_id,
            conversation_subject_scope=subject.scope_key,
            conversation_subject_kind="direct",
            destination_kind="direct",
            destination_id=chat_id,
            transport_metadata={"chat_id": chat_id, "chat_type": "p2p"},
        )
    return InboundMessage(
        channel=channel,
        bot_id=bot_id,
        external_user_id=user_id,
        external_message_id=message_id,
        text=text,
        session_id=session_id,
    )


def _task_values(
    inbound: InboundMessage,
    *,
    agent_id: str = "codex",
    mode_id: str = "chat",
    profile_version: int = 1,
    policy_version: int = 1,
    model: str = "",
    reasoning_effort: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route_user = inbound.conversation_subject_scope or inbound.external_user_id
    return {
        "agent_id": agent_id,
        "conversation_id": conversation_id(
            inbound.channel,
            inbound.bot_id,
            route_user,
            inbound.session_id,
            agent_id,
        ),
        "mode_id": mode_id,
        "profile_version": profile_version,
        "policy_version": policy_version,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "reply_target": inbound.target(),
        "inputs": {"text": inbound.text},
        "metadata": dict(metadata or {}),
    }


async def _accept(
    store: SQLiteStore,
    inbound: InboundMessage,
    **task_overrides: Any,
):
    return await store.accept_inbound(
        inbound,
        task=_task_values(inbound, **task_overrides),
    )


async def _start_manually(
    store: SQLiteStore,
    *,
    worker_id: str = "steering-owner",
    message_id: str = "origin",
):
    accepted = await _accept(store, _inbound(message_id, "initial"))
    assert accepted.task is not None
    claim = await store.claim_task_by_id(accepted.task.task_id, worker_id)
    assert claim is not None
    assert await store.mark_task_running(
        accepted.task.task_id,
        claim.claim_token,
        execution_id=claim.execution_id,
    )
    return accepted, claim


async def _complete(store: SQLiteStore, accepted: Any, claim: Any) -> None:
    await store.complete_task(
        accepted.task.task_id,
        status="completed",
        result="done",
        claim_token=claim.claim_token,
        execution_id=claim.execution_id,
    )


async def _eventually(predicate, *, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition did not become true before timeout")
        await asyncio.sleep(0.01)


class _RecordingRuntime:
    agent_id = "codex"

    def __init__(self, *, steer_result: bool = True, uncertain: bool = False) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.steer_entered = asyncio.Event()
        self.allow_steer = asyncio.Event()
        self.allow_steer.set()
        self.steer_result = steer_result
        self.uncertain = uncertain
        self.steer_calls: list[tuple[str, Any, str, str]] = []

    async def run(self, task, _emit) -> AgentResult:
        self.started.set()
        await self.release.wait()
        return AgentResult(
            task_id=task.task_id,
            execution_id=task.execution_id,
            content="origin reply",
        )

    async def steer(
        self,
        task_id: str,
        inputs: Any,
        *,
        steering_id: str = "",
        execution_id: str = "",
    ) -> bool:
        self.steer_entered.set()
        await self.allow_steer.wait()
        self.steer_calls.append(
            (str(task_id), inputs, str(steering_id), str(execution_id))
        )
        if self.uncertain:
            error = RuntimeError("steering acknowledgement lost")
            error.execution_uncertain = True  # type: ignore[attr-defined]
            raise error
        return self.steer_result

    async def interrupt(self, _task_id: str) -> bool:
        self.release.set()
        return True


def test_rapid_followups_reach_runtime_in_durable_fifo_order(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        runtime = _RecordingRuntime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="fifo-worker",
            poll_interval=0.01,
        )
        try:
            origin = await _accept(store, _inbound("origin", "initial"))
            assert origin.task is not None
            running = asyncio.create_task(worker.run_once())
            await runtime.started.wait()
            invocation_count = len(await store.list_agent_invocations(limit=100))

            followups = [
                await _accept(store, _inbound(f"follow-{index}", f"text-{index}"))
                for index in range(1, 4)
            ]
            assert all(item.steered for item in followups)
            assert [item.steering.sequence for item in followups] == [1, 2, 3]
            # Absorbed input does not consume another task/admission slot.
            assert len(await store.list_agent_invocations(limit=100)) == invocation_count

            await _eventually(lambda: len(runtime.steer_calls) == 3)
            assert [call[1]["text"] for call in runtime.steer_calls] == [
                "text-1",
                "text-2",
                "text-3",
            ]
            assert [call[2] for call in runtime.steer_calls] == [
                item.steering_id for item in followups
            ]
            assert {call[3] for call in runtime.steer_calls} == {
                origin.task.execution_id
            }

            runtime.release.set()
            assert await running is True
            records = [
                await store.get_task_steering(item.steering_id)
                for item in followups
            ]
            assert {record.state for record in records if record} == {
                TaskSteeringState.APPLIED
            }
            outbox = await store.list_outbox()
            assert len(outbox) == 1
            assert outbox[0].reply_target.channel == "wechat"
            assert outbox[0].reply_target.source_message_id == "origin"
        finally:
            runtime.release.set()
            await store.close()

    asyncio.run(scenario())


def test_new_input_does_not_overtake_an_older_incompatible_queued_turn(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            first = await _accept(store, _inbound("first", "first"))
            second = await _accept(
                store,
                _inbound("second", "second"),
                mode_id="plan",
            )
            assert first.task is not None and second.task is not None
            assert not first.steered and not second.steered

            first_claim = await store.claim_task_by_id(
                first.task.task_id,
                "ordering-worker",
            )
            assert first_claim is not None
            assert await store.mark_task_running(
                first_claim.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )

            # Even though the first turn is now steerable and the older queued
            # turn has a different execution context, conversation FIFO means
            # the third inbound must remain behind that queued turn.
            third = await _accept(store, _inbound("third", "third"))
            assert third.task is not None
            assert not third.steered
            assert third.task.task_id not in {
                first.task.task_id,
                second.task.task_id,
            }

            tasks = await store.list_tasks(limit=10, newest_first=False)
            assert [task.inputs["text"] for task in tasks] == [
                "first",
                "second",
                "third",
            ]

            await _complete(store, first, first_claim)
            second_claim = await store.claim_next_task("ordering-worker")
            assert second_claim is not None
            assert second_claim.task_id == second.task.task_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_two_store_duplicate_race_creates_one_steer_and_one_runtime_call(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = SQLiteStore(database)
        second = SQLiteStore(database)
        await first.initialize()
        await second.initialize()
        runtime = _RecordingRuntime()
        worker = TaskWorker(
            first,
            runtime=runtime,
            worker_id="dedupe-worker",
            poll_interval=0.01,
        )
        try:
            origin = await _accept(first, _inbound("origin", "initial"))
            assert origin.task is not None
            running = asyncio.create_task(worker.run_once())
            await runtime.started.wait()

            left = _inbound("duplicate", "same body")
            right = _inbound("duplicate", "same body")
            results = await asyncio.gather(
                _accept(first, left),
                _accept(second, right),
            )
            assert sorted(item.duplicate for item in results) == [False, True]
            assert len({item.steering_id for item in results}) == 1
            assert all(item.steered for item in results)
            pending = await first.list_pending_task_steering(
                task_id=origin.task.task_id,
                execution_id=origin.task.execution_id,
            )
            assert len(pending) == 1

            await _eventually(lambda: len(runtime.steer_calls) == 1)
            await asyncio.sleep(0.05)
            assert len(runtime.steer_calls) == 1
            runtime.release.set()
            assert await running is True
        finally:
            runtime.release.set()
            await second.close()
            await first.close()

    asyncio.run(scenario())


def test_completion_linearization_never_drops_or_duplicates_followup(
    tmp_path: Path,
) -> None:
    async def scenario(database: Path, *, input_wins: bool) -> None:
        store = SQLiteStore(database)
        await store.initialize()
        try:
            origin, claim = await _start_manually(store)
            follow = _inbound("follow", "after or during")
            if input_wins:
                accepted = await _accept(store, follow)
                assert accepted.steered
                await _complete(store, origin, claim)
                record = await store.get_task_steering(accepted.steering_id)
                assert record is not None
                assert record.state is TaskSteeringState.PROMOTED
                assert record.promoted_task_id == record.fallback_task_id
                fallback = await store.get_task(record.promoted_task_id)
                assert fallback is not None
                assert fallback.inputs["text"] == "after or during"

                replay = await _accept(store, _inbound("follow", "after or during"))
                assert replay.duplicate is True
                assert replay.steering_id == record.steering_id
                assert replay.task.task_id == fallback.task_id
                assert await store.recover_task_steering() == 0
            else:
                await _complete(store, origin, claim)
                accepted = await _accept(store, follow)
                assert not accepted.steered
                assert accepted.task is not None
                assert accepted.task.task_id != origin.task.task_id
                assert accepted.task.inputs["text"] == "after or during"

            tasks = await store.list_tasks(limit=20)
            assert len(tasks) == 2
            assert len({task.inbound_message_id for task in tasks}) == 2
        finally:
            await store.close()

    asyncio.run(scenario(tmp_path / "input-first.sqlite", input_wins=True))
    asyncio.run(scenario(tmp_path / "finish-first.sqlite", input_wins=False))


def test_commands_and_incompatible_contexts_never_steer(tmp_path: Path) -> None:
    async def assert_mismatch(
        database_name: str,
        inbound: InboundMessage,
        **task_overrides: Any,
    ) -> None:
        store = SQLiteStore(tmp_path / database_name)
        await store.initialize()
        try:
            origin, _claim = await _start_manually(store)
            assert origin.task is not None
            candidate = await _accept(store, inbound, **task_overrides)
            assert not candidate.steered
            assert candidate.task is not None
            assert candidate.task.task_id != origin.task.task_id
        finally:
            await store.close()

    async def command_scenario() -> None:
        store = SQLiteStore(tmp_path / "command.sqlite")
        await store.initialize()
        try:
            origin, _claim = await _start_manually(store)
            command = await store.accept_inbound(
                _inbound("command", "/status"),
                create_task=False,
            )
            assert command.task is None
            assert not command.steered

            compatible = await _accept(store, _inbound("compatible", "ordinary"))
            assert compatible.steered
            assert compatible.task.task_id == origin.task.task_id
        finally:
            await store.close()

    asyncio.run(
        assert_mismatch(
            "other-user.sqlite",
            _inbound("other-user", "foreign", user_id="someone-else"),
        )
    )
    asyncio.run(
        assert_mismatch(
            "mode.sqlite",
            _inbound("mode-change", "execute differently"),
            mode_id="plan",
            policy_version=2,
        )
    )
    asyncio.run(
        assert_mismatch(
            "model.sqlite",
            _inbound("model-change", "use another model"),
            model="different-model",
        )
    )
    asyncio.run(
        assert_mismatch(
            "group.sqlite",
            _inbound(
                "group-change",
                "group message",
                channel="lark",
                bot_id="lark-bot",
                user_id="user",
                group_id="group-1",
            ),
        )
    )
    asyncio.run(command_scenario())


def test_delivery_unknown_is_never_replayed_but_does_not_strand_later_input(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            origin, claim = await _start_manually(store)
            first = await _accept(store, _inbound("first", "maybe delivered"))
            second = await _accept(store, _inbound("second", "definitely pending"))
            assert first.steering is not None and second.steering is not None

            delivery = await store.claim_next_task_steering(
                origin.task.task_id,
                claim.execution_id,
                task_claim_token=claim.claim_token,
                claimed_by="steering-owner",
                lease_seconds=60,
            )
            assert delivery is not None
            assert delivery.steering_id == first.steering_id
            await store.release_task_steering(
                delivery.steering_id,
                claim_token=delivery.claim_token,
                delivery_unknown=True,
            )

            await _complete(store, origin, claim)
            first_record = await store.get_task_steering(first.steering_id)
            second_record = await store.get_task_steering(second.steering_id)
            assert first_record is not None and second_record is not None
            assert first_record.state is TaskSteeringState.DELIVERY_UNKNOWN
            assert first_record.promoted_task_id is None
            assert second_record.state is TaskSteeringState.PROMOTED
            assert second_record.promoted_task_id == second_record.fallback_task_id

            for _ in range(3):
                assert await store.recover_task_steering() == 0
            assert (
                await store.get_task_steering(first.steering_id)
            ).state is TaskSteeringState.DELIVERY_UNKNOWN
            assert len(await store.list_tasks(limit=20)) == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_restart_fences_delivering_and_preserves_pending_fallback_order(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first_store = SQLiteStore(database)
        await first_store.initialize()
        origin, claim = await _start_manually(first_store)
        first = await _accept(first_store, _inbound("first", "in flight"))
        second = await _accept(first_store, _inbound("second", "after it"))
        delivery = await first_store.claim_next_task_steering(
            origin.task.task_id,
            claim.execution_id,
            task_claim_token=claim.claim_token,
            claimed_by="steering-owner",
            lease_seconds=600,
        )
        assert delivery is not None
        assert delivery.steering_id == first.steering_id
        await first_store.close()

        recovered = SQLiteStore(database)
        await recovered.initialize()
        try:
            first_record = await recovered.get_task_steering(first.steering_id)
            second_record = await recovered.get_task_steering(second.steering_id)
            assert first_record is not None and second_record is not None
            assert first_record.state is TaskSteeringState.DELIVERY_UNKNOWN
            assert second_record.state is TaskSteeringState.PROMOTED
            fallback = await recovered.get_task(second_record.promoted_task_id)
            assert fallback is not None
            assert fallback.inputs["text"] == "after it"
            assert len(await recovered.list_tasks(limit=20)) == 2
        finally:
            await recovered.close()

    asyncio.run(scenario())


def test_pre_registration_wait_is_retained_until_runtime_can_accept(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        runtime = _RecordingRuntime()
        runtime.allow_steer.clear()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="registration-worker",
            poll_interval=0.01,
        )
        try:
            origin = await _accept(store, _inbound("origin", "initial"))
            running = asyncio.create_task(worker.run_once())
            await runtime.started.wait()
            follow = await _accept(store, _inbound("follow", "during startup"))
            await runtime.steer_entered.wait()
            await asyncio.sleep(0.05)
            record = await store.get_task_steering(follow.steering_id)
            assert record is not None
            assert record.state is TaskSteeringState.DELIVERING
            assert runtime.steer_calls == []

            runtime.allow_steer.set()
            await _eventually(lambda: len(runtime.steer_calls) == 1)
            record = await store.get_task_steering(follow.steering_id)
            assert record is not None
            assert record.state is TaskSteeringState.APPLIED
            runtime.release.set()
            assert await running is True
            assert origin.task.task_id == runtime.steer_calls[0][0]
        finally:
            runtime.allow_steer.set()
            runtime.release.set()
            await store.close()

    asyncio.run(scenario())


def test_false_promotes_once_while_uncertain_never_promotes(tmp_path: Path) -> None:
    async def run_case(database: Path, *, uncertain: bool) -> None:
        store = SQLiteStore(database)
        await store.initialize()
        runtime = _RecordingRuntime(steer_result=False, uncertain=uncertain)
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="outcome-worker",
            poll_interval=0.01,
        )
        try:
            origin = await _accept(store, _inbound("origin", "initial"))
            running = asyncio.create_task(worker.run_once())
            await runtime.started.wait()
            follow = await _accept(store, _inbound("follow", "follow up"))
            await _eventually(lambda: len(runtime.steer_calls) >= 1)
            runtime.release.set()
            assert await running is True

            record = await store.get_task_steering(follow.steering_id)
            assert record is not None
            if uncertain:
                assert record.state is TaskSteeringState.DELIVERY_UNKNOWN
                assert record.promoted_task_id is None
                assert len(await store.list_tasks(limit=20)) == 1
            else:
                assert record.state is TaskSteeringState.PROMOTED
                assert record.promoted_task_id == record.fallback_task_id
                assert len(await store.list_tasks(limit=20)) == 2
                for _ in range(3):
                    assert await store.recover_task_steering() == 0
                replay = await _accept(
                    store, _inbound("follow", "follow up")
                )
                assert replay.duplicate is True
                assert replay.task.task_id == record.promoted_task_id
                assert len(await store.list_tasks(limit=20)) == 2
        finally:
            runtime.release.set()
            await store.close()

    asyncio.run(run_case(tmp_path / "false.sqlite", uncertain=False))
    asyncio.run(run_case(tmp_path / "uncertain.sqlite", uncertain=True))


async def _map_account(
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
        configured_by="steering-test",
    )


def test_mapped_wechat_and_lark_share_turn_but_keep_origin_delivery(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        runtime = _RecordingRuntime()
        worker = TaskWorker(
            store,
            runtime=runtime,
            worker_id="cross-channel-worker",
            poll_interval=0.01,
        )
        try:
            await _map_account(
                store,
                principal_id="owner",
                channel="wechat",
                bot_id="wx-bot",
                user_id="wx-owner",
            )
            await _map_account(
                store,
                principal_id="owner",
                channel="lark",
                bot_id="lark-bot",
                user_id="ou-owner",
            )
            origin = await _accept(
                store,
                _inbound(
                    "wx-origin",
                    "initial",
                    channel="wechat",
                    bot_id="wx-bot",
                    user_id="wx-owner",
                ),
            )
            assert origin.task is not None
            assert origin.task.conversation_id == principal_conversation_id(
                "owner", "default", "codex"
            )
            running = asyncio.create_task(worker.run_once())
            await runtime.started.wait()

            lark = await _accept(
                store,
                _inbound(
                    "lark-follow",
                    "continue from Lark",
                    channel="lark",
                    bot_id="lark-bot",
                    user_id="ou-owner",
                ),
            )
            assert lark.steered
            assert lark.task.task_id == origin.task.task_id
            assert lark.steering.reply_target.channel == "lark"
            assert lark.steering.reply_target.bot_id == "lark-bot"
            await _eventually(lambda: len(runtime.steer_calls) == 1)
            assert runtime.steer_calls[0][1]["text"] == "continue from Lark"

            runtime.release.set()
            assert await running is True
            outbox = await store.list_outbox()
            assert len(outbox) == 1
            assert outbox[0].channel == "wechat"
            assert outbox[0].bot_id == "wx-bot"
            assert outbox[0].reply_target.source_message_id == "wx-origin"
            assert not await store.list_outbox(channel="lark", bot_id="lark-bot")
        finally:
            runtime.release.set()
            await store.close()

    asyncio.run(scenario())
