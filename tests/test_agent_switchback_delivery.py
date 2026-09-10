"""Durable presentation regressions for Agent switch-back delivery."""

from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from src.channels.models import ReplyTarget, UserDelivery
from src.channels.wechat import MVPCommandRouter, WeChatGateway
from src.runtime.models import InboundMessage, ReplyFragmentState
from src.runtime.sqlite_store import SQLiteStore, StoreError
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


async def _accept_scope(
    store: SQLiteStore,
    source: str,
    *,
    bot_id: str = "bot",
    external_user_id: str = "user",
    session_id: str = "default",
) -> tuple[ReplyTarget, str]:
    accepted = await store.accept_inbound(
        InboundMessage(
            channel="wechat",
            bot_id=bot_id,
            external_user_id=external_user_id,
            external_message_id=source,
            session_id=session_id,
            text=source,
        ),
        create_task=False,
    )
    return accepted.inbound.target(), accepted.inbound.message_id


async def _candidate(
    store: SQLiteStore,
    target: ReplyTarget,
    source: str,
    *,
    agent_id: str = "alpha",
    notify_enabled: bool = False,
    foreground: bool = False,
):
    projection = await store.project_reply_candidate(
        target=target,
        source_key=source,
        content=source,
        agent_id=agent_id,
        notify_enabled=notify_enabled,
        foreground=foreground,
    )
    assert projection.candidate is not None
    return projection


def _command_message(text: str, *, message_id: int) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        context_token=f"context-{message_id}",
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


class _SwitchManager:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.switch_calls = 0
        self.inbox_calls = 0

    async def get_active_agent(self, **scope):
        return await self.store.get_route(
            channel=scope.get("channel", "wechat"),
            bot_id=scope.get("bot_id", "bot"),
            external_user_id=scope.get(
                "external_user_id", scope.get("user_id", "user")
            ),
            session_id=scope.get("session_id", "default"),
        )

    async def set_active_agent(self, agent_id: str, **scope):
        self.switch_calls += 1
        await self.store.set_route(
            channel=scope.get("channel", "wechat"),
            bot_id=scope.get("bot_id", "bot"),
            external_user_id=scope.get("external_user_id", "user"),
            session_id=scope.get("session_id", "default"),
            active_agent_id=agent_id,
        )
        return agent_id

    async def switch_back_inbox(self, **scope):
        self.inbox_calls += 1
        return await self.store.present_inbox_candidates(
            channel=scope.get("channel", "wechat"),
            bot_id=scope.get("bot_id", "bot"),
            external_user_id=scope.get("external_user_id", "user"),
            session_id=scope.get("session_id", "default"),
            agent_id=scope["agent_id"],
            limit=scope.get("limit", 100),
            present=scope.get("present", False),
            switch_only=True,
        )


def test_switchback_selector_returns_only_unseen_background_inbox_candidates(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            target, _ = await _accept_scope(store, "origin")

            old = await _candidate(store, target, "already-presented")
            first_presentation = await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
            )
            assert [item.reply_candidate_id for item in first_presentation] == [
                old.candidate.reply_candidate_id
            ]

            missed = await _candidate(store, target, "missed-while-background")
            foreground = await _candidate(
                store,
                target,
                "foreground-history",
                foreground=True,
            )
            other_agent = await _candidate(
                store,
                target,
                "other-agent-output",
                agent_id="beta",
            )
            other_target, _ = await _accept_scope(
                store,
                "other-user-origin",
                external_user_id="other-user",
            )
            await _candidate(store, other_target, "other-user-output")
            attachment_only = await store.project_reply_candidate(
                target=target,
                source_key="attachment-only",
                attachments=("opaque-attachment",),
                agent_id="alpha",
                notify_enabled=False,
                foreground=False,
            )
            assert attachment_only.candidate is not None

            # Allocated and quota-deferred fragments already belong to their
            # normal SendMsg or /recv paths. Neither may be reclassified as a
            # switch-back presentation.
            quota_target, _ = await _accept_scope(store, "quota-origin")
            allocated = []
            for ordinal in range(1, 12):
                allocated.append(
                    await _candidate(
                        store,
                        quota_target,
                        f"allocated-{ordinal}",
                        notify_enabled=True,
                    )
                )
            assert all(
                projection.fragments[0].state
                == ReplyFragmentState.ALLOCATED
                for projection in allocated[:10]
            )
            assert (
                allocated[10].fragments[0].state
                == ReplyFragmentState.DEFERRED_QUOTA
            )

            switchback = await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            )
            assert [item.reply_candidate_id for item in switchback] == [
                missed.candidate.reply_candidate_id
            ]
            assert switchback[0].presentation_id == missed.candidate.reply_candidate_id

            # Explicit /inbox is broader than implicit switch-back and may
            # still present a retained foreground-class candidate.
            ordinary_inbox = await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=False,
                present=False,
            )
            assert {item.reply_candidate_id for item in ordinary_inbox} == {
                missed.candidate.reply_candidate_id,
                foreground.candidate.reply_candidate_id,
            }
            assert other_agent.candidate.reply_candidate_id not in {
                item.reply_candidate_id for item in ordinary_inbox
            }
            assert attachment_only.candidate.reply_candidate_id not in {
                item.reply_candidate_id for item in ordinary_inbox
            }
            # Text presentation cannot represent an attachment-only item.
            # Leave it unread for a future media-specific UX instead of
            # consuming it invisibly during an Agent switch.
            with sqlite3.connect(tmp_path / "runtime.sqlite") as connection:
                assert connection.execute(
                    "SELECT presentation FROM reply_candidates "
                    "WHERE reply_candidate_id=?",
                    (attachment_only.candidate.reply_candidate_id,),
                ).fetchone() == ("unseen",)

            # Inbox-only candidates have no wire allocation and cannot be
            # claimed by the delivery worker merely because they were read.
            assert all(
                item.reply_candidate_id != missed.candidate.reply_candidate_id
                for item in await store.list_outbox(limit=100)
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_command_receipt_fragments_are_exact_replayable_and_bounded(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            receipt = await store.begin_command_receipt(
                "switch-fragment-receipt",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="fragment-command",
                command_name="agent",
                command_args=("alpha",),
                command_text="/agent alpha",
            )
            assert receipt["state"] == "started"
            supplied = (
                {"fragment_kind": "text", "text": "switch acknowledgement"},
                {"kind": "text", "content": "one retained Agent item"},
            )
            completed = await store.complete_command_receipt(
                receipt["command_id"],
                response_text=(
                    "switch acknowledgement\n\none retained Agent item"
                ),
                response_agent_id="alpha",
                presentation_ids=("candidate-one",),
                response_fragments=supplied,
            )
            expected = (
                {"kind": "text", "content": "switch acknowledgement"},
                {"kind": "text", "content": "one retained Agent item"},
            )
            assert completed["response_fragments"] == expected
            assert (await store.get_command_receipt(receipt["command_id"]))[
                "response_fragments"
            ] == expected

            replay = await store.complete_command_receipt(
                receipt["command_id"],
                response_text=(
                    "switch acknowledgement\n\none retained Agent item"
                ),
                response_agent_id="alpha",
                presentation_ids=("candidate-one",),
                response_fragments=supplied,
            )
            assert replay == completed
            with pytest.raises(StoreError, match="completion conflicts"):
                await store.complete_command_receipt(
                    receipt["command_id"],
                    response_text=(
                        "switch acknowledgement\n\none retained Agent item"
                    ),
                    response_agent_id="alpha",
                    presentation_ids=("candidate-one",),
                    response_fragments=(
                        {"kind": "text", "content": "mutated fragment"},
                    ),
                )

            oversized = await store.begin_command_receipt(
                "oversized-switch-fragment-receipt",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                external_message_id="oversized-fragment-command",
                command_name="agent",
                command_args=("alpha",),
                command_text="/agent alpha",
            )
            # The tenth and later fragment must reserve room for the durable
            # `/recv` continuation suffix. A 3,000-character tenth fragment
            # is therefore invalid even though that size is valid earlier.
            with pytest.raises(
                ValueError,
                match="deterministic text limit",
            ):
                await store.complete_command_receipt(
                    oversized["command_id"],
                    response_text="oversized",
                    response_fragments=(
                        *(
                            {"kind": "text", "content": str(ordinal)}
                            for ordinal in range(1, 10)
                        ),
                        {"kind": "text", "content": "x" * 3_000},
                    ),
                )
            retained = await store.get_command_receipt(oversized["command_id"])
            assert retained is not None
            assert retained["state"] == "started"
            assert retained["response_fragments"] == ()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_v32_migration_marks_preexisting_reply_candidates_as_history(tmp_path):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first = SQLiteStore(database)
        await first.initialize()
        target, _ = await _accept_scope(first, "pre-v32-origin")
        historical = await _candidate(first, target, "pre-v32-history")
        historical_id = historical.candidate.reply_candidate_id
        await first.close()

        # Re-running migration 32 against an existing candidate simulates a
        # v31 database without having to maintain a second copy of the full
        # historical schema in this focused regression.
        with sqlite3.connect(database) as connection:
            connection.execute(
                "DELETE FROM schema_migrations WHERE version IN (32,33,34,35,36,37,38,39,40,41)"
            )
            connection.commit()

        migrated = SQLiteStore(database)
        await migrated.initialize()
        try:
            assert await migrated.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            ) == []
            with sqlite3.connect(database) as connection:
                presentation = connection.execute(
                    "SELECT presentation, presented_at FROM reply_candidates "
                    "WHERE reply_candidate_id=?",
                    (historical_id,),
                ).fetchone()
            assert presentation is not None
            assert presentation[0] == "presented"
            assert presentation[1]

            # Candidates created after the migration retain the new unseen
            # default and are eligible for normal switch-back selection.
            new_target, _ = await _accept_scope(migrated, "post-v32-origin")
            current = await _candidate(migrated, new_target, "post-v32-unseen")
            unseen = await migrated.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            )
            assert [item.reply_candidate_id for item in unseen] == [
                current.candidate.reply_candidate_id
            ]
        finally:
            await migrated.close()

    asyncio.run(scenario())


def test_command_outbox_marks_candidate_presentation_atomically(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            origin_target, origin_message_id = await _accept_scope(
                store, "background-origin"
            )
            missed = await _candidate(
                store,
                origin_target,
                "background-completion",
            )
            missed_id = missed.candidate.reply_candidate_id

            command_target, command_message_id = await _accept_scope(
                store, "switch-command"
            )
            command = UserDelivery(
                delivery_id="switch-command-outbox",
                target=command_target,
                from_user_id="bot",
                content="switched to Agent: alpha\n\n- background-completion",
                client_id="switch-command-client",
            )

            with pytest.raises(
                StoreError,
                match="unavailable or ambiguous item",
            ):
                await store.create_user_outbox(
                    command,
                    agent_id="alpha",
                    foreground=True,
                    present_outbox_ids=(missed_id, "missing-presentation"),
                )

            # The command candidate/outbox allocation and presentation update
            # share one transaction. Validation failure leaves both absent and
            # the completed Agent item unread.
            assert await store.get_outbox_item(command.delivery_id) is None
            still_unseen = await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            )
            assert [item.reply_candidate_id for item in still_unseen] == [missed_id]

            projected = await store.create_user_outbox(
                command,
                agent_id="alpha",
                foreground=True,
                present_outbox_ids=(missed_id,),
            )
            assert projected.outbox_id == command.delivery_id
            assert projected.reply_slot_id is not None
            assert await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            ) == []

            # Exact replay is idempotent: the original task scope spent no
            # wire quota for the suppressed output, while the fresh /agent
            # inbound spends exactly one slot for its combined response.
            replay = await store.create_user_outbox(
                command,
                agent_id="alpha",
                foreground=True,
                present_outbox_ids=(missed_id,),
            )
            assert replay.outbox_id == projected.outbox_id
            assert replay.reply_slot_id == projected.reply_slot_id
            origin_scope = await store.get_reply_scope_for_inbound(origin_message_id)
            command_scope = await store.get_reply_scope_for_inbound(command_message_id)
            assert origin_scope is not None and origin_scope.used_slots == 0
            assert command_scope is not None and command_scope.used_slots == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_agent_switchback_gateway_replays_receipt_once_and_caps_reply_quota(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        manager = _SwitchManager(store)
        sends = []

        def send(request):
            sends.append(request.model_copy(deep=True))
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        client = SimpleNamespace(bot_id="bot", send_message=send)
        gateway = WeChatGateway(
            store,
            bot_id="bot",
            command_router=MVPCommandRouter(manager),
        )
        try:
            origin_target, _ = await _accept_scope(store, "long-background-origin")
            missed_ids: list[str] = []
            markers: list[str] = []
            for ordinal in range(1, 12):
                marker = f"missed-item-{ordinal:02d}"
                projection = await _candidate(
                    store,
                    origin_target,
                    marker + ":" + (str(ordinal % 10) * 2_900),
                )
                missed_ids.append(projection.candidate.reply_candidate_id)
                markers.append(marker)

            first_message = _command_message("/agent alpha", message_id=501)
            first = await gateway.handle_message(client, first_message)
            assert first is not None and not first.duplicate
            assert first.command_response.startswith("switched to Agent: alpha")
            assert "unseen messages:" in first.command_response
            assert all(marker in first.command_response for marker in markers)
            assert first.presentation_ids == tuple(missed_ids)
            # Completed items keep their durable identities, but compatible
            # text shares bounded wire messages.  The acknowledgement and
            # first item fit together; every later 2,900-character item needs
            # its own message.
            assert len(first.response_fragments) == 11
            assert first.response_fragments[0]["content"].startswith(
                "switched to Agent: alpha"
            )
            for marker in markers:
                matching = [
                    fragment
                    for fragment in first.response_fragments
                    if marker in str(fragment["content"])
                ]
                assert len(matching) == 1
                assert sum(
                    other in str(matching[0]["content"])
                    for other in markers
                ) == 1
            assert manager.switch_calls == 1
            assert manager.inbox_calls == 1

            receipt = await store.get_command_receipt(first.response_delivery_id)
            assert receipt is not None and receipt["state"] == "completed"
            assert tuple(receipt["presentation_ids"]) == tuple(missed_ids)
            assert receipt["response_text"] == first.command_response
            assert receipt["response_fragments"] == first.response_fragments

            # Eleven long items make the combined switch response exceed one
            # inbound's wire allowance. The full response remains on the
            # command receipt, while exactly ten fragments receive SendMsg
            # slots and the remainder stays on the normal /recv FIFO.
            command_scope = await store.get_reply_scope_for_target(
                first.envelope.reply_target()
            )
            assert command_scope is not None and command_scope.used_slots == 10
            command_rows = [
                item
                for item in await store.list_outbox(limit=100)
                if item.reply_target.source_message_id == "501"
            ]
            assert len(command_rows) == 10
            # The synchronous command path sends only the first durable
            # fragment; the delivery worker owns the other allocated rows.
            assert len(sends) == 1
            assert await store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            ) == []

            # Channel redelivery reuses both command receipt and projection;
            # it cannot switch again, mark another batch, or issue another
            # direct SendMsg.
            replay = await gateway.handle_message(client, first_message)
            assert replay is not None and replay.duplicate
            assert replay.command_response == first.command_response
            assert replay.presentation_ids == tuple(missed_ids)
            assert replay.response_fragments == first.response_fragments
            assert manager.switch_calls == 1
            assert manager.inbox_calls == 1
            assert len(sends) == 1

            # A genuine later switch away and back has a fresh command scope,
            # but the candidates are already presented and therefore cannot
            # be replayed as history.
            away = await gateway.handle_message(
                client, _command_message("/agent beta", message_id=502)
            )
            back = await gateway.handle_message(
                client, _command_message("/agent alpha", message_id=503)
            )
            assert away is not None and back is not None
            assert back.command_response == "switched to Agent: alpha"
            assert back.presentation_ids == ()
            assert manager.switch_calls == 3
            assert manager.inbox_calls == 3
        finally:
            await store.close()

    asyncio.run(scenario())


def test_switchback_crash_after_receipt_replays_fragments_without_rerouting(
    tmp_path,
):
    async def scenario() -> None:
        database = tmp_path / "runtime.sqlite"
        first_store = SQLiteStore(database)
        await first_store.initialize()
        first_manager = _SwitchManager(first_store)
        origin_target, _ = await _accept_scope(first_store, "crash-origin")
        missed = await _candidate(
            first_store,
            origin_target,
            "completed while alpha was away",
        )
        missed_id = missed.candidate.reply_candidate_id
        message = _command_message("/agent alpha", message_id=601)
        first_gateway = WeChatGateway(
            first_store,
            bot_id="bot",
            command_router=MVPCommandRouter(first_manager),
        )

        # `accept` commits the route effect and immutable command receipt but
        # intentionally stops before the command outbox/presentation
        # transaction. This is the exact process-loss window in question.
        accepted = await first_gateway.accept(message)
        assert accepted is not None and not accepted.duplicate
        assert accepted.presentation_ids == (missed_id,)
        assert len(accepted.response_fragments) == 1
        assert first_manager.switch_calls == 1
        assert first_manager.inbox_calls == 1
        assert await first_store.get_outbox_item(
            accepted.response_delivery_id
        ) is None
        assert [
            item.reply_candidate_id
            for item in await first_store.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            )
        ] == [missed_id]
        await first_store.close()

        restarted = SQLiteStore(database)
        await restarted.initialize()
        replay_manager = _SwitchManager(restarted)
        sends = []

        def send(request):
            sends.append(request.model_copy(deep=True))
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        replay_gateway = WeChatGateway(
            restarted,
            bot_id="bot",
            command_router=MVPCommandRouter(replay_manager),
        )
        try:
            replay = await replay_gateway.handle_message(
                SimpleNamespace(bot_id="bot", send_message=send),
                message,
            )
            assert replay is not None and replay.duplicate
            assert replay.command_response == accepted.command_response
            assert replay.presentation_ids == accepted.presentation_ids
            assert replay.response_fragments == accepted.response_fragments
            # The completed receipt, not the live router, is the recovery
            # authority. Replaying it must not execute `/agent` a second time.
            assert replay_manager.switch_calls == 0
            assert replay_manager.inbox_calls == 0
            assert len(sends) == 1
            assert await restarted.present_inbox_candidates(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="alpha",
                switch_only=True,
                present=False,
            ) == []
            receipt = await restarted.get_command_receipt(
                replay.response_delivery_id
            )
            assert receipt is not None
            assert receipt["response_fragments"] == accepted.response_fragments
        finally:
            await restarted.close()

    asyncio.run(scenario())
