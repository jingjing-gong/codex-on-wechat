"""Focused regressions for durable inbound reply scopes and quota overflow."""

from __future__ import annotations

import asyncio
import sqlite3
import threading

import pytest

from src.runtime.models import (
    DeliveryMode,
    InboundMessage,
    ReplyFragmentState,
    ReplyTarget,
)
from src.runtime.media import AttachmentStore
from src.runtime.sqlite_store import SQLiteStore, StoreError


_WECHAT_TEXT_LIMIT = 3_000
_RECV_SUFFIX = "\n\nReply /recv to continue."


def _inbound(
    source_message_id: str,
    *,
    user: str = "user",
    session: str = "default",
    context_token: str = "context-token",
) -> InboundMessage:
    return InboundMessage(
        channel="wechat",
        bot_id="bot",
        external_user_id=user,
        external_message_id=source_message_id,
        session_id=session,
        text="test input",
        context_token=context_token,
    )


async def _accept(
    store: SQLiteStore,
    source_message_id: str,
    *,
    user: str = "user",
    session: str = "default",
    context_token: str = "context-token",
):
    return await store.accept_inbound(
        _inbound(
            source_message_id,
            user=user,
            session=session,
            context_token=context_token,
        ),
        create_task=False,
    )


def test_v19_reply_scope_is_created_replayed_and_backfilled(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        try:
            accepted = await _accept(store, "scope-source", context_token="first")
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            assert scope.inbound_message_id == accepted.inbound.message_id
            assert scope.capacity == 10
            assert scope.used_slots == 0
            assert scope.channel == "wechat"
            assert scope.bot_id == "bot"
            assert scope.external_user_id == "user"
            assert scope.session_id == "default"

            # A rolling transport token is not reply-scope identity.  The
            # duplicate envelope must reuse both its inbound and scope rows.
            replay = await _accept(store, "scope-source", context_token="second")
            replay_scope = await store.get_reply_scope_for_inbound(
                replay.inbound.message_id
            )
            assert replay.duplicate
            assert replay.inbound.message_id == accepted.inbound.message_id
            assert replay_scope is not None
            assert replay_scope.reply_scope_id == scope.reply_scope_id
        finally:
            await store.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone() == (19,)
            # Model interruption after the v19 DDL landed but before its
            # scope backfill and migration marker committed.  Initialization
            # must be idempotent and recreate the missing legacy scope.
            connection.execute(
                "DELETE FROM reply_scopes WHERE inbound_message_id=?",
                (accepted.inbound.message_id,),
            )
            connection.execute("DELETE FROM schema_migrations WHERE version=19")
            connection.commit()

        recovered = SQLiteStore(path)
        await recovered.initialize()
        try:
            backfilled = await recovered.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert backfilled is not None
            assert backfilled.reply_scope_id == scope.reply_scope_id
            assert backfilled.capacity == 10
            assert backfilled.used_slots == 0
        finally:
            await recovered.close()

    asyncio.run(scenario())


def test_candidate_replay_reserves_exactly_ten_canonical_slots(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        try:
            accepted = await _accept(store, "quota-source")
            target = accepted.inbound.target()
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None

            projections = []
            for ordinal in range(1, 12):
                result = await store.project_reply_candidate(
                    target=target,
                    source_key=f"completed-item:{ordinal}",
                    content=f"reply {ordinal}",
                    source_item_id=f"item-{ordinal}",
                    source_item_type="agentMessage",
                    source_item_ordinal=ordinal,
                )
                projections.append(result)
                assert result.candidate is not None
                assert result.candidate.origin_reply_scope_id == scope.reply_scope_id
                assert result.candidate.source_item_ordinal == ordinal
                assert len(result.fragments) == 1

                fragment = result.fragments[0]
                assert fragment.reply_candidate_id == result.candidate.reply_candidate_id
                assert fragment.fragment_ordinal == 1
                assert fragment.origin_reply_scope_id == scope.reply_scope_id
                if ordinal <= 10:
                    assert fragment.state == ReplyFragmentState.ALLOCATED
                    assert fragment.deferred_sequence is None
                    assert len(result.slots) == 1
                    assert len(result.outbox_items) == 1
                    slot = result.slots[0]
                    outbox = result.outbox_items[0]
                    assert slot.reply_ordinal == ordinal
                    assert slot.reply_scope_id == scope.reply_scope_id
                    assert slot.reply_fragment_id == fragment.reply_fragment_id
                    assert slot.outbox_id == outbox.outbox_id
                    assert outbox.reply_scope_id == scope.reply_scope_id
                    assert outbox.reply_slot_id == slot.reply_slot_id
                    assert outbox.reply_ordinal == ordinal
                    assert outbox.reply_candidate_id == result.candidate.reply_candidate_id
                    assert outbox.reply_fragment_id == fragment.reply_fragment_id
                else:
                    assert fragment.state == ReplyFragmentState.DEFERRED_QUOTA
                    assert fragment.reply_slot_id is None
                    assert fragment.delivery_reply_scope_id is None
                    assert fragment.deferred_sequence is not None
                    assert result.slots == ()
                    assert result.outbox_items == ()

            allocated = projections[:10]
            assert [result.slots[0].reply_ordinal for result in allocated] == list(
                range(1, 11)
            )
            assert len(
                {result.slots[0].reply_slot_id for result in allocated}
            ) == 10
            assert len(
                {result.outbox_items[0].outbox_id for result in allocated}
            ) == 10

            # Duplicate SDK observation reuses every identity and cannot
            # reserve a second canonical send for the same source item.
            replay = await store.project_reply_candidate(
                target=target,
                source_key="completed-item:1",
                content="reply 1",
                source_item_id="item-1",
                source_item_type="agentMessage",
                source_item_ordinal=1,
            )
            assert replay.replayed
            assert replay.candidate == projections[0].candidate
            assert replay.fragments == projections[0].fragments
            assert replay.slots == projections[0].slots
            assert replay.outbox_items == projections[0].outbox_items
            with pytest.raises(StoreError):
                await store.project_reply_candidate(
                    target=target,
                    source_key="completed-item:1",
                    content="mutated replay",
                    source_item_id="item-1",
                    source_item_type="agentMessage",
                    source_item_ordinal=1,
                )

            refreshed = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert refreshed is not None
            assert refreshed.used_slots == 10
            assert refreshed.remaining_slots == 0

            # Deferred fragments have no ordinary pending outbox row, while
            # each reserved slot has exactly one canonical send.  Claims are
            # intentionally exposed one ordinal at a time so two workers
            # cannot put later fragments on the wire before their predecessor.
            claimed = []
            for expected_ordinal in range(1, 11):
                batch = await store.claim_outbox("reply-worker", limit=20)
                assert len(batch) == 1
                item = batch[0]
                assert item.reply_ordinal == expected_ordinal
                assert item.claim_token
                claimed.append(item)
                assert await store.mark_outbox_sending(
                    item.outbox_id, item.claim_token
                )
                assert await store.mark_outbox_sent(
                    item.outbox_id,
                    item.claim_token,
                    client_id=item.client_id,
                )
            assert len({item.reply_slot_id for item in claimed}) == 10
        finally:
            await store.close()

        with sqlite3.connect(path) as connection:
            assert connection.execute(
                "SELECT MIN(reply_ordinal), MAX(reply_ordinal), COUNT(*) "
                "FROM reply_slots"
            ).fetchone() == (1, 10, 10)
            assert connection.execute(
                "SELECT COUNT(*) FROM ("
                "SELECT reply_slot_id FROM user_outbox "
                "WHERE reply_slot_id IS NOT NULL "
                "GROUP BY reply_slot_id HAVING COUNT(*) <> 1)"
            ).fetchone() == (0,)

    asyncio.run(scenario())


def test_concurrent_projectors_share_one_atomic_ten_slot_allowance(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SQLiteStore(path)
        second = SQLiteStore(path)
        await first.initialize()
        await second.initialize()
        try:
            accepted = await _accept(first, "concurrent-quota")
            target = accepted.inbound.target()

            async def project(index: int):
                store = first if index % 2 else second
                return await store.project_reply_candidate(
                    target=target,
                    source_key=f"concurrent-item:{index}",
                    content=f"concurrent reply {index}",
                    source_item_id=f"concurrent-source-{index}",
                    source_item_ordinal=index,
                )

            projections = await asyncio.gather(
                *(project(index) for index in range(1, 15))
            )
            allocated = [result for result in projections if result.slots]
            deferred = [result for result in projections if not result.slots]
            assert len(allocated) == 10
            assert len(deferred) == 4
            assert sorted(result.slots[0].reply_ordinal for result in allocated) == list(
                range(1, 11)
            )
            assert len({result.slots[0].reply_slot_id for result in allocated}) == 10
            assert all(
                result.fragments[0].state == ReplyFragmentState.DEFERRED_QUOTA
                and result.fragments[0].reply_slot_id is None
                and result.outbox_items == ()
                for result in deferred
            )
            scope = await second.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            assert scope.used_slots == 10
        finally:
            await second.close()
            await first.close()

    asyncio.run(scenario())


def test_inbox_only_candidate_has_no_send_or_claimable_outbox(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(store, "inbox-only-source")
            projection = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="inbox-only:item",
                content="retained for explicit presentation",
                delivery_mode=DeliveryMode.INBOX_ONLY,
            )
            assert projection.candidate is not None
            assert len(projection.fragments) == 1
            assert projection.fragments[0].state == ReplyFragmentState.INBOX_ONLY
            assert projection.fragments[0].reply_slot_id is None
            assert projection.slots == ()
            assert projection.outbox_items == ()
            assert await store.claim_outbox("reply-worker", limit=10) == []

            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            assert scope.used_slots == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_recv_replay_preserves_recipient_fifo_and_origin_identity(tmp_path):
    async def fill(
        store: SQLiteStore,
        *,
        target: ReplyTarget,
        prefix: str,
        count: int,
    ) -> None:
        for ordinal in range(1, count + 1):
            await store.project_reply_candidate(
                target=target,
                source_key=f"{prefix}:item:{ordinal}",
                content=f"{prefix}-{ordinal}",
                source_item_id=f"{prefix}-item-{ordinal}",
                source_item_ordinal=ordinal,
            )

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted_a = await _accept(store, "origin-a", user="user-a")
            accepted_b = await _accept(store, "origin-b", user="user-b")
            scope_a = await store.get_reply_scope_for_inbound(
                accepted_a.inbound.message_id
            )
            scope_b = await store.get_reply_scope_for_inbound(
                accepted_b.inbound.message_id
            )
            assert scope_a is not None and scope_b is not None
            await fill(
                store,
                target=accepted_a.inbound.target(),
                prefix="a",
                count=14,
            )
            await fill(
                store,
                target=accepted_b.inbound.target(),
                prefix="b",
                count=12,
            )

            recv_a = await _accept(store, "recv-a-1", user="user-a")
            recv_scope_a = await store.get_reply_scope_for_inbound(
                recv_a.inbound.message_id
            )
            assert recv_scope_a is not None
            first = await store.drain_deferred_replies(
                target=recv_a.inbound.target(),
                source_key="recv:a:batch:1",
                limit=2,
            )
            assert not first.replayed
            assert first.batch_id
            assert [item.content for item in first.outbox_items] == ["a-11", "a-12"]
            assert [slot.reply_ordinal for slot in first.slots] == [1, 2]
            assert all(
                fragment.origin_reply_scope_id == scope_a.reply_scope_id
                for fragment in first.fragments
            )
            assert all(
                fragment.delivery_reply_scope_id == recv_scope_a.reply_scope_id
                for fragment in first.fragments
            )

            # Redelivery of the same /recv inbound is an exact batch replay;
            # it must not consume a-13/a-14 from the durable queue.
            replay = await store.drain_deferred_replies(
                target=recv_a.inbound.target(),
                source_key="recv:a:batch:1",
                limit=2,
            )
            assert replay.replayed
            assert replay.batch_id == first.batch_id
            assert [item.outbox_id for item in replay.outbox_items] == [
                item.outbox_id for item in first.outbox_items
            ]
            assert [fragment.reply_fragment_id for fragment in replay.fragments] == [
                fragment.reply_fragment_id for fragment in first.fragments
            ]

            recv_a_2 = await _accept(store, "recv-a-2", user="user-a")
            second = await store.drain_deferred_replies(
                target=recv_a_2.inbound.target(),
                source_key="recv:a:batch:2",
            )
            assert [item.content for item in second.outbox_items] == ["a-13", "a-14"]
            assert all(
                fragment.origin_reply_scope_id == scope_a.reply_scope_id
                for fragment in second.fragments
            )

            # The other recipient's queue was never visible to either A
            # batch, and retains its own committed FIFO order.
            recv_b = await _accept(store, "recv-b-1", user="user-b")
            batch_b = await store.drain_deferred_replies(
                target=recv_b.inbound.target(),
                source_key="recv:b:batch:1",
            )
            assert [item.content for item in batch_b.outbox_items] == ["b-11", "b-12"]
            assert all(
                fragment.origin_reply_scope_id == scope_b.reply_scope_id
                for fragment in batch_b.fragments
            )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_recv_rejects_another_message_target_for_explicit_scope(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            origin = await _accept(store, "origin", context_token="origin-ctx")
            for ordinal in range(1, 12):
                await store.project_reply_candidate(
                    target=origin.inbound.target(),
                    source_key=f"origin:item:{ordinal}",
                    content=f"reply-{ordinal}",
                    source_item_id=f"item-{ordinal}",
                    source_item_ordinal=ordinal,
                )

            recv_a = await _accept(store, "recv-a", context_token="recv-a-ctx")
            recv_b = await _accept(store, "recv-b", context_token="recv-b-ctx")
            scope_a = await store.get_reply_scope_for_inbound(
                recv_a.inbound.message_id
            )
            assert scope_a is not None

            with pytest.raises(
                StoreError,
                match="reply drain target conflicts with its stored inbound scope",
            ):
                await store.drain_deferred_replies(
                    target=recv_b.inbound.target(),
                    source_key="recv:mismatched-target",
                    reply_scope_id=scope_a.reply_scope_id,
                )

            unchanged = await store.get_reply_scope(scope_a.reply_scope_id)
            assert unchanged is not None and unchanged.used_slots == 0
            valid = await store.drain_deferred_replies(
                target=recv_a.inbound.target(),
                source_key="recv:valid-target",
                reply_scope_id=scope_a.reply_scope_id,
            )
            assert [item.content for item in valid.outbox_items] == ["reply-11"]
            assert valid.outbox_items[0].reply_target.source_message_id == "recv-a"
            assert valid.outbox_items[0].reply_target.context_token == "recv-a-ctx"
        finally:
            await store.close()

    asyncio.run(scenario())


def test_create_user_outbox_auto_allocates_but_keeps_legacy_unscoped_calls(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(store, "scoped-command")
            scope = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None

            scoped = await store.create_user_outbox(
                target=accepted.inbound.target(),
                content="command response",
                outbox_id="command:scoped",
                foreground=True,
            )
            assert scoped.reply_scope_id == scope.reply_scope_id
            assert scoped.reply_slot_id
            assert scoped.reply_ordinal == 1
            assert scoped.reply_candidate_id
            assert scoped.reply_fragment_id

            replay = await store.create_user_outbox(
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                    source_message_id="scoped-command",
                    context_token="refreshed-token",
                ),
                content="command response",
                outbox_id="command:scoped",
                foreground=True,
            )
            assert replay.outbox_id == scoped.outbox_id
            assert replay.reply_slot_id == scoped.reply_slot_id
            assert replay.reply_ordinal == scoped.reply_ordinal
            assert replay.client_id == scoped.client_id

            # Existing integrations may enqueue a notification without any
            # matching inbound.  It remains a valid compatibility row and
            # must not manufacture a reply scope or consume another scope.
            legacy = await store.create_user_outbox(
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="legacy-user",
                    session_id="default",
                    source_message_id="not-a-stored-inbound",
                ),
                content="legacy background notification",
                outbox_id="legacy:unscoped",
            )
            assert legacy.reply_scope_id is None
            assert legacy.reply_slot_id is None
            assert legacy.reply_ordinal is None
            assert legacy.reply_candidate_id is None
            assert legacy.reply_fragment_id is None

            refreshed = await store.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert refreshed is not None
            assert refreshed.used_slots == 1
        finally:
            await store.close()

    asyncio.run(scenario())


def test_contextless_wire_variant_is_claim_fenced_and_one_way(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(store, "contextless-source")
            projection = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="contextless:item",
                content="reply",
            )
            original = projection.outbox_items[0]
            slot_id = original.reply_slot_id
            primary_id = original.client_id
            alternate_id = original.contextless_client_id
            assert alternate_id
            assert alternate_id != primary_id

            claimed = await store.claim_outbox("sender", limit=1)
            assert len(claimed) == 1
            claim = claimed[0]
            assert claim.outbox_id == original.outbox_id
            assert claim.claim_token

            assert not await store.activate_outbox_contextless_variant(
                claim.outbox_id,
                "wrong-claim-token",
                contextless_client_id=alternate_id,
            )
            assert await store.activate_outbox_contextless_variant(
                claim.outbox_id,
                claim.claim_token,
                contextless_client_id=alternate_id,
            )
            switched = await store.get_outbox_item(claim.outbox_id)
            assert switched is not None
            assert switched.active_wire_variant == "contextless"
            assert switched.contextless_client_id == alternate_id
            assert switched.wire_client_id == alternate_id
            assert switched.client_id == primary_id
            assert switched.reply_slot_id == slot_id
            assert switched.reply_ordinal == 1
            assert switched.claim_token == claim.claim_token

            # Repeating the exact persisted transition is safe.  A different
            # alternate ID cannot fork the same logical slot into a new wire
            # send and must leave the active variant unchanged.
            assert await store.activate_outbox_contextless_variant(
                claim.outbox_id,
                claim.claim_token,
                contextless_client_id=alternate_id,
            )
            with pytest.raises(StoreError):
                await store.activate_outbox_contextless_variant(
                    claim.outbox_id,
                    claim.claim_token,
                    contextless_client_id="different-contextless-client",
                )
            unchanged = await store.get_outbox_item(claim.outbox_id)
            assert unchanged is not None
            assert unchanged.active_wire_variant == "contextless"
            assert unchanged.contextless_client_id == alternate_id
            assert unchanged.reply_slot_id == slot_id
            assert unchanged.client_id == primary_id
        finally:
            await store.close()

    asyncio.run(scenario())


def test_long_item_chunking_reserves_suffix_capacity_without_losing_tail(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(store, "long-item-source")
            # Nine full fragments, one suffix-aware ordinal-ten fragment, and
            # a non-empty deferred tail.  A repeating digit pattern makes an
            # off-by-one split observable without relying on whitespace that
            # candidate normalization is allowed to trim.
            content = "".join(str(index % 10) for index in range(30_137))
            projection = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="completed-item:long",
                content=content,
                source_item_id="long-item",
                source_item_type="agentMessage",
                source_item_ordinal=1,
            )

            assert projection.candidate is not None
            assert projection.candidate.source_item_id == "long-item"
            assert len(projection.fragments) == 11
            assert {fragment.reply_candidate_id for fragment in projection.fragments} == {
                projection.candidate.reply_candidate_id
            }
            assert [fragment.fragment_ordinal for fragment in projection.fragments] == list(
                range(1, 12)
            )
            assert "".join(fragment.content for fragment in projection.fragments) == content

            assert [slot.reply_ordinal for slot in projection.slots] == list(
                range(1, 11)
            )
            assert len(projection.outbox_items) == 10
            tenth = projection.outbox_items[-1]
            assert tenth.reply_ordinal == 10
            assert len(tenth.content) == _WECHAT_TEXT_LIMIT
            assert tenth.content.endswith(_RECV_SUFFIX)
            assert projection.fragments[9].content == tenth.content.removesuffix(
                _RECV_SUFFIX
            )
            assert len(projection.fragments[9].content) == (
                _WECHAT_TEXT_LIMIT - len(_RECV_SUFFIX)
            )
            assert projection.fragments[10].state == ReplyFragmentState.DEFERRED_QUOTA
            assert projection.fragments[10].reply_slot_id is None

            continuation = await _accept(store, "long-item-recv")
            drained = await store.drain_deferred_replies(
                target=continuation.inbound.target(),
                source_key="recv:long-item",
            )
            assert len(drained.outbox_items) == 1
            delivered = (
                "".join(item.content for item in projection.outbox_items[:9])
                + tenth.content.removesuffix(_RECV_SUFFIX)
                + drained.outbox_items[0].content
            )
            assert delivered == content
        finally:
            await store.close()

    asyncio.run(scenario())


def test_text_and_media_keep_one_candidate_boundary_and_one_slot_per_send(tmp_path):
    async def scenario() -> None:
        attachment_root = tmp_path / "attachments"
        files = AttachmentStore(attachment_root)
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", attachment_root=attachment_root
        )
        await store.initialize()
        try:
            first = files.put_bytes(b"first image", attachment_id="first-image")
            second = files.put_bytes(b"second image", attachment_id="second-image")
            await store.register_attachment(first, kind="image")
            await store.register_attachment(second, kind="image")
            accepted = await _accept(store, "text-media-source")

            projection = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="completed-item:text-media",
                content="界" * 3_001,
                attachments=(first.attachment_id, second.attachment_id),
                source_item_id="text-media-item",
                source_item_type="agentMessage",
                source_item_ordinal=1,
            )

            assert projection.candidate is not None
            assert projection.candidate.attachments == (
                first.attachment_id,
                second.attachment_id,
            )
            # Chunking and channel-required media sends stay ordered inside
            # one source candidate; they are not reprojected as unrelated
            # reply candidates merely because each SendMsg needs a slot.
            assert [fragment.fragment_kind for fragment in projection.fragments] == [
                "text",
                "text",
                "media",
                "media",
            ]
            assert [
                len(fragment.content) for fragment in projection.fragments[:2]
            ] == [3_000, 1]
            assert [fragment.fragment_ordinal for fragment in projection.fragments] == [
                1,
                2,
                3,
                4,
            ]
            assert {fragment.reply_candidate_id for fragment in projection.fragments} == {
                projection.candidate.reply_candidate_id
            }
            assert [slot.reply_ordinal for slot in projection.slots] == [1, 2, 3, 4]
            assert len({slot.reply_slot_id for slot in projection.slots}) == 4

            media_outboxes = [
                item for item in projection.outbox_items if item.attachments
            ]
            assert [item.reply_ordinal for item in media_outboxes] == [3, 4]
            assert [item.attachments for item in media_outboxes] == [
                (first.attachment_id,),
                (second.attachment_id,),
            ]
            media_rows = await store.list_outgoing_media(limit=10)
            assert len(media_rows) == 2
            assert {item.outbox_id for item in media_rows} == {
                item.outbox_id for item in media_outboxes
            }
            assert all(
                slot.delivery_id == outbox.outbox_id
                for slot, outbox in zip(projection.slots, projection.outbox_items)
            )

            media_only = await _accept(store, "media-only-source")
            canonical_media = await store.project_reply_candidate(
                target=media_only.inbound.target(),
                source_key="completed-item:media-only",
                attachments=(first.attachment_id,),
                source_item_id="media-only-item",
                source_item_type="agentMessage",
                source_item_ordinal=1,
            )
            assert len(canonical_media.outbox_items) == 1
            assert canonical_media.outbox_items[0].attachments == (
                first.attachment_id,
            )

            # Upload metadata is subordinate to the slot's canonical outbox
            # claim.  A media worker must not independently claim it and issue
            # a second SendMsg identity before the canonical record is owned.
            assert await store.claim_outgoing_media(
                "media-worker", limit=10, channel="wechat", bot_id="bot"
            ) == []
            scoped_claim = await store.claim_scoped_outgoing_media(
                "canonical-media-worker",
                limit=10,
                channel="wechat",
                bot_id="bot",
            )
            assert len(scoped_claim) == 1
            claimed_media = scoped_claim[0]
            claimed_parent = await store.get_outbox_item(
                canonical_media.outbox_items[0].outbox_id
            )
            assert claimed_parent is not None
            assert claimed_media.outbox_id == claimed_parent.outbox_id
            assert claimed_media.delivery_id == claimed_parent.outbox_id
            assert claimed_media.reply_slot_id == claimed_parent.reply_slot_id
            assert claimed_media.reply_ordinal == claimed_parent.reply_ordinal
            assert claimed_media.client_id == claimed_parent.client_id
            assert (
                claimed_media.contextless_client_id
                == claimed_parent.contextless_client_id
            )
            assert claimed_media.from_user_id == claimed_parent.from_user_id == "bot"
            assert claimed_media.claim_token == claimed_parent.claim_token
            assert claimed_media.lease_expires_at == claimed_parent.lease_expires_at

            claimed = await store.claim_outbox(
                "canonical-worker", channel="wechat", bot_id="bot", limit=10
            )
            assert canonical_media.outbox_items[0].outbox_id not in {
                item.outbox_id for item in claimed
            }
        finally:
            await store.close()

    asyncio.run(scenario())


def test_media_at_ordinal_ten_stays_media_without_an_eleventh_notice(tmp_path):
    async def scenario() -> None:
        attachment_root = tmp_path / "attachments"
        files = AttachmentStore(attachment_root)
        store = SQLiteStore(
            tmp_path / "runtime.sqlite", attachment_root=attachment_root
        )
        await store.initialize()
        try:
            image = files.put_bytes(b"tenth image", attachment_id="tenth-image")
            await store.register_attachment(image, kind="image")
            accepted = await _accept(store, "tenth-media-source")
            target = accepted.inbound.target()
            for ordinal in range(1, 10):
                await store.project_reply_candidate(
                    target=target,
                    source_key=f"text-before-media:{ordinal}",
                    content=f"reply {ordinal}",
                    source_item_id=f"text-item-{ordinal}",
                    source_item_ordinal=ordinal,
                )

            media = await store.project_reply_candidate(
                target=target,
                source_key="media-at-ten",
                attachments=(image.attachment_id,),
                source_item_id="media-item-10",
                source_item_type="agentMessage",
                source_item_ordinal=10,
            )
            overflow = await store.project_reply_candidate(
                target=target,
                source_key="text-after-media",
                content="reply 11",
                source_item_id="text-item-11",
                source_item_ordinal=11,
            )

            assert len(media.fragments) == len(media.slots) == len(media.outbox_items) == 1
            assert media.fragments[0].fragment_kind == "media"
            assert media.slots[0].reply_ordinal == 10
            assert media.slots[0].payload["kind"] == "media"
            assert media.slots[0].payload["content"] == ""
            assert media.outbox_items[0].content == ""
            assert media.outbox_items[0].attachments == (image.attachment_id,)
            assert overflow.slots == ()
            assert overflow.outbox_items == ()
            assert overflow.fragments[0].state == ReplyFragmentState.DEFERRED_QUOTA

            canonical = await store.list_outbox(
                channel="wechat", bot_id="bot", limit=20
            )
            assert len(canonical) == 10
            assert {item.reply_ordinal for item in canonical} == set(range(1, 11))
            assert sum(bool(item.attachments) for item in canonical) == 1
            assert all(_RECV_SUFFIX not in item.content for item in canonical)
        finally:
            await store.close()

    asyncio.run(scenario())


def _task_for_inbound(accepted, *, task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "agent_id": "codex",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "inbound_message_id": accepted.inbound.message_id,
        "reply_target": accepted.inbound.target(),
        "inputs": {"text": accepted.inbound.text},
    }


def test_create_task_commits_initial_reply_before_concurrent_claim(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        projection_entered = threading.Event()
        projection_release = threading.Event()
        claim_entered_sql = threading.Event()

        class PausingInitialReplyStore(SQLiteStore):
            @classmethod
            def _project_initial_task_reply_tx(cls, conn, **kwargs):
                projection_entered.set()
                if not projection_release.wait(timeout=5):
                    raise AssertionError("initial-reply projection was not released")
                return super()._project_initial_task_reply_tx(conn, **kwargs)

        creator = PausingInitialReplyStore(path)
        dispatcher = SQLiteStore(path)
        await creator.initialize()
        await dispatcher.initialize()
        create_future = None
        claim_future = None
        try:
            accepted = await _accept(creator, "atomic-create", context_token="create-token")
            scope = await creator.get_reply_scope_for_inbound(
                accepted.inbound.message_id
            )
            assert scope is not None
            task_id = "task-with-atomic-ack"
            initial_reply = {
                "target": accepted.inbound.target(),
                "source_key": "create:ack",
                "content": "Task accepted",
                "outbox_id": "create:ack:outbox",
                "client_id": "create-ack-client",
                "agent_id": "command-router",
                "from_user_id": "bot",
            }

            create_future = asyncio.create_task(
                creator.create_task(
                    _task_for_inbound(accepted, task_id=task_id),
                    initial_reply=initial_reply,
                )
            )
            assert await asyncio.wait_for(
                asyncio.to_thread(projection_entered.wait), timeout=3
            )

            await dispatcher._call(
                lambda conn: conn.set_trace_callback(
                    lambda statement: (
                        claim_entered_sql.set()
                        if statement.strip().upper().startswith("BEGIN IMMEDIATE")
                        else None
                    )
                )
            )
            claim_future = asyncio.create_task(
                dispatcher.claim_task_by_id(task_id, "dispatcher")
            )
            assert await asyncio.wait_for(
                asyncio.to_thread(claim_entered_sql.wait), timeout=3
            )
            await asyncio.sleep(0.02)
            assert not claim_future.done()

            projection_release.set()
            created = await create_future
            claim = await claim_future
            assert claim is not None

            acknowledgement = await creator.get_outbox_item("create:ack:outbox")
            assert acknowledgement is not None
            assert acknowledgement.task_id is None
            assert acknowledgement.agent_id == "command-router"
            assert acknowledgement.reply_scope_id == scope.reply_scope_id
            assert acknowledgement.reply_ordinal == 1
            assert created.pending_delivery_reply_scope_id == scope.reply_scope_id
            assert claim.execution.delivery_reply_scope_id == scope.reply_scope_id
            assert claim.task.pending_delivery_reply_scope_id is None
            persisted = await creator.get_task(task_id)
            assert persisted is not None
            assert persisted.pending_delivery_reply_scope_id is None
        finally:
            projection_release.set()
            if create_future is not None and not create_future.done():
                await create_future
            if claim_future is not None and not claim_future.done():
                await claim_future
            await dispatcher.close()
            await creator.close()

    asyncio.run(scenario())


def test_retry_execution_projects_to_fresh_retry_reply_scope(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            original = await _accept(
                store, "original-task-source", context_token="original-token"
            )
            task = await store.create_task(
                _task_for_inbound(original, task_id="retry-scope-task")
            )
            first_claim = await store.claim_task_by_id(task.task_id, "worker-one")
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

            retry_inbound = await _accept(
                store, "retry-command-source", context_token="retry-token"
            )
            retry_scope = await store.get_reply_scope_for_inbound(
                retry_inbound.inbound.message_id
            )
            assert retry_scope is not None
            retried = await store.retry_task(
                task.task_id,
                actor="user",
                initial_reply={
                    "target": retry_inbound.inbound.target(),
                    "source_key": "retry:ack",
                    "content": "Retrying task",
                    "outbox_id": "retry:ack:outbox",
                    "client_id": "retry-ack-client",
                    "from_user_id": "bot",
                },
            )
            assert retried is not None
            assert retried.pending_delivery_reply_scope_id == retry_scope.reply_scope_id
            acknowledgement = await store.get_outbox_item("retry:ack:outbox")
            assert acknowledgement is not None
            assert acknowledgement.reply_scope_id == retry_scope.reply_scope_id
            assert acknowledgement.reply_ordinal == 1
            assert acknowledgement.reply_target.source_message_id == "retry-command-source"
            assert acknowledgement.reply_target.context_token == "retry-token"

            second_claim = await store.claim_task_by_id(task.task_id, "worker-two")
            assert second_claim is not None
            assert (
                second_claim.execution.delivery_reply_scope_id
                == retry_scope.reply_scope_id
            )
            assert second_claim.task.pending_delivery_reply_scope_id is None
            assert await store.mark_task_running(
                task.task_id,
                second_claim.claim_token,
                execution_id=second_claim.execution_id,
            )
            event = await store.append_task_event(
                task.task_id,
                {
                    "event_id": "retry-execution-event",
                    "execution_id": second_claim.execution_id,
                    "content": "Result from the retry execution",
                    "source_item_id": "retry-source-item",
                    "source_item_type": "agentMessage",
                    "source_item_ordinal": 1,
                },
                claim_token=second_claim.claim_token,
            )
            projected = next(
                item
                for item in await store.list_outbox(limit=20)
                if item.event_id == event.event_id
            )
            assert projected.reply_scope_id == retry_scope.reply_scope_id
            assert projected.reply_ordinal == 2
            assert projected.reply_target.source_message_id == "retry-command-source"
            assert projected.reply_target.context_token == "retry-token"

            persisted = await store.get_task(task.task_id)
            assert persisted is not None
            assert persisted.reply_target.source_message_id == "original-task-source"
            assert persisted.reply_target.context_token == "original-token"
            assert second_claim.task.reply_target == persisted.reply_target
        finally:
            await store.close()

    asyncio.run(scenario())


def test_claim_outbox_releases_scoped_ordinals_only_after_predecessor_terminal(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(store, "ordered-claims")
            target = accepted.inbound.target()
            for ordinal in (1, 2):
                await store.project_reply_candidate(
                    target=target,
                    source_key=f"ordered:{ordinal}",
                    content=f"reply {ordinal}",
                )

            # Ordinal two has a pending predecessor, so even a large claim
            # batch can expose only ordinal one from this scope.
            first_batch = await store.claim_outbox("worker-one", limit=20)
            assert len(first_batch) == 1
            first = first_batch[0]
            assert first.reply_ordinal == 1
            assert first.claim_token
            assert await store.claim_outbox("worker-two", limit=20) == []

            legacy = await store.create_user_outbox(
                target=ReplyTarget(
                    channel="wechat",
                    bot_id="bot",
                    external_user_id="user",
                    session_id="default",
                    source_message_id="not-a-stored-inbound",
                ),
                content="legacy notification",
                outbox_id="ordered:legacy",
            )
            assert legacy.reply_scope_id is None
            legacy_claim = await store.claim_outbox("legacy-worker", limit=20)
            assert [item.outbox_id for item in legacy_claim] == [legacy.outbox_id]

            assert await store.mark_outbox_sending(
                first.outbox_id, first.claim_token
            )
            assert await store.claim_outbox("worker-three", limit=20) == []
            assert await store.mark_outbox_sent(first.outbox_id, first.claim_token)

            second_batch = await store.claim_outbox("worker-four", limit=20)
            assert len(second_batch) == 1
            assert second_batch[0].reply_scope_id == first.reply_scope_id
            assert second_batch[0].reply_ordinal == 2
        finally:
            await store.close()

    asyncio.run(scenario())


def test_rolling_token_replay_retains_canonical_send_envelope(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()
        try:
            accepted = await _accept(
                store, "canonical-replay", context_token="first-token"
            )
            first = await store.project_reply_candidate(
                target=accepted.inbound.target(),
                source_key="canonical:item",
                content="canonical payload",
                outbox_id="canonical:outbox",
                client_id="canonical-client",
                contextless_client_id="canonical-contextless-client",
                from_user_id="bot",
            )
            refreshed_target = ReplyTarget(
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                source_message_id="canonical-replay",
                context_token="refreshed-token",
            )
            replay = await store.project_reply_candidate(
                target=refreshed_target,
                source_key="canonical:item",
                content="canonical payload",
                outbox_id="canonical:outbox",
                client_id="canonical-client",
                contextless_client_id="canonical-contextless-client",
                from_user_id="bot",
            )

            assert replay.replayed
            assert replay.slots == first.slots
            assert replay.outbox_items == first.outbox_items
            assert replay.slots[0].payload["content"] == "canonical payload"
            assert replay.slots[0].payload["target"]["context_token"] == "first-token"
            assert replay.slots[0].payload["from_user_id"] == "bot"
            assert replay.outbox_items[0].reply_target.context_token == "first-token"
            assert replay.outbox_items[0].from_user_id == "bot"

            with pytest.raises(StoreError, match="sender identity"):
                await store.project_reply_candidate(
                    target=refreshed_target,
                    source_key="canonical:item",
                    content="canonical payload",
                    outbox_id="canonical:outbox",
                    client_id="canonical-client",
                    contextless_client_id="canonical-contextless-client",
                    from_user_id="wrong-sender",
                )
        finally:
            await store.close()

    asyncio.run(scenario())


def test_tokenless_contextless_activation_requires_active_direct_send(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        await store.initialize()

        def target(source_message_id: str) -> ReplyTarget:
            return ReplyTarget(
                channel="wechat",
                bot_id="bot",
                external_user_id="legacy-user",
                session_id="default",
                source_message_id=source_message_id,
            )

        try:
            direct = await store.create_user_outbox(
                target=target("direct-send-source"),
                content="direct response",
                outbox_id="direct-send-outbox",
            )
            assert not await store.activate_outbox_contextless_variant(
                direct.outbox_id,
                contextless_client_id="direct-contextless-client",
            )
            assert await store.mark_outbox_sending(
                direct.outbox_id, allow_pending=True
            )
            active_direct = await store.get_outbox_item(direct.outbox_id)
            assert active_direct is not None
            assert active_direct.state.value == "sending"
            assert active_direct.claimed_by == "direct-send"
            assert await store.activate_outbox_contextless_variant(
                direct.outbox_id,
                contextless_client_id="direct-contextless-client",
            )

            worker_owned = await store.create_user_outbox(
                target=target("worker-source"),
                content="worker response",
                outbox_id="worker-outbox",
            )
            claimed = await store.claim_outbox("delivery-worker", limit=10)
            assert [item.outbox_id for item in claimed] == [worker_owned.outbox_id]
            worker_claim = claimed[0]
            assert not await store.activate_outbox_contextless_variant(
                worker_claim.outbox_id,
                contextless_client_id="worker-contextless-client",
            )
            assert await store.mark_outbox_sending(
                worker_claim.outbox_id, worker_claim.claim_token
            )
            assert not await store.activate_outbox_contextless_variant(
                worker_claim.outbox_id,
                contextless_client_id="worker-contextless-client",
            )
            assert await store.activate_outbox_contextless_variant(
                worker_claim.outbox_id,
                worker_claim.claim_token,
                contextless_client_id="worker-contextless-client",
            )

            expired = await store.create_user_outbox(
                target=target("expired-direct-source"),
                content="expired direct response",
                outbox_id="expired-direct-outbox",
            )
            assert await store.mark_outbox_sending(
                expired.outbox_id,
                allow_pending=True,
                lease_seconds=1,
                now="2030-01-01T00:00:00+00:00",
            )
            assert not await store.activate_outbox_contextless_variant(
                expired.outbox_id,
                contextless_client_id="expired-contextless-client",
                now="2030-01-01T00:00:02+00:00",
            )
        finally:
            await store.close()

    asyncio.run(scenario())
