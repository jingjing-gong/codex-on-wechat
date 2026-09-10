from __future__ import annotations

from pathlib import Path

import pytest

from src.channels.lark import (
    LarkBotProfile,
    LarkProtocolError,
    normalize_lark_event,
)
from src.runtime.models import BotProfileRecord


def _profile(tmp_path: Path) -> LarkBotProfile:
    config = tmp_path / "lark-config"
    config.mkdir(mode=0o700)
    return LarkBotProfile(
        profile_id="work",
        app_id="cli_example1234",
        bot_open_id="ou_bot1234",
        config_dir=config,
    )


def _event(
    *,
    app_id: str = "cli_example1234",
    sender: str = "ou_user1234",
    chat_type: str = "p2p",
    chat_id: str = "oc_chat1234",
    text: str = "hello",
    mentions=(),
    root_id: str = "",
) -> dict:
    return {
        "header": {
            "event_id": "event-1",
            "event_type": "im.message.receive_v1",
            "app_id": app_id,
        },
        # Spoofable identity-shaped fields outside the authenticated actor are
        # intentionally ignored by normalization.
        "principal_id": "attacker-chosen",
        "event": {
            "sender": {"sender_id": {"open_id": sender}},
            "message": {
                "message_id": "om_message1234",
                "chat_id": chat_id,
                "chat_type": chat_type,
                "message_type": "text",
                "content": {"text": text},
                "mentions": list(mentions),
                "root_id": root_id,
                "create_time": "1700000000000",
            },
        },
    }


def test_profile_coercion_accepts_transport_neutral_durable_record(tmp_path):
    config = (tmp_path / "onboarded-config").resolve()
    durable = BotProfileRecord(
        profile_id="onboarded",
        channel="lark",
        bot_id="cli_onboarded1234",
        brand="feishu",
        config_dir=str(config),
        config_dir_identity="path:onboarded",
        cli_version="1.0.92",
        credential_ref="keychain:appsecret:cli_onboarded1234",
        restart_policy={
            "max_attempts": 5,
            "base_delay": 2,
            "max_delay": 20,
            "bot_open_id": "ou_onboarded_bot1234",
        },
    )

    profile = LarkBotProfile.from_value(durable)

    assert profile.profile_id == "onboarded"
    assert profile.app_id == durable.bot_id
    assert profile.config_dir == config
    assert profile.bot_open_id == "ou_onboarded_bot1234"
    assert profile.restart_max_attempts == 5
    assert profile.restart_base_delay == 2
    assert profile.restart_max_delay == 20


def test_direct_message_uses_authenticated_actor_and_exact_destination(tmp_path):
    envelope = normalize_lark_event(_event(), _profile(tmp_path))
    assert envelope is not None
    assert envelope.channel == "lark"
    assert envelope.bot_id == "cli_example1234"
    assert envelope.external_user_id == "ou_user1234"
    assert envelope.conversation_subject_id
    assert envelope.conversation_subject_scope == "ou_user1234"
    assert envelope.destination_kind == "open_id"
    assert envelope.destination_id == "ou_user1234"
    assert envelope.principal_id == ""
    assert "principal_id" not in envelope.raw["channel_metadata"]


def test_group_requires_structured_mention_and_strips_only_entity_key(tmp_path):
    profile = _profile(tmp_path)
    assert normalize_lark_event(
        _event(chat_type="group", text="Bot please help"), profile
    ) is None

    envelope = normalize_lark_event(
        _event(
            chat_type="group",
            text="@_user_1 Bot please help",
            mentions=(
                {
                    "key": "@_user_1",
                    "name": "Bot",
                    "id": {"open_id": "ou_bot1234"},
                },
            ),
        ),
        profile,
    )
    assert envelope is not None
    assert envelope.text == "Bot please help"
    assert envelope.conversation_subject_kind == "group"
    assert envelope.destination_id == "oc_chat1234"
    assert envelope.conversation_subject_id != envelope.external_user_id


def test_group_mention_never_authenticates_an_app_id_shape(tmp_path):
    profile = _profile(tmp_path)
    assert normalize_lark_event(
        _event(
            chat_type="group",
            text="@_user_1 please help",
            mentions=(
                {
                    "key": "@_user_1",
                    # Even a value equal to the configured bot open_id is not
                    # authenticated when it appears in the app_id namespace.
                    "id": {"app_id": "ou_bot1234"},
                },
            ),
        ),
        profile,
    ) is None


def test_topic_threads_have_distinct_subjects_and_exact_thread_target(tmp_path):
    profile = _profile(tmp_path)
    mention = ({"key": "@bot", "id": {"open_id": "ou_bot1234"}},)
    first = normalize_lark_event(
        _event(chat_type="group", text="@bot one", mentions=mention, root_id="omt_1"),
        profile,
    )
    second = normalize_lark_event(
        _event(chat_type="group", text="@bot two", mentions=mention, root_id="omt_2"),
        profile,
    )
    assert first is not None and second is not None
    assert first.thread_id == "omt_1"
    assert first.destination_kind == "thread"
    assert first.conversation_subject_id != second.conversation_subject_id


def test_wrong_profile_and_unstable_sender_are_rejected(tmp_path):
    profile = _profile(tmp_path)
    with pytest.raises(LarkProtocolError, match="different app"):
        normalize_lark_event(_event(app_id="cli_foreign1234"), profile)
    with pytest.raises(LarkProtocolError, match="open_id"):
        normalize_lark_event(_event(sender="user-id-not-open-id"), profile)


def test_stale_generation_is_dropped_before_normalization(tmp_path):
    assert (
        normalize_lark_event(
            _event(),
            _profile(tmp_path),
            generation=2,
            active_generation=3,
        )
        is None
    )


def test_official_flattened_event_shape_keeps_display_name_text(tmp_path):
    payload = {
        "type": "im.message.receive_v1",
        "event_id": "evt-flat-1",
        "message_id": "om_flat1234",
        "create_time": "1700000000000",
        "chat_id": "oc_group1234",
        "chat_type": "group",
        "message_type": "text",
        "sender_id": "ou_flatuser1234",
        "root_id": "om_root1234",
        # lark-cli 1.0.92 has already rendered the mention placeholder to its
        # display name.  The adapter must not remove ordinary display text.
        "content": "Work Bot please inspect this",
        "mentions": [
            {"key": "@_user_1", "id": "ou_bot1234", "name": "Work Bot"}
        ],
    }
    envelope = normalize_lark_event(payload, _profile(tmp_path))
    assert envelope is not None
    assert envelope.text == "Work Bot please inspect this"
    assert envelope.external_user_id == "ou_flatuser1234"
    assert envelope.destination_id == "oc_group1234"
    assert envelope.thread_id == "om_root1234"
    assert envelope.root_message_id == "om_root1234"
    assert envelope.raw["channel_metadata"]["mentions"][0]["open_id"] == (
        "ou_bot1234"
    )


def test_plain_reply_parent_does_not_create_a_topic_subject(tmp_path):
    payload = {
        "type": "im.message.receive_v1",
        "message_id": "om_plainreply1234",
        "chat_id": "oc_group1234",
        "chat_type": "group",
        "message_type": "text",
        "sender_id": "ou_flatuser1234",
        "reply_to": "om_parent1234",
        "content": "Work Bot follow-up",
        "mentions": [
            {"key": "@_user_1", "id": "ou_bot1234", "name": "Work Bot"}
        ],
    }
    envelope = normalize_lark_event(payload, _profile(tmp_path))
    assert envelope is not None
    assert envelope.conversation_subject_kind == "group"
    assert envelope.thread_id == ""
    assert envelope.transport_metadata["reply_to"] == "om_parent1234"


@pytest.mark.parametrize(
    ("message_type", "content", "kind", "remote_id", "filename"),
    [
        ("image", "[Image: img_flat1234]", "image", "img_flat1234", "image"),
        (
            "file",
            '<file key="file_flat1234" name="report.pdf"/>',
            "file",
            "file_flat1234",
            "report.pdf",
        ),
        (
            "audio",
            '<audio key="file_audio1234" duration="1200"/>',
            "audio",
            "file_audio1234",
            "",
        ),
    ],
)
def test_official_flattened_media_markers_are_normalized(
    tmp_path, message_type, content, kind, remote_id, filename
):
    payload = {
        "type": "im.message.receive_v1",
        "message_id": f"om_{message_type}1234",
        "create_time": "1700000000000",
        "chat_id": "oc_direct1234",
        "chat_type": "p2p",
        "message_type": message_type,
        "sender_id": "ou_flatuser1234",
        "content": content,
    }
    envelope = normalize_lark_event(payload, _profile(tmp_path))
    assert envelope is not None
    assert envelope.raw["media"] == [
        {
            "kind": kind,
            "remote_id": remote_id,
            "filename": filename,
            "mime_type": envelope.raw["media"][0]["mime_type"],
        }
    ]
