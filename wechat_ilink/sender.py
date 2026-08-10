"""Send text replies through WeChat iLink for codex-wechat-bot."""

from __future__ import annotations

import logging
import uuid

from .client import Client
from .markdown import markdown_to_plain_text
from .types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_BOT,
    TYPING_STATUS_TYPING,
    BaseInfo,
    MessageItem,
    SendMessageRequest,
    SendMsg,
    TextItem,
)

logger = logging.getLogger("wechat_ilink.sender")

MAX_TEXT_REPLY_LENGTH = 2500


def new_client_id() -> str:
    """Generate a new unique client ID for message correlation."""
    return str(uuid.uuid4())


def _split_text(text: str, max_length: int = MAX_TEXT_REPLY_LENGTH) -> list[str]:
    """Split text into non-empty chunks that fit the iLink text limit."""
    if max_length <= 0:
        raise ValueError("max_length must be positive")
    return [
        text[index : index + max_length] for index in range(0, len(text), max_length)
    ]


def prepare_text_reply(text: str) -> list[str]:
    """Convert a reply to plain text and split it into iLink-sized chunks."""
    chunks = _split_text(markdown_to_plain_text(text))
    return chunks or [""]


def send_typing_state(client: Client, user_id: str, context_token: str = "") -> None:
    """Send a typing indicator to a user.

    Fetches a typing_ticket via getconfig first, then sends the typing status.
    """
    config_resp = client.get_config(user_id, context_token)
    if not config_resp.typing_ticket:
        raise RuntimeError("no typing_ticket returned from getconfig")

    client.send_typing(user_id, config_resp.typing_ticket, TYPING_STATUS_TYPING)
    logger.info("sent typing indicator to %s", user_id)


def send_text_chunk(
    client: Client,
    to_user_id: str,
    text: str,
    context_token: str = "",
    client_id: str = "",
    message_state: int = MESSAGE_STATE_FINISH,
) -> None:
    """Send one already-prepared text chunk through the iLink API.

    If client_id is empty, a new one is generated.
    """
    if len(text) > MAX_TEXT_REPLY_LENGTH:
        raise ValueError("text chunk exceeds MAX_TEXT_REPLY_LENGTH")
    chunk_client_id = client_id or new_client_id()
    req = SendMessageRequest(
        msg=SendMsg(
            from_user_id=client.bot_id,
            to_user_id=to_user_id,
            client_id=chunk_client_id,
            message_type=MESSAGE_TYPE_BOT,
            message_state=message_state,
            item_list=[
                MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
            ],
            context_token=context_token,
        ),
        base_info=BaseInfo(),
    )

    resp = client.send_message(req)
    if resp.ret != 0:
        logger.error(
            "iLink send failed: to=%s ret=%s errmsg=%s chars=%d "
            "has_context=%s client_id=%s",
            to_user_id,
            resp.ret,
            resp.errmsg,
            len(text),
            bool(context_token),
            chunk_client_id,
        )
        raise RuntimeError(f"send message failed: ret={resp.ret} errmsg={resp.errmsg}")

    logger.info("sent reply to %s: chars=%d", to_user_id, len(text))


def send_text_reply(
    client: Client,
    to_user_id: str,
    text: str,
    context_token: str = "",
    client_id: str = "",
    message_state: int = MESSAGE_STATE_FINISH,
) -> None:
    """Convert, split, and send a text reply through the iLink API."""
    chunks = prepare_text_reply(text)
    for index, chunk in enumerate(chunks):
        send_text_chunk(
            client,
            to_user_id,
            chunk,
            context_token=context_token,
            client_id=client_id if len(chunks) == 1 else "",
            message_state=message_state,
        )
        logger.info(
            "sent reply to %s: chars=%d chunk=%d/%d",
            to_user_id,
            len(chunk),
            index + 1,
            len(chunks),
        )
