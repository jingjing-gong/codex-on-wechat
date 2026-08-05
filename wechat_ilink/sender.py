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


def new_client_id() -> str:
    """Generate a new unique client ID for message correlation."""
    return str(uuid.uuid4())


def send_typing_state(client: Client, user_id: str, context_token: str = "") -> None:
    """Send a typing indicator to a user.

    Fetches a typing_ticket via getconfig first, then sends the typing status.
    """
    config_resp = client.get_config(user_id, context_token)
    if not config_resp.typing_ticket:
        raise RuntimeError("no typing_ticket returned from getconfig")

    client.send_typing(user_id, config_resp.typing_ticket, TYPING_STATUS_TYPING)
    logger.info("sent typing indicator to %s", user_id)


def send_text_reply(
    client: Client,
    to_user_id: str,
    text: str,
    context_token: str = "",
    client_id: str = "",
) -> None:
    """Send a text reply to a user through the iLink API.

    If client_id is empty, a new one is generated.
    """
    if not client_id:
        client_id = new_client_id()

    # Convert markdown to plain text for WeChat display.
    plain_text = markdown_to_plain_text(text)

    req = SendMessageRequest(
        msg=SendMsg(
            from_user_id=client.bot_id,
            to_user_id=to_user_id,
            client_id=client_id,
            message_type=MESSAGE_TYPE_BOT,
            message_state=MESSAGE_STATE_FINISH,
            item_list=[
                MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=plain_text))
            ],
            context_token=context_token,
        ),
        base_info=BaseInfo(),
    )

    resp = client.send_message(req)
    if resp.ret != 0:
        raise RuntimeError(f"send message failed: ret={resp.ret} errmsg={resp.errmsg}")

    logger.info("sent reply to %s: %r", to_user_id, text[:50])
