"""Send text replies through WeChat iLink for codex-wechat-bot."""

from __future__ import annotations

import logging
import uuid

from .client import Client, ILinkError
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


class SendMessageError(RuntimeError):
    """Structured iLink rejection used by durable delivery classification."""

    def __init__(
        self,
        ret: int,
        errcode: int,
        errmsg: str,
        *,
        retryable: bool = True,
    ) -> None:
        self.ret = int(ret)
        self.errcode = int(errcode)
        self.errmsg = str(errmsg)
        self.retryable = bool(retryable)
        super().__init__(
            f"send message failed: ret={self.ret} "
            f"errcode={self.errcode} errmsg={self.errmsg}"
        )


class SenderIdentityError(ValueError):
    """The durable sender does not match the selected iLink client."""

    retryable = False


def _is_prepare_failure(ret: int, errcode: int, errmsg: str) -> bool:
    """Match only iLink's known context-token preparation rejection."""

    return (
        int(ret) == -2
        and int(errcode) == 0
        and str(errmsg).strip().casefold() == "prepare failed"
    )


def is_context_prepare_failure(error: BaseException) -> bool:
    """Return whether *error* is the exact context preparation rejection."""

    return _is_prepare_failure(
        int(getattr(error, "ret", 0) or 0),
        int(getattr(error, "errcode", 0) or 0),
        str(getattr(error, "errmsg", "") or ""),
    )


def derive_contextless_client_id(client_id: str) -> str:
    """Derive the stable wire identity for a token-free fallback attempt."""

    primary = str(client_id or "")
    if not primary:
        raise ValueError("primary client_id is required")
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"codex-wechat:contextless-send:{primary}",
        )
    )


def _contextless_client_id(client_id: str) -> str:
    """Compatibility alias for the former private helper."""

    return derive_contextless_client_id(client_id)


def resolve_sender_identity(client: Client, from_user_id: str = "") -> str:
    """Resolve and validate an explicit outbound bot against *client*.

    ``from_user_id`` is optional only for legacy direct callers.  Canonical
    delivery code always supplies the durable value.  Once supplied, an empty
    or different ``client.bot_id`` is a permanent routing error; it is never
    replaced with ambient client state.
    """

    explicit = str(from_user_id or "")
    selected = str(getattr(client, "bot_id", "") or "")
    if not explicit:
        return selected
    if not selected:
        raise SenderIdentityError(
            f"selected WeChat client has no bot_id for sender {explicit!r}"
        )
    if selected != explicit:
        raise SenderIdentityError(
            "selected WeChat client bot_id does not match outbound sender: "
            f"expected {explicit!r}, got {selected!r}"
        )
    return explicit


def _send_message(client: Client, request: SendMessageRequest) -> None:
    """Normalize strict-client exceptions and compatible fake responses."""

    try:
        response = client.send_message(request)
    except ILinkError as exc:
        raise SendMessageError(
            exc.ret,
            exc.errcode,
            exc.errmsg,
        ) from exc
    ret = int(getattr(response, "ret", 0) or 0)
    errcode = int(getattr(response, "errcode", 0) or 0)
    errmsg = str(getattr(response, "errmsg", "") or "")
    if ret != 0 or errcode != 0:
        raise SendMessageError(ret, errcode, errmsg)


def new_client_id() -> str:
    """Generate a new unique client ID for message correlation."""
    return str(uuid.uuid4())


def send_typing_state(client: Client, user_id: str, context_token: str = "") -> None:
    """Send a typing indicator to a user.

    Fetches a typing_ticket via getconfig first, then sends the typing status.
    """
    config_resp = client.get_config(user_id, context_token)
    config_ret = int(getattr(config_resp, "ret", 0) or 0)
    config_errcode = int(getattr(config_resp, "errcode", 0) or 0)
    if config_ret != 0 or config_errcode != 0:
        raise RuntimeError(
            "get config failed: "
            f"ret={config_ret} errcode={config_errcode} "
            f"errmsg={getattr(config_resp, 'errmsg', '')}"
        )
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
    message_state: int = MESSAGE_STATE_FINISH,
    *,
    from_user_id: str = "",
    allow_contextless_fallback: bool = True,
    contextless_client_id: str = "",
) -> None:
    """Send a text reply to a user through the iLink API.

    If ``client_id`` is empty, a new one is generated.  Canonical durable
    callers pass an explicit ``from_user_id`` and disable the in-call fallback
    so the primary-to-contextless transition can be committed before retry.
    Legacy direct callers retain the historical one-call fallback behavior.
    """
    if not client_id:
        client_id = new_client_id()
    sender_id = resolve_sender_identity(client, from_user_id)

    # Convert markdown to plain text for WeChat display.
    plain_text = markdown_to_plain_text(text)

    req = SendMessageRequest(
        msg=SendMsg(
            from_user_id=sender_id,
            to_user_id=to_user_id,
            client_id=client_id,
            message_type=MESSAGE_TYPE_BOT,
            message_state=message_state,
            item_list=[
                MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=plain_text))
            ],
            context_token=context_token,
        ),
        base_info=BaseInfo(),
    )

    try:
        _send_message(client, req)
    except SendMessageError as exc:
        if not _is_prepare_failure(exc.ret, exc.errcode, exc.errmsg):
            raise
        if not context_token:
            # This immutable request already omitted the only recoverable
            # transport hint. Repeating it cannot change iLink preparation.
            raise SendMessageError(
                exc.ret,
                exc.errcode,
                exc.errmsg,
                retryable=False,
            ) from exc
        if not allow_contextless_fallback:
            # A durable caller must first atomically persist the one-way wire
            # variant change.  Returning the exact structured rejection lets
            # that caller request the transition without sending an alternate
            # identity prematurely.
            raise

        # A context token is a rolling reply hint, not destination identity.
        # iLink historically returns ret=-2 when it cannot prepare that hint;
        # retry once without it. The alternate client ID is derived from the
        # durable original so a lost acknowledgement remains idempotent across
        # process restarts.
        fallback = req.model_copy(deep=True)
        fallback.msg.context_token = ""
        fallback.msg.client_id = (
            str(contextless_client_id)
            if contextless_client_id
            else derive_contextless_client_id(client_id)
        )
        if fallback.msg.client_id == client_id:
            raise ValueError("contextless client_id must differ from primary client_id")
        try:
            _send_message(client, fallback)
        except SendMessageError as fallback_exc:
            if _is_prepare_failure(
                fallback_exc.ret,
                fallback_exc.errcode,
                fallback_exc.errmsg,
            ):
                raise SendMessageError(
                    fallback_exc.ret,
                    fallback_exc.errcode,
                    fallback_exc.errmsg,
                    retryable=False,
                ) from fallback_exc
            raise

    logger.info("sent reply to %s: %r", to_user_id, text[:50])
