"""HTTP client for WeChat iLink used by codex-wechat-bot."""

from __future__ import annotations

import base64
import os
import struct
from typing import Optional

import requests

from .types import (
    BaseInfo,
    Credentials,
    GetConfigRequest,
    GetConfigResponse,
    GetUpdatesRequest,
    GetUpdatesResponse,
    GetUploadURLRequest,
    GetUploadURLResponse,
    ILinkModel,
    SendMessageRequest,
    SendMessageResponse,
    SendTypingRequest,
    SendTypingResponse,
)

DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
LONG_POLL_TIMEOUT = 35  # seconds
SEND_TIMEOUT = 15  # seconds


class ILinkError(Exception):
    """Raised when the iLink API returns a non-zero ret/errcode."""


def generate_wechat_uin() -> str:
    """Generate a random value for the X-WECHAT-UIN header (mirrors the Go client)."""
    n = struct.unpack("<I", os.urandom(4))[0]
    return base64.b64encode(str(n).encode()).decode()


class Client:
    """iLink HTTP API client."""

    def __init__(self, creds: Optional[Credentials] = None, timeout: float = 10.0):
        if creds is not None:
            self.base_url = creds.baseurl or DEFAULT_BASE_URL
            self.bot_token = creds.bot_token
            self.bot_id = creds.ilink_bot_id
        else:
            self.base_url = DEFAULT_BASE_URL
            self.bot_token = ""
            self.bot_id = ""
        self._timeout = timeout
        self._session = requests.Session()
        self.wechat_uin = generate_wechat_uin()

    @classmethod
    def new_unauthenticated(cls) -> "Client":
        """Create a client without credentials, used during the login/QR flow."""
        return cls(creds=None, timeout=40.0)

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "Authorization": f"Bearer {self.bot_token}",
            "X-WECHAT-UIN": self.wechat_uin,
        }

    def _post(self, path: str, body: ILinkModel, timeout: float) -> dict:
        resp = self._session.post(
            self.base_url + path,
            data=body.model_dump_json(exclude_none=True),
            headers=self._headers(),
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_updates(self, buf: str = "") -> GetUpdatesResponse:
        """Long-poll for new messages."""
        req = GetUpdatesRequest(
            get_updates_buf=buf, base_info=BaseInfo(channel_version="1.0.0")
        )
        data = self._post("/ilink/bot/getupdates", req, timeout=LONG_POLL_TIMEOUT + 5)
        return GetUpdatesResponse.model_validate(data)

    def send_message(self, req: SendMessageRequest) -> SendMessageResponse:
        """Send a message through iLink."""
        data = self._post("/ilink/bot/sendmessage", req, timeout=SEND_TIMEOUT)
        return SendMessageResponse.model_validate(data)

    def get_config(self, user_id: str, context_token: str = "") -> GetConfigResponse:
        """Fetch bot config for a user (includes typing_ticket)."""
        req = GetConfigRequest(
            ilink_user_id=user_id, context_token=context_token or None
        )
        data = self._post("/ilink/bot/getconfig", req, timeout=10)
        return GetConfigResponse.model_validate(data)

    def send_typing(self, user_id: str, typing_ticket: str, status: int) -> None:
        """Send a typing indicator to a user."""
        req = SendTypingRequest(
            ilink_user_id=user_id, typing_ticket=typing_ticket, status=status
        )
        data = self._post("/ilink/bot/sendtyping", req, timeout=10)
        resp = SendTypingResponse.model_validate(data)
        if resp.ret != 0:
            raise ILinkError(f"sendtyping failed: ret={resp.ret} errmsg={resp.errmsg}")

    def get_upload_url(self, req: GetUploadURLRequest) -> GetUploadURLResponse:
        """Get a pre-signed CDN upload URL for media files."""
        data = self._post("/ilink/bot/getuploadurl", req, timeout=SEND_TIMEOUT)
        return GetUploadURLResponse.model_validate(data)
