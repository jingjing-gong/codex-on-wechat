"""HTTP client for WeChat iLink used by codex-wechat-bot."""

from __future__ import annotations

import base64
import math
import os
import struct
import threading

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


class ILinkError(RuntimeError):
    """Raised when the iLink API returns a non-zero ret/errcode."""

    def __init__(
        self,
        operation: str,
        *,
        ret: int = 0,
        errcode: int = 0,
        errmsg: str = "",
    ) -> None:
        self.operation = str(operation)
        self.ret = int(ret)
        self.errcode = int(errcode)
        self.errmsg = str(errmsg)
        super().__init__(
            f"{self.operation} failed: ret={self.ret} "
            f"errcode={self.errcode} errmsg={self.errmsg}"
        )


def generate_wechat_uin() -> str:
    """Generate a random value for the X-WECHAT-UIN header (mirrors the Go client)."""
    n = struct.unpack("<I", os.urandom(4))[0]
    return base64.b64encode(str(n).encode()).decode()


class Client:
    """iLink HTTP API client."""

    def __init__(self, creds: Credentials | None = None, timeout: float | None = None):
        if creds is not None:
            self.base_url = creds.baseurl or DEFAULT_BASE_URL
            self.bot_token = creds.bot_token
            self.bot_id = creds.ilink_bot_id
        else:
            self.base_url = DEFAULT_BASE_URL
            self.bot_token = ""
            self.bot_id = ""
        parsed_timeout = float(timeout) if timeout is not None else None
        if parsed_timeout is not None and (
            not math.isfinite(parsed_timeout) or parsed_timeout <= 0
        ):
            raise ValueError("timeout must be a finite positive number")
        self._timeout = parsed_timeout
        # ``requests.Session`` is not safe for overlapping use from multiple
        # threads.  The monitor long-polls while handler threads send replies,
        # so give every in-flight request exclusive ownership of one pooled
        # session.  Retain the first session under the historical private
        # attribute for compatibility with simple test adapters.
        self._session_factory = requests.Session
        self._session = self._session_factory()
        self._sessions = [self._session]
        self._available_sessions = [self._session]
        self._close_condition = threading.Condition()
        self._active_requests = 0
        self._closed = False
        self._closing = False
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

    @staticmethod
    def _ensure_success(operation: str, response: ILinkModel) -> None:
        ret = int(getattr(response, "ret", 0) or 0)
        errcode = int(getattr(response, "errcode", 0) or 0)
        if ret != 0 or errcode != 0:
            errmsg = str(getattr(response, "errmsg", "") or "")
            raise ILinkError(
                operation,
                ret=ret,
                errcode=errcode,
                errmsg=errmsg,
            )

    def close(self) -> None:
        """Drain active requests and close all owned HTTP sessions once."""

        with self._close_condition:
            if self._closed:
                while self._closing:
                    self._close_condition.wait()
                return
            self._closed = True
            self._closing = True
            self._close_condition.notify_all()
            while self._active_requests:
                self._close_condition.wait()
            sessions = tuple(self._sessions)
            self._sessions.clear()
            self._available_sessions.clear()

        first_error: Exception | None = None
        seen: set[int] = set()
        try:
            for session in sessions:
                # A test factory may deliberately return the same adapter more
                # than once.  Preserve idempotent close semantics in that case.
                identity = id(session)
                if identity in seen:
                    continue
                seen.add(identity)
                try:
                    session.close()
                except Exception as exc:  # close every remaining owned session
                    if first_error is None:
                        first_error = exc
        finally:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()

        if first_error is not None:
            raise first_error

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def _acquire_session(self):
        """Admit one request and return a session owned by that request."""

        with self._close_condition:
            if self._closed:
                raise RuntimeError("iLink client is closed")
            if self._available_sessions:
                session = self._available_sessions.pop()
            else:
                session = self._session_factory()
                self._sessions.append(session)
            self._active_requests += 1
            return session

    def _release_session(self, session) -> None:
        with self._close_condition:
            self._available_sessions.append(session)
            self._active_requests -= 1
            if self._active_requests == 0:
                self._close_condition.notify_all()

    def _post(self, path: str, body: ILinkModel, timeout: float) -> dict:
        session = self._acquire_session()
        try:
            resp = session.post(
                self.base_url + path,
                data=body.model_dump_json(exclude_none=True),
                headers=self._headers(),
                timeout=timeout,
            )
            try:
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise ValueError("iLink API returned a non-object JSON response")
                return data
            finally:
                resp.close()
        finally:
            self._release_session(session)

    def get_updates(self, buf: str = "") -> GetUpdatesResponse:
        """Long-poll for new messages."""
        req = GetUpdatesRequest(
            get_updates_buf=buf, base_info=BaseInfo(channel_version="1.0.0")
        )
        timeout = (
            max(LONG_POLL_TIMEOUT + 5, self._timeout)
            if self._timeout is not None
            else LONG_POLL_TIMEOUT + 5
        )
        data = self._post("/ilink/bot/getupdates", req, timeout=timeout)
        return GetUpdatesResponse.model_validate(data)

    def send_message(self, req: SendMessageRequest) -> SendMessageResponse:
        """Send a message through iLink."""
        data = self._post(
            "/ilink/bot/sendmessage",
            req,
            timeout=self._timeout if self._timeout is not None else SEND_TIMEOUT,
        )
        response = SendMessageResponse.model_validate(data)
        self._ensure_success("sendmessage", response)
        return response

    def get_config(self, user_id: str, context_token: str = "") -> GetConfigResponse:
        """Fetch bot config for a user (includes typing_ticket)."""
        req = GetConfigRequest(
            ilink_user_id=user_id, context_token=context_token or None
        )
        data = self._post(
            "/ilink/bot/getconfig",
            req,
            timeout=self._timeout if self._timeout is not None else 10,
        )
        response = GetConfigResponse.model_validate(data)
        self._ensure_success("getconfig", response)
        return response

    def send_typing(self, user_id: str, typing_ticket: str, status: int) -> None:
        """Send a typing indicator to a user."""
        req = SendTypingRequest(
            ilink_user_id=user_id, typing_ticket=typing_ticket, status=status
        )
        data = self._post(
            "/ilink/bot/sendtyping",
            req,
            timeout=self._timeout if self._timeout is not None else 10,
        )
        resp = SendTypingResponse.model_validate(data)
        self._ensure_success("sendtyping", resp)

    def get_upload_url(self, req: GetUploadURLRequest) -> GetUploadURLResponse:
        """Get a pre-signed CDN upload URL for media files."""
        data = self._post(
            "/ilink/bot/getuploadurl",
            req,
            timeout=self._timeout if self._timeout is not None else SEND_TIMEOUT,
        )
        response = GetUploadURLResponse.model_validate(data)
        self._ensure_success("getuploadurl", response)
        return response
