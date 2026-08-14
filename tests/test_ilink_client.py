"""iLink application-response and HTTP-session lifecycle regressions."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from wechat_ilink import cdn
from wechat_ilink.client import Client, ILinkError
from wechat_ilink.sender import (
    SendMessageError,
    SenderIdentityError,
    send_text_reply,
    send_typing_state,
)
from wechat_ilink.types import (
    GetUploadURLRequest,
    GetUploadURLResponse,
    MESSAGE_STATE_FINISH,
    SendMessageRequest,
    SendMsg,
)


@pytest.mark.parametrize("operation", ["send_message", "get_config", "send_typing", "get_upload_url"])
def test_client_rejects_nonzero_errcode_for_every_nonpoll_operation(
    monkeypatch, operation
):
    client = Client()
    monkeypatch.setattr(
        client,
        "_post",
        lambda *_args, **_kwargs: {"ret": 0, "errcode": 42, "errmsg": "denied"},
    )
    try:
        with pytest.raises(ILinkError, match=r"errcode=42.*denied") as caught:
            if operation == "send_message":
                client.send_message(SendMessageRequest(msg=SendMsg()))
            elif operation == "get_config":
                client.get_config("user")
            elif operation == "send_typing":
                client.send_typing("user", "ticket", 1)
            else:
                client.get_upload_url(GetUploadURLRequest())
        assert caught.value.ret == 0
        assert caught.value.errcode == 42
        assert caught.value.errmsg == "denied"
    finally:
        client.close()


def test_sender_rejects_errcode_from_compatible_fake_clients():
    send_client = SimpleNamespace(
        bot_id="bot",
        send_message=lambda _request: SimpleNamespace(
            ret=0, errcode=7, errmsg="send denied"
        ),
    )
    with pytest.raises(RuntimeError, match=r"errcode=7.*send denied"):
        send_text_reply(send_client, "user", "hello")

    typing_calls = 0

    def send_typing(*_args):
        nonlocal typing_calls
        typing_calls += 1

    typing_client = SimpleNamespace(
        get_config=lambda *_args: SimpleNamespace(
            ret=0, errcode=8, errmsg="config denied", typing_ticket="ticket"
        ),
        send_typing=send_typing,
    )
    with pytest.raises(RuntimeError, match=r"errcode=8.*config denied"):
        send_typing_state(typing_client, "user")
    assert typing_calls == 0


def test_text_sender_uses_explicit_bot_and_finish_state():
    requests = []
    client = SimpleNamespace(
        bot_id="bot-a",
        send_message=lambda request: (
            requests.append(request), SimpleNamespace(ret=0, errcode=0)
        )[1],
    )

    send_text_reply(
        client,
        "user",
        "hello",
        context_token="token",
        client_id="primary-id",
        from_user_id="bot-a",
        message_state=MESSAGE_STATE_FINISH,
    )

    assert len(requests) == 1
    assert requests[0].msg.from_user_id == "bot-a"
    assert requests[0].msg.client_id == "primary-id"
    assert requests[0].msg.message_state == MESSAGE_STATE_FINISH


def test_text_sender_rejects_explicit_bot_client_mismatch_without_sending():
    requests = []
    client = SimpleNamespace(
        bot_id="bot-b",
        send_message=lambda request: requests.append(request),
    )

    with pytest.raises(SenderIdentityError, match="does not match"):
        send_text_reply(
            client,
            "user",
            "hello",
            from_user_id="bot-a",
        )

    assert requests == []


def test_sender_recovers_strict_client_prepare_failure_without_context(monkeypatch):
    client = Client()
    requests = []
    responses = iter(
        [
            {"ret": -2, "errcode": 0, "errmsg": "prepare failed"},
            {"ret": 0},
            {"ret": -2, "errcode": 0, "errmsg": "prepare failed"},
            {"ret": 0},
        ]
    )

    def post(_path, request, **_kwargs):
        requests.append(request.model_copy(deep=True))
        return next(responses)

    monkeypatch.setattr(client, "_post", post)
    try:
        for _ in range(2):
            send_text_reply(
                client,
                "user",
                "reply",
                context_token="rolling-token",
                client_id="durable-client-id",
            )
    finally:
        client.close()

    assert [request.msg.context_token for request in requests] == [
        "rolling-token",
        "",
        "rolling-token",
        "",
    ]
    assert requests[0].msg.client_id == requests[2].msg.client_id == "durable-client-id"
    assert requests[1].msg.client_id == requests[3].msg.client_id
    assert requests[1].msg.client_id != "durable-client-id"


def test_contextless_prepare_failure_is_nonretryable():
    client = SimpleNamespace(
        bot_id="bot",
        send_message=lambda _request: SimpleNamespace(
            ret=-2, errcode=0, errmsg="prepare failed"
        ),
    )

    with pytest.raises(SendMessageError) as caught:
        send_text_reply(client, "user", "reply", context_token="")

    assert caught.value.retryable is False


@pytest.mark.parametrize(
    ("ret", "errcode", "errmsg"),
    [
        (-2, 1, "prepare failed"),
        (-2, 0, "prepare failed temporarily"),
        (-1, 0, "prepare failed"),
    ],
)
def test_prepare_fallback_match_is_narrow_and_other_errors_remain_retryable(
    ret, errcode, errmsg
):
    calls = 0

    def reject(_request):
        nonlocal calls
        calls += 1
        return SimpleNamespace(ret=ret, errcode=errcode, errmsg=errmsg)

    client = SimpleNamespace(bot_id="bot", send_message=reject)
    with pytest.raises(SendMessageError) as caught:
        send_text_reply(
            client,
            "user",
            "reply",
            context_token="rolling-token",
            client_id="durable-client-id",
        )

    assert caught.value.retryable is True
    assert calls == 1


def test_cdn_rejects_upload_url_errcode_before_upload(monkeypatch):
    client = SimpleNamespace(
        get_upload_url=lambda _request: GetUploadURLResponse(
            ret=0, errcode=9, errmsg="upload denied", upload_param="query"
        )
    )
    monkeypatch.setattr(
        cdn,
        "_upload_to_cdn",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not upload")),
    )

    with pytest.raises(RuntimeError, match=r"errcode=9.*upload denied"):
        cdn.upload_file_to_cdn(client, b"data", "user", 1)


def test_client_closes_http_responses_and_session_idempotently(monkeypatch):
    class Response:
        closed = False

        def raise_for_status(self):
            return None

        def json(self):
            return {"ret": 0}

        def close(self):
            self.closed = True

    class Session:
        def __init__(self):
            self.response = Response()
            self.close_calls = 0

        def post(self, *_args, **_kwargs):
            return self.response

        def close(self):
            self.close_calls += 1

    session = Session()
    monkeypatch.setattr("wechat_ilink.client.requests.Session", lambda: session)

    with Client() as client:
        client.send_message(SendMessageRequest(msg=SendMsg()))
        assert session.response.closed is True
    client.close()

    assert session.close_calls == 1
    with pytest.raises(RuntimeError, match="client is closed"):
        client.send_message(SendMessageRequest(msg=SendMsg()))


@pytest.mark.parametrize("operation", ["send_message", "get_upload_url"])
def test_client_does_not_share_session_between_poll_and_concurrent_operation(
    monkeypatch, operation
):
    poll_started = threading.Event()
    release_poll = threading.Event()
    sessions = []
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ret": 0, "errcode": 0}

        def close(self):
            return None

    class Session:
        def __init__(self):
            self.close_calls = 0
            self._in_post = False
            self._lock = threading.Lock()

        def post(self, url, **_kwargs):
            with self._lock:
                assert not self._in_post, "one session was used concurrently"
                self._in_post = True
            calls.append((url, self))
            try:
                if url.endswith("/getupdates"):
                    poll_started.set()
                    assert release_poll.wait(2), "test did not release long poll"
                return Response()
            finally:
                with self._lock:
                    self._in_post = False

        def close(self):
            self.close_calls += 1

    def make_session():
        session = Session()
        sessions.append(session)
        return session

    monkeypatch.setattr("wechat_ilink.client.requests.Session", make_session)
    client = Client()
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            poll = executor.submit(client.get_updates)
            assert poll_started.wait(1)
            if operation == "send_message":
                client.send_message(SendMessageRequest(msg=SendMsg()))
            else:
                client.get_upload_url(GetUploadURLRequest())
            release_poll.set()
            poll.result(timeout=2)
    finally:
        release_poll.set()
        client.close()

    assert len(sessions) == 2
    poll_session = next(session for url, session in calls if url.endswith("/getupdates"))
    operation_session = next(
        session for url, session in calls if not url.endswith("/getupdates")
    )
    assert operation_session is not poll_session
    assert [session.close_calls for session in sessions] == [1, 1]


def test_concurrent_close_waits_for_poll_and_send_and_closes_sessions_once(
    monkeypatch,
):
    poll_started = threading.Event()
    send_started = threading.Event()
    release_poll = threading.Event()
    release_send = threading.Event()
    lifecycle = []
    sessions = []

    class Response:
        def __init__(self, operation):
            self.operation = operation

        def raise_for_status(self):
            return None

        def json(self):
            return {"ret": 0, "errcode": 0}

        def close(self):
            lifecycle.append(f"{self.operation}-response-close")

    class Session:
        def __init__(self, name):
            self.name = name
            self.close_calls = 0

        def post(self, url, **_kwargs):
            if url.endswith("/getupdates"):
                operation = "poll"
                poll_started.set()
                assert release_poll.wait(2), "test did not release long poll"
            else:
                assert url.endswith("/sendmessage")
                operation = "send"
                send_started.set()
                assert release_send.wait(2), "test did not release send"
            return Response(operation)

        def close(self):
            self.close_calls += 1
            lifecycle.append(f"{self.name}-close")

    def make_session():
        session = Session(f"session-{len(sessions)}")
        sessions.append(session)
        return session

    monkeypatch.setattr("wechat_ilink.client.requests.Session", make_session)
    client = Client()

    with ThreadPoolExecutor(max_workers=4) as executor:
        poll = executor.submit(client.get_updates)
        assert poll_started.wait(1)
        send = executor.submit(
            client.send_message, SendMessageRequest(msg=SendMsg())
        )
        assert send_started.wait(1)
        first_close = executor.submit(client.close)
        with client._close_condition:
            assert client._close_condition.wait_for(lambda: client._closed, timeout=1)
        second_close = executor.submit(client.close)

        assert not first_close.done()
        assert not second_close.done()
        assert [session.close_calls for session in sessions] == [0, 0]
        with pytest.raises(RuntimeError, match="client is closed"):
            client.send_message(SendMessageRequest(msg=SendMsg()))

        release_send.set()
        send.result(timeout=2)
        assert not first_close.done()
        assert [session.close_calls for session in sessions] == [0, 0]

        release_poll.set()
        poll.result(timeout=2)
        first_close.result(timeout=2)
        second_close.result(timeout=2)

    assert lifecycle == [
        "send-response-close",
        "poll-response-close",
        "session-0-close",
        "session-1-close",
    ]
    assert [session.close_calls for session in sessions] == [1, 1]


def test_explicit_client_timeout_controls_requests_without_shortening_long_poll(
    monkeypatch,
):
    client = Client(timeout=2.5)
    calls: list[tuple[str, float]] = []

    def fake_post(path, _body, timeout):
        calls.append((path, timeout))
        if path.endswith("getupdates"):
            return {"ret": 0, "errcode": 0}
        return {"ret": 0, "errcode": 0}

    monkeypatch.setattr(client, "_post", fake_post)
    try:
        client.send_message(SendMessageRequest(msg=SendMsg()))
        client.get_config("user")
        client.send_typing("user", "ticket", 1)
        client.get_upload_url(GetUploadURLRequest())
        client.get_updates()
    finally:
        client.close()

    assert calls == [
        ("/ilink/bot/sendmessage", 2.5),
        ("/ilink/bot/getconfig", 2.5),
        ("/ilink/bot/sendtyping", 2.5),
        ("/ilink/bot/getuploadurl", 2.5),
        ("/ilink/bot/getupdates", 40),
    ]


def test_client_rejects_nonpositive_timeout():
    with pytest.raises(ValueError, match="finite positive"):
        Client(timeout=0)
    with pytest.raises(ValueError, match="finite positive"):
        Client(timeout=float("nan"))


@pytest.mark.parametrize(
    ("entrypoint", "runner_name"),
    [("_legacy_main", "_run_legacy"), ("_durable_main", "_run_durable")],
)
@pytest.mark.parametrize("runner_fails", [False, True])
def test_bot_runner_closes_client_after_runtime_shutdown(
    monkeypatch, entrypoint, runner_name, runner_fails
):
    from src import codex_wechat_bot as bot

    events: list[str] = []

    class FakeClient:
        def close(self):
            events.append("close")

    monkeypatch.setattr(bot.sys, "argv", ["bot"])
    monkeypatch.setattr(bot, "login", lambda: FakeClient())

    def runner(_client):
        events.append("runner")
        if runner_fails:
            raise RuntimeError("startup failed")

    monkeypatch.setattr(bot, runner_name, runner)

    invoke = getattr(bot, entrypoint)
    if runner_fails:
        with pytest.raises(RuntimeError, match="startup failed"):
            invoke()
    else:
        invoke()

    assert events == ["runner", "close"]


@pytest.mark.parametrize("entrypoint", ["_legacy_main", "_durable_main"])
def test_login_only_closes_authenticated_client(monkeypatch, entrypoint):
    from src import codex_wechat_bot as bot

    closed = 0

    class FakeClient:
        def close(self):
            nonlocal closed
            closed += 1

    monkeypatch.setattr(bot.sys, "argv", ["bot", "--login"])
    monkeypatch.setattr(bot, "login", lambda: FakeClient())

    getattr(bot, entrypoint)()

    assert closed == 1


def test_legacy_flag_is_a_durable_runtime_alias(monkeypatch):
    from src import codex_wechat_bot as bot

    calls: list[str] = []
    monkeypatch.setattr(bot.sys, "argv", ["bot", "--legacy"])
    monkeypatch.setattr(bot, "_durable_main", lambda: calls.append("durable"))
    monkeypatch.setattr(bot, "_legacy_main", lambda: calls.append("legacy"))

    bot.main()

    assert calls == ["durable"]


def test_legacy_startup_failure_stops_agent_and_loop(monkeypatch):
    from src import codex_wechat_bot as bot

    events: list[str] = []

    class Agent:
        async def start(self):
            events.append("agent-start")

        async def stop(self):
            events.append("agent-stop")

        def info(self):
            return {}

    class Loop:
        def start(self):
            events.append("loop-start")

        def run_coro(self, coro, *, timeout):
            import asyncio

            return asyncio.run(coro)

        def stop(self):
            events.append("loop-stop")

    monkeypatch.setattr(bot, "AsyncLoopThread", Loop)
    monkeypatch.setattr(bot, "CodexAgent", lambda **_kwargs: Agent())

    def failed_serve(*_args):
        events.append("serve")
        raise RuntimeError("monitor construction failed")

    monkeypatch.setattr(bot, "_serve_legacy", failed_serve)

    with pytest.raises(RuntimeError, match="monitor construction failed"):
        bot._run_legacy(SimpleNamespace())

    assert events == [
        "loop-start",
        "agent-start",
        "serve",
        "agent-stop",
        "loop-stop",
    ]
