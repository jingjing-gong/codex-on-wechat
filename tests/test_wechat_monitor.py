"""Durable WeChat monitor ordering regressions."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace

from wechat_ilink.monitor import Monitor, _is_immediate_command_message
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def _message(sequence: int, text: str | None = None) -> WeixinMessage:
    return WeixinMessage(
        seq=sequence,
        message_id=sequence,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(
                type=ITEM_TYPE_TEXT,
                text_item=TextItem(text=text or f"message {sequence}"),
            )
        ],
    )


def test_durable_monitor_stops_contact_group_after_first_failure(monkeypatch):
    stop = threading.Event()
    monkeypatch.setattr("wechat_ilink.monitor.INITIAL_BACKOFF", 0.0)

    class Client:
        bot_id = "bot"

        def __init__(self) -> None:
            self.cursors: list[str] = []

        def get_updates(self, cursor: str):
            self.cursors.append(cursor)
            if len(self.cursors) == 1:
                return SimpleNamespace(
                    ret=0,
                    errcode=0,
                    errmsg="",
                    msgs=[_message(1), _message(2)],
                    get_updates_buf="cursor-after-batch",
                )
            stop.set()
            return SimpleNamespace(
                ret=0,
                errcode=0,
                errmsg="",
                msgs=[],
                get_updates_buf="",
            )

    handled: list[int] = []

    def handler(_client, message: WeixinMessage) -> bool:
        handled.append(int(message.seq or 0))
        return message.seq != 1

    monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
    client = Client()
    monitor = Monitor(
        client,
        handler,
        max_workers=1,
        durable_acceptance=True,
        initial_cursor="",
    )
    try:
        monitor.run(stop)
    finally:
        monitor._executor.shutdown(wait=True)

    assert handled == [1]
    # The failed first acceptance retains the durable cursor for replay.
    assert client.cursors == ["", ""]


def test_only_public_read_and_cancel_commands_bypass_contact_ordering():
    for command in (
        "/status",
        "/tasks 5",
        "/cancel",
        "/cancel task-1",
        "/agents",
        "/models",
        "/modes",
        "/skills",
    ):
        assert _is_immediate_command_message(_message(1, command)), command

    for command in ("/interrupt", "/model gpt-5 high", "/agent planner", "/ask planner work"):
        assert not _is_immediate_command_message(_message(1, command)), command


def test_monitor_does_not_advance_cursor_when_either_server_code_fails(monkeypatch):
    # Protocol failures now back off like transport failures. Keep this cursor
    # regression fast; the dedicated test below verifies the delay sequence.
    monkeypatch.setattr("wechat_ilink.monitor.INITIAL_BACKOFF", 0.0)
    for ret, errcode in ((1, 0), (0, 1)):
        stop = threading.Event()

        class Client:
            bot_id = "bot"

            def __init__(self) -> None:
                self.cursors: list[str] = []

            def get_updates(self, cursor: str):
                self.cursors.append(cursor)
                if len(self.cursors) == 1:
                    return SimpleNamespace(
                        ret=ret,
                        errcode=errcode,
                        errmsg="failed",
                        msgs=[_message(1)],
                        get_updates_buf="must-not-advance",
                    )
                stop.set()
                return SimpleNamespace(
                    ret=0,
                    errcode=0,
                    errmsg="",
                    msgs=[],
                    get_updates_buf="",
                )

        handled: list[int] = []
        monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
        client = Client()
        monitor = Monitor(
            client,
            lambda _client, message: handled.append(int(message.seq or 0)),
            max_workers=1,
            durable_acceptance=True,
            initial_cursor="",
        )
        try:
            monitor.run(stop)
        finally:
            monitor._executor.shutdown(wait=True)

        assert handled == []
        assert client.cursors == ["", ""]


def test_monitor_backs_off_repeated_protocol_errors(monkeypatch):
    class StopAfterTwoWaits:
        def __init__(self) -> None:
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return len(self.waits) >= 2

        def wait(self, timeout: float) -> bool:
            self.waits.append(timeout)
            return False

    class Client:
        bot_id = "bot"

        def __init__(self) -> None:
            self.cursors: list[str] = []

        def get_updates(self, cursor: str):
            self.cursors.append(cursor)
            return SimpleNamespace(
                ret=0,
                errcode=500,
                errmsg="retry later",
                msgs=[_message(1)],
                get_updates_buf="must-not-advance",
            )

    monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
    monkeypatch.setattr("wechat_ilink.monitor.INITIAL_BACKOFF", 1.0)
    stop = StopAfterTwoWaits()
    client = Client()
    handled: list[int] = []
    monitor = Monitor(
        client,
        lambda _client, message: handled.append(int(message.seq or 0)),
        max_workers=1,
        durable_acceptance=True,
        initial_cursor="",
    )
    try:
        monitor.run(stop)
    finally:
        monitor.close()

    assert stop.waits == [1.0, 2.0]
    assert client.cursors == ["", ""]
    assert handled == []


def test_monitor_backs_off_repeated_durable_acceptance_failures(monkeypatch):
    class StopAfterTwoWaits:
        def __init__(self) -> None:
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return len(self.waits) >= 2

        def wait(self, timeout: float) -> bool:
            self.waits.append(timeout)
            return False

    class Client:
        bot_id = "bot"

        def __init__(self) -> None:
            self.cursors: list[str] = []

        def get_updates(self, cursor: str):
            self.cursors.append(cursor)
            return SimpleNamespace(
                ret=0,
                errcode=0,
                errmsg="",
                msgs=[_message(1)],
                get_updates_buf="must-not-advance",
            )

    monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
    monkeypatch.setattr("wechat_ilink.monitor.INITIAL_BACKOFF", 1.0)
    stop = StopAfterTwoWaits()
    client = Client()
    handled: list[int] = []

    def reject(_client, message: WeixinMessage) -> bool:
        handled.append(int(message.seq or 0))
        return False

    monitor = Monitor(
        client,
        reject,
        max_workers=1,
        durable_acceptance=True,
        initial_cursor="",
    )
    try:
        monitor.run(stop)
    finally:
        monitor.close()

    assert stop.waits == [1.0, 2.0]
    assert client.cursors == ["", ""]
    assert handled == [1, 1]


def test_monitor_close_releases_executor_and_is_idempotent(monkeypatch):
    monkeypatch.setattr(Monitor, "_load_buf", lambda self: None)
    monitor = Monitor(SimpleNamespace(bot_id="bot"), lambda *_args: None)
    executor = monitor._executor

    monitor.close()
    monitor.close()

    assert isinstance(executor, ThreadPoolExecutor)
    assert executor._shutdown
