"""Regression coverage for /cancel releasing Agent mailbox admission."""

from __future__ import annotations

import asyncio

from src.agents.base import AgentResult
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter
from src.runtime.manager import TaskManager
from src.runtime.models import InvocationState, MailboxState
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import MAILBOX_OPERATOR_CANCEL_REASON
from src.runtime.worker import AgentMailboxSupervisor, AgentMailboxWorker


def _envelope(text: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id="cancel-message",
        text=text,
        session_id="default",
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
    )


def _queued_task(task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "agent_id": "codex",
        "conversation_id": "wechat:bot:user:default:codex",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "reply_target": {
            "channel": "wechat",
            "bot_id": "bot",
            "external_user_id": "user",
            "session_id": "default",
        },
        "inputs": {"text": "next user task"},
    }


class _BlockingRuntime:
    agent_id = "codex"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return AgentResult(task_id=task.task_id, content="unreachable")

    async def interrupt(self, _task_id: str) -> bool:
        return False


class _CancelFacade:
    """Model the foreground task that /cancel already interrupted."""

    def __init__(self, manager: TaskManager) -> None:
        self.manager = manager
        self.state = "running"
        self.cancel_calls = 0

    async def get_active_agent(self, **_kwargs) -> str:
        return "codex"

    async def get_task(self, task_id: str):
        if task_id != "foreground-task":
            return None
        return {
            "task_id": task_id,
            "agent_id": "codex",
            "status": self.state,
            "reply_target": {
                "channel": "wechat",
                "bot_id": "bot",
                "external_user_id": "user",
                "session_id": "default",
            },
        }

    async def cancel(self, task_id: str, **_kwargs) -> bool:
        assert task_id == "foreground-task"
        self.cancel_calls += 1
        self.state = "cancel_requested"
        return True

    async def cancel_active_agent_mailbox(self, agent_id: str) -> bool:
        return await self.manager.cancel_active_agent_mailbox(agent_id)


def test_cancel_releases_running_mailbox_and_unblocks_next_task(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _BlockingRuntime()
        registry = AgentRegistry()
        registry.register(
            "codex", runtime, profile=codex_profile(default_mode_id="chat")
        )
        manager = TaskManager(store, registry, worker_count=0)
        replies: list[object] = []

        async def reply_handler(_item, result, **_kwargs) -> None:
            replies.append(result)

        supervisor = AgentMailboxSupervisor(
            store,
            registry,
            poll_interval=0.01,
            lease_seconds=60,
            reply_handler=reply_handler,
        )
        manager.set_mailbox_cancel_handler(supervisor.request_cancel)
        await manager.start()
        supervisor_task = asyncio.create_task(supervisor.run())
        try:
            mailbox = await store.create_agent_message(
                source_agent_id="planner",
                destination_agent_id="codex",
                content="long-running peer ask",
                request_id="starvation-mailbox-request",
                message_id="starvation-mailbox-message",
            )
            await asyncio.wait_for(runtime.started.wait(), timeout=1)

            active_mailbox = await store.get_mailbox_item(mailbox.mailbox_id)
            assert active_mailbox is not None
            assert active_mailbox.state is MailboxState.PROCESSING
            assert active_mailbox.claim_token
            active_invocation = await store.get_agent_invocation(
                str(active_mailbox.current_invocation_id)
            )
            assert active_invocation is not None
            assert active_invocation.state is InvocationState.RUNNING

            queued = await store.create_task(_queued_task("next-user-task"))
            assert queued.attempts == 0
            assert await store.claim_task_by_id(
                queued.task_id, "blocked-task-worker"
            ) is None
            still_queued = await store.get_task(queued.task_id)
            assert still_queued is not None
            assert still_queued.attempts == 0

            facade = _CancelFacade(manager)
            router = MVPCommandRouter(facade)
            response = await router.handle_command(
                parse_command("/cancel foreground-task"),
                _envelope("/cancel foreground-task"),
            )
            assert response == (
                "cancel requested: foreground-task; "
                "agent mailbox turn cancelled"
            )
            await asyncio.wait_for(runtime.cancelled.wait(), timeout=1)

            rejected = await store.get_mailbox_item(mailbox.mailbox_id)
            assert rejected is not None
            assert rejected.state is MailboxState.REJECTED
            assert rejected.last_error == MAILBOX_OPERATOR_CANCEL_REASON
            assert rejected.claimed_by is None
            assert rejected.claim_token is None
            assert rejected.lease_expires_at is None
            invocation = await store.get_agent_invocation(
                str(rejected.current_invocation_id)
            )
            assert invocation is not None
            assert invocation.state is InvocationState.CANCELLED
            assert invocation.last_error == MAILBOX_OPERATOR_CANCEL_REASON
            assert invocation.admission_released_at is not None
            assert invocation.terminal_at is not None
            assert replies == []

            second = await router.handle_command(
                parse_command("/cancel foreground-task"),
                _envelope("/cancel foreground-task"),
            )
            assert second == "cancel requested: foreground-task"
            assert facade.cancel_calls == 2
            assert not await manager.cancel_active_agent_mailbox("codex")

            claim = await store.claim_task_by_id(
                queued.task_id, "unblocked-task-worker"
            )
            assert claim is not None
            assert claim.task.task_id == queued.task_id
        finally:
            supervisor.stop()
            await asyncio.wait_for(supervisor_task, timeout=1)
            await manager.stop()

    asyncio.run(scenario())


def test_worker_cancel_during_processing_transition_never_dispatches():
    class Store:
        def __init__(self) -> None:
            self.mark_started = asyncio.Event()
            self.release_mark = asyncio.Event()
            self.cancel_started = asyncio.Event()
            self.release_cancel = asyncio.Event()

        async def claim_mailbox(self, *_args, **_kwargs):
            return [{"mailbox_id": "mailbox-race", "claim_token": "token"}]

        async def mark_mailbox_processing(
            self, _mailbox_id: str, **_kwargs
        ) -> bool:
            self.mark_started.set()
            await self.release_mark.wait()
            return True

        async def cancel_active_mailbox_invocation(
            self, agent_id: str, **_kwargs
        ) -> bool:
            assert agent_id == "codex"
            self.cancel_started.set()
            await self.release_cancel.wait()
            return True

    async def scenario() -> None:
        store = Store()
        dispatched = asyncio.Event()

        async def handler(_item):
            dispatched.set()
            return "must not run"

        worker = AgentMailboxWorker(
            store, {}, "codex", handler=handler, lease_seconds=60
        )
        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(store.mark_started.wait(), timeout=1)
        cancelling = asyncio.create_task(worker.request_cancel())
        await asyncio.wait_for(store.cancel_started.wait(), timeout=1)

        store.release_mark.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not dispatched.is_set()

        store.release_cancel.set()
        assert await asyncio.wait_for(cancelling, timeout=1)
        assert await asyncio.wait_for(running, timeout=1) == 0
        assert not dispatched.is_set()

    asyncio.run(scenario())
