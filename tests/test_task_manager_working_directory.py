"""Focused TaskManager coverage for per-Agent execution workspaces."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from pathlib import Path

import pytest

from src.agents.base import AgentResult, ReplyTarget
from src.agents.workspace import (
    EXECUTION_WORKSPACE_KEY,
    WorkspaceError,
    build_workspace_snapshot,
)
from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter, WeChatGateway, command_delivery_id
from src.runtime.manager import TaskManager
from src.runtime.policy import AgentProfile
from src.runtime.registry import AgentRegistry
from src.runtime.sqlite_store import SQLiteStore
from src.runtime.store import (
    WORKING_DIRECTORY_RESPONSE_MAX_CHARS,
    format_working_directory_response,
)
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


class _Runtime:
    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, task, _emit) -> AgentResult:
        return AgentResult(task_id=task.task_id, content="done")

    async def interrupt(self, _task_id: str) -> bool:
        return False


def _profile(
    agent_id: str,
    *,
    peer: str | None = None,
    allow_children: bool = False,
) -> AgentProfile:
    request_types = {"ask"} if peer is not None else set()
    return AgentProfile(
        agent_id=agent_id,
        display_name=agent_id,
        capabilities=frozenset({"read", "delegate"}),
        allowed_peers=frozenset({"*"} if peer is not None else ()),
        allowed_request_types=frozenset(request_types),
        max_child_depth=2 if allow_children else 0,
        max_children_per_task=2 if allow_children else 0,
        profile_version=3 if peer is not None else 1,
        default_mode_id="plan" if allow_children else "chat",
    )


def _manager(
    database: Path,
    workspace: Path,
    *,
    allow_children: bool = False,
    allow_mailbox: bool = False,
) -> TaskManager:
    registry = AgentRegistry()
    registry.register(
        "agent-a",
        _Runtime("agent-a"),
        profile=_profile(
            "agent-a",
            peer="agent-b" if allow_mailbox else None,
            allow_children=allow_children,
        ),
    )
    registry.register(
        "agent-b",
        _Runtime("agent-b"),
        profile=_profile(
            "agent-b",
            peer="agent-a" if allow_mailbox else None,
        ),
    )
    return TaskManager(
        SQLiteStore(database),
        registry,
        worker_count=0,
        default_agent_id="agent-a",
        workspace_root=workspace,
    )


def _scope(
    *,
    user: str = "user-1",
    session: str = "session-1",
) -> dict[str, str]:
    return {
        "channel": "wechat",
        "bot_id": "bot",
        "external_user_id": user,
        "session_id": session,
    }


def _target(
    *,
    user: str = "user-1",
    session: str = "session-1",
) -> ReplyTarget:
    return ReplyTarget(**_scope(user=user, session=session))


def _task_workspace_path(task) -> str:
    return str(task.metadata[EXECUTION_WORKSPACE_KEY]["path"])


def _message(text: str, message_id: int) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user-1",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


def test_working_directory_is_agent_session_user_isolated_and_persistent(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    paths = {
        name: workspace / name
        for name in ("agent-a", "agent-b", "other-session", "other-user")
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    database = tmp_path / "runtime.sqlite"

    async def scenario() -> None:
        manager = _manager(database, workspace)
        await manager.start()
        try:
            # /cd without an explicit Agent follows the front-Agent route.
            await manager.set_working_directory("agent-a", **_scope())
            await manager.set_active_agent("agent-b", **_scope())
            await manager.set_working_directory("agent-b", **_scope())

            await manager.set_active_agent("agent-a", **_scope())
            current_a = await manager.get_working_directory(**_scope())
            await manager.set_active_agent("agent-b", **_scope())
            current_b = await manager.get_working_directory(**_scope())
            assert current_a["path"] == str(paths["agent-a"].resolve())
            assert current_b["path"] == str(paths["agent-b"].resolve())

            await manager.set_working_directory(
                "other-session",
                agent_id="agent-a",
                **_scope(session="session-2"),
            )
            await manager.set_working_directory(
                "other-user",
                agent_id="agent-a",
                **_scope(user="user-2"),
            )
            assert (
                await manager.get_working_directory(
                    agent_id="agent-a", **_scope()
                )
            )["path"] == str(paths["agent-a"].resolve())
            assert (
                await manager.get_working_directory(
                    agent_id="agent-a", **_scope(session="session-2")
                )
            )["path"] == str(paths["other-session"].resolve())
            assert (
                await manager.get_working_directory(
                    agent_id="agent-a", **_scope(user="user-2")
                )
            )["path"] == str(paths["other-user"].resolve())
        finally:
            await manager.stop()

        restarted = _manager(database, workspace)
        await restarted.start()
        try:
            for agent_id, name in (
                ("agent-a", "agent-a"),
                ("agent-b", "agent-b"),
            ):
                value = await restarted.get_working_directory(
                    agent_id=agent_id,
                    **_scope(),
                )
                assert value["path"] == str(paths[name].resolve())
            assert (
                await restarted.get_working_directory(
                    agent_id="agent-a", **_scope(session="session-2")
                )
            )["path"] == str(paths["other-session"].resolve())
            assert (
                await restarted.get_working_directory(
                    agent_id="agent-a", **_scope(user="user-2")
                )
            )["path"] == str(paths["other-user"].resolve())
        finally:
            await restarted.stop()

    asyncio.run(scenario())


def test_tasks_freeze_cwd_at_acceptance_and_ignore_caller_workspace_metadata(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    before_path = workspace / "before"
    after_path = workspace / "after"
    forged_path = workspace / "caller-forged"
    for path in (before_path, after_path, forged_path):
        path.mkdir(parents=True, exist_ok=True)

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "before", agent_id="agent-a", **_scope()
            )
            before = await manager.submit(
                "accepted before /cd",
                _target(),
                agent_id="agent-a",
                metadata={
                    EXECUTION_WORKSPACE_KEY: build_workspace_snapshot(
                        workspace, forged_path
                    )
                },
            )

            await manager.set_working_directory(
                "../after", agent_id="agent-a", **_scope()
            )
            after = await manager.submit(
                "accepted after /cd",
                _target(),
                agent_id="agent-a",
            )

            assert _task_workspace_path(before) == str(before_path.resolve())
            assert _task_workspace_path(after) == str(after_path.resolve())
            assert _task_workspace_path(
                await manager.get_task(before.task_id)
            ) == str(before_path.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("invalid_kind", "error_pattern"),
    (
        ("missing", "unavailable"),
        ("file", "not a directory"),
        ("symlink_escape", "outside"),
        ("unenterable", "not enterable"),
    ),
)
def test_manager_rejects_invalid_working_directory_targets(
    tmp_path: Path,
    invalid_kind: str,
    error_pattern: str,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    candidate = workspace / invalid_kind
    restore_permissions = False

    if invalid_kind == "file":
        candidate.write_text("not a directory", encoding="utf-8")
    elif invalid_kind == "symlink_escape":
        outside = tmp_path / "outside"
        outside.mkdir()
        try:
            candidate.symlink_to(outside, target_is_directory=True)
        except (NotImplementedError, OSError):
            pytest.skip("directory symlinks are unavailable")
    elif invalid_kind == "unenterable":
        candidate.mkdir()
        candidate.chmod(0)
        restore_permissions = True
        if os.access(candidate, os.X_OK):
            candidate.chmod(0o700)
            pytest.skip("the current user can enter mode-000 directories")

    async def scenario() -> None:
        manager = _manager(
            tmp_path / f"runtime-{invalid_kind}.sqlite",
            workspace,
        )
        await manager.start()
        try:
            with pytest.raises(WorkspaceError, match=error_pattern):
                await manager.set_working_directory(
                    candidate,
                    agent_id="agent-a",
                    **_scope(),
                )
            assert await manager.store.get_session_working_directory(
                agent_id="agent-a",
                **_scope(),
            ) is None
        finally:
            await manager.stop()

    try:
        asyncio.run(scenario())
    finally:
        if restore_permissions:
            candidate.chmod(0o700)


def test_configured_workspace_root_replacement_fails_closed(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    selected = workspace / "selected"
    selected.mkdir(parents=True)

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "selected", agent_id="agent-a", **_scope()
            )
            accepted = await manager.submit(
                "accepted before root replacement",
                _target(),
                agent_id="agent-a",
            )

            workspace.rename(tmp_path / "workspace-original")
            selected.mkdir(parents=True)

            with pytest.raises(
                WorkspaceError,
                match="configured workspace root identity has changed",
            ):
                await manager.get_working_directory(
                    agent_id="agent-a", **_scope()
                )
            with pytest.raises(
                WorkspaceError,
                match="configured workspace root identity has changed",
            ):
                await manager.submit(
                    "must not use the replacement root",
                    _target(),
                    agent_id="agent-a",
                )
            with pytest.raises(
                WorkspaceError,
                match="configured workspace root identity has changed",
            ):
                await manager.set_working_directory(
                    selected,
                    agent_id="agent-a",
                    **_scope(),
                )

            assert _task_workspace_path(accepted) == str(selected.resolve())
            assert len(await manager.store.list_tasks(limit=10)) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_delayed_relative_cd_resolves_from_its_accepted_snapshot(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    accepted_child = workspace / "accepted" / "child"
    live_child = workspace / "live" / "child"
    accepted_child.mkdir(parents=True)
    live_child.mkdir(parents=True)

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "accepted", agent_id="agent-a", **_scope()
            )
            envelope = InboundEnvelope(
                **_scope(),
                external_message_id="delayed-relative-cd",
                text="/cd child",
                agent_id="agent-a",
                conversation_id="wechat:bot:user-1:session-1:agent-a",
            )
            accepted = await manager.accept_inbound(
                envelope,
                create_task=False,
            )
            command_snapshot = accepted.inbound.payload["__command_snapshot"]

            # Model another already-completed control changing mutable state
            # while this accepted command is waiting for its receipt owner.
            await manager.set_working_directory(
                live_child.parent,
                agent_id="agent-a",
                **_scope(),
            )
            delayed = replace(
                envelope,
                raw={"__command_snapshot": command_snapshot},
            )
            command = parse_command(delayed.text)
            assert command is not None
            response = await MVPCommandRouter(manager).handle_command(
                command,
                delayed,
            )

            assert response == f"working directory: {accepted_child.resolve()}"
            selected = await manager.get_working_directory(
                agent_id="agent-a", **_scope()
            )
            assert selected["path"] == str(accepted_child.resolve())
            assert selected["path"] != str(live_child.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_retry_retains_the_tasks_original_working_directory_snapshot(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    original = workspace / "original"
    future = workspace / "future"
    original.mkdir(parents=True)
    future.mkdir()

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "original", agent_id="agent-a", **_scope()
            )
            task = await manager.submit(
                "retry in the accepted directory",
                _target(),
                agent_id="agent-a",
            )
            original_snapshot = dict(
                task.metadata[EXECUTION_WORKSPACE_KEY]
            )

            first_claim = await manager.store.claim_task_by_id(
                task.task_id,
                "worker-first",
            )
            assert first_claim is not None
            assert await manager.store.mark_task_running(
                task.task_id,
                first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            failed = await manager.store.complete_task(
                task.task_id,
                result={"status": "failed", "error": "retry probe"},
                claim_token=first_claim.claim_token,
                execution_id=first_claim.execution_id,
            )
            assert failed.state.value == "failed"

            await manager.set_working_directory(
                future,
                agent_id="agent-a",
                **_scope(),
            )
            retried = await manager.retry(task.task_id)
            assert retried is not None
            assert retried.metadata[EXECUTION_WORKSPACE_KEY] == original_snapshot
            assert _task_workspace_path(retried) == str(original.resolve())

            second_claim = await manager.store.claim_task_by_id(
                task.task_id,
                "worker-second",
            )
            assert second_claim is not None
            assert (
                second_claim.task.metadata[EXECUTION_WORKSPACE_KEY]
                == original_snapshot
            )
            assert _task_workspace_path(second_claim.task) != str(future.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_gateway_cd_commits_preference_and_receipt_once(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    selected = workspace / "My Project"
    selected.mkdir(parents=True)

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            gateway = WeChatGateway(manager, bot_id="bot")
            message = _message('/cd "My Project"', 71)
            first = await gateway.accept(message)
            assert first is not None and first.accepted and not first.duplicate
            assert first.task_id == ""
            assert first.command_response == (
                f"working directory: {selected.resolve()}"
            )
            receipt = await manager.store.get_command_receipt(
                command_delivery_id(first.envelope)
            )
            assert receipt is not None
            assert receipt["state"] == "completed"
            assert receipt["outcome"]["type"] == "working_directory"
            assert receipt["outcome"]["working_directory"]["relative_path"] == (
                "My Project"
            )

            replay = await gateway.accept(message)
            assert replay is not None and replay.duplicate
            assert replay.command_response == first.command_response
            assert await manager.get_working_directory(
                **_scope(session="default")
            ) == {
                "agent_id": "agent-a",
                "path": str(selected.resolve()),
                EXECUTION_WORKSPACE_KEY: build_workspace_snapshot(
                    workspace,
                    selected,
                ),
            }
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_gateway_cd_acknowledgement_is_safe_bounded_and_exactly_replayed(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    selected = workspace / "line\n\t`tick`"
    for ordinal in range(6):
        selected /= f"{ordinal}-" + ("x" * 85)
    selected.mkdir(parents=True)
    absolute = str(selected.resolve())
    expected = format_working_directory_response(absolute)

    async def scenario() -> None:
        manager = _manager(tmp_path / "safe-response.sqlite", workspace)
        await manager.start()
        try:
            direct = await manager.set_working_directory(
                absolute,
                **_scope(session="default"),
            )
            assert direct["path"] == absolute
            assert direct["command_response"] == expected

            gateway = WeChatGateway(manager, bot_id="bot")
            message = _message(f'/cd "{absolute}"', 72)
            first = await gateway.accept(message)
            assert first is not None and first.accepted and not first.duplicate
            assert first.command_response == expected
            assert len(first.command_response) == (
                WORKING_DIRECTORY_RESPONSE_MAX_CHARS
            )
            assert first.command_response.endswith("...")
            assert "line 'tick'" in first.command_response
            assert "`" not in first.command_response
            assert not any(
                character in first.command_response
                for character in "\n\r\t\v\f"
            )

            receipt = await manager.store.get_command_receipt(
                command_delivery_id(first.envelope)
            )
            assert receipt is not None
            assert receipt["response_text"] == expected
            assert receipt["outcome"]["command_response"] == expected
            assert receipt["outcome"]["absolute_path"] == absolute

            replay = await gateway.accept(message)
            assert replay is not None and replay.duplicate
            assert replay.command_response == expected

            query = await gateway.accept(_message("/cd", 73))
            assert query is not None
            assert query.command_response == expected
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_ask_task_uses_destination_agents_working_directory(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    source_path = workspace / "source"
    destination_path = workspace / "destination"
    source_path.mkdir(parents=True)
    destination_path.mkdir()

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "source", agent_id="agent-a", **_scope()
            )
            await manager.set_working_directory(
                "destination", agent_id="agent-b", **_scope()
            )
            envelope = InboundEnvelope(
                **_scope(),
                external_message_id="ask-workspace-message",
                text="/ask agent-b inspect this tree",
                agent_id="agent-a",
                conversation_id="wechat:bot:user-1:session-1:agent-a",
            )
            await manager.store.accept_inbound(envelope, create_task=False)
            command = parse_command(envelope.text)
            assert command is not None

            response = await MVPCommandRouter(manager).handle_command(
                command,
                envelope,
            )
            assert response is not None
            task_id = str(response).removeprefix("Agent task queued: ")
            task = await manager.get_task(task_id)
            assert task is not None
            assert task.agent_id == "agent-b"
            assert _task_workspace_path(task) == str(destination_path.resolve())
            assert _task_workspace_path(task) != str(source_path.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_same_agent_child_inherits_parent_workspace_snapshot(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    parent_path = workspace / "parent"
    future_path = workspace / "future"
    parent_path.mkdir(parents=True)
    future_path.mkdir()

    async def scenario() -> None:
        manager = _manager(
            tmp_path / "runtime.sqlite",
            workspace,
            allow_children=True,
        )
        await manager.start()
        try:
            await manager.set_working_directory(
                "parent", agent_id="agent-a", **_scope()
            )
            parent = await manager.submit(
                "parent task",
                _target(),
                agent_id="agent-a",
                mode_id="plan",
            )
            await manager.set_working_directory(
                future_path, agent_id="agent-a", **_scope()
            )

            child = await manager.submit_child_task(
                parent.task_id,
                "same-Agent child",
                agent_id="agent-a",
            )
            ordinary = await manager.submit(
                "new ordinary task",
                _target(),
                agent_id="agent-a",
                mode_id="plan",
            )

            assert _task_workspace_path(parent) == str(parent_path.resolve())
            assert _task_workspace_path(child) == str(parent_path.resolve())
            assert _task_workspace_path(ordinary) == str(future_path.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_mailbox_snapshot_uses_destination_agents_working_directory(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    source_path = workspace / "source"
    destination_path = workspace / "destination"
    destination_next = workspace / "destination-next"
    source_path.mkdir(parents=True)
    destination_path.mkdir()
    destination_next.mkdir()

    async def scenario() -> None:
        manager = _manager(
            tmp_path / "runtime.sqlite",
            workspace,
            allow_mailbox=True,
        )
        await manager.start()
        try:
            await manager.set_working_directory(
                "source", agent_id="agent-a", **_scope()
            )
            await manager.set_working_directory(
                "destination", agent_id="agent-b", **_scope()
            )
            await manager.set_mode(
                "chat", agent_id="agent-a", **_scope()
            )

            message = await manager.send_agent_message(
                "agent-b",
                "inspect the destination workspace",
                source_agent_id="agent-a",
                request_id="workspace-mailbox-request",
                **_scope(),
            )
            snapshot = message.execution_snapshot["metadata"][
                EXECUTION_WORKSPACE_KEY
            ]
            assert snapshot["path"] == str(destination_path.resolve())
            assert snapshot["path"] != str(source_path.resolve())

            # A response-loss retry keeps the first committed mailbox cwd even
            # after the destination's mutable selection changes.
            await manager.set_working_directory(
                "../destination-next", agent_id="agent-b", **_scope()
            )
            replay = await manager.send_agent_message(
                "agent-b",
                "inspect the destination workspace",
                source_agent_id="agent-a",
                request_id="workspace-mailbox-request",
                **_scope(),
            )
            assert replay.mailbox_id == message.mailbox_id
            assert replay.execution_snapshot == message.execution_snapshot
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_correlated_mailbox_turn_uses_the_destinations_current_cwd(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    original = workspace / "original"
    current = workspace / "current"
    original.mkdir(parents=True)
    current.mkdir()

    async def scenario() -> None:
        manager = _manager(tmp_path / "runtime.sqlite", workspace)
        await manager.start()
        try:
            await manager.set_working_directory(
                "original", agent_id="agent-a", **_scope()
            )
            source_task = await manager.submit(
                "source task",
                _target(),
                agent_id="agent-a",
            )
            await manager.set_working_directory(
                "../current", agent_id="agent-a", **_scope()
            )

            snapshot = await manager._mailbox_execution_snapshot(
                "agent-a",
                task_id=source_task.task_id,
                request_id="correlated-cwd",
            )
            assert snapshot["metadata"][EXECUTION_WORKSPACE_KEY]["path"] == (
                str(current.resolve())
            )
            assert snapshot["metadata"][EXECUTION_WORKSPACE_KEY]["path"] != (
                str(original.resolve())
            )
        finally:
            await manager.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["delete", "replace"])
def test_deleted_or_replaced_working_directory_fails_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    workspace = tmp_path / "workspace"
    selected = workspace / "selected"
    recovery = workspace / "recovery"
    selected.mkdir(parents=True)
    recovery.mkdir()

    async def scenario() -> None:
        manager = _manager(
            tmp_path / f"runtime-{mutation}.sqlite",
            workspace,
        )
        await manager.start()
        try:
            await manager.set_working_directory(
                "selected", agent_id="agent-a", **_scope()
            )
            if mutation == "delete":
                selected.rmdir()
            else:
                selected.rename(workspace / "selected-original")
                selected.mkdir()

            with pytest.raises(WorkspaceError):
                await manager.get_working_directory(
                    agent_id="agent-a", **_scope()
                )
            with pytest.raises(WorkspaceError):
                await manager.submit(
                    "must not run elsewhere",
                    _target(),
                    agent_id="agent-a",
                )
            assert await manager.store.list_tasks(limit=10) == []

            with pytest.raises(WorkspaceError):
                await manager.accept_inbound(
                    InboundEnvelope(
                        **_scope(),
                        external_message_id=f"stale-candidate-{mutation}",
                        text="pending transcription",
                        agent_id="agent-a",
                        conversation_id=(
                            "wechat:bot:user-1:session-1:agent-a"
                        ),
                    ),
                    create_task=False,
                    transcription_candidates=({"candidate_text": "later"},),
                )

            # A broken task cwd must not poison cwd-independent controls, and
            # an absolute /cd must be able to restore the Agent scope.
            gateway = WeChatGateway(
                manager,
                bot_id="bot",
                session_resolver=lambda _user: "session-1",
            )
            ordinal = 81 if mutation == "delete" else 82
            help_result = await gateway.accept(_message("/help", ordinal))
            assert help_result is not None
            assert "`/cd [path]`" in help_result.command_response

            cd_result = await gateway.accept(
                _message(f'/cd "{recovery}"', ordinal + 10)
            )
            assert cd_result is not None
            assert cd_result.command_response == (
                f"working directory: {recovery.resolve()}"
            )
            restored = await manager.submit(
                "safe after recovery",
                _target(),
                agent_id="agent-a",
            )
            assert _task_workspace_path(restored) == str(recovery.resolve())
        finally:
            await manager.stop()

    asyncio.run(scenario())
