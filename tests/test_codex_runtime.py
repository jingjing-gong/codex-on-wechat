"""SDK-shape regressions for the Codex Agent runtime adapter."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any, Iterable

import pytest

openai_codex = pytest.importorskip("openai_codex")
from openai_codex import (  # noqa: E402
    ApprovalMode,
    LocalImageInput,
    Sandbox,
    SkillInput,
    TextInput,
)
from openai_codex.generated.v2_all import (  # noqa: E402
    AgentMessageThreadItem,
    ImageGenerationThreadItem,
    InputModality,
    Model,
    ModelListResponse,
    ReasoningEffort,
    ReasoningEffortOption,
    ThreadItem,
    Turn,
    TurnError,
    TurnStatus,
)
from openai_codex.models import (  # noqa: E402
    AgentMessageDeltaNotification,
    ItemCompletedNotification,
    Notification,
    TurnCompletedNotification,
)

from src.agents.base import AgentTask  # noqa: E402
from src.agents.codex_runtime import CodexRuntime  # noqa: E402
from src.codex_agent import CodexAgent  # noqa: E402
from src.runtime.manager import TaskManager  # noqa: E402
from src.runtime.registry import AgentRegistry, codex_profile  # noqa: E402
from src.runtime.skills import hash_skill_bundle  # noqa: E402


def _turn_notification(
    status: TurnStatus,
    *,
    error: str | None = None,
    turn_id: str = "turn-1",
) -> Notification:
    return Notification(
        method="turn/completed",
        payload=TurnCompletedNotification(
            threadId="thread-1",
            turn=Turn(
                id=turn_id,
                status=status,
                error=TurnError(message=error) if error else None,
                items=[],
            ),
        ),
    )


def _message_notifications(text: str = "Hello") -> list[Notification]:
    item = ThreadItem(
        root=AgentMessageThreadItem(id="item-1", text=text, type="agentMessage")
    )
    return [
        Notification(
            method="item/agentMessage/delta",
            payload=AgentMessageDeltaNotification(
                delta=text[:2], itemId="item-1", threadId="thread-1", turnId="turn-1"
            ),
        ),
        Notification(
            method="item/agentMessage/delta",
            payload=AgentMessageDeltaNotification(
                delta=text[2:], itemId="item-1", threadId="thread-1", turnId="turn-1"
            ),
        ),
        Notification(
            method="item/completed",
            payload=ItemCompletedNotification(
                completedAtMs=1,
                item=item,
                threadId="thread-1",
                turnId="turn-1",
            ),
        ),
        _turn_notification(TurnStatus.completed),
    ]


class _FakeTurn:
    def __init__(self, notifications: Iterable[Notification]) -> None:
        self.notifications = tuple(notifications)
        self.turn_calls: list[tuple[Any, dict[str, Any]]] = []
        self.interrupt_calls = 0

    async def interrupt(self) -> None:
        self.interrupt_calls += 1

    def stream(self):
        async def produce():
            for notification in self.notifications:
                yield notification

        return produce()


class _FakeThread:
    id = "thread-1"

    def __init__(self, notifications: Iterable[Notification]) -> None:
        self.notifications = tuple(notifications)
        self.turns: list[_FakeTurn] = []
        self.start_kwargs: list[dict[str, Any]] = []

    async def turn(
        self,
        input_value: Any,
        *,
        approval_mode: Any = None,
        cwd: str | None = None,
        effort: Any = None,
        model: str | None = None,
        output_schema: Any = None,
        personality: Any = None,
        sandbox: Any = None,
        service_tier: Any = None,
        summary: Any = None,
    ) -> _FakeTurn:
        kwargs = {
            "approval_mode": approval_mode,
            "cwd": cwd,
            "effort": effort,
            "model": model,
            "sandbox": sandbox,
        }
        # Keep optional arguments visible if a future adapter starts passing
        # them; the current SDK contract should leave them absent/None.
        self.start_kwargs.append(kwargs)
        turn = _FakeTurn(self.notifications)
        turn.turn_calls.append((input_value, kwargs))
        self.turns.append(turn)
        return turn


class _FakeCodex:
    def __init__(self, notifications: Iterable[Notification]) -> None:
        self.thread = _FakeThread(notifications)
        self.thread_start_calls: list[dict[str, Any]] = []
        self.thread_resume_calls: list[tuple[str, dict[str, Any]]] = []

    async def thread_start(self, **kwargs: Any) -> _FakeThread:
        self.thread_start_calls.append(dict(kwargs))
        return self.thread

    async def thread_resume(self, thread_id: str, **kwargs: Any) -> _FakeThread:
        self.thread_resume_calls.append((thread_id, dict(kwargs)))
        return self.thread


def _task(**overrides: Any) -> AgentTask:
    values: dict[str, Any] = {
        "task_id": "task-1",
        "agent_id": "codex",
        "conversation_id": "conversation-1",
        "mode_id": "chat",
        "profile_version": 1,
        "policy_version": 1,
        "model": "gpt-test",
        "reasoning_effort": "high",
        "inputs": {"text": "hello"},
    }
    values.update(overrides)
    return AgentTask(**values)


def test_pydantic_notifications_and_delta_completed_deduplication():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []
        result = await runtime.run(_task(), events.append)

        assert result.status == "completed"
        assert result.content == "Hello"
        assert [event.content for event in events] == ["Hello"]
        assert [event.content for event in result.events] == ["Hello"]
        assert events[0].event_type == "agent_message"
        assert events[0].source_item_id == "item-1"
        assert events[0].source_item_type == "agentmessage"
        assert events[0].source_item_ordinal == 0
        assert fake.thread.turns[0].interrupt_calls == 0

    asyncio.run(scenario())


def test_compatibility_events_preserve_execution_identity():
    async def scenario() -> None:
        fake = _FakeCodex(
            [
                "compatibility output",
                _turn_notification(TurnStatus.completed),
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []
        result = await runtime.run(
            _task(execution_id="execution-1"),
            events.append,
        )

        assert result.status == "completed"
        assert result.execution_id == "execution-1"
        assert [event.execution_id for event in events] == ["execution-1"]
        assert [event.execution_id for event in result.events] == ["execution-1"]

    asyncio.run(scenario())


def test_completed_item_fallback_order_counts_ineligible_sdk_items():
    async def scenario() -> None:
        fake = _FakeCodex(
            [
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "tool-1",
                            "type": "commandExecution",
                            "text": "must stay internal",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "answer-1",
                            "type": "agentMessage",
                            "text": "answer",
                        }
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {"turn": {"status": "completed"}},
                },
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []

        result = await runtime.run(_task(), events.append)

        assert result.content == "answer"
        assert [event.content for event in events] == ["answer"]
        assert events[0].source_item_id == "answer-1"
        assert events[0].source_item_ordinal == 1

    asyncio.run(scenario())


def test_completed_image_generation_is_stable_attachment_only_output(tmp_path):
    async def scenario() -> None:
        saved_path = tmp_path / "generated.png"
        saved_path.write_bytes(b"\x89PNG\r\n\x1a\ngenerated")
        item = ThreadItem(
            root=ImageGenerationThreadItem(
                id="image-item-1",
                result="",
                savedPath=str(saved_path),
                status="completed",
                type="imageGeneration",
            )
        )
        fake = _FakeCodex(
            [
                Notification(
                    method="item/completed",
                    payload=ItemCompletedNotification(
                        completedAtMs=1,
                        item=item,
                        threadId="thread-1",
                        turnId="turn-1",
                    ),
                ),
                _turn_notification(TurnStatus.completed),
            ]
        )
        publications: list[tuple[str, dict[str, Any]]] = []

        async def publish(task, **kwargs):
            publications.append((task.task_id, kwargs))
            return "managed-image-1"

        runtime = CodexRuntime(
            codex=fake,
            cwd="/workspace",
            image_output_publisher=publish,
        )
        events = []
        result = await runtime.run(
            _task(execution_id="execution-1"),
            events.append,
        )

        assert result.status == "completed"
        assert result.content == ""
        assert result.events == tuple(events)
        assert len(events) == 1
        assert events[0].event_type == "image_generation"
        assert events[0].content == ""
        assert events[0].attachments == ("managed-image-1",)
        assert events[0].source_item_id == "image-item-1"
        assert events[0].source_item_type == "imagegeneration"
        assert events[0].source_item_ordinal == 0
        assert publications == [
            (
                "task-1",
                {
                    "source_item_id": "image-item-1",
                    "source_item_ordinal": 0,
                    "saved_path": str(saved_path),
                    "result": "",
                },
            )
        ]

    asyncio.run(scenario())


def test_replayed_image_completion_publishes_only_once():
    async def scenario() -> None:
        notification = {
            "method": "item/completed",
            "payload": {
                "item": {
                    "id": "image-replay-1",
                    "type": "imageGeneration",
                    "status": "completed",
                    "savedPath": "/workspace/generated.png",
                    "result": "",
                }
            },
        }
        fake = _FakeCodex(
            [
                notification,
                notification,
                _turn_notification(TurnStatus.completed),
            ]
        )
        calls = 0

        async def publish(_task, **_kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise AssertionError("replayed image item was republished")
            return "managed-image-replay"

        runtime = CodexRuntime(
            codex=fake,
            cwd="/workspace",
            image_output_publisher=publish,
        )
        result = await runtime.run(_task(execution_id="execution-replay"))

        assert result.status == "completed"
        assert calls == 1
        assert len(result.events) == 1
        assert result.events[0].attachments == ("managed-image-replay",)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("item_type", "item_text"),
    [
        ("commandExecution", "tool output must stay internal"),
        ("reasoning", "reasoning must stay internal"),
        ("plan", "plan status must stay internal"),
        ("status", "status must stay internal"),
        # A generic message-shaped item is not the SDK agentMessage boundary.
        ("message", "generic message must stay internal"),
        ("agentMessage", "   "),
    ],
)
def test_only_nonblank_completed_agent_message_items_are_user_events(
    item_type: str,
    item_text: str,
):
    async def scenario() -> None:
        fake = _FakeCodex(
            [
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "ineligible-1",
                            "type": item_type,
                            "text": item_text,
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "answer-1",
                            "type": "agentMessage",
                            "text": "answer",
                        }
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {"turn": {"status": "completed"}},
                },
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []

        result = await runtime.run(_task(), events.append)

        assert result.status == "completed"
        assert result.content == "answer"
        assert [event.content for event in events] == ["answer"]
        assert [event.content for event in result.events] == ["answer"]
        # Fallback order counts the filtered completed notification too.
        assert events[0].source_item_ordinal == 1

    asyncio.run(scenario())


def test_delta_only_stream_becomes_one_deferred_compatibility_final_event():
    async def scenario() -> None:
        fake = _FakeCodex(
            [
                {
                    "method": "item/agentMessage/delta",
                    "payload": {"delta": "first "},
                },
                {
                    "method": "item/agentMessage/delta",
                    "payload": {"delta": "second"},
                },
                {
                    "method": "turn/completed",
                    "payload": {"turn": {"status": "completed"}},
                },
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []

        result = await runtime.run(_task(), events.append)

        assert result.status == "completed"
        assert result.content == "first second"
        assert [event.content for event in events] == ["first second"]
        assert [event.event_type for event in events] == ["message"]
        assert events[0].source_item_id is None
        assert events[0].source_item_type is None
        assert events[0].source_item_ordinal is None
        assert result.events == tuple(events)

    asyncio.run(scenario())


def test_duplicate_completed_item_id_is_emitted_once_and_keeps_item_order():
    async def scenario() -> None:
        first_item = {
            "method": "item/completed",
            "payload": {
                "item": {
                    "id": "answer-1",
                    "type": "agentMessage",
                    "text": "first",
                }
            },
        }
        fake = _FakeCodex(
            [
                first_item,
                # Notification replay for the same immutable SDK item.
                first_item,
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "answer-2",
                            "type": "agentMessage",
                            "text": "second",
                        }
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {"turn": {"status": "completed"}},
                },
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []

        result = await runtime.run(_task(), events.append)

        assert result.status == "completed"
        assert result.content == "firstsecond"
        assert [event.content for event in events] == ["first", "second"]
        assert [event.source_item_id for event in events] == [
            "answer-1",
            "answer-2",
        ]
        # Every observed item/completed notification receives an ordinal;
        # replaying the first item therefore does not renumber the later item.
        assert [event.source_item_ordinal for event in events] == [0, 2]
        assert result.events == tuple(events)

    asyncio.run(scenario())


def test_conflicting_completed_item_id_fails_and_interrupts_the_turn():
    async def scenario() -> None:
        fake = _FakeCodex(
            [
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "answer-1",
                            "type": "agentMessage",
                            "text": "original",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "id": "answer-1",
                            "type": "agentMessage",
                            "text": "rewritten",
                        }
                    },
                },
            ]
        )
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        events = []

        result = await runtime.run(_task(), events.append)

        assert result.status == "failed"
        assert result.error == "Codex completed item identity conflicts: answer-1"
        assert [event.content for event in events] == ["original"]
        assert fake.thread.turns[0].interrupt_calls == 1

    asyncio.run(scenario())


def test_emit_failure_interrupts_the_still_active_turn():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(codex=fake, cwd="/workspace")

        async def fail_emit(_event) -> None:
            raise RuntimeError("event persistence failed")

        result = await runtime.run(_task(), fail_emit)

        assert result.status == "failed"
        assert result.error == "event persistence failed"
        assert fake.thread.turns[0].interrupt_calls == 1

    asyncio.run(scenario())


def test_interactive_stream_emits_deltas_without_repeating_completed_item():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        chunks = [chunk async for chunk in runtime.chat_stream("conversation-1", "hello")]
        assert chunks == ["He", "llo"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "error", "expected_status", "interrupted"),
    [
        (TurnStatus.failed, "provider failed", "failed", False),
        (TurnStatus.interrupted, None, "interrupted", True),
    ],
)
def test_terminal_failed_and_interrupted_pydantic_notifications(
    status: TurnStatus,
    error: str | None,
    expected_status: str,
    interrupted: bool,
):
    async def scenario() -> None:
        fake = _FakeCodex([_turn_notification(status, error=error)])
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        result = await runtime.run(_task(policy_version=2))
        assert result.status == expected_status
        assert result.interrupted is interrupted
        assert result.error == error

    asyncio.run(scenario())


class _BlockingTurn(_FakeTurn):
    def __init__(self) -> None:
        super().__init__(())
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def stream(self):
        async def produce():
            self.started.set()
            await self.release.wait()
            yield _turn_notification(TurnStatus.interrupted)

        return produce()

    async def interrupt(self) -> None:
        await super().interrupt()
        self.release.set()


class _BlockingThread(_FakeThread):
    def __init__(self) -> None:
        super().__init__(())
        self.blocking_turn = _BlockingTurn()

    async def turn(self, input_value: Any, **kwargs: Any) -> _BlockingTurn:
        self.start_kwargs.append(dict(kwargs))
        self.turns.append(self.blocking_turn)
        return self.blocking_turn


class _BlockingCodex(_FakeCodex):
    def __init__(self) -> None:
        self.thread = _BlockingThread()
        self.thread_start_calls = []
        self.thread_resume_calls = []


def test_timeout_requests_interrupt_and_returns_failed_result():
    async def scenario() -> None:
        fake = _BlockingCodex()
        runtime = CodexRuntime(codex=fake, cwd="/workspace", turn_timeout=0.05)
        result = await runtime.run(_task())
        assert result.status == "failed"
        assert result.error == "Codex turn timed out"
        assert fake.thread.blocking_turn.interrupt_calls == 1

    asyncio.run(scenario())


def test_explicit_interrupt_finishes_as_interrupted():
    async def scenario() -> None:
        fake = _BlockingCodex()
        runtime = CodexRuntime(codex=fake, cwd="/workspace", turn_timeout=1)
        running = asyncio.create_task(runtime.run(_task()))
        await fake.thread.blocking_turn.started.wait()
        assert await runtime.interrupt("task-1") is True
        result = await running
        assert result.status == "interrupted"
        assert result.interrupted is True
        assert fake.thread.blocking_turn.interrupt_calls == 1

    asyncio.run(scenario())


def test_local_image_input_requires_managed_root(tmp_path):
    async def scenario() -> None:
        inside = tmp_path / "inside.png"
        outside = tmp_path.parent / "outside.png"
        inside.write_bytes(b"inside")
        outside.write_bytes(b"outside")
        runtime = CodexRuntime(
            codex=_FakeCodex(()), cwd=str(tmp_path), managed_root=tmp_path
        )

        translated = runtime._translate_input(
            {"text": "inspect", "images": [{"kind": "image", "path": str(inside)}]}
        )
        assert isinstance(translated, list)
        assert isinstance(translated[1], LocalImageInput)
        assert translated[1].path == str(inside.resolve())

        outside_input = runtime._translate_input(
            {"text": "inspect", "images": [{"kind": "image", "path": str(outside)}]}
        )
        assert isinstance(outside_input, list)
        assert not any(isinstance(item, LocalImageInput) for item in outside_input)
        assert "native_input_unavailable" in getattr(outside_input[-1], "text", "")

        link = tmp_path / "link.png"
        try:
            link.symlink_to(outside)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks unavailable")
        linked_input = runtime._translate_input(
            {"images": [{"kind": "image", "path": str(link)}]}
        )
        assert not isinstance(linked_input, LocalImageInput)

    asyncio.run(scenario())


def test_managed_image_input_requires_verified_image_bytes(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "spoofed.png"
        payload = b"not an image"
        path.write_bytes(payload)
        runtime = CodexRuntime(
            codex=_FakeCodex(()), cwd=str(tmp_path), managed_root=tmp_path
        )

        translated = runtime._translate_input(
            {
                "images": [
                    {
                        "kind": "image",
                        "mime_type": "image/png",
                        "path": str(path),
                        "size": len(payload),
                        "checksum": hashlib.sha256(payload).hexdigest(),
                        "available": True,
                        "native_input_available": True,
                    }
                ]
            }
        )
        assert not isinstance(translated, LocalImageInput)
        assert "native_input_unavailable" in getattr(translated, "text", "")

    asyncio.run(scenario())


def test_turn_kwargs_match_openai_codex_01444_surface():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        result = await runtime.run(_task(policy_version=2))
        assert result.status == "completed"

        assert fake.thread_start_calls == [
            {
                "approval_mode": ApprovalMode.deny_all,
                "sandbox": Sandbox.full_access,
                "cwd": "/workspace",
                "model": "gpt-test",
                "developer_instructions": runtime._mode_resolver.require(
                    "chat", 2
                ).developer_instructions,
            }
        ]
        input_value, turn_kwargs = fake.thread.turns[0].turn_calls[0]
        assert getattr(input_value, "text", None) == "hello"
        assert set(turn_kwargs) == {"approval_mode", "sandbox", "cwd", "model", "effort"}
        assert turn_kwargs["approval_mode"] is ApprovalMode.deny_all
        assert turn_kwargs["sandbox"] is Sandbox.full_access
        assert turn_kwargs["cwd"] == "/workspace"
        assert turn_kwargs["model"] == "gpt-test"
        assert turn_kwargs["effort"] == "high"

    asyncio.run(scenario())


def test_collaboration_bridge_context_is_scoped_to_the_current_task():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(
            codex=fake,
            cwd="/workspace",
            agent_bridge_command=(
                "/python path/bin/python",
                "-m",
                "src.agent_cli",
                "--socket",
                "/tmp/agent bridge.sock",
            ),
            agent_bridge_capability_issuer=lambda _task: "turn-capability",
        )
        task = _task(
            task_id="task-current",
            profile_version=3,
            policy_version=3,
            metadata={
                "effective_policy": {
                    "profile_id": "codex",
                    "profile_version": 3,
                    "mode_id": "chat",
                    "mode_policy_version": 3,
                    "can_send_agent_messages": True,
                },
                "mode": {
                    "mode_id": "chat",
                    "policy_version": 3,
                    "sandbox_policy": "full-access",
                    "approval_policy": "deny_all",
                },
            },
        )

        result = await runtime.run(task)

        assert result.status == "completed"
        input_value = fake.thread.turns[0].turn_calls[0][0]
        assert isinstance(input_value, list)
        assert len(input_value) == 2
        context, prompt = input_value
        assert isinstance(context, TextInput)
        assert isinstance(prompt, TextInput)
        assert "task-current" in context.text
        assert "--capability turn-capability" in context.text
        assert "src.agent_cli" in context.text
        assert "'/tmp/agent bridge.sock'" in context.text
        assert prompt.text == "hello"

    asyncio.run(scenario())


def test_internal_mailbox_turn_does_not_advertise_task_bridge():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        issued = 0

        def issue(_task):
            nonlocal issued
            issued += 1
            return "must-not-be-issued"

        runtime = CodexRuntime(
            codex=fake,
            cwd="/workspace",
            agent_bridge_command=("python", "-m", "src.agent_cli"),
            agent_bridge_capability_issuer=issue,
        )
        task = _task(
            execution_id="mailbox-execution",
            profile_version=3,
            policy_version=3,
            metadata={
                "internal_mailbox": True,
                "effective_policy": {
                    "profile_id": "codex",
                    "profile_version": 3,
                    "mode_id": "chat",
                    "mode_policy_version": 3,
                    "can_send_agent_messages": True,
                },
                "mode": {
                    "mode_id": "chat",
                    "policy_version": 3,
                    "sandbox_policy": "full-access",
                    "approval_policy": "deny_all",
                },
            },
        )

        result = await runtime.run(task)

        assert result.status == "completed"
        assert issued == 0
        input_value = fake.thread.turns[0].turn_calls[0][0]
        assert isinstance(input_value, TextInput)
        assert input_value.text == "hello"

    asyncio.run(scenario())


def test_forward_compatible_ultra_effort_is_forwarded_unchanged():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications())
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        task = _task(policy_version=2, reasoning_effort="ultra")

        result = await runtime.run(task)

        assert result.status == "completed"
        turn_kwargs = fake.thread.turns[0].turn_calls[0][1]
        assert type(turn_kwargs["effort"]) is str
        assert turn_kwargs["effort"] == task.reasoning_effort == "ultra"

    asyncio.run(scenario())


def test_list_models_normalizes_typed_descriptor_to_json_aliases():
    descriptor = Model(
        additionalSpeedTiers=["fast"],
        defaultReasoningEffort=ReasoningEffort.high,
        description="Model returned by the typed SDK surface",
        displayName="GPT Test",
        hidden=False,
        id="gpt-test",
        inputModalities=[InputModality.text],
        isDefault=True,
        model="gpt-test",
        supportedReasoningEfforts=[
            ReasoningEffortOption(
                description="Quick answers",
                reasoningEffort=ReasoningEffort.low,
            ),
            ReasoningEffortOption(
                description="Deeper analysis",
                reasoningEffort=ReasoningEffort.high,
            ),
        ],
        supportsPersonality=True,
    )

    class TypedModelCodex:
        def __init__(self) -> None:
            self.include_hidden_calls: list[bool] = []

        async def models(self, *, include_hidden: bool = False):
            self.include_hidden_calls.append(include_hidden)
            return ModelListResponse(data=[descriptor])

    async def scenario() -> None:
        codex = TypedModelCodex()
        runtime = CodexRuntime(codex=codex, cwd="/workspace")

        assert await runtime.list_models(include_hidden=True) == [
            {
                "additionalSpeedTiers": ["fast"],
                "defaultReasoningEffort": "high",
                "description": "Model returned by the typed SDK surface",
                "displayName": "GPT Test",
                "hidden": False,
                "id": "gpt-test",
                "inputModalities": ["text"],
                "isDefault": True,
                "model": "gpt-test",
                "serviceTiers": [],
                "supportedReasoningEfforts": [
                    {
                        "description": "Quick answers",
                        "reasoningEffort": "low",
                    },
                    {
                        "description": "Deeper analysis",
                        "reasoningEffort": "high",
                    },
                ],
                "supportsPersonality": True,
            }
        ]
        assert codex.include_hidden_calls == [True]

    asyncio.run(scenario())


def test_list_models_normalizes_forward_compatible_max_and_ultra_efforts():
    descriptor = Model(
        additionalSpeedTiers=[],
        defaultReasoningEffort=ReasoningEffort("max"),
        description="Model with forward-compatible reasoning efforts",
        displayName="GPT Future",
        hidden=False,
        id="gpt-future",
        inputModalities=[InputModality.text],
        isDefault=True,
        model="gpt-future",
        supportedReasoningEfforts=[
            ReasoningEffortOption(
                description="Maximum reasoning",
                reasoningEffort=ReasoningEffort("max"),
            ),
            ReasoningEffortOption(
                description="Ultra reasoning",
                reasoningEffort=ReasoningEffort("ultra"),
            ),
        ],
        supportsPersonality=False,
    )

    class TypedModelCodex:
        async def models(self, *, include_hidden: bool = False):
            assert include_hidden is False
            return ModelListResponse(data=[descriptor])

    async def scenario() -> None:
        runtime = CodexRuntime(codex=TypedModelCodex(), cwd="/workspace")

        assert await runtime.list_models() == [
            {
                "additionalSpeedTiers": [],
                "defaultReasoningEffort": "max",
                "description": "Model with forward-compatible reasoning efforts",
                "displayName": "GPT Future",
                "hidden": False,
                "id": "gpt-future",
                "inputModalities": ["text"],
                "isDefault": True,
                "model": "gpt-future",
                "serviceTiers": [],
                "supportedReasoningEfforts": [
                    {
                        "description": "Maximum reasoning",
                        "reasoningEffort": "max",
                    },
                    {
                        "description": "Ultra reasoning",
                        "reasoningEffort": "ultra",
                    },
                ],
                "supportsPersonality": False,
            }
        ]

    asyncio.run(scenario())


def test_list_models_fetches_all_pinned_sdk_pages():
    def descriptor(model_id: str, *, default: bool = False) -> Model:
        return Model(
            additionalSpeedTiers=[],
            defaultReasoningEffort=ReasoningEffort.medium,
            description=f"{model_id} description",
            displayName=model_id.upper(),
            hidden=False,
            id=model_id,
            inputModalities=[InputModality.text],
            isDefault=default,
            model=model_id,
            supportedReasoningEfforts=[
                ReasoningEffortOption(
                    description="Balanced",
                    reasoningEffort=ReasoningEffort.medium,
                )
            ],
            supportsPersonality=False,
        )

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any], object]] = []

        async def request(self, method, params, *, response_model):
            self.calls.append((method, params, response_model))
            return ModelListResponse(data=[descriptor("gpt-page-2")])

    class PagedModelCodex:
        def __init__(self) -> None:
            self._client = Client()

        async def models(self, *, include_hidden: bool = False):
            assert include_hidden is True
            return ModelListResponse(
                data=[descriptor("gpt-page-1", default=True)],
                nextCursor="page-2",
            )

    async def scenario() -> None:
        codex = PagedModelCodex()
        runtime = CodexRuntime(codex=codex, cwd="/workspace")
        catalog = await runtime.list_models(include_hidden=True)
        assert [item["id"] for item in catalog] == ["gpt-page-1", "gpt-page-2"]
        assert codex._client.calls == [
            (
                "model/list",
                {"cursor": "page-2", "includeHidden": True},
                ModelListResponse,
            )
        ]

    asyncio.run(scenario())


def test_list_models_propagates_page_request_type_error():
    class Client:
        async def request(self, _method, _params, *, response_model):
            assert response_model is ModelListResponse
            raise TypeError("page request failed")

    class PagedModelCodex:
        def __init__(self) -> None:
            self._client = Client()

        async def models(self, *, include_hidden: bool = False):
            assert include_hidden is False
            return ModelListResponse(data=[], nextCursor="page-2")

    async def scenario() -> None:
        runtime = CodexRuntime(codex=PagedModelCodex(), cwd="/workspace")
        with pytest.raises(TypeError, match="page request failed"):
            await runtime.list_models()

    asyncio.run(scenario())


def test_legacy_facade_uses_the_complete_paginated_model_catalog():
    class Client:
        async def request(self, method, params, *, response_model):
            assert method == "model/list"
            assert params == {"cursor": "page-2", "includeHidden": True}
            assert response_model is ModelListResponse
            return ModelListResponse(data=[])

    class PagedModelCodex:
        def __init__(self) -> None:
            self._client = Client()

        async def models(self, *, include_hidden: bool = False):
            assert include_hidden is True
            return ModelListResponse(data=[], nextCursor="page-2")

    async def scenario() -> None:
        agent = CodexAgent(codex=PagedModelCodex(), cwd="/workspace")
        assert await agent.list_models(include_hidden=True) == []

    asyncio.run(scenario())


def test_manager_flattens_direct_typed_model_response():
    descriptor = Model(
        additionalSpeedTiers=[],
        defaultReasoningEffort=ReasoningEffort.high,
        description="Typed manager model",
        displayName="Typed Manager",
        hidden=False,
        id="gpt-manager",
        inputModalities=[InputModality.text],
        isDefault=True,
        model="gpt-manager",
        supportedReasoningEfforts=[
            ReasoningEffortOption(
                description="Deep",
                reasoningEffort=ReasoningEffort.high,
            )
        ],
        supportsPersonality=False,
    )

    class Runtime:
        agent_id = "codex"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def run(self, _task, _emit):
            return None

        async def interrupt(self, _task_id: str) -> bool:
            return False

        async def list_models(self, *, include_hidden: bool = False):
            assert include_hidden is False
            return ModelListResponse(data=[descriptor])

    async def scenario() -> None:
        registry = AgentRegistry()
        registry.register("codex", Runtime(), profile=codex_profile())
        manager = TaskManager(object(), registry, worker_count=0)
        models = await manager.list_models(agent_id="codex")
        assert models == [descriptor]

    asyncio.run(scenario())


def test_execute_mode_maps_thread_and_turn_to_full_access():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications("executed"))
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        result = await runtime.run(
            _task(
                    mode_id="execute",
                    policy_version=2,
                metadata={
                    "effective_policy": {
                        "profile_id": "codex",
                        "profile_version": 1,
                        "mode_id": "execute",
                            "mode_policy_version": 2,
                        "sandbox_policy": "full-access",
                        "approval_policy": "deny_all",
                        "can_write_files": True,
                        "can_execute_commands": True,
                    }
                },
            )
        )
        assert result.status == "completed"
        assert fake.thread_start_calls[0]["sandbox"] is Sandbox.full_access
        assert fake.thread_start_calls[0]["approval_mode"] is ApprovalMode.deny_all
        assert (
            fake.thread_start_calls[0]["developer_instructions"]
            == runtime._mode_resolver.require("execute", 2).developer_instructions
        )
        turn_kwargs = fake.thread.turns[0].turn_calls[0][1]
        assert turn_kwargs["sandbox"] is Sandbox.full_access
        assert turn_kwargs["approval_mode"] is ApprovalMode.deny_all

    asyncio.run(scenario())


def test_legacy_execute_mode_keeps_its_versioned_workspace_sandbox():
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications("executed"))
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        result = await runtime.run(
            _task(
                mode_id="execute",
                policy_version=1,
                metadata={
                    "effective_policy": {
                        "profile_id": "codex",
                        "profile_version": 1,
                        "mode_id": "execute",
                        "mode_policy_version": 1,
                        "sandbox_policy": "workspace-write",
                        "approval_policy": "deny_all",
                        "can_write_files": True,
                        "can_execute_commands": True,
                    }
                },
            )
        )

        assert result.status == "completed"
        assert fake.thread_start_calls[0]["sandbox"] is Sandbox.workspace_write
        assert fake.thread.turns[0].turn_calls[0][1]["sandbox"] is Sandbox.workspace_write

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "effective_policy",
    [
        None,
        "workspace-write",
        {
            "profile_id": "codex",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 1,
            "sandbox_policy": "workspace-write",
        },
        {
            "profile_id": "another-agent",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 1,
            "sandbox_policy": "workspace-write",
            "can_write_files": True,
            "can_execute_commands": True,
        },
        {
            "profile_id": "codex",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 2,
            "sandbox_policy": "workspace-write",
            "can_write_files": True,
            "can_execute_commands": True,
        },
        {
            "profile_id": "codex",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 1,
            "sandbox_policy": "full-access",
            "can_write_files": True,
            "can_execute_commands": True,
        },
        {
            "profile_id": "codex",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 1,
            "sandbox_policy": "full-access",
            "can_write_files": False,
            "can_execute_commands": True,
        },
        {
            "profile_id": "codex",
            "profile_version": 1,
            "mode_id": "execute",
            "mode_policy_version": 1,
            "sandbox_policy": "full-access",
            "can_write_files": True,
            "can_execute_commands": False,
        },
    ],
    ids=(
        "missing",
        "malformed",
        "incomplete",
        "wrong-agent",
        "wrong-version",
        "wrong-sandbox",
        "writes-denied",
        "commands-denied",
    ),
)
def test_execute_mode_rejects_unauthorized_policy_before_thread_start(
    effective_policy: Any,
):
    async def scenario() -> None:
        fake = _FakeCodex(_message_notifications("should not run"))
        runtime = CodexRuntime(codex=fake, cwd="/workspace")
        metadata = (
            {} if effective_policy is None else {"effective_policy": effective_policy}
        )

        result = await runtime.run(
            _task(
                mode_id="execute",
                thread_id="persisted-execute-thread",
                metadata=metadata,
            )
        )

        assert result.status == "failed"
        assert "execute mode" in str(result.error)
        assert fake.thread_start_calls == []
        assert fake.thread_resume_calls == []
        assert fake.thread.turns == []

    asyncio.run(scenario())


def test_skill_input_precedes_text_and_preserves_canonical_fields():
    runtime = CodexRuntime(codex=_FakeCodex(_message_notifications()), cwd="/workspace")

    translated = runtime._translate_input(
        {
            "skill": {
                "name": "alpha",
                "skill_id": "alpha",
                "path": "/trusted/skills/alpha",
            },
            "text": "inspect the diff",
        }
    )

    assert isinstance(translated, list)
    assert len(translated) == 2
    skill_input, text_input = translated
    assert isinstance(skill_input, SkillInput)
    assert skill_input.name == "alpha"
    assert skill_input.path == "/trusted/skills/alpha"
    assert isinstance(text_input, TextInput)
    assert text_input.text == "inspect the diff"


def test_historical_skill_snapshot_survives_catalog_path_rollover(tmp_path):
    """A queued v1 task remains executable after discovery selects v2."""

    root = tmp_path / "skills"
    old_path = root / "alpha-v1"
    new_path = root / "alpha-v2"
    old_path.mkdir(parents=True)
    new_path.mkdir(parents=True)
    old_bytes = b"# Alpha v1\ninspect the diff\n"
    (old_path / "SKILL.md").write_bytes(old_bytes)
    (new_path / "SKILL.md").write_bytes(b"# Alpha v2\ninspect the diff differently\n")

    runtime = CodexRuntime(
        codex=_FakeCodex(_message_notifications()),
        cwd="/workspace",
        trusted_skill_roots=(root,),
    )
    # The live catalog has rolled forward to a different path for the same
    # skill ID.  A task already persisted with v1 must use its own snapshot.
    runtime._skills_cache = [
        {"name": "alpha", "path": str(new_path), "version": "2", "enabled": True}
    ]
    historical_input = {
        "skill": {
            "name": "alpha",
            "skill_id": "alpha",
            "path": str(old_path),
            "version": "1",
            "content_hash": hash_skill_bundle(old_path),
        },
        "text": "inspect this diff",
    }
    translated = runtime._translate_input(historical_input)
    assert isinstance(translated, list)
    assert isinstance(translated[0], SkillInput)
    assert translated[0].path == str(old_path.resolve())

    unrooted = CodexRuntime(
        codex=_FakeCodex(_message_notifications()), cwd="/workspace"
    )
    unrooted._skills_cache = list(runtime._skills_cache)
    with pytest.raises(ValueError, match="catalog"):
        unrooted._translate_input(historical_input)

    missing_name = CodexRuntime(
        codex=_FakeCodex(_message_notifications()),
        cwd="/workspace",
        trusted_skill_roots=(root,),
    )
    missing_name._skills_cache = [
        {"name": "beta", "path": str(new_path), "version": "2", "enabled": True}
    ]
    with pytest.raises(ValueError, match="trusted catalog"):
        missing_name._translate_input(historical_input)

    # The hash is an immutable content check, not merely a path exemption.
    (old_path / "SKILL.md").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="hash"):
        runtime._translate_input(
            {
                "skill": {
                    "name": "alpha",
                    "path": str(old_path),
                    "content_hash": historical_input["skill"]["content_hash"],
                },
                "text": "inspect this diff",
            }
        )


def test_skill_input_revalidates_the_complete_bundle_before_sdk_translation(tmp_path):
    root = tmp_path / "skills"
    skill_path = root / "alpha"
    references = skill_path / "references"
    references.mkdir(parents=True)
    (skill_path / "SKILL.md").write_text("instructions", encoding="utf-8")
    support_file = references / "rules.txt"
    support_file.write_text("trusted rules", encoding="utf-8")
    snapshot_hash = hash_skill_bundle(skill_path)
    runtime = CodexRuntime(
        codex=_FakeCodex(_message_notifications()),
        cwd="/workspace",
        trusted_skill_roots=(root,),
    )

    def translate() -> list[object]:
        translated = runtime._translate_input(
            {
                "skill": {
                    "name": "alpha",
                    "path": str(skill_path),
                    "content_hash": snapshot_hash,
                },
                "text": "inspect this diff",
            }
        )
        assert isinstance(translated, list)
        return translated

    assert isinstance(translate()[0], SkillInput)

    support_file.write_text("mutated rules", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        translate()

    support_file.write_text("trusted rules", encoding="utf-8")
    added = references / "added.txt"
    added.write_text("new instructions", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        translate()

    added.unlink()
    linked = references / "linked.txt"
    linked.symlink_to(tmp_path / "outside.txt")
    with pytest.raises(ValueError, match="not trusted"):
        translate()


def test_resolve_skill_replaces_sdk_manifest_hash_with_bundle_hash(tmp_path):
    skill_path = tmp_path / "skills" / "alpha"
    skill_path.mkdir(parents=True)
    manifest = skill_path / "SKILL.md"
    manifest.write_text("instructions", encoding="utf-8")
    (skill_path / "support.txt").write_text("support data", encoding="utf-8")
    manifest_only_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()

    runtime = CodexRuntime(
        codex=_FakeCodex(_message_notifications()),
        cwd="/workspace",
        trusted_skill_roots=(skill_path.parent,),
    )
    runtime._skills_cache = [
        {
            "name": "alpha",
            "path": str(skill_path),
            "content_hash": manifest_only_hash,
            "enabled": True,
        }
    ]

    resolved = asyncio.run(runtime.resolve_skill("alpha"))
    assert resolved is not None
    assert resolved["content_hash"] == hash_skill_bundle(skill_path)
    assert resolved["content_hash"] != manifest_only_hash


def test_list_skills_accepts_a_flat_compatibility_catalog():
    class FlatCatalogCodex:
        async def list_skills(self, *, refresh: bool = False):
            assert refresh is False
            return [
                {
                    "name": "alpha",
                    "path": "/trusted/skills/alpha",
                    "description": "Alpha skill",
                    "enabled": True,
                }
            ]

    async def scenario() -> None:
        runtime = CodexRuntime(codex=FlatCatalogCodex(), cwd="/workspace")
        catalog = await runtime.list_skills()
        assert catalog == [
            {
                "name": "alpha",
                "path": "/trusted/skills/alpha",
                "description": "Alpha skill",
                "enabled": True,
            }
        ]
        resolved = await runtime.resolve_skill("ALPHA")
        assert resolved is not None
        assert resolved["skill_id"] == "alpha"
        assert resolved["path"] == "/trusted/skills/alpha"

    asyncio.run(scenario())


def test_list_skills_accepts_a_typed_descriptor():
    from src.runtime.skills import SkillDefinition

    class TypedCatalogCodex:
        async def list_skills(self, *, refresh: bool = False):
            return SkillDefinition(
                name="typed",
                path="/trusted/skills/typed",
                description="Typed skill",
            )

    async def scenario() -> None:
        runtime = CodexRuntime(codex=TypedCatalogCodex(), cwd="/workspace")
        catalog = await runtime.list_skills()
        assert len(catalog) == 1
        assert catalog[0]["name"] == "typed"
        assert catalog[0]["path"] == "/trusted/skills/typed"

    asyncio.run(scenario())


def test_list_skills_flattens_sequence_wrapped_sdk_response():
    from openai_codex.generated.v2_all import (
        AbsolutePathBuf,
        SkillInterface,
        SkillMetadata,
        SkillScope,
        SkillsListEntry,
        SkillsListResponse,
    )

    response = SkillsListResponse(
        data=[
            SkillsListEntry(
                cwd="/workspace",
                errors=[],
                skills=[
                    SkillMetadata(
                        name="Typed-SDK",
                        path=AbsolutePathBuf("/trusted/skills/typed-sdk"),
                        description="Full description",
                        shortDescription=None,
                        interface=SkillInterface(
                            displayName="Typed SDK",
                            shortDescription="Public summary",
                        ),
                        enabled=True,
                        scope=SkillScope.repo,
                    )
                ],
            )
        ]
    )

    class TypedResponseCodex:
        async def list_skills(self, *, refresh: bool = False):
            assert refresh is False
            # Compatibility transports may wrap a typed response in a page or
            # batch sequence; every envelope level must still be traversed.
            return [response]

    async def scenario() -> None:
        runtime = CodexRuntime(codex=TypedResponseCodex(), cwd="/workspace")
        catalog = await runtime.list_skills()
        assert catalog == [
            {
                "description": "Full description",
                "enabled": True,
                "interface": {
                    "displayName": "Typed SDK",
                    "shortDescription": "Public summary",
                },
                "name": "Typed-SDK",
                "path": "/trusted/skills/typed-sdk",
                "scope": "repo",
            }
        ]
        resolved = await runtime.resolve_skill("typed-sdk")
        assert resolved is not None
        assert resolved["path"] == "/trusted/skills/typed-sdk"
        assert resolved["display_name"] == "Typed SDK"
        assert resolved["description"] == "Public summary"

    asyncio.run(scenario())


def test_list_skills_flattens_pinned_sdk_rpc_response():
    from openai_codex.generated.v2_all import (
        AbsolutePathBuf,
        SkillMetadata,
        SkillScope,
        SkillsListEntry,
        SkillsListResponse,
    )

    response = SkillsListResponse(
        data=[
            SkillsListEntry(
                cwd="/workspace",
                errors=[],
                skills=[
                    SkillMetadata(
                        name="rpc-skill",
                        path=AbsolutePathBuf("/trusted/skills/rpc-skill"),
                        description="RPC skill",
                        enabled=True,
                        interface=None,
                        scope=SkillScope.repo,
                    )
                ],
            )
        ]
    )

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any], object]] = []

        async def request(self, method, params, *, response_model):
            self.calls.append((method, params, response_model))
            return response

    class RpcCodex:
        def __init__(self) -> None:
            self._client = Client()
            self.initialized = 0

        async def _ensure_initialized(self) -> None:
            self.initialized += 1

    async def scenario() -> None:
        codex = RpcCodex()
        runtime = CodexRuntime(codex=codex, cwd="/workspace")
        catalog = await runtime.list_skills(refresh=True)
        assert [item["name"] for item in catalog] == ["rpc-skill"]
        assert catalog[0]["path"] == "/trusted/skills/rpc-skill"
        assert codex.initialized == 1
        assert codex._client.calls == [
            (
                "skills/list",
                {"cwds": ["/workspace"], "forceReload": True},
                SkillsListResponse,
            )
        ]

    asyncio.run(scenario())


class _ConcurrencyTracker:
    def __init__(self) -> None:
        self.active = 0
        self.maximum_active = 0
        self.started_count = 0
        self.started = asyncio.Event()
        self.releases: list[asyncio.Event] = []


class _GateTurn:
    def __init__(self, tracker: _ConcurrencyTracker, turn_number: int) -> None:
        self.tracker = tracker
        self.turn_number = turn_number
        self.interrupt_calls = 0
        self.release = asyncio.Event()
        tracker.releases.append(self.release)

    async def interrupt(self) -> None:
        self.interrupt_calls += 1
        self.release.set()

    def stream(self):
        async def produce():
            self.tracker.active += 1
            self.tracker.maximum_active = max(
                self.tracker.maximum_active, self.tracker.active
            )
            self.tracker.started_count += 1
            self.tracker.started.set()
            try:
                await self.release.wait()
                yield "done"
                yield _turn_notification(
                    TurnStatus.completed, turn_id=f"turn-{self.turn_number}"
                )
            finally:
                self.tracker.active -= 1

        return produce()


class _GateThread:
    def __init__(self, tracker: _ConcurrencyTracker, thread_number: int) -> None:
        self.id = f"thread-{thread_number}"
        self.tracker = tracker
        self.thread_number = thread_number

    async def turn(self, input_value: Any, **_kwargs: Any) -> _GateTurn:
        return _GateTurn(self.tracker, self.thread_number)


class _GateCodex:
    def __init__(self, tracker: _ConcurrencyTracker) -> None:
        self.tracker = tracker
        self.thread_number = 0

    async def thread_start(self, **_kwargs: Any) -> _GateThread:
        self.thread_number += 1
        return _GateThread(self.tracker, self.thread_number)


async def _wait_for_started(tracker: _ConcurrencyTracker, count: int) -> None:
    while tracker.started_count < count:
        tracker.started.clear()
        await tracker.started.wait()


def test_runtime_serializes_modes_per_conversation_but_allows_other_conversations():
    async def scenario() -> None:
        same_tracker = _ConcurrencyTracker()
        same_runtime = CodexRuntime(codex=_GateCodex(same_tracker), cwd="/workspace")
        first = asyncio.create_task(
            same_runtime.run(_task(task_id="same-1", mode_id="chat"))
        )
        await _wait_for_started(same_tracker, 1)
        second = asyncio.create_task(
            same_runtime.run(_task(task_id="same-2", mode_id="plan"))
        )
        await asyncio.sleep(0)
        assert same_tracker.started_count == 1
        assert same_tracker.maximum_active == 1
        same_tracker.releases[0].set()
        await _wait_for_started(same_tracker, 2)
        same_tracker.releases[1].set()
        first_result, second_result = await asyncio.gather(first, second)
        assert first_result.status == second_result.status == "completed"
        assert same_tracker.maximum_active == 1

        cross_tracker = _ConcurrencyTracker()
        cross_runtime = CodexRuntime(
            codex=_GateCodex(cross_tracker), cwd="/workspace"
        )
        cross_first = asyncio.create_task(
            cross_runtime.run(
                _task(task_id="cross-1", conversation_id="conversation-a")
            )
        )
        cross_second = asyncio.create_task(
            cross_runtime.run(
                _task(
                    task_id="cross-2",
                    conversation_id="conversation-b",
                    mode_id="plan",
                )
            )
        )
        await _wait_for_started(cross_tracker, 2)
        assert cross_tracker.maximum_active == 2
        for release in cross_tracker.releases:
            release.set()
        cross_first_result, cross_second_result = await asyncio.gather(
            cross_first, cross_second
        )
        assert cross_first_result.status == cross_second_result.status == "completed"

    asyncio.run(scenario())
