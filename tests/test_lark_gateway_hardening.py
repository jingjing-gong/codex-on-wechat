from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.channels.lark import (
    COMMAND_INTERRUPTED_RESPONSE,
    AccountState,
    LarkAccountSupervisor,
    LarkBotProfile,
    LarkCommandRouter,
    LarkError,
    LarkGateway,
    parse_lark_command,
)
from src.channels.models import InboundEnvelope, parse_command


def _profile(
    tmp_path: Path,
    *,
    access_policy: str = "all",
    restart_max_attempts: int = 8,
) -> LarkBotProfile:
    config = tmp_path / "config"
    config.mkdir(mode=0o700, exist_ok=True)
    return LarkBotProfile(
        profile_id="gateway",
        app_id="cli_gatewaybot1",
        bot_open_id="ou_gatewaybot1",
        config_dir=config,
        access_policy=access_policy,
        restart_max_attempts=restart_max_attempts,
        restart_base_delay=0.001,
        restart_max_delay=0.002,
    )


def _event(text: str, *, message_id: str = "om_gatewaymessage1") -> dict[str, object]:
    return {
        "type": "im.message.receive_v1",
        "event_id": f"event-{message_id}",
        "message_id": message_id,
        "chat_id": "oc_gatewaychat1",
        "chat_type": "p2p",
        "message_type": "text",
        "sender_id": "ou_gatewayuser1",
        "content": text,
    }


def _media_event(*, message_id: str = "om_gatewaymedia1") -> dict[str, object]:
    value = _event("", message_id=message_id)
    value.update(
        {
            "message_type": "image",
            "content": "[Image: img_gatewayresource1]",
        }
    )
    return value


def _owner_command_envelope(
    text: str,
    *,
    principal_id: str = "owner",
    principal_account_id: str = "principal-account-owner",
    subject_kind: str = "direct",
) -> InboundEnvelope:
    return InboundEnvelope(
        channel="lark",
        bot_id="cli_gatewaybot1",
        external_user_id="ou_gatewayuser1",
        external_message_id="om_onboardingcommand1",
        text=text,
        principal_id=principal_id,
        principal_account_id=principal_account_id,
        conversation_subject_kind=subject_kind,
        destination_kind="open_id" if subject_kind == "direct" else "group",
        destination_id=(
            "ou_gatewayuser1" if subject_kind == "direct" else "oc_gatewaychat1"
        ),
    )


class _ReceiptRuntime:
    def __init__(self, *, initial_state: str = "") -> None:
        self.receipt: dict[str, object] | None = None
        self.initial_state = initial_state
        self.accepted = 0
        self.outbox: list[dict[str, object]] = []
        self.interruptions = 0

    async def accept_inbound(self, _envelope, **_kwargs):
        self.accepted += 1
        return SimpleNamespace(agent_id="codex")

    async def begin_command_receipt(self, command_id: str, **kwargs):
        first = self.receipt is None
        created = first and not self.initial_state
        if first:
            self.receipt = {
                "command_id": command_id,
                **kwargs,
                "state": self.initial_state or "started",
                "response_text": (
                    "already complete" if self.initial_state == "completed" else ""
                ),
                "response_agent_id": "codex",
            }
        return {**self.receipt, "created": created}

    async def get_command_receipt(self, _command_id: str):
        return dict(self.receipt) if self.receipt is not None else None

    async def complete_command_receipt(
        self,
        _command_id: str,
        *,
        response_text: str,
        response_agent_id: str = "",
    ):
        assert self.receipt is not None
        if self.receipt["state"] != "started":
            raise RuntimeError("receipt is not active")
        self.receipt.update(
            state="completed",
            response_text=response_text,
            response_agent_id=response_agent_id,
        )
        return dict(self.receipt)

    async def interrupt_command_receipt(self, _command_id: str):
        self.interruptions += 1
        assert self.receipt is not None
        if self.receipt["state"] == "started":
            self.receipt["state"] = "interrupted"
        return dict(self.receipt)

    async def create_account_outbox(self, **kwargs):
        self.outbox.append(kwargs)
        return kwargs


class _BlockingRouter:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def handle_command(self, *_args, **_kwargs):
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return "command result"


@pytest.mark.parametrize(
    "phrase",
    (
        "新增一个飞书 bot",
        "新增一个飞书bot",
        "新增一个飞书机器人",
        "  新增一个飞书   BOT  ",
    ),
)
def test_lark_onboarding_natural_phrase_is_exact_and_adapter_local(phrase):
    command = parse_lark_command(phrase)
    assert command is not None
    assert command.name == "lark"
    assert command.args == ("add",)

    assert parse_lark_command("请新增一个飞书 bot") is None
    assert parse_lark_command("新增一个飞书 bot，谢谢") is None
    assert parse_command("新增一个飞书 bot") is None


def test_owner_direct_lark_add_invokes_injected_service_once():
    class Service:
        def __init__(self):
            self.calls = []

        async def start(self, envelope, *, command_id, profile_id=""):
            self.calls.append((envelope, command_id, profile_id))

    class Shared:
        async def handle_command(self, *_args, **_kwargs):
            raise AssertionError("Lark onboarding must not reach shared commands")

    async def scenario():
        service = Service()
        router = LarkCommandRouter(
            object(),
            shared_router=Shared(),
            onboarding_service=service,
        )
        envelope = _owner_command_envelope("/lark add work-bot")
        response = await router.handle_command(
            parse_command(envelope.text),
            envelope,
            command_id="command-onboarding-1",
        )

        assert response is None
        assert service.calls == [
            (envelope, "command-onboarding-1", "work-bot")
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("principal_id", "principal_account_id"),
    (("", ""), ("owner", ""), ("administrator", "admin-account")),
)
def test_lark_add_requires_exact_enabled_owner_mapping(
    principal_id,
    principal_account_id,
):
    class Service:
        calls = 0

        async def start(self, *_args, **_kwargs):
            self.calls += 1

    async def scenario():
        service = Service()
        router = LarkCommandRouter(
            object(),
            onboarding_service=service,
            # Lark administrator status deliberately does not grant the
            # narrower credential-provisioning authority.
            administrator=lambda _envelope: True,
        )
        envelope = _owner_command_envelope(
            "/lark add",
            principal_id=principal_id,
            principal_account_id=principal_account_id,
        )
        response = await router.handle_command(
            parse_command(envelope.text), envelope
        )

        assert response == "owner authority is required to add a Lark bot"
        assert service.calls == 0

    asyncio.run(scenario())


def test_lark_add_rejects_group_chat_even_for_owner():
    class Service:
        calls = 0

        async def start(self, *_args, **_kwargs):
            self.calls += 1

    async def scenario():
        service = Service()
        router = LarkCommandRouter(object(), onboarding_service=service)
        envelope = _owner_command_envelope("/lark add", subject_kind="group")
        response = await router.handle_command(
            parse_command(envelope.text), envelope
        )

        assert response == "Lark bot onboarding is only available in a direct chat"
        assert service.calls == 0

    asyncio.run(scenario())


def test_lark_add_start_failure_never_exposes_service_exception(caplog):
    secret = "appSecret=must-not-leak"

    class Service:
        async def start(self, *_args, **_kwargs):
            raise RuntimeError(secret)

    async def scenario():
        router = LarkCommandRouter(object(), onboarding_service=Service())
        envelope = _owner_command_envelope("/lark add")
        response = await router.handle_command(
            parse_command(envelope.text), envelope
        )

        assert response == (
            "Lark bot onboarding could not be started; try again later"
        )
        assert secret not in response

    asyncio.run(scenario())
    assert secret not in caplog.text


def test_gateway_natural_phrase_uses_authenticated_resolution_and_empty_receipt(
    tmp_path,
):
    class Runtime(_ReceiptRuntime):
        def __init__(self):
            super().__init__()
            self.accept_options = []

        async def accept_inbound(self, _envelope, **kwargs):
            self.accept_options.append(kwargs)
            return await super().accept_inbound(_envelope, **kwargs)

    class Resolver:
        async def resolve(self, _actor):
            return SimpleNamespace(
                principal_id="owner",
                principal_account_id="principal-account-owner",
            )

    class Service:
        def __init__(self):
            self.calls = []

        async def start(self, envelope, *, command_id, profile_id=""):
            self.calls.append((envelope, command_id, profile_id))

    async def scenario():
        runtime = Runtime()
        service = Service()
        gateway = LarkGateway(
            runtime,
            _profile(tmp_path),
            principal_resolver=Resolver(),
            onboarding_service=service,
        )
        await gateway.accept_event(_event("新增一个飞书 bot"))

        assert len(service.calls) == 1
        envelope, command_id, profile_id = service.calls[0]
        assert envelope.principal_id == "owner"
        assert envelope.principal_account_id == "principal-account-owner"
        assert command_id.startswith("lark-command:")
        assert profile_id == ""
        assert runtime.accept_options[0]["create_task"] is False
        assert runtime.receipt is not None
        assert runtime.receipt["command_name"] == "lark"
        assert runtime.receipt["command_args"] == ("add",)
        assert runtime.receipt["response_text"] == ""
        assert runtime.outbox == []

    asyncio.run(scenario())


def test_gateway_ignores_spoofed_owner_metadata_for_lark_add(tmp_path):
    class Service:
        calls = 0

        async def start(self, *_args, **_kwargs):
            self.calls += 1

    async def scenario():
        runtime = _ReceiptRuntime()
        service = Service()
        gateway = LarkGateway(
            runtime,
            _profile(tmp_path),
            onboarding_service=service,
        )
        event = _event("新增一个飞书 bot")
        event.update(
            principal_id="owner",
            principal_account_id="forged-account",
        )
        await gateway.accept_event(event)

        assert service.calls == 0
        assert len(runtime.outbox) == 1
        assert runtime.outbox[0]["delivery"]["content"] == (
            "owner authority is required to add a Lark bot"
        )

    asyncio.run(scenario())


def test_concurrent_started_receipt_has_one_effect_owner(tmp_path):
    async def scenario():
        runtime = _ReceiptRuntime()
        router = _BlockingRouter()
        gateway = LarkGateway(runtime, _profile(tmp_path), command_router=router)
        owner = asyncio.create_task(gateway.accept_event(_event("/help")))
        await router.entered.wait()
        duplicate = await gateway.accept_event(_event("/help"))
        assert duplicate.agent_id == "codex"
        assert router.calls == 1
        assert runtime.outbox == []
        router.release.set()
        await owner
        assert runtime.receipt is not None
        assert runtime.receipt["state"] == "completed"
        assert len(runtime.outbox) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("state", ["interrupted", "mystery"])
def test_nonterminal_recovery_receipt_never_reruns_command(tmp_path, state):
    class Router:
        calls = 0

        async def handle_command(self, *_args, **_kwargs):
            self.calls += 1
            return "must not run"

    async def scenario():
        runtime = _ReceiptRuntime(initial_state=state)
        router = Router()
        gateway = LarkGateway(runtime, _profile(tmp_path), command_router=router)
        if state == "mystery":
            with pytest.raises(RuntimeError, match="unsupported state"):
                await gateway.accept_event(_event("/help"))
            assert runtime.outbox == []
        else:
            await gateway.accept_event(_event("/help"))
            assert runtime.outbox[0]["delivery"]["content"] == (
                COMMAND_INTERRUPTED_RESPONSE
            )
        assert router.calls == 0

    asyncio.run(scenario())


def test_completed_command_receipt_replays_without_effect(tmp_path):
    class Router:
        calls = 0

        async def handle_command(self, *_args, **_kwargs):
            self.calls += 1
            return "must not run"

    async def scenario():
        runtime = _ReceiptRuntime(initial_state="completed")
        router = Router()
        gateway = LarkGateway(runtime, _profile(tmp_path), command_router=router)
        await gateway.accept_event(_event("/help"))
        assert router.calls == 0
        assert runtime.outbox[0]["delivery"]["content"] == "already complete"

    asyncio.run(scenario())


def test_cancelled_command_owner_is_durably_interrupted(tmp_path):
    async def scenario():
        runtime = _ReceiptRuntime()
        router = _BlockingRouter()
        gateway = LarkGateway(runtime, _profile(tmp_path), command_router=router)
        owner = asyncio.create_task(gateway.accept_event(_event("/help")))
        await router.entered.wait()
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert runtime.receipt is not None
        assert runtime.receipt["state"] == "interrupted"
        assert runtime.interruptions == 1
        assert runtime.outbox == []

    asyncio.run(scenario())


def test_mapped_access_is_checked_before_attachment_promotion(tmp_path):
    class Resolver:
        async def resolve(self, _actor):
            return SimpleNamespace(principal_id="", principal_account_id="")

    class Promoter:
        calls = 0

        async def promote(self, _envelope):
            self.calls += 1
            raise AssertionError("denied attachment must not be downloaded")

    async def scenario():
        runtime = _ReceiptRuntime()
        promoter = Promoter()
        gateway = LarkGateway(
            runtime,
            _profile(tmp_path, access_policy="mapped"),
            principal_resolver=Resolver(),
            attachment_promoter=promoter,
        )
        assert await gateway.accept_event(_media_event()) is None
        assert promoter.calls == 0
        assert runtime.accepted == 0

    asyncio.run(scenario())


def test_non_admin_ask_can_only_target_an_existing_enabled_agent(tmp_path):
    class Manager:
        allow_dynamic_agents = True

        def __init__(self):
            self._dynamic_agent_lock = asyncio.Lock()

        async def list_agents(self, *, include_disabled=False):
            assert include_disabled is False
            return [
                SimpleNamespace(agent_id="enabled", enabled=True),
                SimpleNamespace(agent_id="disabled", enabled=False),
            ]

    class Shared:
        def __init__(self, manager):
            self.calls = 0
            self.manager = manager

        async def handle_command(self, *_args, **_kwargs):
            assert self.manager._dynamic_agent_lock.locked()
            self.calls += 1
            return "delegated"

    async def scenario():
        manager = Manager()
        shared = Shared(manager)
        router = LarkCommandRouter(manager, shared_router=shared)
        envelope = InboundEnvelope(
            channel="lark",
            bot_id="cli_gatewaybot1",
            external_user_id="ou_gatewayuser1",
            external_message_id="om_askauthority1",
            text="/ask missing do work",
        )
        denied = await router.handle_command(
            parse_command("/ask missing do work"), envelope
        )
        disabled = await router.handle_command(
            parse_command("/ask disabled do work"), envelope
        )
        allowed = await router.handle_command(
            parse_command("/ask enabled do work"), envelope
        )
        assert denied == disabled == "Agent does not exist or is disabled"
        assert allowed == "delegated"
        assert shared.calls == 1

    asyncio.run(scenario())


def test_non_admin_agent_uses_existing_only_switch_capability(tmp_path):
    class Manager:
        async def list_agents(self, *, include_disabled=False):
            assert include_disabled is False
            return [SimpleNamespace(agent_id="enabled", enabled=True)]

    class Shared:
        def __init__(self):
            self.existing_agent_only = False

        async def handle_command(
            self,
            _command,
            _envelope,
            *,
            command_id="",
            existing_agent_only=False,
        ):
            assert command_id == "command-1"
            self.existing_agent_only = existing_agent_only
            return "delegated"

    async def scenario():
        shared = Shared()
        router = LarkCommandRouter(Manager(), shared_router=shared)
        envelope = InboundEnvelope(
            channel="lark",
            bot_id="cli_gatewaybot1",
            external_user_id="ou_gatewayuser1",
            external_message_id="om_agentauthority1",
            text="/agent enabled",
        )
        response = await router.handle_command(
            parse_command("/agent enabled"),
            envelope,
            command_id="command-1",
        )
        assert response == "delegated"
        assert shared.existing_agent_only is True

    asyncio.run(scenario())


def test_ready_then_crash_consumes_bounded_restart_budget(tmp_path):
    class Process:
        def __init__(self):
            self.starts = 0
            self.stops = 0

        async def start(self):
            self.starts += 1
            return self.starts

        async def events(self):
            if False:
                yield None

        async def stop(self):
            self.stops += 1

    class Gateway:
        async def accept_event(self, *_args, **_kwargs):
            raise AssertionError("no events are emitted")

    async def scenario():
        process = Process()
        supervisor = LarkAccountSupervisor(
            _profile(tmp_path, restart_max_attempts=2),
            Gateway(),
            process,
            stable_reset_after=60,
        )
        await asyncio.wait_for(supervisor.run(), timeout=1)
        assert process.starts == 3
        assert supervisor.state is AccountState.FAILED

    asyncio.run(scenario())


def test_disabled_account_preserves_disabled_terminal_state(tmp_path):
    class Process:
        async def start(self):
            raise AssertionError("disabled account must not start")

        async def stop(self):
            raise AssertionError("disabled account has nothing to stop")

    async def scenario():
        profile = replace(_profile(tmp_path), enabled=False)
        supervisor = LarkAccountSupervisor(profile, object(), Process())
        await supervisor.run()
        assert supervisor.state is AccountState.DISABLED

    asyncio.run(scenario())


def test_supervisor_wait_until_ready_returns_started_generation(tmp_path):
    class Process:
        def __init__(self):
            self.never = asyncio.Event()

        async def start(self):
            return 7

        async def events(self):
            await self.never.wait()
            if False:
                yield None

        async def stop(self):
            return None

    class Gateway:
        async def accept_event(self, *_args, **_kwargs):
            raise AssertionError("idle process has no events")

    async def scenario():
        supervisor = LarkAccountSupervisor(_profile(tmp_path), Gateway(), Process())
        running = asyncio.create_task(supervisor.run())
        try:
            assert await supervisor.wait_until_ready(timeout=0.5) == 7
            assert supervisor.state is AccountState.READY
        finally:
            supervisor.stop()
            await asyncio.wait_for(running, timeout=0.5)

    asyncio.run(scenario())


def test_supervisor_wait_until_ready_fails_on_terminal_state_or_timeout(tmp_path):
    async def scenario():
        disabled = LarkAccountSupervisor(
            replace(_profile(tmp_path), enabled=False), object(), object()
        )
        with pytest.raises(LarkError, match="did not become ready \\(disabled\\)"):
            await disabled.wait_until_ready(timeout=0.1)

        waiting = LarkAccountSupervisor(_profile(tmp_path), object(), object())
        with pytest.raises(asyncio.TimeoutError):
            await waiting.wait_until_ready(timeout=0.001)

        waiting.stop()
        with pytest.raises(LarkError, match="did not become ready \\(disconnected\\)"):
            await waiting.wait_until_ready(timeout=0.1)

    asyncio.run(scenario())


def test_supervisor_stop_interrupts_idle_event_wait(tmp_path):
    class Process:
        def __init__(self):
            self.started = asyncio.Event()
            self.never = asyncio.Event()
            self.stops = 0

        async def start(self):
            self.started.set()
            return 1

        async def events(self):
            await self.never.wait()
            if False:
                yield None

        async def stop(self):
            self.stops += 1

    class Gateway:
        async def accept_event(self, *_args, **_kwargs):
            raise AssertionError("idle process has no events")

    async def scenario():
        process = Process()
        supervisor = LarkAccountSupervisor(_profile(tmp_path), Gateway(), process)
        running = asyncio.create_task(supervisor.run())
        await process.started.wait()
        for _ in range(100):
            if supervisor.state is AccountState.READY:
                break
            await asyncio.sleep(0.01)
        supervisor.stop()
        await asyncio.wait_for(running, timeout=0.5)
        assert process.stops >= 1
        assert supervisor.state is AccountState.DISCONNECTED

    asyncio.run(scenario())


def test_supervisor_fences_ingress_then_boundedly_drains_delivery(tmp_path):
    class Process:
        def __init__(self):
            self.started = asyncio.Event()
            self.never = asyncio.Event()

        async def start(self):
            order.append("consumer-start")
            self.started.set()
            return 1

        async def events(self):
            await self.never.wait()
            if False:
                yield None

        async def stop(self):
            order.append("consumer-stop")

    class Worker:
        def __init__(self):
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def run(self):
            self.entered.set()
            try:
                await self.release.wait()
                order.append("delivery-finished")
            except asyncio.CancelledError:
                order.append("delivery-cancelled")
                raise

        def stop(self):
            order.append("delivery-stop")
            asyncio.get_running_loop().call_later(0.01, self.release.set)

    class Gateway:
        async def accept_event(self, *_args, **_kwargs):
            raise AssertionError("idle process has no events")

    async def scenario():
        process = Process()
        worker = Worker()
        supervisor = LarkAccountSupervisor(
            _profile(tmp_path),
            Gateway(),
            process,
            delivery_worker=worker,
            delivery_drain_timeout=0.2,
        )
        running = asyncio.create_task(supervisor.run())
        await process.started.wait()
        await worker.entered.wait()
        supervisor.stop()
        await asyncio.wait_for(running, timeout=0.5)
        assert order.index("consumer-stop") < order.index("delivery-stop")
        assert "delivery-finished" in order
        assert "delivery-cancelled" not in order

    order: list[str] = []
    asyncio.run(scenario())
