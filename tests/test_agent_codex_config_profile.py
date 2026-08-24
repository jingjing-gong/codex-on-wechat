"""Focused command/process contracts for per-Agent Codex config profiles."""

from __future__ import annotations


class _ProfileProbeBackend:
    """Spawn-safe backend that exposes only the profile received by the child."""

    def __init__(
        self,
        *,
        agent_id: str = "",
        codex_config_profile: str = "",
        **_kwargs,
    ) -> None:
        self.agent_id = agent_id
        self.codex_config_profile = codex_config_profile

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def run(self, _task, _emit=None):
        return {"status": "completed"}

    async def list_models(self, *, include_hidden: bool = False):
        assert include_hidden is False
        if self.codex_config_profile == "qwen":
            return [
                {
                    "id": "qwen3.8-27b",
                    "displayName": "Qwen 3.8 27B",
                    "isDefault": True,
                }
            ]
        return [{"id": "base-model", "isDefault": True}]


def profile_probe_backend_factory(**kwargs):
    """Importable factory used by the spawned process-proxy regression."""

    return _ProfileProbeBackend(**kwargs)


def test_agent_command_routes_named_profile_and_models_to_selected_agent():
    import asyncio

    from src.channels.models import InboundEnvelope, parse_command
    from src.channels.wechat import COMMAND_HELP, MVPCommandRouter

    calls: list[tuple[str, str | None, dict[str, object]]] = []

    class Manager:
        active_agent = "codex"

        async def get_active_agent(self, **_scope) -> str:
            return self.active_agent

        async def set_active_agent(
            self,
            agent_id: str,
            *,
            codex_config_profile: str | None = None,
            **scope,
        ) -> str:
            calls.append((agent_id, codex_config_profile, scope))
            self.active_agent = agent_id
            return agent_id

        async def switch_back_inbox(self, **_scope):
            return []

        async def list_models(self, *, agent_id: str, include_hidden: bool = False):
            assert agent_id == "researcher"
            assert include_hidden is False
            return [
                {
                    "id": "qwen3.8-27b",
                    "displayName": "Qwen 3.8 27B",
                    "isDefault": True,
                }
            ]

        async def get_model_selection(self, **scope):
            assert scope["agent_id"] == "researcher"
            return {
                "agent_id": "researcher",
                "model_id": "qwen3.8-27b",
                "reasoning_effort": "",
            }

    def envelope(text: str, message_id: str) -> InboundEnvelope:
        return InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id=message_id,
            text=text,
            session_id="default",
            agent_id="codex",
            conversation_id="wechat:bot:user:default:codex",
        )

    async def scenario() -> None:
        manager = Manager()
        router = MVPCommandRouter(manager)
        switch_text = "/agent researcher qwen"
        switched = await router.handle_command(
            parse_command(switch_text), envelope(switch_text, "switch")
        )
        assert str(switched) == "switched to Agent: researcher"
        assert calls == [
            (
                "researcher",
                "qwen",
                {
                    "channel": "wechat",
                    "bot_id": "bot",
                    "external_user_id": "user",
                    "session_id": "default",
                },
            )
        ]

        models_text = "/models"
        models = await router.handle_command(
            parse_command(models_text), envelope(models_text, "models")
        )
        assert "qwen3.8-27b" in str(models)
        assert "**(current)**" in str(models)

        usage_text = "/agent researcher qwen extra"
        usage = await router.handle_command(
            parse_command(usage_text), envelope(usage_text, "usage")
        )
        assert usage == "usage: /agent [agent-id] [profile]"
        assert len(calls) == 1

    assert "`/agent [agent-id] [profile]`" in COMMAND_HELP
    asyncio.run(scenario())


def test_one_argument_agent_command_preserves_existing_profile_binding():
    import asyncio

    from src.channels.models import InboundEnvelope, parse_command
    from src.channels.wechat import MVPCommandRouter

    received: list[str | None] = []

    class Manager:
        async def get_active_agent(self, **_scope) -> str:
            return "codex"

        async def set_active_agent(
            self,
            _agent_id: str,
            *,
            codex_config_profile: str | None = None,
            **_scope,
        ) -> str:
            received.append(codex_config_profile)
            return "researcher"

        async def switch_back_inbox(self, **_scope):
            return []

    async def scenario() -> None:
        text = "/agent researcher"
        envelope = InboundEnvelope(
            channel="wechat",
            bot_id="bot",
            external_user_id="user",
            external_message_id="one-argument",
            text=text,
            session_id="default",
            agent_id="codex",
            conversation_id="wechat:bot:user:default:codex",
        )
        response = await MVPCommandRouter(Manager()).handle_command(
            parse_command(text), envelope
        )
        assert str(response) == "switched to Agent: researcher"
        assert received == [None]

    asyncio.run(scenario())


def test_process_proxy_propagates_profile_into_child_model_catalog(tmp_path):
    import asyncio

    from src.runtime.process_agent import ProcessAgentRuntime

    async def scenario() -> None:
        runtime = ProcessAgentRuntime.create(
            "researcher",
            cwd=str(tmp_path),
            codex_config_profile="qwen",
            backend_factory=(
                "tests.test_agent_codex_config_profile:"
                "profile_probe_backend_factory"
            ),
            start_timeout=5,
            stop_timeout=5,
            event_ack_timeout=5,
        )
        try:
            assert runtime.codex_config_profile == "qwen"
            child_config = runtime._child_config(1)
            assert child_config["codex_config_profile"] == "qwen"
            # The child contract carries only the validated selector. Config
            # contents, provider tokens, and source paths must stay child-local.
            assert "experimental_bearer_token" not in repr(child_config)
            assert "qwen.config.toml" not in repr(child_config)

            models = await runtime.list_models(include_hidden=False)
            assert [model["id"] for model in models] == ["qwen3.8-27b"]
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_two_argument_switch_receipt_uses_post_switch_agent(tmp_path):
    import asyncio
    from types import SimpleNamespace

    from src.channels.wechat import MVPCommandRouter, WeChatGateway
    from src.runtime.sqlite_store import SQLiteStore
    from wechat_ilink.types import (
        ITEM_TYPE_TEXT,
        MESSAGE_STATE_FINISH,
        MESSAGE_TYPE_USER,
        MessageItem,
        TextItem,
        WeixinMessage,
    )

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "profile-switch.sqlite")
        await store.initialize()
        selected_profiles: list[str | None] = []

        class Manager:
            async def get_active_agent(self, **scope):
                return await store.get_route(
                    channel=scope.get("channel", "wechat"),
                    bot_id=scope.get("bot_id", "bot"),
                    external_user_id=scope.get("external_user_id", "user"),
                    session_id=scope.get("session_id", "default"),
                )

            async def set_active_agent(
                self,
                agent_id: str,
                *,
                codex_config_profile: str | None = None,
                **scope,
            ):
                selected_profiles.append(codex_config_profile)
                await store.set_route(
                    channel=scope.get("channel", "wechat"),
                    bot_id=scope.get("bot_id", "bot"),
                    external_user_id=scope.get("external_user_id", "user"),
                    session_id=scope.get("session_id", "default"),
                    active_agent_id=agent_id,
                )
                return agent_id

            async def switch_back_inbox(self, **_scope):
                return []

        sent = []

        def send(request):
            sent.append(request)
            return SimpleNamespace(ret=0, errcode=0, errmsg="")

        client = SimpleNamespace(bot_id="bot", send_message=send)
        gateway = WeChatGateway(
            store,
            bot_id="bot",
            command_router=MVPCommandRouter(Manager()),
        )
        message = WeixinMessage(
            seq=701,
            message_id=701,
            from_user_id="user",
            to_user_id="bot",
            message_type=MESSAGE_TYPE_USER,
            message_state=MESSAGE_STATE_FINISH,
            context_token="profile-context",
            item_list=[
                MessageItem(
                    type=ITEM_TYPE_TEXT,
                    text_item=TextItem(text="/agent researcher qwen"),
                )
            ],
        )
        try:
            accepted = await gateway.handle_message(client, message)
            assert accepted is not None and not accepted.duplicate
            assert selected_profiles == ["qwen"]
            receipt = await store.get_command_receipt(
                accepted.response_delivery_id
            )
            assert receipt is not None
            assert receipt["response_agent_id"] == "researcher"
            assert len(sent) == 1
        finally:
            await store.close()

    asyncio.run(scenario())
