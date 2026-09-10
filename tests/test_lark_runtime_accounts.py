"""Isolated tests for live Lark account composition and rollback."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.channels.lark import (
    AccountState,
    LarkBotProfile,
    tighten_lark_cli_config_state,
    validate_private_config_directory,
)
from src.lark_runtime_accounts import (
    LarkRuntimeAccountCleanupError,
    LarkRuntimeAccountController,
    LarkRuntimeAccountDuplicate,
    LarkRuntimeAccountError,
    LarkRuntimeAccountPhase,
    LarkRuntimeAccountStateError,
)
from src.runtime.supervisor import SupervisorOwnershipError


def _profile(tmp_path: Path, name: str, app_id: str) -> LarkBotProfile:
    return LarkBotProfile(
        profile_id=name,
        app_id=app_id,
        config_dir=(tmp_path / name).resolve(),
        brand="feishu",
        bot_open_id=f"ou_{name}_bot",
        cli_version="1.0.92",
    )


class _Ownership:
    def __init__(self, *, startup_accounts=()) -> None:
        self.held = True
        self.startup_accounts = set(startup_accounts)
        self.dynamic: dict[tuple[str, str], object] = {}
        self.acquired: list[tuple[str, str]] = []
        self.released: list[object] = []

    def owns_account(self, *, channel: str, bot_id: str) -> bool:
        account = (channel, bot_id)
        return self.held and (
            account in self.startup_accounts or account in self.dynamic
        )

    def acquire_account(self, *, channel: str, bot_id: str):
        account = (channel, bot_id)
        if account in self.startup_accounts or account in self.dynamic:
            raise SupervisorOwnershipError("already owned")
        handle = SimpleNamespace(channel=channel, bot_id=bot_id, held=True)
        self.dynamic[account] = handle
        self.acquired.append(account)
        return handle

    def release_account(self, handle) -> None:
        account = next(
            (key for key, value in self.dynamic.items() if value is handle),
            None,
        )
        if account is None:
            raise SupervisorOwnershipError("foreign handle")
        del self.dynamic[account]
        handle.held = False
        self.released.append(handle)


class _Store:
    def __init__(self) -> None:
        self.status: list[tuple[str, dict[str, object]]] = []
        self.generations: dict[str, int] = {}

    async def get_bot_profile_status(self, profile_id: str):
        return SimpleNamespace(generation=self.generations.get(profile_id, 0))

    async def set_bot_profile_status(self, profile_id: str, **values) -> None:
        self.status.append((profile_id, values))


class _Process:
    def __init__(self, profile, state, **_kwargs) -> None:
        self.profile = profile
        self.state = state
        self.running = False
        self.generation = 0
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> int:
        self.start_calls += 1
        if self.running:
            self.state["order"].append(f"reuse:{self.profile.app_id}")
            return self.generation
        self.state["order"].append(f"consumer-start:{self.profile.app_id}")
        self.running = True
        self.generation += 1
        return self.generation

    async def verify_version(self) -> str:
        self.state["order"].append(f"verify-version:{self.profile.app_id}")
        return "1.0.92"

    async def verify_profile(self):
        self.state["order"].append(f"verify-profile:{self.profile.app_id}")
        if self.profile.app_id in self.state["create_insecure_preflight_state"]:
            cache = self.profile.config_dir / "cache"
            cache.mkdir(mode=0o700, exist_ok=True)
            remote_meta = cache / "remote_meta.meta.json"
            remote_meta.write_text("{}", encoding="utf-8")
            # Match lark-cli 1.0.92, which explicitly overrides the child
            # umask for this cache artifact.
            remote_meta.chmod(0o644)
        if self.profile.app_id in self.state["fail_start"]:
            raise RuntimeError("preflight failed")
        return {"app_id": self.profile.app_id}

    async def stop(self) -> None:
        self.stop_calls += 1
        self.state["order"].append(f"process-stop:{self.profile.app_id}")
        self.running = False
        if self.profile.app_id in self.state["fail_stop"]:
            raise RuntimeError("process stop failed")

    async def download_attachment(self, *_args, **_kwargs) -> bytes:
        return b"attachment"


class _Worker:
    def __init__(self, kind: str, profile, state) -> None:
        self.kind = kind
        self.profile = profile
        self.state = state
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True
        self.state["order"].append(f"{self.kind}-stop:{self.profile.app_id}")


class _Supervisor:
    def __init__(
        self,
        profile,
        gateway,
        process,
        *,
        delivery_worker,
        media_worker,
        status_callback,
    ) -> None:
        self.profile = profile
        self.gateway = gateway
        self.process = process
        self.delivery_worker = delivery_worker
        self.media_worker = media_worker
        self.status_callback = status_callback
        self.state = AccountState.DISCONNECTED
        self.generation = 0
        self.stop_event = asyncio.Event()

    def stop(self) -> None:
        self.stop_event.set()

    async def run(self) -> None:
        self.state = AccountState.STARTING
        await self.status_callback(self.state, self.generation, "")
        self.generation = await self.process.start()
        self.state = AccountState.READY
        await self.status_callback(self.state, self.generation, "")
        await self.stop_event.wait()
        await self.process.stop()
        self.delivery_worker.stop()
        self.media_worker.stop()
        self.state = AccountState.DISCONNECTED
        await self.status_callback(self.state, self.generation, "")


def _controller(
    tmp_path: Path,
    *,
    ownership: _Ownership | None = None,
    fail_start=(),
    fail_stop=(),
    create_insecure_preflight_state=(),
    profile_validator=None,
    profile_normalizer=None,
    supervisor_factory=_Supervisor,
):
    state = {
        "order": [],
        "fail_start": set(fail_start),
        "fail_stop": set(fail_stop),
        "create_insecure_preflight_state": set(create_insecure_preflight_state),
        "gateways": [],
        "services": [],
        "processes": {},
    }
    store = _Store()
    ownership = ownership or _Ownership()

    def process_factory(profile, **kwargs):
        process = _Process(profile, state, **kwargs)
        state["processes"][profile.app_id] = process
        return process

    def promoter_factory(profile, attachment_store, metadata_store, **kwargs):
        return SimpleNamespace(
            profile=profile,
            attachment_store=attachment_store,
            metadata_store=metadata_store,
            downloader=kwargs["downloader"],
        )

    async def onboarding_service_factory(profile):
        service = SimpleNamespace(app_id=profile.app_id)
        state["services"].append(service)
        return service

    def gateway_factory(manager, profile, **kwargs):
        gateway = SimpleNamespace(manager=manager, profile=profile, **kwargs)
        state["gateways"].append(gateway)
        return gateway

    def delivery_factory(_store, profile, _process):
        return _Worker("text", profile, state)

    def media_factory(_store, profile, **_kwargs):
        return _Worker("media", profile, state)

    def media_callbacks(_attachments, process):
        return (
            lambda row: (process.profile.app_id, "upload", row),
            lambda row, upload: (process.profile.app_id, "send", row, upload),
            lambda row: (process.profile.app_id, "text", row),
        )

    controller = LarkRuntimeAccountController(
        store,
        manager=SimpleNamespace(name="manager"),
        principal_resolver=SimpleNamespace(name="resolver"),
        attachment_store=SimpleNamespace(root=tmp_path),
        ownership=ownership,
        executable="lark-cli-test",
        administrator=lambda _envelope: True,
        onboarding_service_factory=onboarding_service_factory,
        media_callbacks_factory=media_callbacks,
        shell_cwd=tmp_path,
        process_factory=process_factory,
        promoter_factory=promoter_factory,
        gateway_factory=gateway_factory,
        delivery_worker_factory=delivery_factory,
        media_worker_factory=media_factory,
        supervisor_factory=supervisor_factory,
        profile_validator=(
            profile_validator
            if profile_validator is not None
            else lambda *_args, **_kwargs: None
        ),
        profile_normalizer=(
            profile_normalizer
            if profile_normalizer is not None
            else (
                tighten_lark_cli_config_state
                if profile_validator is not None
                else lambda *_args, **_kwargs: None
            )
        ),
        activation_timeout=0.5,
        shutdown_timeout=0.5,
    )
    return controller, ownership, store, state


class _ReadyThenExitSupervisor(_Supervisor):
    async def run(self) -> None:
        self.state = AccountState.STARTING
        await self.status_callback(self.state, self.generation, "")
        self.generation = await self.process.start()
        self.state = AccountState.READY
        await self.status_callback(self.state, self.generation, "")
        await self.process.stop()


def test_prepare_then_activate_keeps_ingress_behind_durable_boundary(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, store, state = _controller(tmp_path)
        profile = _profile(tmp_path, "first", "cli_runtime_first")

        handle = await controller.prepare(profile)
        process = state["processes"][profile.app_id]
        assert handle.phase is LarkRuntimeAccountPhase.PREPARED
        assert handle.preflighted is True
        assert process.running is False
        assert process.start_calls == 0
        assert handle.task is None
        assert store.status == []
        assert ownership.acquired == [("lark", profile.app_id)]
        assert state["order"] == [
            f"verify-version:{profile.app_id}",
            f"verify-profile:{profile.app_id}",
        ]

        # This point represents the caller's durable bot-profile commit.
        store.generations[profile.profile_id] = 7
        activated = await controller.activate(handle)
        assert activated is handle
        assert handle.phase is LarkRuntimeAccountPhase.ACTIVE
        assert handle.task is not None and not handle.task.done()
        assert process.start_calls == 1
        assert state["order"][:3] == [
            f"verify-version:{profile.app_id}",
            f"verify-profile:{profile.app_id}",
            f"consumer-start:{profile.app_id}",
        ]
        assert state["gateways"][0].onboarding_service.app_id == profile.app_id
        ready = [values for _profile_id, values in store.status]
        assert ready[-1]["connection_state"] == "ready"
        assert ready[-1]["generation"] == 8

        await controller.rollback(handle)
        assert handle.phase is LarkRuntimeAccountPhase.STOPPED
        assert process.running is False
        assert ownership.dynamic == {}
        assert len(ownership.released) == 1
        assert await controller.handles() == ()

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="owner-only modes are POSIX-specific",
)
def test_dynamic_preflight_normalizes_cli_remote_meta_created_as_0644(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        profile = _profile(tmp_path, "private", "cli_runtime_private")
        profile.config_dir.mkdir(mode=0o700)
        controller, ownership, _store, _state = _controller(
            tmp_path,
            create_insecure_preflight_state=(profile.app_id,),
            profile_validator=validate_private_config_directory,
        )

        handle = await controller.prepare(profile)

        remote_meta = profile.config_dir / "cache" / "remote_meta.meta.json"
        assert handle.preflighted is True
        assert remote_meta.stat().st_mode & 0o777 == 0o600
        validate_private_config_directory(
            profile.config_dir,
            expected_app_id=profile.app_id,
        )
        await controller.rollback(handle)
        assert ownership.dynamic == {}

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.name != "posix",
    reason="owner-only modes are POSIX-specific",
)
def test_failed_dynamic_preflight_does_not_leave_insecure_cli_state(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        profile = _profile(
            tmp_path,
            "failed-private",
            "cli_runtime_failed_private",
        )
        profile.config_dir.mkdir(mode=0o700)
        controller, ownership, _store, _state = _controller(
            tmp_path,
            fail_start=(profile.app_id,),
            create_insecure_preflight_state=(profile.app_id,),
            profile_validator=validate_private_config_directory,
        )

        with pytest.raises(RuntimeError, match="preflight failed"):
            await controller.prepare(profile)

        remote_meta = profile.config_dir / "cache" / "remote_meta.meta.json"
        assert remote_meta.stat().st_mode & 0o777 == 0o600
        validate_private_config_directory(
            profile.config_dir,
            expected_app_id=profile.app_id,
        )
        assert ownership.dynamic == {}

    asyncio.run(scenario())


def test_activate_rejects_handle_that_has_not_passed_preflight(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, store, state = _controller(tmp_path)
        profile = _profile(tmp_path, "unverified", "cli_runtime_unverified")

        handle = await controller.prepare(profile, preflight=False)
        process = state["processes"][profile.app_id]
        assert handle.preflighted is False

        with pytest.raises(
            LarkRuntimeAccountStateError,
            match="must pass preflight",
        ):
            await controller.activate(handle)

        assert handle.phase is LarkRuntimeAccountPhase.PREPARED
        assert handle.task is None
        assert process.start_calls == 0
        assert store.status == []
        assert ownership.owns_account(channel="lark", bot_id=profile.app_id)
        await controller.rollback(handle)

    asyncio.run(scenario())


def test_activate_rejects_supervisor_that_exits_with_ready_marker(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, _store, state = _controller(
            tmp_path,
            supervisor_factory=_ReadyThenExitSupervisor,
        )
        profile = _profile(tmp_path, "brief", "cli_runtime_brief")
        handle = await controller.prepare(profile)

        with pytest.raises(
            LarkRuntimeAccountError,
            match="exited before activation completed",
        ):
            await controller.activate(handle)

        assert handle.phase is LarkRuntimeAccountPhase.FAILED
        assert state["processes"][profile.app_id].running is False
        assert ownership.owns_account(channel="lark", bot_id=profile.app_id)
        await controller.rollback(handle)
        assert ownership.dynamic == {}

    asyncio.run(scenario())


def test_duplicate_dimensions_are_rejected_without_touching_live_peer(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, _store, state = _controller(tmp_path)
        first = _profile(tmp_path, "first", "cli_runtime_first")
        peer = await controller.add(first)
        first_task = peer.task

        duplicates = (
            _profile(tmp_path, "other", first.app_id),
            _profile(tmp_path, first.profile_id, "cli_runtime_second"),
            LarkBotProfile(
                profile_id="third",
                app_id="cli_runtime_third",
                config_dir=first.config_dir,
                brand="feishu",
            ),
        )
        for duplicate in duplicates:
            with pytest.raises(LarkRuntimeAccountDuplicate):
                await controller.prepare(duplicate)

        assert peer.phase is LarkRuntimeAccountPhase.ACTIVE
        assert first_task is not None and not first_task.done()
        assert state["processes"][first.app_id].running
        assert ownership.acquired == [("lark", first.app_id)]
        await controller.stop_all()

    asyncio.run(scenario())


def test_failed_new_account_preflight_rolls_back_only_its_exact_lock(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        failed_app = "cli_runtime_failed"
        controller, ownership, _store, state = _controller(
            tmp_path,
            fail_start=(failed_app,),
        )
        peer = await controller.add(
            _profile(tmp_path, "peer", "cli_runtime_peer")
        )
        peer_task = peer.task

        with pytest.raises(RuntimeError, match="preflight failed"):
            await controller.prepare(_profile(tmp_path, "failed", failed_app))

        assert peer.phase is LarkRuntimeAccountPhase.ACTIVE
        assert peer_task is not None and not peer_task.done()
        assert state["processes"][peer.app_id].running
        assert set(ownership.dynamic) == {("lark", peer.app_id)}
        assert [(item.channel, item.bot_id) for item in ownership.released] == [
            ("lark", failed_app)
        ]
        assert [handle.app_id for handle in await controller.handles()] == [
            peer.app_id
        ]
        await controller.stop_all()

    asyncio.run(scenario())


def test_startup_account_uses_existing_lock_and_never_releases_it(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        profile = _profile(tmp_path, "startup", "cli_runtime_startup")
        ownership = _Ownership(startup_accounts=(("lark", profile.app_id),))
        controller, ownership, _store, _state = _controller(
            tmp_path,
            ownership=ownership,
        )
        handle = await controller.add(profile, acquire_ownership=False)
        assert handle.release_ownership is False
        assert ownership.acquired == []
        await controller.rollback(handle)
        assert ownership.released == []
        assert ownership.owns_account(channel="lark", bot_id=profile.app_id)

        missing = _profile(tmp_path, "missing", "cli_runtime_missing")
        with pytest.raises(SupervisorOwnershipError, match="does not cover"):
            await controller.prepare(missing, acquire_ownership=False)

    asyncio.run(scenario())


def test_rollback_of_one_active_account_does_not_stop_peer(tmp_path: Path) -> None:
    async def scenario() -> None:
        controller, ownership, _store, state = _controller(tmp_path)
        first = await controller.add(
            _profile(tmp_path, "first", "cli_runtime_first")
        )
        second = await controller.add(
            _profile(tmp_path, "second", "cli_runtime_second")
        )
        first_task = first.task

        await controller.rollback(second)
        assert first.phase is LarkRuntimeAccountPhase.ACTIVE
        assert first_task is not None and not first_task.done()
        assert state["processes"][first.app_id].running
        assert set(ownership.dynamic) == {("lark", first.app_id)}
        assert [handle.app_id for handle in await controller.handles()] == [
            first.app_id
        ]
        await controller.stop_all()

    asyncio.run(scenario())


def test_credential_rollback_can_keep_exact_lock_until_explicit_release(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, _store, state = _controller(tmp_path)
        handle = await controller.add(
            _profile(tmp_path, "ordered", "cli_runtime_ordered")
        )
        original_release = ownership.release_account

        def release_account(token) -> None:
            state["order"].append("account-lock-release")
            original_release(token)

        ownership.release_account = release_account
        token = await controller.rollback(handle, release_ownership=False)
        assert token is handle.ownership_handle
        assert token.held is True
        assert handle.phase is LarkRuntimeAccountPhase.STOPPED
        assert await controller.handles() == ()
        assert "process-stop:cli_runtime_ordered" in state["order"]
        assert "account-lock-release" not in state["order"]

        # The orchestration layer restores/removes the credential publication
        # here, while no process can use it and the exact account remains fenced.
        state["order"].append("credential-rollback")
        await controller.release_ownership(handle)
        assert token.held is False
        assert state["order"].index("process-stop:cli_runtime_ordered") < (
            state["order"].index("credential-rollback")
        )
        assert state["order"].index("credential-rollback") < (
            state["order"].index("account-lock-release")
        )

    asyncio.run(scenario())


def test_stop_all_retains_detached_credential_transaction_lock(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        controller, ownership, _store, _state = _controller(tmp_path)
        handle = await controller.add(
            _profile(tmp_path, "detached", "cli_runtime_detached")
        )
        token = await controller.rollback(handle, release_ownership=False)

        with pytest.raises(LarkRuntimeAccountCleanupError) as raised:
            await controller.stop_all()
        assert any(
            "credential rollback" in str(error)
            for error in raised.value.errors
        )
        assert token.held is True
        assert set(ownership.dynamic) == {("lark", handle.app_id)}

        # Shutdown does not broaden authority: only the credential transaction
        # holder's explicit completion releases this exact account lock.
        await controller.release_ownership(handle)
        assert token.held is False
        assert ownership.dynamic == {}
        await controller.stop_all()

    asyncio.run(scenario())


def test_stop_all_attempts_every_peer_and_retains_unproven_account_lock(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        failed_app = "cli_runtime_failed"
        controller, ownership, _store, _state = _controller(
            tmp_path,
            fail_stop=(failed_app,),
        )
        healthy = await controller.add(
            _profile(tmp_path, "healthy", "cli_runtime_healthy")
        )
        failed = await controller.add(
            _profile(tmp_path, "failed", failed_app)
        )

        with pytest.raises(LarkRuntimeAccountCleanupError) as raised:
            await controller.stop_all()
        assert raised.value.errors
        assert healthy.phase is LarkRuntimeAccountPhase.STOPPED
        assert failed.phase is LarkRuntimeAccountPhase.FAILED
        assert set(ownership.dynamic) == {("lark", failed_app)}
        assert [handle.app_id for handle in await controller.handles()] == [
            failed_app
        ]

    asyncio.run(scenario())
