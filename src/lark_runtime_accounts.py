"""Loop-owned lifecycle registry for isolated Lark channel accounts.

Credential staging/publication and durable bot-profile registration deliberately
live outside this module.  The controller owns only the in-process adapter
composition and the incremental channel-account lock transferred to it by an
onboarding transaction.  This separation permits a strict two-phase sequence::

    prepared = await accounts.prepare(profile)  # one-shot identity verification
    await store.create_bot_profile(record)       # durable commit by the caller
    await accounts.activate(prepared)            # exact readiness, then ingress

If any later step fails, ``rollback(prepared)`` fences only that account before
releasing its exact incremental ownership handle.  Existing accounts are never
stopped or unlocked as a side effect of a peer's failed onboarding.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from src.channels.lark import (
    AccountState,
    LarkAccountSupervisor,
    LarkAttachmentPromoter,
    LarkBotProfile,
    LarkCliProcess,
    LarkDeliveryWorker,
    LarkGateway,
    LarkMediaDeliveryWorker,
    LarkProfileError,
    tighten_lark_cli_config_state,
    validate_private_config_directory,
)
from src.runtime.supervisor import (
    ChannelAccountOwnership,
    SupervisorOwnershipError,
)


logger = logging.getLogger(__name__)


class LarkRuntimeAccountError(RuntimeError):
    """A dynamic Lark account could not cross a safe lifecycle boundary."""


class LarkRuntimeAccountDuplicate(LarkRuntimeAccountError):
    """An app ID, profile ID, or isolated config directory is already tracked."""


class LarkRuntimeAccountStateError(LarkRuntimeAccountError):
    """A handle does not belong to this controller or is in the wrong phase."""


class LarkRuntimeAccountCleanupError(LarkRuntimeAccountError):
    """Account cleanup was not proven, so its ownership remains retained."""

    def __init__(self, message: str, *, errors: tuple[BaseException, ...] = ()) -> None:
        super().__init__(message)
        self.errors = errors


class LarkRuntimeAccountPhase(str, Enum):
    PREPARED = "prepared"
    ACTIVE = "active"
    STOPPING = "stopping"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(eq=False, slots=True)
class LarkRuntimeAccountHandle:
    """Exact controller-owned resources for one canonical Lark app."""

    profile: LarkBotProfile
    process: Any
    gateway: Any
    delivery_worker: Any
    media_worker: Any
    supervisor: Any
    ownership_handle: ChannelAccountOwnership | Any | None
    release_ownership: bool
    preflighted: bool = False
    phase: LarkRuntimeAccountPhase = LarkRuntimeAccountPhase.PREPARED
    task: asyncio.Task[Any] | None = None
    failure: BaseException | None = None
    status_generation_base: int = 0
    ready_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def app_id(self) -> str:
        return self.profile.app_id

    @property
    def config_dir(self) -> Path:
        return self.profile.config_dir


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class LarkRuntimeAccountController:
    """Construct, preflight, activate, and drain account-local Lark adapters.

    All public coroutine methods are bound to the first running asyncio loop
    that uses the controller.  Factories are injectable so tests and the
    composition root can share this lifecycle without opening a second SQLite
    connection or importing launcher-specific helpers here.
    """

    def __init__(
        self,
        store: Any,
        manager: Any,
        principal_resolver: Any,
        attachment_store: Any,
        ownership: Any,
        *,
        executable: str = "lark-cli",
        administrator: Callable[[Any], Any] | None = None,
        onboarding_service_factory: Callable[[LarkBotProfile], Any] | None = None,
        media_callbacks_factory: Callable[[Any, Any], Any],
        shell_cwd: str | Path | None = None,
        process_factory: Callable[..., Any] = LarkCliProcess,
        promoter_factory: Callable[..., Any] = LarkAttachmentPromoter,
        gateway_factory: Callable[..., Any] = LarkGateway,
        delivery_worker_factory: Callable[..., Any] = LarkDeliveryWorker,
        media_worker_factory: Callable[..., Any] = LarkMediaDeliveryWorker,
        supervisor_factory: Callable[..., Any] = LarkAccountSupervisor,
        profile_validator: Callable[..., Any] = validate_private_config_directory,
        profile_normalizer: Callable[..., Any] = tighten_lark_cli_config_state,
        status_callback_factory: Callable[[LarkBotProfile], Any] | None = None,
        activation_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
    ) -> None:
        self.store = store
        self.manager = manager
        self.principal_resolver = principal_resolver
        self.attachment_store = attachment_store
        self.ownership = ownership
        self.executable = str(executable or "lark-cli")
        self.administrator = administrator
        self.onboarding_service_factory = onboarding_service_factory
        self.media_callbacks_factory = media_callbacks_factory
        self.shell_cwd = Path(shell_cwd).resolve() if shell_cwd is not None else None
        self.process_factory = process_factory
        self.promoter_factory = promoter_factory
        self.gateway_factory = gateway_factory
        self.delivery_worker_factory = delivery_worker_factory
        self.media_worker_factory = media_worker_factory
        self.supervisor_factory = supervisor_factory
        self.profile_validator = profile_validator
        self.profile_normalizer = profile_normalizer
        self.status_callback_factory = status_callback_factory
        self.activation_timeout = max(0.1, float(activation_timeout))
        self.shutdown_timeout = max(0.1, float(shutdown_timeout))
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._handles_by_app: dict[str, LarkRuntimeAccountHandle] = {}
        self._handles_by_profile: dict[str, LarkRuntimeAccountHandle] = {}
        self._handles_by_config: dict[str, LarkRuntimeAccountHandle] = {}
        # A stopped handle may be deliberately detached while its caller rolls
        # credential publication back under the still-held account lock.  Keep
        # exact identity authority until that caller explicitly releases it.
        self._known_handles: set[LarkRuntimeAccountHandle] = set()
        self._closed = False

    def _bind_loop(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
            self._lock = asyncio.Lock()
        elif self._loop is not loop:
            raise LarkRuntimeAccountStateError(
                "Lark account controller cannot cross an asyncio loop boundary"
            )
        assert self._lock is not None
        return self._lock

    @staticmethod
    def _config_key(profile: LarkBotProfile) -> str:
        return str(profile.config_dir.resolve(strict=False))

    def _assert_unique(self, profile: LarkBotProfile) -> None:
        if profile.app_id in self._handles_by_app:
            raise LarkRuntimeAccountDuplicate(
                f"Lark app ID is already active: {profile.app_id}"
            )
        if profile.profile_id in self._handles_by_profile:
            raise LarkRuntimeAccountDuplicate(
                f"Lark profile ID is already active: {profile.profile_id}"
            )
        if self._config_key(profile) in self._handles_by_config:
            raise LarkRuntimeAccountDuplicate(
                "Lark accounts cannot share one CLI config directory"
            )

    def _ownership_covers(self, channel: str, bot_id: str) -> bool:
        checker = getattr(self.ownership, "owns_account", None)
        if callable(checker):
            return bool(checker(channel=channel, bot_id=bot_id))
        accounts = getattr(self.ownership, "accounts", ())
        if (channel, bot_id) in accounts:
            return True
        return (
            str(getattr(self.ownership, "channel", "")) == channel
            and str(getattr(self.ownership, "bot_id", "")) == bot_id
        )

    def _track(self, handle: LarkRuntimeAccountHandle) -> None:
        self._handles_by_app[handle.app_id] = handle
        self._handles_by_profile[handle.profile.profile_id] = handle
        self._handles_by_config[self._config_key(handle.profile)] = handle
        self._known_handles.add(handle)

    def _untrack(self, handle: LarkRuntimeAccountHandle) -> None:
        if self._handles_by_app.get(handle.app_id) is handle:
            del self._handles_by_app[handle.app_id]
        if self._handles_by_profile.get(handle.profile.profile_id) is handle:
            del self._handles_by_profile[handle.profile.profile_id]
        config_key = self._config_key(handle.profile)
        if self._handles_by_config.get(config_key) is handle:
            del self._handles_by_config[config_key]

    def _assert_tracked(self, handle: LarkRuntimeAccountHandle) -> None:
        if self._handles_by_app.get(handle.app_id) is not handle:
            raise LarkRuntimeAccountStateError(
                "Lark account handle does not belong to this controller"
            )

    async def _persist_status(
        self,
        handle: LarkRuntimeAccountHandle,
        state: AccountState | Any,
        generation: int,
        error: str,
    ) -> None:
        updater = getattr(self.store, "set_bot_profile_status", None)
        if updater is None:
            updater = getattr(self.store, "update_bot_profile_status", None)
        if updater is not None:
            state_value = str(getattr(state, "value", state) or "disconnected")
            values: dict[str, Any] = {
                "connection_state": state_value,
                "generation": handle.status_generation_base + int(generation),
                "last_error_code": "adapter_error" if error else None,
            }
            if state_value == AccountState.READY.value:
                values["last_ready_at"] = datetime.now(timezone.utc)
            await _maybe_await(updater(handle.profile.profile_id, **values))

    async def _generation_base(self, handle: LarkRuntimeAccountHandle) -> int:
        getter = getattr(self.store, "get_bot_profile_status", None)
        if getter is None:
            return 0
        stored = await _maybe_await(getter(handle.profile.profile_id))
        if isinstance(stored, Mapping):
            raw = stored.get("generation", 0)
        else:
            raw = getattr(stored, "generation", 0)
        try:
            return max(0, int(raw or 0))
        except (TypeError, ValueError):
            return 0

    async def _build_handle(
        self,
        profile: LarkBotProfile,
        *,
        ownership_handle: Any | None,
        release_ownership: bool,
        readiness_timeout: float,
    ) -> LarkRuntimeAccountHandle:
        process = self.process_factory(
            profile,
            executable=self.executable,
            ready_timeout=max(0.1, float(readiness_timeout)),
        )
        promoter = self.promoter_factory(
            profile,
            self.attachment_store,
            self.store,
            downloader=process.download_attachment,
        )
        onboarding_service = None
        if self.onboarding_service_factory is not None:
            onboarding_service = await _maybe_await(
                self.onboarding_service_factory(profile)
            )
        gateway = self.gateway_factory(
            self.manager,
            profile,
            principal_resolver=self.principal_resolver,
            administrator=self.administrator,
            onboarding_service=onboarding_service,
            shell_cwd=self.shell_cwd,
            attachment_promoter=promoter,
        )
        delivery_worker = self.delivery_worker_factory(self.store, profile, process)
        callbacks = await _maybe_await(
            self.media_callbacks_factory(self.attachment_store, process)
        )
        try:
            upload_media, send_media, send_bundle_text = callbacks
        except (TypeError, ValueError) as exc:
            raise LarkRuntimeAccountError(
                "Lark media callback factory must return exactly three callbacks"
            ) from exc
        media_worker = self.media_worker_factory(
            self.store,
            profile,
            uploader=upload_media,
            sender=send_media,
            text_sender=send_bundle_text,
        )

        handle_box: dict[str, LarkRuntimeAccountHandle] = {}
        external_status_callback = None
        if self.status_callback_factory is not None:
            external_status_callback = await _maybe_await(
                self.status_callback_factory(profile)
            )

        async def update_status(state: Any, generation: int, error: str) -> None:
            handle = handle_box["handle"]
            state_value = str(getattr(state, "value", state) or "")
            await self._persist_status(handle, state, generation, error)
            if external_status_callback is not None:
                await _maybe_await(
                    external_status_callback(state, generation, error)
                )
            if state_value == AccountState.READY.value:
                # Readiness is visible only after the supervisor's complete
                # status callback succeeds.  Otherwise a failed durable status
                # update could make activation return before workers started.
                handle.ready_event.set()

        supervisor = self.supervisor_factory(
            profile,
            gateway,
            process,
            delivery_worker=delivery_worker,
            media_worker=media_worker,
            status_callback=update_status,
        )
        handle = LarkRuntimeAccountHandle(
            profile=profile,
            process=process,
            gateway=gateway,
            delivery_worker=delivery_worker,
            media_worker=media_worker,
            supervisor=supervisor,
            ownership_handle=ownership_handle,
            release_ownership=release_ownership,
        )
        handle_box["handle"] = handle
        return handle

    async def prepare(
        self,
        profile_value: LarkBotProfile | Mapping[str, Any] | Any,
        *,
        acquire_ownership: bool = True,
        ownership_handle: ChannelAccountOwnership | Any | None = None,
        preflight: bool = True,
        readiness_timeout: float = 90.0,
    ) -> LarkRuntimeAccountHandle:
        """Reserve and preflight one account without exposing ingress.

        With ``acquire_ownership=False``, the profile must already be covered by
        the launcher's startup ownership set.  Passing ``ownership_handle``
        transfers rollback authority for that exact incremental handle to this
        controller; otherwise the controller acquires one itself.
        """

        lock = self._bind_loop()
        profile = LarkBotProfile.from_value(profile_value)
        if not profile.enabled:
            raise LarkRuntimeAccountStateError(
                "disabled Lark profiles cannot be activated at runtime"
            )
        if ownership_handle is not None and not acquire_ownership:
            raise LarkRuntimeAccountStateError(
                "an ownership handle cannot be combined with acquire_ownership=False"
            )

        async with lock:
            if self._closed:
                raise LarkRuntimeAccountStateError(
                    "Lark account controller is shutting down"
                )
            self._assert_unique(profile)

            # Constructors and injected factories are required to be side-effect
            # free with respect to ingress.  Build them before acquiring a new
            # account lock so a constructor error cannot strand that lock.
            handle = await self._build_handle(
                profile,
                ownership_handle=ownership_handle,
                release_ownership=(ownership_handle is not None),
                readiness_timeout=readiness_timeout,
            )
            if ownership_handle is None and acquire_ownership:
                handle.ownership_handle = self.ownership.acquire_account(
                    channel="lark",
                    bot_id=profile.app_id,
                )
                handle.release_ownership = True
            elif ownership_handle is not None:
                if (
                    not bool(getattr(ownership_handle, "held", False))
                    or str(getattr(ownership_handle, "channel", "")) != "lark"
                    or str(getattr(ownership_handle, "bot_id", ""))
                    != profile.app_id
                ):
                    raise LarkRuntimeAccountStateError(
                        "incremental ownership handle does not match the Lark profile"
                    )
            else:
                if not bool(getattr(self.ownership, "held", False)):
                    raise SupervisorOwnershipError(
                        "startup account registration requires held supervisor ownership"
                    )
                if not self._ownership_covers("lark", profile.app_id):
                    raise SupervisorOwnershipError(
                        "startup ownership does not cover this Lark account"
                    )

            self._track(handle)
            try:
                if preflight:
                    await self._preflight_resources(
                        handle,
                        timeout=readiness_timeout,
                    )
            except BaseException as exc:
                handle.failure = exc
                handle.phase = LarkRuntimeAccountPhase.FAILED
                try:
                    await self._rollback_locked(handle)
                except BaseException as cleanup_exc:
                    raise LarkRuntimeAccountCleanupError(
                        "Lark account preflight failed and cleanup was not proven",
                        errors=(exc, cleanup_exc),
                    ) from exc
                raise
            return handle

    async def _preflight_resources(
        self,
        handle: LarkRuntimeAccountHandle,
        *,
        timeout: float,
    ) -> None:
        profile = handle.profile
        config_identity = self.profile_validator(
            profile.config_dir,
            expected_app_id=profile.app_id,
        )
        try:
            await asyncio.wait_for(
                handle.process.verify_version(),
                timeout=max(0.1, float(timeout)),
            )
            await asyncio.wait_for(
                handle.process.verify_profile(),
                timeout=max(0.1, float(timeout)),
            )
        finally:
            # One-shot verification may explicitly create public cache modes.
            # This boundary is quiescent: no long-lived consumer is running.
            await _maybe_await(
                self.profile_normalizer(
                    profile.config_dir,
                    expected_app_id=profile.app_id,
                    expected_identity=config_identity,
                )
            )
            verified_identity = self.profile_validator(
                profile.config_dir,
                expected_app_id=profile.app_id,
            )
            if verified_identity != config_identity:
                raise LarkProfileError(
                    "Lark CLI config directory changed during verification"
                )
        handle.preflighted = True

    async def preflight(
        self,
        handle: LarkRuntimeAccountHandle,
        *,
        timeout: float = 90.0,
    ) -> LarkRuntimeAccountHandle:
        """Verify a prepared account without starting its event consumer.

        Failure stops any one-shot process resources but deliberately retains
        the tracked handle and its exact account lock.  Credential onboarding
        can therefore compensate its published tree before explicitly
        releasing ownership.
        """

        lock = self._bind_loop()
        async with handle.operation_lock:
            async with lock:
                self._assert_tracked(handle)
                if handle.phase is not LarkRuntimeAccountPhase.PREPARED:
                    raise LarkRuntimeAccountStateError(
                        "only a prepared Lark account can be preflighted"
                    )
                if handle.task is not None:
                    raise LarkRuntimeAccountStateError(
                        "active Lark accounts cannot be preflighted"
                    )
            try:
                await self._preflight_resources(handle, timeout=timeout)
            except BaseException as exc:
                handle.failure = exc
                handle.phase = LarkRuntimeAccountPhase.FAILED
                try:
                    await handle.process.stop()
                except BaseException as cleanup_exc:
                    raise LarkRuntimeAccountCleanupError(
                        "Lark account preflight failed and cleanup was not proven",
                        errors=(exc, cleanup_exc),
                    ) from exc
                raise
            return handle

    def _task_finished(
        self,
        handle: LarkRuntimeAccountHandle,
        task: asyncio.Task[Any],
    ) -> None:
        try:
            failure = task.exception()
        except asyncio.CancelledError as exc:
            failure = exc
        if (
            failure is None
            and handle.phase is LarkRuntimeAccountPhase.ACTIVE
        ):
            failure = LarkRuntimeAccountError(
                "Lark account supervisor exited after activation"
            )
        if failure is not None:
            handle.failure = failure
            if handle.phase is LarkRuntimeAccountPhase.ACTIVE:
                handle.phase = LarkRuntimeAccountPhase.FAILED
            logger.error(
                "Lark account supervisor failed for profile %s (%s)",
                handle.profile.profile_id,
                type(failure).__name__,
            )

    async def activate(
        self,
        handle: LarkRuntimeAccountHandle,
        *,
        timeout: float | None = None,
    ) -> LarkRuntimeAccountHandle:
        """Launch ingress/delivery only after the caller's durable commit."""

        lock = self._bind_loop()
        async with handle.operation_lock:
            async with lock:
                self._assert_tracked(handle)
                if self._closed:
                    raise LarkRuntimeAccountStateError(
                        "Lark account controller is shutting down"
                    )
                if handle.phase is not LarkRuntimeAccountPhase.PREPARED:
                    raise LarkRuntimeAccountStateError(
                        "only a prepared Lark account can be activated"
                    )
                if not handle.preflighted:
                    raise LarkRuntimeAccountStateError(
                        "Lark account must pass preflight before activation"
                    )
                handle.status_generation_base = await self._generation_base(handle)
                handle.task = asyncio.create_task(
                    handle.supervisor.run(),
                    name=f"lark-account:{handle.profile.profile_id}",
                )
                handle.task.add_done_callback(
                    lambda task, owned=handle: self._task_finished(owned, task)
                )

            readiness = getattr(handle.supervisor, "wait_until_ready", None)
            ready_waiter = asyncio.create_task(
                readiness(
                    self.activation_timeout
                    if timeout is None
                    else max(0.1, float(timeout))
                )
                if callable(readiness)
                else handle.ready_event.wait()
            )
            try:
                wait_timeout = (
                    self.activation_timeout
                    if timeout is None
                    else max(0.1, float(timeout))
                )
                done, _pending = await asyncio.wait(
                    (ready_waiter, handle.task),
                    # The production supervisor applies this timeout inside
                    # ``wait_until_ready``; keep the outer fence for injected
                    # compatibility supervisors used by tests/deployments.
                    timeout=wait_timeout + 0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if handle.task in done:
                    # Readiness and task completion can become observable in
                    # the same loop turn.  A long-lived account supervisor
                    # that has already exited is never active, even if it set
                    # its ready marker immediately before exiting.
                    await handle.task
                    raise LarkRuntimeAccountError(
                        "Lark account supervisor exited before activation completed"
                    )
                if ready_waiter in done:
                    await ready_waiter
                    handle.phase = LarkRuntimeAccountPhase.ACTIVE
                    return handle
                raise LarkRuntimeAccountError(
                    "Lark account supervisor did not report readiness"
                )
            except BaseException as exc:
                handle.failure = exc
                handle.phase = LarkRuntimeAccountPhase.FAILED
                try:
                    await self._stop_resources(handle)
                except BaseException as cleanup_exc:
                    raise LarkRuntimeAccountCleanupError(
                        "Lark account activation failed and cleanup was not proven",
                        errors=(exc, cleanup_exc),
                    ) from exc
                raise
            finally:
                if not ready_waiter.done():
                    ready_waiter.cancel()
                await asyncio.gather(ready_waiter, return_exceptions=True)

    async def add(
        self,
        profile_value: LarkBotProfile | Mapping[str, Any] | Any,
        *,
        acquire_ownership: bool = True,
        ownership_handle: ChannelAccountOwnership | Any | None = None,
        preflight: bool = True,
        readiness_timeout: float = 90.0,
        activation_timeout: float | None = None,
    ) -> LarkRuntimeAccountHandle:
        """Convenience path for already-durable startup/external profiles."""

        handle = await self.prepare(
            profile_value,
            acquire_ownership=acquire_ownership,
            ownership_handle=ownership_handle,
            preflight=preflight,
            readiness_timeout=readiness_timeout,
        )
        try:
            return await self.activate(handle, timeout=activation_timeout)
        except BaseException:
            await self.rollback(handle)
            raise

    async def _stop_resources(self, handle: LarkRuntimeAccountHandle) -> None:
        if handle.phase is not LarkRuntimeAccountPhase.FAILED:
            handle.phase = LarkRuntimeAccountPhase.STOPPING
        handle.supervisor.stop()
        if handle.task is not None:
            task = handle.task
            try:
                await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=self.shutdown_timeout,
                )
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            except asyncio.CancelledError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
            handle.task = None
        else:
            await handle.process.stop()

    async def _rollback_locked(
        self,
        handle: LarkRuntimeAccountHandle,
        *,
        release_ownership: bool = True,
    ) -> None:
        await self._stop_resources(handle)
        if (
            release_ownership
            and handle.release_ownership
            and handle.ownership_handle is not None
        ):
            self.ownership.release_account(handle.ownership_handle)
            handle.ownership_handle = None
            handle.release_ownership = False
        self._untrack(handle)
        handle.phase = LarkRuntimeAccountPhase.STOPPED
        if not handle.release_ownership:
            self._known_handles.discard(handle)

    async def rollback(
        self,
        handle: LarkRuntimeAccountHandle,
        *,
        release_ownership: bool = True,
    ) -> Any | None:
        """Fence and remove one account without disturbing its peers.

        ``release_ownership=False`` is the credential-transaction boundary:
        resources are proven stopped and the account is untracked, while the
        exact incremental account lock remains held.  The caller can then
        restore/remove the credential tree and invoke :meth:`release_ownership`.
        """

        lock = self._bind_loop()
        async with handle.operation_lock:
            async with lock:
                self._assert_tracked(handle)
            try:
                await self._rollback_locked(
                    handle,
                    release_ownership=release_ownership,
                )
            except BaseException as exc:
                handle.failure = exc
                handle.phase = LarkRuntimeAccountPhase.FAILED
                raise LarkRuntimeAccountCleanupError(
                    "Lark account cleanup was not proven; ownership retained",
                    errors=(exc,),
                ) from exc
            return handle.ownership_handle

    async def release_ownership(self, handle: LarkRuntimeAccountHandle) -> None:
        """Release a detached account's exact post-credential-rollback lock."""

        lock = self._bind_loop()
        async with handle.operation_lock:
            async with lock:
                if handle not in self._known_handles:
                    raise LarkRuntimeAccountStateError(
                        "Lark account handle does not belong to this controller"
                    )
                if handle.phase is not LarkRuntimeAccountPhase.STOPPED:
                    raise LarkRuntimeAccountStateError(
                        "Lark account resources must be stopped before ownership release"
                    )
                if (
                    not handle.release_ownership
                    or handle.ownership_handle is None
                ):
                    raise LarkRuntimeAccountStateError(
                        "Lark account handle has no releasable ownership"
                    )
                try:
                    self.ownership.release_account(handle.ownership_handle)
                except BaseException as exc:
                    handle.failure = exc
                    raise LarkRuntimeAccountCleanupError(
                        "Lark account ownership release was not proven",
                        errors=(exc,),
                    ) from exc
                handle.ownership_handle = None
                handle.release_ownership = False
                self._known_handles.discard(handle)

    async def stop_all(self) -> None:
        """Stop every tracked account, attempting all peers before reporting."""

        lock = self._bind_loop()
        async with lock:
            self._closed = True
            handles = tuple(self._handles_by_app.values())
            detached = tuple(
                handle
                for handle in self._known_handles
                if handle not in handles
                and handle.phase is LarkRuntimeAccountPhase.STOPPED
                and handle.release_ownership
            )
            for handle in handles:
                handle.supervisor.stop()

        results = await asyncio.gather(
            *(self.rollback(handle) for handle in handles),
            return_exceptions=True,
        )
        errors: tuple[BaseException, ...] = tuple(
            result for result in results if isinstance(result, BaseException)
        )
        if detached:
            # A detached handle is intentionally fencing credential rollback.
            # Releasing it here would invert the required shutdown order and
            # permit another process to use credentials while their owning
            # transaction is still active.  The onboarding service must finish
            # that transaction and explicitly release each handle first.
            errors += (
                LarkRuntimeAccountStateError(
                    "credential rollback still owns a detached Lark account lock"
                ),
            )
        if errors:
            raise LarkRuntimeAccountCleanupError(
                "one or more Lark accounts could not be stopped safely",
                errors=errors,
            )

    async def handles(self) -> tuple[LarkRuntimeAccountHandle, ...]:
        """Return a stable in-loop snapshot of tracked account handles."""

        lock = self._bind_loop()
        async with lock:
            return tuple(self._handles_by_app.values())


__all__ = [
    "LarkRuntimeAccountCleanupError",
    "LarkRuntimeAccountController",
    "LarkRuntimeAccountDuplicate",
    "LarkRuntimeAccountError",
    "LarkRuntimeAccountHandle",
    "LarkRuntimeAccountPhase",
    "LarkRuntimeAccountStateError",
]
