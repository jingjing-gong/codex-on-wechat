"""Live-safe QR staging primitives for Lark/Feishu bot onboarding.

The running gateway owns SQLite and its current channel-account locks.  This
module therefore limits itself to the credential filesystem transaction and
the reviewed, version-pinned ``lark-cli config init --new`` protocol.  A
runtime caller must acquire the newly discovered account lock, perform durable
duplicate checks, publish/register the returned profile, and then commit the
session in that order.

No child output is exposed here.  The sole streaming callback receives one
validated official verification URL, byte-for-byte as emitted by the CLI.
"""

from __future__ import annotations

import asyncio
import inspect
import io
import math
import os
import re
import stat
import tempfile
import threading
from collections.abc import Awaitable, Callable
from enum import Enum
from pathlib import Path

from src.lark_cli import (
    DEFAULT_ONBOARD_TIMEOUT_SECONDS,
    LarkProfileError,
    ProvisionedConfig,
    _config_for_profile,
    _contained_child,
    _ensure_private_directory,
    _safe_remove_tree,
    _tighten_owned_staging_tree,
    _unparseable_staging_is_discardable,
    _validate_private_tree,
    build_lark_profile_record,
    cleanup_unowned_provisioned_config,
    inspect_app_owner_identity,
    inspect_bot_identity,
    inspect_provisioned_config,
    normalize_profile_name,
    resolve_lark_cli_binary,
    run_config_init,
    secure_and_reinspect_provisioned_config,
    verify_lark_cli_version,
)
from src.runtime.models import BotProfileRecord
from src.runtime.supervisor import CredentialMutationOwnership


VerificationUrlCallback = Callable[[str], Awaitable[None] | None]

# ``user_code`` is the reviewed config-init contract.  Restricting both the
# origin and query grammar prevents a compromised/unexpected child diagnostic
# from becoming a URL-forwarding primitive.  The matched substring itself is
# returned unchanged; it is never parsed, decoded, encoded, or reconstructed.
_OFFICIAL_VERIFICATION_URL = re.compile(
    r"https://(?:open\.feishu\.cn|open\.larksuite\.com)/page/cli\?"
    r"user_code=[A-Za-z0-9%._~+\-]+"
    r"(?:&[A-Za-z0-9._~\-]+=[A-Za-z0-9%._~+\-]*)*"
)
_GENERIC_ONBOARDING_FAILURE = "Lark bot onboarding failed"
_RETAINED_ONBOARDING_FAILURE = (
    "Lark bot onboarding did not complete safely; private staged credentials "
    "were retained for owner recovery"
)


class LarkOnboardingRetainedError(LarkProfileError):
    """Onboarding failed while a private credential recovery handle remains."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        super().__init__(_RETAINED_ONBOARDING_FAILURE)


def public_onboarding_error(
    exc: BaseException,
    *,
    maximum: int = 512,
) -> str:
    """Return one bounded chat-safe onboarding failure description.

    Arbitrary exceptions and arbitrary subprocess diagnostics are intentionally
    opaque.  Only a small reviewed set of error categories crosses into chat;
    regex redaction cannot prove unknown child output is non-secret.
    """

    try:
        limit = max(1, min(1_024, int(maximum)))
    except (TypeError, ValueError):
        limit = 512
    if isinstance(exc, LarkOnboardingRetainedError):
        rendered = _RETAINED_ONBOARDING_FAILURE
    elif isinstance(exc, LarkProfileError):
        private = str(exc)
        if "timed out" in private.casefold():
            rendered = "Lark bot onboarding timed out"
        elif "cancelled" in private.casefold():
            rendered = "Lark bot onboarding was cancelled"
        elif private == "could not deliver the Lark verification URL":
            rendered = "Could not send the Lark verification link"
        elif private.startswith("Lark profile already exists:"):
            rendered = "That Lark profile name is already registered"
        elif private == "duplicate live app id":
            rendered = "That Lark bot is already registered"
        elif private.startswith("unsupported lark-cli version"):
            rendered = "The installed lark-cli version is unsupported"
        elif private.startswith("lark-cli was not found"):
            rendered = "The required lark-cli executable is unavailable"
        else:
            rendered = _GENERIC_ONBOARDING_FAILURE
    else:
        rendered = _GENERIC_ONBOARDING_FAILURE
    if len(rendered) <= limit:
        return rendered
    if limit <= 3:
        return "." * limit
    return rendered[: limit - 3].rstrip() + "..."


class StagedCredentialDisposition(str, Enum):
    """Caller-proven relationship between staged and durable credentials."""

    UNKNOWN = "unknown"
    SHARED = "shared"
    UNOWNED = "unowned"


class _SessionState(str, Enum):
    STAGED = "staged"
    PUBLISHED = "published"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    RETAINED = "retained"


def _same_provisioned_identity(
    observed: ProvisionedConfig,
    expected: ProvisionedConfig,
) -> bool:
    return (
        observed.app_id,
        observed.brand,
        observed.credential_ref,
        observed.bot_open_id,
        observed.owner_open_id,
    ) == (
        expected.app_id,
        expected.brand,
        expected.credential_ref,
        expected.bot_open_id,
        expected.owner_open_id,
    )


class StagedLarkOnboarding:
    """One verified credential tree awaiting runtime publication/registration.

    The object retains the credential-mutation lock until ``commit``,
    ``rollback``, or ``retain``.  Identity properties are read-only.  Runtime
    orchestration should use this transaction shape::

        staged = await stage_lark_qr_onboarding(...)
        account_handle = ownership.acquire_account(
            channel="lark", bot_id=staged.app_id
        )
        # durable duplicate preflight
        record = staged.publish()
        # atomically register ``record`` in the already-open runtime store
        staged.commit()

    If registration fails after publication, call ``restore_publication`` and
    then ``rollback`` before releasing the newly acquired account handle.
    """

    __slots__ = (
        "_binary",
        "_cli_version",
        "_final_path",
        "_ownership",
        "_profile_id",
        "_provisioned",
        "_root",
        "_staging_path",
        "_state",
    )

    def __init__(
        self,
        *,
        binary: str,
        cli_version: str,
        root: Path,
        profile_id: str,
        staging_path: Path,
        final_path: Path,
        provisioned: ProvisionedConfig,
        ownership: CredentialMutationOwnership,
    ) -> None:
        self._binary = binary
        self._cli_version = cli_version
        self._root = root
        self._profile_id = profile_id
        self._staging_path = staging_path
        self._final_path = final_path
        self._provisioned = provisioned
        self._ownership = ownership
        self._state = _SessionState.STAGED

    @property
    def profile_id(self) -> str:
        return self._profile_id

    @property
    def app_id(self) -> str:
        return self._provisioned.app_id

    @property
    def brand(self) -> str:
        return self._provisioned.brand

    @property
    def bot_open_id(self) -> str:
        return self._provisioned.bot_open_id

    @property
    def owner_open_id(self) -> str:
        return self._provisioned.owner_open_id

    @property
    def credential_ref(self) -> str:
        return self._provisioned.credential_ref

    @property
    def cli_version(self) -> str:
        return self._cli_version

    @property
    def staging_path(self) -> Path:
        return self._staging_path

    @property
    def final_path(self) -> Path:
        return self._final_path

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def active_path(self) -> Path:
        if self._state in {_SessionState.PUBLISHED, _SessionState.COMMITTED}:
            return self._final_path
        return self._staging_path

    def build_profile_record(self) -> BotProfileRecord:
        """Return the exact durable record used by terminal QR onboarding."""

        return build_lark_profile_record(
            self._profile_id,
            self._final_path,
            self._provisioned,
            self._cli_version,
        )

    def _require_state(self, expected: _SessionState, operation: str) -> None:
        if self._state is not expected:
            raise LarkProfileError(
                f"cannot {operation} Lark onboarding in state {self._state.value}"
            )

    def _release_ownership(self) -> None:
        if self._ownership.held:
            self._ownership.close()

    def _reinspect_expected(self, path: Path) -> None:
        observed = secure_and_reinspect_provisioned_config(
            path,
            self._provisioned,
        )
        if not _same_provisioned_identity(observed, self._provisioned):
            raise LarkProfileError(
                "staged Lark identity changed before publication"
            )

    def reverify_owner_identity(self) -> str:
        """Re-attest the QR-created human immediately before store commit.

        The credential transaction must still own its mutation lock, but the
        profile may already be published for runtime preflight.  Validate the
        active config both before and after the bot-authenticated metadata read,
        then require the app, credential, bot, and human owner identities to be
        byte-for-byte identical to the original staged attestation.
        """

        if self._state not in {_SessionState.STAGED, _SessionState.PUBLISHED}:
            raise LarkProfileError(
                f"cannot reverify Lark app owner in state {self._state.value}"
            )
        if not self._ownership.held:
            raise LarkProfileError(
                "credential mutation ownership is required to reverify Lark app owner"
            )
        path = self.active_path
        self._reinspect_expected(path)
        reverified = inspect_app_owner_identity(
            self._binary,
            path,
            self._provisioned,
        )
        reverified = secure_and_reinspect_provisioned_config(path, reverified)
        if not _same_provisioned_identity(reverified, self._provisioned):
            raise LarkProfileError(
                "Lark app owner identity changed during onboarding"
            )
        return reverified.owner_open_id

    def publish(self) -> BotProfileRecord:
        """Atomically rename the verified staging tree to its stable path.

        The credential lock remains held.  Call ``commit`` only after the
        runtime's existing SQLite connection has durably registered the
        returned record.
        """

        self._require_state(_SessionState.STAGED, "publish")
        if self._final_path.exists() or self._final_path.is_symlink():
            raise LarkProfileError(
                f"Lark profile already exists: {self._profile_id}"
            )
        self._reinspect_expected(self._staging_path)
        os.replace(self._staging_path, self._final_path)
        self._state = _SessionState.PUBLISHED
        return self.build_profile_record()

    def commit(self) -> BotProfileRecord:
        """Complete a publication after its durable store transaction commits."""

        self._require_state(_SessionState.PUBLISHED, "commit")
        record = self.build_profile_record()
        self._release_ownership()
        self._state = _SessionState.COMMITTED
        return record

    def restore_publication(self) -> None:
        """Move an unregistered publication back to its private staging name."""

        self._require_state(_SessionState.PUBLISHED, "restore")
        if self._staging_path.exists() or self._staging_path.is_symlink():
            raise LarkProfileError(
                "Lark onboarding staging path reappeared during rollback"
            )
        self._reinspect_expected(self._final_path)
        os.replace(self._final_path, self._staging_path)
        self._state = _SessionState.STAGED

    def remove_runtime_artifacts_after_stop(self) -> None:
        """Remove only the verified app's stopped event-bus socket.

        The caller must first stop and fence every process for this newly
        prepared account while retaining its exact account lock.  Ordinary
        files (including private CLI logs) remain part of the credential tree;
        only ``events/<verified-app-id>/bus.sock`` is an admitted special file.
        """

        self._require_state(_SessionState.PUBLISHED, "clean runtime artifacts")
        event_apps = _validate_private_tree(
            self._final_path,
            allow_runtime_event_socket=True,
        )
        if event_apps.difference({self._provisioned.app_id}):
            raise LarkProfileError(
                "Lark runtime event socket belongs to another app"
            )
        if self._provisioned.app_id in event_apps:
            socket_path = (
                self._final_path
                / "events"
                / self._provisioned.app_id
                / "bus.sock"
            )
            details = socket_path.lstat()
            if (
                not stat.S_ISSOCK(details.st_mode)
                or details.st_nlink != 1
                or stat.S_IMODE(details.st_mode) & 0o077
                or (
                    hasattr(os, "getuid")
                    and details.st_uid != os.getuid()
                )
            ):
                raise LarkProfileError(
                    "Lark runtime event socket changed during cleanup"
                )
            # Unlink never follows a replacement symlink.  A concurrent live
            # process is excluded by the caller's stop fence and account lock.
            socket_path.unlink()
            for directory in (socket_path.parent, socket_path.parent.parent):
                try:
                    directory.rmdir()
                except OSError:
                    # Preserve non-empty private runtime diagnostics.
                    pass
        self._reinspect_expected(self._final_path)

    def retain(self) -> Path:
        """Release the mutation lock while keeping the sole recovery handle."""

        if self._state in {
            _SessionState.COMMITTED,
            _SessionState.ROLLED_BACK,
            _SessionState.RETAINED,
        }:
            raise LarkProfileError(
                f"cannot retain Lark onboarding in state {self._state.value}"
            )
        path = self.active_path
        self._release_ownership()
        self._state = _SessionState.RETAINED
        return path

    async def rollback(
        self,
        disposition: StagedCredentialDisposition,
    ) -> None:
        """Undo one unregistered stage using caller-proven ownership facts.

        ``UNOWNED`` is destructive and may be used only while the caller holds
        the newly discovered account lock and has proved through the live store
        that no durable profile owns the credential.  ``SHARED`` removes only
        this isolated config tree.  ``UNKNOWN`` deliberately retains it.
        """

        try:
            disposition = StagedCredentialDisposition(disposition)
        except ValueError as exc:
            raise LarkProfileError(
                "invalid staged credential disposition"
            ) from exc
        path = self.active_path
        try:
            if self._state is _SessionState.PUBLISHED:
                self.restore_publication()
            self._require_state(_SessionState.STAGED, "roll back")
            path = self._staging_path
            _tighten_owned_staging_tree(path)
            try:
                observed = _config_for_profile(
                    path,
                    allow_absolute_file_reference=False,
                )
            except LarkProfileError:
                if _unparseable_staging_is_discardable(path):
                    _safe_remove_tree(path, self._root)
                    self._release_ownership()
                    self._state = _SessionState.ROLLED_BACK
                    return
                retained = self.retain()
                raise LarkOnboardingRetainedError(retained)

            if (
                observed.app_id,
                observed.brand,
                observed.credential_ref,
            ) != (
                self._provisioned.app_id,
                self._provisioned.brand,
                self._provisioned.credential_ref,
            ):
                retained = self.retain()
                raise LarkOnboardingRetainedError(retained)

            if disposition is StagedCredentialDisposition.UNKNOWN:
                retained = self.retain()
                raise LarkOnboardingRetainedError(retained)
            if disposition is StagedCredentialDisposition.SHARED:
                _safe_remove_tree(path, self._root)
            else:
                cleanup_worker = asyncio.create_task(
                    asyncio.to_thread(
                        cleanup_unowned_provisioned_config,
                        self._binary,
                        self._root,
                        path,
                    ),
                    name=f"lark-onboarding-cleanup:{self._profile_id}",
                )
                try:
                    await asyncio.shield(cleanup_worker)
                except asyncio.CancelledError as cancellation:
                    # Destructive credential cleanup owns a restorable snapshot.
                    # Do not inspect or release its account/credential fences
                    # until that worker has either completed or restored it.
                    try:
                        await asyncio.shield(cleanup_worker)
                    except BaseException as cleanup_exc:
                        raise cleanup_exc from cancellation
                    raise
            self._release_ownership()
            self._state = _SessionState.ROLLED_BACK
        except LarkOnboardingRetainedError:
            raise
        except BaseException as exc:
            retained_path = self.active_path
            if retained_path.exists() or retained_path.is_symlink():
                try:
                    retained = self.retain()
                except LarkProfileError:
                    retained = retained_path
                raise LarkOnboardingRetainedError(retained) from exc
            self._release_ownership()
            self._state = _SessionState.ROLLED_BACK
            raise

    def __del__(self) -> None:  # pragma: no cover - deterministic APIs are tested
        # Never guess whether an external keychain entry is shared.  If a
        # caller abandons the transaction, release only the advisory lock and
        # leave its private directory as the recovery/audit handle.
        try:
            self._release_ownership()
        except Exception:
            pass


def _verification_urls(value: str) -> tuple[str, ...]:
    return tuple(match.group(0) for match in _OFFICIAL_VERIFICATION_URL.finditer(value))


async def _invoke_url_callback(
    callback: VerificationUrlCallback,
    url: str,
) -> None:
    try:
        result = callback(url)
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        raise LarkProfileError(
            "could not deliver the Lark verification URL"
        ) from exc


async def _run_qr_init_with_url_callback(
    *,
    binary: str,
    staging_path: Path,
    timeout: float,
    callback: VerificationUrlCallback,
    cancellation: threading.Event,
) -> str:
    """Bridge the synchronous reviewed runner to one async URL callback."""

    loop = asyncio.get_running_loop()
    output_queue: asyncio.Queue[str | None] = asyncio.Queue()

    def relay_output(value: str) -> None:
        loop.call_soon_threadsafe(output_queue.put_nowait, value)

    def invoke() -> str:
        try:
            return run_config_init(
                binary,
                staging_path,
                output=io.StringIO(),
                timeout=timeout,
                on_output=relay_output,
                cancel_requested=cancellation.is_set,
            )
        finally:
            loop.call_soon_threadsafe(output_queue.put_nowait, None)

    worker = asyncio.create_task(
        asyncio.to_thread(invoke),
        name=f"lark-onboarding-cli:{staging_path.name}",
    )
    verification_url: str | None = None
    try:
        while True:
            item = await output_queue.get()
            if item is None:
                break
            for candidate in _verification_urls(item):
                if verification_url is None:
                    verification_url = candidate
                    await _invoke_url_callback(callback, candidate)
                elif candidate != verification_url:
                    raise LarkProfileError(
                        "lark-cli returned conflicting verification URLs"
                    )
        raw = await asyncio.shield(worker)
    except BaseException:
        cancellation.set()
        try:
            await asyncio.shield(worker)
        except BaseException:
            pass
        raise
    if verification_url is None:
        raise LarkProfileError(
            "lark-cli did not return an official verification URL"
        )
    return raw


def _cleanup_incomplete_staging(
    root: Path,
    staging_path: Path,
) -> LarkOnboardingRetainedError | None:
    """Delete only a provably credential-free failure, otherwise retain it."""

    if not (staging_path.exists() or staging_path.is_symlink()):
        return None
    try:
        _tighten_owned_staging_tree(staging_path)
        if _unparseable_staging_is_discardable(staging_path):
            _safe_remove_tree(staging_path, root)
            return None
    except BaseException:
        return LarkOnboardingRetainedError(staging_path)
    return LarkOnboardingRetainedError(staging_path)


async def stage_lark_qr_onboarding(
    *,
    config_root: Path,
    on_verification_url: VerificationUrlCallback,
    profile_name: str | None = None,
    binary: str | os.PathLike[str] | None = None,
    timeout: float = DEFAULT_ONBOARD_TIMEOUT_SECONDS,
    cancel_event: threading.Event | None = None,
) -> StagedLarkOnboarding:
    """Provision and verify one QR-created app without touching runtime SQLite.

    The URL callback runs as soon as the pinned CLI emits the official link,
    while that child is still waiting for the scan.  Cancelling this coroutine
    sets the subprocess cancellation fence, terminates the complete process
    group, and removes an empty stage.  A non-empty stage whose external
    credential ownership cannot yet be proved is retained and reported via
    :class:`LarkOnboardingRetainedError`.
    """

    if not callable(on_verification_url):
        raise TypeError("on_verification_url must be callable")
    try:
        normalized_timeout = float(timeout)
    except (TypeError, ValueError) as exc:
        raise LarkProfileError(
            "Lark onboarding timeout must be a positive finite number"
        ) from exc
    if not math.isfinite(normalized_timeout) or normalized_timeout <= 0:
        raise LarkProfileError(
            "Lark onboarding timeout must be a positive finite number"
        )

    profile_id = normalize_profile_name(profile_name, generate=True)
    resolved_binary = resolve_lark_cli_binary(binary)
    cli_version = await asyncio.to_thread(
        verify_lark_cli_version,
        resolved_binary,
    )
    # Do not resolve the final component: `_ensure_private_directory` must see
    # and reject a configured symlink instead of silently trusting its target.
    root = Path(config_root).expanduser().absolute()
    _ensure_private_directory(root, create=True)
    final_path = _contained_child(root, profile_id)

    ownership = CredentialMutationOwnership(root)
    ownership.acquire()
    staging_path: Path | None = None
    cancellation = cancel_event or threading.Event()
    try:
        if final_path.exists() or final_path.is_symlink():
            raise LarkProfileError(f"Lark profile already exists: {profile_id}")
        staging_path = Path(
            tempfile.mkdtemp(prefix=f".add-{profile_id}-", dir=root)
        ).absolute()
        os.chmod(staging_path, 0o700)

        raw = await _run_qr_init_with_url_callback(
            binary=resolved_binary,
            staging_path=staging_path,
            timeout=normalized_timeout,
            callback=on_verification_url,
            cancellation=cancellation,
        )
        _tighten_owned_staging_tree(staging_path)
        provisioned = inspect_provisioned_config(staging_path, raw)

        def inspect_qr_created_identities() -> ProvisionedConfig:
            verified_bot = inspect_bot_identity(
                resolved_binary,
                staging_path,
                provisioned,
            )
            return inspect_app_owner_identity(
                resolved_binary,
                staging_path,
                verified_bot,
            )

        identity_worker = asyncio.create_task(
            asyncio.to_thread(inspect_qr_created_identities),
            name=f"lark-onboarding-identity:{profile_id}",
        )
        try:
            provisioned = await asyncio.shield(identity_worker)
        except asyncio.CancelledError:
            # The one-shot helper owns and fences its process group.  Wait for
            # that fence before inspecting/removing its config directory.
            try:
                await asyncio.shield(identity_worker)
            except BaseException:
                pass
            raise
        provisioned = secure_and_reinspect_provisioned_config(
            staging_path,
            provisioned,
        )
        return StagedLarkOnboarding(
            binary=resolved_binary,
            cli_version=cli_version,
            root=root,
            profile_id=profile_id,
            staging_path=staging_path,
            final_path=final_path,
            provisioned=provisioned,
            ownership=ownership,
        )
    except BaseException as exc:
        retained_error = (
            _cleanup_incomplete_staging(root, staging_path)
            if staging_path is not None
            else None
        )
        try:
            ownership.close()
        except BaseException:
            if retained_error is None and staging_path is not None:
                retained_error = LarkOnboardingRetainedError(staging_path)
        if retained_error is not None:
            raise retained_error from exc
        raise


__all__ = [
    "LarkOnboardingRetainedError",
    "StagedCredentialDisposition",
    "StagedLarkOnboarding",
    "VerificationUrlCallback",
    "public_onboarding_error",
    "stage_lark_qr_onboarding",
]
