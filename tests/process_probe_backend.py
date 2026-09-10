"""Child-safe deterministic backend for process-runtime integration tests.

This module is imported inside a spawned Agent process.  Keep it independent
of pytest, the runtime package, SQLite, channel code, and supervisor objects.
The process boundary accepts mapping-shaped events/results and reconstructs
the SDK-independent contracts on the supervisor side.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any


class BarrierProbeBackend:
    """Expose child entry, interruption, and release through local files."""

    def __init__(self, agent_id: str = "", **_kwargs: Any) -> None:
        self.agent_id = str(agent_id)
        self.configured_cwd = str(_kwargs.get("cwd") or "")
        self._interrupts: dict[str, asyncio.Event] = {}
        self._steering: dict[str, dict[str, Any]] = {}
        self._image_output_publisher = _kwargs.get("image_output_publisher")

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        for interrupt in self._interrupts.values():
            interrupt.set()

    async def run(self, task: Any, emit: Any) -> dict[str, Any]:
        metadata = dict(task.metadata)
        if metadata.get("probe_task_provenance"):
            return {
                "task_id": str(task.task_id),
                "execution_id": task.execution_id,
                "status": "completed",
                "content": "task-provenance-probed",
                "metadata": {
                    "inbound_message_id": getattr(
                        task, "inbound_message_id", None
                    ),
                },
            }
        if metadata.get("probe_workspace"):
            snapshot = metadata.get("execution_workspace")
            return {
                "task_id": str(task.task_id),
                "execution_id": task.execution_id,
                "status": "completed",
                "content": "workspace-probed",
                "metadata": {
                    "pid": os.getpid(),
                    "process_cwd": os.getcwd(),
                    "configured_cwd": self.configured_cwd,
                    "execution_workspace": dict(snapshot)
                    if isinstance(snapshot, dict)
                    else snapshot,
                },
            }
        if metadata.get("probe_import_boundary"):
            forbidden = (
                "_sqlite3",
                "sqlite3",
                "src.runtime.sqlite_store",
                "src.runtime.store",
                "src.runtime.worker",
                "src.channels",
                "wechat_ilink",
            )
            violations = sorted(
                name
                for name in sys.modules
                if any(
                    name == prefix or name.startswith(prefix + ".")
                    for prefix in forbidden
                )
            )
            return {
                "task_id": str(task.task_id),
                "execution_id": task.execution_id,
                "status": "completed",
                "content": "boundary-probed",
                "metadata": {"forbidden_modules": violations},
            }
        if metadata.get("probe_artifact"):
            publisher = self._image_output_publisher
            if publisher is None:
                raise RuntimeError("artifact publisher was not provided to child")
            published = publisher(
                task,
                source_item_id="image-item-1",
                source_item_ordinal=1,
                saved_path=str(metadata.get("saved_path", "")),
                result="",
            )
            if hasattr(published, "__await__"):
                published = await published
            attachment_id = (
                published.get("attachment_id", published.get("id", ""))
                if isinstance(published, dict)
                else str(published or "")
            )
            return {
                "task_id": str(task.task_id),
                "execution_id": task.execution_id,
                "status": "completed",
                "content": str(attachment_id),
                "metadata": {"attachment_id": str(attachment_id)},
            }
        root = Path(str(metadata["probe_root"]))
        root.mkdir(parents=True, exist_ok=True)
        task_id = str(task.task_id)
        steering_state = {
            "root": root,
            "calls": [],
            "delay": float(metadata.get("steer_delay", 0) or 0),
            "entered_marker": str(
                metadata.get("steer_entered_marker", "") or ""
            ),
            "fail_uncertain": bool(metadata.get("steer_fail_uncertain", False)),
            "ready": asyncio.Event(),
        }
        self._steering[task_id] = steering_state
        try:
            pre_register_marker = str(metadata.get("pre_register_marker", "") or "")
            if pre_register_marker:
                Path(pre_register_marker).touch()
                await asyncio.sleep(float(metadata.get("pre_register_delay", 1.0)))
            steering_state["ready"].set()
            interrupt = asyncio.Event()
            self._interrupts[task_id] = interrupt
            (root / f"{task_id}.entered.json").write_text(
                json.dumps(
                    {
                        "agent_id": task.agent_id,
                        "backend_agent_id": self.agent_id,
                        "pid": os.getpid(),
                        "task_id": task_id,
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            release = root / f"{task_id}.release"
            while not release.exists() and not interrupt.is_set():
                await asyncio.sleep(0.01)
            if interrupt.is_set():
                (root / f"{task_id}.interrupted").touch()
                return {
                    "task_id": task_id,
                    "execution_id": task.execution_id,
                    "status": "interrupted",
                    "interrupted": True,
                }
            content = f"{task.agent_id}:{os.getpid()}"
            event = {
                "task_id": task_id,
                "execution_id": task.execution_id,
                "sequence": 1,
                "event_type": "message",
                "visibility": "user",
                "priority": 1,
                "content": content,
                "source_item_id": f"probe:{task_id}",
                "source_item_type": "agentMessage",
                "source_item_ordinal": 1,
            }
            await emit(event)
            return {
                "task_id": task_id,
                "execution_id": task.execution_id,
                "status": "completed",
                "content": content,
                "events": [],
            }
        finally:
            steering_state["ready"].set()
            self._interrupts.pop(task_id, None)
            self._steering.pop(task_id, None)

    async def steer(
        self,
        task_id: str,
        inputs: Any,
        *,
        steering_id: str = "",
        execution_id: str = "",
    ) -> bool:
        state = self._steering.get(str(task_id))
        if state is None:
            return False
        await state["ready"].wait()
        if self._steering.get(str(task_id)) is not state:
            return False
        if state["entered_marker"]:
            Path(state["entered_marker"]).touch()
        delay = float(state["delay"])
        if delay:
            await asyncio.sleep(delay)
        calls = state["calls"]
        calls.append(
            {
                "execution_id": str(execution_id),
                "inputs": inputs,
                "steering_id": str(steering_id),
            }
        )
        root = Path(state["root"])
        (root / f"{task_id}.steering.json").write_text(
            json.dumps(calls, ensure_ascii=False),
            encoding="utf-8",
        )
        if state["fail_uncertain"]:
            error = RuntimeError("synthetic post-steer acknowledgement failure")
            error.execution_uncertain = True
            raise error
        return True

    async def interrupt(self, task_id: str) -> bool:
        interrupt = self._interrupts.get(str(task_id))
        if interrupt is None:
            return False
        interrupt.set()
        return True

    async def compact_session(
        self,
        conversation_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Expose control-call ownership for the parent proxy tests."""

        return {
            "thread_id": str(kwargs.get("thread_id") or ""),
            "compaction": {
                "agent_id": self.agent_id,
                "conversation_id": str(conversation_id),
                "pid": os.getpid(),
            },
        }


def barrier_probe_backend_factory(*args: Any, **kwargs: Any) -> BarrierProbeBackend:
    return BarrierProbeBackend(*args, **kwargs)


def contaminated_backend_factory(*args: Any, **kwargs: Any) -> BarrierProbeBackend:
    # The child must reject this backend after construction because merely
    # importing SQLite into an Agent process violates the process boundary.
    import sqlite3  # noqa: F401

    return BarrierProbeBackend(*args, **kwargs)


class ProfileLifecycleProbeBackend:
    """Child-safe backend for named profile/process lifecycle tests."""

    def __init__(
        self,
        agent_id: str = "",
        codex_config_profile: str = "",
        **_kwargs: Any,
    ) -> None:
        self.agent_id = str(agent_id)
        self.codex_config_profile = str(codex_config_profile)

    async def start(self) -> None:
        if self.agent_id.startswith("fail-"):
            raise RuntimeError("synthetic named child startup failure")

    async def stop(self) -> None:
        return None

    async def run(self, task: Any, _emit: Any = None) -> dict[str, Any]:
        return {
            "task_id": str(task.task_id),
            "execution_id": task.execution_id,
            "status": "completed",
            "content": f"{self.agent_id}:{self.codex_config_profile}",
            "metadata": {
                "pid": os.getpid(),
                "codex_config_profile": self.codex_config_profile,
            },
        }

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_models(
        self,
        *,
        include_hidden: bool = False,
    ) -> list[dict[str, Any]]:
        del include_hidden
        model_id = (
            f"{self.codex_config_profile}-model"
            if self.codex_config_profile
            else "base-model"
        )
        return [{"id": model_id, "isDefault": True}]


def profile_lifecycle_probe_backend_factory(
    *args: Any,
    **kwargs: Any,
) -> ProfileLifecycleProbeBackend:
    return ProfileLifecycleProbeBackend(*args, **kwargs)


IPC_SYNTHETIC_SECRET = "SYNTHETIC-IPC-URI-CREDENTIAL-7E19"
IPC_SYNTHETIC_ESC_SECRET = "SYNTHETIC-IPC-ESC-CREDENTIAL-91A4"
IPC_SYNTHETIC_BEARER_SECRET = "SYNTHETIC-IPC-QUOTED-BEARER-4C2D"
IPC_SYNTHETIC_BASIC_SECRET = "SYNTHETIC-IPC-QUOTED-BASIC-8F31"
IPC_SYNTHETIC_CAMEL_SECRET = "SYNTHETIC-IPC-CAMEL-SECRET-63B7"
IPC_SYNTHETIC_HOST = "provider-ipc-secret.invalid"


def _ipc_sensitive_uri_text() -> str:
    return (
        "provider failed at "
        f"acme+tls://fake-user:{IPC_SYNTHETIC_SECRET}@"
        f"{IPC_SYNTHETIC_HOST}/v1?api_key={IPC_SYNTHETIC_SECRET}"
    )


def _ipc_sensitive_c0_text() -> str:
    return (
        "provider rejected "
        f"to\x1b[31mken\x1b[0m = {IPC_SYNTHETIC_ESC_SECRET}"
    )


def _ipc_sensitive_failure_text() -> str:
    """Return fake malformed diagnostics that must remain child-local."""

    return f"{_ipc_sensitive_uri_text()}; {_ipc_sensitive_c0_text()}"


def _ipc_sensitive_quoted_auth_text() -> str:
    return (
        f'provider rejected Bearer "{IPC_SYNTHETIC_BEARER_SECRET}"; '
        f"retry rejected Basic '{IPC_SYNTHETIC_BASIC_SECRET}'; "
        f'provider echoed "Bearer" "{IPC_SYNTHETIC_BEARER_SECRET}"; '
        f"provider echoed 'Basic' '{IPC_SYNTHETIC_BASIC_SECRET}'; "
        f"Bearer [{IPC_SYNTHETIC_BEARER_SECRET}]; "
        f"Basic: {IPC_SYNTHETIC_BASIC_SECRET}; "
        f"bearerToken={IPC_SYNTHETIC_CAMEL_SECRET}; "
        f"refreshToken={IPC_SYNTHETIC_CAMEL_SECRET}; "
        f"clientSecret={IPC_SYNTHETIC_CAMEL_SECRET}"
    )


class IPCSecrecyProbeBackend:
    """Exercise every error-bearing child/parent IPC direction."""

    def __init__(self, agent_id: str = "", **_kwargs: Any) -> None:
        self.agent_id = str(agent_id)
        self._image_output_publisher = _kwargs.get("image_output_publisher")
        self._interrupt_waiters: dict[str, asyncio.Event] = {}
        self._interrupt_failed_once = False

    async def start(self) -> None:
        if self.agent_id == "fatal-uri":
            raise RuntimeError(_ipc_sensitive_uri_text())
        if self.agent_id == "fatal-c0":
            raise RuntimeError(_ipc_sensitive_c0_text())

    async def stop(self) -> None:
        if self.agent_id == "stop-error":
            raise RuntimeError(_ipc_sensitive_failure_text())
        return None

    async def interrupt(self, task_id: str) -> bool:
        waiter = self._interrupt_waiters.get(str(task_id))
        if waiter is None:
            return False
        if not self._interrupt_failed_once:
            self._interrupt_failed_once = True
            raise RuntimeError(_ipc_sensitive_failure_text())
        waiter.set()
        return True

    async def list_models(
        self,
        *,
        include_hidden: bool = False,
    ) -> list[dict[str, Any]]:
        del include_hidden
        raise RuntimeError(_ipc_sensitive_uri_text())

    async def run(self, task: Any, emit: Any = None) -> Any:
        operation_value = task.inputs
        if hasattr(operation_value, "get"):
            operation_value = operation_value.get(
                "text", operation_value.get("content", "")
            )
        elif isinstance(operation_value, (list, tuple)) and operation_value:
            first = operation_value[0]
            operation_value = (
                first.get("text", first.get("content", ""))
                if hasattr(first, "get")
                else first
            )
        operation = str(operation_value or "")
        if operation == "raise-uri-error":
            raise RuntimeError(_ipc_sensitive_uri_text())
        if operation == "raise-c0-error":
            raise RuntimeError(_ipc_sensitive_c0_text())
        if operation == "raise-quoted-auth-error":
            raise RuntimeError(_ipc_sensitive_quoted_auth_text())
        if operation == "return-error":
            return {
                "task_id": str(task.task_id),
                "execution_id": task.execution_id,
                "status": "failed",
                "error": _ipc_sensitive_failure_text(),
            }
        if operation == "return-error-event":
            return _IPCSecrecyWireResult(task)
        if operation == "artifact-callback-error":
            publisher = self._image_output_publisher
            if publisher is None:
                raise RuntimeError("artifact publisher was not provided")
            await publisher(
                task,
                source_item_id="synthetic-image-item",
                source_item_ordinal=1,
                saved_path="synthetic-image.png",
                result="",
            )
        if operation == "interrupt-error":
            waiter = asyncio.Event()
            self._interrupt_waiters[str(task.task_id)] = waiter
            try:
                await emit(
                    {
                        "task_id": str(task.task_id),
                        "execution_id": task.execution_id,
                        "sequence": 1,
                        "event_type": "message",
                        "visibility": "internal",
                        "priority": 0,
                        "content": "interrupt probe entered",
                    }
                )
                await waiter.wait()
                return {
                    "task_id": str(task.task_id),
                    "execution_id": task.execution_id,
                    "status": "interrupted",
                    "interrupted": True,
                }
            finally:
                self._interrupt_waiters.pop(str(task.task_id), None)
        if operation in {"emit-error", "callback-error"}:
            event_content = (
                _ipc_sensitive_uri_text()
                if operation == "emit-error"
                else "safe callback probe"
            )
            await emit(
                {
                    "task_id": str(task.task_id),
                    "execution_id": task.execution_id,
                    "sequence": 1,
                    "event_type": "provider_error",
                    "visibility": "internal",
                    "priority": 0,
                    "content": event_content,
                }
            )
        return {
            "task_id": str(task.task_id),
            "execution_id": task.execution_id,
            "status": "completed",
            "content": "probe complete",
        }


class _IPCSecrecyWireResult:
    """Result-shaped child value carrying an explicit provider error event."""

    def __init__(self, task: Any) -> None:
        self.task = task

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": str(self.task.task_id),
            "execution_id": self.task.execution_id,
            "status": "failed",
            "content": "",
            "output": "",
            "error": "provider operation failed",
            "events": [
                {
                    "task_id": str(self.task.task_id),
                    "execution_id": self.task.execution_id,
                    "sequence": 1,
                    "event_type": "provider_error",
                    "visibility": "internal",
                    "priority": 0,
                    "content": _ipc_sensitive_uri_text(),
                }
            ],
            "interrupted": False,
            "thread_id": None,
            "usage": {},
            "metadata": {},
        }


def ipc_secrecy_probe_backend_factory(
    *args: Any,
    **kwargs: Any,
) -> IPCSecrecyProbeBackend:
    return IPCSecrecyProbeBackend(*args, **kwargs)
