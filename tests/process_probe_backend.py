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
        self._interrupts: dict[str, asyncio.Event] = {}
        self._image_output_publisher = _kwargs.get("image_output_publisher")

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        for interrupt in self._interrupts.values():
            interrupt.set()

    async def run(self, task: Any, emit: Any) -> dict[str, Any]:
        metadata = dict(task.metadata)
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
        pre_register_marker = str(metadata.get("pre_register_marker", "") or "")
        if pre_register_marker:
            Path(pre_register_marker).touch()
            await asyncio.sleep(float(metadata.get("pre_register_delay", 1.0)))
        interrupt = asyncio.Event()
        self._interrupts[task_id] = interrupt
        try:
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
            self._interrupts.pop(task_id, None)

    async def interrupt(self, task_id: str) -> bool:
        interrupt = self._interrupts.get(str(task_id))
        if interrupt is None:
            return False
        interrupt.set()
        return True


def barrier_probe_backend_factory(*args: Any, **kwargs: Any) -> BarrierProbeBackend:
    return BarrierProbeBackend(*args, **kwargs)


def contaminated_backend_factory(*args: Any, **kwargs: Any) -> BarrierProbeBackend:
    # The child must reject this backend after construction because merely
    # importing SQLite into an Agent process violates the process boundary.
    import sqlite3  # noqa: F401

    return BarrierProbeBackend(*args, **kwargs)
