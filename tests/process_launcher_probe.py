"""Subprocess entrypoint that mirrors the production launcher's import graph."""

from __future__ import annotations

import asyncio
import json
import sys

# Multiprocessing spawn re-executes this file as ``__mp_main__``.  Importing
# the real launcher at module scope reproduces the exact supervisor module
# graph that must remain outside the subsequently constructed Agent runtime.
import src.codex_wechat_bot  # noqa: F401
from src.runtime.process_agent import ProcessAgentRuntime


async def _main(workspace: str) -> None:
    runtime = ProcessAgentRuntime.create(
        "probe",
        cwd=workspace,
        backend_factory="tests.process_probe_backend:barrier_probe_backend_factory",
        start_timeout=5,
        stop_timeout=5,
    )
    await runtime.start()
    try:
        print(
            json.dumps(
                {
                    "child_pid": runtime.pid,
                    "generation": runtime.generation,
                    "health": runtime.health,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        await runtime.stop()


if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1]))
