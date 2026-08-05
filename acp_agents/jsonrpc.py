"""Small async JSON-RPC 2.0 connection over a subprocess stdio pair.

Provides the stdio JSON-RPC transport used by codex-wechat-bot while staying
dependency-free for tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

JsonRpcHandler = Callable[[dict[str, Any]], Awaitable[None]]


class JsonRpcError(RuntimeError):
    """Raised when a JSON-RPC request receives an `error` response."""

    def __init__(self, code: int, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


class StdioJsonRpcConnection:
    """A JSON-RPC 2.0 connection to a subprocess over stdin/stdout (one message per line)."""

    def __init__(
        self,
        command: str,
        args: Optional[list[str]] = None,
        *,
        cwd: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ):
        self.command = command
        self.args = args or []
        self.cwd = cwd
        self.env = env

        self._process: Optional[asyncio.subprocess.Process] = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._request_handlers: dict[str, JsonRpcHandler] = {}
        self._notification_handlers: dict[str, JsonRpcHandler] = {}
        self._read_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._last_stderr_line = ""
        self._write_lock = asyncio.Lock()
        self._started = False

    @property
    def pid(self) -> int:
        return self._process.pid if self._process and self._process.pid else 0

    def on_request(self, method: str, handler: JsonRpcHandler) -> None:
        """Register a handler for a server->client request (must send a response)."""
        self._request_handlers[method] = handler

    def on_notification(self, method: str, handler: JsonRpcHandler) -> None:
        """Register a handler for a server->client notification (no response)."""
        self._notification_handlers[method] = handler

    async def start(self) -> None:
        if self._started:
            return

        full_env = dict(os.environ)
        if self.env:
            full_env.update(self.env)

        try:
            self._process = await asyncio.create_subprocess_exec(
                self.command,
                *self.args,
                cwd=self.cwd,
                env=full_env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"agent binary not found: {self.command}") from exc

        self._started = True
        self._read_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._stderr_loop())
        logger.info(
            "started subprocess (command=%s, pid=%s)", self.command, self._process.pid
        )

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False

        for task in (self._read_task, self._stderr_task):
            if task:
                task.cancel()

        if self._process:
            try:
                if self._process.stdin:
                    self._process.stdin.close()
                if self._process.returncode is None:
                    self._process.kill()
                await self._process.wait()
            except ProcessLookupError:
                pass

    async def request(
        self, method: str, params: Any = None, *, timeout: Optional[float] = 30.0
    ) -> Any:
        if not self._started:
            await self.start()

        self._next_id += 1
        msg_id = self._next_id
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": msg_id, "method": method}
        if params is not None:
            payload["params"] = params

        await self._write(payload)
        try:
            if timeout is not None:
                return await asyncio.wait_for(fut, timeout=timeout)
            return await fut
        finally:
            self._pending.pop(msg_id, None)

    async def notify(self, method: str, params: Any = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        await self._write(payload)

    async def respond(
        self,
        request_id: Any,
        result: Any = None,
        error: Optional[dict[str, Any]] = None,
    ) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result
        await self._write(payload)

    def last_stderr(self) -> str:
        line, self._last_stderr_line = self._last_stderr_line, ""
        return line

    async def _write(self, payload: dict[str, Any]) -> None:
        if not self._process or not self._process.stdin:
            raise RuntimeError("connection not started")
        data = (json.dumps(payload) + "\n").encode("utf-8")
        async with self._write_lock:
            self._process.stdin.write(data)
            await self._process.stdin.drain()

    async def _read_loop(self) -> None:
        assert self._process and self._process.stdout
        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("failed to parse message: %.200s", line)
                    continue
                await self._dispatch(msg)
        except asyncio.CancelledError:
            pass
        logger.info("read loop ended (command=%s)", self.command)

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        msg_id = msg.get("id")
        method = msg.get("method")

        # Response to a request we made (has id, no method).
        if msg_id is not None and not method:
            fut = self._pending.get(msg_id)
            if fut and not fut.done():
                error = msg.get("error")
                if error:
                    fut.set_exception(
                        JsonRpcError(
                            error.get("code", -1), error.get("message", "unknown error")
                        )
                    )
                else:
                    fut.set_result(msg.get("result"))
            return

        # Request from the agent needing a response.
        if method and msg_id is not None:
            handler = self._request_handlers.get(method)
            if handler:
                asyncio.create_task(handler(msg))
            else:
                logger.warning("unhandled request from agent: %s", method)
            return

        # Notification (no id).
        if method:
            handler = self._notification_handlers.get(method)
            if handler:
                asyncio.create_task(handler(msg))
            else:
                logger.debug("unhandled notification: %s", method)

    async def _stderr_loop(self) -> None:
        assert self._process and self._process.stderr
        try:
            while True:
                line = await self._process.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="replace").rstrip()
                if text:
                    logger.info("[stderr:%s] %s", self.command, text)
                    if not text.startswith(" ") and not text.startswith("Traceback"):
                        self._last_stderr_line = text
        except asyncio.CancelledError:
            pass
