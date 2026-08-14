"""Small client for the local task-scoped Agent collaboration bridge."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
from typing import Any, Sequence


SOCKET_ENV = "CODEX_WECHAT_AGENT_SOCKET"
TASK_ENV = "CODEX_WECHAT_TASK_ID"
CAPABILITY_ENV = "CODEX_WECHAT_AGENT_CAPABILITY"
_MAX_RESPONSE_BYTES = 128 * 1024


class AgentCLIError(RuntimeError):
    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.agent_cli",
        description="List or message Agents authorized for the current task.",
    )
    parser.add_argument(
        "--socket",
        dest="socket_path",
        default=os.environ.get(SOCKET_ENV, ""),
        help=f"Agent bridge Unix socket (default: ${SOCKET_ENV})",
    )
    parser.add_argument(
        "--task-id",
        default=os.environ.get(TASK_ENV, ""),
        help=f"active task ID (default: ${TASK_ENV})",
    )
    parser.add_argument(
        "--capability",
        default=os.environ.get(CAPABILITY_ENV, ""),
        help="task-execution capability supplied by the Codex turn context",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    commands = parser.add_subparsers(dest="command", required=True)

    listing = commands.add_parser("list", help="list authorized Agent peers")
    listing.add_argument("--request-type", default="ask")

    sending = commands.add_parser("send", help="send one durable Agent request")
    sending.add_argument("destination_agent_id")
    sending.add_argument("content", nargs="+")
    sending.add_argument("--request-type", default="ask")
    sending.add_argument("--request-id", default="")
    return parser


def _request(socket_path: str, request: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    if timeout <= 0:
        raise AgentCLIError("timeout must be positive")
    path = str(Path(socket_path).expanduser())
    if not path:
        raise AgentCLIError(
            f"Agent bridge socket is required (pass --socket or set {SOCKET_ENV})"
        )
    payload = json.dumps(
        request, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8") + b"\n"
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect(path)
            client.sendall(payload)
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > _MAX_RESPONSE_BYTES:
                    raise AgentCLIError("Agent bridge response exceeds size limit")
                if b"\n" in chunk:
                    break
    except (OSError, TimeoutError) as exc:
        raise AgentCLIError(f"Agent bridge connection failed: {exc}") from exc
    raw = b"".join(chunks).split(b"\n", 1)[0]
    if not raw:
        raise AgentCLIError("Agent bridge returned an empty response")
    try:
        response = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AgentCLIError("Agent bridge returned invalid JSON") from exc
    if not isinstance(response, dict):
        raise AgentCLIError("Agent bridge returned an invalid response")
    return response


def _default_request_id(
    *,
    task_id: str,
    destination_agent_id: str,
    request_type: str,
    content: str,
) -> str:
    """Return a retry-stable ID for one logical CLI request.

    If the socket response is lost after SQLite commits, repeating the same
    command must resolve the existing destination/request row instead of
    enqueueing a duplicate. Callers that intentionally need the same content
    twice can supply a distinct explicit ``--request-id``.
    """

    encoded = json.dumps(
        [task_id, destination_agent_id, request_type, content],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return "agent-cli-" + hashlib.sha256(encoded).hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        task_id = str(args.task_id or "").strip()
        if not task_id:
            raise AgentCLIError(
                f"active task ID is required (pass --task-id or set {TASK_ENV})"
            )
        capability = str(args.capability or "").strip()
        if not capability:
            raise AgentCLIError(
                "Agent bridge capability is required (use the command supplied "
                "in the current Codex turn context)"
            )
        if not str(args.socket_path or "").strip():
            raise AgentCLIError(
                f"Agent bridge socket is required (pass --socket or set {SOCKET_ENV})"
            )
        if args.command == "list":
            request = {
                "operation": "list",
                "task_id": task_id,
                "capability": capability,
                "request_type": args.request_type,
            }
        else:
            content = " ".join(args.content)
            request = {
                "operation": "send",
                "task_id": task_id,
                "capability": capability,
                "destination_agent_id": args.destination_agent_id,
                "content": content,
                "request_type": args.request_type,
                "request_id": args.request_id
                or _default_request_id(
                    task_id=task_id,
                    destination_agent_id=args.destination_agent_id,
                    request_type=args.request_type,
                    content=content,
                ),
            }
        response = _request(
            str(args.socket_path), request, timeout=float(args.timeout)
        )
    except AgentCLIError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    rendered = json.dumps(response, ensure_ascii=False, sort_keys=True)
    if not bool(response.get("ok")):
        print(rendered, file=sys.stderr)
        return 1
    print(rendered)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())


__all__ = ["AgentCLIError", "main"]
