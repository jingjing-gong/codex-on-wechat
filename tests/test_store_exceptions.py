"""Backend-neutral durable-store exception contract tests."""

from __future__ import annotations

import src.runtime as runtime
from src.runtime.agent_bridge import AgentBridgeServer
from src.runtime.sqlite_store import (
    InvalidTransition as SQLiteInvalidTransition,
    NotFoundError as SQLiteNotFoundError,
    QueueFullError as SQLiteQueueFullError,
    StoreError as SQLiteStoreError,
)
from src.runtime.store import (
    InvalidTransition,
    NotFoundError,
    QueueFullError,
    StoreError,
)


def test_sqlite_and_public_runtime_reexport_backend_neutral_exceptions() -> None:
    assert SQLiteStoreError is StoreError
    assert SQLiteNotFoundError is NotFoundError
    assert SQLiteInvalidTransition is InvalidTransition
    assert SQLiteQueueFullError is QueueFullError
    assert runtime.StoreError is StoreError
    assert runtime.NotFoundError is NotFoundError
    assert runtime.InvalidTransition is InvalidTransition
    assert runtime.QueueFullError is QueueFullError


def test_queue_full_error_retains_stable_backend_neutral_contract() -> None:
    agent = QueueFullError(" AGENT ", 12)
    assert isinstance(agent, StoreError)
    assert agent.scope == "agent"
    assert agent.limit == 12
    assert str(agent) == "queue_full: agent limit 12"

    fallback = QueueFullError("unknown", 3)
    assert fallback.scope == "global"
    assert fallback.limit == 3
    assert str(fallback) == "queue_full: global limit 3"


def test_agent_bridge_matches_exception_type_instead_of_class_name() -> None:
    response = AgentBridgeServer._error_response(QueueFullError("global", 4))
    assert response["error"] == "queue_full"

    SameNameOnly = type("QueueFullError", (RuntimeError,), {})
    lookalike = SameNameOnly("not a durable-store exception")
    lookalike.scope = "agent"
    response = AgentBridgeServer._error_response(lookalike)
    assert response["error"] == "bridge_error"
