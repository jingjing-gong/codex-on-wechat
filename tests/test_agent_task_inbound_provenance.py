"""Runtime-contract preservation of durable human-ingress provenance."""

from src.agents.base import AgentTask


def test_agent_task_round_trip_preserves_inbound_message_id() -> None:
    task = AgentTask.from_record(
        {
            "task_id": "task-a",
            "conversation_id": "conversation-a",
            "inputs": {"text": "schedule this"},
            "inbound_message_id": "inbound-a",
        }
    )

    assert task.inbound_message_id == "inbound-a"
    assert task.as_dict()["inbound_message_id"] == "inbound-a"
    assert AgentTask.from_record(task).inbound_message_id == "inbound-a"

