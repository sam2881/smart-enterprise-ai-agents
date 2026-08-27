"""Tests for LangGraph workflow state machine patterns.

Tests the node error handling pattern and conditional edge routing
that is used throughout backend/orchestrator/langgraph_workflow.py.
"""
import pytest
from typing import Dict, Any, Literal


# --- Replicate patterns under test (mirrors CLAUDE.md patterns) ---

def make_failing_node(error_message: str, agent_name: str = "test_node"):
    """Create a LangGraph node that raises and returns error state."""
    def node(state: Dict[str, Any]) -> Dict[str, Any]:
        try:
            raise ValueError(error_message)
        except Exception as e:
            return {"error_message": str(e), "error_agent": agent_name}
    return node


def make_successful_node(output_key: str, output_value: Any):
    """Create a LangGraph node that succeeds and returns partial state."""
    def node(state: Dict[str, Any]) -> Dict[str, Any]:
        return {output_key: output_value}
    return node


def should_continue(state: Dict[str, Any]) -> Literal["next", "error"]:
    """Standard conditional edge: routes to error if error_message is set."""
    return "error" if state.get("error_message") else "next"


def route_approval(state: Dict[str, Any]) -> Literal["approved", "rejected", "pending"]:
    """Approval routing conditional edge."""
    status = state.get("approval_status")
    if status == "APPROVED":
        return "approved"
    elif status == "REJECTED":
        return "rejected"
    return "pending"


# --- Tests ---

class TestNodeErrorHandlingPattern:
    """Test the standard LangGraph node error pattern from CLAUDE.md."""

    def test_failing_node_returns_error_message(self):
        node = make_failing_node("Connection refused to kafka:9092")
        result = node({})
        assert result["error_message"] == "Connection refused to kafka:9092"

    def test_failing_node_returns_error_agent_name(self):
        node = make_failing_node("timeout", agent_name="node_swarm_rag")
        result = node({})
        assert result["error_agent"] == "node_swarm_rag"

    def test_failing_node_only_returns_error_keys(self):
        """Failed node only returns error_message and error_agent (LangGraph merges state)."""
        node = make_failing_node("error")
        result = node({"existing_key": "existing_value"})
        assert set(result.keys()) == {"error_message", "error_agent"}

    def test_successful_node_returns_partial_state(self):
        node = make_successful_node("incident_type", "INFRASTRUCTURE")
        result = node({})
        assert result == {"incident_type": "INFRASTRUCTURE"}

    def test_successful_node_does_not_copy_existing_state(self):
        """Nodes return only the keys they update — LangGraph merges the rest."""
        node = make_successful_node("new_key", "new_value")
        result = node({"old_key": "old_value"})
        assert "old_key" not in result


class TestConditionalEdgeRouting:
    """Test conditional edge routing based on error state."""

    def test_routes_to_error_when_error_message_present(self):
        state = {"error_message": "LLM call failed", "error_agent": "node_classify"}
        assert should_continue(state) == "error"

    def test_routes_to_next_when_no_error_key(self):
        state = {"incident_id": "INC001", "incident_type": "INFRASTRUCTURE"}
        assert should_continue(state) == "next"

    def test_routes_to_next_when_error_message_is_none(self):
        state = {"error_message": None, "incident_id": "INC001"}
        assert should_continue(state) == "next"

    def test_routes_to_next_when_error_message_is_empty_string(self):
        """Empty string is falsy — should route to next."""
        state = {"error_message": ""}
        assert should_continue(state) == "next"


class TestApprovalRoutingPattern:
    """Test approval workflow conditional edge routing."""

    def test_approved_state_routes_to_approved(self):
        state = {"approval_status": "APPROVED", "approved_by": "manager@company.com"}
        assert route_approval(state) == "approved"

    def test_rejected_state_routes_to_rejected(self):
        state = {"approval_status": "REJECTED", "rejection_reason": "Risk too high"}
        assert route_approval(state) == "rejected"

    def test_missing_approval_status_routes_to_pending(self):
        state = {"incident_id": "INC001"}
        assert route_approval(state) == "pending"

    def test_none_approval_status_routes_to_pending(self):
        state = {"approval_status": None}
        assert route_approval(state) == "pending"

    def test_partial_state_with_approval_routes_correctly(self, sample_incident_state):
        """Test with fixture state that has no approval_status set."""
        assert route_approval(sample_incident_state) == "pending"
