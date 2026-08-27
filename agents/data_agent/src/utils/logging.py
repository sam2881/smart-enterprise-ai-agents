"""
Structured logging utilities for the Data Engineering Agent.

Provides:
- get_logger(): Returns a structlog BoundLogger instance
- AgentLogger: Higher-level logger with agent-specific methods
  (log_start, log_complete, log_error, log_decision)
"""

from typing import Any, Dict, List, Optional

import structlog


def get_logger(name: str = "data-agent") -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


class AgentLogger:
    """
    Agent-aware structured logger.

    Wraps structlog with convenience methods for the common agent
    lifecycle events: start, complete, error, and decision.
    """

    def __init__(self, agent_name: str) -> None:
        self._log = structlog.get_logger(f"agent.{agent_name}")
        self.agent_name = agent_name

    # ── Lifecycle events ──────────────────────────────────────────────

    def log_start(
        self,
        request_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log agent execution start."""
        self._log.info(
            "agent_start",
            agent=self.agent_name,
            request_id=request_id,
            **(context or {}),
        )

    def log_complete(
        self,
        request_id: str,
        result: Optional[Dict[str, Any]] = None,
        duration_ms: int = 0,
    ) -> None:
        """Log agent execution completion."""
        status = "error" if (result or {}).get("error_message") else "success"
        self._log.info(
            "agent_complete",
            agent=self.agent_name,
            request_id=request_id,
            status=status,
            duration_ms=duration_ms,
        )

    def log_error(
        self,
        request_id: str,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log agent execution error."""
        self._log.error(
            "agent_error",
            agent=self.agent_name,
            request_id=request_id,
            error=str(error),
            error_type=type(error).__name__,
            **(context or {}),
        )

    # ── Decision logging ──────────────────────────────────────────────

    def log_decision(
        self,
        request_id: str,
        decision: str,
        reasoning: str,
        alternatives: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log an agent decision with reasoning and alternatives."""
        self._log.info(
            "agent_decision",
            agent=self.agent_name,
            request_id=request_id,
            decision=decision,
            reasoning=reasoning,
            alternatives=alternatives or [],
            **(context or {}),
        )
