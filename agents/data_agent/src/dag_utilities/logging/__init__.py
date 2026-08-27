"""
DAG Utilities Logging Module

Audit logging utilities for APEX DAG patterns.
"""

from datetime import datetime
from typing import Dict, Any, Optional
import json
import logging

logger = logging.getLogger(__name__)


class AuditLogger:
    """
    Audit Logger for APEX DAG patterns.

    Provides structured audit logging to:
    - PostgreSQL audit table
    - Structured logs (for GCP logging)
    - Optional external sinks (Splunk, Datadog)
    """

    def __init__(self, metadata_client):
        """
        Initialize AuditLogger.

        Args:
            metadata_client: MetadataClient instance for database access
        """
        self.metadata = metadata_client

    def log_event(
        self,
        event_type: str,
        feed_id: int = None,
        feed_group_id: int = None,
        execution_id: str = None,
        severity: str = "INFO",
        **kwargs
    ) -> None:
        """
        Log an audit event.

        Args:
            event_type: Type of event (e.g., PLAN_GENERATION_STARTED)
            feed_id: Optional feed ID
            feed_group_id: Optional feed group ID
            execution_id: Optional execution ID
            severity: Log severity (INFO, WARNING, ERROR, CRITICAL)
            **kwargs: Additional event data
        """
        event = {
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity,
            "feed_id": feed_id,
            "feed_group_id": feed_group_id,
            "execution_id": execution_id,
            "data": kwargs,
        }

        # Log to Python logger
        log_message = json.dumps(event, default=str)

        if severity == "CRITICAL":
            logger.critical(log_message)
        elif severity == "ERROR":
            logger.error(log_message)
        elif severity == "WARNING":
            logger.warning(log_message)
        else:
            logger.info(log_message)

        # Persist to database
        self._persist_event(event)

    def _persist_event(self, event: Dict[str, Any]) -> None:
        """
        Persist audit event to database.

        Args:
            event: Event dictionary
        """
        try:
            conn = self.metadata._get_connection()
            if conn is None:
                return

            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO platform_audit_log (
                    event_type, severity, feed_id, feed_group_id,
                    execution_id, event_data, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    event["event_type"],
                    event["severity"],
                    event.get("feed_id"),
                    event.get("feed_group_id"),
                    event.get("execution_id"),
                    json.dumps(event.get("data", {})),
                )
            )
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to persist audit event: {e}")

    def log(
        self,
        message: str,
        execution_id: str = None,
        severity: str = "INFO",
        **kwargs
    ) -> None:
        """
        Convenience method for simple logging.

        Args:
            message: Log message
            execution_id: Optional execution ID
            severity: Log severity
            **kwargs: Additional data
        """
        self.log_event(
            event_type="LOG_MESSAGE",
            execution_id=execution_id,
            severity=severity,
            message=message,
            **kwargs
        )


__all__ = ["AuditLogger"]
