"""
DAG Utilities Remediation Module

Self-healing and retry handling utilities for APEX DAG patterns.
"""

from datetime import timedelta
from typing import Dict, Any, Optional, List


class RetryHandler:
    """
    Retry Handler for APEX DAG patterns.

    Provides configurable retry logic for Airflow tasks with:
    - Exponential backoff
    - Configurable max retries
    - Retry delay configuration
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay_minutes: int = 5,
        exponential_backoff: bool = True
    ):
        """
        Initialize RetryHandler.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay_minutes: Base delay between retries in minutes
            exponential_backoff: Whether to use exponential backoff
        """
        self.max_retries = max_retries
        self.retry_delay_minutes = retry_delay_minutes
        self.exponential_backoff = exponential_backoff

    def get_airflow_retry_args(self) -> Dict[str, Any]:
        """
        Get Airflow-compatible retry arguments.

        Returns:
            Dictionary of retry configuration for default_args
        """
        return {
            "retries": self.max_retries,
            "retry_delay": timedelta(minutes=self.retry_delay_minutes),
            "retry_exponential_backoff": self.exponential_backoff,
            "max_retry_delay": timedelta(minutes=self.retry_delay_minutes * 4),
        }

    def should_retry(
        self,
        exception: Exception,
        attempt: int
    ) -> bool:
        """
        Determine if a retry should be attempted.

        Args:
            exception: The exception that occurred
            attempt: Current attempt number

        Returns:
            True if retry should be attempted
        """
        if attempt >= self.max_retries:
            return False

        # Define retryable exceptions
        retryable_exceptions = (
            ConnectionError,
            TimeoutError,
            IOError,
        )

        return isinstance(exception, retryable_exceptions)


class SelfHealer:
    """
    Self-Healer for APEX DAG patterns.

    Provides self-healing capabilities for common issues:
    - Schema drift detection and recovery
    - Missing file handling
    - Connection recovery
    - Partition recovery
    """

    def __init__(self, metadata_client, audit_logger=None):
        """
        Initialize SelfHealer.

        Args:
            metadata_client: MetadataClient instance
            audit_logger: Optional AuditLogger instance
        """
        self.metadata = metadata_client
        self.audit = audit_logger

    def heal_schema_drift(
        self,
        feed_id: int,
        detected_schema: Dict[str, Any],
        expected_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Attempt to heal schema drift.

        Args:
            feed_id: Feed ID
            detected_schema: Schema detected in source
            expected_schema: Expected schema from metadata

        Returns:
            Healing result with actions taken
        """
        result = {
            "healed": False,
            "actions": [],
            "requires_manual_intervention": False,
        }

        # Detect type of drift
        detected_columns = set(detected_schema.get("columns", []))
        expected_columns = set(expected_schema.get("columns", []))

        new_columns = detected_columns - expected_columns
        missing_columns = expected_columns - detected_columns

        if new_columns and not missing_columns:
            # Additive change - can potentially auto-heal
            result["actions"].append(f"New columns detected: {new_columns}")
            result["healed"] = True
        elif missing_columns:
            # Breaking change - requires manual intervention
            result["actions"].append(f"Missing columns: {missing_columns}")
            result["requires_manual_intervention"] = True
        else:
            result["healed"] = True

        if self.audit:
            self.audit.log_event(
                "SCHEMA_DRIFT_HEALING",
                feed_id=feed_id,
                result=result,
            )

        return result

    def heal_missing_partition(
        self,
        feed_id: int,
        table: str,
        partition: str
    ) -> Dict[str, Any]:
        """
        Attempt to recover missing partition.

        Args:
            feed_id: Feed ID
            table: Table name
            partition: Missing partition identifier

        Returns:
            Healing result
        """
        result = {
            "healed": False,
            "action": "create_empty_partition",
        }

        # In real implementation, would create empty partition
        # or trigger backfill

        if self.audit:
            self.audit.log_event(
                "PARTITION_HEALING",
                feed_id=feed_id,
                table=table,
                partition=partition,
                result=result,
            )

        return result

    def suggest_remediation(
        self,
        error_type: str,
        error_context: Dict[str, Any]
    ) -> List[str]:
        """
        Suggest remediation actions for an error.

        Args:
            error_type: Type of error
            error_context: Context about the error

        Returns:
            List of suggested remediation actions
        """
        suggestions = []

        if error_type == "SCHEMA_MISMATCH":
            suggestions.append("Review schema changes in source system")
            suggestions.append("Update contract with new schema if intentional")
            suggestions.append("Run schema evolution if in ADAPT mode")

        elif error_type == "NO_FILES":
            suggestions.append("Verify source system is operational")
            suggestions.append("Check file landing schedule")
            suggestions.append("Verify GCS permissions")

        elif error_type == "CONNECTION_FAILED":
            suggestions.append("Check database connection credentials")
            suggestions.append("Verify network connectivity")
            suggestions.append("Check Secret Manager access")

        elif error_type == "QUALITY_FAILED":
            suggestions.append("Review failed GE expectations")
            suggestions.append("Check source data quality")
            suggestions.append("Consider adjusting quality thresholds")

        else:
            suggestions.append("Review execution logs for details")
            suggestions.append("Contact data engineering team")

        return suggestions


__all__ = ["RetryHandler", "SelfHealer"]
