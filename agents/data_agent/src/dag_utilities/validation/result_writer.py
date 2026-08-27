"""
Validation Result Writer - Write GE Results Back to PostgreSQL

Writes validation results from Great Expectations runs back to the
metadata database for:
- Historical tracking of validation outcomes
- Pipeline health score calculation
- Alerting on validation failures
- Audit trail for compliance

Target tables:
- validation_run_log: Summary of each validation run
- validation_result_details: Per-expectation results
- validation_metrics: Aggregated metrics per feed/zone
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import json
import logging

logger = logging.getLogger(__name__)


class ValidationResultWriter:
    """
    Write GE validation results to PostgreSQL metadata database.

    Results are stored in three tables:
    1. validation_run_log - Summary per run (feed_id, zone, pass/fail, score)
    2. validation_result_details - Per-expectation results
    3. validation_metrics - Aggregated historical metrics
    """

    def __init__(self, metadata_client: Any):
        """
        Initialize result writer.

        Args:
            metadata_client: MetadataClient for database writes
        """
        self.meta = metadata_client

    def write_run_log(
        self,
        feed_id: int,
        zone: str,
        batch_id: str,
        suite_name: str,
        success: bool,
        success_rate: float,
        total_expectations: int,
        passed_expectations: int,
        failed_expectations: int,
        execution_time_ms: int = 0,
        checksum: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Write validation run summary to validation_run_log.

        Returns:
            Generated SQL INSERT statement and values
        """
        record = {
            "feed_id": feed_id,
            "zone_level": zone,
            "batch_id": batch_id,
            "suite_name": suite_name,
            "success": success,
            "success_rate": success_rate,
            "total_expectations": total_expectations,
            "passed_expectations": passed_expectations,
            "failed_expectations": failed_expectations,
            "execution_time_ms": execution_time_ms,
            "suite_checksum": checksum,
            "validated_at": datetime.utcnow().isoformat(),
        }

        sql = self._generate_insert_sql("validation_run_log", record)

        # Write to metadata database
        if hasattr(self.meta, 'execute_sql'):
            self.meta.execute_sql(sql, record)

        logger.info(
            f"Validation run logged: feed_id={feed_id}, zone={zone}, "
            f"success={success}, rate={success_rate:.2%}"
        )

        return {"sql": sql, "values": record}

    def write_result_details(
        self,
        feed_id: int,
        zone: str,
        batch_id: str,
        suite_name: str,
        failed_details: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Write per-expectation failure details to validation_result_details.

        Returns:
            List of generated SQL INSERT statements
        """
        results = []

        for detail in failed_details:
            record = {
                "feed_id": feed_id,
                "zone_level": zone,
                "batch_id": batch_id,
                "suite_name": suite_name,
                "expectation_type": detail.get("expectation_type", ""),
                "kwargs": json.dumps(detail.get("kwargs", {})),
                "observed_value": str(detail.get("observed_value", "")),
                "success": False,
                "severity": detail.get("meta", {}).get("severity", "ERROR"),
                "validated_at": datetime.utcnow().isoformat(),
            }

            sql = self._generate_insert_sql("validation_result_details", record)

            if hasattr(self.meta, 'execute_sql'):
                self.meta.execute_sql(sql, record)

            results.append({"sql": sql, "values": record})

        if failed_details:
            logger.info(
                f"Wrote {len(failed_details)} failure details: "
                f"feed_id={feed_id}, zone={zone}"
            )

        return results

    def write_metrics(
        self,
        feed_id: int,
        zone: str,
        batch_id: str,
        metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Write aggregated validation metrics to validation_metrics.

        Used for health score calculation and trend analysis.
        """
        record = {
            "feed_id": feed_id,
            "zone_level": zone,
            "batch_id": batch_id,
            "schema_validation_score": metrics.get("schema_score", 100.0),
            "semantic_validation_score": metrics.get("semantic_score", 100.0),
            "combined_score": metrics.get("combined_score", 100.0),
            "records_validated": metrics.get("records_validated", 0),
            "records_passed": metrics.get("records_passed", 0),
            "records_failed": metrics.get("records_failed", 0),
            "validated_at": datetime.utcnow().isoformat(),
        }

        sql = self._generate_insert_sql("validation_metrics", record)

        if hasattr(self.meta, 'execute_sql'):
            self.meta.execute_sql(sql, record)

        return {"sql": sql, "values": record}

    def write_validation_result(
        self,
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Write a complete ValidationResult dict (from GEValidator).

        Convenience method that writes to all three tables.
        """
        feed_id = result.get("feed_id", 0)
        zone = result.get("zone", "")
        batch_id = result.get("batch_id", "")
        suite_name = result.get("suite_name", "")

        # Write run log
        run_log = self.write_run_log(
            feed_id=feed_id,
            zone=zone,
            batch_id=batch_id,
            suite_name=suite_name,
            success=result.get("success", True),
            success_rate=result.get("success_rate", 1.0),
            total_expectations=result.get("total_expectations", 0),
            passed_expectations=result.get("passed_expectations", 0),
            failed_expectations=result.get("failed_expectations", 0),
            execution_time_ms=result.get("execution_time_ms", 0),
            checksum=result.get("checksum"),
        )

        # Write failure details
        details = self.write_result_details(
            feed_id=feed_id,
            zone=zone,
            batch_id=batch_id,
            suite_name=suite_name,
            failed_details=result.get("failed_details", []),
        )

        # Write aggregated metrics
        metrics = self.write_metrics(
            feed_id=feed_id,
            zone=zone,
            batch_id=batch_id,
            metrics={
                "combined_score": result.get("success_rate", 1.0) * 100,
                "records_validated": result.get("total_expectations", 0),
                "records_passed": result.get("passed_expectations", 0),
                "records_failed": result.get("failed_expectations", 0),
            },
        )

        return {
            "run_log": run_log,
            "details_count": len(details),
            "metrics": metrics,
        }

    def _generate_insert_sql(
        self,
        table_name: str,
        record: Dict[str, Any]
    ) -> str:
        """Generate INSERT SQL statement."""
        columns = ", ".join(record.keys())
        placeholders = ", ".join([f"%({k})s" for k in record.keys()])

        return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"


__all__ = [
    "ValidationResultWriter",
]
