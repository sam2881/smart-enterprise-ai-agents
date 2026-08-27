"""
DAG Utilities Core Module

Core classes for APEX DAG patterns:
- MetadataClient: PostgreSQL metadata database interface
- ExecutionContext: Airflow execution context wrapper
- APEXConfig: Environment configuration
"""

import os
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import json
import hashlib


@dataclass
class APEXConfig:
    """
    APEX environment configuration.

    Loads configuration from environment variables with sensible defaults.
    """
    # GCP Configuration
    gcp_project: str = field(default_factory=lambda: os.getenv("GCP_PROJECT", "apex-project"))
    gcp_region: str = field(default_factory=lambda: os.getenv("GCP_REGION", "us-central1"))
    dataproc_region: str = field(default_factory=lambda: os.getenv("DATAPROC_REGION", "us-central1"))

    # Metadata Database
    metadata_db_url: str = field(default_factory=lambda: os.getenv(
        "METADATA_DB_URL",
        "postgresql://apex:apex@localhost:5432/apex_metadata"
    ))

    # GCS Buckets
    landing_bucket: str = field(default_factory=lambda: os.getenv("LANDING_BUCKET", "apex-landing"))
    archive_bucket: str = field(default_factory=lambda: os.getenv("ARCHIVE_BUCKET", "apex-archive"))
    spark_jobs_bucket: str = field(default_factory=lambda: os.getenv("SPARK_JOBS_BUCKET", "apex-spark-jobs"))

    # Environment
    environment: str = field(default_factory=lambda: os.getenv("APEX_ENVIRONMENT", "dev"))

    @classmethod
    def from_environment(cls) -> "APEXConfig":
        """Create config from environment variables."""
        return cls()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "gcp_project": self.gcp_project,
            "gcp_region": self.gcp_region,
            "dataproc_region": self.dataproc_region,
            "metadata_db_url": self.metadata_db_url,
            "landing_bucket": self.landing_bucket,
            "archive_bucket": self.archive_bucket,
            "spark_jobs_bucket": self.spark_jobs_bucket,
            "environment": self.environment,
        }


@dataclass
class ExecutionContext:
    """
    Execution context for a pipeline run.

    Wraps Airflow context and provides standardized access to
    execution identifiers.
    """
    execution_id: str
    dag_id: str
    run_id: str
    batch_id: str
    execution_date: datetime
    logical_date: datetime

    @classmethod
    def from_airflow_context(cls, context: Dict[str, Any]) -> "ExecutionContext":
        """Create ExecutionContext from Airflow task context."""
        dag_run = context.get("dag_run")
        execution_date = context.get("execution_date") or context.get("logical_date")

        # Generate batch_id as YYYYMMDD
        if isinstance(execution_date, datetime):
            batch_id = execution_date.strftime("%Y%m%d")
        else:
            batch_id = datetime.now().strftime("%Y%m%d")

        # Generate unique execution_id
        run_id = dag_run.run_id if dag_run else context.get("run_id", "manual")
        execution_id = f"{context.get('dag').dag_id}_{run_id}"

        return cls(
            execution_id=execution_id,
            dag_id=context.get("dag").dag_id if context.get("dag") else "unknown",
            run_id=run_id,
            batch_id=batch_id,
            execution_date=execution_date or datetime.now(),
            logical_date=context.get("logical_date", datetime.now()),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "execution_date": self.execution_date.isoformat(),
            "logical_date": self.logical_date.isoformat(),
        }


class MetadataClient:
    """
    PostgreSQL Metadata Database Client.

    Provides interface to the APEX metadata database for:
    - Feed configuration retrieval
    - Contract and schema management
    - Execution plan persistence
    - Health score tracking
    - Watermark management
    """

    def __init__(self, db_url: str):
        """Initialize with database URL."""
        self.db_url = db_url
        self._connection = None

    def _get_connection(self):
        """Get database connection (lazy initialization)."""
        if self._connection is None:
            try:
                import psycopg2
                self._connection = psycopg2.connect(self.db_url)
            except ImportError:
                # Fallback for testing without psycopg2
                self._connection = None
        return self._connection

    def get_feed_group(self, feed_group_id: int) -> Dict[str, Any]:
        """
        Get feed group configuration.

        Args:
            feed_group_id: Numeric feed group ID

        Returns:
            Feed group configuration dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return {"feed_group_id": feed_group_id, "feed_group_name": f"group_{feed_group_id}"}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM feed_group_details WHERE feed_group_id = %s",
            (feed_group_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}

    def get_feeds_by_group(self, feed_group_id: int) -> List[Any]:
        """
        Get all feeds in a feed group.

        Args:
            feed_group_id: Numeric feed group ID

        Returns:
            List of feed configurations
        """
        conn = self._get_connection()
        if conn is None:
            return []

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM feed_details WHERE feed_group_id = %s AND is_active = true ORDER BY feed_id",
            (feed_group_id,)
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return [type("Feed", (), dict(zip(columns, row)))() for row in rows]

    def get_feed_config(self, feed_id: int) -> Dict[str, Any]:
        """
        Get feed configuration.

        Args:
            feed_id: Numeric feed ID

        Returns:
            Feed configuration dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return {"feed_id": feed_id, "feed_name": f"feed_{feed_id}"}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM feed_details WHERE feed_id = %s",
            (feed_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}

    def get_contract_metadata(self, contract_id: int) -> Dict[str, Any]:
        """
        Get contract metadata including expectations and transformations.

        Args:
            contract_id: Numeric contract ID

        Returns:
            Contract metadata dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return {"contract_id": contract_id, "transformations": [], "expectations": []}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM contract_details WHERE contract_id = %s",
            (contract_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        """
        Get schema definition.

        Args:
            schema_id: Numeric schema ID

        Returns:
            Schema definition with columns
        """
        conn = self._get_connection()
        if conn is None:
            return {"schema_id": schema_id, "columns": []}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM schema_details WHERE schema_id = %s",
            (schema_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}

    def get_last_watermark(self, feed_id: int) -> Optional[str]:
        """
        Get last watermark value for incremental loads.

        Args:
            feed_id: Numeric feed ID

        Returns:
            Last watermark value or None
        """
        conn = self._get_connection()
        if conn is None:
            return None

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT current_watermark
            FROM feed_ingestion_details
            WHERE feed_id = %s AND status = 'success'
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            (feed_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def update_watermark(self, feed_id: int, watermark: str) -> None:
        """
        Update watermark for a feed.

        Args:
            feed_id: Numeric feed ID
            watermark: New watermark value
        """
        conn = self._get_connection()
        if conn is None:
            return

        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE feed_details
            SET last_watermark = %s, updated_at = NOW()
            WHERE feed_id = %s
            """,
            (watermark, feed_id)
        )
        conn.commit()

    def save_execution_plan(self, execution_id: str, plan: Dict[str, Any]) -> None:
        """
        Save execution plan for audit and data plane access.

        Args:
            execution_id: Unique execution ID
            plan: Execution plan dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO execution_plans (execution_id, plan_json, created_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (execution_id) DO UPDATE SET plan_json = %s, updated_at = NOW()
            """,
            (execution_id, json.dumps(plan), json.dumps(plan))
        )
        conn.commit()

    def get_execution_plan(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve execution plan by ID.

        Args:
            execution_id: Unique execution ID

        Returns:
            Execution plan dictionary or None
        """
        conn = self._get_connection()
        if conn is None:
            return None

        cursor = conn.cursor()
        cursor.execute(
            "SELECT plan_json FROM execution_plans WHERE execution_id = %s",
            (execution_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def save_health_score(
        self,
        feed_id: int,
        execution_id: str,
        health_score: Any
    ) -> None:
        """
        Save pipeline health score.

        Args:
            feed_id: Numeric feed ID
            execution_id: Execution ID
            health_score: Health score object with component scores
        """
        conn = self._get_connection()
        if conn is None:
            return

        score_dict = health_score.to_dict() if hasattr(health_score, "to_dict") else health_score

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_health_scores (
                feed_id, execution_id, quality_score, freshness_score,
                stability_score, cost_efficiency_score, overall_score,
                calculated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                feed_id,
                execution_id,
                score_dict.get("quality_score", 0),
                score_dict.get("freshness_score", 0),
                score_dict.get("stability_score", 0),
                score_dict.get("cost_efficiency_score", 0),
                score_dict.get("overall_score", 0),
            )
        )
        conn.commit()

    def update_ingestion_status(
        self,
        feed_id: int = None,
        feed_group_id: int = None,
        execution_id: str = None,
        status: str = "success"
    ) -> None:
        """
        Update ingestion status for a feed or feed group.

        Args:
            feed_id: Optional feed ID
            feed_group_id: Optional feed group ID
            execution_id: Execution ID
            status: Status string (success, failed, etc.)
        """
        conn = self._get_connection()
        if conn is None:
            return

        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE feed_ingestion_details
            SET status = %s, completed_at = NOW()
            WHERE execution_id = %s
            """,
            (status, execution_id)
        )
        conn.commit()

    def get_source_config(self, source_id: int) -> Dict[str, Any]:
        """
        Get source configuration.

        Args:
            source_id: Numeric source ID

        Returns:
            Source configuration dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return {"source_id": source_id}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM platform_source_registry WHERE source_id = %s",
            (source_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}

    # ═══════════════════════════════════════════════════════════════════════════
    # NEW: Zone Processing Methods (for 5-zone medallion architecture)
    # ═══════════════════════════════════════════════════════════════════════════

    def get_zone_config(self, feed_id: int, zone: str) -> Dict[str, Any]:
        """
        Get zone configuration for a specific zone.

        Args:
            feed_id: Numeric feed ID
            zone: Zone level (transient, raw, silver, gold, consumption)

        Returns:
            Zone configuration dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return self._default_zone_config(feed_id, zone)

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM zone_configurations
            WHERE feed_id = %s AND zone_level = %s AND is_active = true
            """,
            (feed_id, zone)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return self._default_zone_config(feed_id, zone)

    def _default_zone_config(self, feed_id: int, zone: str) -> Dict[str, Any]:
        """Return default zone config for testing."""
        return {
            "feed_id": feed_id,
            "zone_level": zone,
            "dataset_name": f"{zone}_data",
            "table_name": f"feed_{feed_id}_{zone}",
            "write_mode": "append" if zone in ["transient", "raw"] else "merge",
            "partition_cols": ["batch_id"] if zone in ["transient", "raw"] else ["ingestion_date"],
            "source_path": f"gs://apex-{zone}/feed_{feed_id}/{{batch_id}}/",
            "target_path": f"gs://apex-{zone}/feed_{feed_id}/{{batch_id}}/",
            "source_format": "parquet",
            "target_format": "parquet",
            "fail_on_schema_error": True,
        }

    def get_zone_configs(self, feed_id: int) -> List[Dict[str, Any]]:
        """
        Get all zone configurations for a feed.

        Args:
            feed_id: Numeric feed ID

        Returns:
            List of zone configurations ordered by zone level
        """
        conn = self._get_connection()
        if conn is None:
            return [
                self._default_zone_config(feed_id, zone)
                for zone in ["transient", "raw", "silver", "gold", "consumption"]
            ]

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM zone_configurations
            WHERE feed_id = %s AND is_active = true
            ORDER BY CASE zone_level
                WHEN 'transient' THEN 1
                WHEN 'raw' THEN 2
                WHEN 'silver' THEN 3
                WHEN 'gold' THEN 4
                WHEN 'consumption' THEN 5
                ELSE 6
            END
            """,
            (feed_id,)
        )
        rows = cursor.fetchall()
        if rows:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return []

    def get_transforms(self, feed_id: int, zone: str = None) -> List[Dict[str, Any]]:
        """
        Get transformations for a feed, optionally filtered by zone.

        Args:
            feed_id: Numeric feed ID
            zone: Optional zone level filter

        Returns:
            List of transform configurations ordered by sequence
        """
        conn = self._get_connection()
        if conn is None:
            return []

        cursor = conn.cursor()
        if zone:
            cursor.execute(
                """
                SELECT * FROM transform_definitions
                WHERE feed_id = %s AND zone_level = %s AND is_active = true
                ORDER BY sequence_order
                """,
                (feed_id, zone)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM transform_definitions
                WHERE feed_id = %s AND is_active = true
                ORDER BY zone_level, sequence_order
                """,
                (feed_id,)
            )
        rows = cursor.fetchall()
        if rows:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return []

    def get_quality_rules(self, feed_id: int, zone: str = None) -> List[Dict[str, Any]]:
        """
        Get quality rules for a feed, optionally filtered by zone.

        Args:
            feed_id: Numeric feed ID
            zone: Optional zone level filter

        Returns:
            List of quality rule configurations
        """
        conn = self._get_connection()
        if conn is None:
            return []

        cursor = conn.cursor()
        if zone:
            cursor.execute(
                """
                SELECT * FROM quality_rules
                WHERE feed_id = %s AND zone_level = %s AND is_active = true
                """,
                (feed_id, zone)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM quality_rules
                WHERE feed_id = %s AND is_active = true
                ORDER BY zone_level, rule_id
                """,
                (feed_id,)
            )
        rows = cursor.fetchall()
        if rows:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return []

    def get_encryption_config(self, feed_id: int, zone: str = None) -> Optional[Dict[str, Any]]:
        """
        Get encryption configuration for FPE/hash encryption.

        Args:
            feed_id: Numeric feed ID
            zone: Optional zone level filter

        Returns:
            Encryption configuration or None
        """
        conn = self._get_connection()
        if conn is None:
            return None

        cursor = conn.cursor()
        if zone:
            cursor.execute(
                """
                SELECT * FROM encryption_configs
                WHERE feed_id = %s AND zone_level = %s AND is_active = true
                """,
                (feed_id, zone)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM encryption_configs
                WHERE feed_id = %s AND is_active = true
                """,
                (feed_id,)
            )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None

    def get_contract(self, feed_id: int) -> Dict[str, Any]:
        """
        Get contract details for a feed.

        Args:
            feed_id: Numeric feed ID

        Returns:
            Contract configuration dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return {"feed_id": feed_id, "contract_type": "STANDARD"}

        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM contract_details WHERE feed_id = %s AND is_active = true",
            (feed_id,)
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {"feed_id": feed_id, "contract_type": "STANDARD"}

    def get_feed(self, feed_id: int) -> Dict[str, Any]:
        """Alias for get_feed_config."""
        return self.get_feed_config(feed_id)

    def record_zone_metrics(
        self,
        feed_id: int,
        zone: str,
        batch_id: str,
        metrics: Dict[str, Any]
    ) -> None:
        """
        Record zone processing metrics.

        Args:
            feed_id: Numeric feed ID
            zone: Zone level
            batch_id: Batch identifier
            metrics: Metrics dictionary
        """
        conn = self._get_connection()
        if conn is None:
            return

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO zone_execution_metrics (
                feed_id, zone_level, batch_id, records_in, records_out,
                schema_validation, semantic_validation, validation_score, completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (feed_id, zone_level, batch_id) DO UPDATE SET
                records_in = EXCLUDED.records_in,
                records_out = EXCLUDED.records_out,
                schema_validation = EXCLUDED.schema_validation,
                semantic_validation = EXCLUDED.semantic_validation,
                validation_score = EXCLUDED.validation_score,
                completed_at = NOW()
            """,
            (
                feed_id,
                zone,
                batch_id,
                metrics.get("records_in", 0),
                metrics.get("records_out", 0),
                metrics.get("schema_validation", True),
                metrics.get("semantic_validation", True),
                metrics.get("validation_score", 100),
            )
        )
        conn.commit()


class ExecutionTracker:
    """
    Execution Tracker for pipeline runs.

    Provides static methods for DAG initialization and finalization.
    """

    @staticmethod
    def initialize(feed_id: int, metadata_url: str, **context) -> Dict[str, Any]:
        """
        Initialize execution tracking for a pipeline run.

        Args:
            feed_id: Numeric feed ID
            metadata_url: PostgreSQL connection URL
            **context: Airflow context

        Returns:
            Execution context dictionary
        """
        meta = MetadataClient(metadata_url)
        exec_ctx = ExecutionContext.from_airflow_context(context) if context else None

        execution_id = exec_ctx.execution_id if exec_ctx else f"feed_{feed_id}_manual"
        batch_id = exec_ctx.batch_id if exec_ctx else datetime.now().strftime("%Y%m%d")

        # Record execution start
        conn = meta._get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO feed_ingestion_details (
                    feed_id, execution_id, batch_id, status, started_at
                ) VALUES (%s, %s, %s, 'running', NOW())
                ON CONFLICT (execution_id) DO UPDATE SET
                    status = 'running', started_at = NOW()
                """,
                (feed_id, execution_id, batch_id)
            )
            conn.commit()

        return {
            "feed_id": feed_id,
            "execution_id": execution_id,
            "batch_id": batch_id,
            "status": "running",
        }

    @staticmethod
    def finalize(feed_id: int, status: str = "success", **context) -> Dict[str, Any]:
        """
        Finalize execution tracking for a pipeline run.

        Args:
            feed_id: Numeric feed ID
            status: Final status (success, failed)
            **context: Airflow context

        Returns:
            Finalization result dictionary
        """
        return {
            "feed_id": feed_id,
            "status": status,
            "finalized_at": datetime.now().isoformat(),
        }


__all__ = ["APEXConfig", "ExecutionContext", "MetadataClient", "ExecutionTracker"]
