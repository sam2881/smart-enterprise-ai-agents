"""
Pipeline Repository - CRUD operations for pipeline metadata.

This repository is the interface between the application and PostgreSQL.
It handles:
1. Creating/updating pipeline configurations
2. Retrieving runtime config via get_pipeline_config(dag_id, env)
3. Logging executions and events
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog
from sqlalchemy import text

from ..models.canonical import PipelineMetadata, RuntimeConfig
from ..models.pipeline import PipelineConfig, Environment
from ..models.source import SourceConfig
from ..models.schema import SchemaConfig
from ..models.target import TargetConfig
from ..models.transformation import TransformConfig
from ..models.quality import QualityRule
from ..models.execution import ExecutionPolicy, ExecutionRecord, PipelineEvent

from .connection import DatabaseConnection, get_db_connection

logger = structlog.get_logger(__name__)


class PipelineRepository:
    """
    Repository for pipeline metadata CRUD operations.

    Uses PostgreSQL functions for complex operations:
    - get_pipeline_config(dag_id, env) -> JSONB
    - register_pipeline(...) -> UUID
    - log_pipeline_execution(...) -> UUID
    """

    def __init__(self, db: Optional[DatabaseConnection] = None):
        """
        Initialize repository.

        Args:
            db: Database connection (uses global if not provided)
        """
        self.db = db or get_db_connection()

    # =========================================================================
    # Pipeline CRUD
    # =========================================================================

    async def create_pipeline(self, metadata: PipelineMetadata) -> UUID:
        """
        Create a new pipeline with all related configurations.

        Args:
            metadata: Complete pipeline metadata

        Returns:
            Created pipeline ID
        """
        async with self.db.get_async_session() as session:
            # 1. Register pipeline using PostgreSQL function
            result = await session.execute(
                text("""
                    SELECT register_pipeline(
                        :dag_id,
                        :pipeline_name,
                        :domain,
                        :product_code,
                        :owner_team,
                        :owner_email,
                        :jira_ticket,
                        :environment,
                        :description
                    )
                """),
                {
                    "dag_id": metadata.pipeline.dag_id,
                    "pipeline_name": metadata.pipeline.pipeline_name,
                    "domain": metadata.pipeline.domain,
                    "product_code": metadata.pipeline.product_code,
                    "owner_team": metadata.pipeline.owner_team,
                    "owner_email": metadata.pipeline.owner_email,
                    "jira_ticket": metadata.pipeline.jira_ticket,
                    "environment": metadata.pipeline.environment.value,
                    "description": metadata.pipeline.description,
                },
            )
            pipeline_id = result.scalar()

            # 2. Insert source configuration
            await self._insert_source(session, pipeline_id, metadata.source)

            # 3. Insert schema definition
            await self._insert_schema(session, pipeline_id, metadata.schema)

            # 4. Insert target configuration
            await self._insert_target(session, pipeline_id, metadata.target)

            # 5. Insert transformations
            for transform in metadata.transformations:
                await self._insert_transformation(session, pipeline_id, transform)

            # 6. Insert quality rules
            for rule in metadata.quality_rules:
                await self._insert_quality_rule(session, pipeline_id, rule)

            # 7. Insert execution policy
            await self._insert_execution_policy(session, pipeline_id, metadata.execution_policy)

            await session.commit()

            logger.info(
                "pipeline_created",
                pipeline_id=str(pipeline_id),
                dag_id=metadata.pipeline.dag_id,
            )

            return pipeline_id

    async def get_pipeline_config(
        self,
        dag_id: str,
        environment: str = "dev",
    ) -> Optional[RuntimeConfig]:
        """
        Get pipeline configuration for DAG runtime.

        This is the main function DAGs call to get their configuration.

        Args:
            dag_id: DAG identifier
            environment: Environment (dev, qa, prod)

        Returns:
            RuntimeConfig or None if not found
        """
        result = await self.db.execute_function(
            "get_pipeline_config",
            dag_id,
            environment,
        )

        if not result:
            logger.warning("pipeline_config_not_found", dag_id=dag_id, env=environment)
            return None

        return RuntimeConfig.from_postgres_json(result)

    async def get_pipeline_by_dag_id(
        self,
        dag_id: str,
        environment: str = "dev",
    ) -> Optional[PipelineMetadata]:
        """
        Get full pipeline metadata by DAG ID.

        Args:
            dag_id: DAG identifier
            environment: Environment

        Returns:
            PipelineMetadata or None
        """
        config = await self.get_pipeline_config(dag_id, environment)
        if not config:
            return None

        return PipelineMetadata(
            pipeline=PipelineConfig(**config.pipeline),
            source=SourceConfig(**config.source) if config.source else None,
            schema=SchemaConfig(**config.schema) if config.schema else None,
            target=TargetConfig(**config.target) if config.target else None,
            transformations=[
                TransformConfig(**t) for t in (config.transformations or [])
            ],
            quality_rules=[
                QualityRule(**r) for r in (config.quality_rules or [])
            ],
            execution_policy=ExecutionPolicy(**config.execution_policy) if config.execution_policy else None,
        )

    async def update_pipeline(
        self,
        dag_id: str,
        environment: str,
        updates: Dict[str, Any],
    ) -> bool:
        """
        Update pipeline configuration.

        Args:
            dag_id: DAG identifier
            environment: Environment
            updates: Fields to update

        Returns:
            True if updated successfully
        """
        async with self.db.get_async_session() as session:
            # Build SET clause
            set_parts = []
            params = {"dag_id": dag_id, "environment": environment}

            for key, value in updates.items():
                set_parts.append(f"{key} = :{key}")
                params[key] = value

            set_parts.append("updated_at = NOW()")

            query = text(f"""
                UPDATE pipeline_registry
                SET {', '.join(set_parts)}
                WHERE dag_id = :dag_id AND environment = :environment
            """)

            result = await session.execute(query, params)
            await session.commit()

            return result.rowcount > 0

    async def delete_pipeline(self, dag_id: str, environment: str) -> bool:
        """
        Delete pipeline and all related configurations.

        Args:
            dag_id: DAG identifier
            environment: Environment

        Returns:
            True if deleted successfully
        """
        async with self.db.get_async_session() as session:
            result = await session.execute(
                text("""
                    DELETE FROM pipeline_registry
                    WHERE dag_id = :dag_id AND environment = :environment
                """),
                {"dag_id": dag_id, "environment": environment},
            )
            await session.commit()

            if result.rowcount > 0:
                logger.info("pipeline_deleted", dag_id=dag_id, env=environment)
                return True
            return False

    async def list_pipelines(
        self,
        domain: Optional[str] = None,
        environment: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List pipelines with optional filters.

        Args:
            domain: Filter by domain
            environment: Filter by environment
            status: Filter by status
            limit: Maximum results

        Returns:
            List of pipeline summaries
        """
        async with self.db.get_async_session() as session:
            query = """
                SELECT dag_id, pipeline_name, domain, environment, status,
                       owner_team, jira_ticket, created_at, updated_at
                FROM pipeline_registry
                WHERE 1=1
            """
            params = {"limit": limit}

            if domain:
                query += " AND domain = :domain"
                params["domain"] = domain
            if environment:
                query += " AND environment = :environment"
                params["environment"] = environment
            if status:
                query += " AND status = :status"
                params["status"] = status

            query += " ORDER BY updated_at DESC LIMIT :limit"

            result = await session.execute(text(query), params)
            rows = result.fetchall()

            return [dict(row._mapping) for row in rows]

    # =========================================================================
    # Execution Tracking
    # =========================================================================

    async def log_execution(
        self,
        dag_id: str,
        dag_run_id: str,
        execution_date: datetime,
        status: str,
        records_read: int = 0,
        records_written: int = 0,
        quality_score: Optional[float] = None,
        error_message: Optional[str] = None,
    ) -> UUID:
        """
        Log pipeline execution using PostgreSQL function.

        Args:
            dag_id: DAG identifier
            dag_run_id: Airflow DAG run ID
            execution_date: Execution timestamp
            status: Execution status
            records_read: Records read
            records_written: Records written
            quality_score: Data quality score
            error_message: Error message if failed

        Returns:
            Execution ID
        """
        result = await self.db.execute_function(
            "log_pipeline_execution",
            dag_id,
            dag_run_id,
            execution_date.isoformat(),
            status,
            records_read,
            records_written,
            quality_score,
            error_message,
        )

        logger.info(
            "execution_logged",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            status=status,
        )

        return result

    async def log_event(
        self,
        dag_id: str,
        event_type: str,
        event_data: Dict[str, Any],
        execution_date: Optional[str] = None,
        jira_ticket: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> UUID:
        """
        Log pipeline event for audit trail.

        Args:
            dag_id: DAG identifier
            event_type: Event type (STARTED, SUCCESS, FAILED, etc.)
            event_data: Event details
            execution_date: Execution date
            jira_ticket: Related Jira ticket
            environment: Environment

        Returns:
            Event ID
        """
        async with self.db.get_async_session() as session:
            result = await session.execute(
                text("""
                    INSERT INTO pipeline_events (
                        dag_id, event_type, event_data,
                        execution_date, jira_ticket, environment
                    ) VALUES (
                        :dag_id, :event_type, :event_data,
                        :execution_date, :jira_ticket, :environment
                    )
                    RETURNING event_id
                """),
                {
                    "dag_id": dag_id,
                    "event_type": event_type,
                    "event_data": json.dumps(event_data),
                    "execution_date": execution_date,
                    "jira_ticket": jira_ticket,
                    "environment": environment,
                },
            )
            event_id = result.scalar()
            await session.commit()

            return event_id

    # =========================================================================
    # Watermark Management (for incremental loads)
    # =========================================================================

    async def get_watermark(
        self,
        dag_id: str,
        environment: str = "dev",
    ) -> Optional[str]:
        """
        Get last watermark for incremental loads.

        Args:
            dag_id: DAG identifier
            environment: Environment

        Returns:
            Last watermark value or None
        """
        async with self.db.get_async_session() as session:
            result = await session.execute(
                text("""
                    SELECT last_watermark
                    FROM pipeline_watermarks
                    WHERE dag_id = :dag_id AND environment = :environment
                """),
                {"dag_id": dag_id, "environment": environment},
            )
            row = result.fetchone()
            return row[0] if row else None

    async def update_watermark(
        self,
        dag_id: str,
        environment: str,
        watermark: str,
    ) -> None:
        """
        Update watermark for incremental loads.

        Args:
            dag_id: DAG identifier
            environment: Environment
            watermark: New watermark value
        """
        async with self.db.get_async_session() as session:
            await session.execute(
                text("""
                    INSERT INTO pipeline_watermarks (dag_id, environment, last_watermark)
                    VALUES (:dag_id, :environment, :watermark)
                    ON CONFLICT (dag_id, environment)
                    DO UPDATE SET last_watermark = :watermark, updated_at = NOW()
                """),
                {"dag_id": dag_id, "environment": environment, "watermark": watermark},
            )
            await session.commit()

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    async def _insert_source(
        self,
        session,
        pipeline_id: UUID,
        source: SourceConfig,
    ) -> UUID:
        """Insert source configuration."""
        config = source.get_active_config()
        config_dict = config.model_dump(mode="json") if config else {}

        result = await session.execute(
            text("""
                INSERT INTO pipeline_sources (
                    pipeline_id, source_type, source_format,
                    source_bucket, source_prefix, file_pattern,
                    delimiter, has_header, encoding,
                    connection_id, source_schema, source_table, source_query,
                    extraction_mode, watermark_column,
                    kafka_bootstrap_servers, kafka_topic, kafka_consumer_group,
                    pubsub_subscription,
                    api_endpoint, api_method, api_headers, api_pagination_type,
                    dtsx_location, dtsx_package_name, original_server,
                    field_specs, copybook, record_length
                ) VALUES (
                    :pipeline_id, :source_type, :source_format,
                    :source_bucket, :source_prefix, :file_pattern,
                    :delimiter, :has_header, :encoding,
                    :connection_id, :source_schema, :source_table, :source_query,
                    :extraction_mode, :watermark_column,
                    :kafka_bootstrap_servers, :kafka_topic, :kafka_consumer_group,
                    :pubsub_subscription,
                    :api_endpoint, :api_method, :api_headers, :api_pagination_type,
                    :dtsx_location, :dtsx_package_name, :original_server,
                    :field_specs, :copybook, :record_length
                )
                RETURNING source_id
            """),
            {
                "pipeline_id": pipeline_id,
                "source_type": source.source_type.value,
                "source_format": source.source_format.value if source.source_format else None,
                "source_bucket": config_dict.get("source_bucket"),
                "source_prefix": config_dict.get("source_prefix"),
                "file_pattern": config_dict.get("file_pattern", "*"),
                "delimiter": config_dict.get("delimiter", ","),
                "has_header": config_dict.get("has_header", True),
                "encoding": config_dict.get("encoding", "utf-8"),
                "connection_id": config_dict.get("connection_id"),
                "source_schema": config_dict.get("source_schema"),
                "source_table": config_dict.get("source_table"),
                "source_query": config_dict.get("source_query"),
                "extraction_mode": config_dict.get("extraction_mode", "full"),
                "watermark_column": config_dict.get("watermark_column"),
                "kafka_bootstrap_servers": config_dict.get("kafka_bootstrap_servers"),
                "kafka_topic": config_dict.get("kafka_topic"),
                "kafka_consumer_group": config_dict.get("kafka_consumer_group"),
                "pubsub_subscription": config_dict.get("pubsub_subscription"),
                "api_endpoint": config_dict.get("api_endpoint"),
                "api_method": config_dict.get("api_method"),
                "api_headers": json.dumps(config_dict.get("api_headers", {})),
                "api_pagination_type": config_dict.get("pagination_type"),
                "dtsx_location": config_dict.get("dtsx_location"),
                "dtsx_package_name": config_dict.get("dtsx_package_name"),
                "original_server": config_dict.get("original_server"),
                "field_specs": json.dumps(source.field_specs),
                "copybook": json.dumps(source.copybook),
                "record_length": source.record_length,
            },
        )
        return result.scalar()

    async def _insert_schema(
        self,
        session,
        pipeline_id: UUID,
        schema: SchemaConfig,
    ) -> UUID:
        """Insert schema definition."""
        columns_json = [col.model_dump(mode="json") for col in schema.columns]

        result = await session.execute(
            text("""
                INSERT INTO pipeline_schemas (
                    pipeline_id, schema_version, is_current,
                    columns, primary_keys, partition_columns, cluster_columns,
                    created_by
                ) VALUES (
                    :pipeline_id, :schema_version, :is_current,
                    :columns, :primary_keys, :partition_columns, :cluster_columns,
                    :created_by
                )
                RETURNING schema_id
            """),
            {
                "pipeline_id": pipeline_id,
                "schema_version": schema.schema_version,
                "is_current": schema.is_current,
                "columns": json.dumps(columns_json),
                "primary_keys": json.dumps(schema.primary_keys),
                "partition_columns": json.dumps(schema.partition_columns),
                "cluster_columns": json.dumps(schema.cluster_columns),
                "created_by": schema.created_by or "system",
            },
        )
        return result.scalar()

    async def _insert_target(
        self,
        session,
        pipeline_id: UUID,
        target: TargetConfig,
    ) -> UUID:
        """Insert target configuration."""
        result = await session.execute(
            text("""
                INSERT INTO pipeline_targets (
                    pipeline_id, target_zone,
                    bq_project, bq_dataset, bq_table, bq_location,
                    write_mode, merge_keys, destination_model,
                    hub_table, sat_table, link_tables
                ) VALUES (
                    :pipeline_id, :target_zone,
                    :bq_project, :bq_dataset, :bq_table, :bq_location,
                    :write_mode, :merge_keys, :destination_model,
                    :hub_table, :sat_table, :link_tables
                )
                RETURNING target_id
            """),
            {
                "pipeline_id": pipeline_id,
                "target_zone": target.target_zone.value,
                "bq_project": target.bq_project,
                "bq_dataset": target.bq_dataset,
                "bq_table": target.bq_table,
                "bq_location": target.bq_location,
                "write_mode": target.write_mode.value,
                "merge_keys": json.dumps(target.merge_keys),
                "destination_model": target.destination_model.value,
                "hub_table": target.hub_table,
                "sat_table": target.sat_table,
                "link_tables": json.dumps(target.link_tables),
            },
        )
        return result.scalar()

    async def _insert_transformation(
        self,
        session,
        pipeline_id: UUID,
        transform: TransformConfig,
    ) -> UUID:
        """Insert transformation rule."""
        result = await session.execute(
            text("""
                INSERT INTO pipeline_transformations (
                    pipeline_id, transform_type, transform_order, zone,
                    config, nl_description, generated_pyspark, generated_sql,
                    is_active
                ) VALUES (
                    :pipeline_id, :transform_type, :transform_order, :zone,
                    :config, :nl_description, :generated_pyspark, :generated_sql,
                    :is_active
                )
                RETURNING transform_id
            """),
            {
                "pipeline_id": pipeline_id,
                "transform_type": transform.transform_type.value,
                "transform_order": transform.transform_order,
                "zone": transform.zone.value,
                "config": json.dumps(transform.config),
                "nl_description": transform.nl_description,
                "generated_pyspark": transform.generated_pyspark,
                "generated_sql": transform.generated_sql,
                "is_active": transform.is_active,
            },
        )
        return result.scalar()

    async def _insert_quality_rule(
        self,
        session,
        pipeline_id: UUID,
        rule: QualityRule,
    ) -> UUID:
        """Insert quality rule."""
        result = await session.execute(
            text("""
                INSERT INTO pipeline_quality_rules (
                    pipeline_id, rule_name, rule_type, column_name,
                    config, severity, threshold_pct, is_active
                ) VALUES (
                    :pipeline_id, :rule_name, :rule_type, :column_name,
                    :config, :severity, :threshold_pct, :is_active
                )
                RETURNING rule_id
            """),
            {
                "pipeline_id": pipeline_id,
                "rule_name": rule.rule_name,
                "rule_type": rule.rule_type.value,
                "column_name": rule.column_name,
                "config": json.dumps(rule.config),
                "severity": rule.severity.value,
                "threshold_pct": rule.threshold_pct,
                "is_active": rule.is_active,
            },
        )
        return result.scalar()

    async def _insert_execution_policy(
        self,
        session,
        pipeline_id: UUID,
        policy: ExecutionPolicy,
    ) -> UUID:
        """Insert execution policy."""
        result = await session.execute(
            text("""
                INSERT INTO pipeline_execution_policies (
                    pipeline_id, schedule_interval, start_date, end_date, catchup,
                    processing_mode, max_active_runs,
                    retry_count, retry_delay_minutes, timeout_hours,
                    sla_hours, requires_human_approval, approval_groups,
                    alert_emails, slack_webhook
                ) VALUES (
                    :pipeline_id, :schedule_interval, :start_date, :end_date, :catchup,
                    :processing_mode, :max_active_runs,
                    :retry_count, :retry_delay_minutes, :timeout_hours,
                    :sla_hours, :requires_human_approval, :approval_groups,
                    :alert_emails, :slack_webhook
                )
                ON CONFLICT (pipeline_id) DO UPDATE SET
                    schedule_interval = EXCLUDED.schedule_interval,
                    processing_mode = EXCLUDED.processing_mode,
                    retry_count = EXCLUDED.retry_count,
                    requires_human_approval = EXCLUDED.requires_human_approval,
                    updated_at = NOW()
                RETURNING policy_id
            """),
            {
                "pipeline_id": pipeline_id,
                "schedule_interval": policy.schedule_interval,
                "start_date": policy.start_date,
                "end_date": policy.end_date,
                "catchup": policy.catchup,
                "processing_mode": policy.processing_mode.value,
                "max_active_runs": policy.max_active_runs,
                "retry_count": policy.retry_count,
                "retry_delay_minutes": policy.retry_delay_minutes,
                "timeout_hours": policy.timeout_hours,
                "sla_hours": policy.sla_hours,
                "requires_human_approval": policy.requires_human_approval,
                "approval_groups": json.dumps(policy.approval_groups),
                "alert_emails": json.dumps([str(e) for e in policy.alert_emails]),
                "slack_webhook": policy.slack_webhook,
            },
        )
        return result.scalar()
