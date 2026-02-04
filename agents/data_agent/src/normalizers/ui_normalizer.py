"""
UI Input Normalizer - Converts structured UI input to PipelineMetadata.

This is the primary normalizer for validated JSON from the frontend.
"""

import structlog
from typing import Any, Dict, List, Optional

from ..models.canonical import PipelineMetadata, UnifiedPipelineInput, InputType
from ..models.pipeline import PipelineConfig, Environment
from ..models.source import (
    SourceConfig,
    SourceType,
    SourceFormat,
    FileSourceConfig,
    DatabaseSourceConfig,
    StreamingSourceConfig,
    APISourceConfig,
    EBCDICSourceConfig,
)
from ..models.schema import SchemaConfig, ColumnDefinition, DataType
from ..models.target import TargetConfig, TargetZone, WriteMode, DestinationModel
from ..models.transformation import TransformConfig, TransformType
from ..models.quality import QualityRule, QualityRuleType, Severity
from ..models.execution import ExecutionPolicy, ProcessingMode

from .base import BaseNormalizer, NormalizerResult

logger = structlog.get_logger(__name__)


class UIInputNormalizer(BaseNormalizer):
    """
    Normalizes structured UI input to PipelineMetadata.

    The frontend sends validated JSON that maps directly to our models.
    This normalizer validates and converts that JSON.
    """

    def can_handle(self, input_data: UnifiedPipelineInput) -> bool:
        """Check if input is structured UI data."""
        return (
            input_data.input_type == InputType.UI_STRUCTURED and
            (input_data.pipeline is not None or input_data.source is not None)
        )

    async def normalize(self, input_data: UnifiedPipelineInput) -> NormalizerResult:
        """
        Normalize UI input to PipelineMetadata.

        Args:
            input_data: Structured input from UI

        Returns:
            NormalizerResult with PipelineMetadata
        """
        processing_log = []
        errors = []
        warnings = []

        try:
            # 1. Parse pipeline configuration
            processing_log.append({"step": "parse_pipeline", "status": "started"})
            pipeline = self._parse_pipeline(input_data.pipeline or {})
            processing_log.append({"step": "parse_pipeline", "status": "completed"})

            # 2. Parse source configuration
            processing_log.append({"step": "parse_source", "status": "started"})
            source = self._parse_source(input_data.source or {})
            processing_log.append({"step": "parse_source", "status": "completed"})

            # 3. Parse schema definition
            processing_log.append({"step": "parse_schema", "status": "started"})
            schema = self._parse_schema(input_data.schema or {})
            processing_log.append({"step": "parse_schema", "status": "completed"})

            # 4. Parse target configuration
            processing_log.append({"step": "parse_target", "status": "started"})
            target = self._parse_target(input_data.target or {}, pipeline)
            processing_log.append({"step": "parse_target", "status": "completed"})

            # 5. Parse transformations
            processing_log.append({"step": "parse_transformations", "status": "started"})
            transformations = self._parse_transformations(input_data.transformations or [])
            processing_log.append({"step": "parse_transformations", "status": "completed"})

            # 6. Parse quality rules
            processing_log.append({"step": "parse_quality_rules", "status": "started"})
            quality_rules = self._parse_quality_rules(input_data.quality_rules or [])
            processing_log.append({"step": "parse_quality_rules", "status": "completed"})

            # 7. Parse execution policy
            processing_log.append({"step": "parse_execution_policy", "status": "started"})
            execution_policy = self._parse_execution_policy(
                input_data.execution_policy or {},
                pipeline.environment
            )
            processing_log.append({"step": "parse_execution_policy", "status": "completed"})

            # 8. Create canonical metadata
            metadata = PipelineMetadata(
                pipeline=pipeline,
                source=source,
                schema=schema,
                target=target,
                transformations=transformations,
                quality_rules=quality_rules,
                execution_policy=execution_policy,
            )

            # 9. Validate output
            validation_errors = self.validate_output(metadata)
            if validation_errors:
                errors.extend(validation_errors)

            logger.info(
                "ui_normalization_complete",
                dag_id=pipeline.dag_id,
                columns=len(schema.columns),
                transforms=len(transformations),
                quality_rules=len(quality_rules),
            )

            return NormalizerResult(
                success=len(errors) == 0,
                metadata=metadata if not errors else None,
                errors=errors,
                warnings=warnings,
                processing_log=processing_log,
            )

        except Exception as e:
            logger.error("ui_normalization_failed", error=str(e))
            errors.append(f"Normalization failed: {str(e)}")
            return NormalizerResult(
                success=False,
                errors=errors,
                processing_log=processing_log,
            )

    def _parse_pipeline(self, data: Dict[str, Any]) -> PipelineConfig:
        """Parse pipeline configuration."""
        return PipelineConfig(
            dag_id=data.get("dag_id", data.get("pipeline_name", "").replace("-", "_").lower()),
            pipeline_name=data.get("pipeline_name", data.get("dag_id", "")),
            domain=data.get("domain", "default"),
            subdomain=data.get("subdomain"),
            product_code=data.get("product_code", data.get("dag_id", "").split("_")[0]),
            description=data.get("description"),
            owner_team=data.get("owner_team", "data-engineering"),
            owner_email=data.get("owner_email", "data-eng@company.com"),
            jira_ticket=data.get("jira_ticket"),
            jira_epic=data.get("jira_epic"),
            environment=Environment(data.get("environment", "dev")),
        )

    def _parse_source(self, data: Dict[str, Any]) -> SourceConfig:
        """Parse source configuration."""
        source_type = SourceType(data.get("source_type", "file_csv"))

        # Build type-specific config
        file_config = None
        database_config = None
        streaming_config = None
        api_config = None
        ebcdic_config = None

        if source_type.value.startswith("file_"):
            if source_type == SourceType.FILE_EBCDIC:
                ebcdic_config = EBCDICSourceConfig(
                    source_bucket=data.get("source_bucket", ""),
                    source_prefix=data.get("source_prefix", ""),
                    file_pattern=data.get("file_pattern", "*"),
                    copybook_content=data.get("copybook_content"),
                    copybook_path=data.get("copybook_path"),
                    encoding=data.get("encoding", "cp037"),
                    record_length=data.get("record_length"),
                    field_specs=data.get("field_specs", []),
                )
            else:
                file_config = FileSourceConfig(
                    source_bucket=data.get("source_bucket", ""),
                    source_prefix=data.get("source_prefix", ""),
                    file_pattern=data.get("file_pattern", "*"),
                    delimiter=data.get("delimiter", ","),
                    has_header=data.get("has_header", True),
                    encoding=data.get("encoding", "utf-8"),
                )
        elif source_type.value.startswith("database_"):
            database_config = DatabaseSourceConfig(
                connection_id=data.get("connection_id", ""),
                source_schema=data.get("source_schema", "public"),
                source_table=data.get("source_table", ""),
                source_query=data.get("source_query"),
                extraction_mode=data.get("extraction_mode", "full"),
                watermark_column=data.get("watermark_column"),
            )
        elif source_type.value.startswith("streaming_"):
            streaming_config = StreamingSourceConfig(
                kafka_bootstrap_servers=data.get("kafka_bootstrap_servers"),
                kafka_topic=data.get("kafka_topic"),
                kafka_consumer_group=data.get("kafka_consumer_group"),
                pubsub_subscription=data.get("pubsub_subscription"),
            )
        elif source_type == SourceType.API_REST:
            api_config = APISourceConfig(
                api_endpoint=data.get("api_endpoint", ""),
                api_method=data.get("api_method", "GET"),
                api_headers=data.get("api_headers", {}),
                api_auth_type=data.get("api_auth_type", "bearer"),
                api_auth_secret=data.get("api_auth_secret", ""),
                pagination_type=data.get("pagination_type"),
            )

        return SourceConfig(
            source_type=source_type,
            source_format=SourceFormat(data.get("source_format", "csv")) if data.get("source_format") else None,
            file_config=file_config,
            database_config=database_config,
            streaming_config=streaming_config,
            api_config=api_config,
            ebcdic_config=ebcdic_config,
        )

    def _parse_schema(self, data: Dict[str, Any]) -> SchemaConfig:
        """Parse schema definition."""
        columns_data = data.get("columns", [])
        columns = [
            ColumnDefinition(
                name=col.get("name", f"col_{i}"),
                type=DataType(col.get("type", "string")),
                nullable=col.get("nullable", True),
                description=col.get("description"),
                pk=col.get("pk", False),
                partition_key=col.get("partition_key", False),
                cluster_key=col.get("cluster_key", False),
                pii=col.get("pii", "none"),
            )
            for i, col in enumerate(columns_data)
        ]

        # If no columns provided, create a placeholder
        if not columns:
            columns = [ColumnDefinition(name="id", type=DataType.STRING, pk=True)]

        return SchemaConfig(
            schema_version=data.get("schema_version", "1.0.0"),
            is_current=True,
            columns=columns,
            primary_keys=data.get("primary_keys", []),
            partition_columns=data.get("partition_columns", []),
            cluster_columns=data.get("cluster_columns", []),
        )

    def _parse_target(self, data: Dict[str, Any], pipeline: PipelineConfig) -> TargetConfig:
        """Parse target configuration."""
        return TargetConfig(
            target_zone=TargetZone(data.get("target_zone", "gold")),
            bq_project=data.get("bq_project"),
            bq_dataset=data.get("bq_dataset", pipeline.domain),
            bq_table=data.get("bq_table", pipeline.product_code),
            bq_location=data.get("bq_location", "US"),
            write_mode=WriteMode(data.get("write_mode", "append")),
            merge_keys=data.get("merge_keys", []),
            destination_model=DestinationModel(data.get("destination_model", "flat")),
            partition_field=data.get("partition_field"),
            clustering_fields=data.get("clustering_fields", []),
        )

    def _parse_transformations(self, data: List[Dict[str, Any]]) -> List[TransformConfig]:
        """Parse transformation rules."""
        return [
            TransformConfig(
                transform_type=TransformType(t.get("transform_type", "derive")),
                transform_order=t.get("transform_order", i),
                zone=TargetZone(t.get("zone", "silver")),
                config=t.get("config", {}),
                nl_description=t.get("nl_description"),
                generated_pyspark=t.get("generated_pyspark"),
                is_active=t.get("is_active", True),
            )
            for i, t in enumerate(data)
        ]

    def _parse_quality_rules(self, data: List[Dict[str, Any]]) -> List[QualityRule]:
        """Parse quality rules."""
        return [
            QualityRule(
                rule_name=r.get("rule_name", f"rule_{i}"),
                rule_type=QualityRuleType(r.get("rule_type", "not_null")),
                column_name=r.get("column_name"),
                config=r.get("config", {}),
                severity=Severity(r.get("severity", "warning")),
                threshold_pct=r.get("threshold_pct", 100.0),
                is_active=r.get("is_active", True),
            )
            for i, r in enumerate(data)
        ]

    def _parse_execution_policy(
        self, data: Dict[str, Any], environment: Environment
    ) -> ExecutionPolicy:
        """Parse execution policy."""
        # Require approval for prod
        requires_approval = data.get("requires_human_approval", environment == Environment.PROD)

        return ExecutionPolicy(
            schedule_interval=data.get("schedule_interval", "@daily"),
            catchup=data.get("catchup", False),
            processing_mode=ProcessingMode(data.get("processing_mode", "batch")),
            max_active_runs=data.get("max_active_runs", 1),
            retry_count=data.get("retry_count", 2),
            retry_delay_minutes=data.get("retry_delay_minutes", 5),
            timeout_hours=data.get("timeout_hours", 2),
            sla_hours=data.get("sla_hours"),
            requires_human_approval=requires_approval,
            approval_groups=data.get("approval_groups", []),
            alert_emails=data.get("alert_emails", []),
            slack_webhook=data.get("slack_webhook"),
        )
