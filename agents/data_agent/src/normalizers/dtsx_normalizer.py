"""
DTSX Normalizer - Converts SSIS packages to PipelineMetadata.

Parses .dtsx files and extracts:
- Source connections
- Data flows
- Transformations
- Destination mappings
"""

import structlog
from typing import Any, Dict, List, Optional

from ..models.canonical import PipelineMetadata, UnifiedPipelineInput, InputType
from ..models.pipeline import PipelineConfig, Environment
from ..models.source import SourceConfig, SourceType, DTSXSourceConfig
from ..models.schema import SchemaConfig, ColumnDefinition, DataType
from ..models.target import TargetConfig, TargetZone, WriteMode
from ..models.transformation import TransformConfig, TransformType
from ..models.quality import QualityRule
from ..models.execution import ExecutionPolicy

from .base import BaseNormalizer, NormalizerResult

logger = structlog.get_logger(__name__)


class DTSXNormalizer(BaseNormalizer):
    """
    Normalizes DTSX/SSIS packages to PipelineMetadata.

    Process:
    1. Parse .dtsx XML file
    2. Extract data flow components
    3. Map SSIS transforms to Spark transforms
    4. Generate column mappings
    5. Create PipelineMetadata
    """

    def can_handle(self, input_data: UnifiedPipelineInput) -> bool:
        """Check if input is DTSX migration."""
        return (
            input_data.input_type == InputType.DTSX_MIGRATION and
            input_data.dtsx_path is not None
        )

    async def normalize(self, input_data: UnifiedPipelineInput) -> NormalizerResult:
        """
        Normalize DTSX package to PipelineMetadata.

        Args:
            input_data: DTSX migration input

        Returns:
            NormalizerResult with PipelineMetadata
        """
        processing_log = []
        errors = []
        warnings = []

        try:
            dtsx_path = input_data.dtsx_path
            package_name = input_data.dtsx_package_name or self._extract_package_name(dtsx_path)

            # 1. Parse DTSX file
            processing_log.append({"step": "parse_dtsx", "status": "started"})
            parsed = await self._parse_dtsx(dtsx_path)
            processing_log.append({
                "step": "parse_dtsx",
                "status": "completed",
                "sources": len(parsed.get("sources", [])),
                "transforms": len(parsed.get("transforms", [])),
            })

            if not parsed:
                return NormalizerResult(
                    success=False,
                    errors=["Failed to parse DTSX file"],
                    processing_log=processing_log,
                )

            # 2. Create pipeline config
            processing_log.append({"step": "create_pipeline", "status": "started"})
            dag_id = self._generate_dag_id(package_name)
            pipeline = PipelineConfig(
                dag_id=dag_id,
                pipeline_name=package_name.replace("_", " ").title(),
                domain=parsed.get("domain", "legacy"),
                product_code=dag_id.split("_")[0],
                description=f"Migrated from SSIS package: {package_name}",
                owner_team="data-engineering",
                owner_email="data-eng@company.com",
                environment=Environment.DEV,
            )
            processing_log.append({"step": "create_pipeline", "status": "completed"})

            # 3. Create source config
            processing_log.append({"step": "create_source", "status": "started"})
            source = self._create_source_config(parsed, dtsx_path, package_name)
            processing_log.append({"step": "create_source", "status": "completed"})

            # 4. Create schema from extracted columns
            processing_log.append({"step": "create_schema", "status": "started"})
            schema = self._create_schema_config(parsed)
            processing_log.append({"step": "create_schema", "status": "completed"})

            # 5. Create target config
            processing_log.append({"step": "create_target", "status": "started"})
            target = self._create_target_config(parsed, pipeline)
            processing_log.append({"step": "create_target", "status": "completed"})

            # 6. Map SSIS transforms to Spark
            processing_log.append({"step": "map_transforms", "status": "started"})
            transformations = self._map_transforms(parsed.get("transforms", []))
            processing_log.append({"step": "map_transforms", "status": "completed"})

            # 7. Create quality rules
            quality_rules = self._create_quality_rules(schema)

            # 8. Create execution policy
            execution_policy = ExecutionPolicy(
                schedule_interval="@daily",
                requires_human_approval=False,
            )

            # 9. Build metadata
            metadata = PipelineMetadata(
                pipeline=pipeline,
                source=source,
                schema=schema,
                target=target,
                transformations=transformations,
                quality_rules=quality_rules,
                execution_policy=execution_policy,
            )

            # Validate
            validation_errors = self.validate_output(metadata)
            if validation_errors:
                warnings.extend(validation_errors)

            logger.info(
                "dtsx_normalization_complete",
                dag_id=dag_id,
                columns=len(schema.columns),
                transforms=len(transformations),
            )

            return NormalizerResult(
                success=True,
                metadata=metadata,
                errors=errors,
                warnings=warnings,
                processing_log=processing_log,
            )

        except Exception as e:
            logger.error("dtsx_normalization_failed", error=str(e))
            errors.append(f"DTSX normalization failed: {str(e)}")
            return NormalizerResult(
                success=False,
                errors=errors,
                processing_log=processing_log,
            )

    async def _parse_dtsx(self, dtsx_path: str) -> Dict[str, Any]:
        """
        Parse DTSX file and extract components.

        Uses the dtsx_parser module for actual parsing.
        """
        try:
            from ..parsers.dtsx_parser import DTSXParser
            parser = DTSXParser()
            return await parser.parse(dtsx_path)
        except ImportError:
            logger.warning("dtsx_parser_not_available", path=dtsx_path)
            # Return minimal structure
            return {
                "sources": [],
                "transforms": [],
                "destinations": [],
                "columns": [],
            }

    def _extract_package_name(self, dtsx_path: str) -> str:
        """Extract package name from file path."""
        import os
        return os.path.splitext(os.path.basename(dtsx_path))[0]

    def _generate_dag_id(self, package_name: str) -> str:
        """Generate valid DAG ID from package name."""
        import re
        # Convert to snake_case
        dag_id = re.sub(r"[^a-zA-Z0-9]", "_", package_name.lower())
        dag_id = re.sub(r"_+", "_", dag_id).strip("_")
        # Ensure starts with letter
        if dag_id and not dag_id[0].isalpha():
            dag_id = "pkg_" + dag_id
        return dag_id or "dtsx_migration"

    def _create_source_config(
        self,
        parsed: Dict[str, Any],
        dtsx_path: str,
        package_name: str,
    ) -> SourceConfig:
        """Create source config from parsed DTSX."""
        sources = parsed.get("sources", [])

        # Determine source type from first source
        source_type = SourceType.DTSX
        if sources:
            first_source = sources[0]
            if "oledb" in str(first_source).lower():
                source_type = SourceType.DATABASE_SQLSERVER

        dtsx_config = DTSXSourceConfig(
            dtsx_location=dtsx_path,
            dtsx_package_name=package_name,
            extracted_sources=sources,
            extracted_transforms=parsed.get("transforms", []),
            extracted_destinations=parsed.get("destinations", []),
            column_mappings=parsed.get("column_mappings", []),
        )

        return SourceConfig(
            source_type=source_type,
            dtsx_config=dtsx_config,
        )

    def _create_schema_config(self, parsed: Dict[str, Any]) -> SchemaConfig:
        """Create schema from extracted columns."""
        columns_data = parsed.get("columns", [])

        if not columns_data:
            # Default columns if none extracted
            columns_data = [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "data", "type": "string", "nullable": True},
            ]

        columns = [
            ColumnDefinition(
                name=self._clean_column_name(col.get("name", f"col_{i}")),
                type=self._map_ssis_type(col.get("type", "string")),
                nullable=col.get("nullable", True),
                description=col.get("description"),
            )
            for i, col in enumerate(columns_data)
        ]

        return SchemaConfig(
            platform_schema_version="1.0.0",
            is_current=True,
            columns=columns,
        )

    def _create_target_config(
        self,
        parsed: Dict[str, Any],
        pipeline: PipelineConfig,
    ) -> TargetConfig:
        """Create target config from parsed DTSX."""
        destinations = parsed.get("destinations", [])

        # Extract table name from first destination
        table_name = pipeline.product_code
        if destinations:
            dest = destinations[0]
            if isinstance(dest, dict):
                table_name = dest.get("table", table_name)

        return TargetConfig(
            target_zone=TargetZone.GOLD,
            bq_dataset=pipeline.domain,
            bq_table=table_name,
            write_mode=WriteMode.MERGE,
        )

    def _map_transforms(
        self,
        ssis_transforms: List[Dict[str, Any]],
    ) -> List[TransformConfig]:
        """Map SSIS transforms to Spark transforms."""
        transforms = []

        # SSIS to Spark transform mapping
        transform_map = {
            "derived column": TransformType.DERIVE,
            "data conversion": TransformType.CAST,
            "conditional split": TransformType.FILTER,
            "aggregate": TransformType.AGGREGATE,
            "lookup": TransformType.JOIN,
            "sort": TransformType.DEDUPLICATE,
            "union all": TransformType.DERIVE,
        }

        for i, ssis_t in enumerate(ssis_transforms):
            ssis_type = str(ssis_t.get("type", "")).lower()

            # Find matching Spark transform
            spark_type = TransformType.DERIVE
            for ssis_key, spark_t in transform_map.items():
                if ssis_key in ssis_type:
                    spark_type = spark_t
                    break

            transforms.append(TransformConfig(
                transform_type=spark_type,
                transform_order=i,
                zone=TargetZone.SILVER,
                config=ssis_t.get("config", {}),
                nl_description=f"Migrated from SSIS: {ssis_t.get('name', ssis_type)}",
                is_active=True,
            ))

        return transforms

    def _create_quality_rules(self, schema: SchemaConfig) -> List[QualityRule]:
        """Create default quality rules based on schema."""
        rules = []

        # Add not_null rule for non-nullable columns
        for col in schema.columns:
            if not col.nullable:
                rules.append(QualityRule(
                    rule_name=f"{col.name}_not_null",
                    rule_type="not_null",
                    column_name=col.name,
                    severity="error",
                ))

        return rules

    def _clean_column_name(self, name: str) -> str:
        """Clean column name to valid identifier."""
        import re
        clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
        clean = re.sub(r"_+", "_", clean).strip("_")
        if clean and not clean[0].isalpha():
            clean = "col_" + clean
        return clean or "column"

    def _map_ssis_type(self, ssis_type: str) -> DataType:
        """Map SSIS data type to our DataType enum."""
        type_map = {
            "dt_str": DataType.STRING,
            "dt_wstr": DataType.STRING,
            "dt_i4": DataType.INTEGER,
            "dt_i8": DataType.BIGINT,
            "dt_r4": DataType.FLOAT,
            "dt_r8": DataType.DOUBLE,
            "dt_bool": DataType.BOOLEAN,
            "dt_date": DataType.DATE,
            "dt_dbtimestamp": DataType.TIMESTAMP,
            "dt_numeric": DataType.DECIMAL,
            "dt_bytes": DataType.BINARY,
        }
        return type_map.get(ssis_type.lower(), DataType.STRING)
