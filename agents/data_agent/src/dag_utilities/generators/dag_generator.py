"""
Deterministic DAG Generator - Step 4 of Agent Workflow

GUARANTEE: Same input metadata → Same output DAG.
No random values, no timestamps in logic, no external state.

Generates:
1. Thin Airflow DAG (imports only from dag_utilities)
2. Spark job configurations
3. GE expectation suite definitions
4. Deterministic checksum for change detection
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .template_manager import TemplateManager


@dataclass
class GeneratedArtifact:
    """A single generated artifact (DAG, Spark config, GE suite)."""
    artifact_type: str  # dag, platform_spark_config, ge_suite, sql
    filename: str
    content: str
    checksum: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filename": self.filename,
            "checksum": self.checksum,
            "content_length": len(self.content),
        }


@dataclass
class GeneratedArtifacts:
    """Collection of all generated artifacts for a pipeline."""
    feed_id: int
    dag_id: str
    pattern_code: str
    dag_file: GeneratedArtifact
    platform_spark_config: GeneratedArtifact
    ge_suites: List[GeneratedArtifact] = field(default_factory=list)
    sql_statements: List[GeneratedArtifact] = field(default_factory=list)
    overall_checksum: str = ""
    generated_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_id": self.feed_id,
            "dag_id": self.dag_id,
            "pattern_code": self.pattern_code,
            "dag_file": self.dag_file.to_dict(),
            "platform_spark_config": self.platform_spark_config.to_dict(),
            "ge_suites": [s.to_dict() for s in self.ge_suites],
            "sql_statements": [s.to_dict() for s in self.sql_statements],
            "overall_checksum": self.overall_checksum,
            "generated_at": self.generated_at,
        }


class DeterministicDAGGenerator:
    """
    GUARANTEE: Same input metadata → Same output DAG.

    No random values, no timestamps in logic, no external state.
    All determinism verified via checksum.
    """

    def __init__(
        self,
        metadata_client: Any,
        template_manager: Optional[TemplateManager] = None,
        output_dir: Optional[Path] = None,
    ):
        """
        Initialize DAG generator.

        Args:
            metadata_client: MetadataClient for database access
            template_manager: TemplateManager instance (creates default if None)
            output_dir: Directory to write generated artifacts
        """
        self.meta = metadata_client
        self.template_manager = template_manager or TemplateManager()
        self.output_dir = output_dir

    def generate(self, feed_id: int) -> GeneratedArtifacts:
        """
        Generate all artifacts deterministically.

        Steps:
        1. Fetch ALL config from metadata (single source of truth)
        2. Compute derived values deterministically
        3. Get/create template
        4. Render DAG (no random/time-based values)
        5. Generate Spark config
        6. Generate GE suite definitions
        7. Compute overall checksum

        Args:
            feed_id: Numeric feed ID

        Returns:
            GeneratedArtifacts with DAG, Spark, GE, and SQL artifacts
        """
        # Step 1: Fetch ALL config from metadata
        feed = self.meta.get_feed_config(feed_id) if hasattr(self.meta, 'get_feed_config') else {}
        contract = self.meta.get_contract(feed_id) if hasattr(self.meta, 'get_contract') else {}
        schema = self.meta.get_schema(feed_id) if hasattr(self.meta, 'get_schema') else {}
        zones = self.meta.get_zone_configs(feed_id) if hasattr(self.meta, 'get_zone_configs') else []
        transforms = self.meta.get_transforms(feed_id) if hasattr(self.meta, 'get_transforms') else []
        quality_rules = self.meta.get_quality_rules(feed_id) if hasattr(self.meta, 'get_quality_rules') else []
        encryption_config = self.meta.get_encryption_config(feed_id) if hasattr(self.meta, 'get_encryption_config') else None

        # Step 2: Compute derived values deterministically
        feed_name = feed.get("feed_name", f"feed_{feed_id}")
        domain = feed.get("domain", "default")
        source_type = feed.get("source_type", "file_csv")
        source_category = self._get_source_category(source_type)
        contract_type = contract.get("contract_type", "STANDARD")
        dag_id = f"{domain}_{feed_name}_pipeline"

        # Pattern selection
        pattern_code = self._select_pattern(source_category, contract_type)

        # Step 3: Get/create template
        template = self.template_manager.get_template(source_category, contract_type)

        # Step 4: Render DAG (deterministic)
        template_vars = self._build_template_variables(
            feed_id=feed_id,
            feed=feed,
            contract=contract,
            schema=schema,
            zones=zones,
            transforms=transforms,
            quality_rules=quality_rules,
            encryption_config=encryption_config,
            dag_id=dag_id,
            pattern_code=pattern_code,
            source_category=source_category,
        )

        dag_content = template.render(**template_vars)
        dag_checksum = self._compute_content_checksum(dag_content)

        dag_artifact = GeneratedArtifact(
            artifact_type="dag",
            filename=f"{dag_id}.py",
            content=dag_content,
            checksum=dag_checksum,
        )

        # Step 5: Generate Spark config
        platform_spark_config = self._generate_spark_config(
            feed_id=feed_id,
            feed=feed,
            zones=zones,
            transforms=transforms,
            domain=domain,
        )
        spark_checksum = self._compute_content_checksum(json.dumps(platform_spark_config, sort_keys=True))

        spark_artifact = GeneratedArtifact(
            artifact_type="platform_spark_config",
            filename=f"{dag_id}_spark_config.json",
            content=json.dumps(platform_spark_config, indent=2, sort_keys=True),
            checksum=spark_checksum,
        )

        # Step 6: Generate GE suite definitions
        ge_artifacts = self._generate_ge_suites(
            feed_id=feed_id,
            schema=schema,
            quality_rules=quality_rules,
            zones=zones,
        )

        # Step 7: Compute overall checksum
        all_checksums = [dag_checksum, spark_checksum] + [a.checksum for a in ge_artifacts]
        overall_checksum = self._compute_content_checksum("|".join(sorted(all_checksums)))

        # Use feed updated_at for deterministic timestamp (NOT datetime.now())
        generated_at = feed.get("updated_at", feed.get("created_at", ""))
        if isinstance(generated_at, datetime):
            generated_at = generated_at.isoformat()

        artifacts = GeneratedArtifacts(
            feed_id=feed_id,
            dag_id=dag_id,
            pattern_code=pattern_code,
            dag_file=dag_artifact,
            platform_spark_config=spark_artifact,
            ge_suites=ge_artifacts,
            overall_checksum=overall_checksum,
            generated_at=generated_at,
        )

        # Write to disk if output directory specified
        if self.output_dir:
            self._write_artifacts(artifacts)

        return artifacts

    def _build_template_variables(
        self,
        feed_id: int,
        feed: Dict[str, Any],
        contract: Dict[str, Any],
        schema: Dict[str, Any],
        zones: List[Dict[str, Any]],
        transforms: List[Dict[str, Any]],
        quality_rules: List[Dict[str, Any]],
        encryption_config: Optional[Dict[str, Any]],
        dag_id: str,
        pattern_code: str,
        source_category: str,
    ) -> Dict[str, Any]:
        """Build template variables from metadata (all deterministic)."""
        feed_name = feed.get("feed_name", f"feed_{feed_id}")
        domain = feed.get("domain", "default")
        environment = feed.get("environment", "dev")

        zone_levels = [z.get("zone_level", z.get("zone", "")) for z in zones]
        if not zone_levels:
            zone_levels = ["transient", "raw", "silver", "gold", "consumption"]

        return {
            # Identity
            "dag_id": dag_id,
            "feed_id": feed_id,
            "feed_name": feed_name,
            "feed_group_id": feed.get("feed_group_id", 0),
            "feed_group_name": feed.get("feed_group_name", feed_name),
            "domain": domain,
            "environment": environment,
            "pattern_code": pattern_code,
            "source_category": source_category,

            # Source
            "source_type": feed.get("source_type", "file_csv"),
            "source_system": feed.get("source_system", "gcs"),
            "file_format": feed.get("file_format", "csv"),
            "delimiter": feed.get("delimiter", ","),
            "has_header": feed.get("has_header", True),
            "encoding": feed.get("encoding", "UTF-8"),
            "file_pattern": feed.get("file_pattern", "*.csv"),

            # Contract
            "contract_type": contract.get("contract_type", "STANDARD"),
            "data_vault_type": contract.get("data_vault_type"),

            # Schema
            "schema": schema,
            "columns": schema.get("columns", []),

            # Zones
            "zones": zone_levels,
            "zone_configs": zones,

            # Transforms and quality
            "transforms": transforms,
            "quality_rules": quality_rules,
            "encryption_config": encryption_config,

            # Execution
            "schedule": feed.get("schedule", {"dev": "@daily", "qa": "@daily", "prod": "@daily"}),
            "retry_count": feed.get("retry_count", 3),
            "retry_delay_minutes": feed.get("retry_delay_minutes", 5),
            "depends_on_past": feed.get("depends_on_past", False),
            "catchup": feed.get("catchup", False),
            "max_active_runs": feed.get("max_active_runs", 1),

            # Owner
            "owner": feed.get("owner", "apex-data-agent"),
            "notification_emails": feed.get("notification_emails", []),
            "notification_on_success": feed.get("notification_on_success", False),

            # GCP
            "gcp_project": feed.get("gcp_project", "apex-data-platform"),
            "gcp_region": feed.get("gcp_region", "us-central1"),
            "dataproc_cluster": feed.get("dataproc_cluster", "apex-dataproc"),
            "spark_jobs_bucket": feed.get("spark_jobs_bucket", "apex-spark-jobs"),

            # Buckets and storage
            "source_bucket": feed.get("source_bucket", ""),
            "source_prefix": feed.get("source_prefix", ""),
            "landing_bucket": feed.get("landing_bucket", "apex-landing"),
            "archive_bucket": feed.get("archive_bucket", "apex-archive"),
            "archive_enabled": feed.get("archive_enabled", True),
            "archive_retention_days": feed.get("archive_retention_days", 90),

            # Quality
            "quality_threshold": feed.get("quality_threshold", 0.95),
            "ge_failure_policy": feed.get("ge_failure_policy", "FAIL"),
            "contract_policy": feed.get("contract_policy", "FAIL_FAST"),

            # Zone targets
            "raw_dataset": feed.get("raw_dataset", "raw"),
            "raw_table": feed.get("raw_table", f"{feed_name}_raw"),
            "silver_dataset": feed.get("silver_dataset", "silver"),
            "silver_table": feed.get("silver_table", feed_name),
            "gold_dataset": feed.get("gold_dataset", "gold"),
            "gold_table": feed.get("gold_table", feed_name),
            "consumption_dataset": feed.get("consumption_dataset", "consumption"),
            "consumption_table": feed.get("consumption_table", f"{feed_name}_summary"),
            "include_consumption": "consumption" in zone_levels,

            # Spark
            "spark_executor_memory": feed.get("spark_executor_memory", "8g"),
            "spark_executor_cores": feed.get("spark_executor_cores", 4),
        }

    def _generate_spark_config(
        self,
        feed_id: int,
        feed: Dict[str, Any],
        zones: List[Dict[str, Any]],
        transforms: List[Dict[str, Any]],
        domain: str,
    ) -> Dict[str, Any]:
        """Generate Spark job configuration (deterministic)."""
        feed_name = feed.get("feed_name", f"feed_{feed_id}")

        return {
            "feed_id": feed_id,
            "feed_name": feed_name,
            "domain": domain,
            "spark_jobs": {
                "zone_processor": {
                    "main_python_file": "gs://apex-spark-jobs/dag_utilities/processors/zone_processor.py",
                    "zones": [z.get("zone_level", z.get("zone", "")) for z in zones],
                    "common_args": [
                        "--feed-id", str(feed_id),
                        "--metadata-url", "${METADATA_DB_URL}",
                    ],
                },
            },
            "execution_config": {
                "executor_memory": feed.get("spark_executor_memory", "8g"),
                "executor_cores": feed.get("spark_executor_cores", 4),
                "dynamic_allocation": True,
                "adaptive_execution": True,
            },
            "labels": {
                "feed_id": str(feed_id),
                "domain": domain,
                "feed_name": feed_name,
                "environment": "${APEX_ENVIRONMENT}",
            },
        }

    def _generate_ge_suites(
        self,
        feed_id: int,
        schema: Dict[str, Any],
        quality_rules: List[Dict[str, Any]],
        zones: List[Dict[str, Any]],
    ) -> List[GeneratedArtifact]:
        """Generate GE expectation suite definitions (deterministic)."""
        artifacts = []

        zone_levels = [z.get("zone_level", z.get("zone", "")) for z in zones]

        for zone in zone_levels:
            # Skip transient zone (no validation)
            if zone == "transient":
                continue

            # Schema expectations
            schema_expectations = self._build_schema_expectations(schema, zone)

            # Semantic expectations for this zone
            zone_rules = [r for r in quality_rules if r.get("zone_level") == zone]
            semantic_expectations = self._build_semantic_expectations(zone_rules)

            suite_config = {
                "suite_name": f"feed_{feed_id}_{zone}_combined",
                "feed_id": feed_id,
                "zone": zone,
                "expectations": schema_expectations + semantic_expectations,
            }

            content = json.dumps(suite_config, indent=2, sort_keys=True)
            checksum = self._compute_content_checksum(content)

            artifacts.append(GeneratedArtifact(
                artifact_type="ge_suite",
                filename=f"feed_{feed_id}_{zone}_suite.json",
                content=content,
                checksum=checksum,
            ))

        return artifacts

    def _build_schema_expectations(
        self,
        schema: Dict[str, Any],
        zone: str
    ) -> List[Dict[str, Any]]:
        """Build schema expectations from schema_details."""
        expectations = []
        for col in schema.get("columns", []):
            col_name = col.get("name", col.get("column_name"))
            if not col_name:
                continue

            expectations.append({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": col_name},
                "meta": {"source": "schema_details"},
            })

            if not col.get("nullable", True) and zone != "raw":
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col_name},
                    "meta": {"source": "schema_details"},
                })

        return expectations

    def _build_semantic_expectations(
        self,
        quality_rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build semantic expectations from quality_rules."""
        rule_mapping = {
            "not_null": "expect_column_values_to_not_be_null",
            "unique": "expect_column_values_to_be_unique",
            "range": "expect_column_values_to_be_between",
            "regex": "expect_column_values_to_match_regex",
            "in_set": "expect_column_values_to_be_in_set",
        }

        expectations = []
        for rule in quality_rules:
            rule_type = rule.get("rule_type", "")
            ge_type = rule_mapping.get(rule_type)
            if not ge_type:
                continue

            kwargs = {"column": rule.get("column_name", "")}
            rule_config = rule.get("rule_config", {})

            if rule_type == "range":
                kwargs["min_value"] = rule_config.get("min")
                kwargs["max_value"] = rule_config.get("max")
            elif rule_type == "regex":
                kwargs["regex"] = rule_config.get("pattern", ".*")
            elif rule_type == "in_set":
                kwargs["value_set"] = rule_config.get("values", [])

            expectations.append({
                "expectation_type": ge_type,
                "kwargs": kwargs,
                "meta": {"source": "quality_rules", "rule_name": rule.get("rule_name", "")},
            })

        return expectations

    def _select_pattern(
        self,
        source_category: str,
        contract_type: str
    ) -> str:
        """Select pattern code deterministically."""
        # Contract type takes priority
        contract_patterns = {
            "SCD2": "P07",
            "DATA_VAULT": "P08",
            "STAR_SCHEMA": "P09",
        }
        if contract_type in contract_patterns:
            return contract_patterns[contract_type]

        # Then source category
        category_patterns = {
            "FILE": "P01",
            "DATABASE": "P03",
            "LEGACY": "P04",
            "STREAMING": "P05",
            "API": "P06",
            "SAAS": "P06",
            "NOSQL": "P03",
            "LOGS": "P01",
            "CLOUD": "P01",
        }
        return category_patterns.get(source_category.upper(), "P01")

    def _get_source_category(self, source_type: str) -> str:
        """Extract category from source_type string."""
        prefix = source_type.split("_")[0] if "_" in source_type else source_type
        return prefix.upper()

    def _compute_content_checksum(self, content: str) -> str:
        """Compute SHA256 checksum (deterministic)."""
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _write_artifacts(self, artifacts: GeneratedArtifacts) -> None:
        """Write artifacts to output directory."""
        if not self.output_dir:
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Write DAG file
        dag_path = self.output_dir / artifacts.dag_file.filename
        dag_path.write_text(artifacts.dag_file.content)

        # Write Spark config
        spark_path = self.output_dir / artifacts.platform_spark_config.filename
        spark_path.write_text(artifacts.platform_spark_config.content)

        # Write GE suites
        ge_dir = self.output_dir / "ge_suites"
        ge_dir.mkdir(exist_ok=True)
        for suite in artifacts.ge_suites:
            suite_path = ge_dir / suite.filename
            suite_path.write_text(suite.content)

        # Write manifest
        manifest = {
            "feed_id": artifacts.feed_id,
            "dag_id": artifacts.dag_id,
            "pattern_code": artifacts.pattern_code,
            "overall_checksum": artifacts.overall_checksum,
            "generated_at": artifacts.generated_at,
            "artifacts": {
                "dag": artifacts.dag_file.to_dict(),
                "platform_spark_config": artifacts.platform_spark_config.to_dict(),
                "ge_suites": [s.to_dict() for s in artifacts.ge_suites],
            },
        }
        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def generate_dag(
    feed_id: int,
    metadata_client: Any,
    output_dir: Optional[Path] = None,
) -> GeneratedArtifacts:
    """
    Convenience function to generate DAG artifacts.

    Args:
        feed_id: Numeric feed ID
        metadata_client: MetadataClient instance
        output_dir: Optional directory to write artifacts

    Returns:
        GeneratedArtifacts with DAG, Spark config, and GE suites
    """
    generator = DeterministicDAGGenerator(
        metadata_client=metadata_client,
        output_dir=output_dir,
    )
    return generator.generate(feed_id)


__all__ = [
    "DeterministicDAGGenerator",
    "GeneratedArtifacts",
    "GeneratedArtifact",
    "generate_dag",
]
