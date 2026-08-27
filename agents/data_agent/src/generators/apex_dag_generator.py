"""
APEX Data Agent - DAG Generator

Generates Airflow DAGs from APEX pattern templates using Jinja2.

Generation Flow:
1. Receive APEXPipelineConfig or APEXGenerationRequest
2. Select pattern using RegistryManager
3. Build template context from configuration
4. Render Jinja2 template
5. Validate generated code
6. Return generated artifacts

All generated DAGs are:
- Metadata-driven (fetch config from PostgreSQL at runtime)
- Self-healing (automatic remediation for common failures)
- Observable (audit logging, metrics, lineage tracking)
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

from src.models.apex_models import (
    PatternCode,
    APEXPipelineConfig,
    APEXGenerationRequest,
    APEXGenerationResponse,
    Feed,
    DataContract,
    SourceRegistry,
    SchemaVersion,
    ViewDefinition,
    ZoneLevel,
)
from src.models.feed_config import FeedGroupConfig, FeedConfig
from src.generators.sql_generator import generate_all_sql, generate_ge_expectations
from src.repository.registry_manager import RegistryManager

logger = structlog.get_logger()


class APEXDAGGenerator:
    """
    Generate Airflow DAGs from APEX pipeline configurations.

    All generated DAGs follow the pattern templates and are:
    1. Metadata-driven - configuration from PostgreSQL
    2. Self-healing - automatic remediation
    3. Observable - audit logging and metrics
    """

    # Template directories
    PATTERN_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "patterns"
    SPARK_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "spark"
    SQL_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "sql"
    V2_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "v2"

    # Output directories
    DAG_OUTPUT_DIR = Path(__file__).parent.parent.parent / "dags" / "generated"
    SPARK_OUTPUT_DIR = Path(__file__).parent.parent.parent / "spark_jobs" / "generated"

    def __init__(self) -> None:
        """Initialize APEX DAG generator."""
        self.registry = RegistryManager()
        self._env: Optional[Environment] = None

    @property
    def env(self) -> Environment:
        """Lazy-load Jinja2 environment."""
        if self._env is None:
            # Create template directories if they don't exist
            self.PATTERN_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

            self._env = Environment(
                loader=FileSystemLoader([
                    str(self.PATTERN_TEMPLATE_DIR),
                    str(self.SPARK_TEMPLATE_DIR),
                    str(self.SQL_TEMPLATE_DIR),
                ]),
                undefined=StrictUndefined,
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
            )

            # Add custom filters
            self._env.filters["to_python_list"] = self._to_python_list
            self._env.filters["to_json"] = self._to_json
            self._env.filters["quote"] = self._quote_string
            self._env.globals["now"] = datetime.utcnow

        return self._env

    def generate(
        self,
        config: APEXPipelineConfig,
        output_dir: Optional[Path] = None,
        validate: bool = True,
    ) -> APEXGenerationResponse:
        """
        Generate DAG and related artifacts from APEX configuration.

        Args:
            config: Complete APEX pipeline configuration
            output_dir: Optional output directory (default: DAG_OUTPUT_DIR)
            validate: Whether to validate generated code

        Returns:
            APEXGenerationResponse with generated artifacts
        """
        logger.info(
            "generating_apex_dag",
            feed_id=config.feed.feed_id,
            contract_id=config.contract.contract_id,
        )

        # Select pattern and template
        pattern, template_path = self.registry.select_and_resolve(config)

        # SP migration: override template when legacy_migration_output is present
        legacy_mig = (config.source and config.source.source_config or {}).get("legacy_migration_output")
        if pattern.value == "P04" and legacy_mig:
            sp_template = "p04_legacy_migration_sp.py.jinja2"
            # Only override if the SP template exists; fall back to base P04 otherwise
            import importlib.resources
            sp_path = self.PATTERN_TEMPLATE_DIR / sp_template
            if sp_path.exists():
                template_path = sp_path

        # Build template context
        context = self._build_context(config, pattern)

        # Render DAG template
        dag_code = self._render_template(template_path.name, context)

        # Get DAG ID from config
        dag_id = config.dag_id

        # Write DAG file
        output_dir = output_dir or self.DAG_OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        dag_path = output_dir / f"{dag_id}.py"
        dag_path.write_text(dag_code)

        # Generate BigQuery security DDL for PII columns
        bq_security_path = None
        try:
            from security.governance_enforcer import GovernanceEnforcer
            enforcer = GovernanceEnforcer()
            bq_project = config.feed.bq_project if hasattr(config.feed, "bq_project") else None
            asset_name = config.feed.feed_name
            bq_ddl = enforcer.generate_bq_policy_tags(asset_name, bq_project)
            if bq_ddl:
                bq_security_file = output_dir / f"{dag_id}_bq_security.sql"
                bq_security_file.write_text(bq_ddl)
                bq_security_path = str(bq_security_file)
        except Exception:
            pass  # BQ security DDL generation is best-effort

        # Validate if requested
        validation_messages = []
        validation_passed = True
        if validate:
            validation_passed, validation_messages = self._validate_dag(dag_code, dag_id)

        logger.info(
            "apex_dag_generated",
            dag_id=dag_id,
            pattern=pattern.value,
            dag_path=str(dag_path),
            validation_passed=validation_passed,
        )

        return APEXGenerationResponse(
            feed_id=config.feed.feed_id,
            contract_id=config.contract.contract_id,
            dag_id=dag_id,
            pattern_code=pattern,
            generated_dag_path=str(dag_path),
            generated_spark_jobs=[],
            generated_sql=[],
            validation_passed=validation_passed,
            validation_messages=validation_messages,
        )

    def generate_from_request(
        self,
        request: APEXGenerationRequest,
        output_dir: Optional[Path] = None,
    ) -> APEXGenerationResponse:
        """
        Generate DAG from a generation request.

        Args:
            request: Generation request with feed, contract, and options
            output_dir: Optional output directory

        Returns:
            APEXGenerationResponse
        """
        # Build APEXPipelineConfig from request
        config = APEXPipelineConfig(
            feed=request.feed,
            contract=request.contract,
            source=request.source or SourceRegistry(
                source_id="default",
                source_name=request.feed.feed_name,
                source_type=SourceType.FILE,
            ),
            schemas={},
            views={},
            transformation_rules=[],
            validation_rules=request.validation_rules,
        )

        # Add bronze schema if provided
        if request.bronze_schema:
            config.schemas[ZoneLevel.BRONZE] = request.bronze_schema

        # Add views if provided
        if request.silver_view_sql:
            config.views[ZoneLevel.SILVER] = ViewDefinition(
                contract_id=request.contract.contract_id,
                view_name=f"v_silver_{request.feed.feed_name}",
                zone_level=ZoneLevel.SILVER,
                view_sql=request.silver_view_sql,
            )

        if request.gold_view_sql:
            config.views[ZoneLevel.GOLD] = ViewDefinition(
                contract_id=request.contract.contract_id,
                view_name=f"v_gold_{request.feed.feed_name}",
                zone_level=ZoneLevel.GOLD,
                view_sql=request.gold_view_sql,
            )

        # Override pattern if specified
        if request.pattern_code:
            config.contract.pattern_code = request.pattern_code

        return self.generate(config, output_dir)

    # ---- v2: FeedGroupConfig-based generation ----

    # Source type → v2 template mapping
    V2_TEMPLATE_MAP = {
        "gcs_file": "file_ingest.py.jinja2",
        "jdbc": "database_ingest.py.jinja2",
        "api": "api_ingest.py.jinja2",
        "streaming": "streaming_ingest.py.jinja2",
        "legacy_ebcdic": "legacy_ingest.py.jinja2",
    }

    # Spark jobs included per generated bundle
    V2_SPARK_JOBS = [
        "validate_schema_with_great_expectations.py",
        "validate_semantics_with_great_expectations.py",
        "ingest_source_to_landing.py",
        "promote_landing_to_bronze.py",
        "promote_bronze_to_silver.py",
        "build_gold_layer.py",
        "load_data_vault_hub.py",
        "load_data_vault_satellite.py",
        "load_dimension_tables.py",
        "load_fact_table.py",
    ]

    def generate_from_feed_group(
        self,
        config: FeedGroupConfig,
        output_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """
        Generate a complete artifact bundle from a FeedGroupConfig.

        Selects template by source_type, renders DAG, generates SQL scripts,
        GE expectation suites, and collects matching spark jobs.

        Returns dict with keys: dag_path, dag_code, sql_scripts, ge_suites,
        spark_jobs, validation_passed, validation_messages.
        """
        logger.info(
            "generating_v2_dag",
            dag_id=config.dag_id,
            domain=config.domain,
            feed_count=len(config.feeds),
        )

        # 1. Select template by primary source type
        source_type = config.primary_source_type
        template_name = self.V2_TEMPLATE_MAP.get(source_type)
        if not template_name:
            raise ValueError(
                f"No v2 template for source_type={source_type}. "
                f"Supported: {list(self.V2_TEMPLATE_MAP.keys())}"
            )

        # 2. Build Jinja2 environment for v2 templates
        self.V2_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
        v2_env = Environment(
            loader=FileSystemLoader(str(self.V2_TEMPLATE_DIR)),
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        v2_env.filters["to_python_list"] = self._to_python_list
        v2_env.filters["to_json"] = self._to_json
        v2_env.filters["quote"] = self._quote_string
        v2_env.globals["now"] = datetime.utcnow

        # 3. Build template context from FeedGroupConfig
        context = self._build_feed_group_context(config)

        # 4. Render template
        template = v2_env.get_template(template_name)
        dag_code = template.render(**context)

        # 5. Write DAG file
        output_dir = output_dir or self.DAG_OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        dag_path = output_dir / f"{config.dag_id}.py"
        dag_path.write_text(dag_code)

        # 6. Generate SQL scripts for each feed
        sql_scripts = {}
        for feed in config.feeds:
            feed_sql = generate_all_sql(feed)
            sql_dir = output_dir / "scripts" / config.domain / feed.feed_id
            sql_dir.mkdir(parents=True, exist_ok=True)
            for name, sql_content in feed_sql.items():
                sql_file = sql_dir / f"{name}.sql"
                sql_file.write_text(sql_content)
                sql_scripts[f"{feed.feed_id}/{name}.sql"] = str(sql_file)

        # 7. Generate GE expectation suites
        ge_suites = {}
        for feed in config.feeds:
            suite = generate_ge_expectations(feed)
            ge_dir = output_dir / "validations" / config.domain / feed.feed_id
            ge_dir.mkdir(parents=True, exist_ok=True)
            import json
            suite_file = ge_dir / "expectations.json"
            suite_file.write_text(json.dumps(suite, indent=2))
            ge_suites[feed.feed_id] = str(suite_file)

        # 8. Collect spark job paths
        spark_jobs_dir = Path(__file__).parent.parent / "spark_jobs" / "v2"
        spark_jobs = []
        for job_name in self.V2_SPARK_JOBS:
            job_path = spark_jobs_dir / job_name
            if job_path.exists():
                spark_jobs.append(str(job_path))

        # 9. Validate
        validation_passed, validation_messages = True, []
        try:
            compile(dag_code, f"{config.dag_id}.py", "exec")
        except SyntaxError as e:
            validation_passed = False
            validation_messages.append(f"Syntax error at line {e.lineno}: {e.msg}")

        logger.info(
            "v2_dag_generated",
            dag_id=config.dag_id,
            template=template_name,
            dag_path=str(dag_path),
            sql_count=len(sql_scripts),
            ge_count=len(ge_suites),
            spark_count=len(spark_jobs),
            validation_passed=validation_passed,
        )

        return {
            "dag_id": config.dag_id,
            "dag_path": str(dag_path),
            "dag_code": dag_code,
            "template_used": template_name,
            "sql_scripts": sql_scripts,
            "ge_suites": ge_suites,
            "spark_jobs": spark_jobs,
            "validation_passed": validation_passed,
            "validation_messages": validation_messages,
        }

    def _build_feed_group_context(self, config: FeedGroupConfig) -> Dict[str, Any]:
        """Build Jinja2 template context from FeedGroupConfig."""
        # Serialize feeds for template
        feeds_data = []
        for feed in config.feeds:
            feed_dict = feed.model_dump()
            # Ensure columns are serializable
            feed_dict["columns"] = [c.model_dump() for c in feed.columns]
            feed_dict["quality_rules"] = [r.model_dump() for r in feed.quality_rules]
            if feed.gold_model:
                feed_dict["gold_model"] = feed.gold_model.model_dump()
            feeds_data.append(feed_dict)

        context = {
            "feed_group_id": config.feed_group_id,
            "dag_id": config.dag_id,
            "domain": config.domain,
            "owner": config.owner,
            "description": config.description or f"Pipeline {config.dag_id}",
            "feeds": feeds_data,
            "enable_duplicate_detection": config.enable_duplicate_detection,
            "enable_self_retrigger": config.enable_self_retrigger,
            "dependencies": [d.model_dump() for d in config.dependencies],
            "environment": "dev",
            "start_year": config.start_year,
            "start_month": config.start_month,
            "start_day": config.start_day,
            "schedule": config.schedule,
            "notification_emails": config.notification_emails,
            "retry_count": config.retry_count,
            "depends_on_past": False,
            "catchup": config.catchup,
            "max_active_runs": config.max_active_runs,
            "tags": config.tags,
        }

        return context

    def _build_context(
        self,
        config: APEXPipelineConfig,
        pattern: PatternCode,
    ) -> Dict[str, Any]:
        """
        Build template context from APEX configuration.

        Args:
            config: APEX pipeline configuration
            pattern: Selected pattern code

        Returns:
            Template context dictionary
        """
        # Build base template context from config
        context = {
            "pipeline_id": config.pipeline_id,
            "pipeline_name": config.pipeline_name,
            "dag_id": config.dag_id,
            "feed_id": config.feed_id,
            "feed_name": config.feed.feed_name if config.feed else config.pipeline_name,
            "source_id": config.source_id,
            "domain_id": config.domain_id,
            "domain": config.domain_id,  # Template expects 'domain' variable
            "contract_id": config.contract_id,
            "pattern_code": pattern.value,
            "pattern_name": pattern.name,
        }

        # Add view SQL if defined
        if config.views:
            if ZoneLevel.SILVER in config.views:
                context["silver_view_sql"] = config.views[ZoneLevel.SILVER].view_sql
            if ZoneLevel.GOLD in config.views:
                context["gold_view_sql"] = config.views[ZoneLevel.GOLD].view_sql

        # Add transformation rules
        context["transformation_rules"] = [
            rule.model_dump() for rule in (config.transformations or [])
        ]

        # Add validation rules
        context["validation_rules"] = [
            rule.model_dump() for rule in (config.validations or [])
        ]

        # Add quality expectations (if available)
        context["quality_expectations"] = [
            exp.model_dump() for exp in getattr(config, "quality_expectations", []) or []
        ]

        # Add dependencies (if available)
        context["dependencies"] = [
            dep.model_dump() for dep in getattr(config, "dependencies", []) or []
        ]

        # Add pattern-specific context (with safe error handling)
        try:
            context = self._add_pattern_specific_context(context, pattern, config)
        except Exception as e:
            logger.warning("pattern_context_failed", error=str(e), pattern=pattern.value)
            # Continue with basic context

        # Add environment configuration with safe defaults
        context.setdefault("environment", "dev")
        context.setdefault("gcs_prefix", "gs://data-lake")
        context.setdefault("bq_project", config.domain_id)

        # Add DAG execution defaults
        context.setdefault("retry_count", 3)
        context.setdefault("owner", "apex-data-agent")
        context.setdefault("depends_on_past", False)
        context.setdefault("notification_emails", [])
        context.setdefault("schedule_interval", "@daily")
        context.setdefault("catchup", False)
        context.setdefault("max_active_runs", 1)

        # Add start_date (default to yesterday to allow immediate runs)
        from datetime import datetime, timedelta
        start_date = datetime.now() - timedelta(days=1)
        context.setdefault("start_date", {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day,
        })

        # Add bronze schema columns for template
        if config.schemas and "bronze" in config.schemas:
            context["bronze_columns"] = [
                col.model_dump() for col in config.schemas["bronze"].columns
            ]
        else:
            context["bronze_columns"] = []

        return context

    def _add_pattern_specific_context(
        self,
        context: Dict[str, Any],
        pattern: PatternCode,
        config: APEXPipelineConfig,
    ) -> Dict[str, Any]:
        """Add pattern-specific context variables."""

        if pattern == PatternCode.P02_BIGDATA_FILE:
            # Big data pattern needs resource configuration
            context["file_size_threshold_gb"] = 10
            context["partition_size_mb"] = 256
            context["executor_memory"] = config.spark_config.executor_memory if config.spark_config else "8g"
            context["executor_cores"] = config.spark_config.executor_cores if config.spark_config else 4
            context["num_executors"] = config.spark_config.num_executors if config.spark_config else 10

        elif pattern == PatternCode.P03_DATABASE_LAKEHOUSE:
            # Database pattern needs connection and watermark config
            context["source_connection_id"] = config.source.connection_id if config.source else None
            context["watermark_column"] = getattr(config.contract, "watermark_column", "updated_at")

        elif pattern == PatternCode.P04_LEGACY_MIGRATION:
            # Legacy pattern needs copybook and encoding config
            context["legacy_source_type"] = self._get_legacy_source_type(config)
            context["copybook_path"] = config.feed.copybook_path if config.feed else None
            context["encoding"] = config.feed.encoding if config.feed else "EBCDIC"
            context["record_length"] = config.feed.record_length if config.feed else 0

            # SP migration variant: inject dependency graph data when present
            legacy_mig = (config.source and config.source.source_config or {}).get("legacy_migration_output")
            if legacy_mig:
                context["job_id"] = legacy_mig.get("job_id", "")
                context["stored_procs"] = legacy_mig.get("dependency_graph", {}).get("nodes", [])
                context["sp_execution_order"] = legacy_mig.get("sp_execution_order", [])
                context["sp_execution_batches"] = legacy_mig.get("sp_execution_batches", {})
                context["objects_extracted"] = legacy_mig.get("objects_extracted", 0)
                context["extraction_source"] = legacy_mig.get("extraction_source", "UNKNOWN")
                context["skipped_objects"] = legacy_mig.get("skipped_objects", [])

        elif pattern == PatternCode.P05_STREAMING_BATCH:
            # Streaming pattern needs topic and consumer config
            context["streaming_source"] = "KAFKA"  # Default
            context["topic_name"] = config.source.source_config.get("topic") if config.source else None
            context["consumer_group"] = f"apex-{config.feed.feed_id}"
            context["batch_interval_minutes"] = 5
            context["checkpoint_location"] = f"gs://checkpoints/{config.feed.feed_id}"

        elif pattern == PatternCode.P06_API_SAAS:
            # API pattern needs endpoint and auth config
            context["api_type"] = "REST"
            context["api_connection_id"] = config.source.connection_id if config.source else None
            context["api_endpoint"] = config.source.source_config.get("endpoint") if config.source else None
            context["pagination_type"] = "OFFSET"
            context["page_size"] = 1000

        elif pattern == PatternCode.P07_SCD2:
            # SCD2 pattern needs business keys and tracked columns
            context["business_keys"] = getattr(config.contract, "primary_keys", ["id"])
            context["tracked_columns"] = self._get_tracked_columns(config)
            context["surrogate_key_name"] = "dim_key"

        elif pattern == PatternCode.P08_DATA_VAULT:
            # Data Vault pattern needs hub, link, satellite definitions
            context["record_source"] = config.feed.feed_name
            context["hubs"] = self._extract_hub_definitions(config)
            context["links"] = self._extract_link_definitions(config)
            context["satellites"] = self._extract_satellite_definitions(config)

        elif pattern == PatternCode.P09_STAR_SCHEMA:
            # Star schema pattern needs dimension and fact definitions
            context["dimensions"] = self._extract_dimension_definitions(config)
            context["fact_table"] = self._extract_fact_definition(config)
            context["aggregates"] = []

        return context

    def _get_legacy_source_type(self, config: APEXPipelineConfig) -> str:
        """Determine legacy source type from configuration."""
        if config.feed:
            if config.feed.copybook_path:
                return "COBOL"
            if config.feed.encoding == "EBCDIC":
                return "EBCDIC"
        if config.source and config.source.source_config:
            source_config = config.source.source_config
            if "dtsx" in str(source_config).lower():
                return "DTSX"
            if "as400" in str(source_config).lower():
                return "AS400"
        return "MAINFRAME"

    def _get_tracked_columns(self, config: APEXPipelineConfig) -> List[str]:
        """Get columns to track for SCD2 from transformation rules."""
        tracked = []
        for rule in config.transformation_rules:
            if rule.rule_type == "SCD2" and rule.tracked_columns:
                tracked.extend(rule.tracked_columns)
        # If no tracked columns defined, use all non-key columns from schema
        if not tracked and ZoneLevel.SILVER in config.schemas:
            schema = config.schemas[ZoneLevel.SILVER]
            pk_set = set(config.contract.primary_keys)
            tracked = [col.column_name for col in schema.columns if col.column_name not in pk_set]
        return tracked

    def _extract_hub_definitions(self, config: APEXPipelineConfig) -> List[Dict[str, Any]]:
        """Extract Data Vault hub definitions from configuration."""
        # This would be populated from contract metadata
        # For now, create a default hub from primary keys
        return [{
            "name": f"hub_{config.feed.feed_name}",
            "business_keys": config.contract.primary_keys,
            "columns": config.contract.primary_keys,
        }]

    def _extract_link_definitions(self, config: APEXPipelineConfig) -> List[Dict[str, Any]]:
        """Extract Data Vault link definitions from configuration."""
        return []  # Would be populated from contract metadata

    def _extract_satellite_definitions(self, config: APEXPipelineConfig) -> List[Dict[str, Any]]:
        """Extract Data Vault satellite definitions from configuration."""
        # Create default satellite from schema
        attributes = []
        if ZoneLevel.SILVER in config.schemas:
            pk_set = set(config.contract.primary_keys)
            attributes = [
                col.column_name
                for col in config.schemas[ZoneLevel.SILVER].columns
                if col.column_name not in pk_set
            ]
        return [{
            "name": f"sat_{config.feed.feed_name}",
            "hub": f"hub_{config.feed.feed_name}",
            "attributes": attributes,
        }]

    def _extract_dimension_definitions(self, config: APEXPipelineConfig) -> List[Dict[str, Any]]:
        """Extract star schema dimension definitions."""
        # Would be populated from contract metadata
        return [{
            "name": f"dim_{config.feed.feed_name}",
            "business_key": config.contract.primary_keys[0] if config.contract.primary_keys else "id",
            "scd_type": 1,
            "attributes": [],
        }]

    def _extract_fact_definition(self, config: APEXPipelineConfig) -> Dict[str, Any]:
        """Extract star schema fact table definition."""
        return {
            "name": f"fact_{config.feed.feed_name}",
            "grain": config.contract.primary_keys,
            "measures": [],
            "dimension_keys": [],
        }

    def _render_template(
        self,
        template_name: str,
        context: Dict[str, Any],
    ) -> str:
        """
        Render a Jinja2 template.

        Args:
            template_name: Template filename
            context: Template variables

        Returns:
            Rendered code string
        """
        try:
            template = self.env.get_template(template_name)
            return template.render(**context)
        except TemplateNotFound:
            raise ValueError(f"Template not found: {template_name}")
        except Exception as e:
            logger.error("template_render_failed", template=template_name, error=str(e))
            raise

    def _validate_dag(
        self,
        dag_code: str,
        dag_id: str,
    ) -> tuple[bool, List[str]]:
        """
        Validate generated DAG code.

        Args:
            dag_code: Generated Python code
            dag_id: DAG identifier

        Returns:
            Tuple of (is_valid, messages)
        """
        messages = []
        is_valid = True

        # Check for syntax errors
        try:
            compile(dag_code, f"{dag_id}.py", "exec")
        except SyntaxError as e:
            is_valid = False
            messages.append(f"Syntax error at line {e.lineno}: {e.msg}")

        # Check for required imports
        required_imports = [
            "from airflow import DAG",
            "from dag_utilities",
        ]
        for imp in required_imports:
            if imp not in dag_code:
                messages.append(f"Missing required import: {imp}")

        # Check for DAG definition
        if f'dag_id="{dag_id}"' not in dag_code and f"dag_id='{dag_id}'" not in dag_code:
            # Check for template variable version
            if "dag_id={{ dag_id }}" not in dag_code and 'dag_id="{{ dag_id }}"' not in dag_code:
                messages.append(f"DAG ID '{dag_id}' not found in generated code")

        return is_valid, messages

    @staticmethod
    def _to_python_list(value: List[Any]) -> str:
        """Convert list to Python list literal."""
        if not value:
            return "[]"
        items = [f'"{v}"' if isinstance(v, str) else str(v) for v in value]
        return "[" + ", ".join(items) + "]"

    @staticmethod
    def _to_json(value: Any) -> str:
        """Convert value to JSON string."""
        import json
        return json.dumps(value)

    @staticmethod
    def _quote_string(value: str) -> str:
        """Quote a string for Python code."""
        if value is None:
            return '""'
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'

    def list_generated_dags(self) -> List[Dict[str, Any]]:
        """List all generated DAGs."""
        dags = []
        if self.DAG_OUTPUT_DIR.exists():
            for dag_file in self.DAG_OUTPUT_DIR.glob("*.py"):
                stat = dag_file.stat()
                dags.append({
                    "dag_id": dag_file.stem,
                    "path": str(dag_file),
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
        return dags
