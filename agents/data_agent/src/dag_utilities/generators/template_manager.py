"""
Template Manager - Auto-Create DAG Templates

Manages Jinja2 DAG templates with auto-creation capability:
- Resolves template by source_category + contract_type
- Auto-creates from base_medallion if missing
- Category-specific customizations (sensor, reader, writer)
- Contract-specific customizations (merge_type, history_tracking)

Template naming convention:
    {source_category}_{contract_type}.py.jinja2
    e.g., file_standard.py.jinja2, database_scd2.py.jinja2

If template doesn't exist → auto-create from base_medallion.py.jinja2
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

logger = logging.getLogger(__name__)


# Category-specific customizations for auto-created templates
CATEGORY_CUSTOMIZATIONS: Dict[str, Dict[str, Any]] = {
    "FILE": {
        "sensor_type": "GCSObjectExistenceSensor",
        "reader": "spark.read",
        "source_check_type": "gcs_file_sensor",
        "landing_method": "gcs_copy",
        "source_imports": [
            "from dag_utilities.sources.file_source import get_gcs_file_sensor, list_source_files",
        ],
    },
    "DATABASE": {
        "sensor_type": "SqlSensor",
        "reader": "spark.read.jdbc",
        "source_check_type": "sql_query",
        "landing_method": "jdbc_extract",
        "source_imports": [
            "from dag_utilities.sources.database_source import get_sql_sensor, extract_table",
        ],
    },
    "STREAMING": {
        "sensor_type": "PubSubPullSensor",
        "reader": "spark.readStream",
        "source_check_type": "pubsub_subscription",
        "landing_method": "pubsub_pull",
        "source_imports": [
            "from dag_utilities.sources.streaming_source import get_pubsub_sensor, pull_messages",
        ],
    },
    "API": {
        "sensor_type": "HttpSensor",
        "reader": "requests_to_df",
        "source_check_type": "http_endpoint",
        "landing_method": "api_extract",
        "source_imports": [
            "from dag_utilities.sources.api_source import get_http_sensor, extract_api_data",
        ],
    },
    "LEGACY": {
        "sensor_type": "GCSObjectExistenceSensor",
        "reader": "ebcdic_reader",
        "source_check_type": "gcs_file_sensor",
        "landing_method": "legacy_extract",
        "source_imports": [
            "from dag_utilities.sources.legacy_source import get_legacy_sensor, extract_legacy_data",
        ],
    },
    "SAAS": {
        "sensor_type": "HttpSensor",
        "reader": "saas_connector",
        "source_check_type": "http_endpoint",
        "landing_method": "saas_extract",
        "source_imports": [
            "from dag_utilities.sources.api_source import get_http_sensor, extract_saas_data",
        ],
    },
    "NOSQL": {
        "sensor_type": "PythonSensor",
        "reader": "nosql_connector",
        "source_check_type": "nosql_ping",
        "landing_method": "nosql_extract",
        "source_imports": [
            "from dag_utilities.sources.database_source import get_nosql_sensor, extract_nosql_data",
        ],
    },
    "LOGS": {
        "sensor_type": "GCSObjectExistenceSensor",
        "reader": "log_parser",
        "source_check_type": "gcs_file_sensor",
        "landing_method": "log_extract",
        "source_imports": [
            "from dag_utilities.sources.file_source import get_gcs_file_sensor, parse_logs",
        ],
    },
    "CLOUD": {
        "sensor_type": "GCSObjectExistenceSensor",
        "reader": "cloud_storage_reader",
        "source_check_type": "gcs_file_sensor",
        "landing_method": "cloud_copy",
        "source_imports": [
            "from dag_utilities.sources.file_source import get_gcs_file_sensor, copy_cloud_files",
        ],
    },
}

# Contract-specific customizations for auto-created templates
CONTRACT_CUSTOMIZATIONS: Dict[str, Dict[str, Any]] = {
    "STANDARD": {
        "merge_type": "simple",
        "history_tracking": False,
        "data_vault_enabled": False,
        "scd_type": None,
        "trusted_write_mode": "merge",
        "consumption_write_mode": "overwrite",
    },
    "SCD2": {
        "merge_type": "scd2",
        "history_tracking": True,
        "data_vault_enabled": False,
        "scd_type": "type2",
        "trusted_write_mode": "merge",
        "consumption_write_mode": "merge",
    },
    "DATA_VAULT": {
        "merge_type": "data_vault",
        "history_tracking": True,
        "data_vault_enabled": True,
        "scd_type": None,
        "trusted_write_mode": "merge",
        "consumption_write_mode": "merge",
    },
    "STAR_SCHEMA": {
        "merge_type": "dimension",
        "history_tracking": False,
        "data_vault_enabled": False,
        "scd_type": None,
        "trusted_write_mode": "overwrite",
        "consumption_write_mode": "overwrite",
    },
}


class TemplateManager:
    """
    Manages DAG templates with auto-creation capability.

    Template resolution:
    1. Look for specific template: {category}_{contract}.py.jinja2
    2. If not found: auto-create from base_medallion.py.jinja2
    3. Cache template for future use
    """

    TEMPLATE_DIR = Path(__file__).parent.parent.parent / "templates" / "patterns"
    BASE_TEMPLATE = "base_medallion.py.jinja2"

    def __init__(self, template_dir: Optional[Path] = None):
        """
        Initialize template manager.

        Args:
            template_dir: Override template directory path (for testing)
        """
        self._template_dir = template_dir or self.TEMPLATE_DIR
        self._jinja_env: Optional[Environment] = None

    @property
    def jinja_env(self) -> Environment:
        """Get or create Jinja2 environment."""
        if self._jinja_env is None:
            self._template_dir.mkdir(parents=True, exist_ok=True)
            self._jinja_env = Environment(
                loader=FileSystemLoader(str(self._template_dir)),
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
            )
        return self._jinja_env

    def get_template(
        self,
        source_category: str,
        contract_type: str = "STANDARD"
    ) -> Any:
        """
        Get template, creating if it doesn't exist.

        Args:
            source_category: Source category (FILE, DATABASE, etc.)
            contract_type: Contract type (STANDARD, SCD2, DATA_VAULT, STAR_SCHEMA)

        Returns:
            Jinja2 Template object
        """
        template_name = self._build_template_name(source_category, contract_type)
        template_path = self._template_dir / template_name

        # Auto-create if missing
        if not template_path.exists():
            logger.info(
                "Template not found, auto-creating from base",
                extra={
                    "template": template_name,
                    "source_category": source_category,
                    "contract_type": contract_type,
                }
            )
            self._create_template(template_path, source_category, contract_type)
            # Reset Jinja env to pick up new template
            self._jinja_env = None

        return self.jinja_env.get_template(template_name)

    def get_pattern_template(self, pattern_code: str, production: bool = False) -> Any:
        """
        Get template by pattern code (P01, P02, etc.).

        Args:
            pattern_code: Pattern code (P01-P09)
            production: If True, prefer production-grade templates (thin DAG pattern
                       with full dag_utilities integration, GCS sensors, PVC config,
                       rejection workflows, etc.)

        Resolution order (when production=True):
            1. Production template: p01_file_production.py.jinja2
            2. Standard template: p01_file_medallion.py.jinja2
            3. Base template: base_medallion.py.jinja2

        Falls back to base_medallion if pattern template not found.
        """
        # Production template mapping (thin DAG pattern with full runtime)
        production_map = {
            "P01": "p01_file_production.py.jinja2",
        }

        # Standard template mapping
        pattern_map = {
            "P01": "p01_file_medallion.py.jinja2",
            "P02": "p02_bigdata_file.py.jinja2",
            "P03": "p03_database_lakehouse.py.jinja2",
            "P04": "p04_legacy_migration.py.jinja2",
            "P05": "p05_streaming_batch.py.jinja2",
            "P06": "p06_api_saas.py.jinja2",
            "P07": "p07_scd2.py.jinja2",
            "P08": "p08_data_vault.py.jinja2",
            "P09": "p09_star_schema.py.jinja2",
        }

        # Try production template first if requested
        if production and pattern_code in production_map:
            prod_name = production_map[pattern_code]
            prod_path = self._template_dir / prod_name
            if prod_path.exists():
                logger.info(f"Using production template: {prod_name}")
                return self.jinja_env.get_template(prod_name)

        template_name = pattern_map.get(pattern_code, self.BASE_TEMPLATE)
        template_path = self._template_dir / template_name

        if not template_path.exists():
            logger.info(
                f"Pattern template {template_name} not found, using base template"
            )
            template_name = self.BASE_TEMPLATE

        return self.jinja_env.get_template(template_name)

    def get_group_template(self) -> Any:
        """
        Get the multi-feed group production template.

        Used when a DAG processes multiple feeds as nested TaskGroups.
        Falls back to base_medallion if not found.
        """
        group_template = "p01_file_group_production.py.jinja2"
        group_path = self._template_dir / group_template

        if group_path.exists():
            logger.info(f"Using group production template: {group_template}")
            return self.jinja_env.get_template(group_template)

        logger.warning("Group template not found, falling back to base")
        return self.jinja_env.get_template(self.BASE_TEMPLATE)

    def _build_template_name(
        self,
        source_category: str,
        contract_type: str
    ) -> str:
        """Build template filename from category and contract type."""
        return f"{source_category.lower()}_{contract_type.lower()}.py.jinja2"

    def _create_template(
        self,
        path: Path,
        source_category: str,
        contract_type: str
    ) -> None:
        """
        Auto-create template from base with category/contract customizations.

        Creates a Jinja2 template that extends base_medallion.py.jinja2
        with source-specific and contract-specific blocks.
        """
        category_config = CATEGORY_CUSTOMIZATIONS.get(
            source_category.upper(), CATEGORY_CUSTOMIZATIONS["FILE"]
        )
        contract_config = CONTRACT_CUSTOMIZATIONS.get(
            contract_type.upper(), CONTRACT_CUSTOMIZATIONS["STANDARD"]
        )

        # Generate template content
        source_imports = "\n".join(category_config.get("source_imports", []))

        template_content = f'''{{% extends "base_medallion.py.jinja2" %}}

{{# Auto-generated template for {source_category} + {contract_type} #}}
{{# Created by TemplateManager - DO NOT EDIT MANUALLY #}}
{{# To customize: override specific blocks below #}}

{{% block pattern_code %}}{source_category}_{contract_type}{{% endblock %}}
{{% block pattern_name %}}{source_category.title()} {contract_type.title()} Pipeline{{% endblock %}}
{{% block pattern_code_value %}}{source_category}_{contract_type}{{% endblock %}}
{{% block pattern_description %}}{source_category.title()} source with {contract_type.title()} contract pattern{{% endblock %}}

{{% block additional_imports %}}
{source_imports}
{{% endblock %}}

{{% block source_check_function %}}
def check_source_available(**context) -> str:
    """
    Source check for {source_category} sources.
    Sensor type: {category_config.get("sensor_type", "PythonSensor")}
    """
    exec_ctx = ExecutionContext.from_airflow_context(context)

    # Source-specific availability check
    audit.log_event(
        "SOURCE_CHECK_STARTED",
        feed_id=FEED_ID,
        source_type="{source_category}",
    )

    # Delegate to source-specific handler
    try:
        from dag_utilities.sources import check_source_availability
        available = check_source_availability(
            feed_id=FEED_ID,
            metadata_client=metadata,
            source_category="{source_category}",
        )
    except ImportError:
        # Fallback: assume available
        available = True

    if not available:
        audit.log_event(
            "SOURCE_CHECK_NO_DATA",
            feed_id=FEED_ID,
            severity="WARNING",
        )
        return "skip_no_data"

    audit.log_event("SOURCE_CHECK_PASSED", feed_id=FEED_ID)
    return "contract_gate"
{{% endblock %}}

{{% block dag_description %}}{{{{ dag_id }}}} - {source_category.title()} {contract_type.title()} Pipeline{{% endblock %}}

{{% block additional_tasks %}}
{{# {contract_type}-specific tasks go here #}}
{{% if contract_type == "SCD2" %}}
    {{# SCD2: Add history tracking task after gold zone #}}
{{% endif %}}
{{% if contract_type == "DATA_VAULT" %}}
    {{# DATA_VAULT: Add hub/link/satellite generation tasks #}}
{{% endif %}}
{{% endblock %}}
'''

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(template_content)

        logger.info(f"Auto-created template: {path.name}")

    def list_templates(self) -> Dict[str, bool]:
        """
        List all expected templates and their availability.

        Returns:
            Dict mapping template name to exists (True/False)
        """
        templates = {}

        for category in CATEGORY_CUSTOMIZATIONS:
            for contract in CONTRACT_CUSTOMIZATIONS:
                name = self._build_template_name(category, contract)
                path = self._template_dir / name
                templates[name] = path.exists()

        # Also check pattern templates
        for pattern_code in ["P01", "P02", "P03", "P04", "P05", "P06", "P07", "P08", "P09"]:
            template_map = {
                "P01": "p01_file_medallion.py.jinja2",
                "P02": "p02_bigdata_file.py.jinja2",
                "P03": "p03_database_lakehouse.py.jinja2",
                "P04": "p04_legacy_migration.py.jinja2",
                "P05": "p05_streaming_batch.py.jinja2",
                "P06": "p06_api_saas.py.jinja2",
                "P07": "p07_scd2.py.jinja2",
                "P08": "p08_data_vault.py.jinja2",
                "P09": "p09_star_schema.py.jinja2",
            }
            name = template_map[pattern_code]
            path = self._template_dir / name
            templates[name] = path.exists()

        # Production templates
        production_templates = [
            "p01_file_production.py.jinja2",
            "p01_file_group_production.py.jinja2",
        ]
        for name in production_templates:
            path = self._template_dir / name
            templates[name] = path.exists()

        # Base template
        templates[self.BASE_TEMPLATE] = (self._template_dir / self.BASE_TEMPLATE).exists()

        return templates

    def ensure_base_template_exists(self) -> bool:
        """Check that base template exists."""
        return (self._template_dir / self.BASE_TEMPLATE).exists()


__all__ = [
    "TemplateManager",
    "CATEGORY_CUSTOMIZATIONS",
    "CONTRACT_CUSTOMIZATIONS",
]
