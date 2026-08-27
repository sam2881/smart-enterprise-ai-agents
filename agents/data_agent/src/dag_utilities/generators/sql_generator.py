"""
SQL Generator - Step 2 of Agent Workflow

Generates INSERT and UPDATE SQL for metadata tables.
This is the FIRST OUTPUT - generated BEFORE any DAG or Spark generation.

Tables:
- feed_details
- feed_group_details
- contract_details
- schema_details (versioned, effective-dated)
- zone_configurations
- quality_rules (ge_validation_ref)
- ge_expectations_store
- join_dependency (if applicable)
- transform_definitions
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import json


@dataclass
class GeneratedSQL:
    """Container for generated SQL statements."""
    table_name: str
    insert_sql: str
    update_sql: Optional[str] = None
    values: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "insert_sql": self.insert_sql,
            "update_sql": self.update_sql,
            "values": self.values,
        }


@dataclass
class SQLGenerationResult:
    """Result of SQL generation for a feed."""
    feed_id: int
    statements: List[GeneratedSQL] = field(default_factory=list)
    combined_insert: str = ""
    combined_update: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_id": self.feed_id,
            "statement_count": len(self.statements),
            "statements": [s.to_dict() for s in self.statements],
            "combined_insert": self.combined_insert,
            "combined_update": self.combined_update,
        }


class SQLGenerator:
    """
    Generates INSERT/UPDATE SQL for metadata tables.

    Step 2 of the 7-step Agent Workflow.
    NO DAG or Spark generation before this step.
    """

    def __init__(self, execution_plan: Any = None):
        """
        Initialize with optional execution plan.

        Args:
            execution_plan: ExecutionPlan from Step 3
        """
        self.plan = execution_plan

    def generate(
        self,
        feed_config: Dict[str, Any],
        contract_config: Dict[str, Any],
        schema_config: Dict[str, Any],
        zones: List[Dict[str, Any]],
        quality_rules: List[Dict[str, Any]],
        transforms: List[Dict[str, Any]] = None
    ) -> SQLGenerationResult:
        """
        Generate all metadata SQL for a feed.

        Args:
            feed_config: Feed configuration dictionary
            contract_config: Contract configuration dictionary
            schema_config: Schema configuration dictionary
            zones: List of zone configurations
            quality_rules: List of quality rule configurations
            transforms: Optional list of transform configurations

        Returns:
            SQLGenerationResult with all generated SQL
        """
        feed_id = feed_config.get("feed_id", 0)
        result = SQLGenerationResult(feed_id=feed_id)

        # 1. Generate feed_group_details SQL
        if feed_config.get("feed_group_id"):
            result.statements.append(
                self._generate_feed_group_sql(feed_config)
            )

        # 2. Generate feed_details SQL
        result.statements.append(
            self._generate_feed_details_sql(feed_config)
        )

        # 3. Generate contract_details SQL
        result.statements.append(
            self._generate_contract_details_sql(contract_config, feed_id)
        )

        # 4. Generate schema_details SQL (versioned)
        result.statements.append(
            self._generate_schema_details_sql(schema_config, feed_id)
        )

        # 5. Generate zone_configurations SQL
        for zone in zones:
            result.statements.append(
                self._generate_zone_config_sql(zone, feed_id)
            )

        # 6. Generate quality_rules SQL
        for rule in quality_rules:
            result.statements.append(
                self._generate_quality_rule_sql(rule, feed_id)
            )

        # 7. Generate transform_definitions SQL
        if transforms:
            for transform in transforms:
                result.statements.append(
                    self._generate_transform_sql(transform, feed_id)
                )

        # Combine all SQL
        result.combined_insert = self._combine_inserts(result.statements)
        result.combined_update = self._combine_updates(result.statements)

        return result

    def _generate_feed_group_sql(self, feed_config: Dict[str, Any]) -> GeneratedSQL:
        """Generate SQL for feed_group_details table."""
        feed_group_id = feed_config.get("feed_group_id")
        feed_group_name = feed_config.get("feed_group_name", f"group_{feed_group_id}")
        domain = feed_config.get("domain", "default")
        source_system = feed_config.get("source_system", "unknown")
        owner = feed_config.get("owner", "apex-data-agent")
        description = feed_config.get("description", "")

        insert_sql = f"""
INSERT INTO feed_group_details (
    feed_group_id,
    feed_group_name,
    domain,
    source_system,
    owner,
    description,
    is_active,
    created_at,
    updated_at
) VALUES (
    {feed_group_id},
    '{feed_group_name}',
    '{domain}',
    '{source_system}',
    '{owner}',
    '{description}',
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (feed_group_id) DO UPDATE SET
    feed_group_name = EXCLUDED.feed_group_name,
    description = EXCLUDED.description,
    updated_at = NOW();
"""

        return GeneratedSQL(
            table_name="feed_group_details",
            insert_sql=insert_sql.strip(),
            values={"feed_group_id": feed_group_id},
        )

    def _generate_feed_details_sql(self, feed_config: Dict[str, Any]) -> GeneratedSQL:
        """Generate SQL for feed_details table."""
        feed_id = feed_config.get("feed_id")
        feed_group_id = feed_config.get("feed_group_id")
        feed_name = feed_config.get("feed_name", f"feed_{feed_id}")
        source_type = feed_config.get("source_type", "file_csv")
        source_config = json.dumps(feed_config.get("source_config", {}))
        file_format = feed_config.get("file_format", "csv")
        delimiter = feed_config.get("delimiter", ",")
        has_header = feed_config.get("has_header", True)
        encoding = feed_config.get("encoding", "UTF-8")

        insert_sql = f"""
INSERT INTO feed_details (
    feed_id,
    feed_group_id,
    feed_name,
    source_type,
    source_config,
    file_format,
    delimiter,
    has_header,
    encoding,
    is_active,
    created_at,
    updated_at
) VALUES (
    {feed_id},
    {feed_group_id or 'NULL'},
    '{feed_name}',
    '{source_type}',
    '{source_config}'::jsonb,
    '{file_format}',
    '{delimiter}',
    {str(has_header).upper()},
    '{encoding}',
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (feed_id) DO UPDATE SET
    source_config = EXCLUDED.source_config,
    updated_at = NOW();
"""

        update_sql = f"""
UPDATE feed_details
SET source_config = '{source_config}'::jsonb,
    updated_at = NOW()
WHERE feed_id = {feed_id};
"""

        return GeneratedSQL(
            table_name="feed_details",
            insert_sql=insert_sql.strip(),
            update_sql=update_sql.strip(),
            values={"feed_id": feed_id},
        )

    def _generate_contract_details_sql(
        self,
        contract_config: Dict[str, Any],
        feed_id: int
    ) -> GeneratedSQL:
        """Generate SQL for contract_details table."""
        contract_id = contract_config.get("contract_id", feed_id * 2)
        contract_type = contract_config.get("contract_type", "STANDARD")
        pattern_code = contract_config.get("pattern_code", "P01")
        primary_keys = contract_config.get("primary_keys", [])
        partition_column = contract_config.get("partition_column", "")
        quality_threshold = contract_config.get("quality_threshold", 0.95)
        schema_evolution_mode = contract_config.get("schema_evolution_mode", "strict")
        data_vault_type = contract_config.get("data_vault_type")
        business_keys = contract_config.get("business_keys", [])

        primary_keys_array = "ARRAY[" + ", ".join(f"'{k}'" for k in primary_keys) + "]" if primary_keys else "NULL"
        business_keys_array = "ARRAY[" + ", ".join(f"'{k}'" for k in business_keys) + "]" if business_keys else "NULL"

        insert_sql = f"""
INSERT INTO contract_details (
    contract_id,
    feed_id,
    contract_type,
    pattern_code,
    primary_keys,
    partition_column,
    quality_threshold,
    schema_evolution_mode,
    data_vault_type,
    business_keys,
    is_active,
    created_at,
    updated_at
) VALUES (
    {contract_id},
    {feed_id},
    '{contract_type}',
    '{pattern_code}',
    {primary_keys_array},
    '{partition_column}',
    {quality_threshold},
    '{schema_evolution_mode}',
    {f"'{data_vault_type}'" if data_vault_type else 'NULL'},
    {business_keys_array},
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (contract_id) DO UPDATE SET
    quality_threshold = EXCLUDED.quality_threshold,
    updated_at = NOW();
"""

        return GeneratedSQL(
            table_name="contract_details",
            insert_sql=insert_sql.strip(),
            values={"contract_id": contract_id, "feed_id": feed_id},
        )

    def _generate_schema_details_sql(
        self,
        schema_config: Dict[str, Any],
        feed_id: int
    ) -> GeneratedSQL:
        """Generate SQL for schema_details table (versioned)."""
        schema_id = schema_config.get("schema_id", feed_id * 3)
        version_number = schema_config.get("version_number", 1)
        columns = schema_config.get("columns", [])
        schema_definition = json.dumps({"columns": columns})

        insert_sql = f"""
INSERT INTO schema_versions (
    schema_id,
    feed_id,
    version_number,
    schema_definition,
    is_current,
    effective_from,
    created_at
) VALUES (
    {schema_id},
    {feed_id},
    {version_number},
    '{schema_definition}'::jsonb,
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (schema_id) DO UPDATE SET
    schema_definition = EXCLUDED.schema_definition,
    is_current = TRUE;

-- Set previous versions as not current
UPDATE schema_versions
SET is_current = FALSE
WHERE feed_id = {feed_id}
  AND schema_id != {schema_id}
  AND is_current = TRUE;
"""

        return GeneratedSQL(
            table_name="schema_versions",
            insert_sql=insert_sql.strip(),
            values={"schema_id": schema_id, "feed_id": feed_id},
        )

    def _generate_zone_config_sql(
        self,
        zone_config: Dict[str, Any],
        feed_id: int
    ) -> GeneratedSQL:
        """Generate SQL for zone_configurations table."""
        zone_config_id = zone_config.get("zone_config_id")
        zone_level = zone_config.get("zone_level")
        dataset_name = zone_config.get("dataset_name", f"{zone_level}_data")
        table_name = zone_config.get("table_name", f"feed_{feed_id}_{zone_level}")
        write_mode = zone_config.get("write_mode", "append")
        partition_config = json.dumps(zone_config.get("partition_config", {}))

        insert_sql = f"""
INSERT INTO zone_configurations (
    zone_config_id,
    feed_id,
    zone_level,
    dataset_name,
    table_name,
    write_mode,
    partition_config,
    is_active,
    created_at
) VALUES (
    {zone_config_id},
    {feed_id},
    '{zone_level}',
    '{dataset_name}',
    '{table_name}',
    '{write_mode}',
    '{partition_config}'::jsonb,
    TRUE,
    NOW()
) ON CONFLICT (zone_config_id) DO UPDATE SET
    dataset_name = EXCLUDED.dataset_name,
    table_name = EXCLUDED.table_name,
    write_mode = EXCLUDED.write_mode;
"""

        return GeneratedSQL(
            table_name="zone_configurations",
            insert_sql=insert_sql.strip(),
            values={"zone_config_id": zone_config_id, "zone_level": zone_level},
        )

    def _generate_quality_rule_sql(
        self,
        rule: Dict[str, Any],
        feed_id: int
    ) -> GeneratedSQL:
        """Generate SQL for quality_rules table."""
        rule_id = rule.get("rule_id")
        zone_level = rule.get("zone_level", "refined")
        rule_name = rule.get("rule_name", f"rule_{rule_id}")
        rule_type = rule.get("rule_type", "not_null")
        column_name = rule.get("column_name", "")
        rule_config = json.dumps(rule.get("rule_config", {}))
        severity = rule.get("severity", "ERROR")

        insert_sql = f"""
INSERT INTO quality_rules (
    rule_id,
    feed_id,
    zone_level,
    rule_name,
    rule_type,
    column_name,
    rule_config,
    severity,
    is_active,
    created_at
) VALUES (
    {rule_id},
    {feed_id},
    '{zone_level}',
    '{rule_name}',
    '{rule_type}',
    '{column_name}',
    '{rule_config}'::jsonb,
    '{severity}',
    TRUE,
    NOW()
) ON CONFLICT (rule_id) DO UPDATE SET
    rule_config = EXCLUDED.rule_config,
    is_active = EXCLUDED.is_active;
"""

        return GeneratedSQL(
            table_name="quality_rules",
            insert_sql=insert_sql.strip(),
            values={"rule_id": rule_id, "rule_type": rule_type},
        )

    def _generate_transform_sql(
        self,
        transform: Dict[str, Any],
        feed_id: int
    ) -> GeneratedSQL:
        """Generate SQL for transform_definitions table."""
        transform_id = transform.get("transform_id")
        zone_level = transform.get("zone_level", "refined")
        transform_type = transform.get("transform_type", "custom")
        sequence_order = transform.get("sequence_order", 1)
        config = json.dumps(transform.get("config", {}))

        insert_sql = f"""
INSERT INTO transform_definitions (
    transform_id,
    feed_id,
    zone_level,
    transform_type,
    sequence_order,
    config,
    is_active,
    created_at
) VALUES (
    {transform_id},
    {feed_id},
    '{zone_level}',
    '{transform_type}',
    {sequence_order},
    '{config}'::jsonb,
    TRUE,
    NOW()
) ON CONFLICT (transform_id) DO UPDATE SET
    config = EXCLUDED.config,
    sequence_order = EXCLUDED.sequence_order;
"""

        return GeneratedSQL(
            table_name="transform_definitions",
            insert_sql=insert_sql.strip(),
            values={"transform_id": transform_id, "transform_type": transform_type},
        )

    def _combine_inserts(self, statements: List[GeneratedSQL]) -> str:
        """Combine all INSERT statements into a single script."""
        header = f"""-- ═══════════════════════════════════════════════════════════════════════════════
-- APEX Data Agent - Metadata Insert Script
-- Generated: {datetime.utcnow().isoformat()}
-- This SQL is generated BEFORE any DAG or Spark generation (Step 2)
-- ═══════════════════════════════════════════════════════════════════════════════

BEGIN;

"""
        footer = """
COMMIT;
"""
        body = "\n\n".join(s.insert_sql for s in statements)
        return header + body + footer

    def _combine_updates(self, statements: List[GeneratedSQL]) -> str:
        """Combine all UPDATE statements into a single script."""
        updates = [s.update_sql for s in statements if s.update_sql]
        if not updates:
            return ""

        header = f"""-- ═══════════════════════════════════════════════════════════════════════════════
-- APEX Data Agent - Metadata Update Script
-- Generated: {datetime.utcnow().isoformat()}
-- ═══════════════════════════════════════════════════════════════════════════════

BEGIN;

"""
        footer = """
COMMIT;
"""
        body = "\n\n".join(updates)
        return header + body + footer


def generate_metadata_sql(
    feed_config: Dict[str, Any],
    contract_config: Dict[str, Any],
    schema_config: Dict[str, Any],
    zones: List[Dict[str, Any]],
    quality_rules: List[Dict[str, Any]],
    transforms: List[Dict[str, Any]] = None
) -> SQLGenerationResult:
    """
    Convenience function to generate metadata SQL.

    Args:
        feed_config: Feed configuration
        contract_config: Contract configuration
        schema_config: Schema configuration
        zones: Zone configurations
        quality_rules: Quality rules
        transforms: Optional transforms

    Returns:
        SQLGenerationResult
    """
    generator = SQLGenerator()
    return generator.generate(
        feed_config=feed_config,
        contract_config=contract_config,
        schema_config=schema_config,
        zones=zones,
        quality_rules=quality_rules,
        transforms=transforms,
    )


__all__ = [
    "SQLGenerator",
    "GeneratedSQL",
    "SQLGenerationResult",
    "generate_metadata_sql",
]
