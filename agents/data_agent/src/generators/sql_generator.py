"""
APEX Data Agent - Deterministic SQL Generator (v2)

Generates SQL scripts from FeedConfig metadata.
All output is deterministic: same config → same SQL.

Supports:
- BigQuery dialect (primary)
- Snowflake dialect (optional)
- INSERT, MERGE, DELETE+INSERT load strategies
- Data Vault 2.0 (hub, satellite, link)
- Star Schema (dimension with SCD, fact with lookups)
- BigQuery DDL with partitioning and clustering
- Great Expectations expectation suites
"""

import json
from typing import Any, Dict, List, Optional

from src.models.feed_config import (
    ColumnDef,
    DimensionConfig,
    DimensionLookup,
    FactConfig,
    FeedConfig,
    HubConfig,
    LinkConfig,
    MeasureConfig,
    QualityRule,
    SatelliteConfig,
)


# ═══════════════════════════════════════════════════════════════════════════════
# TYPE MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

BQ_TYPE_MAP = {
    "STRING": "STRING",
    "INTEGER": "INT64",
    "BIGINT": "INT64",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "DECIMAL": "NUMERIC",
    "BOOLEAN": "BOOL",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "BINARY": "BYTES",
    "ARRAY": "ARRAY<STRING>",
    "STRUCT": "STRUCT<>",
    "MAP": "JSON",
}

SNOWFLAKE_TYPE_MAP = {
    "STRING": "VARCHAR",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "NUMBER",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "BINARY": "BINARY",
}


def _bq_type(col: ColumnDef) -> str:
    """Map column to BigQuery type."""
    base = BQ_TYPE_MAP.get(col.data_type, "STRING")
    if col.data_type == "DECIMAL" and col.precision and col.scale:
        return f"NUMERIC({col.precision},{col.scale})"
    return base


def _sf_type(col: ColumnDef) -> str:
    """Map column to Snowflake type."""
    base = SNOWFLAKE_TYPE_MAP.get(col.data_type, "VARCHAR")
    if col.data_type == "DECIMAL" and col.precision and col.scale:
        return f"NUMBER({col.precision},{col.scale})"
    return base


def _col_list(columns: List[ColumnDef]) -> str:
    """Comma-separated column name list."""
    return ", ".join(c.name for c in columns)


def _qualified_table(feed: FeedConfig, zone: str = "gold") -> str:
    """Fully qualified BigQuery table name."""
    project = feed.bq_target.project or "${GCP_PROJECT}"
    return f"`{project}.{feed.bq_target.dataset}.{feed.bq_target.table}`"


# ═══════════════════════════════════════════════════════════════════════════════
# INCREMENTAL LOAD SQL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_insert_sql(feed: FeedConfig, source_table: str = "staging") -> str:
    """Generate BigQuery INSERT with optional watermark filter."""
    cols = _col_list(feed.columns)
    target = _qualified_table(feed)

    sql = f"-- INSERT: {feed.feed_name} (incremental)\n"
    sql += f"INSERT INTO {target}\n"
    sql += f"  ({cols}, _load_timestamp, _run_id)\n"
    sql += f"SELECT\n"
    sql += f"  {cols},\n"
    sql += f"  CURRENT_TIMESTAMP() AS _load_timestamp,\n"
    sql += f"  @run_id AS _run_id\n"
    sql += f"FROM `{source_table}`\n"

    if feed.watermark_column:
        sql += f"WHERE {feed.watermark_column} > @last_watermark\n"

    sql += ";\n"
    return sql


def generate_merge_sql(feed: FeedConfig, source_table: str = "staging") -> str:
    """Generate BigQuery MERGE on business_keys."""
    target = _qualified_table(feed)
    keys = feed.business_keys

    if not keys:
        return generate_insert_sql(feed, source_table)

    join_cond = " AND ".join(f"T.{k} = S.{k}" for k in keys)
    update_cols = [c for c in feed.columns if c.name not in keys]
    update_set = ", ".join(f"T.{c.name} = S.{c.name}" for c in update_cols)
    all_cols = _col_list(feed.columns)

    sql = f"-- MERGE: {feed.feed_name} on ({', '.join(keys)})\n"
    sql += f"MERGE INTO {target} AS T\n"
    sql += f"USING `{source_table}` AS S\n"
    sql += f"ON {join_cond}\n"
    sql += f"WHEN MATCHED THEN\n"
    sql += f"  UPDATE SET {update_set},\n"
    sql += f"    T._load_timestamp = CURRENT_TIMESTAMP(),\n"
    sql += f"    T._run_id = @run_id\n"
    sql += f"WHEN NOT MATCHED THEN\n"
    sql += f"  INSERT ({all_cols}, _load_timestamp, _run_id)\n"
    sql += f"  VALUES (S.{', S.'.join(c.name for c in feed.columns)}, CURRENT_TIMESTAMP(), @run_id)\n"
    sql += ";\n"
    return sql


def generate_delete_insert_sql(
    feed: FeedConfig,
    source_table: str = "staging",
    partition_column: Optional[str] = None,
) -> str:
    """Generate DELETE+INSERT for partition-based idempotent loads."""
    target = _qualified_table(feed)
    part_col = partition_column or feed.bq_target.partition_field or "_reporting_date"
    cols = _col_list(feed.columns)

    sql = f"-- DELETE+INSERT: {feed.feed_name} (partition: {part_col})\n"
    sql += f"DELETE FROM {target}\n"
    sql += f"WHERE {part_col} = @execution_date;\n\n"
    sql += f"INSERT INTO {target}\n"
    sql += f"  ({cols}, _load_timestamp, _run_id)\n"
    sql += f"SELECT\n"
    sql += f"  {cols},\n"
    sql += f"  CURRENT_TIMESTAMP() AS _load_timestamp,\n"
    sql += f"  @run_id AS _run_id\n"
    sql += f"FROM `{source_table}`\n"
    sql += f"WHERE {part_col} = @execution_date\n"
    sql += ";\n"
    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# DDL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_bq_ddl(feed: FeedConfig) -> str:
    """Generate BigQuery CREATE TABLE with partitioning and clustering."""
    target = _qualified_table(feed)

    sql = f"-- DDL: {feed.feed_name}\n"
    sql += f"CREATE TABLE IF NOT EXISTS {target}\n(\n"

    col_defs = []
    for c in feed.columns:
        nullable = "" if c.nullable else " NOT NULL"
        desc = f' OPTIONS(description="{c.description}")' if c.description else ""
        col_defs.append(f"  {c.name} {_bq_type(c)}{nullable}{desc}")

    # Audit columns
    col_defs.append("  _load_timestamp TIMESTAMP NOT NULL")
    col_defs.append("  _run_id STRING NOT NULL")
    col_defs.append("  _record_uuid STRING")
    col_defs.append("  _reporting_date DATE")

    sql += ",\n".join(col_defs)
    sql += "\n)\n"

    if feed.bq_target.partition_field:
        sql += f"PARTITION BY {feed.bq_target.partition_type}({feed.bq_target.partition_field})\n"
    elif feed.partition_keys:
        sql += f"PARTITION BY DATE({feed.partition_keys[0]})\n"

    if feed.bq_target.clustering_fields:
        sql += f"CLUSTER BY {', '.join(feed.bq_target.clustering_fields)}\n"
    elif feed.business_keys:
        cluster_cols = feed.business_keys[:4]  # BQ max 4 clustering columns
        sql += f"CLUSTER BY {', '.join(cluster_cols)}\n"

    sql += ";\n"
    return sql


def generate_view_sql(feed: FeedConfig, zone: str, view_name: Optional[str] = None) -> str:
    """Generate CREATE OR REPLACE VIEW for zone transitions."""
    project = feed.bq_target.project or "${GCP_PROJECT}"
    dataset = f"{zone}_{feed.domain}"
    name = view_name or f"v_{feed.feed_name}_{zone}"

    sql = f"-- VIEW: {name} ({zone} zone)\n"
    sql += f"CREATE OR REPLACE VIEW `{project}.{dataset}.{name}` AS\n"
    sql += f"SELECT\n"
    sql += f"  {_col_list(feed.columns)}\n"
    sql += f"FROM {_qualified_table(feed)}\n"
    sql += ";\n"
    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VAULT 2.0 SQL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_hub_insert_sql(hub: HubConfig, source_table: str, project: str = "${GCP_PROJECT}") -> str:
    """Generate Data Vault hub INSERT (insert-only, dedup by hash key)."""
    bk_list = ", ".join(hub.business_keys)
    bk_concat = " || '|' || ".join(f"CAST(S.{k} AS STRING)" for k in hub.business_keys)

    sql = f"-- HUB: {hub.hub_name}\n"
    sql += f"INSERT INTO `{project}.gold.{hub.target_table}`\n"
    sql += f"  (hub_hash_key, {bk_list}, load_date, record_source)\n"
    sql += f"SELECT DISTINCT\n"
    sql += f"  TO_HEX(MD5({bk_concat})) AS hub_hash_key,\n"
    sql += f"  {', '.join(f'S.{k}' for k in hub.business_keys)},\n"
    sql += f"  CURRENT_TIMESTAMP() AS load_date,\n"
    sql += f"  @record_source AS record_source\n"
    sql += f"FROM `{source_table}` S\n"
    sql += f"LEFT JOIN `{project}.gold.{hub.target_table}` H\n"
    sql += f"  ON H.hub_hash_key = TO_HEX(MD5({bk_concat}))\n"
    sql += f"WHERE H.hub_hash_key IS NULL\n"
    sql += ";\n"
    return sql


def generate_satellite_insert_sql(
    sat: SatelliteConfig, source_table: str, project: str = "${GCP_PROJECT}"
) -> str:
    """Generate Data Vault satellite INSERT (change-detect by hash_diff)."""
    desc_cols = ", ".join(sat.descriptive_columns)
    desc_concat = " || '|' || ".join(f"CAST(S.{c} AS STRING)" for c in sat.descriptive_columns)

    sql = f"-- SATELLITE: {sat.satellite_name}\n"
    sql += f"-- Close existing records\n"
    sql += f"UPDATE `{project}.gold.{sat.target_table}` SAT\n"
    sql += f"SET SAT.load_end_date = CURRENT_TIMESTAMP()\n"
    sql += f"WHERE SAT.load_end_date IS NULL\n"
    sql += f"  AND SAT.hub_hash_key IN (\n"
    sql += f"    SELECT TO_HEX(MD5({desc_concat})) FROM `{source_table}` S\n"
    sql += f"    WHERE TO_HEX(MD5({desc_concat})) != SAT.hash_diff\n"
    sql += f"  );\n\n"
    sql += f"-- Insert new/changed records\n"
    sql += f"INSERT INTO `{project}.gold.{sat.target_table}`\n"
    sql += f"  (hub_hash_key, hash_diff, {desc_cols}, load_date, load_end_date, record_source)\n"
    sql += f"SELECT\n"
    sql += f"  S.hub_hash_key,\n"
    sql += f"  TO_HEX(MD5({desc_concat})) AS hash_diff,\n"
    sql += f"  {', '.join(f'S.{c}' for c in sat.descriptive_columns)},\n"
    sql += f"  CURRENT_TIMESTAMP() AS load_date,\n"
    sql += f"  NULL AS load_end_date,\n"
    sql += f"  @record_source AS record_source\n"
    sql += f"FROM `{source_table}` S\n"
    sql += f"LEFT JOIN `{project}.gold.{sat.target_table}` SAT\n"
    sql += f"  ON SAT.hub_hash_key = S.hub_hash_key\n"
    sql += f"  AND SAT.load_end_date IS NULL\n"
    sql += f"  AND SAT.hash_diff = TO_HEX(MD5({desc_concat}))\n"
    sql += f"WHERE SAT.hub_hash_key IS NULL\n"
    sql += ";\n"
    return sql


def generate_link_insert_sql(
    link: LinkConfig, source_table: str, project: str = "${GCP_PROJECT}"
) -> str:
    """Generate Data Vault link INSERT."""
    hub_refs = link.hub_references
    hub_hash_cols = [f"{h}_hash_key" for h in hub_refs]
    link_concat = " || '|' || ".join(f"CAST(S.{hk} AS STRING)" for hk in hub_hash_cols)

    sql = f"-- LINK: {link.link_name}\n"
    sql += f"INSERT INTO `{project}.gold.{link.target_table}`\n"
    sql += f"  (link_hash_key, {', '.join(hub_hash_cols)}, load_date, record_source)\n"
    sql += f"SELECT DISTINCT\n"
    sql += f"  TO_HEX(MD5({link_concat})) AS link_hash_key,\n"
    sql += f"  {', '.join(f'S.{hk}' for hk in hub_hash_cols)},\n"
    sql += f"  CURRENT_TIMESTAMP() AS load_date,\n"
    sql += f"  @record_source AS record_source\n"
    sql += f"FROM `{source_table}` S\n"
    sql += f"LEFT JOIN `{project}.gold.{link.target_table}` L\n"
    sql += f"  ON L.link_hash_key = TO_HEX(MD5({link_concat}))\n"
    sql += f"WHERE L.link_hash_key IS NULL\n"
    sql += ";\n"
    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# STAR SCHEMA SQL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_dimension_merge_sql(
    dim: DimensionConfig, source_table: str, project: str = "${GCP_PROJECT}"
) -> str:
    """Generate dimension MERGE with SCD type support."""
    target = f"`{project}.gold.{dim.target_table}`"
    cols = ", ".join(dim.columns)

    if dim.scd_type == 0:
        # Insert only, no updates
        sql = f"-- DIMENSION (SCD0): {dim.dimension_name}\n"
        sql += f"INSERT INTO {target}\n"
        sql += f"  ({dim.surrogate_key_name}, {dim.natural_key}, {cols}, _load_timestamp)\n"
        sql += f"SELECT\n"
        sql += f"  GENERATE_UUID() AS {dim.surrogate_key_name},\n"
        sql += f"  S.{dim.natural_key},\n"
        sql += f"  {', '.join(f'S.{c}' for c in dim.columns)},\n"
        sql += f"  CURRENT_TIMESTAMP()\n"
        sql += f"FROM `{source_table}` S\n"
        sql += f"LEFT JOIN {target} T ON T.{dim.natural_key} = S.{dim.natural_key}\n"
        sql += f"WHERE T.{dim.natural_key} IS NULL\n"
        sql += ";\n"

    elif dim.scd_type == 1:
        # Overwrite
        sql = f"-- DIMENSION (SCD1): {dim.dimension_name}\n"
        sql += f"MERGE INTO {target} AS T\n"
        sql += f"USING `{source_table}` AS S\n"
        sql += f"ON T.{dim.natural_key} = S.{dim.natural_key}\n"
        sql += f"WHEN MATCHED THEN\n"
        sql += f"  UPDATE SET {', '.join(f'T.{c} = S.{c}' for c in dim.columns)},\n"
        sql += f"    T._load_timestamp = CURRENT_TIMESTAMP()\n"
        sql += f"WHEN NOT MATCHED THEN\n"
        sql += f"  INSERT ({dim.surrogate_key_name}, {dim.natural_key}, {cols}, _load_timestamp)\n"
        sql += f"  VALUES (GENERATE_UUID(), S.{dim.natural_key}, {', '.join(f'S.{c}' for c in dim.columns)}, CURRENT_TIMESTAMP())\n"
        sql += ";\n"

    elif dim.scd_type == 2:
        # Add new row, close old
        sql = f"-- DIMENSION (SCD2): {dim.dimension_name}\n"
        sql += f"-- Close expired records\n"
        sql += f"UPDATE {target}\n"
        sql += f"SET effective_end_date = CURRENT_DATE(), is_current = FALSE\n"
        sql += f"WHERE {dim.natural_key} IN (SELECT {dim.natural_key} FROM `{source_table}`)\n"
        sql += f"  AND is_current = TRUE;\n\n"
        sql += f"-- Insert new versions\n"
        sql += f"INSERT INTO {target}\n"
        sql += f"  ({dim.surrogate_key_name}, {dim.natural_key}, {cols},\n"
        sql += f"   effective_start_date, effective_end_date, is_current, _load_timestamp)\n"
        sql += f"SELECT\n"
        sql += f"  GENERATE_UUID(),\n"
        sql += f"  S.{dim.natural_key},\n"
        sql += f"  {', '.join(f'S.{c}' for c in dim.columns)},\n"
        sql += f"  CURRENT_DATE() AS effective_start_date,\n"
        sql += f"  DATE '9999-12-31' AS effective_end_date,\n"
        sql += f"  TRUE AS is_current,\n"
        sql += f"  CURRENT_TIMESTAMP()\n"
        sql += f"FROM `{source_table}` S\n"
        sql += ";\n"

    elif dim.scd_type == 3:
        # Previous value columns
        prev_cols = ", ".join(f"T.{c}" for c in dim.columns)
        prev_aliases = ", ".join(f"T.{c} AS prev_{c}" for c in dim.columns)
        sql = f"-- DIMENSION (SCD3): {dim.dimension_name}\n"
        sql += f"MERGE INTO {target} AS T\n"
        sql += f"USING `{source_table}` AS S\n"
        sql += f"ON T.{dim.natural_key} = S.{dim.natural_key}\n"
        sql += f"WHEN MATCHED THEN\n"
        sql += f"  UPDATE SET\n"
        for c in dim.columns:
            sql += f"    T.prev_{c} = T.{c},\n"
            sql += f"    T.{c} = S.{c},\n"
        sql += f"    T._load_timestamp = CURRENT_TIMESTAMP()\n"
        sql += f"WHEN NOT MATCHED THEN\n"
        sql += f"  INSERT ({dim.surrogate_key_name}, {dim.natural_key}, {cols}, _load_timestamp)\n"
        sql += f"  VALUES (GENERATE_UUID(), S.{dim.natural_key}, {', '.join(f'S.{c}' for c in dim.columns)}, CURRENT_TIMESTAMP())\n"
        sql += ";\n"

    else:
        sql = f"-- Unknown SCD type: {dim.scd_type}\n"

    return sql


def generate_fact_insert_sql(
    fact: FactConfig, source_table: str, project: str = "${GCP_PROJECT}"
) -> str:
    """Generate fact table INSERT with dimension surrogate key lookups."""
    target = f"`{project}.gold.{fact.target_table}`"

    # Build dimension joins
    dim_joins = []
    dim_sk_selects = []
    for lookup in fact.dimension_lookups:
        alias = f"d_{lookup.dimension_name}"
        dim_joins.append(
            f"LEFT JOIN `{project}.gold.dim_{lookup.dimension_name}` {alias}\n"
            f"  ON {alias}.{lookup.join_key} = S.{lookup.join_key}\n"
            f"  AND {alias}.is_current = TRUE"
        )
        dim_sk_selects.append(f"{alias}.{lookup.surrogate_key} AS {lookup.dimension_name}_{lookup.surrogate_key}")

    # Build measure selects
    measure_selects = [f"S.{m.source_column} AS {m.name}" for m in fact.measures]

    # Build grain selects
    grain_selects = [f"S.{g}" for g in fact.grain_columns]

    all_selects = grain_selects + dim_sk_selects + measure_selects
    all_selects.append("CURRENT_TIMESTAMP() AS _load_timestamp")
    all_selects.append("@run_id AS _run_id")

    # Column names for INSERT
    grain_cols = fact.grain_columns
    sk_cols = [f"{l.dimension_name}_{l.surrogate_key}" for l in fact.dimension_lookups]
    measure_cols = [m.name for m in fact.measures]
    all_cols = grain_cols + sk_cols + measure_cols + ["_load_timestamp", "_run_id"]

    sql = f"-- FACT: {fact.fact_name}\n"
    sql += f"INSERT INTO {target}\n"
    sql += f"  ({', '.join(all_cols)})\n"
    sql += f"SELECT\n"
    sql += f"  {(','+chr(10)+'  ').join(all_selects)}\n"
    sql += f"FROM `{source_table}` S\n"
    sql += "\n".join(dim_joins) + "\n" if dim_joins else ""
    sql += ";\n"
    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# GREAT EXPECTATIONS SUITE
# ═══════════════════════════════════════════════════════════════════════════════

GE_RULE_MAP = {
    "not_null": "expect_column_values_to_not_be_null",
    "unique": "expect_column_values_to_be_unique",
    "range": "expect_column_values_to_be_between",
    "regex": "expect_column_values_to_match_regex",
    "in_set": "expect_column_values_to_be_in_set",
    "row_count": "expect_table_row_count_to_be_between",
    "freshness": "expect_column_max_to_be_between",
    "compound_unique": "expect_compound_columns_to_be_unique",
    "column_pair_comparison": "expect_column_pair_values_A_to_be_greater_than_B",
    "completeness": "expect_column_values_to_not_be_null",
    "custom_sql": "expect_column_values_to_match_sql_condition",
}


def generate_ge_expectations(feed: FeedConfig) -> str:
    """Generate Great Expectations expectation suite JSON from quality rules."""
    expectations = []

    for rule in feed.quality_rules:
        ge_type = GE_RULE_MAP.get(rule.rule_type)
        if not ge_type:
            continue

        kwargs: Dict[str, Any] = {}
        if rule.column:
            kwargs["column"] = rule.column

        # Map rule params to GE kwargs
        if rule.rule_type == "range":
            kwargs["min_value"] = rule.params.get("min")
            kwargs["max_value"] = rule.params.get("max")
        elif rule.rule_type == "regex":
            kwargs["regex"] = rule.params.get("pattern", ".*")
        elif rule.rule_type == "in_set":
            kwargs["value_set"] = rule.params.get("values", [])
        elif rule.rule_type == "row_count":
            kwargs["min_value"] = rule.params.get("min", 0)
            kwargs["max_value"] = rule.params.get("max")
        elif rule.rule_type == "freshness":
            kwargs["min_value"] = rule.params.get("min_date")
            kwargs["max_value"] = rule.params.get("max_date")
        elif rule.rule_type == "compound_unique":
            kwargs["column_list"] = rule.params.get("columns", [])
        elif rule.rule_type == "column_pair_comparison":
            kwargs["column_A"] = rule.params.get("column_a")
            kwargs["column_B"] = rule.params.get("column_b")
        elif rule.rule_type == "custom_sql":
            kwargs["sql_condition"] = rule.params.get("sql", "1=1")

        # Remove None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        expectations.append({
            "expectation_type": ge_type,
            "kwargs": kwargs,
            "meta": {
                "severity": rule.severity,
                "feed_id": feed.feed_id,
            },
        })

    suite = {
        "expectation_suite_name": f"{feed.feed_name}_expectations",
        "ge_cloud_id": None,
        "expectations": expectations,
        "data_asset_type": None,
        "meta": {
            "great_expectations_version": "0.18.0",
            "feed_id": feed.feed_id,
            "domain": feed.domain,
        },
    }

    return json.dumps(suite, indent=2, sort_keys=True)


# ═══════════════════════════════════════════════════════════════════════════════
# METADATA INSERT SQL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_metadata_insert_sql(feed: FeedConfig) -> str:
    """Generate PostgreSQL INSERT for pipeline metadata tables."""
    cols_json = json.dumps(
        [{"name": c.name, "type": c.data_type, "nullable": c.nullable} for c in feed.columns]
    )
    bk_json = json.dumps(feed.business_keys)

    sql = f"-- METADATA: {feed.feed_name}\n"
    sql += f"INSERT INTO schema_details (feed_id, schema_version, columns, business_keys, created_at)\n"
    sql += f"VALUES (\n"
    sql += f"  '{feed.feed_id}',\n"
    sql += f"  {feed.schema_version},\n"
    sql += f"  '{cols_json}'::jsonb,\n"
    sql += f"  '{bk_json}'::jsonb,\n"
    sql += f"  NOW()\n"
    sql += f")\n"
    sql += f"ON CONFLICT (feed_id, schema_version) DO UPDATE SET\n"
    sql += f"  columns = EXCLUDED.columns,\n"
    sql += f"  business_keys = EXCLUDED.business_keys;\n"
    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE: GENERATE ALL SQL FOR A FEED
# ═══════════════════════════════════════════════════════════════════════════════

def generate_all_sql(feed: FeedConfig) -> Dict[str, str]:
    """Generate all SQL artifacts for a feed. Returns dict of filename → SQL content."""
    artifacts = {}

    # DDL
    artifacts["ddl.sql"] = generate_bq_ddl(feed)

    # Load SQL based on strategy
    if feed.sql_load_type == "merge":
        artifacts["load.sql"] = generate_merge_sql(feed)
    elif feed.sql_load_type == "delete_insert":
        artifacts["load.sql"] = generate_delete_insert_sql(feed)
    else:
        artifacts["load.sql"] = generate_insert_sql(feed)

    # Metadata
    artifacts["metadata.sql"] = generate_metadata_insert_sql(feed)

    # GE expectations
    artifacts["expectations.json"] = generate_ge_expectations(feed)

    # Gold modeling SQL
    if feed.gold_model:
        if feed.gold_model.model_type == "data_vault":
            for hub in feed.gold_model.hubs:
                artifacts[f"hub_{hub.hub_name}.sql"] = generate_hub_insert_sql(hub, "silver_staging")
            for sat in feed.gold_model.satellites:
                artifacts[f"sat_{sat.satellite_name}.sql"] = generate_satellite_insert_sql(sat, "silver_staging")
            for link in feed.gold_model.links:
                artifacts[f"link_{link.link_name}.sql"] = generate_link_insert_sql(link, "silver_staging")
        elif feed.gold_model.model_type == "star_schema":
            for dim in feed.gold_model.dimensions:
                artifacts[f"dim_{dim.dimension_name}.sql"] = generate_dimension_merge_sql(dim, "silver_staging")
            for fact in feed.gold_model.facts:
                artifacts[f"fact_{fact.fact_name}.sql"] = generate_fact_insert_sql(fact, "silver_staging")

    return artifacts
