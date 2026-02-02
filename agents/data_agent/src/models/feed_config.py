"""
APEX Data Agent - Feed Configuration Models (v2)

Single metadata contract that drives all DAG generation.
FeedGroupConfig → template selection → DAG + SQL + spark jobs + GE suites.

All generated artifacts are deterministic:
    Same FeedGroupConfig → Same output. Always.
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA & COLUMN DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ColumnDef(BaseModel):
    """Column definition for feed schema."""
    name: str
    data_type: Literal[
        "STRING", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL",
        "BOOLEAN", "DATE", "TIMESTAMP", "BINARY", "ARRAY", "STRUCT", "MAP",
    ]
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None
    pii_classification: Optional[Literal["PII", "PHI", "PCI", "SENSITIVE"]] = None
    description: Optional[str] = None
    default_value: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# TRANSFORM & QUALITY RULES
# ═══════════════════════════════════════════════════════════════════════════════

class TransformRule(BaseModel):
    """Transformation rule applied at a zone level."""
    type: Literal[
        "rename", "cast", "derive", "filter", "dedup", "null_fill",
        "mask", "hash", "trim", "standardize_date", "regex_replace",
        "window", "aggregate", "join", "pivot", "unpivot",
    ]
    config: Dict[str, Any] = Field(default_factory=dict)


class QualityRule(BaseModel):
    """Data quality rule for Great Expectations validation."""
    column: Optional[str] = None
    rule_type: Literal[
        "not_null", "unique", "range", "regex", "in_set",
        "row_count", "freshness", "custom_sql", "compound_unique",
        "column_pair_comparison", "completeness",
    ]
    params: Dict[str, Any] = Field(default_factory=dict)
    severity: Literal["INFO", "WARNING", "ERROR", "CRITICAL"] = "ERROR"


# ═══════════════════════════════════════════════════════════════════════════════
# TARGET CONFIGURATIONS
# ═══════════════════════════════════════════════════════════════════════════════

class SnowflakeTarget(BaseModel):
    """Optional Snowflake target configuration."""
    account: str
    database: str
    schema_name: str
    table: str
    warehouse: str
    role: Optional[str] = None


class BigQueryTarget(BaseModel):
    """BigQuery target configuration (GCP-first, always present)."""
    project: Optional[str] = None  # Defaults to env GCP_PROJECT
    dataset: str
    table: str
    partition_field: Optional[str] = None
    partition_type: Literal["DAY", "MONTH", "YEAR", "HOUR"] = "DAY"
    clustering_fields: List[str] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VAULT 2.0 MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class HubConfig(BaseModel):
    """Data Vault 2.0 — Hub definition."""
    hub_name: str
    business_keys: List[str]
    target_table: str


class SatelliteConfig(BaseModel):
    """Data Vault 2.0 — Satellite definition."""
    satellite_name: str
    hub_reference: str
    descriptive_columns: List[str]
    target_table: str


class LinkConfig(BaseModel):
    """Data Vault 2.0 — Link definition."""
    link_name: str
    hub_references: List[str]  # 2+ hub names
    link_keys: Dict[str, str] = Field(default_factory=dict)  # hub_name → join_key
    target_table: str


# ═══════════════════════════════════════════════════════════════════════════════
# STAR SCHEMA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class DimensionConfig(BaseModel):
    """Star Schema — Dimension definition."""
    dimension_name: str
    natural_key: str
    scd_type: Literal[0, 1, 2, 3] = 2
    columns: List[str]
    target_table: str
    surrogate_key_name: str = "sk_id"


class MeasureConfig(BaseModel):
    """Fact table measure definition."""
    name: str
    source_column: str
    agg_function: Literal["sum", "count", "avg", "min", "max", "count_distinct"] = "sum"
    is_additive: bool = True


class DimensionLookup(BaseModel):
    """Fact-to-dimension lookup definition."""
    dimension_name: str
    join_key: str
    surrogate_key: str = "sk_id"


class FactConfig(BaseModel):
    """Star Schema — Fact table definition."""
    fact_name: str
    grain_columns: List[str]
    measures: List[MeasureConfig]
    dimension_lookups: List[DimensionLookup]
    target_table: str
    date_dimension_key: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# GOLD ZONE MODELING
# ═══════════════════════════════════════════════════════════════════════════════

class GoldModelConfig(BaseModel):
    """Gold zone modeling strategy."""
    model_type: Literal["flat", "scd2", "data_vault", "star_schema"]
    # Data Vault 2.0
    hubs: List[HubConfig] = Field(default_factory=list)
    satellites: List[SatelliteConfig] = Field(default_factory=list)
    links: List[LinkConfig] = Field(default_factory=list)
    # Star Schema
    dimensions: List[DimensionConfig] = Field(default_factory=list)
    facts: List[FactConfig] = Field(default_factory=list)
    # SCD2 (simple mode — when not using full Data Vault)
    scd2_tracked_columns: List[str] = Field(default_factory=list)
    scd2_effective_date_column: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

class DagDependency(BaseModel):
    """External DAG dependency (ExternalTaskSensor)."""
    upstream_dag_id: str
    upstream_task_id: str = "finalize"
    timeout_seconds: int = 3600
    poke_interval: int = 300


# ═══════════════════════════════════════════════════════════════════════════════
# FEED CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

class FeedConfig(BaseModel):
    """
    Single feed configuration — drives template rendering, spark job args,
    SQL generation, and GE expectation suites.
    """
    feed_id: str
    feed_name: str
    domain: str

    # Source (determines which template is selected)
    source_type: Literal["gcs_file", "jdbc", "api", "streaming", "legacy_ebcdic"]
    source_connection_id: str = ""
    source_config: Dict[str, Any] = Field(default_factory=dict)
    # source_config examples:
    #   gcs_file:  {"bucket": "...", "prefix": "data/sales/", "pattern": "*.csv"}
    #   jdbc:      {"schema": "public", "table": "orders", "query": "SELECT ..."}
    #   api:       {"endpoint": "https://...", "auth_type": "oauth2", "pagination": "offset"}
    #   streaming: {"topic": "events", "bootstrap_servers": "kafka:9092", "group_id": "..."}
    #   legacy:    {"copybook_path": "gs://...", "data_path": "gs://..."}

    file_format: Optional[Literal["csv", "json", "parquet", "avro", "ebcdic", "fixed_width", "orc", "xml"]] = None
    delimiter: str = ","
    copybook_path: Optional[str] = None
    header_rows: int = 0
    footer_rows: int = 0
    encoding: str = "UTF-8"
    read_as_csv: bool = False

    # Schema
    schema_version: int = 1
    columns: List[ColumnDef] = Field(default_factory=list)
    business_keys: List[str] = Field(default_factory=list)
    partition_keys: List[str] = Field(default_factory=list)
    watermark_column: Optional[str] = None

    # Load strategy
    load_type: Literal["full", "incremental", "merge", "append", "cdc"] = "incremental"

    # Target (GCP-first)
    bq_target: BigQueryTarget
    snowflake_target: Optional[SnowflakeTarget] = None

    # Zones enabled
    zones: List[Literal["landing", "bronze", "silver", "gold"]] = Field(
        default_factory=lambda: ["landing", "bronze", "silver", "gold"]
    )

    # Transforms per zone
    zone_transforms: Dict[str, List[TransformRule]] = Field(default_factory=dict)
    # Example: {"bronze": [TransformRule(type="cast", config={...})], "silver": [...]}

    # Gold modeling
    gold_model: Optional[GoldModelConfig] = None

    # Quality rules
    quality_rules: List[QualityRule] = Field(default_factory=list)

    # SQL load type for zone transitions
    sql_load_type: Literal["insert", "merge", "delete_insert"] = "insert"


# ═══════════════════════════════════════════════════════════════════════════════
# FEED GROUP CONFIGURATION (Multi-feed DAG)
# ═══════════════════════════════════════════════════════════════════════════════

class FeedGroupConfig(BaseModel):
    """
    Multi-feed DAG configuration.
    One DAG with N feeds rendered as TaskGroups.
    """
    feed_group_id: str
    dag_id: str
    domain: str
    owner: str = "apex-data-agent"
    description: Optional[str] = None

    # Primary feeds (each becomes a TaskGroup)
    feeds: List[FeedConfig]

    # Consumption/downstream feeds (secondary TaskGroups)
    consumption_feeds: List[FeedConfig] = Field(default_factory=list)

    # DAG-level features
    enable_duplicate_detection: bool = True
    enable_self_retrigger: bool = False

    # Dependencies on other DAGs
    dependencies: List[DagDependency] = Field(default_factory=list)

    # Execution
    schedule: Dict[str, str] = Field(default_factory=lambda: {"dev": "@daily", "prod": "0 6 * * *"})
    notification_emails: Dict[str, List[str]] = Field(default_factory=dict)
    retry_count: int = 3
    retry_delay_minutes: int = 5
    timeout_hours: float = 2.0
    max_active_runs: int = 1
    catchup: bool = False

    # Start date
    start_year: int = 2024
    start_month: int = 1
    start_day: int = 1

    # Tags
    tags: List[str] = Field(default_factory=list)

    @property
    def primary_source_type(self) -> str:
        """Source type of the first feed — used for template selection."""
        return self.feeds[0].source_type if self.feeds else "gcs_file"
