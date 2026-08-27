"""
Production Metadata Models - PostgreSQL Table Mirrors

These models exactly match the production PostgreSQL metadata schema tables:
- feed_group_details: Group multiple related feeds
- feed_details: Individual feed configurations
- contract_details: Data contracts with validation rules
- schema_details: Column definitions with zone-specific schemas
- feed_ingestion_details: Ingestion tracking and watermarks

Based on production DAG patterns (e.g., fraud_falcon_dailyfile_load)
Note: All IDs are numeric (fetched from PostgreSQL via max() + 1)
"""

from datetime import datetime, date
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Production Enums (matching PostgreSQL enum types)
# =============================================================================

class DataVaultType(str, Enum):
    """Data Vault entity types (production patterns)."""
    SATELLITE = "satellite"
    HUB = "hub"
    FPE_HUB = "fpe_hub"  # Full Point-in-Time Evolution Hub
    LINK = "link"
    PIT = "pit"  # Point-in-Time table
    BRIDGE = "bridge"


class ZoneType(str, Enum):
    """Production zone types. GOLD is the final/business-ready layer."""
    TRANSIENT = "transient"  # Temporary landing zone
    RAW = "raw"  # Raw ingested data (immutable)
    SILVER = "silver"  # Cleaned/validated data
    GOLD = "gold"  # Business-ready data (final layer)
    CONSUMPTION = "consumption"  # Analytics/reporting layer (Data Vault)


class LoadTypeEnum(str, Enum):
    """Load type strategies."""
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    MERGE = "merge"
    APPEND = "append"


class FileFormatEnum(str, Enum):
    """Supported file formats."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    FIXED_WIDTH = "fixed_width"
    EBCDIC = "ebcdic"
    XML = "xml"
    EXCEL = "xlsx"


class IngestionStatus(str, Enum):
    """Ingestion execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    NO_FILE = "no_file"
    SKIPPED = "skipped"
    PARTIAL = "partial"


class ContractStatus(str, Enum):
    """Contract lifecycle status."""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class ValidationSeverity(str, Enum):
    """Great Expectations validation severity."""
    CRITICAL = "critical"  # Fail pipeline
    ERROR = "error"  # Fail but continue other feeds
    WARNING = "warning"  # Log and continue
    INFO = "info"  # Informational only


# =============================================================================
# Feed Group Details (PostgreSQL: feed_group_details)
# =============================================================================

class FeedGroupDetails(BaseModel):
    """
    Feed Group - groups related feeds from the same source.

    Example: One source file splits into multiple tables
    feed_group_id = 350 (numeric, from max() + 1)
    """
    model_config = ConfigDict(from_attributes=True)

    feed_group_id: int = Field(..., description="Numeric ID (max() + 1 from PostgreSQL)")
    feed_group_name: str = Field(..., max_length=100)
    domain: str = Field(..., max_length=50, description="Business domain (e.g., 'fraud', 'risk')")
    source_system: str = Field(..., max_length=100, description="Source system name")
    source_type: str = Field(..., max_length=50, description="file, database, api, streaming")

    # Schedule configuration
    schedule_interval: Optional[str] = Field(None, description="Cron expression or @daily")
    dag_id: str = Field(..., max_length=100, description="Airflow DAG ID")

    # Ownership
    owner_email: str = Field(..., max_length=255)
    team_slack_channel: Optional[str] = Field(None, max_length=100)

    # GCS paths
    landing_path: Optional[str] = Field(None, description="GCS landing zone path")
    archive_path: Optional[str] = Field(None, description="GCS archive path")

    # Status
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(None)
    created_by: str = Field(..., max_length=100)

    # Multi-feed configuration (inputparam structure from production)
    inputparam: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Multi-feed configuration dict (production pattern)"
    )


# =============================================================================
# Feed Details (PostgreSQL: feed_details)
# =============================================================================

class FeedDetails(BaseModel):
    """
    Feed Details - individual feed within a group.

    Example: feed_id = 947 within feed_group_id = 350
    """
    model_config = ConfigDict(from_attributes=True)

    feed_id: int = Field(..., description="Numeric ID (max() + 1 from PostgreSQL)")
    feed_group_id: int = Field(..., description="Parent feed group ID")
    feed_name: str = Field(..., max_length=100)
    feed_description: Optional[str] = Field(None, max_length=500)

    # Source configuration
    source_table: Optional[str] = Field(None, max_length=255, description="Source table/file pattern")
    source_query: Optional[str] = Field(None, description="Custom extraction query")
    file_pattern: Optional[str] = Field(None, max_length=255, description="File glob pattern")
    file_format: Optional[FileFormatEnum] = Field(None)
    delimiter: Optional[str] = Field(None, max_length=10)
    header: Optional[bool] = Field(True)
    encoding: Optional[str] = Field("utf-8", max_length=20)

    # Load configuration
    load_type: LoadTypeEnum = Field(default=LoadTypeEnum.FULL)
    watermark_column: Optional[str] = Field(None, max_length=100)
    primary_key_columns: List[str] = Field(default_factory=list)
    partition_columns: List[str] = Field(default_factory=list)
    clustering_columns: List[str] = Field(default_factory=list)

    # Data Vault configuration
    data_vault_type: Optional[DataVaultType] = Field(
        None,
        description="satellite, hub, fpe_hub, link for Data Vault patterns"
    )
    hub_columns: List[str] = Field(default_factory=list, description="Hub business key columns")
    satellite_columns: List[str] = Field(default_factory=list, description="Satellite attribute columns")

    # Target configuration
    target_dataset: str = Field(..., max_length=100, description="BigQuery dataset")
    target_table: str = Field(..., max_length=100, description="BigQuery table")
    target_zones: List[ZoneType] = Field(
        default_factory=lambda: [ZoneType.RAW, ZoneType.SILVER],
        description="Zones this feed populates"
    )

    # Contract reference
    contract_id: Optional[int] = Field(None, description="Associated contract ID")
    schema_id: Optional[int] = Field(None, description="Associated schema ID")

    # Dependencies
    depends_on_feeds: List[int] = Field(
        default_factory=list,
        description="Feed IDs this feed depends on"
    )

    # Status
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(None)

    # Extended metadata for production patterns
    extra_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional configuration (production flexibility)"
    )


# =============================================================================
# Contract Details (PostgreSQL: contract_details)
# =============================================================================

class ExpectationRule(BaseModel):
    """Great Expectations validation rule."""
    expectation_type: str = Field(..., description="e.g., expect_column_values_to_not_be_null")
    kwargs: Dict[str, Any] = Field(default_factory=dict, description="GE kwargs")
    severity: ValidationSeverity = Field(default=ValidationSeverity.ERROR)
    zone: ZoneType = Field(..., description="Zone where this rule applies")
    meta: Optional[Dict[str, Any]] = Field(None, description="GE meta info")


class TransformationSpec(BaseModel):
    """Transformation specification for a zone."""
    zone: ZoneType = Field(..., description="Target zone for this transformation")
    order: int = Field(default=1, description="Execution order within zone")
    transform_type: str = Field(..., description="deduplicate, clean_nulls, type_cast, etc.")
    columns: Optional[List[str]] = Field(None, description="Columns to transform (None = all)")
    expression: Optional[str] = Field(None, description="SQL/PySpark expression")
    config: Dict[str, Any] = Field(default_factory=dict, description="Transform-specific config")


class ContractDetails(BaseModel):
    """
    Data Contract - defines validation, transformation, and SLA rules.

    Includes Great Expectations integration and zone-specific rules.
    """
    model_config = ConfigDict(from_attributes=True)

    contract_id: int = Field(..., description="Numeric ID (max() + 1)")
    contract_name: str = Field(..., max_length=100)
    contract_version: int = Field(default=1)
    feed_id: int = Field(..., description="Associated feed ID")

    # Contract type determines DAG pattern
    contract_type: str = Field(
        default="standard",
        description="standard, scd2, data_vault, star_schema"
    )

    # Great Expectations rules
    expectations: List[ExpectationRule] = Field(
        default_factory=list,
        description="GE validation rules by zone"
    )

    # Transformations by zone
    transformations: List[TransformationSpec] = Field(
        default_factory=list,
        description="Transformation specs by zone"
    )

    # SLA configuration
    sla_hours: Optional[float] = Field(None, description="Max allowed latency in hours")
    freshness_threshold_hours: Optional[float] = Field(None, description="Data freshness requirement")

    # Quality thresholds
    min_row_count: Optional[int] = Field(None, description="Minimum expected rows")
    max_null_percentage: Optional[float] = Field(None, description="Max null % threshold")
    quality_score_threshold: Optional[float] = Field(0.95, description="Min quality score (0-1)")

    # Status
    status: ContractStatus = Field(default=ContractStatus.DRAFT)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(None)
    approved_by: Optional[str] = Field(None, max_length=100)
    approved_at: Optional[datetime] = Field(None)


# =============================================================================
# Schema Details (PostgreSQL: schema_details)
# =============================================================================

class ColumnSpec(BaseModel):
    """Column specification with zone-specific types."""
    column_name: str = Field(..., max_length=100)
    source_type: str = Field(..., description="Source data type")
    target_type: str = Field(..., description="Target BigQuery type")

    # Column properties
    nullable: bool = Field(default=True)
    is_primary_key: bool = Field(default=False)
    is_partition_key: bool = Field(default=False)
    is_clustering_key: bool = Field(default=False)

    # Data Vault columns
    is_hub_key: bool = Field(default=False)
    is_satellite_attr: bool = Field(default=False)
    is_link_key: bool = Field(default=False)

    # SCD2 columns
    is_business_key: bool = Field(default=False)
    is_tracked_column: bool = Field(default=False)

    # Validation
    not_null: bool = Field(default=False)
    unique: bool = Field(default=False)
    default_value: Optional[str] = Field(None)
    validation_regex: Optional[str] = Field(None)

    # Documentation
    description: Optional[str] = Field(None, max_length=500)
    business_name: Optional[str] = Field(None, max_length=100)
    pii: bool = Field(default=False, description="Contains PII data")

    # Column order
    ordinal_position: int = Field(default=0)


class SchemaDetails(BaseModel):
    """
    Schema Details - versioned schema for a feed.

    Supports zone-specific column mappings.
    """
    model_config = ConfigDict(from_attributes=True)

    schema_id: int = Field(..., description="Numeric ID (max() + 1)")
    feed_id: int = Field(..., description="Associated feed ID")
    platform_schema_version: int = Field(default=1)

    # Zone this schema applies to
    zone: ZoneType = Field(default=ZoneType.RAW)

    # Column definitions
    columns: List[ColumnSpec] = Field(default_factory=list)

    # Schema properties
    has_header: bool = Field(default=True)
    record_delimiter: Optional[str] = Field(None, description="For fixed-width/EBCDIC")

    # Schema evolution
    evolution_mode: str = Field(
        default="strict",
        description="strict, additive, flexible"
    )
    previous_version_id: Optional[int] = Field(None)

    # Status
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(None)

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.column_name for col in self.columns]

    @property
    def primary_key_columns(self) -> List[str]:
        """Get primary key column names."""
        return [col.column_name for col in self.columns if col.is_primary_key]

    @property
    def business_key_columns(self) -> List[str]:
        """Get business key columns for SCD2."""
        return [col.column_name for col in self.columns if col.is_business_key]


# =============================================================================
# Feed Ingestion Details (PostgreSQL: feed_ingestion_details)
# =============================================================================

class FeedIngestionDetails(BaseModel):
    """
    Feed Ingestion - execution/run tracking.

    Tracks each ingestion attempt with metrics and status.
    """
    model_config = ConfigDict(from_attributes=True)

    ingestion_id: int = Field(..., description="Numeric ID (max() + 1)")
    feed_id: int = Field(..., description="Feed being ingested")
    feed_group_id: int = Field(..., description="Parent group ID")

    # Execution context
    dag_run_id: str = Field(..., max_length=255)
    execution_date: datetime = Field(...)
    batch_id: str = Field(..., max_length=50, description="YYYYMMDD format")

    # Timing
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = Field(None)
    duration_seconds: Optional[int] = Field(None)

    # Status
    status: IngestionStatus = Field(default=IngestionStatus.PENDING)
    current_zone: Optional[ZoneType] = Field(None, description="Zone currently processing")

    # Metrics
    records_read: int = Field(default=0)
    records_written: int = Field(default=0)
    records_rejected: int = Field(default=0)
    bytes_processed: int = Field(default=0)

    # File tracking (for file sources)
    source_file_path: Optional[str] = Field(None, max_length=500)
    source_file_size: Optional[int] = Field(None)
    source_file_modified: Optional[datetime] = Field(None)

    # Watermark tracking (for incremental)
    previous_watermark: Optional[str] = Field(None)
    current_watermark: Optional[str] = Field(None)

    # Quality metrics
    validation_passed: bool = Field(default=True)
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    failed_expectations: List[str] = Field(default_factory=list)

    # Error tracking
    error_message: Optional[str] = Field(None)
    error_zone: Optional[ZoneType] = Field(None)
    error_task: Optional[str] = Field(None)
    retry_count: int = Field(default=0)

    # Lineage
    spark_application_id: Optional[str] = Field(None, max_length=100)
    airflow_task_id: Optional[str] = Field(None, max_length=255)


# =============================================================================
# InputParam Structure (Production Multi-Feed Pattern)
# =============================================================================

class FeedInputParam(BaseModel):
    """
    Individual feed configuration within inputparam.

    Production pattern: inputparam = {947: {...}, 948: {...}}
    """
    feed_id: int
    feed_name: str
    target_table: str
    load_type: LoadTypeEnum = Field(default=LoadTypeEnum.FULL)
    data_vault_type: Optional[DataVaultType] = Field(None)

    # Zone-specific config
    zones: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Zone-specific configuration: {'raw': {...}, 'silver': {...}}"
    )

    # Validation config
    ge_suite_name: Optional[str] = Field(None, description="Great Expectations suite name")
    quality_threshold: float = Field(default=0.95)


class InputParamConfig(BaseModel):
    """
    Complete inputparam structure for multi-feed DAGs.

    Production pattern from fraud_falcon_dailyfile_load:
    inputparam = {
        350: {  # feed_group_id
            "feeds": {
                947: FeedInputParam(...),
                948: FeedInputParam(...),
            },
            "common_config": {...}
        }
    }
    """
    feed_group_id: int
    feeds: Dict[int, FeedInputParam] = Field(default_factory=dict)

    # Common configuration for all feeds in group
    common_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Shared config for all feeds (GCS paths, Spark config, etc.)"
    )

    # Execution order (if feeds have dependencies)
    execution_order: List[int] = Field(
        default_factory=list,
        description="Ordered list of feed_ids for sequential execution"
    )


# =============================================================================
# Production Execution Plan (Enhanced)
# =============================================================================

class ProductionExecutionPlan(BaseModel):
    """
    Production Execution Plan - enhanced version matching actual DAG patterns.

    This is the immutable plan generated by Control Plane and
    passed to Spark jobs for deterministic execution.
    """
    model_config = ConfigDict(extra="allow")

    # Identifiers (numeric)
    plan_id: str = Field(..., description="Unique execution plan ID")
    feed_group_id: int = Field(..., description="Feed group ID")
    feed_id: int = Field(..., description="Specific feed ID")
    batch_id: str = Field(..., description="Batch ID (YYYYMMDD)")
    dag_run_id: str = Field(..., description="Airflow DAG run ID")

    # Pattern and type
    pattern_code: str = Field(..., description="P01-P09")
    data_vault_type: Optional[DataVaultType] = Field(None)

    # Source snapshot
    source: Dict[str, Any] = Field(..., description="Immutable source config")

    # Schema snapshot
    schema: Dict[str, Any] = Field(..., description="Versioned schema snapshot")

    # Zone-specific configurations
    zones: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Per-zone config: transient, raw, refined, gold, consumption"
    )

    # Great Expectations configuration
    ge_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="GE suite names and expectations by zone"
    )

    # Transformations (ordered)
    transformations: List[Dict[str, Any]] = Field(default_factory=list)

    # Target configuration
    targets: Dict[str, Any] = Field(default_factory=dict)

    # Execution context
    metadata_db_url: str = Field(...)
    execution_id: str = Field(...)

    # Retry and error handling
    retry_config: Optional[Dict[str, Any]] = Field(None)
    schema_evolution_mode: str = Field(default="strict")


# =============================================================================
# API Models for Agent Endpoints
# =============================================================================

class CreateFeedGroupRequest(BaseModel):
    """Request to create a new feed group."""
    feed_group_name: str
    domain: str
    source_system: str
    source_type: str
    owner_email: str
    schedule_interval: Optional[str] = "@daily"
    created_by: str

    # Optional configurations
    landing_path: Optional[str] = None
    team_slack_channel: Optional[str] = None


class CreateFeedRequest(BaseModel):
    """Request to create a new feed within a group."""
    feed_group_id: int
    feed_name: str
    target_dataset: str
    target_table: str

    # Source config
    source_table: Optional[str] = None
    file_pattern: Optional[str] = None
    file_format: Optional[FileFormatEnum] = None
    load_type: LoadTypeEnum = LoadTypeEnum.FULL

    # Data Vault config
    data_vault_type: Optional[DataVaultType] = None
    hub_columns: Optional[List[str]] = None
    satellite_columns: Optional[List[str]] = None

    # Schema (can be auto-inferred or provided)
    schema: Optional[List[ColumnSpec]] = None


class CreateContractRequest(BaseModel):
    """Request to create a data contract."""
    feed_id: int
    contract_name: str
    contract_type: str = "standard"

    # GE expectations
    expectations: Optional[List[ExpectationRule]] = None

    # Transformations
    transformations: Optional[List[TransformationSpec]] = None

    # SLA
    sla_hours: Optional[float] = None
    quality_score_threshold: float = 0.95


class GeneratePipelineRequest(BaseModel):
    """Request to generate a complete pipeline."""
    feed_group_id: int
    feed_ids: Optional[List[int]] = None  # None = all feeds in group

    # Generation options
    include_ge_validation: bool = True
    include_data_vault: bool = False
    target_zones: List[ZoneType] = Field(
        default_factory=lambda: [ZoneType.RAW, ZoneType.SILVER, ZoneType.GOLD]
    )

    # Created by
    created_by: str
    jira_ticket: Optional[str] = None
