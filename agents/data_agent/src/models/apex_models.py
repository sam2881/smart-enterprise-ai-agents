"""
APEX Registry Models

Enums and models for APEX component registry integration.

Note: Canonical definitions for shared types are in their respective modules:
- Severity: quality.py
- ColumnDefinition, SchemaVersion: schema.py
- SourceCategory, SourceType: source.py
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel

# Re-export canonical types for convenience
from src.models.source import SourceCategory, SourceType
from src.models.schema import SchemaVersion, ColumnDefinition


# =============================================================================
# APEX-Specific Enums
# =============================================================================

class ContractType(str, Enum):
    """APEX contract types - determines which DAG pattern template is used."""
    STANDARD = "STANDARD"
    SCD2 = "SCD2"
    DATA_VAULT = "DATA_VAULT"
    STAR_SCHEMA = "STAR_SCHEMA"


class PatternCode(str, Enum):
    """APEX DAG pattern codes (P01-P09)."""
    P01 = "P01"  # File Medallion Pipeline
    P02 = "P02"  # Big Data File Pipeline
    P03 = "P03"  # Database Lakehouse Pipeline
    P04 = "P04"  # Legacy Migration Pipeline
    P05 = "P05"  # Streaming Batch Pipeline
    P06 = "P06"  # API SaaS Pipeline
    P07 = "P07"  # SCD Type 2 Pipeline
    P08 = "P08"  # Data Vault Pipeline
    P09 = "P09"  # Star Schema Pipeline


class FeedType(str, Enum):
    """Feed execution type."""
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    HYBRID = "HYBRID"


class FileFormat(str, Enum):
    """File formats supported by APEX."""
    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "PARQUET"
    AVRO = "AVRO"
    XML = "XML"
    FIXED_WIDTH = "FIXED_WIDTH"
    EBCDIC = "EBCDIC"
    ORC = "ORC"


class LoadType(str, Enum):
    """Data load strategies."""
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    APPEND = "APPEND"
    CDC = "CDC"
    WATERMARK = "WATERMARK"
    MERGE = "MERGE"


class ZoneLevel(str, Enum):
    """Data zones in medallion architecture. GOLD is the final layer."""
    RAW = "RAW"
    TRANSIENT = "TRANSIENT"
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class ValidationType(str, Enum):
    """Validation types for data quality."""
    SCHEMA = "SCHEMA"
    SEMANTIC = "SEMANTIC"
    QUALITY = "QUALITY"
    REFERENTIAL = "REFERENTIAL"
    BUSINESS = "BUSINESS"


class ExecutionStatus(str, Enum):
    """Execution status for pipeline runs."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    NO_FILE = "NO_FILE"
    TIMEOUT = "TIMEOUT"


class TemplateType(str, Enum):
    """DAG template types."""
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    HYBRID = "HYBRID"
    CDC = "CDC"


class PipelineStatus(str, Enum):
    """Pipeline lifecycle status."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class PipelinePhase(str, Enum):
    """Pipeline creation workflow phase."""
    INIT = "init"
    PLANNING = "planning"
    GENERATING = "generating"
    VALIDATING = "validating"
    AWAITING_APPROVAL = "awaiting_approval"
    DEPLOYING = "deploying"
    COMPLETE = "complete"
    FAILED = "failed"


# =============================================================================
# Pattern & Template Models
# =============================================================================

class PatternInfo(BaseModel):
    """Pattern metadata from APEX registry."""
    pattern_code: PatternCode
    pattern_name: str
    contract_type: ContractType
    description: str
    source_types_supported: List[str]
    load_types_supported: List[str]
    zones: List[str]
    selection_priority: int
    required_variables: List[str]
    spark_jobs_used: List[str]


# =============================================================================
# Core Registry Models
# =============================================================================

class ConnectionRegistry(BaseModel):
    """Database connection registry."""
    connection_id: str
    connection_name: str
    connection_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    credentials_secret_id: Optional[str] = None
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None


class DomainRegistry(BaseModel):
    """Business domain registry."""
    domain_id: str
    domain_name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    is_active: bool = True


class SourceRegistry(BaseModel):
    """Source system registry."""
    source_id: str
    source_name: str
    source_type: str
    domain_id: Optional[str] = None
    connection_id: Optional[str] = None
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None


class DAGTemplate(BaseModel):
    """DAG template definition."""
    template_id: str
    template_name: str
    pattern_code: str
    template_type: str
    dag_code: str
    spark_jobs: Optional[List[str]] = None
    variables: Optional[Dict[str, Any]] = None
    is_active: bool = True


class SparkConfig(BaseModel):
    """Spark job configuration."""
    job_id: str
    job_name: str
    job_type: str
    pyspark_code: str
    dependencies: Optional[List[str]] = None
    config: Optional[Dict[str, Any]] = None


# =============================================================================
# Feed and Contract Models
# =============================================================================

class FeedGroup(BaseModel):
    """Feed group for organizing related feeds."""
    feed_group_id: str
    feed_group_name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    is_active: bool = True


class Feed(BaseModel):
    """Data feed definition."""
    feed_id: str
    feed_name: str
    feed_group_id: Optional[str] = None
    domain_id: Optional[str] = None
    source_id: Optional[str] = None
    feed_type: str = "BATCH"
    schedule: Optional[str] = None
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None


class DataContract(BaseModel):
    """Data contract specification."""
    contract_id: str
    contract_name: str
    feed_id: Optional[str] = None
    contract_type: str
    pattern_code: Optional[str] = None
    version: int = 1
    schema_definition: Optional[Dict[str, Any]] = None
    quality_rules: Optional[List[Dict[str, Any]]] = None
    sla_definition: Optional[Dict[str, Any]] = None
    is_active: bool = True


# =============================================================================
# View and Transformation Models
# =============================================================================

class ViewDefinition(BaseModel):
    """SQL view definition."""
    view_id: str
    view_name: str
    zone_level: str
    view_sql: str
    dependencies: Optional[List[str]] = None
    is_active: bool = True


class TransformationRule(BaseModel):
    """Transformation rule definition."""
    rule_id: str
    rule_name: str
    rule_type: str
    expression: str
    description: Optional[str] = None
    is_active: bool = True


class ContractTransformation(BaseModel):
    """Contract-level transformation."""
    transformation_id: str
    contract_id: str
    zone_level: str
    transformation_logic: str
    order: int = 1
    is_active: bool = True


# =============================================================================
# Validation Models
# =============================================================================

class ValidationRule(BaseModel):
    """Data validation rule."""
    rule_id: str
    rule_name: str
    validation_type: str
    expression: str
    severity: str = "ERROR"
    description: Optional[str] = None
    is_active: bool = True


class QualityExpectation(BaseModel):
    """Data quality expectation."""
    expectation_id: str
    expectation_name: str
    expectation_type: str
    column_name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    severity: str = "ERROR"
    is_active: bool = True


class SLADefinition(BaseModel):
    """SLA definition."""
    sla_id: str
    sla_name: str
    metric_name: str
    threshold_value: float
    comparison_operator: str
    severity: str = "WARNING"
    is_active: bool = True


class PipelineDependency(BaseModel):
    """Pipeline dependency tracking."""
    dependency_id: str
    pipeline_id: str
    depends_on_pipeline_id: str
    dependency_type: str = "SEQUENTIAL"
    is_active: bool = True


# =============================================================================
# Execution Models
# =============================================================================

class PipelineExecution(BaseModel):
    """Pipeline execution record."""
    execution_id: str
    pipeline_id: str
    dag_run_id: Optional[str] = None
    execution_status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class TaskExecution(BaseModel):
    """Task execution record."""
    task_execution_id: str
    execution_id: str
    task_id: str
    task_name: str
    execution_status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class AuditLog(BaseModel):
    """Audit log entry."""
    audit_id: str
    entity_type: str
    entity_id: str
    action: str
    performed_by: str
    performed_at: datetime
    details: Optional[Dict[str, Any]] = None


class ValidationLog(BaseModel):
    """Validation log entry."""
    validation_log_id: str
    execution_id: str
    validation_rule_id: str
    zone_level: str
    passed: bool
    failed_records: Optional[int] = None
    error_message: Optional[str] = None
    validated_at: datetime


class ErrorLog(BaseModel):
    """Error log entry."""
    error_log_id: str
    execution_id: str
    error_type: str
    error_message: str
    error_details: Optional[Dict[str, Any]] = None
    occurred_at: datetime


class DataLineage(BaseModel):
    """Data lineage tracking."""
    lineage_id: str
    source_entity: str
    target_entity: str
    transformation_id: Optional[str] = None
    lineage_type: str = "DERIVED"
    metadata: Optional[Dict[str, Any]] = None


class AgentDecisionLog(BaseModel):
    """Agent decision log."""
    decision_id: str
    agent_name: str
    decision_type: str
    decision_rationale: str
    confidence_score: Optional[float] = None
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None


# =============================================================================
# Composite Models
# =============================================================================

class APEXPipelineConfig(BaseModel):
    """Complete APEX pipeline configuration."""
    pipeline_id: str
    pipeline_name: str
    dag_id: str
    pattern_code: str
    feed_id: str
    source_id: str
    domain_id: str
    contract_id: Optional[str] = None
    schemas: Dict[str, Any]
    views: Optional[Dict[str, ViewDefinition]] = None
    transformations: Optional[List[TransformationRule]] = None
    validations: Optional[List[ValidationRule]] = None
    is_active: bool = True
    metadata: Optional[Dict[str, Any]] = None
    feed: Optional["Feed"] = None
    contract: Optional["DataContract"] = None
    source: Optional["SourceRegistry"] = None


class APEXGenerationRequest(BaseModel):
    """Request model for APEX pipeline generation."""
    request_id: str
    feed_name: str
    domain: str
    source_type: str
    file_format: Optional[str] = None
    load_type: str = "FULL"
    target_zones: List[str]
    bronze_schema: Optional[Dict[str, Any]] = None
    natural_language_description: Optional[str] = None
    created_by: str
    jira_ticket: Optional[str] = None


class APEXGenerationResponse(BaseModel):
    """Response model for APEX pipeline generation."""
    feed_id: str
    contract_id: str
    dag_id: str
    pattern_code: str
    generated_dag_path: str
    generated_spark_jobs: List[str]
    generated_sql: Optional[List[str]] = None
    validation_passed: bool = True
    validation_messages: Optional[List[str]] = None
    metadata_inserted: bool = False
    git_commit_sha: Optional[str] = None
    pull_request_url: Optional[str] = None
    created_at: Optional[datetime] = None
    request_id: Optional[str] = None
