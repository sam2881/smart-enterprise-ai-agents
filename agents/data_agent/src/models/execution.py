"""
Execution policy models matching platform_pipeline_execution_policies table.
"""

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, EmailStr


class ProcessingMode(str, Enum):
    """Data processing modes."""
    BATCH = "batch"
    MICRO_BATCH = "micro_batch"
    STREAMING = "streaming"


class ExecutionPolicy(BaseModel):
    """
    Execution policy matching platform_pipeline_execution_policies table.
    """
    model_config = ConfigDict(from_attributes=True)

    policy_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)

    # Schedule
    schedule_interval: Optional[str] = Field(
        None,
        description="Cron expression or @daily, @hourly"
    )
    start_date: date = Field(default_factory=date.today)
    end_date: Optional[date] = Field(None)
    catchup: bool = Field(default=False)

    # Processing mode
    processing_mode: ProcessingMode = Field(default=ProcessingMode.BATCH)
    max_active_runs: int = Field(default=1, ge=1)

    # Retry policy
    retry_count: int = Field(default=2, ge=0, le=10)
    retry_delay_minutes: int = Field(default=5, ge=1)
    timeout_hours: int = Field(default=2, ge=1)

    # SLA
    sla_hours: Optional[float] = Field(None, ge=0)

    # Human approval (required for prod)
    requires_human_approval: bool = Field(default=False)
    approval_groups: List[str] = Field(
        default_factory=list,
        description="Groups that can approve: ['data-stewards', 'domain-owners']"
    )

    # Alerting
    alert_emails: List[EmailStr] = Field(default_factory=list)
    slack_webhook: Optional[str] = Field(None)

    # Timestamps
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)


class ExecutionRecord(BaseModel):
    """
    Execution tracking matching platform_pipeline_executions table.
    """
    model_config = ConfigDict(from_attributes=True)

    execution_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)
    dag_run_id: str

    # Timing
    execution_date: datetime
    start_time: datetime
    end_time: Optional[datetime] = Field(None)
    duration_seconds: Optional[int] = Field(None)

    # Status
    status: str = Field(default="running")

    # Metrics
    records_read: int = Field(default=0)
    records_written: int = Field(default=0)
    records_failed: int = Field(default=0)
    bytes_processed: int = Field(default=0)

    # Quality
    quality_score: Optional[float] = Field(None)
    quality_passed: Optional[bool] = Field(None)

    # Error details
    error_message: Optional[str] = Field(None)
    error_task: Optional[str] = Field(None)

    # Metadata
    triggered_by: str = Field(default="scheduler")
    airflow_log_url: Optional[str] = Field(None)


class PipelineEvent(BaseModel):
    """
    Audit event matching platform_pipeline_events table.
    """
    model_config = ConfigDict(from_attributes=True)

    event_id: Optional[UUID] = Field(default=None)
    dag_id: str
    execution_date: Optional[str] = Field(None)

    event_type: str = Field(
        ...,
        description="STARTED, SUCCESS, FAILED, APPROVAL_REQUESTED, APPROVED, REJECTED"
    )
    event_data: dict = Field(default_factory=dict)

    jira_ticket: Optional[str] = Field(None)
    environment: Optional[str] = Field(None)

    created_at: Optional[datetime] = Field(default=None)


# =============================================================================
# Control Plane / Data Plane Separation
# =============================================================================


class SourcePlanConfig(BaseModel):
    """
    Source configuration within execution plan.
    Simplified structure passed to Spark jobs.
    """
    model_config = ConfigDict(extra="allow")

    source_type: str
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    file_pattern: Optional[str] = None
    format: Optional[str] = None
    delimiter: Optional[str] = None
    header: Optional[bool] = None
    encoding: Optional[str] = None

    # Database configs
    connection_string: Optional[str] = None
    query: Optional[str] = None
    table: Optional[str] = None

    # Streaming configs
    topic: Optional[str] = None
    bootstrap_servers: Optional[str] = None

    # Additional configs (flexible)
    extra_config: Dict[str, Any] = Field(default_factory=dict)


class ZoneValidationRules(BaseModel):
    """Validation rules for a specific zone."""
    model_config = ConfigDict(extra="allow")

    zone: str
    rules: List[Dict[str, Any]] = Field(default_factory=list)


class ZoneTargetConfig(BaseModel):
    """Target configuration for a specific zone."""
    model_config = ConfigDict(extra="allow")

    dataset: str
    table: str
    partition_column: Optional[str] = None
    clustering_columns: List[str] = Field(default_factory=list)
    write_mode: str = Field(default="append")


class ResolvedExecutionPlan(BaseModel):
    """
    CONTROL PLANE OUTPUT → DATA PLANE INPUT

    Immutable execution plan generated by Control Plane (Airflow DAG).
    Spark jobs receive this and execute deterministically - NO decisions in data plane.

    This is the contract between control plane and data plane:
    - Control Plane: Decides WHAT to do (transformations, validations, retry logic)
    - Data Plane: Executes HOW (PySpark code, SQL queries)

    Benefits:
    1. Reproducibility: Same plan = same result
    2. Auditability: Plan is versioned and stored
    3. Testability: Can test plan generation separately from execution
    4. Separation of concerns: Decision logic isolated from execution logic
    """
    model_config = ConfigDict(extra="allow")

    # Identifiers
    plan_id: str = Field(..., description="Unique execution plan ID (same as execution_id)")
    feed_id: str = Field(..., description="Feed identifier")
    batch_id: str = Field(..., description="Batch identifier (e.g., YYYYMMDD)")
    pattern: str = Field(..., description="APEX pattern code (P01-P09)")

    # Source configuration (immutable snapshot)
    source: SourcePlanConfig = Field(..., description="Source details for this execution")

    # Schema (versioned snapshot from metadata)
    schema: Dict[str, Any] = Field(..., description="Schema metadata including columns")

    # Transformations (ordered list - deterministic execution)
    transformations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Ordered list of transformations to apply"
    )

    # Validations by zone (explicit rules)
    validations: Dict[str, List[Dict[str, Any]]] = Field(
        default_factory=dict,
        description="Validation rules per zone: {'bronze': [...], 'silver': [...], 'gold': [...]}"
    )

    # Targets by zone
    targets: Dict[str, Any] = Field(
        default_factory=dict,
        description="Target configs: {'landing': 'gs://...', 'bronze': {...}, 'silver': {...}, 'gold': {...}}"
    )

    # Execution context
    metadata_db_url: str = Field(..., description="Metadata database URL for logging/audit")
    execution_id: str = Field(..., description="Airflow execution ID for correlation")

    # Optional: Retry/error handling config
    retry_config: Optional[Dict[str, Any]] = Field(
        None,
        description="Retry strategy if errors occur"
    )

    # Optional: Schema evolution decisions
    schema_evolution_mode: Optional[str] = Field(
        None,
        description="How to handle schema changes: 'REJECT', 'WARN', 'ADAPT'"
    )


class PipelineHealthScore(BaseModel):
    """
    Pipeline Health Score - First-Class Entity

    User improvement #3: Health should be explicit, not derived.

    health_score = quality + freshness + stability + cost_efficiency

    Used for:
    - Promotion gates (dev → qa → prod)
    - Alert suppression (don't alert if health is good)
    - Dashboards (track health over time)
    - Capacity planning (identify struggling pipelines)
    """
    model_config = ConfigDict(from_attributes=True)

    score_id: Optional[UUID] = Field(default=None)
    feed_id: str
    execution_id: str
    calculated_at: datetime = Field(default_factory=datetime.utcnow)

    # Component scores (0.0 - 1.0)
    quality_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Data quality: pass rate of validation rules"
    )
    freshness_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Data freshness: on-time delivery rate"
    )
    stability_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Pipeline stability: success rate, no errors"
    )
    cost_efficiency_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Cost efficiency: slot-hours usage vs budget"
    )

    # Overall health (weighted average)
    overall_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Overall health score (weighted: 40% quality, 30% freshness, 20% stability, 10% cost)"
    )

    # Context
    window_days: int = Field(
        default=7,
        description="Rolling window for score calculation"
    )
    sample_size: int = Field(
        ...,
        description="Number of executions analyzed"
    )

    # Details (for drill-down)
    details: Dict[str, Any] = Field(
        default_factory=dict,
        description="Detailed breakdown: failed rules, late runs, error messages, cost breakdown"
    )
