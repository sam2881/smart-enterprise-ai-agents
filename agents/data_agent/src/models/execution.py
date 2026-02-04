"""
Execution policy models matching pipeline_execution_policies table.
"""

from datetime import date, datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, EmailStr


class ProcessingMode(str, Enum):
    """Data processing modes."""
    BATCH = "batch"
    MICRO_BATCH = "micro_batch"
    STREAMING = "streaming"


class ExecutionPolicy(BaseModel):
    """
    Execution policy matching pipeline_execution_policies table.
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
    Execution tracking matching pipeline_executions table.
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
    Audit event matching pipeline_events table.
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
