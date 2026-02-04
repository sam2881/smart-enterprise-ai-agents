"""
Pipeline registry models matching pipeline_registry table.

Maps to: agents/data_agent/src/templates/sql/metadata/schema.sql
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, EmailStr


class Environment(str, Enum):
    """Deployment environments."""
    DEV = "dev"
    QA = "qa"
    PROD = "prod"


class PipelineStatus(str, Enum):
    """Pipeline lifecycle status."""
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"


class PipelineConfig(BaseModel):
    """
    Pipeline configuration matching pipeline_registry table.

    This is the core identity for a data pipeline.
    """
    model_config = ConfigDict(from_attributes=True)

    # Identity
    pipeline_id: Optional[UUID] = Field(default=None)
    dag_id: str = Field(..., max_length=255, pattern=r"^[a-z][a-z0-9_]*$")
    pipeline_name: str = Field(..., max_length=255)

    # Domain classification
    domain: str = Field(..., max_length=100, description="Business domain: sales, finance, hr")
    subdomain: Optional[str] = Field(None, max_length=100)
    product_code: str = Field(..., max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    description: Optional[str] = Field(None)

    # Ownership
    owner_team: str = Field(..., max_length=100)
    owner_email: EmailStr

    # Jira integration
    jira_ticket: Optional[str] = Field(None, max_length=50, pattern=r"^[A-Z]+-\d+$")
    jira_epic: Optional[str] = Field(None, max_length=50)

    # Environment & Status
    environment: Environment = Field(default=Environment.DEV)
    status: PipelineStatus = Field(default=PipelineStatus.ACTIVE)
    version: str = Field(default="1.0.0", max_length=20)

    # Timestamps (read-only from DB)
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)
    deployed_at: Optional[datetime] = Field(default=None)


class PipelineCreate(BaseModel):
    """Input model for creating a new pipeline."""
    model_config = ConfigDict(extra="forbid")

    dag_id: str = Field(..., max_length=255, pattern=r"^[a-z][a-z0-9_]*$")
    pipeline_name: str = Field(..., max_length=255)
    domain: str = Field(..., max_length=100)
    subdomain: Optional[str] = Field(None, max_length=100)
    product_code: str = Field(..., max_length=100)
    description: Optional[str] = Field(None)
    owner_team: str = Field(..., max_length=100)
    owner_email: EmailStr
    jira_ticket: Optional[str] = Field(None)
    jira_epic: Optional[str] = Field(None)
    environment: Environment = Field(default=Environment.DEV)


class PipelineUpdate(BaseModel):
    """Input model for updating a pipeline."""
    model_config = ConfigDict(extra="forbid")

    pipeline_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None)
    owner_team: Optional[str] = Field(None, max_length=100)
    owner_email: Optional[EmailStr] = Field(None)
    status: Optional[PipelineStatus] = Field(None)
    version: Optional[str] = Field(None, max_length=20)
