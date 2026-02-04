"""
Canonical pipeline metadata model.

This is the UNIFIED internal representation that:
1. All UI input gets normalized to
2. All NL input gets converted to
3. PostgreSQL stores and retrieves
4. DAGs and Spark jobs read at runtime

IMPORTANT: This is the single source of truth.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .pipeline import PipelineConfig, Environment
from .source import SourceConfig, SourceType
from .schema import SchemaConfig, ColumnDefinition
from .target import TargetConfig, TargetZone
from .transformation import TransformConfig, NLTransformInput
from .quality import QualityRule
from .execution import ExecutionPolicy


class InputType(str, Enum):
    """Input source types."""
    UI_STRUCTURED = "ui_structured"
    NATURAL_LANGUAGE = "natural_language"
    JIRA_TICKET = "jira_ticket"
    DTSX_MIGRATION = "dtsx_migration"


class UnifiedPipelineInput(BaseModel):
    """
    Unified input model that accepts both structured and NL input.

    The normalizer converts this to PipelineMetadata.
    """
    model_config = ConfigDict(extra="forbid")

    input_type: InputType = Field(default=InputType.UI_STRUCTURED)

    # Metadata
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    created_by: str = Field(...)
    jira_ticket: Optional[str] = Field(None)

    # =========================================================================
    # Option 1: Structured UI Input (preferred)
    # =========================================================================
    pipeline: Optional[Dict[str, Any]] = Field(None)
    source: Optional[Dict[str, Any]] = Field(None)
    schema: Optional[Dict[str, Any]] = Field(None)
    target: Optional[Dict[str, Any]] = Field(None)
    transformations: Optional[List[Dict[str, Any]]] = Field(None)
    joins: Optional[List[Dict[str, Any]]] = Field(
        None, description="Multi-table join definitions for Gold zone"
    )
    quality_rules: Optional[List[Dict[str, Any]]] = Field(None)
    execution_policy: Optional[Dict[str, Any]] = Field(None)

    # =========================================================================
    # Option 2: Natural Language Input (gets converted to structured)
    # =========================================================================
    natural_language: Optional[str] = Field(
        None,
        description="Natural language description of the pipeline"
    )

    # =========================================================================
    # Option 3: DTSX Migration Input
    # =========================================================================
    dtsx_path: Optional[str] = Field(None, description="GCS path to .dtsx file")
    dtsx_package_name: Optional[str] = Field(None)

    @model_validator(mode="after")
    def validate_input(self) -> "UnifiedPipelineInput":
        """Ensure at least one input type is provided."""
        has_structured = bool(self.pipeline or self.source)
        has_nl = bool(self.natural_language)
        has_dtsx = bool(self.dtsx_path)

        if not any([has_structured, has_nl, has_dtsx]):
            raise ValueError(
                "Must provide at least one of: structured input (pipeline/source), "
                "natural_language, or dtsx_path"
            )

        # Set input type based on provided data
        if has_nl:
            object.__setattr__(self, 'input_type', InputType.NATURAL_LANGUAGE)
        elif has_dtsx:
            object.__setattr__(self, 'input_type', InputType.DTSX_MIGRATION)

        return self


class PipelineMetadata(BaseModel):
    """
    Canonical pipeline metadata model.

    This is the SINGLE SOURCE OF TRUTH that:
    1. Gets persisted to PostgreSQL
    2. Gets retrieved by DAGs at runtime via get_pipeline_config(dag_id, env)
    3. Drives all code generation via Jinja templates

    Structure matches the PostgreSQL function response:
    {
        "pipeline": {...},
        "source": {...},
        "schema": {...},
        "target": {...},
        "transformations": [...],
        "quality_rules": [...],
        "execution_policy": {...}
    }
    """
    model_config = ConfigDict(from_attributes=True)

    # Core configuration objects
    pipeline: PipelineConfig
    source: SourceConfig
    schema: SchemaConfig
    target: TargetConfig
    transformations: List[TransformConfig] = Field(default_factory=list)
    quality_rules: List[QualityRule] = Field(default_factory=list)
    execution_policy: ExecutionPolicy

    # Metadata for tracking
    metadata_version: str = Field(default="1.0.0")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(None)

    # NL processing artifacts (if input was NL)
    nl_input: Optional[str] = Field(None, description="Original NL input if applicable")
    nl_processing_log: Optional[List[Dict[str, Any]]] = Field(None)

    def to_postgres_json(self) -> Dict[str, Any]:
        """Convert to JSON format matching get_pipeline_config() response."""
        return {
            "pipeline": self.pipeline.model_dump(mode="json"),
            "source": self.source.model_dump(mode="json"),
            "schema": self.schema.model_dump(mode="json"),
            "target": self.target.model_dump(mode="json"),
            "transformations": [t.model_dump(mode="json") for t in self.transformations],
            "quality_rules": [r.model_dump(mode="json") for r in self.quality_rules],
            "execution_policy": self.execution_policy.model_dump(mode="json"),
        }

    def get_dag_id(self) -> str:
        """Get the DAG ID for this pipeline."""
        return self.pipeline.dag_id

    def get_environment(self) -> str:
        """Get the environment for this pipeline."""
        return self.pipeline.environment.value

    def get_source_type(self) -> SourceType:
        """Get the source type."""
        return self.source.source_type

    def get_target_zone(self) -> TargetZone:
        """Get the target zone."""
        return self.target.target_zone

    def requires_approval(self) -> bool:
        """Check if this pipeline requires human approval."""
        return (
            self.execution_policy.requires_human_approval or
            self.pipeline.environment == Environment.PROD
        )

    def get_quality_threshold(self) -> float:
        """Get the minimum quality score threshold."""
        critical_rules = [r for r in self.quality_rules if r.severity == "critical"]
        if critical_rules:
            return min(r.threshold_pct for r in critical_rules)
        return 95.0  # Default threshold


class RuntimeConfig(BaseModel):
    """
    Runtime configuration fetched by DAGs via get_pipeline_config().

    This is what DAGs see at execution time.
    """
    model_config = ConfigDict(extra="allow")

    dag_id: str
    environment: str
    pipeline: Dict[str, Any]
    source: Dict[str, Any]
    schema: Dict[str, Any]
    target: Dict[str, Any]
    transformations: List[Dict[str, Any]]
    quality_rules: List[Dict[str, Any]]
    execution_policy: Dict[str, Any]

    @classmethod
    def from_postgres_json(cls, data: Dict[str, Any]) -> "RuntimeConfig":
        """Create from PostgreSQL get_pipeline_config() response."""
        return cls(
            dag_id=data["pipeline"]["dag_id"],
            environment=data["pipeline"]["environment"],
            **data
        )
