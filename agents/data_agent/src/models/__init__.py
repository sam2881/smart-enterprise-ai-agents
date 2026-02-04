"""
Core Pydantic models matching PostgreSQL metadata schema.

These models serve as the canonical internal representation.
All input (UI or NL) gets normalized to these models before processing.
"""

from .pipeline import (
    PipelineConfig,
    PipelineCreate,
    PipelineUpdate,
)
from .source import (
    SourceConfig,
    SourceType,
    SourceFormat,
    FileSourceConfig,
    DatabaseSourceConfig,
    StreamingSourceConfig,
    APISourceConfig,
    EBCDICSourceConfig,
    DTSXSourceConfig,
)
from .schema import (
    ColumnDefinition,
    SchemaConfig,
    SchemaVersion,
)
from .target import (
    TargetConfig,
    TargetZone,
    WriteMode,
    DestinationModel,
)
from .transformation import (
    TransformConfig,
    TransformType,
    NLTransformInput,
)
from .quality import (
    QualityRule,
    QualityRuleType,
    Severity,
)
from .execution import (
    ExecutionPolicy,
    ProcessingMode,
)
from .canonical import (
    PipelineMetadata,
    UnifiedPipelineInput,
)

# Gold Zone models
from .gold_zone import (
    GoldModelingStrategy,
    GoldZoneConfig,
    DataVault2Config,
    StarSchemaConfig,
    SnowflakeSchemaConfig,
    FlatTableConfig,
    HubConfig,
    LinkConfig,
    SatelliteConfig,
    FactTableConfig,
    MeasureConfig,
    DimensionConfig,
)

# APEX-specific models (mirror PostgreSQL metadata schema)
from .apex_models import (
    # Enums
    SourceType as APEXSourceType,
    FileFormat,
    LoadType,
    ZoneLevel,
    ValidationType,
    Severity as APEXSeverity,
    ExecutionStatus,
    TemplateType,
    PatternCode,
    FeedType,
    ContractType,
    # Core registry models
    ConnectionRegistry,
    DomainRegistry,
    SourceRegistry,
    DAGTemplate,
    SparkConfig,
    # Feed and contract models
    FeedGroup,
    Feed,
    DataContract,
    # Schema and view models
    ColumnDefinition as APEXColumnDefinition,
    SchemaVersion as APEXSchemaVersion,
    ViewDefinition,
    TransformationRule,
    ContractTransformation,
    # Validation models
    ValidationRule,
    QualityExpectation,
    SLADefinition,
    PipelineDependency,
    # Execution models
    PipelineExecution,
    TaskExecution,
    AuditLog,
    ValidationLog,
    ErrorLog,
    DataLineage,
    AgentDecisionLog,
    # Composite models
    APEXPipelineConfig,
    APEXGenerationRequest,
    APEXGenerationResponse,
)

__all__ = [
    # Pipeline
    "PipelineConfig",
    "PipelineCreate",
    "PipelineUpdate",
    # Source
    "SourceConfig",
    "SourceType",
    "SourceFormat",
    "FileSourceConfig",
    "DatabaseSourceConfig",
    "StreamingSourceConfig",
    "APISourceConfig",
    "EBCDICSourceConfig",
    "DTSXSourceConfig",
    # Schema
    "ColumnDefinition",
    "SchemaConfig",
    "SchemaVersion",
    # Target
    "TargetConfig",
    "TargetZone",
    "WriteMode",
    "DestinationModel",
    # Transformation
    "TransformConfig",
    "TransformType",
    "NLTransformInput",
    # Quality
    "QualityRule",
    "QualityRuleType",
    "Severity",
    # Execution
    "ExecutionPolicy",
    "ProcessingMode",
    # Canonical
    "PipelineMetadata",
    "UnifiedPipelineInput",
    # APEX Enums
    "APEXSourceType",
    "FileFormat",
    "LoadType",
    "ZoneLevel",
    "ValidationType",
    "APEXSeverity",
    "ExecutionStatus",
    "TemplateType",
    "PatternCode",
    "FeedType",
    "ContractType",
    # APEX Core registry
    "ConnectionRegistry",
    "DomainRegistry",
    "SourceRegistry",
    "DAGTemplate",
    "SparkConfig",
    # APEX Feed and contract
    "FeedGroup",
    "Feed",
    "DataContract",
    # APEX Schema and view
    "APEXColumnDefinition",
    "APEXSchemaVersion",
    "ViewDefinition",
    "TransformationRule",
    "ContractTransformation",
    # APEX Validation
    "ValidationRule",
    "QualityExpectation",
    "SLADefinition",
    "PipelineDependency",
    # APEX Execution
    "PipelineExecution",
    "TaskExecution",
    "AuditLog",
    "ValidationLog",
    "ErrorLog",
    "DataLineage",
    "AgentDecisionLog",
    # APEX Composite
    "APEXPipelineConfig",
    "APEXGenerationRequest",
    "APEXGenerationResponse",
]
