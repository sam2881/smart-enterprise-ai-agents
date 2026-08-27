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
    SourceCategory,
    SourceFormat,
    FileSourceConfig,
    DatabaseSourceConfig,
    NoSQLSourceConfig,
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
    ResolvedExecutionPlan,
    PipelineHealthScore,
    SourcePlanConfig,
    ZoneValidationRules,
    ZoneTargetConfig,
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

# Production Metadata models (PostgreSQL table mirrors)
from .production_metadata import (
    DataVaultType,
    ZoneType,
    LoadTypeEnum,
    FileFormatEnum,
    IngestionStatus,
    ContractStatus,
    ValidationSeverity,
    FeedGroupDetails,
    FeedDetails,
    ContractDetails,
    SchemaDetails,
    FeedIngestionDetails,
    ColumnSpec,
    ExpectationRule,
    TransformationSpec,
    FeedInputParam,
    InputParamConfig,
    ProductionExecutionPlan,
    CreateFeedGroupRequest,
    CreateFeedRequest,
    CreateContractRequest,
    GeneratePipelineRequest,
)

# APEX-specific models
from .apex_models import (
    # Enums
    FileFormat,
    LoadType,
    ZoneLevel,
    ValidationType,
    ExecutionStatus,
    TemplateType,
    PatternCode,
    FeedType,
    ContractType,
    PipelineStatus,
    PipelinePhase,
    # Core registry models
    ConnectionRegistry,
    DomainRegistry,
    SourceRegistry,
    DAGTemplate,
    SparkConfig,
    PatternInfo,
    # Feed and contract models
    FeedGroup,
    Feed,
    DataContract,
    # View and transformation models
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
    # Source (canonical)
    "SourceConfig",
    "SourceType",
    "SourceCategory",
    "SourceFormat",
    "FileSourceConfig",
    "DatabaseSourceConfig",
    "NoSQLSourceConfig",
    "StreamingSourceConfig",
    "APISourceConfig",
    "EBCDICSourceConfig",
    "DTSXSourceConfig",
    # Schema (canonical)
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
    # Quality (canonical)
    "QualityRule",
    "QualityRuleType",
    "Severity",
    # Execution
    "ExecutionPolicy",
    "ProcessingMode",
    "ResolvedExecutionPlan",
    "PipelineHealthScore",
    "SourcePlanConfig",
    "ZoneValidationRules",
    "ZoneTargetConfig",
    # Canonical
    "PipelineMetadata",
    "UnifiedPipelineInput",
    # Gold Zone
    "GoldModelingStrategy",
    "GoldZoneConfig",
    "DataVault2Config",
    "StarSchemaConfig",
    "SnowflakeSchemaConfig",
    "FlatTableConfig",
    "HubConfig",
    "LinkConfig",
    "SatelliteConfig",
    "FactTableConfig",
    "MeasureConfig",
    "DimensionConfig",
    # APEX Enums
    "FileFormat",
    "LoadType",
    "ZoneLevel",
    "ValidationType",
    "ExecutionStatus",
    "TemplateType",
    "PatternCode",
    "FeedType",
    "ContractType",
    "PipelineStatus",
    "PipelinePhase",
    # APEX Core registry
    "ConnectionRegistry",
    "DomainRegistry",
    "SourceRegistry",
    "DAGTemplate",
    "SparkConfig",
    "PatternInfo",
    # APEX Feed and contract
    "FeedGroup",
    "Feed",
    "DataContract",
    # APEX View and transformation
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
    # Production Metadata Enums
    "DataVaultType",
    "ZoneType",
    "LoadTypeEnum",
    "FileFormatEnum",
    "IngestionStatus",
    "ContractStatus",
    "ValidationSeverity",
    # Production Metadata Core
    "FeedGroupDetails",
    "FeedDetails",
    "ContractDetails",
    "SchemaDetails",
    "FeedIngestionDetails",
    # Production Metadata Components
    "ColumnSpec",
    "ExpectationRule",
    "TransformationSpec",
    # Production Multi-Feed Patterns
    "FeedInputParam",
    "InputParamConfig",
    "ProductionExecutionPlan",
    # Production API Models
    "CreateFeedGroupRequest",
    "CreateFeedRequest",
    "CreateContractRequest",
    "GeneratePipelineRequest",
]
