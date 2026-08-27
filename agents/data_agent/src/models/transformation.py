"""
Transformation models matching platform_pipeline_transformations table.

Supports both structured transforms and NL-generated transforms.
NL transforms are ALWAYS converted to structured metadata before execution.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .target import TargetZone


class TransformType(str, Enum):
    """Transformation types."""
    RENAME = "rename"
    CAST = "cast"
    DERIVE = "derive"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    WINDOW = "window"
    SCD = "scd"
    DEDUPLICATE = "deduplicate"
    DEDUP = "dedup"  # Alias for DEDUPLICATE
    NULL_HANDLING = "null_handling"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    MASK = "mask"
    HASH = "hash"
    ENCRYPT = "encrypt"
    SQL = "sql"  # SQL expression transform
    PYSPARK = "pyspark"  # PySpark code transform
    NL_TRANSFORM = "nl_transform"  # Natural language transformed to code


# =============================================================================
# Transform Configuration Schemas (for config JSONB)
# =============================================================================


class RenameConfig(BaseModel):
    """Config for rename transformation."""
    source: str = Field(..., description="Original column name")
    target: str = Field(..., description="New column name")


class CastConfig(BaseModel):
    """Config for cast transformation."""
    column: str
    cast_type: str = Field(..., description="Target data type")
    format: Optional[str] = Field(None, description="Format for date/timestamp")


class DeriveConfig(BaseModel):
    """Config for derive (computed column) transformation."""
    column: str = Field(..., description="New column name")
    expression: str = Field(..., description="Spark SQL expression")
    dependencies: List[str] = Field(default_factory=list, description="Source columns used")


class FilterConfig(BaseModel):
    """Config for filter transformation."""
    condition: str = Field(..., description="SQL WHERE condition")


class AggregateConfig(BaseModel):
    """Config for aggregate transformation."""
    group_by: List[str] = Field(...)
    aggregations: List[Dict[str, str]] = Field(
        ...,
        description="[{column, function, alias}]"
    )


class JoinConfig(BaseModel):
    """Config for join transformation."""
    left_table: str
    right_table: str
    join_type: str = Field(default="inner", description="inner, left, right, full, cross")
    join_keys: List[Dict[str, str]] = Field(
        ...,
        description="[{left: 'col1', right: 'col2'}]"
    )
    select_columns: Optional[List[str]] = Field(None)


class WindowConfig(BaseModel):
    """Config for window function transformation."""
    partition_by: List[str] = Field(default_factory=list)
    order_by: List[Dict[str, str]] = Field(
        ...,
        description="[{column, direction: 'asc'|'desc'}]"
    )
    functions: List[Dict[str, str]] = Field(
        ...,
        description="[{type: 'row_number'|'rank'|'lead'|'lag', alias}]"
    )


class SCDConfig(BaseModel):
    """Config for SCD Type 2 transformation."""
    type: int = Field(default=2, description="SCD type (1 or 2)")
    tracked_columns: List[str] = Field(...)
    effective_from: str = Field(default="valid_from")
    effective_to: str = Field(default="valid_to")
    current_flag: str = Field(default="is_current")
    business_keys: List[str] = Field(...)


class DeduplicateConfig(BaseModel):
    """Config for deduplication transformation."""
    partition_by: List[str] = Field(..., description="Columns to dedupe on")
    order_by: List[Dict[str, str]] = Field(
        ...,
        description="[{column, direction}] - keeps first after ordering"
    )


class MaskConfig(BaseModel):
    """Config for data masking transformation."""
    column: str
    mask_type: str = Field(default="partial", description="full, partial, hash")
    visible_chars: int = Field(default=4, description="Chars to show for partial")
    mask_char: str = Field(default="*")


# =============================================================================
# Natural Language Transform Input
# =============================================================================


class NLTransformInput(BaseModel):
    """
    Natural language transform input.

    IMPORTANT: NL is NEVER executed directly.
    It gets converted to structured TransformConfig before execution.
    """
    model_config = ConfigDict(extra="forbid")

    description: str = Field(
        ...,
        min_length=10,
        description="Natural language description of the transformation"
    )
    source_tables: List[str] = Field(
        default_factory=list,
        description="Tables involved in transformation"
    )
    target_columns: Optional[List[str]] = Field(
        None,
        description="Expected output columns (optional hint)"
    )

    # LLM-generated output (populated after NL processing)
    generated_code: Optional[str] = Field(None)
    generated_config: Optional[Dict[str, Any]] = Field(None)
    confidence_score: Optional[float] = Field(None)


# =============================================================================
# Unified Transform Configuration
# =============================================================================


class TransformConfig(BaseModel):
    """
    Transformation configuration matching platform_pipeline_transformations table.
    """
    model_config = ConfigDict(from_attributes=True)

    transform_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)

    # Transform type and order
    transform_type: TransformType
    transform_order: int = Field(default=0, ge=0)
    zone: TargetZone = Field(default=TargetZone.SILVER)

    # Configuration (type-specific JSONB)
    config: Dict[str, Any] = Field(default_factory=dict)

    # Natural language transform fields
    nl_description: Optional[str] = Field(None)
    generated_pyspark: Optional[str] = Field(None)
    generated_sql: Optional[str] = Field(None)

    is_active: bool = Field(default=True)
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)

    def get_typed_config(self) -> Optional[BaseModel]:
        """Get typed configuration based on transform_type."""
        config_map = {
            TransformType.RENAME: RenameConfig,
            TransformType.CAST: CastConfig,
            TransformType.DERIVE: DeriveConfig,
            TransformType.FILTER: FilterConfig,
            TransformType.AGGREGATE: AggregateConfig,
            TransformType.JOIN: JoinConfig,
            TransformType.WINDOW: WindowConfig,
            TransformType.SCD: SCDConfig,
            TransformType.DEDUPLICATE: DeduplicateConfig,
            TransformType.MASK: MaskConfig,
        }
        config_class = config_map.get(self.transform_type)
        if config_class and self.config:
            return config_class(**self.config)
        return None
