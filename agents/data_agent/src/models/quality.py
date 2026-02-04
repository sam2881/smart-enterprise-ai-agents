"""
Quality rule models matching pipeline_quality_rules table.

Data quality gates enforce thresholds at each medallion zone.
"""

from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class QualityRuleType(str, Enum):
    """Quality rule types."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    REGEX = "regex"
    REFERENTIAL = "referential"
    FRESHNESS = "freshness"
    CUSTOM = "custom"
    ROW_COUNT = "row_count"
    COMPLETENESS = "completeness"


class Severity(str, Enum):
    """Rule violation severity levels."""
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# =============================================================================
# Rule Configuration Schemas
# =============================================================================


class NotNullConfig(BaseModel):
    """Config for not_null rule."""
    column: str


class UniqueConfig(BaseModel):
    """Config for uniqueness rule."""
    columns: list[str] = Field(..., min_length=1)


class RangeConfig(BaseModel):
    """Config for range validation rule."""
    column: str
    min: Optional[float] = Field(None)
    max: Optional[float] = Field(None)


class RegexConfig(BaseModel):
    """Config for regex pattern validation."""
    column: str
    pattern: str = Field(...)


class ReferentialConfig(BaseModel):
    """Config for referential integrity rule."""
    column: str
    reference_table: str
    reference_column: str


class FreshnessConfig(BaseModel):
    """Config for data freshness rule."""
    column: str
    max_age_hours: int = Field(default=24, ge=1)


class RowCountConfig(BaseModel):
    """Config for row count validation."""
    min_count: Optional[int] = Field(None)
    max_count: Optional[int] = Field(None)


class CustomConfig(BaseModel):
    """Config for custom SQL expression rule."""
    expression: str = Field(..., description="SQL expression returning boolean")


# =============================================================================
# Unified Quality Rule
# =============================================================================


class QualityRule(BaseModel):
    """
    Quality rule configuration matching pipeline_quality_rules table.
    """
    model_config = ConfigDict(from_attributes=True)

    rule_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)

    # Rule definition
    rule_name: str = Field(..., max_length=100)
    rule_type: QualityRuleType
    column_name: Optional[str] = Field(None, max_length=100)

    # Configuration (type-specific JSONB)
    config: Dict[str, Any] = Field(default_factory=dict)

    # Thresholds
    severity: Severity = Field(default=Severity.WARNING)
    threshold_pct: float = Field(
        default=100.0,
        ge=0,
        le=100,
        description="Percentage of records that must pass"
    )

    is_active: bool = Field(default=True)

    def get_typed_config(self) -> Optional[BaseModel]:
        """Get typed configuration based on rule_type."""
        config_map = {
            QualityRuleType.NOT_NULL: NotNullConfig,
            QualityRuleType.UNIQUE: UniqueConfig,
            QualityRuleType.RANGE: RangeConfig,
            QualityRuleType.REGEX: RegexConfig,
            QualityRuleType.REFERENTIAL: ReferentialConfig,
            QualityRuleType.FRESHNESS: FreshnessConfig,
            QualityRuleType.ROW_COUNT: RowCountConfig,
            QualityRuleType.CUSTOM: CustomConfig,
        }
        config_class = config_map.get(self.rule_type)
        if config_class and self.config:
            return config_class(**self.config)
        return None

    def to_great_expectations(self) -> Dict[str, Any]:
        """Convert to Great Expectations expectation format."""
        ge_map = {
            QualityRuleType.NOT_NULL: {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": self.column_name},
            },
            QualityRuleType.UNIQUE: {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": self.column_name},
            },
            QualityRuleType.RANGE: {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": self.column_name,
                    "min_value": self.config.get("min"),
                    "max_value": self.config.get("max"),
                },
            },
            QualityRuleType.REGEX: {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": self.column_name,
                    "regex": self.config.get("pattern"),
                },
            },
        }
        return ge_map.get(self.rule_type, {})
