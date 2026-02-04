"""
Gold Zone Modeling Configuration Models

Pydantic models for Data Vault 2.0, Star Schema, Snowflake Schema, and Flat Table patterns.
These models mirror the TypeScript definitions in frontend/src/types/pipeline-canonical.ts
"""

from enum import Enum
from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field


class GoldModelingStrategy(str, Enum):
    """Gold zone modeling patterns."""
    DATA_VAULT_2 = "data_vault_2"
    STAR_SCHEMA = "star_schema"
    SNOWFLAKE_SCHEMA = "snowflake_schema"
    FLAT_TABLE = "flat_table"


class HashAlgorithm(str, Enum):
    """Hash algorithms for Data Vault."""
    MD5 = "md5"
    SHA256 = "sha256"


class RelationshipType(str, Enum):
    """Data Vault relationship types."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"


class SCDType(str, Enum):
    """Slowly Changing Dimension types."""
    TYPE_0 = "type_0"  # Retain original
    TYPE_1 = "type_1"  # Overwrite
    TYPE_2 = "type_2"  # Add new row
    TYPE_3 = "type_3"  # Add new column


class AggregationType(str, Enum):
    """Aggregation functions."""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"


class RefreshStrategy(str, Enum):
    """Flat table refresh strategies."""
    FULL_REFRESH = "full_refresh"
    INCREMENTAL_APPEND = "incremental_append"
    INCREMENTAL_MERGE = "incremental_merge"


# =============================================================================
# Data Vault 2.0 Configuration
# =============================================================================

class HubConfig(BaseModel):
    """Data Vault Hub configuration."""
    hub_name: str
    business_keys: List[str]
    hash_algorithm: HashAlgorithm = HashAlgorithm.SHA256
    source_system: Optional[str] = None


class LinkConfig(BaseModel):
    """Data Vault Link configuration."""
    link_name: str
    hub_references: List[str]  # Hub names to link
    relationship_type: RelationshipType
    descriptive_attributes: Optional[List[str]] = Field(default_factory=list)


class SatelliteConfig(BaseModel):
    """Data Vault Satellite configuration."""
    satellite_name: str
    parent_hub: str  # Hub name this satellite describes
    descriptive_attributes: List[str]
    hash_diff: bool = True
    effective_from_column: str = "effective_from"
    load_timestamp_column: str = "load_timestamp"


class DataVault2Config(BaseModel):
    """Complete Data Vault 2.0 configuration."""
    hubs: List[HubConfig]
    links: List[LinkConfig]
    satellites: List[SatelliteConfig]
    record_source: str
    default_hash_algorithm: HashAlgorithm = HashAlgorithm.SHA256


# =============================================================================
# Star Schema Configuration
# =============================================================================

class FactTableConfig(BaseModel):
    """Star schema fact table configuration."""
    fact_name: str
    grain_columns: List[str]
    measures: List["MeasureConfig"]
    dimension_foreign_keys: List[str]
    partition_by: Optional[List[str]] = None


class MeasureConfig(BaseModel):
    """Fact table measure configuration."""
    column: str
    aggregation: AggregationType
    data_type: str = "decimal"
    nullable: bool = True


class DimensionConfig(BaseModel):
    """Star schema dimension configuration."""
    dimension_name: str
    scd_type: SCDType
    natural_key: List[str]
    attributes: List[str]
    is_conformed: bool = False
    effective_from_column: Optional[str] = "effective_from"
    effective_to_column: Optional[str] = "effective_to"
    current_flag_column: Optional[str] = "is_current"


class StarSchemaConfig(BaseModel):
    """Complete star schema configuration."""
    fact_table: FactTableConfig
    dimensions: List[DimensionConfig]
    conformed_dimensions: Optional[List[str]] = Field(default_factory=list)


# =============================================================================
# Snowflake Schema Configuration
# =============================================================================

class SnowflakeDimensionConfig(DimensionConfig):
    """Snowflake schema dimension with parent references."""
    parent_dimension: Optional[str] = None  # For normalization
    normalization_columns: Optional[List[str]] = Field(default_factory=list)


class SnowflakeSchemaConfig(BaseModel):
    """Complete snowflake schema configuration."""
    fact_table: FactTableConfig
    dimensions: List[SnowflakeDimensionConfig]
    conformed_dimensions: Optional[List[str]] = Field(default_factory=list)
    normalization_level: int = Field(default=2, ge=1, le=3)


# =============================================================================
# Flat Table Configuration
# =============================================================================

class FlatTableConfig(BaseModel):
    """Flat table (OBT) configuration."""
    table_name: str
    refresh_strategy: RefreshStrategy
    partition_by: Optional[List[str]] = None
    cluster_by: Optional[List[str]] = None
    incremental_key: Optional[str] = None
    full_refresh_schedule: Optional[str] = "@weekly"


# =============================================================================
# Unified Gold Zone Configuration
# =============================================================================

class GoldZoneConfig(BaseModel):
    """Unified gold zone configuration supporting multiple modeling strategies."""
    modeling_strategy: GoldModelingStrategy
    data_vault_config: Optional[DataVault2Config] = None
    star_schema_config: Optional[StarSchemaConfig] = None
    snowflake_schema_config: Optional[SnowflakeSchemaConfig] = None
    flat_table_config: Optional[FlatTableConfig] = None

    def validate_config(self) -> bool:
        """Validate that the appropriate config exists for the selected strategy."""
        if self.modeling_strategy == GoldModelingStrategy.DATA_VAULT_2:
            return self.data_vault_config is not None
        elif self.modeling_strategy == GoldModelingStrategy.STAR_SCHEMA:
            return self.star_schema_config is not None
        elif self.modeling_strategy == GoldModelingStrategy.SNOWFLAKE_SCHEMA:
            return self.snowflake_schema_config is not None
        elif self.modeling_strategy == GoldModelingStrategy.FLAT_TABLE:
            return self.flat_table_config is not None
        return False


# Update forward references
FactTableConfig.model_rebuild()
