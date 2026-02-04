"""
Schema definition models matching pipeline_schemas table.

Schemas are versioned (SCD Type 2 pattern) and stored as JSONB.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DataType(str, Enum):
    """Supported data types."""
    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    ARRAY = "array"
    STRUCT = "struct"
    MAP = "map"


class PIIClassification(str, Enum):
    """PII classification levels."""
    NONE = "none"
    PII = "pii"
    PHI = "phi"
    PCI = "pci"
    SENSITIVE = "sensitive"


class ColumnDefinition(BaseModel):
    """
    Single column definition.

    Stored in JSONB array in pipeline_schemas.columns.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=255)
    type: DataType = Field(default=DataType.STRING)
    nullable: bool = Field(default=True)
    description: Optional[str] = Field(None)

    # Keys
    pk: bool = Field(default=False, description="Is primary key")
    partition_key: bool = Field(default=False)
    cluster_key: bool = Field(default=False)

    # Constraints
    default_value: Optional[str] = Field(None)
    validation_regex: Optional[str] = Field(None)

    # Data governance
    pii: PIIClassification = Field(default=PIIClassification.NONE)

    # For EBCDIC/Fixed-width
    start_position: Optional[int] = Field(None, description="1-based start position")
    length: Optional[int] = Field(None)
    decimal_places: Optional[int] = Field(None, description="For COMP-3/packed decimal")

    # For nested types
    element_type: Optional[str] = Field(None, description="For ARRAY type")
    struct_fields: Optional[List["ColumnDefinition"]] = Field(None, description="For STRUCT type")

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        """Ensure column name is a valid identifier."""
        import re
        if not re.match(r"^[a-z_][a-z0-9_]*$", v.lower()):
            raise ValueError(
                f"Column name '{v}' must start with letter/underscore and contain "
                "only letters, numbers, and underscores"
            )
        return v.lower()


class SchemaConfig(BaseModel):
    """
    Schema configuration matching pipeline_schemas table.
    """
    model_config = ConfigDict(from_attributes=True)

    schema_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)
    schema_version: str = Field(default="1.0.0")
    is_current: bool = Field(default=True)

    # Column definitions (stored as JSONB)
    columns: List[ColumnDefinition] = Field(..., min_length=1)

    # Keys (stored as JSONB arrays)
    primary_keys: List[str] = Field(default_factory=list)
    partition_columns: List[str] = Field(default_factory=list)
    cluster_columns: List[str] = Field(default_factory=list)

    # Metadata
    created_at: Optional[datetime] = Field(default=None)
    created_by: Optional[str] = Field(None)

    @field_validator("columns")
    @classmethod
    def validate_unique_columns(cls, v: List[ColumnDefinition]) -> List[ColumnDefinition]:
        """Ensure column names are unique."""
        names = [col.name for col in v]
        if len(names) != len(set(names)):
            duplicates = [n for n in names if names.count(n) > 1]
            raise ValueError(f"Duplicate column names: {set(duplicates)}")
        return v

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_pk_columns(self) -> List[ColumnDefinition]:
        """Get primary key columns."""
        return [col for col in self.columns if col.pk]

    def get_pii_columns(self) -> List[ColumnDefinition]:
        """Get columns with PII classification."""
        return [col for col in self.columns if col.pii != PIIClassification.NONE]


class SchemaVersion(BaseModel):
    """
    Schema version for tracking changes (SCD Type 2).
    """
    model_config = ConfigDict(extra="forbid")

    version: str
    is_current: bool
    change_type: str = Field(..., description="initial, add_column, drop_column, modify_column")
    change_description: Optional[str] = Field(None)
    effective_from: datetime
    effective_to: Optional[datetime] = Field(None)
    schema_hash: str = Field(..., description="SHA256 of column definitions")
