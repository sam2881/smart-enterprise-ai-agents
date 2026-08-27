"""
Target configuration models matching platform_pipeline_targets table.

Supports medallion architecture zones and multiple destination types.
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TargetZone(str, Enum):
    """Medallion architecture zones. Gold is the final layer."""
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class WriteMode(str, Enum):
    """Data write modes."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
    SCD_TYPE_1 = "scd_type_1"
    SCD_TYPE_2 = "scd_type_2"


class TableFormat(str, Enum):
    """Storage table format."""
    PARQUET = "parquet"
    DELTA = "delta"
    ICEBERG = "iceberg"


class DestinationModel(str, Enum):
    """Data model types for gold layer."""
    FLAT = "flat"
    DIMENSIONAL = "dimensional"
    DATA_VAULT = "data_vault"
    OBT = "obt"  # One Big Table


class TargetConfig(BaseModel):
    """
    Target configuration matching platform_pipeline_targets table.
    """
    model_config = ConfigDict(from_attributes=True)

    target_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)

    # Target zone
    target_zone: TargetZone = Field(default=TargetZone.GOLD)

    # BigQuery target
    bq_project: Optional[str] = Field(None)
    bq_dataset: str = Field(...)
    bq_table: str = Field(...)
    bq_location: str = Field(default="US")

    # Write configuration
    write_mode: WriteMode = Field(default=WriteMode.APPEND)
    merge_keys: List[str] = Field(default_factory=list)

    # Data model type (for gold layer)
    destination_model: DestinationModel = Field(default=DestinationModel.FLAT)

    # Data Vault specific (optional)
    hub_table: Optional[str] = Field(None)
    sat_table: Optional[str] = Field(None)
    link_tables: List[str] = Field(default_factory=list)

    # SCD Type 2 configuration
    scd_effective_from: Optional[str] = Field(None, description="Effective from column name")
    scd_effective_to: Optional[str] = Field(None, description="Effective to column name")
    scd_current_flag: Optional[str] = Field(None, description="Current record flag column")
    scd_tracked_columns: List[str] = Field(
        default_factory=list,
        description="Columns to track for SCD changes"
    )

    # Partitioning
    partition_field: Optional[str] = Field(None)
    partition_type: str = Field(default="DAY", description="DAY, MONTH, YEAR, HOUR")
    clustering_fields: List[str] = Field(default_factory=list, max_length=4)

    # Table format
    table_format: TableFormat = Field(
        default=TableFormat.DELTA,
        description="Storage format: parquet, delta, iceberg"
    )

    # Table maintenance configuration
    table_maintenance: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Maintenance config: vacuum_hours, optimize_schedule, z_order_columns, retention_days"
    )

    # Additional targets (multi-destination support)
    additional_targets: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Additional destinations like Snowflake, Delta Lake"
    )


class GCSPaths(BaseModel):
    """GCS paths for medallion architecture zones."""
    model_config = ConfigDict(extra="forbid")

    landing_path: str = Field(...)
    bronze_path: str = Field(...)
    silver_path: str = Field(...)
    gold_path: Optional[str] = Field(None)
    rejected_path: str = Field(...)

    @classmethod
    def from_dag_id(cls, dag_id: str, project_id: str) -> "GCSPaths":
        """Generate standard paths from dag_id."""
        base = f"gs://{project_id}"
        return cls(
            landing_path=f"{base}-raw-data/{dag_id}/",
            bronze_path=f"{base}-bronze/{dag_id}/",
            silver_path=f"{base}-silver/{dag_id}/",
            gold_path=f"{base}-gold/{dag_id}/",
            rejected_path=f"{base}-bronze/{dag_id}/_rejected/",
        )
