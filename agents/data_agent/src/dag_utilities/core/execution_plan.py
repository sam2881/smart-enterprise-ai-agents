"""
Execution Plan Builder - Step 3 of Agent Workflow

Builds an IMMUTABLE execution plan that drives all downstream execution.
The plan includes checksum for deterministic verification.

Execution Plan Structure:
{
    "plan_id": "feed_1001_v3_20260205",
    "feed_id": 1001,
    "source_type": "file_csv",
    "source_category": "FILE",
    "zones": ["transient", "raw", "silver", "gold", "consumption"],
    "validations": {"schema": true, "semantic": true, "referential": true},
    "load_mode": "incremental",
    "delete_mode": "monthly",
    "contract_type": "DATA_VAULT",
    "data_vault_type": "satellite",
    "encryption": ["fpe", "fpe_hk", "fpe_col_concat_hk"],
    "target": "bigquery",
    "checksum": "a1b2c3d4e5f6"
}
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum


class LoadMode(str, Enum):
    """Data load modes."""
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    SNAPSHOT = "snapshot"


class DeleteMode(str, Enum):
    """Data deletion/retention modes."""
    NONE = "none"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class TargetType(str, Enum):
    """Target storage types."""
    BIGQUERY = "bigquery"
    GCS = "gcs"
    SNOWFLAKE = "snowflake"
    BOTH = "both"  # GCS + BigQuery


@dataclass
class ValidationConfig:
    """Validation configuration for a zone."""
    schema: bool = True
    semantic: bool = True
    referential: bool = False

    def to_dict(self) -> Dict[str, bool]:
        return asdict(self)


@dataclass
class ZonePlan:
    """Execution plan for a single zone."""
    zone_level: str
    enabled: bool = True
    source_path: str = ""
    target_path: str = ""
    write_mode: str = "append"
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    transforms: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "zone_level": self.zone_level,
            "enabled": self.enabled,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "write_mode": self.write_mode,
            "validation": self.validation.to_dict(),
            "transforms": self.transforms,
        }


@dataclass
class ExecutionPlan:
    """
    Immutable Execution Plan for a pipeline run.

    This plan is the SINGLE SOURCE OF TRUTH for all downstream execution.
    Same input metadata → Same plan (deterministic).
    """
    # Identity
    plan_id: str
    feed_id: int
    version: int = 1
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Source Configuration
    source_type: str = ""
    source_category: str = ""
    source_config: Dict[str, Any] = field(default_factory=dict)

    # Zone Configuration
    zones: List[ZonePlan] = field(default_factory=list)
    zone_levels: List[str] = field(default_factory=list)

    # Contract & Processing
    contract_type: str = "STANDARD"
    data_vault_type: Optional[str] = None
    load_mode: str = "incremental"
    delete_mode: str = "none"

    # Encryption
    encryption: List[str] = field(default_factory=list)
    encryption_config: Dict[str, Any] = field(default_factory=dict)

    # Target
    target: str = "bigquery"
    target_config: Dict[str, Any] = field(default_factory=dict)

    # Validation
    validations: Dict[str, bool] = field(default_factory=lambda: {
        "schema": True,
        "semantic": True,
        "referential": False,
    })

    # Quality Rules
    quality_rules_count: int = 0

    # Determinism
    checksum: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "plan_id": self.plan_id,
            "feed_id": self.feed_id,
            "version": self.version,
            "created_at": self.created_at,
            "source_type": self.source_type,
            "source_category": self.source_category,
            "source_config": self.source_config,
            "zones": [z.to_dict() for z in self.zones],
            "zone_levels": self.zone_levels,
            "contract_type": self.contract_type,
            "data_vault_type": self.data_vault_type,
            "load_mode": self.load_mode,
            "delete_mode": self.delete_mode,
            "encryption": self.encryption,
            "encryption_config": self.encryption_config,
            "target": self.target,
            "target_config": self.target_config,
            "validations": self.validations,
            "quality_rules_count": self.quality_rules_count,
            "checksum": self.checksum,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


class ExecutionPlanBuilder:
    """
    Builds immutable execution plans from metadata.

    Step 3 of the 7-step Agent Workflow.
    Produces deterministic plans with checksum verification.
    """

    ZONE_ORDER = ["transient", "raw", "silver", "gold", "consumption"]

    def __init__(self, metadata_client: Any):
        """
        Initialize with metadata client.

        Args:
            metadata_client: MetadataClient instance
        """
        self.meta = metadata_client

    def build(self, feed_id: int, batch_id: str = None) -> ExecutionPlan:
        """
        Build an immutable execution plan for a feed.

        Args:
            feed_id: Numeric feed ID
            batch_id: Optional batch identifier (defaults to today's date)

        Returns:
            ExecutionPlan with all configuration and checksum
        """
        batch_id = batch_id or datetime.now().strftime("%Y%m%d")

        # Fetch all metadata
        feed = self.meta.get_feed(feed_id)
        contract = self.meta.get_contract(feed_id)
        zones = self.meta.get_zone_configs(feed_id)
        transforms = self.meta.get_transforms(feed_id)
        quality_rules = self.meta.get_quality_rules(feed_id)
        encryption = self.meta.get_encryption_config(feed_id)

        # Build source configuration
        source_type = feed.get("source_type", "file_csv")
        source_category = self._get_source_category(source_type)

        # Build zone plans
        zone_plans = self._build_zone_plans(feed_id, zones, transforms, batch_id)
        zone_levels = [z.zone_level for z in zone_plans if z.enabled]

        # Build validation config
        validations = self._build_validations(quality_rules)

        # Build encryption config
        encryption_types = []
        encryption_config = {}
        if encryption:
            encryption_types = encryption.get("types", [])
            encryption_config = encryption

        # Generate plan ID
        version = feed.get("version", 1)
        plan_id = f"feed_{feed_id}_v{version}_{batch_id}"

        # Create plan
        plan = ExecutionPlan(
            plan_id=plan_id,
            feed_id=feed_id,
            version=version,
            source_type=source_type,
            source_category=source_category,
            source_config=feed.get("source_config", {}),
            zones=zone_plans,
            zone_levels=zone_levels,
            contract_type=contract.get("contract_type", "STANDARD"),
            data_vault_type=contract.get("data_vault_type"),
            load_mode=feed.get("load_mode", "incremental"),
            delete_mode=feed.get("delete_mode", "none"),
            encryption=encryption_types,
            encryption_config=encryption_config,
            target=feed.get("target", "bigquery"),
            target_config=feed.get("target_config", {}),
            validations=validations,
            quality_rules_count=len(quality_rules),
        )

        # Compute checksum for determinism verification
        plan.checksum = self._compute_checksum(plan)

        return plan

    def _get_source_category(self, source_type: str) -> str:
        """
        Get high-level source category from source type.

        Args:
            source_type: Granular source type (e.g., 'file_csv')

        Returns:
            Source category (e.g., 'FILE')
        """
        if not source_type or "_" not in source_type:
            return "FILE"

        prefix = source_type.split("_")[0].upper()

        category_mapping = {
            "FILE": "FILE",
            "DATABASE": "DATABASE",
            "STREAMING": "STREAMING",
            "API": "API",
            "LEGACY": "LEGACY",
            "NOSQL": "NOSQL",
            "LOGS": "LOGS",
            "CLOUD": "CLOUD",
            "SPECIAL": "SPECIAL",
        }

        return category_mapping.get(prefix, "FILE")

    def _build_zone_plans(
        self,
        feed_id: int,
        zones: List[Dict[str, Any]],
        transforms: List[Dict[str, Any]],
        batch_id: str
    ) -> List[ZonePlan]:
        """Build zone plans from metadata."""
        zone_plans = []

        # Create a map of configured zones
        zone_map = {z.get("zone_level"): z for z in zones}
        transform_map = {}
        for t in transforms:
            zone = t.get("zone_level", "silver")
            if zone not in transform_map:
                transform_map[zone] = []
            transform_map[zone].append(t.get("transform_type", "unknown"))

        for zone_level in self.ZONE_ORDER:
            config = zone_map.get(zone_level, {})

            # Default validation config based on zone
            validation = ValidationConfig(
                schema=(zone_level != "transient"),
                semantic=(zone_level in ["silver", "gold", "consumption"]),
                referential=(zone_level in ["gold", "consumption"]),
            )

            zone_plan = ZonePlan(
                zone_level=zone_level,
                enabled=config.get("is_active", True),
                source_path=config.get("source_path", f"gs://apex-{zone_level}/feed_{feed_id}/{batch_id}/"),
                target_path=config.get("target_path", f"gs://apex-{zone_level}/feed_{feed_id}/{batch_id}/"),
                write_mode=config.get("write_mode", "append" if zone_level in ["transient", "raw"] else "merge"),
                validation=validation,
                transforms=transform_map.get(zone_level, []),
            )
            zone_plans.append(zone_plan)

        return zone_plans

    def _build_validations(self, quality_rules: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Build validation configuration from quality rules."""
        validations = {
            "schema": True,
            "semantic": False,
            "referential": False,
        }

        for rule in quality_rules:
            rule_type = rule.get("rule_type", "")
            if rule_type in ["not_null", "type_check", "column_exists"]:
                validations["schema"] = True
            elif rule_type in ["range", "regex", "in_set", "unique"]:
                validations["semantic"] = True
            elif rule_type in ["referential", "foreign_key"]:
                validations["referential"] = True

        return validations

    def _compute_checksum(self, plan: ExecutionPlan) -> str:
        """
        Compute deterministic checksum for the execution plan.

        Same input → Same checksum (always).

        Args:
            plan: ExecutionPlan to checksum

        Returns:
            16-character hex checksum
        """
        # Create a deterministic representation
        # Exclude created_at and checksum itself
        plan_dict = plan.to_dict()
        plan_dict.pop("created_at", None)
        plan_dict.pop("checksum", None)

        # Sort keys for determinism
        content = json.dumps(plan_dict, sort_keys=True, default=str)

        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def verify_checksum(self, plan: ExecutionPlan) -> bool:
        """
        Verify that a plan's checksum is valid.

        Args:
            plan: ExecutionPlan to verify

        Returns:
            True if checksum is valid, False otherwise
        """
        expected = self._compute_checksum(plan)
        return plan.checksum == expected


def build_execution_plan(
    feed_id: int,
    metadata_client: Any,
    batch_id: str = None
) -> ExecutionPlan:
    """
    Convenience function to build an execution plan.

    Args:
        feed_id: Numeric feed ID
        metadata_client: MetadataClient instance
        batch_id: Optional batch identifier

    Returns:
        ExecutionPlan
    """
    builder = ExecutionPlanBuilder(metadata_client)
    return builder.build(feed_id, batch_id)


__all__ = [
    "ExecutionPlan",
    "ExecutionPlanBuilder",
    "ZonePlan",
    "ValidationConfig",
    "LoadMode",
    "DeleteMode",
    "TargetType",
    "build_execution_plan",
]
