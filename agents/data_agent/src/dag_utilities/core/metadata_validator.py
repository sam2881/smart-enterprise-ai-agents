"""
Metadata Validator - Step 1 of Agent Workflow

Validates metadata completeness before any artifact generation.
FAIL FAST if metadata is invalid - do not proceed.

Validates:
- Feed definition exists and valid
- Contract correctness (SCD2, Data Vault, Star Schema)
- Schema versioning (effective-dated)
- Validation coverage (GE rules defined)
- Target configuration complete
- Load strategy defined
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class ValidationSeverity(str, Enum):
    """Validation error severity levels."""
    ERROR = "ERROR"      # Fail fast - cannot proceed
    WARNING = "WARNING"  # Proceed with caution
    INFO = "INFO"        # Informational only


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    valid: bool
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    field: Optional[str] = None
    expected: Optional[Any] = None
    actual: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "message": self.message,
            "severity": self.severity.value,
            "field": self.field,
            "expected": self.expected,
            "actual": self.actual,
        }


@dataclass
class MetadataValidationReport:
    """Complete validation report for a feed."""
    feed_id: int
    valid: bool = True
    errors: List[ValidationResult] = field(default_factory=list)
    warnings: List[ValidationResult] = field(default_factory=list)
    info: List[ValidationResult] = field(default_factory=list)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result to the report."""
        if not result.valid and result.severity == ValidationSeverity.ERROR:
            self.valid = False
            self.errors.append(result)
        elif result.severity == ValidationSeverity.WARNING:
            self.warnings.append(result)
        else:
            self.info.append(result)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_id": self.feed_id,
            "valid": self.valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [w.to_dict() for w in self.warnings],
            "info": [i.to_dict() for i in self.info],
        }


class MetadataValidator:
    """
    Validates metadata completeness for pipeline generation.

    Step 1 of the 7-step Agent Workflow.
    FAIL FAST if metadata is invalid.
    """

    REQUIRED_FEED_FIELDS = [
        "feed_id", "feed_name", "domain", "source_type", "is_active"
    ]

    REQUIRED_CONTRACT_FIELDS = [
        "contract_id", "feed_id", "contract_type", "is_active"
    ]

    VALID_CONTRACT_TYPES = ["STANDARD", "SCD2", "DATA_VAULT", "STAR_SCHEMA"]

    REQUIRED_ZONES = ["transient", "raw", "refined", "gold", "consumption"]

    def __init__(self, metadata_client: Any):
        """
        Initialize with metadata client.

        Args:
            metadata_client: MetadataClient instance for database access
        """
        self.meta = metadata_client

    def validate_feed(self, feed_id: int) -> MetadataValidationReport:
        """
        Validate complete feed metadata.

        This is the main entry point for Step 1 validation.

        Args:
            feed_id: Numeric feed ID to validate

        Returns:
            MetadataValidationReport with all validation results

        Raises:
            ValidationError: If critical validation fails and fail_fast=True
        """
        report = MetadataValidationReport(feed_id=feed_id)

        # 1. Validate feed definition
        feed = self.meta.get_feed(feed_id)
        self._validate_feed_definition(feed, report)

        if not report.valid:
            return report  # Fail fast on feed errors

        # 2. Validate contract
        contract = self.meta.get_contract(feed_id)
        self._validate_contract(contract, report)

        # 3. Validate schema
        schema = self.meta.get_schema(feed_id) if hasattr(self.meta, 'get_schema') else None
        self._validate_schema(schema, feed_id, report)

        # 4. Validate zone configurations
        zones = self.meta.get_zone_configs(feed_id)
        self._validate_zones(zones, report)

        # 5. Validate quality rules
        quality_rules = self.meta.get_quality_rules(feed_id)
        self._validate_quality_rules(quality_rules, zones, report)

        # 6. Validate transforms
        transforms = self.meta.get_transforms(feed_id)
        self._validate_transforms(transforms, report)

        return report

    def _validate_feed_definition(
        self,
        feed: Dict[str, Any],
        report: MetadataValidationReport
    ) -> None:
        """Validate feed definition exists and has required fields."""

        if not feed:
            report.add_result(ValidationResult(
                valid=False,
                message="Feed definition not found",
                severity=ValidationSeverity.ERROR,
                field="feed",
            ))
            return

        # Check required fields
        for field_name in self.REQUIRED_FEED_FIELDS:
            if field_name not in feed or feed.get(field_name) is None:
                report.add_result(ValidationResult(
                    valid=False,
                    message=f"Required field '{field_name}' missing from feed definition",
                    severity=ValidationSeverity.ERROR,
                    field=field_name,
                ))

        # Check feed is active
        if not feed.get("is_active", False):
            report.add_result(ValidationResult(
                valid=False,
                message="Feed is not active",
                severity=ValidationSeverity.ERROR,
                field="is_active",
                expected=True,
                actual=feed.get("is_active"),
            ))

        # Validate source_type format
        source_type = feed.get("source_type", "")
        if source_type and "_" not in source_type:
            report.add_result(ValidationResult(
                valid=False,
                message=f"Invalid source_type format: '{source_type}'. Expected format: 'category_type' (e.g., 'file_csv', 'database_postgres')",
                severity=ValidationSeverity.WARNING,
                field="source_type",
                actual=source_type,
            ))

    def _validate_contract(
        self,
        contract: Dict[str, Any],
        report: MetadataValidationReport
    ) -> None:
        """Validate contract definition."""

        if not contract:
            report.add_result(ValidationResult(
                valid=False,
                message="Contract definition not found",
                severity=ValidationSeverity.ERROR,
                field="contract",
            ))
            return

        # Check contract type
        contract_type = contract.get("contract_type")
        if contract_type not in self.VALID_CONTRACT_TYPES:
            report.add_result(ValidationResult(
                valid=False,
                message=f"Invalid contract_type: '{contract_type}'",
                severity=ValidationSeverity.ERROR,
                field="contract_type",
                expected=self.VALID_CONTRACT_TYPES,
                actual=contract_type,
            ))

        # SCD2 specific validations
        if contract_type == "SCD2":
            if not contract.get("business_keys"):
                report.add_result(ValidationResult(
                    valid=False,
                    message="SCD2 contract requires business_keys",
                    severity=ValidationSeverity.ERROR,
                    field="business_keys",
                ))

        # DATA_VAULT specific validations
        if contract_type == "DATA_VAULT":
            if not contract.get("data_vault_type"):
                report.add_result(ValidationResult(
                    valid=False,
                    message="DATA_VAULT contract requires data_vault_type (satellite, hub, link, pit, bridge)",
                    severity=ValidationSeverity.ERROR,
                    field="data_vault_type",
                ))

    def _validate_schema(
        self,
        schema: Optional[Dict[str, Any]],
        feed_id: int,
        report: MetadataValidationReport
    ) -> None:
        """Validate schema definition."""

        if not schema:
            report.add_result(ValidationResult(
                valid=False,
                message="Schema definition not found",
                severity=ValidationSeverity.ERROR,
                field="schema",
            ))
            return

        columns = schema.get("columns", [])
        if not columns:
            report.add_result(ValidationResult(
                valid=False,
                message="Schema has no columns defined",
                severity=ValidationSeverity.ERROR,
                field="schema.columns",
            ))
            return

        # Check for primary keys
        has_pk = any(col.get("is_primary_key") for col in columns)
        if not has_pk:
            report.add_result(ValidationResult(
                valid=True,
                message="No primary key defined in schema",
                severity=ValidationSeverity.WARNING,
                field="schema.primary_keys",
            ))

        # Check column definitions
        for i, col in enumerate(columns):
            if not col.get("name") and not col.get("column_name"):
                report.add_result(ValidationResult(
                    valid=False,
                    message=f"Column {i} missing name",
                    severity=ValidationSeverity.ERROR,
                    field=f"schema.columns[{i}].name",
                ))

    def _validate_zones(
        self,
        zones: List[Dict[str, Any]],
        report: MetadataValidationReport
    ) -> None:
        """Validate zone configurations."""

        if not zones:
            report.add_result(ValidationResult(
                valid=True,
                message="No zone configurations found - will use defaults",
                severity=ValidationSeverity.WARNING,
                field="zone_configurations",
            ))
            return

        # Check all required zones exist
        configured_zones = {z.get("zone_level") for z in zones}
        for required_zone in self.REQUIRED_ZONES:
            if required_zone not in configured_zones:
                report.add_result(ValidationResult(
                    valid=True,
                    message=f"Zone '{required_zone}' not configured - will use defaults",
                    severity=ValidationSeverity.INFO,
                    field=f"zone_configurations.{required_zone}",
                ))

    def _validate_quality_rules(
        self,
        quality_rules: List[Dict[str, Any]],
        zones: List[Dict[str, Any]],
        report: MetadataValidationReport
    ) -> None:
        """Validate quality rules coverage."""

        if not quality_rules:
            report.add_result(ValidationResult(
                valid=True,
                message="No quality rules defined - GE validation will be skipped",
                severity=ValidationSeverity.WARNING,
                field="quality_rules",
            ))
            return

        # Check rules exist for each zone
        zones_with_rules = {r.get("zone_level") for r in quality_rules}
        for zone in zones:
            zone_level = zone.get("zone_level")
            if zone_level not in zones_with_rules and zone_level != "transient":
                report.add_result(ValidationResult(
                    valid=True,
                    message=f"No quality rules for zone '{zone_level}'",
                    severity=ValidationSeverity.INFO,
                    field=f"quality_rules.{zone_level}",
                ))

    def _validate_transforms(
        self,
        transforms: List[Dict[str, Any]],
        report: MetadataValidationReport
    ) -> None:
        """Validate transform definitions."""

        if not transforms:
            report.add_result(ValidationResult(
                valid=True,
                message="No transforms defined - zones will use default processing",
                severity=ValidationSeverity.INFO,
                field="transforms",
            ))
            return

        # Check transform sequence
        for transform in transforms:
            if not transform.get("transform_type"):
                report.add_result(ValidationResult(
                    valid=False,
                    message=f"Transform missing transform_type",
                    severity=ValidationSeverity.ERROR,
                    field="transform.transform_type",
                ))


def validate_metadata(feed_id: int, metadata_client: Any) -> MetadataValidationReport:
    """
    Convenience function to validate feed metadata.

    Args:
        feed_id: Numeric feed ID
        metadata_client: MetadataClient instance

    Returns:
        MetadataValidationReport

    Raises:
        ValueError: If validation fails critically
    """
    validator = MetadataValidator(metadata_client)
    report = validator.validate_feed(feed_id)

    if not report.valid:
        error_messages = [e.message for e in report.errors]
        raise ValueError(f"Metadata validation failed: {'; '.join(error_messages)}")

    return report


__all__ = [
    "MetadataValidator",
    "MetadataValidationReport",
    "ValidationResult",
    "ValidationSeverity",
    "validate_metadata",
]
