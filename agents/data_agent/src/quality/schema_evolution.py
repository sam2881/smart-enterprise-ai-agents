"""
APEX Data Agent - Schema Evolution Policy Enforcement

Compares incoming data schema against registered schema in metadata.
Supports three policies:
  - STRICT:   Fail on any schema change (added, removed, or type-changed columns)
  - ADDITIVE: Allow new columns, fail on removed or type-changed columns
  - FLEXIBLE: Allow all changes, log warnings

Called from raw_to_bronze.py after reading source data, before writing to Bronze.
"""

import structlog
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = structlog.get_logger(__name__)


class SchemaEvolutionPolicy(str, Enum):
    STRICT = "STRICT"
    ADDITIVE = "ADDITIVE"
    FLEXIBLE = "FLEXIBLE"


@dataclass
class SchemaChange:
    """A single schema change detected."""
    change_type: str  # "ADDED", "REMOVED", "TYPE_CHANGED"
    column_name: str
    old_type: Optional[str] = None
    new_type: Optional[str] = None


@dataclass
class SchemaEvolutionResult:
    """Result of schema evolution validation."""
    is_valid: bool
    policy: SchemaEvolutionPolicy
    changes: List[SchemaChange] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0


def detect_schema_changes(
    registered_columns: Dict[str, str],
    incoming_columns: Dict[str, str],
) -> List[SchemaChange]:
    """
    Compare registered schema against incoming data schema.

    Args:
        registered_columns: {"column_name": "data_type"} from metadata
        incoming_columns: {"column_name": "data_type"} from incoming DataFrame

    Returns:
        List of detected changes
    """
    changes = []
    registered_set = set(registered_columns.keys())
    incoming_set = set(incoming_columns.keys())

    # New columns (in incoming but not in registered)
    for col in sorted(incoming_set - registered_set):
        # Skip system/audit columns
        if col.startswith("_"):
            continue
        changes.append(SchemaChange(
            change_type="ADDED",
            column_name=col,
            new_type=incoming_columns[col],
        ))

    # Removed columns (in registered but not in incoming)
    for col in sorted(registered_set - incoming_set):
        if col.startswith("_"):
            continue
        changes.append(SchemaChange(
            change_type="REMOVED",
            column_name=col,
            old_type=registered_columns[col],
        ))

    # Type changes (in both but different types)
    for col in sorted(registered_set & incoming_set):
        if col.startswith("_"):
            continue
        reg_type = _normalize_type(registered_columns[col])
        inc_type = _normalize_type(incoming_columns[col])
        if reg_type != inc_type:
            changes.append(SchemaChange(
                change_type="TYPE_CHANGED",
                column_name=col,
                old_type=registered_columns[col],
                new_type=incoming_columns[col],
            ))

    return changes


def _normalize_type(dtype: str) -> str:
    """Normalize data type strings for comparison."""
    dtype = dtype.lower().strip()
    # Map common aliases
    type_map = {
        "int": "integer",
        "bigint": "long",
        "str": "string",
        "varchar": "string",
        "text": "string",
        "bool": "boolean",
        "float": "double",
        "numeric": "decimal",
        "datetime": "timestamp",
    }
    return type_map.get(dtype, dtype)


def validate_schema_evolution(
    registered_columns: Dict[str, str],
    incoming_columns: Dict[str, str],
    policy: str = "ADDITIVE",
) -> SchemaEvolutionResult:
    """
    Validate schema changes against the configured policy.

    Args:
        registered_columns: Schema from metadata
        incoming_columns: Schema from incoming data
        policy: STRICT, ADDITIVE, or FLEXIBLE

    Returns:
        SchemaEvolutionResult with validity and details
    """
    try:
        policy_enum = SchemaEvolutionPolicy(policy.upper())
    except ValueError:
        policy_enum = SchemaEvolutionPolicy.ADDITIVE

    changes = detect_schema_changes(registered_columns, incoming_columns)

    result = SchemaEvolutionResult(
        is_valid=True,
        policy=policy_enum,
        changes=changes,
    )

    if not changes:
        return result

    for change in changes:
        if policy_enum == SchemaEvolutionPolicy.STRICT:
            # Any change is an error
            result.is_valid = False
            result.errors.append(
                f"Schema change not allowed (STRICT): {change.change_type} "
                f"column '{change.column_name}'"
                + (f" (was {change.old_type}, now {change.new_type})"
                   if change.change_type == "TYPE_CHANGED" else "")
            )

        elif policy_enum == SchemaEvolutionPolicy.ADDITIVE:
            if change.change_type == "ADDED":
                result.warnings.append(
                    f"New column detected: '{change.column_name}' ({change.new_type})"
                )
            else:
                # REMOVED or TYPE_CHANGED are errors
                result.is_valid = False
                result.errors.append(
                    f"Schema change not allowed (ADDITIVE): {change.change_type} "
                    f"column '{change.column_name}'"
                    + (f" (was {change.old_type}, now {change.new_type})"
                       if change.change_type == "TYPE_CHANGED" else "")
                )

        elif policy_enum == SchemaEvolutionPolicy.FLEXIBLE:
            result.warnings.append(
                f"Schema change detected: {change.change_type} "
                f"column '{change.column_name}'"
            )

    # Log results
    log = logger.bind(policy=policy_enum.value, num_changes=len(changes))
    if result.is_valid:
        if result.warnings:
            log.warning("schema_evolution_warnings", warnings=result.warnings)
        else:
            log.info("schema_evolution_no_changes")
    else:
        log.error("schema_evolution_failed", errors=result.errors)

    return result
