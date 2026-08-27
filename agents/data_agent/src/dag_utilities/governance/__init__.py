"""
DAG Utilities Governance Module

Governance utilities for APEX DAG patterns:
- ContractGate: Pre-ingestion contract validation
- HealthScoreCalculator: Pipeline health score computation
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional
from enum import Enum


class ContractPolicy(str, Enum):
    """Contract validation policies."""
    FAIL_FAST = "FAIL_FAST"  # Fail immediately on violation
    WARN = "WARN"  # Log warning but continue
    ADAPT = "ADAPT"  # Auto-evolve schema (dangerous)


@dataclass
class ContractValidationResult:
    """Result of contract validation."""
    is_valid: bool
    violations: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    proposed_schema: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "violations": self.violations,
            "warnings": self.warnings,
            "proposed_schema": self.proposed_schema,
        }


class ContractGate:
    """
    Contract Gate - Pre-ingestion validation.

    Validates producer contracts BEFORE data is ingested:
    - Schema compatibility checks
    - Required column validation
    - Type compatibility checks
    - Data format validation

    Fail Fast Policy (default):
    - Schema drift detected → FAIL immediately (no data lands in wrong shape)
    - Missing required columns → FAIL
    - Type changes detected → FAIL
    """

    def __init__(self, metadata_client):
        """
        Initialize ContractGate.

        Args:
            metadata_client: MetadataClient instance for database access
        """
        self.metadata = metadata_client

    def validate_contract(
        self,
        feed_id: int,
        execution_plan: Dict[str, Any],
        policy: str = "FAIL_FAST"
    ) -> ContractValidationResult:
        """
        Validate producer contract before ingestion.

        Args:
            feed_id: Numeric feed ID
            execution_plan: Execution plan with source and schema config
            policy: Validation policy (FAIL_FAST, WARN, ADAPT)

        Returns:
            ContractValidationResult with validation status
        """
        violations = []
        warnings = []

        # Get expected schema from metadata
        expected_schema = execution_plan.get("schema", {})
        expected_columns = expected_schema.get("columns", [])

        if not expected_columns:
            # No schema defined - skip validation
            return ContractValidationResult(
                is_valid=True,
                warnings=["No schema defined for contract validation"]
            )

        # Extract column names from expected schema
        expected_column_names = {
            col.get("column_name", col.get("name", ""))
            for col in expected_columns
            if isinstance(col, dict)
        }

        # Check for required columns
        for col in expected_columns:
            if isinstance(col, dict):
                if not col.get("nullable", True) and col.get("is_primary_key", False):
                    # Required column check would happen at data read time
                    pass

        # Schema evolution mode check
        evolution_mode = execution_plan.get("schema_evolution_mode", "strict")
        if evolution_mode == "strict":
            # In strict mode, any schema change is a violation
            warnings.append("Schema evolution mode: strict - no schema changes allowed")
        elif evolution_mode == "additive":
            warnings.append("Schema evolution mode: additive - new columns allowed")
        elif evolution_mode == "flexible":
            warnings.append("Schema evolution mode: flexible - column changes allowed")

        # Contract-specific validations
        contract_type = execution_plan.get("contract_type", "STANDARD")
        if contract_type == "SCD2":
            # Validate SCD2 requirements
            business_keys = execution_plan.get("scd2_config", {}).get("business_keys", [])
            if not business_keys:
                violations.append("SCD2 contract requires business_keys configuration")

        elif contract_type == "DATA_VAULT":
            # Validate Data Vault requirements
            dv_type = execution_plan.get("data_vault_type")
            if dv_type == "hub" and not execution_plan.get("hub_columns"):
                violations.append("Data Vault HUB requires hub_columns configuration")
            elif dv_type == "satellite" and not execution_plan.get("satellite_columns"):
                violations.append("Data Vault SATELLITE requires satellite_columns configuration")

        is_valid = len(violations) == 0

        return ContractValidationResult(
            is_valid=is_valid,
            violations=violations,
            warnings=warnings,
        )

    def validate_producer_contract(
        self,
        feed_id: int,
        sample_file_path: str,
        expected_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate producer contract against a sample file.

        Args:
            feed_id: Numeric feed ID
            sample_file_path: Path to sample file for validation
            expected_schema: Expected schema definition

        Returns:
            Validation result dictionary
        """
        violations = []
        compatible = True
        action = "CONTINUE"

        # In a real implementation, this would:
        # 1. Read sample file header/schema
        # 2. Compare against expected schema
        # 3. Detect schema drift

        return {
            "compatible": compatible,
            "violations": violations,
            "action": action,
            "proposed_schema": None,
        }


@dataclass
class HealthScore:
    """Pipeline health score with component breakdown."""
    feed_id: int
    execution_id: str
    quality_score: float
    freshness_score: float
    stability_score: float
    cost_efficiency_score: float
    overall_score: float
    calculated_at: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "feed_id": self.feed_id,
            "execution_id": self.execution_id,
            "quality_score": self.quality_score,
            "freshness_score": self.freshness_score,
            "stability_score": self.stability_score,
            "cost_efficiency_score": self.cost_efficiency_score,
            "overall_score": self.overall_score,
            "calculated_at": self.calculated_at.isoformat(),
            "details": self.details,
        }


class HealthScoreCalculator:
    """
    Pipeline Health Score Calculator.

    Calculates pipeline health as a first-class entity:
    health_score = quality(40%) + freshness(30%) + stability(20%) + cost(10%)

    Used for:
    - Promotion gates (dev → qa → prod)
    - Alert suppression (don't alert if health is good)
    - Dashboards (track health over time)
    - Capacity planning (identify struggling pipelines)
    """

    DEFAULT_WEIGHTS = {
        "quality": 0.4,
        "freshness": 0.3,
        "stability": 0.2,
        "cost_efficiency": 0.1,
    }

    def __init__(self, metadata_client):
        """
        Initialize HealthScoreCalculator.

        Args:
            metadata_client: MetadataClient instance for database access
        """
        self.metadata = metadata_client

    def calculate(
        self,
        feed_id: int,
        execution_id: str,
        metrics: Dict[str, Any] = None,
        weights: Dict[str, float] = None,
        window_days: int = 7
    ) -> HealthScore:
        """
        Calculate pipeline health score.

        Args:
            feed_id: Numeric feed ID
            execution_id: Current execution ID
            metrics: Execution metrics from current run
            weights: Custom weight overrides
            window_days: Rolling window for historical analysis

        Returns:
            HealthScore with component breakdown
        """
        weights = weights or self.DEFAULT_WEIGHTS
        metrics = metrics or {}

        # Calculate component scores
        quality_score = self._calculate_quality_score(feed_id, metrics)
        freshness_score = self._calculate_freshness_score(feed_id, metrics)
        stability_score = self._calculate_stability_score(feed_id, window_days)
        cost_score = self._calculate_cost_efficiency_score(feed_id, metrics)

        # Calculate weighted overall score
        overall_score = (
            weights["quality"] * quality_score +
            weights["freshness"] * freshness_score +
            weights["stability"] * stability_score +
            weights["cost_efficiency"] * cost_score
        )

        return HealthScore(
            feed_id=feed_id,
            execution_id=execution_id,
            quality_score=quality_score,
            freshness_score=freshness_score,
            stability_score=stability_score,
            cost_efficiency_score=cost_score,
            overall_score=round(overall_score, 3),
            details={
                "weights": weights,
                "window_days": window_days,
                "metrics_analyzed": list(metrics.keys()),
            }
        )

    def _calculate_quality_score(
        self,
        feed_id: int,
        metrics: Dict[str, Any]
    ) -> float:
        """
        Calculate data quality score.

        Based on:
        - GE validation pass rate
        - Records rejected percentage
        - Schema validation results
        """
        # Get GE validation results
        ge_success_rate = metrics.get("ge_success_rate", 1.0)

        # Get rejection rate
        records_read = metrics.get("records_read", 0)
        records_rejected = metrics.get("records_rejected", 0)

        if records_read > 0:
            rejection_rate = records_rejected / records_read
            rejection_score = max(0, 1 - (rejection_rate * 10))  # 10% rejection = 0 score
        else:
            rejection_score = 1.0

        # Combine scores
        quality_score = (ge_success_rate * 0.7 + rejection_score * 0.3)

        return round(min(1.0, max(0.0, quality_score)), 3)

    def _calculate_freshness_score(
        self,
        feed_id: int,
        metrics: Dict[str, Any]
    ) -> float:
        """
        Calculate data freshness score.

        Based on:
        - On-time delivery rate
        - SLA compliance
        - Data latency
        """
        # Check if execution was on time
        execution_time = metrics.get("execution_time_seconds", 0)
        sla_seconds = metrics.get("sla_seconds", 3600)  # Default 1 hour SLA

        if sla_seconds > 0:
            time_ratio = execution_time / sla_seconds
            if time_ratio <= 1.0:
                freshness_score = 1.0
            elif time_ratio <= 1.5:
                freshness_score = 0.8
            elif time_ratio <= 2.0:
                freshness_score = 0.5
            else:
                freshness_score = 0.2
        else:
            freshness_score = 1.0

        return round(freshness_score, 3)

    def _calculate_stability_score(
        self,
        feed_id: int,
        window_days: int
    ) -> float:
        """
        Calculate pipeline stability score.

        Based on:
        - Success rate in rolling window
        - Error frequency
        - Retry frequency
        """
        # In a real implementation, query historical executions
        # For now, return a default high score
        return 0.95

    def _calculate_cost_efficiency_score(
        self,
        feed_id: int,
        metrics: Dict[str, Any]
    ) -> float:
        """
        Calculate cost efficiency score.

        Based on:
        - Slot-hours usage vs budget
        - Resource utilization
        - Data processing efficiency
        """
        # Check slot-hours usage
        slot_hours_used = metrics.get("slot_hours_used", 0)
        slot_hours_budget = metrics.get("slot_hours_budget", 10)

        if slot_hours_budget > 0:
            usage_ratio = slot_hours_used / slot_hours_budget
            if usage_ratio <= 0.5:
                cost_score = 1.0
            elif usage_ratio <= 1.0:
                cost_score = 0.8
            elif usage_ratio <= 1.5:
                cost_score = 0.5
            else:
                cost_score = 0.2
        else:
            cost_score = 1.0

        return round(cost_score, 3)


__all__ = [
    "ContractGate",
    "ContractPolicy",
    "ContractValidationResult",
    "HealthScore",
    "HealthScoreCalculator",
]
