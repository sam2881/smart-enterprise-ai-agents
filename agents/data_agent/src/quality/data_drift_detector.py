"""
Data Drift Detection Module

Detects schema drift, statistical drift, volume drift, and freshness drift.
Enterprise-grade data quality and observability feature.
"""

from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import StructType


class DriftType(str, Enum):
    """Types of data drift."""
    SCHEMA_DRIFT = "schema_drift"  # Column added/removed/type changed
    STATISTICAL_DRIFT = "statistical_drift"  # Distribution changes
    VOLUME_DRIFT = "volume_drift"  # Row count changes
    FRESHNESS_DRIFT = "freshness_drift"  # Data not arriving on time


class DriftSeverity(str, Enum):
    """Drift severity levels."""
    INFO = "info"  # Minor change, informational
    WARNING = "warning"  # Noticeable change, monitor
    CRITICAL = "critical"  # Significant change, investigate


@dataclass
class SchemaDrift:
    """Schema drift detection result."""
    drift_type: DriftType = DriftType.SCHEMA_DRIFT
    severity: DriftSeverity = DriftSeverity.INFO
    columns_added: List[str] = None
    columns_removed: List[str] = None
    columns_type_changed: Dict[str, Tuple[str, str]] = None  # {col: (old_type, new_type)}
    detected_at: datetime = None
    message: str = ""

    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()
        if not self.columns_added:
            self.columns_added = []
        if not self.columns_removed:
            self.columns_removed = []
        if not self.columns_type_changed:
            self.columns_type_changed = {}


@dataclass
class StatisticalDrift:
    """Statistical drift detection result."""
    drift_type: DriftType = DriftType.STATISTICAL_DRIFT
    severity: DriftSeverity = DriftSeverity.INFO
    column: str = ""
    metric: str = ""  # mean, stddev, min, max, distinct_count
    baseline_value: float = 0.0
    current_value: float = 0.0
    change_percentage: float = 0.0
    threshold_exceeded: bool = False
    detected_at: datetime = None
    message: str = ""

    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()


@dataclass
class VolumeDrift:
    """Volume drift detection result."""
    drift_type: DriftType = DriftType.VOLUME_DRIFT
    severity: DriftSeverity = DriftSeverity.INFO
    baseline_row_count: int = 0
    current_row_count: int = 0
    change_percentage: float = 0.0
    threshold_exceeded: bool = False
    detected_at: datetime = None
    message: str = ""

    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()


@dataclass
class FreshnessDrift:
    """Freshness drift detection result."""
    drift_type: DriftType = DriftType.FRESHNESS_DRIFT
    severity: DriftSeverity = DriftSeverity.INFO
    expected_arrival_time: datetime = None
    actual_arrival_time: datetime = None
    delay_minutes: float = 0.0
    threshold_exceeded: bool = False
    detected_at: datetime = None
    message: str = ""

    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()


class DataDriftDetector:
    """Detects various types of data drift."""

    def __init__(
        self,
        statistical_threshold: float = 0.20,  # 20% change
        volume_threshold: float = 0.30,  # 30% change
        freshness_threshold_minutes: int = 60  # 60 minutes late
    ):
        """
        Initialize drift detector.

        Args:
            statistical_threshold: Percentage change to flag statistical drift (0.0-1.0)
            volume_threshold: Percentage change to flag volume drift (0.0-1.0)
            freshness_threshold_minutes: Minutes late to flag freshness drift
        """
        self.statistical_threshold = statistical_threshold
        self.volume_threshold = volume_threshold
        self.freshness_threshold_minutes = freshness_threshold_minutes

    def detect_schema_drift(
        self,
        baseline_schema: StructType,
        current_schema: StructType
    ) -> Optional[SchemaDrift]:
        """
        Detect schema drift between baseline and current DataFrame.

        Args:
            baseline_schema: Expected schema
            current_schema: Current schema

        Returns:
            SchemaDrift if drift detected, None otherwise
        """
        baseline_fields = {f.name: f.dataType.simpleString() for f in baseline_schema.fields}
        current_fields = {f.name: f.dataType.simpleString() for f in current_schema.fields}

        # Detect added columns
        columns_added = [col for col in current_fields if col not in baseline_fields]

        # Detect removed columns
        columns_removed = [col for col in baseline_fields if col not in current_fields]

        # Detect type changes
        columns_type_changed = {}
        for col in set(baseline_fields.keys()) & set(current_fields.keys()):
            if baseline_fields[col] != current_fields[col]:
                columns_type_changed[col] = (baseline_fields[col], current_fields[col])

        # Determine severity
        severity = DriftSeverity.INFO
        if columns_removed or columns_type_changed:
            severity = DriftSeverity.CRITICAL
        elif len(columns_added) > 5:
            severity = DriftSeverity.WARNING

        if columns_added or columns_removed or columns_type_changed:
            message = f"Schema drift detected: {len(columns_added)} added, {len(columns_removed)} removed, {len(columns_type_changed)} type changed"
            return SchemaDrift(
                severity=severity,
                columns_added=columns_added,
                columns_removed=columns_removed,
                columns_type_changed=columns_type_changed,
                message=message
            )

        return None

    def detect_statistical_drift(
        self,
        baseline_stats: Dict[str, Dict[str, float]],
        current_df: DataFrame
    ) -> List[StatisticalDrift]:
        """
        Detect statistical drift in numeric columns.

        Args:
            baseline_stats: Baseline statistics {column: {metric: value}}
            current_df: Current DataFrame

        Returns:
            List of statistical drift results
        """
        drifts = []

        # Calculate current statistics
        numeric_columns = [
            f.name for f in current_df.schema.fields
            if f.dataType.simpleString() in ['int', 'bigint', 'float', 'double', 'decimal']
        ]

        if not numeric_columns:
            return drifts

        # Aggregate statistics
        agg_exprs = []
        for col_name in numeric_columns:
            agg_exprs.extend([
                avg(col(col_name)).alias(f"{col_name}_mean"),
                stddev(col(col_name)).alias(f"{col_name}_stddev"),
                spark_min(col(col_name)).alias(f"{col_name}_min"),
                spark_max(col(col_name)).alias(f"{col_name}_max"),
                countDistinct(col(col_name)).alias(f"{col_name}_distinct")
            ])

        current_stats_row = current_df.agg(*agg_exprs).collect()[0]
        current_stats = current_stats_row.asDict()

        # Compare with baseline
        for col_name in numeric_columns:
            if col_name not in baseline_stats:
                continue

            for metric in ['mean', 'stddev', 'min', 'max', 'distinct']:
                metric_key = f"{col_name}_{metric}"
                baseline_value = baseline_stats[col_name].get(metric, 0)
                current_value = current_stats.get(metric_key, 0)

                if baseline_value == 0:
                    continue

                change_pct = abs((current_value - baseline_value) / baseline_value)

                if change_pct > self.statistical_threshold:
                    severity = DriftSeverity.WARNING if change_pct < 0.5 else DriftSeverity.CRITICAL
                    message = f"Statistical drift in {col_name}.{metric}: {baseline_value:.2f} -> {current_value:.2f} ({change_pct*100:.1f}% change)"

                    drifts.append(StatisticalDrift(
                        severity=severity,
                        column=col_name,
                        metric=metric,
                        baseline_value=float(baseline_value),
                        current_value=float(current_value),
                        change_percentage=float(change_pct * 100),
                        threshold_exceeded=True,
                        message=message
                    ))

        return drifts

    def detect_volume_drift(
        self,
        baseline_row_count: int,
        current_df: DataFrame
    ) -> Optional[VolumeDrift]:
        """
        Detect volume drift (row count changes).

        Args:
            baseline_row_count: Expected row count
            current_df: Current DataFrame

        Returns:
            VolumeDrift if drift detected, None otherwise
        """
        current_row_count = current_df.count()

        if baseline_row_count == 0:
            return None

        change_pct = abs((current_row_count - baseline_row_count) / baseline_row_count)

        if change_pct > self.volume_threshold:
            severity = DriftSeverity.WARNING if change_pct < 0.5 else DriftSeverity.CRITICAL
            message = f"Volume drift detected: {baseline_row_count} -> {current_row_count} rows ({change_pct*100:.1f}% change)"

            return VolumeDrift(
                severity=severity,
                baseline_row_count=baseline_row_count,
                current_row_count=current_row_count,
                change_percentage=float(change_pct * 100),
                threshold_exceeded=True,
                message=message
            )

        return None

    def detect_freshness_drift(
        self,
        expected_arrival_time: datetime,
        actual_arrival_time: Optional[datetime] = None
    ) -> Optional[FreshnessDrift]:
        """
        Detect freshness drift (data arriving late).

        Args:
            expected_arrival_time: When data was expected
            actual_arrival_time: When data actually arrived (None = not arrived yet)

        Returns:
            FreshnessDrift if drift detected, None otherwise
        """
        if actual_arrival_time is None:
            actual_arrival_time = datetime.utcnow()

        delay = actual_arrival_time - expected_arrival_time
        delay_minutes = delay.total_seconds() / 60

        if delay_minutes > self.freshness_threshold_minutes:
            severity = DriftSeverity.WARNING if delay_minutes < 180 else DriftSeverity.CRITICAL
            message = f"Freshness drift detected: Data arrived {delay_minutes:.1f} minutes late (expected: {expected_arrival_time}, actual: {actual_arrival_time})"

            return FreshnessDrift(
                severity=severity,
                expected_arrival_time=expected_arrival_time,
                actual_arrival_time=actual_arrival_time,
                delay_minutes=float(delay_minutes),
                threshold_exceeded=True,
                message=message
            )

        return None

    def compute_baseline_stats(self, df: DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Compute baseline statistics for a DataFrame.

        Args:
            df: DataFrame to profile

        Returns:
            Dictionary of statistics {column: {metric: value}}
        """
        numeric_columns = [
            f.name for f in df.schema.fields
            if f.dataType.simpleString() in ['int', 'bigint', 'float', 'double', 'decimal']
        ]

        if not numeric_columns:
            return {}

        # Aggregate statistics
        agg_exprs = []
        for col_name in numeric_columns:
            agg_exprs.extend([
                avg(col(col_name)).alias(f"{col_name}_mean"),
                stddev(col(col_name)).alias(f"{col_name}_stddev"),
                spark_min(col(col_name)).alias(f"{col_name}_min"),
                spark_max(col(col_name)).alias(f"{col_name}_max"),
                countDistinct(col(col_name)).alias(f"{col_name}_distinct")
            ])

        stats_row = df.agg(*agg_exprs).collect()[0]
        stats_dict = stats_row.asDict()

        # Reorganize into {column: {metric: value}}
        baseline = {}
        for col_name in numeric_columns:
            baseline[col_name] = {
                'mean': float(stats_dict.get(f"{col_name}_mean", 0) or 0),
                'stddev': float(stats_dict.get(f"{col_name}_stddev", 0) or 0),
                'min': float(stats_dict.get(f"{col_name}_min", 0) or 0),
                'max': float(stats_dict.get(f"{col_name}_max", 0) or 0),
                'distinct': int(stats_dict.get(f"{col_name}_distinct", 0) or 0)
            }

        return baseline


def detect_all_drifts(
    baseline_schema: StructType,
    baseline_stats: Dict[str, Dict[str, float]],
    baseline_row_count: int,
    current_df: DataFrame,
    expected_arrival_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Convenience function to detect all drift types.

    Args:
        baseline_schema: Expected schema
        baseline_stats: Baseline statistics
        baseline_row_count: Expected row count
        current_df: Current DataFrame
        expected_arrival_time: When data was expected (for freshness)

    Returns:
        Dictionary with all drift results
    """
    detector = DataDriftDetector()

    results = {
        'schema_drift': None,
        'statistical_drifts': [],
        'volume_drift': None,
        'freshness_drift': None,
        'has_critical_drift': False
    }

    # Schema drift
    schema_drift = detector.detect_schema_drift(baseline_schema, current_df.schema)
    if schema_drift:
        results['schema_drift'] = schema_drift
        if schema_drift.severity == DriftSeverity.CRITICAL:
            results['has_critical_drift'] = True

    # Statistical drift
    statistical_drifts = detector.detect_statistical_drift(baseline_stats, current_df)
    results['statistical_drifts'] = statistical_drifts
    if any(d.severity == DriftSeverity.CRITICAL for d in statistical_drifts):
        results['has_critical_drift'] = True

    # Volume drift
    volume_drift = detector.detect_volume_drift(baseline_row_count, current_df)
    if volume_drift:
        results['volume_drift'] = volume_drift
        if volume_drift.severity == DriftSeverity.CRITICAL:
            results['has_critical_drift'] = True

    # Freshness drift
    if expected_arrival_time:
        freshness_drift = detector.detect_freshness_drift(expected_arrival_time)
        if freshness_drift:
            results['freshness_drift'] = freshness_drift
            if freshness_drift.severity == DriftSeverity.CRITICAL:
                results['has_critical_drift'] = True

    return results


def persist_observability_metrics(
    pg_connection_string: str,
    feed_id: str,
    execution_id: str,
    execution_date: str,
    zone_level: str,
    row_count: int,
    quality_score: Optional[float] = None,
    column_stats: Optional[Dict[str, Any]] = None,
    drift_results: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Persist observability metrics to PostgreSQL for baseline computation.

    Called after each zone job completes to build historical baselines.
    """
    try:
        import psycopg2
        from psycopg2.extras import Json

        conn = psycopg2.connect(pg_connection_string)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO observability_metrics (
                    feed_id, execution_id, execution_date, zone_level,
                    row_count, quality_score, column_stats,
                    schema_drift, volume_drift, freshness_drift, statistical_drift
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                feed_id,
                execution_id,
                execution_date,
                zone_level,
                row_count,
                quality_score,
                Json(column_stats) if column_stats else None,
                bool(drift_results and drift_results.get("schema_drift")),
                bool(drift_results and drift_results.get("volume_drift")),
                bool(drift_results and drift_results.get("freshness_drift")),
                bool(drift_results and drift_results.get("statistical_drifts")),
            ])
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: Failed to persist observability metrics: {e}")
