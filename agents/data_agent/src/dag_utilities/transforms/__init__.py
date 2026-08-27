"""
APEX Transform Dispatch Module

Maps transform_type → PySpark transformation function.

The TransformDispatcher reads transform_definitions from metadata and applies
them to DataFrames in sequence. Transform behavior changes ONLY via metadata,
never by editing code.

Supported transform types:
- deduplicate: Remove duplicate rows by key columns
- null_fill: Fill null values with default
- null_drop: Drop rows with null values
- rename: Rename columns
- cast: Cast column type
- expression: Apply SQL expression
- window: Window function (sum, avg, count, row_number)
- aggregate: Group by + aggregation
- hash: Generate hash key from columns
- filter: Filter rows by condition
- join: Join with reference data (from metadata)
"""

from .transform_dispatcher import TransformDispatcher

__all__ = [
    "TransformDispatcher",
]
