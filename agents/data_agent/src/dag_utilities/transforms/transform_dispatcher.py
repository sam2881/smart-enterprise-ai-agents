"""
Transform Dispatcher - Maps transform_type → PySpark function

DATA PLANE component - executes transforms, never makes decisions.
All transform configurations come from metadata (transform_definitions table).

Usage:
    dispatcher = TransformDispatcher(metadata_client)
    for transform in transforms:
        df = dispatcher.apply(df, transform)
"""

from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class TransformDispatcher:
    """
    Maps transform_type → PySpark transformation function.

    Supported types:
    - deduplicate, null_fill, null_drop, rename, cast
    - expression, window, aggregate, hash
    - filter, join, scd2_merge, data_vault_hash
    """

    def __init__(self, metadata_client: Any = None):
        """
        Initialize dispatcher.

        Args:
            metadata_client: Optional MetadataClient for join lookups
        """
        self.meta = metadata_client

        # Registry of transform handlers
        self._handlers = {
            "deduplicate": self._apply_deduplicate,
            "null_fill": self._apply_null_fill,
            "null_drop": self._apply_null_drop,
            "rename": self._apply_rename,
            "cast": self._apply_cast,
            "expression": self._apply_expression,
            "window": self._apply_window,
            "aggregate": self._apply_aggregate,
            "hash": self._apply_hash,
            "filter": self._apply_filter,
            "select": self._apply_select,
            "drop_columns": self._apply_drop_columns,
            "add_column": self._apply_add_column,
            "trim": self._apply_trim,
            "uppercase": self._apply_uppercase,
            "lowercase": self._apply_lowercase,
            "date_format": self._apply_date_format,
            "coalesce": self._apply_coalesce,
            "scd2_merge": self._apply_scd2_merge,
            "data_vault_hash": self._apply_data_vault_hash,
        }

    def apply(self, df: Any, transform: Dict[str, Any]) -> Any:
        """
        Apply a single transform to a DataFrame.

        Args:
            df: PySpark DataFrame
            transform: Transform configuration from metadata

        Returns:
            Transformed DataFrame
        """
        transform_type = transform.get("transform_type", "")
        config = transform.get("config", {})
        sequence_order = transform.get("sequence_order", 0)

        handler = self._handlers.get(transform_type)
        if handler is None:
            logger.warning(
                f"Unknown transform type: {transform_type} (sequence={sequence_order}), skipping"
            )
            return df

        logger.info(
            f"Applying transform: {transform_type} (sequence={sequence_order})"
        )

        return handler(df, config)

    def apply_all(
        self,
        df: Any,
        transforms: List[Dict[str, Any]]
    ) -> Any:
        """
        Apply all transforms in sequence order.

        Args:
            df: PySpark DataFrame
            transforms: List of transform configs (ordered by sequence_order)

        Returns:
            Transformed DataFrame
        """
        # Sort by sequence_order for deterministic execution
        sorted_transforms = sorted(
            transforms,
            key=lambda t: t.get("sequence_order", 0)
        )

        for transform in sorted_transforms:
            if not transform.get("is_active", True):
                continue
            df = self.apply(df, transform)

        return df

    # ─────────────────────────────────────────────────────────────────────────
    # Transform Handlers
    # ─────────────────────────────────────────────────────────────────────────

    def _apply_deduplicate(self, df: Any, config: Dict[str, Any]) -> Any:
        """Remove duplicate rows by key columns."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        keys = config.get("keys", [])
        order_by = config.get("order_by", "_ingestion_timestamp")
        order_desc = config.get("order_desc", True)

        if not keys:
            return df

        order_col = F.desc(order_by) if order_desc else F.asc(order_by)
        window = Window.partitionBy(*keys).orderBy(order_col)

        return (
            df
            .withColumn("_dedup_row_num", F.row_number().over(window))
            .filter(F.col("_dedup_row_num") == 1)
            .drop("_dedup_row_num")
        )

    def _apply_null_fill(self, df: Any, config: Dict[str, Any]) -> Any:
        """Fill null values with default."""
        from pyspark.sql import functions as F

        column = config.get("column")
        fill_value = config.get("value", "")
        columns = config.get("columns", {})  # Dict of column -> fill_value

        if column:
            df = df.withColumn(column, F.coalesce(F.col(column), F.lit(fill_value)))

        for col_name, val in columns.items():
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(val)))

        return df

    def _apply_null_drop(self, df: Any, config: Dict[str, Any]) -> Any:
        """Drop rows with null values."""
        columns = config.get("columns", [])
        how = config.get("how", "any")  # "any" or "all"

        if columns:
            return df.dropna(subset=columns, how=how)
        return df.dropna(how=how)

    def _apply_rename(self, df: Any, config: Dict[str, Any]) -> Any:
        """Rename columns."""
        old_name = config.get("old_name")
        new_name = config.get("new_name")
        mapping = config.get("mapping", {})  # Dict of old_name -> new_name

        if old_name and new_name:
            df = df.withColumnRenamed(old_name, new_name)

        for old, new in mapping.items():
            df = df.withColumnRenamed(old, new)

        return df

    def _apply_cast(self, df: Any, config: Dict[str, Any]) -> Any:
        """Cast column type."""
        from pyspark.sql import functions as F

        column = config.get("column")
        target_type = config.get("type", "string")
        columns = config.get("columns", {})  # Dict of column -> type

        if column:
            df = df.withColumn(column, F.col(column).cast(target_type))

        for col_name, col_type in columns.items():
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

        return df

    def _apply_expression(self, df: Any, config: Dict[str, Any]) -> Any:
        """Apply SQL expression."""
        from pyspark.sql import functions as F

        column = config.get("column")
        expression = config.get("expression")

        if column and expression:
            df = df.withColumn(column, F.expr(expression))

        return df

    def _apply_window(self, df: Any, config: Dict[str, Any]) -> Any:
        """Apply window function."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        column = config.get("column")
        function = config.get("function", "sum")
        partition_by = config.get("partition_by", [])
        order_by = config.get("order_by", [])
        output_column = config.get("output_column", f"{column}_{function}")
        rows_between = config.get("rows_between")

        if not column or not partition_by:
            return df

        window = Window.partitionBy(*partition_by)
        if order_by:
            window = window.orderBy(*order_by)
        if rows_between:
            window = window.rowsBetween(rows_between[0], rows_between[1])

        func_map = {
            "sum": F.sum,
            "avg": F.avg,
            "count": F.count,
            "min": F.min,
            "max": F.max,
            "row_number": F.row_number,
            "rank": F.rank,
            "dense_rank": F.dense_rank,
            "lag": lambda c: F.lag(c, config.get("offset", 1)),
            "lead": lambda c: F.lead(c, config.get("offset", 1)),
        }

        func = func_map.get(function)
        if func:
            if function in ("row_number", "rank", "dense_rank"):
                df = df.withColumn(output_column, func().over(window))
            else:
                df = df.withColumn(output_column, func(column).over(window))

        return df

    def _apply_aggregate(self, df: Any, config: Dict[str, Any]) -> Any:
        """Apply group by + aggregation."""
        from pyspark.sql import functions as F

        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations", [])

        if not group_by or not aggregations:
            return df

        agg_map = {
            "sum": F.sum,
            "avg": F.avg,
            "count": F.count,
            "min": F.min,
            "max": F.max,
            "count_distinct": F.countDistinct,
            "collect_list": F.collect_list,
            "collect_set": F.collect_set,
            "first": F.first,
            "last": F.last,
        }

        agg_exprs = []
        for agg in aggregations:
            col = agg.get("column")
            func_name = agg.get("function", "sum")
            alias = agg.get("alias", f"{col}_{func_name}")

            func = agg_map.get(func_name)
            if func and col:
                agg_exprs.append(func(col).alias(alias))

        if agg_exprs:
            return df.groupBy(*group_by).agg(*agg_exprs)

        return df

    def _apply_hash(self, df: Any, config: Dict[str, Any]) -> Any:
        """Generate hash key from columns."""
        from pyspark.sql import functions as F

        columns = config.get("columns", [])
        output_column = config.get("output_column", "_hash_key")
        algorithm = config.get("algorithm", "md5")  # md5, sha256

        if not columns:
            return df

        concat_expr = F.concat_ws("|", *[F.col(c).cast("string") for c in columns])

        if algorithm == "sha256":
            df = df.withColumn(output_column, F.sha2(concat_expr, 256))
        else:
            df = df.withColumn(output_column, F.md5(concat_expr))

        return df

    def _apply_filter(self, df: Any, config: Dict[str, Any]) -> Any:
        """Filter rows by condition."""
        condition = config.get("condition", "")
        if condition:
            return df.filter(condition)
        return df

    def _apply_select(self, df: Any, config: Dict[str, Any]) -> Any:
        """Select specific columns."""
        columns = config.get("columns", [])
        if columns:
            return df.select(*columns)
        return df

    def _apply_drop_columns(self, df: Any, config: Dict[str, Any]) -> Any:
        """Drop specified columns."""
        columns = config.get("columns", [])
        for col in columns:
            if col in df.columns:
                df = df.drop(col)
        return df

    def _apply_add_column(self, df: Any, config: Dict[str, Any]) -> Any:
        """Add a new column with a literal or expression value."""
        from pyspark.sql import functions as F

        column = config.get("column")
        value = config.get("value")
        expression = config.get("expression")

        if column and expression:
            df = df.withColumn(column, F.expr(expression))
        elif column and value is not None:
            df = df.withColumn(column, F.lit(value))

        return df

    def _apply_trim(self, df: Any, config: Dict[str, Any]) -> Any:
        """Trim whitespace from string columns."""
        from pyspark.sql import functions as F

        columns = config.get("columns", [])
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.trim(F.col(col_name)))
        return df

    def _apply_uppercase(self, df: Any, config: Dict[str, Any]) -> Any:
        """Convert string columns to uppercase."""
        from pyspark.sql import functions as F

        columns = config.get("columns", [])
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.upper(F.col(col_name)))
        return df

    def _apply_lowercase(self, df: Any, config: Dict[str, Any]) -> Any:
        """Convert string columns to lowercase."""
        from pyspark.sql import functions as F

        columns = config.get("columns", [])
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.lower(F.col(col_name)))
        return df

    def _apply_date_format(self, df: Any, config: Dict[str, Any]) -> Any:
        """Format date/timestamp column."""
        from pyspark.sql import functions as F

        column = config.get("column")
        input_format = config.get("input_format")
        output_format = config.get("output_format", "yyyy-MM-dd")

        if column and input_format:
            df = df.withColumn(
                column,
                F.date_format(F.to_date(F.col(column), input_format), output_format)
            )
        elif column:
            df = df.withColumn(
                column,
                F.date_format(F.col(column), output_format)
            )

        return df

    def _apply_coalesce(self, df: Any, config: Dict[str, Any]) -> Any:
        """Coalesce multiple columns into one."""
        from pyspark.sql import functions as F

        columns = config.get("columns", [])
        output_column = config.get("output_column")

        if columns and output_column:
            df = df.withColumn(
                output_column,
                F.coalesce(*[F.col(c) for c in columns])
            )

        return df

    def _apply_scd2_merge(self, df: Any, config: Dict[str, Any]) -> Any:
        """
        Apply SCD Type 2 merge logic.

        Adds:
        - _hash_diff: Hash of tracked columns (for change detection)
        - _effective_from: Current timestamp
        - _effective_to: None (current record)
        - _is_current: True
        """
        from pyspark.sql import functions as F

        business_keys = config.get("business_keys", [])
        tracked_columns = config.get("tracked_columns", [])

        if not business_keys or not tracked_columns:
            return df

        # Generate hash of tracked columns for change detection
        hash_expr = F.md5(
            F.concat_ws("|", *[F.col(c).cast("string") for c in tracked_columns])
        )

        df = (
            df
            .withColumn("_hash_diff", hash_expr)
            .withColumn("_effective_from", F.current_timestamp())
            .withColumn("_effective_to", F.lit(None).cast("timestamp"))
            .withColumn("_is_current", F.lit(True))
        )

        return df

    def _apply_data_vault_hash(self, df: Any, config: Dict[str, Any]) -> Any:
        """
        Generate Data Vault hash keys.

        Supports: hub hash keys, link hash keys, hash diffs
        """
        from pyspark.sql import functions as F

        hash_type = config.get("hash_type", "hub")  # hub, link, diff
        columns = config.get("columns", [])
        output_column = config.get("output_column", "_hk")

        if not columns:
            return df

        # Data Vault standard: UPPER + TRIM + CONCAT + MD5
        hash_expr = F.md5(
            F.concat_ws("|", *[
                F.upper(F.trim(F.col(c).cast("string")))
                for c in columns
            ])
        )

        df = df.withColumn(output_column, hash_expr)

        # Add load timestamp for Data Vault
        if hash_type in ("hub", "link"):
            if "_load_timestamp" not in df.columns:
                df = df.withColumn("_load_timestamp", F.current_timestamp())
            if "_record_source" not in df.columns:
                record_source = config.get("record_source", "APEX")
                df = df.withColumn("_record_source", F.lit(record_source))

        return df

    def get_supported_types(self) -> List[str]:
        """Return list of supported transform types."""
        return sorted(self._handlers.keys())


__all__ = [
    "TransformDispatcher",
]
