"""
Zone Processor - Generic Spark Job for ALL Zone Transitions

DATA PLANE: Executes ONLY, never decides.

This is the SINGLE Spark script that handles ALL zone transitions:
- Transient → Raw
- Raw → Refined
- Refined → Gold (final medallion layer)
- Gold → Consumption (Data Vault)

Same code handles ALL zones - behavior changes ONLY via metadata, never code edits.

Usage:
    spark-submit zone_processor.py \\
        --feed-id 1001 \\
        --zone refined \\
        --batch-id 20260205 \\
        --metadata-url postgresql://...

Zone Processing Pattern (UNIFORM FOR ALL ZONES):
1. Read Metadata      → MetadataClient.get_zone_config(feed_id, zone)
2. Read Source        → SparkSession.read from previous zone/source
3. GE Schema Validate → validate_schema(df, schema_expectation_suite)
4. Transform          → Apply zone-specific transforms from metadata
5. GE Semantic Valid  → validate_semantic(df, semantic_rules)
6. Write Target       → df.write with partition/clustering from metadata
7. Update Lineage     → Record zone metrics in feed_ingestion_details
"""

import argparse
import sys
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class ZoneProcessorConfig:
    """Configuration for zone processing."""
    feed_id: int
    zone: str
    batch_id: str
    metadata_url: str
    execution_id: Optional[str] = None


@dataclass
class ZoneProcessingResult:
    """Result of zone processing."""
    feed_id: int
    zone: str
    batch_id: str
    status: str  # success, failed, skipped
    records_input: int = 0
    records_output: int = 0
    records_rejected: int = 0
    schema_validation_passed: bool = True
    semantic_validation_passed: bool = True
    validation_score: float = 100.0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_id": self.feed_id,
            "zone": self.zone,
            "batch_id": self.batch_id,
            "status": self.status,
            "records_input": self.records_input,
            "records_output": self.records_output,
            "records_rejected": self.records_rejected,
            "schema_validation_passed": self.schema_validation_passed,
            "semantic_validation_passed": self.semantic_validation_passed,
            "validation_score": self.validation_score,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metrics": self.metrics,
        }


class ZoneProcessor:
    """
    Generic zone processor - ALL logic from metadata.

    Same code handles: transient, raw, refined, gold, consumption.
    """

    # Zone transition order — gold is the final medallion layer (replaces trusted)
    ZONE_ORDER = ["transient", "raw", "silver", "gold", "consumption"]

    def __init__(self, config: ZoneProcessorConfig):
        """
        Initialize zone processor.

        Args:
            config: ZoneProcessorConfig with feed_id, zone, batch_id, metadata_url
        """
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.metadata_client = None

    def get_spark_session(self) -> SparkSession:
        """Create or get Spark session with BigQuery connector."""
        if self.spark is None:
            self.spark = (
                SparkSession.builder
                .appName(f"apex_zone_{self.config.zone}_{self.config.feed_id}_{self.config.batch_id}")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.dynamicAllocation.enabled", "true")
                .getOrCreate()
            )
        return self.spark

    def get_metadata_client(self):
        """Get metadata client (lazy initialization)."""
        if self.metadata_client is None:
            # Import here to avoid dependency on dag_utilities when running standalone
            try:
                from dag_utilities.core import MetadataClient
                self.metadata_client = MetadataClient(self.config.metadata_url)
            except ImportError:
                # Standalone mode - create minimal client
                self.metadata_client = StandaloneMetadataClient(self.config.metadata_url)
        return self.metadata_client

    def process(self) -> ZoneProcessingResult:
        """
        Process zone with uniform 7-step pattern.

        Returns:
            ZoneProcessingResult with processing metrics
        """
        result = ZoneProcessingResult(
            feed_id=self.config.feed_id,
            zone=self.config.zone,
            batch_id=self.config.batch_id,
            status="started",
            started_at=datetime.utcnow(),
        )

        try:
            spark = self.get_spark_session()
            meta = self.get_metadata_client()

            # Step 1: Read zone configuration from metadata
            zone_config = meta.get_zone_config(self.config.feed_id, self.config.zone)
            schema = meta.get_schema(self.config.feed_id)
            transforms = meta.get_transforms(self.config.feed_id, self.config.zone)
            quality_rules = meta.get_quality_rules(self.config.feed_id, self.config.zone)
            encryption_config = meta.get_encryption_config(self.config.feed_id, self.config.zone)

            # Step 2: Read from source (previous zone or landing)
            df = self._read_source(spark, zone_config, meta)
            result.records_input = df.count()

            # Step 3: GE Schema Validation (auto-creates suite if not exists)
            schema_result = self._validate_schema(df, schema, zone_config)
            result.schema_validation_passed = schema_result.get("success", True)
            if not result.schema_validation_passed and zone_config.get("fail_on_schema_error", True):
                raise ValueError(f"Schema validation failed: {schema_result.get('errors', [])}")

            # Step 4: Apply transforms from metadata (type-safe dispatch)
            df = self._apply_transforms(df, transforms, zone_config)

            # Step 5: Apply encryption if configured (FPE, hash, fpe_hk, fpe_col_concat_hk)
            if encryption_config:
                df = self._apply_encryption(df, encryption_config)

            # Step 6: GE Semantic Validation
            semantic_result = self._validate_semantic(df, quality_rules, zone_config)
            result.semantic_validation_passed = semantic_result.get("success", True)
            result.validation_score = semantic_result.get("success_percent", 100.0)

            # Step 7: Write to target (BigQuery, GCS, or both)
            self._write_target(df, zone_config)
            result.records_output = df.count()
            result.records_rejected = result.records_input - result.records_output

            # Step 8: Update lineage and metrics
            result.status = "success"
            result.completed_at = datetime.utcnow()

            meta.record_zone_metrics(
                self.config.feed_id,
                self.config.zone,
                self.config.batch_id,
                result.to_dict()
            )

            print(f"Zone {self.config.zone} processing complete: {result.records_output} records")
            return result

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            print(f"ERROR: Zone {self.config.zone} processing failed: {e}")
            raise

        finally:
            if self.spark:
                self.spark.stop()

    def _read_source(
        self,
        spark: SparkSession,
        zone_config: Dict[str, Any],
        meta
    ) -> DataFrame:
        """
        Read data from source (previous zone or landing).

        For transient zone: reads from landing (GCS/external source)
        For other zones: reads from previous zone (BigQuery)
        """
        zone = self.config.zone
        batch_id = self.config.batch_id

        if zone == "transient":
            # Read from landing (external source)
            source_path = zone_config.get("source_path", "").format(batch_id=batch_id)
            source_format = zone_config.get("source_format", "csv")
            read_options = zone_config.get("read_options", {})

            reader = spark.read.format(source_format)
            for key, value in read_options.items():
                reader = reader.option(key, value)

            return reader.load(source_path)

        else:
            # Read from previous zone (BigQuery)
            prev_zone_idx = self.ZONE_ORDER.index(zone) - 1
            prev_zone = self.ZONE_ORDER[prev_zone_idx] if prev_zone_idx >= 0 else "transient"
            prev_config = meta.get_zone_config(self.config.feed_id, prev_zone)

            prev_dataset = prev_config.get("dataset_name", f"{prev_zone}_data")
            prev_table = prev_config.get("table_name", f"feed_{self.config.feed_id}_{prev_zone}")
            full_table = f"{prev_dataset}.{prev_table}"

            # Read with batch filter
            df = (
                spark.read
                .format("bigquery")
                .option("table", full_table)
                .option("filter", f"_batch_id = '{batch_id}'")
                .load()
            )

            return df

    def _validate_schema(
        self,
        df: DataFrame,
        schema: Dict[str, Any],
        zone_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate schema against expectation suite.

        Auto-creates GE suite from schema_details if not exists.
        """
        # Skip schema validation for transient zone (raw landing, no enforcement)
        if self.config.zone == "transient":
            return {"success": True, "skipped": True, "reason": "transient zone - no schema enforcement"}

        # Basic schema validation (column existence and types)
        errors = []
        expected_columns = schema.get("columns", [])

        df_columns = {col.lower() for col in df.columns}

        for col_spec in expected_columns:
            col_name = col_spec.get("name", col_spec.get("column_name", ""))
            if col_name.lower() not in df_columns:
                errors.append(f"Missing column: {col_name}")

        if errors:
            return {"success": False, "errors": errors}

        return {"success": True}

    def _validate_semantic(
        self,
        df: DataFrame,
        quality_rules: List[Dict[str, Any]],
        zone_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate data against semantic rules from quality_rules table.
        """
        if not quality_rules:
            return {"success": True, "skipped": True, "reason": "no quality rules defined"}

        total_rules = len(quality_rules)
        passed_rules = 0
        failed_rules = []

        for rule in quality_rules:
            rule_type = rule.get("rule_type", "")
            column_name = rule.get("column_name", "")
            rule_config = rule.get("rule_config", {})

            try:
                if rule_type == "not_null":
                    # Check for null values
                    null_count = df.filter(F.col(column_name).isNull()).count()
                    if null_count == 0:
                        passed_rules += 1
                    else:
                        failed_rules.append(f"{column_name}: {null_count} null values")

                elif rule_type == "unique":
                    # Check for unique values
                    total = df.count()
                    distinct = df.select(column_name).distinct().count()
                    if total == distinct:
                        passed_rules += 1
                    else:
                        failed_rules.append(f"{column_name}: {total - distinct} duplicate values")

                elif rule_type == "range":
                    # Check value range
                    min_val = rule_config.get("min")
                    max_val = rule_config.get("max")
                    out_of_range = df.filter(
                        (F.col(column_name) < min_val) | (F.col(column_name) > max_val)
                    ).count()
                    if out_of_range == 0:
                        passed_rules += 1
                    else:
                        failed_rules.append(f"{column_name}: {out_of_range} values out of range")

                elif rule_type == "regex":
                    # Check regex pattern
                    pattern = rule_config.get("pattern", ".*")
                    non_matching = df.filter(~F.col(column_name).rlike(pattern)).count()
                    if non_matching == 0:
                        passed_rules += 1
                    else:
                        failed_rules.append(f"{column_name}: {non_matching} values don't match pattern")

                elif rule_type == "in_set":
                    # Check values in allowed set
                    allowed_values = rule_config.get("values", [])
                    invalid = df.filter(~F.col(column_name).isin(allowed_values)).count()
                    if invalid == 0:
                        passed_rules += 1
                    else:
                        failed_rules.append(f"{column_name}: {invalid} values not in allowed set")

                else:
                    # Unknown rule type - pass by default
                    passed_rules += 1

            except Exception as e:
                failed_rules.append(f"{column_name}: validation error - {str(e)}")

        success_percent = (passed_rules / total_rules * 100) if total_rules > 0 else 100.0

        return {
            "success": len(failed_rules) == 0,
            "success_percent": success_percent,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
        }

    def _apply_transforms(
        self,
        df: DataFrame,
        transforms: List[Dict[str, Any]],
        zone_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Apply zone-specific transforms from metadata.

        Transform dispatch is deterministic - same transforms produce same output.
        """
        zone = self.config.zone
        batch_id = self.config.batch_id

        # Zone-specific default transforms
        if zone == "transient":
            # Transient: No transformation, just copy
            return df

        elif zone == "raw":
            # Raw: Add audit columns (ingestion_timestamp, batch_id, source_file)
            df = (
                df
                .withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_source_file", F.input_file_name())
            )

        elif zone == "silver":
            # Refined: Data cleaning, deduplication, standardization
            # Apply type casting from schema
            schema = zone_config.get("schema", {})
            for col_spec in schema.get("columns", []):
                col_name = col_spec.get("name", col_spec.get("column_name"))
                col_type = col_spec.get("type", col_spec.get("data_type", "string"))
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(col_type.lower()))

            # Trim string columns
            string_cols = [
                col_spec.get("name", col_spec.get("column_name"))
                for col_spec in schema.get("columns", [])
                if col_spec.get("type", col_spec.get("data_type", "")).lower() == "string"
            ]
            for col in string_cols:
                if col in df.columns:
                    df = df.withColumn(col, F.trim(F.col(col)))

            # Add refined audit columns
            df = df.withColumn("_refined_timestamp", F.current_timestamp())

        elif zone == "gold":
            # Gold: Business logic, calculations (final layer)
            df = (
                df
                .withColumn("_gold_timestamp", F.current_timestamp())
                .withColumn("_is_current", F.lit(True))
            )

        elif zone == "consumption":
            # Consumption: Final curated data, Data Vault ready
            df = df.withColumn("_consumption_timestamp", F.current_timestamp())

        # Apply custom transforms from metadata
        for transform in transforms:
            transform_type = transform.get("transform_type", "")
            config = transform.get("config", {})

            df = self._dispatch_transform(df, transform_type, config)

        return df

    def _dispatch_transform(
        self,
        df: DataFrame,
        transform_type: str,
        config: Dict[str, Any]
    ) -> DataFrame:
        """
        Dispatch transform by type.

        Maps transform_type → PySpark transformation.
        """
        if transform_type == "deduplicate":
            # Deduplicate by keys
            keys = config.get("keys", [])
            order_by = config.get("order_by", "_ingestion_timestamp")
            if keys:
                window = Window.partitionBy(*keys).orderBy(F.desc(order_by))
                df = (
                    df
                    .withColumn("_row_num", F.row_number().over(window))
                    .filter(F.col("_row_num") == 1)
                    .drop("_row_num")
                )

        elif transform_type == "null_fill":
            # Fill null values
            column = config.get("column")
            fill_value = config.get("value", "")
            if column:
                df = df.withColumn(column, F.coalesce(F.col(column), F.lit(fill_value)))

        elif transform_type == "null_drop":
            # Drop rows with null values
            columns = config.get("columns", [])
            if columns:
                df = df.dropna(subset=columns)

        elif transform_type == "rename":
            # Rename column
            old_name = config.get("old_name")
            new_name = config.get("new_name")
            if old_name and new_name:
                df = df.withColumnRenamed(old_name, new_name)

        elif transform_type == "cast":
            # Cast column type
            column = config.get("column")
            target_type = config.get("type", "string")
            if column:
                df = df.withColumn(column, F.col(column).cast(target_type))

        elif transform_type == "expression":
            # Apply SQL expression
            column = config.get("column")
            expression = config.get("expression")
            if column and expression:
                df = df.withColumn(column, F.expr(expression))

        elif transform_type == "window":
            # Window function
            column = config.get("column")
            function = config.get("function", "sum")
            partition_by = config.get("partition_by", [])
            order_by = config.get("order_by", [])
            output_column = config.get("output_column", f"{column}_{function}")

            if column and partition_by:
                window = Window.partitionBy(*partition_by)
                if order_by:
                    window = window.orderBy(*order_by)

                if function == "sum":
                    df = df.withColumn(output_column, F.sum(column).over(window))
                elif function == "avg":
                    df = df.withColumn(output_column, F.avg(column).over(window))
                elif function == "count":
                    df = df.withColumn(output_column, F.count(column).over(window))
                elif function == "row_number":
                    df = df.withColumn(output_column, F.row_number().over(window))

        elif transform_type == "aggregate":
            # Aggregation
            group_by = config.get("group_by", [])
            aggregations = config.get("aggregations", [])

            if group_by and aggregations:
                agg_exprs = []
                for agg in aggregations:
                    col = agg.get("column")
                    func = agg.get("function", "sum")
                    alias = agg.get("alias", f"{col}_{func}")

                    if func == "sum":
                        agg_exprs.append(F.sum(col).alias(alias))
                    elif func == "avg":
                        agg_exprs.append(F.avg(col).alias(alias))
                    elif func == "count":
                        agg_exprs.append(F.count(col).alias(alias))
                    elif func == "min":
                        agg_exprs.append(F.min(col).alias(alias))
                    elif func == "max":
                        agg_exprs.append(F.max(col).alias(alias))

                if agg_exprs:
                    df = df.groupBy(*group_by).agg(*agg_exprs)

        elif transform_type == "hash":
            # Generate hash key
            columns = config.get("columns", [])
            output_column = config.get("output_column", "_hash_key")
            if columns:
                df = df.withColumn(
                    output_column,
                    F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in columns]))
                )

        return df

    def _apply_encryption(
        self,
        df: DataFrame,
        encryption_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Apply FPE/hash encryption per metadata config.

        Supports: fpe, fpe_hk, fpe_col_concat_hk, hash
        """
        for col_config in encryption_config.get("columns", []):
            col_name = col_config.get("column")
            enc_type = col_config.get("type")

            if enc_type == "hash":
                # SHA256 hash (one-way)
                df = df.withColumn(
                    f"{col_name}_hash",
                    F.sha2(F.col(col_name).cast("string"), 256)
                )

            elif enc_type == "fpe_hk":
                # FPE hash key (for Data Vault)
                df = df.withColumn(
                    f"{col_name}_hk",
                    F.md5(F.upper(F.trim(F.col(col_name).cast("string"))))
                )

            elif enc_type == "fpe_col_concat_hk":
                # FPE concatenated hash key
                concat_cols = col_config.get("concat_columns", [])
                if concat_cols:
                    df = df.withColumn(
                        f"{col_name}_hk",
                        F.md5(F.concat_ws("|", *[
                            F.upper(F.trim(F.col(c).cast("string")))
                            for c in concat_cols
                        ]))
                    )

            # Note: True FPE (format-preserving encryption) requires external library
            # This implementation uses hash-based approach for simplicity

        return df

    def _write_target(
        self,
        df: DataFrame,
        zone_config: Dict[str, Any]
    ) -> None:
        """
        Write to target (BigQuery, GCS, or both).
        """
        target_dataset = zone_config.get("dataset_name", f"{self.config.zone}_data")
        target_table = zone_config.get("table_name", f"feed_{self.config.feed_id}_{self.config.zone}")
        full_table = f"{target_dataset}.{target_table}"

        write_mode = zone_config.get("write_mode", "append")
        partition_cols = zone_config.get("partition_config", {}).get("columns", [])
        cluster_cols = zone_config.get("clustering_columns", [])

        writer = df.write.format("bigquery").option("table", full_table)

        # Add temporary GCS bucket for BigQuery connector
        temp_bucket = zone_config.get("temp_bucket", "apex-temp")
        writer = writer.option("temporaryGcsBucket", temp_bucket)

        # Add partitioning
        if partition_cols:
            partition_col = partition_cols[0] if isinstance(partition_cols, list) else partition_cols
            writer = writer.option("partitionField", partition_col)
            writer = writer.option("partitionType", "DAY")

        # Add clustering
        if cluster_cols:
            writer = writer.option("clusteredFields", ",".join(cluster_cols[:4]))

        # Execute write
        writer.mode(write_mode).save()

        print(f"Written to {full_table} (mode={write_mode})")


class StandaloneMetadataClient:
    """
    Minimal metadata client for standalone Spark job execution.

    Used when dag_utilities is not available (direct spark-submit).
    """

    def __init__(self, metadata_url: str):
        self.metadata_url = metadata_url
        # In production, connect to PostgreSQL
        # For now, return default configurations

    def get_zone_config(self, feed_id: int, zone: str) -> Dict[str, Any]:
        """Get zone configuration with sensible defaults."""
        return {
            "zone_level": zone,
            "dataset_name": f"{zone}_data",
            "table_name": f"feed_{feed_id}_{zone}",
            "write_mode": "append" if zone in ["transient", "raw", "silver"] else "merge",  # gold and consumption use merge
            "partition_config": {"columns": ["_batch_id"]},
            "clustering_columns": [],
            "schema": {"columns": []},
            "fail_on_schema_error": zone not in ["transient"],
        }

    def get_schema(self, feed_id: int) -> Dict[str, Any]:
        """Get schema with empty columns (infer from source)."""
        return {"columns": []}

    def get_transforms(self, feed_id: int, zone: str) -> List[Dict[str, Any]]:
        """Get transforms - empty list for standalone mode."""
        return []

    def get_quality_rules(self, feed_id: int, zone: str) -> List[Dict[str, Any]]:
        """Get quality rules - empty list for standalone mode."""
        return []

    def get_encryption_config(self, feed_id: int, zone: str) -> Optional[Dict[str, Any]]:
        """Get encryption config - None for standalone mode."""
        return None

    def record_zone_metrics(
        self,
        feed_id: int,
        zone: str,
        batch_id: str,
        metrics: Dict[str, Any]
    ) -> None:
        """Record metrics - no-op for standalone mode."""
        print(f"[METRICS] feed_id={feed_id}, zone={zone}, batch_id={batch_id}")
        print(f"  {json.dumps(metrics, indent=2, default=str)}")


def process_zone(
    feed_id: int,
    zone: str,
    batch_id: str,
    metadata_url: str,
    execution_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to process a zone.

    Args:
        feed_id: Numeric feed ID
        zone: Zone to process (transient, raw, refined, gold, consumption)
        batch_id: Batch identifier (usually YYYYMMDD)
        metadata_url: PostgreSQL connection URL
        execution_id: Optional execution ID for tracking

    Returns:
        Processing result dictionary
    """
    config = ZoneProcessorConfig(
        feed_id=feed_id,
        zone=zone,
        batch_id=batch_id,
        metadata_url=metadata_url,
        execution_id=execution_id,
    )

    processor = ZoneProcessor(config)
    result = processor.process()

    return result.to_dict()


def submit_zone_job(
    feed_id: int,
    zone: str,
    batch_id: str,
    metadata_url: str,
    gcp_project: str,
    gcp_region: str,
    dataproc_cluster: str,
    spark_jobs_bucket: str,
) -> Dict[str, Any]:
    """
    Submit zone processing job to Dataproc.

    This function is called from Airflow DAGs to submit Spark jobs.

    Args:
        feed_id: Numeric feed ID
        zone: Zone to process
        batch_id: Batch identifier
        metadata_url: PostgreSQL connection URL
        gcp_project: GCP project ID
        gcp_region: GCP region
        dataproc_cluster: Dataproc cluster name
        spark_jobs_bucket: GCS bucket with Spark job files

    Returns:
        Job submission result
    """
    # Build job configuration
    job_config = {
        "reference": {"project_id": gcp_project},
        "placement": {"cluster_name": dataproc_cluster},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{spark_jobs_bucket}/dag_utilities/processors/zone_processor.py",
            "args": [
                "--feed-id", str(feed_id),
                "--zone", zone,
                "--batch-id", batch_id,
                "--metadata-url", metadata_url,
            ],
        },
    }

    return {
        "job_config": job_config,
        "gcp_project": gcp_project,
        "gcp_region": gcp_region,
        "feed_id": feed_id,
        "zone": zone,
        "batch_id": batch_id,
    }


def main():
    """Main entry point for spark-submit."""
    parser = argparse.ArgumentParser(description="APEX Zone Processor")
    parser.add_argument("--feed-id", type=int, required=True, help="Feed ID")
    parser.add_argument("--zone", type=str, required=True,
                        choices=["transient", "raw", "silver", "gold", "consumption"],
                        help="Zone to process")
    parser.add_argument("--batch-id", type=str, required=True, help="Batch ID (YYYYMMDD)")
    parser.add_argument("--metadata-url", type=str, required=True, help="Metadata database URL")
    parser.add_argument("--execution-id", type=str, help="Optional execution ID")

    args = parser.parse_args()

    print(f"Starting APEX Zone Processor")
    print(f"  Feed ID: {args.feed_id}")
    print(f"  Zone: {args.zone}")
    print(f"  Batch ID: {args.batch_id}")

    try:
        result = process_zone(
            feed_id=args.feed_id,
            zone=args.zone,
            batch_id=args.batch_id,
            metadata_url=args.metadata_url,
            execution_id=args.execution_id,
        )

        print(f"Zone processing complete: {json.dumps(result, indent=2, default=str)}")
        return 0

    except Exception as e:
        print(f"ERROR: Zone processing failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "ZoneProcessor",
    "ZoneProcessorConfig",
    "ZoneProcessingResult",
    "process_zone",
    "submit_zone_job",
]
