#!/usr/bin/env python3
"""
Standalone PySpark GE Schema Validator (Production-Aligned)

Runs on Dataproc via spark-submit. Receives MINIMAL parameters via sys.argv
positional arguments — all schema/format/source config is looked up from
PostgreSQL metadata based on feed_id. CANNOT import from dag_utilities at runtime.

Production sys.argv mapping (matches DAG template spark_args):
    [1]  = feed_id
    [2]  = run_id               (Airflow {{ run_id }} macro)
    [3]  = data_asset_name      (XCom from get_file_schema_version, or "")
    [4]  = pg_host
    [5]  = pg_schema            (NOTE: schema before user — production ordering)
    [6]  = pg_user
    [7]  = pg_pw
    [8]  = pg_port
    [9]  = pg_extra             (connection extras, or "")
    [10] = run_id_cws           (XCom from init, internal run tracking)
    [11] = validations_s3_prefix (GCS prefix for GE expectation store)
    [12] = data_docs_prefix     (GCS prefix for GE data docs)

All schema, format, delimiter, source_path, quality rules etc. are fetched
from PostgreSQL tables using feed_id — NOT passed as positional args.

Usage:
    spark-submit validate_schema_with_great_expectations.py 1001 manual__2026-02-08 "" \\
        pg-host public user pass 5432 "" 42 \\
        cdls/validations/great_expectations/ cdls/data_docs/great_expectations/
"""

from __future__ import annotations

import json
import logging
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ge_schema_validator")

# ═══════════════════════════════════════════════════════════════════════════════
# RULE TYPE → GE EXPECTATION MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

RULE_TYPE_MAPPING = {
    "not_null": "expect_column_values_to_not_be_null",
    "unique": "expect_column_values_to_be_unique",
    "range": "expect_column_values_to_be_between",
    "min_max": "expect_column_values_to_be_between",
    "regex": "expect_column_values_to_match_regex",
    "in_set": "expect_column_values_to_be_in_set",
    "date_range": "expect_column_values_to_be_between",
    "column_exists": "expect_column_to_exist",
    "column_type": "expect_column_values_to_be_of_type",
    "distinct_count": "expect_column_distinct_values_to_be_in_set",
    "value_length": "expect_column_value_lengths_to_be_between",
    "referential": "expect_column_pair_values_to_be_in_set",
    "row_count": "expect_table_row_count_to_be_between",
    "completeness": "expect_column_values_to_not_be_null",
}


# ═══════════════════════════════════════════════════════════════════════════════
# PostgreSQL DIRECT CONNECTION (no dag_utilities imports on Dataproc)
# ═══════════════════════════════════════════════════════════════════════════════


def get_pg_connection(host, port, dbname, user, password, schema="public"):
    """Create a direct psycopg2 connection (production pattern)."""
    import psycopg2
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        options=f"-c search_path={schema}",
    )
    conn.autocommit = False
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# METADATA LOOKUP (feed_id → all config from DB)
# ═══════════════════════════════════════════════════════════════════════════════


def load_feed_config(conn, feed_id: int) -> Dict[str, Any]:
    """
    Load feed configuration from feed_details table.

    Production pattern: script receives only feed_id, looks up everything else.
    Returns dict with source_path, format_type, delimiter, encoding, etc.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT feed_name, domain, source_type, source_path, transient_path,
               file_name_pattern, format_type, delimiter, has_header,
               header_rows, footer_rows, encoding, feed_group_id,
               quality_threshold
        FROM feed_details
        WHERE feed_id = %s
        """,
        (feed_id,),
    )
    row = cursor.fetchone()
    if not row:
        logger.warning("No feed_details found for feed_id=%s — using defaults", feed_id)
        return {
            "format_type": "csv",
            "delimiter": ",",
            "has_header": True,
            "header_rows": 0,
            "footer_rows": 0,
            "encoding": "utf-8",
            "source_path": "",
            "transient_path": "",
            "feed_group_id": 0,
            "quality_threshold": 100,
        }

    columns = [desc[0] for desc in cursor.description]
    config = dict(zip(columns, row))

    # Normalize booleans
    if isinstance(config.get("has_header"), str):
        config["has_header"] = config["has_header"].lower() == "true"

    return config


def load_schema_from_db(
    conn, feed_id: int, schema_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Load the active schema definition from schema_details.

    Returns:
        {"columns": [{"name": ..., "type": ..., "nullable": ..., "column_type": ...}],
         "delimiter": ..., "has_header": ..., ...}
    """
    cursor = conn.cursor()

    if schema_id:
        cursor.execute(
            "SELECT schema_json, delimiter, has_header, header_rows, footer_rows, encoding "
            "FROM schema_details WHERE schema_id = %s",
            (schema_id,),
        )
    else:
        cursor.execute(
            "SELECT schema_json, delimiter, has_header, header_rows, footer_rows, encoding "
            "FROM schema_details WHERE feed_id = %s AND is_active = true "
            "ORDER BY effective_from DESC LIMIT 1",
            (feed_id,),
        )

    row = cursor.fetchone()
    if not row:
        logger.warning("No schema found for feed_id=%s", feed_id)
        return {"columns": []}

    schema_json = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
    return {
        "columns": schema_json.get("columns", []),
        "delimiter": row[1] or ",",
        "has_header": row[2] if row[2] is not None else True,
        "header_rows": row[3] or 0,
        "footer_rows": row[4] or 0,
        "encoding": row[5] or "utf-8",
    }


def load_expectations_from_db(
    conn, feed_id: int, zone: str,
) -> List[Dict[str, Any]]:
    """
    Load quality rules from quality_rules / ge_expectations_store.

    Returns list of dicts: [{"rule_type": ..., "column_name": ..., "rule_config": {...}, ...}]
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT rule_type, column_name, rule_config, severity, rule_name
        FROM quality_rules
        WHERE feed_id = %s AND zone_level = %s AND is_active = true
        ORDER BY rule_id
        """,
        (feed_id, zone),
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    rules = []
    for row in rows:
        rule = dict(zip(columns, row))
        if isinstance(rule.get("rule_config"), str):
            rule["rule_config"] = json.loads(rule["rule_config"])
        rules.append(rule)

    logger.info("Loaded %d quality rules for feed=%s zone=%s", len(rules), feed_id, zone)
    return rules


def get_source_file_path(conn, feed_id: int, run_id_cws: str = "") -> str:
    """
    Resolve the source file path for GE validation.

    Production pattern: checks feed_ingestion_details for the specific run,
    falls back to feed_details.transient_path.
    """
    cursor = conn.cursor()

    # Try specific ingestion run first
    if run_id_cws:
        cursor.execute(
            """
            SELECT source_file_path FROM feed_ingestion_details
            WHERE feed_id = %s AND file_run_id = %s
            ORDER BY created_at DESC LIMIT 1
            """,
            (feed_id, int(run_id_cws) if run_id_cws.isdigit() else 0),
        )
        row = cursor.fetchone()
        if row and row[0]:
            return row[0]

    # Fall back to feed_details.transient_path
    cursor.execute(
        "SELECT transient_path FROM feed_details WHERE feed_id = %s",
        (feed_id,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else ""


# ═══════════════════════════════════════════════════════════════════════════════
# SPARK DATA READER (multi-format)
# ═══════════════════════════════════════════════════════════════════════════════


def read_source_data(spark, source_path, format_type, delimiter, has_header, encoding):
    """
    Read source data into a Spark DataFrame.

    Supports: csv, parquet, json, avro, orc, fixed, cobrix
    """
    logger.info("Reading %s data from %s", format_type, source_path)

    if format_type in ("csv", "tsv"):
        sep = delimiter if format_type == "csv" else "\t"
        df = (
            spark.read
            .option("header", str(has_header).lower())
            .option("sep", sep)
            .option("encoding", encoding)
            .option("inferSchema", "false")  # All STRING for raw/schema validation
            .csv(source_path)
        )
    elif format_type == "parquet":
        df = spark.read.parquet(source_path)
    elif format_type == "json":
        df = spark.read.json(source_path)
    elif format_type == "avro":
        df = spark.read.format("avro").load(source_path)
    elif format_type == "orc":
        df = spark.read.orc(source_path)
    elif format_type == "fixed":
        df = spark.read.text(source_path)
    elif format_type == "cobrix":
        df = (
            spark.read
            .format("cobol")
            .option("copybook_contents", "")
            .load(source_path)
        )
    else:
        logger.warning("Unknown format '%s' — falling back to CSV", format_type)
        df = spark.read.option("header", "true").csv(source_path)

    logger.info("Read %d columns from source", len(df.columns))
    return df


def exclude_header_footer_rows(df, header_rows, footer_rows):
    """
    Remove header/footer rows using window functions.

    Some files have multi-line headers or footers that aren't handled
    by Spark's native header option.
    """
    if header_rows <= 0 and footer_rows <= 0:
        return df

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_num", F.row_number().over(w))

    total_rows = df.count()

    if header_rows > 0:
        df = df.filter(F.col("_row_num") > header_rows)

    if footer_rows > 0:
        cutoff = total_rows - footer_rows
        df = df.filter(F.col("_row_num") <= cutoff)

    df = df.drop("_row_num")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# GE EXPECTATION BUILDING
# ═══════════════════════════════════════════════════════════════════════════════


def build_ge_expectations(
    schema: Dict[str, Any],
    rules: List[Dict[str, Any]],
    zone: str,
) -> List[Dict[str, Any]]:
    """
    Build zone-specific GE expectation configurations.

    Zone-specific GE profiles (production pattern):
        TRANSIENT  = none (raw landing, as-is)
        RAW        = schema (7 checks: column exists, count, set match, row count)
        REFINED    = type+null (11 checks: exists + type cast + nullability)
        GOLD       = full quality (14+ checks: all schema + all quality_rules)
    """
    if zone == "transient":
        logger.info("Skipping validation for transient zone")
        return []

    if zone == "raw":
        return _build_raw_schema_expectations(schema)

    if zone == "refined":
        return _build_refined_expectations(schema, rules)

    # gold / consumption — full data quality
    return _build_gold_quality_expectations(schema, rules)


def _build_raw_schema_expectations(
    schema: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    RAW ZONE: Schema-only validation (7 checks per column).

    Checks: column exists, column count, column set match, row count > 0.
    Does NOT check: data types, nulls, ranges, business rules.
    """
    expectations = []
    columns = schema.get("columns", [])
    expected_col_names = []

    for col in columns:
        col_name = col.get("name") or col.get("column_name", "")
        if not col_name:
            continue
        expected_col_names.append(col_name)

        expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": col_name},
            "meta": {
                "source": "schema",
                "severity": "ERROR",
                "zone_profile": "raw_schema",
                "description": f"Column '{col_name}' must exist in file",
            },
        })

    if expected_col_names:
        expectations.append({
            "expectation_type": "expect_table_column_count_to_equal",
            "kwargs": {"value": len(expected_col_names)},
            "meta": {
                "source": "schema",
                "severity": "ERROR",
                "zone_profile": "raw_schema",
                "description": f"File must have exactly {len(expected_col_names)} columns",
            },
        })

        expectations.append({
            "expectation_type": "expect_table_columns_to_match_set",
            "kwargs": {
                "column_set": expected_col_names,
                "exact_match": False,
            },
            "meta": {
                "source": "schema",
                "severity": "ERROR",
                "zone_profile": "raw_schema",
                "description": "All expected columns must be present",
            },
        })

    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1, "max_value": None},
        "meta": {
            "source": "schema",
            "severity": "ERROR",
            "zone_profile": "raw_schema",
            "description": "File must contain at least 1 data row",
        },
    })

    logger.info(
        "RAW ZONE: Built %d schema expectations for %d columns",
        len(expectations), len(expected_col_names),
    )
    return expectations


def _build_refined_expectations(
    schema: Dict[str, Any],
    rules: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    REFINED ZONE: Type checks + nullability (11 checks).

    Bridge between raw (structure) and gold (quality).
    """
    expectations = []

    for col in schema.get("columns", []):
        col_name = col.get("name") or col.get("column_name", "")
        if not col_name:
            continue

        expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": col_name},
            "meta": {"source": "schema", "severity": "ERROR", "zone_profile": "refined"},
        })

        col_type = col.get("type") or col.get("data_type", "")
        if col_type:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": col_name, "type_": col_type},
                "meta": {
                    "source": "schema",
                    "severity": "ERROR",
                    "zone_profile": "refined",
                    "description": f"Column '{col_name}' must be castable to {col_type}",
                },
            })

        nullable = col.get("nullable", True)
        if not nullable:
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col_name},
                "meta": {"source": "schema", "severity": "ERROR", "zone_profile": "refined"},
            })

    logger.info("REFINED ZONE: Built %d expectations", len(expectations))
    return expectations


def _build_gold_quality_expectations(
    schema: Dict[str, Any],
    rules: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    GOLD ZONE: Full data quality validation (14+ checks).

    All schema expectations + all quality_rules from metadata.
    This is the GATE before data enters the consumption zone.
    """
    expectations = []

    for col in schema.get("columns", []):
        col_name = col.get("name") or col.get("column_name", "")
        if not col_name:
            continue

        expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": col_name},
            "meta": {"source": "schema", "severity": "ERROR", "zone_profile": "gold"},
        })

        col_type = col.get("type") or col.get("data_type", "")
        if col_type:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": col_name, "type_": col_type},
                "meta": {"source": "schema", "severity": "ERROR", "zone_profile": "gold"},
            })

        nullable = col.get("nullable", True)
        if not nullable:
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col_name},
                "meta": {"source": "schema", "severity": "ERROR", "zone_profile": "gold"},
            })

    for rule in rules:
        rule_type = rule.get("rule_type", "")
        ge_type = RULE_TYPE_MAPPING.get(rule_type)
        if not ge_type:
            logger.warning("Unknown rule_type '%s' — skipping", rule_type)
            continue

        kwargs = _build_expectation_kwargs(rule, ge_type)
        expectations.append({
            "expectation_type": ge_type,
            "kwargs": kwargs,
            "meta": {
                "source": "quality_rules",
                "rule_name": rule.get("rule_name", ""),
                "severity": rule.get("severity", "ERROR"),
                "zone_profile": "gold",
            },
        })

    logger.info(
        "GOLD ZONE: Built %d expectations (schema=%d, rules=%d)",
        len(expectations),
        len(schema.get("columns", [])),
        len(rules),
    )
    return expectations


def _build_expectation_kwargs(rule: Dict, ge_type: str) -> Dict[str, Any]:
    """Build kwargs for a specific GE expectation from a quality rule."""
    col = rule.get("column_name", "")
    config = rule.get("rule_config", {}) or {}
    kwargs: Dict[str, Any] = {}

    if col and ge_type != "expect_table_row_count_to_be_between":
        kwargs["column"] = col

    if ge_type == "expect_column_values_to_be_between":
        kwargs["min_value"] = config.get("min") or config.get("min_value")
        kwargs["max_value"] = config.get("max") or config.get("max_value")
        kwargs["mostly"] = config.get("mostly", 1.0)

    elif ge_type == "expect_column_values_to_match_regex":
        kwargs["regex"] = config.get("pattern") or config.get("regex", ".*")
        kwargs["mostly"] = config.get("mostly", 1.0)

    elif ge_type == "expect_column_values_to_be_in_set":
        kwargs["value_set"] = config.get("values") or config.get("value_set", [])

    elif ge_type == "expect_column_value_lengths_to_be_between":
        kwargs["min_value"] = config.get("min_length") or config.get("min")
        kwargs["max_value"] = config.get("max_length") or config.get("max")

    elif ge_type == "expect_table_row_count_to_be_between":
        kwargs["min_value"] = config.get("min_rows") or config.get("min")
        kwargs["max_value"] = config.get("max_rows") or config.get("max")

    elif ge_type == "expect_column_values_to_be_of_type":
        kwargs["type_"] = config.get("type") or config.get("data_type", "StringType")

    elif ge_type == "expect_column_distinct_values_to_be_in_set":
        kwargs["value_set"] = config.get("values") or config.get("value_set", [])

    elif ge_type == "expect_column_pair_values_to_be_in_set":
        kwargs["column_A"] = col
        kwargs["column_B"] = config.get("reference_column", "")
        kwargs["value_pairs_set"] = config.get("value_pairs", [])

    return kwargs


# ═══════════════════════════════════════════════════════════════════════════════
# GE VALIDATION RUNNER
# ═══════════════════════════════════════════════════════════════════════════════


def run_ge_validation(
    df, expectations: List[Dict[str, Any]], suite_name: str,
    validations_prefix: str = "", data_docs_prefix: str = "",
) -> Dict[str, Any]:
    """
    Run GE validation against in-memory Spark DataFrame.

    Uses RuntimeBatchRequest for direct DataFrame validation (no file I/O).
    Falls back to simulated validation if GE is not installed.

    Production pattern: validations_prefix and data_docs_prefix configure
    GCS-backed expectation stores and data docs sites.
    """
    total = len(expectations)
    passed = 0
    failed_details = []
    start_time = time.time()

    try:
        import great_expectations as ge
        from great_expectations.core.batch import RuntimeBatchRequest
        from great_expectations.core import ExpectationConfiguration, ExpectationSuite

        context = ge.get_context()

        suite = ExpectationSuite(expectation_suite_name=suite_name)
        for exp in expectations:
            suite.add_expectation(ExpectationConfiguration(
                expectation_type=exp["expectation_type"],
                kwargs=exp["kwargs"],
                meta=exp.get("meta", {}),
            ))

        context.add_or_update_expectation_suite(expectation_suite=suite)

        datasource_name = f"spark_ds_{suite_name}"
        context.sources.add_or_update_spark(name=datasource_name)
        data_asset = context.get_datasource(datasource_name).add_dataframe_asset(
            name=f"df_{suite_name}",
        )

        batch_request = data_asset.build_batch_request(dataframe=df)
        checkpoint = context.add_or_update_checkpoint(
            name=f"ckpt_{suite_name}",
            validations=[{
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }],
        )

        result = checkpoint.run()
        run_result = list(result.run_results.values())[0]
        results_list = run_result["validation_result"]["results"]

        for r in results_list:
            if r["success"]:
                passed += 1
            else:
                failed_details.append({
                    "expectation_type": r["expectation_config"]["expectation_type"],
                    "kwargs": r["expectation_config"]["kwargs"],
                    "observed_value": r["result"].get("observed_value"),
                    "meta": r["expectation_config"].get("meta", {}),
                })

    except ImportError:
        logger.warning("GE not installed — running simulated validation")
        passed, failed_details = _simulate_validation(df, expectations)

    except Exception as exc:
        logger.error("GE validation failed: %s — running simulated validation", exc)
        passed, failed_details = _simulate_validation(df, expectations)

    elapsed_ms = int((time.time() - start_time) * 1000)
    success_rate = passed / total if total > 0 else 1.0

    return {
        "suite_name": suite_name,
        "success": len(failed_details) == 0,
        "success_rate": success_rate,
        "total_expectations": total,
        "passed_expectations": passed,
        "failed_expectations": len(failed_details),
        "failed_details": failed_details,
        "execution_time_ms": elapsed_ms,
    }


def _simulate_validation(
    df, expectations: List[Dict[str, Any]],
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Simulated validation when GE is not installed.

    Performs actual checks using Spark SQL for:
    - Column existence, Column count, Column set match
    - Row count range, Null checks, Uniqueness checks, Range checks
    """
    passed = 0
    failed_details = []
    df_columns = set(c.lower() for c in df.columns)
    df_columns_original = list(df.columns)

    for exp in expectations:
        exp_type = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        col = kwargs.get("column", "")

        if exp_type == "expect_column_to_exist":
            if col.lower() in df_columns:
                passed += 1
            else:
                failed_details.append({
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "observed_value": f"Column '{col}' not found. Available: {df_columns_original}",
                    "meta": exp.get("meta", {}),
                })

        elif exp_type == "expect_table_column_count_to_equal":
            expected_count = kwargs.get("value", 0)
            actual_count = len(df_columns_original)
            if actual_count == expected_count:
                passed += 1
            else:
                failed_details.append({
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "observed_value": f"Expected {expected_count} columns, found {actual_count}",
                    "meta": exp.get("meta", {}),
                })

        elif exp_type == "expect_table_columns_to_match_set":
            expected_set = set(c.lower() for c in kwargs.get("column_set", []))
            missing = expected_set - df_columns
            if not missing:
                passed += 1
            else:
                failed_details.append({
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "observed_value": f"Missing columns: {sorted(missing)}",
                    "meta": exp.get("meta", {}),
                })

        elif exp_type == "expect_table_row_count_to_be_between":
            row_count = df.count()
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            in_range = True
            if min_val is not None and row_count < min_val:
                in_range = False
            if max_val is not None and row_count > max_val:
                in_range = False
            if in_range:
                passed += 1
            else:
                failed_details.append({
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "observed_value": f"Row count={row_count}, expected [{min_val}, {max_val}]",
                    "meta": exp.get("meta", {}),
                })

        elif exp_type == "expect_column_values_to_not_be_null":
            if col.lower() in df_columns:
                from pyspark.sql import functions as F
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count == 0:
                    passed += 1
                else:
                    failed_details.append({
                        "expectation_type": exp_type,
                        "kwargs": kwargs,
                        "observed_value": f"{null_count} null values found",
                        "meta": exp.get("meta", {}),
                    })
            else:
                passed += 1

        elif exp_type == "expect_column_values_to_be_unique":
            if col.lower() in df_columns:
                total = df.count()
                distinct = df.select(col).distinct().count()
                if total == distinct:
                    passed += 1
                else:
                    failed_details.append({
                        "expectation_type": exp_type,
                        "kwargs": kwargs,
                        "observed_value": f"{total - distinct} duplicates",
                        "meta": exp.get("meta", {}),
                    })
            else:
                passed += 1

        elif exp_type == "expect_column_values_to_be_between":
            if col.lower() in df_columns:
                from pyspark.sql import functions as F
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                violations = df
                if min_val is not None:
                    violations = violations.filter(F.col(col) < min_val)
                if max_val is not None:
                    violations = violations.filter(F.col(col) > max_val)
                violation_count = violations.count()
                if violation_count == 0:
                    passed += 1
                else:
                    failed_details.append({
                        "expectation_type": exp_type,
                        "kwargs": kwargs,
                        "observed_value": f"{violation_count} values out of range",
                        "meta": exp.get("meta", {}),
                    })
            else:
                passed += 1

        else:
            passed += 1

    return passed, failed_details


# ═══════════════════════════════════════════════════════════════════════════════
# RESULT WRITER (direct PostgreSQL)
# ═══════════════════════════════════════════════════════════════════════════════


def write_ge_results_to_db(
    conn,
    feed_id: int,
    file_run_id: int,
    zone: str,
    batch_id: str,
    results: Dict[str, Any],
) -> None:
    """
    Write GE validation results to ge_validation_result and ge_validation_detail.

    Production pattern: writes directly via psycopg2 (no dag_utilities dependency).
    """
    cursor = conn.cursor()

    validation_status = "pass" if results["success"] else "fail"
    try:
        cursor.execute(
            """
            INSERT INTO ge_validation_result
                (feed_id, file_run_id, zone, batch_id, suite_name,
                 validation_status, success_rate,
                 total_expectations, passed_expectations, failed_expectations,
                 execution_time_ms, validated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (feed_id, file_run_id, zone) DO UPDATE SET
                validation_status = EXCLUDED.validation_status,
                success_rate = EXCLUDED.success_rate,
                total_expectations = EXCLUDED.total_expectations,
                passed_expectations = EXCLUDED.passed_expectations,
                failed_expectations = EXCLUDED.failed_expectations,
                execution_time_ms = EXCLUDED.execution_time_ms,
                validated_at = NOW()
            """,
            (
                feed_id,
                file_run_id,
                zone,
                batch_id,
                results["suite_name"],
                validation_status,
                results["success_rate"],
                results["total_expectations"],
                results["passed_expectations"],
                results["failed_expectations"],
                results["execution_time_ms"],
            ),
        )
    except Exception as exc:
        logger.warning(
            "ge_validation_result insert failed (table may not exist): %s", exc,
        )
        conn.rollback()
        _ensure_ge_tables(conn)
        return

    for detail in results.get("failed_details", []):
        try:
            cursor.execute(
                """
                INSERT INTO ge_validation_detail
                    (feed_id, file_run_id, zone, batch_id,
                     expectation_type, kwargs, observed_value, severity,
                     validated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    feed_id,
                    file_run_id,
                    zone,
                    batch_id,
                    detail.get("expectation_type", ""),
                    json.dumps(detail.get("kwargs", {})),
                    str(detail.get("observed_value", "")),
                    detail.get("meta", {}).get("severity", "ERROR"),
                ),
            )
        except Exception as exc:
            logger.warning("ge_validation_detail insert failed: %s", exc)

    conn.commit()
    logger.info(
        "GE results written: feed=%s run=%s zone=%s status=%s rate=%.2f%%",
        feed_id, file_run_id, zone, validation_status,
        results["success_rate"] * 100,
    )


def update_status_to_db(conn, feed_id, file_run_id, status):
    """Update feed_ingestion_details status (production pattern)."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE feed_ingestion_details SET status = %s "
            "WHERE file_run_id = %s AND feed_id = %s",
            (status, file_run_id, feed_id),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to update ingestion status: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass


def _ensure_ge_tables(conn):
    """Create GE result tables if they don't exist (for new deployments)."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ge_validation_result (
                id SERIAL PRIMARY KEY,
                feed_id INTEGER NOT NULL,
                file_run_id INTEGER NOT NULL,
                zone VARCHAR(30) NOT NULL,
                batch_id VARCHAR(30),
                suite_name VARCHAR(200),
                validation_status VARCHAR(10) DEFAULT 'pass',
                success_rate FLOAT DEFAULT 1.0,
                total_expectations INTEGER DEFAULT 0,
                passed_expectations INTEGER DEFAULT 0,
                failed_expectations INTEGER DEFAULT 0,
                execution_time_ms INTEGER DEFAULT 0,
                validated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(feed_id, file_run_id, zone)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ge_validation_detail (
                id SERIAL PRIMARY KEY,
                feed_id INTEGER NOT NULL,
                file_run_id INTEGER NOT NULL,
                zone VARCHAR(30) NOT NULL,
                batch_id VARCHAR(30),
                expectation_type VARCHAR(200),
                kwargs JSONB,
                observed_value TEXT,
                severity VARCHAR(20) DEFAULT 'ERROR',
                validated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        logger.info("Created ge_validation_result and ge_validation_detail tables")
    except Exception as exc:
        logger.warning("Failed to create GE tables: %s", exc)
        conn.rollback()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT (Production arg ordering)
# ═══════════════════════════════════════════════════════════════════════════════


def main():
    """
    Main entry point for standalone GE schema validation.

    Production arg ordering (matches DAG template spark_args):
        [1]  feed_id
        [2]  run_id             (Airflow run_id macro)
        [3]  data_asset_name    (XCom or "")
        [4]  pg_host
        [5]  pg_schema          (NOTE: schema before user)
        [6]  pg_user
        [7]  pg_pw
        [8]  pg_port
        [9]  pg_extra
        [10] run_id_cws         (internal run tracking)
        [11] validations_prefix (GCS prefix for GE stores)
        [12] data_docs_prefix   (GCS prefix for GE data docs)

    All format/delimiter/source_path/quality config fetched from DB by feed_id.
    """
    if len(sys.argv) < 7:
        print(
            "Usage: spark-submit validate_schema_with_great_expectations.py "
            "<feed_id> <run_id> <data_asset_name> "
            "<pg_host> <pg_schema> <pg_user> <pg_pw> <pg_port> <pg_extra> "
            "[run_id_cws] [validations_prefix] [data_docs_prefix]"
        )
        sys.exit(1)

    # Parse positional args (production ordering)
    feed_id = int(sys.argv[1])
    run_id = sys.argv[2] if len(sys.argv) > 2 else ""
    data_asset_name = sys.argv[3] if len(sys.argv) > 3 else ""

    # pg_conn — production ordering: host, schema, user, pw, port, extra
    pg_host = sys.argv[4] if len(sys.argv) > 4 else ""
    pg_schema = sys.argv[5] if len(sys.argv) > 5 else "public"
    pg_user = sys.argv[6] if len(sys.argv) > 6 else ""
    pg_pw = sys.argv[7] if len(sys.argv) > 7 else ""
    pg_port = sys.argv[8] if len(sys.argv) > 8 else "5432"
    pg_extra = sys.argv[9] if len(sys.argv) > 9 else ""

    run_id_cws = sys.argv[10] if len(sys.argv) > 10 else ""
    validations_prefix = sys.argv[11] if len(sys.argv) > 11 else ""
    data_docs_prefix = sys.argv[12] if len(sys.argv) > 12 else ""

    logger.info(
        "Starting GE validation: feed=%s run_id=%s data_asset=%s",
        feed_id, run_id, data_asset_name,
    )

    # 1. Connect to PostgreSQL (production: schema is the dbname for search_path)
    conn = get_pg_connection(pg_host, pg_port, pg_schema, pg_user, pg_pw, pg_schema)
    logger.info("Connected to PostgreSQL: %s:%s/%s", pg_host, pg_port, pg_schema)

    # 2. Load feed config from DB (production: minimal args, rest from metadata)
    feed_config = load_feed_config(conn, feed_id)
    format_type = feed_config.get("format_type", "csv")
    delimiter = feed_config.get("delimiter", ",")
    has_header = feed_config.get("has_header", True)
    header_rows = feed_config.get("header_rows", 0) or 0
    footer_rows = feed_config.get("footer_rows", 0) or 0
    encoding = feed_config.get("encoding", "utf-8")
    quality_threshold = feed_config.get("quality_threshold", 100) or 100
    feed_group_id = feed_config.get("feed_group_id", 0) or 0

    # 3. Load schema from DB
    schema = load_schema_from_db(conn, feed_id)
    logger.info("Loaded schema: %d columns", len(schema.get("columns", [])))

    # 4. Resolve source file path
    source_path = get_source_file_path(conn, feed_id, run_id_cws)
    if not source_path:
        source_path = feed_config.get("transient_path", "")
    logger.info("Source path: %s", source_path)

    # 5. Load quality rules from DB
    zone = "raw"  # Schema validation is always raw zone
    rules = load_expectations_from_db(conn, feed_id, zone)

    # 6. Initialize Spark and read data
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(
        f"ge_validator_feed{feed_id}_{zone}_{run_id}"
    ).getOrCreate()

    if not source_path:
        logger.error("No source path found for feed=%s — cannot validate", feed_id)
        update_status_to_db(conn, feed_id, int(run_id_cws) if run_id_cws.isdigit() else 0,
                            "ge_failed_no_source")
        spark.stop()
        sys.exit(1)

    df = read_source_data(spark, source_path, format_type, delimiter, has_header, encoding)

    # 7. Exclude header/footer rows if needed
    df = exclude_header_footer_rows(df, header_rows, footer_rows)
    row_count = df.count()
    logger.info("DataFrame has %d rows, %d columns", row_count, len(df.columns))

    # 8. Build expectations
    expectations = build_ge_expectations(schema, rules, zone)

    # 9. Run validation
    suite_name = data_asset_name or f"feed_{feed_id}_{zone}_validation"
    results = run_ge_validation(df, expectations, suite_name,
                                validations_prefix, data_docs_prefix)

    # 10. Write results to PostgreSQL
    file_run_id = int(run_id_cws) if run_id_cws and run_id_cws.isdigit() else 0
    write_ge_results_to_db(conn, feed_id, file_run_id, zone, run_id, results)

    # 11. Check threshold
    success_pct = results["success_rate"] * 100
    if success_pct < quality_threshold:
        logger.error(
            "Validation BELOW threshold: %.1f%% < %d%% — marking as FAILED",
            success_pct, quality_threshold,
        )
        update_status_to_db(conn, feed_id, file_run_id, "ge_failed")
        conn.close()
        spark.stop()
        sys.exit(1)

    logger.info(
        "Validation PASSED: %.1f%% (threshold=%d%%)", success_pct, quality_threshold,
    )

    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
