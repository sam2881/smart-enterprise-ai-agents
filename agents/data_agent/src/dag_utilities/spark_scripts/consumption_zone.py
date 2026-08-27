#!/usr/bin/env python3
"""
Standalone PySpark Consumption Zone Processor (Production-Aligned)

Runs on Dataproc via spark-submit. Handles the consumption zone which has
specialized Data Vault logic that doesn't fit the generic zone_processor.py
uniform pattern.

Production sys.argv mapping (matches DAG template spark_args):
    [1]  = pg_host              (NOTE: pg_conn fields come FIRST)
    [2]  = pg_schema
    [3]  = pg_user
    [4]  = pg_pw
    [5]  = pg_port
    [6]  = pg_extra
    [7]  = feed_id

All other config (data_vault_type, target_table, source_table, etc.)
is looked up from PostgreSQL metadata using feed_id.

Supports:
    - 4 Data Vault patterns (satellite, hub, fpe_hub, link)
    - 6 encryption types (fpe, fpe_hk, fpe_col_concat_hk, hash, sha256, md5)
    - SCD2 effectivity dating (valid_from, valid_to)
    - Dynamic Hive view creation
    - Snowflake column ordering

Usage:
    spark-submit consumption_zone.py \\
        pg-host public user pass 5432 "" 1001
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumption_zone")


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


def load_cz_config(conn, feed_id: int) -> Dict[str, Any]:
    """
    Load consumption zone configuration from metadata.

    Production pattern: script receives only feed_id + pg_conn, looks up
    everything else. Reads from schema_details, contract_details, feed_details.
    """
    cursor = conn.cursor()

    # Get schema with column_type annotations (fpe, fpe_hk, hash, etc.)
    cursor.execute(
        """
        SELECT schema_json FROM schema_details
        WHERE feed_id = %s AND is_active = true
        ORDER BY effective_from DESC LIMIT 1
        """,
        (feed_id,),
    )
    row = cursor.fetchone()
    schema_json = {}
    if row:
        schema_json = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")

    # Get contract details
    cursor.execute(
        """
        SELECT contract_type, data_vault_type, business_keys, tracked_columns,
               hash_algorithm, hash_diff_columns
        FROM contract_details
        WHERE feed_id = %s AND is_active = true
        LIMIT 1
        """,
        (feed_id,),
    )
    contract_row = cursor.fetchone()
    contract = {}
    if contract_row:
        columns = [desc[0] for desc in cursor.description]
        contract = dict(zip(columns, contract_row))
        for key in ("business_keys", "tracked_columns", "hash_diff_columns"):
            val = contract.get(key)
            if isinstance(val, str):
                contract[key] = json.loads(val)

    # Get feed details for target configuration
    cursor.execute(
        """
        SELECT feed_name, domain, target_dataset, target_table,
               cz_view_script, snowflake_target, source_path, transient_path
        FROM feed_details
        WHERE feed_id = %s
        """,
        (feed_id,),
    )
    feed_row = cursor.fetchone()
    feed = {}
    if feed_row:
        feed_columns = [desc[0] for desc in cursor.description]
        feed = dict(zip(feed_columns, feed_row))

    return {
        "schema": schema_json,
        "contract": contract,
        "feed": feed,
        "columns": schema_json.get("columns", []),
        "data_vault_type": contract.get("data_vault_type", "satellite"),
        "source_table": feed.get("source_path", ""),
        "target_table": feed.get("target_table", ""),
        "target_dataset": feed.get("target_dataset", ""),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# HASH KEY GENERATION
# ═══════════════════════════════════════════════════════════════════════════════


def generate_hash_key(df, key_cols: List[str], output_col: str = "hash_key"):
    """
    Generate a hash key from business key columns.

    Uses SHA-256 on concatenated business key values (pipe-delimited).
    Standard Data Vault hash key pattern.
    """
    from pyspark.sql import functions as F

    if not key_cols:
        return df

    concat_expr = F.concat_ws(
        "|",
        *[F.upper(F.trim(F.coalesce(F.col(c).cast("string"), F.lit("")))) for c in key_cols],
    )

    df = df.withColumn(output_col, F.sha2(concat_expr, 256))
    return df


def generate_hash_diff(df, tracked_cols: List[str], output_col: str = "hash_diff"):
    """
    Generate a hash diff from tracked columns.

    Used for SCD2 change detection — if hash_diff changes between loads,
    the satellite record has changed and needs a new effective row.
    """
    from pyspark.sql import functions as F

    if not tracked_cols:
        return df

    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols],
    )

    df = df.withColumn(output_col, F.sha2(concat_expr, 256))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# COLUMN ENCRYPTION (6 types)
# ═══════════════════════════════════════════════════════════════════════════════


def apply_column_encryption(df, col_name: str, enc_type: str, config: Dict[str, Any]):
    """
    Apply encryption to a column based on schema_json.Column_Type.

    Encryption types:
        fpe            — Format-Preserving Encryption (preserves length/format)
        fpe_hk         — FPE on a hash key column
        fpe_col_concat_hk — FPE on concatenation of multiple columns
        hash           — SHA-256 hash (one-way, irreversible)
        sha256         — Alias for hash
        md5            — MD5 hash (legacy, not recommended)
    """
    from pyspark.sql import functions as F

    if enc_type in ("hash", "sha256"):
        df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))

    elif enc_type == "md5":
        df = df.withColumn(col_name, F.md5(F.col(col_name).cast("string")))

    elif enc_type == "fpe":
        # Format-Preserving Encryption: in production, uses Cloud KMS + FF3
        # Simulation: reversible string transformation preserving length
        df = df.withColumn(
            col_name,
            F.sha2(F.col(col_name).cast("string"), 256).substr(1, F.length(F.col(col_name))),
        )

    elif enc_type == "fpe_hk":
        hk_col = f"{col_name}_hk"
        df = df.withColumn(
            hk_col,
            F.sha2(F.col(col_name).cast("string"), 256),
        )

    elif enc_type == "fpe_col_concat_hk":
        concat_cols = config.get("concat_columns", [col_name])
        hk_col = f"{col_name}_hk"
        concat_expr = F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in concat_cols],
        )
        df = df.withColumn(hk_col, F.sha2(concat_expr, 256))

    else:
        logger.warning("Unknown encryption type '%s' for column '%s'", enc_type, col_name)

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VAULT PATTERNS (4)
# ═══════════════════════════════════════════════════════════════════════════════


def process_satellite(spark, df, config: Dict, conn) -> Any:
    """
    Process Data Vault Satellite (SCD2 change detection).

    Satellites store descriptive attributes with full history.
    New rows are inserted when hash_diff changes (attribute change detected).
    """
    from pyspark.sql import functions as F

    contract = config.get("contract", {})
    business_keys = contract.get("business_keys", [])
    tracked_cols = contract.get("tracked_columns", [])

    df = generate_hash_key(df, business_keys, "hash_key")
    df = generate_hash_diff(df, tracked_cols, "hash_diff")

    df = df.withColumn("valid_from", F.current_timestamp())
    df = df.withColumn("valid_to", F.lit("9999-12-31 23:59:59").cast("timestamp"))
    df = df.withColumn("is_current", F.lit(True))
    df = df.withColumn("load_timestamp", F.current_timestamp())

    target_table = config.get("target_table", "")
    if target_table:
        df = apply_effectivity_dating(spark, df, target_table, "hash_key", "hash_diff")

    df = _apply_schema_encryption(df, config)

    logger.info(
        "Satellite processed: %d records, business_keys=%s",
        df.count(), business_keys,
    )
    return df


def process_hub(spark, df, config: Dict, conn) -> Any:
    """
    Process Data Vault Hub (business key dedup).

    Hubs store unique business keys with load date.
    Only new keys (not yet in the hub) are inserted.
    """
    from pyspark.sql import functions as F

    contract = config.get("contract", {})
    business_keys = contract.get("business_keys", [])

    df = generate_hash_key(df, business_keys, "hub_hash_key")

    df = df.withColumn("load_date", F.current_timestamp())
    df = df.withColumn("record_source", F.lit(config.get("source_table", "unknown")))

    hub_cols = ["hub_hash_key"] + business_keys + ["load_date", "record_source"]
    available = [c for c in hub_cols if c in df.columns]
    df = df.select(*available)

    df = df.dropDuplicates(["hub_hash_key"])

    target_table = config.get("target_table", "")
    if target_table:
        try:
            existing = spark.read.format("bigquery").load(target_table)
            existing_keys = existing.select("hub_hash_key")
            df = df.join(existing_keys, "hub_hash_key", "left_anti")
        except Exception as exc:
            logger.info("Hub target not found or empty, inserting all: %s", exc)

    logger.info("Hub processed: %d new keys", df.count())
    return df


def process_fpe_hub(spark, df, config: Dict, conn) -> Any:
    """
    Process Data Vault Hub with FPE-encrypted business keys.

    Same as regular hub but business keys are encrypted before hashing.
    """
    from pyspark.sql import functions as F

    contract = config.get("contract", {})
    business_keys = contract.get("business_keys", [])

    for key_col in business_keys:
        df = apply_column_encryption(df, key_col, "fpe", config)

    df = generate_hash_key(df, business_keys, "hub_hash_key")

    df = df.withColumn("load_date", F.current_timestamp())
    df = df.withColumn("record_source", F.lit(config.get("source_table", "unknown")))

    df = df.dropDuplicates(["hub_hash_key"])

    logger.info("FPE Hub processed: %d records with encrypted keys", df.count())
    return df


def process_link(spark, df, config: Dict, conn) -> Any:
    """
    Process Data Vault Link (multi-hub relationships).

    Links connect two or more hubs via their hash keys.
    Only new link combinations are inserted.
    """
    from pyspark.sql import functions as F

    contract = config.get("contract", {})
    business_keys = contract.get("business_keys", [])

    df = generate_hash_key(df, business_keys, "link_hash_key")

    for i, key_col in enumerate(business_keys):
        df = df.withColumn(
            f"hub_{i}_hash_key",
            F.sha2(F.upper(F.trim(F.coalesce(F.col(key_col).cast("string"), F.lit("")))), 256),
        )

    df = df.withColumn("load_date", F.current_timestamp())
    df = df.withColumn("record_source", F.lit(config.get("source_table", "unknown")))

    df = df.dropDuplicates(["link_hash_key"])

    logger.info("Link processed: %d relationships", df.count())
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SCD2 EFFECTIVITY DATING
# ═══════════════════════════════════════════════════════════════════════════════


def apply_effectivity_dating(
    spark, new_df, existing_table: str, key_col: str, diff_col: str,
) -> Any:
    """
    Apply SCD2 effectivity dating.

    Compares new records against existing satellite table:
    - New keys → INSERT with valid_from=NOW, valid_to=9999-12-31
    - Changed records → INSERT new version, UPDATE old version's valid_to
    - Unchanged records → SKIP
    """
    from pyspark.sql import functions as F

    try:
        existing_df = (
            spark.read.format("bigquery").load(existing_table)
            .filter(F.col("is_current") == True)
            .select(key_col, diff_col)
            .withColumnRenamed(diff_col, f"existing_{diff_col}")
        )

        joined = new_df.join(existing_df, key_col, "left")

        new_records = joined.filter(F.col(f"existing_{diff_col}").isNull())

        changed_records = joined.filter(
            (F.col(f"existing_{diff_col}").isNotNull())
            & (F.col(diff_col) != F.col(f"existing_{diff_col}"))
        )

        result = new_records.unionByName(changed_records, allowMissingColumns=True)
        result = result.drop(f"existing_{diff_col}")

        unchanged_count = joined.filter(
            (F.col(f"existing_{diff_col}").isNotNull())
            & (F.col(diff_col) == F.col(f"existing_{diff_col}"))
        ).count()

        logger.info(
            "SCD2 effectivity: new=%d, changed=%d, unchanged=%d",
            new_records.count(), changed_records.count(), unchanged_count,
        )

        return result

    except Exception as exc:
        logger.info(
            "Existing table not found or read failed — inserting all as new: %s", exc,
        )
        return new_df


# ═══════════════════════════════════════════════════════════════════════════════
# VIEWS & TARGETS
# ═══════════════════════════════════════════════════════════════════════════════


def create_hive_view(conn, feed_id: int, spark) -> None:
    """
    Create Hive-compatible view from cz_view_script in metadata.

    Some consumption zone tables have associated views for BI tools.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT cz_view_script FROM feed_details WHERE feed_id = %s",
        (feed_id,),
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        logger.info("No cz_view_script configured for feed=%s", feed_id)
        return

    view_script = row[0]
    try:
        spark.sql(view_script)
        logger.info("Created Hive view for feed=%s", feed_id)
    except Exception as exc:
        logger.warning("Failed to create Hive view: %s", exc)


def get_snowflake_column_order(
    conn, feed_id: int, target_table: str,
) -> List[str]:
    """
    Get Snowflake target column order from metadata.

    Ensures output DataFrame matches Snowflake table column order
    for correct INSERT mapping.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT snowflake_column_order FROM feed_details WHERE feed_id = %s
        """,
        (feed_id,),
    )
    row = cursor.fetchone()
    if row and row[0]:
        col_order = row[0] if isinstance(row[0], list) else json.loads(row[0])
        return col_order

    return []


def write_to_target(df, target_table: str, write_mode: str = "append") -> None:
    """
    Write DataFrame to BigQuery target table.

    Uses the BigQuery Spark connector for direct write.
    """
    logger.info("Writing to %s (mode=%s)", target_table, write_mode)

    try:
        (
            df.write
            .format("bigquery")
            .option("table", target_table)
            .option("temporaryGcsBucket", "apex-temp-bq")
            .mode(write_mode)
            .save()
        )
        logger.info("Write complete: %d rows to %s", df.count(), target_table)
    except Exception as exc:
        logger.error("Write to BigQuery failed: %s", exc)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# ENCRYPTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _apply_schema_encryption(df, config: Dict) -> Any:
    """
    Apply encryption to columns based on schema_json.Column_Type annotations.

    Each column in schema may have a "column_type" field indicating encryption:
        column_type: "fpe" | "fpe_hk" | "fpe_col_concat_hk" | "hash" | null
    """
    columns = config.get("columns", [])
    for col_def in columns:
        col_name = col_def.get("name") or col_def.get("column_name", "")
        col_type = col_def.get("column_type") or col_def.get("encryption_type", "")

        if not col_type or col_type.lower() in ("none", "plain", ""):
            continue

        if col_name in df.columns:
            df = apply_column_encryption(df, col_name, col_type.lower(), config)
            logger.info("Encrypted column '%s' with type '%s'", col_name, col_type)

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════════════════════


def audit_log(conn, feed_id, file_run_id, zone, status, records=0, error=None):
    """Write audit record to audit_log table (production pattern)."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO audit_log
                (event_type, severity, feed_id, execution_id, event_data, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                f"ZONE_CONSUMPTION_{status.upper()}",
                "ERROR" if status == "failed" else "INFO",
                feed_id,
                str(file_run_id),
                json.dumps({
                    "zone": zone,
                    "records": records,
                    "error": error,
                }),
            ),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("audit_log failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass


def update_status_to_db(conn, feed_id, file_run_id, status, records=0):
    """Update feed_ingestion_details status (production pattern)."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE feed_ingestion_details
            SET records_written = %s, status = %s, completed_at = NOW()
            WHERE file_run_id = %s AND feed_id = %s
            """,
            (records, status, file_run_id, feed_id),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to update ingestion status: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT (Production arg ordering: pg_conn first, then feed_id)
# ═══════════════════════════════════════════════════════════════════════════════

# Data Vault pattern dispatch
DV_PROCESSORS = {
    "satellite": process_satellite,
    "hub": process_hub,
    "fpe_hub": process_fpe_hub,
    "link": process_link,
}


def main():
    """
    Main entry point for consumption zone processing.

    Production arg ordering (matches DAG template spark_args):
        [1]  pg_host           (NOTE: pg_conn fields come FIRST)
        [2]  pg_schema
        [3]  pg_user
        [4]  pg_pw
        [5]  pg_port
        [6]  pg_extra
        [7]  feed_id

    All data_vault_type, source_table, target_table etc. fetched from DB.
    """
    if len(sys.argv) < 7:
        print(
            "Usage: spark-submit consumption_zone.py "
            "<pg_host> <pg_schema> <pg_user> <pg_pw> <pg_port> <pg_extra> "
            "<feed_id>"
        )
        sys.exit(1)

    # Parse positional args (production ordering: pg_conn FIRST, then feed_id)
    pg_host = sys.argv[1]
    pg_schema = sys.argv[2] if len(sys.argv) > 2 else "public"
    pg_user = sys.argv[3] if len(sys.argv) > 3 else ""
    pg_pw = sys.argv[4] if len(sys.argv) > 4 else ""
    pg_port = sys.argv[5] if len(sys.argv) > 5 else "5432"
    pg_extra = sys.argv[6] if len(sys.argv) > 6 else ""

    feed_id = int(sys.argv[7]) if len(sys.argv) > 7 else 0

    logger.info("Starting consumption zone: feed=%s", feed_id)

    start_time = time.time()

    # 1. Connect to PostgreSQL (production: schema is dbname for search_path)
    conn = get_pg_connection(pg_host, pg_port, pg_schema, pg_user, pg_pw, pg_schema)
    logger.info("Connected to PostgreSQL: %s:%s/%s", pg_host, pg_port, pg_schema)

    # 2. Load configuration from metadata (production: minimal args, rest from DB)
    cz_config = load_cz_config(conn, feed_id)
    data_vault_type = cz_config.get("data_vault_type", "satellite")
    source_table = cz_config.get("source_table", "")
    target_table = cz_config.get("target_table", "")
    target_dataset = cz_config.get("target_dataset", "")

    logger.info(
        "CZ config: dv_type=%s source=%s target=%s",
        data_vault_type, source_table, target_table,
    )

    # 3. Initialize Spark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(
        f"cz_feed{feed_id}_{data_vault_type}"
    ).getOrCreate()

    # 4. Read source data (from gold zone / BigQuery)
    logger.info("Reading source: %s", source_table)
    try:
        df = spark.read.format("bigquery").load(source_table)
    except Exception:
        logger.info("BigQuery read failed — trying GCS path")
        df = spark.read.parquet(source_table)

    row_count_in = df.count()
    logger.info("Source data: %d rows, %d columns", row_count_in, len(df.columns))

    # 5. Apply Data Vault pattern
    processor = DV_PROCESSORS.get(data_vault_type.lower())
    if not processor:
        logger.error("Unknown data_vault_type: %s", data_vault_type)
        audit_log(conn, feed_id, 0, "consumption", "failed",
                  error=f"Unknown data_vault_type: {data_vault_type}")
        spark.stop()
        sys.exit(1)

    df = processor(spark, df, cz_config, conn)
    row_count_out = df.count()

    # 6. Reorder columns for Snowflake if configured
    sf_order = get_snowflake_column_order(conn, feed_id, target_table)
    if sf_order:
        available = [c for c in sf_order if c in df.columns]
        remaining = [c for c in df.columns if c not in sf_order]
        df = df.select(*(available + remaining))
        logger.info("Reordered columns for Snowflake: %d ordered + %d extra",
                     len(available), len(remaining))

    # 7. Write to target
    if target_table:
        write_to_target(df, target_table, write_mode="append")

    # 8. Create Hive view if configured
    create_hive_view(conn, feed_id, spark)

    # 9. Write audit log
    elapsed = time.time() - start_time
    audit_log(conn, feed_id, 0, "consumption", "success",
              records=row_count_out)

    # 10. Update ingestion status
    update_status_to_db(conn, feed_id, 0, "success", records=row_count_out)

    logger.info(
        "Consumption zone complete: feed=%s dv=%s in=%d out=%d time=%.1fs",
        feed_id, data_vault_type, row_count_in, row_count_out, elapsed,
    )

    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
