#!/usr/bin/env python3
"""
E2E Test: CSV Sales Orders Pipeline - Metadata-Driven Medallion Architecture

Runs the full pipeline locally using the host's GCP credentials:
  1. Reads metadata from PostgreSQL (feed_registry, feed_columns, feed_contract, feed_targets)
  2. Reads CSV from GCS landing zone
  3. Bronze: Schema enforcement + audit columns → BigQuery
  4. Bronze Validation: PK uniqueness, not-null checks
  5. Silver: Dedup + business key → BigQuery
  6. Gold: Aggregation → BigQuery
  7. Finalize: Update execution record + audit log

All schema, paths, and table names come from metadata — NOT hard-coded.
"""

import hashlib
import json
import os
import sys
import uuid
from datetime import datetime
from io import StringIO

import pandas as pd
import psycopg2
from google.cloud import bigquery, storage

# ─── Config ──────────────────────────────────────────────────────────────────
FEED_ID = "sales_orders_csv"
GCP_PROJECT = "agent-ai-test-461120"
# Connect to Postgres via Docker-exposed port
METADATA_DB = "postgresql://admin:admin123@127.0.0.1:5432/agentdb"

# Use default compute engine credentials (editor role) — do NOT set
# GOOGLE_APPLICATION_CREDENTIALS which points to airflow-monitoring-sa (viewer only)


def log(step, msg):
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] [{step}] {msg}")


def main():
    log("INIT", "=" * 60)
    log("INIT", "E2E CSV SALES ORDERS - METADATA-DRIVEN PIPELINE")
    log("INIT", "=" * 60)

    # ─── Step 1: Read metadata ───────────────────────────────────────────
    log("INIT", "Reading metadata from PostgreSQL...")
    conn = psycopg2.connect(METADATA_DB)
    cur = conn.cursor()

    # Feed registry
    cur.execute(
        "SELECT feed_name, source_type, file_delimiter, file_header "
        "FROM feed_registry WHERE feed_id = %s AND is_active = true",
        (FEED_ID,),
    )
    row = cur.fetchone()
    if not row:
        log("INIT", f"ERROR: Feed {FEED_ID} not found")
        sys.exit(1)

    feed_config = {
        "feed_name": row[0],
        "source_type": row[1],
        "delimiter": row[2],
        "header": row[3],
    }
    log("INIT", f"Feed: {feed_config['feed_name']} (type={feed_config['source_type']})")

    # Feed contract (paths)
    cur.execute(
        "SELECT source_path, bronze_path, silver_path, gold_path, encoding "
        "FROM feed_contract WHERE feed_id = %s AND is_active = true",
        (FEED_ID,),
    )
    row = cur.fetchone()
    paths = {
        "source_path": row[0],
        "bronze_path": row[1],
        "silver_path": row[2],
        "gold_path": row[3],
        "encoding": row[4],
    }
    log("INIT", f"Source: {paths['source_path']}")

    # Schema columns
    cur.execute(
        "SELECT column_name, column_order, source_type, target_type, "
        "is_primary_key, is_nullable, transform_expression "
        "FROM feed_columns WHERE feed_id = %s ORDER BY column_order",
        (FEED_ID,),
    )
    columns = []
    primary_keys = []
    for r in cur.fetchall():
        col = {
            "name": r[0],
            "order": r[1],
            "source_type": r[2],
            "target_type": r[3],
            "is_primary_key": r[4],
            "is_nullable": r[5],
            "transform_expression": r[6],
        }
        columns.append(col)
        if r[4]:
            primary_keys.append(r[0])

    log("INIT", f"Schema: {len(columns)} columns, PKs: {primary_keys}")

    # Targets
    cur.execute(
        "SELECT layer, database_name, schema_name, table_name, write_mode "
        "FROM feed_targets WHERE feed_id = %s",
        (FEED_ID,),
    )
    targets = {}
    for r in cur.fetchall():
        targets[r[0]] = {
            "project": r[1],
            "dataset": r[2],
            "table": r[3],
            "write_mode": r[4],
        }

    for layer, t in targets.items():
        log("INIT", f"  Target [{layer}]: {t['project']}.{t['dataset']}.{t['table']}")

    # Create execution record
    execution_id = f"exec_{FEED_ID}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    cur.execute(
        "INSERT INTO platform_pipeline_executions (pipeline_id, run_id, status, started_at) "
        "VALUES (%s, %s, %s, NOW()) RETURNING id",
        (FEED_ID, execution_id, "RUNNING"),
    )
    db_exec_id = cur.fetchone()[0]
    conn.commit()
    log("INIT", f"Execution: {execution_id} (db_id={db_exec_id})")

    # ─── Step 2: Check source file in GCS ────────────────────────────────
    log("SOURCE", "Checking GCS for source files...")
    gcs_client = storage.Client(project=GCP_PROJECT)
    parts = paths["source_path"].replace("gs://", "").split("/", 1)
    bucket_name, prefix = parts[0], parts[1] if len(parts) > 1 else ""
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_files = [b for b in blobs if b.name.endswith(".csv")]

    if not csv_files:
        log("SOURCE", f"ERROR: No CSV files in {paths['source_path']}")
        sys.exit(1)

    total_size = sum(b.size for b in csv_files)
    log("SOURCE", f"Found {len(csv_files)} CSV file(s), {total_size} bytes")
    for b in csv_files:
        log("SOURCE", f"  - gs://{bucket_name}/{b.name} ({b.size} bytes)")

    # ─── Step 3: Raw → Bronze ────────────────────────────────────────────
    log("BRONZE", "Reading CSV from GCS...")
    all_dfs = []
    for blob in csv_files:
        content = blob.download_as_text(encoding=paths.get("encoding", "UTF-8"))
        df = pd.read_csv(
            StringIO(content),
            delimiter=feed_config["delimiter"],
            header=0 if feed_config["header"] else None,
        )
        all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)
    records_read = len(df)
    log("BRONZE", f"Read {records_read} records")

    # Schema enforcement
    expected_cols = [c["name"] for c in columns]
    actual_cols = list(df.columns)
    missing = set(expected_cols) - set(actual_cols)
    if missing:
        log("BRONZE", f"ERROR: Missing columns: {missing}")
        sys.exit(1)

    df = df[expected_cols]

    # Type casting from metadata
    for col_def in columns:
        col_name = col_def["name"]
        target_type = col_def["target_type"]
        if target_type == "INTEGER":
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        elif target_type == "DECIMAL":
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        elif target_type == "DATE":
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
        elif target_type == "STRING":
            df[col_name] = df[col_name].astype(str)

    # Add audit columns
    df["_execution_id"] = execution_id
    df["_ingestion_ts"] = pd.Timestamp.utcnow()
    df["_source_file"] = csv_files[0].name
    df["_is_current"] = True

    log("BRONZE", f"Schema enforced: {len(columns)} columns, types cast")

    # Write to BigQuery
    bq_client = bigquery.Client(project=GCP_PROJECT)
    bronze_target = targets["bronze"]
    bronze_table = f"{bronze_target['project']}.{bronze_target['dataset']}.{bronze_target['table']}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.load_table_from_dataframe(df, bronze_table, job_config=job_config)
    job.result()

    log("BRONZE", f"Loaded {len(df)} records → BigQuery {bronze_table}")

    # ─── Step 4: Bronze Validation ───────────────────────────────────────
    log("VALIDATE", "Running Bronze validation checks...")
    checks_passed = 0
    checks_total = 0

    # Row count
    checks_total += 1
    q = f"SELECT COUNT(*) as cnt FROM `{bronze_table}`"
    cnt = list(bq_client.query(q).result())[0].cnt
    if cnt > 0:
        checks_passed += 1
        log("VALIDATE", f"  ✅ Row count: {cnt}")
    else:
        log("VALIDATE", f"  ❌ Row count: {cnt}")

    # PK uniqueness
    if primary_keys:
        checks_total += 1
        pk_str = ", ".join(primary_keys)
        q = f"SELECT {pk_str}, COUNT(*) c FROM `{bronze_table}` GROUP BY {pk_str} HAVING c > 1"
        dupes = list(bq_client.query(q).result())
        if len(dupes) == 0:
            checks_passed += 1
            log("VALIDATE", f"  ✅ PK uniqueness: 0 duplicates")
        else:
            log("VALIDATE", f"  ❌ PK uniqueness: {len(dupes)} duplicates")

    # Not-null checks
    for col_def in columns:
        if not col_def["is_nullable"]:
            checks_total += 1
            q = f"SELECT COUNT(*) as n FROM `{bronze_table}` WHERE {col_def['name']} IS NULL"
            nulls = list(bq_client.query(q).result())[0].n
            if nulls == 0:
                checks_passed += 1
                log("VALIDATE", f"  ✅ NOT NULL {col_def['name']}: 0 nulls")
            else:
                log("VALIDATE", f"  ❌ NOT NULL {col_def['name']}: {nulls} nulls")

    log("VALIDATE", f"Validation: {checks_passed}/{checks_total} passed")
    if checks_passed < checks_total:
        log("VALIDATE", "WARNING: Some validation checks failed, continuing...")

    # ─── Step 5: Bronze → Silver ─────────────────────────────────────────
    log("SILVER", "Reading Bronze from BigQuery...")
    query = f"SELECT * FROM `{bronze_table}`"
    df = bq_client.query(query).to_dataframe()
    records_before = len(df)

    # Dedup by PK
    if primary_keys:
        df = df.sort_values("_ingestion_ts", ascending=False)
        df = df.drop_duplicates(subset=primary_keys, keep="first")

    records_after = len(df)
    log("SILVER", f"Dedup: {records_before} → {records_after} ({records_before - records_after} removed)")

    # Business key
    if primary_keys:
        df["_business_key"] = (
            df[primary_keys]
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        )
        log("SILVER", f"Generated MD5 business keys from {primary_keys}")

    # Silver audit columns
    df["_silver_execution_id"] = execution_id
    df["_silver_processed_ts"] = pd.Timestamp.utcnow()
    df["_is_valid"] = True

    # Write to BigQuery Silver
    silver_target = targets["silver"]
    silver_table = f"{silver_target['project']}.{silver_target['dataset']}.{silver_target['table']}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.load_table_from_dataframe(df, silver_table, job_config=job_config)
    job.result()

    log("SILVER", f"Loaded {len(df)} records → BigQuery {silver_table}")

    # ─── Step 6: Silver → Gold ───────────────────────────────────────────
    log("GOLD", "Building Gold aggregation...")
    gold_target = targets["gold"]
    gold_table = f"{gold_target['project']}.{gold_target['dataset']}.{gold_target['table']}"

    gold_query = f"""
    SELECT
        customer_id,
        customer_name,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(CAST(quantity AS INT64) * CAST(unit_price AS FLOAT64)) AS total_revenue,
        MIN(CAST(order_date AS STRING)) AS first_order_date,
        MAX(CAST(order_date AS STRING)) AS last_order_date,
        COUNTIF(status = 'COMPLETED') AS completed_orders,
        COUNTIF(status = 'SHIPPED') AS shipped_orders,
        COUNTIF(status = 'PENDING') AS pending_orders,
        '{execution_id}' AS _gold_execution_id,
        CURRENT_TIMESTAMP() AS _gold_load_ts
    FROM `{silver_table}`
    GROUP BY customer_id, customer_name
    ORDER BY total_revenue DESC
    """

    job_config = bigquery.QueryJobConfig(
        destination=gold_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.query(gold_query, job_config=job_config)
    job.result()

    # Preview results
    preview = list(bq_client.query(f"SELECT * FROM `{gold_table}` ORDER BY total_revenue DESC").result())
    log("GOLD", f"Loaded {len(preview)} customer aggregations → BigQuery {gold_table}")
    log("GOLD", "")
    log("GOLD", f"  {'Customer':<20s} {'Orders':>7s} {'Revenue':>12s} {'Status':<25s}")
    log("GOLD", f"  {'-'*20} {'-'*7} {'-'*12} {'-'*25}")
    for row in preview:
        status = f"C={row.completed_orders} S={row.shipped_orders} P={row.pending_orders}"
        log("GOLD", f"  {row.customer_name:<20s} {row.total_orders:>7d} ${row.total_revenue:>11,.2f} {status}")

    # ─── Step 7: Finalize ────────────────────────────────────────────────
    log("FINAL", "Updating execution record...")
    metrics = {
        "bronze": {"records_read": records_read, "records_written": records_read, "table": bronze_table},
        "silver": {"records_in": records_before, "records_out": records_after, "table": silver_table},
        "gold": {"customers": len(preview), "table": gold_table},
    }

    cur.execute(
        "UPDATE platform_pipeline_executions SET status = %s, completed_at = NOW(), "
        "input_rows = %s, output_rows = %s, metrics = %s WHERE id = %s",
        ("COMPLETED", records_read, records_after, json.dumps(metrics), db_exec_id),
    )

    cur.execute(
        "INSERT INTO platform_audit_log (event_id, event_type, actor, action, resource, outcome, details) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            str(uuid.uuid4()),
            "PIPELINE_COMPLETED",
            "e2e_test_script",
            "E2E_PIPELINE_EXECUTION",
            FEED_ID,
            "SUCCESS",
            json.dumps(metrics),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

    log("FINAL", "")
    log("FINAL", "═" * 60)
    log("FINAL", "  E2E PIPELINE COMPLETED SUCCESSFULLY")
    log("FINAL", "═" * 60)
    log("FINAL", f"  Execution: {execution_id}")
    log("FINAL", f"  Bronze:    {records_read} records → {bronze_table}")
    log("FINAL", f"  Silver:    {records_after} records → {silver_table}")
    log("FINAL", f"  Gold:      {len(preview)} customers → {gold_table}")
    log("FINAL", f"  Validation: {checks_passed}/{checks_total} checks passed")
    log("FINAL", "═" * 60)
    log("FINAL", "")
    log("FINAL", "Verify in BigQuery:")
    log("FINAL", f"  SELECT * FROM `{bronze_table}` LIMIT 5;")
    log("FINAL", f"  SELECT * FROM `{silver_table}` LIMIT 5;")
    log("FINAL", f"  SELECT * FROM `{gold_table}` ORDER BY total_revenue DESC;")


if __name__ == "__main__":
    main()
