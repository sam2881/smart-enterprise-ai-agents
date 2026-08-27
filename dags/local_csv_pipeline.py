"""
Local CSV Pipeline — Medallion Architecture (pandas, no cloud dependencies)

Reads a CSV file from /opt/airflow/data/input/ and processes it through
Bronze → Silver → Gold layers, writing Parquet files to /opt/airflow/data/output/.

Trigger with DAG Run config:
  {"csv_filename": "sample_sales.csv"}

Default:  uses sample_sales.csv if no config provided.
"""

import hashlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Paths (/opt/airflow/data mounted from ./data via docker-compose) ──────────
DATA_ROOT = Path("/opt/airflow/data")
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

DAG_ID = "local_csv_pipeline"

default_args = {
    "owner": "apex-local",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# =============================================================================
# Helpers
# =============================================================================

def _csv_path(**kwargs) -> Path:
    conf = kwargs.get("dag_run").conf or {}
    filename = conf.get("csv_filename", "sample_sales.csv")
    return INPUT_DIR / filename


# =============================================================================
# Tasks
# =============================================================================

def task_ingest(**kwargs):
    """Read CSV from input dir; write raw copy as Parquet."""
    csv_path = _csv_path(**kwargs)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found: {csv_path}\n"
            f"Put your file in /opt/airflow/data/input/ (host: ./data/input/)"
        )

    df = pd.read_csv(csv_path)
    print(f"[INGEST] Read {len(df)} rows from {csv_path.name}")
    print(f"[INGEST] Columns: {list(df.columns)}")

    out = OUTPUT_DIR / "raw"
    out.mkdir(parents=True, exist_ok=True)
    out_path = out / (csv_path.stem + ".parquet")
    df.to_parquet(out_path, index=False)
    print(f"[INGEST] Wrote raw → {out_path}")


def task_bronze(**kwargs):
    """Schema enforcement + audit columns → bronze layer."""
    csv_path = _csv_path(**kwargs)
    df = pd.read_csv(csv_path)
    run_id = kwargs.get("run_id", "local_run")

    # Audit columns
    df["_execution_id"] = run_id
    df["_ingestion_ts"]  = datetime.utcnow().isoformat()
    df["_source_file"]   = csv_path.name
    df["_is_current"]    = True

    # Basic type coercion
    for col in ["quantity", "unit_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    out = OUTPUT_DIR / "bronze"
    out.mkdir(parents=True, exist_ok=True)
    out_path = out / "sales.parquet"
    df.to_parquet(out_path, index=False)

    print(f"[BRONZE] {len(df)} rows → {out_path}")
    preview_cols = [c for c in ["order_id", "customer_name", "quantity", "unit_price", "status"] if c in df.columns]
    if preview_cols:
        print(df[preview_cols].to_string(index=False))


def task_silver(**kwargs):
    """Dedup by PK + business key → silver layer."""
    bronze_path = OUTPUT_DIR / "bronze" / "sales.parquet"
    df = pd.read_parquet(bronze_path)
    before = len(df)

    pk_cols = [c for c in ["order_id"] if c in df.columns]
    if pk_cols:
        df = df.sort_values("_ingestion_ts", ascending=False)
        df = df.drop_duplicates(subset=pk_cols, keep="first")
        df["_business_key"] = (
            df[pk_cols]
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        )

    df["_silver_ts"] = datetime.utcnow().isoformat()
    df["_is_valid"]  = True

    out = OUTPUT_DIR / "silver"
    out.mkdir(parents=True, exist_ok=True)
    out_path = out / "sales.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[SILVER] Dedup: {before} → {len(df)} rows → {out_path}")


def task_gold(**kwargs):
    """Customer aggregation → gold layer (Parquet + CSV)."""
    silver_path = OUTPUT_DIR / "silver" / "sales.parquet"
    df = pd.read_parquet(silver_path)

    # Revenue requires both quantity and unit_price columns
    if "quantity" in df.columns and "unit_price" in df.columns:
        df["_line_revenue"] = df["quantity"] * df["unit_price"]
        agg = (
            df.groupby(["customer_id", "customer_name"])
            .agg(
                total_orders   =("order_id", "count"),
                total_revenue  =("_line_revenue", "sum"),
                completed_orders=("status", lambda x: (x == "COMPLETED").sum()),
                shipped_orders  =("status", lambda x: (x == "SHIPPED").sum()),
                pending_orders  =("status", lambda x: (x == "PENDING").sum()),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
    else:
        agg = df.groupby(["customer_id"]).size().reset_index(name="total_orders")

    agg["_gold_ts"] = datetime.utcnow().isoformat()

    out = OUTPUT_DIR / "gold"
    out.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(out / "customer_summary.parquet", index=False)
    agg.to_csv(out / "customer_summary.csv", index=False)

    print(f"\n[GOLD] Customer summary — {len(agg)} customers")
    print()
    print(f"  {'Customer':<20} {'Orders':>7} {'Revenue':>12} {'Done':>5} {'Ship':>5} {'Pend':>5}")
    print(f"  {'-'*20} {'-'*7} {'-'*12} {'-'*5} {'-'*5} {'-'*5}")
    for _, row in agg.iterrows():
        print(
            f"  {row.get('customer_name', row['customer_id']):<20} "
            f"{int(row['total_orders']):>7} "
            f"${row.get('total_revenue', 0):>11,.2f} "
            f"{int(row.get('completed_orders', 0)):>5} "
            f"{int(row.get('shipped_orders', 0)):>5} "
            f"{int(row.get('pending_orders', 0)):>5}"
        )
    print()


def task_validate(**kwargs):
    """Quality checks across all layers."""
    layers = {
        "raw":    OUTPUT_DIR / "raw" / "sample_sales.parquet",
        "bronze": OUTPUT_DIR / "bronze" / "sales.parquet",
        "silver": OUTPUT_DIR / "silver" / "sales.parquet",
        "gold":   OUTPUT_DIR / "gold" / "customer_summary.parquet",
    }
    print("\n[VALIDATE] Layer summary:")
    all_ok = True
    bronze_rows = 0
    silver_rows = 0
    for name, path in layers.items():
        if path.exists():
            df = pd.read_parquet(path)
            status = "PASS" if len(df) > 0 else "FAIL"
            print(f"  [{status}] {name:8s}: {len(df):>4} rows  {len(df.columns):>3} cols")
            if name == "bronze":
                bronze_rows = len(df)
            if name == "silver":
                silver_rows = len(df)
            all_ok = all_ok and len(df) > 0
        else:
            print(f"  [FAIL] {name:8s}: file missing")
            all_ok = False

    if bronze_rows > 0 and silver_rows > 0:
        assert bronze_rows >= silver_rows, "Silver must have <= rows than Bronze"
        print("  [PASS] bronze >= silver row count")

    print(f"\n  Overall: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    if not all_ok:
        raise ValueError("One or more validation checks failed")


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Local CSV Medallion Pipeline — pandas, no cloud dependencies",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["local", "csv", "medallion", "pandas"],
    params={
        "csv_filename": "sample_sales.csv",
    },
) as dag:

    start    = EmptyOperator(task_id="start")
    ingest   = PythonOperator(task_id="ingest",   python_callable=task_ingest)
    bronze   = PythonOperator(task_id="bronze",   python_callable=task_bronze)
    silver   = PythonOperator(task_id="silver",   python_callable=task_silver)
    gold     = PythonOperator(task_id="gold",     python_callable=task_gold)
    validate = PythonOperator(task_id="validate", python_callable=task_validate)
    end      = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> ingest >> bronze >> silver >> gold >> validate >> end
