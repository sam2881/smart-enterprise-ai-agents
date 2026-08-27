#!/usr/bin/env python3
"""E2E test: Generate a CSV file ingest DAG via v2 FeedGroupConfig pipeline."""

import sys
import os

# Add the data agent root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents", "data_agent"))

from src.models.feed_config import (
    FeedGroupConfig, FeedConfig, ColumnDef, QualityRule, BigQueryTarget,
)
from src.generators.apex_dag_generator import APEXDAGGenerator
from pathlib import Path
import tempfile


def build_csv_feed_group() -> FeedGroupConfig:
    """Build a sample CSV file ingest FeedGroupConfig."""
    return FeedGroupConfig(
        feed_group_id="fg_sales_daily",
        dag_id="sales_daily_csv_ingest",
        domain="sales",
        owner="apex-data-agent",
        description="Daily sales CSV file ingest pipeline",
        feeds=[
            FeedConfig(
                feed_id="feed_daily_sales",
                feed_name="daily_sales",
                domain="sales",
                source_type="gcs_file",
                source_connection_id="gcs_default",
                source_config={
                    "bucket": "data-lake-raw",
                    "prefix": "inbound/sales/",
                    "pattern": "*.csv",
                },
                file_format="csv",
                delimiter=",",
                header_rows=1,
                encoding="UTF-8",
                platform_schema_version=1,
                columns=[
                    ColumnDef(name="order_id", data_type="STRING", nullable=False),
                    ColumnDef(name="customer_id", data_type="STRING", nullable=False),
                    ColumnDef(name="order_date", data_type="DATE", nullable=False),
                    ColumnDef(name="product_name", data_type="STRING"),
                    ColumnDef(name="quantity", data_type="INTEGER"),
                    ColumnDef(name="unit_price", data_type="DECIMAL", precision=10, scale=2),
                    ColumnDef(name="total_amount", data_type="DECIMAL", precision=12, scale=2),
                    ColumnDef(name="region", data_type="STRING"),
                ],
                business_keys=["order_id"],
                partition_keys=["order_date"],
                watermark_column="order_date",
                load_type="incremental",
                bq_target=BigQueryTarget(
                    dataset="sales_data",
                    table="daily_sales",
                    partition_field="order_date",
                    clustering_fields=["customer_id", "region"],
                ),
                quality_rules=[
                    QualityRule(column="order_id", rule_type="not_null"),
                    QualityRule(column="order_id", rule_type="unique"),
                    QualityRule(column="total_amount", rule_type="range", params={"min": 0, "max": 1000000}),
                ],
                sql_load_type="merge",
            ),
        ],
        schedule={"dev": "@daily", "prod": "0 6 * * *"},
        notification_emails={"prod": ["data-team@company.com"]},
        enable_duplicate_detection=True,
        enable_self_retrigger=False,
        tags=["sales", "csv", "daily"],
        start_year=2024,
        start_month=1,
        start_day=1,
    )


def main():
    config = build_csv_feed_group()
    print(f"[OK] FeedGroupConfig built: dag_id={config.dag_id}, feeds={len(config.feeds)}")

    # Generate into a temp dir first
    output_dir = Path(tempfile.mkdtemp(prefix="v2_dag_"))
    print(f"[OK] Output dir: {output_dir}")

    generator = APEXDAGGenerator()
    try:
        result = generator.generate_from_feed_group(config, output_dir=output_dir)
    except Exception as e:
        print(f"[FAIL] Generation error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print(f"[OK] DAG generated: {result['dag_path']}")
    print(f"     Template: {result['template_used']}")
    print(f"     SQL scripts: {len(result['sql_scripts'])}")
    print(f"     GE suites: {len(result['ge_suites'])}")
    print(f"     Spark jobs: {len(result['spark_jobs'])}")
    print(f"     Validation: {'PASS' if result['validation_passed'] else 'FAIL'}")
    if result['validation_messages']:
        for msg in result['validation_messages']:
            print(f"     - {msg}")

    # Print generated DAG code
    dag_code = result['dag_code']
    print(f"\n{'='*60}")
    print("GENERATED DAG CODE:")
    print(f"{'='*60}")
    for i, line in enumerate(dag_code.split('\n'), 1):
        print(f"{i:4d} | {line}")

    # Try compile
    try:
        compile(dag_code, "sales_daily_csv_ingest.py", "exec")
        print(f"\n[OK] Python compile: PASS")
    except SyntaxError as e:
        print(f"\n[FAIL] Python compile: line {e.lineno}: {e.msg}")
        sys.exit(1)

    # Copy to dags/ for Airflow testing
    dags_dir = Path(__file__).parent.parent / "dags"
    dags_dir.mkdir(exist_ok=True)
    final_path = dags_dir / f"{config.dag_id}.py"
    final_path.write_text(dag_code)
    print(f"[OK] DAG copied to: {final_path}")

    print(f"\n{'='*60}")
    print("E2E DAG GENERATION: SUCCESS")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
