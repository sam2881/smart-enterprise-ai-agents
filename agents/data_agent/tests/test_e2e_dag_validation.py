#!/usr/bin/env python3
"""
E2E DAG Validation - Task 14.1

Tests production template rendering end-to-end:
1. Renders p01_file_production.py.jinja2 with representative metadata
2. Renders p01_file_group_production.py.jinja2 with multi-feed metadata
3. Validates generated code is syntactically valid Python
4. Checks for required Airflow imports and DAG constructs
5. Verifies production patterns (dag_utilities imports, TaskGroups, etc.)
"""

import ast
import os
import sys
import json
from pathlib import Path

# Add the project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from jinja2 import Environment, FileSystemLoader

# ============================================================================
# Template Setup
# ============================================================================

TEMPLATE_DIR = PROJECT_ROOT / "src" / "templates" / "patterns"


def get_jinja_env():
    """Create Jinja2 environment matching production settings."""
    return Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )


# ============================================================================
# Representative Metadata - Single Feed (EBCDIC File Ingestion)
# ============================================================================

SINGLE_FEED_METADATA = {
    "dag_id": "fraud_detection_dailyfile_load",
    "feed_id": 947,
    "feed_name": "fraud_detection_daily",
    "feed_group_id": 100,
    "feed_group_name": "fraud_detection_dailyfile_load",
    "domain": "risk_management",
    "environment": "dev",
    "pattern_code": "P01",
    "source_category": "FILE",
    "source_type": "file_ebcdic",
    "source_system": "mainframe",
    "file_format": "ebcdic",
    "delimiter": "",
    "has_header": False,
    "encoding": "EBCDIC",
    "file_pattern": "*.dat",

    # Contract
    "contract_type": "STANDARD",
    "data_vault_type": None,

    # Schema
    "schema": {
        "columns": [
            {"name": "transaction_id", "type": "string", "nullable": False},
            {"name": "account_number", "type": "string", "nullable": False},
            {"name": "transaction_amount", "type": "decimal", "nullable": False},
            {"name": "transaction_date", "type": "date", "nullable": False},
            {"name": "merchant_code", "type": "string", "nullable": True},
        ]
    },
    "columns": [
        {"name": "transaction_id", "type": "string", "nullable": False},
        {"name": "account_number", "type": "string", "nullable": False},
        {"name": "transaction_amount", "type": "decimal", "nullable": False},
        {"name": "transaction_date", "type": "date", "nullable": False},
        {"name": "merchant_code", "type": "string", "nullable": True},
    ],

    # Zones
    "zones": ["transient", "raw", "refined", "gold", "consumption"],
    "zone_configs": [],

    # Transforms and quality
    "transforms": [],
    "quality_rules": [],
    "encryption_config": None,

    # Execution
    "schedule": {"dev": "@daily", "qa": "@daily", "prod": "@daily"},
    "retry_count": 3,
    "retry_delay_minutes": 5,
    "depends_on_past": False,
    "catchup": False,
    "max_active_runs": 1,

    # Owner
    "owner": "apex-data-agent",
    "notification_emails": ["data-team@company.com"],
    "notification_on_success": False,

    # GCP
    "gcp_project": "apex-data-platform",
    "gcp_region": "us-central1",
    "dataproc_cluster": "apex-dataproc",
    "spark_jobs_bucket": "apex-spark-jobs",

    # Storage
    "source_bucket": "apex-source-bucket",
    "source_prefix": "fraud/daily/",
    "landing_bucket": "apex-landing",
    "archive_bucket": "apex-archive",
    "archive_enabled": True,
    "archive_retention_days": 90,

    # Quality
    "quality_threshold": 0.95,
    "ge_failure_policy": "FAIL",
    "contract_policy": "FAIL_FAST",

    # Zone targets
    "raw_dataset": "raw",
    "raw_table": "fraud_detection_daily_raw",
    "refined_dataset": "refined",
    "refined_table": "fraud_detection_daily",
    "gold_dataset": "gold",
    "gold_table": "fraud_detection_daily",
    "consumption_dataset": "consumption",
    "consumption_table": "fraud_detection_daily_summary",
    "include_consumption": True,

    # Spark
    "spark_executor_memory": "8g",
    "spark_executor_cores": 4,

    # Production features
    "use_file_sensor": True,
    "check_business_date": True,
    "use_pvc_config": True,
    "bucket_template": "gs://apex-{zone}-bucket",
}


# ============================================================================
# Representative Metadata - Multi-Feed Group
# ============================================================================

GROUP_FEED_METADATA = {
    "dag_id": "fraud_falcon_dailyfile_load",
    "feed_group_id": 100,
    "feed_group_name": "fraud_falcon_dailyfile_load",
    "domain": "risk_management",
    "environment": "dev",
    "owner": "apex-data-agent",
    "schedule": {"dev": "@daily", "qa": "@daily", "prod": "@daily"},
    "retry_count": 3,
    "retry_delay_minutes": 5,
    "depends_on_past": False,
    "catchup": False,
    "max_active_runs": 1,
    "notification_emails": ["data-team@company.com"],
    "notification_on_success": False,
    "gcp_project": "apex-data-platform",
    "gcp_region": "us-central1",
    "dataproc_cluster": "apex-dataproc",
    "spark_jobs_bucket": "apex-spark-jobs",
    "use_file_sensor": True,
    "check_business_date": True,
    "use_pvc_config": True,
    "bucket_template": "gs://apex-{zone}-bucket",

    # Multiple feeds
    "feeds": [
        {
            "feed_id": 947,
            "feed_name": "fraud_transaction_detail",
            "source_type": "file_ebcdic",
            "file_format": "ebcdic",
            "file_pattern": "FRAUD_TXN_*.dat",
            "source_bucket": "apex-source-bucket",
            "source_prefix": "fraud/transactions/",
            "encoding": "EBCDIC",
            "include_consumption": True,
            "raw_table": "fraud_transaction_detail_raw",
            "refined_table": "fraud_transaction_detail",
            "gold_table": "fraud_transaction_detail",
            "consumption_table": "fraud_transaction_summary",
        },
        {
            "feed_id": 1314,
            "feed_name": "fraud_account_master",
            "source_type": "file_csv",
            "file_format": "csv",
            "file_pattern": "ACCT_MASTER_*.csv",
            "source_bucket": "apex-source-bucket",
            "source_prefix": "fraud/accounts/",
            "encoding": "UTF-8",
            "include_consumption": False,
            "raw_table": "fraud_account_master_raw",
            "refined_table": "fraud_account_master",
            "gold_table": "fraud_account_master",
            "consumption_table": "",
        },
    ],
}


# ============================================================================
# Validation Functions
# ============================================================================

def validate_python_syntax(code: str, name: str) -> list:
    """Validate that code is syntactically valid Python."""
    errors = []
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append(f"[{name}] Syntax error at line {e.lineno}: {e.msg}")
        # Show context around error
        lines = code.split("\n")
        if e.lineno:
            start = max(0, e.lineno - 3)
            end = min(len(lines), e.lineno + 2)
            for i in range(start, end):
                marker = ">>>" if i + 1 == e.lineno else "   "
                errors.append(f"  {marker} {i+1}: {lines[i]}")
    return errors


def validate_compile(code: str, name: str) -> list:
    """Validate that code compiles without errors."""
    errors = []
    try:
        compile(code, f"{name}.py", "exec")
    except SyntaxError as e:
        errors.append(f"[{name}] Compile error at line {e.lineno}: {e.msg}")
    return errors


def validate_airflow_constructs(code: str, name: str) -> list:
    """Check for required Airflow constructs in generated code."""
    warnings = []

    required_patterns = [
        ("from airflow import DAG", "Missing 'from airflow import DAG' import"),
        ("dag_id=", "Missing dag_id= in DAG definition"),
        ("from dag_utilities", "Missing 'from dag_utilities' import (thin DAG pattern)"),
    ]

    for pattern, message in required_patterns:
        if pattern not in code:
            warnings.append(f"[{name}] WARNING: {message}")

    # Check for production-specific patterns
    production_patterns = [
        ("PythonOperator", "Uses PythonOperator (production pattern)"),
        ("TaskGroup", "Uses TaskGroup (production pattern)"),
        ("generate_pg_conn_info", "Uses generate_pg_conn_info (DB connection)"),
        ("get_spark_tg_wrapper", "Uses get_spark_tg_wrapper (Spark+GE TaskGroup)"),
    ]

    found_production = []
    for pattern, desc in production_patterns:
        if pattern in code:
            found_production.append(desc)

    if not found_production:
        warnings.append(f"[{name}] WARNING: No production patterns found in generated code")

    return warnings


def validate_no_inline_functions(code: str, name: str) -> list:
    """Check that DAG follows thin DAG pattern (no inline function definitions)."""
    warnings = []
    # Parse AST and check for function definitions that aren't inside imports
    try:
        tree = ast.parse(code)
        function_count = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                function_count += 1

        # Production thin DAGs should have minimal inline functions
        # (typically just _source_to_transient, _handle_failure, etc. - all imported)
        # But the template defines helper functions that call into dag_utilities
        if function_count > 20:
            warnings.append(
                f"[{name}] WARNING: {function_count} inline functions found. "
                "Thin DAG pattern should have minimal inline functions."
            )
    except SyntaxError:
        pass  # Already caught by syntax validation

    return warnings


def count_lines(code: str) -> int:
    """Count non-empty lines in code."""
    return len([l for l in code.split("\n") if l.strip()])


# ============================================================================
# Main Test Runner
# ============================================================================

def run_tests():
    """Run all E2E DAG validation tests."""
    print("=" * 70)
    print("E2E DAG VALIDATION - Task 14.1")
    print("=" * 70)

    env = get_jinja_env()
    all_errors = []
    all_warnings = []
    results = {}

    # ---- Test 1: Single Feed Production Template ----
    print("\n--- Test 1: p01_file_production.py.jinja2 (Single Feed) ---")
    try:
        template = env.get_template("p01_file_production.py.jinja2")
        single_code = template.render(**SINGLE_FEED_METADATA)

        lines = count_lines(single_code)
        print(f"  Rendered: {lines} lines, {len(single_code)} chars")
        results["single_feed"] = {"lines": lines, "chars": len(single_code)}

        # Syntax validation
        errors = validate_python_syntax(single_code, "single_feed")
        all_errors.extend(errors)
        if not errors:
            print("  [PASS] Python syntax valid")
        else:
            print(f"  [FAIL] Python syntax errors: {len(errors)}")

        # Compile validation
        errors = validate_compile(single_code, "single_feed")
        all_errors.extend(errors)
        if not errors:
            print("  [PASS] Code compiles successfully")
        else:
            print(f"  [FAIL] Compilation errors: {len(errors)}")

        # Airflow construct validation
        warnings = validate_airflow_constructs(single_code, "single_feed")
        all_warnings.extend(warnings)
        if not [w for w in warnings if "Missing" in w]:
            print("  [PASS] All required Airflow constructs present")
        else:
            for w in warnings:
                print(f"  {w}")

        # Thin DAG pattern check
        warnings = validate_no_inline_functions(single_code, "single_feed")
        all_warnings.extend(warnings)
        if not warnings:
            print("  [PASS] Thin DAG pattern followed")
        else:
            for w in warnings:
                print(f"  {w}")

        # Check specific production features
        features = {
            "GCS File Sensor": "GCSObjectExistenceSensor" in single_code or "file_sensor" in single_code,
            "Business Date Check": "is_posting_date" in single_code or "check_is_posting_date" in single_code,
            "PVC Config": "pvc_config" in single_code or "get_dataproc_pvc_config" in single_code,
            "Zone GCS Buckets": "zone_bucket" in single_code or "build_zone_gcs_paths" in single_code,
            "Rejection Workflow": "rejected" in single_code or "move_to_rejected" in single_code,
            "Environment Emails": "get_environment_recipients" in single_code,
            "Self Re-trigger": "TriggerDagRunOperator" in single_code,
            "Audit Logging": "platform_audit_log" in single_code,
            "GE Validation": "ge_check" in single_code or "check_ge_status" in single_code,
            "Spark TaskGroup": "get_spark_tg_wrapper" in single_code,
        }

        present = sum(1 for v in features.values() if v)
        total = len(features)
        print(f"  Production features: {present}/{total}")
        for feature, found in features.items():
            status = "YES" if found else "NO "
            print(f"    [{status}] {feature}")

        results["single_feed"]["features"] = {k: v for k, v in features.items()}

    except Exception as e:
        all_errors.append(f"[single_feed] Template render FAILED: {str(e)}")
        print(f"  [FAIL] Template render error: {e}")

    # ---- Test 2: Multi-Feed Group Production Template ----
    print("\n--- Test 2: p01_file_group_production.py.jinja2 (Multi-Feed Group) ---")
    try:
        template = env.get_template("p01_file_group_production.py.jinja2")
        group_code = template.render(**GROUP_FEED_METADATA)

        lines = count_lines(group_code)
        print(f"  Rendered: {lines} lines, {len(group_code)} chars")
        results["group_feed"] = {"lines": lines, "chars": len(group_code)}

        # Syntax validation
        errors = validate_python_syntax(group_code, "group_feed")
        all_errors.extend(errors)
        if not errors:
            print("  [PASS] Python syntax valid")
        else:
            print(f"  [FAIL] Python syntax errors: {len(errors)}")

        # Compile validation
        errors = validate_compile(group_code, "group_feed")
        all_errors.extend(errors)
        if not errors:
            print("  [PASS] Code compiles successfully")
        else:
            print(f"  [FAIL] Compilation errors: {len(errors)}")

        # Airflow construct validation
        warnings = validate_airflow_constructs(group_code, "group_feed")
        all_warnings.extend(warnings)
        if not [w for w in warnings if "Missing" in w]:
            print("  [PASS] All required Airflow constructs present")
        else:
            for w in warnings:
                print(f"  {w}")

        # Check group-specific patterns
        group_features = {
            "Group Run ID": "get_group_run_id" in group_code or "group_run_id" in group_code,
            "Group Audit Log": "group_audit_log" in group_code,
            "Feed Loop": "for feed" in group_code or "feed_tg" in group_code,
            "Nested TaskGroup": "TaskGroup" in group_code,
            "Feed Target Details": "get_feed_target_details" in group_code,
        }

        present = sum(1 for v in group_features.values() if v)
        total = len(group_features)
        print(f"  Group features: {present}/{total}")
        for feature, found in group_features.items():
            status = "YES" if found else "NO "
            print(f"    [{status}] {feature}")

        results["group_feed"]["features"] = {k: v for k, v in group_features.items()}

    except Exception as e:
        all_errors.append(f"[group_feed] Template render FAILED: {str(e)}")
        print(f"  [FAIL] Template render error: {e}")

    # ---- Test 3: Base Medallion Template (for auto-generation) ----
    print("\n--- Test 3: base_medallion.py.jinja2 (Template Auto-Gen Base) ---")
    base_template_path = TEMPLATE_DIR / "base_medallion.py.jinja2"
    if base_template_path.exists():
        try:
            template = env.get_template("base_medallion.py.jinja2")
            base_code = template.render(**SINGLE_FEED_METADATA)
            lines = count_lines(base_code)
            print(f"  Rendered: {lines} lines, {len(base_code)} chars")

            errors = validate_python_syntax(base_code, "base_medallion")
            all_errors.extend(errors)
            if not errors:
                print("  [PASS] Python syntax valid")
            else:
                print(f"  [FAIL] Python syntax errors: {len(errors)}")

            results["base_medallion"] = {"lines": lines, "chars": len(base_code)}
        except Exception as e:
            all_errors.append(f"[base_medallion] Template render FAILED: {str(e)}")
            print(f"  [FAIL] Template render error: {e}")
    else:
        print("  [SKIP] base_medallion.py.jinja2 not found")
        results["base_medallion"] = {"status": "not_found"}

    # ---- Test 4: Python File Syntax Checks ----
    print("\n--- Test 4: Python File Syntax Checks (dag_utilities/) ---")
    dag_utils_dir = PROJECT_ROOT / "src" / "dag_utilities"
    checked = 0
    syntax_errors = 0
    for py_file in sorted(dag_utils_dir.rglob("*.py")):
        checked += 1
        try:
            source = py_file.read_text()
            ast.parse(source)
        except SyntaxError as e:
            syntax_errors += 1
            all_errors.append(f"[{py_file.name}] Syntax error at line {e.lineno}: {e.msg}")
            print(f"  [FAIL] {py_file.relative_to(PROJECT_ROOT)}: line {e.lineno}: {e.msg}")

    if syntax_errors == 0:
        print(f"  [PASS] All {checked} Python files in dag_utilities/ are syntax-valid")
    else:
        print(f"  [FAIL] {syntax_errors}/{checked} files have syntax errors")

    results["dag_utilities_syntax"] = {"checked": checked, "errors": syntax_errors}

    # ---- Summary ----
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_errors = len(all_errors)
    total_warnings = len([w for w in all_warnings if "WARNING" in w])

    if total_errors == 0:
        print(f"  RESULT: ALL TESTS PASSED")
    else:
        print(f"  RESULT: {total_errors} ERRORS FOUND")
        for err in all_errors:
            print(f"    - {err}")

    if total_warnings > 0:
        print(f"  WARNINGS: {total_warnings}")
        for w in all_warnings:
            print(f"    - {w}")

    print(f"\n  Templates rendered:")
    for name, info in results.items():
        if isinstance(info, dict) and "lines" in info:
            print(f"    {name}: {info['lines']} lines, {info['chars']} chars")

    print("=" * 70)

    return total_errors == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
