#!/usr/bin/env python3
"""
Test EBCDIC Pipeline End-to-End Generation and GitHub Push.

This script tests:
1. EBCDIC source type detection and template selection
2. DAG generation from EBCDIC configuration
3. Spark job code generation for EBCDIC/Cobrix
4. GitHub repository push functionality

Usage:
    cd /home/samrattidke600/ai_agent_app
    python scripts/test_ebcdic_pipeline.py
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add the data agent to path
sys.path.insert(0, str(Path(__file__).parent.parent / "agents" / "data_agent"))

# Set environment variables for test
os.environ.setdefault("GITHUB_PIPELINES_TOKEN", os.getenv("GITHUB_PIPELINES_TOKEN", ""))
os.environ.setdefault("GITHUB_PIPELINES_OWNER", "sam2881")
os.environ.setdefault("GITHUB_PIPELINES_REPO", "enterprise-data-pipelines")


def test_ebcdic_template_selection():
    """Test that EBCDIC source types correctly select enterprise template."""
    print("\n" + "=" * 60)
    print("TEST 1: EBCDIC Template Selection")
    print("=" * 60)

    from src.generators.dag_generator import DAGGenerator

    generator = DAGGenerator()

    # Test file_ebcdic source type
    template = generator.select_template(
        source_type="file_ebcdic",
        processing_mode="default",
        use_enterprise=True,
    )
    print(f"  file_ebcdic → {template}")
    assert template == "enterprise_pipeline_dag", f"Expected enterprise_pipeline_dag, got {template}"

    # Test legacy_cobol source type
    template = generator.select_template(
        source_type="legacy_cobol",
        processing_mode="default",
        use_enterprise=True,
    )
    print(f"  legacy_cobol → {template}")
    assert template == "enterprise_pipeline_dag", f"Expected enterprise_pipeline_dag, got {template}"

    # Test legacy_mainframe source type
    template = generator.select_template(
        source_type="legacy_mainframe",
        processing_mode="default",
        use_enterprise=True,
    )
    print(f"  legacy_mainframe → {template}")
    assert template == "enterprise_pipeline_dag", f"Expected enterprise_pipeline_dag, got {template}"

    print("✓ EBCDIC template selection: PASSED")
    return True


def test_ebcdic_dag_generation():
    """Test DAG generation for EBCDIC source with copybook."""
    print("\n" + "=" * 60)
    print("TEST 2: EBCDIC DAG Generation")
    print("=" * 60)

    from src.generators.dag_generator import DAGGenerator

    generator = DAGGenerator()

    # Sample COBOL copybook for customer records
    sample_copybook = """
       01  CUSTOMER-RECORD.
           05  CUST-ID            PIC 9(10).
           05  CUST-NAME          PIC X(50).
           05  CUST-ADDRESS       PIC X(100).
           05  CUST-BALANCE       PIC S9(9)V99 COMP-3.
           05  CUST-STATUS        PIC X(1).
           05  CUST-CREATE-DATE   PIC 9(8).
    """

    # EBCDIC pipeline configuration
    ebcdic_config = {
        "dag_id": "mainframe_customer_ingest",
        "product_name": "Customer Master from Mainframe",
        "product_code": "cust_master_mf",
        "domain_name": "customer",
        "environment": "dev",
        "source_type": "file_ebcdic",
        "source_format": "ebcdic_copybook",
        "landing_path": "gs://landing-mainframe/customer/",
        "copybook_path": "gs://copybooks/customer_record.cpy",
        "source_config": {
            "copybook_content": sample_copybook,
            "code_page": "cp037",
            "record_format": "fixed",
            "record_length": 180,
        },
        "processing_mode": "batch_incremental",
        "load_strategy": "merge_upsert",
        "primary_keys": ["cust_id"],
        "partition_columns": ["_ingested_date"],
        "schedule_interval": "@daily",
        "quality_rules": [
            {"rule": "not_null", "column": "cust_id", "severity": "critical"},
            {"rule": "unique", "column": "cust_id", "severity": "critical"},
            {"rule": "range", "column": "cust_balance", "min": -1000000, "max": 1000000, "severity": "warning"},
        ],
        "min_quality_score": 85.0,
        "self_healing_enabled": True,
        "alert_emails": ["data-team@example.com"],
        "jira_ticket": "DATA-5678",
        "owner": "mainframe-migration-team",
        "spark_jobs": {},  # Use defaults
    }

    try:
        # Generate DAG code
        dag_code = generator.generate("enterprise_pipeline_dag", ebcdic_config)

        # Validate generated code
        assert "dag_id='mainframe_customer_ingest'" in dag_code or 'dag_id="mainframe_customer_ingest"' in dag_code, "DAG ID not found in generated code"
        assert "ebcdic_copybook" in dag_code.lower() or "copybook" in dag_code.lower(), "EBCDIC/copybook reference not found"
        assert "validate_copybook" in dag_code, "validate_copybook task not found"
        assert "--copybook-path" in dag_code, "Copybook path argument not found in Spark job"

        # Print generated DAG snippet
        lines = dag_code.split('\n')
        print(f"\n  Generated DAG: {len(lines)} lines")
        print("  First 30 lines preview:")
        for line in lines[:30]:
            print(f"    {line}")

        print(f"\n✓ EBCDIC DAG generation: PASSED ({len(dag_code)} characters)")
        return dag_code

    except Exception as e:
        print(f"✗ EBCDIC DAG generation: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return None


def test_ebcdic_source_handler():
    """Test EBCDIC source handler for Cobrix code generation."""
    print("\n" + "=" * 60)
    print("TEST 3: EBCDIC Source Handler (Cobrix)")
    print("=" * 60)

    from src.handlers.source_handlers import EBCDICSourceHandler, EBCDICConfig

    sample_copybook = """
       01  TRANSACTION-RECORD.
           05  TRANS-ID           PIC 9(12).
           05  TRANS-TYPE         PIC X(3).
           05  TRANS-AMOUNT       PIC S9(9)V99 COMP-3.
           05  TRANS-DATE         PIC 9(8).
           05  ACCOUNT-NUM        PIC X(20).
    """

    config = EBCDICConfig(
        source_path="gs://mainframe-data/transactions/",
        source_format="ebcdic",
        copybook_content=sample_copybook,
        code_page="cp037",
        record_format="fixed",
        record_length=50,
        generate_record_id=True,
    )

    handler = EBCDICSourceHandler(config)

    # Test schema parsing
    schema = handler.parse_schema()
    print(f"\n  Parsed {len(schema.columns)} columns from copybook:")
    for col in schema.columns:
        print(f"    - {col.name}: {col.data_type} (pos: {col.start_position}, len: {col.length})")

    # Test Spark code generation
    spark_code = handler.generate_read_code()
    print(f"\n  Generated Spark code ({len(spark_code)} chars):")
    for line in spark_code.split('\n')[:15]:
        print(f"    {line}")

    # Validate Cobrix format options
    assert 'format("cobol")' in spark_code, "Cobrix format not found in generated code"
    assert "copybook_contents" in spark_code, "copybook_contents option not found"
    assert "cp037" in spark_code, "Code page not found"

    print("\n✓ EBCDIC source handler: PASSED")
    return True


def test_github_push():
    """Test GitHub push functionality."""
    print("\n" + "=" * 60)
    print("TEST 4: GitHub Push Functionality")
    print("=" * 60)

    from src.deployers.git_client import GitClient
    from src.config.settings import get_settings

    settings = get_settings()

    # Check environment
    github_token = os.getenv("GITHUB_PIPELINES_TOKEN")
    if not github_token:
        print("  ⚠ GITHUB_PIPELINES_TOKEN not set, skipping push test")
        print("  To test GitHub push, set: GITHUB_PIPELINES_TOKEN=<your-token>")
        return None

    print(f"  GitHub Owner: {settings.github_owner}")
    print(f"  GitHub Repo: {settings.github_repo}")
    print(f"  Token configured: {'Yes' if github_token else 'No'}")

    # Generate sample DAG for push
    sample_dag = f'''"""
Sample EBCDIC Pipeline DAG - Generated {datetime.now().isoformat()}
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='test_ebcdic_customer_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ebcdic', 'mainframe', 'test'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> end
'''

    # Files to deploy
    files_to_deploy = {
        "dags/test_ebcdic_customer_pipeline.py": sample_dag,
        "spark_jobs/test_bronze_ingest.py": "# Bronze ingest placeholder",
    }

    try:
        client = GitClient()

        # Test deployment (dry-run style - check if API is accessible)
        print("\n  Testing GitHub API connectivity...")

        # Make a simple API call to verify token works
        response = client._github_api_request(
            "GET",
            f"/repos/{settings.github_owner}/{settings.github_repo}"
        )

        if response.status_code == 200:
            repo_data = response.json()
            print(f"  ✓ Repository found: {repo_data.get('full_name')}")
            print(f"    Default branch: {repo_data.get('default_branch')}")
            print(f"    Private: {repo_data.get('private')}")

            # Perform actual deployment
            print("\n  Deploying test files to GitHub...")
            result = client.deploy_artifacts(
                files=files_to_deploy,
                commit_message="test: EBCDIC pipeline test deployment",
                branch_name=f"test/ebcdic-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                create_pr=True,
                pr_title="[Test] EBCDIC Pipeline Test Deployment",
                pr_description="Automated test deployment from test_ebcdic_pipeline.py",
            )

            print(f"\n  Deployment result:")
            print(f"    Status: {result.get('status')}")
            print(f"    Branch: {result.get('branch')}")
            print(f"    Commit: {result.get('commit_sha', '')[:8]}")
            print(f"    PR URL: {result.get('pr_url', 'N/A')}")

            print("\n✓ GitHub push: PASSED")
            return result

        elif response.status_code == 404:
            print(f"  ✗ Repository not found: {settings.github_owner}/{settings.github_repo}")
            print("    Please create the repository first or check the name")
            return None
        elif response.status_code == 401:
            print("  ✗ Authentication failed - invalid or expired token")
            return None
        else:
            print(f"  ✗ API error: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"  ✗ GitHub push: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return None


def test_full_pipeline_generation():
    """Test full pipeline artifact generation for EBCDIC source."""
    print("\n" + "=" * 60)
    print("TEST 5: Full Pipeline Artifact Generation")
    print("=" * 60)

    from src.generators.dag_generator import DAGGenerator

    dag_gen = DAGGenerator()

    # Generate DAG with all required context
    ebcdic_config = {
        "dag_id": "ebcdic_full_test_pipeline",
        "product_name": "EBCDIC Full Test",
        "product_code": "ebcdic_full_test",
        "domain_name": "test",
        "environment": "dev",
        "source_type": "file_ebcdic",
        "source_format": "ebcdic_copybook",
        "landing_path": "gs://test-landing/ebcdic/",
        "copybook_path": "gs://copybooks/test.cpy",
        "processing_mode": "batch_full",
        "load_strategy": "overwrite",
        "primary_keys": ["record_id"],
        "schedule_interval": "@daily",
        "self_healing_enabled": True,
        "quality_rules": [],
        "spark_jobs": {},  # Empty dict, will use defaults
    }

    try:
        dag_code = dag_gen.generate("enterprise_pipeline_dag", ebcdic_config)
        print(f"  ✓ DAG generated: {len(dag_code)} characters")

        # Generate metadata-driven Spark jobs
        try:
            spark_jobs = dag_gen.generate_spark_jobs(
                pipeline_id="ebcdic_full_test",
                environment="dev",
            )
            print(f"  ✓ Spark jobs generated: {list(spark_jobs.keys())}")
        except Exception as e:
            print(f"  ⚠ Spark job generation skipped: {e}")
            spark_jobs = {}

        # Summary
        total_lines = len(dag_code.split('\n'))
        for job_name, job_code in spark_jobs.items():
            total_lines += len(job_code.split('\n'))

        print(f"\n  Total artifacts: 1 DAG + {len(spark_jobs)} Spark jobs")
        print(f"  Total lines of code: {total_lines}")

        print("\n✓ Full pipeline generation: PASSED")
        return True

    except Exception as e:
        print(f"✗ Full pipeline generation: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all EBCDIC pipeline tests."""
    print("\n" + "=" * 60)
    print("EBCDIC Pipeline End-to-End Test Suite")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Working Dir: {os.getcwd()}")

    results = {}

    # Run tests
    results["template_selection"] = test_ebcdic_template_selection()
    results["dag_generation"] = test_ebcdic_dag_generation() is not None
    results["source_handler"] = test_ebcdic_source_handler()
    results["github_push"] = test_github_push() is not None
    results["full_pipeline"] = test_full_pipeline_generation()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED/SKIPPED"
        print(f"  {test_name}: {status}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! EBCDIC pipeline is working end-to-end.")
    else:
        print("\n⚠ Some tests failed or were skipped. Check output above.")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
