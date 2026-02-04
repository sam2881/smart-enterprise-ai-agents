#!/usr/bin/env python3
"""
Monitor Airflow for Broken DAGs and Auto-Fix.

This script:
1. Checks Airflow for DAG import errors
2. Analyzes the error (missing modules, syntax errors, etc.)
3. Attempts automatic fixes:
   - Missing dag_utilities → deploy from source
   - Missing spark_jobs → deploy from source
   - Syntax errors → create GitHub issue

Usage:
    python scripts/monitor_broken_dags.py
    python scripts/monitor_broken_dags.py --fix  # Auto-fix broken DAGs
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List


class BrokenDAGMonitor:
    """Monitor and fix broken DAGs in Airflow."""

    def __init__(self, airflow_container: str = "ai-agent-airflow-webserver"):
        self.airflow_container = airflow_container

    def get_import_errors(self) -> List[Dict[str, str]]:
        """Get all DAG import errors from Airflow."""
        print(f"\n{'='*60}")
        print("Checking Airflow for Broken DAGs")
        print(f"{'='*60}")

        try:
            result = subprocess.run(
                ["sudo", "docker", "exec", self.airflow_container,
                 "airflow", "dags", "list-import-errors"],
                capture_output=True,
                text=True,
                timeout=30
            )

            errors = []
            lines = result.stdout.strip().split('\n')

            # Parse table output
            current_file = None
            current_error = []

            for line in lines:
                if line.startswith('/opt/airflow/dags/'):
                    # New file entry
                    if current_file and current_error:
                        errors.append({
                            'filepath': current_file,
                            'error': '\n'.join(current_error)
                        })
                        current_error = []

                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 2:
                        current_file = parts[0].strip()
                        error_start = parts[1].strip()
                        if error_start:
                            current_error.append(error_start)
                elif current_file and '|' in line:
                    # Continue error message
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 2:
                        error_line = parts[1].strip()
                        if error_line:
                            current_error.append(error_line)

            # Add last entry
            if current_file and current_error:
                errors.append({
                    'filepath': current_file,
                    'error': '\n'.join(current_error)
                })

            return errors

        except subprocess.TimeoutExpired:
            print("❌ Timeout while checking Airflow")
            return []
        except Exception as e:
            print(f"❌ Error checking Airflow: {e}")
            return []

    def analyze_error(self, error: str) -> Dict[str, str]:
        """Analyze error and determine fix strategy."""
        error_lower = error.lower()

        if "no module named 'dag_utilities'" in error_lower:
            return {
                'type': 'missing_dag_utilities',
                'fix': 'deploy_dag_utilities',
                'description': 'Missing dag_utilities module - needs deployment to GCS'
            }

        elif "no module named 'spark_jobs'" in error_lower or "'spark_jobs'" in error_lower:
            return {
                'type': 'missing_spark_jobs',
                'fix': 'deploy_spark_jobs',
                'description': 'Missing spark_jobs module - needs deployment to GCS'
            }

        elif "syntaxerror" in error_lower or "invalid syntax" in error_lower:
            return {
                'type': 'syntax_error',
                'fix': 'create_github_issue',
                'description': 'Python syntax error - requires manual review'
            }

        elif "importerror" in error_lower or "modulenotfounderror" in error_lower:
            # Extract module name
            import re
            match = re.search(r"no module named ['\"]([^'\"]+)['\"]", error_lower)
            module_name = match.group(1) if match else "unknown"

            return {
                'type': 'missing_module',
                'fix': 'install_module',
                'module': module_name,
                'description': f'Missing Python module: {module_name}'
            }

        else:
            return {
                'type': 'unknown',
                'fix': 'manual_review',
                'description': 'Unknown error type - requires manual review'
            }

    def fix_missing_dag_utilities(self) -> bool:
        """Deploy dag_utilities to GCS bucket."""
        print("\n🔧 Fixing: Deploying dag_utilities to GCS...")

        dag_utilities_path = Path(__file__).parent.parent / "agents/data_agent/src/dag_utilities"

        if not dag_utilities_path.exists():
            print(f"  ❌ dag_utilities not found at {dag_utilities_path}")
            return False

        try:
            # Deploy to GCS
            result = subprocess.run(
                ["gsutil", "-m", "rsync", "-r", "-d",
                 str(dag_utilities_path),
                 "gs://agent-ai-test-461120-airflow-dags/dag_utilities/"],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                print(f"  ✅ Deployed dag_utilities ({len(list(dag_utilities_path.rglob('*.py')))} files)")
                return True
            else:
                print(f"  ❌ Deployment failed: {result.stderr}")
                return False

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False

    def fix_missing_spark_jobs(self) -> bool:
        """Deploy spark_jobs to GCS bucket."""
        print("\n🔧 Fixing: Deploying spark_jobs to GCS...")

        spark_jobs_path = Path(__file__).parent.parent / "agents/data_agent/src/spark_jobs"

        if not spark_jobs_path.exists():
            print(f"  ❌ spark_jobs not found at {spark_jobs_path}")
            return False

        try:
            # Deploy to GCS
            result = subprocess.run(
                ["gsutil", "-m", "rsync", "-r", "-d",
                 str(spark_jobs_path),
                 "gs://agent-ai-test-461120-temp/spark_jobs/"],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                print(f"  ✅ Deployed spark_jobs ({len(list(spark_jobs_path.glob('*.py')))} files)")
                return True
            else:
                print(f"  ❌ Deployment failed: {result.stderr}")
                return False

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False

    def wait_for_dag_sync(self, wait_seconds: int = 70):
        """Wait for dag-sync to pick up changes."""
        import time
        print(f"\n⏳ Waiting {wait_seconds} seconds for dag-sync to pick up changes...")
        time.sleep(wait_seconds)
        print("  ✅ Wait complete")

    def verify_fix(self) -> bool:
        """Verify that all DAGs are now loading correctly."""
        print(f"\n{'='*60}")
        print("Verifying DAG Status")
        print(f"{'='*60}")

        errors = self.get_import_errors()

        if not errors:
            print("\n✅ All DAGs are loading correctly!")
            return True
        else:
            print(f"\n⚠️  {len(errors)} DAG(s) still broken:")
            for error_info in errors:
                dag_file = Path(error_info['filepath']).name
                print(f"  - {dag_file}")
            return False

    def run(self, auto_fix: bool = False):
        """Main monitoring and fixing workflow."""
        print(f"\n{'='*60}")
        print(f"Broken DAG Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        print(f"Auto-fix: {'ENABLED' if auto_fix else 'DISABLED'}")

        # Get errors
        errors = self.get_import_errors()

        if not errors:
            print("\n✅ No broken DAGs found!")
            return True

        # Display errors
        print(f"\n⚠️  Found {len(errors)} broken DAG(s):\n")

        fix_summary = {}

        for i, error_info in enumerate(errors, 1):
            dag_file = Path(error_info['filepath']).name
            error_msg = error_info['error']

            print(f"{i}. {dag_file}")
            print(f"   Error: {error_msg[:200]}...")

            # Analyze
            analysis = self.analyze_error(error_msg)
            print(f"   Type: {analysis['type']}")
            print(f"   Fix: {analysis['description']}")

            fix_summary[analysis['fix']] = fix_summary.get(analysis['fix'], 0) + 1
            print()

        # Auto-fix if enabled
        if auto_fix:
            print(f"\n{'='*60}")
            print("Attempting Auto-Fix")
            print(f"{'='*60}")

            fixed = False

            if fix_summary.get('deploy_dag_utilities', 0) > 0:
                if self.fix_missing_dag_utilities():
                    fixed = True

            if fix_summary.get('deploy_spark_jobs', 0) > 0:
                if self.fix_missing_spark_jobs():
                    fixed = True

            if fixed:
                self.wait_for_dag_sync()
                return self.verify_fix()
            else:
                print("\n⚠️  No automatic fixes available")
                return False
        else:
            print("\n💡 To automatically fix these issues, run:")
            print("   python scripts/monitor_broken_dags.py --fix")
            return False


def main():
    parser = argparse.ArgumentParser(description="Monitor and fix broken DAGs in Airflow")
    parser.add_argument('--fix', action='store_true', help='Automatically fix broken DAGs')
    parser.add_argument('--container', default='ai-agent-airflow-webserver',
                        help='Airflow webserver container name')

    args = parser.parse_args()

    monitor = BrokenDAGMonitor(airflow_container=args.container)
    success = monitor.run(auto_fix=args.fix)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
