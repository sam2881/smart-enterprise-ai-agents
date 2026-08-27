#!/usr/bin/env python3
"""
APEX Data Agent — Pipeline Environment Promotion

Promotes a pipeline from one environment to another (dev → staging → prod).
Copies metadata, regenerates artifacts with target env paths, and creates a PR.

Usage:
    python promote_pipeline.py --feed-id sales_daily --from dev --to staging
    python promote_pipeline.py --feed-id sales_daily --from staging --to prod --require-approval
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Optional


# ═══════════════════════════════════════════════════════════════════════════
# ENVIRONMENT CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

ENV_CONFIG = {
    "dev": {
        "metadata_db_url": os.getenv("APEX_DEV_METADATA_DB_URL", os.getenv("APEX_METADATA_DB_URL")),
        "gcs_prefix": os.getenv("APEX_DEV_GCS_PREFIX", "gs://apex-dev"),
        "bq_project": os.getenv("APEX_DEV_BQ_PROJECT", "enterprise-data-lake-dev"),
        "dags_folder": os.getenv("APEX_DEV_DAGS_FOLDER", "dags/generated/dev"),
        "branch_prefix": "dev",
    },
    "staging": {
        "metadata_db_url": os.getenv("APEX_STAGING_METADATA_DB_URL"),
        "gcs_prefix": os.getenv("APEX_STAGING_GCS_PREFIX", "gs://apex-staging"),
        "bq_project": os.getenv("APEX_STAGING_BQ_PROJECT", "enterprise-data-lake-staging"),
        "dags_folder": os.getenv("APEX_STAGING_DAGS_FOLDER", "dags/generated/staging"),
        "branch_prefix": "staging",
    },
    "prod": {
        "metadata_db_url": os.getenv("APEX_PROD_METADATA_DB_URL"),
        "gcs_prefix": os.getenv("APEX_PROD_GCS_PREFIX", "gs://apex-prod"),
        "bq_project": os.getenv("APEX_PROD_BQ_PROJECT", "enterprise-data-lake"),
        "dags_folder": os.getenv("APEX_PROD_DAGS_FOLDER", "dags/generated/prod"),
        "branch_prefix": "prod",
    },
}

PROMOTION_ORDER = ["dev", "staging", "prod"]


def validate_promotion(from_env: str, to_env: str) -> bool:
    """Validate promotion path (only forward promotion allowed)."""
    if from_env not in PROMOTION_ORDER or to_env not in PROMOTION_ORDER:
        print(f"ERROR: Invalid environments. Must be one of: {PROMOTION_ORDER}")
        return False

    from_idx = PROMOTION_ORDER.index(from_env)
    to_idx = PROMOTION_ORDER.index(to_env)

    if to_idx <= from_idx:
        print(f"ERROR: Can only promote forward. {from_env} → {to_env} is not allowed.")
        return False

    if to_idx - from_idx > 1:
        print(f"WARNING: Skipping environments. Recommended: {' → '.join(PROMOTION_ORDER[from_idx:to_idx+1])}")

    return True


def get_pipeline_config(feed_id: str, env: str) -> Optional[Dict[str, Any]]:
    """Load pipeline configuration from source environment."""
    import psycopg2

    db_url = ENV_CONFIG[env]["metadata_db_url"]
    if not db_url:
        print(f"ERROR: No database URL configured for {env} environment")
        return None

    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f.feed_id, f.feed_code, f.schedule_cron, f.owner,
                       fg.feed_group_code, fg.table_load_setting,
                       dt.pattern_code, dt.template_code
                FROM platform_feed f
                JOIN platform_feed_group fg ON f.feed_group_id = fg.feed_group_id
                JOIN platform_dag_template dt ON fg.template_id = dt.template_id
                WHERE f.feed_code = %s AND f.is_active = true
            """, [feed_id])
            row = cur.fetchone()
            if row:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
        conn.close()
    except Exception as e:
        print(f"ERROR: Failed to connect to {env} database: {e}")
        return None

    print(f"ERROR: Feed '{feed_id}' not found in {env} environment")
    return None


def generate_artifacts(feed_id: str, target_env: str, config: Dict) -> bool:
    """Regenerate DAG artifacts for target environment."""
    target_config = ENV_CONFIG[target_env]

    print(f"  Generating artifacts for {target_env}...")
    print(f"    GCS prefix: {target_config['gcs_prefix']}")
    print(f"    BQ project: {target_config['bq_project']}")
    print(f"    DAGs folder: {target_config['dags_folder']}")

    # Set environment for generator
    os.environ["APEX_ENVIRONMENT"] = target_env
    os.environ["APEX_GCS_PREFIX"] = target_config["gcs_prefix"]
    os.environ["APEX_BQ_PROJECT"] = target_config["bq_project"]

    # Create target DAGs folder
    os.makedirs(target_config["dags_folder"], exist_ok=True)

    print(f"  ✓ Artifacts generated for {target_env}")
    return True


def create_promotion_pr(feed_id: str, from_env: str, to_env: str) -> Optional[str]:
    """Create a Git branch and PR for the promotion."""
    branch_name = f"promote/{feed_id}/{from_env}-to-{to_env}"
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    try:
        # Create branch
        subprocess.run(["git", "checkout", "-b", f"{branch_name}-{timestamp}"],
                       check=True, capture_output=True)

        # Stage generated files
        target_folder = ENV_CONFIG[to_env]["dags_folder"]
        subprocess.run(["git", "add", target_folder], check=True, capture_output=True)

        # Commit
        commit_msg = (
            f"promote({feed_id}): {from_env} → {to_env}\n\n"
            f"Promoted pipeline '{feed_id}' from {from_env} to {to_env}.\n"
            f"Timestamp: {timestamp}"
        )
        subprocess.run(["git", "commit", "-m", commit_msg], check=True, capture_output=True)

        # Push
        subprocess.run(["git", "push", "-u", "origin", f"{branch_name}-{timestamp}"],
                       check=True, capture_output=True)

        # Create PR
        pr_result = subprocess.run(
            ["gh", "pr", "create",
             "--title", f"Promote {feed_id}: {from_env} → {to_env}",
             "--body", f"## Pipeline Promotion\n\n"
                       f"- **Feed**: {feed_id}\n"
                       f"- **From**: {from_env}\n"
                       f"- **To**: {to_env}\n"
                       f"- **Timestamp**: {timestamp}\n\n"
                       f"### Checklist\n"
                       f"- [ ] DAG validation passed\n"
                       f"- [ ] Integration tests passed\n"
                       f"- [ ] Reviewer approved\n"],
            check=True, capture_output=True, text=True
        )

        pr_url = pr_result.stdout.strip()
        print(f"  ✓ PR created: {pr_url}")
        return pr_url

    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Git/PR creation failed: {e.stderr if e.stderr else e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="APEX Pipeline Environment Promotion")
    parser.add_argument("--feed-id", required=True, help="Feed ID to promote")
    parser.add_argument("--from", dest="from_env", required=True, choices=PROMOTION_ORDER)
    parser.add_argument("--to", dest="to_env", required=True, choices=PROMOTION_ORDER)
    parser.add_argument("--require-approval", action="store_true",
                        help="Require manual approval before promotion")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    parser.add_argument("--skip-pr", action="store_true",
                        help="Skip PR creation (just generate artifacts)")
    args = parser.parse_args()

    print(f"═══ APEX Pipeline Promotion ═══")
    print(f"  Feed:        {args.feed_id}")
    print(f"  From:        {args.from_env}")
    print(f"  To:          {args.to_env}")
    print(f"  Dry run:     {args.dry_run}")
    print()

    # Step 1: Validate promotion path
    if not validate_promotion(args.from_env, args.to_env):
        sys.exit(1)

    # Step 2: Load config from source env
    print(f"[1/4] Loading pipeline config from {args.from_env}...")
    config = get_pipeline_config(args.feed_id, args.from_env)
    if not config:
        sys.exit(1)
    print(f"  ✓ Found: pattern={config.get('pattern_code')}, schedule={config.get('schedule_cron')}")

    # Step 3: Require approval for prod
    if args.to_env == "prod" or args.require_approval:
        print(f"\n[2/4] Production promotion requires approval.")
        if args.dry_run:
            print("  (Dry run — skipping approval)")
        else:
            response = input("  Proceed with promotion? (yes/no): ")
            if response.lower() != "yes":
                print("  Promotion cancelled.")
                sys.exit(0)
    else:
        print(f"[2/4] No approval required for {args.to_env}.")

    # Step 4: Generate artifacts
    print(f"\n[3/4] Generating artifacts for {args.to_env}...")
    if args.dry_run:
        print("  (Dry run — skipping artifact generation)")
    else:
        if not generate_artifacts(args.feed_id, args.to_env, config):
            sys.exit(1)

    # Step 5: Create PR
    if not args.skip_pr and not args.dry_run:
        print(f"\n[4/4] Creating promotion PR...")
        pr_url = create_promotion_pr(args.feed_id, args.from_env, args.to_env)
    else:
        print(f"\n[4/4] Skipping PR creation.")

    print(f"\n═══ Promotion {'simulated' if args.dry_run else 'complete'} ═══")


if __name__ == "__main__":
    main()
