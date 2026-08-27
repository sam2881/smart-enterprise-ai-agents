# ── Dev environment variables ────────────────────────────────────────────────
# Run scripts/setup-gcp.sh (or setup-gcp.ps1 on Windows) to auto-generate
# this file. Or fill in manually and run: bash scripts/infra-up.sh dev

project_id  = "YOUR_GCP_PROJECT_ID"   # e.g. "my-company-ai-dev"
region      = "us-central1"
zone        = "us-central1-a"
environment = "dev"
alert_email = "YOUR_EMAIL@example.com" # receives cost + alert notifications
db_tier     = "db-g1-small"            # cheapest tier, fine for dev
