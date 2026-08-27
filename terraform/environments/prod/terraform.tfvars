# ── Prod environment variables ───────────────────────────────────────────────
# Run scripts/setup-gcp.sh (or setup-gcp.ps1 on Windows) to auto-generate
# this file. Or fill in manually and run: bash scripts/infra-up.sh prod

project_id  = "YOUR_GCP_PROJECT_ID"   # same project, different env suffix
region      = "us-central1"
zone        = "us-central1-a"
environment = "prod"
alert_email = "YOUR_EMAIL@example.com" # receives cost + alert notifications
db_tier     = "db-custom-2-7680"       # 2 vCPU / 7.5 GB for production load
