#!/usr/bin/env bash
# scripts/infra-up.sh — Provision GCP infrastructure (Linux/Mac/CI)
# Usage:
#   bash scripts/infra-up.sh              # dev
#   bash scripts/infra-up.sh prod         # prod
#   bash scripts/infra-up.sh dev --plan   # dry-run only
set -euo pipefail

ENV="${1:-dev}"; shift || true
PLAN_ONLY=0; [[ "${1:-}" == "--plan" ]] && PLAN_ONLY=1

TF_DIR="$(dirname "$0")/../terraform/environments/$ENV"
STATE_BUCKET="agent-ai-test-461120-tfstate"
PROJECT_ID="agent-ai-test-461120"
REGION="us-central1"

cyan='\033[0;36m'; green='\033[0;32m'; yellow='\033[0;33m'; red='\033[0;31m'; reset='\033[0m'
step() { echo -e "\n${cyan}==> $1${reset}"; }
ok()   { echo -e "  ${green}[OK]  $1${reset}"; }
warn() { echo -e "  ${yellow}[WARN] $1${reset}"; }
fail() { echo -e "  ${red}[FAIL] $1${reset}"; exit 1; }

# ── Preflight ─────────────────────────────────────────────────────────────────
step "Preflight checks"
command -v gcloud    > /dev/null || fail "gcloud CLI not found."
command -v terraform > /dev/null || fail "Terraform not found."
gcloud config get-value account 2>/dev/null | grep -q "@" || fail "Not authenticated. Run: gcloud auth login"
ok "Preflight passed"

# ── DB password ───────────────────────────────────────────────────────────────
if [[ -z "${TF_VAR_db_password:-}" ]]; then
    read -rsp "Enter Cloud SQL password: " TF_VAR_db_password; echo
    export TF_VAR_db_password
fi

# ── State bucket ──────────────────────────────────────────────────────────────
step "Ensure Terraform state bucket"
if ! gsutil ls -b "gs://$STATE_BUCKET" > /dev/null 2>&1; then
    gsutil mb -l "$REGION" "gs://$STATE_BUCKET"
    gsutil versioning set on "gs://$STATE_BUCKET"
    ok "Bucket created: gs://$STATE_BUCKET"
else
    ok "Bucket exists: gs://$STATE_BUCKET"
fi

# ── Enable APIs ───────────────────────────────────────────────────────────────
step "Enable required GCP APIs"
gcloud services enable \
    run.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com \
    sqladmin.googleapis.com redis.googleapis.com container.googleapis.com \
    composer.googleapis.com pubsub.googleapis.com vpcaccess.googleapis.com \
    servicenetworking.googleapis.com monitoring.googleapis.com cloudtrace.googleapis.com \
    logging.googleapis.com iam.googleapis.com \
    --project="$PROJECT_ID" --quiet
ok "APIs enabled"

# ── Terraform ─────────────────────────────────────────────────────────────────
cd "$TF_DIR"

step "Terraform init"
terraform init -reconfigure
ok "Init complete"

step "Terraform plan ($ENV)"
terraform plan -var-file="terraform.tfvars" -out="tfplan-$ENV"
ok "Plan complete"

[[ "$PLAN_ONLY" -eq 1 ]] && { warn "--plan flag set. Stopping before apply."; exit 0; }

step "Terraform apply ($ENV)"
if [[ "$ENV" == "prod" ]]; then
    read -rp "Applying to PRODUCTION. Type 'yes' to confirm: " confirm
    [[ "$confirm" == "yes" ]] || { warn "Aborted."; exit 0; }
fi

terraform apply "tfplan-$ENV"
ok "Apply complete"

step "Infrastructure URLs"
terraform output -json | python3 -c "
import json, sys
o = json.load(sys.stdin)
for k in ['frontend_url','backend_url','data_agent_url','composer_url','gke_cluster','ar_repo','sql_host','redis_host']:
    v = o.get(k,{}).get('value','N/A')
    print(f'  {k:<20}: {v}')
"
echo ""
echo "  Next: populate Secret Manager secrets, then: git push origin main"
