#!/usr/bin/env bash
# scripts/infra-down.sh — Destroy GCP infrastructure (Linux/Mac/CI)
# Usage:
#   bash scripts/infra-down.sh              # dev
#   bash scripts/infra-down.sh prod         # prod (double confirmation)
#   bash scripts/infra-down.sh dev cloud_run  # specific module only
set -euo pipefail

ENV="${1:-dev}"
TARGET="${2:-}"

TF_DIR="$(dirname "$0")/../terraform/environments/$ENV"

cyan='\033[0;36m'; green='\033[0;32m'; yellow='\033[0;33m'; red='\033[0;31m'; reset='\033[0m'
step() { echo -e "\n${cyan}==> $1${reset}"; }
ok()   { echo -e "  ${green}[OK]  $1${reset}"; }
warn() { echo -e "  ${yellow}[WARN] $1${reset}"; }
fail() { echo -e "  ${red}[FAIL] $1${reset}"; exit 1; }

command -v terraform > /dev/null || fail "Terraform not found."

# ── Confirmation ──────────────────────────────────────────────────────────────
if [[ "$ENV" == "prod" ]]; then
    warn "You are about to DESTROY PRODUCTION. Cloud SQL and GKE have deletion_protection=true."
    read -rp "Type 'prod' to confirm: " c1
    [[ "$c1" == "prod" ]] || { warn "Aborted."; exit 0; }
    read -rp "Type 'destroy production': " c2
    [[ "$c2" == "destroy production" ]] || { warn "Aborted."; exit 0; }
else
    read -rp "Destroy dev infrastructure? Type 'yes': " confirm
    [[ "$confirm" == "yes" ]] || { warn "Aborted."; exit 0; }
fi

export TF_VAR_db_password="${TF_VAR_db_password:-placeholder-for-destroy}"

cd "$TF_DIR"
terraform init -reconfigure

if [[ -n "$TARGET" ]]; then
    step "Destroying module: $TARGET"
    terraform destroy -var-file="terraform.tfvars" -target="module.$TARGET" -auto-approve
else
    step "Step 1/4 — Cloud Run"
    terraform destroy -var-file="terraform.tfvars" -target=module.cloud_run -auto-approve

    step "Step 2/4 — Cloud Composer"
    terraform destroy -var-file="terraform.tfvars" -target=module.composer -auto-approve

    step "Step 3/4 — GKE, Monitoring, Pub/Sub"
    terraform destroy -var-file="terraform.tfvars" \
        -target=module.gke -target=module.monitoring -target=module.pubsub -auto-approve

    step "Step 4/4 — Remaining (data layer + networking)"
    terraform destroy -var-file="terraform.tfvars" -auto-approve
fi

ok "Destroy complete for $ENV"
warn "Secrets in Secret Manager are NOT deleted automatically:"
echo "  gcloud secrets list --project=agent-ai-test-461120"
warn "Artifact Registry images are NOT deleted:"
echo "  gcloud artifacts repositories delete ai-agent-platform --location=us-central1 --project=agent-ai-test-461120"
