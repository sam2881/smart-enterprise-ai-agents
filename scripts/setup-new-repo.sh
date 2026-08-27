#!/usr/bin/env bash
# Creates the GitHub repo sam2881/ai_agent_app, configures secrets, and pushes code.
# Prerequisites: gh CLI authenticated, git remote NOT yet set to sam2881/ai_agent_app
set -euo pipefail

REPO="sam2881/ai_agent_app"
REGION="us-central1"
PROJECT_ID="agent-ai-test-461120"
AR_REPO="${REGION}-docker.pkg.dev/${PROJECT_ID}/ai-agent-platform"

echo "==> Creating GitHub repo ${REPO} ..."
gh repo create "${REPO}" \
  --private \
  --description "Enterprise AI Agent Platform — Incident Management + Data Engineering" \
  --confirm 2>/dev/null || echo "Repo already exists, continuing."

echo "==> Adding remote and pushing ..."
git remote remove gcp-origin 2>/dev/null || true
git remote add gcp-origin "https://github.com/${REPO}.git"
git push gcp-origin main

echo "==> Seeding GitHub Actions secrets ..."

gh secret set GCP_PROJECT_ID   --body "${PROJECT_ID}"   -R "${REPO}"
gh secret set CLOUD_RUN_REGION --body "${REGION}"       -R "${REPO}"
gh secret set AR_REPO          --body "${AR_REPO}"      -R "${REPO}"

# GCP service-account key (used until Workload Identity Federation is configured)
if [[ -f "gcp-service-account-key.json" ]]; then
  gh secret set GCP_SA_KEY --body "$(cat gcp-service-account-key.json)" -R "${REPO}"
  echo "  GCP_SA_KEY set from gcp-service-account-key.json"
else
  echo "  WARNING: gcp-service-account-key.json not found. Set GCP_SA_KEY manually."
fi

# Workload Identity Federation (set after running Terraform)
echo "  NOTE: Set WIF_PROVIDER and TF_SA_EMAIL / DEPLOY_SA_EMAIL after Terraform apply."
echo "    WIF_PROVIDER = projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL/providers/PROVIDER"
echo "    TF_SA_EMAIL  = terraform@${PROJECT_ID}.iam.gserviceaccount.com"
echo "    DEPLOY_SA_EMAIL = backend-sa-prod@${PROJECT_ID}.iam.gserviceaccount.com"

# Application secrets (values from .env — ROTATE BEFORE RUNNING IN PROD)
echo "==> Seeding application secrets ..."
[[ -f ".env" ]] && source .env || echo "No .env found; skipping application secrets."

set_secret_if_set() {
  local name="$1"
  local value="${2:-}"
  if [[ -n "${value}" ]]; then
    gh secret set "${name}" --body "${value}" -R "${REPO}"
    echo "  ${name} set"
  else
    echo "  SKIPPED ${name} (not in env)"
  fi
}

set_secret_if_set "OPENAI_API_KEY"    "${OPENAI_API_KEY:-}"
set_secret_if_set "ANTHROPIC_API_KEY" "${ANTHROPIC_API_KEY:-}"
set_secret_if_set "SNOW_PASSWORD"     "${SNOW_PASSWORD:-}"
set_secret_if_set "JIRA_API_TOKEN"    "${JIRA_API_TOKEN:-}"
set_secret_if_set "GITHUB_TOKEN_APP"  "${GITHUB_TOKEN:-}"
set_secret_if_set "NEO4J_PASSWORD"    "${NEO4J_PASSWORD:-}"
set_secret_if_set "WEAVIATE_API_KEY"  "${WEAVIATE_API_KEY:-}"
set_secret_if_set "DB_PASSWORD"       "${POSTGRES_PASSWORD:-}"
set_secret_if_set "DB_PASSWORD_PROD"  "${POSTGRES_PASSWORD:-}"

# GitHub environment protection rules
echo "==> Configuring environment protection rules ..."
gh api -X PUT "repos/${REPO}/environments/prod" \
  --field "wait_timer=0" \
  --field 'reviewers=[{"type":"User","id":0}]' 2>/dev/null \
  || echo "  Set environment protection for prod manually in GitHub Settings."

echo ""
echo "==> Done! Next steps:"
echo "  1. Rotate all secrets listed in docs/SECURITY.md"
echo "  2. Run: cd terraform/environments/dev && terraform init && terraform apply -var-file=terraform.tfvars -var=db_password=\$DB_PASSWORD"
echo "  3. Set WIF_PROVIDER, TF_SA_EMAIL, DEPLOY_SA_EMAIL, WORKER_SA_EMAIL as GitHub secrets"
echo "  4. Push a commit to main → CI fires automatically"
echo ""
echo "  Repo: https://github.com/${REPO}"
