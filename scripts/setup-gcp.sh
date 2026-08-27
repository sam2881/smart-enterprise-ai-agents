#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/setup-gcp.sh — First-time GCP project setup wizard
#
# What this script does (all idempotent — safe to re-run):
#   1. Validates gcloud + terraform CLI are installed
#   2. Prompts for project ID, region, alert email, GitHub org/repo
#   3. Creates the Terraform state GCS bucket
#   4. Enables all 16 required GCP APIs
#   5. Creates 3 service accounts (terraform-sa, deploy-sa, worker-sa)
#   6. Grants IAM roles to each service account
#   7. Sets up Workload Identity Federation for GitHub Actions (keyless auth)
#   8. Generates terraform/environments/{dev,prod}/terraform.tfvars
#   9. Generates .github/workflows/wif-config.env (WIF values for secrets)
#  10. Prints the exact GitHub Secrets table you need to fill in
#
# Usage:
#   bash scripts/setup-gcp.sh
#
# Prerequisites:
#   gcloud CLI installed and authenticated (gcloud auth login)
#   terraform >= 1.5 installed
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; BOLD='\033[1m'; RESET='\033[0m'

step()  { echo -e "\n${CYAN}${BOLD}==> $1${RESET}"; }
ok()    { echo -e "  ${GREEN}[OK]${RESET}  $1"; }
warn()  { echo -e "  ${YELLOW}[WARN]${RESET} $1"; }
fail()  { echo -e "  ${RED}[FAIL]${RESET} $1"; exit 1; }
prompt(){ echo -e "${BOLD}$1${RESET}"; }

# ── Preflight ─────────────────────────────────────────────────────────────────
step "Preflight checks"
command -v gcloud    >/dev/null 2>&1 || fail "gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install"
command -v terraform >/dev/null 2>&1 || fail "Terraform not found. Install: https://developer.hashicorp.com/terraform/downloads"
command -v gh        >/dev/null 2>&1 && GH_AVAILABLE=true || GH_AVAILABLE=false

ACCOUNT=$(gcloud config get-value account 2>/dev/null || true)
[[ "$ACCOUNT" == *"@"* ]] || fail "Not authenticated. Run: gcloud auth login && gcloud auth application-default login"
ok "Authenticated as: $ACCOUNT"

# ── Collect inputs ────────────────────────────────────────────────────────────
step "Configuration"
echo ""

prompt "GCP Project ID (existing project, e.g. 'my-company-ai-dev'):"
read -r PROJECT_ID
[[ -n "$PROJECT_ID" ]] || fail "Project ID is required."

prompt "GCP Region [us-central1]:"
read -r REGION
REGION="${REGION:-us-central1}"

prompt "GCP Zone [${REGION}-a]:"
read -r ZONE
ZONE="${ZONE:-${REGION}-a}"

prompt "Alert email (receives billing and monitoring alerts):"
read -r ALERT_EMAIL
[[ "$ALERT_EMAIL" == *"@"* ]] || fail "Invalid email address."

prompt "GitHub organization or username (for Workload Identity Federation):"
read -r GITHUB_ORG
[[ -n "$GITHUB_ORG" ]] || fail "GitHub org/user is required."

prompt "GitHub repository name (e.g. 'enterprise-agentic-platform'):"
read -r GITHUB_REPO
[[ -n "$GITHUB_REPO" ]] || fail "GitHub repo is required."

STATE_BUCKET="${PROJECT_ID}-tfstate"

echo ""
echo -e "${BOLD}Summary${RESET}"
echo "  Project    : $PROJECT_ID"
echo "  Region     : $REGION / $ZONE"
echo "  Alert email: $ALERT_EMAIL"
echo "  GitHub     : $GITHUB_ORG/$GITHUB_REPO"
echo "  TF bucket  : gs://$STATE_BUCKET"
echo ""
read -rp "Proceed? [y/N] " CONFIRM
[[ "${CONFIRM,,}" == "y" ]] || { echo "Aborted."; exit 0; }

gcloud config set project "$PROJECT_ID" --quiet

# ── Terraform state bucket ────────────────────────────────────────────────────
step "Terraform state bucket"
if gsutil ls -b "gs://$STATE_BUCKET" >/dev/null 2>&1; then
    ok "Bucket already exists: gs://$STATE_BUCKET"
else
    gsutil mb -l "$REGION" "gs://$STATE_BUCKET"
    gsutil versioning set on "gs://$STATE_BUCKET"
    ok "Created: gs://$STATE_BUCKET (versioning enabled)"
fi

# ── Enable APIs ───────────────────────────────────────────────────────────────
step "Enable required GCP APIs (this takes ~2 minutes)"
gcloud services enable \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    secretmanager.googleapis.com \
    sqladmin.googleapis.com \
    redis.googleapis.com \
    container.googleapis.com \
    composer.googleapis.com \
    pubsub.googleapis.com \
    vpcaccess.googleapis.com \
    servicenetworking.googleapis.com \
    monitoring.googleapis.com \
    cloudtrace.googleapis.com \
    logging.googleapis.com \
    iam.googleapis.com \
    iamcredentials.googleapis.com \
    modelarmor.googleapis.com \
    --project="$PROJECT_ID" --quiet
ok "All APIs enabled"

# ── Service accounts ──────────────────────────────────────────────────────────
step "Create service accounts"

create_sa() {
    local NAME="$1" DISPLAY="$2"
    local EMAIL="${NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
    if gcloud iam service-accounts describe "$EMAIL" --project="$PROJECT_ID" >/dev/null 2>&1; then
        ok "SA already exists: $EMAIL"
    else
        gcloud iam service-accounts create "$NAME" --display-name="$DISPLAY" --project="$PROJECT_ID"
        ok "Created SA: $EMAIL"
    fi
    echo "$EMAIL"
}

TF_SA=$(create_sa "terraform-sa"  "Terraform Provisioner")
DEPLOY_SA=$(create_sa "deploy-sa"  "CI/CD Deployer")
WORKER_SA=$(create_sa "worker-sa"  "Runtime Worker")

# Terraform SA — broad provisioner rights
TF_ROLES=(
    roles/owner
)
for ROLE in "${TF_ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$TF_SA" --role="$ROLE" --quiet >/dev/null
done
ok "Terraform SA: roles/owner granted"

# Deploy SA — Cloud Run + Artifact Registry
DEPLOY_ROLES=(
    roles/run.admin
    roles/artifactregistry.writer
    roles/secretmanager.secretAccessor
    roles/iam.serviceAccountUser
)
for ROLE in "${DEPLOY_ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$DEPLOY_SA" --role="$ROLE" --quiet >/dev/null
done
ok "Deploy SA: Cloud Run + Artifact Registry roles granted"

# Worker SA — runtime data access
WORKER_ROLES=(
    roles/bigquery.dataEditor
    roles/storage.objectAdmin
    roles/secretmanager.secretAccessor
    roles/pubsub.publisher
    roles/pubsub.subscriber
    roles/cloudsql.client
)
for ROLE in "${WORKER_ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$WORKER_SA" --role="$ROLE" --quiet >/dev/null
done
ok "Worker SA: BigQuery + GCS + PubSub + SQL roles granted"

# ── Workload Identity Federation ──────────────────────────────────────────────
step "Workload Identity Federation (GitHub Actions → GCP, no long-lived keys)"

POOL_ID="github-actions-pool"
PROVIDER_ID="github-provider"
POOL_NAME="projects/${PROJECT_ID}/locations/global/workloadIdentityPools/${POOL_ID}"
PROVIDER_NAME="${POOL_NAME}/providers/${PROVIDER_ID}"

# Create pool
if gcloud iam workload-identity-pools describe "$POOL_ID" \
    --location=global --project="$PROJECT_ID" >/dev/null 2>&1; then
    ok "WIF pool already exists: $POOL_ID"
else
    gcloud iam workload-identity-pools create "$POOL_ID" \
        --location=global \
        --display-name="GitHub Actions Pool" \
        --project="$PROJECT_ID"
    ok "Created WIF pool: $POOL_ID"
fi

# Create provider
if gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
    --workload-identity-pool="$POOL_ID" \
    --location=global --project="$PROJECT_ID" >/dev/null 2>&1; then
    ok "WIF provider already exists: $PROVIDER_ID"
else
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
        --workload-identity-pool="$POOL_ID" \
        --location=global \
        --issuer-uri="https://token.actions.githubusercontent.com" \
        --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor" \
        --attribute-condition="assertion.repository=='${GITHUB_ORG}/${GITHUB_REPO}'" \
        --project="$PROJECT_ID"
    ok "Created WIF provider for: $GITHUB_ORG/$GITHUB_REPO"
fi

# Get numeric project number for WIF provider resource name
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
WIF_PROVIDER_FULL="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"

# Bind WIF → deploy-sa
gcloud iam service-accounts add-iam-policy-binding "$DEPLOY_SA" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${POOL_NAME}/attribute.repository/${GITHUB_ORG}/${GITHUB_REPO}" \
    --project="$PROJECT_ID" --quiet >/dev/null

# Bind WIF → terraform-sa (for terraform-apply workflow)
gcloud iam service-accounts add-iam-policy-binding "$TF_SA" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${POOL_NAME}/attribute.repository/${GITHUB_ORG}/${GITHUB_REPO}" \
    --project="$PROJECT_ID" --quiet >/dev/null

ok "WIF bound to deploy-sa and terraform-sa"

# ── Artifact Registry repository ──────────────────────────────────────────────
step "Artifact Registry repository"
AR_REPO_NAME="ai-agent-platform"
AR_FULL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO_NAME}"

if gcloud artifacts repositories describe "$AR_REPO_NAME" \
    --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    ok "Artifact Registry already exists: $AR_FULL"
else
    gcloud artifacts repositories create "$AR_REPO_NAME" \
        --repository-format=docker \
        --location="$REGION" \
        --description="AI Agent Platform Docker images" \
        --project="$PROJECT_ID"
    ok "Created: $AR_FULL"
fi

# ── Generate terraform.tfvars ─────────────────────────────────────────────────
step "Generate terraform.tfvars"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

for ENV in dev prod; do
    TFVARS="${REPO_ROOT}/terraform/environments/${ENV}/terraform.tfvars"
    DB_TIER="db-g1-small"
    [[ "$ENV" == "prod" ]] && DB_TIER="db-custom-2-7680"

    cat > "$TFVARS" <<EOF
# Auto-generated by scripts/setup-gcp.sh — $(date -u +"%Y-%m-%d %H:%M UTC")
# Re-run the script to regenerate, or edit manually.

project_id  = "${PROJECT_ID}"
region      = "${REGION}"
zone        = "${ZONE}"
environment = "${ENV}"
alert_email = "${ALERT_EMAIL}"
db_tier     = "${DB_TIER}"
EOF
    ok "Written: terraform/environments/${ENV}/terraform.tfvars"
done

# Update backend.tf with correct bucket names
for ENV in dev prod; do
    BACKEND="${REPO_ROOT}/terraform/environments/${ENV}/backend.tf"
    cat > "$BACKEND" <<EOF
# Auto-generated by scripts/setup-gcp.sh
terraform {
  backend "gcs" {
    bucket = "${STATE_BUCKET}"
    prefix = "terraform/${ENV}"
  }
}
EOF
    ok "Written: terraform/environments/${ENV}/backend.tf"
done

# ── Print GitHub Secrets table ────────────────────────────────────────────────
step "GitHub Secrets — configure these in your repository"
echo ""
echo "  Go to: https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/settings/secrets/actions"
echo "  Add the following Repository Secrets:"
echo ""
printf "  %-35s %s\n" "SECRET NAME" "VALUE"
printf "  %-35s %s\n" "-----------" "-----"
printf "  %-35s %s\n" "GCP_PROJECT_ID"        "$PROJECT_ID"
printf "  %-35s %s\n" "CLOUD_RUN_REGION"      "$REGION"
printf "  %-35s %s\n" "AR_REPO"               "$AR_FULL"
printf "  %-35s %s\n" "WIF_PROVIDER"          "$WIF_PROVIDER_FULL"
printf "  %-35s %s\n" "TF_SA_EMAIL"           "$TF_SA"
printf "  %-35s %s\n" "DEPLOY_SA_EMAIL"       "$DEPLOY_SA"
printf "  %-35s %s\n" "WORKER_SA_EMAIL"       "$WORKER_SA"
printf "  %-35s %s\n" "DB_PASSWORD"           "<choose a strong password>"
printf "  %-35s %s\n" "DB_PASSWORD_PROD"      "<choose a strong password>"
printf "  %-35s %s\n" "GCS_DAG_BUCKET"        "${PROJECT_ID}-airflow-dags"
printf "  %-35s %s\n" "GCS_SPARK_BUCKET"      "${PROJECT_ID}-spark-jobs"
printf "  %-35s %s\n" "OPENAI_API_KEY"        "<your OpenAI key>"
printf "  %-35s %s\n" "ANTHROPIC_API_KEY"     "<your Anthropic key>"
printf "  %-35s %s\n" "NEO4J_PASSWORD"        "<choose a strong password>"
printf "  %-35s %s\n" "WEAVIATE_API_KEY"      "<choose a strong password>"
printf "  %-35s %s\n" "BACKEND_URL"           "<filled after first deploy>"
printf "  %-35s %s\n" "DATA_AGENT_URL"        "<filled after first deploy>"
echo ""

# ── Done ─────────────────────────────────────────────────────────────────────
step "Setup complete"
echo ""
echo -e "${GREEN}${BOLD}Next steps:${RESET}"
echo ""
echo "  1. Configure the GitHub Secrets above (table printed above)"
echo ""
echo "  2. Copy and fill your .env file:"
echo "       cp .env.example .env"
echo "       # Edit .env with your API keys and passwords"
echo ""
echo "  3. Test locally first:"
echo "       bash scripts/local-pipeline.sh"
echo ""
echo "  4. Deploy to GCP (via Terraform — one command):"
echo "       bash scripts/infra-up.sh dev"
echo ""
echo "  5. Push to GitHub to trigger CI/CD:"
echo "       git push origin main"
echo ""
echo "  Estimated GCP deployment time: ~45 minutes (Cloud Composer is slowest)"
echo ""
