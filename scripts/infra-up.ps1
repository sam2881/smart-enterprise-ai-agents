# scripts/infra-up.ps1
# Provision GCP infrastructure with a single command.
#
# Usage:
#   .\scripts\infra-up.ps1                   # dev environment
#   .\scripts\infra-up.ps1 -Env prod         # prod environment
#   .\scripts\infra-up.ps1 -Plan             # dry-run only (terraform plan)
#   .\scripts\infra-up.ps1 -Env dev -Target networking,iam   # specific modules only

param(
    [ValidateSet("dev","prod")]
    [string]$Env = "dev",
    [switch]$Plan,
    [string]$Target = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$TF_DIR   = "$PSScriptRoot\..\terraform\environments\$Env"
$STATE_BUCKET = "agent-ai-test-461120-tfstate"
$PROJECT_ID   = "agent-ai-test-461120"
$REGION       = "us-central1"

function Write-Step { param($m) Write-Host "`n==> $m" -ForegroundColor Cyan }
function Write-Ok   { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Write-Fail { param($m) Write-Host "  [FAIL] $m" -ForegroundColor Red; exit 1 }

# ── Preflight ─────────────────────────────────────────────────────────────────
Write-Step "Preflight checks"

try { gcloud --version > $null 2>&1 } catch { Write-Fail "gcloud CLI not found. Install: cloud.google.com/sdk" }
try { terraform --version > $null 2>&1 } catch { Write-Fail "Terraform not found. Install: terraform.io/downloads" }

$account = gcloud config get-value account 2>$null
if (-not $account) { Write-Fail "Not authenticated. Run: gcloud auth login && gcloud auth application-default login" }
Write-Ok "Authenticated as: $account"

# ── DB password ───────────────────────────────────────────────────────────────
if (-not $env:TF_VAR_db_password) {
    $pw = Read-Host "Enter database password for Cloud SQL (will be stored in Secret Manager)" -AsSecureString
    $plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pw))
    $env:TF_VAR_db_password = $plain
    Write-Ok "DB password set"
}

# ── Bootstrap: create GCS state bucket if it doesn't exist ───────────────────
Write-Step "Ensure Terraform state bucket exists"
$exists = gsutil ls -b "gs://$STATE_BUCKET" 2>$null
if (-not $exists) {
    Write-Host "  Creating gs://$STATE_BUCKET ..." -ForegroundColor Yellow
    gsutil mb -l $REGION "gs://$STATE_BUCKET"
    gsutil versioning set on "gs://$STATE_BUCKET"
    Write-Ok "Bucket created"
} else {
    Write-Ok "Bucket already exists"
}

# ── Enable required APIs (idempotent) ─────────────────────────────────────────
Write-Step "Enable required GCP APIs"
$apis = @(
    "run.googleapis.com","artifactregistry.googleapis.com","secretmanager.googleapis.com",
    "sqladmin.googleapis.com","redis.googleapis.com","container.googleapis.com",
    "composer.googleapis.com","pubsub.googleapis.com","vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com","monitoring.googleapis.com","cloudtrace.googleapis.com",
    "logging.googleapis.com","iam.googleapis.com"
)
$apiList = $apis -join ","
gcloud services enable $apiList --project=$PROJECT_ID --quiet
Write-Ok "APIs enabled"

# ── Terraform init ────────────────────────────────────────────────────────────
Write-Step "Terraform init"
Push-Location $TF_DIR
try {
    terraform init -reconfigure
    Write-Ok "Init complete"

    # ── Terraform plan ────────────────────────────────────────────────────────
    Write-Step "Terraform plan ($Env)"
    $targetArgs = if ($Target) { ($Target -split ",") | ForEach-Object { "-target=module.$_" } } else { @() }
    terraform plan -var-file="terraform.tfvars" @targetArgs -out="tfplan-$Env"
    Write-Ok "Plan complete — review above"

    if ($Plan) {
        Write-Host "`n  -Plan flag set. Stopping before apply." -ForegroundColor Yellow
        exit 0
    }

    # ── Terraform apply ───────────────────────────────────────────────────────
    Write-Step "Terraform apply ($Env)"
    if ($Env -eq "prod") {
        $confirm = Read-Host "  Applying to PRODUCTION. Type 'yes' to confirm"
        if ($confirm -ne "yes") { Write-Host "  Aborted." -ForegroundColor Yellow; exit 0 }
    }

    terraform apply "tfplan-$Env"
    Write-Ok "Apply complete"

    # ── Print outputs ─────────────────────────────────────────────────────────
    Write-Step "Infrastructure URLs"
    $outputs = terraform output -json | ConvertFrom-Json
    Write-Host ""
    Write-Host "  Frontend      : $($outputs.frontend_url.value)"   -ForegroundColor Green
    Write-Host "  Backend API   : $($outputs.backend_url.value)"    -ForegroundColor Green
    Write-Host "  Data Agent    : $($outputs.data_agent_url.value)" -ForegroundColor Green
    Write-Host "  Airflow       : $($outputs.composer_url.value)"   -ForegroundColor Green
    Write-Host "  GKE Cluster   : $($outputs.gke_cluster.value)"    -ForegroundColor Green
    Write-Host "  Artifact Reg  : $($outputs.ar_repo.value)"        -ForegroundColor Green
    Write-Host "  Postgres Host : $($outputs.sql_host.value)"       -ForegroundColor Green
    Write-Host "  Redis Host    : $($outputs.redis_host.value)"     -ForegroundColor Green
    Write-Host ""
    Write-Host "  Next step: populate Secret Manager secrets, then run:" -ForegroundColor Cyan
    Write-Host "    git push origin main    # triggers CI/CD deploy" -ForegroundColor White

} finally {
    Pop-Location
}
