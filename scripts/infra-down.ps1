# scripts/infra-down.ps1
# Destroy GCP infrastructure with a single command.
#
# Usage:
#   .\scripts\infra-down.ps1                  # destroy dev (asks confirmation)
#   .\scripts\infra-down.ps1 -Env prod        # destroy prod (double confirmation)
#   .\scripts\infra-down.ps1 -Target cloud_run  # destroy only specific module
#
# WARNING: Cloud SQL (prod) and GKE (prod) have deletion_protection=true.
# Set deletion_protection=false manually in Terraform before destroying prod.

param(
    [ValidateSet("dev","prod")]
    [string]$Env = "dev",
    [string]$Target = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$TF_DIR = "$PSScriptRoot\..\terraform\environments\$Env"

function Write-Step { param($m) Write-Host "`n==> $m" -ForegroundColor Cyan }
function Write-Warn { param($m) Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Write-Ok   { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Write-Fail { param($m) Write-Host "  [FAIL] $m" -ForegroundColor Red; exit 1 }

# ── Safety checks ─────────────────────────────────────────────────────────────
Write-Step "Destroy preflight ($Env)"

try { terraform --version > $null 2>&1 } catch { Write-Fail "Terraform not found." }
try { gcloud --version  > $null 2>&1 } catch { Write-Fail "gcloud CLI not found." }

if ($Env -eq "prod") {
    Write-Warn "You are about to DESTROY the PRODUCTION environment."
    Write-Warn "Cloud SQL and GKE have deletion_protection=true — you must disable it first."
    Write-Warn "This will delete ALL data: Postgres, Redis, GKE workloads, Cloud Run services."
    Write-Host ""
    $c1 = Read-Host "Type the environment name to confirm ('prod')"
    if ($c1 -ne "prod") { Write-Host "Aborted." -ForegroundColor Yellow; exit 0 }
    $c2 = Read-Host "Type 'destroy production' to proceed"
    if ($c2 -ne "destroy production") { Write-Host "Aborted." -ForegroundColor Yellow; exit 0 }
} else {
    $confirm = Read-Host "Destroy dev infrastructure? This will delete all dev GCP resources. Type 'yes'"
    if ($confirm -ne "yes") { Write-Host "Aborted." -ForegroundColor Yellow; exit 0 }
}

# ── DB password (required by terraform for plan) ─────────────────────────────
if (-not $env:TF_VAR_db_password) {
    $env:TF_VAR_db_password = "placeholder-for-destroy"
}

# ── Terraform destroy ─────────────────────────────────────────────────────────
Write-Step "Terraform destroy ($Env)"
Push-Location $TF_DIR
try {
    terraform init -reconfigure

    $targetArgs = if ($Target) { ($Target -split ",") | ForEach-Object { "-target=module.$_" } } else { @() }

    if ($Target) {
        Write-Step "Destroying modules: $Target"
        terraform destroy -var-file="terraform.tfvars" @targetArgs -auto-approve
    } else {
        # Destroy in safe order: app layer first, then data layer, then networking
        Write-Step "Step 1/4 — Destroy Cloud Run services"
        terraform destroy -var-file="terraform.tfvars" -target=module.cloud_run -auto-approve

        Write-Step "Step 2/4 — Destroy Cloud Composer"
        terraform destroy -var-file="terraform.tfvars" -target=module.composer -auto-approve

        Write-Step "Step 3/4 — Destroy GKE, Monitoring, Pub/Sub"
        terraform destroy -var-file="terraform.tfvars" `
            -target=module.gke `
            -target=module.monitoring `
            -target=module.pubsub `
            -auto-approve

        Write-Step "Step 4/4 — Destroy data layer + networking"
        terraform destroy -var-file="terraform.tfvars" -auto-approve
    }

    Write-Ok "Destroy complete for $Env"
    Write-Host ""
    Write-Warn "Secret Manager secrets still hold your credentials — delete manually if needed:"
    Write-Host "  gcloud secrets list --project=agent-ai-test-461120" -ForegroundColor Gray
    Write-Host "  gcloud secrets delete SECRET_NAME --project=agent-ai-test-461120" -ForegroundColor Gray
    Write-Host ""
    Write-Warn "Artifact Registry images are NOT deleted — delete manually to avoid storage costs:"
    Write-Host "  gcloud artifacts repositories delete ai-agent-platform --location=us-central1 --project=agent-ai-test-461120" -ForegroundColor Gray

} finally {
    Pop-Location
}
