#Requires -Version 5.1
<#
.SYNOPSIS
    First-time GCP project setup wizard (Windows PowerShell)

.DESCRIPTION
    Identical behavior to scripts/setup-gcp.sh but runs on Windows PowerShell 5.1+.
    All operations are idempotent — safe to re-run.

    What this script does:
      1. Validates gcloud + terraform CLI are installed
      2. Prompts for project ID, region, alert email, GitHub org/repo
      3. Creates the Terraform state GCS bucket
      4. Enables all 16 required GCP APIs
      5. Creates 3 service accounts (terraform-sa, deploy-sa, worker-sa)
      6. Grants IAM roles to each service account
      7. Sets up Workload Identity Federation for GitHub Actions (keyless auth)
      8. Creates Artifact Registry repository
      9. Generates terraform/environments/{dev,prod}/terraform.tfvars
     10. Generates terraform/environments/{dev,prod}/backend.tf
     11. Prints the exact GitHub Secrets table you need to fill in

.EXAMPLE
    .\scripts\setup-gcp.ps1

.NOTES
    Prerequisites:
      - gcloud CLI installed and authenticated (gcloud auth login)
      - terraform >= 1.5 installed
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step  { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Write-Ok    { param([string]$msg) Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Write-Warn  { param([string]$msg) Write-Host "  [WARN] $msg" -ForegroundColor Yellow }
function Write-Fail  { param([string]$msg) Write-Host "  [FAIL] $msg" -ForegroundColor Red; exit 1 }

# ── Preflight ─────────────────────────────────────────────────────────────────
Write-Step "Preflight checks"

if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Fail "gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install"
}
if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
    Write-Fail "Terraform not found. Install: https://developer.hashicorp.com/terraform/downloads"
}

$account = (gcloud config get-value account 2>$null)
if ($account -notmatch "@") {
    Write-Fail "Not authenticated. Run: gcloud auth login && gcloud auth application-default login"
}
Write-Ok "Authenticated as: $account"

# ── Collect inputs ────────────────────────────────────────────────────────────
Write-Step "Configuration"
Write-Host ""

$projectId = Read-Host "GCP Project ID (existing project, e.g. 'my-company-ai-dev')"
if ([string]::IsNullOrWhiteSpace($projectId)) { Write-Fail "Project ID is required." }

$region = Read-Host "GCP Region [us-central1]"
if ([string]::IsNullOrWhiteSpace($region)) { $region = "us-central1" }

$zone = Read-Host "GCP Zone [$region-a]"
if ([string]::IsNullOrWhiteSpace($zone)) { $zone = "$region-a" }

$alertEmail = Read-Host "Alert email (receives billing and monitoring alerts)"
if ($alertEmail -notmatch "@") { Write-Fail "Invalid email address." }

$githubOrg = Read-Host "GitHub organization or username (for Workload Identity Federation)"
if ([string]::IsNullOrWhiteSpace($githubOrg)) { Write-Fail "GitHub org/user is required." }

$githubRepo = Read-Host "GitHub repository name (e.g. 'enterprise-agentic-platform')"
if ([string]::IsNullOrWhiteSpace($githubRepo)) { Write-Fail "GitHub repo is required." }

$stateBucket = "$projectId-tfstate"

Write-Host ""
Write-Host "Summary" -ForegroundColor White
Write-Host "  Project    : $projectId"
Write-Host "  Region     : $region / $zone"
Write-Host "  Alert email: $alertEmail"
Write-Host "  GitHub     : $githubOrg/$githubRepo"
Write-Host "  TF bucket  : gs://$stateBucket"
Write-Host ""
$confirm = Read-Host "Proceed? [y/N]"
if ($confirm -ne "y") { Write-Host "Aborted."; exit 0 }

gcloud config set project $projectId --quiet

# ── Terraform state bucket ────────────────────────────────────────────────────
Write-Step "Terraform state bucket"
$bucketExists = $null
try { $bucketExists = gsutil ls -b "gs://$stateBucket" 2>$null } catch {}

if ($bucketExists) {
    Write-Ok "Bucket already exists: gs://$stateBucket"
} else {
    gsutil mb -l $region "gs://$stateBucket"
    gsutil versioning set on "gs://$stateBucket"
    Write-Ok "Created: gs://$stateBucket (versioning enabled)"
}

# ── Enable APIs ───────────────────────────────────────────────────────────────
Write-Step "Enable required GCP APIs (this takes ~2 minutes)"
$apis = @(
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "sqladmin.googleapis.com",
    "redis.googleapis.com",
    "container.googleapis.com",
    "composer.googleapis.com",
    "pubsub.googleapis.com",
    "vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "logging.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "modelarmor.googleapis.com"
)
gcloud services enable @apis --project=$projectId --quiet
Write-Ok "All APIs enabled"

# ── Service accounts ──────────────────────────────────────────────────────────
Write-Step "Create service accounts"

function New-ServiceAccount {
    param([string]$Name, [string]$Display)
    $email = "$Name@$projectId.iam.gserviceaccount.com"
    $exists = $null
    try { $exists = gcloud iam service-accounts describe $email --project=$projectId 2>$null } catch {}
    if ($exists) {
        Write-Ok "SA already exists: $email"
    } else {
        gcloud iam service-accounts create $Name --display-name=$Display --project=$projectId
        Write-Ok "Created SA: $email"
    }
    return $email
}

$tfSa     = New-ServiceAccount -Name "terraform-sa" -Display "Terraform Provisioner"
$deploySa = New-ServiceAccount -Name "deploy-sa"    -Display "CI/CD Deployer"
$workerSa = New-ServiceAccount -Name "worker-sa"    -Display "Runtime Worker"

# Terraform SA
gcloud projects add-iam-policy-binding $projectId --member="serviceAccount:$tfSa" --role="roles/owner" --quiet | Out-Null
Write-Ok "Terraform SA: roles/owner granted"

# Deploy SA
@("roles/run.admin", "roles/artifactregistry.writer", "roles/secretmanager.secretAccessor", "roles/iam.serviceAccountUser") | ForEach-Object {
    gcloud projects add-iam-policy-binding $projectId --member="serviceAccount:$deploySa" --role=$_ --quiet | Out-Null
}
Write-Ok "Deploy SA: Cloud Run + Artifact Registry roles granted"

# Worker SA
@("roles/bigquery.dataEditor", "roles/storage.objectAdmin", "roles/secretmanager.secretAccessor",
  "roles/pubsub.publisher", "roles/pubsub.subscriber", "roles/cloudsql.client") | ForEach-Object {
    gcloud projects add-iam-policy-binding $projectId --member="serviceAccount:$workerSa" --role=$_ --quiet | Out-Null
}
Write-Ok "Worker SA: BigQuery + GCS + PubSub + SQL roles granted"

# ── Workload Identity Federation ──────────────────────────────────────────────
Write-Step "Workload Identity Federation (GitHub Actions -> GCP, no long-lived keys)"

$poolId      = "github-actions-pool"
$providerId  = "github-provider"
$poolName    = "projects/$projectId/locations/global/workloadIdentityPools/$poolId"

# Create pool
$poolExists = $null
try { $poolExists = gcloud iam workload-identity-pools describe $poolId --location=global --project=$projectId 2>$null } catch {}
if ($poolExists) {
    Write-Ok "WIF pool already exists: $poolId"
} else {
    gcloud iam workload-identity-pools create $poolId --location=global --display-name="GitHub Actions Pool" --project=$projectId
    Write-Ok "Created WIF pool: $poolId"
}

# Create provider
$providerExists = $null
try { $providerExists = gcloud iam workload-identity-pools providers describe $providerId --workload-identity-pool=$poolId --location=global --project=$projectId 2>$null } catch {}
if ($providerExists) {
    Write-Ok "WIF provider already exists: $providerId"
} else {
    gcloud iam workload-identity-pools providers create-oidc $providerId `
        --workload-identity-pool=$poolId `
        --location=global `
        --issuer-uri="https://token.actions.githubusercontent.com" `
        --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor" `
        "--attribute-condition=assertion.repository=='$githubOrg/$githubRepo'" `
        --project=$projectId
    Write-Ok "Created WIF provider for: $githubOrg/$githubRepo"
}

$projectNumber = (gcloud projects describe $projectId --format="value(projectNumber)")
$wifProviderFull = "projects/$projectNumber/locations/global/workloadIdentityPools/$poolId/providers/$providerId"

# Bind WIF
$principal = "principalSet://iam.googleapis.com/$poolName/attribute.repository/$githubOrg/$githubRepo"
gcloud iam service-accounts add-iam-policy-binding $deploySa --role="roles/iam.workloadIdentityUser" --member=$principal --project=$projectId --quiet | Out-Null
gcloud iam service-accounts add-iam-policy-binding $tfSa     --role="roles/iam.workloadIdentityUser" --member=$principal --project=$projectId --quiet | Out-Null
Write-Ok "WIF bound to deploy-sa and terraform-sa"

# ── Artifact Registry ─────────────────────────────────────────────────────────
Write-Step "Artifact Registry repository"
$arRepoName = "ai-agent-platform"
$arFull     = "$region-docker.pkg.dev/$projectId/$arRepoName"

$arExists = $null
try { $arExists = gcloud artifacts repositories describe $arRepoName --location=$region --project=$projectId 2>$null } catch {}
if ($arExists) {
    Write-Ok "Artifact Registry already exists: $arFull"
} else {
    gcloud artifacts repositories create $arRepoName `
        --repository-format=docker `
        --location=$region `
        --description="AI Agent Platform Docker images" `
        --project=$projectId
    Write-Ok "Created: $arFull"
}

# ── Generate terraform.tfvars and backend.tf ──────────────────────────────────
Write-Step "Generate terraform.tfvars"

$repoRoot = Split-Path -Parent $PSScriptRoot
$timestamp = (Get-Date -Format "yyyy-MM-dd HH:mm UTC")

foreach ($env in @("dev", "prod")) {
    $dbTier = if ($env -eq "prod") { "db-custom-2-7680" } else { "db-g1-small" }
    $tfvarsPath = Join-Path $repoRoot "terraform\environments\$env\terraform.tfvars"

    @"
# Auto-generated by scripts/setup-gcp.ps1 -- $timestamp
# Re-run the script to regenerate, or edit manually.

project_id  = "$projectId"
region      = "$region"
zone        = "$zone"
environment = "$env"
alert_email = "$alertEmail"
db_tier     = "$dbTier"
"@ | Set-Content -Encoding utf8 $tfvarsPath
    Write-Ok "Written: terraform\environments\$env\terraform.tfvars"

    $backendPath = Join-Path $repoRoot "terraform\environments\$env\backend.tf"
    @"
# Auto-generated by scripts/setup-gcp.ps1
terraform {
  backend "gcs" {
    bucket = "$stateBucket"
    prefix = "terraform/$env"
  }
}
"@ | Set-Content -Encoding utf8 $backendPath
    Write-Ok "Written: terraform\environments\$env\backend.tf"
}

# ── Print GitHub Secrets table ────────────────────────────────────────────────
Write-Step "GitHub Secrets -- configure these in your repository"
Write-Host ""
Write-Host "  Go to: https://github.com/$githubOrg/$githubRepo/settings/secrets/actions"
Write-Host "  Add the following Repository Secrets:"
Write-Host ""
Write-Host ("  {0,-35} {1}" -f "SECRET NAME", "VALUE")
Write-Host ("  {0,-35} {1}" -f "-----------", "-----")
Write-Host ("  {0,-35} {1}" -f "GCP_PROJECT_ID",       $projectId)
Write-Host ("  {0,-35} {1}" -f "CLOUD_RUN_REGION",     $region)
Write-Host ("  {0,-35} {1}" -f "AR_REPO",              $arFull)
Write-Host ("  {0,-35} {1}" -f "WIF_PROVIDER",         $wifProviderFull)
Write-Host ("  {0,-35} {1}" -f "TF_SA_EMAIL",          $tfSa)
Write-Host ("  {0,-35} {1}" -f "DEPLOY_SA_EMAIL",      $deploySa)
Write-Host ("  {0,-35} {1}" -f "WORKER_SA_EMAIL",      $workerSa)
Write-Host ("  {0,-35} {1}" -f "DB_PASSWORD",          "<choose a strong password>")
Write-Host ("  {0,-35} {1}" -f "DB_PASSWORD_PROD",     "<choose a strong password>")
Write-Host ("  {0,-35} {1}" -f "GCS_DAG_BUCKET",       "$projectId-airflow-dags")
Write-Host ("  {0,-35} {1}" -f "GCS_SPARK_BUCKET",     "$projectId-spark-jobs")
Write-Host ("  {0,-35} {1}" -f "OPENAI_API_KEY",       "<your OpenAI key>")
Write-Host ("  {0,-35} {1}" -f "ANTHROPIC_API_KEY",    "<your Anthropic key>")
Write-Host ("  {0,-35} {1}" -f "NEO4J_PASSWORD",       "<choose a strong password>")
Write-Host ("  {0,-35} {1}" -f "WEAVIATE_API_KEY",     "<choose a strong password>")
Write-Host ("  {0,-35} {1}" -f "BACKEND_URL",          "<filled after first deploy>")
Write-Host ("  {0,-35} {1}" -f "DATA_AGENT_URL",       "<filled after first deploy>")
Write-Host ""

# ── Done ─────────────────────────────────────────────────────────────────────
Write-Step "Setup complete"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Green
Write-Host ""
Write-Host "  1. Configure the GitHub Secrets above"
Write-Host ""
Write-Host "  2. Copy and fill your .env file:"
Write-Host "       Copy-Item .env.example .env"
Write-Host "       notepad .env"
Write-Host ""
Write-Host "  3. Test locally:"
Write-Host "       .\scripts\start-dev.ps1"
Write-Host ""
Write-Host "  4. Deploy to GCP:"
Write-Host "       bash scripts/infra-up.sh dev   # or open Git Bash"
Write-Host ""
Write-Host "  5. Push to GitHub to trigger CI/CD:"
Write-Host "       git push origin main"
Write-Host ""
Write-Host "  Estimated GCP deployment time: ~45 minutes (Cloud Composer is slowest)"
Write-Host ""
