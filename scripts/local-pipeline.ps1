# scripts/local-pipeline.ps1
# One-command local CI pipeline — build, migrate, test, smoke-test.
# Run from the project root: .\scripts\local-pipeline.ps1
#
# Flags:
#   -SkipBuild    Use cached images (faster re-runs)
#   -SkipTests    Skip unit tests
#   -Down         Tear down all containers after the run
#   -Infra        Start only infra (no app services) — useful for local dev

param(
    [switch]$SkipBuild,
    [switch]$SkipTests,
    [switch]$Down,
    [switch]$Infra
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Colour helpers ────────────────────────────────────────────────────────────
function Write-Step  { param($msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Write-Ok    { param($msg) Write-Host "  [OK] $msg" -ForegroundColor Green }
function Write-Fail  { param($msg) Write-Host "  [FAIL] $msg" -ForegroundColor Red }
function Write-Info  { param($msg) Write-Host "  $msg" -ForegroundColor Gray }

$COMPOSE_BASE = "docker compose -f docker-compose.yml"
$COMPOSE_FULL = "docker compose -f docker-compose.yml -f docker-compose.local.yml"

$INFRA_SERVICES  = "postgres redis zookeeper kafka neo4j weaviate kafka-ui"
$OBS_SERVICES    = "prometheus grafana tempo loki otel-collector langfuse"
$APP_SERVICES    = "db-migrate backend-api data-agent frontend event-orchestrator incident-consumer pipeline-consumer"

$PASSED = @()
$FAILED = @()

function Invoke-Step {
    param([string]$Name, [scriptblock]$Block)
    Write-Step $Name
    try {
        & $Block
        $script:PASSED += $Name
        Write-Ok "Done"
    } catch {
        $script:FAILED += $Name
        Write-Fail "Failed: $_"
        if ($Name -match "Build|Infra|Migrate") {
            Write-Host "`nCritical step failed — aborting." -ForegroundColor Red
            Show-Summary
            exit 1
        }
    }
}

function Show-Summary {
    Write-Host "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor White
    Write-Host " LOCAL PIPELINE SUMMARY" -ForegroundColor White
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor White
    foreach ($s in $PASSED) { Write-Host "  PASS  $s" -ForegroundColor Green }
    foreach ($s in $FAILED) { Write-Host "  FAIL  $s" -ForegroundColor Red }
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor White
    if ($FAILED.Count -eq 0) {
        Write-Host "`n  All checks passed. Safe to push to GCP." -ForegroundColor Green
    } else {
        Write-Host "`n  $($FAILED.Count) step(s) failed. Fix before pushing." -ForegroundColor Red
    }
}

function Wait-Healthy {
    param([string]$Service, [string]$Url, [int]$TimeoutSec = 120)
    Write-Info "Waiting for $Service at $Url..."
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3 -EA Stop
            if ($resp.StatusCode -lt 400) {
                Write-Ok "$Service is up"
                return
            }
        } catch {}
        Start-Sleep -Seconds 5
    }
    throw "$Service did not become healthy within ${TimeoutSec}s"
}

# ── Preflight checks ──────────────────────────────────────────────────────────
Write-Step "Preflight checks"

if (-not (Test-Path ".env")) {
    Write-Fail ".env file not found. Copy .env.example to .env and fill in ANTHROPIC_API_KEY."
    exit 1
}

$envContent = Get-Content ".env" -Raw
if ($envContent -notmatch "ANTHROPIC_API_KEY=sk-") {
    Write-Host "  [WARN] ANTHROPIC_API_KEY looks unset in .env — LLM calls will fail at runtime" -ForegroundColor Yellow
}

try {
    docker info > $null 2>&1
    Write-Ok "Docker is running"
} catch {
    Write-Fail "Docker is not running. Start Docker Desktop first."
    exit 1
}

Write-Ok "Preflight passed"
$PASSED += "Preflight"

# ── Tear down existing containers if -Down requested up front ────────────────
if ($Down) {
    Invoke-Step "Tear down existing containers" {
        Invoke-Expression "$COMPOSE_FULL down --remove-orphans -v" 2>&1 | Out-Null
    }
}

# ── Start infra ───────────────────────────────────────────────────────────────
Invoke-Step "Start infrastructure services" {
    Invoke-Expression "$COMPOSE_BASE up -d $INFRA_SERVICES $OBS_SERVICES"
}

Invoke-Step "Wait for Kafka to be ready" {
    $deadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect --format='{{.State.Health.Status}}' agentic-kafka 2>$null
        if ($status -eq "healthy") { Write-Ok "Kafka healthy"; return }
        Start-Sleep -Seconds 5
    }
    throw "Kafka did not become healthy within 90s"
}

Invoke-Step "Wait for Postgres to be ready" {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect --format='{{.State.Health.Status}}' agentic-postgres 2>$null
        if ($status -eq "healthy") { Write-Ok "Postgres healthy"; return }
        Start-Sleep -Seconds 3
    }
    throw "Postgres did not become healthy within 60s"
}

if ($Infra) {
    Write-Host "`nInfra-only mode. Services available:" -ForegroundColor Cyan
    Write-Info "  Kafka UI   http://localhost:8090"
    Write-Info "  Grafana    http://localhost:3001"
    Write-Info "  Prometheus http://localhost:9090"
    Write-Info "  Langfuse   http://localhost:3002"
    Write-Info "  Airflow    http://localhost:8083"
    exit 0
}

# ── Build app images ──────────────────────────────────────────────────────────
if (-not $SkipBuild) {
    Invoke-Step "Build application images" {
        Invoke-Expression "$COMPOSE_FULL build --parallel"
    }
} else {
    Write-Step "Build skipped (-SkipBuild)"
    $PASSED += "Build application images"
}

# ── DB migrations ─────────────────────────────────────────────────────────────
Invoke-Step "Run database migrations" {
    Invoke-Expression "$COMPOSE_FULL run --rm db-migrate"
}

# ── Start app services ────────────────────────────────────────────────────────
Invoke-Step "Start application services" {
    Invoke-Expression "$COMPOSE_FULL up -d backend-api data-agent"
}

Invoke-Step "Wait for Backend API" {
    Wait-Healthy -Service "backend-api" -Url "http://localhost:8000/health" -TimeoutSec 90
}

Invoke-Step "Wait for Data Agent API" {
    Wait-Healthy -Service "data-agent" -Url "http://localhost:8001/health" -TimeoutSec 90
}

Invoke-Step "Start remaining app services" {
    Invoke-Expression "$COMPOSE_FULL up -d frontend event-orchestrator incident-consumer pipeline-consumer"
}

# ── Unit tests ────────────────────────────────────────────────────────────────
if (-not $SkipTests) {
    Invoke-Step "Run unit tests" {
        $result = Invoke-Expression "$COMPOSE_FULL run --rm test-runner --profile test" 2>&1
        Write-Info $result
        if ($LASTEXITCODE -ne 0) { throw "Unit tests failed (exit $LASTEXITCODE)" }
    }
}

# ── Smoke tests ───────────────────────────────────────────────────────────────
Invoke-Step "Smoke test: Backend /health" {
    $r = Invoke-RestMethod "http://localhost:8000/health" -TimeoutSec 5
    if ($r.status -ne "healthy") { throw "Backend /health returned: $($r.status)" }
}

Invoke-Step "Smoke test: Data Agent /health" {
    $r = Invoke-RestMethod "http://localhost:8001/health" -TimeoutSec 5
    if ($r.status -ne "healthy") { throw "Data Agent /health returned: $($r.status)" }
}

Invoke-Step "Smoke test: Frontend loads" {
    $r = Invoke-WebRequest "http://localhost:3000" -UseBasicParsing -TimeoutSec 10
    if ($r.StatusCode -ne 200) { throw "Frontend returned HTTP $($r.StatusCode)" }
}

Invoke-Step "Smoke test: Kafka topics exist" {
    $topics = docker exec agentic-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>$null
    if ($topics -notmatch "incident") {
        Write-Info "  Auto-created topics not yet visible (normal on first run)"
    } else {
        Write-Info "  Topics: $($topics -join ', ')"
    }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Show-Summary

Write-Host "`n  URLs:" -ForegroundColor Cyan
Write-Info "  Frontend       http://localhost:3000"
Write-Info "  Backend API    http://localhost:8000/docs"
Write-Info "  Data Agent     http://localhost:8001/docs"
Write-Info "  Kafka UI       http://localhost:8090"
Write-Info "  Grafana        http://localhost:3001"
Write-Info "  Langfuse       http://localhost:3002"
Write-Info "  Airflow        http://localhost:8083"

if ($Down) {
    Write-Step "Tearing down"
    Invoke-Expression "$COMPOSE_FULL down --remove-orphans"
}

if ($FAILED.Count -gt 0) { exit 1 }
