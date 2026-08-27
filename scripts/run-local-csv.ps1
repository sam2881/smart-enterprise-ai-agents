# =============================================================================
# run-local-csv.ps1 — Local CSV Pipeline Runner
# =============================================================================
# Runs any CSV file through Bronze → Silver → Gold medallion pipeline
# using Docker Airflow + pandas (no GCP / Spark / BigQuery required).
#
# Usage (from project root):
#   .\scripts\run-local-csv.ps1
#   .\scripts\run-local-csv.ps1 -CsvFile "C:\path\to\your_file.csv"
#   .\scripts\run-local-csv.ps1 -CsvFile ".\data\input\sample_sales.csv" -Rebuild
#
# Output:  .\data\output\  (bronze / silver / gold as Parquet + gold as CSV)
# Airflow: http://localhost:8083  (admin / admin123)
# =============================================================================

param(
    [string]$CsvFile   = ".\data\input\sample_sales.csv",
    [switch]$Rebuild,
    [string]$AirflowUrl  = "http://localhost:8083",
    [string]$AirflowUser = "admin",
    [string]$AirflowPass = "admin123"
)

$ErrorActionPreference = "Stop"
$DAG_ID   = "local_csv_pipeline"
$ROOT     = Split-Path $PSScriptRoot -Parent

function banner([string]$msg) {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host $('=' * 60) -ForegroundColor Cyan
}
function step([string]$msg) {
    Write-Host "`n$('-' * 60)" -ForegroundColor DarkCyan
    Write-Host "  $msg" -ForegroundColor DarkCyan
}
function ok([string]$msg)   { Write-Host "  [OK]   $msg" -ForegroundColor Green }
function info([string]$msg) { Write-Host "  [INFO] $msg" -ForegroundColor Gray }
function warn([string]$msg) { Write-Host "  [WARN] $msg" -ForegroundColor Yellow }
function err([string]$msg)  { Write-Host "  [ERR]  $msg" -ForegroundColor Red; exit 1 }

# Auth header for Airflow REST API
$authBytes  = [Text.Encoding]::ASCII.GetBytes("${AirflowUser}:${AirflowPass}")
$authHeader = @{ Authorization = "Basic " + [Convert]::ToBase64String($authBytes) }
$jsonHeader = $authHeader + @{ "Content-Type" = "application/json" }

# ---------------------------------------------------------------------------
banner "Local CSV Pipeline"

# ── Resolve & copy CSV ──────────────────────────────────────────────────────
step "1. Preparing CSV input"

$resolvedCsv = $null
try { $resolvedCsv = Resolve-Path $CsvFile } catch {}
if (-not $resolvedCsv) { err "CSV not found: $CsvFile" }

$CsvFilename = Split-Path $resolvedCsv -Leaf
$inputDir    = Join-Path $ROOT "data\input"
New-Item -ItemType Directory -Force $inputDir | Out-Null

$destPath = Join-Path $inputDir $CsvFilename
if ($resolvedCsv -ne $destPath) {
    Copy-Item $resolvedCsv $destPath -Force
    ok "Copied $CsvFilename to data\input\"
} else {
    ok "$CsvFilename already in data\input\"
}

# Count rows for info
$rows = (Get-Content $destPath).Count - 1
info "File: $CsvFilename  ($rows data rows)"

# ── Start infra ─────────────────────────────────────────────────────────────
step "2. Starting Docker services"

# Always start postgres first
info "Starting postgres..."
docker compose up -d postgres 2>&1 | Out-Null

info "Waiting for postgres..."
$tries = 0
do {
    Start-Sleep 3; $tries++
    $h = docker inspect --format "{{.State.Health.Status}}" agentic-postgres 2>$null
} while ($h -ne "healthy" -and $tries -lt 20)
if ($h -ne "healthy") { err "Postgres failed to start" }
ok "Postgres healthy"

# Start airflow (optionally rebuild)
if ($Rebuild) {
    info "Rebuilding Airflow image (first time or after changes)..."
    docker compose build airflow 2>&1 | Select-Object -Last 8
}

info "Starting Airflow..."
docker compose up -d airflow 2>&1 | Select-Object -Last 3

info "Waiting for Airflow web server (up to 3 min)..."
$tries = 0
do {
    Start-Sleep 5; $tries++
    try {
        $r = Invoke-WebRequest "$AirflowUrl/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
        $up = $r.StatusCode -eq 200
    } catch { $up = $false }
    if ($tries % 6 -eq 0) { info "  Still waiting... ($($tries * 5)s)" }
} while (-not $up -and $tries -lt 36)

if (-not $up) { err "Airflow did not start. Run: docker compose logs airflow --tail 80" }
ok "Airflow running at $AirflowUrl"

# ── Wait for DAG registration ───────────────────────────────────────────────
step "3. Waiting for DAG to register"

$tries = 0
do {
    Start-Sleep 5; $tries++
    try {
        $d = Invoke-RestMethod "$AirflowUrl/api/v1/dags/$DAG_ID" -Headers $authHeader -ErrorAction SilentlyContinue
        $found = $d -ne $null
    } catch { $found = $false }
} while (-not $found -and $tries -lt 18)

if (-not $found) { err "DAG '$DAG_ID' not found. Check: docker compose logs airflow --tail 30" }
ok "DAG '$DAG_ID' registered"

# Unpause DAG
try {
    Invoke-RestMethod "$AirflowUrl/api/v1/dags/$DAG_ID" -Method Patch `
        -Headers $jsonHeader -Body '{"is_paused": false}' | Out-Null
    ok "DAG unpaused"
} catch { warn "Could not unpause (may already be active)" }

# ── Trigger run ─────────────────────────────────────────────────────────────
step "4. Triggering pipeline"

$runId = "local_$((Get-Date).ToString('yyyyMMdd_HHmmss'))"
$body  = @{
    dag_run_id = $runId
    conf       = @{ csv_filename = $CsvFilename; run_label = $runId }
} | ConvertTo-Json

$tr = Invoke-RestMethod "$AirflowUrl/api/v1/dags/$DAG_ID/dagRuns" `
    -Method Post -Headers $jsonHeader -Body $body

ok "Run triggered: $runId"
info "Monitor: $AirflowUrl/dags/$DAG_ID/grid"

# ── Poll for completion ──────────────────────────────────────────────────────
step "5. Running pipeline (Bronze > Silver > Gold)"
info "Polling every 8s..."

$tries = 0
do {
    Start-Sleep 8; $tries++
    try {
        $run   = Invoke-RestMethod "$AirflowUrl/api/v1/dags/$DAG_ID/dagRuns/$runId" -Headers $authHeader
        $state = $run.state
    } catch { $state = "unknown" }

    $elapsed = $tries * 8
    $bar = switch ($state) {
        "running" { ">>>>>>>" }
        "success" { "DONE   " }
        "failed"  { "FAILED " }
        default   { "...    " }
    }
    Write-Host "  [$bar] $state  (${elapsed}s)" -NoNewline
    Write-Host "`r" -NoNewline

} while ($state -notin @("success","failed","upstream_failed") -and $tries -lt 50)

Write-Host ""

if ($state -eq "success") {
    ok "Pipeline COMPLETED in $($tries * 8)s"
} else {
    err "Pipeline $state — check Airflow UI: $AirflowUrl/dags/$DAG_ID/grid"
}

# ── Show results ─────────────────────────────────────────────────────────────
step "6. Results"

$goldCsv = Join-Path $ROOT "data\output\gold\customer_summary.csv"
if (Test-Path $goldCsv) {
    ok "Gold layer: data\output\gold\customer_summary.csv"
    Write-Host ""
    $content = Get-Content $goldCsv
    # Print header + rows with basic formatting
    $header = $content[0] -split ","
    Write-Host ("  " + ($header -join "  |  ")) -ForegroundColor Cyan
    Write-Host ("  " + ("-" * 80)) -ForegroundColor DarkGray
    foreach ($line in ($content | Select-Object -Skip 1)) {
        Write-Host ("  " + $line) -ForegroundColor White
    }
    Write-Host ""
}

# Summary of all layers
Write-Host "  Output files:" -ForegroundColor DarkCyan
foreach ($layer in @("raw","bronze","silver","gold")) {
    $dir = Join-Path $ROOT "data\output\$layer"
    if (Test-Path $dir) {
        Get-ChildItem $dir -File | ForEach-Object {
            $kb = [math]::Round($_.Length / 1KB, 1)
            ok "$layer/$($_.Name)  ($kb KB)"
        }
    }
}

banner "Done!  data\output\  |  Airflow: $AirflowUrl"
