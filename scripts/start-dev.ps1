# Enterprise Agentic Platform — Dev Startup Script
# Run from project root: .\scripts\start-dev.ps1
# Requires: Docker Desktop running, Python 3.12+, Node.js 18+

param(
    [switch]$StopAll,
    [switch]$InfraOnly,
    [switch]$Status
)

$Root = Split-Path $PSScriptRoot -Parent
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
$env:PYTHONPATH = $Root
New-Item -ItemType Directory -Force "$Root\logs" | Out-Null

if ($StopAll) {
    Write-Host "Stopping all services..." -ForegroundColor Yellow
    Get-Process python, node -ErrorAction SilentlyContinue | Stop-Process -Force
    Set-Location $Root; docker compose down
    Write-Host "All services stopped." -ForegroundColor Green
    exit
}

if ($Status) {
    Write-Host ""
    Write-Host "=== PLATFORM STATUS ===" -ForegroundColor Cyan
    $ports = @(
        @{Port=3000; Name="Frontend       "; Url="http://localhost:3000"},
        @{Port=8000; Name="Backend API    "; Url="http://localhost:8000/docs"},
        @{Port=8001; Name="Data Agent API "; Url="http://localhost:8001/docs"},
        @{Port=8090; Name="Kafka UI       "; Url="http://localhost:8090"},
        @{Port=5432; Name="PostgreSQL     "; Url=$null},
        @{Port=6379; Name="Redis          "; Url=$null},
        @{Port=29092;Name="Kafka          "; Url=$null},
        @{Port=8080; Name="Weaviate       "; Url="http://localhost:8080/v1/.well-known/ready"},
        @{Port=9090; Name="Prometheus     "; Url="http://localhost:9090/-/healthy"},
        @{Port=3001; Name="Grafana        "; Url="http://localhost:3001/api/health"},
        @{Port=3200; Name="Tempo          "; Url="http://localhost:3200/ready"},
        @{Port=3100; Name="Loki           "; Url="http://localhost:3100/ready"},
        @{Port=3002; Name="Langfuse       "; Url="http://localhost:3002/api/public/health"},
        @{Port=8083; Name="Airflow        "; Url="http://localhost:8083/health"}
    )
    foreach ($p in $ports) {
        try {
            $r = Invoke-WebRequest -Uri $p.Url -TimeoutSec 3 -UseBasicParsing -ErrorAction Stop 2>$null
            Write-Host "  [UP]  $($p.Name) :$($p.Port)  $($p.Url)" -ForegroundColor Green
        } catch {
            $code = $_.Exception.Response.StatusCode.value__
            if ($code) {
                Write-Host "  [UP]  $($p.Name) :$($p.Port)" -ForegroundColor Green
            } else {
                $tcp = netstat -an 2>$null | Select-String ":$($p.Port) "
                if ($tcp) {
                    Write-Host "  [UP]  $($p.Name) :$($p.Port)" -ForegroundColor Green
                } else {
                    Write-Host "  [--]  $($p.Name) :$($p.Port)" -ForegroundColor Red
                }
            }
        }
    }
    Write-Host ""
    docker compose -f "$Root\docker-compose.yml" ps --format "  Docker: {{.Name}} | {{.Status}}" 2>$null
    exit
}

Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  Enterprise Agentic Platform — Starting Up" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""

# 1. Start Docker infrastructure
Write-Host "[1/4] Starting infrastructure (Kafka, Postgres, Redis)..." -ForegroundColor Yellow
Set-Location $Root
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker compose failed. Is Docker Desktop running?" -ForegroundColor Red
    Write-Host "  Start Docker Desktop first, then re-run this script." -ForegroundColor Yellow
    exit 1
}
Write-Host "      Waiting 15s for services..." -ForegroundColor Gray
Start-Sleep 15

if ($InfraOnly) {
    Write-Host ""
    Write-Host "[OK] Infrastructure running." -ForegroundColor Green
    Write-Host "  Kafka UI:  http://localhost:8090"
    Write-Host "  Postgres:  localhost:5432  (admin / admin123)"
    Write-Host "  Redis:     localhost:6379"
    exit
}

# Helper to start a service
function Start-Service {
    param($Name, $Script, $LogFile)
    $Script | Out-File $LogFile.Replace(".log","-cmd.ps1") -Encoding utf8
    $proc = Start-Process powershell -ArgumentList "-NonInteractive -File `"$($LogFile.Replace('.log','-cmd.ps1'))`"" -PassThru -WindowStyle Hidden
    Write-Host "      $Name started (PID: $($proc.Id)), logging to $LogFile" -ForegroundColor Gray
    return $proc
}

# 2. Backend API
Write-Host "[2/4] Starting Backend API (port 8000)..." -ForegroundColor Yellow
Start-Service "Backend" @"
`$env:PYTHONPATH = '$Root'
Set-Location '$Root'
python -m uvicorn backend.app:app --host 0.0.0.0 --port 8000 2>&1 | Tee-Object -FilePath '$Root\logs\backend.log'
"@ "$Root\logs\backend.log"

# 3. Data Agent API
Write-Host "[3/4] Starting Data Agent API (port 8001)..." -ForegroundColor Yellow
Start-Service "DataAgent" @"
`$env:PYTHONPATH = '$Root'
Set-Location '$Root\agents\data_agent'
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8001 2>&1 | Tee-Object -FilePath '$Root\logs\data-agent.log'
"@ "$Root\logs\data-agent.log"

# 4. Frontend
Write-Host "[4/4] Starting Frontend (port 3000)..." -ForegroundColor Yellow
Start-Service "Frontend" @"
Set-Location '$Root\frontend'
npm run dev 2>&1 | Tee-Object -FilePath '$Root\logs\frontend.log'
"@ "$Root\logs\frontend.log"

Write-Host ""
Write-Host "Waiting 25 seconds for all services..." -ForegroundColor Gray
Start-Sleep 25

Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  All Services Running" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Frontend      http://localhost:3000" -ForegroundColor White
Write-Host "  Backend API   http://localhost:8000/docs" -ForegroundColor White
Write-Host "  Agent API     http://localhost:8001/docs" -ForegroundColor White
Write-Host "  Kafka UI      http://localhost:8090" -ForegroundColor White
Write-Host ""
Write-Host "  Status:       .\scripts\start-dev.ps1 -Status" -ForegroundColor Gray
Write-Host "  Stop all:     .\scripts\start-dev.ps1 -StopAll" -ForegroundColor Gray
Write-Host "  Logs:         d:\projects\ai_agent_app\logs\" -ForegroundColor Gray
Write-Host ""
