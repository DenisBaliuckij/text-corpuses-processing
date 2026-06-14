$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not in PATH."
    exit 1
}

docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker daemon is not running. Start Docker Desktop and try again."
    exit 1
}

Write-Host "Building Airflow image (first run takes 10-20 minutes due to PyTorch and spaCy models)..."
docker compose build
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose build failed"; exit 1 }

Write-Host "Starting all services..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up failed"; exit 1 }

Write-Host ""
Write-Host "Airflow UI:  http://localhost:8080"
Write-Host "Login:       admin / admin"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  docker compose logs -f                     # stream logs from all services"
Write-Host "  docker compose logs -f airflow-scheduler   # scheduler logs only"
Write-Host "  docker compose down                        # stop and remove containers"
Write-Host "  docker compose down -v                     # stop and also delete the postgres volume"
