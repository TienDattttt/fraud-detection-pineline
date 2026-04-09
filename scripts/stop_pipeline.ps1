Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot
try {
    Write-Host "Stopping Fraud Detection Pipeline..."
    Write-Host "  [1/2] Stopping containers..."
    docker compose down

    Write-Host "  [2/2] Cleaning up..."
    Write-Host ""
    Write-Host "Pipeline stopped."
    Write-Host ""
    Write-Host "To also remove data volumes:"
    Write-Host "  docker compose down -v"
}
finally {
    Pop-Location
}
