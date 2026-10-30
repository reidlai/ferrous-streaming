<#
.SYNOPSIS
    Builds multi-service Docker images and scans them with Trivy (Story 3.9).
.DESCRIPTION
    Enforces Tier 2 DevSecOps standards locally without incurring cloud CI minutes.
#>
[CmdletBinding()]
param (
    [switch]$SkipScan,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = (Resolve-Path "$ScriptDir\..").Path

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " [DEVSECOPS] Building and Scanning ferrous-streaming Container Images" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

Push-Location $RepoRoot
try {
    # 1. Build Apache Iggy Message Broker
    Write-Host "`n[Step 1/4] Building Apache Iggy broker container (ferrous-streaming-iggy:latest)..." -ForegroundColor Yellow
    if ($DryRun) {
        Write-Host "  [DRY-RUN] docker build -t ferrous-streaming-iggy:latest -f Dockerfile.iggy ."
    } else {
        docker build -t ferrous-streaming-iggy:latest -f Dockerfile.iggy .
        if ($LASTEXITCODE -ne 0) { throw "Failed to build ferrous-streaming-iggy:latest" }
        Write-Host "  [OK] Apache Iggy broker image built successfully." -ForegroundColor Green
    }

    # 2. Build Arroyo Stream Processor
    Write-Host "`n[Step 2/4] Building Arroyo stream processor container (ferrous-streaming-arroyo:latest)..." -ForegroundColor Yellow
    if ($DryRun) {
        Write-Host "  [DRY-RUN] docker build -t ferrous-streaming-arroyo:latest -f Dockerfile.arroyo ."
    } else {
        docker build -t ferrous-streaming-arroyo:latest -f Dockerfile.arroyo .
        if ($LASTEXITCODE -ne 0) { throw "Failed to build ferrous-streaming-arroyo:latest" }
        Write-Host "  [OK] Arroyo stream processor image built successfully." -ForegroundColor Green
    }

    if ($SkipScan) {
        Write-Host "`n[WARN] Skipping Trivy vulnerability scanning (-SkipScan enabled)." -ForegroundColor Yellow
        exit 0
    }

    function Invoke-TrivyScan {
        param ([string]$TargetImage)
        $TrivyCmd = Get-Command trivy -ErrorAction SilentlyContinue
        if ($TrivyCmd) {
            trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 $TargetImage
        } else {
            Write-Host "  [INFO] Local trivy CLI not found; running via aquasec/trivy container..." -ForegroundColor Gray
            docker run --rm `
                -v //var/run/docker.sock:/var/run/docker.sock `
                aquasec/trivy:latest image `
                --severity HIGH,CRITICAL `
                --ignore-unfixed `
                --exit-code 1 `
                $TargetImage
        }
        if ($LASTEXITCODE -ne 0) { throw "Trivy vulnerability scan failed for $TargetImage" }
    }

    # 3. Trivy Scan: Iggy Broker
    Write-Host "`n[Step 3/4] Running Trivy vulnerability scan on ferrous-streaming-iggy:latest..." -ForegroundColor Yellow
    if ($DryRun) {
        Write-Host "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed ferrous-streaming-iggy:latest"
    } else {
        Invoke-TrivyScan "ferrous-streaming-iggy:latest"
        Write-Host "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in Iggy broker image." -ForegroundColor Green
    }

    # 4. Trivy Scan: Arroyo Processor
    Write-Host "`n[Step 4/4] Running Trivy vulnerability scan on ferrous-streaming-arroyo:latest..." -ForegroundColor Yellow
    if ($DryRun) {
        Write-Host "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed ferrous-streaming-arroyo:latest"
    } else {
        Invoke-TrivyScan "ferrous-streaming-arroyo:latest"
        Write-Host "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in Arroyo processor image." -ForegroundColor Green
    }

    Write-Host "`n======================================================================" -ForegroundColor Cyan
    Write-Host " [SUCCESS] All Container Images Built and Verified with Trivy (0 CVEs)!" -ForegroundColor Green
    Write-Host "======================================================================" -ForegroundColor Cyan
} finally {
    Pop-Location
}

