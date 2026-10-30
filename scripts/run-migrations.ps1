<#
.SYNOPSIS
    Applies Arroyo SQL streaming pipeline migrations (Story 3.6 & 3.7).
.DESCRIPTION
    Deploys Arroyo streaming pipelines into the running cluster.
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")

Write-Host "=== [ferrous-streaming] Applying Streaming Pipeline Migrations ===" -ForegroundColor Cyan

Push-Location $RepoRoot
try {
    # Deploy Pipeline 1: raw_event_archiver (Story 3.6)
    Write-Host "--> Applying Pipeline 1: raw_event_archiver..." -ForegroundColor Yellow
    $Deploy1 = Join-Path $ScriptDir "deploy-pipeline-raw-archiver.ps1"
    & $Deploy1 @ArgsList
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    # Deploy Pipeline 2: state_snapshot_consolidator (Story 3.7)
    Write-Host "--> Applying Pipeline 2: state_snapshot_consolidator..." -ForegroundColor Yellow
    $Deploy2 = Join-Path $ScriptDir "deploy-pipeline-state-consolidator.ps1"
    & $Deploy2 @ArgsList
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    Write-Host "=== [ferrous-streaming] All Streaming Pipeline Migrations Applied Successfully ===" -ForegroundColor Green
} finally {
    Pop-Location
}

