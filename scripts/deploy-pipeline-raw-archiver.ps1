<#
.SYNOPSIS
    PowerShell wrapper for raw_event_archiver deployment (Story 3.6).
.DESCRIPTION
    Wraps scripts/deploy-pipeline-raw-archiver.py to deploy and verify the raw event archiver pipeline.
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")

Write-Host "=== [ferrous-streaming] Deploying Arroyo Pipeline: raw_event_archiver ===" -ForegroundColor Cyan

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python is required but neither python nor python3 was found in PATH."
    exit 1
}

$RunnerScript = Join-Path $ScriptDir "deploy-pipeline-raw-archiver.py"

Push-Location $RepoRoot
try {
    if ($ArgsList -and $ArgsList.Count -gt 0) {
        & $PythonCmd $RunnerScript @ArgsList
    } else {
        & $PythonCmd $RunnerScript
    }
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}

