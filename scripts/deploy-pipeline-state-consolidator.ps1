<#
.SYNOPSIS
    PowerShell wrapper for state_snapshot_consolidator deployment (Story 3.7).
.DESCRIPTION
    Wraps scripts/deploy-pipeline-state-consolidator.py to deploy and verify the state snapshot consolidator pipeline.
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")

Write-Host "=== [ferrous-streaming] Deploying Arroyo Pipeline: state_snapshot_consolidator ===" -ForegroundColor Cyan

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python is required but neither python nor python3 was found in PATH."
    exit 1
}

$RunnerScript = Join-Path $ScriptDir "deploy-pipeline-state-consolidator.py"

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
