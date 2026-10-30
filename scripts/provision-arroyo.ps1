<#
.SYNOPSIS
    PowerShell wrapper for Arroyo Stream Processor deployment & ingestion verification (Story 3.5).
.DESCRIPTION
    Wraps scripts/provision-arroyo.py to verify Arroyo health on port 5115, broker connectivity,
    and M2M authentication for svc_ferrous_streaming.
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")

Write-Host "=== [ferrous-streaming] Provisioning Arroyo Stream Processor & Ingestion ===" -ForegroundColor Cyan

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python is required but neither python nor python3 was found in PATH."
    exit 1
}

$RunnerScript = Join-Path $ScriptDir "provision-arroyo.py"

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

