<#
.SYNOPSIS
    Apache Iggy Ingestion Topic Partitioning PowerShell Provisioner & Verifier
.DESCRIPTION
    Provisions stream 'orders' and partitioned topics 'intake' and 'consolidated' (4 partitions each) on Apache Iggy.
    Story: 3.3 (Ingestion Topic Partitioning & Immediate fsync Durability)
#>

[CmdletBinding()]
param (
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$RunnerScript = Join-Path $ScriptDir "provision-topics.py"

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python 3.11+ is required but neither python nor python3 was found in PATH."
    exit 1
}

Write-Host "=== [ferrous-streaming] Provisioning Stream & Partitioned Topics ===" -ForegroundColor Cyan

Push-Location $RepoRoot
try {
    & $PythonCmd $RunnerScript @ArgsList
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
