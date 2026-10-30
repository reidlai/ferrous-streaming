<#
.SYNOPSIS
    Apache Iggy Zero-Copy Multi-Consumer Group PowerShell Provisioner
.DESCRIPTION
    Provisions consumer groups on topics 'orders/intake' and 'orders/consolidated'.
    Story: 3.4 (Zero-Copy Multi-Consumer Group Fan-Out)
#>

[CmdletBinding()]
param (
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$RunnerScript = Join-Path $ScriptDir "provision-consumer-groups.py"

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python 3.11+ is required but neither python nor python3 was found in PATH."
    exit 1
}

Write-Host "=== [ferrous-streaming] Provisioning Zero-Copy Consumer Groups ===" -ForegroundColor Cyan

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

