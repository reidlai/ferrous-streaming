<#
.SYNOPSIS
    Apache Iggy Broker Administrative Security PowerShell Verification Runner
.DESCRIPTION
    Cross-platform token validation, Secret Zero inspection, and M2M security verification runner for ferrous-streaming.
    Story: 3.2 (Broker Administrative Security via Kanidm Service Account)
#>

[CmdletBinding()]
param (
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunnerScript = Join-Path $ScriptDir "verify-admin-auth.py"

$PythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    "python3"
} else {
    Write-Error "[ERROR] Python 3.11+ is required but neither python nor python3 was found in PATH."
    exit 1
}

& $PythonCmd $RunnerScript @ArgsList
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
