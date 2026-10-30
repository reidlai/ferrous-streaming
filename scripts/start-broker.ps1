<#
.SYNOPSIS
    Apache Iggy Broker PowerShell Startup Runner
.DESCRIPTION
    Cross-platform pre-flight validation and container lifecycle runner for ferrous-streaming.
#>

[CmdletBinding()]
param (
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunnerScript = Join-Path $ScriptDir "start-broker.py"

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
