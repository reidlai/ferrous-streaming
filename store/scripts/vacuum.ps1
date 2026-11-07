# PowerShell Runner for lakehouse VACUUM
param(
    [string]$Table = "orders_current_state",
    [int]$RetentionHours = 168,
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

Write-Host "==> Running lakehouse VACUUM on table: $Table (Retention: ${RetentionHours}h)..." -ForegroundColor Cyan
$cmdArgs = @("--table", $Table, "--retention-hours", $RetentionHours)
if ($DataDir) {
    $cmdArgs += @("--data-dir", $DataDir)
}
python "$ScriptDir/vacuum.py" @cmdArgs
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    Write-Host "VACUUM failed with exit code $exitCode." -ForegroundColor Red
    exit $exitCode
}
Write-Host "==> VACUUM completed successfully." -ForegroundColor Green
