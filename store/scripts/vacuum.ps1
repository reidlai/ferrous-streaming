# PowerShell Runner for authenticated lakehouse VACUUM
param(
    [string]$Table = "orders_current_state",
    [int]$RetentionHours = 168
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==> Running authenticated lakehouse VACUUM on table: $Table (Retention: ${RetentionHours}h)..." -ForegroundColor Cyan
python "$ScriptDir/vacuum.py" --table "$Table" --retention-hours $RetentionHours
if ($LASTEXITCODE -ne 0) {
    Write-Error "VACUUM failed."
    exit $LASTEXITCODE
}
Write-Host "==> VACUUM completed successfully." -ForegroundColor Green
