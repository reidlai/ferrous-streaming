# PowerShell Runner for authenticated lakehouse OPTIMIZE
param(
    [string]$Table = "orders_current_state",
    [int]$TargetMB = 128
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==> Running authenticated lakehouse OPTIMIZE on table: $Table..." -ForegroundColor Cyan
python "$ScriptDir/optimize.py" --table "$Table" --target-mb $TargetMB
if ($LASTEXITCODE -ne 0) {
    Write-Error "OPTIMIZE failed."
    exit $LASTEXITCODE
}
Write-Host "==> OPTIMIZE completed successfully." -ForegroundColor Green
