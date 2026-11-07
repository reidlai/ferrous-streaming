# PowerShell Runner for lakehouse OPTIMIZE
param(
    [string]$Table = "orders_current_state",
    [int]$TargetMB = 128,
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

Write-Host "==> Running lakehouse OPTIMIZE on table: $Table (Target: ${TargetMB}MB)..." -ForegroundColor Cyan
$cmdArgs = @("--table", $Table, "--target-mb", $TargetMB)
if ($DataDir) {
    $cmdArgs += @("--data-dir", $DataDir)
}
python "$ScriptDir/optimize.py" @cmdArgs
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    Write-Host "OPTIMIZE failed with exit code $exitCode." -ForegroundColor Red
    exit $exitCode
}
Write-Host "==> OPTIMIZE completed successfully." -ForegroundColor Green
