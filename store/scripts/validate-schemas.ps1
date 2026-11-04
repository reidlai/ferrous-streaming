# PowerShell Runner for ferrous-lakehouse schema validation
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

Write-Host "==> Running ferrous-lakehouse schema validation..." -ForegroundColor Cyan
python "$ScriptDir/validate-schemas.py"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Schema validation failed."
    exit $LASTEXITCODE
}
Write-Host "==> Schema validation completed successfully." -ForegroundColor Green
