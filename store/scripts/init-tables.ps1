# PowerShell Runner for ferrous-lakehouse table initialization
param(
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

if (-not $DataDir) {
    $DataDir = Join-Path $RepoRoot "data/delta"
}

Write-Host "==> Running ferrous-lakehouse table initialization at: $DataDir" -ForegroundColor Cyan
python "$ScriptDir/init-tables.py" --data-dir "$DataDir"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Table initialization failed."
    exit $LASTEXITCODE
}
Write-Host "==> Lakehouse tables initialized successfully." -ForegroundColor Green
