# PowerShell Runner for ferrous-lakehouse table initialization
param(
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$RepoRoot = Split-Path -Parent $ScriptDir

if (-not $DataDir) {
    $DataDir = Join-Path $RepoRoot "data/delta"
}

Write-Host "==> Running ferrous-lakehouse table initialization at: $DataDir" -ForegroundColor Cyan
python "$ScriptDir/init-tables.py" --data-dir "$DataDir"
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    Write-Host "Table initialization failed with exit code $exitCode." -ForegroundColor Red
    exit $exitCode
}
Write-Host "==> Lakehouse tables initialized successfully." -ForegroundColor Green
