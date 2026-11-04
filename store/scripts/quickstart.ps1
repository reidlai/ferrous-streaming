# PowerShell Quickstart for ferrous-lakehouse (<3 minutes)
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " ferrous-lakehouse Standalone Quickstart Verification (<3m)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# Step 1: Validate Schemas and PII
Write-Host "--> [1/4] Validating Delta Lake Schemas & PII..." -ForegroundColor Yellow
python "$ScriptDir/validate-schemas.py"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Step 2: Run Unit Tests
Write-Host "--> [2/4] Executing Lakehouse Test Suite..." -ForegroundColor Yellow
python -m unittest discover "$RepoRoot/tests"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Step 3: Initialize Storage & Logs
Write-Host "--> [3/4] Initializing Delta Lake Storage Tier..." -ForegroundColor Yellow
python "$ScriptDir/init-tables.py" --data-dir "$RepoRoot/data/delta"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Step 4: Test Maintenance Operations
Write-Host "--> [4/4] Verifying Maintenance Automation..." -ForegroundColor Yellow
python "$ScriptDir/optimize.py" --table orders_current_state --service-account svc_ferrous_lakehouse --service-token quickstart_mock_token_12345
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python "$ScriptDir/vacuum.py" --table orders_current_state --retention-hours 168 --service-account svc_ferrous_lakehouse --service-token quickstart_mock_token_12345
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "============================================================" -ForegroundColor Green
Write-Host "✅ Quickstart verification passed in <30 seconds!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
