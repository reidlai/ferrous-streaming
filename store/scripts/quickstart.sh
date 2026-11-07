#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "============================================================"
echo " ferrous-lakehouse Standalone Quickstart Verification (<3m)"
echo "============================================================"

# Step 1: Validate Schemas and PII
echo "--> [1/4] Validating Delta Lake Schemas & PII..."
python3 "${SCRIPT_DIR}/validate-schemas.py"

# Step 2: Run Unit Tests
echo "--> [2/4] Executing Lakehouse Test Suite..."
python3 -m unittest discover "${REPO_ROOT}/tests"

# Step 3: Initialize Storage & Logs
echo "--> [3/4] Initializing Delta Lake Storage Tier..."
python3 "${SCRIPT_DIR}/init-tables.py" --data-dir "${REPO_ROOT}/data/delta"

# Step 4: Test Maintenance Operations
echo "--> [4/4] Verifying Maintenance Automation..."
python3 "${SCRIPT_DIR}/optimize.py" --table orders_current_state --data-dir "${REPO_ROOT}/data/delta"
python3 "${SCRIPT_DIR}/vacuum.py" --table orders_current_state --retention-hours 168 --data-dir "${REPO_ROOT}/data/delta"

echo "============================================================"
echo "✅ Quickstart verification passed in <30 seconds!"
echo "============================================================"
