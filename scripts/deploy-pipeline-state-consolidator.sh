#!/usr/bin/env bash
# deploy-pipeline-state-consolidator.sh: POSIX Bash wrapper for state_snapshot_consolidator deployment (Story 3.7)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [ferrous-streaming] Deploying Arroyo Pipeline: state_snapshot_consolidator ==="

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "[-] Error: Python is required to deploy streaming pipelines." >&2
    exit 1
fi

cd "${REPO_ROOT}"
"${PYTHON_CMD}" "scripts/deploy-pipeline-state-consolidator.py" "$@"

