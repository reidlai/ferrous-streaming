#!/usr/bin/env bash
# provision-arroyo.sh: POSIX Bash wrapper for Arroyo deployment & ingestion verification (Story 3.5)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [ferrous-streaming] Provisioning Arroyo Stream Processor & Ingestion ==="

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "[-] Error: Python is required to run Arroyo provisioning." >&2
    exit 1
fi

cd "${REPO_ROOT}"
"${PYTHON_CMD}" "scripts/provision-arroyo.py" "$@"

