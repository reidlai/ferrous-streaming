#!/usr/bin/env bash
# provision-topics.sh: POSIX Bash wrapper for topic provisioning on Apache Iggy (Story 3.3)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [ferrous-streaming] Provisioning Stream & Partitioned Topics ==="

# Check python3
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "[-] Error: Python is required to run topic provisioning." >&2
    exit 1
fi

cd "${REPO_ROOT}"
"${PYTHON_CMD}" "${SCRIPT_DIR}/provision-topics.py" "$@"

