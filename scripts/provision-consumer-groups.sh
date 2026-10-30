#!/usr/bin/env bash
# provision-consumer-groups.sh: POSIX Bash wrapper for consumer groups provisioning (Story 3.4)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [ferrous-streaming] Provisioning Zero-Copy Consumer Groups ==="

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "[-] Error: Python is required to run consumer group provisioning." >&2
    exit 1
fi

cd "${REPO_ROOT}"
"${PYTHON_CMD}" "scripts/provision-consumer-groups.py" "$@"

