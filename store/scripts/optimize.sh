#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "==> Running lakehouse OPTIMIZE..."
if [ $# -eq 0 ]; then
    python3 "${SCRIPT_DIR}/optimize.py" --table orders_current_state --target-mb 128
elif [ "${1#-}" != "$1" ]; then
    python3 "${SCRIPT_DIR}/optimize.py" "$@"
else
    TABLE="${1:-orders_current_state}"
    TARGET_MB="${2:-128}"
    python3 "${SCRIPT_DIR}/optimize.py" --table "${TABLE}" --target-mb "${TARGET_MB}"
fi
