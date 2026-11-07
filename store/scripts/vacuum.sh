#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "==> Running lakehouse VACUUM..."
if [ $# -eq 0 ]; then
    python3 "${SCRIPT_DIR}/vacuum.py" --table orders_current_state --retention-hours 168
elif [ "${1#-}" != "$1" ]; then
    python3 "${SCRIPT_DIR}/vacuum.py" "$@"
else
    TABLE="${1:-orders_current_state}"
    RETENTION_HOURS="${2:-168}"
    python3 "${SCRIPT_DIR}/vacuum.py" --table "${TABLE}" --retention-hours "${RETENTION_HOURS}"
fi
