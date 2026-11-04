#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TABLE="${1:-orders_current_state}"
RETENTION_HOURS="${2:-168}"

echo "==> Running authenticated lakehouse VACUUM on table: ${TABLE}..."
python3 "${SCRIPT_DIR}/vacuum.py" --table "${TABLE}" --retention-hours "${RETENTION_HOURS}"
