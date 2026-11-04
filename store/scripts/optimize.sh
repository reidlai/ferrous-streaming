#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TABLE="${1:-orders_current_state}"
TARGET_MB="${2:-128}"

echo "==> Running authenticated lakehouse OPTIMIZE on table: ${TABLE}..."
python3 "${SCRIPT_DIR}/optimize.py" --table "${TABLE}" --target-mb "${TARGET_MB}"
