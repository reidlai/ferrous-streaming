#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${1:-${REPO_ROOT}/data/delta}"

echo "==> Running ferrous-lakehouse table initialization..."
python3 "${SCRIPT_DIR}/init-tables.py" --data-dir "${DATA_DIR}"
