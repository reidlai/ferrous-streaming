#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "==> Running ferrous-lakehouse table initialization..."
if [ $# -eq 0 ]; then
    python3 "${SCRIPT_DIR}/init-tables.py" --data-dir "${REPO_ROOT}/data/delta"
elif [ "$1" = "--data-dir" ]; then
    python3 "${SCRIPT_DIR}/init-tables.py" "$@"
else
    python3 "${SCRIPT_DIR}/init-tables.py" --data-dir "$1"
fi
