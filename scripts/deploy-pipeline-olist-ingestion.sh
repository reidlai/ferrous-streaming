#!/usr/bin/env bash
# Deploy and Verify Arroyo Olist Historical Ingestion Pipeline (Story 3.10)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/deploy-pipeline-olist-ingestion.py"

python3 "${PY_SCRIPT}" "$@"
