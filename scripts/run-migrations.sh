#!/usr/bin/env bash
# run-migrations.sh: Applies Arroyo SQL streaming pipeline migrations (Story 3.6 & 3.7)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [ferrous-streaming] Applying Streaming Pipeline Migrations ==="

cd "${REPO_ROOT}"

# Deploy Pipeline 1: raw_event_archiver (Story 3.6)
echo "--> Applying Pipeline 1: raw_event_archiver..."
bash "${SCRIPT_DIR}/deploy-pipeline-raw-archiver.sh" "$@"

# Deploy Pipeline 2: state_snapshot_consolidator (Story 3.7)
echo "--> Applying Pipeline 2: state_snapshot_consolidator..."
bash "${SCRIPT_DIR}/deploy-pipeline-state-consolidator.sh" "$@"

echo "=== [ferrous-streaming] All Streaming Pipeline Migrations Applied Successfully ==="

