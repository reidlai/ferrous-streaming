#!/usr/bin/env bash
# Apache Iggy Broker POSIX Startup Runner
# Submodule: ferrous-streaming

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Prefer python3, fallback to python
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "[ERROR] Python 3.11+ is required but neither python3 nor python was found." >&2
    exit 1
fi

exec "${PYTHON_CMD}" "${SCRIPT_DIR}/start-broker.py" "$@"
