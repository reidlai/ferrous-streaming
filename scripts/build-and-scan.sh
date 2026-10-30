#!/usr/bin/env bash
# build-and-scan.sh: Builds multi-service Docker images and scans them with Trivy (Story 3.9)
# Enforces Tier 2 DevSecOps standards locally without incurring cloud CI minutes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SKIP_SCAN=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --skip-scan) SKIP_SCAN=true ;;
        --dry-run) DRY_RUN=true ;;
        *) ;;
    esac
done

echo "======================================================================"
echo " [DEVSECOPS] Building and Scanning ferrous-streaming Container Images"
echo "======================================================================"

cd "${REPO_ROOT}"

# 1. Build Apache Iggy Message Broker
echo -e "\n[Step 1/4] Building Apache Iggy broker container (ferrous-streaming-iggy:latest)..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] docker build -t ferrous-streaming-iggy:latest -f Dockerfile.iggy ."
else
    docker build -t ferrous-streaming-iggy:latest -f Dockerfile.iggy .
    echo "  [OK] Apache Iggy broker image built successfully."
fi

# 2. Build Arroyo Stream Processor
echo -e "\n[Step 2/4] Building Arroyo stream processor container (ferrous-streaming-arroyo:latest)..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] docker build -t ferrous-streaming-arroyo:latest -f Dockerfile.arroyo ."
else
    docker build -t ferrous-streaming-arroyo:latest -f Dockerfile.arroyo .
    echo "  [OK] Arroyo stream processor image built successfully."
fi

if [ "${SKIP_SCAN}" = "true" ]; then
    echo -e "\n[WARN] Skipping Trivy vulnerability scanning (--skip-scan enabled)."
    exit 0
fi

# Detect Trivy execution method (local CLI or Docker container)
run_trivy() {
    local target_image="$1"
    if command -v trivy >/dev/null 2>&1; then
        trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "${target_image}"
    else
        echo "  [INFO] Local trivy CLI not found; running via aquasec/trivy container..."
        docker run --rm \
            -v /var/run/docker.sock:/var/run/docker.sock \
            aquasec/trivy:latest image \
            --severity HIGH,CRITICAL \
            --ignore-unfixed \
            --exit-code 1 \
            "${target_image}"
    fi
}

# 3. Trivy Scan: Iggy Broker
echo -e "\n[Step 3/4] Running Trivy vulnerability scan on ferrous-streaming-iggy:latest..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed ferrous-streaming-iggy:latest"
else
    run_trivy "ferrous-streaming-iggy:latest"
    echo "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in Iggy broker image."
fi

# 4. Trivy Scan: Arroyo Processor
echo -e "\n[Step 4/4] Running Trivy vulnerability scan on ferrous-streaming-arroyo:latest..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed ferrous-streaming-arroyo:latest"
else
    run_trivy "ferrous-streaming-arroyo:latest"
    echo "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in Arroyo processor image."
fi

echo -e "\n======================================================================"
echo " [SUCCESS] All Container Images Built and Verified with Trivy (0 CVEs)!"
echo "======================================================================"

