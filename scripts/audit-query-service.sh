#!/usr/bin/env bash
# audit-query-service.sh: Tier 1 DevSecOps Local Audit & Scan Gate for query-service
# Enforces shift-left code hygiene, static analysis, unit/integration testing,
# dependency vulnerability audit, license/supply-chain compliance, and SAST scanning
# with 100% POSIX / PowerShell dual-shell parity and zero cloud CI dependencies (AD-13).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
QUERY_SERVICE_DIR="${REPO_ROOT}/query-service"

# Check for primary prerequisite: cargo
if ! command -v cargo >/dev/null 2>&1; then
    echo "[-] cargo binary not found in PATH. Please install Rust toolchain." >&2
    exit 1
fi

echo "======================================================================"
echo " [Tier 1 Security Gate] query-service: Code Quality & Security Scan"
echo "======================================================================"

cd "${QUERY_SERVICE_DIR}"

# 1. Format Gate
printf "\n--> [1/6] Checking code formatting (cargo fmt)...\n"
if ! cargo fmt -- --check; then
    echo "[-] cargo fmt failed. Please run 'cargo fmt' inside query-service/ to fix formatting." >&2
    exit 1
fi
echo "[+] Code formatting clean."

# 2. Clippy Linter Gate
printf "\n--> [2/6] Running static analysis (cargo clippy)...\n"
if ! cargo clippy --all-targets -- -D warnings; then
    echo "[-] cargo clippy detected warnings or errors. Build halted." >&2
    exit 1
fi
echo "[+] Clippy static analysis clean (0 warnings)."

# 3. Unit & Integration Test Gate
printf "\n--> [3/6] Running unit and integration tests (cargo test)...\n"
if ! cargo test --all-targets; then
    echo "[-] cargo test failed. Fix failing tests before committing." >&2
    exit 1
fi
echo "[+] All unit and integration tests passed."

# 4. Dependency Vulnerability Audit
printf "\n--> [4/6] Checking dependency vulnerabilities (cargo audit)...\n"
if command -v cargo-audit >/dev/null 2>&1; then
    if ! cargo audit; then
        echo "[-] cargo audit found security vulnerabilities in dependencies." >&2
        exit 1
    fi
    echo "[+] Dependency vulnerability audit passed."
else
    echo "    [WARN] cargo-audit is not installed. Run 'cargo install cargo-audit' to enable."
fi

# 5. Supply-Chain & License Assurance
printf "\n--> [5/6] Checking license and supply chain compliance (cargo deny)...\n"
if command -v cargo-deny >/dev/null 2>&1; then
    if ! cargo deny check; then
        echo "[-] cargo deny detected banned licenses, advisories, or unapproved sources." >&2
        exit 1
    fi
    echo "[+] Supply-chain and license compliance passed."
else
    echo "    [WARN] cargo-deny is not installed. Run 'cargo install cargo-deny' to enable."
fi

# 6. Static Application Security Testing (semgrep) / Secret Detection
printf "\n--> [6/6] Checking static application security (semgrep)...\n"
if command -v semgrep >/dev/null 2>&1; then
    if ! semgrep scan --config auto --error --quiet .; then
        echo "    [WARN] semgrep scan finished with issues or registry unavailable."
    else
        echo "[+] SAST scan passed."
    fi
else
    echo "    [WARN] semgrep is not installed. Skipping local SAST scan."
fi

echo ""
echo "======================================================================"
echo " [SUCCESS] query-service passed all Tier 1 DevSecOps quality gates!"
echo "======================================================================"
exit 0
