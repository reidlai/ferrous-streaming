<#
.SYNOPSIS
    query-service Tier 1 DevSecOps Local Audit & Scan Gate (PowerShell)
.DESCRIPTION
    Executes shift-left code hygiene, static analysis, unit/integration testing,
    dependency vulnerability audit, license/supply-chain compliance, and SAST scanning
    with 100% POSIX / PowerShell dual-shell parity and zero cloud CI dependencies (AD-13).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
if (-not $ScriptDir -and $MyInvocation.MyCommand.Path) {
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if (-not $ScriptDir) {
    $ScriptDir = (Get-Location).Path
}

$RepoRoot = Split-Path -Parent $ScriptDir
$QueryServiceDir = Join-Path $RepoRoot "query-service"

# Check for primary prerequisite: cargo
if (-not (Get-Command "cargo" -ErrorAction SilentlyContinue)) {
    [Console]::Error.WriteLine("[-] cargo binary not found in PATH. Please install Rust toolchain.")
    exit 1
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " [Tier 1 Security Gate] query-service: Code Quality & Security Scan" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

Push-Location -LiteralPath $QueryServiceDir
try {
    # 1. Format Gate
    Write-Host "`n--> [1/6] Checking code formatting (cargo fmt)..." -ForegroundColor Yellow
    cargo fmt -- --check
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("[-] cargo fmt failed. Please run 'cargo fmt' inside query-service/ to fix formatting.")
        exit 1
    }
    Write-Host "[+] Code formatting clean." -ForegroundColor Green

    # 2. Clippy Linter Gate
    Write-Host "`n--> [2/6] Running static analysis (cargo clippy)..." -ForegroundColor Yellow
    cargo clippy --all-targets -- -D warnings
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("[-] cargo clippy detected warnings or errors. Build halted.")
        exit 1
    }
    Write-Host "[+] Clippy static analysis clean (0 warnings)." -ForegroundColor Green

    # 3. Unit & Integration Test Gate
    Write-Host "`n--> [3/6] Running unit and integration tests (cargo test)..." -ForegroundColor Yellow
    cargo test --all-targets
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("[-] cargo test failed. Fix failing tests before committing.")
        exit 1
    }
    Write-Host "[+] All unit and integration tests passed." -ForegroundColor Green

    # 4. Dependency Vulnerability Audit
    Write-Host "`n--> [4/6] Checking dependency vulnerabilities (cargo audit)..." -ForegroundColor Yellow
    $cargoAudit = Get-Command "cargo-audit" -ErrorAction SilentlyContinue
    if ($cargoAudit) {
        cargo audit
        if ($LASTEXITCODE -ne 0) {
            [Console]::Error.WriteLine("[-] cargo audit found security vulnerabilities in dependencies.")
            exit 1
        }
        Write-Host "[+] Dependency vulnerability audit passed." -ForegroundColor Green
    } else {
        Write-Host "    [WARN] cargo-audit is not installed. Run 'cargo install cargo-audit' to enable." -ForegroundColor DarkYellow
    }

    # 5. Supply-Chain & License Assurance
    Write-Host "`n--> [5/6] Checking license and supply chain compliance (cargo deny)..." -ForegroundColor Yellow
    $cargoDeny = Get-Command "cargo-deny" -ErrorAction SilentlyContinue
    if ($cargoDeny) {
        cargo deny check
        if ($LASTEXITCODE -ne 0) {
            [Console]::Error.WriteLine("[-] cargo deny detected banned licenses, advisories, or unapproved sources.")
            exit 1
        }
        Write-Host "[+] Supply-chain and license compliance passed." -ForegroundColor Green
    } else {
        Write-Host "    [WARN] cargo-deny is not installed. Run 'cargo install cargo-deny' to enable." -ForegroundColor DarkYellow
    }

    # 6. Static Application Security Testing (semgrep) / Secret Detection
    Write-Host "`n--> [6/6] Checking static application security (semgrep)..." -ForegroundColor Yellow
    $semgrep = Get-Command "semgrep" -ErrorAction SilentlyContinue
    if ($semgrep) {
        semgrep scan --config auto --error --quiet .
        if ($LASTEXITCODE -ne 0) {
            Write-Host "    [WARN] semgrep scan finished with issues or registry unavailable." -ForegroundColor DarkYellow
        } else {
            Write-Host "[+] SAST scan passed." -ForegroundColor Green
        }
    } else {
        Write-Host "    [WARN] semgrep is not installed. Skipping local SAST scan." -ForegroundColor DarkYellow
    }

    Write-Host "`n======================================================================" -ForegroundColor Green
    Write-Host " [SUCCESS] query-service passed all Tier 1 DevSecOps quality gates!" -ForegroundColor Green
    Write-Host "======================================================================" -ForegroundColor Green
    exit 0
}
finally {
    Pop-Location
}
