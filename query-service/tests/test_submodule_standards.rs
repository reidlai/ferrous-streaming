use std::fs;
use std::path::{Path, PathBuf};

fn manifest_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn repo_root_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("query-service parent is ferrous-streaming repo root")
        .join(relative)
}

#[test]
fn test_pre_commit_config_exists_and_valid() {
    let path = repo_root_path(".pre-commit-config.yaml");
    assert!(
        path.exists(),
        ".pre-commit-config.yaml must exist at repo root"
    );

    let content = fs::read_to_string(&path).expect("Failed to read .pre-commit-config.yaml");
    assert!(!content.trim().is_empty());
    assert!(
        content.contains("gitleaks"),
        "Pre-commit must include Gitleaks hook"
    );
    assert!(
        content.contains("id: query-service-cargo-fmt"),
        "Pre-commit must include explicit hook id: query-service-cargo-fmt"
    );
    assert!(
        content.contains("id: query-service-cargo-clippy"),
        "Pre-commit must include explicit hook id: query-service-cargo-clippy"
    );
    assert!(
        content.contains("id: query-service-cargo-test"),
        "Pre-commit must include explicit hook id: query-service-cargo-test"
    );
    assert!(
        content.contains("--all-targets"),
        "Pre-commit test hook must run with --all-targets"
    );
    assert!(
        content.contains("repos:"),
        "Must be a valid pre-commit config with repos block"
    );
}

#[test]
fn test_deny_toml_exists_and_configured() {
    let path = manifest_path("deny.toml");
    assert!(path.exists(), "deny.toml must exist in query-service/");

    let content = fs::read_to_string(&path).expect("Failed to read deny.toml");
    let parsed: toml::Value = toml::from_str(&content).expect("deny.toml must be valid TOML");

    assert!(parsed.get("advisories").is_some(), "Must have [advisories]");
    assert!(parsed.get("licenses").is_some(), "Must have [licenses]");
    assert!(parsed.get("bans").is_some(), "Must have [bans]");
    assert!(parsed.get("sources").is_some(), "Must have [sources]");

    let licenses = parsed.get("licenses").unwrap();
    let allow_list = licenses
        .get("allow")
        .and_then(|v| v.as_array())
        .expect("licenses.allow must be an array");

    let allowed_strings: Vec<&str> = allow_list.iter().filter_map(|v| v.as_str()).collect();
    assert!(allowed_strings.contains(&"MIT"), "Must allow MIT");
    assert!(
        allowed_strings.contains(&"Apache-2.0"),
        "Must allow Apache-2.0"
    );
    assert!(
        allowed_strings.contains(&"Unicode-3.0"),
        "Must allow Unicode-3.0"
    );
    assert!(
        allowed_strings.contains(&"BSD-3-Clause"),
        "Must allow BSD-3-Clause"
    );

    let sources = parsed.get("sources").unwrap();
    let allow_registries: Vec<&str> = sources
        .get("allow-registry")
        .and_then(|v| v.as_array())
        .expect("sources.allow-registry must be an array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        allow_registries.contains(&"https://github.com/rust-lang/crates.io-index"),
        "Must allow crates.io git index"
    );
    assert!(
        allow_registries.contains(&"https://index.crates.io/"),
        "Must allow crates.io sparse index"
    );
}

#[test]
fn test_cargo_audit_config_and_package_metadata() {
    let audit_path = manifest_path(".cargo/audit.toml");
    assert!(
        audit_path.exists(),
        ".cargo/audit.toml must exist for cargo-audit"
    );
    let audit_content = fs::read_to_string(&audit_path).expect("Failed to read .cargo/audit.toml");
    let audit_parsed: toml::Value =
        toml::from_str(&audit_content).expect(".cargo/audit.toml must be valid TOML");
    assert!(
        audit_parsed.get("advisories").is_some(),
        ".cargo/audit.toml must contain [advisories]"
    );

    let cargo_path = manifest_path("Cargo.toml");
    assert!(cargo_path.exists(), "Cargo.toml must exist");
    let cargo_content = fs::read_to_string(&cargo_path).expect("Failed to read Cargo.toml");
    let cargo_parsed: toml::Value =
        toml::from_str(&cargo_content).expect("Cargo.toml must be valid TOML");
    let package = cargo_parsed
        .get("package")
        .expect("Cargo.toml must have [package]");
    assert_eq!(
        package.get("license").and_then(|v| v.as_str()),
        Some("MIT OR Apache-2.0"),
        "Package license must be MIT OR Apache-2.0"
    );
    assert_eq!(
        package.get("readme").and_then(|v| v.as_str()),
        Some("README.md"),
        "Package readme must be README.md"
    );
    assert_eq!(
        package.get("publish").and_then(|v| v.as_bool()),
        Some(false),
        "query-service must have publish = false"
    );
}

#[test]
fn test_dual_shell_audit_scripts_parity() {
    let bash_script = repo_root_path("scripts/audit-query-service.sh");
    let ps_script = repo_root_path("scripts/audit-query-service.ps1");

    assert!(
        bash_script.exists(),
        "scripts/audit-query-service.sh must exist"
    );
    assert!(
        ps_script.exists(),
        "scripts/audit-query-service.ps1 must exist"
    );

    let bash_content = fs::read_to_string(&bash_script).expect("Failed to read bash audit script");
    let ps_content =
        fs::read_to_string(&ps_script).expect("Failed to read PowerShell audit script");

    // Check shebang and strict error handling in bash script
    assert!(
        bash_content.starts_with("#!/usr/bin/env bash"),
        "Bash audit script must have valid shebang"
    );
    assert!(
        bash_content.contains("set -euo pipefail"),
        "Bash audit script must enable strict error handling"
    );

    // Check PowerShell strict error handling and directory preservation
    assert!(
        ps_content.contains("$ErrorActionPreference = \"Stop\""),
        "PowerShell audit script must set ErrorActionPreference = Stop"
    );
    assert!(
        ps_content.contains("Push-Location") && ps_content.contains("Pop-Location"),
        "PowerShell audit script must preserve caller directory"
    );

    // Check prerequisite validation
    assert!(
        bash_content.contains("command -v cargo"),
        "Bash script must check for cargo binary"
    );
    assert!(
        ps_content.contains("Get-Command \"cargo\""),
        "PowerShell script must check for cargo binary"
    );

    // Check shared stages across dual shells
    for required_cmd in &[
        "cargo fmt",
        "cargo clippy",
        "cargo test",
        "cargo audit",
        "cargo deny",
        "semgrep",
    ] {
        assert!(
            bash_content.contains(required_cmd),
            "Bash audit script missing check: {required_cmd}"
        );
        assert!(
            ps_content.contains(required_cmd),
            "PowerShell audit script missing check: {required_cmd}"
        );
    }

    // Check graceful fallback indicators
    assert!(
        bash_content.contains("[WARN]"),
        "Bash script must contain graceful [WARN] fallback indicators"
    );
    assert!(
        ps_content.contains("[WARN]"),
        "PowerShell script must contain graceful [WARN] fallback indicators"
    );
}

#[test]
fn test_readme_documentation_completeness() {
    let readme_path = manifest_path("README.md");
    assert!(
        readme_path.exists(),
        "README.md must exist in query-service/"
    );

    let readme = fs::read_to_string(&readme_path).expect("Failed to read README.md");

    // Must have C4 architecture or Mermaid flow
    assert!(
        readme.contains("```mermaid"),
        "README.md must contain Mermaid architecture diagram"
    );
    assert!(
        readme.contains("query-service"),
        "README.md must document query-service"
    );
    assert!(
        readme.contains("DataFusion"),
        "README.md must document DataFusion engine"
    );

    // Endpoints documentation
    assert!(
        readme.contains("50051"),
        "README.md must document Flight SQL port 50051"
    );
    assert!(
        readme.contains("8085"),
        "README.md must document HTTP REST port 8085"
    );

    // Performance and benchmarks (<3ms p95)
    assert!(
        readme.contains("3ms") || readme.contains("3 ms"),
        "README.md must document sub-3ms performance latency targets"
    );

    // Extensible TableProvider federation
    assert!(
        readme.contains("TableProvider"),
        "README.md must document TableProvider federation"
    );
    assert!(
        readme.contains("SQLite") || readme.contains("sqlite"),
        "README.md must document SQLite federation"
    );
    assert!(
        readme.contains("Delta Lake") || readme.contains("delta"),
        "README.md must document Delta Lake catalog"
    );

    // Quickstart local verification script (<3 minutes)
    assert!(
        readme.contains("Quickstart") || readme.contains("quickstart"),
        "README.md must include quickstart section"
    );
}

#[test]
fn test_ferrous_streaming_root_readme_references_query_service() {
    let root_readme_path = repo_root_path("README.md");
    assert!(root_readme_path.exists(), "Root README.md must exist");

    let root_readme = fs::read_to_string(&root_readme_path).expect("Failed to read root README.md");
    assert!(
        root_readme.contains("query-service"),
        "Root ferrous-streaming README.md must reference query-service"
    );
    assert!(
        root_readme.contains("50051"),
        "Root README.md must document Flight SQL port 50051"
    );
    assert!(
        root_readme.contains("8085"),
        "Root README.md must document HTTP port 8085"
    );
}

#[test]
fn test_ad13_zero_cloud_ci_dependency_boundary() {
    let workflows_dir = repo_root_path(".github/workflows");
    assert!(
        !workflows_dir.exists(),
        "AD-13 violation: Submodule must have zero .github/workflows directory"
    );
}
