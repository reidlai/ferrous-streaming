//! Integration tests for cross-platform streaming administrative tools (Story 13.10)

use std::process::Command;

#[test]
fn test_provision_topics_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_provision-topics"))
        .arg("--dry-run")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute provision-topics");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Ingestion Topics & Partition Durability"));
    assert!(stdout.contains("Validated stream 'orders' (id: 1) specification."));
    assert!(stdout.contains("Validated topic 'intake' (id: 1, partitions: 4, fsync: immediate)"));
    assert!(stdout.contains("Validated topic 'consolidated' (id: 2, partitions: 4, fsync: immediate)"));
}

#[test]
fn test_provision_consumer_groups_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_provision-consumer-groups"))
        .arg("--dry-run")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute provision-consumer-groups");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Zero-Copy Consumer Groups"));
    assert!(stdout.contains("Validated consumer group 'cg-arroyo-raw' (id: 1)"));
    assert!(stdout.contains("Validated consumer group 'cg-cmd-handler' (id: 2)"));
    assert!(stdout.contains("Validated consumer group 'cg-arroyo-state' (id: 1)"));
}

#[test]
fn test_verify_arroyo_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_verify-arroyo"))
        .arg("--dry-run")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute verify-arroyo");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Verify Arroyo Stream Processor & Broker Ingestion"));
    assert!(stdout.contains("Validated Arroyo endpoint contract (port 5115)"));
    assert!(stdout.contains("Validated Apache Iggy health probe (port 3000/ping)"));
}

#[test]
fn test_deploy_pipelines_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_deploy-pipelines"))
        .arg("--dry-run")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute deploy-pipelines");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Run Schema Migrations & Deploy Streaming Pipelines"));
    assert!(stdout.contains("Validated pipeline 'raw_event_archiver'"));
    assert!(stdout.contains("Validated pipeline 'state_snapshot_consolidator'"));
    assert!(stdout.contains("orders_raw_events"));
    assert!(stdout.contains("orders_current_state"));
}

#[test]
fn test_streaming_admin_subcommands_dry_run() {
    // 1. streaming-admin provision-topics --dry-run
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["provision-topics", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin provision-topics");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Ingestion Topics & Partition Durability"));

    // 2. streaming-admin provision-consumer-groups --dry-run
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["provision-consumer-groups", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin provision-consumer-groups");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Zero-Copy Consumer Groups"));

    // 3. streaming-admin verify-arroyo --dry-run
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["verify-arroyo", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin verify-arroyo");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Verify Arroyo Stream Processor & Broker Ingestion"));

    // 4. streaming-admin deploy-pipelines --dry-run
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["deploy-pipelines", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin deploy-pipelines");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Run Schema Migrations & Deploy Streaming Pipelines"));
}

#[test]
fn test_streaming_admin_all_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["all", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin all");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[Step 2] Provision Ingestion Topics & Partition Durability"));
    assert!(stdout.contains("[Step 3] Provision Zero-Copy Consumer Groups"));
    assert!(stdout.contains("[Step 4] Verify Arroyo Stream Processor & Broker Ingestion"));
    assert!(stdout.contains("[Step 5] Run Schema Migrations & Deploy Streaming Pipelines"));
    assert!(stdout.contains("All streaming stack components provisioned & verified!"));
}

#[test]
fn test_deploy_pipelines_missing_dir_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_deploy-pipelines"))
        .args(["--pipeline-dir", "nonexistent_pipelines_dir_12345", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute deploy-pipelines with missing dir");

    // Must fail because required SQL pipelines cannot be found
    assert!(!output.status.success(), "Expected failure on missing pipeline directory");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not found in") || stderr.contains("nonexistent_pipelines_dir_12345"));
}
