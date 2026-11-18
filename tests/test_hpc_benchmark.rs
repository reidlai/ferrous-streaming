//! Integration tests for Pure-Rust HPC Benchmark Runner and stream-replay-rs Replay Profile (Story 15.1, FR-32, SM-8)

use std::path::PathBuf;
use std::process::Command;

#[test]
fn test_replay_profile_toml_contract() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let profile_path = manifest_dir.join("benchmark").join("replay-profile.toml");
    assert!(
        profile_path.is_file(),
        "benchmark/replay-profile.toml must exist at {:?}",
        profile_path
    );

    let content = std::fs::read_to_string(&profile_path).expect("read replay-profile.toml");
    let parsed: toml::Value = toml::from_str(&content).expect("parse replay-profile.toml as TOML");

    let dataset = parsed
        .get("dataset")
        .expect("[dataset] section must exist in replay-profile.toml");
    assert_eq!(
        dataset.get("input_path").and_then(|v| v.as_str()),
        Some("./hpc_workload.jsonl")
    );
    assert_eq!(
        dataset.get("format").and_then(|v| v.as_str()),
        Some("jsonl")
    );
    assert_eq!(
        dataset.get("timestamp_column").and_then(|v| v.as_str()),
        Some("client_timestamp_us")
    );
    assert_eq!(
        dataset.get("timestamp_unit").and_then(|v| v.as_str()),
        Some("us")
    );
    assert_eq!(
        dataset.get("partition_key").and_then(|v| v.as_str()),
        Some("record_id")
    );

    let replay = parsed
        .get("replay")
        .expect("[replay] section must exist in replay-profile.toml");
    assert_eq!(replay.get("rate").and_then(|v| v.as_integer()), Some(25));
    assert_eq!(
        replay.get("shift_timestamps").and_then(|v| v.as_bool()),
        Some(true)
    );

    let sink = parsed
        .get("sink")
        .expect("[sink] section must exist in replay-profile.toml");
    assert_eq!(
        sink.get("default_type").and_then(|v| v.as_str()),
        Some("iggy")
    );
    assert_eq!(
        sink.get("default_url").and_then(|v| v.as_str()),
        Some("127.0.0.1:9092")
    );
    assert_eq!(
        sink.get("destination").and_then(|v| v.as_str()),
        Some("benchmark.hpc_mqbench")
    );
}

#[test]
fn test_run_hpc_benchmark_qualified_json_and_endpoints() {
    let output = Command::new(env!("CARGO_BIN_EXE_run-hpc-benchmark"))
        .args([
            "--count",
            "200",
            "--rate",
            "25",
            "--payload-size",
            "1024",
            "--sink",
            "iggy",
            "--sink-url",
            "127.0.0.1:9092",
            "--iggy-url",
            "http://127.0.0.1:3000",
            "--arroyo-url",
            "http://127.0.0.1:5115",
            "--query-url",
            "http://127.0.0.1:8085",
            "--offline",
            "--json",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute run-hpc-benchmark");

    assert!(output.status.success(), "Command failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let res: serde_json::Value = serde_json::from_str(&stdout).expect("parse JSON output");

    assert_eq!(res["status"], "QUALIFIED");
    assert!(res["disqualification_reason"].is_null());
    assert_eq!(res["endpoints"]["sink"], "iggy");
    assert_eq!(res["endpoints"]["sink_url"], "127.0.0.1:9092");
    assert_eq!(res["endpoints"]["iggy_url"], "http://127.0.0.1:3000");
    assert_eq!(res["endpoints"]["arroyo_url"], "http://127.0.0.1:5115");
    assert_eq!(res["endpoints"]["query_url"], "http://127.0.0.1:8085");

    let replayer_cmd = res["replayer_command"].as_str().unwrap_or("");
    assert!(replayer_cmd.contains("stream-replay-rs --config benchmark/replay-profile.toml"));
    assert!(replayer_cmd.contains("--sink iggy --sink-url 127.0.0.1:9092"));
    assert!(replayer_cmd.contains("--topic benchmark.hpc_mqbench --rate 25"));

    assert_eq!(res["accounting"]["sent_records"], 200);
    assert_eq!(res["accounting"]["bridge_accepted"], 200);
    assert_eq!(res["accounting"]["iggy_stored"], 200);
    assert_eq!(res["accounting"]["arroyo_processed"], 200);
    assert_eq!(res["accounting"]["delta_stored"], 200);
    assert_eq!(res["accounting"]["distinct_delta_records"], 200);
}

#[test]
fn test_run_hpc_benchmark_with_pregenerated_jsonl_file() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let jsonl_path = tmp.path().join("hpc_workload.jsonl");

    // 1. Generate workload file with workload-generator
    let gen_out = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args([
            "--count",
            "50",
            "--payload-size",
            "1024",
            "--run-id",
            "hpc-file-test",
            "--output",
            jsonl_path.to_str().unwrap(),
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute workload-generator");
    assert!(gen_out.status.success(), "workload-generator failed: {:?}", gen_out);
    assert!(jsonl_path.is_file());

    // 2. Run benchmark reading from pre-generated JSONL file
    let bench_out = Command::new(env!("CARGO_BIN_EXE_run-hpc-benchmark"))
        .args([
            "--input-file-path",
            jsonl_path.to_str().unwrap(),
            "--offline",
            "--json",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute run-hpc-benchmark with --input-file-path");
    assert!(bench_out.status.success(), "run-hpc-benchmark failed: {:?}", bench_out);

    let stdout = String::from_utf8_lossy(&bench_out.stdout);
    let res: serde_json::Value = serde_json::from_str(&stdout).expect("parse JSON output");
    assert_eq!(res["status"], "QUALIFIED");
    assert_eq!(res["run_id"], "hpc-file-test");
    assert_eq!(res["accounting"]["sent_records"], 50);
    assert_eq!(res["accounting"]["delta_stored"], 50);
}

#[test]
fn test_run_hpc_benchmark_disqualified_on_loss_and_duplicates() {
    // Data loss disqualification
    let loss_out = Command::new(env!("CARGO_BIN_EXE_run-hpc-benchmark"))
        .args([
            "--count",
            "100",
            "--simulate-loss",
            "0.1",
            "--offline",
            "--json",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute run-hpc-benchmark with loss");
    assert!(!loss_out.status.success(), "Expected exit code 1 on data loss");
    let loss_json: serde_json::Value =
        serde_json::from_slice(&loss_out.stdout).expect("parse loss JSON");
    assert_eq!(loss_json["status"], "DISQUALIFIED");
    assert!(loss_json["disqualification_reason"]
        .as_str()
        .unwrap_or("")
        .contains("Data loss detected"));

    // Duplicate disqualification
    let dup_out = Command::new(env!("CARGO_BIN_EXE_run-hpc-benchmark"))
        .args([
            "--count",
            "100",
            "--simulate-dup",
            "0.1",
            "--offline",
            "--json",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute run-hpc-benchmark with dup");
    assert!(!dup_out.status.success(), "Expected exit code 1 on duplicates");
    let dup_json: serde_json::Value =
        serde_json::from_slice(&dup_out.stdout).expect("parse dup JSON");
    assert_eq!(dup_json["status"], "DISQUALIFIED");
    assert!(dup_json["disqualification_reason"]
        .as_str()
        .unwrap_or("")
        .contains("Duplicates detected"));
}

#[test]
fn test_run_hpc_benchmark_live_mode_unreachable_endpoint_fails() {
    let out = Command::new(env!("CARGO_BIN_EXE_run-hpc-benchmark"))
        .args([
            "--count",
            "10",
            "--sink-url",
            "127.0.0.1:59998",
            "--iggy-url",
            "http://127.0.0.1:59999",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute run-hpc-benchmark in live mode");

    assert!(
        !out.status.success(),
        "Expected failure when live endpoints are unreachable"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("127.0.0.1:59998") || stderr.contains("Cannot connect"),
        "Expected clear endpoint error in stderr, got: {}",
        stderr
    );
}

#[test]
fn test_readme_hpc_benchmark_documentation_completeness() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root_readme = std::fs::read_to_string(manifest_dir.join("README.md"))
        .expect("read ferrous-streaming/README.md");
    let bench_readme = std::fs::read_to_string(manifest_dir.join("benchmark").join("README.md"))
        .expect("read ferrous-streaming/benchmark/README.md");

    for (label, doc) in [
        ("README.md", &root_readme),
        ("benchmark/README.md", &bench_readme),
    ] {
        assert!(
            doc.contains("Server Endpoint Discovery & Configuration"),
            "{label} must contain 'Server Endpoint Discovery & Configuration'"
        );
        assert!(
            doc.contains("benchmark/replay-profile.toml"),
            "{label} must reference 'benchmark/replay-profile.toml'"
        );
        assert!(
            doc.contains("stream-replay-rs --config benchmark/replay-profile.toml --sink stdout"),
            "{label} must document dry-run dataset inspection with 'stream-replay-rs --config benchmark/replay-profile.toml --sink stdout'"
        );
        assert!(
            doc.contains("cargo run --bin workload-generator"),
            "{label} must document dataset generation with 'cargo run --bin workload-generator'"
        );
        assert!(
            doc.contains("cargo run --bin run-hpc-benchmark"),
            "{label} must document the pure-Rust benchmark runner 'cargo run --bin run-hpc-benchmark'"
        );
        for flag in [
            "--sink-url",
            "--iggy-url",
            "--arroyo-url",
            "--query-url",
            "BRIDGE_URL",
            "IGGY_HTTP_URL",
            "ARROYO_API_URL",
            "QUERY_SERVICE_URL",
        ] {
            assert!(
                doc.contains(flag),
                "{label} must document server endpoint flag/env '{flag}'"
            );
        }
    }
}
