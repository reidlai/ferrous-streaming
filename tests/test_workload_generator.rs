use ferrous_streaming::WorkloadRecord;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::Command;
use std::time::Instant;
use tempfile::NamedTempFile;

#[test]
fn test_cli_file_output_and_exact_byte_sizes() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args([
            "--count",
            "1000",
            "--payload-size",
            "1024",
            "--partitions",
            "4",
            "--run-id",
            "bench-run-99",
            "--output",
            &temp_path,
        ])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let file = File::open(&temp_path).unwrap();
    let reader = BufReader::new(file);

    let mut line_count = 0;
    for (seq, line_res) in reader.lines().enumerate() {
        let line = line_res.unwrap();
        // Byte length of the serialized JSON (excluding newline) must match payload_size_bytes exactly
        assert_eq!(
            line.len(),
            1024,
            "Line {} length {} != 1024",
            seq,
            line.len()
        );

        let rec: WorkloadRecord =
            serde_json::from_str(&line).expect("Must parse into WorkloadRecord");
        assert_eq!(rec.run_id, "bench-run-99");
        assert_eq!(rec.seq, seq as u64);
        assert_eq!(rec.record_id, format!("bench-run-99:{:08}", seq));
        assert_eq!(rec.partition, (seq % 4) as u32);
        assert_eq!(rec.payload_size_bytes, 1024);
        line_count += 1;
    }

    assert_eq!(line_count, 1000);
}

#[test]
fn test_cli_stdout_stream() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-c", "100", "-s", "256", "-p", "2", "-o", "-"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(output.status.success());
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout_str.lines().collect();
    assert_eq!(lines.len(), 100);

    for (seq, line) in lines.iter().enumerate() {
        assert_eq!(line.len(), 256);
        let rec: WorkloadRecord = serde_json::from_str(line).unwrap();
        assert_eq!(rec.seq, seq as u64);
        assert_eq!(rec.partition, (seq % 2) as u32);
        assert_eq!(rec.payload_size_bytes, 256);
    }
}

#[test]
fn test_cli_high_throughput_generation() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-c", "50000", "-s", "512", "-p", "4", "-o", &temp_path])
        .output()
        .expect("Failed to execute workload-generator");

    let duration = start.elapsed();
    assert!(output.status.success());

    let records_per_sec = 50_000.0 / duration.as_secs_f64();
    println!(
        "Workload generation: 50,000 records in {:?} ({:.0} recs/sec)",
        duration, records_per_sec
    );

    // Generation of 50k records in Rust should easily take under 2.5 seconds (>20,000 rec/sec even in unoptimized debug mode)
    assert!(
        duration.as_secs_f64() < 3.0,
        "Generation too slow: {:?}",
        duration
    );
}

#[test]
fn test_cli_empty_run_id_rejected() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["--run-id", "", "-c", "10"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error: --run-id cannot be empty"));
}

#[test]
fn test_cli_invalid_output_path_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-o", "/non_existent_dir_99999/test.ndjson", "-c", "10"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error opening output file"));
}

#[test]
fn test_cli_zero_partitions_handled_safely() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-p", "0", "-c", "5", "-o", "-"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(output.status.success());
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    for line in stdout_str.lines() {
        let rec: WorkloadRecord = serde_json::from_str(line).unwrap();
        assert_eq!(rec.partition, 0);
    }
}

#[test]
fn test_cli_undersized_payload_fallback() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-s", "50", "-c", "5", "-o", "-"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(output.status.success());
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    for line in stdout_str.lines() {
        let rec: WorkloadRecord = serde_json::from_str(line).unwrap();
        assert!(rec.data.is_empty());
    }
}
