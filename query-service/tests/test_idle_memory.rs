use query_service::{config::QueryConfig, context::QueryEngine};
use sysinfo::{Pid, ProcessesToUpdate, System};

#[tokio::test]
async fn test_idle_memory_overhead_under_40mb() {
    // NFR-9: baseline memory overhead remains under 40MB RAM at idle
    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");
    assert_eq!(engine.batch_size(), 8192);

    let mut sys = System::new_all().expect("failed to initialize System");
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let pid = Pid::from_u32(std::process::id());
    let process = sys
        .process(pid)
        .expect("current process should exist in process table");
    let memory_bytes = process.memory();
    let memory_mb = memory_bytes as f64 / (1024.0 * 1024.0);
    println!("Process RSS memory: {:.2} MB (threshold: 40 MB)", memory_mb);
    assert!(
        memory_mb < 40.0,
        "Idle memory overhead ({:.2} MB) exceeded 40MB limit (NFR-9)",
        memory_mb
    );
}
