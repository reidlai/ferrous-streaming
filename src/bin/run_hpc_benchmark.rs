//! Pure-Rust CLI binary for running the HPC-MQBench qualification-first benchmark (Story 15.1, FR-32, SM-8).

use clap::Parser;
use ferrous_streaming::benchmark_runner::{
    format_report, run_benchmark, BenchmarkConfig, BenchmarkEndpoints, DEFAULT_ARROYO_API_URL,
    DEFAULT_BENCHMARK_TOPIC, DEFAULT_IGGY_HTTP_URL, DEFAULT_QUERY_SERVICE_URL,
    DEFAULT_REPLAY_PROFILE_PATH, DEFAULT_SINK_TYPE, DEFAULT_SINK_URL,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "run-hpc-benchmark",
    about = "Run reproducible HPC-MQBench qualification-first benchmark across the ferrous-streaming stack"
)]
struct Args {
    /// Number of records to generate when --input-file-path is not provided
    #[arg(long, default_value_t = 1000)]
    count: u64,

    /// Target ingestion rate in messages per second
    #[arg(long, default_value_t = 25)]
    rate: u64,

    /// Payload size in bytes
    #[arg(long, default_value_t = 1024)]
    payload_size: usize,

    /// Number of partitions
    #[arg(long, default_value_t = 4)]
    partitions: u32,

    /// Explicit benchmark run identifier
    #[arg(long, default_value = "")]
    run_id: String,

    /// Output JSON results instead of a formatted table report
    #[arg(long)]
    json: bool,

    /// Simulate packet/record drop fraction for testing (e.g. 0.1)
    #[arg(long, default_value_t = 0.0)]
    simulate_loss: f64,

    /// Simulate duplicate record fraction for testing (e.g. 0.1)
    #[arg(long, default_value_t = 0.0)]
    simulate_dup: f64,

    /// Target stream-replay-rs sink type (iggy, kafka, nats, fluvio, iceoryx2, stdout)
    #[arg(long, default_value = DEFAULT_SINK_TYPE)]
    sink: String,

    /// Target Iggy-Kafka Bridge or broker endpoint URL for stream-replay-rs
    #[arg(long, env = "BRIDGE_URL", default_value = DEFAULT_SINK_URL)]
    sink_url: String,

    /// Apache Iggy HTTP API URL for broker liveness and topic/offset verification
    #[arg(long, env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_HTTP_URL)]
    iggy_url: String,

    /// Arroyo Control Plane API URL for pipeline health verification
    #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_API_URL)]
    arroyo_url: String,

    /// Query Service HTTP API URL for Delta Lake reconciliation
    #[arg(long, env = "QUERY_SERVICE_URL", default_value = DEFAULT_QUERY_SERVICE_URL)]
    query_url: String,

    /// Target benchmark topic name
    #[arg(long, env = "BENCHMARK_TOPIC", default_value = DEFAULT_BENCHMARK_TOPIC)]
    topic: String,

    /// Optional pre-generated NDJSON workload file (e.g. ./hpc_workload.jsonl)
    #[arg(short = 'i', long)]
    input_file_path: Option<PathBuf>,

    /// Path to stream-replay-rs TOML profile
    #[arg(short = 'c', long, default_value = DEFAULT_REPLAY_PROFILE_PATH)]
    config: PathBuf,

    /// Run offline deterministic qualification verification without probing live Docker containers
    #[arg(long, visible_alias = "dry-run")]
    offline: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let run_id = if args.run_id.trim().is_empty() {
        let short_id = &uuid::Uuid::new_v4().simple().to_string()[..12];
        format!("hpc-{short_id}")
    } else {
        args.run_id
    };

    let config = BenchmarkConfig {
        run_id,
        rate_msg_sec: args.rate,
        payload_size_bytes: args.payload_size,
        total_records: args.count,
        partitions: args.partitions,
        topic: args.topic,
        simulate_loss_rate: args.simulate_loss,
        simulate_dup_rate: args.simulate_dup,
        input_file_path: args.input_file_path,
        config_path: args.config,
        offline: args.offline,
        endpoints: BenchmarkEndpoints {
            sink: args.sink,
            sink_url: args.sink_url,
            iggy_url: args.iggy_url,
            arroyo_url: args.arroyo_url,
            query_url: args.query_url,
        },
    };

    match run_benchmark(&config).await {
        Ok(result) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&result).unwrap_or_default()
                );
            } else {
                println!("{}", format_report(&result));
            }
            if result.status != "QUALIFIED" {
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("[ERROR] HPC Benchmark failed: {err}");
            std::process::exit(1);
        }
    }
}
