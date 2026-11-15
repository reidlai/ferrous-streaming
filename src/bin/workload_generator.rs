use clap::Parser;
use ferrous_streaming::generate_workload_stream;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::process;
use uuid::Uuid;

/// HPC-MQBench Synthetic Workload Generator (FR-32)
#[derive(Parser, Debug)]
#[command(name = "workload-generator")]
#[command(about = "HPC-MQBench Synthetic Workload Generator (FR-32)", long_about = None)]
struct Args {
    /// Number of records to generate (default: 1000)
    #[arg(short = 'c', long, default_value_t = 1000)]
    count: u64,

    /// Target payload size in bytes (default: 1024)
    #[arg(short = 's', long, default_value_t = 1024)]
    payload_size: usize,

    /// Number of partitions (default: 4)
    #[arg(short = 'p', long, default_value_t = 4)]
    partitions: u32,

    /// Optional run ID (defaults to new UUID)
    #[arg(short = 'r', long)]
    run_id: Option<String>,

    /// Output file path or '-' for stdout (default: -)
    #[arg(short = 'o', long, default_value = "-")]
    output: String,
}

fn main() {
    let args = Args::parse();
    let run_id = match args.run_id {
        Some(ref s) if s.trim().is_empty() => {
            eprintln!("Error: --run-id cannot be empty");
            process::exit(1);
        }
        Some(s) => s,
        None => format!("run-{}", &Uuid::new_v4().simple().to_string()[..12]),
    };

    let mut writer: Box<dyn Write> = if args.output == "-" {
        Box::new(BufWriter::with_capacity(64 * 1024, io::stdout().lock()))
    } else {
        match File::create(&args.output) {
            Ok(file) => Box::new(BufWriter::with_capacity(64 * 1024, file)),
            Err(err) => {
                eprintln!("Error opening output file '{}': {}", args.output, err);
                process::exit(1);
            }
        }
    };

    let stream = generate_workload_stream(&run_id, args.count, args.payload_size, args.partitions);

    for record in stream {
        let serialized = match serde_json::to_string(&record) {
            Ok(json_str) => json_str,
            Err(err) => {
                eprintln!("Serialization error: {}", err);
                process::exit(1);
            }
        };

        if let Err(err) = writeln!(writer, "{}", serialized) {
            if err.kind() == io::ErrorKind::BrokenPipe {
                process::exit(0);
            }
            eprintln!("Write error: {}", err);
            process::exit(1);
        }
    }

    if let Err(err) = writer.flush() {
        if err.kind() != io::ErrorKind::BrokenPipe {
            eprintln!("Flush error: {}", err);
            process::exit(1);
        }
    }
}
