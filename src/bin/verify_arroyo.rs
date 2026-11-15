//! Cross-Platform Arroyo Stream Processor & Broker Ingestion Verifier (Step 4)
//! Usage: cargo run --bin verify-arroyo -- [--dry-run]

use clap::Parser;
use ferrous_streaming::admin::{run_verify_arroyo, DEFAULT_ARROYO_URL, DEFAULT_IGGY_URL};

#[derive(Parser, Debug)]
#[command(
    name = "verify-arroyo",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Verify Arroyo Stream Processor health and broker ingestion attachment"
)]
pub struct Args {
    /// Arroyo API URL
    #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_URL)]
    pub arroyo_url: String,

    /// Apache Iggy HTTP URL
    #[arg(long, env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
    pub iggy_url: String,

    /// Iggy management username
    #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
    pub username: String,

    /// Iggy management password
    #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
    pub password: String,

    /// Run in dry-run conformance mode without network probes
    #[arg(short, long)]
    pub dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    run_verify_arroyo(
        &args.arroyo_url,
        &args.iggy_url,
        &args.username,
        &args.password,
        args.dry_run,
    )
    .await
}
