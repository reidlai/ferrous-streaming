//! Cross-Platform Ingestion Topic & Partition Durability Provisioner (Step 2)
//! Usage: cargo run --bin provision-topics -- [--dry-run]

use clap::Parser;
use ferrous_streaming::admin::{run_provision_topics, DEFAULT_IGGY_URL};

#[derive(Parser, Debug)]
#[command(
    name = "provision-topics",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Provision stream 'orders' and partitioned topics 'intake' and 'consolidated'"
)]
pub struct Args {
    /// Apache Iggy HTTP URL
    #[arg(short, long, env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
    pub url: String,

    /// Iggy management username
    #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
    pub username: String,

    /// Iggy management password
    #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
    pub password: String,

    /// Run in dry-run conformance mode without network calls
    #[arg(short, long)]
    pub dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    run_provision_topics(&args.url, &args.username, &args.password, args.dry_run).await
}
