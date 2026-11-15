//! Cross-Platform Streaming Pipelines & Schema Migrations Deployer (Step 5)
//! Usage: cargo run --bin deploy-pipelines -- [--dry-run]

use clap::Parser;
use ferrous_streaming::admin::{run_deploy_pipelines, DEFAULT_ARROYO_URL};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "deploy-pipelines",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Deploy streaming SQL pipelines to Arroyo and initialize Delta sinks"
)]
pub struct Args {
    /// Arroyo API URL
    #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_URL)]
    pub arroyo_url: String,

    /// Path to directory containing pipeline SQL files
    #[arg(short, long, default_value = "pipelines")]
    pub pipeline_dir: PathBuf,

    /// Path to base Delta Lake storage directory
    #[arg(long, default_value = "data/delta")]
    pub delta_dir: PathBuf,

    /// Run in dry-run conformance mode without submitting to live Arroyo
    #[arg(short, long)]
    pub dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    run_deploy_pipelines(
        &args.arroyo_url,
        &args.pipeline_dir,
        &args.delta_dir,
        args.dry_run,
    )
    .await
}
