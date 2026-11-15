//! Unified Cross-Platform Streaming Administration CLI (Story 13.10)
//! Coordinates Apache Iggy provisioning, consumer groups, Arroyo health checks, and pipeline deployment.

use clap::{Parser, Subcommand};
use ferrous_streaming::admin::{
    run_deploy_pipelines, run_provision_consumer_groups, run_provision_topics, run_verify_arroyo,
    DEFAULT_ARROYO_URL, DEFAULT_IGGY_URL,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "streaming-admin",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Cross-Platform Streaming Administration Suite for Apache Iggy & Arroyo"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Provision stream 'orders' and partitioned topics 'intake' and 'consolidated' (Step 2)
    ProvisionTopics {
        /// Apache Iggy HTTP URL
        #[arg(short, long, visible_alias = "iggy-url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without mutating live broker
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Provision zero-copy consumer groups cg-arroyo-raw, cg-cmd-handler, cg-arroyo-state (Step 3)
    ProvisionConsumerGroups {
        /// Apache Iggy HTTP URL
        #[arg(short, long, visible_alias = "iggy-url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without mutating live broker
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Verify Arroyo stream processor health and broker ingestion attachment (Step 4)
    VerifyArroyo {
        /// Arroyo API URL
        #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_URL)]
        arroyo_url: String,

        /// Apache Iggy HTTP URL
        #[arg(long, visible_alias = "url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        iggy_url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without network probes
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Deploy streaming SQL pipelines to Arroyo and initialize Delta sinks (Step 5)
    DeployPipelines {
        /// Arroyo API URL
        #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_URL)]
        arroyo_url: String,

        /// Path to directory containing pipeline SQL files
        #[arg(short, long, default_value = "pipelines")]
        pipeline_dir: PathBuf,

        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Run in dry-run conformance mode without submitting to live Arroyo
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Execute the full streaming setup sequence in order (Steps 2 -> 3 -> 4 -> 5)
    All {
        /// Apache Iggy HTTP URL
        #[arg(long, visible_alias = "url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        iggy_url: String,

        /// Arroyo API URL
        #[arg(long, env = "ARROYO_API_URL", default_value = DEFAULT_ARROYO_URL)]
        arroyo_url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Path to directory containing pipeline SQL files
        #[arg(short, long, default_value = "pipelines")]
        pipeline_dir: PathBuf,

        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Run in dry-run conformance mode
        #[arg(short, long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Command::ProvisionTopics {
            url,
            username,
            password,
            dry_run,
        } => {
            run_provision_topics(&url, &username, &password, dry_run).await?;
        }
        Command::ProvisionConsumerGroups {
            url,
            username,
            password,
            dry_run,
        } => {
            run_provision_consumer_groups(&url, &username, &password, dry_run).await?;
        }
        Command::VerifyArroyo {
            arroyo_url,
            iggy_url,
            username,
            password,
            dry_run,
        } => {
            run_verify_arroyo(&arroyo_url, &iggy_url, &username, &password, dry_run).await?;
        }
        Command::DeployPipelines {
            arroyo_url,
            pipeline_dir,
            delta_dir,
            dry_run,
        } => {
            run_deploy_pipelines(&arroyo_url, &pipeline_dir, &delta_dir, dry_run).await?;
        }
        Command::All {
            iggy_url,
            arroyo_url,
            username,
            password,
            pipeline_dir,
            delta_dir,
            dry_run,
        } => {
            println!("##################################################################");
            println!("  [Full Setup] Executing Steps 2 through 5 in order...            ");
            println!("##################################################################\n");

            // Step 2
            run_provision_topics(&iggy_url, &username, &password, dry_run).await?;
            println!();

            // Step 3
            run_provision_consumer_groups(&iggy_url, &username, &password, dry_run).await?;
            println!();

            // Step 4
            run_verify_arroyo(&arroyo_url, &iggy_url, &username, &password, dry_run).await?;
            println!();

            // Step 5
            run_deploy_pipelines(&arroyo_url, &pipeline_dir, &delta_dir, dry_run).await?;

            println!("\n##################################################################");
            println!("  [SUCCESS] All streaming stack components provisioned & verified! ");
            println!("##################################################################");
        }
    }

    Ok(())
}
