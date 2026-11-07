use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use query_service::{config::QueryConfig, server::create_router, context::QueryEngine, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| query_service::error::QueryError::Config(e.to_string()))?;

    let config = QueryConfig::default();
    info!(
        batch_size = config.batch_size,
        http_port = config.http_port,
        flight_port = config.flight_port,
        "Initializing ferrous-streaming query-service"
    );

    let engine = QueryEngine::try_new(config.clone())?;
    info!("Apache DataFusion SessionContext successfully initialized with batch size {}", engine.batch_size());

    let app = create_router(engine);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.http_port));
    info!("Starting HTTP REST query service on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| query_service::error::QueryError::Server(e.to_string()))?;

    info!("Query service shut down gracefully");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C signal handler");
}

