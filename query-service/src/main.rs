use query_service::{config::QueryConfig, context::QueryEngine, server::create_router, Result};
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

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
        data_dir = %config.data_dir,
        "Initializing ferrous-streaming query-service"
    );

    let engine = QueryEngine::try_new(config.clone())?;
    info!(
        "Apache DataFusion SessionContext successfully initialized with batch size {}",
        engine.batch_size()
    );

    let summary = engine.catalog().register_all().await;
    info!(
        registered = summary.registered_count,
        missing = summary.missing_count,
        "LakehouseCatalog registered Delta Lake tables"
    );

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
