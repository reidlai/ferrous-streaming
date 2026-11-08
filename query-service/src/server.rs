use crate::catalog::CatalogSummary;
use crate::context::QueryEngine;
use crate::error::QueryError;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
    pub batch_size: usize,
    pub engine: String,
    pub registered_tables_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryResponse {
    pub row_count: usize,
    pub message: String,
}

pub fn create_router(engine: QueryEngine) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/query", post(query_handler))
        .route("/api/v1/query", post(query_handler))
        .route("/api/catalog", get(catalog_handler))
        .route("/api/v1/catalog/tables", get(catalog_handler))
        .with_state(Arc::new(engine))
}

async fn health_handler(State(engine): State<Arc<QueryEngine>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        batch_size: engine.batch_size(),
        engine: "DataFusion".to_string(),
        registered_tables_count: engine.catalog().registered_tables_count(),
    })
}

async fn catalog_handler(State(engine): State<Arc<QueryEngine>>) -> Json<CatalogSummary> {
    Json(engine.catalog().summary())
}

async fn query_handler(
    State(engine): State<Arc<QueryEngine>>,
    Json(payload): Json<QueryRequest>,
) -> std::result::Result<Json<QueryResponse>, (StatusCode, String)> {
    match engine.sql(&payload.sql).await {
        Ok(batches) => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            Ok(Json(QueryResponse {
                row_count,
                message: format!("Executed successfully, returned {row_count} rows"),
            }))
        }
        Err(QueryError::DataFusion(e)) => Err((
            StatusCode::BAD_REQUEST,
            format!("SQL syntax/execution error: {e}"),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Query engine error: {e}"),
        )),
    }
}
