use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use datafusion::arrow::array::Int64Array;
use http_body_util::BodyExt;
use query_service::{
    config::{QueryConfig, DEFAULT_BATCH_SIZE},
    context::QueryEngine,
    error::QueryError,
    server::{create_router, HealthResponse, QueryRequest, QueryResponse},
};
use std::sync::Arc;
use tower::ServiceExt; // for oneshot

#[tokio::test]
async fn test_default_config_and_batch_size() {
    let config = QueryConfig::default();
    assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(config.batch_size, 8192);

    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");
    assert_eq!(engine.batch_size(), 8192);
}

#[tokio::test]
async fn test_batch_size_zero_validation() {
    let config = QueryConfig::default().with_batch_size(0);
    let result = QueryEngine::try_new(config);
    assert!(result.is_err());
    match result {
        Err(QueryError::Config(msg)) => assert!(msg.contains("batch_size must be positive")),
        other => panic!("Expected QueryError::Config, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_multithreaded_concurrent_sql_queries() {
    let config = QueryConfig::default();
    let engine = Arc::new(QueryEngine::try_new(config).expect("QueryEngine should initialize"));

    let mut handles = Vec::new();
    for i in 0..10 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let sql = format!("SELECT {} as id, {} * 2 as doubled", i, i);
            let batches = engine_clone
                .sql(&sql)
                .await
                .expect("query execution should succeed");
            assert!(!batches.is_empty());
            assert_eq!(batches[0].num_rows(), 1);

            let id_col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column 0 should be Int64Array");
            assert_eq!(id_col.value(0), i as i64);

            let doubled_col = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column 1 should be Int64Array");
            assert_eq!(doubled_col.value(0), (i * 2) as i64);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .expect("task should complete without panicking");
    }
}

#[tokio::test]
async fn test_invalid_sql_syntax_error() {
    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");

    let result = engine.sql("SELECT FROM WHERE INVALID").await;
    assert!(result.is_err());
    match result {
        Err(QueryError::DataFusion(_)) => (),
        other => panic!("Expected DataFusion error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_healthcheck_endpoint() {
    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");
    let router = create_router(engine);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let health: HealthResponse =
        serde_json::from_slice(&body_bytes).expect("Valid HealthResponse JSON");
    assert_eq!(health.status, "healthy");
    assert_eq!(health.batch_size, 8192);
    assert_eq!(health.engine, "DataFusion");
    assert_eq!(health.registered_tables_count, 0);
}

#[tokio::test]
async fn test_query_endpoint_success_and_error() {
    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");
    let router = create_router(engine);

    // 1. Success case: valid SQL
    let req_payload = QueryRequest {
        sql: "SELECT 42 as answer, 'ferrous' as name".to_string(),
    };
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&req_payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let query_res: QueryResponse =
        serde_json::from_slice(&body_bytes).expect("Valid QueryResponse JSON");
    assert_eq!(query_res.row_count, 1);
    assert!(query_res.message.contains("returned 1 rows"));

    // 2. Error case: invalid SQL syntax
    let bad_payload = QueryRequest {
        sql: "SELECT NOT A VALID SQL".to_string(),
    };
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&bad_payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
