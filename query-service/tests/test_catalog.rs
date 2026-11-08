use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{DictionaryArray, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;
use tower::ServiceExt;

use query_service::catalog::CatalogSummary;
use query_service::config::QueryConfig;
use query_service::context::QueryEngine;
use query_service::error::QueryError;
use query_service::server::{create_router, HealthResponse};

fn find_existing_store_dir() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir.join("../store/data/delta"),
        manifest_dir.join("../../ferrous-streaming/store/data/delta"),
        PathBuf::from("./store/data/delta"),
        PathBuf::from("./data/delta"),
    ];
    for c in &candidates {
        if c.exists() && c.join("orders_current_state").exists() {
            return c.canonicalize().unwrap_or_else(|_| c.clone());
        }
    }
    manifest_dir.join("../store/data/delta")
}

fn write_test_parquet(path: &Path, ids: &[i64], vals: &[&str]) -> usize {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    let id_array = Arc::new(Int64Array::from(ids.to_vec()));
    let val_array = Arc::new(StringArray::from(vals.to_vec()));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, val_array]).unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    std::fs::metadata(path).unwrap().len() as usize
}

fn create_delta_table(table_dir: &Path, table_id: &str, part1_name: &str, part1_size: usize) {
    let log_dir = table_dir.join("_delta_log");
    create_dir_all(&log_dir).unwrap();

    let commit_0 = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
        {{\"metaData\":{{\"id\":\"{table_id}\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"val\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],\"configuration\":{{}},\"createdTime\":1700000000000}}}}\n\
        {{\"add\":{{\"path\":\"{part1_name}\",\"partitionValues\":{{}},\"size\":{part1_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n"
    );

    let mut f0 = File::create(log_dir.join("00000000000000000000.json")).unwrap();
    f0.write_all(commit_0.as_bytes()).unwrap();
}

fn append_delta_commit(table_dir: &Path, commit_idx: usize, part_name: &str, part_size: usize) {
    let log_dir = table_dir.join("_delta_log");
    let filename = format!("{:020}.json", commit_idx);
    let commit_content = format!(
        "{{\"add\":{{\"path\":\"{part_name}\",\"partitionValues\":{{}},\"size\":{part_size},\"modificationTime\":1700000001000,\"dataChange\":true}}}}\n"
    );
    let mut f = File::create(log_dir.join(filename)).unwrap();
    f.write_all(commit_content.as_bytes()).unwrap();
}

fn write_test_parquet_with_status(
    path: &Path,
    ids: &[i64],
    vals: &[&str],
    statuses: &[&str],
) -> usize {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let id_array = Arc::new(Int64Array::from(ids.to_vec()));
    let val_array = Arc::new(StringArray::from(vals.to_vec()));
    let status_array = Arc::new(StringArray::from(statuses.to_vec()));

    let batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, val_array, status_array]).unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    std::fs::metadata(path).unwrap().len() as usize
}

#[allow(clippy::too_many_arguments)]
fn create_partitioned_delta_table(
    table_dir: &Path,
    table_id: &str,
    part_a_name: &str,
    part_a_size: usize,
    part_a_status: &str,
    part_b_name: &str,
    part_b_size: usize,
    part_b_status: &str,
) {
    let log_dir = table_dir.join("_delta_log");
    create_dir_all(&log_dir).unwrap();

    let commit_0 = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
        {{\"metaData\":{{\"id\":\"{table_id}\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"val\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"status\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[\"status\"],\"configuration\":{{}},\"createdTime\":1700000000000}}}}\n\
        {{\"add\":{{\"path\":\"{part_a_name}\",\"partitionValues\":{{\"status\":\"{part_a_status}\"}},\"size\":{part_a_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n\
        {{\"add\":{{\"path\":\"{part_b_name}\",\"partitionValues\":{{\"status\":\"{part_b_status}\"}},\"size\":{part_b_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n"
    );

    let mut f0 = File::create(log_dir.join("00000000000000000000.json")).unwrap();
    f0.write_all(commit_0.as_bytes()).unwrap();
}

#[tokio::test]
async fn test_catalog_auto_discovery_and_schema_existing_store() {
    let store_dir = find_existing_store_dir();
    let config = QueryConfig::default().with_data_dir(store_dir.to_str().unwrap());
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");

    let summary = engine.catalog().register_all().await;
    assert!(
        summary.registered_count >= 3,
        "Expected at least 3 tables registered from {:?}, got {}",
        store_dir,
        summary.registered_count
    );

    let names = engine.catalog().table_names();
    assert!(names.contains(&"orders_raw_events".to_string()));
    assert!(names.contains(&"orders_current_state".to_string()));
    assert!(names.contains(&"llm_benchmarks".to_string()));

    // Verify orders_current_state schema mapping
    let state_table = engine
        .catalog()
        .get_table("orders_current_state")
        .expect("orders_current_state exists");
    assert_eq!(state_table.status, "registered");
    let col_names: Vec<&str> = state_table
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(col_names.contains(&"order_id"));
    assert!(col_names.contains(&"status"));
    assert!(col_names.contains(&"credit_issued"));
    assert!(col_names.contains(&"updated_at"));
    assert!(col_names.contains(&"version"));
}

#[tokio::test]
async fn test_direct_table_registration_valid_and_invalid() {
    let store_dir = find_existing_store_dir();
    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");

    // Valid path registration
    let valid_path = store_dir.join("orders_current_state");
    let res = engine
        .register_delta_table("my_current_state", valid_path.to_str().unwrap())
        .await;
    assert!(
        res.is_ok(),
        "Valid table registration should succeed: {:?}",
        res
    );
    assert_eq!(engine.catalog().registered_tables_count(), 1);

    // Invalid path registration
    let bad_res = engine
        .register_delta_table("non_existent", "./this/path/does/not/exist")
        .await;
    assert!(
        bad_res.is_err(),
        "Invalid Delta path registration should fail"
    );
    match bad_res {
        Err(QueryError::Delta(msg))
        | Err(QueryError::Execution(msg))
        | Err(QueryError::Catalog(msg)) => {
            assert!(
                msg.contains("Failed to open Delta table")
                    || msg.contains("Invalid Delta table path")
            );
        }
        other => panic!("Expected Delta or Execution error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_dynamic_acid_commit_pickup_without_server_restart() {
    let tmp = tempdir().unwrap();
    let table_dir = tmp.path().join("dynamic_orders");
    create_dir_all(&table_dir).unwrap();

    // Commit 0: 2 rows
    let part1_path = table_dir.join("part-1.parquet");
    let size1 = write_test_parquet(&part1_path, &[101, 102], &["order_101", "order_102"]);
    create_delta_table(&table_dir, "dyn-table-id", "part-1.parquet", size1);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).unwrap();

    let table_dir_str = table_dir.to_str().unwrap();
    engine
        .register_delta_table("dyn_orders", table_dir_str)
        .await
        .unwrap();

    // Query commit 0
    let batches = engine
        .sql("SELECT COUNT(*) as cnt, SUM(id) as total FROM dyn_orders")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let cnt_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(cnt_col.value(0), 2);
    assert_eq!(total_col.value(0), 203);

    // Append commit 1: 1 more row (total 3 rows) WITHOUT server or engine restart
    let part2_path = table_dir.join("part-2.parquet");
    let size2 = write_test_parquet(&part2_path, &[103], &["order_103"]);
    append_delta_commit(&table_dir, 1, "part-2.parquet", size2);

    // Subsequent query should dynamically pick up commit 1!
    let batches2 = engine
        .sql("SELECT COUNT(*) as cnt, SUM(id) as total FROM dyn_orders")
        .await
        .unwrap();
    assert_eq!(batches2[0].num_rows(), 1);
    let cnt_col2 = batches2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total_col2 = batches2[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        cnt_col2.value(0),
        3,
        "New commit must be dynamically picked up without restart"
    );
    assert_eq!(total_col2.value(0), 306);
}

#[tokio::test]
async fn test_columnar_projection_pushdown() {
    let tmp = tempdir().unwrap();
    let table_dir = tmp.path().join("proj_test");
    create_dir_all(&table_dir).unwrap();

    let part1_path = table_dir.join("part-1.parquet");
    let size1 = write_test_parquet(&part1_path, &[1, 2, 3], &["alice", "bob", "charlie"]);
    create_delta_table(&table_dir, "proj-table-id", "part-1.parquet", size1);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).unwrap();
    engine
        .register_delta_table("proj_test", table_dir.to_str().unwrap())
        .await
        .unwrap();

    // Query projecting only 'val' column
    let batches = engine
        .sql("SELECT val FROM proj_test WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "val");
    assert_eq!(batches[0].num_rows(), 1);

    let val_str = if let Some(view) = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
    {
        view.value(0).to_string()
    } else if let Some(arr) = batches[0].column(0).as_any().downcast_ref::<StringArray>() {
        arr.value(0).to_string()
    } else {
        panic!(
            "Unexpected column type: {:?}",
            batches[0].column(0).data_type()
        );
    };
    assert_eq!(val_str, "bob");

    // Explain query to verify projection pushdown in physical plan
    let explain = engine
        .sql("EXPLAIN SELECT val FROM proj_test")
        .await
        .unwrap();
    let plan_col = explain[0].column(1);
    let plan_str = if let Some(view) = plan_col.as_any().downcast_ref::<StringViewArray>() {
        view.value(1).to_string()
    } else if let Some(arr) = plan_col.as_any().downcast_ref::<StringArray>() {
        arr.value(1).to_string()
    } else {
        format!("{:?}", explain[0])
    };
    assert!(plan_str.contains("val"));
    assert!(!plan_str.contains("projection=[id, val]"));
}

#[tokio::test]
async fn test_partition_pruning_and_filter_pushdown() {
    let tmp = tempdir().unwrap();
    let table_dir = tmp.path().join("part_test");
    create_dir_all(&table_dir).unwrap();

    let part_a_path = table_dir.join("part-delivered.parquet");
    let size_a = write_test_parquet_with_status(
        &part_a_path,
        &[10, 11],
        &["pkg_1", "pkg_2"],
        &["delivered", "delivered"],
    );

    let part_b_path = table_dir.join("part-cancelled.parquet");
    let size_b = write_test_parquet_with_status(
        &part_b_path,
        &[20, 21, 22],
        &["pkg_3", "pkg_4", "pkg_5"],
        &["cancelled", "cancelled", "cancelled"],
    );

    create_partitioned_delta_table(
        &table_dir,
        "part-test-id",
        "part-delivered.parquet",
        size_a,
        "delivered",
        "part-cancelled.parquet",
        size_b,
        "cancelled",
    );

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).unwrap();
    engine
        .register_delta_table("part_test", table_dir.to_str().unwrap())
        .await
        .unwrap();

    // Query with filter on partition column
    let batches = engine
        .sql("SELECT id, val, status FROM part_test WHERE status = 'delivered'")
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    let status_col = batches[0].column(2);
    let s0 = if let Some(dict) = status_col
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
    {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dict values");
        let key = dict.keys().value(0) as usize;
        values.value(key).to_string()
    } else if let Some(view) = status_col.as_any().downcast_ref::<StringViewArray>() {
        view.value(0).to_string()
    } else if let Some(arr) = status_col.as_any().downcast_ref::<StringArray>() {
        arr.value(0).to_string()
    } else {
        panic!(
            "unexpected status column type: {:?}",
            status_col.data_type()
        );
    };
    assert_eq!(s0, "delivered");

    // Explain query to verify filter pushdown / pruning
    let explain = engine
        .sql("EXPLAIN SELECT id, val, status FROM part_test WHERE status = 'delivered'")
        .await
        .unwrap();
    assert!(!explain.is_empty());
    let plan_str = format!("{:?}", explain[0]);
    assert!(plan_str.contains("delivered"));
}

#[tokio::test]
async fn test_missing_and_uninitialized_tables_resilience_nfr5() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path();

    // 1. Create an uninitialized directory (no _delta_log)
    let uninit_dir = data_dir.join("orders_raw_events");
    create_dir_all(&uninit_dir).unwrap();

    // 2. orders_current_state is missing completely

    // 3. Create a valid Delta table discovered as olist_sellers
    let olist_dir = data_dir.join("olist_sellers");
    create_dir_all(&olist_dir).unwrap();
    let part_path = olist_dir.join("part-1.parquet");
    let size = write_test_parquet(&part_path, &[1], &["seller_1"]);
    create_delta_table(&olist_dir, "olist-sellers-id", "part-1.parquet", size);

    let config = QueryConfig::default()
        .with_data_dir(data_dir.to_str().unwrap())
        .with_tables(vec![
            "orders_raw_events".into(),
            "orders_current_state".into(),
        ]);

    let engine = QueryEngine::try_new(config).unwrap();
    let summary = engine.catalog().register_all().await;

    // Must not panic!
    assert_eq!(
        summary.registered_count, 1,
        "Only olist_sellers should be registered"
    );
    assert!(
        summary.missing_count >= 2,
        "Missing/uninitialized tables recorded"
    );

    let raw_events = engine.catalog().get_table("orders_raw_events").unwrap();
    assert_eq!(raw_events.status, "uninitialized");
    assert!(raw_events.error.unwrap().contains("Missing _delta_log"));

    let current_state = engine.catalog().get_table("orders_current_state").unwrap();
    assert_eq!(current_state.status, "missing");
    assert!(current_state.error.unwrap().contains("does not exist"));

    let sellers = engine.catalog().get_table("olist_sellers").unwrap();
    assert_eq!(sellers.status, "registered");
}

#[tokio::test]
async fn test_rest_catalog_endpoint_and_health_integration() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path();

    let table_dir = data_dir.join("orders_current_state");
    create_dir_all(&table_dir).unwrap();
    let part_path = table_dir.join("part-1.parquet");
    let size = write_test_parquet(&part_path, &[42], &["delivered"]);
    create_delta_table(&table_dir, "rest-test-id", "part-1.parquet", size);

    let config = QueryConfig::default()
        .with_data_dir(data_dir.to_str().unwrap())
        .with_tables(vec!["orders_current_state".into(), "missing_table".into()]);

    let engine = QueryEngine::try_new(config).unwrap();
    engine.catalog().register_all().await;

    let router = create_router(engine);

    // 1. Healthcheck includes registered_tables_count
    let health_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(health_resp.status(), StatusCode::OK);
    let bytes = health_resp.into_body().collect().await.unwrap().to_bytes();
    let health: HealthResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(health.status, "healthy");
    assert_eq!(health.registered_tables_count, 1);

    // 2. GET /api/catalog
    let cat_resp = router
        .oneshot(
            Request::builder()
                .uri("/api/catalog")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(cat_resp.status(), StatusCode::OK);
    let cat_bytes = cat_resp.into_body().collect().await.unwrap().to_bytes();
    let cat_summary: CatalogSummary = serde_json::from_slice(&cat_bytes).unwrap();
    assert_eq!(cat_summary.registered_count, 1);
    assert_eq!(cat_summary.missing_count, 1);

    let reg_table = cat_summary
        .tables
        .iter()
        .find(|t| t.name == "orders_current_state")
        .unwrap();
    assert_eq!(reg_table.status, "registered");
    assert_eq!(reg_table.columns.len(), 2);
    assert_eq!(reg_table.columns[0].name, "id");
    assert_eq!(reg_table.columns[1].name, "val");

    let miss_table = cat_summary
        .tables
        .iter()
        .find(|t| t.name == "missing_table")
        .unwrap();
    assert_eq!(miss_table.status, "missing");
}
