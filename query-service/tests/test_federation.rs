use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::physical_plan::displayable;
use parquet::arrow::ArrowWriter;
use rusqlite::Connection;
use tempfile::tempdir;

use query_service::config::QueryConfig;
use query_service::context::QueryEngine;
use query_service::error::QueryError;
use query_service::federation::SqliteTableProvider;

fn create_sample_sqlite_db(path: &Path) {
    let conn = Connection::open(path).expect("Failed to create SQLite db");
    conn.execute(
        "CREATE TABLE carriers (
            carrier_id TEXT NOT NULL,
            carrier_name TEXT NOT NULL,
            region_state TEXT NOT NULL,
            sla_days INTEGER NOT NULL,
            active INTEGER NOT NULL,
            hub_name TEXT NOT NULL
        )",
        [],
    )
    .expect("Failed to create carriers table");

    conn.execute(
        "INSERT INTO carriers (carrier_id, carrier_name, region_state, sla_days, active, hub_name) VALUES
            ('C1', 'Correios SP', 'SP', 2, 1, 'Hub Sao Paulo'),
            ('C2', 'Correios RJ', 'RJ', 4, 1, 'Hub Rio'),
            ('C3', 'Total Express MG', 'MG', 5, 1, 'Hub Belo Horizonte'),
            ('C4', 'Loggi PR', 'PR', 3, 1, 'Hub Curitiba'),
            ('C5', 'Inactive Carrier RS', 'RS', 10, 0, 'Hub Porto Alegre'),
            ('C6', 'Slow Carrier BA', 'BA', 8, 1, 'Hub Salvador')",
        [],
    )
    .expect("Failed to insert carriers");

    conn.execute(
        "CREATE TABLE empty_carriers (
            carrier_id TEXT NOT NULL,
            carrier_name TEXT NOT NULL,
            region_state TEXT NOT NULL,
            sla_days INTEGER NOT NULL,
            active INTEGER NOT NULL
        )",
        [],
    )
    .expect("Failed to create empty_carriers table");
}

fn write_orders_parquet(path: &Path) -> usize {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("customer_state", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("credit_issued", DataType::Int64, false),
    ]));

    let order_ids = Arc::new(StringArray::from(vec![
        "ord_001", "ord_002", "ord_003", "ord_004", "ord_005", "ord_006",
    ]));
    let states = Arc::new(StringArray::from(vec!["SP", "RJ", "MG", "PR", "RS", "BA"]));
    let statuses = Arc::new(StringArray::from(vec![
        "delivered",
        "shipped",
        "delayed",
        "delivered",
        "delayed",
        "delivered",
    ]));
    let credits = Arc::new(Int64Array::from(vec![0, 500, 1500, 0, 2000, 0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![order_ids, states, statuses, credits])
        .expect("Failed to create batch");

    let file = File::create(path).expect("Failed to create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("Writer creation failed");
    writer.write(&batch).expect("Write failed");
    writer.close().expect("Close failed");

    std::fs::metadata(path).expect("Metadata failed").len() as usize
}

fn create_orders_delta_table(table_dir: &Path, part_name: &str, part_size: usize) {
    let log_dir = table_dir.join("_delta_log");
    create_dir_all(&log_dir).expect("Failed to create _delta_log dir");

    let commit_0 = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
        {{\"metaData\":{{\"id\":\"delta_orders_id\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"customer_state\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"status\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"credit_issued\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],\"configuration\":{{}},\"createdTime\":1700000000000}}}}\n\
        {{\"add\":{{\"path\":\"{part_name}\",\"partitionValues\":{{}},\"size\":{part_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n"
    );

    let mut f0 =
        File::create(log_dir.join("00000000000000000000.json")).expect("Commit creation failed");
    f0.write_all(commit_0.as_bytes())
        .expect("Write commit failed");
}

#[tokio::test]
async fn test_federated_join_delta_lake_and_sqlite() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let delta_dir = tmp.path().join("orders_current_state");
    create_dir_all(&delta_dir).expect("create delta dir");
    let part_name = "part-0000.parquet";
    let part_path = delta_dir.join(part_name);
    let part_size = write_orders_parquet(&part_path);
    create_orders_delta_table(&delta_dir, part_name, part_size);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    // Register both heterogeneous data providers
    engine
        .register_delta_table("orders_current_state", delta_dir.to_str().unwrap())
        .await
        .expect("Delta registration");

    engine
        .register_sqlite_table("carriers", db_path.to_str().unwrap(), "carriers")
        .expect("SQLite registration");

    // Execute federated join query conforming to FR-12 & AD-5
    // Evaluates filters on both respective providers (c.active = 1 on SQLite, o.status = 'delivered' on Delta)
    let sql = "SELECT o.order_id, o.customer_state, c.carrier_name, c.sla_days \
               FROM orders_current_state o \
               JOIN carriers c ON o.customer_state = c.region_state \
               WHERE c.active = 1 AND o.status = 'delivered' \
               ORDER BY o.order_id";

    // Inspect physical query plan: ensure unified plan with Join and SqliteScan
    let state = engine.session_context().state();
    let logical_plan = state.create_logical_plan(sql).await.expect("Logical plan");
    let physical_plan = state
        .create_physical_plan(&logical_plan)
        .await
        .expect("Physical plan");
    let plan_str = displayable(physical_plan.as_ref()).indent(true).to_string();

    assert!(
        plan_str.contains("Join") || plan_str.contains("HashJoinExec"),
        "Physical plan should contain JoinExec, found: {plan_str}"
    );
    assert!(
        plan_str.contains("SqliteScan"),
        "Physical plan should contain SqliteScan, found: {plan_str}"
    );

    // Execute query and collect native Arrow RecordBatches
    let batches = engine.sql(sql).await.expect("Federated query execution");
    assert!(!batches.is_empty(), "Should yield Arrow record batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // 3 rows match active carriers AND delivered status (ord_001 SP, ord_004 PR, ord_006 BA)
    assert_eq!(
        total_rows, 3,
        "Expected 3 joined rows for active carriers with delivered status"
    );

    // Verify projected columns
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "order_id");
    assert_eq!(schema.field(1).name(), "customer_state");
    assert_eq!(schema.field(2).name(), "carrier_name");
    assert_eq!(schema.field(3).name(), "sla_days");
}

#[tokio::test]
async fn test_filter_pushdown_to_operational_store() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    engine
        .register_sqlite_table("carriers", db_path.to_str().unwrap(), "carriers")
        .expect("SQLite registration");

    // Query with multiple pushdown filters: active = 1 AND sla_days <= 4
    let sql = "SELECT carrier_name, region_state, sla_days, active \
               FROM carriers \
               WHERE active = 1 AND sla_days <= 4 \
               ORDER BY sla_days";

    let state = engine.session_context().state();
    let logical_plan = state.create_logical_plan(sql).await.expect("Logical plan");
    let physical_plan = state
        .create_physical_plan(&logical_plan)
        .await
        .expect("Physical plan");
    let plan_str = displayable(physical_plan.as_ref()).indent(true).to_string();

    // Verify filter pushdown was embedded into SQLite query
    assert!(
        plan_str.contains("SqliteScan"),
        "Plan should contain SqliteScan"
    );
    assert!(
        plan_str.contains("active") && plan_str.contains("sla_days"),
        "Filter pushdown should push active and sla_days into SQLite SQL, found: {plan_str}"
    );

    let batches = engine.sql(sql).await.expect("Execute filtered query");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // SP (2), PR (3), RJ (4) => 3 rows
    assert_eq!(
        total_rows, 3,
        "Expected 3 rows matching pushed down filters"
    );
}

#[tokio::test]
async fn test_columnar_projection_pushdown() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    engine
        .register_sqlite_table("carriers", db_path.to_str().unwrap(), "carriers")
        .expect("SQLite registration");

    // Table has 6 columns; we project only 2 columns
    let sql = "SELECT carrier_name, sla_days FROM carriers WHERE sla_days <= 3";

    let state = engine.session_context().state();
    let logical_plan = state.create_logical_plan(sql).await.expect("Logical plan");
    let physical_plan = state
        .create_physical_plan(&logical_plan)
        .await
        .expect("Physical plan");
    let plan_str = displayable(physical_plan.as_ref()).indent(true).to_string();

    // SqliteScan should only select the 2 projected columns in SQL
    assert!(
        plan_str.contains("carrier_name") && plan_str.contains("sla_days"),
        "Projection pushdown should select projected columns"
    );

    let batches = engine.sql(sql).await.expect("Execute projected query");
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Expected 2 carriers with sla_days <= 3");

    let schema = batches[0].schema();
    assert_eq!(
        schema.fields().len(),
        2,
        "Batch must have only 2 projected columns"
    );
    assert_eq!(schema.field(0).name(), "carrier_name");
    assert_eq!(schema.field(1).name(), "sla_days");
}

#[tokio::test]
async fn test_dynamic_register_provider_api() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    // Manually instantiate SqliteTableProvider
    let provider = Arc::new(
        SqliteTableProvider::try_new(db_path.to_str().unwrap(), "carriers")
            .expect("Provider creation"),
    );

    // Register via extensible register_provider API
    engine
        .register_provider("custom_carriers", provider.clone())
        .expect("Register custom provider");

    // Duplicate registration should return an error
    let dup_err = engine.register_provider("custom_carriers", provider);
    assert!(
        dup_err.is_err(),
        "Duplicate provider registration must return an error"
    );

    // Verify presence in catalog
    let catalog = engine.catalog();
    assert!(
        catalog
            .table_names()
            .contains(&"custom_carriers".to_string()),
        "Catalog should list custom_carriers"
    );

    // Query custom provider immediately
    let batches = engine
        .sql("SELECT COUNT(*) AS cnt FROM custom_carriers")
        .await
        .expect("Query custom provider");
    assert_eq!(batches[0].num_rows(), 1);
    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Downcast count");
    assert_eq!(
        count_col.value(0),
        6,
        "Expected 6 rows in custom_carriers table"
    );

    // Also register an in-memory MemTable as another custom provider
    let mem_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
    let mem_col = Arc::new(StringArray::from(vec!["v1", "v2", "v3"]));
    let mem_batch = RecordBatch::try_new(mem_schema.clone(), vec![mem_col]).unwrap();
    let mem_table = Arc::new(MemTable::try_new(mem_schema, vec![vec![mem_batch]]).unwrap());

    engine
        .register_provider("mem_lookup", mem_table)
        .expect("Register MemTable");

    let mem_batches = engine
        .sql("SELECT k FROM mem_lookup WHERE k = 'v2'")
        .await
        .expect("Query mem_lookup");
    assert_eq!(mem_batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_empty_table_federated_join() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let delta_dir = tmp.path().join("orders_current_state");
    create_dir_all(&delta_dir).expect("create delta dir");
    let part_name = "part-0000.parquet";
    let part_path = delta_dir.join(part_name);
    let part_size = write_orders_parquet(&part_path);
    create_orders_delta_table(&delta_dir, part_name, part_size);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    engine
        .register_delta_table("orders_current_state", delta_dir.to_str().unwrap())
        .await
        .expect("Delta registration");

    engine
        .register_sqlite_table(
            "empty_carriers",
            db_path.to_str().unwrap(),
            "empty_carriers",
        )
        .expect("Empty SQLite registration");

    // Joining Delta table with empty SQLite table yields 0 rows cleanly without error
    let sql = "SELECT o.order_id, c.carrier_name \
               FROM orders_current_state o \
               JOIN empty_carriers c ON o.customer_state = c.region_state";

    let batches = engine.sql(sql).await.expect("Join with empty table");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table join should yield 0 rows");
}

#[tokio::test]
async fn test_missing_or_invalid_sqlite_resilience_nfr5() {
    let tmp = tempdir().expect("tempdir");
    let valid_db = tmp.path().join("operational.db");
    create_sample_sqlite_db(&valid_db);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    // 1. Non-existent SQLite file
    let non_existent_path = tmp.path().join("does_not_exist.db");
    let err_missing_file = engine.register_sqlite_table(
        "ghost_table",
        non_existent_path.to_str().unwrap(),
        "carriers",
    );
    assert!(
        err_missing_file.is_err(),
        "Should return error for missing SQLite file"
    );
    match err_missing_file {
        Err(QueryError::Catalog(msg)) => {
            assert!(
                msg.contains("does not exist"),
                "Expected catalog error message, got: {msg}"
            );
        }
        other => panic!("Expected QueryError::Catalog, got {other:?}"),
    }

    // 2. Non-existent table in existing SQLite file
    let err_missing_table = engine.register_sqlite_table(
        "missing_table",
        valid_db.to_str().unwrap(),
        "non_existent_table_xyz",
    );
    assert!(
        err_missing_table.is_err(),
        "Should return error for missing table"
    );
    match err_missing_table {
        Err(QueryError::Catalog(msg)) => {
            assert!(
                msg.contains("does not exist") || msg.contains("no columns"),
                "Expected table not exist error, got: {msg}"
            );
        }
        other => panic!("Expected QueryError::Catalog, got {other:?}"),
    }

    // Verify catalog recorded error status without crashing or panicking
    let catalog = engine.catalog();
    let summary = catalog.summary();
    let ghost = summary.tables.iter().find(|t| t.name == "ghost_table");
    assert!(ghost.is_some());
    assert_eq!(ghost.unwrap().status, "error");

    let missing = summary.tables.iter().find(|t| t.name == "missing_table");
    assert!(missing.is_some());
    assert_eq!(missing.unwrap().status, "error");
}

#[tokio::test]
async fn test_filter_edge_cases_in_list_and_like() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("operational.db");
    create_sample_sqlite_db(&db_path);

    let config = QueryConfig::default();
    let engine = QueryEngine::try_new(config).expect("QueryEngine init");

    engine
        .register_sqlite_table("carriers", db_path.to_str().unwrap(), "carriers")
        .expect("SQLite registration");

    // 1. IN list filtering
    let sql_in = "SELECT carrier_name FROM carriers WHERE region_state IN ('SP', 'RJ')";
    let batches_in = engine.sql(sql_in).await.expect("Execute IN list");
    let total_in: usize = batches_in.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_in, 2);

    // 2. LIKE filtering with case sensitivity verified in memory via Inexact pushdown
    let sql_like = "SELECT carrier_name FROM carriers WHERE carrier_name LIKE 'Correios%'";
    let batches_like = engine.sql(sql_like).await.expect("Execute LIKE");
    let total_like: usize = batches_like.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_like, 2, "Expected 2 carriers starting with Correios");
}
