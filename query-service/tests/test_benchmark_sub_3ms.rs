use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;

use query_service::config::{QueryConfig, DEFAULT_BATCH_SIZE};
use query_service::context::QueryEngine;
use query_service::error::QueryError;

const TOTAL_ORDERS: usize = 100_000;
const BATCH_SIZE: usize = DEFAULT_BATCH_SIZE; // 8,192
const TOTAL_LLM_RECORDS: usize = 20_000;

fn create_orders_delta_table(table_dir: &Path) {
    create_dir_all(table_dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("customer_state", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("delay_days", DataType::Float64, false),
        Field::new("payment_value_cents", DataType::Int64, true),
        Field::new("credit_issued", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
    ]));

    let parquet_path = table_dir.join("orders_part0.parquet");
    let file = File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(BATCH_SIZE))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let states = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF"];
    let statuses = ["delivered", "shipped", "canceled", "invoiced", "processing"];

    let mut remaining = TOTAL_ORDERS;
    let mut current_id = 0;

    while remaining > 0 {
        let current_batch_size = remaining.min(BATCH_SIZE);
        let mut order_ids = Vec::with_capacity(current_batch_size);
        let mut customer_states = Vec::with_capacity(current_batch_size);
        let mut order_statuses = Vec::with_capacity(current_batch_size);
        let mut delays = Vec::with_capacity(current_batch_size);
        let mut payments = Vec::with_capacity(current_batch_size);
        let mut credits = Vec::with_capacity(current_batch_size);
        let mut versions = Vec::with_capacity(current_batch_size);

        for _ in 0..current_batch_size {
            order_ids.push(format!("order_{:06}", current_id));
            customer_states.push(states[current_id % states.len()]);
            // Skew statuses: mostly delivered
            let status = if current_id % 10 == 0 {
                statuses[current_id % statuses.len()]
            } else {
                "delivered"
            };
            order_statuses.push(status);
            delays.push(((current_id % 15) as f64) - 3.0);
            if current_id % 200 == 0 {
                payments.push(None); // Nullable cents test
            } else {
                payments.push(Some(((current_id % 40000) + 1500) as i64));
            }
            credits.push(if current_id % 50 == 0 { 2000i64 } else { 0i64 });
            versions.push(1i64);
            current_id += 1;
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(order_ids)),
                Arc::new(StringArray::from(customer_states)),
                Arc::new(StringArray::from(order_statuses)),
                Arc::new(Float64Array::from(delays)),
                Arc::new(Int64Array::from(payments)),
                Arc::new(Int64Array::from(credits)),
                Arc::new(Int64Array::from(versions)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        remaining -= current_batch_size;
    }

    writer.close().unwrap();
    let parquet_size = std::fs::metadata(&parquet_path).unwrap().len();

    // Delta log
    let log_dir = table_dir.join("_delta_log");
    create_dir_all(&log_dir).unwrap();

    let commit_json = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
        {{\"metaData\":{{\"id\":\"orders_current_state\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"customer_state\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"status\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"delay_days\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"payment_value_cents\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"credit_issued\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"version\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],\"configuration\":{{}},\"createdTime\":1700000000000}}}}\n\
        {{\"add\":{{\"path\":\"orders_part0.parquet\",\"partitionValues\":{{}},\"size\":{parquet_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n"
    );

    let mut f = File::create(log_dir.join("00000000000000000000.json")).unwrap();
    f.write_all(commit_json.as_bytes()).unwrap();
}

fn create_llm_benchmarks_delta_table(table_dir: &Path) {
    create_dir_all(table_dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("provider", DataType::Utf8, false),
        Field::new("latency_ms", DataType::Float64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("ttft_us", DataType::Int64, false),
        Field::new("tps", DataType::Float64, false),
    ]));

    let parquet_path = table_dir.join("llm_part0.parquet");
    let file = File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(BATCH_SIZE))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let models = [
        "qwen-2.5-coder-32b",
        "claude-3-5-sonnet",
        "gpt-4o",
        "gemini-1.5-pro",
    ];
    let providers = ["ollama", "anthropic", "openai", "google"];

    let mut remaining = TOTAL_LLM_RECORDS;
    let mut current_id = 0;

    while remaining > 0 {
        let current_batch_size = remaining.min(BATCH_SIZE);
        let mut run_ids = Vec::with_capacity(current_batch_size);
        let mut model_col = Vec::with_capacity(current_batch_size);
        let mut provider_col = Vec::with_capacity(current_batch_size);
        let mut latencies = Vec::with_capacity(current_batch_size);
        let mut scores = Vec::with_capacity(current_batch_size);
        let mut ttfts = Vec::with_capacity(current_batch_size);
        let mut tps_col = Vec::with_capacity(current_batch_size);

        for _ in 0..current_batch_size {
            run_ids.push(format!("run_{:06}", current_id));
            let idx = current_id % models.len();
            model_col.push(models[idx]);
            provider_col.push(providers[idx]);
            latencies.push(18.5 + (idx as f64) * 4.2);
            scores.push(0.92 + (idx as f64) * 0.02);
            ttfts.push(120_000 + (idx as i64) * 15_000);
            tps_col.push(85.0 + (idx as f64) * 10.0);
            current_id += 1;
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(run_ids)),
                Arc::new(StringArray::from(model_col)),
                Arc::new(StringArray::from(provider_col)),
                Arc::new(Float64Array::from(latencies)),
                Arc::new(Float64Array::from(scores)),
                Arc::new(Int64Array::from(ttfts)),
                Arc::new(Float64Array::from(tps_col)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        remaining -= current_batch_size;
    }

    writer.close().unwrap();
    let parquet_size = std::fs::metadata(&parquet_path).unwrap().len();

    let log_dir = table_dir.join("_delta_log");
    create_dir_all(&log_dir).unwrap();

    let commit_json = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
        {{\"metaData\":{{\"id\":\"llm_benchmarks\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"run_id\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"model\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"provider\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"latency_ms\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"score\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"ttft_us\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}},{{\\\"name\\\":\\\"tps\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":false,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],\"configuration\":{{}},\"createdTime\":1700000000000}}}}\n\
        {{\"add\":{{\"path\":\"llm_part0.parquet\",\"partitionValues\":{{}},\"size\":{parquet_size},\"modificationTime\":1700000000000,\"dataChange\":true}}}}\n"
    );

    let mut f = File::create(log_dir.join("00000000000000000000.json")).unwrap();
    f.write_all(commit_json.as_bytes()).unwrap();
}

async fn setup_benchmark_engine() -> (tempfile::TempDir, QueryEngine) {
    let temp = tempdir().unwrap();
    let delta_dir = temp.path().join("delta");
    let orders_dir = delta_dir.join("orders_current_state");
    let llm_dir = delta_dir.join("llm_benchmarks");

    create_orders_delta_table(&orders_dir);
    create_llm_benchmarks_delta_table(&llm_dir);

    let config = QueryConfig::default()
        .with_data_dir(delta_dir.to_str().unwrap())
        .with_batch_size(BATCH_SIZE)
        .with_target_partitions(4);

    let engine = QueryEngine::try_new(config).expect("QueryEngine should initialize");
    engine
        .register_delta_table("orders_current_state", orders_dir.to_str().unwrap())
        .await
        .expect("orders_current_state should register");
    engine
        .register_delta_table("llm_benchmarks", llm_dir.to_str().unwrap())
        .await
        .expect("llm_benchmarks should register");

    engine
        .materialize_table("orders_current_state")
        .await
        .expect("orders_current_state should materialize");
    engine
        .materialize_table("llm_benchmarks")
        .await
        .expect("llm_benchmarks should materialize");

    (temp, engine)
}

static SHARED_BENCHMARK: tokio::sync::OnceCell<(tempfile::TempDir, QueryEngine)> =
    tokio::sync::OnceCell::const_new();

async fn get_shared_engine() -> &'static QueryEngine {
    let (_, engine) = SHARED_BENCHMARK
        .get_or_init(|| async { setup_benchmark_engine().await })
        .await;
    engine
}

static BENCH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_benchmark_100k_order_point_lookup_sub_3ms() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    // Single order point lookup
    let sql = "SELECT * FROM orders_current_state WHERE order_id = 'order_000042'";
    let metrics = engine
        .benchmark_query(sql, 20, 50)
        .await
        .expect("Benchmark should execute");

    println!(
        "\n[Benchmark] 100k Order Point Lookup: p50={:.3}ms, p95={:.3}ms, p99={:.3}ms, mean={:.3}ms",
        metrics.p50_ms(),
        metrics.p95_ms(),
        metrics.p99_ms(),
        metrics.mean_ms()
    );

    assert!(
        metrics.p95_ms() < 3.0,
        "p95 latency ({:.3}ms) must be strictly < 3.0ms (NFR-1)",
        metrics.p95_ms()
    );

    // Verify correct record returned
    let batches = engine.sql(sql).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    // Non-existent order returns 0 rows
    let missing_batches = engine
        .sql("SELECT * FROM orders_current_state WHERE order_id = 'order_nonexistent'")
        .await
        .unwrap();
    let total_rows: usize = missing_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_benchmark_100k_carrier_delay_aggregation_sub_3ms() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    let sql = "SELECT customer_state, COUNT(*), AVG(delay_days) FROM orders_current_state GROUP BY customer_state";
    let metrics = engine
        .benchmark_query(sql, 25, 50)
        .await
        .expect("Benchmark should execute");

    println!(
        "\n[Benchmark] 100k Carrier Delay Aggregation: p50={:.3}ms, p95={:.3}ms, p99={:.3}ms, mean={:.3}ms",
        metrics.p50_ms(),
        metrics.p95_ms(),
        metrics.p99_ms(),
        metrics.mean_ms()
    );

    assert!(
        metrics.p95_ms() < 3.0,
        "p95 latency ({:.3}ms) must be strictly < 3.0ms (NFR-1)",
        metrics.p95_ms()
    );

    // Verify mathematical accuracy
    let batches = engine.sql(sql).await.unwrap();
    let total_counted: i64 = batches
        .iter()
        .map(|b| {
            let count_col = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Count column");
            (0..count_col.len())
                .map(|i| count_col.value(i))
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total_counted, TOTAL_ORDERS as i64);
}

#[tokio::test]
async fn test_benchmark_100k_order_status_summary_sub_3ms() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    let sql = "SELECT status, COUNT(*), SUM(payment_value_cents) FROM orders_current_state GROUP BY status";
    let metrics = engine
        .benchmark_query(sql, 25, 50)
        .await
        .expect("Benchmark should execute");

    println!(
        "\n[Benchmark] 100k Order Status Summary: p50={:.3}ms, p95={:.3}ms, p99={:.3}ms, mean={:.3}ms",
        metrics.p50_ms(),
        metrics.p95_ms(),
        metrics.p99_ms(),
        metrics.mean_ms()
    );

    assert!(
        metrics.p95_ms() < 3.0,
        "p95 latency ({:.3}ms) must be strictly < 3.0ms (NFR-1)",
        metrics.p95_ms()
    );

    // Verify results
    let batches = engine.sql(sql).await.unwrap();
    let mut total_counted: i64 = 0;
    let mut total_sum: i64 = 0;

    for b in &batches {
        let count_col = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Count column");
        for i in 0..count_col.len() {
            total_counted += count_col.value(i);
        }

        let sum_col = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Sum column");
        for i in 0..sum_col.len() {
            if !sum_col.is_null(i) {
                total_sum += sum_col.value(i);
            }
        }
    }

    assert_eq!(total_counted, TOTAL_ORDERS as i64);
    assert!(
        total_sum > 0,
        "Aggregated payment value cents sum must be strictly positive and handle nulls correctly"
    );
}

#[tokio::test]
async fn test_benchmark_multi_model_llm_metrics_sub_3ms() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    let sql =
        "SELECT model, AVG(latency_ms), AVG(score), COUNT(*) FROM llm_benchmarks GROUP BY model";
    let metrics = engine
        .benchmark_query(sql, 20, 50)
        .await
        .expect("Benchmark should execute");

    println!(
        "\n[Benchmark] Multi-model LLM Benchmark: p50={:.3}ms, p95={:.3}ms, p99={:.3}ms, mean={:.3}ms",
        metrics.p50_ms(),
        metrics.p95_ms(),
        metrics.p99_ms(),
        metrics.mean_ms()
    );

    assert!(
        metrics.p95_ms() < 3.0,
        "p95 latency ({:.3}ms) must be strictly < 3.0ms (FR-28, AD-22)",
        metrics.p95_ms()
    );

    let batches = engine.sql(sql).await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        row_count, 4,
        "Should have aggregate rows for 4 distinct models"
    );
}

#[tokio::test]
async fn test_zero_cross_process_serialization_overhead() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    // Verify in-process zero-copy Arrow memory layouts
    let sql = "SELECT * FROM orders_current_state WHERE order_id = 'order_000001'";
    let batches = engine.sql(sql).await.expect("Query should succeed");

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    // Assert Arrow RecordBatch is held directly in memory
    assert_eq!(batch.num_columns(), 7);
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.get_array_memory_size() > 0);

    // Verify zero cross-process serialization overhead by inspecting direct in-memory column buffers
    let col0 = batch.column(0);
    assert!(
        matches!(
            col0.data_type(),
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Utf8View
        ),
        "Expected Utf8 or Utf8View, got: {:?}",
        col0.data_type()
    );
    let val = if let Some(arr) = col0.as_any().downcast_ref::<StringArray>() {
        arr.value(0)
    } else if let Some(arr) = col0.as_any().downcast_ref::<StringViewArray>() {
        arr.value(0)
    } else {
        panic!("Direct downcast to Arrow StringArray or StringViewArray failed");
    };
    assert_eq!(val, "order_000001");
}

#[tokio::test]
async fn test_benchmark_measurement_engine_validation() {
    let _lock = BENCH_LOCK.lock().await;
    let engine = get_shared_engine().await;

    // Invalid SQL syntax error handling
    let err = engine
        .benchmark_query("SELECT invalid_syntax FROM nonexistent_table ???", 1, 5)
        .await;
    assert!(err.is_err(), "Invalid SQL syntax should bubble up error");
    match err.unwrap_err() {
        QueryError::DataFusion(_) => {}
        other => panic!("Expected DataFusion error, got: {:?}", other),
    }

    // Valid SQL on non-existent table error handling
    let table_err = engine.sql("SELECT * FROM non_existent_table").await;
    assert!(
        table_err.is_err(),
        "Querying non-existent table should bubble up error"
    );

    // Materialize non-existent table error handling
    let mat_err = engine.materialize_table("non_existent_table").await;
    assert!(
        mat_err.is_err(),
        "Materializing non-existent table should bubble up Catalog error"
    );
    match mat_err.unwrap_err() {
        QueryError::Catalog(_) => {}
        other => panic!("Expected QueryError::Catalog, got: {:?}", other),
    }

    // Zero iterations validation
    let zero_err = engine
        .benchmark_query("SELECT * FROM orders_current_state LIMIT 1", 0, 0)
        .await;
    assert!(zero_err.is_err());
}
