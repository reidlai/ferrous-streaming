-- Arroyo Streaming SQL Pipeline: state_snapshot_consolidator
-- Story: 3.7 (Consolidated State Materialization Pipeline - orders_current_state)
-- Consumes state projections from orders/consolidated and commits updated entity snapshots to Delta Lake orders_current_state.

CREATE TABLE IF NOT EXISTS orders_consolidated_source (
    order_id TEXT,
    status TEXT,
    credit_issued BIGINT,
    updated_at TIMESTAMP,
    version BIGINT
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'iggy-broker:9092',
    topic = 'orders/consolidated',
    type = 'source',
    format = 'json'
);

CREATE TABLE IF NOT EXISTS orders_current_state_sink (
    order_id TEXT,
    status TEXT,
    credit_issued BIGINT,
    updated_at TIMESTAMP,
    version BIGINT
) WITH (
    connector = 'filesystem',
    path = '/data/delta/orders_current_state',
    type = 'sink',
    format = 'parquet'
);

INSERT INTO orders_current_state_sink
SELECT
    order_id,
    status,
    COALESCE(credit_issued, 0) AS credit_issued,
    COALESCE(updated_at, now()) AS updated_at,
    COALESCE(version, 1) AS version
FROM orders_consolidated_source;
