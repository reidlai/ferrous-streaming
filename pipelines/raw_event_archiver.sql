-- Arroyo Streaming SQL Pipeline: raw_event_archiver
-- Story: 3.6 (Raw Event Archiving Pipeline & Delta Lake 2PC Micro-Batch Sink)
-- Decouples raw event archiving into Delta Lake orders_raw_events table with 10s tumbling micro-batches.

CREATE TABLE IF NOT EXISTS orders_intake_source (
    payload_id TEXT,
    order_id TEXT,
    event_timestamp TIMESTAMP,
    payload TEXT,
    ingested_at TIMESTAMP
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'iggy-broker:9092',
    topic = 'orders/intake',
    type = 'source',
    format = 'json'
);

CREATE TABLE IF NOT EXISTS orders_raw_events_sink (
    payload_id TEXT,
    order_id TEXT,
    event_timestamp TIMESTAMP,
    payload TEXT,
    ingested_at TIMESTAMP
) WITH (
    connector = 'filesystem',
    path = '/data/delta/orders_raw_events',
    type = 'sink',
    format = 'parquet'
);

INSERT INTO orders_raw_events_sink
SELECT
    payload_id,
    order_id,
    event_timestamp,
    payload,
    COALESCE(ingested_at, now()) AS ingested_at
FROM orders_intake_source;
