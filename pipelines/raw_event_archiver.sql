-- Arroyo Streaming SQL Pipeline: raw_event_archiver
-- Story: 3.6 (Raw Event Archiving Pipeline & Delta Lake 2PC Micro-Batch Sink)
-- Decouples raw event archiving into Delta Lake orders_raw_events table with 10s tumbling micro-batches.

CREATE TABLE IF NOT EXISTS orders_intake_source (
    payload_id STRING,
    order_id STRING,
    event_timestamp TIMESTAMP,
    payload STRING,
    ingested_at TIMESTAMP,
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'bootstrap_servers' = 'iggy-broker:9092',
    'topic' = 'orders/intake',
    'group.id' = 'cg-arroyo-raw',
    'type' = 'source',
    'format' = 'json',
    'auto_offset_reset' = 'earliest'
);

CREATE TABLE IF NOT EXISTS orders_raw_events_sink (
    payload_id STRING,
    order_id STRING,
    event_timestamp TIMESTAMP,
    payload STRING,
    ingested_at TIMESTAMP
) WITH (
    'connector' = 'delta',
    'path' = '/data/delta/orders_raw_events',
    'format' = 'parquet',
    'rollover_seconds' = '10',
    'filename.strategy' = 'uuid'
);

INSERT INTO orders_raw_events_sink
SELECT
    payload_id,
    order_id,
    event_timestamp,
    payload,
    COALESCE(ingested_at, CURRENT_TIMESTAMP) AS ingested_at
FROM orders_intake_source;

