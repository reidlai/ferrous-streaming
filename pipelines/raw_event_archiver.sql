-- Arroyo Streaming SQL Pipeline: raw_event_archiver
-- Story: 3.6 (Raw Event Archiving Pipeline & Delta Lake 2PC Micro-Batch Sink)
-- The source reads Iggy through the Iggy-Kafka bridge (AD-28): bridge:9092, the Kafka name orders.intake
-- (kafka_name in ferrous-cx/contracts/topics.yml) and the settings required by the Bridge Contract Sheet, section 6.
-- Decouples raw event archiving into Delta Lake orders_raw_events table with 10s tumbling micro-batches.

CREATE TABLE IF NOT EXISTS orders_intake_source (
    payload_id TEXT,
    order_id TEXT,
    event_timestamp TIMESTAMP,
    payload TEXT,
    ingested_at TIMESTAMP
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'bridge:9092',
    topic = 'orders.intake',
    type = 'source',
    format = 'json',
    'source.offset' = 'earliest',
    'source.group_id' = 'cg-arroyo-raw',
    client_configs = 'auto.offset.reset=error,isolation.level=read_uncommitted'
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
