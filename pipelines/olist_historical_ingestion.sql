-- Arroyo Streaming SQL Pipeline: olist_historical_ingestion
-- Story: 3.10 (Olist Historical Ingestion Job: olist/orders_stream to olist_*)
-- Consumes historical replay from olist/orders_stream via bridge:9092 (kafka_name: olist.orders_stream)
-- and group cg-arroyo-olist, committing Parquet micro-batches to Delta Lake tables.

CREATE TABLE IF NOT EXISTS olist_orders_stream_source (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'bridge:9092',
    topic = 'olist.orders_stream',
    type = 'source',
    format = 'json',
    'source.offset' = 'earliest',
    'source.group_id' = 'cg-arroyo-olist',
    client_configs = 'auto.offset.reset=error,isolation.level=read_uncommitted'
);

CREATE TABLE IF NOT EXISTS olist_orders_sink (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
) WITH (
    connector = 'filesystem',
    path = '/data/delta/olist_orders',
    type = 'sink',
    format = 'parquet'
);

INSERT INTO olist_orders_sink
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM olist_orders_stream_source;
