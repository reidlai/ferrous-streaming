-- Delta Lake Schema: orders_raw_events
-- Append-only raw ingestion event store capturing all inbound commands and Iggy payloads.
-- Partition strategy: date(event_timestamp)
-- Zero PII: Tax IDs, customer contact info, and payment card numbers are strictly excluded.

CREATE TABLE IF NOT EXISTS orders_raw_events (
    payload_id STRING NOT NULL,                  -- UUIDv7 unique identifier for the event
    order_id STRING NOT NULL,                    -- Olist order identifier
    event_timestamp TIMESTAMP NOT NULL,          -- Microsecond precision source event timestamp
    payload STRING NOT NULL,                     -- Raw JSON string payload (PII redacted)
    ingested_at TIMESTAMP NOT NULL               -- Lakehouse ingestion arrival timestamp (micros)
)
USING DELTA
PARTITIONED BY (event_timestamp)
LOCATION 'data/delta/orders_raw_events'
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2',
    'delta.appendOnly' = 'true'
);
