-- Delta Lake Schema: hpc_benchmarks
-- High-Performance Public Platform Benchmark Telemetry (FR-32, FR-33, OQ-9).
-- Qualification-first accounting across Bridge, Iggy, Arroyo, and Delta Lake stages.

CREATE TABLE IF NOT EXISTS hpc_benchmarks (
    run_id STRING NOT NULL,                      -- UUIDv7 / benchmark execution run identifier
    record_id STRING NOT NULL,                   -- Distinct record identity (FR-32 deduplication key)
    seq BIGINT NOT NULL,                         -- Replay sequence number
    payload_size_bytes INT NOT NULL,             -- Workload payload size in bytes (e.g. 256, 1024)
    partition INT NOT NULL,                      -- Stream topic partition index (0..3)
    status STRING NOT NULL,                      -- Qualification state: QUALIFIED, OVERDRIVEN, INVALID
    client_timestamp_us BIGINT NOT NULL,         -- Ingestion client emit timestamp in microseconds
    bridge_hop_latency_us BIGINT NOT NULL,       -- Extra hop latency introduced by Iggy-Kafka bridge (FR-33, OQ-9)
    iggy_latency_us BIGINT NOT NULL,             -- Broker commit / fsync latency in microseconds
    arroyo_latency_us BIGINT NOT NULL,           -- Arroyo stream processing latency in microseconds
    delta_latency_us BIGINT NOT NULL,            -- Delta Lake 2PC commit latency in microseconds
    e2e_latency_us BIGINT NOT NULL,              -- End-to-end ingest-to-store latency in microseconds
    timestamp_utc TIMESTAMP NOT NULL             -- Microsecond precision UTC benchmark timestamp
)
USING DELTA
PARTITIONED BY (status)
LOCATION 'data/delta/hpc_benchmarks'
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
);

