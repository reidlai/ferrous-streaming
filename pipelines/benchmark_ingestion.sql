-- Arroyo Streaming SQL Pipeline: benchmark_ingestion
-- Story: 3.11 (Public HPC Benchmark in ferrous-streaming, FR-32, FR-33, OQ-9)
-- Consumes synthetic benchmark stream from benchmark.hpc_mqbench via bridge:9092
-- and group cg-arroyo-benchmark, committing Parquet micro-batches to Delta Lake table hpc_benchmarks.

CREATE TABLE IF NOT EXISTS benchmark_stream_source (
    run_id TEXT,
    record_id TEXT,
    seq BIGINT,
    payload_size_bytes INT,
    partition INT,
    client_timestamp_us BIGINT,
    data TEXT
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'bridge:9092',
    topic = 'benchmark.hpc_mqbench',
    type = 'source',
    format = 'json',
    'source.offset' = 'earliest',
    'source.group_id' = 'cg-arroyo-benchmark',
    client_configs = 'auto.offset.reset=error,isolation.level=read_uncommitted'
);

CREATE TABLE IF NOT EXISTS benchmark_sink (
    run_id TEXT,
    record_id TEXT,
    seq BIGINT,
    payload_size_bytes INT,
    partition INT,
    status TEXT,
    client_timestamp_us BIGINT,
    timestamp_utc TIMESTAMP
) WITH (
    connector = 'filesystem',
    path = '/data/delta/hpc_benchmarks',
    type = 'sink',
    format = 'parquet'
);

INSERT INTO benchmark_sink
SELECT
    run_id,
    record_id,
    seq,
    payload_size_bytes,
    partition,
    'QUALIFIED' as status,
    client_timestamp_us,
    now() as timestamp_utc
FROM
    benchmark_stream_source;

