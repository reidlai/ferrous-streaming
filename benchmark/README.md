# HPC-MQBench Benchmark (`benchmark/`)

[← Back to ferrous-streaming](../README.md)

This directory owns the workload parameters (`workload.toml`), the `stream-replay-rs` replay profile (`replay-profile.toml`), the synthetic workload generator (`workload-generator`), and the qualification-first benchmark runner (`run-hpc-benchmark` / `run_hpc_benchmark.py`) for the **HPC-MQBench** platform ingestion benchmark (Story 3.11, Story 13.8, Story 15.1, Story 15.2, FR-32, FR-33, OQ-9, SM-8).

The benchmark measures end-to-end platform ingestion latency across all five ingestion stages under **qualification-first** reporting:
$$N_{\text{sent}} = N_{\text{bridge}} = N_{\text{iggy}} = N_{\text{arroyo}} = N_{\text{delta}}$$

---

## 🌐 Server Endpoint Discovery & Configuration

How do `stream-replay-rs` and `run-hpc-benchmark` know which servers to connect to?

| Ingestion Stage | CLI Flag | Environment Variable | Default Endpoint | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Stage 1: Replayer Sink** | `--sink` | — | `iggy` | `stream-replay-rs` sink type (`iggy`, `kafka`, `nats`, `fluvio`, `iceoryx2`, `stdout`) |
| **Stage 2: Iggy-Kafka Bridge** | `--sink-url` | `BRIDGE_URL` | `127.0.0.1:9092` | Host-published `bridge-host` Kafka wire endpoint for `stream-replay-rs` |
| **Stage 3: Apache Iggy Broker** | `--iggy-url` | `IGGY_HTTP_URL` | `http://127.0.0.1:3000` | Apache Iggy HTTP API for `/ping` liveness and topic/offset verification |
| **Stage 4: Arroyo Processor** | `--arroyo-url` | `ARROYO_API_URL` | `http://127.0.0.1:5115` | Arroyo Control Plane API for `benchmark_ingestion` pipeline health |
| **Stage 5: Delta Lake / Query** | `--query-url` | `QUERY_SERVICE_URL` | `http://127.0.0.1:8085` | DataFusion Query Service HTTP API over `/data/delta/hpc_benchmarks` |
| **Target Topic** | `--topic` | `BENCHMARK_TOPIC` | `benchmark.hpc_mqbench` | Target topic mapped to Iggy stream `benchmark`, topic `hpc_mqbench` (4 partitions) |

---

## 🚀 Step 1: Generate Synthetic HPC Workload (`workload-generator`)

Synthetic datasets are generated on-the-fly without bundling proprietary or static data files.

```bash
# Generate 1,000 records of 1,024 bytes each across 4 partitions into ./hpc_workload.jsonl
cargo run --bin workload-generator -- \
  --count 1000 \
  --payload-size 1024 \
  --partitions 4 \
  --output ./hpc_workload.jsonl

# Generate 5,000 records of 256 bytes each (Small Control profile)
cargo run --bin workload-generator -- --count 5000 --payload-size 256 --partitions 4 --output ./hpc_workload.jsonl

# Generate with a custom run ID
cargo run --bin workload-generator -- --run-id custom-bench-1 --count 2500 -o ./hpc_workload.jsonl
```

### Generator Options

| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--count` | `-c` | `1000` | Number of records to generate |
| `--payload-size` | `-s` | `1024` | Target payload size in bytes (exact byte-padded) |
| `--partitions` | `-p` | `4` | Number of topic partitions for round-robin distribution |
| `--run-id` | `-r` | `run-<uuid>` | Run identifier prefix used in `run_id` and `record_id` |
| `--output` | `-o` | `-` | Output file path or `-` for standard output |

### Workload Record Schema

Each record is serialized as a single-line JSON object (NDJSON):

```json
{
  "run_id": "run-42a1b2c3d4e5",
  "record_id": "run-42a1b2c3d4e5:00000000",
  "seq": 0,
  "partition": 0,
  "client_timestamp_us": 1731640000000000,
  "payload_size_bytes": 1024,
  "data": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX..."
}
```

The `data` field is dynamically padded with `'X'` characters so that the entire serialized JSON string matches `--payload-size` byte-for-byte.

---

## 🔁 Step 2: Replay Dataset with `stream-replay-rs` (`replay-profile.toml`)

`benchmark/replay-profile.toml` is a turnkey `stream-replay-rs` configuration file that maps `client_timestamp_us` (`us`) and `record_id` to `benchmark.hpc_mqbench` on `127.0.0.1:9092`:

```bash
# 1. Live replay into the ferrous-streaming stack via the Iggy-Kafka Bridge (127.0.0.1:9092)
stream-replay-rs --config benchmark/replay-profile.toml

# 2. Live replay with explicit CLI endpoint and rate overrides
stream-replay-rs \
  --config benchmark/replay-profile.toml \
  --input-file-path ./hpc_workload.jsonl \
  --sink iggy \
  --sink-url 127.0.0.1:9092 \
  --topic benchmark.hpc_mqbench \
  --rate 100
```

---

## 📊 Step 3: Run End-to-End Qualification Benchmark (`run-hpc-benchmark`)

```bash
# 1. Provision streams/topics (orders + benchmark/hpc_mqbench), consumer groups, Delta logs, and Arroyo pipelines
cargo run --bin streaming-admin -- all
# -> [SUCCESS] All streaming stack components provisioned & verified!

# 2. Generate synthetic HPC workload dataset
cargo run --bin workload-generator -- --count 1000 --payload-size 1024 --partitions 4 --output ./hpc_workload.jsonl

# 3. Replay dataset into live Iggy-Kafka Bridge (127.0.0.1:9092 -> benchmark.hpc_mqbench)
stream-replay-rs --config benchmark/replay-profile.toml

# 4. Run live HPC qualification benchmark
cargo run --bin run-hpc-benchmark -- --input-file-path ./hpc_workload.jsonl --rate 25
# -> Mode: live | Status: [QUALIFIED]
```

### Sample Qualification Output

```text
========================================================================
 FerrousCX Public Platform Benchmark Report (HPC-MQBench Model)
 Run ID:   run-d90a361eba5b
 Topic:    benchmark.hpc_mqbench
 Mode:     live
 Status:   [QUALIFIED]
========================================================================
Server Endpoints & stream-replay-rs Command:
  - Sink Type:           iggy
  - Bridge / Sink URL:   127.0.0.1:9092
  - Apache Iggy URL:     http://127.0.0.1:3000
  - Arroyo API URL:      http://127.0.0.1:5115
  - Query Service URL:   http://127.0.0.1:8085
  - Replay Command:      stream-replay-rs --config benchmark/replay-profile.toml --input-file-path ./hpc_workload.jsonl --sink iggy --sink-url 127.0.0.1:9092 --topic benchmark.hpc_mqbench --rate 25

Workload Parameters:
  - Configured Rate:     25 msg/sec
  - Payload Size:        1024 bytes
  - Partitions:          4
  - Total Ingest Count:  1000
  - Achieved Rate:       89856.95 msg/sec

Qualification Accounting Across Stages (FR-32):
  1. Ingest Sent:        1000
  2. Bridge Accepted:    1000
  3. Apache Iggy:        1000
  4. Arroyo Processor:   1000
  5. Delta Lake 2PC:     1000 (Distinct: 1000)

Stage Latencies (microseconds) & Bridge Hop Overhead (FR-33, OQ-9):
  - Bridge Hop (OQ-9):   p50=474.5us, p95=587us, p99=597us, max=599us
  - Iggy WAL Commit:     p50=649.5us, p95=785us, p99=797us, max=799us
  - Arroyo Stream:       p50=2595.5us, p95=2958.05us, p99=2991.01us, max=2999us
  - Total End-to-End:    p50=17701us, p95=19580.1us, p99=19858.26us, max=20242us
========================================================================
```

See [`../README.md`](../README.md#understanding-the-benchmark-report) for a detailed section-by-section explanation of this report.

---

## 📋 Benchmark Profiles (`workload.toml`)

Parameters from `benchmark/workload.toml`:

- **Small Control (`profiles.small_control`):** 256 bytes payload, target rates [25, 100, 500, 1000] msg/s, default count 5,000.
- **Standard 1KB (`profiles.standard_1kb`):** 1024 bytes payload, target rates [25, 100, 250, 500] msg/s, default count 2,500.
- **Large Burst (`profiles.large_burst`):** 4096 bytes payload, target rates [25, 50, 100] msg/s, default count 1,000.

---

## 🐍 Legacy Python Scripts (Reference)

The Python workload generator and benchmark runner are retained for parity verification:

```bash
python benchmark/workload_generator.py --count 1000 --payload-size 1024 --partitions 4 > ./hpc_workload.jsonl
python benchmark/run_hpc_benchmark.py --input-file-path ./hpc_workload.jsonl --sink-url 127.0.0.1:9092 --offline
```
