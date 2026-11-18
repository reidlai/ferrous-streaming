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
# 1. Dry-run inspection to stdout (verifies JSONL parsing and timestamp shifting without a broker)
stream-replay-rs --config benchmark/replay-profile.toml --sink stdout

# 2. Live replay into the ferrous-streaming stack via the Iggy-Kafka Bridge (127.0.0.1:9092)
stream-replay-rs --config benchmark/replay-profile.toml

# 3. Live replay with explicit CLI endpoint and rate overrides
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
# 1. Live stack qualification (probes --sink-url, --iggy-url, --arroyo-url, --query-url)
cargo run --bin run-hpc-benchmark -- \
  --input-file-path ./hpc_workload.jsonl \
  --rate 25 \
  --sink iggy \
  --sink-url 127.0.0.1:9092 \
  --iggy-url http://127.0.0.1:3000 \
  --arroyo-url http://127.0.0.1:5115 \
  --query-url http://127.0.0.1:8085

# 2. Offline / CI qualification verification (no running Docker containers required)
cargo run --bin run-hpc-benchmark -- \
  --count 1000 \
  --rate 25 \
  --payload-size 1024 \
  --offline

# 3. JSON qualification report
cargo run --bin run-hpc-benchmark -- --input-file-path ./hpc_workload.jsonl --offline --json
```

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
