# HPC-MQBench Benchmark (`benchmark/`)

[← Back to ferrous-streaming](../../README.md)

This directory owns the workload parameters and synthetic generator for the **HPC-MQBench** platform ingestion benchmark (Story 3.11, Story 13.8, FR-32, FR-33, OQ-9).

The benchmark measures end-to-end platform ingestion latency across all five ingestion stages under **qualification-first** reporting:
$$N_{\text{sent}} = N_{\text{bridge}} = N_{\text{iggy}} = N_{\text{arroyo}} = N_{\text{delta}}$$

---

## 🚀 Synthetic Workload Generator

Synthetic datasets are generated on-the-fly without bundling proprietary or static data files.

### Cross-Platform Cargo Tool (Recommended)

Generate synthetic JSON workload records using the pure-Rust Cargo CLI tool:

```bash
# Generate 1,000 records of 1KB each across 4 partitions to stdout
cargo run --bin workload-generator

# Generate 5,000 records of 256 bytes each to a file
cargo run --bin workload-generator -- --count 5000 --payload-size 256 --partitions 4 --output /tmp/workload.ndjson

# Generate with a custom run ID
cargo run --bin workload-generator -- --run-id custom-bench-1 --count 2500 -o dataset.ndjson
```

### Options

| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--count` | `-c` | `1000` | Number of records to generate |
| `--payload-size` | `-s` | `1024` | Target payload size in bytes (exact byte-padded) |
| `--partitions` | `-p` | `4` | Number of topic partitions for round-robin distribution |
| `--run-id` | `-r` | `run-<uuid>` | Run identifier prefix used in `run_id` and `record_id` |
| `--output` | `-o` | `-` | Output file path or `-` for standard output |

### Workload Record Schema

Each record is serialized as a single-line JSON object (NDJSON) matching `workload_generator.py`:

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

## 📋 Benchmark Profiles (`workload.toml`)

Parameters from `benchmark/workload.toml`:

- **Small Control (`profiles.small_control`):** 256 bytes payload, target rates [25, 100, 500, 1000] msg/s, default count 5,000.
- **Standard 1KB (`profiles.standard_1kb`):** 1024 bytes payload, target rates [25, 100, 250, 500] msg/s, default count 2,500.
- **Large Burst (`profiles.large_burst`):** 4096 bytes payload, target rates [25, 50, 100] msg/s, default count 1,000.

---

## 🐍 Legacy Python Generator (Reference)

The legacy Python script is retained for parity verification:

```bash
python benchmark/workload_generator.py --count 1000 --payload-size 1024 --partitions 4 > /tmp/hpc_workload.jsonl
```
