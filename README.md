# ferrous-streaming

[← Back to FerrousCX Root Architecture](https://github.com/reidlai/ferrous-cx)

[![Component Type: Streaming & Lakehouse Ingestion](https://img.shields.io/badge/component-streaming%20%26%20lakehouse-blue.svg)](https://github.com/reidlai/ferrous-cx)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-green.svg)](LICENSE)
[![DevSecOps CI: Tier 2](https://img.shields.io/badge/DevSecOps%20CI-Tier%202%20Passing-brightgreen.svg)](https://github.com/reidlai/ferrous-cx/blob/main/.github/workflows/devsecops-infra-ci.yml)
[![Container Security: Trivy Passing](https://img.shields.io/badge/Trivy-0%20High%2FCritical-brightgreen.svg)](#container-security--devsecops)
[![Target: Apache Iggy + Arroyo](https://img.shields.io/badge/stack-Iggy%20%2B%20Arroyo%20%2B%20Delta%20Lake-orange.svg)](#system-architecture)

**`ferrous-streaming`** is the **consolidated data platform** of the **FerrousCX** enterprise platform. One repository packages and orchestrates **Apache Iggy** (high-throughput Write-Ahead Log streaming broker), **Arroyo** (Rust-native stateful stream processing engine), the **Delta store** (`store/`) and a standalone **Apache DataFusion** query service, together with the platform's HPC benchmark. They are deployed as one compose deliverable of separate services, not merged into one process. The former `ferrous-lakehouse` and `ferrous-query-engine` repositories are retired into this one.

This submodule packages and runs the streaming engine and broker runtime, orchestrating Arroyo SQL pipelines, schema migration scripts, and transactional two-phase commit (2PC) Parquet sinks to **Delta Lake**, while dataset replay is performed externally by the generic, platform-agnostic **`stream-replay-rs`**. Topic names, replay profiles and the Olist dataset are owned by `ferrous-cx`, which provisions the topics this platform consumes.

---

## 🏛️ System Architecture

```mermaid
flowchart LR
    subgraph Ingestion_Source ["External Traffic Replayer"]
        Replayer["stream-replay-rs<br/>(Olist Dataset Replay)"]
    end

    subgraph Streaming_Submodule ["ferrous-streaming Submodule"]
        Iggy["Apache Iggy Broker<br/>• Port 9092 (Kafka Wire Gateway)<br/>• Port 8090 (Native TCP)"]
        
        subgraph Arroyo_Engine ["Arroyo Processing Engine"]
            Migrations["Schema Migrations<br/>(v1 -> v2 DDL Evolution)"]
            SQLPipeline["Arroyo SQL Streaming Jobs<br/>(Stream-to-Parquet Batcher)"]
        end
    end

    subgraph Storage ["Delta Lake 2PC Parquet Sinks"]
        DeltaRaw[("Delta Lake: orders_raw_events<br/>(Append-only Audit Log)")]
        DeltaState[("Delta Lake: orders_current_state<br/>(Compacted State Projection)")]
    end

    subgraph Storage_Targets ["Storage Targets"]
        Local["Local Filesystem (file://)"]
        MinIO["MinIO S3 (s3://)"]
        AWSS3["AWS S3 (s3://)"]
        GCS["GCP Cloud Storage (gs://)"]
    end

    subgraph Downstream_Consumption ["Downstream Analytical Engine"]
        DeltaSink["DeltaSink<br/>(Parquet Store)"]
        DataFusion["Apache DataFusion<br/>(Vectorized Engine)"]
        ApiService["FerrousCX API Service<br/>(Rust / Axum)"]
    end

    Replayer -->|Kafka: Port 9092| Iggy
    Iggy -->|orders/intake| SQLPipeline
    Migrations --> SQLPipeline
    SQLPipeline -->|2PC Micro-Batches| DeltaRaw
    SQLPipeline -->|2PC State Projections| DeltaState

    DeltaSink -.-> Local
    DeltaSink -.-> MinIO
    DeltaSink -.-> AWSS3
    DeltaSink -.-> GCS

    DeltaSink -->|SQL Scan &lt;3ms| DataFusion
    DataFusion -->|Historical Analytics| ApiService
```

> **Note on Pipeline Boundaries:** The **FerrousCX API Service (Rust / Axum)** connects to **Apache DataFusion** to query this Delta Lake Arroyo storage **strictly for historical streaming datasets** (e.g. Olist historical transactions, reviews, delivery logs). Live transactional commands and state changes bypass this pipeline and are handled strictly by the CQRS-ES pattern in **`ferrous-cqrses`**.

### Communication Protocols & Ports

| Component | Port | Protocol | Purpose |
| :--- | :--- | :--- | :--- |
| **Apache Iggy (Kafka Gateway)** | `9092` | Kafka Wire Protocol (TCP) | Ingestion sink for `stream-replay-rs` and Kafka producers |
| **Apache Iggy (Native TCP)** | `8090` | Iggy Custom Binary Protocol | High-performance, low-latency management and streaming |
| **Apache Iggy (HTTP Management)** | `3000` | HTTP / REST | Health probe (`/ping`), Auth (`/users/login`), and `/metrics` |
| **Arroyo Web Dashboard** | `5115` | HTTP / WebSocket | Stream pipeline monitoring, metrics, and checkpoint inspection |
| **DataFusion Query Service** *(planned)* | `50051` | Arrow Flight SQL (gRPC) | Analytical SQL over the Delta store for consumers other than `ferrous-api` |
| **DataFusion Query Service** *(planned)* | `8085` | HTTP | Query and health endpoints of the same service |

> [!NOTE]
> **Kafka gateway status.** With the Iggy 0.9 image the stack builds, port `9092` is published but nothing listens on it, so a Kafka-protocol producer cannot reach the broker through it today. How Arroyo and the replayer ingest from Iggy is an open architecture question (`OPEN Arroyo-Iggy Path`) that blocks the pipeline stories.

---

## ⚡ Key Capabilities

1. **Dedicated Historical Data Ingestion Broker:**
   * Pre-configured Apache Iggy stream topologies for the topics provisioned by `ferrous-cx` (`olist/orders_stream`, `orders/intake`, `orders/consolidated`), optimized for sub-millisecond fsync ingestion and zero-copy consumer groups.
   * Native Kafka protocol gateway allowing external producer tools (like `stream-replay-rs`) to publish events with zero custom dependencies.
2. **Schema Versioning & SQL Migrations as Code:**
   * Versioned SQL DDL scripts (`migrations/001_initial_olist_schema.sql`, `migrations/002_add_delivery_sla_fields.sql`).
   * Manages stream schema evolution without downtime, schema drift, or corrupted Parquet files.
3. **Turnkey Delta Lake 2PC Parquet Sinks:**
   * Configured Arroyo two-phase commit (2PC) Parquet sink connectors ensuring transactional ACID micro-batch commits (`_delta_log/`).
   * Pluggable storage abstraction supporting local NVMe storage (`file://`), MinIO (`s3://`), AWS S3, and Google Cloud Storage (`gs://`).
4. **Zero-Python, Zero-JVM Footprint:**
   * Compiled native Rust executables throughout; operates with minimal memory overhead (<200MB combined RSS).

---

## Delta Store (`store/`)

The Delta Lake table definitions, retention config, and `init-tables` / `optimize` / `vacuum` scripts live in `store/`, relocated from the former `ferrous-lakehouse` repository with history preserved. Arroyo writes the streaming tables; the `delta-store` compose service initializes and maintains them. See `store/README.md`.

---

## HPC Benchmark (`benchmark/`, planned)

The platform's high-performance benchmark is public and lives here, based on the workload and method of **HPC-MQBench**. It measures the FerrousCX platform end to end, and a run counts only when every record is accounted for. It uses the generic `stream-replay-rs` as its load driver (the `ferrous-cx` load profile, `--rate 25`, or a rate set by the benchmark) and holds no benchmark knowledge in the replayer.

Published third-party broker figures, with their conditions and caveats, are kept in [`docs/published-broker-figures.md`](docs/published-broker-figures.md). They are not results of this benchmark.

---

## 🛠️ Standalone Developer Quickstart

You can build and test `ferrous-streaming` in Standalone Mode using Docker Compose:

### Prerequisites

* **Docker & Docker Compose:** `v2.29+`
* **Pre-commit:** `3.8+` (`pip install pre-commit` or `pipx install pre-commit`)

### 1. Launch the Streaming Stack

```bash
# Clone the repository
git clone https://github.com/reidlai/ferrous-streaming.git
cd ferrous-streaming

# Install local pre-commit hooks
pre-commit install

# Boot Apache Iggy and Arroyo locally
docker compose up -d

# Verify services are healthy
docker compose ps
```

* **Apache Iggy Kafka Gateway:** `localhost:9092`
* **Apache Iggy Native TCP:** `localhost:8090`
* **Apache Iggy HTTP Health Check:** `http://localhost:3000/ping` (`pong`)
* **Apache Iggy Prometheus Metrics:** `http://localhost:3000/metrics` (Requires Bearer token)
* **Arroyo Web UI:** `http://localhost:5115`

### 2. Provision Ingestion Topics & Partition Durability

Idempotently initialize stream `orders` and partitioned topics (`intake` and `consolidated`, 4 partitions each) with immediate `fsync` persistence:

```bash
# POSIX Bash
./scripts/provision-topics.sh

# PowerShell
.\scripts\provision-topics.ps1
```

### 3. Provision Zero-Copy Consumer Groups

Idempotently provision consumer groups for decoupled stream processing and command consumption (`cg-arroyo-raw`, `cg-cmd-handler`, and `cg-arroyo-state`):

```bash
# POSIX Bash
./scripts/provision-consumer-groups.sh

# PowerShell
.\scripts\provision-consumer-groups.ps1
```

### 4. Verify Arroyo Stream Processor & Broker Ingestion

Verify that the Arroyo Stream Processor (port 5115) is healthy and authenticated to Apache Iggy with its own native credentials (these services are not Kanidm clients), attaching the consumer groups `cg-arroyo-olist` (`olist/orders_stream`), `cg-arroyo-raw` (`orders/intake`) and `cg-arroyo-state` (`orders/consolidated`):

```bash
# POSIX Bash
./scripts/provision-arroyo.sh

# PowerShell
.\scripts\provision-arroyo.ps1
```

### 5. Run Schema Migrations & Deploy Streaming Pipelines

Deploy both Arroyo streaming pipelines (Pipeline 1: `raw_event_archiver` connecting to `orders/intake` via `cg-arroyo-raw`; Pipeline 2: `state_snapshot_consolidator` connecting to `orders/consolidated` via `cg-arroyo-state`):

```bash
# POSIX Bash (Deploys both pipelines sequentially)
./scripts/run-migrations.sh

# Or deploy individual pipelines:
./scripts/deploy-pipeline-raw-archiver.sh
./scripts/deploy-pipeline-state-consolidator.sh

# PowerShell (Deploys both pipelines sequentially)
.\scripts\run-migrations.ps1

# Or deploy individual pipelines:
.\scripts\deploy-pipeline-raw-archiver.ps1
.\scripts\deploy-pipeline-state-consolidator.ps1
```

---

## 🛡️ Container Security & DevSecOps (AD-13)

This infrastructure submodule adheres strictly to the **Tier 2 DevSecOps Standards**:

* **Local Pre-Commit Guardrails (`.pre-commit-config.yaml`):**
  - Secret scanning via `gitleaks` to block committed credentials.
  - File formatting and syntax checks (`check-yaml`, `check-toml`, trailing whitespace, end-of-file).
  - Automated local execution of all 9 unit and integration test suites.
* **Local Container Build & Trivy Vulnerability Scanning (`build-and-scan`):**
  - **Base Image SCA:** **Trivy** scanning base images (Alpine/Debian) for OS-level CVEs, failing on unpatched **HIGH** or **CRITICAL** vulnerabilities.
  - Zero cloud CI minute consumption for free GitHub accounts.
  ```bash
  # POSIX Bash
  ./scripts/build-and-scan.sh

  # PowerShell
  .\scripts\build-and-scan.ps1
  ```

---

## ⚙️ Configuration & Environment Variables Reference

| Environment Variable | Default Value | Description | Required |
| :--- | :--- | :--- | :--- |
| `IGGY_KAFKA_PORT` | `9092` | Port for Kafka-compatible wire protocol gateway | No |
| `IGGY_TCP_PORT` | `8090` | Port for native Apache Iggy TCP transport | No |
| `ARROYO_API_PORT` | `5115` | Port for Arroyo Web Dashboard and REST API | No |
| `DELTA_STORAGE_URI` | `file:///data/delta` | Storage destination URI (`file://`, `s3://`, `gs://`) | No |
| `AWS_ENDPOINT_URL` | *None* | Custom S3 endpoint URL (for MinIO) | If using MinIO |
| `AWS_ACCESS_KEY_ID` | *None* | AWS / MinIO Access Key ID | If using S3 |
| `AWS_SECRET_ACCESS_KEY` | *None* | AWS / MinIO Secret Access Key | If using S3 |
| `GOOGLE_APPLICATION_CREDENTIALS` | *None* | Path to GCP service account key | If using GCS |
