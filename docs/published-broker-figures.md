# Published Broker Figures: Iggy, Kafka, NATS JetStream, Fluvio

This page collects published figures for **Ferrous Streaming** (Apache Iggy broker + Arroyo stream engine + Delta Lake) with **Apache Kafka**, **NATS JetStream** and **Fluvio**. Apache Iggy and Arroyo are a broker plus a stream engine, while the others are brokers only, so the comparison has two layers.

## Layer 1: Broker Architecture (broker only)

| Dimension | Apache Iggy | Apache Kafka | NATS JetStream | Fluvio |
| :--- | :--- | :--- | :--- | :--- |
| **Language / Runtime** | Rust (no GC) | Java / Scala (JVM, GC) | Go (GC) | Rust (no GC) |
| **I/O Model** | Thread-per-core, shared-nothing, `io_uring` | Multi-threaded, OS page cache, `sendfile` | Go runtime, file or memory storage | Async Rust (SPU) |
| **Deployment Footprint** | Single native binary | JVM + KRaft controllers | Single native binary | Cluster of SC + SPU processes |
| **Tail-Latency Sources** | No GC pauses | JVM GC, producer batching (`linger.ms`) | Go GC | No GC pauses |
| **Inline Processing** | None (use a stream engine) | Kafka Streams / external engines | Consumers only | SmartModules (WASM) |
| **`stream-replay-rs` Sink** | `--sink kafka` (Kafka-compatible gateway) | `--sink kafka` | `--sink nats` | `--sink fluvio` |

## Layer 2: End-to-End Ingest to Lakehouse

| Pipeline | Broker | Processing | Storage |
| :--- | :--- | :--- | :--- |
| **Ferrous Streaming** | Apache Iggy | Arroyo (SQL, 2PC checkpoints) | Delta Lake |
| **Kafka stack** | Apache Kafka | Flink / Spark Structured Streaming / Kafka Connect | Parquet / Delta / Iceberg |
| **NATS stack** | NATS JetStream | Custom consumers | Custom Parquet writer |
| **Fluvio stack** | Fluvio | SmartModules + sink connectors | Custom Delta / Parquet writer |

## Published Benchmark Figures

Figures below are **as published by each project or vendor** (retrieved 2026-10-31). They come from different hardware, payload sizes, replication and durability settings, so they are **not a head-to-head comparison**. Read each row with its conditions, and follow the source before quoting a number.

| System | Published figure | Published by / conditions | Confidence |
| :--- | :--- | :--- | :--- |
| **Apache Iggy** | Peak ~**2.0 GB/s** (~3.36M msgs/s), p99 ~**0.495 ms** at peak | Apache Iggy project benchmarks (`iggy-bench`); AWS `i4i` instances, Intel Xeon, 16 consumers, 1000 B messages, 250 msgs/batch. Source: [benchmarks.iggy.apache.org](https://benchmarks.iggy.apache.org) | Primary, project-published; tuned for peak throughput |
| **Apache Kafka** | Peak **605 MB/s**; p99 end-to-end ~**5 ms** at 200 MB/s | Confluent, Aug 2020, Kafka vs Pulsar vs RabbitMQ on a standard cloud testbed. Source: [Confluent benchmark blog](https://www.confluent.io/blog/kafka-fastest-messaging-system/) | Primary, vendor-published; 2020 results |
| **NATS JetStream** | ~**50K to 400K+** msgs/s with file storage; ~**1 to 5 ms** with persistence | No single authoritative benchmark found; ranges come from secondary write-ups and depend on payload, disk, sync interval and replicas. Reference: [NATS docs](https://docs.nats.io) and `nats bench` | **Low**: secondary sources only |
| **Fluvio** | InfinyOn reports **~20x to 38x lower p99 latency** and **~1.5x higher throughput** than Kafka; **~50 MB** idle RAM vs **~1 GB** for Kafka | InfinyOn (Fluvio's maintainers) comparison with Kafka at default settings (replication factor 1), later discussed critically by the community. Source: [InfinyOn blog](https://www.infinyon.com/blog/) | Vendor claim, reported via secondary sources; defaults and RF=1 |
| **Arroyo** (processing layer) | Arroyo reports large gains over Flink on **sliding windows** (up to ~10x); similar to Flink on simple filters and tumbling windows | Arroyo project engineering blog. Source: [arroyo.dev blog](https://www.arroyo.dev/blog/) | Vendor claim; workload-specific |

> [!WARNING]
> Not directly comparable: Iggy's figure is peak throughput with many consumers, Kafka's is a 2020 mid-size cloud setup, NATS's is a range, and Fluvio's is a relative claim against Kafka. No source above benchmarks Iggy, Kafka, NATS JetStream and Fluvio together on the same hardware. A single independent head-to-head would be needed to rank them, and none of the published figures involve `stream-replay-rs`, and none is a result of the FerrousCX HPC benchmark.
