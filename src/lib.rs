use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// HPC-MQBench Synthetic Workload Record schema matching `benchmark/workload_generator.py` (Story 13.8, FR-32).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadRecord {
    pub run_id: String,
    pub record_id: String,
    pub seq: u64,
    pub partition: u32,
    pub client_timestamp_us: u64,
    pub payload_size_bytes: usize,
    pub data: String,
}

/// Generate a single workload record conforming to HPC-MQBench workload shape.
pub fn generate_record(
    run_id: &str,
    seq: u64,
    payload_size_bytes: usize,
    partition: u32,
) -> WorkloadRecord {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let record_id = format!("{}:{:08}", run_id, seq);

    let mut record = WorkloadRecord {
        run_id: run_id.to_string(),
        record_id,
        seq,
        partition,
        client_timestamp_us: now_us,
        payload_size_bytes,
        data: String::new(),
    };

    let base_json = serde_json::to_string(&record).unwrap_or_default();
    let target_overhead = base_json.len();
    let padding_needed = payload_size_bytes.saturating_sub(target_overhead);
    record.data = "X".repeat(padding_needed);

    record
}

/// Yield an iterator of benchmark workload records across configured partitions.
pub fn generate_workload_stream(
    run_id: &str,
    total_records: u64,
    payload_size_bytes: usize,
    num_partitions: u32,
) -> impl Iterator<Item = WorkloadRecord> + '_ {
    let partitions = num_partitions.max(1);
    (0..total_records).map(move |seq| {
        let partition = (seq % partitions as u64) as u32;
        generate_record(run_id, seq, payload_size_bytes, partition)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_exact_payload_size() {
        let rec = generate_record("test-run", 0, 1024, 0);
        let serialized = serde_json::to_string(&rec).unwrap();
        assert_eq!(serialized.len(), 1024);
        assert_eq!(rec.record_id, "test-run:00000000");
        assert_eq!(rec.seq, 0);
        assert_eq!(rec.partition, 0);
        assert_eq!(rec.payload_size_bytes, 1024);
    }

    #[test]
    fn test_small_control_profile_size() {
        let rec = generate_record("run-42", 5, 256, 1);
        let serialized = serde_json::to_string(&rec).unwrap();
        assert_eq!(serialized.len(), 256);
        assert_eq!(rec.record_id, "run-42:00000005");
        assert_eq!(rec.partition, 1);
    }

    #[test]
    fn test_large_burst_profile_size() {
        let rec = generate_record("burst-run", 99, 4096, 3);
        let serialized = serde_json::to_string(&rec).unwrap();
        assert_eq!(serialized.len(), 4096);
    }

    #[test]
    fn test_undersized_payload_fallback() {
        let rec = generate_record("tiny", 0, 50, 0);
        let serialized = serde_json::to_string(&rec).unwrap();
        assert!(serialized.len() >= 50);
        assert!(rec.data.is_empty());
    }

    #[test]
    fn test_partition_distribution() {
        let stream: Vec<_> = generate_workload_stream("part-test", 10, 256, 4).collect();
        assert_eq!(stream.len(), 10);
        assert_eq!(stream[0].partition, 0);
        assert_eq!(stream[1].partition, 1);
        assert_eq!(stream[2].partition, 2);
        assert_eq!(stream[3].partition, 3);
        assert_eq!(stream[4].partition, 0);
    }
}
