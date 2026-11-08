use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Benchmark performance metrics with percentile and summary statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkMetrics {
    pub iterations: usize,
    pub total_rows: usize,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub mean_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub p99_duration: Duration,
    pub total_duration: Duration,
    pub latencies: Vec<Duration>,
}

impl BenchmarkMetrics {
    /// Calculate summary and percentile metrics from raw execution latencies.
    pub fn new(latencies: Vec<Duration>, total_rows: usize) -> Self {
        assert!(!latencies.is_empty(), "latencies cannot be empty");
        let iterations = latencies.len();
        let total_duration: Duration = latencies.iter().sum();
        let mean_duration = total_duration.div_f64(iterations as f64);

        let mut sorted = latencies.clone();
        sorted.sort();

        let min_duration = sorted[0];
        let max_duration = sorted[sorted.len() - 1];

        let p50_duration = quantile(&sorted, 0.50);
        let p95_duration = quantile(&sorted, 0.95);
        let p99_duration = quantile(&sorted, 0.99);

        Self {
            iterations,
            total_rows,
            min_duration,
            max_duration,
            mean_duration,
            p50_duration,
            p95_duration,
            p99_duration,
            total_duration,
            latencies,
        }
    }

    /// 50th percentile (median) latency in milliseconds.
    pub fn p50_ms(&self) -> f64 {
        self.p50_duration.as_secs_f64() * 1000.0
    }

    /// 95th percentile latency in milliseconds.
    pub fn p95_ms(&self) -> f64 {
        self.p95_duration.as_secs_f64() * 1000.0
    }

    /// 99th percentile latency in milliseconds.
    pub fn p99_ms(&self) -> f64 {
        self.p99_duration.as_secs_f64() * 1000.0
    }

    /// Mean latency in milliseconds.
    pub fn mean_ms(&self) -> f64 {
        self.mean_duration.as_secs_f64() * 1000.0
    }

    /// Minimum latency in milliseconds.
    pub fn min_ms(&self) -> f64 {
        self.min_duration.as_secs_f64() * 1000.0
    }

    /// Maximum latency in milliseconds.
    pub fn max_ms(&self) -> f64 {
        self.max_duration.as_secs_f64() * 1000.0
    }
}

/// Compute quantile using standard nearest-rank method on sorted slice.
fn quantile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let n = sorted.len();
    let rank = ((n as f64) * p).ceil() as usize;
    let idx = if rank == 0 {
        0
    } else if rank >= n {
        n - 1
    } else {
        rank - 1
    };
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_metrics_quantiles() {
        // Test with unsorted inputs to ensure sorting works
        let mut latencies: Vec<Duration> =
            (1..=100).map(|i| Duration::from_millis(i as u64)).collect();
        latencies.reverse(); // 100 down to 1
        let metrics = BenchmarkMetrics::new(latencies, 1000);

        assert_eq!(metrics.iterations, 100);
        assert_eq!(metrics.total_rows, 1000);
        assert_eq!(metrics.min_duration, Duration::from_millis(1));
        assert_eq!(metrics.max_duration, Duration::from_millis(100));
        assert_eq!(metrics.p50_duration, Duration::from_millis(50));
        assert_eq!(metrics.p95_duration, Duration::from_millis(95));
        assert_eq!(metrics.p99_duration, Duration::from_millis(99));

        assert!((metrics.min_ms() - 1.0).abs() < f64::EPSILON);
        assert!((metrics.max_ms() - 100.0).abs() < f64::EPSILON);
        assert!((metrics.p50_ms() - 50.0).abs() < f64::EPSILON);
        assert!((metrics.p95_ms() - 95.0).abs() < f64::EPSILON);
        assert!((metrics.p99_ms() - 99.0).abs() < f64::EPSILON);
        assert!((metrics.mean_ms() - 50.5).abs() < 1e-6);
    }

    #[test]
    fn test_benchmark_metrics_single_element() {
        let latencies = vec![Duration::from_millis(5)];
        let metrics = BenchmarkMetrics::new(latencies, 42);

        assert_eq!(metrics.iterations, 1);
        assert_eq!(metrics.total_rows, 42);
        assert_eq!(metrics.min_duration, Duration::from_millis(5));
        assert_eq!(metrics.max_duration, Duration::from_millis(5));
        assert_eq!(metrics.p50_duration, Duration::from_millis(5));
        assert_eq!(metrics.p95_duration, Duration::from_millis(5));
        assert_eq!(metrics.p99_duration, Duration::from_millis(5));
        assert_eq!(metrics.mean_duration, Duration::from_millis(5));
    }
}
