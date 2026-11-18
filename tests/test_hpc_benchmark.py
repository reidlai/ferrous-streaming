"""
test_hpc_benchmark.py: Comprehensive test suite for HPC-MQBench public benchmark (Story 3.11, FR-32, FR-33, OQ-9).
"""

import json
import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "benchmark"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from workload_generator import generate_record, generate_workload_stream
from run_hpc_benchmark import HPCBenchmarkRunner, calculate_percentiles, format_report


class TestWorkloadGenerator(unittest.TestCase):
    def test_record_structure_and_sizing(self):
        target_size = 1024
        rec = generate_record(run_id="test-run-1", seq=42, payload_size_bytes=target_size, partition=2)
        self.assertEqual(rec["run_id"], "test-run-1")
        self.assertEqual(rec["seq"], 42)
        self.assertEqual(rec["record_id"], "test-run-1:00000042")
        self.assertEqual(rec["partition"], 2)
        self.assertGreater(rec["client_timestamp_us"], 0)
        
        encoded_len = len(json.dumps(rec).encode("utf-8"))
        self.assertGreaterEqual(encoded_len, target_size - 10)
        self.assertLessEqual(encoded_len, target_size + 10)

    def test_workload_stream_partitioning(self):
        records = list(generate_workload_stream(run_id="test-run-2", total_records=16, num_partitions=4))
        self.assertEqual(len(records), 16)
        
        # Verify sequence and round-robin partition distribution
        for i, r in enumerate(records):
            self.assertEqual(r["seq"], i)
            self.assertEqual(r["partition"], i % 4)


class TestQualificationFirstReconciliation(unittest.TestCase):
    def test_qualified_run(self):
        runner = HPCBenchmarkRunner(
            run_id="run-qualified",
            total_records=200,
            rate_msg_sec=25,
            payload_size_bytes=1024,
            simulate_loss_rate=0.0,
            simulate_dup_rate=0.0,
        )
        res = runner.run_synthetic_benchmark()
        self.assertEqual(res["status"], "QUALIFIED")
        self.assertIsNone(res["disqualification_reason"])
        self.assertEqual(res["accounting"]["sent_records"], 200)
        self.assertEqual(res["accounting"]["delta_stored"], 200)
        self.assertEqual(res["accounting"]["distinct_delta_records"], 200)
        
        # Check bridge hop latency is accounted for
        bridge_lat = res["latencies_us"]["bridge_hop"]
        self.assertGreater(bridge_lat["p50"], 0)
        self.assertGreater(bridge_lat["p99"], 0)

    def test_disqualified_on_data_loss(self):
        runner = HPCBenchmarkRunner(
            run_id="run-loss",
            total_records=100,
            simulate_loss_rate=0.1,  # 10% loss
        )
        res = runner.run_synthetic_benchmark()
        self.assertEqual(res["status"], "DISQUALIFIED")
        self.assertIn("Data loss detected", res["disqualification_reason"])

    def test_disqualified_on_duplicates(self):
        runner = HPCBenchmarkRunner(
            run_id="run-dup",
            total_records=100,
            simulate_dup_rate=0.1,  # 10% duplicate insertion
        )
        res = runner.run_synthetic_benchmark()
        self.assertEqual(res["status"], "DISQUALIFIED")
        self.assertIn("Duplicates detected", res["disqualification_reason"])


class TestTopicRegistryAlignment(unittest.TestCase):
    def test_benchmark_topic_in_contracts(self):
        import yaml
        topics_yml_path = REPO_ROOT.parent / "ferrous-cx" / "contracts" / "topics.yml"
        if not topics_yml_path.is_file():
            self.skipTest("ferrous-cx/contracts/topics.yml not found")
            
        with open(topics_yml_path, "r", encoding="utf-8") as f:
            registry = yaml.safe_load(f)
            
        topics = registry.get("topics", [])
        bench_topics = [t for t in topics if t.get("kafka_name") == "benchmark.hpc_mqbench"]
        self.assertEqual(len(bench_topics), 1, "benchmark.hpc_mqbench must be registered in topics.yml")
        bt = bench_topics[0]
        self.assertEqual(bt["stream"], "benchmark")
        self.assertEqual(bt["topic"], "hpc_mqbench")
        self.assertEqual(bt["partitions"], 4)
        self.assertTrue(bt.get("kafka", {}).get("produce", False), "kafka.produce must be true")
        self.assertIn("cg-arroyo-benchmark", bt.get("kafka", {}).get("consume", []))


class TestPipelineSQLValidity(unittest.TestCase):
    def test_pipeline_sql_content(self):
        pipeline_file = REPO_ROOT / "pipelines" / "benchmark_ingestion.sql"
        self.assertTrue(pipeline_file.is_file(), "benchmark_ingestion.sql must exist")
        content = pipeline_file.read_text(encoding="utf-8")
        self.assertIn("benchmark.hpc_mqbench", content)
        self.assertIn("cg-arroyo-benchmark", content)
        self.assertIn("/data/delta/hpc_benchmarks", content)


class TestEndpointConfigurationAndReplayProfile(unittest.TestCase):
    def test_runner_reports_endpoints_and_replayer_command(self):
        runner = HPCBenchmarkRunner(
            run_id="run-endpoints",
            total_records=25,
            rate_msg_sec=50,
            sink="iggy",
            sink_url="127.0.0.1:9092",
            iggy_url="http://127.0.0.1:3000",
            arroyo_url="http://127.0.0.1:5115",
            query_url="http://127.0.0.1:8085",
        )
        res = runner.run_synthetic_benchmark()
        self.assertEqual(res["endpoints"]["sink"], "iggy")
        self.assertEqual(res["endpoints"]["sink_url"], "127.0.0.1:9092")
        self.assertEqual(res["endpoints"]["iggy_url"], "http://127.0.0.1:3000")
        self.assertEqual(res["endpoints"]["arroyo_url"], "http://127.0.0.1:5115")
        self.assertEqual(res["endpoints"]["query_url"], "http://127.0.0.1:8085")
        self.assertIn("stream-replay-rs --config benchmark/replay-profile.toml", res["replayer_command"])
        report = format_report(res)
        self.assertIn("127.0.0.1:9092", report)
        self.assertIn("http://127.0.0.1:3000", report)
        self.assertIn("http://127.0.0.1:5115", report)
        self.assertIn("http://127.0.0.1:8085", report)

    def test_replay_profile_toml_exists(self):
        profile = REPO_ROOT / "benchmark" / "replay-profile.toml"
        self.assertTrue(profile.is_file(), "benchmark/replay-profile.toml must exist")
        text = profile.read_text(encoding="utf-8")
        self.assertIn('input_path = "./hpc_workload.jsonl"', text)
        self.assertIn('timestamp_column = "client_timestamp_us"', text)
        self.assertIn('default_url = "127.0.0.1:9092"', text)
        self.assertIn('destination = "benchmark.hpc_mqbench"', text)


if __name__ == "__main__":
    unittest.main()

