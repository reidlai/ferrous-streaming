import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from init_tables import init_table, convert_arrow_to_delta_schema, main as init_main, REPO_ROOT

class TestLakehouseStorageInit(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_schema_conversion_current_state(self):
        arrow_file = REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"
        schema_string, partition_cols, meta = convert_arrow_to_delta_schema(arrow_file)
        
        schema = json.loads(schema_string)
        self.assertEqual(schema["type"], "struct")
        field_map = {f["name"]: f["type"] for f in schema["fields"]}
        self.assertEqual(field_map["order_id"], "string")
        self.assertEqual(field_map["status"], "string")
        self.assertEqual(field_map["credit_issued"], "long")
        self.assertEqual(field_map["updated_at"], "timestamp")
        self.assertEqual(field_map["version"], "long")
        self.assertEqual(partition_cols, ["status"])

    def test_schema_conversion_benchmarks_and_raw_events(self):
        # Benchmarks schema conversion with floatingpoint -> double mapping
        benchmarks_file = REPO_ROOT / "schemas" / "benchmarks" / "llm_benchmarks.arrow.json"
        bench_schema_str, bench_parts, _ = convert_arrow_to_delta_schema(benchmarks_file)
        bench_schema = json.loads(bench_schema_str)
        bench_types = {f["name"]: f["type"] for f in bench_schema["fields"]}
        
        self.assertEqual(bench_types["run_id"], "string")
        self.assertEqual(bench_types["ttft_us"], "long")
        self.assertEqual(bench_types["tps"], "double")
        self.assertEqual(bench_types["itl_jitter_us"], "double")
        self.assertEqual(bench_types["tool_call_valid"], "boolean")
        self.assertEqual(bench_types["timestamp_utc"], "timestamp")
        self.assertEqual(bench_parts, ["model_id"])

        # Raw events schema conversion
        raw_file = REPO_ROOT / "schemas" / "streaming" / "orders_raw_events.arrow.json"
        raw_schema_str, raw_parts, _ = convert_arrow_to_delta_schema(raw_file)
        raw_schema = json.loads(raw_schema_str)
        raw_types = {f["name"]: f["type"] for f in raw_schema["fields"]}
        self.assertEqual(raw_types["payload_id"], "string")
        self.assertEqual(raw_types["event_timestamp"], "timestamp")
        self.assertEqual(raw_parts, ["event_timestamp"])

    def test_table_initialization_and_protocol(self):
        tables = [
            ("orders_raw_events", REPO_ROOT / "schemas" / "streaming" / "orders_raw_events.arrow.json", "true"),
            ("orders_current_state", REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json", "false"),
            ("llm_benchmarks", REPO_ROOT / "schemas" / "benchmarks" / "llm_benchmarks.arrow.json", "true")
        ]

        for name, schema_file, expected_append_only in tables:
            # First run: initializes
            created = init_table(self.temp_dir, name, schema_file)
            self.assertTrue(created)

            commit_file = self.temp_dir / name / "_delta_log" / "00000000000000000000.json"
            self.assertTrue(commit_file.exists())

            # Read and validate commit actions
            with open(commit_file, "r", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f if line.strip()]

            self.assertEqual(len(lines), 3)
            actions = {list(k.keys())[0]: list(k.values())[0] for k in lines}
            
            # Verify Delta Protocol (Protocol v1 / reader:1, writer:2)
            self.assertIn("protocol", actions)
            self.assertEqual(actions["protocol"]["minReaderVersion"], 1)
            self.assertEqual(actions["protocol"]["minWriterVersion"], 2)

            # Verify MetaData
            self.assertIn("metaData", actions)
            self.assertEqual(actions["metaData"]["format"]["provider"], "parquet")
            self.assertEqual(actions["metaData"]["name"], name)
            # Valid UUID
            table_uuid = uuid.UUID(actions["metaData"]["id"])
            self.assertIsNotNone(table_uuid)
            # Valid schemaString JSON
            meta_schema = json.loads(actions["metaData"]["schemaString"])
            self.assertEqual(meta_schema["type"], "struct")
            # Config
            self.assertEqual(actions["metaData"]["configuration"]["delta.appendOnly"], expected_append_only)

            # Verify CommitInfo
            self.assertIn("commitInfo", actions)
            self.assertEqual(actions["commitInfo"]["operation"], "CREATE TABLE")
            self.assertEqual(actions["commitInfo"]["readVersion"], 0)

            # Idempotency: re-running returns False and preserves exact contents
            original_content = commit_file.read_text(encoding="utf-8")
            re_created = init_table(self.temp_dir, name, schema_file)
            self.assertFalse(re_created)
            self.assertEqual(commit_file.read_text(encoding="utf-8"), original_content)

    def test_corrupted_log_recovery(self):
        """Initializer recovers and recreates log if commit file is empty or corrupted."""
        schema_file = REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"
        table_dir = self.temp_dir / "test_corrupt"
        log_dir = table_dir / "_delta_log"
        log_dir.mkdir(parents=True)
        commit_file = log_dir / "00000000000000000000.json"
        
        # Write corrupted 0-byte file
        commit_file.write_text("", encoding="utf-8")
        recovered = init_table(self.temp_dir, "test_corrupt", schema_file)
        self.assertTrue(recovered)
        self.assertTrue(commit_file.stat().st_size > 0)


if __name__ == "__main__":
    unittest.main()
