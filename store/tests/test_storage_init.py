import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from init_tables import init_table, convert_arrow_to_delta_schema, REPO_ROOT

class TestLakehouseStorageInit(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_schema_conversion(self):
        arrow_file = REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"
        schema_string, partition_cols = convert_arrow_to_delta_schema(arrow_file)
        
        schema = json.loads(schema_string)
        self.assertEqual(schema["type"], "struct")
        field_names = [f["name"] for f in schema["fields"]]
        self.assertIn("order_id", field_names)
        self.assertIn("status", field_names)
        self.assertIn("credit_issued", field_names)
        self.assertIn("version", field_names)
        self.assertEqual(partition_cols, ["status"])

    def test_table_initialization_and_protocol(self):
        tables = [
            ("orders_raw_events", REPO_ROOT / "schemas" / "streaming" / "orders_raw_events.arrow.json"),
            ("orders_current_state", REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"),
            ("llm_benchmarks", REPO_ROOT / "schemas" / "benchmarks" / "llm_benchmarks.arrow.json")
        ]

        for name, schema_file in tables:
            # First run: initializes
            created = init_table(self.temp_dir, name, schema_file)
            self.assertTrue(created)

            commit_file = self.temp_dir / name / "_delta_log" / "00000000000000000000.json"
            self.assertTrue(commit_file.exists())

            # Read and validate commit actions
            with open(commit_file, "r", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f if line.strip()]

            actions = {list(k.keys())[0]: list(k.values())[0] for k in lines}
            
            # Verify Delta Protocol (Protocol v1 / reader:1, writer:2)
            self.assertIn("protocol", actions)
            self.assertEqual(actions["protocol"]["minReaderVersion"], 1)
            self.assertEqual(actions["protocol"]["minWriterVersion"], 2)

            # Verify MetaData
            self.assertIn("metaData", actions)
            self.assertEqual(actions["metaData"]["format"]["provider"], "parquet")
            self.assertEqual(actions["metaData"]["name"], name)

            # Verify CommitInfo
            self.assertIn("commitInfo", actions)
            self.assertEqual(actions["commitInfo"]["operation"], "CREATE TABLE")

            # Second run: idempotent skip
            re_created = init_table(self.temp_dir, name, schema_file)
            self.assertFalse(re_created)

if __name__ == "__main__":
    unittest.main()
