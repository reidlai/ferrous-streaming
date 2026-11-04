import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from auth_check import verify_service_account
from init_tables import init_table, REPO_ROOT
from optimize import run_optimize
from vacuum import run_vacuum

class TestLakehouseMaintenance(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.table_name = "orders_current_state"
        self.schema_file = REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"
        init_table(self.temp_dir, self.table_name, self.schema_file)
        self.table_dir = self.temp_dir / self.table_name

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_authentication_validation(self):
        # Missing or bad credentials
        self.assertFalse(verify_service_account("random_user", "bad_token"))
        self.assertFalse(verify_service_account("svc_ferrous_lakehouse", "<REPLACE_WITH_..."))
        self.assertFalse(verify_service_account("svc_ferrous_lakehouse", "short"))

        # Valid service account credentials
        self.assertTrue(verify_service_account("svc_ferrous_lakehouse", "lakehouse_secret_token_12345"))

    def test_optimize_compaction_commit(self):
        # Create dummy uncompacted parquet files
        (self.table_dir / "part-0001.parquet").write_bytes(b"small_parquet_1")
        (self.table_dir / "part-0002.parquet").write_bytes(b"small_parquet_2")

        res = run_optimize(self.table_dir, target_mb=128)
        self.assertEqual(res["status"], "success")
        self.assertEqual(res["version"], 1)

        # Verify new commit exists
        commit_1 = self.table_dir / "_delta_log" / "00000000000000000001.json"
        self.assertTrue(commit_1.exists())

        with open(commit_1, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        actions = [list(k.keys())[0] for k in lines]
        self.assertIn("add", actions)
        self.assertIn("remove", actions)
        self.assertIn("commitInfo", actions)

    def test_vacuum_retention_pruning(self):
        # 1. Add file then remove it via optimize
        dummy_old = self.table_dir / "old-part.parquet"
        dummy_old.write_bytes(b"old_data")

        # Run optimize so old-part is marked as remove
        run_optimize(self.table_dir, target_mb=128)

        # Modify commit timestamps in _delta_log to simulate 8 days ago
        eight_days_ago_ms = int((time.time() - (8 * 24 * 3600)) * 1000)
        commit_1 = self.table_dir / "_delta_log" / "00000000000000000001.json"
        
        with open(commit_1, "r", encoding="utf-8") as f:
            content = f.read()

        # Update deletionTimestamp in commit to 8 days ago
        content_aged = content.replace(str(int(time.time() * 1000))[:6], str(eight_days_ago_ms)[:6])
        with open(commit_1, "w", encoding="utf-8") as f:
            f.write(content_aged)

        # Run vacuum with 168 hours (7 days) retention
        res = run_vacuum(self.table_dir, retention_hours=168)
        self.assertEqual(res["status"], "success")
        self.assertGreaterEqual(res["version"], 2)

if __name__ == "__main__":
    unittest.main()
