import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

# Add scripts directory to path
STORE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = STORE_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(STORE_DIR))

import entrypoint
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

    def _env_without_credentials(self):
        """Environment with every LAKEHOUSE_* variable removed (no token, no account)."""
        return {k: v for k, v in os.environ.items() if not k.startswith("LAKEHOUSE_")}

    def test_scripts_run_without_any_token(self):
        # Maintenance has no application-level auth: it must run with no credentials set.
        (self.table_dir / "part-0001.parquet").write_bytes(b"small_parquet_1")
        env = self._env_without_credentials()
        runs = (
            ("optimize.py", []),
            ("vacuum.py", ["--retention-hours", "168"]),
        )
        for script, extra in runs:
            proc = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / script), "--table", self.table_name,
                 "--data-dir", str(self.temp_dir), *extra],
                env=env, capture_output=True, text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertNotIn("401", proc.stderr)
        log_dir = self.table_dir / "_delta_log"
        self.assertTrue((log_dir / "00000000000000000001.json").exists())
        self.assertTrue((log_dir / "00000000000000000002.json").exists())

    def test_entrypoint_maintain_without_any_token(self):
        (self.table_dir / "part-0001.parquet").write_bytes(b"small_parquet_1")
        env = self._env_without_credentials()
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(entrypoint, "DATA_DIR", self.temp_dir):
                self.assertEqual(entrypoint.do_maintain(), 0)
        # OPTIMIZE then VACUUM each committed once on the initialised table.
        self.assertTrue((self.table_dir / "_delta_log" / "00000000000000000002.json").exists())

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

    def test_optimize_empty_table_noop(self):
        """OPTIMIZE on table with 0 parquet files is a no-op that creates no dummy commits."""
        empty_table = self.temp_dir / "empty_table"
        init_table(self.temp_dir, "empty_table", self.schema_file)
        res = run_optimize(empty_table, target_mb=128)
        self.assertEqual(res["status"], "success")
        self.assertEqual(res["files_removed"], 0)
        self.assertEqual(res["files_added"], 0)
        # No new commit created
        self.assertFalse((empty_table / "_delta_log" / "00000000000000000001.json").exists())

    def test_uninitialized_table_error(self):
        """VACUUM and OPTIMIZE return errors on uninitialized tables."""
        uninit = self.temp_dir / "uninit"
        uninit.mkdir()
        res_v = run_vacuum(uninit, 168)
        self.assertEqual(res_v["status"], "error")
        res_o = run_optimize(uninit, 128)
        self.assertEqual(res_o["status"], "error")

    def test_vacuum_retention_pruning(self):
        # 1. Add files then remove them via optimize
        dummy_old = self.table_dir / "old-part.parquet"
        dummy_old.write_bytes(b"old_data")

        # Run optimize so old-part is marked as remove
        run_optimize(self.table_dir, target_mb=128)

        # Modify commit timestamps in _delta_log to simulate 8 days ago
        eight_days_ago_ms = int((time.time() - (8 * 24 * 3600)) * 1000)
        commit_1 = self.table_dir / "_delta_log" / "00000000000000000001.json"
        
        # Read commit lines, parse JSON, and set deletionTimestamp on remove action
        lines = []
        with open(commit_1, "r", encoding="utf-8") as f:
            for l in f:
                if l.strip():
                    action = json.loads(l)
                    if "remove" in action:
                        action["remove"]["deletionTimestamp"] = eight_days_ago_ms
                    lines.append(json.dumps(action))

        with open(commit_1, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        # Run vacuum with 168 hours (7 days) retention
        res = run_vacuum(self.table_dir, retention_hours=168)
        self.assertEqual(res["status"], "success")
        self.assertGreaterEqual(res["version"], 2)

        # Verify old file was deleted from filesystem
        self.assertFalse(dummy_old.exists())

    def test_vacuum_unexpired_files_preserved(self):
        """Files tombstoned within retention window (e.g. 1 day ago) must NOT be deleted."""
        recent_part = self.table_dir / "recent-part.parquet"
        recent_part.write_bytes(b"recent_data")

        run_optimize(self.table_dir, target_mb=128)

        # Modify deletionTimestamp to 24 hours ago (retention is 168h = 7 days)
        one_day_ago_ms = int((time.time() - (24 * 3600)) * 1000)
        commit_1 = self.table_dir / "_delta_log" / "00000000000000000001.json"
        
        lines = []
        with open(commit_1, "r", encoding="utf-8") as f:
            for l in f:
                if l.strip():
                    action = json.loads(l)
                    if "remove" in action:
                        action["remove"]["deletionTimestamp"] = one_day_ago_ms
                    lines.append(json.dumps(action))

        with open(commit_1, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        res = run_vacuum(self.table_dir, retention_hours=168)
        self.assertEqual(res["status"], "success")
        # Recent tombstoned file must be preserved
        self.assertTrue(recent_part.exists())


if __name__ == "__main__":
    unittest.main()
