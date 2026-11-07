import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

# Add store root and scripts to path
STORE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(STORE_DIR))
sys.path.insert(0, str(STORE_DIR / "scripts"))

import entrypoint


class TestContainerEntrypoint(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.data_dir_patch = mock.patch.object(entrypoint, "DATA_DIR", self.temp_dir)
        self.data_dir_patch.start()

    def tearDown(self):
        self.data_dir_patch.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_do_init_scaffolds_all_tables(self):
        """do_init creates directories and commit 0 for all core tables."""
        ret = entrypoint.do_init()
        self.assertEqual(ret, 0)
        for t in ["orders_current_state", "orders_raw_events", "llm_benchmarks"]:
            commit_0 = self.temp_dir / t / "_delta_log" / "00000000000000000000.json"
            self.assertTrue(commit_0.exists(), f"Missing commit 0 for table {t}")

    def test_do_healthcheck_success_and_failure(self):
        """do_healthcheck passes on initialized tables and fails on missing or invalid commits."""
        # Uninitialized: fails because commit 0 does not exist
        ret_uninit = entrypoint.do_healthcheck()
        self.assertEqual(ret_uninit, 1)

        # Initialized: passes
        entrypoint.do_init()
        ret_init = entrypoint.do_healthcheck()
        self.assertEqual(ret_init, 0)

        # Corrupted commit: fails
        commit_file = self.temp_dir / "orders_current_state" / "_delta_log" / "00000000000000000000.json"
        commit_file.write_text("corrupted json", encoding="utf-8")
        ret_corrupt = entrypoint.do_healthcheck()
        self.assertEqual(ret_corrupt, 1)

    def test_do_maintain_execution(self):
        """do_maintain executes compaction and vacuum routines without errors."""
        entrypoint.do_init()
        ret = entrypoint.do_maintain()
        self.assertEqual(ret, 0)

    def test_cli_flags_dispatch(self):
        """CLI arguments correctly invoke target functions."""
        with mock.patch("sys.exit") as mock_exit:
            with mock.patch("sys.argv", ["entrypoint.py", "--init"]):
                entrypoint.main()
                mock_exit.assert_called_with(0)

        with mock.patch("sys.exit") as mock_exit:
            with mock.patch("sys.argv", ["entrypoint.py", "--healthcheck"]):
                entrypoint.main()
                mock_exit.assert_called_with(0)


if __name__ == "__main__":
    unittest.main()
