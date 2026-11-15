#!/usr/bin/env python3
"""
test_workload_generator_parity.py: Cross-language parity verification test for HPC-MQBench (Story 13.8).
Verifies that Rust workload-generator produces identical schema and behavior to workload_generator.py.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SUBMODULE_DIR = Path(__file__).resolve().parent.parent
PYTHON_GEN = SUBMODULE_DIR / "benchmark" / "workload_generator.py"
CARGO_TARGET = SUBMODULE_DIR / "target" / "debug" / ("workload-generator.exe" if sys.platform == "win32" else "workload-generator")


class TestWorkloadGeneratorParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure Rust binary is built
        build_res = subprocess.run(
            ["cargo", "build", "--bin", "workload-generator"],
            cwd=str(SUBMODULE_DIR),
            capture_output=True,
            text=True
        )
        if build_res.returncode != 0:
            raise RuntimeError(f"cargo build failed: {build_res.stderr}")

    def test_schema_and_payload_parity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            py_out = Path(tmpdir) / "python_out.ndjson"
            rs_out = Path(tmpdir) / "rust_out.ndjson"

            run_id = "parity-run-42"
            count = 100
            payload_size = 512
            partitions = 4

            # Generate via Python
            subprocess.run(
                [
                    sys.executable,
                    str(PYTHON_GEN),
                    "--count", str(count),
                    "--payload-size", str(payload_size),
                    "--partitions", str(partitions),
                    "--run-id", run_id,
                    "--output", str(py_out)
                ],
                check=True
            )

            # Generate via Rust
            subprocess.run(
                [
                    str(CARGO_TARGET),
                    "--count", str(count),
                    "--payload-size", str(payload_size),
                    "--partitions", str(partitions),
                    "--run-id", run_id,
                    "--output", str(rs_out)
                ],
                check=True
            )

            py_lines = py_out.read_text(encoding="utf-8").strip().splitlines()
            rs_lines = rs_out.read_text(encoding="utf-8").strip().splitlines()

            self.assertEqual(len(py_lines), count)
            self.assertEqual(len(rs_lines), count)

            expected_keys = {
                "run_id", "record_id", "seq", "partition",
                "client_timestamp_us", "payload_size_bytes", "data"
            }

            for seq in range(count):
                py_rec = json.loads(py_lines[seq])
                rs_rec = json.loads(rs_lines[seq])

                self.assertEqual(set(py_rec.keys()), expected_keys)
                self.assertEqual(set(rs_rec.keys()), expected_keys)

                self.assertEqual(rs_rec["run_id"], py_rec["run_id"])
                self.assertEqual(rs_rec["record_id"], py_rec["record_id"])
                self.assertEqual(rs_rec["seq"], py_rec["seq"])
                self.assertEqual(rs_rec["partition"], py_rec["partition"])
                self.assertEqual(rs_rec["payload_size_bytes"], py_rec["payload_size_bytes"])
                self.assertIsInstance(rs_rec["client_timestamp_us"], int)
                self.assertGreater(rs_rec["client_timestamp_us"], 0)

                # Verify exact byte sizes
                self.assertEqual(len(py_lines[seq].encode("utf-8")), payload_size)
                self.assertEqual(len(rs_lines[seq].encode("utf-8")), payload_size)


if __name__ == "__main__":
    unittest.main()
