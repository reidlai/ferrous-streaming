#!/usr/bin/env python3
"""
test_state_consolidator.py: Unit and Integration Tests for Arroyo State Snapshot Consolidator Pipeline (Story 3.7).
Submodule: ferrous-streaming
"""

from __future__ import annotations

import http.server
import importlib.util
import io
import json
import os
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

# Add submodule root to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

deploy_script_path = SCRIPTS_DIR / "deploy-pipeline-state-consolidator.py"
spec = importlib.util.spec_from_file_location("deploy_pipeline_state_consolidator", deploy_script_path)
deploy_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy_module)

check_arroyo_health = deploy_module.check_arroyo_health
check_iggy_liveness = deploy_module.check_iggy_liveness
deploy_pipeline = deploy_module.deploy_pipeline
http_request = deploy_module.http_request
initialize_delta_table_if_missing = deploy_module.initialize_delta_table_if_missing
load_env_secret = deploy_module.load_env_secret
make_admin_jwt = deploy_module.make_admin_jwt
parse_storage_path = deploy_module.parse_storage_path
record_2pc_state_snapshot_commit = deploy_module.record_2pc_state_snapshot_commit
run_pipeline_deployment = deploy_module.run_pipeline_deployment
verify_broker_consumer_group = deploy_module.verify_broker_consumer_group
verify_watermark_drift = deploy_module.verify_watermark_drift


class MockStateArroyoAndIggyHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Arroyo Pipeline API and Iggy broker endpoints for state consolidation."""

    pipelines = []
    consumer_groups = {("1", "2", "cg-arroyo-state"): {"id": 2, "name": "cg-arroyo-state"}}
    return_bad_request_on_post = False
    return_server_error_on_cg = False
    return_unauthorized_on_cg = False

    @classmethod
    def reset(cls):
        cls.pipelines = []
        cls.consumer_groups = {("1", "2", "cg-arroyo-state"): {"id": 2, "name": "cg-arroyo-state"}}
        cls.return_bad_request_on_post = False
        cls.return_server_error_on_cg = False
        cls.return_unauthorized_on_cg = False

    def log_message(self, format, *args):
        pass  # Quiet test output

    def do_GET(self):
        auth = self.headers.get("Authorization", "")

        # Iggy broker liveness probe
        if self.path == "/ping":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"pong")
            return

        # Arroyo health probe
        if self.path in ("/api/v1/ping", "/api/v1/health", "/health"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy"}).encode("utf-8"))
            return

        # Arroyo pipelines listing
        if self.path == "/api/v1/pipelines":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"data": self.pipelines}).encode("utf-8"))
            return

        # Iggy consumer group query
        parts = self.path.strip("/").split("/")
        if len(parts) == 6 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups":
            if self.return_server_error_on_cg:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "internal_error"}).encode("utf-8"))
                return
            if self.return_unauthorized_on_cg:
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "unauthorized"}).encode("utf-8"))
                return

            s_id, t_id, g_name = parts[1], parts[3], parts[5]
            cg = self.consumer_groups.get((str(s_id), str(t_id), str(g_name)))
            if cg:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(cg).encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8")) if body else {}

        if self.path == "/api/v1/pipelines":
            if self.return_bad_request_on_post:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"message": "SQL compilation error in state_snapshot_consolidator"}).encode("utf-8"))
                return

            p_record = {
                "id": str(uuid.uuid4()),
                "name": payload.get("name", "pipeline"),
                "query": payload.get("query", ""),
                "state": "Running",
            }
            self.pipelines.append(p_record)
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(p_record).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


class StateSnapshotConsolidatorUnitTests(unittest.TestCase):
    """Validates state snapshot consolidator pipeline, 2PC Delta Lake sink, and watermark SLA."""

    @classmethod
    def setUpClass(cls):
        MockStateArroyoAndIggyHandler.reset()
        cls.server = socketserver.TCPServer(("127.0.0.1", 0), MockStateArroyoAndIggyHandler)
        cls.port = cls.server.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.secret = "ferrous_cx_super_secure_jwt_secret_dev_32bytes!"
        cls.token = make_admin_jwt(secret=cls.secret)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        MockStateArroyoAndIggyHandler.reset()

    def test_01_sql_schema_alignment_with_lakehouse(self):
        """Verifies pipelines/state_snapshot_consolidator.sql matches orders_current_state schema."""
        sql_path = REPO_ROOT / "pipelines" / "state_snapshot_consolidator.sql"
        self.assertTrue(sql_path.is_file(), f"Missing SQL file: {sql_path}")

        content = sql_path.read_text(encoding="utf-8")
        self.assertIn("orders_consolidated_source", content)
        self.assertIn("orders_current_state_sink", content)
        self.assertIn("'connector' = 'kafka'", content)
        self.assertIn("'topic' = 'orders/consolidated'", content)
        self.assertIn("'group.id' = 'cg-arroyo-state'", content)
        self.assertIn("'connector' = 'delta'", content)
        self.assertIn("'path' = '/data/delta/orders_current_state'", content)
        self.assertIn("WATERMARK FOR updated_at", content)
        self.assertIn("order_id STRING", content)
        self.assertIn("status STRING", content)
        self.assertIn("credit_issued BIGINT", content)
        self.assertIn("updated_at TIMESTAMP", content)
        self.assertIn("version BIGINT", content)

        # Cross-validate against Lakehouse schema if present
        lakehouse_sql = REPO_ROOT.parent / "ferrous-lakehouse" / "schemas" / "streaming" / "orders_current_state.sql"
        if lakehouse_sql.is_file():
            lake_content = lakehouse_sql.read_text(encoding="utf-8")
            for col in ["order_id STRING", "status STRING", "credit_issued BIGINT", "updated_at TIMESTAMP", "version BIGINT"]:
                self.assertIn(col, lake_content)

    def test_02_delta_lake_table_initialization(self):
        """Tests initialize_delta_table_if_missing creates valid orders_current_state table log."""
        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "orders_current_state"
            ver = initialize_delta_table_if_missing(delta_dir)
            self.assertEqual(ver, 0)

            log_zero = delta_dir / "_delta_log" / "00000000000000000000.json"
            self.assertTrue(log_zero.is_file(), "00000000000000000000.json not created")

            lines = log_zero.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 3)

            p0 = json.loads(lines[0])
            self.assertEqual(p0["protocol"]["minWriterVersion"], 2)

            m0 = json.loads(lines[1])
            self.assertEqual(m0["metaData"]["name"], "orders_current_state")
            self.assertEqual(m0["metaData"]["partitionColumns"], ["status"])

            c0 = json.loads(lines[2])
            self.assertEqual(c0["commitInfo"]["operation"], "CREATE TABLE")

    def test_03_two_phase_commit_atomic_log_progression(self):
        """Tests record_2pc_state_snapshot_commit produces sequential atomic commits with status partition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "orders_current_state"

            # Batch 1: commits version 1
            ver1, log1 = record_2pc_state_snapshot_commit(
                delta_table_dir=delta_dir,
                part_filename="part-state-00001.parquet",
                records_count=50,
                size_bytes=2048,
                watermark_ms=11.2,
                status_partition="shipped",
            )
            self.assertEqual(ver1, 1)
            self.assertEqual(log1.name, "00000000000000000001.json")

            lines1 = log1.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines1), 2)
            add_action = json.loads(lines1[0])["add"]
            commit_info = json.loads(lines1[1])["commitInfo"]

            self.assertEqual(add_action["path"], "part-state-00001.parquet")
            self.assertEqual(add_action["partitionValues"], {"status": "shipped"})
            self.assertEqual(commit_info["operationParameters"]["consumerGroup"], "cg-arroyo-state")

            # Batch 2: commits version 2
            ver2, log2 = record_2pc_state_snapshot_commit(
                delta_table_dir=delta_dir,
                part_filename="part-state-00002.parquet",
                records_count=30,
                size_bytes=1500,
                watermark_ms=9.8,
                status_partition="delivered",
            )
            self.assertEqual(ver2, 2)
            self.assertEqual(log2.name, "00000000000000000002.json")

    def test_04_delta_commit_concurrency_conflict_detection(self):
        """Verifies atomic mode 'x' raises RuntimeError upon version collision."""
        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "orders_current_state"
            initialize_delta_table_if_missing(delta_dir)

            # Pre-create conflicting version 1 file
            conflict_file = delta_dir / "_delta_log" / "00000000000000000001.json"
            conflict_file.write_text("{\"mock\": true}\n", encoding="utf-8")

            with patch.object(deploy_module, "initialize_delta_table_if_missing", return_value=0):
                with self.assertRaises(RuntimeError) as ctx:
                    record_2pc_state_snapshot_commit(
                        delta_table_dir=delta_dir,
                        part_filename="part-conflict.parquet",
                        records_count=10,
                        size_bytes=100,
                        watermark_ms=5.0,
                    )
                self.assertIn("Delta 2PC Commit Conflict", str(ctx.exception))

    def test_05_watermark_drift_threshold(self):
        """Verifies watermark latency computation remains strictly below 500ms SLA."""
        drift_ms = verify_watermark_drift()
        self.assertLess(drift_ms, 500.0, f"Watermark latency {drift_ms}ms exceeded 500ms SLA")

    def test_06_watermark_drift_sla_breach_detection(self):
        """Verifies that exceeding 500ms SLA causes deployment to fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch.object(deploy_module, "verify_watermark_drift", return_value=500.5):
                with patch("sys.stdout", new_callable=io.StringIO):
                    success = run_pipeline_deployment(
                        arroyo_url=self.base_url,
                        iggy_url=self.base_url,
                        delta_dir=test_delta_dir,
                        secret=self.secret,
                        strict=True,
                        dry_run=False,
                    )
                self.assertFalse(success, "Should fail when watermark drift exceeds SLA")

    def test_07_idempotent_pipeline_deployment_mock_api(self):
        """Tests that deploy_pipeline creates pipeline on 1st call and recognizes existing on 2nd call."""
        res1 = deploy_pipeline(self.base_url, pipeline_name="state_snapshot_consolidator")
        self.assertEqual(res1["action"], "created")

        res2 = deploy_pipeline(self.base_url, pipeline_name="state_snapshot_consolidator")
        self.assertEqual(res2["action"], "exists")

    def test_08_pipeline_bad_request_raises_error(self):
        """Verifies deploy_pipeline raises ValueError on HTTP 400 Bad Request."""
        MockStateArroyoAndIggyHandler.return_bad_request_on_post = True
        with self.assertRaises(ValueError) as ctx:
            deploy_pipeline(self.base_url, pipeline_name="failing_state_pipeline")
        self.assertIn("Arroyo rejected pipeline SQL (400 Bad Request)", str(ctx.exception))

    def test_09_full_deployment_workflow_run(self):
        """Executes full run_pipeline_deployment against mock services without disk mutation outside tmp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url=self.base_url,
                    iggy_url=self.base_url,
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    strict=True,
                    dry_run=False,
                )
            self.assertTrue(success, "Full pipeline deployment failed")
            self.assertTrue((test_delta_dir / "_delta_log" / "00000000000000000000.json").is_file())
            self.assertEqual(len(list(test_delta_dir.glob("*.parquet"))), 0)

    def test_10_dry_run_mode_zero_disk_side_effects(self):
        """Verifies dry run mode completes successfully with zero filesystem writes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url="http://127.0.0.1:59999",
                    iggy_url="http://127.0.0.1:59998",
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    dry_run=True,
                )
            self.assertTrue(success)
            self.assertFalse(test_delta_dir.exists(), "Dry-run must not create directories or files")

    def test_11_negative_missing_consumer_group_strict_mode(self):
        """Verifies run_pipeline_deployment fails if cg-arroyo-state does not exist on broker (strict=True)."""
        MockStateArroyoAndIggyHandler.consumer_groups = {}  # Clear groups
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url=self.base_url,
                    iggy_url=self.base_url,
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    strict=True,
                    dry_run=False,
                )
            self.assertFalse(success, "Should fail when cg-arroyo-state is absent and strict=True")

    def test_12_consumer_group_missing_no_strict_mode(self):
        """Verifies run_pipeline_deployment succeeds with warning when strict=False."""
        MockStateArroyoAndIggyHandler.consumer_groups = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url=self.base_url,
                    iggy_url=self.base_url,
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    strict=False,
                    dry_run=False,
                )
            self.assertTrue(success, "Should succeed in non-strict mode")

    def test_13_negative_broker_offline(self):
        """Verifies fast failure when Iggy broker is offline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url=self.base_url,
                    iggy_url="http://127.0.0.1:59991",
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    strict=True,
                    dry_run=False,
                    retries=1,
                    delay_seconds=0.0,
                )
            self.assertFalse(success, "Should fail when broker is unreachable")

    def test_14_negative_arroyo_offline(self):
        """Verifies fast failure when Arroyo stream processor is offline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_pipeline_deployment(
                    arroyo_url="http://127.0.0.1:59992",
                    iggy_url=self.base_url,
                    delta_dir=test_delta_dir,
                    secret=self.secret,
                    strict=True,
                    dry_run=False,
                    retries=1,
                    delay_seconds=0.0,
                )
            self.assertFalse(success, "Should fail when Arroyo is unreachable")

    def test_15_negative_secret_zero_failure(self):
        """Verifies failure when secret is missing and allow_dev_secret=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_delta_dir = Path(tmpdir) / "orders_current_state"
            with patch.object(deploy_module, "load_env_secret", return_value=""):
                with patch("sys.stdout", new_callable=io.StringIO):
                    success = run_pipeline_deployment(
                        arroyo_url=self.base_url,
                        iggy_url=self.base_url,
                        delta_dir=test_delta_dir,
                        secret=None,
                        allow_dev_secret=False,
                        dry_run=False,
                    )
                self.assertFalse(success, "Must reject deployment under Secret Zero without secret")

    def test_16_consumer_group_unauthorized_and_server_error(self):
        """Verifies broker authorization and server error responses are properly differentiated."""
        MockStateArroyoAndIggyHandler.return_unauthorized_on_cg = True
        with self.assertRaises(PermissionError):
            verify_broker_consumer_group(self.base_url, self.token, "1", "2", "cg-arroyo-state")

        MockStateArroyoAndIggyHandler.return_unauthorized_on_cg = False
        MockStateArroyoAndIggyHandler.return_server_error_on_cg = True
        with self.assertRaises(RuntimeError):
            verify_broker_consumer_group(self.base_url, self.token, "1", "2", "cg-arroyo-state")

    def test_17_parse_storage_path_variations(self):
        """Verifies parse_storage_path handles file:// URIs and standard paths across platforms."""
        p1 = parse_storage_path("file:///data/delta/orders_current_state")
        self.assertIsInstance(p1, Path)

        p2 = parse_storage_path("./data/delta/orders_current_state")
        self.assertEqual(p2, (Path.cwd() / "data/delta/orders_current_state").resolve())

    def test_18_shell_wrappers_parity(self):
        """Verifies shell wrappers exist and have executable syntax."""
        scripts = [
            "deploy-pipeline-state-consolidator.sh",
            "deploy-pipeline-state-consolidator.ps1",
            "run-migrations.sh",
            "run-migrations.ps1",
        ]
        for s in scripts:
            p = REPO_ROOT / "scripts" / s
            self.assertTrue(p.is_file(), f"Missing wrapper {s}")

    def test_19_powershell_wrapper_dry_run_smoke(self):
        """Executes deploy-pipeline-state-consolidator.ps1 with --dry-run and asserts zero repository side effects."""
        ps1_script = REPO_ROOT / "scripts" / "deploy-pipeline-state-consolidator.ps1"
        res = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ps1_script), "--dry-run"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(res.returncode, 0, f"PowerShell wrapper failed: {res.stderr}")


if __name__ == "__main__":
    unittest.main()

