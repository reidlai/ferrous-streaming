#!/usr/bin/env python3
"""
test_olist_historical_ingestion.py: Unit and Integration Tests for Arroyo Olist Historical Ingestion Pipeline (Story 3.10).
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

deploy_script_path = SCRIPTS_DIR / "deploy-pipeline-olist-ingestion.py"
spec = importlib.util.spec_from_file_location("deploy_pipeline_olist_ingestion", deploy_script_path)
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
record_2pc_micro_batch_commit = deploy_module.record_2pc_micro_batch_commit
run_pipeline_deployment = deploy_module.run_pipeline_deployment
verify_broker_consumer_group = deploy_module.verify_broker_consumer_group
verify_watermark_drift = deploy_module.verify_watermark_drift


class MockOlistArroyoAndIggyHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Arroyo Pipeline API and Iggy broker endpoints for Olist historical ingestion."""

    pipelines = []
    consumer_groups = {("2", "1", "cg-arroyo-olist"): {"id": 1, "name": "cg-arroyo-olist"}}
    return_bad_request_on_post = False
    return_server_error_on_cg = False
    return_unauthorized_on_cg = False

    @classmethod
    def reset(cls):
        cls.pipelines = []
        cls.consumer_groups = {("2", "1", "cg-arroyo-olist"): {"id": 1, "name": "cg-arroyo-olist"}}
        cls.return_bad_request_on_post = False
        cls.return_server_error_on_cg = False
        cls.return_unauthorized_on_cg = False

    def log_message(self, format, *args):
        pass  # Suppress HTTP logging

    def do_GET(self):
        if self.path in ("/ping", "/api/v1/ping"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        if "/consumer-groups" in self.path:
            if self.return_server_error_on_cg:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'{"error":"Internal Server Error"}')
                return
            if self.return_unauthorized_on_cg:
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b'{"error":"Unauthorized"}')
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            cgs = list(self.consumer_groups.values())
            self.wfile.write(json.dumps(cgs).encode("utf-8"))
            return

        if self.path.startswith("/api/v1/pipelines"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"data": self.pipelines}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path.startswith("/api/v1/pipelines"):
            if self.return_bad_request_on_post:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"Invalid pipeline SQL syntax"}')
                return

            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            p_id = f"pipe_{len(self.pipelines) + 1}"
            entry = {
                "id": p_id,
                "name": body.get("name", "unnamed"),
                "query": body.get("query", ""),
                "parallelism": body.get("parallelism", 1),
            }
            self.pipelines.append(entry)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": p_id, "status": "active"}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def do_PATCH(self):
        if self.path.startswith("/api/v1/pipelines/"):
            p_id = self.path.split("/")[-1]
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            for p in self.pipelines:
                if p["id"] == p_id:
                    p["query"] = body.get("query", p["query"])
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"status":"updated"}')
                    return
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class TestOlistHistoricalIngestion(unittest.TestCase):
    """Test suite for Story 3.10 Arroyo Olist Historical Ingestion Job."""

    @classmethod
    def setUpClass(cls):
        cls.server = ThreadedTCPServer(("127.0.0.1", 0), MockOlistArroyoAndIggyHandler)
        cls.server_port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.mock_url = f"http://127.0.0.1:{cls.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        MockOlistArroyoAndIggyHandler.reset()

    def test_01_sql_schema_alignment_with_historical_orders(self):
        """Validates that olist_historical_ingestion.sql mirrors olist_orders schema."""
        sql_path = REPO_ROOT / "pipelines" / "olist_historical_ingestion.sql"
        self.assertTrue(sql_path.is_file(), f"Missing SQL pipeline file: {sql_path}")
        sql_content = sql_path.read_text(encoding="utf-8")

        self.assertIn("connector = 'kafka'", sql_content)
        self.assertIn("topic = 'olist.orders_stream'", sql_content)
        self.assertIn("'source.group_id' = 'cg-arroyo-olist'", sql_content)
        self.assertIn("path = '/data/delta/olist_orders'", sql_content)
        self.assertIn("order_id", sql_content)
        self.assertIn("customer_id", sql_content)
        self.assertIn("order_status", sql_content)
        self.assertIn("order_purchase_timestamp", sql_content)

    def test_02_idempotent_pipeline_deployment(self):
        """Verifies initial POST creation and subsequent PATCH update."""
        p1 = deploy_pipeline(self.mock_url, "olist_historical_ingestion")
        self.assertEqual(p1["action"], "created")
        self.assertEqual(len(MockOlistArroyoAndIggyHandler.pipelines), 1)

        p2 = deploy_pipeline(self.mock_url, "olist_historical_ingestion")
        self.assertEqual(p2["action"], "updated")
        self.assertEqual(len(MockOlistArroyoAndIggyHandler.pipelines), 1)

    def test_03_delta_lake_2pc_micro_batch_atomic_commit(self):
        """Verifies 2PC Delta transaction commit log appending."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_dir = Path(tmp_dir) / "olist_orders"
            ver_zero = initialize_delta_table_if_missing(delta_dir)
            self.assertEqual(ver_zero, 0)

            ver_one = record_2pc_micro_batch_commit(delta_dir, batch_records=250, file_path="part-0000.parquet")
            self.assertEqual(ver_one, 1)

            log_file = delta_dir / "_delta_log" / "00000000000000000001.json"
            self.assertTrue(log_file.is_file())
            content = log_file.read_text(encoding="utf-8")
            self.assertIn("numRecords", content)
            self.assertIn("250", content)
            self.assertIn('"operation": "STREAMING UPDATE"', content)

    def test_04_watermark_latency_bounds(self):
        """Verifies that watermark drift remains bounded under 500ms SLA."""
        drift_ms = verify_watermark_drift()
        self.assertLess(drift_ms, 500.0)

    def test_05_dry_run_execution(self):
        """Verifies that run_pipeline_deployment works in dry-run mode."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_dir = Path(tmp_dir) / "olist_orders"
            success = run_pipeline_deployment(
                arroyo_url="http://dummy:5115",
                iggy_url="http://dummy:3000",
                delta_dir=delta_dir,
                dry_run=True,
            )
            self.assertTrue(success)

    def test_06_live_deployment_flow(self):
        """Verifies the complete 5-step live deployment against mock servers."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_dir = Path(tmp_dir) / "olist_orders"
            success = run_pipeline_deployment(
                arroyo_url=self.mock_url,
                iggy_url=self.mock_url,
                delta_dir=delta_dir,
                secret="test_secret_for_jwt_auth_32_bytes_long!",
                service_account="svc_ferrous_streaming",
                dry_run=False,
            )
            self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
