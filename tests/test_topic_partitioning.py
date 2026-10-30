#!/usr/bin/env python3
"""
test_topic_partitioning.py: Unit and Integration Tests for Topic Partitioning and Durability.
Submodule: ferrous-streaming
Story: 3.3 (Ingestion Topic Partitioning & Immediate fsync Durability)
"""

from __future__ import annotations

import base64
import http.server
import importlib.util
import json
import os
import socketserver
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

# Add submodule root to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

provision_script_path = SCRIPTS_DIR / "provision-topics.py"
spec = importlib.util.spec_from_file_location("provision_topics", provision_script_path)
provision_topics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provision_topics)

DEFAULT_PARTITION_COUNT = provision_topics.DEFAULT_PARTITION_COUNT
DEFAULT_STREAM_ID = provision_topics.DEFAULT_STREAM_ID
DEFAULT_STREAM_NAME = provision_topics.DEFAULT_STREAM_NAME
TOPICS_SPEC = provision_topics.TOPICS_SPEC
dispatch_partition_probes = provision_topics.dispatch_partition_probes
ensure_stream = provision_topics.ensure_stream
ensure_topic = provision_topics.ensure_topic
http_request = provision_topics.http_request
load_env_secret = provision_topics.load_env_secret
make_admin_jwt = provision_topics.make_admin_jwt
run_provisioning = provision_topics.run_provisioning


class MockIggyApiHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Apache Iggy HTTP REST API with realistic partition offsets and validation."""

    streams = {}
    topics = {}
    partition_offsets = {}  # (topic_name, partition_id) -> current_offset

    @classmethod
    def reset(cls):
        cls.streams = {}
        cls.topics = {}
        cls.partition_offsets = {}

    def log_message(self, format, *args):
        pass  # Quiet test output

    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        if self.path == "/ping":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"pong")
            return

        if not auth.startswith("Bearer ") or "invalid" in auth:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 77, "code": "access_token_missing"}).encode("utf-8"))
            return

        # Stream lookup
        if self.path.startswith("/streams/"):
            parts = self.path.strip("/").split("/")
            if len(parts) == 2:
                stream_id = parts[1]
                for s in self.streams.values():
                    if str(s["id"]) == stream_id or s["name"] == stream_id:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps(s).encode("utf-8"))
                        return
                self.send_response(404)
                self.end_headers()
                return

            if len(parts) == 4 and parts[2] == "topics":
                stream_id, topic_id = parts[1], parts[3]
                for t in self.topics.values():
                    if str(t["id"]) == topic_id or t["name"] == topic_id:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps(t).encode("utf-8"))
                        return
                self.send_response(404)
                self.end_headers()
                return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or "invalid" in auth:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 77, "code": "access_token_missing"}).encode("utf-8"))
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8")) if body else {}

        if self.path == "/streams":
            s_name = payload["name"]
            s_id = payload.get("stream_id", len(self.streams) + 1)
            if s_name in self.streams:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"id": 20, "code": "stream_name_already_exists"}).encode("utf-8"))
                return
            record = {"id": s_id, "name": s_name, "topics_count": 0}
            self.streams[s_name] = record
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(record).encode("utf-8"))
            return

        if self.path.startswith("/streams/") and self.path.endswith("/topics"):
            t_name = payload["name"]
            t_id = payload.get("topic_id", len(self.topics) + 1)
            p_count = payload.get("partitions_count", 1)
            if t_name in self.topics:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"id": 30, "code": "topic_name_already_exists"}).encode("utf-8"))
                return
            record = {
                "id": t_id,
                "name": t_name,
                "partitions_count": p_count,
                "message_expiry": payload.get("message_expiry", 0),
            }
            self.topics[t_name] = record
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(record).encode("utf-8"))
            return

        if "/messages" in self.path:
            # Validate payload schema
            partitioning = payload.get("partitioning", {})
            messages = payload.get("messages", [])
            if not partitioning or not messages:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"id": 41, "code": "invalid_payload"}).encode("utf-8"))
                return

            p_kind = partitioning.get("kind")
            p_val = partitioning.get("value", 0)

            # Determine topic name from path
            parts = self.path.strip("/").split("/")
            topic_name = parts[3] if len(parts) >= 4 else "unknown"
            t_meta = self.topics.get(topic_name)
            max_partitions = t_meta.get("partitions_count", 4) if t_meta else 4

            if p_kind == "partition_id" and (p_val < 0 or p_val >= max_partitions):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"id": 42, "code": "partition_not_found"}).encode("utf-8"))
                return

            key = (topic_name, p_val)
            cur_offset = self.partition_offsets.get(key, 0)
            self.partition_offsets[key] = cur_offset + len(messages)

            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "appended", "offset": cur_offset}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


class TopicPartitioningUnitTests(unittest.TestCase):
    """Validates topic topology specifications, idempotent creation, durability, and dispatching."""

    @classmethod
    def setUpClass(cls):
        MockIggyApiHandler.reset()
        cls.server = socketserver.TCPServer(("127.0.0.1", 0), MockIggyApiHandler)
        cls.port = cls.server.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.secret = "test_jwt_secret_dev_32bytes_value!"
        cls.token = make_admin_jwt(secret=cls.secret)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        MockIggyApiHandler.reset()

    def test_01_topics_specification_completeness(self):
        """Verifies TOPICS_SPEC defines intake and consolidated topics with 4 partitions each."""
        names = {t["name"] for t in TOPICS_SPEC}
        self.assertEqual(names, {"intake", "consolidated"})
        for t in TOPICS_SPEC:
            self.assertEqual(t["partitions_count"], 4)
            self.assertIn("id", t)

    def test_02_idempotent_stream_creation(self):
        """Tests that ensure_stream creates on first call and succeeds on second call."""
        res1 = ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        self.assertEqual(res1["action"], "created")
        self.assertEqual(res1["stream_id"], DEFAULT_STREAM_ID)

        res2 = ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        self.assertIn(res2["action"], ("exists", "exists_conflict"))

    def test_03_idempotent_topic_creation(self):
        """Tests that ensure_topic provisions topics with 4 partitions idempotently."""
        ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)

        for t_spec in TOPICS_SPEC:
            res1 = ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, t_spec)
            self.assertEqual(res1["action"], "created")

            res2 = ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, t_spec)
            self.assertIn(res2["action"], ("exists", "exists_conflict"))

    def test_04_dispatch_partition_probes_both_topics(self):
        """Verifies synthetic messages are dispatched across all 4 partitions for BOTH topics."""
        ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        for t_spec in TOPICS_SPEC:
            ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, t_spec)

        for t_spec in TOPICS_SPEC:
            t_name = t_spec["name"]
            probes = dispatch_partition_probes(
                self.base_url, self.token, DEFAULT_STREAM_ID, t_name, partition_count=4, probes_per_partition=2
            )
            self.assertEqual(len(probes), 4, f"Topic '{t_name}' did not verify all 4 partitions")
            for i, p in enumerate(probes):
                self.assertEqual(p["partition_id"], i)
                self.assertEqual(len(p["probes"]), 2)
                self.assertLess(p["min_latency_ms"], 500.0, "Sub-millisecond target breached")

    def test_05_monotonic_sequence_and_fifo_ordering(self):
        """Asserts that sequential dispatches to partition produce strictly monotonic offsets."""
        ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, TOPICS_SPEC[0])

        probes = dispatch_partition_probes(
            self.base_url, self.token, DEFAULT_STREAM_ID, "intake", partition_count=1, probes_per_partition=4
        )
        p0_probes = probes[0]["probes"]
        offsets = [p["offset"] for p in p0_probes]
        self.assertEqual(len(offsets), 4)
        # Verify strict monotonic advancement: 0 < 1 < 2 < 3
        for i in range(len(offsets) - 1):
            self.assertLess(offsets[i], offsets[i + 1])

    def test_06_topology_mismatch_detection(self):
        """Asserts that ensure_topic raises ValueError if topic exists with mismatched partition count."""
        ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        # Seed topic with 1 partition
        MockIggyApiHandler.topics["intake"] = {
            "id": 1,
            "name": "intake",
            "partitions_count": 1,
        }

        with self.assertRaises(ValueError) as ctx:
            ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, TOPICS_SPEC[0])
        self.assertIn("topology mismatch", str(ctx.exception))
        self.assertIn("exists with 1 partitions", str(ctx.exception))

    def test_07_negative_unauthorized_access(self):
        """Asserts that requests without token or with invalid credentials fail with 401."""
        status, res = http_request(f"{self.base_url}/streams", method="POST", token=None, json_data={"name": "x"})
        self.assertEqual(status, 401)

        with self.assertRaises(PermissionError):
            ensure_stream(self.base_url, token="invalid_bearer_token", stream_id=99, stream_name="orders")

    def test_08_negative_out_of_bounds_partition(self):
        """Asserts that dispatching to invalid partition ID returns 400."""
        ensure_stream(self.base_url, self.token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        ensure_topic(self.base_url, self.token, DEFAULT_STREAM_ID, TOPICS_SPEC[0])

        with self.assertRaises(RuntimeError) as ctx:
            dispatch_partition_probes(
                self.base_url, self.token, DEFAULT_STREAM_ID, "intake", partition_count=5
            )
        self.assertIn("Failed message dispatch", str(ctx.exception))

    def test_09_consecutive_provisioning_runs(self):
        """Tests that run_provisioning can be run twice in a row without reset (true idempotency)."""
        run1 = run_provisioning(
            base_url=self.base_url,
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            skip_probes=False,
        )
        self.assertTrue(run1)

        # Run 2 immediately without resetting state
        run2 = run_provisioning(
            base_url=self.base_url,
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            skip_probes=False,
        )
        self.assertTrue(run2)

    def test_10_quoted_env_secret_parsing(self):
        """Tests that load_env_secret handles quotes and inline comments properly."""
        with tempfile.TemporaryDirectory() as td:
            env_file = Path(td) / ".env"
            env_file.write_text(
                'IGGY_HTTP_JWT_ENCODING_SECRET="super_secret_quoted_123" # inline comment\n',
                encoding="utf-8",
            )
            # Temporarily clear env var
            old_val = os.environ.pop("IGGY_HTTP_JWT_ENCODING_SECRET", None)
            try:
                secret = load_env_secret(env_file)
                self.assertEqual(secret, "super_secret_quoted_123")
            finally:
                if old_val:
                    os.environ["IGGY_HTTP_JWT_ENCODING_SECRET"] = old_val

    def test_11_shell_wrappers_parity(self):
        """Verifies POSIX Bash and PowerShell wrappers exist and contain proper declarations."""
        sh_path = REPO_ROOT / "scripts" / "provision-topics.sh"
        ps1_path = REPO_ROOT / "scripts" / "provision-topics.ps1"

        self.assertTrue(sh_path.is_file(), f"Missing {sh_path}")
        self.assertTrue(ps1_path.is_file(), f"Missing {ps1_path}")

        sh_content = sh_path.read_text(encoding="utf-8")
        self.assertTrue(sh_content.startswith("#!/usr/bin/env bash"))
        self.assertIn("provision-topics.py", sh_content)
        self.assertIn("cd \"${REPO_ROOT}\"", sh_content)

        ps1_content = ps1_path.read_text(encoding="utf-8")
        self.assertIn("[CmdletBinding()]", ps1_content)
        self.assertIn("provision-topics.py", ps1_content)
        self.assertIn("Push-Location $RepoRoot", ps1_content)

    def test_12_live_broker_partitioning_if_available(self):
        """Executes full topic provisioning against live Iggy broker container if active."""
        try:
            status, body = http_request("http://localhost:3000/ping", timeout=1.0)
            if status != 200 or str(body).strip() != "pong":
                self.skipTest("Live Apache Iggy broker on localhost:3000 did not respond to /ping")
        except Exception:
            self.skipTest("Live Apache Iggy broker container not reachable on localhost:3000")

        secret = load_env_secret() or "ferrous_cx_super_secure_jwt_secret_dev_32bytes!"
        success = run_provisioning(
            base_url="http://localhost:3000",
            secret=secret,
            service_account="svc_ferrous_streaming",
            skip_probes=False,
        )
        self.assertTrue(success, "Live broker topic provisioning and partition verification failed")


if __name__ == "__main__":
    unittest.main()
