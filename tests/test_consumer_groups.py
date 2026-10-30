#!/usr/bin/env python3
"""
test_consumer_groups.py: Unit and Integration Tests for Zero-Copy Multi-Consumer Group Fan-Out.
Submodule: ferrous-streaming
Story: 3.4 (Zero-Copy Multi-Consumer Group Fan-Out)
"""

from __future__ import annotations

import base64
import http.server
import importlib.util
import json
import os
import socketserver
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

# Add submodule root to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

cg_script_path = SCRIPTS_DIR / "provision-consumer-groups.py"
spec = importlib.util.spec_from_file_location("provision_consumer_groups", cg_script_path)
provision_cg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provision_cg)

CONSUMER_GROUPS_SPEC = provision_cg.CONSUMER_GROUPS_SPEC
DEFAULT_STREAM_ID = provision_cg.DEFAULT_STREAM_ID
DEFAULT_STREAM_NAME = provision_cg.DEFAULT_STREAM_NAME
ensure_consumer_group = provision_cg.ensure_consumer_group
http_request = provision_cg.http_request
load_env_secret = provision_cg.load_env_secret
make_admin_jwt = provision_cg.make_admin_jwt
run_provisioning = provision_cg.run_provisioning
verify_fan_out_throughput = provision_cg.verify_fan_out_throughput


class MockIggyConsumerGroupHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Apache Iggy HTTP REST API for consumer groups."""

    consumer_groups = {}  # (stream_id, topic_id, group_name) -> group_dict
    consumer_offsets = {}  # (stream_id, topic_id, group_id, partition_id) -> int offset
    messages = []

    @classmethod
    def reset(cls):
        cls.consumer_groups = {}
        cls.consumer_offsets = {}
        cls.messages = []

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

        if "forbidden" in auth:
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 78, "code": "forbidden"}).encode("utf-8"))
            return

        if "server_error" in auth:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 99, "code": "internal_server_error"}).encode("utf-8"))
            return

        if not auth.startswith("Bearer ") or "invalid" in auth:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 77, "code": "access_token_missing"}).encode("utf-8"))
            return

        # Lookup consumer group offsets: /streams/{s}/topics/{t}/consumer-groups/{g}/offsets/{p}
        parts = self.path.strip("/").split("/")
        if len(parts) == 8 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups" and parts[6] == "offsets":
            s_id, t_id, g_id, p_id = parts[1], parts[3], parts[5], parts[7]
            offset = self.consumer_offsets.get((str(s_id), str(t_id), str(g_id), str(p_id)), 0)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"partition_id": int(p_id), "current_offset": offset}).encode("utf-8"))
            return

        # Lookup stream: /streams/{s}
        if len(parts) == 2 and parts[0] == "streams":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": int(parts[1]) if parts[1].isdigit() else 1, "name": "orders"}).encode("utf-8"))
            return

        # Lookup consumer group: /streams/{s}/topics/{t}/consumer-groups/{g}
        if len(parts) == 6 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups":
            s_id, t_id, g_id = parts[1], parts[3], parts[5]
            for key, val in self.consumer_groups.items():
                if str(key[0]) == str(s_id) and str(key[1]) == str(t_id):
                    if str(val["id"]) == str(g_id) or str(val["name"]) == str(g_id):
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps(val).encode("utf-8"))
                        return
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(404)
        self.end_headers()

    def do_PUT(self):
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

        # Store offset: /streams/{s}/topics/{t}/consumer-groups/{g}/offsets/{p}
        parts = self.path.strip("/").split("/")
        if len(parts) == 8 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups" and parts[6] == "offsets":
            s_id, t_id, g_id, p_id = parts[1], parts[3], parts[5], parts[7]
            new_offset = payload.get("offset", 0)
            self.consumer_offsets[(str(s_id), str(t_id), str(g_id), str(p_id))] = new_offset
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"partition_id": int(p_id), "stored_offset": new_offset}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        auth = self.headers.get("Authorization", "")
        if "forbidden" in auth:
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 78, "code": "forbidden"}).encode("utf-8"))
            return

        if "server_error" in auth:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 99, "code": "internal_server_error"}).encode("utf-8"))
            return

        if not auth.startswith("Bearer ") or "invalid" in auth:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 77, "code": "access_token_missing"}).encode("utf-8"))
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8")) if body else {}

        # POST /streams/{s}/topics/{t}/consumer-groups
        parts = self.path.strip("/").split("/")
        if len(parts) == 5 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups":
            s_id, t_id = parts[1], parts[3]
            g_name = payload["name"]
            g_id = payload.get("group_id", len(self.consumer_groups) + 1)

            # Check if name or group_id already exists
            for key, val in self.consumer_groups.items():
                if str(key[0]) == str(s_id) and str(key[1]) == str(t_id):
                    if val["name"] == g_name:
                        self.send_response(400)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            "id": 40,
                            "code": "consumer_group_name_already_exists"
                        }).encode("utf-8"))
                        return
                    if int(val["id"]) == int(g_id):
                        self.send_response(400)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            "id": 41,
                            "code": "consumer_group_id_already_exists"
                        }).encode("utf-8"))
                        return

            record = {
                "id": g_id,
                "name": g_name,
                "members_count": 0,
                "partitions_count": 4,
            }
            self.consumer_groups[(s_id, t_id, g_name)] = record
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(record).encode("utf-8"))
            return

        if "/messages" in self.path:
            self.messages.append(payload)
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "appended"}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


class ConsumerGroupsUnitTests(unittest.TestCase):
    """Validates consumer group specifications, fan-out isolation, and throughput."""

    @classmethod
    def setUpClass(cls):
        MockIggyConsumerGroupHandler.reset()
        cls.server = socketserver.TCPServer(("127.0.0.1", 0), MockIggyConsumerGroupHandler)
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
        MockIggyConsumerGroupHandler.reset()

    def test_01_consumer_groups_specification_completeness(self):
        """Verifies CONSUMER_GROUPS_SPEC defines cg-arroyo-raw, cg-cmd-handler, and cg-arroyo-state."""
        names = {cg["name"] for cg in CONSUMER_GROUPS_SPEC}
        self.assertEqual(names, {"cg-arroyo-raw", "cg-cmd-handler", "cg-arroyo-state"})

        # Check intake topic has both arroyo-raw and cmd-handler
        intake_groups = [cg["name"] for cg in CONSUMER_GROUPS_SPEC if cg["topic_name"] == "intake"]
        self.assertEqual(set(intake_groups), {"cg-arroyo-raw", "cg-cmd-handler"})

        # Check consolidated topic has arroyo-state
        consolidated_groups = [cg["name"] for cg in CONSUMER_GROUPS_SPEC if cg["topic_name"] == "consolidated"]
        self.assertEqual(consolidated_groups, ["cg-arroyo-state"])

    def test_02_idempotent_consumer_group_creation(self):
        """Tests that ensure_consumer_group creates on 1st call and succeeds idempotently on 2nd call."""
        res1 = ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-arroyo-raw")
        self.assertEqual(res1["action"], "created")
        self.assertEqual(res1["group_id"], 1)

        # 2nd call -> exists
        res2 = ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-arroyo-raw")
        self.assertIn(res2["action"], ("exists", "exists_conflict"))

    def test_03_multi_consumer_group_fan_out_isolation(self):
        """Verifies both consumer groups can be provisioned concurrently on topic 'intake'."""
        res_raw = ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-arroyo-raw")
        res_cmd = ensure_consumer_group(self.base_url, self.token, 1, 1, 2, "cg-cmd-handler")

        self.assertEqual(res_raw["action"], "created")
        self.assertEqual(res_cmd["action"], "created")
        self.assertNotEqual(res_raw["group_id"], res_cmd["group_id"])

    def test_04_fan_out_burst_throughput(self):
        """Tests burst message dispatch to orders/intake measuring throughput."""
        bench = verify_fan_out_throughput(self.base_url, self.token, stream_id=1, topic_name="intake", batch_size=20)
        self.assertEqual(bench["batch_size"], 20)
        self.assertGreater(bench["throughput_msgs_per_sec"], 25.0)
        self.assertEqual(bench["fan_out_groups"], ["cg-arroyo-raw", "cg-cmd-handler"])

    def test_05_consecutive_provisioning_runs(self):
        """Tests that run_provisioning runs consecutively without failure (idempotency)."""
        run1 = run_provisioning(
            base_url=self.base_url,
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            skip_benchmark=False,
        )
        self.assertTrue(run1)

        run2 = run_provisioning(
            base_url=self.base_url,
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            skip_benchmark=False,
        )
        self.assertTrue(run2)

    def test_06_negative_unauthorized_access(self):
        """Asserts that requests without valid credentials fail with 401."""
        status, res = http_request(
            f"{self.base_url}/streams/1/topics/1/consumer-groups",
            method="POST",
            token="invalid_token",
            json_data={"name": "test"},
        )
        self.assertEqual(status, 401)

        with self.assertRaises(PermissionError):
            ensure_consumer_group(self.base_url, token="invalid_token", stream_id=1, topic_id=1, group_id=1, group_name="x")

    def test_07_shell_wrappers_parity(self):
        """Verifies POSIX Bash and PowerShell wrappers exist and contain proper declarations."""
        sh_path = REPO_ROOT / "scripts" / "provision-consumer-groups.sh"
        ps1_path = REPO_ROOT / "scripts" / "provision-consumer-groups.ps1"

        self.assertTrue(sh_path.is_file(), f"Missing {sh_path}")
        self.assertTrue(ps1_path.is_file(), f"Missing {ps1_path}")

        sh_content = sh_path.read_text(encoding="utf-8")
        self.assertTrue(sh_content.startswith("#!/usr/bin/env bash"))
        self.assertIn("provision-consumer-groups.py", sh_content)

        ps1_content = ps1_path.read_text(encoding="utf-8")
        self.assertIn("[CmdletBinding()]", ps1_content)
        self.assertIn("provision-consumer-groups.py", ps1_content)

    def test_08_cg_arroyo_state_provisioning_on_consolidated_topic(self):
        """Validates cg-arroyo-state is provisioned on stream 1, topic 2 (consolidated)."""
        res = ensure_consumer_group(self.base_url, self.token, 1, 2, 1, "cg-arroyo-state")
        self.assertEqual(res["action"], "created")
        self.assertEqual(res["group_id"], 1)

        # Idempotent second check
        res2 = ensure_consumer_group(self.base_url, self.token, 1, 2, 1, "cg-arroyo-state")
        self.assertIn(res2["action"], ("exists", "exists_conflict"))

    def test_09_isolated_offset_simulation_no_leakage(self):
        """Simulates and verifies that advancing cg-arroyo-raw offset on partition 0 does not alter cg-cmd-handler."""
        # Ensure groups exist
        ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-arroyo-raw")
        ensure_consumer_group(self.base_url, self.token, 1, 1, 2, "cg-cmd-handler")

        # Initial offsets should be 0
        s_raw, o_raw = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/1/offsets/0", token=self.token)
        s_cmd, o_cmd = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/2/offsets/0", token=self.token)
        self.assertEqual(s_raw, 200)
        self.assertEqual(s_cmd, 200)
        self.assertEqual(o_raw.get("current_offset"), 0)
        self.assertEqual(o_cmd.get("current_offset"), 0)

        # Advance cg-arroyo-raw offset to 450
        s_put, _ = http_request(
            f"{self.base_url}/streams/1/topics/1/consumer-groups/1/offsets/0",
            method="PUT",
            token=self.token,
            json_data={"offset": 450},
        )
        self.assertEqual(s_put, 200)

        # Verify cg-arroyo-raw offset is 450, while cg-cmd-handler offset remains 0
        _, o_raw_after = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/1/offsets/0", token=self.token)
        _, o_cmd_after = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/2/offsets/0", token=self.token)
        self.assertEqual(o_raw_after.get("current_offset"), 450)
        self.assertEqual(o_cmd_after.get("current_offset"), 0, "Offset leakage detected between consumer groups!")

    def test_10_micro_burst_throughput_threshold(self):
        """Tests micro-burst batch throughput ensuring >500 msgs/s threshold (AC 3)."""
        bench = verify_fan_out_throughput(self.base_url, self.token, stream_id=1, topic_name="intake", batch_size=100)
        self.assertEqual(bench["batch_size"], 100)
        self.assertGreater(bench["throughput_msgs_per_sec"], 500.0, "Throughput fell below 500 msgs/s micro-burst threshold")

    def test_11_negative_auth_forbidden_and_empty_secret(self):
        """Validates error paths: empty secret ValueError, 403 Forbidden PermissionError, 500 RuntimeError."""
        with self.assertRaises(ValueError):
            make_admin_jwt(secret="")

        with self.assertRaises(PermissionError):
            ensure_consumer_group(self.base_url, token="forbidden_token", stream_id=1, topic_id=1, group_id=1, group_name="x")

        with self.assertRaises(RuntimeError):
            ensure_consumer_group(self.base_url, token="server_error_token", stream_id=1, topic_id=1, group_id=1, group_name="x")

    def test_12_consumer_group_id_collision_raises_value_error(self):
        """Verifies that attempting to register an existing group_id under a different name raises ValueError."""
        # Provision group 1 as 'cg-alpha'
        ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-alpha")

        # Attempt to provision group 1 as 'cg-beta' (mismatched ID collision)
        with self.assertRaises(ValueError) as ctx:
            ensure_consumer_group(self.base_url, self.token, 1, 1, 1, "cg-beta")
        self.assertIn("already occupied", str(ctx.exception))

    def test_13_live_broker_consumer_groups_if_available(self):
        """Executes full consumer group provisioning against live broker container if running."""
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
            skip_benchmark=False,
        )
        self.assertTrue(success, "Live broker consumer group provisioning failed")


if __name__ == "__main__":
    unittest.main()

