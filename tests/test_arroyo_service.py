#!/usr/bin/env python3
"""
test_arroyo_service.py: Unit and Integration Tests for Arroyo Stream Processor Deployment.
Submodule: ferrous-streaming
Story: 3.5 (Arroyo Stream Processor Deployment & Broker Ingestion via M2M Account)
"""

from __future__ import annotations

import base64
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

arroyo_script_path = SCRIPTS_DIR / "provision-arroyo.py"
spec = importlib.util.spec_from_file_location("provision_arroyo", arroyo_script_path)
provision_arroyo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provision_arroyo)

ARROYO_CONSUMER_GROUPS = provision_arroyo.ARROYO_CONSUMER_GROUPS
check_arroyo_health = provision_arroyo.check_arroyo_health
check_iggy_liveness = provision_arroyo.check_iggy_liveness
http_request = provision_arroyo.http_request
load_env_secret = provision_arroyo.load_env_secret
make_admin_jwt = provision_arroyo.make_admin_jwt
register_arroyo_connection = provision_arroyo.register_arroyo_connection
run_arroyo_provisioning = provision_arroyo.run_arroyo_provisioning
verify_broker_consumer_group = provision_arroyo.verify_broker_consumer_group


class MockCombinedServerHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Arroyo API on port 5115 and Iggy broker on port 3000."""

    arroyo_tables = []
    consumer_groups = {
        ("1", "1", "cg-arroyo-raw"): {"id": 1, "name": "cg-arroyo-raw", "partitions_count": 4},
        ("1", "2", "cg-arroyo-state"): {"id": 1, "name": "cg-arroyo-state", "partitions_count": 4},
    }
    consumer_offsets = {}  # (s, t, g, p) -> int

    @classmethod
    def reset(cls):
        cls.arroyo_tables = []
        cls.consumer_offsets = {}
        cls.consumer_groups = {
            ("1", "1", "cg-arroyo-raw"): {"id": 1, "name": "cg-arroyo-raw", "partitions_count": 4},
            ("1", "2", "cg-arroyo-state"): {"id": 1, "name": "cg-arroyo-state", "partitions_count": 4},
        }

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
        if self.path in ("/api/v1/health", "/api/v1/ping", "/health"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy", "service": "arroyo-controller"}).encode("utf-8"))
            return

        # Arroyo connection tables
        if self.path == "/api/v1/connection_tables":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"data": self.arroyo_tables}).encode("utf-8"))
            return

        # Iggy Consumer group check: /streams/{s}/topics/{t}/consumer-groups/{name}
        if not auth.startswith("Bearer ") or "invalid" in auth:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": 77, "code": "access_token_missing"}).encode("utf-8"))
            return

        parts = self.path.strip("/").split("/")
        # Offset query: /streams/{s}/topics/{t}/consumer-groups/{g}/offsets/{p}
        if len(parts) == 8 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups" and parts[6] == "offsets":
            s_id, t_id, g_name, p_id = parts[1], parts[3], parts[5], parts[7]
            offset = self.consumer_offsets.get((str(s_id), str(t_id), str(g_name), str(p_id)), 0)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"partition_id": int(p_id), "offset": offset}).encode("utf-8"))
            return

        if len(parts) == 6 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups":
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

    def do_PUT(self):
        # Store offset: /streams/{s}/topics/{t}/consumer-groups/{g}/offsets/{p}
        parts = self.path.strip("/").split("/")
        if len(parts) == 8 and parts[0] == "streams" and parts[2] == "topics" and parts[4] == "consumer-groups" and parts[6] == "offsets":
            s_id, t_id, g_name, p_id = parts[1], parts[3], parts[5], parts[7]
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8")) if body else {}
            new_offset = payload.get("offset", 0)
            self.consumer_offsets[(str(s_id), str(t_id), str(g_name), str(p_id))] = new_offset
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"partition_id": int(p_id), "stored_offset": new_offset}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8")) if body else {}

        if self.path == "/api/v1/connection_tables":
            self.arroyo_tables.append(payload)
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": len(self.arroyo_tables), "status": "created", **payload}).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


class ArroyoServiceUnitTests(unittest.TestCase):
    """Tests Arroyo stream processor deployment and broker ingestion configuration."""

    @classmethod
    def setUpClass(cls):
        MockCombinedServerHandler.reset()
        cls.server = socketserver.TCPServer(("127.0.0.1", 0), MockCombinedServerHandler)
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
        MockCombinedServerHandler.reset()

    def test_01_arroyo_service_docker_compose_specification(self):
        """Verifies docker-compose.yml defines arroyo with port 5115, non-root user, and volume mounts."""
        compose_path = REPO_ROOT / "docker-compose.yml"
        self.assertTrue(compose_path.is_file(), "docker-compose.yml not found")

        content = compose_path.read_text(encoding="utf-8")
        self.assertIn("arroyo:", content)
        self.assertIn("ferrous-streaming-arroyo:latest", content)
        self.assertIn('"5115:5115"', content)
        self.assertIn('"10001:10001"', content)
        self.assertIn("./data/arroyo:/data/arroyo", content)
        self.assertIn("./data/delta:/data/delta", content)
        self.assertIn("depends_on:", content)
        self.assertIn("iggy-broker:", content)

    def test_02_arroyo_dockerfile_specification(self):
        """Verifies Dockerfile.arroyo adheres to rootless UID 10001 and exposes port 5115."""
        dockerfile_path = REPO_ROOT / "Dockerfile.arroyo"
        self.assertTrue(dockerfile_path.is_file(), "Dockerfile.arroyo not found")

        content = dockerfile_path.read_text(encoding="utf-8")
        self.assertIn("USER 10001:10001", content)
        self.assertIn("EXPOSE 5115", content)
        self.assertIn("/api/v1/ping", content)

    def test_03_arroyo_config_toml_specification(self):
        """Verifies config/arroyo.toml specifies correct port, checkpoint storage, and scheduler."""
        toml_path = REPO_ROOT / "config" / "arroyo.toml"
        self.assertTrue(toml_path.is_file(), "config/arroyo.toml not found")

        content = toml_path.read_text(encoding="utf-8")
        self.assertIn('bind_address = "0.0.0.0:5115"', content)
        self.assertIn('checkpoint-url = "file:///data/arroyo/checkpoints"', content)
        self.assertIn('scheduler = "node"', content)
        self.assertIn('default_parallelism = 1', content)

    def test_04_m2m_jwt_generation_for_svc_ferrous_streaming(self):
        """Verifies JWT structure, signature, and claims for svc_ferrous_streaming."""
        jwt = make_admin_jwt(secret=self.secret, service_account="svc_ferrous_streaming")
        parts = jwt.split(".")
        self.assertEqual(len(parts), 3)

        payload_bytes = base64.urlsafe_b64decode(parts[1] + "==")
        payload = json.loads(payload_bytes.decode("utf-8"))
        self.assertEqual(payload["name"], "svc_ferrous_streaming")
        self.assertEqual(payload["role"], "admin")
        self.assertEqual(payload["iss"], "iggy.apache.org")
        self.assertEqual(payload["aud"], "iggy.apache.org")

    def test_05_dual_consumer_group_specification_isolation(self):
        """Verifies ARROYO_CONSUMER_GROUPS defines both cg-arroyo-raw and cg-arroyo-state."""
        names = {cg["name"] for cg in ARROYO_CONSUMER_GROUPS}
        self.assertEqual(names, {"cg-arroyo-raw", "cg-arroyo-state"})

        raw_cg = next(cg for cg in ARROYO_CONSUMER_GROUPS if cg["name"] == "cg-arroyo-raw")
        self.assertEqual(raw_cg["topic_name"], "intake")
        self.assertEqual(raw_cg["pipeline"], "raw_event_archiver")

        state_cg = next(cg for cg in ARROYO_CONSUMER_GROUPS if cg["name"] == "cg-arroyo-state")
        self.assertEqual(state_cg["topic_name"], "consolidated")
        self.assertEqual(state_cg["pipeline"], "state_snapshot_consolidator")

    def test_06_mock_arroyo_health_and_broker_liveness(self):
        """Tests health check probes against simulated Arroyo (5115) and Iggy (3000)."""
        self.assertTrue(check_arroyo_health(self.base_url))
        self.assertTrue(check_iggy_liveness(self.base_url))

    def test_07_full_arroyo_provisioning_run(self):
        """Tests full run_arroyo_provisioning against the mock server."""
        success = run_arroyo_provisioning(
            arroyo_url=self.base_url,
            iggy_url=self.base_url,
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            dry_run=False,
        )
        self.assertTrue(success)

    def test_08_dry_run_mode(self):
        """Verifies dry-run mode completes without requiring active network listeners."""
        success = run_arroyo_provisioning(
            arroyo_url="http://127.0.0.1:59999",
            iggy_url="http://127.0.0.1:59998",
            secret=self.secret,
            service_account="svc_ferrous_streaming",
            dry_run=True,
        )
        self.assertTrue(success)

    def test_09_negative_unauthorized_iggy_broker(self):
        """Verifies querying consumer group with invalid token raises PermissionError."""
        with self.assertRaises(PermissionError):
            verify_broker_consumer_group(
                iggy_url=self.base_url,
                token="invalid_token",
                stream_id=1,
                topic_id=1,
                group_id=1,
                group_name="cg-arroyo-raw",
            )

    def test_10_negative_empty_secret(self):
        """Verifies make_admin_jwt raises ValueError on empty secret."""
        with self.assertRaises(ValueError):
            make_admin_jwt(secret="")

    def test_11_shell_wrappers_parity(self):
        """Verifies POSIX Bash and PowerShell wrappers exist and contain proper declarations."""
        sh_path = REPO_ROOT / "scripts" / "provision-arroyo.sh"
        ps1_path = REPO_ROOT / "scripts" / "provision-arroyo.ps1"

        self.assertTrue(sh_path.is_file(), f"Missing {sh_path}")
        self.assertTrue(ps1_path.is_file(), f"Missing {ps1_path}")

        sh_content = sh_path.read_text(encoding="utf-8")
        self.assertTrue(sh_content.startswith("#!/usr/bin/env bash"))
        self.assertIn("provision-arroyo.py", sh_content)

        ps1_content = ps1_path.read_text(encoding="utf-8")
        self.assertIn("[CmdletBinding()]", ps1_content)
        self.assertIn("provision-arroyo.py", ps1_content)

    def test_12_offline_service_health_check_failure(self):
        """Verifies health check probes fail quickly when service endpoints are offline."""
        self.assertFalse(check_arroyo_health("http://127.0.0.1:59997", retries=1, delay_seconds=0.1))
        self.assertFalse(check_iggy_liveness("http://127.0.0.1:59997", retries=1, delay_seconds=0.1))

    def test_13_secret_zero_rejection_without_dev_fallback(self):
        """Verifies run_arroyo_provisioning fails when no secret is configured and allow_dev_secret=False."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("sys.stdout", new_callable=io.StringIO):
                success = run_arroyo_provisioning(
                    arroyo_url=self.base_url,
                    iggy_url=self.base_url,
                    secret=None,
                    allow_dev_secret=False,
                    dry_run=True,
                )
                self.assertFalse(success, "Should fail when secret is missing and allow_dev_secret=False")

    def test_14_load_env_secret_variations(self):
        """Tests load_env_secret handling of quoted values, inline comments, and masked placeholders."""
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / ".env"

            # 1. Masked secret (should be rejected)
            env_file.write_text("IGGY_HTTP_JWT_ENCODING_SECRET=********\n", encoding="utf-8")
            self.assertEqual(load_env_secret(env_file), "")

            # 2. Quoted secret with inline comments
            env_file.write_text('IGGY_HTTP_JWT_ENCODING_SECRET="my_secret_with_#_hash!" # comment\n', encoding="utf-8")
            self.assertEqual(load_env_secret(env_file), "my_secret_with_#_hash!")

            # 3. Unquoted secret
            env_file.write_text("IGGY_HTTP_JWT_ENCODING_SECRET=plain_secret_value\n", encoding="utf-8")
            self.assertEqual(load_env_secret(env_file), "plain_secret_value")

    def test_15_dual_consumer_group_offset_isolation_simulation(self):
        """Simulates and verifies that advancing cg-arroyo-raw offset on partition 0 does not alter cg-arroyo-state."""
        # Query initial offsets
        _, raw_o0 = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/cg-arroyo-raw/offsets/0", token=self.token)
        _, state_o0 = http_request(f"{self.base_url}/streams/1/topics/2/consumer-groups/cg-arroyo-state/offsets/0", token=self.token)
        self.assertEqual(raw_o0.get("offset"), 0)
        self.assertEqual(state_o0.get("offset"), 0)

        # Advance cg-arroyo-raw offset to 750
        s_put, _ = http_request(
            f"{self.base_url}/streams/1/topics/1/consumer-groups/cg-arroyo-raw/offsets/0",
            method="PUT",
            token=self.token,
            json_data={"offset": 750},
        )
        self.assertEqual(s_put, 200)

        # Confirm cg-arroyo-raw is 750 while cg-arroyo-state remains 0
        _, raw_o_after = http_request(f"{self.base_url}/streams/1/topics/1/consumer-groups/cg-arroyo-raw/offsets/0", token=self.token)
        _, state_o_after = http_request(f"{self.base_url}/streams/1/topics/2/consumer-groups/cg-arroyo-state/offsets/0", token=self.token)
        self.assertEqual(raw_o_after.get("offset"), 750)
        self.assertEqual(state_o_after.get("offset"), 0, "Offset leakage detected between consumer groups!")

    def test_16_strict_mode_rejects_missing_consumer_groups(self):
        """Verifies that strict provisioning fails if required consumer groups do not exist on broker."""
        MockCombinedServerHandler.consumer_groups = {}  # Clear groups
        with patch("sys.stdout", new_callable=io.StringIO):
            success = run_arroyo_provisioning(
                arroyo_url=self.base_url,
                iggy_url=self.base_url,
                secret=self.secret,
                strict=True,
                dry_run=False,
            )
            self.assertFalse(success, "Strict mode must fail when consumer groups are missing")

    def test_17_shell_wrapper_execution_smoke_test(self):
        """Executes PowerShell wrapper with --dry-run and verifies exit code 0."""
        ps1_script = REPO_ROOT / "scripts" / "provision-arroyo.ps1"
        res = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ps1_script), "--dry-run"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(res.returncode, 0, f"PowerShell wrapper failed: {res.stderr}")


if __name__ == "__main__":
    unittest.main()

