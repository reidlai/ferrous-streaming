#!/usr/bin/env python3
"""
Test Suite: Broker Administrative Security via Kanidm Service Account (Story 3.2)
Verifies:
1. Server TOML [http.jwt] configuration (algorithm, issuer, audience, Secret Zero compliance).
2. Masked environment blueprint (.env.example) and gitignore isolation.
3. Cryptographic JWT issuance, claim structure, tamper detection, and expiration handling.
4. HTTP security endpoint matrix:
   - Unauthenticated GET /ping -> 200 OK ("pong")
   - Missing token GET /metrics -> 401 Unauthorized ("access_token_missing", error 77)
   - Invalid token GET /metrics -> 401 Unauthorized ("invalid_access_token")
   - Valid Bearer token GET /metrics -> 200 OK with Prometheus metrics payload
5. CLI dry-run and live integration runners.
"""

import importlib.util
import json
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Use tomllib in Python 3.11+, fallback to tomli if needed
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

# Dynamically import verify-admin-auth.py
verify_script_path = SCRIPTS_DIR / "verify-admin-auth.py"
spec = importlib.util.spec_from_file_location("verify_admin_auth", verify_script_path)
verify_admin_auth = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_admin_auth)

b64url_encode = verify_admin_auth.b64url_encode
b64url_decode = verify_admin_auth.b64url_decode
create_hs256_jwt = verify_admin_auth.create_hs256_jwt
decode_and_verify_jwt = verify_admin_auth.decode_and_verify_jwt
generate_service_account_token = verify_admin_auth.generate_service_account_token
validate_jwt_config = verify_admin_auth.validate_jwt_config
validate_env_blueprint = verify_admin_auth.validate_env_blueprint
request_http = verify_admin_auth.request_http
run_live_verification = verify_admin_auth.run_live_verification


class MockIggyHttpHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler emulating Apache Iggy's authentication behavior."""

    def log_message(self, format, *args):
        # Suppress standard HTTP request logging during unit tests
        pass

    def do_GET(self):
        if self.path == "/ping":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Access-Control-Expose-Headers", "iggy-view")
            self.end_headers()
            self.wfile.write(b"pong")
            return

        if self.path == "/metrics":
            auth_header = self.headers.get("Authorization")
            if not auth_header:
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                err_resp = {
                    "id": 77,
                    "code": "access_token_missing",
                    "reason": "Access token is missing",
                    "field": None,
                }
                self.wfile.write(json.dumps(err_resp).encode("utf-8"))
                return

            if not auth_header.startswith("Bearer "):
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                err_resp = {
                    "id": 40,
                    "code": "unauthenticated",
                    "reason": "Unauthenticated",
                    "field": None,
                }
                self.wfile.write(json.dumps(err_resp).encode("utf-8"))
                return

            token = auth_header[7:].strip()
            # Validate token: must be valid format and not corrupted
            try:
                # Mock server verifies against test secret
                decode_and_verify_jwt(
                    token,
                    secret="mock_test_secret_32bytes_long_key!!",
                    expected_issuer="iggy.apache.org",
                    expected_audience="iggy.apache.org",
                )
            except Exception:
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                err_resp = {
                    "id": 40,
                    "code": "invalid_access_token",
                    "reason": "Invalid or expired access token",
                    "field": None,
                }
                self.wfile.write(json.dumps(err_resp).encode("utf-8"))
                return

            # Valid token: return Prometheus metrics format
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.end_headers()
            metrics_payload = (
                "# HELP http_requests total count of http_requests.\n"
                "# TYPE http_requests counter\n"
                "http_requests_total 42\n"
                "# HELP streams total count of streams.\n"
                "# TYPE streams gauge\n"
                "streams 2\n"
            )
            self.wfile.write(metrics_payload.encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


class TestBrokerSecurity(unittest.TestCase):
    """Automated security test suite for Story 3.2."""

    @classmethod
    def setUpClass(cls):
        # Start mock Iggy HTTP server on ephemeral port for deterministic offline unit testing
        cls.mock_server = HTTPServer(("127.0.0.1", 0), MockIggyHttpHandler)
        cls.mock_port = cls.mock_server.server_address[1]
        cls.mock_url = f"http://127.0.0.1:{cls.mock_port}"
        cls.server_thread = threading.Thread(target=cls.mock_server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.shutdown()
        cls.mock_server.server_close()

    def test_01_server_config_jwt_parameters(self):
        """Assertion 1: config/iggy.toml defines valid [http.jwt] section complying with Secret Zero."""
        config_file = REPO_ROOT / "config" / "iggy.toml"
        self.assertTrue(config_file.exists(), f"Missing config file: {config_file}")

        jwt_cfg = validate_jwt_config(config_file)
        self.assertEqual(jwt_cfg.get("algorithm"), "HS256")
        self.assertEqual(jwt_cfg.get("issuer"), "iggy.apache.org")
        self.assertEqual(jwt_cfg.get("audience"), "iggy.apache.org")
        self.assertIn("iggy.apache.org", jwt_cfg.get("valid_issuers", []))
        self.assertIn("iggy.apache.org", jwt_cfg.get("valid_audiences", []))
        self.assertEqual(jwt_cfg.get("access_token_expiry"), "1h")

        # DevSecOps Secret Zero: secrets must not be stored in iggy.toml
        self.assertEqual(jwt_cfg.get("encoding_secret"), "")
        self.assertEqual(jwt_cfg.get("decoding_secret"), "")

    def test_02_validate_jwt_config_error_handling(self):
        """Assertion 2: validate_jwt_config raises ValueError on invalid algorithms, issuers, or plaintext secrets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Case 1: Missing [http.jwt]
            bad1 = Path(tmpdir) / "no_jwt.toml"
            bad1.write_text('[http]\naddress = "0.0.0.0:3000"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                validate_jwt_config(bad1)
            self.assertIn("[http.jwt]", str(ctx.exception))

            # Case 2: Wrong algorithm
            bad2 = Path(tmpdir) / "bad_alg.toml"
            bad2.write_text(
                '[http.jwt]\nalgorithm = "RS256"\nissuer = "iggy.apache.org"\naudience = "iggy.apache.org"\nvalid_issuers = ["iggy.apache.org"]\nvalid_audiences = ["iggy.apache.org"]\n',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                validate_jwt_config(bad2)
            self.assertIn("algorithm", str(ctx.exception))

            # Case 3: Plaintext secret violation (Secret Zero)
            bad3 = Path(tmpdir) / "secret_zero_violation.toml"
            bad3.write_text(
                '[http.jwt]\nalgorithm = "HS256"\nissuer = "iggy.apache.org"\naudience = "iggy.apache.org"\nvalid_issuers = ["iggy.apache.org"]\nvalid_audiences = ["iggy.apache.org"]\nencoding_secret = "plaintext_secret_leak"\n',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                validate_jwt_config(bad3)
            self.assertIn("Secret Zero violation", str(ctx.exception))

    def test_03_secret_zero_env_blueprint(self):
        """Assertion 3: .env.example defines masked M2M credentials and .gitignore protects secrets."""
        env_example = REPO_ROOT / ".env.example"
        gitignore = REPO_ROOT / ".gitignore"

        self.assertTrue(env_example.exists(), f"Missing .env.example: {env_example}")
        self.assertTrue(gitignore.exists(), f"Missing .gitignore: {gitignore}")

        env_vars = validate_env_blueprint(env_example)
        self.assertEqual(env_vars.get("STREAMING_SERVICE_ACCOUNT"), "svc_ferrous_streaming")
        self.assertIn("********", env_vars.get("STREAMING_SERVICE_TOKEN"))
        jwt_secret_val = env_vars.get("IGGY_HTTP_JWT_ENCODING_SECRET") or env_vars.get("IGGY_HTTP_JWT_SECRET")
        self.assertIsNotNone(jwt_secret_val, "Must declare IGGY_HTTP_JWT_ENCODING_SECRET or IGGY_HTTP_JWT_SECRET")
        self.assertIn("********", jwt_secret_val)

        gi_content = gitignore.read_text(encoding="utf-8")
        self.assertIn(".env", gi_content, ".gitignore must ignore .env")

    def test_04_docker_compose_environment_injection(self):
        """Assertion 4: docker-compose.yml configures IGGY_HTTP_JWT_ENCODING_SECRET and non-root security standards."""
        compose_file = REPO_ROOT / "docker-compose.yml"
        self.assertTrue(compose_file.exists(), f"Missing compose file: {compose_file}")

        content = compose_file.read_text(encoding="utf-8")
        self.assertTrue(
            "IGGY_HTTP_JWT_ENCODING_SECRET" in content or "IGGY_HTTP_JWT_SECRET" in content,
            "Must declare IGGY_HTTP_JWT_ENCODING_SECRET or IGGY_HTTP_JWT_SECRET",
        )
        self.assertIn("STREAMING_SERVICE_ACCOUNT", content, "Must declare STREAMING_SERVICE_ACCOUNT")
        self.assertIn('user: "10001:10001"', content, "Must enforce non-root UID 10001")
        self.assertIn("cap_drop:", content, "Must drop capabilities")

    def test_05_jwt_generation_and_claims_verification(self):
        """Assertion 5: Cryptographic JWT generation signs token with HS256 and preserves claim structure."""
        secret = "unit_test_signing_secret_key_32bytes!"
        token = generate_service_account_token(
            service_account="svc_ferrous_streaming",
            secret=secret,
            expiry_seconds=1800,
        )

        parts = token.split(".")
        self.assertEqual(len(parts), 3, "JWT must consist of header, payload, signature")

        payload = decode_and_verify_jwt(
            token,
            secret=secret,
            expected_issuer="iggy.apache.org",
            expected_audience="iggy.apache.org",
        )
        self.assertEqual(payload.get("sub"), "svc_ferrous_streaming")
        self.assertEqual(payload.get("iss"), "iggy.apache.org")
        self.assertEqual(payload.get("aud"), "iggy.apache.org")
        self.assertIn("jti", payload)
        self.assertIn("iat", payload)
        self.assertIn("exp", payload)

    def test_06_jwt_tampering_and_expiration_rejection(self):
        """Assertion 6: decode_and_verify_jwt rejects tampered signatures, expired tokens, and wrong claims."""
        secret = "unit_test_signing_secret_key_32bytes!"
        token = generate_service_account_token(
            service_account="svc_ferrous_streaming",
            secret=secret,
            expiry_seconds=3600,
        )

        # 1. Tampered signature
        tampered_token = token[:-5] + "XXXXX"
        with self.assertRaises(ValueError) as ctx:
            decode_and_verify_jwt(tampered_token, secret=secret)
        self.assertIn("signature", str(ctx.exception).lower())

        # 2. Expired token
        expired_token = generate_service_account_token(
            service_account="svc_ferrous_streaming",
            secret=secret,
            expiry_seconds=-10,
        )
        with self.assertRaises(ValueError) as ctx:
            decode_and_verify_jwt(expired_token, secret=secret)
        self.assertIn("expired", str(ctx.exception).lower())

        # 3. Issuer mismatch
        with self.assertRaises(ValueError) as ctx:
            decode_and_verify_jwt(token, secret=secret, expected_issuer="rogue.issuer.org")
        self.assertIn("issuer mismatch", str(ctx.exception).lower())

        # 4. Audience mismatch
        with self.assertRaises(ValueError) as ctx:
            decode_and_verify_jwt(token, secret=secret, expected_audience="rogue.audience.org")
        self.assertIn("audience mismatch", str(ctx.exception).lower())

    def test_07_unauthenticated_ping_exempted(self):
        """Assertion 7: GET /ping succeeds with HTTP 200 OK and body 'pong' without credentials."""
        status, body, _ = request_http(f"{self.mock_url}/ping")
        self.assertEqual(status, 200)
        self.assertEqual(body.strip(), "pong")

    def test_08_missing_token_returns_401(self):
        """Assertion 8: GET /metrics without Authorization header returns HTTP 401 Unauthorized."""
        status, body, _ = request_http(f"{self.mock_url}/metrics")
        self.assertEqual(status, 401)
        resp_json = json.loads(body)
        self.assertEqual(resp_json.get("id"), 77)
        self.assertEqual(resp_json.get("code"), "access_token_missing")

    def test_09_invalid_token_returns_401(self):
        """Assertion 9: GET /metrics with tampered or invalid token returns HTTP 401 Unauthorized."""
        invalid_token = "eyJhbGciOiJIUzI1NiJ9.corruptedpayload.invalidsig"
        status, body, _ = request_http(f"{self.mock_url}/metrics", token=invalid_token)
        self.assertEqual(status, 401)
        resp_json = json.loads(body)
        self.assertIn(resp_json.get("code"), ["invalid_access_token", "unauthenticated"])

    def test_10_valid_bearer_token_returns_200_and_metrics(self):
        """Assertion 10: GET /metrics with valid Bearer token returns HTTP 200 OK and Prometheus metrics."""
        valid_token = generate_service_account_token(
            service_account="svc_ferrous_streaming",
            secret="mock_test_secret_32bytes_long_key!!",
            expiry_seconds=3600,
        )
        status, body, _ = request_http(f"{self.mock_url}/metrics", token=valid_token)
        self.assertEqual(status, 200)
        self.assertIn("http_requests_total", body)
        self.assertIn("# HELP", body)
        self.assertIn("# TYPE", body)

    def test_11_verify_admin_auth_dry_run_cli(self):
        """Assertion 11: verify-admin-auth.py --dry-run CLI succeeds with exit code 0."""
        cmd = [sys.executable, str(verify_script_path), "--dry-run"]
        res = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.assertEqual(res.returncode, 0, f"verify-admin-auth.py --dry-run failed:\n{res.stderr}")
        self.assertIn("[OK] iggy.toml defines valid [http.jwt] section", res.stdout)
        self.assertIn("[OK] STREAMING_SERVICE_ACCOUNT = svc_ferrous_streaming", res.stdout)
        self.assertIn("[OK] Generated valid HS256 JWT", res.stdout)
        self.assertIn("[SUCCESS] Dry-Run Security Verification Passed", res.stdout)

    def test_12_live_broker_verification_if_available(self):
        """Assertion 12: Live verification against running Apache Iggy broker if reachable on port 3000."""
        # Quick check if live broker is reachable on localhost:3000
        try:
            status, body, _ = request_http("http://localhost:3000/ping", timeout=1.0)
            if status != 200 or "pong" not in body:
                self.skipTest("Live broker on localhost:3000 did not respond to /ping")
        except Exception:
            self.skipTest("Live broker container not reachable on localhost:3000")

        # Run live verification suite
        success = run_live_verification(base_url="http://localhost:3000")
        self.assertTrue(success, "Live broker administrative security verification failed")

    def test_13_shell_wrappers_existence(self):
        """Assertion 13: Cross-platform shell wrappers exist and have non-empty content."""
        sh_script = SCRIPTS_DIR / "verify-admin-auth.sh"
        ps1_script = SCRIPTS_DIR / "verify-admin-auth.ps1"
        self.assertTrue(sh_script.exists(), f"Missing {sh_script}")
        self.assertTrue(ps1_script.exists(), f"Missing {ps1_script}")
        self.assertGreater(sh_script.stat().st_size, 50)
        self.assertGreater(ps1_script.stat().st_size, 50)

    def test_14_run_live_verification_against_mock_server(self):
        """Assertion 14: run_live_verification succeeds against active MockIggyHttpHandler."""
        valid_token = generate_service_account_token(
            service_account="svc_ferrous_streaming",
            secret="mock_test_secret_32bytes_long_key!!",
            expiry_seconds=3600,
        )
        success = run_live_verification(base_url=self.mock_url, token=valid_token)
        self.assertTrue(success, "run_live_verification failed against mock server")


if __name__ == "__main__":
    unittest.main()
