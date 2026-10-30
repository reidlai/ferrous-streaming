#!/usr/bin/env python3
"""
Test Suite: Apache Iggy Broker Container & Kafka Gateway Scaffolding (Story 3.1)
Verifies configuration validity, port allocations, container hygiene,
DevSecOps Secret Zero compliance, socket conflict detection, and lifecycle runners.
"""

import importlib.util
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

# Use tomllib in Python 3.11+, fallback to tomli if needed
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

# Dynamically import start-broker.py
start_broker_path = SCRIPTS_DIR / "start-broker.py"
spec = importlib.util.spec_from_file_location("start_broker", start_broker_path)
start_broker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(start_broker)

check_port_availability = start_broker.check_port_availability
check_and_create_storage = start_broker.check_and_create_storage
validate_config = start_broker.validate_config


class TestBrokerScaffolding(unittest.TestCase):
    """Scaffolding, security, and configuration verification tests."""

    def test_01_server_config_ports_and_storage(self):
        """Assertion 1: Server TOML config defines TCP (8090), HTTP (3000), Kafka (9092), and persistent WAL path."""
        config_file = REPO_ROOT / "config" / "iggy.toml"
        self.assertTrue(config_file.exists(), f"Missing config file: {config_file}")

        with open(config_file, "rb") as f:
            cfg = tomllib.load(f)

        # Validate storage path
        storage_path = cfg.get("path") or cfg.get("system", {}).get("path")
        self.assertEqual(storage_path, "/data/iggy", "Storage path must be /data/iggy")

        # Validate node advertised address
        node_cfg = cfg.get("node", {})
        self.assertEqual(
            node_cfg.get("advertised_address"),
            "localhost",
            "Node advertised address must be 'localhost'",
        )

        # Validate TCP binary port 8090
        tcp_cfg = cfg.get("tcp", {})
        self.assertIn("8090", tcp_cfg.get("address", ""), "TCP listener must bind port 8090")

        # Validate HTTP management port 3000 (resolving port collision with TCP 8090)
        http_cfg = cfg.get("http", {})
        self.assertIn("3000", http_cfg.get("address", ""), "HTTP listener must bind port 3000")

        # Validate Kafka protocol gateway port 9092 if declared in config
        kafka_cfg = cfg.get("kafka", {})
        if kafka_cfg:
            self.assertIn("9092", kafka_cfg.get("address", ""), "Kafka listener must bind port 9092")

    def test_02_docker_compose_scaffolding(self):
        """Assertion 2: Docker Compose defines iggy-broker service, port mappings, env_file, and persistent volume."""
        compose_file = REPO_ROOT / "docker-compose.yml"
        self.assertTrue(compose_file.exists(), f"Missing compose file: {compose_file}")

        content = compose_file.read_text(encoding="utf-8")

        # Verify service definition
        self.assertIn("iggy-broker:", content, "Must define 'iggy-broker' service")

        # Verify ports 8090 (TCP), 3000 (HTTP), and 9092 (Kafka)
        self.assertIn('"8090:8090"', content, "Must expose port binding 8090:8090")
        self.assertIn('"3000:3000"', content, "Must expose port binding 3000:3000")
        self.assertIn('"9092:9092"', content, "Must expose port binding 9092:9092")

        # Verify env_file declaration
        self.assertIn("env_file:", content, "Must declare env_file configuration")
        self.assertIn(".env", content, "Must reference .env in compose configuration")

        # Verify persistent volume mount
        self.assertIn("./data/iggy:/data/iggy", content, "Must mount ./data/iggy to /data/iggy")

        # Verify rootless execution & Tier 2 security standards
        self.assertIn('user: "10001:10001"', content, "Must enforce non-root UID 10001")
        self.assertIn("no-new-privileges:true", content, "Must enforce no-new-privileges")
        self.assertIn("cap_drop:", content, "Must drop unneeded capabilities")

        # Verify memory allocation limit
        self.assertTrue(
            "memory: 256M" in content or "memory: 50M" in content,
            "Must configure memory limit <= 256M",
        )

    def test_03_dockerfile_security_and_rootless(self):
        """Assertion 3: Dockerfile & Dockerfile.iggy enforce non-root UID 10001, upstream build, and pure Rust core."""
        dockerfiles = [REPO_ROOT / "Dockerfile", REPO_ROOT / "Dockerfile.iggy"]

        for df in dockerfiles:
            self.assertTrue(df.exists(), f"Missing Dockerfile: {df}")
            content = df.read_text(encoding="utf-8")

            # Check upstream multi-stage build stage
            self.assertIn("FROM apache/iggy:latest AS upstream", content, f"Dockerfile {df.name} must use upstream multi-stage")
            self.assertIn("COPY --from=upstream", content, f"Dockerfile {df.name} must copy binary from upstream")

            # Check rootless user creation and switch
            self.assertIn("10001", content, f"Dockerfile {df.name} must reference non-root UID 10001")
            self.assertIn("USER 10001:10001", content, f"Dockerfile {df.name} must run as USER 10001:10001")

            # Check ports and volume
            self.assertIn("EXPOSE 8090 3000 9092", content, f"Dockerfile {df.name} must expose ports 8090, 3000, 9092")
            self.assertIn('VOLUME ["/data/iggy"]', content, f"Dockerfile {df.name} must declare VOLUME /data/iggy")

            # Verify pure Rust core (zero JVM, zero Python dependencies inside broker image)
            instruction_lines = [
                line.strip().lower()
                for line in content.splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            instructions_text = " ".join(instruction_lines)
            self.assertNotIn("openjdk", instructions_text, f"Dockerfile {df.name} must not contain JVM dependencies")
            self.assertNotIn("python", instructions_text, f"Dockerfile {df.name} must not contain Python dependencies")
            self.assertNotIn("pip", instructions_text, f"Dockerfile {df.name} must not contain Python pip")

    def test_04_secret_zero_and_gitignore_hygiene(self):
        """Assertion 4: DevSecOps Secret Zero template and gitignore isolation for secrets and data."""
        env_example = REPO_ROOT / ".env.example"
        gitignore = REPO_ROOT / ".gitignore"

        self.assertTrue(env_example.exists(), f"Missing .env.example: {env_example}")
        self.assertTrue(gitignore.exists(), f"Missing .gitignore: {gitignore}")

        env_content = env_example.read_text(encoding="utf-8")
        # Ensure credentials use masked placeholders
        self.assertIn("STREAMING_SERVICE_TOKEN=********", env_content, "Must mask secret tokens in .env.example")
        self.assertIn("STREAMING_PORT=8090", env_content)
        self.assertIn("STREAMING_HTTP_PORT=3000", env_content)
        self.assertIn("STREAMING_KAFKA_PORT=9092", env_content)

        gi_content = gitignore.read_text(encoding="utf-8")
        # Ensure .env and persistent data directories are gitignored
        self.assertIn(".env", gi_content, ".gitignore must ignore .env")
        self.assertTrue(
            "data/" in gi_content or "./data/iggy/*" in gi_content,
            ".gitignore must ignore data/ directory",
        )

    def test_05_lifecycle_scripts_and_dry_run(self):
        """Assertion 5: Cross-platform lifecycle scripts exist and start-broker.py --dry-run succeeds."""
        start_py = REPO_ROOT / "scripts" / "start-broker.py"
        start_sh = REPO_ROOT / "scripts" / "start-broker.sh"
        start_ps1 = REPO_ROOT / "scripts" / "start-broker.ps1"

        self.assertTrue(start_py.exists(), f"Missing {start_py}")
        self.assertTrue(start_sh.exists(), f"Missing {start_sh}")
        self.assertTrue(start_ps1.exists(), f"Missing {start_ps1}")

        # Run start-broker.py --dry-run
        cmd = [sys.executable, str(start_py), "--dry-run"]
        res = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.assertEqual(res.returncode, 0, f"start-broker.py --dry-run failed: {res.stderr}")

        stdout = res.stdout
        self.assertIn("[OK] Port 8090 available", stdout, "Dry run output must confirm port 8090")
        self.assertIn("[OK] Port 3000 available", stdout, "Dry run output must confirm port 3000")
        self.assertIn("[OK] Port 9092 available", stdout, "Dry run output must confirm port 9092")
        self.assertIn("Configuration valid", stdout, "Dry run output must confirm configuration validity")
        self.assertIn("Persistent WAL storage directory verified", stdout, "Dry run output must confirm storage directory")

    def test_06_check_port_availability_occupied_socket(self):
        """Assertion 6: check_port_availability returns False when a port is occupied by an active listener."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except OSError:
                pass
        s.bind(("127.0.0.1", 0))  # Bind an ephemeral port
        port = s.getsockname()[1]
        s.listen(1)
        try:
            available, err = check_port_availability("127.0.0.1", port)
            self.assertFalse(available, f"Expected port {port} to be detected as occupied")
            self.assertIsNotNone(err)
            self.assertIn(f"Port {port} is already in use", err)
        finally:
            s.close()

    def test_07_check_and_create_storage_on_missing_dir(self):
        """Assertion 7: check_and_create_storage creates missing directories and returns appropriate status."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "data" / "iggy"
            self.assertFalse(target_dir.exists())

            # First invocation creates directory
            created = check_and_create_storage(target_dir)
            self.assertTrue(created)
            self.assertTrue(target_dir.exists())
            self.assertTrue(target_dir.is_dir())

            # Second invocation detects existing directory and does not recreate
            recreated = check_and_create_storage(target_dir)
            self.assertFalse(recreated)

    def test_08_validate_config_error_handling(self):
        """Assertion 8: validate_config raises ValueError when required fields are missing or invalid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Case 1: Missing storage path
            bad_cfg1 = Path(tmpdir) / "missing_path.toml"
            bad_cfg1.write_text('[node]\nadvertised_address = "localhost"\n[tcp]\naddress = "0.0.0.0:8090"\n[http]\naddress = "0.0.0.0:3000"\n[kafka]\naddress = "0.0.0.0:9092"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx1:
                validate_config(bad_cfg1)
            self.assertIn("path", str(ctx1.exception).lower())

            # Case 2: Missing advertised_address
            bad_cfg2 = Path(tmpdir) / "missing_node.toml"
            bad_cfg2.write_text('path = "/data/iggy"\n[tcp]\naddress = "0.0.0.0:8090"\n[http]\naddress = "0.0.0.0:3000"\n[kafka]\naddress = "0.0.0.0:9092"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx2:
                validate_config(bad_cfg2)
            self.assertIn("advertised_address", str(ctx2.exception).lower())

            # Case 3: Missing TCP 8090
            bad_cfg3 = Path(tmpdir) / "bad_tcp.toml"
            bad_cfg3.write_text('path = "/data/iggy"\n[node]\nadvertised_address = "localhost"\n[tcp]\naddress = "0.0.0.0:1234"\n[http]\naddress = "0.0.0.0:3000"\n[kafka]\naddress = "0.0.0.0:9092"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx3:
                validate_config(bad_cfg3)
            self.assertIn("8090", str(ctx3.exception))

            # Case 4: Missing HTTP 3000
            bad_cfg4 = Path(tmpdir) / "bad_http.toml"
            bad_cfg4.write_text('path = "/data/iggy"\n[node]\nadvertised_address = "localhost"\n[tcp]\naddress = "0.0.0.0:8090"\n[http]\naddress = "0.0.0.0:1234"\n[kafka]\naddress = "0.0.0.0:9092"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx4:
                validate_config(bad_cfg4)
            self.assertIn("3000", str(ctx4.exception))

            # Case 5: Missing Kafka 9092
            bad_cfg5 = Path(tmpdir) / "bad_kafka.toml"
            bad_cfg5.write_text('path = "/data/iggy"\n[node]\nadvertised_address = "localhost"\n[tcp]\naddress = "0.0.0.0:8090"\n[http]\naddress = "0.0.0.0:3000"\n[kafka]\naddress = "0.0.0.0:1234"\n', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx5:
                validate_config(bad_cfg5)
            self.assertIn("9092", str(ctx5.exception))

    def test_09_dry_run_fails_on_port_conflict(self):
        """Assertion 9: start-broker.py --dry-run returns exit code 1 when port 8090 is in conflict."""
        start_py = REPO_ROOT / "scripts" / "start-broker.py"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except OSError:
                pass
        s.bind(("127.0.0.1", 8090))
        s.listen(1)
        try:
            cmd = [sys.executable, str(start_py), "--dry-run"]
            res = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            self.assertEqual(res.returncode, 1, "Dry run must exit with code 1 on port conflict")
            self.assertIn("Port binding conflict", res.stderr + res.stdout)
        finally:
            s.close()


if __name__ == "__main__":
    unittest.main()
