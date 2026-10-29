#!/usr/bin/env python3
"""
Apache Iggy Broker Lifecycle, Pre-Flight Check, and Startup Runner.
Submodule: ferrous-streaming
Validates storage directories, parses configuration, detects port conflicts,
and manages broker container lifecycle.
"""

import argparse
import os
import socket
import subprocess
import sys
from pathlib import Path

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Use tomllib in Python 3.11+, fallback to basic parser if needed
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

DEFAULT_CONFIG_PATH = "config/iggy.toml"
DEFAULT_DATA_DIR = "data/iggy"
DEFAULT_ENV_FILE = ".env"
DEFAULT_ENV_EXAMPLE = ".env.example"


def check_and_create_storage(data_dir_path: Path) -> bool:
    """Verifies that the WAL storage directory exists, creating it if missing."""
    if not data_dir_path.exists():
        print(f"[WARN] Storage directory '{data_dir_path}' does not exist on host. Auto-creating directory (0755)...")
        data_dir_path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(data_dir_path, 0o755)
        except OSError:
            pass
        return True
    return False


def validate_config(config_path: Path) -> dict:
    """Parses and validates Apache Iggy TOML configuration."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)

    # Validate storage path
    storage_path = cfg.get("path") or cfg.get("system", {}).get("path")
    if not storage_path:
        raise ValueError("Invalid iggy.toml: Missing storage 'path' definition.")

    # Validate node configuration
    node_cfg = cfg.get("node", {})
    advertised_address = node_cfg.get("advertised_address")
    if not advertised_address:
        raise ValueError("Invalid iggy.toml: Missing '[node].advertised_address'.")

    # Validate TCP port 8090
    tcp_addr = cfg.get("tcp", {}).get("address", "")
    if "8090" not in tcp_addr:
        raise ValueError(f"Invalid iggy.toml: Expected port 8090 for TCP binary protocol, got tcp='{tcp_addr}'.")

    # Validate HTTP port 3000
    http_addr = cfg.get("http", {}).get("address", "")
    if "3000" not in http_addr:
        raise ValueError(f"Invalid iggy.toml: Expected port 3000 for HTTP management API, got http='{http_addr}'.")

    # Validate Kafka port 9092
    kafka_addr = cfg.get("kafka", {}).get("address", "")
    if "9092" not in kafka_addr:
        raise ValueError(f"Invalid iggy.toml: Expected port 9092 for Kafka gateway, got '{kafka_addr}'.")

    return cfg


def check_env(env_path: Path, env_example_path: Path) -> None:
    """Checks for .env file and warns if missing, pointing to .env.example."""
    if not env_path.exists():
        print(
            f"[WARN] Missing environment secrets file '{env_path.name}'. "
            f"Falling back to safe defaults in 'config/iggy.toml'. "
            f"Please copy '{env_example_path.name}' to '{env_path.name}' to configure local credentials."
        )


def check_port_availability(host: str, port: int) -> tuple[bool, str | None]:
    """
    Checks if a TCP port can be bound locally.
    On Windows (win32), avoids SO_REUSEADDR and uses SO_EXCLUSIVEADDRUSE if available
    so Winsock detects occupied ports without masking conflicts.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if sys.platform == "win32":
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except OSError:
                pass
    else:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((host, port))
        s.close()
        return True, None
    except OSError as e:
        s.close()
        return False, f"Port binding conflict: Port {port} is already in use by an external process ({e})."


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apache Iggy Broker Lifecycle, Pre-Flight Check, and Startup Runner"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pre-flight validation checks without launching container",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to iggy.toml configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Path to WAL persistent storage directory (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"Path to environment file (default: {DEFAULT_ENV_FILE})",
    )

    args = parser.parse_args()

    # Determine repo root relative to this script
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    config_path = (repo_root / args.config).resolve() if not Path(args.config).is_absolute() else Path(args.config)
    data_dir_path = (repo_root / args.data_dir).resolve() if not Path(args.data_dir).is_absolute() else Path(args.data_dir)
    env_path = (repo_root / args.env_file).resolve() if not Path(args.env_file).is_absolute() else Path(args.env_file)
    env_example_path = repo_root / DEFAULT_ENV_EXAMPLE

    print("[PRE-FLIGHT] Checking Apache Iggy Broker Scaffolding...")

    # 1. Check & create storage directory
    check_and_create_storage(data_dir_path)
    print(f"[OK] Persistent WAL storage directory verified: {data_dir_path}")

    # 2. Parse & validate configuration
    try:
        cfg = validate_config(config_path)
        print(
            f"[OK] Configuration valid: {config_path.name} "
            f"(TCP: 8090, HTTP: 3000, Kafka: 9092, Path: {cfg.get('path')}, Advertised: {cfg.get('node', {}).get('advertised_address')})"
        )
    except Exception as e:
        print(f"[ERROR] Configuration validation failed: {e}", file=sys.stderr)
        return 1

    # 3. Check environment file
    check_env(env_path, env_example_path)

    # 4. Check port availability for 8090, 3000, and 9092
    has_conflict = False
    ports_to_check = [
        (8090, "native TCP transport"),
        (3000, "HTTP management and metrics API"),
        (9092, "Kafka protocol wire gateway"),
    ]
    for port, desc in ports_to_check:
        ok, err = check_port_availability("127.0.0.1", port)
        if not ok:
            print(f"[ERROR] {err}", file=sys.stderr)
            has_conflict = True
        else:
            print(f"[OK] Port {port} available for {desc}")

    if has_conflict:
        print("[ERROR] Port binding conflicts detected during pre-flight checks.", file=sys.stderr)
        return 1

    if args.dry_run:
        print(
            "[DRY-RUN] Pre-flight validation successful. "
            "Ports 8090, 3000, and 9092, configuration validity, and directory structure confirmed."
        )
        return 0

    # 5. Launch container via Docker Compose
    compose_file = repo_root / "docker-compose.yml"
    if not compose_file.exists():
        print(f"[ERROR] Docker Compose manifest not found: {compose_file}", file=sys.stderr)
        return 1

    print("[STARTUP] Launching Apache Iggy Broker via Docker Compose...")
    try:
        cmd = ["docker", "compose", "-f", str(compose_file)]
        if args.env_file and env_path.exists():
            cmd.extend(["--env-file", str(env_path)])
        cmd.extend(["up", "-d"])
        res = subprocess.run(cmd, cwd=str(repo_root), check=True)
        print("[SUCCESS] Apache Iggy broker container started successfully.")
        return res.returncode
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to start broker container: {e}", file=sys.stderr)
        return e.returncode
    except FileNotFoundError:
        print("[ERROR] Docker executable not found in PATH.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
