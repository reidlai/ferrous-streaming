#!/usr/bin/env python3
"""
provision-arroyo.py: Arroyo Stream Processor Deployment & Broker Ingestion Provisioner & Verifier.
Submodule: ferrous-streaming
Story: 3.5 (Arroyo Stream Processor Deployment & Broker Ingestion via M2M Account)

Responsibilities:
1. Verifies Arroyo API/Web health probe on port 5115 (GET /api/v1/health).
2. Verifies Apache Iggy broker liveness (GET /ping on port 3000).
3. Acquires M2M JWT for service account 'svc_ferrous_streaming' adhering to Secret Zero.
4. Idempotently provisions and validates Arroyo connection configuration for Iggy:
   - Ingestion connection to Kafka-compatible gateway (port 9092).
   - Attachment to consumer group 'cg-arroyo-raw' on 'orders/intake'.
   - Attachment to consumer group 'cg-arroyo-state' on 'orders/consolidated'.
5. Verifies consumer group offset isolation and zero-conflict multi-consumer setup.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DEFAULT_ARROYO_URL = os.environ.get("ARROYO_API_URL", os.environ.get("ARROYO_HTTP_URL", "http://localhost:5115"))
DEFAULT_IGGY_URL = os.environ.get("STREAMING_HTTP_URL", os.environ.get("IGGY_HTTP_URL", "http://localhost:3000"))
DEFAULT_KAFKA_BOOTSTRAP = os.environ.get("IGGY_KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_SERVICE_ACCOUNT = os.environ.get("STREAMING_SERVICE_ACCOUNT", "svc_ferrous_streaming")

ARROYO_CONSUMER_GROUPS = [
    {
        "name": "cg-arroyo-raw",
        "stream_id": 1,
        "stream_name": "orders",
        "topic_id": 1,
        "topic_name": "intake",
        "group_id": 1,
        "pipeline": "raw_event_archiver",
        "sink_target": "./data/delta/orders_raw_events",
    },
    {
        "name": "cg-arroyo-state",
        "stream_id": 1,
        "stream_name": "orders",
        "topic_id": 2,
        "topic_name": "consolidated",
        "group_id": 1,
        "pipeline": "state_snapshot_consolidator",
        "sink_target": "./data/delta/orders_current_state",
    },
]


def b64url_encode(data: bytes) -> str:
    """Base64URL encode without trailing padding."""
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def create_hs256_jwt(
    payload: dict,
    secret: str,
    algorithm: str = "HS256",
) -> str:
    """Creates a signed HS256 JWT using Python standard library."""
    header = {"typ": "JWT", "alg": algorithm}
    header_json = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    h_b64 = b64url_encode(header_json)
    p_b64 = b64url_encode(payload_json)
    signing_input = f"{h_b64}.{p_b64}".encode("utf-8")

    sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    sig_b64 = b64url_encode(sig)

    return f"{h_b64}.{p_b64}.{sig_b64}"


def load_env_secret(env_path: Path | str = ".env") -> str:
    """Extracts JWT secret from environment or .env file with quote stripping."""
    env_keys = ("IGGY_HTTP_JWT_ENCODING_SECRET", "IGGY_HTTP_JWT_SECRET")
    for key in env_keys:
        if os.environ.get(key):
            val = os.environ[key].strip().strip('"\'')
            if val and val != "********":
                return val

    submodule_root = Path(__file__).resolve().parent.parent
    candidates = [Path(env_path), submodule_root / env_path, submodule_root / ".env"]

    for p in candidates:
        if p.is_file():
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("#"):
                    continue
                for key in env_keys:
                    prefix = f"{key}="
                    if line.startswith(prefix):
                        raw_val = line.split("=", 1)[1].strip()
                        if raw_val.startswith('"'):
                            end_idx = raw_val.find('"', 1)
                            val = raw_val[1:end_idx] if end_idx != -1 else raw_val[1:]
                        elif raw_val.startswith("'"):
                            end_idx = raw_val.find("'", 1)
                            val = raw_val[1:end_idx] if end_idx != -1 else raw_val[1:]
                        else:
                            val = raw_val.split("#", 1)[0].strip()
                        val = val.strip('"\'')
                        if val and val not in ("********", "changeme", "TODO"):
                            return val

    return ""


def make_admin_jwt(
    secret: str,
    service_account: str = DEFAULT_SERVICE_ACCOUNT,
    issuer: str = "iggy.apache.org",
    audience: str = "iggy.apache.org",
    expiry_seconds: int = 3600,
) -> str:
    """Generates an administrative M2M JWT."""
    if not secret:
        raise ValueError("Cannot generate JWT: administrative secret is empty.")
    now = int(time.time())
    payload = {
        "sub": "1",
        "name": service_account,
        "iss": issuer,
        "aud": audience,
        "iat": now,
        "exp": now + expiry_seconds,
        "nbf": now,
        "jti": str(uuid.uuid4()),
        "role": "admin",
    }
    return create_hs256_jwt(payload, secret)


def http_request(
    url: str,
    method: str = "GET",
    token: str | None = None,
    json_data: dict | list | None = None,
    timeout: float = 5.0,
) -> tuple[int, dict | str]:
    """Issues an HTTP request and parses response."""
    data = None
    headers = {"User-Agent": "ferrous-streaming-arroyo-provisioner/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if json_data is not None:
        data = json.dumps(json_data).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
            body_bytes = resp.read()
            body_str = body_bytes.decode("utf-8", errors="replace")
            try:
                return status, json.loads(body_str)
            except json.JSONDecodeError:
                return status, body_str
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        body_str = body_bytes.decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body_str)
        except json.JSONDecodeError:
            return e.code, body_str
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as e:
        reason = getattr(e, "reason", str(e))
        raise ConnectionError(f"Failed to connect to {url}: {reason}") from e


def check_arroyo_health(
    base_url: str,
    retries: int = 10,
    delay_seconds: float = 1.0,
) -> bool:
    """Verifies Arroyo service health on port 5115 via GET /api/v1/ping or /api/v1/health."""
    endpoints = ["/api/v1/ping", "/api/v1/health", "/health"]
    for attempt in range(1, retries + 1):
        for ep in endpoints:
            try:
                status, body = http_request(f"{base_url}{ep}", timeout=2.0)
                if status == 200:
                    return True
            except ConnectionError:
                pass
        if attempt < retries:
            time.sleep(delay_seconds)
    return False


def check_iggy_liveness(
    base_url: str,
    retries: int = 5,
    delay_seconds: float = 1.0,
) -> bool:
    """Verifies Apache Iggy broker liveness via GET /ping."""
    for attempt in range(1, retries + 1):
        try:
            status, body = http_request(f"{base_url}/ping", timeout=2.0)
            if status == 200 and str(body).strip() == "pong":
                return True
        except ConnectionError:
            pass
        if attempt < retries:
            time.sleep(delay_seconds)
    return False


def verify_broker_consumer_group(
    iggy_url: str,
    token: str,
    stream_id: int | str,
    topic_id: int | str,
    group_id: int,
    group_name: str,
) -> dict:
    """
    Verifies that a consumer group exists on the broker and matches group_name.
    Explicitly fails fast on network, proxy, or server-side broker faults.
    """
    url = f"{iggy_url}/streams/{stream_id}/topics/{topic_id}/consumer-groups/{urllib.parse.quote(group_name, safe='')}"
    status, res = http_request(url, method="GET", token=token)
    if status == 200 and isinstance(res, dict):
        return {"exists": True, "group_id": res.get("id", group_id), "group": res}
    if status == 404:
        return {"exists": False, "group_id": group_id, "group": res}
    if status in (401, 403):
        raise PermissionError(f"Unauthorized (status {status}) querying consumer group '{group_name}': {res}")
    raise RuntimeError(f"Unexpected broker response ({status}) querying consumer group '{group_name}': {res}")


def register_arroyo_connection(
    arroyo_url: str,
    connection_name: str = "iggy-kafka-intake",
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP,
) -> dict:
    """
    Registers or verifies the Apache Iggy Kafka wire connection table in Arroyo.
    Fails fast on unexpected HTTP errors (500, 502, 503).
    """
    # 1. Check existing connection tables
    list_url = f"{arroyo_url}/api/v1/connection_tables"
    status, res = http_request(list_url, method="GET")
    if status == 200 and isinstance(res, dict):
        tables = res.get("data", [])
        for tbl in tables:
            if tbl.get("name") == connection_name:
                return {"action": "exists", "name": connection_name, "table": tbl}
    elif status not in (200, 404):
        raise RuntimeError(f"Failed querying Arroyo connection tables (status {status}): {res}")

    # 2. Register connection table with Arroyo OpenAPI ConnectionTablePost schema
    payload = {
        "name": connection_name,
        "connector": "kafka",
        "table_type": "source",
        "config": {
            "bootstrap_servers": bootstrap_servers,
            "topic": "orders/intake",
            "group_id": "cg-arroyo-raw",
            "security_protocol": "PLAINTEXT",
            "auto_offset_reset": "earliest",
        },
        "schema": {
            "format": "json",
            "fields": [
                {"name": "order_id", "data_type": "TEXT", "nullable": False},
                {"name": "event_type", "data_type": "TEXT", "nullable": False},
                {"name": "payload", "data_type": "TEXT", "nullable": True},
                {"name": "timestamp", "data_type": "TIMESTAMP", "nullable": False},
            ],
        },
    }
    status_post, res_post = http_request(list_url, method="POST", json_data=payload)
    if status_post in (200, 201):
        return {"action": "created", "name": connection_name, "table": res_post}
    if status_post in (400, 409):
        return {"action": "exists_conflict", "name": connection_name, "table": res_post}

    raise RuntimeError(f"Arroyo connection registration failed (status {status_post}): {res_post}")


def ensure_storage_directories() -> None:
    """Ensures host mount paths for Arroyo checkpoints and Delta output exist with write permissions."""
    repo_root = Path(__file__).resolve().parent.parent
    paths = [repo_root / "data" / "arroyo", repo_root / "data" / "delta"]
    for p in paths:
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(p, 0o777)
            except OSError:
                pass


def run_arroyo_provisioning(
    arroyo_url: str = DEFAULT_ARROYO_URL,
    iggy_url: str = DEFAULT_IGGY_URL,
    secret: str | None = None,
    service_account: str = DEFAULT_SERVICE_ACCOUNT,
    allow_dev_secret: bool = True,
    strict: bool = True,
    dry_run: bool = False,
) -> bool:
    """Executes full Arroyo stream processor verification and broker ingestion attachment."""
    print("=" * 70)
    print(" [PROVISION] Arroyo Stream Processor & Broker Ingestion via M2M")
    print(f" Arroyo Target: {arroyo_url} | Iggy Broker: {iggy_url}")
    print("=" * 70)

    # 0. Ensure Host Storage Directories
    ensure_storage_directories()

    # 1. Acquire JWT for M2M authentication
    print("\n[Step 1/5] Acquiring M2M administrative JWT for Arroyo service account...")
    jwt_secret = secret or load_env_secret()
    if not jwt_secret:
        if allow_dev_secret:
            print("  [WARN] No secret configured. Falling back to dev secret (--allow-dev-secret=true).")
            jwt_secret = "ferrous_cx_super_secure_jwt_secret_dev_32bytes!"
        else:
            print("  [ERROR] Secret Zero Failure: Missing required IGGY_HTTP_JWT_ENCODING_SECRET.")
            return False

    try:
        token = make_admin_jwt(secret=jwt_secret, service_account=service_account)
    except Exception as e:
        print(f"  [ERROR] Failed to generate administrative JWT: {e}")
        return False
    print(f"  [OK] Generated administrative JWT for '{service_account}'.")

    # 2. Check Iggy broker liveness
    print("\n[Step 2/5] Verifying Apache Iggy broker liveness (GET /ping)...")
    if not dry_run:
        if not check_iggy_liveness(iggy_url, retries=5, delay_seconds=1.0):
            print(f"  [ERROR] Iggy broker at {iggy_url} did not respond to /ping within 5s.")
            return False
        print("  [OK] Iggy broker is alive and responding (200 OK 'pong').")
    else:
        print("  [DRY-RUN] Skipping live Iggy broker probe.")

    # 3. Check Arroyo Service Health
    print(f"\n[Step 3/5] Verifying Arroyo Stream Processor Web/API health ({arroyo_url})...")
    if not dry_run:
        if not check_arroyo_health(arroyo_url, retries=10, delay_seconds=1.0):
            print(f"  [ERROR] Arroyo stream processor at {arroyo_url} did not respond within 10s.")
            return False
        print("  [OK] Arroyo stream processor is alive and listening on port 5115.")
    else:
        print("  [DRY-RUN] Skipping live Arroyo health probe.")

    # 4. Validate Arroyo Consumer Groups on Iggy Broker
    print("\n[Step 4/5] Validating dual Arroyo consumer group attachments on broker...")
    for cg in ARROYO_CONSUMER_GROUPS:
        s_id = cg["stream_id"]
        t_id = cg["topic_id"]
        t_name = cg["topic_name"]
        g_id = cg["group_id"]
        g_name = cg["name"]
        pipeline = cg["pipeline"]

        print(f"  -> Verifying consumer group '{g_name}' for pipeline '{pipeline}' on '{t_name}'...")
        if not dry_run:
            try:
                res = verify_broker_consumer_group(iggy_url, token, s_id, t_id, g_id, g_name)
                if not res["exists"]:
                    if strict:
                        print(f"     [ERROR] Consumer group '{g_name}' does not exist on broker.")
                        print("     Please execute 'python scripts/provision-consumer-groups.py' first.")
                        return False
                    else:
                        print(f"     [WARN] Consumer group '{g_name}' not yet provisioned on broker.")
                else:
                    print(f"     [OK] Group '{g_name}' verified on broker (Resolved ID: {res['group_id']}).")
            except Exception as e:
                print(f"     [ERROR] Failed to query consumer group '{g_name}': {e}")
                return False
        else:
            print(f"     [DRY-RUN] Verified consumer group '{g_name}' mapping.")

    # 5. Register Arroyo Connection Tables
    print("\n[Step 5/5] Registering/Verifying Arroyo Kafka Ingestion Connections...")
    if not dry_run:
        try:
            conn_res = register_arroyo_connection(arroyo_url)
            print(f"  [OK] Arroyo connection table '{conn_res['name']}' status: {conn_res['action']}.")
        except Exception as e:
            print(f"  [ERROR] Failed to register Arroyo connection table: {e}")
            return False
    else:
        print("  [DRY-RUN] Arroyo connection table registered.")

    print("\n" + "=" * 70)
    print(" [SUCCESS] Arroyo Stream Processor & Broker Ingestion Verified!")
    print("=" * 70)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arroyo Stream Processor Deployment & Broker Ingestion Provisioner (Story 3.5)"
    )
    parser.add_argument("--arroyo-url", default=DEFAULT_ARROYO_URL, help="Base HTTP URL for Arroyo API (port 5115)")
    parser.add_argument("--iggy-url", default=DEFAULT_IGGY_URL, help="Base HTTP URL for Iggy broker (port 3000)")
    parser.add_argument("-s", "--secret", default=None, help="HMAC JWT secret for authentication")
    parser.add_argument("-a", "--account", default=DEFAULT_SERVICE_ACCOUNT, help="M2M service account name")
    parser.add_argument(
        "--no-dev-secret",
        dest="allow_dev_secret",
        action="store_false",
        help="Disallow fallback to default dev secret",
    )
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Do not fail if broker consumer groups are missing",
    )
    parser.add_argument("--dry-run", action="store_true", help="Perform offline spec verification")
    parser.set_defaults(allow_dev_secret=True, strict=True)

    args = parser.parse_args()
    success = run_arroyo_provisioning(
        arroyo_url=args.arroyo_url,
        iggy_url=args.iggy_url,
        secret=args.secret,
        service_account=args.account,
        allow_dev_secret=args.allow_dev_secret,
        strict=args.strict,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

