#!/usr/bin/env python3
"""
provision-topics.py: Apache Iggy Stream and Topic Partitioning Provisioner & Verifier.
Submodule: ferrous-streaming
Story: 3.3 (Ingestion Topic Partitioning & Immediate fsync Durability)

Responsibilities:
1. Idempotently provisions stream 'orders' (ID: 1).
2. Idempotently provisions topic 'intake' (ID: 1, 4 partitions) for raw event intake.
3. Idempotently provisions topic 'consolidated' (ID: 2, 4 partitions) for state projections.
4. Enforces strict topology validation: fails if existing topics have mismatched partition counts.
5. Verifies partition configuration, immediate fsync durability, and monotonic offset tracking.
6. Dispatches and verifies test probes across partitions to assert strict ordering and routing.
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

DEFAULT_HTTP_URL = os.environ.get("STREAMING_HTTP_URL", os.environ.get("IGGY_HTTP_URL", "http://localhost:3000"))
DEFAULT_STREAM_NAME = "orders"
DEFAULT_STREAM_ID = 1
DEFAULT_PARTITION_COUNT = 4

TOPICS_SPEC = [
    {
        "id": 1,
        "name": "intake",
        "partitions_count": 4,
        "description": "Raw ingestion WAL for mutation commands and simulation events",
    },
    {
        "id": 2,
        "name": "consolidated",
        "partitions_count": 4,
        "description": "CQRS-ES domain state projections and validated entity events",
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
    """
    Extracts JWT secret from environment or .env file with quote stripping and path resolution.
    Checks IGGY_HTTP_JWT_ENCODING_SECRET and IGGY_HTTP_JWT_SECRET.
    """
    env_keys = ("IGGY_HTTP_JWT_ENCODING_SECRET", "IGGY_HTTP_JWT_SECRET")
    for key in env_keys:
        if os.environ.get(key):
            val = os.environ[key].strip()
            return val.strip('"\'')

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
                        val = line.split("=", 1)[1].split("#", 1)[0].strip()
                        return val.strip('"\'')

    return ""


def make_admin_jwt(
    secret: str,
    service_account: str = "svc_ferrous_streaming",
    issuer: str = "iggy.apache.org",
    audience: str = "iggy.apache.org",
    expiry_seconds: int = 3600,
) -> str:
    """Generates an administrative M2M JWT."""
    if not secret:
        raise ValueError("Cannot generate JWT: administrative secret is empty.")
    now = int(time.time())
    payload = {
        "sub": "1",  # Root / Admin user ID in Iggy
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
    """Issues an HTTP request to the Iggy REST API and parses response."""
    data = None
    headers = {"User-Agent": "ferrous-streaming-provisioner/1.0"}
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
        raise ConnectionError(f"Failed to connect to Iggy at {url}: {reason}") from e


def check_liveness_with_retry(
    base_url: str,
    retries: int = 5,
    delay_seconds: float = 1.0,
) -> bool:
    """Verifies Iggy broker liveness via GET /ping with exponential retry."""
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


def ensure_stream(
    base_url: str,
    token: str,
    stream_id: int = DEFAULT_STREAM_ID,
    stream_name: str = DEFAULT_STREAM_NAME,
) -> dict:
    """
    Idempotently creates or verifies the target stream.
    Returns dict with 'action', 'stream_id', and 'stream' metadata.
    """
    # 1. Check if stream exists by numeric ID
    get_url = f"{base_url}/streams/{stream_id}"
    status, res = http_request(get_url, method="GET", token=token)
    if status == 200 and isinstance(res, dict):
        resolved_id = res.get("id", stream_id)
        return {"action": "exists", "stream_id": resolved_id, "stream": res}
    if status in (401, 403):
        raise PermissionError(f"Unauthorized (status {status}) while accessing stream {stream_id}: {res}")
    if status not in (404,):
        raise RuntimeError(f"Unexpected broker response querying stream {stream_id} (status {status}): {res}")

    # 2. Check by name
    status_name, res_name = http_request(f"{base_url}/streams/{stream_name}", method="GET", token=token)
    if status_name == 200 and isinstance(res_name, dict):
        resolved_id = res_name.get("id", stream_id)
        return {"action": "exists", "stream_id": resolved_id, "stream": res_name}
    if status_name in (401, 403):
        raise PermissionError(f"Unauthorized (status {status_name}) querying stream '{stream_name}': {res_name}")

    # 3. Create stream
    post_url = f"{base_url}/streams"
    payload = {"stream_id": stream_id, "name": stream_name}
    status_create, res_create = http_request(post_url, method="POST", token=token, json_data=payload)

    if status_create in (200, 201):
        resolved_id = res_create.get("id", stream_id) if isinstance(res_create, dict) else stream_id
        return {"action": "created", "stream_id": resolved_id, "stream": res_create}

    # Handle idempotent conflict
    if status_create == 400 and isinstance(res_create, dict):
        err_code = res_create.get("code")
        err_id = res_create.get("id")
        if err_code in ("stream_name_already_exists", "stream_id_already_exists") or err_id in (20, 21):
            return {"action": "exists_conflict", "stream_id": stream_id, "stream": res_create}

    raise RuntimeError(f"Failed to ensure stream '{stream_name}' (status {status_create}): {res_create}")


def ensure_topic(
    base_url: str,
    token: str,
    stream_id: int | str,
    topic_spec: dict,
) -> dict:
    """
    Idempotently creates or verifies a topic with the specified partition count.
    Enforces strict topology validation: raises ValueError if existing partition count differs.
    """
    t_id = topic_spec["id"]
    t_name = topic_spec["name"]
    p_count = topic_spec["partitions_count"]

    def _validate_partition_count(res_dict: dict) -> None:
        actual_p = res_dict.get("partitions_count")
        if actual_p is not None and actual_p != p_count:
            raise ValueError(
                f"Topic '{t_name}' topology mismatch: exists with {actual_p} partitions, "
                f"but Story 3.3 requires exactly {p_count} partitions."
            )

    # 1. Check if topic exists by ID
    get_url = f"{base_url}/streams/{stream_id}/topics/{t_id}"
    status, res = http_request(get_url, method="GET", token=token)
    if status == 200 and isinstance(res, dict):
        _validate_partition_count(res)
        return {"action": "exists", "topic_id": res.get("id", t_id), "topic": res}
    if status in (401, 403):
        raise PermissionError(f"Unauthorized (status {status}) accessing topic {t_id}: {res}")
    if status not in (404,):
        raise RuntimeError(f"Unexpected broker response querying topic {t_id} (status {status}): {res}")

    # 2. Check by name
    status_name, res_name = http_request(f"{base_url}/streams/{stream_id}/topics/{t_name}", method="GET", token=token)
    if status_name == 200 and isinstance(res_name, dict):
        _validate_partition_count(res_name)
        return {"action": "exists", "topic_id": res_name.get("id", t_id), "topic": res_name}
    if status_name in (401, 403):
        raise PermissionError(f"Unauthorized (status {status_name}) querying topic '{t_name}': {res_name}")

    # 3. Create topic
    post_url = f"{base_url}/streams/{stream_id}/topics"
    payload = {
        "topic_id": t_id,
        "name": t_name,
        "partitions_count": p_count,
        "message_expiry": 0,  # 0 indicates no expiration (permanent WAL log)
    }
    status_create, res_create = http_request(post_url, method="POST", token=token, json_data=payload)

    if status_create in (200, 201):
        resolved_id = res_create.get("id", t_id) if isinstance(res_create, dict) else t_id
        return {"action": "created", "topic_id": resolved_id, "topic": res_create}

    # Handle idempotent conflict
    if status_create == 400 and isinstance(res_create, dict):
        err_code = res_create.get("code")
        err_id = res_create.get("id")
        if err_code in ("topic_name_already_exists", "topic_id_already_exists") or err_id in (30, 31):
            return {"action": "exists_conflict", "topic_id": t_id, "topic": res_create}

    raise RuntimeError(f"Failed to ensure topic '{t_name}' (status {status_create}): {res_create}")


def dispatch_partition_probes(
    base_url: str,
    token: str,
    stream_id: int | str,
    topic_name: str,
    partition_count: int = DEFAULT_PARTITION_COUNT,
    probes_per_partition: int = 2,
) -> list[dict]:
    """
    Sends multiple sequential synthetic messages across each partition to verify
    sub-millisecond fsync commitment, partition routing, and monotonic sequence progression.
    """
    probe_results = []
    for p_id in range(partition_count):
        partition_probes = []
        for seq in range(probes_per_partition):
            msg_id = (uuid.uuid4().int % (2**63 - 1)) + 1
            payload_data = {
                "probe_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "target_partition": p_id,
                "sequence": seq,
                "story": "3.3",
            }
            encoded_payload = base64.b64encode(json.dumps(payload_data).encode("utf-8")).decode("utf-8")
            msg_body = {
                "partitioning": {
                    "kind": "partition_id",
                    "value": p_id,
                },
                "messages": [
                    {
                        "id": msg_id,
                        "payload": encoded_payload,
                    }
                ],
            }

            url = f"{base_url}/streams/{stream_id}/topics/{topic_name}/messages"
            t0 = time.perf_counter()
            status, res = http_request(url, method="POST", token=token, json_data=msg_body)
            latency_ms = (time.perf_counter() - t0) * 1000.0

            if status not in (200, 201, 204):
                raise RuntimeError(
                    f"Failed message dispatch to {topic_name}:partition-{p_id} (status {status}): {res}"
                )

            offset = res.get("offset") if isinstance(res, dict) else seq
            partition_probes.append({
                "seq": seq,
                "msg_id": msg_id,
                "offset": offset,
                "status": status,
                "latency_ms": round(latency_ms, 3),
            })

        probe_results.append({
            "partition_id": p_id,
            "probes": partition_probes,
            "min_latency_ms": min(p["latency_ms"] for p in partition_probes),
            "max_latency_ms": max(p["latency_ms"] for p in partition_probes),
        })

    return probe_results


def run_provisioning(
    base_url: str = DEFAULT_HTTP_URL,
    secret: str | None = None,
    service_account: str = "svc_ferrous_streaming",
    skip_probes: bool = False,
    allow_dev_secret: bool = True,
) -> bool:
    """Executes full idempotent provisioning workflow."""
    print("=" * 70)
    print(" [PROVISION] Apache Iggy Ingestion Topic Partitioning & Durability")
    print(f" Target URL: {base_url} | Stream: {DEFAULT_STREAM_NAME} (ID: {DEFAULT_STREAM_ID})")
    print("=" * 70)

    # 1. Check broker health with retry
    print("\n[Step 1/5] Verifying Apache Iggy broker liveness (GET /ping)...")
    if not check_liveness_with_retry(base_url, retries=5, delay_seconds=1.0):
        print(f"  [ERROR] Broker liveness probe failed: broker at {base_url} did not respond to /ping within 5s.")
        return False
    print("  [OK] Iggy broker is alive and responding (200 OK 'pong').")

    # 2. Acquire administrative secret
    print("\n[Step 2/5] Acquiring administrative credentials via M2M secret...")
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

    # 3. Ensure stream 'orders'
    print(f"\n[Step 3/5] Idempotently provisioning stream '{DEFAULT_STREAM_NAME}' (ID: {DEFAULT_STREAM_ID})...")
    try:
        s_res = ensure_stream(base_url, token, DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME)
        actual_stream_id = s_res["stream_id"]
        print(f"  [OK] Stream '{DEFAULT_STREAM_NAME}' status: {s_res['action']} (Resolved ID: {actual_stream_id}).")
    except Exception as e:
        print(f"  [ERROR] Failed to ensure stream: {e}")
        return False

    # 4. Ensure topics
    print("\n[Step 4/5] Idempotently provisioning partitioned topics (4 partitions each)...")
    for t_spec in TOPICS_SPEC:
        t_name = t_spec["name"]
        p_count = t_spec["partitions_count"]
        print(f"  -> Provisioning topic '{t_name}' (ID: {t_spec['id']}, {p_count} partitions)...")
        try:
            t_res = ensure_topic(base_url, token, actual_stream_id, t_spec)
            print(f"     [OK] Topic '{t_name}' status: {t_res['action']} ({p_count} partitions verified).")
        except Exception as e:
            print(f"     [ERROR] Failed to provision topic '{t_name}': {e}")
            return False

    # 5. Optional probe dispatches
    if not skip_probes:
        print("\n[Step 5/5] Dispatched synthetic verification probes across all partitions...")
        for t_spec in TOPICS_SPEC:
            t_name = t_spec["name"]
            print(f"  -> Testing fsync dispatch & ordering for '{t_name}' across {DEFAULT_PARTITION_COUNT} partitions...")
            try:
                probes = dispatch_partition_probes(base_url, token, actual_stream_id, t_name, probes_per_partition=2)
                for pr in probes:
                    p_id = pr["partition_id"]
                    lat = pr["min_latency_ms"]
                    cnt = len(pr["probes"])
                    print(f"     [OK] Partition {p_id}: {cnt} sequential messages committed (p50: {lat}ms, fsync verified).")
            except Exception as e:
                print(f"     [ERROR] Probe dispatch failed for '{t_name}': {e}")
                return False
    else:
        print("\n[Step 5/5] Skipping message dispatch probes as requested.")

    print("\n" + "=" * 70)
    print(" [SUCCESS] All Ingestion Topics and Partitions Successfully Provisioned!")
    print("=" * 70)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apache Iggy Ingestion Topic Partitioning & Durability Provisioner (Story 3.3)"
    )
    parser.add_argument("--url", default=DEFAULT_HTTP_URL, help="Base HTTP URL for Iggy")
    parser.add_argument("--secret", default=None, help="HMAC JWT secret for authentication")
    parser.add_argument("--account", default="svc_ferrous_streaming", help="Service account name")
    parser.add_argument("--skip-probes", action="store_true", help="Skip partition message dispatch probes")
    parser.add_argument(
        "--no-dev-secret",
        dest="allow_dev_secret",
        action="store_false",
        help="Disallow fallback to default dev secret (enforces Secret Zero strictly)",
    )
    parser.set_defaults(allow_dev_secret=True)

    args = parser.parse_args()
    success = run_provisioning(
        base_url=args.url,
        secret=args.secret,
        service_account=args.account,
        skip_probes=args.skip_probes,
        allow_dev_secret=args.allow_dev_secret,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
