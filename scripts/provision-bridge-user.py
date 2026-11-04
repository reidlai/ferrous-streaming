#!/usr/bin/env python3
"""
provision-bridge-user.py: Builds and applies the Iggy user of the Iggy-Kafka bridge.
Submodule: ferrous-streaming
Stories: 3.13 (consume side) and 3.14 (produce side); AD-27 rule 2 and AD-28 rule 6.

The bridge is one Iggy client that serves every Kafka client (Arroyo and the replayer), so it has
one Iggy user. The produce allowlist of the platform lives here and nowhere else: the gateway does
no per-client authorization, so a Kafka produce is refused only because this user holds no
`send_messages` permission on the topic. The user therefore gets:

  * read and poll on every topic that a Kafka consumer group is allowed to read;
  * send on the topics that topics.yml marks `kafka.produce: true`, and on no other topic;
  * no global manage_* permission (Kafka CreateTopics, DeleteTopics and AlterConfigs are then
    refused by Iggy).

STATUS: the permission set is a desk design. It has not been applied to a running Iggy server
(live check L13). The gateway also keeps a cluster id in the stream "<default stream>.meta" and
creates a consumer group `kafka.cg.<group>` on first offset commit; L13 finds out whether the
minimum below is enough or needs more.

Usage:
    python scripts/provision-bridge-user.py                 # print the permission document (dry run)
    python scripts/provision-bridge-user.py --apply         # create the user on a running Iggy server

--apply needs the Iggy admin JWT secret (IGGY_HTTP_JWT_ENCODING_SECRET) and the bridge password
(BRIDGE_IGGY_PASSWORD) in the environment or .env; neither is printed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Callable

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

SCRIPTS_DIR = Path(__file__).resolve().parent
DEFAULT_USERNAME = os.environ.get("BRIDGE_IGGY_USERNAME", "svc_kafka_bridge")
DEFAULT_HTTP_URL = os.environ.get("STREAMING_HTTP_URL", os.environ.get("IGGY_HTTP_URL", "http://localhost:3000"))


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _global_permissions() -> dict:
    """No global permission at all: the user can manage neither streams, topics, users nor servers."""
    return {
        key: False
        for key in (
            "manage_servers",
            "read_servers",
            "manage_users",
            "read_users",
            "manage_streams",
            "read_streams",
            "manage_topics",
            "read_topics",
            "poll_messages",
            "send_messages",
        )
    }


def build_permissions(topics: list[dict], resolve_ids: Callable[[str, str], tuple[int, int]]) -> dict:
    """
    Builds the Iggy permission document from the topics registry.

    `resolve_ids(stream, topic)` returns the numeric (stream_id, topic_id): Iggy keys per-stream and
    per-topic permissions by numeric id. A topic needs `poll_messages` if some consumer group may read
    it and `send_messages` if a Kafka producer may write to it; with neither, it gets no entry.
    """
    streams: dict[str, dict] = {}
    for entry in topics:
        consume = bool(entry["kafka"]["consume"])
        produce = bool(entry["kafka"]["produce"])
        if not (consume or produce):
            continue
        stream_id, topic_id = resolve_ids(entry["stream"], entry["topic"])
        stream = streams.setdefault(
            str(stream_id),
            {
                "manage_stream": False,
                "read_stream": True,
                "manage_topics": False,
                "read_topics": True,
                "poll_messages": False,
                "send_messages": False,
                "topics": {},
            },
        )
        stream["topics"][str(topic_id)] = {
            "manage_topic": False,
            "read_topic": True,
            "poll_messages": consume,
            "send_messages": produce,
        }
    return {"global": _global_permissions(), "streams": streams}


def make_resolver(base_url: str, token: str, http_request: Callable) -> Callable[[str, str], tuple[int, int]]:
    """Resolves {stream, topic} names to numeric ids through the Iggy HTTP API (read-only calls)."""

    def resolve(stream: str, topic: str) -> tuple[int, int]:
        status, res = http_request(f"{base_url}/streams/{stream}", token=token)
        if status != 200 or not isinstance(res, dict) or "id" not in res:
            raise RuntimeError(f"stream '{stream}' not found on the Iggy server (status {status}); provision it first")
        stream_id = res["id"]
        status, res = http_request(f"{base_url}/streams/{stream}/topics/{topic}", token=token)
        if status != 200 or not isinstance(res, dict) or "id" not in res:
            raise RuntimeError(f"topic '{stream}/{topic}' not found on the Iggy server (status {status}); provision it first")
        return int(stream_id), int(res["id"])

    return resolve


def apply_user(base_url: str, token: str, username: str, password: str, permissions: dict, http_request: Callable) -> str:
    """Creates the bridge user, or replaces the permissions of an existing one. Returns 'created' or 'updated'."""
    status, res = http_request(f"{base_url}/users/{username}", token=token)
    if status == 200:
        status_p, res_p = http_request(
            f"{base_url}/users/{username}/permissions", method="PUT", token=token, json_data={"permissions": permissions}
        )
        if status_p not in (200, 204):
            raise RuntimeError(f"failed to update permissions of '{username}' (status {status_p}): {res_p}")
        return "updated"
    if status not in (404,):
        raise RuntimeError(f"unexpected answer looking up user '{username}' (status {status}): {res}")
    payload = {"username": username, "password": password, "status": "active", "permissions": permissions}
    status_c, res_c = http_request(f"{base_url}/users", method="POST", token=token, json_data=payload)
    if status_c not in (200, 201):
        raise RuntimeError(f"failed to create user '{username}' (status {status_c}): {res_c}")
    return "created"


def main(argv: list[str] | None = None) -> int:
    gen = _load("generate_bridge_topic_map", "generate-bridge-topic-map.py")
    parser = argparse.ArgumentParser(description="Provision the Iggy user of the Iggy-Kafka bridge")
    parser.add_argument("--topics", type=Path, default=gen.DEFAULT_TOPICS, help="topics.yml")
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--http-url", default=DEFAULT_HTTP_URL)
    parser.add_argument("--apply", action="store_true", help="create the user on a running Iggy server")
    args = parser.parse_args(argv)

    try:
        topics = gen.load_topics(args.topics)
    except gen.TopicsError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        return 2

    if not args.apply:
        # Dry run: stand-in ids (position in the file) so the document shape can be reviewed offline.
        ids = {(t["stream"], t["topic"]): (i + 1, i + 1) for i, t in enumerate(topics)}
        permissions = build_permissions(topics, lambda s, t: ids[(s, t)])
        print(json.dumps({"username": args.username, "permissions": permissions}, indent=2))
        print("\n[DRY RUN] numeric ids above are placeholders; nothing was sent.", file=sys.stderr)
        return 0

    pt = _load("provision_topics", "provision-topics.py")
    password = os.environ.get("BRIDGE_IGGY_PASSWORD", "")
    if not password:
        print("[FAIL] BRIDGE_IGGY_PASSWORD is not set", file=sys.stderr)
        return 2
    token = pt.make_admin_jwt(pt.load_env_secret())
    resolver = make_resolver(args.http_url, token, pt.http_request)
    permissions = build_permissions(topics, resolver)
    action = apply_user(args.http_url, token, args.username, password, permissions, pt.http_request)
    print(f"[OK] bridge user '{args.username}' {action}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
