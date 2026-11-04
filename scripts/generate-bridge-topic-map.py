#!/usr/bin/env python3
"""
generate-bridge-topic-map.py: Generates the Iggy-Kafka bridge topic map from topics.yml.
Submodule: ferrous-streaming
Stories: 3.13 (consume side) and 3.14 (produce side); AD-27 and AD-28.

The upstream `iggy-gateway-kafka` image reads a TOML file (IGGY_KAFKA_TOPIC_MAP_PATH) that
maps each Kafka topic name to an Iggy {stream, topic}. The map is the allowlist of the
bridge: a Kafka name that is not in the map resolves to the empty default stream and fails
with UNKNOWN_TOPIC_OR_PARTITION. It is generated from the contracts registry in ferrous-cx
(topics.yml) and never edited by hand.

Usage:
    python scripts/generate-bridge-topic-map.py                 # write config/bridge/topic-map.toml
    python scripts/generate-bridge-topic-map.py --check         # exit 1 if the committed file is stale
    python scripts/generate-bridge-topic-map.py --topics PATH   # use another topics.yml

PyYAML is required (pip install pyyaml).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TOPICS = REPO_ROOT.parent / "ferrous-cx" / "contracts" / "topics.yml"
DEFAULT_OUTPUT = REPO_ROOT / "config" / "bridge" / "topic-map.toml"

# The gateway requires a default stream. It must hold NO topics, so that any Kafka name
# that is not in the map resolves to a topic that does not exist.
DEFAULT_STREAM = "kafka"

KAFKA_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
GROUP_RE = re.compile(r"^[A-Za-z0-9._-]{1,246}$")
IGGY_NAME_MAX = 255


class TopicsError(ValueError):
    """topics.yml is invalid for the bridge."""


def load_topics(path: Path) -> list[dict]:
    """Loads and validates topics.yml; returns the topic entries."""
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover - environment guard
        raise SystemExit("PyYAML is required: pip install pyyaml") from exc

    if not path.is_file():
        raise TopicsError(f"topics.yml not found: {path}")
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or doc.get("version") != 1:
        raise TopicsError("topics.yml must be a mapping with 'version: 1'")
    topics = doc.get("topics")
    if not isinstance(topics, list) or not topics:
        raise TopicsError("topics.yml must list at least one topic")

    seen_kafka: dict[str, str] = {}
    seen_iggy: dict[tuple[str, str], str] = {}
    for entry in topics:
        if not isinstance(entry, dict):
            raise TopicsError(f"topic entry must be a mapping: {entry!r}")
        stream, topic = entry.get("stream"), entry.get("topic")
        for label, value in (("stream", stream), ("topic", topic)):
            if not isinstance(value, str) or not value or value != value.strip():
                raise TopicsError(f"{label} must be a non-empty string without padding: {entry!r}")
            if len(value.encode("utf-8")) > IGGY_NAME_MAX:
                raise TopicsError(f"{label} exceeds {IGGY_NAME_MAX} bytes: {value!r}")
        if stream == DEFAULT_STREAM or stream == f"{DEFAULT_STREAM}.meta":
            raise TopicsError(
                f"stream {stream!r} is reserved for the bridge's default stream and must hold no topics"
            )

        expected = f"{stream}.{topic}"
        kafka_name = entry.get("kafka_name", expected)
        if kafka_name != expected:
            raise TopicsError(
                f"kafka_name for {stream}/{topic} must be {expected!r} (stream + '.' + topic), got {kafka_name!r}"
            )
        if not KAFKA_NAME_RE.match(kafka_name) or kafka_name in (".", ".."):
            raise TopicsError(f"kafka_name {kafka_name!r} is not a valid Kafka topic name ([A-Za-z0-9._-], <=249 bytes)")
        if kafka_name in seen_kafka:
            raise TopicsError(f"kafka_name {kafka_name!r} is used twice")
        if (stream, topic) in seen_iggy:
            raise TopicsError(f"{stream}/{topic} is listed twice; the map must be one to one")
        seen_kafka[kafka_name] = f"{stream}/{topic}"
        seen_iggy[(stream, topic)] = kafka_name
        entry["kafka_name"] = kafka_name

        kafka = entry.get("kafka")
        if not isinstance(kafka, dict):
            raise TopicsError(f"{stream}/{topic} needs a 'kafka' block with 'consume' and 'produce'")
        groups = kafka.get("consume")
        if not isinstance(groups, list) or not all(isinstance(g, str) and GROUP_RE.match(g) for g in groups):
            raise TopicsError(f"{stream}/{topic}: kafka.consume must be a list of valid group names")
        if not isinstance(kafka.get("produce"), bool):
            raise TopicsError(f"{stream}/{topic}: kafka.produce must be true or false")
        if not isinstance(entry.get("partitions"), int) or entry["partitions"] < 1:
            raise TopicsError(f"{stream}/{topic}: partitions must be a positive integer")
    return topics


def render(topics: list[dict], source: str) -> str:
    """Renders the gateway's TOML topic map (sorted by Kafka name for a stable diff)."""
    lines = [
        "# GENERATED by scripts/generate-bridge-topic-map.py. Do not edit by hand.",
        f"# Source: {source}",
        "# Read by the Iggy-Kafka bridge (IGGY_KAFKA_TOPIC_MAP_PATH). Only the names below resolve;",
        "# any other Kafka topic name falls through to the default stream, which holds no topics.",
        "",
        f'default_stream = "{DEFAULT_STREAM}"',
    ]
    for entry in sorted(topics, key=lambda e: e["kafka_name"]):
        lines += [
            "",
            f'[topics."{entry["kafka_name"]}"]',
            f'stream = "{entry["stream"]}"',
            f'topic  = "{entry["topic"]}"',
        ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the Iggy-Kafka bridge topic map from topics.yml")
    parser.add_argument("--topics", type=Path, default=DEFAULT_TOPICS, help=f"topics.yml (default: {DEFAULT_TOPICS})")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"map file (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--check", action="store_true", help="do not write; exit 1 if the output file is stale")
    args = parser.parse_args(argv)

    try:
        topics = load_topics(args.topics)
    except TopicsError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        return 2

    text = render(topics, "ferrous-cx/contracts/topics.yml")
    if args.check:
        current = args.output.read_text(encoding="utf-8").replace("\r\n", "\n") if args.output.is_file() else None
        if current != text:
            print(f"[FAIL] {args.output} is stale; run scripts/generate-bridge-topic-map.py", file=sys.stderr)
            return 1
        print(f"[OK] {args.output} matches {args.topics}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(f"[OK] wrote {args.output} ({len(topics)} topics)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
