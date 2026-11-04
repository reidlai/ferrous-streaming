#!/usr/bin/env python3
"""
Test Suite: Iggy-Kafka bridge configuration (Stories 3.13 and 3.14, AD-28).

These tests check the configuration that the bridge is deployed with. They do NOT start a bridge,
an Iggy server or Arroyo: Docker was not available when they were written, so the behaviour
(N sent equals N stored, restarts lose nothing, unknown topics are refused) is covered by the live
checks L1-L14 of the Bridge Contract Sheet, which are still open.
"""

import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path

import yaml

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
TOPIC_MAP = REPO_ROOT / "config" / "bridge" / "topic-map.toml"
COMPOSE = REPO_ROOT / "docker-compose.yml"
TOPICS_YML = REPO_ROOT.parent / "ferrous-cx" / "contracts" / "topics.yml"


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen = _load("generate_bridge_topic_map", "generate-bridge-topic-map.py")
bridge_user = _load("provision_bridge_user", "provision-bridge-user.py")
provision_topics = _load("provision_topics", "provision-topics.py")

KAFKA_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def load_map() -> dict:
    with open(TOPIC_MAP, "rb") as f:
        return tomllib.load(f)


def load_compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


VALID_TOPICS = """
version: 1
topics:
  - stream: orders
    topic: intake
    kafka_name: orders.intake
    partitions: 4
    kafka: {consume: [cg-arroyo-raw], produce: false}
  - stream: olist
    topic: orders_stream
    kafka_name: olist.orders_stream
    partitions: 4
    kafka: {consume: [cg-arroyo-olist], produce: true}
"""


class TestTopicMap(unittest.TestCase):
    """The generated map is the consume and produce allowlist of AD-28 rule 6."""

    def test_01_default_stream_is_required_and_holds_no_topics(self):
        cfg = load_map()
        self.assertEqual(cfg["default_stream"], "kafka")
        streams = {t["stream"] for t in cfg["topics"].values()}
        self.assertNotIn("kafka", streams, "no mapped topic may live in the default stream")
        self.assertNotIn("kafka.meta", streams)

    def test_02_names_are_valid_kafka_names_and_one_to_one(self):
        cfg = load_map()
        pairs = []
        for kafka_name, target in cfg["topics"].items():
            self.assertRegex(kafka_name, KAFKA_NAME_RE)
            self.assertEqual(kafka_name, f"{target['stream']}.{target['topic']}", "kafka_name is stream + '.' + topic")
            pairs.append((target["stream"], target["topic"]))
        self.assertEqual(len(pairs), len(set(pairs)), "two Kafka names must not point at one Iggy topic")

    def test_03_exactly_the_three_platform_topics(self):
        self.assertEqual(
            set(load_map()["topics"]),
            {"orders.intake", "orders.consolidated", "olist.orders_stream"},
        )

    def test_04_orders_topics_match_the_topic_provisioner(self):
        cfg = load_map()
        for spec in provision_topics.TOPICS_SPEC:
            name = f"{provision_topics.DEFAULT_STREAM_NAME}.{spec['name']}"
            self.assertIn(name, cfg["topics"], f"{name} must be mapped")
            self.assertEqual(cfg["topics"][name]["topic"], spec["name"])

    def test_05_committed_map_matches_topics_yml(self):
        if not TOPICS_YML.is_file():
            self.skipTest("ferrous-cx/contracts/topics.yml is not next to this repository")
        topics = gen.load_topics(TOPICS_YML)
        expected = gen.render(topics, "ferrous-cx/contracts/topics.yml")
        actual = TOPIC_MAP.read_text(encoding="utf-8").replace("\r\n", "\n")
        self.assertEqual(actual, expected, "topic-map.toml is stale: run scripts/generate-bridge-topic-map.py")


class TestGenerator(unittest.TestCase):
    """The generator refuses a registry that would break the contract."""

    def _load_text(self, text: str):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "topics.yml"
            p.write_text(text, encoding="utf-8")
            return gen.load_topics(p)

    def test_01_valid_registry_renders_sorted_toml(self):
        text = gen.render(self._load_text(VALID_TOPICS), "test")
        cfg = tomllib.loads(text)
        self.assertEqual(cfg["default_stream"], "kafka")
        self.assertEqual(list(cfg["topics"]), ["olist.orders_stream", "orders.intake"])
        self.assertEqual(cfg["topics"]["orders.intake"], {"stream": "orders", "topic": "intake"})

    def test_02_rejects_kafka_name_that_breaks_the_rule(self):
        with self.assertRaises(gen.TopicsError):
            self._load_text(VALID_TOPICS.replace("kafka_name: orders.intake", "kafka_name: intake"))

    def test_03_rejects_invalid_characters(self):
        with self.assertRaises(gen.TopicsError):
            self._load_text(VALID_TOPICS.replace("topic: intake", "topic: in/take").replace("orders.intake", "orders.in/take"))

    def test_04_rejects_duplicate_topic(self):
        dup = VALID_TOPICS + "  - stream: orders\n    topic: intake\n    kafka_name: orders.intake\n    partitions: 4\n    kafka: {consume: [], produce: false}\n"
        with self.assertRaises(gen.TopicsError):
            self._load_text(dup)

    def test_05_rejects_reserved_default_stream(self):
        with self.assertRaises(gen.TopicsError):
            self._load_text(VALID_TOPICS.replace("stream: orders", "stream: kafka").replace("orders.intake", "kafka.intake"))

    def test_06_rejects_missing_kafka_block_and_bad_partitions(self):
        with self.assertRaises(gen.TopicsError):
            self._load_text(VALID_TOPICS.replace("    kafka: {consume: [cg-arroyo-raw], produce: false}\n", ""))
        with self.assertRaises(gen.TopicsError):
            self._load_text(VALID_TOPICS.replace("partitions: 4", "partitions: 0", 1))

    def test_07_check_mode_detects_a_stale_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            topics = Path(tmp) / "topics.yml"
            topics.write_text(VALID_TOPICS, encoding="utf-8")
            out = Path(tmp) / "map.toml"
            self.assertEqual(gen.main(["--topics", str(topics), "--output", str(out)]), 0)
            self.assertEqual(gen.main(["--check", "--topics", str(topics), "--output", str(out)]), 0)
            out.write_text(out.read_text(encoding="utf-8") + "\n# edited by hand\n", encoding="utf-8")
            self.assertEqual(gen.main(["--check", "--topics", str(topics), "--output", str(out)]), 1)


class TestBridgeServices(unittest.TestCase):
    """The compose services follow Contract Sheet section 9 and AD-28 rule 5."""

    def setUp(self):
        self.compose = load_compose()
        self.services = self.compose["services"]

    def test_01_two_gateway_instances_with_one_pinned_image(self):
        bridge, host = self.services["bridge"], self.services["bridge-host"]
        self.assertEqual(bridge["image"], host["image"], "both listeners run the same image")
        self.assertRegex(bridge["image"], r"^apache/iggy-kafka-gateway@sha256:[0-9a-f]{64}$", "the image is pinned by digest")

    def test_02_listener_exposure(self):
        bridge, host = self.services["bridge"], self.services["bridge-host"]
        self.assertNotIn("ports", bridge, "the compose-network bridge publishes nothing")
        self.assertEqual(bridge["expose"], ["9092"])
        self.assertEqual(host["ports"], ["127.0.0.1:9092:9092"], "the host listener is loopback only")
        for name, svc in self.services.items():
            for port in svc.get("ports", []):
                if "9092" in str(port):
                    self.assertTrue(str(port).startswith("127.0.0.1:"), f"{name} publishes 9092 beyond loopback")

    def test_03_each_instance_advertises_its_own_address(self):
        bridge = self.services["bridge"]["environment"]
        host = self.services["bridge-host"]["environment"]
        self.assertEqual(bridge["IGGY_KAFKA_ADVERTISED_HOST"], "bridge")
        self.assertEqual(host["IGGY_KAFKA_ADVERTISED_HOST"], "127.0.0.1")
        self.assertEqual(bridge["IGGY_KAFKA_ADVERTISED_PORT"], "9092")
        self.assertEqual(host["IGGY_KAFKA_ADVERTISED_PORT"], "9092")
        self.assertNotEqual(bridge["IGGY_KAFKA_NODE_ID"], host["IGGY_KAFKA_NODE_ID"])

    def test_04_bridge_is_enabled_and_never_creates_topics_or_dedups(self):
        for name in ("bridge", "bridge-host"):
            env = self.services[name]["environment"]
            self.assertEqual(env["IGGY_KAFKA_BRIDGE_ENABLED"], "true", "the image is a stub unless enabled")
            self.assertEqual(env["IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED"], "false")
            self.assertEqual(env["IGGY_KAFKA_IDEMPOTENT_PRODUCERS"], "false")
            self.assertEqual(env["IGGY_KAFKA_TOPIC_MAP_PATH"], "/etc/iggy-gateway/topic-map.toml")
            self.assertEqual(env["IGGY_KAFKA_IGGY_ADDR"], "iggy-broker:8090")
            self.assertNotIn("IGGY_KAFKA_BROKERS", env, "no coordinator log: the instances share no state")
            self.assertNotIn("IGGY_KAFKA_SASL_ENABLED", env)
            self.assertNotIn("IGGY_KAFKA_LISTENERS", env)

    def test_05_password_comes_from_the_environment(self):
        for name in ("bridge", "bridge-host"):
            value = self.services[name]["environment"]["IGGY_KAFKA_IGGY_PASSWORD"]
            self.assertIn("${BRIDGE_IGGY_PASSWORD", value, "no literal password in the compose file")
        text = COMPOSE.read_text(encoding="utf-8")
        self.assertNotRegex(text, r"IGGY_KAFKA_IGGY_PASSWORD:\s*[A-Za-z0-9]")

    def test_06_container_hardening(self):
        for name in ("bridge", "bridge-host"):
            svc = self.services[name]
            self.assertNotEqual(svc["user"].split(":")[0], "0", "must not run as root")
            self.assertTrue(svc["read_only"])
            self.assertIn("ALL", svc["cap_drop"])
            self.assertIn("no-new-privileges:true", svc["security_opt"])
            self.assertIn("healthcheck", svc)
            self.assertEqual(svc["healthcheck"]["test"][:3], ["CMD", "nc", "-z"])

    def test_07_topic_map_is_mounted_read_only(self):
        for name in ("bridge", "bridge-host"):
            self.assertIn(
                "./config/bridge/topic-map.toml:/etc/iggy-gateway/topic-map.toml:ro",
                self.services[name]["volumes"],
            )

    def test_08_consumers_wait_for_a_healthy_bridge(self):
        self.assertEqual(self.services["arroyo"]["depends_on"]["bridge"]["condition"], "service_healthy")
        for name in ("bridge", "bridge-host"):
            self.assertEqual(self.services[name]["depends_on"]["iggy-broker"]["condition"], "service_healthy")

    def test_09_iggy_server_no_longer_owns_the_kafka_port(self):
        ports = self.services["iggy-broker"]["ports"]
        self.assertEqual(ports, ["8090:8090", "3000:3000"])
        self.assertNotIn("IGGY_KAFKA_PORT", " ".join(self.services["arroyo"]["environment"]))


class TestArroyoPipelines(unittest.TestCase):
    """Each Arroyo Kafka source follows Contract Sheet section 6."""

    PIPELINES = {
        "raw_event_archiver.sql": ("orders.intake", "cg-arroyo-raw"),
        "state_snapshot_consolidator.sql": ("orders.consolidated", "cg-arroyo-state"),
    }

    def test_01_sources_use_the_bridge_and_the_required_settings(self):
        mapped = set(load_map()["topics"])
        for filename, (kafka_name, group) in self.PIPELINES.items():
            sql = (REPO_ROOT / "pipelines" / filename).read_text(encoding="utf-8")
            self.assertIn("bootstrap_servers = 'bridge:9092'", sql, filename)
            self.assertIn(f"topic = '{kafka_name}'", sql, filename)
            self.assertIn(kafka_name, mapped, f"{kafka_name} must be in the bridge topic map")
            self.assertIn("'source.offset' = 'earliest'", sql, f"{filename}: the first deploy must read the whole log")
            self.assertIn(f"'source.group_id' = '{group}'", sql, filename)
            self.assertIn("auto.offset.reset=error", sql, f"{filename}: an out-of-range offset must stall, not skip")
            self.assertIn("isolation.level=read_uncommitted", sql, filename)

    def test_02_no_iggy_slash_names_or_direct_iggy_kafka_port_remain(self):
        for filename in self.PIPELINES:
            sql = (REPO_ROOT / "pipelines" / filename).read_text(encoding="utf-8")
            self.assertNotIn("iggy-broker:9092", sql)
            self.assertNotRegex(sql, r"topic = '[^']*/", "Kafka clients use kafka_name, never stream/topic")

    def test_03_groups_belong_to_the_registry(self):
        if not TOPICS_YML.is_file():
            self.skipTest("ferrous-cx/contracts/topics.yml is not next to this repository")
        registry = {t["kafka_name"]: set(t["kafka"]["consume"]) for t in gen.load_topics(TOPICS_YML)}
        for filename, (kafka_name, group) in self.PIPELINES.items():
            self.assertIn(group, registry[kafka_name], f"{group} is not allowed to read {kafka_name}")


class TestBridgeUserPermissions(unittest.TestCase):
    """The Iggy user is the produce allowlist: send only where topics.yml says so (AD-28 rule 6)."""

    def setUp(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "topics.yml"
            p.write_text(VALID_TOPICS, encoding="utf-8")
            self.topics = gen.load_topics(p)
        ids = {("orders", "intake"): (1, 1), ("olist", "orders_stream"): (7, 3)}
        self.perms = bridge_user.build_permissions(self.topics, lambda s, t: ids[(s, t)])

    def test_01_no_global_permission(self):
        self.assertFalse(any(self.perms["global"].values()), "no global permission, so no manage_*")

    def test_02_send_only_on_produce_topics(self):
        intake = self.perms["streams"]["1"]["topics"]["1"]
        orders_stream = self.perms["streams"]["7"]["topics"]["3"]
        self.assertTrue(intake["poll_messages"])
        self.assertFalse(intake["send_messages"], "a Kafka produce to orders/intake must be refused")
        self.assertTrue(orders_stream["send_messages"])
        self.assertTrue(orders_stream["poll_messages"])

    def test_03_stream_level_send_and_manage_are_never_granted(self):
        for stream in self.perms["streams"].values():
            self.assertFalse(stream["send_messages"])
            self.assertFalse(stream["manage_stream"])
            self.assertFalse(stream["manage_topics"])
            for topic in stream["topics"].values():
                self.assertFalse(topic["manage_topic"])

    def test_04_unlisted_topics_get_no_entry(self):
        listed = {tid for s in self.perms["streams"].values() for tid in s["topics"]}
        self.assertEqual(listed, {"1", "3"})

    def test_05_dry_run_prints_a_document_and_sends_nothing(self):
        if not TOPICS_YML.is_file():
            self.skipTest("ferrous-cx/contracts/topics.yml is not next to this repository")
        import contextlib
        import io

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            rc = bridge_user.main(["--topics", str(TOPICS_YML)])
        self.assertEqual(rc, 0)
        doc = json.loads(buf.getvalue())
        self.assertEqual(doc["username"], "svc_kafka_bridge")
        self.assertNotIn("password", json.dumps(doc))

    def test_06_apply_creates_or_updates_through_the_http_api(self):
        calls = []

        def fake_http(url, method="GET", token=None, json_data=None, timeout=5.0):
            calls.append((method, url))
            if method == "GET":
                return 404, {}
            return 201, {}

        action = bridge_user.apply_user("http://x", "t", "u", "pw", self.perms, fake_http)
        self.assertEqual(action, "created")
        self.assertEqual(calls[-1], ("POST", "http://x/users"))

        calls.clear()

        def existing(url, method="GET", token=None, json_data=None, timeout=5.0):
            calls.append((method, url))
            return (200, {}) if method == "GET" else (204, {})

        self.assertEqual(bridge_user.apply_user("http://x", "t", "u", "pw", self.perms, existing), "updated")
        self.assertEqual(calls[-1], ("PUT", "http://x/users/u/permissions"))


if __name__ == "__main__":
    unittest.main()
