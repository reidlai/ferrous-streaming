#!/usr/bin/env python3
"""
test_container_builds.py: Unit tests for Multi-Service (Apache Iggy & Arroyo) Docker Image Builds (Story 3.9).
Submodule: ferrous-streaming
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


class ContainerBuildsUnitTests(unittest.TestCase):
    """Validates container specifications for Apache Iggy and Arroyo stream processor."""

    def test_01_dockerfile_iggy_specification(self):
        """Verifies Dockerfile.iggy adheres to non-root UID 10001, pure Rust core, and port exposures."""
        iggy_df = REPO_ROOT / "Dockerfile.iggy"
        self.assertTrue(iggy_df.is_file(), "Dockerfile.iggy must exist")

        content = iggy_df.read_text(encoding="utf-8")
        self.assertIn("FROM apache/iggy:latest AS upstream", content)
        self.assertIn("FROM alpine:3.20", content)
        self.assertIn("USER 10001:10001", content)
        self.assertIn('VOLUME ["/data/iggy"]', content)
        self.assertIn("EXPOSE 8090 3000 9092", content)
        self.assertIn("/usr/local/bin/iggy-server", content)

        # DevSecOps: Zero JVM, Zero Python in broker image
        lower = content.lower()
        self.assertNotIn("openjdk", lower)
        self.assertNotIn("python", lower)
        self.assertNotIn("pip", lower)

    def test_02_dockerfile_arroyo_specification(self):
        """Verifies Dockerfile.arroyo packages streaming pipelines, non-root UID 10001, and health probe."""
        arroyo_df = REPO_ROOT / "Dockerfile.arroyo"
        self.assertTrue(arroyo_df.is_file(), "Dockerfile.arroyo must exist")

        content = arroyo_df.read_text(encoding="utf-8")
        self.assertIn("FROM ghcr.io/arroyosystems/arroyo:0.11.0 AS upstream", content)
        self.assertIn("FROM debian:bookworm-slim", content)
        self.assertIn("USER 10001:10001", content)
        self.assertIn("/etc/arroyo/pipelines", content)
        self.assertIn("EXPOSE 5115", content)
        self.assertIn("/api/v1/ping", content)
        self.assertIn('VOLUME ["/data/arroyo", "/data/delta"]', content)

    def test_03_docker_compose_multi_service_targets(self):
        """Verifies docker-compose.yml defines both services with hardened security flags."""
        compose_file = REPO_ROOT / "docker-compose.yml"
        self.assertTrue(compose_file.is_file(), "docker-compose.yml must exist")

        content = compose_file.read_text(encoding="utf-8")
        self.assertIn("iggy-broker:", content)
        self.assertIn("arroyo:", content)
        self.assertIn("ferrous-streaming-iggy", content)
        self.assertIn("ferrous-streaming-arroyo", content)
        self.assertIn('user: "10001:10001"', content)
        self.assertIn("no-new-privileges:true", content)
        self.assertIn("cap_drop:", content)
        self.assertIn("condition: service_healthy", content)

    def test_04_pipeline_definitions_availability(self):
        """Verifies all required streaming SQL pipelines exist for Arroyo packaging."""
        raw_sql = REPO_ROOT / "pipelines" / "raw_event_archiver.sql"
        state_sql = REPO_ROOT / "pipelines" / "state_snapshot_consolidator.sql"

        self.assertTrue(raw_sql.is_file(), "raw_event_archiver.sql must exist")
        self.assertTrue(state_sql.is_file(), "state_snapshot_consolidator.sql must exist")


if __name__ == "__main__":
    unittest.main()

