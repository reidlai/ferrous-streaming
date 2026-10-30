#!/usr/bin/env python3
"""
test_submodule_standards.py: Unit tests for Submodule Documentation & Tier 2 Security CI Gate (Story 3.8).
Submodule: ferrous-streaming
"""

from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


class SubmoduleStandardsUnitTests(unittest.TestCase):
    """Validates documentation, pre-commit configuration, and Tier 2 DevSecOps CI workflow."""

    def test_01_readme_c4_and_port_mappings(self):
        """Verifies README.md contains C4 architecture diagram and port mapping table."""
        readme = REPO_ROOT / "README.md"
        self.assertTrue(readme.is_file(), "README.md must exist")

        content = readme.read_text(encoding="utf-8")
        self.assertIn("```mermaid", content, "README must contain Mermaid architecture diagram")
        self.assertIn("8090", content, "README must document port 8090 (Native TCP)")
        self.assertIn("9092", content, "README must document port 9092 (Kafka Wire Protocol)")
        self.assertIn("3000", content, "README must document port 3000 (HTTP Management & /ping)")
        self.assertIn("5115", content, "README must document port 5115 (Arroyo Web Dashboard)")
        self.assertIn("Standalone Developer Quickstart", content)
        self.assertIn("docker compose up -d", content)

    def test_02_pre_commit_configuration(self):
        """Verifies .pre-commit-config.yaml contains Gitleaks, format fixers, and test hooks."""
        pre_commit = REPO_ROOT / ".pre-commit-config.yaml"
        self.assertTrue(pre_commit.is_file(), ".pre-commit-config.yaml must exist")

        content = pre_commit.read_text(encoding="utf-8")
        self.assertIn("gitleaks", content, "Must declare Gitleaks hook")
        self.assertIn("check-yaml", content, "Must declare check-yaml hook")
        self.assertIn("check-toml", content, "Must declare check-toml hook")
        self.assertIn("test-broker-scaffolding", content)
        self.assertIn("test-broker-security", content)
        self.assertIn("test-topic-partitioning", content)
        self.assertIn("test-consumer-groups", content)
        self.assertIn("test-arroyo-service", content)
        self.assertIn("test-raw-event-archiver", content)
        self.assertIn("test-state-consolidator", content)

    def test_03_tier_2_devsecops_build_and_scan(self):
        """Verifies local build-and-scan scripts configure Docker builds and Trivy scans."""
        sh_script = REPO_ROOT / "scripts" / "build-and-scan.sh"
        ps1_script = REPO_ROOT / "scripts" / "build-and-scan.ps1"

        self.assertTrue(sh_script.is_file(), "build-and-scan.sh must exist")
        self.assertTrue(ps1_script.is_file(), "build-and-scan.ps1 must exist")

        sh_content = sh_script.read_text(encoding="utf-8")
        self.assertIn("Dockerfile.iggy", sh_content)
        self.assertIn("Dockerfile.arroyo", sh_content)
        self.assertIn("ferrous-streaming-iggy", sh_content)
        self.assertIn("ferrous-streaming-arroyo", sh_content)
        self.assertIn("trivy", sh_content)
        self.assertIn("HIGH,CRITICAL", sh_content)

        ps1_content = ps1_script.read_text(encoding="utf-8")
        self.assertIn("Dockerfile.iggy", ps1_content)
        self.assertIn("Dockerfile.arroyo", ps1_content)
        self.assertIn("trivy", ps1_content)
        self.assertIn("HIGH,CRITICAL", ps1_content)


if __name__ == "__main__":
    unittest.main()

