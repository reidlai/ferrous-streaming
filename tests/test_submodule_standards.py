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

    def test_03_tier_2_devsecops_ci_workflow(self):
        """Verifies GitHub Actions workflow configures Gitleaks, Hadolint, and Trivy scans."""
        ci_workflow = REPO_ROOT / ".github" / "workflows" / "devsecops-infra-ci.yml"
        self.assertTrue(ci_workflow.is_file(), "devsecops-infra-ci.yml must exist")

        content = ci_workflow.read_text(encoding="utf-8")
        self.assertIn("gitleaks/gitleaks-action", content)
        self.assertIn("hadolint/hadolint-action", content)
        self.assertIn("aquasecurity/trivy-action", content)
        self.assertIn("Dockerfile.iggy", content)
        self.assertIn("Dockerfile.arroyo", content)
        self.assertIn("python -m unittest discover tests", content)


if __name__ == "__main__":
    unittest.main()

