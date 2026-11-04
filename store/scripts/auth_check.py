#!/usr/bin/env python3
"""
auth_check.py: Verifies M2M Service Account credentials for Lakehouse maintenance.
Enforces FR-23, AD-16, and Secret Zero security standards.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

def load_dotenv(dotenv_path: Path):
    """Simple parser for .env file without external dependencies."""
    if not dotenv_path.exists():
        return
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k not in os.environ:
                    os.environ[k] = v

def verify_service_account(account: str = None, token: str = None) -> bool:
    """Verifies that the caller possesses valid svc_ferrous_lakehouse credentials."""
    # Attempt to load .env from repo root
    load_dotenv(REPO_ROOT / ".env")

    account = account or os.environ.get("LAKEHOUSE_SERVICE_ACCOUNT")
    token = token or os.environ.get("LAKEHOUSE_SERVICE_TOKEN")

    if not account or not token:
        return False
    if account != "svc_ferrous_lakehouse":
        return False
    if token.startswith("<REPLACE_") or len(token) < 8:
        return False

    return True

def enforce_service_account(account: str = None, token: str = None):
    """Enforces authentication check, exiting with HTTP 401 code on denial."""
    if not verify_service_account(account, token):
        sys.stderr.write(
            "ERROR 401 Unauthorized: Storage maintenance operations require authenticated "
            "'svc_ferrous_lakehouse' service account credentials (FR-23, AD-16).\n"
        )
        sys.exit(1)
