#!/usr/bin/env python3
"""
vacuum.py: Delta Lake VACUUM Retention Automation.
Prunes historical tombstoned Parquet files older than retention threshold (default 168h).
Requires authenticated svc_ferrous_lakehouse credentials (FR-23, AD-16).
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent

# Import auth helper
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from auth_check import enforce_service_account

def get_latest_commit_version(log_dir: Path) -> int:
    commits = list(log_dir.glob("*.json"))
    if not commits:
        return -1
    versions = []
    for c in commits:
        try:
            versions.append(int(c.stem))
        except ValueError:
            pass
    return max(versions) if versions else -1

def run_vacuum(table_dir: Path, retention_hours: int = 168) -> dict:
    """Performs Delta Lake VACUUM on a table directory."""
    log_dir = table_dir / "_delta_log"
    if not log_dir.is_dir():
        print(f"  [ERROR] Table {table_dir.name} has no _delta_log directory.")
        return {"status": "error", "message": "no_delta_log"}

    now_ms = int(time.time() * 1000)
    retention_ms = retention_hours * 3600 * 1000
    cutoff_ms = now_ms - retention_ms

    # Scan all commits to find active vs removed files
    active_files = set()
    tombstones = {} # path -> deletionTimestamp

    commit_files = sorted(log_dir.glob("*.json"))
    for cf in commit_files:
        with open(cf, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    action = json.loads(line)
                    if "add" in action:
                        p = action["add"]["path"]
                        active_files.add(p)
                        tombstones.pop(p, None)
                    elif "remove" in action:
                        p = action["remove"]["path"]
                        del_ts = action["remove"].get("deletionTimestamp", now_ms)
                        active_files.discard(p)
                        tombstones[p] = del_ts
                except Exception:
                    pass

    # Prune tombstoned files that exceed retention threshold
    pruned_files = []
    for path_str, del_ts in tombstones.items():
        if del_ts <= cutoff_ms:
            target_path = table_dir / path_str
            if target_path.exists():
                try:
                    target_path.unlink()
                    pruned_files.append(path_str)
                except Exception as exc:
                    print(f"  [WARN] Failed to delete expired file {path_str}: {exc}")

    # Record VACUUM commit
    current_version = get_latest_commit_version(log_dir)
    next_version = current_version + 1
    commit_file = log_dir / f"{next_version:020d}.json"

    vacuum_action = {
        "commitInfo": {
            "timestamp": now_ms,
            "operation": "VACUUM END",
            "operationParameters": {
                "retentionCheckEnabled": "true",
                "specifiedRetentionMillis": retention_ms,
                "defaultParallelism": 1
            },
            "metrics": {
                "numFilesDeleted": len(pruned_files),
                "numActiveFiles": len(active_files)
            },
            "engineInfo": "ferrous-lakehouse-maintenance/1.0.0"
        }
    }

    with open(commit_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(vacuum_action) + "\n")

    print(f"  [VACUUMED] Table '{table_dir.name}' -> Deleted {len(pruned_files)} expired files "
          f"(Retention: {retention_hours}h, Active files retained: {len(active_files)})")
    return {
        "status": "success",
        "table": table_dir.name,
        "version": next_version,
        "files_pruned": len(pruned_files),
        "active_files": len(active_files),
        "retention_hours": retention_hours
    }

def main():
    parser = argparse.ArgumentParser(description="Delta Lake VACUUM retention cleanup automation.")
    parser.add_argument("--table", type=str, default="orders_current_state",
                        help="Table name to vacuum (default: orders_current_state)")
    parser.add_argument("--data-dir", type=str, default=str(REPO_ROOT / "data" / "delta"),
                        help="Root directory for Delta tables")
    parser.add_argument("--retention-hours", type=int, default=168,
                        help="Retention threshold in hours (default: 168)")
    parser.add_argument("--service-account", type=str, default=None,
                        help="IAM Service Account name")
    parser.add_argument("--service-token", type=str, default=None,
                        help="IAM Service Account token")
    args = parser.parse_args()

    # Enforce Auth
    enforce_service_account(args.service_account, args.service_token)

    data_dir = Path(args.data_dir).resolve()
    table_dir = data_dir / args.table

    print("=" * 60)
    print(f"Running Authenticated VACUUM on table: {args.table} (Retention: {args.retention_hours}h)")
    print("=" * 60)

    if not table_dir.is_dir():
        print(f"ERROR: Table directory does not exist: {table_dir}")
        sys.exit(1)

    result = run_vacuum(table_dir, args.retention_hours)
    print("=" * 60)
    return 0 if result.get("status") == "success" else 1

if __name__ == "__main__":
    sys.exit(main())
