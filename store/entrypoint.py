#!/usr/bin/env python3
"""
entrypoint.py: Container lifecycle entrypoint for ferrous-lakehouse.
Supports --init, --maintain, --healthcheck, and daemon operational modes.
Adheres to AD-13, AD-17, and AD-23.
"""

import argparse
import os
import signal
import sys
import time
from pathlib import Path

REPO_ROOT = Path("/app") if Path("/app").is_dir() else Path(__file__).resolve().parent

# Add scripts directory to path
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from init_tables import init_table, REPO_ROOT as SCRIPTS_ROOT
from optimize import run_optimize
from vacuum import run_vacuum
from auth_check import verify_service_account

DATA_DIR = Path(os.environ.get("LAKEHOUSE_DATA_DIR", "/data/delta"))

def do_init() -> int:
    print(f"==> [INIT] Scaffolding Delta Lake tables under {DATA_DIR}...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tables = [
        ("orders_raw_events", REPO_ROOT / "schemas" / "streaming" / "orders_raw_events.arrow.json"),
        ("orders_current_state", REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"),
        ("llm_benchmarks", REPO_ROOT / "schemas" / "benchmarks" / "llm_benchmarks.arrow.json")
    ]
    for name, schema_file in tables:
        init_table(DATA_DIR, name, schema_file)
    print("==> [INIT] Initialization completed.")
    return 0

def do_maintain() -> int:
    print("==> [MAINTAIN] Running authenticated compaction and log cleanup...")
    # Verify auth
    account = os.environ.get("LAKEHOUSE_SERVICE_ACCOUNT")
    token = os.environ.get("LAKEHOUSE_SERVICE_TOKEN")
    if not verify_service_account(account, token):
        sys.stderr.write("ERROR 401: Authentication required for lakehouse maintenance (FR-23, AD-16).\n")
        return 1

    tables = ["orders_current_state", "orders_raw_events", "llm_benchmarks"]
    for t in tables:
        t_dir = DATA_DIR / t
        if t_dir.is_dir():
            run_optimize(t_dir, target_mb=128)
            run_vacuum(t_dir, retention_hours=168)
    print("==> [MAINTAIN] Maintenance completed.")
    return 0

def do_healthcheck() -> int:
    # 1. Verify DATA_DIR is accessible and readable
    if not DATA_DIR.exists():
        sys.stderr.write(f"Healthcheck failed: {DATA_DIR} does not exist\n")
        return 1
    # 2. Check that at least one table's _delta_log exists
    core_log = DATA_DIR / "orders_current_state" / "_delta_log"
    if not core_log.is_dir():
        # Tables might not be initialized yet; ensure parent is writable
        test_probe = DATA_DIR / ".healthcheck_probe"
        try:
            test_probe.write_text("ok", encoding="utf-8")
            test_probe.unlink()
        except Exception as exc:
            sys.stderr.write(f"Healthcheck failed: disk not writable: {exc}\n")
            return 1
    print("Lakehouse container healthy.")
    return 0

def run_daemon():
    print("==> [DAEMON] Starting ferrous-lakehouse lifecycle manager...")
    do_init()

    running = True
    def sig_handler(sig, frame):
        nonlocal running
        print("==> [DAEMON] Received shutdown signal, terminating cleanly...")
        running = False

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    print("==> [DAEMON] Entering maintenance loop (hourly cycle)...")
    while running:
        try:
            # Sleep in short increments to respond quickly to signals
            for _ in range(3600):
                if not running:
                    break
                time.sleep(1)
            if running:
                do_maintain()
        except Exception as exc:
            print(f"==> [WARN] Maintenance cycle error: {exc}")

def main():
    parser = argparse.ArgumentParser(description="ferrous-lakehouse container entrypoint")
    parser.add_argument("--init", action="store_true", help="Initialize Delta Lake tables and exit")
    parser.add_argument("--maintain", action="store_true", help="Run maintenance (OPTIMIZE & VACUUM) and exit")
    parser.add_argument("--healthcheck", action="store_true", help="Run container healthcheck probe and exit")
    parser.add_argument("--daemon", action="store_true", help="Run as background lifecycle daemon")
    args = parser.parse_args()

    if args.healthcheck:
        sys.exit(do_healthcheck())
    elif args.init:
        sys.exit(do_init())
    elif args.maintain:
        sys.exit(do_maintain())
    else:
        run_daemon()

if __name__ == "__main__":
    main()
