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
    # No application-level auth: access is bounded by the container and the Delta volume.
    print("==> [MAINTAIN] Running compaction and log cleanup...")
    tables = ["orders_current_state", "orders_raw_events", "llm_benchmarks"]
    errors = 0
    for t in tables:
        t_dir = DATA_DIR / t
        if t_dir.is_dir():
            opt_res = run_optimize(t_dir, target_mb=128)
            if opt_res.get("status") == "error":
                print(f"  [ERROR] OPTIMIZE failed on {t}: {opt_res.get('message')}")
                errors += 1
            vac_res = run_vacuum(t_dir, retention_hours=168)
            if vac_res.get("status") == "error":
                print(f"  [ERROR] VACUUM failed on {t}: {vac_res.get('message')}")
                errors += 1
    print("==> [MAINTAIN] Maintenance completed.")
    return 1 if errors > 0 else 0

def do_healthcheck() -> int:
    import json
    # 1. Verify DATA_DIR is accessible and readable
    if not DATA_DIR.exists():
        sys.stderr.write(f"Healthcheck failed: {DATA_DIR} does not exist\n")
        return 1

    # 2. Verify disk writability
    probe_file = DATA_DIR / f".healthcheck_probe_{os.getpid()}"
    try:
        probe_file.write_text("ok", encoding="utf-8")
    except Exception as exc:
        sys.stderr.write(f"Healthcheck failed: storage not writable: {exc}\n")
        return 1
    finally:
        try:
            if probe_file.exists():
                probe_file.unlink()
        except Exception:
            pass

    # 3. Verify Delta Lake log and schema integrity on core tables
    core_tables = ["orders_current_state", "orders_raw_events", "llm_benchmarks"]
    for table_name in core_tables:
        log_file = DATA_DIR / table_name / "_delta_log" / "00000000000000000000.json"
        if not log_file.exists():
            sys.stderr.write(f"Healthcheck failed: missing Delta commit log for table '{table_name}'\n")
            return 1
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                actions = [json.loads(line) for line in f if line.strip()]
            action_keys = {list(a.keys())[0] for a in actions}
            if not {"protocol", "metaData", "commitInfo"}.issubset(action_keys):
                sys.stderr.write(f"Healthcheck failed: incomplete actions in '{table_name}' commit 0\n")
                return 1
        except Exception as exc:
            sys.stderr.write(f"Healthcheck failed: invalid transaction log in '{table_name}': {exc}\n")
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
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--init", action="store_true", help="Initialize Delta Lake tables and exit")
    group.add_argument("--maintain", action="store_true", help="Run maintenance (OPTIMIZE & VACUUM) and exit")
    group.add_argument("--healthcheck", action="store_true", help="Run container healthcheck probe and exit")
    group.add_argument("--daemon", action="store_true", help="Run as background lifecycle daemon")
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
