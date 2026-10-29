#!/usr/bin/env python3
"""
validate-schemas.py: Validates Delta Lake SQL DDL, Apache Arrow JSON schemas,
and audits for PII leaks. Adheres to NFR-8, AD-17, and AD-22.
"""

import json
import os
import re
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent

PII_FORBIDDEN_PATTERNS = [
    r"\bcpf\b",
    r"\bcredit_card\b",
    r"\bcard_number\b",
    r"\bcvv\b",
    r"\bcustomer_email\b",
    r"\bemail_address\b",
    r"\bpassword\b",
    r"\bsecret\b"
]

REQUIRED_TABLES = {
    "orders_raw_events": [
        "payload_id", "order_id", "event_timestamp", "payload", "ingested_at"
    ],
    "orders_current_state": [
        "order_id", "status", "credit_issued", "updated_at", "version"
    ],
    "llm_benchmarks": [
        "run_id", "model_id", "provider", "ttft_us", "tps", "itl_jitter_us",
        "tool_call_valid", "domain_invariant_valid", "pii_masked", "timestamp_utc"
    ]
}

def audit_pii(file_path: Path) -> list:
    violations = []
    content = file_path.read_text(encoding="utf-8")
    for pattern in PII_FORBIDDEN_PATTERNS:
        matches = re.findall(pattern, content, re.IGNORECASE)
        if matches:
            violations.append(f"Forbidden PII keyword '{pattern}' matched in {file_path.name}")
    return violations

def validate_arrow_schema(json_path: Path, table_name: str) -> list:
    errors = []
    if not json_path.exists():
        return [f"Missing Arrow schema file: {json_path}"]
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"Invalid JSON in {json_path}: {exc}"]
    
    fields = [f.get("name") for f in data.get("fields", [])]
    expected_fields = REQUIRED_TABLES.get(table_name, [])
    for exp in expected_fields:
        if exp not in fields:
            errors.append(f"Table '{table_name}' Arrow schema missing required field '{exp}'")
    return errors

def validate_sql_ddl(sql_path: Path, table_name: str) -> list:
    errors = []
    if not sql_path.exists():
        return [f"Missing SQL DDL file: {sql_path}"]
    content = sql_path.read_text(encoding="utf-8")
    expected_fields = REQUIRED_TABLES.get(table_name, [])
    for exp in expected_fields:
        if not re.search(rf"\b{exp}\b", content, re.IGNORECASE):
            errors.append(f"Table '{table_name}' SQL DDL missing required column '{exp}'")
    return errors

def main() -> int:
    print("=" * 60)
    print("Validating ferrous-lakehouse Schemas and PII Audit (NFR-8)")
    print("=" * 60)
    all_errors = []
    all_pii_violations = []

    # 1. Audit all files in schemas/ for PII
    schemas_dir = REPO_ROOT / "schemas"
    if not schemas_dir.is_dir():
        print(f"ERROR: {schemas_dir} directory not found.")
        return 1

    for file_path in schemas_dir.rglob("*"):
        if file_path.is_file() and file_path.suffix in [".sql", ".json"]:
            pii_errs = audit_pii(file_path)
            all_pii_violations.extend(pii_errs)

    # 2. Validate Core Streaming Tables
    streaming_dir = schemas_dir / "streaming"
    for table in ["orders_raw_events", "orders_current_state"]:
        sql_file = streaming_dir / f"{table}.sql"
        arrow_file = streaming_dir / f"{table}.arrow.json"
        all_errors.extend(validate_sql_ddl(sql_file, table))
        all_errors.extend(validate_arrow_schema(arrow_file, table))

    # 3. Validate Benchmark Table
    benchmarks_dir = schemas_dir / "benchmarks"
    llm_sql = benchmarks_dir / "llm_benchmarks.sql"
    llm_arrow = benchmarks_dir / "llm_benchmarks.arrow.json"
    all_errors.extend(validate_sql_ddl(llm_sql, "llm_benchmarks"))
    all_errors.extend(validate_arrow_schema(llm_arrow, "llm_benchmarks"))

    # 4. Report results
    if all_pii_violations:
        print("\n❌ PII AUDIT FAILED:")
        for v in all_pii_violations:
            print(f"  - {v}")
        return 1
    else:
        print("\n✅ PII Audit Passed: 0 customer PII references detected across all schemas.")

    if all_errors:
        print("\n❌ SCHEMA VALIDATION FAILED:")
        for e in all_errors:
            print(f"  - {e}")
        return 1

    print("✅ All SQL DDL and Arrow JSON schemas validated successfully.")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())
