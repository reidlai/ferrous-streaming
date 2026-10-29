#!/usr/bin/env python3
"""
init_tables.py: Delta Lake ACID Transaction Log & Storage Initializer.
Initializes table directory structures and valid _delta_log/00000000000000000000.json
commit files according to Delta Lake Protocol v1 (minReaderVersion: 1, minWriterVersion: 2).
Adheres to FR-24, AD-4, and AD-17.
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent

# Mapping from Arrow type names to Delta Lake schema types
ARROW_TO_DELTA_TYPE = {
    "utf8": "string",
    "bool": "boolean",
    "double": "double",
    "int": "long",
    "timestamp": "timestamp"
}

def convert_arrow_to_delta_schema(arrow_json_path: Path) -> tuple[str, list[str]]:
    """Converts Arrow JSON schema format to Delta Lake struct schemaString."""
    with open(arrow_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fields = []
    for f in data.get("fields", []):
        f_name = f["name"]
        raw_type = f["type"]
        type_name = raw_type.get("name", "utf8")
        delta_type = ARROW_TO_DELTA_TYPE.get(type_name, "string")
        
        # Check integer bit width
        if type_name == "int" and raw_type.get("bitWidth") == 32:
            delta_type = "integer"
        elif type_name == "int" and raw_type.get("bitWidth") == 64:
            delta_type = "long"
            
        fields.append({
            "name": f_name,
            "type": delta_type,
            "nullable": f.get("nullable", True),
            "metadata": f.get("metadata", {})
        })

    delta_struct = {
        "type": "struct",
        "fields": fields
    }
    partition_keys = data.get("metadata", {}).get("partition_keys", [])
    return json.dumps(delta_struct, separators=(",", ":")), partition_keys

def init_table(data_dir: Path, table_name: str, schema_file: Path) -> bool:
    """Initializes a single Delta Lake table directory with _delta_log."""
    table_dir = data_dir / table_name
    log_dir = table_dir / "_delta_log"
    commit_file = log_dir / "00000000000000000000.json"

    if commit_file.exists():
        print(f"  [SKIPPED] Table '{table_name}' already initialized at {table_dir}")
        return False

    log_dir.mkdir(parents=True, exist_ok=True)
    schema_string, partition_cols = convert_arrow_to_delta_schema(schema_file)
    now_ms = int(time.time() * 1000)

    # 1. Delta Protocol Action
    protocol_action = {
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    }

    # 2. Delta MetaData Action
    metadata_action = {
        "metaData": {
            "id": str(uuid.uuid4()),
            "name": table_name,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": schema_string,
            "partitionColumns": partition_cols,
            "configuration": {
                "delta.appendOnly": "true" if "events" in table_name or "benchmarks" in table_name else "false",
                "delta.autoOptimize.autoCompact": "true",
                "delta.autoOptimize.optimizeWrite": "true"
            },
            "createdTime": now_ms
        }
    }

    # 3. Delta CommitInfo Action
    commit_info_action = {
        "commitInfo": {
            "timestamp": now_ms,
            "operation": "CREATE TABLE",
            "operationParameters": {
                "isManaged": "false",
                "location": str(table_dir.as_posix()),
                "partitionBy": json.dumps(partition_cols)
            },
            "engineInfo": "ferrous-lakehouse-initializer/1.0.0",
            "readVersion": -1,
            "isolationLevel": "Serializable"
        }
    }

    # Delta transaction log files are NDJSON (Newline Delimited JSON)
    commit_lines = [
        json.dumps(protocol_action),
        json.dumps(metadata_action),
        json.dumps(commit_info_action)
    ]

    with open(commit_file, "w", encoding="utf-8") as f:
        f.write("\n".join(commit_lines) + "\n")

    print(f"  [INITIALIZED] Table '{table_name}' -> {commit_file}")
    return True

def main():
    parser = argparse.ArgumentParser(description="Initialize Delta Lake table storage and ACID logs.")
    parser.add_argument("--data-dir", type=str, default=str(REPO_ROOT / "data" / "delta"),
                        help="Root directory for Delta Lake tables")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    print("=" * 60)
    print(f"Initializing Delta Lake Storage Tier at: {data_dir}")
    print("=" * 60)

    # Core tables to initialize
    tables = [
        ("orders_raw_events", REPO_ROOT / "schemas" / "streaming" / "orders_raw_events.arrow.json"),
        ("orders_current_state", REPO_ROOT / "schemas" / "streaming" / "orders_current_state.arrow.json"),
        ("llm_benchmarks", REPO_ROOT / "schemas" / "benchmarks" / "llm_benchmarks.arrow.json")
    ]

    count = 0
    for name, schema_path in tables:
        if not schema_path.exists():
            print(f"ERROR: Schema not found: {schema_path}")
            sys.exit(1)
        if init_table(data_dir, name, schema_path):
            count += 1

    print("=" * 60)
    print(f"Lakehouse initialization complete. {count} new tables initialized.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
