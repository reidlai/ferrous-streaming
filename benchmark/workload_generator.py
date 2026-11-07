#!/usr/bin/env python3
"""
workload_generator.py: Synthetic workload generator for HPC-MQBench in FerrousCX (Story 3.11, FR-32).

Generates fixed-size or templated JSON records for public reproducible benchmarking
without bundling proprietary or static datasets. Records contain distinct identifiers
(run_id, seq, record_id) and high-precision timestamps for stage-by-stage reconciliation.
"""

import argparse
import json
import os
import sys
import time
import uuid
from typing import Dict, Any, Iterator, List


def generate_record(
    run_id: str,
    seq: int,
    payload_size_bytes: int = 1024,
    partition: int = 0,
) -> Dict[str, Any]:
    """Generate a single workload record conforming to HPC-MQBench workload shape."""
    now_us = int(time.time() * 1_000_000)
    record_id = f"{run_id}:{seq:08d}"
    
    # Base structure without padding
    base_record = {
        "run_id": run_id,
        "record_id": record_id,
        "seq": seq,
        "partition": partition,
        "client_timestamp_us": now_us,
        "payload_size_bytes": payload_size_bytes,
        "data": ""
    }
    
    # Calculate required padding to achieve exact payload_size_bytes
    base_json = json.dumps(base_record)
    target_overhead = len(base_json.encode("utf-8"))
    padding_needed = max(0, payload_size_bytes - target_overhead)
    
    base_record["data"] = "X" * padding_needed
    return base_record


def generate_workload_stream(
    run_id: str,
    total_records: int,
    payload_size_bytes: int = 1024,
    num_partitions: int = 4,
) -> Iterator[Dict[str, Any]]:
    """Yield a stream of benchmark workload records across configured partitions."""
    for seq in range(total_records):
        partition = seq % num_partitions
        yield generate_record(
            run_id=run_id,
            seq=seq,
            payload_size_bytes=payload_size_bytes,
            partition=partition,
        )


def main():
    parser = argparse.ArgumentParser(description="HPC-MQBench Synthetic Workload Generator (FR-32)")
    parser.add_argument("--count", type=int, default=1000, help="Number of records to generate (default: 1000)")
    parser.add_argument("--payload-size", type=int, default=1024, help="Target payload size in bytes (default: 1024)")
    parser.add_argument("--partitions", type=int, default=4, help="Number of partitions (default: 4)")
    parser.add_argument("--run-id", type=str, default="", help="Optional run ID (defaults to new UUID)")
    parser.add_argument("--output", type=str, default="-", help="Output file path or '-' for stdout")
    
    args = parser.parse_args()
    run_id = args.run_id or f"run-{uuid.uuid4().hex[:12]}"
    
    out_file = sys.stdout if args.output == "-" else open(args.output, "w", encoding="utf-8")
    try:
        for record in generate_workload_stream(
            run_id=run_id,
            total_records=args.count,
            payload_size_bytes=args.payload_size,
            num_partitions=args.partitions,
        ):
            out_file.write(json.dumps(record) + "\n")
    finally:
        if args.output != "-":
            out_file.close()


if __name__ == "__main__":
    main()

