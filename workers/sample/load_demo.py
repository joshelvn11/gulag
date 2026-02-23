#!/usr/bin/env python3
"""
Sample ETL load step.

Reads transformed records and appends a load event to a local history file.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load transformed demo data into local history.")
    parser.add_argument("--input", default="workers/sample/state/transformed_orders.json")
    parser.add_argument("--history-output", default="workers/sample/state/load_history.jsonl")
    parser.add_argument("--warehouse", default="sample-warehouse")
    parser.add_argument("--table", default="fact_orders_demo")
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--fail-if-empty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: transformed input file not found: {input_path}")
        return 1
    if args.sleep_seconds > 0:
        time.sleep(args.sleep_seconds)

    transformed = json.loads(input_path.read_text(encoding="utf-8"))
    records: List[Dict[str, object]] = transformed.get("records", [])
    if args.fail_if_empty and not records:
        print("Error: transformed dataset is empty and --fail-if-empty is enabled.")
        return 2

    event = {
        "loaded_at": datetime.now(tz=timezone.utc).isoformat(),
        "warehouse": args.warehouse,
        "table": args.table,
        "batch_id": transformed.get("batch_id"),
        "rows_loaded": len(records),
        "total_revenue": transformed.get("metrics", {}).get("total_revenue", 0.0),
        "avg_order_value": transformed.get("metrics", {}).get("avg_order_value", 0.0),
    }

    history_path = Path(args.history_output)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")

    print(
        f"Load complete: warehouse={args.warehouse}, table={args.table}, "
        f"rows={event['rows_loaded']}, history={history_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
