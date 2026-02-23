#!/usr/bin/env python3
"""
Sample ETL transform step.

Reads extracted records and computes transformed facts + summary metrics.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from monitor_client import monitor
except Exception:  # pragma: no cover - optional dependency path
    monitor = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform synthetic order data for demo ETL.")
    parser.add_argument("--input", default="workers/sample/state/extracted_orders.json")
    parser.add_argument("--output", default="workers/sample/state/transformed_orders.json")
    parser.add_argument("--min-order-total", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    return parser.parse_args()


def channel_counts(records: List[Dict[str, object]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for rec in records:
        channel = str(rec.get("channel", "unknown"))
        counts[channel] = counts.get(channel, 0) + 1
    return counts


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if monitor:
        monitor.info(
            "transform_demo started",
            input=args.input,
            output=args.output,
            min_order_total=args.min_order_total,
        )
    if not input_path.exists():
        if monitor:
            monitor.error("transform_demo input missing", input=str(input_path))
        print(f"Error: input file not found: {input_path}")
        return 1
    if args.sleep_seconds > 0:
        time.sleep(args.sleep_seconds)

    raw = json.loads(input_path.read_text(encoding="utf-8"))
    records = raw.get("records", [])
    if not isinstance(records, list):
        if monitor:
            monitor.error("transform_demo invalid payload", input=str(input_path))
        print("Error: invalid extracted payload. 'records' must be a list.")
        return 1

    filtered = [r for r in records if float(r.get("order_total", 0.0)) >= args.min_order_total]
    total_revenue = round(sum(float(r["order_total"]) for r in filtered), 2) if filtered else 0.0
    avg_order = round(total_revenue / len(filtered), 2) if filtered else 0.0

    transformed = {
        "batch_id": raw.get("batch_id"),
        "source": raw.get("source"),
        "transformed_at": datetime.now(tz=timezone.utc).isoformat(),
        "min_order_total": args.min_order_total,
        "input_record_count": len(records),
        "output_record_count": len(filtered),
        "metrics": {
            "total_revenue": total_revenue,
            "avg_order_value": avg_order,
            "orders_by_channel": channel_counts(filtered),
        },
        "records": filtered,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(transformed, indent=2), encoding="utf-8")

    if monitor:
        monitor.info(
            "transform_demo completed",
            input_record_count=len(records),
            output_record_count=len(filtered),
            total_revenue=total_revenue,
            output=str(output_path),
        )
    print(
        "Transform complete: "
        f"input={len(records)}, output={len(filtered)}, revenue={total_revenue}, output_file={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
