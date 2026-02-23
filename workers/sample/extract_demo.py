#!/usr/bin/env python3
"""
Sample ETL extract step.

Generates synthetic order records for local testing.
"""

from __future__ import annotations

import argparse
import json
import random
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
    parser = argparse.ArgumentParser(description="Generate synthetic order data for demo ETL.")
    parser.add_argument("--output", default="workers/sample/state/extracted_orders.json")
    parser.add_argument("--records", type=int, default=25)
    parser.add_argument("--source", default="demo-orders-api")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    return parser.parse_args()


def generate_records(records: int, seed: int) -> List[Dict[str, object]]:
    rng = random.Random(seed)
    channels = ["organic", "paid-search", "email", "direct", "social"]
    products = ["coffee", "tea", "mug", "grinder", "filter"]
    out: List[Dict[str, object]] = []
    now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
    for idx in range(records):
        amount = round(rng.uniform(9.5, 145.0), 2)
        item_count = rng.randint(1, 8)
        out.append(
            {
                "order_id": f"DEMO-{seed}-{idx + 1:05d}",
                "event_time": now,
                "channel": rng.choice(channels),
                "product": rng.choice(products),
                "item_count": item_count,
                "order_total": amount,
                "currency": "USD",
            }
        )
    return out


def main() -> int:
    args = parse_args()
    if monitor:
        monitor.info(
            "extract_demo started",
            source=args.source,
            records=args.records,
            output=args.output,
        )
    if args.records <= 0:
        if monitor:
            monitor.error("extract_demo invalid record count", records=args.records)
        print("Error: --records must be >= 1")
        return 1
    if args.sleep_seconds > 0:
        time.sleep(args.sleep_seconds)

    payload = {
        "batch_id": f"extract-{int(time.time())}",
        "source": args.source,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "record_count": args.records,
        "records": generate_records(args.records, args.seed),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if monitor:
        monitor.info(
            "extract_demo completed",
            source=args.source,
            record_count=args.records,
            output=str(output_path),
        )
    print(
        f"Extract complete: source={args.source}, records={args.records}, output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
