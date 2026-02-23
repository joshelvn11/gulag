#!/usr/bin/env python3
"""
Sample ETL quality check step.

Validates that transformed data and latest load event meet simple expectations.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality checks against sample ETL artifacts.")
    parser.add_argument("--transformed-file", default="workers/sample/state/transformed_orders.json")
    parser.add_argument("--history-file", default="workers/sample/state/load_history.jsonl")
    parser.add_argument("--max-age-hours", type=int, default=24)
    parser.add_argument("--min-records", type=int, default=1)
    parser.add_argument("--warn-average-below", type=float, default=25.0)
    return parser.parse_args()


def read_latest_history_event(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        return None
    return json.loads(lines[-1])


def main() -> int:
    args = parse_args()
    transformed_path = Path(args.transformed_file)
    history_path = Path(args.history_file)

    if not transformed_path.exists():
        print(f"Error: transformed file not found: {transformed_path}")
        return 1

    transformed = json.loads(transformed_path.read_text(encoding="utf-8"))
    out_count = int(transformed.get("output_record_count", 0))
    avg_order = float(transformed.get("metrics", {}).get("avg_order_value", 0.0))
    if out_count < args.min_records:
        print(
            f"Error: transformed output_record_count={out_count} is below min-records={args.min_records}"
        )
        return 2

    latest = read_latest_history_event(history_path)
    if latest is None:
        print(f"Error: no load history event found in {history_path}")
        return 3

    loaded_at_raw = latest.get("loaded_at")
    if not isinstance(loaded_at_raw, str):
        print("Error: latest load event missing loaded_at")
        return 4
    loaded_at = datetime.fromisoformat(loaded_at_raw)
    if loaded_at.tzinfo is None:
        loaded_at = loaded_at.replace(tzinfo=timezone.utc)
    age = datetime.now(tz=timezone.utc) - loaded_at.astimezone(timezone.utc)
    if age > timedelta(hours=args.max_age_hours):
        print(
            f"Error: latest load is stale (age={age}, max_age_hours={args.max_age_hours})"
        )
        return 5

    status = "WARN" if avg_order < args.warn_average_below else "OK"
    print(
        f"{status}: quality checks passed, records={out_count}, avg_order={avg_order:.2f}, "
        f"latest_load_age={age}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
