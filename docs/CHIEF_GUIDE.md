# Chief Guide

This is a quick-start, hands-on guide for using Chief as a YAML-based job runner and scheduler.

For full reference, see `docs/CHIEF.md`.

## 1. What Chief Does

Chief runs Python scripts in ordered jobs, with:

- YAML configuration (`chief/chief.yaml`)
- human-readable schedules
- one-shot runs and daemon scheduling
- optional monitor telemetry

## 2. Prerequisites

- Python 3.9+
- `pip`
- Repo cloned locally

Install dependencies from repo root:

```bash
python -m pip install -r requirements.txt
```

## 3. Quick Start (5 Minutes)

Run these commands from repo root:

1. Validate your config

```bash
python chief/chief.py validate --config chief/chief.yaml
```

2. Preview upcoming runs

```bash
python chief/chief.py preview --config chief/chief.yaml
```

3. Run all enabled jobs once

```bash
python chief/chief.py run --config chief/chief.yaml
```

4. Run one job only

```bash
python chief/chief.py run --config chief/chief.yaml --job sample-etl-pipeline
```

5. Run daemon scheduler

```bash
python chief/chief.py daemon --config chief/chief.yaml --poll-seconds 10
```

## 4. Command Guide

## `validate`

Checks YAML structure, script paths, and schedule rules.

```bash
python chief/chief.py validate --config chief/chief.yaml
```

## `preview`

Shows schedule description, next run times, and cron equivalent when available.

```bash
python chief/chief.py preview --config chief/chief.yaml --count 5
python chief/chief.py preview --config chief/chief.yaml --job sample-etl-pipeline
```

## `run`

Runs selected jobs once.

```bash
python chief/chief.py run --config chief/chief.yaml
python chief/chief.py run --config chief/chief.yaml --job sample-quality-check
```

Use `--respect-schedule` to only run jobs that are due now:

```bash
python chief/chief.py run --config chief/chief.yaml --respect-schedule
```

## `daemon`

Starts the continuous scheduler loop.

```bash
python chief/chief.py daemon --config chief/chief.yaml --poll-seconds 10
```

## `export-cron`

Exports cron lines for cron-compatible jobs.

```bash
python chief/chief.py export-cron --config chief/chief.yaml
```

## 5. `chief.yaml` Configuration

Main file: `chief/chief.yaml`

Top-level keys:

- `version`
- `defaults`
- `monitor`
- `jobs`

Minimal shape:

```yaml
version: 1
defaults:
  working_dir: .
  stop_on_failure: true
  overlap: skip
  timezone: UTC
monitor:
  enabled: true
  endpoint: http://127.0.0.1:7410
  heartbeat_seconds: 15
jobs:
  - name: my-job
    schedule:
      frequency: daily
      time: "09:00"
    scripts:
      - path: workers/sample/extract_demo.py
```

## Job Options

Per job:

- `name` (required, unique)
- `enabled` (default `true`)
- `working_dir`
- `stop_on_failure` (default `true`)
- `overlap`: `skip | queue | parallel`
- `schedule` (required)
- `scripts` (required, non-empty)
- `monitor` (optional override)

## Script Options

Per script:

- `path` (required)
- `args` (optional list or shell-style string)
- `timeout` (seconds, optional)

Examples:

```yaml
scripts:
  - path: workers/sample/load_demo.py
    args:
      - --input
      - workers/sample/state/transformed_orders.json
    timeout: 120
```

```yaml
scripts:
  - path: workers/sample/transform_demo.py
    args: --input in.json --output out.json
```

## 6. Schedule Options (Friendly DSL)

Each job must define exactly one `frequency`:

- `daily`
- `weekly`
- `monthly`
- `yearly`
- `interval`
- `custom`

Quick examples:

```yaml
schedule:
  frequency: daily
  time: "14:30"
```

```yaml
schedule:
  frequency: weekly
  day: monday-friday
  time: "09:00"
```

```yaml
schedule:
  frequency: interval
  every: 5m
```

Global schedule modifiers (all frequencies):

- `timezone`
- `start`
- `end`
- `exclude` (list of `YYYY-MM-DD`)

## 7. Worker Monitor Helper (`monitor_client.py`)

If monitor telemetry is enabled, worker scripts can send custom messages using:

- `chief/monitor_client.py`

Available methods:

- `monitor.debug(message, **meta)`
- `monitor.info(message, **meta)`
- `monitor.warn(message, **meta)`
- `monitor.error(message, **meta)`
- `monitor.critical(message, **meta)`

Minimal worker example:

```python
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from monitor_client import monitor

monitor.info("worker started", step="extract", source="orders-api")
monitor.warn("slow upstream response", latency_ms=1450)
```

Chief injects monitor context into each script automatically:

- `CHIEF_MONITOR_ENDPOINT`
- `CHIEF_MONITOR_API_KEY`
- `CHIEF_RUN_ID`
- `CHIEF_JOB_NAME`
- `CHIEF_SCRIPT_PATH`
- `CHIEF_SCHEDULED_FOR`

Notes:

- When script is run through Chief, you usually do not need to configure anything in the worker.
- If script is run directly (outside Chief), you can set `CHIEF_MONITOR_ENDPOINT` manually.
- The helper is best-effort and returns `False` on network failure; it will not crash your script.

## 8. Tutorial: Add a New Job

1. Create your worker script under `chief/workers/`.
2. Add a new `jobs:` entry in `chief/chief.yaml`.
3. (Optional) add monitor helper messages in your script.
4. Run:

```bash
python chief/chief.py validate --config chief/chief.yaml
python chief/chief.py preview --config chief/chief.yaml --job your-job-name
python chief/chief.py run --config chief/chief.yaml --job your-job-name
```

5. If successful, run it continuously with:

```bash
python chief/chief.py daemon --config chief/chief.yaml
```

## 9. Troubleshooting

- `Config file not found`: check `--config` path.
- `Script file does not exist`: fix script `path` or `working_dir`.
- `ModuleNotFoundError`: install dependencies with `python -m pip install -r requirements.txt`.
- Nothing runs in `run --respect-schedule`: job may not be due at current time.

## 10. Next Steps

- Enable monitor telemetry in `chief/chief.yaml` to get UI/alerts.
- Use `export-cron` for cron-based deployment.
- See `docs/CHIEF.md` for full rule-by-rule schedule validation details.
