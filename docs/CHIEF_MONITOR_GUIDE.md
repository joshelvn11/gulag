# Chief Monitor Guide

This is a practical setup and operations guide for Chief Monitor (API + web UI + alerts).

For full technical reference, see `docs/MONITOR.md` and `monitor/README.md`.

## 1. What Chief Monitor Does

Chief Monitor receives telemetry from Chief and workers, stores it in SQLite, and gives you:

- live status summary
- job health tracking
- alert feed (`FAILURE`, `MISSED`, `RECOVERY`)
- event explorer
- manual alert close controls in UI/API

## 2. Prerequisites

- Node.js and npm
- Chief configured in `chief/chief.yaml`

## 3. Quick Start (Monitor + UI)

From repo root:

1. Install monitor dependencies

```bash
cd monitor
npm install
```

2. Run DB migrations

```bash
npm run db:migrate
```

3. Start monitor API service

```bash
npm run dev
```

4. Open:

- Health: `http://127.0.0.1:7410/v1/health`
- Dashboard UI: `http://127.0.0.1:7410/`

5. In another terminal, run Chief jobs to generate telemetry:

```bash
python chief/chief.py run --config chief/chief.yaml
```

## 4. Monitor UI Pages

- `/` System Overview
  - check totals, alert totals, chief online/offline status
- `/jobs`
  - current health per job
- `/jobs/:jobName`
  - check state + recent events + open alerts
- `/alerts`
  - filter alerts and manually close open alerts
- `/events`
  - filter/search raw telemetry events

## 5. Local Dev Workflow (Two Terminals)

Terminal A (API):

```bash
cd monitor
npm run dev
```

Terminal B (UI hot reload):

```bash
cd monitor
npm run ui:install
npm run ui:dev
```

UI dev server runs on `http://127.0.0.1:5173` and proxies `/v1` calls to monitor API.

## 6. Production Build Workflow

```bash
cd monitor
npm run ui:build
npm run build
npm run start
```

After build, UI is served by Express at `http://127.0.0.1:7410/`.

## 7. Chief Configuration for Monitor

Edit `chief/chief.yaml`:

```yaml
monitor:
  enabled: true
  endpoint: http://127.0.0.1:7410
  api_key: ""
  timeout_ms: 400
  heartbeat_seconds: 15
  buffer:
    max_events: 5000
    flush_interval_ms: 1000
    spool_file: .chief/telemetry_spool.jsonl
```

Per-job monitor/check override example:

```yaml
jobs:
  - name: sample-etl-pipeline
    monitor:
      enabled: true
      check:
        enabled: true
        grace_seconds: 120
        alert_on_failure: true
        alert_on_miss: true
```

## 8. Worker Monitor Helper (`monitor_client.py`)

Workers can post custom monitor messages using:

- `chief/monitor_client.py`

Recommended pattern in worker scripts:

```python
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from monitor_client import monitor
```

Send messages with any metadata fields:

```python
monitor.info("extract started", source="orders-api")
monitor.warn("row quality warning", bad_rows=3)
monitor.error("load failed", table="fact_orders", reason="duplicate key")
```

Supported helper methods:

- `monitor.debug(...)`
- `monitor.info(...)`
- `monitor.warn(...)`
- `monitor.error(...)`
- `monitor.critical(...)`

Chief automatically injects context env vars when scripts run via Chief:

- `CHIEF_MONITOR_ENDPOINT`
- `CHIEF_MONITOR_API_KEY`
- `CHIEF_RUN_ID`
- `CHIEF_JOB_NAME`
- `CHIEF_SCRIPT_PATH`
- `CHIEF_SCHEDULED_FOR`

If you run a worker directly (not through Chief), set at least:

```bash
export CHIEF_MONITOR_ENDPOINT=http://127.0.0.1:7410
```

Then run your script normally.

The helper is non-blocking/best-effort from the worker perspective:

- returns `False` on send failure
- does not throw hard failures for network errors

## 9. Monitor Service Environment Variables

Set these when starting `monitor`:

- `MONITOR_HOST` (default `127.0.0.1`)
- `MONITOR_PORT` (default `7410`)
- `MONITOR_DB_PATH` (default `./monitor.sqlite`)
- `MONITOR_API_KEY` (optional; secures ingest endpoints)
- `MONITOR_RETENTION_DAYS` (default `30`)
- `MONITOR_EVALUATOR_INTERVAL_SECONDS` (default `15`)
- `MONITOR_RETENTION_INTERVAL_SECONDS` (default `3600`)

Example:

```bash
MONITOR_API_KEY=my-secret MONITOR_PORT=7410 npm run dev
```

## 10. API Key Behavior (Important)

- API key is only checked on ingest endpoints:
  - `POST /v1/events`
  - `POST /v1/events/batch`
- Read endpoints (`/v1/status/*`, `/v1/alerts`, `/v1/events`) are open in v1.
- If you set `MONITOR_API_KEY`, set matching `monitor.api_key` in `chief/chief.yaml`.

## 11. Alert Lifecycle

- `FAILURE` opens on failed run, closes after later successful run.
- `MISSED` opens when expected heartbeat is missed beyond grace window, closes on next heartbeat.
- `RECOVERY` opens when a job recovers, then auto-closes:
  - on next heartbeat for that job, or
  - after ~15 minutes (TTL fallback).

## 12. Manual Alert Close

You can close open alerts in two ways:

1. UI:
- Alerts page (`/alerts`) "Close" button
- Job detail page (`/jobs/:jobName`) "Close" button

2. API:

```bash
curl -s -X POST http://127.0.0.1:7410/v1/alerts/1/close \
  -H 'Content-Type: application/json' \
  -d '{"reason":"manual"}'
```

## 13. Useful API Endpoints

- `GET /v1/health`
- `GET /v1/status/summary`
- `GET /v1/status/jobs`
- `GET /v1/status/jobs/:jobName`
- `GET /v1/alerts`
- `GET /v1/events`
- `POST /v1/alerts/:alertId/close`

## 14. Tutorial: End-to-End Alert Flow

1. Start monitor (`npm run dev` in `monitor/`).
2. Run a job with Chief:

```bash
python chief/chief.py run --config chief/chief.yaml --job sample-etl-pipeline
```

3. Open `/events` and confirm telemetry is arriving.
4. Force a script failure in a job, run again, then check `/alerts` for `FAILURE`.
5. Fix and rerun job; confirm recovery behavior.
6. Use "Close" in UI to manually close any remaining open alert.

## 15. Troubleshooting

- No telemetry in UI:
  - confirm monitor service is running on `endpoint`
  - confirm `monitor.enabled: true` in `chief/chief.yaml`
  - check `chief/chief.log` for emitter warnings
- Ingest 401 errors:
  - mismatch between `MONITOR_API_KEY` and `monitor.api_key`
- UI not loading on `/`:
  - run `npm run ui:build` in `monitor/` before `npm run start`
