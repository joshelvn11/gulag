# Chief Monitor Guide

This document is the detailed operational guide for Chief observability/telemetry.

It covers:

- architecture and data flow
- setup and startup
- `chief.yaml` monitor configuration
- worker instrumentation with `monitor_client.py`
- monitor API endpoints and query usage
- check/alert behavior
- reliability semantics (buffer/spool/non-blocking)
- troubleshooting and runbook steps

## 1. Architecture

The monitor system has 3 parts:

1. Chief emitter (`chief.py`)
2. Worker helper (`monitor_client.py`)
3. Monitor service (`monitor`)

### 1.1 Data Flow

1. `chief.py` runs jobs/scripts.
2. Chief emits lifecycle telemetry to monitor HTTP ingest endpoints.
3. Chief injects monitor context env vars into worker subprocesses.
4. Workers call `monitor_client.monitor.info()/warn()/error()` to post `worker.message` events.
5. Monitor service stores events in SQLite and updates check/alert state.
6. Status/query APIs expose health, events, alerts, and per-job state.

## 2. Event Sources and Event Types

### 2.1 Chief Lifecycle Events

Chief emits:

- `job.started`
- `script.started`
- `script.completed`
- `job.completed`
- `job.failed`
- `job.next_scheduled`
- `daemon.dispatch`
- `daemon.overlap_skipped`
- `daemon.queued_pending`

### 2.2 Worker Custom Events

Workers emit:

- `worker.message`

### 2.3 Severity Levels

- `DEBUG`
- `INFO`
- `WARN`
- `ERROR`
- `CRITICAL`

### 2.4 Correlation Fields

When available, events include:

- `runId`
- `jobName`
- `scriptPath`
- `scheduledFor`

## 3. Setup

## 3.1 Prerequisites

- Python environment for Chief.
- Node/npm for monitor service.

From repo root:

```bash
python -m pip install -r requirements.txt
```

From monitor project:

```bash
cd monitor
npm install
```

## 3.2 Start the Monitor Service

```bash
cd monitor
npm run db:migrate
npm run dev
```

Default service URL:

- `http://127.0.0.1:7410`

Health check:

```bash
curl -s http://127.0.0.1:7410/v1/health
```

## 4. Chief Configuration (`chief.yaml`)

Use a top-level `monitor` block plus optional per-job overrides.

```yaml
monitor:
  enabled: true
  endpoint: http://127.0.0.1:7410
  api_key: ""
  timeout_ms: 400
  buffer:
    max_events: 5000
    flush_interval_ms: 1000
    spool_file: .chief/telemetry_spool.jsonl
```

Per-job:

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

### 4.1 Validation Rules

- `monitor.endpoint` must be `http://` or `https://`.
- `timeout_ms > 0`
- `buffer.max_events > 0`
- `buffer.flush_interval_ms > 0`
- `check.grace_seconds >= 0`

## 5. Non-Blocking Reliability Semantics

Chief monitor delivery is best-effort by design.

- Job execution is never blocked by telemetry failures.
- Events go into a bounded in-memory queue.
- Background sender flushes batches to `/v1/events/batch`.
- On send failure, events are spooled to JSONL (`spool_file`).
- Spool is replayed in background on later flush attempts.

Implication:

- If monitor is down, jobs still run; warnings appear in `chief.log`.

## 6. Worker Instrumentation (`monitor_client.py`)

Chief injects the following env vars into script subprocesses:

- `CHIEF_MONITOR_ENDPOINT`
- `CHIEF_MONITOR_API_KEY`
- `CHIEF_RUN_ID`
- `CHIEF_JOB_NAME`
- `CHIEF_SCRIPT_PATH`
- `CHIEF_SCHEDULED_FOR`

Worker usage:

```python
from monitor_client import monitor

monitor.info("step started", phase="extract")
monitor.warn("latency elevated", duration_ms=1800)
monitor.error("load failed", table="fact_orders")
```

Supported methods:

- `monitor.debug(message, **meta)`
- `monitor.info(message, **meta)`
- `monitor.warn(message, **meta)`
- `monitor.error(message, **meta)`
- `monitor.critical(message, **meta)`

Behavior:

- If endpoint is missing/unreachable, helper no-ops and returns `False`.
- Worker script behavior is unaffected.

## 7. Sample Workers (Already Wired)

These sample workers are monitor-enabled:

- `workers/sample/extract_demo.py`
- `workers/sample/transform_demo.py`
- `workers/sample/load_demo.py`
- `workers/sample/quality_check_demo.py`

Run sample pipeline:

```bash
python chief.py run --config chief.yaml --job sample-etl-pipeline
python chief.py run --config chief.yaml --job sample-quality-check
```

## 8. Monitor Service API

Base URL default: `http://127.0.0.1:7410`

If `MONITOR_API_KEY` is set, send `x-api-key` on ingest endpoints.

### 8.1 Ingest

- `POST /v1/events` (single event)
- `POST /v1/events/batch` (batch)

Single ingest example:

```bash
curl -s -X POST http://127.0.0.1:7410/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "sourceType":"chief",
    "eventType":"job.started",
    "level":"INFO",
    "message":"Job started",
    "jobName":"sample-etl-pipeline",
    "runId":"demo-1"
  }'
```

### 8.2 Health

- `GET /v1/health`

### 8.3 Status

- `GET /v1/status/summary`
- `GET /v1/status/jobs`
- `GET /v1/status/jobs/:jobName`

### 8.4 Query

- `GET /v1/alerts`
- `GET /v1/events`

Filters:

- alerts: `jobName`, `status`, `alertType`, `severity`, `limit`, `offset`
- events: `jobName`, `scriptPath`, `level`, `eventType`, `from`, `to`, `limit`, `offset`

Examples:

```bash
curl -s 'http://127.0.0.1:7410/v1/status/summary'
curl -s 'http://127.0.0.1:7410/v1/events?jobName=sample-etl-pipeline&limit=50'
curl -s 'http://127.0.0.1:7410/v1/alerts?status=OPEN'
```

## 9. SQLite Data Model

Monitor stores telemetry in SQLite (default `monitor/monitor.sqlite`).

Primary tables:

- `telemetry_events`
- `check_states`
- `alerts`
- `alert_deliveries`
- `service_config`

## 10. Check + Alert Semantics

## 10.1 Check State

Each job has a check state with:

- last heartbeat
- expected next run
- grace window
- status (`UP|LATE|DOWN`)
- consecutive failures

## 10.2 Missed-Run Alert

`MISSED` opens when:

- `now > expected_next_at + grace_seconds`
- and no heartbeat arrived in time

`RECOVERY` opens when a heartbeat arrives after a missed condition and missed alert closes.

## 10.3 Failure Alert

`FAILURE` opens/refreshes on `job.failed` (or failed completion).

On successful completion after failure:

- failure alert closes
- recovery alert may open

Deduping is keyed to avoid repeated open duplicates.

## 11. Service Configuration (Environment)

Monitor environment variables:

- `MONITOR_HOST` (default `127.0.0.1`)
- `MONITOR_PORT` (default `7410`)
- `MONITOR_DB_PATH` (default `./monitor.sqlite`)
- `MONITOR_API_KEY` (optional)
- `MONITOR_RETENTION_DAYS` (default `30`)
- `MONITOR_EVALUATOR_INTERVAL_SECONDS` (default `15`)
- `MONITOR_RETENTION_INTERVAL_SECONDS` (default `3600`)

## 12. Runbook

## 12.1 Start Everything Locally

Terminal A:

```bash
cd monitor
npm install
npm run db:migrate
npm run dev
```

Terminal B:

```bash
python chief.py validate --config chief.yaml
python chief.py run --config chief.yaml
```

Terminal C:

```bash
curl -s http://127.0.0.1:7410/v1/status/summary
curl -s 'http://127.0.0.1:7410/v1/events?limit=25'
```

## 12.2 If Monitor Is Down

Expected behavior:

- jobs still execute
- `chief.log` includes monitor send warnings
- events spool locally for replay

## 13. Troubleshooting

### `tsc: command not found` in `monitor/`

You have not installed node dependencies:

```bash
cd monitor
npm install
```

### No events in API

1. Check monitor service health:
   - `curl -s http://127.0.0.1:7410/v1/health`
2. Confirm `chief.yaml` monitor block is enabled and endpoint matches service.
3. Run one job and inspect `chief.log`.

### Worker events missing but chief lifecycle events exist

1. Ensure worker imports `monitor_client.monitor`.
2. Ensure worker script runs under Chief (so env vars are injected).
3. Confirm script does not exit before monitor calls.

### Unauthorized ingest responses

If monitor uses API key:

- set `monitor.api_key` in `chief.yaml`
- ensure key matches `MONITOR_API_KEY` in monitor service environment

## 14. Files of Record

- Chief runtime + emitter: `chief.py`
- Worker helper: `monitor_client.py`
- Monitor service API: `monitor/src/server.ts`
- Monitor state engine: `monitor/src/services/monitorService.ts`
- Monitor schema: `monitor/src/db/schema.ts`
- Active config: `chief.yaml`

