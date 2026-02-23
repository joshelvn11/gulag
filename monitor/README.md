# Chief Monitor Service

TypeScript monitor service for Chief telemetry using Drizzle ORM + SQLite.

For full operational documentation, see:

- `MONITOR.md`

## Features

- Ingest telemetry events from Chief and workers
- Persist events/checks/alerts in SQLite
- Evaluate missed-heartbeat and failure alerts
- Auto-prune old telemetry events
- API-first status and query endpoints

## Setup

```bash
cd monitor
npm install
npm run db:migrate
npm run dev
```

Default endpoint:

- `http://127.0.0.1:7410`

## Integrated Web UI

The monitor now supports an integrated React dashboard served by the same Express service.

### Local Development (two terminals)

Terminal A (API):

```bash
cd monitor
npm run dev
```

Terminal B (UI):

```bash
cd monitor
npm run ui:install
npm run ui:dev
```

Vite dev server runs on `http://127.0.0.1:5173` and proxies `/v1` to monitor API at `http://127.0.0.1:7410`.

### Production Build + Serve

```bash
cd monitor
npm run ui:build
npm run build
npm run start
```

After UI build, Express serves the dashboard at:

- `http://127.0.0.1:7410/`

If UI build output is missing, monitor runs in API-only mode and logs a startup warning.

## Chief Integration

Enable telemetry in `chief.yaml`:

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

Chief emits lifecycle events and sample workers emit `worker.message` events via `monitor_client.py`.

## Environment Variables

- `MONITOR_HOST` (default `127.0.0.1`)
- `MONITOR_PORT` (default `7410`)
- `MONITOR_DB_PATH` (default `./monitor.sqlite`)
- `MONITOR_RETENTION_DAYS` (default `30`)
- `MONITOR_EVALUATOR_INTERVAL_SECONDS` (default `15`)
- `MONITOR_RETENTION_INTERVAL_SECONDS` (default `3600`)
- `MONITOR_API_KEY` (optional; if set, required via `x-api-key` for ingest endpoints)

## API

### Ingest

- `POST /v1/events`
- `POST /v1/events/batch`

### Status

- `GET /v1/health`
- `GET /v1/status/summary`
- `GET /v1/status/jobs`
- `GET /v1/status/jobs/:jobName`

### Query

- `GET /v1/alerts`
- `GET /v1/events`

## Curl Examples

```bash
curl -s http://127.0.0.1:7410/v1/health

curl -s -X POST http://127.0.0.1:7410/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"sourceType":"chief","eventType":"job.started","level":"INFO","message":"Job started","jobName":"sample-etl-pipeline","runId":"demo-1"}'

curl -s 'http://127.0.0.1:7410/v1/events?jobName=sample-etl-pipeline&limit=20'

curl -s 'http://127.0.0.1:7410/v1/alerts?status=OPEN'
```
