# Chief Orchestrator Demo

This project is a lightweight, YAML-driven job orchestrator centered on `chief.py`.
It includes a sample ETL workflow in `workers/sample` so you can validate scheduling and execution behavior locally.
It also includes an optional Cronitor-style telemetry monitor service in `monitor/` (TypeScript + Drizzle + SQLite).

## What This Repo Contains

- `chief.py`: orchestrator + scheduler CLI
- `chief.yaml`: active job configuration
- `CHIEF.md`: full command and config reference
- `workers/sample/`: demo ETL scripts (extract, transform, load, quality check)
- `monitor/`: telemetry ingest/status API service
- `monitor_client.py`: worker helper for sending custom monitor messages
- `tests/test_chief.py`: test coverage for parsing, scheduling, and execution semantics

## Quick Start

From the repository root:

```bash
python -m pip install -r requirements.txt

python chief.py validate --config chief.yaml
python chief.py preview --config chief.yaml
python chief.py run --config chief.yaml
```

Optional: start monitor API before running jobs:

```bash
cd monitor
npm install
npm run dev
```

Run the scheduler daemon:

```bash
python chief.py daemon --config chief.yaml --poll-seconds 10
```

Export cron-compatible schedules:

```bash
python chief.py export-cron --config chief.yaml
```

## Sample ETL Outputs

When sample jobs run, local artifacts are written to:

- `workers/sample/state/extracted_orders.json`
- `workers/sample/state/transformed_orders.json`
- `workers/sample/state/load_history.jsonl`

## Testing

```bash
python -m pytest -q tests/test_chief.py
```

## Documentation

- Full Chief guide: `CHIEF.md`
- Detailed monitor guide: `MONITOR.md`
- Monitor service guide: `monitor/README.md`
- Sample worker notes: `workers/sample/README.md`
