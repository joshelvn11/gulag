# Sample Workers

This directory contains demo ETL scripts intended for local testing with `chief.py`.

## Scripts

- `extract_demo.py`: generates synthetic order records
- `transform_demo.py`: filters/transforms records and computes metrics
- `load_demo.py`: appends load events to a local JSONL history file
- `quality_check_demo.py`: validates transformed output and latest load freshness

Each sample script emits optional `worker.message` telemetry using `monitor_client.py` when
`CHIEF_MONITOR_ENDPOINT` is present in environment (injected by `chief.py`).

State files are written to:

- `workers/sample/state/extracted_orders.json`
- `workers/sample/state/transformed_orders.json`
- `workers/sample/state/load_history.jsonl`

## Run with Chief

From repo root:

```bash
python chief.py validate --config chief.yaml
python chief.py run --config chief.yaml
python chief.py preview --config chief.yaml
```
