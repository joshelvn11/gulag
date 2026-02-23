# Chief Orchestrator Guide

This document is the complete reference for using `chief.py` and configuring `chief.yaml`.

## What Chief Is

`chief.py` is a YAML-driven script orchestrator and scheduler for this repository.

It provides:

- sequential script execution per job
- a human-friendly scheduling DSL
- config validation
- schedule preview with next run times
- daemon scheduling mode
- cron export for cron-compatible schedules
- monitor telemetry emission to a local monitor service (optional)

## File Locations

- Runner: `chief.py`
- Default config: `chief.yaml`
- Runtime log file: `chief.log`
- Worker monitor client: `monitor_client.py`
- Monitor service project: `monitor/`

## Requirements

Install dependencies from repo root:

```bash
python -m pip install -r requirements.txt
```

Important dependencies:

- `PyYAML`
- `croniter`

If you see environment mismatch issues, use:

```bash
python -m pip install -r requirements.txt
```

instead of plain `pip`.

## Quick Start

1. Validate configuration:

```bash
python chief.py validate --config chief.yaml
```

2. Preview schedules:

```bash
python chief.py preview --config chief.yaml
```

3. Run all enabled jobs once:

```bash
python chief.py run --config chief.yaml
```

4. Run daemon scheduler:

```bash
python chief.py daemon --config chief.yaml --poll-seconds 10
```

5. Export cron-compatible schedules:

```bash
python chief.py export-cron --config chief.yaml
```

6. Start the monitor service (optional observability API):

```bash
cd monitor
npm install
npm run dev
```

## CLI Commands

## `validate`

Validate YAML structure, script paths, schedule rules, and compilation mode.

```bash
python chief.py validate [--config PATH]
```

## `preview`

Shows:

- schedule description
- schedule mode (`pure_cron`, `hybrid`, `runtime_only`)
- cron equivalent when available
- bounds and exclusions
- scripts with timeout and args
- next N run times

```bash
python chief.py preview [--config PATH] [--job NAME] [--count N]
```

## `run`

Runs jobs immediately in YAML order (one-shot mode).

Behavior:

- default: runs all enabled jobs once
- `--job`: runs one job
- `--respect-schedule`: only runs selected job(s) if due now

```bash
python chief.py run [--config PATH] [--job NAME] [--respect-schedule]
```

## `daemon`

Starts the scheduler loop and dispatches due jobs continuously.

```bash
python chief.py daemon [--config PATH] [--poll-seconds N]
```

## `export-cron`

Prints cron lines for cron-compatible jobs and labels hybrid/runtime-only jobs.

```bash
python chief.py export-cron [--config PATH] [--job NAME]
```

## Command-Line Arguments Reference

Chief supports a global config flag plus command-specific flags.

## Global Flag

- `--config PATH`
  : path to YAML config file (default: `chief.yaml`)

You can pass `--config` either before or after the subcommand:

```bash
python chief.py --config chief.yaml validate
python chief.py validate --config chief.yaml
```

## `validate` Flags

- `--config PATH`
  : config file to validate

Example:

```bash
python chief.py validate --config chief.yaml
```

## `preview` Flags

- `--config PATH`
  : config file to preview
- `--job NAME`
  : preview only one job by name
- `--count N`
  : number of upcoming run times to show (default: `5`, must be `>= 1`)

Examples:

```bash
python chief.py preview
python chief.py preview --job daily
python chief.py preview --job daily --count 10
```

## `run` Flags

- `--config PATH`
  : config file to run
- `--job NAME`
  : run only one enabled job
- `--respect-schedule`
  : only run selected job(s) if currently due

Examples:

```bash
python chief.py run
python chief.py run --job daily
python chief.py run --job daily --respect-schedule
```

## `daemon` Flags

- `--config PATH`
  : config file for scheduler daemon
- `--poll-seconds N`
  : scheduler polling interval in seconds (default: `10`, must be `>= 1`)

Example:

```bash
python chief.py daemon --config chief.yaml --poll-seconds 10
```

## `export-cron` Flags

- `--config PATH`
  : config file to export from
- `--job NAME`
  : export only one job by name

Examples:

```bash
python chief.py export-cron
python chief.py export-cron --job daily
```

## Common CLI Behavior

- Unknown job names cause an error.
- `run` and `daemon` only act on enabled jobs.
- `preview` can show disabled jobs so you can inspect schedule output safely.
- `--respect-schedule` is useful when cron invokes `chief.py run` and you want runtime guards enforced.

## YAML Structure (`chief.yaml`)

Top-level schema:

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
  api_key: ""
  timeout_ms: 400
  buffer:
    max_events: 5000
    flush_interval_ms: 1000
    spool_file: .chief/telemetry_spool.jsonl
jobs:
  - name: example-job
    enabled: true
    working_dir: .
    stop_on_failure: true
    overlap: skip
    monitor:
      enabled: true
      check:
        enabled: true
        grace_seconds: 120
        alert_on_failure: true
        alert_on_miss: true
    schedule:
      frequency: daily
      time: "06:00"
    scripts:
      - path: scripts/example.py
        args: ["--flag", "value"]
        timeout: 1800
```

## Top-Level Keys

- `version`: config version (current usage: `1`)
- `defaults`: optional defaults applied to jobs
- `monitor`: optional monitor emitter settings for telemetry
- `jobs`: required non-empty list of job definitions

## `defaults` Keys

- `working_dir`: default working directory for scripts
- `stop_on_failure`: default job behavior on script failure
- `overlap`: default overlap policy (`skip`, `queue`, `parallel`)
- `timezone`: default schedule timezone (IANA name, example `America/New_York`)

## Job Keys

- `name` (required, unique)
- `enabled` (optional, default `true`)
- `working_dir` (optional, inherits `defaults.working_dir`)
- `stop_on_failure` (optional, inherits default)
- `overlap` (optional, inherits default)
- `schedule` (required)
- `scripts` (required, non-empty)
- `monitor` (optional, per-job telemetry/check settings)

## Script Keys

- `path` (required)
- `args` (optional)
- `timeout` (optional, default `3600` seconds)

`args` supports:

1. list form:

```yaml
args:
  - --start-date
  - 2026-01-01
  - --end-date
  - 2026-01-31
```

2. shell-style string form:

```yaml
args: --start-date 2026-01-01 --end-date 2026-01-31 --label "weekly run"
```

Notes:

- relative `path` values resolve against the job `working_dir`
- script path existence is validated at load time

## Monitor Configuration

Chief can emit lifecycle telemetry events to a local monitor service over HTTP.

Top-level monitor block:

- `enabled` (default `false`)
- `endpoint` (must start with `http://` or `https://`)
- `api_key` (optional; sent as `x-api-key`)
- `timeout_ms` (> 0)
- `buffer.max_events` (> 0)
- `buffer.flush_interval_ms` (> 0)
- `buffer.spool_file` (local fallback JSONL path)

Per-job monitor block:

- `enabled` (inherits top-level monitor `enabled`)
- `check.enabled`
- `check.grace_seconds` (>= 0)
- `check.alert_on_failure`
- `check.alert_on_miss`

Telemetry is best-effort and never blocks job execution.
If the endpoint is unavailable, events are queued/spooled and Chief logs warnings.

## Scheduling DSL

Each job must define exactly one `schedule.frequency`:

- `daily`
- `weekly`
- `monthly`
- `yearly`
- `interval`
- `custom`

## Global Schedule Modifiers

Allowed on any frequency:

- `timezone`: IANA timezone (defaults to job/default/system)
- `start`: ISO datetime lower bound
- `end`: ISO datetime upper bound
- `exclude`: list of `YYYY-MM-DD` dates

Important rules:

- naive `start`/`end` datetimes are interpreted in the schedule timezone
- named holidays are intentionally unsupported in v1
- execution outside `[start, end]` is skipped
- excluded dates are skipped

## Frequency: `daily`

Required:

- `time` (`HH:MM`, 24-hour)

Optional:

- `weekdays_only: true|false`

Examples:

```yaml
schedule:
  frequency: daily
  time: "14:30"
```

```yaml
schedule:
  frequency: daily
  time: "14:30"
  weekdays_only: true
```

## Frequency: `weekly`

Required:

- `day`
- `time`

`day` supports:

- single: `monday`
- comma list: `monday,wednesday,friday`
- range: `monday-friday`
- YAML list: `[monday, wednesday, friday]`

Example:

```yaml
schedule:
  frequency: weekly
  day: monday-friday
  time: "09:00"
```

## Frequency: `monthly`

Two valid styles:

1. Day of month:

```yaml
schedule:
  frequency: monthly
  day_of_month: 15
  time: "08:00"
```

2. Ordinal weekday:

```yaml
schedule:
  frequency: monthly
  ordinal: last
  day: friday
  time: "18:00"
```

Valid ordinals:

- `first`
- `second`
- `third`
- `fourth`
- `last`

Rule:

- do not mix `day_of_month` with `ordinal/day`

## Frequency: `yearly`

Required:

- `month`
- `day_of_month`
- `time`

`month` can be name (`january`) or number (`1`).

Example:

```yaml
schedule:
  frequency: yearly
  month: january
  day_of_month: 1
  time: "00:00"
```

## Frequency: `interval`

Required:

- `every` in `<number><unit>` format

Supported units:

- `m` minutes
- `h` hours
- `d` days

Examples:

```yaml
schedule:
  frequency: interval
  every: 5m
```

```yaml
schedule:
  frequency: interval
  every: 2h
```

Rules:

- `time` is not allowed in interval mode
- seconds intervals are unsupported in v1

## Frequency: `custom`

Power mode with labeled cron-like fields:

- `minute`
- `hour`
- `day_of_month`
- `month`
- `day_of_week`

At least one field is required.

Examples:

```yaml
schedule:
  frequency: custom
  minute: 0
  hour: 9
  day_of_week: monday-friday
```

```yaml
schedule:
  frequency: custom
  minute: "*/10"
  hour: "*"
```

## Compilation Modes

Chief compiles each schedule into one of:

- `pure_cron`: fully representable in cron
- `hybrid`: cron trigger plus runtime guard (for cases like monthly ordinal weekday)
- `runtime_only`: needs runtime scheduler semantics (for example `every: 90m`)

`preview` and `validate` show the compiled mode.

## Execution Semantics

## Script Execution Within a Job

- scripts run sequentially
- each script uses configured `timeout`
- args are passed exactly as parsed from YAML
- on failure:
  - if `stop_on_failure: true`, remaining scripts are skipped
  - if `false`, remaining scripts continue

## Daemon Behavior

- no catch-up on startup (cron-like behavior)
- global ordering is deterministic by YAML job order
- overlap policy is per job:
  - `skip`: skip trigger if already running
  - `queue`: allow one queued pending run
  - `parallel`: allow same-job concurrent runs only; other jobs stay globally sequential

## Telemetry Event Model

Chief emits these event types:

- `job.started`
- `script.started`
- `script.completed`
- `job.completed`
- `job.failed`
- `job.next_scheduled`
- `daemon.dispatch`
- `daemon.overlap_skipped`
- `daemon.queued_pending`

Worker scripts can emit custom events as:

- `worker.message`

Standard event levels:

- `DEBUG`
- `INFO`
- `WARN`
- `ERROR`
- `CRITICAL`

Correlation fields included when available:

- `run_id`
- `job_name`
- `script_path`
- `scheduled_for`

## Timezone and DST

- schedule evaluation is timezone-aware
- wall-clock semantics are used
- nonexistent spring-forward local times are skipped
- ambiguous fall-back local times run once

## Validation Rules and Common Errors

Examples of enforced rules:

- frequency is required and must be valid
- required fields per frequency must exist
- conflicting fields are rejected
- invalid `HH:MM` is rejected
- invalid timezone is rejected
- interval mode cannot include `time`
- monthly must be either `day_of_month` or `ordinal + day`
- script files must exist

Typical error text:

```text
Error: "monthly" requires either "day_of_month" or "ordinal + day".
```

## Cron Export Usage

Generate cron lines:

```bash
python chief.py export-cron --config chief.yaml
```

Use exported entries in crontab. Output includes:

- `CRON_TZ=<timezone>`
- cron expression + command for pure/hybrid schedules
- comments for runtime-only schedules

Hybrid schedules still require `chief run --respect-schedule` runtime guard to decide final execution.

## Practical Examples

## Example 1: Daily ETL with args

```yaml
jobs:
  - name: ga-daily
    enabled: true
    schedule:
      frequency: daily
      time: "06:00"
      timezone: America/New_York
    scripts:
      - path: scripts/google-analytics/google_analytics_to_supabase.py
        args:
          - --function
          - sessions_by_channel
          - --start-date
          - 2026-01-01
          - --end-date
          - 2026-01-31
        timeout: 1800
```

## Example 2: Monthly last Friday with exclusion

```yaml
jobs:
  - name: monthly-report
    enabled: true
    overlap: queue
    schedule:
      frequency: monthly
      ordinal: last
      day: friday
      time: "18:00"
      timezone: UTC
      exclude:
        - 2026-12-25
    scripts:
      - path: scripts/other/offer_report_to_supabase.py
        timeout: 1200
```

## Example 3: Runtime-only interval

```yaml
jobs:
  - name: rolling-check
    enabled: true
    schedule:
      frequency: interval
      every: 90m
    scripts:
      - path: scripts/weather/weather_to_supabase.py
        timeout: 600
```

## Worker Instrumentation (`monitor_client.py`)

Chief injects monitor context env vars into worker subprocesses:

- `CHIEF_MONITOR_ENDPOINT`
- `CHIEF_MONITOR_API_KEY`
- `CHIEF_RUN_ID`
- `CHIEF_JOB_NAME`
- `CHIEF_SCRIPT_PATH`
- `CHIEF_SCHEDULED_FOR`

Use the helper in worker scripts:

```python
from monitor_client import monitor

monitor.info("extract started", source="api-x")
monitor.warn("high latency", duration_ms=1800)
monitor.error("load failed", table="fact_orders", reason="constraint violation")
```

Helper methods:

- `monitor.debug(message, **meta)`
- `monitor.info(message, **meta)`
- `monitor.warn(message, **meta)`
- `monitor.error(message, **meta)`
- `monitor.critical(message, **meta)`

If endpoint config is missing/unavailable, helper calls no-op gracefully.

## Testing Chief

Run tests with pytest (from repo root):

```bash
python -m pytest -q tests/test_chief.py
```

Do not run pytest files directly with `python tests/test_chief.py`.

## Migration Notes

- Use `chief.py` + `chief.yaml` as the primary workflow.

## Troubleshooting

## `ModuleNotFoundError: No module named 'pytest'`

Use the same interpreter for install and run:

```bash
python -m pip install pytest
python -m pytest -q tests/test_chief.py
```

## `Missing required dependency: PyYAML` or `croniter`

```bash
python -m pip install -r requirements.txt
```

## Config validates but job does not run in `run --respect-schedule`

That is expected when the current timestamp is not due according to the compiled schedule.
Use `preview` to inspect next run times:

```bash
python chief.py preview --job <job-name>
```

## Monitor warnings in `chief.log`

If you see warnings like `Monitor emitter failed to send batch`, Chief is still running jobs.
This means monitor delivery is temporarily unavailable.

Checklist:

1. Start monitor service:

```bash
cd monitor
npm install
npm run dev
```

2. Confirm health endpoint:

```bash
curl -s http://127.0.0.1:7410/v1/health
```

3. Verify `monitor.endpoint` and optional `monitor.api_key` in `chief.yaml`.

## See Also

- `chief.yaml` for live configuration examples
- `monitor/README.md` for monitor API and local setup
