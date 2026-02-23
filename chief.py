#!/usr/bin/env python3
"""
chief.py

YAML-driven orchestrator and scheduler for ETL scripts.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import yaml
except ImportError:  # pragma: no cover - dependency check at runtime
    yaml = None

try:
    from croniter import croniter
except ImportError:  # pragma: no cover - dependency check at runtime
    croniter = None


LOG_FILE = "chief.log"
DEFAULT_CONFIG = "chief.yaml"
DEFAULT_TIMEOUT_SECONDS = 3600
DEFAULT_PREVIEW_COUNT = 5
DEFAULT_POLL_SECONDS = 10
DEFAULT_MONITOR_ENDPOINT = "http://127.0.0.1:7410"
DEFAULT_MONITOR_TIMEOUT_MS = 400
DEFAULT_MONITOR_BUFFER_MAX_EVENTS = 5000
DEFAULT_MONITOR_BUFFER_FLUSH_MS = 1000
DEFAULT_MONITOR_SPOOL_FILE = ".chief/telemetry_spool.jsonl"

DAY_NAME_TO_CRON = {
    "sunday": 0,
    "monday": 1,
    "tuesday": 2,
    "wednesday": 3,
    "thursday": 4,
    "friday": 5,
    "saturday": 6,
}
CRON_TO_DAY_NAME = {v: k for k, v in DAY_NAME_TO_CRON.items()}
MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
ORDINAL_TO_INDEX = {
    "first": 0,
    "second": 1,
    "third": 2,
    "fourth": 3,
    "last": -1,
}
VALID_FREQUENCIES = {"daily", "weekly", "monthly", "yearly", "interval", "custom"}
VALID_OVERLAPS = {"skip", "queue", "parallel"}
INTERVAL_RE = re.compile(r"^(\d+)([smhd])$")
HHMM_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
CRON_FIELD_RE = re.compile(r"^[0-9*,/\-]+$")


class ChiefError(Exception):
    """Base error for chief."""


class ConfigError(ChiefError):
    """Config validation error."""


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("chief")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


logger = setup_logging()
UTC = timezone.utc


@dataclass(frozen=True)
class ScriptSpec:
    path: str
    args: List[str]
    timeout: int
    resolved_path: Path


@dataclass(frozen=True)
class ScheduleSpec:
    frequency: str
    raw: Dict[str, Any]
    timezone: ZoneInfo
    timezone_name: str
    start: Optional[datetime]
    end: Optional[datetime]
    exclude_dates: Set[date]


@dataclass(frozen=True)
class JobSpec:
    name: str
    enabled: bool
    working_dir: Path
    stop_on_failure: bool
    overlap: str
    scripts: List[ScriptSpec]
    schedule: ScheduleSpec
    monitor: "JobMonitorSettings" = field(default_factory=lambda: JobMonitorSettings.default())


@dataclass(frozen=True)
class CompiledSchedule:
    kind: str  # pure_cron | hybrid | runtime_only
    cron_expr: Optional[str]
    guard: Callable[[datetime], bool]
    description: str
    timezone: ZoneInfo
    timezone_name: str
    start: Optional[datetime]
    end: Optional[datetime]
    exclude_dates: Set[date]
    interval: Optional[timedelta]
    interval_text: Optional[str]


@dataclass(frozen=True)
class JobRuntime:
    spec: JobSpec
    compiled: CompiledSchedule
    index: int


@dataclass
class ScriptRunResult:
    script: ScriptSpec
    success: bool
    return_code: int
    duration_seconds: float
    stdout: str
    stderr: str
    error: Optional[str] = None


@dataclass
class JobRunResult:
    job_name: str
    success: bool
    script_results: List[ScriptRunResult]
    started_at: datetime
    ended_at: datetime
    scheduled_for: Optional[datetime]


@dataclass
class TriggerEvent:
    job_name: str
    scheduled_for: datetime


@dataclass
class JobState:
    next_fire: Optional[datetime]
    running_count: int = 0
    queued_pending: bool = False


@dataclass(frozen=True)
class MonitorBufferSettings:
    max_events: int
    flush_interval_ms: int
    spool_file: Path


@dataclass(frozen=True)
class MonitorSettings:
    enabled: bool
    endpoint: str
    api_key: str
    timeout_ms: int
    buffer: MonitorBufferSettings


@dataclass(frozen=True)
class MonitorCheckSettings:
    enabled: bool
    grace_seconds: int
    alert_on_failure: bool
    alert_on_miss: bool

    @staticmethod
    def default() -> "MonitorCheckSettings":
        return MonitorCheckSettings(
            enabled=True,
            grace_seconds=120,
            alert_on_failure=True,
            alert_on_miss=True,
        )


@dataclass(frozen=True)
class JobMonitorSettings:
    enabled: bool
    check: MonitorCheckSettings

    @staticmethod
    def default(enabled: bool = False) -> "JobMonitorSettings":
        return JobMonitorSettings(
            enabled=enabled,
            check=MonitorCheckSettings(
                enabled=enabled,
                grace_seconds=120,
                alert_on_failure=True,
                alert_on_miss=True,
            ),
        )


@dataclass(frozen=True)
class MonitorEvent:
    source_type: str
    event_type: str
    level: str
    message: str
    event_at: datetime
    job_name: Optional[str] = None
    script_path: Optional[str] = None
    run_id: Optional[str] = None
    scheduled_for: Optional[datetime] = None
    success: Optional[bool] = None
    return_code: Optional[int] = None
    duration_ms: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "sourceType": self.source_type,
            "eventType": self.event_type,
            "level": self.level,
            "message": self.message,
            "eventAt": self.event_at.astimezone(UTC).isoformat(),
            "metadata": self.metadata,
        }
        if self.job_name:
            payload["jobName"] = self.job_name
        if self.script_path:
            payload["scriptPath"] = self.script_path
        if self.run_id:
            payload["runId"] = self.run_id
        if self.scheduled_for:
            payload["scheduledFor"] = self.scheduled_for.astimezone(UTC).isoformat()
        if self.success is not None:
            payload["success"] = self.success
        if self.return_code is not None:
            payload["returnCode"] = self.return_code
        if self.duration_ms is not None:
            payload["durationMs"] = self.duration_ms
        return payload


class MonitorEmitter:
    """Best-effort, non-blocking monitor event emitter."""

    def __init__(self, settings: MonitorSettings):
        self.settings = settings
        self._queue: "Queue[MonitorEvent]" = Queue(maxsize=settings.buffer.max_events)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._dropped_events = 0
        self._warned_disabled = False

        if self.settings.enabled:
            self._thread = threading.Thread(target=self._run, daemon=True, name="chief-monitor-emitter")
            self._thread.start()

    def emit(self, event: MonitorEvent) -> None:
        if not self.settings.enabled:
            if not self._warned_disabled:
                logger.info("Monitor emitter disabled; telemetry events will not be sent.")
                self._warned_disabled = True
            return

        try:
            self._queue.put_nowait(event)
        except Exception:
            self._dropped_events += 1
            logger.warning(
                "Monitor emitter queue is full; dropping telemetry event (dropped=%s).",
                self._dropped_events,
            )

    def close(self, timeout_seconds: float = 2.0) -> None:
        if not self.settings.enabled:
            return
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout_seconds)
        # Final best-effort flush.
        batch = self._collect_batch(limit=10000)
        if batch:
            if not self._send_batch(batch):
                self._spool_payloads(batch)
        self._replay_spool(limit=1000)

    def _run(self) -> None:
        interval = max(0.05, self.settings.buffer.flush_interval_ms / 1000.0)
        while not self._stop_event.is_set():
            try:
                batch = self._collect_batch(limit=250)
                if batch:
                    if not self._send_batch(batch):
                        self._spool_payloads(batch)
                self._replay_spool(limit=250)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Monitor emitter loop error: %s", str(exc))
            self._stop_event.wait(interval)

    def _collect_batch(self, limit: int) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        for _ in range(limit):
            try:
                event = self._queue.get_nowait()
            except Empty:
                break
            payloads.append(event.to_payload())
        return payloads

    def _send_batch(self, payloads: List[Dict[str, Any]]) -> bool:
        if not payloads:
            return True
        body = json.dumps({"events": payloads}).encode("utf-8")
        url = self.settings.endpoint.rstrip("/") + "/v1/events/batch"
        headers = {"Content-Type": "application/json"}
        if self.settings.api_key:
            headers["x-api-key"] = self.settings.api_key
        req = urllib_request.Request(url=url, data=body, method="POST", headers=headers)
        try:
            with urllib_request.urlopen(req, timeout=max(0.1, self.settings.timeout_ms / 1000.0)) as response:
                return 200 <= response.status < 300
        except urllib_error.URLError as exc:
            logger.warning("Monitor emitter failed to send batch: %s", str(exc))
            return False
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Monitor emitter unexpected failure: %s", str(exc))
            return False

    def _spool_payloads(self, payloads: List[Dict[str, Any]]) -> None:
        if not payloads:
            return
        try:
            spool_path = self.settings.buffer.spool_file
            spool_path.parent.mkdir(parents=True, exist_ok=True)
            with spool_path.open("a", encoding="utf-8") as handle:
                for payload in payloads:
                    handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Monitor emitter failed to spool events: %s", str(exc))

    def _replay_spool(self, limit: int) -> None:
        try:
            spool_path = self.settings.buffer.spool_file
            if not spool_path.exists():
                return

            lines = spool_path.read_text(encoding="utf-8").splitlines()
            if not lines:
                return

            replayable = lines[:limit]
            remaining = lines[limit:]
            payloads: List[Dict[str, Any]] = []
            for line in replayable:
                try:
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        payloads.append(payload)
                except Exception:
                    continue

            if not payloads:
                spool_path.write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")
                return

            if self._send_batch(payloads):
                spool_path.write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Monitor emitter failed to replay spool: %s", str(exc))


def monitor_check_metadata(job_monitor: JobMonitorSettings) -> Dict[str, Any]:
    return {
        "check_enabled": job_monitor.check.enabled,
        "grace_seconds": job_monitor.check.grace_seconds,
        "alert_on_failure": job_monitor.check.alert_on_failure,
        "alert_on_miss": job_monitor.check.alert_on_miss,
    }


def emit_monitor_event(emitter: Optional[MonitorEmitter], event: MonitorEvent) -> None:
    if emitter is None:
        return
    try:
        emitter.emit(event)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Monitor emitter failed to enqueue event: %s", str(exc))


def build_monitor_env(
    script: ScriptSpec,
    spec: JobSpec,
    run_id: str,
    scheduled_for: Optional[datetime],
    monitor_settings: Optional[MonitorSettings],
) -> Dict[str, str]:
    env: Dict[str, str] = {
        "CHIEF_RUN_ID": run_id,
        "CHIEF_JOB_NAME": spec.name,
        "CHIEF_SCRIPT_PATH": str(script.resolved_path),
    }
    if scheduled_for is not None:
        env["CHIEF_SCHEDULED_FOR"] = scheduled_for.astimezone(UTC).isoformat()
    if monitor_settings and monitor_settings.enabled:
        env["CHIEF_MONITOR_ENDPOINT"] = monitor_settings.endpoint
        if monitor_settings.api_key:
            env["CHIEF_MONITOR_API_KEY"] = monitor_settings.api_key
    return env


def require_yaml_dependency() -> None:
    if yaml is None:
        raise ChiefError(
            "Missing required dependency: PyYAML. Install with: pip install -r requirements.txt"
        )


def require_croniter_dependency() -> None:
    if croniter is None:
        raise ChiefError(
            "Missing required dependency: croniter. Install with: pip install -r requirements.txt"
        )


def system_timezone() -> Tuple[ZoneInfo, str]:
    local_tz = datetime.now().astimezone().tzinfo
    if isinstance(local_tz, ZoneInfo):
        return local_tz, local_tz.key
    tz_name = os.environ.get("TZ")
    if tz_name:
        try:
            zone = ZoneInfo(tz_name)
            return zone, tz_name
        except ZoneInfoNotFoundError:
            pass
    return ZoneInfo("UTC"), "UTC"


def parse_timezone(name: str, field_path: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(f'Error: Invalid timezone "{name}" at {field_path}.') from exc


def parse_hhmm(value: Any, field_path: str) -> Tuple[int, int, str]:
    if not isinstance(value, str):
        raise ConfigError(f"Error: {field_path} must be HH:MM string.")
    match = HHMM_RE.match(value)
    if not match:
        raise ConfigError(f'Error: {field_path} must be HH:MM (24-hour), got "{value}".')
    hour = int(match.group(1))
    minute = int(match.group(2))
    return hour, minute, value


def parse_iso_datetime(value: Any, tz: ZoneInfo, field_path: str) -> datetime:
    if not isinstance(value, str):
        raise ConfigError(f"Error: {field_path} must be an ISO datetime string.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f'Error: {field_path} must be ISO datetime, got "{value}".') from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    else:
        parsed = parsed.astimezone(tz)
    return parsed


def parse_exclude_dates(raw: Any, field_path: str) -> Set[date]:
    if raw is None:
        return set()
    if isinstance(raw, dict) and "holidays" in raw:
        raise ConfigError(
            "Error: named holidays are disabled; use explicit date exclusions in exclude: [YYYY-MM-DD]."
        )
    if not isinstance(raw, list):
        raise ConfigError(f"Error: {field_path} must be a list of YYYY-MM-DD dates.")
    out: Set[date] = set()
    for idx, value in enumerate(raw):
        if not isinstance(value, str):
            raise ConfigError(f"Error: {field_path}[{idx}] must be YYYY-MM-DD string.")
        try:
            out.add(date.fromisoformat(value))
        except ValueError as exc:
            raise ConfigError(
                f'Error: {field_path}[{idx}] must be YYYY-MM-DD, got "{value}".'
            ) from exc
    return out


def ensure_bool(value: Any, field_path: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigError(f"Error: {field_path} must be true or false.")
    return value


def ensure_int(value: Any, field_path: str, default: int, minimum: int = 1) -> int:
    if value is None:
        return default
    if not isinstance(value, int):
        raise ConfigError(f"Error: {field_path} must be an integer.")
    if value < minimum:
        raise ConfigError(f"Error: {field_path} must be >= {minimum}.")
    return value


def ensure_str(value: Any, field_path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Error: {field_path} must be a non-empty string.")
    return value.strip()


def parse_overlap(value: Any, field_path: str, default: str) -> str:
    if value is None:
        return default
    overlap = ensure_str(value, field_path).lower()
    if overlap not in VALID_OVERLAPS:
        raise ConfigError(
            f'Error: {field_path} must be one of {sorted(VALID_OVERLAPS)}, got "{overlap}".'
        )
    return overlap


def normalize_weekday_token(token: str, field_path: str) -> int:
    tok = token.strip().lower()
    if tok in DAY_NAME_TO_CRON:
        return DAY_NAME_TO_CRON[tok]
    if tok.isdigit():
        num = int(tok)
        if num == 7:
            return 0
        if 0 <= num <= 6:
            return num
    raise ConfigError(f'Error: Invalid weekday "{token}" at {field_path}.')


def weekday_name_from_cron(cron_num: int) -> str:
    return CRON_TO_DAY_NAME[cron_num]


def parse_weekday_expression(value: Any, field_path: str) -> Tuple[str, str]:
    if isinstance(value, list):
        if not value:
            raise ConfigError(f"Error: {field_path} cannot be empty.")
        parts = [parse_weekday_expression(item, f"{field_path}[]")[0] for item in value]
        cron_expr = ",".join(parts)
        human = ", ".join(parse_weekday_expression(item, f"{field_path}[]")[1] for item in value)
        return cron_expr, human
    if not isinstance(value, str):
        raise ConfigError(f"Error: {field_path} must be weekday string or list.")

    raw = value.strip().lower()
    if "," in raw:
        segments = [segment.strip() for segment in raw.split(",") if segment.strip()]
        if not segments:
            raise ConfigError(f"Error: {field_path} is empty.")
        cron_tokens: List[str] = []
        names: List[str] = []
        for segment in segments:
            token, name = parse_weekday_expression(segment, field_path)
            cron_tokens.append(token)
            names.append(name)
        return ",".join(cron_tokens), ", ".join(names)

    if "-" in raw and raw.count("-") == 1:
        left, right = [chunk.strip() for chunk in raw.split("-", 1)]
        left_num = normalize_weekday_token(left, field_path)
        right_num = normalize_weekday_token(right, field_path)
        if left_num > right_num:
            raise ConfigError(f'Error: Invalid weekday range "{raw}" at {field_path}.')
        return f"{left_num}-{right_num}", f"{weekday_name_from_cron(left_num)}-{weekday_name_from_cron(right_num)}"

    num = normalize_weekday_token(raw, field_path)
    return str(num), weekday_name_from_cron(num)


def parse_single_weekday(value: Any, field_path: str) -> Tuple[int, int, str]:
    cron_token, human = parse_weekday_expression(value, field_path)
    if "," in cron_token or "-" in cron_token:
        raise ConfigError(f"Error: {field_path} must be a single weekday for this frequency.")
    cron_num = int(cron_token)
    py_weekday = 6 if cron_num == 0 else cron_num - 1
    return cron_num, py_weekday, human


def normalize_month_token(token: Any, field_path: str) -> int:
    if isinstance(token, int):
        month = token
    elif isinstance(token, str):
        raw = token.strip().lower()
        if raw in MONTH_NAME_TO_NUM:
            month = MONTH_NAME_TO_NUM[raw]
        elif raw.isdigit():
            month = int(raw)
        else:
            raise ConfigError(f'Error: Invalid month "{token}" at {field_path}.')
    else:
        raise ConfigError(f"Error: {field_path} must be month name or number.")
    if month < 1 or month > 12:
        raise ConfigError(f"Error: {field_path} must be between 1 and 12.")
    return month


def validate_day_of_month(value: Any, field_path: str) -> int:
    if not isinstance(value, int):
        raise ConfigError(f"Error: {field_path} must be an integer.")
    if value < 1 or value > 31:
        raise ConfigError(f"Error: {field_path} must be between 1 and 31.")
    return value


def parse_interval(value: Any, field_path: str) -> Tuple[int, str]:
    if not isinstance(value, str):
        raise ConfigError(f"Error: {field_path} must be interval string like 5m, 2h, 1d.")
    match = INTERVAL_RE.match(value.strip().lower())
    if not match:
        raise ConfigError(f'Error: {field_path} must be in format <number><m|h|d>, got "{value}".')
    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        raise ConfigError(f"Error: {field_path} must be > 0.")
    if unit == "s":
        raise ConfigError(
            'Error: seconds intervals are unsupported. Use m, h, or d in "every".'
        )
    return amount, unit


def replace_named_tokens(raw: str, mapping: Dict[str, int], field_path: str) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(0).lower()
        if token not in mapping:
            raise ConfigError(f'Error: Invalid token "{token}" at {field_path}.')
        return str(mapping[token])

    return re.sub(r"[A-Za-z]+", repl, raw)


def validate_cron_token(raw: Any, field_path: str, min_value: int, max_value: int) -> str:
    token: str
    if isinstance(raw, int):
        token = str(raw)
    elif isinstance(raw, str):
        token = raw.strip()
    else:
        raise ConfigError(f"Error: {field_path} must be string/int cron token.")
    if not token:
        raise ConfigError(f"Error: {field_path} cannot be empty.")
    if not CRON_FIELD_RE.match(token):
        raise ConfigError(f'Error: Invalid cron token "{token}" at {field_path}.')

    for part in token.split(","):
        if not part:
            raise ConfigError(f'Error: Invalid cron token "{token}" at {field_path}.')
        if "/" in part:
            base, step_str = part.split("/", 1)
            if not step_str.isdigit() or int(step_str) <= 0:
                raise ConfigError(f'Error: Invalid step "{part}" at {field_path}.')
            step = int(step_str)
            if base == "*":
                continue
            _validate_range_or_single(base, field_path, min_value, max_value)
            if step > (max_value - min_value + 1):
                raise ConfigError(f'Error: Step "{step}" too large at {field_path}.')
            continue
        _validate_range_or_single(part, field_path, min_value, max_value)
    return token


def _validate_range_or_single(token: str, field_path: str, min_value: int, max_value: int) -> None:
    if token == "*":
        return
    if "-" in token:
        left, right = token.split("-", 1)
        if not left.isdigit() or not right.isdigit():
            raise ConfigError(f'Error: Invalid range "{token}" at {field_path}.')
        start = int(left)
        end = int(right)
        if start > end:
            raise ConfigError(f'Error: Invalid range "{token}" at {field_path}.')
        if start < min_value or end > max_value:
            raise ConfigError(
                f'Error: Range "{token}" out of bounds {min_value}-{max_value} at {field_path}.'
            )
        return
    if not token.isdigit():
        raise ConfigError(f'Error: Invalid token "{token}" at {field_path}.')
    value = int(token)
    if value < min_value or value > max_value:
        raise ConfigError(f'Error: Value "{value}" out of bounds {min_value}-{max_value} at {field_path}.')


def _load_config_payload(config_path: Path) -> Dict[str, Any]:
    require_yaml_dependency()
    if not config_path.exists():
        raise ConfigError(f"Error: Config file not found: {config_path}")

    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Error: Failed to parse YAML in {config_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigError("Error: Top-level config must be a mapping.")
    return payload


def parse_monitor_settings(
    raw: Any,
    config_dir: Path,
    field_path: str = "monitor",
) -> MonitorSettings:
    default_spool = (config_dir / DEFAULT_MONITOR_SPOOL_FILE).resolve()
    if raw is None:
        return MonitorSettings(
            enabled=False,
            endpoint=DEFAULT_MONITOR_ENDPOINT,
            api_key="",
            timeout_ms=DEFAULT_MONITOR_TIMEOUT_MS,
            buffer=MonitorBufferSettings(
                max_events=DEFAULT_MONITOR_BUFFER_MAX_EVENTS,
                flush_interval_ms=DEFAULT_MONITOR_BUFFER_FLUSH_MS,
                spool_file=default_spool,
            ),
        )
    if not isinstance(raw, dict):
        raise ConfigError(f"Error: {field_path} must be a mapping.")

    unknown = set(raw.keys()) - {"enabled", "endpoint", "api_key", "timeout_ms", "buffer"}
    if unknown:
        raise ConfigError(f"Error: Unknown keys in {field_path}: {sorted(unknown)}.")

    enabled = ensure_bool(raw.get("enabled"), f"{field_path}.enabled", False)
    endpoint = ensure_str(raw.get("endpoint", DEFAULT_MONITOR_ENDPOINT), f"{field_path}.endpoint")
    if not (endpoint.startswith("http://") or endpoint.startswith("https://")):
        raise ConfigError(f"Error: {field_path}.endpoint must be an HTTP URL.")
    api_key_raw = raw.get("api_key", "")
    if not isinstance(api_key_raw, str):
        raise ConfigError(f"Error: {field_path}.api_key must be a string.")
    timeout_ms = ensure_int(raw.get("timeout_ms"), f"{field_path}.timeout_ms", DEFAULT_MONITOR_TIMEOUT_MS, 1)

    buffer_raw = raw.get("buffer", {}) or {}
    if not isinstance(buffer_raw, dict):
        raise ConfigError(f"Error: {field_path}.buffer must be a mapping.")
    buffer_unknown = set(buffer_raw.keys()) - {"max_events", "flush_interval_ms", "spool_file"}
    if buffer_unknown:
        raise ConfigError(f"Error: Unknown keys in {field_path}.buffer: {sorted(buffer_unknown)}.")
    max_events = ensure_int(
        buffer_raw.get("max_events"),
        f"{field_path}.buffer.max_events",
        DEFAULT_MONITOR_BUFFER_MAX_EVENTS,
        1,
    )
    flush_interval_ms = ensure_int(
        buffer_raw.get("flush_interval_ms"),
        f"{field_path}.buffer.flush_interval_ms",
        DEFAULT_MONITOR_BUFFER_FLUSH_MS,
        1,
    )
    spool_raw = buffer_raw.get("spool_file", DEFAULT_MONITOR_SPOOL_FILE)
    if not isinstance(spool_raw, str) or not spool_raw.strip():
        raise ConfigError(f"Error: {field_path}.buffer.spool_file must be a non-empty path string.")
    spool_path = Path(spool_raw.strip())
    if not spool_path.is_absolute():
        spool_path = (config_dir / spool_path).resolve()

    return MonitorSettings(
        enabled=enabled,
        endpoint=endpoint,
        api_key=api_key_raw,
        timeout_ms=timeout_ms,
        buffer=MonitorBufferSettings(
            max_events=max_events,
            flush_interval_ms=flush_interval_ms,
            spool_file=spool_path,
        ),
    )


def parse_job_monitor_settings(
    raw: Any,
    field_path: str,
    global_settings: MonitorSettings,
) -> JobMonitorSettings:
    if raw is None:
        return JobMonitorSettings.default(enabled=global_settings.enabled)

    if not isinstance(raw, dict):
        raise ConfigError(f"Error: {field_path} must be a mapping.")
    unknown = set(raw.keys()) - {"enabled", "check"}
    if unknown:
        raise ConfigError(f"Error: Unknown keys in {field_path}: {sorted(unknown)}.")

    enabled = ensure_bool(raw.get("enabled"), f"{field_path}.enabled", global_settings.enabled)

    check_raw = raw.get("check", {}) or {}
    if not isinstance(check_raw, dict):
        raise ConfigError(f"Error: {field_path}.check must be a mapping.")
    check_unknown = set(check_raw.keys()) - {"enabled", "grace_seconds", "alert_on_failure", "alert_on_miss"}
    if check_unknown:
        raise ConfigError(f"Error: Unknown keys in {field_path}.check: {sorted(check_unknown)}.")

    check_enabled = ensure_bool(check_raw.get("enabled"), f"{field_path}.check.enabled", enabled)
    grace_seconds = ensure_int(check_raw.get("grace_seconds"), f"{field_path}.check.grace_seconds", 120, 0)
    alert_on_failure = ensure_bool(
        check_raw.get("alert_on_failure"),
        f"{field_path}.check.alert_on_failure",
        True,
    )
    alert_on_miss = ensure_bool(
        check_raw.get("alert_on_miss"),
        f"{field_path}.check.alert_on_miss",
        True,
    )

    return JobMonitorSettings(
        enabled=enabled,
        check=MonitorCheckSettings(
            enabled=check_enabled,
            grace_seconds=grace_seconds,
            alert_on_failure=alert_on_failure,
            alert_on_miss=alert_on_miss,
        ),
    )


def parse_monitor_settings_from_file(config_path: Path) -> MonitorSettings:
    payload = _load_config_payload(config_path)
    return parse_monitor_settings(payload.get("monitor"), config_path.parent, "monitor")


def effective_monitor_settings(settings: MonitorSettings, runtimes: List[JobRuntime]) -> MonitorSettings:
    should_enable = settings.enabled or any(runtime.spec.monitor.enabled for runtime in runtimes)
    if should_enable == settings.enabled:
        return settings
    return MonitorSettings(
        enabled=should_enable,
        endpoint=settings.endpoint,
        api_key=settings.api_key,
        timeout_ms=settings.timeout_ms,
        buffer=settings.buffer,
    )


def parse_config(config_path: Path) -> List[JobSpec]:
    payload = _load_config_payload(config_path)

    unknown_top = set(payload.keys()) - {"version", "defaults", "jobs", "monitor"}
    if unknown_top:
        raise ConfigError(f"Error: Unknown top-level keys: {sorted(unknown_top)}.")

    defaults = payload.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        raise ConfigError("Error: defaults must be a mapping.")

    _, system_tz_name = system_timezone()
    default_timezone_name = defaults.get("timezone", system_tz_name)
    if not isinstance(default_timezone_name, str):
        raise ConfigError("Error: defaults.timezone must be a timezone string.")
    parse_timezone(default_timezone_name, "defaults.timezone")

    default_working_dir_raw = defaults.get("working_dir", ".")
    default_working_dir = _resolve_working_dir(default_working_dir_raw, config_path.parent, "defaults.working_dir")
    default_stop_on_failure = ensure_bool(defaults.get("stop_on_failure"), "defaults.stop_on_failure", True)
    default_overlap = parse_overlap(defaults.get("overlap"), "defaults.overlap", "skip")
    monitor_settings = parse_monitor_settings(payload.get("monitor"), config_path.parent, "monitor")

    jobs_raw = payload.get("jobs")
    if not isinstance(jobs_raw, list) or not jobs_raw:
        raise ConfigError("Error: jobs must be a non-empty list.")

    seen_names: Set[str] = set()
    jobs: List[JobSpec] = []

    for idx, job_raw in enumerate(jobs_raw):
        path = f"jobs[{idx}]"
        if not isinstance(job_raw, dict):
            raise ConfigError(f"Error: {path} must be a mapping.")

        unknown_job = set(job_raw.keys()) - {
            "name",
            "enabled",
            "working_dir",
            "stop_on_failure",
            "overlap",
            "schedule",
            "scripts",
            "monitor",
        }
        if unknown_job:
            raise ConfigError(f"Error: Unknown keys in {path}: {sorted(unknown_job)}.")

        name = ensure_str(job_raw.get("name"), f"{path}.name")
        if name in seen_names:
            raise ConfigError(f'Error: Duplicate job name "{name}".')
        seen_names.add(name)

        enabled = ensure_bool(job_raw.get("enabled"), f"{path}.enabled", True)
        working_dir = _resolve_working_dir(
            job_raw.get("working_dir", str(default_working_dir)),
            config_path.parent,
            f"{path}.working_dir",
        )
        stop_on_failure = ensure_bool(
            job_raw.get("stop_on_failure"), f"{path}.stop_on_failure", default_stop_on_failure
        )
        overlap = parse_overlap(job_raw.get("overlap"), f"{path}.overlap", default_overlap)
        schedule = parse_schedule(
            job_raw.get("schedule"),
            f"{path}.schedule",
            default_timezone_name,
        )
        scripts = parse_scripts(job_raw.get("scripts"), f"{path}.scripts", working_dir)
        monitor = parse_job_monitor_settings(job_raw.get("monitor"), f"{path}.monitor", monitor_settings)

        jobs.append(
            JobSpec(
                name=name,
                enabled=enabled,
                working_dir=working_dir,
                stop_on_failure=stop_on_failure,
                overlap=overlap,
                scripts=scripts,
                schedule=schedule,
                monitor=monitor,
            )
        )

    return jobs


def _resolve_working_dir(value: Any, config_dir: Path, field_path: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Error: {field_path} must be a non-empty path string.")
    raw = Path(value.strip())
    resolved = raw if raw.is_absolute() else (config_dir / raw)
    resolved = resolved.resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise ConfigError(f"Error: working directory does not exist at {field_path}: {resolved}")
    return resolved


def parse_scripts(raw: Any, field_path: str, working_dir: Path) -> List[ScriptSpec]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError(f"Error: {field_path} must be a non-empty list.")
    scripts: List[ScriptSpec] = []
    for idx, script_raw in enumerate(raw):
        item_path = f"{field_path}[{idx}]"
        if not isinstance(script_raw, dict):
            raise ConfigError(f"Error: {item_path} must be a mapping.")
        unknown = set(script_raw.keys()) - {"path", "args", "timeout"}
        if unknown:
            raise ConfigError(f"Error: Unknown keys in {item_path}: {sorted(unknown)}.")
        path_str = ensure_str(script_raw.get("path"), f"{item_path}.path")
        args_raw = script_raw.get("args", [])
        if args_raw is None:
            args_raw = []
        if isinstance(args_raw, str):
            # Support shell-style arg strings for convenience in YAML.
            args = shlex.split(args_raw)
        elif isinstance(args_raw, list):
            args = []
            for arg_idx, arg in enumerate(args_raw):
                if not isinstance(arg, (str, int, float, bool)):
                    raise ConfigError(
                        f"Error: {item_path}.args[{arg_idx}] must be scalar value convertible to string."
                    )
                args.append(str(arg))
        else:
            raise ConfigError(f"Error: {item_path}.args must be a list or shell-style string.")

        timeout = ensure_int(script_raw.get("timeout"), f"{item_path}.timeout", DEFAULT_TIMEOUT_SECONDS, 1)
        raw_path = Path(path_str)
        resolved = raw_path if raw_path.is_absolute() else (working_dir / raw_path)
        resolved = resolved.resolve()
        if not resolved.exists() or not resolved.is_file():
            raise ConfigError(
                f"Error: Script path does not exist for {item_path}.path: {resolved}"
            )
        scripts.append(ScriptSpec(path=path_str, args=args, timeout=timeout, resolved_path=resolved))
    return scripts


def parse_schedule(
    raw: Any,
    field_path: str,
    default_timezone_name: str,
) -> ScheduleSpec:
    if not isinstance(raw, dict):
        raise ConfigError(f"Error: {field_path} must be a mapping.")

    frequency = ensure_str(raw.get("frequency"), f"{field_path}.frequency").lower()
    if frequency not in VALID_FREQUENCIES:
        raise ConfigError(
            f'Error: {field_path}.frequency must be one of {sorted(VALID_FREQUENCIES)}, got "{frequency}".'
        )

    timezone_name = raw.get("timezone", default_timezone_name)
    if not isinstance(timezone_name, str):
        raise ConfigError(f"Error: {field_path}.timezone must be a timezone string.")
    timezone_obj = parse_timezone(timezone_name, f"{field_path}.timezone")

    start = parse_iso_datetime(raw["start"], timezone_obj, f"{field_path}.start") if "start" in raw else None
    end = parse_iso_datetime(raw["end"], timezone_obj, f"{field_path}.end") if "end" in raw else None
    if start and end and start > end:
        raise ConfigError(f"Error: {field_path}.start must be <= {field_path}.end.")

    exclude_dates = parse_exclude_dates(raw.get("exclude"), f"{field_path}.exclude")

    _validate_schedule_fields(raw, field_path, frequency)
    _validate_frequency_payload(raw, field_path, frequency)

    return ScheduleSpec(
        frequency=frequency,
        raw=dict(raw),
        timezone=timezone_obj,
        timezone_name=timezone_name,
        start=start,
        end=end,
        exclude_dates=exclude_dates,
    )


def _validate_schedule_fields(raw: Dict[str, Any], field_path: str, frequency: str) -> None:
    global_fields = {"frequency", "timezone", "start", "end", "exclude"}
    per_frequency: Dict[str, Set[str]] = {
        "daily": {"time", "weekdays_only"},
        "weekly": {"day", "time"},
        "monthly": {"day_of_month", "ordinal", "day", "time"},
        "yearly": {"month", "day_of_month", "time"},
        "interval": {"every"},
        "custom": {"minute", "hour", "day_of_month", "month", "day_of_week"},
    }
    allowed = global_fields | per_frequency[frequency]
    unknown = set(raw.keys()) - allowed
    if unknown:
        raise ConfigError(f"Error: Unknown fields for {frequency} schedule at {field_path}: {sorted(unknown)}.")


def _validate_frequency_payload(raw: Dict[str, Any], field_path: str, frequency: str) -> None:
    if frequency == "daily":
        parse_hhmm(raw.get("time"), f"{field_path}.time")
        if "weekdays_only" in raw and not isinstance(raw["weekdays_only"], bool):
            raise ConfigError(f"Error: {field_path}.weekdays_only must be true/false.")
        return

    if frequency == "weekly":
        parse_hhmm(raw.get("time"), f"{field_path}.time")
        parse_weekday_expression(raw.get("day"), f"{field_path}.day")
        return

    if frequency == "monthly":
        has_dom = "day_of_month" in raw
        has_ordinal = "ordinal" in raw
        has_day = "day" in raw
        parse_hhmm(raw.get("time"), f"{field_path}.time")
        if has_dom and (has_ordinal or has_day):
            raise ConfigError(
                f'Error: "{field_path}" monthly schedule cannot mix "day_of_month" with "ordinal/day".'
            )
        if has_dom:
            validate_day_of_month(raw.get("day_of_month"), f"{field_path}.day_of_month")
            return
        if has_ordinal and has_day:
            ordinal = ensure_str(raw["ordinal"], f"{field_path}.ordinal").lower()
            if ordinal not in ORDINAL_TO_INDEX:
                raise ConfigError(
                    f'Error: {field_path}.ordinal must be one of {sorted(ORDINAL_TO_INDEX.keys())}.'
                )
            parse_single_weekday(raw["day"], f"{field_path}.day")
            return
        raise ConfigError('Error: "monthly" requires either "day_of_month" or "ordinal + day".')

    if frequency == "yearly":
        normalize_month_token(raw.get("month"), f"{field_path}.month")
        validate_day_of_month(raw.get("day_of_month"), f"{field_path}.day_of_month")
        parse_hhmm(raw.get("time"), f"{field_path}.time")
        return

    if frequency == "interval":
        parse_interval(raw.get("every"), f"{field_path}.every")
        if "time" in raw:
            raise ConfigError('Error: Interval mode cannot include "time".')
        return

    if frequency == "custom":
        custom_fields = ["minute", "hour", "day_of_month", "month", "day_of_week"]
        if not any(field in raw for field in custom_fields):
            raise ConfigError(
                f'Error: "custom" requires at least one of {custom_fields}.'
            )
        if "minute" in raw:
            validate_cron_token(raw["minute"], f"{field_path}.minute", 0, 59)
        if "hour" in raw:
            validate_cron_token(raw["hour"], f"{field_path}.hour", 0, 23)
        if "day_of_month" in raw:
            validate_cron_token(raw["day_of_month"], f"{field_path}.day_of_month", 1, 31)
        if "month" in raw:
            month_token = str(raw["month"]).strip().lower()
            month_token = replace_named_tokens(month_token, MONTH_NAME_TO_NUM, f"{field_path}.month")
            validate_cron_token(month_token, f"{field_path}.month", 1, 12)
        if "day_of_week" in raw:
            dow_token = str(raw["day_of_week"]).strip().lower()
            dow_token = replace_named_tokens(dow_token, DAY_NAME_TO_CRON, f"{field_path}.day_of_week")
            validate_cron_token(dow_token, f"{field_path}.day_of_week", 0, 7)
        return

    raise ConfigError(f"Error: Unsupported frequency at {field_path}.")


def compile_jobs(jobs: List[JobSpec]) -> List[JobRuntime]:
    runtimes: List[JobRuntime] = []
    for idx, job in enumerate(jobs):
        compiled = compile_schedule(job.schedule)
        runtimes.append(JobRuntime(spec=job, compiled=compiled, index=idx))
    return runtimes


def compile_schedule(spec: ScheduleSpec) -> CompiledSchedule:
    frequency = spec.frequency
    raw = spec.raw

    def default_guard(_: datetime) -> bool:
        return True

    if frequency == "daily":
        hour, minute, time_text = parse_hhmm(raw["time"], "schedule.time")
        weekdays_only = bool(raw.get("weekdays_only", False))
        dow = "1-5" if weekdays_only else "*"
        cron_expr = f"{minute} {hour} * * {dow}"
        desc = (
            f"Runs every weekday at {time_text} ({spec.timezone_name})"
            if weekdays_only
            else f"Runs daily at {time_text} ({spec.timezone_name})"
        )
        return _compiled("pure_cron", cron_expr, default_guard, desc, spec)

    if frequency == "weekly":
        hour, minute, time_text = parse_hhmm(raw["time"], "schedule.time")
        dow_token, human_days = parse_weekday_expression(raw["day"], "schedule.day")
        cron_expr = f"{minute} {hour} * * {dow_token}"
        desc = f"Runs every {human_days} at {time_text} ({spec.timezone_name})"
        return _compiled("pure_cron", cron_expr, default_guard, desc, spec)

    if frequency == "monthly":
        hour, minute, time_text = parse_hhmm(raw["time"], "schedule.time")
        if "day_of_month" in raw:
            day_of_month = validate_day_of_month(raw["day_of_month"], "schedule.day_of_month")
            cron_expr = f"{minute} {hour} {day_of_month} * *"
            desc = f"Runs monthly on day {day_of_month} at {time_text} ({spec.timezone_name})"
            return _compiled("pure_cron", cron_expr, default_guard, desc, spec)

        ordinal = str(raw["ordinal"]).strip().lower()
        if ordinal not in ORDINAL_TO_INDEX:
            raise ConfigError(f'Error: Invalid ordinal "{ordinal}".')
        cron_dow, py_weekday, weekday_name = parse_single_weekday(raw["day"], "schedule.day")
        cron_expr = f"{minute} {hour} * * {cron_dow}"

        def monthly_guard(candidate: datetime, ordinal_value: str = ordinal, target_py: int = py_weekday) -> bool:
            if candidate.weekday() != target_py:
                return False
            return _is_monthly_ordinal_weekday(candidate, target_py, ordinal_value)

        desc = (
            f"Runs monthly on the {ordinal} {weekday_name} at {time_text} ({spec.timezone_name})"
        )
        return _compiled("hybrid", cron_expr, monthly_guard, desc, spec)

    if frequency == "yearly":
        hour, minute, time_text = parse_hhmm(raw["time"], "schedule.time")
        month = normalize_month_token(raw["month"], "schedule.month")
        day_of_month = validate_day_of_month(raw["day_of_month"], "schedule.day_of_month")
        cron_expr = f"{minute} {hour} {day_of_month} {month} *"
        month_name = next(name for name, num in MONTH_NAME_TO_NUM.items() if num == month)
        desc = (
            f"Runs yearly on {month_name} {day_of_month} at {time_text} ({spec.timezone_name})"
        )
        return _compiled("pure_cron", cron_expr, default_guard, desc, spec)

    if frequency == "interval":
        amount, unit = parse_interval(raw["every"], "schedule.every")
        interval_delta = _interval_delta(amount, unit)
        interval_text = f"{amount}{unit}"
        if unit == "m" and 60 % amount == 0:
            cron_expr = f"*/{amount} * * * *"
            desc = f"Runs every {amount} minute(s) ({spec.timezone_name})"
            return _compiled("pure_cron", cron_expr, default_guard, desc, spec, interval_delta, interval_text)
        if unit == "h" and 24 % amount == 0:
            cron_expr = f"0 */{amount} * * *"
            desc = f"Runs every {amount} hour(s) ({spec.timezone_name})"
            return _compiled("pure_cron", cron_expr, default_guard, desc, spec, interval_delta, interval_text)
        if unit == "d" and amount == 1:
            cron_expr = "0 0 * * *"
            desc = f"Runs every day at 00:00 ({spec.timezone_name})"
            return _compiled("pure_cron", cron_expr, default_guard, desc, spec, interval_delta, interval_text)

        desc = f"Runs every {amount}{unit} using runtime scheduler ({spec.timezone_name})"
        return _compiled("runtime_only", None, default_guard, desc, spec, interval_delta, interval_text)

    if frequency == "custom":
        minute = _normalize_custom_field(raw.get("minute", "*"), "schedule.minute", 0, 59)
        hour = _normalize_custom_field(raw.get("hour", "*"), "schedule.hour", 0, 23)
        day_of_month = _normalize_custom_field(raw.get("day_of_month", "*"), "schedule.day_of_month", 1, 31)
        month_raw = str(raw.get("month", "*")).strip().lower()
        month_norm = replace_named_tokens(month_raw, MONTH_NAME_TO_NUM, "schedule.month")
        month = _normalize_custom_field(month_norm, "schedule.month", 1, 12)
        dow_raw = str(raw.get("day_of_week", "*")).strip().lower()
        dow_norm = replace_named_tokens(dow_raw, DAY_NAME_TO_CRON, "schedule.day_of_week")
        day_of_week = _normalize_custom_field(dow_norm, "schedule.day_of_week", 0, 7)
        cron_expr = f"{minute} {hour} {day_of_month} {month} {day_of_week}"
        desc = f"Runs on custom schedule ({spec.timezone_name})"
        return _compiled("pure_cron", cron_expr, default_guard, desc, spec)

    raise ConfigError(f"Error: Unsupported frequency {frequency}.")


def _compiled(
    kind: str,
    cron_expr: Optional[str],
    guard: Callable[[datetime], bool],
    description: str,
    spec: ScheduleSpec,
    interval: Optional[timedelta] = None,
    interval_text: Optional[str] = None,
) -> CompiledSchedule:
    return CompiledSchedule(
        kind=kind,
        cron_expr=cron_expr,
        guard=guard,
        description=description,
        timezone=spec.timezone,
        timezone_name=spec.timezone_name,
        start=spec.start,
        end=spec.end,
        exclude_dates=set(spec.exclude_dates),
        interval=interval,
        interval_text=interval_text,
    )


def _normalize_custom_field(value: Any, field_path: str, minimum: int, maximum: int) -> str:
    token = validate_cron_token(value, field_path, minimum, maximum)
    return token


def _interval_delta(amount: int, unit: str) -> timedelta:
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    raise ConfigError(f'Error: Unsupported interval unit "{unit}".')


def _is_monthly_ordinal_weekday(candidate: datetime, py_weekday: int, ordinal: str) -> bool:
    month = candidate.month
    year = candidate.year
    days: List[int] = []
    day_cursor = datetime(year, month, 1, tzinfo=candidate.tzinfo)
    while day_cursor.month == month:
        if day_cursor.weekday() == py_weekday:
            days.append(day_cursor.day)
        day_cursor += timedelta(days=1)
    if not days:
        return False
    if ordinal == "last":
        return candidate.day == days[-1]
    idx = ORDINAL_TO_INDEX[ordinal]
    if idx >= len(days):
        return False
    return candidate.day == days[idx]


def _is_nonexistent_local(local_dt: datetime, tz: ZoneInfo) -> bool:
    naive = local_dt.replace(tzinfo=None)
    assumed = naive.replace(tzinfo=tz, fold=0)
    roundtrip = assumed.astimezone(UTC).astimezone(tz).replace(tzinfo=None)
    return roundtrip != naive


def _is_ambiguous_local(local_dt: datetime, tz: ZoneInfo) -> bool:
    naive = local_dt.replace(tzinfo=None)
    fold0 = naive.replace(tzinfo=tz, fold=0)
    fold1 = naive.replace(tzinfo=tz, fold=1)
    return fold0.utcoffset() != fold1.utcoffset()


def candidate_allowed(compiled: CompiledSchedule, candidate_local: datetime) -> bool:
    local = candidate_local.astimezone(compiled.timezone)

    if _is_nonexistent_local(local, compiled.timezone):
        return False
    if _is_ambiguous_local(local, compiled.timezone) and local.fold == 1:
        return False

    if compiled.start and local < compiled.start:
        return False
    if compiled.end and local > compiled.end:
        return False
    if local.date() in compiled.exclude_dates:
        return False
    return compiled.guard(local)


def next_run_after(compiled: CompiledSchedule, after_utc: datetime) -> Optional[datetime]:
    after_utc = _ensure_aware_utc(after_utc)
    if compiled.kind in {"pure_cron", "hybrid"}:
        return _next_cron_after(compiled, after_utc)
    return _next_interval_after(compiled, after_utc)


def _next_cron_after(compiled: CompiledSchedule, after_utc: datetime) -> Optional[datetime]:
    require_croniter_dependency()
    if compiled.cron_expr is None:
        return None
    local_after = after_utc.astimezone(compiled.timezone)
    iterator = croniter(compiled.cron_expr, local_after)
    for _ in range(10000):
        nxt = iterator.get_next(datetime)
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=compiled.timezone)
        else:
            nxt = nxt.astimezone(compiled.timezone)
        if candidate_allowed(compiled, nxt):
            return nxt.astimezone(UTC)
        if compiled.end and nxt > compiled.end:
            return None
    return None


def _next_interval_after(compiled: CompiledSchedule, after_utc: datetime) -> Optional[datetime]:
    if not compiled.interval:
        return None
    local_after = after_utc.astimezone(compiled.timezone)

    if compiled.start:
        if local_after < compiled.start:
            candidate = compiled.start
        else:
            elapsed = local_after - compiled.start
            steps = int(elapsed.total_seconds() // compiled.interval.total_seconds()) + 1
            candidate = compiled.start + (compiled.interval * steps)
    else:
        candidate = local_after + compiled.interval

    for _ in range(10000):
        if compiled.end and candidate > compiled.end:
            return None
        if candidate_allowed(compiled, candidate):
            return candidate.astimezone(UTC)
        candidate += compiled.interval
    return None


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def next_run_times(compiled: CompiledSchedule, count: int, now_utc: Optional[datetime] = None) -> List[datetime]:
    now = _ensure_aware_utc(now_utc or datetime.now(tz=UTC))
    runs: List[datetime] = []
    seen_local_slots: Set[str] = set()
    cursor = now
    while len(runs) < count:
        nxt = next_run_after(compiled, cursor)
        if nxt is None:
            break
        local = nxt.astimezone(compiled.timezone)
        slot_key = local.strftime("%Y-%m-%d %H:%M")
        if slot_key not in seen_local_slots:
            runs.append(nxt)
            seen_local_slots.add(slot_key)
        cursor = nxt + timedelta(seconds=1)
    return runs


def is_due_now(runtime: JobRuntime, at_utc: Optional[datetime] = None) -> bool:
    if runtime.compiled.kind in {"pure_cron", "hybrid"}:
        require_croniter_dependency()
    now = _ensure_aware_utc(at_utc or datetime.now(tz=UTC))
    compiled = runtime.compiled

    if compiled.kind == "runtime_only":
        marker = now.replace(second=0, microsecond=0)
        candidate = next_run_after(compiled, marker - timedelta(seconds=1))
        return candidate is not None and abs((candidate - marker).total_seconds()) < 1

    if compiled.cron_expr is None:
        return False

    local = now.astimezone(compiled.timezone).replace(second=0, microsecond=0)
    if not candidate_allowed(compiled, local):
        return False
    return bool(croniter.match(compiled.cron_expr, local))


def run_script(
    script: ScriptSpec,
    working_dir: Path,
    env_overrides: Optional[Dict[str, str]] = None,
) -> ScriptRunResult:
    command = [sys.executable, str(script.resolved_path), *script.args]
    started = datetime.now(tz=UTC)
    env = os.environ.copy()
    if env_overrides:
        env.update({k: v for k, v in env_overrides.items() if v is not None})
    try:
        result = subprocess.run(
            command,
            cwd=str(working_dir),
            capture_output=True,
            text=True,
            timeout=script.timeout,
            check=False,
            env=env,
        )
        duration = (datetime.now(tz=UTC) - started).total_seconds()
        return ScriptRunResult(
            script=script,
            success=result.returncode == 0,
            return_code=result.returncode,
            duration_seconds=duration,
            stdout=result.stdout or "",
            stderr=result.stderr or "",
        )
    except subprocess.TimeoutExpired:
        duration = (datetime.now(tz=UTC) - started).total_seconds()
        return ScriptRunResult(
            script=script,
            success=False,
            return_code=-1,
            duration_seconds=duration,
            stdout="",
            stderr=f"Timed out after {script.timeout} seconds.",
            error="timeout",
        )
    except Exception as exc:  # pragma: no cover - defensive
        duration = (datetime.now(tz=UTC) - started).total_seconds()
        return ScriptRunResult(
            script=script,
            success=False,
            return_code=-2,
            duration_seconds=duration,
            stdout="",
            stderr=str(exc),
            error="exception",
        )


def run_job(
    runtime: JobRuntime,
    scheduled_for: Optional[datetime] = None,
    monitor_emitter: Optional[MonitorEmitter] = None,
) -> JobRunResult:
    spec = runtime.spec
    started = datetime.now(tz=UTC)
    run_id = f"{spec.name}:{started.strftime('%Y%m%d%H%M%S')}-{started.microsecond:06d}-{os.getpid()}"
    job_monitor_emitter = monitor_emitter if (monitor_emitter and spec.monitor.enabled) else None
    monitor_settings = job_monitor_emitter.settings if job_monitor_emitter else None
    check_meta = monitor_check_metadata(spec.monitor)
    if scheduled_for:
        logger.info(
            "[%s] Starting job %s (scheduled_for=%s)",
            run_id,
            spec.name,
            scheduled_for.astimezone(runtime.compiled.timezone).isoformat(),
        )
    else:
        logger.info("[%s] Starting job %s", run_id, spec.name)

    emit_monitor_event(
        job_monitor_emitter,
        MonitorEvent(
            source_type="chief",
            event_type="job.started",
            level="INFO",
            message=f"Job {spec.name} started.",
            event_at=started,
            job_name=spec.name,
            run_id=run_id,
            scheduled_for=scheduled_for,
            metadata={
                "overlap": spec.overlap,
                "script_count": len(spec.scripts),
                **check_meta,
            },
        ),
    )

    script_results: List[ScriptRunResult] = []
    for idx, script in enumerate(spec.scripts, start=1):
        logger.info("[%s] [%s/%s] Running %s", run_id, idx, len(spec.scripts), script.path)
        if script.args:
            logger.info("[%s] Args: %s", run_id, " ".join(shlex.quote(arg) for arg in script.args))
        emit_monitor_event(
            job_monitor_emitter,
            MonitorEvent(
                source_type="chief",
                event_type="script.started",
                level="INFO",
                message=f"Script started: {script.path}",
                event_at=datetime.now(tz=UTC),
                job_name=spec.name,
                script_path=str(script.resolved_path),
                run_id=run_id,
                scheduled_for=scheduled_for,
                metadata={
                    "script_index": idx,
                    "script_total": len(spec.scripts),
                    "args": list(script.args),
                    "timeout_seconds": script.timeout,
                },
            ),
        )

        script_env = build_monitor_env(
            script=script,
            spec=spec,
            run_id=run_id,
            scheduled_for=scheduled_for,
            monitor_settings=monitor_settings,
        )
        result = run_script(script, spec.working_dir, env_overrides=script_env)
        script_results.append(result)

        script_level = "INFO" if result.success else "ERROR"
        stdout_preview = result.stdout.strip()[:1000]
        stderr_preview = result.stderr.strip()[:1000]
        emit_monitor_event(
            job_monitor_emitter,
            MonitorEvent(
                source_type="chief",
                event_type="script.completed",
                level=script_level,
                message=(
                    f"Script completed: {script.path}"
                    if result.success
                    else f"Script failed: {script.path} (code={result.return_code})"
                ),
                event_at=datetime.now(tz=UTC),
                job_name=spec.name,
                script_path=str(script.resolved_path),
                run_id=run_id,
                scheduled_for=scheduled_for,
                success=result.success,
                return_code=result.return_code,
                duration_ms=int(result.duration_seconds * 1000),
                metadata={
                    "error": result.error,
                    "stdout_preview": stdout_preview,
                    "stderr_preview": stderr_preview,
                },
            ),
        )

        if result.success:
            logger.info(
                "[%s] Script succeeded: %s (%.2fs)",
                run_id,
                script.path,
                result.duration_seconds,
            )
        else:
            logger.error(
                "[%s] Script failed: %s (code=%s, duration=%.2fs)",
                run_id,
                script.path,
                result.return_code,
                result.duration_seconds,
            )
            if result.stderr:
                logger.error("[%s] stderr: %s", run_id, result.stderr.strip())
            if spec.stop_on_failure:
                logger.error("[%s] stop_on_failure=true; aborting remaining scripts.", run_id)
                break

    ended = datetime.now(tz=UTC)
    success = all(result.success for result in script_results)
    completed_event_type = "job.completed" if success else "job.failed"
    completed_level = "INFO" if success else "ERROR"
    failed_script = next((result.script.path for result in script_results if not result.success), None)
    emit_monitor_event(
        job_monitor_emitter,
        MonitorEvent(
            source_type="chief",
            event_type=completed_event_type,
            level=completed_level,
            message=(
                f"Job {spec.name} completed successfully."
                if success
                else f"Job {spec.name} failed."
            ),
            event_at=ended,
            job_name=spec.name,
            run_id=run_id,
            scheduled_for=scheduled_for,
            success=success,
            duration_ms=int((ended - started).total_seconds() * 1000),
            metadata={
                "scripts_executed": len(script_results),
                "scripts_total": len(spec.scripts),
                "stop_on_failure": spec.stop_on_failure,
                "failed_script": failed_script,
                **check_meta,
            },
        ),
    )

    logger.info(
        "[%s] Job %s completed with success=%s in %.2fs",
        run_id,
        spec.name,
        success,
        (ended - started).total_seconds(),
    )
    try:
        next_run_utc = next_run_after(runtime.compiled, ended)
        next_run_iso = next_run_utc.astimezone(UTC).isoformat() if next_run_utc is not None else None
        if next_run_utc is None:
            logger.info(
                "[%s] Next scheduled run for %s: none (outside bounds/exclusions or schedule ended)",
                run_id,
                spec.name,
            )
        else:
            next_local = next_run_utc.astimezone(runtime.compiled.timezone)
            logger.info(
                "[%s] Next scheduled run for %s: %s (%s)",
                run_id,
                spec.name,
                next_local.isoformat(),
                runtime.compiled.timezone_name,
            )
        emit_monitor_event(
            job_monitor_emitter,
            MonitorEvent(
                source_type="chief",
                event_type="job.next_scheduled",
                level="INFO",
                message=(
                    f"Next run for {spec.name}: {next_run_iso}"
                    if next_run_iso
                    else f"Next run for {spec.name}: none"
                ),
                event_at=datetime.now(tz=UTC),
                job_name=spec.name,
                run_id=run_id,
                scheduled_for=scheduled_for,
                metadata={
                    "next_run_at": next_run_iso,
                    **check_meta,
                },
            ),
        )
    except ChiefError as exc:
        logger.warning(
            "[%s] Unable to compute next scheduled run for %s: %s",
            run_id,
            spec.name,
            str(exc),
        )
        emit_monitor_event(
            job_monitor_emitter,
            MonitorEvent(
                source_type="chief",
                event_type="job.next_scheduled",
                level="WARN",
                message=f"Unable to compute next run for {spec.name}: {str(exc)}",
                event_at=datetime.now(tz=UTC),
                job_name=spec.name,
                run_id=run_id,
                scheduled_for=scheduled_for,
                metadata={
                    "next_run_at": None,
                    "error": str(exc),
                    **check_meta,
                },
            ),
        )

    return JobRunResult(
        job_name=spec.name,
        success=success,
        script_results=script_results,
        started_at=started,
        ended_at=ended,
        scheduled_for=scheduled_for,
    )


def filter_jobs(runtimes: List[JobRuntime], job_name: Optional[str], include_disabled: bool = False) -> List[JobRuntime]:
    selected = runtimes
    if job_name:
        selected = [runtime for runtime in selected if runtime.spec.name == job_name]
        if not selected:
            raise ChiefError(f'Unknown job "{job_name}".')
    if include_disabled:
        return selected
    selected = [runtime for runtime in selected if runtime.spec.enabled]
    if not selected:
        raise ChiefError("No enabled jobs selected.")
    return selected


def command_validate(config_path: Path) -> int:
    jobs = parse_config(config_path)
    runtimes = compile_jobs(jobs)
    enabled_count = sum(1 for runtime in runtimes if runtime.spec.enabled)
    print(f"Config valid: {config_path}")
    print(f"Total jobs: {len(runtimes)}")
    print(f"Enabled jobs: {enabled_count}")
    for runtime in runtimes:
        print(
            f"- {runtime.spec.name}: {runtime.compiled.kind}"
            + (f" ({runtime.compiled.cron_expr})" if runtime.compiled.cron_expr else "")
        )
    return 0


def command_preview(config_path: Path, job_name: Optional[str], count: int) -> int:
    jobs = parse_config(config_path)
    runtimes = compile_jobs(jobs)
    selected = filter_jobs(runtimes, job_name, include_disabled=True)
    now_utc = datetime.now(tz=UTC)

    for runtime in selected:
        spec = runtime.spec
        compiled = runtime.compiled
        print("=" * 80)
        print(f"Job: {spec.name} (enabled={spec.enabled})")
        print(compiled.description)
        print(f"Schedule mode: {compiled.kind}")
        if compiled.cron_expr:
            if compiled.kind == "hybrid":
                print(f"Cron trigger + runtime guard: {compiled.cron_expr}")
            else:
                print(f"Cron equivalent: {compiled.cron_expr}")
        else:
            print("Cron equivalent: runtime-only")
        if compiled.start:
            print(f"Start bound: {compiled.start.isoformat()}")
        if compiled.end:
            print(f"End bound: {compiled.end.isoformat()}")
        if compiled.exclude_dates:
            listed = ", ".join(sorted(d.isoformat() for d in compiled.exclude_dates))
            print(f"Exclude dates: {listed}")
        print("Scripts:")
        for script in spec.scripts:
            args_text = " ".join(shlex.quote(arg) for arg in script.args) if script.args else "(none)"
            print(f"- {script.path} | timeout={script.timeout}s | args={args_text}")
        print(f"Next {count} run(s):")
        runs = next_run_times(compiled, count, now_utc=now_utc)
        if not runs:
            print("- none")
        for run_dt in runs:
            print(f"- {run_dt.astimezone(compiled.timezone).isoformat()}")
    print("=" * 80)
    return 0


def command_run(config_path: Path, job_name: Optional[str], respect_schedule: bool) -> int:
    jobs = parse_config(config_path)
    runtimes = compile_jobs(jobs)
    selected = filter_jobs(runtimes, job_name, include_disabled=False)
    monitor_settings = effective_monitor_settings(
        parse_monitor_settings_from_file(config_path),
        selected,
    )
    monitor_emitter = MonitorEmitter(monitor_settings)

    exit_code = 0
    now = datetime.now(tz=UTC)
    try:
        for runtime in selected:
            if respect_schedule and not is_due_now(runtime, at_utc=now):
                logger.info("Skipping %s: not due now.", runtime.spec.name)
                continue
            result = run_job(runtime, monitor_emitter=monitor_emitter)
            if not result.success:
                exit_code = 1
    finally:
        monitor_emitter.close()
    return exit_code


def command_export_cron(config_path: Path, job_name: Optional[str]) -> int:
    jobs = parse_config(config_path)
    runtimes = compile_jobs(jobs)
    selected = filter_jobs(runtimes, job_name, include_disabled=False)
    chief_path = Path(__file__).resolve()
    config_abs = config_path.resolve()

    print("# chief.py cron export")
    print(f"# generated_at={datetime.now(tz=UTC).isoformat()}")
    for runtime in selected:
        compiled = runtime.compiled
        name = runtime.spec.name
        print("")
        print(f"# job: {name}")
        print(f"# mode: {compiled.kind}")
        print(f"CRON_TZ={compiled.timezone_name}")
        if compiled.kind == "runtime_only":
            print(f"# runtime-only schedule ({compiled.description}); no cron equivalent.")
            continue
        if compiled.kind == "hybrid":
            print("# NOTE: runtime guard required (ordinal/exclusion/bounds).")
        command = (
            f"cd {shlex.quote(str(runtime.spec.working_dir))} && "
            f"{shlex.quote(sys.executable)} {shlex.quote(str(chief_path))} run "
            f"--config {shlex.quote(str(config_abs))} --job {shlex.quote(name)} --respect-schedule"
        )
        print(f"{compiled.cron_expr} {command}")
    return 0


def command_daemon(config_path: Path, poll_seconds: int) -> int:
    jobs = parse_config(config_path)
    runtimes = compile_jobs(jobs)
    selected = filter_jobs(runtimes, job_name=None, include_disabled=False)
    selected_by_name = {runtime.spec.name: runtime for runtime in selected}
    ordered_names = [runtime.spec.name for runtime in selected]
    monitor_settings = effective_monitor_settings(
        parse_monitor_settings_from_file(config_path),
        selected,
    )
    monitor_emitter = MonitorEmitter(monitor_settings)

    now = datetime.now(tz=UTC)
    states: Dict[str, JobState] = {}
    for runtime in selected:
        states[runtime.spec.name] = JobState(next_fire=next_run_after(runtime.compiled, now))

    trigger_queue: deque[TriggerEvent] = deque()
    completion_queue: Queue[Tuple[str, JobRunResult]] = Queue()
    active_job_name: Optional[str] = None

    def launch(job_runtime: JobRuntime, scheduled_for: datetime) -> None:
        state = states[job_runtime.spec.name]
        state.running_count += 1
        job_emitter = monitor_emitter if job_runtime.spec.monitor.enabled else None
        logger.info(
            "Dispatching %s (overlap=%s, running=%s)",
            job_runtime.spec.name,
            job_runtime.spec.overlap,
            state.running_count,
        )
        emit_monitor_event(
            job_emitter,
            MonitorEvent(
                source_type="chief",
                event_type="daemon.dispatch",
                level="INFO",
                message=f"Dispatching {job_runtime.spec.name}.",
                event_at=datetime.now(tz=UTC),
                job_name=job_runtime.spec.name,
                scheduled_for=scheduled_for,
                metadata={
                    "overlap": job_runtime.spec.overlap,
                    "running_count": state.running_count,
                    **monitor_check_metadata(job_runtime.spec.monitor),
                },
            ),
        )

        def worker() -> None:
            result = run_job(
                job_runtime,
                scheduled_for=scheduled_for,
                monitor_emitter=monitor_emitter,
            )
            completion_queue.put((job_runtime.spec.name, result))

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    logger.info(
        "Starting daemon with %s enabled job(s), poll_seconds=%s",
        len(selected),
        poll_seconds,
    )
    try:
        while True:
            now = datetime.now(tz=UTC)

            # Completion handling
            while True:
                try:
                    name, result = completion_queue.get_nowait()
                except Empty:
                    break
                state = states[name]
                state.running_count = max(state.running_count - 1, 0)
                logger.info(
                    "Job %s finished with success=%s; running_count=%s",
                    name,
                    result.success,
                    state.running_count,
                )
                if state.running_count == 0 and state.queued_pending:
                    state.queued_pending = False
                    trigger_queue.appendleft(TriggerEvent(job_name=name, scheduled_for=now))
                    logger.info("Enqueued queued-pending run for %s", name)
                    emit_monitor_event(
                        monitor_emitter if selected_by_name[name].spec.monitor.enabled else None,
                        MonitorEvent(
                            source_type="chief",
                            event_type="daemon.queued_pending",
                            level="INFO",
                            message=f"Queued pending run for {name}.",
                            event_at=now,
                            job_name=name,
                            scheduled_for=now,
                            metadata={"reason": "prior run completed"},
                        ),
                    )
                if active_job_name == name and state.running_count == 0:
                    active_job_name = None

            # Trigger detection in deterministic job order.
            for job_name_in_order in ordered_names:
                runtime = selected_by_name[job_name_in_order]
                state = states[job_name_in_order]
                while state.next_fire and state.next_fire <= now:
                    trigger_queue.append(
                        TriggerEvent(job_name=job_name_in_order, scheduled_for=state.next_fire)
                    )
                    state.next_fire = next_run_after(runtime.compiled, state.next_fire + timedelta(seconds=1))

            # Dispatch queue; scan for any dispatchable trigger.
            made_progress = True
            while made_progress:
                made_progress = False
                for idx, trigger in enumerate(list(trigger_queue)):
                    runtime = selected_by_name[trigger.job_name]
                    state = states[trigger.job_name]
                    overlap = runtime.spec.overlap

                    if state.running_count > 0:
                        if overlap == "skip":
                            logger.info(
                                "Skipping overlapping run for %s at %s",
                                runtime.spec.name,
                                trigger.scheduled_for.isoformat(),
                            )
                            emit_monitor_event(
                                monitor_emitter if runtime.spec.monitor.enabled else None,
                                MonitorEvent(
                                    source_type="chief",
                                    event_type="daemon.overlap_skipped",
                                    level="INFO",
                                    message=f"Skipped overlapping trigger for {runtime.spec.name}.",
                                    event_at=now,
                                    job_name=runtime.spec.name,
                                    scheduled_for=trigger.scheduled_for,
                                    metadata={"overlap": overlap},
                                ),
                            )
                            _remove_trigger_index(trigger_queue, idx)
                            made_progress = True
                            break
                        if overlap == "queue":
                            if not state.queued_pending:
                                state.queued_pending = True
                                logger.info(
                                    "Queueing one pending run for %s",
                                    runtime.spec.name,
                                )
                                emit_monitor_event(
                                    monitor_emitter if runtime.spec.monitor.enabled else None,
                                    MonitorEvent(
                                        source_type="chief",
                                        event_type="daemon.queued_pending",
                                        level="INFO",
                                        message=f"Queued overlapping trigger for {runtime.spec.name}.",
                                        event_at=now,
                                        job_name=runtime.spec.name,
                                        scheduled_for=trigger.scheduled_for,
                                        metadata={"overlap": overlap},
                                    ),
                                )
                            _remove_trigger_index(trigger_queue, idx)
                            made_progress = True
                            break
                        # overlap=parallel
                        if active_job_name in (None, runtime.spec.name):
                            active_job_name = runtime.spec.name
                            launch(runtime, trigger.scheduled_for)
                            _remove_trigger_index(trigger_queue, idx)
                            made_progress = True
                            break
                        continue

                    # Not currently running
                    if active_job_name not in (None, runtime.spec.name):
                        continue
                    active_job_name = runtime.spec.name
                    launch(runtime, trigger.scheduled_for)
                    _remove_trigger_index(trigger_queue, idx)
                    made_progress = True
                    break

            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        logger.info("Daemon interrupted by user.")
        return 130
    finally:
        monitor_emitter.close()


def _remove_trigger_index(queue_obj: deque[TriggerEvent], index: int) -> None:
    if index == 0:
        queue_obj.popleft()
        return
    temp = list(queue_obj)
    del temp[index]
    queue_obj.clear()
    queue_obj.extend(temp)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="chief.py YAML orchestrator and scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to chief YAML config (default: {DEFAULT_CONFIG})",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate config and compile schedules")
    validate_parser.add_argument("--config", help=f"Path to config (default: {DEFAULT_CONFIG})")

    preview_parser = subparsers.add_parser("preview", help="Show friendly schedule preview")
    preview_parser.add_argument("--config", help=f"Path to config (default: {DEFAULT_CONFIG})")
    preview_parser.add_argument("--job", help="Preview a single job by name")
    preview_parser.add_argument("--count", type=int, default=DEFAULT_PREVIEW_COUNT, help="Next run count")

    run_parser = subparsers.add_parser("run", help="Run jobs once")
    run_parser.add_argument("--config", help=f"Path to config (default: {DEFAULT_CONFIG})")
    run_parser.add_argument("--job", help="Run one job by name")
    run_parser.add_argument(
        "--respect-schedule",
        action="store_true",
        help="Only run selected job(s) if currently due",
    )

    daemon_parser = subparsers.add_parser("daemon", help="Run scheduler daemon loop")
    daemon_parser.add_argument("--config", help=f"Path to config (default: {DEFAULT_CONFIG})")
    daemon_parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help=f"Polling interval in seconds (default: {DEFAULT_POLL_SECONDS})",
    )

    export_parser = subparsers.add_parser("export-cron", help="Export cron-compatible schedules")
    export_parser.add_argument("--config", help=f"Path to config (default: {DEFAULT_CONFIG})")
    export_parser.add_argument("--job", help="Export one job by name")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config or DEFAULT_CONFIG).resolve()

    try:
        if args.command == "validate":
            return command_validate(config_path)
        if args.command == "preview":
            if args.count <= 0:
                raise ChiefError("--count must be >= 1")
            return command_preview(config_path, job_name=args.job, count=args.count)
        if args.command == "run":
            return command_run(config_path, job_name=args.job, respect_schedule=args.respect_schedule)
        if args.command == "daemon":
            if args.poll_seconds <= 0:
                raise ChiefError("--poll-seconds must be >= 1")
            return command_daemon(config_path, poll_seconds=args.poll_seconds)
        if args.command == "export-cron":
            return command_export_cron(config_path, job_name=args.job)
        raise ChiefError(f"Unsupported command: {args.command}")
    except ChiefError as exc:
        logger.error(str(exc))
        return 1
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Unexpected error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
