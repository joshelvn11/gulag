#!/usr/bin/env python3
"""
Worker-facing monitor client for posting telemetry events to the chief monitor service.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request


UTC = timezone.utc
DEFAULT_TIMEOUT_MS = 400


def _non_empty(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    return text if text else None


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


@dataclass
class MonitorContext:
    endpoint: Optional[str]
    api_key: Optional[str]
    run_id: Optional[str]
    job_name: Optional[str]
    script_path: Optional[str]
    scheduled_for: Optional[str]

    @staticmethod
    def from_env() -> "MonitorContext":
        return MonitorContext(
            endpoint=_non_empty(os.getenv("CHIEF_MONITOR_ENDPOINT")),
            api_key=_non_empty(os.getenv("CHIEF_MONITOR_API_KEY")),
            run_id=_non_empty(os.getenv("CHIEF_RUN_ID")),
            job_name=_non_empty(os.getenv("CHIEF_JOB_NAME")),
            script_path=_non_empty(os.getenv("CHIEF_SCRIPT_PATH")),
            scheduled_for=_non_empty(os.getenv("CHIEF_SCHEDULED_FOR")),
        )


class MonitorClient:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
    ) -> None:
        env = MonitorContext.from_env()
        self.context = env
        self.endpoint = _non_empty(endpoint) or env.endpoint
        self.api_key = _non_empty(api_key) or env.api_key
        self.timeout_ms = timeout_ms if timeout_ms > 0 else DEFAULT_TIMEOUT_MS

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    def debug(self, message: str, **meta: Any) -> bool:
        return self._post("DEBUG", message, meta)

    def info(self, message: str, **meta: Any) -> bool:
        return self._post("INFO", message, meta)

    def warn(self, message: str, **meta: Any) -> bool:
        return self._post("WARN", message, meta)

    def error(self, message: str, **meta: Any) -> bool:
        return self._post("ERROR", message, meta)

    def critical(self, message: str, **meta: Any) -> bool:
        return self._post("CRITICAL", message, meta)

    def _post(self, level: str, message: str, meta: Dict[str, Any]) -> bool:
        if not self.endpoint:
            return False
        if not isinstance(message, str) or not message.strip():
            return False

        payload: Dict[str, Any] = {
            "sourceType": "worker",
            "eventType": "worker.message",
            "level": level,
            "message": message.strip(),
            "eventAt": _now_iso(),
            "metadata": meta or {},
        }
        if self.context.job_name:
            payload["jobName"] = self.context.job_name
        if self.context.script_path:
            payload["scriptPath"] = self.context.script_path
        if self.context.run_id:
            payload["runId"] = self.context.run_id
        if self.context.scheduled_for:
            payload["scheduledFor"] = self.context.scheduled_for

        url = self.endpoint.rstrip("/") + "/v1/events"
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        req = urllib_request.Request(url=url, data=data, method="POST", headers=headers)

        try:
            with urllib_request.urlopen(req, timeout=max(0.1, self.timeout_ms / 1000.0)) as response:
                return 200 <= response.status < 300
        except urllib_error.URLError:
            return False
        except Exception:
            return False


monitor = MonitorClient()

