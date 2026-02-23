CREATE TABLE IF NOT EXISTS telemetry_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at TEXT NOT NULL,
  event_at TEXT NOT NULL,
  source_type TEXT NOT NULL,
  event_type TEXT NOT NULL,
  level TEXT NOT NULL,
  message TEXT NOT NULL,
  job_name TEXT,
  script_path TEXT,
  run_id TEXT,
  scheduled_for TEXT,
  success INTEGER,
  return_code INTEGER,
  duration_ms INTEGER,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_job_event_at ON telemetry_events(job_name, event_at);
CREATE INDEX IF NOT EXISTS idx_events_level_event_at ON telemetry_events(level, event_at);
CREATE INDEX IF NOT EXISTS idx_events_run ON telemetry_events(run_id);

CREATE TABLE IF NOT EXISTS check_states (
  job_name TEXT PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 1,
  alert_on_failure INTEGER NOT NULL DEFAULT 1,
  alert_on_miss INTEGER NOT NULL DEFAULT 1,
  grace_seconds INTEGER NOT NULL DEFAULT 120,
  status TEXT NOT NULL DEFAULT 'UP',
  last_heartbeat_at TEXT,
  expected_next_at TEXT,
  last_success_at TEXT,
  last_failure_at TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_checks_status_updated ON check_states(status, updated_at);

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_name TEXT NOT NULL,
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL,
  opened_at TEXT NOT NULL,
  closed_at TEXT,
  dedupe_key TEXT NOT NULL,
  title TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_alerts_status_opened ON alerts(status, opened_at);
CREATE INDEX IF NOT EXISTS idx_alerts_job_status ON alerts(job_name, status);
CREATE INDEX IF NOT EXISTS idx_alerts_dedupe_status ON alerts(dedupe_key, status);

CREATE TABLE IF NOT EXISTS alert_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id INTEGER NOT NULL,
  channel TEXT NOT NULL,
  attempted_at TEXT NOT NULL,
  status TEXT NOT NULL,
  response_code INTEGER,
  error_text TEXT,
  FOREIGN KEY(alert_id) REFERENCES alerts(id)
);

CREATE TABLE IF NOT EXISTS service_config (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
