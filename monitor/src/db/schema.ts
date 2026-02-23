import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const telemetryEvents = sqliteTable(
  "telemetry_events",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    receivedAt: text("received_at").notNull(),
    eventAt: text("event_at").notNull(),
    sourceType: text("source_type").notNull(),
    eventType: text("event_type").notNull(),
    level: text("level").notNull(),
    message: text("message").notNull(),
    jobName: text("job_name"),
    scriptPath: text("script_path"),
    runId: text("run_id"),
    scheduledFor: text("scheduled_for"),
    success: integer("success", { mode: "boolean" }),
    returnCode: integer("return_code"),
    durationMs: integer("duration_ms"),
    metadataJson: text("metadata_json").notNull().default("{}"),
  },
  (table) => ({
    eventsJobEventAt: index("idx_events_job_event_at").on(table.jobName, table.eventAt),
    eventsLevelEventAt: index("idx_events_level_event_at").on(table.level, table.eventAt),
    eventsRun: index("idx_events_run").on(table.runId),
  })
);

export const checkStates = sqliteTable(
  "check_states",
  {
    jobName: text("job_name").primaryKey(),
    enabled: integer("enabled", { mode: "boolean" }).notNull().default(true),
    alertOnFailure: integer("alert_on_failure", { mode: "boolean" }).notNull().default(true),
    alertOnMiss: integer("alert_on_miss", { mode: "boolean" }).notNull().default(true),
    graceSeconds: integer("grace_seconds").notNull().default(120),
    status: text("status").notNull().default("UP"),
    lastHeartbeatAt: text("last_heartbeat_at"),
    expectedNextAt: text("expected_next_at"),
    lastSuccessAt: text("last_success_at"),
    lastFailureAt: text("last_failure_at"),
    consecutiveFailures: integer("consecutive_failures").notNull().default(0),
    updatedAt: text("updated_at").notNull(),
  },
  (table) => ({
    checksStatusUpdated: index("idx_checks_status_updated").on(table.status, table.updatedAt),
  })
);

export const alerts = sqliteTable(
  "alerts",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    jobName: text("job_name").notNull(),
    alertType: text("alert_type").notNull(),
    severity: text("severity").notNull(),
    status: text("status").notNull(),
    openedAt: text("opened_at").notNull(),
    closedAt: text("closed_at"),
    dedupeKey: text("dedupe_key").notNull(),
    title: text("title").notNull(),
    detailsJson: text("details_json").notNull().default("{}"),
  },
  (table) => ({
    alertsStatusOpened: index("idx_alerts_status_opened").on(table.status, table.openedAt),
    alertsJobStatus: index("idx_alerts_job_status").on(table.jobName, table.status),
    alertsDedupeStatus: index("idx_alerts_dedupe_status").on(table.dedupeKey, table.status),
  })
);

export const alertDeliveries = sqliteTable("alert_deliveries", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  alertId: integer("alert_id").notNull(),
  channel: text("channel").notNull(),
  attemptedAt: text("attempted_at").notNull(),
  status: text("status").notNull(),
  responseCode: integer("response_code"),
  errorText: text("error_text"),
});

export const serviceConfig = sqliteTable("service_config", {
  key: text("key").primaryKey(),
  valueJson: text("value_json").notNull(),
  updatedAt: text("updated_at").notNull(),
});
