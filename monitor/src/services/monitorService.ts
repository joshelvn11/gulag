import { and, desc, eq, lt, sql } from "drizzle-orm";

import { MonitorConfig } from "../types.js";
import { alertDeliveries, alerts, checkStates, telemetryEvents } from "../db/schema.js";

const HEARTBEAT_EVENT_TYPES = new Set(["job.started", "job.completed", "job.failed"]);
const CHIEF_HEARTBEAT_EVENT_TYPE = "chief.heartbeat";
const DEFAULT_CHIEF_OFFLINE_AFTER_SECONDS = 45;
const MIN_CHIEF_OFFLINE_AFTER_SECONDS = 5;
const DEFAULT_RECOVERY_AUTO_CLOSE_SECONDS = 900;

type EventPayload = {
  sourceType: "chief" | "worker" | "monitor";
  eventType: string;
  level: "DEBUG" | "INFO" | "WARN" | "ERROR" | "CRITICAL";
  message: string;
  eventAt: string;
  jobName: string | null;
  scriptPath: string | null;
  runId: string | null;
  scheduledFor: string | null;
  success: boolean | null;
  returnCode: number | null;
  durationMs: number | null;
  metadata: Record<string, unknown>;
};

type CheckConfig = {
  enabled: boolean;
  graceSeconds: number;
  alertOnFailure: boolean;
  alertOnMiss: boolean;
};

function parseJson<T>(value: string | null, fallback: T): T {
  if (!value) {
    return fallback;
  }
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function asBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    if (value.toLowerCase() === "true") {
      return true;
    }
    if (value.toLowerCase() === "false") {
      return false;
    }
  }
  return fallback;
}

function asInt(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function parsePositiveIntOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    const normalized = Math.trunc(value);
    return normalized > 0 ? normalized : null;
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return null;
}

function nonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function nowIso(): string {
  return new Date().toISOString();
}

export class MonitorService {
  private db: any;
  private config: MonitorConfig;

  constructor(
    db: any,
    config: MonitorConfig
  ) {
    this.db = db;
    this.config = config;
  }

  normalizeEvents(rawEvents: unknown[]): EventPayload[] {
    const out: EventPayload[] = [];
    for (const raw of rawEvents) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const rec = raw as Record<string, unknown>;
      const sourceType = nonEmptyString(rec.sourceType)?.toLowerCase();
      const level = nonEmptyString(rec.level)?.toUpperCase();
      const message = nonEmptyString(rec.message);
      const eventType = nonEmptyString(rec.eventType);

      if (!sourceType || !["chief", "worker", "monitor"].includes(sourceType)) {
        continue;
      }
      if (!level || !["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"].includes(level)) {
        continue;
      }
      if (!message || !eventType) {
        continue;
      }

      const metadata =
        rec.metadata && typeof rec.metadata === "object" && !Array.isArray(rec.metadata)
          ? (rec.metadata as Record<string, unknown>)
          : {};

      out.push({
        sourceType: sourceType as EventPayload["sourceType"],
        eventType,
        level: level as EventPayload["level"],
        message,
        eventAt: nonEmptyString(rec.eventAt) ?? nowIso(),
        jobName: nonEmptyString(rec.jobName),
        scriptPath: nonEmptyString(rec.scriptPath),
        runId: nonEmptyString(rec.runId),
        scheduledFor: nonEmptyString(rec.scheduledFor),
        success: typeof rec.success === "boolean" ? rec.success : null,
        returnCode: typeof rec.returnCode === "number" ? Math.trunc(rec.returnCode) : null,
        durationMs: typeof rec.durationMs === "number" ? Math.trunc(rec.durationMs) : null,
        metadata,
      });
    }
    return out;
  }

  ingestEvents(rawEvents: unknown[]): { inserted: number; dropped: number } {
    const events = this.normalizeEvents(rawEvents);
    const dropped = rawEvents.length - events.length;
    if (!events.length) {
      return { inserted: 0, dropped };
    }

    const receivedAt = nowIso();
    for (const event of events) {
      this.db.insert(telemetryEvents).values({
        receivedAt,
        eventAt: event.eventAt,
        sourceType: event.sourceType,
        eventType: event.eventType,
        level: event.level,
        message: event.message,
        jobName: event.jobName,
        scriptPath: event.scriptPath,
        runId: event.runId,
        scheduledFor: event.scheduledFor,
        success: event.success,
        returnCode: event.returnCode,
        durationMs: event.durationMs,
        metadataJson: JSON.stringify(event.metadata ?? {}),
      }).run();

      this.applyEventToChecks(event);
    }

    return { inserted: events.length, dropped };
  }

  private checkConfigFromEvent(event: EventPayload): CheckConfig {
    const metadata = event.metadata ?? {};
    return {
      enabled: asBool(metadata.check_enabled, true),
      graceSeconds: Math.max(0, asInt(metadata.grace_seconds, 120)),
      alertOnFailure: asBool(metadata.alert_on_failure, true),
      alertOnMiss: asBool(metadata.alert_on_miss, true),
    };
  }

  private ensureCheckState(jobName: string, cfg: CheckConfig): void {
    const now = nowIso();
    const existing = this.db
      .select()
      .from(checkStates)
      .where(eq(checkStates.jobName, jobName))
      .limit(1)
      .get();

    if (!existing) {
      this.db.insert(checkStates).values({
        jobName,
        enabled: cfg.enabled,
        alertOnFailure: cfg.alertOnFailure,
        alertOnMiss: cfg.alertOnMiss,
        graceSeconds: cfg.graceSeconds,
        status: "UP",
        updatedAt: now,
      }).run();
      return;
    }

    this.db
      .update(checkStates)
      .set({
        enabled: cfg.enabled,
        alertOnFailure: cfg.alertOnFailure,
        alertOnMiss: cfg.alertOnMiss,
        graceSeconds: cfg.graceSeconds,
        updatedAt: now,
      })
      .where(eq(checkStates.jobName, jobName))
      .run();
  }

  private closeOpenAlerts(jobName: string, alertType: "FAILURE" | "MISSED" | "RECOVERY"): number {
    const now = nowIso();
    const existing = this.db
      .select({ id: alerts.id })
      .from(alerts)
      .where(and(eq(alerts.jobName, jobName), eq(alerts.alertType, alertType), eq(alerts.status, "OPEN")))
      .all();

    const count = existing.length;
    if (count > 0) {
      this.db
        .update(alerts)
        .set({ status: "CLOSED", closedAt: now })
        .where(and(eq(alerts.jobName, jobName), eq(alerts.alertType, alertType), eq(alerts.status, "OPEN")))
        .run();
    }
    return count;
  }

  private closeStaleRecoveryAlerts(ttlSeconds: number = DEFAULT_RECOVERY_AUTO_CLOSE_SECONDS): number {
    if (ttlSeconds <= 0) {
      return 0;
    }
    const now = nowIso();
    const cutoff = new Date(Date.now() - ttlSeconds * 1000).toISOString();
    const result = this.db
      .update(alerts)
      .set({ status: "CLOSED", closedAt: now })
      .where(and(eq(alerts.alertType, "RECOVERY"), eq(alerts.status, "OPEN"), lt(alerts.openedAt, cutoff)))
      .run();
    return result?.changes ?? 0;
  }

  private openAlert(params: {
    jobName: string;
    alertType: "FAILURE" | "MISSED" | "RECOVERY";
    severity: "WARN" | "ERROR" | "CRITICAL" | "INFO";
    title: string;
    dedupeKey: string;
    details: Record<string, unknown>;
  }): void {
    const now = nowIso();

    const existing = this.db
      .select({ id: alerts.id })
      .from(alerts)
      .where(and(eq(alerts.dedupeKey, params.dedupeKey), eq(alerts.status, "OPEN")))
      .limit(1)
      .get();

    if (existing) {
      return;
    }

    this.db
      .insert(alerts)
      .values({
        jobName: params.jobName,
        alertType: params.alertType,
        severity: params.severity,
        status: "OPEN",
        openedAt: now,
        dedupeKey: params.dedupeKey,
        title: params.title,
        detailsJson: JSON.stringify(params.details),
      })
      .run();

    const insertedAlert = this.db
      .select({ id: alerts.id })
      .from(alerts)
      .where(and(eq(alerts.dedupeKey, params.dedupeKey), eq(alerts.status, "OPEN")))
      .limit(1)
      .get();

    const alertId = insertedAlert?.id;
    if (alertId !== undefined) {
      this.db.insert(alertDeliveries).values({
        alertId,
        channel: "webhook",
        attemptedAt: now,
        status: "STUB",
        responseCode: null,
        errorText: "Webhook integration not configured in v1.",
      }).run();
    }
  }

  private applyEventToChecks(event: EventPayload): void {
    const jobName = event.jobName;
    if (!jobName) {
      return;
    }

    const cfg = this.checkConfigFromEvent(event);
    this.ensureCheckState(jobName, cfg);
    const now = nowIso();

    if (event.eventType === "job.next_scheduled") {
      const nextRunRaw = event.metadata?.next_run_at;
      const nextRunAt = nonEmptyString(nextRunRaw) ?? null;
      this.db
        .update(checkStates)
        .set({ expectedNextAt: nextRunAt, updatedAt: now })
        .where(eq(checkStates.jobName, jobName))
        .run();
      return;
    }

    if (!HEARTBEAT_EVENT_TYPES.has(event.eventType)) {
      return;
    }

    const prev = this.db
      .select()
      .from(checkStates)
      .where(eq(checkStates.jobName, jobName))
      .limit(1)
      .get();

    // Keep RECOVERY alerts transient: close prior ones when the next job heartbeat arrives.
    this.closeOpenAlerts(jobName, "RECOVERY");

    this.db
      .update(checkStates)
      .set({
        lastHeartbeatAt: event.eventAt,
        status: "UP",
        updatedAt: now,
      })
      .where(eq(checkStates.jobName, jobName))
      .run();

    if (cfg.alertOnMiss) {
      const closedMissed = this.closeOpenAlerts(jobName, "MISSED");
      if (closedMissed > 0) {
        this.openAlert({
          jobName,
          alertType: "RECOVERY",
          severity: "INFO",
          dedupeKey: `${jobName}:RECOVERY:MISSED`,
          title: `Job ${jobName} recovered from missed heartbeat`,
          details: { recoveredAt: event.eventAt, sourceEvent: event.eventType },
        });
      }
    }

    const failed = event.eventType === "job.failed" || (event.eventType === "job.completed" && event.success === false);
    const succeeded = event.eventType === "job.completed" && event.success === true;

    if (failed) {
      const failures = (prev?.consecutiveFailures ?? 0) + 1;
      this.db
        .update(checkStates)
        .set({
          lastFailureAt: event.eventAt,
          consecutiveFailures: failures,
          updatedAt: now,
        })
        .where(eq(checkStates.jobName, jobName))
        .run();

      if (cfg.alertOnFailure) {
        this.openAlert({
          jobName,
          alertType: "FAILURE",
          severity: "ERROR",
          dedupeKey: `${jobName}:FAILURE`,
          title: `Job ${jobName} failed`,
          details: {
            eventType: event.eventType,
            returnCode: event.returnCode,
            runId: event.runId,
          },
        });
      }
      return;
    }

    if (succeeded) {
      this.db
        .update(checkStates)
        .set({
          lastSuccessAt: event.eventAt,
          consecutiveFailures: 0,
          updatedAt: now,
        })
        .where(eq(checkStates.jobName, jobName))
        .run();

      if (cfg.alertOnFailure) {
        const closedFailures = this.closeOpenAlerts(jobName, "FAILURE");
        if (closedFailures > 0) {
          this.openAlert({
            jobName,
            alertType: "RECOVERY",
            severity: "INFO",
            dedupeKey: `${jobName}:RECOVERY:FAILURE`,
            title: `Job ${jobName} recovered from failure`,
            details: { recoveredAt: event.eventAt, sourceEvent: event.eventType },
          });
        }
      }
    }
  }

  evaluateChecks(): { late: number; down: number; openedMissed: number } {
    this.closeStaleRecoveryAlerts();

    const rows = this.db.select().from(checkStates).where(eq(checkStates.enabled, true)).all();
    const now = new Date();
    let late = 0;
    let down = 0;
    let openedMissed = 0;

    for (const row of rows) {
      if (!row.expectedNextAt) {
        continue;
      }
      const expected = new Date(row.expectedNextAt);
      if (Number.isNaN(expected.getTime())) {
        continue;
      }

      const diffSeconds = Math.floor((now.getTime() - expected.getTime()) / 1000);
      if (diffSeconds > row.graceSeconds) {
        down += 1;
        if (row.status !== "DOWN") {
          this.db
            .update(checkStates)
            .set({ status: "DOWN", updatedAt: nowIso() })
            .where(eq(checkStates.jobName, row.jobName))
            .run();

          if (row.alertOnMiss) {
            this.openAlert({
              jobName: row.jobName,
              alertType: "MISSED",
              severity: "WARN",
              dedupeKey: `${row.jobName}:MISSED`,
              title: `Job ${row.jobName} missed expected heartbeat`,
              details: {
                expectedNextAt: row.expectedNextAt,
                graceSeconds: row.graceSeconds,
                observedAt: now.toISOString(),
              },
            });
            openedMissed += 1;
          }
        }
        continue;
      }

      if (diffSeconds > 0) {
        late += 1;
        if (row.status !== "LATE") {
          this.db
            .update(checkStates)
            .set({ status: "LATE", updatedAt: nowIso() })
            .where(eq(checkStates.jobName, row.jobName))
            .run();
        }
        continue;
      }

      if (row.status !== "UP") {
        this.db
          .update(checkStates)
          .set({ status: "UP", updatedAt: nowIso() })
          .where(eq(checkStates.jobName, row.jobName))
          .run();
      }
    }

    return { late, down, openedMissed };
  }

  pruneTelemetry(retentionDays: number): number {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();
    const removed = this.db.delete(telemetryEvents).where(lt(telemetryEvents.eventAt, cutoff)).run();
    return removed.changes;
  }

  getSummary() {
    const statusCounts = this.db
      .select({ status: checkStates.status, count: sql<number>`count(*)` })
      .from(checkStates)
      .groupBy(checkStates.status)
      .all();

    const activeAlertCounts = this.db
      .select({ alertType: alerts.alertType, count: sql<number>`count(*)` })
      .from(alerts)
      .where(eq(alerts.status, "OPEN"))
      .groupBy(alerts.alertType)
      .all();

    const totalEventsRow = this.db
      .select({ count: sql<number>`count(*)` })
      .from(telemetryEvents)
      .get();

    const latestEventRow = this.db
      .select({ latest: sql<string | null>`max(${telemetryEvents.eventAt})` })
      .from(telemetryEvents)
      .get();

    const latestChiefHeartbeat = this.db
      .select({
        eventAt: telemetryEvents.eventAt,
        metadataJson: telemetryEvents.metadataJson,
      })
      .from(telemetryEvents)
      .where(
        and(
          eq(telemetryEvents.sourceType, "chief"),
          eq(telemetryEvents.eventType, CHIEF_HEARTBEAT_EVENT_TYPE)
        )
      )
      .orderBy(desc(telemetryEvents.eventAt))
      .limit(1)
      .get() as { eventAt: string; metadataJson: string } | undefined;

    const heartbeatMeta = parseJson<Record<string, unknown>>(latestChiefHeartbeat?.metadataJson ?? null, {});
    const pingIntervalSeconds = parsePositiveIntOrNull(heartbeatMeta.ping_interval_seconds);
    const offlineAfterSeconds =
      pingIntervalSeconds !== null
        ? Math.max(MIN_CHIEF_OFFLINE_AFTER_SECONDS, pingIntervalSeconds * 2)
        : DEFAULT_CHIEF_OFFLINE_AFTER_SECONDS;

    let chiefOnline = false;
    const lastHeartbeatAt = latestChiefHeartbeat?.eventAt ?? null;
    if (lastHeartbeatAt) {
      const lastHeartbeatMs = Date.parse(lastHeartbeatAt);
      if (Number.isFinite(lastHeartbeatMs)) {
        chiefOnline = Date.now() - lastHeartbeatMs <= offlineAfterSeconds * 1000;
      }
    }

    return {
      checks: statusCounts,
      activeAlerts: activeAlertCounts,
      totalEvents: totalEventsRow?.count ?? 0,
      latestEventAt: latestEventRow?.latest ?? null,
      chief: {
        online: chiefOnline,
        lastHeartbeatAt,
        pingIntervalSeconds,
        offlineAfterSeconds,
      },
    };
  }

  getJobsStatus() {
    const rows = this.db
      .select()
      .from(checkStates)
      .orderBy(checkStates.jobName)
      .all();

    return rows.map((row: any) => {
      const latest = this.db
        .select({
          eventAt: telemetryEvents.eventAt,
          eventType: telemetryEvents.eventType,
          level: telemetryEvents.level,
          message: telemetryEvents.message,
          success: telemetryEvents.success,
        })
        .from(telemetryEvents)
        .where(eq(telemetryEvents.jobName, row.jobName))
        .orderBy(desc(telemetryEvents.eventAt))
        .limit(1)
        .get();

      return {
        ...row,
        latestEvent: latest ?? null,
      };
    });
  }

  getJobDetails(jobName: string) {
    const check = this.db
      .select()
      .from(checkStates)
      .where(eq(checkStates.jobName, jobName))
      .limit(1)
      .get();

    const events = this.db
      .select()
      .from(telemetryEvents)
      .where(eq(telemetryEvents.jobName, jobName))
      .orderBy(desc(telemetryEvents.eventAt))
      .limit(100)
      .all()
      .map((row: any) => ({ ...row, metadata: parseJson<Record<string, unknown>>(row.metadataJson, {}) }));

    const openAlerts = this.db
      .select()
      .from(alerts)
      .where(and(eq(alerts.jobName, jobName), eq(alerts.status, "OPEN")))
      .orderBy(desc(alerts.openedAt))
      .all()
      .map((row: any) => ({ ...row, details: parseJson<Record<string, unknown>>(row.detailsJson, {}) }));

    return {
      check: check ?? null,
      events,
      openAlerts,
    };
  }

  listAlerts(filters: {
    jobName?: string;
    status?: string;
    alertType?: string;
    severity?: string;
    limit: number;
    offset: number;
  }) {
    const clauses: any[] = [];
    if (filters.jobName) {
      clauses.push(eq(alerts.jobName, filters.jobName));
    }
    if (filters.status) {
      clauses.push(eq(alerts.status, filters.status));
    }
    if (filters.alertType) {
      clauses.push(eq(alerts.alertType, filters.alertType));
    }
    if (filters.severity) {
      clauses.push(eq(alerts.severity, filters.severity));
    }

    const where = clauses.length ? and(...clauses) : undefined;

    const query = this.db
      .select()
      .from(alerts)
      .orderBy(desc(alerts.openedAt))
      .limit(filters.limit)
      .offset(filters.offset);

    const rows = where ? query.where(where).all() : query.all();
    return rows.map((row: any) => ({ ...row, details: parseJson<Record<string, unknown>>(row.detailsJson, {}) }));
  }

  closeAlertById(alertId: number, reason: string) {
    const existing = this.db
      .select({
        id: alerts.id,
        status: alerts.status,
        closedAt: alerts.closedAt,
      })
      .from(alerts)
      .where(eq(alerts.id, alertId))
      .limit(1)
      .get() as { id: number; status: string; closedAt: string | null } | undefined;

    if (!existing) {
      return { found: false as const, updated: false as const };
    }

    if (existing.status === "CLOSED") {
      return {
        found: true as const,
        updated: false as const,
        reason,
        alert: existing,
      };
    }

    const closedAt = nowIso();
    this.db
      .update(alerts)
      .set({ status: "CLOSED", closedAt })
      .where(and(eq(alerts.id, alertId), eq(alerts.status, "OPEN")))
      .run();

    return {
      found: true as const,
      updated: true as const,
      reason,
      alert: {
        id: alertId,
        status: "CLOSED",
        closedAt,
      },
    };
  }

  listEvents(filters: {
    jobName?: string;
    scriptPath?: string;
    level?: string;
    eventType?: string;
    from?: string;
    to?: string;
    limit: number;
    offset: number;
  }) {
    const clauses: any[] = [];
    if (filters.jobName) {
      clauses.push(eq(telemetryEvents.jobName, filters.jobName));
    }
    if (filters.scriptPath) {
      clauses.push(eq(telemetryEvents.scriptPath, filters.scriptPath));
    }
    if (filters.level) {
      clauses.push(eq(telemetryEvents.level, filters.level));
    }
    if (filters.eventType) {
      clauses.push(eq(telemetryEvents.eventType, filters.eventType));
    }
    if (filters.from) {
      clauses.push(sql`${telemetryEvents.eventAt} >= ${filters.from}`);
    }
    if (filters.to) {
      clauses.push(sql`${telemetryEvents.eventAt} <= ${filters.to}`);
    }

    const where = clauses.length ? and(...clauses) : undefined;

    const query = this.db
      .select()
      .from(telemetryEvents)
      .orderBy(desc(telemetryEvents.eventAt))
      .limit(filters.limit)
      .offset(filters.offset);

    const rows = where ? query.where(where).all() : query.all();
    return rows.map((row: any) => ({ ...row, metadata: parseJson<Record<string, unknown>>(row.metadataJson, {}) }));
  }

  startBackgroundLoops(): Array<NodeJS.Timeout> {
    const evaluator = setInterval(() => {
      this.evaluateChecks();
    }, this.config.evaluatorIntervalSeconds * 1000);

    const retention = setInterval(() => {
      this.pruneTelemetry(this.config.retentionDays);
    }, this.config.retentionIntervalSeconds * 1000);

    return [evaluator, retention];
  }
}
