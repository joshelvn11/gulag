export const EVENT_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"] as const;
export const SOURCE_TYPES = ["chief", "worker", "monitor"] as const;
export const CHECK_STATUSES = ["UP", "LATE", "DOWN"] as const;
export const ALERT_TYPES = ["FAILURE", "MISSED", "RECOVERY"] as const;
export const ALERT_STATUSES = ["OPEN", "CLOSED"] as const;

export type EventLevel = (typeof EVENT_LEVELS)[number];
export type SourceType = (typeof SOURCE_TYPES)[number];
export type CheckStatus = (typeof CHECK_STATUSES)[number];
export type AlertType = (typeof ALERT_TYPES)[number];
export type AlertStatus = (typeof ALERT_STATUSES)[number];

export type IngestEvent = {
  sourceType: SourceType;
  eventType: string;
  level: EventLevel;
  message: string;
  eventAt?: string;
  jobName?: string;
  scriptPath?: string;
  runId?: string;
  scheduledFor?: string;
  success?: boolean;
  returnCode?: number;
  durationMs?: number;
  metadata?: Record<string, unknown>;
};

export type MonitorConfig = {
  host: string;
  port: number;
  dbPath: string;
  apiKey: string;
  retentionDays: number;
  evaluatorIntervalSeconds: number;
  retentionIntervalSeconds: number;
};
