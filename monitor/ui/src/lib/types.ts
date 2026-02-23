export type CheckStatus = "UP" | "LATE" | "DOWN" | string;

export type SummaryResponse = {
  checks: Array<{ status: string; count: number }>;
  activeAlerts: Array<{ alertType: string; count: number }>;
  totalEvents: number;
  latestEventAt: string | null;
  chief: {
    online: boolean;
    lastHeartbeatAt: string | null;
    pingIntervalSeconds: number | null;
    offlineAfterSeconds: number;
  };
};

export type LatestEvent = {
  eventAt: string;
  eventType: string;
  level: string;
  message: string;
  success: boolean | null;
};

export type CheckState = {
  jobName: string;
  enabled: boolean;
  alertOnFailure: boolean;
  alertOnMiss: boolean;
  graceSeconds: number;
  status: CheckStatus;
  lastHeartbeatAt: string | null;
  expectedNextAt: string | null;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
  consecutiveFailures: number;
  updatedAt: string;
};

export type JobStatusRow = CheckState & {
  latestEvent: LatestEvent | null;
};

export type JobsStatusResponse = {
  jobs: JobStatusRow[];
};

export type AlertRow = {
  id: number;
  jobName: string;
  alertType: string;
  severity: string;
  status: string;
  openedAt: string;
  closedAt: string | null;
  dedupeKey: string;
  title: string;
  detailsJson?: string;
  details?: Record<string, unknown>;
};

export type EventRow = {
  id: number;
  receivedAt: string;
  eventAt: string;
  sourceType: string;
  eventType: string;
  level: string;
  message: string;
  jobName: string | null;
  scriptPath: string | null;
  runId: string | null;
  scheduledFor: string | null;
  success: boolean | null;
  returnCode: number | null;
  durationMs: number | null;
  metadataJson?: string;
  metadata?: Record<string, unknown>;
};

export type JobDetailsResponse = {
  check: CheckState | null;
  events: EventRow[];
  openAlerts: AlertRow[];
};

export type AlertsResponse = {
  alerts: AlertRow[];
  limit: number;
  offset: number;
};

export type CloseAlertResponse = {
  found: boolean;
  updated: boolean;
  reason: string;
  alert: {
    id: number;
    status: string;
    closedAt: string | null;
  };
};

export type EventsResponse = {
  events: EventRow[];
  limit: number;
  offset: number;
};

export type AlertsQueryParams = {
  jobName?: string;
  status?: string;
  alertType?: string;
  severity?: string;
  limit?: number;
  offset?: number;
};

export type EventsQueryParams = {
  jobName?: string;
  scriptPath?: string;
  level?: string;
  eventType?: string;
  from?: string;
  to?: string;
  limit?: number;
  offset?: number;
};
