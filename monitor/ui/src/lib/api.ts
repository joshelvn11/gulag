import type {
  AlertsQueryParams,
  CloseAlertResponse,
  AlertsResponse,
  EventsQueryParams,
  EventsResponse,
  JobDetailsResponse,
  JobsStatusResponse,
  SummaryResponse,
} from "./types";

export type QueryValue = string | number | boolean | null | undefined;

export function buildQueryString(params: Record<string, QueryValue>): string {
  const search = new URLSearchParams();
  for (const [key, rawValue] of Object.entries(params)) {
    if (rawValue === null || rawValue === undefined) {
      continue;
    }
    const value = String(rawValue).trim();
    if (!value) {
      continue;
    }
    search.set(key, value);
  }
  return search.toString();
}

async function parseError(response: Response): Promise<string> {
  try {
    const body = await response.json();
    if (body && typeof body === "object" && "error" in body) {
      const error = (body as { error?: unknown }).error;
      if (typeof error === "string" && error.trim()) {
        return error;
      }
    }
  } catch {
    // Ignore parse errors and fallback to text/status.
  }

  const text = await response.text();
  if (text.trim()) {
    return text;
  }
  return `HTTP ${response.status}`;
}

export async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    const message = await parseError(response);
    throw new Error(message);
  }
  return (await response.json()) as T;
}

export async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body ?? {}),
  });
  if (!response.ok) {
    const message = await parseError(response);
    throw new Error(message);
  }
  return (await response.json()) as T;
}

export function getSummary(): Promise<SummaryResponse> {
  return fetchJson<SummaryResponse>("/v1/status/summary");
}

export function getJobsStatus(): Promise<JobsStatusResponse> {
  return fetchJson<JobsStatusResponse>("/v1/status/jobs");
}

export function getJobDetails(jobName: string): Promise<JobDetailsResponse> {
  return fetchJson<JobDetailsResponse>(`/v1/status/jobs/${encodeURIComponent(jobName)}`);
}

export function getAlerts(params: AlertsQueryParams = {}): Promise<AlertsResponse> {
  const query = buildQueryString({
    jobName: params.jobName,
    status: params.status,
    alertType: params.alertType,
    severity: params.severity,
    limit: params.limit,
    offset: params.offset,
  });
  return fetchJson<AlertsResponse>(`/v1/alerts${query ? `?${query}` : ""}`);
}

export function getEvents(params: EventsQueryParams = {}): Promise<EventsResponse> {
  const query = buildQueryString({
    jobName: params.jobName,
    scriptPath: params.scriptPath,
    level: params.level,
    eventType: params.eventType,
    from: params.from,
    to: params.to,
    limit: params.limit,
    offset: params.offset,
  });
  return fetchJson<EventsResponse>(`/v1/events${query ? `?${query}` : ""}`);
}

export function closeAlert(alertId: number, reason = "manual-ui"): Promise<CloseAlertResponse> {
  return postJson<CloseAlertResponse>(`/v1/alerts/${encodeURIComponent(String(alertId))}/close`, { reason });
}
