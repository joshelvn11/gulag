import { MonitorConfig } from "./types.js";

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export function loadConfig(): MonitorConfig {
  return {
    host: process.env.MONITOR_HOST ?? "127.0.0.1",
    port: envInt("MONITOR_PORT", 7410),
    dbPath: process.env.MONITOR_DB_PATH ?? "./monitor.sqlite",
    apiKey: process.env.MONITOR_API_KEY ?? "",
    retentionDays: envInt("MONITOR_RETENTION_DAYS", 30),
    evaluatorIntervalSeconds: envInt("MONITOR_EVALUATOR_INTERVAL_SECONDS", 15),
    retentionIntervalSeconds: envInt("MONITOR_RETENTION_INTERVAL_SECONDS", 3600),
  };
}
