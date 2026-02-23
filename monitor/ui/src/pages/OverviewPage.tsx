import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, Pie, PieChart, Cell, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { ColumnDef } from "@tanstack/react-table";

import { getAlerts, getSummary } from "../lib/api";
import { formatCount, formatDateTime, formatRelative } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import type { AlertRow } from "../lib/types";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";

const alertColumns: Array<ColumnDef<AlertRow>> = [
  {
    accessorKey: "jobName",
    header: "Job",
  },
  {
    accessorKey: "alertType",
    header: "Type",
  },
  {
    accessorKey: "severity",
    header: "Severity",
    cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} />,
  },
  {
    accessorKey: "openedAt",
    header: "Opened",
    cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")),
  },
  {
    accessorKey: "title",
    header: "Title",
  },
];

const chartColors = ["#178f9f", "#f28f3b", "#c44536", "#2a7f62", "#8f5e4f"];

export function OverviewPage() {
  const summaryQuery = useQuery({
    queryKey: ["summary"],
    queryFn: () => getSummary(),
    refetchInterval: () => visiblePollingInterval(),
  });

  const alertsQuery = useQuery({
    queryKey: ["alerts", { status: "OPEN", limit: 10, offset: 0 }],
    queryFn: () => getAlerts({ status: "OPEN", limit: 10, offset: 0 }),
    refetchInterval: () => visiblePollingInterval(),
  });

  const checksMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const item of summaryQuery.data?.checks ?? []) {
      map.set(item.status.toUpperCase(), item.count);
    }
    return map;
  }, [summaryQuery.data?.checks]);

  const activeAlertsMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const item of summaryQuery.data?.activeAlerts ?? []) {
      map.set(item.alertType.toUpperCase(), item.count);
    }
    return map;
  }, [summaryQuery.data?.activeAlerts]);

  const isLoading = summaryQuery.isLoading || alertsQuery.isLoading;
  const chiefPresence = summaryQuery.data?.chief;
  const chiefOnline = chiefPresence?.online ?? false;
  const chiefLastHeartbeat = chiefPresence?.lastHeartbeatAt ?? null;
  const chiefIntervalSeconds = chiefPresence?.pingIntervalSeconds ?? null;
  const chiefOfflineAfterSeconds = chiefPresence?.offlineAfterSeconds ?? 0;

  const refresh = () => {
    void summaryQuery.refetch();
    void alertsQuery.refetch();
  };

  return (
    <section className="stack">
      <PageHeader
        title="System Overview"
        subtitle="Live status across checks, alerts, and telemetry volume."
        actions={
          <button type="button" onClick={refresh} className="button button--primary" disabled={isLoading}>
            Refresh
          </button>
        }
      />

      {summaryQuery.error ? <ErrorBanner message={(summaryQuery.error as Error).message} /> : null}
      {alertsQuery.error ? <ErrorBanner message={(alertsQuery.error as Error).message} /> : null}

      <div className="metrics-grid">
        <MetricCard
          label="Total Events"
          value={formatCount(summaryQuery.data?.totalEvents ?? 0)}
          detail={`Latest: ${formatDateTime(summaryQuery.data?.latestEventAt)}`}
        />
        <MetricCard
          label="Checks Up"
          value={formatCount(checksMap.get("UP") ?? 0)}
          detail={`Late: ${formatCount(checksMap.get("LATE") ?? 0)} | Down: ${formatCount(
            checksMap.get("DOWN") ?? 0
          )}`}
        />
        <MetricCard
          label="Open Alerts"
          value={formatCount((alertsQuery.data?.alerts ?? []).length)}
          detail={`Failure: ${formatCount(activeAlertsMap.get("FAILURE") ?? 0)} | Missed: ${formatCount(
            activeAlertsMap.get("MISSED") ?? 0
          )}`}
        />
        <MetricCard
          label="Last Event Age"
          value={formatRelative(summaryQuery.data?.latestEventAt)}
          detail="Calculated from monitor event stream"
        />
        <MetricCard
          label="Chief Runtime"
          value={
            <span className="presence-indicator">
              <span
                className={`presence-dot ${chiefOnline ? "is-online" : "is-offline"}`}
                aria-label={chiefOnline ? "Chief online" : "Chief offline"}
              />
              <span>{chiefOnline ? "ONLINE" : "OFFLINE"}</span>
            </span>
          }
          detail={
            chiefLastHeartbeat
              ? `Last ping ${formatRelative(chiefLastHeartbeat)} (${formatDateTime(chiefLastHeartbeat)}), interval ${
                  chiefIntervalSeconds !== null ? `${chiefIntervalSeconds}s` : "unknown"
                }`
              : `No heartbeat seen yet. Offline timeout ${chiefOfflineAfterSeconds}s`
          }
        />
      </div>

      <div className="charts-grid">
        <article className="card card--chart">
          <h3>Checks by Status</h3>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={summaryQuery.data?.checks ?? []}>
              <CartesianGrid strokeDasharray="4 4" stroke="#d7d4cd" />
              <XAxis dataKey="status" />
              <YAxis allowDecimals={false} />
              <Tooltip />
              <Bar dataKey="count" fill="#178f9f" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </article>

        <article className="card card--chart">
          <h3>Open Alerts by Type</h3>
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie
                data={summaryQuery.data?.activeAlerts ?? []}
                dataKey="count"
                nameKey="alertType"
                cx="50%"
                cy="50%"
                outerRadius={84}
                innerRadius={42}
                paddingAngle={4}
              >
                {(summaryQuery.data?.activeAlerts ?? []).map((entry, index) => (
                  <Cell key={`${entry.alertType}-${index}`} fill={chartColors[index % chartColors.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </article>
      </div>

      <article className="card">
        <h3>Recent Open Alerts</h3>
        <DataTable
          data={alertsQuery.data?.alerts ?? []}
          columns={alertColumns}
          emptyTitle="No open alerts"
          emptyMessage="System currently has no active alerts."
        />
      </article>
    </section>
  );
}
