import { Link, useParams } from "react-router-dom";
import { useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { closeAlert, getJobDetails } from "../lib/api";
import { formatDateTime, formatDuration } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import type { AlertRow, EventRow } from "../lib/types";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { JsonDetails } from "../components/JsonDetails";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";

const eventColumns: Array<ColumnDef<EventRow>> = [
  { accessorKey: "eventAt", header: "Event Time", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
  { accessorKey: "eventType", header: "Type" },
  { accessorKey: "level", header: "Level", cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} /> },
  { accessorKey: "message", header: "Message" },
  { accessorKey: "durationMs", header: "Duration", cell: ({ getValue }) => formatDuration((getValue() as number | null) ?? null) },
  {
    id: "metadata",
    header: "Metadata",
    cell: ({ row }) => <JsonDetails title="Details" value={row.original.metadata ?? {}} />,
  },
];

export function JobDetailPage() {
  const queryClient = useQueryClient();
  const params = useParams<{ jobName: string }>();
  const jobName = params.jobName ?? "";

  const query = useQuery({
    queryKey: ["job-detail", jobName],
    queryFn: () => getJobDetails(jobName),
    enabled: Boolean(jobName),
    refetchInterval: () => visiblePollingInterval(),
  });

  const closeMutation = useMutation({
    mutationFn: (input: { alertId: number }) => closeAlert(input.alertId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["job-detail", jobName] }),
        queryClient.invalidateQueries({ queryKey: ["alerts"] }),
        queryClient.invalidateQueries({ queryKey: ["summary"] }),
        queryClient.invalidateQueries({ queryKey: ["jobs"] }),
      ]);
    },
  });

  const alertColumns = useMemo<Array<ColumnDef<AlertRow>>>(
    () => [
      { accessorKey: "alertType", header: "Type" },
      {
        accessorKey: "severity",
        header: "Severity",
        cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} />,
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} />,
      },
      { accessorKey: "openedAt", header: "Opened", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
      { accessorKey: "title", header: "Title" },
      {
        id: "actions",
        header: "Actions",
        cell: ({ row }) => {
          const isOpen = row.original.status === "OPEN";
          const isPending = closeMutation.isPending && closeMutation.variables?.alertId === row.original.id;
          if (!isOpen) {
            return <span className="muted">-</span>;
          }
          return (
            <button
              type="button"
              className="button button--danger"
              disabled={isPending}
              onClick={() => closeMutation.mutate({ alertId: row.original.id })}
            >
              {isPending ? "Closing..." : "Close"}
            </button>
          );
        },
      },
    ],
    [closeMutation.isPending, closeMutation.mutate, closeMutation.variables?.alertId]
  );

  return (
    <section className="stack">
      <PageHeader
        title={`Job Detail: ${jobName}`}
        subtitle="Live check state, open alerts, and recent events for this job."
        actions={
          <div className="inline-actions">
            <Link className="button" to="/jobs">
              Back to Jobs
            </Link>
            <button type="button" className="button button--primary" onClick={() => void query.refetch()}>
              Refresh
            </button>
          </div>
        }
      />

      {query.error ? <ErrorBanner message={(query.error as Error).message} /> : null}
      {closeMutation.error ? <ErrorBanner message={(closeMutation.error as Error).message} /> : null}

      <article className="card">
        <h3>Check State</h3>
        {query.data?.check ? (
          <div className="detail-grid mono">
            <p>
              Status: <StatusBadge value={query.data.check.status} />
            </p>
            <p>Expected Next: {formatDateTime(query.data.check.expectedNextAt)}</p>
            <p>Last Heartbeat: {formatDateTime(query.data.check.lastHeartbeatAt)}</p>
            <p>Last Success: {formatDateTime(query.data.check.lastSuccessAt)}</p>
            <p>Last Failure: {formatDateTime(query.data.check.lastFailureAt)}</p>
            <p>Consecutive Failures: {query.data.check.consecutiveFailures}</p>
            <p>Grace Seconds: {query.data.check.graceSeconds}</p>
            <p>Updated At: {formatDateTime(query.data.check.updatedAt)}</p>
          </div>
        ) : (
          <p className="muted">No check state available.</p>
        )}
      </article>

      <article className="card">
        <h3>Open Alerts</h3>
        <DataTable
          data={query.data?.openAlerts ?? []}
          columns={alertColumns}
          emptyTitle="No open alerts"
          emptyMessage="This job has no active alerts."
        />
      </article>

      <article className="card">
        <h3>Recent Events</h3>
        <DataTable
          data={query.data?.events ?? []}
          columns={eventColumns}
          emptyTitle="No events"
          emptyMessage="No events have been stored for this job yet."
        />
      </article>
    </section>
  );
}
