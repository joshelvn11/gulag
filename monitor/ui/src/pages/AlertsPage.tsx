import { useMemo, useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { closeAlert, getAlerts } from "../lib/api";
import { formatDateTime } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import type { AlertRow } from "../lib/types";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { JsonDetails } from "../components/JsonDetails";
import { PageHeader } from "../components/PageHeader";
import { PaginationControls } from "../components/PaginationControls";
import { StatusBadge } from "../components/StatusBadge";

const PAGE_SIZE = 25;

export function AlertsPage() {
  const queryClient = useQueryClient();
  const [jobName, setJobName] = useState("");
  const [status, setStatus] = useState("");
  const [alertType, setAlertType] = useState("");
  const [severity, setSeverity] = useState("");
  const [page, setPage] = useState(0);

  const params = useMemo(
    () => ({
      jobName: jobName.trim() || undefined,
      status: status || undefined,
      alertType: alertType || undefined,
      severity: severity || undefined,
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
    }),
    [alertType, jobName, page, severity, status]
  );

  const query = useQuery({
    queryKey: ["alerts", params],
    queryFn: () => getAlerts(params),
    refetchInterval: () => visiblePollingInterval(),
  });

  const closeMutation = useMutation({
    mutationFn: (input: { alertId: number }) => closeAlert(input.alertId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["alerts"] }),
        queryClient.invalidateQueries({ queryKey: ["summary"] }),
        queryClient.invalidateQueries({ queryKey: ["jobs"] }),
      ]);
    },
  });

  const columns = useMemo<Array<ColumnDef<AlertRow>>>(
    () => [
      { accessorKey: "jobName", header: "Job" },
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
      { accessorKey: "closedAt", header: "Closed", cell: ({ getValue }) => formatDateTime((getValue() as string | null) ?? null) },
      { accessorKey: "title", header: "Title" },
      {
        id: "details",
        header: "Details",
        cell: ({ row }) => <JsonDetails value={row.original.details ?? {}} />,
      },
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

  const onFilterSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setPage(0);
    void query.refetch();
  };

  return (
    <section className="stack">
      <PageHeader
        title="Alerts Explorer"
        subtitle="Filter and inspect monitor alerts across jobs and severities."
        actions={
          <button type="button" className="button button--primary" onClick={() => void query.refetch()}>
            Refresh
          </button>
        }
      />

      {query.error ? <ErrorBanner message={(query.error as Error).message} /> : null}
      {closeMutation.error ? <ErrorBanner message={(closeMutation.error as Error).message} /> : null}

      <form className="filters" onSubmit={onFilterSubmit}>
        <label>
          Job
          <input value={jobName} onChange={(event) => setJobName(event.target.value)} placeholder="sample-etl-pipeline" />
        </label>
        <label>
          Status
          <select value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="">Any</option>
            <option value="OPEN">OPEN</option>
            <option value="CLOSED">CLOSED</option>
          </select>
        </label>
        <label>
          Alert Type
          <select value={alertType} onChange={(event) => setAlertType(event.target.value)}>
            <option value="">Any</option>
            <option value="FAILURE">FAILURE</option>
            <option value="MISSED">MISSED</option>
            <option value="RECOVERY">RECOVERY</option>
          </select>
        </label>
        <label>
          Severity
          <select value={severity} onChange={(event) => setSeverity(event.target.value)}>
            <option value="">Any</option>
            <option value="INFO">INFO</option>
            <option value="WARN">WARN</option>
            <option value="ERROR">ERROR</option>
            <option value="CRITICAL">CRITICAL</option>
          </select>
        </label>
        <button type="submit" className="button">
          Apply Filters
        </button>
      </form>

      <article className="card">
        <DataTable
          data={query.data?.alerts ?? []}
          columns={columns}
          emptyTitle="No alerts"
          emptyMessage="No alerts match the current filters."
        />
        <PaginationControls
          page={page}
          pageSize={PAGE_SIZE}
          receivedCount={(query.data?.alerts ?? []).length}
          onPageChange={setPage}
        />
      </article>
    </section>
  );
}
