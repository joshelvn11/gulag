import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { getJobsStatus } from "../lib/api";
import { formatDateTime } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import type { JobStatusRow } from "../lib/types";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";

const columns: Array<ColumnDef<JobStatusRow>> = [
  {
    accessorKey: "jobName",
    header: "Job",
    cell: ({ row }) => <Link to={`/jobs/${encodeURIComponent(row.original.jobName)}`}>{row.original.jobName}</Link>,
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} />,
  },
  {
    accessorKey: "expectedNextAt",
    header: "Expected Next",
    cell: ({ getValue }) => formatDateTime(getValue() as string | null),
  },
  {
    accessorKey: "lastHeartbeatAt",
    header: "Last Heartbeat",
    cell: ({ getValue }) => formatDateTime(getValue() as string | null),
  },
  {
    accessorKey: "consecutiveFailures",
    header: "Consecutive Failures",
  },
  {
    id: "latestEvent",
    header: "Latest Event",
    cell: ({ row }) => {
      const event = row.original.latestEvent;
      if (!event) {
        return "-";
      }
      return `${event.eventType} (${formatDateTime(event.eventAt)})`;
    },
  },
];

export function JobsPage() {
  const query = useQuery({
    queryKey: ["jobs"],
    queryFn: () => getJobsStatus(),
    refetchInterval: () => visiblePollingInterval(),
  });

  return (
    <section className="stack">
      <PageHeader
        title="Job Health"
        subtitle="Current check state and latest heartbeat for each monitored job."
        actions={
          <button type="button" className="button button--primary" onClick={() => void query.refetch()}>
            Refresh
          </button>
        }
      />

      {query.error ? <ErrorBanner message={(query.error as Error).message} /> : null}

      <article className="card">
        <DataTable
          data={query.data?.jobs ?? []}
          columns={columns}
          emptyTitle="No jobs"
          emptyMessage="No check state has been recorded yet."
        />
      </article>
    </section>
  );
}
