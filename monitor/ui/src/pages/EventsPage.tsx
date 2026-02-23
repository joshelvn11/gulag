import { useMemo, useState, type FormEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { getEvents } from "../lib/api";
import { formatDateTime, formatDuration } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import type { EventRow } from "../lib/types";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { JsonDetails } from "../components/JsonDetails";
import { PageHeader } from "../components/PageHeader";
import { PaginationControls } from "../components/PaginationControls";
import { StatusBadge } from "../components/StatusBadge";

const PAGE_SIZE = 50;

const columns: Array<ColumnDef<EventRow>> = [
  { accessorKey: "eventAt", header: "Event Time", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
  { accessorKey: "jobName", header: "Job", cell: ({ getValue }) => (getValue() ? String(getValue()) : "-") },
  { accessorKey: "scriptPath", header: "Script", cell: ({ getValue }) => (getValue() ? String(getValue()) : "-") },
  { accessorKey: "eventType", header: "Type" },
  { accessorKey: "level", header: "Level", cell: ({ getValue }) => <StatusBadge value={String(getValue() ?? "")} /> },
  { accessorKey: "message", header: "Message" },
  { accessorKey: "durationMs", header: "Duration", cell: ({ getValue }) => formatDuration((getValue() as number | null) ?? null) },
  {
    id: "metadata",
    header: "Metadata",
    cell: ({ row }) => <JsonDetails value={row.original.metadata ?? {}} />,
  },
];

export function EventsPage() {
  const [jobName, setJobName] = useState("");
  const [scriptPath, setScriptPath] = useState("");
  const [level, setLevel] = useState("");
  const [eventType, setEventType] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [page, setPage] = useState(0);

  const params = useMemo(
    () => ({
      jobName: jobName.trim() || undefined,
      scriptPath: scriptPath.trim() || undefined,
      level: level || undefined,
      eventType: eventType.trim() || undefined,
      from: from || undefined,
      to: to || undefined,
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
    }),
    [eventType, from, jobName, level, page, scriptPath, to]
  );

  const query = useQuery({
    queryKey: ["events", params],
    queryFn: () => getEvents(params),
    refetchInterval: () => visiblePollingInterval(),
  });

  const onFilterSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setPage(0);
    void query.refetch();
  };

  return (
    <section className="stack">
      <PageHeader
        title="Events Explorer"
        subtitle="Query raw monitor telemetry events with metadata context."
        actions={
          <button type="button" className="button button--primary" onClick={() => void query.refetch()}>
            Refresh
          </button>
        }
      />

      {query.error ? <ErrorBanner message={(query.error as Error).message} /> : null}

      <form className="filters filters--dense" onSubmit={onFilterSubmit}>
        <label>
          Job
          <input value={jobName} onChange={(event) => setJobName(event.target.value)} placeholder="sample-etl-pipeline" />
        </label>
        <label>
          Script Path
          <input value={scriptPath} onChange={(event) => setScriptPath(event.target.value)} placeholder="workers/sample/load_demo.py" />
        </label>
        <label>
          Level
          <select value={level} onChange={(event) => setLevel(event.target.value)}>
            <option value="">Any</option>
            <option value="DEBUG">DEBUG</option>
            <option value="INFO">INFO</option>
            <option value="WARN">WARN</option>
            <option value="ERROR">ERROR</option>
            <option value="CRITICAL">CRITICAL</option>
          </select>
        </label>
        <label>
          Event Type
          <input value={eventType} onChange={(event) => setEventType(event.target.value)} placeholder="job.completed" />
        </label>
        <label>
          From (ISO)
          <input value={from} onChange={(event) => setFrom(event.target.value)} placeholder="2026-01-01T00:00:00Z" />
        </label>
        <label>
          To (ISO)
          <input value={to} onChange={(event) => setTo(event.target.value)} placeholder="2026-01-01T23:59:59Z" />
        </label>
        <button type="submit" className="button">
          Apply Filters
        </button>
      </form>

      <article className="card">
        <DataTable
          data={query.data?.events ?? []}
          columns={columns}
          emptyTitle="No events"
          emptyMessage="No telemetry events match the current filters."
          wrapClassName="table-wrap--events"
          tableClassName="table--events"
        />
        <PaginationControls
          page={page}
          pageSize={PAGE_SIZE}
          receivedCount={(query.data?.events ?? []).length}
          onPageChange={setPage}
        />
      </article>
    </section>
  );
}
