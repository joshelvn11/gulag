import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getJobsStatus } from "../lib/api";
import { formatDateTime } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";
const columns = [
    {
        accessorKey: "jobName",
        header: "Job",
        cell: ({ row }) => _jsx(Link, { to: `/jobs/${encodeURIComponent(row.original.jobName)}`, children: row.original.jobName }),
    },
    {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }),
    },
    {
        accessorKey: "expectedNextAt",
        header: "Expected Next",
        cell: ({ getValue }) => formatDateTime(getValue()),
    },
    {
        accessorKey: "lastHeartbeatAt",
        header: "Last Heartbeat",
        cell: ({ getValue }) => formatDateTime(getValue()),
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
    return (_jsxs("section", { className: "stack", children: [_jsx(PageHeader, { title: "Job Health", subtitle: "Current check state and latest heartbeat for each monitored job.", actions: _jsx("button", { type: "button", className: "button button--primary", onClick: () => void query.refetch(), children: "Refresh" }) }), query.error ? _jsx(ErrorBanner, { message: query.error.message }) : null, _jsx("article", { className: "card", children: _jsx(DataTable, { data: query.data?.jobs ?? [], columns: columns, emptyTitle: "No jobs", emptyMessage: "No check state has been recorded yet." }) })] }));
}
