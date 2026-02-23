import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getJobDetails } from "../lib/api";
import { formatDateTime, formatDuration } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { JsonDetails } from "../components/JsonDetails";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";
const alertColumns = [
    { accessorKey: "alertType", header: "Type" },
    { accessorKey: "severity", header: "Severity", cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }) },
    { accessorKey: "status", header: "Status", cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }) },
    { accessorKey: "openedAt", header: "Opened", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
    { accessorKey: "title", header: "Title" },
];
const eventColumns = [
    { accessorKey: "eventAt", header: "Event Time", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
    { accessorKey: "eventType", header: "Type" },
    { accessorKey: "level", header: "Level", cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }) },
    { accessorKey: "message", header: "Message" },
    { accessorKey: "durationMs", header: "Duration", cell: ({ getValue }) => formatDuration(getValue() ?? null) },
    {
        id: "metadata",
        header: "Metadata",
        cell: ({ row }) => _jsx(JsonDetails, { title: "Details", value: row.original.metadata ?? {} }),
    },
];
export function JobDetailPage() {
    const params = useParams();
    const jobName = params.jobName ?? "";
    const query = useQuery({
        queryKey: ["job-detail", jobName],
        queryFn: () => getJobDetails(jobName),
        enabled: Boolean(jobName),
        refetchInterval: () => visiblePollingInterval(),
    });
    return (_jsxs("section", { className: "stack", children: [_jsx(PageHeader, { title: `Job Detail: ${jobName}`, subtitle: "Live check state, open alerts, and recent events for this job.", actions: _jsxs("div", { className: "inline-actions", children: [_jsx(Link, { className: "button", to: "/jobs", children: "Back to Jobs" }), _jsx("button", { type: "button", className: "button button--primary", onClick: () => void query.refetch(), children: "Refresh" })] }) }), query.error ? _jsx(ErrorBanner, { message: query.error.message }) : null, _jsxs("article", { className: "card", children: [_jsx("h3", { children: "Check State" }), query.data?.check ? (_jsxs("div", { className: "detail-grid mono", children: [_jsxs("p", { children: ["Status: ", _jsx(StatusBadge, { value: query.data.check.status })] }), _jsxs("p", { children: ["Expected Next: ", formatDateTime(query.data.check.expectedNextAt)] }), _jsxs("p", { children: ["Last Heartbeat: ", formatDateTime(query.data.check.lastHeartbeatAt)] }), _jsxs("p", { children: ["Last Success: ", formatDateTime(query.data.check.lastSuccessAt)] }), _jsxs("p", { children: ["Last Failure: ", formatDateTime(query.data.check.lastFailureAt)] }), _jsxs("p", { children: ["Consecutive Failures: ", query.data.check.consecutiveFailures] }), _jsxs("p", { children: ["Grace Seconds: ", query.data.check.graceSeconds] }), _jsxs("p", { children: ["Updated At: ", formatDateTime(query.data.check.updatedAt)] })] })) : (_jsx("p", { className: "muted", children: "No check state available." }))] }), _jsxs("article", { className: "card", children: [_jsx("h3", { children: "Open Alerts" }), _jsx(DataTable, { data: query.data?.openAlerts ?? [], columns: alertColumns, emptyTitle: "No open alerts", emptyMessage: "This job has no active alerts." })] }), _jsxs("article", { className: "card", children: [_jsx("h3", { children: "Recent Events" }), _jsx(DataTable, { data: query.data?.events ?? [], columns: eventColumns, emptyTitle: "No events", emptyMessage: "No events have been stored for this job yet." })] })] }));
}
