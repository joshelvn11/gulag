import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getEvents } from "../lib/api";
import { formatDateTime, formatDuration } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { JsonDetails } from "../components/JsonDetails";
import { PageHeader } from "../components/PageHeader";
import { PaginationControls } from "../components/PaginationControls";
import { StatusBadge } from "../components/StatusBadge";
const PAGE_SIZE = 50;
const columns = [
    { accessorKey: "eventAt", header: "Event Time", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
    { accessorKey: "jobName", header: "Job", cell: ({ getValue }) => (getValue() ? String(getValue()) : "-") },
    { accessorKey: "scriptPath", header: "Script", cell: ({ getValue }) => (getValue() ? String(getValue()) : "-") },
    { accessorKey: "eventType", header: "Type" },
    { accessorKey: "level", header: "Level", cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }) },
    { accessorKey: "message", header: "Message" },
    { accessorKey: "durationMs", header: "Duration", cell: ({ getValue }) => formatDuration(getValue() ?? null) },
    {
        id: "metadata",
        header: "Metadata",
        cell: ({ row }) => _jsx(JsonDetails, { value: row.original.metadata ?? {} }),
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
    const params = useMemo(() => ({
        jobName: jobName.trim() || undefined,
        scriptPath: scriptPath.trim() || undefined,
        level: level || undefined,
        eventType: eventType.trim() || undefined,
        from: from || undefined,
        to: to || undefined,
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
    }), [eventType, from, jobName, level, page, scriptPath, to]);
    const query = useQuery({
        queryKey: ["events", params],
        queryFn: () => getEvents(params),
        refetchInterval: () => visiblePollingInterval(),
    });
    const onFilterSubmit = (event) => {
        event.preventDefault();
        setPage(0);
        void query.refetch();
    };
    return (_jsxs("section", { className: "stack", children: [_jsx(PageHeader, { title: "Events Explorer", subtitle: "Query raw monitor telemetry events with metadata context.", actions: _jsx("button", { type: "button", className: "button button--primary", onClick: () => void query.refetch(), children: "Refresh" }) }), query.error ? _jsx(ErrorBanner, { message: query.error.message }) : null, _jsxs("form", { className: "filters filters--dense", onSubmit: onFilterSubmit, children: [_jsxs("label", { children: ["Job", _jsx("input", { value: jobName, onChange: (event) => setJobName(event.target.value), placeholder: "sample-etl-pipeline" })] }), _jsxs("label", { children: ["Script Path", _jsx("input", { value: scriptPath, onChange: (event) => setScriptPath(event.target.value), placeholder: "workers/sample/load_demo.py" })] }), _jsxs("label", { children: ["Level", _jsxs("select", { value: level, onChange: (event) => setLevel(event.target.value), children: [_jsx("option", { value: "", children: "Any" }), _jsx("option", { value: "DEBUG", children: "DEBUG" }), _jsx("option", { value: "INFO", children: "INFO" }), _jsx("option", { value: "WARN", children: "WARN" }), _jsx("option", { value: "ERROR", children: "ERROR" }), _jsx("option", { value: "CRITICAL", children: "CRITICAL" })] })] }), _jsxs("label", { children: ["Event Type", _jsx("input", { value: eventType, onChange: (event) => setEventType(event.target.value), placeholder: "job.completed" })] }), _jsxs("label", { children: ["From (ISO)", _jsx("input", { value: from, onChange: (event) => setFrom(event.target.value), placeholder: "2026-01-01T00:00:00Z" })] }), _jsxs("label", { children: ["To (ISO)", _jsx("input", { value: to, onChange: (event) => setTo(event.target.value), placeholder: "2026-01-01T23:59:59Z" })] }), _jsx("button", { type: "submit", className: "button", children: "Apply Filters" })] }), _jsxs("article", { className: "card", children: [_jsx(DataTable, { data: query.data?.events ?? [], columns: columns, emptyTitle: "No events", emptyMessage: "No telemetry events match the current filters.", wrapClassName: "table-wrap--events", tableClassName: "table--events" }), _jsx(PaginationControls, { page: page, pageSize: PAGE_SIZE, receivedCount: (query.data?.events ?? []).length, onPageChange: setPage })] })] }));
}
