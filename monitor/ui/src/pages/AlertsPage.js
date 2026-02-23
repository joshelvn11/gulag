import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { closeAlert, getAlerts } from "../lib/api";
import { formatDateTime } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
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
    const params = useMemo(() => ({
        jobName: jobName.trim() || undefined,
        status: status || undefined,
        alertType: alertType || undefined,
        severity: severity || undefined,
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
    }), [alertType, jobName, page, severity, status]);
    const query = useQuery({
        queryKey: ["alerts", params],
        queryFn: () => getAlerts(params),
        refetchInterval: () => visiblePollingInterval(),
    });
    const closeMutation = useMutation({
        mutationFn: (input) => closeAlert(input.alertId),
        onSuccess: async () => {
            await Promise.all([
                queryClient.invalidateQueries({ queryKey: ["alerts"] }),
                queryClient.invalidateQueries({ queryKey: ["summary"] }),
                queryClient.invalidateQueries({ queryKey: ["jobs"] }),
            ]);
        },
    });
    const columns = useMemo(() => [
        { accessorKey: "jobName", header: "Job" },
        { accessorKey: "alertType", header: "Type" },
        {
            accessorKey: "severity",
            header: "Severity",
            cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }),
        },
        {
            accessorKey: "status",
            header: "Status",
            cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }),
        },
        { accessorKey: "openedAt", header: "Opened", cell: ({ getValue }) => formatDateTime(String(getValue() ?? "")) },
        { accessorKey: "closedAt", header: "Closed", cell: ({ getValue }) => formatDateTime(getValue() ?? null) },
        { accessorKey: "title", header: "Title" },
        {
            id: "details",
            header: "Details",
            cell: ({ row }) => _jsx(JsonDetails, { value: row.original.details ?? {} }),
        },
        {
            id: "actions",
            header: "Actions",
            cell: ({ row }) => {
                const isOpen = row.original.status === "OPEN";
                const isPending = closeMutation.isPending && closeMutation.variables?.alertId === row.original.id;
                if (!isOpen) {
                    return _jsx("span", { className: "muted", children: "-" });
                }
                return (_jsx("button", { type: "button", className: "button button--danger", disabled: isPending, onClick: () => closeMutation.mutate({ alertId: row.original.id }), children: isPending ? "Closing..." : "Close" }));
            },
        },
    ], [closeMutation.isPending, closeMutation.mutate, closeMutation.variables?.alertId]);
    const onFilterSubmit = (event) => {
        event.preventDefault();
        setPage(0);
        void query.refetch();
    };
    return (_jsxs("section", { className: "stack", children: [_jsx(PageHeader, { title: "Alerts Explorer", subtitle: "Filter and inspect monitor alerts across jobs and severities.", actions: _jsx("button", { type: "button", className: "button button--primary", onClick: () => void query.refetch(), children: "Refresh" }) }), query.error ? _jsx(ErrorBanner, { message: query.error.message }) : null, closeMutation.error ? _jsx(ErrorBanner, { message: closeMutation.error.message }) : null, _jsxs("form", { className: "filters", onSubmit: onFilterSubmit, children: [_jsxs("label", { children: ["Job", _jsx("input", { value: jobName, onChange: (event) => setJobName(event.target.value), placeholder: "sample-etl-pipeline" })] }), _jsxs("label", { children: ["Status", _jsxs("select", { value: status, onChange: (event) => setStatus(event.target.value), children: [_jsx("option", { value: "", children: "Any" }), _jsx("option", { value: "OPEN", children: "OPEN" }), _jsx("option", { value: "CLOSED", children: "CLOSED" })] })] }), _jsxs("label", { children: ["Alert Type", _jsxs("select", { value: alertType, onChange: (event) => setAlertType(event.target.value), children: [_jsx("option", { value: "", children: "Any" }), _jsx("option", { value: "FAILURE", children: "FAILURE" }), _jsx("option", { value: "MISSED", children: "MISSED" }), _jsx("option", { value: "RECOVERY", children: "RECOVERY" })] })] }), _jsxs("label", { children: ["Severity", _jsxs("select", { value: severity, onChange: (event) => setSeverity(event.target.value), children: [_jsx("option", { value: "", children: "Any" }), _jsx("option", { value: "INFO", children: "INFO" }), _jsx("option", { value: "WARN", children: "WARN" }), _jsx("option", { value: "ERROR", children: "ERROR" }), _jsx("option", { value: "CRITICAL", children: "CRITICAL" })] })] }), _jsx("button", { type: "submit", className: "button", children: "Apply Filters" })] }), _jsxs("article", { className: "card", children: [_jsx(DataTable, { data: query.data?.alerts ?? [], columns: columns, emptyTitle: "No alerts", emptyMessage: "No alerts match the current filters." }), _jsx(PaginationControls, { page: page, pageSize: PAGE_SIZE, receivedCount: (query.data?.alerts ?? []).length, onPageChange: setPage })] })] }));
}
