import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, Pie, PieChart, Cell, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { getAlerts, getSummary } from "../lib/api";
import { formatCount, formatDateTime, formatRelative } from "../lib/format";
import { visiblePollingInterval } from "../lib/polling";
import { DataTable } from "../components/DataTable";
import { ErrorBanner } from "../components/ErrorBanner";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";
const alertColumns = [
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
        cell: ({ getValue }) => _jsx(StatusBadge, { value: String(getValue() ?? "") }),
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
        const map = new Map();
        for (const item of summaryQuery.data?.checks ?? []) {
            map.set(item.status.toUpperCase(), item.count);
        }
        return map;
    }, [summaryQuery.data?.checks]);
    const activeAlertsMap = useMemo(() => {
        const map = new Map();
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
    return (_jsxs("section", { className: "stack", children: [_jsx(PageHeader, { title: "System Overview", subtitle: "Live status across checks, alerts, and telemetry volume.", actions: _jsx("button", { type: "button", onClick: refresh, className: "button button--primary", disabled: isLoading, children: "Refresh" }) }), summaryQuery.error ? _jsx(ErrorBanner, { message: summaryQuery.error.message }) : null, alertsQuery.error ? _jsx(ErrorBanner, { message: alertsQuery.error.message }) : null, _jsxs("div", { className: "metrics-grid", children: [_jsx(MetricCard, { label: "Total Events", value: formatCount(summaryQuery.data?.totalEvents ?? 0), detail: `Latest: ${formatDateTime(summaryQuery.data?.latestEventAt)}` }), _jsx(MetricCard, { label: "Checks Up", value: formatCount(checksMap.get("UP") ?? 0), detail: `Late: ${formatCount(checksMap.get("LATE") ?? 0)} | Down: ${formatCount(checksMap.get("DOWN") ?? 0)}` }), _jsx(MetricCard, { label: "Open Alerts", value: formatCount((alertsQuery.data?.alerts ?? []).length), detail: `Failure: ${formatCount(activeAlertsMap.get("FAILURE") ?? 0)} | Missed: ${formatCount(activeAlertsMap.get("MISSED") ?? 0)}` }), _jsx(MetricCard, { label: "Last Event Age", value: formatRelative(summaryQuery.data?.latestEventAt), detail: "Calculated from monitor event stream" }), _jsx(MetricCard, { label: "Chief Runtime", value: _jsxs("span", { className: "presence-indicator", children: [_jsx("span", { className: `presence-dot ${chiefOnline ? "is-online" : "is-offline"}`, "aria-label": chiefOnline ? "Chief online" : "Chief offline" }), _jsx("span", { children: chiefOnline ? "ONLINE" : "OFFLINE" })] }), detail: chiefLastHeartbeat
                            ? `Last ping ${formatRelative(chiefLastHeartbeat)} (${formatDateTime(chiefLastHeartbeat)}), interval ${chiefIntervalSeconds !== null ? `${chiefIntervalSeconds}s` : "unknown"}`
                            : `No heartbeat seen yet. Offline timeout ${chiefOfflineAfterSeconds}s` })] }), _jsxs("div", { className: "charts-grid", children: [_jsxs("article", { className: "card card--chart", children: [_jsx("h3", { children: "Checks by Status" }), _jsx(ResponsiveContainer, { width: "100%", height: 240, children: _jsxs(BarChart, { data: summaryQuery.data?.checks ?? [], children: [_jsx(CartesianGrid, { strokeDasharray: "4 4", stroke: "#d7d4cd" }), _jsx(XAxis, { dataKey: "status" }), _jsx(YAxis, { allowDecimals: false }), _jsx(Tooltip, {}), _jsx(Bar, { dataKey: "count", fill: "#178f9f", radius: [6, 6, 0, 0] })] }) })] }), _jsxs("article", { className: "card card--chart", children: [_jsx("h3", { children: "Open Alerts by Type" }), _jsx(ResponsiveContainer, { width: "100%", height: 240, children: _jsxs(PieChart, { children: [_jsx(Pie, { data: summaryQuery.data?.activeAlerts ?? [], dataKey: "count", nameKey: "alertType", cx: "50%", cy: "50%", outerRadius: 84, innerRadius: 42, paddingAngle: 4, children: (summaryQuery.data?.activeAlerts ?? []).map((entry, index) => (_jsx(Cell, { fill: chartColors[index % chartColors.length] }, `${entry.alertType}-${index}`))) }), _jsx(Tooltip, {})] }) })] })] }), _jsxs("article", { className: "card", children: [_jsx("h3", { children: "Recent Open Alerts" }), _jsx(DataTable, { data: alertsQuery.data?.alerts ?? [], columns: alertColumns, emptyTitle: "No open alerts", emptyMessage: "System currently has no active alerts." })] })] }));
}
