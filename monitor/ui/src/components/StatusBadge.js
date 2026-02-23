import { jsx as _jsx } from "react/jsx-runtime";
function normalize(value) {
    return (value ?? "UNKNOWN").toString().toUpperCase();
}
export function StatusBadge({ value }) {
    const status = normalize(value);
    const statusClass = status === "UP" || status === "OPEN"
        ? "is-up"
        : status === "LATE" || status === "WARN"
            ? "is-late"
            : status === "DOWN" || status === "ERROR" || status === "CRITICAL" || status === "CLOSED"
                ? "is-down"
                : "is-neutral";
    return _jsx("span", { className: `status-badge ${statusClass}`, children: status });
}
