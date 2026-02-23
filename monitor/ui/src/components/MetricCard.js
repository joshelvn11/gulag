import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function MetricCard({ label, value, detail }) {
    return (_jsxs("article", { className: "metric-card", children: [_jsx("p", { className: "metric-card__label", children: label }), _jsx("p", { className: "metric-card__value", children: value }), detail ? _jsx("p", { className: "metric-card__detail", children: detail }) : null] }));
}
