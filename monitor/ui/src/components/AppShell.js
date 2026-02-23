import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { NavLink } from "react-router-dom";
const navItems = [
    { to: "/", label: "Overview" },
    { to: "/jobs", label: "Jobs" },
    { to: "/alerts", label: "Alerts" },
    { to: "/events", label: "Events" },
];
export function AppShell({ children }) {
    return (_jsxs("div", { className: "shell", children: [_jsxs("header", { className: "shell__header", children: [_jsxs("div", { className: "brand", children: [_jsx("span", { className: "brand__dot", "aria-hidden": "true" }), _jsxs("div", { children: [_jsx("h1", { children: "Chief Monitor" }), _jsx("p", { children: "Operational telemetry dashboard" })] })] }), _jsx("nav", { className: "nav", "aria-label": "Primary", children: navItems.map((item) => (_jsx(NavLink, { to: item.to, className: ({ isActive }) => `nav__link${isActive ? " is-active" : ""}`, end: item.to === "/", children: item.label }, item.to))) })] }), _jsx("main", { className: "shell__content", children: children })] }));
}
