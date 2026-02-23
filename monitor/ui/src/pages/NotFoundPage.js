import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Link } from "react-router-dom";
export function NotFoundPage() {
    return (_jsx("section", { className: "stack", children: _jsxs("div", { className: "card", children: [_jsx("h2", { children: "Page not found" }), _jsx("p", { className: "muted", children: "The route you requested does not exist in the monitor dashboard." }), _jsx(Link, { className: "button button--primary", to: "/", children: "Go to Overview" })] }) }));
}
