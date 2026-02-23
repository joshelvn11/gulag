import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function ErrorBanner({ title = "Request failed", message }) {
    return (_jsxs("div", { className: "error-banner", role: "alert", children: [_jsx("strong", { children: title }), _jsx("p", { children: message })] }));
}
