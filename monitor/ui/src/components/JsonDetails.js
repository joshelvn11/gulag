import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function JsonDetails({ title = "View JSON", value }) {
    let pretty = "{}";
    try {
        pretty = JSON.stringify(value ?? {}, null, 2);
    }
    catch {
        pretty = "{}";
    }
    return (_jsxs("details", { className: "json-details", children: [_jsx("summary", { children: title }), _jsx("pre", { children: pretty })] }));
}
