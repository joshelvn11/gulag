import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function PageHeader({ title, subtitle, actions }) {
    return (_jsxs("section", { className: "page-header", children: [_jsxs("div", { children: [_jsx("h2", { children: title }), subtitle ? _jsx("p", { children: subtitle }) : null] }), actions ? _jsx("div", { className: "page-header__actions", children: actions }) : null] }));
}
