import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function PaginationControls({ page, pageSize, receivedCount, onPageChange, }) {
    const canPrev = page > 0;
    const canNext = receivedCount >= pageSize;
    return (_jsxs("div", { className: "pager", children: [_jsx("button", { type: "button", onClick: () => onPageChange(page - 1), disabled: !canPrev, children: "Previous" }), _jsxs("span", { children: ["Page ", page + 1] }), _jsx("button", { type: "button", onClick: () => onPageChange(page + 1), disabled: !canNext, children: "Next" })] }));
}
