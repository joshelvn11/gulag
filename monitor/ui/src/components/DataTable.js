import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { flexRender, getCoreRowModel, useReactTable, } from "@tanstack/react-table";
import { EmptyState } from "./EmptyState";
function joinClasses(...classes) {
    return classes.filter(Boolean).join(" ");
}
export function DataTable({ data, columns, emptyTitle = "No rows", emptyMessage = "No data matched this query.", wrapClassName, tableClassName, }) {
    const table = useReactTable({
        data,
        columns,
        getCoreRowModel: getCoreRowModel(),
    });
    if (data.length === 0) {
        return _jsx(EmptyState, { title: emptyTitle, message: emptyMessage });
    }
    return (_jsx("div", { className: joinClasses("table-wrap", wrapClassName), children: _jsxs("table", { className: joinClasses("table", tableClassName), children: [_jsx("thead", { children: table.getHeaderGroups().map((headerGroup) => (_jsx("tr", { children: headerGroup.headers.map((header) => (_jsx("th", { children: header.isPlaceholder
                                ? null
                                : flexRender(header.column.columnDef.header, header.getContext()) }, header.id))) }, headerGroup.id))) }), _jsx("tbody", { children: table.getRowModel().rows.map((row) => (_jsx("tr", { children: row.getVisibleCells().map((cell) => (_jsx("td", { children: flexRender(cell.column.columnDef.cell, cell.getContext()) }, cell.id))) }, row.id))) })] }) }));
}
