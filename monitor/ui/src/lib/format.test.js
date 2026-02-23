import { describe, expect, it } from "vitest";
import { formatCount, formatDateTime, formatDuration } from "./format";
describe("format utilities", () => {
    it("formats counts", () => {
        expect(formatCount(1234)).toContain("1");
    });
    it("formats datetime and handles missing values", () => {
        expect(formatDateTime("2026-01-01T00:00:00Z")).toBeTypeOf("string");
        expect(formatDateTime(null)).toBe("-");
    });
    it("formats duration values", () => {
        expect(formatDuration(500)).toBe("500 ms");
        expect(formatDuration(1500)).toContain("s");
        expect(formatDuration(null)).toBe("-");
    });
});
