import { describe, expect, it } from "vitest";

import { buildQueryString } from "./api";

describe("buildQueryString", () => {
  it("includes only defined non-empty params", () => {
    const query = buildQueryString({
      jobName: "sample-etl-pipeline",
      status: "OPEN",
      empty: "",
      unknown: undefined,
      nullable: null,
      page: 2,
    });

    expect(query).toBe("jobName=sample-etl-pipeline&status=OPEN&page=2");
  });

  it("returns empty string when no usable params", () => {
    const query = buildQueryString({ a: "", b: undefined, c: null });
    expect(query).toBe("");
  });
});
