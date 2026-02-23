import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { EmptyState } from "./EmptyState";
import { ErrorBanner } from "./ErrorBanner";
import { PaginationControls } from "./PaginationControls";
import { StatusBadge } from "./StatusBadge";

describe("component smoke tests", () => {
  it("renders status badge", () => {
    render(<StatusBadge value="UP" />);
    expect(screen.getByText("UP")).toBeInTheDocument();
  });

  it("renders empty and error states", () => {
    render(
      <>
        <EmptyState title="No Data" message="Nothing to show" />
        <ErrorBanner message="Backend unavailable" />
      </>
    );

    expect(screen.getByText("No Data")).toBeInTheDocument();
    expect(screen.getByText("Backend unavailable")).toBeInTheDocument();
  });

  it("renders pagination controls", () => {
    const onPageChange = vi.fn();
    render(
      <PaginationControls page={0} pageSize={25} receivedCount={25} onPageChange={onPageChange} />
    );

    expect(screen.getByText("Page 1")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Next" })).toBeEnabled();
  });
});
