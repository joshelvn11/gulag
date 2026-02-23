type StatusBadgeProps = {
  value: string | null | undefined;
};

function normalize(value: string | null | undefined): string {
  return (value ?? "UNKNOWN").toString().toUpperCase();
}

export function StatusBadge({ value }: StatusBadgeProps) {
  const status = normalize(value);
  const statusClass =
    status === "UP" || status === "OPEN"
      ? "is-up"
      : status === "LATE" || status === "WARN"
      ? "is-late"
      : status === "DOWN" || status === "ERROR" || status === "CRITICAL" || status === "CLOSED"
      ? "is-down"
      : "is-neutral";

  return <span className={`status-badge ${statusClass}`}>{status}</span>;
}
