const dtf = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

export function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return dtf.format(parsed);
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0";
  }
  return new Intl.NumberFormat().format(value);
}

export function formatDuration(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  if (value < 1000) {
    return `${value} ms`;
  }
  const seconds = value / 1000;
  return `${seconds.toFixed(seconds >= 10 ? 1 : 2)} s`;
}

export function formatRelative(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const target = new Date(value);
  if (Number.isNaN(target.getTime())) {
    return value;
  }
  const diffMs = Date.now() - target.getTime();
  const abs = Math.abs(diffMs);
  const suffix = diffMs >= 0 ? "ago" : "from now";

  const units: Array<[number, string]> = [
    [1000, "second"],
    [60 * 1000, "minute"],
    [60 * 60 * 1000, "hour"],
    [24 * 60 * 60 * 1000, "day"],
  ];

  let unitLabel = "second";
  let unitValue = 0;
  for (const [ms, label] of units) {
    if (abs >= ms) {
      unitLabel = label;
      unitValue = Math.floor(abs / ms);
    }
  }

  const plural = unitValue === 1 ? "" : "s";
  return `${unitValue} ${unitLabel}${plural} ${suffix}`;
}
