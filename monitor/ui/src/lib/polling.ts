export const POLL_INTERVAL_MS = 10_000;

export function visiblePollingInterval(): number | false {
  if (typeof document === "undefined") {
    return POLL_INTERVAL_MS;
  }
  return document.visibilityState === "visible" ? POLL_INTERVAL_MS : false;
}
