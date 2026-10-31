const byteUnits = ["B", "KB", "MB", "GB", "TB", "PB"];

/** Formats a number of bytes for people, in powers of 1,000: 1.5 MB. */
export function formatBytes(bytes: number): string {
  let value = bytes;
  let unit = 0;
  while (Math.abs(value) >= 1000 && unit < byteUnits.length - 1) {
    value /= 1000;
    unit++;
  }
  const digits = unit === 0 || value >= 100 ? 0 : 1;
  return `${value.toFixed(digits)} ${byteUnits[unit] ?? "B"}`;
}

/** Formats bytes per second: 1.5 MB/s. */
export function formatRate(bytesPerSecond: number): string {
  return `${formatBytes(bytesPerSecond)}/s`;
}

/** Formats a retention in milliseconds as the largest whole unit: 7 days. */
export function formatRetention(ms: number | undefined): string {
  if (ms === undefined) {
    return "Forever";
  }
  const units: [number, string][] = [
    [24 * 60 * 60 * 1000, "day"],
    [60 * 60 * 1000, "hour"],
    [60 * 1000, "minute"],
    [1000, "second"],
  ];
  for (const [size, name] of units) {
    if (ms >= size && ms % size === 0) {
      const count = ms / size;
      return `${String(count)} ${name}${count === 1 ? "" : "s"}`;
    }
  }
  return `${String(ms)} ms`;
}

const plural = (count: number, unit: string) => `${String(count)} ${unit}${count === 1 ? "" : "s"}`;

/**
 * Formats a duration in seconds for people, to the minute once it is longer
 * than one: 45 seconds, 3 minutes, 1 hour 20 minutes, 12 hours.
 */
export function formatDuration(seconds: number): string {
  if (seconds < 60) {
    return plural(Math.floor(seconds), "second");
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return plural(minutes, "minute");
  }
  const hours = Math.floor(minutes / 60);
  // Past a few hours, the minutes no longer matter.
  if (hours >= 10 || minutes % 60 === 0) {
    return plural(hours, "hour");
  }
  return `${plural(hours, "hour")} ${plural(minutes % 60, "minute")}`;
}

/**
 * Says how far a consumer group is behind, in plain language: caught up,
 * less than a minute behind, 3 minutes behind. capped means the lag is older
 * than the agent looks up (23 hours), so seconds is only a lower bound.
 */
export function formatLag(seconds: number, messages: number, capped: boolean): string {
  if (messages === 0) {
    return "Caught up";
  }
  if (capped) {
    return `More than ${formatDuration(seconds)} behind`;
  }
  if (seconds < 60) {
    return "Less than a minute behind";
  }
  return `${formatDuration(seconds)} behind`;
}

/** Says how long ago a time was, coarsely: just now, 3 minutes ago. */
export function formatAgo(at: string, now: number = Date.now()): string {
  const seconds = Math.max(0, (now - Date.parse(at)) / 1000);
  if (seconds < 60) {
    return "just now";
  }
  return `${formatDuration(seconds)} ago`;
}
