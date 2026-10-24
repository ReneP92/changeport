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
