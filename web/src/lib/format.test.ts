import { describe, expect, test } from "bun:test";
import { formatBytes, formatRate, formatRetention } from "@/lib/format";

describe("formatBytes", () => {
  test("uses powers of 1,000", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(999)).toBe("999 B");
    expect(formatBytes(1500)).toBe("1.5 KB");
    expect(formatBytes(123_456_789)).toBe("123 MB");
    expect(formatBytes(2.5e12)).toBe("2.5 TB");
  });
  test("formats rates", () => {
    expect(formatRate(1_500_000)).toBe("1.5 MB/s");
  });
});

describe("formatRetention", () => {
  test("uses the largest whole unit", () => {
    expect(formatRetention(7 * 24 * 60 * 60 * 1000)).toBe("7 days");
    expect(formatRetention(60 * 60 * 1000)).toBe("1 hour");
    expect(formatRetention(90 * 60 * 1000)).toBe("90 minutes");
    expect(formatRetention(1500)).toBe("1500 ms");
  });
  test("says forever without a limit", () => {
    expect(formatRetention(undefined)).toBe("Forever");
  });
});
