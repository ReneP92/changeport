import { describe, expect, test } from "bun:test";
import { formatBytes, formatDuration, formatLag, formatRate, formatRetention } from "@/lib/format";

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

describe("formatDuration", () => {
  test("rounds down to the largest units that matter", () => {
    expect(formatDuration(0)).toBe("0 seconds");
    expect(formatDuration(1.9)).toBe("1 second");
    expect(formatDuration(59)).toBe("59 seconds");
    expect(formatDuration(60)).toBe("1 minute");
    expect(formatDuration(3 * 60 + 59)).toBe("3 minutes");
    expect(formatDuration(3600)).toBe("1 hour");
    expect(formatDuration(80 * 60)).toBe("1 hour 20 minutes");
    expect(formatDuration(9 * 3600 + 61)).toBe("9 hours 1 minute");
    expect(formatDuration(12 * 3600 + 30 * 60)).toBe("12 hours");
  });
});

describe("formatLag", () => {
  test("says how far behind in plain language", () => {
    expect(formatLag(0, 0, false)).toBe("Caught up");
    expect(formatLag(12, 3, false)).toBe("Less than a minute behind");
    expect(formatLag(185, 400, false)).toBe("3 minutes behind");
    expect(formatLag(2 * 3600, 400, false)).toBe("2 hours behind");
  });
  test("is caught up without unconsumed messages, whatever their age", () => {
    expect(formatLag(30, 0, false)).toBe("Caught up");
  });
  test("gives a lower bound when the lag is capped", () => {
    expect(formatLag(23 * 3600, 9000, true)).toBe("More than 23 hours behind");
  });
});
