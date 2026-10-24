import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, render, screen } from "@testing-library/react";
import { Sparkline } from "@/components/Sparkline";

afterEach(cleanup);

describe("Sparkline", () => {
  test("draws the points from start to end and from 0 to the highest", () => {
    render(
      <Sparkline
        points={[
          { time: 1_000, value: 0 },
          { time: 2_000, value: 50 },
          { time: 3_000, value: 100 },
        ]}
        start={1_000}
        end={3_000}
        label="Lag over the last hour, at most 1 minute"
      />,
    );
    const chart = screen.getByRole("img", { name: "Lag over the last hour, at most 1 minute" });
    const [area, line] = Array.from(chart.querySelectorAll("path"));
    expect(line?.getAttribute("d")).toBe("M0.0,26.0L60.0,14.0L120.0,2.0");
    expect(area?.getAttribute("d")).toBe("M0.0,26.0L60.0,14.0L120.0,2.0L120.0,28L0.0,28Z");
  });

  test("keeps a flat line of zeros on the baseline", () => {
    render(
      <Sparkline
        points={[
          { time: 0, value: 0 },
          { time: 60_000, value: 0 },
        ]}
        start={0}
        end={120_000}
        label="Caught up throughout the last hour"
      />,
    );
    const chart = screen.getByRole("img", { name: "Caught up throughout the last hour" });
    expect(chart.querySelectorAll("path")[1]?.getAttribute("d")).toBe("M0.0,26.0L60.0,26.0");
    expect(chart.querySelector("line")).not.toBeNull();
  });
});
