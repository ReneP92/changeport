/** A point of a sparkline: a time in milliseconds and a value. */
export interface SparkPoint {
  time: number;
  value: number;
}

const width = 120;
const height = 28;
// Room for the line's width at the top and bottom.
const inset = 2;

/**
 * A small line chart without axes, from start to end in time and from 0 to
 * the highest value, over a baseline, with label saying what it shows for
 * screen readers.
 */
export function Sparkline({
  points,
  start,
  end,
  label,
}: {
  points: SparkPoint[];
  start: number;
  end: number;
  label: string;
}) {
  const top = Math.max(...points.map((point) => point.value), 0);
  const x = (time: number) => ((time - start) / Math.max(end - start, 1)) * width;
  const y = (value: number) =>
    height - inset - (top === 0 ? 0 : (value / top) * (height - 2 * inset));
  const line = points
    .map(
      (point, i) =>
        `${i === 0 ? "M" : "L"}${x(point.time).toFixed(1)},${y(point.value).toFixed(1)}`,
    )
    .join("");
  const first = points[0];
  const last = points.at(-1);
  return (
    <svg
      role="img"
      aria-label={label}
      viewBox={`0 0 ${String(width)} ${String(height)}`}
      width={width}
      height={height}
      className="block overflow-visible"
    >
      {/* The baseline spans the whole time, also where there is no data. */}
      <line
        x1={0}
        x2={width}
        y1={height - 0.5}
        y2={height - 0.5}
        stroke="var(--border)"
        strokeWidth={1}
      />
      {first && last && (
        <>
          <path
            d={`${line}L${x(last.time).toFixed(1)},${String(height)}L${x(first.time).toFixed(1)},${String(height)}Z`}
            fill="var(--chart-1)"
            fillOpacity={0.15}
          />
          <path
            d={line}
            fill="none"
            stroke="var(--chart-1)"
            strokeWidth={1.5}
            strokeLinejoin="round"
            strokeLinecap="round"
          />
        </>
      )}
    </svg>
  );
}
