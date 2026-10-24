import { useState } from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import type { ThroughputRange } from "@/api/client";
import { useThroughput } from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { formatRate } from "@/lib/format";

const ranges: ThroughputRange[] = ["1h", "6h", "24h", "7d"];

const chartConfig = {
  bytesIn: { label: "In", color: "var(--chart-1)" },
  bytesOut: { label: "Out", color: "var(--chart-2)" },
} satisfies ChartConfig;

/** Bytes in and out per second over the whole cluster. */
export function ThroughputCard() {
  const [range, setRange] = useState<ThroughputRange>("1h");
  const throughput = useThroughput(range);
  const points =
    throughput.data?.bytesIn.map((point, i) => ({
      time: new Date(point.time).getTime(),
      bytesIn: point.value,
      bytesOut: throughput.data.bytesOut[i]?.value ?? 0,
    })) ?? [];
  const long = range === "24h" || range === "7d";
  const formatTime = (time: number) =>
    new Date(time).toLocaleString(
      undefined,
      long ? { weekday: "short", hour: "2-digit" } : { timeStyle: "short" },
    );
  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-4">
        <div className="flex flex-col gap-1.5">
          <CardTitle>
            <h2>Throughput</h2>
          </CardTitle>
          <CardDescription>Bytes per second, over all topics</CardDescription>
        </div>
        <div role="group" aria-label="Range" className="flex gap-1">
          {ranges.map((r) => (
            <Button
              key={r}
              size="sm"
              variant={r === range ? "default" : "ghost"}
              aria-pressed={r === range}
              onClick={() => {
                setRange(r);
              }}
            >
              {r}
            </Button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {throughput.isError && (
          <p className="text-sm text-destructive">{throughput.error.message}</p>
        )}
        {throughput.data && points.length === 0 && (
          <p className="text-sm text-muted-foreground">No traffic in this range yet.</p>
        )}
        {points.length > 0 && (
          <ChartContainer config={chartConfig} className="aspect-auto h-56 w-full">
            <AreaChart data={points} margin={{ left: 8, right: 8 }}>
              <CartesianGrid vertical={false} />
              <XAxis
                dataKey="time"
                type="number"
                scale="time"
                domain={["dataMin", "dataMax"]}
                tickFormatter={formatTime}
                tickLine={false}
                axisLine={false}
                minTickGap={40}
              />
              <YAxis tickFormatter={formatRate} tickLine={false} axisLine={false} width={80} />
              <ChartTooltip
                content={
                  <ChartTooltipContent
                    labelFormatter={(_, payload) => {
                      const time = (payload[0]?.payload as { time?: number } | undefined)?.time;
                      return time === undefined ? "" : new Date(time).toLocaleString();
                    }}
                    formatter={(value, name) => (
                      <span>
                        {chartConfig[name as keyof typeof chartConfig].label}{" "}
                        <span className="font-mono tabular-nums">{formatRate(Number(value))}</span>
                      </span>
                    )}
                  />
                }
              />
              <ChartLegend content={<ChartLegendContent />} />
              <Area
                dataKey="bytesIn"
                type="monotone"
                stroke="var(--color-bytesIn)"
                strokeWidth={2}
                fill="var(--color-bytesIn)"
                fillOpacity={0.2}
              />
              <Area
                dataKey="bytesOut"
                type="monotone"
                stroke="var(--color-bytesOut)"
                strokeWidth={2}
                fill="var(--color-bytesOut)"
                fillOpacity={0.2}
              />
            </AreaChart>
          </ChartContainer>
        )}
      </CardContent>
    </Card>
  );
}
