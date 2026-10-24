import type { ConsumerGroupLag } from "@/api/client";
import { useConsumerGroups } from "@/api/queries";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Sparkline } from "@/components/Sparkline";
import { formatDuration, formatLag } from "@/lib/format";
import { cn } from "@/lib/utils";

const hour = 60 * 60 * 1000;

/** How far behind each consumer group is on each topic it reads. */
export function ConsumerGroupsCard() {
  const groups = useConsumerGroups();
  // Every sparkline spans the same hour, up to the newest point, so that
  // they line up. Without any points, no sparkline is drawn.
  const end = Math.max(
    ...(groups.data ?? []).flatMap((group) =>
      group.history.map((point) => new Date(point.time).getTime()),
    ),
    0,
  );
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Consumer groups</h2>
        </CardTitle>
        <CardDescription>
          How far behind each group is: the age of the oldest message it has not consumed yet.
        </CardDescription>
      </CardHeader>
      <CardContent className="text-sm">
        {groups.isPending && <p className="text-muted-foreground">Loading consumer groups</p>}
        {groups.isError && <p className="text-destructive">{groups.error.message}</p>}
        {groups.data?.length === 0 && (
          <p className="text-muted-foreground">No consumer group has committed offsets yet.</p>
        )}
        {groups.data && groups.data.length > 0 && (
          // Wide tables scroll sideways on phones instead of overflowing.
          <div className="-mx-1 overflow-x-auto px-1">
            <table className="w-full text-left whitespace-nowrap [&_td]:px-3 [&_td:first-child]:pl-0 [&_td:last-child]:pr-0 [&_th]:px-3 [&_th:first-child]:pl-0 [&_th:last-child]:pr-0">
              <thead className="border-b text-muted-foreground">
                <tr>
                  <th className="py-2 font-medium">Group</th>
                  <th className="py-2 font-medium">Topic</th>
                  <th className="py-2 font-medium">Lag</th>
                  <th className="py-2 text-right font-medium">Messages</th>
                  <th className="py-2 font-medium">Last hour</th>
                </tr>
              </thead>
              <tbody>
                {groups.data.map((group) => (
                  <GroupRow
                    key={`${group.group}\u0000${group.topic}`}
                    group={group}
                    start={end - hour}
                    end={end}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function GroupRow({ group, start, end }: { group: ConsumerGroupLag; start: number; end: number }) {
  const caughtUp = group.lagMessages === 0;
  const points = group.history.map((point) => ({
    time: new Date(point.time).getTime(),
    value: point.value,
  }));
  const highest = Math.max(...points.map((point) => point.value), 0);
  return (
    <tr className="border-b last:border-0">
      <td className="py-2 font-medium">{group.group}</td>
      <td className="py-2">{group.topic}</td>
      <td className={cn("py-2", caughtUp ? "text-muted-foreground" : "font-medium")}>
        {formatLag(group.lagSeconds, group.lagMessages, group.capped)}
      </td>
      <td className="py-2 text-right tabular-nums">{group.lagMessages.toLocaleString()}</td>
      <td className="py-1.5">
        {points.length < 2 ? (
          <span className="text-muted-foreground">–</span>
        ) : (
          <Sparkline
            points={points}
            start={start}
            end={end}
            label={
              highest === 0
                ? "Caught up throughout the last hour"
                : `Lag over the last hour, at most ${formatDuration(highest)}`
            }
          />
        )}
      </td>
    </tr>
  );
}
