import { useTopics } from "@/api/queries";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { formatBytes, formatRate, formatRetention } from "@/lib/format";

/** The topics with their settings, size and throughput. */
export function TopicsCard() {
  const topics = useTopics();
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Topics</h2>
        </CardTitle>
        <CardDescription>
          Tiered topics keep their history in object storage and only recent data on the brokers.
        </CardDescription>
      </CardHeader>
      <CardContent className="text-sm">
        {topics.isPending && <p className="text-muted-foreground">Loading topics</p>}
        {topics.isError && <p className="text-destructive">{topics.error.message}</p>}
        {topics.data?.length === 0 && (
          <p className="text-muted-foreground">There are no topics yet.</p>
        )}
        {topics.data && topics.data.length > 0 && (
          // Wide tables scroll sideways on phones instead of overflowing.
          <div className="-mx-1 overflow-x-auto px-1">
            <table className="w-full text-left whitespace-nowrap [&_td]:px-3 [&_td:first-child]:pl-0 [&_td:last-child]:pr-0 [&_th]:px-3 [&_th:first-child]:pl-0 [&_th:last-child]:pr-0">
              <thead className="border-b text-muted-foreground">
                <tr>
                  <th className="py-2 font-medium">Topic</th>
                  <th className="py-2 text-right font-medium">Partitions</th>
                  <th className="py-2 font-medium">Retention</th>
                  <th className="py-2 font-medium">Storage</th>
                  <th className="py-2 text-right font-medium">Size</th>
                  <th className="py-2 text-right font-medium">In</th>
                  <th className="py-2 text-right font-medium">Out</th>
                </tr>
              </thead>
              <tbody>
                {topics.data.map((topic) => (
                  <tr key={topic.name} className="border-b last:border-0">
                    <td className="py-2 font-medium">{topic.name}</td>
                    <td className="py-2 text-right tabular-nums">{topic.partitions}</td>
                    <td className="py-2">
                      {topic.cleanupPolicy.includes("compact")
                        ? "Compacted"
                        : formatRetention(topic.retentionMs)}
                    </td>
                    <td className="py-2">
                      {topic.tiered ? (
                        <Badge variant="secondary">Tiered</Badge>
                      ) : (
                        <span className="text-muted-foreground">Brokers only</span>
                      )}
                    </td>
                    <td
                      className="py-2 text-right tabular-nums"
                      title={sizeTitle(topic.localBytes, topic.remoteBytes)}
                    >
                      {topic.localBytes === undefined && topic.remoteBytes === undefined
                        ? "–"
                        : formatBytes((topic.localBytes ?? 0) + (topic.remoteBytes ?? 0))}
                    </td>
                    <td className="py-2 text-right tabular-nums">
                      {topic.bytesInPerSecond === undefined
                        ? "–"
                        : formatRate(topic.bytesInPerSecond)}
                    </td>
                    <td className="py-2 text-right tabular-nums">
                      {topic.bytesOutPerSecond === undefined
                        ? "–"
                        : formatRate(topic.bytesOutPerSecond)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function sizeTitle(local: number | undefined, remote: number | undefined): string {
  return `${formatBytes(local ?? 0)} on the brokers, ${formatBytes(remote ?? 0)} in tiered storage`;
}
