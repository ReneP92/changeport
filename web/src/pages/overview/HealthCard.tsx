import type { Health } from "@/api/client";
import { useHealth } from "@/api/queries";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

/** A count that is healthy at want, with what it means. */
function Stat({
  label,
  value,
  healthy,
}: {
  label: string;
  value: number | undefined;
  healthy: (value: number) => boolean;
}) {
  const known = value !== undefined;
  return (
    <div className="flex flex-col gap-1 rounded-md border p-3">
      <span className="text-muted-foreground">{label}</span>
      <span
        className={cn(
          "text-2xl font-semibold tabular-nums",
          known && !healthy(value) && "text-destructive",
        )}
      >
        {known ? value : "–"}
      </span>
    </div>
  );
}

/** The brokers, and whether every partition is in sync and led. */
export function HealthCard() {
  const health = useHealth();
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Health</h2>
        </CardTitle>
        <CardDescription>{summary(health.data)}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4 text-sm">
        {health.isError && <p className="text-destructive">{health.error.message}</p>}
        {health.data && (
          <>
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <Stat
                label="Under-replicated partitions"
                value={health.data.underReplicatedPartitions}
                healthy={(v) => v === 0}
              />
              <Stat
                label="Under min. in-sync replicas"
                value={health.data.underMinIsrPartitions}
                healthy={(v) => v === 0}
              />
              <Stat
                label="Offline partitions"
                value={health.data.offlinePartitions}
                healthy={(v) => v === 0}
              />
              <Stat
                label="Active controllers"
                value={health.data.activeControllers}
                healthy={(v) => v === 1}
              />
            </div>
            <ul aria-label="Brokers" className="flex flex-col gap-2">
              {health.data.brokers.map((broker) => (
                <li key={broker.name} className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  <Badge variant={broker.ready ? "default" : "destructive"}>
                    {broker.ready ? "Ready" : "Not ready"}
                  </Badge>
                  <span className="min-w-0 font-medium break-all">{broker.name}</span>
                  {broker.zone && (
                    <span className="whitespace-nowrap text-muted-foreground">{broker.zone}</span>
                  )}
                  {broker.restarts > 0 && (
                    <span className="whitespace-nowrap text-muted-foreground">
                      {broker.restarts} restart{broker.restarts === 1 ? "" : "s"}
                    </span>
                  )}
                </li>
              ))}
            </ul>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function summary(health: Health | undefined): string {
  if (!health) {
    return "Checking the brokers";
  }
  const ready = health.brokers.filter((broker) => broker.ready).length;
  const problems =
    (health.underReplicatedPartitions ?? 0) +
    (health.underMinIsrPartitions ?? 0) +
    (health.offlinePartitions ?? 0);
  const total = health.brokers.length;
  const brokers =
    total === 1
      ? ready === 1
        ? "The broker is ready"
        : "The broker is not ready"
      : `${String(ready)} of ${String(total)} brokers ready`;
  return problems === 0
    ? `${brokers}; every partition is in sync`
    : `${brokers}; some partitions are not in sync`;
}
