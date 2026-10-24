import type { Customer } from "@/api/client";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { parts } from "@/lib/parts";

/** What the cluster waits for, in words, per condition type. */
const conditionNames: Record<Customer["conditions"][number]["type"], string> = {
  CertificateIssued: "Listener certificate",
  KafkaReady: "Kafka cluster",
  UsersReady: "Users and credentials",
  AgentAvailable: "Changeport agent",
  Ready: "Ready for clients",
};

/** Whether a customer's cluster is ready, and if not, what it waits for. */
export function StatusBadge({ customer }: { customer: Customer }) {
  if (customer.deleting) {
    return <Badge variant="destructive">Being deleted</Badge>;
  }
  if (customer.ready) {
    return <Badge>Ready</Badge>;
  }
  return <Badge variant="secondary">Starting</Badge>;
}

/** A customer's cluster: how to reach it and how far it is. */
export function CustomerSummary({ customer }: { customer: Customer }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <CardTitle className="text-xl">{customer.displayName}</CardTitle>
          <StatusBadge customer={customer} />
        </div>
        <CardDescription>
          Customer {customer.id}, {customer.profile} cluster
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-6">
        <section aria-label="Connection">
          <h2 className="text-sm font-medium">Bootstrap address</h2>
          {customer.bootstrapAddress ? (
            <code className="text-sm">{customer.bootstrapAddress}</code>
          ) : (
            <p className="text-sm text-muted-foreground">Known once the cluster is created.</p>
          )}
        </section>
        {!customer.ready && (
          <p className="text-sm text-muted-foreground" role="status">
            {customer.status.message}
          </p>
        )}
        {/* Every part shows from the start, also before the controller
            has said anything about it. */}
        <ul aria-label="Progress" className="flex flex-col gap-2">
          {parts.map((part) => {
            const condition = customer.conditions.find((c) => c.type === part);
            const ready = condition?.ready ?? false;
            return (
              <li key={part} className="flex items-baseline gap-3 text-sm">
                <span aria-hidden className={ready ? "text-primary" : "text-muted-foreground"}>
                  {ready ? "●" : "○"}
                </span>
                <span className="w-44 shrink-0">{conditionNames[part]}</span>
                <span className="truncate text-muted-foreground">
                  {ready ? "Done" : (condition?.message ?? "Not started")}
                </span>
              </li>
            );
          })}
        </ul>
      </CardContent>
    </Card>
  );
}
