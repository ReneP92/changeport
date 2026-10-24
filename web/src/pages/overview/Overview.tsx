import type { Customer } from "@/api/client";
import { StatusBadge } from "@/components/CustomerSummary";
import { ConnectionCard } from "@/pages/overview/ConnectionCard";
import { ConsumerGroupsCard } from "@/pages/overview/ConsumerGroupsCard";
import { HealthCard } from "@/pages/overview/HealthCard";
import { ThroughputCard } from "@/pages/overview/ThroughputCard";
import { TopicsCard } from "@/pages/overview/TopicsCard";

/**
 * A ready cluster: how to connect, its health, throughput, topics and
 * consumer groups.
 */
export function Overview({ customer, admin }: { customer: Customer; admin: boolean }) {
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-6">
      <header className="flex flex-col gap-1">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">{customer.displayName}</h1>
          <StatusBadge customer={customer} />
        </div>
        <p className="text-sm text-muted-foreground">
          Customer {customer.id}, {customer.profile} cluster
        </p>
      </header>
      <ConnectionCard admin={admin} />
      <HealthCard />
      <ThroughputCard />
      <TopicsCard admin={admin} />
      <ConsumerGroupsCard />
    </main>
  );
}
