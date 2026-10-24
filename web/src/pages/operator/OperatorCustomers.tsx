import type { ReactNode } from "react";
import { Link } from "react-router";
import type { Customer } from "@/api/client";
import { useCustomers, useMe } from "@/api/queries";
import { StatusBadge } from "@/components/CustomerSummary";
import { Notice } from "@/components/Notice";
import { ProvisionCustomerDialog } from "@/pages/operator/ProvisionCustomerDialog";

/** Shows children to operators, and everyone else what to do instead. */
export function OperatorsOnly({ children }: { children: ReactNode }) {
  const me = useMe();
  if (me.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (me.isError || me.data.access !== "operator") {
    return (
      <Notice title="Operators only">
        Switch to the Changeport staff organization to see every customer.
      </Notice>
    );
  }
  return children;
}

/** Every customer, for operators. */
export function OperatorCustomers() {
  return (
    <OperatorsOnly>
      <CustomerTable />
    </OperatorsOnly>
  );
}

function CustomerTable() {
  const customers = useCustomers();
  if (customers.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (customers.isError) {
    return <Notice title="The console is unavailable">{customers.error.message}</Notice>;
  }
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-4 p-6">
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-2xl font-semibold tracking-tight">Customers</h1>
        <ProvisionCustomerDialog />
      </div>
      {customers.data.length === 0 ? (
        <p className="text-muted-foreground">There are no customers yet.</p>
      ) : (
        // Wide tables scroll sideways on phones instead of overflowing.
        <div className="-mx-1 overflow-x-auto px-1">
          <table className="w-full text-left text-sm [&_td]:px-3 [&_td:first-child]:pl-0 [&_td:last-child]:pr-0 [&_th]:px-3 [&_th:first-child]:pl-0 [&_th:last-child]:pr-0">
            <thead className="border-b text-muted-foreground">
              <tr>
                <th className="py-2 font-medium">Customer</th>
                <th className="py-2 font-medium">Profile</th>
                <th className="py-2 font-medium">Progress</th>
                <th className="py-2 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {customers.data.map((customer) => (
                <tr key={customer.id} className="border-b last:border-0">
                  <td className="py-3 whitespace-nowrap">
                    <Link
                      to={`/operator/customers/${customer.id}`}
                      className="font-medium underline-offset-4 hover:underline"
                    >
                      {customer.displayName}
                    </Link>
                    <div className="text-muted-foreground">
                      {customer.id}
                      {customer.deletionProtection && " · protected"}
                    </div>
                  </td>
                  <td className="py-3 whitespace-nowrap">{customer.profile}</td>
                  <td className="py-3">
                    <ConditionDots customer={customer} />
                  </td>
                  <td className="py-3">
                    {/* Long messages wrap below the badge, never to a sliver. */}
                    <div className="flex min-w-60 flex-col items-start gap-1">
                      <StatusBadge customer={customer} />
                      {!customer.ready && (
                        <span className="text-muted-foreground">{customer.status.message}</span>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

/** The parts of a cluster in the order it gets ready, before Ready itself. */
const parts: Customer["conditions"][number]["type"][] = [
  "CertificateIssued",
  "KafkaReady",
  "UsersReady",
  "AgentAvailable",
];

/** A dot per part of the customer's cluster, filled once it is ready. */
export function ConditionDots({ customer }: { customer: Customer }) {
  const ready = parts.filter(
    (part) => customer.conditions.find((condition) => condition.type === part)?.ready,
  );
  return (
    <span
      role="img"
      aria-label={`${String(ready.length)} of ${String(parts.length)} parts ready`}
      className="flex gap-1"
    >
      {parts.map((part) => (
        <span
          key={part}
          className={
            ready.includes(part)
              ? "size-2 rounded-full bg-primary"
              : "size-2 rounded-full border border-muted-foreground/40"
          }
        />
      ))}
    </span>
  );
}
