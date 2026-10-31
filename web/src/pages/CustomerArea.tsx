import type { ReactNode } from "react";
import { Navigate } from "react-router";
import { ApiError, type Customer } from "@/api/client";
import { useCustomer, useMe } from "@/api/queries";
import { CustomerSummary } from "@/components/CustomerSummary";
import { Notice } from "@/components/Notice";

/**
 * Shows a page of the active organization's ready cluster, with whether the
 * user is its admin; until the cluster is ready, how far it is. Everyone
 * else goes home, which says what they can do.
 */
export function CustomerArea({
  children,
}: {
  children: (customer: Customer, admin: boolean) => ReactNode;
}) {
  const me = useMe();
  if (me.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (me.isError) {
    return <Notice title="The console is unavailable">{me.error.message}</Notice>;
  }
  if (me.data.access !== "customer") {
    return <Navigate to="/" replace />;
  }
  return <ReadyCustomer admin={me.data.admin ?? false}>{children}</ReadyCustomer>;
}

function ReadyCustomer({
  admin,
  children,
}: {
  admin: boolean;
  children: (customer: Customer, admin: boolean) => ReactNode;
}) {
  const customer = useCustomer();
  if (customer.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (customer.isError) {
    const missing = customer.error instanceof ApiError && customer.error.status === 404;
    return (
      <Notice title={missing ? "No Kafka cluster yet" : "The console is unavailable"}>
        {missing ? "Your organization has no Changeport cluster yet." : customer.error.message}
      </Notice>
    );
  }
  if (!customer.data.ready) {
    return (
      <main className="mx-auto max-w-5xl p-6">
        <CustomerSummary customer={customer.data} />
      </main>
    );
  }
  return children(customer.data, admin);
}
