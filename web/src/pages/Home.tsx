import { OrganizationSwitcher } from "@clerk/react";
import { Navigate } from "react-router";
import { ApiError } from "@/api/client";
import { useCustomer, useMe } from "@/api/queries";
import { CustomerSummary } from "@/components/CustomerSummary";
import { Notice } from "@/components/Notice";
import { Overview } from "@/pages/overview/Overview";

/** Where a signed-in user lands: their customer, or the operator console. */
export function Home() {
  const me = useMe();
  if (me.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (me.isError) {
    return <Notice title="The console is unavailable">{me.error.message}</Notice>;
  }
  switch (me.data.access) {
    case "operator":
      return <Navigate to="/operator" replace />;
    case "customer":
      return <CustomerHome admin={me.data.admin ?? false} />;
    default:
      return (
        <main className="mx-auto flex max-w-xl flex-col gap-4 p-8">
          <h1 className="text-2xl font-semibold tracking-tight">Choose your organization</h1>
          <p className="text-muted-foreground">
            Your Kafka cluster belongs to your company&apos;s organization. Switch to it, or ask a
            colleague for an invitation.
          </p>
          <OrganizationSwitcher hidePersonal afterSelectOrganizationUrl="/" />
        </main>
      );
  }
}

function CustomerHome({ admin }: { admin: boolean }) {
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
  if (customer.data.ready) {
    return <Overview customer={customer.data} admin={admin} />;
  }
  return (
    <main className="mx-auto max-w-5xl p-6">
      <CustomerSummary customer={customer.data} />
    </main>
  );
}
