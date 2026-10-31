import { OrganizationSwitcher } from "@clerk/react";
import { Navigate } from "react-router";
import { useMe } from "@/api/queries";
import { Notice } from "@/components/Notice";
import { CustomerArea } from "@/pages/CustomerArea";
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
      return <CustomerHome />;
    default:
      if (me.data.organizationID) {
        // An organization without a cluster, such as one deprovisioned.
        return (
          <main className="mx-auto flex max-w-xl flex-col gap-4 p-8">
            <h1 className="text-2xl font-semibold tracking-tight">No Kafka cluster yet</h1>
            <p className="text-muted-foreground">
              Your organization has no Changeport cluster. If you belong to another organization,
              switch to it.
            </p>
            <OrganizationSwitcher hidePersonal afterSelectOrganizationUrl="/" />
          </main>
        );
      }
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

function CustomerHome() {
  return (
    <CustomerArea>
      {(customer, admin) => <Overview customer={customer} admin={admin} />}
    </CustomerArea>
  );
}
