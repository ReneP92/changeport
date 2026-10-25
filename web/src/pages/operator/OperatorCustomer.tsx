import { Link, useParams } from "react-router";
import { ApiError } from "@/api/client";
import { useOperatorCustomer } from "@/api/queries";
import { CustomerSummary } from "@/components/CustomerSummary";
import { Notice } from "@/components/Notice";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DeprovisionDialog } from "@/pages/operator/DeprovisionDialog";
import { OperatorsOnly } from "@/pages/operator/OperatorCustomers";

/** One customer, for operators: its cluster's progress, and deprovisioning. */
export function OperatorCustomer() {
  return (
    <OperatorsOnly>
      <CustomerPage />
    </OperatorsOnly>
  );
}

function CustomerPage() {
  const { customerID = "" } = useParams();
  const customer = useOperatorCustomer(customerID);
  const back = (
    <Link
      to="/operator"
      className="text-sm text-muted-foreground underline-offset-4 hover:underline"
    >
      ← All customers
    </Link>
  );
  if (customer.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (customer.isError) {
    const gone = customer.error instanceof ApiError && customer.error.status === 404;
    return (
      <main className="mx-auto flex max-w-5xl flex-col gap-4 p-6">
        {back}
        <h1 className="text-2xl font-semibold tracking-tight">
          {gone ? `There is no customer ${customerID}` : "The console is unavailable"}
        </h1>
        <p className="text-muted-foreground">
          {gone
            ? "It does not exist, or it has been deprovisioned. Its Clerk organization, if any, is kept."
            : customer.error.message}
        </p>
      </main>
    );
  }
  const c = customer.data;
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-6">
      {back}
      <CustomerSummary customer={c} />
      <Card>
        <CardHeader>
          <CardTitle>
            <h2>Clerk organization</h2>
          </CardTitle>
          <CardDescription>Its members sign in to the console as this customer.</CardDescription>
        </CardHeader>
        <CardContent className="text-sm">
          {c.organizationID ? (
            <code>{c.organizationID}</code>
          ) : (
            <span className="text-muted-foreground">None</span>
          )}
        </CardContent>
      </Card>
      {!c.deleting && (
        <Card>
          <CardHeader>
            <CardTitle>
              <h2>Deprovision</h2>
            </CardTitle>
            <CardDescription>
              {c.deletionProtection
                ? "Deletion protection is on, so deprovisioning is refused until an operator turns it off with provisioner unprotect."
                : "Deletes the cluster and its data for good. The Clerk organization is kept, no longer linked to a cluster."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <DeprovisionDialog customer={c} />
          </CardContent>
        </Card>
      )}
    </main>
  );
}
