import { useCustomers, useMe } from "@/api/queries";
import { StatusBadge } from "@/components/CustomerSummary";
import { Notice } from "@/components/Notice";

/** Every customer, for operators. */
export function OperatorCustomers() {
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
  return <CustomerTable />;
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
      <h1 className="text-2xl font-semibold tracking-tight">Customers</h1>
      {customers.data.length === 0 ? (
        <p className="text-muted-foreground">There are no customers yet.</p>
      ) : (
        <table className="w-full text-left text-sm">
          <thead className="border-b text-muted-foreground">
            <tr>
              <th className="py-2 font-medium">Customer</th>
              <th className="py-2 font-medium">Profile</th>
              <th className="py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {customers.data.map((customer) => (
              <tr key={customer.id} className="border-b last:border-0">
                <td className="py-3">
                  <div className="font-medium">{customer.displayName}</div>
                  {customer.displayName !== customer.id && (
                    <div className="text-muted-foreground">{customer.id}</div>
                  )}
                </td>
                <td className="py-3">{customer.profile}</td>
                <td className="py-3">
                  <div className="flex items-center gap-2">
                    <StatusBadge customer={customer} />
                    {!customer.ready && (
                      <span className="truncate text-muted-foreground">
                        {customer.status.message}
                      </span>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
