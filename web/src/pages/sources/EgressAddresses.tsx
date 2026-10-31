/** The addresses Changeport connects to databases from, which they allow in. */
export function EgressAddresses({ addresses }: { addresses: string[] }) {
  return (
    <div className="flex flex-col gap-2 rounded-lg border bg-muted/40 p-3 text-sm">
      <span className="font-medium">Changeport connects from</span>
      {addresses.length > 0 ? (
        <>
          <ul aria-label="Changeport's addresses" className="flex flex-wrap gap-2">
            {addresses.map((address) => (
              <li key={address}>
                <code className="rounded-md border bg-background px-1.5 py-0.5">{address}</code>
              </li>
            ))}
          </ul>
          <span className="text-muted-foreground">
            Allow these addresses to reach the database&apos;s port, in its firewall or security
            group.
          </span>
        </>
      ) : (
        <span className="text-muted-foreground">
          This environment&apos;s own network; it has no fixed public addresses.
        </span>
      )}
    </div>
  );
}
