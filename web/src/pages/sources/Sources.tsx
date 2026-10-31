import { errorText } from "@/api/client";
import { Link } from "react-router";
import { useSources } from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { formatBytes } from "@/lib/format";
import { CustomerArea } from "@/pages/CustomerArea";
import { PhaseBadge } from "@/pages/sources/PhaseBadge";

/** The customer's Postgres sources, and for admins adding one. */
export function Sources() {
  return <CustomerArea>{(_, admin) => <SourceList admin={admin} />}</CustomerArea>;
}

function SourceList({ admin }: { admin: boolean }) {
  const sources = useSources();
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-6">
      <header className="flex items-start justify-between gap-4">
        <div className="flex flex-col gap-1">
          <h1 className="text-2xl font-semibold tracking-tight">Postgres sources</h1>
          <p className="text-sm text-muted-foreground">
            Each table of a source streams into a topic of its own: its existing rows first, then
            every insert, update and delete.
          </p>
        </div>
        {admin && (
          <Button asChild className="shrink-0">
            <Link to="/sources/new">Add source</Link>
          </Button>
        )}
      </header>
      <Card>
        <CardHeader>
          <CardTitle>
            <h2>Sources</h2>
          </CardTitle>
          <CardDescription>
            The WAL a database keeps for a source grows while the source is paused or behind.
          </CardDescription>
        </CardHeader>
        <CardContent className="text-sm">
          {sources.isPending && <p className="text-muted-foreground">Loading sources</p>}
          {sources.isError && <p className="text-destructive">{errorText(sources.error)}</p>}
          {sources.data?.length === 0 && (
            <p className="text-muted-foreground">
              There are no sources yet.{" "}
              {admin
                ? "Add one to stream a Postgres database's tables into topics."
                : "Ask an admin of your organization to add one."}
            </p>
          )}
          {sources.data && sources.data.length > 0 && (
            // Wide tables scroll sideways on phones instead of overflowing.
            <div className="relative -mx-1 overflow-x-auto px-1">
              <table className="w-full text-left whitespace-nowrap [&_td]:px-3 [&_td:first-child]:pl-0 [&_td:last-child]:pr-0 [&_th]:px-3 [&_th:first-child]:pl-0 [&_th:last-child]:pr-0">
                <thead className="border-b text-muted-foreground">
                  <tr>
                    <th className="py-2 font-medium">Source</th>
                    <th className="py-2 font-medium">Status</th>
                    <th className="py-2 font-medium">Database</th>
                    <th className="py-2 text-right font-medium">Tables</th>
                    <th className="py-2 text-right font-medium">Retained WAL</th>
                  </tr>
                </thead>
                <tbody>
                  {sources.data.map((source) => (
                    <tr key={source.name} className="border-b last:border-0">
                      <td className="py-2 font-medium">
                        <Link
                          to={`/sources/${source.name}`}
                          className="underline-offset-4 hover:underline"
                        >
                          {source.name}
                        </Link>
                      </td>
                      <td className="py-2">
                        <PhaseBadge source={source} />
                      </td>
                      <td className="py-2 text-muted-foreground">
                        {source.database} on {source.host}
                      </td>
                      <td className="py-2 text-right tabular-nums">{source.tables.length}</td>
                      <td className="py-2 text-right tabular-nums">
                        {source.slot ? formatBytes(source.slot.retainedWALBytes) : "–"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
