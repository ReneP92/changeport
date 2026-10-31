import type { ReactNode } from "react";
import { Link, useParams } from "react-router";
import { errorText, ApiError, type Source, type SourceOptions } from "@/api/client";
import { useChangeSource, useConfirmSlotDropped, useSource, useSourceOptions } from "@/api/queries";
import { Notice } from "@/components/Notice";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { formatAgo, formatBytes, formatRetention } from "@/lib/format";
import { cn } from "@/lib/utils";
import { CustomerArea } from "@/pages/CustomerArea";
import { AddTablesDialog } from "@/pages/sources/AddTablesDialog";
import { DeleteSourceDialog } from "@/pages/sources/DeleteSourceDialog";
import { EgressAddresses } from "@/pages/sources/EgressAddresses";
import { PasswordDialog } from "@/pages/sources/PasswordDialog";
import { PhaseBadge } from "@/pages/sources/PhaseBadge";
import { RemoveTablesDialog } from "@/pages/sources/RemoveTablesDialog";
import { CodeBlock } from "@/pages/sources/SqlBlock";

/** One Postgres source: its tables' progress, its slot, and for admins its changes. */
export function SourcePage() {
  return <CustomerArea>{(_, admin) => <SourceDetails admin={admin} />}</CustomerArea>;
}

const back = (
  <Link to="/sources" className="text-sm text-muted-foreground underline-offset-4 hover:underline">
    ← Sources
  </Link>
);

function SourceDetails({ admin }: { admin: boolean }) {
  const { name = "" } = useParams();
  const source = useSource(name);
  const options = useSourceOptions();
  if (source.isPending || options.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (source.isError || options.isError) {
    const error = source.error ?? options.error;
    const gone = error instanceof ApiError && error.status === 404;
    return (
      <main className="mx-auto flex max-w-5xl flex-col gap-4 p-6">
        {back}
        <h1 className="text-2xl font-semibold tracking-tight">
          {gone ? `There is no source ${name}` : "The console is unavailable"}
        </h1>
        <p className="text-muted-foreground">
          {gone
            ? "It does not exist, or it has been deleted. Its topics, if any, are kept."
            : error?.message}
        </p>
      </main>
    );
  }
  const s = source.data;
  const deleting = s.deleting ?? false;
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-6">
      {back}
      <header className="flex flex-col gap-1">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight break-all">{s.name}</h1>
          <PhaseBadge source={s} />
        </div>
        <p className="text-sm break-all text-muted-foreground">
          {s.database} on {s.host}:{s.port}
        </p>
      </header>
      <StatusNote source={s} />
      {deleting && <DeletingCard source={s} admin={admin} />}
      <TablesCard source={s} options={options.data} admin={admin && !deleting} />
      <SlotCard source={s} />
      <DetailsCard source={s} options={options.data} admin={admin && !deleting} />
      {admin && !deleting && <ManageCard source={s} options={options.data} />}
    </main>
  );
}

/** What the source is doing, or waits for, in words. */
function StatusNote({ source }: { source: Source }) {
  const failed = source.phase === "Failed";
  return (
    <p
      role="status"
      className={cn(
        "rounded-lg border p-3 text-sm",
        failed ? "border-destructive/40 bg-destructive/5 text-destructive" : "bg-muted/40",
      )}
    >
      {source.state === "Stopped" && !source.deleting
        ? "Paused. The database keeps the WAL the source has not read until it resumes, so do not leave it paused for long."
        : source.message}
    </p>
  );
}

function DeletingCard({ source, admin }: { source: Source; admin: boolean }) {
  const confirm = useConfirmSlotDropped(source.name);
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Being deleted</h2>
        </CardTitle>
        <CardDescription>
          {source.dropSlotSQL
            ? "Changeport cannot reach the database to drop the source's replication slot, which keeps WAL until it is dropped. Drop it yourself with this SQL, then confirm."
            : "Changeport drops the source's replication slot, then the source goes. Its topics stay, as ordinary topics."}
        </CardDescription>
      </CardHeader>
      {source.dropSlotSQL && (
        <CardContent className="flex flex-col gap-4">
          <CodeBlock code={source.dropSlotSQL} label="SQL" />
          {admin && (
            <div className="flex flex-col gap-2">
              <Button
                className="self-start"
                disabled={confirm.isPending}
                onClick={() => {
                  confirm.mutate();
                }}
              >
                {confirm.isPending ? "Confirming" : "I dropped the slot"}
              </Button>
              {confirm.error && (
                <p role="alert" className="text-sm text-destructive">
                  {errorText(confirm.error)}
                </p>
              )}
            </div>
          )}
        </CardContent>
      )}
    </Card>
  );
}

function TablesCard({
  source,
  options,
  admin,
}: {
  source: Source;
  options: SourceOptions;
  admin: boolean;
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-start justify-between gap-4">
        <div className="flex flex-col gap-1.5">
          <CardTitle>
            <h2>Tables</h2>
          </CardTitle>
          <CardDescription>
            Each table streams into its topic, starting with a copy of its existing rows.
          </CardDescription>
        </div>
        {admin && (
          <div className="flex gap-2">
            <AddTablesDialog source={source} max={options.maxTables} />
            <RemoveTablesDialog source={source} />
          </div>
        )}
      </CardHeader>
      <CardContent className="text-sm">
        <ul aria-label="Tables" className="flex flex-col divide-y">
          {source.tables.map((table) => (
            <li
              key={`${table.schema}.${table.name}`}
              className="grid gap-x-6 gap-y-2 py-3 first:pt-0 last:pb-0 sm:grid-cols-[minmax(0,1fr)_minmax(0,18rem)]"
            >
              <div className="flex min-w-0 flex-col gap-0.5">
                <span className="font-medium break-all">
                  {table.schema}.{table.name}
                </span>
                <span className="break-all text-muted-foreground">
                  Topic <code className="text-foreground">{table.topic}</code>
                </span>
              </div>
              <SnapshotState table={table} />
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

/** How far copying a table's existing rows is. */
export function SnapshotState({ table }: { table: Source["tables"][number] }) {
  const copied = table.rowsCopied ?? 0;
  const estimated = table.rowsEstimated ?? 0;
  let state: ReactNode;
  switch (table.snapshot) {
    case "Done":
      state = <span>Copied {copied.toLocaleString("en-US")}</span>;
      break;
    case "Running": {
      // Until the first chunk, the copy has nothing to show.
      if (estimated === 0) {
        state = <span className="text-muted-foreground">Starting the copy</span>;
        break;
      }
      const share = estimated > 0 ? Math.min(copied / estimated, 1) : 0;
      state = (
        <div className="flex flex-col gap-1.5">
          <div
            role="progressbar"
            aria-label={`Copying ${table.schema}.${table.name}`}
            aria-valuemin={0}
            aria-valuemax={estimated}
            aria-valuenow={Math.min(copied, estimated)}
            className="h-1.5 w-full max-w-48 overflow-hidden rounded-full bg-muted"
          >
            <div
              className="h-full bg-primary transition-all"
              style={{ width: `${String(share * 100)}%` }}
            />
          </div>
          <span className="text-muted-foreground tabular-nums">
            {copied.toLocaleString("en-US")} of about {estimated.toLocaleString("en-US")}
          </span>
        </div>
      );
      break;
    }
    case "Failed":
      state = <Badge variant="destructive">Failed</Badge>;
      break;
    default:
      state = <span className="text-muted-foreground">Waiting</span>;
  }
  return (
    <div className="flex flex-col gap-1">
      {state}
      {table.message && (
        <span
          className={cn(
            "leading-snug",
            table.snapshot === "Failed" ? "text-destructive" : "text-muted-foreground",
          )}
        >
          {table.message}
        </span>
      )}
    </div>
  );
}

/** What each of Postgres's wal_status values means for the slot. */
const walStatuses: Record<string, string> = {
  reserved: "Within max_wal_size",
  extended: "Beyond max_wal_size, kept for the slot",
  unreserved: "At risk: the database may remove WAL the slot needs",
  lost: "Lost: the database removed WAL the slot needed",
};

function SlotCard({ source }: { source: Source }) {
  const slot = source.slot;
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Replication slot</h2>
        </CardTitle>
        <CardDescription>
          The database keeps every change for the slot until the source has read it.
        </CardDescription>
      </CardHeader>
      <CardContent className="text-sm">
        {slot ? (
          <dl className={detailsGrid}>
            <dt className="text-muted-foreground">Slot</dt>
            <dd>
              <code className="break-all">{slot.name}</code>
            </dd>
            <dt className="text-muted-foreground">Read by the source</dt>
            <dd>{slot.active ? "Yes" : "No"}</dd>
            <dt className="text-muted-foreground">Retained WAL</dt>
            <dd className="tabular-nums">{formatBytes(slot.retainedWALBytes)}</dd>
            {slot.walStatus && (
              <>
                <dt className="text-muted-foreground">WAL status</dt>
                <dd
                  className={cn(
                    (slot.walStatus === "unreserved" || slot.walStatus === "lost") &&
                      "text-destructive",
                  )}
                >
                  {walStatuses[slot.walStatus] ?? slot.walStatus}
                </dd>
              </>
            )}
            <dt className="text-muted-foreground">Room left</dt>
            <dd>
              {slot.safeWALSizeBytes === undefined
                ? "No limit: set max_slot_wal_keep_size, so that a stalled source cannot fill the database's disk"
                : `${formatBytes(slot.safeWALSizeBytes)} before the database gives up on the slot`}
            </dd>
            <dt className="text-muted-foreground">Checked</dt>
            <dd>{formatAgo(slot.checkedAt)}</dd>
          </dl>
        ) : (
          <p className="text-muted-foreground">
            The source creates its slot when it first connects; Changeport reads it every minute.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

const detailsGrid =
  "grid grid-cols-1 gap-y-1 sm:grid-cols-[max-content_1fr] sm:gap-x-6 sm:gap-y-2 [&>dd]:min-w-0 [&>dd]:break-words max-sm:[&>dd]:mb-2";

function DetailsCard({
  source,
  options,
  admin,
}: {
  source: Source;
  options: SourceOptions;
  admin: boolean;
}) {
  const change = useChangeSource(source.name);
  const provider = options.providers.find((p) => p.id === source.provider);
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Details</h2>
        </CardTitle>
        <CardDescription>
          Messages are Avro; your schema registry has their schemas, and takes your Kafka
          credentials.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4 text-sm">
        <dl className={detailsGrid}>
          <dt className="text-muted-foreground">Database</dt>
          <dd>
            {source.database} on {source.host}:{source.port}, {provider?.title ?? source.provider}
          </dd>
          <dt className="text-muted-foreground">Role</dt>
          <dd>
            <code className="break-all">{source.role}</code>
          </dd>
          <dt className="text-muted-foreground">Certificate check</dt>
          <dd>
            <code>{source.tlsMode}</code>
          </dd>
          <dt className="text-muted-foreground">Schema registry</dt>
          <dd>
            {options.registryURL ? (
              <code className="break-all">{options.registryURL}</code>
            ) : (
              <span className="text-muted-foreground">Known once the cluster is ready</span>
            )}
          </dd>
          <dt id="source-retention-label" className="text-muted-foreground">
            Topics keep messages for
          </dt>
          <dd>
            {admin ? (
              <div className="flex flex-col gap-2">
                <Select
                  value={String(source.retentionMs)}
                  disabled={change.isPending}
                  onValueChange={(value) => {
                    change.mutate({ retentionMs: Number(value) });
                  }}
                >
                  <SelectTrigger
                    aria-labelledby="source-retention-label"
                    size="sm"
                    className="w-40"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {options.retentionPresets.map((preset) => (
                      <SelectItem key={preset.ms} value={String(preset.ms)}>
                        {preset.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {change.error && (
                  <span role="alert" className="text-destructive">
                    {errorText(change.error)}
                  </span>
                )}
              </div>
            ) : (
              formatRetention(source.retentionMs)
            )}
          </dd>
        </dl>
        <EgressAddresses addresses={options.egressAddresses} />
      </CardContent>
    </Card>
  );
}

function ManageCard({ source, options }: { source: Source; options: SourceOptions }) {
  const change = useChangeSource(source.name);
  const paused = source.state === "Stopped";
  const provider = options.providers.find((p) => p.id === source.provider);
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Manage</h2>
        </CardTitle>
        <CardDescription>
          A paused source keeps its slot and its place, and catches up when it resumes.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3 text-sm">
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            disabled={change.isPending}
            onClick={() => {
              change.mutate({ state: paused ? "Running" : "Stopped" });
            }}
          >
            {change.isPending ? (paused ? "Resuming" : "Pausing") : paused ? "Resume" : "Pause"}
          </Button>
          <PasswordDialog
            source={source}
            passwordFromCustomer={provider?.passwordFromCustomer ?? false}
          />
          <DeleteSourceDialog source={source} />
        </div>
        {change.error && (
          <p role="alert" className="text-destructive">
            {errorText(change.error)}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
