import { useState } from "react";
import { errorText, type Source, type SourceSQL, type TableRef } from "@/api/client";
import { useChangeSource, useSourceCatalog, useSourceSQL } from "@/api/queries";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { SqlBlock } from "@/pages/sources/SqlBlock";
import { TablePicker } from "@/pages/sources/TablePicker";

/** Wide dialogs, for tables and SQL, that scroll within a short screen. */
export const wideDialog = "max-h-[calc(100svh-2rem)] overflow-y-auto sm:max-w-xl";

/**
 * A button that opens the dialog to add tables to a source: the source
 * controller lists the database's tables, the admin picks some and runs the
 * SQL that grants and publishes them, and the source copies them.
 */
export function AddTablesDialog({ source, max }: { source: Source; max: number }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" disabled={source.tables.length >= max}>
          Add tables
        </Button>
      </DialogTrigger>
      <DialogContent className={wideDialog}>
        {open && (
          <AddTables
            source={source}
            max={max}
            onAdded={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function AddTables({ source, max, onAdded }: { source: Source; max: number; onAdded: () => void }) {
  const catalog = useSourceCatalog(source.name, true);
  const sql = useSourceSQL(source.name);
  const change = useChangeSource(source.name);
  const [chosen, setChosen] = useState<TableRef[]>([]);
  const [script, setScript] = useState<SourceSQL | null>(null);
  const synced = source.tables.map((t) => ({ schema: t.schema, name: t.name }));

  if (script) {
    return (
      <form
        className="flex flex-col gap-5"
        onSubmit={(event) => {
          event.preventDefault();
          change.mutate({ tables: [...synced, ...chosen] }, { onSuccess: onAdded });
        }}
      >
        <DialogHeader>
          <DialogTitle>Grant and publish</DialogTitle>
          <DialogDescription>
            Run this as the owner of the tables. Changeport then checks each table, and the source
            copies its rows.
          </DialogDescription>
        </DialogHeader>
        <SqlBlock sql={script} label="SQL" />
        {change.error && (
          <p role="alert" className="text-sm text-destructive">
            {errorText(change.error)}
          </p>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              setScript(null);
            }}
          >
            Back
          </Button>
          <Button type="submit" disabled={change.isPending}>
            {change.isPending
              ? "Adding"
              : `I ran it, add ${String(chosen.length)} table${chosen.length === 1 ? "" : "s"}`}
          </Button>
        </DialogFooter>
      </form>
    );
  }
  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(event) => {
        event.preventDefault();
        sql.mutate({ step: "add", tables: chosen }, { onSuccess: setScript });
      }}
    >
      <DialogHeader>
        <DialogTitle>Add tables to {source.name}</DialogTitle>
        <DialogDescription>
          Each table streams into a topic of its own, starting with a copy of its existing rows.
        </DialogDescription>
      </DialogHeader>
      {!catalog.data && !catalog.error && (
        <p role="status" className="text-sm text-muted-foreground">
          Listing the database&apos;s tables
        </p>
      )}
      {catalog.error && (
        <p role="alert" className="text-sm text-destructive">
          {errorText(catalog.error)}
        </p>
      )}
      {catalog.data?.message && (
        <div className="flex flex-col items-start gap-2 text-sm">
          <p role="alert" className="text-destructive">
            {catalog.data.message}
          </p>
          <Button type="button" variant="outline" size="sm" onClick={() => void catalog.retry()}>
            Try again
          </Button>
        </div>
      )}
      {catalog.data && !catalog.data.message && (
        <>
          <TablePicker
            tables={catalog.data.tables}
            selected={chosen}
            onChange={setChosen}
            max={max}
            synced={synced}
          />
          {catalog.data.truncated && (
            <p className="text-sm text-muted-foreground">
              The database has more tables than the list shows.
            </p>
          )}
        </>
      )}
      {sql.error && (
        <p role="alert" className="text-sm text-destructive">
          {errorText(sql.error)}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button type="submit" disabled={chosen.length === 0 || sql.isPending}>
          {sql.isPending ? "Preparing" : "Continue"}
        </Button>
      </DialogFooter>
    </form>
  );
}
