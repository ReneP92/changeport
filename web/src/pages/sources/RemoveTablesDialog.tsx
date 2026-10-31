import { useState } from "react";
import { errorText, type Source, type SourceSQL, type TableRef } from "@/api/client";
import { useChangeSource, useSourceSQL } from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
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
import { Label } from "@/components/ui/label";
import { wideDialog } from "@/pages/sources/AddTablesDialog";
import { SqlBlock } from "@/pages/sources/SqlBlock";
import { tableKey, tableName } from "@/lib/tables";

/**
 * A button that opens the dialog to remove tables from a source. Their topics
 * stay, as ordinary topics; the SQL stops publishing them and takes the
 * role's access back.
 */
export function RemoveTablesDialog({ source }: { source: Source }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" variant="outline" disabled={source.tables.length < 2}>
          Remove tables
        </Button>
      </DialogTrigger>
      <DialogContent className={wideDialog}>
        {open && (
          <RemoveTables
            source={source}
            onRemoved={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function RemoveTables({ source, onRemoved }: { source: Source; onRemoved: () => void }) {
  const sql = useSourceSQL(source.name);
  const change = useChangeSource(source.name);
  const [chosen, setChosen] = useState<TableRef[]>([]);
  const [script, setScript] = useState<SourceSQL | null>(null);
  const removed = new Set(chosen.map(tableKey));
  const remaining = source.tables
    .map((t) => ({ schema: t.schema, name: t.name }))
    .filter((t) => !removed.has(tableKey(t)));

  if (script) {
    return (
      <form
        className="flex flex-col gap-5"
        onSubmit={(event) => {
          event.preventDefault();
          change.mutate({ tables: remaining }, { onSuccess: onRemoved });
        }}
      >
        <DialogHeader>
          <DialogTitle>Remove {chosen.map(tableName).join(", ")}?</DialogTitle>
          <DialogDescription>
            The source stops streaming them; their topics stay, as ordinary topics. Then run this
            SQL as the owner of the tables, to stop publishing them and take the role&apos;s access
            back.
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
          <Button type="submit" variant="destructive" disabled={change.isPending}>
            {change.isPending
              ? "Removing"
              : `Remove ${String(chosen.length)} table${chosen.length === 1 ? "" : "s"}`}
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
        sql.mutate({ step: "remove", tables: chosen }, { onSuccess: setScript });
      }}
    >
      <DialogHeader>
        <DialogTitle>Remove tables from {source.name}</DialogTitle>
        <DialogDescription>A source keeps at least one table.</DialogDescription>
      </DialogHeader>
      <ul aria-label="Tables" className="flex flex-col divide-y rounded-lg border text-sm">
        {source.tables.map((table, i) => {
          const key = tableKey(table);
          const checked = removed.has(key);
          const id = `remove-table-${String(i)}`;
          return (
            <li key={key} className="flex items-center gap-3 px-3 py-2.5">
              <Checkbox
                id={id}
                checked={checked}
                disabled={!checked && remaining.length <= 1}
                onCheckedChange={(value) => {
                  setChosen(
                    value === true
                      ? [...chosen, { schema: table.schema, name: table.name }]
                      : chosen.filter((t) => tableKey(t) !== key),
                  );
                }}
              />
              <Label htmlFor={id} className="min-w-0 font-normal break-all">
                {tableName(table)}
              </Label>
            </li>
          );
        })}
      </ul>
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
