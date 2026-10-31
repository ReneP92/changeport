import { useId, useState } from "react";
import type { SourceTable, TableRef } from "@/api/client";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { tableKey, tableName, unpickable } from "@/lib/tables";
import { cn } from "@/lib/utils";

/** Shows a filter once there are more tables than fit at a glance. */
const filterFrom = 8;

/**
 * The tables of a database to choose from: each with its estimated rows and
 * its topic, and the tables that cannot be synced disabled, with the reason.
 * Synced tables, of an existing source, show checked and cannot change here.
 */
export function TablePicker({
  tables,
  selected,
  onChange,
  max,
  synced = [],
}: {
  tables: SourceTable[];
  selected: TableRef[];
  onChange: (selected: TableRef[]) => void;
  max: number;
  synced?: TableRef[];
}) {
  const id = useId();
  const [filter, setFilter] = useState("");
  const chosen = new Set(selected.map(tableKey));
  const already = new Set(synced.map(tableKey));
  const shown = tables.filter((t) =>
    tableName(t).toLowerCase().includes(filter.trim().toLowerCase()),
  );
  const full = chosen.size + already.size >= max;
  const toggle = (table: SourceTable, on: boolean) => {
    onChange(
      on
        ? [...selected, { schema: table.schema, name: table.name }]
        : selected.filter((t) => tableKey(t) !== tableKey(table)),
    );
  };
  return (
    <div className="flex flex-col gap-3 text-sm">
      {tables.length >= filterFrom && (
        <div className="flex flex-col gap-2">
          <Label htmlFor={`${id}-filter`}>Filter tables</Label>
          <Input
            id={`${id}-filter`}
            type="search"
            autoComplete="off"
            placeholder="public.orders"
            value={filter}
            onChange={(event) => {
              setFilter(event.target.value);
            }}
          />
        </div>
      )}
      <ul aria-label="Tables" className="flex flex-col divide-y rounded-lg border">
        {shown.map((table) => {
          const key = tableKey(table);
          // Element IDs by position: table names hold characters IDs should not.
          const checkbox = `${id}-table-${String(tables.indexOf(table))}`;
          const isSynced = already.has(key);
          const problem = isSynced ? null : unpickable(table);
          const checked = isSynced || chosen.has(key);
          const disabled = isSynced || problem !== null || (!checked && full);
          return (
            <li key={key} className="flex items-start gap-3 px-3 py-2.5">
              <Checkbox
                id={checkbox}
                className="mt-0.5"
                checked={checked}
                disabled={disabled}
                onCheckedChange={(value) => {
                  toggle(table, value === true);
                }}
              />
              <Label
                htmlFor={checkbox}
                className={cn(
                  "flex min-w-0 flex-1 flex-col items-start gap-1 font-normal",
                  disabled && !isSynced && "cursor-not-allowed",
                )}
              >
                <span className="flex flex-wrap items-center gap-2">
                  <span
                    className={cn(
                      "font-medium [overflow-wrap:anywhere]",
                      problem !== null && "text-muted-foreground",
                    )}
                  >
                    {tableName(table)}
                  </span>
                  {table.partitioned && <Badge variant="outline">Partitioned</Badge>}
                  {isSynced && <Badge variant="secondary">Synced</Badge>}
                </span>
                <span className="leading-snug [overflow-wrap:anywhere] text-muted-foreground">
                  {table.estimatedRows < 0
                    ? "Rows not counted yet"
                    : `About ${table.estimatedRows.toLocaleString("en-US")} rows`}
                  {problem === null && <> · topic {table.topic}</>}
                </span>
                {problem !== null && (
                  <span className="leading-snug text-muted-foreground">{problem}</span>
                )}
              </Label>
            </li>
          );
        })}
        {shown.length === 0 && (
          <li className="px-3 py-2.5 text-muted-foreground">
            {tables.length === 0 ? "The database has no tables." : "No table matches the filter."}
          </li>
        )}
      </ul>
      <p className="text-muted-foreground" role="status">
        {chosen.size === 0
          ? "No table chosen yet."
          : `${String(chosen.size)} table${chosen.size === 1 ? "" : "s"} chosen.`}
        {full && ` A source syncs at most ${String(max)} tables.`}
      </p>
    </div>
  );
}
