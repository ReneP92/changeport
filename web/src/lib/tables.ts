import type { SourceTable, TableRef } from "@/api/client";

/** Identifies a table by schema and name, which may both hold dots. */
export function tableKey(table: TableRef): string {
  return JSON.stringify([table.schema, table.name]);
}

/** A table as people read it: public.orders. */
export function tableName(table: TableRef): string {
  return `${table.schema}.${table.name}`;
}

/** Why a table cannot be picked, or null. */
export function unpickable(table: SourceTable): string | null {
  if (!table.keyed) {
    return table.reason ?? "It has no primary key Changeport can sync it by.";
  }
  return table.topicProblem ?? null;
}
