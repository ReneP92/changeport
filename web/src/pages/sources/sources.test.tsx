import { afterEach, describe, expect, mock, test } from "bun:test";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { Source, SourceTable, TableRef } from "@/api/client";
import { CheckList } from "@/pages/sources/CheckList";
import { PhaseBadge } from "@/pages/sources/PhaseBadge";
import { SnapshotState } from "@/pages/sources/SourcePage";
import { TablePicker } from "@/pages/sources/TablePicker";

afterEach(cleanup);

const table = (name: string, extra: Partial<SourceTable> = {}): SourceTable => ({
  schema: "public",
  name,
  partitioned: false,
  estimatedRows: 1200,
  readable: true,
  keyed: true,
  topic: `shop.public.${name}`,
  ...extra,
});

describe("TablePicker", () => {
  const tables = [
    table("orders"),
    table("events", { keyed: false, reason: "Its primary key has a timestamp column." }),
    table("invoices", { topicProblem: "Its topic shop.public.invoices exists already." }),
    table("customers", { estimatedRows: -1, partitioned: true }),
  ];

  test("disables the tables that cannot be synced, with the reason", () => {
    render(<TablePicker tables={tables} selected={[]} onChange={() => undefined} max={200} />);
    expect(screen.getByRole("checkbox", { name: /public\.orders/ }).hasAttribute("disabled")).toBe(
      false,
    );
    const events = screen.getByRole("checkbox", { name: /public\.events/ });
    expect(events.hasAttribute("disabled")).toBe(true);
    expect(screen.getByText("Its primary key has a timestamp column.")).toBeDefined();
    expect(
      screen.getByRole("checkbox", { name: /public\.invoices/ }).hasAttribute("disabled"),
    ).toBe(true);
    expect(screen.getByText("Its topic shop.public.invoices exists already.")).toBeDefined();
    const customers = screen.getByRole("checkbox", { name: /public\.customers/ }).closest("li");
    expect(customers?.textContent).toContain("Rows not counted yet");
    expect(customers?.textContent).toContain("Partitioned");
    expect(screen.getByRole("status").textContent).toBe("No table chosen yet.");
  });

  test("chooses tables, up to the most a source syncs", () => {
    const onChange = mock((chosen: TableRef[]) => chosen.length);
    const { rerender } = render(
      <TablePicker tables={tables} selected={[]} onChange={onChange} max={1} />,
    );
    fireEvent.click(screen.getByRole("checkbox", { name: /public\.orders/ }));
    expect(onChange).toHaveBeenLastCalledWith([{ schema: "public", name: "orders" }]);
    rerender(
      <TablePicker
        tables={tables}
        selected={[{ schema: "public", name: "orders" }]}
        onChange={onChange}
        max={1}
      />,
    );
    expect(
      screen.getByRole("checkbox", { name: /public\.customers/ }).hasAttribute("disabled"),
    ).toBe(true);
    expect(screen.getByRole("status").textContent).toContain("A source syncs at most 1 tables.");
  });

  test("shows a source's synced tables checked, and filters many tables", () => {
    const many = Array.from({ length: 10 }, (_, i) => table(`t${String(i)}`));
    render(
      <TablePicker
        tables={many}
        selected={[]}
        onChange={() => undefined}
        max={200}
        synced={[{ schema: "public", name: "t3" }]}
      />,
    );
    const synced = screen.getByRole("checkbox", { name: /public\.t3/ });
    expect(synced.getAttribute("data-state")).toBe("checked");
    expect(synced.hasAttribute("disabled")).toBe(true);
    fireEvent.change(screen.getByLabelText("Filter tables"), { target: { value: "T7" } });
    const list = screen.getByRole("list", { name: "Tables" });
    expect(within(list).getAllByRole("checkbox")).toHaveLength(1);
    expect(list.textContent).toContain("public.t7");
  });
});

describe("CheckList", () => {
  test("says what each check found, with its status", () => {
    render(
      <CheckList
        label="Checks"
        report={{
          ok: false,
          results: [
            { check: "reachable", status: "pass", message: "db.example.com:5432 answers." },
            { check: "wal_keep_size", status: "warn", message: "Set max_slot_wal_keep_size." },
            { check: "authentication", status: "fail", message: "The login failed." },
            {
              check: "version",
              status: "skipped",
              message: "Skipped until Changeport can log in.",
            },
          ],
        }}
      />,
    );
    const items = within(screen.getByRole("list", { name: "Checks" })).getAllByRole("listitem");
    expect(items.map((item) => within(item).getByRole("img").getAttribute("aria-label"))).toEqual([
      "Passed",
      "Warning",
      "Failed",
      "Skipped",
    ]);
    expect(items[2]?.textContent).toBe("LoginThe login failed.");
  });
});

const source: Source = {
  name: "shop",
  id: "k3x9q2",
  provider: "generic",
  role: "changeport_shop_k3x9q2",
  host: "db.example.com",
  port: 5432,
  database: "shop",
  tlsMode: "verify-full",
  tables: [],
  retentionMs: 604800000,
  state: "Running",
  phase: "Streaming",
  ready: true,
  reason: "Streaming",
  message: "Changes stream.",
  createdAt: "2026-10-30T09:00:00Z",
};

describe("PhaseBadge", () => {
  test("names each phase in words", () => {
    const cases: [Partial<Source>, string][] = [
      [{ phase: "Streaming" }, "Streaming"],
      [{ phase: "Snapshotting" }, "Copying rows"],
      [{ phase: "Stopped" }, "Paused"],
      [{ phase: "Failed" }, "Failed"],
      [{ phase: "Pending" }, "Starting"],
      [{ phase: "Streaming", deleting: true }, "Being deleted"],
    ];
    for (const [change, want] of cases) {
      const { unmount } = render(<PhaseBadge source={{ ...source, ...change }} />);
      expect(screen.getByText(want)).toBeDefined();
      unmount();
    }
  });
});

describe("SnapshotState", () => {
  test("shows a running copy's progress, capped at the estimate", () => {
    render(
      <SnapshotState
        table={{
          schema: "public",
          name: "orders",
          topic: "shop.public.orders",
          snapshot: "Running",
          rowsCopied: 1300,
          rowsEstimated: 1200,
        }}
      />,
    );
    const bar = screen.getByRole("progressbar", { name: "Copying public.orders" });
    expect(bar.getAttribute("aria-valuenow")).toBe("1200");
    expect(screen.getByText("1,300 of about 1,200")).toBeDefined();
  });

  test("shows a copy that has not reported a chunk yet as starting", () => {
    render(
      <SnapshotState
        table={{
          schema: "public",
          name: "orders",
          topic: "shop.public.orders",
          snapshot: "Running",
        }}
      />,
    );
    expect(screen.getByText("Starting the copy")).toBeDefined();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  test("shows why a copy waits", () => {
    render(
      <SnapshotState
        table={{
          schema: "public",
          name: "items",
          topic: "shop.public.items",
          snapshot: "Pending",
          message: "The publication does not publish public.items.",
        }}
      />,
    );
    expect(screen.getByText("Waiting")).toBeDefined();
    expect(screen.getByText("The publication does not publish public.items.")).toBeDefined();
  });
});
