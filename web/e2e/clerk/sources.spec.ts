import { expect, test, type Locator, type Page } from "@playwright/test";
import { kubectl, readFixtures } from "./fixtures";
import {
  expectNoSidewaysScroll,
  expectStreaming,
  openOverview,
  postgresCA,
  psql,
  shownSQL,
  sourceLeftovers,
} from "./helpers";

// A Postgres source of customer A, added through the wizard against the
// Postgres on kind that stands in for a customer's database
// (deploy/base/source-postgres), then changed, looked at by a member, and
// deleted, as its admin would. The SQL the console shows runs as the
// database's owner, as the customer runs it.

let fixtures: ReturnType<typeof readFixtures>;
// The source, and a table of the test's own that it adds later.
let name: string;
let items: string;

test.beforeAll(() => {
  fixtures = readFixtures();
  name = `src-${fixtures.run}`;
  items = `items_${fixtures.run}`;
  psql(
    `CREATE TABLE public.${items} (id integer PRIMARY KEY, label text NOT NULL);
     INSERT INTO public.${items} SELECT g, 'item ' || g FROM generate_series(1, 50) g;`,
  );
});

test.afterAll(() => {
  const namespace = `customer-${fixtures.customers.a.id}`;
  kubectl([
    "-n",
    namespace,
    "delete",
    "postgressource",
    name,
    "--ignore-not-found",
    "--wait=false",
  ]);
  kubectl(["-n", namespace, "wait", "--for=delete", `postgressource/${name}`, "--timeout=3m"]);
  psql(sourceLeftovers(`changeport_${name.replaceAll("-", "_")}_`));
  psql(`DROP TABLE IF EXISTS public.${items}`);
});

/** The source page's phase badge. */
function phase(page: Page, text: string): Locator {
  return page
    .getByRole("main")
    .locator("header [data-slot=badge]")
    .filter({ hasText: new RegExp(`^${text}$`) });
}

test("an admin adds a Postgres source, changes it and deletes it", async ({ page, browser }) => {
  test.setTimeout(20 * 60_000);
  const a = fixtures.customers.a;
  await openOverview(page, fixtures.users.adminA);
  const main = page.getByRole("main");

  // The sources page, from the header.
  await page
    .getByRole("navigation", { name: "Console" })
    .getByRole("link", { name: "Sources" })
    .click();
  await expect(main.getByRole("heading", { name: "Postgres sources" })).toBeVisible();
  await main.getByRole("link", { name: "Add source" }).click();
  await expect(main.getByRole("heading", { name: "Add a Postgres source" })).toBeVisible();

  // 1. Connect, verifying the stand-in's certificate against the local CA.
  await main.getByLabel("Source name").fill("Not A Name");
  await expect(
    main.getByText("Use lowercase letters, digits and dashes", { exact: false }),
  ).toBeVisible();
  await main.getByLabel("Source name").fill(name);
  await main.getByLabel("Host", { exact: true }).fill("postgres.source-postgres.svc");
  await main.getByLabel("Database", { exact: true }).fill("app");
  await main.getByLabel("CA certificate (optional)").fill(postgresCA());
  await expectNoSidewaysScroll(page);
  await main.getByRole("button", { name: "Check the connection" }).click();

  // 2. The role, with the password the console chose.
  await expect(main.getByRole("heading", { name: "Create the role" })).toBeVisible({
    timeout: 30_000,
  });
  const roleSQL = await shownSQL(main);
  expect(roleSQL).toContain("CREATE ROLE");
  psql(roleSQL);
  await main.getByRole("button", { name: "I ran it" }).click();

  // 3. The tables: those without a key Changeport can use are disabled, with the reason.
  await expect(main.getByRole("heading", { name: "Choose tables" })).toBeVisible({
    timeout: 30_000,
  });
  const notes = main.getByRole("checkbox", { name: /public\.notes/ });
  await expect(notes).toBeDisabled();
  await expect(main.getByRole("listitem").filter({ hasText: "public.notes" })).toContainText(
    "primary key",
  );
  await main.getByRole("checkbox", { name: /public\.customers/ }).check();
  await main.getByRole("checkbox", { name: /public\.orders/ }).check();
  await main.getByRole("combobox", { name: "Topics keep messages for" }).click();
  await page.getByRole("option", { name: "1 hour" }).click();
  await main.getByRole("button", { name: "Continue" }).click();

  // 4. Grants and the publication.
  await expect(main.getByRole("heading", { name: "Grant and publish" })).toBeVisible();
  const tablesSQL = await shownSQL(main);
  expect(tablesSQL).toContain("CREATE PUBLICATION");
  psql(tablesSQL);
  await main.getByRole("button", { name: "I ran it" }).click();

  // 5. Every check passes, then 6. the source starts.
  const checks = main.getByRole("list", { name: "Checks" });
  await expect(checks).toBeVisible({ timeout: 30_000 });
  await expect(checks.getByRole("img", { name: "Failed" })).toHaveCount(0);
  await main.getByRole("button", { name: "Continue" }).click();
  await expect(main.getByRole("heading", { name: "Start" })).toBeVisible();
  await expect(main.getByText("public.customers, public.orders")).toBeVisible();
  await main.getByRole("button", { name: "Start source" }).click();

  // The source copies the tables' rows, then streams.
  await expect(page).toHaveURL(new RegExp(`/sources/${name}$`), { timeout: 30_000 });
  await expect(main.getByRole("heading", { name })).toBeVisible();
  await expectStreaming(page, a.id, name, 10 * 60_000);
  const tables = main.getByRole("list", { name: "Tables" });
  const customers = Number(psql("SELECT count(*) FROM customers"));
  await expect(tables.getByRole("listitem").filter({ hasText: "public.customers" })).toContainText(
    `Copied ${customers.toLocaleString("en-US")}`,
  );
  await expect(main.getByText("Topics keep messages for")).toBeVisible();
  await expect(main.getByText(`changeport_${name.replaceAll("-", "_")}_`).first()).toBeVisible();
  await expectNoSidewaysScroll(page);

  // The source's topics are marked as its own, and cannot be deleted alone.
  await page
    .getByRole("navigation", { name: "Console" })
    .getByRole("link", { name: "Cluster" })
    .click();
  const topic = main.getByRole("row").filter({ hasText: `${name}.public.orders` });
  await expect(topic).toBeVisible({ timeout: 60_000 });
  await expect(topic.getByRole("link", { name: `Source ${name}` })).toBeVisible();
  await expect(topic.getByRole("button", { name: /^Delete / })).toHaveCount(0);
  await topic.getByRole("link", { name: `Source ${name}` }).click();
  await expect(main.getByRole("heading", { name })).toBeVisible();

  // Paused, the source keeps its slot; resumed, it streams again.
  await main.getByRole("button", { name: "Pause" }).click();
  await expect(phase(page, "Paused")).toBeVisible({ timeout: 3 * 60_000 });
  await expect(main.getByRole("status").first()).toContainText("Paused.");
  await main.getByRole("button", { name: "Resume" }).click();
  await expect(phase(page, "Streaming")).toBeVisible({ timeout: 3 * 60_000 });

  // A table added later: the controller lists the database's tables, and
  // the source copies the new one's rows.
  await main.getByRole("button", { name: "Add tables" }).click();
  // The dialog is named after its step: the tables, then the SQL.
  const add = page.getByRole("dialog");
  await expect(add.getByRole("heading", { name: `Add tables to ${name}` })).toBeVisible();
  const synced = add.getByRole("checkbox", { name: /public\.customers/ });
  await expect(synced).toBeChecked({ timeout: 60_000 });
  await expect(synced).toBeDisabled();
  await add.getByRole("checkbox", { name: new RegExp(`public\\.${items}`) }).check();
  await add.getByRole("button", { name: "Continue" }).click();
  psql(await shownSQL(add));
  await add.getByRole("button", { name: "I ran it, add 1 table" }).click();
  await expect(add).toBeHidden();
  await expect(tables.getByRole("listitem").filter({ hasText: `public.${items}` })).toContainText(
    "Copied 50",
    { timeout: 5 * 60_000 },
  );

  // And removed again; its topic stays.
  await main.getByRole("button", { name: "Remove tables" }).click();
  const remove = page.getByRole("dialog");
  await expect(remove.getByRole("heading", { name: `Remove tables from ${name}` })).toBeVisible();
  await remove.getByRole("checkbox", { name: `public.${items}` }).check();
  await remove.getByRole("button", { name: "Continue" }).click();
  const removeSQL = await shownSQL(remove);
  await remove.getByRole("button", { name: "Remove 1 table" }).click();
  await expect(remove).toBeHidden();
  psql(removeSQL);
  await expect(tables.getByRole("listitem").filter({ hasText: `public.${items}` })).toHaveCount(0);

  // A new password, used once it logs in.
  await main.getByRole("button", { name: "New password" }).click();
  const password = page.getByRole("dialog", { name: /^A new password for / });
  psql(await shownSQL(password));
  await password.getByRole("button", { name: "I ran it" }).click();
  await expect(password).toBeHidden({ timeout: 30_000 });

  // The retention of the source's topics.
  await main.getByRole("combobox", { name: "Topics keep messages for" }).click();
  await page.getByRole("option", { name: "1 day" }).click();
  await expect
    .poll(() =>
      kubectl([
        "-n",
        `customer-${a.id}`,
        "get",
        "postgressource",
        name,
        "-o",
        "jsonpath={.spec.retentionMs}",
      ]),
    )
    .toBe("86400000");

  // A member sees the source, and changes nothing.
  const member = await (await browser.newContext()).newPage();
  await openOverview(member, fixtures.users.memberA);
  await member.goto(`/sources/${name}`);
  const memberMain = member.getByRole("main");
  await expect(memberMain.getByRole("heading", { name })).toBeVisible();
  await expect(memberMain.getByRole("heading", { name: "Tables" })).toBeVisible();
  for (const button of ["Add tables", "Remove tables", "Pause", "New password", "Delete source"]) {
    await expect(memberMain.getByRole("button", { name: button })).toHaveCount(0);
  }
  await member.goto("/sources/new");
  await expect(member.getByRole("heading", { name: "Admins only" })).toBeVisible();
  await member.context().close();

  // Deleting takes the source's name, and drops its slot.
  await main.getByRole("button", { name: "Delete source" }).click();
  const deletion = page.getByRole("dialog", { name: `Delete ${name}?` });
  const cleanupSQL = await shownSQL(deletion);
  expect(cleanupSQL).toContain("DROP ROLE");
  const confirm = deletion.getByRole("button", { name: "Delete source" });
  await expect(confirm).toBeDisabled();
  await deletion.getByLabel(/to confirm/).fill(name);
  await confirm.click();
  await expect(deletion).toBeHidden();
  await expect(main.getByRole("heading", { name: `There is no source ${name}` })).toBeVisible({
    timeout: 3 * 60_000,
  });
  expect(
    psql(
      `SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, 'changeport_${name.replaceAll("-", "_")}_')`,
    ),
  ).toBe("0");
  psql(cleanupSQL);

  // Every change is in the console's audit log, with the user.
  const logs = kubectl([
    "-n",
    "changeport",
    "logs",
    "-l",
    "app.kubernetes.io/name=changeport-console",
    "--tail=-1",
    "--since=30m",
  ]);
  for (const change of [
    "source created",
    "source changed",
    "source password changed",
    "source deleted",
  ]) {
    const line = logs
      .split("\n")
      .find((l) => l.includes(`msg="${change}"`) && l.includes(`source=${name}`));
    expect(line, `an audit line for "${change}"`).toBeDefined();
    expect(line).toContain(`user=${fixtures.users.adminA.id}`);
  }
});
