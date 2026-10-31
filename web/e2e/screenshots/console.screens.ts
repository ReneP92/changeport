import { mkdirSync } from "node:fs";
import path from "node:path";
import { clerk } from "@clerk/testing/playwright";
import { expect, test, type Browser, type Page } from "@playwright/test";
import { clerkClient, kubectl, readFixtures, testEmail, type UserFixture } from "../clerk/fixtures";
import {
  connect,
  downloadClientProperties,
  expectStreaming,
  kafka,
  kafkaInBackground,
  postgresCA,
  psql,
  shownSQL,
  sourceLeftovers,
} from "../clerk/helpers";

// Captures every screen of the console, on a desktop and a phone, into
// CHANGEPORT_SCREENSHOTS_DIR (just ui-screenshots). An operator provisions
// Acme Corp and Globex through the operator console; Acme's admin, Ada,
// creates topics; traffic and consumer groups fill the charts; then Acme is
// deprovisioned. Ada also adds a Postgres source, the Postgres on kind that
// stands in for a customer's database. Everything it creates goes again at
// the end.

const out = process.env.CHANGEPORT_SCREENSHOTS_DIR ?? "";
const devices = [
  ["desktop", { width: 1280, height: 900 }],
  ["phone", { width: 390, height: 844 }],
] as const;

/**
 * Saves the page as name-desktop.png and name-phone.png, once Clerk's
 * controls have loaded: the whole page, or with a dialog open only the
 * viewport, which the dialog's overlay covers.
 */
async function shot(page: Page, name: string): Promise<void> {
  // Signed in, the header links home; its user menu comes from Clerk.
  if ((await page.getByRole("link", { name: "Changeport" }).count()) > 0) {
    await expect(page.getByRole("button", { name: "Open user menu" })).toBeVisible({
      timeout: 20_000,
    });
  }
  const dialog = (await page.getByRole("dialog").count()) > 0;
  const size = page.viewportSize();
  for (const [device, viewport] of devices) {
    await page.setViewportSize(viewport);
    // Dialogs and layout settle after a resize.
    await page.waitForTimeout(600);
    await page.screenshot({ path: path.join(out, `${name}-${device}.png`), fullPage: !dialog });
  }
  if (size) {
    await page.setViewportSize(size);
  }
}

/** Signs user in, in a browser context of their own, and opens path. */
async function signIn(browser: Browser, user: UserFixture, at: string): Promise<Page> {
  const page = await (await browser.newContext({ viewport: devices[0][1] })).newPage();
  await page.goto("/");
  await clerk.signIn({ page, emailAddress: user.email });
  await page.goto(at);
  return page;
}

let fixtures: ReturnType<typeof readFixtures>;
// What the tour creates, for the clean-up.
const created = {
  users: [] as string[],
  organizations: [] as string[],
  customers: [] as string[],
  // What the Postgres source leaves in the database start with these.
  sources: [] as string[],
};
const running: Promise<unknown>[] = [];

test.beforeAll(() => {
  fixtures = readFixtures();
  expect(out, "CHANGEPORT_SCREENSHOTS_DIR names where the screenshots go").not.toBe("");
  mkdirSync(out, { recursive: true });
});

test.afterAll(async () => {
  // Deprovisioning the customers takes a few minutes.
  test.setTimeout(12 * 60_000);
  await Promise.allSettled(running);
  const clerkAPI = clerkClient();
  for (const id of created.customers) {
    if (kubectl(["get", "customer", id, "--ignore-not-found", "-o", "name"]).trim() === "") {
      continue;
    }
    // Unprotected first, so that the controller deletes the cluster.
    kubectl([
      "patch",
      "customer",
      id,
      "--type=merge",
      "-p",
      '{"spec":{"deletionProtection":false}}',
    ]);
    kubectl(["delete", "customer", id, "--wait=false"]);
  }
  // The sources' leftovers in the database go once the customers are gone:
  // deprovisioning drops their slots as their roles, which the leftovers
  // include.
  if (created.sources.length > 0) {
    for (const id of created.customers) {
      kubectl(["wait", "--for=delete", `customer/${id}`, "--timeout=5m"]);
    }
    for (const prefix of created.sources) {
      psql(sourceLeftovers(prefix));
    }
  }
  for (const id of created.organizations) {
    await clerkAPI.organizations.deleteOrganization(id).catch(() => undefined);
  }
  for (const id of created.users) {
    await clerkAPI.users.deleteUser(id).catch(() => undefined);
  }
});

test("every screen of the console", async ({ browser }) => {
  test.setTimeout(30 * 60_000);
  const clerkAPI = clerkClient();

  // Signed out, the console shows Clerk's sign-in.
  const visitor = await (await browser.newContext({ viewport: devices[0][1] })).newPage();
  await visitor.goto("/");
  await expect(visitor.getByLabel(/email address/i)).toBeVisible({ timeout: 20_000 });
  await shot(visitor, "01-sign-in");

  // The operator provisions Globex, protected, and Acme Corp.
  const operator = await signIn(browser, fixtures.users.staff, "/operator");
  await expect(operator.getByRole("heading", { name: "Customers" })).toBeVisible({
    timeout: 20_000,
  });
  const provision = async (name: string, protect: boolean, screenshot?: string) => {
    await operator.goto("/operator");
    await operator.getByRole("button", { name: "Provision customer" }).click();
    const dialog = operator.getByRole("dialog", { name: "Provision a customer" });
    await dialog.getByLabel("Name").fill(name);
    await dialog.getByRole("radio", { name: /^dev One broker/ }).check();
    const protection = dialog.getByRole("checkbox", { name: /Deletion protection/ });
    if ((await protection.isChecked()) !== protect) {
      await protection.click();
    }
    if (screenshot) {
      // Shown with an invitation, but sent without: example addresses
      // receive no mail.
      await dialog.getByLabel("Invite an admin (optional)").fill("ada@acme.example");
      await shot(operator, screenshot);
      await dialog.getByLabel("Invite an admin (optional)").fill("");
    }
    await dialog.getByRole("button", { name: "Provision", exact: true }).click();
    await expect(operator).toHaveURL(/\/operator\/customers\/c-/, { timeout: 30_000 });
    const id = new URL(operator.url()).pathname.split("/").at(-1) ?? "";
    created.customers.push(id);
    const organization = (await operator.getByText(/^org_[A-Za-z0-9]+$/).textContent()) ?? "";
    created.organizations.push(organization);
    return { id, organization };
  };
  const globex = await provision("Globex", true);
  const acme = await provision("Acme Corp", false, "10-operator-provision");
  await shot(operator, "11-operator-customer-starting");

  // Ada, Acme's admin, sees the cluster getting ready.
  const ada = await clerkAPI.users.createUser({
    emailAddress: [testEmail(fixtures.run, "ada")],
    firstName: "Ada",
    lastName: "Lovelace",
    skipPasswordRequirement: true,
  });
  created.users.push(ada.id);
  await clerkAPI.organizations.createOrganizationMembership({
    organizationId: acme.organization,
    userId: ada.id,
    role: "org:admin",
  });
  const admin = await signIn(browser, { id: ada.id, email: testEmail(fixtures.run, "ada") }, "/");
  await expect(admin.getByRole("list", { name: "Progress" })).toBeVisible({ timeout: 20_000 });
  await shot(admin, "02-customer-starting");

  await operator.goto("/operator");
  await expect(operator.getByRole("row").filter({ hasText: globex.id })).toBeVisible();
  await shot(operator, "12-operator-customers");
  await operator.goto(`/operator/customers/${acme.id}`);
  await expect(operator.getByRole("main").getByText("Ready", { exact: true })).toBeVisible({
    timeout: 6 * 60_000,
  });
  await shot(operator, "13-operator-customer-ready");

  // Ada creates topics within the guardrails.
  await admin.reload();
  const main = admin.getByRole("main");
  await expect(main.getByRole("heading", { name: "Connection" })).toBeVisible({ timeout: 30_000 });
  const createTopic = async (
    name: string,
    partitions: number,
    retention: string,
    compact: boolean,
    screenshot?: string,
  ) => {
    await main.getByRole("button", { name: "Create topic" }).click();
    const dialog = admin.getByRole("dialog", { name: "Create a topic" });
    await dialog.getByLabel("Name").fill(name);
    await dialog.getByLabel("Partitions").fill(String(partitions));
    await dialog.getByRole("combobox", { name: "Retention" }).click();
    await admin.getByRole("option", { name: retention }).click();
    if (compact) {
      await dialog.getByRole("radio", { name: /Keep the latest message per key/ }).check();
    }
    if (screenshot) {
      await shot(admin, screenshot);
    }
    await dialog.getByRole("button", { name: "Create topic" }).click();
    await expect(dialog).toBeHidden();
  };
  await createTopic("orders", 6, "7 days", false, "04-create-topic");
  await createTopic("payments", 3, "30 days", false);
  await createTopic("customer-addresses", 3, "7 days", true);
  await createTopic("clickstream", 6, "1 hour", false);

  // The guardrails, as Ada types.
  await main.getByRole("button", { name: "Create topic" }).click();
  const guarded = admin.getByRole("dialog", { name: "Create a topic" });
  await guarded.getByLabel("Name").fill("__internal");
  await guarded.getByLabel("Partitions").fill("64");
  await shot(admin, "05-create-topic-guardrails");
  await admin.keyboard.press("Escape");

  // Ada adds a Postgres source through the wizard, running its SQL as the
  // database's owner would.
  const nav = admin.getByRole("navigation", { name: "Console" });
  await nav.getByRole("link", { name: "Sources" }).click();
  await expect(main.getByText("There are no sources yet.", { exact: false })).toBeVisible();
  await shot(admin, "20-sources-empty");
  await main.getByRole("link", { name: "Add source" }).click();
  await main.getByLabel("Source name").fill("shop");
  await main.getByLabel("Host", { exact: true }).fill("postgres.source-postgres.svc");
  await main.getByLabel("Database", { exact: true }).fill("app");
  await main.getByLabel("CA certificate (optional)").fill(postgresCA());
  await shot(admin, "21-source-connect");
  await main.getByRole("button", { name: "Check the connection" }).click();
  await expect(main.getByRole("heading", { name: "Create the role" })).toBeVisible({
    timeout: 30_000,
  });
  await shot(admin, "22-source-role");
  psql(await shownSQL(main));
  await main.getByRole("button", { name: "I ran it" }).click();
  await expect(main.getByRole("checkbox", { name: /public\.orders/ })).toBeVisible({
    timeout: 30_000,
  });
  await main.getByRole("checkbox", { name: /public\.customers/ }).check();
  await main.getByRole("checkbox", { name: /public\.orders/ }).check();
  await shot(admin, "23-source-tables");
  await main.getByRole("button", { name: "Continue" }).click();
  await expect(main.getByRole("heading", { name: "Grant and publish" })).toBeVisible();
  psql(await shownSQL(main));
  await main.getByRole("button", { name: "I ran it" }).click();
  await expect(main.getByRole("list", { name: "Checks" })).toBeVisible({ timeout: 30_000 });
  await shot(admin, "24-source-verify");
  await main.getByRole("button", { name: "Continue" }).click();
  await main.getByRole("button", { name: "Start source" }).click();
  await expect(admin).toHaveURL(/\/sources\/shop$/, { timeout: 30_000 });
  const sourceID = kubectl([
    "-n",
    `customer-${acme.id}`,
    "get",
    "postgressource",
    "shop",
    "-o",
    "jsonpath={.spec.id}",
  ]);
  created.sources.push(`changeport_shop_${sourceID}`);
  await shot(admin, "25-source-starting");
  await nav.getByRole("link", { name: "Cluster" }).click();

  // Traffic for a few minutes: producers at a steady rate, two consumer
  // groups keeping up and one left at the start of orders.
  const properties = test.info().outputPath("client.properties");
  await downloadClientProperties(admin, properties);
  const bootstrap = connect(acme.id)[1] ?? "";
  const produce = (topic: string, records: number, perSecond: number) =>
    kafkaInBackground(acme.id, properties, "kafka-producer-perf-test.sh", [
      "--topic",
      topic,
      "--num-records",
      String(records),
      "--record-size",
      "600",
      "--throughput",
      String(perSecond),
      "--producer.config",
      "/config/client.properties",
      "--producer-props",
      `bootstrap.servers=${bootstrap}`,
    ]);
  const consume = (topic: string, group: string) =>
    kafkaInBackground(acme.id, properties, "kafka-console-consumer.sh", [
      ...connect(acme.id),
      "--topic",
      topic,
      "--group",
      group,
      "--timeout-ms",
      "30000",
    ]).catch(() => "");
  kafka(acme.id, properties, "kafka-consumer-groups.sh", [
    ...connect(acme.id),
    "--group",
    "search-indexer",
    "--topic",
    "orders",
    "--reset-offsets",
    "--to-earliest",
    "--execute",
  ]);
  running.push(
    produce("orders", 60_000, 200),
    produce("payments", 9_000, 30),
    produce("clickstream", 120_000, 400),
    consume("orders", "billing"),
    consume("payments", "fraud-checks"),
  );
  // Long enough for the charts to have a few minutes, the lag of
  // search-indexer to read minutes, and its sparkline to rise.
  await admin.waitForTimeout(4 * 60_000);
  await admin.reload();
  await expect(main.getByRole("img", { name: /^Lag over the last hour/ }).first()).toBeVisible({
    timeout: 60_000,
  });
  await expect(main.getByText(/minutes? behind/).first()).toBeVisible({ timeout: 60_000 });
  await shot(admin, "03-overview");

  // Deleting a topic names the groups still reading it.
  await main
    .getByRole("row")
    .filter({ hasText: "orders" })
    .getByRole("button", { name: "Delete orders" })
    .click();
  const remove = admin.getByRole("dialog", { name: "Delete orders?" });
  await expect(remove.getByText(/consumer groups? still reads?/)).toBeVisible();
  await remove.getByLabel(/to confirm/).fill("orders");
  await shot(admin, "06-delete-topic");
  await admin.keyboard.press("Escape");

  // The source, streaming, with what admins change about it.
  await admin.goto("/sources/shop");
  await expectStreaming(admin, acme.id, "shop", 10 * 60_000);
  await shot(admin, "26-source");
  await main.getByRole("button", { name: "Add tables" }).click();
  const addTables = admin.getByRole("dialog");
  await expect(addTables.getByRole("checkbox", { name: /public\.customers/ })).toBeChecked({
    timeout: 60_000,
  });
  await shot(admin, "27-source-add-tables");
  await admin.keyboard.press("Escape");
  await main.getByRole("button", { name: "Delete source" }).click();
  const deleteSource = admin.getByRole("dialog", { name: "Delete shop?" });
  await expect(deleteSource.locator("pre code")).toBeVisible();
  await deleteSource.getByLabel(/to confirm/).fill("shop");
  await shot(admin, "28-source-delete");
  await admin.keyboard.press("Escape");
  await admin.goto("/sources");
  await expect(main.getByRole("link", { name: "shop" })).toBeVisible();
  await shot(admin, "29-sources");

  // Customers cannot open the operator console.
  await admin.goto("/operator");
  await expect(admin.getByRole("heading", { name: "Operators only" })).toBeVisible();
  await shot(admin, "07-operators-only");
  await admin.goto("/no-such-page");
  await expect(admin.getByRole("heading", { name: "There is no such page" })).toBeVisible();
  await shot(admin, "08-not-found");

  // Globex is protected; Acme is deprovisioned with its ID typed.
  await operator.goto(`/operator/customers/${globex.id}`);
  await expect(operator.getByText("Deletion protection is on", { exact: false })).toBeVisible({
    timeout: 20_000,
  });
  await shot(operator, "14-operator-customer-protected");
  await Promise.allSettled(running);
  await operator.goto(`/operator/customers/${acme.id}`);
  await operator.getByRole("button", { name: `Deprovision ${acme.id}` }).click();
  await operator.getByLabel(/to confirm/).fill(acme.id);
  await shot(operator, "15-operator-deprovision");
  await operator.getByRole("button", { name: "Deprovision", exact: true }).click();
  await expect(operator.getByText("Being deleted")).toBeVisible();
  await shot(operator, "16-operator-customer-deleting");

  // Once deprovisioned, Acme's organization has no cluster.
  await expect(
    operator.getByRole("heading", { name: `There is no customer ${acme.id}` }),
  ).toBeVisible({ timeout: 4 * 60_000 });
  await admin.goto("/");
  await expect(admin.getByRole("heading", { name: "No Kafka cluster yet" })).toBeVisible({
    timeout: 20_000,
  });
  await shot(admin, "09-no-cluster");
});
