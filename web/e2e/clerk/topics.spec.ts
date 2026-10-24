import { expect, test } from "@playwright/test";
import { kubectl, readFixtures } from "./fixtures";
import {
  connect,
  downloadClientProperties,
  expectNoSidewaysScroll,
  kafka,
  openOverview,
} from "./helpers";

// Topic management on customer A's cluster, within the guardrails of
// internal/topics.

let fixtures: ReturnType<typeof readFixtures>;
test.beforeAll(() => {
  fixtures = readFixtures();
});

test("an admin creates a tiered one-hour topic and deletes it", async ({ page }, testInfo) => {
  test.setTimeout(4 * 60_000);
  const a = fixtures.customers.a;
  const topic = `hourly-${fixtures.run}`;
  await openOverview(page, fixtures.users.adminA);
  const main = page.getByRole("main");

  await main.getByRole("button", { name: "Create topic" }).click();
  const create = page.getByRole("dialog", { name: "Create a topic" });
  await create.getByLabel("Name").fill(topic);
  await create.getByLabel("Partitions").fill("2");
  await create.getByRole("combobox", { name: "Retention" }).click();
  await page.getByRole("option", { name: "1 hour" }).click();
  await create.getByRole("button", { name: "Create topic" }).click();
  await expect(create).toBeHidden();

  const row = main.getByRole("row").filter({ hasText: topic });
  await expect(row).toBeVisible();
  await expect(row.getByRole("cell").nth(1)).toHaveText("2");
  await expect(row.getByRole("cell").nth(2)).toHaveText("1 hour");
  await expect(row.getByText("Tiered")).toBeVisible();
  // The table with its actions scrolls sideways on a phone; the page does not.
  await expectNoSidewaysScroll(page);

  // Kafka has the topic tiered, keeping its hour locally.
  const properties = testInfo.outputPath("client.properties");
  await downloadClientProperties(page, properties);
  const configs = kafka(a.id, properties, "kafka-configs.sh", [
    ...connect(a.id),
    "--describe",
    "--entity-type",
    "topics",
    "--entity-name",
    topic,
  ]);
  for (const want of [
    "remote.storage.enable=true",
    "retention.ms=3600000",
    "local.retention.ms=3600000",
  ]) {
    expect(configs).toContain(want);
  }

  // A consumer group reads it, which the delete dialog warns about.
  const group = `reader-${fixtures.run}`;
  kafka(
    a.id,
    properties,
    "kafka-console-producer.sh",
    [...connect(a.id), "--topic", topic],
    "hello\n",
  );
  kafka(a.id, properties, "kafka-console-consumer.sh", [
    ...connect(a.id),
    "--topic",
    topic,
    "--group",
    group,
    "--from-beginning",
    "--max-messages",
    "1",
    "--timeout-ms",
    "60000",
  ]);

  await row.getByRole("button", { name: `Delete ${topic}` }).click();
  const remove = page.getByRole("dialog", { name: `Delete ${topic}?` });
  await expect(remove.getByText(`1 consumer group still reads it: ${group}.`)).toBeVisible();
  const confirm = remove.getByRole("button", { name: "Delete topic" });
  await expect(confirm).toBeDisabled();
  await remove.getByLabel(/to confirm/).fill(`${topic}-not`);
  await expect(confirm).toBeDisabled();
  await remove.getByLabel(/to confirm/).fill(topic);
  await confirm.click();
  await expect(remove).toBeHidden();
  await expect(row).toHaveCount(0);

  // Both changes are in the console's audit log, with the user.
  const logs = kubectl([
    "-n",
    "changeport",
    "logs",
    "-l",
    "app.kubernetes.io/name=changeport-console",
    "--tail=-1",
    "--since=15m",
  ]);
  for (const change of ["topic created", "topic deleted"]) {
    const line = logs
      .split("\n")
      .find((l) => l.includes(`msg="${change}"`) && l.includes(`topic=${topic}`));
    expect(line, `an audit line for "${change}"`).toBeDefined();
    expect(line).toContain(`user=${fixtures.users.adminA.id}`);
    expect(line).toContain("audit=true");
  }
});

test("the create dialog applies the guardrails as you type", async ({ page }) => {
  await openOverview(page, fixtures.users.adminA);
  await page.getByRole("main").getByRole("button", { name: "Create topic" }).click();
  const create = page.getByRole("dialog", { name: "Create a topic" });
  const name = create.getByLabel("Name");

  await name.fill("__internal");
  await expect(
    create.getByText('Names starting with "_" are reserved for the platform.'),
  ).toBeVisible();
  await name.fill("my topic");
  await expect(
    create.getByText("Use only letters, digits, dots, underscores and dashes."),
  ).toBeVisible();
  await name.fill(`guarded-${fixtures.run}`);
  await create.getByLabel("Partitions").fill("99");
  await expect(create.getByText("Choose 1 to 12 partitions.")).toBeVisible();

  // Compaction is offered, and why it is not tiered said.
  await expect(
    create.getByText("Not tiered: tiered storage does not support compaction.", { exact: false }),
  ).toBeVisible();

  // Nothing breaking a guardrail is sent.
  await create.getByRole("button", { name: "Create topic" }).click();
  await expect(create).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(
    page
      .getByRole("main")
      .getByRole("row")
      .filter({ hasText: `guarded-${fixtures.run}` }),
  ).toHaveCount(0);
});

test("a member sees the topics but cannot change them", async ({ page }) => {
  await openOverview(page, fixtures.users.memberA);
  const main = page.getByRole("main");
  await expect(main.getByRole("heading", { name: "Topics" })).toBeVisible();
  await expect(main.getByRole("button", { name: "Create topic" })).toHaveCount(0);
  await expect(main.getByRole("button", { name: /^Delete / })).toHaveCount(0);
});
