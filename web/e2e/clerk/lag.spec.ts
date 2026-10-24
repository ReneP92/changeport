import { expect, test } from "@playwright/test";
import { readFixtures } from "./fixtures";
import { connect, downloadClientProperties, kafka, openOverview } from "./helpers";

// Consumer lag on customer A's cluster, as the agent measures it every 30
// seconds and the metrics store scrapes it every 30 seconds.

let fixtures: ReturnType<typeof readFixtures>;
test.beforeAll(() => {
  fixtures = readFixtures();
});

test("the overview says how far behind a lagging consumer group is", async ({ page }, testInfo) => {
  test.setTimeout(6 * 60_000);
  const a = fixtures.customers.a;
  const topic = `lag-${fixtures.run}`;
  const lagging = `lagging-${fixtures.run}`;
  const current = `current-${fixtures.run}`;
  await openOverview(page, fixtures.users.adminA);
  const properties = testInfo.outputPath("client.properties");
  await downloadClientProperties(page, properties);

  // 100 messages at one moment; one group has consumed none of them, the
  // other all of them.
  kafka(a.id, properties, "kafka-topics.sh", [
    ...connect(a.id),
    "--create",
    "--topic",
    topic,
    "--partitions",
    "1",
  ]);
  kafka(
    a.id,
    properties,
    "kafka-console-producer.sh",
    [...connect(a.id), "--topic", topic],
    Array.from({ length: 100 }, (_, i) => String(i)).join("\n") + "\n",
  );
  kafka(a.id, properties, "kafka-consumer-groups.sh", [
    ...connect(a.id),
    "--group",
    lagging,
    "--topic",
    topic,
    "--reset-offsets",
    "--to-earliest",
    "--execute",
  ]);
  kafka(a.id, properties, "kafka-console-consumer.sh", [
    ...connect(a.id),
    "--topic",
    topic,
    "--group",
    current,
    "--from-beginning",
    "--max-messages",
    "100",
    "--timeout-ms",
    "60000",
  ]);

  const card = page
    .getByRole("main")
    .locator('[data-slot="card"]')
    .filter({ has: page.getByRole("heading", { name: "Consumer groups" }) });
  const row = (group: string) => card.getByRole("row").filter({ hasText: group });
  const cells = (group: string) => row(group).getByRole("cell");

  // Once measured, the lagging group is 100 messages behind and, as the
  // messages age, minutes behind; the other is caught up. The agent may
  // measure one group before the other has committed, so each gets the
  // time of a measurement and a scrape.
  await expect(cells(lagging).nth(3)).toHaveText("100", { timeout: 150_000 });
  await expect(cells(lagging).nth(1)).toHaveText(topic);
  await expect(cells(current).nth(2)).toHaveText("Caught up", { timeout: 150_000 });
  await expect(cells(current).nth(3)).toHaveText("0");
  await expect(cells(lagging).nth(2)).toHaveText(/^\d+ minutes? behind$/, { timeout: 150_000 });
  // The most behind come first.
  await expect(card.getByRole("row").nth(1)).toContainText(lagging);

  // The last hour shows the lag growing.
  await expect(
    row(lagging).getByRole("img", {
      name: /^Lag over the last hour, at most \d+ (seconds?|minutes?)$/,
    }),
  ).toBeVisible({ timeout: 150_000 });
});

test("a member sees the consumer groups too", async ({ page }) => {
  await openOverview(page, fixtures.users.memberA);
  await expect(
    page.getByRole("main").getByRole("heading", { name: "Consumer groups" }),
  ).toBeVisible();
});
