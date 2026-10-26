import { expect, test } from "@playwright/test";
import { readFixtures } from "./fixtures";
import { connect, kafka, openOverview } from "./helpers";

// The overview of customer A's cluster, which the setup waits to be ready.

let fixtures: ReturnType<typeof readFixtures>;
test.beforeAll(() => {
  fixtures = readFixtures();
});

test("an admin downloads client.properties and produces with it", async ({ page }, testInfo) => {
  test.setTimeout(4 * 60_000);
  await openOverview(page, fixtures.users.adminA);
  const a = fixtures.customers.a;
  const bootstrap = `bootstrap.${a.id}.kafka.changeport.test:9094`;
  const main = page.getByRole("main");
  await expect(main.getByText(bootstrap)).toBeVisible();
  await expect(main.getByText("The broker is ready; every partition is in sync")).toBeVisible();

  const downloading = page.waitForEvent("download");
  await main.getByRole("button", { name: "Download client.properties" }).click();
  const download = await downloading;
  expect(download.suggestedFilename()).toBe(`${a.id}-client.properties`);
  const properties = testInfo.outputPath("client.properties");
  await download.saveAs(properties);

  // A client outside the cluster connects with nothing but the file.
  const topic = `overview-${fixtures.run}`;
  const config = connect(a.id);
  kafka(a.id, properties, "kafka-topics.sh", [
    ...config,
    "--create",
    "--topic",
    topic,
    "--partitions",
    "3",
  ]);
  kafka(
    a.id,
    properties,
    "kafka-console-producer.sh",
    [...config, "--topic", topic],
    "one\ntwo\nthree\n",
  );

  // The topic shows, with its size once the brokers' metrics have it.
  const row = main.getByRole("row").filter({ hasText: topic });
  await expect(async () => {
    await page.reload();
    await expect(row).toBeVisible();
    await expect(row.getByRole("cell").nth(4)).not.toHaveText("–");
  }).toPass({ timeout: 150_000, intervals: [10_000] });
  await expect(row.getByRole("cell").nth(1)).toHaveText("3");
  await expect(row.getByText("Tiered")).toBeVisible();
});

// Organizations are customers, which operators create. The setup's users
// get Clerk's default for new users, as invited customers do, so this checks
// the instance's setting: "Allow user-created organizations" is off.
test("an admin cannot create organizations", async ({ page }) => {
  await openOverview(page, fixtures.users.adminA);
  await page.locator(".cl-organizationSwitcherTrigger").click();
  const switcher = page.locator(".cl-organizationSwitcherPopoverCard");
  await expect(switcher).toBeVisible();
  await expect(switcher.getByText(`E2E ${fixtures.run} A`)).toBeVisible();
  await expect(switcher.getByText("Create organization")).toHaveCount(0);
});

test("a member sees how to connect, but not the credentials", async ({ page }) => {
  await openOverview(page, fixtures.users.memberA);
  const main = page.getByRole("main");
  await expect(
    main.getByText(`bootstrap.${fixtures.customers.a.id}.kafka.changeport.test:9094`),
  ).toBeVisible();
  await expect(main.getByRole("button", { name: "Download client.properties" })).toHaveCount(0);
  await expect(main.getByText("Ask an admin of your organization")).toBeVisible();
});

test("the throughput chart switches ranges", async ({ page }) => {
  await openOverview(page, fixtures.users.memberA);
  const ranges = page.getByRole("group", { name: "Range" });
  await expect(ranges.getByRole("button", { name: "1h" })).toHaveAttribute("aria-pressed", "true");
  await ranges.getByRole("button", { name: "24h" }).click();
  await expect(ranges.getByRole("button", { name: "24h" })).toHaveAttribute("aria-pressed", "true");
  await expect(ranges.getByRole("button", { name: "1h" })).toHaveAttribute("aria-pressed", "false");
});
