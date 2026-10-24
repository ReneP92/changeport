import { execFileSync } from "node:child_process";
import path from "node:path";
import { clerk } from "@clerk/testing/playwright";
import { expect, test, type Page } from "@playwright/test";
import { env, readFixtures, type UserFixture } from "./fixtures";

// The overview of customer A's cluster, which the setup waits to be ready.

let fixtures: ReturnType<typeof readFixtures>;
test.beforeAll(() => {
  fixtures = readFixtures();
});

async function openOverview(page: Page, user: UserFixture): Promise<void> {
  await page.goto("/");
  await clerk.signIn({ page, emailAddress: user.email });
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Connection" })).toBeVisible({ timeout: 20_000 });
}

/** Runs a Kafka tool as customer A's own client, with a client.properties. */
function kafka(properties: string, tool: string, args: string[], input?: string): string {
  const root = path.join(import.meta.dirname, "..", "..", "..");
  return execFileSync(
    path.join(root, "hack", "kafka-with.sh"),
    [fixtures.customers.a.id, properties, tool, ...args],
    {
      cwd: root,
      env: { ...process.env, KUBE_CONTEXT: env("KUBE_CONTEXT") },
      input,
      encoding: "utf8",
      timeout: 120_000,
    },
  );
}

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
  const config = ["--bootstrap-server", bootstrap, "--command-config", "/config/client.properties"];
  kafka(properties, "kafka-topics.sh", [
    ...config,
    "--create",
    "--topic",
    topic,
    "--partitions",
    "3",
  ]);
  kafka(
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
