import { execFileSync } from "node:child_process";
import path from "node:path";
import { clerk } from "@clerk/testing/playwright";
import { expect, type Page } from "@playwright/test";
import { env, type UserFixture } from "./fixtures";

/** Signs user in and opens the overview of their ready cluster. */
export async function openOverview(page: Page, user: UserFixture): Promise<void> {
  await page.goto("/");
  await clerk.signIn({ page, emailAddress: user.email });
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Connection" })).toBeVisible({ timeout: 20_000 });
}

/** Downloads the admin user's client.properties from the overview into path. */
export async function downloadClientProperties(page: Page, path: string): Promise<void> {
  const downloading = page.waitForEvent("download");
  await page.getByRole("main").getByRole("button", { name: "Download client.properties" }).click();
  await (await downloading).saveAs(path);
}

/**
 * Runs a Kafka tool as a customer's own client from the kind network, with a
 * client.properties (hack/kafka-with.sh), and returns its output.
 */
export function kafka(
  customerID: string,
  properties: string,
  tool: string,
  args: string[],
  input?: string,
): string {
  const root = path.join(import.meta.dirname, "..", "..", "..");
  return execFileSync(
    path.join(root, "hack", "kafka-with.sh"),
    [customerID, properties, tool, ...args],
    {
      cwd: root,
      env: { ...process.env, KUBE_CONTEXT: env("KUBE_CONTEXT") },
      input,
      encoding: "utf8",
      timeout: 120_000,
    },
  );
}

/** The command-line arguments that connect a Kafka tool to a customer's cluster. */
export function connect(customerID: string): string[] {
  return [
    "--bootstrap-server",
    `bootstrap.${customerID}.kafka.changeport.test:9094`,
    "--command-config",
    "/config/client.properties",
  ];
}
