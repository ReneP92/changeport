import { execFileSync, spawn } from "node:child_process";
import path from "node:path";
import { clerk } from "@clerk/testing/playwright";
import { expect, test, type Locator, type Page } from "@playwright/test";
import { env, kubectl, type UserFixture } from "./fixtures";

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

/**
 * Starts a Kafka tool like kafka, in the background, and returns its output
 * once it exits, or rejects when it fails.
 */
export function kafkaInBackground(
  customerID: string,
  properties: string,
  tool: string,
  args: string[],
): Promise<string> {
  const root = path.join(import.meta.dirname, "..", "..", "..");
  return new Promise((resolve, reject) => {
    const child = spawn(
      path.join(root, "hack", "kafka-with.sh"),
      [customerID, properties, tool, ...args],
      {
        cwd: root,
        env: { ...process.env, KUBE_CONTEXT: env("KUBE_CONTEXT") },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let output = "";
    child.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
    child.stderr.on("data", (chunk: Buffer) => (output += chunk.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(output);
      } else {
        reject(new Error(`${tool} exited with ${String(code)}:\n${output}`));
      }
    });
  });
}

/** Checks that the page fits a phone's width, without scrolling sideways. */
export async function expectNoSidewaysScroll(page: Page): Promise<void> {
  const size = page.viewportSize();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect
    .poll(
      () =>
        page.evaluate(
          () =>
            (globalThis as unknown as { document: { documentElement: { scrollWidth: number } } })
              .document.documentElement.scrollWidth,
        ),
      {
        message: "the page is wider than a phone",
      },
    )
    .toBeLessThanOrEqual(390);
  if (size) {
    await page.setViewportSize(size);
  }
}

/**
 * Runs SQL on the Postgres on kind that stands in for a customer's database
 * (deploy/base/source-postgres), as its owner, and returns the rows.
 */
export function psql(sql: string): string {
  return kubectl(
    [
      "-n",
      "source-postgres",
      "exec",
      "-i",
      "deploy/postgres",
      "--",
      "psql",
      "-U",
      "postgres",
      "-d",
      "app",
      "-v",
      "ON_ERROR_STOP=1",
      "-qtA",
    ],
    sql,
  ).trim();
}

/** The CA that signs the stand-in database's certificate. */
export function postgresCA(): string {
  return Buffer.from(
    kubectl([
      "-n",
      "source-postgres",
      "get",
      "secret",
      "postgres-tls",
      "-o",
      "jsonpath={.data.ca\\.crt}",
    ]),
    "base64",
  ).toString();
}

/** The SQL a step of the console shows, as the customer copies it. */
export async function shownSQL(scope: Locator): Promise<string> {
  const code = scope.locator("pre code");
  await expect(code).toBeVisible();
  return (await code.textContent()) ?? "";
}

/**
 * The SQL that removes what sources left in the stand-in database: the
 * slots no connector reads, the publications and the roles whose names
 * start with prefix, such as changeport_shop_k3x9q2.
 */
export function sourceLeftovers(prefix: string): string {
  return `DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, '${prefix}') AND NOT active LOOP
    PERFORM pg_drop_replication_slot(r.slot_name);
  END LOOP;
  FOR r IN SELECT pubname FROM pg_publication WHERE starts_with(pubname, '${prefix}') LOOP
    EXECUTE format('DROP PUBLICATION %I', r.pubname);
  END LOOP;
  FOR r IN SELECT rolname FROM pg_roles WHERE starts_with(rolname, '${prefix}') LOOP
    EXECUTE format('DROP OWNED BY %I', r.rolname);
    EXECUTE format('DROP ROLE %I', r.rolname);
  END LOOP;
END $$;`;
}

/**
 * Waits until a Postgres source streams, as its page shows it. When it does
 * not, it attaches what tells why to the test: the source's status and its
 * Connect worker's log.
 */
export async function expectStreaming(
  page: Page,
  customerID: string,
  source: string,
  timeout: number,
): Promise<void> {
  try {
    await expect(
      page
        .getByRole("main")
        .locator("header [data-slot=badge]")
        .filter({ hasText: /^Streaming$/ }),
    ).toBeVisible({ timeout });
  } catch (error) {
    const namespace = `customer-${customerID}`;
    const read = (args: string[]) => {
      try {
        return kubectl(["-n", namespace, ...args]);
      } catch (failure) {
        return String(failure);
      }
    };
    await test.info().attach("source.yaml", {
      body: read(["get", "postgressource", source, "-o", "yaml"]),
      contentType: "text/yaml",
    });
    await test.info().attach("connect.log", {
      body: read(["logs", "changeport-connect-connect-0", "--tail=2000"]),
      contentType: "text/plain",
    });
    throw error;
  }
}
