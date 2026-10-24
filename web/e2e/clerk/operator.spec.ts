import { clerk } from "@clerk/testing/playwright";
import { expect, test, type Page } from "@playwright/test";
import { clerkClient, kubectl, readFixtures, testEmail } from "./fixtures";
import { expectNoSidewaysScroll } from "./helpers";

// The operator console, signed in as this run's staff member. The customer
// it provisions is named like the run's organizations, so that the teardown
// and the janitor remove it and its organization.

let fixtures: ReturnType<typeof readFixtures>;
test.beforeAll(() => {
  fixtures = readFixtures();
});

/** Signs the run's staff member in and opens the operator console. */
async function openOperatorConsole(page: Page): Promise<void> {
  await page.goto("/");
  await clerk.signIn({ page, emailAddress: fixtures.users.staff.email });
  await page.goto("/operator");
  await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible({ timeout: 20_000 });
}

/** Calls the console's API with the signed-in user's session token. */
async function callAPI(page: Page, method: string, path: string): Promise<number> {
  return page.evaluate(
    async ({ method, path }) => {
      const window = globalThis as unknown as {
        Clerk: { session: { getToken(): Promise<string | null> } | null };
      };
      const token = (await window.Clerk.session?.getToken()) ?? "";
      const response = await fetch(path, { method, headers: { Authorization: `Bearer ${token}` } });
      return response.status;
    },
    { method, path },
  );
}

test("an operator provisions a customer, watches it get ready, and deprovisions it", async ({
  page,
  browser,
}) => {
  // Creating a cluster takes a minute or two, deleting it about one.
  test.setTimeout(10 * 60_000);
  const clerkAPI = clerkClient();
  const displayName = `E2E ${fixtures.run} C`;
  const invited = testEmail(fixtures.run, "invited");
  await openOperatorConsole(page);

  await page.getByRole("button", { name: "Provision customer" }).click();
  const dialog = page.getByRole("dialog", { name: "Provision a customer" });
  await dialog.getByLabel("Name").fill(displayName);
  await dialog.getByRole("radio", { name: /^dev One broker/ }).check();
  await expect(dialog.getByRole("checkbox", { name: /Deletion protection/ })).not.toBeChecked();
  await dialog.getByLabel("Invite an admin (optional)").fill(invited);
  await dialog.getByRole("button", { name: "Provision", exact: true }).click();

  // The customer's page follows its cluster as it gets ready.
  await expect(page).toHaveURL(/\/operator\/customers\/c-[a-km-z2-9]{6}$/, { timeout: 30_000 });
  const id = new URL(page.url()).pathname.split("/").at(-1) ?? "";
  const main = page.getByRole("main");
  await expect(main.getByText(displayName)).toBeVisible();
  await expect(main.getByText(`Customer ${id}, dev cluster`)).toBeVisible();
  await expect(main.getByRole("list", { name: "Progress" })).toBeVisible();
  await expect(main.getByText("Ready", { exact: true })).toBeVisible({ timeout: 6 * 60_000 });

  // Its Clerk organization exists, with the invitation of its first admin.
  const organizationID = (await main.getByText(/^org_[A-Za-z0-9]+$/).textContent()) ?? "";
  expect(organizationID).toMatch(/^org_/);
  const organization = await clerkAPI.organizations.getOrganization({
    organizationId: organizationID,
  });
  expect(organization.name).toBe(displayName);
  const invitations = await clerkAPI.organizations.getOrganizationInvitationList({
    organizationId: organizationID,
  });
  expect(invitations.data.map((i) => [i.emailAddress, i.role, i.status])).toEqual([
    [invited, "org:admin", "pending"],
  ]);

  // The list shows it with every part ready.
  await page.getByRole("link", { name: "All customers" }).click();
  const row = main.getByRole("row").filter({ hasText: id });
  await expect(row.getByRole("img", { name: "4 of 4 parts ready" })).toBeVisible();
  await expectNoSidewaysScroll(page);
  await row.getByRole("link", { name: displayName }).click();

  // Deprovisioning takes the customer ID typed.
  await page.getByRole("button", { name: `Deprovision ${id}` }).click();
  const confirm = page.getByRole("dialog", { name: `Deprovision ${displayName}?` });
  const deprovision = confirm.getByRole("button", { name: "Deprovision", exact: true });
  await expect(deprovision).toBeDisabled();
  await confirm.getByLabel(/to confirm/).fill(displayName);
  await expect(deprovision).toBeDisabled();
  await confirm.getByLabel(/to confirm/).fill(id);
  await deprovision.click();
  await expect(main.getByText("Being deleted")).toBeVisible();
  await expect(main.getByRole("heading", { name: `There is no customer ${id}` })).toBeVisible({
    timeout: 4 * 60_000,
  });
  expect(kubectl(["get", "customers", "-o", "name"])).not.toContain(
    `customer.changeport.io/${id}\n`,
  );

  // The organization is kept, linked to no customer: its members are told
  // it has no cluster.
  expect(
    (await clerkAPI.organizations.getOrganization({ organizationId: organizationID })).name,
  ).toBe(displayName);
  const member = await clerkAPI.users.createUser({
    emailAddress: [testEmail(fixtures.run, "kept")],
    firstName: "E2E",
    lastName: "kept",
    skipPasswordRequirement: true,
  });
  try {
    await clerkAPI.organizations.createOrganizationMembership({
      organizationId: organizationID,
      userId: member.id,
      role: "org:member",
    });
    const context = await browser.newContext();
    const memberPage = await context.newPage();
    await memberPage.goto("/");
    await clerk.signIn({ page: memberPage, emailAddress: testEmail(fixtures.run, "kept") });
    await memberPage.goto("/");
    await expect(memberPage.getByRole("heading", { name: "No Kafka cluster yet" })).toBeVisible({
      timeout: 20_000,
    });
    await context.close();
  } finally {
    await clerkAPI.users.deleteUser(member.id);
  }
  const audit = kubectl([
    "-n",
    "changeport",
    "logs",
    "-l",
    "app.kubernetes.io/name=changeport-console",
    "--tail=-1",
    "--since=15m",
  ]);
  for (const change of ["customer provisioned", "customer deprovisioned"]) {
    expect(
      audit.split("\n").find((l) => l.includes(`msg="${change}"`) && l.includes(`tenant=${id}`)),
      `an audit line for "${change}"`,
    ).toContain(`user=${fixtures.users.staff.id}`);
  }
});

test("a protected customer cannot be deprovisioned", async ({ page }) => {
  const b = fixtures.customers.b;
  const protect = (on: boolean) =>
    kubectl([
      "patch",
      "customer",
      b.id,
      "--type=merge",
      "-p",
      JSON.stringify({ spec: { deletionProtection: on } }),
    ]);
  protect(true);
  try {
    await openOperatorConsole(page);
    await page.goto(`/operator/customers/${b.id}`);
    await expect(page.getByText("Deletion protection is on", { exact: false })).toBeVisible();
    await expect(page.getByRole("button", { name: `Deprovision ${b.id}` })).toBeDisabled();
    // The API refuses it too.
    expect(await callAPI(page, "DELETE", `/api/operator/customers/${b.id}`)).toBe(409);
    expect(kubectl(["get", "customer", b.id, "-o", "name"]).trim()).toBe(
      `customer.changeport.io/${b.id}`,
    );
  } finally {
    // Unprotected, the teardown can remove the customer.
    protect(false);
  }
});
