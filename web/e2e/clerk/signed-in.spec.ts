import { clerk, setupClerkTestingToken } from "@clerk/testing/playwright";
import { expect, test, type APIRequestContext, type Browser, type Page } from "@playwright/test";
import { readFixtures, type UserFixture } from "./fixtures";

// Signed in as this run's test users (clerk.setup.ts), against the console on
// kind with a real Clerk development instance. Customers are looked for in the
// page's main content: the organization switcher in the header shows the
// names of organizations too.

// Playwright loads this file before the setup project has run, so the
// fixtures are read in the tests.
let fixtures: ReturnType<typeof readFixtures>;
let a: (typeof fixtures)["customers"]["a"];
let b: (typeof fixtures)["customers"]["b"];
test.beforeAll(() => {
  fixtures = readFixtures();
  ({ a, b } = fixtures.customers);
});

/** Signs in as user through a Clerk sign-in ticket, and opens the console. */
async function signIn(page: Page, user: UserFixture): Promise<void> {
  await page.goto("/");
  await clerk.signIn({ page, emailAddress: user.email });
  await page.goto("/");
}

/**
 * Signs user in, in a browser context of their own, and returns their session
 * token as the web app sends it.
 */
async function sessionToken(browser: Browser, user: UserFixture): Promise<string> {
  const context = await browser.newContext();
  try {
    const page = await context.newPage();
    await signIn(page, user);
    // The app has loaded the session once it shows the signed-in header.
    await expect(page.getByRole("link", { name: "Changeport" })).toBeVisible();
    const token = await page.evaluate(async () => {
      const window = globalThis as unknown as {
        Clerk: { session: { getToken(): Promise<string | null> } | null };
      };
      return (await window.Clerk.session?.getToken()) ?? null;
    });
    if (!token) {
      throw new Error(`${user.email} has no session token`);
    }
    return token;
  } finally {
    await context.close();
  }
}

/** Calls the console's API with token, returning the status and body. */
async function call(request: APIRequestContext, path: string, token: string) {
  const response = await request.get(path, { headers: { Authorization: `Bearer ${token}` } });
  return { status: response.status(), body: (await response.json()) as Record<string, unknown> };
}

test("a customer signs in with the form and sees their cluster", async ({ page }) => {
  await setupClerkTestingToken({ page });
  await page.goto("/");
  await page.getByLabel(/email address/i).fill(fixtures.users.formA.email);
  // Clerk shows the code step before it has sent the code; a code entered
  // sooner, which no person manages, is refused.
  const codeSent = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url().includes("/prepare_first_factor") &&
      response.ok(),
  );
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await codeSent;
  await expect(page.getByRole("heading", { name: "Check your email" })).toBeVisible();
  // Clerk's test addresses take this code instead of an email.
  await page.getByRole("textbox", { name: "Enter verification code" }).pressSequentially("424242");
  await expect(page.getByRole("main").getByText(a.displayName)).toBeVisible({ timeout: 20_000 });
  await expect(page.getByRole("main").getByText(`Customer ${a.id}, dev cluster`)).toBeVisible();
});

test("a customer only ever sees their own cluster", async ({ page }) => {
  await signIn(page, fixtures.users.memberB);
  await expect(page.getByRole("main").getByText(b.displayName)).toBeVisible();
  await expect(page.getByRole("main").getByText(a.displayName)).toHaveCount(0);
});

test("a customer cannot open the operator console", async ({ page }) => {
  await signIn(page, fixtures.users.adminA);
  await expect(page.getByRole("main").getByText(a.displayName)).toBeVisible();
  await page.goto("/operator");
  await expect(page.getByRole("heading", { name: "Operators only" })).toBeVisible();
  await expect(page.getByRole("main").getByText(b.displayName)).toHaveCount(0);
});

test("an operator sees every customer", async ({ page }) => {
  await signIn(page, fixtures.users.staff);
  await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();
  await expect(page.getByRole("main").getByText(a.displayName)).toBeVisible();
  await expect(page.getByRole("main").getByText(b.displayName)).toBeVisible();
});

test("the API scopes customers to their organization and operators to staff", async ({
  browser,
  request,
}) => {
  // Four users sign in, one after the other.
  test.setTimeout(120_000);
  const memberA = await sessionToken(browser, fixtures.users.memberA);
  expect(await call(request, "/api/me", memberA)).toMatchObject({
    status: 200,
    body: { access: "customer", customerID: a.id, admin: false },
  });
  expect(await call(request, "/api/customer", memberA)).toMatchObject({
    status: 200,
    body: { id: a.id },
  });
  for (const path of ["/api/operator/customers", `/api/operator/customers/${b.id}`]) {
    expect((await call(request, path, memberA)).status).toBe(403);
  }
  // Only an organization's admins may download its credentials.
  expect((await call(request, "/api/customer/client-properties", memberA)).status).toBe(403);

  const adminA = await sessionToken(browser, fixtures.users.adminA);
  expect(await call(request, "/api/me", adminA)).toMatchObject({
    status: 200,
    body: { access: "customer", customerID: a.id, admin: true },
  });

  const memberB = await sessionToken(browser, fixtures.users.memberB);
  expect(await call(request, "/api/customer", memberB)).toMatchObject({
    status: 200,
    body: { id: b.id },
  });

  const staff = await sessionToken(browser, fixtures.users.staff);
  expect(await call(request, "/api/me", staff)).toMatchObject({
    status: 200,
    body: { access: "operator" },
  });
  const list = await call(request, "/api/operator/customers", staff);
  expect(list.status).toBe(200);
  const ids = (list.body.customers as { id: string }[]).map((c) => c.id);
  expect(ids).toEqual(expect.arrayContaining([a.id, b.id]));
  expect(await call(request, `/api/operator/customers/${a.id}`, staff)).toMatchObject({
    status: 200,
    body: { id: a.id },
  });
  // The staff organization is no customer.
  expect((await call(request, "/api/customer", staff)).status).toBe(403);
});

test("signing out returns to the sign-in form", async ({ page }) => {
  await signIn(page, fixtures.users.signOutA);
  await expect(page.getByRole("main").getByText(a.displayName)).toBeVisible();
  await page.getByRole("button", { name: /open user menu/i }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  // Signing out reloads Clerk before the form shows again.
  await expect(page.getByLabel(/email address/i)).toBeVisible({ timeout: 20_000 });
  await expect(page.getByRole("main").getByText(a.displayName)).toHaveCount(0);
});
