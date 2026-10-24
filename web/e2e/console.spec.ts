import { expect, test, type Page } from "@playwright/test";

// Against the preview build there is no console behind /api, so these tests
// answer /api/config themselves; against a real console
// (PLAYWRIGHT_BASE_URL) they check what it really serves.
const realConsole = Boolean(process.env.PLAYWRIGHT_BASE_URL);

/** Collects the Content-Security-Policy violations the page reports. */
function cspViolations(page: Page): string[] {
  const violations: string[] = [];
  page.on("console", (message) => {
    if (message.text().includes("Content Security Policy")) {
      violations.push(message.text());
    }
  });
  return violations;
}

test("an unconfigured console says so instead of offering to sign in", async ({
  page,
  request,
}) => {
  if (realConsole) {
    const config = await request.get("/api/config");
    test.skip(config.status() !== 503, "this console is configured");
  } else {
    await page.route("/api/config", (route) =>
      route.fulfill({
        status: 503,
        contentType: "application/problem+json",
        body: JSON.stringify({ status: 503, title: "The console is not configured" }),
      }),
    );
  }
  await page.goto("/");
  await expect(page).toHaveTitle("Changeport");
  await expect(page.getByRole("heading", { name: "The console is not configured" })).toBeVisible();
});

test("a configured console offers Clerk's sign-in", async ({ page, request }) => {
  test.skip(!realConsole, "needs a real console and Clerk instance");
  const config = await request.get("/api/config");
  test.skip(config.status() !== 200, "this console is not configured");
  const violations = cspViolations(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Changeport", exact: true })).toBeVisible();
  // Clerk's sign-in form, loaded from the instance's Frontend API.
  await expect(page.getByLabel(/email address/i)).toBeVisible({ timeout: 20_000 });
  // Clerk's bot protection reaches out after the form shows.
  await page.waitForLoadState("networkidle");
  await page.getByLabel(/email address/i).fill("someone@example.com");
  await page.waitForLoadState("networkidle");
  expect(violations).toEqual([]);
});

test("a console that cannot reach Clerk says so", async ({ page, request }) => {
  test.skip(!realConsole, "needs a real console and Clerk instance");
  const config = await request.get("/api/config");
  test.skip(config.status() !== 200, "this console is not configured");
  // Everything but the console itself fails, as on a broken connection.
  await page.route(
    (url) => url.hostname !== "localhost",
    (route) => route.abort("internetdisconnected"),
  );
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sign-in could not load" })).toBeVisible({
    timeout: 30_000,
  });
});
