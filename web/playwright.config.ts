import { defineConfig, devices, type Project } from "@playwright/test";

// End-to-end tests. They run against the built app served by vite preview;
// PLAYWRIGHT_BASE_URL points them at a running console instead, such as the
// one on kind. With a Clerk secret key too (make ui-e2e), the signed-in tests
// run as well: a setup project creates this run's Clerk test users,
// organizations and customers, and a teardown project removes them.
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? "http://localhost:4173";
const ci = Boolean(process.env.CI);
const signedIn = Boolean(process.env.PLAYWRIGHT_BASE_URL && process.env.CLERK_SECRET_KEY);
const chrome = { ...devices["Desktop Chrome"] };

const projects: Project[] = [{ name: "console", testMatch: "console.spec.ts", use: chrome }];
if (signedIn) {
  projects.push(
    { name: "clerk setup", testMatch: "clerk/clerk.setup.ts", teardown: "clerk teardown" },
    { name: "clerk teardown", testMatch: "clerk/clerk.teardown.ts" },
    { name: "signed in", testMatch: "clerk/*.spec.ts", dependencies: ["clerk setup"], use: chrome },
  );
}

export default defineConfig({
  testDir: "./e2e",
  forbidOnly: ci,
  // Flaky tests get fixed, not retried.
  retries: 0,
  reporter: ci ? [["list"], ["html", { open: "never" }]] : "list",
  use: { baseURL, trace: "retain-on-failure" },
  projects,
  webServer: process.env.PLAYWRIGHT_BASE_URL
    ? undefined
    : {
        // The Bun running this config, so no bun has to be on PATH.
        command: `${process.execPath} run build && ${process.execPath} run preview --port 4173 --strictPort`,
        url: baseURL,
        reuseExistingServer: !ci,
      },
});
