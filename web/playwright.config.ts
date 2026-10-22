import { defineConfig, devices } from "@playwright/test";

// End-to-end tests. They run against the built app served by vite preview;
// PLAYWRIGHT_BASE_URL points them at a running console instead, such as the
// one on kind.
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? "http://localhost:4173";
const ci = Boolean(process.env.CI);

export default defineConfig({
  testDir: "./e2e",
  forbidOnly: ci,
  // Flaky tests get fixed, not retried.
  retries: 0,
  reporter: ci ? [["list"], ["html", { open: "never" }]] : "list",
  use: { baseURL, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: process.env.PLAYWRIGHT_BASE_URL
    ? undefined
    : {
        // The Bun running this config, so no bun has to be on PATH.
        command: `${process.execPath} run build && ${process.execPath} run preview --port 4173 --strictPort`,
        url: baseURL,
        reuseExistingServer: !ci,
      },
});
