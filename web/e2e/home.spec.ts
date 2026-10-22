import { expect, test } from "@playwright/test";

test("the home page names the product", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle("Changeport");
  await expect(page.getByRole("heading", { level: 1, name: "Changeport" })).toBeVisible();
});
