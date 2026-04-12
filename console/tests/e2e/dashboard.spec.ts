import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Dashboard view", () => {
  test("loads and renders without placeholder text", async ({ page }) => {
    await gotoView(page, "/dashboard");
    await expect(page).toHaveTitle(/moon/i);
    await expectNoPlaceholder(page);
  });

  test("renders dashboard content (chart or INFO card)", async ({ page }) => {
    await gotoView(page, "/dashboard");
    const chart = page.locator(".recharts-wrapper, .recharts-surface").first();
    const card = page.getByText(/server|memory|clients|keyspace/i).first();
    // At least one of these must be visible within 10s.
    await expect(chart.or(card)).toBeVisible({ timeout: 10_000 });
  });
});
