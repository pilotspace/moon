import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("KV Browser view", () => {
  test("loads and shows the namespace tree header", async ({ page }) => {
    await gotoView(page, "/browser");
    await expect(page.getByText("Namespaces")).toBeVisible({ timeout: 10_000 });
    await expect(page.getByText("All Keys")).toBeVisible();
    await expectNoPlaceholder(page);
  });
});
