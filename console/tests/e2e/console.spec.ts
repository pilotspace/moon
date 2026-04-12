import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Query Console view", () => {
  test("Monaco editor mounts on the console route", async ({ page }) => {
    await gotoView(page, "/console");
    // Monaco lazy-loads; give it generous time.
    await expect(page.locator(".monaco-editor").first()).toBeVisible({
      timeout: 15_000,
    });
    await expectNoPlaceholder(page);
  });
});
