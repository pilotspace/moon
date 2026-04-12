import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Vector Explorer view", () => {
  test("mounts a canvas or empty-state", async ({ page }) => {
    await gotoView(page, "/vectors");
    const canvas = page.locator("canvas").first();
    // Actual empty-state copy from VectorExplorer.tsx/IndexMetadataPanel.tsx.
    const emptyState = page
      .getByText(/no (index|indexes|vectors)/i)
      .first();
    await expect(canvas.or(emptyState)).toBeVisible({ timeout: 15_000 });
    await expectNoPlaceholder(page);
  });
});
