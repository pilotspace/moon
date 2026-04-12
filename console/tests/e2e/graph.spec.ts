import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Graph Explorer view", () => {
  test("mounts a canvas or empty-state", async ({ page }) => {
    await gotoView(page, "/graph");
    const canvas = page.locator("canvas").first();
    // Actual empty-state copy from GraphExplorer.tsx: "No graph data".
    const emptyState = page.getByText(/no graph( data)?|empty/i).first();
    await expect(canvas.or(emptyState)).toBeVisible({ timeout: 15_000 });
    await expectNoPlaceholder(page);
  });
});
