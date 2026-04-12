import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Vector Explorer view", () => {
  test("mounts a canvas or empty-state", async ({ page }) => {
    await gotoView(page, "vectors");
    const canvas = page.locator("canvas").first();
    // Actual empty-state copy from VectorExplorer.tsx/IndexMetadataPanel.tsx.
    // Also accept loading/error states for headless CI (no WebGL).
    // Accept: canvas (WebGL available), empty-state text, or the index
    // selector (page loaded but no projection triggered yet).
    const fallback = page
      .getByText(/no vector indexes|no indexes|select index|loading/i)
      .first();
    await expect(canvas.or(fallback).first()).toBeVisible({ timeout: 15_000 });
    await expectNoPlaceholder(page);
  });
});
