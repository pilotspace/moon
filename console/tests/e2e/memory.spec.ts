import { test, expect, gotoView, expectNoPlaceholder } from "./fixtures";

test.describe("Memory view", () => {
  test("renders treemap content or empty-state", async ({ page }) => {
    await gotoView(page, "memory");
    // Treemap rects OR slowlog table OR empty-state message.
    // Actual empty-state copy from MemoryTreemap.tsx: "No keys found".
    const rect = page.locator("svg rect").first();
    const tableOrEmpty = page
      .getByText(/slowlog|no (data|keys)|command stats|loading/i)
      .first();
    await expect(rect.or(tableOrEmpty).first()).toBeVisible({ timeout: 10_000 });
    await expectNoPlaceholder(page);
  });
});
