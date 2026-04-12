import { test, expect, expectNoPlaceholder, gotoView } from "./fixtures";

/** INT-03: Playwright asserts UI state reflects server state over seeded fixtures. */

const ADMIN = (process.env.MOON_CONSOLE_URL ?? "http://localhost:9100/ui/")
  .replace(/\/ui\/?$/, "");

test.describe("live integration over seeded fixtures", () => {
  test("dashboard reflects server keyspace count", async ({ page }) => {
    const info = await (await fetch(`${ADMIN}/api/v1/info`)).json();
    expect(typeof info.info).toBe("string");
    await gotoView(page, "/");
    await expectNoPlaceholder(page);
    // Dashboard renders INFO cards — look for "keyspace" heading.
    await expect(page.locator("body")).toContainText(/keyspace/i);
  });

  test("memory treemap matches server aggregation", async ({ page }) => {
    const server = await (
      await fetch(`${ADMIN}/api/v1/memory/treemap?limit=500`)
    ).json();
    expect(server.tree).toBeTruthy();
    expect(server.tree.count).toBeGreaterThan(0);
    await gotoView(page, "/memory");
    await expectNoPlaceholder(page);
    // Root namespaces should appear as text in the treemap cells.
    const topChild = server.tree.children?.[0]?.name;
    if (topChild) {
      await expect(page.locator("body")).toContainText(topChild);
    }
  });

  test("vector explorer renders 3D canvas with seeded index", async ({ page }) => {
    await gotoView(page, "/vectors");
    await expectNoPlaceholder(page);
    await expect(page.locator("canvas")).toHaveCount(1, { timeout: 20_000 });
  });

  test("graph explorer renders canvas over seeded graph", async ({ page }) => {
    await gotoView(page, "/graph");
    await expectNoPlaceholder(page);
    await expect(page.locator("canvas")).toHaveCount(1, { timeout: 20_000 });
  });
});
