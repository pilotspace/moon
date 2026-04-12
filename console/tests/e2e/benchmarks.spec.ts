import { test, expect, gotoView } from "./fixtures";

/** INT-04: enforce UMAP and force-layout perf budgets. */

test.setTimeout(60_000);

test("50K-point UMAP projects within 10s", async ({ page }) => {
  await gotoView(page, "/vectors");
  await page.evaluate(() => performance.mark("umap:start"));
  // Trigger projection via UI button labelled "Project" or "Run UMAP".
  // Fallback: wait for console's "umap:done" PerformanceEntry emitted by worker.
  await page.waitForFunction(
    () =>
      performance
        .getEntriesByName("umap:done", "mark")
        .length > 0,
    null,
    { timeout: 15_000 },
  );
  const ms = await page.evaluate(() => {
    performance.mark("umap:end");
    performance.measure("umap:total", "umap:start", "umap:done");
    return performance.getEntriesByName("umap:total", "measure")[0].duration;
  });
  expect(ms).toBeLessThan(10_000);
});

test("10K-node force layout stabilises within 15s", async ({ page }) => {
  await gotoView(page, "/graph");
  await page.evaluate(() => performance.mark("graph:start"));
  await page.waitForFunction(
    () => performance.getEntriesByName("graph:stable", "mark").length > 0,
    null,
    { timeout: 20_000 },
  );
  const ms = await page.evaluate(() => {
    performance.measure("graph:total", "graph:start", "graph:stable");
    return performance.getEntriesByName("graph:total", "measure")[0].duration;
  });
  expect(ms).toBeLessThan(15_000);
});
