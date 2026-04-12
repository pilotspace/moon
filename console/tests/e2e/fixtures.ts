import { test as base, expect, type Page } from "@playwright/test";

/** Asserts the page does not render leftover dev placeholders. */
export async function expectNoPlaceholder(page: Page): Promise<void> {
  const body = page.locator("body");
  await expect(body).not.toContainText(/placeholder/i);
  await expect(body).not.toContainText(/\bTODO\b/);
  await expect(body).not.toContainText(/lorem ipsum/i);
}

/** Navigate to a /ui/<path> route and wait for the page to settle. */
export async function gotoView(page: Page, path: string): Promise<void> {
  await page.goto(path, { waitUntil: "domcontentloaded" });
  // Give lazy-loaded routes (Console, Vectors, Graph, Memory) time to hydrate.
  // networkidle can fail on long-lived SSE connections; fall through on timeout.
  await page
    .waitForLoadState("networkidle", { timeout: 10_000 })
    .catch(() => undefined);
}

export const test = base.extend<Record<string, never>>({});
export { expect };
