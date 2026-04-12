import { test as base, expect, type Page } from "@playwright/test";

/** Asserts the page does not render leftover dev placeholders. */
export async function expectNoPlaceholder(page: Page): Promise<void> {
  const body = page.locator("body");
  await expect(body).not.toContainText(/placeholder/i);
  await expect(body).not.toContainText(/\bTODO\b/);
  await expect(body).not.toContainText(/lorem ipsum/i);
}

/**
 * Navigate to a baseURL-relative path and wait for the page to settle.
 *
 * NOTE: The Playwright `baseURL` is `http://localhost:9100/ui/` (see
 * `playwright.config.ts`). A leading `/` makes Playwright treat the path as
 * absolute and strips the `/ui/` prefix, which is exactly the UX-03 bug.
 * We defensively normalize by trimming any leading slashes so call sites
 * written in either style both work.
 */
export async function gotoView(page: Page, path: string): Promise<void> {
  // Strip leading slashes so baseURL ("http://host/ui/") is honored.
  // Empty string -> baseURL itself, which is the `/` integration-test case.
  const relative = path.replace(/^\/+/, "");
  await page.goto(relative, { waitUntil: "domcontentloaded" });
  // Give lazy-loaded routes (Console, Vectors, Graph, Memory) time to hydrate.
  // networkidle can fail on long-lived SSE connections; fall through on timeout.
  await page
    .waitForLoadState("networkidle", { timeout: 10_000 })
    .catch(() => undefined);
}

export const test = base.extend<Record<string, never>>({});
export { expect };
