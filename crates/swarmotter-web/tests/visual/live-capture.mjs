// SPDX-License-Identifier: Apache-2.0
// Read-only browser capture of the running daemon, with its own live API and clock.
import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_CORE || "playwright-core");
const origin = new URL(process.argv[2] || "http://127.0.0.1:9091").origin;
const evidence = fileURLToPath(new URL("../../../../design/ui/swarmotter_UI_refactor_evidence/", import.meta.url));
const out = join(evidence, "live-daemon");
const expected = JSON.parse(await readFile(join(evidence, "layout-results.json"), "utf8")).canonical;
await mkdir(join(out, "actual"), { recursive: true });
const browser = await chromium.launch({ args: ["--disable-gpu", "--disable-partial-raster", "--num-raster-threads=1"] });
try {
  const page = await browser.newPage({
    viewport: { width: 1672, height: 941 }, deviceScaleFactor: 1, locale: "en-US",
    timezoneId: "UTC", colorScheme: "dark", reducedMotion: "reduce",
  });
  const errors = [], writes = [], responses = new Set();
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  page.on("request", (request) => {
    if (!["GET", "HEAD"].includes(request.method())) writes.push(request.method() + " " + request.url());
  });
  page.on("requestfailed", (request) => errors.push(request.failure()?.errorText + " " + request.url()));
  page.on("response", (response) => {
    const url = new URL(response.url());
    if (url.origin !== origin || !response.ok()) errors.push(response.status() + " " + response.url());
    else responses.add(url.pathname);
  });
  const doc = await page.goto(origin);
  assert(doc.ok());
  assert.match(doc.headers()["content-security-policy"], /script-src 'self'/);
  await page.waitForFunction(() => /^\d+$/.test(document.querySelector("#tele-count")?.textContent));
  const panes = {};
  for (const view of ["torrents", "add", "network", "settings", "watch", "logs"]) {
    const diagnostics = view === "network" ? page.waitForResponse(origin + "/api/v1/network/diagnostics") : null;
    if (view !== "torrents") await page.locator(`.nav[data-view="${view}"]`).click();
    const root = page.locator("#view-" + view);
    assert(await root.isVisible());
    assert.equal(await page.locator(".nav.active").getAttribute("data-view"), view);
    if (view === "torrents") {
      await page.waitForSelector(".name-cell,.tabulator-placeholder");
      if (await page.locator(".name-cell").count()) {
        await page.locator(".name-cell").first().click();
        await page.waitForSelector(".ti-section");
      }
    }
    if (view === "add") {
      await page.waitForSelector("#recent-imports > *");
      await page.fill("#magnet-input", "magnet:?xt=urn:btih:7d4e4e01234567890123456789012345678929c1&dn=ubuntu-24.04.1-desktop-amd64.iso&xl=6227702579");
      await page.click("#inspect-magnet");
      await page.waitForFunction(() => document.querySelector("#import-preview").textContent.includes("ubuntu-24.04.1"));
    }
    if (view === "network") {
      const { health } = (await (await diagnostics).json()).data;
      await page.waitForSelector("#network-metrics .metric-card");
      if (health.mode === "strict" && health.status === "healthy" && health.traffic_allowed)
        assert.equal(await page.locator("#network-warning").getAttribute("data-tone"), "ok");
    }
    if (view === "settings") await page.waitForFunction(() => document.querySelector("#config-health").textContent.includes("Configuration loaded"));
    if (view === "watch") await page.waitForFunction(() => /^\d+$/.test(document.querySelector("#rail-feeds").textContent));
    if (view === "logs") {
      await page.waitForFunction(() => document.querySelector("#live-tag").textContent.includes("LIVE") && document.querySelector("#log-source").textContent.trim());
      const rows = page.locator(".log-entry");
      if (await rows.count()) await rows.first().click();
    }
    await page.evaluate(async () => {
      await document.fonts.ready;
      await Promise.all([...document.images].map((image) => image.decode()));
      await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    });
    assert(await page.evaluate(() => document.fonts.check('600 13px "Workspace Inter"')));
    const geometry = await page.locator("header,#telemetry,.workspace:not(.hidden)>.pane").evaluateAll(nodes =>
      Object.fromEntries(nodes.map(node => {
        const { x, y, width, height } = node.getBoundingClientRect();
        return [node.id || node.className || node.tagName, [x, y, width, height]];
      }))
    );
    for (const [region, box] of Object.entries(geometry)) assert.deepEqual(box, expected[view][region], view + " " + region);
    panes[view] = geometry;
    await page.screenshot({ path: join(out, "actual", view + ".png"), animations: "disabled", caret: "hide" });
  }
  assert.deepEqual(errors, []);
  assert.deepEqual(writes, []);
  for (const path of ["/workspace.css", "/js/workspace.js", "/api/v1/stats", "/api/v1/torrents/query", "/api/v1/network/diagnostics", "/api/v1/settings", "/api/v1/rss", "/api/v1/logs/recent", "/api/v1/events"]) assert(responses.has(path), path);
  const report = { origin, chromium: browser.version(), at: new Date().toISOString(), deviceScaleFactor: 1, viewport: page.viewportSize(), panes, observedPaths: [...responses].sort(), writes, errors };
  await writeFile(join(out, "browser-results.json"), JSON.stringify(report, null, 2) + "\n");
  console.log("Captured six live views with matching shell and pane bounds. No API writes, failed responses or browser errors.");
} finally {
  await browser.close();
}
