// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { join } from "node:path";

const navigate = (page, route) => page.locator(`.nav[data-view="${route}"]`).click();
const waitText = (page, selector, value) =>
  page.waitForFunction(([s, v]) => document.querySelector(s)?.textContent.includes(v), [selector, value], {
    polling: 40,
  });
const hit = (page, path, method, action) =>
  Promise.all([
    page.waitForResponse(
      (r) => new URL(r.url()).pathname === "/api/v1" + path && r.request().method() === method,
    ),
    action(),
  ]);

export async function verifyLayouts(page, routes, secondary) {
  const result = [];
  for (const [width, height] of [
    [1280, 800],
    [390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    for (const route of routes) {
      await navigate(page, route);
      const root = page.locator("#view-" + route);
      assert(await root.isVisible());
      if (route === "torrents") {
        await page.clock.runFor(80);
        await page.waitForSelector("#torrent-table .name-cell", { state: "visible" });
        const row = await page.locator(".name-cell").first().boundingBox();
        const holder = await page.locator("#torrent-table .tabulator-tableholder").boundingBox();
        assert(
          row && holder && row.y + row.height > holder.y && row.y < holder.y + holder.height,
          "First torrent is outside the visible table",
        );
        const search = await page.locator("#search").boundingBox();
        const add = await page.locator("#go-add").boundingBox();
        assert(search.y + search.height <= add.y, "Search overlaps the torrent toolbar");
      }
      assert.equal(await page.locator(`.nav[data-view="${route}"]`).getAttribute("aria-current"), "page");
      const dimensions = await page.evaluate(() => ({
        body: document.documentElement.scrollWidth,
        viewport: innerWidth,
      }));
      assert(dimensions.body <= width, `${route} at ${width}px has document width ${dimensions.body}`);
      if (route === "torrents")
        await page.screenshot({
          path: join(secondary, `torrents-${width}x${height}.png`),
          animations: "disabled",
          caret: "hide",
        });
      if (width < 720) {
        for (const [button, pane] of [
          ["sections", "rail"],
          ["inspector", "inspector"],
        ]) {
          const toggle = page.locator(`[data-reveal="${button}"]`);
          await toggle.click();
          assert(await root.locator("." + pane).isVisible(), `${route} ${pane} unreachable`);
          if (route === "add" && pane === "inspector")
            await page.screenshot({
              path: join(secondary, "add-inspector-390x844.png"),
              animations: "disabled",
              caret: "hide",
            });
          await page.keyboard.press("Escape");
          assert.equal(await toggle.getAttribute("aria-expanded"), "false");
          assert(await toggle.evaluate((b) => b === document.activeElement));
        }
      } else assert(await root.locator(".inspector").isVisible());
      result.push({ view: route, width, height, documentWidth: dimensions.body, inspectorReachable: true });
    }
  }
  await page.setViewportSize({ width: 1672, height: 941 });
  return result;
}

export async function verifyInteractions(page, { requests, faults, streams, library }) {
  const checks = [];
  const pass = (name) => {
    checks.push(name);
    console.log("Passed " + name);
  };
  const calls = (path, method) => requests.filter((r) => r.path === path && (!method || r.method === method));
  const writes = () => requests.filter((r) => r.method !== "GET");
  const click = (id) => page.locator(id).click();
  await page.clock.resume();
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  assert.equal(
    writes().length,
    0,
    "Navigating, inspecting URI, previewing saved feed and responsive checks must only read.",
  );
  assert.equal(streams.size, 1);
  pass("Read-only navigation on all six views and a single authenticated-fetch event stream");

  await navigate(page, "torrents");
  await page.fill("#search", "Dune");
  await page.waitForFunction(() => document.querySelectorAll(".name-cell").length === 1);
  assert.equal(await page.locator(".name-cell").innerText(), library[1].name);
  await waitText(page, "#torrent-inspect", library[0].name);
  page.once("dialog", (dialog) => dialog.accept("Dune fixture"));
  await click("#name-view");
  await waitText(page, "#saved-links", "Dune fixture");
  await page.locator("#saved-links button").filter({ hasText: "Dune fixture" }).click();
  assert.equal(await page.inputValue("#search"), "Dune");
  await page.fill("#search", "");
  await page.waitForFunction(() => document.querySelectorAll(".name-cell").length === 12);
  await click('#smart-filters [data-state="paused"]');
  await page.waitForFunction(() => document.querySelectorAll(".name-cell").length === 8);
  await click('#smart-filters [data-state=""]');
  await page.waitForSelector(".name-cell[data-inspect='" + library[0].info_hash + "']");
  await hit(page, "/torrents/query", "GET", () =>
    click(".tabulator-col[tabulator-field=name] .tabulator-col-title"),
  );
  await click(".torrent-tools summary");
  await hit(page, "/torrents/query", "GET", () => page.selectOption("#library-order", "queue"));
  await click(".torrent-tools summary");
  await page.locator('input[aria-label^="Select The.Matrix"]').check();
  await page.locator('input[aria-label^="Select Dune"]').check();
  await page.locator(".name-cell").filter({ hasText: "Silo" }).click();
  await waitText(page, "#torrent-inspect", "Silo.");
  for (const [id, path] of [
    ["#pause-selected-torrents-btn", "pause"],
    ["#resume-selected-torrents-btn", "resume"],
    ["#recheck-selected-torrents-btn", "recheck"],
  ]) {
    await hit(page, "/torrents/bulk/" + path, "POST", () => click(id));
    assert.deepEqual(calls("/torrents/bulk/" + path).at(-1).body.info_hashes, [
      library[0].info_hash,
      library[1].info_hash,
    ]);
    await waitText(page, "#toast-region", "Fixture action failure");
  }
  page.once("dialog", (d) => d.accept());
  await hit(page, "/torrents/remove", "POST", () => click("#remove-selected-torrents-btn"));
  assert.equal(calls("/torrents/remove").at(-1).body.delete_data, false);
  pass(
    "Library search, state filter, server sort, independent hash selection, partial lifecycle failure and keep-data removal",
  );

  await navigate(page, "add");
  await page.fill("#magnet-input", "bad link");
  await click("#commit-import");
  await waitText(page, "#toast-region", "Add magnet failed");
  assert.equal(calls("/torrents/magnet").length, 0);
  const magnet = "magnet:?xt=urn:btih:" + library[0].info_hash + "&dn=Local";
  await page.fill("#magnet-input", magnet);
  await click("#inspect-magnet");
  await waitText(page, "#import-preview", "Local");
  await click(".tabrow [data-method=file]");
  await click(".tabrow [data-method=magnet]");
  assert.equal(await page.inputValue("#magnet-input"), magnet);
  await page.check("#magnet-start");
  await page.check("#magnet-paused");
  assert(!(await page.isChecked("#magnet-start")));
  faults.set("/torrents/magnet", 400);
  await hit(page, "/torrents/magnet", "POST", () => click("#commit-import"));
  await waitText(page, "#toast-region", "Injected request failure");
  assert.equal(await page.inputValue("#magnet-input"), magnet);
  faults.delete("/torrents/magnet");
  const beforeAdd = calls("/torrents/magnet").length;
  await hit(page, "/torrents/magnet", "POST", () =>
    page.evaluate(() => {
      document.querySelector("#commit-import").click();
      document.querySelector("#add-magnet-btn").click();
    }),
  );
  await page.waitForFunction(() => document.querySelector("#magnet-input").value === "");
  assert.equal(calls("/torrents/magnet").length, beforeAdd + 1);
  const addBody = calls("/torrents/magnet").at(-1).body;
  assert.equal(addBody.paused, true);
  assert.equal(addBody.profile, "Linux ISOs");
  assert.deepEqual(addBody.labels, ["linux", "open-source", "iso"]);
  await click(".tabrow [data-method=file]");
  await page.setInputFiles("#torrent-file", {
    name: "local.torrent",
    mimeType: "application/x-bittorrent",
    buffer: Buffer.from("de"),
  });
  await hit(page, "/torrents/file", "POST", () => click("#commit-import"));
  assert.equal(calls("/torrents/file").at(-1).bytes, 2);
  await click(".tabrow [data-method=create]");
  await page.fill("#create-source", "/fixture/local");
  await hit(page, "/torrents/create", "POST", () => click("#commit-import"));
  await page.waitForSelector("#create-download-btn");
  pass(
    "Magnet validation, side-effect-free inspection, selection of start policy, retry, double submission, binary upload and creation request",
  );

  await navigate(page, "network");
  await page.waitForSelector("#network-metrics strong");
  faults.set("/network/diagnostics", 503);
  await hit(page, "/network/diagnostics", "GET", () => click("#network-refresh"));
  await waitText(page, "#net-indicator", "Unavailable");
  assert.equal(await page.locator("#network-warning").getAttribute("data-tone"), "unknown");
  faults.delete("/network/diagnostics");
  await hit(page, "/network/diagnostics", "GET", () => click("#network-run"));
  await waitText(page, "#network-checks", "Containment state");
  assert.equal(await page.locator("#network-warning").getAttribute("data-tone"), "warn");
  await click("button[data-monitor=reach]");
  await hit(page, "/network/port-test", "POST", () => click("#network-port-test-btn"));
  await waitText(page, "#network-port-test", "Closed");
  faults.set("/network/health", 503);
  await page.evaluate(async () => (await import("/js/workspace.js")).refreshTelemetry());
  assert.equal(await page.textContent("#tele-network"), "Unavailable");
  faults.delete("/network/health");
  await page.evaluate(async () => (await import("/js/workspace.js")).refreshTelemetry());
  assert.equal(await page.textContent("#tele-mode"), "Disabled");
  pass(
    "Network diagnostics error and recovery, configured port test, health fetch failure and explicit disabled containment",
  );

  await navigate(page, "settings");
  await waitText(page, "#config-health", "Configuration loaded");
  const field = "#cfg-api-max-request-body-bytes";
  await page.fill(field, "0");
  await click("#save-settings-btn");
  assert.equal(calls("/settings", "PUT").length, 0);
  await page.fill(field, "17000000");
  await waitText(page, "#pending-fields", "unsaved");
  page.once("dialog", (d) => d.dismiss());
  await click("#reload-settings-btn");
  assert.equal(await page.inputValue(field), "17000000");
  faults.set("/settings", 400);
  await hit(page, "/settings", "PUT", () => click("#save-settings-btn"));
  await waitText(page, "#settings-save-status", "Injected request failure");
  assert.equal(await page.inputValue(field), "17000000");
  faults.delete("/settings");
  await hit(page, "/settings", "PUT", () => click("#save-settings-btn"));
  await waitText(page, "#pending-fields", "No pending changes");
  assert.match(await page.textContent("#config-impact"), /Restart neededYes/);
  assert.equal(calls("/settings", "PUT").at(-1).body.rss.subscriptions.length, 3);
  await page.fill(field, "18000000");
  page.once("dialog", (d) => d.accept());
  await click("#reload-settings-btn");
  await page.waitForFunction(
    () => document.querySelector("#cfg-api-max-request-body-bytes").value === "17000000",
  );
  await click('[data-settings-target="watch"]');
  await page.waitForSelector('[data-watch-field="path"]');
  pass(
    "Configuration field validation, confirmed reload, preserved failed edits, persisted save response, restart and retained RSS configuration",
  );

  await navigate(page, "watch");
  await click('[data-feed="fedora"]');
  await waitText(page, "#feed-inspector", "Fedora Project");
  const before = writes().length;
  await hit(page, "/rss/fedora/preview", "GET", () => click("#preview-subscription"));
  assert.equal(writes().length, before);
  await click("#edit-subscription");
  await page.uncheck("#rss-enabled");
  await hit(page, "/rss/fedora", "PUT", () => click("#rss-save-btn"));
  await waitText(page, "#feed-inspector", "Paused");
  await click("#new-subscription");
  await page.fill("#rss-name", "Local test");
  await page.fill("#rss-url", "http://fixture.invalid/feed");
  await hit(page, "/rss/preview", "POST", () => click("#rss-preview-btn"));
  await page.waitForSelector("#rss-draft-preview .preview-row");
  assert.equal(calls("/rss", "POST").length, 0);
  await hit(page, "/rss", "POST", () => click("#rss-save-btn"));
  await waitText(page, "#feed-inspector", "Local test");
  await click(".feed-row[data-active=true] summary");
  await hit(page, "/rss/local-test/poll", "POST", () =>
    click(".feed-row[data-active=true] [data-rss-action=poll]"),
  );
  await click("#watch-scan-btn");
  await waitText(page, "#watch-scan-result", "import results recorded");
  await click("[data-watch-pane=trash].rail-link");
  await hit(page, "/trash/preview", "POST", () => click("#trash-preview-btn"));
  assert.equal(calls("/trash/apply").length, 0);
  pass(
    "RSS inspection by ID, saved and draft decision previews, create/edit/pause, explicit poll, watch scan and cleanup preview",
  );

  await navigate(page, "logs");
  const selectedColor = () => page.locator('.log-entry[aria-pressed="true"]').evaluate(node => getComputedStyle(node).backgroundColor);
  const iconColor = () => page.locator(".event-summary .glyph").evaluate(node => getComputedStyle(node).color);
  await page.locator(".log-entry").filter({ has: page.locator(".level.INFO") }).first().click();
  const infoRow = await selectedColor();
  const infoIcon = await iconColor();
  await click("[data-log-filter=WARN]");
  assert.equal(await page.locator(".log-entry").count(), 1);
  await click(".log-entry");
  assert.notEqual(await selectedColor(), infoRow, "Selected INFO and WARN rows must remain distinguishable");
  assert.notEqual(await iconColor(), infoIcon, "Event inspector must convey the selected severity");
  await click("#copy-event");
  assert.match(await page.evaluate(() => navigator.clipboard.readText()), /Feed request exceeded/);
  const file = page.waitForEvent("download");
  await click("#export-logs");
  assert.equal((await file).suggestedFilename(), "swarmotter-filtered.log");
  await click("[data-log-filter=all]");
  await page.fill("#log-search", "does-not-match");
  assert.equal(await page.locator(".log-entry").count(), 0);
  await page.fill("#log-search", "");
  const emit = (i) => {
    for (const res of streams)
      res.write(
        "event: torrent_changed\r\ndata: " +
          JSON.stringify({
            kind: "torrent_changed",
            info_hash: library[0].info_hash,
            payload: { sequence: i },
          }) +
          "\r\n\r\n",
      );
  };
  await click("#pause-logs");
  const frozen = await page.locator(".log-entry").count();
  emit(1);
  await click("#pause-logs");
  await page.waitForFunction((n) => document.querySelectorAll(".log-entry").length === n + 1, frozen);
  for (let i = 2; i < 510; i++) emit(i);
  await page.waitForFunction(() => document.querySelectorAll(".log-entry").length === 500);
  for (const res of streams) res.end();
  await waitText(page, "#live-tag", "Disconnected");
  await page.clock.fastForward(11000);
  await waitText(page, "#live-tag", "LIVE");
  assert.equal(streams.size, 1);
  const prior = writes().length;
  await click("#clear-logs");
  assert.equal(await page.locator(".log-entry").count(), 0);
  await hit(page, "/logs/recent", "GET", () => click("#refresh-logs-btn"));
  assert.equal(await page.locator(".log-entry").count(), 13);
  assert.equal(writes().length, prior);
  pass(
    "Log search, severity filter, copy, filtered export, pause, CRLF stream, bounded buffer, reconnect and local clear with tail recovery",
  );
  await click("#theme-toggle");
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  assert.equal(await page.evaluate(() => getComputedStyle(document.body).color), "rgb(32, 37, 47)");
  await click("#theme-toggle");
  pass("Persisted light and dark theme control");
  await page.clock.pauseAt(new Date(await page.evaluate(() => Date.now() + 100)));
  await page.evaluate(async () => (await import("/js/workspace.js")).refreshTelemetry());
  const statsCalls = calls("/stats").length;
  await hit(page, "/stats", "GET", () => page.clock.fastForward(5000));
  assert.equal(calls("/stats").length, statsCalls + 1);
  pass("Repeated navigation leaves one periodic global statistics request per sampling interval");
  return checks;
}
