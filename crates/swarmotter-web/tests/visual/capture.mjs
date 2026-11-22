// SPDX-License-Identifier: Apache-2.0
import { createRequire } from "node:module";
import { createServer } from "node:http";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve, extname } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyInteractions, verifyLayouts } from "./verification.mjs";
import { library, query, health, stats } from "./library.mjs";
import { settings, feeds, preview, network, watch, logs } from "./operations.mjs";
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_CORE || "playwright-core");
const here = dirname(fileURLToPath(import.meta.url));
const assets = resolve(here, "../../assets");
const evidence = resolve(process.env.UI_EVIDENCE_DIR || resolve(here, "../../../../design/ui/swarmotter_UI_refactor_evidence"));
let sample = 0;
const requests = [],
  streams = new Set();
const faults = new Map();
let currentSettings = structuredClone(settings);
const localFeeds = structuredClone(feeds);
function mutate(path, method, body) {
  if (path.startsWith("/torrents/bulk/"))
    return {
      succeeded: body.info_hashes.slice(0, 1),
      failed: body.info_hashes
        .slice(1)
        .map((info_hash) => ({ info_hash, message: "Fixture action failure" })),
      not_found: [],
    };
  if (path === "/torrents/remove") return { removed: body.info_hashes, not_found: [] };
  if (["/torrents/magnet", "/torrents/file"].includes(path)) return library[0].info_hash;
  if (path === "/torrents/create")
    return {
      name: "Local example",
      format: body.format,
      metainfo_hex: "6465",
      metainfo_bytes: 2,
      seeded: false,
    };
  if (path === "/settings" && method === "PUT") {
    currentSettings = body;
    return {
      config: body,
      restart_required: true,
      restart_required_fields: ["api.max_request_body_bytes"],
      config_path: "fixture.toml",
    };
  }
  if (path === "/network/port-test") return { enabled: true, endpoint_configured: true, state: "closed" };
  if (path === "/watch/scan") return {};
  if (path === "/trash/preview") return { items: [] };
  if (path === "/rss/preview") return preview(body.id);
  if (path === "/rss" && method === "POST") {
    localFeeds.push(body);
    return body;
  }
  const id = path.split("/")[2];
  if (path.endsWith("/poll")) return { imported: [], ...preview(id) };
  if (path.startsWith("/rss/")) {
    const index = localFeeds.findIndex((sub) => sub.id === id);
    if (index < 0) throw Error("Unknown subscription");
    if (method === "DELETE") {
      localFeeds.splice(index, 1);
      return {};
    }
    if (method === "PUT") {
      const prev = localFeeds[index];
      return (localFeeds[index] = { ...prev, ...body, url: body.url || prev.url });
    }
  }
  throw Error("Unexpected mutation " + method + " " + path);
}
function response(path, q) {
  if (path === "/torrents/query") return query(q);
  if (path === "/stats") return stats(sample++);
  if (path === "/doctor") return { level: "ok", summary: "Local fixture", checks: [] };
  if (path === "/network/health")
    return {
      ...health,
      port_test: { enabled: true, endpoint_configured: true, state: "unknown" },
      port_mapping: { enabled: false },
    };
  if (path === "/network/diagnostics") return network;
  if (path === "/settings") return currentSettings;
  if (path === "/peer-filter") return { enabled: false, manual_bans: [] };
  if (path === "/watch/status") return watch;
  if (path === "/rss") return localFeeds;
  if (path.endsWith("/decisions")) return { decisions: [], next_before: null };
  if (/^\/rss\/[^/]+\/preview$/.test(path)) return preview(path.split("/")[2]);
  if (path === "/trash") return { enabled: false, entries: [], rules: [] };
  if (path === "/logs/recent") return logs;
  if (path === "/profiles") return settings.profiles;
  if (path === "/torrents/create/roots") return ["/fixture"];
  const item = library.find((t) => t.info_hash === path.split("/")[2]);
  if (/^\/torrents\/[0-9a-f]+$/.test(path) && item) return item;
  if (path.endsWith("/stats") && item) return { ...item, seeders: 72, leechers: 14 };
  if (path.endsWith("/files")) return [{ path: item.name, length: item.total_length }];
  if (path.endsWith("/trackers"))
    return [
      "https://tracker.example/announce",
      "https://backup.example/announce",
      "udp://tracker.example:80/announce",
    ].map((url, i) => ({
      url,
      status: i < 2 ? "ok" : "not_contacted",
      last_success: i < 2 ? 1795298000 - i * 60 : null,
    }));
  throw Error("Missing fixture " + path);
}
const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname.startsWith("/api/v1/")) {
      const path = url.pathname.slice(7);
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const raw = Buffer.concat(chunks);
      const body =
        req.headers["content-type"]?.includes("application/json") && raw.length ? JSON.parse(raw) : null;
      requests.push({
        path,
        method: req.method,
        query: Object.fromEntries(url.searchParams),
        body,
        bytes: raw.length,
      });
      const fault = faults.get(path);
      if (fault) {
        res.writeHead(fault, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            success: false,
            error: { code: "fixture_error", message: "Injected request failure" },
          }),
        );
        return;
      }
      if (path === "/events") {
        streams.add(res);
        res.on("close", () => streams.delete(res));
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write(": connected\n\n");
        return;
      }
      const data = req.method === "GET" ? response(path, url.searchParams) : mutate(path, req.method, body);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ success: true, data }));
      return;
    }
    let path = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
    if (path === "swarmotter-icon-64x64.png") path = "brandmark.png";
    const file = resolve(assets, path);
    if (!file.startsWith(assets + "/")) throw Error("Invalid asset path");
    const bytes = await readFile(file);
    res.setHeader(
      "content-type",
      {
        ".html": "text/html",
        ".css": "text/css",
        ".js": "text/javascript",
        ".png": "image/png",
        ".woff2": "font/woff2",
      }[extname(file)] || "application/octet-stream",
    );
    res.end(bytes);
  } catch (e) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ success: false, error: { message: e.message } }));
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const browser = await chromium.launch({ args: ["--disable-gpu","--disable-partial-raster","--num-raster-threads=1"] });
try {
  const page = await browser.newPage({
    viewport: { width: 1672, height: 941 },
    deviceScaleFactor: 1,
    locale: "en-US",
    timezoneId: "UTC",
    colorScheme: "dark",
    reducedMotion: "reduce",
  });
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.clock.install({ time: new Date("2026-11-21T21:40:12Z") });
  await page.clock.pauseAt(new Date("2026-11-21T21:40:13Z"));
  await page.addInitScript(() => {
    localStorage.setItem(
      "swarmotter.torrentQueryView",
      JSON.stringify({ sort: "queue", dir: "asc", per_page: 200 }),
    );
    localStorage.setItem(
      "swarmotter.namedQueryViews",
      JSON.stringify([
        { name: "Media (4K)", query: { q: "4K" } },
        { name: "Linux ISOs", query: { label: "linux" } },
        { name: "Music (Lossless)", query: { label: "music" } },
        { name: "Active (High Speed)", query: { performance: "transferring" } },
      ]),
    );
  });
  const fixtureOrigin = "http://127.0.0.1:" + server.address().port;
  const origin = process.env.UI_DAEMON_ORIGIN || fixtureOrigin;
  if (process.env.UI_DAEMON_ORIGIN) {
    // Chromium loads the document, embedded assets and production CSP from the
    // daemon. All API traffic, including the streaming response and mutations
    // exercised later, goes to the existing in-memory test handler.
    await page.route("**/api/v1/**", (route) => {
      const url = new URL(route.request().url());
      return route.continue({ url: fixtureOrigin + url.pathname + url.search });
    });
  }
  await page.goto(origin);
  await page.waitForSelector(".name-cell");
  for (let i = 0; i < 17; i++) {
    await page.clock.setSystemTime(new Date(Date.UTC(2026, 10, 21, 21, 40, 34) + i * 55000));
    await page.evaluate(async () => (await import("/js/workspace.js")).refreshTelemetry());
  }
  await page.waitForFunction(() => document.querySelector("#tele-size").textContent.includes("TiB"));
  await page.locator(".torrent-tools summary").click();
  const repaged = page.waitForResponse(
    (r) => r.url().includes("/torrents/query") && r.url().includes("per_page=12"),
  );
  await page.fill("#torrent-per-page", "12");
  await page.locator("#torrent-per-page").press("Tab");
  await page.clock.runFor(400);
  await repaged;
  await page.locator(".torrent-tools summary").click();
  await page.locator(".name-cell").filter({ hasText: "The.Matrix" }).click();
  await page.locator('input[aria-label^="Select The.Matrix"]').check();
  await page.waitForSelector(".ti-section");
  await page.waitForFunction(() =>
    Array.from(document.querySelectorAll("#saved-links small")).every((n) => /^\d+$/.test(n.textContent)),
  );
  const layout = {};
  const routes = (process.env.UI_VIEWS || "torrents,add,network,settings,watch,logs").split(",");
  const out = resolve(evidence, "actual");
  await mkdir(out, { recursive: true });
  for (const route of routes) {
    if (route !== "torrents") await page.locator(".nav[data-view=" + route + "]").click();
    if (route === "add") {
      await page.waitForSelector("#recent-imports button");
      await page.fill(
        "#magnet-input",
        "magnet:?xt=urn:btih:7d4e4e01234567890123456789012345678929c1&dn=ubuntu-24.04.1-desktop-amd64.iso&xl=6227702579&tr=https%3A%2F%2Ftracker.example%2Fannounce",
      );
      await page.fill("#magnet-dir", "/downloads/linux");
      await page.selectOption("#magnet-profile", "Linux ISOs");
      await page.fill("#magnet-labels", "linux, open-source, iso");
      await page.click("#inspect-magnet");
    }
    if (route === "network") await page.waitForSelector("#network-metrics .metric-card");
    if (route === "settings")
      await page.waitForFunction(() =>
        document.querySelector("#config-health").textContent.includes("Configuration loaded"),
      );
    if (route === "watch") {
      await page.waitForSelector(".feed-row");
      await page.click("#feed-preview-action");
      await page.waitForSelector(".preview-row");
    }
    if (route === "logs") {
      await page.selectOption("#log-time", "24");
      await page.waitForSelector(".log-entry .WARN");
      await page.locator(".log-entry").filter({ hasText: "Feed request exceeded" }).click();
      await page.waitForFunction(() => document.querySelector("#live-tag").textContent.includes("LIVE"));
    }
    await page.evaluate(async () => {
      await document.fonts.ready;
      await Promise.all([...document.images].map((i) => i.decode()));
    });
    await page.clock.runFor(64);
    layout[route] = await page
      .locator(
        "header,.nav.active,#telemetry,.workspace:not(.hidden)>.pane,.workspace:not(.hidden) .page-head,.workspace:not(.hidden) .table-foot,.workspace:not(.hidden) .ins-footer,.workspace:not(.hidden) .torrent-heading",
      )
      .evaluateAll((nodes) =>
        Object.fromEntries(
          nodes.map((n) => {
            const r = n.getBoundingClientRect();
            return [
              n.id || n.className || n.tagName,
              [r.x, r.y, r.width, r.height].map((v) => Math.round(v * 10) / 10),
            ];
          }),
        ),
      );
    if (process.env.UI_MEASURE)
      console.log(
        await page
          .locator(
            ".tabulator-col,.workspace:not(.hidden)>.pane,.torrent-tools,#search,#stats-summary,.piece-chart,.torrent-heading",
          )
          .evaluateAll((nodes) =>
            nodes.map((n) => [
              n.className,
              n.id,
              n.offsetWidth,
              n.offsetHeight,
              Math.round(n.getBoundingClientRect().x),
              Math.round(n.getBoundingClientRect().y),
            ]),
          ),
      );
    await page.screenshot({ path: resolve(out, route + ".png"), animations: "disabled", caret: "hide" });
  }
  if (routes.length === 6) {
    const secondary = resolve(evidence, "secondary");
    await mkdir(secondary, { recursive: true });
    const layouts = await verifyLayouts(page, routes, secondary);
    await writeFile(
      resolve(evidence, "layout-results.json"),
      JSON.stringify({ canonical: layout, responsive: layouts }, null, 2) + "\n",
    );
    const checks = await verifyInteractions(page, { requests, faults, streams, library }).catch(async (e) => {
      console.error(requests.slice(-8).map((r) => [r.method, r.path, r.query]));
      console.error(
        await page
          .locator(".workspace:not(.hidden) .center")
          .innerText()
          .then((t) => t.slice(0, 1000)),
      );
      throw e;
    });
    await writeFile(
      resolve(evidence, "browser-results.json"),
      JSON.stringify({ chromium: browser.version(), checks }, null, 2) + "\n",
    );
  }
  if (errors.length) throw Error(errors.join("\n"));
  console.log("Captured " + routes.join(", ") + " in Chromium " + browser.version());
} finally {
  await browser.close();
  server.closeAllConnections();
  server.close();
}
