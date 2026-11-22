// SPDX-License-Identifier: Apache-2.0
// Exercise the embedded assets and native APIs using a disposable daemon.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { once } from "node:events";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_CORE || "playwright-core");
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const dir = await mkdtemp(join(tmpdir(), "swarmotter-web-smoke-"));
const download = join(dir, "downloads"),
  incomplete = join(dir, "incomplete");
const feed = createServer((req, res) => {
  res.setHeader("content-type", "application/rss+xml");
  res.end(
    '<rss version="2.0"><channel><title>Local fixture</title><link>http://localhost/</link><description>Local feed</description><item><guid>local-1</guid><title>Local preview listing</title><link>http://127.0.0.1/release</link></item></channel></rss>',
  );
});
const listen = async (s) => {
  s.listen(0, "127.0.0.1");
  await once(s, "listening");
  return s.address().port;
};
const unusedPort = async () => {
  const s = createServer();
  const port = await listen(s);
  await new Promise((r) => s.close(r));
  return port;
};
let daemon,
  browser,
  closed,
  trace = "",
  stdout = "";
try {
  await mkdir(download);
  await mkdir(incomplete);
  const feedPort = await listen(feed),
    apiPort = await unusedPort(),
    peerPort = await unusedPort();
  const base = "http://127.0.0.1:" + apiPort;
  const config = join(dir, "config.toml"),
    stateFile = join(dir, "state.db");
  await writeFile(
    config,
    `[api]\nbind_address="127.0.0.1:${apiPort}"\n[storage]\ndownload_dir="${download}"\nincomplete_dir="${incomplete}"\n[network]\nmode="strict"\nrequired_source_ipv4="127.0.0.1"\nfail_closed=true\n[torrent]\nlisten_port=${peerPort}\n[dht]\nenabled=false\n[logging]\nfile=true\nfile_path="${dir}/daemon.log"\n`,
  );
  const source = join(download, "local-example.txt");
  await writeFile(source, "SwarmOtter embedded UI test.\n");
  daemon = spawn(resolve(root, "target/debug/swarmotterd"), ["--config", config, "--state-file", stateFile], {
    cwd: dir,
    stdio: ["ignore", "pipe", "pipe"],
  });
  closed = once(daemon, "exit");
  daemon.stderr.on("data", (c) => (trace = (trace + c).slice(-4096)));
  daemon.stdout.on("data", (c) => (stdout = (stdout + c).slice(-4096)));
  for (let i = 0; ; i++) {
    try {
      if ((await fetch(base + "/api/v1/stats")).ok) break;
    } catch {}
    if (i > 160 || daemon.exitCode != null) throw Error("Isolated daemon did not start. " + trace + stdout);
    await delay(50);
  }
  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1672, height: 941 }, deviceScaleFactor: 1 });
  page.setDefaultTimeout(8000);
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(base + "/index.html");
  const nav = (name) => page.click('.nav[data-view="' + name + '"]');
  const fetchData = async (path) => (await (await fetch(base + "/api/v1" + path)).json()).data;
  const action = async (path, method, click) => {
    const [r] = await Promise.all([
      page.waitForResponse(
        (r) => new URL(r.url()).pathname === "/api/v1" + path && r.request().method() === method,
      ),
      click(),
    ]);
    const body = await r.json();
    assert(body.success, JSON.stringify(body));
    return body.data;
  };
  await page.waitForSelector(".tabulator-placeholder");
  await page.evaluate(() => document.fonts.ready);
  assert(await page.evaluate(() => document.fonts.check('600 13px "Workspace Inter"')));
  await nav("add");
  await page.fill("#magnet-input", "magnet:?xt=urn:btih:" + "12".repeat(20) + "&dn=Local");
  await page.click("#inspect-magnet");
  assert.equal((await fetchData("/stats")).torrent_count, 0);
  await page.click(".tabrow [data-method=create]");
  await page.fill("#create-source", source);
  await page.uncheck("#create-seed");
  const result = await action("/torrents/create", "POST", () => page.click("#create-btn"));
  assert.equal(result.seeded, false);
  await page.click(".tabrow [data-method=file]");
  await page.setInputFiles("#torrent-file", {
    name: "local-example.torrent",
    mimeType: "application/x-bittorrent",
    buffer: Buffer.from(result.metainfo_hex, "hex"),
  });
  await page.check("#file-paused");
  const hash = await action("/torrents/file", "POST", () => page.click("#add-file-btn"));
  assert.equal((await fetchData("/torrents/" + hash)).state, "paused");
  await nav("torrents");
  await page.click(".name-cell");
  await page.waitForSelector(".ti-section");
  await page.click("#torrent-inspect [data-tab=Files]");
  assert.match(await page.textContent("#torrent-inspect"), /local-example.txt/);
  await nav("network");
  await page.waitForSelector("#network-metrics strong");
  await page.click("#network-run");
  await nav("settings");
  await page.waitForFunction(() =>
    document.querySelector("#config-health").textContent.includes("Configuration loaded"),
  );
  const n = Number(await page.inputValue("#cfg-api-max-request-body-bytes")) + 1;
  await page.fill("#cfg-api-max-request-body-bytes", String(n));
  const saved = await action("/settings", "PUT", () => page.click("#save-settings-btn"));
  assert.equal(saved.config.api.max_request_body_bytes, n);
  assert.equal(saved.config.network.mode, "strict");
  assert.match(await readFile(config, "utf8"), new RegExp("max_request_body_bytes = " + n));
  await nav("watch");
  await page.click("#new-subscription");
  await page.fill("#rss-name", "Local feed");
  await page.fill("#rss-url", "http://127.0.0.1:" + feedPort + "/feed");
  await page.uncheck("#rss-enabled");
  await action("/rss/preview", "POST", () => page.click("#rss-preview-btn"));
  assert.equal((await fetchData("/rss")).length, 0);
  await action("/rss", "POST", () => page.click("#rss-save-btn"));
  assert.equal((await fetchData("/rss"))[0].enabled, false);
  await nav("logs");
  await page.waitForFunction(() => document.querySelector("#live-tag").textContent.includes("LIVE"));
  await page.click("#clear-logs");
  await action("/logs/recent", "GET", () => page.click("#refresh-logs-btn"));
  assert.equal((await fetchData("/stats")).torrent_count, 1);
  assert.deepEqual(errors, []);
  console.log(
    "Embedded Axum UI passed. Local metainfo creation, paused binary upload, inspector, strict diagnostics, TOML persistence, local RSS preview/save, logs and stream. All test state was disposable.",
  );
} finally {
  if (browser) await browser.close();
  if (daemon && daemon.exitCode == null) {
    daemon.kill("SIGTERM");
    await closed;
  }
  feed.closeAllConnections();
  await new Promise((r) => feed.close(r));
  await rm(dir, { recursive: true, force: true });
}
