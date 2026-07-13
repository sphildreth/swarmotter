// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const assetsDirectory = join(testDirectory, "..", "assets");
const fixtureDirectory = await mkdtemp(join(tmpdir(), "swarmotter-web-startup-"));

class ClassList {
  constructor(tokens = []) {
    this.tokens = new Set(tokens);
  }

  add(...tokens) { tokens.forEach(token => this.tokens.add(token)); }
  remove(...tokens) { tokens.forEach(token => this.tokens.delete(token)); }
  contains(token) { return this.tokens.has(token); }
  toggle(token, force) {
    const enabled = force === undefined ? !this.contains(token) : Boolean(force);
    if (enabled) this.add(token);
    else this.remove(token);
    return enabled;
  }
}

class ElementFixture {
  constructor(id = "", classes = []) {
    this.id = id;
    this.classList = new ClassList(classes);
    this.dataset = {};
    this.attributes = new Map();
    this.listeners = new Map();
    this.children = [];
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.textContent = "";
    this.innerHTML = "";
    this.title = "";
    this.files = [];
  }

  addEventListener(kind, handler) {
    const handlers = this.listeners.get(kind) || [];
    handlers.push(handler);
    this.listeners.set(kind, handlers);
  }

  appendChild(child) { this.children.push(child); return child; }
  remove() {}
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  removeAttribute(name) { this.attributes.delete(name); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  closest() { return null; }
  focus() {}
  showModal() {}
  get firstElementChild() { return this.children[0] || null; }
}

function documentFixture(indexHtml) {
  const elements = new Map();
  const tags = indexHtml.matchAll(/<[^>]*\bid="([^"]+)"[^>]*>/g);
  for (const match of tags) {
    const tag = match[0];
    const id = match[1];
    const classes = tag.match(/\bclass="([^"]*)"/)?.[1].split(/\s+/).filter(Boolean) || [];
    const element = new ElementFixture(id, classes);
    element.value = tag.match(/\bvalue="([^"]*)"/)?.[1] || "";
    element.checked = /\schecked(?:\s|>|=)/.test(tag);
    element.disabled = /\sdisabled(?:\s|>|=)/.test(tag);
    elements.set(id, element);
  }

  const selectorElements = new Map();
  const document = {
    documentElement: new ElementFixture("document-element"),
    body: new ElementFixture("body"),
    addEventListener() {},
    createElement: () => new ElementFixture(),
    getElementById: id => elements.get(id) || null,
    querySelector(selector) {
      if (/^#[A-Za-z0-9_-]+$/.test(selector)) return elements.get(selector.slice(1)) || null;
      if (!selectorElements.has(selector)) selectorElements.set(selector, new ElementFixture(selector));
      return selectorElements.get(selector);
    },
    querySelectorAll(selector) {
      if (selector.startsWith(".")) {
        const className = selector.slice(1);
        return Array.from(elements.values()).filter(element => element.classList.contains(className));
      }
      return [];
    },
  };
  return { document, elements };
}

class TabulatorFixture {
  static instances = [];

  constructor(selector, options) {
    this.selector = selector;
    this.options = options;
    this.rows = [];
    this.sorters = [{ field: "name", dir: "asc" }];
    TabulatorFixture.instances.push(this);
  }

  on(kind, handler) {
    if (kind === "tableBuilt") queueMicrotask(handler);
  }

  replaceData(rows) { this.rows = rows; return Promise.resolve(); }
  getRows() { return []; }
  getSorters() { return this.sorters; }
  setSort(sorters) { this.sorters = sorters.map(sorter => ({ field: sorter.column, dir: sorter.dir })); }
  getHeaderFilters() { return []; }
  clearFilter() {}
  redraw() {}
}

const apiCalls = [];
const unhandled = [];
const originalConsoleLog = console.log;
const recordUnhandledRejection = error => unhandled.push(error);
process.on("unhandledRejection", recordUnhandledRejection);

try {
  await cp(assetsDirectory, fixtureDirectory, { recursive: true });
  await writeFile(join(fixtureDirectory, "package.json"), "{\"type\":\"module\"}\n");
  const indexHtml = await readFile(join(assetsDirectory, "index.html"), "utf8");
  const { document, elements } = documentFixture(indexHtml);
  assert.ok(elements.has("torrent-table"), "startup fixture must use the production index markup");

  const storage = new Map();
  globalThis.window = globalThis;
  globalThis.document = document;
  globalThis.localStorage = {
    getItem: key => storage.get(key) ?? null,
    setItem: (key, value) => storage.set(key, String(value)),
    removeItem: key => storage.delete(key),
  };
  globalThis.confirm = () => false;
  globalThis.prompt = () => null;
  globalThis.requestAnimationFrame = callback => { queueMicrotask(callback); return 1; };
  globalThis.setInterval = () => 1;
  globalThis.clearInterval = () => {};
  globalThis.Tabulator = TabulatorFixture;
  console.log = () => {};

  globalThis.fetch = async input => {
    const path = new URL(String(input), "http://127.0.0.1").pathname;
    apiCalls.push(path);
    let data;
    if (path === "/api/v1/torrents/query") {
      data = {
        rows: [{
          info_hash: "0123456789abcdef0123456789abcdef01234567",
          name: "Local test fixture",
          state: "paused",
          total_length: 1024,
          bytes_completed: 0,
          rate_down: 0,
          rate_up: 0,
          ratio: 0,
          active_peer_workers: 0,
          known_peers: 0,
          health: { label: "paused", score: 100, bars: 5, reasons: [] },
        }], total: 1, filtered: 1, page: 1, page_count: 1,
        per_page: 200, sort: "name", dir: "asc",
      };
    } else if (path === "/api/v1/stats") {
      data = { torrent_count: 1, download_rate: 0, upload_rate: 0 };
    } else if (path === "/api/v1/doctor") {
      data = { level: "ok", summary: "startup fixture healthy", checks: [] };
    } else {
      throw new Error(`unexpected startup API request: ${path}`);
    }
    return new Response(JSON.stringify({ success: true, data }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  await import(pathToFileURL(join(fixtureDirectory, "watch-history.js")));
  await import(pathToFileURL(join(fixtureDirectory, "seeding-policy.js")));
  await import(pathToFileURL(join(fixtureDirectory, "app.js")));

  for (let attempt = 0; attempt < 20 && apiCalls.length < 3; attempt++) {
    await new Promise(resolve => setImmediate(resolve));
  }
  await new Promise(resolve => setImmediate(resolve));

  const { state } = await import(pathToFileURL(join(fixtureDirectory, "js", "state.js")));
  const settings = await import(pathToFileURL(join(fixtureDirectory, "js", "settings.js")));
  settings.renderSettingsEditor({
    port_mapping: {
      enabled: true,
      protocols: ["upnp"],
      nat_pmp_gateway: "192.168.1.1",
      upnp_service_url: "http://192.168.1.1:49000/control",
      lease_seconds: 7200,
      refresh_before_expiry_seconds: 600,
    },
  });
  const renderedMapping = settings.collectSettingsConfig().port_mapping;
  assert.deepEqual(renderedMapping, {
    enabled: true,
    protocols: ["upnp"],
    nat_pmp_gateway: "192.168.1.1",
    upnp_service_url: "http://192.168.1.1:49000/control",
    lease_seconds: 7200,
    refresh_before_expiry_seconds: 600,
  }, "a full Settings save must retain port-mapping configuration");
  process.off("unhandledRejection", recordUnhandledRejection);
  assert.deepEqual(apiCalls.sort(), [
    "/api/v1/doctor",
    "/api/v1/stats",
    "/api/v1/torrents/query",
  ]);
  assert.equal(unhandled.length, 0, unhandled.map(error => error?.stack || error).join("\n"));
  assert.equal(state.torrentTableBuilt, true, "initial torrent table must finish building");
  assert.equal(TabulatorFixture.instances.length, 1, "startup must build one torrent table");
  assert.equal(TabulatorFixture.instances[0].rows.length, 1, "initial torrent row must render");
  assert.equal(elements.get("health-badge").textContent, "OK");
  assert.match(elements.get("stats-summary").textContent, /1 torrents/);
} finally {
  process.off("unhandledRejection", recordUnhandledRejection);
  console.log = originalConsoleLog;
  await rm(fixtureDirectory, { recursive: true, force: true });
}
