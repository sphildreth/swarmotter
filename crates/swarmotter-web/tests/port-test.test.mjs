// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const assetsDirectory = join(testDirectory, "..", "assets");
const fixtureDirectory = await mkdtemp(join(tmpdir(), "swarmotter-port-test-ui-"));

class ElementFixture {
  constructor(id = "") {
    this.id = id;
    this.classList = { contains: () => false };
    this.listeners = new Map();
    this.children = [];
    this.innerHTML = "";
    this.textContent = "";
    this.disabled = false;
    this.isConnected = true;
  }

  addEventListener(kind, handler) {
    const handlers = this.listeners.get(kind) || [];
    handlers.push(handler);
    this.listeners.set(kind, handlers);
  }
  appendChild(child) { this.children.push(child); return child; }
  remove() {}
  setAttribute() {}
  querySelector(selector) {
    return selector === "#network-port-test-btn" && this.innerHTML.includes("network-port-test-btn")
      ? new ElementFixture("network-port-test-btn")
      : null;
  }
  querySelectorAll() { return []; }
  get firstElementChild() { return this.children[0] || null; }
}

try {
  await cp(assetsDirectory, fixtureDirectory, { recursive: true });
  await writeFile(join(fixtureDirectory, "package.json"), "{\"type\":\"module\"}\n");
  const elements = new Map([
    "network-port-test",
    "watch-scan-btn",
    "refresh-logs-btn",
    "log-stream",
    "toast-region",
  ].map(id => [id, new ElementFixture(id)]));
  globalThis.window = globalThis;
  globalThis.document = {
    documentElement: new ElementFixture("document-element"),
    body: new ElementFixture("body"),
    createElement: () => new ElementFixture(),
    querySelector(selector) {
      return selector.startsWith("#") ? elements.get(selector.slice(1)) || null : null;
    },
    querySelectorAll: () => [],
  };
  globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
  globalThis.setTimeout = () => 0;
  globalThis.clearTimeout = () => {};

  const events = await import(pathToFileURL(join(fixtureDirectory, "js", "events.js")));
  assert.equal(events.portTestStateLabel({ enabled: false }), "Disabled");
  assert.equal(events.portTestStateLabel({ enabled: true, state: "open" }), "Open");
  assert.equal(events.portTestStateLabel({ enabled: true, state: "closed" }), "Closed");
  assert.equal(events.portTestTone({ enabled: true, state: "open" }), "open");
  assert.equal(events.portTestTone({ enabled: true, state: "timeout" }), "closed");
  assert.equal(events.portTestTone({ enabled: true, state: "unknown" }), "unknown");

  events.renderPortTest({
    enabled: true,
    endpoint_configured: true,
    listen_port: 51413,
    state: "open",
    checked_at: 1_700_000_000,
    cache_expires_at: 1_700_000_900,
    detail: "Endpoint accepted the listener",
  });
  const html = elements.get("network-port-test").innerHTML;
  assert.match(html, /Open/);
  assert.match(html, /Run port test/);
  assert.match(html, /Endpoint accepted the listener/);

  events.renderPortTest({ enabled: false, endpoint_configured: false, state: "unknown" });
  assert.match(elements.get("network-port-test").innerHTML, /disabled/);
} finally {
  await rm(fixtureDirectory, { recursive: true, force: true });
}
