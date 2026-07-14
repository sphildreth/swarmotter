// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const assetsDirectory = join(testDirectory, "..", "assets");
const fixtureDirectory = await mkdtemp(join(tmpdir(), "swarmotter-port-mapping-ui-"));

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
    return selector === "#network-port-mapping-refresh-btn" && this.innerHTML.includes("network-port-mapping-refresh-btn")
      ? new ElementFixture("network-port-mapping-refresh-btn")
      : null;
  }
  querySelectorAll() { return []; }
  get firstElementChild() { return this.children[0] || null; }
}

try {
  await cp(assetsDirectory, fixtureDirectory, { recursive: true });
  await writeFile(join(fixtureDirectory, "package.json"), "{\"type\":\"module\"}\n");
  const elements = new Map([
    "network-port-mapping",
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
  assert.equal(events.portMappingStateLabel({ enabled: false }), "Disabled");
  assert.equal(events.portMappingStateLabel({ enabled: true, state: "active" }), "Active");
  assert.equal(events.portMappingStateLabel({ enabled: true, state: "blocked" }), "Blocked");
  assert.equal(events.portMappingTone({ enabled: true, state: "active" }), "active");
  assert.equal(events.portMappingTone({ enabled: true, state: "unavailable" }), "unavailable");
  assert.equal(events.portMappingTone({ enabled: true, state: "pending" }), "unknown");

  events.renderPortMapping({
    enabled: true,
    protocols: ["nat_pmp", "upnp"],
    state: "active",
    active_protocol: "upnp",
    listen_port: 51413,
    external_port: 51413,
    gateway: "192.168.1.1",
    attempted_at: 1_700_000_000,
    lease_expires_at: 1_700_003_600,
    detail: "Contained UPnP lease is active",
  });
  const html = elements.get("network-port-mapping").innerHTML;
  assert.match(html, /Active/);
  assert.match(html, /nat_pmp, upnp/);
  assert.match(html, /Refresh mapping/);
  assert.match(html, /Contained UPnP lease is active/);

  events.renderPortMapping({ enabled: false, state: "disabled" });
  assert.match(elements.get("network-port-mapping").innerHTML, /disabled/);
} finally {
  await rm(fixtureDirectory, { recursive: true, force: true });
}
