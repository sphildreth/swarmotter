// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const assetsDirectory = join(testDirectory, "..", "assets");
const fixtureDirectory = await mkdtemp(join(tmpdir(), "swarmotter-peer-filter-ui-"));

class ElementFixture {
  constructor(id = "") {
    this.id = id;
    this.classList = { contains: () => false };
    this.dataset = {};
    this.listeners = new Map();
    this.children = [];
    this.innerHTML = "";
    this.textContent = "";
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.parentElement = null;
    this.unbanButtons = [];
  }

  addEventListener(kind, handler) {
    const handlers = this.listeners.get(kind) || [];
    handlers.push(handler);
    this.listeners.set(kind, handlers);
  }

  async dispatch(kind) {
    // DOM dispatch snapshots listeners; a re-render during a click must not
    // invoke a newly registered listener for that same click.
    for (const handler of [...(this.listeners.get(kind) || [])]) {
      await handler({ currentTarget: this, target: this, preventDefault() {} });
    }
  }

  appendChild(child) {
    child.parentElement = this;
    this.children.push(child);
    return child;
  }
  querySelectorAll(selector) {
    return selector === "[data-peer-filter-unban]"
      && this.innerHTML.includes("data-peer-filter-unban")
      ? this.unbanButtons
      : [];
  }
  querySelector() { return null; }
  closest() { return null; }
  setAttribute() {}
  remove() {
    if (!this.parentElement) return;
    const index = this.parentElement.children.indexOf(this);
    if (index >= 0) this.parentElement.children.splice(index, 1);
    this.parentElement = null;
  }
  get firstElementChild() { return this.children[0] || null; }
}

function documentFixture() {
  const ids = [
    "settings-editor",
    "reload-settings-btn",
    "reset-downloads-btn",
    "add-watch-folder-btn",
    "settings-watch-list",
    "add-storage-root-control-btn",
    "settings-storage-root-controls",
    "save-toast-btn",
    "toast-seconds",
    "toast-region",
    "log-stream",
    "peer-filter-status",
    "peer-filter-manual-bans",
  ];
  const elements = new Map(ids.map(id => [id, new ElementFixture(id)]));
  return {
    elements,
    document: {
      documentElement: new ElementFixture("document-element"),
      body: new ElementFixture("body"),
      createElement: () => new ElementFixture(),
      querySelector(selector) {
        if (selector.startsWith("#")) return elements.get(selector.slice(1)) || null;
        return null;
      },
      querySelectorAll() { return []; },
    },
  };
}

try {
  await cp(assetsDirectory, fixtureDirectory, { recursive: true });
  await writeFile(join(fixtureDirectory, "package.json"), "{\"type\":\"module\"}\n");
  const { document, elements } = documentFixture();
  const requests = [];
  const unbanButton = new ElementFixture("unban-button");
  unbanButton.dataset.peerFilterIp = "203.0.113.7";
  elements.get("peer-filter-manual-bans").unbanButtons = [unbanButton];

  globalThis.window = globalThis;
  globalThis.document = document;
  globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
  globalThis.confirm = () => true;
  // Let toast cleanup run after the timer value is assigned. The element
  // fixture implements removal so repeated feedback cannot spin in
  // showToast's visible-toast cap loop.
  globalThis.setTimeout = callback => {
    queueMicrotask(callback);
    return 0;
  };
  globalThis.clearTimeout = () => {};
  globalThis.fetch = async input => {
    const path = new URL(String(input), "http://127.0.0.1").pathname;
    requests.push(path);
    assert.equal(path, "/api/v1/peer-filter/unban");
    return {
      status: 200,
      text: async () => JSON.stringify({
        success: true,
        data: {
          enabled: true,
          rules: ["198.51.100.0/24"],
          configured_rule_count: 1,
          imported_rule_count: 2,
          manual_bans: [],
          blocked_client_ids: ["-BAD"],
          sources: [{ path: "/var/lib/swarmotter/peerguard.dat", rules_loaded: 2, skipped_lines: 1 }],
          rejections: {
            ip_checks: 4,
            client_id_checks: 2,
            manual_bans: 1,
            configured_rules: 1,
            imported_rules: 1,
            client_ids: 1,
            fail_closed: 0,
          },
        },
      }),
    };
  };

  const settings = await import(pathToFileURL(join(fixtureDirectory, "js", "settings.js")));
  const { state } = await import(pathToFileURL(join(fixtureDirectory, "js", "state.js")));
  state.fullConfigSnapshot = {
    peer_filter: {
      enabled: true,
      manual_bans: [{ ip: "203.0.113.7", reason: "operator review" }],
    },
  };
  const active = {
    enabled: true,
    rules: ["198.51.100.0/24"],
    configured_rule_count: 1,
    imported_rule_count: 2,
    manual_bans: [{ ip: "203.0.113.7", reason: "operator review" }],
    blocked_client_ids: ["-BAD"],
    sources: [{ path: "/var/lib/swarmotter/peerguard.dat", rules_loaded: 2, skipped_lines: 1 }],
    rejections: {
      ip_checks: 4,
      client_id_checks: 2,
      manual_bans: 1,
      configured_rules: 1,
      imported_rules: 1,
      client_ids: 1,
      fail_closed: 0,
    },
  };

  settings.renderPeerFilterStatus(active);
  assert.match(elements.get("peer-filter-status").innerHTML, /Live policy status/);
  assert.match(elements.get("peer-filter-status").innerHTML, /peerguard\.dat/);
  assert.match(elements.get("peer-filter-status").innerHTML, /198\.51\.100\.0\/24/);
  assert.match(elements.get("peer-filter-manual-bans").innerHTML, /operator review/);
  assert.match(elements.get("peer-filter-manual-bans").innerHTML, /Unban/);

  await unbanButton.dispatch("click");
  assert.deepEqual(requests, ["/api/v1/peer-filter/unban"]);
  assert.deepEqual(state.fullConfigSnapshot.peer_filter.manual_bans, []);
  assert.match(elements.get("peer-filter-manual-bans").innerHTML, /No global manual bans/);
} finally {
  await rm(fixtureDirectory, { recursive: true, force: true });
}
