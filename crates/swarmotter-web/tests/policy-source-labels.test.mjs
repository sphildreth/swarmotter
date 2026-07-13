// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const assetsDirectory = join(testDirectory, "..", "assets");
const fixtureDirectory = await mkdtemp(join(tmpdir(), "swarmotter-policy-source-ui-"));

class ElementFixture {
  constructor(id = "") {
    this.id = id;
    this.classList = { add() {}, remove() {}, contains: () => false };
    this.dataset = {};
    this.listeners = new Map();
    this.children = [];
    this.innerHTML = "";
    this.textContent = "";
    this.value = "";
    this.checked = false;
    this.disabled = false;
  }

  addEventListener(kind, handler) {
    const handlers = this.listeners.get(kind) || [];
    handlers.push(handler);
    this.listeners.set(kind, handlers);
  }

  appendChild(child) {
    this.children.push(child);
    return child;
  }
}

try {
  await cp(assetsDirectory, fixtureDirectory, { recursive: true });
  await writeFile(join(fixtureDirectory, "package.json"), "{\"type\":\"module\"}\n");
  const elements = new Map();
  const elementFor = selector => {
    if (!elements.has(selector)) elements.set(selector, new ElementFixture(selector));
    return elements.get(selector);
  };

  globalThis.window = globalThis;
  globalThis.document = {
    documentElement: new ElementFixture("document-element"),
    body: new ElementFixture("body"),
    createElement: () => new ElementFixture(),
    querySelector: elementFor,
    querySelectorAll: () => [],
  };
  globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
  globalThis.confirm = () => false;
  globalThis.prompt = () => null;
  globalThis.setTimeout = () => 0;
  globalThis.clearTimeout = () => {};

  const details = await import(pathToFileURL(join(fixtureDirectory, "js", "details.js")));

  assert.equal(
    details.policySourceLabel({ kind: "registration_storage_snapshot" }),
    "storage fixed at registration",
  );
  assert.equal(
    details.policySourceLabel({ kind: "initial_admission_snapshot" }),
    "initial admission decision",
  );

  details.renderDetailsPolicy({
    profile: null,
    download_dir: {
      value: "/srv/releases",
      source: { kind: "registration_storage_snapshot" },
    },
    incomplete_dir: {
      value: null,
      source: { kind: "registration_storage_snapshot" },
    },
    queue_priority: { value: "normal", source: { kind: "global" } },
    start_behavior: {
      value: "paused",
      source: { kind: "initial_admission_snapshot" },
    },
    ratio_limit: { value: null, source: { kind: "global" } },
    idle_limit: { value: null, source: { kind: "global" } },
    seed_forever: { value: false, source: { kind: "global" } },
    download_limit: { value: 0, source: { kind: "global" } },
    upload_limit: { value: 0, source: { kind: "global" } },
  });
  const policyHtml = elements.get("#details-policy").innerHTML;
  assert.match(policyHtml, /storage fixed at registration/);
  assert.match(policyHtml, /initial admission decision/);
} finally {
  await rm(fixtureDirectory, { recursive: true, force: true });
}
