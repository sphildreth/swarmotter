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
  assert.equal(
    details.policySourceLabel({ kind: "intake_snapshot", profile: "linux" }),
    "intake policy fixed at registration (linux)",
  );
  assert.equal(
    details.formatTorrentIdentity(
      { kind: "hybrid", v1: "a".repeat(40), v2: "b".repeat(64) },
      "ignored",
    ),
    `hybrid — v1 ${"a".repeat(40)}; v2 ${"b".repeat(64)}`,
  );
  assert.equal(
    details.formatTorrentIdentity(undefined, "c".repeat(40)),
    `legacy v1 — ${"c".repeat(40)}`,
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
    encryption_mode: { value: "required", source: { kind: "torrent" } },
    tracker: {
      host_rules: {
        value: [{ host_pattern: "tracker.example", enabled: false, priority: "low" }],
        source: { kind: "profile", profile: "linux", origin: "add_request" },
      },
    },
    intake: {
      excluded_file_patterns: {
        value: ["*.nfo", "samples/*"],
        source: { kind: "intake_snapshot", profile: "linux" },
      },
      excluded_file_rules: {
        value: [{ suffix: ".sfv" }, { path_segment: "proof", max_size_bytes: 1024 }],
        source: { kind: "intake_snapshot", profile: "linux" },
      },
      organization_subdirectory: {
        value: "lawful/linux",
        source: { kind: "intake_snapshot", profile: "linux" },
      },
      incomplete_subdirectory: {
        value: "staging/linux",
        source: { kind: "intake_snapshot", profile: "linux" },
      },
      unwanted_file_indices: [2, 4],
      preview_until_started: true,
    },
  });
  const policyHtml = elements.get("#details-policy").innerHTML;
  assert.match(policyHtml, /storage fixed at registration/);
  assert.match(policyHtml, /initial admission decision/);
  assert.match(policyHtml, /Peer encryption/);
  assert.match(policyHtml, /required.*torrent override/);
  assert.match(policyHtml, /Tracker host policy/);
  assert.match(policyHtml, /tracker\.example: disabled, low priority/);
  assert.match(policyHtml, /intake policy fixed at registration \(linux\)/);
  assert.match(policyHtml, /Structured intake rules/);
  assert.match(policyHtml, /suffix \.sfv/);
  assert.match(policyHtml, /segment proof and ≤ 1\.0 KB/);
  assert.match(policyHtml, /Incomplete content organization/);
  assert.match(policyHtml, /staging\/linux/);
  assert.match(policyHtml, /metadata preview — select files, then use Start/);

  details.renderDetailsEncryptionSelector({
    encryption_mode: { value: "required", source: { kind: "torrent" } },
  });
  assert.equal(elements.get("#details-encryption-mode").value, "required");
  details.renderDetailsEncryptionSelector({
    encryption_mode: { value: "required", source: { kind: "profile" } },
  });
  assert.equal(elements.get("#details-encryption-mode").value, "");
} finally {
  await rm(fixtureDirectory, { recursive: true, force: true });
}
