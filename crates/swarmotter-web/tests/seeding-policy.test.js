// SPDX-License-Identifier: Apache-2.0

"use strict";

const assert = require("node:assert/strict");
const policyUi = require("../assets/seeding-policy.js");

function documentFixture() {
  const ids = [
    "details-seeding-ratio",
    "details-seeding-uploaded",
    "details-seeding-status",
    "details-seeding-stored-ratio",
    "details-seeding-effective-ratio",
    "details-seeding-stored-idle",
    "details-seeding-effective-idle",
    "details-seeding-forever",
    "details-ratio-inherit",
    "details-ratio-limit",
    "details-idle-inherit",
    "details-idle-limit",
    "details-seed-forever",
    "details-seeding-error",
  ];
  const fields = Object.fromEntries(ids.map(id => [id, {
    id,
    textContent: "",
    value: "",
    checked: false,
    disabled: false,
  }]));
  return { fields, getElementById: id => fields[id] || null };
}

async function main() {
  const document = documentFixture();
  const torrent = {
    ratio: 1.25,
    uploaded: 4096,
    seeding_status: "stopped_idle",
    seeding: { ratio_limit: null, idle_limit: 0, seed_forever: false },
    effective_ratio_limit: 2.0,
    effective_idle_limit: 0,
  };
  policyUi.render(document, torrent);
  assert.equal(document.fields["details-seeding-status"].textContent, "stopped idle");
  assert.equal(document.fields["details-seeding-stored-ratio"].textContent, "inherit");
  assert.equal(document.fields["details-seeding-effective-ratio"].textContent, "2");
  assert.equal(document.fields["details-seeding-stored-idle"].textContent, "0 seconds");
  assert.equal(document.fields["details-seeding-effective-idle"].textContent, "0 seconds");
  assert.equal(document.fields["details-ratio-inherit"].checked, true);
  assert.equal(document.fields["details-idle-inherit"].checked, false);
  assert.equal(document.fields["details-idle-limit"].value, "0");

  document.fields["details-ratio-inherit"].checked = false;
  document.fields["details-ratio-limit"].value = "0";
  document.fields["details-idle-inherit"].checked = true;
  document.fields["details-seed-forever"].checked = true;
  let requestRecord = null;
  await policyUi.save(document, "abc123", async (path, options) => {
    requestRecord = { path, options };
    return { ok: true };
  });
  assert.equal(requestRecord.path, "/torrents/abc123/seeding");
  assert.equal(requestRecord.options.method, "PUT");
  assert.deepEqual(JSON.parse(requestRecord.options.body), {
    ratio_limit: 0,
    idle_limit: null,
    seed_forever: true,
  });

  const renderedBeforeRejection = {
    status: document.fields["details-seeding-status"].textContent,
    storedRatio: document.fields["details-seeding-stored-ratio"].textContent,
    effectiveRatio: document.fields["details-seeding-effective-ratio"].textContent,
  };
  await assert.rejects(
    policyUi.save(document, "abc123", async () => {
      const error = new Error("ratio_limit rejected by server");
      error.code = "invalid_argument";
      throw error;
    }),
    /ratio_limit rejected by server/,
  );
  assert.deepEqual({
    status: document.fields["details-seeding-status"].textContent,
    storedRatio: document.fields["details-seeding-stored-ratio"].textContent,
    effectiveRatio: document.fields["details-seeding-effective-ratio"].textContent,
  }, renderedBeforeRejection);
  assert.equal(document.fields["details-seeding-error"].textContent, "ratio_limit rejected by server");
}

main().catch(error => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
