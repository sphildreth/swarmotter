// SPDX-License-Identifier: Apache-2.0

"use strict";

const assert = require("node:assert/strict");
const historyUi = require("../assets/watch-history.js");

assert.equal(historyUi.outcomeLabel({ outcome: "imported", success: true }), "imported");
assert.equal(historyUi.statusKey({ outcome: "imported", success: true }), "ok");
assert.equal(historyUi.outcomeLabel({ outcome: "duplicate", duplicate: true }), "duplicate");
assert.match(historyUi.detail({ outcome: "duplicate", success: true }), /Existing torrent retained/);

const transient = {
  outcome: "transient_failure",
  success: false,
  error: "state persistence failed",
  post_action_error: "archive destination exists",
};
assert.equal(historyUi.outcomeLabel(transient), "transient failure");
assert.equal(historyUi.statusKey(transient), "warning");
assert.match(historyUi.detail(transient), /state persistence failed/);
assert.match(historyUi.detail(transient), /Post action: archive destination exists/);
assert.equal(
  historyUi.statusKey({
    outcome: "imported",
    success: true,
    post_action_error: "archive destination exists",
  }),
  "warning",
);
assert.equal(
  historyUi.outcomeLabel({
    outcome: "imported",
    success: true,
    post_action_error: "archive destination exists",
  }),
  "imported",
);

assert.equal(
  historyUi.statusKey({ outcome: "permanent_failure", success: false }),
  "invalid",
);
assert.equal(historyUi.outcome({ success: true, duplicate: true }), "duplicate");
assert.equal(historyUi.outcome({ success: true }), "imported");
assert.equal(historyUi.detail({ success: false, error: "legacy failure" }), "legacy failure");
