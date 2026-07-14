// SPDX-License-Identifier: Apache-2.0

(function expose(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.SwarmOtterWatchHistory = api;
}(typeof globalThis === "object" ? globalThis : this, function buildModule() {
  "use strict";

  const STABLE_OUTCOMES = new Set([
    "imported",
    "duplicate",
    "permanent_failure",
    "transient_failure",
  ]);

  function outcome(item) {
    if (STABLE_OUTCOMES.has(item?.outcome)) return item.outcome;
    if (item?.duplicate === true) return "duplicate";
    if (item?.success === true) return "imported";
    return "failure";
  }

  function outcomeLabel(item) {
    return outcome(item).replace(/_/g, " ");
  }

  function statusKey(item) {
    if (item?.post_action_error) return "warning";
    switch (outcome(item)) {
      case "imported":
      case "duplicate":
        return "ok";
      case "transient_failure":
        return "warning";
      case "permanent_failure":
      default:
        return item?.success === true ? "ok" : "invalid";
    }
  }

  function detail(item) {
    const parts = [];
    if (item?.error) parts.push(item.error);
    if (item?.post_action_error) parts.push(`Post action: ${item.post_action_error}`);
    if (parts.length > 0) return parts.join(" · ");
    if (outcome(item) === "duplicate") return "Existing torrent retained; success action applied.";
    return item?.success === true ? "ok" : "fail";
  }

  return { outcome, outcomeLabel, statusKey, detail };
}));
