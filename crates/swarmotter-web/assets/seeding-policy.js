// SPDX-License-Identifier: Apache-2.0

(function expose(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.SwarmOtterSeedingPolicy = api;
}(typeof globalThis === "object" ? globalThis : this, function buildModule() {
  "use strict";

  function field(document, id) {
    const value = document.getElementById(id);
    if (!value) throw new Error(`missing seeding policy field ${id}`);
    return value;
  }

  function target(value, suffix = "") {
    return value === null || value === undefined ? "none" : `${value}${suffix}`;
  }

  function render(document, torrent) {
    const policy = torrent.seeding || {};
    const ratioInherit = policy.ratio_limit === null || policy.ratio_limit === undefined;
    const idleInherit = policy.idle_limit === null || policy.idle_limit === undefined;
    field(document, "details-seeding-ratio").textContent = String(torrent.ratio ?? 0);
    field(document, "details-seeding-uploaded").textContent = String(torrent.uploaded ?? 0);
    field(document, "details-seeding-status").textContent = String(torrent.seeding_status || "not_eligible").replace(/_/g, " ");
    field(document, "details-seeding-stored-ratio").textContent = ratioInherit ? "inherit" : String(policy.ratio_limit);
    field(document, "details-seeding-effective-ratio").textContent = target(torrent.effective_ratio_limit);
    field(document, "details-seeding-stored-idle").textContent = idleInherit ? "inherit" : target(policy.idle_limit, " seconds");
    field(document, "details-seeding-effective-idle").textContent = target(torrent.effective_idle_limit, " seconds");
    field(document, "details-seeding-forever").textContent = policy.seed_forever ? "yes" : "no";

    field(document, "details-ratio-inherit").checked = ratioInherit;
    field(document, "details-ratio-limit").value = ratioInherit ? "" : String(policy.ratio_limit);
    field(document, "details-ratio-limit").disabled = ratioInherit;
    field(document, "details-idle-inherit").checked = idleInherit;
    field(document, "details-idle-limit").value = idleInherit ? "" : String(policy.idle_limit);
    field(document, "details-idle-limit").disabled = idleInherit;
    field(document, "details-seed-forever").checked = Boolean(policy.seed_forever);
    field(document, "details-seeding-error").textContent = "";
  }

  function syncInheritance(document) {
    field(document, "details-ratio-limit").disabled = field(document, "details-ratio-inherit").checked;
    field(document, "details-idle-limit").disabled = field(document, "details-idle-inherit").checked;
  }

  function payload(document) {
    const ratioInherit = field(document, "details-ratio-inherit").checked;
    const idleInherit = field(document, "details-idle-inherit").checked;
    const ratioText = field(document, "details-ratio-limit").value.trim();
    const idleText = field(document, "details-idle-limit").value.trim();
    const ratio = ratioInherit ? null : Number(ratioText);
    const idle = idleInherit ? null : Number(idleText);
    if (!ratioInherit && (ratioText === "" || !Number.isFinite(ratio) || ratio < 0)) {
      throw new Error("Ratio limit must be a finite non-negative number, or choose inherit.");
    }
    if (!idleInherit && (idleText === "" || !Number.isSafeInteger(idle) || idle < 0)) {
      throw new Error("Idle limit must be a non-negative integer number of seconds, or choose inherit.");
    }
    return {
      ratio_limit: ratio,
      idle_limit: idle,
      seed_forever: field(document, "details-seed-forever").checked,
    };
  }

  async function save(document, hash, request) {
    const errorPanel = field(document, "details-seeding-error");
    errorPanel.textContent = "";
    try {
      const body = payload(document);
      return await request(`/torrents/${hash}/seeding`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    } catch (error) {
      errorPanel.textContent = error?.message || "Server rejected the seeding policy.";
      throw error;
    }
  }

  return { render, syncInheritance, payload, save };
}));
