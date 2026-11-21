// SPDX-License-Identifier: Apache-2.0
// RSS subscription controls for the Watch view.

import { api } from "./js/api.js";
import { $, escapeHtml, fmtCount, fmtUnixSeconds, renderKv, renderStatus, showError, showToast, log } from "./js/ui.js";

let editingId = null;
let selectedId = null;
let decisionsBefore = null;
let editingLabels = [];

function ruleLines(rules) {
  return [...(rules?.include || []).map(rule => ({ ...rule, include: true })),
          ...(rules?.exclude || []).map(rule => ({ ...rule, include: false }))]
    .map(rule => `${rule.include ? "+" : "-"}${rule.title || ""}|${rule.category || ""}`)
    .join("\n");
}

function parseRules(text) {
  const include = [];
  const exclude = [];
  for (const raw of String(text || "").split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    const sign = line.startsWith("-") ? "exclude" : "include";
    const body = line.startsWith("+") || line.startsWith("-") ? line.slice(1) : line;
    const [titleRaw, categoryRaw] = body.split("|", 2);
    const title = (titleRaw || "").trim();
    const category = (categoryRaw || "").trim();
    const rule = {};
    if (title) rule.title = title;
    if (category) rule.category = category;
    if (rule.title || rule.category) (sign === "include" ? include : exclude).push(rule);
  }
  return { include, exclude };
}

function actionStatus(action) {
  if (["import", "already_present", "previously_completed"].includes(action)) return "ok";
  if (["filtered", "dismissed", "baseline"].includes(action)) return "warning";
  if (action === "failed") return "error";
  return "warning";
}

function renderSubscriptionRow(sub) {
  const counts = sub.counts || {};
  return `<tr>
    <td><strong>${escapeHtml(sub.name)}</strong><div class="muted">${escapeHtml(sub.id)}</div></td>
    <td>${escapeHtml(sub.url)}</td>
    <td>${sub.enabled ? "enabled" : "paused"}<div class="muted">every ${fmtCount(sub.poll_interval_seconds)}s</div></td>
    <td>${renderStatus(sub.poll_in_progress ? "warning" : sub.baseline_established ? "ok" : "warning")}<div class="muted">${sub.poll_in_progress ? "polling" : sub.baseline_established ? "baseline ready" : "baseline pending"}</div></td>
    <td>${fmtCount(counts.imported)} imported<br>${fmtCount(counts.previously_completed)} completed${counts.failed ? `<br>${fmtCount(counts.failed)} failed` : ""}</td>
    <td>${escapeHtml(sub.last_success_at ? fmtUnixSeconds(sub.last_success_at) : "never")}${sub.latest_error ? `<div class="muted">${escapeHtml(sub.latest_error)}</div>` : ""}</td>
    <td class="rss-actions">
      <button type="button" data-rss-action="preview" data-rss-id="${escapeHtml(sub.id)}">Preview</button>
      <button type="button" data-rss-action="poll" data-rss-id="${escapeHtml(sub.id)}">Poll</button>
      <button type="button" data-rss-action="import-current" data-rss-id="${escapeHtml(sub.id)}">Import current</button>
      <button type="button" data-rss-action="edit" data-rss-id="${escapeHtml(sub.id)}">Edit</button>
      <button type="button" data-rss-action="decisions" data-rss-id="${escapeHtml(sub.id)}">Decisions</button>
      <button type="button" data-rss-action="reconsider" data-rss-id="${escapeHtml(sub.id)}">Reconsider older</button>
      <button type="button" data-rss-action="retry" data-rss-id="${escapeHtml(sub.id)}">Retry dismissed</button>
      <button type="button" class="danger" data-rss-action="delete" data-rss-id="${escapeHtml(sub.id)}">Delete</button>
    </td>
  </tr>`;
}

function renderRss(statuses) {
  const list = $("#rss-subscriptions");
  if (!list) return;
  list.innerHTML = statuses.length === 0
    ? `<p class="muted">No RSS subscriptions configured.</p>`
    : `<table><thead><tr><th>Name</th><th>Feed</th><th>Schedule</th><th>Baseline</th><th>Counts</th><th>Last success</th><th>Actions</th></tr></thead><tbody>${statuses.map(renderSubscriptionRow).join("")}</tbody></table>`;
  const template = statuses.find(sub => sub.id === editingId);
  if (template && $("#rss-form")) {
    $("#rss-id").value = template.id;
    $("#rss-id").disabled = true;
    $("#rss-name").value = template.name;
    $("#rss-url").value = "";
    $("#rss-url").placeholder = "Leave blank to keep the configured feed URL";
    $("#rss-interval").value = String(template.poll_interval_seconds);
    $("#rss-enabled").checked = !!template.enabled;
    $("#rss-profile").value = template.profile || "";
    $("#rss-start").value = template.start_behavior || "start";
    $("#rss-import-mode").value = template.import_mode || "new_only";
    $("#rss-rules").value = ruleLines(template.rules);
    editingLabels = Array.isArray(template.labels) ? template.labels : [];
    $("#rss-save-btn").textContent = "Save changes";
  }
}

function renderPreview(preview) {
  const target = $("#rss-preview");
  if (!target) return;
  target.innerHTML = `
    <h3>Preview: ${escapeHtml(preview.feed_title || preview.subscription_id)}</h3>
    <p class="muted">Preview performed no library changes and did not mark entries seen. ${preview.conditional_not_modified ? "Feed reported not modified." : ""}</p>
    ${preview.decisions.length === 0 ? `<p class="muted">No listings in this feed.</p>` : `
      <table><thead><tr><th>Listing</th><th>Action</th><th>Reason</th><th>Torrent identity</th></tr></thead>
      <tbody>${preview.decisions.map(row => `<tr>
        <td>${escapeHtml(row.title)}<div class="muted">${escapeHtml(row.entry_key)}</div></td>
        <td>${renderStatus(actionStatus(row.action))} ${escapeHtml(row.action)}</td>
        <td>${escapeHtml(row.reason)}</td>
        <td>${escapeHtml((row.torrent_keys || []).join(", "))}</td>
      </tr>`).join("")}</tbody></table>`}`;
}

function renderDecisions(page, append = false) {
  const target = $("#rss-decisions");
  if (!target) return;
  const rows = page.decisions || [];
  const body = rows.map(row => `<tr>
    <td>${escapeHtml(fmtUnixSeconds(row.decided_at))}</td>
    <td>${escapeHtml(row.title)}<div class="muted">${escapeHtml(row.entry_key)}</div></td>
    <td>${renderStatus(actionStatus(row.action))} ${escapeHtml(row.action)}</td>
    <td>${escapeHtml(row.reason)}</td>
    <td>${escapeHtml((row.torrent_keys || []).join(", "))}</td>
  </tr>`).join("");
  if (append) {
    const tbody = target.querySelector("tbody");
    if (tbody) tbody.insertAdjacentHTML("beforeend", body);
  } else {
    target.innerHTML = `<h3>Decision history</h3>
      <p class="muted">Bounded per-feed history. Verified completion identities are retained separately and are not pruned.</p>
      <table><thead><tr><th>When</th><th>Listing</th><th>Action</th><th>Reason</th><th>Torrent identity</th></tr></thead><tbody>${body}</tbody></table>`;
  }
  decisionsBefore = page.next_before || null;
  const more = target.querySelector("#rss-more-decisions");
  if (more) more.remove();
  if (decisionsBefore) {
    target.insertAdjacentHTML("beforeend", `<button id="rss-more-decisions" type="button">Load older decisions</button>`);
  }
}

export async function refreshRss() {
  try {
    const statuses = await api("/rss");
    renderRss(statuses);
    return statuses;
  } catch (error) {
    log("rss error: " + error.message);
    return [];
  }
}

function subscriptionPayload() {
  const id = $("#rss-id").value.trim();
  const url = $("#rss-url").value.trim();
  return {
    id,
    name: $("#rss-name").value.trim(),
    url,
    enabled: $("#rss-enabled").checked,
    poll_interval_seconds: Number($("#rss-interval").value || 900),
    profile: $("#rss-profile").value.trim() || null,
    start_behavior: $("#rss-start").value,
    import_mode: $("#rss-import-mode").value,
    rules: parseRules($("#rss-rules").value),
    labels: editingId ? editingLabels : [],
  };
}

export async function saveRssSubscription(event) {
  event?.preventDefault?.();
  try {
    const payload = subscriptionPayload();
    if (!payload.name || !payload.id) throw new Error("Subscription id and name are required");
    if (editingId) {
      // A blank URL leaves the configured value unchanged on the server.
      await api(`/rss/${encodeURIComponent(editingId)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      showToast("RSS subscription updated", payload.name, "success");
    } else {
      if (!payload.url) throw new Error("Feed URL is required");
      await api("/rss", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      showToast("RSS subscription created", payload.name, "success");
    }
    resetRssForm();
    await refreshRss();
  } catch (error) {
    showError("RSS save failed", error);
  }
}

export function resetRssForm() {
  editingId = null;
  editingLabels = [];
  const form = $("#rss-form");
  if (!form) return;
  form.reset();
  $("#rss-id").disabled = false;
  $("#rss-url").placeholder = "https://example.test/feed.xml";
  $("#rss-enabled").checked = true;
  $("#rss-interval").value = "900";
  $("#rss-start").value = "start";
  $("#rss-import-mode").value = "new_only";
  $("#rss-save-btn").textContent = "Add subscription";
}

async function runAction(action, id) {
  try {
    if (action === "edit") {
      editingId = id;
      const statuses = await api("/rss");
      renderRss(statuses);
      return;
    }
    if (action === "delete") {
      if (!window.confirm(`Delete RSS subscription ${id}? Durable history and verified completions are retained.`)) return;
      await api(`/rss/${encodeURIComponent(id)}`, { method: "DELETE" });
      showToast("RSS subscription deleted", id, "success");
      await refreshRss();
      return;
    }
    if (action === "preview") {
      renderPreview(await api(`/rss/${encodeURIComponent(id)}/preview`));
      return;
    }
    if (action === "poll" || action === "import-current") {
      const result = await api(`/rss/${encodeURIComponent(id)}/poll?import_current=${action === "import-current"}`, { method: "POST" });
      showToast("RSS poll finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: result.conditional_not_modified });
      await refreshRss();
      return;
    }
    if (action === "reconsider") {
      const result = await api(`/rss/${encodeURIComponent(id)}/poll?reconsider=true`, { method: "POST" });
      showToast("RSS reconsider finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: false });
      await refreshRss();
      return;
    }
    if (action === "retry") {
      const result = await api(`/rss/${encodeURIComponent(id)}/retry`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      showToast("RSS retry finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: false });
      await refreshRss();
      return;
    }
    if (action === "decisions") {
      selectedId = id;
      decisionsBefore = null;
      renderDecisions(await api(`/rss/${encodeURIComponent(id)}/decisions?limit=100`));
    }
  } catch (error) {
    showError("RSS action failed", error);
  }
}

$("#rss-subscriptions")?.addEventListener("click", event => {
  const button = event.target.closest("button[data-rss-action]");
  if (!button) return;
  runAction(button.dataset.rssAction, button.dataset.rssId);
});

$("#rss-form")?.addEventListener("submit", saveRssSubscription);
$("#rss-reset-btn")?.addEventListener("click", resetRssForm);
$("#rss-preview-btn")?.addEventListener("click", () => runAction("preview", editingId || $("#rss-id")?.value.trim()));
$("#rss-decisions")?.addEventListener("click", async event => {
  if (event.target.id !== "rss-more-decisions") return;
  if (!selectedId || !decisionsBefore) return;
  try {
    renderDecisions(await api(`/rss/${encodeURIComponent(selectedId)}/decisions?limit=100&before=${decisionsBefore}`), true);
  } catch (error) {
    showError("RSS decision history failed", error);
  }
});
