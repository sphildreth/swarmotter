// SPDX-License-Identifier: Apache-2.0

import { api } from "./api.js";
import { $, escapeHtml, fmtBytes, fmtCount, fmtUnixSeconds, renderKv, renderStatus, showError, showToast } from "./ui.js";

let lastSelection = null;

function selectionRequest() {
  const hashes = String($("#trash-hashes").value || "")
    .split(/[\s,]+/)
    .map(value => value.trim())
    .filter(Boolean);
  const rule = $("#trash-rule").value || null;
  return {
    hashes,
    rule,
    mode: $("#trash-mode").value,
  };
}

function renderRules(status) {
  const target = $("#trash-rules");
  if (!target) return;
  const rules = status.retention || [];
  const select = $("#trash-rule");
  const previous = select.value;
  select.innerHTML = `<option value="">No retention rule (explicit hashes)</option>${rules
    .map(rule => `<option value="${escapeHtml(rule.name)}">${escapeHtml(rule.name)}</option>`)
    .join("")}`;
  select.value = rules.some(rule => rule.name === previous) ? previous : "";
  target.innerHTML = rules.length === 0
    ? `<p class="muted">No retention rules configured. Use explicit torrent hashes in preview/apply.</p>`
    : `<table><thead><tr><th>Rule</th><th>Match</th><th>Minimum age</th></tr></thead><tbody>${rules.map(rule => {
        const match = [
          rule.profile ? `profile ${rule.profile}` : "",
          rule.label ? `label ${rule.label}` : "",
          rule.states?.length ? `state ${rule.states.join(", ")}` : "any state",
        ].filter(Boolean).join("; ");
        return `<tr><td>${escapeHtml(rule.name)}</td><td>${escapeHtml(match)}</td><td>${fmtCount(rule.min_age_seconds)} s</td></tr>`;
      }).join("")}</tbody></table>`;
}

function renderEntries(status) {
  const target = $("#trash-entries");
  if (!target) return;
  const entries = status.entries || [];
  target.innerHTML = entries.length === 0
    ? `<p class="muted">Trash is empty.</p>`
    : `<table><thead><tr><th>Entry</th><th>Torrent</th><th>Status</th><th>Reason</th><th>Size</th><th>Actions</th></tr></thead><tbody>${entries.map(entry => `
        <tr>
          <td>${escapeHtml(entry.id)}<div class="muted">${escapeHtml(fmtUnixSeconds(entry.created_at))}</div></td>
          <td>${escapeHtml(entry.name)}<div class="muted">${escapeHtml(entry.torrent_key || "")}</div></td>
          <td>${renderStatus(entry.status === "ready" ? "ok" : "warning")} ${escapeHtml(entry.status)}${entry.has_original_metainfo ? `<div class="muted">original metainfo retained</div>` : ""}</td>
          <td>${escapeHtml(entry.reason)}${entry.applied_rule ? `<div class="muted">rule ${escapeHtml(entry.applied_rule)}</div>` : ""}${entry.error ? `<div class="form-error">${escapeHtml(entry.error)}</div>` : ""}</td>
          <td>${fmtBytes(entry.moved_bytes || 0)}</td>
          <td class="rss-actions">
            <button type="button" data-trash-action="restore" data-trash-id="${escapeHtml(entry.id)}" ${entry.status === "ready" ? "" : "disabled"}>Restore</button>
            <button type="button" class="danger" data-trash-action="purge" data-trash-id="${escapeHtml(entry.id)}">Purge</button>
          </td>
        </tr>`).join("")}</tbody></table>`;
}

function renderPreview(preview) {
  const target = $("#trash-preview-results");
  if (!target) return;
  const items = preview.items || [];
  target.innerHTML = items.length === 0
    ? `<p class="muted">No cleanup candidates for this selection.</p>`
    : items.map(item => `<div class="card-subsection">
        <h4>${escapeHtml(item.name)} <span class="muted">${escapeHtml(item.torrent_key)}</span></h4>
        ${renderKv([
          ["Action", item.action],
          ["State", item.state],
          ["Profile", item.effective_profile || "none"],
          ["Labels", (item.labels || []).join(", ")],
          ["Age", `${fmtCount(item.age_seconds)} s`],
          ["Retention rule", item.applied_rule || "none"],
          ["Rule decision", item.rule_reason],
          ["Qualifies at", item.qualifies_at ? fmtUnixSeconds(item.qualifies_at) : "now"],
          ["Eligible", item.eligible ? "yes" : "no"],
        ])}
        ${item.blockers?.length ? `<p class="form-error">${escapeHtml(item.blockers.join("; "))}</p>` : ""}
        ${item.paths?.length ? `<table><thead><tr><th>Path</th><th>Role</th><th>Exists</th><th>Bytes</th><th>Action</th><th>Reason</th></tr></thead><tbody>${item.paths.map(path => `<tr>
          <td>${escapeHtml(path.path)}</td>
          <td>${escapeHtml(path.role)}</td>
          <td>${path.exists ? "yes" : "no"}</td>
          <td>${fmtBytes(path.bytes || 0)}</td>
          <td>${escapeHtml(path.action)}</td>
          <td>${escapeHtml(path.reason)}${path.blocked_reason ? `<div class="form-error">${escapeHtml(path.blocked_reason)}</div>` : ""}</td>
        </tr>`).join("")}</tbody></table>` : `<p class="muted">No payload paths; the record itself is eligible.</p>`}
      </div>`).join("");
  const apply = $("#trash-apply-btn");
  if (apply) apply.disabled = !items.some(item => item.eligible);
}

export async function refreshTrash() {
  try {
    const status = await api("/trash");
    const summary = $("#trash-summary");
    if (summary) {
      summary.textContent = status.enabled
        ? `Trash: ${status.path} — ${(status.entries || []).length} entries`
        : "Trash is disabled. Configure trash.path and restart the daemon.";
    }
    renderRules(status);
    renderEntries(status);
    return status;
  } catch (error) {
    showError("Trash status failed", error);
    return null;
  }
}

export async function previewTrash() {
  try {
    lastSelection = selectionRequest();
    const preview = await api("/trash/preview", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(lastSelection),
    });
    renderPreview(preview);
  } catch (error) {
    showError("Cleanup preview failed", error);
  }
}

export async function applyTrash() {
  if (!lastSelection) return;
  try {
    const result = await api("/trash/apply", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(lastSelection),
    });
    const moved = (result.items || []).filter(item => item.status === "trashed").length;
    const failed = (result.items || []).filter(item => item.status === "failed");
    showToast("Cleanup applied", `${moved} trashed; ${failed.length} failed`, failed.length ? "warning" : "success");
    if (failed.length) {
      $("#trash-preview-results").innerHTML = `<div class="form-error">${failed.map(item => `${escapeHtml(item.name)}: ${escapeHtml(item.error || "failed")}`).join("<br>")}</div>`;
    }
    lastSelection = null;
    await refreshTrash();
  } catch (error) {
    showError("Cleanup apply failed", error);
  }
}

async function runEntryAction(action, id) {
  try {
    if (action === "restore") {
      if (!window.confirm(`Restore trash entry ${id}? Existing files are never overwritten.`)) return;
      const result = await api(`/trash/${encodeURIComponent(id)}/restore`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ collision_policy: "fail" }),
      });
      showToast("Trash entry restored", `${result.paths_restored} paths`, "success");
    } else if (action === "purge") {
      if (!window.confirm(`Permanently purge trash entry ${id}? This cannot be undone.`)) return;
      await api(`/trash/${encodeURIComponent(id)}`, { method: "DELETE" });
      showToast("Trash entry purged", id, "success");
    }
    await refreshTrash();
  } catch (error) {
    showError(`Trash ${action} failed`, error);
  }
}

$("#trash-preview-btn")?.addEventListener("click", previewTrash);
$("#trash-apply-btn")?.addEventListener("click", applyTrash);
$("#trash-refresh-btn")?.addEventListener("click", refreshTrash);
$("#trash-entries")?.addEventListener("click", event => {
  const button = event.target.closest("button[data-trash-action]");
  if (!button) return;
  runEntryAction(button.dataset.trashAction, button.dataset.trashId);
});
