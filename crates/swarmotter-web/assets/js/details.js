// SPDX-License-Identifier: Apache-2.0

import { state, TORRENT_QUERY_STORAGE_KEY, TORRENT_DEFAULT_PER_PAGE, TORRENT_MAX_PER_PAGE, TORRENT_SORT_OPTIONS, TORRENT_TABLE_TO_QUERY_SORT, TORRENT_QUERY_TO_TABLE_SORT, TORRENT_ACTIONS, EVENT_KINDS, MAX_LOG_LINES, watchHistoryUi } from "./state.js";
import * as ui from "./ui.js";
import * as requests from "./api.js";
const { $, $$, showToast, showError, finiteNumber, fmtCount, fmtBytes, fmtRate, fmtRatio, fmtPercentFromFraction, fmtPercent, fmtProgress, fmtUnixSeconds, renderProgressCell, renderHealth, renderHealthSummary, fmtScore, renderStatus, renderKv, renderCheckList, escapeHtml, cssToken, log, setToastDisplaySeconds } = ui;
const { api, apiFetch, responseErrorMessage, saveApiToken } = requests;

let refreshTorrentsHandler = async () => {};
export function setRefreshTorrentsHandler(handler) { refreshTorrentsHandler = handler || (async () => {}); }

export function detailsRequestIsCurrent(hash) {
  const view = $("#view-details");
  return state.currentHash === hash && view && !view.classList.contains("hidden");
}

export function beginDetailsLoad() {
  $("#details-title").textContent = "Loading torrent details";
  $("#details-health").innerHTML = "";
  $("#details-summary").innerHTML = "";
  $$("#details-seeding-summary dd").forEach(field => { field.textContent = ""; });
  $("#details-autopilot").innerHTML = "";
  $("#details-activity").innerHTML = `<h3>Activity</h3><p class="muted">Loading activity...</p>`;
  $("#details-controls").classList.add("hidden");
  $("#details-seeding-error").textContent = "";
  $("#tracker-add-btn").disabled = true;
  $("#tracker-add-url").value = "";
  for (const selector of ["#files-table tbody", "#peers-table tbody", "#trackers-table tbody"]) {
    $(selector).innerHTML = "";
  }
}

export async function openDetails(hash) {
  state.currentHash = hash;
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-details").classList.remove("hidden");
  beginDetailsLoad();
  try {
    const [t, stats, decision, autopilotStatus, networkDiag] = await Promise.all([
      api(`/torrents/${hash}`),
      api(`/torrents/${hash}/stats`).catch(() => null),
      api(`/torrents/${hash}/autopilot`).catch(() => null),
      api("/autopilot/status").catch(() => null),
      api("/network/diagnostics").catch(() => null),
    ]);
    if (!detailsRequestIsCurrent(hash)) return;
    $("#details-title").textContent = t.name;
    renderDetailsHealth(t.health);
    renderDetailsSummary(t);
    renderDetailsControls(t);
    renderDetailsActivity(stats || t);
    renderAutopilotDiagnostics({
      torrent: t,
      decision,
      globalAutopilot: autopilotStatus,
      networkDiagnostics: networkDiag,
    });
    bindAutopilotModeSelector(hash, t.autopilot_mode_override);
    $("#details-controls").classList.remove("hidden");
    $("#tracker-add-btn").disabled = false;
    loadFiles(hash);
    loadPeers(hash);
    loadTrackers(hash);
  } catch (e) {
    if (!detailsRequestIsCurrent(hash)) return;
    $("#details-title").textContent = "Torrent details";
    renderDetailsHealth(null);
    renderDetailsActivity(null);
    renderAutopilotDiagnostics({ torrent: null, decision: null, globalAutopilot: null, networkDiagnostics: null });
    $("#details-summary").innerHTML = "";
    showError("Open torrent details failed", e);
  }
}

export function renderDetailsHealth(h) {
  if (!h) { $("#details-health").innerHTML = ""; return; }
  const reasons = (h.reasons || []).map(r => `<li>${escapeHtml(r)}</li>`).join("");
  const reasonsHtml = reasons ? `<ul class="health-list">${reasons}</ul>` : "";
  const subs = `
    <table class="health-subscores">
      <thead><tr><th>Component</th><th>Score</th></tr></thead>
      <tbody>
        <tr><td>Availability</td><td>${fmtScore(h.availability_score)}</td></tr>
        <tr><td>Throughput</td><td>${fmtScore(h.throughput_score)}</td></tr>
        <tr><td>Peers</td><td>${fmtScore(h.peer_score)}</td></tr>
        <tr><td>Stability</td><td>${fmtScore(h.stability_score)}</td></tr>
        <tr><td>Discovery</td><td>${fmtScore(h.discovery_score)}</td></tr>
      </tbody>
    </table>`;
  $("#details-health").innerHTML = `
    <h3>Health</h3>
    ${renderHealth(h)}
    <p class="muted">${renderHealthSummary(h)}</p>
    ${reasonsHtml}
    ${subs}
  `;
}

export function renderDetailsSummary(t) {
  if (!t) {
    $("#details-summary").innerHTML = "";
    return;
  }
  $("#details-summary").innerHTML = `
    <h3>Details</h3>
    ${renderKv([
      ["State", t.state],
      ["Last error", t.error || ""],
      ["Peers", `${fmtCount(t.active_peer_workers)} active / ${fmtCount(t.known_peers)} known`],
      ["Rate down", fmtRate(t.rate_down)],
      ["Rate up", fmtRate(t.rate_up)],
      ["Ratio", finiteNumber(t.ratio) === null ? "" : String(t.ratio)],
      ["Uploaded", fmtBytes(t.uploaded)],
      ["Seeding status", String(t.seeding_status || "not_eligible").replace(/_/g, " ")],
      ["Stored ratio target", t.seeding?.ratio_limit === null ? "inherit" : String(t.seeding?.ratio_limit)],
      ["Effective ratio target", t.effective_ratio_limit === null ? "none" : String(t.effective_ratio_limit)],
      ["Stored idle target", t.seeding?.idle_limit === null ? "inherit" : `${t.seeding?.idle_limit} s`],
      ["Effective idle target", t.effective_idle_limit === null ? "none" : `${t.effective_idle_limit} s`],
      ["Seed forever", t.seeding?.seed_forever ? "yes" : "no"],
      ["Download cap", fmtBytes(t.download_limit || 0)],
      ["Upload cap", fmtBytes(t.upload_limit || 0)],
      ["Queue position", fmtCount(t.queue_position)],
      ["Labels", (t.labels || []).join(", ")],
    ])}
  `;
}

export function renderDetailsControls(t) {
  if (!t) return;
  $("#details-move-path").value = t.download_dir || "";
  $("#details-labels").value = (t.labels || []).join(", ");
  $("#details-download-limit").value = String(finiteNumber(t.download_limit) ?? 0);
  $("#details-upload-limit").value = String(finiteNumber(t.upload_limit) ?? 0);
  SwarmOtterSeedingPolicy.render(document, t);
}

export function renderDetailsActivity(stats) {
  const panel = $("#details-activity");
  if (!panel) return;
  if (!stats) {
    panel.innerHTML = `<h3>Activity</h3><p class="muted">Activity data is unavailable.</p>`;
    return;
  }
  panel.innerHTML = `
    <h3>Activity</h3>
    ${renderKv([
      ["State", stats.state],
      ["Progress", fmtPercentFromFraction(stats.progress, 1) || fmtProgress(stats.bytes_completed, stats.total_length)],
      ["Downloaded", fmtBytes(stats.downloaded)],
      ["Uploaded", fmtBytes(stats.uploaded)],
      ["Download rate", fmtRate(stats.rate_down)],
      ["Upload rate", fmtRate(stats.rate_up)],
      ["Pieces", `${fmtCount(stats.pieces_have)} / ${fmtCount(stats.piece_count)}`],
      ["Active / known peers", `${fmtCount(stats.active_peer_workers)} / ${fmtCount(stats.known_peers)}`],
      ["Tracker", stats.tracker_message || (stats.tracker_ok ? "healthy" : "unavailable")],
    ])}`;
}

export function renderAutopilotDiagnostics({ torrent, decision, globalAutopilot, networkDiagnostics }) {
  const panel = $("#details-autopilot");
  if (!panel) return;
  if (!torrent) {
    panel.innerHTML = `<h3>Autopilot diagnostics</h3><p class="muted">No diagnostics available for this torrent.</p>`;
    return;
  }
  const override = torrent.autopilot_mode_override === null ? null : (torrent.autopilot_mode_override || null);
  const globalMode = globalAutopilot?.mode || "unknown";
  const effectiveMode = override ?? globalMode;
  const health = networkDiagnostics?.health || {};
  const checks = (networkDiagnostics?.checks || []).filter((c) => c && c.level && c.level !== "ok");
  const containment = (networkDiagnostics?.containment_matrix || []).filter((c) => c && c.level && c.level !== "ok");
  const reasons = (decision?.reasons || []);
  const reasonLines = reasons.length
    ? reasons.map((item) => {
      const cause = item.cause ? `<code>${escapeHtml(item.cause)}</code> ` : "";
      return `<li>${cause}${escapeHtml(item.message || String(item))}</li>`;
    }).join("")
    : "<li>No slow-condition reasons reported.</li>";
  const snapshot = decision?.snapshot || null;
  const action = decision?.action || null;
  const networkSummary = networkHealthSummary(health, checks, containment);
  panel.innerHTML = `
    <h3>Autopilot diagnostics</h3>
    <div class="autopilot-control-row">
      <label for="details-autopilot-mode">Per-torrent mode</label>
      <select id="details-autopilot-mode" aria-label="Per-torrent autopilot mode">
        <option value="inherit"${override === null ? " selected" : ""}>inherit</option>
        <option value="disabled"${override === "disabled" ? " selected" : ""}>disabled</option>
        <option value="observe"${override === "observe" ? " selected" : ""}>observe</option>
        <option value="act"${override === "act" ? " selected" : ""}>act</option>
      </select>
    </div>
    ${renderKv([
      ["Global mode", renderAutopilotModeLabel(globalMode)],
      ["Effective mode", renderAutopilotModeLabel(effectiveMode)],
      ["Network", networkSummary],
      ["Decision", decision?.apply ? "recommendation ready" : "no immediate action"],
    ])}
    <div class="autopilot-section-heading">Why is this slow?</div>
    <ul class="compact-list">${reasonLines}</ul>
    ${action ? `<p><strong>Recommended action:</strong> ${escapeHtml(renderAutopilotActionKind(action.kind))}<code> (${escapeHtml(action.kind || "action")})</code>. ${escapeHtml(action.rationale || "")}</p>` : ""}
    ${snapshot ? `${renderAutopilotSnapshot(snapshot)}` : ""}
    ${checks.length || containment.length ? `<div class="autopilot-section-heading">Network impact</div>${renderAutopilotChecks(checks, containment)}` : ""}
  `;
}

export function renderAutopilotChecks(checks, containment) {
  const items = [...checks, ...containment];
  if (!items.length) return "<p class='muted'>Network checks pass for autopilot conditions.</p>";
  return `<ul class="compact-list">${items.map(check => `
    <li>
      <div>${renderStatus(check.level)} <strong>${escapeHtml(check.label || check.id)}</strong></div>
      <div class="muted">${escapeHtml(check.detail || "")}</div>
    </li>`).join("")}</ul>`;
}

export function renderAutopilotSnapshot(snapshot) {
  const rows = [
    ["Known peers", snapshot.known_peers],
    ["Useful peers", snapshot.useful_peers],
    ["Active workers", snapshot.active_peer_workers],
    ["Peer worker limit", snapshot.peer_worker_limit],
    ["Tracker healthy", snapshot.tracker_ok ? "yes" : "no"],
    ["Discovery", snapshot.discovery_ok ? "yes" : "no"],
    ["Backed off peers", snapshot.backed_off_peers],
    ["Observed peak down", fmtRate(snapshot.rate_down_observed_peak)],
  ];
  return `<div class="autopilot-section-heading">Why now</div>${renderKv(rows.map(([label, value]) => [label, value]))}`;
}

export function renderAutopilotModeLabel(mode) {
  if (mode === null || mode === undefined || mode === "") return "inherit";
  if (mode === "act") return "act";
  if (mode === "observe") return "observe";
  if (mode === "disabled") return "disabled";
  return String(mode);
}

export function renderAutopilotActionKind(kind) {
  if (kind === "increase_peer_workers") return "Increase peer workers";
  if (kind === "expand_discovery") return "Expand discovery";
  if (kind === "relax_peer_backoff") return "Relax peer backoff";
  if (kind === "release_queue_slot") return "Release queue slot";
  if (kind === "raise_download_ceiling") return "Raise download ceiling";
  return String(kind || "recommendation");
}

export function networkHealthSummary(health, checks, containment) {
  const status = health.status || "unknown";
  const allowed = health.traffic_allowed;
  const traffic = allowed === false ? "blocked" : "allowed";
  const issues = [...checks, ...containment].filter(
    item => item && (item.level === "invalid" || item.level === "warning"),
  );
  const issueText = issues.length
    ? ` (${issues.length} impacting containment)`
    : "";
  return `${status} / traffic ${traffic}${issueText}`;
}

export function bindAutopilotModeSelector(hash) {
  const select = $("#details-autopilot-mode");
  if (!select) return;
  select.disabled = state.autopilotModeUpdateInFlight;
  const setMode = async () => {
    const nextMode = select.value;
    await setAutopilotModeOverride(hash, nextMode);
  };
  select.onchange = setMode;
}

export async function setAutopilotModeOverride(hash, nextMode) {
  if (!hash || state.autopilotModeUpdateInFlight) return;
  state.autopilotModeUpdateInFlight = true;
  const select = $("#details-autopilot-mode");
  const previous = select ? select.value : "inherit";
  if (select) select.disabled = true;
  try {
    const body = { mode: nextMode === "inherit" ? null : nextMode };
    await api(`/torrents/${hash}/autopilot`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!detailsRequestIsCurrent(hash)) return;
    showToast("Autopilot mode saved", "Override updated for this torrent.", "success");
    await openDetails(hash);
  } catch (e) {
    if (select) select.value = previous;
    if (detailsRequestIsCurrent(hash)) showError("Failed to update autopilot mode", e);
  } finally {
    state.autopilotModeUpdateInFlight = false;
    if (select) select.disabled = false;
    const activeSelect = $("#details-autopilot-mode");
    if (activeSelect) activeSelect.disabled = false;
  }
}

$$(".tab").forEach(btn => btn.addEventListener("click", () => {
  $$(".tab").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  $$(".tab-pane").forEach(p => p.classList.add("hidden"));
  $("#tab-" + btn.dataset.tab).classList.remove("hidden");
}));

export async function loadFiles(hash) {
  try {
    const files = await api(`/torrents/${hash}/files`);
    if (!detailsRequestIsCurrent(hash)) return;
    const tbody = $("#files-table tbody");
    tbody.innerHTML = "";
    files.forEach(f => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(f.path)}</td><td>${fmtBytes(f.length)}</td><td>${fmtBytes(f.bytes_completed)}</td><td><select data-fi="${f.index}" class="prio" aria-label="Priority for ${escapeHtml(f.path)}"><option value="unwanted">Unwanted</option><option value="low">Low</option><option value="normal">Normal</option><option value="high">High</option></select></td><td><input type="checkbox" data-fi="${f.index}" class="want" aria-label="Download ${escapeHtml(f.path)}" ${f.wanted ? "checked" : ""}></td><td><div class="file-rename-row"><input type="text" data-fi="${f.index}" class="rename-path" value="${escapeHtml(f.path)}" aria-label="New path for ${escapeHtml(f.path)}"><button type="button" data-fi="${f.index}" class="rename-file secondary">Rename</button></div></td>`;
      tbody.appendChild(tr);
    });
    $$("#files-table .prio").forEach(sel => {
      const file = files.find(f => f.index == sel.dataset.fi);
      if (file && file.priority) sel.value = file.priority;
    });
    $$("#files-table .prio").forEach(sel => sel.addEventListener("change", async () => {
      const fi = parseInt(sel.dataset.fi, 10);
      const priority = sel.value;
      try {
        await api(`/torrents/${hash}/files/priority`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ file_indices: [fi], priority }) });
        if (!detailsRequestIsCurrent(hash)) return;
        showToast("File priority saved", "", "success");
      } catch (e) {
        if (!detailsRequestIsCurrent(hash)) return;
        showError("File priority failed", e);
        await loadFiles(hash);
      }
    }));
    $$("#files-table .want").forEach(cb => cb.addEventListener("change", async () => {
      const fi = parseInt(cb.dataset.fi, 10);
      try {
        await api(`/torrents/${hash}/files/wanted`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ file_indices: [fi], wanted: cb.checked }) });
        if (!detailsRequestIsCurrent(hash)) return;
        showToast("File selection saved", "", "success");
      } catch (e) {
        if (!detailsRequestIsCurrent(hash)) return;
        showError("File selection failed", e);
        await loadFiles(hash);
      }
    }));
    $$("#files-table .rename-file").forEach(button => button.addEventListener("click", async () => {
      const fi = parseInt(button.dataset.fi, 10);
      const input = $(`#files-table .rename-path[data-fi="${fi}"]`);
      const newPath = input?.value.trim() || "";
      if (!newPath) {
        showToast("Enter a new file path", "", "warning");
        return;
      }
      button.disabled = true;
      try {
        await api(`/torrents/${hash}/files/${fi}/rename`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ new_path: newPath }),
        });
        if (!detailsRequestIsCurrent(hash)) return;
        showToast("File renamed", newPath, "success");
        await loadFiles(hash);
      } catch (e) {
        if (detailsRequestIsCurrent(hash)) showError("File rename failed", e);
      } finally {
        button.disabled = false;
      }
    }));
  } catch (e) { log("files error: " + e.message); }
}

export async function loadPeers(hash) {
  try {
    const peers = await api(`/torrents/${hash}/peers`) || [];
    if (!detailsRequestIsCurrent(hash)) return;
    const tbody = $("#peers-table tbody");
    tbody.innerHTML = "";
    peers.forEach(p => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(p.address)}</td><td>${escapeHtml(p.client)}</td><td>${fmtPercentFromFraction(p.progress, 0)}</td><td>${fmtRate(p.rate_down)}</td><td>${fmtRate(p.rate_up)}</td>`;
      tbody.appendChild(tr);
    });
  } catch (e) { log("peers error: " + e.message); }
}

export async function loadTrackers(hash) {
  try {
    const trackers = await api(`/torrents/${hash}/trackers`) || [];
    if (!detailsRequestIsCurrent(hash)) return;
    const tbody = $("#trackers-table tbody");
    tbody.innerHTML = "";
    trackers.forEach(t => {
      const tr = document.createElement("tr");
      const scrapeStatus = t.scrape_status || "not_contacted";
      const scrapeDetail = t.last_scrape_error
        ? `${scrapeStatus}: ${t.last_scrape_error}`
        : scrapeStatus;
      const scrapeCounts = [t.scrape_seeders, t.scrape_leechers, t.scrape_downloads]
        .map(value => fmtCount(value) || "–")
        .join(" / ");
      tr.innerHTML = `<td>${escapeHtml(t.url)}</td><td>${fmtCount(t.tier)}</td><td>${escapeHtml(t.status)}</td><td>${escapeHtml(scrapeDetail)}</td><td>${escapeHtml(fmtUnixSeconds(t.last_scrape))}</td><td>${escapeHtml(scrapeCounts)}</td><td>${fmtCount(t.seeders)}</td><td>${fmtCount(t.leechers)}</td><td>${fmtCount(t.downloads)}</td><td><div class="tracker-edit-row"><input type="url" class="tracker-edit-url" value="${escapeHtml(t.url)}" aria-label="Edit tracker ${escapeHtml(t.url)}"><button type="button" class="tracker-save secondary" data-url="${escapeHtml(t.url)}">Save</button><button type="button" class="tracker-remove danger" data-url="${escapeHtml(t.url)}">Remove</button></div></td>`;
      tbody.appendChild(tr);
    });
    $$("#trackers-table .tracker-save").forEach(button => button.addEventListener("click", async () => {
      const oldUrl = button.dataset.url;
      const newUrl = button.closest(".tracker-edit-row")?.querySelector(".tracker-edit-url")?.value.trim() || "";
      if (!newUrl || newUrl === oldUrl) return;
      try {
        await api(`/torrents/${hash}/trackers/edit`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ old_url: oldUrl, new_url: newUrl }),
        });
        if (!detailsRequestIsCurrent(hash)) return;
        showToast("Tracker updated", newUrl, "success");
        await loadTrackers(hash);
      } catch (e) {
        if (detailsRequestIsCurrent(hash)) showError("Tracker update failed", e);
      }
    }));
    $$("#trackers-table .tracker-remove").forEach(button => button.addEventListener("click", async () => {
      const url = button.dataset.url;
      if (!window.confirm(`Remove tracker ${url}?`)) return;
      try {
        await api(`/torrents/${hash}/trackers/${encodeURIComponent(url)}`, { method: "DELETE" });
        if (!detailsRequestIsCurrent(hash)) return;
        showToast("Tracker removed", url, "success");
        await loadTrackers(hash);
      } catch (e) {
        if (detailsRequestIsCurrent(hash)) showError("Tracker removal failed", e);
      }
    }));
  } catch (e) { log("trackers error: " + e.message); }
}

$("#tracker-add-btn").addEventListener("click", async () => {
  const hash = state.currentHash;
  const url = $("#tracker-add-url").value.trim();
  if (!hash || !url) {
    showToast("Enter a tracker URL", "", "warning");
    return;
  }
  try {
    await api(`/torrents/${hash}/trackers`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ url }),
    });
    if (!detailsRequestIsCurrent(hash)) return;
    $("#tracker-add-url").value = "";
    showToast("Tracker added", url, "success");
    await loadTrackers(hash);
  } catch (e) {
    if (detailsRequestIsCurrent(hash)) showError("Tracker add failed", e);
  }
});

$("#back-btn").addEventListener("click", () => {
  state.currentHash = null;
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-torrents").classList.remove("hidden");
  $$(".nav").forEach(b => b.classList.remove("active"));
  $$(".nav")[0].classList.add("active");
  refreshTorrentsHandler();
});

export async function runDetailsCommand(button, suffix, title, body = null) {
  if (!state.currentHash) return;
  const hash = state.currentHash;
  button.dataset.pendingHash = hash;
  button.disabled = true;
  try {
    const options = { method: "POST" };
    if (body !== null) {
      options.headers = { "content-type": "application/json" };
      options.body = JSON.stringify(body);
    }
    await api(`/torrents/${hash}${suffix}`, options);
    if (!detailsRequestIsCurrent(hash)) return;
    showToast(title, "", "success");
    await openDetails(hash);
    refreshTorrentsHandler();
  } catch (e) {
    if (detailsRequestIsCurrent(hash)) showError(`${title} failed`, e);
  } finally {
    if (button.dataset.pendingHash === hash) {
      delete button.dataset.pendingHash;
      button.disabled = false;
    }
  }
}

[
  ["details-start-btn", "/start", "Torrent started"],
  ["details-stop-btn", "/stop", "Torrent stopped"],
  ["details-reannounce-btn", "/reannounce", "Reannounce requested"],
  ["details-queue-top-btn", "/queue/move-top", "Moved to queue top"],
  ["details-queue-up-btn", "/queue/move-up", "Moved up in queue"],
  ["details-queue-down-btn", "/queue/move-down", "Moved down in queue"],
  ["details-queue-bottom-btn", "/queue/move-bottom", "Moved to queue bottom"],
].forEach(([id, path, title]) => {
  $("#" + id).addEventListener("click", event => runDetailsCommand(event.currentTarget, path, title));
});

$("#details-move-btn").addEventListener("click", event => {
  const path = $("#details-move-path").value.trim();
  if (!path) {
    showToast("Enter a destination path", "", "warning");
    return;
  }
  runDetailsCommand(event.currentTarget, "/move", "Torrent data moved", { path });
});

$("#details-labels-btn").addEventListener("click", event => {
  const labels = $("#details-labels").value.split(",").map(label => label.trim()).filter(Boolean);
  runDetailsCommand(event.currentTarget, "/labels", "Torrent labels saved", { labels });
});

$("#details-limits-btn").addEventListener("click", event => {
  const downloadLimit = Math.max(0, Math.trunc(finiteNumber($("#details-download-limit").value) ?? 0));
  const uploadLimit = Math.max(0, Math.trunc(finiteNumber($("#details-upload-limit").value) ?? 0));
  runDetailsCommand(event.currentTarget, "/limits", "Torrent limits saved", {
    download_limit: downloadLimit,
    upload_limit: uploadLimit,
  });
});

$("#details-ratio-inherit").addEventListener("change", event => {
  void event;
  SwarmOtterSeedingPolicy.syncInheritance(document);
});

$("#details-idle-inherit").addEventListener("change", event => {
  void event;
  SwarmOtterSeedingPolicy.syncInheritance(document);
});

$("#details-seeding-save-btn").addEventListener("click", async event => {
  if (!state.currentHash) return;
  const hash = state.currentHash;
  const button = event.currentTarget;
  button.disabled = true;
  try {
    await SwarmOtterSeedingPolicy.save(document, hash, api);
    if (!detailsRequestIsCurrent(hash)) return;
    showToast("Seeding policy saved", "", "success");
    await openDetails(hash);
    refreshTorrentsHandler();
  } catch (error) {
    if (!detailsRequestIsCurrent(hash)) return;
    showError("Seeding policy failed", error);
  } finally {
    button.disabled = false;
  }
});
