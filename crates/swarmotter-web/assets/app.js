// SPDX-License-Identifier: Apache-2.0
// SwarmOtter Web UI controller. Consumes the same REST API as external tools.
const API = "/api/v1";
const DEFAULT_TOAST_DISPLAY_MS = 5000;
const TOAST_DISPLAY_STORAGE_KEY = "swarmotter.toastDisplayMs";
let currentHash = null;
let toastDisplayMs = loadToastDisplayMs();
let torrentsLoaded = false;
let knownTorrents = new Map();
let expectedRemovedTorrents = new Map();

const TORRENT_ACTIONS = [
  {
    act: "pause",
    label: "Pause",
    icon: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M8 5v14M16 5v14"/></svg>`,
  },
  {
    act: "resume",
    label: "Resume",
    icon: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M8 5v14l11-7-11-7z"/></svg>`,
  },
  {
    act: "recheck",
    label: "Recheck",
    icon: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M20 6v6h-6"/><path d="M4 18v-6h6"/><path d="M19 9a7 7 0 0 0-11.9-4.9L4 7"/><path d="M5 15a7 7 0 0 0 11.9 4.9L20 17"/></svg>`,
  },
  {
    act: "remove",
    label: "Remove",
    danger: true,
    icon: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M6 6l1 15h10l1-15"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>`,
  },
];

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function loadToastDisplayMs() {
  try {
    const raw = window.localStorage.getItem(TOAST_DISPLAY_STORAGE_KEY);
    const ms = Number(raw);
    return Number.isFinite(ms) && ms >= 1000 ? ms : DEFAULT_TOAST_DISPLAY_MS;
  } catch {
    return DEFAULT_TOAST_DISPLAY_MS;
  }
}

function setToastDisplaySeconds(seconds) {
  const n = Number(seconds);
  const ms = Number.isFinite(n)
    ? Math.max(1000, Math.min(60000, Math.round(n * 1000)))
    : DEFAULT_TOAST_DISPLAY_MS;
  toastDisplayMs = ms;
  try { window.localStorage.setItem(TOAST_DISPLAY_STORAGE_KEY, String(ms)); } catch {}
  return ms;
}

function showToast(title, message = "", type = "info", durationMs = toastDisplayMs) {
  const region = $("#toast-region");
  if (!region) return;
  const toast = document.createElement("div");
  toast.className = "toast " + cssToken(type || "info");
  toast.setAttribute("role", type === "error" ? "alert" : "status");
  toast.innerHTML = `
    <div class="toast-title">${escapeHtml(title)}</div>
    ${message ? `<div class="toast-message">${escapeHtml(message)}</div>` : ""}`;
  region.appendChild(toast);
  window.setTimeout(() => toast.remove(), durationMs);
}

function showError(title, error) {
  showToast(title, error && error.message ? error.message : String(error || ""), "error");
}

function finiteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fmtCount(value) {
  const n = finiteNumber(value);
  return n === null ? "" : String(n);
}

function fmtBytes(n) {
  n = finiteNumber(n);
  if (n === null) return "";
  if (n <= 0) return "0 B";
  const u = ["B","KB","MB","GB","TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(i === 0 ? 0 : 1) + " " + u[i];
}
function fmtRate(n) {
  const bytes = fmtBytes(n);
  return bytes ? bytes + "/s" : "";
}
function fmtRatio(n) {
  n = finiteNumber(n);
  return n === null ? "" : n.toFixed(2);
}
function fmtPercentFromFraction(n, digits = 1) {
  n = finiteNumber(n);
  return n === null ? "" : (n * 100).toFixed(digits) + "%";
}
function fmtProgress(bytesCompleted, totalLength) {
  const completed = finiteNumber(bytesCompleted);
  const total = finiteNumber(totalLength);
  if (completed === null || total === null || total <= 0) return "";
  return (completed / total * 100).toFixed(1) + "%";
}
function renderProgressCell(bytesCompleted, totalLength) {
  const completed = finiteNumber(bytesCompleted);
  const total = finiteNumber(totalLength);
  if (completed === null || total === null || total <= 0) return "";
  return `<progress value="${completed}" max="${total}"></progress> ${fmtProgress(completed, total)}`;
}

async function api(path, opts = {}) {
  const res = await fetch(API + path, opts);
  const text = await res.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { success: false, error: { code: "parse_error", message: text } }; }
  if (!body.success && body.error) {
    const err = new Error(body.error.message || body.error.code);
    err.code = body.error.code;
    err.status = res.status;
    throw err;
  }
  return body.data;
}

// --- Navigation ---
$$(".nav").forEach(btn => btn.addEventListener("click", () => {
  $$(".nav").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  const view = btn.dataset.view;
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-" + view).classList.remove("hidden");
  if (view === "torrents") refreshTorrents();
  if (view === "network") refreshNetwork();
  if (view === "settings") refreshSettings();
  if (view === "watch") refreshWatch();
  if (view === "logs") refreshLogs();
}));

// --- Torrents ---
async function refreshTorrents() {
  try {
    const list = await api("/torrents");
    const stats = await api("/stats");
    observeTorrentRemovals(list);
    const tbody = $("#torrent-table tbody");
    tbody.innerHTML = "";
    const filter = $("#search").value.toLowerCase();
    list.filter(t => String(t.name || "").toLowerCase().includes(filter)).forEach(t => {
      const tr = document.createElement("tr");
      tr.className = "torrent";
      tr.dataset.hash = t.info_hash;
      tr.innerHTML = `
        <td>${escapeHtml(t.name)}</td>
        <td>${fmtBytes(t.total_length)}</td>
        <td>${renderProgressCell(t.bytes_completed, t.total_length)}</td>
        <td>${escapeHtml(t.state)}</td>
        <td>${renderHealth(t.health)}</td>
        <td>${fmtRate(t.rate_down)}</td>
        <td>${fmtRate(t.rate_up)}</td>
        <td>${fmtRatio(t.ratio)}</td>
        <td>${renderPeerCount(t)}</td>
        <td>${renderTorrentActions()}</td>`;
      tr.addEventListener("click", (e) => {
        if (e.target.closest("button")) return;
        openDetails(t.info_hash);
      });
      tbody.appendChild(tr);
    });
    $("#stats-summary").textContent = renderStatsSummary(stats);
    bindActionButtons();
  } catch (e) {
    log("torrent list error: " + e.message);
  }
}

function observeTorrentRemovals(list) {
  const current = new Map((list || []).map(t => [t.info_hash, String(t.name || t.info_hash || "")]));
  if (torrentsLoaded) {
    for (const [hash, name] of knownTorrents.entries()) {
      if (current.has(hash)) continue;
      if (expectedRemovedTorrents.has(hash)) {
        expectedRemovedTorrents.delete(hash);
        continue;
      }
      showToast("Torrent removed", name, "info");
    }
  }
  knownTorrents = current;
  torrentsLoaded = true;
}

function renderPeerCount(t) {
  const active = finiteNumber(t.active_peer_workers);
  const known = finiteNumber(t.known_peers);
  if (active === null && known === null) return "";
  if (known === null) return String(active);
  if (active === null) return String(known);
  return `${active}/${known}`;
}

function renderStatsSummary(stats) {
  const parts = [];
  const torrentCount = fmtCount(stats.torrent_count);
  const down = fmtRate(stats.download_rate);
  const up = fmtRate(stats.upload_rate);
  if (torrentCount) parts.push(`${torrentCount} torrents`);
  if (down) parts.push(`${down} down`);
  if (up) parts.push(`${up} up`);
  return parts.join(" · ");
}

function renderTorrentActions() {
  return `<div class="torrent-actions">${TORRENT_ACTIONS.map(action => {
    const danger = action.danger ? " danger" : "";
    return `<button type="button" data-act="${action.act}" class="icon-button${danger}" aria-label="${action.label}" title="${action.label}">${action.icon}</button>`;
  }).join("")}</div>`;
}

function bindActionButtons() {
  $$("#torrent-table tbody button").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const tr = btn.closest("tr");
      const hash = tr.dataset.hash;
      const name = tr.querySelector("td")?.textContent || hash;
      const act = btn.dataset.act;
      try {
        if (act === "pause") await api(`/torrents/${hash}/pause`, { method: "POST" });
        else if (act === "resume") await api(`/torrents/${hash}/resume`, { method: "POST" });
        else if (act === "recheck") await api(`/torrents/${hash}/recheck`, { method: "POST" });
        else if (act === "remove") {
          if (confirm("Remove torrent? Delete data too?")) await api(`/torrents/${hash}?delete_data=true`, { method: "DELETE" });
          else await api(`/torrents/${hash}`, { method: "DELETE" });
          expectedRemovedTorrents.set(hash, name);
          showToast("Torrent removed", name, "info");
        }
        refreshTorrents();
      } catch (e) { showError("Torrent action failed", e); }
    });
  });
}

// --- Details ---
async function openDetails(hash) {
  currentHash = hash;
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-details").classList.remove("hidden");
  const t = await api(`/torrents/${hash}`);
  $("#details-title").textContent = t.name;
  renderDetailsHealth(t.health);
  $("#details-summary").innerHTML = `<pre>${escapeHtml(JSON.stringify(t, null, 2))}</pre>`;
  loadFiles(hash);
  loadPeers(hash);
  loadTrackers(hash);
}

function healthLabelName(label) {
  switch (label) {
    case "unknown": return "Unknown";
    case "excellent": return "Excellent";
    case "good": return "Good";
    case "fair": return "Fair";
    case "poor": return "Poor";
    case "critical": return "Critical";
    case "stalled": return "Stalled";
    case "network_blocked": return "Blocked";
    case "paused": return "Paused";
    case "complete": return "Complete";
    default: return String(label || "").replace(/_/g, " ");
  }
}

function renderHealth(h) {
  if (!h) return "";
  const label = h.label;
  const labelName = healthLabelName(label);
  const score = fmtCount(h.score);
  const rawBars = finiteNumber(h.bars);
  const bars = rawBars === null ? null : Math.max(0, Math.min(5, rawBars));
  const reasons = (h.reasons || []).join("; ");
  const tooltip = `${labelName}${score ? " - " + score + "/100" : ""}${reasons ? ": " + reasons : ""}`;
  const srText = `Health: ${labelName}${score ? ", " + score + " out of 100" : ""}`;
  let barsHtml = "";
  if (bars !== null) {
    for (let i = 0; i < 5; i++) {
      barsHtml += `<span class="bar${i < bars ? " active" : ""}"></span>`;
    }
  }
  const healthClass = label ? " health-" + cssToken(label) : "";
  return `<div class="torrent-health${healthClass}" title="${escapeHtml(tooltip)}">`
    + `<span class="sr-only">${escapeHtml(srText)}</span>`
    + `<span class="health-bars" aria-hidden="true">${barsHtml}</span>`
    + `<span class="health-label">${escapeHtml(labelName)}</span>`
    + `</div>`;
}

function renderDetailsHealth(h) {
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

function fmtScore(value) {
  const score = fmtCount(value);
  return score ? `${score}/100` : "";
}

function renderHealthSummary(h) {
  const score = fmtScore(h.score);
  return score ? `Score ${score}. Health answers: can this torrent complete, and is it downloading well right now?` : "";
}

$$(".tab").forEach(btn => btn.addEventListener("click", () => {
  $$(".tab").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  $$(".tab-pane").forEach(p => p.classList.add("hidden"));
  $("#tab-" + btn.dataset.tab).classList.remove("hidden");
}));

async function loadFiles(hash) {
  try {
    const files = await api(`/torrents/${hash}/files`);
    const tbody = $("#files-table tbody");
    tbody.innerHTML = "";
    files.forEach(f => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(f.path)}</td><td>${fmtBytes(f.length)}</td><td>${fmtBytes(f.bytes_completed)}</td><td><select data-fi="${f.index}" class="prio"><option value="unwanted">Unwanted</option><option value="low">Low</option><option value="normal">Normal</option><option value="high">High</option></select></td><td><input type="checkbox" data-fi="${f.index}" class="want" ${f.wanted ? "checked" : ""}></td>`;
      tbody.appendChild(tr);
    });
    $$("#files-table .prio").forEach(sel => {
      const file = files.find(f => f.index == sel.dataset.fi);
      if (file && file.priority) sel.value = file.priority;
    });
    $$("#files-table .prio").forEach(sel => sel.addEventListener("change", async () => {
      const fi = parseInt(sel.dataset.fi, 10);
      const priority = sel.value;
      await api(`/torrents/${hash}/files/priority`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ file_indices: [fi], priority }) });
    }));
    $$("#files-table .want").forEach(cb => cb.addEventListener("change", async () => {
      const fi = parseInt(cb.dataset.fi, 10);
      await api(`/torrents/${hash}/files/wanted`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ file_indices: [fi], wanted: cb.checked }) });
    }));
  } catch (e) { log("files error: " + e.message); }
}

async function loadPeers(hash) {
  try {
    const peers = await api(`/torrents/${hash}/peers`) || [];
    const tbody = $("#peers-table tbody");
    tbody.innerHTML = "";
    peers.forEach(p => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(p.address)}</td><td>${escapeHtml(p.client)}</td><td>${fmtPercentFromFraction(p.progress, 0)}</td><td>${fmtRate(p.rate_down)}</td><td>${fmtRate(p.rate_up)}</td>`;
      tbody.appendChild(tr);
    });
  } catch (e) { log("peers error: " + e.message); }
}

async function loadTrackers(hash) {
  try {
    const trackers = await api(`/torrents/${hash}/trackers`) || [];
    const tbody = $("#trackers-table tbody");
    tbody.innerHTML = "";
    trackers.forEach(t => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(t.url)}</td><td>${fmtCount(t.tier)}</td><td>${escapeHtml(t.status)}</td><td>${fmtCount(t.seeders)}</td><td>${fmtCount(t.leechers)}</td>`;
      tbody.appendChild(tr);
    });
  } catch (e) { log("trackers error: " + e.message); }
}

$("#back-btn").addEventListener("click", () => {
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-torrents").classList.remove("hidden");
  $$(".nav").forEach(b => b.classList.remove("active"));
  $$(".nav")[0].classList.add("active");
  refreshTorrents();
});

// --- Add ---
$("#add-magnet-btn").addEventListener("click", async () => {
  try {
    const magnet = $("#magnet-input").value.trim();
    const dir = $("#magnet-dir").value.trim();
    const body = { magnet };
    if (dir) body.download_dir = dir;
    const h = await api("/torrents/magnet", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    showToast("Torrent added", h, "success");
    $("#magnet-input").value = "";
    refreshTorrents();
  } catch (e) { showError("Add magnet failed", e); }
});

$("#add-file-btn").addEventListener("click", async () => {
  try {
    const file = $("#torrent-file").files[0];
    if (!file) { showToast("Choose a .torrent file", "", "warning"); return; }
    const h = await uploadTorrentFile(file);
    showToast("Torrent added", h, "success");
    refreshTorrents();
  } catch (e) { showError("Upload failed", e); }
});

async function uploadTorrentFile(file) {
  const buf = await file.arrayBuffer();
  return api("/torrents/file", {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: buf
  });
}

function torrentFilesFromTransfer(items) {
  return Array.from(items || []).filter(file => file.name.toLowerCase().endsWith(".torrent"));
}

async function uploadDroppedFiles(files) {
  const torrents = torrentFilesFromTransfer(files);
  if (torrents.length === 0) {
    showToast("No .torrent file found", "", "warning");
    return;
  }
  showToast(`Adding ${torrents.length} file${torrents.length === 1 ? "" : "s"}...`);
  let added = 0;
  let failed = 0;
  for (const file of torrents) {
    try {
      await uploadTorrentFile(file);
      added++;
    } catch (e) {
      failed++;
      showToast(`Error adding ${file.name}`, e.message, "error");
      log(`drop upload error (${file.name}): ${e.message}`);
    }
  }
  if (added > 0) {
    refreshTorrents();
  }
  if (failed > 0 && added > 0) {
    showToast(
      `Added ${added} file${added === 1 ? "" : "s"}`,
      `${failed} failed`,
      "warning",
    );
  } else if (failed > 0) {
    showToast("No files added", `${failed} failed`, "error");
  } else {
    showToast(`Added ${added} file${added === 1 ? "" : "s"}`, "", "success");
  }
}

let dragDepth = 0;
function setDropActive(active) {
  $("#drop-overlay").classList.toggle("hidden", !active);
}

function hasDroppedFiles(e) {
  return e.dataTransfer && Array.from(e.dataTransfer.types || []).includes("Files");
}

document.addEventListener("dragenter", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  dragDepth++;
  setDropActive(true);
});
document.addEventListener("dragover", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  e.dataTransfer.dropEffect = "copy";
});
document.addEventListener("dragleave", (e) => {
  if (!hasDroppedFiles(e)) return;
  dragDepth = Math.max(0, dragDepth - 1);
  if (dragDepth === 0) setDropActive(false);
});
document.addEventListener("drop", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  dragDepth = 0;
  setDropActive(false);
  uploadDroppedFiles(e.dataTransfer.files);
});

// --- Network ---
async function refreshNetwork() {
  try {
    const h = await api("/network/health");
    const el = $("#network-health");
    const badge = $("#health-badge");
    el.innerHTML = `<pre>${escapeHtml(JSON.stringify(h, null, 2))}</pre>`;
    badge.textContent = h.status;
    badge.className = "badge " + (h.traffic_allowed ? "ok" : "bad");
  } catch (e) { log("network error: " + e.message); }
}

// --- Settings ---
async function refreshSettings() {
  try {
    const cfg = await api("/settings");
    $("#bw-dl").value = cfg.bandwidth.global_download ?? "";
    $("#bw-ul").value = cfg.bandwidth.global_upload ?? "";
    $("#bw-alt").checked = !!cfg.bandwidth.alt_enabled;
    $("#toast-seconds").value = String(Math.round(toastDisplayMs / 1000));
  } catch (e) { log("settings error: " + e.message); }
}

$("#save-bw-btn").addEventListener("click", async () => {
  try {
    const cfg = await api("/settings");
    cfg.bandwidth.global_download = parseInt($("#bw-dl").value, 10) || 0;
    cfg.bandwidth.global_upload = parseInt($("#bw-ul").value, 10) || 0;
    cfg.bandwidth.alt_enabled = $("#bw-alt").checked;
    await api("/settings", { method: "PATCH", headers: { "content-type": "application/json" }, body: JSON.stringify({ bandwidth: cfg.bandwidth }) });
    showToast("Bandwidth settings saved", "", "success");
  } catch (e) { showError("Save bandwidth failed", e); }
});

$("#save-toast-btn").addEventListener("click", () => {
  const ms = setToastDisplaySeconds($("#toast-seconds").value);
  $("#toast-seconds").value = String(Math.round(ms / 1000));
  showToast("Notification settings saved", "", "success");
});

// --- Watch ---
async function refreshWatch() {
  try {
    const hist = await api("/watch/history") || [];
    $("#watch-history").innerHTML = hist.map(h => `<div>${escapeHtml(h.path)} - ${escapeHtml(importStatus(h))}</div>`).join("");
  } catch (e) { log("watch error: " + e.message); }
}
function importStatus(item) {
  if (item.success === true) return "ok";
  if (item.error) return item.error;
  if (item.success === false) return "fail";
  return "";
}
$("#watch-scan-btn").addEventListener("click", async () => {
  try {
    await api("/watch/scan", { method: "POST" });
    showToast("Watch scan complete", "", "success");
    refreshWatch();
  } catch (e) { showError("Watch scan failed", e); }
});

// --- Logs ---
async function refreshLogs() {
  $("#log-stream").textContent = "Live event stream connecting...";
  try {
    const es = new EventSource(API + "/events");
    es.onmessage = (e) => { $("#log-stream").textContent += "\n" + e.data; };
    es.onerror = () => { $("#log-stream").textContent += "\n[event stream closed]"; };
  } catch (e) { log("events error: " + e.message); }
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
}
function cssToken(s) {
  return String(s ?? "").replace(/[^a-zA-Z0-9_-]/g, "");
}
function log(msg) {
  const el = $("#log-stream");
  if (el) el.textContent += "\n" + msg;
  else console.log(msg);
}

$("#search").addEventListener("input", refreshTorrents);

// --- Init ---
(async function init() {
  await refreshTorrents();
  await refreshNetwork();
  setInterval(refreshTorrents, 5000);
  setInterval(refreshNetwork, 10000);
})();
