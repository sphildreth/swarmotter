// SPDX-License-Identifier: Apache-2.0
// SwarmOtter Web UI controller. Consumes the same REST API as external tools.
const API = "/api/v1";
let currentHash = null;

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function fmtBytes(n) {
  if (!n || n <= 0) return "0 B";
  const u = ["B","KB","MB","GB","TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(i === 0 ? 0 : 1) + " " + u[i];
}
function fmtRate(n) { return fmtBytes(n) + "/s"; }

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
    const tbody = $("#torrent-table tbody");
    tbody.innerHTML = "";
    const filter = $("#search").value.toLowerCase();
    list.filter(t => t.name.toLowerCase().includes(filter)).forEach(t => {
      const tr = document.createElement("tr");
      tr.className = "torrent";
      tr.dataset.hash = t.info_hash;
      tr.innerHTML = `
        <td>${escapeHtml(t.name)}</td>
        <td>${fmtBytes(t.total_length)}</td>
        <td><progress value="${t.bytes_completed}" max="${t.total_length}"></progress> ${((t.total_length ? t.bytes_completed / t.total_length : 0) * 100).toFixed(1)}%</td>
        <td>${t.state}</td>
        <td>${renderHealth(t.health)}</td>
        <td>${fmtRate(t.rate_down)}</td>
        <td>${fmtRate(t.rate_up)}</td>
        <td>${t.ratio.toFixed(2)}</td>
        <td>${renderPeerCount(t)}</td>
        <td>
          <button data-act="pause">Pause</button>
          <button data-act="resume">Resume</button>
          <button data-act="recheck">Recheck</button>
          <button data-act="remove" class="danger">Remove</button>
        </td>`;
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === "BUTTON") return;
        openDetails(t.info_hash);
      });
      tbody.appendChild(tr);
    });
    $("#stats-summary").textContent = `${stats.torrent_count} torrents · ${fmtRate(stats.download_rate)} down · ${fmtRate(stats.upload_rate)} up`;
    bindActionButtons();
  } catch (e) {
    log("torrent list error: " + e.message);
  }
}

function renderPeerCount(t) {
  const active = Number.isFinite(t.active_peer_workers) ? t.active_peer_workers : 0;
  const known = Number.isFinite(t.known_peers) ? t.known_peers : 0;
  if (known === 0) return String(active);
  return `${active}/${known}`;
}

function bindActionButtons() {
  $$("#torrent-table tbody button").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const tr = btn.closest("tr");
      const hash = tr.dataset.hash;
      const act = btn.dataset.act;
      try {
        if (act === "pause") await api(`/torrents/${hash}/pause`, { method: "POST" });
        else if (act === "resume") await api(`/torrents/${hash}/resume`, { method: "POST" });
        else if (act === "recheck") await api(`/torrents/${hash}/recheck`, { method: "POST" });
        else if (act === "remove") {
          if (confirm("Remove torrent? Delete data too?")) await api(`/torrents/${hash}?delete_data=true`, { method: "DELETE" });
          else await api(`/torrents/${hash}`, { method: "DELETE" });
        }
        refreshTorrents();
      } catch (e) { alert(e.message); }
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
    case "excellent": return "Excellent";
    case "good": return "Good";
    case "fair": return "Fair";
    case "poor": return "Poor";
    case "critical": return "Critical";
    case "stalled": return "Stalled";
    case "network_blocked": return "Blocked";
    case "paused": return "Paused";
    case "complete": return "Complete";
    default: return "Unknown";
  }
}

function renderHealth(h) {
  if (!h) return "";
  const label = h.label || "unknown";
  const score = h.score == null ? 0 : h.score;
  const bars = Math.max(0, Math.min(5, h.bars || 0));
  const reasons = (h.reasons || []).join("; ");
  const tooltip = `${healthLabelName(label)} — ${score}/100${reasons ? ": " + reasons : ""}`;
  const srText = `Health: ${healthLabelName(label)}, ${score} out of 100`;
  let barsHtml = "";
  for (let i = 0; i < 5; i++) {
    barsHtml += `<span class="bar${i < bars ? " active" : ""}"></span>`;
  }
  return `<div class="torrent-health health-${escapeHtml(label)}" title="${escapeHtml(tooltip)}">`
    + `<span class="sr-only">${escapeHtml(srText)}</span>`
    + `<span class="health-bars" aria-hidden="true">${barsHtml}</span>`
    + `<span class="health-label">${escapeHtml(healthLabelName(label))}</span>`
    + `</div>`;
}

function renderDetailsHealth(h) {
  if (!h) { $("#details-health").innerHTML = ""; return; }
  const label = h.label || "unknown";
  const score = h.score == null ? 0 : h.score;
  const bars = Math.max(0, Math.min(5, h.bars || 0));
  const reasons = (h.reasons || []).map(r => `<li>${escapeHtml(r)}</li>`).join("");
  const subs = `
    <table class="health-subscores">
      <thead><tr><th>Component</th><th>Score</th></tr></thead>
      <tbody>
        <tr><td>Availability</td><td>${h.availability_score == null ? 0 : h.availability_score}/100</td></tr>
        <tr><td>Throughput</td><td>${h.throughput_score == null ? 0 : h.throughput_score}/100</td></tr>
        <tr><td>Peers</td><td>${h.peer_score == null ? 0 : h.peer_score}/100</td></tr>
        <tr><td>Stability</td><td>${h.stability_score == null ? 0 : h.stability_score}/100</td></tr>
        <tr><td>Discovery</td><td>${h.discovery_score == null ? 0 : h.discovery_score}/100</td></tr>
      </tbody>
    </table>`;
  $("#details-health").innerHTML = `
    <h3>Health</h3>
    ${renderHealth(h)}
    <p class="muted">Score ${score}/100. Health answers: can this torrent complete, and is it downloading well right now?</p>
    <ul class="health-list">${reasons}</ul>
    ${subs}
  `;
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
    $$("#files-table .prio").forEach(sel => sel.value = files.find(f => f.index == sel.dataset.fi).priority);
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
      tr.innerHTML = `<td>${escapeHtml(p.address)}</td><td>${escapeHtml(p.client || "")}</td><td>${(p.progress * 100).toFixed(0)}%</td><td>${fmtRate(p.rate_down)}</td><td>${fmtRate(p.rate_up)}</td>`;
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
      tr.innerHTML = `<td>${escapeHtml(t.url)}</td><td>${t.tier}</td><td>${t.status}</td><td>${t.seeders}</td><td>${t.leechers}</td>`;
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
    $("#add-magnet-result").textContent = "Added: " + h;
    $("#magnet-input").value = "";
  } catch (e) { $("#add-magnet-result").textContent = "Error: " + e.message; }
});

$("#add-file-btn").addEventListener("click", async () => {
  try {
    const file = $("#torrent-file").files[0];
    if (!file) { $("#add-file-result").textContent = "Choose a file"; return; }
    const h = await uploadTorrentFile(file);
    $("#add-file-result").textContent = "Added: " + h;
  } catch (e) { $("#add-file-result").textContent = "Error: " + e.message; }
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
  const status = $("#drop-status");
  if (torrents.length === 0) {
    status.textContent = "No .torrent file found";
    return;
  }
  status.textContent = `Adding ${torrents.length} file${torrents.length === 1 ? "" : "s"}...`;
  let added = 0;
  for (const file of torrents) {
    try {
      await uploadTorrentFile(file);
      added++;
    } catch (e) {
      status.textContent = `Error adding ${file.name}: ${e.message}`;
      log(`drop upload error (${file.name}): ${e.message}`);
      return;
    }
  }
  status.textContent = `Added ${added} file${added === 1 ? "" : "s"}`;
  refreshTorrents();
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
    $("#bw-dl").value = cfg.bandwidth.global_download || 0;
    $("#bw-ul").value = cfg.bandwidth.global_upload || 0;
    $("#bw-alt").checked = !!cfg.bandwidth.alt_enabled;
  } catch (e) { log("settings error: " + e.message); }
}

$("#save-bw-btn").addEventListener("click", async () => {
  try {
    const cfg = await api("/settings");
    cfg.bandwidth.global_download = parseInt($("#bw-dl").value, 10) || 0;
    cfg.bandwidth.global_upload = parseInt($("#bw-ul").value, 10) || 0;
    cfg.bandwidth.alt_enabled = $("#bw-alt").checked;
    await api("/settings", { method: "PATCH", headers: { "content-type": "application/json" }, body: JSON.stringify({ bandwidth: cfg.bandwidth }) });
    $("#save-bw-result").textContent = "Saved";
  } catch (e) { $("#save-bw-result").textContent = "Error: " + e.message; }
});

// --- Watch ---
async function refreshWatch() {
  try {
    const hist = await api("/watch/history") || [];
    $("#watch-history").innerHTML = hist.map(h => `<div>${escapeHtml(h.path)} - ${h.success ? "ok" : "fail"}</div>`).join("");
  } catch (e) { log("watch error: " + e.message); }
}
$("#watch-scan-btn").addEventListener("click", async () => {
  try { await api("/watch/scan", { method: "POST" }); refreshWatch(); } catch (e) { alert(e.message); }
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
  return String(s || "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
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
