// SPDX-License-Identifier: Apache-2.0

import { state, TORRENT_QUERY_STORAGE_KEY, TORRENT_DEFAULT_PER_PAGE, TORRENT_MAX_PER_PAGE, TORRENT_SORT_OPTIONS, TORRENT_TABLE_TO_QUERY_SORT, TORRENT_QUERY_TO_TABLE_SORT, TORRENT_ACTIONS, EVENT_KINDS, MAX_LOG_LINES, watchHistoryUi } from "./state.js";
import * as ui from "./ui.js";
import * as requests from "./api.js";
const { $, $$, showToast, showError, finiteNumber, fmtCount, fmtBytes, fmtRate, fmtRatio, fmtPercentFromFraction, fmtPercent, fmtProgress, fmtUnixSeconds, renderProgressCell, renderHealth, renderHealthSummary, fmtScore, renderStatus, renderKv, renderCheckList, escapeHtml, cssToken, log, setToastDisplaySeconds } = ui;
const { api, apiFetch, responseErrorMessage, saveApiToken } = requests;

let eventsDependencies = { refreshSettings: async () => {} };
export function setEventsDependencies(dependencies) { eventsDependencies = { ...eventsDependencies, ...dependencies }; }

export async function refreshNetwork() {
  try {
    const d = await api("/network/diagnostics");
    const h = d.health || {};
    $("#network-summary").innerHTML = `
      <h3>Network summary</h3>
      ${renderKv([
        ["Mode", h.mode],
        ["Status", h.status],
        ["Traffic allowed", String(!!h.traffic_allowed)],
        ["Listen port", d.listen_port],
        ["DHT port", d.dht_port],
        ["Peer transports", `${d.utp_enabled ? "TCP + uTP" : "TCP only"} (${d.utp_prefer_tcp ? "TCP first" : "uTP first"})`],
      ])}
      <p class="muted">${escapeHtml(h.detail || "")}</p>`;
    $("#network-health").innerHTML = `<h3>Health payload</h3><pre class="health-payload">${escapeHtml(JSON.stringify(h, null, 2))}</pre>`;
    $("#network-config").innerHTML = `
      <h3>Configuration</h3>
      ${renderKv([
        ["Required interface", h.required_interface || "not set"],
        ["Required IPv4", h.required_source_ipv4 || "not set"],
        ["Required IPv6", h.required_source_ipv6 || "not set"],
        ["Network IPv6", String(!!h.allow_ipv6)],
        ["Torrent IPv6", String(!!d.torrent_allow_ipv6)],
        ["Fail closed", String(!!h.fail_closed)],
      ])}`;
    $("#network-interfaces").innerHTML = `
      <h3>Interfaces</h3>
      <table><thead><tr><th>Name</th><th>Status</th><th>Families</th><th>Addresses</th></tr></thead>
      <tbody>${(d.interfaces || []).map(iface => `
        <tr>
          <td>${iface.selected ? "<strong>" : ""}${escapeHtml(iface.name)}${iface.selected ? "</strong>" : ""}</td>
          <td>${escapeHtml(iface.status)}</td>
          <td>${iface.has_ipv4 ? "IPv4" : ""}${iface.has_ipv4 && iface.has_ipv6 ? " + " : ""}${iface.has_ipv6 ? "IPv6" : ""}</td>
          <td>${escapeHtml((iface.addresses || []).join(", "))}</td>
        </tr>`).join("")}</tbody></table>`;
    $("#network-originality").innerHTML = `
      <h3>Containment matrix</h3>
      ${renderCheckList(d.containment_matrix || [])}
      <h3>Checks</h3>
      ${renderCheckList(d.checks || [])}`;
  } catch (e) { log("network error: " + e.message); }
}
export async function refreshWatch() {
  try {
    const status = await api("/watch/status");
    renderWatch(status);
    return status;
  } catch (e) { log("watch error: " + e.message); }
}

export function renderWatch(status, scanDetail = "") {
  const folders = status?.folders || [];
  const imports = status?.recent_imports || [];
  const pending = folders.reduce((sum, folder) => sum + (finiteNumber(folder.pending_torrent_files) || 0), 0);
  const scanButton = $("#watch-scan-btn");
  if (scanButton) {
    scanButton.disabled = folders.length === 0;
    scanButton.title = folders.length === 0 ? "No watch folders configured" : "";
  }
  $("#watch-config").innerHTML = `
    <h3>Watch config</h3>
    ${folders.length === 0 ? `<p class="muted">No watch folders configured.</p>` : `
      <table>
        <thead><tr><th>Path</th><th>Status</th><th>Pending</th><th>Defaults</th></tr></thead>
        <tbody>${folders.map(folder => renderWatchFolderRow(folder)).join("")}</tbody>
      </table>`}`;
  $("#watch-scan-result").innerHTML = `
    <h3>Scan result</h3>
    ${renderKv([
      ["Automatic watch", status?.enabled ? "enabled" : "not configured"],
      ["Configured folders", String(folders.length)],
      ["Pending .torrent files", String(pending)],
      ["Scan Now action", "scan configured watch folders, import .torrent files, then apply delete/archive/failure handling"],
      ["Last manual scan", scanDetail],
    ])}`;
  $("#watch-history").innerHTML = `
    <h3>Scan history</h3>
    ${imports.length === 0 ? `<p class="muted">No imports recorded.</p>` : `
      <table>
        <thead><tr><th>Path</th><th>Outcome</th><th>Status</th><th>Info hash</th><th>Detail</th></tr></thead>
        <tbody>${imports.slice().reverse().slice(0, 40).map(item => `
          <tr>
            <td>${escapeHtml(item.path)}</td>
            <td>${escapeHtml(watchHistoryUi.outcomeLabel(item))}</td>
            <td>${renderStatus(watchHistoryUi.statusKey(item))}</td>
            <td>${escapeHtml(item.info_hash_hex || "")}</td>
            <td>${escapeHtml(watchHistoryUi.detail(item))}</td>
          </tr>`).join("")}</tbody>
      </table>`}`;
}

export function renderWatchFolderRow(folder) {
  const cfg = folder.config || {};
  const defaults = [
    cfg.download_dir ? `dir ${cfg.download_dir}` : "",
    cfg.label ? `label ${cfg.label}` : "",
    cfg.start_behavior ? `start ${cfg.start_behavior}` : "",
    cfg.recursive ? "recursive" : "",
    cfg.delete_after_import ? "delete after import" : "leave source",
    cfg.archive_dir ? `archive ${cfg.archive_dir}` : "",
    cfg.failure_dir ? `failures ${cfg.failure_dir}` : "",
  ].filter(Boolean).join(", ");
  return `
    <tr>
      <td>${escapeHtml(cfg.path || "")}</td>
      <td>${renderStatus(folder.exists ? "ok" : "warning")}</td>
      <td>${fmtCount(folder.pending_torrent_files)}</td>
      <td>${escapeHtml(defaults)}</td>
    </tr>`;
}

$("#watch-scan-btn").addEventListener("click", async () => {
  try {
    const before = await api("/watch/status");
    const beforeCount = (before.recent_imports || []).length;
    await api("/watch/scan", { method: "POST" });
    const after = await api("/watch/status");
    const imported = Math.max(0, (after.recent_imports || []).length - beforeCount);
    const detail = `${imported} import result${imported === 1 ? "" : "s"} recorded`;
    renderWatch(after, detail);
    showToast("Watch scan complete", detail, "success");
  } catch (e) { showError("Watch scan failed", e); }
});

// --- Logs ---
export async function refreshLogs() {
  try {
    const snapshot = await api("/logs/recent?lines=200");
    renderLogSnapshot(snapshot);
    connectEventStream();
  } catch (e) { log("events error: " + e.message); }
}

$("#refresh-logs-btn").addEventListener("click", refreshLogs);

export function renderLogSnapshot(snapshot) {
  const lines = snapshot?.lines || [];
  const source = snapshot?.path
    ? `${snapshot.path}${snapshot.truncated ? " (tail)" : ""}`
    : "live event stream";
  $("#log-source").textContent = snapshot?.enabled ? source : `${source} unavailable`;
  $("#log-stream").textContent = lines.length ? lines.join("\n") : "[no recent log lines]";
}

export function connectEventStream() {
  if (state.logEventStreamController) return;
  readEventStream();
}

export async function readEventStream() {
  const controller = new AbortController();
  state.logEventStreamController = controller;
  try {
    const res = await apiFetch("/events", {
      headers: { accept: "text/event-stream" },
      signal: controller.signal,
    });
    if (!res.ok || !res.body) throw new Error(await responseErrorMessage(res));
    appendLogLine("[event stream connected]");
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      buffer = drainEventStreamBuffer(buffer);
    }
  } catch (e) {
    if (!controller.signal.aborted) {
      const nowMs = Date.now();
      if (nowMs - state.lastEventStreamErrorAt > 10000) {
        appendLogLine("[event stream unavailable] " + e.message);
        state.lastEventStreamErrorAt = nowMs;
      }
    }
  } finally {
    if (state.logEventStreamController === controller) {
      state.logEventStreamController = null;
      if (!$("#view-logs").classList.contains("hidden")) {
        window.setTimeout(connectEventStream, 10000);
      }
    }
  }
}

export function drainEventStreamBuffer(buffer) {
  let sep;
  while ((sep = buffer.indexOf("\n\n")) >= 0) {
    const raw = buffer.slice(0, sep);
    buffer = buffer.slice(sep + 2);
    dispatchEventStreamBlock(raw);
  }
  return buffer;
}

export function dispatchEventStreamBlock(raw) {
  let kind = "message";
  const data = [];
  for (const line of raw.split(/\r?\n/)) {
    if (line.startsWith("event:")) kind = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
  }
  if (EVENT_KINDS.includes(kind)) appendEventLine(kind, data.join("\n"));
}

export function appendEventLine(kind, raw) {
  let event = null;
  try { event = JSON.parse(raw); } catch {}
  const hash = event?.info_hash ? ` ${event.info_hash}` : "";
  const payload = event?.payload && Object.keys(event.payload).length
    ? " " + JSON.stringify(event.payload)
    : "";
  appendLogLine(`[event] ${kind}${hash}${payload}`);
  if (kind === "daemon_health_changed") refreshDoctorBadge();
  if (kind === "network_status_changed" && !$("#view-network").classList.contains("hidden")) refreshNetwork();
  if ((kind === "watch_folder_imported" || kind === "watch_folder_failed") && !$("#view-watch").classList.contains("hidden")) refreshWatch();
  if (kind === "settings_changed" && !$("#view-settings").classList.contains("hidden")) eventsDependencies.refreshSettings();
}

export function appendLogLine(line) {
  const el = $("#log-stream");
  if (!el) return;
  const current = el.textContent && !el.textContent.startsWith("[no recent")
    ? el.textContent.split("\n")
    : [];
  current.push(line);
  while (current.length > MAX_LOG_LINES) current.shift();
  el.textContent = current.join("\n");
  el.scrollTop = el.scrollHeight;
}

// --- Doctor ---
export async function refreshDoctor() {
  try {
    const report = await api("/doctor");
    const [version, storageRoots] = await Promise.all([
      api("/version").catch((e) => {
        log("version error: " + e.message);
        return null;
      }),
      api("/storage/roots").catch((e) => {
        log("storage roots error: " + e.message);
        return { error: e.message };
      }),
    ]);
    renderDoctor(report, version, storageRoots);
    updateHealthBadge(report);
    return report;
  } catch (e) {
    updateHealthBadge({ level: "invalid", summary: e.message || String(e), checks: [] });
    log("doctor error: " + e.message);
  }
}

export async function refreshDoctorBadge() {
  try {
    updateHealthBadge(await api("/doctor"));
  } catch (e) {
    updateHealthBadge({ level: "invalid", summary: e.message || String(e), checks: [] });
  }
}

export function renderStorageRootWarnings(root) {
  const warnings = Array.isArray(root?.warnings) ? root.warnings.filter(Boolean) : [];
  if (!warnings.length) return "";
  const items = warnings.map(w => `<li>${escapeHtml(String(w))}</li>`).join("");
  return `<ul class="storage-root-warnings compact-list">${items}</ul>`;
}

export function renderDoctorStorageRoots(storageRoots = {}) {
  if (!storageRoots || storageRoots.error) {
    return `
      <h3>Storage diagnostics</h3>
      <p class="muted">${escapeHtml(storageRoots?.error || "Storage diagnostics are unavailable.")}</p>
    `;
  }
  const roots = Array.isArray(storageRoots.roots) ? storageRoots.roots : [];
  const generatedAt = fmtUnixSeconds(storageRoots.generated_at);
  const header = renderKv([
    ["Minimum free bytes", fmtBytes(storageRoots.minimum_free_space_bytes)],
    ["Minimum free percent", fmtPercent(storageRoots.minimum_free_space_percent)],
    ["Generated", generatedAt],
  ]);
  if (!roots.length) {
    return `
      <h3>Storage diagnostics</h3>
      ${header}
      <p class="muted">No storage root diagnostics were returned.</p>
    `;
  }
  const rows = roots.map((root) => {
    const roles = Array.isArray(root?.roles) ? root.roles.join(", ") : "";
    const free = `${fmtBytes(root.free_space_bytes)} / ${fmtBytes(root.available_space_bytes)}`;
    const total = fmtBytes(root.total_space_bytes);
    const required = fmtBytes(root.required_free_space_bytes);
    const warnings = renderStorageRootWarnings(root);
    const status = [
      root.exists ? "exists" : "missing",
      root.is_directory ? "dir" : "file",
      root.writable ? "writable" : "read-only",
    ].filter(Boolean).join(", ");
    return `
      <tr>
        <td>${escapeHtml(root.path || "")}</td>
        <td>${escapeHtml(roles || "")}</td>
        <td>${escapeHtml(status)}</td>
        <td>${escapeHtml(root.filesystem_type || "")}</td>
        <td>${escapeHtml(total ? `${total}` : "")}</td>
        <td>free ${escapeHtml(free)}</td>
        <td>${escapeHtml(required || "")}</td>
        <td>${renderStatus(root.reserve_satisfied ? "ok" : "warning")}</td>
        <td>${fmtCount(root.torrent_count)}</td>
        <td>${fmtCount(root.active_torrents)}</td>
        <td>${fmtRate(root.active_write_rate)} / ${fmtRate(root.active_recheck_rate)}</td>
        <td>${warnings || ""}</td>
      </tr>`;
  }).join("");
  return `
    <h3>Storage diagnostics</h3>
    ${header}
    <table class="storage-root-table">
      <thead>
        <tr>
          <th>Path</th>
          <th>Roles</th>
          <th>State</th>
          <th>Filesystem</th>
          <th>Total</th>
          <th>Free / Available</th>
          <th>Required free</th>
          <th>Reserve</th>
          <th>Torrents</th>
          <th>Active</th>
          <th>Rates</th>
          <th>Warnings</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

export function renderDoctor(report, version = null, storageRoots = null) {
  $("#doctor-summary").innerHTML = `
    <h3>Health summary</h3>
    ${renderKv([
      ["Overall", levelLabel(report.level)],
      ["Summary", report.summary || ""],
      ["Checks", String((report.checks || []).length)],
    ])}`;
  $("#doctor-storage").innerHTML = renderDoctorStorageRoots(storageRoots);
  $("#doctor-application").innerHTML = `
    <h3>Application</h3>
    ${renderKv([
      ["Name", version?.name || "SwarmOtter"],
      ["Version", version?.version || "unknown"],
      ["Commit", version?.commit || "unknown"],
      ["Target", version?.target || "unknown"],
    ])}`;
  $("#doctor-checks").innerHTML = `
    <h3>Checks</h3>
    ${renderCheckList(report.checks || [])}`;
}

export function updateHealthBadge(report) {
  const badge = $("#health-badge");
  if (!badge) return;
  const level = report?.level || "warning";
  badge.classList.remove("ok", "warn", "bad");
  if (level === "ok") badge.classList.add("ok");
  else if (level === "warning") badge.classList.add("warn");
  else badge.classList.add("bad");
  badge.textContent = levelLabel(level);
  badge.title = report?.summary || "";
}
