// SPDX-License-Identifier: Apache-2.0
import {
  $,
  escapeHtml,
  fmtBytes,
  fmtRate,
  fmtRatio,
  fmtProgress,
  fmtUnixSeconds,
  renderKv,
  showError,
} from "./ui.js";
import { api } from "./api.js";
import { glyph, titleCase } from "./workspace.js";
import { openDetails } from "./details.js";

let torrent,
  files,
  trackers,
  stats,
  tab = "Overview",
  generation = 0,
  observation = 0;
export function fileGlyph(name) {
  if (/\.(mkv|mp4)$/i.test(name)) return /S\d\dE\d\d/i.test(name) ? "monitor" : "movie";
  if (/\.(flac|mp3)$/i.test(name)) return "music";
  return /\.(zip|tar|gz)$/i.test(name) ? "box" : "folder";
}
export async function inspectTorrent(row) {
  const request = ++generation;
  torrent = row;
  tab = "Overview";
  files = trackers = stats = null;
  render();
  const result = await Promise.allSettled(
    ["files", "trackers", "stats"].map((path) =>
      api(`/torrents/${encodeURIComponent(row.info_hash)}/${path}`),
    ),
  );
  if (request !== generation) return;
  [files, trackers, stats] = result.map((r) => (r.status === "fulfilled" ? r.value : null));
  render();
}
export async function syncInspector(rows, complete) {
  const revision = ++observation;
  if (!torrent) return;
  const next = rows.find((row) => row.info_hash === torrent.info_hash);
  if (next) {
    if (JSON.stringify(next) !== JSON.stringify(torrent)) {
      torrent = next;
      render();
    }
    return;
  }
  if (complete) {
    closeInspector();
    return;
  }
  const request = generation;
  try {
    const nextTorrent = await api("/torrents/" + encodeURIComponent(torrent.info_hash));
    if (request === generation && revision === observation) {
      torrent = nextTorrent;
      render();
    }
  } catch (error) {
    if (request !== generation || revision !== observation) return;
    if (error.status === 404) closeInspector();
    else $("#torrent-inspect .inspector-head small").textContent = "Update unavailable · " + error.message;
  }
}
function closeInspector() {
  generation++;
  torrent = null;
  $("#torrent-inspect").innerHTML =
    '<div class="inspector-head"><strong>Torrent inspector</strong></div><p class="empty">Select a torrent to inspect its transfer and storage details.</p>';
}
function render() {
  const t = torrent;
  if (!t) return;
  const focusedTab = $("#torrent-inspect")?.contains?.(document.activeElement)
    ? document.activeElement?.dataset?.tab
    : null;
  const fileList = files?.map((f) => [f.path, fmtBytes(f.length)]) ?? [];
  const trackerList = trackers?.map((r) => [r.url, titleCase(r.status)]) ?? [];
  const activity = [
    ["Added", fmtUnixSeconds(t.date_added)],
    ["Completed", t.date_completed ? fmtUnixSeconds(t.date_completed) : "—"],
    ["Downloaded", fmtBytes(t.downloaded)],
    ["Uploaded", fmtBytes(t.uploaded)],
  ];
  const remaining =
    t.rate_down > 0 && t.total_length > t.bytes_completed
      ? Math.ceil((t.total_length - t.bytes_completed) / t.rate_down)
      : null;
  const eta = remaining == null ? "—" : Math.floor(remaining / 60) + "m " + (remaining % 60) + "s";
  const recent = [
    [t.date_added, "Torrent added"],
    [t.date_completed, "Torrent completed"],
    ...(trackers || []).map((r) => [r.last_success, "Tracker replied · " + r.url]),
  ]
    .filter(([time]) => time)
    .sort((a, b) => b[0] - a[0])
    .slice(0, 3);
  const pieces = t.piece_count ? Math.min(1, t.pieces_have / t.piece_count) : null;
  const iconRows = (rows) =>
    `<dl class="kv">${rows.map(([key, value, icon]) => `<dt>${escapeHtml(key)}</dt><dd>${glyph(icon)}${escapeHtml(value ?? "—")}</dd>`).join("")}</dl>`;
  const overview = `<div class="ti-section"><b>Piece completion</b><span class="float-end">${pieces == null ? "—" : (100 * pieces).toFixed(1) + "%"}</span>
    <svg class="piece-chart" viewBox="0 0 360 40" preserveAspectRatio="none" role="img" aria-label="Aggregate completed pieces"><defs><pattern id="pieces-grid" width="10" height="10" patternUnits="userSpaceOnUse"><rect width="8" height="8" rx=".5" fill="var(--accent)"/></pattern></defs><defs><pattern id="missing-grid" width="10" height="10" patternUnits="userSpaceOnUse"><rect width="8" height="8" fill="var(--border)"/></pattern></defs><rect width="360" height="40" fill="url(#missing-grid)"/><rect width="${(pieces || 0) * 360}" height="40" fill="url(#pieces-grid)" color="var(--accent)"/></svg></div>
    <div class="ti-section">${iconRows([
      ["Status", titleCase(t.state), "activity"],
      ["Health", titleCase(t.health?.label), "chart"],
      ["Download", fmtRate(t.rate_down), "down"],
      ["Upload", fmtRate(t.rate_up), "up"],
      ["Ratio", fmtRatio(t.ratio), "gauge"],
      ["Peers", `${t.active_peer_workers} (${t.known_peers})`, "network"],
      ["Seeds", stats?.seeders ?? "—", "network"],
      ["ETA", eta, "clock"],
    ])}</div>
    <div class="ti-section ti-storage"><button class="icon-only copy-path" aria-label="Copy save path" data-copy-path ${t.download_dir ? "" : "disabled"}>${glyph("copy")}</button>${iconRows(
      [
        ["Save path", t.download_dir || "Default", "folder"],
        ["Total size", fmtBytes(t.total_length), "storage"],
        ["Files", files?.length ?? "—", "file"],
        ["Piece length", fmtBytes(t.piece_length), "layers"],
        ["Pieces", `${t.piece_count} (${t.pieces_have} completed)`, "box"],
        ["Created", fmtUnixSeconds(t.creation_date) || "—", "clock"],
        ["Added", fmtUnixSeconds(t.date_added), "clock"],
      ],
    )}</div>
    <div class="ti-section ti-trackers"><b>Trackers (${trackers?.length ?? "—"})</b>${(trackers || [])
      .slice(0, 3)
      .map(
        (r) =>
          `<div class="tracker-line"><i data-ok="${["working", "ok"].includes(r.status)}"></i><span>${escapeHtml(r.url)}</span><small>${escapeHtml(titleCase(r.status))}</small></div>`,
      )
      .join("")}</div>
    <div class="ti-section"><b>Recent activity</b>${recent.map(([time, event]) => `<div class="activity-line"><time>${escapeHtml(new Date(time * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }))}</time>${glyph("activity")}<span>${escapeHtml(event)}</span></div>`).join("")}</div>`;
  const body =
    tab === "Overview"
      ? overview
      : renderKv(tab === "Files" ? fileList : tab === "Trackers" ? trackerList : activity);
  $("#torrent-inspect").innerHTML =
    `<div class="inspector-head">${glyph(fileGlyph(t.name))}<div><strong>${escapeHtml(t.name)}</strong><small>${fmtBytes(t.total_length)} · ${escapeHtml(titleCase(t.state))} (${fmtProgress(t.bytes_completed, t.total_length) || "—"})</small></div><button class="icon-only" aria-label="Close torrent inspector" data-close>${glyph("close")}</button></div>
    <div class="detail-tabs">${["Overview", "Files", "Trackers", "Activity"].map((item) => `<button data-tab="${item}" aria-pressed="${item === tab}">${item}</button>`).join("")}</div>
    <div class="ti-body">${body || '<p class="empty">No data available.</p>'}</div><button class="details-open" data-full>Full torrent details ${glyph("right")}</button>`;
  if (focusedTab) $("#torrent-inspect [data-tab='" + focusedTab + "']")?.focus({ preventScroll: true });
}
$("#torrent-inspect")?.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button || !torrent) return;
  if ("copyPath" in button.dataset) {
    try {
      await navigator.clipboard.writeText(torrent.download_dir);
    } catch (e) {
      showError("Copy save path failed", e);
    }
  }
  if (button.dataset.tab) {
    tab = button.dataset.tab;
    render();
  }
  if ("close" in button.dataset) closeInspector();
  if ("full" in button.dataset) openDetails(torrent.info_hash);
});
