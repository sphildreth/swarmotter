// SPDX-License-Identifier: Apache-2.0
import { $, $$, escapeHtml, fmtBytes, fmtRate, fmtRatio } from "./ui.js";
import { api } from "./api.js";

export const glyph = (name) =>
  `<svg class="glyph" aria-hidden="true" viewBox="0 0 24 24"><use href="#ico-${escapeHtml(name)}"></use></svg>`;
export const titleCase = (value) =>
  String(value ?? "Unknown")
    .replaceAll("_", " ")
    .replace(/^./, (c) => c.toUpperCase());
export function setText(selector, value) {
  const node = $(selector);
  if (node) node.textContent = value ?? "—";
}
export function badge(value, tone = "") {
  return `<span class="chip ${tone}">${escapeHtml(value)}</span>`;
}
export function metric(label, value, detail) {
  return `<div class="metric-card"><small>${escapeHtml(label)}</small><strong>${escapeHtml(value ?? "—")}</strong><small>${escapeHtml(detail)}</small></div>`;
}
export function selectPanel(attribute, selected, root = document) {
  root.querySelectorAll(`[data-${attribute}]`).forEach((node) => {
    const active = node.getAttribute(`data-${attribute}`) === selected;
    if (node.tagName === "BUTTON") node.setAttribute("aria-pressed", String(active));
    else node.hidden = !active;
  });
}
export function downloadText(text, filename, type = "text/plain") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const samples = [];
let pending;
let lastHealth = null;
let completeSize = null;
let sizeCount = null;
export function updateLibrarySize(query) {
  if (query?.total === query?.rows?.length) {
    completeSize = query.rows.reduce((sum, row) => sum + row.total_length, 0);
    sizeCount = query.total;
  } else if (query?.total !== sizeCount) completeSize = null;
  const size = fmtBytes(completeSize);
  setText("#tele-size", size || "—");
  $("#tele-size").title =
    query.rows.length === query.total
      ? "Size of the complete library"
      : size
        ? "Last complete library observation"
        : "Total unavailable for a partial library";
  return size;
}
function chartLine(points, key, width, height, max, from, to) {
  return points
    .map((sample) => {
      const x = ((sample.time - from) / (to - from || 1)) * width;
      const y = height - (sample[key] / max) * (height - 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}
function drawRates() {
  const end = samples.at(-1)?.time;
  if (!end) return;
  for (const key of ["down", "up"]) {
    const points = samples.slice(-24);
    const max = Math.max(1, ...points.map((sample) => sample[key])) * 1.12;
    const line = chartLine(points, key, 100, 24, max, points[0].time, end);
    if (points.length < 2) {
      $("#spark-" + key).innerHTML = "";
      continue;
    }
    $("#spark-" + key).innerHTML =
      `<defs><linearGradient id="spark-fill-${key}" x2="0" y2="1" gradientUnits="objectBoundingBox"><stop stop-color="currentColor" stop-opacity=".24"/><stop offset="1" stop-color="currentColor" stop-opacity="0"/></linearGradient></defs><polygon points="0,26 ${line} 100,26" fill="url(#spark-fill-${key})"/><polyline points="${line}" fill="none" stroke="currentColor" stroke-width="1.7"/>`;
  }
  const chart = $("#throughput-chart");
  if (!chart) return;
  const max = Math.max(1, ...samples.map((sample) => Math.max(sample.down, sample.up)));
  const third = max / 1048576 / 3;
  const unit = 10 ** Math.floor(Math.log10(Math.max(0.001, third)));
  const scale = 3 * unit * [1, 2, 5, 10].find((step) => step * unit >= third);
  const start = end - 900000;
  const series = ["down", "up"].map((key) => chartLine(samples, key, 900, 170, scale * 1048576, start, end));
  chart.innerHTML = `<div class="axis-y">${[scale, (scale * 2) / 3, scale / 3, 0].map((n) => `<span>${n} MiB/s</span>`).join("")}</div><svg viewBox="0 0 900 170" preserveAspectRatio="none" aria-label="Download and upload over the last 15 minutes"><defs><linearGradient id="throughput-fill" x2="0" y2="1"><stop stop-color="var(--accent)" stop-opacity=".26"/><stop offset="1" stop-color="var(--accent)" stop-opacity="0"/></linearGradient></defs><polygon fill="url(#throughput-fill)" points="${series[0].split(" ")[0].split(",")[0]},170 ${series[0]} 900,170"/>${series.map((line, i) => `<polyline fill="none" stroke="${i ? "var(--purple)" : "var(--accent)"}" stroke-width="${i ? 2 : 3}" points="${line}"/>`).join("")}</svg>`;
}

export function refreshTelemetry() {
  if (pending) return pending;
  const signal = AbortSignal.timeout(12000);
  if (lastHealth !== null && Date.now() - lastHealth > 15000) {
    setText("#tele-network", "Stale");
    $("#network-dot").dataset.status = "warning";
  }
  pending = Promise.all([
    api("/stats", { signal })
      .then((stats) => {
        for (const [id, value] of Object.entries({
          count: stats.torrent_count,
          downloads: stats.active_downloads,
          seeds: stats.active_seeds,
          paused: stats.paused,
          down: fmtRate(stats.download_rate),
          up: fmtRate(stats.upload_rate),
          ratio: stats.total_downloaded > 0 ? fmtRatio(stats.total_uploaded / stats.total_downloaded) : "—",
        }))
          setText("#tele-" + id, value);
        setText("#tele-size", stats.torrent_count === sizeCount ? fmtBytes(completeSize) || "—" : "—");
        $("#telemetry").removeAttribute("title");
        if (Number.isFinite(stats.download_rate) && Number.isFinite(stats.upload_rate)) {
          const time = Date.now();
          samples.push({ time, down: stats.download_rate, up: stats.upload_rate });
          while (samples.length > 181 || samples[0].time < time - 900000) samples.shift();
          drawRates();
        }
      })
      .catch((error) => {
        for (const id of ["count", "downloads", "seeds", "paused", "down", "up", "ratio", "size"])
          setText("#tele-" + id, "—");
        $("#telemetry").title = error.message;
      }),
    api("/network/health", { signal })
      .then((health) => {
        lastHealth = Date.now();
        $("#network-dot").removeAttribute("title");
        const allowed = health?.traffic_allowed;
        setText("#tele-network", allowed === true ? "Online" : allowed === false ? "Blocked" : "Unknown");
        setText("#tele-mode", titleCase(health?.mode));
        $("#network-dot").dataset.status = allowed === true ? "online" : "warning";
      })
      .catch((error) => {
        setText(
          "#tele-network",
          error.name === "TimeoutError" && lastHealth !== null ? "Stale" : "Unavailable",
        );
        setText("#tele-mode", "Unknown");
        $("#network-dot").dataset.status = "error";
        $("#network-dot").title = error.message;
      }),
  ]).finally(() => {
    pending = null;
  });
  return pending;
}
