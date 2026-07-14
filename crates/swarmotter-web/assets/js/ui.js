// SPDX-License-Identifier: Apache-2.0

import { state, DEFAULT_TOAST_DISPLAY_MS, MAX_TOAST_DISPLAY_MS, MAX_VISIBLE_TOASTS, TOAST_DISPLAY_STORAGE_KEY, THEME_STORAGE_KEY, THEME_DARK, THEME_LIGHT, DEFAULT_THEME } from "./state.js";

let themeRefreshHandler = () => {};
let logHandler = null;
export function setThemeRefreshHandler(handler) { themeRefreshHandler = handler || (() => {}); }
export function setLogHandler(handler) { logHandler = handler || null; }
function refreshTorrentTableTheme(theme) { themeRefreshHandler(theme); }
export const $ = (sel) => document.querySelector(sel);
export const $$ = (sel) => Array.from(document.querySelectorAll(sel));

export function loadThemePreference() {
  try {
    return normalizeTheme(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    return DEFAULT_THEME;
  }
}

export function normalizeTheme(rawTheme) {
  return rawTheme === THEME_LIGHT || rawTheme === THEME_DARK
    ? rawTheme
    : DEFAULT_THEME;
}

export function applyTheme(theme, { persist = true } = {}) {
  const next = normalizeTheme(theme);
  state.currentTheme = next;
  document.documentElement.dataset.theme = next;
  refreshTorrentTableTheme(next);
  const button = $("#theme-toggle");
  if (button) {
    button.dataset.theme = next;
    const label = next === THEME_DARK ? "Switch to light theme" : "Switch to dark theme";
    button.setAttribute("aria-label", label);
    button.title = label;
  }
  if (!persist) return;
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, next);
  } catch {}
}

export function toggleTheme() {
  applyTheme(state.currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK);
}

export function loadToastDisplayMs() {
  try {
    const raw = window.localStorage.getItem(TOAST_DISPLAY_STORAGE_KEY);
    return normalizeToastDurationMs(raw);
  } catch {
    return DEFAULT_TOAST_DISPLAY_MS;
  }
}

export function normalizeToastDurationMs(value) {
  const ms = Number(value);
  return Number.isFinite(ms)
    ? Math.max(1000, Math.min(MAX_TOAST_DISPLAY_MS, Math.round(ms)))
    : DEFAULT_TOAST_DISPLAY_MS;
}

export function setToastDisplaySeconds(seconds) {
  const n = Number(seconds);
  const ms = Number.isFinite(n)
    ? normalizeToastDurationMs(n * 1000)
    : DEFAULT_TOAST_DISPLAY_MS;
  state.toastDisplayMs = ms;
  try { window.localStorage.setItem(TOAST_DISPLAY_STORAGE_KEY, String(ms)); } catch {}
  return ms;
}

export function showToast(title, message = "", type = "info", durationMs = state.toastDisplayMs) {
  const region = $("#toast-region");
  if (!region) return;
  const safeDurationMs = normalizeToastDurationMs(durationMs);
  while (region.children.length >= MAX_VISIBLE_TOASTS) {
    region.firstElementChild.remove();
  }
  const toast = document.createElement("div");
  toast.className = "toast " + cssToken(type || "info");
  toast.setAttribute("role", type === "error" ? "alert" : "status");
  toast.tabIndex = 0;
  toast.innerHTML = `
    <div class="toast-title">${escapeHtml(title)}</div>
    ${message ? `<div class="toast-message">${escapeHtml(message)}</div>` : ""}`;
  region.appendChild(toast);
  const remove = () => {
    window.clearTimeout(timer);
    toast.remove();
  };
  const timer = window.setTimeout(remove, safeDurationMs);
  toast.addEventListener("click", remove);
  toast.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") remove();
  });
}

export function showError(title, error) {
  showToast(title, error && error.message ? error.message : String(error || ""), "error");
}

export function finiteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function fmtCount(value) {
  const n = finiteNumber(value);
  return n === null ? "" : String(n);
}

export function fmtBytes(n) {
  n = finiteNumber(n);
  if (n === null) return "";
  if (n <= 0) return "0 B";
  const u = ["B","KB","MB","GB","TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(i === 0 ? 0 : 1) + " " + u[i];
}
export function fmtRate(n) {
  const bytes = fmtBytes(n);
  return bytes ? bytes + "/s" : "";
}
export function fmtRatio(n) {
  n = finiteNumber(n);
  return n === null ? "" : n.toFixed(2);
}
export function fmtPercentFromFraction(n, digits = 1) {
  n = finiteNumber(n);
  return n === null ? "" : (n * 100).toFixed(digits) + "%";
}
export function fmtPercent(value) {
  value = finiteNumber(value);
  return value === null ? "" : `${value}%`;
}
export function fmtProgress(bytesCompleted, totalLength) {
  const completed = finiteNumber(bytesCompleted);
  const total = finiteNumber(totalLength);
  if (completed === null || total === null || total <= 0) return "";
  return (Math.min(completed, total) / total * 100).toFixed(1) + "%";
}
export function fmtUnixSeconds(seconds) {
  const value = finiteNumber(seconds);
  if (value === null) return "";
  const timestamp = new Date(value * 1000);
  return Number.isNaN(timestamp.getTime()) ? "" : timestamp.toLocaleString();
}
export function renderProgressCell(bytesCompleted, totalLength) {
  const completed = finiteNumber(bytesCompleted);
  const total = finiteNumber(totalLength);
  if (completed === null || total === null || total <= 0) return "";
  const safeCompleted = Math.min(completed, total);
  return `<progress value="${safeCompleted}" max="${total}"></progress> ${fmtProgress(safeCompleted, total)}`;
}

export function healthLabelName(label) {
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

export function renderHealth(h) {
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
export function fmtScore(value) {
  const score = fmtCount(value);
  return score ? `${score}/100` : "";
}

export function renderHealthSummary(h) {
  const score = fmtScore(h.score);
  return score ? `Score ${score}. Health answers: can this torrent complete, and is it downloading well right now?` : "";
}

export function levelLabel(level) {
  switch (level) {
    case "ok": return "OK";
    case "warning": return "Warning";
    case "invalid": return "Invalid";
    default: return healthLabelName(level);
  }
}

export function levelClass(level) {
  if (level === "ok") return "status-ok";
  if (level === "warning") return "status-warning";
  if (level === "invalid") return "status-invalid";
  return "";
}

export function renderStatus(level) {
  return `<span class="status-pill ${levelClass(level)}">${escapeHtml(levelLabel(level))}</span>`;
}

export function renderKv(rows) {
  return `<dl class="kv">${rows.map(([key, value]) => (
    `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value ?? "")}</dd>`
  )).join("")}</dl>`;
}

export function renderCheckList(checks) {
  if (!checks || checks.length === 0) return `<p class="muted">No checks reported.</p>`;
  return `<ul class="compact-list">${checks.map(c => `
    <li>
      <div>${renderStatus(c.level)} <strong>${escapeHtml(c.label || c.id)}</strong></div>
      <div class="muted">${escapeHtml(c.detail || "")}</div>
      ${c.remediation ? `<div>${escapeHtml(c.remediation)}</div>` : ""}
    </li>`).join("")}</ul>`;
}
export function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
}
export function cssToken(s) {
  return String(s ?? "").replace(/[^a-zA-Z0-9_-]/g, "");
}
export function log(msg) {
  if ($("#log-stream") && logHandler) logHandler(msg);
  else console.log(msg);
}

state.toastDisplayMs = loadToastDisplayMs();
state.currentTheme = loadThemePreference();
