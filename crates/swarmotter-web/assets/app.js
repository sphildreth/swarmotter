// SPDX-License-Identifier: Apache-2.0
// SwarmOtter Web UI controller. Consumes the same REST API as external tools.
const API = "/api/v1";
const DEFAULT_TOAST_DISPLAY_MS = 5000;
const MAX_TOAST_DISPLAY_MS = 60000;
const MAX_VISIBLE_TOASTS = 3;
const MAX_LOG_LINES = 500;
const TOAST_DISPLAY_STORAGE_KEY = "swarmotter.toastDisplayMs";
const THEME_STORAGE_KEY = "swarmotter.theme";
const THEME_DARK = "dark";
const THEME_LIGHT = "light";
const DEFAULT_THEME = THEME_DARK;
let currentHash = null;
let toastDisplayMs = loadToastDisplayMs();
let currentTheme = loadThemePreference();
let torrentsLoaded = false;
let knownTorrents = new Map();
let expectedRemovedTorrents = new Map();
let selectedTorrents = new Map();
let visibleTorrents = [];
let torrentTable = null;
let torrentTableBuilt = false;
let torrentTableReady = Promise.resolve();
let bulkRemoveInFlight = false;
let magnetAddInFlight = false;
let logEventSource = null;
let lastEventStreamErrorAt = 0;
let fullConfigSnapshot = null;

const EVENT_KINDS = [
  "torrent_added",
  "torrent_changed",
  "torrent_removed",
  "torrent_error",
  "torrent_metadata_received",
  "torrent_completed",
  "torrent_files_changed",
  "torrent_trackers_changed",
  "torrent_peers_changed",
  "stats_updated",
  "network_status_changed",
  "watch_folder_imported",
  "watch_folder_failed",
  "settings_changed",
  "daemon_health_changed",
];

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

function loadThemePreference() {
  try {
    return normalizeTheme(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    return DEFAULT_THEME;
  }
}

function normalizeTheme(rawTheme) {
  return rawTheme === THEME_LIGHT || rawTheme === THEME_DARK
    ? rawTheme
    : DEFAULT_THEME;
}

function applyTheme(theme, { persist = true } = {}) {
  const next = normalizeTheme(theme);
  currentTheme = next;
  document.documentElement.dataset.theme = next;
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

function toggleTheme() {
  applyTheme(currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK);
}

function loadToastDisplayMs() {
  try {
    const raw = window.localStorage.getItem(TOAST_DISPLAY_STORAGE_KEY);
    return normalizeToastDurationMs(raw);
  } catch {
    return DEFAULT_TOAST_DISPLAY_MS;
  }
}

function normalizeToastDurationMs(value) {
  const ms = Number(value);
  return Number.isFinite(ms)
    ? Math.max(1000, Math.min(MAX_TOAST_DISPLAY_MS, Math.round(ms)))
    : DEFAULT_TOAST_DISPLAY_MS;
}

function setToastDisplaySeconds(seconds) {
  const n = Number(seconds);
  const ms = Number.isFinite(n)
    ? normalizeToastDurationMs(n * 1000)
    : DEFAULT_TOAST_DISPLAY_MS;
  toastDisplayMs = ms;
  try { window.localStorage.setItem(TOAST_DISPLAY_STORAGE_KEY, String(ms)); } catch {}
  return ms;
}

function showToast(title, message = "", type = "info", durationMs = toastDisplayMs) {
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
  return (Math.min(completed, total) / total * 100).toFixed(1) + "%";
}
function renderProgressCell(bytesCompleted, totalLength) {
  const completed = finiteNumber(bytesCompleted);
  const total = finiteNumber(totalLength);
  if (completed === null || total === null || total <= 0) return "";
  const safeCompleted = Math.min(completed, total);
  return `<progress value="${safeCompleted}" max="${total}"></progress> ${fmtProgress(safeCompleted, total)}`;
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
function openView(view, activeButton = null) {
  $$(".nav").forEach(b => b.classList.remove("active"));
  if (activeButton && activeButton.classList.contains("nav")) activeButton.classList.add("active");
  $$(".view").forEach(v => v.classList.add("hidden"));
  $("#view-" + view).classList.remove("hidden");
  if (view === "torrents") refreshTorrents();
  if (view === "network") refreshNetwork();
  if (view === "settings") refreshSettings();
  if (view === "watch") refreshWatch();
  if (view === "logs") refreshLogs();
  if (view === "doctor") refreshDoctor();
}

$$(".nav").forEach(btn => btn.addEventListener("click", () => {
  openView(btn.dataset.view, btn);
}));

// --- Torrents ---
async function refreshTorrents() {
  try {
    const [list, stats] = await Promise.all([api("/torrents"), api("/stats")]);
    const torrents = list || [];
    observeTorrentRemovals(torrents);
    syncSelectedTorrents(torrents);
    const rows = torrents.map(normalizeTorrentRow);
    ensureTorrentTable();
    await setTorrentTableData(rows);
    $("#stats-summary").textContent = renderStatsSummary(stats);
    updateTorrentTableViewState();
  } catch (e) {
    log("torrent list error: " + e.message);
  }
}

function normalizeTorrentRow(t) {
  const total = finiteNumber(t.total_length);
  const completed = finiteNumber(t.bytes_completed);
  const progress = completed === null || total === null || total <= 0
    ? null
    : Math.min(completed, total) / total * 100;
  const rawHealthLabel = t?.health?.label || "unknown";
  return {
    ...t,
    info_hash: String(t.info_hash || ""),
    name: torrentDisplayName(t),
    progress_percent: progress === null ? 0 : progress,
    health_label: healthLabelName(rawHealthLabel),
    health_score: finiteNumber(t?.health?.score) ?? 0,
    active_peers: finiteNumber(t.active_peer_workers) ?? 0,
    known_peer_count: finiteNumber(t.known_peers) ?? 0,
  };
}

function ensureTorrentTable() {
  if (torrentTable) return torrentTableReady;
  if (typeof Tabulator === "undefined") {
    throw new Error("Tabulator asset did not load");
  }
  let resolveReady;
  torrentTableBuilt = false;
  torrentTableReady = new Promise(resolve => { resolveReady = resolve; });
  torrentTable = new Tabulator("#torrent-table", {
    data: [],
    index: "info_hash",
    layout: "fitDataStretch",
    height: "calc(100vh - 11rem)",
    movableColumns: true,
    placeholder: "No torrents match the current filters.",
    columnHeaderVertAlign: "bottom",
    headerFilterLiveFilterDelay: 250,
    initialSort: [{ column: "name", dir: "asc" }],
    columns: torrentTableColumns(),
    rowFormatter(row) {
      const data = row.getData();
      const element = row.getElement();
      element.classList.add("torrent");
      element.classList.toggle("selected", selectedTorrents.has(data.info_hash));
    },
  });
  torrentTable.on("tableBuilt", () => {
    torrentTableBuilt = true;
    resolveReady(torrentTable);
    updateTorrentTableViewState();
  });
  torrentTable.on("rowClick", (event, row) => {
    if (event.target.closest("button, input, label, select")) return;
    openDetails(row.getData().info_hash);
  });
  torrentTable.on("dataFiltered", updateTorrentTableViewState);
  torrentTable.on("dataSorted", updateTorrentTableViewState);
  torrentTable.on("renderComplete", updateTorrentTableViewState);
  return torrentTableReady;
}

function torrentTableColumns() {
  return [
    {
      title: "",
      field: "_selected",
      width: 44,
      minWidth: 44,
      frozen: true,
      headerSort: false,
      resizable: false,
      hozAlign: "center",
      cssClass: "selection-column",
      titleFormatter: () => `<span class="sr-only">Selected</span>`,
      formatter: torrentSelectionFormatter,
    },
    {
      title: "Name",
      field: "name",
      minWidth: 260,
      sorter: "string",
      headerFilter: "input",
      headerFilterPlaceholder: "Filter name",
      formatter: textCellFormatter,
    },
    {
      title: "Size",
      field: "total_length",
      width: 105,
      hozAlign: "right",
      sorter: "number",
      headerFilter: "input",
      headerFilterPlaceholder: "> 0",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => fmtBytes(cell.getValue()),
    },
    {
      title: "Progress",
      field: "progress_percent",
      width: 180,
      sorter: "number",
      headerFilter: "input",
      headerFilterPlaceholder: "> 50",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => {
        const data = cell.getRow().getData();
        return renderProgressCell(data.bytes_completed, data.total_length);
      },
    },
    {
      title: "Status",
      field: "state",
      width: 145,
      sorter: "string",
      headerFilter: "list",
      headerFilterParams: { valuesLookup: true, clearable: true },
      headerFilterPlaceholder: "All",
      formatter: textCellFormatter,
    },
    {
      title: "Health",
      field: "health_label",
      width: 165,
      sorter: healthSorter,
      headerFilter: "list",
      headerFilterParams: { valuesLookup: true, clearable: true },
      headerFilterPlaceholder: "All",
      formatter: cell => renderHealth(cell.getRow().getData().health),
    },
    {
      title: "Down",
      field: "rate_down",
      width: 105,
      hozAlign: "right",
      sorter: "number",
      headerFilter: "input",
      headerFilterPlaceholder: "> 0",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => fmtRate(cell.getValue()),
    },
    {
      title: "Up",
      field: "rate_up",
      width: 105,
      hozAlign: "right",
      sorter: "number",
      headerFilter: "input",
      headerFilterPlaceholder: "> 0",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => fmtRate(cell.getValue()),
    },
    {
      title: "Ratio",
      field: "ratio",
      width: 95,
      hozAlign: "right",
      sorter: "number",
      headerFilter: "input",
      headerFilterPlaceholder: "> 1",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => fmtRatio(cell.getValue()),
    },
    {
      title: "Peers",
      field: "active_peers",
      width: 105,
      hozAlign: "right",
      sorter: peerCountSorter,
      headerFilter: "input",
      headerFilterPlaceholder: "> 0",
      headerFilterFunc: numericHeaderFilter,
      formatter: cell => renderPeerCount(cell.getRow().getData()),
    },
    {
      title: "Actions",
      field: "_actions",
      width: 150,
      headerSort: false,
      resizable: false,
      formatter: () => renderTorrentActions(),
      cellClick: handleTorrentActionCellClick,
    },
  ];
}

function isTorrentTableReady() {
  return !!torrentTable && torrentTableBuilt;
}

async function setTorrentTableData(rows) {
  if (!torrentTable) return Promise.resolve();
  await torrentTableReady;
  const result = torrentTable.replaceData(rows);
  return result && typeof result.then === "function" ? result : Promise.resolve();
}

function textCellFormatter(cell) {
  return escapeHtml(cell.getValue());
}

function healthSorter(_a, _b, aRow, bRow) {
  return compareNumbers(aRow.getData().health_score, bRow.getData().health_score);
}

function peerCountSorter(_a, _b, aRow, bRow) {
  const a = aRow.getData();
  const b = bRow.getData();
  return compareNumbers(a.active_peers, b.active_peers)
    || compareNumbers(a.known_peer_count, b.known_peer_count);
}

function compareNumbers(a, b) {
  return (finiteNumber(a) ?? 0) - (finiteNumber(b) ?? 0);
}

function numericHeaderFilter(headerValue, rowValue) {
  const parsed = parseNumericFilter(headerValue);
  if (!parsed) return true;
  const value = finiteNumber(rowValue);
  if (value === null) return false;
  switch (parsed.operator) {
    case ">": return value > parsed.value;
    case ">=": return value >= parsed.value;
    case "<": return value < parsed.value;
    case "<=": return value <= parsed.value;
    case "!=": return value !== parsed.value;
    default: return value === parsed.value;
  }
}

function parseNumericFilter(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const match = text.match(/^(<=|>=|!=|==|=|<|>)?\s*(-?(?:\d+(?:\.\d+)?|\.\d+))$/);
  if (!match) return null;
  return {
    operator: match[1] === "==" ? "=" : (match[1] || "="),
    value: Number(match[2]),
  };
}

function torrentSelectionFormatter(cell, _formatterParams, onRendered) {
  onRendered(() => bindTorrentSelectionCheckbox(cell));
  return renderTorrentSelection(cell.getRow().getData());
}

function bindTorrentSelectionCheckbox(cell) {
  const checkbox = cell.getElement().querySelector(".torrent-select");
  if (!checkbox) return;
  checkbox.addEventListener("click", event => event.stopPropagation());
  checkbox.addEventListener("change", () => {
    const row = cell.getRow();
    const data = row.getData();
    if (checkbox.checked) selectedTorrents.set(data.info_hash, torrentDisplayName(data));
    else selectedTorrents.delete(data.info_hash);
    row.getElement().classList.toggle("selected", checkbox.checked);
    updateSelectionControls();
  });
}

function handleTorrentActionCellClick(event, cell) {
  const button = event.target.closest("button");
  if (!button) return;
  event.stopPropagation();
  const data = cell.getRow().getData();
  handleTorrentAction(button.dataset.act, data.info_hash, torrentDisplayName(data));
}

function activeTorrentRows() {
  if (!isTorrentTableReady()) return [];
  try {
    return torrentTable.getRows("active");
  } catch {
    return torrentTable.getRows();
  }
}

function updateVisibleTorrentsFromTable() {
  visibleTorrents = activeTorrentRows().map(row => {
    const data = row.getData();
    return {
      hash: data.info_hash,
      name: torrentDisplayName(data),
    };
  }).filter(t => t.hash);
}

function updateTorrentTableViewState() {
  updateVisibleTorrentsFromTable();
  updateRenderedSelection();
  updateSelectionControls();
  updateClearFiltersButton();
}

function torrentGlobalFilter(data) {
  const query = $("#search").value.trim().toLowerCase();
  if (!query) return true;
  return [
    data.name,
    data.info_hash,
    data.state,
    data.health_label,
  ].some(value => String(value || "").toLowerCase().includes(query));
}

async function applyTorrentSearchFilter() {
  if (!torrentTable) return;
  await torrentTableReady;
  if ($("#search").value.trim()) torrentTable.setFilter(torrentGlobalFilter);
  else torrentTable.clearFilter();
  updateTorrentTableViewState();
}

async function clearTorrentFilters() {
  $("#search").value = "";
  if (torrentTable) {
    await torrentTableReady;
    torrentTable.clearFilter(true);
  }
  updateTorrentTableViewState();
}

function updateClearFiltersButton() {
  const button = $("#clear-torrent-filters-btn");
  if (!button) return;
  const hasSearch = !!$("#search").value.trim();
  let hasHeaderFilters = false;
  if (isTorrentTableReady()) {
    try { hasHeaderFilters = torrentTable.getHeaderFilters().length > 0; } catch {}
  }
  button.disabled = !hasSearch && !hasHeaderFilters;
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

function torrentDisplayName(t) {
  return String(t?.name || t?.info_hash || "");
}

function syncSelectedTorrents(list) {
  const current = new Map((list || []).map(t => [t.info_hash, torrentDisplayName(t)]));
  for (const hash of Array.from(selectedTorrents.keys())) {
    if (current.has(hash)) selectedTorrents.set(hash, current.get(hash));
    else selectedTorrents.delete(hash);
  }
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

function renderTorrentSelection(t) {
  const checked = selectedTorrents.has(t.info_hash) ? " checked" : "";
  const name = torrentDisplayName(t);
  return `<input type="checkbox" class="torrent-select" data-hash="${escapeHtml(t.info_hash)}"${checked} aria-label="Select ${escapeHtml(name)}">`;
}

function updateRenderedSelection() {
  if (!isTorrentTableReady()) return;
  torrentTable.getRows().forEach(row => {
    const data = row.getData();
    const selected = selectedTorrents.has(data.info_hash);
    row.getElement().classList.toggle("selected", selected);
    const cb = row.getElement().querySelector(".torrent-select");
    if (cb) cb.checked = selected;
  });
}

function updateSelectionControls() {
  const selectedCount = selectedTorrents.size;
  const visibleCount = visibleTorrents.length;
  const allVisibleSelected = visibleCount > 0 && visibleTorrents.every(t => selectedTorrents.has(t.hash));
  const selectAll = $("#select-all-torrents-btn");
  const deselectAll = $("#deselect-all-torrents-btn");
  const removeSelected = $("#remove-selected-torrents-btn");
  const summary = $("#selection-summary");
  if (selectAll) selectAll.disabled = visibleCount === 0 || allVisibleSelected || bulkRemoveInFlight;
  if (deselectAll) deselectAll.disabled = selectedCount === 0 || bulkRemoveInFlight;
  if (removeSelected) removeSelected.disabled = selectedCount === 0 || bulkRemoveInFlight;
  if (summary) summary.textContent = `${selectedCount} selected`;
}

function selectAllVisibleTorrents() {
  visibleTorrents.forEach(t => selectedTorrents.set(t.hash, t.name));
  updateRenderedSelection();
  updateSelectionControls();
}

function deselectAllTorrents() {
  selectedTorrents.clear();
  updateRenderedSelection();
  updateSelectionControls();
}

async function removeSelectedTorrents() {
  if (bulkRemoveInFlight) return;
  const selected = Array.from(selectedTorrents.entries());
  if (selected.length === 0) return;
  const noun = selected.length === 1 ? "torrent" : "torrents";
  const confirmed = window.confirm(`Remove ${selected.length} selected ${noun} from SwarmOtter? Downloaded data will be kept.`);
  if (!confirmed) return;
  bulkRemoveInFlight = true;
  updateSelectionControls();
  try {
    const result = await api("/torrents/remove", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        info_hashes: selected.map(([hash]) => hash),
        delete_data: false,
      }),
    });
    const removedSet = new Set(result?.removed || []);
    const notFoundSet = new Set(result?.not_found || []);
    for (const [hash, name] of selected) {
      if (removedSet.has(hash) || notFoundSet.has(hash)) {
        expectedRemovedTorrents.set(hash, name);
        selectedTorrents.delete(hash);
      }
    }
    await refreshTorrents();
    const removed = removedSet.size;
    const alreadyGone = notFoundSet.size;
    if (removed > 0 && alreadyGone > 0) {
      showToast(`Removed ${removed} ${removed === 1 ? "torrent" : "torrents"}`, `${alreadyGone} already gone`, "warning");
    } else if (removed > 0) {
      showToast(`Removed ${removed} ${removed === 1 ? "torrent" : "torrents"}`, "Downloaded data kept", "info");
    } else if (alreadyGone > 0) {
      showToast("Selected torrents already removed", `${alreadyGone} no longer present`, "info");
    } else {
      showToast("No selected torrents removed", "", "warning");
    }
  } catch (e) {
    showError("Remove selected failed", e);
    log(`bulk remove error: ${e.message}`);
  } finally {
    bulkRemoveInFlight = false;
    updateSelectionControls();
  }
}

async function handleTorrentAction(act, hash, name) {
  try {
    if (act === "pause") await api(`/torrents/${hash}/pause`, { method: "POST" });
    else if (act === "resume") await api(`/torrents/${hash}/resume`, { method: "POST" });
    else if (act === "recheck") await api(`/torrents/${hash}/recheck`, { method: "POST" });
    else if (act === "remove") {
      if (confirm("Remove torrent? Delete data too?")) await api(`/torrents/${hash}?delete_data=true`, { method: "DELETE" });
      else await api(`/torrents/${hash}`, { method: "DELETE" });
      expectedRemovedTorrents.set(hash, name);
      selectedTorrents.delete(hash);
      showToast("Torrent removed", name, "info");
    }
    refreshTorrents();
  } catch (e) {
    showError("Torrent action failed", e);
  }
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

function levelLabel(level) {
  switch (level) {
    case "ok": return "OK";
    case "warning": return "Warning";
    case "invalid": return "Invalid";
    default: return healthLabelName(level);
  }
}

function levelClass(level) {
  if (level === "ok") return "status-ok";
  if (level === "warning") return "status-warning";
  if (level === "invalid") return "status-invalid";
  return "";
}

function renderStatus(level) {
  return `<span class="status-pill ${levelClass(level)}">${escapeHtml(levelLabel(level))}</span>`;
}

function renderKv(rows) {
  return `<dl class="kv">${rows.map(([key, value]) => (
    `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value ?? "")}</dd>`
  )).join("")}</dl>`;
}

function renderCheckList(checks) {
  if (!checks || checks.length === 0) return `<p class="muted">No checks reported.</p>`;
  return `<ul class="compact-list">${checks.map(c => `
    <li>
      <div>${renderStatus(c.level)} <strong>${escapeHtml(c.label || c.id)}</strong></div>
      <div class="muted">${escapeHtml(c.detail || "")}</div>
      ${c.remediation ? `<div>${escapeHtml(c.remediation)}</div>` : ""}
    </li>`).join("")}</ul>`;
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
  if (magnetAddInFlight) return;
  const button = $("#add-magnet-btn");
  const input = $("#magnet-input");
  try {
    const magnet = input.value.trim();
    if (!magnet) {
      showToast("Enter a magnet link", "", "warning");
      return;
    }
    magnetAddInFlight = true;
    button.disabled = true;
    button.setAttribute("aria-busy", "true");
    showToast("Adding magnet", "", "info");
    const dir = $("#magnet-dir").value.trim();
    const body = { magnet };
    if (dir) body.download_dir = dir;
    const h = await api("/torrents/magnet", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    showToast("Torrent added", h, "success");
    input.value = "";
    refreshTorrents();
  } catch (e) {
    if (e && e.code === "duplicate_torrent") {
      showToast("Torrent already added", "", "warning");
      input.value = "";
      refreshTorrents();
    } else {
      showError("Add magnet failed", e);
    }
  } finally {
    magnetAddInFlight = false;
    button.disabled = false;
    button.removeAttribute("aria-busy");
  }
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

// --- Settings ---
async function refreshSettings() {
  try {
    const cfg = await api("/settings");
    fullConfigSnapshot = cfg;
    renderSettingsEditor(cfg);
  } catch (e) { log("settings error: " + e.message); }
}

function settingsField(id) {
  return $("#" + id);
}

function setSettingsValue(id, value) {
  const el = settingsField(id);
  if (el) el.value = value ?? "";
}

function setSettingsChecked(id, value) {
  const el = settingsField(id);
  if (el) el.checked = !!value;
}

function settingsString(id) {
  return settingsField(id).value.trim();
}

function settingsOptionalString(id) {
  const value = settingsString(id);
  return value ? value : null;
}

function settingsInteger(id, fallback = 0) {
  const value = settingsField(id).value;
  if (value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function settingsFloatOrNull(id) {
  const value = settingsField(id).value;
  if (value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function settingsIntegerOrNull(id) {
  const value = settingsField(id).value;
  if (value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function settingsLineList(id) {
  return settingsField(id).value
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
}

function renderSettingsEditor(cfg) {
  const apiCfg = cfg.api || {};
  const compatibility = cfg.compatibility || {};
  const transmission = compatibility.transmission || {};
  const storage = cfg.storage || {};
  const network = cfg.network || {};
  const torrent = cfg.torrent || {};
  const bandwidth = cfg.bandwidth || {};
  const queue = cfg.queue || {};
  const seeding = cfg.seeding || {};
  const dht = cfg.dht || {};
  const pex = cfg.pex || {};
  const logging = cfg.logging || {};

  setSettingsValue("cfg-api-bind-address", apiCfg.bind_address);
  setSettingsValue("cfg-api-auth-token", "");
  setSettingsValue("cfg-api-max-request-body-bytes", apiCfg.max_request_body_bytes);
  setSettingsChecked("cfg-api-require-auth", apiCfg.require_auth);

  setSettingsChecked("cfg-compat-transmission-enabled", transmission.enabled);

  setSettingsValue("cfg-storage-download-dir", storage.download_dir);
  setSettingsValue("cfg-storage-incomplete-dir", storage.incomplete_dir);
  setSettingsChecked("cfg-storage-preallocate", storage.preallocate);
  setSettingsChecked("cfg-storage-sparse", storage.sparse);

  setSettingsValue("cfg-network-mode", network.mode || "disabled");
  setSettingsValue("cfg-network-required-interface", network.required_interface);
  setSettingsValue("cfg-network-required-source-ipv4", network.required_source_ipv4);
  setSettingsValue("cfg-network-required-source-ipv6", network.required_source_ipv6);
  setSettingsValue("cfg-network-required-network-namespace", network.required_network_namespace);
  setSettingsChecked("cfg-network-allow-ipv6", network.allow_ipv6);
  setSettingsChecked("cfg-network-fail-closed", network.fail_closed);
  setSettingsChecked("cfg-network-validate-route", network.validate_route);
  setSettingsChecked("cfg-network-validate-dns", network.validate_dns);

  setSettingsValue("cfg-torrent-listen-port", torrent.listen_port);
  setSettingsChecked("cfg-torrent-allow-ipv6", torrent.allow_ipv6);
  setSettingsChecked("cfg-torrent-utp-enabled", torrent.utp_enabled);
  setSettingsChecked("cfg-torrent-utp-prefer-tcp", torrent.utp_prefer_tcp);
  setSettingsChecked("cfg-torrent-selfish", torrent.selfish);

  setSettingsValue("cfg-bandwidth-global-download", bandwidth.global_download);
  setSettingsValue("cfg-bandwidth-global-upload", bandwidth.global_upload);
  setSettingsValue("cfg-bandwidth-alt-download", bandwidth.alt_download);
  setSettingsValue("cfg-bandwidth-alt-upload", bandwidth.alt_upload);
  setSettingsValue("cfg-bandwidth-max-peers", bandwidth.max_peers);
  setSettingsValue("cfg-bandwidth-max-peers-per-torrent", bandwidth.max_peers_per_torrent);
  setSettingsChecked("cfg-bandwidth-alt-enabled", bandwidth.alt_enabled);

  setSettingsValue("cfg-queue-max-active-downloads", queue.max_active_downloads);
  setSettingsValue("cfg-queue-max-active-seeds", queue.max_active_seeds);
  setSettingsChecked("cfg-queue-auto-start", queue.auto_start);

  setSettingsValue("cfg-seeding-global-ratio-limit", seeding.global_ratio_limit);
  setSettingsValue("cfg-seeding-global-idle-limit", seeding.global_idle_limit);

  setSettingsValue("cfg-dht-port", dht.port);
  setSettingsChecked("cfg-dht-enabled", dht.enabled);
  setSettingsValue("cfg-dht-bootstrap-nodes", (dht.bootstrap_nodes || []).join("\n"));

  setSettingsValue("cfg-pex-max-peers", pex.max_peers);
  setSettingsChecked("cfg-pex-enabled", pex.enabled);

  setSettingsValue("cfg-logging-level", logging.level || "info");
  setSettingsValue("cfg-logging-file-path", logging.file_path);
  setSettingsChecked("cfg-logging-json", logging.json);
  setSettingsChecked("cfg-logging-file", logging.file);

  renderWatchFolderEditors(cfg.watch || []);
  setSettingsValue("toast-seconds", String(Math.round(toastDisplayMs / 1000)));
}

function collectSettingsConfig() {
  const authToken = settingsOptionalString("cfg-api-auth-token");
  return {
    api: {
      bind_address: settingsString("cfg-api-bind-address"),
      auth_token: authToken,
      require_auth: settingsField("cfg-api-require-auth").checked,
      max_request_body_bytes: settingsInteger("cfg-api-max-request-body-bytes", 1),
    },
    compatibility: {
      transmission: {
        enabled: settingsField("cfg-compat-transmission-enabled").checked,
      },
    },
    storage: {
      download_dir: settingsOptionalString("cfg-storage-download-dir"),
      incomplete_dir: settingsOptionalString("cfg-storage-incomplete-dir"),
      preallocate: settingsField("cfg-storage-preallocate").checked,
      sparse: settingsField("cfg-storage-sparse").checked,
    },
    network: {
      mode: settingsString("cfg-network-mode"),
      required_interface: settingsOptionalString("cfg-network-required-interface"),
      required_source_ipv4: settingsOptionalString("cfg-network-required-source-ipv4"),
      required_source_ipv6: settingsOptionalString("cfg-network-required-source-ipv6"),
      required_network_namespace: settingsOptionalString("cfg-network-required-network-namespace"),
      allow_ipv6: settingsField("cfg-network-allow-ipv6").checked,
      fail_closed: settingsField("cfg-network-fail-closed").checked,
      validate_route: settingsField("cfg-network-validate-route").checked,
      validate_dns: settingsField("cfg-network-validate-dns").checked,
    },
    torrent: {
      listen_port: settingsInteger("cfg-torrent-listen-port", 51413),
      allow_ipv6: settingsField("cfg-torrent-allow-ipv6").checked,
      utp_enabled: settingsField("cfg-torrent-utp-enabled").checked,
      utp_prefer_tcp: settingsField("cfg-torrent-utp-prefer-tcp").checked,
      selfish: settingsField("cfg-torrent-selfish").checked,
    },
    bandwidth: {
      global_download: settingsInteger("cfg-bandwidth-global-download"),
      global_upload: settingsInteger("cfg-bandwidth-global-upload"),
      alt_download: settingsInteger("cfg-bandwidth-alt-download"),
      alt_upload: settingsInteger("cfg-bandwidth-alt-upload"),
      alt_enabled: settingsField("cfg-bandwidth-alt-enabled").checked,
      max_peers: settingsInteger("cfg-bandwidth-max-peers"),
      max_peers_per_torrent: settingsInteger("cfg-bandwidth-max-peers-per-torrent"),
    },
    queue: {
      max_active_downloads: settingsInteger("cfg-queue-max-active-downloads"),
      max_active_seeds: settingsInteger("cfg-queue-max-active-seeds"),
      auto_start: settingsField("cfg-queue-auto-start").checked,
    },
    seeding: {
      global_ratio_limit: settingsFloatOrNull("cfg-seeding-global-ratio-limit"),
      global_idle_limit: settingsIntegerOrNull("cfg-seeding-global-idle-limit"),
    },
    dht: {
      enabled: settingsField("cfg-dht-enabled").checked,
      bootstrap_nodes: settingsLineList("cfg-dht-bootstrap-nodes"),
      port: settingsInteger("cfg-dht-port", 51413),
    },
    pex: {
      enabled: settingsField("cfg-pex-enabled").checked,
      max_peers: settingsInteger("cfg-pex-max-peers"),
    },
    watch: collectWatchFolderEditors(),
    logging: {
      level: settingsString("cfg-logging-level"),
      json: settingsField("cfg-logging-json").checked,
      file: settingsField("cfg-logging-file").checked,
      file_path: settingsOptionalString("cfg-logging-file-path"),
    },
  };
}

function renderWatchFolderEditors(folders) {
  const list = $("#settings-watch-list");
  if (!list) return;
  if (!folders || folders.length === 0) {
    list.innerHTML = `<p class="muted">No watch folders configured.</p>`;
    return;
  }
  list.innerHTML = folders.map((folder, index) => renderWatchFolderEditor(folder, index)).join("");
}

function renderWatchFolderEditor(folder, index) {
  const cfg = folder || {};
  return `
    <div class="watch-folder-editor">
      <div class="watch-folder-header">
        <strong>Folder ${index + 1}</strong>
        <button type="button" class="icon-button danger" data-settings-action="remove-watch-folder" aria-label="Remove watch folder" title="Remove watch folder">
          <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M6 6l1 15h10l1-15"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
        </button>
      </div>
      <div class="settings-form-grid">
        <label class="settings-field"><span>Path</span><input data-watch-field="path" type="text" required value="${escapeHtml(cfg.path || "")}"></label>
        <label class="settings-field"><span>Download directory</span><input data-watch-field="download_dir" type="text" value="${escapeHtml(cfg.download_dir || "")}"></label>
        <label class="settings-field"><span>Label</span><input data-watch-field="label" type="text" value="${escapeHtml(cfg.label || "")}"></label>
        <label class="settings-field">
          <span>Start behavior</span>
          <select data-watch-field="start_behavior">
            <option value="start"${(cfg.start_behavior || "start") === "start" ? " selected" : ""}>Start</option>
            <option value="paused"${cfg.start_behavior === "paused" ? " selected" : ""}>Paused</option>
          </select>
        </label>
        <label class="settings-field"><span>Archive directory</span><input data-watch-field="archive_dir" type="text" value="${escapeHtml(cfg.archive_dir || "")}"></label>
        <label class="settings-field"><span>Failure directory</span><input data-watch-field="failure_dir" type="text" value="${escapeHtml(cfg.failure_dir || "")}"></label>
        <label class="settings-check"><input data-watch-field="recursive" type="checkbox"${cfg.recursive ? " checked" : ""}><span>Recursive</span></label>
        <label class="settings-check"><input data-watch-field="delete_after_import" type="checkbox"${cfg.delete_after_import !== false ? " checked" : ""}><span>Delete after import</span></label>
      </div>
    </div>`;
}

function collectWatchFolderEditors() {
  return $$("#settings-watch-list .watch-folder-editor").map(row => ({
    path: watchInput(row, "path").value.trim(),
    recursive: watchInput(row, "recursive").checked,
    download_dir: watchOptionalString(row, "download_dir"),
    label: watchOptionalString(row, "label"),
    start_behavior: watchInput(row, "start_behavior").value,
    archive_dir: watchOptionalString(row, "archive_dir"),
    failure_dir: watchOptionalString(row, "failure_dir"),
    delete_after_import: watchInput(row, "delete_after_import").checked,
  }));
}

function watchInput(row, field) {
  return row.querySelector(`[data-watch-field="${field}"]`);
}

function watchOptionalString(row, field) {
  const value = watchInput(row, field).value.trim();
  return value ? value : null;
}

$("#settings-editor").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  if (!form.reportValidity()) return;
  const status = $("#settings-save-status");
  try {
    const result = await api("/settings", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(collectSettingsConfig()),
    });
    fullConfigSnapshot = result.config;
    renderSettingsEditor(result.config);
    renderConfigSaveStatus(result);
    showToast("Configuration saved", result.restart_required ? "Restart required for some fields" : "", "success");
    await refreshDoctorBadge();
  } catch (e) {
    if (status) {
      status.innerHTML = `<h3>Save status</h3>${renderCheckList([{
        id: "config_save",
        label: "Configuration save",
        level: "invalid",
        detail: e.message || String(e),
      }])}`;
    }
    showError("Save configuration failed", e);
  }
});

$("#reload-settings-btn").addEventListener("click", async () => {
  await refreshSettings();
  showToast("Settings reloaded", "", "info");
});

$("#reset-downloads-btn").addEventListener("click", async () => {
  const confirmed = window.confirm(
    "Reset all downloads? This stops all torrents, removes torrent records, deletes all files inside the configured download and incomplete directories, and clears daemon logs."
  );
  if (!confirmed) return;
  const button = $("#reset-downloads-btn");
  button.disabled = true;
  try {
    const result = await api("/reset", { method: "POST" });
    currentHash = null;
    knownTorrents.clear();
    expectedRemovedTorrents.clear();
    torrentsLoaded = false;
    await refreshTorrents();
    if (!$("#view-logs").classList.contains("hidden")) await refreshLogs();
    await refreshDoctorBadge();
    const detail = [
      `${fmtCount(result.torrents_removed)} torrents`,
      `${fmtCount(result.storage_entries_removed)} storage entries`,
      `${fmtCount(result.log_files_cleared)} log files`,
    ].join(" cleared; ");
    showToast("Reset complete", detail, "success");
  } catch (e) {
    showError("Reset failed", e);
  } finally {
    button.disabled = false;
  }
});

$("#add-watch-folder-btn").addEventListener("click", () => {
  const folders = collectWatchFolderEditors();
  folders.push({
    path: "",
    recursive: false,
    download_dir: null,
    label: null,
    start_behavior: "start",
    archive_dir: null,
    failure_dir: null,
    delete_after_import: true,
  });
  renderWatchFolderEditors(folders);
  const rows = $$("#settings-watch-list .watch-folder-editor");
  const last = rows[rows.length - 1];
  if (last) watchInput(last, "path").focus();
});

$("#settings-watch-list").addEventListener("click", (event) => {
  const remove = event.target.closest('[data-settings-action="remove-watch-folder"]');
  if (!remove) return;
  remove.closest(".watch-folder-editor")?.remove();
  if ($$("#settings-watch-list .watch-folder-editor").length === 0) {
    renderWatchFolderEditors([]);
  }
});

$("#save-toast-btn").addEventListener("click", () => {
  const ms = setToastDisplaySeconds($("#toast-seconds").value);
  $("#toast-seconds").value = String(Math.round(ms / 1000));
  showToast("Notification settings saved", "", "success");
});

function renderConfigSaveStatus(result) {
  $("#settings-save-status").innerHTML = `
    <h3>Save status</h3>
    ${renderKv([
      ["Persisted to config.toml", String(!!result.persisted)],
      ["Config path", result.config_path || ""],
      ["Restart required", String(!!result.restart_required)],
      ["Restart fields", (result.restart_required_fields || []).join(", ")],
      ["Runtime fields applied", (result.applied_runtime_fields || []).join(", ")],
    ])}`;
}

// --- Watch ---
async function refreshWatch() {
  try {
    const status = await api("/watch/status");
    renderWatch(status);
    return status;
  } catch (e) { log("watch error: " + e.message); }
}

function renderWatch(status, scanDetail = "") {
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
        <thead><tr><th>Path</th><th>Status</th><th>Info hash</th><th>Detail</th></tr></thead>
        <tbody>${imports.slice().reverse().slice(0, 40).map(item => `
          <tr>
            <td>${escapeHtml(item.path)}</td>
            <td>${renderStatus(item.success ? "ok" : "invalid")}</td>
            <td>${escapeHtml(item.info_hash_hex || "")}</td>
            <td>${escapeHtml(importStatus(item))}</td>
          </tr>`).join("")}</tbody>
      </table>`}`;
}

function renderWatchFolderRow(folder) {
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

function importStatus(item) {
  if (item.success === true) return "ok";
  if (item.error) return item.error;
  if (item.success === false) return "fail";
  return "";
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
async function refreshLogs() {
  try {
    const snapshot = await api("/logs/recent?lines=200");
    renderLogSnapshot(snapshot);
    connectEventStream();
  } catch (e) { log("events error: " + e.message); }
}

$("#refresh-logs-btn").addEventListener("click", refreshLogs);

function renderLogSnapshot(snapshot) {
  const lines = snapshot?.lines || [];
  const source = snapshot?.path
    ? `${snapshot.path}${snapshot.truncated ? " (tail)" : ""}`
    : "live event stream";
  $("#log-source").textContent = snapshot?.enabled ? source : `${source} unavailable`;
  $("#log-stream").textContent = lines.length ? lines.join("\n") : "[no recent log lines]";
}

function connectEventStream() {
  if (logEventSource) return;
  try {
    logEventSource = new EventSource(API + "/events");
    logEventSource.onopen = () => appendLogLine("[event stream connected]");
    EVENT_KINDS.forEach(kind => {
      logEventSource.addEventListener(kind, (event) => {
        appendEventLine(kind, event.data);
      });
    });
    logEventSource.onerror = () => {
      const nowMs = Date.now();
      if (nowMs - lastEventStreamErrorAt > 10000) {
        appendLogLine("[event stream disconnected; browser will retry]");
        lastEventStreamErrorAt = nowMs;
      }
    };
  } catch (e) {
    appendLogLine("[event stream unavailable] " + e.message);
  }
}

function appendEventLine(kind, raw) {
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
  if (kind === "settings_changed" && !$("#view-settings").classList.contains("hidden")) refreshSettings();
}

function appendLogLine(line) {
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
async function refreshDoctor() {
  try {
    const report = await api("/doctor");
    const version = await api("/version").catch((e) => {
      log("version error: " + e.message);
      return null;
    });
    renderDoctor(report, version);
    updateHealthBadge(report);
    return report;
  } catch (e) {
    updateHealthBadge({ level: "invalid", summary: e.message || String(e), checks: [] });
    log("doctor error: " + e.message);
  }
}

async function refreshDoctorBadge() {
  try {
    updateHealthBadge(await api("/doctor"));
  } catch (e) {
    updateHealthBadge({ level: "invalid", summary: e.message || String(e), checks: [] });
  }
}

function renderDoctor(report, version = null) {
  $("#doctor-summary").innerHTML = `
    <h3>Health summary</h3>
    ${renderKv([
      ["Overall", levelLabel(report.level)],
      ["Summary", report.summary || ""],
      ["Checks", String((report.checks || []).length)],
    ])}`;
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

function updateHealthBadge(report) {
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

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
}
function cssToken(s) {
  return String(s ?? "").replace(/[^a-zA-Z0-9_-]/g, "");
}
function log(msg) {
  if ($("#log-stream")) appendLogLine(msg);
  else console.log(msg);
}

$("#search").addEventListener("input", applyTorrentSearchFilter);
$("#clear-torrent-filters-btn").addEventListener("click", clearTorrentFilters);
$("#select-all-torrents-btn").addEventListener("click", selectAllVisibleTorrents);
$("#deselect-all-torrents-btn").addEventListener("click", deselectAllTorrents);
$("#remove-selected-torrents-btn").addEventListener("click", removeSelectedTorrents);
const themeToggle = $("#theme-toggle");
if (themeToggle) themeToggle.addEventListener("click", toggleTheme);

// --- Init ---
(async function init() {
  applyTheme(currentTheme, { persist: false });
  await refreshTorrents();
  await refreshDoctorBadge();
  setInterval(refreshTorrents, 5000);
  setInterval(refreshDoctorBadge, 10000);
})();
