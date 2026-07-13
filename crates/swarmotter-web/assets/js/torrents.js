// SPDX-License-Identifier: Apache-2.0

import { state, TORRENT_QUERY_STORAGE_KEY, TORRENT_DEFAULT_PER_PAGE, TORRENT_MAX_PER_PAGE, TORRENT_SORT_OPTIONS, TORRENT_TABLE_TO_QUERY_SORT, TORRENT_QUERY_TO_TABLE_SORT, TORRENT_ACTIONS, EVENT_KINDS, MAX_LOG_LINES, watchHistoryUi } from "./state.js";
import * as ui from "./ui.js";
import * as requests from "./api.js";
const { $, $$, showToast, showError, finiteNumber, fmtCount, fmtBytes, fmtRate, fmtRatio, fmtPercentFromFraction, fmtPercent, fmtProgress, fmtUnixSeconds, renderProgressCell, renderHealth, renderHealthSummary, fmtScore, renderStatus, renderKv, renderCheckList, escapeHtml, cssToken, log, setToastDisplaySeconds } = ui;
const { api, apiFetch, responseErrorMessage, saveApiToken } = requests;

let openDetailsHandler = () => {};
export function setOpenDetailsHandler(handler) { openDetailsHandler = handler || (() => {}); }
export function refreshTorrentTableTheme(theme) {
  const tableElement = $("#torrent-table");
  if (tableElement) tableElement.dataset.theme = theme;
  if (!isTorrentTableReady() || typeof state.torrentTable.redraw !== "function") return;
  const redraw = () => state.torrentTable.redraw(true);
  if (typeof window.requestAnimationFrame === "function") window.requestAnimationFrame(redraw);
  else window.setTimeout(redraw, 0);
}

// --- Torrents ---
export function clampTorrentPerPage(value) {
  const n = finiteNumber(value);
  if (n === null) return TORRENT_DEFAULT_PER_PAGE;
  return Math.min(Math.max(Math.floor(n), 0), TORRENT_MAX_PER_PAGE);
}

export function clampTorrentPage(value) {
  const n = finiteNumber(value);
  return n === null || n <= 0 ? 1 : Math.floor(n);
}

export function normalizeTorrentDir(rawDir) {
  return String(rawDir || "").toLowerCase() === "desc" ? "desc" : "asc";
}

export function normalizeTorrentSort(rawSort) {
  const normalized = String(rawSort || "").toLowerCase();
  return TORRENT_SORT_OPTIONS.has(normalized) ? normalized : "name";
}

export function tableSortToQuerySort(field) {
  return TORRENT_TABLE_TO_QUERY_SORT[field] || null;
}

export function querySortToTableSort(sort) {
  return TORRENT_QUERY_TO_TABLE_SORT[sort] || null;
}

export function collectTorrentQueryControls() {
  return {
    q: ($("#search")?.value || "").trim(),
    state: ($("#torrent-state-filter")?.value || "").trim().toLowerCase(),
    health: ($("#torrent-health-filter")?.value || "").trim().toLowerCase(),
    performance: ($("#torrent-performance-filter")?.value || "").trim().toLowerCase(),
    per_page: clampTorrentPerPage($("#torrent-per-page")?.value),
  };
}

export function applyTorrentQueryControls(state) {
  if ($("#search")) $("#search").value = state.q || "";
  if ($("#torrent-state-filter")) $("#torrent-state-filter").value = state.state || "";
  if ($("#torrent-health-filter")) $("#torrent-health-filter").value = state.health || "";
  if ($("#torrent-performance-filter")) $("#torrent-performance-filter").value = state.performance || "";
  if ($("#torrent-per-page")) $("#torrent-per-page").value = String(state.per_page || TORRENT_DEFAULT_PER_PAGE);
}

export function collectTorrentQueryState() {
  const controls = collectTorrentQueryControls();
  return {
    ...torrentQueryState,
    ...controls,
    sort: normalizeTorrentSort(state.torrentQueryState.sort),
    dir: normalizeTorrentDir(state.torrentQueryState.dir),
    page: clampTorrentPage(state.torrentQueryState.page),
    per_page: controls.per_page,
  };
}

export function buildTorrentQueryParams(state) {
  const query = new URLSearchParams();
  if (state.q) query.set("q", state.q);
  if (state.state) query.set("state", state.state);
  if (state.health) query.set("health", state.health);
  if (state.performance) query.set("performance", state.performance);
  if (state.sort) query.set("sort", state.sort);
  if (state.dir) query.set("dir", state.dir);
  if (state.page) query.set("page", String(state.page));
  if (state.per_page !== undefined && state.per_page !== null) query.set("per_page", String(state.per_page));
  return query.toString();
}

export async function refreshTorrents() {
  if (state.torrentQueryRefreshTimer) {
    window.clearTimeout(state.torrentQueryRefreshTimer);
    state.torrentQueryRefreshTimer = null;
  }
  state.torrentQueryState = collectTorrentQueryState();
  try {
    const queryParams = buildTorrentQueryParams(state.torrentQueryState);
    const [query, stats] = await Promise.all([
      api(`/torrents/query${queryParams ? `?${queryParams}` : ""}`),
      api("/stats"),
    ]);
    const torrents = query?.rows || [];
    state.torrentQueryState.sort = normalizeTorrentSort(query?.sort);
    state.torrentQueryState.dir = normalizeTorrentDir(query?.dir);
    state.torrentQueryState.page = clampTorrentPage(query?.page);
    state.torrentQueryState.per_page = clampTorrentPerPage(query?.per_page);
    applyTorrentQueryControls(state.torrentQueryState);
    if (query?.page_count > 0 && query?.page > query?.page_count) {
      state.torrentQueryState.page = query.page_count;
      return refreshTorrents();
    }
    observeTorrentRemovals(torrents, queryParams, query);
    syncSelectedTorrents(torrents);
    const rows = torrents.map(normalizeTorrentRow);
    ensureTorrentTable();
    await setTorrentTableData(rows);
    if (!state.isApplyingSortFromServer) syncTorrentTableSort(state.torrentQueryState.sort, state.torrentQueryState.dir);
    if ($("#query-summary")) $("#query-summary").textContent = renderTorrentQuerySummary(query);
    updateTorrentPaginationControls(query);
    $("#stats-summary").textContent = renderStatsSummary(stats);
    if ($("#torrent-prev-page-btn")) {
      const pageCount = query?.page_count || 0;
      const page = query?.page || 1;
      $("#torrent-prev-page-btn").disabled = page <= 1 || pageCount === 0;
      $("#torrent-next-page-btn").disabled = page >= pageCount || pageCount === 0;
    }
    updateTorrentTableViewState();
    updateClearFiltersButton();
    return query;
  } catch (e) {
    log("torrent list error: " + e.message);
    return null;
  }
}

export function updateTorrentPaginationControls(query) {
  if (!query) return;
  const page = query.page || 1;
  const pageCount = query.page_count || 0;
  const summary = $("#torrent-page-summary");
  if (summary) summary.textContent = pageCount === 0 ? "Page 0/0" : `Page ${page}/${pageCount}`;
  const prev = $("#torrent-prev-page-btn");
  const next = $("#torrent-next-page-btn");
  if (prev) prev.disabled = page <= 1 || pageCount === 0;
  if (next) next.disabled = page >= pageCount || pageCount === 0;
}

export function renderTorrentQuerySummary(query) {
  const filtered = finiteNumber(query?.filtered);
  const total = finiteNumber(query?.total);
  const perPage = clampTorrentPerPage(query?.per_page);
  const page = clampTorrentPage(query?.page);
  const pageCountRaw = finiteNumber(query?.page_count);
  const pageCount = pageCountRaw === null ? 0 : Math.floor(Math.max(pageCountRaw, 0));
  if (filtered === null || total === null) return "";
  if (filtered === 0) return `${fmtCount(filtered)}/${fmtCount(total)} matching torrents · page 0/0`;
  if (perPage === 0) return `${fmtCount(filtered)}/${fmtCount(total)} matching torrents · counts only`;
  const start = Math.min((page - 1) * perPage + 1, filtered);
  const end = Math.min(page * perPage, filtered);
  return `${fmtCount(filtered)}/${fmtCount(total)} matching torrents · ${start}-${end} · page ${page}/${pageCount}`;
}

export function syncTorrentTableSort(sort, dir) {
  if (!isTorrentTableReady()) return;
  const tableSort = querySortToTableSort(sort);
  if (!tableSort) return;
  const sorters = state.torrentTable.getSorters();
  const current = sorters && sorters[0] ? sorters[0] : null;
  const nextDir = normalizeTorrentDir(dir);
  if (current && current.field === tableSort && current.dir === nextDir) return;
  state.isApplyingSortFromServer = true;
  state.torrentTable.setSort([{ column: tableSort, dir: nextDir }]);
  window.setTimeout(() => {
    state.isApplyingSortFromServer = false;
  }, 0);
}

export function handleTorrentTableSort(sorters) {
  if (state.isApplyingSortFromServer) return;
  const first = Array.isArray(sorters) && sorters.length > 0 ? sorters[0] : null;
  if (!first) return;
  const field = first.field || (first.column && first.column.getField && first.column.getField());
  const sort = tableSortToQuerySort(field);
  if (!sort) return;
  const dir = normalizeTorrentDir(first.dir);
  if (sort === state.torrentQueryState.sort && dir === state.torrentQueryState.dir) return;
  state.torrentQueryState.sort = sort;
  state.torrentQueryState.dir = dir;
  state.torrentQueryState.page = 1;
  refreshTorrents();
}

export function scheduleTorrentRefresh() {
  if (state.torrentQueryRefreshTimer) {
    window.clearTimeout(state.torrentQueryRefreshTimer);
  }
  state.torrentQueryRefreshTimer = window.setTimeout(() => {
    state.torrentQueryRefreshTimer = null;
    refreshTorrents();
  }, 250);
}

export function setTorrentPage(page) {
  const next = clampTorrentPage(page);
  if (next === state.torrentQueryState.page) return;
  state.torrentQueryState.page = next;
  refreshTorrents();
}

export function normalizeTorrentRow(t) {
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

export function ensureTorrentTable() {
  if (state.torrentTable) return state.torrentTableReady;
  if (typeof Tabulator === "undefined") {
    throw new Error("Tabulator asset did not load");
  }
  let resolveReady;
  state.torrentTableBuilt = false;
  state.torrentTableReady = new Promise(resolve => { resolveReady = resolve; });
  state.torrentTable = new Tabulator("#torrent-table", {
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
      element.classList.toggle("selected", state.selectedTorrents.has(data.info_hash));
    },
  });
  state.torrentTable.on("tableBuilt", () => {
    state.torrentTableBuilt = true;
    resolveReady(state.torrentTable);
    refreshTorrentTableTheme(state.currentTheme);
    updateTorrentTableViewState();
  });
  state.torrentTable.on("rowClick", (event, row) => {
    if (event.target.closest("button, input, label, select")) return;
    openDetailsHandler(row.getData().info_hash);
  });
  state.torrentTable.on("dataFiltered", updateTorrentTableViewState);
  state.torrentTable.on("dataSorted", (sorters) => {
    handleTorrentTableSort(sorters);
    updateTorrentTableViewState();
  });
  state.torrentTable.on("renderComplete", updateTorrentTableViewState);
  return state.torrentTableReady;
}

export function torrentTableColumns() {
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
      width: 190,
      headerSort: false,
      resizable: false,
      formatter: () => renderTorrentActions(),
      cellClick: handleTorrentActionCellClick,
    },
  ];
}

export function isTorrentTableReady() {
  return !!state.torrentTable && state.torrentTableBuilt;
}

export async function setTorrentTableData(rows) {
  if (!state.torrentTable) return Promise.resolve();
  await state.torrentTableReady;
  const result = state.torrentTable.replaceData(rows);
  return result && typeof result.then === "function" ? result : Promise.resolve();
}

export function textCellFormatter(cell) {
  return escapeHtml(cell.getValue());
}

export function healthSorter(_a, _b, aRow, bRow) {
  return compareNumbers(aRow.getData().health_score, bRow.getData().health_score);
}

export function peerCountSorter(_a, _b, aRow, bRow) {
  const a = aRow.getData();
  const b = bRow.getData();
  return compareNumbers(a.active_peers, b.active_peers)
    || compareNumbers(a.known_peer_count, b.known_peer_count);
}

export function compareNumbers(a, b) {
  return (finiteNumber(a) ?? 0) - (finiteNumber(b) ?? 0);
}

export function numericHeaderFilter(headerValue, rowValue) {
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

export function parseNumericFilter(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const match = text.match(/^(<=|>=|!=|==|=|<|>)?\s*(-?(?:\d+(?:\.\d+)?|\.\d+))$/);
  if (!match) return null;
  return {
    operator: match[1] === "==" ? "=" : (match[1] || "="),
    value: Number(match[2]),
  };
}

export function torrentSelectionFormatter(cell, _formatterParams, onRendered) {
  onRendered(() => bindTorrentSelectionCheckbox(cell));
  return renderTorrentSelection(cell.getRow().getData());
}

export function bindTorrentSelectionCheckbox(cell) {
  const checkbox = cell.getElement().querySelector(".torrent-select");
  if (!checkbox) return;
  checkbox.addEventListener("click", event => event.stopPropagation());
  checkbox.addEventListener("change", () => {
    const row = cell.getRow();
    const data = row.getData();
    if (checkbox.checked) state.selectedTorrents.set(data.info_hash, torrentDisplayName(data));
    else state.selectedTorrents.delete(data.info_hash);
    row.getElement().classList.toggle("selected", checkbox.checked);
    updateSelectionControls();
  });
}

export function handleTorrentActionCellClick(event, cell) {
  const button = event.target.closest("button");
  if (!button) return;
  event.stopPropagation();
  const data = cell.getRow().getData();
  handleTorrentAction(button.dataset.act, data.info_hash, torrentDisplayName(data));
}

export function activeTorrentRows() {
  if (!isTorrentTableReady()) return [];
  try {
    return state.torrentTable.getRows("active");
  } catch {
    return state.torrentTable.getRows();
  }
}

export function updateVisibleTorrentsFromTable() {
  state.visibleTorrents = activeTorrentRows().map(row => {
    const data = row.getData();
    return {
      hash: data.info_hash,
      name: torrentDisplayName(data),
    };
  }).filter(t => t.hash);
}

export function updateTorrentTableViewState() {
  updateVisibleTorrentsFromTable();
  updateRenderedSelection();
  updateSelectionControls();
  updateClearFiltersButton();
}

export async function applyTorrentSearchFilter() {
  state.torrentQueryState.page = 1;
  scheduleTorrentRefresh();
}

export async function clearTorrentFilters() {
  $("#search").value = "";
  if ($("#torrent-state-filter")) $("#torrent-state-filter").value = "";
  if ($("#torrent-health-filter")) $("#torrent-health-filter").value = "";
  if ($("#torrent-performance-filter")) $("#torrent-performance-filter").value = "";
  if ($("#torrent-per-page")) $("#torrent-per-page").value = String(TORRENT_DEFAULT_PER_PAGE);
  state.torrentQueryState.page = 1;
  state.torrentQueryState.per_page = TORRENT_DEFAULT_PER_PAGE;
  state.torrentQueryState.sort = "name";
  state.torrentQueryState.dir = "asc";
  if (state.torrentTable) {
    await state.torrentTableReady;
    state.torrentTable.clearFilter(true);
  }
  scheduleTorrentRefresh();
  updateTorrentTableViewState();
}

export function updateClearFiltersButton() {
  const button = $("#clear-torrent-filters-btn");
  if (!button) return;
  const controls = collectTorrentQueryControls();
  const hasSearch = !!controls.q || !!controls.state || !!controls.health || !!controls.performance;
  const hasPerPageOverride = controls.per_page !== TORRENT_DEFAULT_PER_PAGE;
  let hasHeaderFilters = false;
  if (isTorrentTableReady()) {
    try { hasHeaderFilters = state.torrentTable.getHeaderFilters().length > 0; } catch {}
  }
  button.disabled = !hasSearch && !hasPerPageOverride && !hasHeaderFilters;
}

export function saveTorrentQueryView() {
  const base = collectTorrentQueryState();
  const payload = {
    q: base.q,
    state: base.state,
    health: base.health,
    performance: base.performance,
    per_page: base.per_page,
    sort: base.sort,
    dir: base.dir,
  };
  try {
    window.localStorage.setItem(TORRENT_QUERY_STORAGE_KEY, JSON.stringify(payload));
    showToast("Torrent view saved", "Default query view stored.", "success");
  } catch {
    showError("Save view failed", new Error("Could not write to local storage"));
  }
}

export function readSavedTorrentQueryView() {
  try {
    const raw = window.localStorage.getItem(TORRENT_QUERY_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return {
      q: String(parsed.q || "").trim(),
      state: String(parsed.state || "").trim(),
      health: String(parsed.health || "").trim(),
      performance: String(parsed.performance || "").trim(),
      per_page: clampTorrentPerPage(parsed.per_page),
      sort: normalizeTorrentSort(parsed.sort),
      dir: normalizeTorrentDir(parsed.dir),
    };
  } catch {
    return null;
  }
}

export function loadTorrentQueryView() {
  const loaded = readSavedTorrentQueryView();
  if (!loaded) {
    showToast("No saved view", "Save a view first.", "warning");
    return;
  }
  state.torrentQueryState = {
    ...torrentQueryState,
    ...loaded,
    page: 1,
  };
  applyTorrentQueryControls(state.torrentQueryState);
  refreshTorrents();
}

export function clearTorrentQueryView() {
  try {
    window.localStorage.removeItem(TORRENT_QUERY_STORAGE_KEY);
  } catch {}
  state.torrentQueryState.sort = "name";
  state.torrentQueryState.dir = "asc";
  state.torrentQueryState.q = "";
  state.torrentQueryState.state = "";
  state.torrentQueryState.health = "";
  state.torrentQueryState.performance = "";
  state.torrentQueryState.per_page = TORRENT_DEFAULT_PER_PAGE;
  state.torrentQueryState.page = 1;
  applyTorrentQueryControls(state.torrentQueryState);
  refreshTorrents();
  showToast("Saved view cleared", "", "info");
}

export function applySavedTorrentQueryView() {
  const saved = readSavedTorrentQueryView();
  if (!saved) return;
  state.torrentQueryState = {
    ...torrentQueryState,
    ...saved,
    page: 1,
  };
  applyTorrentQueryControls(state.torrentQueryState);
}

export function observeTorrentRemovals(list, observationKey, query) {
  const current = new Map((list || []).map(t => [t.info_hash, String(t.name || t.info_hash || "")]));
  const total = finiteNumber(query?.total);
  const filtered = finiteNumber(query?.filtered);
  const observesCompleteLibrary = !state.torrentQueryState.q
    && !state.torrentQueryState.state
    && !state.torrentQueryState.health
    && !state.torrentQueryState.performance
    && clampTorrentPage(query?.page) === 1
    && total !== null
    && filtered === total
    && current.size === total;
  if (!observesCompleteLibrary) {
    state.knownTorrents.clear();
    state.lastTorrentObservationKey = null;
    state.expectedRemovedTorrents.clear();
    state.torrentsLoaded = false;
    return;
  }
  if (observationKey !== state.lastTorrentObservationKey) {
    state.knownTorrents = current;
    state.lastTorrentObservationKey = observationKey;
    state.torrentsLoaded = true;
    return;
  }
  if (state.torrentsLoaded) {
    for (const [hash, name] of state.knownTorrents.entries()) {
      if (current.has(hash)) continue;
      if (state.expectedRemovedTorrents.has(hash)) {
        state.expectedRemovedTorrents.delete(hash);
        continue;
      }
      showToast("Torrent removed", name, "info");
    }
  }
  state.knownTorrents = current;
  state.torrentsLoaded = true;
}

export function torrentDisplayName(t) {
  return String(t?.name || t?.info_hash || "");
}

export function syncSelectedTorrents(list) {
  const current = new Map((list || []).map(t => [t.info_hash, torrentDisplayName(t)]));
  for (const hash of Array.from(state.selectedTorrents.keys())) {
    if (current.has(hash)) state.selectedTorrents.set(hash, current.get(hash));
    else state.selectedTorrents.delete(hash);
  }
}

export function renderPeerCount(t) {
  const active = finiteNumber(t.active_peer_workers);
  const known = finiteNumber(t.known_peers);
  if (active === null && known === null) return "";
  if (known === null) return String(active);
  if (active === null) return String(known);
  return `${active}/${known}`;
}

export function renderStatsSummary(stats) {
  const parts = [];
  const torrentCount = fmtCount(stats.torrent_count);
  const down = fmtRate(stats.download_rate);
  const up = fmtRate(stats.upload_rate);
  if (torrentCount) parts.push(`${torrentCount} torrents`);
  if (down) parts.push(`${down} down`);
  if (up) parts.push(`${up} up`);
  return parts.join(" · ");
}

export function renderTorrentActions() {
  return `<div class="torrent-actions">${TORRENT_ACTIONS.map(action => {
    const danger = action.danger ? " danger" : "";
    return `<button type="button" data-act="${action.act}" class="icon-button${danger}" aria-label="${action.label}" title="${action.label}">${action.icon}</button>`;
  }).join("")}</div>`;
}

export function renderTorrentSelection(t) {
  const checked = state.selectedTorrents.has(t.info_hash) ? " checked" : "";
  const name = torrentDisplayName(t);
  return `<input type="checkbox" class="torrent-select" data-hash="${escapeHtml(t.info_hash)}"${checked} aria-label="Select ${escapeHtml(name)}">`;
}

export function updateRenderedSelection() {
  if (!isTorrentTableReady()) return;
  state.torrentTable.getRows().forEach(row => {
    const data = row.getData();
    const selected = state.selectedTorrents.has(data.info_hash);
    row.getElement().classList.toggle("selected", selected);
    const cb = row.getElement().querySelector(".torrent-select");
    if (cb) cb.checked = selected;
  });
}

export function updateSelectionControls() {
  const selectedCount = state.selectedTorrents.size;
  const visibleCount = state.visibleTorrents.length;
  const allVisibleSelected = visibleCount > 0 && state.visibleTorrents.every(t => state.selectedTorrents.has(t.hash));
  const selectAll = $("#select-all-torrents-btn");
  const deselectAll = $("#deselect-all-torrents-btn");
  const removeSelected = $("#remove-selected-torrents-btn");
  const summary = $("#selection-summary");
  if (selectAll) selectAll.disabled = visibleCount === 0 || allVisibleSelected || state.bulkRemoveInFlight;
  if (deselectAll) deselectAll.disabled = selectedCount === 0 || state.bulkRemoveInFlight;
  if (removeSelected) removeSelected.disabled = selectedCount === 0 || state.bulkRemoveInFlight;
  if (summary) summary.textContent = `${selectedCount} selected`;
}

export function selectAllVisibleTorrents() {
  state.visibleTorrents.forEach(t => state.selectedTorrents.set(t.hash, t.name));
  updateRenderedSelection();
  updateSelectionControls();
}

export function deselectAllTorrents() {
  state.selectedTorrents.clear();
  updateRenderedSelection();
  updateSelectionControls();
}

export async function removeSelectedTorrents() {
  if (state.bulkRemoveInFlight) return;
  const selected = Array.from(state.selectedTorrents.entries());
  if (selected.length === 0) return;
  const noun = selected.length === 1 ? "torrent" : "torrents";
  const confirmed = window.confirm(`Remove ${selected.length} selected ${noun} from SwarmOtter? Downloaded data will be kept.`);
  if (!confirmed) return;
  state.bulkRemoveInFlight = true;
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
        state.expectedRemovedTorrents.set(hash, name);
        state.selectedTorrents.delete(hash);
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
    state.bulkRemoveInFlight = false;
    updateSelectionControls();
  }
}

export async function handleTorrentAction(act, hash, name) {
  try {
    if (act === "details") {
      await openDetailsHandler(hash);
      return;
    }
    if (act === "pause") await api(`/torrents/${hash}/pause`, { method: "POST" });
    else if (act === "resume") await api(`/torrents/${hash}/resume`, { method: "POST" });
    else if (act === "recheck") await api(`/torrents/${hash}/recheck`, { method: "POST" });
    else if (act === "remove") {
      const removal = await chooseTorrentRemoval(name);
      if (removal === "cancel") return;
      const deleteData = removal === "delete";
      await api(`/torrents/${hash}?delete_data=${deleteData}`, { method: "DELETE" });
      state.expectedRemovedTorrents.set(hash, name);
      state.selectedTorrents.delete(hash);
      showToast("Torrent removed", deleteData ? `${name}; downloaded data deleted` : `${name}; downloaded data kept`, "info");
    }
    refreshTorrents();
  } catch (e) {
    showError("Torrent action failed", e);
  }
}

export function chooseTorrentRemoval(name) {
  const dialog = $("#remove-torrent-dialog");
  const message = $("#remove-torrent-message");
  if (!dialog || typeof dialog.showModal !== "function") {
    if (!window.confirm(`Remove ${name} from SwarmOtter?`)) return Promise.resolve("cancel");
    return Promise.resolve(window.confirm("Delete the downloaded data too? Choose Cancel to keep it.") ? "delete" : "keep");
  }
  message.textContent = `${name} can be removed while keeping its downloaded data, or removed with its downloaded data permanently deleted.`;
  dialog.returnValue = "cancel";
  return new Promise(resolve => {
    dialog.addEventListener("close", () => resolve(dialog.returnValue || "cancel"), { once: true });
    dialog.showModal();
  });
}
// --- Add ---
$("#add-magnet-btn").addEventListener("click", async () => {
  if (state.magnetAddInFlight) return;
  const button = $("#add-magnet-btn");
  const input = $("#magnet-input");
  try {
    const magnet = input.value.trim();
    if (!magnet) {
      showToast("Enter a magnet link", "", "warning");
      return;
    }
    state.magnetAddInFlight = true;
    button.disabled = true;
    button.setAttribute("aria-busy", "true");
    showToast("Adding magnet", "", "info");
    const dir = $("#magnet-dir").value.trim();
    const body = { magnet, paused: $("#magnet-paused").checked };
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
    state.magnetAddInFlight = false;
    button.disabled = false;
    button.removeAttribute("aria-busy");
  }
});

$("#add-file-btn").addEventListener("click", async () => {
  try {
    const file = $("#torrent-file").files[0];
    if (!file) { showToast("Choose a .torrent file", "", "warning"); return; }
    const h = await uploadTorrentFile(file, $("#file-paused").checked);
    showToast("Torrent added", h, "success");
    refreshTorrents();
  } catch (e) { showError("Upload failed", e); }
});

export async function uploadTorrentFile(file, paused = false) {
  const buf = await file.arrayBuffer();
  return api(`/torrents/file?paused=${paused}`, {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: buf
  });
}

export function torrentFilesFromTransfer(items) {
  return Array.from(items || []).filter(file => file.name.toLowerCase().endsWith(".torrent"));
}

export async function uploadDroppedFiles(files) {
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

export function setDropActive(active) {
  $("#drop-overlay").classList.toggle("hidden", !active);
}

export function hasDroppedFiles(e) {
  return e.dataTransfer && Array.from(e.dataTransfer.types || []).includes("Files");
}

document.addEventListener("dragenter", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  state.dragDepth++;
  setDropActive(true);
});
document.addEventListener("dragover", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  e.dataTransfer.dropEffect = "copy";
});
document.addEventListener("dragleave", (e) => {
  if (!hasDroppedFiles(e)) return;
  state.dragDepth = Math.max(0, state.dragDepth - 1);
  if (state.dragDepth === 0) setDropActive(false);
});
document.addEventListener("drop", (e) => {
  if (!hasDroppedFiles(e)) return;
  e.preventDefault();
  state.dragDepth = 0;
  setDropActive(false);
  uploadDroppedFiles(e.dataTransfer.files);
});
$("#search").addEventListener("input", applyTorrentSearchFilter);
$("#clear-torrent-filters-btn").addEventListener("click", clearTorrentFilters);
$("#torrent-state-filter").addEventListener("change", () => {
  state.torrentQueryState.page = 1;
  scheduleTorrentRefresh();
});
$("#torrent-health-filter").addEventListener("change", () => {
  state.torrentQueryState.page = 1;
  scheduleTorrentRefresh();
});
$("#torrent-performance-filter").addEventListener("change", () => {
  state.torrentQueryState.page = 1;
  scheduleTorrentRefresh();
});
$("#torrent-per-page").addEventListener("change", () => {
  state.torrentQueryState.per_page = clampTorrentPerPage($("#torrent-per-page").value);
  state.torrentQueryState.page = 1;
  scheduleTorrentRefresh();
});
$("#torrent-prev-page-btn").addEventListener("click", () => setTorrentPage(state.torrentQueryState.page - 1));
$("#torrent-next-page-btn").addEventListener("click", () => setTorrentPage(state.torrentQueryState.page + 1));
$("#save-torrent-view-btn").addEventListener("click", saveTorrentQueryView);
$("#load-torrent-view-btn").addEventListener("click", loadTorrentQueryView);
$("#clear-torrent-view-btn").addEventListener("click", clearTorrentQueryView);
$("#select-all-torrents-btn").addEventListener("click", selectAllVisibleTorrents);
$("#deselect-all-torrents-btn").addEventListener("click", deselectAllTorrents);
$("#remove-selected-torrents-btn").addEventListener("click", removeSelectedTorrents);
