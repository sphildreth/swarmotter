// SPDX-License-Identifier: Apache-2.0
// Shared mutable client state. Feature modules communicate through app.js callbacks.

export const API = "/api/v1";
export const watchHistoryUi = globalThis.SwarmOtterWatchHistory;
export const DEFAULT_TOAST_DISPLAY_MS = 5000;
export const MAX_TOAST_DISPLAY_MS = 60000;
export const MAX_VISIBLE_TOASTS = 3;
export const MAX_LOG_LINES = 500;
export const TOAST_DISPLAY_STORAGE_KEY = "swarmotter.toastDisplayMs";
export const THEME_STORAGE_KEY = "swarmotter.theme";
export const TORRENT_QUERY_STORAGE_KEY = "swarmotter.torrentQueryView";
export const API_TOKEN_STORAGE_KEY = "swarmotter.apiToken";
export const TORRENT_DEFAULT_PER_PAGE = 200;
export const TORRENT_MAX_PER_PAGE = 500;
export const THEME_DARK = "dark";
export const THEME_LIGHT = "light";
export const DEFAULT_THEME = THEME_DARK;
export const TORRENT_SORT_OPTIONS = new Set([
  "name",
  "state",
  "health",
  "health_score",
  "progress",
  "size",
  "down_rate",
  "up_rate",
  "ratio",
  "peers",
  "added",
  "completed",
  "queue",
]);
export const TORRENT_TABLE_TO_QUERY_SORT = {
  name: "name",
  state: "state",
  health_label: "health",
  total_length: "size",
  progress_percent: "progress",
  rate_down: "down_rate",
  rate_up: "up_rate",
  ratio: "ratio",
  active_peers: "peers",
};
export const TORRENT_QUERY_TO_TABLE_SORT = {
  name: "name",
  state: "state",
  health: "health_label",
  health_score: "health_label",
  progress: "progress_percent",
  size: "total_length",
  down_rate: "rate_down",
  up_rate: "rate_up",
  ratio: "ratio",
  peers: "active_peers",
  added: "name",
  completed: "name",
  queue: "name",
};

export const EVENT_KINDS = [
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

export const TORRENT_ACTIONS = [
  {
    act: "details",
    label: "Details",
    icon: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg>`,
  },
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

export const state = {
  currentHash: null,
  toastDisplayMs: null,
  currentTheme: null,
  torrentsLoaded: false,
  knownTorrents: new Map(),
  lastTorrentObservationKey: null,
  expectedRemovedTorrents: new Map(),
  selectedTorrents: new Map(),
  visibleTorrents: [],
  torrentTable: null,
  torrentTableBuilt: false,
  torrentTableReady: Promise.resolve(),
  bulkRemoveInFlight: false,
  magnetAddInFlight: false,
  logEventStreamController: null,
  lastEventStreamErrorAt: 0,
  fullConfigSnapshot: null,
  autopilotModeUpdateInFlight: false,
  activeSettingsPanel: "api",
  torrentQueryRefreshTimer: null,
  isApplyingSortFromServer: false,
  apiTokenPromptInFlight: null,
  dragDepth: 0,
  torrentQueryState: {
    q: "", state: "", health: "", performance: "",
    sort: "name", dir: "asc", page: 1, per_page: TORRENT_DEFAULT_PER_PAGE,
  },
};
