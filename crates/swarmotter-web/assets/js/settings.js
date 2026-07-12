// SPDX-License-Identifier: Apache-2.0

import { state, TORRENT_QUERY_STORAGE_KEY, TORRENT_DEFAULT_PER_PAGE, TORRENT_MAX_PER_PAGE, TORRENT_SORT_OPTIONS, TORRENT_TABLE_TO_QUERY_SORT, TORRENT_QUERY_TO_TABLE_SORT, TORRENT_ACTIONS, EVENT_KINDS, MAX_LOG_LINES, watchHistoryUi } from "./state.js";
import * as ui from "./ui.js";
import * as requests from "./api.js";
const { $, $$, showToast, showError, finiteNumber, fmtCount, fmtBytes, fmtRate, fmtRatio, fmtPercentFromFraction, fmtPercent, fmtProgress, fmtUnixSeconds, renderProgressCell, renderHealth, renderHealthSummary, fmtScore, renderStatus, renderKv, renderCheckList, escapeHtml, cssToken, log, setToastDisplaySeconds } = ui;
const { api, apiFetch, responseErrorMessage, saveApiToken } = requests;

let settingsDependencies = {
  refreshTorrents: async () => ({}), refreshLogs: async () => {}, refreshDoctorBadge: async () => {},
};
export function setSettingsDependencies(dependencies) { settingsDependencies = { ...settingsDependencies, ...dependencies }; }

export async function refreshSettings() {
  try {
    const cfg = await api("/settings");
    state.fullConfigSnapshot = cfg;
    renderSettingsEditor(cfg);
  } catch (e) { log("settings error: " + e.message); }
}

export function settingsField(id) {
  return $("#" + id);
}

export function setSettingsValue(id, value) {
  const el = settingsField(id);
  if (el) el.value = value ?? "";
}

export function setSettingsChecked(id, value) {
  const el = settingsField(id);
  if (el) el.checked = !!value;
}

export function settingsString(id) {
  return settingsField(id).value.trim();
}

export function settingsOptionalString(id) {
  const value = settingsString(id);
  return value ? value : null;
}

export function settingsInteger(id, fallback = 0) {
  const value = settingsField(id).value;
  if (value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

export function settingsFloatOrNull(id) {
  const value = settingsField(id).value;
  if (value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function settingsIntegerOrNull(id) {
  const value = settingsField(id).value;
  if (value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

export function settingsLineList(id) {
  return settingsField(id).value
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
}

export function renderSettingsEditor(cfg) {
  const apiCfg = cfg.api || {};
  const compatibility = cfg.compatibility || {};
  const transmission = compatibility.transmission || {};
  const qbittorrent = compatibility.qbittorrent || {};
  const autopilot = cfg.autopilot || {};
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
  setSettingsChecked("cfg-compat-qbittorrent-enabled", qbittorrent.enabled);

  setSettingsValue("cfg-autopilot-mode", autopilot.mode || "act");

  setSettingsValue("cfg-storage-download-dir", storage.download_dir);
  setSettingsValue("cfg-storage-incomplete-dir", storage.incomplete_dir);
  setSettingsValue("cfg-storage-minimum-free-space-bytes", storage.minimum_free_space_bytes);
  setSettingsValue("cfg-storage-minimum-free-space-percent", storage.minimum_free_space_percent);
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

  setSettingsValue("cfg-torrent-encryption-mode", torrent.encryption_mode || "preferred");
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
  setSettingsValue("cfg-queue-max-active-metadata-fetches", queue.max_active_metadata_fetches);
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
  setSettingsValue("toast-seconds", String(Math.round(state.toastDisplayMs / 1000)));
  activateSettingsPanel(state.activeSettingsPanel, { focus: false });
}

export function activateSettingsPanel(panelName, options = {}) {
  const panels = $$("[data-settings-panel]");
  if (!panels.length) return;
  const target = panels.some(panel => panel.dataset.settingsPanel === panelName)
    ? panelName
    : "api";
  state.activeSettingsPanel = target;
  panels.forEach(panel => {
    const active = panel.dataset.settingsPanel === target;
    panel.classList.toggle("active", active);
    panel.hidden = !active;
  });
  $$(".settings-nav-item").forEach(button => {
    const active = button.dataset.settingsTarget === target;
    button.classList.toggle("active", active);
    button.setAttribute("aria-current", active ? "page" : "false");
  });
  if (options.focus) {
    const panel = $(`[data-settings-panel="${target}"]`);
    if (panel) panel.focus({ preventScroll: true });
  }
}

export function collectSettingsConfig() {
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
      qbittorrent: {
        enabled: settingsField("cfg-compat-qbittorrent-enabled").checked,
      },
    },
    autopilot: {
      mode: settingsString("cfg-autopilot-mode"),
    },
    storage: {
      download_dir: settingsOptionalString("cfg-storage-download-dir"),
      incomplete_dir: settingsOptionalString("cfg-storage-incomplete-dir"),
      minimum_free_space_bytes: settingsInteger("cfg-storage-minimum-free-space-bytes", 0),
      minimum_free_space_percent: settingsInteger("cfg-storage-minimum-free-space-percent", 0),
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
      encryption_mode: settingsString("cfg-torrent-encryption-mode"),
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
      max_active_metadata_fetches: settingsInteger("cfg-queue-max-active-metadata-fetches"),
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

export function renderWatchFolderEditors(folders) {
  const list = $("#settings-watch-list");
  if (!list) return;
  if (!folders || folders.length === 0) {
    list.innerHTML = `<p class="muted">No watch folders configured.</p>`;
    return;
  }
  list.innerHTML = folders.map((folder, index) => renderWatchFolderEditor(folder, index)).join("");
}

export function renderWatchFolderEditor(folder, index) {
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

export function collectWatchFolderEditors() {
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

export async function replaceSettingsWithRuntimeFallback(nextConfig) {
  try {
    const result = await api("/settings", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(nextConfig),
    });
    return { result, persistenceError: null };
  } catch (error) {
    if (error?.status !== 500) throw error;
    await api("/settings", {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        bandwidth: nextConfig.bandwidth,
        queue: nextConfig.queue,
        seeding: nextConfig.seeding,
        autopilot: nextConfig.autopilot,
      }),
    });
    return { result: null, persistenceError: error };
  }
}

export function watchInput(row, field) {
  return row.querySelector(`[data-watch-field="${field}"]`);
}

export function watchOptionalString(row, field) {
  const value = watchInput(row, field).value.trim();
  return value ? value : null;
}

$("#settings-editor").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  if (!form.checkValidity()) {
    const invalid = form.querySelector(":invalid");
    const panel = invalid?.closest("[data-settings-panel]")?.dataset.settingsPanel;
    if (panel) activateSettingsPanel(panel, { focus: false });
    form.reportValidity();
    return;
  }
  const status = $("#settings-save-status");
  try {
    const nextConfig = collectSettingsConfig();
    const { result, persistenceError } = await replaceSettingsWithRuntimeFallback(nextConfig);
    if (persistenceError) {
      await refreshSettings();
      renderRuntimeSettingsFallbackStatus(persistenceError);
      showToast("Runtime settings applied", "Full configuration was not persisted; non-runtime fields were not changed.", "warning");
    } else {
      if (nextConfig.api.auth_token) saveApiToken(nextConfig.api.auth_token);
      state.fullConfigSnapshot = result.config;
      renderSettingsEditor(result.config);
      renderConfigSaveStatus(result);
      showToast("Configuration saved", result.restart_required ? "Restart required for some fields" : "", "success");
    }
    await settingsDependencies.refreshDoctorBadge();
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
  let result = null;
  let resetError = null;
  try {
    result = await api("/reset", { method: "POST" });
  } catch (e) {
    resetError = e;
  }
  try {
    state.currentHash = null;
    state.knownTorrents.clear();
    state.lastTorrentObservationKey = null;
    state.expectedRemovedTorrents.clear();
    state.selectedTorrents.clear();
    state.torrentsLoaded = false;
    const query = await settingsDependencies.refreshTorrents();
    if (!$("#view-logs").classList.contains("hidden")) await settingsDependencies.refreshLogs();
    await settingsDependencies.refreshDoctorBadge();
    if (resetError) {
      showError("Reset failed", resetError);
      return;
    }
    const remaining = finiteNumber(query?.total);
    if (remaining !== null && remaining > 0) {
      showError("Reset incomplete", new Error(`${fmtCount(remaining)} torrents are still listed after reset.`));
      return;
    }
    const detail = [
      `${fmtCount(result.torrents_removed)} torrents`,
      `${fmtCount(result.storage_entries_removed)} storage entries`,
      `${fmtCount(result.log_files_cleared)} log files`,
    ].join(" cleared; ");
    showToast("Reset complete", detail, "success");
  } catch (e) {
    showError("Reset refresh failed", e);
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

export function renderConfigSaveStatus(result) {
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

export function renderRuntimeSettingsFallbackStatus(error) {
  $("#settings-save-status").innerHTML = `
    <h3>Save status</h3>
    ${renderCheckList([{
      id: "runtime_only",
      label: "Runtime-only fallback",
      level: "warning",
      detail: `Bandwidth, queue, seeding, and autopilot settings were applied in memory. Full persistence failed: ${error.message || error}`,
      remediation: "Make the configured file writable by the SwarmOtter service before changing other settings.",
    }])}`;
}
$$(".settings-nav-item").forEach(button => {
  button.addEventListener("click", () => {
    activateSettingsPanel(button.dataset.settingsTarget, { focus: true });
  });
});
