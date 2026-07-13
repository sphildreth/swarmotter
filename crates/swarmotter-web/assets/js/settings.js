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
    await refreshPeerFilterStatus();
  } catch (e) { log("settings error: " + e.message); }
}

export async function refreshPeerFilterStatus() {
  try {
    const status = await api("/peer-filter");
    renderPeerFilterStatus(status);
    return status;
  } catch (e) {
    renderPeerFilterStatus(null, e);
    log("peer admission status error: " + e.message);
    return null;
  }
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

export function collectPolicyProfiles() {
  const raw = settingsString("cfg-profiles-json");
  if (!raw) return { profiles: {}, labels: {} };
  let profiles;
  try {
    profiles = JSON.parse(raw);
  } catch (error) {
    throw new Error(`Policy profiles must be valid JSON: ${error.message}`);
  }
  if (!profiles || Array.isArray(profiles) || typeof profiles !== "object") {
    throw new Error("Policy profiles must be a JSON object with profiles and labels.");
  }
  return profiles;
}

export function renderSettingsEditor(cfg) {
  const apiCfg = cfg.api || {};
  const compatibility = cfg.compatibility || {};
  const transmission = compatibility.transmission || {};
  const qbittorrent = compatibility.qbittorrent || {};
  const autopilot = cfg.autopilot || {};
  const storage = cfg.storage || {};
  const network = cfg.network || {};
  const portMapping = cfg.port_mapping || {};
  const portTest = cfg.port_test || {};
  const torrent = cfg.torrent || {};
  const bandwidth = cfg.bandwidth || {};
  const queue = cfg.queue || {};
  const profiles = cfg.profiles || { profiles: {}, labels: {} };
  const seeding = cfg.seeding || {};
  const dht = cfg.dht || {};
  const pex = cfg.pex || {};
  const peerFilter = cfg.peer_filter || {};
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
  renderStorageRootControlEditors(storage.root_controls || []);

  setSettingsValue("cfg-network-mode", network.mode || "disabled");
  setSettingsValue("cfg-network-required-interface", network.required_interface);
  setSettingsValue("cfg-network-required-source-ipv4", network.required_source_ipv4);
  setSettingsValue("cfg-network-required-source-ipv6", network.required_source_ipv6);
  setSettingsValue("cfg-network-required-network-namespace", network.required_network_namespace);
  setSettingsChecked("cfg-network-allow-ipv6", network.allow_ipv6);
  setSettingsChecked("cfg-network-fail-closed", network.fail_closed);
  setSettingsChecked("cfg-network-validate-route", network.validate_route);
  setSettingsChecked("cfg-network-validate-dns", network.validate_dns);

  const mappingProtocols = Array.isArray(portMapping.protocols)
    ? portMapping.protocols
    : ["nat_pmp", "upnp"];
  setSettingsChecked("cfg-port-mapping-enabled", portMapping.enabled);
  setSettingsChecked("cfg-port-mapping-nat-pmp", mappingProtocols.includes("nat_pmp"));
  setSettingsChecked("cfg-port-mapping-upnp", mappingProtocols.includes("upnp"));
  setSettingsValue("cfg-port-mapping-nat-pmp-gateway", portMapping.nat_pmp_gateway);
  setSettingsValue("cfg-port-mapping-upnp-service-url", portMapping.upnp_service_url);
  setSettingsValue("cfg-port-mapping-lease-seconds", portMapping.lease_seconds);
  setSettingsValue("cfg-port-mapping-refresh-before-expiry-seconds", portMapping.refresh_before_expiry_seconds);

  setSettingsChecked("cfg-port-test-enabled", portTest.enabled);
  setSettingsValue("cfg-port-test-endpoint", portTest.endpoint);
  setSettingsValue("cfg-port-test-cache-ttl-seconds", portTest.cache_ttl_seconds);
  setSettingsValue("cfg-port-test-timeout-seconds", portTest.timeout_seconds);

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
  setSettingsValue("cfg-profiles-json", JSON.stringify(profiles, null, 2));

  setSettingsValue("cfg-seeding-global-ratio-limit", seeding.global_ratio_limit);
  setSettingsValue("cfg-seeding-global-idle-limit", seeding.global_idle_limit);

  setSettingsValue("cfg-dht-port", dht.port);
  setSettingsChecked("cfg-dht-enabled", dht.enabled);
  setSettingsValue("cfg-dht-bootstrap-nodes", (dht.bootstrap_nodes || []).join("\n"));

  setSettingsValue("cfg-pex-max-peers", pex.max_peers);
  setSettingsChecked("cfg-pex-enabled", pex.enabled);

  setSettingsChecked("cfg-peer-filter-enabled", peerFilter.enabled);
  setSettingsValue("cfg-peer-filter-rules", (peerFilter.rules || []).join("\n"));
  setSettingsValue("cfg-peer-filter-blocklist-paths", (peerFilter.blocklist_paths || []).join("\n"));
  setSettingsValue("cfg-peer-filter-client-ids", (peerFilter.blocked_client_ids || []).join("\n"));

  setSettingsValue("cfg-logging-level", logging.level || "info");
  setSettingsValue("cfg-logging-file-path", logging.file_path);
  setSettingsChecked("cfg-logging-json", logging.json);
  setSettingsChecked("cfg-logging-file", logging.file);

  renderWatchFolderEditors(cfg.watch || []);
  setSettingsValue("toast-seconds", String(Math.round(state.toastDisplayMs / 1000)));
  activateSettingsPanel(state.activeSettingsPanel, { focus: false });
}

export function renderPeerFilterStatus(status, error = null) {
  const summary = $("#peer-filter-status");
  const manualBans = $("#peer-filter-manual-bans");
  if (!summary && !manualBans) return;
  if (!status) {
    const detail = error?.message || "Peer-admission status is unavailable.";
    if (summary) {
      summary.innerHTML = `<h4>Live policy status</h4>${renderCheckList([{
        id: "peer_filter_status",
        label: "Peer-admission status",
        level: "warning",
        detail,
      }])}`;
    }
    if (manualBans) {
      manualBans.innerHTML = `<h4>Global manual bans</h4><p class="muted">Manual-ban status is unavailable.</p>`;
    }
    return;
  }

  const sources = Array.isArray(status.sources) ? status.sources : [];
  const rejections = status.rejections || {};
  const rules = Array.isArray(status.rules) ? status.rules : [];
  const statusLevel = status.fail_closed_detail ? "invalid" : status.enabled ? "ok" : "warning";
  const sourceTable = sources.length
    ? `<table class="peer-filter-source-table">
        <thead><tr><th>Local blocklist path</th><th>Rules loaded</th><th>Skipped rows</th></tr></thead>
        <tbody>${sources.map(source => `<tr>
          <td>${escapeHtml(source.path || "")}</td>
          <td>${fmtCount(source.rules_loaded)}</td>
          <td>${fmtCount(source.skipped_lines)}</td>
        </tr>`).join("")}</tbody>
      </table>`
    : `<p class="muted">No local blocklist files are active.</p>`;
  const rulesDetail = rules.length
    ? `<details class="peer-filter-rules"><summary>Configured address rules (${fmtCount(rules.length)})</summary><pre>${escapeHtml(rules.join("\n"))}</pre></details>`
    : "";

  if (summary) {
    summary.innerHTML = `
      <h4>Live policy status</h4>
      ${renderStatus(statusLevel)}
      ${renderKv([
        ["Admission filtering", status.enabled ? "enabled" : "disabled"],
        ["Configured address rules", fmtCount(status.configured_rule_count)],
        ["Imported address rules", fmtCount(status.imported_rule_count)],
        ["Manual IP bans", fmtCount((status.manual_bans || []).length)],
        ["Blocked client-ID prefixes", fmtCount((status.blocked_client_ids || []).length)],
        ["IP admission checks", fmtCount(rejections.ip_checks)],
        ["Peer-ID checks", fmtCount(rejections.client_id_checks)],
        ["Manual-ban rejections", fmtCount(rejections.manual_bans)],
        ["Configured-rule rejections", fmtCount(rejections.configured_rules)],
        ["Imported-rule rejections", fmtCount(rejections.imported_rules)],
        ["Client-ID rejections", fmtCount(rejections.client_ids)],
        ["Fail-closed rejections", fmtCount(rejections.fail_closed)],
      ])}
      ${status.fail_closed_detail ? `<p class="status-detail status-invalid">${escapeHtml(status.fail_closed_detail)}</p>` : ""}
      <h5>Local import outcomes</h5>
      ${sourceTable}
      ${rulesDetail}`;
  }

  if (manualBans) {
    renderPeerFilterManualBans(manualBans, status.manual_bans || []);
  }
}

export function renderPeerFilterManualBans(container, bans) {
  const rows = Array.isArray(bans) ? bans : [];
  container.innerHTML = `
    <h4>Global manual bans</h4>
    <p class="muted">Manual bans apply to every torrent. Remove a ban here without selecting a torrent.</p>
    ${rows.length ? `<table class="peer-filter-manual-ban-table">
      <thead><tr><th>IP address</th><th>Reason</th><th>Action</th></tr></thead>
      <tbody>${rows.map(ban => `<tr>
        <td>${escapeHtml(ban.ip || "")}</td>
        <td>${escapeHtml(ban.reason || "")}</td>
        <td><button type="button" class="secondary peer-filter-unban" data-peer-filter-unban data-peer-filter-ip="${escapeHtml(ban.ip || "")}">Unban</button></td>
      </tr>`).join("")}</tbody>
    </table>` : `<p class="muted">No global manual bans are configured.</p>`}`;
  container.querySelectorAll("[data-peer-filter-unban]").forEach(button => {
    button.addEventListener("click", () => unbanManualPeer(button.dataset.peerFilterIp, button));
  });
}

export async function unbanManualPeer(ip, button = null) {
  if (!ip) return;
  if (!window.confirm(`Remove the global manual ban for ${ip}?`)) return;
  if (button) button.disabled = true;
  try {
    const status = await api("/peer-filter/unban", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ip }),
    });
    if (state.fullConfigSnapshot) {
      state.fullConfigSnapshot.peer_filter ||= {};
      state.fullConfigSnapshot.peer_filter.manual_bans = status.manual_bans || [];
    }
    renderPeerFilterStatus(status);
    showToast("Peer IP unbanned", `${ip} is no longer blocked globally`, "success");
  } catch (e) {
    showError("Remove peer ban failed", e);
  } finally {
    if (button) button.disabled = false;
  }
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
      root_controls: collectStorageRootControlEditors(),
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
    port_test: {
      enabled: settingsField("cfg-port-test-enabled").checked,
      endpoint: settingsOptionalString("cfg-port-test-endpoint"),
      cache_ttl_seconds: settingsInteger("cfg-port-test-cache-ttl-seconds", 900),
      timeout_seconds: settingsInteger("cfg-port-test-timeout-seconds", 10),
    },
    port_mapping: {
      enabled: settingsField("cfg-port-mapping-enabled").checked,
      protocols: [
        ...(settingsField("cfg-port-mapping-nat-pmp").checked ? ["nat_pmp"] : []),
        ...(settingsField("cfg-port-mapping-upnp").checked ? ["upnp"] : []),
      ],
      nat_pmp_gateway: settingsOptionalString("cfg-port-mapping-nat-pmp-gateway"),
      upnp_service_url: settingsOptionalString("cfg-port-mapping-upnp-service-url"),
      lease_seconds: settingsInteger("cfg-port-mapping-lease-seconds", 3600),
      refresh_before_expiry_seconds: settingsInteger("cfg-port-mapping-refresh-before-expiry-seconds", 300),
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
    profiles: collectPolicyProfiles(),
    dht: {
      enabled: settingsField("cfg-dht-enabled").checked,
      bootstrap_nodes: settingsLineList("cfg-dht-bootstrap-nodes"),
      port: settingsInteger("cfg-dht-port", 51413),
    },
    pex: {
      enabled: settingsField("cfg-pex-enabled").checked,
      max_peers: settingsInteger("cfg-pex-max-peers"),
    },
    peer_filter: {
      enabled: settingsField("cfg-peer-filter-enabled").checked,
      rules: settingsLineList("cfg-peer-filter-rules"),
      blocklist_paths: settingsLineList("cfg-peer-filter-blocklist-paths"),
      // Manual bans are managed by the peer action API. Preserve the latest
      // loaded list so an unrelated Settings save never erases one.
      manual_bans: state.fullConfigSnapshot?.peer_filter?.manual_bans || [],
      blocked_client_ids: settingsLineList("cfg-peer-filter-client-ids"),
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

export function renderStorageRootControlEditors(controls) {
  const list = $("#settings-storage-root-controls");
  if (!list) return;
  if (!controls || controls.length === 0) {
    list.innerHTML = `<p class="muted">No per-root controls configured.</p>`;
    return;
  }
  list.innerHTML = controls
    .map((control, index) => renderStorageRootControlEditor(control, index))
    .join("");
}

export function renderStorageRootControlEditor(control, index) {
  const cfg = control || {};
  return `
    <div class="storage-root-control-editor">
      <div class="storage-root-control-header">
        <strong>Root ${index + 1}</strong>
        <button type="button" class="icon-button danger" data-settings-action="remove-storage-root-control" aria-label="Remove storage root control" title="Remove storage root control">
          <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M6 6l1 15h10l1-15"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
        </button>
      </div>
      <div class="settings-form-grid">
        <label class="settings-field"><span>Root path</span><input data-storage-root-control-field="path" type="text" required value="${escapeHtml(cfg.path || "")}"></label>
        <label class="settings-field"><span>Max active downloads</span><input data-storage-root-control-field="max_active_downloads" type="number" min="0" step="1" value="${escapeHtml(String(cfg.max_active_downloads ?? 0))}"></label>
        <label class="settings-field"><span>Max active bytes</span><input data-storage-root-control-field="max_active_bytes" type="number" min="0" step="1" value="${escapeHtml(String(cfg.max_active_bytes ?? 0))}"></label>
        <label class="settings-field"><span>Max write bytes/sec</span><input data-storage-root-control-field="max_write_bytes_per_second" type="number" min="0" step="1" value="${escapeHtml(String(cfg.max_write_bytes_per_second ?? 0))}"></label>
        <label class="settings-field"><span>Max concurrent rechecks</span><input data-storage-root-control-field="max_concurrent_rechecks" type="number" min="0" step="1" value="${escapeHtml(String(cfg.max_concurrent_rechecks ?? 0))}"></label>
      </div>
    </div>`;
}

export function collectStorageRootControlEditors() {
  return $$("#settings-storage-root-controls .storage-root-control-editor").map(row => ({
    path: storageRootControlInput(row, "path").value.trim(),
    max_active_downloads: storageRootControlInteger(row, "max_active_downloads"),
    max_active_bytes: storageRootControlInteger(row, "max_active_bytes"),
    max_write_bytes_per_second: storageRootControlInteger(row, "max_write_bytes_per_second"),
    max_concurrent_rechecks: storageRootControlInteger(row, "max_concurrent_rechecks"),
  }));
}

export function storageRootControlInput(row, field) {
  return row.querySelector(`[data-storage-root-control-field="${field}"]`);
}

export function storageRootControlInteger(row, field) {
  const value = Number(storageRootControlInput(row, field).value);
  return Number.isFinite(value) && value >= 0 ? Math.trunc(value) : 0;
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
        <label class="settings-field"><span>Policy profile</span><input data-watch-field="profile" type="text" value="${escapeHtml(cfg.profile || "")}"></label>
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
    profile: watchOptionalString(row, "profile"),
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
      await refreshPeerFilterStatus();
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
    profile: null,
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

$("#add-storage-root-control-btn").addEventListener("click", () => {
  const controls = collectStorageRootControlEditors();
  controls.push({
    path: "",
    max_active_downloads: 0,
    max_active_bytes: 0,
    max_write_bytes_per_second: 0,
    max_concurrent_rechecks: 0,
  });
  renderStorageRootControlEditors(controls);
  const rows = $$("#settings-storage-root-controls .storage-root-control-editor");
  const last = rows[rows.length - 1];
  if (last) storageRootControlInput(last, "path").focus();
});

$("#settings-storage-root-controls").addEventListener("click", (event) => {
  const remove = event.target.closest('[data-settings-action="remove-storage-root-control"]');
  if (!remove) return;
  remove.closest(".storage-root-control-editor")?.remove();
  if ($$("#settings-storage-root-controls .storage-root-control-editor").length === 0) {
    renderStorageRootControlEditors([]);
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
