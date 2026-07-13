// SPDX-License-Identifier: Apache-2.0
// SwarmOtter Web UI ES-module entry. This file alone composes feature modules.

import { state } from "./js/state.js";
import { $, $$, applyTheme, toggleTheme, setThemeRefreshHandler, setLogHandler } from "./js/ui.js";
import { refreshTorrents, refreshTorrentTableTheme, setOpenDetailsHandler, applySavedTorrentQueryView, refreshProfileChoices } from "./js/torrents.js";
import { openDetails, setRefreshTorrentsHandler } from "./js/details.js";
import { refreshSettings, setSettingsDependencies } from "./js/settings.js";
import { refreshNetwork, refreshWatch, refreshLogs, refreshDoctor, refreshDoctorBadge, appendLogLine, setEventsDependencies } from "./js/events.js";

setOpenDetailsHandler(openDetails);
setRefreshTorrentsHandler(refreshTorrents);
setSettingsDependencies({ refreshTorrents, refreshLogs, refreshDoctorBadge });
setEventsDependencies({ refreshSettings });
setThemeRefreshHandler(refreshTorrentTableTheme);
setLogHandler(appendLogLine);

function openView(view, activeButton = null) {
  state.currentHash = null;
  $$(".nav").forEach(button => button.classList.remove("active"));
  if (activeButton?.classList.contains("nav")) activeButton.classList.add("active");
  $$(".view").forEach(element => element.classList.add("hidden"));
  $("#view-" + view).classList.remove("hidden");
  if (view === "torrents") refreshTorrents();
  if (view === "add") refreshProfileChoices();
  if (view === "network") refreshNetwork();
  if (view === "settings") refreshSettings();
  if (view === "watch") refreshWatch();
  if (view === "logs") refreshLogs();
  if (view === "doctor") refreshDoctor();
}

$$(".nav").forEach(button => button.addEventListener("click", () => openView(button.dataset.view, button)));
const themeToggle = $("#theme-toggle");
if (themeToggle) themeToggle.addEventListener("click", toggleTheme);

(async function init() {
  applyTheme(state.currentTheme, { persist: false });
  applySavedTorrentQueryView();
  await refreshTorrents();
  await refreshDoctorBadge();
  window.setInterval(refreshTorrents, 5000);
  window.setInterval(refreshDoctorBadge, 10000);
})();
