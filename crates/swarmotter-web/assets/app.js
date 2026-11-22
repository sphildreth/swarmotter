// SPDX-License-Identifier: Apache-2.0
// SwarmOtter Web UI ES-module entry. This file alone composes feature modules.

import { refreshTelemetry } from "./js/workspace.js";
import { state } from "./js/state.js";
import { $, $$, applyTheme, toggleTheme, setThemeRefreshHandler, setLogHandler } from "./js/ui.js";
import { pollTorrents, refreshTorrents, refreshTorrentTableTheme, setOpenDetailsHandler, applySavedTorrentQueryView, refreshProfileChoices } from "./js/torrents.js";
import { openDetails, setRefreshTorrentsHandler } from "./js/details.js";
import { refreshSettings, setSettingsDependencies } from "./js/settings.js";
import { refreshNetwork, refreshWatch, refreshLogs, refreshDoctor, refreshDoctorBadge, appendLogLine, setEventsDependencies } from "./js/events.js";
import { refreshRss } from "./rss.js";
import { refreshTrash } from "./js/trash.js";
import "./js/create.js";
import { refreshIntake } from "./js/intake.js";

setOpenDetailsHandler(openDetails);
setRefreshTorrentsHandler(refreshTorrents);
setSettingsDependencies({ refreshTorrents, refreshLogs, refreshDoctorBadge });
setEventsDependencies({ refreshSettings });
setThemeRefreshHandler(refreshTorrentTableTheme);
setLogHandler(appendLogLine);

function openView(view, activeButton = null) {
  state.currentHash = null;
  for (const pane of $$(".workspace")) pane.classList.remove("show-sections","show-inspector");
  for (const btn of $$("[data-reveal]")) btn.setAttribute("aria-expanded","false");
  $$(".nav").forEach(button => button.classList.remove("active"));
  $$(".nav").forEach(button => {
    const active = button === activeButton;
    button.classList.toggle("active", active);
    button.setAttribute("aria-current", active ? "page" : "false");
  });
  $$(".view").forEach(element => element.classList.add("hidden"));
  $("#view-" + view).classList.remove("hidden");
  if (view === "torrents") refreshTorrents();
  if (view === "add") { refreshProfileChoices().then(refreshIntake); }
  if (view === "network") refreshNetwork();
  if (view === "settings") refreshSettings();
  if (view === "watch") { refreshWatch(); refreshRss(); refreshTrash(); }
  if (view === "logs") refreshLogs();
  if (view === "doctor") refreshDoctor();
}

$$(".nav").forEach(button => button.addEventListener("click", () => openView(button.dataset.view, button)));
const themeToggle = $("#theme-toggle");
if (themeToggle) themeToggle.addEventListener("click", toggleTheme);

(async function init() {
  applyTheme(state.currentTheme, { persist: false });
  applySavedTorrentQueryView();
  await Promise.all([refreshTorrents(), refreshTelemetry(), refreshDoctorBadge()]);
  window.setInterval(() => { if (!document.hidden) { pollTorrents(); refreshTelemetry(); } }, 5000);
  window.setInterval(refreshDoctorBadge, 10000);
})();

$("#go-add").addEventListener("click", () => $('.nav[data-view="add"]').click());
document.addEventListener("keydown", e => {
  if (!(e.ctrlKey || e.metaKey) || e.key.toLowerCase() !== "k") return;
  const field = $(".view:not(.hidden) input[type=search]");
  if (field) { e.preventDefault(); field.focus(); }
});


$("#compact-controls").addEventListener("click",event=>{
  const button=event.target.closest("[data-reveal]");
  const view=$(".workspace:not(.hidden)");
  if (!view || !button) return;
  const name="show-"+button.dataset.reveal;
  const open=!view.classList.contains(name);
  view.classList.remove("show-sections","show-inspector");
  view.classList.toggle(name,open);
  for(const b of $$("[data-reveal]")) b.setAttribute("aria-expanded",String(b===button && open));
  if(open) view.querySelector(button.dataset.reveal==="sections"?".rail":".inspector").scrollIntoView({block:"nearest"});
});
document.addEventListener("keydown",event=>{
  if(event.key!=="Escape") return;
  const open=$("[data-reveal][aria-expanded=true]");
  if(open){open.click();open.focus();}
  for(const menu of $$("details.options[open]")){menu.open=false;menu.querySelector("summary").focus();}
});
