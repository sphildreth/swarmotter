// SPDX-License-Identifier: Apache-2.0
import { $, escapeHtml, fmtBytes, fmtUnixSeconds, renderKv, showError } from "./ui.js";
import { badge, glyph, selectPanel } from "./workspace.js";
import { api } from "./api.js";
import { openDetails } from "./details.js";

let mode = "magnet",
  tab = "Overview",
  inspected;
export function readMagnet(value) {
  const url = new URL(value.trim());
  const xt = url.searchParams
    .getAll("xt")
    .find((x) => /^urn:(?:btih:(?:[a-f0-9]{40}|[a-z2-7]{32})|btmh:1220[a-f0-9]{64})$/i.test(x));
  if (url.protocol !== "magnet:" || !xt) throw Error("Enter a magnet URI with a valid BitTorrent info hash.");
  return {
    hash: xt.split(":").pop(),
    name: url.searchParams.get("dn"),
    size: url.searchParams.get("xl"),
    trackers: url.searchParams.getAll("tr"),
  };
}
function render() {
  const labels = $("#magnet-labels")
    .value.split(",")
    .map((label) => label.trim())
    .filter(Boolean);
  $("#label-chips").innerHTML = labels
    .map((label) => `<span class="chip">${escapeHtml(label)}</span>`)
    .join("");
  const priority = $("#magnet-profile").selectedOptions?.[0]?.dataset?.priority;
  $("#import-priority").value = priority
    ? priority.charAt(0).toUpperCase() + priority.slice(1)
    : "Inherit queue policy";
  setIntakeStatus();

  const m = mode === "magnet" ? inspected : null;
  const file = $("#torrent-file").files?.[0];
  const fileMode = mode === "file";
  const info =
    mode === "magnet"
      ? m?.name || "Magnet link"
      : fileMode
        ? file?.name || "Torrent file"
        : "Local torrent creation";
  const config =
    mode === "create"
      ? [
          ["Format", $("#create-format").value],
          ["Source", $("#create-source").value || "—"],
        ]
      : [
          [
            "Save to",
            fileMode ? "Profile or global default" : $("#magnet-dir").value || "Profile or global default",
          ],
          ["Profile", $("#" + mode + "-profile").value || "Default"],
          [
            "Start mode",
            $("#" + mode + "-preview").checked
              ? "Metadata only"
              : $("#" + mode + "-paused").checked
                ? "Paused"
                : !fileMode && $("#magnet-start").checked
                  ? "Immediate"
                  : "Automatic",
          ],
          ["Labels", $("#" + mode + "-labels").value || "—"],
        ];
  const metadata = [
    ["Declared size", m?.size ? fmtBytes(m.size) : "—"],
    ["Piece length", "Unavailable"],
    ["File list", "Unavailable before import"],
    ["Trackers", m?.trackers.length ?? "—"],
    ["Info hash", m?.hash ? m.hash.slice(0, 6) + "…" + m.hash.slice(-6) : "—"],
  ];
  let content;
  if (tab === "Files") {
    content = "<p>File metadata is available after queued intake.</p>";
  } else if (tab === "Trackers") {
    content = m?.trackers.length
      ? renderKv(m.trackers.map((url, i) => ["Tracker " + (i + 1), url]))
      : "<p>No tracker URLs available.</p>";
  } else if (m) {
    content = `<div class="declared-item">${glyph("file")}<span>${escapeHtml(m.name || "Name unspecified")}</span><small>${escapeHtml(fmtBytes(m.size))}</small></div>
      <div class="declared-item">${glyph("link")}<span>Exact topic</span><small title="${escapeHtml(m.hash)}">${escapeHtml(metadata.at(-1)[1])}</small></div>`;
  } else {
    content = `<p>${glyph("file")} ${escapeHtml((fileMode && file?.name) || "No metadata inspected.")}</p>`;
  }
  $("#import-preview").innerHTML =
    `<div class="import-identity">${glyph(fileMode ? "file" : "drive")}<div><b>${escapeHtml(info)}</b><small>${m ? "Magnet URI · declared metadata" : "Import details"}</small></div></div><div class="detail-tabs">${["Overview", "Files", "Trackers"].map((t) => `<button data-preview-tab="${t}" aria-pressed="${t === tab}">${t}</button>`).join("")}</div>
  <div class="ins-section content-inspect"><div class="block-head"><h3>${tab === "Overview" ? "Content inspection" : tab}</h3>${tab === "Overview" && m ? badge("URI parsed", "ok") : ""}</div>${content}</div>
  <div class="ins-section"><h3>Import settings</h3>${renderKv(config)}</div><div class="ins-section"><h3>Torrent metadata</h3>${renderKv(metadata)}</div>`;
}
function setIntakeStatus() {
  $("#import-status").textContent = inspected ? "Link inspected" : "Magnet intake";
  $("#copy-input").disabled = !$("#magnet-input").value;
}
function switchMethod(next) {
  mode = next;
  selectPanel("method", mode, $("#view-add"));
  $("#commit-import").innerHTML = glyph("plus") + (mode === "create" ? "Create torrent" : "Add to queue");
  render();
}
export async function refreshIntake() {
  render();
  try {
    const result = await api("/torrents/query?sort=added&dir=desc&per_page=3");
    $("#recent-imports").innerHTML =
      result.rows
        .map(
          (t) =>
            `<button class="rail-link recent-link" data-recent="${escapeHtml(t.info_hash)}">${glyph("file")}<span>${escapeHtml(t.name)}<small>${escapeHtml(fmtUnixSeconds(t.date_added))}</small></span></button>`,
        )
        .join("") || "<small>No imports yet.</small>";
  } catch (e) {
    $("#recent-imports").textContent = e.message;
  }
}
$("#view-add").addEventListener("click", async (event) => {
  const control = event.target.closest("button");
  if (!control) return;
  if (control.id === "copy-input") {
    try {
      await navigator.clipboard.writeText($("#magnet-input").value);
    } catch (e) {
      showError("Copy magnet failed", e);
    }
  }
  const next = control.dataset.method || control.dataset.switchMethod;
  if (next) switchMethod(next);
  if (control.dataset.recent) openDetails(control.dataset.recent);
  if (control.dataset.previewTab) {
    tab = control.dataset.previewTab;
    render();
  }
  if (control.id === "commit-import")
    $("#" + { magnet: "add-magnet-btn", file: "add-file-btn", create: "create-btn" }[mode]).click();
  if (control.id === "inspect-magnet") {
    try {
      inspected = readMagnet($("#magnet-input").value);
      render();
    } catch (e) {
      inspected = null;
      render();
      showError("Inspect magnet link", e);
    }
  }
});
$("#view-add").addEventListener("input", (e) => {
  if (e.target.id === "magnet-input") inspected = null;
  if (e.target.id === "magnet-preview" && e.target.checked && $("#magnet-start").checked)
    $("#magnet-inherit").checked = true;
  if (e.target.id === "magnet-start" && e.target.checked) $("#magnet-preview").checked = false;
  render();
});
