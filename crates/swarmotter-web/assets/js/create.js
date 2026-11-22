// SPDX-License-Identifier: Apache-2.0

import { api } from "./api.js";
import { $, escapeHtml, fmtBytes, fmtCount, renderKv, showError, showToast } from "./ui.js";
import { refreshTorrents } from "./torrents.js";

let lastMetainfo = null;

function splitLines(value) {
  return String(value || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean);
}

function trackerTiers() {
  return splitLines($("#create-trackers").value).map(line => line.split("|").map(url => url.trim()).filter(Boolean)).filter(tier => tier.length > 0);
}

function hexToBytes(hex) {
  if (!/^[0-9a-fA-F]*$/.test(hex) || hex.length % 2 !== 0) throw new Error("Metainfo export is not valid hex");
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index++) bytes[index] = parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  return bytes;
}

export function resetCreateForm() {
  const form = $("#create-form");
  if (form) form.reset();
  $("#create-format").value = "v1";
  lastMetainfo = null;
  const result = $("#create-result");
  if (result) result.innerHTML = `<p class="muted">Generate metainfo from a file or directory inside a configured storage root.</p>`;
}

async function refreshCreateRoots() {
  const target = $("#create-roots");
  if (!target) return;
  try {
    const roots = await api("/torrents/create/roots");
    target.textContent = roots.length
      ? `Allowed source roots: ${roots.join(", ")}`
      : "No configured storage roots currently permit local creation.";
  } catch (error) {
    target.textContent = "Allowed source roots could not be loaded.";
  }
}

function renderResult(result) {
  lastMetainfo = result.metainfo_hex || null;
  const seeded = result.seeded
    ? `yes (${escapeHtml(result.seed_state || "registered")})`
    : result.seed_error
      ? `no — ${escapeHtml(result.seed_error)}`
      : "no";
  $("#create-result").innerHTML = `
    <h3>${escapeHtml(result.name)}</h3>
    ${renderKv([
      ["Format", result.format],
      ["Total size", fmtBytes(result.total_length)],
      ["Piece length", fmtBytes(result.piece_length)],
      ["Pieces", fmtCount(result.piece_count)],
      ["Files", fmtCount(result.file_count)],
      ["Private", result.private ? "yes" : "no"],
      ["Torrent identities", (result.torrent_keys || []).join(", ")],
      ["Trackers", (result.tracker_tiers || []).map(tier => tier.join(" | ")).join(" / ") || "none"],
      ["Webseeds", (result.webseeds || []).join(", ") || "none"],
      ["Immediate seed", seeded],
      ["Metainfo size", fmtBytes(result.metainfo_bytes)],
    ])}
    <button id="create-download-btn" type="button" ${lastMetainfo ? "" : "disabled"}>Download .torrent</button>`;
  const download = $("#create-download-btn");
  if (download && lastMetainfo) {
    download.addEventListener("click", () => {
      const bytes = hexToBytes(lastMetainfo);
      const url = URL.createObjectURL(new Blob([bytes], { type: "application/x-bittorrent" }));
      const link = document.createElement("a");
      link.href = url;
      link.download = `${result.name || "created"}.torrent`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
  }
}

export async function createTorrent() {
  const button = $("#create-btn");
  if (button?.disabled) return;
  if (button) button.disabled = true;
  try {
    const payload = {
      source: $("#create-source").value.trim(),
      name: $("#create-name").value.trim() || null,
      format: $("#create-format").value,
      piece_length: $("#create-piece-length").value ? Number($("#create-piece-length").value) : null,
      tracker_tiers: trackerTiers(),
      webseeds: splitLines($("#create-webseeds").value),
      private: $("#create-private").checked,
      seed: $("#create-seed").checked,
    };
    if (!payload.source) throw new Error("Source path is required");
    const result = await api("/torrents/create", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    renderResult(result);
    showToast(result.seeded ? "Torrent created and seeding" : "Torrent created", result.name, "success");
    if (result.seeded) await refreshTorrents();
  } catch (error) {
    showError("Torrent creation failed", error);
  } finally {
    if (button?.isConnected) button.disabled = false;
  }
}

$("#create-form")?.addEventListener("submit", event => {
  event.preventDefault();
  createTorrent();
});
$("#create-reset-btn")?.addEventListener("click", resetCreateForm);
$("#create-source")?.addEventListener("focus", refreshCreateRoots, { once: true });
