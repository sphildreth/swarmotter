// SPDX-License-Identifier: Apache-2.0

import { api, apiFetch, responseErrorMessage } from "./api.js";
import { showError, showToast } from "./ui.js";

async function copyText(value, title) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
    } else {
      window.prompt(title, value);
    }
    showToast(title, "", "success");
  } catch {
    window.prompt(title, value);
  }
}

export async function copyTorrentMagnet(hash, name = "", trackerPolicy = "all") {
  try {
    let result;
    try {
      result = await api(`/torrents/${encodeURIComponent(hash)}/magnet?tracker_policy=${encodeURIComponent(trackerPolicy)}`);
    } catch (error) {
      if (String(error.message || "").includes("explicit consent")) {
        if (!window.confirm("This is a private torrent. Include its private tracker URLs in the magnet?")) return;
        result = await api(`/torrents/${encodeURIComponent(hash)}/magnet?tracker_policy=${encodeURIComponent(trackerPolicy)}&include_private_trackers=true`);
      } else {
        throw error;
      }
    }
    await copyText(result.magnet, `Magnet for ${name || hash}`);
  } catch (error) {
    showError("Copy magnet failed", error);
  }
}

function downloadBlob(bytes, type, filename) {
  const url = URL.createObjectURL(new Blob([bytes], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export async function downloadTorrentMetainfo(hash, name = "") {
  try {
    const response = await apiFetch(`/torrents/${encodeURIComponent(hash)}/metainfo`);
    if (!response.ok) {
      throw new Error(await responseErrorMessage(response));
    }
    const bytes = await response.arrayBuffer();
    downloadBlob(bytes, "application/x-bittorrent", `${name || hash}.torrent`);
    showToast("Metainfo exported", name || hash, "success");
  } catch (error) {
    showError("Metainfo export failed", error);
  }
}

export async function downloadMetainfoArchive(hashes) {
  if (!hashes.length) {
    showError("Metainfo export failed", new Error("Select at least one torrent"));
    return;
  }
  try {
    const response = await apiFetch("/torrents/export", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ hashes }),
    });
    if (!response.ok) {
      throw new Error(await responseErrorMessage(response));
    }
    const bytes = await response.arrayBuffer();
    downloadBlob(bytes, "application/x-tar", "swarmotter-metainfo-export.tar");
    showToast("Metainfo archive exported", `${hashes.length} requested`, "success");
  } catch (error) {
    showError("Metainfo archive export failed", error);
  }
}
