// SPDX-License-Identifier: Apache-2.0
const mib = 2 ** 20;
const rows = [
  ["The.Matrix.1999.4K.Remux.mkv", "movies", 82.4, 24.3, 3.1, 0.28, 86, 342],
  ["Dune.Part.Two.2024.4K.mkv", "movies", 100, 0, 1.8, 3.92, 12, 98],
  ["Silo.S02E01.2160p.WEB-DL.mkv", "tv", 56.7, 8.9, 1.2, 0.14, 64, 210],
  ["Daft.Punk…Lossless.FLAC", "music", 100, 0, 0.422, 4.87, 8, 32],
  ["Ubuntu.24.04.1.Desktop.iso", "linux", 100, 0, 0, 6.21, 0, 14],
  ["Foundation.S03E05.2160p.mkv", "tv", 37.1, 5.7, 0.5, 0.07, 52, 188],
  ["Creative_Suite_2024_Archive.zip", "archive", 100, 0, 0.25, 2.1, 4, 17],
  ["Interstellar.2014.4K.Remux.mkv", "movies", 100, 0, 1.4, 3.56, 16, 67],
  ["The.Expanse.S06.Complete", "tv", 12.8, 3.1, 0.125, 0.03, 24, 76],
  ["Radiohead.Discography.FLAC", "music", 100, 0, 2.6, 8.11, 28, 91],
  ["Blade.Runner.2049.4K.mkv", "movies", 100, 0, 0.599, 2.94, 9, 36],
  ["Fedora-Workstation-41.iso", "linux", 100, 0, 0.125, 3.01, 2, 11],
];
export const library = rows.map(([name, category, progress, down, up, ratio, peers, known], i) => {
  const total = Math.round((i ? 1.5 + i / 2 : 4.7) * 2 ** 30);
  const pieces = Math.ceil(total / (4 * mib));
  return {
    name,
    info_hash: (i + 1).toString(16).padStart(40, "0"),
    labels: [category],
    total_length: total,
    state: progress === 100 ? "seeding" : "downloading",
    bytes_completed: Math.round((total * progress) / 100),
    rate_down: Math.round(down * mib),
    rate_up: Math.round(up * mib),
    ratio,
    active_peer_workers: peers,
    known_peers: known,
    health: { label: "good", score: 84, bars: 4, reasons: [] },
    download_dir: "/downloads/" + category,
    piece_count: pieces,
    pieces_have: Math.floor((pieces * progress) / 100),
    piece_length: 4 * mib,
    creation_date: 947735040,
    date_added: 1795297320 - i,
    downloaded: Math.round((total * progress) / 100),
    uploaded: Math.round(((total * progress) / 100) * ratio),
    queue_position: i + 1,
  };
});
// The published image shows one page of a larger library. The remaining entries
// have distinct identities and contribute to the same counts, sizes and rates.
const remainder = 128 - library.length;
const remainingBytes = Math.round(18.4 * 2 ** 40) - library.reduce((n, t) => n + t.total_length, 0);
const categories = ["movies", "tv", "music", "linux", "archive"];
for (let i = 0; i < remainder; i++) {
  const index = library.length;
  const total = Math.floor(remainingBytes / remainder) + (i < remainingBytes % remainder ? 1 : 0);
  const state = i < 8 ? "paused" : i < 16 ? "downloading" : "seeding";
  const category = categories[i % categories.length];
  library.push({
    ...library[0],
    info_hash: (index + 1).toString(16).padStart(40, "0"),
    name: ["ubuntu-24.04.1.iso", "fedora-41.iso", "archlinux-2026.iso"][i] || `Local collection ${i + 1}`,
    total_length: total,
    state,
    rate_down: state === "downloading" ? Math.round(0.075 * 2 ** 20) : 0,
    rate_up: 0,
    bytes_completed: state === "seeding" ? total : Math.floor(total / 2),
    downloaded: total,
    uploaded: Math.round(total * 2.87),
    piece_count: Math.ceil(total / (4 * mib)),
    pieces_have: Math.floor((total / (4 * mib)) * (state === "seeding" ? 1 : 0.5)),
    labels: [category],
    queue_position: index + 1,
    date_added: i < 3 ? 1795298000 - i * 60 : 1795200000 - i,
  });
}
export function query(q) {
  const rows = library.filter(
    (t) =>
      (!q.get("q") || t.name.toLowerCase().includes(q.get("q").toLowerCase())) &&
      (!q.get("state") || t.state === q.get("state")) &&
      (!q.get("label") || t.labels.includes(q.get("label"))) &&
      (q.get("performance") !== "transferring" || t.rate_up > 0 || t.rate_down > 0),
  );
  const counts = { states: {}, labels: {} };
  for (const r of rows)
    for (const [key, name] of [
      ["states", r.state],
      ["labels", r.labels[0]],
    ])
      counts[key][name] = (counts[key][name] || 0) + 1;
  const field =
    {
      queue: "queue_position",
      added: "date_added",
      size: "total_length",
      down_rate: "rate_down",
      up_rate: "rate_up",
    }[q.get("sort")] ||
    q.get("sort") ||
    "name";
  const dir = q.get("dir") === "desc" ? -1 : 1;
  rows.sort(
    (a, b) =>
      dir *
      (typeof a[field] === "number" ? a[field] - b[field] : String(a[field]).localeCompare(String(b[field]))),
  );
  const page = Number(q.get("page") || 1),
    perPage = Number(q.get("per_page") || 200);
  return {
    rows: rows.slice((page - 1) * perPage, page * perPage),
    total: library.length,
    filtered: rows.length,
    page,
    page_count: perPage ? Math.ceil(rows.length / perPage) : 0,
    per_page: perPage,
    sort: q.get("sort") || "name",
    dir: q.get("dir") || "asc",
    counts,
  };
}
export const health = {
  mode: "disabled",
  status: "disabled",
  traffic_allowed: true,
  fail_closed: true,
  allow_ipv6: true,
  detail: "Selected interface and source-IP constraints are not configured. Traffic is permitted.",
};
export function stats(index) {
  const variation = [
    0.22, 0.32, 0.3, 0.47, 0.36, 0.44, 0.56, 0.46, 0.45, 0.6, 0.64, 0.57, 0.74, 0.72, 0.8, 0.67, 0.83, 0.76,
    0.92, 0.85, 1,
  ];
  const factor = variation[Math.min(Math.round((index * (variation.length - 1)) / 17), variation.length - 1)];
  const sum = (field) => library.reduce((n, t) => n + t[field], 0);
  return {
    torrent_count: library.length,
    active_downloads: library.filter((t) => t.state === "downloading").length,
    active_seeds: library.filter((t) => t.state === "seeding").length,
    paused: library.filter((t) => t.state === "paused").length,
    download_rate: Math.round(sum("rate_down") * factor),
    upload_rate: Math.round(sum("rate_up") * factor),
    total_downloaded: sum("downloaded"),
    total_uploaded: sum("uploaded"),
  };
}
