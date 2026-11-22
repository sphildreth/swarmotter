// SPDX-License-Identifier: Apache-2.0
import { readFileSync } from "node:fs";
import { health } from "./library.mjs";
export const settings = JSON.parse(readFileSync(new URL("./settings.json", import.meta.url)));
export const feeds = [
  ["ubuntu", "Ubuntu releases", "releases.ubuntu.com", 15, true, "iso"],
  ["fedora", "Fedora Project", "fedoraproject.org", 30, true, "fedora"],
  ["debian", "Debian images", "cdimage.debian.org", 60, false, "iso"],
].map(([id, name, host, minutes, enabled, label], i) => ({
  id,
  name,
  url: "https://" + host + "/rss.xml",
  enabled,
  poll_interval_seconds: minutes * 60,
  profile: "Linux ISOs",
  labels: ["linux", label],
  start_behavior: "start",
  import_mode: "new_only",
  rules: {
    include: [{ title: "*linux*" }, { title: "*iso*" }],
    exclude: [{ title: "*daily*" }, { title: "*alpha*" }],
  },
  last_poll_at: 1795298000 - i * 60,
  last_success_at: 1795298000 - i * 60,
  baseline_established: true,
  counts: { listings: 18, imported: 2, filtered: 1 },
}));
export const preview = (id) => ({
  subscription_id: id,
  feed_title: feeds.find((s) => s.id === id)?.name,
  fetched_at: 1795298000,
  decisions: [
    ["Ubuntu 24.04.1 LTS · Desktop ISO", "import", "Release matches +linux and +iso"],
    ["Ubuntu 24.04.1 LTS · Server ISO", "import", "Release matches +linux and +iso"],
    ["Ubuntu daily build · 2026-11-21", "filtered", "Excluded by −daily rule"],
  ].map(([title, action, reason], i) => ({
    title,
    action,
    reason,
    entry_key: id + "-" + i,
    torrent_keys: [],
  })),
});
export const network = {
  health,
  listen_port: 51413,
  dht_port: 51413,
  torrent_allow_ipv6: true,
  utp_enabled: true,
  utp_prefer_tcp: true,
  interfaces: [
    ["br0", "192.0.2.36"],
    ["docker0", "172.17.0.1"],
    ["br-88e", "172.18.0.1"],
    ["lo", "127.0.0.1"],
  ].map(([name, ip]) => ({ name, status: "up", addresses: [ip], has_ipv4: true })),
  containment_matrix: [
    ["peer_tcp", "Peer TCP", "Outbound TCP uses the configured network binder"],
    ["peer_utp", "Peer µTP", "UDP sockets follow peer transport policy"],
    ["dht_udp", "DHT & UDP trackers", "Discovery uses bound UDP sockets"],
    ["dns", "Web seeds & DNS", "DNS containment validation is disabled"],
  ].map(([id, label, detail]) => ({ id, label, detail, level: "warning" })),
  checks: [{ id: "containment_status", label: "Containment state", level: "warning", detail: health.detail }],
};
export const watch = {
  enabled: true,
  folders: ["/incoming/linux", "/incoming/media"].map((path) => ({
    exists: true,
    pending_torrent_files: 0,
    config: { path },
  })),
  recent_imports: [],
};
const logData = [
  ["21:55:12.117", "INFO", "swarmotterd", "Event stream connected to the browser UI"],
  ["21:55:08.931", "INFO", "persistence", "Restored daemon state"],
  ["21:55:08.923", "INFO", "swarmotterd", "Starting API + web UI on 127.0.0.1:9091"],
  ["21:55:08.922", "INFO", "network", "Containment disabled · traffic allowed=true"],
  ["21:55:08.921", "INFO", "swarmotterd", "Configuration loaded from config.toml"],
  ["21:54:58.218", "WARN", "rss.poller", "Feed request exceeded configured timeout (15s)"],
  ["21:54:42.112", "INFO", "rss.import", "Accepted ubuntu-24.04.1-desktop-amd64.iso"],
  ["21:54:33.607", "INFO", "watch.scanner", "Scan completed: 2 folders · 0 pending files"],
  ["21:54:28.334", "DEBUG", "peer.transport", "µTP peer socket recycled after idle timeout"],
  ["21:54:19.051", "INFO", "torrent.session", "Rechecked 256 pieces · all verified"],
  ["21:53:41.048", "INFO", "tracker", "Announce succeeded: 142 peers returned"],
  ["21:53:05.274", "INFO", "network", "Listening on TCP/UDP port 51413"],
  ["21:52:36.260", "INFO", "swarmotterd", "Daemon file logging enabled"],
];
export const logs = {
  enabled: true,
  path: "/var/log/swarmotter/swarmotterd.log",
  truncated: false,
  lines: logData
    .reverse()
    .map(([time, level, target, message]) =>
      JSON.stringify({
        timestamp: "2026-11-21T" + time + "Z",
        level,
        target,
        fields:
          level === "WARN"
            ? {
                message,
                summary: "RSS feed polling timed out",
                feed: "Fedora Project",
                feed_id: "fedora",
                retry_attempt: 2,
                retry_limit: 3,
                request_duration_ms: 15018,
                event: "feed.timeout",
                retryable: true,
                backoff_seconds: 30,
              }
            : { message },
      }),
    ),
};

settings.profiles = { profiles: { "Linux ISOs": { queue: { priority: "normal" } } }, labels: {} };
settings.rss.subscriptions = feeds.map(
  ({
    id,
    name,
    url,
    enabled,
    poll_interval_seconds,
    profile,
    labels,
    start_behavior,
    import_mode,
    rules,
  }) => ({
    id,
    name,
    url,
    enabled,
    poll_interval_seconds,
    profile,
    labels,
    start_behavior,
    import_mode,
    rules,
  }),
);
settings.watch = watch.folders.map((f) => ({
  ...f.config,
  recursive: false,
  start_behavior: "start",
  delete_after_import: true,
}));
