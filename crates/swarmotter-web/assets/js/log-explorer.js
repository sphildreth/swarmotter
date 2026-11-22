// SPDX-License-Identifier: Apache-2.0
import { $, $$, escapeHtml as esc, renderKv, showError } from "./ui.js";
import { MAX_LOG_LINES } from "./state.js";
import { glyph, downloadText, setText } from "./workspace.js";

let tail = [],
  live = [],
  frozen = null,
  serial = 0,
  stream = "Disconnected",
  filter = "all",
  selected = null;
let drawn = [],
  frame = false,
  loaded = false;
let revision = 0;
export const logTailRevision = () => revision;
export function hasLogSnapshot() {
  return loaded;
}
function parseLine(raw, origin) {
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    /* Plain tracing output is also supported. */
  }
  const clean = raw.replace(/\x1b\[[0-9;]*m/g, "");
  const match = clean.match(
    /^(\d{4}-\d\d-\d\dT\S+)\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+([^: ]+(?:::[^: ]+)*):\s*([\s\S]*)$/,
  );
  return {
    id: ++serial,
    raw,
    origin,
    time: data?.timestamp ?? match?.[1],
    level: String(data?.level ?? match?.[2] ?? "").toUpperCase(),
    component: String(data?.target ?? match?.[3] ?? origin),
    message: String(data?.fields?.message ?? data?.message ?? match?.[4] ?? clean),
    attributes: data?.fields || null,
  };
}
export function replaceLogTail(snapshot) {
  // Reuse identities across overlapping tail refreshes, including repeated identical lines.
  const old = new Map();
  for (const e of tail) {
    if (!old.has(e.raw)) old.set(e.raw, []);
    old.get(e.raw).push(e);
  }
  tail = (snapshot.lines || [])
    .slice(-MAX_LOG_LINES)
    .map((raw) => old.get(raw)?.shift() || parseLine(raw, "daemon"));
  loaded = true;
  setText(
    "#log-source",
    snapshot.enabled ? snapshot.path || "Daemon log" : "Daemon file logging unavailable",
  );
  render();
}
export function addLog(raw, event = null) {
  if (!$("#log-stream")) return;
  const row = parseLine(raw, event ? "event" : "browser");
  if (event) {
    row.component = event.kind;
    row.attributes = event;
    row.message = JSON.stringify(event.payload ?? {});
  }
  row.time ||= new Date().toISOString();
  live.push(row);
  if (live.length > MAX_LOG_LINES) live.shift();
  if (!frozen && !frame && !$("#view-logs")?.classList.contains("hidden")) {
    frame = true;
    requestAnimationFrame(() => {
      frame = false;
      render();
    });
  }
}
export function setConnection(value) {
  stream = value;
  updateConnection();
}
function updateConnection() {
  const label = frozen ? "Paused" : stream;
  setText("#log-connection", label);
  setText("#live-tag", label === "Connected" ? "● LIVE" : label);
  if ($("#live-tag")) $("#live-tag").className = "chip " + (label === "Connected" ? "ok" : "");
  setText("#log-state", frozen ? "View paused" : stream);
}
function entries() {
  if (frozen) return frozen;
  return [...tail, ...live]
    .sort((a, b) => {
      const delta = Date.parse(b.time) - Date.parse(a.time);
      return (Number.isFinite(delta) && delta) || b.id - a.id;
    })
    .slice(0, MAX_LOG_LINES);
}
function matchesGroup(e, key) {
  if (key === "all") return true;
  if (key === "daemon") return e.origin === "daemon";
  if (key === "automation") return /rss|watch|autopilot/i.test(e.component);
  if (key === "network") return /network|peer|tracker|port_mapping/i.test(e.component);
  if (key === "problems") return ["WARN", "ERROR"].includes(e.level);
  return e.level === key;
}
function levelPill(e) {
  return e.level ? `<b class="level ${esc(e.level)}">${esc(e.level)}</b>` : "<span>—</span>";
}
function selectEvent(row) {
  selected = row?.id ?? null;
  $("#copy-event").disabled = !row;
  setText("#event-level", row?.level);
  const inspector = $("#event-inspector");
  inspector.dataset.level = row?.level || "";
  if (!row) {
    setText("#event-subtitle", "Selected log entry");
    $("#event-level").className = "";
    inspector.innerHTML = '<p class="empty">Select a row to inspect the full event.</p>';
    return;
  }
  const fields = row.attributes || {};
  const { message, summary, feed, retry_attempt, retry_limit, request_duration_ms, ...context } = fields;
  const attrs = [
    ["Timestamp", row.time || "Unavailable"],
    ["Level", row.level || "Unspecified"],
    ["Component", row.component],
  ];
  if (feed) attrs.push(["Feed", feed]);
  if (retry_attempt != null)
    attrs.push(["Retry attempt", retry_limit == null ? retry_attempt : retry_attempt + " of " + retry_limit]);
  if (request_duration_ms != null)
    attrs.push(["Request duration", Number(request_duration_ms).toLocaleString() + " ms"]);
  setText(
    "#event-subtitle",
    [row.time?.split("T")[1]?.slice(0, 8), row.component].filter(Boolean).join(" · "),
  );
  $("#event-level").className =
    "chip " + (row.level === "WARN" ? "warn" : row.level === "ERROR" ? "error" : "");
  inspector.innerHTML = `<div class="event-summary">${glyph(["WARN", "ERROR"].includes(row.level) ? "warning" : "info")}<b>${esc(summary || row.message)}</b>${summary ? "<small>" + esc(row.message) + "</small>" : ""}</div>
    <div class="ins-section"><h3>Event attributes</h3>${renderKv(attrs)}</div>
    <div class="ins-section"><h3>Structured context</h3>${Object.keys(context).length ? `<pre>${esc(JSON.stringify(context, null, 2))}</pre>` : "<small>No additional structured attributes supplied.</small>"}</div>
    ${typeof fields.feed_id === "string" ? `<div class="ins-section"><h3>Related subscription</h3><button class="btn" data-open-feed="${esc(fields.feed_id)}">${glyph("rss")} View RSS subscription</button></div>` : ""}
    <details class="ins-section"><summary>Original entry</summary><pre>${esc(row.raw)}</pre></details>`;
}
function render() {
  if (!$("#log-search")) return;
  const records = entries();
  const component = $("#log-component");
  const chosen = component.value;
  const choices = [...new Set(records.map((e) => e.component))].sort();
  component.innerHTML =
    '<option value="">All components</option>' +
    choices.map((k) => `<option value="${esc(k)}">${esc(k)}</option>`).join("");
  component.value = choices.includes(chosen) ? chosen : "";
  const query = $("#log-search").value.toLowerCase();
  const level = $("#log-level").value;
  const cutoff = Number($("#log-time").value);
  drawn = records.filter(
    (e) =>
      matchesGroup(e, filter) &&
      (!level || e.level === level) &&
      (!component.value || e.component === component.value) &&
      (!query || (e.raw + " " + e.component).toLowerCase().includes(query)) &&
      (!cutoff || Date.parse(e.time) >= Date.now() - cutoff * 3600000),
  );
  const el = $("#log-stream");
  const top = el.scrollTop;
  const anchor = Array.from(el.children).find((n) => n.offsetTop + n.offsetHeight > el.offsetTop + top);
  const anchorOffset = anchor?.offsetTop;
  const focus = el.contains?.(document.activeElement) ? document.activeElement?.dataset?.eventId : null;
  el.innerHTML =
    drawn
      .map(
        (e) =>
          `<button type="button" class="log-grid log-entry" data-event-id="${e.id}" data-level="${esc(e.level || "")}" aria-pressed="${selected === e.id}"><span>${esc(e.time?.match(/T([\d:.]+)/)?.[1]?.slice(0, 12) || e.time || "—")}</span>${levelPill(e)}<span>${esc(e.component)}</span><span>${esc(e.message)}</span></button>`,
      )
      .join("") || '<p class="empty">No matching events.</p>';
  if (top > 0 && anchor?.dataset.eventId) {
    const next = el.querySelector(`[data-event-id="${anchor.dataset.eventId}"]`);
    el.scrollTop = next ? top + next.offsetTop - anchorOffset : top;
  }
  if (focus) el.querySelector(`[data-event-id="${focus}"]`)?.focus({ preventScroll: true });
  const selectedRow = drawn.find((e) => e.id === selected);
  if (selected && !selectedRow) selectEvent(null);
  for (const b of $$("[data-log-filter]")) {
    const key = b.dataset.logFilter;
    b.setAttribute("aria-pressed", String(filter === key));
    let count = b.querySelector("small");
    if (!count) {
      count = document.createElement("small");
      b.appendChild(count);
    }
    count.textContent = records.filter((e) => matchesGroup(e, key)).length;
  }
  setText("#log-showing", `Displaying ${drawn.length} of ${records.length} entries`);
  updateConnection();
}
export function showLogView() {
  render();
}
$("#view-logs")?.addEventListener("input", render);
$("#view-logs")?.addEventListener("click", async (e) => {
  const button = e.target.closest("button");
  if (!button) return;
  if (button.dataset.openFeed) {
    document.querySelector('.nav[data-view="watch"]').click();
    const { inspectSubscription } = await import("../rss.js");
    await inspectSubscription(button.dataset.openFeed);
    return;
  }
  if (button.dataset.eventId) {
    selectEvent(drawn.find((row) => row.id === Number(button.dataset.eventId)));
    for (const node of $$(".log-entry")) node.setAttribute("aria-pressed", String(node === button));
    return;
  }
  if (button.dataset.logFilter) {
    filter = button.dataset.logFilter;
    $("#log-level").value = "";
    render();
  }
  if (button.id === "pause-logs") {
    frozen = frozen ? null : entries();
    button.innerHTML = glyph(frozen ? "play" : "pause") + (frozen ? "Resume stream" : "Pause stream");
    render();
  }
  if (button.id === "clear-logs") {
    revision++;
    loaded = true;
    tail = [];
    live = [];
    if (frozen) frozen = [];
    selectEvent(null);
    render();
  }
  const text = drawn.map((row) => row.raw).join("\n");
  if (button.id === "export-logs") downloadText(text, "swarmotter-filtered.log");
  if (["copy-event", "copy-logs"].includes(button.id)) {
    try {
      await navigator.clipboard.writeText(
        button.id === "copy-event" ? drawn.find((row) => row.id === selected)?.raw || "" : text,
      );
    } catch (err) {
      showError("Copy failed", err);
    }
  }
});
