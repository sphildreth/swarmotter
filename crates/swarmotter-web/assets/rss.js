// SPDX-License-Identifier: Apache-2.0
// RSS subscription controls for the Watch view.

import { api } from "./js/api.js";
import { uniqueRssId } from "./js/rss-id.js";
import { $, escapeHtml, fmtCount, fmtUnixSeconds, renderKv, renderStatus, showError, showToast, log } from "./js/ui.js";

import { badge, glyph, selectPanel, titleCase, setText, metric } from "./js/workspace.js";

let subscriptions = [], feedId = null, watchStatus = null, refreshGeneration = 0;
let feedsLoaded = false;
const runningActions = new Set();
let editingId = null;
let originalDraft = null;
let selectedId = null;
let decisionsBefore = null;
let editingLabels = [];

function ruleLines(rules) {
  return [...(rules?.include || []).map(rule => ({ ...rule, include: true })),
          ...(rules?.exclude || []).map(rule => ({ ...rule, include: false }))]
    .map(rule => `${rule.include ? "+" : "-"}${rule.title || ""}|${rule.category || ""}`)
    .join("\n");
}

function parseRules(text) {
  const include = [];
  const exclude = [];
  for (const raw of String(text || "").split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    const sign = line.startsWith("-") ? "exclude" : "include";
    const body = line.startsWith("+") || line.startsWith("-") ? line.slice(1) : line;
    const [titleRaw, categoryRaw] = body.split("|", 2);
    const title = (titleRaw || "").trim();
    const category = (categoryRaw || "").trim();
    const rule = {};
    if (title) rule.title = title;
    if (category) rule.category = category;
    if (rule.title || rule.category) (sign === "include" ? include : exclude).push(rule);
  }
  return { include, exclude };
}

function actionStatus(action) {
  if (["import", "already_present", "previously_completed"].includes(action)) return "ok";
  if (["filtered", "dismissed", "baseline"].includes(action)) return "warning";
  if (action === "failed") return "error";
  return "warning";
}

const feedActions = [["poll","Poll now"],["import-current","Import current"],["decisions","Decision history"],["reconsider","Reconsider older"],["retry","Retry dismissed"],["delete","Delete subscription"]];
function renderSubscriptionRow(sub) {
  const id = escapeHtml(sub.id);
  return `<article class="feed-grid feed-row" data-active="${feedId===sub.id}">
  <button class="feed-name" data-feed="${id}">${glyph("rss")}<span><b>${escapeHtml(sub.name)}</b><small>${escapeHtml(sub.url)}</small></span></button>
  <small>${escapeHtml((sub.labels || []).join(", "))}</small><small>${sub.poll_interval_seconds/60} min</small>
  <span>${badge(sub.enabled?"Active":"Paused",sub.enabled?"ok":"")}</span>
  <details class="options"><summary aria-label="Actions for ${escapeHtml(sub.name)}">${glyph("more")}</summary><div class="options-panel">${feedActions.map(([action,name])=>`<button type="button" data-rss-action="${action}" data-rss-id="${id}">${name}</button>`).join("")}</div></details></article>`;
}
function renderFeedList() {
  const search = ($("#feed-search")?.value || "").trim().toLowerCase();
  const visible = subscriptions.filter(sub => (sub.name+" "+sub.url).toLowerCase().includes(search));
  $("#rss-subscriptions").innerHTML = visible.map(renderSubscriptionRow).join("") || `<p class="empty">${subscriptions.length ? "No matching subscriptions." : "No subscriptions yet."}</p>`;
  setText("#feed-count",`${visible.length} shown · ${subscriptions.filter(sub=>sub.enabled).length} active across all subscriptions`);
}
function renderFeedInspector() {
  const sub = subscriptions.find(row=>row.id===feedId);
  for(const id of ["edit-subscription","feed-preview-action","preview-subscription"]) $("#"+id).disabled=!sub;
  if(!sub) { $("#feed-inspector").innerHTML='<p class="empty">Select a subscription to inspect its rules and activity.</p>';return; }
  const ruleChips = ["include","exclude"].flatMap(key=>(sub.rules?.[key]||[]).map(rule=>badge((key==="include"?"+":"−")+[rule.title,rule.category].filter(Boolean).join(" | "),key==="include"?"ok":"warn")));
  $("#feed-inspector").innerHTML = `<div class="inspector-head">${glyph("rss")}<div><strong>${escapeHtml(sub.name)}</strong><small>RSS subscription inspector</small></div>${badge(sub.enabled?"Active":"Paused",sub.enabled?"ok":"")}</div>
  <div class="ins-section"><h3>Subscription</h3>${[["Feed URL",sub.url],["Poll interval",sub.poll_interval_seconds+" seconds"],["First poll behavior",sub.import_mode==="new_only"?"New entries only":"Import current matches"],["Intake profile",sub.profile || "Default"]].map(([k,v])=>`<label class="stack-field">${k}<input readonly value="${escapeHtml(v)}"></label>`).join("")}</div>
  <div class="ins-section"><h3>Matching rules</h3>${ruleChips.join(" ") || "No matching rules."}</div>
  <div class="ins-section"><h3>Activity</h3>${renderKv([["Last polled",fmtUnixSeconds(sub.last_poll_at)||"Never"],["Last success",fmtUnixSeconds(sub.last_success_at)||"Never"],["Baseline",sub.baseline_established?"Established":"Pending"],["Items reviewed",sub.counts?.listings],["Imported",sub.counts?.imported],["Filtered",sub.counts?.filtered],["Latest error",sub.latest_error || "—"]])}</div>
  <details class="ins-section"><summary>Subscription details</summary>${renderKv([["Identifier",sub.id],...Object.entries(sub.counts || {}).map(([key,value])=>[titleCase(key),value])])}</details>`;
}
function selectFeed(id) {
  if (feedId !== id) {
    feedId=id;
    setText("#rss-preview",id ? "Choose Preview feed to inspect intake decisions." : "Add a subscription to preview feed decisions.");
    setText("#feed-preview-note","Proposed actions for the selected subscription");
    setText("#rss-decisions","Open Decision history for this subscription.");
    decisionsBefore=null;
    selectedId=null;
  }
  renderFeedList();
  renderFeedInspector();
}
export function updateWatchMetrics(status) {
  if (status !== undefined) watchStatus=status;
  const active=subscriptions.filter(row=>row.enabled).length;
  const folders=watchStatus?.folders;
  const polled=subscriptions.reduce((n,s)=>Math.max(n,s.last_poll_at || 0),0);
  const imported=subscriptions.reduce((n,s)=>n+(s.counts?.imported || 0),0);
  $("#watch-metrics").innerHTML=metric("RSS subscriptions",feedsLoaded ? subscriptions.length : null, feedsLoaded ? `${active} active feeds` : "Awaiting feed status") +
    metric("Watch folders",folders?.length,folders ? folders.reduce((n,f)=>n+(f.pending_torrent_files||0),0)+" pending .torrent files" : "Status unavailable") +
    metric("Last feed poll",polled ? new Date(polled*1000).toLocaleTimeString() : "—","Across subscriptions") +
    metric("Feed imports",feedsLoaded ? imported : null,"Recorded by subscriptions");
  setText("#watch-note",subscriptions.some(s=>s.latest_error) ? "A feed reported a polling error." : `${active} subscriptions enabled for automatic polling.`);
}
function renderRss(statuses) {
  subscriptions=statuses;
  feedsLoaded = true;
  setText("#rail-feeds",statuses.length);
  selectFeed(statuses.some(s=>s.id===feedId) ? feedId : statuses[0]?.id || null);
  updateWatchMetrics();
}
function populateEditor(statuses) {
  const template = statuses.find(sub => sub.id === editingId);
  if (template && $("#rss-form")) {
    $("#rss-id").value = template.id;
    $("#rss-name").value = template.name;
    $("#rss-url").value = "";
    $("#rss-url").placeholder = "Leave blank to keep the configured feed URL";
    $("#rss-interval").value = String(template.poll_interval_seconds);
    $("#rss-enabled").checked = !!template.enabled;
    $("#rss-profile").value = template.profile || "";
    $("#rss-start").value = template.start_behavior || "start";
    $("#rss-import-mode").value = template.import_mode || "new_only";
    $("#rss-rules").value = ruleLines(template.rules);
    editingLabels = Array.isArray(template.labels) ? template.labels : [];
    $("#rss-save-btn").textContent = "Save changes";
    originalDraft = JSON.stringify(subscriptionPayload());
  }
}

function renderPreview(preview, draft = false) {
  const target = $(draft ? "#rss-draft-preview" : "#rss-preview");
  if (!target) return;
  if (!draft && preview.subscription_id !== feedId) return;
  target.hidden = false;
  const note = preview.conditional_not_modified ? "Feed reported not modified." : "Preview performed no library changes and did not mark entries seen.";
  if (!draft) setText("#feed-preview-note",note);
  target.innerHTML = (draft ? `<small>${escapeHtml(note)}</small>` : "") +
    (preview.decisions || []).map(row=>`<div class="preview-row">${glyph("file")}<div><strong>${escapeHtml(row.title)}</strong><small>${escapeHtml(row.reason)}</small><details><summary>Entry details</summary>${escapeHtml(row.entry_key)} · ${escapeHtml((row.torrent_keys || []).join(", "))}</details></div>${badge(titleCase(row.action),row.action==="import"?"ok":"")}</div>`).join("");
  if (draft) target.scrollIntoView({block:"nearest"});
}

function renderDecisions(page, append = false) {
  const target = $("#rss-decisions");
  if (!target) return;
  const rows = page.decisions || [];
  const body = rows.map(row => `<tr>
    <td>${escapeHtml(fmtUnixSeconds(row.decided_at))}</td>
    <td>${escapeHtml(row.title)}<div class="muted">${escapeHtml(row.entry_key)}</div></td>
    <td>${renderStatus(actionStatus(row.action))} ${escapeHtml(row.action)}</td>
    <td>${escapeHtml(row.reason)}</td>
    <td>${escapeHtml((row.torrent_keys || []).join(", "))}</td>
  </tr>`).join("");
  if (append) {
    const tbody = target.querySelector("tbody");
    if (tbody) tbody.insertAdjacentHTML("beforeend", body);
  } else {
    target.innerHTML = `<h3>Decision history</h3>
      <p class="muted">Bounded per-feed history. Verified completion identities are retained separately and are not pruned.</p>
      <table><thead><tr><th>When</th><th>Listing</th><th>Action</th><th>Reason</th><th>Torrent identity</th></tr></thead><tbody>${body}</tbody></table>`;
  }
  decisionsBefore = page.next_before || null;
  const more = target.querySelector("#rss-more-decisions");
  if (more) more.remove();
  if (decisionsBefore) {
    target.insertAdjacentHTML("beforeend", `<button id="rss-more-decisions" type="button">Load older decisions</button>`);
  }
}

export async function refreshRss() {
  const generation=++refreshGeneration;
  try {
    const statuses = await api("/rss");
    if(generation===refreshGeneration) renderRss(statuses);
    return statuses;
  } catch (error) {
    if (generation!==refreshGeneration) return [];
    feedsLoaded=false;updateWatchMetrics();
    setText("#rail-feeds","—");
    setText("#rss-subscriptions","Subscriptions unavailable · "+error.message);
    setText("#watch-note","Feed status unavailable");
    log("rss error: " + error.message);
    return [];
  }
}

function subscriptionPayload() {
  const id = $("#rss-id").value.trim();
  const url = $("#rss-url").value.trim();
  return {
    id,
    name: $("#rss-name").value.trim(),
    url,
    enabled: $("#rss-enabled").checked,
    poll_interval_seconds: Number($("#rss-interval").value || 900),
    profile: $("#rss-profile").value.trim() || null,
    start_behavior: $("#rss-start").value,
    import_mode: $("#rss-import-mode").value,
    rules: parseRules($("#rss-rules").value),
    labels: editingId ? editingLabels : [],
  };
}

async function previewCurrentSubscription() {
  const payload = subscriptionPayload();
  if (!payload.name) throw new Error("Subscription name is required");
  if (editingId) {
    payload.id = editingId;
    if (!payload.url) {
      if (JSON.stringify(payload) !== originalDraft) throw Error("Enter the feed URL to preview unsaved changes. The stored address may contain redacted credentials.");
      renderPreview(await api(`/rss/${encodeURIComponent(editingId)}/preview`), true);
      return;
    }
  } else {
    if (!payload.url) throw new Error("Feed URL is required to preview");
    const statuses = await api("/rss");
    payload.id = uniqueRssId(
      payload.name,
      statuses.map(subscription => subscription.id),
    );
  }
  renderPreview(
    await api("/rss/preview", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    }), true,
  );
}

export async function saveRssSubscription(event) {
  event?.preventDefault?.();
  try {
    const payload = subscriptionPayload();
    if (!payload.name) throw new Error("Subscription name is required");
    let saved;
    if (editingId) {
      payload.id = editingId;
      // A blank URL leaves the configured value unchanged on the server.
      saved = await api(`/rss/${encodeURIComponent(editingId)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      showToast("RSS subscription updated", payload.name, "success");
    } else {
      if (!payload.url) throw new Error("Feed URL is required");
      const statuses = await api("/rss");
      payload.id = uniqueRssId(
        payload.name,
        statuses.map(subscription => subscription.id),
      );
      saved = await api("/rss", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      showToast("RSS subscription created", payload.name, "success");
    }
    resetRssForm();
    if(saved?.id) feedId=saved.id;
    selectPanel("watch-pane","feeds");
    await refreshRss();
    if (saved?.id) {
      try {
        renderPreview(await api(`/rss/${encodeURIComponent(saved.id)}/preview`));
      } catch (previewError) {
        showError("RSS saved, but preview failed", previewError);
      }
    }
  } catch (error) {
    showError("RSS save failed", error);
  }
}

export function resetRssForm() {
  editingId = null;
  $("#rss-draft-preview").hidden = true;
  editingLabels = [];
  const form = $("#rss-form");
  if (!form) return;
  form.reset();
  $("#rss-id").value = "";
  $("#rss-url").placeholder = "https://example.test/feed.xml";
  $("#rss-enabled").checked = true;
  $("#rss-interval").value = "900";
  $("#rss-start").value = "start";
  $("#rss-import-mode").value = "new_only";
  $("#rss-save-btn").textContent = "Add subscription";
}

async function runAction(action, id) {
  const key = ["poll","import-current","reconsider","retry"].includes(action) ? id+"/intake" : id+"/"+action;
  if (runningActions.has(key)) return;
  runningActions.add(key);
  try {
    if (action === "edit") {
      $("#rss-draft-preview").hidden = true;
      editingId = id;
      const statuses = await api("/rss");
      populateEditor(statuses);
      selectPanel("watch-pane","edit");
      $("#rss-form-card")?.scrollIntoView?.({ behavior: "smooth", block: "start" });
      $("#rss-name")?.focus?.();
      return;
    }
    if (action === "delete") {
      if (!window.confirm(`Delete RSS subscription ${id}? Durable history and verified completions are retained.`)) return;
      await api(`/rss/${encodeURIComponent(id)}`, { method: "DELETE" });
      showToast("RSS subscription deleted", id, "success");
      await refreshRss();
      return;
    }
    if (action === "preview") {
      selectPanel("watch-pane","feeds");
      renderPreview(await api(`/rss/${encodeURIComponent(id)}/preview`));
      return;
    }
    if (action === "poll" || action === "import-current") {
      const result = await api(`/rss/${encodeURIComponent(id)}/poll?import_current=${action === "import-current"}`, { method: "POST" });
      showToast("RSS poll finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: result.conditional_not_modified });
      await refreshRss();
      return;
    }
    if (action === "reconsider") {
      const result = await api(`/rss/${encodeURIComponent(id)}/poll?reconsider=true`, { method: "POST" });
      showToast("RSS reconsider finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: false });
      await refreshRss();
      return;
    }
    if (action === "retry") {
      const result = await api(`/rss/${encodeURIComponent(id)}/retry`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      showToast("RSS retry finished", `${result.imported?.length || 0} imports`, "success");
      renderPreview({ subscription_id: id, feed_title: id, decisions: result.decisions || [], conditional_not_modified: false });
      await refreshRss();
      return;
    }
    if (action === "decisions") {
      selectPanel("watch-pane","decisions");
      selectedId = id;
      decisionsBefore = null;
      const page = await api(`/rss/${encodeURIComponent(id)}/decisions?limit=100`);
      if (selectedId === id) renderDecisions(page);
    }
  } catch (error) {
    showError("RSS action failed", error);
  } finally { runningActions.delete(key); }
}

$("#rss-subscriptions")?.addEventListener("click", event => {
  const button = event.target.closest("button[data-rss-action]");
  if (!button) return;
  selectFeed(button.dataset.rssId);
  runAction(button.dataset.rssAction, button.dataset.rssId);
});

$("#rss-form")?.addEventListener("submit", saveRssSubscription);
$("#rss-reset-btn")?.addEventListener("click", resetRssForm);
$("#rss-preview-btn")?.addEventListener("click", async () => {
  try {
    await previewCurrentSubscription();
  } catch (error) {
    showError("RSS preview failed", error);
  }
});
$("#rss-decisions")?.addEventListener("click", async event => {
  if (event.target.id !== "rss-more-decisions") return;
  if (!selectedId || !decisionsBefore) return;
  try {
    const id = selectedId;
    const cursor = decisionsBefore;
    const page = await api(`/rss/${encodeURIComponent(id)}/decisions?limit=100&before=${cursor}`);
    if (selectedId === id && decisionsBefore === cursor) renderDecisions(page,true);
  } catch (error) {
    showError("RSS decision history failed", error);
  }
});

$("#view-watch")?.addEventListener("click", event=>{
  const button=event.target.closest("button");
  if(!button) return;
  if(button.dataset.feed) selectFeed(button.dataset.feed);
  const pane=button.dataset.watchPane;
  if(pane==="defaults") {
    document.querySelector('.nav[data-view="settings"]').click();
    document.querySelector('[data-settings-target="watch"]').click();
  } else if(pane==="decisions" && feedId) runAction("decisions",feedId);
  else if(pane) selectPanel("watch-pane",pane);
  if(button.id==="new-subscription") {resetRssForm();selectPanel("watch-pane","edit");$("#rss-name").focus();}
  if(button.id==="edit-subscription" && feedId) runAction("edit",feedId);
  if(["preview-subscription","feed-preview-action"].includes(button.id) && feedId) runAction("preview",feedId);
});
$("#feed-search")?.addEventListener("input",renderFeedList);


export async function inspectSubscription(id) {
  const statuses = await refreshRss();
  if (!statuses.some(sub => sub.id === id)) {showToast("Subscription unavailable",id,"warning");return;}
  selectPanel("watch-pane","feeds");
  selectFeed(id);
}
