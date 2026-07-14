// SPDX-License-Identifier: Apache-2.0

import { state, API, API_TOKEN_STORAGE_KEY } from "./state.js";

export function loadApiToken() {
  try {
    return window.localStorage.getItem(API_TOKEN_STORAGE_KEY) || "";
  } catch {
    return "";
  }
}

export function saveApiToken(token) {
  const normalized = String(token || "").trim();
  try {
    if (normalized) window.localStorage.setItem(API_TOKEN_STORAGE_KEY, normalized);
    else window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
  } catch {}
  return normalized;
}

export function withApiAuth(opts = {}) {
  const next = { ...opts };
  const headers = new Headers(opts.headers || {});
  const token = loadApiToken();
  if (token && !headers.has("x-swarmotter-auth") && !headers.has("authorization")) {
    headers.set("x-swarmotter-auth", token);
  }
  next.headers = headers;
  return next;
}

export async function readResponseJson(res) {
  const text = await res.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { success: false, error: { code: "parse_error", message: text } }; }
  return body;
}

export async function responseErrorMessage(res) {
  try {
    const body = await readResponseJson(res.clone());
    return body?.error?.message || body?.error?.code || res.statusText || `HTTP ${res.status}`;
  } catch {
    return res.statusText || `HTTP ${res.status}`;
  }
}

export async function promptForApiToken(message = "API token required") {
  if (state.apiTokenPromptInFlight) return state.apiTokenPromptInFlight;
  state.apiTokenPromptInFlight = Promise.resolve().then(() => {
    const token = window.prompt(`${message}\n\nEnter SwarmOtter API token:`, loadApiToken());
    if (token === null) return "";
    return saveApiToken(token);
  }).finally(() => {
    state.apiTokenPromptInFlight = null;
  });
  return state.apiTokenPromptInFlight;
}

export async function apiFetch(path, opts = {}, retryAuth = true) {
  const res = await fetch(API + path, withApiAuth(opts));
  if (res.status === 401 && retryAuth) {
    const token = await promptForApiToken(await responseErrorMessage(res));
    if (token) return apiFetch(path, opts, false);
  }
  return res;
}

export async function api(path, opts = {}) {
  const res = await apiFetch(path, opts);
  const body = await readResponseJson(res);
  if (!body.success && body.error) {
    const err = new Error(body.error.message || body.error.code);
    err.code = body.error.code;
    err.status = res.status;
    throw err;
  }
  return body.data;
}
