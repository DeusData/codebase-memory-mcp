"use strict";
const t = require("./i18n").t;

const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_MAX_RESPONSE_BYTES = 2 * 1024 * 1024;

function chatUrl(base) {
  const url = new URL(String(base || ""));
  if (url.protocol !== "http:" && url.protocol !== "https:")
    throw new Error(t("provider.httpOnly"));
  const path = url.pathname.replace(/\/+$/, "");
  url.pathname = /\/v1$/.test(path) ? `${path}/chat/completions` : `${path}/v1/chat/completions`;
  url.search = "";
  url.hash = "";
  return url.toString();
}

async function apiChatRequest({ provider, model, prompt, base, apiKey = "",
  timeoutMs = DEFAULT_TIMEOUT_MS, maxResponseBytes = DEFAULT_MAX_RESPONSE_BYTES,
  fetchImpl = globalThis.fetch }) {
  const name = String(provider || "provider");
  try {
    const headers = { "Content-Type": "application/json" };
    if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
    const response = await fetchImpl(chatUrl(base), {
      method: "POST",
      headers,
      signal: AbortSignal.timeout(timeoutMs),
      body: JSON.stringify({ model, messages: [{ role: "user", content: prompt }] }),
    });
    const declared = Number(response.headers.get("content-length"));
    if (Number.isFinite(declared) && declared > maxResponseBytes)
      return { ok: false, out: `${name}: ${t("provider.tooBig")}` };
    const raw = await response.text();
    if (Buffer.byteLength(raw) > maxResponseBytes)
      return { ok: false, out: `${name}: ${t("provider.tooBig")}` };
    let json;
    try { json = JSON.parse(raw); }
    catch { return { ok: false, out: `${name}: ${t("provider.badJson")}` }; }
    const out = json?.choices?.[0]?.message?.content;
    return { ok: response.ok && !!out,
      out: out || json?.error?.message || raw.slice(0, 400) || `${name}: ${t("provider.empty")}` };
  } catch (error) {
    const timedOut = error?.name === "TimeoutError" || error?.name === "AbortError";
    return { ok: false, out: timedOut
      ? `${name}: ${t("provider.timeout", { ms: timeoutMs })}`
      : `${name}: ${error?.message || t("provider.requestError")}` };
  }
}

module.exports = { apiChatRequest, chatUrl, DEFAULT_TIMEOUT_MS, DEFAULT_MAX_RESPONSE_BYTES };
