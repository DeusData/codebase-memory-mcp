"use strict";

const fs = require("fs");
const t = require("./i18n").t;
const { spawn } = require("child_process");

const DEFAULT_GRAPH_URL = "http://127.0.0.1:9749";

async function probeGraphUi(url = DEFAULT_GRAPH_URL, fetchImpl = globalThis.fetch, timeoutMs = 800) {
  try {
    const response = await fetchImpl(url, { signal: AbortSignal.timeout(timeoutMs) });
    if (!response.ok || !String(response.headers.get("content-type") || "").includes("text/html")) return false;
    const html = (await response.text()).slice(0, 4096).toLowerCase();
    return html.includes("<html") || html.includes("<!doctype html");
  } catch { return false; }
}

function createGraphSupervisor({ binPath, cwd, openExternal,
  url = DEFAULT_GRAPH_URL, port = 9749, attempts = 40, delayMs = 150,
  probeImpl = probeGraphUi, spawnImpl = spawn, existsImpl = fs.existsSync,
  waitImpl = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) }) {
  let child = null;
  let opening = null;

  function stop() {
    if (!child) return;
    try { child.stdin?.end(); } catch {}
    try { if (!child.killed) child.kill(); } catch {}
    child = null;
  }

  async function doOpen() {
    if (await probeImpl(url)) {
      await openExternal(url);
      return { ok: true, reused: true };
    }
    if (!existsImpl(binPath))
      return { ok: false, error: t("graphcore.engineMissing") };

    let exited = false;
    let stderr = "";
    try {
      child = spawnImpl(binPath, ["--ui=true", `--port=${port}`], {
        cwd, env: process.env, stdio: ["pipe", "ignore", "pipe"],
      });
      child.once?.("exit", () => { exited = true; });
      child.once?.("error", (error) => { exited = true; stderr += error.message; });
      child.stderr?.on?.("data", (chunk) => { stderr = (stderr + String(chunk)).slice(-2000); });
    } catch (error) {
      child = null;
      return { ok: false, error: t("graphcore.startFailed", { msg: error.message }) };
    }

    for (let i = 0; i < attempts && !exited; i++) {
      await waitImpl(delayMs);
      if (await probeImpl(url)) {
        await openExternal(url);
        return { ok: true, started: true };
      }
    }
    stop();
    if (/without the embedded UI|no frontend embedded/i.test(stderr))
      return { ok: false, error: t("graphcore.noUi") };
    return { ok: false, error: exited
      ? t("graphcore.exitedOnStart")
      : t("graphcore.noResponse") };
  }

  async function open() {
    if (opening) return opening;
    opening = doOpen();
    try { return await opening; }
    finally { opening = null; }
  }

  return { open, stop };
}

module.exports = { createGraphSupervisor, probeGraphUi, DEFAULT_GRAPH_URL };
