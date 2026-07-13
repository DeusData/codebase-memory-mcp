"use strict";

const { describe, test } = require("node:test");
const assert = require("node:assert/strict");
const { apiChatRequest, chatUrl } = require("../providercore");

describe("chatUrl", () => {
  test("accepts a base with or without /v1", () => {
    assert.equal(chatUrl("https://api.example.test"),
      "https://api.example.test/v1/chat/completions");
    assert.equal(chatUrl("https://api.example.test/custom/v1/"),
      "https://api.example.test/custom/v1/chat/completions");
  });

  test("rejects non-http protocols", () => {
    assert.throws(() => chatUrl("file:///tmp/provider"), /http\/https/);
  });
});

describe("apiChatRequest", () => {
  test("sends the OpenAI-compatible contract without leaking the key", async () => {
    let captured;
    const fetchImpl = async (url, options) => {
      captured = { url, options };
      return new Response(JSON.stringify({ choices: [{ message: { content: "answer" } }] }),
        { status: 200, headers: { "Content-Type": "application/json" } });
    };
    const result = await apiChatRequest({ provider: "test", model: "model-a",
      prompt: "hello", base: "https://api.example.test", apiKey: "secret", fetchImpl });
    assert.deepEqual(result, { ok: true, out: "answer" });
    assert.equal(captured.url, "https://api.example.test/v1/chat/completions");
    assert.equal(captured.options.headers.Authorization, "Bearer secret");
    assert.deepEqual(JSON.parse(captured.options.body), {
        model: "model-a", messages: [{ role: "user", content: "hello" }],
    });
  });

  test("returns a provider error", async () => {
    const fetchImpl = async () => new Response(JSON.stringify({ error: { message: "rate limited" } }),
      { status: 429 });
    const result = await apiChatRequest({ provider: "test", model: "m", prompt: "p",
      base: "https://api.example.test", fetchImpl });
    assert.deepEqual(result, { ok: false, out: "rate limited" });
  });

  test("times out a provider that never responds", async () => {
    const fetchImpl = async (_url, { signal }) => new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
    // AbortSignal.timeout uses an unref'ed timer in Node; this handle mirrors
    // Electron's live event loop so the test runner waits for the abort event.
    const keepAlive = setTimeout(() => {}, 100);
    try {
      const result = await apiChatRequest({ provider: "slow", model: "m", prompt: "p",
        base: "https://api.example.test", timeoutMs: 10, fetchImpl });
      assert.equal(result.ok, false);
      assert.match(result.out, /timed out/);
    } finally { clearTimeout(keepAlive); }
  });

  test("rejects malformed and oversized responses", async () => {
    const malformed = await apiChatRequest({ provider: "bad", model: "m", prompt: "p",
      base: "https://api.example.test", fetchImpl: async () => new Response("not-json") });
    assert.match(malformed.out, /invalid JSON/);

    const oversized = await apiChatRequest({ provider: "large", model: "m", prompt: "p",
      base: "https://api.example.test", maxResponseBytes: 100,
      fetchImpl: async () => new Response("{}", { headers: { "Content-Length": "1000" } }) });
    assert.match(oversized.out, /response too large/);
  });
});
