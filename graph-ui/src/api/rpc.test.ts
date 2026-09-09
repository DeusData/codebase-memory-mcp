import { afterEach, describe, expect, it, vi } from "vitest";

import { callTool } from "./rpc";
import { clearDashboardToken, setDashboardToken } from "./dashboardAuth";

const TOKEN = "a".repeat(64);

describe("callTool", () => {
  afterEach(() => {
    clearDashboardToken();
    vi.unstubAllGlobals();
  });

  it("sends a tokenized tools/call request and prefers structuredContent", async () => {
    expect(setDashboardToken(TOKEN)).toBe(true);
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        result: {
          structuredContent: { status: "ready" },
          content: [{ text: '{"status":"stale-copy"}' }],
        },
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(callTool("index_status")).resolves.toEqual({ status: "ready" });

    expect(fetchMock).toHaveBeenCalledWith(`/rpc?token=${TOKEN}`, expect.objectContaining({
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: expect.stringContaining('"name":"index_status"'),
    }));
  });

  it("returns an unwrapped result when the server does not provide text content", async () => {
    setDashboardToken(TOKEN);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ result: { accepted: true } }),
    }));

    await expect(callTool("index_status")).resolves.toEqual({ accepted: true });
  });

  it("surfaces transport and JSON-RPC errors as RpcError", async () => {
    setDashboardToken(TOKEN);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      statusText: "Unavailable",
    }));
    await expect(callTool("search_graph")).rejects.toMatchObject({
      code: -1,
      message: "HTTP 503: Unavailable",
    });

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ error: { code: -32602, message: "invalid params" } }),
    }));
    await expect(callTool("search_graph")).rejects.toMatchObject({
      code: -32602,
      message: "invalid params",
    });
  });

  it("refuses to send requests when the launch capability is missing", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(callTool("index_status")).rejects.toMatchObject({
      message: "Dashboard authentication token is missing",
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
