import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchLayout,
  clampNodeBudget,
  GRAPH_RENDER_NODE_LIMIT,
  GRAPH_NODE_BUDGET_STEP,
  GRAPH_NODE_BUDGET_MAX,
} from "./useGraphData";

describe("fetchLayout", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("uses the default node budget when none is given", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ nodes: [], edges: [], total_nodes: 0 }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchLayout("large-project");

    expect(GRAPH_RENDER_NODE_LIMIT).toBe(5000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const calls = fetchMock.mock.calls as unknown as Array<[string]>;
    const [url] = calls[0];
    expect(url).toBe(
      "/api/layout?project=large-project&max_nodes=5000",
    );
  });

  it("passes an explicit node budget through to the layout endpoint", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ nodes: [], edges: [], total_nodes: 0 }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchLayout("large-project", 250000);

    const calls = fetchMock.mock.calls as unknown as Array<[string]>;
    const [url] = calls[0];
    expect(url).toBe(
      "/api/layout?project=large-project&max_nodes=250000",
    );
  });

  it("reports streaming progress while the body downloads", async () => {
    const payload = new TextEncoder().encode(
      JSON.stringify({ nodes: [], edges: [], total_nodes: 7 }),
    );
    const half = Math.floor(payload.length / 2);
    const chunks = [payload.slice(0, half), payload.slice(half)];
    let read = 0;
    const fetchMock = vi.fn(async () => ({
      ok: true,
      headers: new Headers({ "content-length": String(payload.length) }),
      body: {
        getReader: () => ({
          read: async () =>
            read < chunks.length
              ? { done: false, value: chunks[read++] }
              : { done: true, value: undefined },
        }),
      },
      json: async () => {
        throw new Error("json() must not be used when streaming");
      },
    }));
    vi.stubGlobal("fetch", fetchMock);

    const seen: number[] = [];
    const result = await fetchLayout("p", 5000, (p) => {
      seen.push(p.receivedBytes);
      expect(p.totalBytes).toBe(payload.length);
    });

    expect(result.total_nodes).toBe(7);
    expect(seen).toEqual([half, payload.length]);
  });
});

describe("clampNodeBudget", () => {
  it("snaps to 5k steps within the 5k..10M range", () => {
    expect(GRAPH_NODE_BUDGET_STEP).toBe(5000);
    expect(GRAPH_NODE_BUDGET_MAX).toBe(10_000_000);
    expect(clampNodeBudget(5000)).toBe(5000);
    expect(clampNodeBudget(12345)).toBe(10000);
    expect(clampNodeBudget(12501)).toBe(15000);
    expect(clampNodeBudget(0)).toBe(5000);
    expect(clampNodeBudget(-500)).toBe(5000);
    expect(clampNodeBudget(99_999_999)).toBe(10_000_000);
    expect(clampNodeBudget(Number.NaN)).toBe(GRAPH_RENDER_NODE_LIMIT);
  });
});
