import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchLayout,
  clampNodeBudget,
  internGraphStrings,
  GRAPH_RENDER_NODE_LIMIT,
  GRAPH_NODE_BUDGET_STEP,
  GRAPH_NODE_BUDGET_MAX,
} from "./useGraphData";
import type { GraphData } from "../lib/types";

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

  it("decodes correctly even when a chunk boundary splits a multi-byte UTF-8 character", async () => {
    /* "café" — the é is a 2-byte UTF-8 sequence; split the payload so the
     * chunk boundary lands inside it. Only correct with {stream: true}
     * decoding across chunks. */
    const payload = new TextEncoder().encode(
      JSON.stringify({ nodes: [{ id: 1, x: 0, y: 0, z: 0, label: "café", name: "n", size: 1, color: "#fff" }], edges: [], total_nodes: 1 }),
    );
    const splitPoint = payload.indexOf(0xc3) + 1; // inside the 2-byte UTF-8 sequence
    const chunks = [payload.slice(0, splitPoint), payload.slice(splitPoint)];
    let read = 0;
    const fetchMock = vi.fn(async () => ({
      ok: true,
      headers: new Headers(),
      body: {
        getReader: () => ({
          read: async () =>
            read < chunks.length
              ? { done: false, value: chunks[read++] }
              : { done: true, value: undefined },
        }),
      },
    }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchLayout("p", 5000, () => {});
    expect(result.nodes[0].label).toBe("café");
  });
});

describe("internGraphStrings", () => {
  it("makes equal-valued node/edge strings share the same instance", () => {
    const data: GraphData = {
      nodes: [
        { id: 1, x: 0, y: 0, z: 0, label: "Function", name: "a", size: 1, color: "#fff", status: "dead" },
        { id: 2, x: 0, y: 0, z: 0, label: String("Function"), name: "b", size: 1, color: String("#fff"), status: String("dead") as GraphData["nodes"][number]["status"] },
      ],
      edges: [
        { source: 1, target: 2, type: "CALLS" },
        { source: 2, target: 1, type: String("CALLS") },
      ],
      total_nodes: 2,
    };

    const result = internGraphStrings(data);

    expect(result.nodes[0].label).toBe(result.nodes[1].label);
    expect(result.nodes[0].color).toBe(result.nodes[1].color);
    expect(result.nodes[0].status).toBe(result.nodes[1].status);
    expect(result.edges[0].type).toBe(result.edges[1].type);
  });

  it("interns strings inside linked_projects and missed_graph too", () => {
    const data: GraphData = {
      nodes: [],
      edges: [],
      total_nodes: 0,
      linked_projects: [
        {
          project: "lp",
          nodes: [{ id: 1, x: 0, y: 0, z: 0, label: "Function", name: "a", size: 1, color: "#fff" }],
          edges: [{ source: 1, target: 1, type: "CALLS" }],
          offset: { x: 0, y: 0, z: 0 },
          cross_edges: [{ source: 1, target: 1, type: String("CALLS") }],
        },
      ],
      missed_graph: {
        nodes: [{ id: 2, x: 0, y: 0, z: 0, label: String("Function"), name: "b", size: 1, color: "#fff" }],
        edges: [],
        offset: { x: 0, y: 0, z: 0 },
      },
    };

    const result = internGraphStrings(data);

    expect(result.linked_projects![0].edges[0].type).toBe(
      result.linked_projects![0].cross_edges[0].type,
    );
    expect(result.missed_graph!.nodes[0].label).toBe("Function");
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
