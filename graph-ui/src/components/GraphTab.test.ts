import { describe, expect, it } from "vitest";
import {
  formatGraphLimitNotice,
  formatEdgeLimitNotice,
  type FilteredGraphData,
} from "./GraphTab";
import type { GraphData } from "../lib/types";

describe("formatGraphLimitNotice", () => {
  it("reports when the graph response is truncated for render safety", () => {
    const data = {
      nodes: Array.from({ length: 2000 }, (_, id) => ({
        id,
        x: 0,
        y: 0,
        z: 0,
        label: "Function",
        name: `fn${id}`,
        size: 1,
        color: "#ffffff",
      })),
      edges: [],
      total_nodes: 43729,
    } satisfies GraphData;

    expect(formatGraphLimitNotice(data)).toBe(
      "Showing 2,000 of 43,729 nodes (0 edges). Raise the node budget or use filters.",
    );
  });

  it("stays quiet when the full graph is rendered", () => {
    const data = {
      nodes: [],
      edges: [],
      total_nodes: 0,
    } satisfies GraphData;

    expect(formatGraphLimitNotice(data)).toBeNull();
  });
});

describe("formatEdgeLimitNotice", () => {
  it("reports when edges were sampled down to the render budget", () => {
    const data = {
      nodes: [],
      edges: Array.from({ length: 1_000_000 }, (_, i) => ({
        source: i,
        target: i + 1,
        type: "CALLS",
      })),
      total_nodes: 0,
      edgeFilterTotal: 4_200_000,
    } satisfies FilteredGraphData;

    expect(formatEdgeLimitNotice(data)).toBe(
      "Rendering 1,000,000 of 4,200,000 edges (render budget). Use filters to narrow further.",
    );
  });

  it("stays quiet when every filtered edge is rendered", () => {
    const data = {
      nodes: [],
      edges: [],
      total_nodes: 0,
      edgeFilterTotal: 0,
    } satisfies FilteredGraphData;

    expect(formatEdgeLimitNotice(data)).toBeNull();
  });

  it("stays quiet for null data", () => {
    expect(formatEdgeLimitNotice(null)).toBeNull();
  });
});
