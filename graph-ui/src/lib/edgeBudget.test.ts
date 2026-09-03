import { describe, expect, it } from "vitest";
import { EDGE_RENDER_LIMIT, sampleEdges } from "./edgeBudget";
import type { GraphEdge } from "./types";

function edges(n: number): GraphEdge[] {
  return Array.from({ length: n }, (_, i) => ({
    source: i,
    target: i + 1,
    type: "CALLS",
  }));
}

describe("EDGE_RENDER_LIMIT", () => {
  it("is 1,000,000", () => {
    expect(EDGE_RENDER_LIMIT).toBe(1_000_000);
  });
});

describe("sampleEdges", () => {
  it("keeps all edges (same reference, no copy) when under the limit", () => {
    const input = edges(500);
    const result = sampleEdges(input, 1000);
    expect(result).toBe(input);
    expect(result.length).toBe(500);
  });

  it("keeps all edges when exactly at the limit", () => {
    const input = edges(1000);
    const result = sampleEdges(input, 1000);
    expect(result).toBe(input);
  });

  it("respects the limit when over it", () => {
    const input = edges(10_000);
    const result = sampleEdges(input, 1000);
    expect(result.length).toBe(1000);
  });

  it("is deterministic — the same input always samples to the same output", () => {
    const input = edges(4_200_000);
    const a = sampleEdges(input, EDGE_RENDER_LIMIT);
    const b = sampleEdges(input, EDGE_RENDER_LIMIT);
    expect(a).toEqual(b);
    expect(a.length).toBe(EDGE_RENDER_LIMIT);
  });

  it("spreads the sample across the full input range (stride sampling, not a prefix)", () => {
    const input = edges(1000);
    const result = sampleEdges(input, 100);
    /* The last sampled edge should come from near the end of the input, not
     * just the first 100 elements. */
    const lastSourceIndex = result[result.length - 1].source;
    expect(lastSourceIndex).toBeGreaterThan(900);
  });

  it("returns an empty array for a non-positive limit", () => {
    expect(sampleEdges(edges(10), 0)).toEqual([]);
    expect(sampleEdges(edges(10), -5)).toEqual([]);
  });
});
