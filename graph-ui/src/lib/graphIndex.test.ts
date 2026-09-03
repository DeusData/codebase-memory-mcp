import { describe, expect, it } from "vitest";
import { buildGraphIndex } from "./graphIndex";
import type { GraphEdge, GraphNode } from "./types";

function node(id: number): GraphNode {
  return { id, x: 0, y: 0, z: 0, label: "Function", name: `n${id}`, size: 1, color: "#fff" };
}

describe("buildGraphIndex", () => {
  it("indexes nodes by id", () => {
    const nodes = [node(1), node(2)];
    const index = buildGraphIndex({ nodes, edges: [] });
    expect(index.nodeById.get(1)).toBe(nodes[0]);
    expect(index.nodeById.get(2)).toBe(nodes[1]);
  });

  it("builds bidirectional connection lists from edges", () => {
    const nodes = [node(1), node(2), node(3)];
    const edges: GraphEdge[] = [
      { source: 1, target: 2, type: "CALLS" },
      { source: 3, target: 1, type: "IMPORTS" },
    ];
    const index = buildGraphIndex({ nodes, edges });

    const fromOne = index.connectionsByNode.get(1) ?? [];
    expect(fromOne).toHaveLength(2);
    expect(fromOne).toContainEqual({ node: nodes[1], edgeType: "CALLS", direction: "outbound" });
    expect(fromOne).toContainEqual({ node: nodes[2], edgeType: "IMPORTS", direction: "inbound" });

    const fromTwo = index.connectionsByNode.get(2) ?? [];
    expect(fromTwo).toEqual([{ node: nodes[0], edgeType: "CALLS", direction: "inbound" }]);
  });

  it("skips edges with a dangling endpoint", () => {
    const nodes = [node(1)];
    const edges: GraphEdge[] = [{ source: 1, target: 999, type: "CALLS" }];
    const index = buildGraphIndex({ nodes, edges });
    expect(index.connectionsByNode.get(1)).toBeUndefined();
  });

  it("returns empty maps for null/undefined data", () => {
    expect(buildGraphIndex(null).nodeById.size).toBe(0);
    expect(buildGraphIndex(undefined).connectionsByNode.size).toBe(0);
  });
});
