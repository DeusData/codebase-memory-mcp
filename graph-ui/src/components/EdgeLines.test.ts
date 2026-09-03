/* @vitest-environment jsdom */
import { describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import * as THREE from "three";
import {
  createEdgeBuffers,
  fillEdgeBuffers,
  useEdgeGeometry,
} from "./EdgeLines";
import type { GraphEdge, GraphNode } from "../lib/types";

function node(id: number, x = 0): GraphNode {
  return { id, x, y: 0, z: 0, label: "Function", name: `n${id}`, size: 1, color: "#fff" };
}

const NODES: GraphNode[] = [node(1, 0), node(2, 10), node(3, 20)];
const EDGES: GraphEdge[] = [
  { source: 1, target: 2, type: "CALLS" },
  { source: 2, target: 3, type: "IMPORTS" },
];

describe("createEdgeBuffers / fillEdgeBuffers", () => {
  it("allocates position/color attributes sized to capacity", () => {
    const buf = createEdgeBuffers(2);
    expect(buf.geometry.getAttribute("position").array.length).toBe(2 * 6);
    expect(buf.geometry.getAttribute("color").array.length).toBe(2 * 6);
    expect(buf.capacity).toBe(2);
  });

  it("fills positions/colors and sets the draw range to the valid edge count", () => {
    const buf = createEdgeBuffers(EDGES.length);
    const validCount = fillEdgeBuffers(buf, NODES, EDGES, null, undefined);
    expect(validCount).toBe(2);
    expect(buf.geometry.drawRange).toEqual({ start: 0, count: 4 });

    const positions = buf.positionAttr.array as Float32Array;
    /* First edge: node 1 (x=0) -> node 2 (x=10) */
    expect(positions[0]).toBe(0);
    expect(positions[3]).toBe(10);
  });

  it("skips edges with a dangling endpoint and still fills the valid ones", () => {
    const buf = createEdgeBuffers(3);
    const edgesWithDangling: GraphEdge[] = [
      { source: 1, target: 999, type: "CALLS" },
      ...EDGES,
    ];
    const validCount = fillEdgeBuffers(buf, NODES, edgesWithDangling, null, undefined);
    expect(validCount).toBe(2);
  });

  it("refills the SAME TypedArray in place — no reallocation for equal capacity", () => {
    const buf = createEdgeBuffers(EDGES.length);
    const positionsArray = buf.positionAttr.array;
    const colorsArray = buf.colorAttr.array;
    fillEdgeBuffers(buf, NODES, EDGES, null, undefined);
    fillEdgeBuffers(buf, NODES, EDGES, new Set([1, 2]), undefined);
    expect(buf.positionAttr.array).toBe(positionsArray);
    expect(buf.colorAttr.array).toBe(colorsArray);
  });
});

describe("useEdgeGeometry", () => {
  it("reuses the same geometry instance across re-renders when capacity doesn't grow (e.g. a brightness-only change)", () => {
    const { result, rerender } = renderHook(
      ({ highlightedIds }: { highlightedIds: Set<number> | null }) =>
        useEdgeGeometry(NODES, EDGES, highlightedIds, undefined),
      { initialProps: { highlightedIds: null as Set<number> | null } },
    );

    const first = result.current;
    expect(first).toBeInstanceOf(THREE.BufferGeometry);

    /* Simulate a brightness slider tick: brightness is applied to the
     * material, not the geometry, so it is not even a hook input — but we
     * also verify that re-rendering with the SAME edges/nodes/highlight
     * (as happens on every brightness change) keeps the same geometry. */
    rerender({ highlightedIds: null });
    expect(result.current).toBe(first);

    /* A highlight change refills in place too (same edge count → same
     * capacity), so it must not allocate a new geometry either. */
    rerender({ highlightedIds: new Set([1]) });
    expect(result.current).toBe(first);
  });

  it("disposes the geometry when the capacity grows (new geometry replaces the old, disposed one)", () => {
    const { result, rerender } = renderHook(
      ({ edges }: { edges: GraphEdge[] }) =>
        useEdgeGeometry(NODES, edges, null, undefined),
      { initialProps: { edges: EDGES } },
    );

    const first = result.current;
    const disposeSpy = vi.spyOn(first, "dispose");

    const biggerEdges: GraphEdge[] = [
      ...EDGES,
      { source: 3, target: 1, type: "CALLS" },
      { source: 1, target: 3, type: "IMPORTS" },
    ];
    rerender({ edges: biggerEdges });

    expect(disposeSpy).toHaveBeenCalledTimes(1);
    expect(result.current).not.toBe(first);
  });

  it("disposes the current geometry on unmount", () => {
    const { result, unmount } = renderHook(() =>
      useEdgeGeometry(NODES, EDGES, null, undefined),
    );
    const disposeSpy = vi.spyOn(result.current, "dispose");
    unmount();
    expect(disposeSpy).toHaveBeenCalledTimes(1);
  });

  it("fills the geometry exactly once on mount (initializer, not initializer + effect)", () => {
    /* createEdgeBuffers calls setDrawRange(0, 0) once at construction; a
     * single fillEdgeBuffers call adds exactly one more setDrawRange call.
     * A double fill (mount initializer + the effect's first, unguarded run)
     * would show up as a third call. */
    const setDrawRangeSpy = vi.spyOn(
      THREE.BufferGeometry.prototype,
      "setDrawRange",
    );
    renderHook(() => useEdgeGeometry(NODES, EDGES, null, undefined));
    expect(setDrawRangeSpy).toHaveBeenCalledTimes(2);
    setDrawRangeSpy.mockRestore();
  });
});
