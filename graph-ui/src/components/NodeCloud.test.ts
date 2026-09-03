/* @vitest-environment jsdom */
import { describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import * as THREE from "three";
import {
  createPointBuffers,
  fillPointBuffers,
  usePointGeometry,
} from "./NodeCloud";
import type { GraphNode } from "../lib/types";

function node(id: number, x = 0): GraphNode {
  return { id, x, y: 0, z: 0, label: "Function", name: `n${id}`, size: 1, color: "#80a0ff" };
}

const NODES: GraphNode[] = [node(1, 0), node(2, 10), node(3, 20)];

describe("createPointBuffers / fillPointBuffers", () => {
  it("allocates position/color attributes sized to capacity", () => {
    const buf = createPointBuffers(3);
    expect(buf.geometry.getAttribute("position").array.length).toBe(3 * 3);
    expect(buf.geometry.getAttribute("color").array.length).toBe(3 * 3);
    expect(buf.capacity).toBe(3);
  });

  it("fills positions and sets the draw range to the node count", () => {
    const buf = createPointBuffers(3);
    fillPointBuffers(buf, NODES, null, 1, 1);
    expect(buf.geometry.drawRange).toEqual({ start: 0, count: 3 });
    const positions = buf.positionAttr.array as Float32Array;
    expect(positions[3]).toBe(10);
    expect(positions[6]).toBe(20);
  });

  it("refills the SAME TypedArray in place across calls", () => {
    const buf = createPointBuffers(3);
    const positionsArray = buf.positionAttr.array;
    fillPointBuffers(buf, NODES, null, 1, 1);
    fillPointBuffers(buf, NODES, new Set([1]), 1, 1);
    expect(buf.positionAttr.array).toBe(positionsArray);
  });

  it("recomputes the bounding sphere on every fill, even an in-place refill with unchanged capacity", () => {
    /* Points.raycast() only computes geometry.boundingSphere lazily when it
     * is null — a stale sphere left over from an earlier fill would break
     * hover/click raycasting and could frustum-cull the whole cloud once the
     * node positions move away from it. */
    const buf = createPointBuffers(3);
    fillPointBuffers(buf, NODES, null, 1, 1);
    expect(buf.geometry.boundingSphere).not.toBeNull();
    const firstCenterX = buf.geometry.boundingSphere!.center.x;

    const movedNodes = NODES.map((n) => ({ ...n, x: n.x + 1000 }));
    fillPointBuffers(buf, movedNodes, null, 1, 1);

    expect(buf.geometry.boundingSphere).not.toBeNull();
    expect(buf.geometry.boundingSphere!.center.x).not.toBe(firstCenterX);
    expect(buf.geometry.boundingSphere!.center.x).toBeGreaterThan(500);
  });
});

describe("usePointGeometry", () => {
  it("reuses the same geometry instance when the node count doesn't grow", () => {
    const { result, rerender } = renderHook(
      ({ opacity }: { opacity: number }) =>
        usePointGeometry(NODES, null, opacity, 1),
      { initialProps: { opacity: 1 } },
    );

    const first = result.current;
    expect(first).toBeInstanceOf(THREE.BufferGeometry);

    rerender({ opacity: 0.5 });
    expect(result.current).toBe(first);
  });

  it("disposes the geometry when node count grows past capacity", () => {
    const { result, rerender } = renderHook(
      ({ nodes }: { nodes: GraphNode[] }) => usePointGeometry(nodes, null, 1, 1),
      { initialProps: { nodes: NODES } },
    );

    const first = result.current;
    const disposeSpy = vi.spyOn(first, "dispose");

    rerender({ nodes: [...NODES, node(4, 30)] });

    expect(disposeSpy).toHaveBeenCalledTimes(1);
    expect(result.current).not.toBe(first);
  });

  it("disposes the current geometry on unmount", () => {
    const { result, unmount } = renderHook(() =>
      usePointGeometry(NODES, null, 1, 1),
    );
    const disposeSpy = vi.spyOn(result.current, "dispose");
    unmount();
    expect(disposeSpy).toHaveBeenCalledTimes(1);
  });

  it("fills the geometry exactly once on mount (initializer, not initializer + effect)", () => {
    /* createPointBuffers calls setDrawRange(0, 0) once at construction; a
     * single fillPointBuffers call adds exactly one more setDrawRange call.
     * A double fill (mount initializer + the effect's first, unguarded run)
     * would show up as a third call. */
    const setDrawRangeSpy = vi.spyOn(
      THREE.BufferGeometry.prototype,
      "setDrawRange",
    );
    renderHook(() => usePointGeometry(NODES, null, 1, 1));
    expect(setDrawRangeSpy).toHaveBeenCalledTimes(2);
    setDrawRangeSpy.mockRestore();
  });
});
