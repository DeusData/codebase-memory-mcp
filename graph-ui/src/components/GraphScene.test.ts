import { describe, expect, it } from "vitest";
import { computeCameraTarget, GRAPH_CANVAS_DPR } from "./GraphScene";
import type { GraphNode } from "../lib/types";


function node(id: number, x: number, y: number, z: number): GraphNode {
  return { id, x, y, z, label: "Function", name: `node-${id}`, size: 1, color: "#ffffff" };
}

describe("GraphScene render limits", () => {
  it("caps the high-DPI WebGL backing store below the MSAA failure range", () => {
    expect(GRAPH_CANVAS_DPR[0]).toBe(1);
    expect(GRAPH_CANVAS_DPR[1]).toBeLessThanOrEqual(1.5);
  });

  it("returns no camera target for empty or unknown selections", () => {
    expect(computeCameraTarget([node(1, 0, 0, 0)], new Set())).toBeNull();
    expect(computeCameraTarget([node(1, 0, 0, 0)], new Set([2]))).toBeNull();
  });

  it("centers the camera on a selected cluster with a bounded distance", () => {
    const target = computeCameraTarget(
      [node(1, 0, 0, 0), node(2, 100, 200, 300), node(3, 999, 999, 999)],
      new Set([1, 2]),
    );

    expect(target).not.toBeNull();
    expect(target!.lookAt.toArray()).toEqual([50, 100, 150]);
    expect(target!.position.z).toBeGreaterThan(target!.lookAt.z);
  });
});
