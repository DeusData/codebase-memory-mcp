import * as THREE from "three";
import { describe, expect, it } from "vitest";
import { nodeColor, sphereDetail } from "./NodeCloud";
import type { GraphNode } from "../lib/types";


const NODE: GraphNode = {
  id: 7,
  x: 0,
  y: 0,
  z: 0,
  label: "Function",
  name: "library-node",
  size: 1,
  color: "#ffffff",
};

describe("NodeCloud rendering helpers", () => {
  it("reduces sphere detail as the graph grows", () => {
    expect(sphereDetail(8_000)).toEqual([1, 32, 24]);
    expect(sphereDetail(8_001)).toEqual([1, 16, 12]);
    expect(sphereDetail(25_001)).toEqual([1, 10, 7]);
  });

  it("dims nodes outside the highlighted set", () => {
    const color = nodeColor(NODE, new Set([99]), 1, 1, new THREE.Color());
    expect(color[0]).toBeCloseTo(0.15);
    expect(color[1]).toBeCloseTo(0.15);
    expect(color[2]).toBeCloseTo(0.15);
  });

  it("applies opacity and the configured glow boost to active nodes", () => {
    const flat = nodeColor(NODE, null, 0.5, 0, new THREE.Color());
    const boosted = nodeColor(NODE, new Set([7]), 0.5, 1, new THREE.Color());
    expect(flat).toEqual([0.5, 0.5, 0.5]);
    expect(boosted[0]).toBeGreaterThanOrEqual(flat[0]);
  });
});
