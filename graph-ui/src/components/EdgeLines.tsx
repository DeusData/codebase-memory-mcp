import { useEffect, useRef, useState } from "react";
import * as THREE from "three";
import type { GraphNode, GraphEdge } from "../lib/types";
import { edgeIntensityScale } from "../lib/density";

interface EdgeLinesProps {
  nodes: GraphNode[];
  edges: GraphEdge[];
  highlightedIds: Set<number> | null;
  opacity?: number;
  /* User edge-brightness multiplier (see DisplaySettings). Applied to the
   * material color, never to the geometry buffers — see EdgeBuffers below. */
  brightness?: number;
  /* When set, edge.target is looked up in this array instead of `nodes`.
   * Used for cross-galaxy edges where source lives in the primary graph
   * and target lives in a linked project's offset-adjusted nodes. */
  targetNodes?: GraphNode[];
}

function getClusterKey(fp?: string): string {
  if (!fp) return "";
  const parts = fp.split("/");
  return parts.slice(0, Math.min(2, parts.length)).join("/");
}

/* Edge type → color (matches the filter panel) */
const EDGE_TYPE_COLORS: Record<string, string> = {
  CALLS: "#1DA27E",
  IMPORTS: "#3b82f6",
  DEFINES: "#a855f7",
  DEFINES_METHOD: "#a855f7",
  CONTAINS_FILE: "#22c55e",
  CONTAINS_FOLDER: "#22c55e",
  CONTAINS_PACKAGE: "#22c55e",
  HANDLES: "#eab308",
  IMPLEMENTS: "#f97316",
  HTTP_CALLS: "#e11d48",
  ASYNC_CALLS: "#ec4899",
  GRPC_CALLS: "#f59e0b",
  GRAPHQL_CALLS: "#e879f9",
  TRPC_CALLS: "#a78bfa",
  CROSS_HTTP_CALLS: "#fb923c",
  CROSS_ASYNC_CALLS: "#fb7185",
  CROSS_GRPC_CALLS: "#fbbf24",
  CROSS_GRAPHQL_CALLS: "#f0abfc",
  CROSS_TRPC_CALLS: "#c4b5fd",
  CROSS_CHANNEL: "#fdba74",
  MEMBER_OF: "#64748b",
  TESTS_FILE: "#06b6d4",
};

const DEFAULT_EDGE_COLOR = "#1C8585";

/* ── Allocate-once / refill-in-place geometry management ──────────────
 *
 * The naive approach (a fresh THREE.BufferGeometry + two new Float32Arrays
 * on every dependency change, handed to <lineSegments geometry={...}>) leaks:
 * R3F does not dispose a geometry passed as a prop (only geometries it
 * constructs itself via JSX), and three.js keeps a strong reference to every
 * attribute of an undisposed geometry in WebGLBindingStates. On a
 * 435k-node/4.2M-edge graph that is ~400MB of JS heap + ~200MB of GPU memory
 * per brightness-slider tick, never reclaimed (#2039).
 *
 * Instead we keep one BufferGeometry per "capacity" (the largest edge count
 * seen so far), preallocate its position/color attributes for that capacity,
 * and on every data change just refill the live TypedArrays in place and
 * adjust the draw range — no new geometry, no new attribute, no GC pressure.
 * The geometry is only replaced (and the old one disposed) when the edge
 * count grows past the current capacity, and it is always disposed on
 * unmount. */

interface EdgeBuffers {
  geometry: THREE.BufferGeometry;
  positionAttr: THREE.BufferAttribute;
  colorAttr: THREE.BufferAttribute;
  /** Number of edge "slots" currently allocated (each slot = one line = 2
   * vertices = 6 floats per attribute). */
  capacity: number;
}

export function createEdgeBuffers(capacity: number): EdgeBuffers {
  const geometry = new THREE.BufferGeometry();
  const positionAttr = new THREE.BufferAttribute(
    new Float32Array(Math.max(1, capacity) * 6),
    3,
  );
  const colorAttr = new THREE.BufferAttribute(
    new Float32Array(Math.max(1, capacity) * 6),
    3,
  );
  positionAttr.setUsage(THREE.DynamicDrawUsage);
  colorAttr.setUsage(THREE.DynamicDrawUsage);
  geometry.setAttribute("position", positionAttr);
  geometry.setAttribute("color", colorAttr);
  geometry.setDrawRange(0, 0);
  return { geometry, positionAttr, colorAttr, capacity: Math.max(1, capacity) };
}

/** Refill `buffers`' position/color attributes in place from the given
 * edges (never allocating new TypedArrays). Returns the number of valid
 * (rendered) edges. Density scaling (automatic, edge-count based) is baked
 * into the vertex colors here; the user brightness multiplier is NOT — it is
 * applied on the material instead, so the slider never touches this data. */
export function fillEdgeBuffers(
  buffers: EdgeBuffers,
  nodes: GraphNode[],
  edges: GraphEdge[],
  highlightedIds: Set<number> | null,
  targetNodes: GraphNode[] | undefined,
): number {
  const densityScale = edgeIntensityScale(edges.length);
  const srcMap = new Map<number, number>();
  for (let i = 0; i < nodes.length; i++) {
    srcMap.set(nodes[i].id, i);
  }
  const tgtArr = targetNodes ?? nodes;
  const tgtMap = targetNodes ? new Map<number, number>() : srcMap;
  if (targetNodes) {
    for (let i = 0; i < targetNodes.length; i++) {
      tgtMap.set(targetNodes[i].id, i);
    }
  }

  const hasHighlight = highlightedIds && highlightedIds.size > 0;
  const positions = buffers.positionAttr.array as Float32Array;
  const colors = buffers.colorAttr.array as Float32Array;
  const edgeColor = new THREE.Color();
  let validCount = 0;

  for (const edge of edges) {
    const si = srcMap.get(edge.source);
    const ti = tgtMap.get(edge.target);
    if (si === undefined || ti === undefined) continue;

    const s = nodes[si];
    const t = tgtArr[ti];

    const sHL = !hasHighlight || highlightedIds.has(s.id);
    const tHL = !hasHighlight || highlightedIds.has(t.id);
    if (hasHighlight && !sHL && !tHL) continue;

    const sameCluster =
      getClusterKey(s.file_path) === getClusterKey(t.file_path);

    /* Intensity based on cluster membership and highlight.
     * With additive blending + dark background, these glow nicely. */
    let intensity = sameCluster ? 0.25 : 0.06;
    if (hasHighlight) {
      /* A selection stays at full strength (never density-scaled) so it
       * pops against the dimmed rest; only the un-selected bulk is scaled. */
      intensity = sHL && tHL ? 0.5 : 0.04 * densityScale;
    } else {
      intensity *= densityScale;
    }

    const off = validCount * 6;
    positions[off] = s.x;
    positions[off + 1] = s.y;
    positions[off + 2] = s.z;
    positions[off + 3] = t.x;
    positions[off + 4] = t.y;
    positions[off + 5] = t.z;

    /* Color from edge TYPE (correlates with edge type filter) */
    edgeColor.set(EDGE_TYPE_COLORS[edge.type] ?? DEFAULT_EDGE_COLOR);
    colors[off] = edgeColor.r * intensity;
    colors[off + 1] = edgeColor.g * intensity;
    colors[off + 2] = edgeColor.b * intensity;
    colors[off + 3] = edgeColor.r * intensity;
    colors[off + 4] = edgeColor.g * intensity;
    colors[off + 5] = edgeColor.b * intensity;
    validCount++;
  }

  buffers.positionAttr.needsUpdate = true;
  buffers.colorAttr.needsUpdate = true;
  buffers.geometry.setDrawRange(0, validCount * 2);
  return validCount;
}

/** Owns one EdgeBuffers for the lifetime of the component, growing (and
 * disposing the previous geometry) only when the edge count exceeds the
 * current capacity, and disposing on unmount. Brightness is intentionally
 * NOT a dependency — it never touches the geometry. */
export function useEdgeGeometry(
  nodes: GraphNode[],
  edges: GraphEdge[],
  highlightedIds: Set<number> | null,
  targetNodes: GraphNode[] | undefined,
): THREE.BufferGeometry {
  /* Records the exact (by-reference) deps the mount initializer below filled
   * the buffers with, so the effect's first run — which always fires with
   * those same references, since it belongs to the same render — can skip
   * redoing that fill. Without this, a 4.2M-edge mount fills the buffers
   * twice: once synchronously (to avoid a one-tick empty-frame flash) and
   * once more in the effect. */
  const initialFillDeps = useRef<
    | [GraphNode[], GraphEdge[], Set<number> | null, GraphNode[] | undefined]
    | null
  >(null);

  const [buffers, setBuffers] = useState<EdgeBuffers>(() => {
    /* Fill synchronously on first mount (not just in the effect below) so
     * the first painted frame already has data instead of a one-tick flash
     * of an empty draw range. */
    const buf = createEdgeBuffers(edges.length);
    fillEdgeBuffers(buf, nodes, edges, highlightedIds, targetNodes);
    initialFillDeps.current = [nodes, edges, highlightedIds, targetNodes];
    return buf;
  });
  /* Mirrors the latest `buffers` so the unmount cleanup (empty deps) always
   * disposes the current geometry, not the one captured at mount time. */
  const latest = useRef(buffers);
  latest.current = buffers;

  useEffect(() => {
    const init = initialFillDeps.current;
    initialFillDeps.current = null;
    if (
      init &&
      init[0] === nodes &&
      init[1] === edges &&
      init[2] === highlightedIds &&
      init[3] === targetNodes
    ) {
      /* Same references the initializer just filled with — skip the
       * redundant refill. */
      return;
    }
    setBuffers((prev) => {
      let buf = prev;
      if (edges.length > buf.capacity) {
        prev.geometry.dispose();
        buf = createEdgeBuffers(edges.length);
      }
      fillEdgeBuffers(buf, nodes, edges, highlightedIds, targetNodes);
      return buf;
    });
  }, [nodes, edges, highlightedIds, targetNodes]);

  useEffect(() => {
    return () => {
      latest.current.geometry.dispose();
    };
  }, []);

  return buffers.geometry;
}

export function EdgeLines({
  nodes,
  edges,
  highlightedIds,
  opacity = 1.0,
  brightness = 1.0,
  targetNodes,
}: EdgeLinesProps) {
  const geometry = useEdgeGeometry(nodes, edges, highlightedIds, targetNodes);

  return (
    /* frustumCulled={false}, not a computeBoundingSphere() call after every
     * fill: geometry.boundingSphere is computed lazily once (by Line's
     * raycast or by WebGLRenderer's frustum check) and then goes stale on
     * every in-place refill above, same as NodePoints — but unlike
     * NodePoints, these lineSegments have no pointer handlers, so raycast
     * accuracy doesn't matter here, only "don't let a stale sphere
     * frustum-cull the whole edge cloud" does. Opting out of culling is
     * cheaper than rescanning a potentially multi-million-vertex buffer on
     * every highlight/filter change just to keep the sphere fresh, and
     * matches NodeCloud's InstancedMesh path, which already does the same. */
    <lineSegments geometry={geometry} frustumCulled={false}>
      {/* LineBasicMaterial multiplies the vertex color by material.color
       * when vertexColors is set, so the brightness slider rides entirely on
       * this tiny material uniform — it never touches the (potentially
       * millions-of-floats) geometry attributes above. A new array literal
       * each render is intentional: it is what makes R3F re-apply `.set()`
       * on the material's color, since mutating a stable Color instance in
       * place would not be detected by R3F's prop diff. */}
      <lineBasicMaterial
        vertexColors
        color={[brightness, brightness, brightness]}
        transparent
        opacity={opacity}
        blending={THREE.AdditiveBlending}
        depthWrite={false}
        toneMapped={false}
      />
    </lineSegments>
  );
}
