import { useMemo } from "react";
import * as THREE from "three";
import type { GraphNode, GraphEdge } from "../lib/types";

interface EdgeLinesProps {
  nodes: GraphNode[];
  edges: GraphEdge[];
  highlightedIds: Set<number> | null;
  opacity?: number;
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

export function EdgeLines({
  nodes,
  edges,
  highlightedIds,
  opacity = 1.0,
  targetNodes,
}: EdgeLinesProps) {
  const geometry = useMemo(() => {
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
    const positions = new Float32Array(edges.length * 6);
    const colors = new Float32Array(edges.length * 6);
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
        intensity = sHL && tHL ? 0.5 : 0.04;
      }

      const off = validCount * 6;
      positions[off] = s.x;
      positions[off + 1] = s.y;
      positions[off + 2] = s.z;
      positions[off + 3] = t.x;
      positions[off + 4] = t.y;
      positions[off + 5] = t.z;

      /* Color from edge TYPE (correlates with edge type filter) */
      const edgeColor = new THREE.Color(
        EDGE_TYPE_COLORS[edge.type] ?? DEFAULT_EDGE_COLOR,
      );
      colors[off] = edgeColor.r * intensity;
      colors[off + 1] = edgeColor.g * intensity;
      colors[off + 2] = edgeColor.b * intensity;
      colors[off + 3] = edgeColor.r * intensity;
      colors[off + 4] = edgeColor.g * intensity;
      colors[off + 5] = edgeColor.b * intensity;
      validCount++;
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute(
      "position",
      new THREE.BufferAttribute(positions.slice(0, validCount * 6), 3),
    );
    geo.setAttribute(
      "color",
      new THREE.BufferAttribute(colors.slice(0, validCount * 6), 3),
    );
    return geo;
  }, [nodes, edges, highlightedIds, targetNodes]);

  return (
    <lineSegments geometry={geometry}>
      <lineBasicMaterial
        vertexColors
        transparent
        opacity={opacity}
        blending={THREE.AdditiveBlending}
        depthWrite={false}
        toneMapped={false}
      />
    </lineSegments>
  );
}
