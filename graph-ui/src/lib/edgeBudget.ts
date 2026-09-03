import type { GraphEdge } from "./types";

/* Hard cap on edges actually handed to EdgeLines for rendering. Above this,
 * every additional edge is still one more Float32Array(...*6) slot in the
 * geometry buffers (see EdgeLines.tsx / #2039) — on a 4.2M-edge graph that is
 * tens of megabytes of GPU-resident line data the user gets essentially no
 * visual benefit from (additively-blended lines this dense already read as a
 * flat wash). Down-sampling to a fixed budget keeps memory bounded
 * regardless of how large the underlying graph is. */
export const EDGE_RENDER_LIMIT = 1_000_000;

/** Deterministically sample down to at most `limit` edges, preserving
 * relative order. Uses a fixed stride over the input so the same input
 * always yields the same sampled set — no randomness, no per-render churn —
 * and every edge is kept untouched when the input is already at or under the
 * limit (same array reference is returned, no allocation). */
export function sampleEdges(edges: GraphEdge[], limit: number): GraphEdge[] {
  if (limit <= 0) return [];
  if (edges.length <= limit) return edges;

  const stride = edges.length / limit;
  const sampled = new Array<GraphEdge>(limit);
  for (let i = 0; i < limit; i++) {
    sampled[i] = edges[Math.floor(i * stride)];
  }
  return sampled;
}
