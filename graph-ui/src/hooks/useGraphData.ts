import { useCallback, useState } from "react";
import type { GraphData, GraphEdge, GraphNode, NodeStatus } from "../lib/types";

export interface LoadProgress {
  receivedBytes: number;
  totalBytes: number | null;
}

interface UseGraphDataResult {
  data: GraphData | null;
  loading: boolean;
  error: string | null;
  progress: LoadProgress;
  fetchOverview: (
    project: string,
    maxNodes?: number,
    graph?: "code" | "missed",
  ) => void;
  fetchDetail: (project: string, centerNode: string) => void;
}

/* Node budget: how many nodes the layout endpoint is asked for. The default
 * keeps first paint fast; the user can raise it in 5k steps up to the hard
 * ceiling (mirrors HARD_MAX_NODES in src/ui/layout3d.c). Edges always follow
 * the budget — the server returns every edge between the loaded nodes. */
export const GRAPH_RENDER_NODE_LIMIT = 5000;
export const GRAPH_NODE_BUDGET_STEP = 5000;
export const GRAPH_NODE_BUDGET_MAX = 10_000_000;

export function clampNodeBudget(value: number): number {
  if (!Number.isFinite(value)) return GRAPH_RENDER_NODE_LIMIT;
  const stepped =
    Math.round(value / GRAPH_NODE_BUDGET_STEP) * GRAPH_NODE_BUDGET_STEP;
  if (stepped < GRAPH_NODE_BUDGET_STEP) return GRAPH_NODE_BUDGET_STEP;
  if (stepped > GRAPH_NODE_BUDGET_MAX) return GRAPH_NODE_BUDGET_MAX;
  return stepped;
}

/** Which graph to lay out: the code graph (default) or the missed graph —
 *  only files the indexer could not fully cover, as their file structure. */
export type GraphVariant = "code" | "missed";

/** A 4.2M-edge graph has 4.2M freshly-parsed `type` strings (and similarly
 * many node `label`/`color`/`status` strings) that are almost all one of a
 * few dozen distinct values, but JSON.parse gives each occurrence its own
 * string object. Intern them through a shared pool so the whole graph ends
 * up referencing ~20 string instances instead of millions. Mutates the
 * parsed objects in place (they are freshly parsed, not shared with
 * anything else yet). */
export function internGraphStrings(data: GraphData): GraphData {
  const pool = new Map<string, string>();
  const intern = (s: string): string => {
    const existing = pool.get(s);
    if (existing !== undefined) return existing;
    pool.set(s, s);
    return s;
  };

  const internNode = (n: GraphNode): GraphNode => {
    n.label = intern(n.label);
    n.color = intern(n.color);
    if (n.status) n.status = intern(n.status) as NodeStatus;
    return n;
  };
  const internEdge = (e: GraphEdge): GraphEdge => {
    e.type = intern(e.type);
    return e;
  };

  data.nodes.forEach(internNode);
  data.edges.forEach(internEdge);
  for (const lp of data.linked_projects ?? []) {
    lp.nodes.forEach(internNode);
    lp.edges.forEach(internEdge);
    lp.cross_edges.forEach(internEdge);
  }
  if (data.missed_graph) {
    data.missed_graph.nodes.forEach(internNode);
    data.missed_graph.edges.forEach(internEdge);
  }

  return data;
}

export async function fetchLayout(
  project: string,
  maxNodes = GRAPH_RENDER_NODE_LIMIT,
  onProgress?: (progress: LoadProgress) => void,
  graph: GraphVariant = "code",
): Promise<GraphData> {
  const params = new URLSearchParams({ project, max_nodes: String(maxNodes) });
  if (graph === "missed") params.set("graph", "missed");
  const res = await fetch(`/api/layout?${params}`);

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error ?? `HTTP ${res.status}`);
  }

  /* Stream the body when possible so large budgets show live download
   * progress instead of a silent stall. */
  if (!res.body || !onProgress) {
    return internGraphStrings(await res.json());
  }

  const lengthHeader = res.headers.get("content-length");
  const totalBytes = lengthHeader ? parseInt(lengthHeader, 10) || null : null;
  const reader = res.body.getReader();
  /* Decode each chunk to text as it arrives instead of buffering every raw
   * byte chunk and merging them into one big Uint8Array before decoding —
   * that merge step is a full extra copy of the payload (a 382MB response
   * meant a 382MB Uint8Array copy, then a 382MB string, then the parsed
   * object, all transiently live at once). Streaming the decode avoids the
   * byte-level copy; only the (unavoidable) decoded string pieces + their
   * join remain. */
  const decoder = new TextDecoder();
  const textChunks: string[] = [];
  let receivedBytes = 0;

  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    receivedBytes += value.length;
    textChunks.push(decoder.decode(value, { stream: true }));
    onProgress({ receivedBytes, totalBytes });
  }
  textChunks.push(decoder.decode());

  return internGraphStrings(JSON.parse(textChunks.join("")));
}

const NO_PROGRESS: LoadProgress = { receivedBytes: 0, totalBytes: null };

export function useGraphData(): UseGraphDataResult {
  const [data, setData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<LoadProgress>(NO_PROGRESS);

  const fetchOverview = useCallback(
    async (project: string, maxNodes?: number, graph: GraphVariant = "code") => {
      setLoading(true);
      setError(null);
      setProgress(NO_PROGRESS);
      try {
        const result = await fetchLayout(project, maxNodes, setProgress, graph);
        setData(result);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to fetch layout");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const fetchDetail = useCallback(
    async (project: string, _centerNode: string) => {
      setLoading(true);
      setError(null);
      setProgress(NO_PROGRESS);
      try {
        /* TODO: detail level with center_node filtering */
        const result = await fetchLayout(project, undefined, setProgress);
        setData(result);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to fetch layout");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  return { data, loading, error, progress, fetchOverview, fetchDetail };
}
