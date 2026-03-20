import { useCallback, useState } from "react";
import type { GraphData, ServiceGraph } from "../lib/types";
import { callTool } from "../api/rpc";
import { layoutServiceGraph } from "../lib/serviceGraphLayout";

export const SERVICE_GRAPH_SENTINEL = "__service_graph__";

interface UseGraphDataResult {
  data: GraphData | null;
  loading: boolean;
  error: string | null;
  fetchOverview: (project: string) => void;
  fetchDetail: (project: string, centerNode: string) => void;
}

async function fetchLayout(
  project: string,
  maxNodes = 50000,
): Promise<GraphData> {
  const params = new URLSearchParams({ project, max_nodes: String(maxNodes) });
  const res = await fetch(`/api/layout?${params}`);

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error ?? `HTTP ${res.status}`);
  }

  return res.json();
}

export function useGraphData(): UseGraphDataResult {
  const [data, setData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchOverview = useCallback(async (project: string) => {
    setLoading(true);
    setError(null);
    try {
      let result: GraphData;
      if (project === SERVICE_GRAPH_SENTINEL) {
        const sg = await callTool<ServiceGraph>("get_graph");
        result = layoutServiceGraph(sg);
      } else {
        result = await fetchLayout(project, 50000);
      }
      setData(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch layout");
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchDetail = useCallback(
    async (project: string, _centerNode: string) => {
      setLoading(true);
      setError(null);
      try {
        /* TODO: detail level with center_node filtering */
        const result = await fetchLayout(project, 50000);
        setData(result);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to fetch layout");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  return { data, loading, error, fetchOverview, fetchDetail };
}
