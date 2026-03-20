import { useEffect, useState } from "react";
import { callTool } from "../api/rpc";
import { SERVICE_NODE_COLORS } from "../lib/colors";
import type {
  GraphNode,
  GraphEdge,
  GraphData,
  ServiceGraph,
} from "../lib/types";

interface OverlayResult {
  overlayNodes: GraphNode[];
  overlayEdges: GraphEdge[];
  loading: boolean;
}

/**
 * Fetches the service graph and produces overlay nodes/edges that can be
 * merged into an existing code-graph view.
 *
 * For each service-graph edge whose `file` starts with `projectRootPath`,
 * we find the matching File node in the code graph and create a new node
 * for the cross-service target (Topic, Table, etc.) positioned near it.
 */
export function useServiceOverlay(
  codeGraphData: GraphData | null,
  projectRootPath: string | null,
): OverlayResult {
  const [overlayNodes, setOverlayNodes] = useState<GraphNode[]>([]);
  const [overlayEdges, setOverlayEdges] = useState<GraphEdge[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!codeGraphData || !projectRootPath) {
      setOverlayNodes([]);
      setOverlayEdges([]);
      return;
    }

    let cancelled = false;

    async function fetch() {
      setLoading(true);
      try {
        const sg = await callTool<ServiceGraph>("get_graph");
        if (cancelled) return;

        // Build a lookup: file_path → code-graph node
        const fileNodeMap = new Map<string, GraphNode>();
        for (const n of codeGraphData!.nodes) {
          if (n.file_path && n.label === "File") {
            fileNodeMap.set(n.file_path, n);
          }
        }

        // ID space for overlay nodes: start well above existing IDs
        const maxExistingId = codeGraphData!.nodes.reduce(
          (m, n) => Math.max(m, n.id),
          0,
        );
        let nextId = maxExistingId + 1000;

        // Track created overlay targets to avoid duplicates
        const targetNodeMap = new Map<string, number>(); // target string id → numeric id
        const newNodes: GraphNode[] = [];
        const newEdges: GraphEdge[] = [];

        // Determine which edge types map to which node kinds
        function kindForEdgeType(type: string): string {
          if (type === "publishes" || type === "subscribes") return "Topic";
          if (type.startsWith("graphql_")) return "GraphQL";
          if (type.startsWith("db_")) return "Table";
          return "Service";
        }

        for (const edge of sg.edges) {
          // Filter edges to those relevant to this project
          if (!edge.file.startsWith(projectRootPath!)) continue;

          // Try to find the File node matching this edge's file
          // The code graph file_path may be relative while edge.file is absolute
          let fileNode: GraphNode | undefined;
          for (const [fp, node] of fileNodeMap) {
            if (edge.file.endsWith(fp) || fp.endsWith(edge.file) || edge.file === fp) {
              fileNode = node;
              break;
            }
          }
          if (!fileNode) continue;

          // Get or create overlay node for the target
          const targetKey = edge.target;
          let targetId = targetNodeMap.get(targetKey);
          if (targetId === undefined) {
            targetId = nextId++;
            targetNodeMap.set(targetKey, targetId);

            // Position near the connected file node with a radial offset
            const angle = (targetNodeMap.size * 2.4) % (2 * Math.PI);
            const offsetDist = 15 + Math.random() * 10;
            const kind = kindForEdgeType(edge.type);

            newNodes.push({
              id: targetId,
              x: fileNode.x + Math.cos(angle) * offsetDist,
              y: fileNode.y + Math.sin(angle) * offsetDist,
              z: fileNode.z + (Math.random() - 0.5) * 5,
              label: kind,
              name: targetKey,
              size: 3,
              color: SERVICE_NODE_COLORS[kind] ?? "#94a3b8",
            });
          }

          newEdges.push({
            source: fileNode.id,
            target: targetId,
            type: edge.type,
          });
        }

        if (!cancelled) {
          setOverlayNodes(newNodes);
          setOverlayEdges(newEdges);
        }
      } catch {
        // Service graph unavailable — no overlay
        if (!cancelled) {
          setOverlayNodes([]);
          setOverlayEdges([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetch();
    return () => { cancelled = true; };
  }, [codeGraphData, projectRootPath]);

  return { overlayNodes, overlayEdges, loading };
}
