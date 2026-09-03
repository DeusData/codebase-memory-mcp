import type { GraphData, GraphEdge, GraphNode } from "./types";

export interface Connection {
  node: GraphNode;
  edgeType: string;
  direction: "inbound" | "outbound";
}

export interface GraphIndex {
  nodeById: Map<number, GraphNode>;
  /** Precomputed per-node connection lists (both directions), built once per
   * filteredData change instead of scanning every edge on every node click
   * (NodeDetailPanel previously rebuilt a node Map and scanned all edges on
   * every render — O(nodes + edges) per click on a 4.2M-edge graph). */
  connectionsByNode: Map<number, Connection[]>;
}

function addConnection(
  index: Map<number, Connection[]>,
  id: number,
  conn: Connection,
) {
  let list = index.get(id);
  if (!list) {
    list = [];
    index.set(id, list);
  }
  list.push(conn);
}

export function buildGraphIndex(
  data: Pick<GraphData, "nodes" | "edges"> | null | undefined,
): GraphIndex {
  const nodeById = new Map<number, GraphNode>();
  const connectionsByNode = new Map<number, Connection[]>();
  if (!data) return { nodeById, connectionsByNode };

  for (const n of data.nodes) nodeById.set(n.id, n);

  for (const edge of data.edges as GraphEdge[]) {
    const s = nodeById.get(edge.source);
    const t = nodeById.get(edge.target);
    if (!s || !t) continue;
    addConnection(connectionsByNode, edge.source, {
      node: t,
      edgeType: edge.type,
      direction: "outbound",
    });
    addConnection(connectionsByNode, edge.target, {
      node: s,
      edgeType: edge.type,
      direction: "inbound",
    });
  }

  return { nodeById, connectionsByNode };
}
